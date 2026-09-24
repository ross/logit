//! Persisted read offsets, so a restart resumes tailing instead of replaying or skipping.
//!
//! Written on an interval and only when dirty, never per line, so a crash can replay up to
//! `checkpoint_interval` of already-emitted lines: accepted at-least-once behavior
//! (`docs/adr/file-tailing-and-docker-json-logs.md`'s "Checkpoints: optional, written on an
//! interval, only when dirty"). Every accumulator is flushed before a write, and the offset
//! written excludes a held partial line (`Tailer::write_checkpoint` subtracts
//! `LineSplitter::pending_bytes()`), so a crash can re-emit lines but never lose one that was read.

use logit_core::{Diagnostics, Telemetry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

/// A tailed file's `(st_dev, st_ino)` identity, stable across a rename and a restart.
///
/// Rotation keeps the path and changes the inode, so a checkpoint matches on this pair only. The
/// persisted path is for a human reading the file, never matched on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct FileId {
    pub dev: u64,
    pub ino: u64,
}

impl FileId {
    #[cfg(unix)]
    pub fn from_metadata(meta: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self { dev: meta.dev(), ino: meta.ino() }
    }
}

const CHECKPOINT_VERSION: u32 = 1;

#[derive(Serialize, Deserialize)]
struct CheckpointFile {
    version: u32,
    files: Vec<CheckpointEntry>,
}

#[derive(Serialize, Deserialize, Clone)]
struct CheckpointEntry {
    dev: u64,
    ino: u64,
    path: String,
    offset: u64,
}

/// One tailing component's checkpoint file: loaded once at startup, and written only when dirty
/// or forced.
pub(crate) struct CheckpointStore {
    path: PathBuf,
    dirty: bool,
}

impl CheckpointStore {
    /// Loads `path`, returning the store and a resume map keyed by [`FileId`], not path, so a
    /// file renamed before the restart still resumes under its new name.
    ///
    /// A missing file is the first-run case, not an error. An unreadable, malformed, or
    /// wrong-version one is diagnosed `checkpoint_error` and treated as missing (every file falls
    /// back to `read_from`); it's never fatal to the component.
    pub fn load(path: PathBuf, diag: &mut Diagnostics) -> (Self, HashMap<FileId, (PathBuf, u64)>) {
        let mut resume = HashMap::new();
        match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<CheckpointFile>(&bytes) {
                Ok(cp) if cp.version == CHECKPOINT_VERSION => {
                    for entry in cp.files {
                        resume.insert(
                            FileId { dev: entry.dev, ino: entry.ino },
                            (PathBuf::from(entry.path), entry.offset),
                        );
                    }
                }
                Ok(cp) => {
                    diag.warn_throttled(
                        "checkpoint_error",
                        format!(
                            "unsupported checkpoint version {} in {} -- ignoring, every file \
                             falls back to its configured starting point",
                            cp.version,
                            path.display()
                        ),
                    );
                }
                Err(err) => {
                    diag.warn_throttled(
                        "checkpoint_error",
                        format!("malformed checkpoint at {}: {err}", path.display()),
                    );
                }
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => {}
            Err(err) => {
                diag.warn_throttled(
                    "checkpoint_error",
                    format!("reading checkpoint {}: {err}", path.display()),
                );
            }
        }
        (Self { path, dirty: false }, resume)
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Replaces the checkpoint with `entries`; a no-op unless dirty or `force`.
    ///
    /// `entries` must be the tailer's tracked files, which is what prunes: a rotated or removed
    /// file has left that set, so its entry isn't rewritten. The tmp file + rename is atomic
    /// against a process crash, but nothing is fsynced, so a power loss can still lose or empty
    /// the file (then treated as missing by [`CheckpointStore::load`]). A failed write leaves the
    /// store dirty, so the next tick retries.
    pub fn write<'a>(
        &mut self,
        entries: impl Iterator<Item = (FileId, &'a Path, u64)>,
        force: bool,
        diag: &mut Diagnostics,
        telemetry: &Telemetry,
    ) {
        if !self.dirty && !force {
            return;
        }
        let files: Vec<CheckpointEntry> = entries
            .map(|(id, path, offset)| CheckpointEntry {
                dev: id.dev,
                ino: id.ino,
                path: path.to_string_lossy().into_owned(),
                offset,
            })
            .collect();
        let doc = CheckpointFile { version: CHECKPOINT_VERSION, files };
        let bytes = match serde_json::to_vec_pretty(&doc) {
            Ok(bytes) => bytes,
            Err(err) => {
                diag.warn_throttled("checkpoint_error", format!("encoding checkpoint: {err}"));
                return;
            }
        };
        let tmp = self.path.with_extension("tmp");
        let result = std::fs::write(&tmp, &bytes).and_then(|()| std::fs::rename(&tmp, &self.path));
        match result {
            Ok(()) => {
                self.dirty = false;
                telemetry.count("logit.input.checkpoint.writes", 1.0, &[]);
            }
            Err(err) => {
                diag.warn_throttled(
                    "checkpoint_error",
                    format!("writing checkpoint {}: {err}", self.path.display()),
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loading_a_missing_checkpoint_returns_an_empty_resume_map_without_diagnosing() {
        let dir = crate::tail::test_support::scratch_dir("checkpoint-missing");
        let mut diag = Diagnostics::new("test");
        let (_store, resume) = CheckpointStore::load(dir.join("does-not-exist.json"), &mut diag);
        assert!(resume.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_write_then_load_round_trips_every_entry() {
        let dir = crate::tail::test_support::scratch_dir("checkpoint-roundtrip");
        let path = dir.join("checkpoint.json");
        let mut diag = Diagnostics::new("test");
        let telemetry = Telemetry::default();

        let (mut store, _) = CheckpointStore::load(path.clone(), &mut diag);
        store.mark_dirty();
        let id = FileId { dev: 1, ino: 42 };
        let file_path = dir.join("app.log");
        store.write([(id, file_path.as_path(), 123u64)].into_iter(), false, &mut diag, &telemetry);

        let (_store2, resume) = CheckpointStore::load(path, &mut diag);
        assert_eq!(resume.get(&id), Some(&(file_path, 123)));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_write_with_nothing_dirty_and_not_forced_is_a_no_op() {
        let dir = crate::tail::test_support::scratch_dir("checkpoint-noop");
        let path = dir.join("checkpoint.json");
        let mut diag = Diagnostics::new("test");
        let telemetry = Telemetry::default();

        let (mut store, _) = CheckpointStore::load(path.clone(), &mut diag);
        store.write(std::iter::empty(), false, &mut diag, &telemetry);

        assert!(!path.exists(), "an undirtied, unforced write should not create the file");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_forced_write_persists_even_when_not_dirty() {
        let dir = crate::tail::test_support::scratch_dir("checkpoint-forced");
        let path = dir.join("checkpoint.json");
        let mut diag = Diagnostics::new("test");
        let telemetry = Telemetry::default();

        let (mut store, _) = CheckpointStore::load(path.clone(), &mut diag);
        let id = FileId { dev: 2, ino: 7 };
        let file_path = dir.join("app.log");
        store.write([(id, file_path.as_path(), 5u64)].into_iter(), true, &mut diag, &telemetry);

        assert!(path.exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A write naming fewer entries than the last drops the missing one's stale offset.
    #[test]
    fn a_subsequent_write_prunes_entries_no_longer_passed_in() {
        let dir = crate::tail::test_support::scratch_dir("checkpoint-prune");
        let path = dir.join("checkpoint.json");
        let mut diag = Diagnostics::new("test");
        let telemetry = Telemetry::default();

        let (mut store, _) = CheckpointStore::load(path.clone(), &mut diag);
        let a = FileId { dev: 1, ino: 1 };
        let b = FileId { dev: 1, ino: 2 };
        let a_path = dir.join("a.log");
        let b_path = dir.join("b.log");
        store.mark_dirty();
        store.write(
            [(a, a_path.as_path(), 10u64), (b, b_path.as_path(), 20u64)].into_iter(),
            false,
            &mut diag,
            &telemetry,
        );
        store.mark_dirty();
        store.write([(a, a_path.as_path(), 15u64)].into_iter(), false, &mut diag, &telemetry);

        let (_store2, resume) = CheckpointStore::load(path, &mut diag);
        assert_eq!(resume.get(&a), Some(&(a_path, 15)));
        assert_eq!(resume.get(&b), None, "b should have been pruned");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_checkpoint_with_an_unsupported_version_is_ignored_and_diagnosed() {
        let dir = crate::tail::test_support::scratch_dir("checkpoint-badversion");
        let path = dir.join("checkpoint.json");
        std::fs::write(&path, br#"{"version": 99, "files": []}"#).unwrap();
        let mut diag = Diagnostics::new("test");
        let (_store, resume) = CheckpointStore::load(path, &mut diag);
        assert!(resume.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }
}
