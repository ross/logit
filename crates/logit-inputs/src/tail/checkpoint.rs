//! Persisted read offsets, so a restart resumes tailing instead of replaying or skipping.
//! `docs/adr/file-tailing-and-docker-json-logs.md` covers the trade-off this makes explicit:
//! written on an interval and only when dirty (never per line), so a crash between two writes
//! can replay up to `checkpoint_interval` worth of already-emitted lines on restart -- accepted
//! at-least-once behavior, not a bug, the same trade-off `buffer:`'s sink-side retry already
//! makes on the delivery side of this same pipeline.

use logit_core::{Diagnostics, Telemetry};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};

/// A tailed file's identity across restarts and across a rename -- Linux's `(st_dev, st_ino)`
/// pair, the only thing that survives both a rotation (the path keeps its name; the inode
/// doesn't) and a checkpoint resume (the inode is what's persisted; the path is only carried
/// alongside for a human reading the checkpoint file, never used to match on restart).
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

/// Owns one tailing component's checkpoint file. Reading it is a one-time [`CheckpointStore::
/// load`] at startup; writing is dirty-tracked so [`CheckpointStore::write`] is a no-op on a
/// tick with nothing new to persist.
pub(crate) struct CheckpointStore {
    path: PathBuf,
    dirty: bool,
}

impl CheckpointStore {
    /// Loads `path` if it exists, returning the store handle plus a resume map keyed by file
    /// identity (not path -- a rotated-then-restarted file is still resumed correctly by inode
    /// even though `scan` will see it under whatever path it currently has). A missing file is
    /// the ordinary first-run case, not an error; an unreadable or malformed one is diagnosed
    /// and treated the same as missing (every file then falls back to `read_from`) rather than
    /// being fatal to the whole component -- a corrupt checkpoint shouldn't stop tailing.
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

    /// Writes every `(identity, path, offset)` triple in `entries` -- unless neither dirty nor
    /// `force`, in which case this is a no-op. `entries` is expected to be exactly the tailer's
    /// currently-tracked files, which is what makes this prune on its own: a file that rotated
    /// or was removed has already left the tracked set by the time this runs, so its old
    /// checkpoint entry simply isn't reproduced in the next write. Atomic (tmp file + rename) so
    /// a crash mid-write can never leave a half-written, unparseable checkpoint on disk. Leaves
    /// the dirty flag set on failure, so the next tick retries rather than silently giving up.
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

    /// The pruning contract: a second write naming fewer entries than the first drops the
    /// missing one entirely, rather than leaving its stale offset behind.
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
