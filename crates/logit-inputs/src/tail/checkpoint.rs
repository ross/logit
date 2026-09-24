//! Persisted read offsets, so a restart resumes tailing instead of replaying or skipping.
//!
//! Written on an interval and only when dirty, never per line, so a crash can replay up to
//! `checkpoint_interval` of already-emitted lines: accepted at-least-once behavior
//! (`docs/adr/file-tailing-and-docker-json-logs.md`'s "Checkpoints: optional, written on an
//! interval, only when dirty"). Every accumulator is flushed before a write, and the offset
//! written excludes a held partial line (`Tailer::write_checkpoint` subtracts
//! `LineSplitter::pending_bytes()`), so a crash can re-emit lines but never lose one that was read.
//! Each write is durable against a power loss, and a checkpoint that exists but can't be used
//! replays every file from its beginning rather than falling back to `read_from`, so that holds
//! after a power loss or corruption too (`docs/adr/durable-checkpoint-writes-and-fault-injection.md`).

use logit_core::{Diagnostics, Telemetry};
use logit_pipeline::atomic_write;
use logit_pipeline::fault::sites;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::future::Future;
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

/// What [`CheckpointStore::load`] found at the checkpoint path.
#[derive(Debug)]
pub(crate) enum Loaded {
    /// No checkpoint and no tmp file beside it: a first run, so `read_from` decides.
    Missing,
    /// A usable checkpoint's offsets, keyed by [`FileId`], not path, so a file renamed before the
    /// restart still resumes under its new name.
    Resume(HashMap<FileId, (PathBuf, u64)>),
    /// A checkpoint that exists but can't be used. A previous run read these files, so every file
    /// present at the first scan starts at its beginning, whatever `read_from` says
    /// (`docs/adr/durable-checkpoint-writes-and-fault-injection.md`, decision 4).
    Unusable,
}

/// One tailing component's checkpoint file: loaded once at startup, and written only when dirty
/// or forced.
pub(crate) struct CheckpointStore {
    path: PathBuf,
    dirty: bool,
}

impl CheckpointStore {
    /// Loads `path`. Never fatal to the component.
    ///
    /// Unreadable, malformed, empty, or wrong-version is [`Loaded::Unusable`], and so is a
    /// missing checkpoint with its tmp file beside it (a crash before the first rename landed).
    /// Each counts `logit.input.checkpoint.errors{op="load"}` and is diagnosed `checkpoint_error`.
    /// A blocking read: it runs once, at bind.
    pub fn load(path: PathBuf, diag: &mut Diagnostics, telemetry: &Telemetry) -> (Self, Loaded) {
        let unusable = match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<CheckpointFile>(&bytes) {
                Ok(cp) if cp.version == CHECKPOINT_VERSION => {
                    let resume = cp
                        .files
                        .into_iter()
                        .map(|entry| {
                            (
                                FileId { dev: entry.dev, ino: entry.ino },
                                (PathBuf::from(entry.path), entry.offset),
                            )
                        })
                        .collect();
                    return (Self { path, dirty: false }, Loaded::Resume(resume));
                }
                Ok(cp) => format!("unsupported version {}", cp.version),
                Err(err) => format!("malformed: {err}"),
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                let tmp = atomic_write::tmp_path(&path);
                if !tmp.exists() {
                    return (Self { path, dirty: false }, Loaded::Missing);
                }
                format!("missing, but {} exists beside it", tmp.display())
            }
            Err(err) => format!("unreadable: {err}"),
        };
        telemetry.count("logit.input.checkpoint.errors", 1.0, &[("op", "load")]);
        diag.warn_throttled(
            "checkpoint_error",
            format!(
                "checkpoint {} is unusable ({unusable}) -- every file present now starts from \
                 its beginning, so expect duplicates",
                path.display()
            ),
        );
        (Self { path, dirty: false }, Loaded::Unusable)
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Replaces the checkpoint with `entries`; a no-op unless dirty or `force`.
    ///
    /// `entries` must be the tailer's tracked files, which is what prunes: a rotated or removed
    /// file has left that set, so its entry isn't rewritten. The document is serialized here, then
    /// [`atomic_write::write_file_durably`] runs on the blocking pool, so the new checkpoint
    /// survives a power loss once this returns. A failed write counts
    /// `logit.input.checkpoint.errors{op="write"}` and leaves the store dirty, so the next tick
    /// retries.
    ///
    /// Not an `async fn`: `entries` borrows the tailer's files, which aren't `Sync`, so it's
    /// consumed before the returned future is built rather than held across its `.await`.
    pub fn write<'s, 'a>(
        &'s mut self,
        entries: impl Iterator<Item = (FileId, &'a Path, u64)>,
        force: bool,
        diag: &'s mut Diagnostics,
        telemetry: &'s Telemetry,
    ) -> impl Future<Output = ()> + Send + 's {
        let encoded = (self.dirty || force).then(|| {
            let files = entries
                .map(|(id, path, offset)| CheckpointEntry {
                    dev: id.dev,
                    ino: id.ino,
                    path: path.to_string_lossy().into_owned(),
                    offset,
                })
                .collect();
            serde_json::to_vec_pretty(&CheckpointFile { version: CHECKPOINT_VERSION, files })
        });
        async move {
            let bytes = match encoded {
                None => return,
                Some(Ok(bytes)) => bytes,
                Some(Err(err)) => {
                    telemetry.count("logit.input.checkpoint.errors", 1.0, &[("op", "write")]);
                    diag.warn_throttled("checkpoint_error", format!("encoding checkpoint: {err}"));
                    return;
                }
            };
            let path = self.path.clone();
            let result = tokio::task::spawn_blocking(move || {
                atomic_write::write_file_durably(&path, &bytes, sites::TAIL_CHECKPOINT)
            })
            .await;
            let (err, outcome) = match result {
                Ok(Ok(())) => {
                    self.dirty = false;
                    telemetry.count("logit.input.checkpoint.writes", 1.0, &[]);
                    return;
                }
                Ok(Err(err)) if err.replaced() => (
                    err.to_string(),
                    "the new checkpoint is in place but may not survive a power loss",
                ),
                Ok(Err(err)) => (err.to_string(), "the previous checkpoint stays in effect"),
                // A panic in the helper; the step it reached is unknown.
                Err(join) => (join.to_string(), "the checkpoint on disk may be old or new"),
            };
            telemetry.count("logit.input.checkpoint.errors", 1.0, &[("op", "write")]);
            diag.warn_throttled(
                "checkpoint_error",
                format!(
                    "writing checkpoint {}: {err} -- {outcome}; the next tick retries",
                    self.path.display()
                ),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tail::test_support::scratch_dir;
    use logit_core::{MetricKind, Registry, Value};
    use logit_pipeline::atomic_write::{tmp_path, Step};
    use logit_pipeline::fault::{self, errno, Op, Point};
    use std::sync::Arc;

    /// A diagnostics/telemetry pair a test can read back.
    struct Observed {
        diag: Diagnostics,
        registry: Arc<Registry>,
        telemetry: Telemetry,
    }

    fn observed() -> Observed {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("tail_in", "tail_in", "listener");
        Observed { diag: Diagnostics::new("test"), registry, telemetry }
    }

    impl Observed {
        fn load(&mut self, path: &Path) -> (CheckpointStore, Loaded) {
            CheckpointStore::load(path.to_path_buf(), &mut self.diag, &self.telemetry)
        }

        /// Sums `logit.input.checkpoint.<name>` tagged `op` (or untagged, for `None`). Drains the
        /// registry, so it's a one-shot check.
        fn counted(&self, name: &str, op: Option<&str>) -> f64 {
            self.registry
                .drain(0)
                .iter()
                .filter(|e| e.attributes.get("op").and_then(Value::as_str) == op)
                .flat_map(|e| &e.metrics)
                .filter(|m| logit_core::interner::resolve(m.name) == name)
                .map(|m| match &m.kind {
                    MetricKind::Sum(sum) => sum.value,
                    other => panic!("{name} must be a counter, got {other:?}"),
                })
                .sum()
        }
    }

    fn resume_of(loaded: Loaded) -> HashMap<FileId, (PathBuf, u64)> {
        match loaded {
            Loaded::Resume(resume) => resume,
            other => panic!("expected a usable checkpoint, got {other:?}"),
        }
    }

    async fn write_one(store: &mut CheckpointStore, obs: &mut Observed, id: FileId, offset: u64) {
        let file = PathBuf::from("/var/log/app.log");
        store.mark_dirty();
        store
            .write([(id, file.as_path(), offset)].into_iter(), false, &mut obs.diag, &obs.telemetry)
            .await;
    }

    /// Asserts `path` loads as unusable, counted once and diagnosed once.
    fn assert_unusable(path: &Path) {
        let mut obs = observed();
        let (_store, loaded) = obs.load(path);
        assert!(matches!(loaded, Loaded::Unusable), "got {loaded:?}");
        assert_eq!(obs.counted("logit.input.checkpoint.errors", Some("load")), 1.0);
        assert_eq!(obs.diag.occurrences("checkpoint_error"), 1);
    }

    const ID: FileId = FileId { dev: 1, ino: 42 };
    const STEPS: [(Step, Op); 4] = [
        (Step::Write, Op::Write),
        (Step::SyncFile, Op::SyncFile),
        (Step::Rename, Op::Rename),
        (Step::SyncDir, Op::SyncDir),
    ];

    fn point(op: Op) -> Point {
        Point::new(fault::sites::TAIL_CHECKPOINT, op)
    }

    #[test]
    fn a_missing_checkpoint_alone_is_missing() {
        let dir = scratch_dir("checkpoint-missing");
        let mut obs = observed();
        let (_store, loaded) = obs.load(&dir.join("does-not-exist.json"));
        assert!(matches!(loaded, Loaded::Missing), "got {loaded:?}");
        assert_eq!(obs.counted("logit.input.checkpoint.errors", Some("load")), 0.0);
        assert_eq!(obs.diag.occurrences("checkpoint_error"), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// What a power loss leaves when the rename was durable and the bytes weren't.
    #[test]
    fn an_empty_checkpoint_is_unusable_not_missing() {
        let dir = scratch_dir("checkpoint-empty");
        let path = dir.join("checkpoint.json");
        std::fs::write(&path, b"").unwrap();
        assert_unusable(&path);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_truncated_checkpoint_document_is_unusable_and_counted() {
        let dir = scratch_dir("checkpoint-truncated");
        let path = dir.join("checkpoint.json");
        std::fs::write(&path, br#"{"version":1,"files":[{"dev":1,"ino":42,"pa"#).unwrap();
        assert_unusable(&path);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_unreadable_checkpoint_is_unusable() {
        let dir = scratch_dir("checkpoint-unreadable");
        let path = dir.join("checkpoint.json");
        std::fs::create_dir(&path).unwrap(); // `read` fails `EISDIR`, not `ENOENT`
        assert_unusable(&path);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_wrong_version_checkpoint_is_unusable() {
        let dir = scratch_dir("checkpoint-badversion");
        let path = dir.join("checkpoint.json");
        std::fs::write(&path, br#"{"version": 99, "files": []}"#).unwrap();
        assert_unusable(&path);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A crash between the very first checkpoint's write and its rename: files were read, so this
    /// isn't a first run.
    #[test]
    fn a_missing_checkpoint_with_a_stray_tmp_beside_it_is_unusable() {
        let dir = scratch_dir("checkpoint-stray-tmp");
        let path = dir.join("checkpoint.json");
        std::fs::write(tmp_path(&path), br#"{"version":1,"files":[]}"#).unwrap();
        assert_unusable(&path);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn checkpoints_differing_only_in_extension_never_share_a_tmp() {
        let dir = scratch_dir("checkpoint-extensions");
        let json = dir.join("state.json");
        let yaml = dir.join("state.yaml");
        let mut obs = observed();
        let (mut a, _) = obs.load(&json);
        let (mut b, _) = obs.load(&yaml);

        // `with_extension("tmp")` would map both to `state.tmp`.
        let scope = fault::scope(&dir);
        scope.record();
        write_one(&mut a, &mut obs, ID, 1).await;
        write_one(&mut b, &mut obs, ID, 2).await;
        let tmps: Vec<PathBuf> = scope
            .hits()
            .into_iter()
            .filter(|h| h.point == point(Op::Write))
            .map(|h| h.path)
            .collect();
        drop(scope);

        assert_eq!(tmps, vec![tmp_path(&json), tmp_path(&yaml)]);
        assert_ne!(tmp_path(&json), tmp_path(&yaml));
        assert!(!dir.join("state.tmp").exists());
        assert_eq!(resume_of(obs.load(&json).1)[&ID].1, 1);
        assert_eq!(resume_of(obs.load(&yaml).1)[&ID].1, 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_write_at_any_step_leaves_the_store_dirty_and_the_next_write_lands() {
        for (step, op) in STEPS {
            let dir = scratch_dir("checkpoint-write-fails");
            let path = dir.join("checkpoint.json");
            let mut obs = observed();
            let (mut store, _) = obs.load(&path);
            write_one(&mut store, &mut obs, ID, 10).await;
            assert_eq!(obs.counted("logit.input.checkpoint.writes", None), 1.0);

            let scope = fault::scope(&dir);
            scope.fail_nth(point(op), 1, errno::EIO);
            write_one(&mut store, &mut obs, ID, 20).await;
            assert!(store.dirty, "{step:?}: a failed write keeps the store dirty");
            assert_eq!(
                obs.counted("logit.input.checkpoint.errors", Some("write")),
                1.0,
                "{step:?}"
            );
            assert_eq!(obs.diag.occurrences("checkpoint_error"), 1, "{step:?}");
            let expected = if step == Step::SyncDir { 20 } else { 10 };
            assert_eq!(resume_of(obs.load(&path).1)[&ID].1, expected, "{step:?}");

            // The next tick needs no new data to retry: the store is still dirty.
            let file = PathBuf::from("/var/log/app.log");
            store
                .write([(ID, file.as_path(), 30)].into_iter(), false, &mut obs.diag, &obs.telemetry)
                .await;
            drop(scope);
            assert!(!store.dirty, "{step:?}");
            assert_eq!(resume_of(obs.load(&path).1)[&ID].1, 30, "{step:?}");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[tokio::test]
    async fn a_crash_at_any_step_of_a_write_leaves_the_previous_checkpoint_loadable() {
        for (step, op) in STEPS {
            let dir = scratch_dir("checkpoint-write-crash");
            let path = dir.join("checkpoint.json");
            let mut obs = observed();
            let (mut store, _) = obs.load(&path);
            write_one(&mut store, &mut obs, ID, 10).await;

            let scope = fault::scope(&dir);
            scope.crash_at(point(op), 1);
            write_one(&mut store, &mut obs, ID, 20).await;
            assert!(scope.crashed(), "{step:?}");
            drop(store);
            scope.revive();
            drop(scope);

            // Only a crash at the directory sync comes after the rename. The tmp file a crash
            // at `SyncFile` or `Rename` leaves behind doesn't matter: the checkpoint is present.
            let expected = if step == Step::SyncDir { 20 } else { 10 };
            assert_eq!(resume_of(obs.load(&path).1)[&ID].1, expected, "{step:?}");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[tokio::test]
    async fn a_checkpoint_write_fsyncs_the_file_before_the_rename_and_the_directory_after() {
        let dir = scratch_dir("checkpoint-fsync-order");
        let path = dir.join("checkpoint.json");
        let mut obs = observed();
        let (mut store, _) = obs.load(&path);

        let scope = fault::scope(&dir);
        scope.record();
        write_one(&mut store, &mut obs, ID, 10).await;
        let hits: Vec<(Point, PathBuf)> =
            scope.hits().into_iter().map(|h| (h.point, h.path)).collect();
        drop(scope);

        assert_eq!(
            hits,
            vec![
                (point(Op::Write), tmp_path(&path)),
                (point(Op::SyncFile), tmp_path(&path)),
                (point(Op::Rename), path.clone()),
                (point(Op::SyncDir), dir.clone()),
            ]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_write_then_load_round_trips_every_entry() {
        let dir = scratch_dir("checkpoint-roundtrip");
        let path = dir.join("checkpoint.json");
        let mut obs = observed();

        let (mut store, loaded) = obs.load(&path);
        assert!(matches!(loaded, Loaded::Missing));
        store.mark_dirty();
        let file_path = dir.join("app.log");
        store
            .write(
                [(ID, file_path.as_path(), 123u64)].into_iter(),
                false,
                &mut obs.diag,
                &obs.telemetry,
            )
            .await;

        let resume = resume_of(obs.load(&path).1);
        assert_eq!(resume.get(&ID), Some(&(file_path, 123)));
        assert!(!tmp_path(&path).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_write_with_nothing_dirty_and_not_forced_is_a_no_op() {
        let dir = scratch_dir("checkpoint-noop");
        let path = dir.join("checkpoint.json");
        let mut obs = observed();

        let (mut store, _) = obs.load(&path);
        store.write(std::iter::empty(), false, &mut obs.diag, &obs.telemetry).await;

        assert!(!path.exists(), "an undirtied, unforced write should not create the file");
        assert_eq!(obs.counted("logit.input.checkpoint.writes", None), 0.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_forced_write_persists_even_when_not_dirty() {
        let dir = scratch_dir("checkpoint-forced");
        let path = dir.join("checkpoint.json");
        let mut obs = observed();

        let (mut store, _) = obs.load(&path);
        let id = FileId { dev: 2, ino: 7 };
        let file_path = dir.join("app.log");
        store
            .write(
                [(id, file_path.as_path(), 5u64)].into_iter(),
                true,
                &mut obs.diag,
                &obs.telemetry,
            )
            .await;

        assert!(path.exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A write naming fewer entries than the last drops the missing one's stale offset.
    #[tokio::test]
    async fn a_subsequent_write_prunes_entries_no_longer_passed_in() {
        let dir = scratch_dir("checkpoint-prune");
        let path = dir.join("checkpoint.json");
        let mut obs = observed();

        let (mut store, _) = obs.load(&path);
        let a = FileId { dev: 1, ino: 1 };
        let b = FileId { dev: 1, ino: 2 };
        let a_path = dir.join("a.log");
        let b_path = dir.join("b.log");
        store.mark_dirty();
        store
            .write(
                [(a, a_path.as_path(), 10u64), (b, b_path.as_path(), 20u64)].into_iter(),
                false,
                &mut obs.diag,
                &obs.telemetry,
            )
            .await;
        store.mark_dirty();
        store
            .write([(a, a_path.as_path(), 15u64)].into_iter(), false, &mut obs.diag, &obs.telemetry)
            .await;

        let resume = resume_of(obs.load(&path).1);
        assert_eq!(resume.get(&a), Some(&(a_path, 15)));
        assert_eq!(resume.get(&b), None, "b should have been pruned");
        std::fs::remove_dir_all(&dir).ok();
    }
}
