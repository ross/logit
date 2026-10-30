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
//!
//! # Format 4
//!
//! One entry per file: `dev` and `ino` (the [`FileId`] a resume matches on), `path` (for a human
//! reading the file, and for whether a pattern still covers it; never matched against a file),
//! `offset`, `len`, the head fingerprint `head_len` and `head_hash` ([`Head`]), and `marker`.
//!
//! - **Length.** `len` is the file's size as the tailer last saw it, from a `stat` or `fstat` it
//!   already makes or a read that reached the end, and never less than the offset read. When an
//!   entry's file is gone at a restart, `len - offset` is counted as unread once a write without
//!   the entry lands (`Tailer::prune_checkpoint_entries` has what that count means). It's the size
//!   last seen, not the size at the write: the file may have grown since.
//! - **Head.** The tailer keeps the first `min(HEAD_BYTES, offset)` bytes of each file as it reads
//!   them, and clears them on a truncation. The fingerprint is XXH64, seed 0, over those bytes, so
//!   a head is always from the same generation of the file as the offset beside it, and capturing
//!   it costs no I/O. A file opened at its end reads its head once, at open.
//! - **Marker.** XXH64, seed 0, over the up to `HEAD_BYTES` bytes that end at `offset`
//!   ([`marker`]). Its length is `min(HEAD_BYTES, offset)`, so it isn't stored. The tailer keeps
//!   the bytes ending at its read offset and at its last line boundary as it reads them, and a
//!   persisted offset is one of the two for `tail_in`, so a write reads nothing. A `docker_in`
//!   file whose decoder holds lines persists an offset at the oldest held line's start, which
//!   neither covers; the write reads those bytes back from the open descriptor, once per offset.
//! - **Accept.** A resume at `offset` is accepted iff the file is at least `head_len` bytes long,
//!   its first `head_len` bytes hash to `head_hash`, `offset` is at most its length, and the bytes
//!   ending at `offset` hash to `marker`. Anything else starts the file at `0`, counted
//!   `logit.input.files.resume_rejected` and diagnosed `resume_rejected`. An append-only file
//!   keeps those bytes, so it's never rejected; a recycled inode, or a file truncated and refilled
//!   while not tailed, has to share both ends of what the resume skips to be accepted.
//! - **Claim.** An inode no entry names may resume from an entry by content: the same head and
//!   marker, and at least `offset` bytes (`Tailer::claim_unmatched` has the rule).
//! - **Versioning.** The hashes and `HEAD_BYTES` are part of format 4: changing any is a version
//!   bump. No other version is read, so a file of any other version is unusable, and upgrading
//!   replays every file once.
//!
//! The decisions are `docs/adr/tail-discovery-failure-and-resume-identity.md`, decisions 2 to 4,
//! its amendment adding `len`, and `docs/adr/tail-content-identity.md`, decisions 4 and 5.

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
/// persisted path is for a human reading the file, never matched on. An inode number can be
/// reused, which is what [`Head`] guards against.
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

/// How many leading bytes of a file its [`Head`] covers, and how many bytes before an offset its
/// [`marker`] covers. Part of format 4.
pub(crate) const HEAD_BYTES: usize = 256;

/// A fingerprint of a file's first `len` bytes (at most [`HEAD_BYTES`]): XXH64, seed 0. Part of
/// format 4, and separate from `logit_core::sampling`'s hash, whose freeze is that contract's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Head {
    pub len: u32,
    pub hash: u64,
}

impl Head {
    pub fn of(bytes: &[u8]) -> Self {
        debug_assert!(bytes.len() <= HEAD_BYTES, "a head covers at most {HEAD_BYTES} bytes");
        Self { len: bytes.len() as u32, hash: twox_hash::XxHash64::oneshot(0, bytes) }
    }

    /// Whether `current_prefix`, the file's leading bytes now, starts with the bytes this head
    /// was taken over. A prefix shorter than `len` never matches.
    pub fn matches(&self, current_prefix: &[u8]) -> bool {
        let len = self.len as usize;
        current_prefix.len() >= len && Head::of(&current_prefix[..len]) == *self
    }
}

/// The marker over `bytes`, the up to [`HEAD_BYTES`] bytes ending at an offset: XXH64, seed 0, as
/// [`Head`] hashes. Part of format 4.
pub(crate) fn marker(bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() <= HEAD_BYTES, "a marker covers at most {HEAD_BYTES} bytes");
    twox_hash::XxHash64::oneshot(0, bytes)
}

/// Where a [`Retained`] entry came from, which decides when it's pruned and whether a checkpoint
/// write persists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Source {
    /// Loaded from the checkpoint and not yet consumed. Persisted by every write until pruned.
    Checkpoint,
    /// Kept by the tailer for a file de-selected while still alive. Process-local.
    Deselected,
}

/// A position an inode resumes from when next opened, and the head that must still match.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Retained {
    pub path: PathBuf,
    pub offset: u64,
    /// The file's size as last seen, at least `offset`: a gone file's unread bytes are counted
    /// against it.
    pub len: u64,
    pub head: Head,
    /// The [`marker`] over the bytes ending at `offset`.
    pub marker: u64,
    pub source: Source,
}

/// One entry a [`CheckpointStore::write`] persists.
pub(crate) struct Persisted<'a> {
    pub id: FileId,
    pub path: &'a Path,
    pub offset: u64,
    pub len: u64,
    pub head: Head,
    pub marker: u64,
}

const CHECKPOINT_VERSION: u32 = 4;

/// Parsed before [`CheckpointFile`], so another version's document reports its version rather
/// than the first field it lacks.
#[derive(Deserialize)]
struct VersionProbe {
    version: u32,
}

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
    len: u64,
    head_len: u32,
    head_hash: u64,
    marker: u64,
}

/// What [`CheckpointStore::load`] found at the checkpoint path.
#[derive(Debug)]
pub(crate) enum Loaded {
    /// No checkpoint and no tmp file beside it: a first run, so `read_from` decides.
    Missing,
    /// A usable checkpoint's entries, each [`Source::Checkpoint`], keyed by [`FileId`], not path,
    /// so a file renamed before the restart still resumes under its new name.
    Resume(HashMap<FileId, Retained>),
    /// A checkpoint that exists but can't be used. A previous run read these files, so every file
    /// present at the first scan starts at its beginning, whatever `read_from` says
    /// (`docs/adr/durable-checkpoint-writes-and-fault-injection.md`, decision 4).
    Unusable,
}

/// Parses a checkpoint document: its entries, or why it's unusable.
fn parse(bytes: &[u8]) -> Result<HashMap<FileId, Retained>, String> {
    let probe: VersionProbe =
        serde_json::from_slice(bytes).map_err(|err| format!("malformed: {err}"))?;
    if probe.version != CHECKPOINT_VERSION {
        return Err(format!("unsupported version {}", probe.version));
    }
    let cp: CheckpointFile =
        serde_json::from_slice(bytes).map_err(|err| format!("malformed: {err}"))?;
    cp.files
        .into_iter()
        .map(|entry| {
            if entry.head_len as usize > HEAD_BYTES {
                return Err(format!(
                    "malformed: head_len {} is over {HEAD_BYTES} for {}",
                    entry.head_len, entry.path
                ));
            }
            let retained = Retained {
                path: PathBuf::from(entry.path),
                offset: entry.offset,
                len: entry.len,
                head: Head { len: entry.head_len, hash: entry.head_hash },
                marker: entry.marker,
                source: Source::Checkpoint,
            };
            Ok((FileId { dev: entry.dev, ino: entry.ino }, retained))
        })
        .collect()
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
    /// Unreadable, malformed, empty, or another version is [`Loaded::Unusable`], and so is a
    /// missing checkpoint with its tmp file beside it (a crash before the first rename landed), or
    /// with a tmp path that can't be checked.
    /// Each counts `logit.input.checkpoint.errors{op="load"}` and is diagnosed `checkpoint_error`.
    /// A blocking read: it runs once, at bind.
    pub fn load(path: PathBuf, diag: &mut Diagnostics, telemetry: &Telemetry) -> (Self, Loaded) {
        let unusable = match std::fs::read(&path) {
            Ok(bytes) => match parse(&bytes) {
                Ok(resume) => return (Self { path, dirty: false }, Loaded::Resume(resume)),
                Err(reason) => reason,
            },
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                // Not `exists()`, which reads any stat error as "absent": a tmp that can't be
                // checked can't be ruled out as a crash's leftover.
                let tmp = atomic_write::tmp_path(&path);
                match std::fs::symlink_metadata(&tmp) {
                    Err(err) if err.kind() == io::ErrorKind::NotFound => {
                        return (Self { path, dirty: false }, Loaded::Missing);
                    }
                    Ok(_) => format!("missing, but {} exists beside it", tmp.display()),
                    Err(err) => format!("missing, and {} is unreadable: {err}", tmp.display()),
                }
            }
            Err(err) => format!("unreadable: {err}"),
        };
        telemetry.count("logit.input.checkpoint.errors", 1.0, &[("op", "load")]);
        diag.warn_throttled("checkpoint_error", unusable_message(&path, &unusable));
        (Self { path, dirty: false }, Loaded::Unusable)
    }

    pub fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    /// Whether the next unforced [`CheckpointStore::write`] writes.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Replaces the checkpoint with `entries`; a no-op unless dirty or `force`.
    ///
    /// `entries` must be the tailer's tracked files plus its unconsumed [`Source::Checkpoint`]
    /// entries, which is what prunes: a rotated or removed file has left that set, so its entry
    /// isn't rewritten. The document is serialized here, then
    /// [`atomic_write::write_file_durably`] runs on the blocking pool, so the new checkpoint
    /// survives a power loss once this returns. A failed write counts
    /// `logit.input.checkpoint.errors{op="write"}` and leaves the store dirty, so the next tick
    /// retries.
    ///
    /// Resolves to `true` once this call put `entries` in place at the checkpoint path, so a later
    /// load reads them: a durable write, and a rename that landed before a failed directory sync,
    /// which stays dirty so the next tick still makes it durable. A no-op, an encoding error, and
    /// every failure before the rename are `false`.
    ///
    /// Not an `async fn`: `entries` borrows the tailer's files, which aren't `Sync`, so it's
    /// consumed before the returned future is built rather than held across its `.await`.
    pub fn write<'s, 'a>(
        &'s mut self,
        entries: impl Iterator<Item = Persisted<'a>>,
        force: bool,
        diag: &'s mut Diagnostics,
        telemetry: &'s Telemetry,
    ) -> impl Future<Output = bool> + Send + 's {
        let encoded = (self.dirty || force).then(|| {
            let files = entries
                .map(|Persisted { id, path, offset, len, head, marker }| CheckpointEntry {
                    dev: id.dev,
                    ino: id.ino,
                    path: path.to_string_lossy().into_owned(),
                    offset,
                    len,
                    head_len: head.len,
                    head_hash: head.hash,
                    marker,
                })
                .collect();
            serde_json::to_vec_pretty(&CheckpointFile { version: CHECKPOINT_VERSION, files })
        });
        async move {
            let bytes = match encoded {
                None => return false,
                Some(Ok(bytes)) => bytes,
                Some(Err(err)) => {
                    telemetry.count("logit.input.checkpoint.errors", 1.0, &[("op", "write")]);
                    diag.warn_throttled("checkpoint_error", format!("encoding checkpoint: {err}"));
                    return false;
                }
            };
            let path = self.path.clone();
            let result = tokio::task::spawn_blocking(move || {
                atomic_write::write_file_durably(&path, &bytes, sites::TAIL_CHECKPOINT)
            })
            .await;
            let (err, outcome, in_place) = match result {
                Ok(Ok(())) => {
                    self.dirty = false;
                    telemetry.count("logit.input.checkpoint.writes", 1.0, &[]);
                    return true;
                }
                Ok(Err(err)) if err.replaced() => (
                    err.to_string(),
                    "the new checkpoint is in place but may not survive a power loss",
                    true,
                ),
                Ok(Err(err)) => (err.to_string(), "the previous checkpoint stays in effect", false),
                // A panic in the helper; the step it reached is unknown.
                Err(join) => (join.to_string(), "the checkpoint on disk may be old or new", false),
            };
            telemetry.count("logit.input.checkpoint.errors", 1.0, &[("op", "write")]);
            diag.warn_throttled(
                "checkpoint_error",
                format!(
                    "writing checkpoint {}: {err} -- {outcome}; the next tick retries",
                    self.path.display()
                ),
            );
            in_place
        }
    }
}

/// The `checkpoint_error` diagnostic for a checkpoint that loaded as [`Loaded::Unusable`] because
/// of `reason`.
fn unusable_message(path: &Path, reason: &str) -> String {
    format!(
        "checkpoint {} is unusable ({reason}) -- every file present now starts from its \
         beginning, so expect duplicates",
        path.display()
    )
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
                    MetricKind::Sum(sum) => sum.value.as_f64(),
                    other => panic!("{name} must be a counter, got {other:?}"),
                })
                .sum()
        }
    }

    fn resume_of(loaded: Loaded) -> HashMap<FileId, Retained> {
        match loaded {
            Loaded::Resume(resume) => resume,
            other => panic!("expected a usable checkpoint, got {other:?}"),
        }
    }

    async fn write_one(store: &mut CheckpointStore, obs: &mut Observed, id: FileId, offset: u64) {
        let file = PathBuf::from("/var/log/app.log");
        store.mark_dirty();
        store
            .write([at(id, &file, offset)].into_iter(), false, &mut obs.diag, &obs.telemetry)
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
    const HEAD: Head = Head { len: 9, hash: 0x0123_4567_89ab_cdef };
    const MARKER: u64 = 0xfedc_ba98_7654_3210;
    const STEPS: [(Step, Op); 4] = [
        (Step::Write, Op::Write),
        (Step::SyncFile, Op::SyncFile),
        (Step::Rename, Op::Rename),
        (Step::SyncDir, Op::SyncDir),
    ];

    fn point(op: Op) -> Point {
        Point::new(fault::sites::TAIL_CHECKPOINT, op)
    }

    /// An entry for `id` at `offset`, with `len` a fixed 100 bytes past it, [`HEAD`], and
    /// [`MARKER`].
    fn at(id: FileId, path: &Path, offset: u64) -> Persisted<'_> {
        Persisted { id, path, offset, len: offset + 100, head: HEAD, marker: MARKER }
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
        std::fs::write(&path, br#"{"version":4,"files":[{"dev":1,"ino":42,"pa"#).unwrap();
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
        std::fs::write(tmp_path(&path), br#"{"version":4,"files":[]}"#).unwrap();
        assert_unusable(&path);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A tmp file that can't be checked might be a crash's leftover, so it can't be ruled out as
    /// one. A permission error needs a non-root test; `ENAMETOOLONG` doesn't: a 252-byte name is
    /// missing (`ENOENT`), and its 256-byte tmp name fails `lstat` outright.
    #[test]
    fn an_unreadable_tmp_beside_a_missing_checkpoint_is_unusable() {
        let dir = scratch_dir("checkpoint-tmp-unstattable");
        let path = dir.join("c".repeat(252));
        assert_eq!(
            std::fs::read(&path).unwrap_err().kind(),
            io::ErrorKind::NotFound,
            "the checkpoint itself must read as missing"
        );
        let stat = std::fs::symlink_metadata(tmp_path(&path)).unwrap_err();
        assert_ne!(stat.kind(), io::ErrorKind::NotFound, "the tmp must fail to stat: {stat}");
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
        assert_eq!(resume_of(obs.load(&json).1)[&ID].offset, 1);
        assert_eq!(resume_of(obs.load(&yaml).1)[&ID].offset, 2);
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
            assert_eq!(resume_of(obs.load(&path).1)[&ID].offset, expected, "{step:?}");

            // The next tick needs no new data to retry: the store is still dirty.
            let file = PathBuf::from("/var/log/app.log");
            store
                .write([at(ID, &file, 30)].into_iter(), false, &mut obs.diag, &obs.telemetry)
                .await;
            drop(scope);
            assert!(!store.dirty, "{step:?}");
            assert_eq!(resume_of(obs.load(&path).1)[&ID].offset, 30, "{step:?}");
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
            assert_eq!(resume_of(obs.load(&path).1)[&ID].offset, expected, "{step:?}");
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
        let written = Persisted {
            id: ID,
            path: &file_path,
            offset: 123,
            len: 456,
            head: HEAD,
            marker: MARKER,
        };
        store.write([written].into_iter(), false, &mut obs.diag, &obs.telemetry).await;

        let resume = resume_of(obs.load(&path).1);
        let expected = Retained {
            path: file_path,
            offset: 123,
            len: 456,
            head: HEAD,
            marker: MARKER,
            source: Source::Checkpoint,
        };
        assert_eq!(resume.get(&ID), Some(&expected));
        assert!(!tmp_path(&path).exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An entry loaded and written back unspent, as every write until it's pruned does, keeps the
    /// length it was loaded with rather than one derived from its offset.
    #[tokio::test]
    async fn an_unspent_entry_keeps_its_len_through_a_rewrite() {
        let dir = scratch_dir("checkpoint-len-rewrite");
        let path = dir.join("checkpoint.json");
        std::fs::write(
            &path,
            br#"{"version":4,"files":[{"dev":1,"ino":42,"path":"/var/log/app.log","offset":4,"len":90,"head_len":4,"head_hash":7,"marker":9}]}"#,
        )
        .unwrap();
        let mut obs = observed();
        let (mut store, loaded) = obs.load(&path);
        let loaded = resume_of(loaded).remove(&ID).expect("the entry loads");
        assert_eq!((loaded.offset, loaded.len), (4, 90));

        let rewritten = Persisted {
            id: ID,
            path: &loaded.path,
            offset: loaded.offset,
            len: loaded.len,
            head: loaded.head,
            marker: loaded.marker,
        };
        store.write([rewritten].into_iter(), true, &mut obs.diag, &obs.telemetry).await;
        assert_eq!(resume_of(obs.load(&path).1).remove(&ID), Some(loaded));
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
        store.write([at(id, &file_path, 5)].into_iter(), true, &mut obs.diag, &obs.telemetry).await;

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
                [at(a, &a_path, 10), at(b, &b_path, 20)].into_iter(),
                false,
                &mut obs.diag,
                &obs.telemetry,
            )
            .await;
        store.mark_dirty();
        store.write([at(a, &a_path, 15)].into_iter(), false, &mut obs.diag, &obs.telemetry).await;

        let resume = resume_of(obs.load(&path).1);
        assert_eq!(resume.get(&a).map(|r| (&r.path, r.offset)), Some((&a_path, 15)));
        assert_eq!(resume.get(&b), None, "b should have been pruned");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Format 1 had no head fields. Probing the version first reports that, rather than
    /// "missing field `head_len`".
    #[test]
    fn load_reports_a_v1_checkpoint_as_an_unsupported_version_not_malformed() {
        let v1 =
            br#"{"version":1,"files":[{"dev":1,"ino":42,"path":"/var/log/app.log","offset":4}]}"#;
        assert_eq!(parse(v1).unwrap_err(), "unsupported version 1");

        let dir = scratch_dir("checkpoint-v1");
        let path = dir.join("checkpoint.json");
        std::fs::write(&path, v1).unwrap();
        assert_unusable(&path);
        let message = unusable_message(&path, "unsupported version 1");
        assert!(message.contains("(unsupported version 1)"), "{message}");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Format 2 had no `len`. A full format 2 document is another version, not a malformed one,
    /// so it takes the counted, diagnosed unusable path and replays once.
    #[test]
    fn a_v2_checkpoint_is_an_unsupported_version() {
        let v2 = br#"{"version":2,"files":[{"dev":1,"ino":42,"path":"/var/log/app.log","offset":4,"head_len":4,"head_hash":7}]}"#;
        assert_eq!(parse(v2).unwrap_err(), "unsupported version 2");

        let dir = scratch_dir("checkpoint-v2");
        let path = dir.join("checkpoint.json");
        std::fs::write(&path, v2).unwrap();
        assert_unusable(&path);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Format 3 had no `marker`. Its offsets can't be checked against the bytes before them, so it
    /// takes the counted, diagnosed unusable path like any other version.
    #[test]
    fn a_v3_checkpoint_is_an_unsupported_version() {
        let v3 = br#"{"version":3,"files":[{"dev":1,"ino":42,"path":"/var/log/app.log","offset":4,"len":4,"head_len":4,"head_hash":7}]}"#;
        assert_eq!(parse(v3).unwrap_err(), "unsupported version 3");

        let dir = scratch_dir("checkpoint-v3");
        let path = dir.join("checkpoint.json");
        std::fs::write(&path, v3).unwrap();
        assert_unusable(&path);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_checkpoint_head_len_over_head_bytes_is_unusable() {
        let over = format!(
            r#"{{"version":4,"files":[{{"dev":1,"ino":42,"path":"a","offset":4,"len":4,"head_len":{},"head_hash":0,"marker":0}}]}}"#,
            HEAD_BYTES + 1
        );
        let reason = parse(over.as_bytes()).unwrap_err();
        assert!(reason.starts_with("malformed: head_len 257"), "{reason}");

        let at = over.replace("257", "256");
        assert_eq!(resume_of(Loaded::Resume(parse(at.as_bytes()).unwrap()))[&ID].head.len, 256);

        let dir = scratch_dir("checkpoint-head-len");
        let path = dir.join("checkpoint.json");
        std::fs::write(&path, over).unwrap();
        assert_unusable(&path);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_v4_entry_missing_its_len_head_or_marker_is_malformed() {
        let no_len = br#"{"version":4,"files":[{"dev":1,"ino":42,"path":"a","offset":4,"head_len":4,"head_hash":7,"marker":9}]}"#;
        let reason = parse(no_len).unwrap_err();
        assert!(reason.starts_with("malformed:") && reason.contains("`len`"), "{reason}");

        let no_head =
            br#"{"version":4,"files":[{"dev":1,"ino":42,"path":"a","offset":4,"len":4,"marker":9}]}"#;
        let reason = parse(no_head).unwrap_err();
        assert!(reason.starts_with("malformed:") && reason.contains("head_len"), "{reason}");

        let no_marker = br#"{"version":4,"files":[{"dev":1,"ino":42,"path":"a","offset":4,"len":4,"head_len":4,"head_hash":7}]}"#;
        let reason = parse(no_marker).unwrap_err();
        assert!(reason.starts_with("malformed:") && reason.contains("`marker`"), "{reason}");
    }

    #[test]
    fn a_head_matches_only_a_prefix_that_starts_with_its_bytes() {
        let head = Head::of(b"line one\n");
        assert_eq!(head.len, 9);
        assert!(head.matches(b"line one\n"));
        assert!(head.matches(b"line one\nline two\n"), "an append keeps the head");
        assert!(!head.matches(b"line one"), "a shorter file never matches");
        assert!(!head.matches(b"LINE one\nline two\n"), "rewritten bytes don't match");
        assert!(Head::of(b"").matches(b""), "an empty head matches any file");
        assert!(Head::of(b"").matches(b"anything"));
    }

    /// Both hashes are part of format 4: XXH64 with seed 0. A changed hash would make every
    /// checkpoint written by an earlier build reject its resumes.
    #[test]
    fn the_head_hash_and_the_marker_are_xxh64_with_seed_zero() {
        assert_eq!(Head::of(b"").hash, 0xef46_db37_51d8_e999);
        assert_eq!(Head::of(b"abc").hash, 0x44bc_2cf5_ad77_0999);
        assert_eq!(marker(b""), 0xef46_db37_51d8_e999);
        assert_eq!(marker(b"abc"), 0x44bc_2cf5_ad77_0999);
    }
}
