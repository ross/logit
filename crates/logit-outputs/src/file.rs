//! The rotating half of `crate::stdio::StreamOutput`'s file target: size- and/or calendar-
//! interval-triggered rotation with logrotate-style numbered-suffix retention
//! (`docs/adr/rotating-file-output.md`). `stdio_out`'s plain file target and `file_out`'s rotating
//! one are both a [`FileTarget`], differing only in [`RotatePolicy`] -- see `crate::stdio`'s
//! module doc comment.
//!
//! [`RotatePolicy`]/[`RotateInterval`] are local mirrors of `logit_config::RotateConfig`/
//! `RotateInterval` -- `logit-outputs` must not depend on `logit-config`
//! (`docs/design/pipeline-graph.md`'s crate layout), the same reason `crate::syslog::Format`
//! mirrors `logit_config::SyslogFormat`; `crates/logit-cli/src/pipeline.rs::build_spec` is the
//! sole place a config value crosses into this type.

use anyhow::Context;
use logit_core::Diagnostics;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

/// Which calendar boundary [`RotationState::should_rotate`] rotates on. Calendar periods, not a
/// `Duration`: a duration measured from an arbitrary start (process start, first write) drifts
/// against the wall clock, which is the opposite of what a daily log file is for. UTC only, never
/// the host's local zone -- matching the reasoning already recorded at
/// `docs/known-gaps.md`'s syslog-timestamp-resolution entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RotateInterval {
    Hourly,
    Daily,
}

impl RotateInterval {
    fn period_seconds(self) -> i64 {
        match self {
            RotateInterval::Hourly => 3_600,
            RotateInterval::Daily => 86_400,
        }
    }
}

/// A file target's rotation policy. Both triggers can be set together -- either firing rotates.
/// `max_files` counts *every* file the target maintains, active plus rotated, so
/// `max_files * max_bytes` reads as a disk budget an operator can compute directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RotatePolicy {
    pub max_bytes: Option<u64>,
    pub interval: Option<RotateInterval>,
    pub max_files: u32,
}

impl RotatePolicy {
    /// No rotation at all -- what `stdio_out`'s plain file target uses.
    /// [`RotationState::should_rotate`] always returns `false` under this policy, regardless of
    /// `now_unix`/`incoming`. `max_files: 1` is never consulted (nothing can trigger a rotation to
    /// retain), but kept at a shape that would be safe if it ever were.
    pub fn never() -> Self {
        Self { max_bytes: None, interval: None, max_files: 1 }
    }
}

/// Current Unix time in whole seconds, UTC -- the clock [`crate::stdio::StreamOutput::send`]
/// passes into [`RotationState::should_rotate`]/[`RotationState::note_written`]. A free function
/// rather than a method so a test can call those directly with an arbitrary injected `now_unix`
/// instead of racing the real clock, mirroring `logit_pipeline::Transform::flush(now)`'s
/// injected-clock precedent.
pub(crate) fn now_unix() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

/// The rotation bookkeeping for one active file, with **no file handle or path of its own** --
/// deliberately separated from [`FileTarget`] so every rotation-trigger decision is a plain
/// synchronous unit test against this struct alone, with no real file or clock involved. See
/// [`RotationState::should_rotate`]'s own doc comment for the trigger semantics.
#[derive(Debug, Clone, Copy)]
struct RotationState {
    /// Bytes written to the active file so far. Seeded from the file's length at open, not 0 --
    /// restarting against an already-large file must not get another full `max_bytes` for free.
    written: u64,
    /// The calendar period (`RotateInterval::period_seconds`-sized bucket of `now_unix`) the
    /// active file belongs to. `None` until the first write after open/rotation, so the very
    /// first batch into a freshly opened file never spuriously rotates just because no period was
    /// known yet -- see [`RotationState::should_rotate`].
    period: Option<i64>,
    policy: RotatePolicy,
}

impl RotationState {
    fn new(written: u64, policy: RotatePolicy) -> Self {
        Self { written, period: None, policy }
    }

    fn reset(&mut self) {
        self.written = 0;
        self.period = None;
    }

    /// True when either:
    /// - `interval` is set and `now_unix` falls in a different calendar period than the one the
    ///   active file was last written in, or
    /// - `max_bytes` is set and this write would cross it.
    ///
    /// **`max_bytes` is a threshold, not a hard cap.** A single batch larger than `max_bytes` is
    /// written whole into its own file rather than split -- tearing an event block across two
    /// files would produce a file no reader can parse, which is strictly worse than one oversized
    /// file. The `self.written > 0` guard is what makes that work: an empty active file never
    /// rotates before its first (possibly oversized) batch lands.
    ///
    /// **Time rotation is write-triggered, not boundary-triggered.** `Output` gets no periodic
    /// tick (`write_loop`'s `select!` has only a queue-peek arm and a shutdown-grace arm,
    /// `crates/logit-pipeline/src/runtime.rs`), so an idle target rolls on its *next* write after
    /// the boundary, not at it. The rolled file still holds exactly the previous period's events,
    /// so this delays when a file appears, not what ends up in it.
    fn should_rotate(&self, now_unix: i64, incoming: usize) -> bool {
        if let Some(period) = self.period {
            if self.period_for(now_unix) != period {
                return true;
            }
        }
        if let Some(max_bytes) = self.policy.max_bytes {
            if self.written > 0 && self.written + incoming as u64 > max_bytes {
                return true;
            }
        }
        false
    }

    /// Records that `len` bytes were just written at `now_unix` -- called once per `send`, after
    /// any rotation `should_rotate` triggered has already happened. Sets `period` on the first
    /// call after open/rotation (see the field's own doc comment) rather than at open time itself,
    /// since opening a file has no clock of its own to call.
    fn note_written(&mut self, now_unix: i64, len: usize) {
        if self.policy.interval.is_some() && self.period.is_none() {
            self.period = Some(self.period_for(now_unix));
        }
        self.written += len as u64;
    }

    fn period_for(&self, now_unix: i64) -> i64 {
        match self.policy.interval {
            Some(interval) => now_unix.div_euclid(interval.period_seconds()),
            None => 0,
        }
    }
}

/// The open file `Target::File` writes to (`crate::stdio`), plus everything needed to decide when
/// and how to rotate it. Opened eagerly via [`FileTarget::open`], exactly like `stdio_out`'s
/// previous standalone `open_path` did -- called from `build_spec` at config-build time, so a bad
/// path or a permissions error is a config error that fails before anything starts listening.
#[derive(Debug)]
pub struct FileTarget {
    path: PathBuf,
    file: tokio::fs::File,
    state: RotationState,
}

impl FileTarget {
    pub fn open(path: impl AsRef<Path>, policy: RotatePolicy) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let std_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("opening file target {}", path.display()))?;
        let written = std_file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            path: path.to_path_buf(),
            file: tokio::fs::File::from_std(std_file),
            state: RotationState::new(written, policy),
        })
    }

    pub fn file_mut(&mut self) -> &mut tokio::fs::File {
        &mut self.file
    }

    /// See [`RotationState::should_rotate`].
    pub fn should_rotate(&self, now_unix: i64, incoming: usize) -> bool {
        self.state.should_rotate(now_unix, incoming)
    }

    /// See [`RotationState::note_written`].
    pub fn note_written(&mut self, now_unix: i64, len: usize) {
        self.state.note_written(now_unix, len);
    }

    fn rotated_path(&self, n: u32) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(format!(".{n}"));
        PathBuf::from(name)
    }

    /// Rotates the active file: cascades retained files up one suffix (logrotate's own numbered
    /// scheme, `.1` always the most recent), then starts a fresh active file. Failure policy,
    /// deliberately asymmetric:
    ///
    /// | Failure | Handling |
    /// |---|---|
    /// | Flushing the active file before rotating | Fatal -- bubbles as `Err` |
    /// | Deleting/renaming a *retained* file (the cascade) | `retention_failure`, continue |
    /// | Renaming the *active* file to `.1` | `rotate_failure`, keep writing the current file |
    /// | Re-opening `path` after a successful rename | Fatal -- there is no file left to write to |
    ///
    /// A retention-cascade failure only risks losing history, not correctness, so it's reported
    /// and skipped. A failure renaming the active file itself means rotation didn't happen at
    /// all -- the safest response is to keep the existing, already-flushed handle open and try
    /// again on the next write (throttled diagnostics bound how often this actually reports), not
    /// to error the whole sink over what might be a transient permissions issue.
    pub async fn rotate(&mut self, diag: &mut Diagnostics) -> anyhow::Result<()> {
        self.file
            .flush()
            .await
            .with_context(|| format!("flushing {} before rotation", self.path.display()))?;

        let max_files = self.state.policy.max_files.max(1);
        if max_files == 1 {
            // No room to keep any rotated file -- "rotating" a single-file policy means starting
            // a fresh, empty file in place rather than ever creating a `.1`.
            return self.reopen(true).await;
        }

        // Retention cascade, oldest first so no rename ever overwrites a file not yet moved:
        // drop the file that would fall off the end, then shift every remaining rotated file up
        // by one suffix.
        let oldest = self.rotated_path(max_files - 1);
        if oldest.exists() {
            if let Err(e) = std::fs::remove_file(&oldest) {
                diag.warn_throttled(
                    "retention_failure",
                    format_args!("removing {}: {e}", oldest.display()),
                );
            }
        }
        for n in (1..max_files - 1).rev() {
            let from = self.rotated_path(n);
            if from.exists() {
                let to = self.rotated_path(n + 1);
                if let Err(e) = std::fs::rename(&from, &to) {
                    diag.warn_throttled(
                        "retention_failure",
                        format_args!("renaming {} to {}: {e}", from.display(), to.display()),
                    );
                }
            }
        }

        let rotated = self.rotated_path(1);
        if let Err(e) = std::fs::rename(&self.path, &rotated) {
            diag.warn_throttled(
                "rotate_failure",
                format_args!("renaming {} to {}: {e}", self.path.display(), rotated.display()),
            );
            return Ok(());
        }

        self.reopen(false).await
    }

    /// Re-opens `self.path` fresh after a rotation -- `truncate` for the `max_files == 1` case
    /// (there is no `.1` to rename into, so the old content is simply discarded), append
    /// otherwise (the normal case: `path` was just renamed away, so a fresh `create` starts empty
    /// regardless). Failure here is fatal: there is no file left to write to.
    async fn reopen(&mut self, truncate: bool) -> anyhow::Result<()> {
        let mut opts = std::fs::OpenOptions::new();
        opts.create(true);
        if truncate {
            opts.write(true).truncate(true);
        } else {
            opts.append(true);
        }
        let std_file = opts
            .open(&self.path)
            .with_context(|| format!("re-opening {} after rotation", self.path.display()))?;
        self.file = tokio::fs::File::from_std(std_file);
        self.state.reset();
        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    // This crate has no `tempfile` dependency (`docs/adr/file-tailing-and-docker-json-logs.md`'s
    // Alternatives), so tests build and tear down their own unique scratch directories by hand,
    // following `crates/logit-cli/src/pipeline.rs`'s own `std::env::temp_dir()`-based precedent
    // (`crates/logit-inputs/src/tail/mod.rs::test_support::scratch_dir` is the same helper).
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    pub(crate) fn scratch_dir(label: &str) -> PathBuf {
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir()
            .join(format!("logit-file-out-test-{label}-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create scratch dir");
        dir
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::scratch_dir;
    use super::*;
    use logit_core::Registry;

    fn diag() -> Diagnostics {
        Diagnostics::new("test")
    }

    fn size_policy(max_bytes: u64) -> RotatePolicy {
        RotatePolicy { max_bytes: Some(max_bytes), interval: None, max_files: 5 }
    }

    fn interval_policy(interval: RotateInterval) -> RotatePolicy {
        RotatePolicy { max_bytes: None, interval: Some(interval), max_files: 5 }
    }

    // ---------------------------------------------------------------------------------------
    // RotationState::should_rotate -- pure, synchronous, no real files or clocks involved.
    // ---------------------------------------------------------------------------------------

    #[test]
    fn never_policy_never_rotates_for_any_input() {
        let mut state = RotationState::new(u64::MAX / 2, RotatePolicy::never());
        state.note_written(0, 0);
        assert!(!state.should_rotate(0, 0));
        assert!(!state.should_rotate(i64::MAX, usize::MAX));
    }

    #[test]
    fn crossing_max_bytes_rotates_and_staying_under_does_not() {
        let mut state = RotationState::new(0, size_policy(100));
        assert!(!state.should_rotate(0, 50), "50 more on top of 0 written should not rotate");
        state.note_written(0, 90);
        assert!(!state.should_rotate(0, 10), "exactly at the limit should not rotate");
        assert!(state.should_rotate(0, 11), "one byte over the limit should rotate");
    }

    #[test]
    fn an_empty_active_file_never_rotates_on_size_even_for_an_oversized_batch() {
        let state = RotationState::new(0, size_policy(10));
        assert!(!state.should_rotate(0, 1_000_000), "written == 0 must never trigger on size");
    }

    #[test]
    fn a_now_in_the_next_hour_rotates_under_hourly_and_the_same_hour_does_not() {
        let mut state = RotationState::new(0, interval_policy(RotateInterval::Hourly));
        state.note_written(0, 10); // period 0
        assert!(!state.should_rotate(3_599, 10), "still hour 0");
        assert!(state.should_rotate(3_600, 10), "now in hour 1");
    }

    #[test]
    fn a_now_in_the_next_day_rotates_under_daily_and_the_same_day_does_not() {
        let mut state = RotationState::new(0, interval_policy(RotateInterval::Daily));
        state.note_written(0, 10); // period 0
        assert!(!state.should_rotate(86_399, 10), "still day 0");
        assert!(state.should_rotate(86_400, 10), "now in day 1");
    }

    #[test]
    fn the_first_write_ever_never_rotates_even_under_an_interval_policy() {
        // `period` starts `None` -- there is nothing to compare `now_unix`'s period against yet.
        let state = RotationState::new(0, interval_policy(RotateInterval::Daily));
        assert!(!state.should_rotate(0, 10));
        assert!(!state.should_rotate(999_999_999, 10));
    }

    #[test]
    fn interval_and_max_bytes_together_either_alone_fires() {
        let policy = RotatePolicy {
            max_bytes: Some(100),
            interval: Some(RotateInterval::Daily),
            max_files: 5,
        };
        let mut state = RotationState::new(0, policy);
        state.note_written(0, 50); // sets period to 0, written to 50
        assert!(!state.should_rotate(0, 10), "neither trigger should fire");
        assert!(state.should_rotate(86_400, 10), "interval alone should fire");
        assert!(state.should_rotate(0, 51), "max_bytes alone should fire");
    }

    #[test]
    fn reset_clears_written_and_period_back_to_their_open_state() {
        let mut state = RotationState::new(0, interval_policy(RotateInterval::Daily));
        state.note_written(0, 100);
        assert!(state.period.is_some());
        state.reset();
        assert_eq!(state.written, 0);
        assert_eq!(state.period, None);
    }

    // ---------------------------------------------------------------------------------------
    // rotate / reopen -- real files, real filesystem state.
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_size_rotation_moves_the_old_content_to_dot_1_and_starts_fresh() {
        let dir = scratch_dir("size-rotation");
        let path = dir.join("events.log");
        let mut target = FileTarget::open(&path, size_policy(10)).expect("open");
        target.file_mut().write_all(b"0123456789").await.unwrap();
        target.note_written(0, 10);

        assert!(target.should_rotate(0, 1));
        target.rotate(&mut diag()).await.expect("rotate should succeed");
        target.file_mut().write_all(b"new").await.unwrap();
        target.note_written(0, 3);
        target.file_mut().flush().await.unwrap();

        let rotated = std::fs::read_to_string(dir.join("events.log.1")).unwrap();
        assert_eq!(rotated, "0123456789");
        let active = std::fs::read_to_string(&path).unwrap();
        assert_eq!(active, "new");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_batch_larger_than_max_bytes_lands_whole_in_one_file_never_split() {
        // should_rotate is checked *before* the write in `StreamOutput::send`, not enforced here
        // -- this pins that a target itself never splits a write; the sink is what decides not to
        // call `rotate` mid-write.
        let dir = scratch_dir("oversized-batch");
        let path = dir.join("events.log");
        let mut target = FileTarget::open(&path, size_policy(10)).expect("open");
        let big = vec![b'x'; 1000];
        target.file_mut().write_all(&big).await.unwrap();
        target.note_written(0, big.len());
        target.file_mut().flush().await.unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents.len(), 1000, "the whole oversized batch landed in one file");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn reopening_an_existing_file_seeds_written_from_its_length() {
        let dir = scratch_dir("seed-written");
        let path = dir.join("events.log");
        std::fs::write(&path, b"0123456789").unwrap(); // 10 bytes already on disk

        let target = FileTarget::open(&path, size_policy(15)).expect("open");
        assert_eq!(
            target.state.written, 10,
            "written should be seeded from the existing file's length"
        );
        assert!(
            target.should_rotate(0, 6),
            "10 existing + 6 incoming crosses a 15-byte cap on the very next write"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn max_files_three_keeps_exactly_the_active_file_and_two_rotated_ones() {
        let dir = scratch_dir("max-files");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 3 };
        let mut target = FileTarget::open(&path, policy).expect("open");

        for label in [b"a" as &[u8], b"b", b"c"] {
            target.file_mut().write_all(label).await.unwrap();
            target.note_written(0, label.len());
            target.rotate(&mut diag()).await.expect("rotate should succeed");
        }
        target.file_mut().write_all(b"d").await.unwrap();
        target.file_mut().flush().await.unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "d");
        assert_eq!(std::fs::read_to_string(dir.join("events.log.1")).unwrap(), "c");
        assert_eq!(std::fs::read_to_string(dir.join("events.log.2")).unwrap(), "b");
        assert!(!dir.join("events.log.3").exists(), "max_files: 3 must not keep a 4th file");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn max_files_one_truncates_in_place_rather_than_ever_creating_a_dot_1() {
        let dir = scratch_dir("max-files-one");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 1 };
        let mut target = FileTarget::open(&path, policy).expect("open");
        target.file_mut().write_all(b"old").await.unwrap();
        target.note_written(0, 3);

        target.rotate(&mut diag()).await.expect("rotate should succeed");
        target.file_mut().write_all(b"new").await.unwrap();
        target.file_mut().flush().await.unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
        assert!(!dir.join("events.log.1").exists(), "max_files: 1 must never create a .1");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_clean_rotation_reports_no_diagnostics() {
        // `FileTarget` doesn't hold a `Telemetry` handle of its own -- `logit.output.file.
        // rotations` is `StreamOutput`'s to count (exercised in `stdio.rs`'s own tests). This
        // just pins that a clean rotation reports nothing via `Diagnostics`.
        let dir = scratch_dir("clean-rotate");
        let path = dir.join("events.log");
        let mut target = FileTarget::open(&path, size_policy(1)).expect("open");
        target.file_mut().write_all(b"x").await.unwrap();
        target.note_written(0, 1);

        let registry = Registry::new();
        let mut diag =
            Diagnostics::new("f").with_telemetry(registry.telemetry_for("f", "file_out", "sink"));
        target.rotate(&mut diag).await.expect("rotate should succeed");
        assert!(registry.drain(0).is_empty(), "a clean rotation should report no diagnostics");
        std::fs::remove_dir_all(&dir).ok();
    }
}
