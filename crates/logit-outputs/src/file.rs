//! `file_out`: the rotating half of `crate::stdio::StreamOutput`'s file target
//! (`docs/adr/rotating-file-output.md`). `stdio_out`'s file target and `file_out` are both a
//! [`FileTarget`], differing only in [`RotatePolicy`].
//!
//! Rotation fires on size, on a UTC calendar boundary, or both, and is checked before each batch's
//! write. Retention is logrotate-style: the active file is `path`, rotated files are `path.1`
//! (newest) through `path.{max_files - 1}`, and the oldest is removed once it would fall off the
//! end. `max_files: 1` truncates in place. There is no `fsync`; each batch is flushed to the OS.
//! [`FileTarget::rotate`] has the crash-safety order and the per-failure handling.
//!
//! [`RotatePolicy`]/[`RotateInterval`] mirror `logit_config`'s types because `logit-outputs` must
//! not depend on `logit-config` (`docs/design/pipeline-graph.md`'s "Crate layout");
//! `build_spec` is the one place a config value crosses into them.

use anyhow::Context;
use logit_core::Diagnostics;
use logit_pipeline::fault::{sites, Op, Point};
use logit_pipeline::{fault_io, Fault};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::AsyncWriteExt;

/// Which UTC calendar boundary [`RotationState::should_rotate`] rotates on. Not a `Duration`: one
/// measured from process start or the first write drifts against the wall clock. Never the host's
/// local zone.
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

/// A file target's rotation policy; either trigger firing rotates. `max_files` counts the active
/// file too, so `max_files * max_bytes` is the disk budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RotatePolicy {
    pub max_bytes: Option<u64>,
    pub interval: Option<RotateInterval>,
    pub max_files: u32,
}

impl RotatePolicy {
    /// No rotation: `stdio_out`'s file target. `max_files: 1` is never consulted.
    pub fn never() -> Self {
        Self { max_bytes: None, interval: None, max_files: 1 }
    }
}

/// `t` as whole Unix seconds, or `None` if it predates the epoch. Shared by [`now_unix`] and
/// [`FileTarget::open`]'s mtime read.
fn unix_seconds(t: SystemTime) -> Option<i64> {
    t.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs() as i64)
}

/// Current Unix time in whole seconds, the clock `StreamOutput::send` passes to
/// [`RotationState::should_rotate`]/[`RotationState::note_written`]. Read by the caller, not
/// inside them, so a test can inject any `now_unix`.
pub(crate) fn now_unix() -> i64 {
    unix_seconds(SystemTime::now()).unwrap_or_default()
}

/// Rotation bookkeeping for the active file, with no handle or path, so every trigger decision
/// is a synchronous unit test with no real file or clock.
#[derive(Debug, Clone, Copy)]
struct RotationState {
    /// Bytes in the active file. Seeded from its length at open, so a restart against a large
    /// file doesn't get another full `max_bytes`.
    written: u64,
    /// The calendar period (`now_unix / period_seconds`) the active file belongs to. `None` until
    /// [`RotationState::seed_period`] or the first write sets it, so an empty file's first batch
    /// never rotates.
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

    /// Seeds `period` from an existing file's mtime at [`FileTarget::open`], as `written` is
    /// seeded from its length. Without it, a restart under an `interval` policy would merge two
    /// periods into one file, or miss a boundary crossed while the process was down. Skipped for
    /// an empty file, which has no previous period to protect.
    fn seed_period(&mut self, mtime_unix: i64) {
        if self.policy.interval.is_some() && self.written > 0 {
            self.period = Some(self.period_for(mtime_unix));
        }
    }

    /// True when either:
    /// - `interval` is set and `now_unix` falls in a different calendar period than the one the
    ///   active file was last written in, or
    /// - `max_bytes` is set and this write would cross it.
    ///
    /// **`max_bytes` is a threshold, not a hard cap.** A batch larger than `max_bytes` is written
    /// whole into its own file, since a block torn across two files is unparseable. The
    /// `self.written > 0` guard is what allows that: an empty file never rotates before its first
    /// batch.
    ///
    /// **Time rotation is write-triggered.** An `Output` gets no periodic tick, so an idle target
    /// rolls on its first write after the boundary. The rolled file still holds only the previous
    /// period's events; only when it appears is delayed.
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

    /// Records `len` bytes written at `now_unix`, once per `send` after any rotation. Sets
    /// `period` if unset: the first write to a new or just-rotated file.
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

/// The result of [`FileTarget::rotate`]. `NotRotated`: the active file's rename (or, under
/// `max_files: 1`, its truncate) failed, nothing on disk changed, and the target keeps writing to
/// its open handle. Not an error, but never counted in `logit.output.file.rotations`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum RotateOutcome {
    Rotated,
    NotRotated,
}

// Every filesystem mutation below is preceded by `fault::check` at one of these points
// (`docs/adr/durable-checkpoint-writes-and-fault-injection.md`, decision 8). A retained file's
// `arg` is its generation `N`; every other point passes 0.
const ACTIVE_OPEN: Point = Point::new(sites::FILE_OUT_ACTIVE, Op::Open);
const ACTIVE_TRUNCATE: Point = Point::new(sites::FILE_OUT_ACTIVE, Op::SetLen);
const ACTIVE_WRITE: Point = Point::new(sites::FILE_OUT_ACTIVE, Op::Write);
const ACTIVE_FLUSH: Point = Point::new(sites::FILE_OUT_ACTIVE, Op::Flush);
const ACTIVE_RENAME: Point = Point::new(sites::FILE_OUT_ACTIVE, Op::Rename);
const STAGING_RENAME: Point = Point::new(sites::FILE_OUT_STAGING, Op::Rename);
const RETAINED_RENAME: Point = Point::new(sites::FILE_OUT_RETAINED, Op::Rename);
const RETAINED_UNLINK: Point = Point::new(sites::FILE_OUT_RETAINED, Op::Unlink);

/// Opens the active file at `path`, creating it if needed. `truncate: true` is `max_files: 1`'s
/// in-place rotation; otherwise it appends.
fn open_active(path: &Path, truncate: bool) -> std::io::Result<std::fs::File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.create(true);
    if truncate {
        opts.write(true).truncate(true);
    } else {
        opts.append(true);
    }
    opts.open(path)
}

/// The open file `Target::File` writes to, plus its rotation state. Opened eagerly at
/// config-build time, so a bad path or permissions error fails startup.
#[derive(Debug)]
pub struct FileTarget {
    path: PathBuf,
    /// The handle to `path`. `None` only between a committed rotation rename and a successful
    /// re-open; never a handle to a rotated-away file. [`FileTarget::write_all`] re-opens it.
    file: Option<tokio::fs::File>,
    state: RotationState,
}

impl FileTarget {
    pub fn open(path: impl AsRef<Path>, policy: RotatePolicy) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let std_file = fault_io!(ACTIVE_OPEN, path, 0, open_active(path, false))
            .with_context(|| format!("opening file target {}", path.display()))?;
        let metadata = std_file.metadata().ok();
        let written = metadata.as_ref().map(|m| m.len()).unwrap_or(0);
        let mtime_unix = metadata.and_then(|m| m.modified().ok()).and_then(unix_seconds);

        let mut state = RotationState::new(written, policy);
        if let Some(mtime_unix) = mtime_unix {
            state.seed_period(mtime_unix);
        }

        Ok(Self {
            path: path.to_path_buf(),
            file: Some(tokio::fs::File::from_std(std_file)),
            state,
        })
    }

    /// Re-opens the active file if `self.file` is `None`, so a write after a failed post-rotation
    /// re-open heals itself. A failure is `Fault::Clean` for the reason [`FileTarget::rotate`]
    /// gives.
    fn ensure_open(&mut self) -> anyhow::Result<()> {
        if self.file.is_none() {
            let std_file = fault_io!(ACTIVE_OPEN, &self.path, 0, open_active(&self.path, false))
                .with_context(|| format!("re-opening {} for write", self.path.display()))
                .context(Fault::Clean)?;
            self.file = Some(tokio::fs::File::from_std(std_file));
        }
        Ok(())
    }

    /// Writes `bytes` to the active file, re-opening it first if a rotation's re-open failed.
    pub async fn write_all(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.ensure_open()?;
        let file = self.file.as_mut().expect("ensure_open leaves a handle");
        fault_io!(ACTIVE_WRITE, &self.path, 0, file.write_all(bytes).await)?;
        Ok(())
    }

    /// Flushes the active file to the OS (no `fsync`); a no-op with no open handle.
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(file) = self.file.as_mut() {
            fault_io!(ACTIVE_FLUSH, &self.path, 0, file.flush().await)?;
        }
        Ok(())
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

    /// `path.rotating`: holds the rotated file between [`FileTarget::rotate`]'s commit-point
    /// rename and [`FileTarget::promote_staged`]. An orphan from a kill in that window is promoted
    /// on the next rotation. `tail_in`'s anchored wildcard never matches it, as with `.1`
    /// (`docs/adr/file-tailing-and-docker-json-logs.md`, "Rotation and truncation").
    fn staging_path(&self) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(".rotating");
        PathBuf::from(name)
    }

    /// Promotes the staged file to `.1`, first shifting each retained `.N` up one and removing
    /// `.{max_files - 1}`. A no-op with nothing staged; only reached when `max_files >= 2`.
    ///
    /// Every failure is a throttled `retention_failure` and is skipped: it risks losing history,
    /// not correctness.
    fn promote_staged(&self, diag: &mut Diagnostics) {
        let staging = self.staging_path();
        if !staging.exists() {
            return;
        }

        let max_files = self.state.policy.max_files.max(1);
        let oldest = self.rotated_path(max_files - 1);
        if oldest.exists() {
            if let Err(e) = fault_io!(
                RETAINED_UNLINK,
                &oldest,
                u64::from(max_files - 1),
                std::fs::remove_file(&oldest)
            ) {
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
                if let Err(e) =
                    fault_io!(RETAINED_RENAME, &from, u64::from(n), std::fs::rename(&from, &to))
                {
                    diag.warn_throttled(
                        "retention_failure",
                        format_args!("renaming {} to {}: {e}", from.display(), to.display()),
                    );
                }
            }
        }

        let rotated = self.rotated_path(1);
        if let Err(e) = fault_io!(STAGING_RENAME, &staging, 0, std::fs::rename(&staging, &rotated))
        {
            diag.warn_throttled(
                "retention_failure",
                format_args!("renaming {} to {}: {e}", staging.display(), rotated.display()),
            );
        }
    }

    /// Rotates the active file, **commit point first**: the active file is renamed to its staging
    /// path before anything retained is touched, so a kill mid-rotation leaves at most an orphaned
    /// staging file, recovered on the next rotation. Order:
    ///
    /// 1. Flush the active handle, if any.
    /// 2. `max_files == 1`: truncate the active file in place; no staging, no cascade.
    /// 3. Otherwise, [`FileTarget::promote_staged`], recovering an orphan from a previous run.
    /// 4. **Commit point:** rename the active file to its staging path.
    /// 5. Drop the handle and reset rotation state.
    /// 6. Re-open `path` fresh.
    /// 7. [`FileTarget::promote_staged`] again, whether or not step 6 succeeded, so the previous
    ///    period's events reach `.1` either way.
    ///
    /// Failure policy:
    ///
    /// - Flushing before rotating: unclassified `Err`.
    /// - Renaming the active file to its staging path, or the `max_files: 1` truncate:
    ///   `rotate_failure` and [`RotateOutcome::NotRotated`]; nothing on disk or in state
    ///   changed, so the next write retries.
    /// - Re-opening `path` after the rename: `Err` with [`Fault::Clean`].
    /// - Cascading or promoting a retained file: `retention_failure`, continue.
    ///
    /// A re-open failure is `Fault::Clean`: the batch provably reached no file, so a retry is safe
    /// under either delivery posture, and a likely-transient ENOSPC/EMFILE-class failure isn't a
    /// configuration error (`docs/adr/rotating-file-output.md`, "Retention").
    pub async fn rotate(&mut self, diag: &mut Diagnostics) -> anyhow::Result<RotateOutcome> {
        self.rotate_inner(diag, open_active).await
    }

    /// [`FileTarget::rotate`] with an injectable opener: once `path` is renamed away, no test can
    /// make a `create` there fail, so injection is the only way to exercise that path. A `fn`
    /// pointer, not `impl Fn`, so the future stays `Send`.
    async fn rotate_inner(
        &mut self,
        diag: &mut Diagnostics,
        open: fn(&Path, bool) -> std::io::Result<std::fs::File>,
    ) -> anyhow::Result<RotateOutcome> {
        if let Some(file) = self.file.as_mut() {
            fault_io!(ACTIVE_FLUSH, &self.path, 0, file.flush().await)
                .with_context(|| format!("flushing {} before rotation", self.path.display()))?;
        }

        let max_files = self.state.policy.max_files.max(1);
        if max_files == 1 {
            // No room for a `.1`: start an empty file in place.
            return match fault_io!(ACTIVE_TRUNCATE, &self.path, 0, open(&self.path, true)) {
                Ok(std_file) => {
                    self.file = Some(tokio::fs::File::from_std(std_file));
                    self.state.reset();
                    Ok(RotateOutcome::Rotated)
                }
                // As with the commit-point rename below: the flushed handle and the rotation
                // state are untouched, so this batch lands in the existing file and the next
                // write retries.
                Err(e) => {
                    diag.warn_throttled(
                        "rotate_failure",
                        format_args!("truncating {}: {e}", self.path.display()),
                    );
                    Ok(RotateOutcome::NotRotated)
                }
            };
        }

        // Recover an orphaned staging file before this rotation stages a new one.
        self.promote_staged(diag);

        // Commit point. On failure nothing rotated: keep the flushed handle and retry on the
        // next write rather than failing the sink over a possibly transient permissions issue.
        let staging = self.staging_path();
        if let Err(e) =
            fault_io!(ACTIVE_RENAME, &self.path, 0, std::fs::rename(&self.path, &staging))
        {
            diag.warn_throttled(
                "rotate_failure",
                format_args!("renaming {} to {}: {e}", self.path.display(), staging.display()),
            );
            return Ok(RotateOutcome::NotRotated);
        }

        self.file = None;
        self.state.reset();

        let reopened: anyhow::Result<()> =
            match fault_io!(ACTIVE_OPEN, &self.path, 0, open(&self.path, false)) {
                Ok(std_file) => {
                    self.file = Some(tokio::fs::File::from_std(std_file));
                    Ok(())
                }
                Err(e) => Err(e)
                    .with_context(|| format!("re-opening {} after rotation", self.path.display()))
                    .context(Fault::Clean),
            };

        // Whether or not the re-open succeeded, so the staged file still reaches `.1`.
        self.promote_staged(diag);

        reopened.map(|()| RotateOutcome::Rotated)
    }

    #[cfg(test)]
    async fn rotate_with(
        &mut self,
        diag: &mut Diagnostics,
        open: fn(&Path, bool) -> std::io::Result<std::fs::File>,
    ) -> anyhow::Result<RotateOutcome> {
        self.rotate_inner(diag, open).await
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    // No `tempfile` dependency (`docs/adr/file-tailing-and-docker-json-logs.md`, Alternatives),
    // so tests make unique scratch directories by hand, as `logit-inputs`' tail tests do.
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
    use logit_pipeline::fault::{self, errno};

    fn diag() -> Diagnostics {
        Diagnostics::new("test")
    }

    fn size_policy(max_bytes: u64) -> RotatePolicy {
        RotatePolicy { max_bytes: Some(max_bytes), interval: None, max_files: 5 }
    }

    fn interval_policy(interval: RotateInterval) -> RotatePolicy {
        RotatePolicy { max_bytes: None, interval: Some(interval), max_files: 5 }
    }

    /// Always fails, unconditionally -- the injected opener for every `rotate_with` test below.
    /// Once `path` has been renamed away, there is no real filesystem trick left to make a
    /// `create` at that freed name fail, so this is the only way to exercise that failure mode.
    fn failing_open(_: &Path, _: bool) -> std::io::Result<std::fs::File> {
        Err(std::io::Error::other("injected open failure"))
    }

    fn backdate_mtime(path: &Path, unix_seconds: i64) {
        let time = SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(unix_seconds as u64);
        let file =
            std::fs::OpenOptions::new().write(true).open(path).expect("open for mtime backdate");
        file.set_modified(time).expect("set_modified");
    }

    /// The `key` of every diagnostic reported into `registry`, in report order.
    fn reported_diagnostic_keys(registry: &Registry) -> Vec<String> {
        registry
            .drain(0)
            .iter()
            .filter_map(|e| e.attributes.get("key").and_then(|v| v.as_str()).map(str::to_string))
            .collect()
    }

    // --- RotationState::should_rotate: no real files or clocks ---

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
        // `period` starts `None`, so there is nothing to compare against yet.
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

    #[test]
    fn seed_period_takes_the_period_from_an_existing_files_last_write_not_the_current_clock() {
        let mut state = RotationState::new(10, interval_policy(RotateInterval::Daily));
        state.seed_period(50_000); // day 0
        assert_eq!(state.period, Some(0));
        assert!(!state.should_rotate(86_399, 10), "still day 0 relative to the seeded period");
        assert!(state.should_rotate(86_400, 10), "now in day 1 relative to the seeded period");
    }

    #[test]
    fn seed_period_ignores_an_empty_file_so_a_fresh_targets_first_write_still_never_rotates() {
        let mut state = RotationState::new(0, interval_policy(RotateInterval::Daily));
        state.seed_period(999_999); // far in the "past", but nothing was ever written
        assert_eq!(state.period, None, "an empty file has no previous period's events to protect");
        assert!(!state.should_rotate(0, 10), "the first write into a fresh file must not rotate");
    }

    // --- rotate / write_all / flush: real files ---

    #[tokio::test]
    async fn a_size_rotation_moves_the_old_content_to_dot_1_and_starts_fresh() {
        let dir = scratch_dir("size-rotation");
        let path = dir.join("events.log");
        let mut target = FileTarget::open(&path, size_policy(10)).expect("open");
        target.write_all(b"0123456789").await.unwrap();
        target.note_written(0, 10);

        assert!(target.should_rotate(0, 1));
        assert_eq!(
            target.rotate(&mut diag()).await.expect("rotate should succeed"),
            RotateOutcome::Rotated
        );
        target.write_all(b"new").await.unwrap();
        target.note_written(0, 3);
        target.flush().await.unwrap();

        let rotated = std::fs::read_to_string(dir.join("events.log.1")).unwrap();
        assert_eq!(rotated, "0123456789");
        let active = std::fs::read_to_string(&path).unwrap();
        assert_eq!(active, "new");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_batch_larger_than_max_bytes_lands_whole_in_one_file_never_split() {
        // The target never splits a write; `StreamOutput::send` decides when to rotate.
        let dir = scratch_dir("oversized-batch");
        let path = dir.join("events.log");
        let mut target = FileTarget::open(&path, size_policy(10)).expect("open");
        let big = vec![b'x'; 1000];
        target.write_all(&big).await.unwrap();
        target.note_written(0, big.len());
        target.flush().await.unwrap();

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
    async fn a_file_last_written_in_an_earlier_period_rotates_on_the_first_write_after_a_restart() {
        let dir = scratch_dir("seed-period-restart-earlier");
        let path = dir.join("events.log");
        std::fs::write(&path, b"yesterday's events").unwrap();
        backdate_mtime(&path, now_unix() - 2 * 86_400); // two days ago -- a different day for sure

        let target = FileTarget::open(&path, interval_policy(RotateInterval::Daily)).expect("open");
        assert!(
            target.should_rotate(now_unix(), 1),
            "a restart against a file last written in an earlier day should rotate on the very \
             first write after that restart"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_file_just_written_in_the_current_period_does_not_rotate_on_the_first_write_after_a_restart(
    ) {
        let dir = scratch_dir("seed-period-restart-current");
        let path = dir.join("events.log");
        std::fs::write(&path, b"just now").unwrap(); // mtime is already "now" -- no backdating

        let target = FileTarget::open(&path, interval_policy(RotateInterval::Daily)).expect("open");
        assert!(
            !target.should_rotate(now_unix(), 1),
            "a restart against a file last written in the current day must not rotate on the \
             first write after that restart"
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
            target.write_all(label).await.unwrap();
            target.note_written(0, label.len());
            assert_eq!(
                target.rotate(&mut diag()).await.expect("rotate should succeed"),
                RotateOutcome::Rotated
            );
        }
        target.write_all(b"d").await.unwrap();
        target.flush().await.unwrap();

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
        target.write_all(b"old").await.unwrap();
        target.note_written(0, 3);

        assert_eq!(
            target.rotate(&mut diag()).await.expect("rotate should succeed"),
            RotateOutcome::Rotated
        );
        target.write_all(b"new").await.unwrap();
        target.flush().await.unwrap();

        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
        assert!(!dir.join("events.log.1").exists(), "max_files: 1 must never create a .1");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_clean_rotation_reports_no_diagnostics() {
        // `logit.output.file.rotations` is `StreamOutput`'s to count (`stdio.rs`'s tests).
        let dir = scratch_dir("clean-rotate");
        let path = dir.join("events.log");
        let mut target = FileTarget::open(&path, size_policy(1)).expect("open");
        target.write_all(b"x").await.unwrap();
        target.note_written(0, 1);

        let registry = Registry::new();
        let mut diag =
            Diagnostics::new("f").with_telemetry(registry.telemetry_for("f", "file_out", "sink"));
        assert_eq!(
            target.rotate(&mut diag).await.expect("rotate should succeed"),
            RotateOutcome::Rotated
        );
        assert!(registry.drain(0).is_empty(), "a clean rotation should report no diagnostics");
        std::fs::remove_dir_all(&dir).ok();
    }

    // --- Commit point first: a failed active-file rename touches nothing retained ---

    #[tokio::test]
    async fn a_failed_active_file_rename_leaves_every_retained_file_completely_untouched() {
        let dir = scratch_dir("failed-rename-untouched");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 3 };
        let mut target = FileTarget::open(&path, policy).expect("open");

        target.write_all(b"a").await.unwrap();
        target.note_written(0, 1);
        assert_eq!(
            target.rotate(&mut diag()).await.expect("rotate should succeed"),
            RotateOutcome::Rotated
        );
        target.write_all(b"b").await.unwrap();
        target.note_written(0, 1);
        assert_eq!(
            target.rotate(&mut diag()).await.expect("rotate should succeed"),
            RotateOutcome::Rotated
        );

        // Now .1 == "b", .2 == "a". The fd stays valid after the unlink, but the commit-point
        // rename has nothing at `path`.
        target.write_all(b"c").await.unwrap();
        std::fs::remove_file(&path).unwrap();

        let registry = Registry::new();
        let mut diag =
            Diagnostics::new("f").with_telemetry(registry.telemetry_for("f", "file_out", "sink"));
        let outcome = target.rotate(&mut diag).await.expect("a failed rename must not be fatal");
        assert_eq!(outcome, RotateOutcome::NotRotated);

        assert_eq!(std::fs::read_to_string(dir.join("events.log.1")).unwrap(), "b");
        assert_eq!(std::fs::read_to_string(dir.join("events.log.2")).unwrap(), "a");
        assert!(!dir.join("events.log.3").exists());
        assert!(!dir.join("events.log.rotating").exists(), "the rename never even started");
        assert_eq!(
            reported_diagnostic_keys(&registry),
            vec!["rotate_failure"],
            "a failed active-file rename must report rotate_failure, not retention_failure"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn repeated_failed_rotations_never_delete_retained_history() {
        let dir = scratch_dir("repeated-failed-rotations");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 3 };
        let mut target = FileTarget::open(&path, policy).expect("open");

        target.write_all(b"a").await.unwrap();
        target.note_written(0, 1);
        assert_eq!(
            target.rotate(&mut diag()).await.expect("rotate should succeed"),
            RotateOutcome::Rotated
        );
        target.write_all(b"b").await.unwrap();
        target.note_written(0, 1);
        assert_eq!(
            target.rotate(&mut diag()).await.expect("rotate should succeed"),
            RotateOutcome::Rotated
        );

        std::fs::remove_file(&path).unwrap();

        for _ in 0..5 {
            let outcome =
                target.rotate(&mut diag()).await.expect("a failed rename must not be fatal");
            assert_eq!(outcome, RotateOutcome::NotRotated);
        }

        assert_eq!(std::fs::read_to_string(dir.join("events.log.1")).unwrap(), "b");
        assert_eq!(std::fs::read_to_string(dir.join("events.log.2")).unwrap(), "a");
        assert!(!dir.join("events.log.3").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_stale_staging_file_from_a_killed_process_is_promoted_on_the_next_rotation() {
        let dir = scratch_dir("stale-staging-promoted");
        let path = dir.join("events.log");
        // A previous run killed between the commit-point rename and its promotion.
        std::fs::write(dir.join("events.log.rotating"), b"orphan").unwrap();

        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 3 };
        let mut target =
            FileTarget::open(&path, policy).expect("open recreates a fresh active file");

        target.write_all(b"new").await.unwrap();
        target.note_written(0, 3);
        let outcome =
            target.rotate(&mut diag()).await.expect("rotate should succeed and heal the orphan");
        assert_eq!(outcome, RotateOutcome::Rotated);

        assert_eq!(
            std::fs::read_to_string(dir.join("events.log.1")).unwrap(),
            "new",
            "this rotation's own content should land at .1"
        );
        assert_eq!(
            std::fs::read_to_string(dir.join("events.log.2")).unwrap(),
            "orphan",
            "the orphaned staging file from the killed process should have been promoted, not lost"
        );
        assert!(!dir.join("events.log.rotating").exists());
        std::fs::remove_dir_all(&dir).ok();
    }

    // --- A failed re-open: `file: None` and `Fault::Clean` ---

    #[tokio::test]
    async fn a_failed_reopen_after_a_committed_rename_never_writes_into_the_rotated_file() {
        let dir = scratch_dir("failed-reopen-no-write-into-rotated");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 3 };
        let mut target = FileTarget::open(&path, policy).expect("open");
        target.write_all(b"old").await.unwrap();
        target.note_written(0, 3);

        let err = target
            .rotate_with(&mut diag(), failing_open)
            .await
            .expect_err("a failed re-open should propagate as an error");
        assert!(format!("{err:?}").contains("injected open failure"), "got: {err:?}");

        assert_eq!(
            std::fs::read_to_string(dir.join("events.log.1")).unwrap(),
            "old",
            "the committed rename should still have landed the old content at .1"
        );

        target.write_all(b"new").await.expect("write should self-heal via a lazy re-open");
        target.flush().await.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "new");
        assert_eq!(
            std::fs::read_to_string(dir.join("events.log.1")).unwrap(),
            "old",
            "the rotated file must never receive a write meant for the fresh active file"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_reopen_is_classified_clean_so_the_batch_is_retried_rather_than_dropped() {
        let dir = scratch_dir("failed-reopen-clean");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 3 };
        let mut target = FileTarget::open(&path, policy).expect("open");
        target.write_all(b"x").await.unwrap();
        target.note_written(0, 1);

        let err = target
            .rotate_with(&mut diag(), failing_open)
            .await
            .expect_err("a failed re-open should propagate as an error");

        assert_eq!(logit_pipeline::classify(&err), logit_pipeline::Fault::Clean);
        assert!(
            !logit_pipeline::is_explicitly_permanent(&err),
            "a transient re-open failure must never trip write_loop's sustained-permanent-\
             failure exit window"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_reopen_leaves_the_rotation_state_reset_so_it_does_not_rotate_again_every_write(
    ) {
        let dir = scratch_dir("failed-reopen-state-reset");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(10), interval: None, max_files: 3 };
        let mut target = FileTarget::open(&path, policy).expect("open");
        target.write_all(b"0123456789").await.unwrap();
        target.note_written(0, 10);

        let _ = target.rotate_with(&mut diag(), failing_open).await;

        assert_eq!(target.state.written, 0, "rotation state should be reset at the commit point");
        assert_eq!(target.state.period, None);
        assert!(
            !target.should_rotate(0, 5),
            "a reset target with nothing written yet must not immediately want to rotate again"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_truncate_under_max_files_one_is_not_rotated_and_keeps_writing_to_the_existing_file(
    ) {
        let dir = scratch_dir("failed-truncate-max-files-one");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 1 };
        let mut target = FileTarget::open(&path, policy).expect("open");
        send(&mut target, b"old\n").await.expect("the first batch never rotates");

        let scope = fault::scope(&dir);
        scope.fail(ACTIVE_TRUNCATE, errno::EACCES);
        let registry = Registry::new();
        let mut diag =
            Diagnostics::new("f").with_telemetry(registry.telemetry_for("f", "file_out", "sink"));
        let outcome = target.rotate(&mut diag).await.expect("a failed truncate must not be fatal");
        assert_eq!(outcome, RotateOutcome::NotRotated);
        assert_eq!(reported_diagnostic_keys(&registry), vec!["rotate_failure"]);
        assert_eq!(target.state.written, 4, "a failed truncate must leave written untouched");
        assert!(target.should_rotate(0, 1), "the next batch re-attempts the rotation");

        for batch in [b"b1\n", b"b2\n"] {
            send(&mut target, batch).await.expect("the batch is written, not dropped");
        }
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "old\nb1\nb2\n");

        // With the fault gone, the very next batch's re-attempt truncates.
        drop(scope);
        send(&mut target, b"b3\n").await.expect("rotate and write");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "b3\n");
        assert!(!dir.join("events.log.1").exists(), "max_files: 1 must never create a .1");
        std::fs::remove_dir_all(&dir).ok();
    }

    // --- Crash matrix: a freeze at every filesystem operation of one rotation ---

    /// `StreamOutput::send`'s file-target sequence, at a fixed clock.
    async fn send(target: &mut FileTarget, bytes: &[u8]) -> anyhow::Result<()> {
        if target.should_rotate(0, bytes.len()) {
            let _ = target.rotate(&mut diag()).await?;
        }
        target.note_written(0, bytes.len());
        target.write_all(bytes).await?;
        target.flush().await
    }

    /// Ten bytes, so under [`one_line_per_file`] every file holds exactly one line.
    fn line(i: usize) -> String {
        format!("line-{i:04}\n")
    }

    fn one_line_per_file(max_files: u32) -> RotatePolicy {
        RotatePolicy { max_bytes: Some(10), interval: None, max_files }
    }

    fn suffixed(path: &Path, suffix: &str) -> PathBuf {
        let mut name = path.as_os_str().to_owned();
        name.push(suffix);
        PathBuf::from(name)
    }

    /// Every line in every file the target could have left, oldest first: `.N` from past
    /// `max_files` down to `.1`, then `.rotating` (always newer than every `.N`), then `path`.
    fn lines_oldest_first(path: &Path, max_files: u32) -> Vec<String> {
        let mut files: Vec<PathBuf> =
            (1..max_files + 3).rev().map(|n| suffixed(path, &format!(".{n}"))).collect();
        files.push(suffixed(path, ".rotating"));
        files.push(path.to_path_buf());
        files
            .iter()
            .filter_map(|f| std::fs::read_to_string(f).ok())
            .flat_map(|text| text.lines().map(|l| format!("{l}\n")).collect::<Vec<_>>())
            .collect()
    }

    /// The contents of `.1` through `.{max_files - 1}`, and whether `.rotating` exists.
    fn retained_snapshot(path: &Path, max_files: u32) -> (Vec<Option<String>>, bool) {
        let retained = (1..max_files)
            .map(|n| std::fs::read_to_string(suffixed(path, &format!(".{n}"))).ok())
            .collect();
        (retained, suffixed(path, ".rotating").exists())
    }

    /// Lines written before the rotating one: enough that every retained generation exists and
    /// retention has already deleted some.
    const HISTORY: usize = 8;

    /// Writes [`HISTORY`] lines, then records every operation the next line's send (which
    /// rotates) reaches.
    async fn record_one_rotation(max_files: u32) -> Vec<Point> {
        let dir = scratch_dir(&format!("crash-record-{max_files}"));
        let path = dir.join("events.log");
        let mut target = FileTarget::open(&path, one_line_per_file(max_files)).expect("open");
        for i in 0..HISTORY {
            send(&mut target, line(i).as_bytes()).await.expect("history write");
        }
        let scope = fault::scope(&dir);
        scope.record();
        send(&mut target, line(HISTORY).as_bytes()).await.expect("a clean rotation");
        let points = scope.hits().iter().map(|hit| hit.point).collect();
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
        points
    }

    /// [`record_one_rotation`]'s setup, but freezing at the `n`th hit of `point`. Then drops the
    /// target, revives, reopens a fresh one on the same path, writes two more lines (so at least
    /// one rotation runs after the restart), and checks the oracle. `before_commit`: the crash
    /// point precedes the commit-point rename.
    async fn crash_and_restart(max_files: u32, point: Point, n: u64, before_commit: bool) {
        let label = format!("max_files {max_files}, crash at {point:?} #{n}");
        let dir = scratch_dir(&format!("crash-matrix-{max_files}"));
        let path = dir.join("events.log");
        let policy = one_line_per_file(max_files);
        let mut target = FileTarget::open(&path, policy).expect("open");
        for i in 0..HISTORY {
            send(&mut target, line(i).as_bytes()).await.expect("history write");
        }
        let before = retained_snapshot(&path, max_files);

        let scope = fault::scope(&dir);
        scope.crash_at(point, n);
        let rotating_line = line(HISTORY);
        let result = send(&mut target, rotating_line.as_bytes()).await;
        assert!(scope.crashed(), "{label}: the crash point was never reached");
        assert!(result.is_err(), "{label}: a frozen send can't succeed");
        if before_commit {
            assert_eq!(retained_snapshot(&path, max_files), before, "{label}: retained touched");
        }

        // The seam can't recall a write tokio already handed to its blocking pool (`fault.rs`'s
        // "Limitation"), so settle it outside the seam: it lands, as it would after a kill -9
        // that followed the write syscall.
        if let Some(file) = target.file.as_mut() {
            let _ = file.flush().await;
        }
        drop(target);
        scope.revive();

        let mut target = FileTarget::open(&path, policy).expect("reopen after the crash");
        let after = [line(HISTORY + 1), line(HISTORY + 2)];
        for l in &after {
            send(&mut target, l.as_bytes()).await.unwrap_or_else(|e| panic!("{label}: {e:?}"));
        }

        let present = lines_oldest_first(&path, max_files);
        let mut written: Vec<String> = (0..HISTORY).map(line).collect();
        if present.contains(&rotating_line) {
            written.push(rotating_line);
        }
        written.extend(after);
        assert!(
            present.len() <= written.len() && present == written[written.len() - present.len()..],
            "{label}: {present:?} is not an in-order suffix of {written:?} without duplicates"
        );
        assert_eq!(present.len(), max_files as usize, "{label}: a generation went missing");
        assert!(!suffixed(&path, ".rotating").exists(), "{label}: an orphan was never promoted");
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_crash_at_any_rotation_step_loses_no_line_and_duplicates_none_after_restart() {
        for max_files in [2, 3] {
            let points = record_one_rotation(max_files).await;
            let commit = points.iter().position(|p| *p == ACTIVE_RENAME).expect("a commit rename");
            let first_retention = points
                .iter()
                .position(|p| p.site != sites::FILE_OUT_ACTIVE)
                .expect("a retention step");
            assert!(commit < first_retention, "the commit point runs first: {points:?}");
            // Every generation exists, so the rotation unlinks the oldest and cascades the rest.
            let cascade = vec![RETAINED_RENAME; max_files as usize - 2];
            let expected: Vec<Point> = [ACTIVE_FLUSH, ACTIVE_RENAME, ACTIVE_OPEN, RETAINED_UNLINK]
                .into_iter()
                .chain(cascade)
                .chain([STAGING_RENAME, ACTIVE_WRITE, ACTIVE_FLUSH])
                .collect();
            assert_eq!(points, expected, "max_files {max_files}");
            for (index, point) in points.iter().enumerate() {
                let n = points[..=index].iter().filter(|p| *p == point).count() as u64;
                crash_and_restart(max_files, *point, n, index <= commit).await;
            }
        }
    }

    #[tokio::test]
    async fn promote_staged_keeps_every_generation_in_suffix_order_for_max_files_two_through_six() {
        for max_files in 2..=6u32 {
            let dir = scratch_dir(&format!("promote-{max_files}"));
            let path = dir.join("events.log");
            let mut target = FileTarget::open(&path, one_line_per_file(max_files)).expect("open");
            let total = 2 * max_files as usize + 1;
            for i in 0..total {
                send(&mut target, line(i).as_bytes()).await.expect("write");
            }

            assert_eq!(std::fs::read_to_string(&path).unwrap(), line(total - 1));
            for n in 1..max_files {
                assert_eq!(
                    std::fs::read_to_string(suffixed(&path, &format!(".{n}"))).unwrap(),
                    line(total - 1 - n as usize),
                    "max_files {max_files}: .{n}"
                );
            }
            assert!(!suffixed(&path, &format!(".{max_files}")).exists(), "max_files {max_files}");
            assert!(!suffixed(&path, ".rotating").exists(), "max_files {max_files}");
            std::fs::remove_dir_all(&dir).ok();
        }
    }
}
