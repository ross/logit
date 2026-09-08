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
use logit_pipeline::Fault;
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

/// `t` as whole Unix seconds, or `None` when the conversion fails (`t` predates the epoch --
/// never true for a real mtime or the real clock). The one conversion path both [`now_unix`] (the
/// real clock) and [`FileTarget::open`] (an existing file's mtime) go through.
fn unix_seconds(t: SystemTime) -> Option<i64> {
    t.duration_since(UNIX_EPOCH).ok().map(|d| d.as_secs() as i64)
}

/// Current Unix time in whole seconds, UTC -- the clock [`crate::stdio::StreamOutput::send`]
/// passes into [`RotationState::should_rotate`]/[`RotationState::note_written`]. A free function
/// rather than a method so a test can call those directly with an arbitrary injected `now_unix`
/// instead of racing the real clock, mirroring `logit_pipeline::Transform::flush(now)`'s
/// injected-clock precedent.
pub(crate) fn now_unix() -> i64 {
    unix_seconds(SystemTime::now()).unwrap_or_default()
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
    /// active file belongs to. `None` until either [`RotationState::seed_period`] (an existing
    /// file's mtime, at open) or the first write after open/rotation sets it -- a freshly opened
    /// *empty* file still starts `None`, so its very first batch never spuriously rotates just
    /// because no period was known yet -- see [`RotationState::should_rotate`].
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

    /// Seeds `period` from an already-existing file's own last-modified time, at
    /// [`FileTarget::open`] -- mirrors how `written` is already seeded from the file's length.
    /// Without this, restarting against a file under an `interval` policy would forget which
    /// calendar period it was last written in: either merging two periods' events into the same
    /// file (silently breaking "the rolled file holds exactly the previous period's events" across
    /// a restart), or never noticing a boundary already crossed while the process was down. Only
    /// sets `period` when `written > 0` -- the same guard [`RotationState::should_rotate`] itself
    /// uses -- since an empty file (created but never written to) has no previous period's events
    /// to protect, mirroring the reasoning that keeps a freshly opened file's very first batch
    /// from spuriously rotating.
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
    /// call after open/rotation (see the field's own doc comment) when it isn't already set --
    /// covers a freshly-*created* file, which has nothing for [`RotationState::seed_period`] to
    /// seed from at open time. An existing file's period is seeded from its mtime at open instead
    /// (`FileTarget::open`), so this only actually sets `period` here for a file that had none to
    /// begin with.
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

/// The result of a [`FileTarget::rotate`] call. `NotRotated` is the documented "the active file's
/// rename failed, so the target kept writing to the file it already had open" case -- not an
/// error (nothing on disk was touched, and the existing handle, if any, is still perfectly good to
/// keep writing to), but it must never be counted as an actual rotation
/// (`StreamOutput::send`'s `logit.output.file.rotations`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub enum RotateOutcome {
    Rotated,
    NotRotated,
}

/// Opens the active file at `path` -- shared by [`FileTarget::open`] (the initial open) and
/// [`FileTarget::rotate_inner`]'s two re-open sites (truncate-in-place under `max_files: 1`, and
/// the fresh file after a committed rename). `truncate` selects which: `true` discards the
/// existing content in place (there is no `.1` to rename into under `max_files: 1`), `false`
/// appends (the normal case -- `path` was just renamed away, or never existed, so a fresh `create`
/// starts empty regardless).
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

/// The open file `Target::File` writes to (`crate::stdio`), plus everything needed to decide when
/// and how to rotate it. Opened eagerly via [`FileTarget::open`], exactly like `stdio_out`'s
/// previous standalone `open_path` did -- called from `build_spec` at config-build time, so a bad
/// path or a permissions error is a config error that fails before anything starts listening.
#[derive(Debug)]
pub struct FileTarget {
    path: PathBuf,
    /// The open handle to `path`. `None` only between a committed rotation rename (the instant
    /// `path` is renamed away) and a successful re-open at the same path -- never a handle to an
    /// already-rotated-away file, since the handle is dropped at exactly that rename, not lazily
    /// discovered stale afterward. [`FileTarget::write_all`]/[`FileTarget::flush`] re-open it
    /// lazily via `ensure_open` on the next write if it's still `None`.
    file: Option<tokio::fs::File>,
    state: RotationState,
}

impl FileTarget {
    pub fn open(path: impl AsRef<Path>, policy: RotatePolicy) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let std_file = open_active(path, false)
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

    /// Lazily (re-)opens the active file when `self.file` is `None` -- only ever true between a
    /// committed rotation rename and a successful re-open (see the `file` field's own doc
    /// comment). This is what lets a write that arrives after a failed re-open self-heal on its
    /// own, rather than needing `rotate` itself to have succeeded synchronously. Attaches
    /// `Fault::Clean` on failure -- see [`FileTarget::rotate`]'s doc comment for why a failure here
    /// is always safe to retry.
    fn ensure_open(&mut self) -> anyhow::Result<&mut tokio::fs::File> {
        if self.file.is_none() {
            let std_file = open_active(&self.path, false)
                .with_context(|| format!("re-opening {} for write", self.path.display()))
                .context(Fault::Clean)?;
            self.file = Some(tokio::fs::File::from_std(std_file));
        }
        Ok(self.file.as_mut().expect("just set to Some above if it was None"))
    }

    /// Writes `bytes` to the active file, re-opening it first if a previous rotation's re-open
    /// failed and left `self.file` as `None`. Replaces the old `file_mut().write_all(...)` call
    /// site now that the handle isn't always present.
    pub async fn write_all(&mut self, bytes: &[u8]) -> anyhow::Result<()> {
        self.ensure_open()?.write_all(bytes).await?;
        Ok(())
    }

    /// Flushes the active file -- a no-op when there is no open handle (nothing buffered to
    /// flush), rather than forcing a re-open just to flush nothing.
    pub async fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.flush().await?;
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

    /// Transient staging path used only between the commit-point rename in [`FileTarget::rotate`]
    /// and its promotion to `.1` in [`FileTarget::promote_staged`]. An orphan left behind by a
    /// process killed in that window is picked up and promoted on the *next* rotation, never lost.
    /// Like a `.1`-suffixed file, this is never matched by `tail_in`'s anchored wildcard (ADR
    /// `file-tailing-and-docker-json-logs`, "Rotation and truncation") -- a `.rotating` suffix is
    /// just as much a non-match as a numeric one.
    fn staging_path(&self) -> PathBuf {
        let mut name = self.path.clone().into_os_string();
        name.push(".rotating");
        PathBuf::from(name)
    }

    /// Promotes a staged file (at [`FileTarget::staging_path`]) to `.1`, cascading every currently
    /// retained rotated file up one suffix first (dropping the oldest if it would fall off the end
    /// of `max_files`). A no-op when there is no staging file to promote. Called twice from
    /// [`FileTarget::rotate_inner`]: once before this rotation's own commit-point rename, to
    /// recover a staging file orphaned by a process killed between a *previous* run's rename and
    /// its promotion; once after, to promote what this rotation just staged. Only ever reached
    /// when `max_files >= 2` -- the `max_files == 1` case returns before ever staging anything.
    /// Every failure here is `retention_failure`: it only risks losing history, not correctness,
    /// so it's reported (throttled) and skipped rather than failing the whole rotation.
    fn promote_staged(&self, diag: &mut Diagnostics) {
        let staging = self.staging_path();
        if !staging.exists() {
            return;
        }

        let max_files = self.state.policy.max_files.max(1);
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
        if let Err(e) = std::fs::rename(&staging, &rotated) {
            diag.warn_throttled(
                "retention_failure",
                format_args!("renaming {} to {}: {e}", staging.display(), rotated.display()),
            );
        }
    }

    /// Rotates the active file, **commit-point first**: the active file is renamed to a transient
    /// staging path (see [`FileTarget::staging_path`]) *before* anything retained is touched, so a
    /// process killed mid-rotation leaves at most an orphaned staging file -- recovered on the next
    /// rotation -- rather than ever risking a `.1`/`.2`/... while the rename that would feed it is
    /// still in flight. Order:
    ///
    /// 1. Flush the active handle, if any.
    /// 2. `max_files == 1`: no room to keep any rotated file at all, so "rotating" means
    ///    truncating the active file in place rather than ever creating a `.1` -- no staging, no
    ///    cascade.
    /// 3. Otherwise, [`FileTarget::promote_staged`] first, recovering any staging file orphaned by
    ///    a process killed mid-rotation on a *previous* run, before this rotation stages a new one.
    /// 4. **Commit point:** rename the active file to its staging path.
    /// 5. Drop the handle and reset rotation state.
    /// 6. Re-open `path` fresh.
    /// 7. [`FileTarget::promote_staged`] again -- cascades retained files up one suffix, then
    ///    promotes the just-staged file to `.1` -- unconditionally, regardless of whether step 6
    ///    succeeded, so the previous period's events reach `.1` either way.
    ///
    /// Failure policy, deliberately asymmetric:
    ///
    /// | Failure | Handling |
    /// |---|---|
    /// | Flushing the active file before rotating | Fatal -- bubbles as `Err` |
    /// | Renaming the active file to its staging path (or truncating under `max_files: 1`) | `rotate_failure`, [`RotateOutcome::NotRotated`] -- nothing on disk touched, state left unchanged so the next write retries safely |
    /// | Re-opening `path` after a committed rename (or truncate) | `Err`, classified [`Fault::Clean`] |
    /// | Cascading/promoting a *retained* file | `retention_failure`, continue |
    ///
    /// The re-open failure is classified `Fault::Clean`, not left to default to `Permanent`: the
    /// bytes provably never reached any file, so retrying the batch is safe under either delivery
    /// posture, and per `logit_pipeline::output`'s `is_explicitly_permanent` doc comment, a
    /// transient condition like this (most likely ENOSPC/EMFILE-class) must never count toward
    /// `write_loop`'s sustained-permanent-failure exit window the way a real configuration error
    /// would. See `docs/adr/rotating-file-output.md`'s "Retention" section for the full reasoning.
    pub async fn rotate(&mut self, diag: &mut Diagnostics) -> anyhow::Result<RotateOutcome> {
        self.rotate_inner(diag, open_active).await
    }

    /// The actual rotation logic behind [`FileTarget::rotate`], parameterized over how the active
    /// file gets (re-)opened so a test can inject a failing opener. This seam exists because, once
    /// `path` has been renamed away by the commit-point rename below, a `create` at that
    /// now-freed name in a writable directory can't be made to fail through any path or permission
    /// trick available to a test -- the real failure mode there is ENOSPC/EMFILE-class, which a
    /// unit test can't induce on demand either, so injection is the only way to exercise it. A
    /// plain `fn` pointer, not `impl Fn`, so the returned future stays `Send`.
    async fn rotate_inner(
        &mut self,
        diag: &mut Diagnostics,
        open: fn(&Path, bool) -> std::io::Result<std::fs::File>,
    ) -> anyhow::Result<RotateOutcome> {
        if let Some(file) = self.file.as_mut() {
            file.flush()
                .await
                .with_context(|| format!("flushing {} before rotation", self.path.display()))?;
        }

        let max_files = self.state.policy.max_files.max(1);
        if max_files == 1 {
            // No room to keep any rotated file -- "rotating" a single-file policy means starting
            // a fresh, empty file in place rather than ever creating a `.1`.
            return match open(&self.path, true) {
                Ok(std_file) => {
                    self.file = Some(tokio::fs::File::from_std(std_file));
                    self.state.reset();
                    Ok(RotateOutcome::Rotated)
                }
                Err(e) => Err(e)
                    .with_context(|| format!("re-opening {} after rotation", self.path.display()))
                    .context(Fault::Clean),
            };
        }

        // Recover any staging file orphaned by a process killed mid-rotation on a previous run,
        // before this rotation creates a new one.
        self.promote_staged(diag);

        // Commit point: everything before this line is read-only with respect to what's already
        // on disk. A failure here means rotation didn't happen at all -- the safest response is to
        // keep the existing, already-flushed handle open and retry on the next write (the
        // throttled diagnostic bounds how often this actually reports), not to error the whole
        // sink over what might be a transient permissions issue.
        let staging = self.staging_path();
        if let Err(e) = std::fs::rename(&self.path, &staging) {
            diag.warn_throttled(
                "rotate_failure",
                format_args!("renaming {} to {}: {e}", self.path.display(), staging.display()),
            );
            return Ok(RotateOutcome::NotRotated);
        }

        self.file = None;
        self.state.reset();

        let reopened: anyhow::Result<()> = match open(&self.path, false) {
            Ok(std_file) => {
                self.file = Some(tokio::fs::File::from_std(std_file));
                Ok(())
            }
            Err(e) => Err(e)
                .with_context(|| format!("re-opening {} after rotation", self.path.display()))
                .context(Fault::Clean),
        };

        // Cascades retained files up one suffix, then promotes the file just staged above to
        // `.1` -- unconditionally, regardless of whether the re-open above succeeded, so the
        // previous period's events reach `.1` either way.
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

    /// The `key` attribute of every diagnostic `warn_throttled` reported into `registry`, in
    /// report order -- lets a test assert *which* failure key fired (`rotate_failure` vs.
    /// `retention_failure`) rather than just that something did.
    fn reported_diagnostic_keys(registry: &Registry) -> Vec<String> {
        registry
            .drain(0)
            .iter()
            .filter_map(|e| e.attributes.get("key").and_then(|v| v.as_str()).map(str::to_string))
            .collect()
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

    // ---------------------------------------------------------------------------------------
    // rotate / write_all / flush -- real files, real filesystem state.
    // ---------------------------------------------------------------------------------------

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
        // should_rotate is checked *before* the write in `StreamOutput::send`, not enforced here
        // -- this pins that a target itself never splits a write; the sink is what decides not to
        // call `rotate` mid-write.
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
        // `FileTarget` doesn't hold a `Telemetry` handle of its own -- `logit.output.file.
        // rotations` is `StreamOutput`'s to count (exercised in `stdio.rs`'s own tests). This
        // just pins that a clean rotation reports nothing via `Diagnostics`.
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

    // ---------------------------------------------------------------------------------------
    // F1: commit-point-first rotation -- a failed active-file rename touches nothing retained.
    // ---------------------------------------------------------------------------------------

    #[tokio::test]
    async fn a_failed_active_file_rename_leaves_every_retained_file_completely_untouched() {
        let dir = scratch_dir("failed-rename-untouched");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 3 };
        let mut target = FileTarget::open(&path, policy).expect("open");

        // Two real rotations to populate .1 and .2 with known content.
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

        // Now .1 == "b", .2 == "a". Unlink the active file out from under the still-open handle
        // -- the fd stays valid (writes still land, just nowhere `path` can see), but the
        // commit-point rename below has nothing at `path` to rename any more.
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
        // Simulate a process killed exactly between the commit-point rename and its promotion on
        // a previous run: `path` doesn't exist yet, but a staging file from that rename does.
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

    // ---------------------------------------------------------------------------------------
    // F2: Option<File> + Fault::Clean reopen classification.
    // ---------------------------------------------------------------------------------------

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
    async fn a_failed_truncate_under_max_files_one_keeps_the_existing_file_and_never_creates_a_dot_1(
    ) {
        let dir = scratch_dir("failed-truncate-max-files-one");
        let path = dir.join("events.log");
        let policy = RotatePolicy { max_bytes: Some(1), interval: None, max_files: 1 };
        let mut target = FileTarget::open(&path, policy).expect("open");
        target.write_all(b"old").await.unwrap();
        target.note_written(0, 3);

        let err = target
            .rotate_with(&mut diag(), failing_open)
            .await
            .expect_err("a failed truncate should propagate as an error");
        assert_eq!(logit_pipeline::classify(&err), logit_pipeline::Fault::Clean);

        target.flush().await.unwrap();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "old");
        assert!(!dir.join("events.log.1").exists(), "max_files: 1 must never create a .1");
        assert_eq!(target.state.written, 3, "a failed truncate must leave written untouched");
        std::fs::remove_dir_all(&dir).ok();
    }
}
