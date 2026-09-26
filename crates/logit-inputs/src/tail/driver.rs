//! The read/rotate/checkpoint/shutdown loop shared by [`TailInput`](super::TailInput) and
//! `crate::docker::DockerInput`, generic over the decoder and the factory that builds one per
//! matched path.
//!
//! A file's identity is its [`FileId`]. On each `scan`, a discovered path whose inode differs from
//! the one tracked under it is a rotation: the old inode is read to EOF and closed, and the new one
//! opened at the beginning. A tracked file whose size is below the offset already read was
//! truncated in place: it's re-read from `0` with its partial-line state discarded.

use super::checkpoint::{CheckpointStore, FileId, Loaded};
use super::line::{LineSplitter, TailDecoder};
use super::pattern::PathPattern;
use super::TailConfig;
use bytes::Bytes;
use logit_core::{Diagnostics, Event, EventBatch, Telemetry};
use logit_pipeline::{BatchAccumulator, Fanout, FlushReason};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;

/// One read off a tracked file: large enough to amortize the syscall, small enough that one busy
/// file can't starve the others in `drain`'s round robin. Not configurable.
const READ_CHUNK_BYTES: usize = 64 * 1024;

/// Turns a matched path into a decoder. [`DecoderFactory::accept`] may reject the path
/// (`docker_in`'s container filter); [`DecoderFactory::open`] then builds the decoder.
pub(crate) trait DecoderFactory<D: TailDecoder>: Send {
    /// Whether to tail this path. `&mut self` so a factory can cache what it read (`docker_in`'s
    /// `config.v2.json`) across scans.
    fn accept(&mut self, path: &Path) -> bool;

    /// Builds the decoder for an accepted path. An error is diagnosed `open_error` and the path
    /// retried on the next `scan`.
    fn open(&mut self, path: &Path) -> anyhow::Result<D>;

    /// Re-checks a tracked file's identity and selection, once per tracked file per `scan`.
    ///
    /// Takes the decoder so a factory that rebuilds a resource installs it directly, keeping the
    /// resource type private to the decoder's module (`docker.rs`). Default: nothing can change.
    fn refresh(&mut self, _path: &Path, _decoder: &mut D) -> Refresh {
        Refresh::Unchanged
    }

    /// End of one `scan`: every discovered path has had one `accept` or `refresh` call since the
    /// previous `end_scan`, so a caching factory can evict the rest. Default: nothing cached.
    fn end_scan(&mut self) {}
}

/// What one [`DecoderFactory::refresh`] call decided about a file the [`Tailer`] already tracks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Refresh {
    /// Nothing changed. The only outcome `tail_in` produces.
    Unchanged,
    /// The factory installed a new resource on the decoder. The driver does nothing more:
    /// `BatchAccumulator::absorb` sees a non-`ptr_eq` `Arc` on the next line and flushes the old
    /// batch (`FlushReason::ResourceChange`).
    Identity,
    /// No longer selected (`docker_in`, after a rename moved the container out of
    /// `containers:`). The factory must not swap the resource: what's flushed on the way out
    /// carries the identity its lines were read under. The driver stops reading at once
    /// (`FileState::Deselected`; a running container has no EOF to drain to) and keeps the offset
    /// by inode in case it's selected again.
    Deselected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileState {
    Active,
    /// No pattern matches it any more (renamed away, removed), or a new inode took its path
    /// (rotated): read to EOF, flush, close. Revived to `Active` only by a rebind in
    /// `Tailer::open_tracked`.
    Draining,
    /// Selected away by [`Refresh::Deselected`]. The file is still being written, so nothing
    /// more is read (`read_one` returns at once). `reap_drained` closes it like a `Draining` file
    /// but keeps its offset in `Tailer::resume`.
    Deselected,
}

/// What the run loop's `select!` resolved to: a plain value, so no arm has to `.await` (see
/// [`Tailer::run_until_shutdown`]).
enum Outcome {
    Shutdown,
    Wake(super::watch::Wake),
    Poll,
    Flush,
    Checkpoint,
}

/// Why [`Tailer::drain`] returned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DrainEnd {
    /// Shutdown fired between two files' reads.
    Shutdown,
    /// A full pass made no progress: every file is at EOF.
    Idle,
    /// A pass completed with a poll, flush, or checkpoint deadline already past, so the run loop
    /// goes back to its `select!`, where that timer is ready at once.
    TimerDue,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartOffset {
    Beginning,
    End,
    /// An offset past the file's current length (truncated while stopped) restarts at `0`.
    Resume(u64),
}

impl From<super::ReadFrom> for StartOffset {
    fn from(read_from: super::ReadFrom) -> Self {
        match read_from {
            super::ReadFrom::Beginning => StartOffset::Beginning,
            super::ReadFrom::End => StartOffset::End,
        }
    }
}

struct TrackedFile<D> {
    path: PathBuf,
    id: FileId,
    file: tokio::fs::File,
    offset: u64,
    splitter: LineSplitter,
    decoder: D,
    accumulator: BatchAccumulator,
    state: FileState,
    /// The file offset where the oldest line the decoder still holds starts
    /// ([`TailDecoder::holds_entry`]), or `None` while it holds nothing. Set by `read_one` when a
    /// line starts a held run, cleared once the decoder holds nothing, and on a truncation or
    /// close.
    held_from: Option<u64>,
    /// This file's `inotify` watch, added in `Tailer::open_tracked` and removed in
    /// `Tailer::reap_drained`. `None` under `WatchMode::Poll`, or if `inotify_add_watch` failed
    /// (diagnosed `watch_error`; the file then relies on `poll_interval`). Not re-registered on a
    /// rebind: the kernel watch follows the inode, not the path.
    ///
    /// A rebind leaves the watcher's recorded path stale, so a rotated-away inode's `IN_MODIFY`
    /// arrives as `Wake::Data` under its **old** name, which the replacement may own in `by_path`.
    /// That's harmless only because `on_data_wake`'s inode check returns early, and because `drain`
    /// reads every tracked file after every wake. Making draining wake-driven would break this.
    watch: Option<super::watch::WatchId>,
}

pub(crate) struct Tailer<D: TailDecoder, F: DecoderFactory<D>> {
    patterns: Vec<PathPattern>,
    factory: F,
    config: TailConfig,
    files: HashMap<FileId, TrackedFile<D>>,
    by_path: HashMap<PathBuf, FileId>,
    checkpoint: Option<CheckpointStore>,
    /// The offset an inode resumes from when next discovered, consulted before `read_from`.
    ///
    /// Filled at `bind` from the checkpoint, and by `reap_drained` for a
    /// [`FileState::Deselected`] file, so a container renamed back into the selection resumes
    /// instead of replaying. Those entries are process-local, since the checkpoint writes only
    /// tracked files (`docs/adr/docker-container-identity-and-minimal-watches.md`).
    resume: HashMap<FileId, (PathBuf, u64)>,
    /// Where a file found by the first scan with no `resume` entry starts. Set at `bind`:
    /// `Beginning` after [`Loaded::Unusable`], else from `read_from`.
    first_scan_start: StartOffset,
    diag: Diagnostics,
    telemetry: Telemetry,
    watched_dirs: HashSet<PathBuf>,
    /// Set by [`Tailer::bind`] and taken into a local by [`Tailer::run_until_shutdown`].
    /// `Option` because `Watcher` has no "not yet opened" value.
    watcher: Option<super::watch::Watcher>,
}

impl<D: TailDecoder, F: DecoderFactory<D>> Tailer<D, F> {
    pub fn new(patterns: Vec<PathPattern>, factory: F, config: TailConfig) -> Self {
        Self {
            patterns,
            factory,
            first_scan_start: StartOffset::from(config.read_from),
            config,
            files: HashMap::new(),
            by_path: HashMap::new(),
            checkpoint: None,
            resume: HashMap::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            watched_dirs: HashSet::new(),
            watcher: None,
        }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn factory_mut(&mut self) -> &mut F {
        &mut self.factory
    }

    pub(crate) fn config(&self) -> &TailConfig {
        &self.config
    }

    /// How many files are tracked, so a test can check [`Tailer::bind`]'s initial scan.
    #[cfg(test)]
    pub(crate) fn tracked_len(&self) -> usize {
        self.files.len()
    }

    /// Loads the checkpoint, opens the watcher, and runs the initial scan, so
    /// `logit_pipeline::runtime::run_with_telemetry`'s bind pre-pass fails startup before any
    /// task is spawned. Idempotent (`self.watcher` is `Some`), per
    /// [`logit_pipeline::Input::bind`].
    pub async fn bind(&mut self) -> anyhow::Result<()> {
        if self.watcher.is_some() {
            return Ok(());
        }
        if let Some(checkpoint_path) = self.config.checkpoint_path.clone() {
            let (store, loaded) =
                CheckpointStore::load(checkpoint_path, &mut self.diag, &self.telemetry);
            self.checkpoint = Some(store);
            match loaded {
                Loaded::Missing => {}
                Loaded::Resume(resume) => self.resume = resume,
                Loaded::Unusable => self.first_scan_start = StartOffset::Beginning,
            }
        }

        let mut watcher = match super::watch::Watcher::new(self.config.watch, &mut self.diag) {
            Ok(w) => w,
            Err(err) => {
                self.diag.warn_throttled("watch_error", err);
                return Err(anyhow::anyhow!("setting up file watching: could not proceed"));
            }
        };
        self.scan(true, &mut watcher).await;
        self.watcher = Some(watcher);
        Ok(())
    }

    pub async fn run_until_shutdown(
        &mut self,
        sink: Fanout,
        mut shutdown: watch::Receiver<bool>,
    ) -> anyhow::Result<()> {
        // A no-op after the runtime's pre-pass; binds here for a caller that skipped it (tests).
        self.bind().await?;
        // A local, not `self.watcher`: `select!` and `scan` borrow it `&mut` while `self` is
        // borrowed mutably too.
        let mut watcher = self.watcher.take().expect("bind() leaves a watcher behind");

        let mut next_poll = tokio::time::Instant::now() + self.config.poll_interval;
        let has_flush_interval = !self.config.batching.flush_interval.is_zero();
        let mut next_flush = has_flush_interval
            .then(|| tokio::time::Instant::now() + self.config.batching.flush_interval);
        let mut next_checkpoint = self
            .checkpoint
            .as_ref()
            .map(|_| tokio::time::Instant::now() + self.config.checkpoint_interval);

        loop {
            // No arm body may `.await`: `shutdown.wait_for` yields a `watch::Ref` (a non-`Send`
            // lock guard) that would then live across the await, and `#[async_trait]`'s `Send`
            // bound rejects that at compile time. All async work runs after the `select!`. What a
            // losing arm drops is in `docs/design/pipeline-graph.md`'s "Cancellation points".
            let outcome = tokio::select! {
                _ = shutdown.wait_for(|&due| due) => Outcome::Shutdown,
                wake = watcher.next_wake() => Outcome::Wake(wake),
                () = sleep_until_opt(Some(next_poll)) => Outcome::Poll,
                () = sleep_until_opt(next_flush), if next_flush.is_some() => Outcome::Flush,
                () = sleep_until_opt(next_checkpoint), if next_checkpoint.is_some() => Outcome::Checkpoint,
            };

            match outcome {
                Outcome::Shutdown => break,
                Outcome::Wake(wake) => match wake {
                    // One file's content changed: a truncation check only, no `scan`. `drain`
                    // below reads the new bytes.
                    super::watch::Wake::Data(path) => {
                        self.count_wake("inotify");
                        self.on_data_wake(&path).await;
                    }
                    // An entry came or went under a watched directory; only a `scan` can say
                    // which. The payload is ignored.
                    super::watch::Wake::Discover(_) => {
                        self.count_wake("inotify");
                        self.scan(false, &mut watcher).await;
                    }
                    super::watch::Wake::Overflow => {
                        self.count_wake("inotify");
                        self.telemetry.count("logit.input.watch.overflows", 1.0, &[]);
                        self.scan(false, &mut watcher).await;
                    }
                    // The wake source gave up; the watcher parks afterwards, so this arm runs at
                    // most once. Not counted as an `inotify` wake: that series flatlining while
                    // `{source="poll"}` continues is the operator's alert signal. The poll tick
                    // and `drain` carry on, so this costs latency, not data.
                    super::watch::Wake::Dead(reason) => {
                        self.diag.warn_throttled(
                            "watch_error",
                            format!(
                                "the inotify wake source is no longer usable; discovery falls \
                                 back to poll_interval alone: {reason}"
                            ),
                        );
                    }
                },
                Outcome::Poll => {
                    self.count_wake("poll");
                    next_poll = tokio::time::Instant::now() + self.config.poll_interval;
                    self.scan(false, &mut watcher).await;
                }
                Outcome::Flush => {
                    self.flush_all(&sink, FlushReason::Interval).await;
                    next_flush =
                        Some(tokio::time::Instant::now() + self.config.batching.flush_interval);
                }
                Outcome::Checkpoint => {
                    // Flush first, so the offset written never covers an event still in an
                    // accumulator. `next_flush` is left alone.
                    self.flush_all(&sink, FlushReason::Interval).await;
                    self.write_checkpoint(false).await;
                    next_checkpoint =
                        Some(tokio::time::Instant::now() + self.config.checkpoint_interval);
                }
            }

            // The earliest pending deadline: `drain` hands control back once it's past, so a
            // sustained backlog can't starve the poll, flush, and checkpoint ticks. A flush or
            // checkpoint tick already past goes straight back to the `select!` rather than
            // waiting out another pass: a poll tick due again after every long pass would
            // otherwise keep winning the `select!`'s random pick. With nothing read since either
            // last ran, both finish at once, so this can't loop. A due poll tick doesn't skip
            // `drain`: `scan` can outlast `poll_interval`, and `drain` would then never run.
            let due = [next_flush, next_checkpoint].into_iter().flatten().fold(next_poll, Ord::min);
            let now = tokio::time::Instant::now();
            if [next_flush, next_checkpoint].into_iter().flatten().any(|d| d <= now) {
                continue;
            }
            match self.drain(&sink, &shutdown, &mut watcher, due).await {
                DrainEnd::Shutdown => break,
                DrainEnd::Idle | DrainEnd::TimerDue => {}
            }
        }

        self.close_all_for_shutdown(&sink).await;
        self.flush_all(&sink, FlushReason::Shutdown).await;
        self.write_checkpoint(true).await;
        Ok(())
    }

    /// Counts `logit.input.watch.wakes{source}`. `inotify` flatlining while `poll` continues is
    /// the visible sign the low-latency path stopped (`docs/deploying.md`'s "What to watch for
    /// file tailing").
    fn count_wake(&self, source: &'static str) {
        self.telemetry.count("logit.input.watch.wakes", 1.0, &[("source", source)]);
    }

    /// Unwatches directories no pattern reaches, then `watch_dir`s **every** pattern directory,
    /// at the top of every `scan`. For `docker_in` that's only `root`
    /// (`docs/adr/docker-container-identity-and-minimal-watches.md`). A no-op under
    /// `WatchMode::Poll`.
    ///
    /// **Re-arming every directory every scan is required, not redundant.** The patterns never
    /// change, so arming only the difference would leave a directory missing at `bind` (a volume
    /// mounted later, an app creating its own log directory), or deleted and recreated, on
    /// `poll_interval` for the life of the process. The cost is one `inotify_add_watch(2)` per
    /// pattern directory per scan, which the kernel makes a no-op returning the same `wd` for a
    /// live inode (`InotifyWatcher::watch_dir`).
    ///
    /// `watched_dirs` records what's armed, not what's wanted: a failed directory is diagnosed
    /// `watch_dir_error`, left out, and retried next scan.
    fn reconcile_watches(&mut self, watcher: &mut super::watch::Watcher) {
        let desired: HashSet<PathBuf> =
            self.patterns.iter().map(|p| p.dir().to_path_buf()).collect();
        for dir in self.watched_dirs.difference(&desired) {
            watcher.unwatch_dir(dir);
        }
        let mut armed = HashSet::with_capacity(desired.len());
        for dir in desired {
            match watcher.watch_dir(&dir) {
                Ok(()) => {
                    armed.insert(dir);
                }
                Err(err) => {
                    // Its own key, not `watch_error`: it recurs every scan while the directory
                    // is missing, and `warn_throttled` logs a key only at powers of two of its
                    // count. Sharing the key would bury the one-shot `watch_error`s (a per-file
                    // `ENOSPC`, the wake source dying).
                    self.diag.warn_throttled(
                        "watch_dir_error",
                        super::watch::watch_error_message(&dir, &err),
                    );
                }
            }
        }
        self.watched_dirs = armed;
    }

    /// Discovers matched files, opens new ones, and reconciles rotation, truncation, and removal
    /// for tracked ones.
    ///
    /// Only files found on the `first` scan follow `read_from` (`first_scan_start`, which an
    /// unusable checkpoint overrides to `Beginning`); a later discovery starts at the beginning,
    /// since it has no "before startup" to skip. A checkpoint entry wins over both.
    /// A path missing from this scan (including a failed `read_dir`) starts draining its file.
    async fn scan(&mut self, first: bool, watcher: &mut super::watch::Watcher) {
        self.reconcile_watches(watcher);
        let mut discovered: HashMap<PathBuf, std::fs::Metadata> = HashMap::new();
        for pattern in &self.patterns {
            for path in pattern.scan() {
                if let Ok(meta) = std::fs::metadata(&path) {
                    if meta.is_file() {
                        discovered.insert(path, meta);
                    }
                }
            }
        }

        let stale: Vec<PathBuf> =
            self.by_path.keys().filter(|p| !discovered.contains_key(*p)).cloned().collect();
        for path in stale {
            if let Some(id) = self.by_path.remove(&path) {
                if let Some(tracked) = self.files.get_mut(&id) {
                    tracked.state = FileState::Draining;
                }
            }
        }

        for (path, meta) in discovered {
            let id = FileId::from_metadata(&meta);
            match self.by_path.get(&path).copied() {
                Some(existing_id) if existing_id == id => {
                    self.reconcile_truncation(id, meta.len()).await;
                    self.refresh_identity(id, &path);
                }
                Some(existing_id) => {
                    if let Some(tracked) = self.files.get_mut(&existing_id) {
                        tracked.state = FileState::Draining;
                    }
                    self.telemetry.count("logit.input.files.rotated", 1.0, &[]);
                    self.by_path.remove(&path);
                    self.open_tracked(path, id, StartOffset::Beginning, watcher).await;
                }
                None => {
                    // Peeked, not removed: `accept` may still reject this path (a de-selected
                    // container not yet re-selected), and removing here would lose the retained
                    // offset. `open_tracked` removes it once `accept` succeeds.
                    let start = match self.resume.get(&id) {
                        Some(&(_, offset)) => StartOffset::Resume(offset),
                        None if first => self.first_scan_start,
                        None => StartOffset::Beginning,
                    };
                    self.open_tracked(path, id, start, watcher).await;
                }
            }
        }

        self.factory.end_scan();
        self.telemetry.gauge("logit.input.files.open", self.files.len() as f64, &[]);
        // Armed directories plus files holding a watch: what makes "the watch set is
        // proportional to what's tailed" checkable from outside. Under `Poll` it counts the same
        // set with no kernel watches behind it.
        //
        // It's the intended set, not the live kernel one: a `Draining` file whose inode is gone
        // had its descriptor purged by `IN_IGNORED` while `TrackedFile::watch` still holds it,
        // and two spellings of one directory alias one kernel watch (`docs/deploying.md` says
        // so). `the_live_kernel_watch_count_matches_this_watchers_own_bookkeeping` pins the
        // watcher's own maps against `/proc/self/fdinfo`.
        let file_watches = self.files.values().filter(|f| f.watch.is_some()).count();
        self.telemetry.gauge(
            "logit.input.watch.watches",
            (self.watched_dirs.len() + file_watches) as f64,
            &[],
        );
    }

    /// Runs [`DecoderFactory::refresh`] for a tracked path found by `scan` and applies the
    /// outcome.
    fn refresh_identity(&mut self, id: FileId, path: &Path) {
        let Some(tracked) = self.files.get_mut(&id) else { return };
        match self.factory.refresh(path, &mut tracked.decoder) {
            Refresh::Unchanged => {}
            Refresh::Identity => {
                self.telemetry.count("logit.input.files.identity_changed", 1.0, &[]);
            }
            Refresh::Deselected => {
                tracked.state = FileState::Deselected;
                // Required: the path is still discovered every scan, and once `reap_drained`
                // drops the file, a leftover binding would keep the path out of `scan`'s `None`
                // arm forever, so a reversed rename could never re-select it.
                self.by_path.remove(path);
                self.telemetry.count("logit.input.files.deselected", 1.0, &[]);
            }
        }
    }

    /// If `len`, the file's current size, is below the offset already read, the file was
    /// truncated in place: seek to `0` and reset the splitter and decoder. `len` must come from
    /// the same inode as `id`. Called by `scan` and [`Tailer::on_data_wake`].
    async fn reconcile_truncation(&mut self, id: FileId, len: u64) {
        let max_line_bytes = self.config.max_line_bytes;
        let Some(tracked) = self.files.get_mut(&id) else { return };
        if len >= tracked.offset {
            return;
        }
        if let Err(err) = tracked.file.seek(std::io::SeekFrom::Start(0)).await {
            self.diag.warn_throttled("read_error", err);
            return;
        }
        let prev_offset = tracked.offset;
        tracked.offset = 0;
        // Discard, not emit, the held partial (or a stale `dropping`): it belongs to content
        // that's gone, and would otherwise be spliced onto, or swallow, the new first line. The
        // decoder gets the same reset for its own cross-line state (`docker_in`'s partial-entry
        // reassembly); see `TailDecoder::reset`.
        tracked.splitter = LineSplitter::new(max_line_bytes);
        tracked.decoder.reset();
        tracked.held_from = None;
        let path = tracked.path.clone();
        self.diag.warn_throttled("truncated", truncated_message(&path, prev_offset, len));
        self.telemetry.count("logit.input.files.truncated", 1.0, &[]);
    }

    /// Handles a `Wake::Data` for a tracked `path`: a truncation check only. A write and an
    /// in-place truncation look the same until the length is compared with the offset. No `scan`,
    /// `read_dir`, or `accept`, so one write costs O(1), not O(containers)
    /// (`docs/adr/docker-container-identity-and-minimal-watches.md`). `drain` reads the bytes.
    ///
    /// The inode is re-checked first because `by_path` is only refreshed by `scan`. After a
    /// rotation (rename away, recreate at the same name, as logrotate and Docker do), a
    /// `Wake::Data` can arrive before any `scan`, and a rotation inside a container directory
    /// raises no `Wake::Discover` at all. Pairing the old inode's offset with the new file's small
    /// length would look like a truncation, rewinding the old handle and re-emitting it all.
    async fn on_data_wake(&mut self, path: &Path) {
        let Some(&id) = self.by_path.get(path) else { return }; // no longer tracked; ignore
        let Ok(meta) = std::fs::metadata(path) else { return }; // raced with removal; `scan` will notice
        if FileId::from_metadata(&meta) != id {
            return; // a different inode answers to this name now; `scan` reconciles the rotation
        }
        self.reconcile_truncation(id, meta.len()).await;
    }

    /// Opens a newly discovered `(path, id)` at `start`, or rebinds `id` to `path` if it's
    /// already tracked under another name.
    ///
    /// A rebind happens when a pattern matches a file both before and after a rename (`app.log*`
    /// matching `app.log` and `app.log.1`). The existing entry holds the offset, the splitter's
    /// partial, decoder state, and the accumulator; reopening at `0` would re-emit the file.
    async fn open_tracked(
        &mut self,
        path: PathBuf,
        id: FileId,
        start: StartOffset,
        watcher: &mut super::watch::Watcher,
    ) {
        if let Some(tracked) = self.files.get_mut(&id) {
            if tracked.state == FileState::Deselected {
                // Not reaped yet, but must not be revived like a rebind below: after
                // `reap_drained` removes it, a later scan's `accept` re-admits it if the rename
                // is reversed.
                return;
            }
            // Same inode, new name. A `Draining` entry goes back to `Active`: a pattern reaches
            // it again, and reaping is for inodes no pattern reaches.
            let old_path = std::mem::replace(&mut tracked.path, path.clone());
            tracked.state = FileState::Active;
            // Remove the old binding only if this inode still owns it. `discovered` iterates in
            // no fixed order, so the rotation replacement may already have claimed `old_path`.
            // Removing its binding would orphan a live inode: `scan`'s stale check walks only
            // `by_path`, so it could never be drained or reaped.
            if self.by_path.get(&old_path) == Some(&id) {
                self.by_path.remove(&old_path);
            }
            self.by_path.insert(path, id);
            self.diag.warn_throttled(
                "renamed",
                "a tracked file is now matched under a new name; following the same inode",
            );
            return;
        }
        if !self.factory.accept(&path) {
            return;
        }
        // Accepted: `start` will be applied, so its resume entry (if any) is spent.
        self.resume.remove(&id);
        let mut file = match tokio::fs::File::open(&path).await {
            Ok(f) => f,
            Err(err) => {
                self.diag.warn_throttled("open_error", format!("{}: {err}", path.display()));
                return;
            }
        };
        let len = match file.metadata().await {
            Ok(meta) => meta.len(),
            Err(err) => {
                self.diag.warn_throttled("open_error", format!("{}: {err}", path.display()));
                return;
            }
        };
        let offset = match start {
            StartOffset::Beginning => 0,
            StartOffset::End => len,
            StartOffset::Resume(off) if off > len => 0,
            StartOffset::Resume(off) => off,
        };
        if offset > 0 {
            if let Err(err) = file.seek(std::io::SeekFrom::Start(offset)).await {
                self.diag.warn_throttled("open_error", format!("{}: {err}", path.display()));
                return;
            }
        }
        let decoder = match self.factory.open(&path) {
            Ok(d) => d,
            Err(err) => {
                self.diag.warn_throttled("open_error", format!("{}: {err}", path.display()));
                return;
            }
        };
        // A failure is non-fatal (the file relies on `poll_interval`) but diagnosed: at scale
        // it's `ENOSPC` against `fs.inotify.max_user_watches`, and this diagnostic is the only
        // sign. Registered once per tracked inode and never retried (`docs/known-gaps.md`).
        let watch = match watcher.watch_file(&path) {
            Ok(watch) => watch,
            Err(err) => {
                self.diag
                    .warn_throttled("watch_error", super::watch::watch_error_message(&path, &err));
                None
            }
        };
        let batching = self.config.batching;
        let tracked = TrackedFile {
            path: path.clone(),
            id,
            file,
            offset,
            splitter: LineSplitter::new(self.config.max_line_bytes),
            decoder,
            accumulator: BatchAccumulator::new(batching.max_events, batching.max_bytes),
            state: FileState::Active,
            held_from: None,
            watch,
        };
        self.by_path.insert(path, id);
        self.files.insert(id, tracked);
        if let Some(cp) = &mut self.checkpoint {
            cp.mark_dirty();
        }
    }

    /// Round-robin reads every tracked file, one chunk each per pass, until a pass makes no
    /// progress ([`DrainEnd::Idle`]) or `due` has passed ([`DrainEnd::TimerDue`]), so a burst is
    /// read without waiting for another wake and a backlog still yields to the run loop's timers.
    /// Closes `Draining` and `Deselected` files that made no progress.
    ///
    /// Where shutdown is checked, and what that bounds, is in `docs/design/pipeline-graph.md`'s
    /// "Cancellation points". `due` is checked only after a whole pass and its `reap_drained`:
    /// `at_eof` is the set of files a pass read to EOF, and stopping mid-pass would leave files
    /// that were never read looking like they had reached it. At least one pass always runs, so a
    /// `select!` that keeps picking a ready wake still reads.
    async fn drain(
        &mut self,
        sink: &Fanout,
        shutdown: &watch::Receiver<bool>,
        watcher: &mut super::watch::Watcher,
        due: tokio::time::Instant,
    ) -> DrainEnd {
        loop {
            let mut any_progress = false;
            let mut at_eof: Vec<FileId> = Vec::new();
            let ids: Vec<FileId> = self.files.keys().copied().collect();
            for id in ids {
                if *shutdown.borrow() {
                    return DrainEnd::Shutdown;
                }
                if self.read_one(id, sink).await {
                    any_progress = true;
                } else {
                    at_eof.push(id);
                }
            }
            self.reap_drained(&at_eof, sink, watcher).await;
            if !any_progress {
                return DrainEnd::Idle;
            }
            if tokio::time::Instant::now() >= due {
                return DrainEnd::TimerDue;
            }
        }
    }

    /// Reads one chunk from `id`, decodes its complete lines, and emits any batch that reaches a
    /// bound. Returns `false` (eligible for [`Tailer::reap_drained`]) at EOF, on a read error,
    /// or for a [`FileState::Deselected`] file.
    async fn read_one(&mut self, id: FileId, sink: &Fanout) -> bool {
        if self.files.get(&id).is_some_and(|t| t.state == FileState::Deselected) {
            return false;
        }
        let mut chunk = vec![0u8; READ_CHUNK_BYTES];
        let n = match self.files.get_mut(&id) {
            Some(tracked) => match tracked.file.read(&mut chunk).await {
                Ok(n) => n,
                Err(err) => {
                    self.diag
                        .warn_throttled("read_error", format!("{}: {err}", tracked.path.display()));
                    0
                }
            },
            None => return false,
        };
        if n == 0 {
            return false;
        }
        let read_at = now_nanos();
        let bytes = Bytes::copy_from_slice(&chunk[..n]);

        // Each line with the file offset it starts at, for `TrackedFile::held_from`.
        let mut lines: Vec<(Bytes, u64)> = Vec::new();
        let dropped = {
            let tracked = match self.files.get_mut(&id) {
                Some(t) => t,
                None => return false,
            };
            let chunk_start = tracked.offset;
            let partial_start = chunk_start - tracked.splitter.pending_bytes();
            let stats = tracked.splitter.push(bytes, |line, start| {
                let start = start.map_or(partial_start, |i| chunk_start + i as u64);
                lines.push((line, start));
            });
            tracked.offset += n as u64;
            stats.dropped_lines
        };
        for _ in 0..dropped {
            self.diag.warn_throttled(
                "long_line",
                "a line exceeded max_line_bytes and was dropped whole",
            );
        }

        let mut scratch: Vec<Event> = Vec::new();
        for (line, line_start) in lines {
            let line = ensure_utf8(line, &mut self.diag);
            let Some(tracked) = self.files.get_mut(&id) else { return false };
            self.telemetry.count("logit.input.lines", 1.0, &[]);
            self.telemetry.count("logit.input.line.bytes", line.len() as f64, &[]);
            let decoded = tracked.decoder.decode_line(line, read_at, &mut scratch);
            // A rejected line leaves the held run as it was, so `held_from` keeps its start.
            if !tracked.decoder.holds_entry() {
                tracked.held_from = None;
            } else if tracked.held_from.is_none() {
                tracked.held_from = Some(line_start);
            }
            match decoded {
                Ok(resource) => {
                    // No scope: a tailed line has no instrumentation scope.
                    if let Some((batch, reason)) =
                        tracked.accumulator.absorb(resource, None, &mut scratch)
                    {
                        emit(sink, &self.telemetry, batch, reason).await;
                    }
                }
                Err(err) => {
                    self.diag.warn_throttled("bad_line", err);
                    scratch.clear();
                }
            }
        }
        if let Some(cp) = &mut self.checkpoint {
            cp.mark_dirty();
        }
        true
    }

    /// Closes each [`FileState::Draining`] or [`FileState::Deselected`] file whose `read_one`
    /// returned `false` on this pass (`at_eof`).
    ///
    /// Only those: a draining file with a backlog gets as many passes as it takes to reach EOF,
    /// since reaping it earlier loses the rest for good (it also leaves the next checkpoint). A
    /// read error counts as EOF, or an erroring handle would never be reaped. Emits held decoder
    /// state, flushes the accumulator (`FlushReason::Closed`), and drops the file.
    ///
    /// A reap dirties the checkpoint, so the next interval write drops the file's entry (a write
    /// persists only tracked files). Left on disk, the entry would outlive the inode: a crash, then
    /// a new file reusing that `(dev, ino)`, would resume past its first bytes.
    async fn reap_drained(
        &mut self,
        at_eof: &[FileId],
        sink: &Fanout,
        watcher: &mut super::watch::Watcher,
    ) {
        let draining: Vec<FileId> = self
            .files
            .iter()
            .filter(|(id, f)| {
                matches!(f.state, FileState::Draining | FileState::Deselected)
                    && at_eof.contains(id)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in draining {
            let Some(mut tracked) = self.files.remove(&id) else { continue };
            if let Some(watch_id) = tracked.watch {
                watcher.unwatch(watch_id);
            }
            let deselected = tracked.state == FileState::Deselected;
            close_decoder(&mut tracked, sink, &self.telemetry, &mut self.diag).await;
            if let Some(batch) = tracked.accumulator.take() {
                emit(sink, &self.telemetry, batch, FlushReason::Closed).await;
            }
            if deselected {
                // This inode is alive, only unselected (a `Draining` one may be gone and its
                // number reused), so keep its offset for a rename back into the selection. The
                // full `offset`: `close_decoder` already emitted the held partial.
                self.resume.insert(id, (tracked.path.clone(), tracked.offset));
            }
            if let Some(cp) = &mut self.checkpoint {
                cp.mark_dirty();
            }
        }
    }

    /// At shutdown, emits every tracked file's held partial line and decoder state, so an
    /// unterminated last line isn't lost (with no checkpoint, nothing would re-read it). Unlike
    /// [`Tailer::reap_drained`], files stay tracked: [`Tailer::write_checkpoint`] runs next and
    /// needs their offsets.
    async fn close_all_for_shutdown(&mut self, sink: &Fanout) {
        let ids: Vec<FileId> = self.files.keys().copied().collect();
        for id in ids {
            if let Some(tracked) = self.files.get_mut(&id) {
                close_decoder(tracked, sink, &self.telemetry, &mut self.diag).await;
            }
        }
    }

    async fn flush_all(&mut self, sink: &Fanout, reason: FlushReason) {
        let ids: Vec<FileId> = self.files.keys().copied().collect();
        for id in ids {
            let Some(tracked) = self.files.get_mut(&id) else { continue };
            if let Some(batch) = tracked.accumulator.take() {
                emit(sink, &self.telemetry, batch, reason).await;
            }
        }
    }

    /// Persists every tracked file's offset. The invariant: a persisted offset covers only bytes
    /// whose events have been emitted, or absorbed into an accumulator the caller flushes first,
    /// so a restart can replay lines but never skip one.
    ///
    /// `offset` advances per chunk, so it also covers bytes that haven't produced an event yet:
    /// the splitter's held partial line ([`LineSplitter::pending_bytes`]) and the complete lines
    /// the decoder holds (`docker_in`'s fragments of a split entry). The persisted offset is the
    /// smaller of the splitter's line boundary and `held_from`, the start of the oldest line the
    /// decoder still holds. A line rejected or dropped after that held run advances `offset`
    /// without clearing it, which is why it's a position and not a byte count to subtract. A file
    /// mid-drop holds no partial, so its offset lands inside the dropped line, and a restart there
    /// treats the rest of it as a new line. At shutdown `close_all_for_shutdown` has already
    /// emitted both, so the offset is the file's full `offset`.
    async fn write_checkpoint(&mut self, force: bool) {
        let Some(checkpoint) = &mut self.checkpoint else { return };
        let entries = self.files.values().map(|f| {
            let boundary = f.offset.saturating_sub(f.splitter.pending_bytes());
            (f.id, f.path.as_path(), boundary.min(f.held_from.unwrap_or(u64::MAX)))
        });
        checkpoint.write(entries, force, &mut self.diag, &self.telemetry).await;
    }
}

/// Emits a file's unterminated last line ([`LineSplitter::take_partial`]) and whatever
/// [`TailDecoder::close`] produces into its accumulator, flushing if a bound is reached. Used by
/// [`Tailer::reap_drained`] and [`Tailer::close_all_for_shutdown`].
async fn close_decoder<D: TailDecoder>(
    tracked: &mut TrackedFile<D>,
    sink: &Fanout,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) {
    let mut scratch = Vec::new();

    if let Some(partial) = tracked.splitter.take_partial() {
        let partial = ensure_utf8(partial, diag);
        match tracked.decoder.decode_line(partial, now_nanos(), &mut scratch) {
            Ok(resource) => {
                if let Some((batch, reason)) =
                    tracked.accumulator.absorb(resource, None, &mut scratch)
                {
                    emit(sink, telemetry, batch, reason).await;
                }
            }
            Err(err) => {
                diag.warn_throttled("bad_line", err);
                scratch.clear();
            }
        }
    }

    tracked.decoder.close(&mut scratch);
    tracked.held_from = None;
    if !scratch.is_empty() {
        let resource = tracked.decoder.resource();
        if let Some((batch, reason)) = tracked.accumulator.absorb(resource, None, &mut scratch) {
            emit(sink, telemetry, batch, reason).await;
        }
    }
}

/// Lossily converts (and diagnoses `invalid_utf8`) a line that isn't UTF-8, upholding
/// `Value::Str`'s invariant for every [`TailDecoder`].
fn ensure_utf8(line: Bytes, diag: &mut Diagnostics) -> Bytes {
    match std::str::from_utf8(&line) {
        Ok(_) => line,
        Err(_) => {
            diag.warn_throttled(
                "invalid_utf8",
                "a line contained invalid UTF-8; lossily converted",
            );
            Bytes::from(String::from_utf8_lossy(&line).into_owned())
        }
    }
}

/// The `truncated` diagnostic's text. It reports the *pre*-truncation offset, which tells an
/// operator how much was in flight; a free function so a test can assert that.
fn truncated_message(path: &Path, prev_offset: u64, len: u64) -> String {
    format!(
        "{} shrank from an offset of {prev_offset} bytes to {len} -- resuming from the beginning",
        path.display()
    )
}

async fn emit(sink: &Fanout, telemetry: &Telemetry, batch: EventBatch, reason: FlushReason) {
    telemetry.count("logit.component.receive.flushed", 1.0, &[("reason", reason.as_str())]);
    sink.send(batch).await;
}

/// `sleep_until` for an optional deadline: `None` never resolves.
async fn sleep_until_opt(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending().await,
    }
}

fn now_nanos() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tail::line::LineDecoder;
    use crate::tail::test_support::scratch_dir;
    use crate::tail::{ReadFrom, TailBatching, WatchMode};
    use logit_core::{MetricKind, Registry, Resource};
    use logit_pipeline::{unwrap_batch, Delivered};
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// A stand-in for `tail_in`'s private `LineDecoderFactory`.
    struct LineFactory;

    impl DecoderFactory<LineDecoder> for LineFactory {
        fn accept(&mut self, _path: &Path) -> bool {
            true
        }

        fn open(&mut self, path: &Path) -> anyhow::Result<LineDecoder> {
            Ok(LineDecoder::new(path, Arc::new(Resource::default())))
        }
    }

    /// A factory whose selection follows a shared flag, standing in for `docker_in`'s container
    /// filter. `accept` and `refresh` must agree, as a real filter's do, so a re-selected file is
    /// accepted again.
    struct SelectiveFactory {
        deselected: Arc<std::sync::atomic::AtomicBool>,
    }

    impl DecoderFactory<LineDecoder> for SelectiveFactory {
        fn accept(&mut self, _path: &Path) -> bool {
            !self.deselected.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn open(&mut self, path: &Path) -> anyhow::Result<LineDecoder> {
            Ok(LineDecoder::new(path, Arc::new(Resource::default())))
        }

        fn refresh(&mut self, _path: &Path, _decoder: &mut LineDecoder) -> Refresh {
            if self.deselected.load(std::sync::atomic::Ordering::SeqCst) {
                Refresh::Deselected
            } else {
                Refresh::Unchanged
            }
        }
    }

    /// A `due` for a test that calls `Tailer::drain` directly and wants it to run until idle.
    fn no_timer_due() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(3600)
    }

    fn recording_fanout(capacity: usize) -> (Fanout, mpsc::Receiver<Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]), rx)
    }

    /// Poll/checkpoint/flush intervals short enough for a test to see real ticks within a couple
    /// hundred milliseconds.
    fn fast_config(read_from: ReadFrom) -> TailConfig {
        TailConfig {
            checkpoint_path: None,
            read_from,
            watch: WatchMode::Poll,
            poll_interval: Duration::from_millis(15),
            checkpoint_interval: Duration::from_millis(30),
            max_line_bytes: 1024 * 1024,
            batching: TailBatching {
                max_events: 1_000,
                max_bytes: 1024 * 1024,
                flush_interval: Duration::from_millis(15),
            },
        }
    }

    fn spawn_tailer<F: DecoderFactory<LineDecoder> + 'static>(
        mut tailer: Tailer<LineDecoder, F>,
        sink: Fanout,
    ) -> (watch::Sender<bool>, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { tailer.run_until_shutdown(sink, rx).await });
        (tx, handle)
    }

    /// Signals shutdown and waits up to 5s, so a hang fails the test rather than the suite.
    async fn shutdown(
        tx: watch::Sender<bool>,
        handle: tokio::task::JoinHandle<anyhow::Result<()>>,
    ) {
        let _ = tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("tailer should shut down within 5s")
            .expect("tailer task should not panic")
            .expect("tailer should exit cleanly");
    }

    /// Receives at least `n` events in arrival order, waiting up to 5s per batch.
    async fn expect_events(rx: &mut mpsc::Receiver<Delivered>, n: usize) -> Vec<Event> {
        let mut events = Vec::new();
        while events.len() < n {
            let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("timed out waiting for events")
                .expect("fanout channel closed unexpectedly");
            events.extend(unwrap_batch(delivered).events);
        }
        events
    }

    fn messages(events: &[Event]) -> Vec<String> {
        events
            .iter()
            .map(|e| e.log.as_ref().unwrap().message.as_str().unwrap().to_string())
            .collect()
    }

    /// The latest buffered value of gauge `name`. Drains `registry`, so it's a one-shot check.
    fn gauge_value(registry: &Registry, name: &str) -> Option<f64> {
        registry.drain(0).into_iter().rev().find_map(|e| {
            e.metrics.iter().find_map(|m| {
                if logit_core::interner::resolve(m.name) == name {
                    match m.kind {
                        MetricKind::Gauge(v) => Some(v),
                        _ => None,
                    }
                } else {
                    None
                }
            })
        })
    }

    /// The sum of every buffered `Sum` point of counter `name`. Drains `registry`.
    fn counter_total(registry: &Registry, name: &str) -> f64 {
        registry
            .drain(0)
            .iter()
            .flat_map(|e| e.metrics.iter())
            .filter(|m| logit_core::interner::resolve(m.name) == name)
            .map(|m| match &m.kind {
                MetricKind::Sum(sum) => sum.value,
                _ => 0.0,
            })
            .sum()
    }

    // -- `Tailer::bind` --

    /// `bind()` runs the initial scan before `run_until_shutdown`.
    #[tokio::test]
    async fn bind_discovers_matching_files_before_run() {
        let dir = scratch_dir("bind-scans-first");
        let path = dir.join("app.log");
        std::fs::write(&path, b"line one\n").unwrap();

        let mut tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        assert_eq!(tailer.tracked_len(), 0, "nothing tracked before bind()");
        tailer.bind().await.expect("bind should succeed");
        assert_eq!(tailer.tracked_len(), 1, "bind()'s initial scan should have found app.log");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A second `bind()` doesn't re-scan ([`logit_pipeline::Input::bind`]'s idempotency).
    #[tokio::test]
    async fn a_second_bind_is_a_no_op() {
        let dir = scratch_dir("bind-idempotent");
        let path = dir.join("app.log");
        std::fs::write(&path, b"line one\n").unwrap();

        let mut tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        tailer.bind().await.expect("first bind should succeed");
        tailer.bind().await.expect("second bind should be a harmless no-op");
        assert_eq!(tailer.tracked_len(), 1, "still exactly one tracked file, not re-scanned");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// `run_until_shutdown` binds if the caller didn't ([`logit_pipeline::Input::bind`]).
    #[tokio::test]
    async fn run_until_shutdown_binds_when_the_caller_did_not() {
        let dir = scratch_dir("run-binds-itself");
        let path = dir.join("app.log");
        std::fs::write(&path, b"line one\n").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["line one"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn tail_in_emits_one_event_per_line_with_read_time_and_log_file_path() {
        let dir = scratch_dir("emit-basic");
        let path = dir.join("app.log");
        std::fs::write(&path, b"line one\nline two\n").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let before = now_nanos();
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 2).await;
        let after = now_nanos();
        assert_eq!(messages(&events), vec!["line one", "line two"]);
        for event in &events {
            assert!(
                event.timestamp >= before && event.timestamp <= after,
                "timestamp should be read time"
            );
            assert_eq!(
                event.attributes.get("log.file.path").and_then(|v| v.as_str()),
                Some(path.to_str().unwrap())
            );
        }

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn read_from_end_skips_preexisting_lines_and_read_from_beginning_replays_them() {
        let dir = scratch_dir("read-from");
        let path = dir.join("app.log");
        std::fs::write(&path, b"old line\n").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer =
            Tailer::new(vec![PathPattern::new(&path)], LineFactory, fast_config(ReadFrom::End));
        // Bound first: the initial scan must open the file at its end before the append, or the
        // append could be skipped.
        let (shutdown_tx, handle) = spawn_bound_tailer(tailer, fanout).await;

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"new line\n")
            .unwrap();

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["new line"], "the pre-existing line must be skipped");
        shutdown(shutdown_tx, handle).await;

        // A fresh tailer over the same file, `read_from: beginning`, must replay everything.
        let (fanout2, mut rx2) = recording_fanout(8);
        let tailer2 = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let (shutdown_tx2, handle2) = spawn_tailer(tailer2, fanout2);
        let events2 = expect_events(&mut rx2, 2).await;
        assert_eq!(messages(&events2), vec!["old line", "new line"]);
        shutdown(shutdown_tx2, handle2).await;

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_file_created_after_startup_is_discovered_and_read_from_the_beginning() {
        let dir = scratch_dir("new-file");
        let (fanout, mut rx) = recording_fanout(8);
        // `read_from: end` governs only files present at startup.
        let tailer = Tailer::new(
            vec![PathPattern::new(dir.join("*.log"))],
            LineFactory,
            fast_config(ReadFrom::End),
        );
        // Bound first: a file already present at the initial scan would be skipped by
        // `read_from: end`.
        let (shutdown_tx, handle) = spawn_bound_tailer(tailer, fanout).await;

        std::fs::write(dir.join("app.log"), b"first\nsecond\n").unwrap();

        let events = expect_events(&mut rx, 2).await;
        assert_eq!(messages(&events), vec!["first", "second"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rotation_by_rename_drains_the_old_inode_then_follows_the_new_one() {
        let dir = scratch_dir("rotation");
        let path = dir.join("app.log");
        std::fs::write(&path, b"before\n").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["before"]);

        // Both land within one scan, so the driver sees a new inode at the path, not a removal.
        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"after\n").unwrap();

        let events2 = expect_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events2),
            vec!["after"],
            "the new inode should be followed from its own beginning"
        );

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A `TailConfig` in which only an inotify wake runs `drain` within a test's timeout: no
    /// 15ms flush tick (`Outcome::Flush` drains too), a 30s poll, and one event per batch, so a
    /// line is emitted by the `drain` that reads it.
    fn inotify_only_config() -> TailConfig {
        let mut config = fast_config(ReadFrom::Beginning);
        config.watch = WatchMode::Inotify;
        config.poll_interval = Duration::from_secs(30);
        config.batching.flush_interval = Duration::from_secs(60);
        config.batching.max_events = 1;
        config
    }

    /// Binds before spawning, so the initial scan has armed every watch before the test writes.
    /// No `drain` follows `bind`, so the test's files start empty and every line arrives by a
    /// wake.
    async fn spawn_bound_tailer(
        mut tailer: Tailer<LineDecoder, LineFactory>,
        sink: Fanout,
    ) -> (watch::Sender<bool>, tokio::task::JoinHandle<anyhow::Result<()>>) {
        tailer.bind().await.expect("bind should succeed");
        spawn_tailer(tailer, sink)
    }

    fn append(path: &Path, bytes: &[u8]) {
        let mut f = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        f.write_all(bytes).unwrap();
    }

    /// Under `inotify`, a rotation's replacement is discovered and read, and then followed by its
    /// own watch, all without the 30s poll.
    #[tokio::test]
    async fn under_inotify_a_rotation_registers_a_fresh_watch_on_the_new_inode() {
        let dir = scratch_dir("inotify-rotation-watch");
        let path = dir.join("app.log");
        std::fs::write(&path, b"").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, inotify_only_config());
        let (shutdown_tx, handle) = spawn_bound_tailer(tailer, fanout).await;

        append(&path, b"before\n");
        let events = tokio::time::timeout(Duration::from_secs(3), expect_events(&mut rx, 1))
            .await
            .expect("the original file's own watch should deliver this well within 3s");
        assert_eq!(messages(&events), vec!["before"]);

        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"after\n").unwrap();

        let events2 = tokio::time::timeout(Duration::from_secs(3), expect_events(&mut rx, 1))
            .await
            .expect("the directory watch should discover the replacement well within 3s");
        assert_eq!(messages(&events2), vec!["after"]);

        append(&path, b"more\n");
        let events3 =
            tokio::time::timeout(Duration::from_secs(3), expect_events(&mut rx, 1)).await.expect(
                "the new inode's own watch should deliver this well within 3s, nowhere near the \
                 30s poll_interval",
            );
        assert_eq!(messages(&events3), vec!["more"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn truncation_seeks_to_zero_and_reports_truncated() {
        let dir = scratch_dir("truncate");
        let path = dir.join("app.log");
        std::fs::write(&path, b"aaaaaaaaaa\n").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["aaaaaaaaaa"]);

        // `O_TRUNC` on the existing path: same inode, shorter length.
        std::fs::write(&path, b"new\n").unwrap();

        let events2 = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events2), vec!["new"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_removed_file_is_drained_and_closed() {
        let dir = scratch_dir("removed");
        let path = dir.join("app.log");
        std::fs::write(&path, b"only line\n").unwrap();

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("tail_in", "tail_in", "listener");

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        )
        .with_telemetry(telemetry);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["only line"]);
        assert_eq!(gauge_value(&registry, "logit.input.files.open"), Some(1.0));

        std::fs::remove_file(&path).unwrap();
        wait_until("files.open to drop to 0 once the removed file is reaped", || {
            gauge_value(&registry, "logit.input.files.open") == Some(0.0)
        })
        .await;

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `logit.input.watch.watches` counts the watched directory plus each open file's watch.
    #[tokio::test]
    async fn watch_watches_counts_the_directory_and_each_open_file() {
        let dir = scratch_dir("watch-count");
        let path = dir.join("app.log");
        std::fs::write(&path, b"line\n").unwrap();

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("tail_in", "tail_in", "listener");

        let (fanout, mut rx) = recording_fanout(8);
        let mut config = fast_config(ReadFrom::Beginning);
        config.watch = WatchMode::Inotify;
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config)
            .with_telemetry(telemetry);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["line"]);
        assert_eq!(
            gauge_value(&registry, "logit.input.watch.watches"),
            Some(2.0),
            "the watched directory plus the one open file"
        );

        std::fs::remove_file(&path).unwrap();
        wait_until(
            "watches to fall back to just the watched directory once the removed file is reaped",
            || gauge_value(&registry, "logit.input.watch.watches") == Some(1.0),
        )
        .await;

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    // -- selection follows the rename (docs/adr/docker-container-identity-and-minimal-watches.md)

    /// A de-selected file stops being read at once; as `Draining` it would never reach EOF.
    #[tokio::test]
    async fn a_deselected_file_stops_emitting_immediately_even_while_still_written_to() {
        let dir = scratch_dir("deselect-stop");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();

        let deselected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory = SelectiveFactory { deselected: deselected.clone() };
        let registry = Registry::new();
        let (fanout, mut rx) = recording_fanout(8);
        let tailer =
            Tailer::new(vec![PathPattern::new(&path)], factory, fast_config(ReadFrom::Beginning))
                .with_telemetry(registry.telemetry_for("tail_in", "tail_in", "listener"));
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["one"]);

        deselected.store(true, std::sync::atomic::Ordering::SeqCst);
        // Written before the reap, the append would be read and fail the negative assertion.
        wait_until("the next scan to notice the de-selection and reap the file", || {
            gauge_value(&registry, "logit.input.files.open") == Some(0.0)
        })
        .await;

        {
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"two\nthree\n").unwrap();
        }

        let nothing = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await;
        assert!(
            nothing.is_err(),
            "a de-selected file must not emit anything further, even while still being written to"
        );

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A re-selected file resumes at its retained offset, delivering what was written while
    /// away; also pins `open_tracked`'s guard against reviving a `Deselected` entry.
    #[tokio::test]
    async fn a_reselected_file_resumes_at_the_retained_offset_rather_than_replaying() {
        let dir = scratch_dir("deselect-resume");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();

        let deselected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory = SelectiveFactory { deselected: deselected.clone() };
        let registry = Registry::new();
        let (fanout, mut rx) = recording_fanout(8);
        let tailer =
            Tailer::new(vec![PathPattern::new(&path)], factory, fast_config(ReadFrom::Beginning))
                .with_telemetry(registry.telemetry_for("tail_in", "tail_in", "listener"));
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["one"]);

        deselected.store(true, std::sync::atomic::Ordering::SeqCst);
        wait_until("the next scan to notice the de-selection and reap the file", || {
            gauge_value(&registry, "logit.input.files.open") == Some(0.0)
        })
        .await;

        // Written while de-selected: deferred, not lost.
        {
            let mut f = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(b"two\n").unwrap();
        }
        let nothing = tokio::time::timeout(Duration::from_millis(100), rx.recv()).await;
        assert!(nothing.is_err(), "still de-selected -- nothing should arrive yet");

        deselected.store(false, std::sync::atomic::Ordering::SeqCst);

        let events2 = expect_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events2),
            vec!["two"],
            "must resume from the retained offset -- \"one\" must never be replayed"
        );

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn checkpoint_is_written_on_interval_only_when_dirty_and_resumes_by_inode() {
        let dir = scratch_dir("checkpoint-resume");
        let path = dir.join("app.log");
        let checkpoint_path = dir.join("checkpoint.json");
        std::fs::write(&path, b"line one\n").unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.checkpoint_interval = Duration::from_millis(25);

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone());
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["line one"]);

        // "line one\n" is 9 bytes; an offset of 0 would be a tick that landed before the read.
        wait_until("an interval tick after the read to write the dirty checkpoint", || {
            checkpointed_offset(&checkpoint_path) == Some(9)
        })
        .await;

        shutdown(shutdown_tx, handle).await;

        // A fresh tailer resumes from the checkpoint: only the appended line arrives.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"line two\n")
            .unwrap();
        let (fanout2, mut rx2) = recording_fanout(8);
        let tailer2 = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (shutdown_tx2, handle2) = spawn_tailer(tailer2, fanout2);
        let events2 = expect_events(&mut rx2, 1).await;
        assert_eq!(messages(&events2), vec!["line two"]);
        shutdown(shutdown_tx2, handle2).await;

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn checkpoint_with_an_offset_past_the_file_size_restarts_at_zero() {
        use std::os::unix::fs::MetadataExt;

        let dir = scratch_dir("checkpoint-overshoot");
        let path = dir.join("app.log");
        std::fs::write(&path, b"short\n").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        let checkpoint_path = dir.join("checkpoint.json");
        std::fs::write(
            &checkpoint_path,
            format!(
                r#"{{"version":1,"files":[{{"dev":{},"ino":{},"path":"{}","offset":999999}}]}}"#,
                meta.dev(),
                meta.ino(),
                path.display(),
            ),
        )
        .unwrap();

        let mut config = fast_config(ReadFrom::End); // ignored: the resume entry wins
        config.checkpoint_path = Some(checkpoint_path);

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["short"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn checkpoint_prunes_entries_for_missing_paths_on_write() {
        let dir = scratch_dir("checkpoint-prune-driver");
        let a_path = dir.join("a.log");
        let b_path = dir.join("b.log");
        std::fs::write(&a_path, b"a1\n").unwrap();
        std::fs::write(&b_path, b"b1\n").unwrap();
        let checkpoint_path = dir.join("checkpoint.json");

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.checkpoint_interval = Duration::from_millis(20);

        let registry = Registry::new();
        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&a_path), PathPattern::new(&b_path)],
            LineFactory,
            config,
        )
        .with_telemetry(registry.telemetry_for("tail_in", "tail_in", "listener"));
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let _ = expect_events(&mut rx, 2).await;
        wait_until("a checkpoint tick to record both files", || {
            std::fs::read_to_string(&checkpoint_path)
                .is_ok_and(|text| text.contains("a.log") && text.contains("b.log"))
        })
        .await;

        std::fs::remove_file(&b_path).unwrap();
        // Shutdown's forced write drops `b.log` too, but only if the reap came first.
        wait_until("the removed file to be reaped", || {
            gauge_value(&registry, "logit.input.files.open") == Some(1.0)
        })
        .await;

        shutdown(shutdown_tx, handle).await;

        let text = std::fs::read_to_string(&checkpoint_path).unwrap();
        assert!(text.contains("a.log"), "a.log should still be checkpointed");
        assert!(!text.contains("b.log"), "b.log should have been pruned after removal");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn shutdown_flushes_every_accumulator_and_writes_the_checkpoint_within_grace() {
        let dir = scratch_dir("shutdown-flush");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\nthree\n").unwrap();
        let checkpoint_path = dir.join("checkpoint.json");

        let mut config = fast_config(ReadFrom::Beginning);
        // Only shutdown's final flush may deliver. The checkpoint interval is pushed out too,
        // because a checkpoint tick flushes before it writes.
        config.batching.max_events = 1_000;
        config.batching.flush_interval = Duration::from_secs(60);
        config.checkpoint_interval = Duration::from_secs(60);
        config.checkpoint_path = Some(checkpoint_path.clone());

        let registry = Registry::new();
        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config)
            .with_telemetry(registry.telemetry_for("tail_in", "tail_in", "listener"));
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        // Shutdown before the read would leave nothing to flush and make the negative assertion
        // vacuous. `counter_total` drains, so the total accumulates across polls.
        let mut lines = 0.0;
        wait_until("the initial scan and read to take all three lines", || {
            lines += counter_total(&registry, "logit.input.lines");
            lines >= 3.0
        })
        .await;
        assert!(
            rx.try_recv().is_err(),
            "nothing should have flushed yet -- both intervals are 60s away"
        );

        shutdown(shutdown_tx, handle).await;

        let delivered = rx.try_recv().expect("shutdown should have flushed the buffered batch");
        let events = unwrap_batch(delivered).events;
        assert_eq!(messages(&events), vec!["one", "two", "three"]);
        assert!(checkpoint_path.exists(), "shutdown should force a checkpoint write");

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Sums counter `name` tagged `tag`. Drains `registry`, so it's a one-shot check.
    fn counter_sum(registry: &Registry, name: &str, tag: (&str, &str)) -> f64 {
        registry
            .drain(0)
            .iter()
            .filter(|e| e.attributes.get(tag.0).and_then(logit_core::Value::as_str) == Some(tag.1))
            .flat_map(|e| &e.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == name)
            .map(|m| match &m.kind {
                MetricKind::Sum(sum) => sum.value,
                other => panic!("{name} must be a counter, got {other:?}"),
            })
            .sum()
    }

    /// Polls `cond` every 5ms for up to 5s.
    async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while !cond() {
            assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// The offset the on-disk checkpoint records for its first file, once one is written.
    fn checkpointed_offset(path: &Path) -> Option<u64> {
        let text = std::fs::read_to_string(path).ok()?;
        serde_json::from_str::<serde_json::Value>(&text).ok()?["files"][0]["offset"].as_u64()
    }

    /// Decision 4 of `docs/adr/durable-checkpoint-writes-and-fault-injection.md`: a checkpoint
    /// that exists but can't be used says a previous run read these files, so skipping to their
    /// end under `read_from: end` would drop whatever they gained while `logit` was down. Each
    /// shape here is what a power loss or a crash mid-write can leave behind.
    #[tokio::test]
    async fn an_unusable_checkpoint_starts_every_preexisting_file_at_the_beginning_even_under_read_from_end(
    ) {
        // `(label, bytes, written at the tmp path rather than the checkpoint path)`.
        let cases: [(&str, &[u8], bool); 4] = [
            ("empty", b"", false),
            ("truncated", br#"{"version":1,"files":[{"dev":"#, false),
            ("wrong-version", br#"{"version":99,"files":[]}"#, false),
            ("stray-tmp", br#"{"version":1,"#, true),
        ];
        for (label, bytes, at_tmp) in cases {
            let dir = scratch_dir(&format!("unusable-checkpoint-{label}"));
            let a_path = dir.join("a.log");
            let b_path = dir.join("b.log");
            std::fs::write(&a_path, b"a1\na2\n").unwrap();
            std::fs::write(&b_path, b"b1\n").unwrap();
            let checkpoint_path = dir.join("checkpoint.json");
            let written = if at_tmp {
                logit_pipeline::atomic_write::tmp_path(&checkpoint_path)
            } else {
                checkpoint_path.clone()
            };
            std::fs::write(written, bytes).unwrap();

            let mut config = fast_config(ReadFrom::End);
            config.checkpoint_path = Some(checkpoint_path.clone());
            let registry = Registry::new();
            let telemetry = registry.telemetry_for("tail_in", "tail_in", "listener");
            let diag = Diagnostics::new("test");
            let (fanout, mut rx) = recording_fanout(8);
            let tailer = Tailer::new(
                vec![PathPattern::new(&a_path), PathPattern::new(&b_path)],
                LineFactory,
                config,
            )
            .with_diagnostics(diag.clone())
            .with_telemetry(telemetry);
            let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

            let mut got = messages(&expect_events(&mut rx, 3).await);
            got.sort();
            assert_eq!(got, vec!["a1", "a2", "b1"], "{label}: every pre-existing line replays");
            shutdown(shutdown_tx, handle).await;

            assert_eq!(
                counter_sum(&registry, "logit.input.checkpoint.errors", ("op", "load")),
                1.0,
                "{label}"
            );
            assert!(diag.occurrences("checkpoint_error") >= 1, "{label}");
            // Shutdown's forced write replaces the unusable document with a good one.
            let text = std::fs::read_to_string(&checkpoint_path).unwrap();
            assert!(text.contains("a.log") && text.contains("b.log"), "{label}: {text}");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// The other half of decision 4: no checkpoint and no tmp beside it is a first run, so
    /// `read_from` still decides.
    #[tokio::test]
    async fn a_missing_checkpoint_still_honours_read_from_end() {
        let dir = scratch_dir("missing-checkpoint-read-from-end");
        let path = dir.join("app.log");
        std::fs::write(&path, b"old line\n").unwrap();
        let mut config = fast_config(ReadFrom::End);
        config.checkpoint_path = Some(dir.join("checkpoint.json"));
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("tail_in", "tail_in", "listener");
        let diag = Diagnostics::new("test");

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config)
            .with_diagnostics(diag.clone())
            .with_telemetry(telemetry);
        // Bound first: the initial scan must open the file at its end before the append, or the
        // append could be skipped.
        let (shutdown_tx, handle) = spawn_bound_tailer(tailer, fanout).await;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"new line\n")
            .unwrap();

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["new line"], "the pre-existing line must be skipped");
        shutdown(shutdown_tx, handle).await;

        assert_eq!(counter_sum(&registry, "logit.input.checkpoint.errors", ("op", "load")), 0.0);
        assert_eq!(diag.occurrences("checkpoint_error"), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A `kill -9` after the new checkpoint's tmp file is written but before it's renamed over
    /// the old one. The restart must resume from the old checkpoint, so the lines read since are
    /// delivered again (duplicates), and nothing is skipped even under `read_from: end`.
    #[tokio::test]
    async fn a_crash_between_checkpoint_write_and_rename_resumes_from_the_previous_checkpoint_with_duplicates_only(
    ) {
        use logit_pipeline::fault::{self, sites, Op, Point};

        let dir = scratch_dir("checkpoint-crash-before-rename");
        let path = dir.join("app.log");
        let checkpoint_path = dir.join("checkpoint.json");
        std::fs::write(&path, b"one\n").unwrap();
        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.checkpoint_interval = Duration::from_millis(20);

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone());
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);
        assert_eq!(messages(&expect_events(&mut rx, 1).await), vec!["one"]);
        wait_until("the first checkpoint to cover \"one\"", || {
            std::fs::read_to_string(&checkpoint_path).is_ok_and(|t| t.contains("\"offset\": 4"))
        })
        .await;

        let scope = fault::scope(&dir);
        scope.crash_at(Point::new(sites::TAIL_CHECKPOINT, Op::Rename), 1);
        std::fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(b"two\n").unwrap();
        assert_eq!(messages(&expect_events(&mut rx, 1).await), vec!["two"]);
        wait_until("the next checkpoint write to reach its rename", || scope.crashed()).await;
        // The process is dead from here on: shutdown's forced write fails under the freeze too.
        shutdown(shutdown_tx, handle).await;
        scope.revive();
        drop(scope);

        let text = std::fs::read_to_string(&checkpoint_path).unwrap();
        assert!(text.contains("\"offset\": 4"), "the previous checkpoint survives: {text}");

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"three\n")
            .unwrap();
        config.read_from = ReadFrom::End; // the resume entry wins over it
        let (fanout2, mut rx2) = recording_fanout(8);
        let tailer2 = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (shutdown_tx2, handle2) = spawn_tailer(tailer2, fanout2);
        assert_eq!(
            messages(&expect_events(&mut rx2, 2).await),
            vec!["two", "three"],
            "\"two\" is delivered again, \"one\" is not, and nothing is skipped"
        );
        shutdown(shutdown_tx2, handle2).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn an_unterminated_last_line_is_held_until_its_newline_arrives_and_emitted_on_close() {
        let dir = scratch_dir("unterminated");
        let path = dir.join("app.log");
        std::fs::write(&path, b"complete\nno newline yet").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["complete"]);

        // Held while unterminated.
        let more = tokio::time::timeout(Duration::from_millis(80), rx.recv()).await;
        assert!(more.is_err(), "an unterminated line must not be emitted before it closes");

        // Shutdown emits the held partial.
        shutdown(shutdown_tx, handle).await;
        let delivered = rx.try_recv().expect("the held partial line should flush on shutdown");
        let events2 = unwrap_batch(delivered).events;
        assert_eq!(messages(&events2), vec!["no newline yet"]);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn downstream_backpressure_pauses_reading_without_loss() {
        let dir = scratch_dir("backpressure");
        let path = dir.join("app.log");
        let content: String = (0..50).map(|i| format!("line-{i}\n")).collect();
        std::fs::write(&path, content.as_bytes()).unwrap();

        let (tx, mut rx) = mpsc::channel(1); // tiny capacity forces backpressure
        let fanout = Fanout::new(vec![tx]);
        let mut config = fast_config(ReadFrom::Beginning);
        config.batching.max_events = 1; // one event per batch -- easy to force a stall
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        // Don't read `rx` yet: the tailer must stall, not drop.
        tokio::time::sleep(Duration::from_millis(60)).await;

        let mut received = Vec::new();
        while received.len() < 50 {
            let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("timed out")
                .expect("channel closed");
            received.extend(unwrap_batch(delivered).events);
        }
        assert_eq!(
            messages(&received),
            (0..50).map(|i| format!("line-{i}")).collect::<Vec<_>>(),
            "every line should still arrive, in order, once downstream drains"
        );

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn invalid_utf8_is_lossily_converted_and_diagnosed() {
        let dir = scratch_dir("invalid-utf8");
        let path = dir.join("app.log");
        let mut content = b"good\n".to_vec();
        content.extend_from_slice(b"\xff\xfebad\n"); // invalid UTF-8 before the newline
        std::fs::write(&path, &content).unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 2).await;
        assert_eq!(events[0].log.as_ref().unwrap().message.as_str(), Some("good"));
        let second = events[1].log.as_ref().unwrap().message.as_str().unwrap().to_string();
        assert!(
            second.contains('\u{FFFD}'),
            "invalid UTF-8 should become the replacement character"
        );
        assert!(second.ends_with("bad"));

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn two_files_are_read_round_robin_so_a_busy_file_cannot_starve_the_other() {
        let dir = scratch_dir("round-robin");
        let busy_path = dir.join("busy.log");
        let quick_path = dir.join("quick.log");
        // More than one 64 KiB read chunk, so reading one file to EOF first would put
        // `quick.log`'s line after all of these.
        let busy_content: String = (0..8_000).map(|i| format!("busy-{i:05}\n")).collect();
        assert!(busy_content.len() > 64 * 1024, "fixture must exceed one read chunk");
        std::fs::write(&busy_path, busy_content.as_bytes()).unwrap();
        std::fs::write(&quick_path, b"quick line\n").unwrap();

        let (fanout, mut rx) = recording_fanout(64);
        let mut config = fast_config(ReadFrom::Beginning);
        config.batching.max_events = 1;
        let tailer = Tailer::new(
            vec![PathPattern::new(&busy_path), PathPattern::new(&quick_path)],
            LineFactory,
            config,
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        // Consume everything: stopping at "quick line" would leave the tailer blocked on a full
        // channel.
        let mut seen_before_quick = 0usize;
        let mut quick_found = false;
        let mut busy_seen = 0usize;
        while busy_seen < 8_000 || !quick_found {
            let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("timed out")
                .expect("channel closed");
            for event in unwrap_batch(delivered).events {
                if event.log.as_ref().unwrap().message.as_str() == Some("quick line") {
                    quick_found = true;
                } else {
                    busy_seen += 1;
                    if !quick_found {
                        seen_before_quick += 1;
                    }
                }
            }
        }
        assert!(
            seen_before_quick < 8_000,
            "quick.log's line should arrive well before busy.log fully drains, not after \
             (saw {seen_before_quick} busy events first)"
        );

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Under `inotify`, a new file is discovered long before the 30s `poll_interval`, so only the
    /// wake source can explain it.
    #[tokio::test]
    async fn under_inotify_a_new_file_is_discovered_well_before_the_poll_interval() {
        let dir = scratch_dir("inotify-latency");

        let (fanout, mut rx) = recording_fanout(8);
        let mut config = fast_config(ReadFrom::End);
        config.watch = WatchMode::Inotify;
        config.poll_interval = Duration::from_secs(30);
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config);
        // Bound first: the initial scan must arm the directory watch before the write.
        let (shutdown_tx, handle) = spawn_bound_tailer(tailer, fanout).await;

        std::fs::write(dir.join("app.log"), b"woke\n").unwrap();

        let events =
            tokio::time::timeout(Duration::from_secs(3), expect_events(&mut rx, 1)).await.expect(
                "inotify should discover the new file well within 3s, nowhere near the 30s \
                 poll_interval",
            );
        assert_eq!(messages(&events), vec!["woke"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Under `poll`, a new file waits for the `poll_interval` tick. The 15ms flush timer can't
    /// discover a file (`Outcome::Flush` never scans), so it can't make this pass by accident.
    #[tokio::test]
    async fn under_poll_a_new_file_is_discovered_only_after_the_poll_interval() {
        let dir = scratch_dir("poll-latency");

        let (fanout, mut rx) = recording_fanout(8);
        let mut config = fast_config(ReadFrom::End);
        config.watch = WatchMode::Poll;
        config.poll_interval = Duration::from_millis(300);
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config);
        // Bound first: a file already present at the initial scan would be skipped by
        // `read_from: end`.
        let (shutdown_tx, handle) = spawn_bound_tailer(tailer, fanout).await;

        std::fs::write(dir.join("app.log"), b"woke\n").unwrap();

        // Nothing before the 300ms tick.
        let too_soon = tokio::time::timeout(Duration::from_millis(150), rx.recv()).await;
        assert!(
            too_soon.is_err(),
            "poll mode must not discover a new file before its own poll_interval tick"
        );

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["woke"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Under `inotify`, a write to a tracked file arrives well inside the 30s `poll_interval`.
    /// An `O_APPEND` write raises only `IN_MODIFY` on the file's own watch, and
    /// [`inotify_only_config`] leaves no other `drain` within the timeout, so only `Wake::Data`
    /// can deliver it.
    #[tokio::test]
    async fn under_inotify_a_write_to_an_already_tracked_file_is_delivered_promptly() {
        let dir = scratch_dir("inotify-data-wake");
        let path = dir.join("app.log");
        std::fs::write(&path, b"").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(dir.join("*.log"))],
            LineFactory,
            inotify_only_config(),
        );
        let (shutdown_tx, handle) = spawn_bound_tailer(tailer, fanout).await;

        append(&path, b"first\n");
        let first = tokio::time::timeout(Duration::from_secs(3), expect_events(&mut rx, 1))
            .await
            .expect("the file's own watch should deliver this well within 3s");
        assert_eq!(messages(&first), vec!["first"]);

        append(&path, b"second\n");
        let second =
            tokio::time::timeout(Duration::from_secs(3), expect_events(&mut rx, 1)).await.expect(
                "a write to an already-tracked file should be delivered well within 3s, nowhere \
                 near the 30s poll_interval, via the file's own watch",
            );
        assert_eq!(messages(&second), vec!["second"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Under `inotify`, an in-place truncation is noticed through the file's own `IN_MODIFY`:
    /// `DIR_MASK` has no content bits, and `drain` alone never rewinds.
    ///
    /// The truncation's wake is handled on its own before the replacement is written, and the
    /// replacement is longer than the original. If that wake rewinds the file, the tailer reads
    /// from `0` and delivers the whole replacement line. If it doesn't, the write's wake sees a
    /// length at or past the old offset, doesn't rewind, and the tailer delivers the fragment past
    /// that offset. A replacement shorter than the original would let the write's own wake
    /// rewind, hiding which wake did it.
    #[tokio::test]
    async fn under_inotify_a_truncation_is_noticed_via_the_files_own_watch() {
        let dir = scratch_dir("inotify-truncate-data-wake");
        let path = dir.join("app.log");
        std::fs::write(&path, b"").unwrap();

        let registry = Registry::new();
        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(dir.join("*.log"))],
            LineFactory,
            inotify_only_config(),
        )
        .with_telemetry(registry.telemetry_for("tail_in", "tail_in", "listener"));
        let (shutdown_tx, handle) = spawn_bound_tailer(tailer, fanout).await;

        append(&path, b"first\n");
        let first = tokio::time::timeout(Duration::from_secs(3), expect_events(&mut rx, 1))
            .await
            .expect("the file's own watch should deliver this well within 3s");
        assert_eq!(messages(&first), vec!["first"]);

        // `ftruncate(2)`: same inode, length 0, one `IN_MODIFY`.
        std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(0).unwrap();
        // `counter_total` drains, so the total accumulates across polls.
        let mut truncated = 0.0;
        wait_until("the truncation's own wake to be handled before the replacement", || {
            truncated += counter_total(&registry, "logit.input.files.truncated");
            truncated >= 1.0
        })
        .await;
        append(&path, b"a-much-longer-replacement-line\n");

        let second =
            tokio::time::timeout(Duration::from_secs(3), expect_events(&mut rx, 1)).await.expect(
                "the replacement should be delivered well within 3s via the file's own watch, \
                 nowhere near the 30s poll_interval",
            );
        assert_eq!(
            messages(&second),
            vec!["a-much-longer-replacement-line"],
            "the truncation's own wake should have rewound the file to 0"
        );
        truncated += counter_total(&registry, "logit.input.files.truncated");
        assert_eq!(truncated, 1.0);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A `Wake::Data` for a path a new inode now owns isn't read as a truncation of the old one
    /// (`Tailer::on_data_wake`). Driven by hand: only a data wake handled before the next `scan`
    /// exposes the difference.
    #[tokio::test]
    async fn a_data_wake_for_a_path_a_new_inode_now_owns_is_not_a_truncation() {
        let dir = scratch_dir("data-wake-rotation");
        let path = dir.join("app.log");
        // Longer than the replacement, so a stale-`id` reconcile would see `len < offset`.
        std::fs::write(&path, b"a-longer-first-line\n").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut watcher = crate::tail::watch::Watcher::Poll;
        let mut tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        tailer.scan(true, &mut watcher).await;
        let _ = tailer.drain(&fanout, &shutdown_rx, &mut watcher, no_timer_due()).await;
        tailer.flush_all(&fanout, FlushReason::Interval).await;
        assert_eq!(messages(&expect_events(&mut rx, 1).await), vec!["a-longer-first-line"]);

        let old = FileId::from_metadata(&std::fs::metadata(&path).unwrap());
        let read_offset = tailer.files.get(&old).unwrap().offset;
        assert_eq!(read_offset, 20, "the whole first line should already have been read");

        // Rotate with no `scan` in between: the window a queued `Wake::Data` lands in.
        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"new\n").unwrap();
        assert_ne!(
            FileId::from_metadata(&std::fs::metadata(&path).unwrap()),
            old,
            "test is vacuous if the filesystem reused the rotated-away file's inode"
        );

        tailer.on_data_wake(&path).await;
        assert_eq!(
            tailer.files.get(&old).unwrap().offset,
            read_offset,
            "a data wake naming a path another inode now owns must not rewind the tracked file"
        );

        // The drain after every wake re-emits nothing.
        let _ = tailer.drain(&fanout, &shutdown_rx, &mut watcher, no_timer_due()).await;
        tailer.flush_all(&fanout, FlushReason::Interval).await;
        assert!(
            rx.try_recv().is_err(),
            "the rotated-away file's already-read content must not be re-emitted"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_draining_file_with_more_than_one_chunk_of_backlog_is_fully_read_before_close() {
        let dir = scratch_dir("drain-backlog");
        let path = dir.join("app.log");
        let content: String = (0..8_000).map(|i| format!("drain-{i:05}\n")).collect();
        assert!(content.len() > 64 * 1024, "fixture must exceed one read chunk");
        std::fs::write(&path, content.as_bytes()).unwrap();

        // A capacity-1 fanout and one event per batch stall the driver in `emit` almost at once,
        // so most of the file is unread when it's removed.
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let mut config = fast_config(ReadFrom::Beginning);
        config.batching.max_events = 1;
        // A glob, so the removal is what drives the file into `FileState::Draining`.
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config);
        // Bound first: the initial scan must open the file before it's removed, or the glob never
        // finds it. `rx` stays unconsumed.
        let (shutdown_tx, handle) = spawn_bound_tailer(tailer, fanout).await;
        std::fs::remove_file(&path).unwrap();
        // The first poll tick marks the file `Draining`. `drain` yields to a due tick after its
        // first pass at the latest, so at least one chunk is still unread when that happens.
        tokio::time::sleep(Duration::from_millis(60)).await;

        let mut received = Vec::new();
        while received.len() < 8_000 {
            let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("timed out waiting for the backlog to fully drain")
                .expect("channel closed");
            received.extend(unwrap_batch(delivered).events);
        }
        assert_eq!(
            messages(&received),
            (0..8_000).map(|i| format!("drain-{i:05}")).collect::<Vec<_>>(),
            "a Draining file with more than one chunk of backlog must be read to real EOF, not \
             reaped after a single 64KiB chunk"
        );

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_truncation_discards_the_partial_line_held_from_the_previous_generation() {
        let dir = scratch_dir("truncate-partial");
        let path = dir.join("app.log");
        // "partialpartial" is held as a partial, and makes the offset (18) exceed the
        // post-truncation length (10), so `scan` sees a truncation.
        std::fs::write(&path, b"a\nb\npartialpartial").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 2).await;
        assert_eq!(messages(&events), vec!["a", "b"]);

        // `O_TRUNC` on the existing path: same inode, 10 < 18 bytes.
        std::fs::write(&path, b"restarted\n").unwrap();

        let events2 = expect_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events2),
            vec!["restarted"],
            "the pre-truncation partial must not be spliced onto the first post-truncation line"
        );

        shutdown(shutdown_tx, handle).await;
        assert!(rx.try_recv().is_err(), "the discarded partial must not resurface on close");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_wildcard_matching_both_a_rotated_file_and_its_replacement_keeps_one_entry_per_inode()
    {
        let dir = scratch_dir("wildcard-rotation");
        let path = dir.join("app.log");
        std::fs::write(&path, b"before\n").unwrap();

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(dir.join("app.log*"))],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let events = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["before"]);

        // `app.log*` now matches both the rotated-away file and its replacement.
        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"after\n").unwrap();

        let events2 = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events2), vec!["after"]);

        // `app.log.1` must be rebound, not reopened at byte 0 (re-emitting "before").
        let extra = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(extra.is_err(), "the renamed inode must not be re-read from byte 0");

        // Rebound and still `Active`: an append under the new name is followed.
        std::fs::OpenOptions::new()
            .append(true)
            .open(dir.join("app.log.1"))
            .unwrap()
            .write_all(b"late\n")
            .unwrap();
        let events3 = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events3), vec!["late"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A rebind in `open_tracked` never removes a `by_path` entry another inode owns. Checked on
    /// state, not events: the next `scan` would re-add an orphaned binding, hiding the difference.
    #[tokio::test]
    async fn rebinding_a_renamed_inode_never_removes_a_by_path_entry_another_inode_now_owns() {
        let dir = scratch_dir("rebind-by-path-ownership");
        let path = dir.join("app.log");
        std::fs::write(&path, b"before\n").unwrap();

        let mut tailer = Tailer::new(
            vec![PathPattern::new(dir.join("app.log*"))],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        tailer.scan(true, &mut crate::tail::watch::Watcher::Poll).await;
        let a = FileId::from_metadata(&std::fs::metadata(dir.join("app.log")).unwrap());
        assert_eq!(tailer.by_path.get(&dir.join("app.log")), Some(&a));

        std::fs::rename(dir.join("app.log"), dir.join("app.log.1")).unwrap();
        std::fs::write(dir.join("app.log"), b"after\n").unwrap();
        let b = FileId::from_metadata(&std::fs::metadata(dir.join("app.log")).unwrap());
        assert_ne!(a, b, "test is vacuous if the filesystem reused the old inode for the new file");

        // Replay the `discovered` order that matters: the replacement arm for `app.log` (inode
        // `b`) before the rebind arm for `app.log.1` (inode `a`). A real `scan`'s `HashMap`
        // order may be either.
        tailer.files.get_mut(&a).unwrap().state = FileState::Draining;
        tailer.by_path.remove(&dir.join("app.log"));
        let mut watcher = crate::tail::watch::Watcher::Poll;
        tailer.open_tracked(dir.join("app.log"), b, StartOffset::Beginning, &mut watcher).await;
        tailer.open_tracked(dir.join("app.log.1"), a, StartOffset::Beginning, &mut watcher).await;

        assert_eq!(
            tailer.by_path.get(&dir.join("app.log")),
            Some(&b),
            "the replacement inode's by_path entry must survive the rebind of the renamed inode"
        );
        assert_eq!(tailer.by_path.get(&dir.join("app.log.1")), Some(&a));
        assert_eq!(tailer.by_path.len(), 2);
        assert_eq!(tailer.files.len(), 2);
        for (id, f) in &tailer.files {
            if f.state == FileState::Active {
                assert_eq!(
                    tailer.by_path.get(&f.path),
                    Some(id),
                    "an Active tracked file orphaned from by_path can never be marked Draining or reaped"
                );
            }
        }
        assert_eq!(tailer.files.get(&a).unwrap().state, FileState::Active);

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn an_interval_checkpoint_flushes_before_writing_its_offset() {
        let dir = scratch_dir("checkpoint-flush-order");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\nthree\n").unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        // Only the checkpoint tick's flush can deliver within the test.
        config.batching.flush_interval = Duration::from_secs(60);
        config.batching.max_events = 1_000;
        config.checkpoint_interval = Duration::from_millis(25);
        config.checkpoint_path = Some(dir.join("checkpoint.json"));

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        // The flush timer is 60s out, so only the checkpoint tick can deliver within 5s.
        let events = expect_events(&mut rx, 3).await;
        assert_eq!(messages(&events), vec!["one", "two", "three"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_checkpoint_offset_never_covers_a_line_still_held_as_a_partial() {
        let dir = scratch_dir("checkpoint-partial");
        let path = dir.join("app.log");
        // 14 bytes of complete lines, then an unterminated line the offset must not cover.
        const COMPLETE_PREFIX_LEN: usize = 14;
        std::fs::write(&path, b"one\ntwo\nthree\nnot-terminated").unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(dir.join("checkpoint.json"));
        let checkpoint_path = config.checkpoint_path.clone().unwrap();
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (fanout, mut rx) = recording_fanout(8);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let _ = expect_events(&mut rx, 3).await;
        // A tick can land before the first read and write offset 0, so wait for a nonzero one.
        wait_until("a checkpoint written after the read", || {
            checkpointed_offset(&checkpoint_path).is_some_and(|offset| offset > 0)
        })
        .await;

        // Read while running: shutdown emits the partial and legitimately advances the offset.
        assert_eq!(
            checkpointed_offset(&checkpoint_path),
            Some(COMPLETE_PREFIX_LEN as u64),
            "the checkpoint must not cover the unterminated trailing line"
        );

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn the_truncated_diagnostic_reports_the_pre_truncation_offset() {
        let msg = truncated_message(Path::new("/var/log/app.log"), 4096, 12);
        assert!(msg.contains("from an offset of 4096 bytes to 12"), "{msg}");
        assert!(!msg.contains("offset of 0 bytes"));
    }

    // -- the pattern directory's watch is re-armed on every scan -----------------------------

    /// A `Tailer` and a real `inotify` `Watcher`, driven by hand: these tests turn on what a
    /// second `scan` leaves armed, and under the 30s `poll_interval` none would run otherwise.
    #[cfg(target_os = "linux")]
    fn inotify_tailer(
        pattern: PathPattern,
        registry: &Registry,
    ) -> (Tailer<LineDecoder, LineFactory>, crate::tail::watch::Watcher) {
        let diag = Diagnostics::new("tail_in")
            .with_telemetry(registry.telemetry_for("tail_in", "tail_in", "listener"));
        let mut config = fast_config(ReadFrom::Beginning);
        config.watch = WatchMode::Inotify;
        config.poll_interval = Duration::from_secs(30);
        let tailer = Tailer::new(vec![pattern], LineFactory, config)
            .with_telemetry(registry.telemetry_for("tail_in", "tail_in", "listener"))
            .with_diagnostics(diag);
        let watcher =
            crate::tail::watch::Watcher::new(WatchMode::Inotify, &mut Diagnostics::default())
                .expect("inotify should be available in the dev container");
        (tailer, watcher)
    }

    /// Whether a `Diagnostics` key was reported; `warn_throttled` counts
    /// `logit.component.diagnostics{key}` on every occurrence, logged or not.
    #[cfg(target_os = "linux")]
    fn diagnosed(registry: &Registry, key: &str) -> bool {
        registry.drain(0).into_iter().any(|event| {
            event
                .metrics
                .iter()
                .any(|m| logit_core::interner::resolve(m.name) == "logit.component.diagnostics")
                && event.attributes.get("key").and_then(|v| v.as_str()) == Some(key)
        })
    }

    /// Proves a directory watch is live: creating `path` must yield its `Wake::Discover` off the
    /// real fd. Skips a few leftover wakes from the test's own directory churn.
    #[cfg(target_os = "linux")]
    async fn expect_discover(watcher: &mut crate::tail::watch::Watcher, path: &Path) {
        std::fs::write(path, b"hello\n").unwrap();
        let want = crate::tail::watch::Wake::Discover(path.to_path_buf());
        for _ in 0..8 {
            let wake = tokio::time::timeout(Duration::from_secs(3), watcher.next_wake())
                .await
                .unwrap_or_else(|_| {
                    panic!("no wake for {}; the directory watch is not live", path.display())
                });
            if wake == want {
                return;
            }
        }
        panic!("never saw {want:?} among this watcher's wakes");
    }

    /// A pattern directory missing at bind is diagnosed, not recorded as watched, and armed once
    /// it exists.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn under_inotify_a_pattern_directory_missing_at_bind_is_armed_once_it_exists() {
        let dir = scratch_dir("inotify-late-dir");
        let sub = dir.join("sub"); // not created yet
        let registry = Registry::new();
        let (mut tailer, mut watcher) =
            inotify_tailer(PathPattern::new(sub.join("*.log")), &registry);

        tailer.scan(true, &mut watcher).await;
        assert_eq!(
            watcher.tracked_watch_count(),
            0,
            "there is nothing to watch yet -- and nothing may be recorded as watched either"
        );
        assert!(tailer.watched_dirs.is_empty(), "{:?}", tailer.watched_dirs);
        assert!(
            diagnosed(&registry, "watch_dir_error"),
            "a directory that could not be watched must say so, not fail silently"
        );

        std::fs::create_dir_all(&sub).unwrap();
        tailer.scan(false, &mut watcher).await;

        assert_eq!(watcher.tracked_watch_count(), 1, "the directory exists now; arm it");
        assert!(tailer.watched_dirs.contains(&sub));
        expect_discover(&mut watcher, &sub.join("app.log")).await;

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A watched directory deleted and recreated is re-armed on the new inode. Needs both the
    /// every-scan re-arm and the `IN_IGNORED` purge, which stops a dead `wd` short-circuiting it.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn under_inotify_a_watched_directory_deleted_and_recreated_is_rearmed() {
        let dir = scratch_dir("inotify-dir-recreated");
        let registry = Registry::new();
        let (mut tailer, mut watcher) =
            inotify_tailer(PathPattern::new(dir.join("*.log")), &registry);

        tailer.scan(true, &mut watcher).await;
        assert_eq!(watcher.tracked_watch_count(), 1);
        expect_discover(&mut watcher, &dir.join("first.log")).await;

        // No `.await` between the two, so the driver can't observe the gap.
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::create_dir_all(&dir).unwrap();

        tailer.scan(false, &mut watcher).await;
        assert_eq!(watcher.tracked_watch_count(), 1, "the replacement must be watched");
        assert!(tailer.watched_dirs.contains(&dir));
        expect_discover(&mut watcher, &dir.join("second.log")).await;

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A watched directory renamed away and replaced is re-armed. A rename raises no
    /// `IN_IGNORED` (the watch stays valid on the moved inode), so only `IN_MOVE_SELF` in
    /// `DIR_MASK` reports it.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn under_inotify_a_watched_directory_renamed_away_and_replaced_is_rearmed() {
        let parent = scratch_dir("inotify-dir-renamed");
        let dir = parent.join("live");
        std::fs::create_dir_all(&dir).unwrap();
        let registry = Registry::new();
        let (mut tailer, mut watcher) =
            inotify_tailer(PathPattern::new(dir.join("*.log")), &registry);

        tailer.scan(true, &mut watcher).await;
        expect_discover(&mut watcher, &dir.join("first.log")).await;

        std::fs::rename(&dir, parent.join("live.old")).unwrap();
        std::fs::create_dir_all(&dir).unwrap();

        tailer.scan(false, &mut watcher).await;
        assert_eq!(watcher.tracked_watch_count(), 1, "one watch, on the new directory");
        expect_discover(&mut watcher, &dir.join("second.log")).await;

        std::fs::remove_dir_all(&parent).ok();
    }

    /// After 20 rotate-and-delete cycles, the kernel's live watch count (`inotify wd:` lines in
    /// `/proc/self/fdinfo/<fd>`) matches the watcher's and the driver's bookkeeping; a leak in
    /// either direction grows with the cycle count.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_live_kernel_watch_count_matches_this_watchers_own_bookkeeping() {
        let dir = scratch_dir("inotify-watch-leak");
        let registry = Registry::new();
        let (mut tailer, mut watcher) =
            inotify_tailer(PathPattern::new(dir.join("*.log")), &registry);
        let (fanout, _rx) = recording_fanout(256);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let path = dir.join("app.log");

        tailer.scan(true, &mut watcher).await;

        for i in 0..20 {
            std::fs::write(&path, format!("line {i}\n")).unwrap();
            tailer.scan(false, &mut watcher).await;
            tailer.drain(&fanout, &shutdown_rx, &mut watcher, no_timer_due()).await;

            // Rotate out of `*.log`'s reach, reap, then delete: one watch added and one removed
            // per cycle, plus a queued `IN_IGNORED`.
            std::fs::rename(&path, dir.join("app.log.1")).unwrap();
            tailer.scan(false, &mut watcher).await;
            tailer.drain(&fanout, &shutdown_rx, &mut watcher, no_timer_due()).await;
            std::fs::remove_file(dir.join("app.log.1")).unwrap();
        }

        let fd = watcher.inotify_fd().expect("inotify mode has an fd");
        let fdinfo = match std::fs::read_to_string(format!("/proc/self/fdinfo/{fd}")) {
            Ok(contents) => contents,
            Err(err) => {
                println!("skipping: cannot read /proc/self/fdinfo here ({err})");
                return;
            }
        };
        let live = fdinfo.lines().filter(|line| line.starts_with("inotify wd:")).count();

        let file_watches = tailer.files.values().filter(|f| f.watch.is_some()).count();
        let expected = tailer.watched_dirs.len() + file_watches;
        assert_eq!(
            watcher.tracked_watch_count(),
            expected,
            "the watcher's own map must hold exactly the directory and the still-open files"
        );
        assert_eq!(
            live, expected,
            "after 20 rotate-and-delete cycles the kernel still holds {live} watches for \
             {expected} the driver knows about:\n{fdinfo}"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    // -- timers under a backlog, and the checkpoint at shutdown --

    /// `n` lines of `backlog-NNNNN\n`, [`BACKLOG_LINE_BYTES`] each; 8000 of them span two read
    /// chunks.
    fn backlog(n: usize) -> String {
        (0..n).map(|i| format!("backlog-{i:05}\n")).collect()
    }

    const BACKLOG_LINE_BYTES: u64 = 14;

    /// An empty batch, sent from a clone of the tailer's `Fanout` to fill its channel.
    fn filler_batch() -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events: Vec::new() }
    }

    /// Receives one batch per `per_batch` until `stop` says so or the channel has been quiet for
    /// 5s, returning the events in arrival order.
    async fn consume_slowly(
        rx: &mut mpsc::Receiver<Delivered>,
        per_batch: Duration,
        mut stop: impl FnMut(&[Event]) -> bool,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        while !stop(&events) {
            match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await {
                Ok(Some(delivered)) => events.extend(unwrap_batch(delivered).events),
                Ok(None) | Err(_) => break,
            }
            tokio::time::sleep(per_batch).await;
        }
        events
    }

    /// A final flush parked on a full downstream when the grace backstop drops the task leaves
    /// the on-disk checkpoint where the last interval write put it: a restart re-reads what that
    /// flush held (duplicates), never skips it.
    #[tokio::test(start_paused = true)]
    async fn a_grace_cut_final_flush_leaves_the_checkpoint_at_the_last_checkpointed_offset() {
        let dir = scratch_dir("grace-cut-final-flush");
        let path = dir.join("app.log");
        let checkpoint_path = dir.join("checkpoint.json");
        std::fs::write(&path, b"one\ntwo\n").unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.checkpoint_interval = Duration::from_secs(1);
        config.batching.flush_interval = Duration::from_secs(3600);

        let registry = Registry::new();
        let (fanout, mut rx) = recording_fanout(1);
        let filler = fanout.clone();
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config)
            .with_telemetry(registry.telemetry_for("tail_in", "tail_in", "listener"));
        let (shutdown_tx, mut handle) = spawn_tailer(tailer, fanout);

        // The first checkpoint tick flushes both lines, then records their 8 bytes.
        assert_eq!(messages(&expect_events(&mut rx, 2).await), vec!["one", "two"]);
        wait_until("the interval checkpoint to record the first two lines", || {
            checkpointed_offset(&checkpoint_path) == Some(8)
        })
        .await;

        // Fill the channel before appending: with it full, the next flush parks, and a run loop
        // parked in a flush never observes shutdown.
        filler.send(filler_batch()).await;
        let _ = counter_total(&registry, "logit.input.lines"); // count only the appended lines
        append(&path, b"three\nfour\n");
        let mut lines = 0.0;
        wait_until("the appended lines to be read into the accumulator", || {
            lines += counter_total(&registry, "logit.input.lines");
            lines >= 2.0
        })
        .await;

        let _ = shutdown_tx.send(true);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), &mut handle).await.is_err(),
            "the final flush should be parked on the full channel"
        );
        // `run_input`'s grace backstop drops the task here.
        handle.abort();
        let _ = handle.await;

        assert!(
            counter_sum(&registry, "logit.component.receive.flushed", ("reason", "shutdown"))
                >= 1.0,
            "the final flush is counted before its send parks"
        );
        assert_eq!(
            checkpointed_offset(&checkpoint_path),
            Some(8),
            "a grace-cut final flush must leave the last interval checkpoint in place"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// `drain` yields to a due checkpoint tick between passes, so a backlog read against a slow
    /// downstream is checkpointed while it's still being read, not only once it's done.
    #[tokio::test(start_paused = true)]
    async fn a_checkpoint_tick_lands_while_a_long_backlog_is_still_being_drained() {
        let dir = scratch_dir("checkpoint-under-backlog");
        let path = dir.join("app.log");
        let checkpoint_path = dir.join("checkpoint.json");
        let content = backlog(8_000);
        let file_len = content.len() as u64;
        assert!(file_len > READ_CHUNK_BYTES as u64, "fixture must exceed one read chunk");
        std::fs::write(&path, content.as_bytes()).unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.checkpoint_interval = Duration::from_millis(100);
        config.batching.max_events = 1;

        let (fanout, mut rx) = recording_fanout(1);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let mut mid_backlog: Option<(usize, u64)> = None;
        let events = consume_slowly(&mut rx, Duration::from_millis(1), |events| {
            if mid_backlog.is_none() && events.len() % 50 == 0 {
                if let Some(offset) = checkpointed_offset(&checkpoint_path) {
                    if 0 < offset && offset < file_len {
                        mid_backlog = Some((events.len(), offset));
                    }
                }
            }
            events.len() >= 8_000
        })
        .await;

        let (seen_at, offset) =
            mid_backlog.expect("a checkpoint tick should land before the backlog is fully read");
        assert!(seen_at < 8_000, "the checkpoint landed at event {seen_at}, offset {offset}");
        assert_eq!(
            messages(&events),
            (0..8_000).map(|i| format!("backlog-{i:05}")).collect::<Vec<_>>(),
            "every line should still arrive, in order, after the mid-backlog checkpoint"
        );

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The flush tick's twin of the checkpoint test: a quiet second file's accumulator is flushed
    /// on the tick while another file's backlog is still being read.
    #[tokio::test(start_paused = true)]
    async fn a_flush_tick_lands_while_a_long_backlog_is_still_being_drained() {
        let dir = scratch_dir("flush-under-backlog");
        let busy = dir.join("busy.log");
        let quiet = dir.join("quiet.log");
        std::fs::write(&busy, backlog(8_000).as_bytes()).unwrap();
        std::fs::write(&quiet, b"quiet line\n").unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.batching.max_events = 50;
        config.batching.flush_interval = Duration::from_millis(100);

        let (fanout, mut rx) = recording_fanout(1);
        let tailer = Tailer::new(
            vec![PathPattern::new(&busy), PathPattern::new(&quiet)],
            LineFactory,
            config,
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let is_quiet = |e: &Event| e.log.as_ref().unwrap().message.as_str() == Some("quiet line");
        let events = consume_slowly(&mut rx, Duration::from_millis(50), |events| {
            events.iter().any(is_quiet)
        })
        .await;
        let busy_before_quiet = events.iter().filter(|e| !is_quiet(e)).count();
        assert!(events.iter().any(is_quiet), "the quiet file's line should be flushed");
        assert!(
            busy_before_quiet < 8_000,
            "a flush tick should land before the backlog is fully read, not after all \
             {busy_before_quiet} of its lines"
        );

        drop(rx); // the rest of the backlog then fails fast as closed_consumer
        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Shutdown while a backlog is read against a downstream that then stops taking batches: the
    /// run loop is parked in `emit` and never sees the signal, so the grace backstop drops it with
    /// no final checkpoint. The interval checkpoints written between passes bound what a restart
    /// replays to one read chunk, and nothing delivered is skipped.
    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_backlog_drain_with_a_parked_downstream_replays_at_most_one_chunk() {
        let dir = scratch_dir("shutdown-under-backlog");
        let path = dir.join("app.log");
        let checkpoint_path = dir.join("checkpoint.json");
        std::fs::write(&path, backlog(8_000).as_bytes()).unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.checkpoint_interval = Duration::from_millis(100);
        config.batching.max_events = 1;

        let (fanout, mut rx) = recording_fanout(1);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone());
        let (shutdown_tx, mut handle) = spawn_tailer(tailer, fanout);

        // Into the second chunk, then the downstream stops.
        let delivered =
            consume_slowly(&mut rx, Duration::from_millis(1), |e| e.len() >= 6_000).await.len();
        let _ = shutdown_tx.send(true);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), &mut handle).await.is_err(),
            "the run loop should be parked in emit"
        );
        handle.abort();
        let _ = handle.await;
        drop(rx);

        let offset = checkpointed_offset(&checkpoint_path)
            .expect("an interval checkpoint should have landed during the backlog");
        let delivered_bytes = delivered as u64 * BACKLOG_LINE_BYTES;
        assert!(offset <= delivered_bytes, "checkpoint {offset} covers undelivered lines");
        assert!(
            delivered_bytes - offset <= READ_CHUNK_BYTES as u64,
            "replay window {} exceeds one read chunk",
            delivered_bytes - offset
        );
        assert_eq!(offset % BACKLOG_LINE_BYTES, 0, "the checkpoint must sit on a line boundary");

        // A restart resumes at the checkpointed line: at or before the last one delivered.
        let (fanout2, mut rx2) = recording_fanout(8);
        let tailer2 = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (shutdown_tx2, handle2) = spawn_tailer(tailer2, fanout2);
        let first = expect_events(&mut rx2, 1).await;
        let resumed_at = offset / BACKLOG_LINE_BYTES;
        assert_eq!(messages(&first[..1]), vec![format!("backlog-{resumed_at:05}")]);
        drop(rx2);
        shutdown(shutdown_tx2, handle2).await;

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A reap dirties the checkpoint, so the next interval write, not only shutdown's, drops the
    /// reaped inode's entry. Left in place, a crash followed by a new file reusing the inode would
    /// resume from the stale offset.
    #[tokio::test]
    async fn a_reaped_files_stale_checkpoint_entry_is_gone_before_an_inode_reuse_can_resume_from_it(
    ) {
        let dir = scratch_dir("reap-dirties-checkpoint");
        let a_path = dir.join("a.log");
        let b_path = dir.join("b.log");
        std::fs::write(&a_path, b"a1\n").unwrap();
        std::fs::write(&b_path, b"b1\n").unwrap();
        let checkpoint_path = dir.join("checkpoint.json");

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.checkpoint_interval = Duration::from_millis(20);

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&a_path), PathPattern::new(&b_path)],
            LineFactory,
            config,
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let _ = expect_events(&mut rx, 2).await;
        wait_until("a checkpoint tick to record both files", || {
            std::fs::read_to_string(&checkpoint_path)
                .is_ok_and(|text| text.contains("a.log") && text.contains("b.log"))
        })
        .await;

        // Nothing else changes after the removal, so only the reap can dirty the store.
        std::fs::remove_file(&b_path).unwrap();
        wait_until("an interval write to drop the reaped file's entry", || {
            std::fs::read_to_string(&checkpoint_path)
                .is_ok_and(|text| text.contains("a.log") && !text.contains("b.log"))
        })
        .await;

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }
}
