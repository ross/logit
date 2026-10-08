//! The read/rotate/checkpoint/shutdown loop shared by [`TailInput`](super::TailInput) and
//! `crate::docker::DockerInput`, generic over the decoder and the factory that builds one per
//! matched path.
//!
//! A file's identity is its [`FileId`]. On each `scan`, a discovered path whose inode differs from
//! the one tracked under it is a rotation: the old inode is read to EOF and closed once it has
//! stayed there for one `poll_interval` ([`Tailer::reap_drained`]), and the new one opened at the
//! beginning. A tracked file whose size is below the offset already read was
//! truncated in place: it's re-read from `0` with its partial-line state discarded.

use super::checkpoint::{CheckpointStore, FileId, Head, Loaded, Retained, Source, HEAD_BYTES};
use super::line::{LineSplitter, TailDecoder};
use super::pattern::PathPattern;
use super::TailConfig;
use bytes::{BufMut, Bytes, BytesMut};
use logit_core::{Diagnostics, Event, EventBatch, Telemetry};
use logit_pipeline::{fault, BatchAccumulator, Fanout, FlushReason};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;

/// One read off a tracked file: large enough to amortize the syscall, small enough that one busy
/// file can't starve the others in `drain`'s round robin. Not configurable.
const READ_CHUNK_BYTES: usize = 64 * 1024;

/// Below this much spare capacity, `read_one` starts the shared read buffer on a fresh
/// [`READ_CHUNK_BYTES`] (reclaiming the old allocation if no event still slices it). Above it,
/// small reads (a wake per short write) share one allocation instead of taking one each.
const READ_REFILL_BYTES: usize = 16 * 1024;

/// The fault seam's point for `read_one`'s read of a tracked file.
pub(crate) const READ: fault::Point = fault::Point::new(fault::sites::TAIL_READ, fault::Op::Read);

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

    /// Called for a tracked path `scan` offers neither `accept` nor `refresh`, so a factory that
    /// evicts per-scan state keeps this path's: one kept because its listing failed or its stat
    /// was unknown, an inode rebound under this new path, and a de-selected file not yet reaped.
    /// Default: nothing cached.
    fn retain(&mut self, _path: &Path) {}

    /// End of one `scan`: every discovered path has had one `accept`, `refresh`, or `retain`
    /// call since the previous `end_scan`, and every kept one a `retain` call, so a caching
    /// factory can evict the rest. Default: nothing cached.
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
    /// (rotated): read to EOF, flush, close, no sooner than one `poll_interval` after it began
    /// draining (`Tailer::reap_drained`). Revived to `Active` only by a rebind in
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
    /// No consumer took a batch ([`Tailer::untaken`]): the run loop stops.
    Untaken,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartOffset {
    Beginning,
    End,
    /// A retained offset and the head it was recorded with. `Tailer::open_tracked` resumes there
    /// only if the head still matches and the offset is within the file, else starts at `0`
    /// (`checkpoint.rs`'s module doc has the rule).
    Resume(u64, Head),
}

impl From<super::ReadFrom> for StartOffset {
    fn from(read_from: super::ReadFrom) -> Self {
        match read_from {
            super::ReadFrom::Beginning => StartOffset::Beginning,
            super::ReadFrom::End => StartOffset::End,
        }
    }
}

/// A pattern whose listing failed in one scan.
#[derive(Debug)]
pub(super) struct FailedListing {
    /// The index into `Tailer::patterns`.
    pub pattern: usize,
    pub dir: PathBuf,
    pub error: std::io::Error,
}

/// What one scan's listing step (`Tailer::list`) learned: what's there, and what it couldn't
/// check. A tracked path missing from `discovered` is gone only if [`Listing::listed`] says so.
#[derive(Debug, Default)]
pub(super) struct Listing {
    /// Every regular file a pattern names, with its `stat`, one entry per distinct path.
    pub discovered: HashMap<PathBuf, std::fs::Metadata>,
    /// Paths a pattern may name that couldn't be checked: a pattern's `Scan::unknown`, and a
    /// matched path whose `stat` failed other than as absent.
    pub unknown: HashSet<PathBuf>,
    /// Every pattern whose listing failed.
    pub failed: Vec<FailedListing>,
    /// How many paths went into `unknown`: the `op="stat"` count.
    pub stat_errors: usize,
    /// The first of those, with its error, for the diagnostic.
    pub first_stat_error: Option<(PathBuf, std::io::Error)>,
}

impl Listing {
    /// Whether this listing can say `path` is gone: it isn't unknown, and no failed listing
    /// covers it.
    pub fn listed(&self, patterns: &[PathPattern], path: &Path) -> bool {
        !self.unknown.contains(path)
            && !self.failed.iter().any(|f| patterns[f.pattern].covers(path))
    }

    /// The inode of every discovered path.
    pub fn discovered_ids(&self) -> impl Iterator<Item = FileId> + '_ {
        self.discovered.values().map(FileId::from_metadata)
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
    /// ([`TailDecoder::holds_entry`]), or `None` while it holds nothing. Set by `read_one` (and
    /// by `close_decoder`, for the unterminated last line) when a line starts a held run,
    /// cleared once the decoder holds nothing (asked after every line and after `close`), and on
    /// a truncation.
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
    /// The file's bytes `[0, head.len())`, at most [`HEAD_BYTES`], captured as `read_one` reads
    /// them and cleared on a truncation. Invariant: `head.len() >= min(HEAD_BYTES, offset)`, so
    /// the fingerprint written beside any offset covers what `checkpoint.rs`'s module doc says.
    head: Vec<u8>,
    /// When the file became [`FileState::Draining`], set by [`TrackedFile::start_draining`] and
    /// cleared by a rebind. `None` in every other state. `Tailer::reap_drained` reads it.
    draining_since: Option<tokio::time::Instant>,
    /// The `Tailer::scan_generation` of the scan that made the file `Draining`.
    draining_scan: u64,
    /// Whether a scan after `draining_scan` completed with a listing that could have named this
    /// file's path, and so could have rebound it. `Tailer::reap_drained` requires it.
    rescanned: bool,
}

impl<D> TrackedFile<D> {
    /// Marks the file `Draining` in the scan numbered `scan`. A file already draining keeps its
    /// original start, so a second retirement can't postpone its reap.
    fn start_draining(&mut self, scan: u64) {
        if self.state != FileState::Draining {
            self.state = FileState::Draining;
            self.draining_since = Some(tokio::time::Instant::now());
            self.draining_scan = scan;
            self.rescanned = false;
        }
    }
}

pub(crate) struct Tailer<D: TailDecoder, F: DecoderFactory<D>> {
    patterns: Vec<PathPattern>,
    factory: F,
    config: TailConfig,
    files: HashMap<FileId, TrackedFile<D>>,
    by_path: HashMap<PathBuf, FileId>,
    checkpoint: Option<CheckpointStore>,
    /// The offset and head an inode resumes from when next discovered, consulted before
    /// `read_from`. An entry is spent only once `open_tracked` tracks the file.
    ///
    /// Filled at `bind` from the checkpoint ([`Source::Checkpoint`], persisted by every checkpoint
    /// write until spent or pruned), and by `reap_drained` for a [`FileState::Deselected`] file
    /// ([`Source::Deselected`], process-local), so a container renamed back into the selection
    /// resumes instead of replaying (`docs/adr/docker-container-identity-and-minimal-watches.md`).
    resume: HashMap<FileId, Retained>,
    /// Where a file found by the first scan with no `resume` entry starts. Set at `bind`:
    /// `Beginning` after [`Loaded::Unusable`], else from `read_from`.
    first_scan_start: StartOffset,
    diag: Diagnostics,
    telemetry: Telemetry,
    watched_dirs: HashSet<PathBuf>,
    /// Counts completed scans; `TrackedFile::draining_scan` records it.
    scan_generation: u64,
    /// Set by [`Tailer::bind`] and taken into a local by [`Tailer::run_until_shutdown`].
    /// `Option` because `Watcher` has no "not yet opened" value.
    watcher: Option<super::watch::Watcher>,
    /// Set once an emit finds no consumer to take its batch: every consumer of this listener has
    /// closed. From then on nothing more is emitted and [`Tailer::write_checkpoint`] writes
    /// nothing, so the persisted offset stays at the last checkpoint written, at or before the
    /// last batch a consumer took, and a restart may replay lines a consumer already took. The
    /// run loop then returns `Ok`, and the node finishes.
    untaken: bool,
    /// The buffer `read_one` reads into, shared by every tracked file. Empty between reads: each
    /// read is split off as a frozen `Bytes` the lines slice, and the next reads go into the same
    /// allocation's spare capacity until less than [`READ_REFILL_BYTES`] is left. An allocation
    /// is freed once every event slicing it is gone, so one event can hold up to
    /// [`READ_CHUNK_BYTES`] of earlier reads alive with it.
    read_buf: BytesMut,
    /// `read_one`'s split lines with their start offsets, empty between calls; kept for its
    /// capacity.
    lines: Vec<(Bytes, u64)>,
    /// `read_one`'s decoded events, emptied by every `BatchAccumulator::absorb`; kept for its
    /// capacity.
    scratch: Vec<Event>,
    /// `drain`'s per-pass file list and the files that pass found at EOF; kept for their
    /// capacity.
    pass_ids: Vec<FileId>,
    pass_at_eof: Vec<FileId>,
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
            scan_generation: 0,
            watcher: None,
            untaken: false,
            read_buf: BytesMut::new(),
            lines: Vec::new(),
            scratch: Vec::new(),
            pass_ids: Vec::new(),
            pass_at_eof: Vec::new(),
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

    /// One `scan` after [`Tailer::bind`], with no `drain`, for a test outside this module.
    #[cfg(test)]
    pub(crate) async fn scan_after_bind(&mut self) {
        let mut watcher = self.watcher.take().expect("bind() leaves a watcher behind");
        self.scan(false, &mut watcher).await;
        self.watcher = Some(watcher);
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
            // The flush arm, and the checkpoint arm's flush, can find every consumer closed.
            if self.untaken {
                break;
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
                DrainEnd::Shutdown | DrainEnd::Untaken => break,
                DrainEnd::Idle | DrainEnd::TimerDue => {}
            }
        }

        if !self.untaken {
            self.close_all_for_shutdown(&sink).await;
        }
        if !self.untaken {
            self.flush_all(&sink, FlushReason::Shutdown).await;
        }
        // A no-op once a batch was refused, which leaves the checkpoint where it was.
        self.write_checkpoint(true).await;
        if self.untaken {
            self.diag.warn_throttled(
                "closed_consumer",
                "no consumer took a batch; stopped tailing, with the checkpoint left where it \
                 was, at or before the last batch taken",
            );
        }
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
    /// since it has no "before startup" to skip. A checkpoint entry wins over both. Each scan
    /// also prunes the retained entries it shows nothing can consume
    /// ([`Tailer::prune_deselected`], [`Tailer::prune_checkpoint_entries`]).
    ///
    /// A tracked path this scan shows is gone starts draining its file. One that a failed listing
    /// or `stat` could have named is kept (`pattern.rs`'s module doc has the rule), and its open
    /// handle is `fstat`ed instead, which still sees an unlinked file and a truncation.
    async fn scan(&mut self, first: bool, watcher: &mut super::watch::Watcher) {
        self.reconcile_watches(watcher);
        let mut listing = self.list();
        self.report_scan_errors(&listing, first);
        let mut pruned = self.prune_deselected(&listing);

        let stale: Vec<PathBuf> = self
            .by_path
            .keys()
            .filter(|p| !listing.discovered.contains_key(*p) && listing.listed(&self.patterns, p))
            .cloned()
            .collect();
        for path in stale {
            if let Some(id) = self.by_path.remove(&path) {
                if let Some(tracked) = self.files.get_mut(&id) {
                    tracked.start_draining(self.scan_generation);
                }
            }
        }
        let kept: Vec<(PathBuf, FileId)> = self
            .by_path
            .iter()
            .filter(|(p, _)| !listing.discovered.contains_key(*p))
            .map(|(p, id)| (p.clone(), *id))
            .collect();
        for (path, id) in kept {
            self.keep_unlisted(path, id).await;
        }

        // Only needed while a checkpoint entry is unspent, which is rarely past the first scan.
        let unspent = self.resume.values().any(|r| r.source == Source::Checkpoint);
        let discovered_ids: HashSet<FileId> =
            if !unspent { HashSet::new() } else { listing.discovered_ids().collect() };
        self.count_rotations(&listing.discovered);
        for (path, meta) in std::mem::take(&mut listing.discovered) {
            self.reconcile_discovered(path, &meta, first, watcher).await;
        }
        // A `Draining` file is bound to no path, so nothing above looked at it.
        let draining: Vec<FileId> = self
            .files
            .iter()
            .filter(|(_, f)| f.state == FileState::Draining)
            .map(|(id, _)| *id)
            .collect();
        for id in draining {
            self.recheck_length(id).await;
        }
        pruned |= self.prune_checkpoint_entries(&listing, &discovered_ids);
        if pruned {
            if let Some(cp) = &mut self.checkpoint {
                cp.mark_dirty();
            }
        }

        // A file that started draining in an earlier scan, and whose path this listing could
        // have named, had its chance to be rebound under a new name.
        let generation = self.scan_generation;
        let patterns = &self.patterns;
        for f in self.files.values_mut() {
            if f.state == FileState::Draining
                && f.draining_scan < generation
                && listing.listed(patterns, &f.path)
            {
                f.rescanned = true;
            }
        }
        self.scan_generation += 1;

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

    /// Counts `logit.input.files.rotated`: each discovered path bound to an inode other than the
    /// one now there. Runs before [`Tailer::reconcile_discovered`] mutates `by_path`. Counted in
    /// the loop instead, the total would depend on `discovered`'s order: a rebind that ran first
    /// removes the old binding, and the path would then take the `None` arm uncounted.
    fn count_rotations(&self, discovered: &HashMap<PathBuf, std::fs::Metadata>) {
        let rotated = discovered
            .iter()
            .filter(|(path, meta)| {
                self.by_path.get(*path).is_some_and(|old| *old != FileId::from_metadata(meta))
            })
            .count();
        if rotated > 0 {
            self.telemetry.count("logit.input.files.rotated", rotated as f64, &[]);
        }
    }

    /// Reconciles one discovered `path` against what's tracked under it: the same inode is
    /// checked for truncation and identity, another inode is a rotation, and an untracked path is
    /// opened (or rebound, by `open_tracked`).
    async fn reconcile_discovered(
        &mut self,
        path: PathBuf,
        meta: &std::fs::Metadata,
        first: bool,
        watcher: &mut super::watch::Watcher,
    ) {
        let id = FileId::from_metadata(meta);
        // Peeked, not removed: `accept` may still reject this path (a de-selected container not
        // yet re-selected), or the open may fail, and removing here would lose the retained
        // offset. `open_tracked` removes it once the file is tracked. Both arms that open read
        // it: whether a rotated path is still bound when it's reached depends on `discovered`'s
        // order (a rebind of its old inode elsewhere may have released it first), and the start
        // must not.
        let start = match self.resume.get(&id) {
            Some(retained) => StartOffset::Resume(retained.offset, retained.head),
            None if first => self.first_scan_start,
            None => StartOffset::Beginning,
        };
        match self.by_path.get(&path).copied() {
            Some(existing_id) if existing_id == id => {
                self.reconcile_truncation(id, meta.len()).await;
                self.refresh_identity(id, &path);
            }
            Some(existing_id) => {
                if let Some(tracked) = self.files.get_mut(&existing_id) {
                    tracked.start_draining(self.scan_generation);
                }
                self.by_path.remove(&path);
                self.open_or_rebind(path, id, meta.len(), start, watcher).await;
            }
            None => self.open_or_rebind(path, id, meta.len(), start, watcher).await,
        }
    }

    /// [`Tailer::open_tracked`], then, for an inode it rebound, the truncation check the
    /// same-path arm runs. A rebound inode may have been truncated in place while it was unbound
    /// (retired by a `stat` that raced a rename, then copytruncated); left unchecked, a refill past
    /// its offset before the next scan would be read from mid-line. `len` is the scan's `stat` of
    /// `path`, which names `id`.
    async fn open_or_rebind(
        &mut self,
        path: PathBuf,
        id: FileId,
        len: u64,
        start: StartOffset,
        watcher: &mut super::watch::Watcher,
    ) {
        let rebind = self.files.get(&id).is_some_and(|f| f.state != FileState::Deselected);
        self.open_tracked(path, id, start, watcher).await;
        if rebind {
            self.reconcile_truncation(id, len).await;
        }
    }

    /// Drops each [`Source::Deselected`] entry this listing shows can't be re-selected: its path
    /// is listed and either gone or another inode's. Runs before the discovery loop, so a file
    /// re-selected in this scan resumes. Returns whether it dropped any.
    fn prune_deselected(&mut self, listing: &Listing) -> bool {
        let before = self.resume.len();
        let patterns = &self.patterns;
        self.resume.retain(|id, r| {
            r.source != Source::Deselected
                || !listing.listed(patterns, &r.path)
                || listing.discovered.get(&r.path).is_some_and(|m| FileId::from_metadata(m) == *id)
        });
        self.resume.len() != before
    }

    /// Drops each unspent [`Source::Checkpoint`] entry this listing shows nothing can consume:
    /// its stored path is listed and its inode wasn't discovered under any path. Runs after the
    /// discovery loop, on every scan; an entry whose path a failed listing or `stat` covers stays,
    /// and is persisted by every checkpoint write meanwhile. Returns whether it dropped any.
    fn prune_checkpoint_entries(
        &mut self,
        listing: &Listing,
        discovered_ids: &HashSet<FileId>,
    ) -> bool {
        let before = self.resume.len();
        let patterns = &self.patterns;
        self.resume.retain(|id, r| {
            r.source != Source::Checkpoint
                || !listing.listed(patterns, &r.path)
                || discovered_ids.contains(id)
        });
        self.resume.len() != before
    }

    /// `scan`'s listing step: every pattern's [`PathPattern::scan`], then one `stat` per distinct
    /// matched path, so a path two patterns name costs one `stat`. Blocking, like
    /// `PathPattern::scan`.
    fn list(&self) -> Listing {
        let mut listing = Listing::default();
        let mut candidates: HashSet<PathBuf> = HashSet::new();
        for (pattern, p) in self.patterns.iter().enumerate() {
            match p.scan() {
                Ok(scan) => {
                    candidates.extend(scan.matched);
                    listing.stat_errors += scan.unknown.len();
                    if let (Some(path), Some(err)) =
                        (scan.unknown.first(), scan.first_unknown_error)
                    {
                        listing.first_stat_error.get_or_insert((path.clone(), err));
                    }
                    listing.unknown.extend(scan.unknown);
                }
                Err(error) => listing.failed.push(FailedListing {
                    pattern,
                    dir: p.dir().to_path_buf(),
                    error,
                }),
            }
        }
        for path in candidates {
            match fault::check(super::pattern::STAT, &path, 0)
                .and_then(|()| std::fs::metadata(&path))
            {
                Ok(meta) if meta.is_file() => {
                    listing.discovered.insert(path, meta);
                }
                Ok(_) => {}
                Err(err) if super::pattern::is_absent(&err) => {}
                Err(err) => {
                    listing.stat_errors += 1;
                    listing.first_stat_error.get_or_insert_with(|| (path.clone(), err));
                    listing.unknown.insert(path);
                }
            }
        }
        listing
    }

    /// Counts `logit.input.scan.errors{op}` and diagnoses `scan_error`, once per operation per
    /// scan that had a failure.
    fn report_scan_errors(&mut self, listing: &Listing, first: bool) {
        // `read_from: end` applies to the first scan only.
        let later =
            if first { "; a file first found by a later scan starts at the beginning" } else { "" };
        if let Some(failed) = listing.failed.first() {
            let n = listing.failed.len();
            self.telemetry.count("logit.input.scan.errors", n as f64, &[("op", "read_dir")]);
            self.diag.warn_throttled(
                "scan_error",
                format!(
                    "listing {} failed: {} ({n} listing(s) failed this scan); no tracked file it \
                     may name is closed until a listing succeeds{later}",
                    failed.dir.display(),
                    failed.error,
                ),
            );
        }
        if let Some((path, err)) = &listing.first_stat_error {
            let n = listing.stat_errors;
            self.telemetry.count("logit.input.scan.errors", n as f64, &[("op", "stat")]);
            self.diag.warn_throttled(
                "scan_error",
                format!(
                    "stat of {} failed: {err} ({n} stat(s) failed this scan); a tracked file \
                     there stays open until a stat succeeds{later}",
                    path.display(),
                ),
            );
        }
    }

    /// Keeps a tracked `path` this scan couldn't list, unless its open handle shows the file was
    /// unlinked; a handle shorter than the offset read is a truncation. The `fstat` needs no
    /// permission on the path and sees the tracked inode, as `reconcile_truncation` requires.
    async fn keep_unlisted(&mut self, path: PathBuf, id: FileId) {
        use std::os::unix::fs::MetadataExt;
        let Some(tracked) = self.files.get_mut(&id) else { return };
        match tracked.file.metadata().await {
            Ok(meta) if meta.nlink() == 0 => {
                tracked.start_draining(self.scan_generation);
                self.by_path.remove(&path);
                return;
            }
            Ok(meta) => self.reconcile_truncation(id, meta.len()).await,
            // The handle still reads; a later scan decides.
            Err(_) => {}
        }
        self.factory.retain(&path);
    }

    /// The truncation check for a file no path is bound to: an `fstat` of its handle against its
    /// offset. `scan` runs it for every `Draining` file, and `drain` before each read of one, since
    /// the drain loop runs far more often than `scan` and a refill past the offset before the
    /// first check is the one truncation size-based detection can't see
    /// (`docs/known-gaps/tailing.md`). An `fstat` error says nothing; a later check decides.
    async fn recheck_length(&mut self, id: FileId) {
        let Some(tracked) = self.files.get(&id) else { return };
        let Ok(meta) = tracked.file.metadata().await else { return };
        self.reconcile_truncation(id, meta.len()).await;
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
    /// the same inode as `id`. Called by `scan`, [`Tailer::on_data_wake`], and `drain` (through
    /// [`Tailer::recheck_length`]).
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
        tracked.head.clear();
        let path = tracked.path.clone();
        self.diag.warn_throttled("truncated", truncated_message(&path, prev_offset, len));
        self.telemetry.count("logit.input.files.truncated", 1.0, &[]);
        // The persisted offset is now past the file's end. A write before the next read must
        // replace it with `0`: the head fingerprint would reject it at a restart, but only as a
        // counted `resume_rejected`, and only if the head changed.
        if let Some(cp) = &mut self.checkpoint {
            cp.mark_dirty();
        }
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
                // is reversed. Offered to the factory all the same, which calls neither `accept`
                // nor `refresh` for it this scan, so `end_scan` keeps its cached state.
                self.factory.retain(&path);
                return;
            }
            // Same inode, new name. A `Draining` entry goes back to `Active`: a pattern reaches
            // it again, and reaping is for inodes no pattern reaches.
            let old_path = std::mem::replace(&mut tracked.path, path.clone());
            tracked.state = FileState::Active;
            tracked.draining_since = None;
            tracked.rescanned = false;
            // Remove the old binding only if this inode still owns it. `discovered` iterates in
            // no fixed order, so the rotation replacement may already have claimed `old_path`.
            // Removing its binding would orphan a live inode: `scan`'s stale check walks only
            // `by_path`, so it could never be drained or reaped.
            if self.by_path.get(&old_path) == Some(&id) {
                self.by_path.remove(&old_path);
            }
            // Neither `accept` nor `refresh` runs for a rebind, and the factory keys its cache by
            // the new path.
            self.factory.retain(&path);
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
        let opened = match fault::check(super::pattern::OPEN, &path, 0) {
            Ok(()) => tokio::fs::File::open(&path).await,
            Err(err) => Err(err),
        };
        let mut file = match opened {
            Ok(f) => f,
            Err(err) => {
                self.diag.warn_throttled("open_error", format!("{}: {err}", path.display()));
                return;
            }
        };
        let meta = match file.metadata().await {
            Ok(meta) => meta,
            Err(err) => {
                self.diag.warn_throttled("open_error", format!("{}: {err}", path.display()));
                return;
            }
        };
        if FileId::from_metadata(&meta) != id {
            // Rotated between `scan`'s `stat` and this open: the descriptor is another inode, and
            // `start` belongs to `id`. The next `scan` finds the new inode under this path.
            return;
        }
        let len = meta.len();
        let mut head = Vec::with_capacity(HEAD_BYTES);
        let mut rejected = None;
        let offset = match start {
            StartOffset::Beginning => 0,
            StartOffset::End => {
                // `read_one` never sees the bytes skipped here, so the head is read once, now.
                match read_head(&mut file, len.min(HEAD_BYTES as u64) as usize).await {
                    Ok(bytes) => head = bytes,
                    Err(err) => {
                        self.diag
                            .warn_throttled("open_error", format!("{}: {err}", path.display()));
                        return;
                    }
                }
                len
            }
            StartOffset::Resume(off, retained) => {
                // Read at least `min(HEAD_BYTES, off)` bytes, so the invariant on
                // `TrackedFile::head` holds from here without re-reading.
                let n = off.max(u64::from(retained.len)).min(HEAD_BYTES as u64);
                let current = if off <= len && u64::from(retained.len) <= len {
                    let read = match fault::check(super::pattern::HEAD_READ, &path, 0) {
                        Ok(()) => read_head(&mut file, n as usize).await,
                        Err(err) => Err(err),
                    };
                    match read {
                        Ok(bytes) => Some(bytes),
                        // Shrank since the `metadata` above: the head can't match.
                        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => None,
                        // Says nothing about the file's identity, so keep the entry for the next
                        // `scan` rather than reject it.
                        Err(err) => {
                            self.diag
                                .warn_throttled("open_error", format!("{}: {err}", path.display()));
                            return;
                        }
                    }
                } else {
                    None
                };
                match current {
                    Some(mut bytes) if retained.matches(&bytes) => {
                        bytes.truncate(off.min(HEAD_BYTES as u64) as usize);
                        head = bytes;
                        off
                    }
                    _ => {
                        rejected = Some(off);
                        0
                    }
                }
            }
        };
        // After any head read, even to `0`: the read moved the cursor.
        if start != StartOffset::Beginning {
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
        // sign. Registered once per tracked inode and never retried (`docs/known-gaps/tailing.md`).
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
            head,
            draining_since: None,
            draining_scan: 0,
            rescanned: false,
        };
        if let Some(off) = rejected {
            self.telemetry.count("logit.input.files.resume_rejected", 1.0, &[]);
            self.diag.warn_throttled("resume_rejected", resume_rejected_message(&path, off));
        }
        self.by_path.insert(path, id);
        self.files.insert(id, tracked);
        // Spent only now: an open, head read, seek, or decoder failure above leaves the entry for
        // the next `scan`, so a transient error doesn't replay. Removed whatever `start` was, so
        // no entry outlives its inode being tracked.
        self.resume.remove(&id);
        if let Some(cp) = &mut self.checkpoint {
            cp.mark_dirty();
        }
    }

    /// Round-robin reads every tracked file, one chunk each per pass, until a pass makes no
    /// progress ([`DrainEnd::Idle`]) or `due` has passed ([`DrainEnd::TimerDue`]), so a burst is
    /// read without waiting for another wake and a backlog still yields to the run loop's timers.
    /// Closes the `Draining` and `Deselected` files that made no progress and are due
    /// ([`Tailer::reap_drained`]).
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
            if self.untaken {
                return DrainEnd::Untaken;
            }
            // Before the reads: a pass parked on the downstream mustn't reap on an EOF it saw
            // before the grace ran out.
            let pass_start = tokio::time::Instant::now();
            let mut ids = std::mem::take(&mut self.pass_ids);
            let mut at_eof = std::mem::take(&mut self.pass_at_eof);
            ids.clear();
            ids.extend(self.files.keys().copied());
            at_eof.clear();
            let (any_progress, stopped) = self.read_pass(&ids, &mut at_eof, sink, shutdown).await;
            if stopped.is_none() {
                self.reap_drained(&at_eof, pass_start, sink, watcher).await;
            }
            self.pass_ids = ids;
            self.pass_at_eof = at_eof;
            if let Some(end) = stopped {
                return end;
            }
            if self.untaken {
                return DrainEnd::Untaken;
            }
            if !any_progress {
                return DrainEnd::Idle;
            }
            if tokio::time::Instant::now() >= due {
                return DrainEnd::TimerDue;
            }
        }
    }

    /// One round-robin pass of `drain`: a read of each file in `ids`, collecting the ones that
    /// returned no progress in `at_eof`. Returns whether any file made progress, and why the pass
    /// stopped before its last file, if it did.
    async fn read_pass(
        &mut self,
        ids: &[FileId],
        at_eof: &mut Vec<FileId>,
        sink: &Fanout,
        shutdown: &watch::Receiver<bool>,
    ) -> (bool, Option<DrainEnd>) {
        let mut any_progress = false;
        for &id in ids {
            if *shutdown.borrow() {
                return (any_progress, Some(DrainEnd::Shutdown));
            }
            // One `fstat` per pass, only while the file drains: see `recheck_length`.
            if self.files.get(&id).is_some_and(|f| f.state == FileState::Draining) {
                self.recheck_length(id).await;
            }
            if self.read_one(id, sink).await {
                any_progress = true;
            } else {
                at_eof.push(id);
            }
            if self.untaken {
                return (any_progress, Some(DrainEnd::Untaken));
            }
        }
        (any_progress, None)
    }

    /// Reads one chunk from `id`, decodes its complete lines, and emits any batch that reaches a
    /// bound. Returns `false` (eligible for [`Tailer::reap_drained`]) at EOF, on a read error,
    /// or for a [`FileState::Deselected`] file. A refused emit sets [`Tailer::untaken`] and
    /// leaves the chunk's remaining lines undecoded.
    async fn read_one(&mut self, id: FileId, sink: &Fanout) -> bool {
        if self.files.get(&id).is_some_and(|t| t.state == FileState::Deselected) {
            return false;
        }
        if self.read_buf.capacity() < READ_REFILL_BYTES {
            self.read_buf.reserve(READ_CHUNK_BYTES);
        }
        debug_assert!(self.read_buf.is_empty(), "every read is split off whole");
        let n = match self.files.get_mut(&id) {
            Some(tracked) => match logit_pipeline::fault_io!(
                READ,
                &tracked.path,
                0,
                tracked.file.read_buf(&mut (&mut self.read_buf).limit(READ_CHUNK_BYTES)).await
            ) {
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
        let bytes = self.read_buf.split().freeze();

        // Each line with the file offset it starts at, for `TrackedFile::held_from`.
        let mut lines = std::mem::take(&mut self.lines);
        let dropped = {
            let tracked = match self.files.get_mut(&id) {
                Some(t) => t,
                None => return false,
            };
            let chunk_start = tracked.offset;
            capture_head(&mut tracked.head, chunk_start, &bytes);
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
                "a line exceeded the line-length bound and was dropped whole",
            );
        }

        let mut scratch = std::mem::take(&mut self.scratch);
        let tracked_now = self.decode_lines(id, &mut lines, &mut scratch, read_at, sink).await;
        // Both are already empty (`decode_lines` drains `lines`, and `absorb` empties `scratch`);
        // cleared so a decoder that leaves events behind can't carry them into another file's
        // next read.
        lines.clear();
        scratch.clear();
        self.lines = lines;
        self.scratch = scratch;
        if !tracked_now {
            return false;
        }
        if let Some(cp) = &mut self.checkpoint {
            cp.mark_dirty();
        }
        true
    }

    /// `read_one`'s decode half: decodes `lines` into `scratch` and absorbs each line's events
    /// into the file's accumulator, emitting at a bound. Stops at a refused emit
    /// ([`Tailer::untaken`]). Returns `false` if the file stopped being tracked.
    async fn decode_lines(
        &mut self,
        id: FileId,
        lines: &mut Vec<(Bytes, u64)>,
        scratch: &mut Vec<Event>,
        read_at: i64,
        sink: &Fanout,
    ) -> bool {
        for (line, line_start) in lines.drain(..) {
            let line = ensure_utf8(line, &mut self.diag);
            let Some(tracked) = self.files.get_mut(&id) else { return false };
            self.telemetry.count("logit.input.lines", 1.0, &[]);
            self.telemetry.count("logit.input.line.bytes", line.len() as f64, &[]);
            let decoded = tracked.decoder.decode_line(line, read_at, scratch);
            // Asked after every line, rejected ones included: a rejected line can flush the held
            // run (its events are in `scratch`) or leave it, and `held_from` follows either way.
            if !tracked.decoder.holds_entry() {
                tracked.held_from = None;
            } else if tracked.held_from.is_none() {
                tracked.held_from = Some(line_start);
            }
            let resource = match decoded {
                Ok(resource) => resource,
                Err(err) => {
                    self.diag.warn_throttled("bad_line", err);
                    tracked.decoder.resource()
                }
            };
            // No scope: a tailed line has no instrumentation scope.
            if let Some((batch, reason)) = tracked.accumulator.absorb(resource, None, scratch) {
                if !emit(sink, &self.telemetry, batch, reason).await {
                    self.untaken = true;
                    break;
                }
            }
        }
        true
    }

    /// Closes each file due for it among those whose `read_one` returned `false` on this pass
    /// (`at_eof`):
    ///
    /// - a [`FileState::Deselected`] file, at once;
    /// - a [`FileState::Draining`] file once both hold:
    ///   - `pass_start - draining_since >= poll_interval`, where `pass_start` is when this pass
    ///     began, before its reads, so the EOF it saw was seen after the grace ran out;
    ///   - `rescanned`: a scan after the one that retired it completed with a listing that could
    ///     have named its path (not unknown, and not under a failed listing), so a rename it
    ///     raced has been rebound instead.
    ///
    /// Only files at EOF: a draining file with a backlog gets as many passes as it takes to reach
    /// EOF, since reaping it earlier loses the rest for good (it also leaves the next checkpoint).
    /// A read error counts as EOF, or an erroring handle would never be reaped.
    ///
    /// A `Draining` file's EOF isn't final until a `poll_interval` has passed. A writer that
    /// logrotate renamed and then HUPs keeps appending to the renamed inode until it reopens, and
    /// under an exact pattern that inode is reachable only through this handle. A path whose
    /// `stat` raced a rename (`read_dir` listed it, `stat` got `ENOENT`) retires an inode a later
    /// scan finds under its new name; reaped first, it would be reopened there as a new file and
    /// replayed from `0`. Time alone doesn't order that scan first: a `drain` follows a data wake
    /// or a flush tick too, and the run loop's `select!` may pick one over an overdue poll tick,
    /// hence `rescanned`. The poll tick runs under every `WatchMode`, so a file with no other wake
    /// is reaped within about two poll intervals of starting to drain, unless every later listing
    /// covering its path fails, which pins it. A `Deselected` file is still being written and is
    /// never at a final EOF, so waiting would gain nothing.
    ///
    /// Each reap emits held decoder state, flushes the accumulator (`FlushReason::Closed`), and
    /// drops the file.
    ///
    /// A reap dirties the checkpoint, so the next interval write drops the file's entry (a write
    /// persists only tracked files). Left on disk, the entry would outlive the inode: a crash, then
    /// a new file reusing that `(dev, ino)`, would resume past its first bytes.
    async fn reap_drained(
        &mut self,
        at_eof: &[FileId],
        pass_start: tokio::time::Instant,
        sink: &Fanout,
        watcher: &mut super::watch::Watcher,
    ) {
        let grace = self.config.poll_interval;
        let draining: Vec<FileId> = self
            .files
            .iter()
            .filter(|(id, f)| {
                let due = match f.state {
                    FileState::Active => false,
                    FileState::Deselected => true,
                    FileState::Draining => {
                        f.rescanned
                            && f.draining_since.is_some_and(|since| {
                                pass_start.saturating_duration_since(since) >= grace
                            })
                    }
                };
                due && at_eof.contains(id)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in draining {
            let Some(mut tracked) = self.files.remove(&id) else { continue };
            if let Some(watch_id) = tracked.watch {
                watcher.unwatch(watch_id);
            }
            let deselected = tracked.state == FileState::Deselected;
            let mut taken =
                close_decoder(&mut tracked, sink, &self.telemetry, &mut self.diag, CloseMode::Emit)
                    .await;
            if taken {
                if let Some(batch) = tracked.accumulator.take() {
                    taken = emit(sink, &self.telemetry, batch, FlushReason::Closed).await;
                }
            }
            if !taken {
                self.untaken = true;
                return;
            }
            if deselected {
                // This inode is alive, only unselected (a `Draining` one may be gone and its
                // number reused), so keep its offset for a rename back into the selection.
                // `close_decoder` emitted the held partial and the decoder's held lines. What it
                // leaves is a line still being dropped: by the splitter (`pending_bytes`, so the
                // boundary is that line's start) or by the decoder (`held_from`). Resume at the
                // earlier of the two, so a rename back drops that line whole again. The head is
                // checked again on re-selection, since the file may be rewritten meanwhile.
                let boundary = tracked.offset - tracked.splitter.pending_bytes();
                let retained = Retained {
                    path: tracked.path.clone(),
                    offset: tracked.held_from.map_or(boundary, |h| h.min(boundary)),
                    head: Head::of(&tracked.head),
                    source: Source::Deselected,
                };
                self.resume.insert(id, retained);
            }
            if let Some(cp) = &mut self.checkpoint {
                cp.mark_dirty();
            }
        }
    }

    /// At shutdown, emits every tracked file's held partial line and decoder state, so an
    /// unterminated last line isn't lost (with no checkpoint, nothing would re-read it). A decoder
    /// whose [`TailDecoder::hold_at_shutdown`] is set keeps its partial line unread, and, when a
    /// checkpoint will replay them, its held lines too, but only while the file is `Active`; [`Tailer::write_checkpoint`] then leaves
    /// the offset at their start. Unlike [`Tailer::reap_drained`], files stay tracked:
    /// `write_checkpoint` runs next and needs their offsets.
    async fn close_all_for_shutdown(&mut self, sink: &Fanout) {
        let ids: Vec<FileId> = self.files.keys().copied().collect();
        let replayed = self.checkpoint.is_some();
        for id in ids {
            if let Some(tracked) = self.files.get_mut(&id) {
                // Only an `Active` file is re-read by the restart: a `Draining` file's inode is
                // no longer at a matched path, and a `Deselected` file's resume offset is
                // process-local.
                let mode =
                    if tracked.state == FileState::Active && tracked.decoder.hold_at_shutdown() {
                        CloseMode::Hold { held_lines: replayed }
                    } else {
                        CloseMode::Emit
                    };
                if !close_decoder(tracked, sink, &self.telemetry, &mut self.diag, mode).await {
                    self.untaken = true;
                    return;
                }
            }
        }
    }

    /// Emits every accumulator's batch, stopping at the first one no consumer takes
    /// ([`Tailer::untaken`]).
    async fn flush_all(&mut self, sink: &Fanout, reason: FlushReason) {
        let ids: Vec<FileId> = self.files.keys().copied().collect();
        for id in ids {
            let Some(tracked) = self.files.get_mut(&id) else { continue };
            if let Some(batch) = tracked.accumulator.take() {
                if !emit(sink, &self.telemetry, batch, reason).await {
                    self.untaken = true;
                    return;
                }
            }
        }
    }

    /// Persists every tracked file's offset. The invariant: a persisted offset covers only bytes
    /// whose events have been emitted and taken by a consumer, or absorbed into an accumulator
    /// the caller flushes first, so a restart can replay lines but never skip one. Once a batch
    /// was refused ([`Tailer::untaken`]) nothing is written, forced or not: the offset already
    /// covers lines whose events were never taken.
    ///
    /// `offset` advances per chunk, so it also covers bytes that haven't produced an event yet:
    /// the splitter's held partial line ([`LineSplitter::pending_bytes`]) and the complete lines
    /// the decoder holds (`docker_in`'s fragments of a split entry). The persisted offset is the
    /// smaller of the splitter's line boundary and `held_from`, the start of the oldest line the
    /// decoder still holds. A line rejected or dropped after that held run advances `offset`
    /// without clearing it, which is why it's a position and not a byte count to subtract. A file
    /// mid-drop checkpoints at the dropped line's start ([`LineSplitter::pending_bytes`] for the
    /// splitter, `held_from` for the decoder), so a restart drops it whole again, at shutdown
    /// too. A decoder that holds at shutdown ([`TailDecoder::hold_at_shutdown`]) leaves both
    /// unemitted at a clean stop, so the same rule checkpoints at their start and the restart
    /// re-reads them whole. Otherwise, at shutdown `close_all_for_shutdown` has already emitted
    /// both, so the offset is the file's full `offset`.
    async fn write_checkpoint(&mut self, force: bool) {
        if self.untaken {
            return;
        }
        let Some(checkpoint) = &mut self.checkpoint else { return };
        let tracked = self.files.values().map(|f| {
            let boundary = f.offset.saturating_sub(f.splitter.pending_bytes());
            let offset = boundary.min(f.held_from.unwrap_or(u64::MAX));
            (f.id, f.path.as_path(), offset, Head::of(&f.head))
        });
        // An entry not yet spent persists until `scan` prunes it: dropping it here would lose
        // the position of a file whose listing failed, and a restart under `read_from: end`
        // would then skip what it gained.
        let unspent = self
            .resume
            .iter()
            .filter(|(_, r)| r.source == Source::Checkpoint)
            .map(|(id, r)| (*id, r.path.as_path(), r.offset, r.head));
        checkpoint.write(tracked.chain(unspent), force, &mut self.diag, &self.telemetry).await;
    }
}

/// What [`close_decoder`] emits.
#[derive(Clone, Copy)]
enum CloseMode {
    /// The unterminated last line and the decoder's held lines. A file that rotated away or was
    /// removed ([`Tailer::reap_drained`]) will gain nothing more, so its tail is final.
    Emit,
    /// Leaves the unterminated last line unread, and the decoder's held lines unemitted when
    /// `held_lines` is true, so a restart reads them whole. `held_lines` is true only when a
    /// checkpoint will replay them.
    Hold { held_lines: bool },
}

/// Emits a file's unterminated last line ([`LineSplitter::take_partial`]) and whatever
/// [`TailDecoder::close`] produces into its accumulator, flushing if a bound is reached, as far
/// as `mode` allows. Used by [`Tailer::reap_drained`] and [`Tailer::close_all_for_shutdown`].
/// Returns `false` once an emit finds no consumer to take its batch, emitting nothing after it.
async fn close_decoder<D: TailDecoder>(
    tracked: &mut TrackedFile<D>,
    sink: &Fanout,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
    mode: CloseMode,
) -> bool {
    let mut scratch = Vec::new();
    let hold_partial = matches!(mode, CloseMode::Hold { .. });
    let hold_held = matches!(mode, CloseMode::Hold { held_lines: true });

    // Read before `take_partial` empties the splitter: where the unterminated last line starts.
    let partial_start = tracked.offset - tracked.splitter.pending_bytes();
    let partial = if hold_partial { None } else { tracked.splitter.take_partial() };
    if let Some(partial) = partial {
        let partial = ensure_utf8(partial, diag);
        // Offered to the decoder like any line `read_one` splits, so counted the same way.
        telemetry.count("logit.input.lines", 1.0, &[]);
        telemetry.count("logit.input.line.bytes", partial.len() as f64, &[]);
        let decoded = tracked.decoder.decode_line(partial, now_nanos(), &mut scratch);
        // `read_one`'s rule: this line can start a drop (a `docker_in` fragment over the bound),
        // and the checkpoint must then stay at its start.
        if !tracked.decoder.holds_entry() {
            tracked.held_from = None;
        } else if tracked.held_from.is_none() {
            tracked.held_from = Some(partial_start);
        }
        let resource = match decoded {
            Ok(resource) => resource,
            Err(err) => {
                // Whatever the decoder flushed before rejecting the line is still emitted.
                diag.warn_throttled("bad_line", err);
                tracked.decoder.resource()
            }
        };
        if let Some((batch, reason)) = tracked.accumulator.absorb(resource, None, &mut scratch) {
            if !emit(sink, telemetry, batch, reason).await {
                return false;
            }
        }
    }

    if !hold_held {
        tracked.decoder.close(&mut scratch);
        // `close` emits held lines but not a line being dropped, so `held_from` survives only for
        // a drop in progress, and the checkpoint stays at that line's start.
        if !tracked.decoder.holds_entry() {
            tracked.held_from = None;
        }
    }
    if !scratch.is_empty() {
        let resource = tracked.decoder.resource();
        if let Some((batch, reason)) = tracked.accumulator.absorb(resource, None, &mut scratch) {
            return emit(sink, telemetry, batch, reason).await;
        }
    }
    true
}

/// Reads a newly opened file's first `n` bytes from its current position, `0`. A short file is
/// an error (`UnexpectedEof`).
async fn read_head(file: &mut tokio::fs::File, n: usize) -> std::io::Result<Vec<u8>> {
    let mut buf = vec![0u8; n];
    file.read_exact(&mut buf).await?;
    Ok(buf)
}

/// Appends to `head` the part of `chunk`, read at file offset `chunk_start`, that falls below
/// [`HEAD_BYTES`] and past what `head` already holds. `TrackedFile::head`'s invariant puts
/// `chunk_start` at or below `head.len()` whenever the head is short of `HEAD_BYTES`.
fn capture_head(head: &mut Vec<u8>, chunk_start: u64, chunk: &[u8]) {
    let have = head.len() as u64;
    let chunk_end = chunk_start + chunk.len() as u64;
    if have >= HEAD_BYTES as u64 || chunk_end <= have {
        return;
    }
    debug_assert!(chunk_start <= have, "a gap between the head and the chunk");
    let from = (have - chunk_start) as usize;
    let to = (chunk_end.min(HEAD_BYTES as u64) - chunk_start) as usize;
    head.extend_from_slice(&chunk[from..to]);
}

/// The `resume_rejected` diagnostic's text.
fn resume_rejected_message(path: &Path, offset: u64) -> String {
    format!(
        "{}: the retained offset {offset} doesn't match this file (a recycled inode, or \
         rewritten while not tailed) -- reading from the beginning",
        path.display()
    )
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

/// Sends one batch, returning whether a consumer took it. A refused batch is counted
/// `logit.input.batches.dropped{reason="closed_consumer"}`; the caller stops emitting after it.
async fn emit(
    sink: &Fanout,
    telemetry: &Telemetry,
    batch: EventBatch,
    reason: FlushReason,
) -> bool {
    telemetry.count("logit.component.receive.flushed", 1.0, &[("reason", reason.as_str())]);
    let taken = sink.send(batch).await;
    if !taken {
        telemetry.count("logit.input.batches.dropped", 1.0, &[("reason", "closed_consumer")]);
    }
    taken
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
mod verification;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tail::line::LineDecoder;
    use crate::tail::test_support::scratch_dir;
    use crate::tail::{ReadFrom, TailBatching, WatchMode};
    use logit_core::Resource;
    use logit_pipeline::test_util::{
        assert_no_batch, fanout_channel, recv_events, wait_until, Running, TelemetryProbe,
        RECV_TIMEOUT,
    };
    use logit_pipeline::{unwrap_batch, Delivered};
    use std::io::Write;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;

    /// A stand-in for `tail_in`'s private `LineDecoderFactory`.
    pub(super) struct LineFactory;

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
    pub(super) fn no_timer_due() -> tokio::time::Instant {
        tokio::time::Instant::now() + Duration::from_secs(3600)
    }

    /// Poll/checkpoint/flush intervals short enough for a test to see real ticks within a couple
    /// hundred milliseconds.
    pub(super) fn fast_config(read_from: ReadFrom) -> TailConfig {
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

    /// Runs `tailer` on its own task without binding it first; `run_until_shutdown` binds it
    /// there. `test_util::spawn_input` binds first, but takes an `Input`, which `Tailer` isn't.
    fn spawn_tailer<F: DecoderFactory<LineDecoder> + 'static>(
        mut tailer: Tailer<LineDecoder, F>,
        sink: Fanout,
    ) -> Running {
        let (shutdown, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { tailer.run_until_shutdown(sink, rx).await });
        Running { shutdown, handle }
    }

    pub(super) fn messages(events: &[Event]) -> Vec<String> {
        events
            .iter()
            .map(|e| e.log.as_ref().unwrap().message.as_str().unwrap().to_string())
            .collect()
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

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["line one"]);

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn tail_in_emits_one_event_per_line_with_read_time_and_log_file_path() {
        let dir = scratch_dir("emit-basic");
        let path = dir.join("app.log");
        std::fs::write(&path, b"line one\nline two\n").unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let before = now_nanos();
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 2).await;
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

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn read_from_end_skips_preexisting_lines_and_read_from_beginning_replays_them() {
        let dir = scratch_dir("read-from");
        let path = dir.join("app.log");
        std::fs::write(&path, b"old line\n").unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let tailer =
            Tailer::new(vec![PathPattern::new(&path)], LineFactory, fast_config(ReadFrom::End));
        // Bound first: the initial scan must open the file at its end before the append, or the
        // append could be skipped.
        let running = spawn_bound_tailer(tailer, fanout).await;

        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"new line\n")
            .unwrap();

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["new line"], "the pre-existing line must be skipped");
        running.stop().await;

        // A fresh tailer over the same file, `read_from: beginning`, must replay everything.
        let (fanout2, mut rx2) = fanout_channel(8);
        let tailer2 = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let running2 = spawn_tailer(tailer2, fanout2);
        let events2 = recv_events(&mut rx2, 2).await;
        assert_eq!(messages(&events2), vec!["old line", "new line"]);
        running2.stop().await;

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_file_created_after_startup_is_discovered_and_read_from_the_beginning() {
        let dir = scratch_dir("new-file");
        let (fanout, mut rx) = fanout_channel(8);
        // `read_from: end` governs only files present at startup.
        let tailer = Tailer::new(
            vec![PathPattern::new(dir.join("*.log"))],
            LineFactory,
            fast_config(ReadFrom::End),
        );
        // Bound first: a file already present at the initial scan would be skipped by
        // `read_from: end`.
        let running = spawn_bound_tailer(tailer, fanout).await;

        std::fs::write(dir.join("app.log"), b"first\nsecond\n").unwrap();

        let events = recv_events(&mut rx, 2).await;
        assert_eq!(messages(&events), vec!["first", "second"]);

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn rotation_by_rename_drains_the_old_inode_then_follows_the_new_one() {
        let dir = scratch_dir("rotation");
        let path = dir.join("app.log");
        std::fs::write(&path, b"before\n").unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["before"]);

        // Both land within one scan, so the driver sees a new inode at the path, not a removal.
        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"after\n").unwrap();

        let events2 = recv_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events2),
            vec!["after"],
            "the new inode should be followed from its own beginning"
        );

        running.stop().await;
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
    ) -> Running {
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

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, inotify_only_config());
        let running = spawn_bound_tailer(tailer, fanout).await;

        append(&path, b"before\n");
        let events = tokio::time::timeout(Duration::from_secs(3), recv_events(&mut rx, 1))
            .await
            .expect("the original file's own watch should deliver this well within 3s");
        assert_eq!(messages(&events), vec!["before"]);

        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"after\n").unwrap();

        let events2 = tokio::time::timeout(Duration::from_secs(3), recv_events(&mut rx, 1))
            .await
            .expect("the directory watch should discover the replacement well within 3s");
        assert_eq!(messages(&events2), vec!["after"]);

        append(&path, b"more\n");
        let events3 =
            tokio::time::timeout(Duration::from_secs(3), recv_events(&mut rx, 1)).await.expect(
                "the new inode's own watch should deliver this well within 3s, nowhere near the \
                 30s poll_interval",
            );
        assert_eq!(messages(&events3), vec!["more"]);

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn truncation_seeks_to_zero_and_reports_truncated() {
        let dir = scratch_dir("truncate");
        let path = dir.join("app.log");
        std::fs::write(&path, b"aaaaaaaaaa\n").unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["aaaaaaaaaa"]);

        // `O_TRUNC` on the existing path: same inode, shorter length.
        std::fs::write(&path, b"new\n").unwrap();

        let events2 = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events2), vec!["new"]);

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_removed_file_is_drained_and_closed() {
        let dir = scratch_dir("removed");
        let path = dir.join("app.log");
        std::fs::write(&path, b"only line\n").unwrap();

        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("tail_in", "tail_in", "listener");

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        )
        .with_telemetry(telemetry);
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["only line"]);
        assert_eq!(probe.gauge("logit.input.files.open", &[]), Some(1.0));

        std::fs::remove_file(&path).unwrap();
        wait_until("files.open to drop to 0 once the removed file is reaped", || {
            probe.gauge("logit.input.files.open", &[]) == Some(0.0)
        })
        .await;

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `logit.input.watch.watches` counts the watched directory plus each open file's watch.
    #[tokio::test]
    async fn watch_watches_counts_the_directory_and_each_open_file() {
        let dir = scratch_dir("watch-count");
        let path = dir.join("app.log");
        std::fs::write(&path, b"line\n").unwrap();

        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("tail_in", "tail_in", "listener");

        let (fanout, mut rx) = fanout_channel(8);
        let mut config = fast_config(ReadFrom::Beginning);
        config.watch = WatchMode::Inotify;
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config)
            .with_telemetry(telemetry);
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["line"]);
        assert_eq!(
            probe.gauge("logit.input.watch.watches", &[]),
            Some(2.0),
            "the watched directory plus the one open file"
        );

        std::fs::remove_file(&path).unwrap();
        wait_until(
            "watches to fall back to just the watched directory once the removed file is reaped",
            || probe.gauge("logit.input.watch.watches", &[]) == Some(1.0),
        )
        .await;

        running.stop().await;
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
        let mut probe = TelemetryProbe::new();
        let (fanout, mut rx) = fanout_channel(8);
        let tailer =
            Tailer::new(vec![PathPattern::new(&path)], factory, fast_config(ReadFrom::Beginning))
                .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["one"]);

        deselected.store(true, std::sync::atomic::Ordering::SeqCst);
        // Written before the reap, the append would be read and fail the negative assertion.
        wait_until("the next scan to notice the de-selection and reap the file", || {
            probe.gauge("logit.input.files.open", &[]) == Some(0.0)
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

        running.stop().await;
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
        let mut probe = TelemetryProbe::new();
        let (fanout, mut rx) = fanout_channel(8);
        let tailer =
            Tailer::new(vec![PathPattern::new(&path)], factory, fast_config(ReadFrom::Beginning))
                .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["one"]);

        deselected.store(true, std::sync::atomic::Ordering::SeqCst);
        wait_until("the next scan to notice the de-selection and reap the file", || {
            probe.gauge("logit.input.files.open", &[]) == Some(0.0)
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

        let events2 = recv_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events2),
            vec!["two"],
            "must resume from the retained offset -- \"one\" must never be replayed"
        );

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A file deselected while a line is being dropped retains that line's start, so the
    /// re-selected file drops it whole again instead of emitting its tail.
    #[tokio::test]
    async fn a_file_deselected_mid_drop_retains_the_dropped_lines_start() {
        let dir = scratch_dir("deselect-mid-drop");
        let path = dir.join("app.log");
        // "one\n" fits a limit of 4; "toolo" is over it with its newline not yet written.
        std::fs::write(&path, b"one\ntoolo").unwrap();

        let deselected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory = SelectiveFactory { deselected: deselected.clone() };
        let mut probe = TelemetryProbe::new();
        let diag = Diagnostics::new("test");
        let mut config = fast_config(ReadFrom::Beginning);
        config.max_line_bytes = 4;
        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], factory, config)
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"))
            .with_diagnostics(diag.clone());
        let running = spawn_tailer(tailer, fanout);

        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["one"]);
        assert_eq!(diag.occurrences("long_line"), 1, "the read reached the oversized line");

        deselected.store(true, std::sync::atomic::Ordering::SeqCst);
        wait_until("the next scan to notice the de-selection and reap the file", || {
            probe.gauge("logit.input.files.open", &[]) == Some(0.0)
        })
        .await;

        append(&path, b"ng\nok\n");
        deselected.store(false, std::sync::atomic::Ordering::SeqCst);

        assert_eq!(
            messages(&recv_events(&mut rx, 1).await),
            vec!["ok"],
            "the dropped line's tail must not be emitted as a line"
        );
        assert_eq!(diag.occurrences("long_line"), 2, "the re-selected file drops the line again");

        running.stop().await;
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

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone());
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["line one"]);

        // "line one\n" is 9 bytes; an offset of 0 would be a tick that landed before the read.
        wait_until("an interval tick after the read to write the dirty checkpoint", || {
            checkpointed_offset(&checkpoint_path) == Some(9)
        })
        .await;

        running.stop().await;

        // A fresh tailer resumes from the checkpoint: only the appended line arrives.
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"line two\n")
            .unwrap();
        let (fanout2, mut rx2) = fanout_channel(8);
        let tailer2 = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let running2 = spawn_tailer(tailer2, fanout2);
        let events2 = recv_events(&mut rx2, 1).await;
        assert_eq!(messages(&events2), vec!["line two"]);
        running2.stop().await;

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

        let mut probe = TelemetryProbe::new();
        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&a_path), PathPattern::new(&b_path)],
            LineFactory,
            config,
        )
        .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        let running = spawn_tailer(tailer, fanout);

        let _ = recv_events(&mut rx, 2).await;
        wait_until("a checkpoint tick to record both files", || {
            std::fs::read_to_string(&checkpoint_path)
                .is_ok_and(|text| text.contains("a.log") && text.contains("b.log"))
        })
        .await;

        std::fs::remove_file(&b_path).unwrap();
        // Shutdown's forced write drops `b.log` too, but only if the reap came first.
        wait_until("the removed file to be reaped", || {
            probe.gauge("logit.input.files.open", &[]) == Some(1.0)
        })
        .await;

        running.stop().await;

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

        let mut probe = TelemetryProbe::new();
        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config)
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        let running = spawn_tailer(tailer, fanout);

        // Shutdown before the read would leave nothing to flush and make the negative assertion
        // vacuous.
        probe
            .wait_for("the initial scan and read to take all three lines", |t| {
                t.sum("logit.input.lines", &[]) >= 3.0
            })
            .await;
        assert!(
            rx.try_recv().is_err(),
            "nothing should have flushed yet -- both intervals are 60s away"
        );

        running.stop().await;

        let delivered = rx.try_recv().expect("shutdown should have flushed the buffered batch");
        let events = unwrap_batch(delivered).events;
        assert_eq!(messages(&events), vec!["one", "two", "three"]);
        assert!(checkpoint_path.exists(), "shutdown should force a checkpoint write");

        std::fs::remove_dir_all(&dir).ok();
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
            ("truncated", br#"{"version":2,"files":[{"dev":"#, false),
            ("wrong-version", br#"{"version":99,"files":[]}"#, false),
            ("stray-tmp", br#"{"version":2,"#, true),
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
            let mut probe = TelemetryProbe::new();
            let telemetry = probe.telemetry("tail_in", "tail_in", "listener");
            let diag = Diagnostics::new("test");
            let (fanout, mut rx) = fanout_channel(8);
            let tailer = Tailer::new(
                vec![PathPattern::new(&a_path), PathPattern::new(&b_path)],
                LineFactory,
                config,
            )
            .with_diagnostics(diag.clone())
            .with_telemetry(telemetry);
            let running = spawn_tailer(tailer, fanout);

            let mut got = messages(&recv_events(&mut rx, 3).await);
            got.sort();
            assert_eq!(got, vec!["a1", "a2", "b1"], "{label}: every pre-existing line replays");
            running.stop().await;

            assert_eq!(
                probe.sum("logit.input.checkpoint.errors", &[("op", "load")]),
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
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("tail_in", "tail_in", "listener");
        let diag = Diagnostics::new("test");

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config)
            .with_diagnostics(diag.clone())
            .with_telemetry(telemetry);
        // Bound first: the initial scan must open the file at its end before the append, or the
        // append could be skipped.
        let running = spawn_bound_tailer(tailer, fanout).await;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"new line\n")
            .unwrap();

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["new line"], "the pre-existing line must be skipped");
        running.stop().await;

        assert_eq!(probe.sum("logit.input.checkpoint.errors", &[("op", "load")]), 0.0);
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

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone());
        let running = spawn_tailer(tailer, fanout);
        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["one"]);
        wait_until("the first checkpoint to cover \"one\"", || {
            std::fs::read_to_string(&checkpoint_path).is_ok_and(|t| t.contains("\"offset\": 4"))
        })
        .await;

        let scope = fault::scope(&dir);
        scope.crash_at(Point::new(sites::TAIL_CHECKPOINT, Op::Rename), 1);
        std::fs::OpenOptions::new().append(true).open(&path).unwrap().write_all(b"two\n").unwrap();
        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["two"]);
        wait_until("the next checkpoint write to reach its rename", || scope.crashed()).await;
        // The process is dead from here on: shutdown's forced write fails under the freeze too.
        running.stop().await;
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
        let (fanout2, mut rx2) = fanout_channel(8);
        let tailer2 = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let running2 = spawn_tailer(tailer2, fanout2);
        assert_eq!(
            messages(&recv_events(&mut rx2, 2).await),
            vec!["two", "three"],
            "\"two\" is delivered again, \"one\" is not, and nothing is skipped"
        );
        running2.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn an_unterminated_last_line_is_held_until_its_newline_arrives_and_emitted_on_close() {
        let dir = scratch_dir("unterminated");
        let path = dir.join("app.log");
        std::fs::write(&path, b"complete\nno newline yet").unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["complete"]);

        // Held while unterminated.
        let more = tokio::time::timeout(Duration::from_millis(80), rx.recv()).await;
        assert!(more.is_err(), "an unterminated line must not be emitted before it closes");

        // Shutdown emits the held partial.
        running.stop().await;
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

        let (fanout, mut rx) = fanout_channel(1); // tiny capacity forces backpressure
        let mut config = fast_config(ReadFrom::Beginning);
        config.batching.max_events = 1; // one event per batch -- easy to force a stall
        let mut probe = TelemetryProbe::new();
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config)
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        let running = spawn_tailer(tailer, fanout);

        // Don't read `rx` yet: the tailer must stall, not drop. The first line's batch fills the
        // channel, so a second line read means the tailer is parked in `emit` with the rest of
        // the file behind it.
        probe
            .wait_for("the tailer to read a second line and park on the full channel", |t| {
                t.sum("logit.input.lines", &[]) >= 2.0
            })
            .await;

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

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn invalid_utf8_is_lossily_converted_and_diagnosed() {
        let dir = scratch_dir("invalid-utf8");
        let path = dir.join("app.log");
        let mut content = b"good\n".to_vec();
        content.extend_from_slice(b"\xff\xfebad\n"); // invalid UTF-8 before the newline
        std::fs::write(&path, &content).unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 2).await;
        assert_eq!(events[0].log.as_ref().unwrap().message.as_str(), Some("good"));
        let second = events[1].log.as_ref().unwrap().message.as_str().unwrap().to_string();
        assert!(
            second.contains('\u{FFFD}'),
            "invalid UTF-8 should become the replacement character"
        );
        assert!(second.ends_with("bad"));

        running.stop().await;
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

        let (fanout, mut rx) = fanout_channel(64);
        let mut config = fast_config(ReadFrom::Beginning);
        config.batching.max_events = 1;
        let tailer = Tailer::new(
            vec![PathPattern::new(&busy_path), PathPattern::new(&quick_path)],
            LineFactory,
            config,
        );
        let running = spawn_tailer(tailer, fanout);

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

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Under `inotify`, a new file is discovered long before the 30s `poll_interval`, so only the
    /// wake source can explain it.
    #[tokio::test]
    async fn under_inotify_a_new_file_is_discovered_well_before_the_poll_interval() {
        let dir = scratch_dir("inotify-latency");

        let (fanout, mut rx) = fanout_channel(8);
        let mut config = fast_config(ReadFrom::End);
        config.watch = WatchMode::Inotify;
        config.poll_interval = Duration::from_secs(30);
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config);
        // Bound first: the initial scan must arm the directory watch before the write.
        let running = spawn_bound_tailer(tailer, fanout).await;

        std::fs::write(dir.join("app.log"), b"woke\n").unwrap();

        let events =
            tokio::time::timeout(Duration::from_secs(3), recv_events(&mut rx, 1)).await.expect(
                "inotify should discover the new file well within 3s, nowhere near the 30s \
                 poll_interval",
            );
        assert_eq!(messages(&events), vec!["woke"]);

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Under `poll`, a new file waits for the `poll_interval` tick. The 15ms flush timer can't
    /// discover a file (`Outcome::Flush` never scans), so it can't make this pass by accident.
    #[tokio::test]
    async fn under_poll_a_new_file_is_discovered_only_after_the_poll_interval() {
        let dir = scratch_dir("poll-latency");

        let (fanout, mut rx) = fanout_channel(8);
        let mut config = fast_config(ReadFrom::End);
        config.watch = WatchMode::Poll;
        config.poll_interval = Duration::from_secs(1);
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config);
        // Bound first: a file already present at the initial scan would be skipped by
        // `read_from: end`.
        let running = spawn_bound_tailer(tailer, fanout).await;

        std::fs::write(dir.join("app.log"), b"woke\n").unwrap();

        // Nothing before the 1s tick. The 150ms window is 10x the 15ms flush tick that would
        // deliver a wrongly discovered file, and the tick is over 6x the window away, so
        // scheduler lag can't carry the window into it.
        assert_no_batch(
            &mut rx,
            Duration::from_millis(150),
            "poll mode must not discover a new file before its own poll_interval tick",
        )
        .await;

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["woke"]);

        running.stop().await;
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

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(dir.join("*.log"))],
            LineFactory,
            inotify_only_config(),
        );
        let running = spawn_bound_tailer(tailer, fanout).await;

        append(&path, b"first\n");
        let first = tokio::time::timeout(Duration::from_secs(3), recv_events(&mut rx, 1))
            .await
            .expect("the file's own watch should deliver this well within 3s");
        assert_eq!(messages(&first), vec!["first"]);

        append(&path, b"second\n");
        let second =
            tokio::time::timeout(Duration::from_secs(3), recv_events(&mut rx, 1)).await.expect(
                "a write to an already-tracked file should be delivered well within 3s, nowhere \
                 near the 30s poll_interval, via the file's own watch",
            );
        assert_eq!(messages(&second), vec!["second"]);

        running.stop().await;
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

        let mut probe = TelemetryProbe::new();
        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(dir.join("*.log"))],
            LineFactory,
            inotify_only_config(),
        )
        .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        let running = spawn_bound_tailer(tailer, fanout).await;

        append(&path, b"first\n");
        let first = tokio::time::timeout(Duration::from_secs(3), recv_events(&mut rx, 1))
            .await
            .expect("the file's own watch should deliver this well within 3s");
        assert_eq!(messages(&first), vec!["first"]);

        // `ftruncate(2)`: same inode, length 0, one `IN_MODIFY`.
        std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(0).unwrap();
        probe
            .wait_for("the truncation's own wake to be handled before the replacement", |t| {
                t.sum("logit.input.files.truncated", &[]) >= 1.0
            })
            .await;
        append(&path, b"a-much-longer-replacement-line\n");

        let second =
            tokio::time::timeout(Duration::from_secs(3), recv_events(&mut rx, 1)).await.expect(
                "the replacement should be delivered well within 3s via the file's own watch, \
                 nowhere near the 30s poll_interval",
            );
        assert_eq!(
            messages(&second),
            vec!["a-much-longer-replacement-line"],
            "the truncation's own wake should have rewound the file to 0"
        );
        assert_eq!(probe.sum("logit.input.files.truncated", &[]), 1.0);

        running.stop().await;
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

        let (fanout, mut rx) = fanout_channel(8);
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
        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["a-longer-first-line"]);

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
        let (fanout, mut rx) = fanout_channel(1);
        let mut config = fast_config(ReadFrom::Beginning);
        config.batching.max_events = 1;
        let mut probe = TelemetryProbe::new();
        // A glob, so the removal is what drives the file into `FileState::Draining`.
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config)
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        // Bound first: the initial scan must open the file before it's removed, or the glob never
        // finds it. `rx` stays unconsumed.
        let running = spawn_bound_tailer(tailer, fanout).await;
        std::fs::remove_file(&path).unwrap();
        // A second line read means the tailer is parked in `emit` on the full channel, inside
        // its first chunk. The first poll tick after the test starts consuming marks the file
        // `Draining`, and `drain` yields to that tick after its first pass at the latest, so the
        // second chunk is still unread when it lands. The assertion below holds whichever pass
        // the tick lands in; the wait makes the stall, not a sleep, set up that order.
        probe
            .wait_for("the tailer to read a second line and park on the full channel", |t| {
                t.sum("logit.input.lines", &[]) >= 2.0
            })
            .await;

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

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_truncation_discards_the_partial_line_held_from_the_previous_generation() {
        let dir = scratch_dir("truncate-partial");
        let path = dir.join("app.log");
        // "partialpartial" is held as a partial, and makes the offset (18) exceed the
        // post-truncation length (10), so `scan` sees a truncation.
        std::fs::write(&path, b"a\nb\npartialpartial").unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 2).await;
        assert_eq!(messages(&events), vec!["a", "b"]);

        // `O_TRUNC` on the existing path: same inode, 10 < 18 bytes.
        std::fs::write(&path, b"restarted\n").unwrap();

        let events2 = recv_events(&mut rx, 1).await;
        assert_eq!(
            messages(&events2),
            vec!["restarted"],
            "the pre-truncation partial must not be spliced onto the first post-truncation line"
        );

        running.stop().await;
        assert!(rx.try_recv().is_err(), "the discarded partial must not resurface on close");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_wildcard_matching_both_a_rotated_file_and_its_replacement_keeps_one_entry_per_inode()
    {
        let dir = scratch_dir("wildcard-rotation");
        let path = dir.join("app.log");
        std::fs::write(&path, b"before\n").unwrap();

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(dir.join("app.log*"))],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let running = spawn_tailer(tailer, fanout);

        let events = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events), vec!["before"]);

        // `app.log*` now matches both the rotated-away file and its replacement.
        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"after\n").unwrap();

        let events2 = recv_events(&mut rx, 1).await;
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
        let events3 = recv_events(&mut rx, 1).await;
        assert_eq!(messages(&events3), vec!["late"]);

        running.stop().await;
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

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let running = spawn_tailer(tailer, fanout);

        // The flush timer is 60s out, so only the checkpoint tick can deliver within 5s.
        let events = recv_events(&mut rx, 3).await;
        assert_eq!(messages(&events), vec!["one", "two", "three"]);

        running.stop().await;
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
        let (fanout, mut rx) = fanout_channel(8);
        let running = spawn_tailer(tailer, fanout);

        let _ = recv_events(&mut rx, 3).await;
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

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A checkpoint taken while an oversized line is being dropped stays at that line's start,
    /// so a crash there and a restart drop the line whole again instead of emitting its tail.
    #[tokio::test]
    async fn a_checkpoint_taken_mid_drop_stays_at_the_dropped_lines_start() {
        let dir = scratch_dir("checkpoint-mid-drop");
        let path = dir.join("app.log");
        let checkpoint_path = dir.join("checkpoint.json");
        // "one\n" is 4 bytes; "toolo" is over the limit with its newline not yet written.
        std::fs::write(&path, b"one\ntoolo").unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.max_line_bytes = 4;
        let diag = Diagnostics::new("test");
        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone())
            .with_diagnostics(diag.clone());
        let running = spawn_tailer(tailer, fanout);

        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["one"]);
        assert_eq!(diag.occurrences("long_line"), 1);
        // Both lines come from one read, so any checkpoint after it is 4, or 9 inside the drop.
        wait_until("a checkpoint written after the read", || {
            checkpointed_offset(&checkpoint_path).is_some_and(|offset| offset > 0)
        })
        .await;
        assert_eq!(checkpointed_offset(&checkpoint_path), Some(4));

        // Restore the interval checkpoint after the stop, as a crash here would leave it, so the
        // restart doesn't depend on shutdown's forced write.
        let crashed = std::fs::read(&checkpoint_path).unwrap();
        running.stop().await;
        std::fs::write(&checkpoint_path, crashed).unwrap();

        append(&path, b"ng\nok\n");
        let diag = Diagnostics::new("test");
        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config)
            .with_diagnostics(diag.clone());
        let running = spawn_tailer(tailer, fanout);

        assert_eq!(
            messages(&recv_events(&mut rx, 1).await),
            vec!["ok"],
            "the dropped line's tail must not be emitted as a line"
        );
        assert_eq!(diag.occurrences("long_line"), 1, "the restart drops the line again");

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Shutdown's forced checkpoint also stays at the start of a line being dropped, since
    /// `take_partial` leaves the drop in place.
    #[tokio::test]
    async fn a_shutdown_mid_drop_checkpoints_at_the_dropped_lines_start() {
        let dir = scratch_dir("shutdown-mid-drop");
        let path = dir.join("app.log");
        let checkpoint_path = dir.join("checkpoint.json");
        // "one\n" is 4 bytes; "toolo" is over the limit with its newline not yet written.
        std::fs::write(&path, b"one\ntoolo").unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.max_line_bytes = 4;
        let diag = Diagnostics::new("test");
        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone())
            .with_diagnostics(diag.clone());
        let running = spawn_tailer(tailer, fanout);

        assert_eq!(messages(&recv_events(&mut rx, 1).await), vec!["one"]);
        assert_eq!(diag.occurrences("long_line"), 1, "the read reached the oversized line");
        running.stop().await;
        assert_eq!(checkpointed_offset(&checkpoint_path), Some(4));

        append(&path, b"ng\nok\n");
        let diag = Diagnostics::new("test");
        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config)
            .with_diagnostics(diag.clone());
        let running = spawn_tailer(tailer, fanout);

        assert_eq!(
            messages(&recv_events(&mut rx, 1).await),
            vec!["ok"],
            "the dropped line's tail must not be emitted as a line"
        );
        assert_eq!(diag.occurrences("long_line"), 1, "the restart drops the line again");

        running.stop().await;
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
        probe: &TelemetryProbe,
    ) -> (Tailer<LineDecoder, LineFactory>, crate::tail::watch::Watcher) {
        let diag = Diagnostics::new("tail_in")
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        let mut config = fast_config(ReadFrom::Beginning);
        config.watch = WatchMode::Inotify;
        config.poll_interval = Duration::from_secs(30);
        let tailer = Tailer::new(vec![pattern], LineFactory, config)
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"))
            .with_diagnostics(diag);
        let watcher =
            crate::tail::watch::Watcher::new(WatchMode::Inotify, &mut Diagnostics::default())
                .expect("inotify should be available in the dev container");
        (tailer, watcher)
    }

    /// Whether a `Diagnostics` key was reported; `warn_throttled` counts
    /// `logit.component.diagnostics{key}` on every occurrence, logged or not.
    #[cfg(target_os = "linux")]
    fn diagnosed(probe: &mut TelemetryProbe, key: &str) -> bool {
        probe.poll().has("logit.component.diagnostics", &[("key", key)])
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
        let mut probe = TelemetryProbe::new();
        let (mut tailer, mut watcher) = inotify_tailer(PathPattern::new(sub.join("*.log")), &probe);

        tailer.scan(true, &mut watcher).await;
        assert_eq!(
            watcher.tracked_watch_count(),
            0,
            "there is nothing to watch yet -- and nothing may be recorded as watched either"
        );
        assert!(tailer.watched_dirs.is_empty(), "{:?}", tailer.watched_dirs);
        assert!(
            diagnosed(&mut probe, "watch_dir_error"),
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
        let probe = TelemetryProbe::new();
        let (mut tailer, mut watcher) = inotify_tailer(PathPattern::new(dir.join("*.log")), &probe);

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
        let probe = TelemetryProbe::new();
        let (mut tailer, mut watcher) = inotify_tailer(PathPattern::new(dir.join("*.log")), &probe);

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
    #[tokio::test(start_paused = true)]
    async fn the_live_kernel_watch_count_matches_this_watchers_own_bookkeeping() {
        let dir = scratch_dir("inotify-watch-leak");
        let probe = TelemetryProbe::new();
        let (mut tailer, mut watcher) = inotify_tailer(PathPattern::new(dir.join("*.log")), &probe);
        let (fanout, _rx) = fanout_channel(256);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let path = dir.join("app.log");

        tailer.scan(true, &mut watcher).await;

        for i in 0..20 {
            std::fs::write(&path, format!("line {i}\n")).unwrap();
            tailer.scan(false, &mut watcher).await;
            tailer.drain(&fanout, &shutdown_rx, &mut watcher, no_timer_due()).await;

            // Rotate out of `*.log`'s reach, reap, then delete: one watch added and one removed
            // per cycle, plus a queued `IN_IGNORED`. The reap needs the grace to pass and a scan
            // after the retiring one.
            std::fs::rename(&path, dir.join("app.log.1")).unwrap();
            tailer.scan(false, &mut watcher).await;
            tokio::time::advance(tailer.config.poll_interval).await;
            tailer.scan(false, &mut watcher).await;
            tailer.drain(&fanout, &shutdown_rx, &mut watcher, no_timer_due()).await;
            assert_eq!(tailer.tracked_len(), 0, "cycle {i}: the rotated file is reaped");
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

        let mut probe = TelemetryProbe::new();
        let (fanout, mut rx) = fanout_channel(1);
        let filler = fanout.clone();
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config)
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        let mut running = spawn_tailer(tailer, fanout);

        // The first checkpoint tick flushes both lines, then records their 8 bytes.
        assert_eq!(messages(&recv_events(&mut rx, 2).await), vec!["one", "two"]);
        wait_until("the interval checkpoint to record the first two lines", || {
            checkpointed_offset(&checkpoint_path) == Some(8)
        })
        .await;

        // Fill the channel before appending: with it full, the next flush parks, and a run loop
        // parked in a flush never observes shutdown.
        filler.send(filler_batch()).await;
        let before = probe.sum("logit.input.lines", &[]);
        append(&path, b"three\nfour\n");
        probe
            .wait_for("the appended lines to be read into the accumulator", |t| {
                t.sum("logit.input.lines", &[]) >= before + 2.0
            })
            .await;

        let _ = running.shutdown.send(true);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), &mut running.handle).await.is_err(),
            "the final flush should be parked on the full channel"
        );
        // `run_input`'s grace backstop drops the task here.
        running.handle.abort();
        let _ = running.handle.await;

        assert!(
            probe.sum("logit.component.receive.flushed", &[("reason", "shutdown")]) >= 1.0,
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

        let (fanout, mut rx) = fanout_channel(1);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let running = spawn_tailer(tailer, fanout);

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

        running.stop().await;
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

        let (fanout, mut rx) = fanout_channel(1);
        let tailer = Tailer::new(
            vec![PathPattern::new(&busy), PathPattern::new(&quiet)],
            LineFactory,
            config,
        );
        let running = spawn_tailer(tailer, fanout);

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

        drop(rx); // the driver then stops at its next emit
        running.stop().await;
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

        let (fanout, mut rx) = fanout_channel(1);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone());
        let mut running = spawn_tailer(tailer, fanout);

        // Into the second chunk, then the downstream stops.
        let delivered =
            consume_slowly(&mut rx, Duration::from_millis(1), |e| e.len() >= 6_000).await.len();
        let _ = running.shutdown.send(true);
        assert!(
            tokio::time::timeout(Duration::from_secs(2), &mut running.handle).await.is_err(),
            "the run loop should be parked in emit"
        );
        running.handle.abort();
        let _ = running.handle.await;
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
        let (fanout2, mut rx2) = fanout_channel(8);
        let tailer2 = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let running2 = spawn_tailer(tailer2, fanout2);
        let first = recv_events(&mut rx2, 1).await;
        let resumed_at = offset / BACKLOG_LINE_BYTES;
        assert_eq!(messages(&first[..1]), vec![format!("backlog-{resumed_at:05}")]);
        drop(rx2);
        running2.stop().await;

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A downstream that closes mid-backlog stops the driver at its next emit, with no shutdown
    /// signal, and freezes the checkpoint: no interval or final write advances it past the lines
    /// a consumer took, so a restart replays from there rather than skipping what was refused.
    #[tokio::test(start_paused = true)]
    async fn a_closed_downstream_mid_backlog_stops_the_driver_and_freezes_the_checkpoint() {
        let dir = scratch_dir("closed-under-backlog");
        let path = dir.join("app.log");
        let checkpoint_path = dir.join("checkpoint.json");
        std::fs::write(&path, backlog(8_000).as_bytes()).unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());
        config.checkpoint_interval = Duration::from_millis(100);
        config.batching.max_events = 1;

        let mut probe = TelemetryProbe::new();
        let (fanout, mut rx) = fanout_channel(1);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone())
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        let mut running = spawn_tailer(tailer, fanout);

        // Into the second chunk, so interval checkpoints have landed, then the downstream closes.
        let delivered =
            consume_slowly(&mut rx, Duration::from_millis(1), |e| e.len() >= 6_000).await.len();
        drop(rx);

        tokio::time::timeout(RECV_TIMEOUT, &mut running.handle)
            .await
            .expect("the driver should stop on its own once no consumer takes a batch")
            .expect("the driver task panicked")
            .expect("a closed downstream is a clean finish, not an error");
        probe
            .wait_for("the refused batch to be counted", |t| {
                t.sum("logit.input.batches.dropped", &[("reason", "closed_consumer")]) == 1.0
            })
            .await;

        let offset = checkpointed_offset(&checkpoint_path)
            .expect("an interval checkpoint should have landed during the backlog");
        let delivered_bytes = delivered as u64 * BACKLOG_LINE_BYTES;
        assert!(
            offset <= delivered_bytes,
            "checkpoint {offset} covers lines past the {delivered} a consumer took"
        );
        assert_eq!(offset % BACKLOG_LINE_BYTES, 0, "the checkpoint must sit on a line boundary");

        // A restart resumes at or before the first line never taken.
        let (fanout2, mut rx2) = fanout_channel(8);
        let tailer2 = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let running2 = spawn_tailer(tailer2, fanout2);
        let first = recv_events(&mut rx2, 1).await;
        let resumed_at = offset / BACKLOG_LINE_BYTES;
        assert_eq!(messages(&first[..1]), vec![format!("backlog-{resumed_at:05}")]);
        drop(rx2);
        running2.stop().await;

        std::fs::remove_dir_all(&dir).ok();
    }

    /// Lines held in an accumulator when the downstream closes are refused by the flush tick
    /// (or the checkpoint tick's flush), which stops the driver before any checkpoint write can
    /// cover them.
    #[tokio::test(start_paused = true)]
    async fn a_closed_downstream_found_by_a_flush_tick_freezes_the_checkpoint() {
        let dir = scratch_dir("closed-at-flush");
        let path = dir.join("app.log");
        let checkpoint_path = dir.join("checkpoint.json");
        std::fs::write(&path, b"a\nb\n").unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint_path.clone());

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config.clone());
        let mut running = spawn_tailer(tailer, fanout);

        assert_eq!(messages(&recv_events(&mut rx, 2).await), vec!["a", "b"]);
        wait_until("a checkpoint covering the two delivered lines", || {
            checkpointed_offset(&checkpoint_path) == Some(4)
        })
        .await;

        // Read into the accumulator only: `max_events` is far away, so the flush tick is the
        // first emit.
        let mut file = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"c\nd\n").unwrap();
        drop(rx);

        tokio::time::timeout(RECV_TIMEOUT, &mut running.handle)
            .await
            .expect("the driver should stop on its own once no consumer takes a batch")
            .expect("the driver task panicked")
            .expect("a closed downstream is a clean finish, not an error");
        assert_eq!(
            checkpointed_offset(&checkpoint_path),
            Some(4),
            "neither an interval nor the final write may cover the refused lines"
        );

        let (fanout2, mut rx2) = fanout_channel(8);
        let tailer2 = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let running2 = spawn_tailer(tailer2, fanout2);
        assert_eq!(messages(&recv_events(&mut rx2, 2).await), vec!["c", "d"]);
        drop(rx2);
        running2.stop().await;

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

        let (fanout, mut rx) = fanout_channel(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&a_path), PathPattern::new(&b_path)],
            LineFactory,
            config,
        );
        let running = spawn_tailer(tailer, fanout);

        let _ = recv_events(&mut rx, 2).await;
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

        running.stop().await;
        std::fs::remove_dir_all(&dir).ok();
    }

    // -- a failed listing is no information (docs/adr/tail-discovery-failure-and-resume-identity.md)
    //
    // Each drives `scan`/`drain` by hand under `Watcher::Poll` and forces the failure through the
    // fault seam, so nothing waits on a timer.

    use crate::tail::pattern::{READ_DIR, STAT};
    use logit_pipeline::fault::{self, errno};

    /// A `Tailer` driven by hand, reporting its telemetry and diagnostics to `probe`.
    fn probed_tailer(
        patterns: Vec<PathPattern>,
        read_from: ReadFrom,
        probe: &TelemetryProbe,
    ) -> Tailer<LineDecoder, LineFactory> {
        let diag = Diagnostics::new("tail_in")
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        Tailer::new(patterns, LineFactory, fast_config(read_from))
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"))
            .with_diagnostics(diag)
    }

    /// One poll tick by hand: a `scan`, a `drain` to idle (which reaps), and a flush. Returns the
    /// messages it emitted.
    async fn tick<F: DecoderFactory<LineDecoder>>(
        tailer: &mut Tailer<LineDecoder, F>,
        first: bool,
    ) -> Vec<String> {
        let (fanout, mut rx) = fanout_channel(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut watcher = crate::tail::watch::Watcher::Poll;
        tailer.scan(first, &mut watcher).await;
        let _ = tailer.drain(&fanout, &shutdown_rx, &mut watcher, no_timer_due()).await;
        tailer.flush_all(&fanout, FlushReason::Interval).await;
        let mut out = Vec::new();
        while let Ok(delivered) = rx.try_recv() {
            out.extend(messages(&unwrap_batch(delivered).events));
        }
        out
    }

    /// Advances the paused clock past the grace `reap_drained` gives a `Draining` file, then runs
    /// one more [`tick`], which reaps every draining file at EOF.
    async fn after_grace<F: DecoderFactory<LineDecoder>>(
        tailer: &mut Tailer<LineDecoder, F>,
    ) -> Vec<String> {
        tokio::time::advance(tailer.config.poll_interval).await;
        tick(tailer, false).await
    }

    fn state_of<F: DecoderFactory<LineDecoder>>(
        tailer: &Tailer<LineDecoder, F>,
        path: &Path,
    ) -> Option<FileState> {
        let id = FileId::from_metadata(&std::fs::metadata(path).ok()?);
        tailer.files.get(&id).map(|f| f.state)
    }

    fn scan_errors(probe: &mut TelemetryProbe, op: &str) -> f64 {
        probe.sum("logit.input.scan.errors", &[("op", op)])
    }

    #[tokio::test]
    async fn a_failed_read_dir_retires_no_tracked_file_and_is_counted() {
        let dir = scratch_dir("scan-read-dir-fails");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer =
            probed_tailer(vec![PathPattern::new(dir.join("*.log"))], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);

        let scope = fault::scope(&dir);
        scope.fail(READ_DIR, errno::EACCES);
        assert!(tick(&mut tailer, false).await.is_empty());
        assert_eq!(state_of(&tailer, &path), Some(FileState::Active));
        assert!(tailer.by_path.contains_key(&path), "the binding survives the failed listing");
        assert_eq!(scan_errors(&mut probe, "read_dir"), 1.0);
        assert_eq!(scan_errors(&mut probe, "stat"), 0.0);
        assert!(diagnosed(&mut probe, "scan_error"));
        assert_eq!(probe.gauge("logit.input.files.open", &[]), Some(1.0));
        assert_eq!(probe.sum("logit.component.receive.flushed", &[("reason", "closed")]), 0.0);
        drop(scope);

        append(&path, b"two\n");
        assert_eq!(tick(&mut tailer, false).await, vec!["two"], "no replay after recovery");
        assert_eq!(scan_errors(&mut probe, "read_dir"), 1.0, "the clean scan counts nothing");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_failed_stat_other_than_not_found_keeps_the_file() {
        let dir = scratch_dir("scan-stat-fails");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);

        let scope = fault::scope(&dir);
        scope.fail(STAT, errno::EIO);
        append(&path, b"two\n");
        assert_eq!(tick(&mut tailer, false).await, vec!["two"], "a kept file is still read");
        assert_eq!(state_of(&tailer, &path), Some(FileState::Active));
        assert_eq!(scan_errors(&mut probe, "stat"), 1.0);
        assert_eq!(scan_errors(&mut probe, "read_dir"), 0.0);
        assert!(diagnosed(&mut probe, "scan_error"));
        drop(scope);

        append(&path, b"three\n");
        assert_eq!(tick(&mut tailer, false).await, vec!["three"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(start_paused = true)]
    async fn a_not_found_stat_still_drains_the_file() {
        let dir = scratch_dir("scan-stat-not-found");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);

        let scope = fault::scope(&dir);
        scope.fail(STAT, errno::ENOENT);
        append(&path, b"two\n");
        assert_eq!(tick(&mut tailer, false).await, vec!["two"], "drained to EOF before the reap");
        assert_eq!(state_of(&tailer, &path), Some(FileState::Draining));
        assert!(after_grace(&mut tailer).await.is_empty());
        assert_eq!(tailer.tracked_len(), 0, "ENOENT is an absence: the file is retired");
        assert_eq!(scan_errors(&mut probe, "stat"), 0.0);
        assert!(!diagnosed(&mut probe, "scan_error"));
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_missing_pattern_directory_is_not_a_scan_error() {
        let dir = scratch_dir("scan-missing-dir");
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(
            vec![PathPattern::new(dir.join("not-yet").join("*.log"))],
            ReadFrom::Beginning,
            &probe,
        );
        assert!(tick(&mut tailer, true).await.is_empty());
        assert!(tick(&mut tailer, false).await.is_empty());
        assert_eq!(scan_errors(&mut probe, "read_dir"), 0.0);
        assert_eq!(scan_errors(&mut probe, "stat"), 0.0);
        assert!(!diagnosed(&mut probe, "scan_error"));

        std::fs::create_dir(dir.join("not-yet")).unwrap();
        std::fs::write(dir.join("not-yet").join("app.log"), b"arrived\n").unwrap();
        assert_eq!(tick(&mut tailer, false).await, vec!["arrived"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A directory that is gone is an absence, so its files drain and close. Renamed back later,
    /// the same inodes are new discoveries and replay from `0`: nothing retained them.
    #[tokio::test(start_paused = true)]
    async fn a_pattern_directory_removed_drains_and_closes_its_files() {
        let dir = scratch_dir("scan-dir-removed");
        let sub = dir.join("sub");
        std::fs::create_dir(&sub).unwrap();
        std::fs::write(sub.join("app.log"), b"one\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer =
            probed_tailer(vec![PathPattern::new(sub.join("*.log"))], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);

        std::fs::remove_dir_all(&sub).unwrap();
        assert!(tick(&mut tailer, false).await.is_empty());
        assert!(after_grace(&mut tailer).await.is_empty());
        assert_eq!(tailer.tracked_len(), 0);
        // The gauge is sampled at the end of a scan, before that tick's reap.
        tick(&mut tailer, false).await;
        assert_eq!(probe.gauge("logit.input.files.open", &[]), Some(0.0));
        assert_eq!(scan_errors(&mut probe, "read_dir"), 0.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The `fstat` on a kept file's handle sees the unlink the failed listing couldn't.
    #[tokio::test(start_paused = true)]
    async fn a_file_deleted_under_a_failing_listing_is_drained_and_closed() {
        let dir = scratch_dir("scan-deleted-while-failing");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer =
            probed_tailer(vec![PathPattern::new(dir.join("*.log"))], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);

        append(&path, b"two\n");
        std::fs::remove_file(&path).unwrap();
        let scope = fault::scope(&dir);
        scope.fail(READ_DIR, errno::EACCES);
        assert_eq!(tick(&mut tailer, false).await, vec!["two"], "read to EOF before the reap");
        assert!(tailer.by_path.is_empty());
        assert_eq!(scan_errors(&mut probe, "read_dir"), 1.0);
        // No listing since it started draining could have rebound it, so it stays.
        assert!(after_grace(&mut tailer).await.is_empty());
        assert_eq!(tailer.tracked_len(), 1, "pinned while the listing fails");
        drop(scope);
        assert!(tick(&mut tailer, false).await.is_empty());
        assert_eq!(tailer.tracked_len(), 0, "reaped after a clean listing");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_truncation_under_a_failing_listing_is_still_detected() {
        let dir = scratch_dir("scan-truncated-while-failing");
        let path = dir.join("app.log");
        std::fs::write(&path, b"aaaaaaaaaa\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer =
            probed_tailer(vec![PathPattern::new(dir.join("*.log"))], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["aaaaaaaaaa"]);

        let scope = fault::scope(&dir);
        scope.fail(READ_DIR, errno::EACCES);
        std::fs::write(&path, b"new\n").unwrap(); // `O_TRUNC`: same inode, shorter
        assert_eq!(tick(&mut tailer, false).await, vec!["new"]);
        assert_eq!(probe.sum("logit.input.files.truncated", &[]), 1.0);
        assert_eq!(state_of(&tailer, &path), Some(FileState::Active));
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// One persistently failing directory doesn't hold back retirement under another pattern.
    #[tokio::test(start_paused = true)]
    async fn two_patterns_one_failing_still_retire_the_others_removed_file() {
        let failing = scratch_dir("scan-two-failing");
        let healthy = scratch_dir("scan-two-healthy");
        let a = failing.join("a.log");
        let b = healthy.join("b.log");
        std::fs::write(&a, b"a1\n").unwrap();
        std::fs::write(&b, b"b1\n").unwrap();
        let probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(
            vec![PathPattern::new(failing.join("*.log")), PathPattern::new(healthy.join("*.log"))],
            ReadFrom::Beginning,
            &probe,
        );
        assert_eq!(tick(&mut tailer, true).await.len(), 2);

        let scope = fault::scope(&failing);
        scope.fail(READ_DIR, errno::EACCES);
        std::fs::remove_file(&b).unwrap();
        tick(&mut tailer, false).await;
        after_grace(&mut tailer).await;
        assert_eq!(state_of(&tailer, &a), Some(FileState::Active));
        assert_eq!(tailer.tracked_len(), 1, "b.log is retired and reaped");
        assert!(!tailer.by_path.contains_key(&b));
        drop(scope);
        std::fs::remove_dir_all(&failing).ok();
        std::fs::remove_dir_all(&healthy).ok();
    }

    /// Two patterns list one directory, and only the second fails: a path only the listed one
    /// covers is retired, and one the failed one covers is kept.
    #[tokio::test(start_paused = true)]
    async fn two_patterns_sharing_a_directory_one_failing_retire_only_what_the_listed_one_covers() {
        let dir = scratch_dir("scan-shared-dir");
        let elsewhere = scratch_dir("scan-shared-dir-elsewhere");
        let log = dir.join("app.log");
        let txt = dir.join("app.txt");
        std::fs::write(&log, b"log\n").unwrap();
        std::fs::write(&txt, b"txt\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(
            vec![PathPattern::new(dir.join("*.log")), PathPattern::new(dir.join("*.txt"))],
            ReadFrom::Beginning,
            &probe,
        );
        assert_eq!(tick(&mut tailer, true).await.len(), 2);
        let txt_id = FileId::from_metadata(&std::fs::metadata(&txt).unwrap());

        // The directory is empty by the failing scan, so each pattern's listing is one `ReadDir`
        // point, in pattern order: the 2nd is `*.txt`'s.
        let scope = fault::scope(&dir);
        scope.fail_nth(READ_DIR, 2, errno::EACCES);
        std::fs::remove_file(&log).unwrap();
        // Moved, not unlinked, so the kept handle's `fstat` has no reason to retire it.
        std::fs::rename(&txt, elsewhere.join("app.txt")).unwrap();
        tick(&mut tailer, false).await;
        assert_eq!(scan_errors(&mut probe, "read_dir"), 1.0);
        assert!(!tailer.by_path.contains_key(&log), "*.log listed, so app.log is retired");
        assert_eq!(tailer.by_path.get(&txt), Some(&txt_id), "*.txt failed, so app.txt is kept");
        assert_eq!(tailer.files.get(&txt_id).map(|f| f.state), Some(FileState::Active));
        drop(scope);

        tick(&mut tailer, false).await;
        after_grace(&mut tailer).await;
        assert_eq!(tailer.tracked_len(), 0, "a clean listing without app.txt retires it");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&elsewhere).ok();
    }

    #[tokio::test]
    async fn a_path_matched_by_two_patterns_is_statted_once() {
        let dir = scratch_dir("scan-stat-once");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(
            vec![PathPattern::new(&path), PathPattern::new(dir.join("*.log"))],
            ReadFrom::Beginning,
            &probe,
        );

        let scope = fault::scope(&dir);
        scope.record();
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);
        let ops = |op| scope.hits().iter().filter(|h| h.point.op == op).count();
        // Per pattern: the `read_dir` itself and its one entry.
        assert_eq!(ops(fault::Op::ReadDir), 4, "one listing per pattern");
        assert_eq!(ops(fault::Op::Stat), 1, "one stat per distinct path");
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An error from the listing's iterator fails the listing: what it already returned isn't
    /// the whole directory, so nothing it left out is retired.
    #[tokio::test]
    async fn a_listing_that_fails_part_way_through_retires_nothing() {
        let dir = scratch_dir("scan-fails-part-way");
        for name in ["a.log", "b.log", "c.log"] {
            std::fs::write(dir.join(name), format!("{name}\n")).unwrap();
        }
        let mut probe = TelemetryProbe::new();
        let mut tailer =
            probed_tailer(vec![PathPattern::new(dir.join("*.log"))], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await.len(), 3);

        // The 1st `ReadDir` point is the `read_dir` call, the 2nd the first entry it yields.
        let scope = fault::scope(&dir);
        scope.fail_nth(READ_DIR, 2, errno::EIO);
        tick(&mut tailer, false).await;
        assert_eq!(scan_errors(&mut probe, "read_dir"), 1.0);
        assert!(diagnosed(&mut probe, "scan_error"));
        assert_eq!(tailer.tracked_len(), 3);
        for name in ["a.log", "b.log", "c.log"] {
            assert_eq!(state_of(&tailer, &dir.join(name)), Some(FileState::Active), "{name}");
        }
        drop(scope);

        append(&dir.join("b.log"), b"more\n");
        assert_eq!(tick(&mut tailer, false).await, vec!["more"], "no replay after recovery");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `read_from` applies to the bind scan only, so a file the bind scan couldn't list is a later
    /// discovery and starts at the beginning. The diagnostic says so.
    #[tokio::test]
    async fn a_listing_that_fails_at_bind_starts_its_files_at_the_beginning_later() {
        let dir = scratch_dir("scan-fails-at-bind");
        std::fs::write(dir.join("app.log"), b"old\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer =
            probed_tailer(vec![PathPattern::new(dir.join("*.log"))], ReadFrom::End, &probe);

        let scope = fault::scope(&dir);
        scope.fail(READ_DIR, errno::EMFILE);
        assert!(tick(&mut tailer, true).await.is_empty());
        assert_eq!(tailer.tracked_len(), 0);
        assert_eq!(scan_errors(&mut probe, "read_dir"), 1.0);
        drop(scope);

        assert_eq!(tick(&mut tailer, false).await, vec!["old"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    // -- resume identity: the head fingerprint (docs/adr/tail-discovery-failure-and-resume-identity.md)

    /// A `Tailer` driven by hand under `Watcher::Poll`: `bind` loads the checkpoint and runs the
    /// first scan, and each step below is one thing the run loop does, so no test waits on a
    /// timer.
    struct Hand<F: DecoderFactory<LineDecoder>> {
        tailer: Tailer<LineDecoder, F>,
        watcher: crate::tail::watch::Watcher,
        fanout: Fanout,
        rx: mpsc::Receiver<Delivered>,
        shutdown: watch::Receiver<bool>,
        _shutdown_tx: watch::Sender<bool>,
        probe: TelemetryProbe,
        diag: Diagnostics,
    }

    impl<F: DecoderFactory<LineDecoder>> Hand<F> {
        async fn bind(patterns: Vec<PathPattern>, factory: F, config: TailConfig) -> Self {
            let probe = TelemetryProbe::new();
            let diag = Diagnostics::new("test");
            let mut tailer = Tailer::new(patterns, factory, config)
                .with_diagnostics(diag.clone())
                .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
            tailer.bind().await.expect("bind should succeed");
            let watcher = tailer.watcher.take().expect("bind() leaves a watcher behind");
            let (fanout, rx) = fanout_channel(64);
            let (shutdown_tx, shutdown) = watch::channel(false);
            Self { tailer, watcher, fanout, rx, shutdown, _shutdown_tx: shutdown_tx, probe, diag }
        }

        async fn scan(&mut self) {
            self.tailer.scan(false, &mut self.watcher).await;
        }

        /// Reads every tracked file to EOF, reaps what's due, flushes, and returns the lines.
        async fn pump(&mut self) -> Vec<String> {
            let end = self
                .tailer
                .drain(&self.fanout, &self.shutdown, &mut self.watcher, no_timer_due())
                .await;
            assert_eq!(end, DrainEnd::Idle);
            self.tailer.flush_all(&self.fanout, FlushReason::Interval).await;
            let mut lines = Vec::new();
            while let Ok(delivered) = self.rx.try_recv() {
                lines.extend(messages(&unwrap_batch(delivered).events));
            }
            lines
        }

        /// An interval checkpoint tick, forced so the test doesn't depend on what dirtied it.
        async fn checkpoint(&mut self) {
            self.tailer.flush_all(&self.fanout, FlushReason::Interval).await;
            self.tailer.write_checkpoint(true).await;
        }

        /// What `run_until_shutdown` does after its loop.
        async fn shutdown(mut self) -> Vec<String> {
            self.tailer.close_all_for_shutdown(&self.fanout).await;
            self.tailer.flush_all(&self.fanout, FlushReason::Shutdown).await;
            self.tailer.write_checkpoint(true).await;
            let mut lines = Vec::new();
            while let Ok(delivered) = self.rx.try_recv() {
                lines.extend(messages(&unwrap_batch(delivered).events));
            }
            lines
        }

        fn rejected(&mut self) -> f64 {
            self.probe.sum("logit.input.files.resume_rejected", &[])
        }

        fn head_of(&self, path: &Path) -> Vec<u8> {
            let id = FileId::from_metadata(&std::fs::metadata(path).unwrap());
            self.tailer.files.get(&id).expect("the file is tracked").head.clone()
        }
    }

    fn checkpointed(dir: &Path, read_from: ReadFrom) -> TailConfig {
        let mut config = fast_config(read_from);
        config.checkpoint_path = Some(dir.join("checkpoint.json"));
        config
    }

    /// A format 2 checkpoint naming `file`'s live `(dev, ino)` at `offset` with `head`.
    fn write_checkpoint_for(dir: &Path, file: &Path, offset: u64, head: Head) {
        use std::os::unix::fs::MetadataExt;
        let meta = std::fs::metadata(file).unwrap();
        let text = format!(
            r#"{{"version":2,"files":[{{"dev":{},"ino":{},"path":"{}","offset":{offset},"head_len":{},"head_hash":{}}}]}}"#,
            meta.dev(),
            meta.ino(),
            file.display(),
            head.len,
            head.hash,
        );
        std::fs::write(dir.join("checkpoint.json"), text).unwrap();
    }

    /// The first entry of the checkpoint on disk, as `(offset, head_len, head_hash)`.
    fn checkpointed_entry(dir: &Path) -> (u64, u32, u64) {
        let text = std::fs::read_to_string(dir.join("checkpoint.json")).unwrap();
        let doc: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(doc["version"], 2, "{text}");
        let entry = &doc["files"][0];
        (
            entry["offset"].as_u64().unwrap(),
            entry["head_len"].as_u64().unwrap() as u32,
            entry["head_hash"].as_u64().unwrap(),
        )
    }

    /// Rewrites `path` in place, as `copytruncate` and a refilling writer leave it: same inode.
    fn copytruncate_and_refill(path: &Path, content: &[u8]) {
        let before = FileId::from_metadata(&std::fs::metadata(path).unwrap());
        std::fs::write(path, content).unwrap(); // `O_TRUNC` on the existing inode
        let after = FileId::from_metadata(&std::fs::metadata(path).unwrap());
        assert_eq!(before, after, "an in-place rewrite keeps the inode");
    }

    /// The state a recycled inode leaves: a checkpoint entry for the live `(dev, ino)` whose
    /// head doesn't match what the file now holds. Tmpfs doesn't recycle an inode on demand.
    #[tokio::test]
    async fn a_checkpoint_entry_whose_head_no_longer_matches_starts_at_zero_and_is_counted() {
        let dir = scratch_dir("resume-head-mismatch");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        write_checkpoint_for(&dir, &path, 4, Head::of(b"old\n"));

        // `read_from: end` loses to the entry, and the rejected entry to `0`.
        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            checkpointed(&dir, ReadFrom::End),
        )
        .await;
        assert_eq!(hand.pump().await, vec!["one", "two"]);
        assert_eq!(hand.rejected(), 1.0);
        assert_eq!(hand.diag.occurrences("resume_rejected"), 1);
        assert_eq!(hand.head_of(&path), b"one\ntwo\n", "the head is recaptured from 0");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_checkpoint_offset_past_the_file_length_is_a_rejected_resume() {
        let dir = scratch_dir("resume-past-end");
        let path = dir.join("app.log");
        std::fs::write(&path, b"short\n").unwrap();
        // The head matches, so only the offset can reject it.
        write_checkpoint_for(&dir, &path, 999_999, Head::of(b"short\n"));

        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            checkpointed(&dir, ReadFrom::End),
        )
        .await;
        assert_eq!(hand.pump().await, vec!["short"]);
        assert_eq!(hand.rejected(), 1.0);
        assert_eq!(hand.diag.occurrences("resume_rejected"), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Under `HEAD_BYTES`, the head is the whole of what was read, so one changed byte anywhere
    /// before the offset rejects the resume.
    #[tokio::test]
    async fn a_file_shorter_than_head_bytes_is_verified_in_full() {
        let dir = scratch_dir("resume-short-file");
        let path = dir.join("app.log");
        std::fs::write(&path, b"abc\n").unwrap();
        let config = checkpointed(&dir, ReadFrom::Beginning);

        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config.clone()).await;
        assert_eq!(hand.pump().await, vec!["abc"]);
        hand.shutdown().await;
        assert_eq!(checkpointed_entry(&dir), (4, 4, Head::of(b"abc\n").hash));

        // Unchanged: resumes at the end and reads nothing.
        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config.clone()).await;
        assert!(hand.pump().await.is_empty());
        assert_eq!(hand.rejected(), 0.0);
        hand.shutdown().await;

        // The last byte before the offset differs: replayed.
        copytruncate_and_refill(&path, b"abd\n");
        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config).await;
        assert_eq!(hand.pump().await, vec!["abd"]);
        assert_eq!(hand.rejected(), 1.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_file_under_head_bytes_that_grew_after_the_checkpoint_still_resumes() {
        let dir = scratch_dir("resume-short-file-grew");
        let path = dir.join("app.log");
        std::fs::write(&path, b"line one\n").unwrap();
        let config = checkpointed(&dir, ReadFrom::Beginning);

        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config.clone()).await;
        assert_eq!(hand.pump().await, vec!["line one"]);
        hand.shutdown().await;
        assert_eq!(checkpointed_entry(&dir), (9, 9, Head::of(b"line one\n").hash));

        append(&path, b"line two\n");
        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config).await;
        assert_eq!(hand.pump().await, vec!["line two"]);
        assert_eq!(hand.rejected(), 0.0);
        assert_eq!(hand.head_of(&path), b"line one\nline two\n");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The head grows with each read until it reaches `HEAD_BYTES`, then stays fixed, so a
    /// checkpoint written on either side of that point resumes.
    #[tokio::test]
    async fn an_appended_file_keeps_its_fingerprint_across_checkpoints() {
        let dir = scratch_dir("resume-head-grows");
        let path = dir.join("app.log");
        let line = |i: usize| format!("line-{i:04}\n"); // 10 bytes
        let first: String = (0..20).map(line).collect(); // 200 bytes
        let second: String = (20..40).map(line).collect(); // to 400
        std::fs::write(&path, &first).unwrap();
        let config = checkpointed(&dir, ReadFrom::Beginning);

        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config.clone()).await;
        assert_eq!(hand.pump().await.len(), 20);
        hand.checkpoint().await;
        assert_eq!(checkpointed_entry(&dir), (200, 200, Head::of(first.as_bytes()).hash));

        append(&path, second.as_bytes());
        assert_eq!(hand.pump().await.len(), 20);
        hand.checkpoint().await;
        let whole = std::fs::read(&path).unwrap();
        assert_eq!(checkpointed_entry(&dir), (400, 256, Head::of(&whole[..HEAD_BYTES]).hash));
        assert_eq!(hand.head_of(&path), &whole[..HEAD_BYTES]);
        hand.shutdown().await;

        append(&path, b"after\n");
        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config).await;
        assert_eq!(hand.pump().await, vec!["after"]);
        assert_eq!(hand.rejected(), 0.0);
        assert_eq!(hand.head_of(&path), &whole[..HEAD_BYTES]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Verifying the head reads it, which moves the cursor; a rejection must still start at 0.
    #[tokio::test]
    async fn a_resume_rejection_seeks_back_to_zero_after_reading_the_head() {
        let dir = scratch_dir("resume-reject-seek");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\nthree\n").unwrap();
        // A 4-byte head read leaves the cursor at "two".
        write_checkpoint_for(&dir, &path, 4, Head::of(b"ONE\n"));

        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            checkpointed(&dir, ReadFrom::Beginning),
        )
        .await;
        assert_eq!(hand.pump().await, vec!["one", "two", "three"]);
        assert_eq!(hand.rejected(), 1.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The size check can't see a truncation the writer refilled past the offset while `logit`
    /// was stopped. The head can.
    #[tokio::test]
    async fn a_copytruncate_refilled_past_the_offset_before_a_restart_replays_instead_of_skipping()
    {
        let dir = scratch_dir("resume-copytruncate");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let config = checkpointed(&dir, ReadFrom::Beginning);

        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config.clone()).await;
        assert_eq!(hand.pump().await, vec!["one"]);
        hand.shutdown().await;

        copytruncate_and_refill(&path, b"ONE\nTWO\n");
        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config).await;
        assert_eq!(hand.pump().await, vec!["ONE", "TWO"], "\"ONE\" must not be skipped");
        assert_eq!(hand.rejected(), 1.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_truncation_resets_the_captured_head() {
        let dir = scratch_dir("truncation-resets-head");
        let path = dir.join("app.log");
        std::fs::write(&path, b"abcdef\n").unwrap();
        let config = checkpointed(&dir, ReadFrom::Beginning);

        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config.clone()).await;
        assert_eq!(hand.pump().await, vec!["abcdef"]);
        assert_eq!(hand.head_of(&path), b"abcdef\n");

        copytruncate_and_refill(&path, b"x\n"); // 2 < 7: a truncation the scan sees
        hand.scan().await;
        assert!(hand.head_of(&path).is_empty(), "the old generation's head is gone");
        assert_eq!(hand.pump().await, vec!["x"]);
        assert_eq!(hand.head_of(&path), b"x\n");
        hand.shutdown().await;
        assert_eq!(checkpointed_entry(&dir), (2, 2, Head::of(b"x\n").hash));

        append(&path, b"y\n");
        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config).await;
        assert_eq!(hand.pump().await, vec!["y"]);
        assert_eq!(hand.rejected(), 0.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A file opened at its end never passes its first bytes through `read_one`, so `open_tracked`
    /// reads them. Without that, the checkpoint's head would be empty and cover nothing.
    #[tokio::test]
    async fn read_from_end_captures_the_head_at_open() {
        let dir = scratch_dir("read-from-end-head");
        let path = dir.join("app.log");
        std::fs::write(&path, b"old\n").unwrap();

        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            checkpointed(&dir, ReadFrom::End),
        )
        .await;
        assert_eq!(hand.head_of(&path), b"old\n");
        append(&path, b"new\n");
        assert_eq!(hand.pump().await, vec!["new"]);
        hand.shutdown().await;
        assert_eq!(checkpointed_entry(&dir), (8, 8, Head::of(b"old\nnew\n").hash));

        append(&path, b"newer\n");
        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            checkpointed(&dir, ReadFrom::Beginning),
        )
        .await;
        assert_eq!(hand.pump().await, vec!["newer"]);
        assert_eq!(hand.rejected(), 0.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_deselected_retention_whose_head_changed_is_rejected_on_reselect() {
        let dir = scratch_dir("deselect-head-changed");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let deselected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory = SelectiveFactory { deselected: deselected.clone() };

        let mut hand =
            Hand::bind(vec![PathPattern::new(&path)], factory, fast_config(ReadFrom::Beginning))
                .await;
        assert_eq!(hand.pump().await, vec!["one"]);

        deselected.store(true, std::sync::atomic::Ordering::SeqCst);
        hand.scan().await;
        assert!(hand.pump().await.is_empty());
        assert_eq!(hand.tailer.files.len(), 0, "reaped");
        let id = FileId::from_metadata(&std::fs::metadata(&path).unwrap());
        assert_eq!(hand.tailer.resume[&id].source, Source::Deselected);

        // Rewritten past the retained offset while not tailed.
        copytruncate_and_refill(&path, b"ONE\nTWO\n");
        deselected.store(false, std::sync::atomic::Ordering::SeqCst);
        hand.scan().await;
        assert_eq!(hand.pump().await, vec!["ONE", "TWO"]);
        assert_eq!(hand.rejected(), 1.0);
        assert!(hand.tailer.resume.is_empty(), "spent once tracked");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A format 1 checkpoint has no head, so it's unusable: every file replays once.
    #[tokio::test]
    async fn a_v1_checkpoint_is_unusable_and_replays() {
        use std::os::unix::fs::MetadataExt;

        let dir = scratch_dir("resume-v1");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        std::fs::write(
            dir.join("checkpoint.json"),
            format!(
                r#"{{"version":1,"files":[{{"dev":{},"ino":{},"path":"{}","offset":4}}]}}"#,
                meta.dev(),
                meta.ino(),
                path.display(),
            ),
        )
        .unwrap();

        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            checkpointed(&dir, ReadFrom::End),
        )
        .await;
        assert_eq!(hand.pump().await, vec!["one", "two"]);
        assert_eq!(hand.probe.sum("logit.input.checkpoint.errors", &[("op", "load")]), 1.0);
        assert_eq!(hand.rejected(), 0.0, "an unusable checkpoint isn't a rejected resume");
        hand.shutdown().await;
        assert_eq!(checkpointed_entry(&dir), (8, 8, Head::of(b"one\ntwo\n").hash));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Rotated between `scan`'s `stat` and `open_tracked`'s open: the descriptor names another
    /// inode, so nothing is tracked and the entry for the scanned one stays.
    #[tokio::test]
    async fn an_open_that_finds_a_different_inode_than_scanned_keeps_the_entry() {
        let dir = scratch_dir("open-finds-other-inode");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let scanned = FileId::from_metadata(&std::fs::metadata(&path).unwrap());
        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"other\n").unwrap();
        assert_ne!(FileId::from_metadata(&std::fs::metadata(&path).unwrap()), scanned);

        let mut tailer = Tailer::new(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        );
        let head = Head::of(b"one\n");
        let retained = Retained { path: path.clone(), offset: 4, head, source: Source::Checkpoint };
        tailer.resume.insert(scanned, retained.clone());
        let mut watcher = crate::tail::watch::Watcher::Poll;
        tailer
            .open_tracked(path.clone(), scanned, StartOffset::Resume(4, head), &mut watcher)
            .await;

        assert!(tailer.files.is_empty(), "the other inode isn't tracked under the scanned id");
        assert!(tailer.by_path.is_empty());
        assert_eq!(tailer.resume.get(&scanned), Some(&retained));
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A decoder that fails to open leaves the entry, and the next scan resumes from it.
    #[tokio::test]
    async fn a_resume_entry_survives_a_failed_decoder_open() {
        struct FailsOnce(bool);
        impl DecoderFactory<LineDecoder> for FailsOnce {
            fn accept(&mut self, _path: &Path) -> bool {
                true
            }
            fn open(&mut self, path: &Path) -> anyhow::Result<LineDecoder> {
                if std::mem::replace(&mut self.0, false) {
                    anyhow::bail!("injected");
                }
                Ok(LineDecoder::new(path, Arc::new(Resource::default())))
            }
        }

        let dir = scratch_dir("resume-decoder-open-fails");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        write_checkpoint_for(&dir, &path, 4, Head::of(b"one\n"));

        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            FailsOnce(true),
            checkpointed(&dir, ReadFrom::Beginning),
        )
        .await;
        assert!(hand.tailer.files.is_empty());
        assert_eq!(hand.diag.occurrences("open_error"), 1);
        hand.scan().await;
        assert_eq!(hand.pump().await, vec!["two"], "resumed at the entry, not replayed");
        assert!(hand.tailer.resume.is_empty());
        assert_eq!(hand.rejected(), 0.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    // -- retained entries are evicted per entry, and only when a listing says so

    /// A de-selected file removed while de-selected can never be re-selected, so its retention
    /// goes at the next scan that lists its directory.
    #[tokio::test]
    async fn a_deselected_retention_is_dropped_once_its_path_is_gone() {
        let dir = scratch_dir("deselect-retention-gone");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let deselected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory = SelectiveFactory { deselected: deselected.clone() };
        let mut hand = Hand::bind(
            vec![PathPattern::new(dir.join("*.log"))],
            factory,
            fast_config(ReadFrom::Beginning),
        )
        .await;
        assert_eq!(hand.pump().await, vec!["one"]);

        deselected.store(true, std::sync::atomic::Ordering::SeqCst);
        hand.scan().await;
        assert!(hand.pump().await.is_empty());
        hand.scan().await;
        assert_eq!(hand.tailer.resume.len(), 1, "kept while its path is still there");

        std::fs::remove_file(&path).unwrap();
        hand.scan().await;
        assert!(hand.tailer.resume.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_deselected_retention_is_dropped_when_its_path_now_names_another_inode() {
        let dir = scratch_dir("deselect-retention-other-inode");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let deselected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory = SelectiveFactory { deselected: deselected.clone() };
        let mut hand =
            Hand::bind(vec![PathPattern::new(&path)], factory, fast_config(ReadFrom::Beginning))
                .await;
        assert_eq!(hand.pump().await, vec!["one"]);

        deselected.store(true, std::sync::atomic::Ordering::SeqCst);
        hand.scan().await;
        assert!(hand.pump().await.is_empty());
        assert_eq!(hand.tailer.resume.len(), 1);

        // Renamed out of the pattern, so the old inode is alive and can't be reused.
        std::fs::rename(&path, dir.join("app.log.old")).unwrap();
        std::fs::write(&path, b"fresh\n").unwrap();
        hand.scan().await;
        assert!(hand.tailer.resume.is_empty());

        deselected.store(false, std::sync::atomic::Ordering::SeqCst);
        hand.scan().await;
        assert_eq!(hand.pump().await, vec!["fresh"]);
        assert_eq!(hand.rejected(), 0.0, "the new inode had no entry to reject");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An entry whose file is gone, or whose path no pattern names any more, is dropped by the
    /// first scan and leaves the next checkpoint.
    #[tokio::test]
    async fn unconsumed_checkpoint_entries_are_dropped_after_a_clean_scan() {
        use std::os::unix::fs::MetadataExt;

        let dir = scratch_dir("resume-prune-clean");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        let entry = |ino: u64, name: &str, offset: u64, head: Head| {
            format!(
                r#"{{"dev":{},"ino":{ino},"path":"{}","offset":{offset},"head_len":{},"head_hash":{}}}"#,
                meta.dev(),
                dir.join(name).display(),
                head.len,
                head.hash
            )
        };
        let files = [
            entry(meta.ino(), "app.log", 4, Head::of(b"one\n")),
            entry(meta.ino() + 1_000_000, "gone.log", 7, Head::of(b"x")),
            entry(meta.ino() + 1_000_001, "elsewhere.txt", 9, Head::of(b"y")),
        ];
        std::fs::write(
            dir.join("checkpoint.json"),
            format!(r#"{{"version":2,"files":[{}]}}"#, files.join(",")),
        )
        .unwrap();

        let mut hand = Hand::bind(
            vec![PathPattern::new(dir.join("*.log"))],
            LineFactory,
            checkpointed(&dir, ReadFrom::Beginning),
        )
        .await;
        assert!(hand.tailer.resume.is_empty(), "one spent, two pruned");
        assert!(hand.pump().await.is_empty(), "app.log resumed at its end");
        hand.shutdown().await;
        let text = std::fs::read_to_string(dir.join("checkpoint.json")).unwrap();
        assert!(text.contains("app.log"), "{text}");
        assert!(!text.contains("gone.log") && !text.contains("elsewhere.txt"), "{text}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_checkpoint_entry_is_kept_while_a_listing_still_fails() {
        use crate::tail::pattern::READ_DIR;
        use logit_pipeline::fault::{self, errno};

        let dir = scratch_dir("resume-kept-read-dir");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        write_checkpoint_for(&dir, &path, 4, Head::of(b"one\n"));
        let config = checkpointed(&dir, ReadFrom::Beginning);

        let scope = fault::scope(&dir);
        scope.fail(READ_DIR, errno::EACCES);
        let mut hand =
            Hand::bind(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config).await;
        hand.scan().await;
        assert!(hand.tailer.files.is_empty());
        assert_eq!(hand.tailer.resume.len(), 1, "a failed listing can't say the file is gone");
        drop(scope);

        hand.scan().await;
        assert_eq!(hand.pump().await, vec!["two"]);
        assert!(hand.tailer.resume.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_checkpoint_entry_is_kept_while_a_stat_is_unknown() {
        use crate::tail::pattern::STAT;
        use logit_pipeline::fault::{self, errno};

        let dir = scratch_dir("resume-kept-stat");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        write_checkpoint_for(&dir, &path, 4, Head::of(b"one\n"));
        let config = checkpointed(&dir, ReadFrom::Beginning);

        let scope = fault::scope(&dir);
        scope.fail(STAT, errno::EIO);
        let mut hand = Hand::bind(vec![PathPattern::new(&path)], LineFactory, config).await;
        hand.scan().await;
        assert!(hand.tailer.files.is_empty());
        assert_eq!(hand.tailer.resume.len(), 1, "an unknown stat can't say the file is gone");
        drop(scope);

        hand.scan().await;
        assert_eq!(hand.pump().await, vec!["two"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A checkpoint written while an entry is unspent keeps it. Otherwise a restart would find
    /// the file with no entry and, under `read_from: end`, skip what it gained.
    #[tokio::test]
    async fn an_unconsumed_checkpoint_entry_survives_a_checkpoint_write_while_its_listing_fails() {
        use crate::tail::pattern::READ_DIR;
        use logit_pipeline::fault::{self, errno};

        let dir = scratch_dir("resume-persisted-while-failing");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        write_checkpoint_for(&dir, &path, 4, Head::of(b"one\n"));
        let config = checkpointed(&dir, ReadFrom::End);
        let patterns = || vec![PathPattern::new(dir.join("*.log"))];

        let scope = fault::scope(&dir);
        scope.fail(READ_DIR, errno::EACCES);
        let mut hand = Hand::bind(patterns(), LineFactory, config.clone()).await;
        hand.checkpoint().await;
        assert!(hand.shutdown().await.is_empty());
        drop(scope);
        assert_eq!(checkpointed_entry(&dir), (4, 4, Head::of(b"one\n").hash));

        append(&path, b"three\n");
        let mut hand = Hand::bind(patterns(), LineFactory, config).await;
        assert_eq!(hand.pump().await, vec!["two", "three"]);
        assert_eq!(hand.rejected(), 0.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// An open that fails after the scan found the file leaves its entry, so the next scan
    /// resumes instead of replaying.
    #[tokio::test]
    async fn a_resume_entry_is_spent_only_when_the_file_is_tracked() {
        use crate::tail::pattern::OPEN;
        use logit_pipeline::fault::{self, errno};

        let dir = scratch_dir("resume-spent-when-tracked");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        write_checkpoint_for(&dir, &path, 4, Head::of(b"one\n"));

        let scope = fault::scope(&dir);
        scope.fail_nth(OPEN, 1, errno::EACCES);
        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            checkpointed(&dir, ReadFrom::Beginning),
        )
        .await;
        assert!(hand.tailer.files.is_empty());
        assert_eq!(hand.diag.occurrences("open_error"), 1);
        assert_eq!(hand.tailer.resume.len(), 1);

        hand.scan().await;
        drop(scope);
        assert_eq!(hand.pump().await, vec!["two"], "resumed at the entry, not replayed");
        assert!(hand.tailer.resume.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A head read that fails for a reason other than a short file says nothing about the file's
    /// identity: the entry is kept and the next scan resumes, rather than a counted rejection.
    #[tokio::test]
    async fn a_transient_head_read_error_on_resume_keeps_the_entry() {
        use crate::tail::pattern::HEAD_READ;
        use logit_pipeline::fault::{self, errno};

        let dir = scratch_dir("resume-head-read-fails");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        write_checkpoint_for(&dir, &path, 4, Head::of(b"one\n"));

        let scope = fault::scope(&dir);
        scope.fail_nth(HEAD_READ, 1, errno::EIO);
        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            checkpointed(&dir, ReadFrom::Beginning),
        )
        .await;
        assert!(hand.tailer.files.is_empty());
        assert_eq!(hand.diag.occurrences("open_error"), 1);
        assert_eq!(hand.rejected(), 0.0);
        assert_eq!(hand.diag.occurrences("resume_rejected"), 0);
        assert_eq!(hand.tailer.resume.len(), 1, "the entry survives");

        hand.scan().await;
        drop(scope);
        assert_eq!(hand.pump().await, vec!["two"], "resumed at the entry, not replayed");
        assert_eq!(hand.rejected(), 0.0);
        assert!(hand.tailer.resume.is_empty());
        std::fs::remove_dir_all(&dir).ok();
    }

    // -- a draining file's reap waits one poll interval (decision 6 of
    // docs/adr/tail-discovery-failure-and-resume-identity.md)

    /// One `drain` by hand, with no flush, returning what it emitted: a reap's
    /// `FlushReason::Closed` batch, or a batch that reached a bound.
    async fn drain_by_hand<F: DecoderFactory<LineDecoder>>(
        tailer: &mut Tailer<LineDecoder, F>,
    ) -> Vec<String> {
        let (fanout, mut rx) = fanout_channel(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut watcher = crate::tail::watch::Watcher::Poll;
        let _ = tailer.drain(&fanout, &shutdown_rx, &mut watcher, no_timer_due()).await;
        let mut out = Vec::new();
        while let Ok(delivered) = rx.try_recv() {
            out.extend(messages(&unwrap_batch(delivered).events));
        }
        out
    }

    fn closed_flushes(probe: &mut TelemetryProbe) -> f64 {
        probe.sum("logit.component.receive.flushed", &[("reason", "closed")])
    }

    /// logrotate's `create` mode renames the file and HUPs the writer, which appends to the
    /// renamed inode until it reopens. Under an exact pattern that inode is reachable only through
    /// the tracked handle, so its first EOF can't be final.
    #[tokio::test(start_paused = true)]
    async fn a_rotated_file_the_writer_still_appends_to_is_drained_before_it_is_reaped() {
        let dir = scratch_dir("reap-grace-writer-reopen");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let mut writer = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);
        let old = FileId::from_metadata(&std::fs::metadata(&path).unwrap());

        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"").unwrap();
        assert!(tick(&mut tailer, false).await.is_empty());
        assert_eq!(probe.sum("logit.input.files.rotated", &[]), 1.0);
        assert_eq!(
            tailer.files.get(&old).map(|f| f.state),
            Some(FileState::Draining),
            "at EOF, but not reaped by the pass after the scan that saw the rotation"
        );

        writer.write_all(b"late-1\nlate-2\n").unwrap();
        assert!(drain_by_hand(&mut tailer).await.is_empty(), "read, still in the accumulator");
        assert!(tailer.files.contains_key(&old));
        assert_eq!(probe.sum("logit.input.lines", &[]), 3.0);

        tokio::time::advance(tailer.config.poll_interval).await;
        tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
        assert_eq!(drain_by_hand(&mut tailer).await, vec!["late-1", "late-2"]);
        assert!(
            !tailer.files.contains_key(&old),
            "reaped: draining for a poll interval, rescanned since, and at EOF"
        );
        assert_eq!(closed_flushes(&mut probe), 1.0);
        assert_eq!(tailer.tracked_len(), 1, "the new app.log stays tracked");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `read_dir` lists `app.log.1`, logrotate renames it to `app.log.2`, and the `stat` gets
    /// `ENOENT`: the path is absent and its inode starts draining. The next scan finds that inode
    /// under its new name and rebinds it, even a full poll interval later: no reap happens until a
    /// scan after the retiring one has completed, and that scan is the one that rebinds it.
    #[tokio::test(start_paused = true)]
    async fn a_path_whose_stat_raced_a_rename_is_rebound_on_the_next_scan_not_replayed() {
        let dir = scratch_dir("reap-grace-stat-race");
        let rotated = dir.join("app.log.1");
        std::fs::write(&rotated, b"old-1\nold-2\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(
            vec![PathPattern::new(dir.join("app.log*"))],
            ReadFrom::Beginning,
            &probe,
        );
        assert_eq!(tick(&mut tailer, true).await, vec!["old-1", "old-2"]);
        let id = FileId::from_metadata(&std::fs::metadata(&rotated).unwrap());

        let scope = fault::scope(&dir);
        scope.fail_nth(STAT, 1, errno::ENOENT);
        assert!(tick(&mut tailer, false).await.is_empty());
        assert_eq!(tailer.files.get(&id).map(|f| f.state), Some(FileState::Draining));
        drop(scope);

        std::fs::rename(&rotated, dir.join("app.log.2")).unwrap();
        tokio::time::advance(tailer.config.poll_interval).await;
        assert!(tick(&mut tailer, false).await.is_empty(), "no replay from 0");
        let tracked = tailer.files.get(&id).expect("rebound, not reaped");
        assert_eq!(tracked.state, FileState::Active);
        assert_eq!(tracked.draining_since, None);
        assert_eq!(tracked.path, dir.join("app.log.2"));
        assert_eq!(tracked.offset, 12, "the offset is kept");
        assert_eq!(tailer.by_path.get(&dir.join("app.log.2")), Some(&id));
        assert!(diagnosed(&mut probe, "renamed"));
        assert_eq!(closed_flushes(&mut probe), 0.0);

        append(&dir.join("app.log.2"), b"new\n");
        assert_eq!(tick(&mut tailer, false).await, vec!["new"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test(start_paused = true)]
    async fn a_draining_file_is_reaped_once_it_has_been_draining_for_a_poll_interval_and_is_at_eof()
    {
        let dir = scratch_dir("reap-grace-boundary");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);
        let grace = tailer.config.poll_interval;

        std::fs::remove_file(&path).unwrap();
        tick(&mut tailer, false).await;
        assert_eq!(tailer.tracked_len(), 1, "draining since this scan");
        tokio::time::advance(grace - Duration::from_millis(1)).await;
        tick(&mut tailer, false).await;
        assert_eq!(tailer.tracked_len(), 1, "a millisecond short of the grace");
        tokio::time::advance(Duration::from_millis(1)).await;
        tick(&mut tailer, false).await;
        assert_eq!(tailer.tracked_len(), 0, "reaped at the grace");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A de-selected container is still being written, so it has no final EOF to wait for.
    #[tokio::test(start_paused = true)]
    async fn a_deselected_file_is_still_reaped_at_its_first_eof() {
        let dir = scratch_dir("reap-deselected-at-once");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let deselected = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory = SelectiveFactory { deselected: deselected.clone() };
        let mut hand =
            Hand::bind(vec![PathPattern::new(&path)], factory, fast_config(ReadFrom::Beginning))
                .await;
        assert_eq!(hand.pump().await, vec!["one"]);

        deselected.store(true, std::sync::atomic::Ordering::SeqCst);
        hand.scan().await;
        let start = tokio::time::Instant::now();
        assert!(hand.pump().await.is_empty());
        assert!(hand.tailer.files.is_empty(), "reaped by the drain after the scan");
        assert_eq!(tokio::time::Instant::now(), start, "with no time passing");
        std::fs::remove_dir_all(&dir).ok();
    }

    // -- accounting

    /// Runs `count_rotations` and then `reconcile_discovered` over `order`, as `scan` does over
    /// `discovered` in whatever order its `HashMap` yields. Returns `files.rotated`.
    async fn rotated_in_order(
        tailer: &mut Tailer<LineDecoder, LineFactory>,
        probe: &mut TelemetryProbe,
        order: &[PathBuf],
    ) -> f64 {
        let discovered: HashMap<PathBuf, std::fs::Metadata> =
            order.iter().map(|p| (p.clone(), std::fs::metadata(p).unwrap())).collect();
        let mut watcher = crate::tail::watch::Watcher::Poll;
        tailer.count_rotations(&discovered);
        for path in order {
            tailer.reconcile_discovered(path.clone(), &discovered[path], false, &mut watcher).await;
        }
        for path in order {
            let id = FileId::from_metadata(&discovered[path]);
            assert_eq!(tailer.by_path.get(path), Some(&id), "{path:?} is bound to its inode");
            assert_eq!(tailer.files[&id].state, FileState::Active);
        }
        probe.sum("logit.input.files.rotated", &[])
    }

    /// `files.rotated` counts each discovered path whose inode changed, before any arm runs, so
    /// the total doesn't depend on which of a rebind and a replacement `scan` reaches first.
    #[tokio::test]
    async fn files_rotated_counts_every_path_whose_inode_changed_in_any_discovery_order() {
        // A two-inode swap: each name now names the other's inode.
        for reversed in [false, true] {
            let dir = scratch_dir("rotated-swap");
            let (a, b) = (dir.join("app.log"), dir.join("app.log.1"));
            std::fs::write(&a, b"a\n").unwrap();
            std::fs::write(&b, b"b\n").unwrap();
            let mut probe = TelemetryProbe::new();
            let mut tailer = probed_tailer(
                vec![PathPattern::new(dir.join("app.log*"))],
                ReadFrom::Beginning,
                &probe,
            );
            tick(&mut tailer, true).await;
            std::fs::rename(&a, dir.join("tmp")).unwrap();
            std::fs::rename(&b, &a).unwrap();
            std::fs::rename(dir.join("tmp"), &b).unwrap();
            let mut order = vec![a.clone(), b.clone()];
            if reversed {
                order.reverse();
            }
            assert_eq!(rotated_in_order(&mut tailer, &mut probe, &order).await, 2.0, "{order:?}");
            std::fs::remove_dir_all(&dir).ok();
        }

        // A rotation chain: `.1` -> `.2`, `app.log` -> `.1`, a new `app.log`. Two known paths now
        // name another inode; `app.log.2` was never tracked under that name.
        let names = ["app.log", "app.log.1", "app.log.2"];
        let orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
        for order in orders {
            let dir = scratch_dir("rotated-chain");
            std::fs::write(dir.join("app.log"), b"a\n").unwrap();
            std::fs::write(dir.join("app.log.1"), b"b\n").unwrap();
            let mut probe = TelemetryProbe::new();
            let mut tailer = probed_tailer(
                vec![PathPattern::new(dir.join("app.log*"))],
                ReadFrom::Beginning,
                &probe,
            );
            tick(&mut tailer, true).await;
            std::fs::rename(dir.join("app.log.1"), dir.join("app.log.2")).unwrap();
            std::fs::rename(dir.join("app.log"), dir.join("app.log.1")).unwrap();
            std::fs::write(dir.join("app.log"), b"c\n").unwrap();
            let order: Vec<PathBuf> = order.iter().map(|&i| dir.join(names[i])).collect();
            assert_eq!(rotated_in_order(&mut tailer, &mut probe, &order).await, 2.0, "{order:?}");
            assert_eq!(tailer.tracked_len(), 3);
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// A truncation moves the offset back to `0`, so the next interval write must persist that
    /// even if nothing is read in between.
    #[tokio::test]
    async fn a_truncation_dirties_the_checkpoint() {
        let dir = scratch_dir("truncation-dirties-checkpoint");
        let path = dir.join("app.log");
        std::fs::write(&path, b"abcdef\n").unwrap();
        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            checkpointed(&dir, ReadFrom::Beginning),
        )
        .await;
        assert_eq!(hand.pump().await, vec!["abcdef"]);
        hand.checkpoint().await;
        assert_eq!(checkpointed_entry(&dir), (7, 7, Head::of(b"abcdef\n").hash));

        copytruncate_and_refill(&path, b"");
        hand.scan().await;
        assert_eq!(hand.probe.sum("logit.input.files.truncated", &[]), 1.0);
        hand.tailer.write_checkpoint(false).await;
        assert_eq!(checkpointed_entry(&dir), (0, 0, Head::of(b"").hash));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn lines_counts_the_partial_line_emitted_at_close() {
        let dir = scratch_dir("lines-partial-at-close");
        let path = dir.join("app.log");
        std::fs::write(&path, b"a\nbcd").unwrap();
        let mut hand = Hand::bind(
            vec![PathPattern::new(&path)],
            LineFactory,
            fast_config(ReadFrom::Beginning),
        )
        .await;
        assert_eq!(hand.pump().await, vec!["a"]);
        assert_eq!(hand.probe.sum("logit.input.lines", &[]), 1.0);
        assert_eq!(hand.probe.sum("logit.input.line.bytes", &[]), 1.0);

        hand.tailer.close_all_for_shutdown(&hand.fanout).await;
        hand.tailer.flush_all(&hand.fanout, FlushReason::Shutdown).await;
        let mut closed = Vec::new();
        while let Ok(delivered) = hand.rx.try_recv() {
            closed.extend(messages(&unwrap_batch(delivered).events));
        }
        assert_eq!(closed, vec!["bcd"]);
        assert_eq!(hand.probe.sum("logit.input.lines", &[]), 2.0);
        assert_eq!(hand.probe.sum("logit.input.line.bytes", &[]), 4.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `logit.input.lines` counts what the decoder is offered: a rejected line is counted, a line
    /// the splitter dropped for length isn't, and the unterminated tail is counted at close.
    #[tokio::test]
    async fn lines_counts_lines_offered_to_the_decoder_not_events_emitted() {
        /// Rejects every line that starts with `bad`, and decodes the rest as `tail_in` does.
        struct RejectingDecoder(LineDecoder);
        impl TailDecoder for RejectingDecoder {
            fn decode_line(
                &mut self,
                line: Bytes,
                read_at: i64,
                out: &mut Vec<Event>,
            ) -> Result<Arc<Resource>, logit_proto::CodecError> {
                if line.starts_with(b"bad") {
                    return Err(logit_proto::CodecError::Malformed("rejected".to_string()));
                }
                self.0.decode_line(line, read_at, out)
            }
            fn resource(&self) -> Arc<Resource> {
                self.0.resource()
            }
        }
        struct RejectingFactory;
        impl DecoderFactory<RejectingDecoder> for RejectingFactory {
            fn accept(&mut self, _path: &Path) -> bool {
                true
            }
            fn open(&mut self, path: &Path) -> anyhow::Result<RejectingDecoder> {
                Ok(RejectingDecoder(LineDecoder::new(path, Arc::new(Resource::default()))))
            }
        }

        let dir = scratch_dir("lines-offered");
        let path = dir.join("app.log");
        let long = "x".repeat(40);
        std::fs::write(&path, format!("ok1\n{long}\nbad1\nbad2\nok2\ntail")).unwrap();
        let mut config = fast_config(ReadFrom::Beginning);
        config.max_line_bytes = 16;
        let mut probe = TelemetryProbe::new();
        let diag = Diagnostics::new("test");
        let mut tailer = Tailer::new(vec![PathPattern::new(&path)], RejectingFactory, config)
            .with_diagnostics(diag.clone())
            .with_telemetry(probe.telemetry("tail_in", "tail_in", "listener"));
        tailer.bind().await.unwrap();
        let mut watcher = tailer.watcher.take().unwrap();
        let (fanout, mut rx) = fanout_channel(64);
        let (_shutdown_tx, shutdown) = watch::channel(false);
        tailer.drain(&fanout, &shutdown, &mut watcher, no_timer_due()).await;
        tailer.close_all_for_shutdown(&fanout).await;
        tailer.flush_all(&fanout, FlushReason::Shutdown).await;
        let mut events = Vec::new();
        while let Ok(delivered) = rx.try_recv() {
            events.extend(unwrap_batch(delivered).events);
        }

        assert_eq!(messages(&events), vec!["ok1", "ok2", "tail"]);
        assert_eq!(probe.sum("logit.input.lines", &[]), 5.0, "ok1, bad1, bad2, ok2, tail");
        assert_eq!(probe.sum("logit.input.line.bytes", &[]), 18.0);
        assert_eq!(diag.occurrences("long_line"), 1);
        assert_eq!(diag.occurrences("bad_line"), 2);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A factory that records every path `scan` offers it (`accept`, `refresh`, `retain`), and
    /// de-selects one path on request.
    struct RecordingFactory {
        offered: Arc<std::sync::Mutex<Vec<PathBuf>>>,
        deselect: Arc<std::sync::Mutex<Option<PathBuf>>>,
    }

    impl DecoderFactory<LineDecoder> for RecordingFactory {
        fn accept(&mut self, path: &Path) -> bool {
            self.offered.lock().unwrap().push(path.to_path_buf());
            self.deselect.lock().unwrap().as_deref() != Some(path)
        }

        fn open(&mut self, path: &Path) -> anyhow::Result<LineDecoder> {
            Ok(LineDecoder::new(path, Arc::new(Resource::default())))
        }

        fn refresh(&mut self, path: &Path, _decoder: &mut LineDecoder) -> Refresh {
            self.offered.lock().unwrap().push(path.to_path_buf());
            if self.deselect.lock().unwrap().as_deref() == Some(path) {
                Refresh::Deselected
            } else {
                Refresh::Unchanged
            }
        }

        fn retain(&mut self, path: &Path) {
            self.offered.lock().unwrap().push(path.to_path_buf());
        }
    }

    /// `end_scan` evicts what no call touched, so a path missing from one scan's calls loses its
    /// cached state: a rebind and a de-selected file awaiting its reap included.
    #[tokio::test]
    async fn every_discovered_path_is_offered_to_the_factory_once_per_scan() {
        let dir = scratch_dir("factory-offers");
        let plain = dir.join("plain.log");
        let moved = dir.join("moved.log");
        let gone = dir.join("gone.log");
        for p in [&plain, &moved, &gone] {
            std::fs::write(p, b"x\n").unwrap();
        }
        let offered = Arc::new(std::sync::Mutex::new(Vec::new()));
        let deselect = Arc::new(std::sync::Mutex::new(None));
        let factory = RecordingFactory { offered: offered.clone(), deselect: deselect.clone() };
        let mut hand = Hand::bind(
            vec![PathPattern::new(dir.join("*"))],
            factory,
            fast_config(ReadFrom::Beginning),
        )
        .await;
        let take = || {
            let mut paths = std::mem::take(&mut *offered.lock().unwrap());
            paths.sort();
            paths
        };
        let sorted = |mut paths: Vec<PathBuf>| {
            paths.sort();
            paths
        };
        assert_eq!(take(), sorted(vec![plain.clone(), moved.clone(), gone.clone()]));

        // `moved.log` is rebound under `moved.log.1`, and `gone.log` is de-selected.
        let renamed = dir.join("moved.log.1");
        std::fs::rename(&moved, &renamed).unwrap();
        *deselect.lock().unwrap() = Some(gone.clone());
        hand.scan().await;
        assert_eq!(take(), sorted(vec![plain.clone(), renamed.clone(), gone.clone()]));

        // No `drain` has reaped `gone.log`, so it reaches `open_tracked`'s de-selected return.
        hand.scan().await;
        assert_eq!(hand.tailer.files.len(), 3);
        assert_eq!(take(), sorted(vec![plain.clone(), renamed.clone(), gone.clone()]));
        std::fs::remove_dir_all(&dir).ok();
    }

    // -- a read error (fault seam `tail.read`)

    #[tokio::test]
    async fn a_read_error_on_an_active_file_never_reaps_it() {
        let dir = scratch_dir("read-error-active");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);

        let scope = fault::scope(&dir);
        scope.fail_nth(READ, 1, errno::EIO);
        append(&path, b"two\n");
        assert!(tick(&mut tailer, false).await.is_empty());
        assert!(diagnosed(&mut probe, "read_error"));
        assert_eq!(state_of(&tailer, &path), Some(FileState::Active));
        assert_eq!(closed_flushes(&mut probe), 0.0);

        assert_eq!(tick(&mut tailer, false).await, vec!["two"], "the next pass reads on");
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The documented gap in `docs/known-gaps/tailing.md`: a read error counts as EOF, so a
    /// `Draining` file past its grace is reaped with its unread bytes.
    #[tokio::test(start_paused = true)]
    async fn a_read_error_on_a_draining_file_reaps_it_and_loses_its_unread_tail() {
        let dir = scratch_dir("read-error-draining");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one"]);

        append(&path, b"two\n");
        std::fs::remove_file(&path).unwrap();
        tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
        assert_eq!(tailer.files.values().next().map(|f| f.state), Some(FileState::Draining));
        tokio::time::advance(tailer.config.poll_interval).await;
        tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;

        let scope = fault::scope(&dir);
        scope.fail_nth(READ, 1, errno::EIO);
        assert!(drain_by_hand(&mut tailer).await.is_empty(), "\"two\" is lost");
        assert_eq!(tailer.tracked_len(), 0);
        assert!(diagnosed(&mut probe, "read_error"));
        assert_eq!(probe.sum("logit.input.lines", &[]), 1.0);
        drop(scope);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A `drain` can follow a data wake or a flush tick with no scan since the retiring one. Time
    /// alone must not reap then: the scan that would rebind a raced rename hasn't run.
    #[tokio::test(start_paused = true)]
    async fn a_drain_with_no_scan_since_a_file_started_draining_never_reaps_it() {
        let dir = scratch_dir("reap-needs-a-scan");
        let rotated = dir.join("app.log.1");
        std::fs::write(&rotated, b"old-1\nold-2\n").unwrap();
        let probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(
            vec![PathPattern::new(dir.join("app.log*"))],
            ReadFrom::Beginning,
            &probe,
        );
        assert_eq!(tick(&mut tailer, true).await, vec!["old-1", "old-2"]);
        let id = FileId::from_metadata(&std::fs::metadata(&rotated).unwrap());

        let scope = fault::scope(&dir);
        scope.fail_nth(STAT, 1, errno::ENOENT);
        tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
        drop(scope);
        assert_eq!(tailer.files.get(&id).map(|f| f.state), Some(FileState::Draining));

        std::fs::rename(&rotated, dir.join("app.log.2")).unwrap();
        tokio::time::advance(tailer.config.poll_interval).await;
        assert!(drain_by_hand(&mut tailer).await.is_empty());
        assert!(tailer.files.contains_key(&id), "past the grace, but no scan since draining");

        tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
        let tracked = tailer.files.get(&id).expect("rebound");
        assert_eq!(tracked.state, FileState::Active);
        assert_eq!(tracked.path, dir.join("app.log.2"));
        assert_eq!(tracked.offset, 12, "the offset is kept");
        assert!(tick(&mut tailer, false).await.is_empty(), "no replay from 0");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The grace is measured when a pass starts, before its reads. A `Draining` file read to EOF
    /// early in a pass whose later `emit` parks on the downstream past the grace isn't reaped by
    /// that pass, so lines the writer appends meanwhile are read by the next one.
    #[tokio::test(start_paused = true)]
    async fn a_pass_parked_on_the_downstream_past_the_grace_does_not_reap_on_its_earlier_eof() {
        // `drain` reads files in `files`' iteration order, which a fresh map's hasher picks, so
        // rebuild until the draining file comes first: only that order puts its EOF before the
        // parked `emit`.
        for attempt in 0..64 {
            let dir = scratch_dir("reap-grace-pass-start");
            let path = dir.join("app.log");
            let busy = dir.join("busy.log");
            std::fs::write(&path, b"one\n").unwrap();
            std::fs::write(&busy, b"").unwrap();
            let mut writer = std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            let probe = TelemetryProbe::new();
            let mut tailer = probed_tailer(
                vec![PathPattern::new(&path), PathPattern::new(&busy)],
                ReadFrom::Beginning,
                &probe,
            );
            tailer.config.batching.max_events = 1;
            assert_eq!(tick(&mut tailer, true).await, vec!["one"]);
            let old = FileId::from_metadata(&std::fs::metadata(&path).unwrap());

            std::fs::rename(&path, dir.join("app.log.1")).unwrap();
            tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
            tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
            assert_eq!(tailer.files.get(&old).map(|f| f.state), Some(FileState::Draining));
            if tailer.files.keys().next() != Some(&old) {
                std::fs::remove_dir_all(&dir).ok();
                continue;
            }

            let grace = tailer.config.poll_interval;
            tokio::time::advance(grace - Duration::from_millis(1)).await;
            append(&busy, b"b1\nb2\nb3\n");
            // Room for one batch: the second `emit` parks until the test receives.
            let (fanout, mut rx) = fanout_channel(1);
            let (_shutdown_tx, shutdown_rx) = watch::channel(false);
            let mut watcher = crate::tail::watch::Watcher::Poll;
            let mut received = Vec::new();
            let mut parked = false;
            {
                let drain = tailer.drain(&fanout, &shutdown_rx, &mut watcher, no_timer_due());
                tokio::pin!(drain);
                loop {
                    tokio::select! {
                        _ = &mut drain => break,
                        Some(delivered) = rx.recv() => {
                            received.extend(messages(&unwrap_batch(delivered).events));
                            if !parked {
                                parked = true;
                                // The pass is inside `busy.log`'s emits, after the draining file's
                                // EOF. The grace runs out, and the writer appends before reopening.
                                tokio::time::advance(Duration::from_millis(2)).await;
                                writer.write_all(b"late-1\nlate-2\n").unwrap();
                            }
                        }
                    }
                }
            }
            while let Ok(delivered) = rx.try_recv() {
                received.extend(messages(&unwrap_batch(delivered).events));
            }
            assert!(parked, "the pass parked on the downstream");
            received.sort();
            assert_eq!(
                received,
                vec!["b1", "b2", "b3", "late-1", "late-2"],
                "the late lines are read by the next pass, not lost to a reap on the earlier EOF \
                 (attempt {attempt})"
            );
            assert!(!tailer.files.contains_key(&old), "reaped once a pass starts past the grace");
            std::fs::remove_dir_all(&dir).ok();
            return;
        }
        panic!("the draining file never came first in 64 fresh maps");
    }

    // -- truncation of a file no path is bound to

    /// A `stat` that raced a rename retires the file, and `copytruncate` truncates it in place.
    /// It's still shorter than its offset at the scan that rebinds it, so the rebind resets the
    /// offset as the same-path arm does; the refill after that is read from `0`, not mid-line.
    #[tokio::test]
    async fn a_rebound_inode_truncated_while_draining_is_read_from_zero_not_from_its_stale_offset()
    {
        let dir = scratch_dir("rebind-after-truncation");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one", "two"]);

        let scope = fault::scope(&dir);
        scope.fail_nth(STAT, 1, errno::ENOENT);
        tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
        drop(scope);
        assert_eq!(state_of(&tailer, &path), Some(FileState::Draining));

        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(0).unwrap();
        append(&path, b"three\n");
        tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
        assert_eq!(state_of(&tailer, &path), Some(FileState::Active), "rebound");
        assert_eq!(probe.sum("logit.input.files.truncated", &[]), 1.0);
        append(&path, b"four-refilled-past-the-old-offset\n");
        assert_eq!(
            tick(&mut tailer, false).await,
            vec!["three", "four-refilled-past-the-old-offset"]
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A draining file truncated in place is checked by `drain` before each read, so a refill
    /// past the old offset after that check, but before any scan rebinds the file, is still read
    /// from `0`. Only a refill before the first check goes unseen (`docs/known-gaps/tailing.md`).
    #[tokio::test]
    async fn a_draining_inode_truncated_and_refilled_before_its_rebind_is_read_from_zero() {
        let dir = scratch_dir("draining-truncated-refilled");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        assert_eq!(tick(&mut tailer, true).await, vec!["one", "two"]);

        let scope = fault::scope(&dir);
        scope.fail_nth(STAT, 1, errno::ENOENT);
        tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
        drop(scope);
        assert_eq!(state_of(&tailer, &path), Some(FileState::Draining));

        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(0).unwrap();
        assert!(drain_by_hand(&mut tailer).await.is_empty());
        assert_eq!(probe.sum("logit.input.files.truncated", &[]), 1.0, "seen by the drain");
        append(&path, b"three-refilled-past-the-old-offset\n");
        assert_eq!(tick(&mut tailer, false).await, vec!["three-refilled-past-the-old-offset"]);
        assert_eq!(state_of(&tailer, &path), Some(FileState::Active), "rebound");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A checkpoint entry stays unspent while its path's `stat` fails. If a rotation then moves
    /// that inode onto a path still bound to another inode, the rotation arm opens it, and it must
    /// resume from the entry as the unbound arm would: which arm a path reaches depends on the
    /// order `scan` visits `discovered` in.
    #[tokio::test]
    async fn a_rotation_arm_resumes_a_new_inode_from_its_unspent_checkpoint_entry_in_either_scan_order(
    ) {
        for rebind_first in [false, true] {
            let dir = scratch_dir("rotation-arm-resume");
            let live = dir.join("app.log");
            let one = dir.join("app.log.1");
            let two = dir.join("app.log.2");
            std::fs::write(&one, b"x1\n").unwrap();
            std::fs::write(&live, b"z1\n").unwrap();
            let patterns = vec![PathPattern::new(dir.join("app.log*"))];
            let config = checkpointed(&dir, ReadFrom::Beginning);
            let mut hand = Hand::bind(patterns.clone(), LineFactory, config.clone()).await;
            assert_eq!(hand.pump().await.len(), 2);
            hand.checkpoint().await;
            drop(hand);

            // A restart whose `stat` of `app.log` fails: `app.log.1` resumes, `app.log`'s entry
            // stays unspent.
            let scope = fault::scope(&live);
            scope.fail(STAT, errno::EIO);
            let mut hand = Hand::bind(patterns, LineFactory, config).await;
            drop(scope);
            let z = FileId::from_metadata(&std::fs::metadata(&live).unwrap());
            assert!(hand.tailer.resume.contains_key(&z), "the entry is unspent");

            std::fs::rename(&one, &two).unwrap();
            std::fs::rename(&live, &one).unwrap();
            std::fs::write(&live, b"").unwrap();
            let order = if rebind_first {
                vec![two.clone(), one.clone(), live.clone()]
            } else {
                vec![one.clone(), two.clone(), live.clone()]
            };
            rotated_in_order(&mut hand.tailer, &mut hand.probe, &order).await;
            assert_eq!(hand.tailer.files[&z].offset, 3, "resumed, rebind_first: {rebind_first}");
            assert!(hand.tailer.resume.is_empty());
            assert_eq!(hand.rejected(), 0.0);
            assert!(hand.pump().await.is_empty(), "nothing replayed, rebind_first: {rebind_first}");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    // -- the refuter's cases for the state-machine proptest, pinned one at a time

    /// A rotation chain under `app.log*`: `.1` to `.2`, `app.log` to `.1`, a new `app.log`. Each
    /// old inode is rebound under its new name with its offset, in every order `scan` may visit
    /// the three paths, and only the new file is read.
    #[tokio::test]
    async fn a_rotation_chain_under_a_wildcard_rebinds_every_inode_in_any_discovery_order() {
        let names = ["app.log", "app.log.1", "app.log.2"];
        let orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]];
        for order in orders {
            let dir = scratch_dir("rotation-chain-rebind");
            std::fs::write(dir.join("app.log"), b"a1\n").unwrap();
            std::fs::write(dir.join("app.log.1"), b"b1\n").unwrap();
            let mut probe = TelemetryProbe::new();
            let mut tailer = probed_tailer(
                vec![PathPattern::new(dir.join("app.log*"))],
                ReadFrom::Beginning,
                &probe,
            );
            let mut first = tick(&mut tailer, true).await;
            first.sort();
            assert_eq!(first, vec!["a1", "b1"]);
            let a = FileId::from_metadata(&std::fs::metadata(dir.join("app.log")).unwrap());
            let b = FileId::from_metadata(&std::fs::metadata(dir.join("app.log.1")).unwrap());

            std::fs::rename(dir.join("app.log.1"), dir.join("app.log.2")).unwrap();
            std::fs::rename(dir.join("app.log"), dir.join("app.log.1")).unwrap();
            std::fs::write(dir.join("app.log"), b"c1\n").unwrap();
            let order: Vec<PathBuf> = order.iter().map(|&i| dir.join(names[i])).collect();
            rotated_in_order(&mut tailer, &mut probe, &order).await;
            assert_eq!(tailer.files[&a].path, dir.join("app.log.1"), "{order:?}");
            assert_eq!(tailer.files[&a].offset, 3, "{order:?}");
            assert_eq!(tailer.files[&b].path, dir.join("app.log.2"), "{order:?}");
            assert_eq!(tailer.files[&b].offset, 3, "{order:?}");
            assert_eq!(tick(&mut tailer, false).await, vec!["c1"], "{order:?}: no replay");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// Two inodes trade names. Each keeps its offset under its new name in either order.
    #[tokio::test]
    async fn two_inodes_swapping_names_keep_their_offsets_in_either_discovery_order() {
        for reversed in [false, true] {
            let dir = scratch_dir("swap-offsets");
            let (x, y) = (dir.join("app.log"), dir.join("app.log.1"));
            std::fs::write(&x, b"x1\n").unwrap();
            std::fs::write(&y, b"y-one\n").unwrap();
            let mut probe = TelemetryProbe::new();
            let mut tailer = probed_tailer(
                vec![PathPattern::new(dir.join("app.log*"))],
                ReadFrom::Beginning,
                &probe,
            );
            tick(&mut tailer, true).await;
            let x_id = FileId::from_metadata(&std::fs::metadata(&x).unwrap());
            let y_id = FileId::from_metadata(&std::fs::metadata(&y).unwrap());

            std::fs::rename(&x, dir.join("tmp")).unwrap();
            std::fs::rename(&y, &x).unwrap();
            std::fs::rename(dir.join("tmp"), &y).unwrap();
            let mut order = vec![x.clone(), y.clone()];
            if reversed {
                order.reverse();
            }
            rotated_in_order(&mut tailer, &mut probe, &order).await;
            assert_eq!((&tailer.files[&x_id].path, tailer.files[&x_id].offset), (&y, 3));
            assert_eq!((&tailer.files[&y_id].path, tailer.files[&y_id].offset), (&x, 6));

            append(&y, b"x2\n");
            assert_eq!(tick(&mut tailer, false).await, vec!["x2"], "{order:?}: no replay");
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// `copytruncate` under `app.log*`: the copy is a new inode the pattern matches, so it's read
    /// from its beginning, re-emitting what the truncated original already had
    /// (`docs/known-gaps/tailing.md`).
    #[tokio::test]
    async fn copytruncate_under_a_wildcard_replays_the_copy_from_its_beginning() {
        let dir = scratch_dir("copytruncate-wildcard");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\ntwo\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(
            vec![PathPattern::new(dir.join("app.log*"))],
            ReadFrom::Beginning,
            &probe,
        );
        assert_eq!(tick(&mut tailer, true).await, vec!["one", "two"]);

        std::fs::copy(&path, dir.join("app.log.1")).unwrap();
        std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(0).unwrap();
        assert_eq!(tick(&mut tailer, false).await, vec!["one", "two"], "the copy, from 0");
        assert_eq!(probe.sum("logit.input.files.truncated", &[]), 1.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `tail_in`'s clean stop emits an unterminated last line and checkpoints past it, so the
    /// rest of that line arrives after the restart as an event of its own
    /// (`docs/known-gaps/tailing.md`).
    #[tokio::test]
    async fn a_clean_restart_mid_line_emits_the_prefix_and_the_remainder_as_two_events() {
        let dir = scratch_dir("restart-mid-line");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\npar").unwrap();
        let patterns = vec![PathPattern::new(&path)];
        let config = checkpointed(&dir, ReadFrom::Beginning);
        let mut hand = Hand::bind(patterns.clone(), LineFactory, config.clone()).await;
        assert_eq!(hand.pump().await, vec!["one"]);
        assert_eq!(hand.shutdown().await, vec!["par"]);
        assert_eq!(checkpointed_entry(&dir).0, 7, "past the emitted prefix");

        append(&path, b"tial\n");
        let mut hand = Hand::bind(patterns, LineFactory, config).await;
        assert_eq!(hand.pump().await, vec!["tial"]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A removed file is read only by `drain`; a clean stop before one closes it with its unread
    /// bytes, and no pattern reaches it afterwards.
    #[tokio::test]
    async fn a_deleted_file_not_drained_before_a_restart_loses_its_unread_tail() {
        let dir = scratch_dir("deleted-before-restart");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let patterns = vec![PathPattern::new(&path)];
        let config = checkpointed(&dir, ReadFrom::Beginning);
        let mut hand = Hand::bind(patterns.clone(), LineFactory, config.clone()).await;
        assert_eq!(hand.pump().await, vec!["one"]);

        append(&path, b"two\n");
        std::fs::remove_file(&path).unwrap();
        hand.scan().await;
        assert!(hand.shutdown().await.is_empty(), "\"two\" was never read");
        let mut hand = Hand::bind(patterns, LineFactory, config).await;
        assert!(hand.pump().await.is_empty());
        assert_eq!(hand.tailer.tracked_len(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Under an exact pattern, `app.log.1` is reachable only through the handle the crash drops.
    #[tokio::test]
    async fn a_rotated_file_under_a_literal_pattern_not_drained_before_a_crash_is_orphaned() {
        let dir = scratch_dir("literal-rotated-crash");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let patterns = vec![PathPattern::new(&path)];
        let config = checkpointed(&dir, ReadFrom::Beginning);
        let mut hand = Hand::bind(patterns.clone(), LineFactory, config.clone()).await;
        assert_eq!(hand.pump().await, vec!["one"]);
        hand.checkpoint().await;

        append(&path, b"two\n");
        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"").unwrap();
        hand.scan().await;
        drop(hand);

        let mut hand = Hand::bind(patterns, LineFactory, config).await;
        assert!(hand.pump().await.is_empty(), "\"two\" is orphaned in app.log.1");
        assert_eq!(hand.tailer.tracked_len(), 1, "only the new app.log");
        assert!(hand.tailer.resume.is_empty(), "the rotated file's entry is pruned");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Truncation is `len < offset`: a file truncated before anything was read from it is at
    /// offset `0`, so nothing is detected, and under an exact pattern its old content survives only
    /// in the copy.
    #[tokio::test]
    async fn a_copytruncate_before_any_read_counts_no_truncation() {
        let dir = scratch_dir("copytruncate-unread");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        tailer.scan(true, &mut crate::tail::watch::Watcher::Poll).await;
        assert_eq!(tailer.files.values().next().map(|f| f.offset), Some(0));

        std::fs::copy(&path, dir.join("app.log.1")).unwrap();
        std::fs::OpenOptions::new().write(true).open(&path).unwrap().set_len(0).unwrap();
        append(&path, b"two\n");
        assert_eq!(tick(&mut tailer, false).await, vec!["two"]);
        assert_eq!(probe.sum("logit.input.files.truncated", &[]), 0.0);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// `logit.input.files.open` is set by `scan` only, so a file `drain` reaps still counts until
    /// the next scan.
    #[tokio::test(start_paused = true)]
    async fn files_open_is_sampled_at_scan_so_a_reap_shows_at_the_next_scan() {
        let dir = scratch_dir("files-open-sampled");
        let path = dir.join("app.log");
        std::fs::write(&path, b"one\n").unwrap();
        let mut probe = TelemetryProbe::new();
        let mut tailer = probed_tailer(vec![PathPattern::new(&path)], ReadFrom::Beginning, &probe);
        tick(&mut tailer, true).await;
        assert_eq!(probe.gauge("logit.input.files.open", &[]), Some(1.0));

        std::fs::remove_file(&path).unwrap();
        tick(&mut tailer, false).await;
        assert_eq!(tailer.tracked_len(), 1, "draining");
        after_grace(&mut tailer).await;
        assert_eq!(tailer.tracked_len(), 0, "reaped by the drain after the scan");
        assert_eq!(probe.gauge("logit.input.files.open", &[]), Some(1.0), "sampled before it");
        tailer.scan(false, &mut crate::tail::watch::Watcher::Poll).await;
        assert_eq!(probe.gauge("logit.input.files.open", &[]), Some(0.0));
        std::fs::remove_dir_all(&dir).ok();
    }
}
