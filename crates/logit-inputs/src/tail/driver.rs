//! The shared read/rotate/checkpoint/shutdown loop [`TailInput`](super::TailInput) and
//! `crate::docker::DockerInput` both reduce to -- generic over the decoder and how a matched
//! path becomes one, the only things those two ever differ in (the same shape
//! `crate::udp::UdpListener<D>` already established for the datagram listeners).

use super::checkpoint::{CheckpointStore, FileId};
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

/// One read off a tracked file at a time -- large enough to amortize the syscall over a busy
/// file, small enough that one file being read never starves the round-robin `drain` loop for
/// long. Not configurable: it's an internal batching detail, not something an operator has a
/// reason to tune (`docs/design/memory.md`'s "expose only what's actually load-bearing" bar).
const READ_CHUNK_BYTES: usize = 64 * 1024;

/// Turns a matched path into a decoder -- the one thing `TailInput`'s [`LineDecoderFactory`
/// (`super::LineDecoderFactory`)] and `docker_in`'s decoder factory differ in. [`accept`] runs
/// first and may reject a path outright (`docker_in`'s container filter); [`open`] then builds
/// the actual decoder for a path this factory has already accepted.
pub(crate) trait DecoderFactory<D: TailDecoder>: Send {
    /// Whether this path should be tailed at all. `tail_in`'s own factory always returns `true`
    /// -- every path a configured pattern matches is, by definition, wanted. `docker_in`'s
    /// factory reads the sibling `config.v2.json` and applies the configured container filter
    /// here, which is also why this takes `&mut self`: a factory may cache what it read to avoid
    /// re-reading it on every `scan`.
    fn accept(&mut self, path: &Path) -> bool;

    /// Builds the decoder for a path [`DecoderFactory::accept`] has already approved. An error
    /// here is diagnosed (`open_error`) and the path is simply not tailed this cycle -- retried
    /// on the next `scan` rather than treated as fatal to the whole listener.
    fn open(&mut self, path: &Path) -> anyhow::Result<D>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileState {
    Active,
    /// No longer matched by any pattern (renamed away, removed) or superseded by a new inode at
    /// the same path (rotated) -- read to EOF, flush, close; never written to `by_path` again.
    Draining,
}

/// What the runtime loop's own `select!` decided happened -- kept as a plain value with no
/// borrowed data, specifically so every `select!` arm can produce one without itself containing
/// an `.await` (see [`Tailer::run_until_shutdown`]'s comment on why that matters).
enum Outcome {
    Shutdown,
    Wake(super::watch::Wake),
    Poll,
    Flush,
    Checkpoint,
}

enum StartOffset {
    Beginning,
    End,
    /// Clamped to the file's current length by the caller if the checkpoint's own offset is now
    /// past it (the file was truncated between the checkpoint write and this restart).
    Resume(u64),
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
}

pub(crate) struct Tailer<D: TailDecoder, F: DecoderFactory<D>> {
    patterns: Vec<PathPattern>,
    factory: F,
    config: TailConfig,
    files: HashMap<FileId, TrackedFile<D>>,
    by_path: HashMap<PathBuf, FileId>,
    checkpoint: Option<CheckpointStore>,
    resume: HashMap<FileId, (PathBuf, u64)>,
    diag: Diagnostics,
    telemetry: Telemetry,
    watched_dirs: HashSet<PathBuf>,
    /// Set by [`Tailer::bind`], taken back out by [`Tailer::run_until_shutdown`]
    /// (`docs/plans/operator-surface.md`, workstream B). `Option`, not a plain field: `Watcher`
    /// has no meaningful "not yet opened" value, and taking it back out restores the exact local
    /// variable this loop had before `bind` existed -- see that method's own comment.
    watcher: Option<super::watch::Watcher>,
}

impl<D: TailDecoder, F: DecoderFactory<D>> Tailer<D, F> {
    pub fn new(patterns: Vec<PathPattern>, factory: F, config: TailConfig) -> Self {
        Self {
            patterns,
            factory,
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

    /// Test-only: how many files this `Tailer` is currently tracking -- lets a test confirm
    /// [`Tailer::bind`]'s initial scan actually discovered something, without going through a
    /// full `run_until_shutdown` + `Fanout` round trip just to prove that.
    #[cfg(test)]
    pub(crate) fn tracked_len(&self) -> usize {
        self.files.len()
    }

    /// Loads the checkpoint (if configured), opens the platform watcher, and performs the
    /// initial directory scan -- everything [`Tailer::run_until_shutdown`] used to do inline
    /// before this method existed, moved here so `crate::runtime::run_with_telemetry`'s bind
    /// pre-pass (`docs/plans/operator-surface.md`, workstream B) can do it *before* any task is
    /// spawned. Idempotent: a second call is a no-op, per [`logit_pipeline::Input::bind`]'s
    /// contract -- `self.watcher` already being `Some` is how it knows.
    pub async fn bind(&mut self) -> anyhow::Result<()> {
        if self.watcher.is_some() {
            return Ok(());
        }
        if let Some(checkpoint_path) = self.config.checkpoint_path.clone() {
            let (store, resume) = CheckpointStore::load(checkpoint_path, &mut self.diag);
            self.checkpoint = Some(store);
            self.resume = resume;
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
        // `bind` normally already ran, in `run_with_telemetry`'s pre-pass, before this task was
        // even spawned; a caller driving a `Tailer` directly (this module's own tests,
        // `spawn_tailer`) gets it here instead -- `Input::bind`'s documented lazy fallback.
        self.bind().await?;
        // Back into a local, exactly the shape this loop had before `bind` existed: `watcher` is
        // borrowed `&mut` by both this `select!` and by `scan`/`reconcile_watches` below, while
        // `self` is separately borrowed mutably by those same calls -- `take()` keeps every one
        // of those borrows a plain, direct-field access rather than reaching through `self`.
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
            // Every arm below produces a plain `Outcome` value with no `.await` inside it --
            // deliberately: an arm that awaits something *after* `shutdown.wait_for(..)` has
            // already been offered as a sibling branch makes the whole `select!` future hold a
            // `tokio::sync::watch::Ref` (a lock guard, not `Send`) live across that further
            // await, which `#[async_trait]`'s `Send`-bound boxing then rejects at compile time.
            // Keeping every arm's body a single, immediately-ready expression and doing all
            // actual async work below, after the `select!` has already resolved, sidesteps that
            // entirely -- the same reason `crate::udp::read_loop`'s own two `shutdown.wait_for`
            // uses never do anything past a bare `break`/value inside their arms either.
            let outcome = tokio::select! {
                _ = shutdown.wait_for(|&due| due) => Outcome::Shutdown,
                wake = watcher.next_wake() => Outcome::Wake(wake),
                () = sleep_until_opt(Some(next_poll)) => Outcome::Poll,
                () = sleep_until_opt(next_flush), if next_flush.is_some() => Outcome::Flush,
                () = sleep_until_opt(next_checkpoint), if next_checkpoint.is_some() => Outcome::Checkpoint,
            };

            match outcome {
                Outcome::Shutdown => break,
                Outcome::Wake(wake) => {
                    self.telemetry.count("logit.input.watch.wakes", 1.0, &[("source", "inotify")]);
                    if matches!(wake, super::watch::Wake::Overflow) {
                        self.telemetry.count("logit.input.watch.overflows", 1.0, &[]);
                    }
                    self.scan(false, &mut watcher).await;
                }
                Outcome::Poll => {
                    self.telemetry.count("logit.input.watch.wakes", 1.0, &[("source", "poll")]);
                    next_poll = tokio::time::Instant::now() + self.config.poll_interval;
                    self.scan(false, &mut watcher).await;
                }
                Outcome::Flush => {
                    self.flush_all(&sink, FlushReason::Interval).await;
                    next_flush =
                        Some(tokio::time::Instant::now() + self.config.batching.flush_interval);
                }
                Outcome::Checkpoint => {
                    // Flush before persisting: once every accumulator has been flushed, every
                    // decoded event is already with `Fanout`, so the offset this is about to
                    // write can never outrun delivery. The two timers stay independent as
                    // *timers* (this doesn't touch `next_flush`) -- they're just correctly
                    // ordered on the tick where the checkpoint one fires.
                    self.flush_all(&sink, FlushReason::Interval).await;
                    self.write_checkpoint(false).await;
                    next_checkpoint =
                        Some(tokio::time::Instant::now() + self.config.checkpoint_interval);
                }
            }

            if self.drain(&sink, &mut shutdown).await {
                break; // shutdown fired mid-drain
            }
        }

        self.close_all_for_shutdown(&sink).await;
        self.flush_all(&sink, FlushReason::Shutdown).await;
        self.write_checkpoint(true).await;
        Ok(())
    }

    /// Brings the set of watched directories in line with what the patterns currently reach --
    /// `watch_dir` on anything newly present, `unwatch_dir` on anything gone. Called at the top of
    /// every `scan`, before discovery, deliberately: `docker_in`'s log file appears inside a
    /// container directory a moment *after* the directory itself does, so the directory has to be
    /// watched on the strength of existing at all, not on already holding a matching file. A no-op
    /// under `WatchMode::Poll` (both `Watcher` methods are), and effectively a no-op for `tail_in`,
    /// whose patterns' `watch_dirs()` is always exactly the single `dir()` already watched -- the
    /// existing `by_path` short-circuit in `InotifyWatcher::watch_dir` makes the repeat call free.
    fn reconcile_watches(&mut self, watcher: &mut super::watch::Watcher) {
        let desired: HashSet<PathBuf> =
            self.patterns.iter().flat_map(PathPattern::watch_dirs).collect();
        for dir in desired.difference(&self.watched_dirs) {
            let _ = watcher.watch_dir(dir); // same ignore-the-error policy the startup loop used
        }
        for dir in self.watched_dirs.difference(&desired) {
            watcher.unwatch_dir(dir);
        }
        self.watched_dirs = desired;
    }

    /// Discovers matched files, opens newly-seen ones, and reconciles rotation/truncation/
    /// removal for ones already tracked. `first` is `true` only for the very first call --
    /// files present then follow `config.read_from`; every later discovery starts at the
    /// beginning, since a file that didn't exist yet has no "before startup" to skip.
    async fn scan(&mut self, first: bool, watcher: &mut super::watch::Watcher) {
        self.reconcile_watches(watcher);
        // Copied out up front so it can be read below while `tracked` (borrowed from
        // `self.files`) is live -- the same precedent `open_tracked` already follows for
        // `self.config.batching`.
        let max_line_bytes = self.config.max_line_bytes;
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
                    if let Some(tracked) = self.files.get_mut(&id) {
                        let len = meta.len();
                        if len < tracked.offset {
                            if let Err(err) = tracked.file.seek(std::io::SeekFrom::Start(0)).await {
                                self.diag.warn_throttled("read_error", err);
                                continue;
                            }
                            let prev_offset = tracked.offset;
                            tracked.offset = 0;
                            // The pre-truncation generation's partial (or a stale `dropping =
                            // true`) belongs to file content that no longer exists -- reset the
                            // splitter along with the offset so it isn't spliced onto (or, mid-
                            // drop, swallows) the first line of the new generation. The held
                            // partial is discarded, not emitted: it's an unterminated fragment,
                            // and emitting it as if it were a whole line is worse than dropping a
                            // fragment the writer itself never terminated. The decoder can hold
                            // the exact same kind of cross-line state of its own -- `docker_in`'s
                            // `DockerDecoder` reassembles a Docker json-file entry split across
                            // more than one line, in `partial`/`dropping` fields that mirror this
                            // splitter's own -- so it gets the same reset, for the same reason.
                            tracked.splitter = LineSplitter::new(max_line_bytes);
                            tracked.decoder.reset();
                            self.diag.warn_throttled(
                                "truncated",
                                truncated_message(&path, prev_offset, len),
                            );
                            self.telemetry.count("logit.input.files.truncated", 1.0, &[]);
                        }
                    }
                }
                Some(existing_id) => {
                    if let Some(tracked) = self.files.get_mut(&existing_id) {
                        tracked.state = FileState::Draining;
                    }
                    self.telemetry.count("logit.input.files.rotated", 1.0, &[]);
                    self.by_path.remove(&path);
                    self.open_tracked(path, id, StartOffset::Beginning).await;
                }
                None => {
                    let start = self.resume.remove(&id).map_or_else(
                        || {
                            if first {
                                match self.config.read_from {
                                    super::ReadFrom::Beginning => StartOffset::Beginning,
                                    super::ReadFrom::End => StartOffset::End,
                                }
                            } else {
                                StartOffset::Beginning
                            }
                        },
                        |(_path, offset)| StartOffset::Resume(offset),
                    );
                    self.open_tracked(path, id, start).await;
                }
            }
        }

        self.telemetry.gauge("logit.input.files.open", self.files.len() as f64, &[]);
    }

    /// Opens a newly-discovered `(path, id)` pair at `start` -- unless `id` is already tracked
    /// under a different path, in which case that existing entry is rebound to `path` instead of
    /// being reopened. This happens when one configured pattern matches a file both before and
    /// after a rename (`app.log*` matching both `app.log` and `app.log.1`): the same inode is then
    /// discovered a second time under its new name in the same `scan`. The existing entry is
    /// authoritative -- it holds the correct offset, the in-flight `LineSplitter` partial, decoder
    /// state, and a possibly non-empty accumulator -- so the only thing that actually changed is
    /// the name the inode is reachable under; re-opening at offset 0 would throw all of that away
    /// and re-emit the whole file.
    async fn open_tracked(&mut self, path: PathBuf, id: FileId, start: StartOffset) {
        if let Some(tracked) = self.files.get_mut(&id) {
            // Same inode, new name: adopt the existing entry rather than reopening. Reviving a
            // `Draining` entry back to `Active` is correct here -- the inode is once again matched
            // by a configured pattern under a real name, so it should keep being tailed, not
            // reaped; reap is for inodes no pattern reaches any more.
            let old_path = std::mem::replace(&mut tracked.path, path.clone());
            tracked.state = FileState::Active;
            // Removing the stale `by_path` entry is load-bearing, not tidiness -- but only if it
            // still belongs to this inode. `discovered` iteration order is nondeterministic: a
            // rotation replacement discovered earlier in the same `scan` may already have claimed
            // `old_path` for a different id (e.g. `app.log*` matching both the rotated `app.log`
            // and its replacement, with the replacement's arm running first). Removing the entry
            // unconditionally would then delete that other inode's freshly-inserted binding,
            // orphaning it from `by_path` while it stays `Active` in `self.files` -- and since the
            // stale-detection loop only walks `by_path.keys()`, an orphaned live inode can never be
            // marked `Draining` or reaped. Checking ownership first makes both iteration orders
            // converge on the same result without ever evicting a binding this rebind doesn't own.
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
        };
        self.by_path.insert(path, id);
        self.files.insert(id, tracked);
        if let Some(cp) = &mut self.checkpoint {
            cp.mark_dirty();
        }
    }

    /// One round-robin pass over every tracked file, reading and decoding whatever is currently
    /// available, repeating until no file made progress -- so a burst of writes to one file is
    /// fully drained in one call rather than waiting for the next external wake. Draining files
    /// that reach EOF are closed. Returns `true` if shutdown fired mid-drain (checked between
    /// files, not mid-read: one file's own read+decode is small and bounded, so this never waits
    /// long past shutdown even without an internal race).
    async fn drain(&mut self, sink: &Fanout, shutdown: &mut watch::Receiver<bool>) -> bool {
        loop {
            let mut any_progress = false;
            let mut at_eof: Vec<FileId> = Vec::new();
            let ids: Vec<FileId> = self.files.keys().copied().collect();
            for id in ids {
                if *shutdown.borrow() {
                    return true;
                }
                if self.read_one(id, sink).await {
                    any_progress = true;
                } else {
                    at_eof.push(id);
                }
            }
            self.reap_drained(&at_eof, sink).await;
            if !any_progress {
                return false;
            }
        }
    }

    /// Reads one chunk from the tracked file `id`, decodes every complete line it yields, and
    /// emits any batch that reaches a bound. Returns whether it actually read anything --
    /// `false` means this file is at EOF for now (nothing more to do until the next wake).
    async fn read_one(&mut self, id: FileId, sink: &Fanout) -> bool {
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

        let mut lines: Vec<Bytes> = Vec::new();
        let dropped = {
            let tracked = match self.files.get_mut(&id) {
                Some(t) => t,
                None => return false,
            };
            let stats = tracked.splitter.push(bytes, |line| lines.push(line));
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
        for line in lines {
            let line = ensure_utf8(line, &mut self.diag);
            let Some(tracked) = self.files.get_mut(&id) else { return false };
            self.telemetry.count("logit.input.lines", 1.0, &[]);
            self.telemetry.count("logit.input.line.bytes", line.len() as f64, &[]);
            match tracked.decoder.decode_line(line, read_at, &mut scratch) {
                Ok(resource) => {
                    if let Some((batch, reason)) =
                        tracked.accumulator.absorb(resource, &mut scratch)
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

    /// Closes every tracked file marked [`FileState::Draining`] whose own `read_one` returned
    /// nothing on the *current* pass (`at_eof`, built by [`Tailer::drain`]) -- not every Draining
    /// file unconditionally. `read_one` reads at most one [`READ_CHUNK_BYTES`] chunk per call, so
    /// a file marked Draining with a large unread backlog must be given as many passes as it takes
    /// to actually reach EOF before it's reaped; reaping it after just one chunk would lose the
    /// rest, unrecoverably (the entry is also pruned from the next checkpoint write). A read
    /// *error* also counts as EOF here (`read_one` returns `false` for it too, deliberately): an
    /// erroring handle will never drain on its own, and waiting for it to would starve every other
    /// tracked file. Gives the reaped decoder a chance to emit anything held back, flushes its
    /// accumulator (`FlushReason::Closed`), and drops it from the tracked set (which is also what
    /// makes it disappear from the next checkpoint write -- see `CheckpointStore::write`'s doc
    /// comment).
    async fn reap_drained(&mut self, at_eof: &[FileId], sink: &Fanout) {
        let draining: Vec<FileId> = self
            .files
            .iter()
            .filter(|(id, f)| f.state == FileState::Draining && at_eof.contains(id))
            .map(|(id, _)| *id)
            .collect();
        for id in draining {
            let Some(mut tracked) = self.files.remove(&id) else { continue };
            close_decoder(&mut tracked, sink, &self.telemetry, &mut self.diag).await;
            if let Some(batch) = tracked.accumulator.take() {
                emit(sink, &self.telemetry, batch, FlushReason::Closed).await;
            }
        }
    }

    /// Gives every still-tracked decoder -- not just [`FileState::Draining`] ones -- a chance to
    /// emit anything held back, as part of shutdown. Without this, a last line that was read (and
    /// already counted in the checkpoint offset -- `read_one` advances `offset` per chunk, not
    /// per decoded line) but never newline-terminated would simply be lost: still sitting in
    /// [`LineSplitter`]'s `partial` buffer when the process exits, with no future restart ever
    /// re-reading those bytes to recover it. Deliberately does *not* remove entries from
    /// `self.files` the way [`Tailer::reap_drained`] does: [`Tailer::write_checkpoint`] runs right
    /// after this and still needs every file's current offset, and these files aren't actually
    /// going away -- only the component reading them is.
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

    async fn write_checkpoint(&mut self, force: bool) {
        let Some(checkpoint) = &mut self.checkpoint else { return };
        // Subtract each file's held partial: those bytes are already counted in `offset` (`
        // read_one` advances it per chunk, not per decoded line) but haven't produced an event, so
        // persisting them would let a restart skip a line nothing downstream has seen. A file
        // that's `dropping` an oversized line deliberately contributes nothing here either -- an
        // empty `partial` while `dropping` -- since those bytes belong to a line already dropped
        // for exceeding `max_line_bytes`, and replaying them on restart simply re-drops it. At
        // shutdown this is moot: `close_all_for_shutdown` drains every partial via `take_partial`
        // before `write_checkpoint(true)` runs, so `pending_bytes()` is already 0 by then.
        let entries = self
            .files
            .values()
            .map(|f| (f.id, f.path.as_path(), f.offset.saturating_sub(f.splitter.pending_bytes())));
        checkpoint.write(entries, force, &mut self.diag, &self.telemetry);
    }
}

/// Gives `tracked`'s decoder a chance to emit anything held back -- an unterminated last line
/// ([`LineSplitter::take_partial`]) plus whatever [`TailDecoder::close`] itself produces
/// (`docker_in`'s dangling reassembled entry) -- absorbing both into `tracked`'s own accumulator
/// and flushing immediately if that reaches a bound. Shared by [`Tailer::reap_drained`] (a file
/// that's actually going away) and [`Tailer::close_all_for_shutdown`] (files that are still
/// tracked, just no longer being read).
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
                if let Some((batch, reason)) = tracked.accumulator.absorb(resource, &mut scratch) {
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
    if !scratch.is_empty() {
        let resource = tracked.decoder.resource();
        if let Some((batch, reason)) = tracked.accumulator.absorb(resource, &mut scratch) {
            emit(sink, telemetry, batch, reason).await;
        }
    }
}

/// Validates `line` as UTF-8, lossily converting (and diagnosing) it if it isn't --
/// `Value::Str`'s own invariant requires valid UTF-8, so this is what upholds that for a
/// decoder, rather than every `TailDecoder` implementation having to.
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

/// The `truncated` diagnostic's text. A free function purely so a test can assert it reports the
/// *pre*-truncation offset -- the number an operator needs to know how much was in flight -- and
/// not the freshly-reset one.
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

/// `tokio::time::sleep_until` doesn't accept `Option<Instant>` directly -- this is the same
/// "sleep forever if there's nothing to wait for" idiom `crate::udp::decode_loop` uses via
/// `tokio::time::timeout`, spelled as a sleep instead since this loop's `select!` needs every
/// branch to be a plain future, not a wrapped one.
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

    /// The `tail_in` factory, reimplemented here rather than reused from `super::super` --
    /// `LineDecoderFactory` (`tail/mod.rs`) is private to that module, and pulling in its own
    /// `Diagnostics`-carrying shape would add nothing these tests need over this trivial stand-in.
    struct LineFactory;

    impl DecoderFactory<LineDecoder> for LineFactory {
        fn accept(&mut self, _path: &Path) -> bool {
            true
        }

        fn open(&mut self, path: &Path) -> anyhow::Result<LineDecoder> {
            Ok(LineDecoder::new(path, Arc::new(Resource::default())))
        }
    }

    fn recording_fanout(capacity: usize) -> (Fanout, mpsc::Receiver<Delivered>) {
        let (tx, rx) = mpsc::channel(capacity);
        (Fanout::new(vec![tx]), rx)
    }

    /// Short poll/checkpoint/flush intervals so every test observes real ticks within a couple
    /// hundred milliseconds rather than waiting out this crate's production defaults (seconds).
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
                shutdown_grace: Duration::from_secs(5),
            },
        }
    }

    fn spawn_tailer(
        mut tailer: Tailer<LineDecoder, LineFactory>,
        sink: Fanout,
    ) -> (watch::Sender<bool>, tokio::task::JoinHandle<anyhow::Result<()>>) {
        let (tx, rx) = watch::channel(false);
        let handle = tokio::spawn(async move { tailer.run_until_shutdown(sink, rx).await });
        (tx, handle)
    }

    /// Signals shutdown and waits for the task to exit -- bounded, so a regression that makes
    /// shutdown hang fails this test instead of the whole suite.
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

    /// Receives batches until at least `n` events have accumulated, flattening them into arrival
    /// order. Bounded by an internal timeout so a stuck driver fails the test instead of hanging
    /// the suite.
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

    /// The most recent value of gauge `name` across everything currently buffered on `registry` --
    /// draining is destructive, so this is only meaningful as a one-shot check at a point where
    /// the test already knows nothing else is racing to re-set it.
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

    // -- workstream B: `Tailer::bind` (docs/plans/operator-surface.md) --

    /// `bind()` performs the initial directory scan on its own, before `run_until_shutdown` is
    /// ever called -- the same discovery `run_until_shutdown`'s prologue used to do inline.
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

    /// A second `bind()` call is a no-op, per [`logit_pipeline::Input::bind`]'s idempotency
    /// contract -- it must not re-scan (which could otherwise double-open an already-tracked
    /// file).
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

    /// `run_until_shutdown` still binds on its own when the caller never called `bind()` first --
    /// [`logit_pipeline::Input::bind`]'s documented lazy fallback, and the reason no existing
    /// direct-`run_until_shutdown` test in this module needed to change for this workstream.
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
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        // Let the initial scan (which applies `read_from`) settle before appending -- otherwise a
        // scheduling race could have the append land before the file was ever opened.
        tokio::time::sleep(Duration::from_millis(45)).await;
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
        // `read_from: end` -- proving this only governs files present at startup; a file that
        // doesn't exist yet always starts at the beginning once it appears.
        let tailer = Tailer::new(
            vec![PathPattern::new(dir.join("*.log"))],
            LineFactory,
            fast_config(ReadFrom::End),
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        tokio::time::sleep(Duration::from_millis(30)).await;
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

        // Rename away, then recreate fresh at the same path -- both fast enough to land within
        // one scan, so the driver sees the same path under a new inode rather than a transient
        // removal.
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

        // `std::fs::write` opens with `O_TRUNC` on the existing path -- same inode, shorter
        // length -- genuine truncation, not a rotation.
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
        // Give a couple of poll cycles time to notice the removal and reap the file.
        tokio::time::sleep(Duration::from_millis(90)).await;

        assert_eq!(
            gauge_value(&registry, "logit.input.files.open"),
            Some(0.0),
            "files.open should drop to 0 once the removed file is reaped"
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

        // Give at least one interval tick a chance to fire now that reading made the store dirty.
        tokio::time::sleep(Duration::from_millis(70)).await;
        assert!(
            checkpoint_path.exists(),
            "an interval tick should have written the dirty checkpoint"
        );

        shutdown(shutdown_tx, handle).await;

        // A fresh tailer resuming from that checkpoint must not replay "line one" -- only content
        // appended after the checkpoint was written.
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

        let mut config = fast_config(ReadFrom::End); // must be ignored -- the resume entry wins
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

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(
            vec![PathPattern::new(&a_path), PathPattern::new(&b_path)],
            LineFactory,
            config,
        );
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let _ = expect_events(&mut rx, 2).await;
        tokio::time::sleep(Duration::from_millis(50)).await; // let a checkpoint tick fire

        std::fs::remove_file(&b_path).unwrap();
        tokio::time::sleep(Duration::from_millis(70)).await; // reap, then another tick

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
        // A batch bound and flush interval this test will never reach on its own -- only
        // shutdown's own final flush should ever deliver anything. The checkpoint interval must
        // be pushed out too: a checkpoint tick now flushes before it persists, so a short one
        // (fast_config's default) would flush this batch early and defeat the point of this test.
        config.batching.max_events = 1_000;
        config.batching.flush_interval = Duration::from_secs(60);
        config.checkpoint_interval = Duration::from_secs(60);
        config.checkpoint_path = Some(checkpoint_path.clone());

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        // Give the initial scan+read time to happen, well before any interval flush would fire.
        tokio::time::sleep(Duration::from_millis(60)).await;
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

        // Nothing more should arrive while the last line has no trailing newline.
        let more = tokio::time::timeout(Duration::from_millis(80), rx.recv()).await;
        assert!(more.is_err(), "an unterminated line must not be emitted before it closes");

        // Shutdown gives the decoder a chance to close, flushing the held partial even though no
        // newline ever arrived.
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

        // Don't read from `rx` at all for a while -- the tailer must simply stop making
        // progress, never drop anything.
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
        // Comfortably larger than one read chunk (64 KiB) so draining `busy.log` takes more than
        // one `read_one` call -- if the driver read one file to EOF before ever touching the
        // next, `quick.log`'s single line would only arrive after every one of these.
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

        // Drains every event from both files to completion (rather than stopping the moment
        // "quick line" is seen) -- the tailer itself keeps running regardless of what this test
        // asserts, and with a one-event fanout capacity of 64 it would simply block forever on a
        // full channel if this stopped consuming early.
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

    /// The whole point of workstream B: `inotify` discovers a *new* file well before the next
    /// `poll_interval` tick would ever fire -- `poll_interval` here (30s) is set far longer than
    /// this test's own timeout, so a discovery at all proves it came from the wake source, not
    /// from polling. (Reading more bytes appended to an *already-tracked* file isn't gated by
    /// either `poll_interval` or the watch mode at all -- `drain`'s round-robin read runs after
    /// every loop iteration regardless of what woke it, since an already-open file handle simply
    /// sees new bytes on its next `read()`. `poll_interval`/`inotify` only govern *discovering*
    /// a path -- new files, rotation, truncation -- which is what these two tests exercise.)
    #[tokio::test]
    async fn under_inotify_a_new_file_is_discovered_well_before_the_poll_interval() {
        let dir = scratch_dir("inotify-latency");

        let (fanout, mut rx) = recording_fanout(8);
        let mut config = fast_config(ReadFrom::End);
        config.watch = WatchMode::Inotify;
        config.poll_interval = Duration::from_secs(30);
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        // Give the initial scan a moment to run (and start watching `dir`) before the file
        // appears.
        tokio::time::sleep(Duration::from_millis(30)).await;
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

    /// The mirror image: under `poll` (never `auto`/`inotify`), the same new file is only ever
    /// discovered on the next `poll_interval` tick -- nothing wakes the driver early. The default
    /// `flush_interval` (15ms) stays on: it can only ever *deliver* what `scan` has already
    /// discovered (`Outcome::Flush` never calls `scan` itself), so it can't make this test pass
    /// by accident -- it's what lets the discovered line actually reach `rx` promptly once the
    /// poll tick does fire, the same as any other test in this module.
    #[tokio::test]
    async fn under_poll_a_new_file_is_discovered_only_after_the_poll_interval() {
        let dir = scratch_dir("poll-latency");

        let (fanout, mut rx) = recording_fanout(8);
        let mut config = fast_config(ReadFrom::End);
        config.watch = WatchMode::Poll;
        config.poll_interval = Duration::from_millis(300);
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        tokio::time::sleep(Duration::from_millis(30)).await;
        std::fs::write(dir.join("app.log"), b"woke\n").unwrap();

        // Nothing should arrive well before the poll tick.
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

    #[tokio::test]
    async fn a_draining_file_with_more_than_one_chunk_of_backlog_is_fully_read_before_close() {
        let dir = scratch_dir("drain-backlog");
        let path = dir.join("app.log");
        let content: String = (0..8_000).map(|i| format!("drain-{i:05}\n")).collect();
        assert!(content.len() > 64 * 1024, "fixture must exceed one read chunk");
        std::fs::write(&path, content.as_bytes()).unwrap();

        // A capacity-1 fanout with one event per batch stalls the driver inside `emit` almost
        // immediately, so only a small prefix has been read by the time the file is removed --
        // this is what makes the test deterministic rather than racing the reader.
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let mut config = fast_config(ReadFrom::Beginning);
        config.batching.max_events = 1;
        // A directory glob -- so the removal below is what makes the tracked path stale, driving
        // it into `FileState::Draining`, rather than the pattern itself no longer matching.
        let tailer = Tailer::new(vec![PathPattern::new(dir.join("*.log"))], LineFactory, config);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        // Don't consume `rx` at all -- the driver stalls with the vast majority of the file still
        // unread.
        tokio::time::sleep(Duration::from_millis(60)).await;
        std::fs::remove_file(&path).unwrap();
        // A poll tick marks the file `Draining` while it is still mostly unread.
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
        // No trailing newline -- the trailing fragment "partialpartial" is held in the splitter's
        // `partial` buffer, and is long enough that the post-truncation length (10) is strictly
        // less than the tracked offset (18), so `scan` classifies this as a truncation at all.
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

        // `std::fs::write` opens with `O_TRUNC` on the existing path -- same inode, 10 < 18 bytes
        // -- a real truncation, exactly as `truncation_seeks_to_zero_and_reports_truncated` does
        // it.
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

        // Both the rotated-away file and the fresh replacement now match the same `app.log*`
        // pattern -- the exact scenario that used to re-open the rotated-away inode from byte 0.
        std::fs::rename(&path, dir.join("app.log.1")).unwrap();
        std::fs::write(&path, b"after\n").unwrap();

        let events2 = expect_events(&mut rx, 1).await;
        assert_eq!(messages(&events2), vec!["after"]);

        // The actual regression: the bug re-emits "before" from byte 0 of `app.log.1`.
        let extra = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(extra.is_err(), "the renamed inode must not be re-read from byte 0");

        // The entry must have been rebound and kept Active, not silently dropped -- appending to
        // the renamed file must still be followed under its new name.
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

    /// White-box regression for the `by_path` orphaning bug in `open_tracked`'s rebind branch:
    /// removing the old `path -> id` entry whenever `old_path != path` (rather than only when
    /// that entry still belongs to this inode) could delete a replacement inode's freshly-inserted
    /// binding if that replacement's `scan` arm ran first. This can't be pinned by driving the
    /// spawned tailer's event stream -- the very next `scan` rediscovers the orphaned inode under
    /// its own name and re-adds the `by_path` entry, so the observable events self-heal either way
    /// and look identical whether the bug is present or fixed. Only inspecting `by_path`/`files`
    /// state directly, at the one problematic iteration order, distinguishes them -- so this test
    /// calls `scan`/`open_tracked` directly instead of spawning `run_until_shutdown`.
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

        // Replay, by hand, the one iteration order of `scan`'s `discovered` map that trips the
        // bug: the rotation-replacement arm for `app.log` (now inode `b`) runs before the
        // rebind arm for the renamed-away `app.log.1` (still inode `a`). `discovered` is a
        // `HashMap`, so the real `scan` picks either order nondeterministically; this pins the
        // one where the bug is observable.
        tailer.files.get_mut(&a).unwrap().state = FileState::Draining;
        tailer.by_path.remove(&dir.join("app.log"));
        tailer.open_tracked(dir.join("app.log"), b, StartOffset::Beginning).await;
        tailer.open_tracked(dir.join("app.log.1"), a, StartOffset::Beginning).await;

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
        // Only the checkpoint tick's own flush can ever deliver anything -- same trick
        // `shutdown_flushes_every_accumulator_and_writes_the_checkpoint_within_grace` uses.
        config.batching.flush_interval = Duration::from_secs(60);
        config.batching.max_events = 1_000;
        config.checkpoint_interval = Duration::from_millis(25);
        config.checkpoint_path = Some(dir.join("checkpoint.json"));

        let (fanout, mut rx) = recording_fanout(8);
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        // Before the fix, nothing is ever delivered within `expect_events`' 5s bound, because the
        // only flush is 60s out -- the checkpoint tick itself must be the one that flushes.
        let events = expect_events(&mut rx, 3).await;
        assert_eq!(messages(&events), vec!["one", "two", "three"]);

        shutdown(shutdown_tx, handle).await;
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_checkpoint_offset_never_covers_a_line_still_held_as_a_partial() {
        let dir = scratch_dir("checkpoint-partial");
        let path = dir.join("app.log");
        // Complete-line prefix is 14 bytes ("one\ntwo\nthree\n"); the rest is an unterminated
        // trailing line that must never be counted in a persisted offset.
        const COMPLETE_PREFIX_LEN: usize = 14;
        std::fs::write(&path, b"one\ntwo\nthree\nnot-terminated").unwrap();

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(dir.join("checkpoint.json"));
        let checkpoint_path = config.checkpoint_path.clone().unwrap();
        let tailer = Tailer::new(vec![PathPattern::new(&path)], LineFactory, config);
        let (fanout, mut rx) = recording_fanout(8);
        let (shutdown_tx, handle) = spawn_tailer(tailer, fanout);

        let _ = expect_events(&mut rx, 3).await;
        tokio::time::sleep(Duration::from_millis(70)).await; // let a checkpoint tick land

        // Read the checkpoint file while the tailer is still running -- `shutdown` would drain
        // the partial and legitimately advance the offset, masking the bug.
        let text = std::fs::read_to_string(&checkpoint_path).unwrap();
        // `serde_json::to_vec_pretty` puts a space after the colon.
        assert!(
            text.contains(&format!(r#""offset": {COMPLETE_PREFIX_LEN}"#)),
            "the checkpoint must not cover the unterminated trailing line: {text}"
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
}
