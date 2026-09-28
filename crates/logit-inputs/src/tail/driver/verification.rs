//! A state-machine property test for [`Tailer`] on the real filesystem, against a model written
//! independently of it, for TAIL-01, TAIL-02, and TAIL-03 of
//! `docs/plans/critical-sections-inventory.md`. The contract is
//! `docs/adr/tail-discovery-failure-and-resume-identity.md` and
//! `docs/adr/file-tailing-and-docker-json-logs.md`.
//!
//! Each case builds a scratch directory with up to three slots (`a.log`, `b.log`, `c.log`), all
//! tailed under one pattern kind: the literal name, or `<slot>.log*`. A random op sequence writes,
//! rotates, truncates, and deletes files for real, and drives the tailer by hand (`scan`, `drain`,
//! `flush_all`, `write_checkpoint`, a clean restart, a crash) on a current-thread runtime with
//! paused time, so a `Wait` op is the only thing that moves the clock. The rotation ops follow the
//! syscall sequences a real `logrotate` run recorded: `RenameRotate` shifts `.N` to `.N+1` highest
//! first, renames the live file to `.1`, and creates a new one, with a `Scan` free to land between
//! any two steps; `AppendToRenamed` is a writer still appending to the renamed inode before it
//! reopens; `CopyTruncate` copies the live file to a new `.1` and truncates it in place, then
//! scans. A listing or `stat` failure comes through the fault seam.
//!
//! [`Model`] restates the driver's rules over the model filesystem: which inode each path names,
//! what a scan retires, rebinds, opens, resumes, or finds truncated, how a drain splits, drops,
//! batches, and reaps, and what a checkpoint persists. After every op the test checks:
//!
//! - the batches received equal the model's, each in file order and in order per inode;
//! - the tracked set, each file's offset, pending bytes, state, path, head, and `rescanned`,
//!   `by_path`, and the retained resume entries equal the model's;
//! - the checkpoint on disk equals the model's persisted entries, and every persisted offset is a
//!   line start or the end of a prefix a clean stop emitted;
//! - every counter and diagnostic the driver keeps: lines, bytes, drops, truncations, rotations,
//!   scan errors, rejected resumes, renames, and flushes by reason; `open_error`, `read_error`,
//!   and every checkpoint error stay at zero;
//! - after a scan, the `by_path` invariants the driver relies on (one path per inode, only
//!   `Active` files bound) and the `files.open` gauge.
//!
//! At the end, every message received is a line written, or one side of a line a clean stop
//! split, and every complete line of an inode a pattern still reaches was received.
//!
//! `PROPTEST_CASES` overrides the case count for a deeper run.

use super::tests::{fast_config, messages, no_timer_due, LineFactory};
use super::*;
use crate::tail::line::LineDecoder;
use crate::tail::pattern::{READ_DIR, STAT};
use crate::tail::test_support::scratch_dir;
use crate::tail::watch::Watcher;
use crate::tail::ReadFrom;
use logit_pipeline::fault::{self, errno};
use logit_pipeline::test_util::{fanout_channel, TelemetryProbe};
use logit_pipeline::{unwrap_batch, Delivered};
use proptest::prelude::*;
use std::collections::VecDeque;
use std::io::Write;
use std::time::Duration;
use tokio::sync::mpsc;

/// `cases`, or `PROPTEST_CASES` when that is set.
fn config(cases: u32) -> ProptestConfig {
    let cases = std::env::var("PROPTEST_CASES").ok().and_then(|v| v.parse().ok()).unwrap_or(cases);
    ProptestConfig { cases, ..ProptestConfig::default() }
}

const SLOT_NAMES: [&str; 3] = ["a", "b", "c"];

/// Under paused time a stalled op (a full channel, a lost wake) lets the clock jump to this
/// deadline at once, so a hang fails the case instead of the run.
const OP_TIMEOUT: Duration = Duration::from_secs(10);

// -- Ops and strategies ------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScanFail {
    None,
    /// Every pattern's `read_dir` fails (`EACCES`): no information.
    ReadDir,
    /// The slot's live path `stat`s as `EIO`: unknown, so kept.
    Stat(usize),
    /// The slot's live path `stat`s as `ENOENT`, as if renamed between the listing and the
    /// `stat`: absent, so its inode starts draining.
    StatGone(usize),
}

/// One step. A slot `f` is taken modulo the case's slot count, and every op that touches a slot's
/// files first completes a line `AppendTorn` left unterminated on it.
#[derive(Debug, Clone)]
enum Op {
    /// Appends complete lines (content lengths `lens`) to `f.log`, creating it if absent.
    Append {
        f: usize,
        lens: Vec<usize>,
    },
    /// Appends the first `k` bytes of a `len`-byte line to `f.log`.
    AppendTorn {
        f: usize,
        len: usize,
        k: usize,
    },
    Create {
        f: usize,
    },
    /// logrotate's `create` mode, with a `Scan` after the shift (`Some(0)`) or the rename
    /// (`Some(1)`).
    RenameRotate {
        f: usize,
        scan_between: Option<u8>,
    },
    /// Appends to `f.log.1` through a new handle: a writer that hasn't reopened since the rename.
    AppendToRenamed {
        f: usize,
        lens: Vec<usize>,
    },
    /// logrotate's `copytruncate`: shift, copy `f.log` to a new `f.log.1`, truncate `f.log` in
    /// place, then scan (failing the listing if `scan_fails`).
    CopyTruncate {
        f: usize,
        scan_fails: bool,
    },
    Delete {
        f: usize,
    },
    /// Removes the highest `f.log.N`.
    DeleteOldest {
        f: usize,
    },
    Scan(ScanFail),
    Drain,
    Flush,
    /// The run loop's checkpoint tick: flush every accumulator, then a write if dirty.
    CheckpointTick,
    /// Advances the paused clock one `poll_interval`.
    Wait,
    /// What `run_until_shutdown` does after its loop, then a new `Tailer` binds.
    Restart {
        first_scan_fails: bool,
    },
    /// The `Tailer` is dropped with nothing flushed or written, then a new one binds.
    Crash {
        first_scan_fails: bool,
    },
}

fn slot() -> impl Strategy<Value = usize> {
    0usize..3
}

fn lens() -> impl Strategy<Value = Vec<usize>> {
    prop::collection::vec(10usize..=60, 1..=4)
}

fn op() -> impl Strategy<Value = Op> {
    let rare = || prop::bool::weighted(0.25);
    prop_oneof![
        30 => (slot(), lens()).prop_map(|(f, lens)| Op::Append { f, lens }),
        15 => Just(Op::Drain),
        10 => Just(Op::Scan(ScanFail::None)),
        2 => Just(Op::Scan(ScanFail::ReadDir)),
        2 => (slot(), any::<bool>()).prop_map(|(f, gone)| {
            Op::Scan(if gone { ScanFail::StatGone(f) } else { ScanFail::Stat(f) })
        }),
        8 => Just(Op::CheckpointTick),
        6 => (slot(), prop::option::of(0u8..2))
            .prop_map(|(f, scan_between)| Op::RenameRotate { f, scan_between }),
        4 => (slot(), (10usize..=60).prop_flat_map(|len| (Just(len), 1..len)))
            .prop_map(|(f, (len, k))| Op::AppendTorn { f, len, k }),
        4 => (slot(), any::<bool>()).prop_map(|(f, scan_fails)| Op::CopyTruncate { f, scan_fails }),
        4 => rare().prop_map(|first_scan_fails| Op::Restart { first_scan_fails }),
        4 => rare().prop_map(|first_scan_fails| Op::Crash { first_scan_fails }),
        3 => slot().prop_map(|f| Op::Create { f }),
        3 => slot().prop_map(|f| Op::Delete { f }),
        2 => Just(Op::Flush),
        2 => slot().prop_map(|f| Op::DeleteOldest { f }),
        6 => Just(Op::Wait),
        4 => (slot(), lens()).prop_map(|(f, lens)| Op::AppendToRenamed { f, lens }),
    ]
}

#[derive(Debug, Clone)]
struct Case {
    slots: usize,
    wildcard: bool,
    max_events: usize,
    max_line_bytes: usize,
    ops: Vec<Op>,
}

// -- The model ---------------------------------------------------------------------------------

/// A retained position: a resume entry, or one the checkpoint persists.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Entry {
    path: PathBuf,
    offset: u64,
    /// The bytes the fingerprint covers.
    head: Vec<u8>,
}

#[derive(Debug)]
struct MInode {
    id: FileId,
    /// The current generation's bytes: what's on disk.
    content: Vec<u8>,
    linked: bool,
    /// Offsets in the current generation where a clean stop emitted a line's prefix, so a
    /// persisted offset may sit there.
    splits: Vec<u64>,
}

#[derive(Debug, Clone, Copy)]
struct Draining {
    waits: u64,
    /// The scan (by count) that started it.
    scan: u64,
    /// A later scan could list the file's path: no failed listing covers it, and its `stat`
    /// wasn't unknown.
    rescanned: bool,
}

#[derive(Debug)]
struct MTracked {
    inode: usize,
    path: PathBuf,
    /// Bytes read.
    offset: u64,
    /// The unterminated line read so far; empty while dropping it.
    line: Vec<u8>,
    /// Its length so far, dropped or not.
    line_len: u64,
    dropping: bool,
    head: Vec<u8>,
    draining: Option<Draining>,
    acc: Vec<String>,
}

#[derive(Debug, Default, Clone, PartialEq)]
struct Counts {
    lines: u64,
    line_bytes: u64,
    long_line: u64,
    truncated: u64,
    rotated: u64,
    read_dir_errors: u64,
    stat_errors: u64,
    scan_error: u64,
    resume_rejected: u64,
    renamed: u64,
    max_events: u64,
    interval: u64,
    closed: u64,
    shutdown: u64,
}

struct Model {
    logs: PathBuf,
    slots: usize,
    wildcard: bool,
    max_events: usize,
    max_line_bytes: u64,
    inodes: Vec<MInode>,
    /// Per slot, the inode at `f.log` (index 0) and at each `f.log.N`.
    chains: Vec<Vec<Option<usize>>>,
    tracked: HashMap<FileId, MTracked>,
    by_path: HashMap<PathBuf, FileId>,
    resume: HashMap<FileId, Entry>,
    /// What the checkpoint on disk holds; empty before the first write.
    persisted: HashMap<FileId, Entry>,
    waits: u64,
    scans: u64,
    /// This op's expected batches, each with the inode it came from.
    out: Vec<(usize, Vec<String>)>,
    counts: Counts,
}

impl Model {
    fn path(&self, f: usize, i: usize) -> PathBuf {
        if i == 0 {
            self.logs.join(format!("{}.log", SLOT_NAMES[f]))
        } else {
            self.logs.join(format!("{}.log.{i}", SLOT_NAMES[f]))
        }
    }

    fn location(&self, inode: usize) -> Option<(usize, usize)> {
        self.chains.iter().enumerate().find_map(|(f, chain)| {
            chain.iter().position(|slot| *slot == Some(inode)).map(|i| (f, i))
        })
    }

    /// Every path the slot's pattern names now, with its inode.
    fn matched(&self, f: usize) -> Vec<(PathBuf, usize)> {
        self.chains[f]
            .iter()
            .enumerate()
            .filter(|(i, _)| self.wildcard || *i == 0)
            .filter_map(|(i, slot)| slot.map(|inode| (self.path(f, i), inode)))
            .collect()
    }

    fn reachable(&self, inode: usize) -> bool {
        self.location(inode).is_some_and(|(_, i)| self.wildcard || i == 0)
    }

    fn id(&self, inode: usize) -> FileId {
        self.inodes[inode].id
    }

    fn emit(&mut self, inode: usize, batch: Vec<String>, reason: FlushReason) {
        match reason {
            FlushReason::MaxEvents => self.counts.max_events += 1,
            FlushReason::Interval => self.counts.interval += 1,
            FlushReason::Closed => self.counts.closed += 1,
            FlushReason::Shutdown => self.counts.shutdown += 1,
            other => panic!("the model never flushes for {other:?}"),
        }
        self.out.push((inode, batch));
    }

    fn emit_line(&mut self, id: FileId, line: Vec<u8>) {
        self.counts.lines += 1;
        self.counts.line_bytes += line.len() as u64;
        let t = self.tracked.get_mut(&id).expect("tracked");
        t.acc.push(String::from_utf8(line).expect("lines are ASCII"));
        if t.acc.len() >= self.max_events {
            let (inode, batch) = (t.inode, std::mem::take(&mut t.acc));
            self.emit(inode, batch, FlushReason::MaxEvents);
        }
    }

    fn flush(&mut self, id: FileId, reason: FlushReason) {
        let t = self.tracked.get_mut(&id).expect("tracked");
        if !t.acc.is_empty() {
            let (inode, batch) = (t.inode, std::mem::take(&mut t.acc));
            self.emit(inode, batch, reason);
        }
    }

    fn flush_all(&mut self, reason: FlushReason) {
        let ids: Vec<FileId> = self.tracked.keys().copied().collect();
        for id in ids {
            self.flush(id, reason);
        }
    }

    /// Reads everything past the offset: a line is dropped, and counted once, when its length
    /// first exceeds the bound; an unterminated one is held.
    fn read(&mut self, id: FileId) {
        let t = self.tracked.get_mut(&id).expect("tracked");
        let content = &self.inodes[t.inode].content;
        if content.len() as u64 <= t.offset {
            return;
        }
        let chunk = content[t.offset as usize..].to_vec();
        capture_head(&mut t.head, t.offset, &chunk);
        t.offset += chunk.len() as u64;
        let mut complete = Vec::new();
        let mut rest = &chunk[..];
        while let Some(nl) = rest.iter().position(|&b| b == b'\n') {
            if t.line_len + nl as u64 > self.max_line_bytes {
                if !t.dropping {
                    self.counts.long_line += 1;
                }
            } else {
                let mut line = std::mem::take(&mut t.line);
                line.extend_from_slice(&rest[..nl]);
                complete.push(line);
            }
            t.line.clear();
            t.line_len = 0;
            t.dropping = false;
            rest = &rest[nl + 1..];
        }
        t.line_len += rest.len() as u64;
        if !t.dropping {
            if t.line_len > self.max_line_bytes {
                self.counts.long_line += 1;
                t.dropping = true;
                t.line.clear();
            } else {
                t.line.extend_from_slice(rest);
            }
        }
        for line in complete {
            self.emit_line(id, line);
        }
    }

    /// A close emits the unterminated line unless it's being dropped.
    fn close(&mut self, id: FileId) {
        let t = self.tracked.get_mut(&id).expect("tracked");
        if t.dropping || t.line_len == 0 {
            return;
        }
        let line = std::mem::take(&mut t.line);
        t.line_len = 0;
        let (inode, offset) = (t.inode, t.offset);
        self.inodes[inode].splits.push(offset);
        self.emit_line(id, line);
    }

    fn truncation_check(&mut self, id: FileId) {
        let t = self.tracked.get_mut(&id).expect("tracked");
        if self.inodes[t.inode].content.len() as u64 >= t.offset {
            return;
        }
        t.offset = 0;
        t.line.clear();
        t.line_len = 0;
        t.dropping = false;
        t.head.clear();
        self.counts.truncated += 1;
    }

    fn start_draining(&mut self, id: FileId) {
        let (waits, scan) = (self.waits, self.scans);
        let t = self.tracked.get_mut(&id).expect("tracked");
        t.draining.get_or_insert(Draining { waits, scan, rescanned: false });
    }

    /// Tracks `inode`, found at `path`, from its resume entry if one is accepted, else from `0`.
    fn open(&mut self, path: PathBuf, inode: usize) {
        let id = self.id(inode);
        let content = &self.inodes[inode].content;
        let len = content.len() as u64;
        let offset = match self.resume.remove(&id) {
            Some(e) => {
                let head_len = e.head.len();
                if e.offset <= len && head_len as u64 <= len && content[..head_len] == e.head[..] {
                    e.offset
                } else {
                    self.counts.resume_rejected += 1;
                    0
                }
            }
            None => 0,
        };
        let head = content[..offset.min(HEAD_BYTES as u64) as usize].to_vec();
        self.tracked.insert(
            id,
            MTracked {
                inode,
                path: path.clone(),
                offset,
                line: Vec::new(),
                line_len: 0,
                dropping: false,
                head,
                draining: None,
                acc: Vec::new(),
            },
        );
        self.by_path.insert(path, id);
    }

    fn scan(&mut self, fail: ScanFail) {
        let read_dir_failed = fail == ScanFail::ReadDir;
        let mut discovered: HashMap<PathBuf, usize> = HashMap::new();
        let mut unknown: HashSet<PathBuf> = HashSet::new();
        if read_dir_failed {
            self.counts.read_dir_errors += self.slots as u64;
            self.counts.scan_error += 1;
        } else {
            for f in 0..self.slots {
                for (path, inode) in self.matched(f) {
                    let live = path == self.path(f, 0);
                    match fail {
                        ScanFail::Stat(g) if g == f && live => {
                            unknown.insert(path);
                        }
                        ScanFail::StatGone(g) if g == f && live => {}
                        _ => {
                            discovered.insert(path, inode);
                        }
                    }
                }
            }
            if !unknown.is_empty() {
                self.counts.stat_errors += unknown.len() as u64;
                self.counts.scan_error += 1;
            }
        }
        // Every bound path matches its own slot's pattern, so a failed listing covers it.
        let listed = |p: &Path| !read_dir_failed && !unknown.contains(p);

        // Retire what a listing shows is gone; keep what it couldn't see unless it was unlinked.
        let bound: Vec<(PathBuf, FileId)> =
            self.by_path.iter().map(|(p, id)| (p.clone(), *id)).collect();
        for (path, id) in bound {
            if discovered.contains_key(&path) {
                continue;
            }
            if listed(&path) || !self.inodes[self.tracked[&id].inode].linked {
                self.start_draining(id);
                self.by_path.remove(&path);
            } else {
                self.truncation_check(id);
            }
        }

        let found: HashMap<FileId, PathBuf> =
            discovered.iter().map(|(p, inode)| (self.id(*inode), p.clone())).collect();
        let rotated = discovered
            .iter()
            .filter(|(p, inode)| self.by_path.get(*p).is_some_and(|old| *old != self.id(**inode)))
            .count();
        self.counts.rotated += rotated as u64;

        // Order-free reconciliation. A path still bound to its inode is checked for truncation.
        // A path bound to another inode releases it, which drains unless it's found elsewhere.
        // Every inode found is then bound where it was found: rebound (and checked for
        // truncation) if tracked, else opened from its accepted entry or `0`.
        for (path, inode) in &discovered {
            let id = self.id(*inode);
            match self.by_path.get(path).copied() {
                Some(old) if old == id => self.truncation_check(id),
                Some(old) => {
                    if !found.contains_key(&old) {
                        self.start_draining(old);
                    }
                    self.by_path.remove(path);
                }
                None => {}
            }
        }
        for (path, inode) in &discovered {
            let id = self.id(*inode);
            if self.by_path.get(path) == Some(&id) {
                continue;
            }
            if let Some(t) = self.tracked.get_mut(&id) {
                t.path = path.clone();
                t.draining = None;
                self.counts.renamed += 1;
                self.by_path.retain(|_, bound| *bound != id);
                self.by_path.insert(path.clone(), id);
                self.truncation_check(id);
            } else {
                self.open(path.clone(), *inode);
            }
        }

        // An unspent entry goes once a listing shows nothing can consume it.
        self.resume.retain(|id, e| !listed(&e.path) || found.contains_key(id));

        let scan = self.scans;
        for t in self.tracked.values_mut() {
            if let Some(d) = &mut t.draining {
                if d.scan < scan && listed(&t.path) {
                    d.rescanned = true;
                }
            }
        }
        self.scans += 1;
    }

    /// Reads every file, then reaps each draining one that has been draining a `poll_interval`
    /// and been rescanned since.
    fn drain(&mut self) {
        let ids: Vec<FileId> = self.tracked.keys().copied().collect();
        for id in &ids {
            self.read(*id);
        }
        for id in ids {
            let due =
                self.tracked[&id].draining.is_some_and(|d| self.waits > d.waits && d.rescanned);
            if due {
                self.close(id);
                self.flush(id, FlushReason::Closed);
                self.tracked.remove(&id);
            }
        }
    }

    fn checkpoint(&self) -> HashMap<FileId, Entry> {
        let tracked = self.tracked.iter().map(|(id, t)| {
            let entry =
                Entry { path: t.path.clone(), offset: t.offset - t.line_len, head: t.head.clone() };
            (*id, entry)
        });
        tracked.chain(self.resume.iter().map(|(id, e)| (*id, e.clone()))).collect()
    }

    fn tick(&mut self) {
        self.flush_all(FlushReason::Interval);
        self.persisted = self.checkpoint();
    }

    /// `close_all_for_shutdown`, the shutdown flush, and the forced write.
    fn stop(&mut self) {
        let ids: Vec<FileId> = self.tracked.keys().copied().collect();
        for id in ids {
            self.close(id);
        }
        self.flush_all(FlushReason::Shutdown);
        self.persisted = self.checkpoint();
    }

    /// A new tailer loads the checkpoint on disk.
    fn reload(&mut self) {
        self.tracked.clear();
        self.by_path.clear();
        self.resume = self.persisted.clone();
    }
}

// -- The real side -----------------------------------------------------------------------------

async fn timed<T>(what: &str, fut: impl std::future::Future<Output = T>) -> T {
    match tokio::time::timeout(OP_TIMEOUT, fut).await {
        Ok(value) => value,
        Err(_) => panic!("{what} stalled"),
    }
}

struct Real {
    logs: PathBuf,
    checkpoint: PathBuf,
    patterns: Vec<PathPattern>,
    config: TailConfig,
    tailer: Tailer<LineDecoder, LineFactory>,
    watcher: Watcher,
    fanout: Fanout,
    rx: mpsc::Receiver<Delivered>,
    shutdown: watch::Receiver<bool>,
    _shutdown_tx: watch::Sender<bool>,
    probe: TelemetryProbe,
    diag: Diagnostics,
    start: tokio::time::Instant,
}

impl Real {
    fn new_tailer(&self) -> Tailer<LineDecoder, LineFactory> {
        Tailer::new(self.patterns.clone(), LineFactory, self.config.clone())
            .with_diagnostics(self.diag.clone())
            .with_telemetry(self.probe.telemetry("tail_in", "tail_in", "listener"))
    }

    /// Replaces the tailer with a new one and binds it. The old one is dropped first: its
    /// descriptors keep unlinked inodes, and their numbers, alive.
    async fn rebind(&mut self, first_scan_fails: bool) {
        let fresh = self.new_tailer();
        drop(std::mem::replace(&mut self.tailer, fresh));
        let scope = first_scan_fails.then(|| {
            let scope = fault::scope(&self.logs);
            scope.fail(READ_DIR, errno::EACCES);
            scope
        });
        timed("bind", self.tailer.bind()).await.expect("bind");
        drop(scope);
        self.watcher = self.tailer.watcher.take().expect("bind() leaves a watcher behind");
    }

    async fn scan(&mut self, fail: ScanFail, model: &Model) {
        let scope = match fail {
            ScanFail::None => None,
            ScanFail::ReadDir => {
                let scope = fault::scope(&self.logs);
                scope.fail(READ_DIR, errno::EACCES);
                Some(scope)
            }
            ScanFail::Stat(f) | ScanFail::StatGone(f) => {
                let scope = fault::scope(model.path(f, 0));
                let errno =
                    if matches!(fail, ScanFail::Stat(_)) { errno::EIO } else { errno::ENOENT };
                scope.fail(STAT, errno);
                Some(scope)
            }
        };
        timed("scan", self.tailer.scan(false, &mut self.watcher)).await;
        drop(scope);
    }

    fn received(&mut self) -> Vec<Vec<String>> {
        let mut batches = Vec::new();
        while let Ok(delivered) = self.rx.try_recv() {
            batches.push(messages(&unwrap_batch(delivered).events));
        }
        batches
    }

    /// The checkpoint on disk as `(offset, head_len, head_hash)` per inode; empty if absent.
    fn on_disk(&self) -> HashMap<FileId, (u64, u32, u64)> {
        let bytes = match std::fs::read(&self.checkpoint) {
            Ok(bytes) => bytes,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return HashMap::new(),
            Err(err) => panic!("reading the checkpoint: {err}"),
        };
        let doc: serde_json::Value = serde_json::from_slice(&bytes).expect("checkpoint JSON");
        let field = |e: &serde_json::Value, k: &str| e[k].as_u64().expect("a u64 field");
        doc["files"]
            .as_array()
            .expect("files")
            .iter()
            .map(|e| {
                let id = FileId { dev: field(e, "dev"), ino: field(e, "ino") };
                (id, (field(e, "offset"), field(e, "head_len") as u32, field(e, "head_hash")))
            })
            .collect()
    }
}

// -- The harness -------------------------------------------------------------------------------

struct World {
    dir: PathBuf,
    model: Model,
    real: Real,
    seq: u64,
    /// Per slot, the inode holding an unterminated line and the bytes that complete it.
    torn: Vec<Option<(usize, Vec<u8>)>>,
    /// Every line written, with where `AppendTorn` split it.
    written: HashMap<String, Option<usize>>,
    received: Vec<String>,
    restarts: u64,
    ticks: u64,
    ctx: String,
}

impl World {
    async fn new(case: &Case) -> Self {
        let dir = scratch_dir("tail-driver-state-machine");
        let logs = dir.join("logs");
        let state = dir.join("state");
        std::fs::create_dir_all(&logs).unwrap();
        std::fs::create_dir_all(&state).unwrap();
        let checkpoint = state.join("checkpoint.json");

        let mut config = fast_config(ReadFrom::Beginning);
        config.checkpoint_path = Some(checkpoint.clone());
        config.max_line_bytes = case.max_line_bytes;
        config.batching.max_events = case.max_events;
        config.batching.max_bytes = 1024 * 1024;
        let patterns = (0..case.slots)
            .map(|f| {
                let star = if case.wildcard { "*" } else { "" };
                PathPattern::new(logs.join(format!("{}.log{star}", SLOT_NAMES[f])))
            })
            .collect();

        let (fanout, rx) = fanout_channel(1 << 16);
        let (shutdown_tx, shutdown) = watch::channel(false);
        let placeholder = Tailer::new(Vec::new(), LineFactory, fast_config(ReadFrom::Beginning));
        let mut real = Real {
            logs: logs.clone(),
            checkpoint,
            patterns,
            config,
            tailer: placeholder,
            watcher: Watcher::Poll,
            fanout,
            rx,
            shutdown,
            _shutdown_tx: shutdown_tx,
            probe: TelemetryProbe::new(),
            diag: Diagnostics::new("test"),
            start: tokio::time::Instant::now(),
        };
        real.rebind(false).await;

        let mut model = Model {
            logs,
            slots: case.slots,
            wildcard: case.wildcard,
            max_events: case.max_events,
            max_line_bytes: case.max_line_bytes as u64,
            inodes: Vec::new(),
            chains: vec![Vec::new(); case.slots],
            tracked: HashMap::new(),
            by_path: HashMap::new(),
            resume: HashMap::new(),
            persisted: HashMap::new(),
            waits: 0,
            scans: 0,
            out: Vec::new(),
            counts: Counts::default(),
        };
        model.scan(ScanFail::None);
        Self {
            dir,
            model,
            real,
            seq: 0,
            torn: vec![None; case.slots],
            written: HashMap::new(),
            received: Vec::new(),
            restarts: 0,
            ticks: 0,
            ctx: "bind".to_string(),
        }
    }

    // -- filesystem ops, applied to disk and model together

    fn chain_slot(&mut self, f: usize, i: usize) -> &mut Option<usize> {
        let chain = &mut self.model.chains[f];
        if chain.len() <= i {
            chain.resize(i + 1, None);
        }
        &mut chain[i]
    }

    fn path_of(&self, inode: usize) -> PathBuf {
        let (f, i) = self.model.location(inode).expect("a linked inode");
        self.model.path(f, i)
    }

    /// Records the file created at `(f, i)`. Its inode number may be one a dead inode had; a
    /// live one (linked, or held open by the tailer) can't share it.
    fn record(&mut self, f: usize, i: usize, content: Vec<u8>) -> usize {
        let path = self.model.path(f, i);
        let id = FileId::from_metadata(&std::fs::metadata(&path).unwrap());
        for (ix, other) in self.model.inodes.iter().enumerate() {
            let open = self.model.tracked.values().any(|t| t.inode == ix);
            assert!(
                other.id != id || !(other.linked || open),
                "{}: a new file took a live inode's number",
                self.ctx
            );
        }
        let inode = self.model.inodes.len();
        self.model.inodes.push(MInode { id, content, linked: true, splits: Vec::new() });
        *self.chain_slot(f, i) = Some(inode);
        inode
    }

    fn ensure(&mut self, f: usize) -> usize {
        if let Some(inode) = self.model.chains[f].first().copied().flatten() {
            return inode;
        }
        std::fs::File::create_new(self.model.path(f, 0)).unwrap();
        self.record(f, 0, Vec::new())
    }

    fn append(&mut self, inode: usize, bytes: &[u8]) {
        let path = self.path_of(inode);
        std::fs::OpenOptions::new().append(true).open(path).unwrap().write_all(bytes).unwrap();
        self.model.inodes[inode].content.extend_from_slice(bytes);
    }

    /// A new line of `len` content bytes, unique by its sequence number, with its `\n`.
    fn line(&mut self, f: usize, len: usize, split: Option<usize>) -> Vec<u8> {
        self.seq += 1;
        let mut text = format!("{}{:06}-", SLOT_NAMES[f], self.seq);
        while text.len() < len {
            text.push('x');
        }
        self.written.insert(text.clone(), split);
        let mut bytes = text.into_bytes();
        bytes.push(b'\n');
        bytes
    }

    fn append_lines(&mut self, f: usize, inode: usize, lens: &[usize]) {
        for &len in lens {
            let line = self.line(f, len, None);
            self.append(inode, &line);
        }
    }

    fn complete_torn(&mut self, f: usize) {
        if let Some((inode, rest)) = self.torn[f].take() {
            assert_eq!(self.model.chains[f][0], Some(inode), "a torn line stays on the live file");
            self.append(inode, &rest);
        }
    }

    fn rename(&mut self, f: usize, from: usize, to: usize) {
        std::fs::rename(self.model.path(f, from), self.model.path(f, to)).unwrap();
        let moved = self.chain_slot(f, from).take();
        if let Some(replaced) = std::mem::replace(self.chain_slot(f, to), moved) {
            self.model.inodes[replaced].linked = false;
        }
    }

    /// `.N` to `.N+1`, highest first.
    fn shift(&mut self, f: usize) {
        for i in (1..self.model.chains[f].len()).rev() {
            if self.model.chains[f][i].is_some() {
                self.rename(f, i, i + 1);
            }
        }
    }

    fn remove(&mut self, f: usize, i: usize) {
        std::fs::remove_file(self.model.path(f, i)).unwrap();
        let inode = self.chain_slot(f, i).take().expect("present");
        self.model.inodes[inode].linked = false;
    }

    // -- ops

    async fn scan(&mut self, fail: ScanFail) {
        self.real.scan(fail, &self.model).await;
        self.model.scan(fail);
        self.check_scan(fail);
    }

    async fn apply(&mut self, op: &Op) {
        let slots = self.model.slots;
        match op.clone() {
            Op::Append { f, lens } => {
                let f = f % slots;
                self.complete_torn(f);
                let inode = self.ensure(f);
                self.append_lines(f, inode, &lens);
            }
            Op::AppendTorn { f, len, k } => {
                let f = f % slots;
                self.complete_torn(f);
                let inode = self.ensure(f);
                let line = self.line(f, len, Some(k));
                self.append(inode, &line[..k]);
                self.torn[f] = Some((inode, line[k..].to_vec()));
            }
            Op::Create { f } => {
                let f = f % slots;
                self.complete_torn(f);
                self.ensure(f);
            }
            Op::RenameRotate { f, scan_between } => {
                let f = f % slots;
                self.complete_torn(f);
                self.shift(f);
                if scan_between == Some(0) {
                    self.scan(ScanFail::None).await;
                }
                if self.model.chains[f].first().copied().flatten().is_some() {
                    self.rename(f, 0, 1);
                }
                if scan_between == Some(1) {
                    self.scan(ScanFail::None).await;
                }
                self.ensure(f);
            }
            Op::AppendToRenamed { f, lens } => {
                let f = f % slots;
                self.complete_torn(f);
                if let Some(inode) = self.model.chains[f].get(1).copied().flatten() {
                    self.append_lines(f, inode, &lens);
                }
            }
            Op::CopyTruncate { f, scan_fails } => {
                let f = f % slots;
                self.complete_torn(f);
                if let Some(live) = self.model.chains[f].first().copied().flatten() {
                    self.shift(f);
                    let content = self.model.inodes[live].content.clone();
                    let copy = self.model.path(f, 1);
                    std::fs::File::create_new(&copy).unwrap().write_all(&content).unwrap();
                    self.record(f, 1, content);
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(self.model.path(f, 0))
                        .unwrap()
                        .set_len(0)
                        .unwrap();
                    let inode = &mut self.model.inodes[live];
                    inode.content.clear();
                    inode.splits.clear();
                }
                self.scan(if scan_fails { ScanFail::ReadDir } else { ScanFail::None }).await;
            }
            Op::Delete { f } => {
                let f = f % slots;
                self.complete_torn(f);
                if self.model.chains[f].first().copied().flatten().is_some() {
                    self.remove(f, 0);
                }
            }
            Op::DeleteOldest { f } => {
                let f = f % slots;
                self.complete_torn(f);
                let oldest = (1..self.model.chains[f].len())
                    .rev()
                    .find(|&i| self.model.chains[f][i].is_some());
                if let Some(i) = oldest {
                    self.remove(f, i);
                }
            }
            Op::Scan(fail) => {
                let fail = match fail {
                    ScanFail::Stat(f) => ScanFail::Stat(f % slots),
                    ScanFail::StatGone(f) => ScanFail::StatGone(f % slots),
                    other => other,
                };
                self.scan(fail).await;
            }
            Op::Drain => {
                let real = &mut self.real;
                let end = timed(
                    "drain",
                    real.tailer.drain(
                        &real.fanout,
                        &real.shutdown,
                        &mut real.watcher,
                        no_timer_due(),
                    ),
                )
                .await;
                assert_eq!(end, DrainEnd::Idle, "{}", self.ctx);
                self.model.drain();
            }
            Op::Flush => {
                timed(
                    "flush",
                    self.real.tailer.flush_all(&self.real.fanout, FlushReason::Interval),
                )
                .await;
                self.model.flush_all(FlushReason::Interval);
            }
            Op::CheckpointTick => {
                let real = &mut self.real;
                timed("flush", real.tailer.flush_all(&real.fanout, FlushReason::Interval)).await;
                timed("checkpoint", real.tailer.write_checkpoint(false)).await;
                self.model.tick();
                self.ticks += 1;
                self.check_checkpoint(false);
            }
            Op::Wait => {
                tokio::time::advance(self.real.config.poll_interval).await;
                self.model.waits += 1;
            }
            Op::Restart { first_scan_fails } => {
                self.stop().await;
                self.restarts += 1;
                self.real.rebind(first_scan_fails).await;
                self.model.reload();
                self.model.scan(if first_scan_fails { ScanFail::ReadDir } else { ScanFail::None });
            }
            Op::Crash { first_scan_fails } => {
                self.real.rebind(first_scan_fails).await;
                self.model.reload();
                self.model.scan(if first_scan_fails { ScanFail::ReadDir } else { ScanFail::None });
            }
        }
    }

    /// What `run_until_shutdown` does after its loop.
    async fn stop(&mut self) {
        let real = &mut self.real;
        timed("close", real.tailer.close_all_for_shutdown(&real.fanout)).await;
        timed("flush", real.tailer.flush_all(&real.fanout, FlushReason::Shutdown)).await;
        timed("checkpoint", real.tailer.write_checkpoint(true)).await;
        self.model.stop();
        self.check_checkpoint(true);
    }

    // -- checks

    fn check(&mut self) {
        let ctx = &self.ctx;
        let got = self.real.received();
        let want = std::mem::take(&mut self.model.out);
        let mut queues: HashMap<usize, VecDeque<Vec<String>>> = HashMap::new();
        for (inode, batch) in want {
            queues.entry(inode).or_default().push_back(batch);
        }
        for batch in &got {
            let from = queues.iter().find(|(_, q)| q.front() == Some(batch)).map(|(k, _)| *k);
            match from {
                Some(inode) => {
                    queues.get_mut(&inode).unwrap().pop_front();
                }
                None => {
                    panic!("{ctx}: unexpected batch {batch:?}\nexpected {queues:?}\ngot {got:?}")
                }
            }
        }
        assert!(queues.values().all(VecDeque::is_empty), "{ctx}: missing {queues:?}; got {got:?}");
        self.received.extend(got.into_iter().flatten());

        // State, field by field.
        let tailer = &self.real.tailer;
        let model = &self.model;
        let ids =
            |keys: Vec<FileId>| keys.into_iter().map(|id| (id.dev, id.ino)).collect::<HashSet<_>>();
        assert_eq!(
            ids(tailer.files.keys().copied().collect()),
            ids(model.tracked.keys().copied().collect()),
            "{ctx}: the tracked set"
        );
        for (id, m) in &model.tracked {
            let f = &tailer.files[id];
            let state = (f.offset, f.splitter.pending_bytes(), f.state, &f.path);
            let draining =
                if m.draining.is_some() { FileState::Draining } else { FileState::Active };
            assert_eq!(
                state,
                (m.offset, m.line_len, draining, &m.path),
                "{ctx}: inode {}",
                m.inode
            );
            assert_eq!(f.head, m.head, "{ctx}: the head of inode {}", m.inode);
            let rescanned = m.draining.is_some_and(|d| d.rescanned);
            assert_eq!(f.rescanned, rescanned, "{ctx}: inode {} rescanned", m.inode);
        }
        assert_eq!(tailer.by_path, model.by_path, "{ctx}: by_path");
        let resume: HashMap<FileId, (u64, Head)> =
            tailer.resume.iter().map(|(id, r)| (*id, (r.offset, r.head))).collect();
        let want: HashMap<FileId, (u64, Head)> =
            model.resume.iter().map(|(id, e)| (*id, (e.offset, Head::of(&e.head)))).collect();
        assert_eq!(resume, want, "{ctx}: resume entries");

        // Time moves only on `Wait`.
        let elapsed = tokio::time::Instant::now() - self.real.start;
        assert_eq!(
            elapsed,
            self.real.config.poll_interval * model.waits as u32,
            "{ctx}: the clock"
        );

        self.check_counts();
    }

    fn check_counts(&mut self) {
        let ctx = &self.ctx;
        let c = &self.model.counts;
        let probe = &mut self.real.probe;
        let diag = &self.real.diag;
        let flushed = |probe: &mut TelemetryProbe, reason: &str| {
            probe.sum("logit.component.receive.flushed", &[("reason", reason)]) as u64
        };
        let got = Counts {
            lines: probe.sum("logit.input.lines", &[]) as u64,
            line_bytes: probe.sum("logit.input.line.bytes", &[]) as u64,
            long_line: diag.occurrences("long_line"),
            truncated: probe.sum("logit.input.files.truncated", &[]) as u64,
            rotated: probe.sum("logit.input.files.rotated", &[]) as u64,
            read_dir_errors: probe.sum("logit.input.scan.errors", &[("op", "read_dir")]) as u64,
            stat_errors: probe.sum("logit.input.scan.errors", &[("op", "stat")]) as u64,
            scan_error: diag.occurrences("scan_error"),
            resume_rejected: probe.sum("logit.input.files.resume_rejected", &[]) as u64,
            renamed: diag.occurrences("renamed"),
            max_events: flushed(probe, "max_events"),
            interval: flushed(probe, "interval"),
            closed: flushed(probe, "closed"),
            shutdown: flushed(probe, "shutdown"),
        };
        assert_eq!(&got, c, "{ctx}: counters");
        assert_eq!(diag.occurrences("truncated"), c.truncated, "{ctx}: the truncated diagnostic");
        assert_eq!(
            diag.occurrences("resume_rejected"),
            c.resume_rejected,
            "{ctx}: the resume_rejected diagnostic"
        );
        for key in ["open_error", "read_error", "checkpoint_error", "bad_line", "invalid_utf8"] {
            assert_eq!(diag.occurrences(key), 0, "{ctx}: {key}");
        }
        assert_eq!(
            probe.sum("logit.input.checkpoint.errors", &[]),
            0.0,
            "{ctx}: checkpoint errors"
        );
    }

    /// After a scan: the `by_path` invariants the driver relies on, and the `files.open` gauge.
    fn check_scan(&mut self, fail: ScanFail) {
        let ctx = &self.ctx;
        let tailer = &self.real.tailer;
        let mut owners = HashSet::new();
        for (path, id) in &tailer.by_path {
            assert!(owners.insert(*id), "{ctx}: two paths bound to one inode");
            let f =
                tailer.files.get(id).unwrap_or_else(|| panic!("{ctx}: {path:?} bound untracked"));
            assert_eq!(f.state, FileState::Active, "{ctx}: {path:?} bound to a non-Active file");
            assert_eq!(&f.path, path, "{ctx}: a bound file's own path");
            if fail == ScanFail::None {
                let now = FileId::from_metadata(&std::fs::metadata(path).unwrap());
                assert_eq!(now, *id, "{ctx}: {path:?} bound to an inode it no longer names");
            }
        }
        for (id, f) in &tailer.files {
            if f.state == FileState::Active {
                assert_eq!(tailer.by_path.get(&f.path), Some(id), "{ctx}: an unbound Active file");
            }
        }
        if fail == ScanFail::None {
            for f in 0..self.model.slots {
                for (path, _) in self.model.matched(f) {
                    assert!(
                        tailer.by_path.contains_key(&path),
                        "{ctx}: {path:?} matched, not bound"
                    );
                }
            }
        }
        let open = self.real.probe.gauge("logit.input.files.open", &[]);
        assert_eq!(open, Some(self.model.tracked.len() as f64), "{ctx}: files.open");
    }

    /// The checkpoint on disk equals the model's, and each offset of a current generation is a
    /// line start, or (after a clean stop) the end of a prefix the stop emitted.
    fn check_checkpoint(&self, stopped: bool) {
        let ctx = &self.ctx;
        let want: HashMap<FileId, (u64, u32, u64)> = self
            .model
            .persisted
            .iter()
            .map(|(id, e)| {
                let head = Head::of(&e.head);
                (*id, (e.offset, head.len, head.hash))
            })
            .collect();
        assert_eq!(self.real.on_disk(), want, "{ctx}: the checkpoint on disk");
        for (id, e) in &self.model.persisted {
            let Some(t) = self.model.tracked.get(id) else { continue };
            let inode = &self.model.inodes[t.inode];
            if e.offset == 0
                || e.offset > inode.content.len() as u64
                || !inode.content.starts_with(&e.head)
            {
                continue;
            }
            let line_start = inode.content[e.offset as usize - 1] == b'\n';
            let split = inode.splits.contains(&e.offset);
            assert!(
                line_start || split,
                "{ctx}: persisted offset {} of inode {} is mid-line (stopped: {stopped})",
                e.offset,
                t.inode
            );
        }
    }

    /// Every message is a written line or one side of a split one, and every complete line of
    /// an inode a pattern still reaches was received, whole or as both sides of its split.
    fn check_end(&self) {
        let mut pieces: HashSet<&str> = HashSet::new();
        for (text, split) in &self.written {
            pieces.insert(text.as_str());
            if let Some(k) = split {
                pieces.insert(&text[..*k]);
                pieces.insert(&text[*k..]);
            }
        }
        for message in &self.received {
            assert!(pieces.contains(message.as_str()), "garbled message {message:?}");
        }
        let received: HashSet<&str> = self.received.iter().map(String::as_str).collect();
        for (ix, inode) in self.model.inodes.iter().enumerate() {
            if !self.model.reachable(ix) {
                continue;
            }
            let complete = match inode.content.iter().rposition(|&b| b == b'\n') {
                Some(end) => &inode.content[..end],
                None => continue,
            };
            for line in complete.split(|&b| b == b'\n') {
                if line.len() as u64 > self.model.max_line_bytes {
                    continue;
                }
                let text = std::str::from_utf8(line).unwrap();
                let whole = received.contains(text);
                let halves = self.written.get(text).copied().flatten().is_some_and(|k| {
                    received.contains(&text[..k]) && received.contains(&text[k..])
                });
                assert!(
                    whole || halves,
                    "line {text:?} of reachable inode {ix} was never received"
                );
            }
        }
    }

    async fn epilogue(&mut self) {
        let steps =
            [Op::Scan(ScanFail::None), Op::Drain, Op::Wait, Op::Drain, Op::Scan(ScanFail::None)];
        for (i, op) in steps.iter().enumerate() {
            self.ctx = format!("epilogue {i}: {op:?}");
            self.apply(op).await;
            self.check();
        }
        self.ctx = "shutdown".to_string();
        self.stop().await;
        self.check();
        let writes = self.real.probe.sum("logit.input.checkpoint.writes", &[]) as u64;
        let floor = self.restarts + 1;
        assert!(
            (floor..=floor + self.ticks).contains(&writes),
            "checkpoint.writes {writes} outside {floor}..={}",
            floor + self.ticks
        );
        self.check_end();
    }
}

fn run(case: Case) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    runtime.block_on(async move {
        let mut world = World::new(&case).await;
        world.check();
        for (i, op) in case.ops.iter().enumerate() {
            world.ctx = format!("op {i}: {op:?}");
            world.apply(op).await;
            world.check();
        }
        world.epilogue().await;
        std::fs::remove_dir_all(&world.dir).ok();
    });
}

proptest! {
    #![proptest_config(config(64))]

    #[test]
    fn the_tail_driver_matches_its_model_on_the_real_filesystem(
        slots in 1usize..=3,
        wildcard in any::<bool>(),
        max_events in prop_oneof![Just(1usize), Just(3)],
        max_line_bytes in prop_oneof![Just(40usize), Just(1024 * 1024)],
        ops in prop::collection::vec(op(), 8..=64),
    ) {
        run(Case { slots, wildcard, max_events, max_line_bytes, ops });
    }
}

// -- Cases the model found before this test was committed, replayed through the harness. Each has
// a hand-driven twin in `driver.rs`'s tests.

/// An unspent checkpoint entry for an inode a rotation moves onto a still-bound path
/// (`a_rotation_arm_resumes_a_new_inode_from_its_unspent_checkpoint_entry_in_either_scan_order`).
#[test]
fn a_rotated_inode_with_an_unspent_entry_resumes() {
    use Op::*;
    run(Case {
        slots: 1,
        wildcard: true,
        max_events: 1,
        max_line_bytes: 1 << 20,
        ops: vec![
            Append { f: 0, lens: vec![20] },
            RenameRotate { f: 0, scan_between: None },
            Append { f: 0, lens: vec![20] },
            Scan(ScanFail::None),
            Drain,
            CheckpointTick,
            Restart { first_scan_fails: true },
            Scan(ScanFail::Stat(0)),
            RenameRotate { f: 0, scan_between: None },
            Scan(ScanFail::None),
            Drain,
        ],
    });
}

/// A stat race retires the live file, copytruncate empties it, and a refill passes the old
/// offset (`a_rebound_inode_truncated_while_draining_is_read_from_zero_not_from_its_stale_offset`).
#[test]
fn a_rebound_inode_truncated_while_draining_is_read_from_zero() {
    use Op::*;
    run(Case {
        slots: 1,
        wildcard: false,
        max_events: 1,
        max_line_bytes: 1 << 20,
        ops: vec![
            Append { f: 0, lens: vec![20, 20] },
            Scan(ScanFail::None),
            Drain,
            Scan(ScanFail::StatGone(0)),
            CopyTruncate { f: 0, scan_fails: false },
            Append { f: 0, lens: vec![30, 30] },
            Drain,
        ],
    });
}
