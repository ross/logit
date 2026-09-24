//! Property tests for the disk spool's recovery walk (DISK-01, DISK-02 in
//! `docs/plans/critical-sections-inventory.md`). Each case writes real records into one segment,
//! applies exactly one mutation, and checks what the walk and `DiskQueue::open` recover against a
//! model that knows which records the mutation touched.
//!
//! The generated records never embed a whole frame in a payload, so a record can only parse at
//! an offset where the model placed one. A payload that did embed one could still be read as a
//! phantom record after corruption; that's an accepted limit of a MAGIC-scan resync.
//!
//! `spool_model_every_push_is_delivered_dropped_or_queued` covers the write path (DISK-03,
//! DISK-05): random pushes, cancelled pushes, peeks, commits, injected failures, and
//! crash-reopens, checked against a model of what the spool counted queued or dropped.
//! `spool_model_a_bounded_block_spool_never_parks_a_push_that_nothing_will_wake` runs the same
//! model under a small `max_bytes` (DISK-05's `Block` concern, DISK-06): no push waits forever.

use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

use proptest::prelude::*;
use proptest::sample::Index;

use crate::disk_queue::test_support::{batch, config, marker_of, metric_sum, raw_record};
use crate::disk_queue::{
    list_segments, segment_path, walk_segment, DiskQueue, WalkOutcome, CONTEXT_LEN,
};
use crate::fanout::{BatchContext, TraceContext};
use crate::fault::{self, errno, sites, Op, Point};
use crate::queue::SINK_QUEUE_METRICS;
use logit_core::{Diagnostics, Provenance, Registry};
use logit_proto::frame;

/// A frame header's `compressed_len` sits 16 bytes into the header.
const COMPRESSED_LEN_AT: usize = CONTEXT_LEN + 16;
const WALK_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
struct RecordSpec {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    /// Where to plant `frame::MAGIC` inside `trace_id`, if anywhere.
    magic_at: Option<usize>,
}

#[derive(Debug, Clone)]
enum Mutation {
    FlipBit {
        at: Index,
        bit: u8,
    },
    Overwrite {
        at: Index,
        with: Vec<u8>,
    },
    Insert {
        at: Index,
        bytes: Vec<u8>,
    },
    Truncate {
        at: Index,
    },
    /// Rewrites one record's `compressed_len` to `extra` bytes past the end of the segment: under
    /// the frame layer's sanity cap, so it reads as `Truncated`.
    InCapLength {
        record: Index,
        extra: u32,
    },
}

fn record_spec() -> impl Strategy<Value = RecordSpec> {
    (any::<[u8; 16]>(), any::<[u8; 8]>(), prop::option::weighted(0.3, 0usize..=12))
        .prop_map(|(trace_id, span_id, magic_at)| RecordSpec { trace_id, span_id, magic_at })
}

fn mutation() -> impl Strategy<Value = Mutation> {
    prop_oneof![
        (any::<Index>(), 0u8..8).prop_map(|(at, bit)| Mutation::FlipBit { at, bit }),
        (any::<Index>(), prop::collection::vec(any::<u8>(), 1..=8))
            .prop_map(|(at, with)| Mutation::Overwrite { at, with }),
        (any::<Index>(), prop::collection::vec(any::<u8>(), 1..=32))
            .prop_map(|(at, bytes)| Mutation::Insert { at, bytes }),
        any::<Index>().prop_map(|at| Mutation::Truncate { at }),
        (any::<Index>(), 1u32..=4096)
            .prop_map(|(record, extra)| Mutation::InCapLength { record, extra }),
    ]
}

/// One original record after the mutation: its marker, its (possibly shifted) start, its length,
/// and whether the mutation changed or removed any of its bytes.
#[derive(Debug, Clone)]
struct Placed {
    marker: String,
    start: u64,
    len: u64,
    touched: bool,
    /// Removed entirely by a truncation.
    gone: bool,
}

/// The mutated segment and what the model knows about it.
#[derive(Debug)]
struct Case {
    bytes: Vec<u8>,
    records: Vec<Placed>,
    /// Garbage inserted between two records (or before the first), touching none.
    interior_garbage: bool,
    /// Garbage appended after the last record.
    tail_garbage_at: Option<u64>,
    /// The length a truncation left, if the mutation was one.
    truncated_to: Option<u64>,
}

fn build(specs: &[RecordSpec], mutation: &Mutation) -> Case {
    let mut bytes = Vec::new();
    let mut records = Vec::new();
    for (i, spec) in specs.iter().enumerate() {
        let mut trace_id = spec.trace_id;
        if let Some(at) = spec.magic_at {
            trace_id[at..at + 4].copy_from_slice(&frame::MAGIC);
        }
        let ctx = BatchContext {
            trace: TraceContext { trace_id, span_id: spec.span_id },
            provenance: Provenance::default(),
        };
        let marker = format!("r{i}");
        let record = raw_record(&batch(&marker), ctx);
        records.push(Placed {
            marker,
            start: bytes.len() as u64,
            len: record.len() as u64,
            touched: false,
            gone: false,
        });
        bytes.extend_from_slice(&record);
    }

    let touch = |records: &mut Vec<Placed>, from: u64, to: u64| {
        for r in records.iter_mut() {
            if r.start < to && from < r.start + r.len {
                r.touched = true;
            }
        }
    };
    let mut interior_garbage = false;
    let mut tail_garbage_at = None;
    let mut truncated_to = None;
    match mutation {
        Mutation::FlipBit { at, bit } => {
            let at = at.index(bytes.len());
            bytes[at] ^= 1 << bit;
            touch(&mut records, at as u64, at as u64 + 1);
        }
        Mutation::Overwrite { at, with } => {
            let at = at.index(bytes.len());
            let end = (at + with.len()).min(bytes.len());
            bytes[at..end].copy_from_slice(&with[..end - at]);
            touch(&mut records, at as u64, end as u64);
        }
        Mutation::Insert { at, bytes: inserted } => {
            let at = at.index(bytes.len() + 1);
            bytes.splice(at..at, inserted.iter().copied());
            let at = at as u64;
            let k = inserted.len() as u64;
            for r in records.iter_mut() {
                if r.start < at && at < r.start + r.len {
                    r.touched = true;
                } else if r.start >= at {
                    r.start += k;
                }
            }
            if at == bytes.len() as u64 - k {
                tail_garbage_at = Some(at);
            } else {
                interior_garbage = records.iter().any(|r| r.start == at + k);
            }
        }
        Mutation::Truncate { at } => {
            let at = at.index(bytes.len());
            bytes.truncate(at);
            let at = at as u64;
            for r in records.iter_mut() {
                if r.start >= at {
                    r.gone = true;
                } else if r.start + r.len > at {
                    r.touched = true;
                }
            }
            truncated_to = Some(at);
        }
        Mutation::InCapLength { record, extra } => {
            let r = &mut records[record.index(specs.len())];
            let body_start = r.start as usize + CONTEXT_LEN + frame::HEADER_LEN;
            let declared = (bytes.len() - body_start) as u32 + extra;
            let at = r.start as usize + COMPRESSED_LEN_AT;
            bytes[at..at + 4].copy_from_slice(&declared.to_le_bytes());
            r.touched = true;
        }
    }
    Case { bytes, records, interior_garbage, tail_garbage_at, truncated_to }
}

/// Runs `walk_segment` on its own thread so a walk that never terminates fails the case instead
/// of stalling the suite.
fn walk_with_timeout(bytes: Vec<u8>) -> (Vec<(u64, u64, String)>, WalkOutcome) {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut emitted = Vec::new();
        let outcome = walk_segment(&bytes, 0, |offset, _, batch, len| {
            emitted.push((offset, len, marker_of(&batch)));
        });
        let _ = tx.send((emitted, outcome));
    });
    rx.recv_timeout(WALK_TIMEOUT).expect("walk_segment must terminate")
}

/// What the model requires of any recovery over `case`, given the records that were recovered
/// (in order, as markers with offsets).
struct Expectation {
    /// Something unparseable sits before a recovered record, so it must be counted.
    corrupt_required: bool,
    /// Nothing is missing and there's no garbage, so nothing may be counted.
    corrupt_forbidden: bool,
    /// Where the confirmed-good bytes must end, when the model knows exactly.
    good_len: Option<u64>,
    /// Where the good bytes end if the tail reads as torn rather than as unrecoverable
    /// corruption (the start of the first unrecovered tail record).
    torn_tail_at: Option<u64>,
}

fn check_recovered(
    case: &Case,
    recovered: &[(u64, u64, String)],
) -> Result<Expectation, TestCaseError> {
    // Offsets strictly increase and never overlap.
    for pair in recovered.windows(2) {
        prop_assert!(pair[0].0 + pair[0].1 <= pair[1].0, "overlap or backwards: {recovered:?}");
    }
    // Every recovered record is an original one, whole. An untouched one is at its own (shifted)
    // offset. A touched one may not be: bytes inserted into its context move its frame, and the
    // resync reads the 24 bytes before the frame as its context.
    for (offset, len, marker) in recovered {
        let original = case.records.iter().find(|r| &r.marker == marker);
        prop_assert!(
            original.is_some_and(|r| r.len == *len && !r.gone && (r.start == *offset || r.touched)),
            "phantom or displaced record {marker} at {offset}+{len}: {:?}",
            case.records
        );
    }
    // Every untouched surviving record is recovered exactly once.
    for r in case.records.iter().filter(|r| !r.touched && !r.gone) {
        prop_assert!(
            recovered.iter().filter(|(_, _, m)| *m == r.marker).count() == 1,
            "untouched record {} at {} not recovered exactly once: {recovered:?}",
            r.marker,
            r.start
        );
    }

    let is_recovered = |r: &Placed| recovered.iter().any(|(_, _, m)| *m == r.marker);
    let last_recovered = case.records.iter().rposition(is_recovered);
    let missing: Vec<usize> = (0..case.records.len())
        .filter(|&i| !case.records[i].gone && !is_recovered(&case.records[i]))
        .collect();
    let interior_missing = missing.iter().any(|&i| last_recovered.is_some_and(|last| i < last));
    let tail_missing: Option<&Placed> = missing
        .iter()
        .map(|&i| &case.records[i])
        .find(|r| last_recovered.is_none_or(|last| r.start > case.records[last].start));

    let end_of_bytes = case.bytes.len() as u64;
    let good_len = if let Some(t) = case.truncated_to {
        Some(
            case.records
                .iter()
                .filter(|r| r.start + r.len <= t)
                .map(|r| r.start + r.len)
                .max()
                .unwrap_or(0),
        )
    } else if tail_missing.is_none() {
        Some(case.tail_garbage_at.unwrap_or(end_of_bytes))
    } else {
        None
    };
    // Recovering a record away from its own offset means the walk resynced to reach it.
    let displaced = recovered.iter().any(|(offset, _, marker)| {
        case.records.iter().any(|r| &r.marker == marker && r.start != *offset)
    });
    Ok(Expectation {
        corrupt_required: interior_missing || case.interior_garbage || displaced,
        corrupt_forbidden: missing.is_empty() && !case.interior_garbage && !displaced,
        good_len,
        torn_tail_at: tail_missing.map(|r| r.start),
    })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn walk_segment_recovers_every_record_outside_the_mutated_range(
        specs in prop::collection::vec(record_spec(), 1..=12),
        mutation in mutation(),
    ) {
        let case = build(&specs, &mutation);
        let (emitted, outcome) = walk_with_timeout(case.bytes.clone());
        let expect = check_recovered(&case, &emitted)?;

        prop_assert_eq!(outcome.valid_count, emitted.len() as u64);
        if expect.corrupt_required {
            prop_assert!(outcome.corrupt_skipped >= 1, "corruption before a record went uncounted");
        }
        if expect.corrupt_forbidden || case.truncated_to.is_some() {
            prop_assert_eq!(outcome.corrupt_skipped, 0, "nothing corrupt to count");
        }
        match (expect.good_len, expect.torn_tail_at) {
            (Some(good_len), _) => prop_assert_eq!(outcome.good_len, good_len),
            // A missing tail record reads either as a torn tail (the walk stops at its start) or
            // as unrecoverable corruption (skipped to the end and counted).
            (None, Some(torn_at)) => prop_assert!(
                outcome.good_len == torn_at
                    || (outcome.good_len == case.bytes.len() as u64 && outcome.corrupt_skipped >= 1),
                "good_len {} is neither the torn tail at {torn_at} nor the end", outcome.good_len
            ),
            (None, None) => unreachable!("good_len is known whenever no tail record is missing"),
        }
    }

    #[test]
    fn open_never_truncates_a_record_that_would_have_parsed(
        specs in prop::collection::vec(record_spec(), 1..=12),
        mutation in mutation(),
    ) {
        let case = build(&specs, &mutation);
        let dir = crate::disk_queue::test_support::scratch_dir("verify-open");
        std::fs::write(segment_path(&dir, 0), &case.bytes).unwrap();

        let result = open_and_drain(&dir);
        let on_disk = std::fs::metadata(segment_path(&dir, 0)).map(|m| m.len()).ok();
        let segments = list_segments(&dir).unwrap();
        std::fs::remove_dir_all(&dir).ok();
        let (delivered, truncated, corrupt) = result;
        prop_assert_eq!(segments, vec![0]);

        // Delivered records carry no offsets; place each at its model offset.
        let placed: Vec<(u64, u64, String)> = delivered
            .iter()
            .map(|m| {
                let r = case.records.iter().find(|r| &r.marker == m).expect("an original marker");
                (r.start, r.len, m.clone())
            })
            .collect();
        let expect = check_recovered(&case, &placed)?;

        if expect.corrupt_required {
            prop_assert!(corrupt >= 1.0, "corruption before a record went uncounted");
        }
        // Delivery carries no offsets, so a touched record the resync recovered away from its
        // own offset looks undisplaced here; its recovery may rightly have counted.
        let touched_delivered = delivered
            .iter()
            .any(|m| case.records.iter().any(|r| &r.marker == m && r.touched));
        if (expect.corrupt_forbidden && !touched_delivered) || case.truncated_to.is_some() {
            prop_assert_eq!(corrupt, 0.0);
        }
        let on_disk = on_disk.expect("the active segment survives open");
        match (expect.good_len, expect.torn_tail_at) {
            (Some(good_len), _) if good_len < case.bytes.len() as u64 => {
                prop_assert_eq!(truncated, 1.0);
                prop_assert_eq!(on_disk, good_len);
            }
            (Some(_), _) => {
                prop_assert_eq!(truncated, 0.0);
                prop_assert_eq!(on_disk, case.bytes.len() as u64);
            }
            (None, Some(torn_at)) => prop_assert!(
                (truncated == 1.0 && on_disk == torn_at)
                    || (truncated == 0.0 && on_disk == case.bytes.len() as u64 && corrupt >= 1.0),
                "tail: truncated {truncated}, {on_disk} bytes left, torn tail at {torn_at}"
            ),
            (None, None) => unreachable!("good_len is known whenever no tail record is missing"),
        }
    }
}

/// Opens the spool at `dir`, returns what `open` counted (`disk.truncated`, `disk_corrupt`
/// batches), then drains it with peek/commit and returns every delivered marker in order.
fn open_and_drain(dir: &Path) -> (Vec<String>, f64, f64) {
    let runtime = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("test", "output", "sink");
        let mut cfg = config(dir.to_path_buf());
        cfg.segment_bytes = 1024 * 1024;
        let q = DiskQueue::open(cfg, telemetry, Diagnostics::new("test")).unwrap();
        let events = registry.drain(0);
        let truncated = metric_sum(&events, "logit.component.buffer.disk.truncated", None);
        let corrupt =
            metric_sum(&events, SINK_QUEUE_METRICS.items_dropped, Some(("reason", "disk_corrupt")));

        // Close first: `peek` then returns `None` once the spool is drained, rather than waiting
        // for a push that never comes.
        q.close();
        let mut delivered = Vec::new();
        loop {
            let next = tokio::time::timeout(WALK_TIMEOUT, q.peek())
                .await
                .expect("peek must not stop responding");
            let Some((batch, _)) = next else { break };
            delivered.push(marker_of(&batch));
            q.commit().expect("commit what was just peeked");
        }
        (delivered, truncated, corrupt)
    })
}

// ---------------------------------------------------------------------------------------------
// A model of the spool's delivery contract under pushes, cancelled pushes, injected failures, and
// crash-reopens (DISK-03, DISK-05).
// ---------------------------------------------------------------------------------------------

const MODEL_TIMEOUT: Duration = Duration::from_secs(5);
/// How long a peek waits when the model has nothing that must be queued: long enough for a
/// replayed record to be read, short enough that an empty spool costs little.
const EMPTY_PEEK_WAIT: Duration = Duration::from_millis(20);

#[derive(Debug, Clone, Copy)]
enum Fault {
    FlushEnospc,
    SetLenEio,
    FsyncEio,
    UnlinkEio,
}

#[derive(Debug, Clone)]
enum SpoolOp {
    /// Pushes a batch whose marker is padded by this many bytes.
    Push(usize),
    Peek,
    Commit,
    /// Polls a push this many times, then drops it. The push runs on a second runtime whose one
    /// blocking thread first sleeps this many microseconds, so what the push handed off lands
    /// before, during, or after the next operation's own work.
    CancelPush(usize, u32, u64),
    /// Fails the next occurrence of one operation.
    Inject(Fault),
    /// Freezes the spool directory at the next occurrence of one of [`CRASH_POINTS`], until the
    /// next `Reopen`.
    Crash(Index),
    /// Drops the spool (after `finish` if `true`) and its runtime, as a process exit would, then
    /// reopens it.
    Reopen(bool),
    /// Peeks and commits until nothing is queued. Generated only by [`bounded_spool_op`], so a
    /// following push can find a bounded spool full with nothing queued.
    ConsumeAll,
}

const CRASH_POINTS: [Point; 9] = [
    Point::new(sites::SPOOL_SEGMENT, Op::Write),
    Point::new(sites::SPOOL_SEGMENT, Op::Flush),
    Point::new(sites::SPOOL_SEGMENT, Op::SetLen),
    Point::new(sites::SPOOL_SEGMENT, Op::SyncFile),
    Point::new(sites::SPOOL_SEGMENT, Op::Create),
    Point::new(sites::SPOOL_SEGMENT, Op::Unlink),
    Point::new(sites::SPOOL_CURSOR, Op::Write),
    Point::new(sites::SPOOL_CURSOR, Op::Rename),
    Point::new(sites::SPOOL_DIR, Op::SyncDir),
];

fn spool_op() -> impl Strategy<Value = SpoolOp> {
    let fault = prop_oneof![
        Just(Fault::FlushEnospc),
        Just(Fault::SetLenEio),
        Just(Fault::FsyncEio),
        Just(Fault::UnlinkEio),
    ];
    prop_oneof![
        6 => (0usize..300).prop_map(SpoolOp::Push),
        3 => Just(SpoolOp::Peek),
        4 => Just(SpoolOp::Commit),
        2 => (0usize..300, 1u32..=6, 0u64..2000)
            .prop_map(|(size, polls, delay)| SpoolOp::CancelPush(size, polls, delay)),
        1 => fault.prop_map(SpoolOp::Inject),
        1 => any::<Index>().prop_map(SpoolOp::Crash),
        1 => any::<bool>().prop_map(SpoolOp::Reopen),
    ]
}

/// [`spool_op`] plus [`SpoolOp::ConsumeAll`], for a spool whose `max_bytes` a few records fill.
fn bounded_spool_op() -> impl Strategy<Value = SpoolOp> {
    prop_oneof![6 => spool_op(), 1 => Just(SpoolOp::ConsumeAll)]
}

/// How long a push may take before the model treats it as parked on a full spool.
const PARK_WAIT: Duration = Duration::from_millis(50);

/// What the model knows about one push, by id (the push's position in the sequence).
#[derive(Debug, Clone, Copy, PartialEq)]
enum Pushed {
    /// Counted queued: it must be delivered.
    Queued,
    /// Counted dropped, or cancelled before it completed. Its bytes may have reached the disk
    /// whole, so it may be delivered, but only after a reopen.
    Unconfirmed,
}

struct Model {
    pushes: Vec<Pushed>,
    /// For each id, the reopen epoch it was first committed in.
    committed_in: Vec<Option<u32>>,
    /// The reopen epoch each id was pushed in.
    pushed_in: Vec<u32>,
    epoch: u32,
    /// The highest id committed for the first time so far: first deliveries are FIFO.
    last_first_commit: Option<usize>,
    /// The depth the queue must report, once a reopen has set a baseline.
    depth: f64,
}

impl Model {
    fn must_deliver(&self) -> bool {
        self.pushes.iter().zip(&self.committed_in).any(|(p, c)| *p == Pushed::Queued && c.is_none())
    }
}

fn marker(id: usize, pad: usize) -> String {
    format!("{id:05}-{}", "p".repeat(pad))
}

fn id_of(marker: &str) -> usize {
    marker[..5].parse().expect("a model marker")
}

fn drops(events: &[logit_core::Event]) -> f64 {
    ["disk_full", "disk_io_error", "frame_too_large"]
        .iter()
        .map(|r| metric_sum(events, SINK_QUEUE_METRICS.items_dropped, Some(("reason", r))))
        .sum()
}

/// The last `buffer.batches` gauge value in `events`, if the queue reported one.
fn depth_gauge(events: &[logit_core::Event]) -> Option<f64> {
    let name = logit_core::interner::intern(SINK_QUEUE_METRICS.depth);
    events.iter().flat_map(|e| e.metrics.iter()).filter(|m| m.name == name).last().and_then(|m| {
        match m.kind {
            logit_core::MetricKind::Gauge(v) => Some(v),
            _ => None,
        }
    })
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap()
}

/// A runtime with one blocking thread, which [`poll_then_cancel`] stalls.
fn side_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
}

/// Stalls `side`'s blocking thread for `delay`, then polls `push` on `side` at most `polls`
/// times and drops it. Returns whether it completed.
fn poll_then_cancel(
    side: &tokio::runtime::Runtime,
    push: impl std::future::Future<Output = ()>,
    polls: u32,
    delay: Duration,
) -> bool {
    side.spawn_blocking(move || std::thread::sleep(delay));
    let mut push = std::pin::pin!(push);
    for _ in 0..polls {
        let ready = side.block_on(std::future::poll_fn(|cx| {
            std::task::Poll::Ready(push.as_mut().poll(cx).is_ready())
        }));
        if ready {
            return true;
        }
        std::thread::sleep(Duration::from_micros(100));
    }
    false
}

/// Pushes `item` to completion, returning the ids a consumer committed meanwhile. A push that
/// hasn't finished within [`PARK_WAIT`] is parked on a full spool. With something queued (`depth`
/// above 0), a consumer then drains alongside it, and its commits must wake it. With nothing
/// queued, nothing will ever free room, so the push must finish on its own. Either way it must
/// finish within [`MODEL_TIMEOUT`].
fn push_waking_if_parked(
    rt: &tokio::runtime::Runtime,
    q: &DiskQueue,
    item: (std::sync::Arc<logit_core::EventBatch>, BatchContext),
    depth: f64,
) -> Result<Vec<usize>, TestCaseError> {
    let mut push = std::pin::pin!(q.push(item));
    if rt.block_on(async { tokio::time::timeout(PARK_WAIT, push.as_mut()).await }).is_ok() {
        return Ok(Vec::new());
    }
    if depth <= 0.0 {
        return rt
            .block_on(async { tokio::time::timeout(MODEL_TIMEOUT, push.as_mut()).await })
            .map(|()| Vec::new())
            .map_err(|_| TestCaseError::fail("a push parked on a full spool with nothing queued"));
    }
    rt.block_on(async {
        tokio::time::timeout(MODEL_TIMEOUT, async {
            let mut committed = Vec::new();
            loop {
                tokio::select! {
                    biased;
                    () = push.as_mut() => break,
                    peeked = q.peek() => {
                        if let Some((batch, _)) = peeked {
                            q.commit().expect("commit what was just peeked");
                            committed.push(id_of(&marker_of(&batch)));
                        }
                    }
                }
            }
            committed
        })
        .await
    })
    .map_err(|_| TestCaseError::fail("a parked push was never woken while the consumer drained"))
}

/// Records a commit of `id`, checking that first deliveries are FIFO and that a repeat, or a
/// push that was never counted queued, comes only after a reopen.
fn on_commit(m: &mut Model, id: usize) -> Result<(), TestCaseError> {
    prop_assert!(id < m.pushes.len(), "delivered an id never pushed: {id}");
    match m.committed_in[id] {
        Some(first) => {
            prop_assert!(m.epoch > first, "id {id} delivered twice with no reopen in between")
        }
        None => {
            if m.pushes[id] == Pushed::Unconfirmed {
                prop_assert!(
                    m.epoch > m.pushed_in[id],
                    "unconfirmed id {id} delivered without a reopen"
                );
            }
            if let Some(last) = m.last_first_commit {
                prop_assert!(id > last, "first delivery of {id} after {last}: not FIFO");
            }
            m.last_first_commit = Some(id);
            m.committed_in[id] = Some(m.epoch);
        }
    }
    m.depth -= 1.0;
    Ok(())
}

/// Runs `ops` against a `Block` spool bounded at `max_bytes`.
fn run_spool_model(
    segment_bytes: u64,
    max_bytes: u64,
    ops: &[SpoolOp],
) -> Result<(), TestCaseError> {
    let dir = crate::disk_queue::test_support::scratch_dir("spool-model");
    let mut cfg = config(dir.clone());
    cfg.segment_bytes = segment_bytes;
    cfg.max_bytes = max_bytes;
    cfg.checkpoint_interval = Duration::ZERO;
    let result = drive_spool_model(&dir, cfg, ops);
    std::fs::remove_dir_all(&dir).ok();
    result
}

fn drive_spool_model(
    dir: &Path,
    cfg: crate::disk_queue::DiskQueueConfig,
    ops: &[SpoolOp],
) -> Result<(), TestCaseError> {
    let registry = Registry::new();
    let open = |registry: &Registry| {
        DiskQueue::open(
            cfg.clone(),
            registry.telemetry_for("test", "output", "sink"),
            Diagnostics::new("test"),
        )
        .unwrap()
    };
    let mut scope = fault::scope(dir);
    let mut rt = runtime();
    let mut side = side_runtime();
    let mut q = open(&registry);
    registry.drain(0);
    let mut m = Model {
        pushes: Vec::new(),
        committed_in: Vec::new(),
        pushed_in: Vec::new(),
        epoch: 0,
        last_first_commit: None,
        depth: 0.0,
    };
    let mut corrupt = 0.0;

    for op in ops {
        match op {
            SpoolOp::Push(pad) | SpoolOp::CancelPush(pad, _, _) => {
                let id = m.pushes.len();
                let item = (batch(&marker(id, *pad)), ctx_for_model());
                let completed = match op {
                    SpoolOp::Push(_) => {
                        for id in push_waking_if_parked(&rt, &q, item, m.depth)? {
                            on_commit(&mut m, id)?;
                        }
                        true
                    }
                    SpoolOp::CancelPush(_, polls, delay) => {
                        poll_then_cancel(&side, q.push(item), *polls, Duration::from_micros(*delay))
                    }
                    _ => unreachable!(),
                };
                let events = registry.drain(0);
                corrupt += corrupt_count(&events);
                let dropped = drops(&events);
                prop_assert!(dropped <= 1.0, "one push counted {dropped} drops");
                let queued = completed && dropped == 0.0;
                // Exactly one of: counted queued (the depth gauge moves), or counted dropped.
                match depth_gauge(&events) {
                    Some(depth) if queued => prop_assert_eq!(depth, m.depth + 1.0),
                    Some(depth) => {
                        prop_assert_eq!(depth, m.depth, "a dropped push moved the depth")
                    }
                    None => prop_assert!(!queued, "a queued push didn't report its depth"),
                }
                if queued {
                    m.depth += 1.0;
                }
                m.pushes.push(if queued { Pushed::Queued } else { Pushed::Unconfirmed });
                m.committed_in.push(None);
                m.pushed_in.push(m.epoch);
            }
            SpoolOp::Peek => {
                let wait = if m.must_deliver() { MODEL_TIMEOUT } else { EMPTY_PEEK_WAIT };
                let peeked = rt.block_on(async { tokio::time::timeout(wait, q.peek()).await });
                prop_assert!(
                    peeked.is_ok() || !m.must_deliver(),
                    "peek stopped responding with a queued record undelivered"
                );
                corrupt += corrupt_count(&registry.drain(0));
            }
            SpoolOp::Commit => {
                if let Some((batch, _)) = q.commit() {
                    on_commit(&mut m, id_of(&marker_of(&batch)))?;
                }
                let events = registry.drain(0);
                corrupt += corrupt_count(&events);
                if let Some(depth) = depth_gauge(&events) {
                    prop_assert_eq!(depth, m.depth);
                }
            }
            SpoolOp::ConsumeAll => {
                loop {
                    let wait = if m.depth > 0.0 { MODEL_TIMEOUT } else { EMPTY_PEEK_WAIT };
                    let peeked = rt.block_on(async { tokio::time::timeout(wait, q.peek()).await });
                    let Ok(Some((batch, _))) = peeked else {
                        prop_assert!(
                            !m.must_deliver(),
                            "peek stopped responding with a queued record undelivered"
                        );
                        break;
                    };
                    q.commit().expect("commit what was just peeked");
                    on_commit(&mut m, id_of(&marker_of(&batch)))?;
                }
                let events = registry.drain(0);
                corrupt += corrupt_count(&events);
                if let Some(depth) = depth_gauge(&events) {
                    prop_assert_eq!(depth, m.depth);
                }
            }
            SpoolOp::Inject(fault) => {
                let (op, errno) = match fault {
                    Fault::FlushEnospc => (Op::Flush, errno::ENOSPC),
                    Fault::SetLenEio => (Op::SetLen, errno::EIO),
                    Fault::FsyncEio => (Op::SyncFile, errno::EIO),
                    Fault::UnlinkEio => (Op::Unlink, errno::EIO),
                };
                scope.fail_nth(Point::new(sites::SPOOL_SEGMENT, op), 1, errno);
            }
            SpoolOp::Crash(at) => {
                scope.crash_at(CRASH_POINTS[at.index(CRASH_POINTS.len())], 1);
            }
            SpoolOp::Reopen(finish) => {
                if *finish {
                    rt.block_on(q.finish());
                }
                drop(q);
                // Dropping the runtime waits for every blocking write already handed off, as
                // those land before a process's exit completes.
                drop(rt);
                drop(side);
                // A fresh scope: a reopen starts with no rule left armed and nothing frozen.
                drop(scope);
                scope = fault::scope(dir);
                rt = runtime();
                side = side_runtime();
                q = open(&registry);
                m.epoch += 1;
                let events = registry.drain(0);
                corrupt += corrupt_count(&events);
                let depth = depth_gauge(&events).expect("open reports the depth");
                let live = m
                    .pushes
                    .iter()
                    .zip(&m.committed_in)
                    .filter(|(p, c)| **p == Pushed::Queued && c.is_none())
                    .count() as f64;
                prop_assert!(depth >= live, "reopened with {depth} queued, {live} undelivered");
                m.depth = depth;
            }
        }
    }

    // Drain: every queued push must come out, and nothing may stall.
    scope.revive();
    q.close();
    loop {
        let peeked = rt
            .block_on(async { tokio::time::timeout(MODEL_TIMEOUT, q.peek()).await })
            .map_err(|_| TestCaseError::fail("peek stopped responding while draining"))?;
        if peeked.is_none() {
            break;
        }
        let (batch, _) = q.commit().expect("commit what was just peeked");
        on_commit(&mut m, id_of(&marker_of(&batch)))?;
    }
    corrupt += corrupt_count(&registry.drain(0));
    for (id, pushed) in m.pushes.iter().enumerate() {
        if *pushed == Pushed::Queued {
            prop_assert!(m.committed_in[id].is_some(), "queued id {id} was never delivered");
        }
    }
    prop_assert_eq!(m.depth, 0.0, "the drained queue's depth");
    prop_assert_eq!(corrupt, 0.0, "no corruption was injected, so none may be found");
    drop(scope);
    Ok(())
}

fn ctx_for_model() -> BatchContext {
    BatchContext { trace: TraceContext::new_root(), provenance: Provenance::default() }
}

fn corrupt_count(events: &[logit_core::Event]) -> f64 {
    metric_sum(events, SINK_QUEUE_METRICS.items_dropped, Some(("reason", "disk_corrupt")))
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Every push is delivered, counted dropped, or still queued; duplicates, and a push that
    /// wasn't counted queued, appear only after a reopen; first deliveries are FIFO; no peek
    /// stalls; and the depth gauge matches the model.
    #[test]
    fn spool_model_every_push_is_delivered_dropped_or_queued(
        segment_bytes in prop_oneof![Just(1u64), Just(400), Just(1 << 20)],
        ops in prop::collection::vec(spool_op(), 1..=40),
    ) {
        // Never full, so no push parks.
        run_spool_model(segment_bytes, 1 << 30, &ops)?;
    }

    /// The same contract under `overflow: block` with a `max_bytes` a few records fill, plus: a
    /// parked push is woken by the consumer's commits, and a push to a full spool with nothing
    /// queued never waits at all, even when `segment_bytes` is at or past `max_bytes`, so the
    /// active segment alone reads as full.
    #[test]
    fn spool_model_a_bounded_block_spool_never_parks_a_push_that_nothing_will_wake(
        segment_bytes in prop_oneof![Just(1u64), Just(400), Just(1 << 20)],
        max_bytes in prop_oneof![Just(600u64), Just(1200)],
        ops in prop::collection::vec(bounded_spool_op(), 1..=40),
    ) {
        run_spool_model(segment_bytes, max_bytes, &ops)?;
    }
}
