//! Property tests for the disk spool's recovery walk (DISK-01, DISK-02 in
//! `docs/plans/critical-sections-inventory.md`). Each case writes real records into one segment,
//! applies exactly one mutation, and checks what the walk and `DiskQueue::open` recover against a
//! model that knows which records the mutation touched.
//!
//! The generated records never embed a whole frame in a payload, so a record can only parse at
//! an offset where the model placed one. A payload that did embed one could still be read as a
//! phantom record after corruption; that's an accepted limit of a MAGIC-scan resync.

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
