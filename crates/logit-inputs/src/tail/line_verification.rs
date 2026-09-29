//! Property tests for [`LineSplitter`] against a reference written independently of it, for
//! TAIL-04 of `docs/plans/critical-sections-inventory.md`.
//!
//! [`Model`] restates the splitter one byte at a time: split on `\n`, strip one trailing `\r`,
//! drop a line whose pre-strip length is over `max_line_bytes`, counted once, when its length
//! first passes the limit. [`Harness`] feeds the same random byte stream to both, cut at random
//! points (empty chunks included, a `\r` and its `\n` free to land in different chunks), with
//! `take_partial` interleaved, and after every op checks:
//!
//! - the emitted lines, their `Some(i)`/`None` starts, and the per-push drop count;
//! - `pending_bytes` against the model's held line, dropping or not;
//! - each line's file offset, computed the way `Tailer::read_one` computes it, against the input
//!   bytes, and that an in-chunk line is a zero-copy slice of its chunk;
//! - that `offset - pending_bytes`, what `Tailer::write_checkpoint` persists, is a line start.
//!
//! At the end the whole input is re-split independently and must account for every line emitted,
//! taken, or dropped.
//!
//! Case counts are floors: a `PROPTEST_CASES` above one raises it for a deeper run.

use super::line::LineSplitter;
use bytes::Bytes;
use proptest::prelude::*;

/// `cases`, or `PROPTEST_CASES` when that is larger.
fn config(cases: u32) -> ProptestConfig {
    let default = ProptestConfig::default();
    ProptestConfig { cases: cases.max(default.cases), ..default }
}

// -- Strategies -------------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum Op {
    Push(Vec<u8>),
    TakePartial,
}

fn byte() -> impl Strategy<Value = u8> {
    prop_oneof![5 => Just(b'a'), 2 => Just(b'\r'), 2 => Just(b'\n'), 1 => any::<u8>()]
}

/// A byte for a long run: no `\n` arm, so a run can outgrow a 64-byte limit.
fn run_byte() -> impl Strategy<Value = u8> {
    prop_oneof![5 => Just(b'a'), 2 => Just(b'\r'), 1 => any::<u8>()]
}

fn segment() -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![
        prop::collection::vec(byte(), 0..=20),
        (prop::collection::vec(run_byte(), 0..=150), any::<bool>()).prop_map(|(mut run, nl)| {
            if nl {
                run.push(b'\n');
            }
            run
        }),
    ]
}

/// The stream is generated first and then cut, so chunk edges fall anywhere in a line.
fn ops() -> impl Strategy<Value = Vec<Op>> {
    (
        prop::collection::vec(segment(), 0..=12),
        prop::collection::vec(0usize..=16, 1..=64),
        prop::collection::vec(prop::bool::weighted(0.1), 1..=64),
    )
        .prop_map(|(segments, cuts, takes)| {
            let stream = segments.concat();
            let mut ops = Vec::new();
            let mut at = 0;
            let mut i = 0;
            let mut stalled = 0;
            while at < stream.len() {
                let mut len = cuts[i % cuts.len()].min(stream.len() - at);
                // All-zero cuts would never finish; after a full cycle of them, force progress.
                stalled = if len == 0 { stalled + 1 } else { 0 };
                if stalled > cuts.len() {
                    len = 1;
                    stalled = 0;
                }
                ops.push(Op::Push(stream[at..at + len].to_vec()));
                if takes[i % takes.len()] {
                    ops.push(Op::TakePartial);
                }
                at += len;
                i += 1;
            }
            ops
        })
}

// -- The reference model ----------------------------------------------------------------------

/// One trailing `\r` removed, and whether it was.
fn strip_one_cr(line: &[u8]) -> (&[u8], bool) {
    match line.split_last() {
        Some((b'\r', rest)) => (rest, true),
        _ => (line, false),
    }
}

/// A line the model emits: its bytes, where it starts in the chunk (`None`: in an earlier one),
/// and whether a `\r` was stripped from it.
type ModelLine = (Vec<u8>, Option<usize>, bool);

struct Model {
    max: usize,
    /// The current unterminated line since the last `\n` or `take_partial`, at full length and
    /// with no `\r` stripped, whether or not it is being dropped.
    cur: Vec<u8>,
    /// `cur` has passed `max` and been counted.
    counted: bool,
}

impl Model {
    fn push(&mut self, chunk: &[u8]) -> (Vec<ModelLine>, u32) {
        let mut line_start = if self.cur.is_empty() { Some(0) } else { None };
        let mut emits = Vec::new();
        let mut dropped = 0;
        for (j, &b) in chunk.iter().enumerate() {
            if b == b'\n' {
                if self.cur.len() > self.max {
                    if !self.counted {
                        dropped += 1;
                    }
                } else {
                    let (line, stripped) = strip_one_cr(&self.cur);
                    emits.push((line.to_vec(), line_start, stripped));
                }
                self.cur.clear();
                self.counted = false;
                line_start = Some(j + 1);
            } else {
                self.cur.push(b);
                if self.cur.len() > self.max && !self.counted {
                    dropped += 1;
                    self.counted = true;
                }
            }
        }
        (emits, dropped)
    }

    /// A line being dropped is left in place, still held and still counted; anything else is
    /// taken and the next byte starts a fresh line.
    fn take_partial(&mut self) -> Option<Vec<u8>> {
        if self.counted {
            return None;
        }
        let taken = (!self.cur.is_empty()).then(|| strip_one_cr(&self.cur).0.to_vec());
        self.cur.clear();
        taken
    }
}

// -- The harness ------------------------------------------------------------------------------

struct Harness {
    real: LineSplitter,
    model: Model,
    /// Bytes pushed so far: `Tailer`'s `offset`.
    offset: u64,
    input: Vec<u8>,
    /// Offsets where a `take_partial` started a fresh line.
    resets: Vec<u64>,
    last_abs: Option<u64>,
    /// Every line emitted or taken, in order, and every drop counted.
    lines_out: Vec<Vec<u8>>,
    dropped_out: u64,
}

impl Harness {
    fn new(max: usize) -> Self {
        Self {
            real: LineSplitter::new(max),
            model: Model { max, cur: Vec::new(), counted: false },
            offset: 0,
            input: Vec::new(),
            resets: Vec::new(),
            last_abs: None,
            lines_out: Vec::new(),
            dropped_out: 0,
        }
    }

    fn is_line_start(&self, pos: u64) -> bool {
        pos == 0 || self.input[pos as usize - 1] == b'\n' || self.resets.contains(&pos)
    }

    fn check_held(&self) {
        let pending = self.real.pending_bytes();
        assert_eq!(pending, self.model.cur.len() as u64, "pending_bytes vs the model's line");
        assert!(
            self.model.counted || pending <= self.model.max as u64,
            "a held partial of {pending} bytes is over the limit {}",
            self.model.max
        );
    }

    fn push(&mut self, bytes: &[u8]) {
        let chunk = Bytes::copy_from_slice(bytes);
        let base = chunk.as_ptr();
        let chunk_start = self.offset;
        let partial_start = chunk_start - self.real.pending_bytes();
        self.input.extend_from_slice(bytes);

        let mut got: Vec<(Bytes, Option<usize>)> = Vec::new();
        let stats = self.real.push(chunk.clone(), |line, at| got.push((line, at)));
        let (want, want_dropped) = self.model.push(bytes);

        let got_pairs: Vec<(Vec<u8>, Option<usize>)> =
            got.iter().map(|(line, at)| (line.to_vec(), *at)).collect();
        let want_pairs: Vec<(Vec<u8>, Option<usize>)> =
            want.iter().map(|(line, at, _)| (line.clone(), *at)).collect();
        assert_eq!(got_pairs, want_pairs, "emitted lines and starts for chunk {bytes:?}");
        assert_eq!(stats.dropped_lines, want_dropped, "dropped_lines for chunk {bytes:?}");

        for ((line, at), (_, _, stripped)) in got.iter().zip(&want) {
            let abs = at.map_or(partial_start, |i| chunk_start + i as u64);
            let (from, to) = (abs as usize, abs as usize + line.len());
            assert_eq!(&self.input[from..to], &line[..], "line bytes at offset {abs}");
            let terminator: &[u8] = if *stripped { b"\r\n" } else { b"\n" };
            assert_eq!(
                &self.input[to..to + terminator.len()],
                terminator,
                "the line at {abs} ends at its terminator"
            );
            assert!(self.is_line_start(abs), "a line starts at {abs}, not a line boundary");
            assert!(self.last_abs.is_none_or(|last| abs > last), "line starts go backwards");
            self.last_abs = Some(abs);
            if let Some(i) = at {
                if !line.is_empty() {
                    let expected = base.wrapping_add(*i);
                    assert_eq!(
                        line.as_ptr(),
                        expected,
                        "an in-chunk line is not a zero-copy slice"
                    );
                }
            }
            self.lines_out.push(line.to_vec());
        }
        self.dropped_out += u64::from(stats.dropped_lines);
        self.offset += bytes.len() as u64;

        self.check_held();
        let checkpoint = self.offset - self.real.pending_bytes();
        assert!(self.is_line_start(checkpoint), "the checkpoint {checkpoint} is inside a line");
    }

    fn take_partial(&mut self) {
        let pending_before = self.real.pending_bytes();
        let dropping = self.model.counted;
        let got = self.real.take_partial().map(|b| b.to_vec());
        let want = self.model.take_partial();
        assert_eq!(got, want, "take_partial");
        if let Some(line) = got {
            self.lines_out.push(line);
        }
        if dropping {
            assert_eq!(self.real.pending_bytes(), pending_before, "a drop survives take_partial");
        } else {
            self.resets.push(self.offset);
            assert_eq!(self.real.pending_bytes(), 0, "take_partial leaves nothing pending");
        }
        self.check_held();
        let checkpoint = self.offset - self.real.pending_bytes();
        assert!(self.is_line_start(checkpoint), "the checkpoint {checkpoint} is inside a line");
    }

    /// Re-splits the whole input, independently of the step-wise model, and checks every line
    /// was emitted, taken, or dropped, and the trailing piece is what is still held.
    fn reconstruct(&self) {
        let max = self.model.max;
        let mut lines = Vec::new();
        let mut dropped = 0u64;
        let mut bounds = self.resets.clone();
        bounds.dedup();
        let mut from = 0usize;
        let mut segments = Vec::new();
        for &reset in &bounds {
            segments.push((&self.input[from..reset as usize], true));
            from = reset as usize;
        }
        segments.push((&self.input[from..], false));
        let mut trailing: &[u8] = &[];
        for (segment, taken) in segments {
            let mut pieces: Vec<&[u8]> = segment.split(|&b| b == b'\n').collect();
            let last = pieces.pop().unwrap_or(&[]);
            for piece in pieces {
                if piece.len() > max {
                    dropped += 1;
                } else {
                    lines.push(strip_one_cr(piece).0.to_vec());
                }
            }
            if !taken {
                trailing = last;
            } else {
                // A drop survives `take_partial`, so a reset never ends a line over the limit.
                assert!(last.len() <= max, "a reset split an oversized line");
                if !last.is_empty() {
                    lines.push(strip_one_cr(last).0.to_vec());
                }
            }
        }
        if trailing.len() > max {
            dropped += 1;
        }
        assert_eq!(self.lines_out, lines, "every line emitted or taken, in order");
        assert_eq!(self.dropped_out, dropped, "every oversized line counted once");
        assert_eq!(trailing, &self.model.cur[..], "the trailing piece is what the model holds");
        assert_eq!(self.real.pending_bytes(), trailing.len() as u64);
    }
}

proptest! {
    #![proptest_config(config(256))]

    #[test]
    fn line_splitter_matches_the_reference_model(max in 0usize..=64, ops in ops()) {
        let mut harness = Harness::new(max);
        for op in &ops {
            match op {
                Op::Push(chunk) => harness.push(chunk),
                Op::TakePartial => harness.take_partial(),
            }
        }
        harness.reconstruct();
    }
}
