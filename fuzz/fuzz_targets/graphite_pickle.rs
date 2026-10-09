//! One unframed carbon pickle payload through `PickleReader::read_datapoints` and through
//! `GraphiteDecoder::decode_into` in pickle mode, as `graphite_in` hands over a frame with its
//! length prefix stripped. Byte 0's low bit chooses the mode: `0` takes the rest of the input as
//! the payload, `1` builds a payload from it (`build`), so the reader's verdict is known ahead.
//!
//! Oracles, each over every input:
//! - the frame-fatal and datapoint-skip dichotomy (`read_datapoints`'s doc): the reader and the
//!   decoder fail on the same payloads with the same error, and a failed frame pushes nothing.
//!   On a payload that reads, each datapoint the reader yields is the event
//!   `shared::graphite`'s `expected_event` builds from it, or no event when the doc's decode table
//!   skips it (a non-finite value; a timestamp that is neither `-1` nor positive and finite; a
//!   malformed tag; an empty path), and costs nothing else;
//! - built payloads: the reader yields every well-shaped item in list order and counts every
//!   wrong-shaped one, and fails the frame only where `append_range`'s tail rule says so: a
//!   non-empty list item inside an outer list grown by `APPEND`/`APPENDS`;
//! - shape and zero-copy: every event has one finite `Gauge` and `Str` tags that slice the
//!   payload (`shared::graphite`'s `check_event`);
//! - the second-generation fixed point in both protocols (`shared::graphite`'s
//!   `assert_second_generation_fixed_point`).
#![no_main]

#[path = "shared/graphite.rs"]
mod graphite;

use bytes::Bytes;
use graphite::{decoder, marker, RECEIVED_AT};
use libfuzzer_sys::fuzz_target;
use logit_proto::graphite::pickle::PickleReader;
use logit_proto::graphite::Protocol;
use logit_proto::Decoder;

/// `(path, timestamp, value)` as the reader yields it.
type Point = (String, f64, f64);

/// What the reader must make of a built payload.
enum Verdict {
    Reads { points: Vec<Point>, skipped: usize },
    Fails,
}

/// How the built payload's outer list is assembled, each the shape of a real producer:
/// CPython's `EMPTY_LIST MARK … APPENDS`, og-rek's `MARK … LIST` (carbon-relay-ng), and one
/// `APPEND` per item (CPython's one-element list, and Dropwizard's `PickledGraphite` in binary).
#[derive(Clone, Copy, PartialEq)]
enum Outer {
    Appends,
    List,
    AppendEach,
}

/// Items a built payload holds at most: past a few hundred, more items reach no new reader state
/// (the memo moves to `LONG_BINPUT` at key 256) and only slow each run.
const MAX_BUILT_ITEMS: usize = 512;

struct Builder {
    out: Vec<u8>,
    memo: usize,
    /// Memo keys of the well-shaped items written so far, and what each one reads as.
    good: Vec<(usize, Point)>,
}

impl Builder {
    fn op(&mut self, op: u8, operand: &[u8]) {
        self.out.push(op);
        self.out.extend_from_slice(operand);
    }

    fn binunicode(&mut self, s: &str) {
        self.op(0x58, &(s.len() as u32).to_le_bytes());
        self.out.extend_from_slice(s.as_bytes());
    }

    fn short_binunicode(&mut self, s: &str) {
        self.op(0x8c, &[s.len() as u8]);
        self.out.extend_from_slice(s.as_bytes());
    }

    /// `BINPUT`, or `LONG_BINPUT` past key 255, on the next ordinal key.
    fn memoize(&mut self, point: Point) {
        let key = self.memo;
        match u8::try_from(key) {
            Ok(key) => self.op(0x71, &[key]),
            Err(_) => self.op(0x72, &(key as u32).to_le_bytes()),
        }
        self.memo += 1;
        self.good.push((key, point));
    }
}

/// A protocol-2 payload built from `spec`: its first byte picks [`Outer`], and each of the next
/// [`MAX_BUILT_ITEMS`] bytes one list item, with what the reader must make of the whole.
fn build(spec: &[u8]) -> (Vec<u8>, Verdict) {
    let Some((&outer, items)) = spec.split_first() else { return (Vec::new(), Verdict::Fails) };
    let outer = [Outer::Appends, Outer::List, Outer::AppendEach][outer as usize % 3];
    let mut b = Builder { out: vec![0x80, 2], memo: 0, good: Vec::new() };
    let mut points = Vec::new();
    let mut skipped = 0;
    let mut fails = false;
    match outer {
        Outer::Appends => b.op(0x5d, &[0x28]),
        Outer::List => b.op(0x28, &[]),
        Outer::AppendEach => b.op(0x5d, &[]),
    }
    for &item in items.iter().take(MAX_BUILT_ITEMS) {
        let n = u32::from(item / 7);
        match item % 7 {
            // `(path, (timestamp, value))` through `TUPLE2`, as CPython writes it.
            0 => {
                let point = (format!("p.{n}"), f64::from(1_700_000_000 + n), f64::from(n) + 0.5);
                b.binunicode(&point.0);
                b.op(0x4a, &(1_700_000_000 + n as i32).to_le_bytes());
                b.op(0x47, &point.2.to_be_bytes());
                b.op(0x86, &[0x86]);
                points.push(point.clone());
                b.memoize(point);
            }
            // The same through `MARK … TUPLE`, a `LONG1` second past 2038, and a numeric-string
            // value, which carbon coerces with `float()`.
            1 => {
                let seconds = (1u64 << 31) + u64::from(n);
                let value = format!("{n}.25");
                let point = (format!("q.{n};k={n}"), seconds as f64, value.parse().unwrap());
                b.op(0x28, &[]);
                b.short_binunicode(&point.0);
                b.op(0x28, &[0x8a, 5]);
                b.out.extend_from_slice(&seconds.to_le_bytes()[..5]);
                b.short_binunicode(&value);
                b.op(0x74, &[0x74]);
                points.push(point.clone());
                b.memoize(point);
            }
            // A repeat of an earlier well-shaped item, through the memo.
            2 if !b.good.is_empty() => {
                let (key, point) = b.good[n as usize % b.good.len()].clone();
                match u8::try_from(key) {
                    Ok(key) => b.op(0x68, &[key]),
                    Err(_) => b.op(0x6a, &(key as u32).to_le_bytes()),
                }
                points.push(point);
            }
            // A stray `None`.
            2 | 3 => {
                b.op(0x4e, &[]);
                skipped += 1;
            }
            // `(path, (timestamp,))`: the inner tuple has one element.
            4 => {
                b.binunicode("r");
                b.op(0x4b, &[n as u8]);
                b.op(0x85, &[0x86]);
                skipped += 1;
            }
            // An empty list: a wrong shape that grows no arena.
            5 => {
                b.op(0x5d, &[]);
                skipped += 1;
            }
            // `[timestamp, value]`: a non-empty list grows the list arena past the outer list's
            // tail, so an outer list still to be appended to can't be extended.
            _ => {
                b.op(0x5d, &[0x28, 0x4b, n as u8, 0x4b, 1, 0x65]);
                skipped += 1;
                fails |= outer != Outer::List;
            }
        }
        if outer == Outer::AppendEach {
            b.op(0x61, &[]);
        }
    }
    match outer {
        Outer::Appends => b.op(0x65, &[]),
        Outer::List => b.op(0x6c, &[]),
        Outer::AppendEach => {}
    }
    b.op(0x2e, &[]);
    let verdict = if fails { Verdict::Fails } else { Verdict::Reads { points, skipped } };
    (b.out, verdict)
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, rest)) = data.split_first() else { return };
    let (payload, built) = if selector & 1 == 0 {
        (rest.to_vec(), None)
    } else {
        let (payload, verdict) = build(rest);
        (payload, Some(verdict))
    };
    let payload = Bytes::from(payload);

    let mut points: Vec<Point> = Vec::new();
    let read = PickleReader::new().read_datapoints(&payload, |path, timestamp, value| {
        points.push((path.to_string(), timestamp, value))
    });
    match (&built, &read) {
        (None, _) | (Some(Verdict::Fails), Err(_)) => {}
        (Some(Verdict::Reads { points: want, skipped }), Ok(got)) => {
            assert_eq!(got, skipped, "built: the wrong-shaped items are the ones counted");
            assert_eq!(points.len(), want.len(), "built: every well-shaped item reads");
            for (got, want) in points.iter().zip(want) {
                assert_eq!(got.0, want.0, "built: a path changed");
                assert_eq!(got.1.to_bits(), want.1.to_bits(), "built: a timestamp changed");
                assert_eq!(got.2.to_bits(), want.2.to_bits(), "built: a value changed");
            }
        }
        (Some(Verdict::Fails), Ok(_)) => panic!("built: a frame the tail rule fails read"),
        (Some(Verdict::Reads { .. }), Err(err)) => panic!("built: a readable frame failed: {err}"),
    }

    let mut out = vec![marker()];
    let decoded = decoder(Protocol::Pickle).decode_into(payload.clone(), RECEIVED_AT, &mut out);
    match (&read, &decoded) {
        (Err(read), Err(decoded)) => {
            assert_eq!(read.to_string(), decoded.to_string(), "dichotomy: different errors");
            assert_eq!(out, [marker()], "dichotomy: a failed frame pushes nothing");
            return;
        }
        (Ok(_), Ok(_)) => {}
        (read, decoded) => panic!("dichotomy: the reader gave {read:?}, the decoder {decoded:?}"),
    }
    assert_eq!(out[0], marker(), "decode_into appends after out's contents");

    let mut events = out[1..].iter();
    for (path, timestamp, value) in &points {
        if let Some(expected) = graphite::expected_event(path, *value, *timestamp) {
            let event = events.next().expect("dichotomy: a datapoint the doc keeps is missing");
            graphite::assert_matches(event, &expected, "dichotomy");
        }
    }
    assert!(events.next().is_none(), "dichotomy: an event no datapoint accounts for");

    for event in &out[1..] {
        graphite::check_event(&payload, event);
    }
    graphite::assert_second_generation_fixed_point(out.split_off(1));
});
