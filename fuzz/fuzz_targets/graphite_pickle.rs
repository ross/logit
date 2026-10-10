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
//! - built payloads, in protocol 2's binary spelling or protocol 0's text one: the reader yields
//!   every well-shaped item in list order and counts every wrong-shaped one, and fails the frame
//!   only where `append_range`'s tail rule says so: a non-empty list item inside an outer list
//!   grown by `APPEND`/`APPENDS`. A built path the builder escaped is decoded, and one it didn't
//!   borrows the payload;
//! - shape and zero-copy: every event has one finite `Gauge` and `Str` tags, which slice the
//!   payload whenever the reader yielded its path from the payload (`shared::graphite`'s
//!   `check_event`);
//! - the second-generation fixed point in both protocols (`shared::graphite`'s
//!   `assert_second_generation_fixed_point`).
#![no_main]

#[path = "shared/graphite.rs"]
mod graphite;

use bytes::Bytes;
use graphite::{decoder, marker, RECEIVED_AT};
use libfuzzer_sys::fuzz_target;
use logit_core::subslice;
use logit_proto::graphite::pickle::PickleReader;
use logit_proto::graphite::Protocol;
use logit_proto::Decoder;

/// `(path, timestamp, value)` as the reader yields it.
type Point = (String, f64, f64);

/// What the reader must make of a built payload. `copied[i]` is whether the builder escaped
/// `points[i]`'s path, so the reader decodes it rather than borrowing the payload.
enum Verdict {
    Reads { points: Vec<Point>, copied: Vec<bool>, skipped: usize },
    Fails,
}

/// How the built payload's outer list is assembled, each the shape of a real producer:
/// CPython's `EMPTY_LIST MARK … APPENDS`, og-rek's `MARK … LIST` (carbon-relay-ng), and one
/// `APPEND` per item (CPython's one-element list, Python 2's `cPickle`, and Dropwizard's
/// `PickledGraphite`, both in protocol 0).
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
    /// Protocol 0's text spelling rather than protocol 2's binary one.
    text: bool,
    memo: usize,
    /// Memo keys of the well-shaped items written so far, what each one reads as, and whether
    /// its path was escaped.
    good: Vec<(usize, Point, bool)>,
}

impl Builder {
    fn op(&mut self, op: u8, operand: &[u8]) {
        self.out.push(op);
        self.out.extend_from_slice(operand);
    }

    /// A protocol-0 opcode and its `\n`-terminated argument.
    fn line(&mut self, op: u8, arg: &[u8]) {
        self.op(op, arg);
        self.out.push(b'\n');
    }

    fn binunicode(&mut self, s: &str) {
        self.op(0x58, &(s.len() as u32).to_le_bytes());
        self.out.extend_from_slice(s.as_bytes());
    }

    fn short_binunicode(&mut self, s: &str) {
        self.op(0x8c, &[s.len() as u8]);
        self.out.extend_from_slice(s.as_bytes());
    }

    /// `PUT`, `BINPUT`, or `LONG_BINPUT` past key 255, on the next ordinal key.
    fn memoize(&mut self, point: Point, copied: bool) {
        let key = self.memo;
        if self.text {
            self.line(b'p', key.to_string().as_bytes());
        } else {
            match u8::try_from(key) {
                Ok(key) => self.op(0x71, &[key]),
                Err(_) => self.op(0x72, &(key as u32).to_le_bytes()),
            }
        }
        self.memo += 1;
        self.good.push((key, point, copied));
    }

    /// `GET`, `BINGET`, or `LONG_BINGET`.
    fn memo_get(&mut self, key: usize) {
        if self.text {
            self.line(b'g', key.to_string().as_bytes());
        } else {
            match u8::try_from(key) {
                Ok(key) => self.op(0x68, &[key]),
                Err(_) => self.op(0x6a, &(key as u32).to_le_bytes()),
            }
        }
    }
}

/// A payload built from `spec`, with what the reader must make of the whole. The first byte's
/// low bits pick [`Outer`], bit 6 numbers the memo from 1 as Python 2's `cPickle` does, and bit 7
/// spells the payload in protocol 0's text opcodes; each of the next [`MAX_BUILT_ITEMS`] bytes is
/// one list item.
fn build(spec: &[u8]) -> (Vec<u8>, Verdict) {
    let Some((&shape, items)) = spec.split_first() else { return (Vec::new(), Verdict::Fails) };
    let outer = [Outer::Appends, Outer::List, Outer::AppendEach][(shape & 0x3f) as usize % 3];
    let text = shape & 0x80 != 0;
    let memo = usize::from(shape & 0x40 != 0);
    let header = if text { Vec::new() } else { vec![0x80, 2] };
    let mut b = Builder { out: header, text, memo, good: Vec::new() };
    let mut points = Vec::new();
    let mut copied = Vec::new();
    let mut skipped = 0;
    let mut fails = false;
    // `EMPTY_LIST` is `MARK LIST` in text.
    let empty_list: &[u8] = if text { b"(l" } else { &[0x5d] };
    match outer {
        Outer::Appends => {
            b.op(empty_list[0], &empty_list[1..]);
            b.op(0x28, &[]);
        }
        Outer::List => b.op(0x28, &[]),
        Outer::AppendEach => b.op(empty_list[0], &empty_list[1..]),
    }
    for &item in items.iter().take(MAX_BUILT_ITEMS) {
        let n = u32::from(item / 7);
        match item % 7 {
            // `(path, (timestamp, value))`: binary through `TUPLE2`, as CPython writes it; text
            // through `MARK … TUPLE` with a `STRING` path, as Python 2 writes it, escaped
            // (`\x2e`, or `\160` for `p`) on two of every three `n`.
            0 => {
                let point = (format!("p.{n}"), f64::from(1_700_000_000 + n), f64::from(n) + 0.5);
                let escaped = text && n % 3 != 0;
                if text {
                    let path: &[u8] = match n % 3 {
                        0 => b"p.",
                        1 => b"p\\x2e",
                        _ => b"\\160.",
                    };
                    b.op(0x28, b"S'");
                    b.out.extend_from_slice(path);
                    b.out.extend_from_slice(n.to_string().as_bytes());
                    b.out.extend_from_slice(b"'\n");
                    b.op(0x28, &[]);
                    b.line(b'I', point.1.to_string().as_bytes());
                    b.line(b'F', format!("{:?}", point.2).as_bytes());
                    b.op(b't', b"t");
                } else {
                    b.binunicode(&point.0);
                    b.op(0x4a, &(1_700_000_000 + n as i32).to_le_bytes());
                    b.op(0x47, &point.2.to_be_bytes());
                    b.op(0x86, &[0x86]);
                }
                points.push(point.clone());
                copied.push(escaped);
                b.memoize(point, escaped);
            }
            // The same through `MARK … TUPLE`, a second past 2038 (`LONG1`, or a text `LONG`
            // with or without its `L`), and a numeric-string value, which carbon coerces with
            // `float()`. In text the path is a `UNICODE`, with `q` for its `q` or a raw
            // Latin-1 `é` in front on two of every three `n`.
            1 => {
                let seconds = (1u64 << 31) + u64::from(n);
                let value = format!("{n}.25");
                let (path, spelled): (String, Vec<u8>) = match (text, n % 3) {
                    (true, 1) => (format!("q.{n};k={n}"), format!("\\u0071.{n};k={n}").into()),
                    (true, 2) => {
                        let mut spelled = vec![0xe9];
                        spelled.extend_from_slice(format!("q.{n};k={n}").as_bytes());
                        (format!("\u{e9}q.{n};k={n}"), spelled)
                    }
                    _ => (format!("q.{n};k={n}"), format!("q.{n};k={n}").into()),
                };
                let escaped = text && n % 3 != 0;
                let point = (path, seconds as f64, value.parse().unwrap());
                b.op(0x28, &[]);
                if text {
                    b.line(b'V', &spelled);
                    b.op(0x28, &[]);
                    let l = if n % 2 == 0 { "L" } else { "" };
                    b.line(b'L', format!("{seconds}{l}").as_bytes());
                    b.line(b'S', format!("'{value}'").as_bytes());
                } else {
                    b.short_binunicode(&point.0);
                    b.op(0x28, &[0x8a, 5]);
                    b.out.extend_from_slice(&seconds.to_le_bytes()[..5]);
                    b.short_binunicode(&value);
                }
                b.op(0x74, &[0x74]);
                points.push(point.clone());
                copied.push(escaped);
                b.memoize(point, escaped);
            }
            // A repeat of an earlier well-shaped item, through the memo.
            2 if !b.good.is_empty() => {
                let (key, point, escaped) = b.good[n as usize % b.good.len()].clone();
                b.memo_get(key);
                points.push(point);
                copied.push(escaped);
            }
            // A stray `None`.
            2 | 3 => {
                b.op(0x4e, &[]);
                skipped += 1;
            }
            // `(path, (timestamp,))`: the inner tuple has one element.
            4 => {
                if text {
                    b.line(0x28, b"Vr");
                    b.op(0x28, &[]);
                    b.line(b'I', n.to_string().as_bytes());
                    b.op(b't', b"t");
                } else {
                    b.binunicode("r");
                    b.op(0x4b, &[n as u8]);
                    b.op(0x85, &[0x86]);
                }
                skipped += 1;
            }
            // An empty list: a wrong shape that grows no arena.
            5 => {
                b.op(empty_list[0], &empty_list[1..]);
                skipped += 1;
            }
            // `[timestamp, value]`: a non-empty list grows the list arena past the outer list's
            // tail, so an outer list still to be appended to can't be extended.
            _ => {
                if text {
                    b.op(0x28, &[]);
                    b.line(b'I', n.to_string().as_bytes());
                    b.line(b'I', b"1");
                    b.op(b'l', &[]);
                } else {
                    b.op(0x5d, &[0x28, 0x4b, n as u8, 0x4b, 1, 0x65]);
                }
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
    let verdict = if fails { Verdict::Fails } else { Verdict::Reads { points, copied, skipped } };
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
    // Whether each path was a slice of the payload rather than the reader's scratch.
    let mut borrowed: Vec<bool> = Vec::new();
    let read = PickleReader::new().read_datapoints(&payload, |path, timestamp, value| {
        points.push((path.to_string(), timestamp, value));
        borrowed.push(subslice::within(&payload, path.as_bytes()));
    });
    match (&built, &read) {
        (None, _) | (Some(Verdict::Fails), Err(_)) => {}
        (Some(Verdict::Reads { points: want, copied, skipped }), Ok(got)) => {
            assert_eq!(got, skipped, "built: the wrong-shaped items are the ones counted");
            assert_eq!(points.len(), want.len(), "built: every well-shaped item reads");
            for (got, want) in points.iter().zip(want) {
                assert_eq!(got.0, want.0, "built: a path changed");
                assert_eq!(got.1.to_bits(), want.1.to_bits(), "built: a timestamp changed");
                assert_eq!(got.2.to_bits(), want.2.to_bits(), "built: a value changed");
            }
            for (borrowed, copied) in borrowed.iter().zip(copied) {
                assert_eq!(*borrowed, !copied, "built: only an escaped path is decoded");
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
    for ((path, timestamp, value), borrowed) in points.iter().zip(&borrowed) {
        if let Some(expected) = graphite::expected_event(path, *value, *timestamp) {
            let event = events.next().expect("dichotomy: a datapoint the doc keeps is missing");
            graphite::assert_matches(event, &expected, "dichotomy");
            graphite::check_event(&payload, event, *borrowed);
        }
    }
    assert!(events.next().is_none(), "dichotomy: an event no datapoint accounts for");
    graphite::assert_second_generation_fixed_point(out.split_off(1));
});
