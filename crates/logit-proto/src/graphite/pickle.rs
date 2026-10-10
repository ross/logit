//! A pickle **writer** (a ten-opcode protocol-2 subset) and a **restricted reader** for carbon's
//! batch protocol.
//!
//! ## Why this is hand-rolled
//!
//! Pickle is a stack machine whose *purpose* is arbitrary object construction: the opcodes that
//! make a general unpickler dangerous (`GLOBAL`, `STACK_GLOBAL`, `REDUCE`, `BUILD`, `INST`, `OBJ`,
//! `NEWOBJ`, the `EXT*` registry, `PERSID`) import names and call them. Carbon's batch payload is a
//! list of `(str, (number, number))` tuples and needs none of them. So this reader is an
//! **allowlist**: any byte not named below fails the frame with
//! `CodecError::Malformed("pickle opcode 0x.. is not permitted")`, including opcodes that don't
//! exist yet. Adding an opcode to the accept list is an ADR-level change.
//!
//! No pickle crate is used: `serde-pickle`/`pickle` implement the *general* format, whose
//! generality is the risk.
//!
//! ## Accepted opcodes
//!
//! | Group | Opcodes |
//! |---|---|
//! | framing | `PROTO` `0x80` (version ≤ 5), `FRAME` `0x95` (its declared length validated against the input), `STOP` `0x2e` |
//! | memo | `BINPUT` `0x71`, `LONG_BINPUT` `0x72`, `MEMOIZE` `0x94`, `BINGET` `0x68`, `LONG_BINGET` `0x6a` (keys bounded by [`super::MAX_PICKLE_ITEMS`]) |
//! | containers | `MARK` `0x28`, `EMPTY_LIST` `0x5d`, `LIST` `0x6c`, `APPEND` `0x61`, `APPENDS` `0x65`, `EMPTY_TUPLE` `0x29`, `TUPLE` `0x74`, `TUPLE1` `0x85`, `TUPLE2` `0x86`, `TUPLE3` `0x87` |
//! | strings | `BINUNICODE` `0x58`, `SHORT_BINUNICODE` `0x8c`, `BINUNICODE8` `0x8d`, `BINSTRING` `0x54`, `SHORT_BINSTRING` `0x55`, `BINBYTES` `0x42`, `SHORT_BINBYTES` `0x43`, `BINBYTES8` `0x8e` -- every one UTF-8 validated |
//! | numbers | `BININT` `0x4a`, `BININT1` `0x4b`, `BININT2` `0x4d`, `LONG1` `0x8a`, `LONG4` `0x8b` (magnitude ≤ 16 bytes), `BINFLOAT` `0x47` |
//! | inert | `NONE` `0x4e`, `NEWTRUE` `0x88`, `NEWFALSE` `0x89` |
//! | text (protocol 0) | `INT` `0x49`, `LONG` `0x4c`, `FLOAT` `0x46`, `STRING` `0x53`, `UNICODE` `0x56`, `PUT` `0x70`, `GET` `0x67` -- each argument runs to the next `\n` ("Protocol 0") |
//!
//! The three inert opcodes are accepted so a stray `None`/`True` in a sender's list costs **that
//! datapoint**, not every datapoint in the frame. `0x8c`/`0x8d`/`0x8e` and `0x95` are
//! protocol-4/5 opcodes that `pickle.dumps(..., protocol=-1)` emits on a modern CPython. A
//! protocol-1 dump has no `PROTO` header, and every opcode it emits for a carbon payload is a
//! binary one from the table. Every protocol, 0 through 5, decodes.
//!
//! Everything else is rejected, in particular: `GLOBAL` `0x63`, `STACK_GLOBAL` `0x93`, `REDUCE`
//! `0x52`, `BUILD` `0x62`, `INST` `0x69`, `OBJ` `0x6f`, `NEWOBJ` `0x81`, `NEWOBJ_EX` `0x92`,
//! `EXT1/2/4` `0x82`/`0x83`/`0x84`, `PERSID` `0x50`, `BINPERSID` `0x51`, `DUP` `0x32`, `POP`
//! `0x30`, `POP_MARK` `0x31`, every dict and set opcode (`EMPTY_DICT` `0x7d`, `DICT` `0x64`,
//! `SETITEM` `0x73`, `SETITEMS` `0x75`, `EMPTY_SET` `0x8f`, `FROZENSET` `0x91`, `ADDITEMS` `0x90`),
//! `BYTEARRAY8` `0x96`, `NEXT_BUFFER` `0x97`, and `READONLY_BUFFER` `0x98`. Python 3 writes a
//! `bytes` value at protocol 0 as `GLOBAL _codecs encode` plus `REDUCE`, so that fails the frame.
//!
//! ## Protocol 0
//!
//! Protocol 0 is the text pickle. Dropwizard Metrics' `PickledGraphite` writes it by hand, and
//! Python 2 senders (Diamond, graphitesend) write it through `cPickle.dumps`'s default.
//! [ADR `graphite-carbon-relay`](../../../../docs/adr/graphite-carbon-relay.md)'s "Amendment: the
//! reader accepts pickle protocol 0" has the survey, and its "Bounds" the rules below. Each argument is read as CPython's loader
//! reads it, except where a bullet names a spelling this reader fails and CPython takes; no
//! surveyed sender writes one:
//!
//! - `INT` and `LONG`: an optional sign and decimal digits that fit an `i128`. CPython's
//!   `int(x, 0)` also takes `0x`/`0o`/`0b`, `_`, and surrounding whitespace, and its C loader
//!   reads `INT 010` as octal 8; this reader fails all of them, and a leading zero before a
//!   non-zero digit. `I00` and `I01` are `False` and `True`. `LONG`'s trailing `L` is optional,
//!   as it is for CPython.
//! - `FLOAT`: Rust's `f64` parse, which takes `nan`, `inf`, and `1e+06`. A literal past `f64`'s
//!   range, such as `1e999`, fails the frame, as CPython raises `OverflowError` on it.
//! - `PUT` and `GET`: decimal digits only.
//! - `STRING`: quoted with the same `'` or `"` at both ends, its escapes decoded as
//!   `codecs.escape_decode` does (`\\ \' \" \a \b \f \n \r \t \v`, `\xHH` with two hex
//!   digits, and `\ooo` with one to three octal digits). An unknown escape keeps its backslash; a
//!   raw byte passes through, so Dropwizard's unescaped UTF-8 names decode.
//! - `UNICODE`: raw-unicode-escape. Only `\uXXXX` and `\UXXXXXXXX` are escapes, and every other
//!   byte is a Latin-1 code point. An escape naming a surrogate fails the frame, where CPython
//!   builds a `str` it can't encode as UTF-8.
//!
//! A decoded string passes the same UTF-8 check a binary string does.
//!
//! ## Bounds
//!
//! - every declared length is validated against the **remaining input** before anything is sized
//!   from it (all through [`slice()`]); `crates/logit-proto/tests/robustness.rs` measures this with
//!   a peak-allocation counter. A protocol-0 argument has no declared length: it ends at the next
//!   `\n`, and a missing one fails the frame;
//! - a binary string, and a protocol-0 string with nothing to decode, is a range into the caller's
//!   buffer, so a frame declaring a gigabyte allocates nothing and fails the bound. Any other
//!   protocol-0 string is decoded into a per-frame scratch: a `STRING` never grows, and a
//!   `UNICODE` at most doubles (a Latin-1 byte at `0x80` or above becomes two UTF-8 bytes), so a
//!   frame's scratch is at most twice its input;
//! - [`super::MAX_PICKLE_DEPTH`] bounds open `MARK`s, and [`super::MAX_PICKLE_ITEMS`] bounds the
//!   stack, each arena, and the memo independently. A memo key must also be ordinal
//!   (`key <= max(memo.len(), 1)`, at most one new slot per `PUT`/`BINPUT`/`LONG_BINPUT`/`MEMOIZE`
//!   after the first), so one corrupt `LONG_BINPUT` can't grow the memo to the size its key names.
//!   The `1` is Python 2's `cPickle`, which numbers its memo from 1 in every protocol (the ADR
//!   amendment's "Memo keys");
//! - `LONG1`/`LONG4` accept a magnitude of at most 16 bytes, and `INT`/`LONG` an `i128`. An
//!   integer past `i64` reads as the nearest `f64`, as carbon's `float()` reads it, so a `u64`
//!   counter decodes; a larger one, which only hand-built code writes, fails the frame;
//! - the stack must hold **exactly one** value at `STOP`, and it must be a list.
//!
//! ## Reusable state
//!
//! [`PickleReader`]'s stack, arenas, memo, and scratch are fields cleared per frame, so a warm
//! pickle decode allocates only the caller's `Vec<Event>` (`docs/design/memory.md` §2). That is
//! why a tuple or list is an index range into an arena: a `Vec` per tuple would allocate twice per
//! datapoint.

use super::{MAX_PICKLE_DEPTH, MAX_PICKLE_ITEMS};
use crate::CodecError;

// -- opcodes ------------------------------------------------------------------------------------

const OP_MARK: u8 = 0x28;
const OP_EMPTY_TUPLE: u8 = 0x29;
const OP_STOP: u8 = 0x2e;
const OP_BINBYTES: u8 = 0x42;
const OP_SHORT_BINBYTES: u8 = 0x43;
const OP_FLOAT: u8 = 0x46;
const OP_BINFLOAT: u8 = 0x47;
const OP_INT: u8 = 0x49;
const OP_BININT: u8 = 0x4a;
const OP_BININT1: u8 = 0x4b;
const OP_LONG: u8 = 0x4c;
const OP_BININT2: u8 = 0x4d;
const OP_NONE: u8 = 0x4e;
const OP_STRING: u8 = 0x53;
const OP_BINSTRING: u8 = 0x54;
const OP_SHORT_BINSTRING: u8 = 0x55;
const OP_UNICODE: u8 = 0x56;
const OP_BINUNICODE: u8 = 0x58;
const OP_EMPTY_LIST: u8 = 0x5d;
const OP_APPEND: u8 = 0x61;
const OP_APPENDS: u8 = 0x65;
const OP_GET: u8 = 0x67;
const OP_BINGET: u8 = 0x68;
const OP_LONG_BINGET: u8 = 0x6a;
const OP_LIST: u8 = 0x6c;
const OP_PUT: u8 = 0x70;
const OP_BINPUT: u8 = 0x71;
const OP_LONG_BINPUT: u8 = 0x72;
const OP_TUPLE: u8 = 0x74;
const OP_PROTO: u8 = 0x80;
const OP_TUPLE1: u8 = 0x85;
const OP_TUPLE2: u8 = 0x86;
const OP_TUPLE3: u8 = 0x87;
const OP_NEWTRUE: u8 = 0x88;
const OP_NEWFALSE: u8 = 0x89;
const OP_LONG1: u8 = 0x8a;
const OP_LONG4: u8 = 0x8b;
const OP_SHORT_BINUNICODE: u8 = 0x8c;
const OP_BINUNICODE8: u8 = 0x8d;
const OP_BINBYTES8: u8 = 0x8e;
const OP_MEMOIZE: u8 = 0x94;
const OP_FRAME: u8 = 0x95;

/// The highest `PROTO` version this reader accepts; nothing above 5 exists.
const MAX_PROTO_VERSION: u8 = 5;

/// The most bytes a `LONG1`/`LONG4` magnitude may carry: an `i128` (this module's "Bounds"
/// section).
const MAX_LONG_BYTES: usize = 16;

// -- writer -------------------------------------------------------------------------------------

/// Bytes [`write_header`] writes: `PROTO` + its version byte, `EMPTY_LIST`, `MARK`.
pub const HEADER_BYTES: usize = 4;

/// Bytes [`write_trailer`] writes: `APPENDS`, `STOP`.
pub const TRAILER_BYTES: usize = 2;

/// Bytes in carbon's frame prefix: one big-endian `u32` payload length, Twisted's
/// `Int32StringReceiver` framing. Written by [`write_length_prefix`], not by the payload writers.
pub const LENGTH_PREFIX_BYTES: usize = 4;

/// The protocol version [`write_header`] declares: the oldest that has every opcode this writer
/// uses, and what carbon's documented `pickle.dumps(..., protocol=2)` example emits.
pub const WRITE_PROTOCOL: u8 = 2;

/// Opens a datapoint list: `PROTO 2`, `EMPTY_LIST`, `MARK`. Pair with [`write_trailer`], one
/// [`write_datapoint`] per datapoint in between.
pub fn write_header(out: &mut Vec<u8>) {
    out.push(OP_PROTO);
    out.push(WRITE_PROTOCOL);
    out.push(OP_EMPTY_LIST);
    out.push(OP_MARK);
}

/// Closes a datapoint list: `APPENDS`, `STOP`.
pub fn write_trailer(out: &mut Vec<u8>) {
    out.push(OP_APPENDS);
    out.push(OP_STOP);
}

/// Writes one `(path, (timestamp, value))` tuple.
///
/// `BINUNICODE`, not `SHORT_BINSTRING`: on Python 3 a `BINSTRING` unpickles to `bytes`, and
/// carbon treats the path as a `str`. The timestamp is `BININT` when it fits an `i32` and `LONG1`
/// otherwise, as CPython's pickler chooses, which keeps a post-2038 second encodable.
pub fn write_datapoint(out: &mut Vec<u8>, path: &str, timestamp: i64, value: f64) {
    write_binunicode(out, path);
    write_int(out, timestamp);
    out.push(OP_BINFLOAT);
    // BINFLOAT is big-endian IEEE-754, unlike every length field in the format.
    out.extend_from_slice(&value.to_be_bytes());
    out.push(OP_TUPLE2); // (timestamp, value)
    out.push(OP_TUPLE2); // (path, (timestamp, value))
}

/// A whole payload in one call: [`write_header`], one [`write_datapoint`] per item,
/// [`write_trailer`]. `out` is **not** cleared first. For tests and benches; the encoder builds
/// frames incrementally to find where `max_frame_bytes` falls.
pub fn write_datapoints<'a>(
    out: &mut Vec<u8>,
    datapoints: impl IntoIterator<Item = (&'a str, i64, f64)>,
) {
    write_header(out);
    for (path, timestamp, value) in datapoints {
        write_datapoint(out, path, timestamp, value);
    }
    write_trailer(out);
}

/// Writes carbon's 4-byte big-endian length prefix for a `payload_len`-byte pickle payload.
///
/// Saturates rather than wrapping: callers are bounded by `max_frame_bytes` (at most 16 MiB by the
/// graph rules), and a wrapped prefix would silently desynchronize the receiver's stream.
pub fn write_length_prefix(out: &mut Vec<u8>, payload_len: usize) {
    let len = u32::try_from(payload_len).unwrap_or(u32::MAX);
    out.extend_from_slice(&len.to_be_bytes());
}

fn write_binunicode(out: &mut Vec<u8>, s: &str) {
    out.push(OP_BINUNICODE);
    // Length fields are little-endian; only BINFLOAT is big-endian.
    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn write_int(out: &mut Vec<u8>, v: i64) {
    if let Ok(v) = i32::try_from(v) {
        out.push(OP_BININT);
        out.extend_from_slice(&v.to_le_bytes());
        return;
    }
    out.push(OP_LONG1);
    let bytes = v.to_le_bytes();
    // Minimal two's-complement little-endian magnitude, as CPython's `encode_long` emits: drop a
    // trailing sign-extension byte only while the byte below still carries the sign in its high
    // bit, so a positive value keeps the `0x00` that stops it reading as negative.
    let sign: u8 = if v < 0 { 0xff } else { 0x00 };
    let mut len = bytes.len();
    while len > 1 {
        let top = bytes[len - 1];
        let below_is_negative = bytes[len - 2] & 0x80 != 0;
        if top == sign && below_is_negative == (sign == 0xff) {
            len -= 1;
        } else {
            break;
        }
    }
    out.push(len as u8);
    out.extend_from_slice(&bytes[..len]);
}

// -- reader -------------------------------------------------------------------------------------

/// One value on the restricted reader's stack. `Copy` and pointer-free: a tuple or list is an
/// index range into a reusable arena (this module's "Reusable state" section).
#[derive(Debug, Clone, Copy, PartialEq)]
enum PValue {
    /// A `MARK` sentinel, seen only by container opcodes.
    Mark,
    None,
    Bool(bool),
    Int(i64),
    Float(f64),
    /// A UTF-8-validated range into the frame the reader was handed.
    Str {
        start: u32,
        len: u32,
    },
    /// A UTF-8-validated range into [`PickleReader::scratch`]: a protocol-0 string the reader
    /// decoded (a `STRING` with a backslash, or a `UNICODE` with a non-ASCII byte or a `\u`/`\U`
    /// escape).
    Scratch {
        start: u32,
        len: u32,
    },
    /// A range into [`PickleReader::tuples`].
    Tuple {
        start: u32,
        len: u32,
    },
    /// A range into [`PickleReader::lists`].
    List {
        start: u32,
        len: u32,
    },
}

/// The restricted pickle reader: one per [`super::GraphiteDecoder`], reused frame after frame.
#[derive(Debug, Default)]
pub struct PickleReader {
    stack: Vec<PValue>,
    /// Flat storage for every tuple's elements.
    tuples: Vec<PValue>,
    /// Flat storage for every list's elements.
    lists: Vec<PValue>,
    /// Indexed by memo key, filled in key order (`memo_put` rejects a key past the end); a
    /// `BINGET` of an unset key fails.
    memo: Vec<Option<PValue>>,
    /// Decoded protocol-0 strings, back to back; at most twice the frame's length ("Bounds").
    scratch: Vec<u8>,
    /// Open `MARK` count, the depth [`MAX_PICKLE_DEPTH`] bounds.
    marks: usize,
}

impl PickleReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses one complete, **unframed** pickle payload (the listener strips the length prefix)
    /// and calls `on_datapoint` once per well-shaped `(path, (timestamp, value))` item, in list
    /// order. Returns how many items were skipped for being the wrong shape.
    ///
    /// A wrong-shaped *item* is skipped. A disallowed opcode, a declared length past the input, a
    /// string that isn't UTF-8, a bound, or a payload that leaves anything but a single list on
    /// the stack fails the whole frame with [`CodecError::Malformed`], and so does a non-empty
    /// list item inside a list grown by `APPEND`/`APPENDS` (`append_range`'s tail rule).
    ///
    /// `path` borrows `input`, unless it is a protocol-0 string the reader decoded ("Bounds": a
    /// `STRING` with a backslash, or a `UNICODE` with a non-ASCII byte or a `\u`/`\U` escape):
    /// then it borrows the reader's scratch. `logit_core::subslice::share` tells the two apart, so a
    /// caller still gets a zero-copy [`bytes::Bytes`] slice wherever one exists.
    pub fn read_datapoints(
        &mut self,
        input: &[u8],
        mut on_datapoint: impl FnMut(&str, f64, f64),
    ) -> Result<usize, CodecError> {
        self.parse(input)?;

        if self.stack.len() != 1 {
            return Err(malformed(format!(
                "pickle payload leaves {} value(s) on the stack, not exactly one",
                self.stack.len()
            )));
        }
        let PValue::List { start, len } = self.stack[0] else {
            return Err(malformed("pickle payload is not a list of datapoints"));
        };

        let mut skipped = 0usize;
        for i in start as usize..(start as usize + len as usize) {
            match self.datapoint(input, self.lists[i]) {
                Some((path, timestamp, value)) => on_datapoint(path, timestamp, value),
                None => skipped += 1,
            }
        }
        Ok(skipped)
    }

    /// Pulls `(path, timestamp, value)` out of one list item, or `None` if it is not a
    /// `(str, (number, number))`. Looks exactly two levels down and never recurses, so crafted
    /// nesting can't make it recurse.
    fn datapoint<'a>(&'a self, input: &'a [u8], item: PValue) -> Option<(&'a str, f64, f64)> {
        let PValue::Tuple { start, len } = item else { return None };
        if len != 2 {
            return None;
        }
        let path = self.as_str(input, self.tuples[start as usize])?;
        let PValue::Tuple { start: inner, len: inner_len } = self.tuples[start as usize + 1] else {
            return None;
        };
        if inner_len != 2 {
            return None;
        }
        let timestamp = self.as_f64(input, self.tuples[inner as usize])?;
        let value = self.as_f64(input, self.tuples[inner as usize + 1])?;
        Some((path, timestamp, value))
    }

    fn as_str<'a>(&'a self, input: &'a [u8], value: PValue) -> Option<&'a str> {
        let bytes = match value {
            PValue::Str { start, len } => &input[start as usize..start as usize + len as usize],
            PValue::Scratch { start, len } => {
                &self.scratch[start as usize..start as usize + len as usize]
            }
            _ => return None,
        };
        // Re-validated rather than trusted with `unsafe`; the cost is a scan of one path.
        std::str::from_utf8(bytes).ok()
    }

    /// A number, or a **numeric string**: Python producers often send `"3.14"`, and carbon
    /// coerces with `float()`, as `str::parse::<f64>` does here. Dropwizard's `PickledGraphite`
    /// sends every value this way.
    fn as_f64(&self, input: &[u8], value: PValue) -> Option<f64> {
        match value {
            PValue::Int(v) => Some(v as f64),
            PValue::Float(v) => Some(v),
            PValue::Str { .. } | PValue::Scratch { .. } => {
                self.as_str(input, value)?.parse::<f64>().ok()
            }
            PValue::Mark
            | PValue::None
            | PValue::Bool(_)
            | PValue::Tuple { .. }
            | PValue::List { .. } => None,
        }
    }

    /// Runs the stack machine over `input`, leaving the result in [`Self::stack`].
    fn parse(&mut self, input: &[u8]) -> Result<(), CodecError> {
        self.stack.clear();
        self.tuples.clear();
        self.lists.clear();
        self.memo.clear();
        self.scratch.clear();
        self.marks = 0;

        // Ranges are `u32`, and the scratch can reach twice the input; `max_frame_bytes` (at most
        // 16 MiB) keeps a larger payload out, but one must not truncate a range if it got here.
        if input.len() > (u32::MAX / 2) as usize {
            return Err(malformed("pickle payload is larger than 2 GiB"));
        }

        let mut at = 0usize;
        loop {
            let op = *input.get(at).ok_or_else(|| malformed("pickle payload ends before STOP"))?;
            at += 1;
            match op {
                OP_STOP => return Ok(()),
                OP_PROTO => {
                    let version = slice(input, at, 1)?[0];
                    at += 1;
                    if version > MAX_PROTO_VERSION {
                        return Err(malformed(format!(
                            "pickle protocol version {version} is not supported"
                        )));
                    }
                }
                // Advisory, since the reader holds the whole payload, but validated so a frame
                // claiming more than it carries fails here.
                OP_FRAME => {
                    let declared = u64::from_le_bytes(slice(input, at, 8)?.try_into().unwrap());
                    at += 8;
                    let remaining = (input.len() - at) as u64;
                    if declared > remaining {
                        return Err(malformed(format!(
                            "pickle FRAME declares {declared} byte(s) over {remaining} remaining"
                        )));
                    }
                }
                OP_MARK => {
                    self.marks += 1;
                    if self.marks > MAX_PICKLE_DEPTH {
                        return Err(malformed(format!(
                            "pickle nesting exceeds the {MAX_PICKLE_DEPTH}-level cap"
                        )));
                    }
                    self.push(PValue::Mark)?;
                }
                OP_NONE => self.push(PValue::None)?,
                OP_NEWTRUE => self.push(PValue::Bool(true))?,
                OP_NEWFALSE => self.push(PValue::Bool(false))?,

                // -- numbers --
                OP_BININT => {
                    let v = i32::from_le_bytes(slice(input, at, 4)?.try_into().unwrap());
                    at += 4;
                    self.push(PValue::Int(v as i64))?;
                }
                OP_BININT1 => {
                    let v = slice(input, at, 1)?[0];
                    at += 1;
                    self.push(PValue::Int(v as i64))?;
                }
                OP_BININT2 => {
                    let v = u16::from_le_bytes(slice(input, at, 2)?.try_into().unwrap());
                    at += 2;
                    self.push(PValue::Int(v as i64))?;
                }
                OP_LONG1 => {
                    let n = slice(input, at, 1)?[0] as usize;
                    at += 1;
                    let v = read_long(input, at, n)?;
                    at += n;
                    self.push(integer(v))?;
                }
                OP_LONG4 => {
                    let n = i32::from_le_bytes(slice(input, at, 4)?.try_into().unwrap());
                    at += 4;
                    let n = usize::try_from(n)
                        .map_err(|_| malformed("pickle LONG4 declares a negative length"))?;
                    let v = read_long(input, at, n)?;
                    at += n;
                    self.push(integer(v))?;
                }
                OP_BINFLOAT => {
                    let v = f64::from_be_bytes(slice(input, at, 8)?.try_into().unwrap());
                    at += 8;
                    self.push(PValue::Float(v))?;
                }

                // -- strings: all eight spellings land on one UTF-8-validated range --
                OP_SHORT_BINUNICODE | OP_SHORT_BINSTRING | OP_SHORT_BINBYTES => {
                    let n = slice(input, at, 1)?[0] as usize;
                    at += 1;
                    self.push_str(input, at, n)?;
                    at += n;
                }
                OP_BINUNICODE | OP_BINBYTES => {
                    let n = u32::from_le_bytes(slice(input, at, 4)?.try_into().unwrap()) as usize;
                    at += 4;
                    self.push_str(input, at, n)?;
                    at += n;
                }
                // BINSTRING's length is *signed*; CPython rejects a negative one rather than
                // sign-extending it into a huge size.
                OP_BINSTRING => {
                    let n = i32::from_le_bytes(slice(input, at, 4)?.try_into().unwrap());
                    at += 4;
                    let n = usize::try_from(n)
                        .map_err(|_| malformed("pickle BINSTRING declares a negative length"))?;
                    self.push_str(input, at, n)?;
                    at += n;
                }
                OP_BINUNICODE8 | OP_BINBYTES8 => {
                    let n = u64::from_le_bytes(slice(input, at, 8)?.try_into().unwrap());
                    // A `u64` length that doesn't fit `usize` can only be crafted.
                    let n = usize::try_from(n).map_err(|_| {
                        malformed("pickle string length does not fit this platform's usize")
                    })?;
                    at += 8;
                    self.push_str(input, at, n)?;
                    at += n;
                }

                // -- containers --
                OP_EMPTY_LIST => {
                    let start = self.lists.len() as u32;
                    self.push(PValue::List { start, len: 0 })?;
                }
                OP_EMPTY_TUPLE => {
                    let start = self.tuples.len() as u32;
                    self.push(PValue::Tuple { start, len: 0 })?;
                }
                OP_TUPLE => self.build_tuple_from_mark()?,
                OP_TUPLE1 => self.build_tuple(1)?,
                OP_TUPLE2 => self.build_tuple(2)?,
                OP_TUPLE3 => self.build_tuple(3)?,
                OP_LIST => self.build_list_from_mark()?,
                OP_APPEND => self.append(1)?,
                OP_APPENDS => self.appends()?,

                // -- memo --
                OP_BINPUT => {
                    let key = slice(input, at, 1)?[0] as usize;
                    at += 1;
                    self.memo_put(key)?;
                }
                OP_LONG_BINPUT => {
                    let key = u32::from_le_bytes(slice(input, at, 4)?.try_into().unwrap()) as usize;
                    at += 4;
                    self.memo_put(key)?;
                }
                OP_MEMOIZE => {
                    // CPython keys `MEMOIZE` by the count of filled slots, and `memo_put` can
                    // leave only slot 0 unfilled.
                    let key =
                        self.memo.len() - usize::from(matches!(self.memo.first(), Some(None)));
                    self.memo_put(key)?;
                }
                OP_BINGET => {
                    let key = slice(input, at, 1)?[0] as usize;
                    at += 1;
                    self.memo_get(key)?;
                }
                OP_LONG_BINGET => {
                    let key = u32::from_le_bytes(slice(input, at, 4)?.try_into().unwrap()) as usize;
                    at += 4;
                    self.memo_get(key)?;
                }

                // -- protocol 0: each argument runs to the next `\n` ("Protocol 0") --
                OP_INT => {
                    let value = match line(input, &mut at)? {
                        b"00" => PValue::Bool(false),
                        b"01" => PValue::Bool(true),
                        arg => integer(parse_decimal(arg, "INT")?),
                    };
                    self.push(value)?;
                }
                OP_LONG => {
                    let arg = line(input, &mut at)?;
                    let digits = arg.strip_suffix(b"L").unwrap_or(arg);
                    self.push(integer(parse_decimal(digits, "LONG")?))?;
                }
                OP_FLOAT => {
                    let value = parse_float(line(input, &mut at)?)?;
                    self.push(PValue::Float(value))?;
                }
                OP_STRING => {
                    let start = at;
                    let arg = line(input, &mut at)?;
                    self.push_quoted_string(input, start, arg.len())?;
                }
                OP_UNICODE => {
                    let start = at;
                    let arg = line(input, &mut at)?;
                    self.push_raw_unicode(input, start, arg.len())?;
                }
                OP_PUT => {
                    let key = parse_memo_key(line(input, &mut at)?)?;
                    self.memo_put(key)?;
                }
                OP_GET => {
                    let key = parse_memo_key(line(input, &mut at)?)?;
                    self.memo_get(key)?;
                }

                other => {
                    return Err(malformed(format!("pickle opcode {other:#04x} is not permitted")))
                }
            }
        }
    }

    fn push(&mut self, value: PValue) -> Result<(), CodecError> {
        if self.stack.len() >= MAX_PICKLE_ITEMS {
            return Err(malformed(format!(
                "pickle stack exceeds the {MAX_PICKLE_ITEMS}-value cap"
            )));
        }
        self.stack.push(value);
        Ok(())
    }

    /// Validates `n` against the remaining input and the bytes as UTF-8, then pushes the
    /// **range**; a crafted length costs a comparison, not an allocation.
    fn push_str(&mut self, input: &[u8], at: usize, n: usize) -> Result<(), CodecError> {
        let bytes = slice(input, at, n)?;
        if std::str::from_utf8(bytes).is_err() {
            return Err(malformed("pickle string is not valid utf-8"));
        }
        self.push(PValue::Str { start: at as u32, len: n as u32 })
    }

    /// `STRING`'s argument, `input[start..start + n]`: a quoted string whose escapes decode as
    /// Python's `codecs.escape_decode` decodes them. An argument with no backslash is pushed as a
    /// range into `input`; any other is decoded into the scratch.
    fn push_quoted_string(
        &mut self,
        input: &[u8],
        start: usize,
        n: usize,
    ) -> Result<(), CodecError> {
        let arg = &input[start..start + n];
        let quoted = n >= 2 && arg[0] == arg[n - 1] && matches!(arg[0], b'\'' | b'"');
        if !quoted {
            return Err(malformed("pickle STRING argument is not quoted"));
        }
        let body = &arg[1..n - 1];
        if !body.contains(&b'\\') {
            return self.push_str(input, start + 1, n - 2);
        }
        let from = self.scratch.len();
        decode_string_escape(body, &mut self.scratch)?;
        self.push_scratch(from)
    }

    /// `UNICODE`'s argument, `input[start..start + n]`, decoded as Python's `raw_unicode_escape`
    /// decodes it. An ASCII argument with no `\u`/`\U` escape decodes to itself, so it is pushed
    /// as a range into `input`; any other is decoded into the scratch.
    fn push_raw_unicode(&mut self, input: &[u8], start: usize, n: usize) -> Result<(), CodecError> {
        let arg = &input[start..start + n];
        if arg.is_ascii() && !has_unicode_escape(arg) {
            return self.push_str(input, start, n);
        }
        let from = self.scratch.len();
        decode_raw_unicode_escape(arg, &mut self.scratch)?;
        self.push_scratch(from)
    }

    /// Validates `scratch[from..]` as UTF-8 and pushes it as one string.
    fn push_scratch(&mut self, from: usize) -> Result<(), CodecError> {
        if std::str::from_utf8(&self.scratch[from..]).is_err() {
            return Err(malformed("pickle string is not valid utf-8"));
        }
        let len = self.scratch.len() - from;
        self.push(PValue::Scratch { start: from as u32, len: len as u32 })
    }

    /// Pops the topmost `MARK`'s position, or fails. Decrements the open-mark depth.
    fn take_mark(&mut self) -> Result<usize, CodecError> {
        let at = self
            .stack
            .iter()
            .rposition(|v| matches!(v, PValue::Mark))
            .ok_or_else(|| malformed("pickle container opcode with no MARK on the stack"))?;
        self.marks -= 1;
        Ok(at)
    }

    fn build_tuple_from_mark(&mut self) -> Result<(), CodecError> {
        let mark = self.take_mark()?;
        let start = self.tuples.len();
        if start + (self.stack.len() - mark - 1) > MAX_PICKLE_ITEMS {
            return Err(malformed(format!(
                "pickle tuple storage exceeds the {MAX_PICKLE_ITEMS}-value cap"
            )));
        }
        self.tuples.extend_from_slice(&self.stack[mark + 1..]);
        let len = self.tuples.len() - start;
        self.stack.truncate(mark);
        self.push(PValue::Tuple { start: start as u32, len: len as u32 })
    }

    fn build_tuple(&mut self, n: usize) -> Result<(), CodecError> {
        if self.stack.len() < n {
            return Err(malformed("pickle TUPLE opcode with too few values on the stack"));
        }
        let from = self.stack.len() - n;
        if self.stack[from..].iter().any(|v| matches!(v, PValue::Mark)) {
            return Err(malformed("pickle TUPLE opcode would capture a MARK"));
        }
        let start = self.tuples.len();
        if start + n > MAX_PICKLE_ITEMS {
            return Err(malformed(format!(
                "pickle tuple storage exceeds the {MAX_PICKLE_ITEMS}-value cap"
            )));
        }
        self.tuples.extend_from_slice(&self.stack[from..]);
        self.stack.truncate(from);
        self.push(PValue::Tuple { start: start as u32, len: n as u32 })
    }

    fn build_list_from_mark(&mut self) -> Result<(), CodecError> {
        let mark = self.take_mark()?;
        let start = self.lists.len();
        if start + (self.stack.len() - mark - 1) > MAX_PICKLE_ITEMS {
            return Err(malformed(format!(
                "pickle list storage exceeds the {MAX_PICKLE_ITEMS}-value cap"
            )));
        }
        self.lists.extend_from_slice(&self.stack[mark + 1..]);
        let len = self.lists.len() - start;
        self.stack.truncate(mark);
        self.push(PValue::List { start: start as u32, len: len as u32 })
    }

    /// `APPENDS`: everything above the topmost `MARK` is appended to the list immediately below it.
    fn appends(&mut self) -> Result<(), CodecError> {
        let mark = self.take_mark()?;
        // The list sits one slot *below* the mark; a mark at the very bottom of the stack has
        // nothing to append onto.
        let target = mark
            .checked_sub(1)
            .ok_or_else(|| malformed("pickle APPENDS with no list below the MARK"))?;
        self.append_range(target, mark + 1)
    }

    /// `APPEND`: the top `n` values are appended to the list below them.
    fn append(&mut self, n: usize) -> Result<(), CodecError> {
        if self.stack.len() < n + 1 {
            return Err(malformed("pickle APPEND with too few values on the stack"));
        }
        let target = self.stack.len() - n - 1;
        self.append_range(target, target + 1)
    }

    /// Appends `stack[from..]` to the list at `stack[target]`, then truncates the stack to
    /// `target + 1`.
    ///
    /// The list must be the **tail** of the arena (`start + len == lists.len()`), as carbon's
    /// `EMPTY_LIST MARK … APPENDS` shape gives, CPython's batches of 1,000 included. Interleaved
    /// open lists are rejected, which fails a list-shaped datapoint, `[path, [ts, value]]`, built
    /// inside a list still to be appended to: carbon's receiver accepts one, but no surveyed
    /// producer writes one (`docs/known-gaps/mappings.md`'s `decode (Graphite)` row). Supporting
    /// them needs a per-list `Vec` or a compaction pass whose cost the frame's shape chooses.
    fn append_range(&mut self, target: usize, from: usize) -> Result<(), CodecError> {
        let PValue::List { start, len } = self.stack[target] else {
            return Err(malformed("pickle APPEND/APPENDS onto something that is not a list"));
        };
        if (start + len) as usize != self.lists.len() {
            return Err(malformed("pickle APPEND/APPENDS onto a list this reader cannot extend"));
        }
        let added = self.stack.len() - from;
        if self.lists.len() + added > MAX_PICKLE_ITEMS {
            return Err(malformed(format!("pickle list exceeds the {MAX_PICKLE_ITEMS}-item cap")));
        }
        self.lists.extend_from_slice(&self.stack[from..]);
        self.stack[target] = PValue::List { start, len: len + added as u32 };
        self.stack.truncate(target + 1);
        Ok(())
    }

    fn memo_put(&mut self, key: usize) -> Result<(), CodecError> {
        if key >= MAX_PICKLE_ITEMS {
            return Err(malformed(format!(
                "pickle memo key {key} exceeds the {MAX_PICKLE_ITEMS}-entry cap"
            )));
        }
        // A memo key must be ordinal: CPython hands out keys sequentially, so a real stream only
        // overwrites a slot or appends the next. A key past that would size the memo from a
        // corrupt index ("Bounds"). Python 2's `cPickle` numbers from 1 rather than 0, so slot 0
        // may be skipped, and only slot 0.
        if key > self.memo.len().max(1) {
            return Err(malformed(format!(
                "pickle memo key {key} skips ahead of the {} entries written so far",
                self.memo.len()
            )));
        }
        let value =
            *self.stack.last().ok_or_else(|| malformed("pickle memo put on an empty stack"))?;
        if matches!(value, PValue::Mark) {
            return Err(malformed("pickle memo put of a MARK"));
        }
        // At most two new slots: the skipped slot 0 and `key` itself.
        if key >= self.memo.len() {
            self.memo.resize(key + 1, None);
        }
        self.memo[key] = Some(value);
        Ok(())
    }

    fn memo_get(&mut self, key: usize) -> Result<(), CodecError> {
        let value = self
            .memo
            .get(key)
            .copied()
            .flatten()
            .ok_or_else(|| malformed(format!("pickle memo key {key} was never set")))?;
        self.push(value)
    }
}

/// `input[at..at + n]`, or [`CodecError::Malformed`]: every declared length in this module is
/// checked against the input here.
fn slice(input: &[u8], at: usize, n: usize) -> Result<&[u8], CodecError> {
    let end = at.checked_add(n).ok_or_else(|| malformed("pickle length overflows usize"))?;
    input
        .get(at..end)
        .ok_or_else(|| malformed(format!("pickle field of {n} byte(s) runs past the payload")))
}

/// A protocol-0 opcode's argument: `input[*at..]` up to the next `\n`, which `*at` moves past. A
/// missing `\n` fails the frame, so an argument is never longer than the input.
fn line<'a>(input: &'a [u8], at: &mut usize) -> Result<&'a [u8], CodecError> {
    let rest = input.get(*at..).unwrap_or_default();
    let n = rest
        .iter()
        .position(|&b| b == b'\n')
        .ok_or_else(|| malformed("pickle protocol-0 argument has no terminating newline"))?;
    *at += n + 1;
    Ok(&rest[..n])
}

/// `INT`'s or `LONG`'s argument: an optional sign and decimal digits that fit an `i128`
/// ("Protocol 0" says which spellings CPython takes that this rejects).
fn parse_decimal(arg: &[u8], op: &str) -> Result<i128, CodecError> {
    let digits = arg.strip_prefix(b"-").or_else(|| arg.strip_prefix(b"+")).unwrap_or(arg);
    let decimal = !digits.is_empty() && digits.iter().all(u8::is_ascii_digit);
    // CPython's C loader reads `INT 010` with `strtol` base 0, as octal 8.
    let octal_looking = digits.len() > 1 && digits[0] == b'0' && digits.iter().any(|&d| d != b'0');
    if !decimal || octal_looking {
        return Err(malformed(format!("pickle {op} argument is not a decimal integer")));
    }
    std::str::from_utf8(arg)
        .ok()
        .and_then(|text| text.parse::<i128>().ok())
        .ok_or_else(|| malformed(format!("pickle {op} argument does not fit an i128")))
}

/// `FLOAT`'s argument: a Python float `repr`, including `nan` and `inf`.
fn parse_float(arg: &[u8]) -> Result<f64, CodecError> {
    let value = std::str::from_utf8(arg)
        .ok()
        .and_then(|text| text.parse::<f64>().ok())
        .ok_or_else(|| malformed("pickle FLOAT argument is not a float"))?;
    // CPython's loader raises `OverflowError` on a literal past `f64`'s range, such as `1e999`,
    // where Rust's parse returns infinity. Only an `inf` spelling contains an `i`.
    if value.is_infinite() && !arg.iter().any(|b| b.eq_ignore_ascii_case(&b'i')) {
        return Err(malformed("pickle FLOAT argument is out of range"));
    }
    Ok(value)
}

/// `PUT`'s or `GET`'s argument: a decimal memo key, which `memo_put`/`memo_get` then bound.
fn parse_memo_key(arg: &[u8]) -> Result<usize, CodecError> {
    let decimal = !arg.is_empty() && arg.iter().all(u8::is_ascii_digit);
    decimal
        .then(|| std::str::from_utf8(arg).ok()?.parse::<usize>().ok())
        .flatten()
        .ok_or_else(|| malformed("pickle PUT/GET memo key is not a decimal integer"))
}

/// Decodes a `STRING` body (its quotes stripped) onto `out`, as CPython's
/// `_PyBytes_DecodeEscape` does. The output is never longer than `body`.
///
/// An escape-newline can't occur, because the body ends at the first `\n`.
fn decode_string_escape(body: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
    let mut i = 0;
    while i < body.len() {
        let c = body[i];
        i += 1;
        if c != b'\\' {
            out.push(c);
            continue;
        }
        let Some(&e) = body.get(i) else {
            return Err(malformed("pickle STRING ends in a lone backslash"));
        };
        i += 1;
        match e {
            b'\\' | b'\'' | b'"' => out.push(e),
            b'a' => out.push(0x07),
            b'b' => out.push(0x08),
            b'f' => out.push(0x0c),
            b'n' => out.push(b'\n'),
            b'r' => out.push(b'\r'),
            b't' => out.push(b'\t'),
            b'v' => out.push(0x0b),
            b'0'..=b'7' => {
                // Up to three octal digits; CPython keeps the low byte of a value past `\377`.
                let mut value = u32::from(e - b'0');
                for _ in 0..2 {
                    match body.get(i) {
                        Some(&d @ b'0'..=b'7') => {
                            value = value * 8 + u32::from(d - b'0');
                            i += 1;
                        }
                        _ => break,
                    }
                }
                out.push(value as u8);
            }
            b'x' => {
                // Two hex digits, or CPython raises "invalid \x escape".
                let hi = body.get(i).and_then(|&d| hex_value(d));
                let lo = body.get(i + 1).and_then(|&d| hex_value(d));
                let (Some(hi), Some(lo)) = (hi, lo) else {
                    return Err(malformed("pickle STRING has an invalid \\x escape"));
                };
                out.push((hi << 4) | lo);
                i += 2;
            }
            // An unknown escape keeps its backslash, and the byte after it is read as ordinary.
            _ => {
                out.push(b'\\');
                i -= 1;
            }
        }
    }
    Ok(())
}

/// Whether `arg` holds a `\u` or `\U` escape as raw-unicode-escape reads it: a backslash pairs
/// with the byte after it, so `\\u` is a backslash pair and then a literal `u`.
fn has_unicode_escape(arg: &[u8]) -> bool {
    let mut i = 0;
    while i + 1 < arg.len() {
        if arg[i] != b'\\' {
            i += 1;
            continue;
        }
        if matches!(arg[i + 1], b'u' | b'U') {
            return true;
        }
        i += 2;
    }
    false
}

/// Decodes a `UNICODE` argument onto `out` as UTF-8, as CPython's
/// `_PyUnicode_DecodeRawUnicodeEscape` does. Only `\uXXXX` and `\UXXXXXXXX` are escapes; a
/// backslash before any other byte stays, and every other byte is a Latin-1 code point. The
/// output is at most twice `arg`: a byte at `0x80` or above becomes two UTF-8 bytes, and an
/// escape of six or ten bytes becomes at most four.
fn decode_raw_unicode_escape(arg: &[u8], out: &mut Vec<u8>) -> Result<(), CodecError> {
    let mut i = 0;
    while i < arg.len() {
        let c = arg[i];
        i += 1;
        if c != b'\\' || i == arg.len() {
            push_latin1(out, c);
            continue;
        }
        let e = arg[i];
        i += 1;
        let width = match e {
            b'u' => 4,
            b'U' => 8,
            _ => {
                out.push(b'\\');
                push_latin1(out, e);
                continue;
            }
        };
        let digits = arg
            .get(i..i + width)
            .ok_or_else(|| malformed("pickle UNICODE has a truncated \\u escape"))?;
        let mut code = 0u32;
        for &d in digits {
            let d = hex_value(d)
                .ok_or_else(|| malformed("pickle UNICODE has a non-hex digit in a \\u escape"))?;
            code = (code << 4) | u32::from(d);
        }
        i += width;
        // CPython decodes a lone surrogate into a `str` that can't be encoded as UTF-8; here
        // it fails the frame, as a non-UTF-8 binary string does.
        let ch = char::from_u32(code).ok_or_else(|| {
            malformed(format!("pickle UNICODE escape {code:#x} is a surrogate or past U+10FFFF"))
        })?;
        let mut buf = [0u8; 4];
        out.extend_from_slice(ch.encode_utf8(&mut buf).as_bytes());
    }
    Ok(())
}

/// Appends Latin-1 byte `c` as UTF-8: itself below `0x80`, two bytes from there.
fn push_latin1(out: &mut Vec<u8>, c: u8) {
    if c < 0x80 {
        out.push(c);
    } else {
        out.push(0xc0 | (c >> 6));
        out.push(0x80 | (c & 0x3f));
    }
}

fn hex_value(d: u8) -> Option<u8> {
    (d as char).to_digit(16).map(|v| v as u8)
}

/// An integer as the stack holds it: an `i64` where it fits, else the nearest `f64`, which is
/// what carbon's `float()` makes of it. `as` rounds an `i128` to nearest, ties to even, as
/// CPython's `float(int)` does.
fn integer(v: i128) -> PValue {
    match i64::try_from(v) {
        Ok(v) => PValue::Int(v),
        Err(_) => PValue::Float(v as f64),
    }
}

/// A `LONG1`/`LONG4` magnitude: little-endian two's complement, at most [`MAX_LONG_BYTES`] bytes.
fn read_long(input: &[u8], at: usize, n: usize) -> Result<i128, CodecError> {
    if n > MAX_LONG_BYTES {
        return Err(malformed(format!(
            "pickle long of {n} byte(s) exceeds the {MAX_LONG_BYTES}-byte magnitude cap"
        )));
    }
    let bytes = slice(input, at, n)?;
    if n == 0 {
        // CPython encodes `0` as a zero-length long.
        return Ok(0);
    }
    let negative = bytes[n - 1] & 0x80 != 0;
    let mut buf = if negative { [0xffu8; MAX_LONG_BYTES] } else { [0u8; MAX_LONG_BYTES] };
    buf[..n].copy_from_slice(bytes);
    Ok(i128::from_le_bytes(buf))
}

fn malformed(msg: impl Into<String>) -> CodecError {
    CodecError::Malformed(msg.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::AssertUnwindSafe;

    // -- CPython fixtures ------------------------------------------------------------------------
    //
    // Every literal below was produced by running CPython's own stdlib `pickle` on this host and
    // hexdumping the result -- the provenance rule `docs/design/memory.md`'s "Fixtures" section
    // states for a wire-format literal. The generator was:
    //
    //     python3 -c "import pickle; b = pickle.dumps(<expr>, protocol=<n>); \
    //                 print(', '.join(f'0x{x:02x}' for x in b))"
    //
    // with `<expr>`/`<n>` as each constant's doc comment records. Python 3.14.0. The bytes are
    // committed, not the generator: fixtures never depend on a running service or interpreter
    // (AGENTS.md).

    /// pickle.dumps([('sys.cpu', (1700000000, 0.5))], protocol=2)
    const CPYTHON_PROTOCOL_2: &[u8] = &[
        0x80, 0x02, 0x5d, 0x71, 0x00, 0x58, 0x07, 0x00, 0x00, 0x00, 0x73, 0x79, 0x73, 0x2e, 0x63,
        0x70, 0x75, 0x71, 0x01, 0x4a, 0x00, 0xf1, 0x53, 0x65, 0x47, 0x3f, 0xe0, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x86, 0x71, 0x02, 0x86, 0x71, 0x03, 0x61, 0x2e,
    ];

    /// pickle.dumps([('sys.cpu', (1700000000, 0.5))], protocol=-1) -- protocol 5 on this CPython,
    /// so `FRAME`, `SHORT_BINUNICODE` and `MEMOIZE` all appear where protocol 2 used `BINUNICODE`
    /// and `BINPUT`.
    const CPYTHON_PROTOCOL_5: &[u8] = &[
        0x80, 0x05, 0x95, 0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x5d, 0x94, 0x8c, 0x07,
        0x73, 0x79, 0x73, 0x2e, 0x63, 0x70, 0x75, 0x94, 0x4a, 0x00, 0xf1, 0x53, 0x65, 0x47, 0x3f,
        0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x94, 0x86, 0x94, 0x61, 0x2e,
    ];

    /// p = 'a.b'; pickle.dumps([(p, (1, 1.0)), (p, (2, 2.0))], protocol=2) -- one `str` object
    /// used twice, so the second datapoint's path is a `BINGET` (0x68) of memo slot 1.
    const CPYTHON_MEMOIZED_PATH: &[u8] = &[
        0x80, 0x02, 0x5d, 0x71, 0x00, 0x28, 0x58, 0x03, 0x00, 0x00, 0x00, 0x61, 0x2e, 0x62, 0x71,
        0x01, 0x4b, 0x01, 0x47, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x71, 0x02,
        0x86, 0x71, 0x03, 0x68, 0x01, 0x4b, 0x02, 0x47, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x86, 0x71, 0x04, 0x86, 0x71, 0x05, 0x65, 0x2e,
    ];

    /// pickle.dumps([('a.b', (2**31 + 5, 1.0))], protocol=2) -- a second past 2038, which CPython
    /// writes as `LONG1` (0x8a) because it no longer fits `BININT`'s `i32`.
    const CPYTHON_LONG1_TIMESTAMP: &[u8] = &[
        0x80, 0x02, 0x5d, 0x71, 0x00, 0x58, 0x03, 0x00, 0x00, 0x00, 0x61, 0x2e, 0x62, 0x71, 0x01,
        0x8a, 0x05, 0x05, 0x00, 0x00, 0x80, 0x00, 0x47, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x86, 0x71, 0x02, 0x86, 0x71, 0x03, 0x61, 0x2e,
    ];

    /// pickle.dumps([('a.b', (1, 1.0))], protocol=0) -- textual opcodes throughout (`PUT` 0x70,
    /// `UNICODE` 0x56, `INT` 0x49, `FLOAT` 0x46).
    const CPYTHON_PROTOCOL_0: &[u8] = &[
        0x28, 0x6c, 0x70, 0x30, 0x0a, 0x28, 0x56, 0x61, 0x2e, 0x62, 0x0a, 0x70, 0x31, 0x0a, 0x28,
        0x49, 0x31, 0x0a, 0x46, 0x31, 0x2e, 0x30, 0x0a, 0x74, 0x70, 0x32, 0x0a, 0x74, 0x70, 0x33,
        0x0a, 0x61, 0x2e,
    ];

    /// Python 3.14.7: pickle.dumps([('sys.cpu', (1700000000, 0.5)),
    /// ('café.€\\x', (2**40, float('nan'))), ('flag', (True, None))], protocol=0) --
    /// `UNICODE` with a raw Latin-1 byte (0xe9) and `\u` escapes (`€`, and `\` for the
    /// backslash), a `LONG` `L…L`, `F nan`, `I01`, and `NONE`.
    const CPYTHON3_PROTOCOL_0_ESCAPED: &[u8] = &[
        0x28, 0x6c, 0x70, 0x30, 0x0a, 0x28, 0x56, 0x73, 0x79, 0x73, 0x2e, 0x63, 0x70, 0x75, 0x0a,
        0x70, 0x31, 0x0a, 0x28, 0x49, 0x31, 0x37, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x0a, 0x46, 0x30, 0x2e, 0x35, 0x0a, 0x74, 0x70, 0x32, 0x0a, 0x74, 0x70, 0x33, 0x0a, 0x61,
        0x28, 0x56, 0x63, 0x61, 0x66, 0xe9, 0x2e, 0x5c, 0x75, 0x32, 0x30, 0x61, 0x63, 0x5c, 0x75,
        0x30, 0x30, 0x35, 0x63, 0x78, 0x0a, 0x70, 0x34, 0x0a, 0x28, 0x4c, 0x31, 0x30, 0x39, 0x39,
        0x35, 0x31, 0x31, 0x36, 0x32, 0x37, 0x37, 0x37, 0x36, 0x4c, 0x0a, 0x46, 0x6e, 0x61, 0x6e,
        0x0a, 0x74, 0x70, 0x35, 0x0a, 0x74, 0x70, 0x36, 0x0a, 0x61, 0x28, 0x56, 0x66, 0x6c, 0x61,
        0x67, 0x0a, 0x70, 0x37, 0x0a, 0x28, 0x49, 0x30, 0x31, 0x0a, 0x4e, 0x74, 0x70, 0x38, 0x0a,
        0x74, 0x70, 0x39, 0x0a, 0x61, 0x2e,
    ];

    /// Python 3.14.7: pickle.dumps([('a.b', b'x')], protocol=0) -- a `bytes` value at protocol 0
    /// is `GLOBAL` 0x63 `_codecs encode` and `REDUCE` 0x52.
    const CPYTHON3_PROTOCOL_0_BYTES: &[u8] = &[
        0x28, 0x6c, 0x70, 0x30, 0x0a, 0x28, 0x56, 0x61, 0x2e, 0x62, 0x0a, 0x70, 0x31, 0x0a, 0x63,
        0x5f, 0x63, 0x6f, 0x64, 0x65, 0x63, 0x73, 0x0a, 0x65, 0x6e, 0x63, 0x6f, 0x64, 0x65, 0x0a,
        0x70, 0x32, 0x0a, 0x28, 0x56, 0x78, 0x0a, 0x70, 0x33, 0x0a, 0x56, 0x6c, 0x61, 0x74, 0x69,
        0x6e, 0x31, 0x0a, 0x70, 0x34, 0x0a, 0x74, 0x70, 0x35, 0x0a, 0x52, 0x70, 0x36, 0x0a, 0x74,
        0x70, 0x37, 0x0a, 0x61, 0x2e,
    ];

    /// Python 2.7.18 (`python:2.7-slim`), Diamond's `GraphitePickleHandler` call: p = 'sys.cpu';
    /// cPickle.dumps([(p, (1700000000, 0.5)), ('caf\xc3\xa9.x', (1700000001L, float('nan'))),
    /// (p, (1700000002, 1.5)), ('flag', (True, 1.0))]) -- `cPickle` numbers its memo from 1
    /// (`lp1`), escapes a UTF-8 path as `\xc3\xa9`, reuses `p` through `GET` (`g2`), and writes a
    /// `long` as `L…L` and `True` as `I01`.
    const CPYTHON2_CPICKLE_PROTOCOL_0: &[u8] = &[
        0x28, 0x6c, 0x70, 0x31, 0x0a, 0x28, 0x53, 0x27, 0x73, 0x79, 0x73, 0x2e, 0x63, 0x70, 0x75,
        0x27, 0x0a, 0x70, 0x32, 0x0a, 0x28, 0x49, 0x31, 0x37, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x0a, 0x46, 0x30, 0x2e, 0x35, 0x0a, 0x74, 0x70, 0x33, 0x0a, 0x74, 0x70, 0x34,
        0x0a, 0x61, 0x28, 0x53, 0x27, 0x63, 0x61, 0x66, 0x5c, 0x78, 0x63, 0x33, 0x5c, 0x78, 0x61,
        0x39, 0x2e, 0x78, 0x27, 0x0a, 0x70, 0x35, 0x0a, 0x28, 0x4c, 0x31, 0x37, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30, 0x30, 0x31, 0x4c, 0x0a, 0x46, 0x6e, 0x61, 0x6e, 0x0a, 0x74, 0x74, 0x70,
        0x36, 0x0a, 0x61, 0x28, 0x67, 0x32, 0x0a, 0x28, 0x49, 0x31, 0x37, 0x30, 0x30, 0x30, 0x30,
        0x30, 0x30, 0x30, 0x32, 0x0a, 0x46, 0x31, 0x2e, 0x35, 0x0a, 0x74, 0x70, 0x37, 0x0a, 0x74,
        0x70, 0x38, 0x0a, 0x61, 0x28, 0x53, 0x27, 0x66, 0x6c, 0x61, 0x67, 0x27, 0x0a, 0x70, 0x39,
        0x0a, 0x28, 0x49, 0x30, 0x31, 0x0a, 0x46, 0x31, 0x0a, 0x74, 0x74, 0x70, 0x31, 0x30, 0x0a,
        0x61, 0x2e,
    ];

    /// Python 2.7.18 (`python:2.7-slim`): cPickle.dumps([('sys.cpu', (1700000000, 0.5))], 2) --
    /// binary, but its memo also starts at 1 (`BINPUT 1`), as carbon's own client writes it on
    /// Python 2.
    const CPYTHON2_CPICKLE_PROTOCOL_2: &[u8] = &[
        0x80, 0x02, 0x5d, 0x71, 0x01, 0x55, 0x07, 0x73, 0x79, 0x73, 0x2e, 0x63, 0x70, 0x75, 0x71,
        0x02, 0x4a, 0x00, 0xf1, 0x53, 0x65, 0x47, 0x3f, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x86, 0x71, 0x03, 0x86, 0x71, 0x04, 0x61, 0x2e,
    ];

    /// Dropwizard Metrics 4.2.25 `PickledGraphite.pickleMetrics`'s spelling, written out by hand
    /// from its source for two `send(name, value, timestamp)` calls: no memo, a `LONG` second, the
    /// `%2.2f` value as a quoted `STRING`, and a name of raw UTF-8 (`é` is 0xc3 0xa9) that the
    /// writer never escapes. `testdata/interop/graphite/graphite-dropwizard-000.raw` is a real
    /// capture of the same shape.
    const DROPWIZARD_PICKLED_GRAPHITE: &[u8] =
        b"(l(S'jvm.heap.used'\n(L1700000000L\nS'12.50'\ntta(S'caf\xc3\xa9.count'\n(L1700000000L\nS'NaN'\ntta.";

    /// pickle.dumps([('a.b', (1, 1.0))], protocol=1) -- no `PROTO` header, `TUPLE` 0x74 rather
    /// than `TUPLE2`, but only binary opcodes.
    const CPYTHON_PROTOCOL_1: &[u8] = &[
        0x5d, 0x71, 0x00, 0x28, 0x58, 0x03, 0x00, 0x00, 0x00, 0x61, 0x2e, 0x62, 0x71, 0x01, 0x28,
        0x4b, 0x01, 0x47, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x74, 0x71, 0x02, 0x74,
        0x71, 0x03, 0x61, 0x2e,
    ];

    /// pickle.dumps({'a': 1}, protocol=2) -- `EMPTY_DICT` 0x7d, `SETITEM` 0x73.
    const CPYTHON_DICT: &[u8] = &[
        0x80, 0x02, 0x7d, 0x71, 0x00, 0x58, 0x01, 0x00, 0x00, 0x00, 0x61, 0x71, 0x01, 0x4b, 0x01,
        0x73, 0x2e,
    ];

    /// class Thing: pass; pickle.dumps(Thing(), protocol=2) -- `GLOBAL` 0x63 then `NEWOBJ` 0x81,
    /// the shape that makes a general unpickler construct an arbitrary object.
    const CPYTHON_GLOBAL: &[u8] = &[
        0x80, 0x02, 0x63, 0x5f, 0x5f, 0x6d, 0x61, 0x69, 0x6e, 0x5f, 0x5f, 0x0a, 0x54, 0x68, 0x69,
        0x6e, 0x67, 0x0a, 0x71, 0x00, 0x29, 0x81, 0x71, 0x01, 0x2e,
    ];

    /// class Thing: pass; pickle.dumps(Thing(), protocol=4) -- the same thing through
    /// `STACK_GLOBAL` 0x93.
    const CPYTHON_STACK_GLOBAL: &[u8] = &[
        0x80, 0x04, 0x95, 0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x8c, 0x08, 0x5f, 0x5f,
        0x6d, 0x61, 0x69, 0x6e, 0x5f, 0x5f, 0x94, 0x8c, 0x05, 0x54, 0x68, 0x69, 0x6e, 0x67, 0x94,
        0x93, 0x94, 0x29, 0x81, 0x94, 0x2e,
    ];

    /// pickle.dumps([('a.b', ('1700000000', '2.5'))], protocol=2) -- a producer that read its
    /// numbers out of a text source and never coerced them.
    const CPYTHON_NUMERIC_STRINGS: &[u8] = &[
        0x80, 0x02, 0x5d, 0x71, 0x00, 0x58, 0x03, 0x00, 0x00, 0x00, 0x61, 0x2e, 0x62, 0x71, 0x01,
        0x58, 0x0a, 0x00, 0x00, 0x00, 0x31, 0x37, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30, 0x30,
        0x71, 0x02, 0x58, 0x03, 0x00, 0x00, 0x00, 0x32, 0x2e, 0x35, 0x71, 0x03, 0x86, 0x71, 0x04,
        0x86, 0x71, 0x05, 0x61, 0x2e,
    ];

    /// pickle.dumps([('a.b', (1, 1.0)), None, ('c.d', (2, 2.0))], protocol=2) -- a stray `NONE`
    /// (0x4e) between two good datapoints.
    const CPYTHON_WRONG_SHAPE: &[u8] = &[
        0x80, 0x02, 0x5d, 0x71, 0x00, 0x28, 0x58, 0x03, 0x00, 0x00, 0x00, 0x61, 0x2e, 0x62, 0x71,
        0x01, 0x4b, 0x01, 0x47, 0x3f, 0xf0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x71, 0x02,
        0x86, 0x71, 0x03, 0x4e, 0x58, 0x03, 0x00, 0x00, 0x00, 0x63, 0x2e, 0x64, 0x71, 0x04, 0x4b,
        0x02, 0x47, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x71, 0x05, 0x86, 0x71,
        0x06, 0x65, 0x2e,
    ];

    /// pickle.dumps({1, 2}, protocol=4) -- `EMPTY_SET` 0x8f, `ADDITEMS` 0x90.
    const CPYTHON_SET: &[u8] = &[
        0x80, 0x04, 0x95, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x8f, 0x94, 0x28, 0x4b,
        0x01, 0x4b, 0x02, 0x90, 0x2e,
    ];

    /// pickle.dumps([('sys.cpu;env=prod;host=web-1', (1700000000, 0.5))], protocol=2) -- carbon
    /// 1.1+'s tagged series ride the pickle protocol as an ordinary path string.
    const CPYTHON_TAGGED_PATH: &[u8] = &[
        0x80, 0x02, 0x5d, 0x71, 0x00, 0x58, 0x1b, 0x00, 0x00, 0x00, 0x73, 0x79, 0x73, 0x2e, 0x63,
        0x70, 0x75, 0x3b, 0x65, 0x6e, 0x76, 0x3d, 0x70, 0x72, 0x6f, 0x64, 0x3b, 0x68, 0x6f, 0x73,
        0x74, 0x3d, 0x77, 0x65, 0x62, 0x2d, 0x31, 0x71, 0x01, 0x4a, 0x00, 0xf1, 0x53, 0x65, 0x47,
        0x3f, 0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x71, 0x02, 0x86, 0x71, 0x03, 0x61,
        0x2e,
    ];

    // -- helpers ---------------------------------------------------------------------------------

    /// One decoded datapoint: `(path, timestamp, value)`, owned so a test can compare it directly.
    type Point = (String, f64, f64);

    /// Every datapoint in `payload`, plus how many items were skipped for being the wrong shape.
    fn read(payload: &[u8]) -> Result<(Vec<Point>, usize), CodecError> {
        let mut reader = PickleReader::new();
        let mut out = Vec::new();
        let skipped = reader.read_datapoints(payload, |path, timestamp, value| {
            out.push((path.to_string(), timestamp, value));
        })?;
        Ok((out, skipped))
    }

    fn point(path: &str, timestamp: f64, value: f64) -> Point {
        (path.to_string(), timestamp, value)
    }

    // -- accepted payloads ------------------------------------------------------------------------

    #[test]
    fn a_cpython_protocol_2_dump_decodes() {
        let (points, skipped) = read(CPYTHON_PROTOCOL_2).expect("protocol 2 must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("sys.cpu", 1_700_000_000.0, 0.5)]);
    }

    /// Protocol `-1` (protocol 5 on a modern CPython) decodes, with `FRAME`/`SHORT_BINUNICODE`/
    /// `MEMOIZE`.
    #[test]
    fn a_cpython_protocol_5_dump_decodes() {
        let (points, skipped) = read(CPYTHON_PROTOCOL_5).expect("protocol 5 must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("sys.cpu", 1_700_000_000.0, 0.5)]);
    }

    /// Protocol 1 has no `PROTO` but uses only allowlisted binary opcodes, so it decodes.
    #[test]
    fn a_cpython_protocol_1_dump_decodes() {
        let (points, skipped) = read(CPYTHON_PROTOCOL_1).expect("protocol 1 must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("a.b", 1.0, 1.0)]);
    }

    #[test]
    fn a_repeated_path_memoized_by_the_sender_decodes_through_binget() {
        assert!(CPYTHON_MEMOIZED_PATH.contains(&OP_BINGET), "fixture must exercise BINGET");
        let (points, skipped) = read(CPYTHON_MEMOIZED_PATH).expect("a memoized path must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("a.b", 1.0, 1.0), point("a.b", 2.0, 2.0)]);
    }

    #[test]
    fn a_long1_timestamp_past_2038_decodes() {
        assert!(CPYTHON_LONG1_TIMESTAMP.contains(&OP_LONG1), "fixture must exercise LONG1");
        let (points, skipped) = read(CPYTHON_LONG1_TIMESTAMP).expect("a LONG1 second must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("a.b", 2_147_483_653.0, 1.0)]);
    }

    /// Numeric strings decode, as carbon coerces them with `float()`.
    #[test]
    fn numeric_strings_parse_as_numbers() {
        let (points, skipped) = read(CPYTHON_NUMERIC_STRINGS).expect("numeric strings must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("a.b", 1_700_000_000.0, 2.5)]);
    }

    /// A stray `None` costs **that** datapoint, not the frame.
    #[test]
    fn a_wrong_shaped_item_is_skipped_and_the_rest_of_the_frame_decodes() {
        let (points, skipped) = read(CPYTHON_WRONG_SHAPE).expect("a stray None must not be fatal");
        assert_eq!(skipped, 1);
        assert_eq!(points, vec![point("a.b", 1.0, 1.0), point("c.d", 2.0, 2.0)]);
    }

    #[test]
    fn a_tagged_path_rides_through_unchanged() {
        let (points, _) = read(CPYTHON_TAGGED_PATH).expect("a tagged path must decode");
        assert_eq!(points, vec![point("sys.cpu;env=prod;host=web-1", 1_700_000_000.0, 0.5)]);
    }

    // -- protocol 0 -------------------------------------------------------------------------------

    /// `(path, timestamp, value)` with the floats compared by bits, so a NaN compares equal.
    fn bits(points: &[Point]) -> Vec<(String, u64, u64)> {
        points.iter().map(|(p, t, v)| (p.clone(), t.to_bits(), v.to_bits())).collect()
    }

    /// A frame of one datapoint whose path is the protocol-0 string opcode `path_op` (opcode,
    /// argument, and `\n`), and the path the reader yields from it.
    fn protocol_0_path(path_op: &[u8]) -> Result<String, CodecError> {
        let mut payload = b"(l(".to_vec();
        payload.extend_from_slice(path_op);
        payload.extend_from_slice(b"(I1\nF2.5\ntta.");
        let (points, skipped) = read(&payload)?;
        assert_eq!(skipped, 0, "{payload:?}");
        assert_eq!(points.len(), 1, "{payload:?}");
        Ok(points[0].0.clone())
    }

    #[test]
    fn a_cpython_protocol_0_dump_decodes() {
        let (points, skipped) = read(CPYTHON_PROTOCOL_0).expect("protocol 0 must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("a.b", 1.0, 1.0)]);
    }

    /// Python 3's protocol 0: `UNICODE` with Latin-1 and `\u` escapes, `LONG`, `F nan`, `I01`.
    #[test]
    fn a_cpython_3_protocol_0_dump_with_escapes_decodes() {
        let (points, skipped) =
            read(CPYTHON3_PROTOCOL_0_ESCAPED).expect("python 3 protocol 0 must decode");
        assert_eq!(skipped, 1, "('flag', (True, None)) is the wrong shape");
        assert_eq!(
            bits(&points),
            bits(&[
                point("sys.cpu", 1_700_000_000.0, 0.5),
                point("caf\u{e9}.\u{20ac}\\x", 1_099_511_627_776.0, f64::NAN),
            ])
        );
    }

    /// Diamond's call on Python 2: `cPickle`'s memo from 1, `\x` escapes, `GET`, `L…L`, `I01`.
    #[test]
    fn a_python_2_cpickle_protocol_0_dump_decodes() {
        let (points, skipped) =
            read(CPYTHON2_CPICKLE_PROTOCOL_0).expect("python 2 cPickle protocol 0 must decode");
        assert_eq!(skipped, 1, "('flag', (True, 1.0)): a bool timestamp is the wrong shape");
        assert_eq!(
            bits(&points),
            bits(&[
                point("sys.cpu", 1_700_000_000.0, 0.5),
                point("caf\u{e9}.x", 1_700_000_001.0, f64::NAN),
                point("sys.cpu", 1_700_000_002.0, 1.5),
            ])
        );
    }

    /// Python 2's `cPickle` numbers its memo from 1 in the binary protocols too.
    #[test]
    fn a_python_2_cpickle_protocol_2_dump_decodes() {
        let (points, skipped) =
            read(CPYTHON2_CPICKLE_PROTOCOL_2).expect("python 2 cPickle protocol 2 must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("sys.cpu", 1_700_000_000.0, 0.5)]);
    }

    /// Slot 0 is the one slot a memo may skip: key 2 on an empty memo still fails.
    #[test]
    fn a_memo_key_past_slot_1_on_an_empty_memo_is_rejected() {
        for payload in [&[OP_PROTO, 2, OP_EMPTY_LIST, OP_BINPUT, 2, OP_STOP][..], b"(lp2\n."] {
            let err = read(payload).expect_err("key 2 on an empty memo must fail");
            assert!(err.to_string().contains("skips ahead"), "{err}");
        }
        let (points, _) = read(b"(lp1\n.").expect("key 1 on an empty memo is cPickle's first");
        assert!(points.is_empty());
    }

    /// After a first `PUT 1` or `BINPUT 1` skips slot 0, `MEMOIZE` keys by filled slots, as
    /// CPython does, so it writes slot 1 and a later `GET 1` reads `c.d`. CPython 3.14.7's
    /// `pickle.loads` reads both payloads as `a.b`, `c.d`, `c.d`. Hand-assembled: no pickler
    /// writes `MEMOIZE` after a skipped slot.
    #[test]
    fn memoize_after_a_skipped_slot_0_keys_by_filled_slots() {
        for payload in [
            &b"\x80\x04]((\x8c\x03a.bp1\nK1K2\x86t(\x8c\x03c.d\x94K1K2\x86t(g1\nK1K2\x86te."[..],
            b"\x80\x04]((\x8c\x03a.bq\x01K1K2\x86t(\x8c\x03c.d\x94K1K2\x86t(h\x01K1K2\x86te.",
        ] {
            let (points, skipped) = read(payload).expect("the payload must decode");
            assert_eq!(skipped, 0);
            let paths: Vec<&str> = points.iter().map(|p| p.0.as_str()).collect();
            assert_eq!(paths, ["a.b", "c.d", "c.d"], "{payload:?}");
        }
    }

    #[test]
    fn a_dropwizard_pickled_graphite_payload_decodes() {
        let (points, skipped) =
            read(DROPWIZARD_PICKLED_GRAPHITE).expect("Dropwizard's spelling must decode");
        assert_eq!(skipped, 0);
        assert_eq!(
            bits(&points),
            bits(&[
                point("jvm.heap.used", 1_700_000_000.0, 12.5),
                point("caf\u{e9}.count", 1_700_000_000.0, f64::NAN),
            ])
        );
    }

    /// A string with nothing to decode stays a range into the frame, as a binary string does.
    #[test]
    fn a_protocol_0_string_with_nothing_to_decode_borrows_the_frame() {
        // One binding, so the frame and the range check name the same bytes.
        let frame = DROPWIZARD_PICKLED_GRAPHITE;
        let mut reader = PickleReader::new();
        let mut within = Vec::new();
        reader
            .read_datapoints(frame, |path, _, _| {
                within.push(frame.as_ptr_range().contains(&path.as_ptr()));
            })
            .expect("Dropwizard's spelling must decode");
        assert_eq!(within, vec![true, true]);
        assert!(reader.scratch.is_empty(), "nothing was decoded into the scratch");
    }

    /// `STRING` escapes, as `codecs.escape_decode` reads them.
    #[test]
    fn string_escapes_decode_as_python_decodes_them() {
        for (arg, want) in [
            (&b"S'a\\\\b'\n"[..], "a\\b"),
            (b"S'it\\'s'\n", "it's"),
            (b"S\"say \\\"hi\\\"\"\n", "say \"hi\""),
            (b"S'\\a\\b\\f\\n\\r\\t\\v'\n", "\x07\x08\x0c\n\r\t\x0b"),
            (b"S'\\x41\\x7a'\n", "Az"),
            (b"S'\\101\\7\\0'\n", "A\x07\0"),
            // A value past `\377` keeps its low byte: `\501` is 0x141, read as 0x41.
            (b"S'\\501'\n", "A"),
            // An octal escape takes at most three digits.
            (b"S'\\1011'\n", "A1"),
            // An unknown escape keeps its backslash.
            (b"S'\\q\\8'\n", "\\q\\8"),
            // Raw UTF-8, as Dropwizard writes a name, passes through.
            (b"S'caf\xc3\xa9'\n", "caf\u{e9}"),
            // Python 2's `repr` uses `"` for a string holding a `'`, and an unescaped quote
            // inside is still part of the body.
            (b"S\"it's\"\n", "it's"),
            (b"S''\n", ""),
        ] {
            assert_eq!(protocol_0_path(arg).as_deref().ok(), Some(want), "{arg:?}");
        }
    }

    /// `UNICODE` escapes, as `raw_unicode_escape` reads them.
    #[test]
    fn unicode_escapes_decode_as_python_decodes_them() {
        for (arg, want) in [
            (&b"Va\\u0041\n"[..], "aA"),
            (b"V\\U0001f600\n", "\u{1f600}"),
            (b"Vcaf\xe9\n", "caf\u{e9}"),
            // Only `\u`/`\U` are escapes; a pair of backslashes leaves the `u` literal.
            (b"V\\\\u0041\n", "\\\\u0041"),
            (b"Va\\qb\\\n", "a\\qb\\"),
            (b"V\\u005cx\n", "\\x"),
        ] {
            assert_eq!(protocol_0_path(arg).as_deref().ok(), Some(want), "{arg:?}");
        }
    }

    #[test]
    fn protocol_0_numbers_read_as_python_reads_them() {
        for (number_ops, timestamp, value) in [
            (&b"I-5\nF-2.5\n"[..], -5.0, -2.5),
            (b"I+7\nF1e+06\n", 7.0, 1e6),
            (b"I0\nFinf\n", 0.0, f64::INFINITY),
            (b"L42L\nF.5\n", 42.0, 0.5),
            (b"L42\nF-inf\n", 42.0, f64::NEG_INFINITY),
            (b"L00L\nF1\n", 0.0, 1.0),
            (b"L-9223372036854775808L\nF0\n", i64::MIN as f64, 0.0),
        ] {
            let mut payload = b"(l(S'a'\n(".to_vec();
            payload.extend_from_slice(number_ops);
            payload.extend_from_slice(b"tta.");
            let (points, skipped) = read(&payload).expect("a protocol-0 number must decode");
            assert_eq!(skipped, 0, "{payload:?}");
            assert_eq!(bits(&points), bits(&[point("a", timestamp, value)]), "{payload:?}");
        }
    }

    /// `I00`/`I01` are bools, which a carbon datapoint can't use, so the item is skipped.
    #[test]
    fn i00_and_i01_are_bools() {
        for flag in [&b"I00\n"[..], b"I01\n"] {
            let mut payload = b"(l(S'a'\n(".to_vec();
            payload.extend_from_slice(flag);
            payload.extend_from_slice(b"F1\ntta.");
            let (points, skipped) = read(&payload).expect("a bool timestamp is a wrong shape");
            assert_eq!((points.len(), skipped), (0, 1), "{payload:?}");
        }
    }

    /// Every protocol-0 argument this reader refuses fails the frame, with the reason.
    #[test]
    fn malformed_protocol_0_arguments_fail_the_frame() {
        for (path_op, want) in [
            (&b"S'abc\n"[..], "not quoted"),
            (b"S'abc\"\n", "not quoted"),
            (b"S'\n", "not quoted"),
            (b"Sabc\n", "not quoted"),
            (b"S'\\x4'\n", "invalid \\x escape"),
            (b"S'\\xzz'\n", "invalid \\x escape"),
            (b"S'abc\\'\n", "lone backslash"),
            (b"S'\xff'\n", "not valid utf-8"),
            (b"S'\\xff'\n", "not valid utf-8"),
            (b"V\\ud800\n", "surrogate"),
            (b"V\\U00110000\n", "past U+10FFFF"),
            (b"V\\u12\n", "truncated"),
            (b"V\\u12zz\n", "non-hex"),
        ] {
            let err = protocol_0_path(path_op).expect_err("a malformed string must fail");
            assert!(err.to_string().contains(want), "{path_op:?}: {err}");
        }
        for (number_ops, want) in [
            (&b"I999999999999999999999999999999999999999\nF1\n"[..], "does not fit"),
            (b"L170141183460469231731687303715884105728L\nF1\n", "does not fit"),
            (b"I010\nF1\n", "not a decimal"),
            (b"I0x10\nF1\n", "not a decimal"),
            (b"I1_000\nF1\n", "not a decimal"),
            (b"I 1\nF1\n", "not a decimal"),
            (b"I\nF1\n", "not a decimal"),
            (b"L\nF1\n", "not a decimal"),
            (b"LL\nF1\n", "not a decimal"),
            (b"I1\nF1e999\n", "out of range"),
            (b"I1\nF1.0x\n", "not a float"),
            (b"I1\nF1", "no terminating newline"),
        ] {
            let mut payload = b"(l(S'a'\n(".to_vec();
            payload.extend_from_slice(number_ops);
            payload.extend_from_slice(b"tta.");
            let err = read(&payload).expect_err("a malformed number must fail");
            assert!(err.to_string().contains(want), "{payload:?}: {err}");
        }
        for (memo_op, want) in [
            (&b"p-1\n"[..], "not a decimal"),
            (b"p\n", "not a decimal"),
            (b"p99999999999999999999999\n", "not a decimal"),
            (b"g5\n", "was never set"),
        ] {
            let mut payload = b"(l".to_vec();
            payload.extend_from_slice(memo_op);
            payload.push(OP_STOP);
            let err = read(&payload).expect_err("a malformed memo key must fail");
            assert!(err.to_string().contains(want), "{payload:?}: {err}");
        }
    }

    /// A decoded `UNICODE` at most doubles: every byte at 0x80 or above becomes two UTF-8 bytes.
    #[test]
    fn the_scratch_never_exceeds_twice_the_frame() {
        let mut payload = b"(l(V".to_vec();
        payload.extend(std::iter::repeat_n(0xe9u8, 4096));
        payload.extend_from_slice(b"\n(I1\nF1\ntta.");
        let mut reader = PickleReader::new();
        let mut paths = 0;
        reader
            .read_datapoints(&payload, |path, _, _| {
                assert_eq!(path.chars().count(), 4096);
                paths += 1;
            })
            .expect("a Latin-1 path must decode");
        assert_eq!(paths, 1);
        assert_eq!(reader.scratch.len(), 2 * 4096);
        assert!(reader.scratch.len() <= 2 * payload.len());
    }

    // -- rejected payloads ------------------------------------------------------------------------

    /// Every opcode that could make a general unpickler construct or call something is refused,
    /// with greppable wording.
    #[test]
    fn object_construction_opcodes_are_rejected() {
        for (name, payload) in [
            ("GLOBAL", CPYTHON_GLOBAL),
            ("STACK_GLOBAL", CPYTHON_STACK_GLOBAL),
            ("dict", CPYTHON_DICT),
            ("set", CPYTHON_SET),
            ("protocol-0 bytes", CPYTHON3_PROTOCOL_0_BYTES),
        ] {
            let err = read(payload).expect_err("{name} must be rejected");
            let message = err.to_string();
            assert!(
                message.contains("is not permitted"),
                "{name} was rejected as {message:?}, not as a forbidden opcode"
            );
        }
    }

    /// `REDUCE` (0x52) and `BUILD` (0x62), hand-assembled: a stock `pickle.dumps` of a plain
    /// class uses `NEWOBJ` instead.
    #[test]
    fn reduce_and_build_are_rejected() {
        for (name, op) in [("REDUCE", 0x52u8), ("BUILD", 0x62u8)] {
            let payload = [OP_PROTO, 2, OP_EMPTY_TUPLE, op, OP_STOP];
            let err = read(&payload).expect_err("{name} must be rejected");
            assert!(
                err.to_string().contains(&format!("{op:#04x} is not permitted")),
                "{name} must be rejected by opcode"
            );
        }
    }

    #[test]
    fn the_rejection_message_names_the_opcode_in_hex() {
        let payload = [OP_PROTO, 2, 0x63, OP_STOP];
        let err = read(&payload).expect_err("GLOBAL must be rejected");
        assert_eq!(err.to_string(), "malformed input: pickle opcode 0x63 is not permitted");
    }

    #[test]
    fn a_payload_that_does_not_end_in_one_list_is_rejected() {
        // `PROTO 2, BININT1 1, STOP` -- one value, but an integer rather than a list.
        let payload = [OP_PROTO, 2, OP_BININT1, 1, OP_STOP];
        let err = read(&payload).expect_err("a non-list payload must be rejected");
        assert!(err.to_string().contains("not a list of datapoints"), "{err}");

        // Two values left on the stack.
        let mut payload = vec![OP_PROTO, 2, OP_EMPTY_LIST, OP_EMPTY_LIST];
        payload.push(OP_STOP);
        let err = read(&payload).expect_err("two stack values must be rejected");
        assert!(err.to_string().contains("not exactly one"), "{err}");
    }

    // -- bounds -----------------------------------------------------------------------------------

    #[test]
    fn nesting_past_the_depth_cap_is_rejected() {
        let mut payload = vec![OP_PROTO, 2];
        payload.extend(std::iter::repeat_n(OP_MARK, MAX_PICKLE_DEPTH));
        payload.push(OP_STOP);
        // At the cap the depth check passes; the marks left at STOP fail "exactly one list".
        let err = read(&payload).expect_err("marks left on the stack must fail");
        assert!(!err.to_string().contains("nesting exceeds"), "at the cap, depth must not fire");

        payload.pop();
        payload.push(OP_MARK);
        payload.push(OP_STOP);
        let err = read(&payload).expect_err("one mark past the cap must fail");
        assert!(err.to_string().contains("nesting exceeds"), "{err}");
    }

    #[test]
    fn a_stack_past_the_item_cap_is_rejected() {
        let mut payload = vec![OP_PROTO, 2];
        for _ in 0..=MAX_PICKLE_ITEMS {
            payload.push(OP_BININT1);
            payload.push(1);
        }
        payload.push(OP_STOP);
        let err = read(&payload).expect_err("an over-cap stack must be rejected");
        assert!(err.to_string().contains("stack exceeds"), "{err}");
    }

    /// A declared length far past the input is rejected (`crates/logit-proto/tests/robustness.rs`
    /// checks that the reject allocates nothing).
    #[test]
    fn an_inflated_string_length_is_rejected_before_anything_is_sized_from_it() {
        let mut payload = vec![OP_PROTO, 2, OP_BINUNICODE];
        payload.extend_from_slice(&u32::MAX.to_le_bytes());
        payload.extend_from_slice(b"short");
        payload.push(OP_STOP);
        let err = read(&payload).expect_err("an inflated BINUNICODE length must be rejected");
        assert!(err.to_string().contains("runs past the payload"), "{err}");
    }

    #[test]
    fn a_long_magnitude_past_sixteen_bytes_is_rejected() {
        let mut payload = vec![OP_PROTO, 2, OP_LONG1, 17];
        payload.extend_from_slice(&[0u8; 17]);
        payload.push(OP_STOP);
        let err = read(&payload).expect_err("a 17-byte long must be rejected");
        assert!(err.to_string().contains("magnitude cap"), "{err}");
    }

    #[test]
    fn a_frame_declaring_more_than_it_carries_is_rejected() {
        let mut payload = vec![OP_PROTO, 5, OP_FRAME];
        payload.extend_from_slice(&1_000_000u64.to_le_bytes());
        payload.push(OP_STOP);
        let err = read(&payload).expect_err("an inflated FRAME length must be rejected");
        assert!(err.to_string().contains("FRAME declares"), "{err}");
    }

    #[test]
    fn a_memo_key_that_was_never_set_is_rejected() {
        let payload = [OP_PROTO, 2, OP_BINGET, 7, OP_STOP];
        let err = read(&payload).expect_err("an unset memo key must be rejected");
        assert!(err.to_string().contains("was never set"), "{err}");
    }

    /// `PROTO 2, EMPTY_LIST, LONG_BINPUT 499999, STOP`: a key that skips ahead is rejected by
    /// `memo_put`'s ordinal check before the memo grows to ~8 MB. The allocation side is
    /// `crates/logit-proto/tests/robustness.rs`'s
    /// `graphite_pickle_never_allocates_from_a_corrupt_memo_key`.
    #[test]
    fn a_memo_key_that_skips_ahead_is_rejected() {
        let payload = [OP_PROTO, 2, OP_EMPTY_LIST, OP_LONG_BINPUT, 0x1f, 0xa1, 0x07, 0x00, OP_STOP];
        let err = read(&payload).expect_err("a memo key past the entries written so far must fail");
        assert!(err.to_string().contains("skips ahead"), "{err}");
    }

    /// Overwriting a memo slot (`BINPUT` 0 on the path, then again on the tuple) still decodes.
    #[test]
    fn a_memo_key_overwriting_an_existing_slot_still_decodes() {
        let payload = [
            OP_PROTO,
            2,
            OP_EMPTY_LIST,
            OP_MARK,
            OP_BINUNICODE,
            0x03,
            0x00,
            0x00,
            0x00,
            b'a',
            b'.',
            b'b',
            OP_BINPUT,
            0x00, // memo[0] = "a.b" (append: key == memo.len() == 0)
            OP_BININT1,
            0x01,
            OP_BINFLOAT,
            0x3f,
            0xf0,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00, // 1.0
            OP_TUPLE2,
            OP_TUPLE2,
            OP_BINPUT,
            0x00, // memo[0] = the full tuple (overwrite: key 0 < memo.len() 1)
            OP_APPENDS,
            OP_STOP,
        ];
        let (points, skipped) = read(&payload).expect("overwriting an existing memo slot decodes");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("a.b", 1.0, 1.0)]);
    }

    /// `LONG_BINPUT` appending slots in order, as a real batch does, is accepted.
    #[test]
    fn a_memo_key_appending_the_next_slot_still_decodes() {
        let payload = [
            OP_PROTO,
            2,
            OP_EMPTY_LIST,
            OP_MARK,
            OP_BINUNICODE,
            0x03,
            0x00,
            0x00,
            0x00,
            b'x',
            b'.',
            b'y',
            OP_LONG_BINPUT,
            0x00,
            0x00,
            0x00,
            0x00, // memo[0] = "x.y" (append: key == memo.len() == 0)
            OP_BININT1,
            0x05,
            OP_BINFLOAT,
            0x40,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00, // 2.0
            OP_TUPLE2,
            OP_TUPLE2,
            OP_LONG_BINPUT,
            0x01,
            0x00,
            0x00,
            0x00, // memo[1] = the full tuple (append: key == memo.len() == 1)
            OP_APPENDS,
            OP_STOP,
        ];
        let (points, skipped) = read(&payload).expect("appending the next memo slot decodes");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("x.y", 5.0, 2.0)]);
    }

    /// Every prefix of every fixture, accepted or rejected: the reader must return, never panic.
    #[test]
    fn every_single_byte_truncation_of_every_fixture_fails_without_panicking() {
        for payload in [
            CPYTHON_PROTOCOL_2,
            CPYTHON_PROTOCOL_5,
            CPYTHON_MEMOIZED_PATH,
            CPYTHON_LONG1_TIMESTAMP,
            CPYTHON_WRONG_SHAPE,
            CPYTHON_GLOBAL,
            CPYTHON_PROTOCOL_0,
            CPYTHON3_PROTOCOL_0_ESCAPED,
            CPYTHON2_CPICKLE_PROTOCOL_0,
            CPYTHON2_CPICKLE_PROTOCOL_2,
            DROPWIZARD_PICKLED_GRAPHITE,
        ] {
            for len in 0..payload.len() {
                let truncated = &payload[..len];
                let result =
                    std::panic::catch_unwind(AssertUnwindSafe(|| read(truncated).is_err()));
                match result {
                    Ok(failed) => assert!(failed, "a {len}-byte truncation decoded successfully"),
                    Err(_) => panic!("a {len}-byte truncation panicked"),
                }
            }
        }
    }

    // -- the writer ---------------------------------------------------------------------------------

    #[test]
    fn write_then_read_round_trips() {
        let mut payload = Vec::new();
        write_datapoints(
            &mut payload,
            [
                ("sys.cpu", 1_700_000_000i64, 0.5f64),
                ("sys.mem;host=web-1", 1_700_000_001, -2.25),
                ("far.future", 2_147_483_653, 1.0),
            ],
        );
        let (points, skipped) = read(&payload).expect("our own writer must read back");
        assert_eq!(skipped, 0);
        assert_eq!(
            points,
            vec![
                point("sys.cpu", 1_700_000_000.0, 0.5),
                point("sys.mem;host=web-1", 1_700_000_001.0, -2.25),
                point("far.future", 2_147_483_653.0, 1.0),
            ]
        );
    }

    /// A CPython dump and this writer's output decode the same, though the bytes differ (CPython
    /// memoizes strings and uses `APPEND` for a one-element list).
    #[test]
    fn a_cpython_dump_and_our_writer_decode_identically() {
        let mut ours = Vec::new();
        write_datapoints(&mut ours, [("sys.cpu", 1_700_000_000i64, 0.5f64)]);
        assert_eq!(read(&ours).unwrap(), read(CPYTHON_PROTOCOL_2).unwrap());
        assert_eq!(read(&ours).unwrap(), read(CPYTHON_PROTOCOL_5).unwrap());
    }

    #[test]
    fn a_long1_timestamp_matches_cpythons_own_minimal_encoding() {
        // CPython's bytes for 2**31 + 5: `LONG1`, length 5, then the minimal little-endian
        // magnitude with the `0x00` that keeps it positive.
        let mut out = Vec::new();
        write_int(&mut out, 2_147_483_653);
        assert_eq!(out, vec![OP_LONG1, 0x05, 0x05, 0x00, 0x00, 0x80, 0x00]);
        assert!(
            CPYTHON_LONG1_TIMESTAMP.windows(out.len()).any(|w| w == out),
            "CPython's own dump must contain exactly these bytes"
        );
    }

    /// The writer emits only its **ten-opcode** subset.
    #[test]
    fn the_writer_emits_only_the_ten_permitted_opcodes() {
        const PERMITTED: [u8; 10] = [
            OP_PROTO,
            OP_EMPTY_LIST,
            OP_MARK,
            OP_BINUNICODE,
            OP_BININT,
            OP_LONG1,
            OP_BINFLOAT,
            OP_TUPLE2,
            OP_APPENDS,
            OP_STOP,
        ];

        let mut payload = Vec::new();
        write_datapoints(
            &mut payload,
            [("a.b", 1_700_000_000i64, 0.5f64), ("c.d", 2_147_483_653, -1.0)],
        );

        // Walks operand widths rather than scanning bytes, so an opcode-valued byte inside a
        // string or float can't be mistaken for an opcode, or hide one.
        let mut at = 0usize;
        let mut seen = Vec::new();
        while at < payload.len() {
            let op = payload[at];
            assert!(PERMITTED.contains(&op), "the writer emitted {op:#04x}, outside the subset");
            if !seen.contains(&op) {
                seen.push(op);
            }
            at += 1;
            at += match op {
                OP_PROTO => 1,
                OP_BININT => 4,
                OP_BINFLOAT => 8,
                OP_LONG1 => {
                    let n = payload[at] as usize;
                    1 + n
                }
                OP_BINUNICODE => {
                    let n = u32::from_le_bytes(payload[at..at + 4].try_into().unwrap()) as usize;
                    4 + n
                }
                _ => 0,
            };
        }
        seen.sort_unstable();
        let mut expected = PERMITTED;
        expected.sort_unstable();
        assert_eq!(seen, expected, "the writer must use every one of the ten, and nothing else");
    }

    #[test]
    fn the_length_prefix_is_four_big_endian_bytes() {
        let mut out = Vec::new();
        write_length_prefix(&mut out, 0x0001_0203);
        assert_eq!(out, vec![0x00, 0x01, 0x02, 0x03]);
        assert_eq!(out.len(), LENGTH_PREFIX_BYTES);
    }

    #[test]
    fn the_header_and_trailer_widths_match_their_constants() {
        let mut out = Vec::new();
        write_header(&mut out);
        assert_eq!(out.len(), HEADER_BYTES);
        let before = out.len();
        write_trailer(&mut out);
        assert_eq!(out.len() - before, TRAILER_BYTES);
    }

    /// A second frame of the same shape reuses the reader's buffers (checked by capacity; this
    /// crate has no allocation counter).
    #[test]
    fn a_warm_reader_reuses_its_buffers() {
        let mut payload = Vec::new();
        write_datapoints(
            &mut payload,
            (0..64).map(|_| ("a.b.c.d", 1_700_000_000i64, 1.0f64)).collect::<Vec<_>>(),
        );
        let mut reader = PickleReader::new();
        let caps = |r: &PickleReader| {
            (
                r.stack.capacity(),
                r.tuples.capacity(),
                r.lists.capacity(),
                r.memo.capacity(),
                r.scratch.capacity(),
            )
        };
        reader.read_datapoints(&payload, |_, _, _| {}).expect("first frame");
        let warm = caps(&reader);
        reader.read_datapoints(&payload, |_, _, _| {}).expect("second frame");
        assert_eq!(caps(&reader), warm, "a warm reader must not reallocate");

        // A protocol-0 frame whose escaped strings decode into the scratch.
        reader.read_datapoints(CPYTHON2_CPICKLE_PROTOCOL_0, |_, _, _| {}).expect("first frame");
        assert!(!reader.scratch.is_empty(), "the fixture must exercise the scratch");
        let warm = caps(&reader);
        reader.read_datapoints(CPYTHON2_CPICKLE_PROTOCOL_0, |_, _, _| {}).expect("second frame");
        assert_eq!(caps(&reader), warm, "a warm reader must not reallocate its scratch");
    }
}
