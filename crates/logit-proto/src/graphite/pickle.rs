//! A pickle **writer** (a ten-opcode protocol-2 subset) and a **restricted reader** for carbon's
//! batch protocol -- see [`super`]'s module doc for where they sit in the codec.
//!
//! ## Why this is hand-rolled, and what that buys
//!
//! Pickle is a stack machine whose *purpose* is arbitrary object construction: the opcodes that
//! make a general unpickler dangerous (`GLOBAL`, `STACK_GLOBAL`, `REDUCE`, `BUILD`, `INST`, `OBJ`,
//! `NEWOBJ`, the `EXT*` registry, `PERSID`) import names and call them. Carbon's batch payload
//! needs none of them: it is a list of `(str, (number, number))` tuples and nothing else. So this
//! reader is an **allowlist**, not a parser with a blocklist bolted on -- any byte that is not one
//! of the opcodes named below fails the frame with
//! `CodecError::Malformed("pickle opcode 0x.. is not permitted")`, including every opcode that
//! does not exist yet. Adding an opcode to the accept list is an ADR-level change
//! (`docs/plans/graphite-carbon-relay.md`'s open risks).
//!
//! There is no new crate dependency behind this. `serde-pickle`/`pickle` crates exist, but they
//! implement the *general* format -- the thing whose generality is the risk -- and `deny.toml` /
//! `script/audit` staying unchanged was a settled decision of the plan.
//!
//! ## Accepted opcodes
//!
//! | Group | Opcodes |
//! |---|---|
//! | framing | `PROTO` `0x80` (version ≤ 5), `FRAME` `0x95` (its declared length validated against the input), `STOP` `0x2e` |
//! | memo | `BINPUT` `0x71`, `LONG_BINPUT` `0x72`, `MEMOIZE` `0x94`, `BINGET` `0x68`, `LONG_BINGET` `0x6a` (keys bounded by [`super::MAX_PICKLE_ITEMS`]) |
//! | containers | `MARK` `0x28`, `EMPTY_LIST` `0x5d`, `LIST` `0x6c`, `APPEND` `0x61`, `APPENDS` `0x65`, `EMPTY_TUPLE` `0x29`, `TUPLE` `0x74`, `TUPLE1` `0x85`, `TUPLE2` `0x86`, `TUPLE3` `0x87` |
//! | strings | `BINUNICODE` `0x58`, `SHORT_BINUNICODE` `0x8c`, `BINUNICODE8` `0x8d`, `BINSTRING` `0x54`, `SHORT_BINSTRING` `0x55`, `BINBYTES` `0x42`, `SHORT_BINBYTES` `0x43`, `BINBYTES8` `0x8e` -- every one UTF-8 validated |
//! | numbers | `BININT` `0x4a`, `BININT1` `0x4b`, `BININT2` `0x4d`, `LONG1` `0x8a`, `LONG4` `0x8b` (magnitude ≤ 8 bytes), `BINFLOAT` `0x47` |
//! | inert | `NONE` `0x4e`, `NEWTRUE` `0x88`, `NEWFALSE` `0x89` |
//!
//! The three inert opcodes are accepted because a stray `None`/`True` in a sender's list must cost
//! **that datapoint**, not the whole frame: rejecting the frame would discard every unrelated
//! datapoint packed behind it, the same per-line isolation rule
//! `crates/logit-inputs/src/statsd.rs` and `crate::collectd`'s decoder already follow. The bytes
//! `0x8c`/`0x8d`/`0x8e` and `0x95` are protocol-4/5 opcodes real senders emit under
//! `pickle.dumps(..., protocol=-1)` on a modern CPython, which is exactly why the plan's settled
//! decision names both protocol 2 and `-1` as senders to accept.
//!
//! Everything else is rejected, in particular: `GLOBAL` `0x63`, `STACK_GLOBAL` `0x93`, `REDUCE`
//! `0x52`, `BUILD` `0x62`, `INST` `0x69`, `OBJ` `0x6f`, `NEWOBJ` `0x81`, `NEWOBJ_EX` `0x92`,
//! `EXT1/2/4` `0x82`/`0x83`/`0x84`, `PERSID` `0x50`, `BINPERSID` `0x51`, `DUP` `0x32`, `POP`
//! `0x30`, `POP_MARK` `0x31`, every dict and set opcode (`EMPTY_DICT` `0x7d`, `DICT` `0x64`,
//! `SETITEM` `0x73`, `SETITEMS` `0x75`, `EMPTY_SET` `0x8f`, `FROZENSET` `0x91`, `ADDITEMS` `0x90`),
//! `BYTEARRAY8` `0x96`, `NEXT_BUFFER` `0x97`, `READONLY_BUFFER` `0x98`, and every protocol-0
//! textual opcode (`INT` `0x49`, `LONG` `0x4c`, `FLOAT` `0x46`, `STRING` `0x53`, `UNICODE` `0x56`,
//! `PUT` `0x70`, `GET` `0x67`, ...). A protocol-0 dump therefore fails at its first value rather
//! than being half-understood. A protocol-1 dump decodes: it has no `PROTO` header, but every
//! opcode it emits for a carbon payload is a binary one from the table above.
//!
//! ## Bounds
//!
//! - every declared length is validated against the **remaining input** before anything is sized
//!   from it -- `crate::frame::read_frame`'s discipline, and what
//!   `crates/logit-proto/tests/robustness.rs` measures with a peak-allocation counter;
//! - no string is ever copied: a string value is a `Range` into the caller's buffer, so a frame
//!   declaring a gigabyte allocates nothing at all, it just fails the bound;
//! - [`super::MAX_PICKLE_DEPTH`] bounds open `MARK`s, [`super::MAX_PICKLE_ITEMS`] bounds the stack,
//!   each arena and the memo independently; the memo is additionally bounded by opcodes actually
//!   consumed, not only by that cap -- a memo key must be ordinal (one new slot per
//!   `BINPUT`/`LONG_BINPUT`/`MEMOIZE`; `key <= self.memo.len()`), so a single `LONG_BINPUT` cannot
//!   grow the memo to an attacker-chosen size the way a declared length could;
//! - `LONG1`/`LONG4` accept a magnitude of at most 8 bytes -- carbon's timestamps are seconds, and
//!   a 2 GB `LONG4` is an attack, not a datapoint;
//! - the stack must hold **exactly one** value at `STOP`, and it must be a list.
//!
//! ## Reusable state
//!
//! [`PickleReader`]'s stack, arenas and memo are struct fields cleared per frame, never
//! reallocated once grown -- so a warm `decode_into` over a pickle frame allocates only the
//! caller's `Vec<Event>` (`docs/design/memory.md` §2, and the allocation rows W2 pins). That is
//! also why a tuple/list is an index range into an arena rather than a `Vec` of its own: a
//! `Vec`-per-tuple design would allocate twice per datapoint forever.

use super::{MAX_PICKLE_DEPTH, MAX_PICKLE_ITEMS};
use crate::CodecError;

// -- opcodes ------------------------------------------------------------------------------------

const OP_MARK: u8 = 0x28;
const OP_EMPTY_TUPLE: u8 = 0x29;
const OP_STOP: u8 = 0x2e;
const OP_BINBYTES: u8 = 0x42;
const OP_SHORT_BINBYTES: u8 = 0x43;
const OP_BINFLOAT: u8 = 0x47;
const OP_BININT: u8 = 0x4a;
const OP_BININT1: u8 = 0x4b;
const OP_BININT2: u8 = 0x4d;
const OP_NONE: u8 = 0x4e;
const OP_BINSTRING: u8 = 0x54;
const OP_SHORT_BINSTRING: u8 = 0x55;
const OP_BINUNICODE: u8 = 0x58;
const OP_EMPTY_LIST: u8 = 0x5d;
const OP_APPEND: u8 = 0x61;
const OP_APPENDS: u8 = 0x65;
const OP_BINGET: u8 = 0x68;
const OP_LONG_BINGET: u8 = 0x6a;
const OP_LIST: u8 = 0x6c;
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

/// The highest `PROTO` version this reader will look at. Nothing above 5 exists; a payload
/// claiming one is either corrupt or probing.
const MAX_PROTO_VERSION: u8 = 5;

/// The most bytes a `LONG1`/`LONG4` magnitude may carry -- see this module's "Bounds" section.
const MAX_LONG_BYTES: usize = 8;

// -- writer -------------------------------------------------------------------------------------

/// Bytes [`write_header`] writes: `PROTO` + its version byte, `EMPTY_LIST`, `MARK`.
pub const HEADER_BYTES: usize = 4;

/// Bytes [`write_trailer`] writes: `APPENDS`, `STOP`.
pub const TRAILER_BYTES: usize = 2;

/// Bytes in carbon's frame prefix: one big-endian `u32` payload length, Twisted's
/// `Int32StringReceiver` framing. Written by [`write_length_prefix`], not by the payload writers.
pub const LENGTH_PREFIX_BYTES: usize = 4;

/// The protocol version [`write_header`] declares. Protocol 2 is the oldest version every opcode
/// this writer uses exists in, and is what `carbon-client`/`pickle.dumps(..., protocol=2)` --
/// carbon's own documented example -- emits.
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
/// `BINUNICODE` rather than `SHORT_BINSTRING`: on Python 3 a `BINSTRING`/`SHORT_BINSTRING` unpickles
/// to `bytes`, and carbon's own receiver indexes the datapoint's first element as a `str`. The
/// timestamp is `BININT` when it fits an `i32` and `LONG1` otherwise -- the same choice CPython's
/// own pickler makes by magnitude, and what keeps a post-2038 second encodable at all.
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
/// [`write_trailer`]. `out` is **not** cleared first. The encoder builds frames incrementally
/// instead (it has to know where `max_frame_bytes` falls); this is for tests, benches and any
/// caller with a whole batch in hand.
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
/// Saturates rather than wrapping: a payload past `u32::MAX` cannot exist here (every caller is
/// bounded by `max_frame_bytes`, itself capped at 16 MiB by the graph rules), and a wrapped prefix
/// would desynchronize the receiver's stream rather than fail loudly.
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
    // Minimal two's-complement little-endian magnitude, exactly as CPython's `encode_long` emits
    // it: drop a trailing sign-extension byte only while the byte below it still carries the sign
    // in its own high bit, so a positive value keeps the leading `0x00` that stops it reading as
    // negative.
    let sign: u8 = if v < 0 { 0xff } else { 0x00 };
    let mut len = MAX_LONG_BYTES;
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

/// One value on the restricted reader's stack. `Copy` and pointer-free on purpose: a tuple or list
/// is an index range into a reusable arena, never a `Vec` of its own, which is what makes a warm
/// frame decode allocation-free (this module's "Reusable state" section).
#[derive(Debug, Clone, Copy, PartialEq)]
enum PValue {
    /// A `MARK` sentinel. Never a datapoint; only [`PickleReader`]'s own container opcodes look at
    /// it.
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
    /// Sparse by key: `BINPUT`/`LONG_BINPUT` name an arbitrary index, `MEMOIZE` appends at the
    /// next one. `None` is "nothing memoized under this key", which a `BINGET` for it rejects.
    memo: Vec<Option<PValue>>,
    /// Open `MARK` count -- the depth [`MAX_PICKLE_DEPTH`] bounds.
    marks: usize,
}

impl PickleReader {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses one complete, **unframed** pickle payload (no 4-byte length prefix -- the listener
    /// strips that) and calls `on_datapoint` once per well-shaped `(path, (timestamp, value))`
    /// item, in list order. Returns how many items were skipped for being the wrong shape.
    ///
    /// A wrong-shaped *item* is skipped, never fatal (this module's "Accepted opcodes" section);
    /// a disallowed opcode, a declared length past the input, a bound, or a payload that does not
    /// leave exactly one list on the stack fails the whole frame with
    /// [`CodecError::Malformed`].
    ///
    /// `path` borrows the caller's `input`, so the caller can turn it back into a zero-copy
    /// [`bytes::Bytes`] slice of the frame it already owns.
    pub fn read_datapoints<'a>(
        &mut self,
        input: &'a [u8],
        mut on_datapoint: impl FnMut(&'a str, f64, f64),
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
    /// `(str, (number, number))`. Depth-bounded by construction: it looks exactly two levels down
    /// and never recurses, so no crafted nesting can make this the recursion the opcode allowlist
    /// is there to prevent.
    fn datapoint<'a>(&self, input: &'a [u8], item: PValue) -> Option<(&'a str, f64, f64)> {
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

    fn as_str<'a>(&self, input: &'a [u8], value: PValue) -> Option<&'a str> {
        let PValue::Str { start, len } = value else { return None };
        // Validated once at parse time; re-validated here rather than carrying an unsafe
        // "trust me" across the two, since the cost is a scan of one path.
        std::str::from_utf8(&input[start as usize..start as usize + len as usize]).ok()
    }

    /// A number, or a **numeric string** -- carbon's own pickle producers are Python, where a
    /// datapoint value read from a text source routinely arrives as `"3.14"` rather than a float,
    /// and carbon coerces it with `float()`. `str::parse::<f64>` is the same coercion.
    fn as_f64(&self, input: &[u8], value: PValue) -> Option<f64> {
        match value {
            PValue::Int(v) => Some(v as f64),
            PValue::Float(v) => Some(v),
            PValue::Str { start, len } => {
                std::str::from_utf8(&input[start as usize..start as usize + len as usize])
                    .ok()?
                    .parse::<f64>()
                    .ok()
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
        self.marks = 0;

        // Every string is a `u32` range into `input`; a payload this large cannot reach here
        // (`max_frame_bytes` is capped at 16 MiB by the graph rules) and must not silently
        // truncate a range if it somehow did.
        if input.len() > u32::MAX as usize {
            return Err(malformed("pickle payload is larger than 4 GiB"));
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
                // The frame length is advisory here -- this reader already holds the whole payload
                // -- but it is validated anyway, so a frame claiming more than it carries fails
                // now rather than confusing a later length check.
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
                    self.push(PValue::Int(v))?;
                }
                OP_LONG4 => {
                    let n = i32::from_le_bytes(slice(input, at, 4)?.try_into().unwrap());
                    at += 4;
                    let n = usize::try_from(n)
                        .map_err(|_| malformed("pickle LONG4 declares a negative length"))?;
                    let v = read_long(input, at, n)?;
                    at += n;
                    self.push(PValue::Int(v))?;
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
                // BINSTRING's length is a *signed* 32-bit count in CPython's own reader, which
                // rejects a negative one outright rather than sign-extending it into a huge size.
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
                    // Checked against the remaining input *before* being used as a length -- a
                    // `u64` that does not fit `usize` can only be a crafted one.
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
                    let key = self.memo.len();
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

    /// Validates `n` against the remaining input, validates the bytes as UTF-8, and pushes the
    /// **range** -- nothing is copied, so a crafted length costs a comparison, not an allocation.
    fn push_str(&mut self, input: &[u8], at: usize, n: usize) -> Result<(), CodecError> {
        let bytes = slice(input, at, n)?;
        if std::str::from_utf8(bytes).is_err() {
            return Err(malformed("pickle string is not valid utf-8"));
        }
        self.push(PValue::Str { start: at as u32, len: n as u32 })
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
    /// The list must be the **tail** of the arena (`start + len == lists.len()`), which is exactly
    /// what carbon's own `EMPTY_LIST MARK … APPENDS` shape gives. A crafted payload that interleaves
    /// two open lists is rejected rather than reshuffled: nothing real produces one, and the
    /// alternative is either a per-list `Vec` (an allocation per frame, forever) or a compaction
    /// pass a hostile sender chooses the cost of.
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
        // A memo key must be ordinal: CPython's pickler hands out keys sequentially, one per
        // `BINPUT`/`LONG_BINPUT`/`MEMOIZE`, so a real stream only ever overwrites an existing slot
        // (`key < self.memo.len()`) or appends the next one (`key == self.memo.len()`). Anything
        // past that sizes the memo from an attacker-chosen index rather than opcodes actually
        // consumed -- see the module doc's "Bounds" section.
        if key > self.memo.len() {
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
        if key == self.memo.len() {
            self.memo.push(None);
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

/// `input[at..at + n]`, or [`CodecError::Malformed`] -- the one place a declared length meets the
/// input's real size, so every length check in this module goes through it.
fn slice(input: &[u8], at: usize, n: usize) -> Result<&[u8], CodecError> {
    let end = at.checked_add(n).ok_or_else(|| malformed("pickle length overflows usize"))?;
    input
        .get(at..end)
        .ok_or_else(|| malformed(format!("pickle field of {n} byte(s) runs past the payload")))
}

/// A `LONG1`/`LONG4` magnitude: little-endian two's complement, at most [`MAX_LONG_BYTES`] bytes.
fn read_long(input: &[u8], at: usize, n: usize) -> Result<i64, CodecError> {
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
    let mut buf = if negative { [0xffu8; 8] } else { [0u8; 8] };
    buf[..n].copy_from_slice(bytes);
    Ok(i64::from_le_bytes(buf))
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
    // with `<expr>`/`<n>` exactly as each constant's own doc comment records. Python 3.14.0.
    // Committing the bytes rather than the generator is deliberate: AGENTS.md's "benchmark and
    // test fixtures never depend on a running service" rule extends to an interpreter, and these
    // are precisely the bytes a real carbon sender puts on the wire.

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
    /// the shape a hostile payload uses to make an unpickler construct an arbitrary object.
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

    /// Protocol `-1` on a modern CPython is protocol 5 -- `FRAME`/`SHORT_BINUNICODE`/`MEMOIZE`,
    /// three opcodes protocol 2 never emits. The plan's settled decision names both as senders to
    /// accept, so both are pinned here.
    #[test]
    fn a_cpython_protocol_5_dump_decodes() {
        let (points, skipped) = read(CPYTHON_PROTOCOL_5).expect("protocol 5 must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("sys.cpu", 1_700_000_000.0, 0.5)]);
    }

    /// Protocol 1 predates `PROTO`, but a carbon payload dumped under it uses only allowlisted
    /// binary opcodes, so it decodes like protocol 2; only protocol 0's textual opcodes are refused.
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

    /// Carbon coerces a datapoint's numbers with `float()`, so a producer that never converted its
    /// text does not lose its data here either.
    #[test]
    fn numeric_strings_parse_as_numbers() {
        let (points, skipped) = read(CPYTHON_NUMERIC_STRINGS).expect("numeric strings must decode");
        assert_eq!(skipped, 0);
        assert_eq!(points, vec![point("a.b", 1_700_000_000.0, 2.5)]);
    }

    /// A stray `None` costs **that** datapoint, not the frame: everything packed behind it still
    /// decodes. The per-line isolation rule `crate::collectd`'s decoder and `statsd_in` follow.
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

    // -- rejected payloads ------------------------------------------------------------------------

    /// The allowlist's whole point: every opcode that could make a general unpickler construct or
    /// call something is refused, with the wording the plan fixes so a diagnostic is greppable.
    #[test]
    fn object_construction_opcodes_are_rejected() {
        for (name, payload) in [
            ("GLOBAL", CPYTHON_GLOBAL),
            ("STACK_GLOBAL", CPYTHON_STACK_GLOBAL),
            ("dict", CPYTHON_DICT),
            ("set", CPYTHON_SET),
            ("protocol 0", CPYTHON_PROTOCOL_0),
        ] {
            let err = read(payload).expect_err("{name} must be rejected");
            let message = err.to_string();
            assert!(
                message.contains("is not permitted"),
                "{name} was rejected as {message:?}, not as a forbidden opcode"
            );
        }
    }

    /// `REDUCE` (0x52) and `BUILD` (0x62) don't appear in a stock `pickle.dumps` of a plain class
    /// (which uses `NEWOBJ`), so they get a hand-assembled payload of their own rather than being
    /// left untested.
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
        // Exactly at the cap is a bounds question, not a depth one: the marks are still on the
        // stack at STOP, so this fails the "exactly one list" rule instead.
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

    /// A declared length far past the input must fail on a comparison, never on an allocation --
    /// the property `crates/logit-proto/tests/robustness.rs` measures with a peak-allocation
    /// counter, asserted here for the reject itself.
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
    fn a_long_magnitude_past_eight_bytes_is_rejected() {
        let mut payload = vec![OP_PROTO, 2, OP_LONG1, 9];
        payload.extend_from_slice(&[0u8; 9]);
        payload.push(OP_STOP);
        let err = read(&payload).expect_err("a 9-byte long must be rejected");
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

    /// `PROTO 2, EMPTY_LIST, LONG_BINPUT 499999, STOP` -- the exact 9-byte frame from the review
    /// finding this test exists to close: a `LONG_BINPUT` key with nothing behind it in the memo
    /// used to `resize` the memo to 500,000 slots (~8 MB of `Option<PValue>`) before failing later
    /// (or not at all). Now the ordinal check in `memo_put` rejects it immediately, so the memo
    /// never grows past what `EMPTY_LIST` itself put on the stack -- asserted at the byte-peak
    /// level by `graphite_pickle_never_allocates_from_a_hostile_memo_key` in
    /// `crates/logit-proto/tests/robustness.rs`.
    #[test]
    fn a_memo_key_that_skips_ahead_is_rejected() {
        let payload = [OP_PROTO, 2, OP_EMPTY_LIST, OP_LONG_BINPUT, 0x1f, 0xa1, 0x07, 0x00, OP_STOP];
        let err = read(&payload).expect_err("a memo key past the entries written so far must fail");
        assert!(err.to_string().contains("skips ahead"), "{err}");
    }

    /// A key at or below the memo's current length is exactly what CPython's own pickler emits --
    /// `BINPUT`/`LONG_BINPUT`/`MEMOIZE` only ever overwrite an existing slot or append the next
    /// one. This drives a `BINPUT` at key 0 twice: once to memoize the path string (append), once
    /// more after building the full tuple (overwrite) -- the datapoint must still decode correctly
    /// even though its memo slot's value changed identity partway through.
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

    /// The ordinary shape a real batch takes: each new memoized value's key is exactly the memo's
    /// current length, via `LONG_BINPUT` specifically -- the same opcode the hostile frame above
    /// abuses, shown here appending two slots in sequence rather than skipping ahead.
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

    /// Every prefix of every fixture, valid or hostile: the reader must return, never panic.
    #[test]
    fn every_single_byte_truncation_of_every_fixture_fails_without_panicking() {
        for payload in [
            CPYTHON_PROTOCOL_2,
            CPYTHON_PROTOCOL_5,
            CPYTHON_MEMOIZED_PATH,
            CPYTHON_LONG1_TIMESTAMP,
            CPYTHON_WRONG_SHAPE,
            CPYTHON_GLOBAL,
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

    /// A CPython dump and this writer's output for the same datapoint decode to the same thing --
    /// they are not byte-identical (CPython memoizes every string and uses `APPEND` for a
    /// one-element list, this writer does neither), and they do not have to be. What matters is
    /// that a carbon receiver reading either gets the same list.
    #[test]
    fn a_cpython_dump_and_our_writer_decode_identically() {
        let mut ours = Vec::new();
        write_datapoints(&mut ours, [("sys.cpu", 1_700_000_000i64, 0.5f64)]);
        assert_eq!(read(&ours).unwrap(), read(CPYTHON_PROTOCOL_2).unwrap());
        assert_eq!(read(&ours).unwrap(), read(CPYTHON_PROTOCOL_5).unwrap());
    }

    #[test]
    fn a_long1_timestamp_matches_cpythons_own_minimal_encoding() {
        // The five bytes CPython emits for 2**31 + 5: `LONG1`, length 5, then the minimal
        // little-endian two's-complement magnitude with the leading `0x00` that keeps it positive.
        let mut out = Vec::new();
        write_int(&mut out, 2_147_483_653);
        assert_eq!(out, vec![OP_LONG1, 0x05, 0x05, 0x00, 0x00, 0x80, 0x00]);
        assert!(
            CPYTHON_LONG1_TIMESTAMP.windows(out.len()).any(|w| w == out),
            "CPython's own dump must contain exactly these bytes"
        );
    }

    /// The writer is a **ten-opcode** subset, and staying that way is what makes it reviewable
    /// against carbon's unpickler. Disassembles its output and asserts the opcode set.
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

        // A miniature disassembler over exactly those ten -- walking operand widths rather than
        // scanning for bytes, so an opcode byte appearing inside a string or a float can't be
        // mistaken for an opcode (and an unexpected opcode can't hide behind one).
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

    /// The reusable-state contract this module's doc claims: a second frame of the same shape
    /// refills the reader's buffers without touching the allocator. Checked via capacity rather
    /// than an allocation counter (this crate has none of its own -- `logit-bench` depends on it,
    /// not the other way around).
    #[test]
    fn a_warm_reader_reuses_its_buffers() {
        let mut payload = Vec::new();
        write_datapoints(
            &mut payload,
            (0..64).map(|_| ("a.b.c.d", 1_700_000_000i64, 1.0f64)).collect::<Vec<_>>(),
        );
        let mut reader = PickleReader::new();
        reader.read_datapoints(&payload, |_, _, _| {}).expect("first frame");
        let caps = (reader.stack.capacity(), reader.tuples.capacity(), reader.lists.capacity());
        reader.read_datapoints(&payload, |_, _, _| {}).expect("second frame");
        assert_eq!(
            (reader.stack.capacity(), reader.tuples.capacity(), reader.lists.capacity()),
            caps,
            "a warm reader must not reallocate"
        );
    }
}
