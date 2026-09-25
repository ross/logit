//! A hand-rolled MessagePack reader and writer (https://github.com/msgpack/msgpack/blob/master/spec.md),
//! for the Datadog traces and APM stats codecs (`crate::datadog::traces_msgpack`,
//! `crate::datadog::stats`).
//!
//! ## Why this is hand-rolled
//!
//! Same reasoning as the pickle reader in `crate::graphite::pickle`: the crates that exist
//! (`rmp`, `rmpv`, `rmp-serde`) implement the *general* format behind a `serde`-shaped API this
//! crate deliberately doesn't use elsewhere (see `docs/adr/otlp-json-decoding.md` for why a plain
//! walk over the wire beats fighting a derive), and ADR `datadog-agent-and-intake-relay` decision
//! 5 settles it: hand-rolled, no new dependency, no `script/audit` surface. Unlike the pickle
//! reader, MessagePack itself carries no opcode that can call into arbitrary code -- every format
//! byte just names a length and a shape -- so there's no opcode allowlist here, only bounds
//! checking against what's actually left in the buffer.
//!
//! ## What's covered
//!
//! Every scalar and container format: nil, bool, all int formats (positive/negative fixint,
//! (u)int8/16/32/64), float32/float64, str (fixstr, str8/16/32), bin (bin8/16/32), array (fixarray,
//! array16/32), and map (fixmap, map16/32). Ext types (fixext1/2/4/8/16, ext8/16/32) are a format
//! Datadog's own payloads don't use for anything this crate reads -- [`Reader::skip_value`] skips
//! one whole, unread, wherever it shows up nested inside a value this crate does care about, and
//! [`Writer`] never emits one. The one reserved byte (`0xc1`) is rejected rather than silently
//! treated as anything else.
//!
//! ## Shape of the API
//!
//! [`Reader`] borrows its input for the reader's whole lifetime (`Reader<'a>` over `&'a [u8]`,
//! not `bytes::Bytes`) so `read_str`/`read_str_bytes`/`read_bin` return slices of the original
//! buffer with no copy -- the caller decides whether a Datadog series' `metric.name` or tag needs
//! to be owned, same as every other codec's zero-copy-on-the-read-path convention
//! (`docs/design/memory.md`). Every length is checked against [`Reader::remaining`] before a
//! single byte of the slice it bounds is touched, so truncated or actively hostile input returns
//! [`MsgpackError::Truncated`] rather than panicking; [`Reader::skip_value`] additionally bounds
//! its own recursion so a deeply nested array-of-arrays can't blow the stack.
//!
//! [`Writer`] always chooses the smallest format that fits a given value (the canonical MessagePack
//! encoding), the same way `native::varint` always emits the shortest LEB128 form.

use std::fmt;

use crate::CodecError;

/// The coarse shape of the value a [`Reader`] is looking at, as reported by
/// [`Reader::peek_type`] and carried in [`MsgpackError::Type::found`]. Positive fixint and
/// uint8/16/32/64 are all `Uint`; negative fixint and int8/16/32/64 are all `Int` -- the same
/// split [`Reader::read_i64`]/[`Reader::read_u64`] use to decide whether a value in the "wrong"
/// half still fits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    Nil,
    Bool,
    Int,
    Uint,
    Float,
    Str,
    Bin,
    Array,
    Map,
    Ext,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Type::Nil => "nil",
            Type::Bool => "bool",
            Type::Int => "int",
            Type::Uint => "uint",
            Type::Float => "float",
            Type::Str => "str",
            Type::Bin => "bin",
            Type::Array => "array",
            Type::Map => "map",
            Type::Ext => "ext",
        })
    }
}

/// The maximum nesting depth [`Reader::skip_value`] will follow into arrays and maps before
/// giving up with [`MsgpackError::DepthExceeded`] -- a bound against a maliciously (or just
/// accidentally) deep array-of-arrays running this reader out of stack.
const MAX_SKIP_DEPTH: usize = 64;

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum MsgpackError {
    #[error("truncated msgpack input")]
    Truncated,
    #[error("expected {expected}, found {found}")]
    Type { expected: &'static str, found: Type },
    #[error("invalid utf-8 in msgpack string")]
    InvalidUtf8,
    #[error("msgpack nesting exceeds depth {MAX_SKIP_DEPTH}")]
    DepthExceeded,
    #[error("reserved msgpack byte 0x{0:02x}")]
    Reserved(u8),
}

impl From<MsgpackError> for CodecError {
    fn from(err: MsgpackError) -> Self {
        CodecError::Malformed(format!("msgpack: {err}"))
    }
}

/// Which half of the 64-bit integer space a decoded int format landed in -- the common core
/// [`Reader::read_i64`], [`Reader::read_u64`], and [`Reader::read_f64`] all read through, so a
/// value the wire spelled as (say) `uint8` still satisfies `read_i64` and one spelled as
/// `int8` with a non-negative value still satisfies `read_u64`.
enum IntRepr {
    Signed(i64),
    Unsigned(u64),
}

/// A zero-copy MessagePack reader over a borrowed byte slice. See the module doc for the overall
/// shape; every method here either advances past a complete, in-bounds value or leaves the
/// position untouched and returns an error.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    pub fn is_empty(&self) -> bool {
        self.remaining() == 0
    }

    fn peek_byte(&self) -> Result<u8, MsgpackError> {
        self.buf.get(self.pos).copied().ok_or(MsgpackError::Truncated)
    }

    fn advance(&mut self, n: usize) {
        self.pos += n;
    }

    /// Takes the next `n` bytes as a slice borrowed from the reader's own lifetime, not
    /// `self`'s -- `self.buf` is copied out first (`&'a [u8]` is `Copy`) so the returned slice
    /// isn't tied to `&mut self`'s shorter borrow. Bounds-checked against [`Self::remaining`]
    /// first, so this never panics on truncated input.
    fn take_bytes(&mut self, n: usize) -> Result<&'a [u8], MsgpackError> {
        if self.remaining() < n {
            return Err(MsgpackError::Truncated);
        }
        let buf = self.buf;
        let start = self.pos;
        self.pos += n;
        Ok(&buf[start..start + n])
    }

    fn raw_u8(&mut self) -> Result<u8, MsgpackError> {
        self.take_bytes(1).map(|b| b[0])
    }

    fn raw_u16(&mut self) -> Result<u16, MsgpackError> {
        self.take_bytes(2).map(|b| u16::from_be_bytes(b.try_into().unwrap()))
    }

    fn raw_u32(&mut self) -> Result<u32, MsgpackError> {
        self.take_bytes(4).map(|b| u32::from_be_bytes(b.try_into().unwrap()))
    }

    fn raw_u64(&mut self) -> Result<u64, MsgpackError> {
        self.take_bytes(8).map(|b| u64::from_be_bytes(b.try_into().unwrap()))
    }

    fn raw_i8(&mut self) -> Result<i8, MsgpackError> {
        self.raw_u8().map(|b| b as i8)
    }

    fn raw_i16(&mut self) -> Result<i16, MsgpackError> {
        self.raw_u16().map(|b| b as i16)
    }

    fn raw_i32(&mut self) -> Result<i32, MsgpackError> {
        self.raw_u32().map(|b| b as i32)
    }

    fn raw_i64(&mut self) -> Result<i64, MsgpackError> {
        self.raw_u64().map(|b| b as i64)
    }

    fn type_error<T>(&self, expected: &'static str) -> Result<T, MsgpackError> {
        let found = type_for_prefix(self.peek_byte()?)?;
        Err(MsgpackError::Type { expected, found })
    }

    /// The coarse shape of the next value, without consuming it. Errs on truncation (no byte to
    /// look at) or on the one reserved byte (`0xc1`), which names no shape at all.
    pub fn peek_type(&self) -> Result<Type, MsgpackError> {
        type_for_prefix(self.peek_byte()?)
    }

    pub fn read_nil(&mut self) -> Result<(), MsgpackError> {
        if self.peek_byte()? == 0xc0 {
            self.advance(1);
            Ok(())
        } else {
            self.type_error("nil")
        }
    }

    /// Runs `f` unless the next value is `nil`, in which case `f` isn't called at all and the
    /// `nil` is consumed. The nullable-field pattern every optional Datadog field needs.
    pub fn read_nil_or<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, MsgpackError>,
    ) -> Result<Option<T>, MsgpackError> {
        if self.peek_type()? == Type::Nil {
            self.advance(1);
            Ok(None)
        } else {
            f(self).map(Some)
        }
    }

    pub fn read_bool(&mut self) -> Result<bool, MsgpackError> {
        match self.peek_byte()? {
            0xc2 => {
                self.advance(1);
                Ok(false)
            }
            0xc3 => {
                self.advance(1);
                Ok(true)
            }
            _ => self.type_error("bool"),
        }
    }

    /// Reads any int format, keeping track of which half of the 64-bit space it came from so
    /// [`Self::read_i64`]/[`Self::read_u64`]/[`Self::read_f64`] can each apply their own
    /// tolerance on top of one decode.
    fn read_int_repr(&mut self) -> Result<IntRepr, MsgpackError> {
        let b = self.peek_byte()?;
        match b {
            0x00..=0x7f => {
                self.advance(1);
                Ok(IntRepr::Unsigned(b as u64))
            }
            0xe0..=0xff => {
                self.advance(1);
                Ok(IntRepr::Signed(b as i8 as i64))
            }
            0xcc => {
                self.advance(1);
                Ok(IntRepr::Unsigned(self.raw_u8()? as u64))
            }
            0xcd => {
                self.advance(1);
                Ok(IntRepr::Unsigned(self.raw_u16()? as u64))
            }
            0xce => {
                self.advance(1);
                Ok(IntRepr::Unsigned(self.raw_u32()? as u64))
            }
            0xcf => {
                self.advance(1);
                Ok(IntRepr::Unsigned(self.raw_u64()?))
            }
            0xd0 => {
                self.advance(1);
                Ok(IntRepr::Signed(self.raw_i8()? as i64))
            }
            0xd1 => {
                self.advance(1);
                Ok(IntRepr::Signed(self.raw_i16()? as i64))
            }
            0xd2 => {
                self.advance(1);
                Ok(IntRepr::Signed(self.raw_i32()? as i64))
            }
            0xd3 => {
                self.advance(1);
                Ok(IntRepr::Signed(self.raw_i64()?))
            }
            _ => self.type_error("int"),
        }
    }

    /// Accepts any int format that fits in an `i64`; errs if a `uint64` (or, in principle, any
    /// unsigned format) carries a value above `i64::MAX`.
    pub fn read_i64(&mut self) -> Result<i64, MsgpackError> {
        match self.read_int_repr()? {
            IntRepr::Signed(v) => Ok(v),
            IntRepr::Unsigned(v) => {
                if v <= i64::MAX as u64 {
                    Ok(v as i64)
                } else {
                    Err(MsgpackError::Type { expected: "i64", found: Type::Uint })
                }
            }
        }
    }

    /// Accepts any int format that isn't negative; errs on a negative fixint or `int*` value.
    pub fn read_u64(&mut self) -> Result<u64, MsgpackError> {
        match self.read_int_repr()? {
            IntRepr::Unsigned(v) => Ok(v),
            IntRepr::Signed(v) => {
                if v >= 0 {
                    Ok(v as u64)
                } else {
                    Err(MsgpackError::Type { expected: "u64", found: Type::Int })
                }
            }
        }
    }

    /// Accepts any int format and returns its 64 bits as a `u64`, wrapping a negative value the
    /// way Go's `uint64(v)` cast does (`-1` reads as `u64::MAX`). A caller that wants an `i64`
    /// casts the result back, which wraps a `uint64` above `i64::MAX` the same way.
    ///
    /// This exists because [`Reader::read_u64`] and [`Reader::read_i64`] consume the int before
    /// they range-check it: once either errs, the value is gone, so retrying with the other
    /// reader reads the *next* value instead. This reads the int once and never errs on range.
    pub fn read_int_wrapping(&mut self) -> Result<u64, MsgpackError> {
        match self.read_int_repr()? {
            IntRepr::Unsigned(v) => Ok(v),
            IntRepr::Signed(v) => Ok(v as u64),
        }
    }

    /// Accepts `float32`, `float64`, or -- for tolerance of encoders that write a whole number as
    /// an int rather than a float -- any int format, widened.
    pub fn read_f64(&mut self) -> Result<f64, MsgpackError> {
        match self.peek_byte()? {
            0xca => {
                self.advance(1);
                Ok(f32::from_be_bytes(self.take_bytes(4)?.try_into().unwrap()) as f64)
            }
            0xcb => {
                self.advance(1);
                Ok(f64::from_be_bytes(self.take_bytes(8)?.try_into().unwrap()))
            }
            _ => match self.read_int_repr() {
                Ok(IntRepr::Signed(v)) => Ok(v as f64),
                Ok(IntRepr::Unsigned(v)) => Ok(v as f64),
                Err(MsgpackError::Type { found, .. }) => {
                    Err(MsgpackError::Type { expected: "f64", found })
                }
                Err(other) => Err(other),
            },
        }
    }

    /// UTF-8-validated string. See [`Self::read_str_bytes`] for the lossless escape hatch.
    pub fn read_str(&mut self) -> Result<&'a str, MsgpackError> {
        std::str::from_utf8(self.read_str_bytes()?).map_err(|_| MsgpackError::InvalidUtf8)
    }

    /// The raw bytes of a `str` format value, with no UTF-8 validation -- for a field that must
    /// round-trip a producer's bytes exactly even if they're not valid UTF-8.
    pub fn read_str_bytes(&mut self) -> Result<&'a [u8], MsgpackError> {
        let b = self.peek_byte()?;
        let len = match b {
            0xa0..=0xbf => {
                self.advance(1);
                (b & 0x1f) as usize
            }
            0xd9 => {
                self.advance(1);
                self.raw_u8()? as usize
            }
            0xda => {
                self.advance(1);
                self.raw_u16()? as usize
            }
            0xdb => {
                self.advance(1);
                self.raw_u32()? as usize
            }
            _ => return self.type_error("str"),
        };
        self.take_bytes(len)
    }

    pub fn read_bin(&mut self) -> Result<&'a [u8], MsgpackError> {
        let len = match self.peek_byte()? {
            0xc4 => {
                self.advance(1);
                self.raw_u8()? as usize
            }
            0xc5 => {
                self.advance(1);
                self.raw_u16()? as usize
            }
            0xc6 => {
                self.advance(1);
                self.raw_u32()? as usize
            }
            _ => return self.type_error("bin"),
        };
        self.take_bytes(len)
    }

    /// The element count of an array format value; the caller reads exactly that many elements
    /// itself (no internal allocation here, so an attacker-chosen huge count costs nothing until
    /// the caller actually tries to read that many values off a finite buffer).
    pub fn read_array_len(&mut self) -> Result<usize, MsgpackError> {
        match self.peek_byte()? {
            b @ 0x90..=0x9f => {
                self.advance(1);
                Ok((b & 0x0f) as usize)
            }
            0xdc => {
                self.advance(1);
                Ok(self.raw_u16()? as usize)
            }
            0xdd => {
                self.advance(1);
                Ok(self.raw_u32()? as usize)
            }
            _ => self.type_error("array"),
        }
    }

    /// The key-value pair count of a map format value.
    pub fn read_map_len(&mut self) -> Result<usize, MsgpackError> {
        match self.peek_byte()? {
            b @ 0x80..=0x8f => {
                self.advance(1);
                Ok((b & 0x0f) as usize)
            }
            0xde => {
                self.advance(1);
                Ok(self.raw_u16()? as usize)
            }
            0xdf => {
                self.advance(1);
                Ok(self.raw_u32()? as usize)
            }
            _ => self.type_error("map"),
        }
    }

    /// Skips one whole value of any type -- scalar, or a container skipped recursively -- without
    /// interpreting it. The one path that has to handle ext formats, since this crate's writer
    /// never emits one but a field it doesn't care about might still be one on the way in.
    /// Depth-limited (see [`MAX_SKIP_DEPTH`]) against runaway nesting.
    pub fn skip_value(&mut self) -> Result<(), MsgpackError> {
        self.skip_value_at_depth(0)
    }

    fn skip_value_at_depth(&mut self, depth: usize) -> Result<(), MsgpackError> {
        if depth > MAX_SKIP_DEPTH {
            return Err(MsgpackError::DepthExceeded);
        }
        let b = self.peek_byte()?;
        match b {
            0x00..=0x7f | 0xe0..=0xff => {
                self.advance(1);
                Ok(())
            }
            0x80..=0x8f => {
                let n = (b & 0x0f) as usize;
                self.advance(1);
                self.skip_map_entries(n, depth)
            }
            0x90..=0x9f => {
                let n = (b & 0x0f) as usize;
                self.advance(1);
                self.skip_array_entries(n, depth)
            }
            0xa0..=0xbf => {
                let n = (b & 0x1f) as usize;
                self.advance(1);
                self.take_bytes(n)?;
                Ok(())
            }
            0xc0 => {
                self.advance(1);
                Ok(())
            }
            0xc1 => Err(MsgpackError::Reserved(b)),
            0xc2 | 0xc3 => {
                self.advance(1);
                Ok(())
            }
            0xc4 => {
                self.advance(1);
                let n = self.raw_u8()? as usize;
                self.take_bytes(n)?;
                Ok(())
            }
            0xc5 => {
                self.advance(1);
                let n = self.raw_u16()? as usize;
                self.take_bytes(n)?;
                Ok(())
            }
            0xc6 => {
                self.advance(1);
                let n = self.raw_u32()? as usize;
                self.take_bytes(n)?;
                Ok(())
            }
            // ext8/16/32: len(N) + type(1) + data(len).
            0xc7 => {
                self.advance(1);
                let n = self.raw_u8()? as usize;
                self.take_bytes(1)?;
                self.take_bytes(n)?;
                Ok(())
            }
            0xc8 => {
                self.advance(1);
                let n = self.raw_u16()? as usize;
                self.take_bytes(1)?;
                self.take_bytes(n)?;
                Ok(())
            }
            0xc9 => {
                self.advance(1);
                let n = self.raw_u32()? as usize;
                self.take_bytes(1)?;
                self.take_bytes(n)?;
                Ok(())
            }
            0xca => {
                self.advance(1);
                self.take_bytes(4)?;
                Ok(())
            }
            0xcb => {
                self.advance(1);
                self.take_bytes(8)?;
                Ok(())
            }
            0xcc => {
                self.advance(1);
                self.take_bytes(1)?;
                Ok(())
            }
            0xcd => {
                self.advance(1);
                self.take_bytes(2)?;
                Ok(())
            }
            0xce => {
                self.advance(1);
                self.take_bytes(4)?;
                Ok(())
            }
            0xcf => {
                self.advance(1);
                self.take_bytes(8)?;
                Ok(())
            }
            0xd0 => {
                self.advance(1);
                self.take_bytes(1)?;
                Ok(())
            }
            0xd1 => {
                self.advance(1);
                self.take_bytes(2)?;
                Ok(())
            }
            0xd2 => {
                self.advance(1);
                self.take_bytes(4)?;
                Ok(())
            }
            0xd3 => {
                self.advance(1);
                self.take_bytes(8)?;
                Ok(())
            }
            // fixext1/2/4/8/16: type(1) + data(N).
            0xd4 => {
                self.advance(1);
                self.take_bytes(1 + 1)?;
                Ok(())
            }
            0xd5 => {
                self.advance(1);
                self.take_bytes(1 + 2)?;
                Ok(())
            }
            0xd6 => {
                self.advance(1);
                self.take_bytes(1 + 4)?;
                Ok(())
            }
            0xd7 => {
                self.advance(1);
                self.take_bytes(1 + 8)?;
                Ok(())
            }
            0xd8 => {
                self.advance(1);
                self.take_bytes(1 + 16)?;
                Ok(())
            }
            0xd9 => {
                self.advance(1);
                let n = self.raw_u8()? as usize;
                self.take_bytes(n)?;
                Ok(())
            }
            0xda => {
                self.advance(1);
                let n = self.raw_u16()? as usize;
                self.take_bytes(n)?;
                Ok(())
            }
            0xdb => {
                self.advance(1);
                let n = self.raw_u32()? as usize;
                self.take_bytes(n)?;
                Ok(())
            }
            0xdc => {
                self.advance(1);
                let n = self.raw_u16()? as usize;
                self.skip_array_entries(n, depth)
            }
            0xdd => {
                self.advance(1);
                let n = self.raw_u32()? as usize;
                self.skip_array_entries(n, depth)
            }
            0xde => {
                self.advance(1);
                let n = self.raw_u16()? as usize;
                self.skip_map_entries(n, depth)
            }
            0xdf => {
                self.advance(1);
                let n = self.raw_u32()? as usize;
                self.skip_map_entries(n, depth)
            }
        }
    }

    fn skip_array_entries(&mut self, n: usize, depth: usize) -> Result<(), MsgpackError> {
        for _ in 0..n {
            self.skip_value_at_depth(depth + 1)?;
        }
        Ok(())
    }

    fn skip_map_entries(&mut self, n: usize, depth: usize) -> Result<(), MsgpackError> {
        for _ in 0..n {
            self.skip_value_at_depth(depth + 1)?;
            self.skip_value_at_depth(depth + 1)?;
        }
        Ok(())
    }
}

/// Maps a format byte to its coarse [`Type`], or [`MsgpackError::Reserved`] for `0xc1`, the one
/// byte the spec names but assigns no meaning.
fn type_for_prefix(b: u8) -> Result<Type, MsgpackError> {
    Ok(match b {
        0x00..=0x7f => Type::Uint,
        0x80..=0x8f => Type::Map,
        0x90..=0x9f => Type::Array,
        0xa0..=0xbf => Type::Str,
        0xc0 => Type::Nil,
        0xc1 => return Err(MsgpackError::Reserved(b)),
        0xc2 | 0xc3 => Type::Bool,
        0xc4..=0xc6 => Type::Bin,
        0xc7..=0xc9 => Type::Ext,
        0xca | 0xcb => Type::Float,
        0xcc..=0xcf => Type::Uint,
        0xd0..=0xd3 => Type::Int,
        0xd4..=0xd8 => Type::Ext,
        0xd9..=0xdb => Type::Str,
        0xdc | 0xdd => Type::Array,
        0xde | 0xdf => Type::Map,
        0xe0..=0xff => Type::Int,
    })
}

/// A MessagePack writer over an owned `Vec<u8>`. Always picks the smallest format that fits a
/// given value -- the canonical encoding -- the same convention `native::varint` follows for its
/// own varints.
#[derive(Debug, Default)]
pub struct Writer {
    buf: Vec<u8>,
}

impl Writer {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self { buf: Vec::with_capacity(capacity) }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    pub fn write_nil(&mut self) {
        self.buf.push(0xc0);
    }

    pub fn write_bool(&mut self, v: bool) {
        self.buf.push(if v { 0xc3 } else { 0xc2 });
    }

    /// Negative fixint/int8/16/32/64 for a negative value, positive fixint/uint8/16/32/64 (via
    /// [`Self::write_u64`]) for a non-negative one -- the canonical choice for a signed value,
    /// same as `msgpack-c`'s own encoder makes.
    pub fn write_i64(&mut self, v: i64) {
        if v >= 0 {
            self.write_u64(v as u64);
        } else if v >= -32 {
            self.buf.push(v as i8 as u8);
        } else if v >= i8::MIN as i64 {
            self.buf.push(0xd0);
            self.buf.push(v as i8 as u8);
        } else if v >= i16::MIN as i64 {
            self.buf.push(0xd1);
            self.buf.extend_from_slice(&(v as i16).to_be_bytes());
        } else if v >= i32::MIN as i64 {
            self.buf.push(0xd2);
            self.buf.extend_from_slice(&(v as i32).to_be_bytes());
        } else {
            self.buf.push(0xd3);
            self.buf.extend_from_slice(&v.to_be_bytes());
        }
    }

    /// Positive fixint/uint8/16/32/64, smallest that fits.
    pub fn write_u64(&mut self, v: u64) {
        if v <= 0x7f {
            self.buf.push(v as u8);
        } else if v <= u8::MAX as u64 {
            self.buf.push(0xcc);
            self.buf.push(v as u8);
        } else if v <= u16::MAX as u64 {
            self.buf.push(0xcd);
            self.buf.extend_from_slice(&(v as u16).to_be_bytes());
        } else if v <= u32::MAX as u64 {
            self.buf.push(0xce);
            self.buf.extend_from_slice(&(v as u32).to_be_bytes());
        } else {
            self.buf.push(0xcf);
            self.buf.extend_from_slice(&v.to_be_bytes());
        }
    }

    pub fn write_f32(&mut self, v: f32) {
        self.buf.push(0xca);
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// Always `float64` -- msgpack has no "smallest float format that fits" notion worth chasing
    /// (a value that round-trips through `f32` isn't the common case for a metric value), so this
    /// always emits the wider, exact form; call [`Self::write_f32`] explicitly when the narrower
    /// format is wanted.
    pub fn write_f64(&mut self, v: f64) {
        self.buf.push(0xcb);
        self.buf.extend_from_slice(&v.to_be_bytes());
    }

    /// fixstr/str8/16/32, smallest that fits.
    pub fn write_str(&mut self, s: &str) {
        let bytes = s.as_bytes();
        let len = bytes.len();
        if len <= 31 {
            self.buf.push(0xa0 | len as u8);
        } else if len <= u8::MAX as usize {
            self.buf.push(0xd9);
            self.buf.push(len as u8);
        } else if len <= u16::MAX as usize {
            self.buf.push(0xda);
            self.buf.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            debug_assert!(len <= u32::MAX as usize, "string too long for msgpack str32");
            self.buf.push(0xdb);
            self.buf.extend_from_slice(&(len as u32).to_be_bytes());
        }
        self.buf.extend_from_slice(bytes);
    }

    /// bin8/16/32, smallest that fits. Unlike `str`, `bin` has no fixed-width short form.
    pub fn write_bin(&mut self, bytes: &[u8]) {
        let len = bytes.len();
        if len <= u8::MAX as usize {
            self.buf.push(0xc4);
            self.buf.push(len as u8);
        } else if len <= u16::MAX as usize {
            self.buf.push(0xc5);
            self.buf.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            debug_assert!(len <= u32::MAX as usize, "bin too long for msgpack bin32");
            self.buf.push(0xc6);
            self.buf.extend_from_slice(&(len as u32).to_be_bytes());
        }
        self.buf.extend_from_slice(bytes);
    }

    /// Writes the array header only -- the caller writes exactly `len` values immediately after.
    pub fn write_array_len(&mut self, len: usize) {
        if len <= 15 {
            self.buf.push(0x90 | len as u8);
        } else if len <= u16::MAX as usize {
            self.buf.push(0xdc);
            self.buf.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            debug_assert!(len <= u32::MAX as usize, "array too long for msgpack array32");
            self.buf.push(0xdd);
            self.buf.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }

    /// Writes the map header only -- the caller writes exactly `len` key/value pairs immediately
    /// after.
    pub fn write_map_len(&mut self, len: usize) {
        if len <= 15 {
            self.buf.push(0x80 | len as u8);
        } else if len <= u16::MAX as usize {
            self.buf.push(0xde);
            self.buf.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            debug_assert!(len <= u32::MAX as usize, "map too long for msgpack map32");
            self.buf.push(0xdf);
            self.buf.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn written(f: impl FnOnce(&mut Writer)) -> Vec<u8> {
        let mut w = Writer::new();
        f(&mut w);
        w.into_inner()
    }

    // -- Known vectors from the spec -------------------------------------------------------

    #[test]
    fn writes_empty_array() {
        assert_eq!(written(|w| w.write_array_len(0)), vec![0x90]);
    }

    #[test]
    fn writes_one_entry_map() {
        assert_eq!(
            written(|w| {
                w.write_map_len(1);
                w.write_str("a");
                w.write_i64(1);
            }),
            vec![0x81, 0xa1, b'a', 0x01]
        );
    }

    #[test]
    fn writes_negative_fixint() {
        assert_eq!(written(|w| w.write_i64(-1)), vec![0xff]);
    }

    #[test]
    fn read_int_wrapping_casts_any_int_format_to_u64_without_erring() {
        let mut int64_min = vec![0xd3];
        int64_min.extend_from_slice(&i64::MIN.to_be_bytes());
        let mut uint64_big = vec![0xcf];
        uint64_big.extend_from_slice(&(u64::MAX - 1).to_be_bytes());
        for (bytes, want) in
            [(vec![0xff], u64::MAX), (int64_min, i64::MIN as u64), (uint64_big, u64::MAX - 1)]
        {
            let mut r = Reader::new(&bytes);
            assert_eq!(r.read_int_wrapping().unwrap(), want, "{bytes:02x?}");
            assert_eq!(r.remaining(), 0, "the int is consumed exactly once");
        }
    }

    #[test]
    fn writes_int8_boundary() {
        assert_eq!(written(|w| w.write_i64(-33)), vec![0xd0, 0xdf]);
    }

    #[test]
    fn writes_uint8() {
        assert_eq!(written(|w| w.write_u64(255)), vec![0xcc, 0xff]);
    }

    #[test]
    fn writes_uint32() {
        assert_eq!(written(|w| w.write_u64(65536)), vec![0xce, 0x00, 0x01, 0x00, 0x00]);
    }

    #[test]
    fn writes_float64() {
        assert_eq!(
            written(|w| w.write_f64(1.5)),
            vec![0xcb, 0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]
        );
    }

    #[test]
    fn writes_str8_boundary() {
        let s = "a".repeat(32);
        let mut expected = vec![0xd9, 0x20];
        expected.extend_from_slice(s.as_bytes());
        assert_eq!(written(|w| w.write_str(&s)), expected);
    }

    #[test]
    fn writes_bin16_boundary() {
        let bin = vec![7u8; 256];
        let mut expected = vec![0xc5, 0x01, 0x00];
        expected.extend_from_slice(&bin);
        assert_eq!(written(|w| w.write_bin(&bin)), expected);
    }

    #[test]
    fn reads_known_vectors() {
        let mut r = Reader::new(&[0x90]);
        assert_eq!(r.read_array_len().unwrap(), 0);
        assert!(r.is_empty());

        let bytes = [0x81, 0xa1, b'a', 0x01];
        let mut r = Reader::new(&bytes);
        assert_eq!(r.read_map_len().unwrap(), 1);
        assert_eq!(r.read_str().unwrap(), "a");
        assert_eq!(r.read_i64().unwrap(), 1);
        assert!(r.is_empty());

        let mut r = Reader::new(&[0xff]);
        assert_eq!(r.read_i64().unwrap(), -1);

        let mut r = Reader::new(&[0xd0, 0xdf]);
        assert_eq!(r.read_i64().unwrap(), -33);

        let mut r = Reader::new(&[0xcc, 0xff]);
        assert_eq!(r.read_u64().unwrap(), 255);

        let mut r = Reader::new(&[0xce, 0x00, 0x01, 0x00, 0x00]);
        assert_eq!(r.read_u64().unwrap(), 65536);

        let mut r = Reader::new(&[0xcb, 0x3f, 0xf8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
        assert_eq!(r.read_f64().unwrap(), 1.5);
    }

    #[test]
    fn read_str_bytes_is_lossless_on_invalid_utf8() {
        // fixstr, length 1, an invalid UTF-8 byte.
        let bytes = [0xa1, 0xff];
        let mut r = Reader::new(&bytes);
        assert_eq!(r.read_str_bytes().unwrap(), &[0xffu8][..]);

        let mut r = Reader::new(&bytes);
        assert_eq!(r.read_str().unwrap_err(), MsgpackError::InvalidUtf8);
    }

    #[test]
    fn read_nil_or_skips_the_callback_on_nil() {
        let bytes = written(|w| w.write_nil());
        let mut r = Reader::new(&bytes);
        let v: Option<i64> = r.read_nil_or(|r| r.read_i64()).unwrap();
        assert_eq!(v, None);

        let bytes = written(|w| w.write_i64(42));
        let mut r = Reader::new(&bytes);
        let v = r.read_nil_or(|r| r.read_i64()).unwrap();
        assert_eq!(v, Some(42));
    }

    #[test]
    fn read_u64_accepts_uint_and_non_negative_int_formats() {
        let bytes = written(|w| w.write_i64(5));
        let mut r = Reader::new(&bytes);
        assert_eq!(r.read_u64().unwrap(), 5);
    }

    #[test]
    fn read_u64_rejects_negative() {
        let bytes = written(|w| w.write_i64(-5));
        let mut r = Reader::new(&bytes);
        assert_eq!(
            r.read_u64().unwrap_err(),
            MsgpackError::Type { expected: "u64", found: Type::Int }
        );
    }

    #[test]
    fn read_i64_rejects_uint64_above_i64_max() {
        let bytes = written(|w| w.write_u64(u64::MAX));
        let mut r = Reader::new(&bytes);
        assert_eq!(
            r.read_i64().unwrap_err(),
            MsgpackError::Type { expected: "i64", found: Type::Uint }
        );
    }

    #[test]
    fn read_f64_tolerates_int_formats() {
        let bytes = written(|w| w.write_i64(7));
        let mut r = Reader::new(&bytes);
        assert_eq!(r.read_f64().unwrap(), 7.0);
    }

    #[test]
    fn read_f64_reads_float32() {
        let bytes = written(|w| w.write_f32(1.5));
        let mut r = Reader::new(&bytes);
        assert_eq!(r.read_f64().unwrap(), 1.5);
    }

    // -- Truncation never panics -----------------------------------------------------------

    #[test]
    fn truncation_at_every_prefix_is_reported_not_panicked() {
        let bytes = written(|w| {
            w.write_map_len(1);
            w.write_str("nested");
            w.write_array_len(2);
            w.write_i64(-1000);
            w.write_bin(&[1, 2, 3, 4, 5]);
        });
        for len in 0..bytes.len() {
            let mut r = Reader::new(&bytes[..len]);
            match r.skip_value() {
                Err(MsgpackError::Truncated) => {}
                other => panic!("expected Truncated at len {len}, got {other:?}"),
            }
        }
    }

    // -- Reserved byte ------------------------------------------------------------------------

    #[test]
    fn reserved_byte_is_rejected() {
        let bytes = [0xc1u8];
        let r = Reader::new(&bytes);
        assert_eq!(r.peek_type().unwrap_err(), MsgpackError::Reserved(0xc1));

        let mut r = Reader::new(&bytes);
        assert_eq!(r.skip_value().unwrap_err(), MsgpackError::Reserved(0xc1));
    }

    // -- Depth limit --------------------------------------------------------------------------

    #[test]
    fn skip_value_rejects_depth_beyond_64() {
        let mut w = Writer::new();
        for _ in 0..65 {
            w.write_array_len(1);
        }
        w.write_nil();
        let bytes = w.into_inner();
        let mut r = Reader::new(&bytes);
        assert_eq!(r.skip_value().unwrap_err(), MsgpackError::DepthExceeded);
    }

    #[test]
    fn skip_value_accepts_depth_of_64() {
        let mut w = Writer::new();
        for _ in 0..64 {
            w.write_array_len(1);
        }
        w.write_nil();
        let bytes = w.into_inner();
        let mut r = Reader::new(&bytes);
        r.skip_value().unwrap();
        assert!(r.is_empty());
    }

    // -- Property tests: read(write(v)) == v, and skip_value consumes exactly what write did --

    /// A small test-only recursive value type covering every format this module implements
    /// (never `Ext`, since [`Writer`] never emits one).
    #[derive(Debug, Clone, PartialEq)]
    enum Value {
        Nil,
        Bool(bool),
        Int(i64),
        /// A `u64` above `i64::MAX` -- the one range [`Writer::write_i64`] can never produce, so
        /// it's generated (and written) separately via [`Writer::write_u64`].
        UInt(u64),
        Float(f64),
        Str(String),
        Bin(Vec<u8>),
        Array(Vec<Value>),
        Map(Vec<(Value, Value)>),
    }

    fn write_value(w: &mut Writer, v: &Value) {
        match v {
            Value::Nil => w.write_nil(),
            Value::Bool(b) => w.write_bool(*b),
            Value::Int(i) => w.write_i64(*i),
            Value::UInt(u) => w.write_u64(*u),
            Value::Float(f) => w.write_f64(*f),
            Value::Str(s) => w.write_str(s),
            Value::Bin(b) => w.write_bin(b),
            Value::Array(items) => {
                w.write_array_len(items.len());
                for item in items {
                    write_value(w, item);
                }
            }
            Value::Map(entries) => {
                w.write_map_len(entries.len());
                for (k, val) in entries {
                    write_value(w, k);
                    write_value(w, val);
                }
            }
        }
    }

    fn read_value(r: &mut Reader) -> Value {
        match r.peek_type().unwrap() {
            Type::Nil => {
                r.read_nil().unwrap();
                Value::Nil
            }
            Type::Bool => Value::Bool(r.read_bool().unwrap()),
            Type::Int => Value::Int(r.read_i64().unwrap()),
            Type::Uint => {
                let u = r.read_u64().unwrap();
                if u <= i64::MAX as u64 {
                    Value::Int(u as i64)
                } else {
                    Value::UInt(u)
                }
            }
            Type::Float => Value::Float(r.read_f64().unwrap()),
            Type::Str => Value::Str(r.read_str().unwrap().to_string()),
            Type::Bin => Value::Bin(r.read_bin().unwrap().to_vec()),
            Type::Array => {
                let n = r.read_array_len().unwrap();
                Value::Array((0..n).map(|_| read_value(r)).collect())
            }
            Type::Map => {
                let n = r.read_map_len().unwrap();
                Value::Map((0..n).map(|_| (read_value(r), read_value(r))).collect())
            }
            Type::Ext => unreachable!("Writer never emits ext"),
        }
    }

    fn leaf_value() -> impl Strategy<Value = Value> {
        prop_oneof![
            Just(Value::Nil),
            any::<bool>().prop_map(Value::Bool),
            any::<i64>().prop_map(Value::Int),
            ((i64::MAX as u64 + 1)..=u64::MAX).prop_map(Value::UInt),
            any::<f64>().prop_filter("no NaN", |f| !f.is_nan()).prop_map(Value::Float),
            ".{0,24}".prop_map(Value::Str),
            proptest::collection::vec(any::<u8>(), 0..24).prop_map(Value::Bin),
        ]
    }

    fn value_strategy() -> impl Strategy<Value = Value> {
        leaf_value().prop_recursive(4, 64, 8, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..8).prop_map(Value::Array),
                proptest::collection::vec((inner.clone(), inner.clone()), 0..8)
                    .prop_map(Value::Map),
            ]
        })
    }

    proptest! {
        #[test]
        fn round_trip_value(v in value_strategy()) {
            let mut w = Writer::new();
            write_value(&mut w, &v);
            let bytes = w.into_inner();
            let mut r = Reader::new(&bytes);
            let decoded = read_value(&mut r);
            prop_assert_eq!(decoded, v);
            prop_assert!(r.is_empty());
        }

        #[test]
        fn skip_value_consumes_exactly_what_write_produced(v in value_strategy()) {
            let mut w = Writer::new();
            write_value(&mut w, &v);
            let bytes = w.into_inner();
            let mut r = Reader::new(&bytes);
            r.skip_value().unwrap();
            prop_assert!(r.is_empty());
        }
    }
}
