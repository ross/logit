//! The collectd binary protocol's part framing -- the one place in this codec that knows what a
//! byte means. Everything above it ([`super::decode`], [`super::encode`]) works in parts, part
//! types and [`DsValue`]s, never in offsets or endianness.
//!
//! A part is `type: u16 BE, len: u16 BE` followed by `len - 4` payload bytes; `len` **includes**
//! the four-byte header, which is why a `len < 4` is malformed rather than merely empty. String
//! parts carry NUL-terminated text (`len = 4 + n + 1`), numeric parts one `u64` BE (`len = 12`),
//! and a Values part a `u16` data-source count, that many data-source type bytes, and that many
//! eight-byte values (`len = 6 + 9 * count`).
//!
//! **Endianness is not uniform**, which is the single easiest thing to get wrong here: COUNTER,
//! DERIVE and ABSOLUTE values are big-endian like every other integer on the wire, but a GAUGE is
//! an IEEE-754 double in **little-endian** byte order -- collectd writes `htole64` for gauges
//! specifically (`network.c`'s `write_part_values`), a quirk of the protocol rather than a bug in
//! this file. `crates/logit-proto/src/collectd/decode.rs`'s unit tests pin the exact bytes of
//! `1.5` in both directions so a "fix" to this asymmetry fails loudly.

/// Part types, from collectd's `network.h`. `TYPE_MESSAGE`/`TYPE_SEVERITY` are notification parts
/// (W5 of `docs/plans/collectd-binary-relay.md`); `TYPE_SIGNATURE`/`TYPE_ENCRYPTION` are the
/// security parts this codec deliberately does not verify or decrypt (see [`super`]'s module doc).
pub const TYPE_HOST: u16 = 0x0000;
pub const TYPE_TIME: u16 = 0x0001;
pub const TYPE_PLUGIN: u16 = 0x0002;
pub const TYPE_PLUGIN_INSTANCE: u16 = 0x0003;
pub const TYPE_TYPE: u16 = 0x0004;
pub const TYPE_TYPE_INSTANCE: u16 = 0x0005;
pub const TYPE_VALUES: u16 = 0x0006;
pub const TYPE_INTERVAL: u16 = 0x0007;
pub const TYPE_TIME_HR: u16 = 0x0008;
pub const TYPE_INTERVAL_HR: u16 = 0x0009;
pub const TYPE_MESSAGE: u16 = 0x0100;
pub const TYPE_SEVERITY: u16 = 0x0101;
pub const TYPE_SIGNATURE: u16 = 0x0200;
pub const TYPE_ENCRYPTION: u16 = 0x0210;

/// Data-source types, from collectd's `plugin.h` (`DS_TYPE_*`). One byte each in a Values part's
/// type vector.
pub const DS_COUNTER: u8 = 0;
pub const DS_GAUGE: u8 = 1;
pub const DS_DERIVE: u8 = 2;
pub const DS_ABSOLUTE: u8 = 3;

/// Bytes of a part header (`type` + `len`), and therefore the smallest legal `len`.
pub const HEADER_LEN: usize = 4;

/// Bytes of a numeric part: [`HEADER_LEN`] plus one `u64` BE.
pub const NUMBER_PART_LEN: usize = HEADER_LEN + 8;

/// Bytes one data source costs inside a Values part: one type byte plus an eight-byte value.
pub const BYTES_PER_VALUE: usize = 9;

/// Bytes of a Values part's own fixed overhead: [`HEADER_LEN`] plus the `u16` data-source count.
pub const VALUES_OVERHEAD: usize = HEADER_LEN + 2;

/// One data source's value, already interpreted. Keeping the four wire types as one enum is what
/// lets [`super::decode`] and [`super::encode`] share a single definition of which byte order each
/// one uses -- see this module's doc comment on why that matters.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DsValue {
    Counter(u64),
    /// **Little-endian** on the wire, unlike every other value type here.
    Gauge(f64),
    Derive(i64),
    Absolute(u64),
}

impl DsValue {
    /// This value's data-source type byte, as it appears in a Values part's type vector.
    pub fn ds_type(self) -> u8 {
        match self {
            DsValue::Counter(_) => DS_COUNTER,
            DsValue::Gauge(_) => DS_GAUGE,
            DsValue::Derive(_) => DS_DERIVE,
            DsValue::Absolute(_) => DS_ABSOLUTE,
        }
    }

    /// The eight payload bytes for this value, in wire order.
    pub fn to_wire(self) -> [u8; 8] {
        match self {
            DsValue::Counter(v) | DsValue::Absolute(v) => v.to_be_bytes(),
            DsValue::Gauge(v) => v.to_le_bytes(),
            DsValue::Derive(v) => v.to_be_bytes(),
        }
    }

    /// The inverse of [`DsValue::to_wire`] for a known-good `ds_type` (one of the four [`DS_COUNTER`]
    /// .. [`DS_ABSOLUTE`] constants -- the caller validates the whole type vector *before* reading
    /// any value, so an unknown byte never reaches here).
    pub fn from_wire(ds_type: u8, raw: [u8; 8]) -> Option<DsValue> {
        Some(match ds_type {
            DS_COUNTER => DsValue::Counter(u64::from_be_bytes(raw)),
            DS_GAUGE => DsValue::Gauge(f64::from_le_bytes(raw)),
            DS_DERIVE => DsValue::Derive(i64::from_be_bytes(raw)),
            DS_ABSOLUTE => DsValue::Absolute(u64::from_be_bytes(raw)),
            _ => return None,
        })
    }

    /// Whether `ds_type` is one of the four types this protocol defines -- checked over a Values
    /// part's whole type vector before a single value is read or a single event allocated
    /// (`decode.rs`'s ordering contract, and the reason a hostile `count` can't make this codec
    /// allocate).
    pub fn is_known_type(ds_type: u8) -> bool {
        matches!(ds_type, DS_COUNTER | DS_GAUGE | DS_DERIVE | DS_ABSOLUTE)
    }
}

/// A part's header: its type and its total length *including* the four header bytes, so a caller
/// advances by exactly `len` to reach the next part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartHeader {
    pub part_type: u16,
    pub len: usize,
}

/// Why a part could not be read. Every variant means "the rest of this datagram is not
/// trustworthy": [`super::decode`] turns one into either a `bad_part` diagnostic (when earlier
/// parts of the same datagram already produced events) or a `CodecError::Malformed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PartError {
    /// Fewer than [`HEADER_LEN`] bytes remain -- a trailing fragment, not a part.
    #[error("part header needs {HEADER_LEN} bytes, only {remaining} remain")]
    ShortHeader { remaining: usize },
    /// A declared length below [`HEADER_LEN`], which would make the payload negative and (for a
    /// `len` of 0) never advance the cursor at all.
    #[error("part declares a length of {len}, below the {HEADER_LEN}-byte header")]
    ShortPart { len: usize },
    /// A declared length past the end of the datagram -- a truncated packet, or a hostile length.
    #[error("part declares {len} bytes, only {remaining} remain")]
    Overlong { len: usize, remaining: usize },
}

/// Reads the part starting at `at`, returning its header and payload (the `len - 4` bytes after the
/// header). Validates only what framing requires: that a header fits, that `len` is at least
/// [`HEADER_LEN`], and that `len` does not run past the end of `bytes`. Payload *shape* -- a
/// string's NUL terminator, a numeric part's width, a Values part's count -- is the caller's, since
/// each part type has its own rule.
pub fn read_part(bytes: &[u8], at: usize) -> Result<(PartHeader, &[u8]), PartError> {
    let remaining = bytes.len().saturating_sub(at);
    if remaining < HEADER_LEN {
        return Err(PartError::ShortHeader { remaining });
    }
    let part_type = u16::from_be_bytes([bytes[at], bytes[at + 1]]);
    let len = u16::from_be_bytes([bytes[at + 2], bytes[at + 3]]) as usize;
    if len < HEADER_LEN {
        return Err(PartError::ShortPart { len });
    }
    if len > remaining {
        return Err(PartError::Overlong { len, remaining });
    }
    Ok((PartHeader { part_type, len }, &bytes[at + HEADER_LEN..at + len]))
}

/// Writes a NUL-terminated string part. `value` is written byte-verbatim (it is already sanitized
/// and length-bounded by the encoder -- see [`super::encode`]'s sanitizer), so this function never
/// inspects or truncates it.
///
/// # Panics
///
/// Panics rather than truncating a `u16` length, for the reason [`write_values_part`] documents at
/// length. The encoder's sanitizer caps every identity field at [`super::DATA_MAX_NAME_LEN`] minus
/// its NUL, so the longest part this can produce is 132 bytes.
pub fn write_string_part(out: &mut Vec<u8>, part_type: u16, value: &[u8]) {
    let len = HEADER_LEN + value.len() + 1;
    let len = u16::try_from(len)
        .expect("an identity field is sanitized to DATA_MAX_NAME_LEN, far inside a u16 length");
    out.extend_from_slice(&part_type.to_be_bytes());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    out.push(0);
}

/// Writes a `u64`-BE numeric part ([`TYPE_TIME_HR`]/[`TYPE_INTERVAL_HR`] and their legacy
/// second-resolution siblings).
pub fn write_number_part(out: &mut Vec<u8>, part_type: u16, value: u64) {
    out.extend_from_slice(&part_type.to_be_bytes());
    out.extend_from_slice(&(NUMBER_PART_LEN as u16).to_be_bytes());
    out.extend_from_slice(&value.to_be_bytes());
}

/// Writes a Values part: the data-source count, then every type byte, then every eight-byte value
/// -- the two-vector layout collectd's own `write_part_values` uses, not one interleaved
/// type/value pair per data source.
///
/// # Panics
///
/// Both the part length and the data-source count are `u16` on the wire, so this panics rather than
/// truncating if `values` holds more than [`super::MAX_VALUES_PER_LIST`] entries. That is an
/// invariant [`super::encode::CollectdEncoder::encode_into`] establishes before ever calling here
/// -- a longer list is dropped whole and counted
/// `logit.output.metrics.skipped{reason="too_many_values"}` -- so reaching either check means a
/// caller bypassed that cap, not that an operator sent something unusual. A silent `as u16` would be
/// far worse than a panic: at 7282 data sources the declared length wraps to 8, shorter than the
/// part's own header, and every receiver then reads structural garbage from a datagram that looks
/// well-formed.
pub fn write_values_part(out: &mut Vec<u8>, values: &[DsValue]) {
    debug_assert!(
        values.len() <= super::MAX_VALUES_PER_LIST,
        "a value list of {} exceeds MAX_VALUES_PER_LIST ({}); the encoder caps this before here",
        values.len(),
        super::MAX_VALUES_PER_LIST
    );
    let len = VALUES_OVERHEAD + BYTES_PER_VALUE * values.len();
    let len = u16::try_from(len)
        .expect("a values part is at most MAX_VALUES_PER_LIST sources, far inside a u16 length");
    let count = u16::try_from(values.len())
        .expect("a values part is at most MAX_VALUES_PER_LIST sources, far inside a u16 count");
    out.extend_from_slice(&TYPE_VALUES.to_be_bytes());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(&count.to_be_bytes());
    for value in values {
        out.push(value.ds_type());
    }
    for value in values {
        out.extend_from_slice(&value.to_wire());
    }
}
