//! A hand-rolled decoder for the records blob a Kafka Fetch response carries: zero or more record
//! batches back to back. It is synchronous and I/O-free, and [`decode`] is the fuzz entry point
//! (`docs/adr/kafka-input.md`, items 9, 10, and 12).
//!
//! `kafka-protocol`'s `RecordBatchDecoder` isn't used, because its `Record` collapses repeated
//! header names into a map and requires UTF-8 names. This decoder keeps headers in wire order,
//! repeats and non-UTF-8 names included, and slices keys, values, and header fields out of the
//! (decompressed) buffer without copying.
//!
//! The layout is Kafka's `DefaultRecordBatch` and `DefaultRecord`
//! (<https://kafka.apache.org/documentation/#recordbatch>). A batch opens with a 61-byte
//! big-endian header: base offset (8), batch length (4, the bytes after this field), partition
//! leader epoch (4), magic (1, byte 16), CRC-32C (4), attributes (2), last offset delta (4), base
//! timestamp (8), max timestamp (8), producer id (8), producer epoch (2), base sequence (4), and
//! record count (4). The records section follows, compressed as a whole when attributes bits 0-2
//! are not 0. The CRC covers byte 21 (attributes) to the end of the batch.
//!
//! Framing and error rules:
//!
//! - A blob can end with a truncated batch, because a broker cuts a response at its byte limit.
//!   Fewer than 12 bytes left, or a batch length that reaches past the end, ends decoding with no
//!   entry; the consumer refetches from there. When the cut batch's first 12 bytes are there, its
//!   base offset is [`Decoded::partial_base`].
//! - A negative batch length, or one too short to hold a header, is [`BatchErrorKind::Corrupt`]
//!   with no last offset, and ends decoding, because the next batch's start is unknown.
//! - Magic 0 and 1 are [`BatchErrorKind::LegacyFormat`] and skipped; any other magic but 2 is
//!   `Corrupt` and skipped.
//! - A bad CRC, a record that doesn't consume its declared length, a record count that doesn't
//!   match the section, or a codec failure other than exceeding the bound is `Corrupt` for the
//!   whole batch. A section that decompresses past `max_records_bytes` is
//!   [`BatchErrorKind::Oversize`].
//! - Control batches decode like any other; the record key holds the control type and the caller
//!   interprets it.
//!
//! Every allocation is bounded by the input length or `max_records_bytes`, and no input panics.

use bytes::Bytes;
use kafka_protocol::records::Compression;

use super::compression::{decompress, TooLarge};

/// The record batch v2 header length.
const HEADER: usize = 61;
/// The smallest batch length (bytes after the length field) that holds a v2 header.
const MIN_BATCH_LENGTH: usize = HEADER - 12;
/// The smallest encoded record: a length, attributes, timestamp delta, offset delta, key length,
/// value length, and header count, one byte each.
const MIN_RECORD: usize = 7;

/// Whether a record's timestamp is set by the producer or by the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimestampType {
    CreateTime,
    LogAppendTime,
}

/// One decoded record batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordBatch {
    pub base_offset: i64,
    pub partition_leader_epoch: i32,
    pub attributes: i16,
    pub last_offset_delta: i32,
    pub base_timestamp: i64,
    pub max_timestamp: i64,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
    pub records: Vec<Record>,
}

impl RecordBatch {
    /// The codec in attributes bits 0-2. A decoded batch never holds an unknown code, so the
    /// fallback to `None` is unreachable for one.
    pub fn compression(&self) -> Compression {
        codec(self.attributes).unwrap_or(Compression::None)
    }

    /// Attributes bit 3.
    pub fn timestamp_type(&self) -> TimestampType {
        timestamp_type(self.attributes)
    }

    /// Attributes bit 4.
    pub fn is_transactional(&self) -> bool {
        self.attributes & 0x10 != 0
    }

    /// Attributes bit 5.
    pub fn is_control(&self) -> bool {
        self.attributes & 0x20 != 0
    }

    /// The batch's last offset: `base_offset + last_offset_delta`.
    pub fn last_offset(&self) -> i64 {
        self.base_offset.wrapping_add(i64::from(self.last_offset_delta))
    }
}

/// One decoded record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The batch's base offset plus this record's offset delta.
    pub offset: i64,
    /// The batch's base timestamp plus this record's delta, or under `LogAppendTime` the batch's
    /// max timestamp, as Java's `DefaultRecordBatch` reports it.
    pub timestamp: i64,
    pub timestamp_type: TimestampType,
    /// A null key (length -1) is `None`; an empty one is `Some` of no bytes.
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    /// In wire order, repeated names kept.
    pub headers: Vec<Header>,
}

/// One record header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub name: Bytes,
    pub value: Option<Bytes>,
}

impl Header {
    /// The name as text, or `None` when it isn't UTF-8; the caller counts that case.
    pub fn name_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.name).ok()
    }
}

/// Why a batch wasn't decoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchErrorKind {
    /// A bad CRC, a malformed header or record, or a codec failure.
    Corrupt,
    /// The records section decompresses past the caller's bound.
    Oversize,
    /// A magic 0 or 1 message, which this decoder doesn't read.
    LegacyFormat,
}

/// A batch that [`decode`] couldn't read, with what a consumer needs to skip past it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{kind:?} record batch at offset {base_offset}: {reason}")]
pub struct BatchError {
    pub kind: BatchErrorKind,
    pub base_offset: i64,
    /// The batch's last offset when its header was readable; for magic 0 and 1, the entry's own
    /// offset.
    pub last_offset: Option<i64>,
    /// The records the header declares, 0 when unknown or legacy.
    pub record_count: i32,
    /// Text for a diagnostic, never holding record bytes.
    pub reason: String,
}

fn codec(attributes: i16) -> Option<Compression> {
    match attributes & 0x7 {
        0 => Some(Compression::None),
        1 => Some(Compression::Gzip),
        2 => Some(Compression::Snappy),
        3 => Some(Compression::Lz4),
        4 => Some(Compression::Zstd),
        _ => None,
    }
}

fn timestamp_type(attributes: i16) -> TimestampType {
    if attributes & 0x8 != 0 {
        TimestampType::LogAppendTime
    } else {
        TimestampType::CreateTime
    }
}

fn be_i64(b: &[u8], at: usize) -> i64 {
    i64::from_be_bytes(b[at..at + 8].try_into().unwrap_or_default())
}

fn be_i32(b: &[u8], at: usize) -> i32 {
    i32::from_be_bytes(b[at..at + 4].try_into().unwrap_or_default())
}

fn be_i16(b: &[u8], at: usize) -> i16 {
    i16::from_be_bytes(b[at..at + 2].try_into().unwrap_or_default())
}

/// What [`decode`] read from one records blob.
#[derive(Debug, Default)]
pub struct Decoded {
    /// Every complete batch, in order.
    pub entries: Vec<Result<RecordBatch, BatchError>>,
    /// The base offset of a batch the blob ends inside of, when at least its first 12 bytes
    /// (base offset and batch length) are there. No CRC covers them.
    pub partial_base: Option<i64>,
}

/// Decodes every complete batch in `bytes`, in order. Each batch's records section decompresses
/// to at most `max_records_bytes`.
pub fn decode(bytes: &Bytes, max_records_bytes: usize) -> Decoded {
    let mut out = Vec::new();
    let mut partial_base = None;
    let mut pos = 0usize;
    while bytes.len() - pos >= 12 {
        let head = &bytes[pos..];
        let base_offset = be_i64(head, 0);
        let length = be_i32(head, 8);
        let error = |kind, last_offset, reason: &str| BatchError {
            kind,
            base_offset,
            last_offset,
            record_count: 0,
            reason: reason.to_owned(),
        };
        let Ok(length) = usize::try_from(length) else {
            out.push(Err(error(BatchErrorKind::Corrupt, None, "negative batch length")));
            break;
        };
        let total = 12 + length;
        if total > head.len() {
            partial_base = Some(base_offset);
            break;
        }
        let batch = bytes.slice(pos..pos + total);
        pos += total;
        if length < 5 {
            out.push(Err(error(BatchErrorKind::Corrupt, None, "batch length too short")));
            break;
        }
        match batch[16] {
            0 | 1 => {
                out.push(Err(error(
                    BatchErrorKind::LegacyFormat,
                    Some(base_offset),
                    "magic 0 or 1 message set",
                )));
                continue;
            }
            2 => {}
            _ => {
                out.push(Err(error(BatchErrorKind::Corrupt, None, "unknown magic")));
                continue;
            }
        }
        if length < MIN_BATCH_LENGTH {
            out.push(Err(error(BatchErrorKind::Corrupt, None, "batch length too short")));
            break;
        }
        out.push(decode_v2(&batch, max_records_bytes));
    }
    Decoded { entries: out, partial_base }
}

/// Decodes one magic-2 batch of at least [`HEADER`] bytes.
fn decode_v2(batch: &Bytes, max_records_bytes: usize) -> Result<RecordBatch, BatchError> {
    let base_offset = be_i64(batch, 0);
    let attributes = be_i16(batch, 21);
    let last_offset_delta = be_i32(batch, 23);
    let record_count = be_i32(batch, 57);
    let fail = |kind, reason: &str| BatchError {
        kind,
        base_offset,
        last_offset: Some(base_offset.wrapping_add(i64::from(last_offset_delta))),
        record_count,
        reason: reason.to_owned(),
    };
    let corrupt = |reason: &str| fail(BatchErrorKind::Corrupt, reason);

    let stored_crc = u32::from_be_bytes(batch[17..21].try_into().unwrap_or_default());
    if crc32c::crc32c(&batch[21..]) != stored_crc {
        return Err(corrupt("CRC-32C mismatch"));
    }
    let Some(codec) = codec(attributes) else {
        return Err(corrupt("unknown compression code"));
    };
    let section = if codec == Compression::None {
        if batch.len() - HEADER > max_records_bytes {
            return Err(fail(BatchErrorKind::Oversize, "records section over the bound"));
        }
        batch.slice(HEADER..)
    } else {
        match decompress(codec, &batch[HEADER..], max_records_bytes) {
            Ok(plain) => Bytes::from(plain),
            Err(err) if err.downcast_ref::<TooLarge>().is_some() => {
                return Err(fail(BatchErrorKind::Oversize, "records section over the bound"));
            }
            Err(_) => return Err(corrupt("records section fails to decompress")),
        }
    };

    let base_timestamp = be_i64(batch, 27);
    let max_timestamp = be_i64(batch, 35);
    let log_append = timestamp_type(attributes) == TimestampType::LogAppendTime;
    let records = parse_records(
        section,
        record_count,
        base_offset,
        base_timestamp,
        log_append.then_some(max_timestamp),
    )
    .map_err(corrupt)?;
    Ok(RecordBatch {
        base_offset,
        partition_leader_epoch: be_i32(batch, 8 + 4),
        attributes,
        last_offset_delta,
        base_timestamp,
        max_timestamp,
        producer_id: be_i64(batch, 43),
        producer_epoch: be_i16(batch, 51),
        base_sequence: be_i32(batch, 53),
        records,
    })
}

/// Reads `count` records from `section`, which must hold nothing else.
fn parse_records(
    section: Bytes,
    count: i32,
    base_offset: i64,
    base_timestamp: i64,
    append_time: Option<i64>,
) -> Result<Vec<Record>, &'static str> {
    let count = usize::try_from(count).map_err(|_| "negative record count")?;
    if count > section.len() / MIN_RECORD {
        return Err("record count exceeds the records section");
    }
    let timestamp_type = if append_time.is_some() {
        TimestampType::LogAppendTime
    } else {
        TimestampType::CreateTime
    };
    let mut outer = Reader { end: section.len(), buf: section, pos: 0 };
    let mut records = Vec::with_capacity(count);
    for _ in 0..count {
        let len = usize::try_from(outer.varint()?).map_err(|_| "negative record length")?;
        if len > outer.end - outer.pos {
            return Err("record reaches past the records section");
        }
        let mut r = Reader { buf: outer.buf.clone(), pos: outer.pos, end: outer.pos + len };
        outer.pos += len;
        r.byte()?; // attributes, unused
        let timestamp_delta = r.varlong()?;
        let offset_delta = r.varint()?;
        let key = r.nullable()?;
        let value = r.nullable()?;
        let header_count = usize::try_from(r.varint()?).map_err(|_| "negative header count")?;
        // A header is at least a key length and a value length.
        if header_count > (r.end - r.pos) / 2 {
            return Err("header count exceeds the record");
        }
        let mut headers = Vec::with_capacity(header_count);
        for _ in 0..header_count {
            let name_len =
                usize::try_from(r.varint()?).map_err(|_| "negative header key length")?;
            let name = r.take(name_len)?;
            headers.push(Header { name, value: r.nullable()? });
        }
        if r.pos != r.end {
            return Err("record shorter than its declared length");
        }
        records.push(Record {
            offset: base_offset.wrapping_add(i64::from(offset_delta)),
            timestamp: append_time.unwrap_or_else(|| base_timestamp.wrapping_add(timestamp_delta)),
            timestamp_type,
            key,
            value,
            headers,
        });
    }
    if outer.pos != outer.end {
        return Err("bytes left after the declared records");
    }
    Ok(records)
}

/// A cursor over `buf[pos..end]` that never reads past `end`.
struct Reader {
    buf: Bytes,
    pos: usize,
    end: usize,
}

const SHORT: &str = "record field reaches past its end";

impl Reader {
    fn byte(&mut self) -> Result<u8, &'static str> {
        if self.pos >= self.end {
            return Err(SHORT);
        }
        self.pos += 1;
        Ok(self.buf[self.pos - 1])
    }

    fn take(&mut self, n: usize) -> Result<Bytes, &'static str> {
        if n > self.end - self.pos {
            return Err(SHORT);
        }
        self.pos += n;
        Ok(self.buf.slice(self.pos - n..self.pos))
    }

    /// A zigzag varint of at most `max_bytes` bytes.
    fn zigzag(&mut self, max_bytes: u32) -> Result<i64, &'static str> {
        let mut raw = 0u64;
        for i in 0..max_bytes {
            let b = self.byte()?;
            let bits = u64::from(b & 0x7f);
            if i == 9 && bits > 1 {
                return Err("varint overflows 64 bits");
            }
            raw |= bits << (7 * i);
            if b & 0x80 == 0 {
                return Ok(((raw >> 1) as i64) ^ -((raw & 1) as i64));
            }
        }
        Err("varint too long")
    }

    fn varint(&mut self) -> Result<i32, &'static str> {
        i32::try_from(self.zigzag(5)?).map_err(|_| "varint overflows 32 bits")
    }

    fn varlong(&mut self) -> Result<i64, &'static str> {
        self.zigzag(10)
    }

    /// A varint-length byte string where -1 is null.
    fn nullable(&mut self) -> Result<Option<Bytes>, &'static str> {
        match self.varint()? {
            -1 => Ok(None),
            n => {
                let n = usize::try_from(n).map_err(|_| "negative field length")?;
                self.take(n).map(Some)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::protocol::StrBytes;

    use crate::kafka::batch::{encode, ProducerRecord, Stamp};

    const MAX: usize = 1 << 20;
    const CODECS: [(&str, Compression); 5] = [
        ("none", Compression::None),
        ("gzip", Compression::Gzip),
        ("snappy", Compression::Snappy),
        ("lz4", Compression::Lz4),
        ("zstd", Compression::Zstd),
    ];

    fn corpus(name: &str) -> Bytes {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/kafka/batches")
            .join(name);
        Bytes::from(std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())))
    }

    fn varint(out: &mut Vec<u8>, v: i64) {
        let mut z = ((v << 1) ^ (v >> 63)) as u64;
        while z >= 0x80 {
            out.push((z as u8 & 0x7f) | 0x80);
            z >>= 7;
        }
        out.push(z as u8);
    }

    fn field(out: &mut Vec<u8>, f: Option<&[u8]>) {
        match f {
            None => varint(out, -1),
            Some(b) => {
                varint(out, b.len() as i64);
                out.extend_from_slice(b);
            }
        }
    }

    type Hdr<'a> = (&'a [u8], Option<&'a [u8]>);

    fn record(ts: i64, off: i64, key: Option<&[u8]>, value: Option<&[u8]>, hs: &[Hdr]) -> Vec<u8> {
        let mut body = vec![0u8];
        varint(&mut body, ts);
        varint(&mut body, off);
        field(&mut body, key);
        field(&mut body, value);
        varint(&mut body, hs.len() as i64);
        for (n, v) in hs {
            field(&mut body, Some(n));
            field(&mut body, *v);
        }
        let mut out = Vec::new();
        varint(&mut out, body.len() as i64);
        out.extend(body);
        out
    }

    fn refresh_crc(b: &mut [u8]) {
        let crc = crc32c::crc32c(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
    }

    /// A magic-2 batch over `section`, uncompressed unless `attributes` says otherwise.
    fn batch(attributes: i16, count: i32, section: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.extend_from_slice(&100i64.to_be_bytes());
        b.extend_from_slice(&((section.len() + MIN_BATCH_LENGTH) as i32).to_be_bytes());
        b.extend_from_slice(&0i32.to_be_bytes());
        b.push(2);
        b.extend_from_slice(&0u32.to_be_bytes());
        b.extend_from_slice(&attributes.to_be_bytes());
        b.extend_from_slice(&(count - 1).to_be_bytes());
        b.extend_from_slice(&1_000i64.to_be_bytes());
        b.extend_from_slice(&1_050i64.to_be_bytes());
        b.extend_from_slice(&(-1i64).to_be_bytes());
        b.extend_from_slice(&(-1i16).to_be_bytes());
        b.extend_from_slice(&(-1i32).to_be_bytes());
        b.extend_from_slice(&count.to_be_bytes());
        b.extend_from_slice(section);
        refresh_crc(&mut b);
        b
    }

    fn one(b: Vec<u8>) -> Result<RecordBatch, BatchError> {
        let mut got = decode(&Bytes::from(b), MAX).entries;
        assert_eq!(got.len(), 1);
        got.remove(0)
    }

    /// The reason a one-record batch over `body` (one record's fields, unframed) fails with.
    fn record_failure(body: &[u8], count: i32) -> String {
        let mut section = Vec::new();
        varint(&mut section, body.len() as i64);
        section.extend_from_slice(body);
        one(batch(0, count, &section)).unwrap_err().reason
    }

    /// A record's fields up to its header count: attributes, timestamp and offset deltas, and a
    /// null key and value.
    fn fields(header_count: i64) -> Vec<u8> {
        let mut body = vec![0u8];
        varint(&mut body, 0);
        varint(&mut body, 0);
        field(&mut body, None);
        field(&mut body, None);
        varint(&mut body, header_count);
        body
    }

    /// The bound keeps `Vec::with_capacity` from sizing itself by the header's count.
    #[test]
    fn a_record_count_the_section_cant_hold_is_refused_before_reading() {
        let reason = record_failure(&fields(0), 1_000);
        assert!(reason.contains("record count exceeds the records section"), "{reason}");
    }

    #[test]
    fn a_header_count_the_record_cant_hold_is_refused_before_reading() {
        let reason = record_failure(&fields(50), 1);
        assert!(reason.contains("header count exceeds the record"), "{reason}");
    }

    /// Java refuses a record whose fields end before its declared length.
    #[test]
    fn a_record_with_bytes_left_inside_its_length_is_refused() {
        let mut body = fields(0);
        body.push(0);
        let reason = record_failure(&body, 1);
        assert!(reason.contains("record shorter than its declared length"), "{reason}");
    }

    /// Six bytes encoding 0 for the offset delta, one past an `int32` varint's five.
    #[test]
    fn a_varint_over_five_bytes_is_refused() {
        let mut body = vec![0u8];
        varint(&mut body, 0);
        body.extend_from_slice(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x00]);
        field(&mut body, None);
        field(&mut body, None);
        varint(&mut body, 0);
        let reason = record_failure(&body, 1);
        assert!(reason.contains("varint too long"), "{reason}");
    }

    /// A timestamp delta whose tenth byte carries more than the 64th bit.
    #[test]
    fn a_varlong_past_64_bits_is_refused() {
        let mut body = vec![0u8];
        body.extend_from_slice(&[0xff; 9]);
        body.push(0x02);
        varint(&mut body, 0);
        field(&mut body, None);
        field(&mut body, None);
        varint(&mut body, 0);
        let reason = record_failure(&body, 1);
        assert!(reason.contains("varint overflows 64 bits"), "{reason}");
    }

    fn kind(b: Vec<u8>) -> BatchErrorKind {
        one(b).unwrap_err().kind
    }

    fn sample(n: usize) -> Vec<ProducerRecord> {
        (0..n)
            .map(|i| {
                let mut headers = IndexMap::new();
                headers.insert(StrBytes::from_static_str("h1"), Some(Bytes::from_static(b"hv1")));
                ProducerRecord {
                    key: (i % 2 == 0).then(|| Bytes::from(format!("key-{i}"))),
                    value: Some(Bytes::from(format!("the quick brown fox, record {i}"))),
                    headers,
                    timestamp: 1_791_739_946_221 + i as i64,
                }
            })
            .collect()
    }

    #[test]
    fn every_recorded_batch_decodes_with_its_dumped_contents() {
        let values = ["one", "two", "three"]
            .map(|n| format!("the quick brown fox jumps over the lazy dog, record {n}"));
        let timestamps = [
            ("java", [1_791_739_946_221, 1_791_739_946_229, 1_791_739_946_229]),
            ("kcat", [1_791_739_961_117; 3]),
        ];
        for (producer, times) in timestamps {
            for (name, codec) in CODECS {
                let file = format!("{producer}-{name}.segment");
                let got = decode(&corpus(&file), MAX).entries;
                assert_eq!(got.len(), 1, "{file}");
                let b = got[0].as_ref().unwrap_or_else(|e| panic!("{file}: {e}"));
                assert_eq!(b.compression(), codec, "{file}");
                assert_eq!(b.timestamp_type(), TimestampType::CreateTime, "{file}");
                assert!(!b.is_transactional() && !b.is_control(), "{file}");
                assert_eq!(b.records.len(), 3, "{file}");
                assert_eq!(b.last_offset(), 2, "{file}");
                if producer == "java" {
                    assert!(b.producer_id >= 0, "{file}");
                    assert_eq!(b.base_sequence, 0, "{file}");
                } else {
                    assert_eq!((b.producer_id, b.base_sequence), (-1, -1), "{file}");
                }
                for (i, r) in b.records.iter().enumerate() {
                    assert_eq!(r.offset, i as i64, "{file}");
                    assert_eq!(r.key.as_deref(), Some(format!("key-{}", i + 1).as_bytes()));
                    assert_eq!(r.value.as_deref(), Some(values[i].as_bytes()), "{file}");
                    assert_eq!(r.headers.len(), 1, "{file}");
                    assert_eq!(r.headers[0].name_str(), Some("h1"), "{file}");
                    assert_eq!(r.headers[0].value.as_deref(), Some(&b"hv1"[..]), "{file}");
                    assert_eq!(r.timestamp_type, TimestampType::CreateTime);
                    if name == "none" {
                        assert_eq!(r.timestamp, times[i], "{file}");
                    }
                }
            }
        }
    }

    #[test]
    fn an_encoded_batch_round_trips_for_every_codec() {
        let records = sample(5);
        let stamp = Stamp { producer_id: 7, producer_epoch: 3, base_sequence: 40 };
        for (name, codec) in CODECS {
            let got = decode(&encode(&records, stamp, codec).unwrap(), MAX).entries;
            assert_eq!(got.len(), 1, "{name}");
            let b = got[0].as_ref().unwrap();
            assert_eq!(b.compression(), codec, "{name}");
            assert_eq!((b.producer_id, b.producer_epoch, b.base_sequence), (7, 3, 40));
            assert_eq!(b.records.len(), records.len(), "{name}");
            for (got, want) in b.records.iter().zip(&records) {
                assert_eq!(got.key, want.key, "{name}");
                assert_eq!(got.value, want.value, "{name}");
                assert_eq!(got.timestamp, want.timestamp, "{name}");
                assert_eq!(got.headers.len(), 1, "{name}");
                assert_eq!(got.headers[0].name_str(), Some("h1"), "{name}");
                assert_eq!(got.headers[0].value.as_deref(), Some(&b"hv1"[..]), "{name}");
            }
        }
    }

    #[test]
    fn repeated_and_non_utf8_header_names_are_kept_in_order() {
        let hs: [Hdr; 4] =
            [(b"h", Some(b"a")), (b"h", Some(b"b")), (b"\xff\xfe", None), (b"", Some(b""))];
        let section =
            [record(0, 0, None, Some(b"v"), &hs), record(3, 1, Some(b""), None, &[])].concat();
        let b = one(batch(0, 2, &section)).unwrap();
        let h = &b.records[0].headers;
        assert_eq!(h.len(), 4);
        assert_eq!((h[0].name_str(), h[0].value.as_deref()), (Some("h"), Some(&b"a"[..])));
        assert_eq!((h[1].name_str(), h[1].value.as_deref()), (Some("h"), Some(&b"b"[..])));
        assert_eq!((h[2].name_str(), h[2].value.clone()), (None, None));
        assert_eq!(&h[2].name[..], b"\xff\xfe");
        assert_eq!(h[3].value.as_deref(), Some(&b""[..]));
        assert_eq!(b.records[0].key, None);
        assert_eq!(b.records[1].key.as_deref(), Some(&b""[..]));
        assert_eq!(b.records[1].value, None);
        assert_eq!(b.records[1].timestamp, 1_003);
        assert_eq!(b.records[1].offset, 101);
    }

    #[test]
    fn a_flipped_crc_byte_is_corrupt_with_a_readable_header() {
        let mut b = batch(0, 1, &record(0, 0, None, Some(b"v"), &[]));
        b[18] ^= 1;
        let err = one(b).unwrap_err();
        assert_eq!(err.kind, BatchErrorKind::Corrupt);
        assert_eq!((err.base_offset, err.last_offset, err.record_count), (100, Some(100), 1));
    }

    #[test]
    fn a_truncated_record_under_a_valid_crc_is_corrupt() {
        let mut b = batch(0, 1, &record(0, 0, Some(b"k"), Some(b"value"), &[]));
        b[HEADER] = 0x02; // declared record length 1
        refresh_crc(&mut b);
        assert_eq!(kind(b), BatchErrorKind::Corrupt);
    }

    #[test]
    fn a_record_count_that_disagrees_with_the_section_is_corrupt() {
        let section =
            [record(0, 0, None, Some(b"a"), &[]), record(1, 1, None, Some(b"b"), &[])].concat();
        assert_eq!(kind(batch(0, 1, &section)), BatchErrorKind::Corrupt);
        assert_eq!(kind(batch(0, 3, &section)), BatchErrorKind::Corrupt);
        assert_eq!(kind(batch(0, i32::MAX, &section)), BatchErrorKind::Corrupt);
        assert_eq!(kind(batch(0, -1, &section)), BatchErrorKind::Corrupt);
    }

    #[test]
    fn a_negative_header_key_length_is_corrupt() {
        let mut body = vec![0u8];
        varint(&mut body, 0);
        varint(&mut body, 0);
        field(&mut body, None);
        field(&mut body, None);
        varint(&mut body, 1);
        varint(&mut body, -1);
        varint(&mut body, -1);
        let mut section = Vec::new();
        varint(&mut section, body.len() as i64);
        section.extend(body);
        assert_eq!(kind(batch(0, 1, &section)), BatchErrorKind::Corrupt);
    }

    #[test]
    fn an_uncompressed_section_is_bounded_at_its_length() {
        let section = record(0, 0, None, Some(&[b'x'; 300]), &[]);
        let b = batch(0, 1, &section);
        let blob = Bytes::from(b);
        assert!(decode(&blob, section.len()).entries[0].is_ok());
        let err = decode(&blob, section.len() - 1).entries.remove(0).unwrap_err();
        assert_eq!(err.kind, BatchErrorKind::Oversize);
        assert_eq!(err.last_offset, Some(100));
    }

    #[test]
    fn a_compressed_section_over_the_bound_is_oversize() {
        let mut records = sample(1);
        records[0].value = Some(Bytes::from(vec![b'z'; 50_000]));
        for (name, codec) in CODECS.into_iter().skip(1) {
            let blob = encode(&records, Stamp::NONE, codec).unwrap();
            assert!(blob.len() < 5_000, "{name} compresses the run");
            let err = decode(&blob, 10_000).entries.remove(0).unwrap_err();
            assert_eq!(err.kind, BatchErrorKind::Oversize, "{name}");
        }
    }

    #[test]
    fn a_corrupt_compressed_payload_is_corrupt_not_oversize() {
        let mut b = batch(1, 1, b"not gzip at all");
        refresh_crc(&mut b);
        assert_eq!(kind(b), BatchErrorKind::Corrupt);
        assert_eq!(kind(batch(5, 1, b"x")), BatchErrorKind::Corrupt);
    }

    fn legacy_message_set() -> Vec<u8> {
        let mut body = vec![1u8, 0]; // magic 1, attributes
        body.extend_from_slice(&1_000i64.to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes());
        body.extend_from_slice(&1i32.to_be_bytes());
        body.push(b'v');
        let mut out = 7i64.to_be_bytes().to_vec();
        out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
        out.extend_from_slice(&0u32.to_be_bytes());
        out.extend(body);
        out
    }

    #[test]
    fn a_legacy_message_set_is_reported_and_the_next_batch_still_decodes() {
        let good = batch(0, 1, &record(0, 0, None, Some(b"v"), &[]));
        let blob = Bytes::from([legacy_message_set(), good].concat());
        let got = decode(&blob, MAX).entries;
        assert_eq!(got.len(), 2);
        let err = got[0].as_ref().unwrap_err();
        assert_eq!(err.kind, BatchErrorKind::LegacyFormat);
        assert_eq!((err.base_offset, err.last_offset, err.record_count), (7, Some(7), 0));
        assert_eq!(got[1].as_ref().unwrap().records.len(), 1);
    }

    #[test]
    fn an_unknown_magic_is_corrupt_and_decoding_continues() {
        let mut bad = batch(0, 1, &record(0, 0, None, Some(b"v"), &[]));
        bad[16] = 9;
        let good = batch(0, 1, &record(0, 0, None, Some(b"v"), &[]));
        let got = decode(&Bytes::from([bad, good].concat()), MAX).entries;
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].as_ref().unwrap_err().kind, BatchErrorKind::Corrupt);
        assert!(got[1].is_ok());
    }

    /// A cut batch with its 12-byte head present reports its base offset, so a skip past a corrupt
    /// batch before it can land there.
    #[test]
    fn a_trailing_truncated_batch_yields_only_the_complete_one() {
        let good = batch(0, 1, &record(0, 0, None, Some(b"v"), &[]));
        for cut in [0, 5, 11, 12, 40, good.len() - 1] {
            let blob = Bytes::from([good.clone(), good[..cut].to_vec()].concat());
            let got = decode(&blob, MAX);
            assert_eq!(got.entries.len(), 1, "cut at {cut}");
            assert!(got.entries[0].is_ok());
            let partial = (cut >= 12).then_some(100);
            assert_eq!(got.partial_base, partial, "cut at {cut}");
        }
        assert_eq!(decode(&Bytes::from(good), MAX).partial_base, None);
    }

    #[test]
    fn a_garbage_batch_length_is_corrupt_without_a_last_offset_and_ends_decoding() {
        let good = batch(0, 1, &record(0, 0, None, Some(b"v"), &[]));
        for length in [-5i32, 0, 4, 48] {
            let mut bad = batch(0, 1, &record(0, 0, None, Some(b"v"), &[]));
            bad[8..12].copy_from_slice(&length.to_be_bytes());
            let blob = Bytes::from([bad, good.clone()].concat());
            let got = decode(&blob, MAX).entries;
            assert_eq!(got.len(), 1, "length {length}");
            let err = got[0].as_ref().unwrap_err();
            assert_eq!((err.kind, err.last_offset), (BatchErrorKind::Corrupt, None), "{length}");
        }
    }

    #[test]
    fn log_append_time_gives_every_record_the_max_timestamp() {
        let section =
            [record(0, 0, None, Some(b"a"), &[]), record(9, 1, None, Some(b"b"), &[])].concat();
        let b = one(batch(0x8, 2, &section)).unwrap();
        assert_eq!(b.timestamp_type(), TimestampType::LogAppendTime);
        for r in &b.records {
            assert_eq!((r.timestamp, r.timestamp_type), (1_050, TimestampType::LogAppendTime));
        }
        let b = one(batch(0x30, 2, &section)).unwrap();
        assert!(b.is_transactional() && b.is_control());
    }

    #[test]
    fn arbitrary_bytes_never_panic() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut random = vec![0u8; 4096];
        for b in &mut random {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            *b = seed as u8;
        }
        let good = batch(0, 1, &record(0, 0, None, Some(b"v"), &[]));
        let mut mangled = good.clone();
        for i in 0..mangled.len() {
            let saved = mangled[i];
            mangled[i] = 0xff;
            let _ = decode(&Bytes::from(mangled.clone()), MAX).entries;
            refresh_crc(&mut mangled);
            let _ = decode(&Bytes::from(mangled.clone()), MAX).entries;
            mangled[i] = saved;
        }
        for blob in [vec![], vec![0; 100], vec![0xff; 100], random, good[..30].to_vec()] {
            let _ = decode(&Bytes::from(blob), 64);
        }
    }
}
