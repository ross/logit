//! A partition batch's records, and their encoding as one record batch v2 (magic 2) through
//! `kafka-protocol`'s encoder with [`super::compression`] as its compression hook, and the decode
//! that reads it back.
//!
//! `kafka-protocol`'s encoder starts a new batch whenever a record's producer fields differ from
//! the first record's or `offset - sequence` changes, and writes the first record's `sequence`,
//! less its offset delta, as the batch's base sequence. [`encode`] numbers record `i` with offset
//! delta `i` and sequence `base + i` (wrapping), so every batch comes out whole, and a producer
//! with no sequence (`-1`) gets `-1` as its base: the broker reads only the base.

use bytes::{Bytes, BytesMut};
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
    NO_PARTITION_LEADER_EPOCH, NO_PRODUCER_EPOCH, NO_PRODUCER_ID, NO_SEQUENCE,
};

/// The record batch v2 header, which Java's size estimate adds to every record's
/// (`AbstractRecords.estimateSizeInBytesUpperBound`).
pub const BATCH_OVERHEAD: usize = 61;

/// The most a record's own framing adds to its key, value, and headers: a 5-byte length varint,
/// the attributes byte, a 10-byte timestamp delta, and a 5-byte offset delta
/// (`DefaultRecord.MAX_RECORD_OVERHEAD`).
const MAX_RECORD_OVERHEAD: usize = 21;

/// One record to produce.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerRecord {
    pub key: Option<Bytes>,
    pub value: Option<Bytes>,
    /// In order; `kafka-protocol`'s `Record` can't carry a repeated key.
    pub headers: IndexMap<StrBytes, Option<Bytes>>,
    /// `CreateTime`, in milliseconds since the Unix epoch.
    pub timestamp: i64,
}

impl ProducerRecord {
    /// An upper bound on the record's encoded size, uncompressed and without the batch header, as
    /// Java's `DefaultRecord.recordSizeUpperBound` computes it: each length is counted as a
    /// 5-byte varint.
    pub fn size_upper_bound(&self) -> usize {
        let field = |b: &Option<Bytes>| 5 + b.as_ref().map_or(0, Bytes::len);
        let headers: usize =
            self.headers.iter().map(|(k, v)| 5 + k.len() + field(v)).sum::<usize>() + 5;
        MAX_RECORD_OVERHEAD + field(&self.key) + field(&self.value) + headers
    }
}

/// The most bytes an unkeyed record's value may hold under `headers` and still pass the
/// producer's `cap` check (`BATCH_OVERHEAD` plus [`ProducerRecord::size_upper_bound`]): that
/// check solved for the value's length, which it counts byte for byte.
pub fn value_budget(cap: usize, headers: &IndexMap<StrBytes, Option<Bytes>>) -> usize {
    let empty = ProducerRecord {
        key: None,
        value: Some(Bytes::new()),
        headers: headers.clone(),
        timestamp: 0,
    };
    cap.saturating_sub(BATCH_OVERHEAD + empty.size_upper_bound())
}

/// The idempotent producer's fields a batch carries, or [`Stamp::NONE`] for a producer without
/// idempotence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

impl Stamp {
    pub const NONE: Stamp = Stamp {
        producer_id: NO_PRODUCER_ID,
        producer_epoch: NO_PRODUCER_EPOCH,
        base_sequence: NO_SEQUENCE,
    };
}

/// Encodes `records` as one record batch v2 under `stamp`, compressed with `codec`. Errors on an
/// empty slice, which has no batch to write.
pub fn encode(
    records: &[ProducerRecord],
    stamp: Stamp,
    codec: Compression,
) -> anyhow::Result<Bytes> {
    anyhow::ensure!(!records.is_empty(), "a record batch needs at least one record");
    let records: Vec<Record> = records
        .iter()
        .enumerate()
        .map(|(i, r)| Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: NO_PARTITION_LEADER_EPOCH,
            producer_id: stamp.producer_id,
            producer_epoch: stamp.producer_epoch,
            timestamp_type: TimestampType::Creation,
            offset: i as i64,
            sequence: stamp.base_sequence.wrapping_add(i as i32),
            timestamp: r.timestamp,
            key: r.key.clone(),
            value: r.value.clone(),
            headers: r.headers.clone(),
        })
        .collect();
    let mut buf = BytesMut::new();
    let options = RecordEncodeOptions { version: 2, compression: codec };
    RecordBatchEncoder::encode_with_custom_compression(
        &mut buf,
        &records,
        &options,
        Some(|records: &mut BytesMut, out: &mut BytesMut, codec| {
            super::compression::compress(codec, records, out)
        }),
    )?;
    Ok(buf.freeze())
}

/// Decodes every record batch in `bytes` through `kafka-protocol`'s decoder, with
/// [`super::compression::decompress`] as its decompression hook, bounding each batch's
/// decompressed records section at `max_records_bytes`.
pub fn decode(
    mut bytes: Bytes,
    max_records_bytes: usize,
) -> anyhow::Result<Vec<kafka_protocol::records::RecordSet>> {
    use bytes::Buf;
    use kafka_protocol::records::RecordBatchDecoder;

    let mut sets = Vec::new();
    while bytes.has_remaining() {
        sets.push(RecordBatchDecoder::decode_with_custom_compression(
            &mut bytes,
            Some(|payload: &mut Bytes, codec| {
                super::compression::decompress(codec, payload, max_records_bytes).map(Bytes::from)
            }),
        )?);
    }
    Ok(sets)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use kafka_protocol::records::RecordBatchDecoder;

    const CODECS: [(&str, Compression); 5] = [
        ("none", Compression::None),
        ("gzip", Compression::Gzip),
        ("snappy", Compression::Snappy),
        ("lz4", Compression::Lz4),
        ("zstd", Compression::Zstd),
    ];

    /// Far above any batch these tests decode.
    const MAX: usize = 1 << 20;

    fn corpus(name: &str) -> Bytes {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/kafka/batches")
            .join(name);
        Bytes::from(std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display())))
    }

    /// What a consumer reads: key, value, and headers. Timestamps, producer ids, and offsets
    /// change on every recording.
    /// A record's key, value, and headers.
    type Content = (Option<Bytes>, Option<Bytes>, Vec<(String, Bytes)>);

    fn content(records: &[Record]) -> Vec<Content> {
        records
            .iter()
            .map(|r| {
                let headers = r
                    .headers
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.clone().unwrap_or_default()))
                    .collect();
                (r.key.clone(), r.value.clone(), headers)
            })
            .collect()
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

    /// Every recorded batch, from the Java client (xerial snappy, idempotent) and from kcat (raw
    /// snappy, no idempotence), decodes to the records its producer's `none` batch holds.
    #[test]
    fn every_recorded_batch_decodes_to_its_producers_uncompressed_records() {
        for producer in ["java", "kcat"] {
            let plain = decode(corpus(&format!("{producer}-none.segment")), MAX).unwrap();
            assert_eq!(plain.len(), 1);
            let want = content(&plain[0].records);
            assert_eq!(want.len(), 3);
            assert_eq!(want[0].0.as_deref(), Some(&b"key-1"[..]));
            for (name, codec) in CODECS {
                let file = format!("{producer}-{name}.segment");
                let sets = decode(corpus(&file), MAX).unwrap_or_else(|e| panic!("{file}: {e:#}"));
                assert_eq!(sets.len(), 1, "{file}");
                assert_eq!(sets[0].compression, codec, "{file}");
                assert_eq!(content(&sets[0].records), want, "{file}");
                let first = &sets[0].records[0];
                if producer == "java" {
                    assert!(first.producer_id >= 0, "{file}: Java is idempotent by default");
                    assert_eq!(first.sequence, 0, "{file}");
                } else {
                    assert_eq!(first.producer_id, NO_PRODUCER_ID, "{file}");
                    assert_eq!(first.sequence, NO_SEQUENCE, "{file}");
                }
            }
        }
    }

    /// The corpus carries both snappy forms, so the reader is exercised on each.
    #[test]
    fn the_corpus_has_xerial_snappy_from_java_and_raw_snappy_from_kcat() {
        let records_at = |bytes: &Bytes| bytes.slice(BATCH_OVERHEAD..);
        assert!(records_at(&corpus("java-snappy.segment"))
            .starts_with(&crate::kafka::compression::XERIAL_MAGIC));
        assert!(!records_at(&corpus("kcat-snappy.segment"))
            .starts_with(&crate::kafka::compression::XERIAL_MAGIC));
    }

    #[test]
    fn an_encoded_batch_decodes_through_the_crates_decoder_for_every_codec() {
        let records = sample(5);
        let stamp = Stamp { producer_id: 7, producer_epoch: 3, base_sequence: 40 };
        for (name, codec) in CODECS {
            let sets = decode(encode(&records, stamp, codec).unwrap(), MAX).unwrap();
            assert_eq!(sets.len(), 1, "{name}: one batch");
            assert_eq!(sets[0].compression, codec, "{name}");
            let got = &sets[0].records;
            assert_eq!(got.len(), records.len(), "{name}");
            for (i, (got, want)) in got.iter().zip(&records).enumerate() {
                assert_eq!(got.key, want.key, "{name} record {i}");
                assert_eq!(got.value, want.value, "{name} record {i}");
                assert_eq!(got.headers, want.headers, "{name} record {i}");
                assert_eq!(got.timestamp, want.timestamp, "{name} record {i}");
                assert_eq!(got.timestamp_type, TimestampType::Creation, "{name}");
                assert_eq!(got.producer_id, 7, "{name}");
                assert_eq!(got.producer_epoch, 3, "{name}");
                assert_eq!(got.sequence, 40 + i as i32, "{name} record {i}");
                assert_eq!(got.offset, i as i64, "{name} record {i}");
            }
        }
    }

    /// The uncompressed form needs no hook at all: the crate's built-in decoder reads it.
    #[test]
    fn an_uncompressed_batch_decodes_with_the_crates_default_decoder() {
        let records = sample(3);
        let mut bytes = encode(&records, Stamp::NONE, Compression::None).unwrap();
        let set = RecordBatchDecoder::decode(&mut bytes).unwrap();
        assert_eq!(set.records.len(), 3);
        assert_eq!(set.records[0].sequence, NO_SEQUENCE);
        assert_eq!(set.records[0].producer_id, NO_PRODUCER_ID);
    }

    /// A producer with no sequence still gets one whole batch: the encoder splits whenever
    /// `offset - sequence` changes, and a constant `-1` sequence would change it on every record.
    #[test]
    fn a_batch_without_a_sequence_is_one_batch_with_base_sequence_minus_one() {
        let bytes = encode(&sample(4), Stamp::NONE, Compression::Gzip).unwrap();
        let mut b = bytes.clone();
        let info = RecordBatchDecoder::decode_batch_info(&mut b).unwrap();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].record_count, 4);
        assert_eq!(info[0].base_sequence, NO_SEQUENCE);
    }

    /// A base sequence near `i32::MAX` keeps the batch whole; the broker computes the last
    /// sequence itself, wrapping past `i32::MAX` to 0.
    #[test]
    fn a_base_sequence_near_the_wrap_stays_one_batch() {
        let stamp = Stamp { producer_id: 1, producer_epoch: 0, base_sequence: i32::MAX - 1 };
        let mut b = encode(&sample(4), stamp, Compression::None).unwrap();
        let info = RecordBatchDecoder::decode_batch_info(&mut b).unwrap();
        assert_eq!(info.len(), 1);
        assert_eq!(info[0].base_sequence, i32::MAX - 1);
    }

    #[test]
    fn an_empty_batch_is_an_error() {
        assert!(encode(&[], Stamp::NONE, Compression::None).is_err());
    }

    /// The bound covers the record's real encoded size, so a record under the cap is never larger
    /// on the wire than the cap check assumed.
    #[test]
    fn the_size_upper_bound_covers_the_encoded_record() {
        for n in [1, 2, 7] {
            let records = sample(n);
            let bound: usize = BATCH_OVERHEAD
                + records.iter().map(ProducerRecord::size_upper_bound).sum::<usize>();
            let encoded = encode(&records, Stamp::NONE, Compression::None).unwrap();
            assert!(encoded.len() <= bound, "{} > {bound}", encoded.len());
        }
    }

    /// A value of the budget's length passes the producer's cap check and one byte more doesn't.
    #[test]
    fn a_value_at_the_budget_fits_and_one_byte_more_does_not() {
        let mut headers = IndexMap::new();
        headers.insert(StrBytes::from_static_str("source"), Some(Bytes::from_static(b"logit")));
        let cap = 1_000;
        let budget = value_budget(cap, &headers);
        let record = |len: usize| ProducerRecord {
            key: None,
            value: Some(Bytes::from(vec![b'x'; len])),
            headers: headers.clone(),
            timestamp: 0,
        };
        assert!(BATCH_OVERHEAD + record(budget).size_upper_bound() <= cap);
        assert!(BATCH_OVERHEAD + record(budget + 1).size_upper_bound() > cap);
        assert_eq!(value_budget(10, &headers), 0, "a cap under the overhead leaves no room");
    }
}
