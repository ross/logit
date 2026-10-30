//! A Kafka Fetch response's records blob (`logit_proto::kafka::decode`), every record batch in
//! it, with each records section decompressing to at most [`MAX_RECORDS_BYTES`], then the walk
//! `kafka_in` makes over each decoded record: its key, value, and every header's name as text.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

/// Far under `kafka_in`'s 64 MiB `max_batch_bytes`, so a section that expands past it is an
/// `Oversize` error, not a buffer. The target's `max_len` is twice this, so an uncompressed
/// section reaches the bound too.
const MAX_RECORDS_BYTES: usize = 64 << 10;

fuzz_target!(|data: &[u8]| {
    let blob = Bytes::copy_from_slice(data);
    let decoded = logit_proto::kafka::decode(&blob, MAX_RECORDS_BYTES);
    let _ = decoded.partial_base;
    for entry in &decoded.entries {
        let batch = match entry {
            Ok(batch) => batch,
            Err(err) => {
                let _ = (err.kind, err.last_offset, err.record_count, err.to_string());
                continue;
            }
        };
        let _ = (
            batch.compression(),
            batch.timestamp_type(),
            batch.is_transactional(),
            batch.is_control(),
            batch.last_offset(),
        );
        for record in &batch.records {
            let _ = (record.offset, record.timestamp, record.timestamp_type);
            let _ = record.key.as_ref().map(|k| std::str::from_utf8(k).is_ok());
            let _ = record.value.as_ref().map(Bytes::len);
            for header in &record.headers {
                let _ = (header.name_str(), header.value.as_ref().map(Bytes::len));
            }
        }
    }
});
