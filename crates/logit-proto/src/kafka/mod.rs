//! Kafka's record batch v2 and its compression codecs: the wire codec `kafka_out` writes with and
//! its fake broker reads back with, synchronous and I/O-free, so the fuzz crate reaches it
//! (`docs/adr/kafka-input.md`, item 1). The broker connection and the request codecs live in
//! `logit-kafka`, because they do I/O.
//!
//! - [`batch`] encodes a partition batch's records as one record batch v2, and decodes every
//!   batch in a buffer.
//! - [`compression`] runs the five codecs through `kafka-protocol`'s custom-compression hook,
//!   with every decompressor bounded.

pub mod batch;
pub mod compression;

/// `seq + count` on the broker's sequence ring, which wraps from `i32::MAX` to 0
/// (`DefaultRecordBatch.incrementSequence`).
pub fn next_sequence(seq: i32, count: i32) -> i32 {
    if i32::MAX - seq < count {
        count - (i32::MAX - seq) - 1
    } else {
        seq + count
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequences_wrap_from_i32_max_to_zero() {
        assert_eq!(next_sequence(0, 3), 3);
        assert_eq!(next_sequence(i32::MAX - 3, 3), i32::MAX);
        assert_eq!(next_sequence(i32::MAX - 2, 3), 0);
        assert_eq!(next_sequence(i32::MAX, 1), 0);
        assert_eq!(next_sequence(i32::MAX - 1, 5), 3);
    }
}
