//! An object's key, and the head's pin on it (`docs/adr/s3-object-archive.md`, decision 3).
//!
//! A key is `{prefix}{partition}{object_time}-{store id}-{first seq}.{ext}`: `partition` is a
//! strftime pattern formatted in UTC at `object_time`, `object_time` is compact ISO 8601 to the
//! millisecond (`20261030T142501.123Z`), the store id is the first batch's sender id in lowercase
//! hex, and the first seq is its number zero-padded to 20 digits, so keys sort by sequence within
//! a millisecond.

use super::{ObjectCompression, ObjectFormat};
use logit_proto::native::SeqId;

/// What the sink remembers against the head's `SeqId` across every retry of it in one process:
/// the object's `object_time`, and from the object's first close, its batch count. A retry
/// writes the same key and closes at the count at the latest, and a retry that closes earlier
/// lowers it, so the count only falls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pin {
    pub(crate) head: SeqId,
    /// Milliseconds since the Unix epoch.
    pub(crate) object_time_ms: i64,
    pub(crate) count: Option<usize>,
}

impl Pin {
    pub(crate) fn new(head: SeqId, object_time_ms: i64) -> Self {
        Self { head, object_time_ms, count: None }
    }

    /// Records a close at `staged` batches.
    pub(crate) fn close_at(&mut self, staged: usize) {
        self.count = Some(self.count.map_or(staged, |count| count.min(staged)));
    }

    /// Whether an object holding `staged` batches has reached the pinned count.
    pub(crate) fn reached(&self, staged: usize) -> bool {
        self.count.is_some_and(|count| staged >= count)
    }
}

/// The key's extension, lowercase.
pub(crate) fn extension(format: ObjectFormat, compression: ObjectCompression) -> &'static str {
    match (format, compression) {
        (ObjectFormat::Json, ObjectCompression::Gzip) => "ndjson.gz",
        (ObjectFormat::Json, ObjectCompression::Zstd) => "ndjson.zst",
        (ObjectFormat::Json, ObjectCompression::None) => "ndjson",
        (ObjectFormat::Native, ObjectCompression::Gzip) => "logit.gz",
        (ObjectFormat::Native, ObjectCompression::Zstd) => "logit.zst",
        (ObjectFormat::Native, ObjectCompression::None) => "logit",
    }
}

/// The key of the object whose first batch is `head`, staged at `object_time_ms`.
pub(crate) fn object_key(
    prefix: &str,
    partition: &str,
    object_time_ms: i64,
    head: SeqId,
    ext: &str,
) -> Result<String, jiff::Error> {
    use std::fmt::Write as _;
    let time = jiff::Timestamp::from_millisecond(object_time_ms)?.to_zoned(jiff::tz::TimeZone::UTC);
    let mut key = String::with_capacity(prefix.len() + partition.len() + 96);
    key.push_str(prefix);
    jiff::fmt::strtime::BrokenDownTime::from(&time).format(partition, &mut key)?;
    // `%Y` pads a year below 1000 and signs one before year 0, so the width holds for any clock.
    write!(
        key,
        "{:04}{:02}{:02}T{:02}{:02}{:02}.{:03}Z-",
        time.year(),
        time.month(),
        time.day(),
        time.hour(),
        time.minute(),
        time.second(),
        time.millisecond()
    )
    .expect("writing to a String");
    for byte in head.id {
        write!(key, "{byte:02x}").expect("writing to a String");
    }
    write!(key, "-{:020}.{ext}", head.seq).expect("writing to a String");
    Ok(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEAD: SeqId = SeqId {
        id: [
            0x6f, 0x1c, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
            0xe0, 0x9a,
        ],
        seq: 42,
    };

    /// 2026-10-30T14:25:01.123Z.
    const OBJECT_TIME_MS: i64 = 1_793_370_301_123;

    #[test]
    fn a_key_formats_as_the_template() {
        let key =
            object_key("logs/", "dt=%Y-%m-%d/hour=%H/", OBJECT_TIME_MS, HEAD, "ndjson.gz").unwrap();
        assert_eq!(
            key,
            "logs/dt=2026-10-30/hour=14/20261030T142501.123Z-6f1c000102030405060708090a0be09a-\
             00000000000000000042.ndjson.gz"
        );
        let key = object_key("", "", 1_000, SeqId { id: [0; 16], seq: u64::MAX }, "logit").unwrap();
        assert_eq!(
            key,
            "19700101T000001.000Z-00000000000000000000000000000000-18446744073709551615.logit"
        );
    }

    #[test]
    fn every_extension_is_lowercase_and_named_by_format_then_compression() {
        use ObjectCompression as C;
        use ObjectFormat as F;
        let all: Vec<_> = [F::Json, F::Native]
            .into_iter()
            .flat_map(|f| [C::Gzip, C::Zstd, C::None].map(|c| extension(f, c)))
            .collect();
        assert_eq!(all, ["ndjson.gz", "ndjson.zst", "ndjson", "logit.gz", "logit.zst", "logit"]);
    }

    /// `logit_config::S3_KEY_TAIL_BYTES`, which validation adds to the longest prefix and
    /// partition, is the longest tail a key can have.
    #[test]
    fn the_longest_tail_is_the_one_validation_reserves() {
        let longest = [ObjectFormat::Json, ObjectFormat::Native]
            .into_iter()
            .flat_map(|f| [ObjectCompression::Gzip, ObjectCompression::Zstd].map(|c| (f, c)))
            .map(|(f, c)| {
                let max = SeqId { id: [0xff; 16], seq: u64::MAX };
                object_key("", "", OBJECT_TIME_MS, max, extension(f, c)).unwrap().len()
            })
            .max()
            .unwrap();
        assert_eq!(longest, logit_config::S3_KEY_TAIL_BYTES);
    }

    #[test]
    fn the_pin_only_falls() {
        let mut pin = Pin::new(HEAD, OBJECT_TIME_MS);
        assert!(!pin.reached(usize::MAX), "no count before the first close");
        pin.close_at(5);
        assert!(pin.reached(5) && !pin.reached(4));
        pin.close_at(7);
        assert_eq!(pin.count, Some(5), "a later close at more batches doesn't raise it");
        pin.close_at(3);
        assert_eq!(pin.count, Some(3));
    }
}
