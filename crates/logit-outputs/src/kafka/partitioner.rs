//! Which partition a record goes to (`docs/adr/kafka-output.md`, items 2 and 3).
//!
//! - A keyed record goes to `toPositive(murmur2(key)) % n`, with `n` counting every partition of
//!   the topic, led or not, as the Java client's `BuiltInPartitioner.partitionForKey` does. The
//!   hash is the Java client's `Utils.murmur2`, pinned by
//!   `testdata/interop/kafka/murmur2.json`: a test that disagrees with that file is a bug here.
//! - Unkeyed records stick to one partition per `send`, picked at random among the partitions that
//!   have a leader.

/// The Java client's `Utils.murmur2`: MurmurHash2, 32-bit, seed `0x9747b28c`, reading the key in
/// little-endian 4-byte words. Returned as Java's signed `int`.
pub(crate) fn murmur2(data: &[u8]) -> i32 {
    const SEED: u32 = 0x9747_b28c;
    const M: u32 = 0x5bd1_e995;
    const R: u32 = 24;

    let mut h = SEED ^ data.len() as u32;
    let (words, tail) = data.as_chunks::<4>();
    for word in words {
        let mut k = u32::from_le_bytes(*word);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }
    if tail.len() == 3 {
        h ^= u32::from(tail[2]) << 16;
    }
    if tail.len() >= 2 {
        h ^= u32::from(tail[1]) << 8;
    }
    if !tail.is_empty() {
        h ^= u32::from(tail[0]);
        h = h.wrapping_mul(M);
    }
    h ^= h >> 13;
    h = h.wrapping_mul(M);
    h ^= h >> 15;
    h as i32
}

/// The partition of `key` among `partitions`, which must be positive: the Java client's
/// `Utils.toPositive(murmur2(key)) % numPartitions`. `toPositive` masks the sign bit rather than
/// taking an absolute value, so `i32::MIN` maps to 0.
pub(crate) fn partition_for_key(key: &[u8], partitions: usize) -> i32 {
    debug_assert!(partitions > 0, "a topic has at least one partition");
    ((murmur2(key) & 0x7fff_ffff) as usize % partitions) as i32
}

/// One of `led`, the partitions with a leader, at random; `None` when there's none.
pub(crate) fn sticky_partition(led: &[i32]) -> Option<i32> {
    if led.is_empty() {
        return None;
    }
    let draw = u64::from_le_bytes(logit_core::trace::random_id_bytes::<8>());
    Some(led[(draw % led.len() as u64) as usize])
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::BTreeSet;

    fn hex(s: &str) -> Vec<u8> {
        (0..s.len()).step_by(2).map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap()).collect()
    }

    fn vectors() -> Vec<serde_json::Value> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../testdata/interop/kafka/murmur2.json");
        let doc: serde_json::Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        doc["vectors"].as_array().unwrap().clone()
    }

    fn key_of(vector: &serde_json::Value) -> Vec<u8> {
        if let Some(h) = vector["key_hex"].as_str() {
            return hex(h);
        }
        let mut key = Vec::new();
        for run in vector["key_runs"].as_array().unwrap() {
            let byte = hex(run[0].as_str().unwrap())[0];
            key.extend(std::iter::repeat_n(byte, run[1].as_u64().unwrap() as usize));
        }
        key
    }

    /// Every vector the Java client computed: the hash and the partition for each count.
    #[test]
    fn murmur2_matches_every_java_client_vector() {
        let vectors = vectors();
        assert_eq!(vectors.len(), 31);
        for vector in &vectors {
            let key = key_of(vector);
            assert_eq!(key.len() as u64, vector["key_len"].as_u64().unwrap());
            let label = vector["key_utf8"].as_str().unwrap_or("<bytes>");
            assert_eq!(
                i64::from(murmur2(&key)),
                vector["murmur2"].as_i64().unwrap(),
                "murmur2({label:?}), {} bytes",
                key.len()
            );
            for (n, want) in vector["partitions"].as_object().unwrap() {
                let n: usize = n.parse().unwrap();
                assert_eq!(
                    i64::from(partition_for_key(&key, n)),
                    want.as_i64().unwrap(),
                    "partition of {label:?} among {n}"
                );
            }
        }
    }

    #[test]
    fn sticky_picks_only_a_led_partition() {
        assert_eq!(sticky_partition(&[]), None);
        assert_eq!(sticky_partition(&[4]), Some(4));
        let led = [1, 3, 5];
        let picked: BTreeSet<i32> = (0..200).filter_map(|_| sticky_partition(&led)).collect();
        assert!(picked.is_subset(&led.into_iter().collect()));
        // 200 draws over three choices miss one with probability about 3 * (2/3)^200.
        assert_eq!(picked.len(), 3, "the pick is random, not fixed");
    }
}
