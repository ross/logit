//! Consistent sampling: the keep/drop compare and the hash every sampler keys it on.
//!
//! Two callers. [`crate::telemetry::trace_is_sampled`] feeds [`keep`] raw trace-id bits: `logit`'s
//! own pipeline trace ids are random and need no hash. The `sample` transform
//! (`crates/logit-transforms/src/sample.rs`) feeds it a hash of an application key through
//! [`hash_value`]/[`hash_trace_id`], so a key that isn't uniformly random (`request_id: "{seq}"`,
//! a `service.name`) still samples at the configured rate. The two can reach different verdicts
//! for the same 16 bytes; they sample different populations.
//!
//! **The hash is a frozen, cross-process contract** (`docs/adr/consistent-sampling-component.md`):
//! XXH64, seed 0, over the canonical bytes [`hash_value`] documents. Every `logit` process in a
//! split-collection topology (`docs/OVERVIEW.md`) must reach the same verdict for the same key
//! with nothing propagated between them, and that includes two different `logit` versions. So the
//! algorithm, the seed, and the canonicalization table are pinned by the test vectors at the bottom
//! of this file, and changing any of them is a wire-breaking change that needs its own ADR -- not a
//! refactor.
//!
//! One implementation rule that follows from that: bytes reach the hasher only through
//! `Hasher::write(&[u8])` (directly, or via [`KeyHasher`]'s `fmt::Write`). `write_u64` and friends
//! hash a value's *native-endian* bytes, which would make the verdict depend on the host's
//! endianness.

use crate::Value;
use std::fmt::Write as _;
use std::hash::Hasher as _;
use twox_hash::XxHash64;

/// The frozen seed. Not configurable: a per-deployment seed would make two processes configured
/// differently disagree, which is what this module exists to prevent.
const SEED: u64 = 0;

/// Whether a uniformly distributed 64-bit `x` falls inside `rate`. Compares the top 53 bits, not
/// all 64: `rate * 2f64.powi(64)` loses precision near 1.0, which would reject values it should
/// keep. 53 bits is `f64`'s exact-integer range, so the comparison is exact. A NaN or `>= 1` rate
/// keeps everything; a `<= 0` rate keeps nothing.
///
/// The internal-span sampler and the `sample` transform share this one compare.
pub fn keep(x: u64, rate: f64) -> bool {
    // `!(rate < 1.0)`, not `rate >= 1.0`, so NaN keeps everything rather than dropping
    // everything. Graph rules 16 and 61 reject NaN in config; this holds for any other caller.
    #[allow(clippy::neg_cmp_op_on_partial_ord)]
    if !(rate < 1.0) {
        return true;
    }
    if rate <= 0.0 {
        return false;
    }
    (x >> 11) < (rate * (1u64 << 53) as f64) as u64
}

/// XXH64, seed 0, of `bytes` -- the contract's hash, one-shot.
pub fn hash_bytes(bytes: &[u8]) -> u64 {
    XxHash64::oneshot(SEED, bytes)
}

/// A streaming XXH64 that accepts `fmt` output, so a number's decimal text is hashed as it is
/// formatted -- `write!(hasher, "{n}")` -- with no intermediate `String`. XXH64's streaming form
/// is chunking-independent (the same bytes in any number of `write`s hash the same as one
/// `oneshot`), which is what makes this equal to [`hash_bytes`] over the formatted text.
struct KeyHasher(XxHash64);

impl KeyHasher {
    fn new() -> Self {
        KeyHasher(XxHash64::with_seed(SEED))
    }

    fn finish(&self) -> u64 {
        self.0.finish()
    }
}

impl std::fmt::Write for KeyHasher {
    fn write_str(&mut self, s: &str) -> std::fmt::Result {
        // `write(&[u8])` only -- see the module doc.
        self.0.write(s.as_bytes());
        Ok(())
    }
}

/// Hashes `args`' formatted text without allocating.
fn hash_display(args: std::fmt::Arguments<'_>) -> u64 {
    let mut hasher = KeyHasher::new();
    // `KeyHasher::write_str` never fails, so neither can this.
    let _ = hasher.write_fmt(args);
    hasher.finish()
}

/// The contract's hash of a key's value, or `None` when the value can't be a key. Canonical bytes,
/// frozen:
///
/// | `Value` | bytes hashed |
/// |---|---|
/// | `Str`, `Bytes` | as-is |
/// | `I64`, `U64` | decimal text (`-7`, `200`) |
/// | `F64` | Rust `Display` (`200`, `0.5`, `-0`) |
/// | `Bool` | `true` / `false` |
/// | `Timestamp` | decimal nanoseconds |
/// | `Null`, `Array`, `Map` | none -- treated as missing |
///
/// So `I64(200)`, `U64(200)`, `F64(200.0)` and `Str("200")` hash identically: a decoder's choice of
/// numeric type never changes a verdict (ADR `kv-metrics-semantics`' identity commitment). Case is
/// not folded. Allocation-free for every variant.
pub fn hash_value(value: &Value) -> Option<u64> {
    Some(match value {
        Value::Str(b) | Value::Bytes(b) => hash_bytes(b),
        Value::I64(n) => hash_display(format_args!("{n}")),
        Value::U64(n) => hash_display(format_args!("{n}")),
        Value::F64(n) => hash_display(format_args!("{n}")),
        Value::Bool(true) => hash_bytes(b"true"),
        Value::Bool(false) => hash_bytes(b"false"),
        Value::Timestamp(ns) => hash_display(format_args!("{ns}")),
        Value::Null | Value::Array(_) | Value::Map(_) => return None,
    })
}

/// The contract's hash of a 16-byte trace id: XXH64 over its 32 lowercase hex characters, so a
/// lifted id (`SpanRecord::trace_id`, `TraceRef::trace_id`) and the same id still sitting in an
/// attribute as a W3C-spelled hex string reach the same verdict. Built in a stack array, no
/// allocation.
pub fn hash_trace_id(trace_id: &[u8; 16]) -> u64 {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = [0u8; 32];
    for (i, byte) in trace_id.iter().enumerate() {
        text[2 * i] = HEX[(byte >> 4) as usize];
        text[2 * i + 1] = HEX[(byte & 0x0f) as usize];
    }
    hash_bytes(&text)
}

/// A per-event draw for a sampler with no key: the contract's hash over `counter ^ seed`'s
/// little-endian bytes. With a random `seed` per sampler and a per-event `counter`, a uniform draw
/// with no RNG dependency; with a fixed seed, reproducible. Not part of the cross-process contract,
/// but the byte order is fixed so a test's seeded sequence is the same on every host.
pub fn mix(counter: u64, seed: u64) -> u64 {
    hash_bytes(&(counter ^ seed).to_le_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AttrMap;
    use bytes::Bytes;

    /// Published XXH64 seed-0 vectors, cross-checked against an implementation written from
    /// xxHash's `doc/xxhash_spec.md`. A failure means the hash changed, and every deployment's
    /// verdicts with it.
    #[test]
    fn xxh64_matches_the_published_seed_zero_vectors() {
        assert_eq!(hash_bytes(b""), 0xEF46_DB37_51D8_E999);
        assert_eq!(hash_bytes(b"a"), 0xD24E_C4F1_A98C_6E5B);
        assert_eq!(hash_bytes(b"abc"), 0x44BC_2CF5_AD77_0999);
    }

    /// Frozen vectors for the canonicalization table. A failure is a wire-breaking change between
    /// `logit` versions (`docs/adr/consistent-sampling-component.md`), not a test to update.
    #[test]
    fn hash_value_is_pinned_for_every_keyable_variant() {
        assert_eq!(hash_value(&Value::str("abc")), Some(0x44BC_2CF5_AD77_0999));
        assert_eq!(
            hash_value(&Value::Bytes(Bytes::from_static(b"abc"))),
            Some(0x44BC_2CF5_AD77_0999)
        );
        assert_eq!(hash_value(&Value::I64(200)), Some(0x2543_6E54_5A8B_CC7C));
        assert_eq!(hash_value(&Value::I64(-7)), Some(0x0C4E_C0C6_AD41_198F));
        assert_eq!(hash_value(&Value::U64(u64::MAX)), Some(0x8D1F_8F6D_8DFA_7C47));
        assert_eq!(hash_value(&Value::F64(0.5)), Some(0x1DC8_9AF3_1B8D_1B23));
        assert_eq!(hash_value(&Value::F64(-0.0)), Some(0x8E56_E730_C492_7269));
        assert_eq!(hash_value(&Value::Bool(true)), Some(0xD7C9_B979_4814_2E4A));
        assert_eq!(hash_value(&Value::Bool(false)), Some(0x6D3F_99CC_C0C0_3A7A));
        assert_eq!(
            hash_value(&Value::Timestamp(1_700_000_000_000_000_000)),
            Some(0x4F04_2F17_9CD3_3215)
        );
    }

    #[test]
    fn hash_trace_id_is_pinned() {
        let id = [
            0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e,
            0x47, 0x36,
        ];
        assert_eq!(hash_trace_id(&id), 0x2AB2_8D25_664B_8CFD);
    }

    #[test]
    fn a_numeric_key_hashes_the_same_whatever_its_decoded_type() {
        let expected = hash_value(&Value::str("200"));
        assert_eq!(hash_value(&Value::I64(200)), expected);
        assert_eq!(hash_value(&Value::U64(200)), expected);
        assert_eq!(hash_value(&Value::F64(200.0)), expected);
        assert_ne!(hash_value(&Value::F64(200.5)), expected);
    }

    #[test]
    fn a_lifted_trace_id_agrees_with_its_lowercase_hex_string() {
        let id = [
            0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e,
            0x47, 0x36,
        ];
        assert_eq!(
            Some(hash_trace_id(&id)),
            hash_value(&Value::str("4bf92f3577b34da6a3ce929d0e0e4736"))
        );
        // Case is not folded: an off-spec uppercase spelling is a different key.
        assert_ne!(
            Some(hash_trace_id(&id)),
            hash_value(&Value::str("4BF92F3577B34DA6A3CE929D0E0E4736"))
        );
    }

    #[test]
    fn null_array_and_map_are_not_keys() {
        assert_eq!(hash_value(&Value::Null), None);
        assert_eq!(hash_value(&Value::Array(vec![Value::I64(1)])), None);
        assert_eq!(hash_value(&Value::Map(Box::new(AttrMap::new()))), None);
    }

    #[test]
    fn streaming_the_formatted_text_equals_hashing_it_whole() {
        assert_eq!(hash_display(format_args!("{}-{}", 12, "ab")), hash_bytes(b"12-ab"));
    }

    #[test]
    fn keep_edges() {
        assert!(keep(0, 1.0));
        assert!(keep(u64::MAX, 1.0));
        assert!(keep(u64::MAX, f64::NAN));
        assert!(!keep(0, 0.0));
        assert!(!keep(0, -0.5));
        // The rate-0.5 threshold is exactly 2^63.
        assert!(keep(0x7FFF_FFFF_FFFF_FFFF, 0.5));
        assert!(!keep(0x8000_0000_0000_0000, 0.5));
        // The smallest nonzero threshold: `rate * 2^53` truncating to 1 keeps only the values
        // whose top 53 bits are zero.
        assert!(keep(0x7FF, 2f64.powi(-53)));
        assert!(!keep(0x800, 2f64.powi(-53)));
    }

    #[test]
    fn hashed_keys_keep_roughly_the_configured_fraction() {
        let rate = 0.25;
        let kept =
            (0..10_000u64).filter(|i| keep(hash_value(&Value::U64(*i)).unwrap(), rate)).count();
        let fraction = kept as f64 / 10_000.0;
        assert!((fraction - rate).abs() <= 0.03, "kept {fraction}");
    }

    #[test]
    fn mix_is_reproducible_and_spreads_consecutive_counters() {
        assert_eq!(mix(1, 42), mix(1, 42));
        assert_ne!(mix(1, 42), mix(2, 42));
        assert_ne!(mix(1, 42), mix(1, 43));
        let kept = (0..10_000u64).filter(|i| keep(mix(*i, 0x5eed), 0.5)).count();
        assert!((kept as f64 / 10_000.0 - 0.5).abs() <= 0.03, "kept {kept}");
    }
}
