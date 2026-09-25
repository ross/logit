//! A remote-write body through `compression::decompress_bounded`. The first byte's low bit
//! picks the encoding (`0` Snappy, `1` zstd); the rest is the body. Oracle: output never
//! exceeds the cap.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_proto::prometheus::compression::{decompress_bounded, Encoding};

/// Well under the target's `-malloc_limit_mb`, so an allocation sized past the cap fails the run.
const MAX: usize = 1 << 20;

fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else {
        return;
    };
    let encoding = if selector & 1 == 0 { Encoding::Snappy } else { Encoding::Zstd };
    if let Ok(out) = decompress_bounded(encoding, body, MAX) {
        assert!(out.len() <= MAX, "{} bytes out, cap {MAX}", out.len());
    }
});
