//! Two `DdSketch`es decoded from one input and merged. The first two bytes (big-endian `u16`)
//! say where the first sketch ends; a mapping mismatch between the two exercises `merge`'s
//! re-binning.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_core::DdSketch;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 {
        return;
    }
    let split = u16::from_be_bytes([data[0], data[1]]) as usize;
    let rest = &data[2..];
    let (first, second) = rest.split_at(split.min(rest.len()));
    let (Ok(mut a), Ok(b)) = (DdSketch::from_bytes(first), DdSketch::from_bytes(second)) else {
        return;
    };
    a.merge(&b);
    let _ = a.quantile(0.5);
    let _ = a.to_bytes();
});
