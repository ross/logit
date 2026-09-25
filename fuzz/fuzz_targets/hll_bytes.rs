//! `HyperLogLog::from_bytes`, then every operation a decoded estimator meets downstream. The
//! drops at the end matter: `cardinality-estimator`'s `Layout` bug surfaces on free, which Miri
//! sees and ASan does not (docs/adr/out-of-ci-fuzzing.md).
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_core::HyperLogLog;

fuzz_target!(|data: &[u8]| {
    let Ok(mut hll) = HyperLogLog::from_bytes(data) else {
        return;
    };
    let _ = hll.estimate();
    let decoded = hll.clone();
    hll.insert(b"x");
    hll.merge(&decoded);
    let mut fresh = HyperLogLog::new();
    fresh.merge(&hll);
    let _ = hll.to_bytes();
    drop(fresh);
    drop(hll);
});
