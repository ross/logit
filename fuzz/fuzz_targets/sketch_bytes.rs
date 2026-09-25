//! `DdSketch::from_bytes`, the native wire format's `Distribution` payload. Oracle: a decoded
//! sketch's `to_bytes` is a fixed point of one more decode.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_core::DdSketch;

fuzz_target!(|data: &[u8]| {
    if let Ok(sketch) = DdSketch::from_bytes(data) {
        let bytes = sketch.to_bytes();
        let again = DdSketch::from_bytes(&bytes).expect("to_bytes output decodes");
        assert_eq!(again.to_bytes(), bytes);
    }
});
