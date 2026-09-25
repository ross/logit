//! A decompressed remote-write body through `remote_write::decode`. The first byte's low bit
//! picks the message (`0` 1.0 `WriteRequest`, `1` 2.0 `Request`); the rest is the body.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_proto::prometheus::remote_write::{decode, Version};
use logit_proto::prometheus::PrometheusDecoder;

fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else {
        return;
    };
    let version = if selector & 1 == 0 { Version::V1 } else { Version::V2 };
    let _ = decode(body, version, &mut PrometheusDecoder::new());
});
