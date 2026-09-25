//! One native v2 batch payload (`logit_proto::native::decode_batch_v2`), unframed.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut bytes = Bytes::copy_from_slice(data);
    let _ = logit_proto::native::decode_batch_v2(&mut bytes);
});
