//! One native v1 batch payload (`logit_proto::native::decode_batch`), unframed.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut bytes = Bytes::copy_from_slice(data);
    let _ = logit_proto::native::decode_batch(&mut bytes);
});
