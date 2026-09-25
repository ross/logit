//! One native v1 batch payload (`logit_proto::native::decode_batch`), unframed, under the
//! decode budget `NativeDecoder` gives a frame under the default frame cap.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_proto::native::DecodeBudget;

fuzz_target!(|data: &[u8]| {
    let mut bytes = Bytes::copy_from_slice(data);
    let _ = logit_proto::native::decode_batch(&mut bytes, &DecodeBudget::default());
});
