//! A record value under `format: msgpack` (`MsgpackEventDecoder::decode_batches_with_budget`):
//! one or more MessagePack event maps back to back, under a [`BUDGET`]-byte decode budget.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_proto::msgpack_event::MsgpackEventDecoder;
use logit_proto::native::DecodeBudget;

/// Far under the default 256 MiB, so an input under `max_len` reaches it: 64 KiB of nils in one
/// array charges a tree node per byte, past 1 MiB.
const BUDGET: u64 = 1 << 20;

fuzz_target!(|data: &[u8]| {
    let bytes = Bytes::copy_from_slice(data);
    let _ =
        MsgpackEventDecoder::new().decode_batches_with_budget(&bytes, &DecodeBudget::new(BUDGET));
});
