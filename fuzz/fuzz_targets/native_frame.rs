//! Native frames (`logit_proto::frame`) off a byte stream, as `DiskQueue` walks a segment file:
//! read a frame, and on a rejected one scan to the next magic with `resync`.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_proto::frame::{read_frame_with_header, resync};
use logit_proto::CodecError;

fuzz_target!(|data: &[u8]| {
    let all = Bytes::copy_from_slice(data);
    let mut pos = 0;
    while pos < all.len() {
        let mut rest = all.slice(pos..);
        match read_frame_with_header(&mut rest) {
            Ok(_) => pos = all.len() - rest.len(),
            Err(CodecError::Truncated { .. }) => break,
            // A spurious magic inside a payload also lands here; scanning resumes one byte on.
            Err(_) => match resync(&all[pos + 1..]) {
                Some(offset) => pos += 1 + offset,
                None => break,
            },
        }
    }
});
