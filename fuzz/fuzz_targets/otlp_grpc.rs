//! An OTLP/gRPC request body as `otlp_in` handles it: `grpc::unframe`, `grpc::inflate_bounded`
//! when the compressed flag is set, then `decode_signal`. The first byte `% 3` picks the signal
//! (logs, metrics, traces); the rest is the framed body.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_proto::otlp::{grpc, OtlpDecoder};
use logit_proto::{Signal, SignalDecoder};

/// `otlp_in`'s `MAX_REQUEST_BYTES`.
const MAX_REQUEST_BYTES: usize = 4 << 20;

fuzz_target!(|data: &[u8]| {
    let Some((&selector, framed)) = data.split_first() else {
        return;
    };
    let signal = [Signal::Logs, Signal::Metrics, Signal::Traces][selector as usize % 3];
    let Some((compressed, payload)) = grpc::unframe(framed) else {
        return;
    };
    let payload = if compressed {
        match grpc::inflate_bounded(payload, MAX_REQUEST_BYTES) {
            Ok(inflated) => {
                assert!(inflated.len() <= MAX_REQUEST_BYTES);
                inflated
            }
            Err(_) => return,
        }
    } else {
        Bytes::copy_from_slice(payload)
    };
    let _ = OtlpDecoder::new().decode_signal(signal, payload);
});
