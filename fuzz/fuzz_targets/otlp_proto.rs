//! An OTLP protobuf body (`OtlpDecoder::decode_signal`). The first byte `% 3` picks the signal
//! (logs, metrics, traces); the rest is the body.
#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use logit_proto::otlp::OtlpDecoder;
use logit_proto::{Signal, SignalDecoder};

fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else {
        return;
    };
    let signal = [Signal::Logs, Signal::Metrics, Signal::Traces][selector as usize % 3];
    let _ = OtlpDecoder::new().decode_signal(signal, Bytes::copy_from_slice(body));
});
