//! The native-wire-format bake-off: four arms encoding/decoding the same [`EventBatch`] fixtures,
//! feeding `docs/adr/native-wire-format-encoding.md` and the throughput/size tables in
//! `docs/design/wire-protocol.md`. See `tests/wire_format_bakeoff.rs` for the fidelity and
//! version-skew gates, and `benches/wire_format.rs` for the timing/size measurements.
//!
//! - **`native`** -- the shipped `logit_proto::native` codec (hand-rolled, dictionary-first).
//! - **`otlp`** -- the existing `logit_proto::otlp` codec, as the interop control arm.
//! - **`rkyv`** -- zero-copy archival, over [`wire_mirror::WireBatch`].
//! - **`postcard`** -- derived `serde` binary encoding, over the same mirror type.

pub mod wire_mirror;

use bytes::Bytes;
use logit_core::EventBatch;
use logit_proto::otlp::{OtlpDecoder, OtlpEncoder};
use logit_proto::{Decoder, Encoder, SignalDecoder, SignalEncoder};
use wire_mirror::WireBatch;

// -- native: the shipped codec -----------------------------------------------------------------

pub fn native_encode(batch: &EventBatch) -> Bytes {
    let mut encoder = logit_proto::native::NativeEncoder::default();
    encoder.encode(batch).expect("native encode")
}

/// Decodes through the [`logit_proto::Decoder`] trait's `decode`/`decode_into`, the same seam
/// every real `ComponentKind` decodes through -- not `logit_proto::native::decode_batch` directly.
/// `Decoder::decode_into` now returns `(Arc<Resource>, Option<Arc<Scope>>)`, so `Decoder::decode`'s
/// default body carries a batch's `scope` through into the `EventBatch` it hands back, exactly
/// like every other field; this arm no longer has to bypass the trait to keep its round trip
/// lossless on `scope`. Native is the one arm expected to be exact end to end
/// (`tests/wire_format_bakeoff.rs`'s fidelity gate).
pub fn native_decode(bytes: Bytes) -> EventBatch {
    let mut decoder = logit_proto::native::NativeDecoder;
    decoder.decode(bytes).expect("native decode")
}

// -- otlp: the interop control arm ---------------------------------------------------------------

/// Encodes `batch` the way `otlp_out` actually would: `SignalEncoder::encode_signals` producing
/// zero to three separate payloads, each tagged with which [`logit_proto::Signal`] it is --
/// `encode_signals` doesn't guarantee a fixed ordering across signals, so a caller that needs to
/// decode these back (`otlp_round_trip`) must keep the tag, not assume a position. Returned
/// individually (not concatenated) since that splitting -- one `EventBatch` becoming several
/// independent wire messages -- is itself part of what this arm costs, both in bytes (repeated
/// resource/scope framing per payload) and in fidelity (see [`otlp_round_trip`]).
pub fn otlp_encode(batch: &EventBatch) -> Vec<(logit_proto::Signal, Bytes)> {
    let mut encoder = OtlpEncoder::new();
    encoder.encode_signals(batch).expect("otlp encode")
}

/// Decodes every payload [`otlp_encode`] produced back into `EventBatch`es. Returns a `Vec`,
/// deliberately: an `EventBatch` carrying a log, a metric, and a span at once
/// (`docs/adr/multi-payload-events.md`) encodes to up to three payloads and decodes back to up to
/// three separate batches -- this arm's fidelity gate is exactly "does this `Vec` have length 1",
/// which for a genuinely mixed batch it does not.
pub fn otlp_round_trip(batch: &EventBatch) -> Vec<EventBatch> {
    let mut decoder = OtlpDecoder::new();
    let mut out = Vec::new();
    for (signal, bytes) in otlp_encode(batch) {
        out.extend(decoder.decode_signal(signal, bytes).expect("otlp decode"));
    }
    out
}

// -- rkyv: zero-copy archival ---------------------------------------------------------------------

pub fn rkyv_encode(batch: &EventBatch) -> Vec<u8> {
    let wire = WireBatch::from_event_batch(batch);
    rkyv::to_bytes::<rkyv::rancor::Error>(&wire).expect("rkyv encode").to_vec()
}

pub fn rkyv_decode(bytes: &[u8]) -> EventBatch {
    let archived = rkyv::access::<wire_mirror::ArchivedWireBatch, rkyv::rancor::Error>(bytes)
        .expect("rkyv access");
    let wire: WireBatch =
        rkyv::deserialize::<WireBatch, rkyv::rancor::Error>(archived).expect("rkyv deserialize");
    wire.into_event_batch()
}

// -- postcard: derived serde binary encoding -------------------------------------------------------

pub fn postcard_encode(batch: &EventBatch) -> Vec<u8> {
    let wire = WireBatch::from_event_batch(batch);
    postcard::to_allocvec(&wire).expect("postcard encode")
}

pub fn postcard_decode(bytes: &[u8]) -> EventBatch {
    let wire: WireBatch = postcard::from_bytes(bytes).expect("postcard decode");
    wire.into_event_batch()
}

#[cfg(test)]
mod smoke_tests {
    use super::*;
    use crate::fixtures;

    #[test]
    fn all_four_arms_round_trip_the_nginx_fixture() {
        let batch = fixtures::nginx_batch(5);

        let native_out = native_decode(native_encode(&batch));
        assert_eq!(native_out.events.len(), batch.events.len());

        let rkyv_out = rkyv_decode(&rkyv_encode(&batch));
        assert_eq!(rkyv_out.events.len(), batch.events.len());

        let postcard_out = postcard_decode(&postcard_encode(&batch));
        assert_eq!(postcard_out.events.len(), batch.events.len());

        // OTLP splits a mixed batch across signals -- see `otlp_round_trip`'s own doc comment --
        // so this only checks that decoding doesn't error and that events survive somewhere
        // across the returned batches, not that it comes back as one batch.
        let otlp_out = otlp_round_trip(&batch);
        let total_events: usize = otlp_out.iter().map(|b| b.events.len()).sum();
        assert!(total_events > 0);
    }
}
