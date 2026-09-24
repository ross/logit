//! The native-wire-format bake-off: four arms encoding and decoding the same [`EventBatch`]
//! fixtures, the evidence behind `docs/adr/native-wire-format-encoding.md` (which holds the
//! comparison, throughput, and size tables). `tests/wire_format_bakeoff.rs` is the fidelity gate
//! every arm passes before its timings count; `benches/wire_format.rs` measures time and encoded
//! size.
//!
//! - **`native`**: the shipped `logit_proto::native` codec (hand-rolled, dictionary-first).
//! - **`otlp`**: the shipped `logit_proto::otlp` codec, as the interop control arm.
//! - **`rkyv`**: zero-copy archival, over [`wire_mirror::WireBatch`].
//! - **`postcard`**: derived `serde` binary encoding, over the same mirror type.
//!
//! Every arm takes and returns an `EventBatch`, so a timing includes whatever conversion the arm
//! needs: `rkyv` and `postcard` pay `WireBatch::from_event_batch`/`into_event_batch` inside the
//! timed region, as an adoption would. `rkyv_decode` fully deserializes rather than reading the
//! archive in place, because the pipeline consumes owned `Event`s (the ADR's "The bake-off"
//! section, finding 3).

/// A second, unrelated bake-off on the same "mirror the shipped type locally, change nothing in
/// production" pattern: the attribute-sizing arms (`benches/attr_arms.rs`, `tests/attr_arms.rs`).
pub mod attr_arms;
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

/// Decodes through the [`logit_proto::Decoder`] trait's `decode`/`decode_into`, the seam every
/// real `ComponentKind` decodes through, not `logit_proto::native::decode_batch` directly.
/// `Decoder::decode` carries the batch's `scope` through like every other field, so the round trip
/// stays exact (`tests/wire_format_bakeoff.rs`'s fidelity gate).
pub fn native_decode(bytes: Bytes) -> EventBatch {
    let mut decoder = logit_proto::native::NativeDecoder;
    decoder.decode(bytes).expect("native decode")
}

// -- otlp: the interop control arm ---------------------------------------------------------------

/// Encodes `batch` the way `otlp_out` would: `SignalEncoder::encode_signals` producing zero to
/// three separate payloads, each tagged with its [`logit_proto::Signal`]. `encode_signals`
/// guarantees no ordering across signals, so a caller decoding them back ([`otlp_round_trip`])
/// must keep the tag, not assume a position. Returned individually, not concatenated: one
/// `EventBatch` becoming several wire messages is part of what this arm costs, in bytes (repeated
/// resource/scope framing per payload) and in fidelity.
pub fn otlp_encode(batch: &EventBatch) -> Vec<(logit_proto::Signal, Bytes)> {
    let mut encoder = OtlpEncoder::new();
    encoder.encode_signals(batch).expect("otlp encode")
}

/// Decodes every payload [`otlp_encode`] produced back into `EventBatch`es. Returns a `Vec`
/// because an `EventBatch` carrying a log, a metric, and a span at once
/// (`docs/adr/multi-payload-events.md`) encodes to up to three payloads and decodes to up to three
/// separate batches. A mixed batch fails this arm's fidelity gate on the `Vec`'s length.
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

        // OTLP splits a mixed batch across signals (see `otlp_round_trip`), so this only checks
        // that decoding succeeds and events survive somewhere across the returned batches.
        let otlp_out = otlp_round_trip(&batch);
        let total_events: usize = otlp_out.iter().map(|b| b.events.len()).sum();
        assert!(total_events > 0);
    }
}
