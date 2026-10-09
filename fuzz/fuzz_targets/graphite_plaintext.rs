//! One carbon plaintext datagram, or one framed line, through `GraphiteDecoder::decode_into`, as
//! `graphite_in` hands it over. The whole input is the datagram.
//!
//! Oracles, each over every input:
//! - lines: the datagram decodes to the same events as its `\n`-separated pieces decoded one at a
//!   time with the same `received_at`, appended after what `out` already held. Plaintext never
//!   fails as a whole: a piece that isn't valid UTF-8 costs only itself
//!   (`crates/logit-proto/src/graphite/decode.rs`'s `decode_plaintext`);
//! - the model: each piece is one event or none, and it's the event `shared::graphite`'s
//!   `expected_event` builds from the piece's three whitespace-separated fields, after one
//!   trailing `\r` comes off: the path, the tags, the value's bits, and the timestamp
//!   at its exact reading, `-1` as `received_at`;
//! - shape and zero-copy: every event has one finite `Gauge` and `Str` tags that slice the
//!   datagram (`shared::graphite`'s `check_event`);
//! - the second-generation fixed point in both protocols (`shared::graphite`'s
//!   `assert_second_generation_fixed_point`).
#![no_main]

#[path = "shared/graphite.rs"]
mod graphite;

use bytes::Bytes;
use graphite::{decoder, marker, RECEIVED_AT};
use libfuzzer_sys::fuzz_target;
use logit_proto::graphite::Protocol;
use logit_proto::Decoder;

/// What the decoder makes of one piece: the expected event, if the piece is a datapoint.
fn expected_from_piece(piece: &[u8]) -> Option<(logit_core::Event, bool)> {
    let line = std::str::from_utf8(piece.strip_suffix(b"\r").unwrap_or(piece)).ok()?;
    let mut fields = line.split_whitespace();
    let (Some(path_field), Some(value), Some(seconds), None) =
        (fields.next(), fields.next(), fields.next(), fields.next())
    else {
        return None;
    };
    graphite::expected_event(path_field, value.parse().ok()?, seconds.parse().ok()?)
}

fuzz_target!(|data: &[u8]| {
    let datagram = Bytes::copy_from_slice(data);
    let mut whole = vec![marker()];
    decoder(Protocol::Plaintext)
        .decode_into(datagram.clone(), RECEIVED_AT, &mut whole)
        .expect("lines: plaintext never fails as a whole");
    assert_eq!(whole[0], marker(), "lines: decode_into appends after out's contents");

    let mut pieces = vec![marker()];
    let mut by_line = decoder(Protocol::Plaintext);
    for piece in data.split(|&b| b == b'\n') {
        let before = pieces.len();
        by_line
            .decode_into(Bytes::copy_from_slice(piece), RECEIVED_AT, &mut pieces)
            .expect("lines: plaintext never fails as a whole");
        match expected_from_piece(piece) {
            Some(expected) => {
                assert_eq!(pieces.len(), before + 1, "model: a datapoint line is one event");
                graphite::assert_matches(&pieces[before], &expected, "model");
            }
            None => assert_eq!(pieces.len(), before, "model: a skipped line is no event"),
        }
    }
    assert_eq!(whole, pieces, "lines: the datagram and its lines one at a time disagree");

    for event in &whole[1..] {
        graphite::check_event(&datagram, event);
    }
    graphite::assert_second_generation_fixed_point(whole.split_off(1));
});
