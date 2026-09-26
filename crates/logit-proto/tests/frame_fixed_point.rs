//! Fixed-point and boundary proptests over `logit_proto::frame`, the disk record envelope
//! (`docs/plans/critical-sections-inventory.md`'s DISK-13). `frame.rs` itself is out of scope for
//! this file: it belongs to a separate rewrite, so every test here goes through the module's
//! public API only, and the one place a test needs a value it can't import (the private
//! `MAX_SANE_COMPRESSED_LEN`), it recomputes the formula from `MAX_SANE_UNCOMPRESSED_LEN` instead
//! of poking the constant directly. See ADR `native-wire-format-encoding` for why the format looks
//! the way it does.
//!
//! `frame.rs`'s own unit tests already cover single hand-built cases (a truncated header, a
//! corrupted CRC, an oversized length field). This file adds what a handful of hand-picked bytes
//! can't: a payload space wide enough, and large enough, to catch a boundary the unit tests happen
//! not to hit.

use bytes::{BufMut, Bytes, BytesMut};
use logit_proto::frame::{
    self, read_frame, read_frame_with_header, write_frame, write_frame_with_flags, Compression,
    FrameHeader, FLAG_CONTROL, MAX_SANE_UNCOMPRESSED_LEN,
};
use logit_proto::CodecError;
use proptest::prelude::*;
use std::time::Instant;

// -- payload and header-field generators ---------------------------------------------------------

/// `max` random bytes -- the case a real compressor (and a real corruptor) has the least to work
/// with.
fn random_bytes(max: usize) -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..=max)
}

/// Up to `max` bytes built by repeating a short (1-8 byte) pattern -- the case lz4 actually
/// shrinks, so a round-trip test run only against `random_bytes` would never exercise the
/// compressed path for real.
fn compressible_bytes(max: usize) -> impl Strategy<Value = Vec<u8>> {
    (proptest::collection::vec(any::<u8>(), 1..=8), 0..=max)
        .prop_map(|(pattern, len)| pattern.into_iter().cycle().take(len).collect())
}

fn payload(max: usize) -> impl Strategy<Value = Vec<u8>> {
    prop_oneof![random_bytes(max), compressible_bytes(max)]
}

fn compression() -> impl Strategy<Value = Compression> {
    prop_oneof![Just(Compression::None), Just(Compression::Lz4)]
}

/// `0`, `FLAG_CONTROL` on its own, and the full `u16` space -- so the control bit is exercised
/// every run rather than only by chance, on top of whatever else a generated value sets.
fn flags() -> impl Strategy<Value = u16> {
    prop_oneof![Just(0u16), Just(FLAG_CONTROL), any::<u16>()]
}

// -- 1: write/read is the identity on (codec, flags, compression, payload) -----------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// `read_frame_with_header(write_frame_with_flags(codec, compression, flags, payload))`
    /// reproduces every input field exactly, over payloads up to 256 KiB drawn from both a random
    /// and a compressible generator, and leaves nothing behind in the buffer -- the property that
    /// makes it safe for a caller (the durable spool, a `logit_in`/`logit_out` connection) to treat
    /// one `Bytes` as a sequence of frames read in a loop.
    #[test]
    fn write_then_read_round_trips_every_payload_under_both_compressions(
        data in payload(256 * 1024),
        codec in any::<u8>(),
        flags in flags(),
    ) {
        for compression in [Compression::None, Compression::Lz4] {
            let framed = write_frame_with_flags(codec, compression, flags, &data).unwrap();
            let mut bytes = framed;
            let (header, out) = read_frame_with_header(&mut bytes).unwrap();
            prop_assert_eq!(header.codec, codec);
            prop_assert_eq!(header.flags, flags);
            prop_assert_eq!(header.compression, compression);
            prop_assert_eq!(&out[..], &data[..]);
            prop_assert!(
                bytes.is_empty(),
                "{} byte(s) left over after read_frame_with_header consumed one frame",
                bytes.len()
            );
        }
    }
}

// -- 2: concatenation --------------------------------------------------------------------------

fn frame_spec() -> impl Strategy<Value = (Vec<u8>, u8, Compression, u16)> {
    (payload(4096), any::<u8>(), compression(), flags())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// One to eight frames of mixed sizes and compressions, concatenated the way a segment file or
    /// a stream of batches is, read back with repeated `read_frame_with_header` calls in the order
    /// they were written, with nothing left in the buffer afterward.
    #[test]
    fn concatenated_frames_read_back_in_order_with_nothing_left_over(
        specs in proptest::collection::vec(frame_spec(), 1..=8),
    ) {
        let mut buf = BytesMut::new();
        for (data, codec, compression, flags) in &specs {
            buf.put_slice(&write_frame_with_flags(*codec, *compression, *flags, data).unwrap());
        }
        let mut cursor = buf.freeze();
        for (data, codec, _compression, flags) in &specs {
            let (header, out) = read_frame_with_header(&mut cursor).unwrap();
            prop_assert_eq!(header.codec, *codec);
            prop_assert_eq!(header.flags, *flags);
            prop_assert_eq!(&out[..], &data[..]);
        }
        prop_assert!(cursor.is_empty(), "{} byte(s) left after reading every frame", cursor.len());
    }
}

// -- 3: lz4 expansion bound ----------------------------------------------------------------------

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// `frame.rs` keeps a private `MAX_SANE_COMPRESSED_LEN = MAX_SANE_UNCOMPRESSED_LEN +
    /// MAX_SANE_UNCOMPRESSED_LEN / 255 + 16` -- the documented worst case for lz4 block
    /// compression, wide enough that a legitimate frame at the uncompressed cap can never declare
    /// more than that. This test can't see the private constant, so it recomputes the same formula
    /// from the public `MAX_SANE_UNCOMPRESSED_LEN` and checks a real encoded frame's declared
    /// `compressed_len` against it directly, over payloads (up to 1 MiB) chosen to be as
    /// incompressible as lz4 ever sees: uniform random bytes, which come closest to its worst case
    /// without exceeding it.
    #[test]
    fn lz4_expansion_on_incompressible_payloads_stays_within_n_plus_n_over_255_plus_16(
        data in random_bytes(1024 * 1024),
    ) {
        let framed = write_frame(1, Compression::Lz4, &data).unwrap();
        let header = FrameHeader::read(&mut framed.clone()).unwrap();
        let n = data.len() as u64;
        let bound = n + n / 255 + 16;
        prop_assert!(
            header.compressed_len as u64 <= bound,
            "compressed_len {} exceeds n + n/255 + 16 = {bound} for n = {n}",
            header.compressed_len,
        );
    }
}

// -- 4: the uncompressed cap itself, and one byte past it -----------------------------------------

/// A minimal linear congruential generator, seeded and reproducible -- there's no RNG crate in
/// this workspace (`tests/robustness.rs`'s own doc comment says so), and this only needs
/// deterministic, non-degenerate filler, not real randomness. Constants are Knuth's MMIX LCG --
/// the same multiplier and increment `tests/robustness.rs`'s own `Lcg` uses.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }
}

/// `len` bytes of LCG output, truncated to the exact length requested.
fn lcg_bytes(seed: u64, len: usize) -> Vec<u8> {
    let mut rng = Lcg::new(seed);
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        out.extend_from_slice(&rng.next_u64().to_le_bytes());
    }
    out.truncate(len);
    out
}

/// A payload of exactly `MAX_SANE_UNCOMPRESSED_LEN` bytes (64 MiB) round-trips under both
/// compressions, and `write_frame` refuses one byte more, the same bound `read_frame` enforces.
#[test]
fn a_payload_at_the_uncompressed_cap_round_trips_under_lz4_and_none() {
    let data = lcg_bytes(0xC0FF_EE15_5EED_0001, MAX_SANE_UNCOMPRESSED_LEN as usize);

    let start = Instant::now();
    for compression in [Compression::None, Compression::Lz4] {
        let framed = write_frame(1, compression, &data).unwrap();
        let mut bytes = framed;
        let (codec, out) = read_frame(&mut bytes).unwrap();
        assert_eq!(codec, 1);
        assert_eq!(out.len(), data.len());
        assert_eq!(&out[..], &data[..]);
        assert!(bytes.is_empty());
    }
    eprintln!(
        "a_payload_at_the_uncompressed_cap_round_trips_under_lz4_and_none: {:?} for both compressions",
        start.elapsed()
    );

    let over = vec![0u8; MAX_SANE_UNCOMPRESSED_LEN as usize + 1];
    assert!(matches!(
        write_frame(1, Compression::None, &over),
        Err(CodecError::Malformed(msg)) if msg.contains("uncompressed cap")
    ));
}

// -- 5: a corrupted compressed_len below the cap is Truncated, not Malformed ----------------------

/// `compressed_len` lives at header bytes 16..20 (magic 0..4, version 4..6, flags 6..8, codec 8,
/// compression 9, reserved 10..12, uncompressed_len 12..16, compressed_len 16..20, crc32c 20..24
/// -- `FrameHeader::write`'s field order, also pinned by `frame.rs`'s own
/// `rejects_a_compressed_len_over_the_sanity_cap` test).
const COMPRESSED_LEN_RANGE: std::ops::Range<usize> = 16..20;

fn corrupt_compressed_len(framed: &Bytes, new_len: u32) -> Bytes {
    let mut mutated = BytesMut::from(&framed[..]);
    mutated[COMPRESSED_LEN_RANGE].copy_from_slice(&new_len.to_le_bytes());
    mutated.freeze()
}

/// DISK-13's premise for F1 in ADR `durable-checkpoint-writes-and-fault-injection`'s Context: a
/// `compressed_len` rewritten to a value that is still under the sanity cap, but larger than the
/// bytes actually present, reads as `CodecError::Truncated`, not `Malformed` -- from
/// `read_frame`'s own perspective this is indistinguishable from a genuine short read (a live
/// socket that hasn't delivered the rest of the frame yet, or a file that ends mid-write), so
/// `Truncated` is the correct answer *at this layer*. It's the caller that has to know more: on a
/// live connection "come back with more bytes" is right, but on a closed disk segment there will
/// never be more bytes, so treating the two the same is what F1 flags -- a closed segment's
/// reader must not keep waiting on a `Truncated` result forever, and `open` must not treat it as
/// a legitimate torn tail without first checking whether anything parseable follows. Fixing that
/// consumer behavior in `DiskQueue` is `dur/w3`; this test only pins that `frame::read_frame`
/// itself is doing the right thing.
#[test]
fn a_compressed_len_corrupted_below_the_cap_reads_as_truncated() {
    let data = vec![b'x'; 4096];
    let framed = write_frame(1, Compression::None, &data).unwrap();
    let present = framed.len() - frame::HEADER_LEN;
    let inflated = present as u32 + 4096; // still far under the sanity cap, but past what follows

    let mut bad = corrupt_compressed_len(&framed, inflated);
    match read_frame(&mut bad) {
        Err(CodecError::Truncated { needed }) => {
            assert_eq!(needed, inflated as usize - present);
        }
        other => panic!("expected Truncated, got {other:?}"),
    }
}

/// The complement: a `compressed_len` rewritten *over* the sanity cap is `Malformed`, never
/// `Truncated` -- the two corrupted-length tests together are what makes `Truncated` mean "short
/// read," full stop, for every caller that branches on the distinction. The cap value is
/// recomputed the same way as the lz4-expansion test above, for the same reason (the constant
/// itself is private to `frame.rs`).
#[test]
fn a_compressed_len_corrupted_over_the_cap_reads_as_malformed() {
    let framed = write_frame(1, Compression::None, b"small").unwrap();
    let cap = MAX_SANE_UNCOMPRESSED_LEN as u64 + MAX_SANE_UNCOMPRESSED_LEN as u64 / 255 + 16;
    let over_cap = (cap + 1) as u32;

    let mut bad = corrupt_compressed_len(&framed, over_cap);
    assert!(
        matches!(read_frame(&mut bad), Err(CodecError::Malformed(msg)) if msg.contains("sanity cap")),
        "a compressed_len over the sanity cap must be Malformed, not Truncated"
    );
}
