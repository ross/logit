//! Robustness/mutation testing over every decoder that will read untrusted bytes off a
//! `logit_in` socket: [`frame::read_frame`], [`native::decode_batch`], and each
//! `native::control::*::decode`. Workstream A of `docs/plans/native-transport.md`: this gate must
//! exist and pass *before* `logit_in` listens on a real network.
//!
//! What's checked, for each decoder: every single-byte truncation of a valid input; several
//! thousand seeded bit flips; a length-bearing field inflated to a value far past what the input
//! actually holds; and (for `decode_batch`, the one decoder whose payload is recursive)
//! `Value::Array`/`Value::Map` nested past the decode-side depth cap. In every case: the decoder
//! must never panic, and never allocate as if it trusted an attacker-declared length before
//! bounding it (checked directly via a peak-allocation counter on the crafted-huge-count cases,
//! where the property actually bites). **Not every decoder here holds the same truncation
//! contract, though** -- see [`assert_every_truncation_never_panics`] vs.
//! [`assert_every_truncation_fails_cleanly`]'s own doc comments for which decoders hold which,
//! and why.
//!
//! No RNG crate exists anywhere in this workspace (`docs/plans/native-transport.md`'s own
//! research confirmed it) and none is added here -- [`Lcg`] below is a hand-rolled, seeded,
//! reproducible PRNG, good enough for "flip a pseudo-random bit," not for anything
//! cryptographic.
//!
//! Keep this suite fast (`script/test` runs it on every `cargo nextest run`) -- iteration counts
//! are tuned to stay well under a second per decoder, not the full 10s budget
//! `docs/plans/native-transport.md` allows.

use bytes::{Bytes, BytesMut};
use logit_core::{AttrMap, Event, EventBatch, LogRecord, Resource, Severity, Value};
use logit_proto::frame::{self, Compression};
use logit_proto::native::control::{self, Ack, Hello, HelloAck, Reject};
use logit_proto::native::varint::write_uvarint;
use logit_proto::native::{self, NativeDecoder};
use logit_proto::{Decoder, Encoder};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

// -- a tiny, self-contained peak-allocation counter -----------------------------------------
//
// Not a reuse of `crates/logit-bench`'s `CountingAlloc` -- `logit-bench` depends on
// `logit-proto`, not the other way around, so this crate can't borrow it. Same idea, scoped down
// to just what this suite needs: how many bytes were live at the high-water mark during one
// measured call.

struct CountingAlloc;

thread_local! {
    static LIVE: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as i64);
        unsafe { System.alloc(layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as i64);
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(-(layout.size() as i64));
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size as i64 - layout.size() as i64);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn record(delta: i64) {
    LIVE.with(|live| {
        let now = live.get() + delta;
        live.set(now);
        PEAK.with(|peak| {
            if now > peak.get() {
                peak.set(now);
            }
        });
    });
}

#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc;

/// Runs `f`, returning the peak live-byte high-water mark reached while it ran. Not
/// warmed/isolated the way `logit-bench`'s allocation tests are (this only needs an order-of-
/// magnitude bound, not an exact count), but each call zeroes the counters first so one crafted
/// call's peak isn't inflated by whatever ran before it.
fn peak_live_bytes(f: impl FnOnce()) -> i64 {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
    f();
    PEAK.with(|peak| peak.get())
}

// -- seeded LCG -------------------------------------------------------------------------------

/// A minimal linear congruential generator -- reproducible across runs (fixed seed), good enough
/// to pick "which byte, which bit" for a mutation sweep. Constants are Numerical Recipes' LCG.
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
        self.0
    }

    fn next_usize(&mut self, bound: usize) -> usize {
        if bound == 0 {
            0
        } else {
            (self.next_u64() % bound as u64) as usize
        }
    }
}

// -- generic mutation sweeps, parameterized over one decoder at a time ------------------------

/// Truncates `valid` to every length from 0 to `valid.len() - 1` and asserts the decoder never
/// panics -- the weaker of the two truncation contracts in this file, for decoders that genuinely
/// don't guarantee more. `decode` returns whether the decode *failed* (every call site passes
/// `.is_err()`), but that return value is deliberately not asserted on here.
///
/// The control messages (`Hello`/`HelloAck`/`Ack`/`Reject`/`ControlMessage`) use this weaker
/// helper on purpose, not by oversight: their TLV fields are all-optional with silent
/// zero/empty defaults by design (this module's own doc comment on
/// `logit_proto::native::control`, `read_field`'s `Ok(None)` on an exhausted body, and the
/// `hello_with_empty_lists_round_trips` unit test all pin this down), so a short truncation of a
/// valid message routinely decodes `Ok` with some fields defaulted -- that is the protocol
/// working as designed, not a bug this suite should flag. It also isn't a production hole:
/// `logit_in::handshake` version-checks a zeroed `Hello` and rejects it explicitly.
fn assert_every_truncation_never_panics(valid: &[u8], decode: impl Fn(&mut Bytes) -> bool) {
    for len in 0..valid.len() {
        let mut truncated = Bytes::copy_from_slice(&valid[..len]);
        let panicked =
            std::panic::catch_unwind(AssertUnwindSafe(|| decode(&mut truncated))).is_err();
        assert!(!panicked, "decoding a {len}-byte truncation of a valid input panicked");
    }
}

/// [`assert_every_truncation_never_panics`], plus the stronger property [`frame::read_frame`] and
/// [`native::decode_batch`] actually hold: no proper prefix of a valid encoding is itself a valid
/// encoding, because every one of them ends in length-checked content rather than optional
/// trailing fields the way the control messages do (see
/// [`assert_every_truncation_never_panics`]'s own doc comment for that contrast). `decode` returns
/// whether the decode *failed* (every call site passes `.is_err()`) -- call sites here assert on
/// that, not just on panic-freedom.
fn assert_every_truncation_fails_cleanly(valid: &[u8], decode: impl Fn(&mut Bytes) -> bool) {
    for len in 0..valid.len() {
        let mut truncated = Bytes::copy_from_slice(&valid[..len]);
        let failed = match std::panic::catch_unwind(AssertUnwindSafe(|| decode(&mut truncated))) {
            Ok(failed) => failed,
            Err(_) => panic!("decoding a {len}-byte truncation of a valid input panicked"),
        };
        assert!(failed, "a {len}-byte truncation of a valid input decoded successfully");
    }
}

/// Flips one pseudo-random bit in a fresh copy of `valid`, `iterations` times, asserting only
/// that the decoder never panics -- a single bit flip can still land on a valid-looking encoding
/// (e.g. inside a string's content), so `Err` is not asserted, only "did not panic."
fn assert_bit_flips_never_panic(
    valid: &[u8],
    iterations: usize,
    decode: impl Fn(&mut Bytes) -> bool,
) {
    if valid.is_empty() {
        return;
    }
    // Fixed, reproducible seed -- a failure here should reproduce on the next run, not require
    // capturing which random bytes it happened to pick.
    let mut rng = Lcg::new(0x5EED_5EED_5EED_5EEDu64);
    for _ in 0..iterations {
        let mut mutated = valid.to_vec();
        let byte_idx = rng.next_usize(mutated.len());
        let bit = rng.next_usize(8);
        mutated[byte_idx] ^= 1 << bit;
        let mut bytes = Bytes::from(mutated);
        let panicked = std::panic::catch_unwind(AssertUnwindSafe(|| decode(&mut bytes))).is_err();
        assert!(!panicked, "a single bit flip at byte {byte_idx} bit {bit} panicked");
    }
}

// -- fixtures -----------------------------------------------------------------------------------

fn sample_batch() -> EventBatch {
    let mut resource_attrs = AttrMap::new();
    resource_attrs.insert("service.name", "robustness-fixture");
    let resource = Arc::new(Resource { attributes: resource_attrs });

    let mut log_attrs = AttrMap::new();
    log_attrs.insert("host", "robustness-host");
    let event = Event::log(
        1,
        log_attrs,
        LogRecord {
            message: Value::str("hello, robustness suite"),
            severity: Some(Severity::Info),
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
        },
    );
    EventBatch { resource, events: vec![event] }
}

fn deeply_nested_batch(depth: usize) -> EventBatch {
    let mut value = Value::Array(vec![Value::Null]);
    for _ in 0..depth {
        value = Value::Array(vec![value]);
    }
    let mut attrs = AttrMap::new();
    attrs.insert("nested", value);
    let event = Event::log(
        1,
        attrs,
        LogRecord {
            message: Value::str("nested"),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
        },
    );
    EventBatch { resource: Arc::new(Resource::default()), events: vec![event] }
}

fn sample_hello() -> Hello {
    Hello {
        version: control::PROTOCOL_VERSION,
        codecs: vec![1],
        compressions: vec![0, 1],
        max_frame_bytes: 4096,
        window: 1,
    }
}

fn sample_hello_ack() -> HelloAck {
    HelloAck {
        version: control::PROTOCOL_VERSION,
        codec: 1,
        compression: 1,
        max_frame_bytes: 4096,
        window: 1,
    }
}

fn sample_ack() -> Ack {
    Ack { seq: 12345 }
}

fn sample_reject() -> Reject {
    Reject { code: control::REJECT_NO_COMMON_CODEC, message: "no shared codec".to_string() }
}

// -- read_frame ---------------------------------------------------------------------------------

#[test]
fn read_frame_survives_every_single_byte_truncation() {
    let framed = frame::write_frame(1, Compression::None, b"robustness payload").unwrap();
    assert_every_truncation_fails_cleanly(&framed, |bytes| frame::read_frame(bytes).is_err());
}

#[test]
fn read_frame_survives_seeded_bit_flips() {
    let framed = frame::write_frame(1, Compression::Lz4, "repeat ".repeat(50).as_bytes()).unwrap();
    assert_bit_flips_never_panic(&framed, 5000, |bytes| frame::read_frame(bytes).is_ok());
}

#[test]
fn read_frame_rejects_an_uncompressed_len_inflated_to_u32_max() {
    let framed = frame::write_frame(1, Compression::None, b"small").unwrap();
    let mut mutated = BytesMut::from(&framed[..]);
    mutated[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut bad = mutated.freeze();
    assert!(frame::read_frame(&mut bad).is_err());
}

#[test]
fn read_frame_rejects_a_compressed_len_inflated_to_u32_max() {
    let framed = frame::write_frame(1, Compression::None, b"small").unwrap();
    let mut mutated = BytesMut::from(&framed[..]);
    mutated[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut bad = mutated.freeze();
    assert!(frame::read_frame(&mut bad).is_err());
}

#[test]
fn read_frame_never_allocates_proportionally_to_a_hostile_uncompressed_len() {
    let framed = frame::write_frame(1, Compression::None, b"small").unwrap();
    let mut mutated = BytesMut::from(&framed[..]);
    mutated[12..16].copy_from_slice(&u32::MAX.to_le_bytes());
    let mut bad = mutated.freeze();
    let peak = peak_live_bytes(|| {
        let _ = frame::read_frame(&mut bad);
    });
    // u32::MAX bytes would be ~4 GiB -- anything under 1 MiB proves the header's declared length
    // was checked against the sanity cap before it was ever used to size an allocation.
    assert!(peak < 1024 * 1024, "peak live bytes {peak} suggests the length field was trusted");
}

// -- native::decode_batch -----------------------------------------------------------------------

#[test]
fn decode_batch_survives_every_single_byte_truncation() {
    let payload = native::encode_batch(&sample_batch());
    assert_every_truncation_fails_cleanly(&payload, |bytes| native::decode_batch(bytes).is_err());
}

#[test]
fn decode_batch_survives_seeded_bit_flips() {
    let payload = native::encode_batch(&sample_batch());
    assert_bit_flips_never_panic(&payload, 5000, |bytes| native::decode_batch(bytes).is_ok());
}

#[test]
fn decode_batch_rejects_a_dictionary_count_inflated_far_past_the_sanity_cap() {
    // The dictionary entry count is the very first varint in the payload (`native::mod`'s
    // `encode_batch`/`Dict::read`) -- overwrite it with a huge count and nothing else, no dict
    // content behind it.
    let mut out = BytesMut::new();
    write_uvarint(&mut out, u32::MAX as u64);
    let mut bytes = out.freeze();
    assert!(native::decode_batch(&mut bytes).is_err());
}

#[test]
fn decode_batch_never_allocates_proportionally_to_a_hostile_dictionary_count() {
    let mut out = BytesMut::new();
    write_uvarint(&mut out, u32::MAX as u64);
    let mut bytes = out.freeze();
    let peak = peak_live_bytes(|| {
        let _ = native::decode_batch(&mut bytes);
    });
    assert!(peak < 1024 * 1024, "peak live bytes {peak} suggests the dict count was trusted");
}

#[test]
fn decode_batch_rejects_value_nesting_past_the_depth_cap() {
    // 128 is `native::value`'s own `MAX_VALUE_DEPTH` -- go one past it, through the real encoder,
    // exercising the cap at the full-batch level rather than `value`'s own inline unit test.
    let batch = deeply_nested_batch(129);
    let mut payload = native::encode_batch(&batch);
    assert!(native::decode_batch(&mut payload).is_err());
}

#[test]
fn decode_batch_at_exactly_the_depth_cap_still_decodes() {
    let batch = deeply_nested_batch(127);
    let mut payload = native::encode_batch(&batch);
    assert!(native::decode_batch(&mut payload).is_ok());
}

#[test]
fn native_decoder_decode_into_never_panics_on_a_truncated_framed_batch() {
    // The full `Decoder` entry point `logit_in` actually calls -- `read_frame` + `decode_batch`
    // composed, rather than either alone.
    let mut encoder = native::NativeEncoder::default();
    let framed = encoder.encode(&sample_batch()).unwrap();
    for len in 0..framed.len() {
        let truncated = framed.slice(0..len);
        let mut decoder = NativeDecoder;
        let mut events = Vec::new();
        let panicked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            decoder.decode_into(truncated.clone(), 0, &mut events)
        }))
        .is_err();
        assert!(!panicked, "decode_into panicked on a {len}-byte truncated frame");
    }
}

// -- control messages -----------------------------------------------------------------------

#[test]
fn hello_survives_every_single_byte_truncation() {
    let encoded = sample_hello().encode();
    assert_every_truncation_never_panics(&encoded, |bytes| Hello::decode(bytes).is_err());
}

#[test]
fn hello_survives_seeded_bit_flips() {
    let encoded = sample_hello().encode();
    assert_bit_flips_never_panic(&encoded, 3000, |bytes| Hello::decode(bytes).is_ok());
}

#[test]
fn hello_ack_survives_every_single_byte_truncation() {
    let encoded = sample_hello_ack().encode();
    assert_every_truncation_never_panics(&encoded, |bytes| HelloAck::decode(bytes).is_err());
}

#[test]
fn hello_ack_survives_seeded_bit_flips() {
    let encoded = sample_hello_ack().encode();
    assert_bit_flips_never_panic(&encoded, 3000, |bytes| HelloAck::decode(bytes).is_ok());
}

#[test]
fn ack_survives_every_single_byte_truncation() {
    let encoded = sample_ack().encode();
    assert_every_truncation_never_panics(&encoded, |bytes| Ack::decode(bytes).is_err());
}

#[test]
fn ack_survives_seeded_bit_flips() {
    let encoded = sample_ack().encode();
    assert_bit_flips_never_panic(&encoded, 3000, |bytes| Ack::decode(bytes).is_ok());
}

#[test]
fn reject_survives_every_single_byte_truncation() {
    let encoded = sample_reject().encode();
    assert_every_truncation_never_panics(&encoded, |bytes| Reject::decode(bytes).is_err());
}

#[test]
fn reject_survives_seeded_bit_flips() {
    let encoded = sample_reject().encode();
    assert_bit_flips_never_panic(&encoded, 3000, |bytes| Reject::decode(bytes).is_ok());
}

#[test]
fn reject_rejects_a_message_length_inflated_far_past_what_the_input_holds() {
    // Hand-build a Reject payload whose message field declares far more bytes than actually
    // follow -- the field-length check (`control::read_field`), not the 1 KiB message-size cap,
    // is what this exercises.
    let mut out = BytesMut::new();
    out.extend_from_slice(&[4]); // MSG_REJECT
    out.extend_from_slice(&[2]); // REJECT_FIELD_MESSAGE tag
    write_uvarint(&mut out, u32::MAX as u64); // declared length, wildly over what follows
    out.extend_from_slice(b"short");
    let mut bytes = out.freeze();
    assert!(Reject::decode(&mut bytes).is_err());
}

#[test]
fn control_message_dispatch_survives_every_single_byte_truncation_of_every_message_kind() {
    for encoded in [
        sample_hello().encode(),
        sample_hello_ack().encode(),
        sample_ack().encode(),
        sample_reject().encode(),
    ] {
        assert_every_truncation_never_panics(&encoded, |bytes| {
            control::ControlMessage::decode(bytes).is_err()
        });
    }
}
