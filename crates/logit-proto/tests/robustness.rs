//! Robustness/mutation testing over every decoder that will read untrusted bytes off a socket:
//! [`frame::read_frame`], [`native::decode_batch`], each `native::control::*::decode`, and
//! `collectd::CollectdDecoder` (a UDP listener's datagrams are as untrusted as a `logit_in`
//! connection's frames, and rather easier to spoof). Workstream A of
//! `docs/plans/native-transport.md`: this gate must exist and pass *before* `logit_in` listens on a
//! real network -- and `docs/plans/collectd-binary-relay.md` holds `collectd_in` to the same bar.
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
use logit_core::{AttrMap, Event, EventBatch, LogRecord, Provenance, Resource, Severity, Value};
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
    let resource = Arc::new(Resource { attributes: resource_attrs, ..Resource::default() });

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
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    EventBatch { resource, scope: None, events: vec![event] }
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
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![event] }
}

/// A complete, realistic collectd datagram: two value lists sharing one host/plugin/type, the
/// second eliding everything but its own TypeInstance -- so a truncation or a bit flip can land in a
/// part header, an identity string, a sticky-state boundary, or a value vector.
fn sample_collectd_packet() -> Vec<u8> {
    /// Appends one part: `type u16 BE, len u16 BE` (the length *includes* the header), payload.
    /// Hand-written rather than reusing `logit_proto::collectd::part`'s writers, so a bug in those
    /// writers cannot quietly produce a fixture this suite then declares safe.
    fn part(out: &mut Vec<u8>, part_type: u16, payload: &[u8]) {
        out.extend_from_slice(&part_type.to_be_bytes());
        out.extend_from_slice(&((4 + payload.len()) as u16).to_be_bytes());
        out.extend_from_slice(payload);
    }
    fn string(out: &mut Vec<u8>, part_type: u16, value: &[u8]) {
        let mut payload = value.to_vec();
        payload.push(0);
        part(out, part_type, &payload);
    }
    fn number(out: &mut Vec<u8>, part_type: u16, value: u64) {
        part(out, part_type, &value.to_be_bytes());
    }
    fn values(out: &mut Vec<u8>, entries: &[(u8, [u8; 8])]) {
        let mut payload = Vec::new();
        payload.extend_from_slice(&(entries.len() as u16).to_be_bytes());
        for (ds_type, _) in entries {
            payload.push(*ds_type);
        }
        for (_, raw) in entries {
            payload.extend_from_slice(raw);
        }
        part(out, 0x0006, &payload);
    }

    let mut out = Vec::new();
    string(&mut out, 0x0000, b"robustness-host");
    number(&mut out, 0x0008, 1_700_000_000u64 << 30); // TimeHR
    number(&mut out, 0x0009, 10u64 << 30); // IntervalHR
    string(&mut out, 0x0002, b"cpu"); // Plugin
    string(&mut out, 0x0003, b"0"); // PluginInstance
    string(&mut out, 0x0004, b"cpu"); // Type
    string(&mut out, 0x0005, b"user"); // TypeInstance
    values(&mut out, &[(2, 1234i64.to_be_bytes())]); // one DERIVE
    string(&mut out, 0x0005, b"system");
    values(&mut out, &[(1, 0.5f64.to_le_bytes()), (0, 7u64.to_be_bytes())]); // GAUGE + COUNTER
    out
}

/// Runs `CollectdDecoder::decode_into` over `bytes` with a fresh decoder, returning whether it
/// failed. A fresh decoder per call because the sweeps below run one mutation at a time and must not
/// let an earlier one's state colour a later one.
fn decode_collectd(bytes: &Bytes) -> bool {
    let mut decoder = logit_proto::collectd::CollectdDecoder::new(Arc::new(Resource::default()));
    let mut events = Vec::new();
    decoder.decode_into(bytes.clone(), 0, &mut events).is_err()
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

// -- native::decode_batch_v2 ----------------------------------------------------------------

fn sample_provenance() -> Provenance {
    Provenance {
        origin: Some(logit_core::interner::intern("robustness_nginx_in")),
        previous: Some(logit_core::interner::intern("robustness_enrich")),
    }
}

#[test]
fn decode_batch_v2_survives_every_single_byte_truncation() {
    let payload = native::encode_batch_v2(&sample_batch(), sample_provenance());
    assert_every_truncation_fails_cleanly(&payload, |bytes| {
        native::decode_batch_v2(bytes).is_err()
    });
}

#[test]
fn decode_batch_v2_survives_seeded_bit_flips() {
    let payload = native::encode_batch_v2(&sample_batch(), sample_provenance());
    assert_bit_flips_never_panic(&payload, 5000, |bytes| native::decode_batch_v2(bytes).is_ok());
}

/// A plain v1 payload is a distinct codec, not a subset of v2's -- `decode_batch_v2` must reject
/// it rather than silently decode it as "no provenance" (`native::mod`'s own inline test covers
/// the same property; this is the adversarial-suite copy for consistency with every other decoder
/// here).
#[test]
fn decode_batch_v2_rejects_a_plain_v1_payload() {
    let mut payload = native::encode_batch(&sample_batch());
    assert!(native::decode_batch_v2(&mut payload).is_err());
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

// -- collectd ---------------------------------------------------------------------------------

/// The **weaker** truncation helper, deliberately: a collectd datagram is a flat sequence of
/// self-delimiting parts with no trailing checksum or total length, so a prefix that happens to end
/// exactly on a part boundary is not a truncation at all -- it is a shorter, perfectly valid
/// datagram, and `decode_into` correctly returns `Ok` with the lists it did read. (The same is true
/// one level down: a prefix ending mid-list still yields `Ok`, because every list the truncation
/// left intact is kept and only the rest of the datagram is abandoned -- `collectd/decode.rs`'s
/// per-datagram isolation rule.) `assert_every_truncation_fails_cleanly`'s stronger property
/// genuinely does not hold here, and asserting it would be asserting a bug.
#[test]
fn collectd_decode_survives_every_single_byte_truncation() {
    let packet = sample_collectd_packet();
    assert_every_truncation_never_panics(&packet, |bytes| decode_collectd(bytes));
}

#[test]
fn collectd_decode_survives_seeded_bit_flips() {
    let packet = sample_collectd_packet();
    assert_bit_flips_never_panic(&packet, 4000, |bytes| decode_collectd(bytes));
}

/// A Values part declaring 65535 data sources over a 20-byte buffer: the count is attacker-chosen
/// (`u16`) and must be checked against the part's own declared length *before* anything is sized
/// from it. 65535 data sources would be ~590 KB of type bytes and values.
#[test]
fn collectd_decode_rejects_a_values_count_inflated_far_past_what_the_input_holds() {
    let mut hostile = Vec::new();
    hostile.extend_from_slice(&0x0006u16.to_be_bytes()); // Values
    hostile.extend_from_slice(&20u16.to_be_bytes()); // a 20-byte part...
    hostile.extend_from_slice(&65535u16.to_be_bytes()); // ...declaring 65535 data sources
    hostile.extend_from_slice(&[0xAA; 14]);
    let bytes = Bytes::from(hostile);
    assert!(decode_collectd(&bytes), "an impossible data-source count must be rejected");

    let peak = peak_live_bytes(|| {
        let _ = decode_collectd(&bytes);
    });
    // 65535 data sources would be ~590 KB before a single byte of it was read; anything under a few
    // KB proves the declared count was checked against the part's own length first.
    assert!(peak < 4096, "peak live bytes {peak} suggests the values count was trusted");
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
