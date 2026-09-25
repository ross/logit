//! Mutation testing over every decoder that reads untrusted bytes off a socket:
//! [`frame::read_frame`], [`native::decode_batch`], each `native::control::*::decode`,
//! `collectd::CollectdDecoder` (UDP datagrams are easy to spoof), `graphite::GraphiteDecoder`
//! in both protocols, and `prometheus::compression::decompress_bounded`, which inflates every
//! remote-write body `prometheus_in` receives. Pickle is the highest-risk parser in the repo: a format built for arbitrary
//! object construction, read from a socket. A network-facing decoder must pass this suite
//! (`docs/plans/native-transport.md`).
//!
//! For each decoder: every single-byte truncation of a valid input, thousands of seeded bit flips,
//! length fields inflated past the input, and, for `decode_batch`, `Value` nesting past the depth
//! cap. The decoder must never panic, and never size an allocation from an attacker-declared
//! length before bounding it (a peak-allocation counter checks the huge-count cases). Truncation
//! contracts differ by decoder: see [`assert_every_truncation_never_panics`] and
//! [`assert_every_truncation_fails_cleanly`].
//!
//! The workspace has no RNG crate; [`Lcg`] is a seeded, reproducible PRNG, fine for picking a bit
//! to flip. Iteration counts keep each decoder well under a second, since `script/test` runs this.

use bytes::{Bytes, BytesMut};
use logit_core::{AttrMap, Event, EventBatch, LogRecord, Provenance, Resource, Severity, Value};
use logit_proto::frame::{self, Compression};
use logit_proto::graphite::{GraphiteDecoder, Protocol};
use logit_proto::native::control::{self, Ack, Hello, HelloAck, Reject};
use logit_proto::native::varint::{read_uvarint, write_uvarint};
use logit_proto::native::{self, DecodeBudget, NativeDecoder};
use logit_proto::prometheus::compression::{self, DecompressError, Encoding};
use logit_proto::{CodecError, Decoder, Encoder};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

// -- a tiny, self-contained peak-allocation counter -----------------------------------------
//
// Not `logit-bench`'s `CountingAlloc`: `logit-bench` depends on `logit-proto`, not the reverse.
// Measures only the live-byte high-water mark during one call.

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

/// Runs `f`, returning its peak live bytes: an order-of-magnitude bound, not an exact count.
/// Zeroes the counters first so an earlier call can't inflate the peak.
fn peak_live_bytes(f: impl FnOnce()) -> i64 {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
    f();
    PEAK.with(|peak| peak.get())
}

// -- seeded LCG -------------------------------------------------------------------------------

/// A linear congruential generator with Numerical Recipes' constants, reproducible from its seed.
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

/// Truncates `valid` to every shorter length and asserts only that the decoder never panics. The
/// weaker truncation contract: `decode`'s "failed" result isn't asserted.
///
/// The control messages use it because their TLV fields are all optional with zero or empty
/// defaults (`control::read_field` returns `Ok(None)` on an exhausted body), so a truncated
/// message can decode `Ok` with fields defaulted. `logit_in`'s handshake rejects a zeroed `Hello`
/// on its version.
fn assert_every_truncation_never_panics(valid: &[u8], decode: impl Fn(&mut Bytes) -> bool) {
    for len in 0..valid.len() {
        let mut truncated = Bytes::copy_from_slice(&valid[..len]);
        let panicked =
            std::panic::catch_unwind(AssertUnwindSafe(|| decode(&mut truncated))).is_err();
        assert!(!panicked, "decoding a {len}-byte truncation of a valid input panicked");
    }
}

/// [`assert_every_truncation_never_panics`] plus the stronger contract: **no proper prefix of a
/// valid encoding decodes**. [`frame::read_frame`] and [`native::decode_batch`] hold it because
/// every encoding ends in length-checked content, never an optional trailing field.
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
/// that the decoder never panics: a flip inside a string's content can still decode.
fn assert_bit_flips_never_panic(
    valid: &[u8],
    iterations: usize,
    decode: impl Fn(&mut Bytes) -> bool,
) {
    if valid.is_empty() {
        return;
    }
    // A fixed seed, so a failure reproduces.
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

/// A realistic collectd datagram: two value lists sharing one host/plugin/type (the second sets
/// only its TypeInstance) and a trailing notification, so a mutation can land in a part header,
/// an identity string, a sticky-state boundary, a value vector, or a notification part.
fn sample_collectd_packet() -> Vec<u8> {
    /// Appends one part: `type u16 BE, len u16 BE` (the length includes the header), payload.
    /// Hand-written, not `collectd::part`'s writers, so a bug there can't shape the fixture.
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
    number(&mut out, 0x0101, 2); // Severity: WARNING
    string(&mut out, 0x0100, b"cpu usage notification"); // Message
    out
}

/// Runs `CollectdDecoder::decode_into` over `bytes`, returning whether it failed. A fresh decoder
/// per call keeps one mutation's sticky state out of the next.
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
    // u32::MAX bytes is ~4 GiB; under 1 MiB means the cap was checked before any allocation.
    assert!(peak < 1024 * 1024, "peak live bytes {peak} suggests the length field was trusted");
}

// -- native::decode_batch -----------------------------------------------------------------------

#[test]
fn decode_batch_survives_every_single_byte_truncation() {
    let payload = native::encode_batch(&sample_batch());
    assert_every_truncation_fails_cleanly(&payload, |bytes| {
        native::decode_batch(bytes, &DecodeBudget::default()).is_err()
    });
}

#[test]
fn decode_batch_survives_seeded_bit_flips() {
    let payload = native::encode_batch(&sample_batch());
    assert_bit_flips_never_panic(&payload, 5000, |bytes| {
        native::decode_batch(bytes, &DecodeBudget::default()).is_ok()
    });
}

#[test]
fn decode_batch_rejects_a_dictionary_count_inflated_far_past_the_sanity_cap() {
    // The dictionary entry count is the payload's first varint; a huge one with nothing behind it.
    let mut out = BytesMut::new();
    write_uvarint(&mut out, u32::MAX as u64);
    let mut bytes = out.freeze();
    assert!(native::decode_batch(&mut bytes, &DecodeBudget::default()).is_err());
}

#[test]
fn decode_batch_never_allocates_proportionally_to_a_hostile_dictionary_count() {
    let mut out = BytesMut::new();
    write_uvarint(&mut out, u32::MAX as u64);
    let mut bytes = out.freeze();
    let peak = peak_live_bytes(|| {
        let _ = native::decode_batch(&mut bytes, &DecodeBudget::default());
    });
    assert!(peak < 1024 * 1024, "peak live bytes {peak} suggests the dict count was trusted");
}

#[test]
fn decode_batch_rejects_value_nesting_past_the_depth_cap() {
    // One past `native::value`'s `MAX_VALUE_DEPTH` (128), through the real encoder.
    let batch = deeply_nested_batch(129);
    let mut payload = native::encode_batch(&batch);
    assert!(native::decode_batch(&mut payload, &DecodeBudget::default()).is_err());
}

#[test]
fn decode_batch_at_exactly_the_depth_cap_still_decodes() {
    let batch = deeply_nested_batch(127);
    let mut payload = native::encode_batch(&batch);
    assert!(native::decode_batch(&mut payload, &DecodeBudget::default()).is_ok());
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
        native::decode_batch_v2(bytes, &DecodeBudget::default()).is_err()
    });
}

#[test]
fn decode_batch_v2_survives_seeded_bit_flips() {
    let payload = native::encode_batch_v2(&sample_batch(), sample_provenance());
    assert_bit_flips_never_panic(&payload, 5000, |bytes| {
        native::decode_batch_v2(bytes, &DecodeBudget::default()).is_ok()
    });
}

/// `decode_batch_v2` rejects a plain v1 payload rather than decoding it as "no provenance".
#[test]
fn decode_batch_v2_rejects_a_plain_v1_payload() {
    let mut payload = native::encode_batch(&sample_batch());
    assert!(native::decode_batch_v2(&mut payload, &DecodeBudget::default()).is_err());
}

#[test]
fn native_decoder_decode_into_never_panics_on_a_truncated_framed_batch() {
    // The whole `Decoder` entry point: `read_frame` and `decode_batch` composed.
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

/// The **weaker** truncation helper: a collectd datagram is self-delimiting parts with no
/// checksum or total length, so a prefix ending on a part boundary is a shorter valid datagram.
/// A prefix ending mid-list is `Ok` too: the intact lists are kept and the rest abandoned
/// (`collectd/decode.rs`'s per-datagram isolation rule).
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

/// A Values part declaring 65535 data sources in 20 bytes: the attacker-chosen count must be
/// checked against the part's length before anything is sized from it.
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
    // 65535 data sources is ~590 KB; a few KB means the count was checked first.
    assert!(peak < 4096, "peak live bytes {peak} suggests the values count was trusted");
}

// -- graphite -----------------------------------------------------------------------------------

/// A realistic carbon plaintext datagram: tagged and untagged lines, a `\r\n` ending, and a
/// trailing blank line, so a mutation can land in a path, tag, value, timestamp, or line boundary.
const GRAPHITE_PLAINTEXT: &[u8] = b"robustness.host.cpu;env=prod;dc=iad 0.5 1700000000\nrobustness.host.mem 2 1700000001\r\nrobustness.host.disk;mount=_root -1.25 1700000002\n\n";

/// `p = 'robustness.host.cpu;env=prod'; pickle.dumps([(p, (1700000000, 0.5)),
/// ('robustness.host.mem', (1700000001, 2**31 + 5)), (p, (1700000002, -1.25))], protocol=2)`,
/// produced by CPython and hexdumped, so the sweep runs over bytes a real sender emits:
/// `BINUNICODE`, `BINPUT`, `BINGET`, `BININT`, `LONG1`, `BINFLOAT`, `TUPLE2`, `MARK`/`APPENDS`.
const GRAPHITE_PICKLE: &[u8] = &[
    0x80, 0x02, 0x5d, 0x71, 0x00, 0x28, 0x58, 0x1c, 0x00, 0x00, 0x00, 0x72, 0x6f, 0x62, 0x75, 0x73,
    0x74, 0x6e, 0x65, 0x73, 0x73, 0x2e, 0x68, 0x6f, 0x73, 0x74, 0x2e, 0x63, 0x70, 0x75, 0x3b, 0x65,
    0x6e, 0x76, 0x3d, 0x70, 0x72, 0x6f, 0x64, 0x71, 0x01, 0x4a, 0x00, 0xf1, 0x53, 0x65, 0x47, 0x3f,
    0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x71, 0x02, 0x86, 0x71, 0x03, 0x58, 0x13, 0x00,
    0x00, 0x00, 0x72, 0x6f, 0x62, 0x75, 0x73, 0x74, 0x6e, 0x65, 0x73, 0x73, 0x2e, 0x68, 0x6f, 0x73,
    0x74, 0x2e, 0x6d, 0x65, 0x6d, 0x71, 0x04, 0x4a, 0x01, 0xf1, 0x53, 0x65, 0x8a, 0x05, 0x05, 0x00,
    0x00, 0x80, 0x00, 0x86, 0x71, 0x05, 0x86, 0x71, 0x06, 0x68, 0x01, 0x4a, 0x02, 0xf1, 0x53, 0x65,
    0x47, 0xbf, 0xf4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x71, 0x07, 0x86, 0x71, 0x08, 0x65,
    0x2e,
];

/// Runs `GraphiteDecoder::decode_into` over `bytes` with a fresh decoder, returning whether it
/// failed.
fn decode_graphite(bytes: &Bytes, protocol: Protocol) -> bool {
    let mut decoder = GraphiteDecoder::new(Arc::new(Resource::default())).with_protocol(protocol);
    let mut events = Vec::new();
    decoder.decode_into(bytes.clone(), 0, &mut events).is_err()
}

/// The **weaker** helper: plaintext lines are self-delimiting, so a prefix ending on a line
/// boundary is a shorter valid datagram, and one ending mid-line costs that line alone.
#[test]
fn graphite_plaintext_survives_every_single_byte_truncation() {
    assert_every_truncation_never_panics(GRAPHITE_PLAINTEXT, |bytes| {
        decode_graphite(bytes, Protocol::Plaintext)
    });
}

#[test]
fn graphite_plaintext_survives_seeded_bit_flips() {
    assert_bit_flips_never_panic(GRAPHITE_PLAINTEXT, 4000, |bytes| {
        decode_graphite(bytes, Protocol::Plaintext)
    });
}

/// The **stronger** helper: a pickle is complete only at its final `STOP` opcode.
#[test]
fn graphite_pickle_fails_cleanly_on_every_single_byte_truncation() {
    assert_every_truncation_fails_cleanly(GRAPHITE_PICKLE, |bytes| {
        decode_graphite(bytes, Protocol::Pickle)
    });
}

#[test]
fn graphite_pickle_survives_seeded_bit_flips() {
    assert_bit_flips_never_panic(GRAPHITE_PICKLE, 6000, |bytes| {
        decode_graphite(bytes, Protocol::Pickle)
    });
}

/// A `BINUNICODE` declaring `u32::MAX` bytes over a 5-byte payload must be checked against the
/// remaining input before anything is sized from it. The reader stores strings as ranges, so the
/// peak is only its own small `Vec`s.
#[test]
fn graphite_pickle_rejects_a_string_length_inflated_far_past_what_the_input_holds() {
    let mut hostile = vec![0x80, 0x02, 0x58];
    hostile.extend_from_slice(&u32::MAX.to_le_bytes());
    hostile.extend_from_slice(b"short");
    hostile.push(0x2e);
    let bytes = Bytes::from(hostile);
    assert!(decode_graphite(&bytes, Protocol::Pickle), "an impossible length must be rejected");

    let peak = peak_live_bytes(|| {
        let _ = decode_graphite(&bytes, Protocol::Pickle);
    });
    assert!(peak < 4096, "peak live bytes {peak} suggests the declared length was trusted");
}

/// The same for protocol 4's eight-byte length opcodes (`BINUNICODE8`, `BINBYTES8`).
#[test]
fn graphite_pickle_rejects_a_64_bit_string_length() {
    for opcode in [0x8du8, 0x8e] {
        let mut hostile = vec![0x80, 0x05, opcode];
        hostile.extend_from_slice(&u64::MAX.to_le_bytes());
        hostile.extend_from_slice(b"short");
        hostile.push(0x2e);
        let bytes = Bytes::from(hostile);
        assert!(decode_graphite(&bytes, Protocol::Pickle), "opcode {opcode:#04x}");

        let peak = peak_live_bytes(|| {
            let _ = decode_graphite(&bytes, Protocol::Pickle);
        });
        assert!(peak < 4096, "opcode {opcode:#04x}: peak live bytes {peak}");
    }
}

/// The depth cap fires at its documented edge: `MAX_PICKLE_DEPTH` open marks fail only for
/// leaving values on the stack, one more fails as a depth violation. A 100k-deep payload must not
/// recurse; the reader loops over a `Vec` stack.
#[test]
fn graphite_pickle_rejects_nesting_past_the_depth_cap() {
    let depth = logit_proto::graphite::MAX_PICKLE_DEPTH;
    for marks in [depth, depth + 1, 100_000] {
        let mut hostile = vec![0x80u8, 0x02];
        hostile.extend(std::iter::repeat_n(0x28u8, marks));
        hostile.push(0x2e);
        let bytes = Bytes::from(hostile);
        assert!(decode_graphite(&bytes, Protocol::Pickle), "{marks} marks must be rejected");
    }
}

/// A `LONG4` declaring a 2 GB magnitude, and an inflated `FRAME` length, must not be trusted.
#[test]
fn graphite_pickle_rejects_inflated_long_and_frame_lengths() {
    let mut long4 = vec![0x80u8, 0x02, 0x8b];
    long4.extend_from_slice(&0x7fff_ffffu32.to_le_bytes());
    long4.push(0x2e);
    assert!(decode_graphite(&Bytes::from(long4), Protocol::Pickle));

    let mut frame = vec![0x80u8, 0x05, 0x95];
    frame.extend_from_slice(&u64::MAX.to_le_bytes());
    frame.push(0x2e);
    assert!(decode_graphite(&Bytes::from(frame), Protocol::Pickle));
}

/// `PROTO 2, EMPTY_LIST, LONG_BINPUT 499999, STOP` (9 bytes): a memo key with nothing behind it
/// must be rejected, not grow the memo to 500,000 slots (~8 MB).
#[test]
fn graphite_pickle_never_allocates_from_a_hostile_memo_key() {
    let hostile = vec![0x80, 0x02, 0x5d, 0x72, 0x1f, 0xa1, 0x07, 0x00, 0x2e];
    let bytes = Bytes::from(hostile);
    assert!(
        decode_graphite(&bytes, Protocol::Pickle),
        "a memo key skipping ahead must be rejected"
    );

    let peak = peak_live_bytes(|| {
        let _ = decode_graphite(&bytes, Protocol::Pickle);
    });
    assert!(peak < 4096, "peak live bytes {peak} suggests the memo key sized the memo");
}

/// Every opcode byte off the allowlist is refused, checked exhaustively.
#[test]
fn graphite_pickle_rejects_every_opcode_outside_the_allowlist() {
    const PERMITTED: &[u8] = &[
        0x28, 0x29, 0x2e, 0x42, 0x43, 0x47, 0x4a, 0x4b, 0x4d, 0x4e, 0x54, 0x55, 0x58, 0x5d, 0x61,
        0x65, 0x68, 0x6a, 0x6c, 0x71, 0x72, 0x74, 0x80, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8a, 0x8b,
        0x8c, 0x8d, 0x8e, 0x94, 0x95,
    ];
    for opcode in 0u8..=255 {
        if PERMITTED.contains(&opcode) {
            continue;
        }
        // Trailing operand bytes, so a refusal is the allowlist, not a short read.
        let mut hostile = vec![0x80u8, 0x02, opcode];
        hostile.extend_from_slice(&[0u8; 16]);
        hostile.push(0x2e);
        assert!(
            decode_graphite(&Bytes::from(hostile), Protocol::Pickle),
            "opcode {opcode:#04x} must not be accepted"
        );
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
    // A Reject whose message field overstates its length: `control::read_field`'s check, not
    // the 1 KiB message cap.
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

// -- prometheus::compression::decompress_bounded -----------------------------------------------

/// `prometheus_in`'s `MAX_REQUEST_BYTES`.
const REMOTE_WRITE_CAP: usize = 4 * 1024 * 1024;

/// A remote-write-sized body with some structure, so the compressed form has real blocks.
fn remote_write_like_body() -> Vec<u8> {
    (0..2000u32)
        .flat_map(|i| format!("series_{}{{job=\"api\"}} {i}\n", i % 37).into_bytes())
        .collect()
}

fn decompress_fails(encoding: Encoding, bytes: &Bytes) -> bool {
    compression::decompress_bounded(encoding, bytes, REMOTE_WRITE_CAP).is_err()
}

/// A zstd frame header with an 8-byte content size and a window descriptor, no checksum.
fn zstd_header_declaring(content_size: u64, window: u8) -> Vec<u8> {
    let mut header = vec![0x28, 0xb5, 0x2f, 0xfd, 0xc0, window];
    header.extend(content_size.to_le_bytes());
    header
}

#[test]
fn zstd_decompression_survives_every_single_byte_truncation() {
    let valid = compression::compress(Encoding::Zstd, &remote_write_like_body()).unwrap();
    assert_every_truncation_fails_cleanly(&valid, |bytes| decompress_fails(Encoding::Zstd, bytes));
}

#[test]
fn snappy_decompression_survives_every_single_byte_truncation() {
    let valid = compression::compress(Encoding::Snappy, &remote_write_like_body()).unwrap();
    assert_every_truncation_never_panics(&valid, |bytes| decompress_fails(Encoding::Snappy, bytes));
}

#[test]
fn both_decompressors_survive_seeded_bit_flips() {
    for encoding in [Encoding::Zstd, Encoding::Snappy] {
        let valid = compression::compress(encoding, &remote_write_like_body()).unwrap();
        assert_bit_flips_never_panic(&valid, 3000, |bytes| !decompress_fails(encoding, bytes));
    }
}

/// A content size of `u64::MAX` is refused on the header, allocating nothing like it.
#[test]
fn zstd_never_allocates_from_a_hostile_declared_content_size() {
    let bad = zstd_header_declaring(u64::MAX, 0x50);
    let mut result = None;
    let peak = peak_live_bytes(|| {
        result = Some(compression::decompress_bounded(Encoding::Zstd, &bad, REMOTE_WRITE_CAP));
    });
    assert!(
        matches!(result, Some(Err(DecompressError::TooLarge { declared: Some(_), .. }))),
        "{result:?}"
    );
    assert!(peak < 1024 * 1024, "peak live bytes {peak} suggests the content size was trusted");
}

/// A 2 TiB window, the largest the spec allows but one, is refused before it is allocated.
#[test]
fn zstd_never_allocates_a_hostile_window() {
    let mut bad = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0xf8];
    bad.extend([0u8; 16]);
    let mut result = None;
    let peak = peak_live_bytes(|| {
        result = Some(compression::decompress_bounded(Encoding::Zstd, &bad, REMOTE_WRITE_CAP));
    });
    assert!(
        matches!(result, Some(Err(DecompressError::TooLarge { declared: None, .. }))),
        "{result:?}"
    );
    assert!(peak < 1024 * 1024, "peak live bytes {peak} suggests the window was allocated");
}

/// A frame with no declared size that would inflate to 128 MiB: the output stops just past the
/// cap, so peak memory is a small multiple of the cap, not of what the frame describes.
#[test]
fn zstd_memory_is_bounded_by_the_cap_not_by_what_an_undeclared_frame_inflates_to() {
    let block: u32 = 128 * 1024;
    let blocks = 1024;
    let mut bomb = vec![0x28, 0xb5, 0x2f, 0xfd, 0x00, 0x50];
    for i in 0..blocks {
        let header = (block << 3) | (1 << 1) | u32::from(i + 1 == blocks);
        bomb.extend(&header.to_le_bytes()[..3]);
        bomb.push(0x41);
    }
    let mut result = None;
    let peak = peak_live_bytes(|| {
        result = Some(compression::decompress_bounded(Encoding::Zstd, &bomb, REMOTE_WRITE_CAP));
    });
    assert!(
        matches!(result, Some(Err(DecompressError::TooLarge { declared: None, .. }))),
        "{result:?}"
    );
    let bound = 4 * REMOTE_WRITE_CAP as i64;
    assert!(peak < bound, "peak live bytes {peak} over {bound} for a {}-byte bomb", bomb.len());
}

// -- native decode: canonical varints, trailing bytes, the decode budget -----------------------
//
// Hand-built payloads, so a test can place bytes the encoder never writes. The builders mirror
// `docs/design/wire-protocol.md`'s "Batch grammar" and "Record layout"; the tag numbers are the
// `record.rs`/`value.rs` constants.

/// Appends `v` as an unsigned LEB128 varint.
fn uv(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            out.push(byte | 0x80);
        } else {
            out.push(byte);
            break;
        }
    }
}

/// One `tag + uvarint(len) + body` TLV field (or `Value`).
fn tlv(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = vec![tag];
    uv(&mut out, body.len() as u64);
    out.extend_from_slice(body);
    out
}

/// A counted list of length-prefixed entries.
fn counted_list(items: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    uv(&mut out, items.len() as u64);
    for item in items {
        uv(&mut out, item.len() as u64);
        out.extend_from_slice(item);
    }
    out
}

/// A v1 payload: `dict`, an empty resource, no scope, then `events` as a counted list.
fn wire_batch(dict: &[&str], events: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    uv(&mut out, dict.len() as u64);
    for s in dict {
        uv(&mut out, s.len() as u64);
        out.extend_from_slice(s.as_bytes());
    }
    uv(&mut out, 0); // resource section length
    out.push(0); // scope absent
    out.extend_from_slice(&counted_list(events));
    out
}

/// A metric record named `dict[0]`, with `extra` fields ahead of its `MR_KIND` field (6).
fn wire_metric_record(kind: &[u8], extra: &[u8]) -> Vec<u8> {
    let mut out = tlv(1, &0u32.to_le_bytes()); // MR_NAME: dictionary index 0
    out.extend_from_slice(extra);
    out.extend_from_slice(&tlv(6, kind));
    out
}

/// An event whose `FIELD_METRICS` (4) list holds one record.
fn wire_metric_event(record: Vec<u8>) -> Vec<u8> {
    tlv(4, &counted_list(&[record]))
}

/// A one-event, one-metric batch whose metric kind is `kind_tag` with `body`.
fn wire_kind_batch(kind_tag: u8, body: &[u8]) -> Vec<u8> {
    wire_batch(&["m"], &[wire_metric_event(wire_metric_record(&tlv(kind_tag, body), &[]))])
}

/// An event whose span (`FIELD_SPAN`, 5) has the required trace id, span id, and a `Null` name,
/// plus `extra` fields.
fn wire_span_event_field(extra: &[u8]) -> Vec<u8> {
    let mut span = tlv(1, &[0u8; 16]); // SR_TRACE_ID
    span.extend_from_slice(&tlv(2, &[0u8; 8])); // SR_SPAN_ID
    span.extend_from_slice(&tlv(4, &tlv(0, &[]))); // SR_NAME: Value::Null
    span.extend_from_slice(extra);
    tlv(5, &span)
}

/// An event whose log (`FIELD_LOG`, 3) has a `Null` message plus `extra` fields.
fn wire_log_event_field(extra: &[u8]) -> Vec<u8> {
    let mut log = tlv(1, &tlv(0, &[])); // LR_MESSAGE: Value::Null
    log.extend_from_slice(extra);
    tlv(3, &log)
}

/// An event whose `FIELD_ATTRIBUTES` (2) map holds one entry: key `dict[0]`, then `value`.
fn wire_attr_event(value: Vec<u8>) -> Vec<u8> {
    let mut map = vec![1u8, 0]; // count 1, key index 0
    map.extend_from_slice(&value);
    tlv(2, &map)
}

fn decode_v1(payload: &[u8]) -> Result<EventBatch, CodecError> {
    native::decode_batch(&mut Bytes::copy_from_slice(payload), &DecodeBudget::default())
}

/// Asserts `result` is a [`CodecError::BudgetExceeded`] for `limit`.
fn assert_over_budget<T>(result: Result<T, CodecError>, limit: u64, case: &str) {
    match result {
        Err(CodecError::BudgetExceeded { limit: got }) => assert_eq!(got, limit, "{case}"),
        Err(other) => panic!("{case}: expected BudgetExceeded {{ limit: {limit} }}, got {other:?}"),
        Ok(_) => panic!("{case}: decoded, expected BudgetExceeded {{ limit: {limit} }}"),
    }
}

fn assert_malformed<T>(result: Result<T, CodecError>, needle: &str, case: &str) {
    match result {
        Err(CodecError::Malformed(msg)) => {
            assert!(msg.contains(needle), "{case}: Malformed({msg:?}) does not name {needle:?}")
        }
        Err(other) => panic!("{case}: expected Malformed naming {needle:?}, got {other:?}"),
        Ok(_) => panic!("{case}: decoded, expected Malformed naming {needle:?}"),
    }
}

/// A 10th varint byte carries bit 63 alone, so any bit above its lowest overflows a `u64`. The
/// over-long encoding of a small value (`80 00` for 0) is still accepted: rejecting it costs a
/// compare per byte, and no writer emits one.
#[test]
fn a_ten_byte_varint_with_bits_above_the_low_bit_is_malformed() {
    let overflowing: [&[u8]; 3] = [
        &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f],
        &[0x81, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02],
        &[0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x03],
    ];
    for bytes in overflowing {
        let result = read_uvarint(&mut Bytes::copy_from_slice(bytes));
        assert_malformed(result, "overflows", &format!("{bytes:02x?}"));
    }
    let mut max = Bytes::from_static(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]);
    assert_eq!(read_uvarint(&mut max).unwrap(), u64::MAX);
    let mut over_long = Bytes::from_static(&[0x80, 0x00]);
    assert_eq!(read_uvarint(&mut over_long).unwrap(), 0);
    assert!(over_long.is_empty());
}

/// Every sequential kind body is parsed field by field, so junk after its last field is
/// `Malformed`. `Distribution`'s blob goes whole to `DdSketch::from_bytes`, which checks its own
/// end. `Set`'s goes whole to `HyperLogLog::from_bytes`, whose end check belongs to
/// `logit-core`'s `HllBytesReader`, so it isn't asserted here.
#[test]
fn every_metric_kind_rejects_trailing_bytes_in_its_body() {
    let f64_bytes = 1.5f64.to_le_bytes();
    let mut sum = f64_bytes.to_vec();
    sum.extend_from_slice(&[0, 1]); // temporality Delta, monotonic
    let mut samples = vec![1u8]; // one sample
    samples.extend_from_slice(&f64_bytes);
    samples.extend_from_slice(&1.0f64.to_le_bytes()); // sample_rate
    let set_members = vec![1u8, 1, b'a']; // one one-byte member
    let mut histogram = vec![1u8]; // one bucket
    histogram.extend_from_slice(&f64_bytes);
    histogram.extend_from_slice(&[3, 0, 0, 0, 0]); // count 3, Delta, sum/min/max absent
    let mut exponential = vec![0u8, 0]; // scale 0, zero_count 0
    exponential.extend_from_slice(&0f64.to_le_bytes()); // zero_threshold
    exponential.extend_from_slice(&[0, 1, 4]); // positive: offset 0, one bucket of 4
    exponential.extend_from_slice(&[0, 0]); // negative: offset 0, no buckets
    exponential.extend_from_slice(&[0, 4, 0, 0, 0]); // Delta, count 4, sum/min/max absent
    let mut summary = vec![1u8]; // one quantile
    summary.extend_from_slice(&0.5f64.to_le_bytes());
    summary.extend_from_slice(&f64_bytes);
    summary.push(2); // count
    summary.extend_from_slice(&f64_bytes); // sum
    let mut sketch = logit_core::DdSketch::new();
    sketch.add(1.5);

    let kinds: [(&str, u8, Vec<u8>); 9] = [
        ("Sum", 0, sum),
        ("Gauge", 1, f64_bytes.to_vec()),
        ("GaugeDelta", 2, f64_bytes.to_vec()),
        ("Samples", 3, samples),
        ("Distribution", 4, sketch.to_bytes()),
        ("SetMembers", 5, set_members),
        ("Histogram", 7, histogram),
        ("ExponentialHistogram", 8, exponential),
        ("Summary", 9, summary),
    ];
    for (name, tag, body) in kinds {
        assert!(decode_v1(&wire_kind_batch(tag, &body)).is_ok(), "{name}: the valid body failed");
        let mut padded = body.clone();
        padded.extend_from_slice(&[0xde, 0xad]);
        let needle = if tag == 4 { "distribution" } else { "trailing bytes" };
        assert_malformed(decode_v1(&wire_kind_batch(tag, &padded)), needle, name);
    }
}

/// A field whose reader consumes a prefix of it (a varint, one `Value`, an attribute map, a list,
/// a trace reference) rejects bytes after that prefix, as does a scalar `Value` payload.
#[test]
fn a_record_field_with_trailing_bytes_is_malformed() {
    const JUNK: [u8; 2] = [0xde, 0xad];
    let with_junk = |body: &[u8]| {
        let mut out = body.to_vec();
        out.extend_from_slice(&JUNK);
        out
    };
    let null = tlv(0, &[]);
    let mut trace = vec![0u8; 16];
    trace.extend_from_slice(&[0, 0]); // no span id, flags 0
    let gauge = tlv(1, &[0; 8]);

    let cases: Vec<(&str, Vec<u8>)> = vec![
        ("FIELD_TIMESTAMP", tlv(1, &with_junk(&[0x02]))),
        ("FIELD_ATTRIBUTES", tlv(2, &with_junk(&[0]))),
        ("FIELD_METRICS", tlv(4, &with_junk(&counted_list(&[])))),
        ("LR_MESSAGE", tlv(3, &tlv(1, &with_junk(&null)))),
        ("LR_SEVERITY", wire_log_event_field(&tlv(2, &with_junk(&[2])))),
        ("LR_BODY_FORMAT", wire_log_event_field(&tlv(3, &with_junk(&[1])))),
        ("LR_TRACE", wire_log_event_field(&tlv(4, &with_junk(&trace)))),
        ("SR_NAME", {
            let mut span = tlv(1, &[0u8; 16]);
            span.extend_from_slice(&tlv(2, &[0u8; 8]));
            span.extend_from_slice(&tlv(4, &with_junk(&null)));
            tlv(5, &span)
        }),
        ("SR_KIND", wire_span_event_field(&tlv(5, &with_junk(&[1])))),
        ("SR_STATUS", wire_span_event_field(&tlv(6, &with_junk(&[1])))),
        ("SR_EVENTS", wire_span_event_field(&tlv(7, &with_junk(&counted_list(&[]))))),
        ("SE_NAME", wire_span_event_field(&tlv(7, &counted_list(&[tlv(2, &with_junk(&null))])))),
        ("SE_ATTRIBUTES", {
            let mut event = tlv(2, &null);
            event.extend_from_slice(&tlv(3, &with_junk(&[0])));
            wire_span_event_field(&tlv(7, &counted_list(&[event])))
        }),
        ("MR_EXEMPLARS", {
            let field = tlv(5, &with_junk(&counted_list(&[Vec::new()])));
            wire_metric_event(wire_metric_record(&gauge, &field))
        }),
        ("EX_TRACE", {
            let field = tlv(5, &counted_list(&[tlv(3, &with_junk(&trace))]));
            wire_metric_event(wire_metric_record(&gauge, &field))
        }),
        ("MR_KIND", {
            let mut record = tlv(1, &0u32.to_le_bytes());
            record.extend_from_slice(&tlv(6, &with_junk(&gauge)));
            wire_metric_event(record)
        }),
        ("TAG_NULL", wire_attr_event(tlv(0, &JUNK))),
        ("TAG_BOOL", wire_attr_event(tlv(1, &with_junk(&[1])))),
        ("TAG_I64", wire_attr_event(tlv(2, &with_junk(&[0x02])))),
        ("TAG_U64", wire_attr_event(tlv(3, &with_junk(&[0x02])))),
        ("TAG_TIMESTAMP", wire_attr_event(tlv(7, &with_junk(&[0x02])))),
        ("TAG_ARRAY", wire_attr_event(tlv(8, &with_junk(&[0])))),
        ("TAG_MAP", wire_attr_event(tlv(9, &with_junk(&[0])))),
    ];
    for (name, event) in cases {
        assert_malformed(decode_v1(&wire_batch(&["k"], &[event])), "trailing bytes", name);
    }
}

/// Bytes after the last event (v1) or after the provenance trailer (v2) are `Malformed`: a
/// payload is one batch.
#[test]
fn a_batch_with_bytes_after_its_last_event_is_malformed() {
    let mut v1 = native::encode_batch(&sample_batch()).to_vec();
    v1.extend_from_slice(b"junk");
    assert_malformed(decode_v1(&v1), "trailing bytes", "v1");

    let mut v2 = native::encode_batch_v2(&sample_batch(), sample_provenance()).to_vec();
    v2.extend_from_slice(b"junk");
    assert_malformed(
        native::decode_batch_v2(&mut Bytes::from(v2), &DecodeBudget::default()),
        "trailing bytes",
        "v2",
    );
}

/// The native codec is a fixed point: re-encoding what it decoded reproduces the payload byte
/// for byte, in both codec versions.
#[test]
fn encode_then_decode_then_encode_is_byte_identical() {
    let v1 = native::encode_batch(&sample_batch());
    let decoded = decode_v1(&v1).unwrap();
    assert_eq!(native::encode_batch(&decoded), v1);

    let v2 = native::encode_batch_v2(&sample_batch(), sample_provenance());
    let (decoded, provenance) =
        native::decode_batch_v2(&mut v2.clone(), &DecodeBudget::default()).unwrap();
    assert_eq!(native::encode_batch_v2(&decoded, provenance), v2);
}

/// `write_frame` refuses what `read_frame` would refuse, so no writer can emit a frame over the
/// uncompressed cap.
#[test]
fn write_frame_refuses_a_payload_over_the_uncompressed_cap() {
    let over = vec![0u8; frame::MAX_SANE_UNCOMPRESSED_LEN as usize + 1];
    for compression in [Compression::None, Compression::Lz4] {
        let result = frame::write_frame(1, compression, &over);
        assert_malformed(result, "uncompressed cap", &format!("{compression:?}"));
    }
}

/// `NativeDecoder::decode_into` moves the decoded events into the caller's empty `Vec`, so each
/// event is held once at peak, not once in the batch and again in `out`.
#[test]
fn decode_into_holds_each_event_once_at_peak() {
    let n = 16 * 1024;
    let payload = wire_batch(&[], &vec![Vec::new(); n]);
    let framed = frame::write_frame(native::CODEC_NATIVE_V1, Compression::None, &payload).unwrap();
    let mut events = Vec::new();
    let peak = peak_live_bytes(|| {
        NativeDecoder.decode_into(framed.clone(), 0, &mut events).unwrap();
    });
    assert_eq!(events.len(), n);
    let once = (n * std::mem::size_of::<Event>()) as i64;
    assert!(
        peak < once + once / 8,
        "peak live bytes {peak} for {n} events of {} bytes: each event held more than once",
        std::mem::size_of::<Event>()
    );
}

/// One empty event is 1 wire byte and a `size_of::<Event>()` slot, so a 4 KiB lz4 frame of a
/// million of them would decode into ~900 MB. `NativeDecoder`'s default budget (256 MiB) refuses
/// it before building any event. An explicit budget of the events' cost admits them; one byte less
/// refuses them.
#[test]
fn a_frame_of_empty_events_is_rejected_past_the_decode_budget() {
    let n = 1 << 20;
    let payload = wire_batch(&[], &vec![Vec::new(); n]);
    let framed = frame::write_frame(native::CODEC_NATIVE_V1, Compression::Lz4, &payload).unwrap();
    assert!(framed.len() < 8 * 1024, "the frame is {} bytes", framed.len());
    let mut result = None;
    let peak = peak_live_bytes(|| {
        let mut events = Vec::new();
        result = Some(NativeDecoder.decode_into(framed.clone(), 0, &mut events).map(|_| ()));
    });
    assert_over_budget(result.unwrap(), native::DEFAULT_DECODE_BUDGET, "a million empty events");
    // The 1 MiB decompressed payload is the only large allocation.
    assert!(peak < 2 * 1024 * 1024, "peak live bytes {peak}: events were built before refusal");

    let n = 1000;
    let payload = Bytes::from(wire_batch(&[], &vec![Vec::new(); n]));
    let cost = (n * std::mem::size_of::<Event>()) as u64;
    let exact = DecodeBudget::new(cost);
    assert_eq!(native::decode_batch(&mut payload.clone(), &exact).unwrap().events.len(), n);
    assert_eq!(exact.charged(), cost);
    let short = DecodeBudget::new(cost - 1);
    let result = native::decode_batch(&mut payload.clone(), &short);
    assert_over_budget(result, cost - 1, "one byte short");
}

/// One empty exemplar is 1 wire byte and a `size_of::<Exemplar>()` slot.
#[test]
fn a_frame_of_empty_exemplars_is_rejected_past_the_decode_budget() {
    let n = 1000;
    let exemplars = tlv(5, &counted_list(&vec![Vec::new(); n])); // MR_EXEMPLARS
    let payload = Bytes::from(wire_batch(
        &["m"],
        &[wire_metric_event(wire_metric_record(&tlv(1, &[0; 8]), &exemplars))],
    ));
    // The dictionary's one string and its `Symbol`, the event, the metric record, the exemplars.
    let cost = (1
        + std::mem::size_of::<logit_core::Symbol>()
        + std::mem::size_of::<Event>()
        + std::mem::size_of::<logit_core::MetricRecord>()
        + n * std::mem::size_of::<logit_core::Exemplar>()) as u64;
    let exact = DecodeBudget::new(cost);
    let batch = native::decode_batch(&mut payload.clone(), &exact).unwrap();
    assert_eq!(batch.events[0].metrics[0].exemplars.len(), n);
    assert_eq!(exact.charged(), cost);

    let short = DecodeBudget::new(cost - 1);
    let result = native::decode_batch(&mut payload.clone(), &short);
    assert_over_budget(result, cost - 1, "one byte short");
    let peak = peak_live_bytes(|| {
        let _ = native::decode_batch(&mut payload.clone(), &DecodeBudget::new(64 * 1024));
    });
    assert!(peak < 64 * 1024, "peak live bytes {peak}: exemplars were built before refusal");
}

/// An ordinary batch decodes well inside its budget, and the budget reports each charge: the
/// dictionary's strings and `Symbol`s, the event slot, and one attribute-map entry each on the
/// resource and the event. The log's `Str` message is a slice of the payload, charged nothing.
#[test]
fn a_batch_under_the_budget_decodes_and_the_budget_reports_what_it_charged() {
    let payload = native::encode_batch(&sample_batch());
    let budget = DecodeBudget::new(1024 * 1024);
    let decoded = native::decode_batch(&mut payload.clone(), &budget).unwrap();
    assert_eq!(decoded, sample_batch());

    let symbol = std::mem::size_of::<logit_core::Symbol>();
    let dictionary = "service.name".len() + symbol + "host".len() + symbol;
    let entry = std::mem::size_of::<(logit_core::Symbol, Value)>();
    let expected = dictionary + std::mem::size_of::<Event>() + 2 * entry;
    assert_eq!(budget.charged(), expected as u64);
    assert_eq!(budget.limit(), 1024 * 1024);
}

/// Peak heap per wire byte for a payload made of one element repeated `n` times, each at its
/// smallest wire encoding. `docs/design/wire-protocol.md`'s "Decode amplification" table records
/// these ratios; this test keeps it true to within 5%. `n` is a power of two so a `Vec`'s
/// doubling lands on its exact capacity; between powers of two a list can hold up to twice its
/// length in capacity.
#[test]
fn peak_allocation_per_wire_byte_matches_the_documented_ratio() {
    const N: usize = 1 << 16;
    fn events(n: usize) -> Vec<u8> {
        wire_batch(&[], &vec![Vec::new(); n])
    }
    fn metric_records(n: usize) -> Vec<u8> {
        let record = wire_metric_record(&tlv(1, &[0; 8]), &[]);
        wire_batch(&["m"], &[tlv(4, &counted_list(&vec![record; n]))])
    }
    fn exemplars(n: usize) -> Vec<u8> {
        let field = tlv(5, &counted_list(&vec![Vec::new(); n]));
        wire_batch(&["m"], &[wire_metric_event(wire_metric_record(&tlv(1, &[0; 8]), &field))])
    }
    fn span_events(n: usize) -> Vec<u8> {
        let event = tlv(2, &tlv(0, &[])); // SE_NAME: Null
        wire_batch(&[], &[wire_span_event_field(&tlv(7, &counted_list(&vec![event; n])))])
    }
    fn span_links(n: usize) -> Vec<u8> {
        let mut link = tlv(1, &[0u8; 16]);
        link.extend_from_slice(&tlv(2, &[0u8; 8]));
        wire_batch(&[], &[wire_span_event_field(&tlv(8, &counted_list(&vec![link; n])))])
    }
    fn array_items(n: usize) -> Vec<u8> {
        let mut items = Vec::new();
        uv(&mut items, n as u64);
        items.extend(std::iter::repeat_n([0u8, 0], n).flatten()); // Null
        wire_batch(&["k"], &[wire_attr_event(tlv(8, &items))])
    }
    fn map_values(n: usize) -> Vec<u8> {
        let mut items = Vec::new();
        uv(&mut items, n as u64);
        items.extend(std::iter::repeat_n([9u8, 1, 0], n).flatten()); // an empty Map
        wire_batch(&["k"], &[wire_attr_event(tlv(8, &items))])
    }
    fn kind(tag: u8, n: usize, element: &[u8], tail: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        uv(&mut body, n as u64);
        body.extend(std::iter::repeat_n(element, n).flatten());
        body.extend_from_slice(tail);
        wire_kind_batch(tag, &body)
    }
    fn set_members(n: usize) -> Vec<u8> {
        kind(5, n, &[0], &[])
    }
    fn samples(n: usize) -> Vec<u8> {
        kind(3, n, &[0; 8], &[0; 8])
    }
    fn histogram(n: usize) -> Vec<u8> {
        kind(7, n, &[0; 9], &[0, 0, 0, 0])
    }
    fn summary(n: usize) -> Vec<u8> {
        kind(9, n, &[0; 16], &[0; 9])
    }
    fn exponential_buckets(n: usize) -> Vec<u8> {
        let mut body = vec![0u8, 0];
        body.extend_from_slice(&[0; 8]);
        body.push(0);
        uv(&mut body, n as u64);
        body.extend(std::iter::repeat_n(0u8, n));
        body.extend_from_slice(&[0, 0, 0, 0, 0, 0, 0]);
        wire_kind_batch(8, &body)
    }

    let arms: [Arm; 13] = [
        ("event", events, RATIO_EVENT),
        ("MR_EXEMPLARS entry", exemplars, RATIO_EXEMPLAR),
        ("TAG_ARRAY of empty maps", map_values, RATIO_MAP_VALUE),
        ("SR_EVENTS entry", span_events, RATIO_SPAN_EVENT),
        ("SET_MEMBERS member", set_members, RATIO_SET_MEMBER),
        ("TAG_ARRAY item", array_items, RATIO_ARRAY_ITEM),
        ("SR_LINKS entry", span_links, RATIO_SPAN_LINK),
        ("FIELD_METRICS record", metric_records, RATIO_METRIC_RECORD),
        ("exponential bucket", exponential_buckets, RATIO_EXPONENTIAL_BUCKET),
        ("sample", samples, RATIO_SAMPLE),
        ("histogram bucket", histogram, RATIO_HISTOGRAM_BUCKET),
        ("summary quantile", summary, RATIO_SUMMARY_QUANTILE),
        ("event, at N / 2 + 1", |n| events(n / 2 + 1), RATIO_EVENT * 2.0),
    ];
    let mut failures = Vec::new();
    for (name, build, documented) in arms {
        let payload = Bytes::from(build(N));
        let wire = payload.len() as f64;
        let budget = DecodeBudget::unlimited();
        let peak = peak_live_bytes(|| {
            let batch = native::decode_batch(&mut payload.clone(), &budget);
            assert!(batch.is_ok(), "{name}: {:?}", batch.err());
        });
        let ratio = peak as f64 / wire;
        eprintln!(
            "{name:<26} wire {wire:>9} peak {peak:>11} ratio {ratio:>8.2} charged/wire {:>8.2}",
            budget.charged() as f64 / wire
        );
        if (ratio - documented).abs() > documented * 0.05 {
            failures.push(format!("{name}: measured {ratio:.2}, documented {documented}"));
        }
    }
    assert!(failures.is_empty(), "{failures:#?}");
}

/// One ratio-test arm: the element, a payload builder taking the element count, and the ratio
/// the doc table records.
type Arm = (&'static str, fn(usize) -> Vec<u8>, f64);

// `docs/design/wire-protocol.md`'s "Decode amplification" table, element by element.
const RATIO_EVENT: f64 = 864.0;
const RATIO_EXEMPLAR: f64 = 440.0;
const RATIO_MAP_VALUE: f64 = 144.0;
const RATIO_SPAN_EVENT: f64 = 89.6;
const RATIO_SET_MEMBER: f64 = 32.0;
const RATIO_ARRAY_ITEM: f64 = 20.0;
const RATIO_SPAN_LINK: f64 = 15.7;
const RATIO_METRIC_RECORD: f64 = 11.8;
const RATIO_EXPONENTIAL_BUCKET: f64 = 8.0;
const RATIO_SAMPLE: f64 = 2.0;
const RATIO_HISTOGRAM_BUCKET: f64 = 1.8;
const RATIO_SUMMARY_QUANTILE: f64 = 1.0;
