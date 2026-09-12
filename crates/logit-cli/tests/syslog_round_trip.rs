//! `syslog_out` -> `syslog_in` round trip, over real UDP sockets -- the syslog counterpart to
//! `otlp_round_trip.rs`/`logit_round_trip.rs`. Lives here (not a dev-dependency cycle between
//! `logit-inputs`/`logit-outputs`) for the same reason those do: `logit-cli` already depends on
//! both as ordinary dependencies.
//!
//! `docs/plans/lossless-transit.md`'s W5 workstream, "Tests" bullet.
//!
//! ## Fixture corpus (`tests/fixtures/syslog/`)
//!
//! One file pair per case: `<name>.in` (the raw line, exactly as a real sender would put it on
//! the wire) and `<name>.expected` (the exact bytes `syslog_out` must emit for that line's
//! decode, or the literal marker [`SAME_AS_INPUT`] when the sink's own canonicalization happens
//! to reproduce the input verbatim). The six `testdata/interop/syslog/*.raw` captures are read
//! from there directly at runtime, per that directory's own README ("`logit`'s own encoder never
//! touches these") -- they are not copied into this corpus.
//!
//! ## Permitted normalizations (recorded here, and in the ADR this test's PR adds)
//!
//! `syslog_out` is not byte-identical to its input in general -- these are the specific,
//! documented ways it differs, each with the reason it's permitted rather than a lossiness bug:
//!
//! 1. **RFC 5424 §6.4 BOM stripped, never re-emitted.** `syslog_in` strips a leading UTF-8 BOM
//!    from MSG on decode (it's a `MSG-UTF8` signal, not payload); `syslog_out` never writes one
//!    back (`docs/adr/syslog-output.md`'s "No RFC 5424 §6.4 BOM" note -- Loki's `| json` stage
//!    doesn't skip one). Exercised by `rfc5424-example1`/`rfc5424-example3`.
//! 2. **RFC 5424 TIMESTAMP fractional digits always render as exactly 6, zero-padded, offset
//!    always UTC (`Z`).** `push_rfc5424_timestamp` reuses `format_rfc3339_utc`'s fixed-width
//!    output; a `+HH:MM`/`-HH:MM` offset in the input is converted to the same instant in `Z`
//!    form, and a fraction shorter than 6 digits is zero-padded (`.003Z` -> `.003000Z`). The
//!    *instant* is unchanged -- only its textual rendering is. Exercised by `rfc5424-example1`
//!    through `-example4` and `interop-logger-rfc5424-basic`.
//! 3. **RFC 3164 output always carries a TIMESTAMP, even when the origin had none.** RFC 3164's
//!    TIMESTAMP is nominally mandatory but real senders sometimes omit it (`python-syslog-handler-*`
//!    below); `syslog_out`'s RFC 3164 header writer has no "omit" branch, so a relayed line always
//!    gets *some* TIMESTAMP, falling to `event.timestamp` (receipt time) when the origin carried
//!    none. Exercised structurally (not byte-for-byte) by the `python_syslog_handler_*` tests.
//! 4. **A 3164 -> 5424 relay's TIMESTAMP falls to receipt time, in RFC 3339.** RFC 3164's raw
//!    `Mmm dd hh:mm:ss` token has no year or timezone and can't be resolved to an instant without
//!    guessing (`syslog_in`'s own module doc); `syslog_out` declines to guess on the way out
//!    either, so a dialect-changing relay's TIMESTAMP is `event.timestamp`, not a reinterpretation
//!    of the original token. Exercised by `a_3164_to_5424_relay_falls_the_timestamp_to_receipt_time`.
//! 5. **A 3164 -> 5424 (or any) relay's STRUCTURED-DATA is always `-`.** RFC 3164 has no such
//!    field, so there is nothing to carry over. Exercised by the same test as (4).
//! 6. **Header hostname/app-name may come from `syslog_out`'s own configured defaults when the
//!    event carries no `syslog.hostname`/`syslog.tag` attribute at all** (`with_hostname`/
//!    `with_app_name`) -- a real relay config commonly sets these as a fallback identity for
//!    traffic that never passed through `syslog_in`. This corpus's round-trip encoders leave both
//!    unset specifically so the byte-for-byte assertions below aren't exercising this
//!    normalization by accident; it's recorded here because the plan's ADR calls it out
//!    explicitly, not because any test in this file depends on it.
//! 7. **SD-ELEMENT and SD-PARAM order is canonicalized by name, not preserved from the wire.**
//!    `write_structured_data`/`write_sd_element` (`crates/logit-outputs/src/syslog.rs`) sort
//!    SD-IDs and PARAM-NAMEs by name bytes before writing, since `AttrMap`/attribute iteration
//!    order is process-global intern order, not wire order -- a relay that saw `[b@2 ..][a@1
//!    ..]` re-emits `[a@1 ..][b@2 ..]`. A repeated PARAM-NAME's occurrences are emitted grouped
//!    (already grouped under one `Value::Array` by the decoder), so a wire `a b a` interleaving
//!    is not preserved -- see `docs/known-gaps.md`. Exercised by `rfc5424-example4` (two
//!    SD-ELEMENTs, one with three distinct PARAM-NAMEs) and
//!    `interop-logger-rfc5424-basic` (three PARAM-NAMEs).
//! 8. **A PARAM-VALUE's bare (non-escape) backslash is re-emitted in canonical escaped form.**
//!    RFC 5424 section 6.3.3 declares only `\"`, `\\`, `\]` as escapes; a backslash before any
//!    other byte is a literal backslash followed by that byte, which `syslog_in`'s
//!    `parse_param_value` keeps literally rather than rejecting. `syslog_out` then re-emits that
//!    literal backslash the canonical way (`\` -> `\\`), so `p="a\xb"` relays as `p="a\\xb"` --
//!    the same PARAM-VALUE, per the RFC's own equivalence, just spelled the canonical way.
//! 9. **A non-UTF-8 RFC 3164 HOSTNAME token is skipped, not relayed.** `parse_3164`
//!    (`crates/logit-inputs/src/syslog.rs`) never fails the whole line over a bad HOSTNAME (the
//!    version-sniff fallback in `parse_line` depends on that), so a HOSTNAME candidate that
//!    isn't valid UTF-8 is simply not stamped as a `syslog.hostname` attribute -- reported
//!    through a throttled `hostname_not_utf8` diagnostic -- rather than reaching the wire on the
//!    far end at all.

use bytes::Bytes;
use logit_core::{Event, EventBatch, Value};
use logit_inputs::syslog::{SyslogDecoder, SyslogInput};
use logit_outputs::syslog::{Format, SyslogEncoder, SyslogOutput};
use logit_pipeline::{Delivered, Fanout, Input, Output};
use logit_proto::Decoder;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// The `.expected` marker meaning "byte-identical to the `.in` file" -- see this file's module
/// doc.
const SAME_AS_INPUT: &[u8] = b"== SAME AS INPUT ==";

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/syslog")
}

fn testdata_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/syslog")
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()))
}

fn read_fixture(name: &str, ext: &str) -> Vec<u8> {
    read(&fixtures_dir().join(format!("{name}.{ext}")))
}

fn read_testdata(name: &str) -> Vec<u8> {
    read(&testdata_dir().join(name))
}

/// The bytes a case's sink output must equal: either the literal `.expected` file, or (when that
/// file is exactly [`SAME_AS_INPUT`]) the case's own `.in` bytes.
fn expected_bytes(name: &str, input: &[u8]) -> Vec<u8> {
    let expected = read_fixture(name, "expected");
    if expected.as_slice() == SAME_AS_INPUT {
        input.to_vec()
    } else {
        expected
    }
}

/// Zeroes every event's receipt-time `timestamp` -- the one field that legitimately differs
/// between two independent decodes of equivalent bytes (this process's wall clock at the moment
/// each decode ran), so a whole-`EventBatch` `assert_eq!` can otherwise be exact. The
/// `syslog.timestamp` *attribute* is never touched here -- it's origin data, not receipt time,
/// and every fixture in this corpus carries a `Value::Timestamp`/`Value::Null`/verbatim
/// `Value::Str` that round-trips to the identical value either way (see the module doc's
/// normalization list).
fn normalize_receipt_time(batch: &mut EventBatch) {
    for event in &mut batch.events {
        event.timestamp = 0;
    }
}

/// Real UDP harness: a plain "byte capture" socket (for the raw-datagram half of every
/// byte-for-byte assertion) alongside a real, bound [`SyslogInput`] draining into a [`Fanout`]
/// channel (for the decoded-`EventBatch` half) -- the plan's "do BOTH" requirement. Mirrors
/// `otlp_round_trip.rs`'s `bind()`-then-`local_addr()`-then-spawn pattern; no bind-drop race and
/// no sleep-based readiness guess.
struct Harness {
    capture: UdpSocket,
    capture_addr: SocketAddr,
    input_addr: SocketAddr,
    rx: mpsc::Receiver<Delivered>,
}

impl Harness {
    async fn new() -> Self {
        let capture = UdpSocket::bind("127.0.0.1:0").await.expect("binding the capture socket");
        let capture_addr = capture.local_addr().expect("capture socket should have a local addr");

        let mut input = SyslogInput::new("127.0.0.1:0");
        input.bind().await.expect("binding the syslog_in listener");
        let input_addr = input.local_addr().expect("bind() should leave a real address behind");

        let (tx, rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });

        Self { capture, capture_addr, input_addr, rx }
    }

    /// Sends `batch` through a fresh [`SyslogOutput`] built from `encoder()` -- once at the raw
    /// capture socket, once at the live `syslog_in` -- and returns the raw datagram bytes
    /// alongside the [`EventBatch`] the real input decoded from them (receipt-time `timestamp`
    /// fields already normalized).
    async fn round_trip(
        &mut self,
        batch: &EventBatch,
        encoder: impl Fn() -> SyslogEncoder,
    ) -> (Vec<u8>, EventBatch) {
        let mut to_capture =
            SyslogOutput::udp(self.capture_addr.to_string()).unwrap().with_encoder(encoder());
        to_capture.send(batch).await.expect("send to the capture socket");
        let mut buf = vec![0u8; 65_536];
        let (n, _) =
            tokio::time::timeout(Duration::from_millis(500), self.capture.recv_from(&mut buf))
                .await
                .expect("capture socket should receive the datagram")
                .expect("recv_from should succeed");
        buf.truncate(n);

        let mut to_input =
            SyslogOutput::udp(self.input_addr.to_string()).unwrap().with_encoder(encoder());
        to_input.send(batch).await.expect("send to the live syslog_in");
        let delivered = tokio::time::timeout(Duration::from_millis(500), self.rx.recv())
            .await
            .expect("syslog_in should decode and forward the batch")
            .expect("the Fanout channel should not have closed");
        let mut decoded = logit_pipeline::unwrap_batch(delivered);
        normalize_receipt_time(&mut decoded);
        (buf, decoded)
    }
}

fn decode_one(raw: &[u8]) -> Event {
    let mut decoder = SyslogDecoder::new(std::sync::Arc::new(logit_core::Resource::default()));
    let mut batch = decoder.decode(Bytes::copy_from_slice(raw)).expect("fixture should decode");
    assert_eq!(batch.events.len(), 1, "each fixture line decodes to exactly one event");
    batch.events.remove(0)
}

fn direct_batch(raw: &[u8]) -> EventBatch {
    let mut decoder = SyslogDecoder::new(std::sync::Arc::new(logit_core::Resource::default()));
    let mut batch = decoder.decode(Bytes::copy_from_slice(raw)).expect("fixture should decode");
    normalize_receipt_time(&mut batch);
    batch
}

/// One deterministic byte-for-byte case: a fixture's own decode round-trips through a live
/// `syslog_in` unchanged (whole-`EventBatch` equality), and the raw datagram the far end actually
/// received equals the case's `.expected` bytes (module doc's normalization list covers every
/// place the two differ).
async fn assert_byte_for_byte(harness: &mut Harness, fixture: &str, format: Format, raw: &[u8]) {
    let batch = direct_batch(raw); // already receipt-time normalized
    let expected = expected_bytes(fixture, raw);
    let (captured, decoded) = harness.round_trip(&batch, || SyslogEncoder::new(format, 16)).await;
    assert_eq!(
        captured, expected,
        "{fixture}: raw datagram bytes should match .expected (modulo the module doc's \
         permitted normalizations)"
    );
    assert_eq!(
        decoded, batch,
        "{fixture}: decode(sink_output) should equal the original decode, as a whole EventBatch"
    );
}

// ---- 5424 -> 5424, byte for byte -------------------------------------------------------------

#[tokio::test]
async fn rfc5424_fixtures_round_trip_byte_for_byte() {
    let mut harness = Harness::new().await;
    let cases: &[&str] = &[
        // RFC 5424 section 6.5's four worked examples.
        "rfc5424-example1",
        "rfc5424-example2",
        "rfc5424-example3",
        "rfc5424-example4",
        // Hand-written STRUCTURED-DATA/field coverage.
        "rfc5424-sd-escapes",
        "rfc5424-sd-repeated-param",
        "rfc5424-sd-dotted-id",
        "rfc5424-nil-timestamp-full",
        "rfc5424-non-numeric-procid",
        "rfc5424-non-utf8-msg",
    ];
    for name in cases {
        let raw = read_fixture(name, "in");
        assert_byte_for_byte(&mut harness, name, Format::Rfc5424, &raw).await;
    }

    // The one RFC 5424 interop capture -- read live from `testdata/interop/syslog/`, not copied.
    let raw = read_testdata("logger-rfc5424-basic-000.raw");
    assert_byte_for_byte(&mut harness, "interop-logger-rfc5424-basic", Format::Rfc5424, &raw).await;
}

/// The non-UTF-8 MSG case decodes to `Value::Bytes`, not `Value::Str` -- called out on its own
/// (the plan's "Non-UTF-8 MSG relays as `Value::Bytes` byte-for-byte" bullet), even though
/// `rfc5424_fixtures_round_trip_byte_for_byte` above already exercises it end to end.
#[tokio::test]
async fn rfc5424_non_utf8_message_relays_as_value_bytes_byte_for_byte() {
    let raw = read_fixture("rfc5424-non-utf8-msg", "in");
    let event = decode_one(&raw);
    match event.log.as_ref().unwrap().message {
        Value::Bytes(ref b) => assert_eq!(b.as_ref(), &[0xff, 0xfe, b'x']),
        ref other => panic!("expected Value::Bytes, got {other:?}"),
    }

    let mut harness = Harness::new().await;
    let batch = direct_batch(&raw);
    let (captured, decoded) =
        harness.round_trip(&batch, || SyslogEncoder::new(Format::Rfc5424, 16)).await;
    let expected = expected_bytes("rfc5424-non-utf8-msg", &raw);
    assert_eq!(captured, expected);
    match decoded.events[0].log.as_ref().unwrap().message {
        Value::Bytes(ref b) => assert_eq!(b.as_ref(), &[0xff, 0xfe, b'x']),
        ref other => panic!("expected Value::Bytes after the round trip, got {other:?}"),
    }
}

/// A 33-byte SD-NAME violates RFC 5424's 32-byte `SD-NAME` limit -- `syslog_in` rejects the whole
/// line (`bad_line`), so nothing reaches `syslog_out` at all. Decode-only: there is no sink output
/// to assert on.
#[tokio::test]
async fn oversize_sd_name_is_rejected_by_the_decoder_and_never_reaches_the_sink() {
    let raw = read_fixture("rfc5424-oversize-sd-name", "in");
    let mut decoder = SyslogDecoder::new(std::sync::Arc::new(logit_core::Resource::default()));
    let mut events = Vec::new();
    let result = decoder.decode_into(Bytes::copy_from_slice(&raw), 0, &mut events);
    // `decode_into` itself never fails for syslog (a malformed *line* is a skip-and-continue,
    // reported through diagnostics) -- the rejection shows up as zero events, not an `Err`.
    assert!(result.is_ok());
    assert!(events.is_empty(), "a 33-byte SD-NAME must reject the whole line, producing no event");
}

// ---- 3164 -> 3164, byte for byte ---------------------------------------------------------------

#[tokio::test]
async fn rfc3164_fixtures_round_trip_byte_for_byte() {
    let mut harness = Harness::new().await;
    let cases: &[&str] = &["rfc3164-nonnumeric-pid", "rfc3164-non-utf8-msg"];
    for name in cases {
        let raw = read_fixture(name, "in");
        assert_byte_for_byte(&mut harness, name, Format::Rfc3164, &raw).await;
    }

    // Interop captures that carry their own well-formed RFC 3164 TIMESTAMP token, so
    // `write_3164_timestamp` writes it back verbatim (no receipt-time fallback) -- fully
    // deterministic, unlike the two `python-syslog-handler-*` captures below.
    let deterministic: &[(&str, &str)] = &[
        ("logger-rfc3164-basic-000.raw", "interop-logger-rfc3164-basic"),
        ("logger-rfc3164-unicode-000.raw", "interop-logger-rfc3164-unicode"),
        ("rsyslog-000.raw", "interop-rsyslog"),
    ];
    for (testdata_name, fixture_name) in deterministic {
        let raw = read_testdata(testdata_name);
        assert_byte_for_byte(&mut harness, fixture_name, Format::Rfc3164, &raw).await;
    }
}

/// `tag[pid]` with a non-numeric `pid`, relayed 3164 -> 3164: `syslog.pid` stays a `Value::Str`
/// end to end, and the bracket contents survive unchanged (the plan's own callout).
#[tokio::test]
async fn rfc3164_non_numeric_pid_stays_a_str_through_the_relay() {
    let raw = read_fixture("rfc3164-nonnumeric-pid", "in");
    let event = decode_one(&raw);
    assert_eq!(event.attributes.get("syslog.pid").and_then(Value::as_str), Some("abc"));

    let mut harness = Harness::new().await;
    let batch = direct_batch(&raw);
    let (_, decoded) = harness.round_trip(&batch, || SyslogEncoder::new(Format::Rfc3164, 16)).await;
    assert_eq!(decoded.events[0].attributes.get("syslog.pid").and_then(Value::as_str), Some("abc"));
}

/// `python-syslog-handler-{000,001}.raw` carry no TIMESTAMP token at all (Python's stdlib
/// `SysLogHandler` in its minimal framing) -- module doc normalization (3): a 3164 -> 3164 relay
/// still emits *a* TIMESTAMP (receipt time), which these two captures can't be fixtured
/// byte-for-byte against, so they get a structural assertion instead: the PRI and the rest of the
/// line (hostname/tag/message) round-trip exactly, and the emitted TIMESTAMP is a well-formed RFC
/// 3164 token in the right position.
async fn assert_receipt_time_relay(
    harness: &mut Harness,
    testdata_name: &str,
    expected_rest: &str,
) {
    let raw = read_testdata(testdata_name);
    let batch = direct_batch(&raw);
    assert!(
        batch.events[0].attributes.get("syslog.timestamp").is_none(),
        "{testdata_name} must carry no syslog.timestamp attribute for this test to be meaningful"
    );
    let (captured, _decoded) =
        harness.round_trip(&batch, || SyslogEncoder::new(Format::Rfc3164, 16)).await;
    let text = String::from_utf8(captured).expect("this fixture's output is ASCII/UTF-8");
    let pri_end = text.find('>').expect("a PRI field") + 1;
    let ts = &text[pri_end..pri_end + 15];
    assert!(
        is_rfc3164_timestamp_shape(ts),
        "{testdata_name}: expected a well-formed RFC 3164 TIMESTAMP at {ts:?} in {text:?}"
    );
    let rest = &text[pri_end + 15..];
    assert_eq!(
        rest, expected_rest,
        "{testdata_name}: everything after the TIMESTAMP must be unchanged"
    );
}

/// Local copy of `logit_outputs::syslog`'s own (private) shape check -- this integration test has
/// no access to that crate's internals, and the shape itself is simple and RFC-defined: 3-letter
/// month, space, a space- or zero-padded day, space, `hh:mm:ss`.
fn is_rfc3164_timestamp_shape(s: &str) -> bool {
    const MONTHS: [&str; 12] =
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let b = s.as_bytes();
    if b.len() != 15 {
        return false;
    }
    let month_ok = MONTHS.iter().any(|m| m.as_bytes() == &b[0..3]);
    let digit = |i: usize| b[i].is_ascii_digit();
    month_ok
        && b[3] == b' '
        && (b[4] == b' ' || digit(4))
        && digit(5)
        && b[6] == b' '
        && digit(7)
        && digit(8)
        && b[9] == b':'
        && digit(10)
        && digit(11)
        && b[12] == b':'
        && digit(13)
        && digit(14)
}

#[tokio::test]
async fn python_syslog_handler_captures_relay_with_a_receipt_time_timestamp() {
    let mut harness = Harness::new().await;
    assert_receipt_time_relay(
        &mut harness,
        "python-syslog-handler-000.raw",
        " hello from python logging.handlers.SysLogHandler, captured for logit interop fixtures",
    )
    .await;
    assert_receipt_time_relay(
        &mut harness,
        "python-syslog-handler-001.raw",
        " {\"level\": \"info\", \"msg\": \"request handled\", \"path\": \"/\", \"status\": 200}",
    )
    .await;
}

// ---- 3164 -> 5424, documented normalizations ---------------------------------------------------

/// The plan's explicit 3164 -> 5424 case: TIMESTAMP falls to receipt time (rendered in RFC 3339,
/// not reinterpreted from the 3164 token), and STRUCTURED-DATA is always `-` (module doc
/// normalizations 4 and 5). Everything dialect-independent (PRI, hostname, tag/pid, message)
/// still carries over.
#[tokio::test]
async fn a_3164_to_5424_relay_falls_the_timestamp_to_receipt_time() {
    let raw = read_fixture("rfc3164-nonnumeric-pid", "in"); // has a real 3164 TIMESTAMP token
    let event = decode_one(&raw);
    let original_ts =
        event.attributes.get("syslog.timestamp").and_then(Value::as_str).expect("a 3164 TIMESTAMP");
    assert_eq!(original_ts, "Jan  1 00:00:00");

    let mut harness = Harness::new().await;
    let batch = direct_batch(&raw);
    let (captured, decoded) =
        harness.round_trip(&batch, || SyslogEncoder::new(Format::Rfc5424, 16)).await;
    let text = String::from_utf8(captured).unwrap();

    // `<PRI>1 ` then an RFC 3339 TIMESTAMP that is *not* a reinterpretation of the 3164 token.
    assert!(text.starts_with("<13>1 "), "got: {text}");
    let ts_field = text["<13>1 ".len()..].split(' ').next().unwrap();
    assert_ne!(ts_field, original_ts);
    assert_ne!(ts_field, "-", "a TIMESTAMP must be present -- event.timestamp is always known");
    assert!(
        ts_field.len() >= 20 && ts_field.ends_with('Z') && ts_field.contains('T'),
        "expected an RFC 3339 TIMESTAMP, got {ts_field:?} in {text:?}"
    );

    // STRUCTURED-DATA is always `-` on a relay that never carried `syslog.sd` to begin with.
    assert!(text.contains(" - hello world"), "expected a nil SD field before the message: {text}");

    // Dialect-independent fields still round-trip.
    assert_eq!(
        decoded.events[0].attributes.get("syslog.hostname").and_then(Value::as_str),
        Some("myhost")
    );
    assert_eq!(decoded.events[0].attributes.get("syslog.tag").and_then(Value::as_str), Some("app"));
    assert_eq!(decoded.events[0].attributes.get("syslog.pid").and_then(Value::as_str), Some("abc"));
}

// ---- opt-in `structured_data` ------------------------------------------------------------------

/// An event with non-`syslog.*` attributes, relayed through a 5424 sink configured with
/// `structured_data: { sd_id: "logit@32473" }`, decodes on the far end with those attributes
/// lifted under `syslog.sd["logit@32473"]` -- the plan's opt-in `structured_data` bullet.
#[tokio::test]
async fn opt_in_structured_data_lifts_non_syslog_attributes_into_syslog_sd() {
    let mut attrs = logit_core::AttrMap::new();
    attrs.insert("env", Value::str("prod"));
    attrs.insert("retries", Value::U64(3));
    let event = Event::log(
        1_000,
        attrs,
        logit_core::LogRecord {
            message: Value::str("relayed via opt-in structured_data"),
            severity: Some(logit_core::Severity::Info),
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    let batch = EventBatch {
        resource: std::sync::Arc::new(logit_core::Resource::default()),
        scope: None,
        events: vec![event],
    };

    let mut harness = Harness::new().await;
    let (_, decoded) = harness
        .round_trip(&batch, || {
            SyslogEncoder::new(Format::Rfc5424, 16).with_structured_data("logit@32473").unwrap()
        })
        .await;

    let sd = decoded.events[0]
        .attributes
        .get("syslog.sd")
        .unwrap_or_else(|| panic!("expected syslog.sd, got {:?}", decoded.events[0].attributes));
    let Value::Map(sd) = sd else { panic!("expected syslog.sd to be a Value::Map, got {sd:?}") };
    let element = sd
        .get("logit@32473")
        .unwrap_or_else(|| panic!("expected an SD-ELEMENT under logit@32473, got {sd:?}"));
    let Value::Map(params) = element else {
        panic!("expected the SD-ELEMENT to be a Value::Map, got {element:?}")
    };
    assert_eq!(params.get("env").and_then(Value::as_str), Some("prod"));
    // The opt-in element's PARAM-VALUEs always render as strings (module doc's "STRUCTURED-DATA"
    // section) -- `retries` comes back as `Value::Str("3")`, not `Value::U64(3)`.
    assert_eq!(params.get("retries").and_then(Value::as_str), Some("3"));
}
