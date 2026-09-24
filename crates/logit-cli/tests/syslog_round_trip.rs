//! `syslog_out` -> `syslog_in` round trip over real UDP, TCP, and TLS sockets. The components
//! run in-process (a `SyslogOutput` sending to a bound, live `SyslogInput`), not through
//! `logit run`. This lives in `logit-cli` because it already depends on both `logit-inputs` and
//! `logit-outputs`; a dev-dependency between those two crates would be a cycle.
//!
//! Each fixture case asserts two things: the bytes on the wire equal the case's `.expected`
//! bytes, and the live `syslog_in`'s decode of those bytes equals the original decode as a whole
//! `EventBatch`, with receipt-time timestamps zeroed. That is the fixed point ADR
//! `lossless-transit` requires, modulo the normalizations listed below.
//!
//! ## Fixture corpus (`tests/fixtures/syslog/`)
//!
//! One file pair per case: `<name>.in` (the raw line as a sender puts it on the wire) and
//! `<name>.expected` (the bytes `syslog_out` must emit for that decode, or the marker
//! [`SAME_AS_INPUT`] when the sink reproduces the input verbatim). `rfc5424-example1` through
//! `-example4` are RFC 5424 section 6.5's worked examples; the other `rfc*` cases are
//! hand-written. The `interop-*` cases have only an `.expected` file: their input is one of the
//! six UDP captures in `testdata/interop/syslog/` (all but `rsyslog-tcp-000.raw`; that
//! directory's README has each sender and version), read from there at runtime, never copied.
//!
//! ## Permitted normalizations (per `docs/adr/lossless-transit.md`)
//!
//! Where `syslog_out`'s output differs from its input, and why each is permitted rather than a
//! loss. `docs/adr/syslog-output.md` records the sink-side decisions behind them.
//!
//! 1. **An RFC 5424 §6.4 BOM is stripped and never re-emitted.** `syslog_in` strips a leading
//!    UTF-8 BOM from MSG; `syslog_out` never writes one, because Loki's `| json` stage doesn't skip
//!    it (`docs/adr/syslog-output.md`, "No RFC 5424 §6.4 BOM"). Fixtures: `rfc5424-example1`,
//!    `rfc5424-example3`.
//! 2. **An RFC 5424 TIMESTAMP renders with 6 fractional digits in UTC (`Z`).** An offset is
//!    converted to the same instant in `Z` form, and a shorter fraction is zero-padded
//!    (`.003Z` -> `.003000Z`). Fixtures: `rfc5424-example1` through `-example4`,
//!    `interop-logger-rfc5424-basic`.
//! 3. **RFC 3164 output always carries a TIMESTAMP.** Some real senders omit it
//!    (`python-syslog-handler-*`); the RFC 3164 header writer has no "omit" branch, so it falls
//!    back to `event.timestamp` (receipt time). Asserted structurally, not byte for byte, by
//!    `python_syslog_handler_captures_relay_with_a_receipt_time_timestamp`.
//! 4. **A 3164 -> 5424 relay's TIMESTAMP is receipt time, in RFC 3339.** A 3164 `Mmm dd hh:mm:ss`
//!    token has no year or timezone, and `syslog_out` won't guess one. Test:
//!    `a_3164_to_5424_relay_falls_the_timestamp_to_receipt_time`.
//! 5. **A relay's STRUCTURED-DATA is `-` when the event carries no `syslog.sd`**, as on any
//!    3164 -> 5424 relay. Same test as (4).
//! 6. **The header hostname and app-name can come from `syslog_out`'s configured defaults**
//!    (`with_hostname`/`with_app_name`) when the event has no `syslog.hostname`/`syslog.tag`. The
//!    round-trip encoders here leave both unset so no byte-for-byte case exercises this by
//!    accident; no test in this file depends on it.
//! 7. **SD-ELEMENTs and SD-PARAMs are ordered by name, not by wire position.**
//!    `write_structured_data`/`write_sd_element` sort SD-IDs and PARAM-NAMEs by name bytes, since
//!    attribute iteration follows process-global intern order: `[b@2 ..][a@1 ..]` re-emits as
//!    `[a@1 ..][b@2 ..]`. A repeated PARAM-NAME's occurrences, already one `Value::Array` after
//!    decode, are emitted together, so a wire `a b a` interleaving becomes `a a b`
//!    (`docs/known-gaps.md`). Fixtures: `rfc5424-example4`, `interop-logger-rfc5424-basic`.
//! 8. **A bare backslash in a PARAM-VALUE is re-emitted escaped.** RFC 5424 section 6.3.3 defines
//!    only `\"`, `\\`, and `\]` as escapes, so `\x` is a literal backslash followed by `x`, which
//!    `parse_param_value` keeps. `syslog_out` writes that backslash as `\\`: `p="a\xb"` relays as
//!    `p="a\\xb"`, the same PARAM-VALUE under the RFC's equivalence. Pinned by a unit test in
//!    `logit_outputs::syslog`, not a fixture here:
//!    `a_bare_backslash_param_value_is_re_emitted_in_canonical_escaped_form`.
//! 9. **A non-UTF-8 RFC 3164 HOSTNAME is dropped.** `parse_3164` never fails a line over its
//!    HOSTNAME, because `parse_line`'s version-sniff fallback depends on that, so it skips the
//!    `syslog.hostname` attribute and reports a throttled `hostname_not_utf8` diagnostic. Pinned by
//!    a unit test in `logit_inputs::syslog`:
//!    `a_non_utf8_rfc3164_hostname_is_skipped_with_a_throttled_diagnostic`.
//!
//! `mod tcp` and `mod tls` add no entry to this list: both transports share
//! `SyslogEncoder`/`SyslogDecoder`, and TCP (and TLS, RFC 5425) changes only the framing
//! (`docs/adr/syslog-tcp-ingress-and-tls.md`).

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

/// The `.expected` marker meaning "byte-identical to the `.in` file".
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
/// file is [`SAME_AS_INPUT`]) the case's own `.in` bytes.
fn expected_bytes(name: &str, input: &[u8]) -> Vec<u8> {
    let expected = read_fixture(name, "expected");
    if expected.as_slice() == SAME_AS_INPUT {
        input.to_vec()
    } else {
        expected
    }
}

/// Zeroes every event's receipt-time `timestamp`, the one field two independent decodes of the
/// same bytes legitimately disagree on. The `syslog.timestamp` attribute is origin data and is
/// left alone.
fn normalize_receipt_time(batch: &mut EventBatch) {
    for event in &mut batch.events {
        event.timestamp = 0;
    }
}

/// A plain UDP capture socket (the raw-bytes half of each assertion) beside a bound, live
/// [`SyslogInput`] draining into a [`Fanout`] channel (the decoded-`EventBatch` half). The input
/// is bound, then `local_addr()` read, then spawned, so there is no bind-drop race and no
/// sleep-based readiness guess.
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

    /// Sends `batch` once to the capture socket and once to the live `syslog_in`, each through a
    /// fresh [`SyslogOutput`], and returns the captured bytes and the receipt-time-normalized
    /// decode.
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

/// Asserts one fixture's wire bytes equal its `.expected` bytes and its live decode equals its
/// direct decode.
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

    // The one RFC 5424 interop capture.
    let raw = read_testdata("logger-rfc5424-basic-000.raw");
    assert_byte_for_byte(&mut harness, "interop-logger-rfc5424-basic", Format::Rfc5424, &raw).await;
}

/// A non-UTF-8 MSG decodes to `Value::Bytes`, not `Value::Str`, on both ends of the relay.
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

/// A 33-byte SD-NAME breaks RFC 5424's 32-byte limit, so `syslog_in` rejects the whole line
/// (`bad_line`). Decode-only: nothing reaches the sink.
#[tokio::test]
async fn oversize_sd_name_is_rejected_by_the_decoder_and_never_reaches_the_sink() {
    let raw = read_fixture("rfc5424-oversize-sd-name", "in");
    let mut decoder = SyslogDecoder::new(std::sync::Arc::new(logit_core::Resource::default()));
    let mut events = Vec::new();
    let result = decoder.decode_into(Bytes::copy_from_slice(&raw), 0, &mut events);
    // A malformed line is skipped and reported through diagnostics, so the rejection shows up
    // as zero events, not an `Err`.
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

    // Interop captures with their own RFC 3164 TIMESTAMP token, which `write_3164_timestamp`
    // writes back verbatim, so these are deterministic (unlike `python-syslog-handler-*`).
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

/// A non-numeric `tag[pid]` stays a `Value::Str` `syslog.pid` through a 3164 -> 3164 relay.
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

/// Normalization (3) for a capture with no TIMESTAMP token (`python-syslog-handler-*`, Python's
/// stdlib `SysLogHandler`). The relay adds a receipt-time TIMESTAMP, so the assertion is
/// structural: a well-formed RFC 3164 TIMESTAMP after the PRI, and everything after it unchanged.
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

/// Copy of `logit_outputs::syslog`'s private shape check: 3-letter month, space, a space- or
/// zero-padded day, space, `hh:mm:ss`.
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

/// Normalizations (4) and (5) on a 3164 -> 5424 relay. PRI, hostname, tag, pid, and message
/// still carry over.
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

    // STRUCTURED-DATA is `-`: the event carries no `syslog.sd`.
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

/// With `structured_data: { sd_id: "logit@32473" }`, an event's non-`syslog.*` attributes decode
/// on the far end under `syslog.sd["logit@32473"]`.
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
    // PARAM-VALUEs always render as strings (`logit_outputs::syslog`'s "STRUCTURED-DATA"), so
    // `retries` comes back as `Value::Str("3")`.
    assert_eq!(params.get("retries").and_then(Value::as_str), Some("3"));
}

// ---- transport: tcp -----------------------------------------------------------------------

/// `syslog_out(transport: tcp) -> syslog_in(transport: tcp)` over the UDP fixture corpus, plus two
/// stream-only cases: a raw LF-framed client, and an octet-counted MSG with an embedded newline.
mod tcp {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener as TokioTcpListener, TcpStream};

    /// TCP twin of [`Harness`]. The capture listener reads each connection to EOF, which works
    /// because each round trip uses a fresh `SyslogOutput::tcp` and drops it after sending.
    struct TcpHarness {
        capture_addr: SocketAddr,
        capture_rx: mpsc::Receiver<Vec<u8>>,
        input_addr: SocketAddr,
        rx: mpsc::Receiver<Delivered>,
    }

    impl TcpHarness {
        async fn new() -> Self {
            let capture =
                TokioTcpListener::bind("127.0.0.1:0").await.expect("binding the capture listener");
            let capture_addr =
                capture.local_addr().expect("capture listener should have a local addr");
            let (capture_tx, capture_rx) = mpsc::channel(16);
            tokio::spawn(async move {
                loop {
                    let Ok((mut stream, _)) = capture.accept().await else { break };
                    let tx = capture_tx.clone();
                    tokio::spawn(async move {
                        let mut buf = Vec::new();
                        let _ = stream.read_to_end(&mut buf).await;
                        let _ = tx.send(buf).await;
                    });
                }
            });

            let mut input = SyslogInput::tcp("127.0.0.1:0");
            input.bind().await.expect("binding the tcp syslog_in listener");
            let input_addr = input.local_addr().expect("bind() should leave a real address behind");

            let (tx, rx) = mpsc::channel(16);
            let sink = Fanout::new(vec![tx]);
            tokio::spawn(async move {
                let _ = input.run(sink).await;
            });

            Self { capture_addr, capture_rx, input_addr, rx }
        }

        /// TCP twin of [`Harness::round_trip`]; the captured bytes are an octet-counted frame.
        async fn round_trip(
            &mut self,
            batch: &EventBatch,
            encoder: impl Fn() -> SyslogEncoder,
        ) -> (Vec<u8>, EventBatch) {
            let mut to_capture =
                SyslogOutput::tcp(self.capture_addr.to_string(), Duration::from_secs(2))
                    .with_encoder(encoder());
            to_capture.send(batch).await.expect("send to the capture listener");
            drop(to_capture); // closes the connection, EOFing the capture task's read_to_end
            let framed = tokio::time::timeout(Duration::from_millis(500), self.capture_rx.recv())
                .await
                .expect("capture listener should receive the frame")
                .expect("the capture channel should not have closed");

            let mut to_input =
                SyslogOutput::tcp(self.input_addr.to_string(), Duration::from_secs(2))
                    .with_encoder(encoder());
            to_input.send(batch).await.expect("send to the live syslog_in");
            drop(to_input);
            let delivered = tokio::time::timeout(Duration::from_millis(500), self.rx.recv())
                .await
                .expect("syslog_in should decode and forward the batch")
                .expect("the Fanout channel should not have closed");
            let mut decoded = logit_pipeline::unwrap_batch(delivered);
            normalize_receipt_time(&mut decoded);
            (framed, decoded)
        }
    }

    /// Strips an RFC 6587 section 3.4.1 octet-counted frame's `MSG-LEN`, asserting it equals the
    /// MSG's length. It's the only framing `syslog_out`'s TCP transport adds.
    fn split_octet_counted(frame: &[u8]) -> &[u8] {
        let sp = frame.iter().position(|&b| b == b' ').expect("a leading MSG-LEN SP");
        let len: usize =
            std::str::from_utf8(&frame[..sp]).unwrap().parse().expect("a numeric MSG-LEN");
        let msg = &frame[sp + 1..];
        assert_eq!(msg.len(), len, "MSG-LEN must equal the actual message length");
        msg
    }

    /// TCP twin of `assert_byte_for_byte`, against the same `.expected` bytes.
    async fn assert_byte_for_byte_tcp(
        harness: &mut TcpHarness,
        fixture: &str,
        format: Format,
        raw: &[u8],
    ) {
        let batch = direct_batch(raw); // already receipt-time normalized
        let expected = expected_bytes(fixture, raw);
        let (framed, decoded) = harness.round_trip(&batch, || SyslogEncoder::new(format, 16)).await;
        let msg = split_octet_counted(&framed);
        assert_eq!(
            msg,
            expected.as_slice(),
            "{fixture}: the octet-counted MSG should match .expected (modulo the module doc's \
             permitted normalizations)"
        );
        assert_eq!(
            decoded, batch,
            "{fixture}: decode(sink_output) should equal the original decode, as a whole EventBatch"
        );
    }

    /// The UDP corpus over TCP, byte for byte inside the octet-counted frame.
    #[tokio::test]
    async fn fixture_corpus_round_trips_over_tcp() {
        let mut harness = TcpHarness::new().await;

        let rfc5424_cases: &[&str] = &[
            "rfc5424-example1",
            "rfc5424-example2",
            "rfc5424-example3",
            "rfc5424-example4",
            "rfc5424-sd-escapes",
            "rfc5424-sd-repeated-param",
            "rfc5424-sd-dotted-id",
            "rfc5424-nil-timestamp-full",
            "rfc5424-non-numeric-procid",
            "rfc5424-non-utf8-msg",
        ];
        for name in rfc5424_cases {
            let raw = read_fixture(name, "in");
            assert_byte_for_byte_tcp(&mut harness, name, Format::Rfc5424, &raw).await;
        }
        let raw = read_testdata("logger-rfc5424-basic-000.raw");
        assert_byte_for_byte_tcp(
            &mut harness,
            "interop-logger-rfc5424-basic",
            Format::Rfc5424,
            &raw,
        )
        .await;

        let rfc3164_cases: &[&str] = &["rfc3164-nonnumeric-pid", "rfc3164-non-utf8-msg"];
        for name in rfc3164_cases {
            let raw = read_fixture(name, "in");
            assert_byte_for_byte_tcp(&mut harness, name, Format::Rfc3164, &raw).await;
        }
        // The UDP test's deterministic interop captures.
        let deterministic: &[(&str, &str)] = &[
            ("logger-rfc3164-basic-000.raw", "interop-logger-rfc3164-basic"),
            ("logger-rfc3164-unicode-000.raw", "interop-logger-rfc3164-unicode"),
            ("rsyslog-000.raw", "interop-rsyslog"),
        ];
        for (testdata_name, fixture_name) in deterministic {
            let raw = read_testdata(testdata_name);
            assert_byte_for_byte_tcp(&mut harness, fixture_name, Format::Rfc3164, &raw).await;
        }
    }

    /// A raw client using LF-delimited framing (rsyslog `omfwd`'s default, and what the listener
    /// picks when the first byte isn't an ASCII digit) decodes like UDP.
    #[tokio::test]
    async fn a_raw_lf_framed_client_is_decoded_like_udp() {
        let mut input = SyslogInput::tcp("127.0.0.1:0");
        input.bind().await.expect("binding the tcp syslog_in listener");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        let (tx, mut rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });

        let mut stream = TcpStream::connect(addr).await.expect("connecting to syslog_in");
        stream
            .write_all(b"<13>1 2023-01-01T00:00:00Z myhost app - - - hello over raw tcp\n")
            .await
            .expect("writing the LF-framed message");
        drop(stream); // the message already ended in its own LF

        let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("syslog_in should decode and forward the message")
            .expect("the Fanout channel should not have closed");
        let batch = logit_pipeline::unwrap_batch(delivered);
        assert_eq!(batch.events.len(), 1);
        let event = &batch.events[0];
        assert_eq!(event.attributes.get("syslog.hostname").and_then(Value::as_str), Some("myhost"));
        assert_eq!(event.attributes.get("syslog.tag").and_then(Value::as_str), Some("app"));
        assert_eq!(event.log.as_ref().unwrap().message.as_str(), Some("hello over raw tcp"));
    }

    /// An octet-counted MSG with an embedded newline is one event: the framer delimits by count,
    /// and `SyslogInput::tcp` turns off the decoder's line splitting
    /// (`SyslogDecoder::with_line_splitting`).
    #[tokio::test]
    async fn a_multiline_octet_counted_message_arrives_as_one_event() {
        let mut input = SyslogInput::tcp("127.0.0.1:0");
        input.bind().await.expect("binding the tcp syslog_in listener");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        let (tx, mut rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });

        let msg = b"<13>1 2023-01-01T00:00:00Z myhost app - - - line one\nline two";
        let mut frame = format!("{} ", msg.len()).into_bytes();
        frame.extend_from_slice(msg);

        let mut stream = TcpStream::connect(addr).await.expect("connecting to syslog_in");
        stream.write_all(&frame).await.expect("writing the octet-counted frame");
        drop(stream);

        let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("syslog_in should decode and forward the message")
            .expect("the Fanout channel should not have closed");
        let batch = logit_pipeline::unwrap_batch(delivered);
        assert_eq!(
            batch.events.len(),
            1,
            "an embedded newline inside an octet-counted MSG must not split into two events"
        );
        assert_eq!(
            batch.events[0].log.as_ref().unwrap().message.as_str(),
            Some("line one\nline two")
        );
    }
}

// ---- transport: tls (RFC 5425) -------------------------------------------------------------

/// `syslog_out` -> `syslog_in` over TLS: server TLS, mutual TLS, and the wrong-CA negative case
/// (`docs/adr/syslog-tcp-ingress-and-tls.md`).
mod tls {
    use super::*;
    use logit_inputs::tcp::TlsServerSettings;
    use logit_outputs::syslog::TlsClientSettings;
    use logit_pipeline::{classify, Fault};

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls`, two levels up from `crates/logit-cli`.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// One log event. The TLS tests cover the transport; the UDP and TCP tests cover decoding.
    fn sample_batch() -> EventBatch {
        let mut attrs = logit_core::AttrMap::new();
        attrs.insert("host", "tls-test-host");
        let event = Event::log(
            1_000,
            attrs,
            logit_core::LogRecord {
                message: Value::str("hello over tls"),
                severity: Some(logit_core::Severity::Info),
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        EventBatch {
            resource: std::sync::Arc::new(logit_core::Resource::default()),
            scope: None,
            events: vec![event],
        }
    }

    /// Spawns a TLS-terminating TCP `syslog_in`, returning its bound address and the receiver
    /// its decoded batches land on.
    async fn spawn_tls_input(
        settings: &TlsServerSettings,
    ) -> (SocketAddr, mpsc::Receiver<Delivered>) {
        let mut input = SyslogInput::tcp("127.0.0.1:0")
            .with_tls(settings, &testdata_dir())
            .expect("a tls: block is legal on a tcp syslog_in");
        input.bind().await.expect("binding the tls syslog_in listener");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        let (tx, rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });
        (addr, rx)
    }

    /// Server TLS only: `syslog_in` sets no `client_ca_file`, so it accepts any client.
    #[tokio::test]
    async fn server_tls_round_trips_a_batch() {
        let (addr, mut rx) = spawn_tls_input(&TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        })
        .await;

        let mut output =
            SyslogOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &TlsClientSettings {
                        ca_file: Some("ca.pem".to_string()),
                        ..Default::default()
                    },
                    &testdata_dir(),
                )
                .expect("a tls: block is legal on a tcp syslog_out");
        let batch = sample_batch();
        output.send(&batch).await.expect("send over server TLS should succeed");

        let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("syslog_in should decode and forward the batch")
            .expect("the Fanout channel should not have closed");
        let mut decoded = logit_pipeline::unwrap_batch(delivered);
        normalize_receipt_time(&mut decoded);
        assert_eq!(decoded.events.len(), 1);
        assert_eq!(
            decoded.events[0].log.as_ref().unwrap().message.as_str(),
            Some("hello over tls")
        );
    }

    /// Mutual TLS: `syslog_in` requires a client certificate chaining to `ca.pem`
    /// (`client_ca_file`), `syslog_out` presents `client.pem`/`client.key` -- both signed by the
    /// same test CA (`testdata/tls/regen.sh`).
    #[tokio::test]
    async fn mutual_tls_round_trips_a_batch() {
        let (addr, mut rx) = spawn_tls_input(&TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: Some("ca.pem".to_string()),
        })
        .await;

        let mut output =
            SyslogOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &TlsClientSettings {
                        ca_file: Some("ca.pem".to_string()),
                        cert_file: Some("client.pem".to_string()),
                        key_file: Some("client.key".to_string()),
                        insecure_skip_verify: false,
                    },
                    &testdata_dir(),
                )
                .expect("a client certificate is legal on the tcp transport");
        let batch = sample_batch();
        output.send(&batch).await.expect("mutual TLS should succeed");

        let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("syslog_in should decode and forward the batch")
            .expect("the Fanout channel should not have closed");
        let mut decoded = logit_pipeline::unwrap_batch(delivered);
        normalize_receipt_time(&mut decoded);
        assert_eq!(
            decoded.events[0].log.as_ref().unwrap().message.as_str(),
            Some("hello over tls")
        );
    }

    /// A `syslog_out` trusting `other-ca.pem` fails its own certificate verification before any
    /// batch byte is written, so the error is `Fault::Clean`. That's deterministic because a
    /// server-cert rejection happens inside `TlsConnector::connect`. A client-cert rejection is
    /// the opposite: under TLS 1.3 the server sends its whole flight before seeing the client's
    /// certificate, so this write-only sink can report success
    /// (`logit_outputs::syslog`'s
    /// `tls_tcp_without_a_client_certificate_delivers_nothing_to_a_client_ca_requiring_collector`).
    #[tokio::test]
    async fn a_client_trusting_the_wrong_ca_is_refused_and_classified_clean() {
        let (addr, _rx) = spawn_tls_input(&TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: None,
        })
        .await;

        let mut output =
            SyslogOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &TlsClientSettings {
                        ca_file: Some("other-ca.pem".to_string()),
                        ..Default::default()
                    },
                    &testdata_dir(),
                )
                .expect("a tls: block is legal on the tcp transport");

        let err = output.send(&sample_batch()).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean);
    }
}
