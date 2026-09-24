//! `statsd_out` -> `statsd_in` round trip over real UDP and TCP sockets. The components run
//! in-process (a `StatsdOutput` sending to a bound, live `StatsdInput`), not through `logit run`.
//! This lives in `logit-cli` because it already depends on both `logit-inputs` and
//! `logit-outputs`; a dev-dependency between those two crates would be a cycle.
//!
//! Each fixture case asserts two things: the bytes on the wire equal the case's `.expected`
//! bytes, and the live `statsd_in`'s decode of those bytes equals the original decode as a whole
//! `EventBatch`. Receipt-time timestamps are zeroed first. That is the fixed point ADR
//! `lossless-transit` requires, modulo the normalizations listed below. The one-way-lossy cases
//! (`sanitizer-name-hash`, `sanitizer-tag-value-at`, and (6)) assert the wire bytes only.
//!
//! ## Fixture corpus (`tests/fixtures/statsd/`)
//!
//! One file pair per case: `<name>.in` (the raw line(s) as a client puts them on the wire) and
//! `<name>.expected` (the bytes `statsd_out` must emit for that decode, or the marker
//! [`SAME_AS_INPUT`] when the sink reproduces the input verbatim). The `dogstatsd-*` cases are the
//! worked examples from Datadog's DogStatsD protocol reference, including its event and service
//! check examples, except `dogstatsd-event-all-fields`. That one and every other case are
//! hand-written to pin one grammar corner or normalization. The ones whose names don't say what
//! they pin:
//!
//! - `dogstatsd-event-all-fields`, `service-check-all-fields`: every optional field, already in
//!   canonical order, so they stay [`SAME_AS_INPUT`] under normalization (9).
//! - `event-text-with-pipe-and-escaped-newline`: `TEXT` also contains a `:`.
//! - `service-check-no-message`: every optional field except `m:`.
//! - `event-multibyte-title-lengths`: `_e{TITLE_LEN,...}` counts bytes, not chars.
//! - `event-title-contains-pipe`: the length prefix, not a `|` scan, delimits the title.
//! - `event-text-trailing-space`, `service-check-message-trailing-space`: the last byte of `TEXT`
//!   or of an `m:` message is a space followed by the `.in` file's own trailing `\n`;
//!   `decode_into` must not trim it.
//!
//! ## Permitted normalizations (per `docs/adr/lossless-transit.md`)
//!
//! Where `statsd_out`'s output differs from its input, and why each is permitted rather than a
//! loss. `crates/logit-outputs/src/statsd.rs`'s module doc explains the encoder side of each.
//!
//! 1. **Counter sample-rate folding.** `hits:1|c|@0.1` decodes to `Counter(10.0)` (extrapolated
//!    by `1 / sample_rate` at decode) and relays as `hits:10|c`: a counter's wire form can't say
//!    "this was extrapolated". Not applied to a `Samples` record, whose `sample_rate` is un-applied
//!    information and survives the relay (`logit_outputs::statsd`'s "Sample rate: never for a
//!    counter, real for `Samples`"). Fixture: `sampled-counter-rate-folded`.
//! 2. **A sample rate of `1.0` is omitted**, since it's the grammar's default. Fixture:
//!    `explicit-rate-one-omitted`.
//! 3. **Tags are emitted in `AttrMap` order**, which is `Symbol` (intern) order. For tag keys the
//!    process has never interned before, that is first-seen order, so one `#k1:v1,k2:v2` segment
//!    relays in wire order even when it isn't alphabetical. Fixture:
//!    `multi-tag-preserves-wire-order` (its key names are non-alphabetical for this reason).
//! 4. **Numbers render with `f64`'s `Display`** (`push_float`): `0.500` relays as `0.5`. Fixture:
//!    `number-formatting-trailing-zeros`.
//! 5. **Sanitizer substitutions.** A metric name or tag key containing `: | @ # , \n \r \0`,
//!    another ASCII control character, or whitespace gets `_` in its place. A tag value forbids the
//!    same set except `:` (only a tag's first colon is significant). A `SetMembers` member forbids
//!    only `:`, `|`, and control characters: `@`/`#`/`,`/whitespace aren't delimiters in a member's
//!    position, and substituting them would make distinct members collide. Most of this is
//!    reachable from a real decode: `statsd_in` only trims a line's leading and trailing
//!    whitespace, so `a#b:1|c` decodes to the name `"a#b"`. Fixtures: `sanitizer-name-hash`
//!    (`a#b:1|c` -> `a_b:1|c`), `sanitizer-tag-value-at` (`x:1|c|#k:a@b` -> `x:1|c|#k:a_b`), and
//!    `sanitizer-member-space` (`users:a b|s`, [`SAME_AS_INPUT`]). A `|` can't reach a member
//!    through a decode, since the grammar splits a segment on it first, so
//!    `sanitizer_substitution_in_a_set_member_is_sanitized_and_counted` builds that batch by hand.
//! 6. **`format: statsd` drops what only DogStatsD can express**, the "sink-configured dialect
//!    change" normalization:
//!    - The `|#k:v,...` tag segment is omitted (never an empty `|#`).
//!    - A multi-value `Samples` line (`name:v1:v2:v3|ms`) splits into one `name:v|ms` line per
//!      value, repeating a non-`1.0` `sample_rate` on each.
//!    - A timer's `h`/`d` type letter becomes `ms`.
//!    - `|c:<container-id>` and `|T<timestamp>` are dropped.
//!    - A DogStatsD event or service check has no classic form, so the whole event is dropped and
//!      counted in `EncodeStats::dropped_dialect_events`.
//!
//!    Fixtures: the `statsd-dialect-*` cases, plus
//!    `events_and_service_checks_produce_no_output_and_are_counted_under_plain_statsd`, which
//!    calls [`logit_outputs::statsd::StatsdEncoder::encode_into`] directly because a wholly-dropped
//!    batch sends no datagram to capture.
//! 7. **`SetMembers` splits one member per line in both dialects.** The classic grammar has no
//!    multi-value set form, so `statsd_in`'s `name:m1:m2|s` decode always relays as one
//!    `name:<member>|s` line per member. Covered by `dogstatsd-set` and by
//!    `statsd_in_aggregate_statsd_out_relay_is_exact` (two members, two lines).
//! 8. **An exact duplicate tag token dedupes at decode**: `#team:a,team:a` -> `#team:a`, and
//!    `#urgent,urgent` -> `#urgent` (the Datadog agent's rule, so a `Value::Array` is never one
//!    element long). A repeated key with distinct values is not a normalization: `insert_tags`
//!    folds it into a `Value::Array` in wire order and `statsd_out` expands it back
//!    (`crates/logit-inputs/src/statsd.rs`'s "DogStatsD tags"). Fixtures:
//!    `repeated-tag-exact-duplicate-deduped`, `bare-tag-exact-duplicate-deduped`.
//! 9. **DogStatsD event and service check fields re-emit in canonical order.** `_e` order is
//!    `d:`/`h:`/`p:`/`t:`/`k:`/`s:`/`#tags`/`c:`; `_sc` order is `d:`/`h:`/`#tags`/`c:`/`m:` (`m:`
//!    last, since it consumes the rest of the line on decode). Nothing else changes: titles, text,
//!    names, hosts, aggregation keys, source types, and messages survive verbatim, embedded `|`
//!    and the `TEXT` `\n` escape included. Fixture: `event-fields-reordered-canonicalized`.
//!
//! Everything else relays byte for byte, modulo (3), (4), and (9): the raw `Samples`/`SetMembers`
//! kinds, `|c:`/`|T` under DogStatsD, relative-gauge deltas, and the negative-absolute-gauge
//! two-line idiom.
//!
//! `mod tcp` and `mod tls` add no entry to this list: both transports share
//! `StatsdEncoder`/`StatsdDecoder`, and only the framing differs. UDP newline-*joins* a batch's
//! lines into one datagram; TCP newline-*terminates* each line, so the TCP capture is the UDP bytes
//! plus one final `\n` (`mod tcp::strip_lf_framing`).

use bytes::Bytes;
use logit_core::{Event, EventBatch, Value};
use logit_inputs::statsd::{StatsdDecoder, StatsdInput};
use logit_outputs::influxdb::InfluxLineEncoder;
use logit_outputs::statsd::{Format, StatsdEncoder, StatsdOutput};
use logit_pipeline::{Delivered, Fanout, Input, Output};
use logit_proto::{Decoder, Encoder, FramedEncoder, MessageBuf};
use logit_transforms::{Aggregator, Distributions, Sets};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;

/// The `.expected` marker meaning "byte-identical to the `.in` file", shared with the other
/// round-trip corpora.
const SAME_AS_INPUT: &[u8] = b"== SAME AS INPUT ==";

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/statsd")
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()))
}

fn read_fixture(name: &str, ext: &str) -> Vec<u8> {
    read(&fixtures_dir().join(format!("{name}.{ext}")))
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

/// Zeroes each event's receipt-time `timestamp`, the one field two independent decodes of the
/// same bytes legitimately disagree on. An event with a `statsd.timestamp` `Value::U64` carrier
/// keeps its timestamp: that came from a wire `|T<secs>` segment, so both decodes agree on it.
fn normalize_receipt_time(batch: &mut EventBatch) {
    for event in &mut batch.events {
        let has_wire_timestamp =
            matches!(event.attributes.get("statsd.timestamp"), Some(Value::U64(_)));
        if !has_wire_timestamp {
            event.timestamp = 0;
        }
    }
}

fn direct_batch(raw: &[u8]) -> EventBatch {
    let mut decoder = StatsdDecoder::new(std::sync::Arc::new(logit_core::Resource::default()));
    let mut batch = decoder.decode(Bytes::copy_from_slice(raw)).expect("fixture should decode");
    normalize_receipt_time(&mut batch);
    batch
}

/// A plain UDP capture socket (the raw-bytes half of each assertion) beside a bound, live
/// [`StatsdInput`] draining into a [`Fanout`] channel (the decoded-`EventBatch` half). The input
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

        let mut input = StatsdInput::new("127.0.0.1:0");
        input.bind().await.expect("binding the statsd_in listener");
        let input_addr = input.local_addr().expect("bind() should leave a real address behind");

        let (tx, rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });

        Self { capture, capture_addr, input_addr, rx }
    }

    /// Sends `batch` once to the capture socket and once to the live `statsd_in`, each through a
    /// fresh [`StatsdOutput`], and returns the captured bytes and the receipt-time-normalized
    /// decode.
    async fn round_trip(
        &mut self,
        batch: &EventBatch,
        encoder: impl Fn() -> StatsdEncoder,
    ) -> (Vec<u8>, EventBatch) {
        let captured = self.capture_only(batch, encoder()).await;

        let mut to_input =
            StatsdOutput::udp(self.input_addr.to_string()).unwrap().with_encoder(encoder());
        to_input.send(batch).await.expect("send to the live statsd_in");
        let delivered = tokio::time::timeout(Duration::from_millis(500), self.rx.recv())
            .await
            .expect("statsd_in should decode and forward the batch")
            .expect("the Fanout channel should not have closed");
        let mut decoded = logit_pipeline::unwrap_batch(delivered);
        normalize_receipt_time(&mut decoded);
        (captured, decoded)
    }

    /// Sends `batch` to the capture socket only and returns the datagram, for the tests that make
    /// no decode-equality claim.
    async fn capture_only(&mut self, batch: &EventBatch, encoder: StatsdEncoder) -> Vec<u8> {
        let mut to_capture =
            StatsdOutput::udp(self.capture_addr.to_string()).unwrap().with_encoder(encoder);
        to_capture.send(batch).await.expect("send to the capture socket");
        let mut buf = vec![0u8; 65_536];
        let (n, _) =
            tokio::time::timeout(Duration::from_millis(500), self.capture.recv_from(&mut buf))
                .await
                .expect("capture socket should receive the datagram")
                .expect("recv_from should succeed");
        buf.truncate(n);
        buf
    }

    /// Sends `raw` straight to the live `statsd_in`, bypassing `StatsdOutput`, so the aggregate
    /// tests start from a UDP decode that no encoder has touched.
    async fn send_raw_and_decode(&mut self, raw: &[u8]) -> EventBatch {
        let sender =
            UdpSocket::bind("127.0.0.1:0").await.expect("binding an ephemeral sender socket");
        sender.send_to(raw, self.input_addr).await.expect("sending the raw datagram");
        let delivered = tokio::time::timeout(Duration::from_millis(500), self.rx.recv())
            .await
            .expect("statsd_in should decode and forward the batch")
            .expect("the Fanout channel should not have closed");
        logit_pipeline::unwrap_batch(delivered)
    }
}

/// Asserts one fixture's wire bytes equal its `.expected` bytes and its live decode equals its
/// direct decode. `encoder` is a factory because each of the two sends needs its own encoder.
async fn assert_byte_for_byte(
    harness: &mut Harness,
    fixture: &str,
    encoder: impl Fn() -> StatsdEncoder,
) {
    let raw = read_fixture(fixture, "in");
    let batch = direct_batch(&raw); // already receipt-time normalized
    let expected = expected_bytes(fixture, &raw);
    let (captured, decoded) = harness.round_trip(&batch, encoder).await;
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

// ---- DogStatsD docs' own worked examples, byte for byte ----------------------------------------

#[tokio::test]
async fn dogstatsd_docs_examples_round_trip_byte_for_byte() {
    let mut harness = Harness::new().await;
    let cases: &[&str] = &[
        "dogstatsd-counter",
        "dogstatsd-gauge",
        "dogstatsd-histogram-sampled",
        "dogstatsd-set",
        "dogstatsd-counter-tag",
        "dogstatsd-distribution-tag",
        "dogstatsd-gauge-timestamp",
        "dogstatsd-counter-container-id",
        "dogstatsd-event",
        "dogstatsd-service-check",
    ];
    for name in cases {
        assert_byte_for_byte(&mut harness, name, || StatsdEncoder::new(Format::DogStatsd)).await;
    }
}

// ---- Hand-written grammar/normalization coverage, byte for byte --------------------------------

#[tokio::test]
async fn hand_written_dogstatsd_fixtures_round_trip_byte_for_byte() {
    let mut harness = Harness::new().await;
    let cases: &[&str] = &[
        "multi-value-timer",
        "bare-tag-counter",
        "tag-value-with-colon",
        "packed-multi-line-datagram",
        "all-segments-together",
        "multi-tag-preserves-wire-order",
        "dogstatsd-event-all-fields",
        "event-text-with-pipe-and-escaped-newline",
        "event-multibyte-title-lengths",
        "event-title-contains-pipe",
        "service-check-all-fields",
        "service-check-no-message",
        "packed-datagram-counter-event-service-check",
        "event-text-trailing-space",
        "service-check-message-trailing-space",
    ];
    for name in cases {
        assert_byte_for_byte(&mut harness, name, || StatsdEncoder::new(Format::DogStatsd)).await;
    }
}

/// Normalizations (1), (2), (4), and (9): the wire bytes change, but the decode still round-trips
/// equal, since each only re-spells the same information.
#[tokio::test]
async fn explicit_normalizations_round_trip_byte_for_byte() {
    let mut harness = Harness::new().await;
    let cases: &[&str] = &[
        "sampled-counter-rate-folded",
        "explicit-rate-one-omitted",
        "number-formatting-trailing-zeros",
        "event-fields-reordered-canonicalized",
    ];
    for name in cases {
        assert_byte_for_byte(&mut harness, name, || StatsdEncoder::new(Format::DogStatsd)).await;
    }
}

/// Normalization (5), reachable from a real decode. The substituted byte is gone for good, so
/// only the wire bytes are asserted.
#[tokio::test]
async fn sanitizer_substitutions_reachable_from_a_real_decode_produce_the_expected_wire_bytes() {
    let mut harness = Harness::new().await;
    for fixture in ["sanitizer-name-hash", "sanitizer-tag-value-at"] {
        let raw = read_fixture(fixture, "in");
        let batch = direct_batch(&raw);
        let expected = read_fixture(fixture, "expected");
        let captured = harness.capture_only(&batch, StatsdEncoder::new(Format::DogStatsd)).await;
        assert_eq!(
            captured, expected,
            "{fixture}: format: dogstatsd output should match .expected (module doc (5))"
        );
    }
}

/// Normalization (5)'s contrasting case: a space in a set member isn't substituted, so this is a
/// full byte-for-byte, decode-equality round trip.
#[tokio::test]
async fn a_member_with_a_space_round_trips_byte_for_byte() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "sanitizer-member-space", || {
        StatsdEncoder::new(Format::DogStatsd)
    })
    .await;
}

// ---- Multi-value DogStatsD tags (normalization (8)) ---------------------------------------------

/// A repeated tag key with distinct values relays byte for byte, through a `Value::Array` in wire
/// order.
#[tokio::test]
async fn a_repeated_tag_key_round_trips_byte_for_byte() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "repeated-tag-key-round-trips", || {
        StatsdEncoder::new(Format::DogStatsd)
    })
    .await;
}

/// Three occurrences of the same tag key fold into a three-element `Array` and expand back to
/// three wire tags, still in wire order.
#[tokio::test]
async fn a_tag_key_repeated_three_times_round_trips_byte_for_byte() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "repeated-tag-three-values", || {
        StatsdEncoder::new(Format::DogStatsd)
    })
    .await;
}

/// A bare tag and a valued tag sharing a key are not duplicates -- both forms survive, in order
/// (`#urgent,urgent:1` -> `Array[Bool(true), Str("1")]` -> `urgent,urgent:1`).
#[tokio::test]
async fn a_bare_and_valued_tag_sharing_a_key_round_trips_byte_for_byte() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "bare-and-valued-tag-mix", || {
        StatsdEncoder::new(Format::DogStatsd)
    })
    .await;
}

/// The same fold applies to a DogStatsD event line's `#` field; `insert_tags` backs both.
#[tokio::test]
async fn a_repeated_tag_key_on_an_event_line_round_trips_byte_for_byte() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "repeated-tag-on-event-line", || {
        StatsdEncoder::new(Format::DogStatsd)
    })
    .await;
}

/// Normalization (8): `#team:a,team:a` decodes to `Str("a")`, never a one-element `Array`, and
/// relays as `#team:a`.
#[tokio::test]
async fn an_exact_duplicate_tag_value_dedupes_to_a_single_tag() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "repeated-tag-exact-duplicate-deduped", || {
        StatsdEncoder::new(Format::DogStatsd)
    })
    .await;
}

/// The bare-token form of the same dedupe rule: `#urgent,urgent` -> `Bool(true)` (never a
/// one-element `Array`) -> `#urgent`.
#[tokio::test]
async fn an_exact_duplicate_bare_tag_dedupes_to_a_single_tag() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "bare-tag-exact-duplicate-deduped", || {
        StatsdEncoder::new(Format::DogStatsd)
    })
    .await;
}

/// `statsd_in -> aggregate -> statsd_out`: two lines with the same multi-valued tag are one
/// series (`SeriesKey` compares an `Array` element-wise), so they merge and the tag bytes survive.
#[tokio::test]
async fn statsd_in_aggregate_statsd_out_relay_preserves_a_multi_valued_tag() {
    let mut harness = Harness::new().await;

    let raw = b"x:1|c|#team:a,team:b\nx:1|c|#team:a,team:b";
    let batch = harness.send_raw_and_decode(raw).await;
    assert_eq!(batch.events.len(), 2, "one event per line");

    let mut aggregator = Aggregator::new(Duration::from_secs(10));
    let resource = batch.resource.clone();
    let mut forwarded = Vec::new();
    for mut event in batch.events {
        if aggregator.process(&resource, &mut event) {
            forwarded.push(event);
        }
    }
    assert!(forwarded.is_empty(), "a delta Counter is always absorbed by aggregate");

    let mut flushed = aggregator.flush(1_800_000_000_000_000_000);
    assert_eq!(flushed.len(), 1, "one (resource, scope) group");
    let (flush_resource, flush_scope, events) = flushed.remove(0);
    let out_batch = EventBatch {
        resource: flush_resource,
        scope: flush_scope,
        events: events.into_iter().map(|(event, _links)| event).collect(),
    };
    assert_eq!(
        out_batch.events.len(),
        1,
        "both lines share the same multi-valued-tag series, so they merge into one"
    );

    let captured = harness.capture_only(&out_batch, StatsdEncoder::new(Format::DogStatsd)).await;
    assert_eq!(
        std::str::from_utf8(&captured).expect("ascii output"),
        "x:2|c|#team:a,team:b",
        "the summed counter should still carry both tag values, in wire order"
    );
}

/// `influxdb_out` renders only the last element of a repeated tag key and counts it: line
/// protocol's tag set is a map (`render_tag_suffix`'s "Multi-value tags: last-value-wins,
/// counted"). It lives here because this file already decodes real statsd lines.
#[test]
fn influxdb_out_renders_the_last_element_of_a_multi_valued_tag_and_counts_it() {
    let batch = direct_batch(b"x:1|c|#team:a,team:b");
    let mut encoder = InfluxLineEncoder::default();
    let body = encoder.encode(&batch).expect("encoding should succeed");
    let line = std::str::from_utf8(&body).expect("ascii output");
    assert!(line.contains("team=b"), "the last tag element should win: {line}");
    assert_eq!(
        encoder.multi_value_tags, 1,
        "one multi-valued tag attribute reached the wire, collapsed to its last element"
    );
}

// ---- Relative gauges (opt-in `relative_gauges: true`) -------------------------------------------

/// The `temp:0|g` then `temp:-5|g` idiom for a negative gauge (`logit_outputs::statsd`'s
/// "Negative absolute gauges"). Needs `relative_gauges: true`: the second line decodes as a
/// `GaugeDelta`, which the encoder otherwise drops as unresolved.
#[tokio::test]
async fn negative_absolute_gauge_idiom_round_trips_with_relative_gauges_enabled() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "negative-absolute-gauge-idiom", || {
        StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true)
    })
    .await;
}

/// `conns:+1|g` decodes to an unresolved `GaugeDelta(1.0)` and, with `relative_gauges: true`,
/// relays natively.
#[tokio::test]
async fn relative_gauge_delta_round_trips_with_relative_gauges_enabled() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "relative-gauge-delta", || {
        StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true)
    })
    .await;
}

// ---- `format: statsd` dialect normalizations (normalization (6)), one-way lossy ----------------

/// Asserts the wire bytes only: `format: statsd` can't express what it drops.
async fn assert_dialect_output(harness: &mut Harness, fixture: &str) {
    let raw = read_fixture(fixture, "in");
    let batch = direct_batch(&raw);
    let expected = read_fixture(fixture, "expected");
    let captured = harness.capture_only(&batch, StatsdEncoder::new(Format::Statsd)).await;
    assert_eq!(
        captured, expected,
        "{fixture}: format: statsd output should match .expected (module doc (6))"
    );
}

#[tokio::test]
async fn statsd_dialect_splits_a_multi_value_timer_into_one_line_per_value() {
    let mut harness = Harness::new().await;
    assert_dialect_output(&mut harness, "statsd-dialect-multi-value-timer-split").await;
}

#[tokio::test]
async fn statsd_dialect_normalizes_h_to_ms() {
    let mut harness = Harness::new().await;
    assert_dialect_output(&mut harness, "statsd-dialect-h-normalizes-to-ms").await;
}

#[tokio::test]
async fn statsd_dialect_drops_container_id_and_timestamp() {
    let mut harness = Harness::new().await;
    assert_dialect_output(&mut harness, "statsd-dialect-drops-container-and-timestamp").await;
}

#[tokio::test]
async fn statsd_dialect_drops_the_tag_segment_entirely() {
    let mut harness = Harness::new().await;
    assert_dialect_output(&mut harness, "statsd-dialect-drops-tags").await;
}

/// Normalization (6): an event or service check is dropped and counted under `format: statsd`.
/// Calls `StatsdEncoder::encode_into` directly: `StatsdOutput::send` returns early on an empty
/// `MessageBuf`, so there'd be no datagram for `capture_only` to receive.
#[tokio::test]
async fn events_and_service_checks_produce_no_output_and_are_counted_under_plain_statsd() {
    for fixture in ["dogstatsd-event", "dogstatsd-service-check"] {
        let raw = read_fixture(fixture, "in");
        let batch = direct_batch(&raw);
        let mut encoder = StatsdEncoder::new(Format::Statsd);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(&batch, &mut out);
        assert!(out.is_empty(), "{fixture}: format: statsd should emit no lines for this event");
        assert_eq!(
            stats.dropped_dialect_events, 1,
            "{fixture}: the drop should be counted, not silent"
        );
    }
}

// ---- Sanitizer substitution (normalization (5)) -------------------------------------------------

/// A `|` in a set member becomes `_`, and the result decodes cleanly on the live `statsd_in`. The
/// batch is hand-built because no decode can put a `|` in a member.
#[tokio::test]
async fn sanitizer_substitution_in_a_set_member_is_sanitized_and_counted() {
    let event = Event::metric(
        0,
        logit_core::AttrMap::new(),
        logit_core::MetricRecord::new(
            logit_core::interner::intern("uniq.paths"),
            logit_core::MetricKind::SetMembers(vec![Bytes::from_static(b"a|b")]),
        ),
    );
    let batch = EventBatch {
        resource: std::sync::Arc::new(logit_core::Resource::default()),
        scope: None,
        events: vec![event],
    };

    let mut harness = Harness::new().await;
    let (captured, decoded) =
        harness.round_trip(&batch, || StatsdEncoder::new(Format::DogStatsd)).await;
    assert_eq!(captured, b"uniq.paths:a_b|s", "the forbidden '|' should be substituted with '_'");
    match &decoded.events[0].metrics[0].kind {
        logit_core::MetricKind::SetMembers(members) => {
            assert_eq!(members, &vec![Bytes::from_static(b"a_b")])
        }
        other => panic!("expected SetMembers, got {other:?}"),
    }
}

// ---- `statsd_in -> aggregate -> statsd_out`, exact ----------------------------------------------

/// An `aggregate` that retains raw shapes (`distributions: samples`, `sets: members`) relays
/// losslessly: every timer value on one multi-value `ms` line, and one `|s` line per member
/// (normalization (7)). The lines are compared sorted, because the `Aggregator`'s series live in
/// a `std::collections::HashMap` whose iteration order is randomized per process.
#[tokio::test]
async fn statsd_in_aggregate_statsd_out_relay_is_exact() {
    let mut harness = Harness::new().await;

    let raw = b"req.latency:10|ms\nreq.latency:20|ms\nreq.latency:30|ms\nuniq.users:alice|s\nuniq.users:bob|s";
    let batch = harness.send_raw_and_decode(raw).await;
    assert_eq!(batch.events.len(), 5, "one event per line");

    let mut aggregator = Aggregator::new(Duration::from_secs(10))
        .with_distributions(Distributions::Samples, 1000)
        .with_sets(Sets::Members, 1000);

    let resource = batch.resource.clone();
    let mut forwarded = Vec::new();
    for mut event in batch.events {
        if aggregator.process(&resource, &mut event) {
            forwarded.push(event);
        }
    }
    assert!(
        forwarded.is_empty(),
        "every metric here is a mergeable Samples/SetMembers record and should be absorbed, not \
         passed through: {forwarded:?}"
    );

    let mut flushed = aggregator.flush(1_700_000_000_000_000_000);
    assert_eq!(flushed.len(), 1, "one (resource, scope) group");
    let (flush_resource, flush_scope, events) = flushed.remove(0);
    assert!(flush_scope.is_none(), "statsd carries no scope concept");
    let out_batch = EventBatch {
        resource: flush_resource,
        scope: flush_scope,
        events: events.into_iter().map(|(event, _links)| event).collect(),
    };
    assert_eq!(out_batch.events.len(), 2, "one flushed event per series");

    // Raw-shape accumulators are tumbling, like their sketch/estimate counterparts.
    assert!(aggregator.flush(1_700_000_010_000_000_000).is_empty());

    let captured = harness.capture_only(&out_batch, StatsdEncoder::new(Format::DogStatsd)).await;
    let mut captured_lines: Vec<&str> =
        std::str::from_utf8(&captured).expect("ascii output").split('\n').collect();
    captured_lines.sort_unstable();
    let mut expected_lines =
        vec!["req.latency:10:20:30|ms", "uniq.users:alice|s", "uniq.users:bob|s"];
    expected_lines.sort_unstable();
    assert_eq!(
        captured_lines, expected_lines,
        "the sink should emit one multi-value ms line with every timer value (rate preserved, \
         here the default 1.0) and one |s line per distinct member"
    );
}

// ---- `|T` carrier survives `aggregate`'s flush-time rebuild -------------------------------------

/// The `|T` value survives `aggregate`, which sets `Event::timestamp` to the flush clock: the
/// sink must read the `statsd.timestamp` carrier's `Value::U64`, which rides on the series key,
/// never `Event::timestamp`.
#[tokio::test]
async fn statsd_in_aggregate_statsd_out_relay_preserves_the_wire_timestamp() {
    let mut harness = Harness::new().await;

    let batch = harness.send_raw_and_decode(b"hits:1|c|T1700000000").await;
    assert_eq!(batch.events.len(), 1);

    let mut aggregator = Aggregator::new(Duration::from_secs(10));
    let resource = batch.resource.clone();
    let mut forwarded = Vec::new();
    for mut event in batch.events {
        if aggregator.process(&resource, &mut event) {
            forwarded.push(event);
        }
    }
    assert!(forwarded.is_empty(), "a delta Counter is always absorbed by aggregate");

    // A different second from the wire value, so a sink reading `Event::timestamp` would emit
    // 1_800_000_000 instead of 1_700_000_000.
    let mut flushed = aggregator.flush(1_800_000_000_000_000_000);
    assert_eq!(flushed.len(), 1, "one (resource, scope) group");
    let (flush_resource, flush_scope, events) = flushed.remove(0);
    let out_batch = EventBatch {
        resource: flush_resource,
        scope: flush_scope,
        events: events.into_iter().map(|(event, _links)| event).collect(),
    };
    assert_eq!(out_batch.events.len(), 1);

    let captured = harness.capture_only(&out_batch, StatsdEncoder::new(Format::DogStatsd)).await;
    assert_eq!(std::str::from_utf8(&captured).expect("ascii output"), "hits:1|c|T1700000000");
}

/// Two lines differing only in their `|T` value are distinct series, since the carrier is part
/// of the series key, so they flush as two lines.
#[tokio::test]
async fn statsd_in_aggregate_statsd_out_relay_keeps_distinct_timestamps_as_distinct_series() {
    let mut harness = Harness::new().await;

    let raw = b"hits:1|c|T1700000000\nhits:1|c|T1700000100";
    let batch = harness.send_raw_and_decode(raw).await;
    assert_eq!(batch.events.len(), 2, "one event per line");

    let mut aggregator = Aggregator::new(Duration::from_secs(10));
    let resource = batch.resource.clone();
    for mut event in batch.events {
        assert!(
            !aggregator.process(&resource, &mut event),
            "a delta Counter is always absorbed by aggregate"
        );
    }

    let mut flushed = aggregator.flush(1_800_000_000_000_000_000);
    assert_eq!(flushed.len(), 1, "one (resource, scope) group");
    let (flush_resource, flush_scope, events) = flushed.remove(0);
    let out_batch = EventBatch {
        resource: flush_resource,
        scope: flush_scope,
        events: events.into_iter().map(|(event, _links)| event).collect(),
    };
    assert_eq!(out_batch.events.len(), 2, "distinct |T values must stay distinct series");

    let captured = harness.capture_only(&out_batch, StatsdEncoder::new(Format::DogStatsd)).await;
    let mut lines: Vec<&str> =
        std::str::from_utf8(&captured).expect("ascii output").split('\n').collect();
    lines.sort_unstable();
    let mut expected = vec!["hits:1|c|T1700000000", "hits:1|c|T1700000100"];
    expected.sort_unstable();
    assert_eq!(lines, expected, "both |T values must survive the flush as separate lines");
}

// ---- `statsd_in -> aggregate -> statsd_out`, a service check ------------------------------------

/// A service check is a `MetricKind::Gauge`, so `aggregate` absorbs it. Its
/// `statsd.service_check.*` carriers ride on the series key, so `_sc|name|status` and its
/// `d:`/`m:` fields survive the window unchanged.
#[tokio::test]
async fn statsd_in_aggregate_statsd_out_relay_is_exact_for_a_service_check() {
    let mut harness = Harness::new().await;

    let raw =
        b"_sc|Redis connection|2|d:1700000000|#env:dev|m:Redis connection timed out after 10s";
    let batch = harness.send_raw_and_decode(raw).await;
    assert_eq!(batch.events.len(), 1);

    let mut aggregator = Aggregator::new(Duration::from_secs(10));
    let resource = batch.resource.clone();
    let mut forwarded = Vec::new();
    for mut event in batch.events {
        if aggregator.process(&resource, &mut event) {
            forwarded.push(event);
        }
    }
    assert!(forwarded.is_empty(), "a Gauge (the service check's own shape) is always absorbed");

    let mut flushed = aggregator.flush(1_800_000_000_000_000_000);
    assert_eq!(flushed.len(), 1, "one (resource, scope) group");
    let (flush_resource, flush_scope, events) = flushed.remove(0);
    let out_batch = EventBatch {
        resource: flush_resource,
        scope: flush_scope,
        events: events.into_iter().map(|(event, _links)| event).collect(),
    };
    assert_eq!(out_batch.events.len(), 1, "one flushed event for the one series");

    let captured = harness.capture_only(&out_batch, StatsdEncoder::new(Format::DogStatsd)).await;
    assert_eq!(
        std::str::from_utf8(&captured).expect("ascii output"),
        "_sc|Redis connection|2|d:1700000000|#env:dev|m:Redis connection timed out after 10s",
        "the _sc line should survive the window exactly, d: and m: included"
    );
}

// ---- transport: tcp -----------------------------------------------------------------------

/// `statsd_out(transport: tcp) -> statsd_in(transport: tcp)` over the UDP fixture corpus, plus two
/// stream-only cases: a line split across two writes, and a line starting with an ASCII digit.
mod tcp {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener as TokioTcpListener, TcpStream};

    /// TCP twin of [`Harness`]. The capture listener reads each connection to EOF, which works
    /// because each round trip uses a fresh `StatsdOutput::tcp` and drops it after sending.
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

            let mut input = StatsdInput::tcp("127.0.0.1:0");
            input.bind().await.expect("binding the tcp statsd_in listener");
            let input_addr = input.local_addr().expect("bind() should leave a real address behind");

            let (tx, rx) = mpsc::channel(16);
            let sink = Fanout::new(vec![tx]);
            tokio::spawn(async move {
                let _ = input.run(sink).await;
            });

            Self { capture_addr, capture_rx, input_addr, rx }
        }

        /// TCP twin of [`Harness::round_trip`].
        async fn round_trip(
            &mut self,
            batch: &EventBatch,
            encoder: impl Fn() -> StatsdEncoder,
        ) -> (Vec<u8>, EventBatch) {
            let mut to_capture =
                StatsdOutput::tcp(self.capture_addr.to_string(), Duration::from_secs(2))
                    .with_encoder(encoder());
            to_capture.send(batch).await.expect("send to the capture listener");
            drop(to_capture); // closes the connection, EOFing the capture task's read_to_end
            let framed = tokio::time::timeout(Duration::from_millis(500), self.capture_rx.recv())
                .await
                .expect("capture listener should receive the frame")
                .expect("the capture channel should not have closed");

            let mut to_input =
                StatsdOutput::tcp(self.input_addr.to_string(), Duration::from_secs(2))
                    .with_encoder(encoder());
            to_input.send(batch).await.expect("send to the live statsd_in");
            // Dropping it EOFs the listener's connection task, which flushes whatever it has
            // accumulated straight away rather than on the 100ms batch timer.
            drop(to_input);
            let delivered = tokio::time::timeout(Duration::from_millis(500), self.rx.recv())
                .await
                .expect("statsd_in should decode and forward the batch")
                .expect("the Fanout channel should not have closed");
            let mut decoded = logit_pipeline::unwrap_batch(delivered);
            normalize_receipt_time(&mut decoded);
            (framed, decoded)
        }
    }

    /// Strips the trailing `LF` that TCP framing adds after the last line, asserting it's there.
    /// It's the only byte difference between the TCP and UDP output.
    fn strip_lf_framing(frame: &[u8]) -> &[u8] {
        let (last, rest) = frame.split_last().expect("a TCP frame is never empty");
        assert_eq!(*last, b'\n', "statsd_out's TCP transport terminates every line with LF");
        rest
    }

    /// TCP twin of `assert_byte_for_byte`, against the same `.expected` bytes.
    async fn assert_byte_for_byte_tcp(
        harness: &mut TcpHarness,
        fixture: &str,
        encoder: impl Fn() -> StatsdEncoder,
    ) {
        let raw = read_fixture(fixture, "in");
        let batch = direct_batch(&raw); // already receipt-time normalized
        let expected = expected_bytes(fixture, &raw);
        let (framed, decoded) = harness.round_trip(&batch, encoder).await;
        assert_eq!(
            strip_lf_framing(&framed),
            expected.as_slice(),
            "{fixture}: the LF-framed lines should match .expected (modulo the module doc's \
             permitted normalizations)"
        );
        assert_eq!(
            decoded, batch,
            "{fixture}: decode(sink_output) should equal the original decode, as a whole EventBatch"
        );
    }

    /// The UDP corpus over TCP, byte for byte. One test so the corpus shares one harness.
    #[tokio::test]
    async fn fixture_corpus_round_trips_over_tcp() {
        let mut harness = TcpHarness::new().await;
        let cases: &[&str] = &[
            // The DogStatsD docs' own worked examples.
            "dogstatsd-counter",
            "dogstatsd-gauge",
            "dogstatsd-histogram-sampled",
            "dogstatsd-set",
            "dogstatsd-counter-tag",
            "dogstatsd-distribution-tag",
            "dogstatsd-gauge-timestamp",
            "dogstatsd-counter-container-id",
            "dogstatsd-event",
            "dogstatsd-service-check",
            // The hand-written grammar/normalization corpus.
            "multi-value-timer",
            "bare-tag-counter",
            "tag-value-with-colon",
            "packed-multi-line-datagram",
            "all-segments-together",
            "multi-tag-preserves-wire-order",
            "dogstatsd-event-all-fields",
            "event-text-with-pipe-and-escaped-newline",
            "event-multibyte-title-lengths",
            "event-title-contains-pipe",
            "service-check-all-fields",
            "service-check-no-message",
            "packed-datagram-counter-event-service-check",
            "event-text-trailing-space",
            "service-check-message-trailing-space",
            // The `.expected`-differs-from-`.in` normalizations.
            "sampled-counter-rate-folded",
            "explicit-rate-one-omitted",
            "number-formatting-trailing-zeros",
            "event-fields-reordered-canonicalized",
            "repeated-tag-key-round-trips",
            "repeated-tag-exact-duplicate-deduped",
            "bare-tag-exact-duplicate-deduped",
        ];
        for name in cases {
            assert_byte_for_byte_tcp(&mut harness, name, || StatsdEncoder::new(Format::DogStatsd))
                .await;
        }
    }

    /// Pins `statsd_in`'s LF framing: a statsd line may begin with an ASCII digit, which RFC 6587's
    /// auto-detecting framing (`syslog_in`'s) would read as an octet count. Uses a raw client, so
    /// the bytes on the wire are the ones asserted.
    #[tokio::test]
    async fn a_raw_client_line_starting_with_a_digit_decodes_as_a_metric_name() {
        let mut input = StatsdInput::tcp("127.0.0.1:0");
        input.bind().await.expect("binding the tcp statsd_in listener");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        let (tx, mut rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });

        let mut client = TcpStream::connect(addr).await.expect("the listener should accept");
        client.write_all(b"1.hits:7|c\n").await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("statsd_in should decode and forward the line")
            .expect("the Fanout channel should not have closed");
        let batch = logit_pipeline::unwrap_batch(delivered);
        assert_eq!(batch.events.len(), 1);
        assert_eq!(logit_core::interner::resolve(batch.events[0].metrics[0].name), "1.hits");
    }

    /// A line whose `\n` arrives in a second write is one event: the framer buffers across reads.
    #[tokio::test]
    async fn a_line_split_across_two_writes_is_one_event() {
        let mut input = StatsdInput::tcp("127.0.0.1:0");
        input.bind().await.expect("binding the tcp statsd_in listener");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        let (tx, mut rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });

        let mut client = TcpStream::connect(addr).await.expect("the listener should accept");
        client.write_all(b"halves.joined:4").await.unwrap();
        client.flush().await.unwrap();
        // The pause is the test: back-to-back writes can coalesce into one `read_buf`, which
        // would pass without the framer ever having buffered across reads. The unit twin in
        // `crates/logit-inputs/src/statsd.rs` does the same.
        tokio::time::sleep(Duration::from_millis(50)).await;
        client.write_all(b"2|c\n").await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("statsd_in should decode and forward the reassembled line")
            .expect("the Fanout channel should not have closed");
        let batch = logit_pipeline::unwrap_batch(delivered);
        assert_eq!(batch.events.len(), 1, "one line, not two malformed halves");
        assert_eq!(logit_core::interner::resolve(batch.events[0].metrics[0].name), "halves.joined");
    }
}

// ---- transport: tls -------------------------------------------------------------------------

/// `statsd_out` -> `statsd_in` over TLS: server TLS, mutual TLS, and the wrong-CA negative case,
/// driven by the real sink's `tls:` block (`docs/adr/statsd-output.md`'s "Amendment: TLS").
///
/// The two wrong-CA tests aren't duplicates. The `statsd_out` one asserts the sink-side `Fault`;
/// the raw `tokio_rustls` one asserts that a refused handshake leaves the listener serving other
/// clients, which the sink can't observe.
mod tls {
    use super::*;
    use logit_inputs::tcp::TlsServerSettings;
    use logit_outputs::statsd::TlsClientSettings;
    use logit_pipeline::{classify, Fault};
    use rustls_pki_types::pem::PemObject;
    use rustls_pki_types::{CertificateDer, PrivateKeyDer};
    use std::sync::Arc;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpStream;

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls`, two levels up from `crates/logit-cli`.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// Spawns a TLS-terminating TCP `statsd_in`, returning its bound address and the receiver
    /// its decoded batches land on.
    async fn spawn_tls_input(
        settings: &TlsServerSettings,
    ) -> (SocketAddr, mpsc::Receiver<Delivered>) {
        let mut input = StatsdInput::tcp("127.0.0.1:0")
            .with_tls(settings, &testdata_dir())
            .expect("a tls: block is legal on a tcp statsd_in");
        input.bind().await.expect("binding the tls statsd_in listener");
        let addr = input.local_addr().expect("bind() should leave a real address behind");
        let (tx, rx) = mpsc::channel(16);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move {
            let _ = input.run(sink).await;
        });
        (addr, rx)
    }

    /// A `tokio-rustls` client trusting only `ca_file` under `testdata/tls`, presenting
    /// `client_cert` (`(cert, key)`) if given. `other-ca.pem` makes a wrong-CA case a trust
    /// failure rather than a name mismatch.
    fn connector(ca_file: &str, client_cert: Option<(&str, &str)>) -> tokio_rustls::TlsConnector {
        let dir = testdata_dir();
        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(dir.join(ca_file))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        roots.add_parsable_certificates(ca);
        let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots);
        let cfg = match client_cert {
            Some((cert_file, key_file)) => {
                let chain: Vec<CertificateDer<'static>> =
                    CertificateDer::pem_file_iter(dir.join(cert_file))
                        .unwrap()
                        .collect::<Result<_, _>>()
                        .unwrap();
                let key = PrivateKeyDer::from_pem_file(dir.join(key_file)).unwrap();
                builder.with_client_auth_cert(chain, key).unwrap()
            }
            None => builder.with_no_client_auth(),
        };
        tokio_rustls::TlsConnector::from(Arc::new(cfg))
    }

    /// `testdata/tls/server.pem` carries a `localhost` SAN (`testdata/tls/README.md`), so that is
    /// the name every TLS client here presents.
    fn server_name() -> rustls_pki_types::ServerName<'static> {
        rustls_pki_types::ServerName::try_from("localhost").unwrap()
    }

    fn server_settings(client_ca_file: Option<&str>) -> TlsServerSettings {
        TlsServerSettings {
            cert_file: "server.pem".to_string(),
            key_file: "server.key".to_string(),
            client_ca_file: client_ca_file.map(str::to_string),
        }
    }

    /// Writes `line` over a completed TLS handshake and returns the batch `statsd_in` decoded.
    /// Dropping the connection EOFs the listener's connection task, which flushes immediately
    /// rather than on its 100ms timer.
    async fn send_over_tls(
        connector: &tokio_rustls::TlsConnector,
        addr: SocketAddr,
        rx: &mut mpsc::Receiver<Delivered>,
        line: &[u8],
    ) -> EventBatch {
        let stream = TcpStream::connect(addr).await.expect("the listener should accept");
        let mut client =
            tokio::time::timeout(Duration::from_secs(5), connector.connect(server_name(), stream))
                .await
                .expect("the TLS handshake should complete within 5s")
                .expect("the TLS handshake should succeed");
        client.write_all(line).await.unwrap();
        client.flush().await.unwrap();
        drop(client);

        let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("statsd_in should decode and forward the line")
            .expect("the Fanout channel should not have closed");
        logit_pipeline::unwrap_batch(delivered)
    }

    fn metric_name(batch: &EventBatch) -> &'static str {
        logit_core::interner::resolve(batch.events[0].metrics[0].name)
    }

    /// Sends `batch` through a TLS `statsd_out` and returns `statsd_in`'s normalized decode. The
    /// endpoint is `localhost`, not `127.0.0.1`, because the sink's SNI must match
    /// `server.pem`'s SAN.
    async fn round_trip_over_tls(
        addr: SocketAddr,
        rx: &mut mpsc::Receiver<Delivered>,
        settings: TlsClientSettings,
        batch: &EventBatch,
    ) -> EventBatch {
        let mut output =
            StatsdOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(&settings, &testdata_dir())
                .expect("a tls: block is legal on a tcp statsd_out");
        output.send(batch).await.expect("send over TLS should succeed");
        // Dropping it EOFs the listener's connection task, which flushes what it has accumulated
        // straight away rather than on the 100ms batch timer.
        drop(output);

        let delivered = tokio::time::timeout(Duration::from_millis(500), rx.recv())
            .await
            .expect("statsd_in should decode and forward the batch")
            .expect("the Fanout channel should not have closed");
        let mut decoded = logit_pipeline::unwrap_batch(delivered);
        normalize_receipt_time(&mut decoded);
        decoded
    }

    /// Server TLS only (no `client_ca_file`): the relay is a fixed point, tags included.
    #[tokio::test]
    async fn server_tls_round_trips_a_batch() {
        let (addr, mut rx) = spawn_tls_input(&server_settings(None)).await;
        let sent = direct_batch(b"over.tls:3|c|#env:prod\n");
        let decoded = round_trip_over_tls(
            addr,
            &mut rx,
            TlsClientSettings { ca_file: Some("ca.pem".to_string()), ..Default::default() },
            &sent,
        )
        .await;

        assert_eq!(decoded.events.len(), 1);
        assert_eq!(metric_name(&decoded), "over.tls");
        assert_eq!(decoded.events[0].attributes.get("env").and_then(Value::as_str), Some("prod"));
        assert_eq!(decoded, sent, "TLS changes the transport, not the relayed batch");
    }

    /// Mutual TLS: `statsd_in` requires a client certificate chaining to `ca.pem`
    /// (`client_ca_file`), `statsd_out` presents `client.pem`/`client.key` -- both signed by the
    /// same test CA (`testdata/tls/regen.sh`).
    #[tokio::test]
    async fn mutual_tls_round_trips_a_batch() {
        let (addr, mut rx) = spawn_tls_input(&server_settings(Some("ca.pem"))).await;
        let sent = direct_batch(b"mutual.tls:1|c\n");
        let decoded = round_trip_over_tls(
            addr,
            &mut rx,
            TlsClientSettings {
                ca_file: Some("ca.pem".to_string()),
                cert_file: Some("client.pem".to_string()),
                key_file: Some("client.key".to_string()),
                insecure_skip_verify: false,
            },
            &sent,
        )
        .await;
        assert_eq!(metric_name(&decoded), "mutual.tls");
        assert_eq!(decoded, sent);
    }

    /// A `statsd_out` trusting `other-ca.pem` fails its own certificate verification before any
    /// batch byte is written, so the error is `Fault::Clean`. That's deterministic because a
    /// server-cert rejection happens inside `TlsConnector::connect`; a client-cert rejection,
    /// under TLS 1.3, is one this write-only sink never sees.
    #[tokio::test]
    async fn a_statsd_out_trusting_the_wrong_ca_is_refused_and_classified_clean() {
        let (addr, _rx) = spawn_tls_input(&server_settings(None)).await;
        let mut output =
            StatsdOutput::tcp(format!("localhost:{}", addr.port()), Duration::from_secs(2))
                .with_tls(
                    &TlsClientSettings {
                        ca_file: Some("other-ca.pem".to_string()),
                        ..Default::default()
                    },
                    &testdata_dir(),
                )
                .expect("a tls: block is legal on the tcp transport");

        let err = output
            .send(&direct_batch(b"never.arrives:1|c\n"))
            .await
            .expect_err("an untrusted CA must fail the handshake");
        assert_eq!(classify(&err), Fault::Clean);
    }

    /// The listener-side half: after a raw client trusting `other-ca.pem` is refused, the
    /// listener keeps serving other clients.
    #[tokio::test]
    async fn a_client_trusting_the_wrong_ca_is_refused_and_the_listener_keeps_serving() {
        let (addr, mut rx) = spawn_tls_input(&server_settings(None)).await;

        let wrong = connector("other-ca.pem", None);
        let stream = TcpStream::connect(addr).await.expect("the listener should accept");
        let refused =
            tokio::time::timeout(Duration::from_secs(5), wrong.connect(server_name(), stream))
                .await
                .expect("the handshake should resolve within 5s");
        assert!(refused.is_err(), "a client trusting only other-ca.pem must not complete");

        let batch =
            send_over_tls(&connector("ca.pem", None), addr, &mut rx, b"still.serving:1|c\n").await;
        assert_eq!(metric_name(&batch), "still.serving");
    }
}
