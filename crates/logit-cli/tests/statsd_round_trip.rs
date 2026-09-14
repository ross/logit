//! `statsd_out` -> `statsd_in` round trip, over real UDP sockets -- the statsd counterpart to
//! `syslog_round_trip.rs`/`otlp_round_trip.rs`/`logit_round_trip.rs`. Lives here (not a
//! dev-dependency cycle between `logit-inputs`/`logit-outputs`) for the same reason those do:
//! `logit-cli` already depends on both as ordinary dependencies.
//!
//! `docs/plans/lossless-transit.md`'s W3 workstream, "Tests" bullet.
//!
//! ## Fixture corpus (`tests/fixtures/statsd/`)
//!
//! One file pair per case: `<name>.in` (the raw line(s), exactly as a real client would put them
//! on the wire) and `<name>.expected` (the exact bytes `statsd_out` must emit for that line's
//! decode, or the literal marker [`SAME_AS_INPUT`] when the sink's own canonicalization happens to
//! reproduce the input verbatim) -- the same convention `syslog_round_trip.rs`'s corpus uses. The
//! DogStatsD-docs cases (`dogstatsd-*`) are the worked examples from Datadog's own DogStatsD
//! protocol reference (`dogstatsd-event`/`dogstatsd-service-check` included -- the docs' own event
//! and service check examples); everything else is hand-written to exercise a specific grammar
//! corner or normalization, including the DogStatsD events/service checks W6 adds:
//! `dogstatsd-event-all-fields`/`service-check-all-fields` (every optional field, already in
//! canonical order), `event-text-with-pipe-and-escaped-newline` (`TEXT` containing `|`, `:`, and a
//! `\n` escape), `event-multibyte-title-lengths` (a multi-byte UTF-8 title, pinning that
//! `_e{TITLE_LEN,...}` counts bytes, not chars), `event-title-contains-pipe` (a title containing a
//! literal `|`, which the length prefix -- not a `|` scan -- delimits), `service-check-no-message`
//! (no `m:` field), `packed-datagram-counter-event-service-check` (an ordinary metric, an
//! event, and a service check sharing one datagram), `event-text-trailing-space` (`TEXT`'s last
//! byte is a space, immediately followed by the `.in` file's own trailing `\n` -- pins that
//! `decode_into` doesn't trim it off), and `service-check-message-trailing-space` (same pin for
//! an `m:` message, which consumes the rest of the line verbatim).
//!
//! ## Permitted normalizations (recorded here, per [`docs/adr/lossless-transit.md`])
//!
//! `statsd_out` is not byte-identical to its input in general -- these are the specific,
//! documented ways it differs, each with the reason it's permitted rather than a lossiness bug:
//!
//! 1. **Counter sample-rate folding.** `hits:1|c|@0.1` decodes to `Counter(10.0)` (the value
//!    already extrapolated by `1 / sample_rate` at decode time -- `logit_inputs::statsd` has
//!    always done this for `c`) and relays as `hits:10|c`, with no `@rate` on the way out: a
//!    statsd counter's wire form has no concept of "this was extrapolated," so there is nothing
//!    left to preserve. Exercised by `sampled-counter-rate-folded`. **This is not folding for a
//!    timer/histogram/distribution** -- a `Samples` record's `sample_rate` is real, un-applied
//!    information (`logit_outputs::statsd`'s module doc, "Sample rate: never for a counter, real
//!    for `Samples`") and survives the relay untouched; see (2) and (6) below.
//! 2. **`@1.0` (or any sample rate that normalizes to exactly `1.0`) is omitted.** `1.0` is the
//!    grammar's own default, so writing it back adds nothing; `render_samples` only emits `@rate`
//!    when `rate != 1.0`. Exercised by `explicit-rate-one-omitted`.
//! 3. **Tags are emitted in `AttrMap` order.** `AttrMap` iterates in sorted-`Symbol` (global
//!    intern) order, not lexicographic or wire order in general -- but for two tag keys neither of
//!    which was ever interned before this decode, `Symbol` assignment happens in the order each is
//!    first seen, which for one `#k1:v1,k2:v2` segment is simply wire order. So a line whose tag
//!    keys are novel to the process relays with its tags in the same relative order they arrived
//!    in, even when that order isn't alphabetical -- exercised (deliberately using
//!    non-alphabetical key names) by `multi-tag-preserves-wire-order`.
//! 4. **Number formatting (`push_float`).** A value's canonical rendering is `f64`'s `Display`
//!    (`write!("{v}")`), not whatever the origin wrote: `0.500` decodes and relays as `0.5`, since
//!    only the numeric value round-trips, not its textual spelling. Exercised by
//!    `number-formatting-trailing-zeros`.
//! 5. **Sanitizer substitutions for injection safety.** A metric name or tag key containing any of
//!    `: | @ # , \n \r \0`, another ASCII control character, or whitespace is substituted with
//!    `_`; a tag value forbids the same set except `:` (deliberately preserved, since only a tag's
//!    first colon is ever significant); a `SetMembers` member forbids only `:`, `|`, and control
//!    characters -- its own, narrower rule, since `@`/`#`/`,`/whitespace are none of them
//!    delimiter-sensitive in a member's own wire position and must survive untouched or distinct
//!    members would collide. **Most of this is reachable from a real decode, not hypothetical**:
//!    `statsd_in`'s decoder only trims a line's leading/trailing whitespace
//!    (`line.trim_end_matches('\r').trim()`) -- it never strips or rejects an embedded delimiter
//!    byte mid-line, so an embedded space, `@`, `#`, or `,` inside a name survives decode unchanged
//!    (`a#b:1|c` decodes to the name `"a#b"`, not an error), and the same bytes inside a member
//!    survive decode *and* this sink's own encode, unsubstituted. Exercised by
//!    `sanitizer-name-hash` (`a#b:1|c` -> `a_b:1|c`), `sanitizer-tag-value-at`
//!    (`x:1|c|#k:a@b` -> `x:1|c|#k:a_b`), and `sanitizer-member-space`
//!    (`users:a b|s` -> `users:a b|s`, [`SAME_AS_INPUT`]) for the preserved case. The one byte that
//!    genuinely can't reach a member through a real decode is `|` itself -- statsd's own grammar
//!    treats it as a new segment before the decoder ever gets far enough to hand it to a member --
//!    so that case alone is still exercised against a hand-built `EventBatch` rather than a decoded
//!    fixture, by `sanitizer_substitution_in_a_set_member_is_sanitized_and_counted` below.
//! 6. **`format: statsd` (the classic dialect) drops what only DogStatsD's grammar can express.**
//!    Selected by the operator on the sink, so this is the "sink-configured dialect change"
//!    normalization by name, not loss:
//!    - The `|#k:v,...` tag segment is omitted entirely (never an empty `|#`).
//!    - A `Samples` record's multi-value line (`name:v1:v2:v3|ms`) splits into one `name:v|ms`
//!      line per value -- the "splitting a multi-value statsd line ... into several lines"
//!      normalization, applied to the encode side here (the decode side already applies it to `s`
//!      lines the other way, see `SetMembers` below). `sample_rate`, when not `1.0`, is repeated
//!      on every split line.
//!    - A timer's own wire-type letter collapses from `h`/`d` to the classic grammar's `ms`.
//!    - `|c:<container-id>`/`|T<timestamp>` have no plain-statsd equivalent at all and are dropped.
//!    - A DogStatsD event or service check has no `_e`/`_sc` wire form at all under the classic
//!      grammar, so the whole event is dropped -- not normalized into anything -- and counted
//!      `EncodeStats::dropped_dialect_events`.
//!
//!    Exercised by `statsd-dialect-multi-value-timer-split`, `statsd-dialect-h-normalizes-to-ms`,
//!    `statsd-dialect-drops-container-and-timestamp`, `statsd-dialect-drops-tags`, and
//!    `events_and_service_checks_produce_no_output_and_are_counted_under_plain_statsd` (the last
//!    one bypasses the UDP harness, calling [`logit_outputs::statsd::StatsdEncoder::encode_into`]
//!    directly, since a wholly-dropped batch never sends a datagram for `capture_only` to receive
//!    -- see that test). These are **one-way lossy by design** (the whole point of (6) is that the
//!    classic dialect can't express what was dropped), so unlike every other case in this file they
//!    are asserted against their `.expected` wire bytes (or, for the events/service-checks case,
//!    the returned [`logit_outputs::statsd::EncodeStats`]) only -- no decoded-batch equality claim
//!    is made for them.
//! 7. **`SetMembers` always splits one member per line, in both dialects.** The classic grammar has
//!    no multi-value extension for sets the way DogStatsD's timers get, so `statsd_in`'s own
//!    multi-value `s` decode (`name:m1:m2|s`, one event) never has a matching multi-value encode:
//!    `statsd_out` always emits one `name:<member>|s` line per member. This is the "splitting a
//!    multi-value statsd line ... into several lines" normalization applied on the *encode* side
//!    (`(6)`'s second bullet is the same normalization on decode's own multi-value `ms`/`h`/`d`
//!    form). Exercised by `dogstatsd-set` (single member, so trivially one line) and by the
//!    `statsd_in -> aggregate -> statsd_out` test below (two distinct members, two lines).
//! 8. **A repeated tag key's exact-duplicate tokens dedupe.** `#team:a,team:b` is two live tags
//!    now -- `statsd_in`'s `insert_tags` folds a repeated key into a `Value::Array` in wire order
//!    (`crates/logit-inputs/src/statsd.rs`'s "DogStatsD tags" section) and `statsd_out` expands it
//!    back to one tag per element, so that case is byte-for-byte, not a normalization (see
//!    `repeated-tag-key-round-trips` below). What *does* still collapse is an **exact** duplicate
//!    token, `#team:a,team:a` -> `#team:a` (and a bare `#urgent,urgent` -> `#urgent`) -- the
//!    Datadog agent's own dedupe rule, applied at decode so a `Value::Array` is never one element
//!    long. Exercised by `repeated-tag-exact-duplicate-deduped` (`x:1|c|#team:a,team:a` ->
//!    `x:1|c|#team:a`) and `bare-tag-exact-duplicate-deduped` (`x:1|c|#urgent,urgent` ->
//!    `x:1|c|#urgent`).
//! 9. **A DogStatsD event/service check's fields re-emit in canonical order, regardless of the
//!    order they arrived in on the wire, and an event `TEXT`'s `\n` escape re-emits the same way it
//!    decoded.** `_e{tlen,xlen}:title|text` canonical order is `d:`/`h:`/`p:`/`t:`/`k:`/`s:`/`#tags`/
//!    `c:`; `_sc|name|status` canonical order is `d:`/`h:`/`#tags`/`c:`/`m:` (`m:` always last,
//!    since it consumes the rest of the line on decode). Nothing else about either shape
//!    normalizes: a title/text/name/host/aggregation-key/source-type/message survives verbatim
//!    (including an embedded `|`, which the byte-length prefix -- not a `|` scan -- delimits, so it
//!    never needs sanitizing out of a title/text at all), and a multi-byte UTF-8 title's `TITLE_LEN`
//!    is its byte length, not its char count. Exercised by `event-fields-reordered-canonicalized`
//!    (order only) and, for the "nothing else changes" half,
//!    `dogstatsd-event-all-fields`/`service-check-all-fields` (every optional field, already
//!    canonical -- [`SAME_AS_INPUT`]), `event-text-with-pipe-and-escaped-newline`,
//!    `event-multibyte-title-lengths`, and `event-title-contains-pipe`.
//!
//! Everything else -- the raw kinds a lossless relay must carry (`Samples`/`SetMembers`), `|c:`/
//! `|T` round-tripping under DogStatsD, relative-gauge deltas, the negative-absolute-gauge
//! two-line idiom, and a DogStatsD event/service check with no reordering to canonicalize -- is not
//! a normalization at all: it relays byte-for-byte (modulo (3)/(4)/(9) above) because nothing about
//! it needs to change.
//!
//! `mod tcp`/`mod tls` below add no new entry to this list -- `transport: tcp` (and TLS on top of
//! it) changes framing and, for TLS, transport security, never message content. The one framing
//! difference is mechanical: UDP newline-*joins* a batch's lines into one datagram, TCP
//! newline-*terminates* each of them, so the captured TCP bytes are the UDP bytes plus one final
//! `\n` (`mod tcp::strip_lf_framing`). Every normalization above still applies unchanged, since
//! both transports share the same `StatsdEncoder`/`StatsdDecoder`.

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

/// The `.expected` marker meaning "byte-identical to the `.in` file" -- see this file's module
/// doc, mirroring `syslog_round_trip.rs`'s identical constant.
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
/// file is exactly [`SAME_AS_INPUT`]) the case's own `.in` bytes.
fn expected_bytes(name: &str, input: &[u8]) -> Vec<u8> {
    let expected = read_fixture(name, "expected");
    if expected.as_slice() == SAME_AS_INPUT {
        input.to_vec()
    } else {
        expected
    }
}

/// Zeroes an event's receipt-time `timestamp` **unless** it carries a `statsd.timestamp ==
/// Value::U64(_)` carrier -- the raw wire seconds `logit_inputs::statsd` stamps when
/// `Event::timestamp` came from a wire `|T<secs>` segment rather than receipt time (that module's
/// own doc comment). The two independent decodes a round-trip test compares (the direct one, and
/// the one a live `statsd_in` produces after a real send) can only legitimately differ in
/// *receipt*-time timestamps -- this process's wall clock at the moment each decode ran -- never
/// in a wire-supplied one, which is the same concrete instant either way. Mirrors
/// `syslog_round_trip.rs`'s `normalize_receipt_time`, narrowed to the one case statsd actually has
/// where "timestamp" isn't always receipt-time-dependent.
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

/// Real UDP harness: a plain "byte capture" socket (for the raw-datagram half of every
/// byte-for-byte assertion) alongside a real, bound [`StatsdInput`] draining into a [`Fanout`]
/// channel (for the decoded-`EventBatch` half). Mirrors `syslog_round_trip.rs`'s `Harness`
/// exactly: `bind()`-then-`local_addr()`-then-spawn, no bind-drop race and no sleep-based
/// readiness guess.
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

    /// Sends `batch` through a fresh [`StatsdOutput`] built from `encoder()` -- once at the raw
    /// capture socket, once at the live `statsd_in` -- and returns the raw datagram bytes
    /// alongside the [`EventBatch`] the real input decoded from them (receipt-time `timestamp`
    /// fields already normalized).
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

    /// Sends `batch` through a fresh [`StatsdOutput`] at the capture socket only, returning the raw
    /// datagram bytes -- no live-decode leg. Used by [`Self::round_trip`], and directly by the
    /// statsd-dialect normalization tests (module doc (6)) and the aggregate-relay test below,
    /// neither of which claims a lossless decode round trip.
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

    /// Sends `raw` bytes directly to the live `statsd_in` listener, bypassing `StatsdOutput`
    /// entirely -- for the aggregate-relay test, which needs a genuinely UDP-decoded `EventBatch`
    /// to feed `Aggregator::process`, not one this harness's own encoder has already round-tripped.
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

/// One deterministic byte-for-byte case: a fixture's own decode round-trips through a live
/// `statsd_in` unchanged (whole-`EventBatch` equality), and the raw datagram the far end actually
/// received equals the case's `.expected` bytes (module doc's normalization list covers every
/// place the two legitimately differ). `encoder` is a factory, not a value, so it can be called
/// twice (once per socket) without moving anything shared.
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

/// The `.expected`-differs-from-`.in` normalizations named in the module doc: counter rate
/// folding, `@1.0` omission, and canonical number formatting. Each still round-trips through a
/// full decode-equality check -- none of these is lossy, just re-spelled.
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

/// Module doc normalization (5): sanitizer substitutions reachable from a real decode -- a name
/// containing `#` and a tag value containing `@` both genuinely change (the substituted byte is
/// gone for good, unlike normalizations (1)-(4) which only re-spell the same information), so
/// these two are asserted against their `.expected` wire bytes only, the same way the one-way-lossy
/// dialect cases (6) are -- no decoded-batch equality claim is made for them.
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

/// The contrasting case: a member's own space survives untouched rather than being substituted
/// (unlike the name/tag-key rule above), so this one *is* a full byte-for-byte, decode-equality
/// round trip -- nothing about the data changed.
#[tokio::test]
async fn a_member_with_a_space_round_trips_byte_for_byte() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "sanitizer-member-space", || {
        StatsdEncoder::new(Format::DogStatsd)
    })
    .await;
}

// ---- Multi-value DogStatsD tags (module doc (8), W9) ----------------------------------------------

/// A repeated tag key is byte-for-byte, not a normalization: `insert_tags` folds it into a
/// `Value::Array` in wire order and `statsd_out` expands it back to one tag per element (module
/// doc (8)).
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

/// The same fold applies to a DogStatsD event line's `#` field -- `insert_tags` backs every `#`
/// field alike, metric line or event line (`crates/logit-inputs/src/statsd.rs`'s doc at the
/// `insert_tags` call sites).
#[tokio::test]
async fn a_repeated_tag_key_on_an_event_line_round_trips_byte_for_byte() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "repeated-tag-on-event-line", || {
        StatsdEncoder::new(Format::DogStatsd)
    })
    .await;
}

/// Module doc (8): an *exact* duplicate token dedupes at decode -- the Datadog agent's own rule --
/// so `#team:a,team:a` never becomes a one-element `Array`, it stays `Str("a")` and relays as
/// `#team:a`.
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

/// `statsd_in -> aggregate -> statsd_out`: two lines carrying the same multi-valued tag are the
/// same series (`aggregate`'s `SeriesKey` recurses element-wise into an `Array`, so equal arrays
/// key equal), so they merge into one flushed event whose tag bytes survive the round trip
/// unchanged -- proof that a multi-valued tag is one series, not two.
#[tokio::test]
async fn statsd_in_aggregate_statsd_out_relay_preserves_a_multi_valued_tag() {
    let mut harness = Harness::new().await;

    let raw = b"x:1|c|#team:a,team:b\nx:1|c|#team:a,team:b";
    let batch = harness.send_raw_and_decode(raw).await;
    assert_eq!(batch.events.len(), 2, "one event per line");

    let mut aggregator = Aggregator::new(Duration::from_secs(10));
    let resource = batch.resource.clone();
    let mut forwarded = Vec::new();
    for event in batch.events {
        if let Some(event) = aggregator.process(&resource, event) {
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

/// `influxdb_out`: a repeated tag key reaches this sink as a `Value::Array` too, and line
/// protocol's tag set is a map, not a multiset, so `render_tag_suffix` renders only the **last**
/// representable element and counts it (`crates/logit-outputs/src/influxdb.rs`'s
/// `render_tag_suffix` doc, "Multi-value tags: last-value-wins, counted"). Decodes with the real
/// statsd decoder (not a hand-built `EventBatch`) and encodes with `InfluxLineEncoder` directly --
/// there is no influx round-trip fixture file in this crate's `tests/`, so this stays a
/// unit-style case in the file that already owns the real-decoder path.
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

/// The documented negative-absolute-gauge idiom (`logit_outputs::statsd`'s "Negative absolute
/// gauges" section): `Gauge(-5.0)` relays as `name:0|g` immediately followed by `name:-5|g`, one
/// indivisible `MessageBuf` entry so the two lines can never be split across datagrams. Needs
/// `relative_gauges: true` on the round-trip encoder -- without it, the second line's `GaugeDelta`
/// would be dropped as unresolved, which isn't what this fixture tests.
#[tokio::test]
async fn negative_absolute_gauge_idiom_round_trips_with_relative_gauges_enabled() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "negative-absolute-gauge-idiom", || {
        StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true)
    })
    .await;
}

/// `conns:+1|g` decodes to an unresolved `GaugeDelta(1.0)`; with `relative_gauges: true` it relays
/// natively (statsd is the one protocol with real wire syntax for a relative adjustment).
#[tokio::test]
async fn relative_gauge_delta_round_trips_with_relative_gauges_enabled() {
    let mut harness = Harness::new().await;
    assert_byte_for_byte(&mut harness, "relative-gauge-delta", || {
        StatsdEncoder::new(Format::DogStatsd).with_relative_gauges(true)
    })
    .await;
}

// ---- `format: statsd` dialect normalizations (module doc (6)), one-way lossy by design ----------

/// Asserts a dialect-changing fixture's captured wire bytes only -- no decoded-equality claim,
/// since `format: statsd` genuinely can't express what it drops (module doc (6)).
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

/// Module doc (6)'s events/service-checks bullet: neither shape has a `format: statsd` wire form
/// at all, so the whole event is dropped -- not normalized -- and counted
/// `EncodeStats::dropped_dialect_events`. This can't go through `Harness`/`assert_dialect_output`
/// the way every other dialect case above does: a batch that drops to *no* lines never sends a
/// datagram at all (`StatsdOutput::send` returns early on an empty `MessageBuf`), so
/// `capture_only`'s `recv_from` would simply time out waiting for one. `StatsdEncoder::encode_into`
/// is called directly instead -- the same pure, socket-free path `logit_outputs::statsd`'s own unit
/// tests use -- so both halves of the drop are observable: the rendered `MessageBuf` is empty, and
/// the returned `EncodeStats` counts it.
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

// ---- Sanitizer substitution (module doc (5)) -----------------------------------------------------

/// A `SetMembers` member containing a forbidden byte (`|`, here) is substituted with `_` and
/// counted -- real behavior, but unreachable from a decoded statsd fixture (see module doc (5)),
/// so this builds the `EventBatch` directly rather than decoding one. Still goes over the real
/// capture socket and the real live `statsd_in`, so the substituted line is confirmed to actually
/// decode cleanly on the far end, not just render plausibly.
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

// ---- `statsd_in -> aggregate -> statsd_out`, exact -----------------------------------------------

/// The plan's explicit aggregate-relay requirement: three timer lines and two set lines of the
/// same two series, decoded over real UDP by a live `statsd_in`, absorbed by an `Aggregator`
/// configured to retain raw shapes (`distributions: samples`, `sets: members` --
/// `docs/adr/lossless-transit.md`'s "summarization is opt-in and named"), flushed once, and
/// re-encoded -- the sink emits one multi-value `ms` line carrying every timer value (sample rate
/// preserved) and one `|s` line per distinct set member (module doc (7)). Every metric here must
/// be absorbed (none passed through), and after one flush nothing survives to a second one --
/// `distributions: samples`/`sets: members` accumulators are always drained, unlike a retained
/// gauge series.
///
/// The two series (a `Samples` and a `SetMembers`) sit in the same `Aggregator`'s
/// `HashMap<SeriesKey, SeriesState>`, whose iteration order across *distinct* series is not
/// something this test may assume (ordinary `std::collections::HashMap`, randomized per process) --
/// only the *values within* one series are order-preserving (a `SmallVec`/`Vec`, not a map). The
/// assertion below is exact per line (module doc's "assert the exact lines") but order-independent
/// across the two series.
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
    for event in batch.events {
        if let Some(event) = aggregator.process(&resource, event) {
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

    // A second flush of the same (now-drained) window emits nothing -- `distributions: samples`/
    // `sets: members` accumulators are tumbling, same as their sketch/estimate counterparts.
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

// ---- `|T` carrier survives `aggregate`'s flush-time rebuild --------------------------------------

/// The `|T` carrier round-trips through `aggregate` even though `aggregate` rebuilds
/// `Event::timestamp` to the flush clock at flush time: `statsd.timestamp` rides on the series key
/// like any other attribute, so the sink reads the carrier's own `Value::U64`, never the
/// flush-time `Event::timestamp` -- this is the regression for the bug the carrier fix closes
/// (a `Value::Bool(true)` marker plus `event.timestamp` would have re-emitted the *flush* second
/// here, not the original wire value).
#[tokio::test]
async fn statsd_in_aggregate_statsd_out_relay_preserves_the_wire_timestamp() {
    let mut harness = Harness::new().await;

    let batch = harness.send_raw_and_decode(b"hits:1|c|T1700000000").await;
    assert_eq!(batch.events.len(), 1);

    let mut aggregator = Aggregator::new(Duration::from_secs(10));
    let resource = batch.resource.clone();
    let mut forwarded = Vec::new();
    for event in batch.events {
        if let Some(event) = aggregator.process(&resource, event) {
            forwarded.push(event);
        }
    }
    assert!(forwarded.is_empty(), "a delta Counter is always absorbed by aggregate");

    // Deliberately a different second from the wire value -- if the sink ever read
    // `Event::timestamp` instead of the `statsd.timestamp` carrier, this would catch it emitting
    // the flush second (1_800_000_000) instead of the wire one (1_700_000_000).
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

/// Two lines differing only in their `|T` value are distinct series by construction (the carrier
/// rides on the series key, which is the event's whole attribute map), so they flush as two
/// separate lines, never merged into one counter.
#[tokio::test]
async fn statsd_in_aggregate_statsd_out_relay_keeps_distinct_timestamps_as_distinct_series() {
    let mut harness = Harness::new().await;

    let raw = b"hits:1|c|T1700000000\nhits:1|c|T1700000100";
    let batch = harness.send_raw_and_decode(raw).await;
    assert_eq!(batch.events.len(), 2, "one event per line");

    let mut aggregator = Aggregator::new(Duration::from_secs(10));
    let resource = batch.resource.clone();
    for event in batch.events {
        assert!(
            aggregator.process(&resource, event).is_none(),
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

// ---- `statsd_in -> aggregate -> statsd_out`, a service check -------------------------------------

/// A service check is a `MetricKind::Gauge`, so `aggregate` absorbs it exactly like an ordinary
/// gauge ("last write wins" on the series' latest source timestamp,
/// `docs/adr/aggregation-window-semantics.md`) rather than passing it through unmerged -- there is
/// only one write here, so the flushed value is unchanged, but the point of this test is that the
/// flush round trip doesn't lose any of the `statsd.service_check.*` carriers (or `statsd.timestamp`
/// / `statsd.container_id`) riding on the series key alongside the gauge value: `_sc|name|status`
/// plus its `d:`/`m:` fields survive a real `aggregate` window exactly, the same guarantee the two
/// `|T`-carrier tests above pin for an ordinary counter.
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
    for event in batch.events {
        if let Some(event) = aggregator.process(&resource, event) {
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

/// `statsd_out(transport: tcp) -> statsd_in(transport: tcp)`, over the same fixture corpus the
/// UDP tests above use, plus the two cases only a stream transport can exercise at all: a raw
/// client whose line straddles two writes, and a line whose first byte is an ASCII digit (which a
/// listener framing RFC 6587-style would read as an octet count and mis-frame the connection on).
///
/// The module doc's permitted-normalization list gains **no new entry** here: `transport: tcp`
/// changes framing and nothing else. On UDP a batch's lines are newline-*joined* into one
/// datagram with no trailing separator; on TCP each line is newline-*terminated*, so the captured
/// frame is exactly the UDP bytes plus one final `\n`, which [`strip_lf_framing`] takes back off
/// before the byte-for-byte comparison. Content, decode and every normalization above are shared:
/// both transports run the same `StatsdEncoder`/`StatsdDecoder`.
mod tcp {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener as TokioTcpListener, TcpStream};

    /// TCP twin of the top-level [`Harness`]: same `bind()`-then-`local_addr()` readiness for the
    /// live `statsd_in`, plus a raw TCP "capture" listener standing in for the UDP capture socket
    /// above -- it accepts one connection per round trip and reads it to EOF, since a fresh
    /// `StatsdOutput::tcp` per call closes its connection (and so EOFs the peer) the moment it is
    /// dropped. Modelled line for line on `syslog_round_trip.rs`'s own `mod tcp::TcpHarness`.
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

        /// Sends `batch` through a fresh TCP [`StatsdOutput`] built from `encoder()` -- once at
        /// the raw capture listener, once at the live `statsd_in` -- and returns the captured
        /// wire bytes alongside the [`EventBatch`] the real input decoded from them (receipt-time
        /// `timestamp` fields already normalized). Mirrors the UDP [`Harness::round_trip`]
        /// exactly, modulo the transport.
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

    /// Strips the single trailing `LF` `statsd_out`'s TCP transport terminates its last line with,
    /// asserting it was there -- the one framing difference between the two transports, and the
    /// only thing a TCP round trip needs to account for that the UDP path above doesn't.
    fn strip_lf_framing(frame: &[u8]) -> &[u8] {
        let (last, rest) = frame.split_last().expect("a TCP frame is never empty");
        assert_eq!(*last, b'\n', "statsd_out's TCP transport terminates every line with LF");
        rest
    }

    /// TCP twin of the top-level `assert_byte_for_byte`: same fixture, same `.expected` bytes, but
    /// the captured wire bytes are LF-terminated lines rather than a bare UDP datagram.
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

    /// `statsd_out(transport: tcp) -> statsd_in(transport: tcp)`, over the same corpus the UDP
    /// tests above use -- the framing changes (LF-terminated lines), the content and permitted
    /// normalizations don't (this file's module doc). One test rather than three so the corpus
    /// shares a single harness, the way the UDP tests share theirs.
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

    /// The pin for `statsd_in`'s framing choice, end to end through the real component: a statsd
    /// line may legally begin with an ASCII digit, which RFC 6587's auto-detecting framing (the
    /// shared driver's default, and what `syslog_in` wants) would latch as an octet count and
    /// reframe the whole connection on. No `statsd_out` involved -- a raw client, so the bytes on
    /// the wire are exactly what is asserted.
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

    /// A line whose `\n` only arrives in the second write: the driver's framer buffers across
    /// reads, so this is one event rather than two halves rejected as malformed. The stream-only
    /// case the UDP corpus cannot express at all, since a datagram is always whole.
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

/// `statsd_out` -> `statsd_in` over TLS -- server TLS, mutual TLS, and the wrong-CA negative
/// case, driven by the real sink, exactly the shape `syslog_round_trip.rs`'s `mod tls` has.
///
/// **Two clients, deliberately.** The three round trips run the real `statsd_out` with its own
/// `tls:` block (`docs/adr/statsd-output.md`'s TLS amendment), which is what a deployment
/// actually looks like and pins both halves at once. One raw `tokio_rustls` client survives
/// alongside them, because it asserts something the sink cannot reach: that a *refused* handshake
/// leaves the listener still serving everyone else. `statsd_out`'s own wrong-CA case asserts the
/// sink-side `Fault` instead, which a raw client has no concept of -- the two are different
/// halves of the same failure, not a duplicate.
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
        // `logit-cli` lives at `crates/logit-cli`; the fixtures live at the repo root's
        // `testdata/tls` -- two levels up from `CARGO_MANIFEST_DIR`, exactly
        // `syslog_round_trip.rs`'s own `mod tls::testdata_dir`.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// Stands up a TLS-terminating TCP `statsd_in` with `settings`, returning its bound address
    /// and the `Fanout` receiver every decoded batch lands on -- `bind()`-then-`local_addr()`
    /// readiness, the same idiom every harness in this file uses, no sleep needed.
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

    /// A `tokio-rustls` client trusting exactly `ca_file` under `testdata/tls` -- `other-ca.pem`
    /// is what makes the "wrong CA" case a real trust failure rather than a name mismatch.
    /// `client_cert` is `(cert, key)` for the mTLS cases, `None` for a client presenting nothing.
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

    /// Writes `line` over a completed TLS handshake and returns the batch `statsd_in` decoded
    /// from it. The connection is dropped afterwards, which EOFs the listener's connection task
    /// and flushes the accumulator immediately rather than on its 100ms timer.
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

    /// Sends `batch` through a real TLS `statsd_out` built with `settings` and returns what
    /// `statsd_in` decoded on the other end (receipt-time `timestamp` fields normalized, exactly
    /// as every other harness in this file does). `localhost` rather than `127.0.0.1`, since
    /// `testdata/tls/server.pem`'s SAN is what the sink's own SNI has to match.
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

    /// Server TLS only: `statsd_in` presents `server.pem`/`server.key` with no `client_ca_file`,
    /// so any client is accepted once the handshake itself completes -- and a real
    /// `statsd_out -> statsd_in` relay over it is a fixed point, tags and all, exactly as it is
    /// in the clear.
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

    /// The sink-side negative: `statsd_out` trusts `other-ca.pem`, which never signed
    /// `server.pem`, so *this* side's own certificate verification fails the handshake before a
    /// single byte of the batch has left the host -- `Fault::Clean`, and deterministically so.
    ///
    /// Deterministic for the same reason `syslog_round_trip.rs`'s twin gives: a *server*-cert
    /// rejection happens inside the client's own verification, before `TlsConnector::connect`
    /// even completes, unlike a client-cert rejection, which under TLS 1.3 this write-only sink
    /// never sees at all.
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

    /// The listener-side half of the same failure, and the one a `statsd_out` cannot assert: a
    /// raw client trusting `other-ca.pem`, which never signed `server.pem`, is
    /// refused inside its own certificate verification -- and the half that matters for a relay,
    /// the listener keeps serving everyone else afterwards.
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
