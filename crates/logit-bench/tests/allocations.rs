//! Exact allocation counts for each stage of the reference nginx pipeline.
//!
//! These are the numbers `docs/design/memory.md` quotes. They're assertions rather than a report
//! so that an allocation regression fails the build. `cargo nextest`'s process-per-test isolation
//! makes the counts reproducible rather than order-dependent.
//!
//! **When one of these fails, read the printed actual/expected line before changing the constant.**
//! A count is a tripwire, not a score (`docs/adr/event-sizing-and-allocation-strategy.md`): decide
//! whether the change is worth it, then update the constant and `docs/design/memory.md`'s table in
//! the same commit. Never relax an assertion to `<=`.
//!
//! Every measurement here:
//!
//! - **Warms its subject first**, because plenty of things allocate once on first use (the
//!   `OnceLock` interner, a `HashMap`'s first table). See [`logit_bench::alloc::measure`].
//! - **Asserts `alloc`, not `realloc`.** Reallocation is printed alongside and asserted separately
//!   where it matters: it means a container grew, i.e. a missing `with_capacity`, which is a
//!   different (and usually cheaper) problem than a missing reuse.

use logit_bench::alloc::{measure, CountingAlloc, Stats};
use logit_bench::fixtures;
use logit_core::{AttrMap, EventBatch, Registry, Resource, Telemetry, TraceRef, Value};
use logit_outputs::influxdb::InfluxLineEncoder;
use logit_outputs::statsd::{Format as StatsdFormat, StatsdEncoder};
use logit_outputs::stdio::{EventDump, Format};
use logit_outputs::syslog::{Format as SyslogFormat, SyslogEncoder};
use logit_pipeline::runtime::{drain_inbox, route_batch, InHand};
use logit_pipeline::{
    process_batch, send_batch, unwrap_batch, BatchContext, Delivered, Fanout, RouterScratch,
    SinkQueue, SinkQueueConfig, SinkStore, Transform,
};
use logit_proto::{Decoder, Encoder, FramedEncoder, MessageBuf};
use logit_script::{ProcessOutcome, ScriptWorker};
use std::sync::Arc;

/// Installed for this test binary only -- no other crate's tests pay the counting overhead.
#[global_allocator]
static ALLOC: CountingAlloc = CountingAlloc::new(std::alloc::System);

/// Asserts an exact allocation count, printing the full [`Stats`] either way so a run with
/// `--nocapture` produces the table `docs/design/memory.md` is built from.
#[track_caller]
fn expect_allocs(label: &str, stats: Stats, expected: u64) {
    println!(
        "{label:<40} allocs={:<6} reallocs={:<5} bytes={:<8} peak_live={}",
        stats.allocs, stats.reallocs, stats.bytes, stats.peak_live_bytes
    );
    assert_eq!(
        stats.allocs, expected,
        "{label}: allocation count changed ({} -> {}); if this is an improvement, update this \
         constant and docs/design/memory.md's table together",
        expected, stats.allocs
    );
}

// ---------------------------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------------------------

/// One allocation: the `Vec<Event>` the batch is collected into. Every field of the event
/// (message, tag, hostname, timestamp) is a refcounted slice of the datagram `Bytes`, so the decode
/// adds nothing per field (`docs/design/data-model.md`'s zero-copy design).
///
/// Excludes the listener's one `Bytes::copy_from_slice` per datagram, which
/// [`datagram_copy_is_one_right_sized_allocation`] pins.
#[test]
fn syslog_decode_one_line() {
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::nginx_syslog_datagram(1);
    drop(decoder.decode(datagram.clone())); // warm: interns every syslog.* key once

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    expect_allocs("syslog_in: decode 1 line", stats, 1);
}

/// One allocation for 100 lines, plus five reallocations: `decode` collects into a `Vec::new()` and
/// grows it 4 -> 8 -> ... -> 128. A `with_capacity` hint (one `memchr` pass counts the lines) would
/// remove them; it's recorded rather than done because it's the least valuable item on
/// `docs/design/memory.md`'s list.
#[test]
fn syslog_decode_100_lines() {
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::nginx_syslog_datagram(100);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 100);
    expect_allocs("syslog_in: decode 100 lines", stats, 1);
    assert_eq!(stats.reallocs, 5, "the events Vec grows 4 -> 8 -> 16 -> 32 -> 64 -> 128");
}

/// Two allocations for one line. Tag values are `slice_of` slices of the datagram, as in
/// `syslog.rs`, so they cost nothing ([`statsd_tag_values_share_the_datagram_allocation`] pins
/// that structurally).
///
/// Syslog pays one `Vec<Event>` total; statsd pays two. The multi-value grammar (`name:1:2:3|c`)
/// makes `parse_line` collect one line's events into its own `Vec` before `decode` appends them to
/// the batch's `Vec`: one allocation per line plus one per batch.
#[test]
fn statsd_decode_one_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    expect_allocs("statsd_in: decode 1 line", stats, 2);
}

/// `ms`/`h`/`d` decode to a raw `MetricKind::Samples` (`docs/adr/lossless-transit.md`), not a
/// sketch. One value fits inline in `Samples`'s `SmallVec` (`SAMPLES_INLINE`), so the count is
/// [`statsd_decode_one_line`]'s per-line/per-batch `Vec<Event>` pair and nothing more.
#[test]
fn statsd_decode_one_distribution_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_distribution_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    match &batch.events[0].metrics[0].kind {
        logit_core::MetricKind::Samples(samples) => {
            assert_eq!(samples.values.as_slice(), &[120.0]);
            assert_eq!(samples.sample_rate, 1.0);
        }
        other => panic!("expected Samples, got {other:?}"),
    }
    expect_allocs("statsd_in: decode 1 distribution line", stats, 2);
}

/// Same line as [`statsd_decode_one_distribution_line`], sampled at `@0.1`: the same count. The
/// raw `sample_rate` rides verbatim on the decoded `Samples` (`docs/adr/lossless-transit.md`);
/// only `aggregate` extrapolates, and only when it sketches, so nothing here scales with the rate.
#[test]
fn statsd_decode_one_sampled_distribution_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_sampled_distribution_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    match &batch.events[0].metrics[0].kind {
        logit_core::MetricKind::Samples(samples) => {
            assert_eq!(samples.values.as_slice(), &[120.0]);
            assert_eq!(samples.sample_rate, 0.1, "the raw rate rides verbatim -- no extrapolation");
        }
        other => panic!("expected Samples, got {other:?}"),
    }
    expect_allocs("statsd_in: decode 1 sampled distribution line", stats, 2);
}

/// `s` decodes to a raw `MetricKind::SetMembers`, a `Vec<Bytes>` of zero-copy datagram slices
/// (`docs/adr/lossless-transit.md`). Three allocations: [`statsd_decode_one_line`]'s
/// per-line/per-batch `Vec<Event>` pair plus the member `Vec`, since `SetMembers`, unlike
/// `Samples`, has no inline storage.
#[test]
fn statsd_decode_one_set_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_set_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    match &batch.events[0].metrics[0].kind {
        logit_core::MetricKind::SetMembers(members) => {
            assert_eq!(members, &vec![bytes::Bytes::from_static(b"abc123")]);
        }
        other => panic!("expected SetMembers, got {other:?}"),
    }
    expect_allocs("statsd_in: decode 1 set line", stats, 3);
}

/// A repeated DogStatsD tag key folds into a `Value::Array` at decode
/// (`crates/logit-inputs/src/statsd.rs`'s "DogStatsD tags" section). Four allocations:
/// [`statsd_decode_one_line`]'s per-line/per-batch `Vec<Event>` pair, plus two for the `Array`.
/// `insert_tags` builds its `Vec` spine (`vec![existing, value]`), and `build_event`'s
/// `attributes.clone()`, run once even for a single-value line, deep-copies that spine. A scalar
/// tag's share of the same clone is free (a `SmallVec` memcpy plus a `Bytes` refcount bump).
#[test]
fn statsd_decode_one_line_with_a_repeated_tag_key() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_repeated_tag_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    match batch.events[0].attributes.get("team") {
        Some(Value::Array(elements)) => assert_eq!(elements.len(), 2, "team:a,team:b"),
        other => panic!("expected a 2-element Array under 'team', got {other:?}"),
    }
    expect_allocs("statsd_in: decode 1 line with a repeated tag key", stats, 4);
}

/// The same repeated tag key on a multi-value counter line (`name:v1:v2:v3|c`): three `Event`s
/// whose `build_event` clones one shared `AttrMap` once per value. Six allocations: the
/// per-line/per-batch `Vec<Event>` pair, the `Array` spine `insert_tags` builds, and one deep copy
/// of that spine per value event (three). Scalar tags clone for free; an `Array` tag doesn't.
#[test]
fn statsd_decode_one_multi_value_counter_line_with_a_repeated_tag_key() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_multi_value_repeated_tag_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 3, "one event per value (1, 2, 3)");
    for event in &batch.events {
        match event.attributes.get("team") {
            Some(Value::Array(elements)) => assert_eq!(elements.len(), 2, "team:a,team:b"),
            other => panic!("expected a 2-element Array under 'team', got {other:?}"),
        }
    }
    expect_allocs("statsd_in: decode 1 multi-value counter line with a repeated tag key", stats, 6);
}

/// A DogStatsD event (`_e{tlen,xlen}:title|text|...`) whose `TEXT` has no `\n` escape:
/// `unescape_event_text` takes its zero-copy `slice_of` path, so the count is
/// [`statsd_decode_one_line`]'s per-line/per-batch `Vec<Event>` pair.
#[test]
fn statsd_decode_one_event_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_event_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    assert!(batch.events[0].log.is_some(), "expected a log-only event");
    expect_allocs("statsd_in: decode 1 event line", stats, 2);
}

/// The one case `unescape_event_text` can't slice: `TEXT` holds a two-byte `\n` escape, so the
/// message needs a newline byte the wire doesn't have. One allocation over
/// [`statsd_decode_one_event_line`]: the decoded length is known up front, so the `Vec` is sized
/// to fit and `Bytes::from(Vec<u8>)` takes its `len == capacity` path, with no realloc and no
/// second `Shared` control-block allocation a slack-capacity buffer would cost (compare
/// [`logfmt_parse_escaped_value_event`], which `shrink_to_fit`s for the same reason).
#[test]
fn statsd_decode_one_event_line_with_an_escaped_newline() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_event_with_escaped_newline_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    let message = batch.events[0].log.as_ref().unwrap().message.as_str().expect("str message");
    assert!(message.contains('\n'), "the escape should have become a real newline");
    expect_allocs("statsd_in: decode 1 event line with an escaped newline", stats, 3);
}

/// A DogStatsD service check (`_sc|name|status|...`) decodes to one `Gauge` event whose
/// `statsd.service_check.*` attributes are zero-copy datagram slices: the count is
/// [`statsd_decode_one_line`]'s per-line/per-batch `Vec<Event>` pair.
#[test]
fn statsd_decode_one_service_check_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_service_check_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].metrics.len(), 1, "expected one Gauge metric");
    expect_allocs("statsd_in: decode 1 service check line", stats, 2);
}

/// The logs-only shape: a plain-text syslog line with no JSON body (`fixtures::SSHD_SYSLOG_LINE`).
/// One allocation, the `Vec<Event>`, for the same zero-copy reason as [`syslog_decode_one_line`].
/// Its six attributes (`syslog.facility`/`severity`/`timestamp`/`hostname`/`tag`/`pid`) fit
/// `AttrMap`'s 8-slot inline capacity, unlike the nginx shape's 10, which spills.
#[test]
fn syslog_decode_one_logs_only_line() {
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::logs_only_syslog_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].attributes.len(), 6, "facility/severity/timestamp/hostname/tag/pid");
    expect_allocs("syslog_in: decode 1 logs-only line", stats, 1);
}

/// `decode_into` against a buffer the caller reuses across datagrams, the listener's hot path
/// (`logit-inputs::udp::decode_loop`, `docs/adr/decoupled-listener-io.md`), rather than
/// `decode()`'s fresh `Vec::new()`. Zero: the `Vec<Event>` is cleared, keeping its capacity,
/// between calls. [`syslog_decode_one_line`]'s one is the cost when nothing reuses the buffer.
#[test]
fn syslog_decode_into_a_warm_reused_buffer_costs_nothing() {
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::nginx_syslog_datagram(1);
    let mut out = Vec::new();
    decoder.decode_into(datagram.clone(), 0, &mut out).expect("should decode"); // warm: grows `out`
    out.clear(); // capacity intact -- this is the property under test

    let (_resource, stats) =
        measure(|| decoder.decode_into(datagram.clone(), 0, &mut out).expect("should decode"));
    assert_eq!(out.len(), 1);
    expect_allocs("syslog_in: decode_into into a warm buffer", stats, 0);
}

/// The statsd analogue of the syslog test above: 1, against [`statsd_decode_one_line`]'s 2. The
/// reused buffer absorbs the batch `Vec<Event>`; the per-line `Vec<Event>` `parse_line` collects
/// into is internal to `decode_into` and stays.
#[test]
fn statsd_decode_into_a_warm_reused_buffer_costs_one_not_two() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_datagram(1);
    let mut out = Vec::new();
    decoder.decode_into(datagram.clone(), 0, &mut out).expect("should decode");
    out.clear();

    let (_resource, stats) =
        measure(|| decoder.decode_into(datagram.clone(), 0, &mut out).expect("should decode"));
    assert_eq!(out.len(), 1);
    expect_allocs("statsd_in: decode_into into a warm buffer", stats, 1);
}

// -- collectd_in (docs/adr/collectd-binary-relay.md) -------------------------------------------

/// One allocation, the `Vec<Event>`, as for `syslog_in`. Every identity field is a refcounted
/// datagram slice (`string_value`), the record name is built in a reused scratch `String` before
/// interning, and a single-data-source list fits `MetricList`'s inline capacity.
#[test]
fn collectd_decode_one_list() {
    let mut decoder = fixtures::collectd_decoder();
    let datagram = fixtures::collectd_packet(1);
    drop(decoder.decode(datagram.clone())); // warm: interns the six attribute keys and the name

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].metrics.len(), 1);
    expect_allocs("collectd_in: decode 1 list", stats, 1);
}

/// The listener's hot path (`docs/adr/decoupled-listener-io.md`): `decode_into` against a buffer
/// `decode_loop` reuses across datagrams. Zero once the caller's `Vec<Event>` keeps its capacity.
#[test]
fn collectd_decode_into_a_warm_reused_buffer_costs_nothing() {
    let mut decoder = fixtures::collectd_decoder();
    let datagram = fixtures::collectd_packet(1);
    let mut out = Vec::new();
    decoder.decode_into(datagram.clone(), 0, &mut out).expect("should decode");
    out.clear(); // capacity intact -- this is the property under test

    let (_resource, stats) =
        measure(|| decoder.decode_into(datagram.clone(), 0, &mut out).expect("should decode"));
    assert_eq!(out.len(), 1);
    expect_allocs("collectd_in: decode_into into a warm buffer", stats, 0);
}

/// Two: the `Vec<Event>`, plus one spill of the event's `MetricList`, a `SmallVec` inlined at 1.
/// A three-data-source `load` list spills once, not once per record (`docs/design/memory.md` §3).
#[test]
fn collectd_decode_one_three_value_list() {
    let mut decoder = fixtures::collectd_decoder();
    let datagram = fixtures::collectd_load_packet();
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1, "one Values part is one event, whatever its width");
    assert_eq!(batch.events[0].metrics.len(), 3);
    expect_allocs("collectd_in: decode 1 three-value list", stats, 2);
}

/// A 25-list datagram, what a collectd agent packs into one 1452-byte packet. One allocation, not
/// 25; the per-list cost is three reallocs growing the `Vec<Event>` (4 -> 8 -> 16 -> 32), because
/// a collectd datagram has no header giving its Values-part count.
#[test]
fn collectd_decode_a_25_list_packet() {
    let mut decoder = fixtures::collectd_decoder();
    let datagram = fixtures::collectd_packet(25);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 25);
    assert_eq!(stats.reallocs, 3, "the events Vec grows 4 -> 8 -> 16 -> 32");
    expect_allocs("collectd_in: decode a 25-list packet", stats, 1);
}

/// The same three-data-source list decoded through a decoder holding a `types.db`, which renames
/// its records `load.load.shortterm`/`midterm`/`longterm`. The same count as without it: the
/// lookup is one `HashMap::get` per Values part returning a borrowed slice, and the names are
/// written into the reused scratch `String` before interning.
#[test]
fn collectd_decode_one_list_with_types_db_resolution() {
    let mut decoder = fixtures::collectd_decoder_with_types_db();
    let datagram = fixtures::collectd_load_packet();
    drop(decoder.decode(datagram.clone())); // warm: interns the three resolved names

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(
        logit_core::interner::resolve(batch.events[0].metrics[0].name),
        "load.load.shortterm",
        "the types.db must actually have resolved, or this measures the wrong thing"
    );
    expect_allocs("collectd_in: decode 1 list with types.db", stats, 2);
}

// -- graphite_in (docs/adr/graphite-carbon-relay.md) --------------------------------------------

/// One allocation, the `Vec<Event>`, as for `syslog_in`/`collectd_in`. The path is interned, the
/// value is an `f64`, and a single `Gauge` fits `MetricList`'s inline capacity.
#[test]
fn graphite_decode_one_plaintext_line() {
    let mut decoder = fixtures::graphite_decoder();
    let datagram = fixtures::graphite_datagram(1);
    drop(decoder.decode(datagram.clone())); // warm: interns the path once

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.events[0].metrics.len(), 1);
    expect_allocs("graphite_in: decode 1 plaintext line", stats, 1);
}

/// The same count with two carbon tags on the line: each tag value is a zero-copy datagram slice
/// (`logit_proto::graphite::decode`'s `slice_of`), and two entries fit `AttrMap`'s inline capacity.
#[test]
fn graphite_decode_one_tagged_line() {
    let mut decoder = fixtures::graphite_decoder();
    let datagram = fixtures::graphite_tagged_datagram();
    drop(decoder.decode(datagram.clone())); // warm: interns the path and both tag keys

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events[0].attributes.len(), 2, "or this measures the wrong thing");
    expect_allocs("graphite_in: decode 1 tagged line (2 tags)", stats, 1);
}

/// The listener's hot path: `decode_into` against a buffer the read loop reuses (`udp`'s
/// `decode_loop`, `tcp`'s per-connection `scratch`). Zero once the `Vec<Event>` keeps its capacity.
#[test]
fn graphite_decode_into_a_warm_reused_buffer_costs_nothing() {
    let mut decoder = fixtures::graphite_decoder();
    let datagram = fixtures::graphite_datagram(1);
    let mut out = Vec::new();
    decoder.decode_into(datagram.clone(), 0, &mut out).expect("should decode");
    out.clear(); // capacity intact -- this is the property under test

    let (_resource, stats) =
        measure(|| decoder.decode_into(datagram.clone(), 0, &mut out).expect("should decode"));
    assert_eq!(out.len(), 1);
    expect_allocs("graphite_in: decode_into into a warm buffer", stats, 0);
}

/// A 25-line datagram, what a UDP carbon sender packs into one MTU-sized packet. One allocation,
/// not 25; the per-line cost is reallocs growing the `Vec<Event>`, because a carbon datagram has
/// no header giving its line count.
#[test]
fn graphite_decode_a_25_line_datagram() {
    let mut decoder = fixtures::graphite_decoder();
    let datagram = fixtures::graphite_datagram(25);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 25);
    assert_eq!(stats.reallocs, 3, "the events Vec grows 4 -> 8 -> 16 -> 32");
    expect_allocs("graphite_in: decode a 25-line datagram", stats, 1);
}

/// Pickle: a 100-datapoint frame costs one allocation plus the `Vec<Event>`'s growth. The
/// reader's stack, arenas, and memo are decoder fields cleared per frame
/// (`logit_proto::graphite::pickle::PickleReader`), not locals, so the stack machine allocates
/// nothing of its own.
#[test]
fn graphite_decode_a_100_datapoint_pickle_frame() {
    let mut decoder = fixtures::graphite_pickle_decoder();
    let frame = fixtures::graphite_pickle_frame(100);
    drop(decoder.decode(frame.clone())); // warm: interns 100 paths and grows the reader's arenas

    let (batch, stats) = measure(|| decoder.decode(frame.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 100);
    assert_eq!(stats.reallocs, 5, "the events Vec grows 4 -> 8 -> ... -> 128");
    expect_allocs("graphite_in: decode a 100-datapoint pickle frame", stats, 1);
}

/// One scrape through the two functions a scrape tick calls (`prometheus_in` has no `Decoder`;
/// `docs/adr/prometheus-scrape-and-exposition.md`'s "No `logit_proto::Encoder`" section):
/// `text::parse_with` (bytes -> families), then `families_to_events`. The fixture is
/// `fixtures::PROMETHEUS_SCRAPE_BODY`'s 11 series (a histogram's buckets and a summary's quantiles
/// are each one composite series). Warm, so names are already interned.
#[test]
fn prometheus_decode_one_scrape() {
    use logit_proto::prometheus::families_to_events;
    use logit_proto::prometheus::text::{parse_with, Dialect};

    let mut decoder = fixtures::prometheus_decoder();
    let body = fixtures::PROMETHEUS_SCRAPE_BODY.as_bytes();
    let warm_families =
        parse_with(body, Dialect::Text0_0_4, &mut decoder).expect("fixture body must parse");
    drop(families_to_events(&warm_families, 0, &mut decoder));

    let (events, stats) = measure(|| {
        let families =
            parse_with(body, Dialect::Text0_0_4, &mut decoder).expect("fixture body must parse");
        families_to_events(&families, 0, &mut decoder)
    });
    assert_eq!(events.len(), 11, "fixtures::PROMETHEUS_SCRAPE_BODY carries 11 series");
    expect_allocs("prometheus_in: decode 1 scrape (11 series)", stats, 161);
}

/// The other Prometheus ingress: one remote-write request through
/// `logit_proto::prometheus::remote_write::decode`, over
/// `fixtures::remote_write_request_v1`'s 100 single-sample gauge series, warm. Only the codec is
/// measured: Snappy and HTTP are the receiver's, and `families_to_events` is the row above's. It
/// decodes into families through the same assembler a scrape uses, so the two compare per series.
#[test]
fn remote_write_decode_one_request_v1() {
    use logit_proto::prometheus::remote_write::{decode, Version};

    let mut decoder = fixtures::prometheus_decoder();
    let body = fixtures::remote_write_request_v1();
    drop(decode(&body, Version::V1, &mut decoder).expect("fixture must decode"));

    let (decoded, stats) =
        measure(|| decode(&body, Version::V1, &mut decoder).expect("fixture must decode"));
    assert_eq!(decoded.samples, 100);
    assert_eq!(decoded.groups.len(), 1, "one timestamp, one group");
    expect_allocs("prometheus_in: decode 1 remote-write 1.0 request (100 series)", stats, 1526);
}

/// The same request in 2.0: 1428 against 1.0's 1526. Both build the same owned `String` label
/// pairs for `MetricFamily`; the difference is what prost materializes. 1.0 decodes a
/// `Label { name, value }` pair per label per series (~200 `String`s here); 2.0 decodes the symbol
/// table once (~105) and `labels_refs` as plain `u32`s.
///
/// The `Metadata` 2.0 repeats on every series of a family costs nothing: declarations are
/// deduplicated by family name before any group opens. Without that, this count would be 1624.
#[test]
fn remote_write_decode_one_request_v2() {
    use logit_proto::prometheus::remote_write::{decode, Version};

    let mut decoder = fixtures::prometheus_decoder();
    let body = fixtures::remote_write_request_v2();
    drop(decode(&body, Version::V2, &mut decoder).expect("fixture must decode"));

    let (decoded, stats) =
        measure(|| decode(&body, Version::V2, &mut decoder).expect("fixture must decode"));
    assert_eq!(decoded.samples, 100);
    assert_eq!(decoded.groups.len(), 1, "one timestamp, one group");
    expect_allocs("prometheus_in: decode 1 remote-write 2.0 request (100 series)", stats, 1428);
}

/// `generate_in`'s **prototype** render path: no placeholder anywhere in the template, so one
/// `Event` is rendered once at construction and `clone`d per generated event with only
/// `timestamp` overwritten (`crates/logit-inputs/src/generate.rs`'s module doc).
///
/// One allocation for a hundred events, the batch's `Vec<Event>`, and nothing per event: this
/// shape's `Event::clone` is free. One attribute fits `AttrMap`'s inline capacity, one metric fits
/// `MetricList`'s, and the log body's `Bytes` clone is a refcount bump. That keeps the generator
/// negligible next to whatever a scenario puts downstream.
///
/// The warm-up call renders the prototype and pays the one `#[cold]` `Bytes` promotion a
/// freshly built buffer's first clone costs (`docs/design/memory.md`'s "Fixtures" section).
#[test]
fn generate_render_literal_100_events() {
    let mut input = fixtures::generate_literal();
    drop(input.build_batch(0, 0, 100, 1));

    let (batch, stats) = measure(|| input.build_batch(1, 100, 100, 2));
    assert_eq!(batch.events.len(), 100);
    expect_allocs("generate_in: render 100 events (literal)", stats, 1);
}

/// `generate_in`'s per-event render path, with two templated fields (`{seq%50}` in the log body,
/// `{seq%10}` in the `host` attribute).
///
/// `1 + 2 × 100`: the batch's `Vec<Event>`, plus one `Bytes::copy_from_slice` per templated field
/// per event. The scratch `String` has already grown to its widest rendering, so it never
/// reallocates (`logit_core::template::Compiled::render`, pinned by
/// `rendering_into_a_cleared_scratch_string_reallocates_nothing`). Literal fields cost nothing.
/// Each placeholder costs an allocation per event (`docs/plans/load-test-harness.md`).
#[test]
fn generate_render_templated_100_events() {
    let mut input = fixtures::generate_templated();
    drop(input.build_batch(0, 0, 100, 1));

    let (batch, stats) = measure(|| input.build_batch(1, 100, 100, 2));
    assert_eq!(batch.events.len(), 100);
    expect_allocs("generate_in: render 100 events (2 templated fields)", stats, 201);
}

// ---------------------------------------------------------------------------------------------
// Listener receive queue and batch accumulator (docs/adr/decoupled-listener-io.md)
// ---------------------------------------------------------------------------------------------

/// A push then a pop on a warm queue: the steady-state cost `read_loop`/`decode_loop` pay per
/// datagram, excluding the datagram's own copy
/// ([`datagram_copy_is_one_right_sized_allocation`]).
///
/// Neither call suspends here, but a `current_thread` runtime is still needed to drive the futures
/// from a non-`async` `measure()` closure.
#[test]
fn receive_queue_push_then_pop_costs_nothing() {
    use logit_inputs::udp::{Datagram, RECEIVE_QUEUE_METRICS};
    use logit_pipeline::{BoundedQueue, OverflowPolicy, QueueConfig};

    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let queue = BoundedQueue::with_metrics(
        QueueConfig { max_items: 16, max_weight: u64::MAX, overflow: OverflowPolicy::DropOldest },
        &RECEIVE_QUEUE_METRICS,
        Telemetry::default(),
    );
    // Built once, outside the measured region: cloning the `Bytes` per push is a refcount bump,
    // where calling the fixture would allocate its `String` each time.
    let payload = fixtures::statsd_datagram(1);
    let warm = || Datagram { bytes: payload.clone(), received_at: 0 };
    rt.block_on(queue.push(warm()));
    rt.block_on(queue.pop()); // warm: past InMemoryBuffer's initial with_capacity sizing

    let (popped, stats) = measure(|| {
        rt.block_on(queue.push(warm()));
        rt.block_on(queue.pop())
    });
    assert!(popped.is_some());
    expect_allocs("receive queue: push then pop, warm", stats, 0);
}

/// The batched twin of the row above (`docs/adr/udp-intake-batching-and-socket-visibility.md`):
/// `push_many` then `pop_many` costs nothing either. Both `Vec`s are the caller's, reused:
/// `push_many` drains the input keeping its capacity, and `pop_many` appends into an output the
/// caller clears. A nonzero count means one of them was rebuilt instead of reused.
#[test]
fn receive_queue_push_many_then_pop_many_costs_nothing() {
    use logit_inputs::udp::{Datagram, RECEIVE_QUEUE_METRICS};
    use logit_pipeline::{BoundedQueue, OverflowPolicy, QueueConfig};

    const BATCH: usize = 8;

    let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
    let queue = BoundedQueue::with_metrics(
        QueueConfig { max_items: 16, max_weight: u64::MAX, overflow: OverflowPolicy::DropOldest },
        &RECEIVE_QUEUE_METRICS,
        Telemetry::default(),
    );
    let payload = fixtures::statsd_datagram(1);
    let datagram = || Datagram { bytes: payload.clone(), received_at: 0 };
    // One round outside the measured region grows both `Vec`s and the `VecDeque`, as
    // `decode_loop`'s reuse does, so the measured round is steady state.
    let mut inbound: Vec<Datagram> = Vec::new();
    let mut outbound: Vec<Datagram> = Vec::new();
    let round = |inbound: &mut Vec<Datagram>, outbound: &mut Vec<Datagram>| {
        inbound.extend((0..BATCH).map(|_| datagram()));
        outbound.clear();
        rt.block_on(queue.push_many(inbound));
        rt.block_on(queue.pop_many(outbound, BATCH))
    };
    round(&mut inbound, &mut outbound);

    let (popped, stats) = measure(|| round(&mut inbound, &mut outbound));
    assert_eq!(popped, BATCH);
    expect_allocs("receive queue: push_many then pop_many, warm", stats, 0);
}

/// `BatchAccumulator::absorb` into a warm buffer costs nothing. It takes `&mut Vec<Event>` so
/// `Vec::append` can drain the caller's buffer and keep its capacity; `std::mem::take` would
/// replace it with a capacity-0 `Vec` and allocate on the next call.
#[test]
fn accumulator_absorb_into_a_warm_buffer_costs_nothing() {
    use logit_core::{AttrMap, Event, Resource};
    use logit_pipeline::BatchAccumulator;

    let mut acc = BatchAccumulator::new(1_000, u64::MAX);
    let resource = Arc::new(Resource::default());
    let mut events = vec![Event::empty(0, AttrMap::new())];
    assert!(acc.absorb(Arc::clone(&resource), None, &mut events).is_none()); // warm: grows acc
    events.push(Event::empty(0, AttrMap::new())); // re-fill the (now-empty, still-capacity) buffer

    let (flushed, stats) = measure(|| acc.absorb(Arc::clone(&resource), None, &mut events));
    assert!(flushed.is_none());
    expect_allocs("accumulator: absorb into a warm buffer", stats, 0);
}

// ---------------------------------------------------------------------------------------------
// Transforms
// ---------------------------------------------------------------------------------------------

/// `json` on the nginx shape, warm: one allocation, `event.attributes` spilling its inline capacity
/// as the merge takes it from 4 to 10 entries.
///
/// `ValueSeed` deserializes straight into `Value`, keeping unescaped strings as zero-copy slices of
/// the message. Parsed pairs collect in a scratch `Vec<(Symbol, Value)>` held on `JsonParser` and
/// cleared per call, merged only on full success so a malformed object can't half-populate the
/// event. Keys are interned straight off the deserializer (`KeySeed`), never an owned `String`.
///
/// The warm-up also fills the parser's key cache (`logit_core::interner::KeyCache`, one `Box<str>`
/// per first-seen key), so every measured key is a cache hit merged by `Symbol`
/// (`AttrMap::insert_sym`). A cold parser would show 6 more.
#[test]
fn json_parse_one_event() {
    let mut json = fixtures::json_parser();
    let resource = fixtures::resource();
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::nginx_syslog_datagram(1);
    let mut decode_one = || {
        decoder.decode(datagram.clone()).expect("should decode").events.pop().expect("one event")
    };

    let mut warm = decode_one();
    json.process(&resource, &mut warm);

    let mut event = decode_one();
    let (forwarded, stats) = measure(|| json.process(&resource, &mut event));
    assert!(forwarded, "json forwards");
    assert_eq!(event.attributes.len(), 10, "6 JSON fields plus 4 syslog.* attributes");
    expect_allocs("json: parse + merge 1 event", stats, 1);
}

/// The wide-JSON shape: 28 flat top-level fields (`fixtures::WIDE_JSON_SYSLOG_LINE`, modeled on
/// pino's default output) against the nginx fixture's 6. The same one allocation (the spill) as
/// [`json_parse_one_event`]: keys are interned straight off the deserializer, so nothing scales
/// with field count.
#[test]
fn json_parse_wide_json_event() {
    let mut json = fixtures::json_parser();
    let resource = fixtures::resource();
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::wide_json_syslog_datagram(1);
    let mut decode_one = || {
        decoder.decode(datagram.clone()).expect("should decode").events.pop().expect("one event")
    };

    let mut warm = decode_one();
    json.process(&resource, &mut warm);

    let mut event = decode_one();
    let (forwarded, stats) = measure(|| json.process(&resource, &mut event));
    assert!(forwarded, "json forwards");
    assert_eq!(event.attributes.len(), 32, "28 JSON fields plus 4 syslog.* attributes");
    expect_allocs("json: parse + merge 1 wide-JSON event", stats, 1);
}

/// The key cache's off-path: warmed on [`fixtures::NGINX_SYSLOG_LINE`], then fed the same six
/// keys in reverse order (`fixtures::NGINX_SYSLOG_LINE_REVERSED_KEYS`). Every key is still a hit,
/// found by the cache's wrapping scan rather than at its cursor, so the count is the in-order
/// line's one `AttrMap` spill, with no re-intern or new cache entry.
#[test]
fn json_parse_reordered_keys_event() {
    let mut json = fixtures::json_parser();
    let resource = fixtures::resource();
    let mut decoder = fixtures::syslog_decoder();
    let warm = fixtures::nginx_syslog_datagram(1);
    let reversed = fixtures::nginx_syslog_datagram_reversed_keys();

    let mut warm = decoder.decode(warm).expect("should decode").events.pop().expect("one event");
    json.process(&resource, &mut warm);

    let mut event =
        decoder.decode(reversed).expect("should decode").events.pop().expect("one event");
    let (forwarded, stats) = measure(|| json.process(&resource, &mut event));
    assert!(forwarded, "json forwards");
    assert_eq!(event.attributes.len(), 10, "6 JSON fields plus 4 syslog.* attributes");
    expect_allocs("json: parse + merge 1 event with the keys reordered", stats, 1);
}

/// `csv`'s counterpart to [`json_parse_one_event`]: zero for seven columns. The quoted field holds
/// the delimiter (`"/a,b"`) but no doubled quote, so it still slices the message
/// (`docs/adr/csv-positional-columns.md`'s zero-copy path), and seven entries stay inline.
#[test]
fn csv_parse_one_event() {
    let mut csv = fixtures::csv_parser();
    let resource = fixtures::resource();

    let mut warm = fixtures::csv_event(fixtures::CSV_ACCESS_LINE);
    csv.process(&resource, &mut warm);

    let mut event = fixtures::csv_event(fixtures::CSV_ACCESS_LINE);
    let (forwarded, stats) = measure(|| csv.process(&resource, &mut event));
    assert!(forwarded, "csv forwards");
    assert_eq!(event.attributes.len(), 7, "seven csv columns, no pre-existing attributes");
    expect_allocs("csv: parse + merge 1 event (quoted, no escaping)", stats, 0);
}

/// The one path in `csv` that allocates: a quoted field containing a doubled `""`, which
/// `unescape` must copy. Every other field on the line stays zero-copy.
#[test]
fn csv_parse_quoted_field_with_doubled_quotes() {
    let mut csv = fixtures::csv_parser();
    let resource = fixtures::resource();
    let line = r#"10.0.0.1,2026-09-07T06:52:01Z,GET,"/a""b",200,612,0.012"#;

    let mut warm = fixtures::csv_event(line);
    csv.process(&resource, &mut warm);

    let mut event = fixtures::csv_event(line);
    let (forwarded, stats) = measure(|| csv.process(&resource, &mut event));
    assert!(forwarded, "csv forwards");
    assert_eq!(event.attributes.get("path"), Some(&Value::str(r#"/a"b"#)));
    expect_allocs("csv: parse + merge 1 event (one doubled-quote field)", stats, 1);
}

/// Sixteen unquoted columns: one allocation, `event.attributes` spilling past its 8 inline slots.
#[test]
fn csv_parse_wide_row_event() {
    let mut csv = fixtures::csv_wide_parser();
    let resource = fixtures::resource();

    let mut warm = fixtures::csv_event(fixtures::CSV_WIDE_LINE);
    csv.process(&resource, &mut warm);

    let mut event = fixtures::csv_event(fixtures::CSV_WIDE_LINE);
    let (forwarded, stats) = measure(|| csv.process(&resource, &mut event));
    assert!(forwarded, "csv forwards");
    assert_eq!(event.attributes.len(), 16, "sixteen csv columns");
    expect_allocs("csv: parse + merge 1 wide row (16 columns)", stats, 1);
}

/// Three named captures onto an event with no attributes: zero, and the map stays inline
/// (`docs/adr/regex-transform.md`). `captures_read` fills a struct-held `CaptureLocations` rather
/// than a fresh `Captures`, and each capture is a `Bytes` slice of the haystack.
#[test]
fn regex_capture_into_an_inline_map() {
    let mut re = fixtures::regex_parser();
    let resource = fixtures::resource();
    let mut warm = fixtures::sshd_message_event();
    re.process(&resource, &mut warm);

    let mut event = fixtures::sshd_message_event();
    let (forwarded, stats) = measure(|| re.process(&resource, &mut event));
    assert!(forwarded, "regex forwards");
    assert_eq!(event.attributes.len(), 3, "ssh_user, client_address, client_port");
    expect_allocs("regex: capture into an inline map", stats, 0);
}

/// The sshd shape: three captures on top of six `syslog.*` attributes take the map to 9 entries,
/// one spill past `AttrMap`'s 8 inline slots.
#[test]
fn regex_parse_one_event() {
    let mut re = fixtures::regex_parser();
    let resource = fixtures::resource();
    let mut warm = fixtures::sshd_event();
    re.process(&resource, &mut warm);

    let mut event = fixtures::sshd_event();
    let (forwarded, stats) = measure(|| re.process(&resource, &mut event));
    assert!(forwarded, "regex forwards");
    assert_eq!(event.attributes.len(), 9, "6 syslog.* attributes plus 3 captures");
    expect_allocs("regex: parse 1 event (sshd shape)", stats, 1);
}

/// A non-matching line: no capture is written, and nothing allocates.
#[test]
fn regex_no_match_one_event() {
    let mut re = fixtures::regex_parser();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    re.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let attrs_before = event.attributes.len();
    let (forwarded, stats) = measure(|| re.process(&resource, &mut event));
    assert!(forwarded, "regex forwards");
    assert_eq!(event.attributes.len(), attrs_before, "no match, no attribute added");
    expect_allocs("regex: no match, 1 event", stats, 0);
}

/// `logfmt` on [`fixtures::LOGFMT_LINE`]'s 9 unquoted or escape-free fields: every value is a
/// `Bytes` slice of the line, so the one allocation is `event.attributes` spilling past 8 entries.
#[test]
fn logfmt_parse_one_event() {
    let mut logfmt = fixtures::logfmt_parser();
    let resource = fixtures::resource();
    let mut warm = fixtures::logfmt_event();
    logfmt.process(&resource, &mut warm);

    let mut event = fixtures::logfmt_event();
    let (forwarded, stats) = measure(|| logfmt.process(&resource, &mut event));
    assert!(forwarded, "logfmt forwards");
    assert_eq!(event.attributes.len(), 9, "every logfmt field should have landed");
    expect_allocs("logfmt: parse + merge 1 event", stats, 1);
}

/// [`fixtures::LOGFMT_ESCAPED_LINE`]'s `query` value contains an escaped quote, so `unescape` must
/// copy it: one allocation. The three attributes stay inline.
#[test]
fn logfmt_parse_escaped_value_event() {
    let mut logfmt = fixtures::logfmt_parser();
    let resource = fixtures::resource();
    let mut warm = fixtures::logfmt_escaped_event();
    logfmt.process(&resource, &mut warm);

    let mut event = fixtures::logfmt_escaped_event();
    let (forwarded, stats) = measure(|| logfmt.process(&resource, &mut event));
    assert!(forwarded, "logfmt forwards");
    assert_eq!(event.attributes.len(), 3, "every logfmt field should have landed");
    expect_allocs("logfmt: parse + merge 1 escaped-value event", stats, 1);
}

/// `kv`'s mirror of [`logfmt_parse_one_event`]: zero. [`fixtures::KV_LINE`]'s three values are
/// slices of the line (`kv` has no quoting or escaping), and three entries stay inline.
#[test]
fn kv_parse_one_event() {
    let mut kv = fixtures::kv_parser();
    let resource = fixtures::resource();
    let mut warm = fixtures::kv_event();
    kv.process(&resource, &mut warm);

    let mut event = fixtures::kv_event();
    let (forwarded, stats) = measure(|| kv.process(&resource, &mut event));
    assert!(forwarded, "kv forwards");
    assert_eq!(event.attributes.len(), 3, "every kv field should have landed");
    expect_allocs("kv: parse + merge 1 event", stats, 0);
}

/// `logfmt`'s zero-copy claim, stated structurally: an unquoted value and a quoted, escape-free
/// value both point into the message buffer, and a value with an escaped quote doesn't (it went
/// through `unescape`, the module's only allocating path).
#[test]
fn logfmt_values_share_the_message_allocation() {
    let mut logfmt = fixtures::logfmt_parser();
    let resource = fixtures::resource();

    let message = bytes::Bytes::from_static(fixtures::LOGFMT_LINE.as_bytes());
    let mut event = logit_core::Event::log(
        0,
        logit_core::AttrMap::new(),
        logit_core::LogRecord {
            message: Value::Str(message.clone()),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    assert!(logfmt.process(&resource, &mut event), "logfmt forwards");

    let Some(Value::Str(status)) = event.attributes.get("status") else {
        panic!("status should be a Str")
    };
    assert!(points_into(&message, status), "an unquoted value should slice the message");

    let Some(Value::Str(msg)) = event.attributes.get("msg") else { panic!("msg should be a Str") };
    assert!(points_into(&message, msg), "a quoted, escape-free value should slice the message");

    let mut escaped = fixtures::logfmt_parser();
    let escaped_message = bytes::Bytes::from_static(fixtures::LOGFMT_ESCAPED_LINE.as_bytes());
    let mut escaped_event = logit_core::Event::log(
        0,
        logit_core::AttrMap::new(),
        logit_core::LogRecord {
            message: Value::Str(escaped_message.clone()),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    assert!(escaped.process(&resource, &mut escaped_event), "logfmt forwards");
    let Some(Value::Str(query)) = escaped_event.attributes.get("query") else {
        panic!("query should be a Str")
    };
    assert!(
        !points_into(&escaped_message, query),
        "an escaped value must not point into the message -- it was unescaped into a fresh String"
    );
}

/// Four metrics attached: one allocation, the `MetricList` spill past its single inline slot,
/// sized once by `process`'s `reserve`. The two distributions are raw `MetricKind::Samples` whose
/// one value sits inline (`docs/adr/kv-metrics-semantics.md`), so they cost nothing.
#[test]
fn kv_metrics_one_event() {
    let mut kv = fixtures::kv_metrics();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    kv.process(&resource, &mut warm);

    let mut event = {
        // `nginx_event` already ran kv_metrics; rebuild the pre-kv_metrics shape by hand.
        let mut decoder = fixtures::syslog_decoder();
        let mut json = fixtures::json_parser();
        let batch = decoder.decode(fixtures::nginx_syslog_datagram(1)).expect("should decode");
        let mut e = batch.events.into_iter().next().expect("one event");
        assert!(json.process(&resource, &mut e), "json forwards");
        e
    };

    let (forwarded, stats) = measure(|| kv.process(&resource, &mut event));
    assert!(forwarded, "kv_metrics forwards");
    assert_eq!(event.metrics.len(), 4);
    expect_allocs("kv_metrics: derive 4 metrics", stats, 1);
}

/// Zero: `filtered` rebuilds the map, but three surviving attributes fit inline. Its per-key
/// `resolve` -> `intern` round trip is CPU cost this count can't see.
#[test]
fn keep_one_event() {
    let mut keep = fixtures::keep();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    keep.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| keep.process(&resource, &mut event));
    assert!(forwarded, "keep forwards");
    assert_eq!(event.attributes.len(), 3);
    expect_allocs("keep: filter to 3 attributes", stats, 0);
}

/// Zero: `nginx_event`'s `host` is `static.local`, allowed and already lowercase, so
/// `Clamp::normalize` returns `None` and the allowed path never calls `insert_sym`.
#[test]
fn keep_values_one_event() {
    let mut kv = fixtures::keep_values();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    kv.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| kv.process(&resource, &mut event));
    assert!(forwarded, "keep_values forwards");
    assert_eq!(event.attributes.get("host"), Some(&Value::str("static.local")));
    expect_allocs("keep_values: host already lowercase and allowed", stats, 0);
}

/// One allocation: `STATIC.LOCAL` has an uppercase byte, so `normalize: [lower]` builds a new
/// `Bytes` to write back, the one path in `keep_values` that isn't free. Compare
/// [`keep_values_one_event`].
#[test]
fn keep_values_one_event_needs_lowering() {
    let mut kv = fixtures::keep_values();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event_with_uppercase_host();
    kv.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event_with_uppercase_host();
    let (forwarded, stats) = measure(|| kv.process(&resource, &mut event));
    assert!(forwarded, "keep_values forwards");
    assert_eq!(event.attributes.get("host"), Some(&Value::str("static.local")));
    expect_allocs("keep_values: host needs lowering before it's allowed", stats, 1);
}

/// Zero: `nginx_event`'s attributes are all flat, so `flatten`'s phase-1 scan selects nothing and
/// nothing is interned, removed, or reinserted (`docs/adr/flatten-transform.md`).
#[test]
fn flatten_already_flat_event() {
    let mut f = fixtures::flatten();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    f.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| f.process(&resource, &mut event));
    assert!(forwarded, "flatten forwards");
    expect_allocs("flatten: already-flat event", stats, 0);
}

/// `pino_http_event`'s `req`/`res` (and their nested `headers`) expand into dot-joined attributes,
/// with every path already in the `KeyCache`. One allocation: six flat attributes plus eight
/// leaves, 14 total, spill the map past 8 slots. `SmallVec`'s first over-capacity push grows to 16
/// slots (`bytes=768`, 48 bytes each), which the remaining inserts fit inside.
#[test]
fn flatten_pino_http_event_warm() {
    let mut f = fixtures::flatten();
    let resource = fixtures::resource();
    let mut warm = fixtures::pino_http_event();
    f.process(&resource, &mut warm);

    let mut event = fixtures::pino_http_event();
    let (forwarded, stats) = measure(|| f.process(&resource, &mut event));
    assert!(forwarded, "flatten forwards");
    assert_eq!(
        event.attributes.get("req.headers.host"),
        Some(&Value::str("api.example.com")),
        "a doubly-nested path should have expanded"
    );
    expect_allocs("flatten: pino-http shape, warm KeyCache", stats, 1);
}

/// [`flatten_pino_http_event_warm`]'s shape on a cold component: 12. Each of the eight distinct
/// paths (`req.method`, `req.headers.host`, ...) is a `KeyCache` miss that stores its own
/// `Box<str>` copy, and the component's scratch buffers (`pending`, `path`, the cache's `Vec`)
/// grow for the first time, on top of the warm case's one spill. A process pays this once per
/// new path, not per event.
#[test]
fn flatten_pino_http_event_cold_key_cache() {
    let mut f = fixtures::flatten();
    let resource = fixtures::resource();

    let mut event = fixtures::pino_http_event();
    let (forwarded, stats) = measure(|| f.process(&resource, &mut event));
    assert!(forwarded, "flatten forwards");
    assert_eq!(event.attributes.get("req.headers.host"), Some(&Value::str("api.example.com")));
    expect_allocs("flatten: pino-http shape, cold KeyCache (first event)", stats, 12);
}

// -- sample (docs/adr/consistent-sampling-component.md) -----------------------------------------
//
// Every `sample` path is free: the key and override names are interned once at construction,
// lookups are `AttrMap::get_sym`, a trace id is hex-encoded into a stack array, a number is
// formatted straight into the streaming hasher (`logit_core::sampling`'s `KeyHasher`), and the
// per-batch telemetry tally is plain integers. Each of the four decision paths is pinned
// separately, on the same event twice (warm, then measured) so the verdict matches.

/// `key: trace_id` on a span -- hex-encode the 16 bytes on the stack, one XXH64.
#[test]
fn sample_by_span_trace_id_one_event() {
    let mut s = fixtures::sample_by_trace_id();
    let resource = fixtures::resource();
    let mut warm = fixtures::span_event();
    let expected = s.process(&resource, &mut warm);

    let mut event = fixtures::span_event();
    let (forwarded, stats) = measure(|| s.process(&resource, &mut event));
    assert_eq!(forwarded, expected, "same trace id, same verdict");
    expect_allocs("sample: key trace_id, span event", stats, 0);
}

/// `key: {attribute: status}` on `nginx_event`'s `I64(200)` -- `write!` into the hasher, no
/// intermediate `String`.
#[test]
fn sample_by_integer_attribute_one_event() {
    let mut s = fixtures::sample_by_status();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    let expected = s.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| s.process(&resource, &mut event));
    assert_eq!(forwarded, expected, "same key, same verdict");
    expect_allocs("sample: key attribute, I64 value", stats, 0);
}

/// No key -- a counter mixed with the seed through the same hash.
#[test]
fn sample_random_one_event() {
    let mut s = fixtures::sample_random();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    s.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let (_, stats) = measure(|| s.process(&resource, &mut event));
    expect_allocs("sample: keyless draw", stats, 0);
}

/// `always_keep` hit -- one `get_sym` and a `value_matches` string compare, before the key is
/// ever looked at.
#[test]
fn sample_override_hit_one_event() {
    let mut s = fixtures::sample_override();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    s.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| s.process(&resource, &mut event));
    assert!(forwarded, "an always_keep hit is kept at rate 0");
    expect_allocs("sample: always_keep hit", stats, 0);
}

// -- http_access (docs/adr/http-access-normalization.md) ----------------------------------------

/// A metric-only event passes through `http_access` untouched: zero, on a component warmed with a
/// conforming line.
#[test]
fn http_access_ignores_an_event_with_no_log() {
    let mut ha = fixtures::http_access();
    let resource = fixtures::resource();
    let mut warm = fixtures::http_access_event();
    ha.process(&resource, &mut warm);

    let mut event = fixtures::statsd_event();
    assert!(event.log.is_none(), "fixture must be metric-only, or this measures the wrong thing");
    let (forwarded, stats) = measure(|| ha.process(&resource, &mut event));
    assert!(forwarded, "http_access always forwards");
    expect_allocs("http_access: ignores an event with no log", stats, 0);
}

/// A fully conforming semconv line on a warm component (every lazy `span.name`/`error.type` cell
/// filled, every alias/cap key interned): zero.
#[test]
fn http_access_normalizes_a_conforming_line_warm() {
    let mut ha = fixtures::http_access();
    let resource = fixtures::resource();
    let mut warm = fixtures::http_access_event();
    ha.process(&resource, &mut warm);

    let mut event = fixtures::http_access_event();
    let (forwarded, stats) = measure(|| ha.process(&resource, &mut event));
    assert!(forwarded, "http_access always forwards");
    assert_eq!(event.attributes.get("http.request.method"), Some(&Value::str("GET")));
    assert_eq!(event.attributes.get("span.name"), Some(&Value::str("GET /{other}")));
    expect_allocs("http_access: normalize a conforming line, warm", stats, 0);
}

/// The same line through a new component, unwarmed: 228, paid once per compiled pattern per
/// thread, not per event. Nearly all of it is the `regex` crate's lazy per-pattern `Cache`, built
/// by the first match against each `Regex`. The fixture's UA matches `browser`, the last of four
/// built-in rules, so `classify_user_agent` builds all four (176). The path matches none of the
/// four route rules, so `classify_route` builds all four too, plus the `span.name` cell's one
/// `format!` + `shared()` pair (52). Measured in isolation: 176 + 52 = 228. Without `url.path` or
/// `user_agent.original`, the line costs 0 cold. The ADR's "scanned with `is_match`, which
/// allocates nothing" holds for the warm row above.
#[test]
fn http_access_normalizes_a_conforming_line_cold() {
    let mut ha = fixtures::http_access();
    let resource = fixtures::resource();

    let mut event = fixtures::http_access_event();
    let (forwarded, stats) = measure(|| ha.process(&resource, &mut event));
    assert!(forwarded, "http_access always forwards");
    assert_eq!(event.attributes.get("http.request.method"), Some(&Value::str("GET")));
    expect_allocs("http_access: normalize a conforming line, cold", stats, 228);
}

/// [`fixtures::http_access_dashed_event`]: the same line, every key in its dashed alias --
/// `dealias` renames each before step 1 runs, so the count matches the warm conforming line: zero.
#[test]
fn http_access_normalizes_a_dashed_line_warm() {
    let mut ha = fixtures::http_access();
    let resource = fixtures::resource();
    let mut warm = fixtures::http_access_event();
    ha.process(&resource, &mut warm);

    let mut event = fixtures::http_access_dashed_event();
    let (forwarded, stats) = measure(|| ha.process(&resource, &mut event));
    assert!(forwarded, "http_access always forwards");
    assert_eq!(event.attributes.get("http.request.method"), Some(&Value::str("GET")));
    assert_eq!(event.attributes.get("span.name"), Some(&Value::str("GET /{other}")));
    expect_allocs("http_access: normalize a dashed line, warm", stats, 0);
}

/// [`fixtures::http_access_event_with_control_byte`]: the same conforming line, except
/// `user_agent.original` carries one raw `0x01` byte. Step 7's cap-and-clean copies the value
/// through `scratch` to blank it, the one path that isn't free.
///
/// Two, not one: the clean warm-up line never takes the dirty branch, so `scratch` (a
/// `Vec::new()`) arrives at capacity 0. The dirty arm pays its first growth (an `alloc`, with
/// nothing to `realloc`) and the exact-size `Bytes::copy_from_slice` out. Once any earlier event
/// has taken this branch the count is 1, the module doc's "one exact-size copy".
#[test]
fn http_access_cleans_a_control_byte() {
    let mut ha = fixtures::http_access();
    let resource = fixtures::resource();
    let mut warm = fixtures::http_access_event();
    ha.process(&resource, &mut warm);

    let mut event = fixtures::http_access_event_with_control_byte();
    let (forwarded, stats) = measure(|| ha.process(&resource, &mut event));
    assert!(forwarded, "http_access always forwards");
    let ua = event
        .attributes
        .get("user_agent.original")
        .and_then(Value::as_str)
        .expect("user_agent.original should still be a Str");
    assert!(!ua.contains('\u{1}'), "the control byte should have been cleaned");
    expect_allocs("http_access: cleans a control byte", stats, 2);
}

/// `shape` is the one transform here that doesn't aim for zero
/// ([ADR `shape-observer-component`](../../../docs/adr/shape-observer-component.md)): a
/// measurement event carries a dozen-odd metric records, so it always spills `MetricList`'s single
/// inline slot. This and the next two tests pin that cost on three fixture shapes.
///
/// The statsd shape (a few attributes, one metric, no log or span): one allocation, the spill.
/// The incoming `MetricList` holds its record inline, so `reserve`ing room for the measurement
/// records goes to the heap. The tags fit inline, and every `Samples` fits `SAMPLES_INLINE`.
#[test]
fn shape_one_statsd_event() {
    let mut shape = fixtures::shape();
    let resource = fixtures::resource();
    let mut warm = fixtures::statsd_event();
    shape.process(&resource, &mut warm);

    let mut event = fixtures::statsd_event();
    let (forwarded, stats) = measure(|| shape.process(&resource, &mut event));
    assert!(forwarded, "shape never absorbs an event");
    expect_allocs("shape: measure 1 statsd event", stats, 1);
}

/// The nginx shape (10 attributes, 4 metrics, a log body): more measurements than the statsd shape
/// and cheaper, zero allocations and one realloc. `shape` clears and refills
/// `event.attributes`/`event.metrics` rather than replacing them, and this event arrives with both
/// already spilled, so the tags land in existing storage and `reserve` grows the existing
/// `MetricList` buffer (a realloc, not an alloc). A wider event costs the tap less, not more.
#[test]
fn shape_one_nginx_event() {
    let mut shape = fixtures::shape();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    shape.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| shape.process(&resource, &mut event));
    assert!(forwarded, "shape never absorbs an event");
    assert_eq!(stats.reallocs, 1, "the `MetricList` growth, not a fresh allocation");
    expect_allocs("shape: measure 1 nginx event", stats, 0);
}

/// The wide-JSON shape (32 string attributes): three allocations. One `MetricList` spill, plus one
/// each for `logit.shape.key_bytes` and `logit.shape.value_bytes`, whose 32 values apiece exceed
/// `SAMPLES_INLINE`'s 19. Those two are inherent (a per-attribute measurement of 32 attributes is
/// 32 numbers), which is why `shape` belongs on a tap branch rather than in the flow.
#[test]
fn shape_one_wide_json_event() {
    let mut shape = fixtures::shape();
    let resource = fixtures::resource();
    let mut json = fixtures::json_parser();
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::wide_json_syslog_datagram(1);
    let mut decode_one = || {
        let mut event = decoder
            .decode(datagram.clone())
            .expect("should decode")
            .events
            .pop()
            .expect("one event");
        assert!(json.process(&resource, &mut event), "json forwards");
        event
    };

    let mut warm = decode_one();
    shape.process(&resource, &mut warm);

    let mut event = decode_one();
    assert_eq!(event.attributes.len(), 32);
    let (forwarded, stats) = measure(|| shape.process(&resource, &mut event));
    assert!(forwarded, "shape never absorbs an event");
    expect_allocs("shape: measure 1 wide-JSON event", stats, 3);
}

/// Zero in steady state, because `keep` ran first. `aggregate` clones the attribute map into a
/// `SeriesKey` per metric per event, but three attributes fit inline, so the clone is a 400-byte
/// memcpy and the `HashMap` entry hits. Compare [`aggregate_absorb_without_keep`].
#[test]
fn aggregate_absorb_one_event() {
    let mut agg = fixtures::aggregator();
    let resource = fixtures::resource();
    let mut keep = fixtures::keep();
    let mut trimmed = || {
        let mut event = fixtures::nginx_event();
        assert!(keep.process(&resource, &mut event), "keep forwards");
        event
    };

    for _ in 0..4 {
        let mut event = trimmed();
        agg.process(&resource, &mut event);
    }

    let mut event = trimmed();
    let (_, stats) = measure(|| agg.process(&resource, &mut event));
    expect_allocs("aggregate: absorb 1 event (after keep)", stats, 0);
}

/// The measurement behind "put `keep` before `aggregate`" (`logit_transforms::keep`'s docs). Same
/// events with `keep` removed: 4, one `SeriesKey` clone of the spilled 10-attribute map per metric
/// per event.
///
/// The cardinality cost isn't visible here: `syslog.timestamp` is distinct per line, so without
/// `keep` every event would also open its own series and the window would grow without bound.
#[test]
fn aggregate_absorb_without_keep() {
    let mut agg = fixtures::aggregator();
    let resource = fixtures::resource();
    for _ in 0..4 {
        let mut event = fixtures::nginx_event();
        agg.process(&resource, &mut event);
    }

    let mut event = fixtures::nginx_event();
    let (_, stats) = measure(|| agg.process(&resource, &mut event));
    expect_allocs("aggregate: absorb 1 event (no keep)", stats, 4);
}

/// Flushing 4 series: 6, the flush's `events` and `result` `Vec`s plus one `Vec<SpanLink>` per
/// series (`docs/adr/trace-context-propagation-on-delivered.md`'s flush-side linking). The fixture
/// never calls `observe_batch_context`, so each series' `ContributingContexts` holds one default
/// context, and `into_links()` collects it into its own `Vec`. A non-empty `Vec` always
/// allocates, so one per series is the floor, and more contributors (up to the cap) don't add to
/// it.
#[test]
fn aggregate_flush_100_series() {
    let resource = fixtures::resource();
    let mut keep = fixtures::keep();
    let mut agg = fixtures::aggregator();
    for _ in 0..100 {
        let mut event = fixtures::nginx_event();
        assert!(keep.process(&resource, &mut event), "keep forwards");
        agg.process(&resource, &mut event);
    }

    let (flushed, stats) = measure(|| agg.flush(1_000_000_000));
    let series: usize = flushed.iter().map(|(_, _, events)| events.len()).sum();
    assert_eq!(series, 4, "one series per metric name -- keep bounds the tag set");
    expect_allocs("aggregate: flush 4 series", stats, 6);
}

/// The retention path's flush cost: 209 for 100 retained gauge series. `flush`'s retain branch
/// (`series_retention > 0`) clones `key.attributes` where the tumbling path moves it, because the
/// key must survive into the next window. The fixture isn't `keep`-trimmed (12 attributes, past 8
/// inline slots) so that clone allocates, one per series. The rest is each series'
/// `Vec<SpanLink>`, the flush's own `Vec`s, and the emptied `series` `HashMap` regrowing its table
/// as the survivors go back in (`docs/design/memory.md`'s "Series retention's own cost" section).
///
/// Measured over a second flush, after a warm-up flush retained all 100 series: the steady state.
#[test]
fn aggregate_flush_retained_gauges() {
    let resource = fixtures::resource();
    let mut agg = fixtures::aggregator_with_series_retention(5, 1_000);
    for i in 0..100 {
        let mut event = fixtures::wide_gauge_event(&format!("gauge{i}"), i as f64);
        agg.process(&resource, &mut event);
    }
    drop(agg.flush(1_000_000_000)); // warm: interns every series name, grows every buffer once

    for i in 0..100 {
        let mut event = fixtures::wide_gauge_event(&format!("gauge{i}"), (i + 1) as f64);
        agg.process(&resource, &mut event);
    }

    let (flushed, stats) = measure(|| agg.flush(2_000_000_000));
    let series: usize = flushed.iter().map(|(_, _, events)| events.len()).sum();
    assert_eq!(series, 100, "every series was updated again before this flush");
    expect_allocs("aggregate: flush 100 retained gauge series (spilled attrs)", stats, 209);
}

/// `temporality: cumulative`'s flush cost (`docs/adr/aggregation-window-semantics.md`'s cumulative
/// amendment): [`aggregate_flush_retained_gauges`]'s 100 series, spilled maps, and second flush,
/// with a retained delta `Sum` in place of a `Gauge`. The same 209 is the finding: a retained `Sum`
/// reports through the same copy-then-keep path (`Accumulator::kind_for_retained`, `Copy` fields
/// both), so cumulative counters cost nothing beyond gauge retention. A cumulative `Histogram`
/// would add one bucket-`Vec` clone per series per flush; it has no fixture yet.
#[test]
fn aggregate_flush_cumulative_sums() {
    let resource = fixtures::resource();
    let mut agg = fixtures::aggregator_cumulative(5, 1_000);
    for i in 0..100 {
        let mut event = fixtures::wide_counter_event(&format!("counter{i}"), 1.0);
        agg.process(&resource, &mut event);
    }
    drop(agg.flush(1_000_000_000)); // warm: interns every series name, grows every buffer once

    for i in 0..100 {
        let mut event = fixtures::wide_counter_event(&format!("counter{i}"), 1.0);
        agg.process(&resource, &mut event);
    }

    let (flushed, stats) = measure(|| agg.flush(2_000_000_000));
    let series: usize = flushed.iter().map(|(_, _, events)| events.len()).sum();
    assert_eq!(series, 100, "every cumulative series was updated again before this flush");
    expect_allocs("aggregate: flush 100 cumulative sum series (spilled attrs)", stats, 209);
}

/// Absorbing a raw `Samples` record in the default `distributions: sketch` mode: zero. Each value
/// sketches via `add_weighted` into the series' already-open `DdSketch`
/// (`Accumulator::Distribution`), and no raw values are retained.
#[test]
fn aggregate_absorb_one_samples_event_sketch_mode() {
    let mut agg = fixtures::aggregator();
    let resource = fixtures::resource();
    for _ in 0..4 {
        let mut event = fixtures::samples_event("app.latency", [1.0, 2.0, 3.0]);
        agg.process(&resource, &mut event);
    }

    let mut event = fixtures::samples_event("app.latency", [1.0, 2.0, 3.0]);
    let (_, stats) = measure(|| agg.process(&resource, &mut event));
    expect_allocs("aggregate: absorb one Samples event (sketch mode)", stats, 0);
}

/// Absorbing in `distributions: samples` mode: raw values concatenate into the series' `Samples`
/// accumulator (`held.values.extend(..)`). A series holding a few inline values that absorbs 25
/// more in one record spills past `SAMPLES_INLINE` (19) on this call: one allocation.
#[test]
fn aggregate_absorb_25_samples_values_into_one_series_samples_mode() {
    let mut agg = fixtures::aggregator_with_samples_retention(1_000);
    let resource = fixtures::resource();
    // Warm with a small, still-inline series so the measured record is the one that spills.
    for _ in 0..4 {
        let mut event = fixtures::samples_event("app.latency", [1.0]);
        agg.process(&resource, &mut event);
    }

    let values: Vec<f64> = (0..25).map(|i| i as f64).collect();
    let mut event = fixtures::samples_event("app.latency", values);
    let (_, stats) = measure(|| agg.process(&resource, &mut event));
    expect_allocs("aggregate: absorb 25 Samples values into one series (samples mode)", stats, 1);
}

// ---------------------------------------------------------------------------------------------
// Fan-out
// ---------------------------------------------------------------------------------------------

/// `Event::clone` on the nginx shape, the per-event cost of a fan-out branch that must copy a
/// shared batch: two allocations (the spilled `AttrMap` and the spilled `MetricList`) plus an
/// 800-byte memcpy. The two distributions are inline `Samples` ([`kv_metrics_one_event`]), so they
/// add nothing. A read-only branch (every sink) shares the `Arc<EventBatch>` and pays none of it
/// (`docs/adr/arc-eventbatch-copy-on-write.md`).
#[test]
fn clone_one_event() {
    let event = fixtures::nginx_event();
    drop(event.clone());

    let (clone, stats) = measure(|| event.clone());
    assert_eq!(clone.metrics.len(), 4);
    expect_allocs("Event::clone (nginx shape)", stats, 2);
}

/// The cheap end: a statsd counter with three tags and one metric fits `Event`'s inline capacity,
/// so cloning it is an 800-byte memcpy and zero allocations. The 800 bytes are paid whatever the
/// event carries.
#[test]
fn clone_one_statsd_event() {
    let event = fixtures::statsd_event();
    drop(event.clone());

    let (_, stats) = measure(|| event.clone());
    expect_allocs("Event::clone (statsd shape)", stats, 0);
}

/// A single-consumer edge costs nothing: `Fanout::send` skips the `Arc` and moves the batch
/// through as `Delivered::Owned` (`docs/adr/arc-eventbatch-copy-on-write.md`). This is the common
/// case: every listener's first hop and every interior edge of a linear chain.
///
/// `CountingAlloc`'s counters are thread-local (`logit_bench::alloc`'s module doc), so this runs
/// on a `current_thread` runtime, which spawns no worker threads.
#[test]
fn fanout_send_one_consumer_costs_nothing() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let fanout = Fanout::new(vec![tx]);

    let warm = fixtures::nginx_batch(1);
    rt.block_on(async {
        fanout.send(warm).await;
        drop(unwrap_delivered(rx.recv().await.expect("should receive")));
    });

    let batch = fixtures::nginx_batch(1);
    let (received, stats) = measure(|| {
        rt.block_on(async {
            fanout.send(batch).await;
            unwrap_delivered(rx.recv().await.expect("should receive"))
        })
    });
    assert_eq!(received.events.len(), 1);
    expect_allocs("fanout: send + receive, 1 consumer", stats, 0);
}

/// The test above runs with telemetry disabled, which never reaches `Telemetry::span`'s sampling
/// decision. This is the live-but-unsampled path a production pipeline with an `internal`
/// component runs for most traces
/// (`docs/adr/internal-span-emission-and-deterministic-sampling.md`): a real `Registry` with
/// `with_span_sampling(0.0)`, attached as `logit_cli::pipeline::prepare` attaches one.
/// `trace_is_sampled` says no, `Telemetry::span` returns `SpanGuard::disabled()`, and the count is
/// still zero.
#[test]
fn fanout_send_one_consumer_with_a_live_unsampled_registry_costs_nothing() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let registry = logit_core::Registry::with_span_sampling(0.0);
    let telemetry = registry.telemetry_for("in", "statsd_in", "listener");
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let fanout = Fanout::new(vec![tx]).with_telemetry(telemetry);

    // Also warms this `ComponentBuffer`'s point map: `logit.component.batches.sent`/`.events.sent`/
    // `.send.blocked.duration` each pay a first-insert growth; updating a resident key is free.
    let warm = fixtures::nginx_batch(1);
    rt.block_on(async {
        fanout.send(warm).await;
        drop(unwrap_delivered(rx.recv().await.expect("should receive")));
    });

    let batch = fixtures::nginx_batch(1);
    let (received, stats) = measure(|| {
        rt.block_on(async {
            fanout.send(batch).await;
            unwrap_delivered(rx.recv().await.expect("should receive"))
        })
    });
    assert_eq!(received.events.len(), 1);
    expect_allocs("fanout: send + receive, 1 consumer, live unsampled registry", stats, 0);
}

/// A two-consumer fan-out where both branches mutate: 4. One `Arc::new` per send, plus one full
/// `EventBatch` deep clone for the branch that unwraps while its sibling still holds the `Arc` (1
/// for the `Vec<Event>` plus [`clone_one_event`]'s 2). The branch that unwraps last is free.
///
/// Sharing doesn't make a mutating fan-out cheaper than cloning for all but the last consumer
/// would (3); it costs one more, for the `Arc`
/// (`docs/adr/arc-eventbatch-copy-on-write.md`'s "What this change actually saves" section). Two
/// fully independent copies would cost 6.
///
/// Branch "a" unwraps while "b" still holds its handle, then "b", to pin the deterministic case.
/// On a multi-thread runtime, concurrent unwrapping can cost more, never less (`unwrap_batch`'s
/// doc comment).
#[test]
fn fanout_send_two_consumers_costs_one_clone_plus_one_arc() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let (tx_a, mut rx_a) = tokio::sync::mpsc::channel(1);
    let (tx_b, mut rx_b) = tokio::sync::mpsc::channel(1);
    let fanout = Fanout::new(vec![tx_a, tx_b]);

    let warm = fixtures::nginx_batch(1);
    rt.block_on(async {
        fanout.send(warm).await;
        let a = rx_a.recv().await.expect("a should receive");
        let b = rx_b.recv().await.expect("b should receive");
        drop(unwrap_delivered(a));
        drop(unwrap_delivered(b));
    });

    let batch = fixtures::nginx_batch(1);
    let ((a, b), stats) = measure(|| {
        rt.block_on(async {
            fanout.send(batch).await;
            let delivered_a = rx_a.recv().await.expect("a should receive");
            let delivered_b = rx_b.recv().await.expect("b should receive");
            // `a` unwraps first, while `b`'s handle is still alive -- forces `a`'s clone. `b`
            // unwraps last, with nothing left holding the `Arc` -- free.
            (unwrap_delivered(delivered_a), unwrap_delivered(delivered_b))
        })
    });
    assert_eq!(a.events.len(), 1);
    assert_eq!(b.events.len(), 1);
    // 1 (Arc::new, once per send) + 3 (one EventBatch deep clone: 1 for the Vec<Event>, 2 for the
    // one nginx-shaped Event inside it, matching clone_one_event) + 0 (the other branch, free).
    expect_allocs(
        "fanout: send + receive, 2 consumers (1 clones, 1 free, +1 for the Arc)",
        stats,
        4,
    );
}

/// A copy of `logit_pipeline::unwrap_batch`'s body.
fn unwrap_delivered(delivered: Delivered) -> EventBatch {
    match delivered {
        Delivered::Owned(batch, _ctx) => batch,
        Delivered::Shared(shared, _ctx) => {
            Arc::try_unwrap(shared).unwrap_or_else(|shared| (*shared).clone())
        }
    }
}

/// The borrow `run_output` does for `Output::send(&EventBatch)`: no unwrap and no clone, however
/// many sibling branches still hold a handle.
fn borrow_delivered(delivered: &Delivered) -> &EventBatch {
    match delivered {
        Delivered::Owned(batch, _ctx) => batch,
        Delivered::Shared(shared, _ctx) => shared,
    }
}

/// A fan-out whose consumers are all `Output`s (two sinks off one node) costs only the one
/// `Arc::new` per send: both branches borrow through their `Delivered::Shared` handle, as
/// `run_output` does, and neither unwraps or clones. Compare
/// [`fanout_send_two_consumers_costs_one_clone_plus_one_arc`], where the branches mutate.
#[test]
fn fanout_send_two_output_consumers_costs_only_the_arc() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let (tx_a, mut rx_a) = tokio::sync::mpsc::channel(1);
    let (tx_b, mut rx_b) = tokio::sync::mpsc::channel(1);
    let fanout = Fanout::new(vec![tx_a, tx_b]);

    let warm = fixtures::nginx_batch(1);
    rt.block_on(async {
        fanout.send(warm).await;
        let a = rx_a.recv().await.expect("a should receive");
        let b = rx_b.recv().await.expect("b should receive");
        assert_eq!(borrow_delivered(&a).events.len(), 1);
        assert_eq!(borrow_delivered(&b).events.len(), 1);
    });

    let batch = fixtures::nginx_batch(1);
    let ((a_len, b_len), stats) = measure(|| {
        rt.block_on(async {
            fanout.send(batch).await;
            let delivered_a = rx_a.recv().await.expect("a should receive");
            let delivered_b = rx_b.recv().await.expect("b should receive");
            // Both sides borrow only, as `run_output` does.
            (
                borrow_delivered(&delivered_a).events.len(),
                borrow_delivered(&delivered_b).events.len(),
            )
        })
    });
    assert_eq!(a_len, 1);
    assert_eq!(b_len, 1);
    expect_allocs("fanout: send + receive, 2 Output consumers (borrow only)", stats, 1);
}

/// The sink-side hop after `Fanout::send` (`docs/adr/buffered-sink-delivery.md`): `drain_inbox`,
/// driven directly, on a single-consumer `Delivered::Owned` batch. One allocation, the `Arc::new`
/// that hands the batch to its `SinkQueue`. The single-consumer edge before it is free
/// ([`fanout_send_one_consumer_costs_nothing`]). Recording the batch in `in_hand` for the
/// shutdown sweep, an `Arc::clone` behind an uncontended mutex, allocates nothing.
#[test]
fn drain_inbox_single_consumer_owned_batch_costs_exactly_the_arc() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let telemetry = logit_core::Telemetry::default();
    let store =
        Arc::new(SinkStore::Memory(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone())));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    // Warm: one push+commit round trip grows the `SinkQueue`'s `VecDeque`, so the queue's first
    // push doesn't fold into the `Arc::new` this test isolates.
    let warm = fixtures::nginx_batch(1);
    rt.block_on(async {
        tx.send(Delivered::Owned(warm, BatchContext::default()))
            .await
            .expect("send should succeed");
        let warmed = match rx.recv().await.expect("should receive") {
            Delivered::Owned(batch, _ctx) => Arc::new(batch),
            Delivered::Shared(shared, _ctx) => shared,
        };
        store.push((warmed, BatchContext::default())).await;
        store.commit();
    });

    let batch = fixtures::nginx_batch(1);
    let store_for_measure = Arc::clone(&store);
    let telemetry_for_measure = telemetry.clone();
    let in_hand = InHand::default();
    let ((), stats) = measure(|| {
        rt.block_on(async move {
            tx.send(Delivered::Owned(batch, BatchContext::default()))
                .await
                .expect("send should succeed");
            drop(tx); // closes the inbox, so `drain_inbox` returns after this one batch
            drain_inbox(&mut rx, store_for_measure, telemetry_for_measure, &in_hand).await;
        })
    });

    expect_allocs("drain_inbox: single-consumer Delivered::Owned batch (the Arc::new)", stats, 1);
}

// ---------------------------------------------------------------------------------------------
// Routing (docs/adr/target-components.md)
// ---------------------------------------------------------------------------------------------

/// A `route` switching on `stream`, `host`/`app` each naming one of two targets --
/// `fixtures::nginx_batch_alternating_stream`'s own split, and the ADR's headline
/// central-collector shape (`examples/fan-out-central.yaml`).
fn route_by_stream() -> logit_transforms::Route {
    logit_transforms::Route::new(
        logit_config::RouteBy::Attribute("stream".to_string()),
        &[
            ("host".to_string(), "host_stream".to_string()),
            ("app".to_string(), "app_stream".to_string()),
        ]
        .into_iter()
        .collect(),
        &["host_stream".to_string(), "app_stream".to_string()],
    )
}

/// The partition pass (`RouterScratch`/`route_batch`'s doc comments): one `Vec::with_capacity`
/// for the returned partition list, plus one `reserve_exact` per destination that received an
/// event, never per event. 64 events split evenly host/app, both targets used: `1 + 2 = 3`.
///
/// The warm-up grows `RouterScratch`'s `marks`/`counts` once. The per-destination `reserve_exact`
/// recurs every batch: `dests[n]` is handed out by `mem::take`, which leaves capacity 0 behind.
#[test]
fn route_batch_two_targets_costs_one_vec_per_used_destination() {
    let mut route = route_by_stream();
    let telemetry = Telemetry::default();
    let mut scratch = RouterScratch::new(2);

    let warm = fixtures::nginx_batch_alternating_stream(64);
    drop(route_batch(&mut route, &mut scratch, warm, &telemetry));

    let batch = fixtures::nginx_batch_alternating_stream(64);
    let (out, stats) = measure(|| route_batch(&mut route, &mut scratch, batch, &telemetry));
    assert_eq!(out.len(), 2, "both host_stream and app_stream should receive events");
    let total: usize = out.iter().map(|(_, batch)| batch.events.len()).sum();
    assert_eq!(total, 64);
    expect_allocs("route_batch: 64 events, 2 targets both used", stats, 3);
}

/// The same partition with `host` and `app` both mapped to one target (many-to-one): one
/// destination used, `1 + 1 = 2`.
#[test]
fn route_batch_all_to_one_target_costs_two() {
    let mut route = logit_transforms::Route::new(
        logit_config::RouteBy::Attribute("stream".to_string()),
        &[("host".to_string(), "t".to_string()), ("app".to_string(), "t".to_string())]
            .into_iter()
            .collect(),
        &["t".to_string()],
    );
    let telemetry = Telemetry::default();
    let mut scratch = RouterScratch::new(1);

    let warm = fixtures::nginx_batch_alternating_stream(64);
    drop(route_batch(&mut route, &mut scratch, warm, &telemetry));

    let batch = fixtures::nginx_batch_alternating_stream(64);
    let (out, stats) = measure(|| route_batch(&mut route, &mut scratch, batch, &telemetry));
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].1.events.len(), 64);
    expect_allocs("route_batch: 64 events, 1 target used (many-to-one)", stats, 2);
}

/// The other one-destination case: nothing matches, so every event lands on the router's own
/// `Forward` partition (slot 0): `1 + 1 = 2`, the same as above, because the cost is per
/// destination used, not per event routed.
#[test]
fn route_batch_all_unrouted_costs_two() {
    let mut route = logit_transforms::Route::new(
        logit_config::RouteBy::Attribute("stream".to_string()),
        &[("db".to_string(), "t".to_string())].into_iter().collect(),
        &["t".to_string()],
    );
    let telemetry = Telemetry::default();
    let mut scratch = RouterScratch::new(1);

    let warm = fixtures::nginx_batch_alternating_stream(64);
    drop(route_batch(&mut route, &mut scratch, warm, &telemetry));

    let batch = fixtures::nginx_batch_alternating_stream(64);
    let (out, stats) = measure(|| route_batch(&mut route, &mut scratch, batch, &telemetry));
    assert_eq!(out.len(), 1, "only the Forward partition should be non-empty");
    assert_eq!(out[0].0, 0, "slot 0 is Destination::Forward");
    assert_eq!(out[0].1.events.len(), 64);
    expect_allocs("route_batch: 64 events, all unrouted (Forward only)", stats, 2);
}

/// The comparison the ADR cites (`docs/adr/target-components.md`'s "Two costs are structural to
/// this shape"): the *same* 64-event host/app split, expressed the way it has to be without
/// targets: a `Fanout` to two `Transform` consumers, each running `has_attributes` over the whole
/// batch to keep its half. 194:
///
/// - **1**: `Arc::new`, once per send to a multi-consumer edge.
/// - **193**: the `EventBatch` deep clone for branch `a`, which unwraps while `b` still holds the
///   `Arc`: 1 for the `Vec<Event>` plus 64 × 3. This fixture's per-event clone is 3, not
///   [`clone_one_event`]'s 2, because `fixtures::nginx_event_with_stream` inserts a `stream`
///   attribute into the already-spilled `AttrMap`, changing its capacity growth. Branch `b`
///   unwraps last and is free.
/// - **0**: the two `has_attributes` passes, which run in place
///   ([`process_batch_through_has_attributes`]). Each still scans all 64 events.
///
/// The cost scales with `branches × events`;
/// [`route_batch_two_targets_costs_one_vec_per_used_destination`]'s scales with destinations used
/// alone.
#[test]
fn fan_out_plus_two_has_attributes_for_the_same_split() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let (tx_a, mut rx_a) = tokio::sync::mpsc::channel(1);
    let (tx_b, mut rx_b) = tokio::sync::mpsc::channel(1);
    let fanout = Fanout::new(vec![tx_a, tx_b]);
    let telemetry = Telemetry::default();
    let mut host_filter = fixtures::has_attributes_stream("host");
    let mut app_filter = fixtures::has_attributes_stream("app");

    // Warm: one full round trip through the same Fanout and filters.
    let warm = fixtures::nginx_batch_alternating_stream(64);
    rt.block_on(async {
        fanout.send(warm).await;
        let a = unwrap_delivered(rx_a.recv().await.expect("a should receive"));
        let b = unwrap_delivered(rx_b.recv().await.expect("b should receive"));
        drop(process_batch(&mut host_filter, a, &telemetry));
        drop(process_batch(&mut app_filter, b, &telemetry));
    });

    let batch = fixtures::nginx_batch_alternating_stream(64);
    let ((host_out, app_out), stats) = measure(|| {
        rt.block_on(async {
            fanout.send(batch).await;
            // `a` unwraps while `b`'s handle is alive and clones; `b` unwraps last, free. The same
            // ordering as `fanout_send_two_consumers_costs_one_clone_plus_one_arc`.
            let a = unwrap_delivered(rx_a.recv().await.expect("a should receive"));
            let b = unwrap_delivered(rx_b.recv().await.expect("b should receive"));
            let host_out = process_batch(&mut host_filter, a, &telemetry);
            let app_out = process_batch(&mut app_filter, b, &telemetry);
            (host_out, app_out)
        })
    });
    assert_eq!(host_out.expect("host events should match").events.len(), 32);
    assert_eq!(app_out.expect("app events should match").events.len(), 32);
    expect_allocs(
        "today: fan-out (1 Arc + 1 clone of 64 events) + 2 has_attributes passes, same split",
        stats,
        194,
    );
}

// ---------------------------------------------------------------------------------------------
// Disk-backed sink buffer (docs/adr/disk-backed-sink-buffer.md)
// ---------------------------------------------------------------------------------------------

/// A scratch spool directory, left for the OS's tmp reaper, as `disk_queue.rs`'s
/// `test_support::scratch_dir` does (no `tempfile` dependency, ADR
/// `file-tailing-and-docker-json-logs`).
fn disk_scratch_dir(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir()
        .join(format!("logit-bench-disk-queue-{label}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

/// The runtime every `disk_queue_*` measurement runs on. `max_blocking_threads(1)` is for
/// determinism: `DiskQueue` does I/O through `tokio::fs` (`spawn_blocking`), and while the blocking
/// worker's allocations are invisible to the thread-local counters, spawning a new worker
/// allocates on the measuring thread. tokio reuses a worker only if it is already marked idle, and
/// the warm-up's worker must re-take the pool lock to mark itself idle. If it's preempted in that
/// window (seen on CI under heavy parallel load), the measured `push` spawns a fresh thread and
/// reports `+4`. With one blocking thread, the warm-up's spawn is the only one.
fn disk_queue_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
}

fn disk_queue_config(dir: std::path::PathBuf) -> logit_pipeline::DiskQueueConfig {
    logit_pipeline::DiskQueueConfig {
        dir,
        max_bytes: 64 * 1024 * 1024,
        segment_bytes: 64 * 1024 * 1024,
        overflow: logit_pipeline::OverflowPolicy::Block,
        compression: logit_proto::frame::Compression::None,
        checkpoint_interval: std::time::Duration::from_secs(3600),
    }
}

/// `DiskQueue::push`: `native::encode_batch_v2` (`encode_batch` plus a provenance trailer,
/// `docs/adr/batch-provenance-on-delivered.md`), `frame::write_frame`, and one `write_all` to the
/// active segment. The disk buffer's ADR accepts that this encode breaks
/// `buffered-sink-delivery`'s zero-clone `Arc<EventBatch>` property. The warm-up push+commit pays
/// the one-time setup (the lock file, the first segment's open).
#[test]
fn disk_queue_push_one_batch() {
    let rt = disk_queue_runtime();
    let dir = disk_scratch_dir("push");
    let telemetry = Telemetry::default();
    let queue = logit_pipeline::DiskQueue::open(
        disk_queue_config(dir.clone()),
        telemetry,
        logit_core::Diagnostics::new("bench"),
    )
    .unwrap();

    let warm = fixtures::nginx_batch(1);
    rt.block_on(queue.push((Arc::new(warm), BatchContext::default())));
    rt.block_on(queue.peek());
    queue.commit();

    let batch = Arc::new(fixtures::nginx_batch(1));
    let ((), stats) =
        measure(|| rt.block_on(queue.push((Arc::clone(&batch), BatchContext::default()))));

    // Among the 34: `encode_batch_v2` builds v1's payload as its own `Bytes`, then copies it into
    // a larger `BytesMut` beside the (here empty) provenance trailer: 2. `write_field`'s
    // temp buffers: 2 per `MetricRecord` (`write_record_list`'s per-entry length prefix, which
    // lets a reader skip a record with unknown fields, and the `MR_KIND` field) for the four
    // metrics, plus 1 for `LogRecord.message`: 9. The two `Samples` are written straight from
    // their values, with no serialized sketch blob.
    expect_allocs("disk_queue: push one batch (encode + write)", stats, 34);
    std::fs::remove_dir_all(&dir).ok();
}

/// A repeated `peek` before `commit` is cached -- `write_loop`'s retry loop calls `peek` once per
/// delivery attempt, so a batch retried several times must not re-decode from disk each time.
#[test]
fn disk_queue_peek_cached_costs_nothing() {
    let rt = disk_queue_runtime();
    let dir = disk_scratch_dir("peek-cached");
    let telemetry = Telemetry::default();
    let queue = logit_pipeline::DiskQueue::open(
        disk_queue_config(dir.clone()),
        telemetry,
        logit_core::Diagnostics::new("bench"),
    )
    .unwrap();

    let batch = Arc::new(fixtures::nginx_batch(1));
    rt.block_on(queue.push((batch, BatchContext::default())));
    rt.block_on(queue.peek()); // warm: the first peek per push does the real disk read + decode

    let ((), stats) = measure(|| {
        rt.block_on(queue.peek());
    });

    expect_allocs("disk_queue: peek, cached (no re-decode)", stats, 0);
    std::fs::remove_dir_all(&dir).ok();
}

/// The common shape (the nginx reference config's `tap`/`trimmed` split): one `Output` branch and
/// one mutating branch off the same fan-out. The `Output` branch only borrows; the mutating branch
/// needs an owned batch, so it goes through `unwrap_batch`.
///
/// Racy, with two reachable outcomes. `run_output` drops its `Delivered` as soon as `output.send`
/// returns, so the mutating branch's unwrap is free or a clone depending on which happens first.
/// This test pins the outcome where the mutating branch unwraps first, while the `Output` handle
/// is alive: 4, the same as [`fanout_send_two_consumers_costs_one_clone_plus_one_arc`].
/// [`fanout_send_mixed_output_and_transform_consumers_when_output_finishes_first`] pins the other.
/// A real `Output::send` does I/O, so this outcome is the likelier one in production.
#[test]
fn fanout_send_mixed_output_and_transform_consumers() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let (tx_out, mut rx_out) = tokio::sync::mpsc::channel(1);
    let (tx_xform, mut rx_xform) = tokio::sync::mpsc::channel(1);
    let fanout = Fanout::new(vec![tx_out, tx_xform]);

    let warm = fixtures::nginx_batch(1);
    rt.block_on(async {
        fanout.send(warm).await;
        let out = rx_out.recv().await.expect("output branch should receive");
        let xform = rx_xform.recv().await.expect("transform branch should receive");
        drop(unwrap_delivered(xform));
        drop(out);
    });

    let batch = fixtures::nginx_batch(1);
    let ((out_len, xform_len), stats) = measure(|| {
        rt.block_on(async {
            fanout.send(batch).await;
            let out = rx_out.recv().await.expect("output branch should receive");
            let xform = rx_xform.recv().await.expect("transform branch should receive");
            // The Transform branch unwraps while the Output branch's handle is alive, forcing
            // its clone. The Output branch borrows, then drops at the end of the block.
            let xform_batch = unwrap_delivered(xform);
            let out_len = borrow_delivered(&out).events.len();
            (out_len, xform_batch.events.len())
        })
    });
    assert_eq!(out_len, 1);
    assert_eq!(xform_len, 1);
    // 1 (Arc::new, once per send) + 3 (the Transform branch's forced deep clone: 1 for the
    // Vec<Event>, 2 for the one nginx-shaped Event inside it) + 0 (the Output branch, which never
    // unwraps or clones).
    expect_allocs(
        "fanout: send + receive, 1 Output + 1 Transform, Output not yet finished (racy outcome A)",
        stats,
        // The clone this outcome pays scales with `clone_one_event`.
        4,
    );
}

/// The other reachable outcome for [`fanout_send_mixed_output_and_transform_consumers`]'s shape:
/// the `Output` branch finishes and drops its handle before the mutating branch unwraps, so
/// `Arc::try_unwrap` succeeds for free. 1, the `Arc::new`, as in
/// [`fanout_send_two_output_consumers_costs_only_the_arc`].
///
/// Together the two tests bound this shape at 1 or 4, decided by scheduling; `Arc::new` is paid
/// whenever there are two or more consumers, so nothing lands between.
#[test]
fn fanout_send_mixed_output_and_transform_consumers_when_output_finishes_first() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let (tx_out, mut rx_out) = tokio::sync::mpsc::channel(1);
    let (tx_xform, mut rx_xform) = tokio::sync::mpsc::channel(1);
    let fanout = Fanout::new(vec![tx_out, tx_xform]);

    let warm = fixtures::nginx_batch(1);
    rt.block_on(async {
        fanout.send(warm).await;
        let out = rx_out.recv().await.expect("output branch should receive");
        let xform = rx_xform.recv().await.expect("transform branch should receive");
        drop(out);
        drop(unwrap_delivered(xform));
    });

    let batch = fixtures::nginx_batch(1);
    let ((out_len, xform_len), stats) = measure(|| {
        rt.block_on(async {
            fanout.send(batch).await;
            let out = rx_out.recv().await.expect("output branch should receive");
            let xform = rx_xform.recv().await.expect("transform branch should receive");
            // The Output branch drops its handle, as `run_output` does once `output.send`
            // returns, before the Transform branch unwraps.
            let out_len = borrow_delivered(&out).events.len();
            drop(out);
            let xform_batch = unwrap_delivered(xform);
            (out_len, xform_batch.events.len())
        })
    });
    assert_eq!(out_len, 1);
    assert_eq!(xform_len, 1);
    // 1 (Arc::new, once per send) + 0 (the Transform branch's try_unwrap now succeeds, since the
    // Output branch already dropped its handle) + 0 (the Output branch, as always).
    expect_allocs(
        "fanout: send + receive, 1 Output + 1 Transform, Output finished first (racy outcome B)",
        stats,
        1,
    );
}

/// Cloning [`fixtures::distribution_heavy_event`]: five `MetricKind::Distribution` sketches and
/// three inline attributes. 6: one `MetricList` spill plus one `bins` `Vec` per sketch. Boxing the
/// `DdSketch` would add one allocation per sketch on top (`docs/design/memory.md`'s
/// "Recommendations" section, "`Box` the `DdSketch`").
#[test]
fn clone_distribution_heavy_event() {
    let event = fixtures::distribution_heavy_event();
    drop(event.clone());

    let (clone, stats) = measure(|| event.clone());
    assert_eq!(clone.metrics.len(), 5);
    expect_allocs("Event::clone (distribution-heavy shape)", stats, 6);
}

/// Cloning [`fixtures::span_event`]: 2, the `SpanRecord`'s `Vec<SpanEvent>` (2 entries) and
/// `Vec<SpanLink>` (1 entry), each a heap allocation on clone. Every `AttrMap` involved (the
/// event's 4 attributes, each `SpanEvent`'s 2, the link's 1) stays inline.
///
/// That makes this narrow span as cheap to clone as the nginx shape, not dearer. A span with more
/// than 8 attributes on itself, an event, or a link would spill those maps and cost more. The two
/// `Vec`s are a fixed cost; boxing `SpanRecord` would add one more
/// (`docs/design/memory.md`'s "Recommendations" section, "`Box` `SpanRecord`").
#[test]
fn clone_span_event() {
    let event = fixtures::span_event();
    drop(event.clone());

    let (clone, stats) = measure(|| event.clone());
    assert!(clone.span.is_some());
    expect_allocs("Event::clone (span shape)", stats, 2);
}

// ---------------------------------------------------------------------------------------------
// Runtime
// ---------------------------------------------------------------------------------------------
//
// The node loops' per-batch bodies, including their telemetry accounting. `run_transform`'s is
// exported as `logit_pipeline::process_batch` (synchronous) and `run_output`'s as
// `logit_pipeline::send_batch` (async, driven on a `current_thread` runtime with no channel), so
// both can be measured directly. `unwrap_batch` is exported for the same reason.
//
// Telemetry has two states, both covered for both functions. `ComponentBuffer::drain`
// `mem::take`s the `points` map on every `internal` tick, so the next `count`/`timer` per key is
// a fresh insert into an empty map. "Telemetry live" is the steady state between drains; "first
// call after a drain" is paid once per drain interval for as long as `internal` runs.

/// The per-batch body with telemetry disabled (`Telemetry::default()`, as with no `internal`
/// component): 0, nothing above `keep_one_event`'s 0. `Transform::process` takes `&mut Event` and
/// returns a bool (ADR `in-place-transform-process`), so `process_batch` is a `Vec::retain_mut`
/// over the batch's own `events`, with no output `Vec`.
#[test]
fn process_batch_through_keep() {
    let mut keep = fixtures::keep();
    let telemetry = Telemetry::default();
    let warm = fixtures::nginx_batch(1);
    drop(process_batch(&mut keep, warm, &telemetry));

    let batch = fixtures::nginx_batch(1);
    let (out, stats) = measure(|| process_batch(&mut keep, batch, &telemetry));
    let out = out.expect("keep forwards events, never fully absorbs");
    assert_eq!(out.events.len(), 1);
    expect_allocs("runtime: process_batch through keep, telemetry disabled", stats, 0);
}

/// `set`'s attribute-only path through `process_batch`: 0, as for `keep`. `map_resource` returns
/// `None` immediately (`resource_pairs` is empty).
#[test]
fn process_batch_through_set_attributes_only() {
    let mut set = fixtures::set_attributes();
    let telemetry = Telemetry::default();
    let warm = fixtures::nginx_batch(1);
    drop(process_batch(&mut set, warm, &telemetry));

    let batch = fixtures::nginx_batch(1);
    let (out, stats) = measure(|| process_batch(&mut set, batch, &telemetry));
    let out = out.expect("set forwards events, never absorbs");
    assert_eq!(out.events.len(), 1);
    expect_allocs("runtime: process_batch through set (attributes only)", stats, 0);
}

/// `trace_context` lifting a valid `trace_id`: 0. `parse_trace_id` works on stack arrays, and
/// `AttrMap::remove` (the default `keep_source: false` path) is an in-place `SmallVec` shift.
#[test]
fn trace_context_lifts_a_valid_trace_id() {
    let mut trace_context = fixtures::trace_context();
    let resource = fixtures::resource();
    let event_with_trace_id = {
        let mut event = fixtures::nginx_event();
        event.attributes.insert("trace_id", Value::str("ab".repeat(16)));
        event
    };
    let telemetry = Telemetry::default();
    let warm = EventBatch {
        resource: resource.clone(),
        scope: None,
        events: vec![event_with_trace_id.clone()],
    };
    drop(process_batch(&mut trace_context, warm, &telemetry));

    let batch = EventBatch { resource, scope: None, events: vec![event_with_trace_id] };
    let (out, stats) = measure(|| process_batch(&mut trace_context, batch, &telemetry));
    let out = out.expect("trace_context forwards events, never absorbs");
    assert!(out.events[0].log.as_ref().unwrap().trace.is_some(), "the lift should have succeeded");
    expect_allocs("transform: trace_context, lifting a valid trace_id", stats, 0);
}

/// `trace_context` with a `span:` block, minting a `SpanRecord` from the convention attributes
/// (`docs/adr/trace-context-span-lifting.md`): 0, as for the log-only lift above. Ids parse into
/// stack arrays, the timing arithmetic is integer, `name` clones a pre-built `Value` (a refcount
/// bump), `events`/`links` are empty `Vec::new()`s, and each consumed attribute is an in-place
/// `AttrMap::remove`. `SpanRecord` is inline in `Event`, so setting `event.span` moves 136 bytes.
#[test]
fn trace_context_mints_a_span_from_the_convention() {
    let mut trace_context = fixtures::trace_context_with_span();
    let resource = fixtures::resource();
    let traced = fixtures::nginx_traced_event();
    let telemetry = Telemetry::default();
    let warm = EventBatch { resource: resource.clone(), scope: None, events: vec![traced.clone()] };
    drop(process_batch(&mut trace_context, warm, &telemetry));

    let batch = EventBatch { resource, scope: None, events: vec![traced] };
    let (out, stats) = measure(|| process_batch(&mut trace_context, batch, &telemetry));
    let out = out.expect("trace_context forwards events, never absorbs");
    let event = &out.events[0];
    let span = event.span.as_ref().expect("the span lift should have succeeded");
    assert_eq!(span.span_id, [0xa1, 0xb2, 0xc3, 0xd4, 0xe5, 0xf6, 0x07, 0x18]);
    assert!(span.parent_span_id.is_some(), "parent from the traceparent");
    assert_eq!(span.end_timestamp - event.timestamp, 4_000_000, "4ms, from span.duration_s");
    assert!(event.attributes.get("traceparent").is_none(), "consumed");
    expect_allocs("transform: trace_context, minting a span from the convention", stats, 0);
}

/// `Set::map_resource`'s one-entry cache: a second call with the same input `Arc<Resource>` hits
/// and costs 0. Compare [`set_resource_map_resource_cache_miss`].
#[test]
fn set_resource_map_resource_cache_hit_costs_nothing() {
    let mut set = fixtures::set_resource();
    let resource = fixtures::resource();
    drop(set.map_resource(&resource)); // warm the cache

    let (mapped, stats) = measure(|| set.map_resource(&resource));
    assert!(mapped.is_some());
    expect_allocs("transform: set.map_resource, cached (same input Arc)", stats, 0);
}

/// A cache miss: a distinct input `Arc<Resource>` each call, as a listener minting a fresh `Arc`
/// per request (`otlp_in`) drives. 1, the `Arc::new(Resource { .. })`: the fixture resource's
/// `AttrMap` starts empty, so cloning it and inserting stays inline.
#[test]
fn set_resource_map_resource_cache_miss() {
    let mut set = fixtures::set_resource();
    drop(set.map_resource(&fixtures::resource())); // warm, distinct Arc from the measured call

    let resource = fixtures::resource();
    let (mapped, stats) = measure(|| set.map_resource(&resource));
    assert!(mapped.is_some());
    expect_allocs("transform: set.map_resource, cache miss (distinct input Arc)", stats, 1);
}

/// A resource carrying `service.name`, which [`fixtures::has_attributes_resource`]'s config matches
/// on; `fixtures::resource()` is always empty.
fn resource_with_service_name() -> Arc<Resource> {
    let mut attrs = AttrMap::new();
    attrs.insert("service.name", Value::str("nginx"));
    Arc::new(Resource { attributes: attrs, ..Default::default() })
}

/// `has_attributes` matching one event attribute: 0. The `AttrMap::get_sym` probe is a binary
/// search over a `SmallVec`, and `value_matches`' numeric coercion (config `status: 200` against
/// the JSON-sourced `status`) parses on the stack.
#[test]
fn has_attributes_one_event() {
    let mut has = fixtures::has_attributes();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    has.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| has.process(&resource, &mut event));
    assert!(forwarded, "the fixture's status should match");
    expect_allocs("has_attributes: match 1 attribute", stats, 0);
}

/// [`has_attributes_one_event`]'s complement: the matching event is dropped instead. Also 0.
#[test]
fn drop_attributes_one_event() {
    let mut drop_attrs = fixtures::drop_attributes();
    let resource = fixtures::resource();
    let mut warm = fixtures::nginx_event();
    drop_attrs.process(&resource, &mut warm);

    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| drop_attrs.process(&resource, &mut event));
    assert!(!forwarded, "the fixture's status should match, so this drops");
    expect_allocs("drop_attributes: match 1 attribute (dropped)", stats, 0);
}

/// The resource-match cache's hit path (`Matcher`'s `ptr_eq` check on the input
/// `Arc<Resource>`): 0.
#[test]
fn has_attributes_resource_match_cache_hit() {
    let mut has = fixtures::has_attributes_resource();
    let resource = resource_with_service_name();
    let mut warm = fixtures::nginx_event();
    has.process(&resource, &mut warm); // warm the cache

    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| has.process(&resource, &mut event));
    assert!(forwarded);
    expect_allocs("has_attributes: resource match, cache hit (same input Arc)", stats, 0);
}

/// The resource-match cache's miss path: also 0, unlike `Set::map_resource`'s miss (1). A miss
/// only re-evaluates `get_sym` against the caller's existing `Arc`, never building a new one, so
/// the always-missing `logit_in` fan-out topology (`docs/adr/attribute-filtering-components.md`)
/// pays nothing for the cache.
#[test]
fn has_attributes_resource_match_cache_miss() {
    let mut has = fixtures::has_attributes_resource();
    let mut warm = fixtures::nginx_event();
    has.process(&resource_with_service_name(), &mut warm); // warm, distinct Arc

    let resource = resource_with_service_name();
    let mut event = fixtures::nginx_event();
    let (forwarded, stats) = measure(|| has.process(&resource, &mut event));
    assert!(forwarded);
    expect_allocs("has_attributes: resource match, cache miss (distinct input Arc)", stats, 0);
}

/// `has_attributes` through `process_batch`: 0, as for `keep`/`set`. The `retain_mut` loop
/// allocates nothing whether the event is forwarded (here) or dropped (below).
#[test]
fn process_batch_through_has_attributes() {
    let mut has = fixtures::has_attributes();
    let telemetry = Telemetry::default();
    let warm = fixtures::nginx_batch(1);
    drop(process_batch(&mut has, warm, &telemetry));

    let batch = fixtures::nginx_batch(1);
    let (out, stats) = measure(|| process_batch(&mut has, batch, &telemetry));
    let out = out.expect("the fixture's status should match, so this forwards");
    assert_eq!(out.events.len(), 1);
    expect_allocs("runtime: process_batch through has_attributes", stats, 0);
}

/// Every event in the batch dropped: still 0. `retain_mut` drops each rejected event in place, so
/// a fully filtered batch costs what a forwarded one does ([`process_batch_fully_absorbed`] pins
/// the same for `aggregate`).
#[test]
fn process_batch_through_has_attributes_dropping_every_event() {
    let mut drop_attrs = fixtures::drop_attributes();
    let telemetry = Telemetry::default();
    let warm = fixtures::nginx_batch(1);
    drop(process_batch(&mut drop_attrs, warm, &telemetry));

    let batch = fixtures::nginx_batch(1);
    let (out, stats) = measure(|| process_batch(&mut drop_attrs, batch, &telemetry));
    assert!(out.is_none(), "the fixture's status should match, so every event is dropped");
    expect_allocs("runtime: process_batch through drop_attributes, dropping every event", stats, 0);
}

/// Every event absorbed, nothing forwarded: 0. `fixtures::statsd_event` is metrics-only, so
/// `aggregate` absorbs all of it rather than forwarding a log/span half, and `retain_mut` empties
/// the batch's `Vec` in place. A metrics-heavy `internal`-fed pipeline hits this case constantly.
#[test]
fn process_batch_fully_absorbed() {
    let mut agg = fixtures::aggregator();
    let resource = fixtures::resource();
    let telemetry = Telemetry::default();
    let warm = EventBatch {
        resource: resource.clone(),
        scope: None,
        events: vec![fixtures::statsd_event()],
    };
    drop(process_batch(&mut agg, warm, &telemetry));

    let batch = EventBatch { resource, scope: None, events: vec![fixtures::statsd_event()] };
    let (out, stats) = measure(|| process_batch(&mut agg, batch, &telemetry));
    assert!(out.is_none(), "a batch with nothing left to forward should not be forwarded");
    expect_allocs("runtime: process_batch, fully absorbed (aggregate)", stats, 0);
}

/// Live telemetry in steady state: 0, the same as [`process_batch_through_keep`]'s disabled path.
/// Every `count`/`timer` updates a `(name, tags)` key the warm-up left resident
/// (`ComponentBuffer::upsert`'s `get_mut` branch), as between two `internal` drains.
/// [`process_batch_first_call_after_a_drain`] pins the fresh-insert case.
#[test]
fn process_batch_with_live_telemetry() {
    let mut keep = fixtures::keep();
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("keep", "keep", "transform");
    let warm = fixtures::nginx_batch(1);
    drop(process_batch(&mut keep, warm, &telemetry));

    let batch = fixtures::nginx_batch(1);
    let (out, stats) = measure(|| process_batch(&mut keep, batch, &telemetry));
    let out = out.expect("keep forwards events, never fully absorbs");
    assert_eq!(out.events.len(), 1);
    expect_allocs("runtime: process_batch through keep, telemetry live (steady state)", stats, 0);
}

/// The first `process_batch` after an `internal` drain, which `mem::take`s the component buffer's
/// map, so all three keys (`batches.received`, `events.received`, `process.duration`) are fresh
/// inserts. 2: the emptied `HashMap`'s new table, plus the fresh `DdSketch` `process.duration`'s
/// `Timer` creates on `Drop` (nothing to merge into). Recurs once per drain interval.
#[test]
fn process_batch_first_call_after_a_drain() {
    let mut keep = fixtures::keep();
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("keep", "keep", "transform");
    let warm = fixtures::nginx_batch(1);
    drop(process_batch(&mut keep, warm, &telemetry));
    registry.drain(0); // what `internal`'s tick does: empties the ComponentBuffer's map

    let batch = fixtures::nginx_batch(1);
    let (out, stats) = measure(|| process_batch(&mut keep, batch, &telemetry));
    let out = out.expect("keep forwards events, never fully absorbs");
    assert_eq!(out.events.len(), 1);
    expect_allocs("runtime: process_batch, first call after an internal drain", stats, 2);
}

/// `unwrap_batch` on `Delivered::Owned` (a single-consumer edge): a plain match, 0.
#[test]
fn unwrap_batch_owned() {
    let batch = fixtures::nginx_batch(1);
    let (out, stats) = measure(|| unwrap_batch(Delivered::Owned(batch, BatchContext::default())));
    assert_eq!(out.events.len(), 1);
    expect_allocs("runtime: unwrap_batch, Delivered::Owned", stats, 0);
}

/// `unwrap_batch` on `Delivered::Shared` when every sibling has dropped its handle:
/// `Arc::try_unwrap` succeeds, 0.
#[test]
fn unwrap_batch_shared_sole_reference() {
    let batch = fixtures::nginx_batch(1);
    let shared = Arc::new(batch);
    let (out, stats) = measure(|| unwrap_batch(Delivered::Shared(shared, BatchContext::default())));
    assert_eq!(out.events.len(), 1);
    expect_allocs("runtime: unwrap_batch, Delivered::Shared, sole reference", stats, 0);
}

/// `unwrap_batch` while a sibling still holds the `Arc`: `try_unwrap` fails and it falls back to
/// `EventBatch::clone`. 3: 1 for the `Vec<Event>` plus [`clone_one_event`]'s 2.
/// [`fanout_send_two_consumers_costs_one_clone_plus_one_arc`]'s 4 is this plus the `Arc::new`.
#[test]
fn unwrap_batch_shared_contended() {
    let warm_shared = Arc::new(fixtures::nginx_batch(1));
    let warm_sibling = warm_shared.clone(); // held across the call below, forcing the fallback
    drop(unwrap_batch(Delivered::Shared(warm_shared, BatchContext::default())));
    drop(warm_sibling);

    let shared = Arc::new(fixtures::nginx_batch(1));
    let _sibling = shared.clone(); // kept alive across the measured call, forcing the fallback
    let (out, stats) = measure(|| unwrap_batch(Delivered::Shared(shared, BatchContext::default())));
    assert_eq!(out.events.len(), 1);
    expect_allocs(
        "runtime: unwrap_batch, Delivered::Shared, contended (falls back to clone)",
        stats,
        3,
    );
}

/// A no-op `Output`, so the `send_batch` tests measure its own accounting (the two receive
/// counters, the `send.duration` timer, the error counter), not a sink's encode cost.
struct NoopOutput;

#[async_trait::async_trait]
impl logit_pipeline::Output for NoopOutput {
    async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Always fails, to exercise `send_batch`'s error path: the `logit.component.errors` counter and
/// `result.with_context(...)`.
struct FailingOutput;

#[async_trait::async_trait]
impl logit_pipeline::Output for FailingOutput {
    async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("simulated output failure"))
    }
}

/// `send_batch`'s failure path, telemetry disabled: 4, against
/// [`send_batch_through_a_noop_output_disabled_telemetry`]'s 1. The `async_trait` box (1), plus
/// `anyhow!` building `FailingOutput`'s error (1), plus `.with_context(|| format!(...))` (2: the
/// `format!`, and the boxed `anyhow::Error` node joining error and context). Each part was
/// measured in isolation.
#[test]
fn send_batch_through_a_failing_output_disabled_telemetry() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = FailingOutput;
    let telemetry = Telemetry::default();
    let warm = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    rt.block_on(async {
        drop(send_batch("out", &mut output, &warm, &telemetry).await);
    });

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    let (result, stats) = measure(|| {
        rt.block_on(async { send_batch("out", &mut output, &delivered, &telemetry).await })
    });
    assert!(result.is_err(), "FailingOutput should make send_batch itself report an error");
    expect_allocs("runtime: send_batch through a failing Output, telemetry disabled", stats, 4);
}

/// `send_batch`'s failure path with live telemetry: the three success-path keys are resident, but
/// this is the component's first failure, so `logit.component.errors` is a new fourth key. 5, one
/// more than the disabled path: the fourth key grows the `ComponentBuffer`'s `HashMap`.
#[test]
fn send_batch_through_a_failing_output_telemetry_live() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut succeeding = NoopOutput;
    let mut failing = FailingOutput;
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "stdio_out", "sink");
    let warm = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    rt.block_on(async {
        // A real success first, so batches.received/events.received/send.duration are already
        // resident -- only `errors` is new when the measured call below fails.
        send_batch("out", &mut succeeding, &warm, &telemetry)
            .await
            .expect("noop output never errors");
    });

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    let (result, stats) = measure(|| {
        rt.block_on(async { send_batch("out", &mut failing, &delivered, &telemetry).await })
    });
    assert!(result.is_err());
    expect_allocs(
        "runtime: send_batch through a failing Output, telemetry live, first failure",
        stats,
        5,
    );
}

/// The first call after an `internal` drain, failing: all four keys (`batches.received`,
/// `events.received`, `send.duration`, `errors`) are fresh inserts, on top of the failure path's
/// `anyhow!`/`.with_context()` cost. 7, which is measured rather than derived: a fourth fresh key
/// can take the map through more than one growth step.
#[test]
fn send_batch_failing_first_call_after_a_drain() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = FailingOutput;
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "stdio_out", "sink");
    let warm = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    rt.block_on(async {
        drop(send_batch("out", &mut output, &warm, &telemetry).await);
    });
    registry.drain(0); // what `internal`'s tick does: empties the ComponentBuffer's map

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    let (result, stats) = measure(|| {
        rt.block_on(async { send_batch("out", &mut output, &delivered, &telemetry).await })
    });
    assert!(result.is_err());
    expect_allocs("runtime: send_batch, failing, first call after an internal drain", stats, 7);
}

/// `run_output`'s per-batch body, `logit_pipeline::send_batch`, telemetry disabled: 1, from
/// neither telemetry nor dispatch. `Output` is `#[async_trait]`, which boxes every `send`'s future
/// (16 bytes); calling `NoopOutput::send` directly, with no `dyn Output`, costs the same 1. It's a
/// per-batch cost on every sink (`docs/known-gaps.md`).
#[test]
fn send_batch_through_a_noop_output_disabled_telemetry() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = NoopOutput;
    let telemetry = Telemetry::default();
    let warm = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    rt.block_on(async {
        send_batch("out", &mut output, &warm, &telemetry).await.expect("noop output never errors")
    });

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    let (_, stats) = measure(|| {
        rt.block_on(async {
            send_batch("out", &mut output, &delivered, &telemetry)
                .await
                .expect("noop output never errors")
        })
    });
    expect_allocs("runtime: send_batch through a no-op Output, telemetry disabled", stats, 1);
}

/// `send_batch` with live telemetry between drains, the counterpart to
/// [`process_batch_with_live_telemetry`]: 1, the same as the disabled path, all of it the
/// `async_trait` box. A realloc can appear depending on which `DdSketch` bucket the elapsed time
/// lands in; `expect_allocs` doesn't assert reallocs.
#[test]
fn send_batch_through_a_noop_output_telemetry_live() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = NoopOutput;
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "stdio_out", "sink");
    let warm = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    rt.block_on(async {
        send_batch("out", &mut output, &warm, &telemetry).await.expect("noop output never errors")
    });

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    let (_, stats) = measure(|| {
        rt.block_on(async {
            send_batch("out", &mut output, &delivered, &telemetry)
                .await
                .expect("noop output never errors")
        })
    });
    expect_allocs(
        "runtime: send_batch through a no-op Output, telemetry live (steady state)",
        stats,
        1,
    );
}

/// `send_batch`'s counterpart to [`process_batch_first_call_after_a_drain`], on its three keys
/// (`batches.received`, `events.received`, `send.duration`). 3: that test's 2 (the emptied
/// `HashMap`'s new table, and the fresh `DdSketch` `send.duration`'s `Timer` creates on `Drop`)
/// plus the `async_trait` box every `send` pays.
#[test]
fn send_batch_first_call_after_a_drain() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = NoopOutput;
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "stdio_out", "sink");
    let warm = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    rt.block_on(async {
        send_batch("out", &mut output, &warm, &telemetry).await.expect("noop output never errors")
    });
    registry.drain(0); // what `internal`'s tick does: empties the ComponentBuffer's map

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default());
    let (_, stats) = measure(|| {
        rt.block_on(async {
            send_batch("out", &mut output, &delivered, &telemetry)
                .await
                .expect("noop output never errors")
        })
    });
    expect_allocs("runtime: send_batch, first call after an internal drain", stats, 3);
}

// ---------------------------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------------------------

/// `influxdb_out` encoding 100 nginx events: 230, 30 of the encoder's own plus 200 re-sketching.
///
/// The encoder escapes and formats straight into reused buffers. Its 30 are per batch: one `Bytes`
/// for the body, one `String` key per distinct series on first sighting, and the per-series
/// timestamp maps' growth. If that 30 starts tracking batch size, the encoder has regressed to
/// per-line allocation.
///
/// The 200 are 2 per event: the fixture's two distributions are raw `Samples`
/// ([`kv_metrics_one_event`], `docs/adr/kv-metrics-semantics.md`), and rendering one re-sketches
/// it (`Samples::sketch`'s `bins` `Vec`, the cost
/// [`graphite_encode_into_100_samples_events_expanded`] documents). This fixture feeds
/// `kv_metrics` output straight to the encoder; the reference pipeline runs `aggregate` first,
/// which sketches once per series ([`aggregate_absorb_one_samples_event_sketch_mode`]: 0). A
/// single-value `Samples` fast path in `render_fields` would remove the 200.
#[test]
fn influx_encode_100_events() {
    let mut encoder = InfluxLineEncoder::default();
    let batch = fixtures::nginx_batch(100);
    drop(encoder.encode(&batch));

    let (body, stats) = measure(|| encoder.encode(&batch).expect("should encode"));
    assert!(!body.is_empty());
    expect_allocs("influxdb_out: encode 100 events", stats, 230);
}

/// `stdio_out` encoding 100 nginx events: 102. It merge-joins the resource and event attribute
/// maps and formats numbers with `write!` straight into one output buffer. The 102 are one
/// `format_rfc3339_utc` per event (`logit_core::time`), one first growth of the output `String`,
/// and one for `Encoder::encode`'s `Bytes::from(String)`, which reuses the buffer but needs its
/// own shared-refcount allocation. Measured through `Encoder::encode`, as `StreamOutput::send`
/// calls it (`docs/adr/rotating-file-output.md`), not the inherent `EventDump::render`.
#[test]
fn stdio_encode_100_events() {
    let mut dump = EventDump::new(Format::Human);
    let batch = fixtures::nginx_batch(100);
    drop(dump.encode(&batch));

    let (result, stats) = measure(|| dump.encode(&batch));
    assert!(!result.expect("should encode").is_empty());
    expect_allocs("stdio_out: encode 100 events", stats, 102);
}

/// `syslog_out` encoding 100 nginx events as RFC 5424: 100, one per event, from
/// `format_rfc3339_utc` (`push_rfc5424_timestamp`), the same timestamp cost `stdio_out` pays.
/// `SyslogEncoder` holds `line`/`raw_msg`/`scratch` as reused struct fields; as function locals
/// they would start from empty capacity on every call, and warming wouldn't help.
#[test]
fn syslog_encode_into_100_events() {
    let mut encoder = SyslogEncoder::new(SyslogFormat::Rfc5424, 16);
    let batch = fixtures::nginx_batch(100);
    let mut out = MessageBuf::default();

    let (stats_out, stats) = measure_framed(&mut encoder, &batch, &mut out);
    assert_eq!(out.len(), 100);
    assert_eq!(stats_out.skipped_no_log, 0);
    expect_allocs("syslog_out: encode_into 100 events", stats, 100);
}

/// Warm-then-measure for any `FramedEncoder` (ADR `framed-encoder`): the warm-up call grows the
/// encoder's scratch buffers and `out`'s backing `Vec`s, then the measured call reuses the same
/// `out`. Generic over `Meta` so `MessageBuf<usize>` sinks use it too.
fn measure_framed<M, E: FramedEncoder<Meta = M>>(
    encoder: &mut E,
    batch: &EventBatch,
    out: &mut MessageBuf<M>,
) -> (E::Stats, Stats) {
    let _ = encoder.encode_into(batch, out);
    measure(|| encoder.encode_into(batch, out))
}

/// Zero for 100 single-counter DogStatsD events: every per-metric buffer
/// (`line`/`name`/`tag_suffix`/`scratch`/...) is a reused struct field, and a statsd line has no
/// timestamp to format. Uses the default (uncapped) `max_packet_bytes`, as `StatsdOutput::send`
/// does on TCP.
#[test]
fn statsd_encode_into_100_events() {
    let mut encoder = StatsdEncoder::new(StatsdFormat::DogStatsd);
    let batch = fixtures::statsd_batch(100);
    let mut out = MessageBuf::default();

    let (stats_out, stats) = measure_framed(&mut encoder, &batch, &mut out);
    assert_eq!(out.len(), 100);
    assert_eq!(stats_out, logit_outputs::statsd::EncodeStats::default());
    expect_allocs("statsd_out: encode_into 100 events", stats, 0);
}

/// Zero: `CollectdEncoder` reuses its `packet`/`list`/`values` scratch, and `Identity` has a
/// hand-written `clone_from` that refills each field in place. `#[derive(Clone)]`'s default
/// `clone_from` is `*self = source.clone()`, which would allocate a `Vec` per non-empty identity
/// field on every list (~3/event).
#[test]
fn collectd_encode_into_100_events() {
    let mut encoder = fixtures::collectd_encoder()
        .with_max_packet_bytes(logit_proto::collectd::DEFAULT_MAX_PACKET_BYTES);
    let batch = fixtures::collectd_batch(100);
    let mut out = MessageBuf::<usize>::default();

    let (stats, alloc_stats) = measure_framed(&mut encoder, &batch, &mut out);
    assert!(!out.is_empty());
    assert_eq!(stats, logit_proto::collectd::EncodeStats::default());
    expect_allocs("collectd_out: encode_into 100 events", alloc_stats, 0);
}

/// Zero for 100 single-gauge events in plaintext: every per-record buffer
/// (`tag_suffix`/`path`/`line`/...) is a reused struct field.
#[test]
fn graphite_encode_into_100_plaintext_events() {
    let mut encoder = fixtures::graphite_encoder();
    let batch = fixtures::graphite_batch(100);
    let mut out = MessageBuf::<usize>::default();

    let (stats, alloc_stats) = measure_framed(&mut encoder, &batch, &mut out);
    assert_eq!(out.len(), 100);
    assert_eq!(stats.datapoints, 100);
    expect_allocs("graphite_out: encode_into 100 plaintext events", alloc_stats, 0);
}

/// Zero in pickle too: packing writes into reused `frame`/`datapoint` fields and patches the
/// length prefix in place.
#[test]
fn graphite_encode_into_100_pickle_events() {
    let mut encoder =
        fixtures::graphite_encoder().with_protocol(logit_proto::graphite::Protocol::Pickle);
    let batch = fixtures::graphite_batch(100);
    let mut out = MessageBuf::<usize>::default();

    let (stats, alloc_stats) = measure_framed(&mut encoder, &batch, &mut out);
    assert!(!out.is_empty());
    assert_eq!(stats.datapoints, 100);
    expect_allocs("graphite_out: encode_into 100 pickle events", alloc_stats, 0);
}

/// Zero: expanding a `Distribution` into `.count`/`.sum`/`.q*` sub-paths reads the existing
/// `DdSketch` in place (`expand_sketch`).
#[test]
fn graphite_encode_into_100_distribution_events_expanded() {
    let mut encoder =
        fixtures::graphite_encoder().with_multi_value(logit_proto::graphite::MultiValue::Expand);
    let batch = fixtures::graphite_distribution_batch(100);
    let mut out = MessageBuf::<usize>::default();

    let (stats, alloc_stats) = measure_framed(&mut encoder, &batch, &mut out);
    assert_eq!(stats.degraded_expanded_kind, 100);
    expect_allocs("graphite_out: encode_into 100 Distribution events (expand)", alloc_stats, 0);
}

/// 100, one per event: expanding a raw `Samples` record calls `Samples::sketch()`, which builds a
/// `DdSketch` from the values (its `bins` `Vec`). Inherent to summarizing `Samples` on the way
/// out, and the same cost `influxdb_out` pays per `Samples`.
#[test]
fn graphite_encode_into_100_samples_events_expanded() {
    let mut encoder =
        fixtures::graphite_encoder().with_multi_value(logit_proto::graphite::MultiValue::Expand);
    let batch = fixtures::graphite_samples_batch(100);
    let mut out = MessageBuf::<usize>::default();

    let (stats, alloc_stats) = measure_framed(&mut encoder, &batch, &mut out);
    assert_eq!(stats.degraded_expanded_kind, 100);
    expect_allocs("graphite_out: encode_into 100 Samples events (expand)", alloc_stats, 100);
}

/// `prometheus_out`'s exposition path, two plain functions rather than an `Encoder`
/// (`docs/adr/prometheus-scrape-and-exposition.md`'s "No `logit_proto::Encoder`" section):
/// `events_to_families` (the sink's `send`) then `text::write` (a scrape's render), over 100
/// distinct gauge series in one family (`fixtures::prometheus_gauge_events`).
#[test]
fn prometheus_encode_100_series() {
    use logit_proto::prometheus::events_to_families;
    use logit_proto::prometheus::text::{write, Dialect};

    let mut encoder = fixtures::prometheus_encoder();
    let events = fixtures::prometheus_gauge_events(100);
    let resource = fixtures::resource();
    let warm_families =
        events_to_families(events.iter().map(|event| (resource.as_ref(), event)), &mut encoder);
    let mut warm_out = Vec::new();
    write(&warm_families, Dialect::Text0_0_4, &mut warm_out);

    let (out, stats) = measure(|| {
        let families =
            events_to_families(events.iter().map(|event| (resource.as_ref(), event)), &mut encoder);
        let mut out = Vec::new();
        write(&families, Dialect::Text0_0_4, &mut out);
        out
    });
    let text = std::str::from_utf8(&out).expect("exposition must be utf-8");
    let series = text.lines().filter(|line| line.starts_with("prom_bench_gauge{")).count();
    assert_eq!(series, 100, "expected all 100 distinct series to render, got:\n{text}");
    expect_allocs("prometheus_out: encode 100 series", stats, 414);
}

/// `prometheus_out`'s remote-write egress: `remote_write::encode` over
/// `fixtures::remote_write_families`' 100-series gauge family. The protobuf codec only;
/// `events_to_families` is pinned above and Snappy is the sink's.
#[test]
fn remote_write_encode_100_series_v1() {
    use logit_proto::prometheus::remote_write::{encode, Version};

    let mut encoder = fixtures::prometheus_encoder();
    let families = fixtures::remote_write_families(100);
    let groups = std::slice::from_ref(&families);
    drop(encode(groups, Version::V1, &mut encoder));

    let (body, stats) = measure(|| encode(groups, Version::V1, &mut encoder));
    assert!(!body.is_empty());
    expect_allocs("prometheus_out: encode 100 series, remote-write 1.0", stats, 1223);
}

/// The same in 2.0, where every label name, label value, and help string goes through the symbol
/// table: 1434 against 1.0's 1223. The extra ~2/series is the table's owned-`String` key per
/// distinct value, one per series for this fixture's `shard` labels. The decode pair has the
/// opposite sign: interning costs the sender and saves the receiver.
#[test]
fn remote_write_encode_100_series_v2() {
    use logit_proto::prometheus::remote_write::{encode, Version};

    let mut encoder = fixtures::prometheus_encoder();
    let families = fixtures::remote_write_families(100);
    let groups = std::slice::from_ref(&families);
    drop(encode(groups, Version::V2, &mut encoder));

    let (body, stats) = measure(|| encode(groups, Version::V2, &mut encoder));
    assert!(!body.is_empty());
    expect_allocs("prometheus_out: encode 100 series, remote-write 2.0", stats, 1434);
}

// ---------------------------------------------------------------------------------------------
// Lua
// ---------------------------------------------------------------------------------------------

/// One round trip across the Rust/Lua boundary for a script that reads one attribute and writes
/// one: 9. `process` is a cached `RegistryKey`, `event.attributes` returns one cached `AttrsProxy`
/// per event, and both metamethods take `mlua::String`, so no key is copied.
///
/// The breakdown, each piece isolated against a narrower script:
///
/// - **4** for any call: the `Rc<RefCell<Event>>` and the `EventProxy` userdata (2), plus 2 in
///   mlua/LuaJIT's per-call bookkeeping (reference-table and GC upkeep as the previous call's
///   objects become collectible).
/// - **+1** for the `Box` on the way out (`ProcessOutcome::Emit`); a `nil`-returning script lands
///   at 4.
/// - **+3** for creating and caching the `AttrsProxy` on an event's first `event.attributes`
///   access, read or write. Later accesses are free: two reads total 8, the same as one.
/// - **+1** for the attribute write: `lua_to_value` allocates a `Bytes` for `"prod"`.
///
/// `lua::to_table` in `benches/pipeline.rs` compares the proxy against a full table conversion.
#[test]
fn lua_process_one_event() {
    let worker = ScriptWorker::new(fixtures::LUA_ENRICH_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::nginx_event()));

    let event = fixtures::nginx_event();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: process 1 event", stats, 9);
}

/// `run_lua`'s per-batch resource contract (`set_resource`, `process`, `take_resource`) for a
/// script that never touches `resource`: 9, the same as [`lua_process_one_event`].
/// `set_resource` assigns two fields, and `take_resource` returns `None` without allocating.
#[test]
fn lua_process_one_event_with_resource_hooks_but_no_write_costs_the_same_as_process_alone() {
    let worker = ScriptWorker::new(fixtures::LUA_ENRICH_SCRIPT).expect("script should load");
    let resource = fixtures::resource();
    worker.set_resource(&resource);
    drop(worker.process(fixtures::nginx_event()));
    drop(worker.take_resource());

    let event = fixtures::nginx_event();
    let (outcome, stats) = measure(|| {
        worker.set_resource(&resource);
        let outcome = worker.process(event).expect("script should run");
        assert!(worker.take_resource().is_none(), "the script never writes resource");
        outcome
    });
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: set_resource + process + take_resource, no write", stats, 9);
}

/// A script writing `resource` (`docs/adr/operator-declared-resource-attributes.md`), touching no
/// event attribute: 7. The 4-allocation call baseline, `+1` for the `Box` on
/// `ProcessOutcome::Emit`, `+1` for `lua_to_resource_value`'s `Bytes` for `"nginx"`, and `+1` for
/// `take_resource`'s `Arc::new(Resource { .. })` commit. The copy-on-write `AttrMap` clone is
/// free because the fixture resource's map is empty and inline.
#[test]
fn lua_process_one_event_writing_resource() {
    let worker =
        ScriptWorker::new(fixtures::LUA_RESOURCE_WRITE_SCRIPT).expect("script should load");
    let resource = fixtures::resource();
    worker.set_resource(&resource);
    drop(worker.process(fixtures::nginx_event()));
    drop(worker.take_resource());

    let event = fixtures::nginx_event();
    let (committed, stats) = measure(|| {
        worker.set_resource(&resource);
        let outcome = worker.process(event).expect("script should run");
        assert!(matches!(outcome, ProcessOutcome::Emit(..)));
        worker.take_resource()
    });
    let committed = committed.expect("the script writes resource on every call");
    assert_eq!(committed.attributes.get("service.name"), Some(&Value::str("nginx")));
    expect_allocs("lua: set_resource + process + take_resource, writing resource", stats, 7);
}

/// A script reading `event.log.trace_id` (`LogProxy`, `docs/adr/log-record-trace-context.md`): 9,
/// the same total as [`lua_process_one_event`] without touching `event.attributes`. Creating and
/// caching the `LogProxy` costs what `AttrsProxy` does (+3), and `to_hex`'s returned `String`
/// costs what the attribute write does (+1). Measured, not derived.
#[test]
fn lua_process_one_event_reading_log_trace() {
    let worker =
        ScriptWorker::new(fixtures::LUA_LOG_TRACE_READ_SCRIPT).expect("script should load");
    let mut warm = fixtures::nginx_event();
    warm.log.as_mut().unwrap().trace =
        Some(TraceRef { trace_id: [1; 16], span_id: None, flags: 0 });
    drop(worker.process(warm));

    let mut event = fixtures::nginx_event();
    event.log.as_mut().unwrap().trace =
        Some(TraceRef { trace_id: [1; 16], span_id: None, flags: 0 });
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: process 1 event, reading event.log.trace_id", stats, 9);
}

/// A script reading `event.metrics[1].value` (`MetricsProxy`/`MetricProxy`): 11. 4 baseline, +1
/// `Box` on `Emit`, +3 for creating and caching `MetricsProxy` on the first `event.metrics` access
/// (as [`lua_process_one_event_reading_metric_len`] pins), and +3 for the per-index `MetricProxy`.
///
/// Two narrower scripts isolate the pieces. Indexing `event.metrics[1]` without reading `.value`
/// is still 11, so the `f64` read is free. Indexing it twice is 14, so each index costs +3
/// (14 - 11 = 11 - 8). An uncached `MetricProxy` minted by `lua.create_userdata` in an `Index`
/// metamethod costs the same 3 that `AttrsProxy`/`LogProxy`/`SpanProxy` pay to create and cache
/// through a `RegistryKey`. `MetricsProxy` doesn't cache per-index handles, so a script indexing
/// the same metric repeatedly pays the 3 on every index (`MetricsProxy`'s doc comment weighs that
/// trade).
///
/// `sum_metric_event` has empty attributes and one inline metric, so an `Event::clone` on the way
/// out would be free here. [`lua_process_one_event_reading_metric_value_on_a_spilled_event`] is
/// the guard for that.
#[test]
fn lua_process_one_event_reading_metric_value() {
    let worker =
        ScriptWorker::new(fixtures::LUA_METRIC_VALUE_READ_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::sum_metric_event()));

    let event = fixtures::sum_metric_event();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: process 1 event, reading event.metrics[1].value", stats, 11);
}

/// [`lua_process_one_event_reading_metric_value`]'s script on
/// [`fixtures::sum_metric_event_with_spilled_attributes`]: the guard that `MetricProxy::event`
/// stays a `Weak<RefCell<Event>>`. 11, the same as the plain fixture:
/// [`lua_process_one_event_passthrough_on_a_spilled_event`]'s 5 plus the +6 that
/// `event.metrics[1].value` costs.
///
/// If a `MetricProxy` held a strong `Rc`, an uncollected `event.metrics[1]` temporary (LuaJIT's GC
/// is incremental) could still be alive when `process()` returns. `EventProxy::into_inner`'s
/// `Rc::try_unwrap` would then fail and fall back to cloning the `Event`, and this fixture's
/// spilled `AttrMap` makes that clone allocate. If this count rises while the passthrough row
/// holds, `MetricProxy` is keeping the event alive past the call.
#[test]
fn lua_process_one_event_reading_metric_value_on_a_spilled_event() {
    let worker =
        ScriptWorker::new(fixtures::LUA_METRIC_VALUE_READ_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::sum_metric_event_with_spilled_attributes()));

    let event = fixtures::sum_metric_event_with_spilled_attributes();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs(
        "lua: process 1 event (spilled attrs), reading event.metrics[1].value",
        stats,
        11,
    );
}

/// [`fixtures::sum_metric_event_with_spilled_attributes`] through a script that touches nothing,
/// the baseline for the row above. 5 = 4 (baseline) + 1 (`Box` on `Emit`). With no proxy created,
/// `EventProxy`'s `Rc` is the only strong reference, so `into_inner`'s `Rc::try_unwrap` succeeds
/// and the spilled map is never cloned.
#[test]
fn lua_process_one_event_passthrough_on_a_spilled_event() {
    let worker = ScriptWorker::new(fixtures::LUA_PASSTHROUGH_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::sum_metric_event_with_spilled_attributes()));

    let event = fixtures::sum_metric_event_with_spilled_attributes();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: process 1 event (spilled attrs), passthrough", stats, 5);
}

/// A script reading only `#event.metrics`, with no indexing: 8 = 4 (baseline) + 1 (`Box` on
/// `Emit`) + 3 (`MetricsProxy` create-and-cache). Three less than
/// [`lua_process_one_event_reading_metric_value`]'s 11: the difference is the per-index
/// `MetricProxy`.
#[test]
fn lua_process_one_event_reading_metric_len() {
    let worker =
        ScriptWorker::new(fixtures::LUA_METRIC_LEN_READ_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::sum_metric_event()));

    let event = fixtures::sum_metric_event();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: process 1 event, reading #event.metrics", stats, 8);
}

/// A script reading `event.span.name` (`SpanProxy`): 8 = 4 (baseline) + 1 (`Box` on `Emit`) + 3
/// (`SpanProxy` create-and-cache on first access). The name goes through `lua.create_string`,
/// which allocates in LuaJIT's own heap, invisible to the counting allocator.
#[test]
fn lua_process_one_event_reading_span_name() {
    let worker =
        ScriptWorker::new(fixtures::LUA_SPAN_NAME_READ_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::span_event()));

    let event = fixtures::span_event();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: process 1 event, reading event.span.name", stats, 8);
}

/// `run_lua`'s per-batch `scope` contract (`set_scope`, `process`, `take_scope`) for a script that
/// never touches `scope`: 9, the same as [`lua_process_one_event`]. `set_scope` assigns two
/// fields and `take_scope` returns `None` without allocating.
#[test]
fn lua_process_one_event_with_scope_hooks_but_no_write_costs_the_same_as_process_alone() {
    let worker = ScriptWorker::new(fixtures::LUA_ENRICH_SCRIPT).expect("script should load");
    let scope = Some(fixtures::scope());
    worker.set_scope(&scope);
    drop(worker.process(fixtures::nginx_event()));
    drop(worker.take_scope());

    let event = fixtures::nginx_event();
    let (outcome, stats) = measure(|| {
        worker.set_scope(&scope);
        let outcome = worker.process(event).expect("script should run");
        assert!(worker.take_scope().is_none(), "the script never writes scope");
        outcome
    });
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: set_scope + process + take_scope, no write", stats, 9);
}

/// A script reading `scope.name`: 5 = 4 (baseline) + 1 (`Box` on `Emit`). No +3 first-access
/// cost: `scope` is a global installed once in `ScriptWorker::new`, not per-event userdata.
#[test]
fn lua_process_one_event_reading_scope_name() {
    let worker =
        ScriptWorker::new(fixtures::LUA_SCOPE_NAME_READ_SCRIPT).expect("script should load");
    let scope = Some(fixtures::scope());
    worker.set_scope(&scope);
    drop(worker.process(fixtures::nginx_event()));
    drop(worker.take_scope());

    let event = fixtures::nginx_event();
    let (outcome, stats) = measure(|| {
        worker.set_scope(&scope);
        let outcome = worker.process(event).expect("script should run");
        drop(worker.take_scope());
        outcome
    });
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: set_scope + process (reading scope.name) + take_scope", stats, 5);
}

/// A script's first write to `scope.attributes` in a batch (`ensure_modified`'s copy-on-write
/// path): 7 = 4 (baseline) + 1 (`Box` on `Emit`) + 1 (`lua_to_scope_value`'s `Bytes` for `"v"`) +
/// 1 (`take_scope`'s `Arc::new(Scope { .. })` commit). The `Scope` clone itself is free: the
/// fixture's `name`/`version` are `Bytes::from_static` and its `attributes` map is empty and
/// inline.
#[test]
fn lua_process_one_event_writing_scope_attribute() {
    let worker =
        ScriptWorker::new(fixtures::LUA_SCOPE_ATTR_WRITE_SCRIPT).expect("script should load");
    let scope = Some(fixtures::scope());
    worker.set_scope(&scope);
    drop(worker.process(fixtures::nginx_event()));
    drop(worker.take_scope());

    let event = fixtures::nginx_event();
    let (committed, stats) = measure(|| {
        worker.set_scope(&scope);
        let outcome = worker.process(event).expect("script should run");
        assert!(matches!(outcome, ProcessOutcome::Emit(..)));
        worker.take_scope()
    });
    let committed = committed.expect("the script writes scope on every call");
    assert_eq!(committed.attributes.get("k"), Some(&Value::str("v")));
    expect_allocs("lua: set_scope + process (writing scope.attributes.k) + take_scope", stats, 7);
}

/// A script writing `resource.schema_url` (`write_schema_url`): 7 = 4 (baseline) + 1 (`Box` on
/// `Emit`) + 1 (`Bytes::copy_from_slice` for the URL) + 1 (`take_resource`'s
/// `Arc::new(Resource { .. })` commit), the same as [`lua_process_one_event_writing_resource`].
/// `ensure_modified`'s clone is free: `Resource::default()` has no `schema_url` and an empty map.
#[test]
fn lua_process_one_event_writing_resource_schema_url() {
    let worker = ScriptWorker::new(fixtures::LUA_RESOURCE_SCHEMA_URL_WRITE_SCRIPT)
        .expect("script should load");
    let resource = fixtures::resource();
    worker.set_resource(&resource);
    drop(worker.process(fixtures::nginx_event()));
    drop(worker.take_resource());

    let event = fixtures::nginx_event();
    let (committed, stats) = measure(|| {
        worker.set_resource(&resource);
        let outcome = worker.process(event).expect("script should run");
        assert!(matches!(outcome, ProcessOutcome::Emit(..)));
        worker.take_resource()
    });
    let committed = committed.expect("the script writes resource.schema_url on every call");
    assert_eq!(committed.schema_url.as_deref(), Some(b"https://example.com/schema".as_slice()));
    expect_allocs(
        "lua: set_resource + process (writing resource.schema_url) + take_resource",
        stats,
        7,
    );
}

/// `scope.name = scope.name`, an identity write: 5, the same as
/// [`lua_process_one_event_reading_scope_name`]. The no-op check (a byte-slice comparison against
/// the installed name) runs before `ensure_modified`, so `take_scope` still returns `None`.
#[test]
fn lua_process_one_event_identity_write_to_scope_name_is_free() {
    let worker =
        ScriptWorker::new(fixtures::LUA_SCOPE_IDENTITY_NAME_SCRIPT).expect("script should load");
    let scope = Some(fixtures::scope());
    worker.set_scope(&scope);
    drop(worker.process(fixtures::nginx_event()));
    drop(worker.take_scope());

    let event = fixtures::nginx_event();
    let (outcome, stats) = measure(|| {
        worker.set_scope(&scope);
        let outcome = worker.process(event).expect("script should run");
        assert!(worker.take_scope().is_none(), "an identity write must not count as a write");
        outcome
    });
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: set_scope + process (scope.name = scope.name) + take_scope", stats, 5);
}

/// `Event.new{timestamp = "1", attributes = {env = "prod"}, log = {message = "hi"}}` returned
/// from `process()` in place of the incoming event (`docs/adr/lua-event-constructor.md`), the
/// smallest useful constructed event. A script that never calls `Event.new` pays none of this.
///
/// 17, each piece isolated against a narrower script:
///
/// - **4** for any call (a `return nil` script).
/// - **+6** for `Event.new{timestamp = "1"}`, returned or not (returning `nil` lands at 10): the
///   new handle's `Rc<RefCell<Event>>` and `EventProxy` userdata, plus mlua's cost of calling a
///   Rust closure and handing back userdata, not attributable line by line. Reading the argument
///   table and parsing `timestamp` allocate nothing, and error-path `format!`s are lazy.
/// - **+1** for the `Box` on `ProcessOutcome::Emit` (11).
/// - **+3** for `attributes = {env = "prod"}`: 1 for the sub-table (`attributes = {}` lands at
///   12) and 2 for the entry, one of them `lua_to_value`'s `Bytes` for `"prod"`. A second
///   attribute adds 1, so the other is the map's first insert, not per entry.
/// - **+3** for `log = {message = "hi"}`: 1 for the sub-table and 2 for `message`, including
///   the `Bytes` for `"hi"`. `LogRecord` is inline in `Event`, so the record needs no box.
#[test]
fn lua_process_one_event_constructing_a_log_event() {
    let worker = ScriptWorker::new(fixtures::LUA_EVENT_NEW_LOG_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::nginx_event()));

    let event = fixtures::nginx_event();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: Event.new log event from a literal table", stats, 17);
}

/// `Event.new{timestamp = "1", metrics = {{name = "tick", kind = "gauge", value = 1}}}` returned
/// from `process()` in place of the incoming event: the smallest useful constructed metric event,
/// the shape a `flush(now)` tick emits. Measured as
/// [`lua_process_one_event_constructing_a_log_event`] is.
///
/// 16, each piece isolated against a narrower script:
///
/// - **11**: the log row's first three terms (4 baseline, +6 `Event.new`, +1 `Box`).
/// - **+1** for the `metrics` array table (`metrics = {}` lands at 12; `with_capacity(0)` and a
///   one-record list both stay inline).
/// - **+4** for the one metric: its sub-table; `validated_sequence_len`'s key `Vec` over the array
///   (once per event: a second metric adds 3 plus the `MetricList` spill, 20 in all); the
///   `format!`'d `metrics[i]` path field errors are prefixed with (a static path lands at 15; the
///   one per-metric cost, cheaper than threading the index through every helper); and the 1 every
///   non-empty record sub-table carries beyond its table and fields (`log = {message = 1}` lands
///   at 13 against 12 for an empty one, and a second field adds nothing).
///
/// `intern("tick")`, `kind`'s borrowed `to_string_lossy`, the `expect_keys` walk, and nil-defaulted
/// fields allocate nothing; `unit = "ms"` adds 0 and an empty `exemplars = {}` adds 1 (the table).
#[test]
fn lua_process_one_event_constructing_a_gauge_event() {
    let worker =
        ScriptWorker::new(fixtures::LUA_EVENT_NEW_GAUGE_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::nginx_event()));

    let event = fixtures::nginx_event();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: Event.new gauge event from a literal table", stats, 16);
}

/// `Event.new{timestamp = "1", span = {trace_id = <hex>, span_id = <hex>, name = "GET /"}}`
/// returned from `process()` in place of the incoming event: the smallest useful constructed span
/// event, every core default applied (`kind` internal, `status` unset, `end_timestamp` the event's
/// own, no `SpanExt`, empty `events`/`links`). Measured as
/// [`lua_process_one_event_constructing_a_log_event`] is.
///
/// 14, each piece isolated against a narrower script:
///
/// - **11**: the log row's first three terms (4 baseline, +6 `Event.new`, +1 `Box`).
/// - **+3** for the `span` sub-table: the table, `lua_to_value`'s `Bytes` for `name`
///   (`name = 1` lands at 13), and the 1 every non-empty record sub-table carries. The hex ids
///   parse into `[u8; N]` arrays, `SpanRecord` is inline in `Event`, and empty
///   `events`/`links` are free.
/// - **+0** for defaulted or scalar fields: `kind = "server", status = "ok",
///   end_timestamp = "2", parent_span_id = <hex>` also land at 14, as does an explicit
///   `dropped_events_count = 0, flags = 0`. A default value never earns the `SpanExt` box.
///
/// Beyond the pin: `status_message = "boom"` adds 2 (the `Box<SpanExt>` and the `Bytes`); an
/// empty `events = {}` or `links = {}` adds 1 (the Lua table); one span event
/// `{timestamp = "1", name = "e"}` adds 7 (the `events` table, its row table, the key `Vec`, the
/// `format!`'d path, the name's `Bytes`, the `Vec<SpanEvent>`, and the per-record 1); one link
/// with only its ids adds 6 (the same minus the `Bytes`).
#[test]
fn lua_process_one_event_constructing_a_span_event() {
    let worker =
        ScriptWorker::new(fixtures::LUA_EVENT_NEW_SPAN_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::nginx_event()));

    let event = fixtures::nginx_event();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(..)));
    expect_allocs("lua: Event.new span event from a literal table", stats, 14);
}

// ---------------------------------------------------------------------------------------------
// End to end
// ---------------------------------------------------------------------------------------------

/// Decode through aggregation for one access-log line -- the number that bounds ingest throughput
/// for the reference config. Excludes the output encoders, which run once per flush window rather
/// than once per event, and excludes fan-out, which the config's `tap` branch adds.
///
/// 3 = 1 (decode) + 1 (json) + 1 (kv_metrics) + 0 (keep) + 0 (keep_values) + 0 (aggregate).
/// `kv_metrics` emits raw `Samples`, which `aggregate` absorbs into its per-series sketch without
/// allocating ([`aggregate_absorb_one_samples_event_sketch_mode`]). `keep_values` is free because
/// the fixture's `host` is already allowed and lowercase; a `Host` needing lowering would add
/// [`keep_values_one_event_needs_lowering`]'s 1.
#[test]
fn full_chain_one_line() {
    let resource = fixtures::resource();
    let mut decoder = fixtures::syslog_decoder();
    let mut json = fixtures::json_parser();
    let mut kv = fixtures::kv_metrics();
    let mut keep = fixtures::keep();
    let mut keep_values = fixtures::keep_values();
    let mut agg = fixtures::aggregator();
    let datagram = fixtures::nginx_syslog_datagram(1);

    macro_rules! run {
        () => {{
            let batch = decoder.decode(datagram.clone()).expect("should decode");
            for mut event in batch.events {
                assert!(json.process(&resource, &mut event), "json forwards");
                assert!(kv.process(&resource, &mut event), "kv forwards");
                assert!(keep.process(&resource, &mut event), "keep forwards");
                assert!(keep_values.process(&resource, &mut event), "keep_values forwards");
                agg.process(&resource, &mut event);
            }
        }};
    }

    for _ in 0..4 {
        run!();
    }
    let (_, stats) = measure(|| run!());
    expect_allocs("full chain: 1 access-log line", stats, 3);
}

// ---------------------------------------------------------------------------------------------
// Structural guards on claims docs/design/memory.md makes
// ---------------------------------------------------------------------------------------------

/// Syslog's zero-copy claim, stated structurally: the message and tag point into the datagram
/// buffer they were decoded from (`docs/design/data-model.md`).
#[test]
fn syslog_fields_share_the_datagram_allocation() {
    let datagram = fixtures::nginx_syslog_datagram(1);
    let mut decoder = fixtures::syslog_decoder();
    let batch = decoder.decode(datagram.clone()).expect("should decode");
    let event = batch.events.into_iter().next().expect("one event");

    let Value::Str(message) = &event.log.as_ref().expect("a log").message else {
        panic!("the message should be a Str");
    };
    assert!(points_into(&datagram, message), "log.message should slice the datagram, not copy it");

    let tag = event.attributes.get("syslog.tag").expect("a tag");
    let Value::Str(tag) = tag else { panic!("the tag should be a Str") };
    assert!(points_into(&datagram, tag), "syslog.tag should slice the datagram too");
}

/// statsd's zero-copy claim, stated structurally: a DogStatsD tag value points into the datagram
/// buffer, via `statsd.rs`'s `slice_of`.
#[test]
fn statsd_tag_values_share_the_datagram_allocation() {
    let datagram = fixtures::statsd_datagram(1);
    let mut decoder = fixtures::statsd_decoder();
    let batch = decoder.decode(datagram.clone()).expect("should decode");
    let event = batch.events.into_iter().next().expect("one event");

    let env = event.attributes.get("env").expect("the env tag");
    let Value::Str(env) = env else { panic!("the tag value should be a Str") };
    assert_eq!(env.as_ref(), b"prod");
    assert!(points_into(&datagram, env), "env tag value should slice the datagram, not copy it");
}

/// A UDP listener copies each datagram out of the reusable 64 KB receive buffer with
/// `Bytes::copy_from_slice`: one allocation sized to the datagram, not the buffer. Reading into a
/// large shared `BytesMut` would save the allocation but let one retained line pin 64 KB
/// (`docs/design/memory.md`'s "Retention: what pins what" section).
#[test]
fn datagram_copy_is_one_right_sized_allocation() {
    let recv_buffer = vec![0u8; 65_507];
    let n = fixtures::NGINX_SYSLOG_LINE.len();
    drop(bytes::Bytes::copy_from_slice(&recv_buffer[..n]));

    let (datagram, stats) = measure(|| bytes::Bytes::copy_from_slice(&recv_buffer[..n]));
    assert_eq!(datagram.len(), n);
    expect_allocs("input read loop: copy 1 datagram", stats, 1);
    assert!(
        stats.bytes < 1024,
        "the copy should be sized to the datagram ({n} bytes), not to the 64 KB receive buffer; \
         got {} bytes",
        stats.bytes
    );
}

fn points_into(haystack: &bytes::Bytes, needle: &bytes::Bytes) -> bool {
    let base = haystack.as_ptr() as usize;
    let start = needle.as_ptr() as usize;
    start >= base && start + needle.len() <= base + haystack.len()
}

// ---------------------------------------------------------------------------------------------
// Interning
// ---------------------------------------------------------------------------------------------

/// Re-interning a string the table already holds allocates nothing, so a pipeline whose keys and
/// metric names come from a fixed schema reaches steady state and stays there. The table grows
/// only for a distinct string: the exposure is names that never repeat, not lookups that miss
/// (`docs/design/memory.md`'s "Interning: the bargain, and its bounds" section).
#[test]
fn re_interning_an_existing_string_is_free() {
    let names: Vec<String> = (0..1000).map(|i| format!("steady.state.metric.{i}")).collect();
    for name in &names {
        logit_core::interner::intern(name);
    }

    let (_, stats) = measure(|| {
        for name in &names {
            logit_core::interner::intern(name);
        }
    });
    expect_allocs("interner: re-intern 1000 known names", stats, 0);
}

// ---------------------------------------------------------------------------------------------
// AttrMap growth (docs/plans/event-sizing.md)
// ---------------------------------------------------------------------------------------------

/// `AttrMap`'s spill-and-grow ladder, which every sizing argument in `docs/plans/event-sizing.md`
/// rests on. Per attribute count `k`, building one map a sorted `insert_sym` at a time:
///
/// | k | allocs | reallocs | heap bytes | capacity |
/// |--:|--:|--:|--:|--:|
/// | 1, 8 | 0 | 0 | 0 (inline) | 8 |
/// | 9, 16 | 1 | 0 | 768 | 16 |
/// | 17, 24, 32 | 1 | 1 | 1536 | 32 |
/// | 33 | 1 | 2 | 3072 | 64 |
///
/// The 9th entry spills to a heap buffer of twice the inline capacity, and every doubling after
/// that is a `realloc`, so allocs stay at 1 and the growth shows only in reallocs. A 24-attribute
/// map holds 1536 bytes of heap for 1152 bytes of entries, on top of 392 abandoned inline bytes.
///
/// Capacity is read off `peak_live_bytes`: `AttrMap` exposes no `capacity` (nor
/// `reserve`/`with_capacity`), and at 48 bytes per `(Symbol, Value)` entry
/// (`crates/logit-core/tests/type_sizes.rs`) the live heap is `capacity * 48`. The symbols are
/// interned up front and the values are `I64`s, so nothing else allocates.
#[test]
fn attr_map_spills_to_double_its_inline_capacity_then_reallocs() {
    let syms: Vec<logit_core::interner::Symbol> =
        (0..33).map(|i| logit_core::interner::intern(&format!("growth.probe.k{i:02}"))).collect();

    let build = |k: usize| {
        measure(|| {
            let mut map = AttrMap::new();
            for (i, sym) in syms.iter().take(k).enumerate() {
                map.insert_sym(*sym, Value::I64(i as i64));
            }
            map
        })
    };
    drop(build(33)); // warm

    // (k, allocs, reallocs, capacity)
    for (k, allocs, reallocs, capacity) in [
        (1usize, 0u64, 0u64, 8u64),
        (8, 0, 0, 8),
        (9, 1, 0, 16),
        (16, 1, 0, 16),
        (17, 1, 1, 32),
        (24, 1, 1, 32),
        (32, 1, 1, 32),
        (33, 1, 2, 64),
    ] {
        let (map, stats) = build(k);
        assert_eq!(map.len(), k);
        expect_allocs(&format!("AttrMap: build {k} attributes"), stats, allocs);
        assert_eq!(stats.reallocs, reallocs, "AttrMap: build {k} attributes -- realloc count");
        let measured = if stats.peak_live_bytes == 0 { 8 } else { stats.peak_live_bytes / 48 };
        assert_eq!(measured, capacity, "AttrMap: build {k} attributes -- capacity");
    }
}

// ---------------------------------------------------------------------------------------------
// Survey-derived shapes (docs/design/data-shapes.md, docs/plans/event-sizing.md)
// ---------------------------------------------------------------------------------------------
//
// The six shapes the data-shape survey measured. Each fixture's doc comment
// (`crates/logit-bench/src/fixtures.rs`) cites the survey row it models and says what is modelled
// rather than captured.
//
// Three measurements per shape, the three `docs/design/data-shapes.md` §6 says a sizing decision
// turns on: **build** (through the real parser where one produces the shape), **clone** (VRL's
// flat-versus-tree crossover moved from ~128 fields to ~16 once its benchmark cloned), and a
// native **encode + decode** round trip, the one codec that reads an exact attribute count off
// the wire and can't use it (`read_attr_map_at`, against `read_record_list_into`, which reserves).

/// The build half: `tail_in`'s one attribute plus [`fixtures::FLAT_JSON_LOG_BODY`]'s eleven JSON
/// keys, merged by `json` into 12, the commonest measured log width
/// (`docs/design/data-shapes.md` §5.3).
///
/// One allocation, the `AttrMap` spill: one 768-byte buffer with room for 16
/// ([`attr_map_spills_to_double_its_inline_capacity_then_reallocs`]). Every value is a zero-copy
/// slice of the message, and `json`'s scratch and key cache are warm. [`json_parse_one_event`]
/// reports the same one at 10 attributes: widths 9 through 16 add no allocations, which is why an
/// allocation count alone is the wrong proxy for what a wider map costs.
#[test]
fn json_parse_flat_json_log_event() {
    let resource = fixtures::resource();
    let mut json = fixtures::json_parser();
    let mut warm = fixtures::flat_json_log_event();
    assert!(json.process(&resource, &mut warm), "json always forwards");
    assert_eq!(warm.attributes.len(), 12);

    let (event, stats) = measure(|| {
        let mut event = fixtures::flat_json_log_event();
        assert!(json.process(&resource, &mut event));
        event
    });
    assert_eq!(event.attributes.len(), 12, "1 from tail_in + 11 JSON keys");
    expect_allocs("json: parse 12-attribute flat log", stats, 1);
}

/// The nested pino-http shape: five allocations for a 10-attribute event, against
/// [`json_parse_flat_json_log_event`]'s one for twelve. One is the map spill. The other four are
/// the nested maps (`req{}`, `res{}`, and the `headers{}` inside each): `Value::Map` is
/// `Box<AttrMap>`, so every nested object is a fresh 392-byte box whatever its width. Each of the
/// four scales with any change to `AttrMap`'s inline capacity (`docs/design/data-shapes.md` §6).
#[test]
fn json_parse_pino_http_nested_event() {
    let resource = fixtures::resource();
    let mut json = fixtures::json_parser();
    let mut warm = fixtures::pino_http_log_event();
    assert!(json.process(&resource, &mut warm), "json always forwards");
    assert_eq!(warm.attributes.len(), 10);

    let (event, stats) = measure(|| {
        let mut event = fixtures::pino_http_log_event();
        assert!(json.process(&resource, &mut event));
        event
    });
    assert_eq!(event.attributes.len(), 10, "1 from tail_in + 9 pino-http keys");
    expect_allocs("json: parse 10-attribute nested pino-http log", stats, 5);
}

/// The widest log class in the survey, 30 attributes: three allocations and two reallocations.
/// The map spills at 9 (capacity 16) and grows to 32 at 17
/// ([`attr_map_spills_to_double_its_inline_capacity_then_reallocs`]), ending in a 1536-byte buffer
/// holding 1440 bytes of entries.
///
/// Only one of the three is the map. PostgreSQL's error message quotes an identifier
/// (`"orders_pkey"`), so that value carries a JSON escape and can't be sliced zero-copy; it costs
/// the other two. That's a property of the format, not the fixture: a constraint-violation line
/// always names the constraint.
#[test]
fn json_parse_access_log_event() {
    let resource = fixtures::resource();
    let mut json = fixtures::json_parser();
    let mut warm = fixtures::access_log_event();
    assert!(json.process(&resource, &mut warm), "json always forwards");
    assert_eq!(warm.attributes.len(), 30);

    let (event, stats) = measure(|| {
        let mut event = fixtures::access_log_event();
        assert!(json.process(&resource, &mut event));
        event
    });
    assert_eq!(event.attributes.len(), 30, "1 from tail_in + PostgreSQL jsonlog's 29 keys");
    expect_allocs("json: parse 30-attribute access log", stats, 3);
    assert_eq!(stats.reallocs, 2, "the map grows 8 -> 16 -> 32 on the way to 30 entries");
}

/// Runs `json` over a JSON-bodied survey shape, so the clone and round-trip tests start from the
/// event the build tests produce.
fn parsed_survey_event(make: fn() -> logit_core::Event) -> logit_core::Event {
    let resource = fixtures::resource();
    let mut json = fixtures::json_parser();
    let mut event = make();
    assert!(json.process(&resource, &mut event), "json always forwards");
    event
}

#[track_caller]
fn expect_clone_allocs(label: &str, event: &logit_core::Event, expected: u64) {
    drop(event.clone());
    let (cloned, stats) = measure(|| event.clone());
    assert_eq!(cloned.attributes.len(), event.attributes.len());
    expect_allocs(label, stats, expected);
}

/// `Event::clone` (one extra fan-out branch) for each survey shape. The spread isn't the one the
/// attribute counts predict:
///
/// | Shape | Attributes | `Event::clone` allocations |
/// |---|--:|--:|
/// | 12-attribute flat JSON log | 12 | 1 |
/// | pino-http nested record | 10 | **5** |
/// | 30-attribute access log | 30 | 1 |
/// | 17-attribute server span | 17 | 1 |
/// | 3-record collectd event | 6 | 1 |
///
/// A spilled `AttrMap` costs one allocation to clone however far past 8 it is: smallvec clones the
/// entries into one fresh buffer, sized by `reserve` to the next power of two (768 bytes for 12
/// entries, 1536 for 30; `tests/attr_arms.rs` pins the bytes). The 30- and 12-attribute logs differ
/// only in entries copied, plus the 864-byte `Event` either way. Nesting moves the count: the
/// pino-http record's four boxed `Value::Map`s add four on a narrower event. Ranked by allocations,
/// these shapes fall in a different order than ranked by bytes moved.
///
/// The collectd event's one allocation is `MetricList`'s spill past its one inline slot, not its
/// six inline attributes (17.4% of collectd events, `docs/design/data-shapes.md` §3, the only
/// measured `MetricList` spill). The span's is its map, with no `events`/`links` `Vec`s: the
/// complement of [`clone_span_event`].
#[test]
fn clone_survey_shapes() {
    expect_clone_allocs(
        "clone: 12-attribute flat log",
        &parsed_survey_event(fixtures::flat_json_log_event),
        1,
    );
    expect_clone_allocs(
        "clone: nested pino-http log",
        &parsed_survey_event(fixtures::pino_http_log_event),
        5,
    );
    expect_clone_allocs(
        "clone: 30-attribute access log",
        &parsed_survey_event(fixtures::access_log_event),
        1,
    );
    expect_clone_allocs("clone: 17-attribute server span", &fixtures::wide_server_span_event(), 1);
    expect_clone_allocs(
        "clone: 3-record collectd event",
        &fixtures::collectd_three_record_event(),
        1,
    );
}

/// The batch-level pair: a five-event OpenTelemetry batch sharing a 17-attribute `Resource`
/// (`docs/design/data-shapes.md` §3's median batch carrying §4's median resource).
///
/// Six allocations, none of them the resource: the batch's `Vec<Event>` plus the five events'
/// maps. The median OpenTelemetry log record carries 9 attributes, one past inline capacity, so
/// every event spills by one slot. The 17-attribute resource, always spilled, is `Arc`-shared and
/// costs a refcount bump. The widest map is nearly free; the one-slot margin is what each event
/// pays.
///
/// Pinned on two code paths to the same number: `EventBatch::clone` directly, and `unwrap_batch`
/// falling back to it on a contended `Delivered::Shared` (a two-mutating-consumer fan-out).
#[test]
fn clone_enriched_resource_batch() {
    let batch = fixtures::enriched_resource_batch();
    assert_eq!(batch.events.len(), 5);
    assert_eq!(batch.resource.attributes.len(), 17);
    assert_eq!(batch.events[0].attributes.len(), 9);
    drop(batch.clone());

    let (cloned, stats) = measure(|| batch.clone());
    assert_eq!(cloned.events.len(), 5);
    expect_allocs("clone: 5-event batch, 17-attr resource", stats, 6);

    let shared = Arc::new(fixtures::enriched_resource_batch());
    let contend = || {
        let _keep_alive = Arc::clone(&shared);
        unwrap_batch(Delivered::Shared(Arc::clone(&shared), BatchContext::default()))
    };
    drop(contend());
    let (unwrapped, stats) = measure(contend);
    assert_eq!(unwrapped.events.len(), 5);
    expect_allocs("unwrap_batch: contended 5-event batch", stats, 6);
}

/// Encodes a one-event batch of `event` through `logit_proto::native` and decodes it back,
/// asserting both halves' allocation counts and that the round trip preserved the attribute width.
#[track_caller]
fn expect_native_round_trip_allocs(
    label: &str,
    event: logit_core::Event,
    encode_allocs: u64,
    decode_allocs: u64,
) {
    let attributes = event.attributes.len();
    let batch = EventBatch { resource: fixtures::resource(), scope: None, events: vec![event] };

    let mut encoder = logit_proto::native::NativeEncoder::default();
    drop(encoder.encode(&batch));
    let (framed, stats) = measure(|| encoder.encode(&batch).expect("should encode"));
    expect_allocs(&format!("native: encode {label}"), stats, encode_allocs);

    let mut decoder = logit_proto::native::NativeDecoder;
    let mut warm = Vec::new();
    drop(decoder.decode_into(framed.clone(), 0, &mut warm));

    let mut events = Vec::new();
    let (_, stats) =
        measure(|| decoder.decode_into(framed.clone(), 0, &mut events).expect("should decode"));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].attributes.len(), attributes, "{label}: round trip changed the width");
    expect_allocs(&format!("native: decode {label}"), stats, decode_allocs);
}

/// A native round trip per survey shape, encode and decode pinned separately. Encode grows with
/// fields written; decode is flat in width. `read_attr_map_at` reads the exact attribute count off
/// the wire and discards it, because `AttrMap` has no `with_capacity`, so a 30-attribute map is
/// rebuilt by 30 sorted inserts that spill and then realloc twice. That growth shows in reallocs,
/// which this test doesn't assert.
///
/// | Shape | encode | decode |
/// |---|--:|--:|
/// | 12-attribute flat JSON log | 16 | 5 |
/// | pino-http nested record | 24 | 9 |
/// | 30-attribute access log | 24 | 5 |
/// | 17-attribute server span | 20 | 5 |
/// | 3-record collectd event | 20 | 5 |
///
/// Decode is 5 for every flat shape (one `Vec<Event>`, one spill, and the frame's buffers) and
/// 9 for the nested one (four boxed `Value::Map`s). Encode tracks fields rather than attributes:
/// the span and the three-record collectd event write more structure than the 12-attribute log.
#[test]
fn native_round_trip_survey_shapes() {
    expect_native_round_trip_allocs(
        "12-attribute flat log",
        parsed_survey_event(fixtures::flat_json_log_event),
        16,
        5,
    );
    expect_native_round_trip_allocs(
        "nested pino-http log",
        parsed_survey_event(fixtures::pino_http_log_event),
        24,
        9,
    );
    expect_native_round_trip_allocs(
        "30-attribute access log",
        parsed_survey_event(fixtures::access_log_event),
        24,
        5,
    );
    expect_native_round_trip_allocs(
        "17-attribute server span",
        fixtures::wide_server_span_event(),
        20,
        5,
    );
    expect_native_round_trip_allocs(
        "3-record collectd event",
        fixtures::collectd_three_record_event(),
        20,
        5,
    );
}

/// The batch round trip, where the 17-attribute `Resource` goes on the wire once per batch: the
/// one measurement in this section that pays for the resource's width.
#[test]
fn native_round_trip_enriched_resource_batch() {
    let batch = fixtures::enriched_resource_batch();
    let mut encoder = logit_proto::native::NativeEncoder::default();
    drop(encoder.encode(&batch));
    let (framed, stats) = measure(|| encoder.encode(&batch).expect("should encode"));
    expect_allocs("native: encode 5-event batch, 17-attr resource", stats, 53);

    let mut decoder = logit_proto::native::NativeDecoder;
    let mut warm = Vec::new();
    drop(decoder.decode_into(framed.clone(), 0, &mut warm));

    let mut events = Vec::new();
    let (_, stats) =
        measure(|| decoder.decode_into(framed.clone(), 0, &mut events).expect("should decode"));
    assert_eq!(events.len(), 5);
    assert_eq!(events[0].attributes.len(), 9);
    expect_allocs("native: decode 5-event batch, 17-attr resource", stats, 10);
}

// ---------------------------------------------------------------------------------------------
// Native wire format (logit_proto::native)
// ---------------------------------------------------------------------------------------------

/// `NativeEncoder::encode` on one nginx event, warm: 30.
#[test]
fn native_encode_one_event() {
    let batch = fixtures::nginx_batch(1);
    let mut encoder = logit_proto::native::NativeEncoder::default();
    drop(encoder.encode(&batch));

    let (framed, stats) = measure(|| encoder.encode(&batch).expect("should encode"));
    assert!(!framed.is_empty());
    // Among the 30: `write_field`'s temp buffers, 2 per `MetricRecord` (`write_record_list`'s
    // per-entry length prefix, which keeps an unrecognized field from desyncing the list, and the
    // wrapped `MetricRecord.kind`) plus 1 for `LogRecord.message`: 4*2 + 1 = 9 for this event's 4
    // metrics and 1 log. The two `Samples` are written from their values; a `Distribution` would
    // add one `to_java_bytes` blob each.
    expect_allocs("native: encode 1 event", stats, 30);
}

/// The decode-side mirror of [`native_encode_one_event`]: 8. `decode_into` appends into a
/// caller-held `Vec<Event>`, as every other decoder here does.
#[test]
fn native_decode_one_event() {
    let batch = fixtures::nginx_batch(1);
    let mut encoder = logit_proto::native::NativeEncoder::default();
    let framed = encoder.encode(&batch).expect("should encode");
    let mut decoder = logit_proto::native::NativeDecoder;
    let mut warm_events = Vec::new();
    drop(decoder.decode_into(framed.clone(), 0, &mut warm_events));

    let mut events = Vec::new();
    let (_, stats) =
        measure(|| decoder.decode_into(framed.clone(), 0, &mut events).expect("should decode"));
    assert_eq!(events.len(), 1);
    // `FIELD_METRICS` decodes through `read_record_list_into` straight into the event's
    // `MetricList` (one `reserve`, then a push per record): one allocation for the list. Collecting
    // an intermediate `Vec<MetricRecord>` into the `SmallVec` would cost a second, since
    // `SmallVec`'s `FromIterator` can't reuse the donor `Vec`'s buffer.
    expect_allocs("native: decode 1 event", stats, 8);
}

/// `logit_out`'s encode+frame step through the primitives it calls (`native::encode_batch`, then
/// `frame::write_frame_with_flags`) rather than `NativeEncoder`: 30, the same two steps as
/// [`native_encode_one_event`], pinned separately so a change to either path shows.
#[test]
fn logit_out_encode_and_frame_one_batch() {
    let batch = fixtures::nginx_batch(1);
    let warm_payload = logit_proto::native::encode_batch(&batch);
    drop(logit_proto::frame::write_frame_with_flags(
        logit_proto::native::CODEC_NATIVE_V1,
        logit_proto::frame::Compression::None,
        0,
        &warm_payload,
    ));

    let (framed, stats) = measure(|| {
        let payload = logit_proto::native::encode_batch(&batch);
        logit_proto::frame::write_frame_with_flags(
            logit_proto::native::CODEC_NATIVE_V1,
            logit_proto::frame::Compression::None,
            0,
            &payload,
        )
        .expect("should frame")
    });
    assert!(!framed.is_empty());
    // Same `native::encode_batch` as `native_encode_one_event`, so the same breakdown.
    expect_allocs("logit_out: encode + frame 1 batch", stats, 30);
}

/// `logit_in`'s read+decode step through the primitives its connection loop calls on a buffered
/// frame (`frame::read_frame_with_header`, then `native::decode_batch`): 7, one less than
/// [`native_decode_one_event`]. `NativeDecoder::decode_into` also extends a caller-held `Vec`;
/// `decode_batch` returns an owned `EventBatch` that `Fanout::send` takes as-is.
#[test]
fn logit_in_read_and_decode_one_batch() {
    let batch = fixtures::nginx_batch(1);
    let mut encoder = logit_proto::native::NativeEncoder::default();
    let framed = encoder.encode(&batch).expect("should encode");

    let mut warm = framed.clone();
    let (_, mut warm_payload) = logit_proto::frame::read_frame_with_header(&mut warm).unwrap();
    drop(logit_proto::native::decode_batch(&mut warm_payload));

    let (event_count, stats) = measure(|| {
        let mut bytes = framed.clone();
        let (_, mut payload) =
            logit_proto::frame::read_frame_with_header(&mut bytes).expect("should read frame");
        logit_proto::native::decode_batch(&mut payload).expect("should decode").events.len()
    });
    assert_eq!(event_count, 1);
    // Same `native::decode_batch` as `native_decode_one_event`, including its one-allocation
    // `MetricList` decode.
    expect_allocs("logit_in: read + decode 1 batch", stats, 7);
}

/// Pins `Dict::read`'s `Vec::with_capacity(count.min(4096))` clamp by bytes, which `dict.rs`'s
/// unit tests can't: a rejected count fails the same whether or not the allocation was clamped. A
/// declared count of 1,000,000 with no entries makes `Dict::read` allocate its `Vec`, then fail on
/// the first entry. Clamped, that's ~4096 4-byte `Symbol`s, under 20 KB; unclamped, ~3.8 MB.
#[test]
fn native_dict_read_clamps_its_capacity_to_a_count_far_larger_than_4096() {
    use logit_proto::native::dict::Dict;
    use logit_proto::native::varint::write_uvarint;

    let mut buf = bytes::BytesMut::new();
    write_uvarint(&mut buf, 1_000_000);
    let declared = buf.freeze();

    let (result, stats) = measure(|| Dict::read(&mut declared.clone()));
    assert!(result.is_err(), "a count with nothing behind it should still fail to decode");
    assert!(
        stats.bytes < 100_000,
        "Dict::read allocated {} bytes for a declared count of 1,000,000 -- \
         the with_capacity(count.min(4096)) clamp appears to be gone (unclamped would be ~3.8 MB)",
        stats.bytes
    );
}
