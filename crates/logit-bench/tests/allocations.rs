//! Exact allocation counts for each stage of the reference nginx pipeline.
//!
//! These are the numbers `docs/design/memory.md` quotes. They're assertions rather than a report
//! so that an allocation regression fails a build: `script/test` runs them in normal CI, and
//! `cargo nextest`'s process-per-test isolation is what makes the counts reproducible rather than
//! order-dependent.
//!
//! **When one of these fails, read the printed actual/expected line before changing the constant.**
//! A drop is worth keeping (update the constant and the doc's table together); a rise is the thing
//! this file exists to catch.
//!
//! Two things every measurement here does deliberately:
//!
//! - **Warms its subject first**, because plenty of things allocate exactly once on first use (the
//!   `OnceLock` interner, a `HashMap`'s first table). See [`logit_bench::alloc::measure`].
//! - **Counts only `alloc`, not `realloc`.** Reallocation is reported alongside but asserted
//!   separately where it's interesting: it means a container was grown, i.e. a missing
//!   `with_capacity`, which is a different (and usually cheaper) problem than a missing reuse.

use logit_bench::alloc::{measure, CountingAlloc, Stats};
use logit_bench::fixtures;
use logit_core::Value;
use logit_outputs::influxdb::InfluxLineEncoder;
use logit_outputs::stdio::{EventDump, Format};
use logit_pipeline::Transform;
use logit_proto::{Decoder, Encoder};
use logit_script::{ProcessOutcome, ScriptWorker};

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

/// One allocation: the `Vec<Event>` the batch is collected into. Every *field* of the event --
/// message, tag, hostname, timestamp -- is a refcounted slice of the datagram `Bytes` that was
/// passed in, so the decode itself adds nothing per field. This is the zero-copy design in
/// `docs/design/data-model.md` working exactly as intended, and it's the bar the statsd decoder
/// below does not currently clear.
///
/// Note what's excluded: `SyslogInput::run` does one `Bytes::copy_from_slice` per datagram before
/// calling this, which this measurement deliberately doesn't cover -- see
/// [`datagram_copy_is_one_right_sized_allocation`].
#[test]
fn syslog_decode_one_line() {
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::nginx_syslog_datagram(1);
    drop(decoder.decode(datagram.clone())); // warm: interns every syslog.* key exactly once

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    expect_allocs("syslog_in: decode 1 line", stats, 1);
}

/// Still one allocation for 100 lines -- but five *reallocations*, because `decode` collects into
/// a `Vec::new()` and grows it 4 -> 8 -> ... -> 128. A `with_capacity` hint (lines can be counted
/// with one `memchr` pass) would remove those; it's the cheapest item on
/// `docs/design/memory.md`'s list and the least valuable, which is why it's recorded rather than
/// done.
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

/// Eight allocations for one line, against syslog's one -- and the gap is entirely
/// tag handling. `statsd.rs` builds attribute values with `attributes.insert(k, v)` on a `&str`,
/// which goes through `impl From<&str> for Value` -> `Value::str` -> `Bytes::from(String)`: a
/// fresh copy of bytes that are already sitting in the datagram buffer. Then `build_event`'s
/// `attributes.clone()` promotes each of those to a shared `Bytes`, allocating a second time.
///
/// Three tags, two allocations each, plus one `Vec<Event>` per line and one for the batch. See
/// [`statsd_tag_values_are_copied_not_sliced`] for the same finding stated structurally.
#[test]
fn statsd_decode_one_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    expect_allocs("statsd_in: decode 1 line", stats, 8);
}

// ---------------------------------------------------------------------------------------------
// Transforms
// ---------------------------------------------------------------------------------------------

/// `json.rs`'s `ValueSeed` deserializes straight into `Value` -- no intermediate
/// `serde_json::Value` tree -- and keeps unescaped strings as zero-copy slices of the message
/// buffer. What's left is the intermediate `AttrMap` it builds (so a malformed object can't leave
/// attributes half-populated) plus the merge into the event, which pushes the attribute count from
/// 4 to 10 and spills `AttrMap` off its 8-entry inline capacity.
#[test]
fn json_parse_one_event() {
    let mut json = fixtures::json_parser();
    let resource = fixtures::resource();
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::nginx_syslog_datagram(1);
    let mut decode_one = || {
        decoder.decode(datagram.clone()).expect("should decode").events.pop().expect("one event")
    };

    let warm = decode_one();
    drop(json.process(&resource, warm));

    let event = decode_one();
    let (event, stats) = measure(|| json.process(&resource, event).expect("json forwards"));
    assert_eq!(event.attributes.len(), 10, "6 JSON fields plus 4 syslog.* attributes");
    expect_allocs("json: parse + merge 1 event", stats, 7);
}

/// Four metrics attached: one `MetricList` spill (past its single inline slot) and one `bins` Vec
/// for each of the two single-sample `DDSketch` distributions. That is the cost of describing two
/// `f64`s -- see `docs/design/memory.md` on `MetricKind::Distribution`.
#[test]
fn kv_metrics_one_event() {
    let mut kv = fixtures::kv_metrics();
    let resource = fixtures::resource();
    drop(kv.process(&resource, fixtures::nginx_event()));

    let event = {
        // `nginx_event` already ran kv_metrics; rebuild the pre-kv_metrics shape by hand.
        let mut decoder = fixtures::syslog_decoder();
        let mut json = fixtures::json_parser();
        let batch = decoder.decode(fixtures::nginx_syslog_datagram(1)).expect("should decode");
        let e = batch.events.into_iter().next().expect("one event");
        json.process(&resource, e).expect("json forwards")
    };

    let (event, stats) = measure(|| kv.process(&resource, event).expect("kv_metrics forwards"));
    assert_eq!(event.metrics.len(), 4);
    expect_allocs("kv_metrics: derive 4 metrics", stats, 3);
}

/// Free, in allocation terms: `filtered` rebuilds the map, but three surviving attributes fit
/// inside `AttrMap`'s inline capacity, so nothing reaches the heap. The `resolve` -> `intern`
/// round trip it does per key is real CPU cost that this measurement can't see.
#[test]
fn keep_one_event() {
    let mut keep = fixtures::keep();
    let resource = fixtures::resource();
    drop(keep.process(&resource, fixtures::nginx_event()));

    let event = fixtures::nginx_event();
    let (event, stats) = measure(|| keep.process(&resource, event).expect("keep forwards"));
    assert_eq!(event.attributes.len(), 3);
    expect_allocs("keep: filter to 3 attributes", stats, 0);
}

/// Also free in steady state, and that is *because* `keep` ran first. `aggregate` clones the whole
/// attribute map into a `SeriesKey` per metric per event, but three attributes fit inline, so the
/// clone is a 400-byte memcpy rather than a heap allocation, and the `HashMap` entry hits.
///
/// Compare [`aggregate_absorb_without_keep`], which is the same code path on an un-trimmed event.
#[test]
fn aggregate_absorb_one_event() {
    let mut agg = fixtures::aggregator();
    let resource = fixtures::resource();
    let mut keep = fixtures::keep();
    let mut trimmed = || keep.process(&resource, fixtures::nginx_event()).expect("keep forwards");

    for _ in 0..4 {
        drop(agg.process(&resource, trimmed()));
    }

    let event = trimmed();
    let (_, stats) = measure(|| agg.process(&resource, event));
    expect_allocs("aggregate: absorb 1 event (after keep)", stats, 0);
}

/// The measurement behind `logit_transforms::keep`'s "put `keep` before `aggregate`" advice, and
/// behind the reference config's ordering. Same events, same aggregator, `keep` removed: the
/// 10-attribute map no longer fits inline, so `SeriesKey`'s clone becomes a heap allocation *per
/// metric per event*.
///
/// The cardinality half of that advice is worse still and isn't visible here: `syslog.timestamp`
/// is distinct per line, so without `keep` every event would also open its own series and the
/// window would grow without bound. This test pins only the per-event allocation half.
#[test]
fn aggregate_absorb_without_keep() {
    let mut agg = fixtures::aggregator();
    let resource = fixtures::resource();
    for _ in 0..4 {
        drop(agg.process(&resource, fixtures::nginx_event()));
    }

    let event = fixtures::nginx_event();
    let (_, stats) = measure(|| agg.process(&resource, event));
    expect_allocs("aggregate: absorb 1 event (no keep)", stats, 4);
}

#[test]
fn aggregate_flush_100_series() {
    let resource = fixtures::resource();
    let mut keep = fixtures::keep();
    let mut agg = fixtures::aggregator();
    for _ in 0..100 {
        let event = keep.process(&resource, fixtures::nginx_event()).expect("keep forwards");
        drop(agg.process(&resource, event));
    }

    let (flushed, stats) = measure(|| agg.flush(1_000_000_000));
    let series: usize = flushed.iter().map(|(_, events)| events.len()).sum();
    assert_eq!(series, 4, "one series per metric name -- keep bounds the tag set");
    expect_allocs("aggregate: flush 4 series", stats, 2);
}

// ---------------------------------------------------------------------------------------------
// Fan-out
// ---------------------------------------------------------------------------------------------

/// What each extra fan-out consumer costs per event: `Fanout::send` deep-clones the batch for
/// every consumer but the last. Four allocations (the spilled `AttrMap`, the spilled `MetricList`,
/// and a `bins` Vec per sketch) plus a 792-byte memcpy, per event, per extra branch.
///
/// The `Arc<EventBatch>` copy-on-write change in `docs/design/memory.md` is aimed at exactly this:
/// a branch that only reads -- every sink -- would pay none of it.
#[test]
fn clone_one_event() {
    let event = fixtures::nginx_event();
    drop(event.clone());

    let (clone, stats) = measure(|| event.clone());
    assert_eq!(clone.metrics.len(), 4);
    expect_allocs("Event::clone (nginx shape)", stats, 4);
}

/// The cheap end of the range: a statsd counter with three tags and one metric fits entirely
/// within `Event`'s inline capacity, so cloning it is a pure 792-byte memcpy. Same 792 bytes as
/// the nginx event above -- that size is paid unconditionally, whatever the event carries.
#[test]
fn clone_one_statsd_event() {
    let event = fixtures::statsd_event();
    drop(event.clone());

    let (_, stats) = measure(|| event.clone());
    expect_allocs("Event::clone (statsd shape)", stats, 0);
}

// ---------------------------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------------------------

/// **The dominant cost in the whole pipeline**: ~180 allocations per event, against 11 to ingest
/// one. Each of the event's four metrics becomes a line costing ~45 allocations, from
/// `escape_tag`/`escape_measurement` building four intermediate `String`s apiece via chained
/// `.replace()` (whether or not anything needs escaping), a `Vec<(String, String)>` of fields with
/// a `format!` per name and a `to_string()` per value, and a `line.clone()` used as a `HashMap`
/// key on every call rather than only on insert.
///
/// This is where an optimization pass should start, and none of it needs any change to the event
/// model -- see `docs/design/memory.md`'s recommendations.
#[test]
fn influx_encode_100_events() {
    let mut encoder = InfluxLineEncoder::default();
    let batch = fixtures::nginx_batch(100);
    drop(encoder.encode(&batch));

    let (body, stats) = measure(|| encoder.encode(&batch).expect("should encode"));
    assert!(!body.is_empty());
    expect_allocs("influxdb_out: encode 100 events", stats, 18024);
}

/// ~18 allocations per event -- an order of magnitude better than the InfluxDB encoder, mostly
/// because it appends into one shared buffer instead of building per-line `String`s. Still a debug
/// sink, so this matters less; recorded for contrast.
#[test]
fn stdio_encode_100_events() {
    let dump = EventDump::new(Format::Human);
    let batch = fixtures::nginx_batch(100);
    drop(dump.encode(&batch));

    let (text, stats) = measure(|| dump.encode(&batch));
    assert!(!text.is_empty());
    expect_allocs("stdio_out: encode 100 events", stats, 1801);
}

// ---------------------------------------------------------------------------------------------
// Lua
// ---------------------------------------------------------------------------------------------

/// One round trip across the Rust/Lua boundary for a script that reads one attribute and writes
/// one: an `Rc<RefCell<Event>>` and an mlua userdata per event, a *fresh* `AttrsProxy` userdata
/// per `event.attributes` access, a Rust `String` per metamethod key, a `_G` lookup of `process`
/// per event, and a `Box` on the way out.
///
/// This is the number `docs/known-gaps.md` has been carrying as an unbenchmarked assumption. It
/// does not invalidate the proxy design -- see `lua::to_table` in `benches/pipeline.rs` for the
/// comparison against the alternative -- but it does show the boundary is not free.
#[test]
fn lua_process_one_event() {
    let worker = ScriptWorker::new(fixtures::LUA_ENRICH_SCRIPT).expect("script should load");
    drop(worker.process(fixtures::nginx_event()));

    let event = fixtures::nginx_event();
    let (outcome, stats) = measure(|| worker.process(event).expect("script should run"));
    assert!(matches!(outcome, ProcessOutcome::Emit(_)));
    expect_allocs("lua: process 1 event", stats, 21);
}

// ---------------------------------------------------------------------------------------------
// End to end
// ---------------------------------------------------------------------------------------------

/// Decode through aggregation for one access-log line -- the number that bounds ingest throughput
/// for the reference config. Excludes the output encoders, which run once per flush window rather
/// than once per event, and excludes fan-out, which the config's `tap` branch adds.
///
/// 11 = 1 (decode) + 7 (json) + 3 (kv_metrics) + 0 (keep) + 0 (aggregate).
#[test]
fn full_chain_one_line() {
    let resource = fixtures::resource();
    let mut decoder = fixtures::syslog_decoder();
    let mut json = fixtures::json_parser();
    let mut kv = fixtures::kv_metrics();
    let mut keep = fixtures::keep();
    let mut agg = fixtures::aggregator();
    let datagram = fixtures::nginx_syslog_datagram(1);

    macro_rules! run {
        () => {{
            let batch = decoder.decode(datagram.clone()).expect("should decode");
            for event in batch.events {
                let event = json.process(&resource, event).expect("json forwards");
                let event = kv.process(&resource, event).expect("kv forwards");
                let event = keep.process(&resource, event).expect("keep forwards");
                drop(agg.process(&resource, event));
            }
        }};
    }

    for _ in 0..4 {
        run!();
    }
    let (_, stats) = measure(|| run!());
    expect_allocs("full chain: 1 access-log line", stats, 11);
}

// ---------------------------------------------------------------------------------------------
// Structural guards on claims docs/design/memory.md makes
// ---------------------------------------------------------------------------------------------

/// The zero-copy claim, stated structurally rather than as a count: every field a syslog line
/// yields points *into* the datagram buffer it was decoded from. If this stops holding, the
/// "`bytes::Bytes` everywhere" story in `docs/design/data-model.md` is no longer true of the one
/// input that best exemplifies it.
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

/// The counterexample, pinned so it can't be quietly assumed away: statsd tag values are *copied*
/// out of the datagram, not sliced. This is the structural cause of
/// [`statsd_decode_one_line`]'s eight allocations, and the fix is to give `statsd.rs` the
/// `slice_of` treatment `syslog.rs` already has.
///
/// Asserted as a currently-true fact about the code, not as desirable behavior -- when someone
/// fixes it, this test should start failing and be replaced by the positive assertion above.
#[test]
fn statsd_tag_values_are_copied_not_sliced() {
    let datagram = fixtures::statsd_datagram(1);
    let mut decoder = fixtures::statsd_decoder();
    let batch = decoder.decode(datagram.clone()).expect("should decode");
    let event = batch.events.into_iter().next().expect("one event");

    let env = event.attributes.get("env").expect("the env tag");
    let Value::Str(env) = env else { panic!("the tag value should be a Str") };
    assert_eq!(env.as_ref(), b"prod");
    assert!(
        !points_into(&datagram, env),
        "this documents a known gap: if statsd tag values now slice the datagram, that's an \
         improvement -- flip this to `points_into` and update docs/design/memory.md"
    );
}

/// `StatsdInput::run`/`SyslogInput::run` copy each datagram out of the reusable 64 KB receive
/// buffer with `Bytes::copy_from_slice`, which is one allocation sized to the datagram, not to the
/// buffer. That right-sizing is the point: reading straight into a large shared `BytesMut` would
/// save this allocation but let one retained log line pin 64 KB. Guarded here so the "considered
/// and rejected" note in `docs/design/memory.md` has something holding it up.
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

/// The property that makes interning the right call for a bounded key space, and the reason
/// `AttrMap::get`'s interning is a CPU problem rather than a memory one: re-interning a string the
/// table already holds allocates **nothing**. A pipeline whose keys and metric names come from a
/// fixed schema reaches steady state and stays there.
///
/// The corollary is what `docs/design/memory.md`'s interner section is about: the table only grows
/// for a *distinct* string, so the exposure is names that are real but never repeat -- not lookups
/// that miss.
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
