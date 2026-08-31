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

/// Two allocations for one line, down from eight: `statsd.rs`'s tag values are now sliced out of
/// the datagram with the same `slice_of` pointer-arithmetic `syslog.rs` uses, instead of going
/// through `impl From<&str> for Value` -> `Bytes::from(String)` (a fresh copy) and then a second
/// copy when `build_event`'s `attributes.clone()` promoted it to a shared `Bytes`.
///
/// What's left, unlike syslog's single `Vec<Event>`: statsd's multi-value grammar
/// (`name:1:2:3|c`) means `parse_line` collects one line's events into its own `Vec` before
/// `decode` appends them into the batch's `Vec`, so this is one allocation per line plus one for
/// the batch rather than syslog's one total. See
/// [`statsd_tag_values_share_the_datagram_allocation`] for the same finding stated structurally.
#[test]
fn statsd_decode_one_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    expect_allocs("statsd_in: decode 1 line", stats, 2);
}

/// The logs-only workload `docs/design/memory.md` §0 names as unmeasured: a plain-text syslog
/// line with no JSON body anywhere in the pipeline (`fixtures::SSHD_SYSLOG_LINE`). Same zero-copy
/// decode as [`syslog_decode_one_line`] -- one allocation for the `Vec<Event>`, nothing per
/// field, for the same reason. What's new is the attribute count: six
/// (`syslog.facility`/`severity`/`timestamp`/`hostname`/`tag`/`pid`), the top of the "4-6
/// attributes" range §1 estimates for this workload -- comfortably inside `AttrMap`'s 8-slot
/// inline capacity, unlike the nginx shape (10 attributes) which spills. That's the concrete data
/// point `docs/design/memory.md`'s item 9 (re-picking `AttrMap`'s inline capacity) was missing
/// for a plain-syslog pipeline specifically.
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

// ---------------------------------------------------------------------------------------------
// Transforms
// ---------------------------------------------------------------------------------------------

/// `json.rs`'s `ValueSeed` deserializes straight into `Value` -- no intermediate
/// `serde_json::Value` tree -- and keeps unescaped strings as zero-copy slices of the message
/// buffer. The intermediate is still there -- a malformed object can't leave attributes
/// half-populated, so the parsed pairs are built up separately from `event.attributes` and only
/// merged in on full success -- but it's now a scratch `Vec<(Symbol, Value)>` held on `JsonParser`
/// and cleared per call (mirroring `InfluxLineEncoder`'s reused buffers) rather than a fresh
/// `AttrMap` from `deserialize`, and object keys are interned straight off the deserializer
/// (`KeySeed`) instead of passing through an owned `String` first. That was where 6 of the
/// original 7 allocations were -- one per JSON key -- not the intermediate map itself, which fits
/// `AttrMap`'s 8-entry inline capacity for this fixture's 6 fields regardless. What's left is the
/// one allocation from `event.attributes` itself spilling its inline capacity once the merge pushes
/// the count from 4 to 10.
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
    expect_allocs("json: parse + merge 1 event", stats, 1);
}

/// The wide-JSON workload `docs/design/memory.md` §0 names as unmeasured: 28 flat top-level
/// fields (`fixtures::WIDE_JSON_SYSLOG_LINE`, modeled on pino's default output shape) against the
/// nginx fixture's 6. This corrects a guess `docs/design/memory.md`'s item 5 made from the nginx
/// number alone: `json`'s allocation cost is **not** dominated by the two `AttrMap`
/// builds/spills (the intermediate map and the merge) -- it's dominated by
/// `collect_attrmap`'s `map.next_key::<String>()?`, which allocates one `String` per JSON object
/// key regardless of value type. 30 allocations for 28 keys is 28 key `String`s plus exactly the
/// same two spills [`json_parse_one_event`] pays (one on the intermediate `AttrMap` past 8
/// entries, one on the merge into the event's, which starts at 4 and passes 8 on the fifth JSON
/// key) -- so unlike the nginx measurement suggested, this cost scales close to linearly with
/// field count, and a checkpoint-and-rollback fix (item 5's proposal) would leave the dominant
/// term untouched; a non-allocating key lookup (closer to what `AttrMap::get` needs per item 3)
/// would matter more here than the intermediate-map rework would.
#[test]
fn json_parse_wide_json_event() {
    let mut json = fixtures::json_parser();
    let resource = fixtures::resource();
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::wide_json_syslog_datagram(1);
    let mut decode_one = || {
        decoder.decode(datagram.clone()).expect("should decode").events.pop().expect("one event")
    };

    let warm = decode_one();
    drop(json.process(&resource, warm));

    let event = decode_one();
    let (event, stats) = measure(|| json.process(&resource, event).expect("json forwards"));
    assert_eq!(event.attributes.len(), 32, "28 JSON fields plus 4 syslog.* attributes");
    expect_allocs("json: parse + merge 1 wide-JSON event", stats, 30);
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

/// Cloning [`fixtures::distribution_heavy_event`] -- five *distinct* `MetricKind::Distribution`
/// metrics, against the nginx shape's two. `clone_one_event` above costs 4 allocations for 2
/// distributions (a spilled `AttrMap`, a spilled `MetricList`, and a `bins` Vec per sketch); this
/// fixture's three attributes stay inline (no `AttrMap` spill), so the difference isolates what
/// distribution *count* costs on its own: one `MetricList` spill (past its single inline slot)
/// plus one `bins` Vec per sketch. This is exactly the number `docs/design/memory.md`'s item 8
/// (`Box` the `DdSketch`) was missing -- boxing turns every one of these `bins`-Vec clones into an
/// *additional* allocation, which is the "distribution-heavy metrics" side of that trade the doc
/// flags as unmeasured.
#[test]
fn clone_distribution_heavy_event() {
    let event = fixtures::distribution_heavy_event();
    drop(event.clone());

    let (clone, stats) = measure(|| event.clone());
    assert_eq!(clone.metrics.len(), 5);
    expect_allocs("Event::clone (distribution-heavy shape)", stats, 6);
}

/// Cloning [`fixtures::span_event`] -- the one payload shape with no coverage at all before this
/// change. `SpanRecord` holds a `Vec<SpanEvent>` (2 entries here) and a `Vec<SpanLink>` (1 entry),
/// each a heap allocation on clone regardless of contents -- that's the 2 allocations measured.
/// What's notably *not* here: every `AttrMap` involved (the event's own 4 attributes, each
/// `SpanEvent`'s 2, the link's 1) is small enough to stay inside `AttrMap`'s 8-slot inline
/// capacity, so none of them spills to the heap on clone.
///
/// **This is cheaper to clone than the nginx shape, not more expensive** -- 2 allocations (1320
/// bytes) against `clone_one_event`'s 4 (3552 bytes). That's the opposite of what a first read of
/// `docs/design/memory.md`'s item 4 ("far more expensive to deep-clone than anything measured
/// here") suggests, and the reason is exactly the inline-attribute point above: this fixture is
/// narrow enough on attribute count everywhere that nothing about it spills. It does **not** show
/// spans are cheap in general -- a span whose `SpanEvent`s/`SpanLink`s (or the span itself)
/// carried more than 8 attributes each would spill those maps on clone just as the nginx event's
/// 10 attributes do, costing more accordingly; this measurement only speaks to the narrow shape
/// this fixture actually builds. What does generalize regardless of attribute width is the fixed
/// cost of the two `Vec`s existing at all: `Box`ing `SpanRecord` (item 7) would add exactly one
/// more allocation on top of whatever a given span shape's total turns out to be.
#[test]
fn clone_span_event() {
    let event = fixtures::span_event();
    drop(event.clone());

    let (clone, stats) = measure(|| event.clone());
    assert!(clone.span.is_some());
    expect_allocs("Event::clone (span shape)", stats, 2);
}

// ---------------------------------------------------------------------------------------------
// Outputs
// ---------------------------------------------------------------------------------------------

/// Was the dominant cost in the whole pipeline at ~180 allocations per event -- more than ingest
/// and fan-out combined -- and is now 0.3, after the encoder was reworked to escape and format
/// straight into reused buffers instead of building a `String` per tag, per field name, per field
/// value, and per line. See `docs/design/memory.md`.
///
/// What's left is genuinely per-batch rather than per-event: one `Bytes` for the finished body,
/// one `String` key per *distinct* series on its first sighting, and the growth of the per-series
/// timestamp maps. Nothing here scales with event count any more, which is the property worth
/// keeping -- if this number starts tracking the batch size again, something has regressed to
/// per-line allocation.
#[test]
fn influx_encode_100_events() {
    let mut encoder = InfluxLineEncoder::default();
    let batch = fixtures::nginx_batch(100);
    drop(encoder.encode(&batch));

    let (body, stats) = measure(|| encoder.encode(&batch).expect("should encode"));
    assert!(!body.is_empty());
    expect_allocs("influxdb_out: encode 100 events", stats, 30);
}

/// Down from 1801 (~18/event) to 101 (~1/event), via the same treatment `influxdb_out` got
/// (`docs/design/memory.md`): merge-join the resource and event attribute maps instead of cloning
/// and re-inserting one, and format numbers straight into the output buffer via `write!` instead
/// of a `format!`/`to_string()` per rendered value. The remaining allocation is one
/// `format_rfc3339_utc` call per event (`logit_core::time`, out of this encoder's scope) plus one
/// for the output `String`'s own first growth -- `influxdb_out` still comes out ahead at ~0.3
/// allocations/event, since it also reuses its per-line buffers across events, which this encoder
/// doesn't need (every `render_*` function here already writes straight into the one buffer this
/// returns; see `EventDump::encode`'s doc comment for why there's no equivalent scratch state left
/// to hoist onto the struct).
#[test]
fn stdio_encode_100_events() {
    let dump = EventDump::new(Format::Human);
    let batch = fixtures::nginx_batch(100);
    drop(dump.encode(&batch));

    let (text, stats) = measure(|| dump.encode(&batch));
    assert!(!text.is_empty());
    expect_allocs("stdio_out: encode 100 events", stats, 101);
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
/// 5 = 1 (decode) + 1 (json) + 3 (kv_metrics) + 0 (keep) + 0 (aggregate).
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
    expect_allocs("full chain: 1 access-log line", stats, 5);
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

/// The zero-copy claim for statsd, stated structurally: every DogStatsD tag value points *into*
/// the datagram buffer it was decoded from, same as [`syslog_fields_share_the_datagram_allocation`]
/// above. `statsd.rs`'s `slice_of` is what makes this true -- if it regresses back to copying tag
/// values into a fresh `Bytes`, this is the test that catches it.
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
