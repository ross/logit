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
use logit_core::{EventBatch, Registry, Telemetry, Value};
use logit_outputs::influxdb::InfluxLineEncoder;
use logit_outputs::stdio::{EventDump, Format};
use logit_outputs::syslog::{Format as SyslogFormat, MessageBuf, SyslogEncoder};
use logit_pipeline::runtime::drain_inbox;
use logit_pipeline::{
    process_batch, send_batch, unwrap_batch, Delivered, Fanout, SinkQueue, SinkQueueConfig,
    TraceContext, Transform,
};
use logit_proto::{Decoder, Encoder};
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

/// Pins the unsampled baseline that sample-rate extrapolation on the decode path
/// (`DdSketch::add_weighted`, `crates/logit-core/src/metric.rs`) must add zero allocations over --
/// which its delegation to `sketches_ddsketch::DDSketch::add_with_count` satisfies regardless of
/// weight. [`statsd_decode_one_sampled_distribution_line`] pins the sampled case at the same
/// count.
#[test]
fn statsd_decode_one_distribution_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_distribution_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    expect_allocs("statsd_in: decode 1 distribution line", stats, 3);
}

/// Same line as [`statsd_decode_one_distribution_line`], sampled at `@0.1` -- ten weighted
/// `DdSketch::add_weighted` samples instead of one unweighted `add`. Must match that test's
/// allocation count exactly: the bin `Vec` a `DdSketch` allocates on its first sample is the same
/// single allocation whether that first sample carries a weight of one or ten, because
/// `add_with_count` computes the bin index once and increments its stored count directly.
#[test]
fn statsd_decode_one_sampled_distribution_line() {
    let mut decoder = fixtures::statsd_decoder();
    let datagram = fixtures::statsd_sampled_distribution_datagram(1);
    drop(decoder.decode(datagram.clone()));

    let (batch, stats) = measure(|| decoder.decode(datagram.clone()).expect("should decode"));
    assert_eq!(batch.events.len(), 1);
    match &batch.events[0].metrics[0].kind {
        logit_core::MetricKind::Distribution(sketch) => {
            assert_eq!(sketch.count(), 10, "@0.1 should extrapolate to 10 weighted samples")
        }
        other => panic!("expected Distribution, got {other:?}"),
    }
    expect_allocs("statsd_in: decode 1 sampled distribution line", stats, 3);
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
/// nginx fixture's 6.
///
/// **This test was originally written against the pre-item-5 `json.rs`**, where 28 keys cost 30
/// allocations -- dominated by `collect_attrmap`'s `map.next_key::<String>()?`, which allocated
/// one `String` per JSON object key regardless of value type, scaling close to linearly with
/// field count. Item 5's rework (`crates/logit-transforms/src/json.rs`) replaced exactly that
/// mechanism: keys are now interned straight off the deserializer (`KeySeed`/`KeyVisitor`) instead
/// of collected into an owned `String` first. The result generalizes past the case item 5 was
/// measured against: 28 keys now costs the same **1** allocation [`json_parse_one_event`] measures
/// for 6, confirming the per-key cost is actually gone, not just reduced for a small field count.
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
    expect_allocs("json: parse + merge 1 wide-JSON event", stats, 1);
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

/// 6, not the pre-flush-linking 2 -- re-measured, not assumed, when `Transform::flush` widened to
/// carry each series' `Vec<SpanLink>` (`docs/adr/0020-trace-context-propagation-on-delivered.md`'s
/// flush-side linking). The 4 new allocations are exactly the 4 series: this fixture never calls
/// `observe_batch_context`, so every absorbed event records the same (default, all-zero)
/// `TraceContext`, and each series' `ContributingContexts` ends up with exactly one distinct
/// context -- one `SpanLink` -- which `into_links()`'s `.collect()` allocates its own `Vec<SpanLink>`
/// for. One allocation per series, unavoidable even at the minimum (a non-empty `Vec` always
/// allocates), so this is the honest floor for a flush where every series has any contributor at
/// all.
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
    expect_allocs("aggregate: flush 4 series", stats, 6);
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

/// The number behind PR #33's review item 1: `Fanout::send` now takes a fast path for a
/// single-consumer edge (`docs/adr/0016-arc-eventbatch-copy-on-write.md`) -- no `Arc` at all, the
/// batch moves through as `Delivered::Owned`. This is the common case (every listener's first hop,
/// and every interior edge of a linear chain like the v0.1 reference config's three
/// single-consumer edges), so it needs to cost nothing, not just less than a deep clone.
///
/// Runs on a `current_thread` runtime built outside the measured region -- `CountingAlloc`'s
/// counters are thread-local (see `logit_bench::alloc`'s module doc), so this has to stay on one
/// thread for the count to mean anything, and a `current_thread` runtime never spawns worker
/// threads to begin with. Warmed once first, same as every other measurement in this file.
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

/// The test above proves the *disabled*-telemetry path is free, but `Fanout::send` now opens a
/// span too (`docs/adr/0025-internal-span-emission-and-deterministic-sampling.md`), and a
/// disabled `Telemetry` handle (`Telemetry::default()`, `Option::None` inside) never reaches
/// `Telemetry::span`'s sample-decision branch at all -- it can't stand in for the *live-but-
/// unsampled* path a production pipeline with an `internal` component and the default (0.1, never
/// 0.0 by default, but a rate below `1.0` is the common case) sample rate actually runs.
/// This is that path, proven directly: a real `Registry` (`with_span_sampling(0.0)`, so every
/// trace is dropped deterministically -- see `trace_is_sampled`), attached the same way
/// `crates/logit-cli/src/pipeline.rs::prepare` attaches one in production. `SpanGuard::disabled()`
/// is what `Telemetry::span` returns once `trace_is_sampled` says no, exactly like the
/// `Option::None` case -- same expected count, 0, now actually exercising the branch that decides
/// it rather than skipping it.
#[test]
fn fanout_send_one_consumer_with_a_live_unsampled_registry_costs_nothing() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let registry = logit_core::Registry::with_span_sampling(0.0);
    let telemetry = registry.telemetry_for("in", "statsd_in", "listener");
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let fanout = Fanout::new(vec![tx]).with_telemetry(telemetry);

    // Warms both the usual first-call cost (`CountingAlloc`'s own module doc) and this
    // `ComponentBuffer`'s point map -- `logit.component.batches.sent`/`.events.sent`/
    // `.send.blocked.duration` all need their first-insert growth to happen here, not inside the
    // measured region below, same reasoning as `docs/design/memory.md`'s "first call after a
    // drain" cost: an update to an already-resident map key is free, a fresh insert isn't.
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

/// The other half of the same story, measured honestly rather than assumed: a real fan-out (two
/// consumers here) still costs *one* branch a full `EventBatch` deep clone -- one `Vec<Event>`
/// allocation plus the 4 allocations [`clone_one_event`] measures for the one nginx-shaped event
/// inside it, so 5 -- exactly like the pre-`Arc<EventBatch>` code's "clone all but the last
/// consumer" did for this same two-branch shape. The other branch, once its sibling has already
/// dropped its handle, costs nothing. **The difference from before this PR is `Arc::new`'s one
/// extra allocation, not a reduction** -- so the total here (6) is one *more* than the equivalent
/// pre-`Arc` code would have paid (5), not less. Compare [`fanout_send_one_consumer_costs_nothing`],
/// which really is a strict improvement; this test exists so that claim isn't quietly assumed to
/// extend to real fan-outs too, when the numbers say otherwise under the current no-trait-change
/// design (`docs/adr/0016-arc-eventbatch-copy-on-write.md`'s "What this change actually saves"
/// section). Six is still far short of two fully independent copies (10, i.e. this same 5 paid by
/// *both* branches, which is what a naive per-`Event` `Arc` or a design with no sharing at all would
/// cost), so isolation is not getting more expensive as fan-out width grows -- it just isn't getting
/// cheaper than the code this PR replaces, either.
///
/// Unwraps branch "a" while branch "b" still holds its handle, then "b" last, to pin the
/// deterministic case rather than the timing-dependent one -- `unwrap_batch`'s doc comment
/// (`runtime.rs`) explains why concurrent unwrapping on a real multi-thread runtime can cost *more*
/// than this best case (more than one branch failing to unwrap for free), never less.
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
    // 1 (Arc::new, once per send) + 5 (one EventBatch deep clone: 1 for the Vec<Event>, 4 for the
    // one nginx-shaped Event inside it, matching clone_one_event) + 0 (the other branch, free).
    // The pre-Arc code paid 5 for this same shape (the clone, with the other branch's move costing
    // nothing) -- so this is 1 *more*, not less; see the doc comment above.
    expect_allocs(
        "fanout: send + receive, 2 consumers (1 clones, 1 free, +1 for the Arc)",
        stats,
        6,
    );
}

/// The same unwrap `runtime.rs`'s `unwrap_batch` does, duplicated here rather than exposed from
/// `logit-pipeline` just for this test -- `Delivered`'s two variants and what to do with each are
/// already public (`docs/adr/0016-arc-eventbatch-copy-on-write.md`).
fn unwrap_delivered(delivered: Delivered) -> EventBatch {
    match delivered {
        Delivered::Owned(batch, _ctx) => batch,
        Delivered::Shared(shared, _ctx) => {
            Arc::try_unwrap(shared).unwrap_or_else(|shared| (*shared).clone())
        }
    }
}

/// The same borrow `runtime.rs`'s `run_output` does, now that `Output::send` takes `&EventBatch`
/// instead of an owned one -- no `Arc::try_unwrap`, no clone, ever, regardless of how many sibling
/// branches still hold their own handle to the same batch.
fn borrow_delivered(delivered: &Delivered) -> &EventBatch {
    match delivered {
        Delivered::Owned(batch, _ctx) => batch,
        Delivered::Shared(shared, _ctx) => shared,
    }
}

/// The number the second round of PR #33's review asked for, after `Output::send` was changed to
/// take `&EventBatch`: a fan-out where every consumer is an `Output` (two sinks off one node --
/// `stdio_out`'s `tap` and an `influxdb_out`, say, both reading the same batch) now costs *only*
/// the one `Arc::new` per send. Neither branch ever calls `Arc::try_unwrap` or clones at all --
/// both just borrow through their own `Delivered::Shared` handle, exactly as `run_output` does.
/// This is the saving `docs/design/memory.md` §8 item 4 originally recommended, and the one the
/// first round of this PR's review found `Delivered` alone didn't deliver
/// (`fanout_send_two_consumers_costs_one_clone_plus_one_arc`, above) -- `Output::send(&EventBatch)`
/// is what closes that gap, for this shape.
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
            // Both sides borrow only, exactly as `run_output` does -- never unwrap, never clone.
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

/// Residual item from workstream C (`docs/plans/0004-buffered-sink-delivery.md`): the
/// `fanout_send_*` tests above measure `Fanout::send` alone, stopping short of the actual
/// `run_output`/`drain_inbox` hop a sink's `Delivered` batch takes next
/// (`docs/adr/0021-buffered-sink-delivery.md`). Drives `drain_inbox` directly (not through a full
/// `run`) against a single-consumer `Delivered::Owned` batch: exactly one allocation, the
/// `Arc::new` `drain_inbox` performs to hand the batch to its `SinkQueue`
/// (`crates/logit-pipeline/src/runtime.rs`) -- previously zero on this path, since a
/// single-consumer `Fanout::send` alone never allocates (`fanout_send_one_consumer_costs_nothing`
/// above) and nothing between there and this hop used to exist at all.
#[test]
fn drain_inbox_single_consumer_owned_batch_costs_exactly_the_arc() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let telemetry = logit_core::Telemetry::default();
    let queue = Arc::new(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone()));
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    // Warm: one full push+commit round trip through the exact same `SinkQueue`, so its `VecDeque`
    // backing allocation (and the interner, etc.) is already grown before the measured run --
    // otherwise the queue's first-ever push would fold its own one-time allocation into the
    // `Arc::new` this test means to isolate.
    let warm = fixtures::nginx_batch(1);
    rt.block_on(async {
        tx.send(Delivered::Owned(warm, TraceContext::default()))
            .await
            .expect("send should succeed");
        let warmed = match rx.recv().await.expect("should receive") {
            Delivered::Owned(batch, _ctx) => Arc::new(batch),
            Delivered::Shared(shared, _ctx) => shared,
        };
        queue.push(warmed, TraceContext::default()).await;
        queue.commit();
    });

    let batch = fixtures::nginx_batch(1);
    let queue_for_measure = Arc::clone(&queue);
    let telemetry_for_measure = telemetry.clone();
    let ((), stats) = measure(|| {
        rt.block_on(async move {
            tx.send(Delivered::Owned(batch, TraceContext::default()))
                .await
                .expect("send should succeed");
            drop(tx); // closes the inbox, so `drain_inbox` returns after this one batch
            drain_inbox(&mut rx, queue_for_measure, telemetry_for_measure).await;
        })
    });

    expect_allocs("drain_inbox: single-consumer Delivered::Owned batch (the Arc::new)", stats, 1);
}

/// The actually-common shape (the nginx reference config's `tap` (`stdio_out`)/`trimmed`
/// (`keep` -> `aggregate` -> ...) split): one `Output` branch and one `Transform`/`ScriptWorker`
/// branch off the same fan-out. The `Output` branch costs nothing, as above -- it only ever
/// borrows, and never itself calls `Arc::try_unwrap`. The `Transform` branch still needs an owned
/// `Event` to mutate, so it goes through `unwrap_batch` exactly as before this round of changes.
///
/// **This is one of the two reachable outcomes for this shape, not the only one -- genuinely racy,
/// not deterministic.** `run_output`'s loop (`runtime.rs`) drops its own `Delivered` the moment
/// `output.send` returns, immediately before its next receive -- it does *not* hold the handle for
/// the rest of its loop. So whether the `Transform` branch's unwrap here succeeds for free or falls
/// back to a clone comes down to real tokio scheduling: whichever happens first, `Output`
/// completing its `send` and dropping its handle, or the `Transform` branch's `unwrap_batch` call
/// actually running. This test pins the case where `Transform` runs first (`Output`'s handle still
/// alive) by unwrapping the `Transform` side before the `Output` side is ever touched --
/// [`fanout_send_mixed_output_and_transform_consumers_when_output_finishes_first`] below pins the
/// other reachable outcome, where `Output` finishes first. In production, `Output::send` typically
/// performs real I/O (network, file), which tends to be slower than a `Transform`'s local
/// processing -- so *this* outcome is the likelier one in practice, not the only possible one.
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
            // The Transform branch unwraps first, while the Output branch's handle is still alive
            // -- forcing the Transform branch's clone. The Output branch only ever borrows, then
            // is dropped once this block ends, matching `run_output`'s loop moving to its next
            // receive once `output.send` has returned.
            let xform_batch = unwrap_delivered(xform);
            let out_len = borrow_delivered(&out).events.len();
            (out_len, xform_batch.events.len())
        })
    });
    assert_eq!(out_len, 1);
    assert_eq!(xform_len, 1);
    // 1 (Arc::new, once per send) + 5 (the Transform branch's forced deep clone: 1 for the
    // Vec<Event>, 4 for the one nginx-shaped Event inside it) + 0 (the Output branch, which never
    // unwraps or clones at all). Same total as the all-Transform 2-consumer case measured above --
    // this is the "Output hasn't finished yet" outcome, not the only reachable one; see
    // `fanout_send_mixed_output_and_transform_consumers_when_output_finishes_first` below for the
    // other. Against `main` (pre-PR): a flat, unconditional 5 for any 2-consumer fan-out regardless
    // of kind -- so this specific outcome is 1 allocation worse than `main`, same as the
    // all-Transform case, though (unlike that case) it's not the only place this shape can land.
    expect_allocs(
        "fanout: send + receive, 1 Output + 1 Transform, Output not yet finished (racy outcome A)",
        stats,
        6,
    );
}

/// The *other* reachable outcome for the same mixed shape as
/// [`fanout_send_mixed_output_and_transform_consumers`] above, not a hypothetical: if `Output`'s
/// task happens to complete its `send` and drop its `Delivered` handle *before* the `Transform`
/// branch's `unwrap_batch` call runs -- plausible whenever `Output::send` is fast, local, or simply
/// wins the scheduling race -- `Arc::try_unwrap` finds itself the sole remaining reference and
/// succeeds for free. Total cost collapses to just the one `Arc::new` per send, the same number
/// [`fanout_send_two_output_consumers_costs_only_the_arc`] measures for two `Output` consumers,
/// because at that point neither branch has paid anything beyond the `Arc` itself.
///
/// Simulated by dropping the `Output` side's `Delivered` (standing in for `run_output` having
/// already returned from `output.send` and moved on) *before* the `Transform` side ever calls
/// `unwrap_batch` -- the mirror image of the ordering the test above pins.
///
/// **The two tests together are the honest picture for this shape: 1 or 6, decided by real
/// scheduling, never anything in between** (there's no path to landing on `main`'s flat 5, since
/// `Arc::new` is always paid the moment there are 2+ consumers). Whether this design is a net win,
/// a wash, or a regression for a given deployment depends on how its `Output` implementations and
/// `Transform`/Lua stages actually get scheduled relative to each other -- not something a fixed
/// allocation count can answer on its own.
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
            // The Output branch finishes and drops its handle first -- matching `run_output`
            // having already returned from `output.send` and moved past this batch entirely --
            // *before* the Transform branch ever attempts its unwrap.
            let out_len = borrow_delivered(&out).events.len();
            drop(out);
            let xform_batch = unwrap_delivered(xform);
            (out_len, xform_batch.events.len())
        })
    });
    assert_eq!(out_len, 1);
    assert_eq!(xform_len, 1);
    // 1 (Arc::new, once per send) + 0 (the Transform branch's try_unwrap now succeeds, since the
    // Output branch already dropped its handle) + 0 (the Output branch, as always). Against
    // `main`'s flat, unconditional 5 for this shape, this outcome is a real, substantial
    // improvement -- the other reachable outcome (the test above) is 1 allocation worse than
    // `main`. Which one a given run lands on is decided by scheduling, not by this design.
    expect_allocs(
        "fanout: send + receive, 1 Output + 1 Transform, Output finished first (racy outcome B)",
        stats,
        1,
    );
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
// Runtime
// ---------------------------------------------------------------------------------------------
//
// Everything above this section calls a decoder/transform/encoder directly, or drives `Fanout`
// alone -- never `logit-pipeline::runtime`'s node loops themselves. That's a real gap, not a
// stylistic choice: `run_transform`/`run_output` are private `async fn`s, and PR #37
// (`docs/design/internal-telemetry.md`) added telemetry accounting to both without this file
// gaining any coverage of what that accounting costs. `run_transform`'s per-batch body is now
// exported as `logit_pipeline::process_batch` (a synchronous fn -- no channel hop, no runtime
// needed), and `run_output`'s as `logit_pipeline::send_batch` (async, since `Output::send` is, but
// still callable with no channel via a `current_thread` runtime -- see `fanout_send_one_consumer`
// et al. above for why that's still safe to count allocations across), specifically so both can be
// measured here directly, the same "call the real thing" rule `docs/design/memory.md` §7 already
// applies to every stage above. `unwrap_batch` is exported for the same reason.
//
// **A second, sharper gap this section closes, found in review of the first draft: "telemetry
// live" below only ever measured a component buffer already warmed with the same keys.** In
// production, `internal`'s every tick calls `Registry::drain`, and `ComponentBuffer::drain`
// (`crates/logit-core/src/telemetry.rs`) `mem::take`s the whole `points` map -- so the *next*
// `count`/`timer` call for each key is a fresh insert into an empty map, not an update to an
// existing entry, and costs more than the steady-state number the first draft's tests measured.
// Both states are covered below, for both `process_batch` and `send_batch`: "telemetry live"
// (steady state, matching every occurrence between two drains) and "first call after a drain"
// (once per drain interval, forever, for as long as `internal` runs).

/// The per-batch body with telemetry disabled (`Telemetry::default()`, what every component has
/// with no `internal` component configured) -- everything above `keep`'s own cost
/// (`keep_one_event`, 0) is `process_batch`'s `Vec::with_capacity(batch.events.len())` for `out`.
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
    expect_allocs("runtime: process_batch through keep, telemetry disabled", stats, 1);
}

/// The other outcome `process_batch` can produce: every event absorbed, nothing forwarded.
/// `fixtures::statsd_event` is metrics-only (`clone_one_statsd_event` above), which is what lets
/// `Aggregator::process` return `None` for it (`logit-transforms`'
/// `a_metric_only_event_fully_absorbed_returns_none`) rather than forwarding a log/span half.
///
/// **The single allocation still costs the same 1** as the forwarding case above --
/// `Vec::with_capacity(batch.events.len())` is built before any event is processed, so a batch
/// that turns out to be *entirely* absorbed still pays for (and immediately drops) a `Vec` it
/// never pushes into. Worth revisiting if a metrics-heavy `internal`-fed pipeline (mostly-absorbed
/// batches, by construction) turns out to make this a real cost in practice -- not fixed here,
/// since this file's job is to measure, not to optimize speculatively.
#[test]
fn process_batch_fully_absorbed() {
    let mut agg = fixtures::aggregator();
    let resource = fixtures::resource();
    let telemetry = Telemetry::default();
    let warm = EventBatch { resource: resource.clone(), events: vec![fixtures::statsd_event()] };
    drop(process_batch(&mut agg, warm, &telemetry));

    let batch = EventBatch { resource, events: vec![fixtures::statsd_event()] };
    let (out, stats) = measure(|| process_batch(&mut agg, batch, &telemetry));
    assert!(out.is_none(), "a batch with nothing left to forward should not be forwarded");
    expect_allocs("runtime: process_batch, fully absorbed (aggregate)", stats, 1);
}

/// What live telemetry costs on top of the disabled path above, in **steady state** -- every
/// `count`/`timer` call here updates a `(name, tags)` key already resident in the component
/// buffer from the warm-up, exactly like every occurrence between two `internal` drains in
/// production (`ComponentBuffer::upsert`'s `get_mut` branch, `crates/logit-core/src/telemetry.rs`).
/// The *other* case -- the first call after a drain, where every key is a fresh insert -- is
/// measured separately below (`process_batch_first_call_after_a_drain`); this test's own claim is
/// narrower than the first draft's was, on review.
///
/// **Measured, not assumed: in this steady state, it costs exactly what the disabled path costs**
/// -- both this test and `process_batch_through_keep` above assert 1, the same single
/// `Vec::with_capacity` allocation. `count`/`timer` update an existing map entry in place
/// (`docs/design/internal-telemetry.md`); an `internal`-fed pipeline's steady-state cost between
/// drains is not distinguishable from `internal` being off at all.
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
    expect_allocs("runtime: process_batch through keep, telemetry live (steady state)", stats, 1);
}

/// The case steady state above doesn't cover, found in review: `internal`'s every tick calls
/// `Registry::drain`, which `mem::take`s the component buffer's whole map
/// (`ComponentBuffer::drain`) -- so the *first* `process_batch` call after each drain re-inserts
/// all three of its keys (`batches.received`, `events.received`, `process.duration`) into a map
/// that was just emptied, rather than updating existing entries. Three allocations, not one: the
/// map's backing table (first insert into an empty `HashMap` after `mem::take` reset it to no
/// capacity) plus the fresh `DdSketch` `process.duration`'s `Timer` creates on `Drop` (a `Pending`
/// this key has no prior value to merge into). This is not a one-time cost -- it recurs once per
/// `internal` drain interval, for as long as `internal` runs, which is why it needs its own
/// assertion rather than being folded into (or assumed equal to) the steady-state number above.
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
    expect_allocs("runtime: process_batch, first call after an internal drain", stats, 3);
}

/// `unwrap_batch`'s free path: `Delivered::Owned` is already the owned `EventBatch` -- no `Arc`
/// was ever involved (a single-consumer edge, `Fanout::send`'s common case), so this is a plain
/// match with nothing to allocate.
#[test]
fn unwrap_batch_owned() {
    let batch = fixtures::nginx_batch(1);
    let (out, stats) = measure(|| unwrap_batch(Delivered::Owned(batch, TraceContext::default())));
    assert_eq!(out.events.len(), 1);
    expect_allocs("runtime: unwrap_batch, Delivered::Owned", stats, 0);
}

/// `unwrap_batch`'s other free path: a real fan-out's `Arc`, but this handle is the only one left
/// (every sibling branch already dropped its own) -- `Arc::try_unwrap` succeeds with no clone,
/// same property `a_shared_batchs_arc_is_uniquely_held_only_once_every_sibling_handle_is_dropped`
/// (`runtime.rs`) pins at the `Fanout` level, measured here at the unwrap itself.
#[test]
fn unwrap_batch_shared_sole_reference() {
    let batch = fixtures::nginx_batch(1);
    let shared = Arc::new(batch);
    let (out, stats) = measure(|| unwrap_batch(Delivered::Shared(shared, TraceContext::default())));
    assert_eq!(out.events.len(), 1);
    expect_allocs("runtime: unwrap_batch, Delivered::Shared, sole reference", stats, 0);
}

/// The documented racy fallback: a sibling branch still holds its own handle to the same `Arc`
/// when this one unwraps, so `Arc::try_unwrap` fails and `unwrap_batch` falls back to a full
/// `EventBatch::clone` -- 1 allocation for the cloned `Vec<Event>` plus `clone_one_event`'s 4 for
/// the one nginx-shaped `Event` inside it, matching the per-branch cost
/// `fanout_send_two_consumers_costs_one_clone_plus_one_arc` measures at the `Fanout::send` level
/// (that test's 6 is this 5 plus the one `Arc::new` per send, which happens outside this measured
/// region).
#[test]
fn unwrap_batch_shared_contended() {
    let warm_shared = Arc::new(fixtures::nginx_batch(1));
    let warm_sibling = warm_shared.clone(); // held across the call below, forcing the fallback
    drop(unwrap_batch(Delivered::Shared(warm_shared, TraceContext::default())));
    drop(warm_sibling);

    let shared = Arc::new(fixtures::nginx_batch(1));
    let _sibling = shared.clone(); // kept alive across the measured call, forcing the fallback
    let (out, stats) = measure(|| unwrap_batch(Delivered::Shared(shared, TraceContext::default())));
    assert_eq!(out.events.len(), 1);
    expect_allocs(
        "runtime: unwrap_batch, Delivered::Shared, contended (falls back to clone)",
        stats,
        5,
    );
}

/// A no-op `Output`, so `send_batch`'s own accounting (the two receive counters, the
/// `send.duration` timer, the error counter) is what's measured here, not any real sink's
/// encode/write cost -- `crates/logit-outputs`' own encoders already have their own coverage
/// above (`mod encode`, `crates/logit-bench/benches/pipeline.rs`).
struct NoopOutput;

#[async_trait::async_trait]
impl logit_pipeline::Output for NoopOutput {
    async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
        Ok(())
    }
}

/// Always fails, so `send_batch`'s error path -- the `logit.component.errors` counter and
/// `result.with_context(...)`'s error-only work -- is exercised at all. Found missing in review:
/// every test above this point uses `NoopOutput`, which always succeeds.
struct FailingOutput;

#[async_trait::async_trait]
impl logit_pipeline::Output for FailingOutput {
    async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("simulated output failure"))
    }
}

/// `send_batch`'s failure path, telemetry disabled -- the direct counterpart to
/// `send_batch_through_a_noop_output_disabled_telemetry`, closing the gap found in the second
/// round of review: every prior `send_batch` test used `NoopOutput`, which always succeeds, so
/// none of them ever exercised `logit.component.errors` or `result.with_context(...)`'s
/// error-only work.
///
/// **4, not 1 -- fully decomposed, not just asserted:** the `async_trait` box (1, same as
/// success) plus `anyhow::anyhow!(...)` constructing `FailingOutput`'s error (1, confirmed in
/// isolation) plus `.with_context(|| format!(...))` (2: the `format!` itself, and wrapping the
/// original error and the new context into one boxed `anyhow::Error` node -- confirmed in
/// isolation too, both parts require a real `anyhow::Context`, not just a closure that never
/// runs). `1 + 1 + 2 = 4`, matching what's measured below exactly.
#[test]
fn send_batch_through_a_failing_output_disabled_telemetry() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = FailingOutput;
    let telemetry = Telemetry::default();
    let warm = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
    rt.block_on(async {
        drop(send_batch("out", &mut output, &warm, &telemetry).await);
    });

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
    let (result, stats) = measure(|| {
        rt.block_on(async { send_batch("out", &mut output, &delivered, &telemetry).await })
    });
    assert!(result.is_err(), "FailingOutput should make send_batch itself report an error");
    expect_allocs("runtime: send_batch through a failing Output, telemetry disabled", stats, 4);
}

/// `send_batch`'s failure path with live telemetry, steady state for the three success-path keys
/// (warmed with a real success first) but the *first* failure this component has seen -- so
/// `logit.component.errors` is a brand-new fourth key, not an update to an existing one. One more
/// allocation than the disabled baseline above (5, not 4): inserting a 4th key can grow the
/// `ComponentBuffer`'s map even though the first 3 already fit, the same `HashMap`-growth
/// mechanism `send_batch_first_call_after_a_drain` already relies on, just triggered by key count
/// growing rather than by `mem::take` emptying the map outright.
#[test]
fn send_batch_through_a_failing_output_telemetry_live() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut succeeding = NoopOutput;
    let mut failing = FailingOutput;
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "stdio_out", "sink");
    let warm = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
    rt.block_on(async {
        // A real success first, so batches.received/events.received/send.duration are already
        // resident -- only `errors` is new when the measured call below fails.
        send_batch("out", &mut succeeding, &warm, &telemetry)
            .await
            .expect("noop output never errors");
    });

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
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

/// The compounding case: the first call after an `internal` drain, and that call fails. Every
/// key `send_batch` touches is a fresh insert this time (`batches.received`, `events.received`,
/// `send.duration`, *and* `errors` -- four, not the three `send_batch_first_call_after_a_drain`
/// covers for a successful send), on top of the same `anyhow!`/`.with_context()` cost the
/// disabled test above pins. 7 total: not simply `4 (failure) + 3 (post-drain success)`, because
/// a map absorbing a 4th fresh key in one call can need more than one growth step -- this is
/// measured, not derived, for exactly that reason.
#[test]
fn send_batch_failing_first_call_after_a_drain() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = FailingOutput;
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "stdio_out", "sink");
    let warm = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
    rt.block_on(async {
        drop(send_batch("out", &mut output, &warm, &telemetry).await);
    });
    registry.drain(0); // what `internal`'s tick does: empties the ComponentBuffer's map

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
    let (result, stats) = measure(|| {
        rt.block_on(async { send_batch("out", &mut output, &delivered, &telemetry).await })
    });
    assert!(result.is_err());
    expect_allocs("runtime: send_batch, failing, first call after an internal drain", stats, 7);
}

/// `run_output`'s per-batch body, `logit_pipeline::send_batch`, with telemetry disabled -- the
/// direct counterpart to `process_batch_through_keep` above, closing the coverage gap found in
/// review: the first draft of this section measured `process_batch`/`unwrap_batch` but never
/// `run_output`'s own loop, despite `docs/known-gaps.md` claiming that gap closed for all three of
/// `run_transform`/`run_output`/`Fanout::send`.
///
/// **This baseline is 1, not 0 -- and telemetry has nothing to do with it.** `Output` is
/// `#[async_trait]` (`crates/logit-pipeline/src/output.rs`); the macro desugars `async fn send`
/// into a fn returning `Pin<Box<dyn Future<...>>>`, so *every* call to `output.send(..).await`
/// heap-allocates its future -- confirmed by measuring `NoopOutput::send` called directly (no
/// `dyn Output`, no vtable) alongside the `dyn Output` call this test actually makes: both cost
/// exactly 1 (16 bytes), so this is `async_trait`'s boxing, not dynamic dispatch and not this PR's
/// `Delivered`/telemetry work. A real, previously unmeasured, per-batch cost on every output sink
/// in production -- worth its own follow-up (a hand-written `Pin<Box<dyn Future>>` impl, or
/// waiting on stable `dyn`-safe async fn in traits), out of scope here.
#[test]
fn send_batch_through_a_noop_output_disabled_telemetry() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = NoopOutput;
    let telemetry = Telemetry::default();
    let warm = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
    rt.block_on(async {
        send_batch("out", &mut output, &warm, &telemetry).await.expect("noop output never errors")
    });

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
    let (_, stats) = measure(|| {
        rt.block_on(async {
            send_batch("out", &mut output, &delivered, &telemetry)
                .await
                .expect("noop output never errors")
        })
    });
    expect_allocs("runtime: send_batch through a no-op Output, telemetry disabled", stats, 1);
}

/// `send_batch` with live telemetry, steady state -- the `send_batch` counterpart to
/// `process_batch_with_live_telemetry` above. Same three keys' shape (two counters, one timing),
/// same steady-state claim, same narrower scope: this is the cost *between* `internal` drains, not
/// the first call after one (see `send_batch_first_call_after_a_drain` below).
///
/// **Same 1 as the disabled baseline above -- telemetry itself still adds nothing in steady
/// state**, the `async_trait` box is the entire cost either way. (A reallocation can show up here
/// depending on which `DdSketch` bucket a given run's elapsed time lands in -- expected and
/// harmless: `expect_allocs` asserts `allocs`, not `reallocs`, for exactly this kind of
/// timing-dependent noise; see this file's own module doc.)
#[test]
fn send_batch_through_a_noop_output_telemetry_live() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = NoopOutput;
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "stdio_out", "sink");
    let warm = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
    rt.block_on(async {
        send_batch("out", &mut output, &warm, &telemetry).await.expect("noop output never errors")
    });

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
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

/// `send_batch`'s counterpart to `process_batch_first_call_after_a_drain` -- the same
/// `mem::take`-empties-the-map effect, recurring once per `internal` drain interval, on
/// `send_batch`'s own three keys (`batches.received`, `events.received`, `send.duration`). Three
/// allocations here too, but composed differently than `process_batch`'s: the `async_trait` box
/// (1, present on every call regardless of telemetry, per the disabled test above) plus the
/// `HashMap`'s backing table (1, first insert since `mem::take` reset it) plus the fresh `DdSketch`
/// `send.duration`'s `Timer` creates on `Drop` (1) -- where `process_batch`'s 3 is instead its own
/// `Vec::with_capacity` (1) plus the same map-table and sketch costs.
#[test]
fn send_batch_first_call_after_a_drain() {
    let rt = tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
    let mut output = NoopOutput;
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("out", "stdio_out", "sink");
    let warm = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
    rt.block_on(async {
        send_batch("out", &mut output, &warm, &telemetry).await.expect("noop output never errors")
    });
    registry.drain(0); // what `internal`'s tick does: empties the ComponentBuffer's map

    let delivered = Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default());
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

/// First measured at 401 (~4/event): `encode_event`'s header/message text, the pre-sanitize
/// render, the sanitized-message copy, and each sanitized header field (hostname/app-name) were
/// all fresh per-event `String` allocations -- three of those four were function-locals recreated
/// on *every* call, so even warming `encode_into` once (per this file's own discipline) didn't
/// help, since the very next call's locals started from empty capacity again. `SyslogEncoder`
/// now holds `line`/`raw_msg`/`scratch` as reused struct fields instead (mirroring
/// `InfluxLineEncoder`'s own scratch buffers), which is what brought this down to 100 (exactly
/// 1/event). What's left is `format_rfc3339_utc`'s own per-call allocation
/// (`push_rfc5424_timestamp`, `logit_core::time`) -- the same shared, already-accepted cost
/// `stdio_out`'s own encoder documents for its own timestamp line, out of this encoder's scope to
/// avoid without changing that function's signature. See `docs/design/memory.md`.
#[test]
fn syslog_encode_into_100_events() {
    let mut encoder = SyslogEncoder::new(SyslogFormat::Rfc5424, 16);
    let batch = fixtures::nginx_batch(100);
    let mut out = MessageBuf::default();
    let _ = encoder.encode_into(&batch, &mut out); // warm-up call; EncodeStats is Copy

    let (stats_out, stats) = measure(|| encoder.encode_into(&batch, &mut out));
    assert_eq!(out.len(), 100);
    assert_eq!(stats_out.skipped_no_log, 0);
    expect_allocs("syslog_out: encode_into 100 events", stats, 100);
}

// ---------------------------------------------------------------------------------------------
// Lua
// ---------------------------------------------------------------------------------------------

/// One round trip across the Rust/Lua boundary for a script that reads one attribute and writes
/// one. Down from 21 (`docs/design/memory.md` §8, item 13) via three independent fixes: `process`
/// is a cached `RegistryKey` instead of a `_G` lookup per call, `event.attributes` returns the
/// same cached `AttrsProxy` userdata on every access instead of a fresh one, and both metamethods
/// take `mlua::String` instead of an owned Rust `String` (no metamethod-key allocation at all).
///
/// The 9 that remain, measured directly rather than assumed (by comparing this script against
/// narrower ones isolating each piece -- a passthrough with no `.attributes` touch at all, a
/// `nil`-returning script to isolate the final `Box`, a read-only access, a write-only access, and
/// two reads against an already-cached proxy):
///
/// - **4** for any call at all, win or lose: the `Rc<RefCell<Event>>` and the `EventProxy`
///   userdata itself account for 2 of those; the other 2 are inside mlua/LuaJIT's own per-call
///   bookkeeping (reference-table and GC upkeep as objects from the *previous* call become
///   collectible), not attributable to a specific line in this crate.
/// - **+1** for the `Box` on the way out (`ProcessOutcome::Emit`) -- confirmed by comparing
///   against a script that returns `nil` instead, which skips it and lands at 4.
/// - **+3** for creating and caching the `AttrsProxy` on the *first* `event.attributes` access of
///   an event, whether that access reads or writes -- a second access against the same
///   already-cached proxy costs nothing more (measured directly: two reads still total 8, the
///   same as one).
/// - **+1** for the one attribute write this fixture's script actually makes, from
///   `lua_to_value` allocating a `Bytes` to hold the new `"prod"` string.
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
    expect_allocs("lua: process 1 event", stats, 9);
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
