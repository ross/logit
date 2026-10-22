use super::*;
use crate::aggregate::Aggregator;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, BodyFormat, DdSketch, EventBatch, Exemplar, HyperLogLog, LogRecord, Mapping, Samples,
    SpanKind, SpanRecord, SpanStatus, Sum, Summary, Value,
};
use logit_pipeline::runtime::process_batch;
use logit_pipeline::test_util::TelemetryProbe;

const MS: i64 = 1_000_000;
/// A timestamp well inside a 1 ms bucket, so `T + n` for a small `n` stays in it.
const T: i64 = 1_700_000_000_000 * MS;

struct Rig {
    t: DedupTimestamps,
    probe: TelemetryProbe,
    telemetry: Telemetry,
}

impl Rig {
    fn new() -> Self {
        Self::with_precision(Duration::from_millis(1))
    }

    fn with_precision(precision: Duration) -> Self {
        let probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("dedup", "dedup_timestamps", "transform");
        let t = DedupTimestamps::new(precision).with_telemetry(telemetry.clone());
        Self { t, probe, telemetry }
    }

    /// One batch through the runtime's own `process_batch`: what's left of each event.
    fn run(&mut self, events: Vec<Event>) -> Vec<Event> {
        let batch = EventBatch { resource: Arc::new(Resource::default()), scope: None, events };
        process_batch(&mut self.t, batch, &mut Vec::new(), &self.telemetry)
            .map(|b| b.events)
            .unwrap_or_default()
    }

    fn sum(&mut self, name: &str, tags: &[(&str, &str)]) -> f64 {
        self.probe.sum(name, tags)
    }

    fn passed(&mut self, reason: &str) -> f64 {
        self.sum("logit.transform.metrics.passed_through", &[("reason", reason)])
    }

    fn merged(&mut self) -> f64 {
        self.sum("logit.transform.metrics.merged", &[])
    }

    fn absorbed(&mut self) -> f64 {
        self.sum("logit.transform.metrics.absorbed", &[])
    }

    fn discarded(&mut self) -> f64 {
        self.sum("logit.component.events.discarded", &[])
    }
}

fn record(kind: MetricKind) -> MetricRecord {
    MetricRecord::new(intern("m"), kind)
}

fn ev(ts: i64, kind: MetricKind) -> Event {
    Event::metric(ts, AttrMap::new(), record(kind))
}

fn delta(v: MetricValue) -> MetricKind {
    MetricKind::Sum(Sum { value: v, temporality: Temporality::Delta, monotonic: true })
}

fn cumulative(v: MetricValue) -> MetricKind {
    MetricKind::Sum(Sum { value: v, temporality: Temporality::Cumulative, monotonic: true })
}

fn marker(kind: MetricKind) -> MetricRecord {
    let mut r = record(kind);
    r.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
    r
}

fn hist(
    buckets: &[(f64, u64)],
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
) -> MetricKind {
    MetricKind::Histogram(Histogram {
        buckets: buckets.to_vec(),
        temporality: Temporality::Delta,
        sum,
        min,
        max,
    })
}

fn exp(scale: i32, positive: (i32, Vec<u64>), zero_count: u64) -> MetricKind {
    let count = positive.1.iter().sum::<u64>() + zero_count;
    MetricKind::ExponentialHistogram(ExpHistogram {
        scale,
        zero_count,
        zero_threshold: 0.0,
        positive,
        negative: (0, Vec::new()),
        temporality: Temporality::Delta,
        count,
        sum: Some(count as f64),
        min: Some(1.0),
        max: Some(2.0),
    })
}

/// Every record left, in batch order.
fn records(out: &[Event]) -> Vec<&MetricRecord> {
    out.iter().flat_map(|e| e.metrics.iter()).collect()
}

fn kinds(out: &[Event]) -> Vec<&MetricKind> {
    records(out).into_iter().map(|r| &r.kind).collect()
}

fn log_record() -> LogRecord {
    LogRecord {
        message: Value::str("msg"),
        severity: None,
        body_format: BodyFormat::Raw,
        trace: None,
        event_name: None,
        observed_timestamp: 0,
        dropped_attributes_count: 0,
    }
}

// ---- The key ----------------------------------------------------------------------------------

#[test]
fn records_of_different_series_or_buckets_pass_untouched_and_uncounted() {
    let mut rig = Rig::new();
    let mut tagged = ev(T, MetricKind::gauge(1.0));
    tagged.attributes.insert("host", "a");
    let mut other_name = ev(T, MetricKind::gauge(2.0));
    other_name.metrics[0].name = intern("n");
    let mut other_unit = ev(T, MetricKind::gauge(3.0));
    other_unit.metrics[0].unit = Some(intern("s"));
    let events = vec![
        ev(T, MetricKind::gauge(0.0)),
        tagged,
        other_name,
        other_unit,
        ev(T, delta(MetricValue::F64(4.0))),
        ev(T, cumulative(MetricValue::F64(5.0))),
        ev(T + MS, MetricKind::gauge(6.0)),
    ];
    let out = rig.run(events.clone());
    assert_eq!(out, events);
    assert_eq!(rig.merged() + rig.absorbed(), 0.0);
    assert!(!rig.probe.poll().has("logit.transform.metrics.passed_through", &[]));
}

/// At `precision: 1ms`, 0.9999 ms and 1.0 ms are two buckets, and 1.0 ms and 1.9999 ms are one.
#[test]
fn precision_buckets_by_floor_at_a_boundary() {
    let mut rig = Rig::new();
    let out = rig.run(vec![ev(999_900, MetricKind::gauge(1.0)), ev(MS, MetricKind::gauge(2.0))]);
    assert_eq!(out.len(), 2, "0.9999ms and 1.0ms must not collide");

    let out = rig.run(vec![ev(MS, MetricKind::gauge(1.0)), ev(2 * MS - 1, MetricKind::gauge(2.0))]);
    assert_eq!(kinds(&out), [&MetricKind::gauge(2.0)]);

    // Before 1970 the floor still rounds down: -1 ns is in the bucket before 0.
    let out = rig.run(vec![ev(-1, MetricKind::gauge(1.0)), ev(0, MetricKind::gauge(2.0))]);
    assert_eq!(out.len(), 2);
}

// ---- Last write wins --------------------------------------------------------------------------

#[test]
fn the_latest_gauge_survives_and_a_tie_goes_to_the_later() {
    let mut rig = Rig::new();
    let out = rig.run(vec![
        ev(T + 5, MetricKind::gauge(1.0)),
        ev(T + 2, MetricKind::gauge(2.0)),
        ev(T + 5, MetricKind::gauge(3.0)),
    ]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].timestamp, T + 5, "the survivor keeps its own timestamp");
    assert_eq!(kinds(&out), [&MetricKind::gauge(3.0)]);
    assert_eq!(rig.absorbed(), 2.0);
    assert_eq!(rig.discarded(), 2.0);
}

/// The survivor is chosen by timestamp, so a later-arriving, earlier-stamped reading loses.
#[test]
fn an_earlier_timestamp_later_in_the_batch_is_absorbed() {
    let mut rig = Rig::new();
    let out = rig.run(vec![ev(T + 9, MetricKind::gauge(1.0)), ev(T + 1, MetricKind::gauge(2.0))]);
    assert_eq!(kinds(&out), [&MetricKind::gauge(1.0)]);
}

#[test]
fn last_write_wins_ignores_value_type_and_nan() {
    let mut rig = Rig::new();
    let out = rig.run(vec![
        ev(T, MetricKind::Gauge(MetricValue::I64(1))),
        ev(T + 1, MetricKind::Gauge(MetricValue::F64(f64::NAN))),
    ]);
    assert!(matches!(kinds(&out)[..], [MetricKind::Gauge(MetricValue::F64(v))] if v.is_nan()));
    assert_eq!(rig.absorbed(), 1.0);
}

#[test]
fn cumulative_kinds_take_the_latest() {
    let mut rig = Rig::new();
    let out = rig.run(vec![
        ev(T, cumulative(MetricValue::I64(10))),
        ev(T + 1, cumulative(MetricValue::F64(12.5))),
    ]);
    assert_eq!(kinds(&out), [&cumulative(MetricValue::F64(12.5))]);

    let summary = |count| {
        MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 1.0)],
            count,
            sum: 1.0,
            temporality: Temporality::Cumulative,
        })
    };
    let cum_hist = |bounds: &[(f64, u64)]| {
        let mut k = hist(bounds, None, None, None);
        if let MetricKind::Histogram(h) = &mut k {
            h.temporality = Temporality::Cumulative;
        }
        k
    };
    let a = cum_hist(&[(1.0, 1)]);
    let b = cum_hist(&[(2.0, 5), (3.0, 1)]);
    let out =
        rig.run(vec![ev(T, summary(1)), ev(T + 1, summary(2)), ev(T, a), ev(T + 1, b.clone())]);
    assert_eq!(kinds(&out), [&summary(2), &b], "layout doesn't matter under last write wins");
    assert_eq!(rig.absorbed(), 3.0);
}

// ---- NO_RECORDED_VALUE ------------------------------------------------------------------------

#[test]
fn a_stale_marker_takes_part_in_last_write_wins_on_a_gauge() {
    let mut rig = Rig::new();
    let value = ev(T, MetricKind::gauge(1.0));
    let stale = Event::metric(T + 1, AttrMap::new(), marker(MetricKind::gauge(0.0)));
    let out = rig.run(vec![value.clone(), stale.clone()]);
    assert_eq!(out, vec![stale.clone()], "a later marker ends the series");

    let later = ev(T + 2, MetricKind::gauge(3.0));
    let out = rig.run(vec![stale, later.clone()]);
    assert_eq!(out, vec![later], "a later value supersedes the marker");
    assert_eq!(rig.absorbed(), 2.0);
}

/// A marker beside delta values passes through, and the deltas merge among themselves whether
/// the marker is the latest or between them.
#[test]
fn a_stale_marker_beside_deltas_passes_through_and_the_deltas_merge() {
    let mut rig = Rig::new();
    for marker_at in [T + 1, T + 9] {
        let flagged =
            Event::metric(marker_at, AttrMap::new(), marker(delta(MetricValue::F64(0.0))));
        let out = rig.run(vec![
            ev(T, delta(MetricValue::F64(1.0))),
            flagged.clone(),
            ev(T + 2, delta(MetricValue::F64(2.0))),
        ]);
        assert_eq!(out.len(), 2, "{marker_at}");
        assert_eq!(out[0], flagged, "the marker is untouched");
        assert_eq!(out[1].metrics[0].kind, delta(MetricValue::F64(3.0)));
        assert!(!out[1].metrics[0].is_no_recorded_value());
    }
    assert_eq!(rig.passed("no_recorded_value"), 2.0);
    assert_eq!(rig.merged(), 2.0);
}

// ---- Delta sums -------------------------------------------------------------------------------

#[test]
fn delta_sums_add_into_the_latest_keeping_the_type() {
    let mut rig = Rig::new();
    let out = rig.run(vec![
        ev(T, delta(MetricValue::I64(2))),
        ev(T + 1, delta(MetricValue::I64(3))),
        ev(T + 2, delta(MetricValue::I64(4))),
    ]);
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].timestamp, T + 2);
    assert_eq!(kinds(&out), [&delta(MetricValue::I64(9))]);
    assert_eq!(rig.merged(), 2.0);
    assert_eq!(rig.discarded(), 2.0);
}

/// The latest record sets the type; one of the other type passes through instead of widening.
#[test]
fn a_mixed_i64_and_f64_pair_passes_through_unwidened() {
    let mut rig = Rig::new();
    let out = rig.run(vec![
        ev(T, delta(MetricValue::I64(2))),
        ev(T + 1, delta(MetricValue::F64(1.5))),
        ev(T + 2, delta(MetricValue::F64(0.5))),
    ]);
    assert_eq!(
        kinds(&out),
        [&delta(MetricValue::I64(2)), &delta(MetricValue::F64(2.0))],
        "the I64 stays as it came and the F64s merge"
    );
    assert_eq!(rig.passed("mixed_type"), 1.0);
    assert_eq!(rig.merged(), 1.0);
}

/// `U64` beside `I64` is a type mismatch too: the merge never changes either's type.
#[test]
fn a_u64_and_i64_pair_passes_through_as_mixed_type() {
    let mut rig = Rig::new();
    let events = vec![ev(T, delta(MetricValue::U64(2))), ev(T + 1, delta(MetricValue::I64(3)))];
    assert_eq!(rig.run(events.clone()), events);
    assert_eq!(rig.passed("mixed_type"), 1.0);
    assert_eq!(rig.merged(), 0.0);
}

/// Two `Bool`s would sum to an `I64` count, so a `Bool` delta pair has no merge rule.
#[test]
fn a_bool_delta_pair_passes_through_as_no_merge_rule() {
    let mut rig = Rig::new();
    let events =
        vec![ev(T, delta(MetricValue::Bool(true))), ev(T + 1, delta(MetricValue::Bool(true)))];
    assert_eq!(rig.run(events.clone()), events);
    assert_eq!(rig.passed("no_merge_rule"), 1.0);
    assert_eq!(rig.merged(), 0.0);
}

#[test]
fn an_overflowing_delta_passes_through() {
    let mut rig = Rig::new();
    let out = rig
        .run(vec![ev(T, delta(MetricValue::I64(1))), ev(T + 1, delta(MetricValue::I64(i64::MAX)))]);
    assert_eq!(kinds(&out), [&delta(MetricValue::I64(1)), &delta(MetricValue::I64(i64::MAX))]);
    assert_eq!(rig.passed("overflow"), 1.0);

    let out = rig.run(vec![
        ev(T, delta(MetricValue::F64(f64::MAX))),
        ev(T + 1, delta(MetricValue::F64(f64::MAX))),
    ]);
    assert_eq!(out.len(), 2, "an F64 total leaving the finite range passes through");
    assert_eq!(rig.passed("overflow"), 2.0);
}

/// A `NaN` latest delta can't be the survivor, so the earlier finite ones still merge.
#[test]
fn a_nan_latest_delta_does_not_swallow_the_earlier_ones() {
    let mut rig = Rig::new();
    let out = rig.run(vec![
        ev(T, delta(MetricValue::F64(1.0))),
        ev(T + 1, delta(MetricValue::F64(2.0))),
        ev(T + 2, delta(MetricValue::F64(f64::NAN))),
    ]);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].timestamp, T + 1);
    assert_eq!(out[0].metrics[0].kind, delta(MetricValue::F64(3.0)));
    assert!(matches!(
        out[1].metrics[0].kind,
        MetricKind::Sum(Sum { value: MetricValue::F64(v), .. }) if v.is_nan()
    ));
    assert_eq!(rig.passed("non_finite"), 1.0);
    assert_eq!(rig.merged(), 1.0);
}

#[test]
fn a_merged_record_keeps_exemplars_in_batch_order_the_earliest_start_and_a_description() {
    let mut rig = Rig::new();
    let exemplar = |v: i64| Exemplar {
        timestamp: T,
        value: MetricValue::I64(v),
        trace: None,
        filtered_attributes: AttrMap::new(),
    };
    let mut a = record(delta(MetricValue::I64(1)));
    a.start_timestamp = 50;
    a.exemplars = vec![exemplar(1)];
    a.description = Some(intern("first"));
    let mut b = record(delta(MetricValue::I64(1)));
    b.start_timestamp = 0;
    b.exemplars = vec![exemplar(2)];
    let mut c = record(delta(MetricValue::I64(1)));
    c.start_timestamp = 70;
    c.exemplars = vec![exemplar(3)];
    c.flags = 1 << 4;
    let out = rig.run(vec![
        Event::metric(T, AttrMap::new(), a),
        Event::metric(T + 9, AttrMap::new(), b),
        Event::metric(T + 1, AttrMap::new(), c),
    ]);
    let merged = records(&out);
    assert_eq!(merged.len(), 1);
    let m = merged[0];
    assert_eq!(m.kind, delta(MetricValue::I64(3)));
    assert_eq!(m.exemplars, vec![exemplar(1), exemplar(2), exemplar(3)]);
    assert_eq!(m.start_timestamp, 50, "the earliest nonzero start");
    assert_eq!(m.description, Some(intern("first")), "the survivor has none, so the first");
    assert_eq!(m.flags, 0, "the survivor's flags");
}

// ---- No merge rule ----------------------------------------------------------------------------

#[test]
fn gauge_deltas_and_delta_summaries_pass_through_counted_once_per_duplicate() {
    let mut rig = Rig::new();
    let events = vec![
        ev(T, MetricKind::GaugeDelta(MetricValue::F64(1.0))),
        ev(T + 1, MetricKind::GaugeDelta(MetricValue::F64(2.0))),
        ev(T + 2, MetricKind::GaugeDelta(MetricValue::F64(3.0))),
    ];
    assert_eq!(rig.run(events.clone()), events);
    assert_eq!(rig.passed("no_merge_rule"), 2.0, "every one but the first");

    let summary = |count| {
        MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 1.0)],
            count,
            sum: 1.0,
            temporality: Temporality::Delta,
        })
    };
    let events = vec![ev(T, summary(1)), ev(T + 1, summary(2))];
    assert_eq!(rig.run(events.clone()), events);
    assert_eq!(rig.passed("no_merge_rule"), 3.0);
}

/// `g:+5`, `g:10`, `g:+1` in one bucket: merging the deltas would move them past the absolute
/// value and resolve to 16 in `aggregate`; passing them through keeps 11.
#[test]
fn the_statsd_gauge_sequence_resolves_to_eleven_through_aggregate() {
    let mut rig = Rig::new();
    let out = rig.run(vec![
        ev(T, MetricKind::GaugeDelta(MetricValue::F64(5.0))),
        ev(T, MetricKind::Gauge(MetricValue::F64(10.0))),
        ev(T, MetricKind::GaugeDelta(MetricValue::F64(1.0))),
    ]);
    let resource = Arc::new(Resource::default());
    let mut aggregate = Aggregator::new(Duration::from_secs(10));
    for mut event in out {
        assert!(!Transform::process(&mut aggregate, &resource, &mut event));
    }
    let flushed = aggregate.flush(T);
    let values: Vec<_> = flushed
        .iter()
        .flat_map(|(_, _, events)| events.iter())
        .flat_map(|(e, _)| e.metrics.iter())
        .map(|r| r.kind.clone())
        .collect();
    assert_eq!(values, [MetricKind::Gauge(MetricValue::F64(11.0))]);
}

// ---- Histograms -------------------------------------------------------------------------------

#[test]
fn delta_histograms_with_one_layout_merge_under_aggregates_rules() {
    let mut rig = Rig::new();
    let bounds = |a, b| [(1.0, a), (f64::INFINITY, b)];
    let out = rig.run(vec![
        ev(T, hist(&bounds(1, 2), Some(3.0), Some(0.5), Some(4.0))),
        // Observed nothing: its min and max don't count.
        ev(T + 1, hist(&bounds(0, 0), Some(0.0), Some(-9.0), Some(99.0))),
        ev(T + 2, hist(&bounds(3, 0), Some(1.5), Some(0.2), Some(0.9))),
    ]);
    assert_eq!(kinds(&out), [&hist(&bounds(4, 2), Some(4.5), Some(0.2), Some(4.0))]);
    assert_eq!(rig.merged(), 2.0);

    let out = rig.run(vec![
        ev(T, hist(&bounds(1, 0), None, Some(0.5), Some(0.5))),
        ev(T + 1, hist(&bounds(1, 0), Some(1.0), Some(0.7), None)),
    ]);
    assert_eq!(
        kinds(&out),
        [&hist(&bounds(2, 0), None, Some(0.5), None)],
        "None once one lacks it"
    );
}

#[test]
fn a_histogram_with_other_bounds_or_an_overflowing_count_passes_through() {
    let mut rig = Rig::new();
    let a = hist(&[(1.0, 1)], Some(1.0), None, None);
    let b = hist(&[(2.0, 1)], Some(1.0), None, None);
    let out = rig.run(vec![ev(T, a.clone()), ev(T + 1, b.clone())]);
    assert_eq!(kinds(&out), [&a, &b]);
    assert_eq!(rig.passed("layout_mismatch"), 1.0);

    let big = hist(&[(1.0, u64::MAX)], Some(1.0), None, None);
    let out = rig.run(vec![ev(T, big.clone()), ev(T + 1, a.clone())]);
    assert_eq!(kinds(&out), [&big, &a], "never saturated");
    assert_eq!(rig.passed("overflow"), 1.0);

    let nan = hist(&[(1.0, 1)], Some(f64::NAN), None, None);
    let out = rig.run(vec![ev(T, a.clone()), ev(T + 1, nan)]);
    assert_eq!(out.len(), 2);
    assert_eq!(rig.passed("non_finite"), 1.0);
}

#[test]
fn exponential_histograms_merge_by_absolute_bucket_index() {
    let mut rig = Rig::new();
    let out = rig.run(vec![ev(T, exp(2, (3, vec![1, 2]), 1)), ev(T + 1, exp(2, (1, vec![5]), 0))]);
    let MetricKind::ExponentialHistogram(e) = kinds(&out)[0] else { panic!("{out:?}") };
    assert_eq!(e.positive, (1, vec![5, 0, 1, 2]));
    assert_eq!((e.zero_count, e.count), (1, 9));
    assert_eq!(e.sum, Some(9.0));
    assert_eq!(rig.merged(), 1.0);

    let out = rig.run(vec![ev(T, exp(2, (0, vec![1]), 0)), ev(T + 1, exp(3, (0, vec![1]), 0))]);
    assert_eq!(out.len(), 2);
    assert_eq!(rig.passed("layout_mismatch"), 1.0);
}

// ---- Observation kinds ------------------------------------------------------------------------

#[test]
fn samples_concatenate_in_batch_order_under_one_rate() {
    let mut rig = Rig::new();
    let samples = |values: &[f64], rate: f64| {
        let mut s = Samples::new(values.iter().copied());
        s.sample_rate = rate;
        MetricKind::Samples(s)
    };
    let out = rig.run(vec![
        ev(T, samples(&[1.0], 1.0)),
        ev(T + 1, samples(&[2.0], 1.0)),
        ev(T + 9, samples(&[3.0], 1.0)),
        ev(T + 2, samples(&[4.0], 1.0)),
        ev(T + 3, samples(&[5.0], 0.5)),
    ]);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].timestamp, T + 9, "the survivor's event");
    assert_eq!(out[0].metrics[0].kind, samples(&[1.0, 2.0, 3.0, 4.0], 1.0));
    assert_eq!(out[1].metrics[0].kind, samples(&[5.0], 0.5));
    assert_eq!(rig.merged(), 3.0);
    assert_eq!(rig.passed("layout_mismatch"), 1.0);
}

#[test]
fn set_members_union_each_member_once() {
    let mut rig = Rig::new();
    let members = |m: &[&'static str]| {
        MetricKind::SetMembers(m.iter().map(|s| Bytes::from_static(s.as_bytes())).collect())
    };
    let out = rig.run(vec![ev(T, members(&["a", "b"])), ev(T + 1, members(&["b", "c"]))]);
    assert_eq!(kinds(&out), [&members(&["a", "b", "c"])]);
    assert_eq!(rig.merged(), 1.0);
}

#[test]
fn distributions_merge_and_re_bin_across_mappings() {
    let mut rig = Rig::new();
    let mut a = DdSketch::new();
    a.add(1.0);
    let mut b = DdSketch::with_mapping(Mapping::logarithmic(1.02, 0.0, 2048));
    b.add(2.0);
    b.add(3.0);
    let out =
        rig.run(vec![ev(T, MetricKind::Distribution(b)), ev(T + 1, MetricKind::Distribution(a))]);
    let MetricKind::Distribution(d) = kinds(&out)[0] else { panic!("{out:?}") };
    assert_eq!(d.count(), 3);
    assert_eq!(d.mapping(), &Mapping::agent(), "the survivor's mapping");
    assert_eq!(rig.merged(), 1.0);
}

#[test]
fn sets_merge_their_estimates() {
    let mut rig = Rig::new();
    let hll = |members: &[&str]| {
        let mut h = HyperLogLog::new();
        for m in members {
            h.insert(m.as_bytes());
        }
        MetricKind::Set(h)
    };
    let out = rig.run(vec![ev(T, hll(&["a", "b"])), ev(T + 1, hll(&["b", "c"]))]);
    let MetricKind::Set(h) = kinds(&out)[0] else { panic!("{out:?}") };
    assert_eq!(h.estimate(), 3);
}

// ---- Events -----------------------------------------------------------------------------------

#[test]
fn an_event_keeps_its_log_and_other_records_and_batch_order_holds() {
    let mut rig = Rig::new();
    let mut with_log = ev(T, MetricKind::gauge(1.0));
    with_log.log = Some(log_record());
    let mut two = ev(T + 1, MetricKind::gauge(2.0));
    two.metrics.push(MetricRecord::new(intern("other"), MetricKind::gauge(9.0)));
    let plain_log = Event::log(T, AttrMap::new(), log_record());
    let out = rig.run(vec![with_log, plain_log.clone(), two, ev(T + 5, MetricKind::gauge(3.0))]);
    assert_eq!(out.len(), 4, "nothing is emptied");
    assert!(out[0].log.is_some() && out[0].metrics.is_empty());
    assert_eq!(out[1], plain_log, "an event with no records is left alone");
    assert_eq!(kinds(&out[2..3]), [&MetricKind::gauge(9.0)]);
    assert_eq!(kinds(&out[3..]), [&MetricKind::gauge(3.0)]);
    assert_eq!(rig.discarded(), 0.0);
}

/// An event whose only metric is absorbed still carries a span, so it isn't discarded.
#[test]
fn an_event_whose_only_metric_is_absorbed_keeps_its_span() {
    let mut rig = Rig::new();
    let span = SpanRecord {
        trace_id: [1; 16],
        span_id: [1; 8],
        parent_span_id: None,
        name: Value::str("span"),
        kind: SpanKind::Internal,
        status: SpanStatus::Unset,
        events: Vec::new(),
        links: Vec::new(),
        end_timestamp: 0,
        flags: 0,
        ext: None,
    };
    let mut with_span = ev(T, MetricKind::gauge(1.0));
    with_span.span = Some(span.clone());
    let out = rig.run(vec![with_span, ev(T + 1, MetricKind::gauge(2.0))]);
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].span, Some(span));
    assert!(out[0].metrics.is_empty());
    assert_eq!(kinds(&out[1..]), [&MetricKind::gauge(2.0)]);
    assert_eq!(rig.absorbed(), 1.0);
    assert_eq!(rig.discarded(), 0.0);
}

/// Two records of one series on one event collide too, ordered by their position.
#[test]
fn two_records_on_one_event_collide() {
    let mut rig = Rig::new();
    let mut event = ev(T, delta(MetricValue::I64(1)));
    event.metrics.push(record(delta(MetricValue::I64(2))));
    let out = rig.run(vec![event]);
    assert_eq!(kinds(&out), [&delta(MetricValue::I64(3))]);
}

/// The table is per batch: a later batch's reading of the same instant isn't collapsed.
#[test]
fn no_state_carries_across_batches() {
    let mut rig = Rig::new();
    assert_eq!(rig.run(vec![ev(T, MetricKind::gauge(1.0))]).len(), 1);
    assert_eq!(rig.run(vec![ev(T, MetricKind::gauge(2.0))]).len(), 1);
    assert_eq!(rig.absorbed(), 0.0);
}
