//! End-to-end: `statsd_in` decodes a relative gauge adjustment, `aggregate` resolves it.
//!
//! Each side's unit tests use its own idea of a `MetricKind::GaugeDelta`; this proves the real
//! decoder and the real transform agree, with no hand-built `GaugeDelta` in this file
//! (`docs/adr/relative-gauge-adjustments.md`).

use bytes::Bytes;
use logit_core::{MetricKind, Resource};
use logit_inputs::statsd::StatsdDecoder;
use logit_proto::Decoder;
use logit_transforms::Aggregator;
use std::sync::Arc;
use std::time::Duration;

#[test]
fn statsd_gauge_then_delta_resolves_through_aggregate() {
    let resource = Arc::new(Resource::default());
    let mut decoder = StatsdDecoder::new(resource.clone());
    let mut agg = Aggregator::new(Duration::from_secs(10));

    let batch = decoder.decode(Bytes::from_static(b"conns:10|g")).expect("should decode");
    assert_eq!(batch.events.len(), 1);
    for mut event in batch.events {
        assert!(!agg.process(&resource, &mut event), "a pure gauge event should absorb");
    }

    let batch = decoder.decode(Bytes::from_static(b"conns:+5|g")).expect("should decode");
    assert_eq!(batch.events.len(), 1);
    // The wire produced a `GaugeDelta`, not a `Gauge`, before `aggregate` sees it.
    assert!(matches!(batch.events[0].metrics[0].kind, MetricKind::GaugeDelta(v) if v == 5.0));
    for mut event in batch.events {
        assert!(!agg.process(&resource, &mut event), "a pure gauge delta event should absorb");
    }

    let flushed = agg.flush(1_000_000_000);
    assert_eq!(flushed.len(), 1);
    let (_, _, events) = &flushed[0];
    assert_eq!(events.len(), 1);
    let (event, _links) = &events[0];
    match event.metrics[0].kind {
        MetricKind::Gauge(v) => {
            assert_eq!(v, 15.0, "10|g then +5|g should resolve to 15.0 within one window")
        }
        ref other => panic!("expected Gauge, got {other:?}"),
    }
}
