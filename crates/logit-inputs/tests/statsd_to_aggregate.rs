//! End-to-end: `statsd_in` decodes a relative gauge adjustment, `aggregate` resolves it.
//!
//! `statsd.rs`'s own unit tests pin the decode side (`+5|g` -> `MetricKind::GaugeDelta(5.0)`) and
//! `aggregate.rs`'s own unit tests pin the resolution side directly against a hand-built
//! `MetricKind::GaugeDelta` event -- neither, on its own, proves the two components actually agree
//! with each other about what a decoded delta looks like. This is that proof, real decoder into
//! real transform, no synthetic `MetricKind::GaugeDelta` construction anywhere in this file. See
//! `docs/adr/0024-relative-gauge-adjustments.md`.

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
    for event in batch.events {
        assert!(agg.process(&resource, event).is_none(), "a pure gauge event should absorb");
    }

    let batch = decoder.decode(Bytes::from_static(b"conns:+5|g")).expect("should decode");
    assert_eq!(batch.events.len(), 1);
    // The event's own kind, straight off the decoder -- confirms the wire produced a real
    // `GaugeDelta`, not a `Gauge`, before it ever reaches `aggregate`.
    assert!(matches!(batch.events[0].metrics[0].kind, MetricKind::GaugeDelta(v) if v == 5.0));
    for event in batch.events {
        assert!(agg.process(&resource, event).is_none(), "a pure gauge delta event should absorb");
    }

    let flushed = agg.flush(1_000_000_000);
    assert_eq!(flushed.len(), 1);
    let (_, events) = &flushed[0];
    assert_eq!(events.len(), 1);
    let (event, _links) = &events[0];
    match event.metrics[0].kind {
        MetricKind::Gauge(v) => {
            assert_eq!(v, 15.0, "10|g then +5|g should resolve to 15.0 within one window")
        }
        ref other => panic!("expected Gauge, got {other:?}"),
    }
}
