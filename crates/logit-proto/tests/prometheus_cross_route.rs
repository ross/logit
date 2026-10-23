//! A scraped Prometheus family relayed to a foreign sink: the `_logit.prometheus.*` carriers the
//! decoder and `prometheus_in` stamp are private
//! ([ADR `private-attribute-namespace`](../../../docs/adr/private-attribute-namespace.md)), so
//! InfluxDB line protocol carries none of them as a tag and counts nothing for them. The
//! fixed-point suites can't see this: each decodes and encodes one protocol.

use std::sync::Arc;

use logit_core::{AttrMap, EventBatch, Resource, Value};
use logit_proto::influxdb::encode::{EncodeStats, InfluxLineEncoder};
use logit_proto::prometheus::text::{parse, Dialect};
use logit_proto::prometheus::{
    families_to_events, PrometheusDecoder, ATTR_TARGET, ATTR_TYPE, LABEL_INSTANCE,
};
use logit_proto::Encoder;

const RECEIVED_AT: i64 = 1_700_000_000_000_000_000;

/// An `untyped` family with an explicit sample timestamp: the decoder stamps its type carrier and
/// the `_logit.metrics.closed` flag on it.
const UNTYPED: &str = "# TYPE queue_depth untyped\nqueue_depth{code=\"200\"} 7 1700000000000\n";

#[test]
fn an_untyped_family_reaches_influxdb_with_no_carrier_tag() {
    let families = parse(UNTYPED.as_bytes(), Dialect::Text0_0_4).unwrap();
    let events = families_to_events(&families, RECEIVED_AT, &mut PrometheusDecoder::new());
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].attributes.get(ATTR_TYPE), Some(&Value::from("untyped")));
    assert_eq!(
        events[0].attributes.get(logit_core::attrs::METRICS_CLOSED),
        Some(&Value::from("timestamped"))
    );

    // The resource scrape mode stamps.
    let mut attributes = AttrMap::new();
    attributes.insert(LABEL_INSTANCE, Value::from("node-exporter:9100"));
    attributes.insert(ATTR_TARGET, Value::from("http://node-exporter:9100/metrics"));
    let resource = Arc::new(Resource { attributes, ..Resource::default() });
    let batch = EventBatch { resource, scope: None, events };

    let mut encoder = InfluxLineEncoder::new();
    let body = encoder.encode(&batch).unwrap();
    let body = std::str::from_utf8(&body).unwrap();
    assert!(body.starts_with("queue_depth,code=200,instance=node-exporter:9100 "), "{body}");
    assert!(!body.contains("prometheus"), "a carrier leaked as a tag: {body}");
    assert!(!body.contains("_logit"), "a private key leaked as a tag: {body}");
    assert_eq!(*encoder.stats(), EncodeStats::default(), "nothing is counted for a carrier");
}
