//! Representative inputs and pre-built components, shared by the allocation tests
//! (`tests/allocations.rs`) and the throughput benches (`benches/pipeline.rs`) so both report
//! against the same workload.
//!
//! The workload is deliberately the repo's own reference example
//! (`examples/nginx-to-influxdb.yaml` driving `examples/nginx/nginx.conf`), not a synthetic shape
//! chosen to flatter the numbers: `syslog_in -> json -> kv_metrics -> keep -> aggregate ->
//! influxdb_out`, with the same metric specs and the same `keep` list. A measurement of a workload
//! nobody runs isn't worth recording.

use bytes::Bytes;
use logit_core::{AttrMap, DdSketch, Event, EventBatch, MetricKind, MetricRecord, Resource};
use logit_inputs::statsd::StatsdDecoder;
use logit_inputs::syslog::SyslogDecoder;
use logit_pipeline::Transform;
use logit_proto::Decoder;
use logit_transforms::{Aggregator, JsonParser, Keep, KvMetrics, MetricSpec};
use std::sync::Arc;
use std::time::Duration;

/// One nginx access-log line exactly as `examples/nginx/nginx.conf`'s `access_json_syslog` format
/// puts it on the wire: RFC 3164, `<190>` (facility `local7`, severity `info` -- nginx's defaults),
/// a 15-byte timestamp, no hostname (`nohostname`), the `nginx_access` tag, and a JSON body of the
/// six fields `nginx_metrics` reads.
///
/// This is the tag-less-hostname shape `syslog.rs`'s two-token header rule exists for, and its
/// body is the `": "`-containing JSON that makes a naive "scan for the first colon-space" parse
/// wrong -- so it exercises the real path, not a simplified one.
pub const NGINX_SYSLOG_LINE: &str = concat!(
    "<190>Aug 31 06:52:01 nginx_access: ",
    r#"{"host":"static.local","request_method":"GET","status":200,"#,
    r#""body_bytes_sent":612,"request_time":0.001,"upstream_response_time":"0.004"}"#
);

/// A statsd datagram line with DogStatsD tags -- the other input in the tree, and the one whose
/// metric names reach `interner::intern` straight off the network
/// (`docs/design/memory.md`'s interner section).
pub const STATSD_LINE: &str = "page.views:1|c|@0.5|#env:prod,region:us-east-1,service:web";

/// `count` copies of [`NGINX_SYSLOG_LINE`] newline-separated, as one UDP datagram would arrive.
///
/// `count = 1` is the honest single-line cost. Larger counts matter because the decoder amortizes
/// one `Bytes` allocation and one `now_nanos()` across the whole datagram, and because every field
/// of every event ends up a refcounted slice of this one buffer -- the retention behavior
/// `docs/design/memory.md` describes.
pub fn nginx_syslog_datagram(count: usize) -> Bytes {
    join_lines(NGINX_SYSLOG_LINE, count)
}

/// `count` copies of [`STATSD_LINE`], newline-separated.
pub fn statsd_datagram(count: usize) -> Bytes {
    join_lines(STATSD_LINE, count)
}

fn join_lines(line: &str, count: usize) -> Bytes {
    let mut out = String::with_capacity((line.len() + 1) * count);
    for i in 0..count {
        if i > 0 {
            out.push('\n');
        }
        out.push_str(line);
    }
    Bytes::from(out.into_bytes())
}

pub fn resource() -> Arc<Resource> {
    Arc::new(Resource::default())
}

pub fn syslog_decoder() -> SyslogDecoder {
    SyslogDecoder::new(resource())
}

pub fn statsd_decoder() -> StatsdDecoder {
    StatsdDecoder::new(resource())
}

/// `skip_to_brace` off, matching `examples/nginx-to-influxdb.yaml` -- the syslog decoder has
/// already stripped the header, so the whole message really is the JSON body.
pub fn json_parser() -> JsonParser {
    JsonParser::new(false)
}

/// The exact metric specs from `examples/nginx-to-influxdb.yaml`: two counters (one per-event,
/// one field-backed) and two distributions. `upstream_response_time` is populated in
/// [`NGINX_SYSLOG_LINE`], so all four metrics fire -- the more expensive of the two real cases
/// (on a non-proxied request that field is empty and the fourth metric is skipped).
pub fn kv_metrics() -> KvMetrics {
    KvMetrics::new(
        vec![
            MetricSpec { name: "nginx.requests".to_string(), field: None, unit: None },
            MetricSpec {
                name: "nginx.bytes_sent".to_string(),
                field: Some("body_bytes_sent".to_string()),
                unit: None,
            },
        ],
        vec![],
        vec![
            MetricSpec {
                name: "nginx.request_time".to_string(),
                field: Some("request_time".to_string()),
                unit: Some("s".to_string()),
            },
            MetricSpec {
                name: "nginx.upstream_response_time".to_string(),
                field: Some("upstream_response_time".to_string()),
                unit: Some("s".to_string()),
            },
        ],
    )
}

/// The `trimmed` component from the reference example: exactly the three tags that reach
/// `aggregate`, which is what bounds its series cardinality (`logit_transforms::keep`'s module
/// docs).
pub fn keep() -> Keep {
    Keep::new(vec!["host".to_string(), "request_method".to_string(), "status".to_string()])
}

pub fn aggregator() -> Aggregator {
    Aggregator::new(Duration::from_secs(10))
}

/// One event as it looks leaving `kv_metrics` -- decoded, JSON-merged, four metrics attached.
/// This is the widest the event ever gets in the reference pipeline (~10 attributes, 4 metrics)
/// and therefore the shape whose clone cost fan-out actually pays.
pub fn nginx_event() -> Event {
    let mut decoder = syslog_decoder();
    let mut json = json_parser();
    let mut kv = kv_metrics();
    let resource = resource();

    let batch = decoder.decode(nginx_syslog_datagram(1)).expect("fixture line should decode");
    let event = batch.events.into_iter().next().expect("fixture line should produce one event");
    let event = json.process(&resource, event).expect("json always forwards");
    kv.process(&resource, event).expect("kv_metrics always forwards")
}

/// `count` copies of [`nginx_event`] in one batch, for measuring the output encoders.
pub fn nginx_batch(count: usize) -> EventBatch {
    let event = nginx_event();
    EventBatch { resource: resource(), events: (0..count).map(|_| event.clone()).collect() }
}

/// A metric-only event of the shape `statsd_in` produces: one counter, a handful of tags, no log
/// and no span. The cheap end of the event-size range, against which `nginx_event` is the
/// expensive end.
pub fn statsd_event() -> Event {
    let mut decoder = statsd_decoder();
    let batch = decoder.decode(statsd_datagram(1)).expect("fixture line should decode");
    batch.events.into_iter().next().expect("fixture line should produce one event")
}

/// A single-sample distribution event -- the shape `kv_metrics` and `statsd`'s `ms`/`h`/`d` types
/// both produce, and the one that carries a whole `DDSketch` to describe one `f64`
/// (`docs/design/memory.md`'s `MetricKind` section).
pub fn distribution_event() -> Event {
    let mut sketch = DdSketch::new();
    sketch.add(0.004);
    Event::metric(
        0,
        AttrMap::new(),
        MetricRecord {
            name: logit_core::interner::intern("nginx.request_time"),
            kind: MetricKind::Distribution(sketch),
            unit: Some(logit_core::interner::intern("s")),
        },
    )
}

/// The Lua stage from `examples/statsd-to-influxdb.yaml`'s shape: reads one attribute, writes
/// another, returns the event. Deliberately small -- the point is to measure what crossing the
/// Rust/Lua boundary costs per event, not what a script's own logic costs.
pub const LUA_ENRICH_SCRIPT: &str = r#"
function process(event)
  if event.attributes.host ~= nil then
    event.attributes.env = "prod"
  end
  return event
end
"#;
