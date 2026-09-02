//! Representative inputs and pre-built components, shared by the allocation tests
//! (`tests/allocations.rs`) and the throughput benches (`benches/pipeline.rs`) so both report
//! against the same workload.
//!
//! The workload is deliberately the repo's own reference example
//! (`examples/nginx-to-influxdb.yaml` driving `examples/nginx/nginx.conf`), not a synthetic shape
//! chosen to flatter the numbers: `syslog_in -> json -> kv_metrics -> keep -> aggregate ->
//! influxdb_out`, with the same metric specs and the same `keep` list. A measurement of a workload
//! nobody runs isn't worth recording.
//!
//! That reference pipeline is one point in the workload space, though -- `docs/design/memory.md`
//! §0 ("What these measurements can and can't tell you") is explicit that it's a *mixed* shape
//! (log + metrics + attributes) and that several sizing decisions in §8 are blocked on seeing
//! logs-only, wide-JSON, distribution-heavy-metrics, and span shapes too. The fixtures below add
//! exactly those, following the same two rules as everything above: a `const` wire-format literal
//! plus a `count` multiplier where a decoder already exists to feed, and a directly-constructed
//! `Event`/`SpanRecord` where none does (`docs/design/memory.md`'s "Fixtures" section).

use bytes::Bytes;
use logit_core::{
    AttrMap, DdSketch, Event, EventBatch, MetricKind, MetricRecord, Resource, SpanEvent, SpanKind,
    SpanLink, SpanRecord, SpanStatus, Value,
};
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

/// A statsd distribution (`ms`) line at the default, unsampled rate -- the baseline
/// [`STATSD_SAMPLED_DISTRIBUTION_LINE`]'s allocation count is measured against: decode-time
/// sample-rate extrapolation (`DdSketch::add_weighted`, `crates/logit-inputs/src/statsd.rs`) must
/// add zero allocations over this unsampled case, which `add_weighted`'s delegation to
/// `sketches_ddsketch::DDSketch::add_with_count` (constant-time, one bin touch regardless of
/// weight) satisfies for free.
pub const STATSD_DISTRIBUTION_LINE: &str = "request.latency:120|ms";

/// The same line as [`STATSD_DISTRIBUTION_LINE`], sampled at `@0.1` -- ten weighted samples
/// instead of one, exercising `DdSketch::add_weighted`'s `add_with_count` delegation on the decode
/// path.
pub const STATSD_SAMPLED_DISTRIBUTION_LINE: &str = "request.latency:120|ms|@0.1";

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

/// `count` copies of [`STATSD_DISTRIBUTION_LINE`], newline-separated.
pub fn statsd_distribution_datagram(count: usize) -> Bytes {
    join_lines(STATSD_DISTRIBUTION_LINE, count)
}

/// `count` copies of [`STATSD_SAMPLED_DISTRIBUTION_LINE`], newline-separated.
pub fn statsd_sampled_distribution_datagram(count: usize) -> Bytes {
    join_lines(STATSD_SAMPLED_DISTRIBUTION_LINE, count)
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

// -------------------------------------------------------------------------------------------
// Logs-only: a plain-text syslog line with no JSON body at all
// -------------------------------------------------------------------------------------------

/// A plain-text syslog line with no JSON body -- the logs-only workload `docs/design/memory.md`
/// §0 names as unmeasured: attributes plus a `log`, no `json` transform anywhere in the pipeline.
///
/// RFC 3164, modeled on the header shape RFC 3164 §5.4's own canonical example uses (`<34>` =
/// facility `auth`(4), severity `crit`(2)), updated to a realistic modern line: an sshd
/// authentication failure the way rsyslog would forward it, with **both** hostname and
/// `tag[pid]:` present. That's deliberately unlike [`NGINX_SYSLOG_LINE`]'s `nohostname` shape --
/// this exercises `parse_3164`'s *other* header branch (`syslog.rs`'s
/// `rfc3164_with_hostname_decodes_message_severity_and_attributes` test covers the same shape),
/// and yields six attributes (`syslog.facility`/`severity`/`timestamp`/`hostname`/`tag`/`pid`),
/// the top of the "4-6 attributes" range `docs/design/memory.md` §1 estimates for a plain syslog
/// pipeline.
///
/// No live syslogd was captured for this -- it's derived from reading `syslog.rs`'s decoder and
/// its own tests, which `docs/design/memory.md`'s "Fixtures" section calls an honest provenance
/// in its own right, not a substitute for one.
pub const SSHD_SYSLOG_LINE: &str = "<34>Aug 31 06:52:01 auth-edge-3 sshd[8843]: Failed password \
     for invalid user admin from 203.0.113.7 port 54321 ssh2";

/// `count` copies of [`SSHD_SYSLOG_LINE`] newline-separated -- the logs-only counterpart to
/// [`nginx_syslog_datagram`].
pub fn logs_only_syslog_datagram(count: usize) -> Bytes {
    join_lines(SSHD_SYSLOG_LINE, count)
}

// -------------------------------------------------------------------------------------------
// Wide JSON: a flat log line with 25-30 fields, well past AttrMap's inline capacity
// -------------------------------------------------------------------------------------------

/// A wide, flat (non-nested) JSON log line -- still `syslog_in -> json`, but 28 top-level fields
/// against [`NGINX_SYSLOG_LINE`]'s six, to stress `AttrMap`'s spill past its 8-entry inline
/// capacity harder than the reference fixture does.
///
/// Modeled on pino's (a widely used Node.js structured-logging library) documented default
/// fields (`level`, `time`, `pid`, `hostname`, `msg`), extended with the request/timing/trace/
/// deployment-metadata fields a typical Express+pino service adds per request log -- this is the
/// realistic shape a verbose structured logger produces, not an invented `field1..field30`. No
/// live pino process was captured for this; it's derived from pino's documented default output
/// shape plus the request-logging fields its ecosystem (`pino-http` and similar) commonly adds,
/// per the same honest-provenance standard [`SSHD_SYSLOG_LINE`] uses.
///
/// Wrapped in the same `nohostname`/tagged RFC 3164 envelope as [`NGINX_SYSLOG_LINE`] (facility
/// `local0`(16), severity `info`(6) -- `<134>`), so decoding it yields four `syslog.*` attributes
/// plus these 28 JSON fields once `json` merges them.
pub const WIDE_JSON_SYSLOG_LINE: &str = concat!(
    "<134>Aug 31 06:52:01 orders_api: ",
    r#"{"level":30,"time":1725091200123,"pid":4821,"#,
    r#""hostname":"api-7c9f8d6b5-abcde","name":"orders-api","#,
    r#""req_id":"c3f7a1e2-9b44-4f0a-8c2d-11f2a9d40abc","method":"POST","#,
    r#""url":"/api/v1/orders","statusCode":201,"responseTime":18.4,"#,
    r#""userId":"u_9f21c8","#,
    r#""userAgent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36","#,
    r#""remoteAddress":"198.51.100.23","remotePort":54871,"#,
    r#""referer":"https://shop.example.com/cart","contentLength":842,"#,
    r#""protocol":"HTTP/1.1","sessionId":"sess_4471bd","#,
    r#""traceId":"4bf92f3577b34da6a3ce929d0e0e4736","spanId":"00f067aa0ba902b7","#,
    r#""service":"orders-api","environment":"production","version":"3.4.1","#,
    r#""region":"us-east-1","cluster":"prod-1","pod":"orders-api-7c9f8d6b5-abcde","#,
    r#""container":"orders-api","msg":"request completed"}"#
);

/// `count` copies of [`WIDE_JSON_SYSLOG_LINE`] newline-separated.
pub fn wide_json_syslog_datagram(count: usize) -> Bytes {
    join_lines(WIDE_JSON_SYSLOG_LINE, count)
}

// -------------------------------------------------------------------------------------------
// Distribution-heavy metrics: several distinct MetricKind::Distribution values on one event
// -------------------------------------------------------------------------------------------

/// A metrics-only event carrying five *distinct* distributions -- the shape a request handler
/// instrumented with several named timers produces, e.g. a DogStatsD client's `ms`/`h`/`d` types
/// (`statsd.rs`'s module docs) firing once per internal operation timed within one request
/// (a cache lookup, a DB query, an external API call, ...), or a multi-histogram Prometheus-style
/// scrape reporting several distributions at once. Every existing metrics fixture before this one
/// carried at most a single distribution; `docs/design/memory.md` §1 is explicit that "`Box`ing
/// the `DdSketch`" trades 168 bytes/event for +1 allocation *per distribution created* and "wins
/// for logs/traces, loses for distribution-heavy metrics" -- this fixture is the distribution-
/// heavy side of that trade, which nothing in the tree measured before.
///
/// Directly constructed, not decoded from a wire literal: real statsd emits one metric per
/// datagram line, so "one event, several distinct distributions" is a post-collection shape no
/// existing decoder produces on its own -- the same reasoning `docs/design/memory.md`'s Fixtures
/// section gives for building a [`SpanRecord`] fixture by hand.
pub fn distribution_heavy_event() -> Event {
    let mut attrs = AttrMap::new();
    attrs.insert("service", "orders-api");
    attrs.insert("env", "prod");
    attrs.insert("region", "us-east-1");

    let mut event = Event::empty(0, attrs);
    for (name, value) in [
        ("http.request.duration", 42.5),
        ("db.query.duration", 8.3),
        ("cache.lookup.duration", 0.7),
        ("external_api.call.duration", 120.4),
        ("queue.wait.duration", 3.1),
    ] {
        let mut sketch = DdSketch::new();
        sketch.add(value);
        event.metrics.push(MetricRecord {
            name: logit_core::interner::intern(name),
            kind: MetricKind::Distribution(sketch),
            unit: Some(logit_core::interner::intern("ms")),
        });
    }
    event
}

// -------------------------------------------------------------------------------------------
// Spans: the one payload shape with no fixture at all before this change
// -------------------------------------------------------------------------------------------

/// A directly-constructed span, wrapped as `Event::span(...)` -- the payload shape
/// `docs/design/memory.md` §0 calls out as having **no fixture at all**: "nothing here has
/// measured the span path." There is no OTLP input in this codebase yet (`AGENTS.md`), so this
/// follows `docs/design/memory.md`'s Fixtures pattern #2 -- built by hand against
/// `crates/logit-core/src/span.rs`'s exact shape, to be replaced by a captured payload once a
/// span decoder lands.
///
/// Modeled on a typical server span for one HTTP request: a parent span (an upstream caller),
/// two [`SpanEvent`]s (a cache miss, then a slow query -- the shape an OTLP `AddEvent` call
/// produces), and one [`SpanLink`] to a related trace (e.g. the batch job that triggered this
/// request), so it exercises every field `SpanRecord` has. Deliberately narrow on attribute count,
/// though: the event's own 4 attributes and each `SpanEvent`/`SpanLink`'s 1-2 all stay well inside
/// `AttrMap`'s 8-slot inline capacity, so cloning this fixture (`clone_span_event`, 2 allocations)
/// is actually *cheaper* than the nginx shape's 4 -- the cost here is only the two `Vec`s
/// (`events`, `links`) existing at all, not any spilled attribute map. That's a finding about
/// *this* shape, not spans in general: a span whose events/links each carried more than 8
/// attributes would spill those maps just as the nginx event's 10 attributes do, and cost more to
/// clone accordingly.
pub fn span_event() -> Event {
    let mut attrs = AttrMap::new();
    attrs.insert("service.name", "orders-api");
    attrs.insert("http.method", "POST");
    attrs.insert("http.route", "/api/v1/orders");
    attrs.insert("http.status_code", Value::U64(201));

    let mut cache_miss_attrs = AttrMap::new();
    cache_miss_attrs.insert("cache.key", "orders:12345");
    cache_miss_attrs.insert("cache.hit", Value::Bool(false));

    let mut slow_query_attrs = AttrMap::new();
    slow_query_attrs.insert("db.statement", "INSERT INTO orders (...) VALUES (...)");
    slow_query_attrs.insert("db.duration_ms", Value::F64(41.2));

    let mut link_attrs = AttrMap::new();
    link_attrs.insert("link.reason", "triggered_by_batch_job");

    let record = SpanRecord {
        trace_id: [0xAB; 16],
        span_id: [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
        parent_span_id: Some([0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17]),
        name: Value::str("POST /api/v1/orders"),
        kind: SpanKind::Server,
        status: SpanStatus::Ok,
        events: vec![
            SpanEvent {
                timestamp: 1_725_091_200_050_000_000,
                name: Value::str("cache.miss"),
                attributes: cache_miss_attrs,
            },
            SpanEvent {
                timestamp: 1_725_091_200_070_000_000,
                name: Value::str("db.slow_query"),
                attributes: slow_query_attrs,
            },
        ],
        links: vec![SpanLink {
            trace_id: [0xCD; 16],
            span_id: [0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27],
            attributes: link_attrs,
        }],
        end_timestamp: 1_725_091_200_090_000_000,
    };
    Event::span(1_725_091_200_000_000_000, attrs, record)
}
