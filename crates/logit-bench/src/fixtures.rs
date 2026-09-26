//! Representative inputs and pre-built components, shared by the allocation tests
//! (`tests/allocations.rs`) and the throughput benches (`benches/pipeline.rs`) so both report
//! against the same workload.
//!
//! The core workload is the repo's own reference example's pre-`http_access` shape
//! (`fixtures/nginx-to-influxdb.yaml` driving `fixtures/nginx/nginx.conf`, from back when the log
//! format was `access_json_syslog`): `syslog_in -> json -> kv_metrics -> keep -> aggregate ->
//! influxdb_out`, with that era's metric specs and `keep` list, kept unchanged so the pinned
//! allocation counts stay comparable. The current example differs: it inserts `http_access` and
//! `trace_context`, its `kv_metrics` reads semconv field names instead, `trimmed` keeps seven
//! fields instead of three, and `bounded` clamps `server.address` instead of `host`. A measurement
//! of a workload nobody ever ran isn't worth recording, which is why this shape stays real rather
//! than drifting into a hypothetical one.
//!
//! That pipeline is one *mixed* shape (log + metrics + attributes), and per-event width is bimodal
//! by signal (`docs/design/data-shapes.md`), so the file also covers logs-only, wide-JSON,
//! distribution-heavy, span, and the survey-derived shapes (`docs/design/memory.md` §0, "What
//! these measurements can and can't tell you"). Two construction rules hold throughout
//! (`docs/design/memory.md`'s "Fixtures" section): a `const` wire-format literal plus a `count`
//! multiplier where a decoder exists to feed, and a directly-constructed `Event` where none does.
//! Every literal states its provenance: which producer emitted it, or that it's hand-written and
//! from what.

use bytes::Bytes;
use logit_core::{
    AttrMap, BodyFormat, DdSketch, Event, EventBatch, LogRecord, MetricKind, MetricRecord,
    Resource, Samples, Scope, SpanEvent, SpanKind, SpanLink, SpanRecord, SpanStatus, Value,
};
use logit_inputs::generate::{GenerateInput, GenerateMetricKind};
use logit_inputs::statsd::StatsdDecoder;
use logit_inputs::syslog::SyslogDecoder;
use logit_pipeline::Transform;
use logit_proto::collectd::types_db::TEST_TYPES_DB;
use logit_proto::collectd::{CollectdDecoder, TypesDb};
use logit_proto::graphite::{GraphiteDecoder, Protocol as GraphiteProtocol};
use logit_proto::prometheus::{PrometheusDecoder, PrometheusEncoder};
use logit_proto::Decoder;
use logit_transforms::{
    AggregateTemporality, Aggregator, Arrays, CsvParser, Distributions, Fields, Flatten,
    HttpAccess, HttpAccessConfig, JsonParser, Keep, KeepValues, Kv, KvMetrics, Logfmt, MetricSpec,
    Normalize, RegexParser, RouteRule, RouteSet, Sample, SampleField, SampleKey, SampleMissing,
    SampleOverride, Set, Shape,
};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

/// One nginx access-log line in the shape `fixtures/nginx/nginx.conf`'s `access_json_syslog`
/// format put on the wire, before that format was renamed `access_semconv` and the example gained
/// `http_access`: RFC 3164, `<190>` (facility `local7`, severity `info`, nginx's defaults), a
/// 15-byte timestamp, no hostname (`nohostname`), the `nginx_access` tag, and a JSON body of the
/// six fields the pipeline's `kv_metrics` stage used to read directly (the current example's
/// `nginx_metrics` reads different, semconv field names instead). Confirmed against a live nginx
/// run when that format still existed (`docs/design/memory.md`'s "Fixtures" section), so
/// `fixtures/nginx/`, which `compose.yaml`'s `nginx` service runs, still has to stay real.
/// `perf/scenarios/json-parse.yaml`'s template is still this line's JSON body.
///
/// It exercises `syslog.rs`'s two-token header rule for a hostname-less line, and its body's
/// `": "` defeats a naive "scan for the first colon-space" parse.
pub const NGINX_SYSLOG_LINE: &str = concat!(
    "<190>Aug 31 06:52:01 nginx_access: ",
    r#"{"host":"static.local","request_method":"GET","status":200,"#,
    r#""body_bytes_sent":612,"request_time":0.001,"upstream_response_time":"0.004"}"#
);

/// A statsd datagram line with DogStatsD tags, whose metric name reaches `interner::intern`
/// straight off the network (`docs/design/memory.md` §4, "Interning: the bargain, and its bounds").
pub const STATSD_LINE: &str = "page.views:1|c|@0.5|#env:prod,region:us-east-1,service:web";

/// The same shape as [`STATSD_LINE`], except one tag key (`team`) repeats, which `insert_tags`
/// folds into a `Value::Array` in wire order (`crates/logit-inputs/src/statsd.rs`'s "DogStatsD
/// tags" section). The plain `env:prod` tag stays so the measurement is the repeated key's cost on
/// top of an ordinary tagged counter.
pub const STATSD_REPEATED_TAG_LINE: &str = "page.views:1|c|#env:prod,team:a,team:b";

/// [`STATSD_REPEATED_TAG_LINE`]'s repeated tag key on a multi-value counter line
/// (`name:v1:v2:v3|c`): `parse_line` decodes it to three `Event`s and `build_event` clones the
/// shared `AttrMap` once per value, so the tag's `Value::Array` (a `Vec` spine, not a slice of the
/// datagram) is deep-cloned once per value event, not once total.
pub const STATSD_MULTI_VALUE_REPEATED_TAG_LINE: &str = "page.views:1:2:3|c|#env:prod,team:a,team:b";

/// A statsd distribution (`ms`) line at the default, unsampled rate: the baseline for
/// [`STATSD_SAMPLED_DISTRIBUTION_LINE`]. Decodes to a raw `MetricKind::Samples`, with no
/// `DdSketch`; only `aggregate` sketches these (`docs/adr/lossless-transit.md`).
pub const STATSD_DISTRIBUTION_LINE: &str = "request.latency:120|ms";

/// [`STATSD_DISTRIBUTION_LINE`] sampled at `@0.1`. The decoder doesn't extrapolate; the raw
/// `sample_rate` rides verbatim on the decoded `Samples` (`docs/adr/lossless-transit.md`).
pub const STATSD_SAMPLED_DISTRIBUTION_LINE: &str = "request.latency:120|ms|@0.1";

/// A statsd set (`s`) line: decodes to one [`logit_core::MetricKind::SetMembers`] event holding
/// one zero-copy member slice of the datagram.
pub const STATSD_SET_LINE: &str = "unique.users:abc123|s";

/// A DogStatsD event (`_e{tlen,xlen}:title|text|...`) line whose `TEXT` has nothing to unescape,
/// so `unescape_event_text` takes its zero-copy path. Datadog's documented example event, as
/// `crates/logit-inputs/src/statsd.rs`'s `dogstatsd_docs_example_event_decodes` also uses.
pub const STATSD_EVENT_LINE: &str =
    "_e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17|t:warning|#err_type:bad_file";

/// [`STATSD_EVENT_LINE`]'s shape with one `\n` (backslash, `n`) escape in `TEXT`: the one case
/// `unescape_event_text` can't slice, since the decoded message needs a newline byte the wire
/// doesn't have. Isolates that allocation from [`STATSD_EVENT_LINE`]'s zero-copy baseline.
pub const STATSD_EVENT_LINE_WITH_ESCAPED_NEWLINE: &str = "_e{5,12}:title|line1\\nline2";

/// A DogStatsD service check (`_sc|name|status|...`) line, Datadog's documented example (as in
/// `crates/logit-inputs/src/statsd.rs`'s `dogstatsd_docs_example_service_check_decodes`). Decodes
/// to one [`logit_core::MetricKind::Gauge`] event plus the `statsd.service_check.*` carriers.
pub const STATSD_SERVICE_CHECK_LINE: &str =
    "_sc|Redis connection|2|#env:dev|m:Redis connection timed out after 10s";

/// A hand-written go-kit-style logfmt line, with one quoted value for the quoted-value scan path.
pub const LOGFMT_LINE: &str = "level=info ts=2026-09-07T06:52:01Z caller=metrics.go:159 \
    component=frontend org_id=fake latency=fast duration=12.3ms status=200 \
    msg=\"query stats\"";

/// A logfmt line with an escaped quote inside a quoted value: isolates the one path that can't
/// slice.
pub const LOGFMT_ESCAPED_LINE: &str = "level=info query=\"{job=\\\"nginx\\\"}\" status=200";

/// A hand-written query-string-style `a=1&b=2` line, the `kv` shape.
pub const KV_LINE: &str = "a=1&b=2&c=hello";

/// `count` copies of [`NGINX_SYSLOG_LINE`] newline-separated, as one UDP datagram would arrive.
///
/// `count = 1` is the single-line cost. Larger counts show the decoder amortizing one `Bytes`
/// allocation and one `now_nanos()` across the datagram, with every field of every event a
/// refcounted slice of this one buffer (`docs/design/memory.md` §2, "Retention: what pins what").
pub fn nginx_syslog_datagram(count: usize) -> Bytes {
    join_lines(NGINX_SYSLOG_LINE, count)
}

/// [`NGINX_SYSLOG_LINE`] with its six JSON keys reversed: a producer that serialises fields in a
/// different order from the one `json` warmed up on. For `json_parse_reordered_keys_event`
/// (`tests/allocations.rs`): the key cache must resynchronise without allocating.
pub const NGINX_SYSLOG_LINE_REVERSED_KEYS: &str = concat!(
    "<190>Aug 31 06:52:01 nginx_access: ",
    r#"{"upstream_response_time":"0.004","request_time":0.001,"body_bytes_sent":612,"#,
    r#""status":200,"request_method":"GET","host":"static.local"}"#
);

/// One [`NGINX_SYSLOG_LINE_REVERSED_KEYS`] datagram.
pub fn nginx_syslog_datagram_reversed_keys() -> Bytes {
    join_lines(NGINX_SYSLOG_LINE_REVERSED_KEYS, 1)
}

/// `count` copies of [`STATSD_LINE`], newline-separated.
pub fn statsd_datagram(count: usize) -> Bytes {
    join_lines(STATSD_LINE, count)
}

/// `count` copies of [`STATSD_REPEATED_TAG_LINE`], newline-separated.
pub fn statsd_repeated_tag_datagram(count: usize) -> Bytes {
    join_lines(STATSD_REPEATED_TAG_LINE, count)
}

/// `count` copies of [`STATSD_MULTI_VALUE_REPEATED_TAG_LINE`], newline-separated.
pub fn statsd_multi_value_repeated_tag_datagram(count: usize) -> Bytes {
    join_lines(STATSD_MULTI_VALUE_REPEATED_TAG_LINE, count)
}

/// `count` copies of [`STATSD_DISTRIBUTION_LINE`], newline-separated.
pub fn statsd_distribution_datagram(count: usize) -> Bytes {
    join_lines(STATSD_DISTRIBUTION_LINE, count)
}

/// `count` copies of [`STATSD_SAMPLED_DISTRIBUTION_LINE`], newline-separated.
pub fn statsd_sampled_distribution_datagram(count: usize) -> Bytes {
    join_lines(STATSD_SAMPLED_DISTRIBUTION_LINE, count)
}

/// `count` copies of [`STATSD_SET_LINE`], newline-separated.
pub fn statsd_set_datagram(count: usize) -> Bytes {
    join_lines(STATSD_SET_LINE, count)
}

/// `count` copies of [`STATSD_EVENT_LINE`], newline-separated.
pub fn statsd_event_datagram(count: usize) -> Bytes {
    join_lines(STATSD_EVENT_LINE, count)
}

/// `count` copies of [`STATSD_EVENT_LINE_WITH_ESCAPED_NEWLINE`], newline-separated.
pub fn statsd_event_with_escaped_newline_datagram(count: usize) -> Bytes {
    join_lines(STATSD_EVENT_LINE_WITH_ESCAPED_NEWLINE, count)
}

/// `count` copies of [`STATSD_SERVICE_CHECK_LINE`], newline-separated.
pub fn statsd_service_check_datagram(count: usize) -> Bytes {
    join_lines(STATSD_SERVICE_CHECK_LINE, count)
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

pub fn collectd_decoder() -> CollectdDecoder {
    CollectdDecoder::new(resource())
}

/// [`collectd_decoder`] with the short hand-written `types.db` fixture attached
/// (`logit_proto::collectd::types_db`), so `load`'s three data sources resolve to
/// `shortterm`/`midterm`/`longterm` instead of `0`/`1`/`2`. The pair shows the lookup costs
/// nothing per list (`docs/design/memory.md` §2).
pub fn collectd_decoder_with_types_db() -> CollectdDecoder {
    let types_db = Arc::new(TypesDb::parse(TEST_TYPES_DB).expect("the fixture types.db parses"));
    CollectdDecoder::new(resource()).with_types_db(types_db)
}

/// A collectd datagram of `lists` single-GAUGE value lists, packed the way collectd's own sender
/// packs them: the identity is written once and **elided** on every later list, which carries only
/// a TypeInstance part plus its Values part. The elision is what's measured: a 25-list datagram is
/// what a real host agent sends, and `decode_into` has to keep its per-list cost flat.
pub fn collectd_packet(lists: usize) -> Bytes {
    let mut bytes = Vec::new();
    collectd_string_part(&mut bytes, 0x0000, b"web-1"); // Host
    collectd_number_part(&mut bytes, 0x0008, 1_700_000_000u64 << 30); // TimeHR
    collectd_number_part(&mut bytes, 0x0009, 10u64 << 30); // IntervalHR
    collectd_string_part(&mut bytes, 0x0002, b"memory"); // Plugin
    collectd_string_part(&mut bytes, 0x0004, b"memory"); // Type
    for index in 0..lists {
        collectd_string_part(&mut bytes, 0x0005, format!("used-{index}").as_bytes());
        collectd_values_part(&mut bytes, &[(1, (index as f64).to_le_bytes())]);
    }
    Bytes::from(bytes)
}

/// One three-data-source `load`/`load` list: the multi-value shape whose records spill
/// `MetricList`'s inline capacity of 1, and the one a `types.db` renames.
pub fn collectd_load_packet() -> Bytes {
    let mut bytes = Vec::new();
    collectd_string_part(&mut bytes, 0x0000, b"web-1");
    collectd_number_part(&mut bytes, 0x0008, 1_700_000_000u64 << 30);
    collectd_string_part(&mut bytes, 0x0002, b"load");
    collectd_string_part(&mut bytes, 0x0004, b"load");
    collectd_values_part(
        &mut bytes,
        &[(1, 0.1f64.to_le_bytes()), (1, 0.2f64.to_le_bytes()), (1, 0.3f64.to_le_bytes())],
    );
    Bytes::from(bytes)
}

/// Part framing, written by hand rather than through `logit_proto::collectd::part`'s writers: a
/// fixture the codec built for itself stops being an independent statement of the wire format the
/// moment that code changes.
fn collectd_part_header(out: &mut Vec<u8>, part_type: u16, payload_len: usize) {
    out.extend_from_slice(&part_type.to_be_bytes());
    out.extend_from_slice(&((payload_len + 4) as u16).to_be_bytes());
}

fn collectd_string_part(out: &mut Vec<u8>, part_type: u16, value: &[u8]) {
    collectd_part_header(out, part_type, value.len() + 1);
    out.extend_from_slice(value);
    out.push(0);
}

fn collectd_number_part(out: &mut Vec<u8>, part_type: u16, value: u64) {
    collectd_part_header(out, part_type, 8);
    out.extend_from_slice(&value.to_be_bytes());
}

fn collectd_values_part(out: &mut Vec<u8>, values: &[(u8, [u8; 8])]) {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(values.len() as u16).to_be_bytes());
    for (ds_type, _) in values {
        payload.push(*ds_type);
    }
    for (_, raw) in values {
        payload.extend_from_slice(raw);
    }
    collectd_part_header(out, 0x0006, payload.len());
    out.extend_from_slice(&payload);
}

pub fn graphite_decoder() -> GraphiteDecoder {
    GraphiteDecoder::new(resource())
}

/// [`graphite_decoder`] reading carbon's pickle protocol instead of plaintext. Pairs with
/// [`graphite_pickle_frame`] to price the other wire for the same datapoints
/// (`docs/design/memory.md` §2).
pub fn graphite_pickle_decoder() -> GraphiteDecoder {
    GraphiteDecoder::new(resource()).with_protocol(GraphiteProtocol::Pickle)
}

/// A carbon plaintext datagram of `lines` datapoints, each `path value timestamp\n`. `graphite_in`
/// hands `decode_into` this shape under both transports: a UDP datagram, or a TCP read's worth of
/// complete lines.
pub fn graphite_datagram(lines: usize) -> Bytes {
    let mut text = String::new();
    for index in 0..lines {
        text.push_str(&format!("servers.web-1.cpu.core{index} 0.5 1700000000\n"));
    }
    Bytes::from(text)
}

/// One tagged line in carbon 1.1+'s `;k=v` syntax. The measurement pins that the decoder slices
/// the tag values zero-copy out of this buffer.
pub fn graphite_tagged_datagram() -> Bytes {
    Bytes::from_static(b"servers.web-1.cpu;env=prod;region=us-east 0.5 1700000000\n")
}

/// One carbon pickle **payload** of `datapoints` datapoints: `[(path, (timestamp, value)), ...]` at
/// protocol 2, with no 4-byte length prefix.
///
/// Unframed, because that's what [`GraphiteDecoder`] is handed. Carbon's framing (a big-endian
/// `u32` payload length, Twisted's `Int32StringReceiver`) belongs to the listener, which strips it
/// before calling `decode_into` (`crates/logit-inputs/src/tcp.rs`'s `Framer`).
///
/// The opcodes are hand-written rather than produced by
/// `logit_proto::graphite::pickle::write_datapoints`, for [`collectd_part_header`]'s reason.
/// Protocol 2 matches what `pickle.dumps(..., protocol=2)`, carbon's documented example, emits.
pub fn graphite_pickle_frame(datapoints: usize) -> Bytes {
    const PROTO: u8 = 0x80;
    const EMPTY_LIST: u8 = 0x5d;
    const MARK: u8 = 0x28;
    const BINUNICODE: u8 = 0x58;
    const BININT: u8 = 0x4a;
    const BINFLOAT: u8 = 0x47;
    const TUPLE2: u8 = 0x86;
    const APPENDS: u8 = 0x65;
    const STOP: u8 = 0x2e;

    let mut out = vec![PROTO, 2, EMPTY_LIST, MARK];
    for index in 0..datapoints {
        let path = format!("servers.web-1.cpu.core{index}");
        out.push(BINUNICODE);
        // Every length field in pickle is little-endian; only BINFLOAT is big-endian.
        out.extend_from_slice(&(path.len() as u32).to_le_bytes());
        out.extend_from_slice(path.as_bytes());
        out.push(BININT);
        out.extend_from_slice(&1_700_000_000i32.to_le_bytes());
        out.push(BINFLOAT);
        out.extend_from_slice(&0.5f64.to_be_bytes());
        out.push(TUPLE2); // (timestamp, value)
        out.push(TUPLE2); // (path, (timestamp, value))
    }
    out.push(APPENDS);
    out.push(STOP);
    Bytes::from(out)
}

/// `skip_to_brace` off, matching `fixtures/nginx-to-influxdb.yaml`: the syslog decoder has already
/// stripped the header, so the whole message is the JSON body.
pub fn json_parser() -> JsonParser {
    JsonParser::new(false)
}

/// Returns a `.clone()` of one [`bytes::Bytes`] per `cache`, built from `line` on first call.
///
/// This is the warm-up rule for a directly-constructed message (`docs/design/memory.md`'s
/// "Fixtures" section). `bytes::Bytes` defers its shared, refcounted representation until a
/// buffer is first cloned or sliced, then pays one `#[cold]` `Box<Shared>` allocation
/// (`bytes-1.x`'s `promotable_{even,odd}_clone` -> `shallow_clone_vec`). A `Bytes` built fresh on
/// every call pays that inside every measured region; a memoized one pays it once, on the warm-up
/// call. Decoder-backed fixtures get the same effect from the test holding one base `Bytes` (see
/// [`nginx_syslog_datagram`]); this keeps the zero-arg `-> Event` signatures.
fn cached_message(line: &'static str, cache: &'static OnceLock<Bytes>) -> Bytes {
    cache.get_or_init(|| Bytes::copy_from_slice(line.as_bytes())).clone()
}

/// A directly-constructed log event whose message is [`LOGFMT_LINE`]. No decoder is needed:
/// `logfmt`/`kv` read `event.log.message` directly.
pub fn logfmt_event() -> Event {
    static MESSAGE: OnceLock<Bytes> = OnceLock::new();
    Event::log(
        0,
        AttrMap::new(),
        LogRecord {
            message: Value::Str(cached_message(LOGFMT_LINE, &MESSAGE)),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// [`logfmt_event`], with [`LOGFMT_ESCAPED_LINE`] as the message instead.
pub fn logfmt_escaped_event() -> Event {
    static MESSAGE: OnceLock<Bytes> = OnceLock::new();
    Event::log(
        0,
        AttrMap::new(),
        LogRecord {
            message: Value::Str(cached_message(LOGFMT_ESCAPED_LINE, &MESSAGE)),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// A directly-constructed log event whose message is [`KV_LINE`].
pub fn kv_event() -> Event {
    static MESSAGE: OnceLock<Bytes> = OnceLock::new();
    Event::log(
        0,
        AttrMap::new(),
        LogRecord {
            message: Value::Str(cached_message(KV_LINE, &MESSAGE)),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// `bare_keys` off, the default.
pub fn logfmt_parser() -> Logfmt {
    Logfmt::new(false)
}

/// `pair_sep: "&"`, `kv_sep: "="` -- [`KV_LINE`]'s shape.
pub fn kv_parser() -> Kv {
    Kv::new("&".to_string(), "=".to_string(), false)
}

/// One hand-written CSV access-log line: seven columns, one a quoted path containing the
/// delimiter, for the quoted path.
pub const CSV_ACCESS_LINE: &str = "10.0.0.1,2026-09-07T06:52:01Z,GET,\"/a,b\",200,612,0.012";
/// The header row [`csv_parser`]'s columns render as, for the header-row-recognition path
/// (`docs/adr/csv-positional-columns.md`).
pub const CSV_ACCESS_HEADER: &str =
    "remote_addr,time_local,request_method,path,status,bytes_sent,request_time";
/// Sixteen columns, no quoting -- past `AttrMap`'s 8 inline slots.
pub const CSV_WIDE_LINE: &str = "a,b,c,d,e,f,g,h,i,j,k,l,m,n,o,p";

/// The seven-column schema [`CSV_ACCESS_LINE`] matches, comma-delimited.
pub fn csv_parser() -> CsvParser {
    CsvParser::new(
        vec![
            "remote_addr".to_string(),
            "time_local".to_string(),
            "request_method".to_string(),
            "path".to_string(),
            "status".to_string(),
            "bytes_sent".to_string(),
            "request_time".to_string(),
        ],
        b',',
    )
}

/// The sixteen-column schema [`CSV_WIDE_LINE`] matches, comma-delimited. Columns are named
/// `field0`..`field15`, not `a`..`p`: matching the data would make the header line and a data row
/// byte-identical and trip header-row recognition.
pub fn csv_wide_parser() -> CsvParser {
    CsvParser::new((0..16).map(|i| format!("field{i}")).collect(), b',')
}

/// One log event whose message is `line` and no attributes: the shape `csv` reads.
///
/// The message is cloned once here, before any transform sees it, so the one-time `Bytes`
/// promotion (see [`cached_message`]) lands outside the measured `process` call. That gives it the
/// already-shared starting state [`nginx_event`]/[`statsd_event`] get from a real decoder.
pub fn csv_event(line: &str) -> Event {
    let message = Value::str(line);
    let _ = message.clone();
    Event::log(
        0,
        AttrMap::new(),
        LogRecord {
            message,
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// The metric specs from the reference example's pre-`http_access` shape, the same era
/// [`NGINX_SYSLOG_LINE`]'s field names come from: two counters (one per-event, one field-backed)
/// and two distributions. [`NGINX_SYSLOG_LINE`] populates `upstream_response_time`, so all four
/// fire: the costlier real case (a non-proxied request skips the fourth). The current example's
/// `nginx_metrics` reads different, semconv field names (`http.response.body.size`,
/// `http.request.duration_s`, `upstream.duration_s`) instead.
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

/// The reference example's pre-`http_access` `trimmed` component: the three tags that reached
/// `aggregate`, bounding its series cardinality, before the current example's `trimmed` grew to
/// keep seven semconv fields instead.
pub fn keep() -> Keep {
    Keep::new(vec!["host".to_string(), "request_method".to_string(), "status".to_string()])
}

/// The reference example's pre-`http_access` `bounded` component: clamps `host` to the two real
/// vhosts, lowercasing first (`tests/allocations.rs`'s
/// `keep_values_one_event`/`keep_values_one_event_needs_lowering`) -- the current example's
/// `bounded` clamps `server.address` instead.
pub fn keep_values() -> KeepValues {
    KeepValues::new(
        vec![],
        vec![(
            "host".to_string(),
            vec![Normalize::Lower],
            vec![Value::str("static.local"), Value::str("proxy.local")],
            Some(Value::str("other")),
        )],
    )
}

/// [`nginx_event`] with `host` uppercased, so `keep_values`' `normalize: [lower]` step has work to
/// do and allocates (`keep_values_one_event_needs_lowering`).
pub fn nginx_event_with_uppercase_host() -> Event {
    let mut event = nginx_event();
    event.attributes.insert("host", Value::str("STATIC.LOCAL"));
    event
}

/// The `tap` component from [`fixtures/shape-tap.yaml`](../../../fixtures/shape-tap.yaml): every
/// field at its default (`resource: drop`, both caps at 4096) plus a name, so the measured path is
/// the one a real config builds, `tap` tag included (`docs/adr/shape-observer-component.md`).
pub fn shape() -> Shape {
    Shape::new(Duration::from_secs(10)).with_name("tap")
}

/// A `flatten` at its defaults (`attributes: all`, `resource: none`, `arrays: index`), for
/// `tests/allocations.rs`'s `flatten_*` measurements.
pub fn flatten() -> Flatten {
    Flatten::new(Fields::All, Fields::None, Arrays::Index)
}

/// A `sample` keyed on `trace_id` at `rate: 0.5`, `fixtures/sample-traces.yaml`'s shape, for
/// `tests/allocations.rs`'s `sample_*` measurements. Seeded so the random draw a missing key falls
/// back to is reproducible.
pub fn sample_by_trace_id() -> Sample {
    Sample::new(0.5, Some(SampleKey::TraceId), SampleMissing::Random, None).with_seed(1)
}

/// A `sample` keyed on [`nginx_event`]'s numeric `status` attribute: the path that formats a
/// number's decimal text straight into the hasher.
pub fn sample_by_status() -> Sample {
    Sample::new(0.5, Some(SampleKey::Attribute("status".to_string())), SampleMissing::Random, None)
        .with_seed(1)
}

/// A keyless `sample`: one seeded draw per event.
pub fn sample_random() -> Sample {
    Sample::new(0.5, None, SampleMissing::Random, None).with_seed(1)
}

/// A `sample` at `rate: 0` with an `always_keep` on [`nginx_event`]'s `host: static.local`: the
/// override-hit path, one `value_matches` string compare.
pub fn sample_override() -> Sample {
    Sample::new(
        0.0,
        Some(SampleKey::TraceId),
        SampleMissing::Drop,
        Some(SampleOverride {
            field: SampleField::Attribute("host".to_string()),
            value: Some(Value::str("static.local")),
        }),
    )
}

/// A `set` with one attribute pair and no resource pairs: the per-event-only path
/// (`tests/allocations.rs`'s `process_batch_through_set_attributes_only`).
pub fn set_attributes() -> Set {
    Set::new(vec![], vec![("env".to_string(), Value::str("prod"))])
}

/// A `set` with one resource pair and no attribute pairs, for `map_resource`'s one-entry cache
/// (`tests/allocations.rs`'s `set_resource_map_resource_cache_hit_costs_nothing`/`_miss`).
pub fn set_resource() -> Set {
    Set::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![])
}

/// A `has_attributes` matching [`nginx_event`]'s `status`, configured as `I64(200)` against the
/// JSON-sourced `U64(200)`, so it exercises `value_matches`' cross-variant coercion rather than an
/// exact-type match (`tests/allocations.rs`'s `has_attributes_one_event`).
pub fn has_attributes() -> logit_transforms::HasAttributes {
    logit_transforms::HasAttributes::new(vec![], vec![("status".to_string(), Value::I64(200))])
}

/// [`has_attributes`]'s complement, same config (`tests/allocations.rs`'s
/// `drop_attributes_one_event`).
pub fn drop_attributes() -> logit_transforms::DropAttributes {
    logit_transforms::DropAttributes::new(vec![], vec![("status".to_string(), Value::I64(200))])
}

/// A `has_attributes` matching on the batch resource instead of event attributes, for the
/// resource-match cache's hit and miss costs (`tests/allocations.rs`'s
/// `has_attributes_resource_match_cache_hit`/`_miss`).
pub fn has_attributes_resource() -> logit_transforms::HasAttributes {
    logit_transforms::HasAttributes::new(
        vec![("service.name".to_string(), Value::str("nginx"))],
        vec![],
    )
}

/// A `has_attributes` matching [`nginx_event_with_stream`]'s `stream` against one value: the
/// fan-out half of the route-vs-fan-out comparison (`tests/allocations.rs`'s `// Routing` section,
/// `docs/adr/target-components.md`). One per branch is a `stream: host`/`stream: app` split without
/// a `route`.
pub fn has_attributes_stream(value: &str) -> logit_transforms::HasAttributes {
    logit_transforms::HasAttributes::new(vec![], vec![("stream".to_string(), Value::str(value))])
}

/// A `trace_context` lifting `trace_id` only (no `span_id`/`flags`, `keep_source: false`), for
/// `tests/allocations.rs`'s `trace_context_lifts_a_valid_trace_id`.
pub fn trace_context() -> logit_transforms::TraceContext {
    logit_transforms::TraceContext::new("trace_id".to_string(), None, None, false)
}

/// A `trace_context` with the convention defaults and a `span:` block (`kind: server`, `name:
/// http.request`, no minting), as `demo/logit.yaml`'s `haproxy_trace`/`nginx_trace` run it
/// (`tests/allocations.rs`'s `trace_context_mints_a_span_from_the_convention`).
pub fn trace_context_with_span() -> logit_transforms::TraceContext {
    logit_transforms::TraceContext::new(
        "trace.id".to_string(),
        Some("span.id".to_string()),
        Some("trace.flags".to_string()),
        false,
    )
    .with_span(logit_transforms::SpanLift {
        mint_id: false,
        name: "http.request".to_string(),
        kind: logit_core::SpanKind::Server,
        max_skew: Duration::from_secs(3600),
    })
}

/// [`nginx_event`] plus the span convention's attributes as `demo/nginx/nginx.conf`'s log_format
/// emits them after `json`: an inbound `traceparent`, this hop's `trace.id`/`span.id`, and nginx's
/// ms-resolution `span.end_s` (`$msec`) / `span.duration_s` (`$request_time`) as JSON floats. The
/// receipt timestamp sits just after `span.end_s`, inside the default `max_skew` window whatever
/// the wall clock.
pub fn nginx_traced_event() -> Event {
    let mut event = nginx_event();
    event.attributes.insert(
        "traceparent",
        Value::str("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
    );
    event.attributes.insert("trace.id", Value::str("4bf92f3577b34da6a3ce929d0e0e4736"));
    event.attributes.insert("span.id", Value::str("a1b2c3d4e5f60718"));
    event.attributes.insert("span.end_s", Value::F64(1_725_091_200.123));
    event.attributes.insert("span.duration_s", Value::F64(0.004));
    event.timestamp = 1_725_091_200_125_000_000;
    event
}

pub fn aggregator() -> Aggregator {
    Aggregator::new(Duration::from_secs(10))
}

/// `count` distinct resources, one `Arc` each, the way an `otlp_in` gateway sees one SDK instance
/// per `ResourceMetrics` (`benches/pipeline.rs`'s `aggregate_absorb_with_groups`). Hand-written
/// from OTel resource semantic conventions, not captured: 12 string attributes, the first four
/// shared by every resource and the other eight unique to it. Every resource interns its keys in
/// the same order, so the shared four sort first and an unequal compare walks them before it
/// finds a difference.
pub fn resources_for_groups(count: usize) -> Vec<Arc<Resource>> {
    (0..count)
        .map(|i| {
            let mut attributes = AttrMap::new();
            attributes.insert("service.namespace", Value::str("shop"));
            attributes.insert("deployment.environment", Value::str("production"));
            attributes.insert("cloud.provider", Value::str("aws"));
            attributes.insert("cloud.region", Value::str("us-east-1"));
            attributes.insert("service.name", Value::str(format!("checkout-{}", i % 7)));
            attributes.insert("service.instance.id", Value::str(format!("instance-{i:06}")));
            attributes.insert("host.name", Value::str(format!("ip-10-0-{}-{}", i / 250, i % 250)));
            attributes.insert("host.id", Value::str(format!("i-0a1b2c3d4e{i:06x}")));
            attributes.insert("k8s.node.name", Value::str(format!("node-{}", i % 40)));
            attributes.insert("k8s.pod.name", Value::str(format!("checkout-7d9f8-{i:05}")));
            attributes
                .insert("k8s.pod.uid", Value::str(format!("5f1c2a9e-0000-4000-8000-{i:012}")));
            attributes.insert("container.id", Value::str(format!("{i:064x}")));
            Arc::new(Resource { attributes, ..Resource::default() })
        })
        .collect()
}

/// [`nginx_event`] after [`keep`], its four metrics replaced by one `Gauge`: hand-written, not a
/// shape a pipeline stage emits. A gauge is the cheapest merge `aggregate` has (no sketch, no
/// allocation), so what varies across `aggregate_absorb_with_groups`'s arguments is the group
/// scan.
pub fn gauge_event_after_keep() -> Event {
    let mut event = nginx_event();
    assert!(keep().process(&resource(), &mut event), "keep forwards");
    event.metrics.clear();
    event.metrics.push(MetricRecord::new(
        logit_core::interner::intern("nginx.connections.active"),
        MetricKind::Gauge(3.0),
    ));
    event
}

/// [`aggregator`] with cross-flush series retention, a path the default (`series_retention: 0`)
/// never takes (`tests/allocations.rs`'s `aggregate_flush_retained_gauges`).
pub fn aggregator_with_series_retention(retention: u32, max_retained: usize) -> Aggregator {
    Aggregator::new(Duration::from_secs(10)).with_series_retention(retention, max_retained)
}

/// [`aggregator_with_series_retention`] in `temporality: cumulative` mode, the only mode that
/// retains a *counter* accumulator across flushes (`docs/adr/aggregation-window-semantics.md`'s
/// cumulative amendment; `tests/allocations.rs`'s `aggregate_flush_cumulative_sums`).
pub fn aggregator_cumulative(retention: u32, max_retained: usize) -> Aggregator {
    Aggregator::new(Duration::from_secs(10))
        .with_temporality(AggregateTemporality::Cumulative)
        .with_series_retention(retention, max_retained)
}

/// [`aggregator`] with `distributions: samples` and the given cap: the raw-retention path the
/// default (`distributions: sketch`) never takes (`tests/allocations.rs`'s
/// `aggregate_absorb_25_samples_values_into_one_series_samples_mode`).
pub fn aggregator_with_samples_retention(max_samples_per_series: usize) -> Aggregator {
    Aggregator::new(Duration::from_secs(10))
        .with_distributions(Distributions::Samples, max_samples_per_series)
}

/// A metric-only event carrying one `MetricKind::Samples` record, the raw shape statsd's
/// `ms`/`h`/`d` timings decode to, for what `aggregate` pays to absorb one
/// (`tests/allocations.rs`'s `aggregate_absorb_one_samples_event_sketch_mode`/
/// `aggregate_absorb_25_samples_values_into_one_series_samples_mode`). Unsampled
/// (`sample_rate: 1.0`), so `Samples::weight`'s clamping stays out of the measurement.
pub fn samples_event(name: &str, values: impl IntoIterator<Item = f64>) -> Event {
    Event::metric(
        0,
        AttrMap::new(),
        MetricRecord::new(
            logit_core::interner::intern(name),
            MetricKind::Samples(Samples::new(values)),
        ),
    )
}

/// One event as it leaves `kv_metrics`: decoded, JSON-merged, four metrics attached. The widest
/// this pre-`http_access` fixture pipeline's event gets (10 attributes, spilled; 4 metrics), so
/// the shape whose clone cost fan-out pays.
pub fn nginx_event() -> Event {
    let mut decoder = syslog_decoder();
    let mut json = json_parser();
    let mut kv = kv_metrics();
    let resource = resource();

    let batch = decoder.decode(nginx_syslog_datagram(1)).expect("fixture line should decode");
    let mut event = batch.events.into_iter().next().expect("fixture line should produce one event");
    assert!(json.process(&resource, &mut event), "json always forwards");
    assert!(kv.process(&resource, &mut event), "kv_metrics always forwards");
    event
}

/// `count` copies of [`nginx_event`] in one batch, for measuring the output encoders.
pub fn nginx_batch(count: usize) -> EventBatch {
    let event = nginx_event();
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count).map(|_| event.clone()).collect(),
    }
}

/// [`nginx_event`] plus a `stream` attribute, the tag the central-collector topology switches on
/// (`tests/allocations.rs`'s `// Routing` section, `docs/adr/target-components.md`).
pub fn nginx_event_with_stream(stream: &str) -> Event {
    let mut event = nginx_event();
    event.attributes.insert("stream", Value::str(stream));
    event
}

/// `count` [`nginx_event_with_stream`] events in one batch, alternating `"host"`/`"app"`: the split
/// the route-vs-fan-out comparison measures both ways (`tests/allocations.rs`'s `// Routing`
/// section).
pub fn nginx_batch_alternating_stream(count: usize) -> EventBatch {
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count)
            .map(|i| nginx_event_with_stream(if i % 2 == 0 { "host" } else { "app" }))
            .collect(),
    }
}

/// A metric-only event as `statsd_in` produces it: one counter, a handful of tags, no log or span.
/// The cheap end of the event-size range; [`nginx_event`] is the expensive end.
pub fn statsd_event() -> Event {
    let mut decoder = statsd_decoder();
    let batch = decoder.decode(statsd_datagram(1)).expect("fixture line should decode");
    batch.events.into_iter().next().expect("fixture line should produce one event")
}

/// `count` copies of [`statsd_event`] in one batch: [`nginx_batch`]'s metric-only twin, for
/// `statsd_out`'s encoder.
pub fn statsd_batch(count: usize) -> EventBatch {
    let event = statsd_event();
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count).map(|_| event.clone()).collect(),
    }
}

/// A single-value `Distribution` event: a whole `DdSketch` describing one `f64`
/// (`docs/design/memory.md` §1). `kv_metrics` and `statsd_in` emit raw `Samples` instead (see
/// [`samples_event`]); a sketch appears only after `aggregate`.
pub fn distribution_event() -> Event {
    let mut sketch = DdSketch::new();
    sketch.add(0.004);
    Event::metric(
        0,
        AttrMap::new(),
        MetricRecord {
            unit: Some(logit_core::interner::intern("s")),
            ..MetricRecord::new(
                logit_core::interner::intern("nginx.request_time"),
                MetricKind::Distribution(sketch),
            )
        },
    )
}

/// A gauge event with a *spilled* 12-attribute map (past `AttrMap`'s 8 inline slots, un-`keep`ed),
/// so `aggregate_flush_retained_gauges` (`tests/allocations.rs`) pins a retained series'
/// `key.attributes.clone()` as a heap allocation, not the memcpy `aggregate_flush_4_series`'
/// `keep`-trimmed fixture gets.
pub fn wide_gauge_event(name: &str, value: f64) -> Event {
    let mut attributes = AttrMap::new();
    for i in 0..12 {
        attributes.insert(&format!("tag{i}"), format!("value{i}").as_str());
    }
    Event::metric(
        0,
        attributes,
        MetricRecord::new(logit_core::interner::intern(name), MetricKind::Gauge(value)),
    )
}

/// [`wide_gauge_event`]'s counter twin: the same spilled 12-attribute map with a delta `Sum`, so
/// `aggregate_flush_cumulative_sums` (`tests/allocations.rs`) is comparable with the retained-gauge
/// number.
pub fn wide_counter_event(name: &str, value: f64) -> Event {
    let mut attributes = AttrMap::new();
    for i in 0..12 {
        attributes.insert(&format!("tag{i}"), format!("value{i}").as_str());
    }
    Event::metric(
        0,
        attributes,
        MetricRecord::new(logit_core::interner::intern(name), MetricKind::counter(value)),
    )
}

/// A directly-constructed log event in pino-http's completion-record shape
/// (`docs/design/data-shapes-rows.md` §A): pino's flat fields plus `reqId`/`responseTime` and
/// nested `req`/`res` maps, each nesting a `headers` map. Four boxed `AttrMap`s at depth 2, which
/// gives `tests/allocations.rs`'s `flatten_*` measurements something to expand.
///
/// Already parsed: the attributes are built directly, not by `json`, and `msg` is the log message,
/// leaving eight top-level attributes, the inline capacity (contrast [`pino_http_log_event`]).
/// Values are illustrative, not captured.
pub fn pino_http_event() -> Event {
    let mut req_headers = AttrMap::new();
    req_headers.insert("host", Value::str("api.example.com"));
    req_headers.insert("user-agent", Value::str("curl/8.0"));
    req_headers.insert("accept", Value::str("*/*"));

    let mut req = AttrMap::new();
    req.insert("method", Value::str("GET"));
    req.insert("url", Value::str("/v1/widgets"));
    req.insert("headers", Value::Map(Box::new(req_headers)));

    let mut res_headers = AttrMap::new();
    res_headers.insert("content-type", Value::str("application/json"));
    res_headers.insert("content-length", Value::I64(612));

    let mut res = AttrMap::new();
    res.insert("statusCode", Value::I64(200));
    res.insert("headers", Value::Map(Box::new(res_headers)));

    let mut attributes = AttrMap::new();
    attributes.insert("level", Value::I64(30));
    attributes.insert("time", Value::I64(1_725_000_000_000));
    attributes.insert("pid", Value::I64(1));
    attributes.insert("hostname", Value::str("web-1"));
    attributes.insert("reqId", Value::str("req-1"));
    attributes.insert("responseTime", Value::F64(12.4));
    attributes.insert("req", Value::Map(Box::new(req)));
    attributes.insert("res", Value::Map(Box::new(res)));

    Event::log(
        0,
        attributes,
        LogRecord {
            message: Value::str("request completed"),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// The Lua stage from `fixtures/statsd-to-influxdb.yaml`'s shape: reads one attribute, writes
/// another, returns the event. Kept small so it measures the Rust/Lua boundary crossing per event,
/// not a script's own logic. The baseline the other `LUA_*` scripts are measured over.
pub const LUA_ENRICH_SCRIPT: &str = r#"
function process(event)
  if event.attributes.host ~= nil then
    event.attributes.env = "prod"
  end
  return event
end
"#;

/// Writes a `resource` attribute on every call: a script stamping a resource identity
/// (`crates/logit-script/src/resource.rs`, `docs/adr/operator-declared-resource-attributes.md`;
/// `tests/allocations.rs`'s `lua_process_one_event_writing_resource`).
pub const LUA_RESOURCE_WRITE_SCRIPT: &str = r#"
function process(event)
  resource["service.name"] = "nginx"
  return event
end
"#;

/// Reads `event.log.trace_id` on every call: the `event.log` proxy's cost
/// (`crates/logit-script/src/proxy.rs`'s `LogProxy`; `tests/allocations.rs`'s
/// `lua_process_one_event_reading_log_trace`).
pub const LUA_LOG_TRACE_READ_SCRIPT: &str = r#"
function process(event)
  local _ = event.log.trace_id
  return event
end
"#;

/// Reads `event.metrics[1].value` on every call: the `event.metrics` surface's cost
/// (`crates/logit-script/src/proxy.rs`'s `MetricsProxy`/`MetricProxy`; `tests/allocations.rs`'s
/// `lua_process_one_event_reading_metric_value`, whose doc says why the read is discarded rather
/// than written back).
pub const LUA_METRIC_VALUE_READ_SCRIPT: &str = r#"
function process(event)
  local _ = event.metrics[1].value
  return event
end
"#;

/// Reads `#event.metrics` (`MetaMethod::Len`) only, with no indexing, isolating `MetricsProxy`'s
/// creation-and-caching cost from `MetricProxy`'s per-index one (`tests/allocations.rs`'s
/// `lua_process_one_event_reading_metric_len`).
pub const LUA_METRIC_LEN_READ_SCRIPT: &str = r#"
function process(event)
  local _ = #event.metrics
  return event
end
"#;

/// Reads `event.span.name` on every call: the `event.span` surface's cost
/// (`crates/logit-script/src/proxy.rs`'s `SpanProxy`; `tests/allocations.rs`'s
/// `lua_process_one_event_reading_span_name`).
pub const LUA_SPAN_NAME_READ_SCRIPT: &str = r#"
function process(event)
  local _ = event.span.name
  return event
end
"#;

/// Reads `scope.name` on every call: the batch-level `scope` global's cost
/// (`crates/logit-script/src/scope.rs`; `tests/allocations.rs`'s
/// `lua_process_one_event_reading_scope_name`).
pub const LUA_SCOPE_NAME_READ_SCRIPT: &str = r#"
function process(event)
  local _ = scope.name
  return event
end
"#;

/// Writes `scope.attributes.k` on every call: the first-write copy-on-write path
/// (`crates/logit-script/src/scope.rs`'s `ensure_modified`; `tests/allocations.rs`'s
/// `lua_process_one_event_writing_scope_attribute`).
pub const LUA_SCOPE_ATTR_WRITE_SCRIPT: &str = r#"
function process(event)
  scope.attributes.k = "v"
  return event
end
"#;

/// Writes `resource.schema_url` on every call: the named-field write path, separate from the
/// attribute map (`crates/logit-script/src/resource.rs`'s `write_schema_url`;
/// `tests/allocations.rs`'s `lua_process_one_event_writing_resource_schema_url`).
pub const LUA_RESOURCE_SCHEMA_URL_WRITE_SCRIPT: &str = r#"
function process(event)
  resource.schema_url = "https://example.com/schema"
  return event
end
"#;

/// Assigns `scope.name = scope.name` on every call: an identity write that
/// `crates/logit-script/src/scope.rs`'s no-op check must catch before `ensure_modified`
/// (`tests/allocations.rs`'s `lua_process_one_event_identity_write_to_scope_name_is_free`).
pub const LUA_SCOPE_IDENTITY_NAME_SCRIPT: &str = r#"
function process(event)
  scope.name = scope.name
  return event
end
"#;

/// Drops the incoming event and returns one minted through `Event.new`
/// (`crates/logit-script/src/construct.rs`, `docs/adr/lua-event-constructor.md`): one attribute and
/// a minimal log (`tests/allocations.rs`'s `lua_process_one_event_constructing_a_log_event`).
pub const LUA_EVENT_NEW_LOG_SCRIPT: &str = r#"
function process(event)
  return Event.new{timestamp = "1", attributes = {env = "prod"}, log = {message = "hi"}}
end
"#;

/// As [`LUA_EVENT_NEW_LOG_SCRIPT`], minting one `gauge` record with only its required fields
/// (`tests/allocations.rs`'s `lua_process_one_event_constructing_a_gauge_event`).
pub const LUA_EVENT_NEW_GAUGE_SCRIPT: &str = r#"
function process(event)
  return Event.new{timestamp = "1", metrics = {{name = "tick", kind = "gauge", value = 1}}}
end
"#;

/// As [`LUA_EVENT_NEW_LOG_SCRIPT`], minting a span from its three required fields only, so every
/// core default applies (`kind` internal, `status` unset, `end_timestamp` the event's own, no
/// `SpanExt`, empty `events`/`links`; `tests/allocations.rs`'s
/// `lua_process_one_event_constructing_a_span_event`).
pub const LUA_EVENT_NEW_SPAN_SCRIPT: &str = r#"
function process(event)
  return Event.new{timestamp = "1", span = {trace_id = "4bf92f3577b34da6a3ce929d0e0e4736", span_id = "00f067aa0ba902b7", name = "GET /"}}
end
"#;

/// A metric-only event carrying one counter `Sum`, as [`kv_metrics`]'s `nginx.requests` spec
/// produces it: what the Lua `event.metrics` surface is measured against (`tests/allocations.rs`'s
/// `lua_process_one_event_reading_metric_value`/`_reading_metric_len`). Empty, inline attributes,
/// so its `Event::clone` is free.
pub fn sum_metric_event() -> Event {
    Event::metric(
        0,
        AttrMap::new(),
        MetricRecord::new(logit_core::interner::intern("nginx.requests"), MetricKind::counter(1.0)),
    )
}

/// The batch-level `scope` (OTLP's `InstrumentationScope`) `run_lua` installs before a batch's
/// events reach `process` (`crates/logit-script/src/scope.rs`): non-empty `name`/`version`, no
/// attributes.
///
/// `name`/`version` are `Bytes::from_static`: a `'static` `Bytes` has no one-time promotion (see
/// [`cached_message`]) to leak into `lua_process_one_event_writing_scope_attribute`'s measured
/// first-write clone.
pub fn scope() -> Arc<Scope> {
    Arc::new(Scope {
        name: Bytes::from_static(b"nginx-otel-module"),
        version: Bytes::from_static(b"1.0.0"),
        ..Scope::default()
    })
}

/// The identity function: no `.attributes`, `.log`, `.metrics`, `.span`, `resource`, or `scope`
/// access. It creates no proxy, so nothing holds a second strong reference to the event's
/// `Rc<RefCell<Event>>` and `EventProxy::into_inner`'s `Rc::try_unwrap` fast path always succeeds.
/// That isolates a fixture's own shape cost from any proxy's first-access cost
/// (`tests/allocations.rs`'s `lua_process_one_event_passthrough_on_a_spilled_event`).
pub const LUA_PASSTHROUGH_SCRIPT: &str = r#"
function process(event)
  return event
end
"#;

/// [`sum_metric_event`] with a *spilled* 9-attribute map (past `AttrMap`'s 8 inline slots), so an
/// `Event::clone` allocates instead of being a free memcpy.
///
/// Guards `MetricProxy`'s `Weak<RefCell<Event>>` field (`crates/logit-script/src/proxy.rs`). If
/// that field held a strong `Rc`, a not-yet-collected `event.metrics[i]` temporary would make
/// `EventProxy::into_inner`'s `Rc::try_unwrap` fail and fall back to `Event::clone`, which only a
/// spilled event makes visible to an exact allocation count (`tests/allocations.rs`'s
/// `lua_process_one_event_reading_metric_value_on_a_spilled_event`).
pub fn sum_metric_event_with_spilled_attributes() -> Event {
    let mut attributes = AttrMap::new();
    for i in 0..9 {
        attributes.insert(&format!("tag{i}"), format!("value{i}").as_str());
    }
    Event::metric(
        0,
        attributes,
        MetricRecord::new(logit_core::interner::intern("nginx.requests"), MetricKind::counter(1.0)),
    )
}

// -------------------------------------------------------------------------------------------
// Logs-only: a plain-text syslog line with no JSON body
// -------------------------------------------------------------------------------------------

/// A plain-text syslog line with no JSON body: the logs-only workload (attributes plus a `log`, no
/// `json` anywhere in the pipeline).
///
/// RFC 3164, with the header shape of RFC 3164 §5.4's example (`<34>` = facility `auth`(4),
/// severity `crit`(2)), carrying an sshd authentication failure as rsyslog would forward it, with
/// **both** hostname and `tag[pid]:`. Unlike [`NGINX_SYSLOG_LINE`]'s `nohostname` shape, this
/// takes `parse_3164`'s other header branch (as `syslog.rs`'s
/// `rfc3164_with_hostname_decodes_message_severity_and_attributes` test does) and yields six
/// attributes (`syslog.facility`/`severity`/`timestamp`/`hostname`/`tag`/`pid`), inside the
/// measured syslog range (`docs/design/data-shapes.md` §6).
///
/// Hand-written: no live syslogd was captured. It's derived from `syslog.rs`'s decoder and tests.
pub const SSHD_SYSLOG_LINE: &str = "<34>Aug 31 06:52:01 auth-edge-3 sshd[8843]: Failed password \
     for invalid user admin from 203.0.113.7 port 54321 ssh2";

/// `count` copies of [`SSHD_SYSLOG_LINE`] newline-separated -- the logs-only counterpart to
/// [`nginx_syslog_datagram`].
pub fn logs_only_syslog_datagram(count: usize) -> Bytes {
    join_lines(SSHD_SYSLOG_LINE, count)
}

/// A `regex` with three named captures over [`SSHD_SYSLOG_LINE`]'s auth-failure text
/// (`docs/adr/regex-transform.md`).
pub fn regex_parser() -> RegexParser {
    RegexParser::new(
        r"for invalid user (?P<ssh_user>\S+) from (?P<client_address>\S+) port (?P<client_port>\d+)",
        None,
    )
    .expect("fixture pattern should compile")
}

/// A bare log event whose message is [`SSHD_SYSLOG_LINE`] and no attributes, so [`regex_parser`]'s
/// three captures land inside `AttrMap`'s 8 inline slots (`tests/allocations.rs`'s
/// `regex_capture_into_an_inline_map`).
///
/// The message is `Bytes::from_static`, not `Value::str`: like a message sliced out of a decoder's
/// buffer, it has no one-time promotion (see [`cached_message`]) to charge to `regex`.
pub fn sshd_message_event() -> Event {
    Event::log(
        0,
        AttrMap::new(),
        LogRecord {
            message: Value::Str(Bytes::from_static(SSHD_SYSLOG_LINE.as_bytes())),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// [`SSHD_SYSLOG_LINE`] decoded by `syslog_in`: six `syslog.*` attributes, so [`regex_parser`]'s
/// three captures spill `AttrMap` past its 8 inline slots (`tests/allocations.rs`'s
/// `regex_parse_one_event`).
pub fn sshd_event() -> Event {
    let mut decoder = syslog_decoder();
    let batch = decoder.decode(logs_only_syslog_datagram(1)).expect("fixture line should decode");
    batch.events.into_iter().next().expect("fixture line should produce one event")
}

// -------------------------------------------------------------------------------------------
// Wide JSON: a flat log line with 25-30 fields, well past AttrMap's inline capacity
// -------------------------------------------------------------------------------------------

/// A wide, flat JSON log line: `syslog_in -> json` with 28 top-level fields against
/// [`NGINX_SYSLOG_LINE`]'s six, spilling `AttrMap` well past its 8 inline slots. At 32 attributes
/// after `json` it's an access-log or audit-log width, wider than any application logging library
/// measured (`docs/design/data-shapes.md` §6); [`FLAT_JSON_LOG_BODY`] is the application-log one.
///
/// Hand-written, modelled on pino's documented default fields (`level`, `time`, `pid`, `hostname`,
/// `msg`) plus the request, timing, trace, and deployment fields an Express+pino service
/// (`pino-http` and similar) adds per request. No live pino process was captured.
///
/// Wrapped in [`NGINX_SYSLOG_LINE`]'s `nohostname`/tagged RFC 3164 envelope (`<134>`: facility
/// `local0`(16), severity `info`(6)), so decoding yields four `syslog.*` attributes plus the 28
/// JSON fields once `json` merges them.
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

/// A metrics-only event carrying five *distinct* distributions and three inline attributes: a
/// request handler timing several internal operations (cache lookup, DB query, external call, ...),
/// or a scrape reporting several histograms at once. The distribution-heavy side of the "`Box` the
/// `DdSketch`" trade, which costs +1 allocation per distribution (`docs/design/memory.md` §1 and
/// §8 item 10).
///
/// Directly constructed: statsd emits one metric per line, so several sketches on one event is a
/// post-collection shape no decoder produces.
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
            unit: Some(logit_core::interner::intern("ms")),
            ..MetricRecord::new(
                logit_core::interner::intern(name),
                MetricKind::Distribution(sketch),
            )
        });
    }
    event
}

// -------------------------------------------------------------------------------------------
// Spans
// -------------------------------------------------------------------------------------------

/// A directly-constructed span, wrapped as `Event::span(...)`.
///
/// Hand-built against `crates/logit-core/src/span.rs` (`docs/design/memory.md`'s "Fixtures"
/// section, pattern 2). A payload captured through `otlp_in` could replace it.
///
/// Modelled on a server span for one HTTP request: a parent span, two [`SpanEvent`]s (a cache miss,
/// then a slow query), and one [`SpanLink`] to a related trace, so it exercises every `SpanRecord`
/// field. Its 4 attributes and each event's or link's 1-2 all stay inline, so its clone cost
/// (`clone_span_event`, 2 allocations) is the two `Vec`s (`events`, `links`) alone. That's this
/// shape, not spans in general: [`wide_server_span_event`] is the spilled-map complement.
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
                dropped_attributes_count: 0,
            },
            SpanEvent {
                timestamp: 1_725_091_200_070_000_000,
                name: Value::str("db.slow_query"),
                attributes: slow_query_attrs,
                dropped_attributes_count: 0,
            },
        ],
        links: vec![SpanLink {
            trace_id: [0xCD; 16],
            span_id: [0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27],
            attributes: link_attrs,
            flags: 0,
            trace_state: None,
            dropped_attributes_count: 0,
        }],
        end_timestamp: 1_725_091_200_090_000_000,
        flags: 0,
        ext: None,
    };
    Event::span(1_725_091_200_000_000_000, attrs, record)
}

/// One Prometheus text 0.0.4 scrape body, hand-written after the metric names and shapes Node
/// Exporter and application scrapes carry (`docs/design/telemetry-landscape.md`'s "Prometheus
/// exposition format / OpenMetrics" section): two counter families, one gauge, one histogram, and
/// one summary. That's 11 series, since a histogram's buckets and a summary's quantiles are one
/// composite series each (`logit_proto::prometheus::Point`), and every kind
/// `prometheus_decode_one_scrape` (`tests/allocations.rs`) has to walk.
pub const PROMETHEUS_SCRAPE_BODY: &str = concat!(
    "# HELP http_requests_total Total HTTP requests processed.\n",
    "# TYPE http_requests_total counter\n",
    "http_requests_total{method=\"get\",code=\"200\"} 1027\n",
    "http_requests_total{method=\"get\",code=\"404\"} 3\n",
    "http_requests_total{method=\"post\",code=\"200\"} 512\n",
    "http_requests_total{method=\"post\",code=\"500\"} 2\n",
    "# HELP node_cpu_seconds_total Seconds the CPUs spent in each mode.\n",
    "# TYPE node_cpu_seconds_total counter\n",
    "node_cpu_seconds_total{cpu=\"0\",mode=\"idle\"} 8523.4\n",
    "node_cpu_seconds_total{cpu=\"0\",mode=\"user\"} 102.4\n",
    "node_cpu_seconds_total{cpu=\"1\",mode=\"idle\"} 8501.9\n",
    "node_cpu_seconds_total{cpu=\"1\",mode=\"user\"} 110.2\n",
    "# HELP node_memory_MemAvailable_bytes Memory available for starting new applications.\n",
    "# TYPE node_memory_MemAvailable_bytes gauge\n",
    "node_memory_MemAvailable_bytes 3.1826688e+09\n",
    "# HELP request_duration_seconds Request latency.\n",
    "# TYPE request_duration_seconds histogram\n",
    "request_duration_seconds_bucket{le=\"0.1\"} 1200\n",
    "request_duration_seconds_bucket{le=\"0.5\"} 1900\n",
    "request_duration_seconds_bucket{le=\"1\"} 1980\n",
    "request_duration_seconds_bucket{le=\"+Inf\"} 2000\n",
    "request_duration_seconds_sum 412.5\n",
    "request_duration_seconds_count 2000\n",
    "# HELP go_gc_duration_seconds A summary of the wall-time pause across GC cycles.\n",
    "# TYPE go_gc_duration_seconds summary\n",
    "go_gc_duration_seconds{quantile=\"0\"} 0.0001\n",
    "go_gc_duration_seconds{quantile=\"0.25\"} 0.0003\n",
    "go_gc_duration_seconds{quantile=\"0.5\"} 0.0006\n",
    "go_gc_duration_seconds{quantile=\"1\"} 0.0021\n",
    "go_gc_duration_seconds_sum 0.412\n",
    "go_gc_duration_seconds_count 812\n",
);

pub fn prometheus_decoder() -> PrometheusDecoder {
    PrometheusDecoder::new()
}

pub fn prometheus_encoder() -> PrometheusEncoder {
    PrometheusEncoder::new()
}

/// `count` gauge series under one family, each with its own `shard` label, for
/// `prometheus_encode_100_series` (`tests/allocations.rs`). One label each, since encoding cost is
/// dominated by the number of series walked, not by any one series' label count.
pub fn prometheus_gauge_events(count: usize) -> Vec<Event> {
    (0..count)
        .map(|i| {
            let mut attributes = AttrMap::new();
            attributes.insert("shard", format!("{i}").as_str());
            Event::metric(
                0,
                attributes,
                MetricRecord::new(
                    logit_core::interner::intern("prom_bench_gauge"),
                    MetricKind::Gauge(i as f64),
                ),
            )
        })
        .collect()
}

/// The instant every remote-write fixture stamps, in whole milliseconds: the only resolution either
/// version carries.
const REMOTE_WRITE_TIMESTAMP_MS: i64 = 1_700_000_000_000;

/// [`prometheus_gauge_events`]'s workload already mapped to a `MetricFamily`, so
/// `remote_write_encode_100_series_v1`/`_v2` (`tests/allocations.rs`) measure the protobuf half
/// alone; the exposition rows already pin `events_to_families`.
pub fn remote_write_families(count: usize) -> Vec<logit_proto::prometheus::MetricFamily> {
    use logit_proto::prometheus::{FamilyType, MetricFamily, Point, Series};

    let series = (0..count)
        .map(|i| Series {
            timestamp: Some(REMOTE_WRITE_TIMESTAMP_MS * 1_000_000),
            ..Series::new(vec![("shard".to_string(), i.to_string())], Point::Gauge(i as f64))
        })
        .collect();
    vec![MetricFamily {
        help: Some("Bench gauge.".to_string()),
        series,
        ..MetricFamily::new("prom_bench_gauge", FamilyType::Gauge)
    }]
}

/// One **uncompressed** remote-write 1.0 request carrying 100 gauge series of one family, as a
/// sender with `max_samples_per_send` well under its default produces
/// (`remote_write_decode_one_request_v1`, `tests/allocations.rs`).
///
/// Built from the vendored `prometheus.WriteRequest` types, not through
/// `logit_proto::prometheus::remote_write::encode`, so the decode measurement isn't circular.
/// Labels are in the byte order both specs require of a sender (`__name__` before `shard`, since
/// `_` is `0x5f`).
pub fn remote_write_request_v1() -> Vec<u8> {
    use logit_proto::prometheus::generated::prometheus as pb;
    use prost::Message;

    let timeseries = (0..100)
        .map(|i| pb::TimeSeries {
            labels: vec![
                pb::Label { name: "__name__".to_string(), value: "prom_bench_gauge".to_string() },
                pb::Label { name: "shard".to_string(), value: i.to_string() },
            ],
            samples: vec![pb::Sample { value: i as f64, timestamp: REMOTE_WRITE_TIMESTAMP_MS }],
            ..Default::default()
        })
        .collect();
    pb::WriteRequest {
        timeseries,
        metadata: vec![pb::MetricMetadata {
            r#type: pb::metric_metadata::MetricType::Gauge as i32,
            metric_family_name: "prom_bench_gauge".to_string(),
            help: "Bench gauge.".to_string(),
            unit: String::new(),
        }],
    }
    .encode_to_vec()
}

/// [`remote_write_request_v1`]'s series and values in remote-write 2.0, through the symbol table
/// and the per-series `Metadata` that replaces `metadata[]`. Symbol `0` is the mandatory empty
/// string; label values dominate the table, as in a real 2.0 request.
pub fn remote_write_request_v2() -> Vec<u8> {
    use logit_proto::prometheus::generated::io::prometheus::write::v2 as pb;
    use prost::Message;

    // "", "__name__", "prom_bench_gauge", "shard", "Bench gauge.", then one per shard value.
    let mut symbols = vec![
        String::new(),
        "__name__".to_string(),
        "prom_bench_gauge".to_string(),
        "shard".to_string(),
        "Bench gauge.".to_string(),
    ];
    let first_shard = symbols.len() as u32;
    let timeseries = (0..100u32)
        .map(|i| {
            symbols.push(i.to_string());
            pb::TimeSeries {
                labels_refs: vec![1, 2, 3, first_shard + i],
                samples: vec![pb::Sample {
                    value: f64::from(i),
                    timestamp: REMOTE_WRITE_TIMESTAMP_MS,
                    start_timestamp: 0,
                }],
                metadata: Some(pb::Metadata {
                    r#type: pb::metadata::MetricType::Gauge as i32,
                    help_ref: 4,
                    unit_ref: 0,
                }),
                ..Default::default()
            }
        })
        .collect();
    pb::Request { symbols, timeseries }.encode_to_vec()
}

/// A collectd like-relay event: `collectd.host`/`plugin`/`type`/`interval` and one gauge record.
/// `collectd_out`'s encoder fast-paths this into one Values part (`logit_proto::collectd`'s module
/// doc, the "like-relay" row); an event without `collectd.type` takes the per-record fallback.
pub fn collectd_event() -> Event {
    let mut attributes = AttrMap::new();
    attributes.insert(logit_proto::collectd::ATTR_HOST, Value::str("fixture-host"));
    attributes.insert(logit_proto::collectd::ATTR_PLUGIN, sstr("load"));
    attributes.insert(logit_proto::collectd::ATTR_TYPE, sstr("load"));
    attributes.insert(logit_proto::collectd::ATTR_INTERVAL, Value::F64(10.0));
    // Positive: `collectd_out`'s encoder drops a `timestamp <= 0` event (`nanos_to_cdtime` reads
    // it as "no time given"), so `0` would silently encode nothing.
    let mut event = Event::empty(1_700_000_000_000_000_000, attributes);
    event
        .metrics
        .push(MetricRecord::new(logit_core::interner::intern("load.load"), MetricKind::Gauge(0.5)));
    event
}

/// `count` copies of [`collectd_event`] in one batch, for `collectd_out`'s encoder
/// (`tests/allocations.rs`'s "collectd_out: encode_into 100 events").
pub fn collectd_batch(count: usize) -> EventBatch {
    let event = collectd_event();
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count).map(|_| event.clone()).collect(),
    }
}

pub fn collectd_encoder() -> logit_proto::collectd::CollectdEncoder {
    logit_proto::collectd::CollectdEncoder::new()
}

/// A gauge event with a positive timestamp. `graphite_out`'s encoder drops any record whose
/// timestamp floors to a non-positive second (`crates/logit-proto/src/graphite/encode.rs`'s drop
/// table), so no graphite fixture can reuse a `ts: 0` fixture such as [`distribution_event`].
pub fn graphite_event() -> Event {
    Event::metric(
        1_700_000_000_000_000_000,
        AttrMap::new(),
        MetricRecord::new(logit_core::interner::intern("app.requests"), MetricKind::Gauge(42.0)),
    )
}

/// `count` copies of [`graphite_event`] in one batch, for `graphite_out`'s encoder in both wire
/// protocols (`tests/allocations.rs`'s "graphite_out: encode_into 100 ... events").
pub fn graphite_batch(count: usize) -> EventBatch {
    let event = graphite_event();
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count).map(|_| event.clone()).collect(),
    }
}

pub fn graphite_encoder() -> logit_proto::graphite::GraphiteEncoder {
    logit_proto::graphite::GraphiteEncoder::new()
}

/// [`distribution_event`] with a positive timestamp (see [`graphite_event`]).
pub fn graphite_distribution_event() -> Event {
    let mut sketch = DdSketch::new();
    sketch.add(0.004);
    Event::metric(
        1_700_000_000_000_000_000,
        AttrMap::new(),
        MetricRecord::new(
            logit_core::interner::intern("nginx.request_time"),
            MetricKind::Distribution(sketch),
        ),
    )
}

/// `count` copies of [`graphite_distribution_event`], for `graphite_out`'s `multi_value: expand`
/// path on a kind whose expansion allocates nothing beyond the caller's `Vec<Event>` (unlike
/// [`graphite_samples_batch`]'s `Samples::sketch()`).
pub fn graphite_distribution_batch(count: usize) -> EventBatch {
    let event = graphite_distribution_event();
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count).map(|_| event.clone()).collect(),
    }
}

/// `count` copies of [`samples_event`]'s shape with a positive timestamp (see [`graphite_event`]),
/// for `graphite_out`'s `multi_value: expand` path on the one kind whose expansion builds a fresh
/// `DdSketch` per record (`Samples::sketch()`, a cost `influxdb_out` shares).
pub fn graphite_samples_batch(count: usize) -> EventBatch {
    let event = Event::metric(
        1_700_000_000_000_000_000,
        AttrMap::new(),
        MetricRecord::new(
            logit_core::interner::intern("nginx.request_time"),
            MetricKind::Samples(Samples::new([0.004])),
        ),
    );
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count).map(|_| event.clone()).collect(),
    }
}

/// The log body of the nginx-shaped event both `generate_in` fixtures render, all-literal. The
/// event also carries one `host` attribute, one `requests` counter, and a `service.name` resource
/// (see [`generate_input`]).
///
/// `{{`/`}}` are `logit_core::template`'s escape for a literal brace, so the rendered body is the
/// JSON it looks like.
const GENERATE_LOG_LITERAL: &str = r#"{{"method":"GET","path":"/x/0","status":200,"bytes":1024}}"#;

/// The same body with `{seq%50}` in its path -- one templated field.
const GENERATE_LOG_TEMPLATED: &str =
    r#"{{"method":"GET","path":"/x/{seq%50}","status":200,"bytes":1024}}"#;

/// One builder for both `generate_in` fixtures, so they differ only by [`generate_templated`]'s
/// two placeholders and the gap between their allocation counts is templating alone
/// (`docs/design/memory.md` §2's `generate_in` rows).
fn generate_input(log: &str, host: &str) -> GenerateInput {
    let template = |raw: &str| logit_core::template::parse(raw).expect("a fixture template parses");
    GenerateInput::new(None, 100)
        .with_resource(std::collections::BTreeMap::from([(
            "service.name".to_string(),
            "synthetic".to_string(),
        )]))
        .expect("a literal resource value always compiles")
        .with_log(template(log))
        .expect("a fixture template names only known placeholders")
        .with_attribute("host", template(host))
        .expect("a fixture template names only known placeholders")
        .with_metric(template("requests"), GenerateMetricKind::Sum, 1.0)
        .expect("a literal metric name always compiles")
}

/// A `generate_in` on its **prototype** render path: no placeholder, so one event is rendered once
/// and `clone`d per generated event with only `timestamp` overwritten
/// (`crates/logit-inputs/src/generate.rs`'s module doc).
pub fn generate_literal() -> GenerateInput {
    generate_input(GENERATE_LOG_LITERAL, "web-1")
}

/// A `generate_in` on its **per-event** render path, with two templated fields (`{seq%50}` in the
/// log body, `{seq%10}` in `host`): the gap from [`generate_literal`]'s count is what two
/// placeholders cost per event.
pub fn generate_templated() -> GenerateInput {
    generate_input(GENERATE_LOG_TEMPLATED, "web-{seq%10}")
}

// -------------------------------------------------------------------------------------------
// Survey-derived shapes (docs/design/data-shapes.md §7, follow-up 2)
// -------------------------------------------------------------------------------------------
//
// Six shapes the data-shape survey asked for (`docs/design/memory.md`'s "Fixtures" section has
// the table). All are **modelled, not captured**: each doc comment names the survey row it sits
// on and which part is the model's own. The survey's numbers aren't production traffic either
// (its §0 and §7), and these inherit that caveat.
//
// Three are `tail_in`-shaped log events carrying a JSON body, run through the real `json`
// transform, so the code under measurement produces the width they pin. `tail_in` stamps one
// attribute, `log.file.path` (`crates/logit-inputs/src/tail/line.rs`), and the survey's §5.3
// counts come from a `shape` tap after `json` on that leg, so the path is part of every width.
// `perf/scenarios/json-parse-{app,nested,access}-log.yaml` render the same bodies verbatim.

/// The attribute `tail_in` stamps on every line it reads (`crates/logit-inputs/src/tail/line.rs`),
/// so part of every width `docs/design/data-shapes.md` §5.3 tabulates.
const TAIL_PATH_KEY: &str = "log.file.path";

/// A `Value::Str` over a `'static` literal, never a `Bytes::from(String)`.
///
/// Every directly-constructed survey fixture uses this rather than [`Value::str`]: a `Value::str`
/// pays a one-time promotion per string (see [`cached_message`]) in whatever region first clones
/// it, which for these shapes is the `Event::clone` measurement itself. A value off the wire is
/// already a shared slice of a decoder's buffer; `Bytes::from_static` behaves the same.
fn sstr(literal: &'static str) -> Value {
    Value::Str(Bytes::from_static(literal.as_bytes()))
}

/// A log event with `body` as its message and only [`TAIL_PATH_KEY`] as an attribute: the shape
/// `tail_in` hands `json`, so `json.process` over it produces the *total* width
/// `docs/design/data-shapes.md` §5.3 tabulates (the library's keys plus the path).
///
/// The message comes from [`cached_message`], so the one-time `Bytes` promotion lands on the
/// warm-up call.
fn tailed_json_event(body: &'static str, cache: &'static OnceLock<Bytes>) -> Event {
    let mut attributes = AttrMap::new();
    attributes.insert(TAIL_PATH_KEY, sstr("/var/log/app/app.log"));
    Event::log(
        1_725_091_200_123_000_000,
        attributes,
        LogRecord {
            message: Value::Str(cached_message(body, cache)),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// **The commonest measured log shape**: eleven flat JSON fields which, merged onto `tail_in`'s
/// `log.file.path`, make a **12-attribute** (spilled) event. That's the p50 in
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §5.3 for both Go
/// `log/slog`'s `JSONHandler` (12 / max 13) and structlog's documented production recipe (12 / 13),
/// and the middle of §2's "after `json` it is 9-14" band.
///
/// Modelled on `log/slog`'s `JSONHandler` because its arithmetic reproduces the measured number:
/// slog's default line is **3** fields (`time`, `level`, `msg`; `log/slog`'s `handler.go`,
/// `docs/design/data-shapes-rows.md` §A), the survey's apps each logged **8** access fields on top,
/// and `tail_in` adds **1**: 3 + 8 + 1 = 12. Key and value lengths follow §2's measured bands:
/// keys 3-11 bytes (median 6, against a measured median of 4-9), string values 3-36 bytes with one
/// user-agent in the tail (median 14, against a measured median of 10-16 and a p90 of 26-37).
///
/// **Modelled, not captured.** No slog process was run: the envelope is slog's documented default
/// output, and the eight access fields follow the survey's description of what its apps logged
/// ("method, path, status, duration and the like"). [`WIDE_JSON_SYSLOG_LINE`] is the access-log
/// width, not this.
pub const FLAT_JSON_LOG_BODY: &str = concat!(
    r#"{"time":"2026-09-07T06:52:01.123456789Z","level":"INFO","msg":"request completed","#,
    r#""method":"POST","path":"/api/v1/orders","status":201,"duration_ms":18.4,"#,
    r#""bytes":842,"remote_addr":"198.51.100.23","request_id":"c3f7a1e2-9b44-4f0a-8c2d-11f2a9d40abc","#,
    r#""user_agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}"#
);

/// A `tail_in`-shaped log event carrying [`FLAT_JSON_LOG_BODY`] -- one attribute before `json`,
/// **12 after**.
pub fn flat_json_log_event() -> Event {
    static MESSAGE: OnceLock<Bytes> = OnceLock::new();
    tailed_json_event(FLAT_JSON_LOG_BODY, &MESSAGE)
}

/// The **nested** log shape: pino-http's completion record, whose request serializers make it
/// *narrower* at the top and deeper underneath.
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §5.3 measures it at 9
/// attributes (max 10) after `json` with **4 nested maps, median width 3, depth 2**. Each
/// `Value::Map` is a boxed `AttrMap` paying the full inline footprint again
/// (`crates/logit-core/src/value.rs`), which is why §6 says nested maps "multiply whatever is
/// chosen".
///
/// Nine top-level keys, the desk row's count (`docs/design/data-shapes-rows.md` §A,
/// pino-http@v11): pino's five (`level`, `time`, `msg`, `pid`, `hostname`) plus `reqId`,
/// `responseTime`, `req{}` and `res{}`. With `tail_in`'s path that's **10 event attributes**, the
/// measured maximum (the measured p50 of 9 is the same record on a leg with no path attribute,
/// such as `docker_in` or OTLP).
///
/// Four maps at depth 2: `req{}` and `res{}` each nest a `headers{}`, matching how
/// `crates/logit-transforms/src/shape.rs` counts (maps recursively, `value_depth` as the deepest
/// container chain), so the measured 4 / 3 / 2 reproduces. **The per-map widths are the model's
/// own**: the survey reports percentiles pooled over all maps, so three keys each is a choice
/// consistent with its median of 3. The `req`/`res` field lists come from `pino-std-serializers`'
/// documented output.
///
/// **Modelled, not captured**: no pino-http process was run. Misconfiguring pino-http's
/// destination silently drops the serializers and the record then carries kilobytes of raw socket
/// internals; this models the configured, narrow shape.
pub const PINO_HTTP_LOG_BODY: &str = concat!(
    r#"{"level":30,"time":1725091200123,"pid":4821,"hostname":"api-7c9f8d6b5-abcde","#,
    r#""reqId":"req-8461","#,
    r#""req":{"method":"POST","url":"/api/v1/orders","#,
    r#""headers":{"host":"shop.example.com","content-type":"application/json","content-length":"842"}},"#,
    r#""res":{"statusCode":201,"#,
    r#""headers":{"content-type":"application/json","content-length":"57","vary":"Accept-Encoding"}},"#,
    r#""responseTime":18,"msg":"request completed"}"#
);

/// A `tail_in`-shaped log event carrying [`PINO_HTTP_LOG_BODY`] -- one attribute before `json`,
/// **10 after**, four of which are (or contain) boxed `AttrMap`s.
pub fn pino_http_log_event() -> Event {
    static MESSAGE: OnceLock<Bytes> = OnceLock::new();
    tailed_json_event(PINO_HTTP_LOG_BODY, &MESSAGE)
}

/// The widest, highest-rate log class in the survey, which
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §7 flags as having **no
/// capture behind it**: "Edge and access-log streams (15-34 fields, the highest event rates) rest
/// on Counted rows."
///
/// **PostgreSQL's `jsonlog`**, 29 keys (`docs/design/data-shapes-rows.md` §A, `T5b#C4`, counted
/// against PostgreSQL's `runtime-config-logging` §19.8.4-5). Chosen because `demo/logit.yaml`'s
/// `postgres_in` already tails this format, so it's a width a config in this repository produces.
/// With `tail_in`'s `log.file.path` that's a **30-attribute** event, mid-band (ALB's 34 and
/// CloudFront's 33, the other candidates §2 counts, would sit one `realloc` further along
/// [the growth ladder](../../tests/allocations.rs)).
///
/// **All 29 keys at once.** PostgreSQL emits the error-detail keys (`detail`, `hint`,
/// `internal_query`, `context`, `statement`, ...) only on lines that have them, so this is the
/// format's full width, its widest line, not its median one; that's what the "desk-counted class"
/// row means. **Modelled, not captured**: no PostgreSQL instance was run, and the values are
/// plausible rather than recorded.
pub const POSTGRES_JSONLOG_BODY: &str = concat!(
    r#"{"timestamp":"2026-09-07 06:52:01.123 UTC","user":"orders_app","dbname":"orders","#,
    r#""pid":4821,"remote_host":"10.0.0.17","remote_port":54871,"#,
    r#""session_id":"68bd2f41.12d5","line_num":142,"ps":"INSERT","#,
    r#""session_start":"2026-09-07 06:40:11 UTC","vxid":"4/2841","txid":"918273","#,
    r#""error_severity":"ERROR","state_code":"23505","#,
    r#""message":"duplicate key value violates unique constraint \"orders_pkey\"","#,
    r#""detail":"Key (id)=(12345) already exists.","hint":"Retry with a fresh identifier.","#,
    r#""internal_query":"INSERT INTO orders (id) VALUES ($1)","internal_position":13,"#,
    r#""context":"PL/pgSQL function place_order(integer) line 8 at SQL statement","#,
    r#""statement":"SELECT place_order(12345)","cursor_position":8,"#,
    r#""func_name":"_bt_check_unique","file_name":"nbtinsert.c","file_line_num":666,"#,
    r#""application_name":"orders-api","backend_type":"client backend","#,
    r#""query_id":-3491082746118273645,"leader_pid":4788}"#
);

/// A `tail_in`-shaped log event carrying [`POSTGRES_JSONLOG_BODY`] -- one attribute before `json`,
/// **30 after**.
pub fn access_log_event() -> Event {
    static MESSAGE: OnceLock<Bytes> = OnceLock::new();
    tailed_json_event(POSTGRES_JSONLOG_BODY, &MESSAGE)
}

/// A **17-attribute HTTP server span**: the measured ceiling
/// ([`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §6, "a span at the 16-17
/// ceiling"). [`span_event`] stays inline and measures the `events`/`links` `Vec`s; this one
/// spills and measures the map.
///
/// Attributes are named from the OpenTelemetry HTTP semantic conventions. §4 counts a conforming
/// HTTP server span at **16** by default (3 required + 7 conditionally required + 6 recommended)
/// and measures the OpenTelemetry Demo at **p50 8, p90 17, max 18** over 114,551 spans; Java's
/// agent, Go's `otelhttp` and .NET's AspNetCore land at 16, 16 and 17 unconfigured. This sits at
/// the p90: `network.transport`, a recommended attribute real agents set, is the seventeenth.
///
/// **No span events and no links**, which is the measured finding: §4 reports 76% of demo spans
/// with no events, and **no span in 114,551 with a link**. So this fixture's clone cost is its
/// spilled map alone, and [`span_event`]'s is two `Vec`s with no spill.
///
/// **Modelled, not captured**: the names are the conventions', but no OTLP payload was recorded;
/// it's built by hand against `crates/logit-core/src/span.rs` (`docs/design/memory.md`'s
/// "Fixtures" section, pattern 2).
pub fn wide_server_span_event() -> Event {
    let mut attrs = AttrMap::new();
    // Required (3).
    attrs.insert("http.request.method", sstr("POST"));
    attrs.insert("url.path", sstr("/api/v1/orders"));
    attrs.insert("url.scheme", sstr("https"));
    // Conditionally required (7).
    attrs.insert("http.response.status_code", Value::I64(201));
    attrs.insert("http.route", sstr("/api/v1/orders"));
    attrs.insert("network.protocol.version", sstr("1.1"));
    attrs.insert("server.address", sstr("shop.example.com"));
    attrs.insert("server.port", Value::I64(443));
    attrs.insert("url.query", sstr("notify=true"));
    attrs.insert("client.address", sstr("198.51.100.23"));
    // Recommended (6).
    attrs.insert("client.port", Value::I64(54871));
    attrs.insert("network.peer.address", sstr("10.0.0.17"));
    attrs.insert("network.peer.port", Value::I64(443));
    attrs.insert("network.protocol.name", sstr("http"));
    attrs.insert("user_agent.original", sstr("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"));
    attrs.insert("url.full", sstr("https://shop.example.com/api/v1/orders?notify=true"));
    // The seventeenth: from the spec's default 16 to the measured p90.
    attrs.insert("network.transport", sstr("tcp"));

    let record = SpanRecord {
        trace_id: [0xAB; 16],
        span_id: [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08],
        parent_span_id: Some([0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17]),
        name: sstr("POST /api/v1/orders"),
        kind: SpanKind::Server,
        status: SpanStatus::Ok,
        events: Vec::new(),
        links: Vec::new(),
        end_timestamp: 1_725_091_200_090_000_000,
        flags: 0,
        ext: None,
    };
    Event::span(1_725_091_200_000_000_000, attrs, record)
}

/// The **only measured `MetricList` spill**: one collectd value list carrying three data sources.
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §3 measures 16,590 events
/// under a default plugin set: **17.4% carry 2 records, 0.4% carry 3**, none more.
/// Every other metric input emits one metric per event, so this shape decides whether
/// `MetricList`'s single inline slot costs anything (`docs/design/memory.md` §8 item 13).
///
/// Six attributes, collectd's measured width: §3 reports p50 = p90 = max = **6**, and six is the
/// full `collectd.*` identity set the decoder stamps (`crates/logit-proto/src/collectd/mod.rs`):
/// host, plugin, plugin instance, type, type instance, interval. So the map stays *inline* while
/// the metric list spills, the inverse of every log fixture above.
///
/// Built by hand rather than decoded from [`collectd_load_packet`], which yields the same three
/// records but only four attributes (no instances). The record names are what `CollectdDecoder`
/// produces for a three-source list with a `types.db` attached: `<plugin>.<type>.<data source>`
/// (`collectd/decode.rs`).
///
/// **Modelled, not captured, and one key is the model's own.** `load` is collectd's canonical
/// three-source type and its `relative` type instance is real (the `load` plugin's
/// `ReportRelative`), but a real `load` list has no *plugin* instance, so a decoded one has five
/// attributes. The survey measures record count (3) and width (6) over the same corpus without
/// saying they co-occur; this fixture crosses them. Anyone reading a per-attribute cost off it
/// should know the sixth key is the fixture's, not collectd's.
pub fn collectd_three_record_event() -> Event {
    let mut attributes = AttrMap::new();
    attributes.insert(logit_proto::collectd::ATTR_HOST, sstr("web-1"));
    attributes.insert(logit_proto::collectd::ATTR_PLUGIN, sstr("load"));
    attributes.insert(logit_proto::collectd::ATTR_PLUGIN_INSTANCE, sstr("0"));
    attributes.insert(logit_proto::collectd::ATTR_TYPE, sstr("load"));
    attributes.insert(logit_proto::collectd::ATTR_TYPE_INSTANCE, sstr("relative"));
    attributes.insert(logit_proto::collectd::ATTR_INTERVAL, Value::F64(10.0));

    let mut event = Event::empty(1_700_000_000_000_000_000, attributes);
    for (name, value) in
        [("load.load.shortterm", 0.1), ("load.load.midterm", 0.2), ("load.load.longterm", 0.3)]
    {
        event
            .metrics
            .push(MetricRecord::new(logit_core::interner::intern(name), MetricKind::Gauge(value)));
    }
    event
}

/// A **17-attribute `Resource`**: the collector-enriched identity
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §4 measured at **min 10,
/// median 17, p90 28, max 29** per batch through the OpenTelemetry Demo's collector with
/// `resource_detection` on. Past 8 inline slots, it allocates on construction and on every clone,
/// once per batch (§6: "`Resource` is the consumer that is already always spilled").
///
/// The names are the conventions' identity groups as a collector fills them: `service.*` (the
/// SDK's default resource), `telemetry.sdk.*` (every SDK sets them), `k8s.*` (what
/// `k8sattributes` adds: §4 counts 6 by default and 30 fully enabled), plus host, container and
/// cloud attributes from `resourcedetection`. **Modelled, not captured**: the survey measured
/// counts, not keys, and "Kubernetes enrichment is Counted, not Measured" (§7), so these are a
/// plausible 17 of the conventions' 38, not a recorded set.
pub fn enriched_resource() -> Arc<Resource> {
    let mut attributes = AttrMap::new();
    for (key, value) in [
        ("service.name", "orders-api"),
        ("service.namespace", "shop"),
        ("service.version", "3.4.1"),
        ("service.instance.id", "c3f7a1e2-9b44-4f0a-8c2d-11f2a9d40abc"),
        ("telemetry.sdk.name", "opentelemetry"),
        ("telemetry.sdk.language", "go"),
        ("telemetry.sdk.version", "1.38.0"),
        ("k8s.cluster.name", "prod-1"),
        ("k8s.namespace.name", "shop"),
        ("k8s.pod.name", "orders-api-7c9f8d6b5-abcde"),
        ("k8s.pod.uid", "9f21c8e4-1b3d-4a7e-9c02-6d5f4b1a8e30"),
        ("k8s.node.name", "ip-10-0-3-17.ec2.internal"),
        ("k8s.deployment.name", "orders-api"),
        ("k8s.container.name", "orders-api"),
        ("container.id", "a1b2c3d4e5f60718293a4b5c6d7e8f90"),
        ("host.name", "ip-10-0-3-17"),
        ("cloud.region", "us-east-1"),
    ] {
        attributes.insert(key, sstr(value));
    }
    Arc::new(Resource { attributes, dropped_attributes_count: 0, schema_url: None })
}

/// One OpenTelemetry log record at its measured median shape
/// ([`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §2: "a median of 9
/// attributes (max 11) through the demo's collector, ... with a median body of 84 bytes"), almost
/// never nested (268 of 36,524 records). Nine attributes is one past `AttrMap`'s inline capacity:
/// the median OpenTelemetry log record spills, by one. The body is 84 bytes.
///
/// **Modelled, not captured**: the counts are measured, the key names are the conventions'.
fn otlp_log_record_event(index: usize) -> Event {
    let mut attributes = AttrMap::new();
    attributes.insert("code.function", sstr("placeOrder"));
    attributes.insert("code.namespace", sstr("shop.orders"));
    attributes.insert("log.iostream", sstr("stdout"));
    attributes.insert("thread.id", Value::I64(42));
    attributes.insert("thread.name", sstr("http-nio-8080-exec-3"));
    attributes.insert("http.request.method", sstr("POST"));
    attributes.insert("http.route", sstr("/api/v1/orders"));
    attributes.insert("http.response.status_code", Value::I64(201));
    attributes.insert("order.id", Value::I64(index as i64));
    Event::log(
        1_725_091_200_123_000_000 + index as i64,
        attributes,
        LogRecord {
            // 84 bytes: the measured median body length.
            message: sstr(
                "order placed: id=0000 customer=shop/eu-west total=42.50 currency=EUR status=OK ",
            ),
            severity: Some(logit_core::Severity::Info),
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 1_725_091_200_123_000_000,
            dropped_attributes_count: 0,
        },
    )
}

/// **Five events sharing a 17-attribute [`enriched_resource`]**: the batch shape
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §3 measured for a
/// collector export (median **5** events per batch, p90 16) carrying §4's median resource.
///
/// A `Resource` is `Arc`-shared across a batch (`crates/logit-core/src/event.rs`), so its spilled
/// map is paid once per five events, and an `EventBatch::clone` (`docs/design/memory.md` §3's
/// copy-on-write path) clones the events but only bumps the resource's `Arc`. The fixture
/// separates the two places an `AttrMap` is paid: five per-event maps at the median width (9, one
/// past inline, from [`otlp_log_record_event`]) and one wider resource map that isn't cloned with
/// the batch.
pub fn enriched_resource_batch() -> EventBatch {
    EventBatch {
        resource: enriched_resource(),
        scope: None,
        events: (0..5).map(otlp_log_record_event).collect(),
    }
}

// -------------------------------------------------------------------------------------------
// http_access (docs/adr/http-access-normalization.md)
// -------------------------------------------------------------------------------------------

/// One nginx access line in the shape of `fixtures/nginx/nginx.conf`'s `access_semconv` format:
/// raw semconv attribute names and untouched values, mixing atomic and composite fields so most of
/// `http_access`'s steps run at once. A raw `url.original` (composite), a string-encoded status
/// beside bare-numeric sizes, an `_s`-suffixed duration already in its target unit, an upstream
/// leg, a W3C `traceparent`, and a Chrome desktop User-Agent (`http_access.rs`'s corpus-verified
/// browser row). Hand-written values, not a capture.
pub const HTTP_ACCESS_SEMCONV_LINE: &str = concat!(
    r#"{"http.request.method":"GET","#,
    r#""url.original":"/api/v1/orders?page=2&limit=20","#,
    r#""url.scheme":"https","#,
    r#""network.protocol.version":"HTTP/1.1","#,
    r#""http.response.status_code":"200","#,
    r#""http.response.body.size":612,"#,
    r#""http.request.size":348,"#,
    r#""http.request.duration_s":0.084,"#,
    r#""server.address":"api.example.com","#,
    r#""client.address":"203.0.113.42","#,
    r#""user_agent.original":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","#,
    r#""http.request.header.referer":"https://example.com/dashboard","#,
    r#""upstream.address":"10.0.0.5:8080","#,
    r#""upstream.status":"200","#,
    r#""upstream.duration_s":"0.012","#,
    r#""traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","#,
    r#""span.end_s":"1758000000.123"}"#
);

/// [`HTTP_ACCESS_SEMCONV_LINE`] with every key in its dashed spelling (`url-original`,
/// `http-response-status_code`, ...), same values and order, as HAProxy's `%{+json}o` logs it.
/// `traceparent` has no `.` to dash; `trace_context`, not `http_access`, reads it.
pub const HTTP_ACCESS_DASHED_LINE: &str = concat!(
    r#"{"http-request-method":"GET","#,
    r#""url-original":"/api/v1/orders?page=2&limit=20","#,
    r#""url-scheme":"https","#,
    r#""network-protocol-version":"HTTP/1.1","#,
    r#""http-response-status_code":"200","#,
    r#""http-response-body-size":612,"#,
    r#""http-request-size":348,"#,
    r#""http-request-duration_s":0.084,"#,
    r#""server-address":"api.example.com","#,
    r#""client-address":"203.0.113.42","#,
    r#""user_agent-original":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36","#,
    r#""http-request-header-referer":"https://example.com/dashboard","#,
    r#""upstream-address":"10.0.0.5:8080","#,
    r#""upstream-status":"200","#,
    r#""upstream-duration_s":"0.012","#,
    r#""traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","#,
    r#""span-end_s":"1758000000.123"}"#
);

/// [`HTTP_ACCESS_SEMCONV_LINE`] with one control byte in `user_agent.original`: a JSON `\u0001`
/// escape, which `json` decodes to a `0x01` byte. The one case `http_access`'s step 7 clean can't
/// slice.
pub const HTTP_ACCESS_CONTROL_BYTE_LINE: &str = concat!(
    r#"{"http.request.method":"GET","#,
    r#""url.original":"/api/v1/orders?page=2&limit=20","#,
    r#""url.scheme":"https","#,
    r#""network.protocol.version":"HTTP/1.1","#,
    r#""http.response.status_code":"200","#,
    r#""http.response.body.size":612,"#,
    r#""http.request.size":348,"#,
    r#""http.request.duration_s":0.084,"#,
    r#""server.address":"api.example.com","#,
    r#""client.address":"203.0.113.42","#,
    r#""user_agent.original":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36\u0001","#,
    r#""http.request.header.referer":"https://example.com/dashboard","#,
    r#""upstream.address":"10.0.0.5:8080","#,
    r#""upstream.status":"200","#,
    r#""upstream.duration_s":"0.012","#,
    r#""traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01","#,
    r#""span.end_s":"1758000000.123"}"#
);

/// [`HTTP_ACCESS_SEMCONV_LINE`] run once through the real [`JsonParser`], as a
/// `syslog_in -> json -> http_access` pipeline hands it over: every string attribute is a
/// zero-copy slice of the JSON body, not a fresh `Value::str`.
pub fn http_access_event() -> Event {
    static MESSAGE: OnceLock<Bytes> = OnceLock::new();
    let mut event = Event::log(
        1_758_000_000,
        AttrMap::new(),
        LogRecord {
            message: Value::Str(cached_message(HTTP_ACCESS_SEMCONV_LINE, &MESSAGE)),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    assert!(json_parser().process(&resource(), &mut event), "fixture line must parse as JSON");
    event
}

/// [`http_access_event`] parsed from [`HTTP_ACCESS_DASHED_LINE`], for `http_access`'s de-alias
/// step (`http_access_normalizes_a_dashed_line_warm`).
pub fn http_access_dashed_event() -> Event {
    static MESSAGE: OnceLock<Bytes> = OnceLock::new();
    let mut event = Event::log(
        1_758_000_000,
        AttrMap::new(),
        LogRecord {
            message: Value::Str(cached_message(HTTP_ACCESS_DASHED_LINE, &MESSAGE)),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    assert!(json_parser().process(&resource(), &mut event), "fixture line must parse as JSON");
    event
}

/// [`http_access_event`] parsed from [`HTTP_ACCESS_CONTROL_BYTE_LINE`], whose control byte
/// `http_access`'s cap-and-clean step must rewrite (`http_access_cleans_a_control_byte`).
pub fn http_access_event_with_control_byte() -> Event {
    static MESSAGE: OnceLock<Bytes> = OnceLock::new();
    let mut event = Event::log(
        1_758_000_000,
        AttrMap::new(),
        LogRecord {
            message: Value::Str(cached_message(HTTP_ACCESS_CONTROL_BYTE_LINE, &MESSAGE)),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    assert!(json_parser().process(&resource(), &mut event), "fixture line must parse as JSON");
    event
}

/// `http_access` at the demo's configuration: the two builtin route sets, two operator patterns
/// (`/work`, `/`), a catch-all `route_other`, and the full `logit_config::CAPPED_FIELDS` cap list,
/// resolved as `logit-cli`'s `to_http_access_config` does rather than copied. No user-agent rules,
/// no extra `redact_query`, `trust_forwarded: false`.
pub fn http_access() -> HttpAccess {
    let config = HttpAccessConfig {
        routes: vec![
            RouteRule::Builtin(RouteSet::Probes),
            RouteRule::Builtin(RouteSet::Assets),
            RouteRule::Pattern { pattern: "^/work$".to_string(), route: "/work".to_string() },
            RouteRule::Pattern { pattern: "^/$".to_string(), route: "/".to_string() },
        ],
        route_other: Some("/{other}".to_string()),
        user_agent_rules: vec![],
        max_length: logit_config::CAPPED_FIELDS
            .iter()
            .map(|(field, cap)| (field.to_string(), *cap))
            .collect(),
        redact_query: vec![],
        trust_forwarded: false,
    };
    HttpAccess::new(config).expect("fixture patterns should compile")
}
