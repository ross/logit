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
    JsonParser, Keep, KeepValues, Kv, KvMetrics, Logfmt, MetricSpec, Normalize, RegexParser, Set,
    Shape,
};
use std::sync::{Arc, OnceLock};
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

/// The same shape as [`STATSD_LINE`], except one tag key (`team`) repeats -- the wire shape
/// `insert_tags` folds into a `Value::Array` in wire order
/// (`crates/logit-inputs/src/statsd.rs`'s "DogStatsD tags" section) rather than the last-token-wins
/// collapse it used to be. Keeping the plain `env:prod` tag alongside the repeated one means the
/// allocation this is measured against is the repeated key's own cost on top of an otherwise
/// ordinary tagged counter, not a worst case with nothing else going on.
pub const STATSD_REPEATED_TAG_LINE: &str = "page.views:1|c|#env:prod,team:a,team:b";

/// [`STATSD_REPEATED_TAG_LINE`]'s same repeated tag key, on a multi-value counter line
/// (`name:v1:v2:v3|c`): `parse_line` decodes this to three `Event`s sharing one `AttrMap`, and
/// `build_event` clones that map once per value (`crates/logit-inputs/src/statsd.rs`'s
/// `build_event` doc) -- so the repeated tag's `Value::Array`, a real `Vec` spine rather than a
/// refcounted slice of the datagram, is deep-cloned once per value event, not once total.
pub const STATSD_MULTI_VALUE_REPEATED_TAG_LINE: &str = "page.views:1:2:3|c|#env:prod,team:a,team:b";

/// A statsd distribution (`ms`) line at the default, unsampled rate -- the baseline
/// [`STATSD_SAMPLED_DISTRIBUTION_LINE`]'s allocation count is measured against. Decodes straight
/// to a raw `MetricKind::Samples` now (`docs/adr/lossless-transit.md`'s W3,
/// `crates/logit-inputs/src/statsd.rs`) -- no `DdSketch`, no decode-time sample-rate
/// extrapolation; only `aggregate` sketches these.
pub const STATSD_DISTRIBUTION_LINE: &str = "request.latency:120|ms";

/// The same line as [`STATSD_DISTRIBUTION_LINE`], sampled at `@0.1` -- the raw `sample_rate` now
/// rides verbatim on the decoded `Samples` (`docs/adr/lossless-transit.md`'s W3: no decode-time
/// extrapolation any more, `crates/logit-inputs/src/statsd.rs`).
pub const STATSD_SAMPLED_DISTRIBUTION_LINE: &str = "request.latency:120|ms|@0.1";

/// A statsd set (`s`) line -- decodes to one [`logit_core::MetricKind::SetMembers`] event, a
/// single zero-copy member slice of the datagram.
pub const STATSD_SET_LINE: &str = "unique.users:abc123|s";

/// A DogStatsD event (`_e{tlen,xlen}:title|text|...`) line whose `TEXT` has nothing to unescape --
/// the docs' own canonical event example (`crates/logit-inputs/src/statsd.rs`'s
/// `dogstatsd_docs_example_event_decodes`) -- so `parse_event`'s `unescape_event_text` takes its
/// zero-copy path, the same `slice_of`-backed slicing every other statsd field here gets.
pub const STATSD_EVENT_LINE: &str =
    "_e{21,36}:An exception occurred|Cannot parse CSV file from 10.0.0.17|t:warning|#err_type:bad_file";

/// The same shape as [`STATSD_EVENT_LINE`], except `TEXT` contains one `\n` (backslash, `n`)
/// escape -- the one case `unescape_event_text` can't slice, since the decoded message needs a
/// real newline byte the wire text doesn't have. Isolates that one extra allocation
/// (`Bytes::from(raw.replace(...))`) from the zero-copy baseline [`STATSD_EVENT_LINE`] measures.
pub const STATSD_EVENT_LINE_WITH_ESCAPED_NEWLINE: &str = "_e{5,12}:title|line1\\nline2";

/// A DogStatsD service check (`_sc|name|status|...`) line -- the docs' own canonical example
/// (`crates/logit-inputs/src/statsd.rs`'s `dogstatsd_docs_example_service_check_decodes`).
/// Decodes to one [`logit_core::MetricKind::Gauge`] event carrying the
/// `statsd.service_check.*` carriers alongside it.
pub const STATSD_SERVICE_CHECK_LINE: &str =
    "_sc|Redis connection|2|#env:dev|m:Redis connection timed out after 10s";

/// A logfmt-shaped log line (go-kit style), used to exercise the quoted-value scan path.
pub const LOGFMT_LINE: &str = "level=info ts=2026-09-07T06:52:01Z caller=metrics.go:159 \
    component=frontend org_id=fake latency=fast duration=12.3ms status=200 \
    msg=\"query stats\"";

/// The same shape with an escaped quote inside the quoted value. Isolates the one path that
/// cannot slice.
pub const LOGFMT_ESCAPED_LINE: &str = "level=info query=\"{job=\\\"nginx\\\"}\" status=200";

/// nginx-ish `a=1&b=2`, the `kv` shape.
pub const KV_LINE: &str = "a=1&b=2&c=hello";

/// `count` copies of [`NGINX_SYSLOG_LINE`] newline-separated, as one UDP datagram would arrive.
///
/// `count = 1` is the honest single-line cost. Larger counts matter because the decoder amortizes
/// one `Bytes` allocation and one `now_nanos()` across the whole datagram, and because every field
/// of every event ends up a refcounted slice of this one buffer -- the retention behavior
/// `docs/design/memory.md` describes.
pub fn nginx_syslog_datagram(count: usize) -> Bytes {
    join_lines(NGINX_SYSLOG_LINE, count)
}

/// [`NGINX_SYSLOG_LINE`] with the same six JSON keys in the reverse order -- a producer that
/// serialises its fields differently from the one `json` warmed up on. Exists for
/// `json_parse_reordered_keys_event` (`tests/allocations.rs`): the parser's key cache must
/// resynchronise on a reordered line without allocating.
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

/// The same decoder with the short hand-written `types.db` fixture attached
/// (`logit_proto::collectd::types_db`), so `load`'s three data sources resolve to
/// `shortterm`/`midterm`/`longterm` instead of `0`/`1`/`2`. Pairs with [`collectd_decoder`] to show
/// the lookup costs nothing per list (`docs/design/memory.md` §2).
pub fn collectd_decoder_with_types_db() -> CollectdDecoder {
    let types_db = Arc::new(TypesDb::parse(TEST_TYPES_DB).expect("the fixture types.db parses"));
    CollectdDecoder::new(resource()).with_types_db(types_db)
}

/// A collectd datagram of `lists` single-GAUGE value lists, packed the way collectd's own sender
/// packs them: the identity is written once and **elided** on every list after the first, each
/// subsequent list carrying only a TypeInstance part to distinguish it plus its Values part. That
/// elision is the whole point of the measurement -- a 25-list datagram is what a real host agent
/// sends, and its per-list cost is what `decode_into` has to keep flat.
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

/// One three-data-source `load`/`load` list -- the multi-value shape whose records spill
/// `MetricList`'s inline capacity of 1, and the one a `types.db` actually renames.
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
/// fixture the codec built for itself would stop being an independent statement of the wire format
/// the moment that code changed.
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

/// The same decoder reading carbon's pickle batch protocol instead of its plaintext lines. Pairs
/// with [`graphite_pickle_frame`] to show what the *other* wire costs for the same datapoints
/// (`docs/design/memory.md` §2).
pub fn graphite_pickle_decoder() -> GraphiteDecoder {
    GraphiteDecoder::new(resource()).with_protocol(GraphiteProtocol::Pickle)
}

/// A carbon plaintext datagram of `lines` datapoints -- `path value timestamp\n`, the whole of the
/// wire format. `graphite_in` hands `decode_into` exactly this shape under both transports: a UDP
/// datagram, or a TCP read's worth of complete lines.
pub fn graphite_datagram(lines: usize) -> Bytes {
    let mut text = String::new();
    for index in 0..lines {
        text.push_str(&format!("servers.web-1.cpu.core{index} 0.5 1700000000\n"));
    }
    Bytes::from(text)
}

/// One tagged line, carbon 1.1+'s `;k=v` syntax -- the shape whose tag values the decoder slices
/// zero-copy out of this very buffer, which is the property the measurement exists to pin.
pub fn graphite_tagged_datagram() -> Bytes {
    Bytes::from_static(b"servers.web-1.cpu;env=prod;region=us-east 0.5 1700000000\n")
}

/// One carbon pickle **payload** of `datapoints` datapoints: `[(path, (timestamp, value)), ...]` at
/// protocol 2, with no 4-byte length prefix.
///
/// Unframed on purpose -- that is exactly what [`GraphiteDecoder`] is handed. Carbon's framing (a
/// big-endian `u32` payload length, Twisted's `Int32StringReceiver`) belongs to the *listener*,
/// which validates and strips it before calling `decode_into`
/// (`crates/logit-inputs/src/tcp.rs`'s `Framer`), so a fixture carrying one would measure a prefix no
/// decoder ever sees.
///
/// The opcodes are written out by hand rather than through
/// `logit_proto::graphite::pickle::write_datapoints`, for [`collectd_part_header`]'s reason: a
/// fixture the codec built for itself stops being an independent statement of the wire format the
/// moment that code changes. Protocol 2, matching what `pickle.dumps(..., protocol=2)` -- carbon's
/// own documented example -- emits.
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

/// `skip_to_brace` off, matching `examples/nginx-to-influxdb.yaml` -- the syslog decoder has
/// already stripped the header, so the whole message really is the JSON body.
pub fn json_parser() -> JsonParser {
    JsonParser::new(false)
}

/// Caches one [`bytes::Bytes`] per distinct `line`, built once (via `f`) and `.clone()`d on every
/// call after that -- the fixture-side mirror of [`nginx_syslog_datagram`]'s own pattern, where a
/// test holds one base `Bytes` in a local and clones it for both the warm-up and the measured call
/// so the allocation counter only ever sees the *second-or-later* clone. That matters here
/// specifically because `bytes::Bytes` defers its shared, atomically-refcounted representation
/// until a buffer is *first* cloned or sliced (`bytes-1.x`'s `promotable_{even,odd}_clone` ->
/// `shallow_clone_vec`, a real, `#[cold]`, one-time `Box<Shared>` allocation) -- a `Bytes` built
/// fresh from a `&str`/`Vec<u8>` on every call (as a naive `logfmt_event()` did originally) pays
/// that promotion on *every* call's first clone, since each call's buffer is a distinct,
/// never-before-shared allocation. Memoizing here, rather than changing `logfmt_event`'s zero-arg
/// signature, keeps every call after the first returning a `.clone()` of the *same* already-shared
/// buffer -- the identical "warm the thing being measured" discipline this crate already applies
/// to the interner (`docs/design/memory.md`'s "Fixtures" section).
fn cached_message(line: &'static str, cache: &'static OnceLock<Bytes>) -> Bytes {
    cache.get_or_init(|| Bytes::copy_from_slice(line.as_bytes())).clone()
}

/// A directly-constructed log event whose message is [`LOGFMT_LINE`] -- no decoder needed, since
/// `logfmt`/`kv` read `event.log.message` directly (`docs/design/memory.md`'s "Fixtures" section).
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

/// One line of a CSV access log -- seven columns, one a quoted request line containing the
/// delimiter, exercising the quoted path rather than a simplified one.
pub const CSV_ACCESS_LINE: &str = "10.0.0.1,2026-09-07T06:52:01Z,GET,\"/a,b\",200,612,0.012";
/// The header row [`CSV_ACCESS_LINE`]'s columns would render as -- for exercising the
/// header-row-recognition path (`docs/adr/csv-positional-columns.md`).
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

/// The sixteen-column schema [`CSV_WIDE_LINE`] matches, comma-delimited. Column names are
/// `field0`..`field15`, deliberately distinct from `CSV_WIDE_LINE`'s own single-letter values --
/// naming the columns `a`..`p` to match the data would make the header line and a data row
/// byte-identical, tripping the header-row-recognition path this fixture isn't meant to exercise.
pub fn csv_wide_parser() -> CsvParser {
    CsvParser::new((0..16).map(|i| format!("field{i}")).collect(), b',')
}

/// One log event whose message is `line` -- the shape `csv` reads (a `LogRecord`, no attributes
/// pre-populated), for measuring `CsvParser::process` in isolation the same way [`json_parser`]'s
/// callers measure `JsonParser::process` starting from a decoded event.
///
/// Every other input fixture in this file hands `process` a message `Bytes` that was already
/// cloned or sliced at least once during (unmeasured) decode -- `bytes::Bytes`'s `Vec`-backed
/// representation lazily promotes to an atomically-refcounted one on its *first* `clone`/`slice`
/// call, a one-time allocation. A message built straight from a fresh `Bytes::from(String)` and
/// handed to `process` untouched would pay that promotion cost on the very first clone inside
/// `process` itself, measuring the fixture's own construction rather than the transform's real
/// per-event cost -- so this clones the message once before it's ever seen by a transform, the
/// same "already decoded" starting shape [`nginx_event`]/[`statsd_event`] get from a real decoder.
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

/// The `bounded` component from the reference example: clamps `host` to the two real vhosts,
/// lowercasing first (`crates/logit-bench/tests/allocations.rs`'s `keep_values_one_event`/
/// `keep_values_one_event_needs_lowering`).
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

/// [`nginx_event`] with its `host` attribute uppercased -- [`nginx_event`]'s own `host` is already
/// `static.local` (`NGINX_SYSLOG_LINE`), so this is what forces `keep_values`' `normalize: [lower]`
/// step to actually allocate, for `keep_values_one_event_needs_lowering`.
pub fn nginx_event_with_uppercase_host() -> Event {
    let mut event = nginx_event();
    event.attributes.insert("host", Value::str("STATIC.LOCAL"));
    event
}

/// The `tap` component from [`examples/shape-tap.yaml`](../../../examples/shape-tap.yaml), with
/// every field at its default -- `resource: drop`, both caps at 4096 -- and a name, so the
/// measured path is the one a real config builds, `tap` tag included
/// (`docs/adr/shape-observer-component.md`).
pub fn shape() -> Shape {
    Shape::new(Duration::from_secs(10)).with_name("tap")
}

/// A `flatten` at its defaults (`attributes: all`, `resource: none`, `arrays: index`) -- the
/// common case, for `crates/logit-bench/tests/allocations.rs`'s `flatten_*` measurements.
pub fn flatten() -> Flatten {
    Flatten::new(Fields::All, Fields::None, Arrays::Index)
}

/// A `set` configured with one attribute pair and no resource pairs -- the per-event-only path
/// (`crates/logit-bench/tests/allocations.rs`'s `set_attributes_one_event`).
pub fn set_attributes() -> Set {
    Set::new(vec![], vec![("env".to_string(), Value::str("prod"))])
}

/// A `set` configured with one resource pair and no attribute pairs -- for measuring
/// `map_resource`'s one-entry cache (`crates/logit-bench/tests/allocations.rs`'s
/// `set_resource_cached_batch_costs_nothing`).
pub fn set_resource() -> Set {
    Set::new(vec![("service.name".to_string(), Value::str("nginx"))], vec![])
}

/// A `has_attributes` matching [`nginx_event`]'s `status` attribute -- configured as `I64(200)`
/// against `NGINX_SYSLOG_LINE`'s JSON-sourced `status` (a `U64`, per `serde_json`'s handling of an
/// unsigned literal), so this fixture deliberately exercises `value_matches`' cross-variant
/// coercion rather than an exact-type match (`crates/logit-bench/tests/allocations.rs`'s
/// `has_attributes_one_event`).
pub fn has_attributes() -> logit_transforms::HasAttributes {
    logit_transforms::HasAttributes::new(vec![], vec![("status".to_string(), Value::I64(200))])
}

/// [`has_attributes`]'s exact complement, same config
/// (`crates/logit-bench/tests/allocations.rs`'s `drop_attributes_one_event`).
pub fn drop_attributes() -> logit_transforms::DropAttributes {
    logit_transforms::DropAttributes::new(vec![], vec![("status".to_string(), Value::I64(200))])
}

/// A `has_attributes` matching on [`resource`] instead of an event's own attributes -- for
/// measuring the resource-match cache's hit and miss costs
/// (`crates/logit-bench/tests/allocations.rs`'s `has_attributes_resource_match_cache_hit`/`_miss`).
pub fn has_attributes_resource() -> logit_transforms::HasAttributes {
    logit_transforms::HasAttributes::new(
        vec![("service.name".to_string(), Value::str("nginx"))],
        vec![],
    )
}

/// A `has_attributes` matching [`nginx_event_with_stream`]'s `stream` attribute against one value
/// -- the "today's shape" half of the route-vs-fan-out comparison
/// (`crates/logit-bench/tests/allocations.rs`'s `// Routing` section,
/// `docs/adr/target-components.md`): one of these per branch is what a `stream: host`/`stream:
/// app` fan-out pair looks like without a `route`/target.
pub fn has_attributes_stream(value: &str) -> logit_transforms::HasAttributes {
    logit_transforms::HasAttributes::new(vec![], vec![("stream".to_string(), Value::str(value))])
}

/// A `trace_context` configured to lift `trace_id` only (no `span_id`/`flags`, `keep_source:
/// false`) -- the common case, for `crates/logit-bench/tests/allocations.rs`'s
/// `trace_context_lifts_a_valid_trace_id`.
pub fn trace_context() -> logit_transforms::TraceContext {
    logit_transforms::TraceContext::new("trace_id".to_string(), None, None, false)
}

/// A `trace_context` with the convention defaults and a `span:` block (`kind: server`, `name:
/// http.request`, no minting) -- the shape `demo/logit.yaml`'s `haproxy_trace`/`nginx_trace` run,
/// for `crates/logit-bench/tests/allocations.rs`'s `trace_context_mints_a_span_from_the_convention`.
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
/// emits them after `json`: an inbound `traceparent`, this hop's own `trace.id`/`span.id`, and
/// nginx's ms-resolution `span.end_s` (`$msec`) / `span.duration_s` (`$request_time`) as JSON
/// floats (`F64` off `serde_json`). The receipt timestamp is set just after the line's `span.end_s`
/// so the fixture sits inside the default `max_skew` window regardless of the wall clock.
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

/// Like [`aggregator`], with cross-flush series retention enabled -- for measuring the retained
/// path's own allocation cost (`aggregate_flush_retained_gauges`,
/// `crates/logit-bench/tests/allocations.rs`), which the default (`series_retention: 0`) fixture
/// above never exercises.
pub fn aggregator_with_series_retention(retention: u32, max_retained: usize) -> Aggregator {
    Aggregator::new(Duration::from_secs(10)).with_series_retention(retention, max_retained)
}

/// Like [`aggregator_with_series_retention`], in `temporality: cumulative` mode -- for measuring
/// what a retained *counter* series costs per flush
/// (`aggregate_flush_cumulative_sums`, `crates/logit-bench/tests/allocations.rs`), the accumulator
/// shape only that mode ever retains (`docs/adr/aggregation-window-semantics.md`'s cumulative
/// amendment).
pub fn aggregator_cumulative(retention: u32, max_retained: usize) -> Aggregator {
    Aggregator::new(Duration::from_secs(10))
        .with_temporality(AggregateTemporality::Cumulative)
        .with_series_retention(retention, max_retained)
}

/// Like [`aggregator`], with `distributions: samples` and the given cap -- for measuring the raw
/// `Samples`-retention path's own allocation cost
/// (`aggregate_absorb_25_samples_values_into_one_series_samples_mode`, `crates/logit-bench/tests/
/// allocations.rs`), which the default (`distributions: sketch`) fixture above never exercises.
pub fn aggregator_with_samples_retention(max_samples_per_series: usize) -> Aggregator {
    Aggregator::new(Duration::from_secs(10))
        .with_distributions(Distributions::Samples, max_samples_per_series)
}

/// A metric-only event carrying one `MetricKind::Samples` record -- the raw shape statsd's
/// `ms`/`h`/`d` timings decode to since W3 (`crates/logit-inputs/src/statsd.rs`,
/// `docs/plans/lossless-transit.md`), used to measure what `aggregate` pays to absorb one
/// (`aggregate_absorb_one_samples_event_sketch_mode`/
/// `aggregate_absorb_25_samples_values_into_one_series_samples_mode`,
/// `crates/logit-bench/tests/allocations.rs`). Unsampled (`sample_rate: 1.0`, `Samples::new`'s
/// default) -- these measurements are about the absorb path's own allocation shape, not
/// `Samples::weight`'s clamping.
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

/// One event as it looks leaving `kv_metrics` -- decoded, JSON-merged, four metrics attached.
/// This is the widest the event ever gets in the reference pipeline (~10 attributes, 4 metrics)
/// and therefore the shape whose clone cost fan-out actually pays.
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

/// [`nginx_event`] with a `stream` attribute added -- the `route`/`has_attributes` split fixture
/// for `crates/logit-bench/tests/allocations.rs`'s `// Routing` section
/// (`docs/adr/target-components.md`): the headline central-collector topology switches on exactly
/// this kind of tag.
pub fn nginx_event_with_stream(stream: &str) -> Event {
    let mut event = nginx_event();
    event.attributes.insert("stream", Value::str(stream));
    event
}

/// `count` [`nginx_event_with_stream`] events in one batch, alternating `"host"`/`"app"` -- the
/// same 64-event split the ADR's route-vs-fan-out comparison measures both ways
/// (`crates/logit-bench/tests/allocations.rs`'s `// Routing` section).
pub fn nginx_batch_alternating_stream(count: usize) -> EventBatch {
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count)
            .map(|i| nginx_event_with_stream(if i % 2 == 0 { "host" } else { "app" }))
            .collect(),
    }
}

/// A metric-only event of the shape `statsd_in` produces: one counter, a handful of tags, no log
/// and no span. The cheap end of the event-size range, against which `nginx_event` is the
/// expensive end.
pub fn statsd_event() -> Event {
    let mut decoder = statsd_decoder();
    let batch = decoder.decode(statsd_datagram(1)).expect("fixture line should decode");
    batch.events.into_iter().next().expect("fixture line should produce one event")
}

/// `count` copies of [`statsd_event`] in one batch -- [`nginx_batch`]'s metric-only twin, for
/// measuring `statsd_out`'s encoder against the shape it actually relays.
pub fn statsd_batch(count: usize) -> EventBatch {
    let event = statsd_event();
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count).map(|_| event.clone()).collect(),
    }
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
            unit: Some(logit_core::interner::intern("s")),
            ..MetricRecord::new(
                logit_core::interner::intern("nginx.request_time"),
                MetricKind::Distribution(sketch),
            )
        },
    )
}

/// A gauge metric event with a *spilled* (12, past `AttrMap`'s 8-slot inline capacity, and
/// deliberately un-`keep`ed) attribute map -- the shape `aggregate_flush_retained_gauges`
/// (`crates/logit-bench/tests/allocations.rs`) uses to pin the real cost of a retained series'
/// `key.attributes.clone()`, where the clone is a genuine heap allocation rather than the memcpy
/// `aggregate_flush_100_series`' `keep`-trimmed fixture gets away with.
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

/// [`wide_gauge_event`]'s counter twin -- same spilled 12-attribute map, a delta `Sum` instead of a
/// `Gauge`, so `aggregate_flush_cumulative_sums` (`crates/logit-bench/tests/allocations.rs`)
/// measures the retained-*counter* flush path (`temporality: cumulative`) against exactly the same
/// attribute shape the retained-gauge measurement uses, and the two numbers are comparable.
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

/// A directly-constructed log event in the pino-http completion-record shape
/// `docs/design/data-shapes-rows.md`'s survey row measured: pino's 5 flat fields
/// (`level`/`time`/`msg`/`pid`/`hostname`) plus `reqId`/`responseTime` (flat) and `req`/`res`
/// (nested `Value::Map`s), each of which itself nests a `headers` map -- four boxed `AttrMap`s per
/// event at depth 2, `docs/design/data-shapes.md`'s headline finding about this shape
/// (`crates/logit-bench/tests/allocations.rs`'s `flatten_*` measurements, the one fixture in this
/// module built to have something for `flatten` to do). Field values are illustrative, not a
/// captured record -- `docs/design/data-shapes.md` §7 is explicit that none of this survey is
/// production traffic.
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

/// Writes `resource` on every call -- for measuring what a script that stamps a resource identity
/// (`crates/logit-script/src/resource.rs`, `docs/adr/operator-declared-resource-attributes.md`)
/// costs over [`LUA_ENRICH_SCRIPT`]'s baseline (`crates/logit-bench/tests/allocations.rs`'s
/// `lua_process_one_event_writing_resource`).
pub const LUA_RESOURCE_WRITE_SCRIPT: &str = r#"
function process(event)
  resource["service.name"] = "nginx"
  return event
end
"#;

/// Reads `event.log.trace_id` on every call -- for measuring what a script touching the new
/// `event.log` proxy costs (`crates/logit-script/src/proxy.rs`'s `LogProxy`,
/// `docs/adr/log-record-trace-context.md`), over [`LUA_ENRICH_SCRIPT`]'s baseline
/// (`crates/logit-bench/tests/allocations.rs`'s `lua_process_one_event_reading_log_trace`).
pub const LUA_LOG_TRACE_READ_SCRIPT: &str = r#"
function process(event)
  local _ = event.log.trace_id
  return event
end
"#;

/// Reads `event.metrics[1].value` on every call -- for measuring what a script touching the
/// `event.metrics` surface (`crates/logit-script/src/proxy.rs`'s `MetricsProxy`/`MetricProxy`)
/// costs, over [`LUA_ENRICH_SCRIPT`]'s baseline
/// (`crates/logit-bench/tests/allocations.rs`'s `lua_process_one_event_reading_metric_value`).
/// A pure read, discarded rather than written back into `event.attributes` -- see that test's own
/// doc comment for why, and for what the write variant would cost instead.
pub const LUA_METRIC_VALUE_READ_SCRIPT: &str = r#"
function process(event)
  local _ = event.metrics[1].value
  return event
end
"#;

/// Reads `#event.metrics` (`MetaMethod::Len`) only -- no `event.metrics[i]` indexing at all, so
/// this isolates `MetricsProxy`'s own creation-and-caching cost from `MetricProxy`'s per-index
/// one (`crates/logit-bench/tests/allocations.rs`'s `lua_process_one_event_reading_metric_len`).
pub const LUA_METRIC_LEN_READ_SCRIPT: &str = r#"
function process(event)
  local _ = #event.metrics
  return event
end
"#;

/// Reads `event.span.name` on every call -- for measuring what a script touching the
/// `event.span` surface (`crates/logit-script/src/proxy.rs`'s `SpanProxy`) costs, over
/// [`LUA_ENRICH_SCRIPT`]'s baseline (`crates/logit-bench/tests/allocations.rs`'s
/// `lua_process_one_event_reading_span_name`).
pub const LUA_SPAN_NAME_READ_SCRIPT: &str = r#"
function process(event)
  local _ = event.span.name
  return event
end
"#;

/// Reads `scope.name` on every call -- for measuring what a script touching the batch-level
/// `scope` global (`crates/logit-script/src/scope.rs`) costs
/// (`crates/logit-bench/tests/allocations.rs`'s `lua_process_one_event_reading_scope_name`).
pub const LUA_SCOPE_NAME_READ_SCRIPT: &str = r#"
function process(event)
  local _ = scope.name
  return event
end
"#;

/// Writes `scope.attributes.k` on every call -- the first-write copy-on-write path
/// (`crates/logit-script/src/scope.rs`'s `ensure_modified`)
/// (`crates/logit-bench/tests/allocations.rs`'s `lua_process_one_event_writing_scope_attribute`).
pub const LUA_SCOPE_ATTR_WRITE_SCRIPT: &str = r#"
function process(event)
  scope.attributes.k = "v"
  return event
end
"#;

/// Writes `resource.schema_url` on every call -- the named-field write path added alongside
/// `resource`'s attribute map (`crates/logit-script/src/resource.rs`'s `write_schema_url`)
/// (`crates/logit-bench/tests/allocations.rs`'s
/// `lua_process_one_event_writing_resource_schema_url`).
pub const LUA_RESOURCE_SCHEMA_URL_WRITE_SCRIPT: &str = r#"
function process(event)
  resource.schema_url = "https://example.com/schema"
  return event
end
"#;

/// Assigns `scope.name = scope.name` on every call -- an identity write, which
/// `crates/logit-script/src/scope.rs`'s no-op check must catch before ever calling
/// `ensure_modified` (`crates/logit-bench/tests/allocations.rs`'s
/// `lua_process_one_event_identity_write_to_scope_name_is_free`).
pub const LUA_SCOPE_IDENTITY_NAME_SCRIPT: &str = r#"
function process(event)
  scope.name = scope.name
  return event
end
"#;

/// Drops the incoming event and returns one minted from a literal table through `Event.new`
/// (`crates/logit-script/src/construct.rs`, `docs/adr/lua-event-constructor.md`): one attribute
/// and a minimal log, the smallest useful constructed event
/// (`crates/logit-bench/tests/allocations.rs`'s `lua_process_one_event_constructing_a_log_event`).
pub const LUA_EVENT_NEW_LOG_SCRIPT: &str = r#"
function process(event)
  return Event.new{timestamp = "1", attributes = {env = "prod"}, log = {message = "hi"}}
end
"#;

/// As [`LUA_EVENT_NEW_LOG_SCRIPT`], minting the smallest useful metric event instead: one
/// `gauge` record with nothing but its required fields, the shape the plan's `flush(now)` smoke
/// test emits (`crates/logit-bench/tests/allocations.rs`'s
/// `lua_process_one_event_constructing_a_gauge_event`).
pub const LUA_EVENT_NEW_GAUGE_SCRIPT: &str = r#"
function process(event)
  return Event.new{timestamp = "1", metrics = {{name = "tick", kind = "gauge", value = 1}}}
end
"#;

/// As [`LUA_EVENT_NEW_LOG_SCRIPT`], minting the smallest useful *span* event instead: the three
/// required span fields and nothing else, so every core default applies (`kind` internal,
/// `status` unset, `end_timestamp` the event's own, no `SpanExt`, empty `events`/`links`) --
/// the last payload kind `Event.new` builds (`crates/logit-bench/tests/allocations.rs`'s
/// `lua_process_one_event_constructing_a_span_event`).
pub const LUA_EVENT_NEW_SPAN_SCRIPT: &str = r#"
function process(event)
  return Event.new{timestamp = "1", span = {trace_id = "4bf92f3577b34da6a3ce929d0e0e4736", span_id = "00f067aa0ba902b7", name = "GET /"}}
end
"#;

/// A metric-only event carrying one `MetricKind::Sum` record -- a counter, the shape
/// `kv_metrics`'s `nginx.requests` spec (`fn kv_metrics` above) produces on the wire, and the
/// fixture the Lua `event.metrics[i].value`/`#event.metrics` surface
/// (`crates/logit-script/src/proxy.rs`'s `MetricsProxy`/`MetricProxy`) is measured against
/// (`crates/logit-bench/tests/allocations.rs`'s `lua_process_one_event_reading_metric_value`/
/// `_reading_metric_len`).
pub fn sum_metric_event() -> Event {
    Event::metric(
        0,
        AttrMap::new(),
        MetricRecord::new(logit_core::interner::intern("nginx.requests"), MetricKind::counter(1.0)),
    )
}

/// The batch-level `scope` (OTLP's `InstrumentationScope`) `run_lua` installs before a batch's
/// events reach `process` (`crates/logit-script/src/scope.rs`) -- non-empty `name`/`version`, no
/// attributes, mirroring [`resource`]'s own minimal shape above. `Bytes::from_static` rather than
/// `Bytes::copy_from_slice` for `name`/`version`: a `'static` `Bytes` never needs the
/// one-time shared-representation promotion a `Vec`-backed one pays on its first clone (see
/// `cached_message`'s own doc comment above), which would otherwise leak into
/// `lua_process_one_event_writing_scope_attribute`'s measured first-write clone as an unrelated
/// one-time cost.
pub fn scope() -> Arc<Scope> {
    Arc::new(Scope {
        name: Bytes::from_static(b"nginx-otel-module"),
        version: Bytes::from_static(b"1.0.0"),
        ..Scope::default()
    })
}

/// Touches nothing at all -- no `.attributes`, `.log`, `.metrics`, `.span`, `resource`, or
/// `scope` access, just the identity function. Isolates whatever a *fixture's own shape* costs
/// (e.g. `Event::clone`, when something forces one) from any proxy's own first-access cost, since
/// a script this narrow creates no proxy and therefore no extra strong reference to `event`'s
/// `Rc<RefCell<Event>>` beyond `EventProxy`'s own -- `EventProxy::into_inner`'s `Rc::try_unwrap`
/// fast path always succeeds here, regardless of the event's shape
/// (`crates/logit-bench/tests/allocations.rs`'s
/// `lua_process_one_event_passthrough_on_a_spilled_event`).
pub const LUA_PASSTHROUGH_SCRIPT: &str = r#"
function process(event)
  return event
end
"#;

/// [`sum_metric_event`], but with a *spilled* (9, past `AttrMap`'s 8-slot inline capacity)
/// event-level attribute map -- makes a real `Event::clone` allocate instead of the free memcpy
/// `sum_metric_event`'s own empty, inline `AttrMap` gets away with (mirrors `wide_gauge_event`'s
/// own reasoning above). Exists to guard `MetricProxy`'s `Weak<RefCell<Event>>` field
/// (`crates/logit-script/src/proxy.rs`): before that field was a `Weak`, a leftover, not-yet-GC'd
/// `event.metrics[i]` temporary held a *strong* `Rc`, so `EventProxy::into_inner`'s
/// `Rc::try_unwrap` fast path could fail and fall back to a real `Event::clone` -- a cost
/// `sum_metric_event`'s own free clone could never make visible to this file's exact-equality
/// assertions (`crates/logit-bench/tests/allocations.rs`'s
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

/// `regex`'s fixture: three named captures onto [`SSHD_SYSLOG_LINE`]'s auth-failure shape --
/// `docs/adr/regex-transform.md`.
pub fn regex_parser() -> RegexParser {
    RegexParser::new(
        r"for invalid user (?P<ssh_user>\S+) from (?P<client_address>\S+) port (?P<client_port>\d+)",
        None,
    )
    .expect("fixture pattern should compile")
}

/// A bare log event carrying [`SSHD_SYSLOG_LINE`]'s full text as its message, with no attributes
/// yet -- exercises [`regex_parser`]'s three captures landing while `AttrMap` is still well
/// inside its 8-entry inline capacity (`crates/logit-bench/tests/allocations.rs`'s
/// `regex_capture_into_an_inline_map`).
///
/// `Bytes::from_static`, not `Value::str` (`Bytes::from(String)`) -- a message that actually
/// arrives off the wire is always already a `Bytes` slice of a decoder's buffer, never a freshly
/// heap-allocated, not-yet-shared one. `bytes::Bytes`'s `Vec`-backed representation defers one
/// allocation to its *first* `slice`/`clone` (promoting from a uniquely-owned buffer to a shared
/// one) regardless of who calls it -- real, but a property of how this fixture would build the
/// buffer, not of what `regex` costs. `Bytes::from_static` (like a decoded message already sliced
/// out of its datagram) carries no such one-time cost, so this fixture isolates the thing it's
/// named for: `AttrMap` capacity, not buffer provenance.
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

/// [`SSHD_SYSLOG_LINE`] decoded by `syslog_in` -- six `syslog.*` attributes already on the event
/// before [`regex_parser`] adds three more captures, pushing past `AttrMap`'s 8-entry inline
/// capacity (`crates/logit-bench/tests/allocations.rs`'s `regex_parse_one_event`).
pub fn sshd_event() -> Event {
    let mut decoder = syslog_decoder();
    let batch = decoder.decode(logs_only_syslog_datagram(1)).expect("fixture line should decode");
    batch.events.into_iter().next().expect("fixture line should produce one event")
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

/// One Prometheus text 0.0.4 scrape body, modeled on the metric names and shapes real Node
/// Exporter and application scrapes actually carry (`docs/design/telemetry-landscape.md`'s
/// "Prometheus exposition format / OpenMetrics" section) rather than a synthetic shape chosen to
/// flatter the numbers: two counter families (HTTP request totals, per-CPU seconds), one gauge, one
/// histogram, and one summary -- 11 series in total (a histogram's buckets and a summary's
/// quantiles are one composite series each, not one per wire sample line -- see
/// `logit_proto::prometheus::Point`), covering every kind `prometheus_decode_one_scrape`
/// (`tests/allocations.rs`) has to walk.
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

/// `count` distinct gauge series under one family, each with its own `shard` label value --
/// `prometheus_encode_100_series` (`tests/allocations.rs`)'s workload. One family rather than
/// `wide_gauge_event`'s many-attribute shape: encoding cost here is dominated by the number of
/// distinct *series* the registry/writer walks, not by any one series' label count.
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

/// The one instant every remote-write fixture below stamps: whole milliseconds, which is the only
/// resolution either version carries.
const REMOTE_WRITE_TIMESTAMP_MS: i64 = 1_700_000_000_000;

/// `count` distinct gauge series under one family, the family shape
/// `remote_write_encode_100_series_v1`/`_v2` (`tests/allocations.rs`) encode --
/// [`prometheus_gauge_events`]'s workload already through `events_to_families`, so the measurement
/// is the *protobuf* half rather than the model mapping the `prometheus_out` rows already pin.
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

/// One **uncompressed** remote-write 1.0 request carrying 100 gauge series of one family, the shape
/// a sender with `max_samples_per_send` well under its default produces -- `remote_write_decode_one_request_v1`
/// (`tests/allocations.rs`).
///
/// Built from the vendored `prometheus.WriteRequest` types directly, not through
/// `logit_proto::prometheus::remote_write::encode`: the request this measures decoding has to be an
/// independent statement of what the wire looks like, or the measurement is circular. The label
/// order is the byte order both specs require of a sender (`__name__` before `shard`, since `_` is
/// `0x5f`).
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

/// The same request in remote-write 2.0 -- same series, same values, expressed through the symbol
/// table and per-series inline `Metadata` that version replaces `metadata[]` with. Symbol `0` is the
/// mandatory empty string; the shard values dominate the table, which is what a real 2.0 request
/// looks like too.
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

/// A collectd-shaped like-relay event: `collectd.host`/`collectd.plugin`/`collectd.type`/
/// `collectd.interval` present, one gauge record -- the shape `collectd_in` produces and
/// `collectd_out`'s encoder fast-paths straight into one Values part
/// (`logit_proto::collectd`'s module doc's "like-relay" row), rather than the slower per-record
/// fallback path a plain metric event without `collectd.type` would take.
pub fn collectd_event() -> Event {
    let mut attributes = AttrMap::new();
    attributes.insert(logit_proto::collectd::ATTR_HOST, Value::str("fixture-host"));
    attributes.insert(logit_proto::collectd::ATTR_PLUGIN, sstr("load"));
    attributes.insert(logit_proto::collectd::ATTR_TYPE, sstr("load"));
    attributes.insert(logit_proto::collectd::ATTR_INTERVAL, Value::F64(10.0));
    // A positive timestamp: `collectd_out`'s encoder drops a `timestamp <= 0` event outright
    // (`nanos_to_cdtime`'s own "no time given" reading), so `0` here would silently encode to
    // nothing at all rather than the one-Values-part-per-event shape this fixture exists for.
    let mut event = Event::empty(1_700_000_000_000_000_000, attributes);
    event
        .metrics
        .push(MetricRecord::new(logit_core::interner::intern("load.load"), MetricKind::Gauge(0.5)));
    event
}

/// `count` copies of [`collectd_event`] in one batch, for measuring `collectd_out`'s encoder
/// (`collectd_out: encode_into 100 events`, `tests/allocations.rs`).
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

/// A positive-timestamp gauge metric event -- `graphite_out`'s encoder drops any record whose
/// timestamp floors to a non-positive second (`crates/logit-proto/src/graphite/encode.rs`'s own
/// drop table), unlike e.g. `distribution_event`'s `ts: 0` (built to measure encoders with no such
/// rule), so every graphite fixture below carries a real one.
pub fn graphite_event() -> Event {
    Event::metric(
        1_700_000_000_000_000_000,
        AttrMap::new(),
        MetricRecord::new(logit_core::interner::intern("app.requests"), MetricKind::Gauge(42.0)),
    )
}

/// `count` copies of [`graphite_event`] in one batch, for measuring `graphite_out`'s encoder
/// (`graphite_out: encode_into 100 events`, `tests/allocations.rs`) in both wire protocols.
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

/// [`distribution_event`]'s shape with a positive timestamp -- see [`graphite_event`]'s doc
/// comment for why a graphite fixture can't reuse `distribution_event` as-is.
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

/// `count` copies of [`graphite_distribution_event`], for measuring `graphite_out`'s
/// `multi_value: expand` path against a kind whose expansion allocates nothing beyond the
/// caller's own `Vec<Event>` (unlike [`graphite_samples_batch`]'s `Samples::sketch()`).
pub fn graphite_distribution_batch(count: usize) -> EventBatch {
    let event = graphite_distribution_event();
    EventBatch {
        resource: resource(),
        scope: None,
        events: (0..count).map(|_| event.clone()).collect(),
    }
}

/// A positive-timestamp `MetricKind::Samples` event -- [`samples_event`]'s shape with a real
/// timestamp (see [`graphite_event`]'s doc comment for why), for measuring `graphite_out`'s
/// `multi_value: expand` path against the one kind whose expansion allocates a fresh `DdSketch`
/// per record (`Samples::sketch()`, inherent -- shared with `influxdb_out`).
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

/// The nginx-shaped event template both `generate_in` fixtures below render, in its all-literal
/// form: a JSON access-log body, one `host` attribute, one `requests` counter, and a
/// `service.name` resource.
///
/// `{{`/`}}` are `logit_core::template`'s escape for a literal brace, so the rendered body really
/// is the JSON it looks like.
const GENERATE_LOG_LITERAL: &str = r#"{{"method":"GET","path":"/x/0","status":200,"bytes":1024}}"#;

/// The same body with `{seq%50}` in its path -- one templated field.
const GENERATE_LOG_TEMPLATED: &str =
    r#"{{"method":"GET","path":"/x/{seq%50}","status":200,"bytes":1024}}"#;

/// One builder for both fixtures below, so they differ by *exactly* the two placeholders
/// [`generate_templated`] adds and nothing else -- which is what makes the difference between
/// their allocation counts attributable to templating alone (`docs/design/memory.md` §2's
/// `generate_in` rows).
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

/// A `generate_in` on its **prototype** render path: no placeholder anywhere, so one event is
/// rendered once and `clone`d per generated event with only `timestamp` overwritten
/// (`crates/logit-inputs/src/generate.rs`'s module doc).
pub fn generate_literal() -> GenerateInput {
    generate_input(GENERATE_LOG_LITERAL, "web-1")
}

/// A `generate_in` on its **per-event** render path, with exactly two templated fields
/// (`{seq%50}` in the log body, `{seq%10}` in the `host` attribute) -- so the gap from
/// [`generate_literal`]'s count is precisely what two placeholders cost per event.
pub fn generate_templated() -> GenerateInput {
    generate_input(GENERATE_LOG_TEMPLATED, "web-{seq%10}")
}

// -------------------------------------------------------------------------------------------
// Survey-derived shapes (docs/design/data-shapes.md §7 follow-up 2,
// docs/plans/event-sizing.md W1)
// -------------------------------------------------------------------------------------------
//
// Six shapes the data-shape survey asked for by name and nothing in this file sat at. Every one
// of them is **modelled, not captured**: each doc comment below names the survey row it is built
// to sit on and what part of it is the model's own invention, per this file's own provenance
// standard (see [`SSHD_SYSLOG_LINE`] and `docs/design/memory.md`'s "Fixtures" section). The
// survey's own §0 and §7 are explicit that none of its numbers are production traffic either, so
// these fixtures inherit that caveat rather than escaping it.
//
// Four of the six are built the way the leg that produces them really works -- a `tail_in`-style
// log event carrying a JSON body, parsed by the real `json` transform -- because that is the only
// way the attribute *width* these exist to pin is produced by the code under measurement rather
// than by the fixture. `tail_in` stamps exactly one attribute of its own, `log.file.path`
// (`crates/logit-inputs/src/tail/line.rs`), and the survey's §5.3 counts are taken from a `shape`
// tap after `json` on exactly that leg -- so the path attribute is part of every measured width
// below, and is included here for the same reason.

/// The attribute `tail_in` stamps on every line it reads (`crates/logit-inputs/src/tail/line.rs`)
/// -- the one attribute an unparsed log event carries on the leg `docs/design/data-shapes.md`
/// §5.3 measured, and therefore part of every width in that table.
const TAIL_PATH_KEY: &str = "log.file.path";

/// A `Value::Str` over a `'static` literal, never a `Bytes::from(String)`.
///
/// Every directly-constructed fixture below uses this rather than [`Value::str`], for
/// [`sshd_message_event`]'s reason applied to attribute values: `bytes::Bytes`'s `Vec`-backed
/// representation defers one allocation to its *first* `clone`, so a fixture built from
/// `Value::str` pays a one-time promotion per string inside whatever region first clones it --
/// which for these shapes is the `Event::clone` measurement itself. A value that really arrived
/// off the wire is always already a shared slice of a decoder's buffer; `Bytes::from_static` is
/// that, with no promotion to leak into a measurement.
fn sstr(literal: &'static str) -> Value {
    Value::Str(Bytes::from_static(literal.as_bytes()))
}

/// A log event carrying `body` as its message and nothing but [`TAIL_PATH_KEY`] in its attributes
/// -- the exact shape `tail_in` hands `json`, so a `json.process` over it produces the *total*
/// attribute width `docs/design/data-shapes.md` §5.3 tabulates (the library's own keys plus the
/// path), not just the JSON key count.
///
/// The message is cloned once before it is ever handed to a transform, for [`csv_event`]'s reason:
/// `bytes::Bytes` defers its shared representation to a buffer's first clone, and a fixture that
/// paid that promotion inside the measured region would be measuring its own construction.
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

/// **The commonest measured log shape**: eleven flat JSON fields which, merged onto `tail_in`'s own
/// `log.file.path`, make a **12-attribute** event -- the p50 in
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §5.3 for both Go
/// `log/slog`'s `JSONHandler` (12 / max 13) and structlog's documented production recipe (12 / 13),
/// and the middle of §2's "after `json` it is 9-14" band. §6 names its absence explicitly: "There
/// is no fixture for the commonest measured log shape (12 flat string attributes)".
///
/// Modelled on `log/slog`'s `JSONHandler` because its arithmetic is the one that reproduces the
/// measured number exactly and visibly: slog's default line is **3** fields (`time`, `level`,
/// `msg`, `log/slog`'s `handler.go` -- `docs/design/data-shapes-rows.md` §A), the survey's apps
/// each logged **8** ordinary access fields on top, and `tail_in` adds **1** -- 3 + 8 + 1 = 12.
/// Key and value lengths follow §2's measured bands rather than being chosen freely: keys are 3-11
/// bytes (median 6, against a measured median of 4-9), string values 3-36 bytes with one
/// user-agent in the tail (median 14, against a measured median of 10-16 and a p90 of 26-37).
///
/// **Modelled, not captured.** No slog process was run for this; the envelope is read off slog's
/// documented default output and the eight access fields are the survey's own description of the
/// workload its apps logged ("method, path, status, duration and the like"), not a recorded line.
/// [`WIDE_JSON_SYSLOG_LINE`] is what this is *not*: at 28 JSON fields that one is an access-log
/// or audit-log width, which §6 says explicitly ("wider than any library measured (9-15)").
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

/// The **nested** log shape, and the only one in this file: pino-http's completion record, whose
/// request serializers make the record *narrower* at the top and deeper underneath.
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §5.3 measures it at 9
/// attributes (max 10) after `json` with **4 nested maps, median width 3, depth 2**, and §6 calls
/// nested maps out as the thing that "multiply whatever is chosen" -- each `Value::Map` is a boxed
/// `AttrMap` paying the full inline footprint again (`crates/logit-core/src/value.rs`). §6 also
/// names the gap this closes: there is no fixture "for a nested-map record".
///
/// Nine top-level keys, exactly the desk row's count (`docs/design/data-shapes-rows.md` §A,
/// pino-http@v11): pino's own five (`level`, `time`, `msg`, `pid`, `hostname`) plus `reqId`,
/// `responseTime`, `req{}` and `res{}`. With `tail_in`'s path that is **10 event attributes**, the
/// measured maximum (the measured p50 of 9 is the same record without a path attribute -- a
/// `docker_in` or OTLP leg).
///
/// Four maps at depth 2: `req{}` and `res{}` each carry a nested `headers{}`, which is
/// `logit_transforms::shape`'s own accounting (it counts maps recursively and reports
/// `value_depth` as the deepest container chain, `crates/logit-transforms/src/shape.rs`) and is
/// what makes the measured 4 / 3 / 2 triple reproduce here. **The per-map widths are the model's
/// own**: the survey reports pooled percentiles over all maps and all events, not a width per map,
/// so three keys each is a choice consistent with its median of 3, not a recorded shape. The
/// serializers' own field lists come from `pino-std-serializers`' documented `req`/`res` output.
///
/// **Modelled, not captured** -- no pino-http process was run for this. One hazard the survey met
/// is worth keeping in view when reading any number off this fixture: misconfiguring pino-http's
/// destination silently drops the serializers, and the record then carries kilobytes of raw socket
/// internals. This models the configured shape, which is the narrow one.
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

/// The widest, highest-rate log class in the survey, and the one
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §7 flags as having **no
/// capture behind it at all**: "Edge and access-log streams (15-34 fields, the highest event rates)
/// rest on Counted rows."
///
/// **PostgreSQL's `jsonlog`**, 29 keys, is the one chosen -- `docs/design/data-shapes-rows.md` §A
/// (`T5b#C4`, counted against PostgreSQL's own `runtime-config-logging` §19.8.4-5), and not an
/// arbitrary pick: `tail_in` is already live against exactly this format in `demo/logit.yaml`'s
/// `postgres_in`, so this fixture's width is one a config in this repository really produces. With
/// `tail_in`'s own `log.file.path` that is a **30-attribute** event, the middle of the 15-34 band
/// (ALB's 34 and CloudFront's 33 are the other candidates §2 counts; either would sit two to four
/// attributes wider and one `realloc` further along [the growth ladder]
/// (../../tests/allocations.rs)).
///
/// **All 29 keys present at once.** PostgreSQL emits the error-detail keys (`detail`, `hint`,
/// `internal_query`, `context`, `statement`, ...) only on the lines that have them, so a routine
/// statement log is narrower than this; the survey's count is the format's full width, and this
/// fixture is that width -- the widest line the format produces, not its median line, which is
/// what the "desk-counted class" row means. **Modelled, not captured**: no PostgreSQL instance was
/// run for this, and the values are plausible rather than recorded.
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

/// A **17-attribute HTTP server span**, the measured ceiling and the shape
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §6 says has no fixture
/// ("for a span at the 16-17 ceiling"). It does not replace [`span_event`], which stays: that one
/// is deliberately inside `AttrMap`'s inline capacity and measures the `events`/`links` `Vec`s,
/// this one is deliberately past it and measures the spilled map.
///
/// Seventeen attributes, named from the OpenTelemetry HTTP semantic conventions on the path an
/// instrumentation really sets them. §4 counts a conforming HTTP server span at **16** under
/// default configuration (3 required + 7 conditionally required + 6 recommended) and measures the
/// OpenTelemetry Demo at **p50 8, p90 17, max 18** over 114,551 spans; Java's agent, Go's
/// `otelhttp` and .NET's AspNetCore land at 16, 16 and 17 respectively with no configuration. This
/// sits at the p90, one attribute over the spec's default count -- `network.transport` is the
/// seventeenth, a recommended attribute a real agent does set.
///
/// **No span events and no links**, unlike [`span_event`], and that is the measured finding rather
/// than a simplification: §4 reports 76% of demo spans carrying no events at all, and **no span in
/// 114,551 carried a link**. So the clone cost of this fixture is its spilled attribute map and
/// nothing else, where [`span_event`]'s is two `Vec`s and no spill -- between them they separate
/// the two costs a span can have.
///
/// **Modelled, not captured**: the attribute *names* are the conventions', but no OTLP payload was
/// recorded for this -- it is built by hand against `crates/logit-core/src/span.rs`, per
/// `docs/design/memory.md`'s Fixtures pattern #2.
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
    // The seventeenth, taking this from the spec's default 16 to the measured p90 of 17.
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

/// The **only measured `MetricList` spill**: one collectd value list carrying three data sources,
/// which [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §3 puts at **17.4% of
/// 16,590 events** under a default plugin set (2 records; 0.4% carry 3, and nothing carried more).
/// Every other metric input in the tree emits one metric per event by construction, so this is the
/// shape that decides whether `MetricList`'s single inline slot costs anything at all --
/// `memory.md` §8 item 13's open question.
///
/// Six attributes, which is collectd's measured width exactly: §3 reports p50 = p90 = max = **6**
/// over the whole corpus, and six is also the full `collectd.*` identity set the decoder stamps
/// (`crates/logit-proto/src/collectd/mod.rs`) -- host, plugin, plugin instance, type, type
/// instance, interval. So this event's map stays *inline* and its metric list spills, the exact
/// inverse of every log fixture above.
///
/// Built by hand rather than decoded from [`collectd_load_packet`] (which produces the same three
/// records but only four attributes, having no instances to stamp): the point here is the
/// post-decode shape at the measured width, and `docs/design/memory.md`'s Fixtures section
/// sanctions a directly-constructed event where no wire literal produces the shape wanted. The
/// record names are what `CollectdDecoder` really produces for a three-source list with a
/// `types.db` attached -- `<plugin>.<type>.<data source>` (`collectd/decode.rs`).
///
/// **Modelled, not captured, and one key is the model's own.** `load` is collectd's canonical
/// three-data-source type, and its `relative` type instance is real (the `load` plugin's
/// `ReportRelative`) -- but a real `load` list carries no *plugin* instance, so a decoded one is
/// five attributes, not six. The sixth is present deliberately: the survey measures the record
/// count (3) and the attribute width (6) over the same corpus but does not say they co-occur on
/// one list, and this fixture crosses them on purpose so one event exercises both spills'
/// absence/presence at the measured numbers. A reviewer reading a per-attribute cost off this
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
/// `resource_detection` on. §6: "Whatever `AttrMap` becomes, `Resource` is the consumer that is
/// already always spilled" -- at 17 attributes against 8 inline slots this one allocates on
/// construction and on every clone, once per batch.
///
/// The names are the conventions' own identity groups as a collector fills them -- `service.*`
/// (the SDK's default resource), `telemetry.sdk.*` (which every SDK sets), `k8s.*` (what
/// `k8sattributes` adds: §4 counts 6 by default and 30 fully enabled), plus host, container and
/// cloud attributes from `resourcedetection`. **Modelled, not captured**: the survey measured
/// *counts*, not which keys; §7 is explicit that "Kubernetes enrichment is Counted, not Measured",
/// so the seventeen names here are a plausible 17 of the conventions' 38, not a recorded set.
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

/// One OpenTelemetry log record at its measured median shape --
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §2: "a median of 9
/// attributes (max 11) through the demo's collector, ... with a median body of 84 bytes", almost
/// never nested (268 of 36,524 records). Nine attributes is one slot *past* `AttrMap`'s inline
/// capacity, which is the point: the median OpenTelemetry log record spills, by one. The body here
/// is 84 bytes exactly.
///
/// **Modelled, not captured**: the counts are measured, the key names are the conventions' own.
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
            // 84 bytes, the measured median body length for an OpenTelemetry log record.
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

/// **Five events sharing a 17-attribute [`enriched_resource`]** -- the batch shape
/// [`docs/design/data-shapes.md`](../../../docs/design/data-shapes.md) §3 measured for a
/// collector export (median **5** events per batch, p90 16) carrying §4's median resource. The
/// pairing is the fixture: a `Resource` is `Arc`-shared across a batch
/// (`crates/logit-core/src/event.rs`), so its spilled map is paid once per five events rather than
/// once each -- and an `EventBatch::clone` (`docs/design/memory.md` §3's copy-on-write path) pays
/// it again per contended fan-out branch, while the `Arc` alone does not.
///
/// The events are [`otlp_log_record_event`]s, so what this fixture separates is the two places an
/// `AttrMap` is paid: five per-event maps at the measured median width (9, one past inline) and
/// one much wider resource map that is *not* cloned with the batch at all, only `Arc`-bumped.
pub fn enriched_resource_batch() -> EventBatch {
    EventBatch {
        resource: enriched_resource(),
        scope: None,
        events: (0..5).map(otlp_log_record_event).collect(),
    }
}
