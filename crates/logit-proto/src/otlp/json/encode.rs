//! OTLP/JSON encoding from the generated `prost` structs, writing what the OpenTelemetry
//! Collector's pdata `JSONMarshaler` writes (`plog`/`pmetric`/`ptrace`'s `JSONMarshaler`, over
//! `pdata/internal`'s generated `MarshalJSON` methods and `pdata/internal/json/stream.go`), so a
//! Collector reading `otlp_json` gets the bytes it would have written itself. Hand-rolled into
//! one `Vec<u8>` for the reason `super`'s module doc gives for the decoder: `pbjson` can't write
//! hex ids.
//!
//! **This module doc is the encoding table.** Where pdata and the
//! [ProtoJSON spec](https://protobuf.dev/programming-guides/json/) differ, pdata wins, because
//! the reader this targets is the Collector:
//!
//! - **Field names** are lowerCamelCase, in the order each `.proto` file declares them.
//! - **`traceId`/`spanId`/`parentSpanId`** are lowercase hex, OTLP's deviation from ProtoJSON.
//!   **An all-zero id is omitted**, as pdata's `IsEmpty` treats it: pdata holds ids as fixed
//!   arrays and can't tell an all-zero id from an absent one.
//! - **`AnyValue.bytesValue`** is standard base64 with padding.
//! - **64-bit integers** (`uint64`, `fixed64`, `int64`, `sfixed64`) are decimal JSON strings;
//!   32-bit integers, flags, and dropped counts are JSON numbers.
//! - **Enums** are integers, as OTLP requires; ProtoJSON's default is the name.
//! - **Doubles** are JSON numbers, or `"NaN"`, `"Infinity"`, `"-Infinity"`. A finite number is
//!   the shortest text that round-trips, in decimal notation when its magnitude is zero or in
//!   `[1e-6, 1e21)`, and otherwise in exponent notation with a signed exponent of at least two
//!   digits (`1e-07`, `1.5e+21`), as `json-iterator` 1.1.12's `WriteFloat64` writes it. An
//!   integral double is written with no fraction (`1`, not `1.0`).
//! - **A scalar, string, or repeated field at its default is omitted**, as ProtoJSON omits a
//!   field without presence. A oneof member (`asInt`, `asDouble`, an `AnyValue` arm) and an
//!   optional `sum`/`min`/`max` are written whenever set, zero included.
//! - **Some message fields are written even when unset**, as `{}`: `resource`, `scope`,
//!   `LogRecord.body`, `KeyValue.value`, `Span.status`, and an exponential histogram point's
//!   `positive` and `negative`. pdata embeds these by value, so it has no unset state for them;
//!   ProtoJSON would omit an unset message field.
//! - **Strings** escape `"`, `\`, and the control characters below U+0020 only: `\n`, `\r`,
//!   `\t` by name, the rest as `\u00xx`. No HTML escaping.
//!
//! The profiles-only `KeyValue.keyStrindex` and `AnyValue.stringValueStrindex` and the
//! `Resource.entityRefs`/`Metric.metadata` fields are written when set, though `super`'s
//! decoder doesn't read them back.
//!
//! The tests re-encode the Collector output recorded in `testdata/interop/otlp/` and compare it
//! byte for byte, which pins field order and number forms against a real pdata.

use super::super::generated::opentelemetry::proto::{
    common::v1 as common_pb, logs::v1 as logs_pb, metrics::v1 as metrics_pb,
    resource::v1 as resource_pb, trace::v1 as trace_pb,
};
use base64::Engine;
use std::io::Write;

/// Appends `data` as an OTLP/JSON `ExportLogsServiceRequest` body to `out`.
pub fn write_logs(data: &logs_pb::LogsData, out: &mut Vec<u8>) {
    let mut o = Obj::open(out);
    if !data.resource_logs.is_empty() {
        array(o.key("resourceLogs"), &data.resource_logs, resource_logs);
    }
    o.close();
}

/// Appends `data` as an OTLP/JSON `ExportMetricsServiceRequest` body to `out`.
pub fn write_metrics(data: &metrics_pb::MetricsData, out: &mut Vec<u8>) {
    let mut o = Obj::open(out);
    if !data.resource_metrics.is_empty() {
        array(o.key("resourceMetrics"), &data.resource_metrics, resource_metrics);
    }
    o.close();
}

/// Appends `data` as an OTLP/JSON `ExportTraceServiceRequest` body to `out`.
pub fn write_traces(data: &trace_pb::TracesData, out: &mut Vec<u8>) {
    let mut o = Obj::open(out);
    if !data.resource_spans.is_empty() {
        array(o.key("resourceSpans"), &data.resource_spans, resource_spans);
    }
    o.close();
}

/// An open JSON object that writes the comma before every key after the first.
struct Obj<'a> {
    out: &'a mut Vec<u8>,
    any: bool,
}

impl<'a> Obj<'a> {
    fn open(out: &'a mut Vec<u8>) -> Self {
        out.push(b'{');
        Obj { out, any: false }
    }

    /// Writes `"key":` and returns the buffer for the value. `key` is a field name, which never
    /// needs escaping.
    fn key(&mut self, key: &str) -> &mut Vec<u8> {
        if self.any {
            self.out.push(b',');
        }
        self.any = true;
        self.out.push(b'"');
        self.out.extend_from_slice(key.as_bytes());
        self.out.extend_from_slice(b"\":");
        self.out
    }

    fn string(&mut self, key: &str, v: &str) {
        if !v.is_empty() {
            string(self.key(key), v);
        }
    }

    fn u32(&mut self, key: &str, v: u32) {
        if v != 0 {
            number(self.key(key), v);
        }
    }

    fn i32(&mut self, key: &str, v: i32) {
        if v != 0 {
            number(self.key(key), v);
        }
    }

    fn u64(&mut self, key: &str, v: u64) {
        if v != 0 {
            quoted(self.key(key), v);
        }
    }

    fn f64(&mut self, key: &str, v: f64) {
        if v != 0.0 {
            double(self.key(key), v);
        }
    }

    fn opt_f64(&mut self, key: &str, v: Option<f64>) {
        if let Some(v) = v {
            double(self.key(key), v);
        }
    }

    fn id(&mut self, key: &str, v: &[u8]) {
        if v.iter().any(|b| *b != 0) {
            hex(self.key(key), v);
        }
    }

    fn key_values(&mut self, key: &str, v: &[common_pb::KeyValue]) {
        if !v.is_empty() {
            array(self.key(key), v, key_value);
        }
    }

    fn close(self) {
        self.out.push(b'}');
    }
}

fn array<T>(out: &mut Vec<u8>, items: &[T], mut write: impl FnMut(&mut Vec<u8>, &T)) {
    out.push(b'[');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        write(out, item);
    }
    out.push(b']');
}

fn number(out: &mut Vec<u8>, v: impl std::fmt::Display) {
    // Writing into a `Vec` can't fail.
    let _ = write!(out, "{v}");
}

fn quoted(out: &mut Vec<u8>, v: impl std::fmt::Display) {
    let _ = write!(out, "\"{v}\"");
}

/// `json-iterator` 1.1.12's `WriteFloat64` behind pdata's non-finite strings. Rust's `Display`
/// and `LowerExp` print the shortest round-tripping digits, as Go's `strconv.AppendFloat` with
/// precision -1 does. Go's exponent always has a sign and at least two digits (`1e-07`,
/// `1.5e+21`), and `LowerExp`'s (`1e-7`, `1.5e21`) doesn't.
fn double(out: &mut Vec<u8>, v: f64) {
    if v.is_nan() {
        out.extend_from_slice(b"\"NaN\"");
    } else if v == f64::INFINITY {
        out.extend_from_slice(b"\"Infinity\"");
    } else if v == f64::NEG_INFINITY {
        out.extend_from_slice(b"\"-Infinity\"");
    } else {
        let abs = v.abs();
        if abs == 0.0 || (1e-6..1e21).contains(&abs) {
            let _ = write!(out, "{v}");
        } else {
            let text = format!("{v:e}");
            let (mantissa, exponent) = text.split_once('e').unwrap_or((&text, "0"));
            let exponent: i32 = exponent.parse().unwrap_or(0);
            let sign = if exponent < 0 { '-' } else { '+' };
            let _ = write!(out, "{mantissa}e{sign}{:02}", exponent.unsigned_abs());
        }
    }
}

fn string(out: &mut Vec<u8>, s: &str) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(b'"');
    let bytes = s.as_bytes();
    let mut start = 0;
    let mut unicode = *b"\\u0000";
    for (i, &b) in bytes.iter().enumerate() {
        let escape: &[u8] = match b {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0..=0x1f => {
                unicode[4] = HEX[(b >> 4) as usize];
                unicode[5] = HEX[(b & 0xf) as usize];
                &unicode
            }
            _ => continue,
        };
        out.extend_from_slice(&bytes[start..i]);
        out.extend_from_slice(escape);
        start = i + 1;
    }
    out.extend_from_slice(&bytes[start..]);
    out.push(b'"');
}

fn hex(out: &mut Vec<u8>, v: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push(b'"');
    for b in v {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0xf) as usize]);
    }
    out.push(b'"');
}

fn resource(out: &mut Vec<u8>, r: Option<&resource_pb::Resource>) {
    let mut o = Obj::open(out);
    if let Some(r) = r {
        o.key_values("attributes", &r.attributes);
        o.u32("droppedAttributesCount", r.dropped_attributes_count);
        if !r.entity_refs.is_empty() {
            array(o.key("entityRefs"), &r.entity_refs, entity_ref);
        }
    }
    o.close();
}

fn entity_ref(out: &mut Vec<u8>, e: &common_pb::EntityRef) {
    let mut o = Obj::open(out);
    o.string("schemaUrl", &e.schema_url);
    o.string("type", &e.r#type);
    if !e.id_keys.is_empty() {
        array(o.key("idKeys"), &e.id_keys, |out, s| string(out, s));
    }
    if !e.description_keys.is_empty() {
        array(o.key("descriptionKeys"), &e.description_keys, |out, s| string(out, s));
    }
    o.close();
}

fn scope(out: &mut Vec<u8>, s: Option<&common_pb::InstrumentationScope>) {
    let mut o = Obj::open(out);
    if let Some(s) = s {
        o.string("name", &s.name);
        o.string("version", &s.version);
        o.key_values("attributes", &s.attributes);
        o.u32("droppedAttributesCount", s.dropped_attributes_count);
    }
    o.close();
}

fn key_value(out: &mut Vec<u8>, kv: &common_pb::KeyValue) {
    let mut o = Obj::open(out);
    o.string("key", &kv.key);
    any_value(o.key("value"), kv.value.as_ref());
    o.i32("keyStrindex", kv.key_strindex);
    o.close();
}

fn any_value(out: &mut Vec<u8>, v: Option<&common_pb::AnyValue>) {
    use common_pb::any_value::Value;
    let mut o = Obj::open(out);
    match v.and_then(|v| v.value.as_ref()) {
        None => {}
        Some(Value::StringValue(s)) => string(o.key("stringValue"), s),
        Some(Value::BoolValue(b)) => number(o.key("boolValue"), b),
        Some(Value::IntValue(i)) => quoted(o.key("intValue"), i),
        Some(Value::DoubleValue(d)) => double(o.key("doubleValue"), *d),
        Some(Value::ArrayValue(a)) => {
            let mut inner = Obj::open(o.key("arrayValue"));
            if !a.values.is_empty() {
                array(inner.key("values"), &a.values, |out, v| any_value(out, Some(v)));
            }
            inner.close();
        }
        Some(Value::KvlistValue(kv)) => {
            let mut inner = Obj::open(o.key("kvlistValue"));
            inner.key_values("values", &kv.values);
            inner.close();
        }
        Some(Value::BytesValue(b)) => {
            let out = o.key("bytesValue");
            out.push(b'"');
            let mut s = String::new();
            base64::engine::general_purpose::STANDARD.encode_string(b, &mut s);
            out.extend_from_slice(s.as_bytes());
            out.push(b'"');
        }
        Some(Value::StringValueStrindex(i)) => number(o.key("stringValueStrindex"), i),
    }
    o.close();
}

fn resource_logs(out: &mut Vec<u8>, rl: &logs_pb::ResourceLogs) {
    let mut o = Obj::open(out);
    resource(o.key("resource"), rl.resource.as_ref());
    if !rl.scope_logs.is_empty() {
        array(o.key("scopeLogs"), &rl.scope_logs, scope_logs);
    }
    o.string("schemaUrl", &rl.schema_url);
    o.close();
}

fn scope_logs(out: &mut Vec<u8>, sl: &logs_pb::ScopeLogs) {
    let mut o = Obj::open(out);
    scope(o.key("scope"), sl.scope.as_ref());
    if !sl.log_records.is_empty() {
        array(o.key("logRecords"), &sl.log_records, log_record);
    }
    o.string("schemaUrl", &sl.schema_url);
    o.close();
}

fn log_record(out: &mut Vec<u8>, r: &logs_pb::LogRecord) {
    let mut o = Obj::open(out);
    o.u64("timeUnixNano", r.time_unix_nano);
    o.u64("observedTimeUnixNano", r.observed_time_unix_nano);
    o.i32("severityNumber", r.severity_number);
    o.string("severityText", &r.severity_text);
    any_value(o.key("body"), r.body.as_ref());
    o.key_values("attributes", &r.attributes);
    o.u32("droppedAttributesCount", r.dropped_attributes_count);
    o.u32("flags", r.flags);
    o.id("traceId", &r.trace_id);
    o.id("spanId", &r.span_id);
    o.string("eventName", &r.event_name);
    o.close();
}

fn resource_spans(out: &mut Vec<u8>, rs: &trace_pb::ResourceSpans) {
    let mut o = Obj::open(out);
    resource(o.key("resource"), rs.resource.as_ref());
    if !rs.scope_spans.is_empty() {
        array(o.key("scopeSpans"), &rs.scope_spans, scope_spans);
    }
    o.string("schemaUrl", &rs.schema_url);
    o.close();
}

fn scope_spans(out: &mut Vec<u8>, ss: &trace_pb::ScopeSpans) {
    let mut o = Obj::open(out);
    scope(o.key("scope"), ss.scope.as_ref());
    if !ss.spans.is_empty() {
        array(o.key("spans"), &ss.spans, span);
    }
    o.string("schemaUrl", &ss.schema_url);
    o.close();
}

fn span(out: &mut Vec<u8>, s: &trace_pb::Span) {
    let mut o = Obj::open(out);
    o.id("traceId", &s.trace_id);
    o.id("spanId", &s.span_id);
    o.string("traceState", &s.trace_state);
    o.id("parentSpanId", &s.parent_span_id);
    o.u32("flags", s.flags);
    o.string("name", &s.name);
    o.i32("kind", s.kind);
    o.u64("startTimeUnixNano", s.start_time_unix_nano);
    o.u64("endTimeUnixNano", s.end_time_unix_nano);
    o.key_values("attributes", &s.attributes);
    o.u32("droppedAttributesCount", s.dropped_attributes_count);
    if !s.events.is_empty() {
        array(o.key("events"), &s.events, span_event);
    }
    o.u32("droppedEventsCount", s.dropped_events_count);
    if !s.links.is_empty() {
        array(o.key("links"), &s.links, span_link);
    }
    o.u32("droppedLinksCount", s.dropped_links_count);
    let mut status = Obj::open(o.key("status"));
    if let Some(st) = &s.status {
        status.string("message", &st.message);
        status.i32("code", st.code);
    }
    status.close();
    o.close();
}

fn span_event(out: &mut Vec<u8>, e: &trace_pb::span::Event) {
    let mut o = Obj::open(out);
    o.u64("timeUnixNano", e.time_unix_nano);
    o.string("name", &e.name);
    o.key_values("attributes", &e.attributes);
    o.u32("droppedAttributesCount", e.dropped_attributes_count);
    o.close();
}

fn span_link(out: &mut Vec<u8>, l: &trace_pb::span::Link) {
    let mut o = Obj::open(out);
    o.id("traceId", &l.trace_id);
    o.id("spanId", &l.span_id);
    o.string("traceState", &l.trace_state);
    o.key_values("attributes", &l.attributes);
    o.u32("droppedAttributesCount", l.dropped_attributes_count);
    o.u32("flags", l.flags);
    o.close();
}

fn resource_metrics(out: &mut Vec<u8>, rm: &metrics_pb::ResourceMetrics) {
    let mut o = Obj::open(out);
    resource(o.key("resource"), rm.resource.as_ref());
    if !rm.scope_metrics.is_empty() {
        array(o.key("scopeMetrics"), &rm.scope_metrics, scope_metrics);
    }
    o.string("schemaUrl", &rm.schema_url);
    o.close();
}

fn scope_metrics(out: &mut Vec<u8>, sm: &metrics_pb::ScopeMetrics) {
    let mut o = Obj::open(out);
    scope(o.key("scope"), sm.scope.as_ref());
    if !sm.metrics.is_empty() {
        array(o.key("metrics"), &sm.metrics, metric);
    }
    o.string("schemaUrl", &sm.schema_url);
    o.close();
}

fn metric(out: &mut Vec<u8>, m: &metrics_pb::Metric) {
    use metrics_pb::metric::Data;
    let mut o = Obj::open(out);
    o.string("name", &m.name);
    o.string("description", &m.description);
    o.string("unit", &m.unit);
    match &m.data {
        None => {}
        Some(Data::Gauge(g)) => {
            let mut d = Obj::open(o.key("gauge"));
            data_points(&mut d, &g.data_points, number_data_point);
            d.close();
        }
        Some(Data::Sum(s)) => {
            let mut d = Obj::open(o.key("sum"));
            data_points(&mut d, &s.data_points, number_data_point);
            d.i32("aggregationTemporality", s.aggregation_temporality);
            if s.is_monotonic {
                number(d.key("isMonotonic"), true);
            }
            d.close();
        }
        Some(Data::Histogram(h)) => {
            let mut d = Obj::open(o.key("histogram"));
            data_points(&mut d, &h.data_points, histogram_data_point);
            d.i32("aggregationTemporality", h.aggregation_temporality);
            d.close();
        }
        Some(Data::ExponentialHistogram(h)) => {
            let mut d = Obj::open(o.key("exponentialHistogram"));
            data_points(&mut d, &h.data_points, exponential_histogram_data_point);
            d.i32("aggregationTemporality", h.aggregation_temporality);
            d.close();
        }
        Some(Data::Summary(s)) => {
            let mut d = Obj::open(o.key("summary"));
            data_points(&mut d, &s.data_points, summary_data_point);
            d.close();
        }
    }
    o.key_values("metadata", &m.metadata);
    o.close();
}

fn data_points<T>(o: &mut Obj<'_>, points: &[T], write: impl FnMut(&mut Vec<u8>, &T)) {
    if !points.is_empty() {
        array(o.key("dataPoints"), points, write);
    }
}

fn exemplars(o: &mut Obj<'_>, key: &str, v: &[metrics_pb::Exemplar]) {
    if !v.is_empty() {
        array(o.key(key), v, exemplar);
    }
}

fn u64s(o: &mut Obj<'_>, key: &str, v: &[u64]) {
    if !v.is_empty() {
        array(o.key(key), v, |out, n| quoted(out, n));
    }
}

fn number_data_point(out: &mut Vec<u8>, p: &metrics_pb::NumberDataPoint) {
    use metrics_pb::number_data_point::Value;
    let mut o = Obj::open(out);
    o.key_values("attributes", &p.attributes);
    o.u64("startTimeUnixNano", p.start_time_unix_nano);
    o.u64("timeUnixNano", p.time_unix_nano);
    match p.value {
        None => {}
        Some(Value::AsDouble(d)) => double(o.key("asDouble"), d),
        Some(Value::AsInt(i)) => quoted(o.key("asInt"), i),
    }
    exemplars(&mut o, "exemplars", &p.exemplars);
    o.u32("flags", p.flags);
    o.close();
}

fn histogram_data_point(out: &mut Vec<u8>, p: &metrics_pb::HistogramDataPoint) {
    let mut o = Obj::open(out);
    o.key_values("attributes", &p.attributes);
    o.u64("startTimeUnixNano", p.start_time_unix_nano);
    o.u64("timeUnixNano", p.time_unix_nano);
    o.u64("count", p.count);
    o.opt_f64("sum", p.sum);
    u64s(&mut o, "bucketCounts", &p.bucket_counts);
    if !p.explicit_bounds.is_empty() {
        array(o.key("explicitBounds"), &p.explicit_bounds, |out, d| double(out, *d));
    }
    exemplars(&mut o, "exemplars", &p.exemplars);
    o.u32("flags", p.flags);
    o.opt_f64("min", p.min);
    o.opt_f64("max", p.max);
    o.close();
}

fn exponential_histogram_data_point(
    out: &mut Vec<u8>,
    p: &metrics_pb::ExponentialHistogramDataPoint,
) {
    let mut o = Obj::open(out);
    o.key_values("attributes", &p.attributes);
    o.u64("startTimeUnixNano", p.start_time_unix_nano);
    o.u64("timeUnixNano", p.time_unix_nano);
    o.u64("count", p.count);
    o.opt_f64("sum", p.sum);
    o.i32("scale", p.scale);
    o.u64("zeroCount", p.zero_count);
    buckets(o.key("positive"), p.positive.as_ref());
    buckets(o.key("negative"), p.negative.as_ref());
    o.u32("flags", p.flags);
    exemplars(&mut o, "exemplars", &p.exemplars);
    o.opt_f64("min", p.min);
    o.opt_f64("max", p.max);
    o.f64("zeroThreshold", p.zero_threshold);
    o.close();
}

fn buckets(out: &mut Vec<u8>, b: Option<&metrics_pb::exponential_histogram_data_point::Buckets>) {
    let mut o = Obj::open(out);
    if let Some(b) = b {
        o.i32("offset", b.offset);
        u64s(&mut o, "bucketCounts", &b.bucket_counts);
    }
    o.close();
}

fn summary_data_point(out: &mut Vec<u8>, p: &metrics_pb::SummaryDataPoint) {
    let mut o = Obj::open(out);
    o.key_values("attributes", &p.attributes);
    o.u64("startTimeUnixNano", p.start_time_unix_nano);
    o.u64("timeUnixNano", p.time_unix_nano);
    o.u64("count", p.count);
    o.f64("sum", p.sum);
    if !p.quantile_values.is_empty() {
        array(o.key("quantileValues"), &p.quantile_values, |out, q| {
            let mut o = Obj::open(out);
            o.f64("quantile", q.quantile);
            o.f64("value", q.value);
            o.close();
        });
    }
    o.u32("flags", p.flags);
    o.close();
}

fn exemplar(out: &mut Vec<u8>, e: &metrics_pb::Exemplar) {
    use metrics_pb::exemplar::Value;
    let mut o = Obj::open(out);
    o.key_values("filteredAttributes", &e.filtered_attributes);
    o.u64("timeUnixNano", e.time_unix_nano);
    match e.value {
        None => {}
        Some(Value::AsDouble(d)) => double(o.key("asDouble"), d),
        Some(Value::AsInt(i)) => quoted(o.key("asInt"), i),
    }
    o.id("traceId", &e.trace_id);
    o.id("spanId", &e.span_id);
    o.close();
}

#[cfg(test)]
#[path = "encode_tests.rs"]
mod tests;
