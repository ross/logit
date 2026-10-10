//! `format: json` for `stdio_out`/`file_out`: one JSON object per event, one per line, for a
//! program to read. This doc is the canonical grammar.
//!
//! Each line is one [`Event`] of an [`EventBatch`], in batch order, terminated by `\n`, with no
//! pretty-printing and nothing between batches. Every populated field of the event model
//! (`docs/design/data-model.md`) appears; a field that is `None`, empty, or `0` is omitted, never
//! written as `null`, `0`, or `[]`. `timestamp` always appears, so an empty event is still one
//! object. A metric kind's own fields and a span's `kind`/`status`/`start`/`end`/`duration_ns`
//! are always written. Keys appear in this order:
//!
//! - `timestamp`
//! - `log`: `severity`, `format` (always), `message`, `event_name`, `observed_timestamp`,
//!   `trace_id`, `span_id`, `trace_flags` (present whenever the trace reference is, even `0`),
//!   `dropped_attributes_count`
//! - `metrics`: an array, each `name`, `kind`, the kind's fields (table below), `unit`,
//!   `description`, `start_timestamp`, `flags` (the raw bitmask), `no_recorded_value: true` when
//!   `flags` bit 0 is set (the kind's fields are then omitted; `kind` stays), `exemplars` (each
//!   `timestamp`, `value`, `trace_id`, `span_id`, `trace_flags`, `attributes`)
//! - `span`: `name`, `trace_id`, `span_id`, `parent_span_id`, `kind`, `status`,
//!   `status_message`, `trace_state`, `flags`, `start` (the event's timestamp), `end`,
//!   `duration_ns` (`end - start`, saturating), `dropped_attributes_count`,
//!   `dropped_events_count`, `dropped_links_count`, `events` (each `name`, `timestamp`,
//!   `attributes`, `dropped_attributes_count`), `links` (each `trace_id`, `span_id`, `flags`,
//!   `trace_state`, `attributes`, `dropped_attributes_count`)
//! - `attributes`
//! - `resource` (the batch's, repeated on every line): `attributes`, `schema_url`,
//!   `dropped_attributes_count`; omitted when all three are
//! - `scope` (the batch's, when it has one): `name`, `version`, `attributes`, `schema_url`,
//!   `dropped_attributes_count`
//!
//! | `kind` | fields, in order |
//! |---|---|
//! | `sum` | `value`, `temporality` (`delta`/`cumulative`), `monotonic` |
//! | `gauge` | `value` |
//! | `gauge_delta` | `delta` (a negative zero writes `-0`) |
//! | `samples` | `values` (array), `sample_rate` |
//! | `set_members` | `members` (array of strings, invalid bytes as `\xHH`) |
//! | `distribution` | `count`, `sum`; then `min`, `max`, `avg` (`sum / count`), `p50`, `p90`, `p95`, `p99` when the sketch is non-empty |
//! | `set` | `estimate` |
//! | `histogram` | `temporality`, `buckets` (`[{"bound": b, "count": n}]` in wire order, each count the bucket's own), then `sum`, `min`, `max` when present |
//! | `exponential_histogram` | `scale`, `temporality`, `count`, `zero_count`, `zero_threshold`, `positive`, `negative` (each `{"offset": o, "counts": [..]}`, omitted when its counts are empty), then `sum`, `min`, `max` when present |
//! | `summary` | `count`, `sum`, `quantiles` (`[{"quantile": q, "value": v}]` in wire order) |
//!
//! Values: `Null`, `Bool`, `I64`, `U64` are JSON's own; `F64` is a JSON number, except that a
//! non-finite one, which JSON has no form for, is the string `"NaN"`, `"inf"`, or `"-inf"` (the
//! rule for every `f64` in the object, bucket bounds included); `Str` is a JSON string; `Timestamp`
//! is an RFC 3339 string; `Array` an array; `Map` an object in `AttrMap` order. `Bytes` is a JSON
//! string holding the text `b"..."`: valid UTF-8 runs as text, each invalid byte as `\xHH`. Byte
//! fields that are text by convention (`status_message`, `trace_state`, `schema_url`, scope
//! `name`/`version`) are strings with invalid bytes as `\xHH` and no `b` prefix. Every timestamp
//! is RFC 3339 UTC with nine fractional digits; every trace and span id is lowercase hex.
//!
//! Strings follow RFC 8259: `\"`, `\\`, `\n`, `\r`, `\t`, `\b`, `\f`, every other control
//! character as `\u00XX`; DEL and non-ASCII are written raw as UTF-8.
//!
//! Readers that mirror this grammar: `tools/shape-survey/summarize.py`,
//! `tools/shape-survey/check_interop.py`, `tools/shape-survey/producers/oteldemo.sh`'s Python,
//! `tools/splunk-interop/check.py`, `tools/victoria-interop/check.py`, and
//! `tools/soak/soaklib/telemetry.py`. A change here is mirrored there.
//!
//! Everything writes straight into the caller's `String` with `push_str`/`write!`: no per-field
//! `String`, no serde (`docs/design/memory.md`).

use bytes::Bytes;
use logit_core::interner::resolve;
use logit_core::time::write_rfc3339_utc;
use logit_core::trace::push_hex;
use logit_core::{
    AttrMap, Event, EventBatch, Exemplar, ExpHistogram, LogRecord, MetricKind, MetricRecord,
    Resource, Scope, SpanEvent, SpanLink, SpanRecord, Temporality, TraceRef, Value,
};
use std::fmt::Write;

/// Appends one JSON line per event of `batch` to `out`, in batch order. Never fails.
pub fn render_ndjson(out: &mut String, batch: &EventBatch) {
    for event in &batch.events {
        render_event(out, &batch.resource, batch.scope.as_deref(), event);
        out.push('\n');
    }
}

/// An open JSON object whose keys are static ASCII names, so they need no escaping. Tracks the
/// comma so an omitted field never leaves a stray separator.
struct Obj<'a> {
    out: &'a mut String,
    first: bool,
}

impl<'a> Obj<'a> {
    fn open(out: &'a mut String) -> Self {
        out.push('{');
        Obj { out, first: true }
    }

    /// Writes `"key":` (after a comma unless first) and returns the buffer for its value.
    fn key(&mut self, key: &str) -> &mut String {
        if !self.first {
            self.out.push(',');
        }
        self.first = false;
        self.out.push('"');
        self.out.push_str(key);
        self.out.push_str("\":");
        self.out
    }

    fn close(self) {
        self.out.push('}');
    }
}

fn render_event(out: &mut String, resource: &Resource, scope: Option<&Scope>, event: &Event) {
    let mut obj = Obj::open(out);
    push_timestamp(obj.key("timestamp"), event.timestamp);
    if let Some(log) = &event.log {
        render_log(obj.key("log"), log);
    }
    if !event.metrics.is_empty() {
        let out = obj.key("metrics");
        out.push('[');
        for (i, metric) in event.metrics.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            render_metric(out, metric);
        }
        out.push(']');
    }
    if let Some(span) = &event.span {
        render_span(obj.key("span"), event.timestamp, span);
    }
    if !event.attributes.is_empty() {
        push_attrs(obj.key("attributes"), &event.attributes);
    }
    if !resource.attributes.is_empty()
        || resource.schema_url.is_some()
        || resource.dropped_attributes_count != 0
    {
        let mut res = Obj::open(obj.key("resource"));
        if !resource.attributes.is_empty() {
            push_attrs(res.key("attributes"), &resource.attributes);
        }
        if let Some(url) = &resource.schema_url {
            push_text_bytes(res.key("schema_url"), url);
        }
        push_nonzero(&mut res, "dropped_attributes_count", resource.dropped_attributes_count);
        res.close();
    }
    if let Some(scope) = scope {
        let mut sc = Obj::open(obj.key("scope"));
        if !scope.name.is_empty() {
            push_text_bytes(sc.key("name"), &scope.name);
        }
        if !scope.version.is_empty() {
            push_text_bytes(sc.key("version"), &scope.version);
        }
        if !scope.attributes.is_empty() {
            push_attrs(sc.key("attributes"), &scope.attributes);
        }
        if let Some(url) = &scope.schema_url {
            push_text_bytes(sc.key("schema_url"), url);
        }
        push_nonzero(&mut sc, "dropped_attributes_count", scope.dropped_attributes_count);
        sc.close();
    }
    obj.close();
}

fn render_log(out: &mut String, log: &LogRecord) {
    let mut obj = Obj::open(out);
    if let Some(severity) = log.severity {
        push_json_str(obj.key("severity"), severity.as_str());
    }
    push_json_str(obj.key("format"), log.body_format.as_str());
    push_value(obj.key("message"), &log.message);
    if let Some(name) = log.event_name {
        push_json_str(obj.key("event_name"), resolve(name));
    }
    if log.observed_timestamp != 0 {
        push_timestamp(obj.key("observed_timestamp"), log.observed_timestamp);
    }
    if let Some(trace) = &log.trace {
        push_trace_ref(&mut obj, trace);
    }
    push_nonzero(&mut obj, "dropped_attributes_count", log.dropped_attributes_count);
    obj.close();
}

/// `trace_id`, `span_id` when present, and `trace_flags` always: a log's or an exemplar's
/// [`TraceRef`].
fn push_trace_ref(obj: &mut Obj<'_>, trace: &TraceRef) {
    push_hex_str(obj.key("trace_id"), &trace.trace_id);
    if let Some(span_id) = &trace.span_id {
        push_hex_str(obj.key("span_id"), span_id);
    }
    let _ = write!(obj.key("trace_flags"), "{}", trace.flags);
}

fn render_metric(out: &mut String, metric: &MetricRecord) {
    let mut obj = Obj::open(out);
    push_json_str(obj.key("name"), resolve(metric.name));
    push_json_str(obj.key("kind"), kind_name(&metric.kind));
    if !metric.is_no_recorded_value() {
        render_kind_fields(&mut obj, &metric.kind);
    }
    if let Some(unit) = metric.unit {
        push_json_str(obj.key("unit"), resolve(unit));
    }
    if let Some(description) = metric.description {
        push_json_str(obj.key("description"), resolve(description));
    }
    if metric.start_timestamp != 0 {
        push_timestamp(obj.key("start_timestamp"), metric.start_timestamp);
    }
    push_nonzero(&mut obj, "flags", metric.flags);
    if metric.is_no_recorded_value() {
        obj.key("no_recorded_value").push_str("true");
    }
    if !metric.exemplars.is_empty() {
        let out = obj.key("exemplars");
        out.push('[');
        for (i, exemplar) in metric.exemplars.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            render_exemplar(out, exemplar);
        }
        out.push(']');
    }
    obj.close();
}

fn kind_name(kind: &MetricKind) -> &'static str {
    match kind {
        MetricKind::Sum(_) => "sum",
        MetricKind::Gauge(_) => "gauge",
        MetricKind::GaugeDelta(_) => "gauge_delta",
        MetricKind::Samples(_) => "samples",
        MetricKind::SetMembers(_) => "set_members",
        MetricKind::Distribution(_) => "distribution",
        MetricKind::Set(_) => "set",
        MetricKind::Histogram(_) => "histogram",
        MetricKind::ExponentialHistogram(_) => "exponential_histogram",
        MetricKind::Summary(_) => "summary",
    }
}

fn render_kind_fields(obj: &mut Obj<'_>, kind: &MetricKind) {
    match kind {
        MetricKind::Sum(s) => {
            push_f64(obj.key("value"), s.value);
            push_temporality(obj, s.temporality);
            let _ = write!(obj.key("monotonic"), "{}", s.monotonic);
        }
        MetricKind::Gauge(v) => push_f64(obj.key("value"), *v),
        MetricKind::GaugeDelta(v) => push_f64(obj.key("delta"), *v),
        MetricKind::Samples(s) => {
            push_f64_array(obj.key("values"), &s.values);
            push_f64(obj.key("sample_rate"), s.sample_rate);
        }
        MetricKind::SetMembers(members) => {
            let out = obj.key("members");
            out.push('[');
            for (i, member) in members.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_text_bytes(out, member);
            }
            out.push(']');
        }
        MetricKind::Distribution(sketch) => {
            let count = sketch.count();
            let _ = write!(obj.key("count"), "{count}");
            push_f64(obj.key("sum"), sketch.sum());
            // `quantile` is `None` only for an empty sketch, and `min`/`max` with it.
            if let Some(p50) = sketch.quantile(0.5) {
                if let Some(min) = sketch.min() {
                    push_f64(obj.key("min"), min);
                }
                if let Some(max) = sketch.max() {
                    push_f64(obj.key("max"), max);
                }
                push_f64(obj.key("avg"), sketch.sum() / count as f64);
                push_f64(obj.key("p50"), p50);
                for (key, q) in [("p90", 0.9), ("p95", 0.95), ("p99", 0.99)] {
                    if let Some(v) = sketch.quantile(q) {
                        push_f64(obj.key(key), v);
                    }
                }
            }
        }
        MetricKind::Set(hll) => {
            let _ = write!(obj.key("estimate"), "{}", hll.estimate());
        }
        MetricKind::Histogram(h) => {
            push_temporality(obj, h.temporality);
            let out = obj.key("buckets");
            out.push('[');
            for (i, (bound, count)) in h.buckets.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str("{\"bound\":");
                push_f64(out, *bound);
                let _ = write!(out, ",\"count\":{count}}}");
            }
            out.push(']');
            push_sum_min_max(obj, h.sum, h.min, h.max);
        }
        MetricKind::ExponentialHistogram(e) => render_exp_histogram(obj, e),
        MetricKind::Summary(s) => {
            let _ = write!(obj.key("count"), "{}", s.count);
            push_f64(obj.key("sum"), s.sum);
            let out = obj.key("quantiles");
            out.push('[');
            for (i, (q, v)) in s.quantiles.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str("{\"quantile\":");
                push_f64(out, *q);
                out.push_str(",\"value\":");
                push_f64(out, *v);
                out.push('}');
            }
            out.push(']');
        }
    }
}

fn render_exp_histogram(obj: &mut Obj<'_>, e: &ExpHistogram) {
    let _ = write!(obj.key("scale"), "{}", e.scale);
    push_temporality(obj, e.temporality);
    let _ = write!(obj.key("count"), "{}", e.count);
    let _ = write!(obj.key("zero_count"), "{}", e.zero_count);
    push_f64(obj.key("zero_threshold"), e.zero_threshold);
    for (key, (offset, counts)) in [("positive", &e.positive), ("negative", &e.negative)] {
        if counts.is_empty() {
            continue;
        }
        let out = obj.key(key);
        let _ = write!(out, "{{\"offset\":{offset},\"counts\":[");
        for (i, count) in counts.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{count}");
        }
        out.push_str("]}");
    }
    push_sum_min_max(obj, e.sum, e.min, e.max);
}

fn push_temporality(obj: &mut Obj<'_>, temporality: Temporality) {
    push_json_str(obj.key("temporality"), temporality.as_str());
}

fn push_sum_min_max(obj: &mut Obj<'_>, sum: Option<f64>, min: Option<f64>, max: Option<f64>) {
    for (key, v) in [("sum", sum), ("min", min), ("max", max)] {
        if let Some(v) = v {
            push_f64(obj.key(key), v);
        }
    }
}

fn render_exemplar(out: &mut String, exemplar: &Exemplar) {
    let mut obj = Obj::open(out);
    push_timestamp(obj.key("timestamp"), exemplar.timestamp);
    push_f64(obj.key("value"), exemplar.value);
    if let Some(trace) = &exemplar.trace {
        push_trace_ref(&mut obj, trace);
    }
    if !exemplar.filtered_attributes.is_empty() {
        push_attrs(obj.key("attributes"), &exemplar.filtered_attributes);
    }
    obj.close();
}

/// `start` is the event's timestamp: a `SpanRecord` carries no start of its own.
fn render_span(out: &mut String, start: i64, span: &SpanRecord) {
    let mut obj = Obj::open(out);
    push_value(obj.key("name"), &span.name);
    push_hex_str(obj.key("trace_id"), &span.trace_id);
    push_hex_str(obj.key("span_id"), &span.span_id);
    if let Some(parent) = &span.parent_span_id {
        push_hex_str(obj.key("parent_span_id"), parent);
    }
    push_json_str(obj.key("kind"), span.kind.as_str());
    push_json_str(obj.key("status"), span.status.as_str());
    let ext = span.ext.as_deref();
    if let Some(message) = ext.and_then(|e| e.status_message.as_ref()) {
        push_text_bytes(obj.key("status_message"), message);
    }
    if let Some(state) = ext.and_then(|e| e.trace_state.as_ref()) {
        push_text_bytes(obj.key("trace_state"), state);
    }
    push_nonzero(&mut obj, "flags", span.flags);
    push_timestamp(obj.key("start"), start);
    push_timestamp(obj.key("end"), span.end_timestamp);
    // Saturating: an end before the start must render, not panic or wrap.
    let _ = write!(obj.key("duration_ns"), "{}", span.end_timestamp.saturating_sub(start));
    if let Some(ext) = ext {
        push_nonzero(&mut obj, "dropped_attributes_count", ext.dropped_attributes_count);
        push_nonzero(&mut obj, "dropped_events_count", ext.dropped_events_count);
        push_nonzero(&mut obj, "dropped_links_count", ext.dropped_links_count);
    }
    if !span.events.is_empty() {
        let out = obj.key("events");
        out.push('[');
        for (i, span_event) in span.events.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            render_span_event(out, span_event);
        }
        out.push(']');
    }
    if !span.links.is_empty() {
        let out = obj.key("links");
        out.push('[');
        for (i, link) in span.links.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            render_span_link(out, link);
        }
        out.push(']');
    }
    obj.close();
}

fn render_span_event(out: &mut String, span_event: &SpanEvent) {
    let mut obj = Obj::open(out);
    push_value(obj.key("name"), &span_event.name);
    push_timestamp(obj.key("timestamp"), span_event.timestamp);
    if !span_event.attributes.is_empty() {
        push_attrs(obj.key("attributes"), &span_event.attributes);
    }
    push_nonzero(&mut obj, "dropped_attributes_count", span_event.dropped_attributes_count);
    obj.close();
}

fn render_span_link(out: &mut String, link: &SpanLink) {
    let mut obj = Obj::open(out);
    push_hex_str(obj.key("trace_id"), &link.trace_id);
    push_hex_str(obj.key("span_id"), &link.span_id);
    push_nonzero(&mut obj, "flags", link.flags);
    if let Some(state) = &link.trace_state {
        push_text_bytes(obj.key("trace_state"), state);
    }
    if !link.attributes.is_empty() {
        push_attrs(obj.key("attributes"), &link.attributes);
    }
    push_nonzero(&mut obj, "dropped_attributes_count", link.dropped_attributes_count);
    obj.close();
}

fn push_nonzero(obj: &mut Obj<'_>, key: &str, n: u32) {
    if n != 0 {
        let _ = write!(obj.key(key), "{n}");
    }
}

/// An attribute map as a JSON object in `AttrMap`'s own (sorted-by-`Symbol`) order.
fn push_attrs(out: &mut String, attrs: &AttrMap) {
    out.push('{');
    for (i, (key, value)) in attrs.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_json_str(out, resolve(key));
        out.push(':');
        push_value(out, value);
    }
    out.push('}');
}

fn push_value(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::I64(i) => {
            let _ = write!(out, "{i}");
        }
        Value::U64(u) => {
            let _ = write!(out, "{u}");
        }
        Value::F64(f) => push_f64(out, *f),
        Value::Str(s) => {
            // `Value::Str` is constructed only from valid UTF-8, so this cannot panic.
            push_json_str(out, std::str::from_utf8(s).expect("Value::Str is always valid UTF-8"));
        }
        Value::Bytes(b) => {
            out.push_str("\"b\\\"");
            push_lossless_bytes_body(out, b);
            out.push_str("\\\"\"");
        }
        Value::Timestamp(ns) => push_timestamp(out, *ns),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                push_value(out, item);
            }
            out.push(']');
        }
        Value::Map(map) => push_attrs(out, map),
    }
}

/// A JSON number, or `"NaN"`/`"inf"`/`"-inf"` for the values JSON has no number for. `Display`
/// never uses exponent notation, so every finite value is a valid JSON number as written.
fn push_f64(out: &mut String, v: f64) {
    if v.is_finite() {
        let _ = write!(out, "{v}");
    } else if v.is_nan() {
        out.push_str("\"NaN\"");
    } else if v > 0.0 {
        out.push_str("\"inf\"");
    } else {
        out.push_str("\"-inf\"");
    }
}

fn push_f64_array(out: &mut String, values: &[f64]) {
    out.push('[');
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        push_f64(out, *v);
    }
    out.push(']');
}

fn push_timestamp(out: &mut String, nanos: i64) {
    out.push('"');
    write_rfc3339_utc(out, nanos);
    out.push('"');
}

fn push_hex_str(out: &mut String, bytes: &[u8]) {
    out.push('"');
    push_hex(out, bytes);
    out.push('"');
}

/// A byte field that is text by convention: a JSON string, each invalid byte as `\xHH`.
fn push_text_bytes(out: &mut String, bytes: &Bytes) {
    out.push('"');
    push_lossless_bytes_body(out, bytes);
    out.push('"');
}

/// `bytes`' valid UTF-8 runs JSON-escaped, each invalid byte as the text `\xHH`, with no quotes.
/// In JSON source that text is `\\xHH`, so a reader decodes the string to `\xHH`.
fn push_lossless_bytes_body(out: &mut String, bytes: &[u8]) {
    for chunk in bytes.utf8_chunks() {
        push_json_escaped(out, chunk.valid());
        for b in chunk.invalid() {
            let _ = write!(out, "\\\\x{b:02x}");
        }
    }
}

/// `s` as a quoted JSON string.
fn push_json_str(out: &mut String, s: &str) {
    out.push('"');
    push_json_escaped(out, s);
    out.push('"');
}

/// RFC 8259 escaping with no quotes: `"`, `\`, and every control below `0x20`, the common ones
/// by their short form. DEL and non-ASCII pass through raw. Copies each unescaped run with one
/// `push_str`; every escaped byte is ASCII, so each slice boundary is a character boundary.
fn push_json_escaped(out: &mut String, s: &str) {
    let mut start = 0;
    for (i, &b) in s.as_bytes().iter().enumerate() {
        if b != b'"' && b != b'\\' && b >= 0x20 {
            continue;
        }
        out.push_str(&s[start..i]);
        match b {
            b'"' => out.push_str("\\\""),
            b'\\' => out.push_str("\\\\"),
            b'\n' => out.push_str("\\n"),
            b'\r' => out.push_str("\\r"),
            b'\t' => out.push_str("\\t"),
            0x08 => out.push_str("\\b"),
            0x0c => out.push_str("\\f"),
            _ => {
                let _ = write!(out, "\\u{b:04x}");
            }
        }
        start = i + 1;
    }
    out.push_str(&s[start..]);
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;
    use logit_core::{
        BodyFormat, DdSketch, Histogram, HyperLogLog, Samples, Severity, SpanExt, SpanKind,
        SpanStatus, Sum, Summary,
    };
    use std::sync::Arc;

    const T0: &str = "1970-01-01T00:00:00.000000000Z";

    fn batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn render(batch: &EventBatch) -> String {
        let mut out = String::new();
        render_ndjson(&mut out, batch);
        out
    }

    fn render_events(events: Vec<Event>) -> String {
        render(&batch(events))
    }

    /// Interns `keys` in order, so a map built from them iterates in that order however the
    /// process's interner was used before. Each test uses keys no other test uses.
    fn attrs(pairs: &[(&str, Value)]) -> AttrMap {
        for (k, _) in pairs {
            intern(k);
        }
        let mut map = AttrMap::new();
        for (k, v) in pairs {
            map.insert(k, v.clone());
        }
        map
    }

    fn log(message: Value) -> LogRecord {
        LogRecord {
            message,
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        }
    }

    fn metric(kind: MetricKind) -> String {
        let event = Event::metric(0, AttrMap::new(), MetricRecord::new(intern("m"), kind));
        let out = render_events(vec![event]);
        let prefix = format!("{{\"timestamp\":\"{T0}\",\"metrics\":[");
        let body = out.strip_prefix(&prefix).expect("metric line prefix");
        body.strip_suffix("]}\n").expect("metric line suffix").to_string()
    }

    fn value(v: Value) -> String {
        let out = render_events(vec![Event::log(0, AttrMap::new(), log(v))]);
        let prefix = format!("{{\"timestamp\":\"{T0}\",\"log\":{{\"format\":\"raw\",\"message\":");
        let body = out.strip_prefix(&prefix).expect("log line prefix");
        body.strip_suffix("}}\n").expect("log line suffix").to_string()
    }

    fn span_record() -> SpanRecord {
        SpanRecord {
            trace_id: [0xab; 16],
            span_id: [0xcd; 8],
            parent_span_id: None,
            name: Value::str("GET /"),
            kind: SpanKind::Server,
            status: SpanStatus::Ok,
            events: vec![],
            links: vec![],
            end_timestamp: 1_500,
            flags: 0,
            ext: None,
        }
    }

    #[test]
    fn an_empty_event_is_a_timestamp_only_object() {
        assert_eq!(
            render_events(vec![Event::empty(1_000_000_001, AttrMap::new())]),
            "{\"timestamp\":\"1970-01-01T00:00:01.000000001Z\"}\n"
        );
    }

    #[test]
    fn a_log_with_only_a_message() {
        assert_eq!(
            render_events(vec![Event::log(0, AttrMap::new(), log(Value::str("hello")))]),
            format!(
                "{{\"timestamp\":\"{T0}\",\"log\":{{\"format\":\"raw\",\"message\":\"hello\"}}}}\n"
            )
        );
    }

    #[test]
    fn a_log_with_every_optional_field() {
        let record = LogRecord {
            message: Value::str("GET /"),
            severity: Some(Severity::Info),
            body_format: BodyFormat::Json,
            trace: Some(TraceRef { trace_id: [0x4b; 16], span_id: Some([0x00; 8]), flags: 0 }),
            event_name: Some(intern("http.request")),
            observed_timestamp: 2_000_000_000,
            dropped_attributes_count: 2,
        };
        assert_eq!(
            render_events(vec![Event::log(0, AttrMap::new(), record)]),
            format!(
                "{{\"timestamp\":\"{T0}\",\"log\":{{\"severity\":\"info\",\"format\":\"json\",\
                 \"message\":\"GET /\",\"event_name\":\"http.request\",\
                 \"observed_timestamp\":\"1970-01-01T00:00:02.000000000Z\",\
                 \"trace_id\":\"{}\",\"span_id\":\"0000000000000000\",\"trace_flags\":0,\
                 \"dropped_attributes_count\":2}}}}\n",
                "4b".repeat(16)
            )
        );
    }

    #[test]
    fn a_log_trace_with_no_span_id_still_writes_trace_flags() {
        let mut record = log(Value::str("x"));
        record.trace = Some(TraceRef { trace_id: [0x01; 16], span_id: None, flags: 1 });
        let out = render_events(vec![Event::log(0, AttrMap::new(), record)]);
        assert!(out.contains(&format!("\"trace_id\":\"{}\",\"trace_flags\":1}}", "01".repeat(16))));
    }

    #[test]
    fn a_sum() {
        assert_eq!(
            metric(MetricKind::Sum(Sum {
                value: 1.0,
                temporality: Temporality::Cumulative,
                monotonic: false
            })),
            "{\"name\":\"m\",\"kind\":\"sum\",\"value\":1,\"temporality\":\"cumulative\",\
             \"monotonic\":false}"
        );
    }

    #[test]
    fn a_gauge() {
        assert_eq!(
            metric(MetricKind::Gauge(0.25)),
            "{\"name\":\"m\",\"kind\":\"gauge\",\"value\":0.25}"
        );
    }

    #[test]
    fn a_gauge_delta_keeps_its_sign_including_negative_zero() {
        assert_eq!(
            metric(MetricKind::GaugeDelta(-3.5)),
            "{\"name\":\"m\",\"kind\":\"gauge_delta\",\"delta\":-3.5}"
        );
        assert_eq!(
            metric(MetricKind::GaugeDelta(-0.0)),
            "{\"name\":\"m\",\"kind\":\"gauge_delta\",\"delta\":-0}"
        );
    }

    #[test]
    fn samples() {
        let mut samples = Samples::new([1.5, 2.0]);
        samples.sample_rate = 0.5;
        assert_eq!(
            metric(MetricKind::Samples(samples)),
            "{\"name\":\"m\",\"kind\":\"samples\",\"values\":[1.5,2],\"sample_rate\":0.5}"
        );
    }

    #[test]
    fn set_members_write_an_invalid_byte_as_hex_text() {
        let members = vec![Bytes::from_static(b"alice"), Bytes::from_static(b"b\xffo")];
        assert_eq!(
            metric(MetricKind::SetMembers(members)),
            "{\"name\":\"m\",\"kind\":\"set_members\",\"members\":[\"alice\",\"b\\\\xffo\"]}"
        );
    }

    #[test]
    fn a_distribution_with_stats() {
        let mut sketch = DdSketch::new();
        for _ in 0..4 {
            sketch.add(5.0);
        }
        assert_eq!(
            metric(MetricKind::Distribution(sketch)),
            "{\"name\":\"m\",\"kind\":\"distribution\",\"count\":4,\"sum\":20,\"min\":5,\
             \"max\":5,\"avg\":5,\"p50\":5,\"p90\":5,\"p95\":5,\"p99\":5}"
        );
    }

    #[test]
    fn an_empty_distribution_writes_count_and_sum_only() {
        assert_eq!(
            metric(MetricKind::Distribution(DdSketch::new())),
            "{\"name\":\"m\",\"kind\":\"distribution\",\"count\":0,\"sum\":0}"
        );
    }

    #[test]
    fn a_set() {
        let mut hll = HyperLogLog::new();
        hll.insert(b"a");
        hll.insert(b"b");
        assert_eq!(
            metric(MetricKind::Set(hll)),
            "{\"name\":\"m\",\"kind\":\"set\",\"estimate\":2}"
        );
    }

    #[test]
    fn a_histogram_with_an_infinite_bound_and_sum_min_max() {
        let h = Histogram {
            buckets: vec![(0.5, 3), (f64::INFINITY, 1)],
            temporality: Temporality::Delta,
            sum: Some(2.5),
            min: Some(0.1),
            max: Some(9.0),
        };
        assert_eq!(
            metric(MetricKind::Histogram(h)),
            "{\"name\":\"m\",\"kind\":\"histogram\",\"temporality\":\"delta\",\
             \"buckets\":[{\"bound\":0.5,\"count\":3},{\"bound\":\"inf\",\"count\":1}],\
             \"sum\":2.5,\"min\":0.1,\"max\":9}"
        );
    }

    #[test]
    fn an_exponential_histogram_with_both_offsets() {
        let e = ExpHistogram {
            scale: 3,
            zero_count: 1,
            zero_threshold: 0.0,
            positive: (-2, vec![1, 2]),
            negative: (4, vec![5]),
            temporality: Temporality::Cumulative,
            count: 9,
            sum: Some(1.5),
            min: None,
            max: Some(8.0),
        };
        assert_eq!(
            metric(MetricKind::ExponentialHistogram(e)),
            "{\"name\":\"m\",\"kind\":\"exponential_histogram\",\"scale\":3,\
             \"temporality\":\"cumulative\",\"count\":9,\"zero_count\":1,\"zero_threshold\":0,\
             \"positive\":{\"offset\":-2,\"counts\":[1,2]},\"negative\":{\"offset\":4,\"counts\":[5]},\
             \"sum\":1.5,\"max\":8}"
        );
    }

    #[test]
    fn an_exponential_histogram_omits_an_empty_side() {
        let e = ExpHistogram {
            scale: 0,
            zero_count: 0,
            zero_threshold: 0.0,
            positive: (1, vec![]),
            negative: (0, vec![]),
            temporality: Temporality::Delta,
            count: 0,
            sum: None,
            min: None,
            max: None,
        };
        assert_eq!(
            metric(MetricKind::ExponentialHistogram(e)),
            "{\"name\":\"m\",\"kind\":\"exponential_histogram\",\"scale\":0,\
             \"temporality\":\"delta\",\"count\":0,\"zero_count\":0,\"zero_threshold\":0}"
        );
    }

    #[test]
    fn a_summary() {
        let s = Summary { quantiles: vec![(0.5, 1.0), (0.99, 4.25)], count: 10, sum: 12.0 };
        assert_eq!(
            metric(MetricKind::Summary(s)),
            "{\"name\":\"m\",\"kind\":\"summary\",\"count\":10,\"sum\":12,\
             \"quantiles\":[{\"quantile\":0.5,\"value\":1},{\"quantile\":0.99,\"value\":4.25}]}"
        );
    }

    #[test]
    fn metric_unit_description_start_and_flags() {
        let mut record = MetricRecord::new(intern("m"), MetricKind::Gauge(1.0));
        record.unit = Some(intern("ms"));
        record.description = Some(intern("Latency"));
        record.start_timestamp = 1;
        record.flags = 2;
        let out = render_events(vec![Event::metric(0, AttrMap::new(), record)]);
        assert!(
            out.contains(
                "\"value\":1,\"unit\":\"ms\",\"description\":\"Latency\",\
                 \"start_timestamp\":\"1970-01-01T00:00:00.000000001Z\",\"flags\":2}"
            ),
            "got: {out}"
        );
    }

    #[test]
    fn no_recorded_value_suppresses_the_kinds_fields_and_writes_raw_flags() {
        let mut record = MetricRecord::new(
            intern("m"),
            MetricKind::Sum(Sum { value: 0.0, temporality: Temporality::Delta, monotonic: true }),
        );
        record.flags = 3;
        record.unit = Some(intern("1"));
        let out = render_events(vec![Event::metric(0, AttrMap::new(), record)]);
        assert_eq!(
            out,
            format!(
                "{{\"timestamp\":\"{T0}\",\"metrics\":[{{\"name\":\"m\",\"kind\":\"sum\",\
                 \"unit\":\"1\",\"flags\":3,\"no_recorded_value\":true}}]}}\n"
            )
        );
    }

    #[test]
    fn exemplars() {
        let mut record = MetricRecord::new(intern("m"), MetricKind::Gauge(1.0));
        record.exemplars = vec![
            Exemplar {
                timestamp: 5,
                value: 0.25,
                trace: Some(TraceRef { trace_id: [0x11; 16], span_id: Some([0x22; 8]), flags: 0 }),
                filtered_attributes: attrs(&[("exemplar.key", Value::str("v"))]),
            },
            Exemplar {
                timestamp: 0,
                value: f64::NAN,
                trace: None,
                filtered_attributes: AttrMap::new(),
            },
        ];
        let out = render_events(vec![Event::metric(0, AttrMap::new(), record)]);
        assert!(
            out.contains(&format!(
                "\"value\":1,\"exemplars\":[{{\"timestamp\":\"1970-01-01T00:00:00.000000005Z\",\
                 \"value\":0.25,\"trace_id\":\"{}\",\"span_id\":\"{}\",\"trace_flags\":0,\
                 \"attributes\":{{\"exemplar.key\":\"v\"}}}},\
                 {{\"timestamp\":\"{T0}\",\"value\":\"NaN\"}}]}}",
                "11".repeat(16),
                "22".repeat(8)
            )),
            "got: {out}"
        );
    }

    #[test]
    fn a_minimal_span() {
        let out = render_events(vec![Event::span(1_000, AttrMap::new(), span_record())]);
        assert_eq!(
            out,
            format!(
                "{{\"timestamp\":\"1970-01-01T00:00:00.000001000Z\",\"span\":{{\"name\":\"GET /\",\
                 \"trace_id\":\"{}\",\"span_id\":\"{}\",\"kind\":\"server\",\"status\":\"ok\",\
                 \"start\":\"1970-01-01T00:00:00.000001000Z\",\
                 \"end\":\"1970-01-01T00:00:00.000001500Z\",\"duration_ns\":500}}}}\n",
                "ab".repeat(16),
                "cd".repeat(8)
            )
        );
    }

    #[test]
    fn a_span_with_parent_ext_flags_events_and_links() {
        let mut span = span_record();
        span.parent_span_id = Some([0xef; 8]);
        span.status = SpanStatus::Error;
        span.flags = 0x301;
        // An end before the start writes a negative duration.
        span.end_timestamp = 500;
        span.ext = Some(Box::new(SpanExt {
            status_message: Some(Bytes::from_static(b"boom\xfe")),
            trace_state: Some(Bytes::from_static(b"vendor=x")),
            dropped_attributes_count: 1,
            dropped_events_count: 2,
            dropped_links_count: 3,
        }));
        span.events = vec![SpanEvent {
            timestamp: 1_200,
            name: Value::str("retrying"),
            attributes: attrs(&[("span_event.retry", Value::I64(1))]),
            dropped_attributes_count: 4,
        }];
        span.links = vec![
            SpanLink {
                trace_id: [0x01; 16],
                span_id: [0x02; 8],
                attributes: attrs(&[("span_link.k", Value::Bool(true))]),
                flags: 1,
                trace_state: Some(Bytes::from_static(b"a=b")),
                dropped_attributes_count: 5,
            },
            SpanLink {
                trace_id: [0x03; 16],
                span_id: [0x04; 8],
                attributes: AttrMap::new(),
                flags: 0,
                trace_state: None,
                dropped_attributes_count: 0,
            },
        ];
        let out = render_events(vec![Event::span(1_000, AttrMap::new(), span)]);
        assert_eq!(
            out,
            format!(
                "{{\"timestamp\":\"1970-01-01T00:00:00.000001000Z\",\"span\":{{\"name\":\"GET /\",\
                 \"trace_id\":\"{ab}\",\"span_id\":\"{cd}\",\"parent_span_id\":\"efefefefefefefef\",\
                 \"kind\":\"server\",\"status\":\"error\",\"status_message\":\"boom\\\\xfe\",\
                 \"trace_state\":\"vendor=x\",\"flags\":769,\
                 \"start\":\"1970-01-01T00:00:00.000001000Z\",\
                 \"end\":\"1970-01-01T00:00:00.000000500Z\",\"duration_ns\":-500,\
                 \"dropped_attributes_count\":1,\"dropped_events_count\":2,\
                 \"dropped_links_count\":3,\
                 \"events\":[{{\"name\":\"retrying\",\
                 \"timestamp\":\"1970-01-01T00:00:00.000001200Z\",\
                 \"attributes\":{{\"span_event.retry\":1}},\"dropped_attributes_count\":4}}],\
                 \"links\":[{{\"trace_id\":\"{l1}\",\"span_id\":\"0202020202020202\",\"flags\":1,\
                 \"trace_state\":\"a=b\",\"attributes\":{{\"span_link.k\":true}},\
                 \"dropped_attributes_count\":5}},\
                 {{\"trace_id\":\"{l3}\",\"span_id\":\"0404040404040404\"}}]}}}}\n",
                ab = "ab".repeat(16),
                cd = "cd".repeat(8),
                l1 = "01".repeat(16),
                l3 = "03".repeat(16),
            )
        );
    }

    #[test]
    fn a_resource_with_schema_url_and_dropped_count() {
        let resource = Resource {
            attributes: attrs(&[
                ("resource_test.host", Value::str("web-1")),
                ("resource_test.service", Value::str("nginx")),
            ]),
            dropped_attributes_count: 1,
            schema_url: Some(Bytes::from_static(b"https://example.com/s")),
        };
        let batch = EventBatch {
            resource: Arc::new(resource),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        };
        assert_eq!(
            render(&batch),
            format!(
                "{{\"timestamp\":\"{T0}\",\"resource\":{{\"attributes\":\
                 {{\"resource_test.host\":\"web-1\",\"resource_test.service\":\"nginx\"}},\
                 \"schema_url\":\"https://example.com/s\",\"dropped_attributes_count\":1}}}}\n"
            )
        );
    }

    #[test]
    fn a_resource_with_only_a_dropped_count_is_still_written() {
        let resource = Resource { dropped_attributes_count: 7, ..Resource::default() };
        let batch = EventBatch {
            resource: Arc::new(resource),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        };
        assert_eq!(
            render(&batch),
            format!("{{\"timestamp\":\"{T0}\",\"resource\":{{\"dropped_attributes_count\":7}}}}\n")
        );
    }

    #[test]
    fn a_scope() {
        let full = Scope {
            name: Bytes::from_static(b"io.opentelemetry.nginx"),
            version: Bytes::from_static(b"1.2.0"),
            attributes: attrs(&[("scope_test.k", Value::str("v"))]),
            dropped_attributes_count: 1,
            schema_url: Some(Bytes::from_static(b"https://example.com/scope")),
        };
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: Some(Arc::new(full)),
            events: vec![Event::empty(0, AttrMap::new())],
        };
        assert_eq!(
            render(&batch),
            format!(
                "{{\"timestamp\":\"{T0}\",\"scope\":{{\"name\":\"io.opentelemetry.nginx\",\
                 \"version\":\"1.2.0\",\"attributes\":{{\"scope_test.k\":\"v\"}},\
                 \"schema_url\":\"https://example.com/scope\",\"dropped_attributes_count\":1}}}}\n"
            )
        );

        let empty = Scope {
            name: Bytes::new(),
            version: Bytes::new(),
            attributes: AttrMap::new(),
            dropped_attributes_count: 0,
            schema_url: None,
        };
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: Some(Arc::new(empty)),
            events: vec![Event::empty(0, AttrMap::new())],
        };
        assert_eq!(render(&batch), format!("{{\"timestamp\":\"{T0}\",\"scope\":{{}}}}\n"));
    }

    #[test]
    fn nested_map_and_array_attributes() {
        let inner = attrs(&[("nested_test.iut", Value::str("3"))]);
        let sd = attrs(&[("nested_test.sdid", Value::Map(Box::new(inner)))]);
        let event = Event::empty(
            0,
            attrs(&[
                ("nested_test.sd", Value::Map(Box::new(sd))),
                (
                    "nested_test.tags",
                    Value::Array(vec![Value::str("a"), Value::U64(2), Value::Null]),
                ),
                ("nested_test.at", Value::Timestamp(3)),
            ]),
        );
        assert_eq!(
            render_events(vec![event]),
            format!(
                "{{\"timestamp\":\"{T0}\",\"attributes\":{{\
                 \"nested_test.sd\":{{\"nested_test.sdid\":{{\"nested_test.iut\":\"3\"}}}},\
                 \"nested_test.tags\":[\"a\",2,null],\
                 \"nested_test.at\":\"1970-01-01T00:00:00.000000003Z\"}}}}\n"
            )
        );
    }

    #[test]
    fn scalar_values() {
        assert_eq!(value(Value::Null), "null");
        assert_eq!(value(Value::Bool(false)), "false");
        assert_eq!(value(Value::I64(-7)), "-7");
        assert_eq!(value(Value::U64(u64::MAX)), "18446744073709551615");
        assert_eq!(value(Value::F64(1e21)), "1000000000000000000000");
    }

    #[test]
    fn bytes_valid_and_invalid() {
        assert_eq!(value(Value::Bytes(Bytes::from_static(b"abc"))), "\"b\\\"abc\\\"\"");
        assert_eq!(
            value(Value::Bytes(Bytes::from_static(b"a\xff\x00\"z"))),
            "\"b\\\"a\\\\xff\\u0000\\\"z\\\"\""
        );
        assert_eq!(value(Value::Bytes(Bytes::new())), "\"b\\\"\\\"\"");
    }

    #[test]
    fn string_escaping() {
        assert_eq!(
            value(Value::str("q\"b\\n\nr\rt\tbs\u{8}ff\u{c}esc\u{1b}nul\u{0}del\u{7f}é✓")),
            "\"q\\\"b\\\\n\\nr\\rt\\tbs\\bff\\fesc\\u001bnul\\u0000del\u{7f}é✓\""
        );
    }

    #[test]
    fn escaped_keys() {
        let event = Event::empty(0, attrs(&[("escape_test.\"k\"\n", Value::I64(1))]));
        assert_eq!(
            render_events(vec![event]),
            format!(
                "{{\"timestamp\":\"{T0}\",\"attributes\":{{\"escape_test.\\\"k\\\"\\n\":1}}}}\n"
            )
        );
    }

    #[test]
    fn non_finite_floats_are_strings() {
        assert_eq!(value(Value::F64(f64::NAN)), "\"NaN\"");
        assert_eq!(value(Value::F64(f64::INFINITY)), "\"inf\"");
        assert_eq!(value(Value::F64(f64::NEG_INFINITY)), "\"-inf\"");
        assert_eq!(
            metric(MetricKind::Gauge(f64::NEG_INFINITY)),
            "{\"name\":\"m\",\"kind\":\"gauge\",\"value\":\"-inf\"}"
        );
    }

    #[test]
    fn a_multi_event_batch_is_one_line_per_event_in_order() {
        let events = (0..3).map(|i| Event::empty(i, AttrMap::new())).collect();
        let out = render_events(events);
        let lines: Vec<&str> = out.split_terminator('\n').collect();
        assert_eq!(lines.len(), 3);
        for (i, line) in lines.iter().enumerate() {
            assert_eq!(*line, format!("{{\"timestamp\":\"1970-01-01T00:00:00.00000000{i}Z\"}}"));
        }
        assert!(out.ends_with('\n'));
        assert_eq!(render_events(vec![]), "");
    }

    fn mixed_batch() -> EventBatch {
        let mut event = Event::empty(
            1_790_598_896_789_012_345,
            attrs(&[
                ("mixed_test.method", Value::str("GET")),
                ("mixed_test.status", Value::I64(200)),
            ]),
        );
        let mut record = log(Value::str("GET /index.html HTTP/1.1"));
        record.severity = Some(Severity::Info);
        event.log = Some(record);
        event
            .metrics
            .push(MetricRecord::new(intern("mixed_test.requests"), MetricKind::counter(1.0)));
        let mut span = span_record();
        span.end_timestamp = 1_790_598_896_801_312_345;
        event.span = Some(span);
        let resource = Resource {
            attributes: attrs(&[("mixed_test.host", Value::str("web-1"))]),
            dropped_attributes_count: 0,
            schema_url: None,
        };
        let scope = Scope {
            name: Bytes::from_static(b"io.opentelemetry.nginx"),
            version: Bytes::from_static(b"1.2.0"),
            attributes: AttrMap::new(),
            dropped_attributes_count: 0,
            schema_url: None,
        };
        EventBatch {
            resource: Arc::new(resource),
            scope: Some(Arc::new(scope)),
            events: vec![event, Event::empty(0, AttrMap::new())],
        }
    }

    #[test]
    fn a_mixed_log_metric_span_event_with_resource_and_scope() {
        let out = render(&mixed_batch());
        let first = out.lines().next().expect("a line");
        assert_eq!(
            first,
            format!(
                "{{\"timestamp\":\"2026-09-28T12:34:56.789012345Z\",\
                 \"log\":{{\"severity\":\"info\",\"format\":\"raw\",\
                 \"message\":\"GET /index.html HTTP/1.1\"}},\
                 \"metrics\":[{{\"name\":\"mixed_test.requests\",\"kind\":\"sum\",\"value\":1,\
                 \"temporality\":\"delta\",\"monotonic\":true}}],\
                 \"span\":{{\"name\":\"GET /\",\"trace_id\":\"{}\",\"span_id\":\"{}\",\
                 \"kind\":\"server\",\"status\":\"ok\",\
                 \"start\":\"2026-09-28T12:34:56.789012345Z\",\
                 \"end\":\"2026-09-28T12:34:56.801312345Z\",\"duration_ns\":12300000}},\
                 \"attributes\":{{\"mixed_test.method\":\"GET\",\"mixed_test.status\":200}},\
                 \"resource\":{{\"attributes\":{{\"mixed_test.host\":\"web-1\"}}}},\
                 \"scope\":{{\"name\":\"io.opentelemetry.nginx\",\"version\":\"1.2.0\"}}}}",
                "ab".repeat(16),
                "cd".repeat(8)
            )
        );
    }

    #[test]
    fn every_line_parses_as_json() {
        let mut events = mixed_batch().events;
        let mut odd = Event::empty(
            0,
            attrs(&[
                ("parse_test.bytes", Value::Bytes(Bytes::from_static(b"\x00\xff\"\\"))),
                ("parse_test.str", Value::str("\u{1b}[31m\u{7f}\"\\\n")),
                ("parse_test.nan", Value::F64(f64::NAN)),
                ("parse_test.neg_zero", Value::F64(-0.0)),
                ("parse_test.tiny", Value::F64(f64::MIN_POSITIVE)),
                ("parse_test.huge", Value::F64(f64::MAX)),
            ]),
        );
        odd.metrics.push(MetricRecord::new(
            intern("parse_test.h"),
            MetricKind::Histogram(Histogram {
                buckets: vec![(f64::NEG_INFINITY, 1), (f64::INFINITY, 2)],
                temporality: Temporality::Delta,
                sum: Some(f64::NAN),
                min: None,
                max: None,
            }),
        ));
        odd.metrics.push(MetricRecord::new(
            intern("parse_test.members"),
            MetricKind::SetMembers(vec![Bytes::from_static(b"\xc3\x28")]),
        ));
        events.push(odd);
        let out = render(&batch(events));
        let mut n = 0;
        for line in out.lines() {
            let parsed: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("line should parse ({e}): {line}"));
            assert!(parsed.is_object());
            n += 1;
        }
        assert_eq!(n, 3);
    }
}
