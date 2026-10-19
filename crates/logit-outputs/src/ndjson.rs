//! `format: json` for `stdio_out`/`file_out`: one JSON object per event, one per line, for a
//! program to read. `kafka_out`'s `format: json` sends each object as one record's value, or a
//! run of these lines as one record under `record: batch`. This doc is the canonical grammar.
//!
//! The form is lossless: a line reads back to the [`Event`], [`Resource`], and [`Scope`] that
//! wrote it, through `logit_proto::event_json` (whose module doc has the reader's side), and
//! `docs/adr/stream-json-format.md` records why. Every key a reader read before still means
//! what it meant; the lossless additions are keys of their own.
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
//!   `flags` bit 0 is set (the kind's fields are still written, as decoded), `exemplars` (each
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
//! - `types`: the type table below, when the line has an entry for it
//!
//! | `kind` | fields, in order |
//! |---|---|
//! | `sum` | `value`, `temporality` (`delta`/`cumulative`), `monotonic` |
//! | `gauge` | `value` |
//! | `gauge_delta` | `delta` (a negative zero writes `-0.0`) |
//! | `samples` | `values` (array), `sample_rate` |
//! | `set_members` | `members` (array of strings, invalid bytes as `\xHH`) |
//! | `distribution` | `count`, `sum`; then `min`, `max`, `avg` (`sum / count`), `p50`, `p90`, `p95`, `p99` when the sketch is non-empty; then `sketch` (below) |
//! | `set` | `estimate`, `hll` (the `HyperLogLog`'s `to_bytes`, standard base64) |
//! | `histogram` | `temporality`, `buckets` (`[{"bound": b, "count": n}]` in wire order, each count the bucket's own), then `sum`, `min`, `max` when present |
//! | `exponential_histogram` | `scale`, `temporality`, `count`, `zero_count`, `zero_threshold`, `positive`, `negative` (each `{"offset": o, "counts": [..]}`, omitted when its counts are empty and its offset `0`), then `sum`, `min`, `max` when present |
//! | `summary` | `count`, `sum`, `quantiles` (`[{"quantile": q, "value": v}]` in wire order) |
//!
//! A distribution's `sketch` is the `DdSketch` as stored, which the statistics above are read
//! from: `mapping` (`agent`, or `logarithmic` followed by its `gamma`, `index_offset`, and
//! `bin_limit`), `stats_exact`, the stored `count` (an `f64`, unrounded), `min` and `max` (`"inf"`
//! and `"-inf"` when the sketch is empty), `zero_count` (omitted when `0`), then `positive` and
//! `negative`, each `[{"key": k, "count": c}]` ascending by key and omitted when empty. The
//! distribution's `sum` is the stored one.
//!
//! A `sum`'s `value`, a `gauge`'s `value`, a `gauge_delta`'s `delta`, and an exemplar's `value`
//! are a `MetricValue` (`docs/adr/typed-metric-scalar.md`), written in its own JSON type: a
//! number for `F64` (the number rule below applies), an integer for `I64` and `U64` (a `U64`
//! above 2^53 keeps every digit), and a boolean for `Bool`.
//!
//! Every `f64` in the object is a JSON number with a fraction or an exponent (`3.0`, `0.25`,
//! `1e21`, `-0.0`), the shortest that parses back to the same bits, so its spelling never
//! matches an integer's. A non-finite one, which JSON has no number for, is the string `"NaN"`,
//! `"inf"`, or `"-inf"`, bucket bounds included.
//!
//! Values: `Null`, `Bool`, `I64`, `U64` are JSON's own; `F64` is a number under the rule above;
//! `Str` is a JSON string; `Timestamp` is an RFC 3339 string; `Array` an array; `Map` an object in
//! `AttrMap` order. `Bytes` is a JSON string holding the text `b"..."`: valid UTF-8 runs as text,
//! each invalid byte and each backslash as `\xHH`. Byte fields that are text by convention
//! (`status_message`, `trace_state`, `schema_url`, scope `name`/`version`, a set member) are
//! strings under the same escaping, with no `b` prefix. Every timestamp is RFC 3339 UTC with nine
//! fractional digits; every trace and span id is lowercase hex.
//!
//! **The `types` table** names the type of each value whose spelling doesn't: an object from an
//! RFC 6901 JSON Pointer into the line (`/attributes/count`, `/metrics/0/value`,
//! `/span/events/1/attributes/k`, `/attributes/tags/2`) to one of these tags, each pointer once,
//! in walk order. The line omits it when no value needs an entry.
//!
//! | Tag | Written for |
//! |---|---|
//! | `u64` | a `Value::U64`, and a `MetricValue::U64` in a `value` or `delta` |
//! | `f64` | a non-finite `Value::F64`, which is a string |
//! | `bytes` | a `Value::Bytes` |
//! | `timestamp` | a `Value::Timestamp` |
//!
//! It covers the values under `attributes` (the event's, the resource's, the scope's, an
//! exemplar's, a span event's or link's), `log`'s `message`, `span`'s and a span event's `name`,
//! and the metric values above. A field with one type in the model needs no entry: a count is a
//! `u64` and a bound an `f64` whatever their spelling.
//!
//! Strings follow RFC 8259: `\"`, `\\`, `\n`, `\r`, `\t`, `\b`, `\f`, every other control
//! character as `\u00XX`; DEL and non-ASCII are written raw as UTF-8.
//!
//! Readers that mirror this grammar: `tools/shape-survey/summarize.py`,
//! `tools/shape-survey/check_interop.py`, `tools/shape-survey/producers/oteldemo.sh`'s Python,
//! `tools/splunk-interop/check.py`, `tools/victoria-interop/check.py`, and
//! `tools/soak/soaklib/telemetry.py`. A change here is mirrored there.
//!
//! One walker visits every field through [`TreeWriter`]: [`Json`] writes this grammar, and
//! `crate::msgpack` writes the same object as MessagePack for `kafka_out`'s `format: msgpack`, so a
//! change to the keys here changes that format too. [`Json`] writes straight into the caller's
//! `String` with `push_str`/`write!`: no per-field `String`, no serde (`docs/design/memory.md`).

use base64::Engine;
use logit_core::interner::resolve;
use logit_core::time::write_rfc3339_utc;
use logit_core::trace::push_hex;
use logit_core::{
    AttrMap, DdSketch, Event, EventBatch, Exemplar, ExpHistogram, LogRecord, MappingKind,
    MetricKind, MetricRecord, MetricValue, Resource, Scope, SpanEvent, SpanLink, SpanRecord,
    Temporality, TraceRef, Value,
};
use std::fmt::Write;

/// Appends one JSON line per event of `batch` to `out`, in batch order. Never fails.
pub fn render_ndjson(out: &mut String, batch: &EventBatch) {
    for event in &batch.events {
        render_event(out, &batch.resource, batch.scope.as_deref(), event);
        out.push('\n');
    }
}

/// Appends one event's object, without the newline: the line [`render_ndjson`] writes, which
/// `kafka_out`'s `format: json` sends as a record's value.
pub(crate) fn render_event(
    out: &mut String,
    resource: &Resource,
    scope: Option<&Scope>,
    event: &Event,
) {
    walk_event(&mut Json(out), resource, scope, event);
}

/// Appends `value` in this grammar's value form: `kafka_out`'s `format: text` writes a map or
/// array log body this way.
pub(crate) fn render_value(out: &mut String, value: &Value) {
    walk_value(&mut Json(out), value);
}

/// What the event walker writes each node of an event's object through. [`Json`] writes this
/// module's grammar and `crate::msgpack::Msgpack` writes the same tree as MessagePack; one walker
/// visits every field for both, so the two can't disagree on which keys appear or their order.
pub(crate) trait TreeWriter {
    /// A map being written: what the writer needs to separate or count its entries.
    type Map;
    fn open_map(&mut self) -> Self::Map;
    /// Starts an entry under a static ASCII key, which needs no escaping.
    fn key(&mut self, map: &mut Self::Map, key: &'static str);
    /// Starts an entry under an attribute's key.
    fn attr_key(&mut self, map: &mut Self::Map, key: &str);
    fn close_map(&mut self, map: Self::Map);
    /// Starts an array of `len` items, each preceded by [`TreeWriter::item`].
    fn open_array(&mut self, len: usize);
    fn item(&mut self, index: usize);
    fn close_array(&mut self);
    fn null(&mut self);
    fn bool(&mut self, v: bool);
    fn i64(&mut self, v: i64);
    fn u64(&mut self, v: u64);
    fn f64(&mut self, v: f64);
    /// A `Value::Str` or a static name.
    fn str(&mut self, s: &str);
    /// A `Value::Bytes`.
    fn bytes(&mut self, bytes: &[u8]);
    /// A byte field that is text by convention: `status_message`, `trace_state`, `schema_url`,
    /// scope `name`/`version`, and a set member.
    fn text_bytes(&mut self, bytes: &[u8]);
    /// Nanoseconds since the Unix epoch.
    fn timestamp(&mut self, nanos: i64);
    /// A trace or span id.
    fn hex(&mut self, bytes: &[u8]);
    /// Starts an entry of the `types` table under `path` as an RFC 6901 JSON Pointer.
    fn pointer_key(&mut self, map: &mut Self::Map, path: &Path<'_>);
}

/// One step of a JSON Pointer into the event's object: an object key or an array index.
#[derive(Clone, Copy)]
pub(crate) enum Seg<'a> {
    Key(&'a str),
    Index(usize),
}

/// A JSON Pointer as a chain of segments on the stack, so building one allocates nothing.
pub(crate) struct Path<'a> {
    parent: Option<&'a Path<'a>>,
    seg: Seg<'a>,
}

impl<'a> Path<'a> {
    fn child(&'a self, seg: Seg<'a>) -> Path<'a> {
        Path { parent: Some(self), seg }
    }

    /// Calls `f` with each segment, root first.
    pub(crate) fn for_each(&self, f: &mut impl FnMut(Seg<'_>)) {
        if let Some(parent) = self.parent {
            parent.for_each(f);
        }
        f(self.seg);
    }
}

/// `seg`'s pointer text: `/`, then an index's digits or a key with `~` as `~0` and `/` as `~1`.
pub(crate) fn push_pointer_seg(out: &mut String, seg: Seg<'_>) {
    out.push('/');
    match seg {
        Seg::Index(i) => {
            let _ = write!(out, "{i}");
        }
        Seg::Key(key) => {
            for c in key.chars() {
                match c {
                    '~' => out.push_str("~0"),
                    '/' => out.push_str("~1"),
                    c => out.push(c),
                }
            }
        }
    }
}

/// The JSON writer: this module's grammar, into a `String`.
pub(crate) struct Json<'a>(pub(crate) &'a mut String);

impl Json<'_> {
    /// The comma before every entry but a map's first. The map state is `true` until a key is
    /// written.
    fn separate(&mut self, first: &mut bool) {
        if !*first {
            self.0.push(',');
        }
        *first = false;
    }
}

impl TreeWriter for Json<'_> {
    type Map = bool;

    fn open_map(&mut self) -> bool {
        self.0.push('{');
        true
    }

    fn key(&mut self, first: &mut bool, key: &'static str) {
        self.separate(first);
        self.0.push('"');
        self.0.push_str(key);
        self.0.push_str("\":");
    }

    fn attr_key(&mut self, first: &mut bool, key: &str) {
        self.separate(first);
        push_json_str(self.0, key);
        self.0.push(':');
    }

    fn close_map(&mut self, _: bool) {
        self.0.push('}');
    }

    fn open_array(&mut self, _len: usize) {
        self.0.push('[');
    }

    fn item(&mut self, index: usize) {
        if index > 0 {
            self.0.push(',');
        }
    }

    fn close_array(&mut self) {
        self.0.push(']');
    }

    fn null(&mut self) {
        self.0.push_str("null");
    }

    fn bool(&mut self, v: bool) {
        self.0.push_str(if v { "true" } else { "false" });
    }

    fn i64(&mut self, v: i64) {
        let _ = write!(self.0, "{v}");
    }

    fn u64(&mut self, v: u64) {
        let _ = write!(self.0, "{v}");
    }

    /// A JSON number, or `"NaN"`/`"inf"`/`"-inf"` for the values JSON has no number for.
    /// `Debug` writes the shortest text that parses back to `v` and always includes a `.` or an
    /// `e` (`3.0`, `1e21`, `-0.0`), so a reader tells an `F64` from an integer by its spelling.
    /// Every form it writes is a valid JSON number.
    fn f64(&mut self, v: f64) {
        if v.is_finite() {
            let _ = write!(self.0, "{v:?}");
        } else if v.is_nan() {
            self.0.push_str("\"NaN\"");
        } else if v > 0.0 {
            self.0.push_str("\"inf\"");
        } else {
            self.0.push_str("\"-inf\"");
        }
    }

    fn str(&mut self, s: &str) {
        push_json_str(self.0, s);
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.0.push_str("\"b\\\"");
        push_lossless_bytes_body(self.0, bytes);
        self.0.push_str("\\\"\"");
    }

    /// A JSON string, each invalid byte as `\xHH`.
    fn text_bytes(&mut self, bytes: &[u8]) {
        self.0.push('"');
        push_lossless_bytes_body(self.0, bytes);
        self.0.push('"');
    }

    fn timestamp(&mut self, nanos: i64) {
        self.0.push('"');
        write_rfc3339_utc(self.0, nanos);
        self.0.push('"');
    }

    fn hex(&mut self, bytes: &[u8]) {
        self.0.push('"');
        push_hex(self.0, bytes);
        self.0.push('"');
    }

    fn pointer_key(&mut self, first: &mut bool, path: &Path<'_>) {
        self.separate(first);
        self.0.push('"');
        path.for_each(&mut |seg| match seg {
            Seg::Index(i) => {
                let _ = write!(self.0, "/{i}");
            }
            Seg::Key(key) => {
                self.0.push('/');
                for part in key.split_inclusive(['~', '/']) {
                    let (text, special) = match part.as_bytes().last() {
                        Some(b'~') => (&part[..part.len() - 1], "~0"),
                        Some(b'/') => (&part[..part.len() - 1], "~1"),
                        _ => (part, ""),
                    };
                    push_json_escaped(self.0, text);
                    self.0.push_str(special);
                }
            }
        });
        self.0.push_str("\":");
    }
}

pub(crate) fn walk_event<W: TreeWriter>(
    w: &mut W,
    resource: &Resource,
    scope: Option<&Scope>,
    event: &Event,
) {
    let mut obj = w.open_map();
    w.key(&mut obj, "timestamp");
    w.timestamp(event.timestamp);
    if let Some(log) = &event.log {
        w.key(&mut obj, "log");
        walk_log(w, log);
    }
    if !event.metrics.is_empty() {
        w.key(&mut obj, "metrics");
        walk_array(w, event.metrics.iter(), walk_metric);
    }
    if let Some(span) = &event.span {
        w.key(&mut obj, "span");
        walk_span(w, event.timestamp, span);
    }
    if !event.attributes.is_empty() {
        w.key(&mut obj, "attributes");
        walk_attrs(w, &event.attributes);
    }
    if !resource.attributes.is_empty()
        || resource.schema_url.is_some()
        || resource.dropped_attributes_count != 0
    {
        w.key(&mut obj, "resource");
        let mut res = w.open_map();
        if !resource.attributes.is_empty() {
            w.key(&mut res, "attributes");
            walk_attrs(w, &resource.attributes);
        }
        if let Some(url) = &resource.schema_url {
            w.key(&mut res, "schema_url");
            w.text_bytes(url);
        }
        push_nonzero(w, &mut res, "dropped_attributes_count", resource.dropped_attributes_count);
        w.close_map(res);
    }
    if let Some(scope) = scope {
        w.key(&mut obj, "scope");
        let mut sc = w.open_map();
        if !scope.name.is_empty() {
            w.key(&mut sc, "name");
            w.text_bytes(&scope.name);
        }
        if !scope.version.is_empty() {
            w.key(&mut sc, "version");
            w.text_bytes(&scope.version);
        }
        if !scope.attributes.is_empty() {
            w.key(&mut sc, "attributes");
            walk_attrs(w, &scope.attributes);
        }
        if let Some(url) = &scope.schema_url {
            w.key(&mut sc, "schema_url");
            w.text_bytes(url);
        }
        push_nonzero(w, &mut sc, "dropped_attributes_count", scope.dropped_attributes_count);
        w.close_map(sc);
    }
    walk_types(w, &mut obj, resource, scope, event);
    w.close_map(obj);
}

/// An array of `items`, each written by `item`.
fn walk_array<W: TreeWriter, T>(
    w: &mut W,
    items: impl ExactSizeIterator<Item = T>,
    mut item: impl FnMut(&mut W, T),
) {
    w.open_array(items.len());
    for (i, it) in items.enumerate() {
        w.item(i);
        item(w, it);
    }
    w.close_array();
}

fn walk_log<W: TreeWriter>(w: &mut W, log: &LogRecord) {
    let mut obj = w.open_map();
    if let Some(severity) = log.severity {
        w.key(&mut obj, "severity");
        w.str(severity.as_str());
    }
    w.key(&mut obj, "format");
    w.str(log.body_format.as_str());
    w.key(&mut obj, "message");
    walk_value(w, &log.message);
    if let Some(name) = log.event_name {
        w.key(&mut obj, "event_name");
        w.str(resolve(name));
    }
    if log.observed_timestamp != 0 {
        w.key(&mut obj, "observed_timestamp");
        w.timestamp(log.observed_timestamp);
    }
    if let Some(trace) = &log.trace {
        walk_trace_ref(w, &mut obj, trace);
    }
    push_nonzero(w, &mut obj, "dropped_attributes_count", log.dropped_attributes_count);
    w.close_map(obj);
}

/// `trace_id`, `span_id` when present, and `trace_flags` always: a log's or an exemplar's
/// [`TraceRef`].
fn walk_trace_ref<W: TreeWriter>(w: &mut W, obj: &mut W::Map, trace: &TraceRef) {
    w.key(obj, "trace_id");
    w.hex(&trace.trace_id);
    if let Some(span_id) = &trace.span_id {
        w.key(obj, "span_id");
        w.hex(span_id);
    }
    w.key(obj, "trace_flags");
    w.u64(u64::from(trace.flags));
}

fn walk_metric<W: TreeWriter>(w: &mut W, metric: &MetricRecord) {
    let mut obj = w.open_map();
    w.key(&mut obj, "name");
    w.str(resolve(metric.name));
    w.key(&mut obj, "kind");
    w.str(kind_name(&metric.kind));
    walk_kind_fields(w, &mut obj, &metric.kind);
    if let Some(unit) = metric.unit {
        w.key(&mut obj, "unit");
        w.str(resolve(unit));
    }
    if let Some(description) = metric.description {
        w.key(&mut obj, "description");
        w.str(resolve(description));
    }
    if metric.start_timestamp != 0 {
        w.key(&mut obj, "start_timestamp");
        w.timestamp(metric.start_timestamp);
    }
    push_nonzero(w, &mut obj, "flags", metric.flags);
    if metric.is_no_recorded_value() {
        w.key(&mut obj, "no_recorded_value");
        w.bool(true);
    }
    if !metric.exemplars.is_empty() {
        w.key(&mut obj, "exemplars");
        walk_array(w, metric.exemplars.iter(), walk_exemplar);
    }
    w.close_map(obj);
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

/// `key: v` for an `f64` field.
fn push_f64<W: TreeWriter>(w: &mut W, obj: &mut W::Map, key: &'static str, v: f64) {
    w.key(obj, key);
    w.f64(v);
}

/// `key: v` for a metric value, in its own JSON type: a number for `F64`, an integer for `I64` and
/// `U64`, a boolean for `Bool`.
fn push_metric_value<W: TreeWriter>(
    w: &mut W,
    obj: &mut W::Map,
    key: &'static str,
    v: MetricValue,
) {
    w.key(obj, key);
    match v {
        MetricValue::F64(f) => w.f64(f),
        MetricValue::I64(n) => w.i64(n),
        MetricValue::U64(n) => w.u64(n),
        MetricValue::Bool(b) => w.bool(b),
    }
}

/// `key: n` for an unsigned integer field.
fn push_u64<W: TreeWriter>(w: &mut W, obj: &mut W::Map, key: &'static str, n: u64) {
    w.key(obj, key);
    w.u64(n);
}

fn walk_kind_fields<W: TreeWriter>(w: &mut W, obj: &mut W::Map, kind: &MetricKind) {
    match kind {
        MetricKind::Sum(s) => {
            push_metric_value(w, obj, "value", s.value);
            push_temporality(w, obj, s.temporality);
            w.key(obj, "monotonic");
            w.bool(s.monotonic);
        }
        MetricKind::Gauge(v) => push_metric_value(w, obj, "value", *v),
        MetricKind::GaugeDelta(v) => push_metric_value(w, obj, "delta", *v),
        MetricKind::Samples(s) => {
            w.key(obj, "values");
            walk_array(w, s.values.iter(), |w, v| w.f64(*v));
            push_f64(w, obj, "sample_rate", s.sample_rate);
        }
        MetricKind::SetMembers(members) => {
            w.key(obj, "members");
            walk_array(w, members.iter(), |w, member| w.text_bytes(member));
        }
        MetricKind::Distribution(sketch) => {
            let count = sketch.count();
            push_u64(w, obj, "count", count as u64);
            push_f64(w, obj, "sum", sketch.sum());
            // `quantile` is `None` only for an empty sketch, and `min`/`max` with it.
            if let Some(p50) = sketch.quantile(0.5) {
                if let Some(min) = sketch.min() {
                    push_f64(w, obj, "min", min);
                }
                if let Some(max) = sketch.max() {
                    push_f64(w, obj, "max", max);
                }
                push_f64(w, obj, "avg", sketch.sum() / count as f64);
                push_f64(w, obj, "p50", p50);
                for (key, q) in [("p90", 0.9), ("p95", 0.95), ("p99", 0.99)] {
                    if let Some(v) = sketch.quantile(q) {
                        push_f64(w, obj, key, v);
                    }
                }
            }
            w.key(obj, "sketch");
            walk_sketch(w, sketch);
        }
        MetricKind::Set(hll) => {
            push_u64(w, obj, "estimate", hll.estimate());
            w.key(obj, "hll");
            w.str(&base64::engine::general_purpose::STANDARD.encode(hll.to_bytes()));
        }
        MetricKind::Histogram(h) => {
            push_temporality(w, obj, h.temporality);
            w.key(obj, "buckets");
            walk_array(w, h.buckets.iter(), |w, (bound, count)| {
                let mut bucket = w.open_map();
                push_f64(w, &mut bucket, "bound", *bound);
                push_u64(w, &mut bucket, "count", *count);
                w.close_map(bucket);
            });
            push_sum_min_max(w, obj, h.sum, h.min, h.max);
        }
        MetricKind::ExponentialHistogram(e) => walk_exp_histogram(w, obj, e),
        MetricKind::Summary(s) => {
            push_u64(w, obj, "count", s.count);
            push_f64(w, obj, "sum", s.sum);
            w.key(obj, "quantiles");
            walk_array(w, s.quantiles.iter(), |w, (q, v)| {
                let mut quantile = w.open_map();
                push_f64(w, &mut quantile, "quantile", *q);
                push_f64(w, &mut quantile, "value", *v);
                w.close_map(quantile);
            });
        }
    }
}

/// A distribution's sketch as stored, which [`walk_kind_fields`]' statistics are read from:
/// enough to rebuild it bin for bin.
fn walk_sketch<W: TreeWriter>(w: &mut W, sketch: &DdSketch) {
    let mut obj = w.open_map();
    let mapping = sketch.mapping();
    w.key(&mut obj, "mapping");
    match mapping.kind() {
        MappingKind::Agent => w.str("agent"),
        MappingKind::Logarithmic => {
            w.str("logarithmic");
            push_f64(w, &mut obj, "gamma", mapping.gamma());
            push_f64(w, &mut obj, "index_offset", mapping.index_offset());
            push_u64(w, &mut obj, "bin_limit", u64::from(mapping.bin_limit()));
        }
    }
    w.key(&mut obj, "stats_exact");
    w.bool(sketch.stats_exact());
    let stats = sketch.stored_stats();
    push_f64(w, &mut obj, "count", stats.count);
    push_f64(w, &mut obj, "min", stats.min);
    push_f64(w, &mut obj, "max", stats.max);
    if sketch.zero_count() != 0.0 {
        push_f64(w, &mut obj, "zero_count", sketch.zero_count());
    }
    for (key, bins) in [("positive", sketch.positive_bins()), ("negative", sketch.negative_bins())]
    {
        if bins.is_empty() {
            continue;
        }
        w.key(&mut obj, key);
        walk_array(w, bins.iter(), |w, bin| {
            let mut b = w.open_map();
            w.key(&mut b, "key");
            w.i64(i64::from(bin.key));
            push_f64(w, &mut b, "count", bin.count);
            w.close_map(b);
        });
    }
    w.close_map(obj);
}

fn walk_exp_histogram<W: TreeWriter>(w: &mut W, obj: &mut W::Map, e: &ExpHistogram) {
    w.key(obj, "scale");
    w.i64(i64::from(e.scale));
    push_temporality(w, obj, e.temporality);
    push_u64(w, obj, "count", e.count);
    push_u64(w, obj, "zero_count", e.zero_count);
    push_f64(w, obj, "zero_threshold", e.zero_threshold);
    for (key, (offset, counts)) in [("positive", &e.positive), ("negative", &e.negative)] {
        if counts.is_empty() && *offset == 0 {
            continue;
        }
        w.key(obj, key);
        let mut side = w.open_map();
        w.key(&mut side, "offset");
        w.i64(i64::from(*offset));
        w.key(&mut side, "counts");
        walk_array(w, counts.iter(), |w, count| w.u64(*count));
        w.close_map(side);
    }
    push_sum_min_max(w, obj, e.sum, e.min, e.max);
}

fn push_temporality<W: TreeWriter>(w: &mut W, obj: &mut W::Map, temporality: Temporality) {
    w.key(obj, "temporality");
    w.str(temporality.as_str());
}

fn push_sum_min_max<W: TreeWriter>(
    w: &mut W,
    obj: &mut W::Map,
    sum: Option<f64>,
    min: Option<f64>,
    max: Option<f64>,
) {
    for (key, v) in [("sum", sum), ("min", min), ("max", max)] {
        if let Some(v) = v {
            push_f64(w, obj, key, v);
        }
    }
}

fn walk_exemplar<W: TreeWriter>(w: &mut W, exemplar: &Exemplar) {
    let mut obj = w.open_map();
    w.key(&mut obj, "timestamp");
    w.timestamp(exemplar.timestamp);
    push_metric_value(w, &mut obj, "value", exemplar.value);
    if let Some(trace) = &exemplar.trace {
        walk_trace_ref(w, &mut obj, trace);
    }
    if !exemplar.filtered_attributes.is_empty() {
        w.key(&mut obj, "attributes");
        walk_attrs(w, &exemplar.filtered_attributes);
    }
    w.close_map(obj);
}

/// `start` is the event's timestamp: a `SpanRecord` carries no start of its own.
fn walk_span<W: TreeWriter>(w: &mut W, start: i64, span: &SpanRecord) {
    let mut obj = w.open_map();
    w.key(&mut obj, "name");
    walk_value(w, &span.name);
    w.key(&mut obj, "trace_id");
    w.hex(&span.trace_id);
    w.key(&mut obj, "span_id");
    w.hex(&span.span_id);
    if let Some(parent) = &span.parent_span_id {
        w.key(&mut obj, "parent_span_id");
        w.hex(parent);
    }
    w.key(&mut obj, "kind");
    w.str(span.kind.as_str());
    w.key(&mut obj, "status");
    w.str(span.status.as_str());
    let ext = span.ext.as_deref();
    if let Some(message) = ext.and_then(|e| e.status_message.as_ref()) {
        w.key(&mut obj, "status_message");
        w.text_bytes(message);
    }
    if let Some(state) = ext.and_then(|e| e.trace_state.as_ref()) {
        w.key(&mut obj, "trace_state");
        w.text_bytes(state);
    }
    push_nonzero(w, &mut obj, "flags", span.flags);
    w.key(&mut obj, "start");
    w.timestamp(start);
    w.key(&mut obj, "end");
    w.timestamp(span.end_timestamp);
    // Saturating: an end before the start must render, not panic or wrap.
    w.key(&mut obj, "duration_ns");
    w.i64(span.end_timestamp.saturating_sub(start));
    if let Some(ext) = ext {
        push_nonzero(w, &mut obj, "dropped_attributes_count", ext.dropped_attributes_count);
        push_nonzero(w, &mut obj, "dropped_events_count", ext.dropped_events_count);
        push_nonzero(w, &mut obj, "dropped_links_count", ext.dropped_links_count);
    }
    if !span.events.is_empty() {
        w.key(&mut obj, "events");
        walk_array(w, span.events.iter(), walk_span_event);
    }
    if !span.links.is_empty() {
        w.key(&mut obj, "links");
        walk_array(w, span.links.iter(), walk_span_link);
    }
    w.close_map(obj);
}

fn walk_span_event<W: TreeWriter>(w: &mut W, span_event: &SpanEvent) {
    let mut obj = w.open_map();
    w.key(&mut obj, "name");
    walk_value(w, &span_event.name);
    w.key(&mut obj, "timestamp");
    w.timestamp(span_event.timestamp);
    if !span_event.attributes.is_empty() {
        w.key(&mut obj, "attributes");
        walk_attrs(w, &span_event.attributes);
    }
    push_nonzero(w, &mut obj, "dropped_attributes_count", span_event.dropped_attributes_count);
    w.close_map(obj);
}

fn walk_span_link<W: TreeWriter>(w: &mut W, link: &SpanLink) {
    let mut obj = w.open_map();
    w.key(&mut obj, "trace_id");
    w.hex(&link.trace_id);
    w.key(&mut obj, "span_id");
    w.hex(&link.span_id);
    push_nonzero(w, &mut obj, "flags", link.flags);
    if let Some(state) = &link.trace_state {
        w.key(&mut obj, "trace_state");
        w.text_bytes(state);
    }
    if !link.attributes.is_empty() {
        w.key(&mut obj, "attributes");
        walk_attrs(w, &link.attributes);
    }
    push_nonzero(w, &mut obj, "dropped_attributes_count", link.dropped_attributes_count);
    w.close_map(obj);
}

fn push_nonzero<W: TreeWriter>(w: &mut W, obj: &mut W::Map, key: &'static str, n: u32) {
    if n != 0 {
        push_u64(w, obj, key, u64::from(n));
    }
}

/// An attribute map in `AttrMap`'s own (sorted-by-`Symbol`) order.
fn walk_attrs<W: TreeWriter>(w: &mut W, attrs: &AttrMap) {
    let mut obj = w.open_map();
    for (key, value) in attrs.iter() {
        w.attr_key(&mut obj, resolve(key));
        walk_value(w, value);
    }
    w.close_map(obj);
}

fn walk_value<W: TreeWriter>(w: &mut W, value: &Value) {
    match value {
        Value::Null => w.null(),
        Value::Bool(b) => w.bool(*b),
        Value::I64(i) => w.i64(*i),
        Value::U64(u) => w.u64(*u),
        Value::F64(f) => w.f64(*f),
        // `Value::Str` is constructed only from valid UTF-8, so this cannot panic.
        Value::Str(s) => w.str(std::str::from_utf8(s).expect("Value::Str is always valid UTF-8")),
        Value::Bytes(b) => w.bytes(b),
        Value::Timestamp(ns) => w.timestamp(*ns),
        Value::Array(items) => walk_array(w, items.iter(), walk_value),
        Value::Map(map) => walk_attrs(w, map),
    }
}

/// The `types` table: one entry per value whose JSON spelling alone doesn't name its type,
/// written after every other key and only when there is one. See the module doc.
struct Types<'w, 'o, W: TreeWriter> {
    w: &'w mut W,
    obj: &'o mut W::Map,
    table: Option<W::Map>,
}

impl<W: TreeWriter> Types<'_, '_, W> {
    fn tag(&mut self, path: &Path<'_>, tag: &'static str) {
        if self.table.is_none() {
            self.w.key(self.obj, "types");
            self.table = Some(self.w.open_map());
        }
        let table = self.table.as_mut().expect("opened above");
        self.w.pointer_key(table, path);
        self.w.str(tag);
    }

    fn metric_value(&mut self, path: &Path<'_>, v: MetricValue) {
        if let MetricValue::U64(_) = v {
            self.tag(path, "u64");
        }
    }

    fn value(&mut self, path: &Path<'_>, value: &Value) {
        match value {
            Value::U64(_) => self.tag(path, "u64"),
            Value::F64(f) if !f.is_finite() => self.tag(path, "f64"),
            Value::Bytes(_) => self.tag(path, "bytes"),
            Value::Timestamp(_) => self.tag(path, "timestamp"),
            Value::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    self.value(&path.child(Seg::Index(i)), item);
                }
            }
            Value::Map(map) => self.attrs(path, map),
            Value::Null | Value::Bool(_) | Value::I64(_) | Value::F64(_) | Value::Str(_) => {}
        }
    }

    fn attrs(&mut self, path: &Path<'_>, attrs: &AttrMap) {
        for (key, value) in attrs.iter() {
            self.value(&path.child(Seg::Key(resolve(key))), value);
        }
    }
}

fn walk_types<W: TreeWriter>(
    w: &mut W,
    obj: &mut W::Map,
    resource: &Resource,
    scope: Option<&Scope>,
    event: &Event,
) {
    let mut t = Types { w, obj, table: None };
    if let Some(log) = &event.log {
        let log_path = Path { parent: None, seg: Seg::Key("log") };
        t.value(&log_path.child(Seg::Key("message")), &log.message);
    }
    let metrics = Path { parent: None, seg: Seg::Key("metrics") };
    for (i, metric) in event.metrics.iter().enumerate() {
        let m = metrics.child(Seg::Index(i));
        match &metric.kind {
            MetricKind::Sum(sum) => t.metric_value(&m.child(Seg::Key("value")), sum.value),
            MetricKind::Gauge(v) => t.metric_value(&m.child(Seg::Key("value")), *v),
            MetricKind::GaugeDelta(v) => t.metric_value(&m.child(Seg::Key("delta")), *v),
            _ => {}
        }
        let exemplars = m.child(Seg::Key("exemplars"));
        for (j, exemplar) in metric.exemplars.iter().enumerate() {
            let e = exemplars.child(Seg::Index(j));
            t.metric_value(&e.child(Seg::Key("value")), exemplar.value);
            t.attrs(&e.child(Seg::Key("attributes")), &exemplar.filtered_attributes);
        }
    }
    if let Some(span) = &event.span {
        let sp = Path { parent: None, seg: Seg::Key("span") };
        t.value(&sp.child(Seg::Key("name")), &span.name);
        let events = sp.child(Seg::Key("events"));
        for (j, span_event) in span.events.iter().enumerate() {
            let e = events.child(Seg::Index(j));
            t.value(&e.child(Seg::Key("name")), &span_event.name);
            t.attrs(&e.child(Seg::Key("attributes")), &span_event.attributes);
        }
        let links = sp.child(Seg::Key("links"));
        for (j, link) in span.links.iter().enumerate() {
            t.attrs(&links.child(Seg::Index(j)).child(Seg::Key("attributes")), &link.attributes);
        }
    }
    t.attrs(&Path { parent: None, seg: Seg::Key("attributes") }, &event.attributes);
    let res = Path { parent: None, seg: Seg::Key("resource") };
    t.attrs(&res.child(Seg::Key("attributes")), &resource.attributes);
    if let Some(scope) = scope {
        let sc = Path { parent: None, seg: Seg::Key("scope") };
        t.attrs(&sc.child(Seg::Key("attributes")), &scope.attributes);
    }
    if let Some(table) = t.table {
        t.w.close_map(table);
    }
}

/// `bytes`' valid UTF-8 runs JSON-escaped, each invalid byte and each backslash as the text
/// `\xHH`, with no quotes. In JSON source that text is `\\xHH`, so a reader decodes the string
/// to `\xHH`. Escaping the backslash too is what makes the text decode back to the same bytes.
fn push_lossless_bytes_body(out: &mut String, bytes: &[u8]) {
    for chunk in bytes.utf8_chunks() {
        for (i, part) in chunk.valid().split('\\').enumerate() {
            if i > 0 {
                out.push_str("\\\\x5c");
            }
            push_json_escaped(out, part);
        }
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
    use bytes::Bytes;
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

    /// A log message's rendering, without the `types` entry a tagged one carries.
    fn value(v: Value) -> String {
        let out = render_events(vec![Event::log(0, AttrMap::new(), log(v))]);
        let prefix = format!("{{\"timestamp\":\"{T0}\",\"log\":{{\"format\":\"raw\",\"message\":");
        let body = out.strip_prefix(&prefix).expect("log line prefix");
        let body = match body.find("},\"types\":{\"/log/message\":") {
            Some(at) => &body[..at + 1],
            None => body.strip_suffix("}\n").expect("log line suffix"),
        };
        body.strip_suffix('}').expect("log object close").to_string()
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
                value: logit_core::MetricValue::F64(1.0),
                temporality: Temporality::Cumulative,
                monotonic: false
            })),
            "{\"name\":\"m\",\"kind\":\"sum\",\"value\":1.0,\"temporality\":\"cumulative\",\
             \"monotonic\":false}"
        );
    }

    #[test]
    fn a_gauge() {
        assert_eq!(
            metric(MetricKind::gauge(0.25)),
            "{\"name\":\"m\",\"kind\":\"gauge\",\"value\":0.25}"
        );
    }

    #[test]
    fn a_typed_metric_value_is_a_json_integer_or_boolean() {
        assert_eq!(
            metric(MetricKind::Gauge(MetricValue::I64(-5))),
            "{\"name\":\"m\",\"kind\":\"gauge\",\"value\":-5}"
        );
        // 2^53 + 1 has no `f64`; every digit survives.
        assert_eq!(
            render_events(vec![Event::metric(
                0,
                AttrMap::new(),
                MetricRecord::new(intern("m"), MetricKind::Gauge(MetricValue::U64((1 << 53) + 1)))
            )]),
            format!(
                "{{\"timestamp\":\"{T0}\",\"metrics\":[{{\"name\":\"m\",\"kind\":\"gauge\",\
                 \"value\":9007199254740993}}],\"types\":{{\"/metrics/0/value\":\"u64\"}}}}\n"
            )
        );
        assert_eq!(
            metric(MetricKind::Gauge(MetricValue::Bool(true))),
            "{\"name\":\"m\",\"kind\":\"gauge\",\"value\":true}"
        );
        assert_eq!(
            metric(MetricKind::GaugeDelta(MetricValue::I64(-2))),
            "{\"name\":\"m\",\"kind\":\"gauge_delta\",\"delta\":-2}"
        );
    }

    #[test]
    fn a_gauge_delta_keeps_its_sign_including_negative_zero() {
        assert_eq!(
            metric(MetricKind::GaugeDelta(logit_core::MetricValue::F64(-3.5))),
            "{\"name\":\"m\",\"kind\":\"gauge_delta\",\"delta\":-3.5}"
        );
        assert_eq!(
            metric(MetricKind::GaugeDelta(logit_core::MetricValue::F64(-0.0))),
            "{\"name\":\"m\",\"kind\":\"gauge_delta\",\"delta\":-0.0}"
        );
    }

    #[test]
    fn samples() {
        let mut samples = Samples::new([1.5, 2.0]);
        samples.sample_rate = 0.5;
        assert_eq!(
            metric(MetricKind::Samples(samples)),
            "{\"name\":\"m\",\"kind\":\"samples\",\"values\":[1.5,2.0],\"sample_rate\":0.5}"
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
            "{\"name\":\"m\",\"kind\":\"distribution\",\"count\":4,\"sum\":20.0,\"min\":5.0,\
             \"max\":5.0,\"avg\":5.0,\"p50\":5.0,\"p90\":5.0,\"p95\":5.0,\"p99\":5.0,\
             \"sketch\":{\"mapping\":\"agent\",\"stats_exact\":true,\"count\":4.0,\"min\":5.0,\
             \"max\":5.0,\"positive\":[{\"key\":1442,\"count\":4.0}]}}"
        );
    }

    #[test]
    fn an_empty_distribution_writes_count_and_sum_only() {
        assert_eq!(
            metric(MetricKind::Distribution(DdSketch::new())),
            "{\"name\":\"m\",\"kind\":\"distribution\",\"count\":0,\"sum\":0.0,\
             \"sketch\":{\"mapping\":\"agent\",\"stats_exact\":true,\"count\":0.0,\
             \"min\":\"inf\",\"max\":\"-inf\"}}"
        );
    }

    #[test]
    fn a_set() {
        let mut hll = HyperLogLog::new();
        hll.insert(b"a");
        hll.insert(b"b");
        assert_eq!(
            metric(MetricKind::Set(hll.clone())),
            format!(
                "{{\"name\":\"m\",\"kind\":\"set\",\"estimate\":2,\"hll\":\"{}\"}}",
                base64::engine::general_purpose::STANDARD.encode(hll.to_bytes())
            )
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
             \"sum\":2.5,\"min\":0.1,\"max\":9.0}"
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
             \"temporality\":\"cumulative\",\"count\":9,\"zero_count\":1,\"zero_threshold\":0.0,\
             \"positive\":{\"offset\":-2,\"counts\":[1,2]},\"negative\":{\"offset\":4,\"counts\":[5]},\
             \"sum\":1.5,\"max\":8.0}"
        );
    }

    #[test]
    fn an_exponential_histogram_omits_an_empty_side_at_offset_zero() {
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
             \"temporality\":\"delta\",\"count\":0,\"zero_count\":0,\"zero_threshold\":0.0,\
             \"positive\":{\"offset\":1,\"counts\":[]}}"
        );
    }

    #[test]
    fn a_summary() {
        let s = Summary { quantiles: vec![(0.5, 1.0), (0.99, 4.25)], count: 10, sum: 12.0 };
        assert_eq!(
            metric(MetricKind::Summary(s)),
            "{\"name\":\"m\",\"kind\":\"summary\",\"count\":10,\"sum\":12.0,\
             \"quantiles\":[{\"quantile\":0.5,\"value\":1.0},{\"quantile\":0.99,\"value\":4.25}]}"
        );
    }

    #[test]
    fn metric_unit_description_start_and_flags() {
        let mut record = MetricRecord::new(intern("m"), MetricKind::gauge(1.0));
        record.unit = Some(intern("ms"));
        record.description = Some(intern("Latency"));
        record.start_timestamp = 1;
        record.flags = 2;
        let out = render_events(vec![Event::metric(0, AttrMap::new(), record)]);
        assert!(
            out.contains(
                "\"value\":1.0,\"unit\":\"ms\",\"description\":\"Latency\",\
                 \"start_timestamp\":\"1970-01-01T00:00:00.000000001Z\",\"flags\":2}"
            ),
            "got: {out}"
        );
    }

    #[test]
    fn no_recorded_value_keeps_the_kinds_fields_and_writes_raw_flags() {
        let mut record = MetricRecord::new(
            intern("m"),
            MetricKind::Sum(Sum {
                value: logit_core::MetricValue::F64(0.0),
                temporality: Temporality::Delta,
                monotonic: true,
            }),
        );
        record.flags = 3;
        record.unit = Some(intern("1"));
        let out = render_events(vec![Event::metric(0, AttrMap::new(), record)]);
        assert_eq!(
            out,
            format!(
                "{{\"timestamp\":\"{T0}\",\"metrics\":[{{\"name\":\"m\",\"kind\":\"sum\",\
                 \"value\":0.0,\"temporality\":\"delta\",\"monotonic\":true,\
                 \"unit\":\"1\",\"flags\":3,\"no_recorded_value\":true}}]}}\n"
            )
        );
    }

    #[test]
    fn exemplars() {
        let mut record = MetricRecord::new(intern("m"), MetricKind::gauge(1.0));
        record.exemplars = vec![
            Exemplar {
                timestamp: 5,
                value: MetricValue::F64(0.25),
                trace: Some(TraceRef { trace_id: [0x11; 16], span_id: Some([0x22; 8]), flags: 0 }),
                filtered_attributes: attrs(&[("exemplar.key", Value::str("v"))]),
            },
            Exemplar {
                timestamp: 0,
                value: MetricValue::F64(f64::NAN),
                trace: None,
                filtered_attributes: AttrMap::new(),
            },
            Exemplar {
                timestamp: 0,
                value: MetricValue::U64(u64::MAX),
                trace: None,
                filtered_attributes: AttrMap::new(),
            },
        ];
        let out = render_events(vec![Event::metric(0, AttrMap::new(), record)]);
        assert!(
            out.contains(&format!(
                "\"value\":1.0,\"exemplars\":[{{\"timestamp\":\"1970-01-01T00:00:00.000000005Z\",\
                 \"value\":0.25,\"trace_id\":\"{}\",\"span_id\":\"{}\",\"trace_flags\":0,\
                 \"attributes\":{{\"exemplar.key\":\"v\"}}}},\
                 {{\"timestamp\":\"{T0}\",\"value\":\"NaN\"}},\
                 {{\"timestamp\":\"{T0}\",\"value\":18446744073709551615}}]}}",
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
                 \"nested_test.at\":\"1970-01-01T00:00:00.000000003Z\"}},\
                 \"types\":{{\"/attributes/nested_test.tags/1\":\"u64\",\
                 \"/attributes/nested_test.at\":\"timestamp\"}}}}\n"
            )
        );
    }

    #[test]
    fn scalar_values() {
        assert_eq!(value(Value::Null), "null");
        assert_eq!(value(Value::Bool(false)), "false");
        assert_eq!(value(Value::I64(-7)), "-7");
        assert_eq!(value(Value::F64(1e21)), "1e21");
        assert_eq!(value(Value::F64(3.0)), "3.0");
        assert_eq!(value(Value::F64(-0.0)), "-0.0");
        assert_eq!(value(Value::F64(f64::MAX)), "1.7976931348623157e308");
    }

    #[test]
    fn bytes_valid_and_invalid() {
        assert_eq!(value(Value::Bytes(Bytes::from_static(b"abc"))), "\"b\\\"abc\\\"\"");
        assert_eq!(
            value(Value::Bytes(Bytes::from_static(b"a\xff\x00\"z"))),
            "\"b\\\"a\\\\xff\\u0000\\\"z\\\"\""
        );
        assert_eq!(value(Value::Bytes(Bytes::new())), "\"b\\\"\\\"\"");
        // A backslash is escaped too, so a literal `\x41` in the data reads back as itself.
        assert_eq!(value(Value::Bytes(Bytes::from_static(b"\\x41"))), "\"b\\\"\\\\x5cx41\\\"\"");
    }

    #[test]
    fn the_types_table_names_each_value_its_spelling_does_not() {
        let event = Event::empty(
            0,
            attrs(&[
                ("types_test.i", Value::I64(5)),
                ("types_test.u", Value::U64(5)),
                ("types_test.f", Value::F64(5.0)),
                ("types_test.a/b~\"c", Value::F64(f64::NAN)),
                ("types_test.b", Value::Bytes(Bytes::from_static(b"x"))),
                ("types_test.s", Value::str("b\"x\"")),
            ]),
        );
        assert_eq!(
            render_events(vec![event]),
            format!(
                "{{\"timestamp\":\"{T0}\",\"attributes\":{{\"types_test.i\":5,\"types_test.u\":5,\
                 \"types_test.f\":5.0,\"types_test.a/b~\\\"c\":\"NaN\",\"types_test.b\":\"b\\\"x\\\"\",\
                 \"types_test.s\":\"b\\\"x\\\"\"}},\"types\":{{\"/attributes/types_test.u\":\"u64\",\
                 \"/attributes/types_test.a~1b~0\\\"c\":\"f64\",\"/attributes/types_test.b\":\"bytes\"}}}}\n"
            )
        );
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
            metric(MetricKind::gauge(f64::NEG_INFINITY)),
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
                 \"metrics\":[{{\"name\":\"mixed_test.requests\",\"kind\":\"sum\",\"value\":1.0,\
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
