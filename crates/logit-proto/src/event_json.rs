//! The reader for `stdio_out`/`file_out` `format: json`: one JSON object per line back into the
//! [`Event`], [`Resource`], and [`Scope`] that wrote it. `crates/logit-outputs/src/ndjson.rs`'s
//! module doc is the grammar; this doc is the reader's half of it.
//!
//! What a line's spelling means:
//!
//! - A value the `types` table names is that type: `u64` an unsigned integer, `f64` a number or
//!   one of the strings `"NaN"`, `"inf"`, `"-inf"`, `bytes` a `b"..."` string, `timestamp` an
//!   RFC 3339 string. An entry that names no value, or a tag this reader doesn't know, is
//!   malformed.
//! - Otherwise, in a value position: an integer literal is `I64` (`U64` only when it doesn't fit
//!   an `i64`), a number with a fraction or an exponent is `F64`, a string is `Str`, and `null`,
//!   a boolean, an array, and an object are `Null`, `Bool`, `Array`, and `Map`.
//! - A metric value (`value`, `delta`, an exemplar's `value`) reads the same way, and a string
//!   there is a non-finite `F64`. A field with one type in the model (a count, a bound, a
//!   sample) reads as that type, so `"NaN"`, `"inf"`, `"-inf"` are the non-finite forms of an
//!   `f64` field and an integer literal is accepted where an `f64` is due.
//! - In a `b"..."` string and a byte field that is text by convention, `\xHH` is the byte `HH`;
//!   any other text is its UTF-8.
//!
//! Keys the reader doesn't use, the derived ones included (a distribution's `p50`, a span's
//! `duration_ns`), are ignored: a distribution is rebuilt from its `sketch`, a set from its `hll`.
//! A span's `start` is the event's `timestamp`. A `SpanExt` reads back only when one of its fields
//! is written, so one with every field empty reads as none, the form every decoder builds.
//!
//! **Batches.** A line repeats its batch's resource and scope, so consecutive lines with equal
//! ones belong to one batch, and they share one `Arc`. [`EventJsonDecoder::decode_batches`]
//! splits input into those runs. [`Decoder::decode_into`] takes one run and refuses input whose
//! resource or scope changes partway, since its signature returns one pair; a caller with mixed
//! input feeds it a run at a time or calls `decode_batches`. Blank lines are skipped.

use crate::event_tree::{Node, Obj};
use crate::{CodecError, Decoder};
use base64::Engine;
use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::time::parse_rfc3339_to_nanos;
use logit_core::{
    AttrMap, Bin, BodyFormat, DdSketch, Event, EventBatch, Exemplar, ExpHistogram, Histogram,
    HyperLogLog, LogRecord, Mapping, MetricKind, MetricList, MetricRecord, MetricValue, Resource,
    Samples, Scope, Severity, SketchStats, SpanEvent, SpanExt, SpanKind, SpanLink, SpanRecord,
    SpanStatus, Sum, Summary, Temporality, TraceRef, Value,
};
use std::collections::HashMap;
use std::sync::Arc;

/// One decoded line: its event, and the batch resource and scope it carried.
#[derive(Debug, Clone, PartialEq)]
pub struct Line {
    pub resource: Arc<Resource>,
    pub scope: Option<Arc<Scope>>,
    pub event: Event,
}

/// Reads `format: json` lines. Holds the last line's resource and scope, so lines that repeat
/// them share one `Arc`.
#[derive(Debug, Default)]
pub struct EventJsonDecoder {
    last: Option<(Arc<Resource>, Option<Arc<Scope>>)>,
}

impl EventJsonDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Decodes one line, without its newline.
    pub fn decode_line(&mut self, line: &[u8]) -> Result<Line, CodecError> {
        let json: Node = serde_json::from_slice(line)
            .map_err(|e| CodecError::Malformed(format!("not a JSON line: {e}")))?;
        let Node::Map(obj) = json else {
            return Err(malformed("a line is not a JSON object"));
        };
        self.decode_object(&obj)
    }

    /// Decodes one event object, however its front end parsed it.
    pub(crate) fn decode_object(&mut self, obj: &Obj) -> Result<Line, CodecError> {
        let mut types = Types::new(obj)?;
        let event = read_event(obj, &mut types)?;
        let resource = match obj.get("resource") {
            Some(r) => read_resource(as_object(r, "resource")?, &mut types)?,
            None => Resource::default(),
        };
        let scope =
            obj.get("scope").map(|s| read_scope(as_object(s, "scope")?, &mut types)).transpose()?;
        types.finish()?;
        let (resource, scope) = self.share(resource, scope);
        Ok(Line { resource, scope, event })
    }

    /// Decodes every line of `bytes` into one batch per run of lines with equal resource and
    /// scope, in input order.
    pub fn decode_batches(&mut self, bytes: &[u8]) -> Result<Vec<EventBatch>, CodecError> {
        let mut batches: Vec<EventBatch> = Vec::new();
        for line in lines(bytes) {
            push_line(&mut batches, self.decode_line(line)?);
        }
        Ok(batches)
    }

    /// The previous line's `Arc`s when this line's resource and scope equal them.
    fn share(
        &mut self,
        resource: Resource,
        scope: Option<Scope>,
    ) -> (Arc<Resource>, Option<Arc<Scope>>) {
        if let Some((last_resource, last_scope)) = &self.last {
            let scope_matches = match (last_scope, &scope) {
                (None, None) => true,
                (Some(a), Some(b)) => **a == *b,
                _ => false,
            };
            if **last_resource == resource && scope_matches {
                return (last_resource.clone(), last_scope.clone());
            }
        }
        let pair = (Arc::new(resource), scope.map(Arc::new));
        self.last = Some(pair.clone());
        pair
    }
}

impl Decoder for EventJsonDecoder {
    /// Decodes one run of lines; `received_at` is unused, since every line carries its own
    /// timestamps. Fails when a line's resource or scope differs from the first line's.
    fn decode_into(
        &mut self,
        bytes: Bytes,
        _received_at: i64,
        out: &mut Vec<Event>,
    ) -> Result<(Arc<Resource>, Option<Arc<Scope>>), CodecError> {
        let mut first: Option<(Arc<Resource>, Option<Arc<Scope>>)> = None;
        let start = out.len();
        for line in lines(&bytes) {
            let decoded = match self.decode_line(line) {
                Ok(decoded) => decoded,
                Err(e) => {
                    out.truncate(start);
                    return Err(e);
                }
            };
            match &first {
                None => first = Some((decoded.resource, decoded.scope)),
                Some((r, s))
                    if Arc::ptr_eq(r, &decoded.resource) && same_scope(s, &decoded.scope) => {}
                Some(_) => {
                    out.truncate(start);
                    return Err(malformed(
                        "the resource or scope changes partway; decode one run at a time",
                    ));
                }
            }
            out.push(decoded.event);
        }
        Ok(first.unwrap_or_else(|| (Arc::new(Resource::default()), None)))
    }
}

/// Appends `line` to the last batch when it shares that batch's resource and scope `Arc`s, else
/// starts a batch.
pub(crate) fn push_line(batches: &mut Vec<EventBatch>, line: Line) {
    let Line { resource, scope, event } = line;
    match batches.last_mut() {
        Some(b) if Arc::ptr_eq(&b.resource, &resource) && same_scope(&b.scope, &scope) => {
            b.events.push(event)
        }
        _ => batches.push(EventBatch { resource, scope, events: vec![event] }),
    }
}

fn same_scope(a: &Option<Arc<Scope>>, b: &Option<Arc<Scope>>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(a), Some(b)) => Arc::ptr_eq(a, b),
        _ => false,
    }
}

/// The non-blank lines of `bytes`, each without its `\n` or `\r\n`.
fn lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    bytes
        .split(|&b| b == b'\n')
        .map(|l| l.strip_suffix(b"\r").unwrap_or(l))
        .filter(|l| !l.iter().all(u8::is_ascii_whitespace))
}

fn malformed(msg: impl Into<String>) -> CodecError {
    CodecError::Malformed(msg.into())
}

/// The `types` table, and which entries a value has claimed.
struct Types {
    tags: HashMap<String, (&'static str, bool)>,
    /// The pointer of the value being read, built and unwound as the reader descends.
    path: String,
}

impl Types {
    fn new(obj: &Obj) -> Result<Self, CodecError> {
        let mut tags = HashMap::new();
        if let Some(table) = obj.get("types") {
            for (pointer, tag) in as_object(table, "types")? {
                let tag = match tag.as_str() {
                    Some("u64") => "u64",
                    Some("f64") => "f64",
                    Some("bytes") => "bytes",
                    Some("timestamp") => "timestamp",
                    _ => return Err(malformed(format!("types: unknown tag for {pointer}"))),
                };
                tags.insert(pointer.clone(), (tag, false));
            }
        }
        Ok(Types { tags, path: String::new() })
    }

    /// Runs `f` with `seg` appended to the current pointer.
    fn at<T>(&mut self, seg: &str, f: impl FnOnce(&mut Self) -> T) -> T {
        let len = self.path.len();
        self.path.push('/');
        for c in seg.chars() {
            match c {
                '~' => self.path.push_str("~0"),
                '/' => self.path.push_str("~1"),
                c => self.path.push(c),
            }
        }
        let out = f(self);
        self.path.truncate(len);
        out
    }

    fn at_index<T>(&mut self, i: usize, f: impl FnOnce(&mut Self) -> T) -> T {
        self.at(&i.to_string(), f)
    }

    /// The current value's tag, marking its entry used.
    fn tag(&mut self) -> Option<&'static str> {
        let entry = self.tags.get_mut(&self.path)?;
        entry.1 = true;
        Some(entry.0)
    }

    fn finish(self) -> Result<(), CodecError> {
        match self.tags.iter().find(|(_, (_, used))| !used) {
            Some((pointer, _)) => Err(malformed(format!("types: {pointer} names no value"))),
            None => Ok(()),
        }
    }
}

fn as_object<'a>(json: &'a Node, what: &str) -> Result<&'a Obj, CodecError> {
    json.as_object().ok_or_else(|| malformed(format!("{what} is not an object")))
}

fn as_array<'a>(json: &'a Node, what: &str) -> Result<&'a Vec<Node>, CodecError> {
    json.as_array().ok_or_else(|| malformed(format!("{what} is not an array")))
}

fn as_str<'a>(json: &'a Node, what: &str) -> Result<&'a str, CodecError> {
    json.as_str().ok_or_else(|| malformed(format!("{what} is not a string")))
}

fn field<'a>(obj: &'a Obj, key: &str) -> Result<&'a Node, CodecError> {
    obj.get(key).ok_or_else(|| malformed(format!("missing {key}")))
}

fn opt_u64(obj: &Obj, key: &str) -> Result<u64, CodecError> {
    obj.get(key).map_or(Ok(0), |v| {
        v.as_u64().ok_or_else(|| malformed(format!("{key} is not an unsigned integer")))
    })
}

fn opt_u32(obj: &Obj, key: &str) -> Result<u32, CodecError> {
    u32::try_from(opt_u64(obj, key)?).map_err(|_| malformed(format!("{key} is out of range")))
}

fn req_u64(obj: &Obj, key: &str) -> Result<u64, CodecError> {
    field(obj, key)?.as_u64().ok_or_else(|| malformed(format!("{key} is not an unsigned integer")))
}

fn req_i32(obj: &Obj, key: &str) -> Result<i32, CodecError> {
    field(obj, key)?
        .as_i64()
        .and_then(|n| i32::try_from(n).ok())
        .ok_or_else(|| malformed(format!("{key} is not a 32-bit integer")))
}

fn req_bool(obj: &Obj, key: &str) -> Result<bool, CodecError> {
    field(obj, key)?.as_bool().ok_or_else(|| malformed(format!("{key} is not a boolean")))
}

/// An `f64` field: any number, or a non-finite string.
fn f64_of(json: &Node, what: &str) -> Result<f64, CodecError> {
    if let Some(f) = json.as_f64() {
        return Ok(f);
    }
    match json {
        Node::Str(s) => non_finite(s).ok_or_else(|| malformed(format!("{what} is not a number"))),
        _ => Err(malformed(format!("{what} is not a number"))),
    }
}

fn non_finite(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "inf" => Some(f64::INFINITY),
        "-inf" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

fn req_f64(obj: &Obj, key: &str) -> Result<f64, CodecError> {
    f64_of(field(obj, key)?, key)
}

fn opt_f64(obj: &Obj, key: &str) -> Result<Option<f64>, CodecError> {
    obj.get(key).map(|v| f64_of(v, key)).transpose()
}

fn timestamp_of(json: &Node, what: &str) -> Result<i64, CodecError> {
    parse_rfc3339_to_nanos(as_str(json, what)?)
        .map_err(|_| malformed(format!("{what} is not an RFC 3339 timestamp")))
}

fn opt_timestamp(obj: &Obj, key: &str) -> Result<i64, CodecError> {
    obj.get(key).map_or(Ok(0), |v| timestamp_of(v, key))
}

fn hex_of<const N: usize>(json: &Node, what: &str) -> Result<[u8; N], CodecError> {
    let s = as_str(json, what)?.as_bytes();
    let bad = || malformed(format!("{what} is not {N} bytes of hex"));
    if s.len() != N * 2 {
        return Err(bad());
    }
    let mut out = [0u8; N];
    for (i, pair) in s.chunks(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16).ok_or_else(bad)?;
        let lo = (pair[1] as char).to_digit(16).ok_or_else(bad)?;
        out[i] = (hi * 16 + lo) as u8;
    }
    Ok(out)
}

/// The bytes a text-by-convention string stands for: `\xHH` is the byte `HH`, any other text its
/// UTF-8.
fn text_bytes(s: &str) -> Bytes {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'\\' && b.get(i + 1) == Some(&b'x') {
            let hex = b.get(i + 2..i + 4).and_then(|h| std::str::from_utf8(h).ok());
            if let Some(byte) = hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                out.push(byte);
                i += 4;
                continue;
            }
        }
        out.push(b[i]);
        i += 1;
    }
    Bytes::from(out)
}

fn opt_text_bytes(obj: &Obj, key: &str) -> Result<Option<Bytes>, CodecError> {
    obj.get(key).map(|v| as_str(v, key).map(text_bytes)).transpose()
}

fn temporality(obj: &Obj) -> Result<Temporality, CodecError> {
    Temporality::from_name(as_str(field(obj, "temporality")?, "temporality")?)
        .ok_or_else(|| malformed("unknown temporality"))
}

fn read_value(json: &Node, types: &mut Types) -> Result<Value, CodecError> {
    let tag = types.tag();
    let bad = |what: &str| malformed(format!("{} is not a {what}", types_path(types)));
    match tag {
        Some("u64") => json.as_u64().map(Value::U64).ok_or_else(|| bad("u64")),
        Some("f64") => f64_of(json, "an f64 value").map(Value::F64),
        Some("bytes") if matches!(json, Node::Bin(_)) => read_untagged(json, types),
        Some("bytes") => as_str(json, "a bytes value")?
            .strip_prefix("b\"")
            .and_then(|s| s.strip_suffix('"'))
            .map(|body| Value::Bytes(text_bytes(body)))
            .ok_or_else(|| bad("b\"...\" string")),
        Some("timestamp") => timestamp_of(json, "a timestamp value").map(Value::Timestamp),
        _ => read_untagged(json, types),
    }
}

/// A value by its own type. A MessagePack `bin` is `Bytes` whether or not the table tags it.
fn read_untagged(json: &Node, types: &mut Types) -> Result<Value, CodecError> {
    match json {
        Node::Null => Ok(Value::Null),
        Node::Bool(b) => Ok(Value::Bool(*b)),
        Node::I64(n) => Ok(Value::I64(*n)),
        Node::U64(n) => Ok(Value::U64(*n)),
        Node::F64(f) => Ok(Value::F64(*f)),
        Node::Str(s) => Ok(Value::str(s.as_str())),
        Node::Bin(b) => Ok(Value::Bytes(b.clone())),
        Node::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for (i, item) in items.iter().enumerate() {
                out.push(types.at_index(i, |t| read_value(item, t))?);
            }
            Ok(Value::Array(out))
        }
        Node::Map(map) => Ok(Value::Map(Box::new(read_attrs(map, types)?))),
    }
}

fn types_path(types: &Types) -> &str {
    &types.path
}

fn read_metric_value(
    json: &Node,
    types: &mut Types,
    what: &str,
) -> Result<MetricValue, CodecError> {
    let bad = || malformed(format!("{what} is not a metric value"));
    if types.tag() == Some("u64") {
        return json.as_u64().map(MetricValue::U64).ok_or_else(bad);
    }
    match json {
        Node::Bool(b) => Ok(MetricValue::Bool(*b)),
        Node::I64(i) => Ok(MetricValue::I64(*i)),
        Node::U64(u) => Ok(MetricValue::U64(*u)),
        Node::F64(f) => Ok(MetricValue::F64(*f)),
        Node::Str(s) => non_finite(s).map(MetricValue::F64).ok_or_else(bad),
        _ => Err(bad()),
    }
}

fn read_attrs(map: &Obj, types: &mut Types) -> Result<AttrMap, CodecError> {
    let mut attrs = AttrMap::new();
    for (key, json) in map {
        let value = types.at(key, |t| read_value(json, t))?;
        attrs.insert(key, value);
    }
    Ok(attrs)
}

/// `obj[key]` as attributes under the pointer segment `key`, empty when absent.
fn opt_attrs(obj: &Obj, key: &str, types: &mut Types) -> Result<AttrMap, CodecError> {
    match obj.get(key) {
        Some(json) => {
            let map = as_object(json, key)?;
            types.at(key, |t| read_attrs(map, t))
        }
        None => Ok(AttrMap::new()),
    }
}

fn read_trace_ref(obj: &Obj) -> Result<Option<TraceRef>, CodecError> {
    let Some(trace_id) = obj.get("trace_id") else {
        return Ok(None);
    };
    let span_id = obj.get("span_id").map(|s| hex_of::<8>(s, "span_id")).transpose()?;
    let flags = u8::try_from(opt_u64(obj, "trace_flags")?)
        .map_err(|_| malformed("trace_flags is out of range"))?;
    Ok(Some(TraceRef { trace_id: hex_of::<16>(trace_id, "trace_id")?, span_id, flags }))
}

fn read_event(obj: &Obj, types: &mut Types) -> Result<Event, CodecError> {
    let timestamp = timestamp_of(field(obj, "timestamp")?, "timestamp")?;
    let log = obj
        .get("log")
        .map(|l| types.at("log", |t| read_log(as_object(l, "log")?, t)))
        .transpose()?;
    let mut metrics = MetricList::new();
    if let Some(list) = obj.get("metrics") {
        types.at("metrics", |t| {
            for (i, m) in as_array(list, "metrics")?.iter().enumerate() {
                metrics.push(t.at_index(i, |t| read_metric(as_object(m, "a metric")?, t))?);
            }
            Ok::<_, CodecError>(())
        })?;
    }
    let span = obj
        .get("span")
        .map(|s| types.at("span", |t| read_span(as_object(s, "span")?, t)))
        .transpose()?;
    let attributes = opt_attrs(obj, "attributes", types)?;
    Ok(Event { timestamp, attributes, log, metrics, span })
}

fn read_log(obj: &Obj, types: &mut Types) -> Result<LogRecord, CodecError> {
    let severity = obj
        .get("severity")
        .map(|s| {
            Severity::from_name(as_str(s, "severity")?).ok_or_else(|| malformed("unknown severity"))
        })
        .transpose()?;
    let body_format = BodyFormat::from_name(as_str(field(obj, "format")?, "format")?)
        .ok_or_else(|| malformed("unknown log format"))?;
    let message = types.at("message", |t| read_value(field(obj, "message")?, t))?;
    let event_name =
        obj.get("event_name").map(|n| as_str(n, "event_name").map(intern)).transpose()?;
    Ok(LogRecord {
        message,
        severity,
        body_format,
        trace: read_trace_ref(obj)?,
        event_name,
        observed_timestamp: opt_timestamp(obj, "observed_timestamp")?,
        dropped_attributes_count: opt_u32(obj, "dropped_attributes_count")?,
    })
}

fn read_metric(obj: &Obj, types: &mut Types) -> Result<MetricRecord, CodecError> {
    let name = intern(as_str(field(obj, "name")?, "name")?);
    let kind = read_kind(obj, types)?;
    let mut record = MetricRecord::new(name, kind);
    record.unit = obj.get("unit").map(|u| as_str(u, "unit").map(intern)).transpose()?;
    record.description =
        obj.get("description").map(|d| as_str(d, "description").map(intern)).transpose()?;
    record.start_timestamp = opt_timestamp(obj, "start_timestamp")?;
    record.flags = opt_u32(obj, "flags")?;
    if let Some(list) = obj.get("exemplars") {
        record.exemplars = types.at("exemplars", |t| {
            as_array(list, "exemplars")?
                .iter()
                .enumerate()
                .map(|(i, e)| t.at_index(i, |t| read_exemplar(as_object(e, "an exemplar")?, t)))
                .collect::<Result<Vec<_>, _>>()
        })?;
    }
    Ok(record)
}

fn read_exemplar(obj: &Obj, types: &mut Types) -> Result<Exemplar, CodecError> {
    Ok(Exemplar {
        timestamp: timestamp_of(field(obj, "timestamp")?, "an exemplar's timestamp")?,
        value: types.at("value", |t| read_metric_value(field(obj, "value")?, t, "value"))?,
        trace: read_trace_ref(obj)?,
        filtered_attributes: opt_attrs(obj, "attributes", types)?,
    })
}

fn read_kind(obj: &Obj, types: &mut Types) -> Result<MetricKind, CodecError> {
    let kind = as_str(field(obj, "kind")?, "kind")?;
    let metric_value = |types: &mut Types, key: &str| {
        types.at(key, |t| read_metric_value(field(obj, key)?, t, key))
    };
    Ok(match kind {
        "sum" => MetricKind::Sum(Sum {
            value: metric_value(types, "value")?,
            temporality: temporality(obj)?,
            monotonic: req_bool(obj, "monotonic")?,
        }),
        "gauge" => MetricKind::Gauge(metric_value(types, "value")?),
        "gauge_delta" => MetricKind::GaugeDelta(metric_value(types, "delta")?),
        "samples" => {
            let values = as_array(field(obj, "values")?, "values")?
                .iter()
                .map(|v| f64_of(v, "a sample"))
                .collect::<Result<Vec<_>, _>>()?;
            let mut samples = Samples::new(values);
            samples.sample_rate = req_f64(obj, "sample_rate")?;
            MetricKind::Samples(samples)
        }
        "set_members" => MetricKind::SetMembers(
            as_array(field(obj, "members")?, "members")?
                .iter()
                .map(|m| as_str(m, "a member").map(text_bytes))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        "distribution" => {
            MetricKind::Distribution(read_sketch(as_object(field(obj, "sketch")?, "sketch")?, obj)?)
        }
        "set" => {
            let b64 = as_str(field(obj, "hll")?, "hll")?;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(b64)
                .map_err(|e| malformed(format!("hll is not base64: {e}")))?;
            MetricKind::Set(
                HyperLogLog::from_bytes(&bytes).map_err(|e| malformed(format!("hll: {e}")))?,
            )
        }
        "histogram" => MetricKind::Histogram(Histogram {
            buckets: as_array(field(obj, "buckets")?, "buckets")?
                .iter()
                .map(|b| {
                    let b = as_object(b, "a bucket")?;
                    Ok((req_f64(b, "bound")?, req_u64(b, "count")?))
                })
                .collect::<Result<Vec<_>, CodecError>>()?,
            temporality: temporality(obj)?,
            sum: opt_f64(obj, "sum")?,
            min: opt_f64(obj, "min")?,
            max: opt_f64(obj, "max")?,
        }),
        "exponential_histogram" => {
            let side = |key: &str| -> Result<(i32, Vec<u64>), CodecError> {
                let Some(side) = obj.get(key) else {
                    return Ok((0, Vec::new()));
                };
                let side = as_object(side, key)?;
                let counts = as_array(field(side, "counts")?, "counts")?
                    .iter()
                    .map(|c| c.as_u64().ok_or_else(|| malformed("a bucket count is not a u64")))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((req_i32(side, "offset")?, counts))
            };
            MetricKind::ExponentialHistogram(ExpHistogram {
                scale: req_i32(obj, "scale")?,
                zero_count: req_u64(obj, "zero_count")?,
                zero_threshold: req_f64(obj, "zero_threshold")?,
                positive: side("positive")?,
                negative: side("negative")?,
                temporality: temporality(obj)?,
                count: req_u64(obj, "count")?,
                sum: opt_f64(obj, "sum")?,
                min: opt_f64(obj, "min")?,
                max: opt_f64(obj, "max")?,
            })
        }
        "summary" => MetricKind::Summary(Summary {
            quantiles: as_array(field(obj, "quantiles")?, "quantiles")?
                .iter()
                .map(|q| {
                    let q = as_object(q, "a quantile")?;
                    Ok((req_f64(q, "quantile")?, req_f64(q, "value")?))
                })
                .collect::<Result<Vec<_>, CodecError>>()?,
            count: req_u64(obj, "count")?,
            sum: req_f64(obj, "sum")?,
            temporality: temporality(obj)?,
        }),
        other => return Err(malformed(format!("unknown metric kind {other}"))),
    })
}

/// A distribution's `sketch`, with the `sum` the distribution carries beside it.
fn read_sketch(sketch: &Obj, metric: &Obj) -> Result<DdSketch, CodecError> {
    let mapping = match as_str(field(sketch, "mapping")?, "mapping")? {
        "agent" => Mapping::agent(),
        "logarithmic" => {
            let bin_limit = u32::try_from(req_u64(sketch, "bin_limit")?)
                .map_err(|_| malformed("bin_limit is out of range"))?;
            Mapping::try_logarithmic(
                req_f64(sketch, "gamma")?,
                req_f64(sketch, "index_offset")?,
                bin_limit,
            )
            .ok_or_else(|| malformed("not a valid logarithmic mapping"))?
        }
        other => return Err(malformed(format!("unknown sketch mapping {other}"))),
    };
    let bins = |key: &str| -> Result<Vec<Bin>, CodecError> {
        let Some(list) = sketch.get(key) else {
            return Ok(Vec::new());
        };
        as_array(list, key)?
            .iter()
            .map(|b| {
                let b = as_object(b, "a bin")?;
                Ok(Bin { key: req_i32(b, "key")?, count: req_f64(b, "count")? })
            })
            .collect()
    };
    let stats = SketchStats {
        count: req_f64(sketch, "count")?,
        min: req_f64(sketch, "min")?,
        max: req_f64(sketch, "max")?,
        sum: req_f64(metric, "sum")?,
    };
    Ok(DdSketch::from_stored(
        mapping,
        bins("positive")?,
        bins("negative")?,
        opt_f64(sketch, "zero_count")?.unwrap_or(0.0),
        stats,
        req_bool(sketch, "stats_exact")?,
    ))
}

fn read_span(obj: &Obj, types: &mut Types) -> Result<SpanRecord, CodecError> {
    let name = types.at("name", |t| read_value(field(obj, "name")?, t))?;
    let kind = SpanKind::from_name(as_str(field(obj, "kind")?, "kind")?)
        .ok_or_else(|| malformed("unknown span kind"))?;
    let status = SpanStatus::from_name(as_str(field(obj, "status")?, "status")?)
        .ok_or_else(|| malformed("unknown span status"))?;
    let ext = SpanExt {
        status_message: opt_text_bytes(obj, "status_message")?,
        trace_state: opt_text_bytes(obj, "trace_state")?,
        dropped_attributes_count: opt_u32(obj, "dropped_attributes_count")?,
        dropped_events_count: opt_u32(obj, "dropped_events_count")?,
        dropped_links_count: opt_u32(obj, "dropped_links_count")?,
    };
    let has_ext = ext.status_message.is_some()
        || ext.trace_state.is_some()
        || ext.dropped_attributes_count != 0
        || ext.dropped_events_count != 0
        || ext.dropped_links_count != 0;
    let mut events = Vec::new();
    if let Some(list) = obj.get("events") {
        types.at("events", |t| {
            for (i, e) in as_array(list, "events")?.iter().enumerate() {
                let e = as_object(e, "a span event")?;
                events.push(t.at_index(i, |t| {
                    Ok::<_, CodecError>(SpanEvent {
                        timestamp: timestamp_of(
                            field(e, "timestamp")?,
                            "a span event's timestamp",
                        )?,
                        name: t.at("name", |t| read_value(field(e, "name")?, t))?,
                        attributes: opt_attrs(e, "attributes", t)?,
                        dropped_attributes_count: opt_u32(e, "dropped_attributes_count")?,
                    })
                })?);
            }
            Ok::<_, CodecError>(())
        })?;
    }
    let mut links = Vec::new();
    if let Some(list) = obj.get("links") {
        types.at("links", |t| {
            for (i, l) in as_array(list, "links")?.iter().enumerate() {
                let l = as_object(l, "a span link")?;
                links.push(t.at_index(i, |t| {
                    Ok::<_, CodecError>(SpanLink {
                        trace_id: hex_of::<16>(field(l, "trace_id")?, "a link's trace_id")?,
                        span_id: hex_of::<8>(field(l, "span_id")?, "a link's span_id")?,
                        attributes: opt_attrs(l, "attributes", t)?,
                        flags: opt_u32(l, "flags")?,
                        trace_state: opt_text_bytes(l, "trace_state")?,
                        dropped_attributes_count: opt_u32(l, "dropped_attributes_count")?,
                    })
                })?);
            }
            Ok::<_, CodecError>(())
        })?;
    }
    Ok(SpanRecord {
        trace_id: hex_of::<16>(field(obj, "trace_id")?, "trace_id")?,
        span_id: hex_of::<8>(field(obj, "span_id")?, "span_id")?,
        parent_span_id: obj
            .get("parent_span_id")
            .map(|p| hex_of::<8>(p, "parent_span_id"))
            .transpose()?,
        name,
        kind,
        status,
        events,
        links,
        end_timestamp: timestamp_of(field(obj, "end")?, "end")?,
        flags: opt_u32(obj, "flags")?,
        ext: has_ext.then(|| Box::new(ext)),
    })
}

fn read_resource(obj: &Obj, types: &mut Types) -> Result<Resource, CodecError> {
    types.at("resource", |t| {
        Ok(Resource {
            attributes: opt_attrs(obj, "attributes", t)?,
            dropped_attributes_count: opt_u32(obj, "dropped_attributes_count")?,
            schema_url: opt_text_bytes(obj, "schema_url")?,
        })
    })
}

fn read_scope(obj: &Obj, types: &mut Types) -> Result<Scope, CodecError> {
    types.at("scope", |t| {
        Ok(Scope {
            name: opt_text_bytes(obj, "name")?.unwrap_or_default(),
            version: opt_text_bytes(obj, "version")?.unwrap_or_default(),
            attributes: opt_attrs(obj, "attributes", t)?,
            dropped_attributes_count: opt_u32(obj, "dropped_attributes_count")?,
            schema_url: opt_text_bytes(obj, "schema_url")?,
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode_one(line: &str) -> Result<Line, CodecError> {
        EventJsonDecoder::new().decode_line(line.as_bytes())
    }

    fn attr(line: &str, key: &str) -> Value {
        decode_one(line).expect("decodes").event.attributes.get(key).cloned().expect("present")
    }

    const T0: &str = "1970-01-01T00:00:00.000000000Z";

    #[test]
    fn integers_are_i64_unless_tagged_and_fractions_are_f64() {
        let line = format!(
            "{{\"timestamp\":\"{T0}\",\"attributes\":{{\"ej.a\":5,\"ej.b\":5,\"ej.c\":5.0,\
             \"ej.d\":18446744073709551615}},\"types\":{{\"/attributes/ej.b\":\"u64\"}}}}"
        );
        assert_eq!(attr(&line, "ej.a"), Value::I64(5));
        assert_eq!(attr(&line, "ej.b"), Value::U64(5));
        assert_eq!(attr(&line, "ej.c"), Value::F64(5.0));
        assert_eq!(attr(&line, "ej.d"), Value::U64(u64::MAX));
    }

    #[test]
    fn tagged_strings_are_bytes_timestamps_and_non_finite_floats() {
        let line = format!(
            "{{\"timestamp\":\"{T0}\",\"attributes\":{{\"ej.bytes\":\"b\\\"a\\\\xff\\\\x5cx\\\"\",\
             \"ej.ts\":\"1970-01-01T00:00:01.000000000Z\",\"ej.nan\":\"NaN\",\"ej.s\":\"NaN\"}},\
             \"types\":{{\"/attributes/ej.bytes\":\"bytes\",\"/attributes/ej.ts\":\"timestamp\",\
             \"/attributes/ej.nan\":\"f64\"}}}}"
        );
        assert_eq!(attr(&line, "ej.bytes"), Value::Bytes(Bytes::from_static(b"a\xff\\x")));
        assert_eq!(attr(&line, "ej.ts"), Value::Timestamp(1_000_000_000));
        assert!(matches!(attr(&line, "ej.nan"), Value::F64(f) if f.is_nan()));
        assert_eq!(attr(&line, "ej.s"), Value::str("NaN"));
    }

    #[test]
    fn a_pointer_escapes_tilde_and_slash() {
        let line = format!(
            "{{\"timestamp\":\"{T0}\",\"attributes\":{{\"ej.a/b~c\":[1,2]}},\
             \"types\":{{\"/attributes/ej.a~1b~0c/1\":\"u64\"}}}}"
        );
        assert_eq!(attr(&line, "ej.a/b~c"), Value::Array(vec![Value::I64(1), Value::U64(2)]));
    }

    #[test]
    fn an_unused_or_unknown_type_entry_is_malformed() {
        let unused =
            format!("{{\"timestamp\":\"{T0}\",\"types\":{{\"/attributes/ej.missing\":\"u64\"}}}}");
        assert!(matches!(decode_one(&unused), Err(CodecError::Malformed(_))));
        let unknown = format!(
            "{{\"timestamp\":\"{T0}\",\"attributes\":{{\"ej.x\":1}},\
             \"types\":{{\"/attributes/ej.x\":\"i128\"}}}}"
        );
        assert!(matches!(decode_one(&unknown), Err(CodecError::Malformed(_))));
    }

    #[test]
    fn runs_of_equal_resource_and_scope_are_one_batch() {
        let a = format!(
            "{{\"timestamp\":\"{T0}\",\"resource\":{{\"attributes\":{{\"ej.h\":\"a\"}}}}}}"
        );
        let b = format!(
            "{{\"timestamp\":\"{T0}\",\"resource\":{{\"attributes\":{{\"ej.h\":\"b\"}}}}}}"
        );
        let input = format!("{a}\n{a}\n\n{b}\n{a}\n");
        let batches = EventJsonDecoder::new().decode_batches(input.as_bytes()).expect("decodes");
        assert_eq!(batches.iter().map(|b| b.events.len()).collect::<Vec<_>>(), vec![2, 1, 1]);

        let mut out = Vec::new();
        let mut decoder = EventJsonDecoder::new();
        let run = decoder.decode_into(Bytes::from(format!("{a}\n{a}\n")), 0, &mut out);
        assert!(run.is_ok());
        assert_eq!(out.len(), 2);
        let mixed = decoder.decode_into(Bytes::from(input), 0, &mut out);
        assert!(matches!(mixed, Err(CodecError::Malformed(_))));
        assert_eq!(out.len(), 2, "a refused run leaves nothing behind");
    }

    #[test]
    fn a_line_that_is_not_an_object_is_malformed() {
        assert!(matches!(decode_one("[1]"), Err(CodecError::Malformed(_))));
        assert!(matches!(decode_one("{"), Err(CodecError::Malformed(_))));
        assert!(matches!(decode_one("{}"), Err(CodecError::Malformed(_))));
    }
}
