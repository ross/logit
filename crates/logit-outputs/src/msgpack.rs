//! `kafka_out`'s `format: msgpack`: each event as one MessagePack map, the object
//! `crate::ndjson` writes as JSON, key for key and in the same order. That module's doc is the
//! grammar for which keys appear and when; one walker writes both, through
//! [`crate::ndjson::TreeWriter`]. This doc is the canonical account of the value forms, which
//! follow MessagePack's types where JSON has none:
//!
//! | Node | MessagePack |
//! |---|---|
//! | An object | a map, its header the smallest that fits (`logit_proto::msgpack::Writer::begin_map`) |
//! | An array | an array |
//! | `Null`, `Bool` | nil, bool |
//! | `I64`, `U64`, and every integer field (counts, `flags`, `trace_flags`, `scale`, `offset`, `duration_ns`, `estimate`) | the smallest int format that fits |
//! | `F64` and every float field | float64, a non-finite value included: NaN and the infinities are floats, not the strings JSON writes |
//! | `Str`, and every name the grammar writes as a string | str |
//! | `Bytes` | bin, the bytes as they are; JSON writes the text `b"..."` instead |
//! | A timestamp | str, RFC 3339 UTC with nine fractional digits, as in JSON |
//! | A trace or span id | str, lowercase hex, as in JSON |
//! | A byte field that is text by convention (`status_message`, `trace_state`, `schema_url`, scope `name` and `version`, a set member) | str, each invalid UTF-8 byte and each backslash as the four characters `\xHH`, the string a JSON reader decodes from the NDJSON line |
//! | The `types` table | a map of str to str, as in JSON: the smallest int format doesn't tell a `U64` from an `I64` either |
//!
//! So a consumer reads the same keys and, outside the `Bytes` and non-finite rows, the same
//! values it would from `format: json`. Under `record: batch` a record's value is several of
//! these maps back to back, which a MessagePack reader takes as a stream of values.

use std::fmt::Write;

use logit_core::time::write_rfc3339_utc;
use logit_core::trace::push_hex;
use logit_core::{Event, Resource, Scope};
use logit_proto::msgpack::{MapStart, Writer};

use crate::ndjson::{push_pointer_seg, Path, TreeWriter};

/// Appends one event's map to `out`. `scratch` holds a timestamp's or an id's text before it's
/// written as a str; it's reused across calls.
pub(crate) fn render_event(
    out: &mut Writer,
    scratch: &mut String,
    resource: &Resource,
    scope: Option<&Scope>,
    event: &Event,
) {
    crate::ndjson::walk_event(&mut Msgpack { out, scratch }, resource, scope, event);
}

/// The MessagePack writer for [`crate::ndjson`]'s walker.
struct Msgpack<'a> {
    out: &'a mut Writer,
    scratch: &'a mut String,
}

impl Msgpack<'_> {
    /// Writes `scratch`, filled by `fill`, as a str.
    fn str_from(&mut self, fill: impl FnOnce(&mut String)) {
        self.scratch.clear();
        fill(self.scratch);
        self.out.write_str(self.scratch);
    }
}

impl TreeWriter for Msgpack<'_> {
    /// The placeholder header's position and the entries written so far.
    type Map = (MapStart, usize);

    fn open_map(&mut self) -> Self::Map {
        (self.out.begin_map(), 0)
    }

    fn key(&mut self, map: &mut Self::Map, key: &'static str) {
        map.1 += 1;
        self.out.write_str(key);
    }

    fn attr_key(&mut self, map: &mut Self::Map, key: &str) {
        map.1 += 1;
        self.out.write_str(key);
    }

    fn close_map(&mut self, (start, len): Self::Map) {
        self.out.end_map(start, len);
    }

    fn open_array(&mut self, len: usize) {
        self.out.write_array_len(len);
    }

    fn item(&mut self, _index: usize) {}

    fn close_array(&mut self) {}

    fn null(&mut self) {
        self.out.write_nil();
    }

    fn bool(&mut self, v: bool) {
        self.out.write_bool(v);
    }

    fn i64(&mut self, v: i64) {
        self.out.write_i64(v);
    }

    fn u64(&mut self, v: u64) {
        self.out.write_u64(v);
    }

    fn f64(&mut self, v: f64) {
        self.out.write_f64(v);
    }

    fn str(&mut self, s: &str) {
        self.out.write_str(s);
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.out.write_bin(bytes);
    }

    fn text_bytes(&mut self, bytes: &[u8]) {
        match std::str::from_utf8(bytes) {
            Ok(text) if !text.contains('\\') => self.out.write_str(text),
            _ => self.str_from(|s| {
                for chunk in bytes.utf8_chunks() {
                    s.push_str(&chunk.valid().replace('\\', "\\x5c"));
                    for b in chunk.invalid() {
                        let _ = write!(s, "\\x{b:02x}");
                    }
                }
            }),
        }
    }

    fn timestamp(&mut self, nanos: i64) {
        self.str_from(|s| write_rfc3339_utc(s, nanos));
    }

    fn hex(&mut self, bytes: &[u8]) {
        self.str_from(|s| push_hex(s, bytes));
    }

    fn pointer_key(&mut self, map: &mut Self::Map, path: &Path<'_>) {
        map.1 += 1;
        self.str_from(|s| path.for_each(&mut |seg| push_pointer_seg(s, seg)));
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bytes::Bytes;
    use logit_core::interner::intern;
    use logit_core::{
        AttrMap, BodyFormat, DdSketch, EventBatch, Exemplar, ExpHistogram, Histogram, HyperLogLog,
        LogRecord, MetricKind, MetricRecord, Samples, Severity, SpanEvent, SpanExt, SpanKind,
        SpanLink, SpanRecord, SpanStatus, Sum, Summary, Temporality, TraceRef, Value,
    };
    use logit_proto::msgpack::{Reader, Type};
    use std::sync::Arc;

    fn attrs(pairs: &[(&str, Value)]) -> AttrMap {
        let mut map = AttrMap::new();
        for (k, v) in pairs {
            map.insert(k, v.clone());
        }
        map
    }

    /// A batch whose events reach every node of the grammar: a log with each optional field,
    /// every metric kind with its optional fields, a no-recorded-value metric, a span with its
    /// extension, events, and links, every `Value` variant (nested, escaped, invalid UTF-8,
    /// non-finite), more than 15 attributes, and a resource and scope with every field.
    pub(crate) fn every_field_batch() -> EventBatch {
        let trace = TraceRef { trace_id: [0x11; 16], span_id: Some([0x22; 8]), flags: 1 };
        let mut wide = Vec::new();
        for i in 0..20 {
            wide.push((format!("wide.k{i:02}"), Value::I64(i)));
        }
        let mut values = attrs(&[
            ("v.null", Value::Null),
            ("v.bool", Value::Bool(true)),
            ("v.neg", Value::I64(-70_000)),
            ("v.big", Value::U64(u64::MAX)),
            ("v.float", Value::F64(0.1)),
            ("v.integral", Value::F64(200.0)),
            ("v.nan", Value::F64(f64::NAN)),
            ("v.inf", Value::F64(f64::NEG_INFINITY)),
            ("v.str", Value::str("quote \" back \\ nl \n tab \t ctl \u{1} é")),
            ("v.bytes", Value::Bytes(Bytes::from_static(b"ok\xff\x00"))),
            ("v.ts", Value::Timestamp(1_700_000_000_123_456_789)),
            (
                "v.nested",
                Value::Array(vec![
                    Value::I64(1),
                    Value::Map(Box::new(attrs(&[("inner \"key\"", Value::str("x"))]))),
                ]),
            ),
        ]);
        for (k, v) in &wide {
            values.insert(k, v.clone());
        }

        let log = LogRecord {
            message: Value::str("GET /index.html"),
            severity: Some(Severity::Warn),
            body_format: BodyFormat::Json,
            trace: Some(trace),
            event_name: Some(intern("http.request")),
            observed_timestamp: 1_700_000_000_000_000_001,
            dropped_attributes_count: 2,
        };
        let log_event = Event::log(1_700_000_000_000_000_000, values, log);

        let mut sketch = DdSketch::new();
        for v in [1.0, 2.0, 40.0] {
            sketch.add(v);
        }
        let mut hll = HyperLogLog::new();
        hll.insert(b"a");
        let mut samples = Samples::new([1.5, 2.0]);
        samples.sample_rate = 0.5;
        let kinds = vec![
            MetricKind::Sum(Sum {
                value: logit_core::MetricValue::F64(3.0),
                temporality: Temporality::Delta,
                monotonic: true,
            }),
            MetricKind::gauge(f64::INFINITY),
            MetricKind::GaugeDelta(logit_core::MetricValue::F64(-0.0)),
            MetricKind::Samples(samples),
            MetricKind::SetMembers(vec![
                Bytes::from_static(b"alice"),
                Bytes::from_static(b"b\xffo"),
            ]),
            MetricKind::Distribution(sketch),
            MetricKind::Distribution(DdSketch::new()),
            MetricKind::Set(hll),
            MetricKind::Histogram(Histogram {
                buckets: vec![(0.5, 3), (f64::INFINITY, 1)],
                temporality: Temporality::Cumulative,
                sum: Some(2.5),
                min: Some(0.1),
                max: None,
            }),
            MetricKind::ExponentialHistogram(ExpHistogram {
                scale: -3,
                zero_count: 1,
                zero_threshold: 0.0,
                positive: (-2, vec![1, 300]),
                negative: (4, vec![]),
                temporality: Temporality::Delta,
                count: 302,
                sum: Some(1.5),
                min: None,
                max: Some(8.0),
            }),
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 1.0), (0.99, 4.25)],
                count: 10,
                sum: 12.0,
                temporality: Temporality::Cumulative,
            }),
        ];
        let mut metric_event = Event::empty(1_700_000_000_000_000_002, AttrMap::new());
        for (i, kind) in kinds.into_iter().enumerate() {
            let mut record = MetricRecord::new(intern(&format!("m.{i}")), kind);
            record.unit = Some(intern("ms"));
            record.description = Some(intern("a description"));
            record.start_timestamp = 5;
            record.flags = 4;
            record.exemplars = vec![Exemplar {
                timestamp: 6,
                value: logit_core::MetricValue::F64(0.25),
                trace: Some(trace),
                filtered_attributes: attrs(&[("ex.k", Value::str("v"))]),
            }];
            metric_event.metrics.push(record);
        }
        let mut nrv = MetricRecord::new(intern("m.nrv"), MetricKind::gauge(0.0));
        nrv.flags = 1;
        metric_event.metrics.push(nrv);

        let span = SpanRecord {
            trace_id: [0xab; 16],
            span_id: [0xcd; 8],
            parent_span_id: Some([0xef; 8]),
            name: Value::str("GET /"),
            kind: SpanKind::Server,
            status: SpanStatus::Error,
            events: vec![SpanEvent {
                timestamp: 1_200,
                name: Value::str("retrying"),
                attributes: attrs(&[("se.k", Value::I64(1))]),
                dropped_attributes_count: 4,
            }],
            links: vec![SpanLink {
                trace_id: [0x01; 16],
                span_id: [0x02; 8],
                attributes: attrs(&[("sl.k", Value::Bool(true))]),
                flags: 1,
                trace_state: Some(Bytes::from_static(b"a=b")),
                dropped_attributes_count: 5,
            }],
            end_timestamp: 500,
            flags: 0x301,
            ext: Some(Box::new(SpanExt {
                status_message: Some(Bytes::from_static(b"boom\xfe")),
                trace_state: Some(Bytes::from_static(b"vendor=x")),
                dropped_attributes_count: 1,
                dropped_events_count: 2,
                dropped_links_count: 3,
            })),
        };
        let span_event = Event::span(1_000, attrs(&[("s.k", Value::str("v"))]), span);

        let resource = Resource {
            attributes: attrs(&[("service.name", Value::str("api"))]),
            dropped_attributes_count: 7,
            schema_url: Some(Bytes::from_static(b"https://opentelemetry.io/schemas/1.26.0")),
        };
        let scope = Scope {
            name: Bytes::from_static(b"io.logit\xff"),
            version: Bytes::from_static(b"1.2.0"),
            attributes: attrs(&[("scope.k", Value::U64(9))]),
            dropped_attributes_count: 8,
            schema_url: Some(Bytes::from_static(b"https://example.com/s")),
        };
        EventBatch {
            resource: Arc::new(resource),
            scope: Some(Arc::new(scope)),
            events: vec![log_event, metric_event, span_event, Event::empty(-1, AttrMap::new())],
        }
    }

    /// A decoded node, for comparing what the two formats carry.
    #[derive(Debug, Clone, PartialEq)]
    pub(crate) enum Tree {
        Null,
        Bool(bool),
        Int(i128),
        Float(f64),
        Str(String),
        Bin(Vec<u8>),
        Array(Vec<Tree>),
        Map(Vec<(String, Tree)>),
    }

    /// Reads one MessagePack value off `r`.
    pub(crate) fn read_tree(r: &mut Reader<'_>) -> Tree {
        match r.peek_type().unwrap() {
            Type::Nil => {
                r.read_nil().unwrap();
                Tree::Null
            }
            Type::Bool => Tree::Bool(r.read_bool().unwrap()),
            Type::Int => Tree::Int(i128::from(r.read_i64().unwrap())),
            Type::Uint => Tree::Int(i128::from(r.read_u64().unwrap())),
            Type::Float => Tree::Float(r.read_f64().unwrap()),
            Type::Str => Tree::Str(r.read_str().unwrap().to_string()),
            Type::Bin => Tree::Bin(r.read_bin().unwrap().to_vec()),
            Type::Array => {
                let n = r.read_array_len().unwrap();
                Tree::Array((0..n).map(|_| read_tree(r)).collect())
            }
            Type::Map => {
                let n = r.read_map_len().unwrap();
                Tree::Map(
                    (0..n)
                        .map(|_| {
                            let key = r.read_str().unwrap().to_string();
                            (key, read_tree(r))
                        })
                        .collect(),
                )
            }
            Type::Ext => panic!("the writer never emits an ext"),
        }
    }

    /// `json` as a [`Tree`], keeping each object's key order.
    pub(crate) fn json_tree(json: &str) -> Tree {
        serde_json::from_str(json).expect("a line of the grammar parses")
    }

    impl<'de> serde::Deserialize<'de> for Tree {
        fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            d.deserialize_any(TreeVisitor)
        }
    }

    struct TreeVisitor;

    impl<'de> serde::de::Visitor<'de> for TreeVisitor {
        type Value = Tree;

        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("a JSON value")
        }
        fn visit_unit<E>(self) -> Result<Tree, E> {
            Ok(Tree::Null)
        }
        fn visit_bool<E>(self, v: bool) -> Result<Tree, E> {
            Ok(Tree::Bool(v))
        }
        fn visit_i64<E>(self, v: i64) -> Result<Tree, E> {
            Ok(Tree::Int(i128::from(v)))
        }
        fn visit_u64<E>(self, v: u64) -> Result<Tree, E> {
            Ok(Tree::Int(i128::from(v)))
        }
        fn visit_f64<E>(self, v: f64) -> Result<Tree, E> {
            Ok(Tree::Float(v))
        }
        fn visit_str<E>(self, v: &str) -> Result<Tree, E> {
            Ok(Tree::Str(v.to_string()))
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut seq: A) -> Result<Tree, A::Error> {
            let mut items = Vec::new();
            while let Some(item) = seq.next_element()? {
                items.push(item);
            }
            Ok(Tree::Array(items))
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Tree, A::Error> {
            let mut entries = Vec::new();
            while let Some(entry) = map.next_entry::<String, Tree>()? {
                entries.push(entry);
            }
            Ok(Tree::Map(entries))
        }
    }

    /// Whether a node of the JSON object and the same node of the MessagePack map carry the same
    /// value, under the module doc's table: a number compares by value whatever its int or
    /// float form, a non-finite float is JSON's string for it, and a bin is JSON's `b"..."`
    /// text for those bytes.
    pub(crate) fn same(json: &Tree, mp: &Tree) -> bool {
        match (json, mp) {
            (Tree::Int(a), Tree::Float(b)) | (Tree::Float(b), Tree::Int(a)) => *a as f64 == *b,
            (Tree::Float(a), Tree::Float(b)) => a == b,
            (Tree::Str(s), Tree::Float(f)) if !f.is_finite() => {
                let want = if f.is_nan() {
                    "NaN"
                } else if *f > 0.0 {
                    "inf"
                } else {
                    "-inf"
                };
                s == want
            }
            (Tree::Str(s), Tree::Bin(b)) => {
                let mut text = String::new();
                for chunk in b.utf8_chunks() {
                    text.push_str(&chunk.valid().replace('\\', "\\x5c"));
                    for byte in chunk.invalid() {
                        text.push_str(&format!("\\x{byte:02x}"));
                    }
                }
                *s == format!("b\"{text}\"")
            }
            (Tree::Array(a), Tree::Array(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| same(x, y))
            }
            (Tree::Map(a), Tree::Map(b)) => {
                a.len() == b.len()
                    && a.iter().zip(b).all(|((ka, va), (kb, vb))| ka == kb && same(va, vb))
            }
            _ => json == mp,
        }
    }

    /// One walker writes both formats, so every event's MessagePack map holds the NDJSON
    /// object's keys in the same order, with the same values under the module doc's table.
    #[test]
    fn each_event_is_the_ndjson_object_key_for_key() {
        let batch = every_field_batch();
        let mut out = Writer::new();
        let mut scratch = String::new();
        for event in &batch.events {
            let mut line = String::new();
            crate::ndjson::render_event(&mut line, &batch.resource, batch.scope.as_deref(), event);
            out.clear();
            render_event(&mut out, &mut scratch, &batch.resource, batch.scope.as_deref(), event);
            let mut r = Reader::new(out.as_slice());
            let mp = read_tree(&mut r);
            assert!(r.is_empty(), "one map, nothing after it");
            let json = json_tree(&line);
            assert!(same(&json, &mp), "json: {json:#?}\nmsgpack: {mp:#?}");
        }
    }

    /// The forms the table names where MessagePack and JSON differ.
    #[test]
    fn bytes_are_bin_and_non_finite_floats_are_floats() {
        let batch = every_field_batch();
        let mut out = Writer::new();
        let mut scratch = String::new();
        render_event(&mut out, &mut scratch, &batch.resource, None, &batch.events[0]);
        let Tree::Map(top) = read_tree(&mut Reader::new(out.as_slice())) else { panic!() };
        let attributes = &top.iter().find(|(k, _)| k == "attributes").unwrap().1;
        let Tree::Map(attributes) = attributes else { panic!() };
        let get = |k: &str| &attributes.iter().find(|(key, _)| key == k).unwrap().1;
        assert_eq!(get("v.bytes"), &Tree::Bin(b"ok\xff\x00".to_vec()));
        assert!(matches!(get("v.nan"), Tree::Float(f) if f.is_nan()));
        assert_eq!(get("v.inf"), &Tree::Float(f64::NEG_INFINITY));
        assert_eq!(get("v.integral"), &Tree::Float(200.0));
        assert_eq!(get("v.big"), &Tree::Int(i128::from(u64::MAX)));
        assert_eq!(get("v.ts"), &Tree::Str("2023-11-14T22:13:20.123456789Z".into()));
    }
}
