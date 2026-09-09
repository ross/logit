//! A plain-data mirror of [`EventBatch`]/[`Event`], used only by the native-wire-format bake-off's
//! `rkyv` and `postcard` arms (`docs/design/wire-protocol.md`'s "Encoding: decide with a
//! benchmark, not up front", `docs/adr/native-wire-format-encoding.md`) -- **not** the shipped
//! format (`logit_proto::native`, which is hand-rolled either way and doesn't use this type at
//! all).
//!
//! Holds no foreign types -- no `bytes::Bytes`, no `SmallVec`, no `lasso::Spur`, no `DDSketch` --
//! so both `rkyv` and `serde` can derive their traits directly with no remote-type wrapper
//! plumbing. Two of the correctness rules `logit_proto::native`'s own module doc states apply here
//! too, for a fair comparison: a `Symbol` is dictionary-indexed rather than written raw, and
//! `MetricKind::Distribution` rides as `DdSketch::to_java_bytes()`'s blob.

use std::collections::HashMap;
use std::sync::Arc;

use logit_core::{
    interner, AttrMap, BodyFormat, DdSketch, Event, EventBatch, HyperLogLog, LogRecord, MetricKind,
    MetricList, MetricRecord, Resource, Severity, SpanEvent, SpanKind, SpanLink, SpanRecord,
    SpanStatus, Symbol, TraceRef, Value,
};

// `WireValue` is directly recursive (`Array`/`Map` hold more `WireValue`s), which `rkyv`'s derive
// can't bound automatically -- `HashMap<String, JsonValue>: Archive` requiring `JsonValue:
// Archive` requiring `HashMap<..>: Archive` again is an infinite regress the compiler reports as
// "overflow evaluating the requirement". `#[rkyv(omit_bounds)]` on the two recursive fields below
// stops the derive from generating that bound at all; the three attributes on the enum restate,
// by hand, the narrower non-recursive bounds `Vec`'s own (de)serialization and validation actually
// need. This is exactly rkyv's own documented pattern for a JSON-like recursive value type
// (`rkyv/examples/json_like_schema.rs` in the `rkyv/rkyv` repository), applied here verbatim.
#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
#[rkyv(serialize_bounds(
    __S: rkyv::ser::Writer + rkyv::ser::Allocator,
    __S::Error: rkyv::rancor::Source,
))]
#[rkyv(deserialize_bounds(__D::Error: rkyv::rancor::Source))]
#[rkyv(bytecheck(bounds(__C: rkyv::validation::ArchiveContext)))]
pub enum WireValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    Bytes(Vec<u8>),
    Str(String),
    Timestamp(i64),
    Array(#[rkyv(omit_bounds)] Vec<WireValue>),
    Map(#[rkyv(omit_bounds)] Vec<(u32, WireValue)>),
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
pub struct WireTraceRef {
    pub trace_id: [u8; 16],
    pub span_id: Option<[u8; 8]>,
    pub flags: u8,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
pub struct WireLog {
    pub message: WireValue,
    pub severity: Option<u8>,
    pub body_format: u8,
    pub trace: Option<WireTraceRef>,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
pub enum WireMetricKind {
    Counter(f64),
    Gauge(f64),
    GaugeDelta(f64),
    Set,
    /// `DdSketch::to_java_bytes()` -- the same canonical, cross-language blob
    /// `logit_proto::native` uses, since `DDSketch`'s fields are private with no bin iteration
    /// (`crates/logit-core/src/metric.rs`).
    Distribution(Vec<u8>),
    Histogram(Vec<(f64, u64)>),
    Summary(Vec<(f64, f64)>),
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
pub struct WireMetric {
    pub name: u32,
    pub unit: Option<u32>,
    pub kind: WireMetricKind,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
pub struct WireSpanEvent {
    pub timestamp: i64,
    pub name: WireValue,
    pub attributes: Vec<(u32, WireValue)>,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
pub struct WireSpanLink {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub attributes: Vec<(u32, WireValue)>,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
pub struct WireSpan {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
    pub parent_span_id: Option<[u8; 8]>,
    pub name: WireValue,
    pub kind: u8,
    pub status: u8,
    pub events: Vec<WireSpanEvent>,
    pub links: Vec<WireSpanLink>,
    pub end_timestamp: i64,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
pub struct WireEvent {
    pub timestamp: i64,
    pub attributes: Vec<(u32, WireValue)>,
    pub log: Option<WireLog>,
    pub metrics: Vec<WireMetric>,
    pub span: Option<WireSpan>,
}

#[derive(
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
    serde::Serialize,
    serde::Deserialize,
    Debug,
    Clone,
    PartialEq,
)]
pub struct WireBatch {
    pub dict: Vec<String>,
    pub resource_attrs: Vec<(u32, WireValue)>,
    pub events: Vec<WireEvent>,
}

#[derive(Default)]
struct DictBuilder {
    strings: Vec<String>,
    index: HashMap<Symbol, u32>,
}

impl DictBuilder {
    fn intern(&mut self, sym: Symbol) -> u32 {
        if let Some(&i) = self.index.get(&sym) {
            return i;
        }
        let i = self.strings.len() as u32;
        self.strings.push(interner::resolve(sym).to_string());
        self.index.insert(sym, i);
        i
    }
}

fn value_to_wire(dict: &mut DictBuilder, value: &Value) -> WireValue {
    match value {
        Value::Null => WireValue::Null,
        Value::Bool(b) => WireValue::Bool(*b),
        Value::I64(v) => WireValue::I64(*v),
        Value::U64(v) => WireValue::U64(*v),
        Value::F64(v) => WireValue::F64(*v),
        Value::Bytes(b) => WireValue::Bytes(b.to_vec()),
        Value::Str(s) => WireValue::Str(
            std::str::from_utf8(s).expect("Value::Str is always valid UTF-8").to_string(),
        ),
        Value::Timestamp(v) => WireValue::Timestamp(*v),
        Value::Array(items) => {
            WireValue::Array(items.iter().map(|v| value_to_wire(dict, v)).collect())
        }
        Value::Map(map) => WireValue::Map(attr_map_to_wire(dict, map)),
    }
}

fn attr_map_to_wire(dict: &mut DictBuilder, map: &AttrMap) -> Vec<(u32, WireValue)> {
    map.iter().map(|(k, v)| (dict.intern(k), value_to_wire(dict, v))).collect()
}

fn wire_to_value(strings: &[Symbol], value: &WireValue) -> Value {
    match value {
        WireValue::Null => Value::Null,
        WireValue::Bool(b) => Value::Bool(*b),
        WireValue::I64(v) => Value::I64(*v),
        WireValue::U64(v) => Value::U64(*v),
        WireValue::F64(v) => Value::F64(*v),
        WireValue::Bytes(b) => Value::Bytes(bytes::Bytes::from(b.clone())),
        WireValue::Str(s) => Value::str(s.as_str()),
        WireValue::Timestamp(v) => Value::Timestamp(*v),
        WireValue::Array(items) => {
            Value::Array(items.iter().map(|v| wire_to_value(strings, v)).collect())
        }
        WireValue::Map(pairs) => Value::Map(Box::new(wire_to_attr_map(strings, pairs))),
    }
}

fn wire_to_attr_map(strings: &[Symbol], pairs: &[(u32, WireValue)]) -> AttrMap {
    let mut map = AttrMap::new();
    for (idx, value) in pairs {
        map.insert_sym(strings[*idx as usize], wire_to_value(strings, value));
    }
    map
}

fn metric_kind_to_wire(kind: &MetricKind) -> WireMetricKind {
    match kind {
        MetricKind::Counter(v) => WireMetricKind::Counter(*v),
        MetricKind::Gauge(v) => WireMetricKind::Gauge(*v),
        MetricKind::GaugeDelta(v) => WireMetricKind::GaugeDelta(*v),
        MetricKind::Set(_) => WireMetricKind::Set,
        MetricKind::Distribution(sketch) => WireMetricKind::Distribution(sketch.to_java_bytes()),
        MetricKind::Histogram { buckets } => WireMetricKind::Histogram(buckets.clone()),
        MetricKind::Summary { quantiles } => WireMetricKind::Summary(quantiles.clone()),
    }
}

fn wire_to_metric_kind(kind: &WireMetricKind) -> MetricKind {
    match kind {
        WireMetricKind::Counter(v) => MetricKind::Counter(*v),
        WireMetricKind::Gauge(v) => MetricKind::Gauge(*v),
        WireMetricKind::GaugeDelta(v) => MetricKind::GaugeDelta(*v),
        WireMetricKind::Set => MetricKind::Set(HyperLogLog::default()),
        WireMetricKind::Distribution(blob) => {
            MetricKind::Distribution(DdSketch::from_java_bytes(blob).expect("valid blob"))
        }
        WireMetricKind::Histogram(buckets) => MetricKind::Histogram { buckets: buckets.clone() },
        WireMetricKind::Summary(quantiles) => MetricKind::Summary { quantiles: quantiles.clone() },
    }
}

fn metric_record_to_wire(dict: &mut DictBuilder, record: &MetricRecord) -> WireMetric {
    WireMetric {
        name: dict.intern(record.name),
        unit: record.unit.map(|u| dict.intern(u)),
        kind: metric_kind_to_wire(&record.kind),
    }
}

fn wire_to_metric_record(strings: &[Symbol], wire: &WireMetric) -> MetricRecord {
    MetricRecord {
        name: strings[wire.name as usize],
        kind: wire_to_metric_kind(&wire.kind),
        unit: wire.unit.map(|i| strings[i as usize]),
    }
}

fn severity_tag(s: Severity) -> u8 {
    match s {
        Severity::Trace => 0,
        Severity::Debug => 1,
        Severity::Info => 2,
        Severity::Warn => 3,
        Severity::Error => 4,
        Severity::Fatal => 5,
    }
}

fn severity_from_tag(tag: u8) -> Severity {
    match tag {
        0 => Severity::Trace,
        1 => Severity::Debug,
        2 => Severity::Info,
        3 => Severity::Warn,
        4 => Severity::Error,
        _ => Severity::Fatal,
    }
}

fn body_format_tag(f: BodyFormat) -> u8 {
    match f {
        BodyFormat::Raw => 0,
        BodyFormat::Json => 1,
        BodyFormat::Structured => 2,
    }
}

fn body_format_from_tag(tag: u8) -> BodyFormat {
    match tag {
        0 => BodyFormat::Raw,
        1 => BodyFormat::Json,
        _ => BodyFormat::Structured,
    }
}

fn log_to_wire(dict: &mut DictBuilder, log: &LogRecord) -> WireLog {
    WireLog {
        message: value_to_wire(dict, &log.message),
        severity: log.severity.map(severity_tag),
        body_format: body_format_tag(log.body_format),
        trace: log.trace.map(|t| WireTraceRef {
            trace_id: t.trace_id,
            span_id: t.span_id,
            flags: t.flags,
        }),
    }
}

fn wire_to_log(strings: &[Symbol], wire: &WireLog) -> LogRecord {
    LogRecord {
        message: wire_to_value(strings, &wire.message),
        severity: wire.severity.map(severity_from_tag),
        body_format: body_format_from_tag(wire.body_format),
        trace: wire.trace.as_ref().map(|t| TraceRef {
            trace_id: t.trace_id,
            span_id: t.span_id,
            flags: t.flags,
        }),
    }
}

fn span_kind_tag(k: SpanKind) -> u8 {
    match k {
        SpanKind::Internal => 0,
        SpanKind::Server => 1,
        SpanKind::Client => 2,
        SpanKind::Producer => 3,
        SpanKind::Consumer => 4,
    }
}

fn span_kind_from_tag(tag: u8) -> SpanKind {
    match tag {
        0 => SpanKind::Internal,
        1 => SpanKind::Server,
        2 => SpanKind::Client,
        3 => SpanKind::Producer,
        _ => SpanKind::Consumer,
    }
}

fn span_status_tag(s: SpanStatus) -> u8 {
    match s {
        SpanStatus::Unset => 0,
        SpanStatus::Ok => 1,
        SpanStatus::Error => 2,
    }
}

fn span_status_from_tag(tag: u8) -> SpanStatus {
    match tag {
        0 => SpanStatus::Unset,
        1 => SpanStatus::Ok,
        _ => SpanStatus::Error,
    }
}

fn span_to_wire(dict: &mut DictBuilder, span: &SpanRecord) -> WireSpan {
    WireSpan {
        trace_id: span.trace_id,
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: value_to_wire(dict, &span.name),
        kind: span_kind_tag(span.kind),
        status: span_status_tag(span.status),
        events: span
            .events
            .iter()
            .map(|e| WireSpanEvent {
                timestamp: e.timestamp,
                name: value_to_wire(dict, &e.name),
                attributes: attr_map_to_wire(dict, &e.attributes),
            })
            .collect(),
        links: span
            .links
            .iter()
            .map(|l| WireSpanLink {
                trace_id: l.trace_id,
                span_id: l.span_id,
                attributes: attr_map_to_wire(dict, &l.attributes),
            })
            .collect(),
        end_timestamp: span.end_timestamp,
    }
}

fn wire_to_span(strings: &[Symbol], wire: &WireSpan) -> SpanRecord {
    SpanRecord {
        trace_id: wire.trace_id,
        span_id: wire.span_id,
        parent_span_id: wire.parent_span_id,
        name: wire_to_value(strings, &wire.name),
        kind: span_kind_from_tag(wire.kind),
        status: span_status_from_tag(wire.status),
        events: wire
            .events
            .iter()
            .map(|e| SpanEvent {
                timestamp: e.timestamp,
                name: wire_to_value(strings, &e.name),
                attributes: wire_to_attr_map(strings, &e.attributes),
            })
            .collect(),
        links: wire
            .links
            .iter()
            .map(|l| SpanLink {
                trace_id: l.trace_id,
                span_id: l.span_id,
                attributes: wire_to_attr_map(strings, &l.attributes),
            })
            .collect(),
        end_timestamp: wire.end_timestamp,
    }
}

fn event_to_wire(dict: &mut DictBuilder, event: &Event) -> WireEvent {
    WireEvent {
        timestamp: event.timestamp,
        attributes: attr_map_to_wire(dict, &event.attributes),
        log: event.log.as_ref().map(|l| log_to_wire(dict, l)),
        metrics: event.metrics.iter().map(|m| metric_record_to_wire(dict, m)).collect(),
        span: event.span.as_ref().map(|s| span_to_wire(dict, s)),
    }
}

fn wire_to_event(strings: &[Symbol], wire: &WireEvent) -> Event {
    Event {
        timestamp: wire.timestamp,
        attributes: wire_to_attr_map(strings, &wire.attributes),
        log: wire.log.as_ref().map(|l| wire_to_log(strings, l)),
        metrics: wire
            .metrics
            .iter()
            .map(|m| wire_to_metric_record(strings, m))
            .collect::<MetricList>(),
        span: wire.span.as_ref().map(|s| wire_to_span(strings, s)),
    }
}

impl WireBatch {
    pub fn from_event_batch(batch: &EventBatch) -> WireBatch {
        let mut dict = DictBuilder::default();
        let resource_attrs = attr_map_to_wire(&mut dict, &batch.resource.attributes);
        let events = batch.events.iter().map(|e| event_to_wire(&mut dict, e)).collect();
        WireBatch { dict: dict.strings, resource_attrs, events }
    }

    pub fn into_event_batch(self) -> EventBatch {
        let strings: Vec<Symbol> = self.dict.iter().map(|s| interner::intern(s)).collect();
        let resource =
            Arc::new(Resource { attributes: wire_to_attr_map(&strings, &self.resource_attrs) });
        let events = self.events.iter().map(|e| wire_to_event(&strings, e)).collect();
        EventBatch { resource, events }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fixtures;

    #[test]
    fn round_trips_the_nginx_event_through_the_mirror() {
        let batch = fixtures::nginx_batch(3);
        let wire = WireBatch::from_event_batch(&batch);
        let back = wire.into_event_batch();
        assert_eq!(back.events.len(), batch.events.len());
        assert_eq!(back.resource.attributes, batch.resource.attributes);
        for (a, b) in back.events.iter().zip(batch.events.iter()) {
            assert_eq!(a.attributes, b.attributes);
            assert_eq!(a.metrics.len(), b.metrics.len());
        }
    }

    #[test]
    fn round_trips_the_span_event_through_the_mirror() {
        let event = fixtures::span_event();
        let batch =
            EventBatch { resource: Arc::new(Resource::default()), events: vec![event.clone()] };
        let wire = WireBatch::from_event_batch(&batch);
        let back = wire.into_event_batch();
        assert!(back.events[0].span.is_some());
        assert_eq!(back.events[0].span.as_ref().unwrap().name, event.span.unwrap().name);
    }
}
