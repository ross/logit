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
    interner, AttrMap, BodyFormat, DdSketch, Event, EventBatch, Exemplar, ExpHistogram, Histogram,
    HyperLogLog, LogRecord, MetricKind, MetricList, MetricRecord, Resource, Samples, Scope,
    Severity, SpanEvent, SpanExt, SpanKind, SpanLink, SpanRecord, SpanStatus, Sum, Summary, Symbol,
    Temporality, TraceRef, Value,
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
    pub event_name: Option<u32>,
    pub observed_timestamp: i64,
    pub dropped_attributes_count: u32,
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
pub struct WireSum {
    pub value: f64,
    pub temporality: u8,
    pub monotonic: bool,
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
pub struct WireSamples {
    pub values: Vec<f64>,
    pub rate: f64,
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
pub struct WireHistogram {
    pub buckets: Vec<(f64, u64)>,
    pub temporality: u8,
    pub sum: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
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
pub struct WireExpHistogram {
    pub scale: i32,
    pub zero_count: u64,
    pub zero_threshold: f64,
    pub positive_offset: i32,
    pub positive: Vec<u64>,
    pub negative_offset: i32,
    pub negative: Vec<u64>,
    pub temporality: u8,
    pub count: u64,
    pub sum: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
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
pub struct WireSummary {
    pub quantiles: Vec<(f64, f64)>,
    pub count: u64,
    pub sum: f64,
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
    Sum(WireSum),
    Gauge(f64),
    GaugeDelta(f64),
    Samples(WireSamples),
    /// `DdSketch::to_java_bytes()` -- the same canonical, cross-language blob
    /// `logit_proto::native` uses, since `DDSketch`'s fields are private with no bin iteration
    /// (`crates/logit-core/src/metric.rs`).
    Distribution(Vec<u8>),
    SetMembers(Vec<Vec<u8>>),
    Set,
    Histogram(WireHistogram),
    ExponentialHistogram(WireExpHistogram),
    Summary(WireSummary),
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
pub struct WireExemplar {
    pub timestamp: i64,
    pub value: f64,
    pub trace: Option<WireTraceRef>,
    pub filtered_attributes: Vec<(u32, WireValue)>,
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
    pub description: Option<u32>,
    pub start_timestamp: i64,
    pub exemplars: Vec<WireExemplar>,
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
    pub dropped_attributes_count: u32,
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
    pub flags: u32,
    pub trace_state: Option<Vec<u8>>,
    pub dropped_attributes_count: u32,
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
pub struct WireSpanExt {
    pub status_message: Option<Vec<u8>>,
    pub trace_state: Option<Vec<u8>>,
    pub dropped_attributes_count: u32,
    pub dropped_events_count: u32,
    pub dropped_links_count: u32,
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
    pub flags: u32,
    pub ext: Option<WireSpanExt>,
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
pub struct WireScope {
    pub name: Vec<u8>,
    pub version: Vec<u8>,
    pub attributes: Vec<(u32, WireValue)>,
    pub dropped_attributes_count: u32,
    pub schema_url: Option<Vec<u8>>,
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
    pub resource_dropped_attributes_count: u32,
    pub resource_schema_url: Option<Vec<u8>>,
    pub scope: Option<WireScope>,
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

fn temporality_tag(t: Temporality) -> u8 {
    match t {
        Temporality::Delta => 0,
        Temporality::Cumulative => 1,
    }
}

fn temporality_from_tag(tag: u8) -> Temporality {
    match tag {
        0 => Temporality::Delta,
        _ => Temporality::Cumulative,
    }
}

fn metric_kind_to_wire(kind: &MetricKind) -> WireMetricKind {
    match kind {
        MetricKind::Sum(sum) => WireMetricKind::Sum(WireSum {
            value: sum.value,
            temporality: temporality_tag(sum.temporality),
            monotonic: sum.monotonic,
        }),
        MetricKind::Gauge(v) => WireMetricKind::Gauge(*v),
        MetricKind::GaugeDelta(v) => WireMetricKind::GaugeDelta(*v),
        MetricKind::Samples(s) => WireMetricKind::Samples(WireSamples {
            values: s.values.iter().copied().collect(),
            rate: s.sample_rate,
        }),
        MetricKind::Distribution(sketch) => WireMetricKind::Distribution(sketch.to_java_bytes()),
        MetricKind::SetMembers(members) => {
            WireMetricKind::SetMembers(members.iter().map(|m| m.to_vec()).collect())
        }
        MetricKind::Set(_) => WireMetricKind::Set,
        MetricKind::Histogram(h) => WireMetricKind::Histogram(WireHistogram {
            buckets: h.buckets.clone(),
            temporality: temporality_tag(h.temporality),
            sum: h.sum,
            min: h.min,
            max: h.max,
        }),
        MetricKind::ExponentialHistogram(e) => {
            WireMetricKind::ExponentialHistogram(WireExpHistogram {
                scale: e.scale,
                zero_count: e.zero_count,
                zero_threshold: e.zero_threshold,
                positive_offset: e.positive.0,
                positive: e.positive.1.clone(),
                negative_offset: e.negative.0,
                negative: e.negative.1.clone(),
                temporality: temporality_tag(e.temporality),
                count: e.count,
                sum: e.sum,
                min: e.min,
                max: e.max,
            })
        }
        MetricKind::Summary(s) => WireMetricKind::Summary(WireSummary {
            quantiles: s.quantiles.clone(),
            count: s.count,
            sum: s.sum,
        }),
    }
}

fn wire_to_metric_kind(kind: &WireMetricKind) -> MetricKind {
    match kind {
        WireMetricKind::Sum(sum) => MetricKind::Sum(Sum {
            value: sum.value,
            temporality: temporality_from_tag(sum.temporality),
            monotonic: sum.monotonic,
        }),
        WireMetricKind::Gauge(v) => MetricKind::Gauge(*v),
        WireMetricKind::GaugeDelta(v) => MetricKind::GaugeDelta(*v),
        WireMetricKind::Samples(s) => MetricKind::Samples(Samples {
            values: s.values.iter().copied().collect(),
            sample_rate: s.rate,
        }),
        WireMetricKind::Distribution(blob) => {
            MetricKind::Distribution(DdSketch::from_java_bytes(blob).expect("valid blob"))
        }
        WireMetricKind::SetMembers(members) => {
            MetricKind::SetMembers(members.iter().map(|m| bytes::Bytes::from(m.clone())).collect())
        }
        WireMetricKind::Set => MetricKind::Set(HyperLogLog::default()),
        WireMetricKind::Histogram(h) => MetricKind::Histogram(Histogram {
            buckets: h.buckets.clone(),
            temporality: temporality_from_tag(h.temporality),
            sum: h.sum,
            min: h.min,
            max: h.max,
        }),
        WireMetricKind::ExponentialHistogram(e) => MetricKind::ExponentialHistogram(ExpHistogram {
            scale: e.scale,
            zero_count: e.zero_count,
            zero_threshold: e.zero_threshold,
            positive: (e.positive_offset, e.positive.clone()),
            negative: (e.negative_offset, e.negative.clone()),
            temporality: temporality_from_tag(e.temporality),
            count: e.count,
            sum: e.sum,
            min: e.min,
            max: e.max,
        }),
        WireMetricKind::Summary(s) => MetricKind::Summary(Summary {
            quantiles: s.quantiles.clone(),
            count: s.count,
            sum: s.sum,
        }),
    }
}

fn exemplar_to_wire(dict: &mut DictBuilder, exemplar: &Exemplar) -> WireExemplar {
    WireExemplar {
        timestamp: exemplar.timestamp,
        value: exemplar.value,
        trace: exemplar.trace.map(|t| WireTraceRef {
            trace_id: t.trace_id,
            span_id: t.span_id,
            flags: t.flags,
        }),
        filtered_attributes: attr_map_to_wire(dict, &exemplar.filtered_attributes),
    }
}

fn wire_to_exemplar(strings: &[Symbol], wire: &WireExemplar) -> Exemplar {
    Exemplar {
        timestamp: wire.timestamp,
        value: wire.value,
        trace: wire.trace.as_ref().map(|t| TraceRef {
            trace_id: t.trace_id,
            span_id: t.span_id,
            flags: t.flags,
        }),
        filtered_attributes: wire_to_attr_map(strings, &wire.filtered_attributes),
    }
}

fn metric_record_to_wire(dict: &mut DictBuilder, record: &MetricRecord) -> WireMetric {
    WireMetric {
        name: dict.intern(record.name),
        unit: record.unit.map(|u| dict.intern(u)),
        description: record.description.map(|d| dict.intern(d)),
        start_timestamp: record.start_timestamp,
        exemplars: record.exemplars.iter().map(|e| exemplar_to_wire(dict, e)).collect(),
        kind: metric_kind_to_wire(&record.kind),
    }
}

fn wire_to_metric_record(strings: &[Symbol], wire: &WireMetric) -> MetricRecord {
    MetricRecord {
        name: strings[wire.name as usize],
        unit: wire.unit.map(|i| strings[i as usize]),
        description: wire.description.map(|i| strings[i as usize]),
        start_timestamp: wire.start_timestamp,
        exemplars: wire.exemplars.iter().map(|e| wire_to_exemplar(strings, e)).collect(),
        kind: wire_to_metric_kind(&wire.kind),
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
        event_name: log.event_name.map(|s| dict.intern(s)),
        observed_timestamp: log.observed_timestamp,
        dropped_attributes_count: log.dropped_attributes_count,
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
        event_name: wire.event_name.map(|i| strings[i as usize]),
        observed_timestamp: wire.observed_timestamp,
        dropped_attributes_count: wire.dropped_attributes_count,
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

fn span_ext_to_wire(ext: &SpanExt) -> WireSpanExt {
    WireSpanExt {
        status_message: ext.status_message.as_ref().map(|b| b.to_vec()),
        trace_state: ext.trace_state.as_ref().map(|b| b.to_vec()),
        dropped_attributes_count: ext.dropped_attributes_count,
        dropped_events_count: ext.dropped_events_count,
        dropped_links_count: ext.dropped_links_count,
    }
}

fn wire_to_span_ext(wire: &WireSpanExt) -> SpanExt {
    SpanExt {
        status_message: wire.status_message.as_ref().map(|b| bytes::Bytes::from(b.clone())),
        trace_state: wire.trace_state.as_ref().map(|b| bytes::Bytes::from(b.clone())),
        dropped_attributes_count: wire.dropped_attributes_count,
        dropped_events_count: wire.dropped_events_count,
        dropped_links_count: wire.dropped_links_count,
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
                dropped_attributes_count: e.dropped_attributes_count,
            })
            .collect(),
        links: span
            .links
            .iter()
            .map(|l| WireSpanLink {
                trace_id: l.trace_id,
                span_id: l.span_id,
                attributes: attr_map_to_wire(dict, &l.attributes),
                flags: l.flags,
                trace_state: l.trace_state.as_ref().map(|b| b.to_vec()),
                dropped_attributes_count: l.dropped_attributes_count,
            })
            .collect(),
        end_timestamp: span.end_timestamp,
        flags: span.flags,
        ext: span.ext.as_deref().map(span_ext_to_wire),
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
                dropped_attributes_count: e.dropped_attributes_count,
            })
            .collect(),
        links: wire
            .links
            .iter()
            .map(|l| SpanLink {
                trace_id: l.trace_id,
                span_id: l.span_id,
                attributes: wire_to_attr_map(strings, &l.attributes),
                flags: l.flags,
                trace_state: l.trace_state.as_ref().map(|b| bytes::Bytes::from(b.clone())),
                dropped_attributes_count: l.dropped_attributes_count,
            })
            .collect(),
        end_timestamp: wire.end_timestamp,
        flags: wire.flags,
        ext: wire.ext.as_ref().map(|e| Box::new(wire_to_span_ext(e))),
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

fn scope_to_wire(dict: &mut DictBuilder, scope: &Scope) -> WireScope {
    WireScope {
        name: scope.name.to_vec(),
        version: scope.version.to_vec(),
        attributes: attr_map_to_wire(dict, &scope.attributes),
        dropped_attributes_count: scope.dropped_attributes_count,
        schema_url: scope.schema_url.as_ref().map(|s| s.to_vec()),
    }
}

fn wire_to_scope(strings: &[Symbol], wire: &WireScope) -> Scope {
    Scope {
        name: bytes::Bytes::from(wire.name.clone()),
        version: bytes::Bytes::from(wire.version.clone()),
        attributes: wire_to_attr_map(strings, &wire.attributes),
        dropped_attributes_count: wire.dropped_attributes_count,
        schema_url: wire.schema_url.as_ref().map(|s| bytes::Bytes::from(s.clone())),
    }
}

impl WireBatch {
    pub fn from_event_batch(batch: &EventBatch) -> WireBatch {
        let mut dict = DictBuilder::default();
        let resource_attrs = attr_map_to_wire(&mut dict, &batch.resource.attributes);
        let scope = batch.scope.as_deref().map(|s| scope_to_wire(&mut dict, s));
        let events = batch.events.iter().map(|e| event_to_wire(&mut dict, e)).collect();
        WireBatch {
            dict: dict.strings,
            resource_attrs,
            resource_dropped_attributes_count: batch.resource.dropped_attributes_count,
            resource_schema_url: batch.resource.schema_url.as_ref().map(|s| s.to_vec()),
            scope,
            events,
        }
    }

    pub fn into_event_batch(self) -> EventBatch {
        let strings: Vec<Symbol> = self.dict.iter().map(|s| interner::intern(s)).collect();
        let resource = Arc::new(Resource {
            attributes: wire_to_attr_map(&strings, &self.resource_attrs),
            dropped_attributes_count: self.resource_dropped_attributes_count,
            schema_url: self.resource_schema_url.map(bytes::Bytes::from),
        });
        let scope = self.scope.as_ref().map(|s| Arc::new(wire_to_scope(&strings, s)));
        let events = self.events.iter().map(|e| wire_to_event(&strings, e)).collect();
        EventBatch { resource, scope, events }
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
        assert_eq!(back, batch);
    }

    #[test]
    fn round_trips_the_span_event_through_the_mirror() {
        let event = fixtures::span_event();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![event.clone()],
        };
        let wire = WireBatch::from_event_batch(&batch);
        let back = wire.into_event_batch();
        assert_eq!(back, batch);
    }

    /// A fully-populated batch -- every metric kind, a populated `Scope`, and a `SpanExt` -- round
    /// trips through the mirror exactly, now that `PartialEq` exists on `EventBatch`.
    #[test]
    fn round_trips_a_fully_populated_batch_through_the_mirror() {
        let mut sketch = DdSketch::new();
        sketch.add(1.0);
        sketch.add(2.0);

        let sum_record = MetricRecord {
            unit: Some(interner::intern("1")),
            description: Some(interner::intern("a delta monotonic sum")),
            start_timestamp: 100,
            exemplars: vec![Exemplar {
                timestamp: 42,
                value: 7.0,
                trace: Some(TraceRef { trace_id: [0xAB; 16], span_id: Some([0xCD; 8]), flags: 1 }),
                filtered_attributes: {
                    let mut m = AttrMap::new();
                    m.insert("dropped", "yes");
                    m
                },
            }],
            ..MetricRecord::new(
                interner::intern("wire_mirror_test.sum"),
                MetricKind::Sum(Sum {
                    value: 3.0,
                    temporality: Temporality::Cumulative,
                    monotonic: false,
                }),
            )
        };

        let samples_record = MetricRecord::new(interner::intern("wire_mirror_test.samples"), {
            let mut samples = Samples::new([1.0, 2.0, 3.0]);
            samples.sample_rate = 0.1;
            MetricKind::Samples(samples)
        });

        let set_members_record = MetricRecord::new(
            interner::intern("wire_mirror_test.set_members"),
            MetricKind::SetMembers(vec![
                bytes::Bytes::from_static(b"a"),
                bytes::Bytes::from_static(b"b"),
            ]),
        );

        let exp_histogram_record = MetricRecord::new(
            interner::intern("wire_mirror_test.exp_histogram"),
            MetricKind::ExponentialHistogram(ExpHistogram {
                scale: 3,
                zero_count: 1,
                zero_threshold: 0.001,
                positive: (0, vec![1, 2, 3]),
                negative: (-1, vec![4, 5]),
                temporality: Temporality::Cumulative,
                count: 15,
                sum: Some(42.0),
                min: Some(0.1),
                max: Some(9.9),
            }),
        );

        let histogram_record = MetricRecord::new(
            interner::intern("wire_mirror_test.histogram"),
            MetricKind::Histogram(Histogram {
                buckets: vec![(1.0, 2), (5.0, 3)],
                temporality: Temporality::Delta,
                sum: Some(11.0),
                min: Some(0.5),
                max: Some(4.5),
            }),
        );

        let summary_record = MetricRecord::new(
            interner::intern("wire_mirror_test.summary"),
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 1.0), (0.99, 9.0)],
                count: 10,
                sum: 20.0,
            }),
        );

        let distribution_record = MetricRecord::new(
            interner::intern("wire_mirror_test.distribution"),
            MetricKind::Distribution(sketch),
        );

        let mut event = fixtures::span_event();
        for record in [
            sum_record,
            samples_record,
            set_members_record,
            exp_histogram_record,
            histogram_record,
            summary_record,
            distribution_record,
        ] {
            event.metrics.push(record);
        }
        if let Some(span) = event.span.as_mut() {
            span.flags = 1;
            span.ext = Some(Box::new(SpanExt {
                status_message: Some(bytes::Bytes::from_static(b"boom")),
                trace_state: Some(bytes::Bytes::from_static(b"vendor=value")),
                dropped_attributes_count: 2,
                dropped_events_count: 1,
                dropped_links_count: 1,
            }));
        }

        let mut scope_attrs = AttrMap::new();
        scope_attrs.insert("scope.attr", "value");
        let scope = Arc::new(Scope {
            name: bytes::Bytes::from_static(b"wire_mirror_test_scope"),
            version: bytes::Bytes::from_static(b"1.2.3"),
            attributes: scope_attrs,
            dropped_attributes_count: 1,
            schema_url: Some(bytes::Bytes::from_static(b"https://example.com/schema")),
        });

        let resource = Arc::new(Resource {
            attributes: {
                let mut m = AttrMap::new();
                m.insert("service.name", "wire-mirror-test");
                m
            },
            dropped_attributes_count: 3,
            schema_url: Some(bytes::Bytes::from_static(b"https://example.com/resource-schema")),
        });

        let batch = EventBatch { resource, scope: Some(scope), events: vec![event] };

        let wire = WireBatch::from_event_batch(&batch);
        let back = wire.into_event_batch();
        assert_eq!(back, batch);
    }
}
