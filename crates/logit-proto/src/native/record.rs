//! `MetricRecord`/`LogRecord`/`SpanRecord`/`Event` wire encoding. `Event` is the one extensible
//! record: its fields are written as `tag(1) + len(varint) + payload`, exactly like [`super::value`]'s
//! per-value framing, so a future field this reader doesn't recognize is skipped whole rather than
//! corrupting the rest of the event -- the version-skew gate `docs/design/wire-protocol.md` calls
//! for. `MetricRecord`, `LogRecord`, and `SpanRecord` are fixed-shape instead: growing their variant
//! set (a new `MetricKind`, say) is the kind of change `docs/design/wire-protocol.md`'s connection
//! handshake negotiates a protocol version for, not something a single reader silently tolerates.

use bytes::{Buf, Bytes, BytesMut};
use logit_core::{
    BodyFormat, DdSketch, Event, HyperLogLog, LogRecord, MetricKind, MetricList, MetricRecord,
    Severity, SpanEvent, SpanKind, SpanLink, SpanRecord, SpanStatus, TraceRef,
};

use crate::native::dict::{Dict, DictBuilder};
use crate::native::value::{read_attr_map, read_value, write_attr_map, write_value};
use crate::native::varint::{read_ivarint, read_u8, read_uvarint, write_ivarint, write_uvarint};
use crate::CodecError;

// -- MetricRecord / MetricKind ---------------------------------------------------------------

const METRIC_COUNTER: u8 = 0;
const METRIC_GAUGE: u8 = 1;
const METRIC_GAUGE_DELTA: u8 = 2;
const METRIC_SET: u8 = 3;
const METRIC_DISTRIBUTION: u8 = 4;
const METRIC_HISTOGRAM: u8 = 5;
const METRIC_SUMMARY: u8 = 6;

pub fn write_metric_record(out: &mut BytesMut, dict: &mut DictBuilder, record: &MetricRecord) {
    write_uvarint(out, dict.intern(record.name) as u64);
    match record.unit {
        Some(unit) => {
            out.extend_from_slice(&[1]);
            write_uvarint(out, dict.intern(unit) as u64);
        }
        None => out.extend_from_slice(&[0]),
    }
    match &record.kind {
        MetricKind::Counter(v) => write_f64_kind(out, METRIC_COUNTER, *v),
        MetricKind::Gauge(v) => write_f64_kind(out, METRIC_GAUGE, *v),
        MetricKind::GaugeDelta(v) => write_f64_kind(out, METRIC_GAUGE_DELTA, *v),
        // `HyperLogLog` is still a stub with no cardinality to serialize
        // (`crates/logit-core/src/metric.rs`) -- the tag alone is enough to preserve the metric's
        // *identity* (name/unit/timestamp survive), unlike OTLP's encoder, which drops the whole
        // record (`docs/known-gaps.md`'s cross-protocol-semantic-gaps entry). Once `HyperLogLog`
        // carries real state, its bytes go here the same way `Distribution`'s blob does.
        MetricKind::Set(_) => out.extend_from_slice(&[METRIC_SET]),
        MetricKind::Distribution(sketch) => {
            let blob = sketch.to_java_bytes();
            out.extend_from_slice(&[METRIC_DISTRIBUTION]);
            write_uvarint(out, blob.len() as u64);
            out.extend_from_slice(&blob);
        }
        MetricKind::Histogram { buckets } => {
            out.extend_from_slice(&[METRIC_HISTOGRAM]);
            write_uvarint(out, buckets.len() as u64);
            for (bound, count) in buckets {
                out.extend_from_slice(&bound.to_le_bytes());
                write_uvarint(out, *count);
            }
        }
        MetricKind::Summary { quantiles } => {
            out.extend_from_slice(&[METRIC_SUMMARY]);
            write_uvarint(out, quantiles.len() as u64);
            for (q, v) in quantiles {
                out.extend_from_slice(&q.to_le_bytes());
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
    }
}

fn write_f64_kind(out: &mut BytesMut, tag: u8, v: f64) {
    out.extend_from_slice(&[tag]);
    out.extend_from_slice(&v.to_le_bytes());
}

fn read_f64(bytes: &mut Bytes) -> Result<f64, CodecError> {
    if bytes.len() < 8 {
        return Err(CodecError::Malformed("expected 8 bytes for an f64".to_string()));
    }
    let mut buf = [0u8; 8];
    bytes.copy_to_slice(&mut buf);
    Ok(f64::from_le_bytes(buf))
}

pub fn read_metric_record(bytes: &mut Bytes, dict: &Dict) -> Result<MetricRecord, CodecError> {
    let name = dict.get(read_uvarint(bytes)? as u32)?;
    let unit = match read_u8(bytes)? {
        0 => None,
        1 => Some(dict.get(read_uvarint(bytes)? as u32)?),
        other => return Err(CodecError::Malformed(format!("bad unit presence byte {other}"))),
    };
    let kind_tag = read_u8(bytes)?;
    let kind = match kind_tag {
        METRIC_COUNTER => MetricKind::Counter(read_f64(bytes)?),
        METRIC_GAUGE => MetricKind::Gauge(read_f64(bytes)?),
        METRIC_GAUGE_DELTA => MetricKind::GaugeDelta(read_f64(bytes)?),
        METRIC_SET => MetricKind::Set(HyperLogLog::default()),
        METRIC_DISTRIBUTION => {
            let len = read_uvarint(bytes)? as usize;
            if bytes.len() < len {
                return Err(CodecError::Malformed("distribution blob truncated".to_string()));
            }
            let blob = bytes.split_to(len);
            let sketch = DdSketch::from_java_bytes(&blob)
                .map_err(|e| CodecError::Malformed(format!("bad distribution blob: {e:?}")))?;
            MetricKind::Distribution(sketch)
        }
        METRIC_HISTOGRAM => {
            let count = read_uvarint(bytes)? as usize;
            let mut buckets = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                let bound = read_f64(bytes)?;
                let n = read_uvarint(bytes)?;
                buckets.push((bound, n));
            }
            MetricKind::Histogram { buckets }
        }
        METRIC_SUMMARY => {
            let count = read_uvarint(bytes)? as usize;
            let mut quantiles = Vec::with_capacity(count.min(4096));
            for _ in 0..count {
                let q = read_f64(bytes)?;
                let v = read_f64(bytes)?;
                quantiles.push((q, v));
            }
            MetricKind::Summary { quantiles }
        }
        other => {
            return Err(CodecError::Malformed(format!(
                "unknown metric kind tag {other} -- a new MetricKind variant needs a protocol \
                 version bump, not silent skipping (see this module's own doc comment)"
            )))
        }
    };
    Ok(MetricRecord { name, kind, unit })
}

// -- LogRecord --------------------------------------------------------------------------------

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

fn severity_from_tag(tag: u8) -> Result<Severity, CodecError> {
    match tag {
        0 => Ok(Severity::Trace),
        1 => Ok(Severity::Debug),
        2 => Ok(Severity::Info),
        3 => Ok(Severity::Warn),
        4 => Ok(Severity::Error),
        5 => Ok(Severity::Fatal),
        other => Err(CodecError::Malformed(format!("unknown severity tag {other}"))),
    }
}

fn body_format_tag(f: BodyFormat) -> u8 {
    match f {
        BodyFormat::Raw => 0,
        BodyFormat::Json => 1,
        BodyFormat::Structured => 2,
    }
}

fn body_format_from_tag(tag: u8) -> Result<BodyFormat, CodecError> {
    match tag {
        0 => Ok(BodyFormat::Raw),
        1 => Ok(BodyFormat::Json),
        2 => Ok(BodyFormat::Structured),
        other => Err(CodecError::Malformed(format!("unknown body_format tag {other}"))),
    }
}

fn write_trace_ref(out: &mut BytesMut, trace: &TraceRef) {
    out.extend_from_slice(&trace.trace_id);
    match trace.span_id {
        Some(span_id) => {
            out.extend_from_slice(&[1]);
            out.extend_from_slice(&span_id);
        }
        None => out.extend_from_slice(&[0]),
    }
    out.extend_from_slice(&[trace.flags]);
}

fn read_trace_ref(bytes: &mut Bytes) -> Result<TraceRef, CodecError> {
    if bytes.len() < 16 {
        return Err(CodecError::Malformed("truncated trace_id".to_string()));
    }
    let mut trace_id = [0u8; 16];
    bytes.copy_to_slice(&mut trace_id);
    let has_span = read_u8(bytes)?;
    let span_id = match has_span {
        0 => None,
        1 => {
            if bytes.len() < 8 {
                return Err(CodecError::Malformed("truncated span_id".to_string()));
            }
            let mut span_id = [0u8; 8];
            bytes.copy_to_slice(&mut span_id);
            Some(span_id)
        }
        other => return Err(CodecError::Malformed(format!("bad span_id presence byte {other}"))),
    };
    let flags = read_u8(bytes)?;
    Ok(TraceRef { trace_id, span_id, flags })
}

pub fn write_log_record(out: &mut BytesMut, dict: &mut DictBuilder, log: &LogRecord) {
    write_value(out, dict, &log.message);
    match log.severity {
        Some(s) => {
            out.extend_from_slice(&[1, severity_tag(s)]);
        }
        None => out.extend_from_slice(&[0]),
    }
    out.extend_from_slice(&[body_format_tag(log.body_format)]);
    match &log.trace {
        Some(trace) => {
            out.extend_from_slice(&[1]);
            write_trace_ref(out, trace);
        }
        None => out.extend_from_slice(&[0]),
    }
}

pub fn read_log_record(bytes: &mut Bytes, dict: &Dict) -> Result<LogRecord, CodecError> {
    let message = read_value(bytes, dict)?;
    let severity = match read_u8(bytes)? {
        0 => None,
        1 => Some(severity_from_tag(read_u8(bytes)?)?),
        other => return Err(CodecError::Malformed(format!("bad severity presence byte {other}"))),
    };
    let body_format = body_format_from_tag(read_u8(bytes)?)?;
    let trace = match read_u8(bytes)? {
        0 => None,
        1 => Some(read_trace_ref(bytes)?),
        other => return Err(CodecError::Malformed(format!("bad trace presence byte {other}"))),
    };
    Ok(LogRecord { message, severity, body_format, trace })
}

// -- SpanRecord ---------------------------------------------------------------------------------

fn span_kind_tag(k: SpanKind) -> u8 {
    match k {
        SpanKind::Internal => 0,
        SpanKind::Server => 1,
        SpanKind::Client => 2,
        SpanKind::Producer => 3,
        SpanKind::Consumer => 4,
    }
}

fn span_kind_from_tag(tag: u8) -> Result<SpanKind, CodecError> {
    match tag {
        0 => Ok(SpanKind::Internal),
        1 => Ok(SpanKind::Server),
        2 => Ok(SpanKind::Client),
        3 => Ok(SpanKind::Producer),
        4 => Ok(SpanKind::Consumer),
        other => Err(CodecError::Malformed(format!("unknown span kind tag {other}"))),
    }
}

fn span_status_tag(s: SpanStatus) -> u8 {
    match s {
        SpanStatus::Unset => 0,
        SpanStatus::Ok => 1,
        SpanStatus::Error => 2,
    }
}

fn span_status_from_tag(tag: u8) -> Result<SpanStatus, CodecError> {
    match tag {
        0 => Ok(SpanStatus::Unset),
        1 => Ok(SpanStatus::Ok),
        2 => Ok(SpanStatus::Error),
        other => Err(CodecError::Malformed(format!("unknown span status tag {other}"))),
    }
}

fn write_span_event(out: &mut BytesMut, dict: &mut DictBuilder, event: &SpanEvent) {
    write_ivarint(out, event.timestamp);
    write_value(out, dict, &event.name);
    write_attr_map(out, dict, &event.attributes);
}

fn read_span_event(bytes: &mut Bytes, dict: &Dict) -> Result<SpanEvent, CodecError> {
    let timestamp = read_ivarint(bytes)?;
    let name = read_value(bytes, dict)?;
    let attributes = read_attr_map(bytes, dict)?;
    Ok(SpanEvent { timestamp, name, attributes })
}

fn write_span_link(out: &mut BytesMut, dict: &mut DictBuilder, link: &SpanLink) {
    out.extend_from_slice(&link.trace_id);
    out.extend_from_slice(&link.span_id);
    write_attr_map(out, dict, &link.attributes);
}

fn read_span_link(bytes: &mut Bytes, dict: &Dict) -> Result<SpanLink, CodecError> {
    if bytes.len() < 24 {
        return Err(CodecError::Malformed("truncated span link ids".to_string()));
    }
    let mut trace_id = [0u8; 16];
    bytes.copy_to_slice(&mut trace_id);
    let mut span_id = [0u8; 8];
    bytes.copy_to_slice(&mut span_id);
    let attributes = read_attr_map(bytes, dict)?;
    Ok(SpanLink { trace_id, span_id, attributes })
}

pub fn write_span_record(out: &mut BytesMut, dict: &mut DictBuilder, span: &SpanRecord) {
    out.extend_from_slice(&span.trace_id);
    out.extend_from_slice(&span.span_id);
    match span.parent_span_id {
        Some(id) => {
            out.extend_from_slice(&[1]);
            out.extend_from_slice(&id);
        }
        None => out.extend_from_slice(&[0]),
    }
    write_value(out, dict, &span.name);
    out.extend_from_slice(&[span_kind_tag(span.kind), span_status_tag(span.status)]);
    write_uvarint(out, span.events.len() as u64);
    for e in &span.events {
        write_span_event(out, dict, e);
    }
    write_uvarint(out, span.links.len() as u64);
    for l in &span.links {
        write_span_link(out, dict, l);
    }
    write_ivarint(out, span.end_timestamp);
}

pub fn read_span_record(bytes: &mut Bytes, dict: &Dict) -> Result<SpanRecord, CodecError> {
    if bytes.len() < 24 {
        return Err(CodecError::Malformed("truncated span record ids".to_string()));
    }
    let mut trace_id = [0u8; 16];
    bytes.copy_to_slice(&mut trace_id);
    let mut span_id = [0u8; 8];
    bytes.copy_to_slice(&mut span_id);
    let parent_span_id = match read_u8(bytes)? {
        0 => None,
        1 => {
            if bytes.len() < 8 {
                return Err(CodecError::Malformed("truncated parent_span_id".to_string()));
            }
            let mut id = [0u8; 8];
            bytes.copy_to_slice(&mut id);
            Some(id)
        }
        other => return Err(CodecError::Malformed(format!("bad parent presence byte {other}"))),
    };
    let name = read_value(bytes, dict)?;
    let kind = span_kind_from_tag(read_u8(bytes)?)?;
    let status = span_status_from_tag(read_u8(bytes)?)?;
    let event_count = read_uvarint(bytes)? as usize;
    let mut events = Vec::with_capacity(event_count.min(4096));
    for _ in 0..event_count {
        events.push(read_span_event(bytes, dict)?);
    }
    let link_count = read_uvarint(bytes)? as usize;
    let mut links = Vec::with_capacity(link_count.min(4096));
    for _ in 0..link_count {
        links.push(read_span_link(bytes, dict)?);
    }
    let end_timestamp = read_ivarint(bytes)?;
    Ok(SpanRecord {
        trace_id,
        span_id,
        parent_span_id,
        name,
        kind,
        status,
        events,
        links,
        end_timestamp,
    })
}

// -- Event: TLV fields, the one extensible record ------------------------------------------------

const FIELD_TIMESTAMP: u8 = 1;
const FIELD_ATTRIBUTES: u8 = 2;
const FIELD_LOG: u8 = 3;
const FIELD_METRICS: u8 = 4;
const FIELD_SPAN: u8 = 5;

fn write_field(out: &mut BytesMut, tag: u8, build: impl FnOnce(&mut BytesMut)) {
    let mut tmp = BytesMut::new();
    build(&mut tmp);
    out.extend_from_slice(&[tag]);
    write_uvarint(out, tmp.len() as u64);
    out.extend_from_slice(&tmp);
}

pub fn write_event(dict: &mut DictBuilder, event: &Event) -> BytesMut {
    let mut out = BytesMut::new();
    write_field(&mut out, FIELD_TIMESTAMP, |buf| write_ivarint(buf, event.timestamp));
    if !event.attributes.is_empty() {
        write_field(&mut out, FIELD_ATTRIBUTES, |buf| write_attr_map(buf, dict, &event.attributes));
    }
    if let Some(log) = &event.log {
        write_field(&mut out, FIELD_LOG, |buf| write_log_record(buf, dict, log));
    }
    if !event.metrics.is_empty() {
        write_field(&mut out, FIELD_METRICS, |buf| {
            write_uvarint(buf, event.metrics.len() as u64);
            for m in &event.metrics {
                write_metric_record(buf, dict, m);
            }
        });
    }
    if let Some(span) = &event.span {
        write_field(&mut out, FIELD_SPAN, |buf| write_span_record(buf, dict, span));
    }
    out
}

/// Decodes one event's TLV field stream. A field tag this reader doesn't recognize (a future
/// addition to `Event`) is skipped whole -- `len` bytes consumed via `split_to`, nothing parsed --
/// which is what lets an older reader keep decoding a newer writer's batches, per
/// `docs/design/wire-protocol.md`'s version-skew requirement.
pub fn read_event(body: &mut Bytes, dict: &Dict) -> Result<Event, CodecError> {
    let mut timestamp = 0i64;
    let mut attributes = logit_core::AttrMap::new();
    let mut log = None;
    let mut metrics = MetricList::new();
    let mut span = None;

    while !body.is_empty() {
        let tag = read_u8(body)?;
        let len = read_uvarint(body)? as usize;
        if body.len() < len {
            return Err(CodecError::Malformed(format!(
                "event field {tag} declares {len} bytes but only {} remain",
                body.len()
            )));
        }
        let mut field = body.split_to(len);
        match tag {
            FIELD_TIMESTAMP => timestamp = read_ivarint(&mut field)?,
            FIELD_ATTRIBUTES => attributes = read_attr_map(&mut field, dict)?,
            FIELD_LOG => log = Some(read_log_record(&mut field, dict)?),
            FIELD_METRICS => {
                let count = read_uvarint(&mut field)? as usize;
                for _ in 0..count {
                    metrics.push(read_metric_record(&mut field, dict)?);
                }
            }
            FIELD_SPAN => span = Some(read_span_record(&mut field, dict)?),
            _unknown => { /* forward compatibility -- see this function's own doc comment */ }
        }
    }
    Ok(Event { timestamp, attributes, log, metrics, span })
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, DdSketch, Value};

    fn dict_round_trip_metric(record: &MetricRecord) -> MetricRecord {
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_metric_record(&mut buf, &mut builder, record);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_metric_record(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty());
        out
    }

    #[test]
    fn round_trips_every_metric_kind() {
        let name = logit_core::interner::intern("record_test_metric");
        let mut sketch = DdSketch::new();
        sketch.add(1.0);
        sketch.add(2.5);

        let kinds = vec![
            MetricKind::Counter(3.0),
            MetricKind::Gauge(-1.5),
            MetricKind::GaugeDelta(0.5),
            MetricKind::Set(HyperLogLog::default()),
            MetricKind::Distribution(sketch.clone()),
            MetricKind::Histogram { buckets: vec![(1.0, 2), (2.0, 5)] },
            MetricKind::Summary { quantiles: vec![(0.5, 10.0), (0.99, 42.0)] },
        ];
        for kind in kinds {
            let record = MetricRecord { name, kind, unit: None };
            let out = dict_round_trip_metric(&record);
            assert_eq!(out.name, record.name);
            assert_eq!(out.unit, record.unit);
            match (&record.kind, &out.kind) {
                (MetricKind::Distribution(a), MetricKind::Distribution(b)) => {
                    // DDSketch has no PartialEq -- compare count and a quantile instead, the same
                    // fidelity check the bake-off's fidelity gate uses.
                    assert_eq!(a.count(), b.count());
                    assert_eq!(a.quantile(0.5), b.quantile(0.5));
                }
                (a, b) => assert_eq!(format!("{a:?}"), format!("{b:?}")),
            }
        }
    }

    #[test]
    fn round_trips_a_log_record_with_trace_context() {
        let log = LogRecord {
            message: Value::str("hello"),
            severity: Some(Severity::Warn),
            body_format: BodyFormat::Json,
            trace: Some(TraceRef { trace_id: [1; 16], span_id: Some([2; 8]), flags: 1 }),
        };
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_log_record(&mut buf, &mut builder, &log);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_log_record(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty());
        assert_eq!(out.message, log.message);
        assert_eq!(out.severity, log.severity);
        assert_eq!(out.body_format, log.body_format);
        assert_eq!(out.trace, log.trace);
    }

    #[test]
    fn round_trips_a_log_record_with_no_trace() {
        let log = LogRecord {
            message: Value::str("no trace here"),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
        };
        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_log_record(&mut buf, &mut builder, &log);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_log_record(&mut bytes, &dict).unwrap();
        assert_eq!(out.severity, None);
        assert_eq!(out.trace, None);
    }

    #[test]
    fn round_trips_a_span_record_with_events_and_links() {
        let mut event_attrs = AttrMap::new();
        event_attrs.insert("k", "v");
        let mut link_attrs = AttrMap::new();
        link_attrs.insert("reason", "test");

        let span = SpanRecord {
            trace_id: [9; 16],
            span_id: [8; 8],
            parent_span_id: Some([7; 8]),
            name: Value::str("span name"),
            kind: SpanKind::Server,
            status: SpanStatus::Error,
            events: vec![SpanEvent {
                timestamp: 100,
                name: Value::str("checkpoint"),
                attributes: event_attrs,
            }],
            links: vec![SpanLink { trace_id: [6; 16], span_id: [5; 8], attributes: link_attrs }],
            end_timestamp: 200,
        };

        let mut builder = DictBuilder::default();
        let mut buf = BytesMut::new();
        write_span_record(&mut buf, &mut builder, &span);
        let mut dict_bytes = BytesMut::new();
        builder.write(&mut dict_bytes);
        let dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let mut bytes = buf.freeze();
        let out = read_span_record(&mut bytes, &dict).unwrap();
        assert!(bytes.is_empty());

        assert_eq!(out.trace_id, span.trace_id);
        assert_eq!(out.span_id, span.span_id);
        assert_eq!(out.parent_span_id, span.parent_span_id);
        assert_eq!(out.name, span.name);
        assert_eq!(out.kind, span.kind);
        assert_eq!(out.status, span.status);
        assert_eq!(out.end_timestamp, span.end_timestamp);
        assert_eq!(out.events.len(), 1);
        assert_eq!(out.events[0].name, Value::str("checkpoint"));
        assert_eq!(out.links.len(), 1);
        assert_eq!(out.links[0].trace_id, [6; 16]);
    }

    #[test]
    fn round_trips_an_event_carrying_log_metrics_and_span_at_once() {
        let mut attrs = AttrMap::new();
        attrs.insert("service", "orders-api");

        let mut event = Event::empty(1_700_000_000_000_000_000, attrs);
        event.log = Some(LogRecord {
            message: Value::str("multi-payload event"),
            severity: Some(Severity::Info),
            body_format: BodyFormat::Raw,
            trace: None,
        });
        event.metrics.push(MetricRecord {
            name: logit_core::interner::intern("record_test_multi_metric"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        });
        event.span = Some(SpanRecord {
            trace_id: [4; 16],
            span_id: [3; 8],
            parent_span_id: None,
            name: Value::str("multi span"),
            kind: SpanKind::Internal,
            status: SpanStatus::Ok,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 1_700_000_000_100_000_000,
        });

        let mut dict = DictBuilder::default();
        let body = write_event(&mut dict, &event);
        let mut dict_bytes = BytesMut::new();
        dict.write(&mut dict_bytes);
        let decoded_dict = Dict::read(&mut dict_bytes.freeze()).unwrap();

        let mut body_bytes = body.freeze();
        let out = read_event(&mut body_bytes, &decoded_dict).unwrap();
        assert!(body_bytes.is_empty());

        assert_eq!(out.timestamp, event.timestamp);
        assert_eq!(out.attributes, event.attributes);
        assert!(out.log.is_some(), "log must survive alongside metrics and span");
        assert_eq!(out.metrics.len(), 1);
        assert!(out.span.is_some(), "span must survive alongside log and metrics");
    }

    #[test]
    fn an_event_with_no_payload_at_all_round_trips_as_empty() {
        let event = Event::empty(42, AttrMap::new());
        let mut dict = DictBuilder::default();
        let body = write_event(&mut dict, &event);
        let mut dict_bytes = BytesMut::new();
        dict.write(&mut dict_bytes);
        let decoded_dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let out = read_event(&mut body.freeze(), &decoded_dict).unwrap();
        assert_eq!(out.timestamp, 42);
        assert!(out.attributes.is_empty());
        assert!(out.log.is_none());
        assert!(out.metrics.is_empty());
        assert!(out.span.is_none());
    }

    /// The version-skew gate at the record level: an unrecognized `Event` field tag (a
    /// hypothetical future field this reader predates) must be skipped whole, leaving every
    /// known field around it intact.
    #[test]
    fn an_unrecognized_event_field_tag_is_skipped_without_disturbing_known_fields() {
        let mut attrs = AttrMap::new();
        attrs.insert("k", "v");
        let event = Event::empty(7, attrs);

        let mut dict = DictBuilder::default();
        let mut body = write_event(&mut dict, &event);
        // Splice in a field this reader doesn't know (tag 200) with a plausible payload, as if a
        // newer writer had added a field.
        write_field(&mut body, 200, |buf| buf.extend_from_slice(b"future field payload"));

        let mut dict_bytes = BytesMut::new();
        dict.write(&mut dict_bytes);
        let decoded_dict = Dict::read(&mut dict_bytes.freeze()).unwrap();
        let out = read_event(&mut body.freeze(), &decoded_dict).unwrap();

        assert_eq!(out.timestamp, 7);
        assert_eq!(out.attributes.get("k").and_then(|v| v.as_str()), Some("v"));
    }
}
