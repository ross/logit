//! `kafka_out`'s record encoder: one [`MessageBuf`] entry per record, its bytes the record's value
//! and its [`RecordMeta`] the key and timestamp (`docs/adr/kafka-output.md`, item 11).
//!
//! `format: json` writes each event as `crate::ndjson`'s object, without the newline; that
//! module's doc is the grammar.
//!
//! A key is text, as UTF-8 bytes (`docs/adr/kafka-output.md`, item 3), the same canonical bytes
//! `sample` hashes for a key (`logit_core::sampling::hash_value`):
//!
//! | Value at the key | Key bytes |
//! |---|---|
//! | `trace_id`: the span's, else the log's `TraceRef` | 32 lowercase hex characters; an all-zero id is no value |
//! | `Str`, `Bytes` | as they are |
//! | `I64`, `U64` | decimal text |
//! | `F64` | Rust `Display`: the shortest decimal text that round-trips, never an exponent |
//! | `Bool` | `true` or `false` |
//! | `Timestamp` | decimal nanoseconds |
//! | `Null`, `Array`, `Map`, or no value | no key: the record goes unkeyed, counted in [`EncodeStats::missing_key`] |
//!
//! A record's timestamp is the event's, in milliseconds since the Unix epoch, rounded down.

use std::fmt::Write;

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::trace::push_hex;
use logit_core::{Event, EventBatch, Resource, Symbol, Value};
use logit_proto::{FramedEncoder, MessageBuf};

/// What each record's key is. Mirrors `logit_config::KafkaKey`, which `logit-cli` converts.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum RecordKey {
    #[default]
    None,
    TraceId,
    /// A top-level event attribute, named literally.
    Attribute(String),
    /// A resource attribute.
    Resource(String),
}

/// What each record's value holds. Mirrors `logit_config::KafkaFormat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RecordFormat {
    #[default]
    Json,
}

/// [`RecordKey`] with its attribute name interned once.
#[derive(Debug, Clone, Copy)]
enum Compiled {
    None,
    TraceId,
    Attribute(Symbol),
    Resource(Symbol),
}

/// A record's key and timestamp; its value is the message's bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RecordMeta {
    pub(crate) key: Option<Bytes>,
    /// `CreateTime`, in milliseconds since the Unix epoch.
    pub(crate) timestamp: i64,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct EncodeStats {
    /// Events sent unkeyed because a configured key had no value on them.
    pub(crate) missing_key: usize,
}

pub(crate) struct KafkaEncoder {
    key: Compiled,
    format: RecordFormat,
    /// Reused across events and batches.
    scratch: String,
}

impl KafkaEncoder {
    pub(crate) fn new(key: &RecordKey, format: RecordFormat) -> Self {
        let key = match key {
            RecordKey::None => Compiled::None,
            RecordKey::TraceId => Compiled::TraceId,
            RecordKey::Attribute(name) => Compiled::Attribute(intern(name)),
            RecordKey::Resource(name) => Compiled::Resource(intern(name)),
        };
        Self { key, format, scratch: String::new() }
    }

    /// The key bytes for `event`; `Err(())` when a configured key has no value on it.
    fn key(&mut self, resource: &Resource, event: &Event) -> Result<Option<Bytes>, ()> {
        let value = match self.key {
            Compiled::None => return Ok(None),
            Compiled::TraceId => {
                let id = event
                    .span
                    .as_ref()
                    .map(|span| &span.trace_id)
                    .or_else(|| event.log.as_ref()?.trace.as_ref().map(|t| &t.trace_id))
                    .filter(|id| id.iter().any(|&b| b != 0))
                    .ok_or(())?;
                self.scratch.clear();
                push_hex(&mut self.scratch, id);
                return Ok(Some(Bytes::copy_from_slice(self.scratch.as_bytes())));
            }
            Compiled::Attribute(sym) => event.attributes.get_sym(sym),
            Compiled::Resource(sym) => resource.attributes.get_sym(sym),
        };
        key_bytes(value.ok_or(())?, &mut self.scratch).map(Some)
    }
}

/// A value's key bytes, by the module doc's table; `Err(())` for one that can't be a key.
fn key_bytes(value: &Value, scratch: &mut String) -> Result<Bytes, ()> {
    scratch.clear();
    match value {
        Value::Str(b) | Value::Bytes(b) => return Ok(b.clone()),
        Value::I64(n) => write!(scratch, "{n}"),
        Value::U64(n) => write!(scratch, "{n}"),
        Value::F64(n) => write!(scratch, "{n}"),
        Value::Bool(b) => write!(scratch, "{b}"),
        Value::Timestamp(ns) => write!(scratch, "{ns}"),
        Value::Null | Value::Array(_) | Value::Map(_) => return Err(()),
    }
    .expect("writing to a String never fails");
    Ok(Bytes::copy_from_slice(scratch.as_bytes()))
}

impl FramedEncoder for KafkaEncoder {
    type Meta = RecordMeta;
    type Stats = EncodeStats;

    fn encode_into(&mut self, batch: &EventBatch, out: &mut MessageBuf<RecordMeta>) -> EncodeStats {
        out.clear();
        let mut stats = EncodeStats::default();
        let mut value = String::new();
        for event in &batch.events {
            let key = self.key(&batch.resource, event).unwrap_or_else(|()| {
                stats.missing_key += 1;
                None
            });
            value.clear();
            match self.format {
                RecordFormat::Json => crate::ndjson::render_event(
                    &mut value,
                    &batch.resource,
                    batch.scope.as_deref(),
                    event,
                ),
            }
            let timestamp = event.timestamp.div_euclid(1_000_000);
            out.push_with(value.as_bytes(), RecordMeta { key, timestamp });
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{AttrMap, LogRecord, TraceRef};

    fn batch(resource: AttrMap, events: Vec<Event>) -> EventBatch {
        let resource = Resource { attributes: resource, ..Resource::default() };
        EventBatch { resource: std::sync::Arc::new(resource), scope: None, events }
    }

    fn event(attributes: AttrMap) -> Event {
        Event::empty(1_700_000_000_123_456_789, attributes)
    }

    fn encode(key: RecordKey, batch: &EventBatch) -> (Vec<(String, RecordMeta)>, EncodeStats) {
        let mut encoder = KafkaEncoder::new(&key, RecordFormat::Json);
        let mut out = MessageBuf::default();
        let stats = encoder.encode_into(batch, &mut out);
        let records = out
            .iter_with()
            .map(|(v, m)| (String::from_utf8(v.to_vec()).unwrap(), m.clone()))
            .collect();
        (records, stats)
    }

    fn key_of(key: RecordKey, value: Value) -> Option<Bytes> {
        let mut attrs = AttrMap::new();
        attrs.insert("k", value);
        let (records, _) = encode(key, &batch(AttrMap::new(), vec![event(attrs)]));
        records[0].1.key.clone()
    }

    #[test]
    fn a_value_is_the_ndjson_object_without_its_newline() {
        let mut attrs = AttrMap::new();
        attrs.insert("a", Value::I64(1));
        let b = batch(AttrMap::new(), vec![event(attrs)]);
        let (records, stats) = encode(RecordKey::None, &b);
        let mut line = String::new();
        crate::ndjson::render_ndjson(&mut line, &b);
        assert_eq!(format!("{}\n", records[0].0), line);
        assert_eq!(records[0].1, RecordMeta { key: None, timestamp: 1_700_000_000_123 });
        assert_eq!(stats.missing_key, 0, "no key configured is not a missing key");
    }

    #[test]
    fn a_timestamp_before_the_epoch_rounds_down() {
        let b = batch(AttrMap::new(), vec![Event::empty(-1, AttrMap::new())]);
        assert_eq!(encode(RecordKey::None, &b).0[0].1.timestamp, -1);
    }

    #[test]
    fn each_value_type_becomes_its_key_text() {
        let attr = || RecordKey::Attribute("k".into());
        let cases = [
            (Value::str("abc"), Some("abc")),
            (Value::Bytes(Bytes::from_static(b"\xff\x00")), Some("\u{0}")),
            (Value::I64(-7), Some("-7")),
            (Value::U64(200), Some("200")),
            (Value::F64(200.0), Some("200")),
            (Value::F64(0.1), Some("0.1")),
            (Value::Bool(true), Some("true")),
            (Value::Timestamp(42), Some("42")),
            (Value::Null, None),
            (Value::Array(vec![Value::I64(1)]), None),
            (Value::Map(Box::default()), None),
        ];
        for (value, want) in cases {
            let got = key_of(attr(), value.clone());
            match (value, want) {
                (Value::Bytes(b), _) => assert_eq!(got, Some(b)),
                (_, want) => assert_eq!(got.as_deref(), want.map(str::as_bytes), "{want:?}"),
            }
        }
    }

    #[test]
    fn a_missing_key_is_counted_and_the_record_goes_unkeyed() {
        let b = batch(AttrMap::new(), vec![event(AttrMap::new()), event(AttrMap::new())]);
        let (records, stats) = encode(RecordKey::Attribute("absent".into()), &b);
        assert_eq!(stats.missing_key, 2);
        assert!(records.iter().all(|(_, m)| m.key.is_none()));
    }

    #[test]
    fn a_resource_key_reads_the_batch_resource() {
        let mut resource = AttrMap::new();
        resource.insert("service.name", Value::str("api"));
        let b = batch(resource, vec![event(AttrMap::new())]);
        let (records, _) = encode(RecordKey::Resource("service.name".into()), &b);
        assert_eq!(records[0].1.key.as_deref(), Some(&b"api"[..]));
    }

    #[test]
    fn a_trace_id_key_is_lowercase_hex_and_all_zero_is_missing() {
        let id = [0xab; 16];
        let mut e = event(AttrMap::new());
        e.log = Some(LogRecord {
            message: Value::str("m"),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: Some(TraceRef { trace_id: id, span_id: None, flags: 0 }),
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        let mut zero = e.clone();
        zero.log.as_mut().unwrap().trace.as_mut().unwrap().trace_id = [0; 16];
        let (records, stats) = encode(RecordKey::TraceId, &batch(AttrMap::new(), vec![e, zero]));
        assert_eq!(records[0].1.key.as_deref(), Some("ab".repeat(16).as_bytes()));
        assert_eq!(records[1].1.key, None);
        assert_eq!(stats.missing_key, 1);
    }
}
