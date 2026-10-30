//! A record's value into events, by `format:`, and the `_logit.kafka.*` carriers every event of
//! the record gets (`docs/adr/kafka-input.md`, items 11 and 12). The module doc of
//! [`super`] has the outcome table this decoder's [`Decoded`] variants feed.

use std::sync::Arc;

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, BodyFormat, Event, EventBatch, LogRecord, Resource, Symbol, Telemetry, Value,
};
use logit_proto::event_json::EventJsonDecoder;
use logit_proto::frame::read_frame;
use logit_proto::kafka::{Record, TimestampType};
use logit_proto::msgpack_event::MsgpackEventDecoder;
use logit_proto::native::{decode_batch, DecodeBudget, CODEC_BATCH};
use logit_proto::otlp::OtlpDecoder;
use logit_proto::{Signal, SignalDecoder};

/// What a record's value holds. Mirrors the `format:` values `kafka_in` reads.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KafkaInFormat {
    /// `logit`'s event JSON: one object, or NDJSON.
    Json,
    /// The same objects as MessagePack maps, one or several back to back.
    Msgpack,
    /// One log per value, or under `batch` one per escaped line.
    Text { batch: bool },
    /// One `CODEC_BATCH` frame.
    Native,
    /// One OTLP export request, protobuf or (`json`) OTLP/JSON, of the signal its topic is
    /// mapped to in `topics`.
    Otlp { json: bool, topics: Vec<(String, Signal)> },
}

/// One record's outcome.
#[derive(Debug)]
pub(crate) enum Decoded {
    /// The record's events, in one batch per resource and scope run, carriers stamped.
    Batches(Vec<EventBatch>),
    /// Nothing to carry: `tombstone` or `empty`.
    Skipped(&'static str),
    /// The value doesn't decode in the format; the text says why.
    Undecodable(String),
}

/// The carrier keys, interned once.
struct Keys {
    topic: Symbol,
    partition: Symbol,
    offset: Symbol,
    key: Symbol,
    headers: Symbol,
    timestamp: Symbol,
    timestamp_type: Symbol,
}

impl Keys {
    fn new() -> Self {
        Self {
            topic: intern("_logit.kafka.topic"),
            partition: intern("_logit.kafka.partition"),
            offset: intern("_logit.kafka.offset"),
            key: intern("_logit.kafka.key"),
            headers: intern("_logit.kafka.headers"),
            timestamp: intern("_logit.kafka.timestamp"),
            timestamp_type: intern("_logit.kafka.timestamp_type"),
        }
    }
}

/// Decodes records of one `format:`.
pub(crate) struct RecordDecoder {
    format: KafkaInFormat,
    json: EventJsonDecoder,
    msgpack: MsgpackEventDecoder,
    otlp: OtlpDecoder,
    /// `text`'s resource, one `Arc` for every record, so consecutive records' logs share a
    /// batch.
    empty: Arc<Resource>,
    keys: Keys,
}

/// A header name that isn't UTF-8, which the carrier map can't key: the caller counts it.
pub(crate) struct OmittedHeaders(pub usize);

impl RecordDecoder {
    pub(crate) fn new(format: KafkaInFormat) -> Self {
        Self {
            format,
            json: EventJsonDecoder::new(),
            msgpack: MsgpackEventDecoder::new(),
            otlp: OtlpDecoder::new(),
            empty: Arc::new(Resource::default()),
            keys: Keys::new(),
        }
    }

    /// Counts what the OTLP mapping skips (`logit.input.metrics.skipped`) on `telemetry`.
    pub(crate) fn set_telemetry(&mut self, telemetry: Telemetry) {
        self.otlp = OtlpDecoder::new().with_telemetry(telemetry);
    }

    /// Decodes `record` of `topic`/`partition`. `now` is the read time, a `text` log's
    /// timestamp when the record has none.
    pub(crate) fn decode(
        &mut self,
        topic: &str,
        partition: i32,
        record: &Record,
        now: i64,
    ) -> (Decoded, OmittedHeaders) {
        let Some(value) = &record.value else {
            return (Decoded::Skipped("tombstone"), OmittedHeaders(0));
        };
        let batches = match &self.format {
            KafkaInFormat::Json => match self.json.decode_batches(value) {
                Ok(batches) => batches,
                Err(e) => return (Decoded::Undecodable(e.to_string()), OmittedHeaders(0)),
            },
            KafkaInFormat::Msgpack => match self.msgpack.decode_batches(value) {
                Ok(batches) => batches,
                Err(e) => return (Decoded::Undecodable(e.to_string()), OmittedHeaders(0)),
            },
            KafkaInFormat::Otlp { json, topics } => {
                let Some(&(_, signal)) = topics.iter().find(|(t, _)| t == topic) else {
                    let why = format!("topic {topic:?} is mapped to no signal");
                    return (Decoded::Undecodable(why), OmittedHeaders(0));
                };
                let decoded = if *json {
                    self.otlp.decode_signal_json(signal, value.clone())
                } else {
                    self.otlp.decode_signal(signal, value.clone())
                };
                match decoded {
                    Ok(batches) => {
                        if *json && batches.iter().all(|b| b.events.is_empty()) {
                            if let Some(found) = other_otlp_json_signal(value, signal) {
                                let why = format!(
                                    "an OTLP/JSON {found} request on topic {topic:?}, mapped to {}",
                                    signal_name(signal),
                                );
                                return (Decoded::Undecodable(why), OmittedHeaders(0));
                            }
                        }
                        batches
                    }
                    Err(e) => return (Decoded::Undecodable(e.to_string()), OmittedHeaders(0)),
                }
            }
            KafkaInFormat::Native => match native(value) {
                Ok(batch) => vec![batch],
                Err(e) => return (Decoded::Undecodable(e), OmittedHeaders(0)),
            },
            &KafkaInFormat::Text { batch } => {
                let timestamp = match record.timestamp {
                    -1 => now,
                    ms => ms.saturating_mul(1_000_000),
                };
                let events = if batch {
                    logit_proto::kafka::text::unescape_lines(value)
                        .into_iter()
                        .map(|body| text_log(timestamp, Bytes::from(body)))
                        .collect()
                } else {
                    vec![text_log(timestamp, value.clone())]
                };
                vec![EventBatch { resource: self.empty.clone(), scope: None, events }]
            }
        };
        let mut batches: Vec<EventBatch> =
            batches.into_iter().filter(|b| !b.events.is_empty()).collect();
        if batches.is_empty() {
            return (Decoded::Skipped("empty"), OmittedHeaders(0));
        }
        let (carriers, omitted) = self.carriers(topic, partition, record);
        for batch in &mut batches {
            for event in &mut batch.events {
                for (key, value) in &carriers {
                    event.attributes.insert_sym(*key, value.clone());
                }
            }
        }
        (Decoded::Batches(batches), omitted)
    }

    /// The record's carriers, by the ADR's item 12 table.
    fn carriers(
        &self,
        topic: &str,
        partition: i32,
        record: &Record,
    ) -> (Vec<(Symbol, Value)>, OmittedHeaders) {
        let k = &self.keys;
        let mut out = vec![
            (k.topic, Value::Str(Bytes::copy_from_slice(topic.as_bytes()))),
            (k.partition, Value::I64(i64::from(partition))),
            (k.offset, Value::I64(record.offset)),
        ];
        if let Some(key) = &record.key {
            out.push((k.key, text_or_bytes(key.clone())));
        }
        let mut omitted = 0;
        if !record.headers.is_empty() {
            // First-appearance order, each name's values in wire order.
            let mut named: Vec<(&str, Vec<Value>)> = Vec::new();
            for header in &record.headers {
                let Some(name) = header.name_str() else {
                    omitted += 1;
                    continue;
                };
                let value = header.value.clone().map_or(Value::Null, text_or_bytes);
                match named.iter_mut().find(|(n, _)| *n == name) {
                    Some((_, values)) => values.push(value),
                    None => named.push((name, vec![value])),
                }
            }
            if !named.is_empty() {
                let mut map = AttrMap::new();
                for (name, mut values) in named {
                    let value = match values.len() {
                        1 => values.pop().expect("one value"),
                        _ => Value::Array(values),
                    };
                    map.insert(name, value);
                }
                out.push((k.headers, Value::Map(Box::new(map))));
            }
        }
        if record.timestamp != -1 {
            out.push((k.timestamp, Value::Timestamp(record.timestamp.saturating_mul(1_000_000))));
        }
        let kind = match record.timestamp_type {
            TimestampType::CreateTime => "create_time",
            TimestampType::LogAppendTime => "log_append_time",
        };
        out.push((k.timestamp_type, Value::static_str(kind)));
        (out, OmittedHeaders(omitted))
    }
}

/// `Str` when `bytes` is UTF-8, else `Bytes`.
fn text_or_bytes(bytes: Bytes) -> Value {
    if std::str::from_utf8(&bytes).is_ok() {
        Value::Str(bytes)
    } else {
        Value::Bytes(bytes)
    }
}

/// A `text` log: the body as its message, no severity, the `raw` body format.
fn text_log(timestamp: i64, body: Bytes) -> Event {
    Event::log(
        timestamp,
        AttrMap::new(),
        LogRecord {
            message: text_or_bytes(body),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// One `CODEC_BATCH` frame filling the whole value, under the default per-frame decode budget.
fn native(value: &Bytes) -> Result<EventBatch, String> {
    let mut bytes = value.clone();
    let (codec, mut payload) = read_frame(&mut bytes).map_err(|e| e.to_string())?;
    if codec != CODEC_BATCH {
        return Err(format!("frame codec byte {codec}, not a native batch ({CODEC_BATCH})"));
    }
    if !bytes.is_empty() {
        return Err(format!("{} bytes after the frame", bytes.len()));
    }
    decode_batch(&mut payload, &DecodeBudget::default()).map_err(|e| e.to_string())
}

/// Decodes `bytes`, one partition's record batches as a Fetch answer carries them, as `kafka_in`
/// reads them: `logit_proto::kafka::decode`, each batch's records decompressed to at most
/// `max_batch_bytes`, then each record through `format:`'s decoder with its carriers stamped.
/// Every event of `topic`'s `partition` comes back in offset order. A batch that doesn't decode,
/// a record the input would skip or drop, and a header name the input would omit each fail,
/// naming the offset: the transit spec reads a sink's own bytes through this, where any of them
/// is a finding.
pub fn decode_record_batches(
    format: KafkaInFormat,
    topic: &str,
    partition: i32,
    bytes: &Bytes,
    max_batch_bytes: usize,
    now: i64,
    telemetry: Telemetry,
) -> anyhow::Result<Vec<EventBatch>> {
    let mut decoder = RecordDecoder::new(format);
    decoder.set_telemetry(telemetry);
    let decoded = logit_proto::kafka::decode(bytes, max_batch_bytes);
    anyhow::ensure!(
        decoded.partial_base.is_none(),
        "the bytes end inside the record batch at offset {}",
        decoded.partial_base.unwrap_or_default()
    );
    anyhow::ensure!(
        batch_frames_end(bytes) == bytes.len(),
        "the bytes end with a stray tail shorter than a batch header"
    );
    let mut out = Vec::new();
    for entry in decoded.entries {
        let batch = entry.map_err(|e| anyhow::anyhow!("{e}"))?;
        for record in &batch.records {
            let offset = record.offset;
            match decoder.decode(topic, partition, record, now) {
                (Decoded::Batches(batches), OmittedHeaders(0)) => out.extend(batches),
                (Decoded::Batches(_), OmittedHeaders(n)) => {
                    anyhow::bail!("the record at offset {offset} has {n} header name(s) not UTF-8")
                }
                (Decoded::Skipped(reason), _) => {
                    anyhow::bail!("the record at offset {offset} is skipped as {reason}")
                }
                (Decoded::Undecodable(why), _) => {
                    anyhow::bail!("the record at offset {offset} doesn't decode: {why}")
                }
            }
        }
    }
    Ok(out)
}

/// Where the run of batch frames in `bytes` ends, read from each frame's length alone: short of
/// `bytes.len()` when fewer than 12 bytes trail the last frame, which
/// `logit_proto::kafka::decode` leaves unread without saying so.
fn batch_frames_end(bytes: &[u8]) -> usize {
    let mut pos = 0usize;
    while bytes.len() - pos >= 12 {
        let length = i32::from_be_bytes(bytes[pos + 8..pos + 12].try_into().unwrap_or_default());
        let Ok(length) = usize::try_from(length) else { return pos };
        let end = pos + 12 + length;
        if end > bytes.len() {
            return pos;
        }
        pos = end;
    }
    pos
}

fn signal_name(signal: Signal) -> &'static str {
    match signal {
        Signal::Logs => "logs",
        Signal::Metrics => "metrics",
        Signal::Traces => "traces",
    }
}

/// The signal of an OTLP/JSON request whose top-level key names another signal than `mapped`.
/// OTLP/JSON ignores a field it doesn't know, so such a request decodes to nothing under the
/// topic's signal; read as `empty` it would be committed past with no loss counted. Called only
/// for a request that decoded to no event.
fn other_otlp_json_signal(value: &Bytes, mapped: Signal) -> Option<&'static str> {
    let keys: std::collections::HashMap<String, serde::de::IgnoredAny> =
        serde_json::from_slice(value).ok()?;
    [
        ("resourceLogs", Signal::Logs),
        ("resourceMetrics", Signal::Metrics),
        ("resourceSpans", Signal::Traces),
    ]
    .into_iter()
    .find(|&(key, signal)| signal != mapped && keys.contains_key(key))
    .map(|(_, signal)| signal_name(signal))
}
