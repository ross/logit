//! Datadog events, both directions, in both of their wire shapes: the Agent's envelope
//! (`POST /api/v2/events`, `{"apiKey","events":{"<source>":[...]},"internalHostname"}`) and the
//! public `POST /api/v1/events` object. The mapping table is in the parent module's doc ("Logs,
//! events, service checks").
//!
//! An event decodes to exactly the [`Event::log`] `statsd_in`'s DogStatsD `_e{...}` parser builds
//! (`crates/logit-inputs/src/statsd.rs`'s `parse_event`): the same `statsd.event.*` attribute
//! names, the same `alert_type` → severity mapping, the text as a `Raw` `Str` message. A
//! DogStatsD event and a Datadog-API event are indistinguishable in the model, so either one
//! re-encodes on either route.

use super::logs::{
    json_to_value, new_batch, parse_json, value_text, write_i64, write_str, write_value, JsonObject,
};
use super::tags::{insert_tags, render_tags};
use super::time::{nanos_to_seconds, or_received, seconds_f64_to_nanos, seconds_to_nanos};
use super::{
    DatadogDecoder, DatadogEncoder, ATTR_EVENT_DEVICE_NAME, ATTR_EVENT_RELATED_EVENT_ID,
    ATTR_EVENT_TYPE, RESOURCE_ATTR_AGENT_HOSTNAME,
};
use crate::CodecError;
use bytes::Bytes;
use logit_core::attrs::merged;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, BodyFormat, Event, EventBatch, LogRecord, Resource, Severity, Symbol, Value,
};
use serde_json::{Map, Value as Json};
use std::collections::BTreeMap;
use std::sync::LazyLock;

/// `statsd_in`'s event attribute names (`crates/logit-inputs/src/statsd.rs`), reused verbatim.
pub const ATTR_EVENT_TITLE: &str = "statsd.event.title";
pub const ATTR_EVENT_PRIORITY: &str = "statsd.event.priority";
pub const ATTR_EVENT_HOST: &str = "statsd.event.host";
pub const ATTR_EVENT_ALERT_TYPE: &str = "statsd.event.alert_type";
pub const ATTR_EVENT_AGGREGATION_KEY: &str = "statsd.event.aggregation_key";
pub const ATTR_EVENT_SOURCE_TYPE: &str = "statsd.event.source_type";
/// `statsd.timestamp`: `statsd_in`'s raw `d:`/`|T` carrier. The event's own timestamp already says
/// it, so the encoders never render it as a tag.
pub const ATTR_STATSD_TIMESTAMP: &str = "statsd.timestamp";

/// The envelope's `events` map key for an event with no `source_type_name`.
const NO_SOURCE: &str = "api";

/// Which of Datadog's two event bodies [`DatadogEncoder::encode_events`] writes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EventFormat {
    /// The Agent's `/api/v2/events` envelope: every event of the batch in one body, grouped by
    /// source.
    #[default]
    AgentEnvelope,
    /// The public `/api/v1/events` object: one body per event.
    PublicV1,
}

struct EventKeys {
    title: Symbol,
    priority: Symbol,
    host: Symbol,
    alert_type: Symbol,
    aggregation_key: Symbol,
    source_type: Symbol,
    event_type: Symbol,
    device_name: Symbol,
    related_event_id: Symbol,
    agent_hostname: Symbol,
    statsd_timestamp: Symbol,
}

impl EventKeys {
    /// Every key an encoder writes into a field of its own, so never as a tag.
    fn consumed(&self) -> [Symbol; 11] {
        [
            self.title,
            self.priority,
            self.host,
            self.alert_type,
            self.aggregation_key,
            self.source_type,
            self.event_type,
            self.device_name,
            self.related_event_id,
            self.agent_hostname,
            self.statsd_timestamp,
        ]
    }
}

static KEYS: LazyLock<EventKeys> = LazyLock::new(|| EventKeys {
    title: intern(ATTR_EVENT_TITLE),
    priority: intern(ATTR_EVENT_PRIORITY),
    host: intern(ATTR_EVENT_HOST),
    alert_type: intern(ATTR_EVENT_ALERT_TYPE),
    aggregation_key: intern(ATTR_EVENT_AGGREGATION_KEY),
    source_type: intern(ATTR_EVENT_SOURCE_TYPE),
    event_type: intern(ATTR_EVENT_TYPE),
    device_name: intern(ATTR_EVENT_DEVICE_NAME),
    related_event_id: intern(ATTR_EVENT_RELATED_EVENT_ID),
    agent_hostname: intern(RESOURCE_ATTR_AGENT_HOSTNAME),
    statsd_timestamp: intern(ATTR_STATSD_TIMESTAMP),
});

impl DatadogDecoder {
    /// Decodes an events body. An object whose `events` member is an object is the Agent's
    /// envelope; any other object is one public `/api/v1/events` event. Either way each event
    /// becomes one [`Event::log`]; one with neither a title nor a text is skipped and counted.
    /// The envelope's `internalHostname` is the batch `Resource`'s `datadog.agent.hostname`.
    pub fn decode_events(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let json = parse_json(body, "events")?;
        let Json::Object(top) = &json else {
            return Err(CodecError::Malformed("datadog events body must be a JSON object".into()));
        };
        let mut resource = Resource::default();
        let mut events = Vec::new();
        match top.get("events") {
            Some(Json::Object(groups)) => {
                if let Some(host) = non_empty_str(top, "internalHostname") {
                    resource.attributes.insert(RESOURCE_ATTR_AGENT_HOSTNAME, Value::str(host));
                }
                for (source, items) in groups {
                    let Json::Array(items) = items else {
                        self.skip_event("malformed");
                        self.diagnostics.warn_throttled(
                            "malformed_event",
                            "datadog events envelope group is not an array",
                        );
                        continue;
                    };
                    let source =
                        (source != NO_SOURCE && !source.is_empty()).then_some(source.as_str());
                    for item in items {
                        if let Some(event) = self.decode_event_item(item, source, received_at) {
                            events.push(event);
                        }
                    }
                }
            }
            _ => {
                if let Some(event) = self.decode_event_item(&json, None, received_at) {
                    events.push(event);
                }
            }
        }
        Ok(new_batch(resource, events))
    }

    fn decode_event_item(
        &mut self,
        item: &Json,
        source: Option<&str>,
        received_at: i64,
    ) -> Option<Event> {
        let Json::Object(obj) = item else {
            self.skip_event("malformed");
            self.diagnostics.warn_throttled("malformed_event", "datadog event is not an object");
            return None;
        };
        let title = str_of(obj, "msg_title").or_else(|| str_of(obj, "title"));
        let text = str_of(obj, "msg_text").or_else(|| str_of(obj, "text"));
        if title.is_none() && text.is_none() {
            self.skip_event("no_title");
            return None;
        }
        let timestamp = obj.get("timestamp").or_else(|| obj.get("date_happened")).and_then(seconds);

        let mut attributes = AttrMap::new();
        if let Some(Json::Array(tags)) = obj.get("tags") {
            insert_tags(&mut attributes, tags.iter().filter_map(Json::as_str));
        }
        attributes.insert(ATTR_EVENT_TITLE, Value::str(title.unwrap_or_default()));
        let mut severity = None;
        if let Some(alert_type) = non_empty_str(obj, "alert_type") {
            // `statsd_in`'s mapping; any other value keeps the attribute and no severity.
            severity = match alert_type {
                "error" => Some(Severity::Error),
                "warning" => Some(Severity::Warn),
                "success" | "info" => Some(Severity::Info),
                _ => None,
            };
            attributes.insert(ATTR_EVENT_ALERT_TYPE, Value::str(alert_type));
        }
        for (wire, attr) in [
            ("priority", ATTR_EVENT_PRIORITY),
            ("host", ATTR_EVENT_HOST),
            ("aggregation_key", ATTR_EVENT_AGGREGATION_KEY),
            ("event_type", ATTR_EVENT_TYPE),
            ("device_name", ATTR_EVENT_DEVICE_NAME),
        ] {
            if let Some(value) = non_empty_str(obj, wire) {
                attributes.insert(attr, Value::str(value));
            }
        }
        if let Some(source) = non_empty_str(obj, "source_type_name").or(source) {
            attributes.insert(ATTR_EVENT_SOURCE_TYPE, Value::str(source));
        }
        match obj.get("related_event_id") {
            None | Some(Json::Null) => {}
            Some(id) => attributes.insert(ATTR_EVENT_RELATED_EVENT_ID, json_to_value(id)),
        }

        Some(Event::log(
            or_received(timestamp, received_at),
            attributes,
            LogRecord {
                message: Value::str(text.unwrap_or_default()),
                severity,
                body_format: BodyFormat::Raw,
                trace: None,
                // `None` for `statsd_in`'s reason: a title is free text, not a bounded vocabulary.
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        ))
    }

    fn skip_event(&self, reason: &'static str) {
        self.telemetry.count("logit.input.events.skipped", 1.0, &[("reason", reason)]);
    }
}

/// A string member, `None` when absent, `null`, or not a string.
fn str_of<'a>(obj: &'a Map<String, Json>, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(Json::as_str)
}

/// A string member that is also non-empty: the Agent omits an empty optional field, so an empty
/// one means the same as an absent one.
pub(super) fn non_empty_str<'a>(obj: &'a Map<String, Json>, key: &str) -> Option<&'a str> {
    str_of(obj, key).filter(|s| !s.is_empty())
}

/// Unix seconds, integer or fractional, → ns. `None` for anything else.
pub(super) fn seconds(value: &Json) -> Option<i64> {
    match value {
        Json::Number(n) => match n.as_i64() {
            Some(s) => Some(seconds_to_nanos(s)),
            None => n.as_f64().and_then(seconds_f64_to_nanos),
        },
        _ => None,
    }
}

impl DatadogEncoder {
    /// Encodes every event carrying `statsd.event.title` in `format`: [`EventFormat::AgentEnvelope`]
    /// gives at most one body for the whole batch, [`EventFormat::PublicV1`] one body per event.
    /// An event without a title (a plain log) is skipped silently: it belongs to the logs route.
    pub fn encode_events(&self, batch: &EventBatch, format: EventFormat) -> Vec<Bytes> {
        let keys = &*KEYS;
        let titled = batch.events.iter().filter(|e| {
            e.attributes.get_sym(keys.title).is_some()
                || batch.resource.attributes.get_sym(keys.title).is_some()
        });
        match format {
            EventFormat::PublicV1 => titled
                .map(|event| {
                    let mut out = Vec::new();
                    self.encode_event(&batch.resource, event, format, &mut out);
                    Bytes::from(out)
                })
                .collect(),
            EventFormat::AgentEnvelope => {
                // Grouped by source, sources in sorted order (the order a decode walks them in,
                // so a relayed envelope keeps its event order), events in batch order within one.
                let mut groups: BTreeMap<String, Vec<u8>> = BTreeMap::new();
                for event in titled {
                    let source = merged(&batch.resource, event)
                        .find(|(k, _)| *k == keys.source_type)
                        .map(|(_, v)| value_text(v).into_owned())
                        .filter(|s| !s.is_empty())
                        .unwrap_or_else(|| NO_SOURCE.to_string());
                    let items = groups.entry(source).or_default();
                    if !items.is_empty() {
                        items.push(b',');
                    }
                    self.encode_event(&batch.resource, event, format, items);
                }
                if groups.is_empty() {
                    return Vec::new();
                }
                let mut out = Vec::new();
                let mut envelope = JsonObject::begin(&mut out);
                write_str(envelope.key("apiKey"), "");
                let buf = envelope.key("events");
                let mut by_source = JsonObject::begin(buf);
                for (source, items) in &groups {
                    let buf = by_source.key(source);
                    buf.push(b'[');
                    buf.extend_from_slice(items);
                    buf.push(b']');
                }
                by_source.finish();
                let hostname = batch
                    .resource
                    .attributes
                    .get_sym(keys.agent_hostname)
                    .map(value_text)
                    .unwrap_or_default();
                write_str(envelope.key("internalHostname"), &hostname);
                envelope.finish();
                vec![Bytes::from(out)]
            }
        }
    }

    fn encode_event(
        &self,
        resource: &Resource,
        event: &Event,
        format: EventFormat,
        out: &mut Vec<u8>,
    ) {
        let keys = &*KEYS;
        let attrs: Vec<(Symbol, &Value)> = merged(resource, event).collect();
        let find = |key: Symbol| attrs.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);
        // An optional field: written only when present and not an empty string.
        let optional = |key: Symbol| find(key).filter(|v| v.as_str() != Some(""));
        let agent = format == EventFormat::AgentEnvelope;

        let mut tags = Vec::new();
        let dropped = render_tags(attrs.iter().map(|(k, v)| (*k, *v)), &keys.consumed(), &mut tags);
        if dropped.unrepresentable > 0 {
            self.telemetry.count(
                "logit.output.tags.dropped",
                dropped.unrepresentable as f64,
                &[("reason", "unrepresentable")],
            );
        }

        let mut obj = JsonObject::begin(out);
        let title = find(keys.title).map(value_text).unwrap_or_default();
        write_str(obj.key(if agent { "msg_title" } else { "title" }), &title);
        let text = event.log.as_ref().map(|log| value_text(&log.message)).unwrap_or_default();
        write_str(obj.key(if agent { "msg_text" } else { "text" }), &text);
        write_i64(
            obj.key(if agent { "timestamp" } else { "date_happened" }),
            nanos_to_seconds(event.timestamp),
        );
        if let Some(priority) = optional(keys.priority) {
            write_str(obj.key("priority"), &value_text(priority));
        }
        // The Agent always writes `host`, empty or not; the public form only when set.
        match optional(keys.host) {
            Some(host) => write_str(obj.key("host"), &value_text(host)),
            None if agent => write_str(obj.key("host"), ""),
            None => {}
        }
        if !tags.is_empty() {
            let buf = obj.key("tags");
            buf.push(b'[');
            for (i, tag) in tags.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                write_str(buf, tag);
            }
            buf.push(b']');
        }
        let alert_type = match optional(keys.alert_type) {
            Some(value) => Some(value_text(value)),
            None => event.log.as_ref().and_then(|log| log.severity).and_then(|s| match s {
                Severity::Error | Severity::Fatal => Some("error".into()),
                Severity::Warn => Some("warning".into()),
                Severity::Info => Some("info".into()),
                Severity::Debug | Severity::Trace => None,
            }),
        };
        if let Some(alert_type) = alert_type {
            write_str(obj.key("alert_type"), &alert_type);
        }
        for (wire, key) in [
            ("aggregation_key", keys.aggregation_key),
            ("source_type_name", keys.source_type),
            ("event_type", keys.event_type),
            ("device_name", keys.device_name),
        ] {
            if let Some(value) = optional(key) {
                write_str(obj.key(wire), &value_text(value));
            }
        }
        if let Some(id) = find(keys.related_event_id) {
            write_value(obj.key("related_event_id"), id);
        }
        obj.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::super::logs::tests::counted;
    use super::*;
    use logit_core::telemetry::Registry;
    use std::sync::Arc;

    const RECEIVED_AT: i64 = 1_690_000_000_000_000_000;

    fn decode(body: &str) -> EventBatch {
        DatadogDecoder::new().decode_events(body.as_bytes(), RECEIVED_AT).expect("decodes")
    }

    fn encode(batch: &EventBatch, format: EventFormat) -> Vec<String> {
        DatadogEncoder::new()
            .encode_events(batch, format)
            .into_iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect()
    }

    fn attr<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
        event.attributes.get(key).and_then(Value::as_str)
    }

    const ENVELOPE: &str = r#"{"apiKey":"","events":{"api":[{"msg_title":"deploy","msg_text":"v2 out","timestamp":1700000000,"priority":"low","host":"web1","tags":["env:prod","urgent"],"alert_type":"warning","aggregation_key":"agg","event_type":"deploy_event"}],"nagios":[{"msg_title":"t","msg_text":"b","timestamp":1700000001,"host":""}]},"internalHostname":"agent-host"}"#;

    #[test]
    fn envelope_decodes_like_a_dogstatsd_event() {
        let batch = decode(ENVELOPE);
        assert_eq!(
            batch.resource.attributes.get(RESOURCE_ATTR_AGENT_HOSTNAME),
            Some(&Value::str("agent-host"))
        );
        assert_eq!(batch.events.len(), 2);
        let event = &batch.events[0];
        assert_eq!(event.timestamp, 1_700_000_000_000_000_000);
        let log = event.log.as_ref().unwrap();
        assert_eq!(log.message, Value::str("v2 out"));
        assert_eq!(log.severity, Some(Severity::Warn));
        assert_eq!(log.body_format, BodyFormat::Raw);
        assert_eq!(log.event_name, None);
        assert_eq!(attr(event, "statsd.event.title"), Some("deploy"));
        assert_eq!(attr(event, "statsd.event.priority"), Some("low"));
        assert_eq!(attr(event, "statsd.event.host"), Some("web1"));
        assert_eq!(attr(event, "statsd.event.alert_type"), Some("warning"));
        assert_eq!(attr(event, "statsd.event.aggregation_key"), Some("agg"));
        assert_eq!(attr(event, "datadog.event_type"), Some("deploy_event"));
        assert_eq!(attr(event, "env"), Some("prod"));
        assert_eq!(event.attributes.get("urgent"), Some(&Value::Bool(true)));
        // `api` means no source.
        assert_eq!(attr(event, "statsd.event.source_type"), None);

        let second = &batch.events[1];
        assert_eq!(attr(second, "statsd.event.source_type"), Some("nagios"));
        assert_eq!(attr(second, "statsd.event.host"), None);
        assert_eq!(second.log.as_ref().unwrap().severity, None);
    }

    #[test]
    fn envelope_encodes_in_the_agents_key_order() {
        let out = encode(&decode(ENVELOPE), EventFormat::AgentEnvelope);
        assert_eq!(
            out,
            [
                r#"{"apiKey":"","events":{"api":[{"msg_title":"deploy","msg_text":"v2 out","timestamp":1700000000,"priority":"low","host":"web1","tags":["env:prod","urgent"],"alert_type":"warning","aggregation_key":"agg","event_type":"deploy_event"}],"nagios":[{"msg_title":"t","msg_text":"b","timestamp":1700000001,"host":"","source_type_name":"nagios"}]},"internalHostname":"agent-host"}"#
            ]
        );
    }

    #[test]
    fn public_v1_event_maps_its_own_field_names() {
        let body = r#"{"title":"T","text":"body","date_happened":1700000000,"priority":"normal","host":"h","tags":["a:1"],"alert_type":"user_update","aggregation_key":"k","source_type_name":"jenkins","device_name":"sda","related_event_id":42}"#;
        let batch = decode(body);
        assert!(batch.resource.attributes.is_empty());
        let event = &batch.events[0];
        assert_eq!(attr(event, "statsd.event.title"), Some("T"));
        assert_eq!(attr(event, "statsd.event.alert_type"), Some("user_update"));
        assert_eq!(event.log.as_ref().unwrap().severity, None);
        assert_eq!(attr(event, "statsd.event.source_type"), Some("jenkins"));
        assert_eq!(attr(event, "datadog.event.device_name"), Some("sda"));
        assert_eq!(event.attributes.get("datadog.event.related_event_id"), Some(&Value::I64(42)));
        assert_eq!(encode(&batch, EventFormat::PublicV1), [body]);
    }

    #[test]
    fn events_without_title_or_text_are_skipped_and_counted() {
        let registry = Registry::new();
        let mut decoder = DatadogDecoder::new().with_telemetry(registry.telemetry_for(
            "datadog_in",
            "datadog_in",
            "listener",
        ));
        let batch = decoder
            .decode_events(
                br#"{"events":{"api":[{"host":"h"},{"msg_text":"only text"}]},"internalHostname":""}"#,
                RECEIVED_AT,
            )
            .unwrap();
        assert!(batch.resource.attributes.is_empty());
        assert_eq!(batch.events.len(), 1);
        assert_eq!(attr(&batch.events[0], "statsd.event.title"), Some(""));
        assert_eq!(batch.events[0].timestamp, RECEIVED_AT);
        assert_eq!(counted(&registry, "logit.input.events.skipped", ("reason", "no_title")), 1.0);
    }

    #[test]
    fn non_object_bodies_are_malformed() {
        assert!(DatadogDecoder::new().decode_events(b"[]", 0).is_err());
        assert!(DatadogDecoder::new().decode_events(b"{", 0).is_err());
    }

    #[test]
    fn plain_logs_are_not_events_and_severity_derives_alert_type() {
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_EVENT_TITLE, Value::str("from a transform"));
        attrs.insert("nested", Value::Map(Box::new(AttrMap::new())));
        let log = LogRecord {
            message: Value::str("x"),
            severity: Some(Severity::Fatal),
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        };
        let mut plain = Event::log(0, AttrMap::new(), log.clone());
        plain.attributes.insert("status", Value::str("info"));
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![plain, Event::log(5_000_000_000, attrs, log)],
        };
        let registry = Registry::new();
        let encoder = DatadogEncoder::new().with_telemetry(registry.telemetry_for(
            "datadog_out",
            "datadog_out",
            "sink",
        ));
        let out = encoder.encode_events(&batch, EventFormat::AgentEnvelope);
        assert_eq!(
            String::from_utf8(out[0].to_vec()).unwrap(),
            r#"{"apiKey":"","events":{"api":[{"msg_title":"from a transform","msg_text":"x","timestamp":5,"host":"","alert_type":"error"}]},"internalHostname":""}"#
        );
        assert_eq!(
            counted(&registry, "logit.output.tags.dropped", ("reason", "unrepresentable")),
            1.0
        );

        let untitled = EventBatch { events: vec![batch.events[0].clone()], ..batch };
        assert!(DatadogEncoder::new()
            .encode_events(&untitled, EventFormat::AgentEnvelope)
            .is_empty());
    }
}
