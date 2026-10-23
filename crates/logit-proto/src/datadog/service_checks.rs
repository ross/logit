//! Datadog service checks: `POST /api/v1/check_run`, both directions. The mapping table is in the
//! parent module's doc ("Logs, events, service checks").
//!
//! A check decodes to exactly the [`Event::metric`] `statsd_in`'s DogStatsD `_sc|...` parser builds
//! (`crates/logit-proto/src/statsd/decode.rs`'s `parse_service_check`): a `Gauge(status)` named
//! after the check, plus the same `_logit.statsd.service_check.*` attributes, so either one
//! re-encodes on either route.

use super::events::{non_empty_str, seconds};
use super::logs::{new_batch, parse_json};
use super::outcomes::{MetricIn, MetricOut};
use super::tags::{insert_tags, render_tags};
use super::time::{nanos_to_seconds, or_received};
use super::{
    host_tag, is_service_check, DatadogDecoder, DatadogEncoder, ATTR_HOST_NAME, ATTR_HOST_TAG,
};
use crate::json::{value_text, write_i64, write_str, JsonObject};
use crate::CodecError;
use bytes::Bytes;
use logit_core::attrs::merged;
use logit_core::interner::intern;
use logit_core::{AttrMap, Event, EventBatch, MetricKind, MetricRecord, Resource, Symbol, Value};
use serde_json::Value as Json;
use std::borrow::Cow;
use std::sync::LazyLock;

/// `statsd_in`'s service-check attribute names (`crates/logit-proto/src/statsd/decode.rs`), reused
/// verbatim.
pub const ATTR_SERVICE_CHECK_NAME: &str = "_logit.statsd.service_check.name";
pub const ATTR_SERVICE_CHECK_STATUS: &str = "_logit.statsd.service_check.status";
pub const ATTR_SERVICE_CHECK_MESSAGE: &str = "_logit.statsd.service_check.message";
pub const ATTR_SERVICE_CHECK_HOST: &str = "_logit.statsd.service_check.host";

/// The highest valid status: 0 OK, 1 WARNING, 2 CRITICAL, 3 UNKNOWN.
const MAX_STATUS: u64 = 3;

struct CheckKeys {
    name: Symbol,
    status: Symbol,
    message: Symbol,
    host: Symbol,
    host_name: Symbol,
    host_tag: Symbol,
}

static KEYS: LazyLock<CheckKeys> = LazyLock::new(|| CheckKeys {
    name: intern(ATTR_SERVICE_CHECK_NAME),
    status: intern(ATTR_SERVICE_CHECK_STATUS),
    message: intern(ATTR_SERVICE_CHECK_MESSAGE),
    host: intern(ATTR_SERVICE_CHECK_HOST),
    host_name: intern(ATTR_HOST_NAME),
    host_tag: intern(ATTR_HOST_TAG),
});

impl DatadogDecoder {
    /// Decodes a `/api/v1/check_run` body: a JSON array of checks (one bare object is accepted
    /// too). One [`Event::metric`] per check; a check whose status isn't 0 to 3, or that has no
    /// name, is skipped and counted. The batch `Resource` is empty.
    pub fn decode_service_checks(
        &mut self,
        body: &[u8],
        received_at: i64,
    ) -> Result<EventBatch, CodecError> {
        let json = parse_json(body, "service checks")?;
        let items = match &json {
            Json::Array(items) => items.as_slice(),
            Json::Object(_) => std::slice::from_ref(&json),
            _ => {
                return Err(CodecError::Malformed(
                    "datadog service checks body must be a JSON array".into(),
                ))
            }
        };
        let mut events = Vec::with_capacity(items.len());
        for item in items {
            if let Some(event) = self.decode_check(item, received_at) {
                events.push(event);
            }
        }
        Ok(new_batch(Resource::default(), events))
    }

    fn decode_check(&mut self, item: &Json, received_at: i64) -> Option<Event> {
        let Json::Object(obj) = item else {
            self.skip_check(MetricIn::Malformed);
            self.diagnostics.warn_throttled(
                "malformed_service_check",
                "datadog service check is not an object",
            );
            return None;
        };
        let Some(name) = non_empty_str(obj, "check") else {
            self.skip_check(MetricIn::NoName);
            return None;
        };
        let status = match obj.get("status").and_then(Json::as_u64) {
            Some(status) if status <= MAX_STATUS => status,
            _ => {
                self.skip_check(MetricIn::InvalidStatus);
                return None;
            }
        };
        let timestamp = obj.get("timestamp").and_then(seconds);

        let mut attributes = AttrMap::new();
        if let Some(Json::Array(tags)) = obj.get("tags") {
            let reserved = insert_tags(&mut attributes, tags.iter().filter_map(Json::as_str));
            self.reserved_tags(reserved);
        }
        attributes.insert(ATTR_SERVICE_CHECK_NAME, Value::str(name));
        attributes.insert(ATTR_SERVICE_CHECK_STATUS, Value::U64(status));
        if let Some(message) = non_empty_str(obj, "message") {
            attributes.insert(ATTR_SERVICE_CHECK_MESSAGE, Value::str(message));
        }
        if let Some(host) = non_empty_str(obj, "host_name") {
            attributes.insert(ATTR_SERVICE_CHECK_HOST, Value::str(host));
        }
        Some(Event::metric(
            or_received(timestamp, received_at),
            attributes,
            MetricRecord::new(intern(name), MetricKind::gauge(status as f64)),
        ))
    }

    fn skip_check(&self, outcome: MetricIn) {
        self.metric_not_decoded(outcome, 1);
    }
}

impl DatadogEncoder {
    /// Encodes every event carrying `_logit.statsd.service_check.name` whose first metric is a
    /// `Gauge` as one `/api/v1/check_run` JSON array, every key present on every item; `None` when
    /// no event qualifies. Any other event is skipped silently (it belongs to another route); a
    /// check with no valid status is skipped and counted.
    pub fn encode_service_checks(&self, batch: &EventBatch) -> Option<Bytes> {
        let keys = &*KEYS;
        let mut out = Vec::new();
        out.push(b'[');
        let mut any = false;
        for event in &batch.events {
            if !is_service_check(&batch.resource, event) {
                continue;
            }
            let Some(MetricKind::Gauge(gauge)) = event.metrics.first().map(|m| &m.kind) else {
                continue;
            };
            let gauge = &gauge.as_f64();
            let attrs: Vec<(Symbol, &Value)> = merged(&batch.resource, event).collect();
            let find = |key: Symbol| attrs.iter().find(|(k, _)| *k == key).map(|(_, v)| *v);
            let Some(name) = find(keys.name) else { continue };
            // The raw carrier outranks the gauge, which a transform may have rescaled.
            let status = match find(keys.status) {
                Some(Value::U64(s)) if *s <= MAX_STATUS => Some(*s),
                _ => (gauge.fract() == 0.0 && (0.0..=MAX_STATUS as f64).contains(gauge))
                    .then_some(*gauge as u64),
            };
            let Some(status) = status else {
                self.metric_not_sent(MetricOut::InvalidStatus);
                continue;
            };

            // `host.name`, else `statsd.service_check.host`, else a DogStatsD `#host:` tag, else
            // the default. A `host.name` or `host` read this way leaves as the host and not a
            // tag; the Agent consumes a `host:` tag even when `h:` wins. A carrier `host.name`
            // outranks is private, so it's never a tag either.
            let non_empty = |key: Symbol| find(key).filter(|v| v.as_str() != Some(""));
            let host_name = non_empty(keys.host_name);
            let own_host = if host_name.is_none() { non_empty(keys.host) } else { None };
            let host_tag = host_tag(&batch.resource, event);
            let mut skip = vec![keys.name, keys.status, keys.message, keys.host];
            // An empty `host.name` is no host, so it isn't a tag either.
            let empty_host_name = find(keys.host_name).is_some_and(|v| v.as_str() == Some(""));
            skip.extend((host_name.is_some() || empty_host_name).then_some(keys.host_name));
            skip.extend(host_tag.map(|_| keys.host_tag));
            let mut tags = Vec::new();
            let dropped = render_tags(attrs.iter().map(|(k, v)| (*k, *v)), &skip, &mut tags);
            if dropped.unrepresentable > 0 {
                self.telemetry.count(
                    "logit.output.tags.omitted",
                    dropped.unrepresentable as f64,
                    &[("reason", "unrepresentable")],
                );
            }

            if any {
                out.push(b',');
            }
            any = true;
            let mut obj = JsonObject::begin(&mut out);
            write_str(obj.key("check"), &value_text(name));
            let host = match (host_name.or(own_host), host_tag) {
                (Some(host), _) => value_text(host),
                // An empty `#host:` tag: marked hostless, never filled.
                (None, Some(tag)) => Cow::Borrowed(tag),
                (None, None) => Cow::Borrowed(self.default_host.as_deref().unwrap_or_default()),
            };
            write_str(obj.key("host_name"), &host);
            write_i64(obj.key("timestamp"), nanos_to_seconds(event.timestamp));
            write_i64(obj.key("status"), status as i64);
            write_str(obj.key("message"), &find(keys.message).map(value_text).unwrap_or_default());
            let buf = obj.key("tags");
            buf.push(b'[');
            for (i, tag) in tags.iter().enumerate() {
                if i > 0 {
                    buf.push(b',');
                }
                write_str(buf, tag);
            }
            buf.push(b']');
            obj.finish();
        }
        if !any {
            return None;
        }
        out.push(b']');
        Some(Bytes::from(out))
    }
}

#[cfg(test)]
mod tests {
    use super::super::logs::tests::counted;
    use super::*;
    use logit_core::interner::resolve;
    use logit_core::telemetry::Registry;
    use std::sync::Arc;

    const RECEIVED_AT: i64 = 1_690_000_000_000_000_000;

    fn decode(body: &str) -> EventBatch {
        DatadogDecoder::new().decode_service_checks(body.as_bytes(), RECEIVED_AT).expect("decodes")
    }

    #[test]
    fn check_decodes_like_a_dogstatsd_service_check() {
        let batch = decode(
            r#"[{"check":"app.ok","host_name":"web1","timestamp":1700000000,"status":2,"message":"down","tags":["env:prod"]}]"#,
        );
        let event = &batch.events[0];
        assert_eq!(event.timestamp, 1_700_000_000_000_000_000);
        assert_eq!(resolve(event.metrics[0].name), "app.ok");
        assert_eq!(event.metrics[0].kind, MetricKind::gauge(2.0));
        assert_eq!(event.attributes.get(ATTR_SERVICE_CHECK_NAME), Some(&Value::str("app.ok")));
        assert_eq!(event.attributes.get(ATTR_SERVICE_CHECK_STATUS), Some(&Value::U64(2)));
        assert_eq!(event.attributes.get(ATTR_SERVICE_CHECK_MESSAGE), Some(&Value::str("down")));
        assert_eq!(event.attributes.get(ATTR_SERVICE_CHECK_HOST), Some(&Value::str("web1")));
        assert_eq!(event.attributes.get("env"), Some(&Value::str("prod")));
    }

    #[test]
    fn the_agents_empty_check_decodes_with_no_optional_attributes() {
        let batch = decode(
            r#"[{"check":"app.ok","host_name":"","timestamp":0,"status":0,"message":"","tags":null}]"#,
        );
        let event = &batch.events[0];
        assert_eq!(event.timestamp, RECEIVED_AT);
        assert_eq!(event.attributes.len(), 2);
    }

    #[test]
    fn invalid_statuses_and_nameless_checks_are_dropped_and_counted() {
        let registry = Registry::new();
        let mut decoder = DatadogDecoder::new().with_telemetry(registry.telemetry_for(
            "datadog_in",
            "datadog_in",
            "listener",
        ));
        let batch = decoder
            .decode_service_checks(
                br#"[{"check":"a","status":4},{"check":"b","status":-1},{"check":"c"},{"status":0},{"check":"d","status":1}]"#,
                RECEIVED_AT,
            )
            .unwrap();
        assert_eq!(batch.events.len(), 1);
        assert_eq!(
            counted(&registry, "logit.input.metrics.dropped", ("reason", "invalid_status")),
            3.0
        );
    }

    #[test]
    fn encode_writes_every_key() {
        let batch = decode(
            r#"[{"check":"app.ok","host_name":"","timestamp":1700000000,"status":1,"message":"","tags":null}]"#,
        );
        let out = DatadogEncoder::new().encode_service_checks(&batch).unwrap();
        assert_eq!(
            std::str::from_utf8(&out).unwrap(),
            r#"[{"check":"app.ok","host_name":"","timestamp":1700000000,"status":1,"message":"","tags":[]}]"#
        );
    }

    /// `host.name` (then no tag of that name), then `statsd.service_check.host`, then the
    /// default host. A carrier `host.name` outranks is left off.
    #[test]
    fn encode_fills_a_missing_host_with_host_name_then_the_default_host() {
        let encoder = DatadogEncoder::new().with_default_host(Some(Arc::from("relay")));
        let encode_with = |batch: &EventBatch| {
            String::from_utf8(encoder.encode_service_checks(batch).unwrap().to_vec()).unwrap()
        };
        let hostless = decode(r#"[{"check":"app.ok","timestamp":1700000000,"status":1}]"#);
        assert_eq!(
            encode_with(&hostless),
            r#"[{"check":"app.ok","host_name":"relay","timestamp":1700000000,"status":1,"message":"","tags":[]}]"#
        );

        let mut stamped = hostless.clone();
        let mut resource = Resource::default();
        resource.attributes.insert(ATTR_HOST_NAME, Value::str("web-1"));
        stamped.resource = Arc::new(resource);
        assert_eq!(
            encode_with(&stamped),
            r#"[{"check":"app.ok","host_name":"web-1","timestamp":1700000000,"status":1,"message":"","tags":[]}]"#
        );

        let mut own =
            decode(r#"[{"check":"app.ok","host_name":"h","timestamp":1700000000,"status":1}]"#);
        own.resource = stamped.resource.clone();
        assert_eq!(
            encode_with(&own),
            r#"[{"check":"app.ok","host_name":"web-1","timestamp":1700000000,"status":1,"message":"","tags":[]}]"#
        );
        let own =
            decode(r#"[{"check":"app.ok","host_name":"h","timestamp":1700000000,"status":1}]"#);
        assert_eq!(
            encode_with(&own),
            r#"[{"check":"app.ok","host_name":"h","timestamp":1700000000,"status":1,"message":"","tags":[]}]"#
        );
    }

    /// A `collectd.host` is another family's key, so it stays a tag and the check takes the
    /// default host.
    #[test]
    fn a_collectd_host_is_an_ordinary_tag() {
        let encoder = DatadogEncoder::new().with_default_host(Some(Arc::from("relay")));
        let body =
            r#"[{"check":"c","timestamp":1700000000,"status":0,"tags":["collectd.host:web1"]}]"#;
        let out = encoder.encode_service_checks(&decode(body)).unwrap();
        assert_eq!(
            String::from_utf8(out.to_vec()).unwrap(),
            r#"[{"check":"c","host_name":"relay","timestamp":1700000000,"status":0,"message":"","tags":["collectd.host:web1"]}]"#
        );
    }

    /// A DogStatsD `#host:` tag (`statsd_in`'s `host`) is the host after `host.name` and `h:`,
    /// and is consumed, not tagged, whenever there's no `host.name`; an empty one stays hostless.
    #[test]
    fn a_host_tag_is_the_host_after_host_name_and_h() {
        let encoder = DatadogEncoder::new().with_default_host(Some(Arc::from("relay")));
        let encode_with = |body: &str| {
            String::from_utf8(encoder.encode_service_checks(&decode(body)).unwrap().to_vec())
                .unwrap()
        };
        let check = |host: &str, tags: &str| {
            format!(
                r#"[{{"check":"c","host_name":"{host}","timestamp":1700000000,"status":0,"message":"","tags":{tags}}}]"#
            )
        };
        assert_eq!(
            encode_with(
                r#"[{"check":"c","timestamp":1700000000,"status":0,"tags":["host:web1"]}]"#
            ),
            check("web1", "[]")
        );
        assert_eq!(
            encode_with(
                r#"[{"check":"c","timestamp":1700000000,"status":0,"tags":["host:web1","host.name:"]}]"#
            ),
            check("web1", "[]")
        );
        assert_eq!(
            encode_with(r#"[{"check":"c","timestamp":1700000000,"status":0,"tags":["host:"]}]"#),
            check("", "[]")
        );
        assert_eq!(
            encode_with(
                r#"[{"check":"c","host_name":"h","timestamp":1700000000,"status":0,"tags":["host:web1"]}]"#
            ),
            check("h", "[]")
        );
        assert_eq!(
            encode_with(
                r#"[{"check":"c","timestamp":1700000000,"status":0,"tags":["host:web1","host.name:m"]}]"#
            ),
            check("m", r#"["host:web1"]"#)
        );
    }

    #[test]
    fn encode_skips_non_checks_and_counts_invalid_statuses() {
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_SERVICE_CHECK_NAME, Value::str("x"));
        let bad = Event::metric(0, attrs, MetricRecord::new(intern("x"), MetricKind::gauge(7.0)));
        let plain = Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(intern("y"), MetricKind::gauge(1.0)),
        );
        let batch = new_batch(Resource::default(), vec![bad, plain]);
        let registry = Registry::new();
        let encoder = DatadogEncoder::new().with_telemetry(registry.telemetry_for(
            "datadog_out",
            "datadog_out",
            "sink",
        ));
        assert!(encoder.encode_service_checks(&batch).is_none());
        assert_eq!(
            counted(&registry, "logit.output.metrics.dropped", ("reason", "invalid_status")),
            1.0
        );
    }
}
