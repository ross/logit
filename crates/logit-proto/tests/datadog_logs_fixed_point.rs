//! Pure-codec fixed-point tests for the Datadog logs, events, and service-checks routes:
//! `docs/adr/lossless-transit.md`'s round-trip requirement, exercised directly against
//! [`DatadogDecoder`]/[`DatadogEncoder`] with no listener, sink, or HTTP in between. The metrics
//! routes have their own file, `tests/datadog_metrics_fixed_point.rs`.
//!
//! Every case starts from wire bytes and asserts, from the first hop on:
//!
//! 1. **`decode(encode(decode(b))) == decode(b)`**, whole-`EventBatch` equality via `PartialEq`
//!    (resource, events, every attribute and log field). The one exception is the Agent's events
//!    envelope, whose first hop may reorder events across source groups (an item whose
//!    `source_type_name` differs from its wire group moves to that group): there the first hop is
//!    compared as a multiset and exact order is asserted from the second hop on.
//! 2. **`encode(decode(encode(decode(b)))) == encode(decode(b))`** on bytes, which catches an
//!    encoder that writes two different bodies for what it itself considers the same batch.
//!
//! Not from the input bytes themselves: the first hop applies the permitted normalizations in
//! `datadog/mod.rs`'s module doc (key order, an Agent envelope's `source_type_name` restated in
//! each item, a check's `tags: null` written as `[]`, an RFC 3339 log timestamp written as integer
//! milliseconds), so the input is not a fixed point but its first decode is.
//!
//! The `proptest`s generate bodies from a grammar rather than a model, so they reach every wire
//! branch the decoder has: array and bare-object log bodies, integer and RFC 3339 timestamps,
//! absent timestamps (stamped with a whole-second `RECEIVED_AT`), extra keys with nested objects
//! and arrays, repeated and bare tags, both event shapes, and `null` check tags. Generated extra
//! log keys start with `x` so none collides with a reserved wire field or with one of the model
//! names the encoder falls back to (`host.name`, `service.name`, `datadog.source`); those
//! fallbacks are a named normalization with their own unit test in `datadog/logs.rs`.

use bytes::Bytes;
use logit_core::{format_rfc3339_utc, Event};
use logit_proto::datadog::events::EventFormat;
use logit_proto::datadog::{DatadogDecoder, DatadogEncoder};
use proptest::prelude::*;
use serde_json::{json, Map, Value as Json};

/// Whole seconds (so also whole milliseconds): an absent timestamp is stamped with this, and the
/// encoders write seconds or milliseconds.
const RECEIVED_AT: i64 = 1_690_000_000_000_000_000;

fn assert_logs_fixed_point(body: &[u8]) {
    let d1 = DatadogDecoder::new().decode_logs(body, RECEIVED_AT).expect("first decode");
    let e1 = DatadogEncoder::new().encode_logs(&d1).expect("first encode");
    let d2 = DatadogDecoder::new().decode_logs(&e1, RECEIVED_AT).expect("second decode");
    assert_eq!(d1, d2, "model fixed point; first encode: {}", String::from_utf8_lossy(&e1));
    let e2 = DatadogEncoder::new().encode_logs(&d2).expect("second encode");
    assert_eq!(e1, e2, "wire fixed point");
}

fn assert_events_fixed_point(body: &[u8], format: EventFormat) {
    let d1 = DatadogDecoder::new().decode_events(body, RECEIVED_AT).expect("first decode");
    let e1 = DatadogEncoder::new().encode_events(&d1, format);
    let e1 = match format {
        EventFormat::AgentEnvelope => {
            assert_eq!(e1.len(), 1, "one envelope");
            e1
        }
        EventFormat::PublicV1 => {
            assert_eq!(e1.len(), d1.events.len(), "one body per event");
            e1
        }
    };
    // The public form is one event per body; decode each and compare event by event.
    let mut d2_events = Vec::new();
    let mut d2_resource = None;
    for body in &e1 {
        let d2 = DatadogDecoder::new().decode_events(body, RECEIVED_AT).expect("second decode");
        d2_resource.get_or_insert(d2.resource.clone());
        d2_events.extend(d2.events);
    }
    match format {
        EventFormat::PublicV1 => {
            assert_eq!(d1.events, d2_events, "model fixed point; first encode: {e1:?}")
        }
        EventFormat::AgentEnvelope => {
            // The envelope regroups an item whose `source_type_name` differs from its wire group,
            // so the first hop may reorder events (a permitted normalization); the events
            // themselves, and every later hop's order, are fixed.
            assert_same_events_any_order(&d1.events, &d2_events, &e1);
            assert_eq!(Some(d1.resource.clone()), d2_resource, "envelope resource");
            let d2 = DatadogDecoder::new().decode_events(&e1[0], RECEIVED_AT).unwrap();
            let e2 = DatadogEncoder::new().encode_events(&d2, format);
            assert_eq!(e2, e1, "wire fixed point");
            let d3 = DatadogDecoder::new().decode_events(&e2[0], RECEIVED_AT).unwrap();
            assert_eq!(d2, d3, "model fixed point from the second hop on, order included");
        }
    }
}

fn assert_same_events_any_order(left: &[Event], right: &[Event], encoded: &[Bytes]) {
    assert_eq!(left.len(), right.len(), "event count; first encode: {encoded:?}");
    let mut unmatched: Vec<&Event> = right.iter().collect();
    for event in left {
        let at = unmatched
            .iter()
            .position(|e| *e == event)
            .unwrap_or_else(|| panic!("{event:?} did not survive; first encode: {encoded:?}"));
        unmatched.swap_remove(at);
    }
}

fn assert_checks_fixed_point(body: &[u8]) {
    let d1 = DatadogDecoder::new().decode_service_checks(body, RECEIVED_AT).expect("first decode");
    let e1 = DatadogEncoder::new().encode_service_checks(&d1).expect("first encode");
    let d2 = DatadogDecoder::new().decode_service_checks(&e1, RECEIVED_AT).expect("second decode");
    assert_eq!(d1, d2, "model fixed point; first encode: {}", String::from_utf8_lossy(&e1));
    let e2 = DatadogEncoder::new().encode_service_checks(&d2).expect("second encode");
    assert_eq!(e1, e2, "wire fixed point");
}

// -- hand-written vectors -------------------------------------------------------------------------

#[test]
fn agent_log_item() {
    assert_logs_fixed_point(
        br#"[{"message":"hello world","status":"info","timestamp":1700000000000,"hostname":"myhost","service":"myservice","ddsource":"nginx","ddtags":"env:prod,version:1"}]"#,
    );
}

#[test]
fn public_log_item_with_nested_attributes_and_extra_keys() {
    assert_logs_fixed_point(
        br#"[{"message":"checkout failed","ddsource":"python","service":"cart","status":"ERROR","user":{"id":12345,"roles":["admin","ops"],"address":{"city":"Oslo","zip":null}},"latency_ms":12.5,"retry":false,"attempts":3,"big":18446744073709551615,"neg":-7,"list":[1,"two",[3.25],{"four":4}]}]"#,
    );
}

#[test]
fn bare_object_log_body() {
    assert_logs_fixed_point(br#"{"message":"just one","hostname":"h"}"#);
}

#[test]
fn rfc3339_log_timestamp() {
    assert_logs_fixed_point(
        br#"[{"message":"a","timestamp":"2023-11-14T22:13:20.123Z"},{"message":"b","timestamp":"2023-11-14T23:13:20+01:00"}]"#,
    );
}

#[test]
fn log_without_timestamp_or_status() {
    assert_logs_fixed_point(br#"[{"message":""},{"message":"x","status":"custom"}]"#);
}

#[test]
fn agent_event_envelope() {
    assert_events_fixed_point(
        br#"{"apiKey":"","events":{"api":[{"msg_title":"deploy","msg_text":"v2 out\nnow","timestamp":1700000000,"priority":"low","host":"web1","tags":["env:prod","team:a","team:b","urgent"],"alert_type":"warning","aggregation_key":"agg","event_type":"deploy_event"},{"msg_title":"t","msg_text":"b","timestamp":1700000002,"host":""}],"nagios":[{"msg_title":"n","msg_text":"x","timestamp":1700000001,"host":"h","source_type_name":"nagios"}]},"internalHostname":"agent-host"}"#,
        EventFormat::AgentEnvelope,
    );
}

#[test]
fn minimal_agent_event() {
    assert_events_fixed_point(
        br#"{"apiKey":"","events":{"api":[{"msg_title":"t","msg_text":"b","timestamp":1700000000,"host":"myhost"}]},"internalHostname":""}"#,
        EventFormat::AgentEnvelope,
    );
}

#[test]
fn public_v1_event() {
    let body = br#"{"title":"T","text":"body","date_happened":1700000000,"priority":"normal","host":"h","tags":["a:1","b"],"alert_type":"success","aggregation_key":"k","source_type_name":"jenkins","device_name":"sda","related_event_id":42}"#;
    assert_events_fixed_point(body, EventFormat::PublicV1);
    // And the same event through the Agent's envelope.
    assert_events_fixed_point(body, EventFormat::AgentEnvelope);
}

#[test]
fn service_checks_including_null_tags() {
    assert_checks_fixed_point(
        br#"[{"check":"app.ok","host_name":"","timestamp":0,"status":0,"message":"","tags":null},{"check":"db.up","host_name":"db1","timestamp":1700000000,"status":2,"message":"connection refused","tags":["env:prod","urgent","role:a","role:b"]},{"check":"x","host_name":"h","timestamp":1700000000,"status":3,"message":"","tags":[]}]"#,
    );
}

// -- generated bodies -----------------------------------------------------------------------------

/// A JSON leaf. Floats are quarter-integers, exact in binary, so the test doesn't depend on
/// `serde_json`'s float parsing being correctly rounded (without its `float_roundtrip` feature it
/// isn't always).
fn json_leaf() -> impl Strategy<Value = Json> {
    prop_oneof![
        Just(Json::Null),
        any::<bool>().prop_map(Json::Bool),
        any::<i64>().prop_map(|n| json!(n)),
        any::<u64>().prop_map(|n| json!(n)),
        (-10_000i32..10_000, 0u8..4).prop_map(|(n, q)| json!(n as f64 + q as f64 / 4.0)),
        "\\PC{0,12}".prop_map(Json::String),
    ]
}

fn json_value() -> impl Strategy<Value = Json> {
    json_leaf().prop_recursive(3, 24, 4, |inner| {
        prop_oneof![
            prop::collection::vec(inner.clone(), 0..4).prop_map(Json::Array),
            prop::collection::btree_map("[a-z.]{1,6}", inner, 0..4)
                .prop_map(|m| Json::Object(m.into_iter().collect())),
        ]
    })
}

fn opt<T: std::fmt::Debug + Clone>(
    s: impl Strategy<Value = T>,
) -> impl Strategy<Value = Option<T>> {
    prop::option::of(s)
}

fn log_timestamp() -> impl Strategy<Value = Option<Json>> {
    let ms = 1i64..4_000_000_000_000;
    opt(prop_oneof![
        ms.clone().prop_map(|ms| json!(ms)),
        ms.prop_map(|ms| json!(format_rfc3339_utc(ms * 1_000_000))),
    ])
}

fn log_item() -> impl Strategy<Value = Json> {
    (
        "\\PC{0,24}",
        opt(prop::sample::select(vec![
            "info", "warning", "error", "debug", "notice", "CRITICAL", "emerg", "ok", "",
        ])),
        log_timestamp(),
        opt("[a-z0-9.-]{0,10}"),
        opt("[a-z_]{0,8}"),
        opt("[a-z]{0,8}"),
        opt("[a-z:,0-9]{0,16}"),
        prop::collection::btree_map("x[a-z_.]{0,6}", json_value(), 0..5),
    )
        .prop_map(
            |(message, status, timestamp, hostname, service, ddsource, ddtags, extra)| {
                let mut obj = Map::new();
                obj.insert("message".into(), json!(message));
                for (key, value) in [
                    ("status", status.map(|s| json!(s))),
                    ("timestamp", timestamp),
                    ("hostname", hostname.map(Json::String)),
                    ("service", service.map(Json::String)),
                    ("ddsource", ddsource.map(Json::String)),
                    ("ddtags", ddtags.map(Json::String)),
                ] {
                    if let Some(value) = value {
                        obj.insert(key.into(), value);
                    }
                }
                obj.extend(extra);
                Json::Object(obj)
            },
        )
}

fn logs_body() -> impl Strategy<Value = Json> {
    prop_oneof![prop::collection::vec(log_item(), 1..5).prop_map(Json::Array), log_item(),]
}

/// Tags with repeated keys, bare tokens, empty values, and exact duplicates.
fn tags() -> impl Strategy<Value = Vec<String>> {
    prop::collection::vec("[a-c](:[a-z0-9]{0,3})?", 0..5)
}

/// One event in either shape: `agent` picks the Agent's field names.
fn event_item(agent: bool) -> impl Strategy<Value = Json> {
    (
        opt("\\PC{0,12}"),
        opt("\\PC{0,24}"),
        opt(1i64..2_000_000_000),
        opt(prop::sample::select(vec!["normal", "low"])),
        opt("[a-z0-9]{0,6}"),
        opt(tags()),
        opt(prop::sample::select(vec!["error", "warning", "info", "success", "user_update"])),
        opt("[a-z]{0,6}"),
        opt("[a-z]{0,6}"),
        opt("[a-z_]{0,6}"),
        opt(any::<i64>()),
    )
        .prop_filter("an event needs a title or a text", |t| t.0.is_some() || t.1.is_some())
        .prop_map(
            move |(
                title,
                text,
                ts,
                priority,
                host,
                tags,
                alert,
                agg,
                source,
                event_type,
                related,
            )| {
                let (title_key, text_key, ts_key) = if agent {
                    ("msg_title", "msg_text", "timestamp")
                } else {
                    ("title", "text", "date_happened")
                };
                let mut obj = Map::new();
                for (key, value) in [
                    (title_key, title.map(Json::String)),
                    (text_key, text.map(Json::String)),
                    (ts_key, ts.map(|t| json!(t))),
                    ("priority", priority.map(|p| json!(p))),
                    ("host", host.map(Json::String)),
                    ("tags", tags.map(|t| json!(t))),
                    ("alert_type", alert.map(|a| json!(a))),
                    ("aggregation_key", agg.map(Json::String)),
                    ("source_type_name", source.map(Json::String)),
                    (
                        if agent { "event_type" } else { "device_name" },
                        event_type.map(Json::String),
                    ),
                    ("related_event_id", if agent { None } else { related.map(|r| json!(r)) }),
                ] {
                    if let Some(value) = value {
                        obj.insert(key.into(), value);
                    }
                }
                Json::Object(obj)
            },
        )
}

fn envelope_body() -> impl Strategy<Value = Json> {
    (
        prop::collection::btree_map(
            prop::sample::select(vec!["api", "nagios", "jenkins"]),
            prop::collection::vec(event_item(true), 1..4),
            1..3,
        ),
        opt("[a-z0-9-]{0,10}"),
    )
        .prop_map(|(groups, hostname)| {
            let mut obj = Map::new();
            obj.insert("apiKey".into(), json!(""));
            obj.insert(
                "events".into(),
                Json::Object(groups.into_iter().map(|(k, v)| (k.to_string(), json!(v))).collect()),
            );
            if let Some(hostname) = hostname {
                obj.insert("internalHostname".into(), json!(hostname));
            }
            Json::Object(obj)
        })
}

fn service_check() -> impl Strategy<Value = Json> {
    ("[a-z][a-z._]{0,8}", "[a-z0-9]{0,6}", 0i64..2_000_000_000, 0u8..=3, "\\PC{0,16}", opt(tags()))
        .prop_map(|(check, host, ts, status, message, tags)| {
            json!({
                "check": check,
                "host_name": host,
                "timestamp": ts,
                "status": status,
                "message": message,
                "tags": tags,
            })
        })
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn generated_logs_are_a_fixed_point(body in logs_body()) {
        assert_logs_fixed_point(&serde_json::to_vec(&body).unwrap());
    }

    #[test]
    fn generated_envelopes_are_a_fixed_point(body in envelope_body()) {
        assert_events_fixed_point(&serde_json::to_vec(&body).unwrap(), EventFormat::AgentEnvelope);
    }

    #[test]
    fn generated_public_events_are_a_fixed_point(body in event_item(false)) {
        assert_events_fixed_point(&serde_json::to_vec(&body).unwrap(), EventFormat::PublicV1);
    }

    #[test]
    fn generated_service_checks_are_a_fixed_point(
        body in prop::collection::vec(service_check(), 1..5)
    ) {
        assert_checks_fixed_point(&serde_json::to_vec(&body).unwrap());
    }
}
