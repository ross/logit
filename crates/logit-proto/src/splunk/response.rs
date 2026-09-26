//! HEC's JSON response bodies, both sides: the `{"text":…,"code":N}` status a listener answers
//! and a sink reads, and the `/services/collector/ack` request and reply. The bodies are written
//! byte-exact, with no whitespace, in Splunk's key order (`text`, `code`, then `ackId` or
//! `invalid-event-number`), so a HEC client's error handling reads a `logit` answer as it reads
//! Splunk's. Lives in the codec rather than in `logit-inputs`/`logit-outputs` because only this
//! crate depends on `serde_json`.

use crate::json::{write_str, JsonObject};
use serde_json::Value as Json;
use std::collections::BTreeMap;

/// One HEC status: the body's `code`, the HTTP status Splunk sends it with, and its `text`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HecStatus {
    pub code: u16,
    pub http: u16,
    pub text: &'static str,
}

impl HecStatus {
    pub const SUCCESS: HecStatus = HecStatus { code: 0, http: 200, text: "Success" };
    pub const TOKEN_DISABLED: HecStatus = HecStatus { code: 1, http: 403, text: "Token disabled" };
    pub const TOKEN_REQUIRED: HecStatus =
        HecStatus { code: 2, http: 401, text: "Token is required" };
    pub const INVALID_AUTHORIZATION: HecStatus =
        HecStatus { code: 3, http: 401, text: "Invalid authorization" };
    pub const INVALID_TOKEN: HecStatus = HecStatus { code: 4, http: 403, text: "Invalid token" };
    pub const NO_DATA: HecStatus = HecStatus { code: 5, http: 400, text: "No data" };
    pub const INVALID_DATA_FORMAT: HecStatus =
        HecStatus { code: 6, http: 400, text: "Invalid data format" };
    pub const INCORRECT_INDEX: HecStatus =
        HecStatus { code: 7, http: 400, text: "Incorrect index" };
    pub const INTERNAL_SERVER_ERROR: HecStatus =
        HecStatus { code: 8, http: 500, text: "Internal server error" };
    pub const SERVER_BUSY: HecStatus = HecStatus { code: 9, http: 503, text: "Server is busy" };
    pub const CHANNEL_MISSING: HecStatus =
        HecStatus { code: 10, http: 400, text: "Data channel is missing" };
    pub const INVALID_CHANNEL: HecStatus =
        HecStatus { code: 11, http: 400, text: "Invalid data channel" };
    pub const EVENT_FIELD_REQUIRED: HecStatus =
        HecStatus { code: 12, http: 400, text: "Event field is required" };
    pub const EVENT_FIELD_BLANK: HecStatus =
        HecStatus { code: 13, http: 400, text: "Event field cannot be blank" };
    pub const ACK_DISABLED: HecStatus = HecStatus { code: 14, http: 400, text: "ACK is disabled" };
    pub const INDEXED_FIELDS_ERROR: HecStatus =
        HecStatus { code: 15, http: 400, text: "Error in handling indexed fields" };
    pub const QUERY_STRING_AUTH_DISABLED: HecStatus =
        HecStatus { code: 16, http: 400, text: "Query string authorization is not enabled" };
    pub const HEALTHY: HecStatus = HecStatus { code: 17, http: 200, text: "HEC is healthy" };
    // Codes 18 through 27: the codes and HTTP statuses are Splunk's documented ones; each `text`
    // is the best reading of Splunk's documentation. No run against a real Splunk has provoked
    // one, so the texts are unverified (ADR `splunk-hec-relay`, "Amendment: what W5's recorded
    // traffic and the Splunk run settled"; `docs/plans/splunk-relay.md`, "Settled by W5"). Codes
    // 21, 22, 24, and 25 aren't modeled.
    pub const UNHEALTHY_QUEUES_FULL: HecStatus =
        HecStatus { code: 18, http: 503, text: "HEC is unhealthy, queues are full" };
    pub const UNHEALTHY_ACK_UNAVAILABLE: HecStatus =
        HecStatus { code: 19, http: 503, text: "HEC is unhealthy, ack service unavailable" };
    pub const UNHEALTHY_QUEUES_FULL_ACK_UNAVAILABLE: HecStatus = HecStatus {
        code: 20,
        http: 503,
        text: "HEC is unhealthy, queues are full, ack service unavailable",
    };
    pub const SHUTTING_DOWN: HecStatus =
        HecStatus { code: 23, http: 503, text: "Server is shutting down" };
    pub const QUEUE_AT_CAPACITY: HecStatus =
        HecStatus { code: 26, http: 429, text: "Queue at capacity" };
    pub const PERFORMANCE_LIMIT_REACHED: HecStatus =
        HecStatus { code: 27, http: 429, text: "Performance limit reached" };
    /// Splunk Cloud Platform's answer to a request without a channel on a `useACK` token, where
    /// Splunk Enterprise answers code 10. The text is verbatim from Splunk Cloud 10.5.2605.9
    /// (`docs/plans/splunk-relay.md`, "Settled by the Cloud run (2026-09-26)").
    pub const CHANNEL_MISSING_STICKY_LB: HecStatus = HecStatus {
        code: 28,
        http: 400,
        text: "Data channel is missing. If you have multiple indexers, sticky session load \
               balancers must be provisioned and client requests must be routed accordingly.",
    };

    /// Every status above, in code order.
    pub const ALL: [HecStatus; 25] = [
        Self::SUCCESS,
        Self::TOKEN_DISABLED,
        Self::TOKEN_REQUIRED,
        Self::INVALID_AUTHORIZATION,
        Self::INVALID_TOKEN,
        Self::NO_DATA,
        Self::INVALID_DATA_FORMAT,
        Self::INCORRECT_INDEX,
        Self::INTERNAL_SERVER_ERROR,
        Self::SERVER_BUSY,
        Self::CHANNEL_MISSING,
        Self::INVALID_CHANNEL,
        Self::EVENT_FIELD_REQUIRED,
        Self::EVENT_FIELD_BLANK,
        Self::ACK_DISABLED,
        Self::INDEXED_FIELDS_ERROR,
        Self::QUERY_STRING_AUTH_DISABLED,
        Self::HEALTHY,
        Self::UNHEALTHY_QUEUES_FULL,
        Self::UNHEALTHY_ACK_UNAVAILABLE,
        Self::UNHEALTHY_QUEUES_FULL_ACK_UNAVAILABLE,
        Self::SHUTTING_DOWN,
        Self::QUEUE_AT_CAPACITY,
        Self::PERFORMANCE_LIMIT_REACHED,
        Self::CHANNEL_MISSING_STICKY_LB,
    ];

    /// The status with body code `code`, when it is one of [`HecStatus::ALL`].
    pub fn from_code(code: u16) -> Option<HecStatus> {
        Self::ALL.into_iter().find(|s| s.code == code)
    }
}

/// A parsed HEC response body. `code` and `text` are whatever the server sent, known code or not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HecReply {
    pub code: u16,
    pub text: String,
    pub ack_id: Option<u64>,
    pub invalid_event_number: Option<u64>,
}

/// `{"text":"<text>","code":<code>}`.
pub fn encode_status(status: HecStatus) -> Vec<u8> {
    status_body(status.text, status.code, |_| {})
}

/// A success body; with `ack_id`, `{"text":"Success","code":0,"ackId":<id>}`, what a `useACK`
/// token's POST answers.
pub fn encode_success(ack_id: Option<u64>) -> Vec<u8> {
    let status = HecStatus::SUCCESS;
    status_body(status.text, status.code, |obj| {
        if let Some(id) = ack_id {
            obj.key("ackId").extend_from_slice(id.to_string().as_bytes());
        }
    })
}

/// `status`'s body plus `"invalid-event-number":<n>`, the index of the first rejected object in
/// a batch (Splunk's code 6 answer to a malformed `/event` body).
pub fn encode_invalid_event(status: HecStatus, n: u64) -> Vec<u8> {
    encode_invalid_event_acked(status, n, None)
}

/// [`encode_invalid_event`] plus, with `ack_id`, `"ackId":<id>` after `invalid-event-number`:
/// Splunk's answer, and `splunk_hec_in`'s, to a request with a channel whose objects before `n`
/// were indexed (`tools/splunk-interop/README.md`'s code 6 probe).
pub fn encode_invalid_event_acked(status: HecStatus, n: u64, ack_id: Option<u64>) -> Vec<u8> {
    status_body(status.text, status.code, |obj| {
        obj.key("invalid-event-number").extend_from_slice(n.to_string().as_bytes());
        if let Some(id) = ack_id {
            obj.key("ackId").extend_from_slice(id.to_string().as_bytes());
        }
    })
}

/// An HTTP-level error Splunk answers without a HEC code (404, 405, 413, 415): the body's `code`
/// is the HTTP status itself.
pub fn encode_http_error(http: u16, text: &str) -> Vec<u8> {
    status_body(text, http, |_| {})
}

fn status_body(text: &str, code: u16, extra: impl FnOnce(&mut JsonObject<'_>)) -> Vec<u8> {
    let mut out = Vec::new();
    let mut obj = JsonObject::begin(&mut out);
    write_str(obj.key("text"), text);
    obj.key("code").extend_from_slice(code.to_string().as_bytes());
    extra(&mut obj);
    obj.finish();
    out
}

/// Reads a HEC status body. `None` when it isn't a JSON object with an integer `code`; a missing
/// `text` reads as empty.
pub fn parse_reply(body: &[u8]) -> Option<HecReply> {
    let Json::Object(obj) = serde_json::from_slice::<Json>(body).ok()? else { return None };
    let code = u16::try_from(obj.get("code")?.as_u64()?).ok()?;
    let text = obj.get("text").and_then(Json::as_str).unwrap_or_default().to_string();
    Some(HecReply {
        code,
        text,
        ack_id: obj.get("ackId").and_then(Json::as_u64),
        invalid_event_number: obj.get("invalid-event-number").and_then(Json::as_u64),
    })
}

/// Reads an `/ack` request, `{"acks":[<id>,…]}`, into its ids in request order. `None` when the
/// body isn't that shape.
pub fn parse_ack_request(body: &[u8]) -> Option<Vec<u64>> {
    let Json::Object(obj) = serde_json::from_slice::<Json>(body).ok()? else { return None };
    obj.get("acks")?.as_array()?.iter().map(Json::as_u64).collect()
}

/// `{"acks":[<id>,…]}`, a sink's poll.
pub fn encode_ack_request(ids: &[u64]) -> Vec<u8> {
    let mut out = Vec::from(&b"{\"acks\":["[..]);
    for (i, id) in ids.iter().enumerate() {
        if i > 0 {
            out.push(b',');
        }
        out.extend_from_slice(id.to_string().as_bytes());
    }
    out.extend_from_slice(b"]}");
    out
}

/// `{"acks":{"<id>":<bool>,…}}`, a listener's answer to a poll, in the order given.
pub fn encode_ack_reply(acks: &[(u64, bool)]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut obj = JsonObject::begin(&mut out);
    let inner = obj.key("acks");
    let mut acks_obj = JsonObject::begin(inner);
    for (id, acked) in acks {
        let slot = acks_obj.key(&id.to_string());
        slot.extend_from_slice(if *acked { b"true" } else { b"false" });
    }
    acks_obj.finish();
    obj.finish();
    out
}

/// Reads an `/ack` reply into `(id, acked)` pairs, ascending by id. `None` when the body isn't
/// that shape (a key that isn't a decimal id, or a value that isn't a bool).
pub fn parse_ack_reply(body: &[u8]) -> Option<Vec<(u64, bool)>> {
    let Json::Object(obj) = serde_json::from_slice::<Json>(body).ok()? else { return None };
    let Json::Object(acks) = obj.get("acks")? else { return None };
    let mut out = BTreeMap::new();
    for (id, acked) in acks {
        out.insert(id.parse::<u64>().ok()?, acked.as_bool()?);
    }
    Some(out.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(body: Vec<u8>) -> String {
        String::from_utf8(body).unwrap()
    }

    #[test]
    fn status_bodies_are_byte_exact() {
        assert_eq!(text(encode_status(HecStatus::SUCCESS)), r#"{"text":"Success","code":0}"#);
        assert_eq!(
            text(encode_status(HecStatus::HEALTHY)),
            r#"{"text":"HEC is healthy","code":17}"#
        );
        assert_eq!(text(encode_success(None)), r#"{"text":"Success","code":0}"#);
        assert_eq!(text(encode_success(Some(7))), r#"{"text":"Success","code":0,"ackId":7}"#);
        assert_eq!(
            text(encode_invalid_event(HecStatus::INVALID_DATA_FORMAT, 2)),
            r#"{"text":"Invalid data format","code":6,"invalid-event-number":2}"#
        );
        assert_eq!(text(encode_http_error(404, "Not Found")), r#"{"text":"Not Found","code":404}"#);
    }

    /// Splunk Cloud 10.5.2605.9's answer to a `useACK` token's request without a channel.
    #[test]
    fn code_28_is_splunk_clouds_channel_missing_body() {
        let cloud = r#"{"text":"Data channel is missing. If you have multiple indexers, sticky session load balancers must be provisioned and client requests must be routed accordingly.","code":28}"#;
        assert_eq!(text(encode_status(HecStatus::CHANNEL_MISSING_STICKY_LB)), cloud);
        let reply = parse_reply(cloud.as_bytes()).expect("parses");
        assert_eq!(HecStatus::from_code(reply.code), Some(HecStatus::CHANNEL_MISSING_STICKY_LB));
        assert_eq!(HecStatus::CHANNEL_MISSING_STICKY_LB.http, 400);
    }

    #[test]
    fn every_status_round_trips_through_parse_reply() {
        let mut codes = Vec::new();
        for status in HecStatus::ALL {
            let reply = parse_reply(&encode_status(status)).expect("parses");
            assert_eq!(reply.code, status.code);
            assert_eq!(reply.text, status.text);
            assert_eq!(reply.ack_id, None);
            assert_eq!(HecStatus::from_code(reply.code), Some(status));
            codes.push(status.code);
        }
        let mut sorted = codes.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(codes, sorted, "ALL is in code order, with no duplicate");
        assert_eq!(HecStatus::from_code(21), None);
    }

    #[test]
    fn replies_carry_ack_ids_and_invalid_event_numbers() {
        assert_eq!(
            parse_reply(&encode_success(Some(42))),
            Some(HecReply {
                code: 0,
                text: "Success".into(),
                ack_id: Some(42),
                invalid_event_number: None
            })
        );
        assert_eq!(
            parse_reply(&encode_invalid_event(HecStatus::INVALID_DATA_FORMAT, 3))
                .unwrap()
                .invalid_event_number,
            Some(3)
        );
        assert_eq!(parse_reply(b"not json"), None);
        assert_eq!(parse_reply(br#"{"text":"x"}"#), None);
        assert_eq!(parse_reply(br#"{"code":9}"#).unwrap().text, "");
    }

    #[test]
    fn ack_requests_and_replies_round_trip() {
        let request = encode_ack_request(&[3, 1, 2]);
        assert_eq!(text(request.clone()), r#"{"acks":[3,1,2]}"#);
        assert_eq!(parse_ack_request(&request), Some(vec![3, 1, 2]));
        assert_eq!(text(encode_ack_request(&[])), r#"{"acks":[]}"#);
        assert_eq!(parse_ack_request(br#"{"acks":["x"]}"#), None);
        assert_eq!(parse_ack_request(br#"{"acks":{}}"#), None);

        let reply = encode_ack_reply(&[(3, true), (1, false)]);
        assert_eq!(text(reply.clone()), r#"{"acks":{"3":true,"1":false}}"#);
        assert_eq!(parse_ack_reply(&reply), Some(vec![(1, false), (3, true)]));
        assert_eq!(parse_ack_reply(br#"{"acks":{"a":true}}"#), None);
        assert_eq!(parse_ack_reply(br#"{"acks":{"1":1}}"#), None);
    }
}
