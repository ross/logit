//! InfluxDB 2.x line-protocol output: `POST /api/v2/write` with org/bucket query params and a
//! `Token` auth header. Matches the `influxdb` service seeded in `compose.yaml`.
//!
//! The reference `Encoder` sink: [`InfluxLineEncoder`] (`logit_proto::influxdb::encode`, whose
//! module doc and its parent's "Encode" section are the mapping) renders one opaque body per
//! batch, and [`InfluxDbOutput::send`] makes one attempt and classifies the outcome as a
//! [`logit_pipeline::Fault`]; retry timing belongs to `logit-pipeline`'s writer
//! (`docs/adr/buffered-sink-delivery.md`). The sink emits the encoder's [`EncodeStats`] as
//! telemetry once per batch ([`emit_encode_stats`]).
//!
//! The client, the status bucket, and the transport classifier are [`crate::http`]'s, so
//! redirects are off ([`crate::http::build_client`] says why) and a `3xx` is the misconfigured
//! URL it names.
//!
//! **Delivery posture.** The default, `at_least_once` (`docs/adr/delivery-semantics.md`, item 5),
//! retries an `Ambiguous` attempt, and a resend overwrites. Every point's timestamp derives from
//! `event.timestamp`, and the encoder's per-batch collision map is cleared at the top of every
//! `encode`, so a retry re-encodes the same body byte for byte, and InfluxDB
//! treats an identical `(measurement, tag set, timestamp)` write as an overwrite, not a second
//! point.
//!
//! ## Response classes
//!
//! A non-2xx answer carries InfluxDB's error JSON (`code` and `message`; a parse error's message
//! quotes each offending line), which the error and the `send_failed` warning quote, read bounded
//! ([`crate::http::read_body_prefix`]). On `/api/v2/write` each documented status maps to one
//! `code`, so the status decides the class and every row is [`crate::http::classify_status`]'s:
//! no documented `400` names an unknown bucket or org, and no documented `404` names the body.
//!
//! | Response | Class | Why | Evidence |
//! |---|---|---|---|
//! | `204` | `Ok` | every point written (OSS) or queued (Cloud) | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | `400` `invalid` (`unable to parse '<line>': <reason>`) | `Rejected` | malformed line protocol: "all data rejected", and a resend parses the same | [troubleshoot writes](https://docs.influxdata.com/influxdb/v2/write-data/troubleshoot/) |
//! | `401` `unauthorized` (`unauthorized access`) | `Refused` | a missing or unknown token, or one without access to the org and bucket: every batch gets it | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | `403` `forbidden` (`insufficient permissions for write`) | `Refused` | the token can't write the bucket | `checkBucketWritePermissions` in [`http/write_handler.go`](https://github.com/influxdata/influxdb/blob/master/http/write_handler.go) |
//! | `404` `not found` (`bucket not found`, `organization name "..." not found`) | `Refused` | the configured `org` or `bucket` doesn't exist: every batch gets it until the operator creates it | [write API](https://docs.influxdata.com/influxdb/v2/api/write/), `decodeWriteRequest` in [`http/write_handler.go`](https://github.com/influxdata/influxdb/blob/master/http/write_handler.go) |
//! | `413` | `Rejected` | the body is over the server's limit, "no data written"; a smaller batch would land | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | `422` `unprocessable entity` (`failure writing points to database`, a partial write such as points outside the bucket's retention) | `Rejected` | the server applied what it could and refused the rest for its content; a resend repeats the refusal and overwrites what landed | [write API](https://docs.influxdata.com/influxdb/v2/api/write/), [troubleshoot writes](https://docs.influxdata.com/influxdb/v2/write-data/troubleshoot/) |
//! | `429` `too many requests` (Cloud) | `Ambiguous` | a rate or quota limit, `Retry-After` set; the next attempt may land | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | `500` `internal error` | `Ambiguous` | "unexpected error writing points"; may have been applied | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | `503` `unavailable` | `Ambiguous` | the server can't take writes for now, `Retry-After` set | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | any other status | [`crate::http::classify_status`]'s class | undocumented on this route | -- |
//! | any `3xx` | `Rejected` | redirects are off: a moved endpoint is a config change | [`crate::http::build_client`] |
//! | connect failure, DNS failure | `Clean` | nothing left the process | [`crate::http::classify_reqwest_error`] |
//! | timeout, any other transport error | `Ambiguous` | the write may have reached the server | [`crate::http::classify_reqwest_error`] |
//!
//! `Retry-After` is ignored: `write_loop`'s backoff applies.

use crate::accounting::BatchAccounting;
use crate::http::{body_snippet, read_body_prefix, ERROR_BODY_SNIPPET_BYTES};
use crate::Output;
use anyhow::Context;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{BatchContext, DeliveryPosture, SeqId};
pub use logit_proto::influxdb::encode::{EncodeStats, InfluxLineEncoder};
use logit_proto::Encoder;
use std::time::Duration;

/// `reqwest` has no request timeout by default; without one, a server that accepts the connection
/// but never responds hangs `send`, and the pipeline worker driving it, forever.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

pub struct InfluxDbOutput {
    url: String,
    org: String,
    bucket: String,
    token: String,
    client: reqwest::Client,
    encoder: InfluxLineEncoder,
    /// Per-request timeout: what `with_timeout` set, else [`DEFAULT_TIMEOUT`]. `send` makes one
    /// attempt, so there's no retry budget to clamp it against.
    request_timeout: Duration,
    /// Layer-3 detail (`docs/design/internal-telemetry.md`): the response class, which
    /// `run_output`'s `logit.component.send.*` can't see inside one `send`. Ungated; the encoder's
    /// diagnostics are gated by `accounting` (`crate::accounting`).
    telemetry: Telemetry,
    accounting: BatchAccounting,
}

impl InfluxDbOutput {
    pub fn new(url: String, org: String, bucket: String, token: String) -> Self {
        Self {
            url,
            org,
            bucket,
            token,
            client: crate::http::build_client(DEFAULT_TIMEOUT, None),
            encoder: InfluxLineEncoder::default(),
            request_timeout: DEFAULT_TIMEOUT,
            telemetry: Telemetry::default(),
            accounting: BatchAccounting::default(),
        }
    }

    /// Overrides the default 10s request timeout. Rebuilds the underlying HTTP client.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.client = crate::http::build_client(timeout, None);
        self.request_timeout = timeout;
        self
    }

    /// Attaches the encoder's diagnostics handle (per-metric encode failures), gated by this
    /// sink's batch accounting. Retry diagnostics come from `logit-pipeline`'s writer, not this
    /// sink.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.encoder = self.encoder.with_diagnostics(diag.gated(self.accounting.gate()));
        self
    }

    /// Attaches the layer-3 telemetry handle.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

impl InfluxDbOutput {
    /// One attempt per call, no loop or sleep: retry timing and budget belong to
    /// `logit-pipeline`'s writer (`docs/adr/buffered-sink-delivery.md`). This classifies the
    /// outcome and attaches it as `.context(fault)`.
    async fn attempt(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let (first, body) = self.accounting.encode(0, || self.encoder.encode(batch));
        let body = body?;
        // Before the empty-body return: a batch whose every line was unencodable still dropped
        // or normalized what it dropped, and an operator chasing a missing point needs to see it.
        if first {
            emit_encode_stats(&self.telemetry, self.encoder.stats());
        }
        if body.is_empty() {
            // Nothing had a line-protocol encoding (e.g. log- or span-only events, or every
            // metric was skipped). Not an error; nothing to write.
            return Ok(());
        }

        if first {
            self.telemetry.count("logit.output.batch.bytes", body.len() as f64, &[]);
        }

        let write_url = format!("{}/api/v2/write", self.url.trim_end_matches('/'));

        let request_timer = self.telemetry.timer("logit.output.request.duration");
        let result = self
            .client
            .post(&write_url)
            .query(&[
                ("org", self.org.as_str()),
                ("bucket", self.bucket.as_str()),
                ("precision", "ns"),
            ])
            .header("Authorization", format!("Token {}", self.token))
            .header("Content-Type", "text/plain; charset=utf-8")
            .timeout(self.request_timeout)
            .body(body)
            .send()
            .await;
        drop(request_timer);

        match result {
            Ok(resp) if resp.status().is_success() => {
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("class", crate::http::status_class(resp.status()))],
                );
                Ok(())
            }
            Ok(resp) => {
                let status = resp.status();
                self.telemetry.count(
                    "logit.output.requests",
                    1.0,
                    &[("class", crate::http::status_class(status))],
                );
                // A bounded read, not `text()`: see `crate::http::read_body_prefix`.
                let text = body_snippet(
                    &read_body_prefix(resp, ERROR_BODY_SNIPPET_BYTES).await,
                    ERROR_BODY_SNIPPET_BYTES,
                );
                let fault = crate::http::classify_status(status);
                Err(anyhow::anyhow!("InfluxDB write failed ({status}): {text}")).context(fault)
            }
            Err(err) => {
                self.telemetry.count("logit.output.requests", 1.0, &[("class", "network_error")]);
                let fault = crate::http::classify_reqwest_error(&err);
                Err(anyhow::Error::new(err)).context(fault)
            }
        }
    }
}

#[async_trait::async_trait]
impl Output for InfluxDbOutput {
    /// Arms this sink's batch accounting (`crate::accounting`).
    fn observe_batch(&mut self, _ctx: BatchContext, _seq: SeqId) {
        self.accounting.observe();
    }

    fn observe_posture(&mut self, posture: DeliveryPosture) {
        self.accounting.observe_posture(posture);
    }

    /// One attempt ([`InfluxDbOutput::attempt`]). A final result (`Ok`, or a fault
    /// `write_loop` won't retry) disarms the batch accounting on every path, a batch that encoded
    /// to nothing included.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let result = self.attempt(batch).await;
        self.accounting.finish(result)
    }
}

/// Emits one batch's [`EncodeStats`] as `logit.output.*` counters, each only when non-zero so an
/// ordinary batch doesn't upsert a permanent zero series.
fn emit_encode_stats(telemetry: &Telemetry, stats: &EncodeStats) {
    const TAGS_NORMALIZED: &str = "logit.output.tags.normalized";
    const TAGS_DROPPED: &str = "logit.output.tags.dropped";
    const NORMALIZED: &str = "logit.output.metrics.normalized";
    const SKIPPED: &str = "logit.output.metrics.skipped";
    let rows: [(usize, &str, (&str, &str)); 17] = [
        (stats.tags_normalized_multi_value, TAGS_NORMALIZED, ("reason", "multi_value")),
        (stats.tags_dropped_unrepresentable, TAGS_DROPPED, ("reason", "unrepresentable")),
        (stats.tags_dropped_empty, TAGS_DROPPED, ("reason", "empty")),
        (stats.tags_dropped_unencodable, TAGS_DROPPED, ("reason", "unencodable")),
        (stats.tags_dropped_reserved, TAGS_DROPPED, ("reason", "reserved")),
        (stats.metrics_normalized_carrier_mismatch, NORMALIZED, ("reason", "carrier_mismatch")),
        (stats.skipped_unencodable_value, SKIPPED, ("reason", "unencodable_value")),
        (stats.skipped_unencodable_key, SKIPPED, ("reason", "unencodable_key")),
        (stats.skipped_reserved_key, SKIPPED, ("reason", "reserved_key")),
        (stats.skipped_oversize_value, SKIPPED, ("reason", "oversize_value")),
        (stats.skipped_no_fields, SKIPPED, ("reason", "no_fields")),
        (stats.skipped_no_recorded_value, SKIPPED, ("reason", "no_recorded_value")),
        (stats.skipped_set_members, SKIPPED, ("metric_kind", "set_members")),
        (stats.skipped_exponential_histogram, SKIPPED, ("metric_kind", "exponential_histogram")),
        (stats.skipped_gauge_delta, SKIPPED, ("metric_kind", "gauge_delta")),
        (stats.skipped_comment_measurement, SKIPPED, ("reason", "comment_measurement")),
        (stats.skipped_timestamp_overflow, SKIPPED, ("reason", "timestamp_overflow")),
    ];
    for (count, name, label) in rows {
        if count > 0 {
            telemetry.count(name, count as f64, &[label]);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use logit_core::{
        AttrMap, BodyFormat, Event, LogRecord, MetricKind, MetricRecord, Resource, Value,
    };
    use logit_pipeline::Fault;
    use std::sync::Arc;

    fn batch_with(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn metric_event(name: &str, kind: MetricKind, attrs: &[(&str, &str)]) -> Event {
        let mut attributes = AttrMap::new();
        for (k, v) in attrs {
            attributes.insert(k, *v);
        }
        Event::metric(
            1_700_000_000_000_000_000,
            attributes,
            MetricRecord::new(logit_core::interner::intern(name), kind),
        )
    }

    fn encode(events: Vec<Event>) -> String {
        let mut encoder = InfluxLineEncoder::default();
        let bytes = encoder.encode(&batch_with(events)).expect("encode should succeed");
        String::from_utf8(bytes.to_vec()).expect("output should be valid utf-8")
    }

    /// A `Bytes` attribute stays out of tags after a Lua stage reassigns every attribute
    /// (`docs/design/lua-value-type-preservation.md`). Here, not beside the encoder, because
    /// `logit-proto` can't depend on `logit-script`.
    #[test]
    fn bytes_attribute_stays_excluded_from_tags_after_a_lua_enrichment_stage() {
        let worker = logit_script::ScriptWorker::new(
            r#"
            function process(event)
                local attrs = event:to_table().attributes
                for k, v in pairs(attrs) do
                    event.attributes[k] = v
                end
                event.attributes.env = "prod"
                return event
            end
            "#,
        )
        .expect("script should load");

        let mut event = metric_event("page.views", MetricKind::counter(1.0), &[]);
        event.attributes.insert("host", Value::Bytes(Bytes::from_static(b"web-01")));
        let event = match worker.process(event).expect("process should succeed") {
            logit_script::ProcessOutcome::Emit(event, _) => *event,
            _ => panic!("expected the script to emit the event unchanged"),
        };

        let out = encode(vec![event]);
        assert!(out.contains("env=prod"), "the new Str tag should be written: {out}");
        assert!(
            !out.contains("host="),
            "a Bytes attribute must stay excluded from tags even after an unrelated Lua stage \
             reads and reassigns it: {out}"
        );
    }

    /// A bare HTTP/1.1 server: one canned response per connection (the last repeats), then close.
    /// `Connection: close` forces a fresh connection per request, so the counter equals the
    /// number of `send` calls that reached it.
    async fn canned_server(
        responses: Vec<&'static str>,
    ) -> (std::net::SocketAddr, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let count_task = count.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let i = count_task.fetch_add(1, Ordering::SeqCst);
                let response = responses.get(i).or(responses.last()).copied().unwrap_or("");
                let mut buf = [0u8; 8192];
                // Drain some of the request so the client's write can't block; the timeout keeps
                // a silent client from wedging this task.
                let _ =
                    tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf)).await;
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        (addr, count)
    }

    const RESP_204: &str = "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
    const RESP_400: &str =
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_400_WITH_BODY: &str =
        "HTTP/1.1 400 Bad Request\r\nContent-Length: 22\r\nConnection: close\r\n\r\nunable to parse points";
    const RESP_401: &str =
        "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_429: &str =
        "HTTP/1.1 429 Too Many Requests\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
    const RESP_503: &str =
        "HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

    async fn output_against(addr: std::net::SocketAddr) -> InfluxDbOutput {
        InfluxDbOutput::new(format!("http://{addr}"), "org".into(), "bucket".into(), "token".into())
    }

    fn one_metric_batch() -> EventBatch {
        batch_with(vec![metric_event("x", MetricKind::counter(1.0), &[])])
    }

    /// A success is `Ok(())` after one attempt.
    #[tokio::test]
    async fn a_successful_response_returns_ok_on_the_first_attempt() {
        let (addr, count) = canned_server(vec![RESP_204]).await;
        let mut output = output_against(addr).await;

        output.send(&one_metric_batch()).await.expect("a 204 should succeed");
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_503_response_is_classified_ambiguous() {
        let (addr, count) = canned_server(vec![RESP_503]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 503 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
    }

    #[tokio::test]
    async fn a_429_rate_limit_response_is_classified_ambiguous() {
        let (addr, count) = canned_server(vec![RESP_429]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 429 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
    }

    #[tokio::test]
    async fn a_400_response_is_classified_rejected() {
        let (addr, count) = canned_server(vec![RESP_400]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 400 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Rejected);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
    }

    #[tokio::test]
    async fn a_401_response_is_classified_refused() {
        let (addr, count) = canned_server(vec![RESP_401]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 401 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Refused);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
    }

    /// A canned response carrying InfluxDB's error JSON, leaked so [`canned_server`] can hold it.
    fn influx_error(status: &str, body: &str) -> &'static str {
        let response = format!(
            "HTTP/1.1 {status}\r\nContent-Type: application/json; charset=utf-8\r\n\
             Content-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        Box::leak(response.into_boxed_str())
    }

    /// The class `send` reads from one canned InfluxDB answer, and the error's text.
    async fn class_of(response: &'static str) -> (Fault, String) {
        let (addr, count) = canned_server(vec![response]).await;
        let mut output = output_against(addr).await;
        let err = output.send(&one_metric_batch()).await.expect_err("a non-2xx fails send");
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "one attempt");
        (logit_pipeline::classify(&err), format!("{err:#}"))
    }

    // One test per row of the module doc's "Response classes" table, each answering the body
    // InfluxDB documents for it.

    #[tokio::test]
    async fn a_400_invalid_parse_error_is_rejected() {
        // The shape `influxd` 2.9.1 answers (`docs/plans/influxdb-relay.md`'s "Settled by probe",
        // row 13.1): no `line` field.
        let body =
            r#"{"code":"invalid","message":"unable to parse 'x value=': missing field value"}"#;
        let (fault, text) = class_of(influx_error("400 Bad Request", body)).await;
        assert_eq!(fault, Fault::Rejected);
        assert!(text.contains("unable to parse"), "the message is quoted: {text}");
    }

    #[tokio::test]
    async fn a_401_unauthorized_is_refused() {
        let body = r#"{"code":"unauthorized","message":"unauthorized access"}"#;
        assert_eq!(class_of(influx_error("401 Unauthorized", body)).await.0, Fault::Refused);
    }

    #[tokio::test]
    async fn a_403_forbidden_is_refused() {
        let body = r#"{"code":"forbidden","message":"insufficient permissions for write"}"#;
        assert_eq!(class_of(influx_error("403 Forbidden", body)).await.0, Fault::Refused);
    }

    #[tokio::test]
    async fn a_404_bucket_not_found_is_refused() {
        let body = r#"{"code":"not found","message":"bucket not found"}"#;
        let (fault, text) = class_of(influx_error("404 Not Found", body)).await;
        assert_eq!(fault, Fault::Refused);
        assert!(text.contains("bucket not found"), "the message is quoted: {text}");
    }

    #[tokio::test]
    async fn a_404_organization_not_found_is_refused() {
        let body = r#"{"code":"not found","message":"organization name \"org\" not found"}"#;
        assert_eq!(class_of(influx_error("404 Not Found", body)).await.0, Fault::Refused);
    }

    #[tokio::test]
    async fn a_413_too_large_is_rejected() {
        let body = r#"{"code":"request too large","message":"unable to read data: points batch is too large"}"#;
        let status = "413 Payload Too Large";
        assert_eq!(class_of(influx_error(status, body)).await.0, Fault::Rejected);
    }

    #[tokio::test]
    async fn a_422_unprocessable_entity_is_rejected() {
        let body = r#"{"code":"unprocessable entity","message":"failure writing points to database: partial write: points beyond retention policy dropped=1"}"#;
        let status = "422 Unprocessable Entity";
        assert_eq!(class_of(influx_error(status, body)).await.0, Fault::Rejected);
    }

    #[tokio::test]
    async fn a_429_too_many_requests_is_ambiguous() {
        let body = r#"{"code":"too many requests","message":"org exceeded write limits"}"#;
        let status = "429 Too Many Requests";
        assert_eq!(class_of(influx_error(status, body)).await.0, Fault::Ambiguous);
    }

    #[tokio::test]
    async fn a_500_internal_error_is_ambiguous() {
        let body = r#"{"code":"internal error","message":"unexpected error writing points to database","err":"engine: timeout"}"#;
        let status = "500 Internal Server Error";
        assert_eq!(class_of(influx_error(status, body)).await.0, Fault::Ambiguous);
    }

    /// A redirect isn't followed: [`class_of`] asserts the server saw one request.
    #[tokio::test]
    async fn a_3xx_is_rejected_and_not_followed() {
        let response = "HTTP/1.1 302 Found\r\nLocation: /api/v2/write\r\nContent-Length: 0\r\n\
                        Connection: close\r\n\r\n";
        assert_eq!(class_of(response).await.0, Fault::Rejected);
    }

    #[tokio::test]
    async fn a_503_unavailable_is_ambiguous() {
        let body = r#"{"code":"unavailable","message":"service unavailable"}"#;
        let status = "503 Service Unavailable";
        assert_eq!(class_of(influx_error(status, body)).await.0, Fault::Ambiguous);
    }

    /// A rejection body is quoted verbatim in the error message when it fits the snippet bound.
    #[tokio::test]
    async fn a_short_error_body_is_quoted_verbatim() {
        let (addr, count) = canned_server(vec![RESP_400_WITH_BODY]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 400 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Rejected);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
        // `.context(fault)` makes `err`'s own `Display` the `Fault` alone; the send message with
        // the quoted body is the wrapped cause.
        let message = err.root_cause().to_string();
        assert!(message.contains("unable to parse points"), "got: {message}");
        assert!(!message.ends_with("..."), "a body under the bound isn't truncated: {message}");
    }

    /// A rejection body past the snippet bound is read only up to the bound (`read_body_prefix`),
    /// not buffered whole, and quoted with an ellipsis.
    #[tokio::test]
    async fn an_oversized_error_body_is_quoted_truncated() {
        let body = "x".repeat(64 * 1024);
        let response = format!(
            "HTTP/1.1 500 Internal Server Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        let response: &'static str = Box::leak(response.into_boxed_str());
        let (addr, count) = canned_server(vec![response]).await;
        let mut output = output_against(addr).await;

        let err = output.send(&one_metric_batch()).await.expect_err("a 500 should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "exactly one attempt");
        let message = err.root_cause().to_string();
        assert!(
            message.len() < ERROR_BODY_SNIPPET_BYTES + 96,
            "message should stay bounded regardless of body size: got {} bytes",
            message.len()
        );
        assert!(
            message.ends_with("..."),
            "a truncated body should be quoted with an ellipsis: {message}"
        );
    }

    /// A timeout against a server that accepts and never answers is `Ambiguous`, never `Clean`.
    #[tokio::test]
    async fn a_request_timeout_is_classified_ambiguous() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                std::mem::forget(stream); // keep it open; a dropped socket would close cleanly
            }
        });

        let mut output = output_against(addr).await.with_timeout(Duration::from_millis(50));
        let start = std::time::Instant::now();
        let err = output.send(&one_metric_batch()).await.expect_err("a stalled write should fail");

        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "should time out promptly, not hang: took {:?}",
            start.elapsed()
        );
    }

    /// A refused connection is `Clean`: pins `is_connect()`, which `at_most_once` relies on.
    #[tokio::test]
    async fn connect_refused_is_reliably_classified_as_a_clean_fault() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener); // nothing is listening on `addr` any more: a real "connection refused".

        let mut output = output_against(addr).await;
        let err = output
            .send(&one_metric_batch())
            .await
            .expect_err("connecting to a dropped listener should fail");

        assert_eq!(
            logit_pipeline::classify(&err),
            Fault::Clean,
            "connection-refused should classify as Clean -- if this ever fails, `is_connect()` \
             is not reliably distinguishing 'never reached the server' any more, and \
             crate::http::classify_reqwest_error must downgrade its mapping to Ambiguous instead"
        );
    }

    /// The multi-value counter fires before the empty-body return (port 1 is never contacted).
    #[tokio::test]
    async fn the_multi_value_tag_counter_is_emitted_even_when_the_batch_encodes_to_nothing() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let mut output = InfluxDbOutput::new(
            "http://127.0.0.1:1".to_string(),
            "org".into(),
            "bucket".into(),
            "token".into(),
        )
        .with_telemetry(telemetry);

        // A NaN counter renders no line, but its tag still collapsed.
        let mut event = metric_event("bad", MetricKind::counter(f64::NAN), &[]);
        event.attributes.insert("team", Value::Array(vec![Value::str("a"), Value::str("b")]));
        output
            .send(&batch_with(vec![event]))
            .await
            .expect("an all-unencodable batch is not an error");

        let events = registry.drain(0);
        let counted = events
            .iter()
            .filter(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("multi_value"))
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name)
                            == "logit.output.tags.normalized" =>
                    {
                        Some(s.value.as_f64())
                    }
                    _ => None,
                })
            })
            .unwrap_or(0.0);
        assert_eq!(counted, 1.0);
    }

    /// Every non-zero [`EncodeStats`] field reaches its counter, a batch that encodes to nothing
    /// included (port 1 is never contacted).
    #[tokio::test]
    async fn the_encoder_stats_are_emitted_as_counters() {
        use logit_pipeline::test_util::TelemetryProbe;

        let mut probe = TelemetryProbe::new();
        let mut output = InfluxDbOutput::new(
            "http://127.0.0.1:1".to_string(),
            "org".into(),
            "bucket".into(),
            "token".into(),
        )
        .with_telemetry(probe.telemetry("out", "influxdb_out", "sink"));
        let mut nan = metric_event("bad", MetricKind::counter(f64::NAN), &[("k\\", "v")]);
        nan.attributes.insert("host", Value::Bytes(Bytes::from_static(b"web-01")));
        let mut carrier = Event::empty(0, AttrMap::new());
        carrier.attributes.insert(logit_proto::influxdb::ATTR_MEASUREMENT, "cpu");
        carrier.metrics.push(MetricRecord::new(
            logit_core::interner::intern("mem.free\\"),
            MetricKind::gauge(1.0),
        ));
        output
            .send(&batch_with(vec![nan, carrier]))
            .await
            .expect("an all-unencodable batch is not an error");

        for (name, label) in [
            ("logit.output.metrics.skipped", ("reason", "unencodable_value")),
            ("logit.output.metrics.skipped", ("reason", "unencodable_key")),
            ("logit.output.metrics.normalized", ("reason", "carrier_mismatch")),
            ("logit.output.tags.dropped", ("reason", "unrepresentable")),
            ("logit.output.tags.dropped", ("reason", "unencodable")),
        ] {
            assert_eq!(probe.sum(name, &[label]), 1.0, "{name} {label:?}");
        }
    }

    /// `send` records one `logit.output.requests` per call, tagged by status class.
    #[tokio::test]
    async fn send_records_one_request_per_call_by_status_class() {
        let (addr, _count) = canned_server(vec![RESP_503]).await;
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let mut output = output_against(addr).await.with_telemetry(telemetry);

        let _ = output.send(&one_metric_batch()).await;

        let events = registry.drain(0);
        let value = |name: &str, tag: Option<(&str, &str)>| -> f64 {
            events
                .iter()
                .find_map(|e| {
                    if let Some((k, v)) = tag {
                        if e.attributes.get(k).and_then(|v2| v2.as_str()) != Some(v) {
                            return None;
                        }
                    }
                    e.metrics.iter().find_map(|m| match &m.kind {
                        MetricKind::Sum(s) if logit_core::interner::resolve(m.name) == name => {
                            Some(s.value.as_f64())
                        }
                        _ => None,
                    })
                })
                .unwrap_or(0.0)
        };

        assert_eq!(value("logit.output.requests", Some(("class", "5xx"))), 1.0);
        assert!(value("logit.output.batch.bytes", None) > 0.0);
    }

    // -- Attempt accounting (ADR `sink-send-path-and-attempt-accounting`, decision 2) ----------

    /// A 503 then a 204: the batch is delivered on its second attempt, and every encode-side
    /// counter and diagnostic reads as it does after one attempt.
    #[tokio::test]
    async fn a_retry_counts_encode_side_counters_once() {
        use crate::test_support::{
            assert_counted_once_per_batch, fast_retry, sum_of, sums_through_write_loop,
        };
        use logit_pipeline::test_util::TelemetryProbe;

        // A collapsed multi-value tag, and a gauge delta skipped with its diagnostic.
        let batch = || {
            let mut tagged = metric_event("hits", MetricKind::counter(1.0), &[]);
            tagged.attributes.insert("team", Value::Array(vec![Value::str("a"), Value::str("b")]));
            batch_with(vec![
                tagged,
                metric_event(
                    "conns",
                    MetricKind::GaugeDelta(logit_core::MetricValue::F64(5.0)),
                    &[],
                ),
            ])
        };
        const ENCODE_SIDE: [(&str, &[(&str, &str)]); 4] = [
            ("logit.output.tags.normalized", &[("reason", "multi_value")]),
            ("logit.component.diagnostics", &[("key", "gauge_delta_unresolved")]),
            ("logit.output.metrics.skipped", &[("metric_kind", "gauge_delta")]),
            ("logit.output.batch.bytes", &[]),
        ];
        let mut runs = Vec::new();
        for responses in [vec![RESP_204], vec![RESP_503, RESP_204]] {
            let (addr, _count) = canned_server(responses).await;
            let mut probe = TelemetryProbe::new();
            let telemetry = probe.telemetry("out", "influxdb_out", "sink");
            let mut output = output_against(addr)
                .await
                .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry);
            runs.push(
                sums_through_write_loop(
                    &mut output,
                    &mut probe,
                    "influxdb_out",
                    vec![batch()],
                    fast_retry(),
                )
                .await,
            );
        }
        let (single, retried) = (&runs[0], &runs[1]);
        assert_eq!(sum_of(single, "logit.output.requests", &[("class", "2xx")]), 1.0);
        assert_eq!(sum_of(retried, "logit.output.requests", &[("class", "5xx")]), 1.0);
        assert_eq!(sum_of(retried, "logit.output.requests", &[("class", "2xx")]), 1.0);
        assert_counted_once_per_batch(single, retried, &ENCODE_SIDE, &[]);
    }

    /// A batch that encodes to an empty body returns `Ok` early and still leaves the accounting
    /// disarmed, so later direct sends count.
    #[tokio::test]
    async fn direct_sends_after_a_batch_that_encoded_nothing_count_every_time() {
        use crate::test_support::assert_direct_sends_count_after_an_empty_batch;
        use logit_pipeline::test_util::TelemetryProbe;

        let (addr, _count) = canned_server(vec![RESP_204]).await;
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "influxdb_out", "sink");
        let mut output = output_against(addr)
            .await
            .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        let log = Event::log(
            0,
            AttrMap::new(),
            LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let batch = || {
            let mut tagged = metric_event("hits", MetricKind::counter(1.0), &[]);
            tagged.attributes.insert("team", Value::Array(vec![Value::str("a"), Value::str("b")]));
            batch_with(vec![
                tagged,
                metric_event(
                    "conns",
                    MetricKind::GaugeDelta(logit_core::MetricValue::F64(5.0)),
                    &[],
                ),
            ])
        };
        assert_direct_sends_count_after_an_empty_batch(
            &mut output,
            &mut probe,
            "influxdb_out",
            batch_with(vec![log]),
            batch,
            &[
                ("logit.output.tags.normalized", &[("reason", "multi_value")]),
                ("logit.component.diagnostics", &[("key", "gauge_delta_unresolved")]),
                ("logit.output.metrics.skipped", &[("metric_kind", "gauge_delta")]),
                ("logit.output.batch.bytes", &[]),
            ],
        )
        .await;
    }
}
