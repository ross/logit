//! `splunk_hec_out`: posts batches to Splunk's HTTP Event Collector (HEC) as `/event` JSON, the
//! sending half of the `splunk_hec_in -> splunk_hec_out` pair under
//! [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md)
//! ([ADR `splunk-hec-relay`](../../../docs/adr/splunk-hec-relay.md), decisions 2, 5, 10, 17, and
//! 18; [`docs/plans/splunk-relay.md`](../../../docs/plans/splunk-relay.md) §2, §4, §5). Every
//! object comes from [`SplunkEncoder::encode_objects`], whose module doc holds the mappings; this
//! module owns HTTP: how objects are cut into requests, what a response means, and
//! acknowledgment.
//!
//! ## Config
//!
//! ```yaml
//! kind: splunk_hec_out
//! endpoint: https://splunk:8088/services/collector   # the base; `/event` and `/ack` are appended
//! token: !env SPLUNK_HEC_TOKEN   # sent as `Authorization: Splunk <token>`; never logged
//! compression: gzip              # default; `none` sends every body uncompressed
//! multi_value: skip              # default; `expand` writes the exporter's series (the codec's doc)
//! ack: false                     # default; `true` polls /ack before a batch counts as delivered
//! ack_timeout: 30s               # default with `ack: true`; only with it
//! timeout: 10s                   # default; bounds one request
//! max_body_bytes: 2MiB           # default; one request body, before compression
//! tls: {}                        # tunes an https:// endpoint
//! ```
//!
//! There are no per-sink `index`, `source`, `sourcetype`, or `host` fields (ADR decision 2): the
//! encoder reads them from the batch resource, so an upstream `set` stamps them. Graph rule 70
//! validates the block.
//!
//! ## Requests
//!
//! One `send` encodes the batch once into a [`MessageBuf<ObjectMeta>`], one HEC object per entry,
//! then packs the objects greedily, in order, into bodies of at most `max_body_bytes` before
//! compression, concatenated with no separator. An object over the cap alone is never sent,
//! counted `logit.output.records.dropped{reason="oversize"}` for its records, with a throttled
//! `oversize` diagnostic, and packing carries on past it. Bodies go out sequentially as
//! `POST {endpoint}/event`, with these headers:
//!
//! | Header | Value |
//! |---|---|
//! | `Authorization` | `Splunk <token>`, marked sensitive |
//! | `Content-Type` | `application/json` |
//! | `Content-Encoding` | `gzip` under `compression: gzip`; absent under `none` |
//! | `User-Agent` | `logit/<version>` |
//! | `X-Splunk-Request-Channel` | one random v4 GUID per sink, on every request |
//!
//! The channel goes out whether or not `ack` is on (ADR decision 17): a `useACK` token answers
//! `400` code 10 (Splunk Enterprise) or code 28 (Splunk Cloud) to a request without one, and any
//! other token ignores it.
//!
//! The token never appears in a diagnostic or an error: a rejection body is read past the quoted
//! snippet size by the token's own length and scrubbed of it by
//! [`crate::http::redacted_snippet`].
//!
//! ## Faults, retries, and duplicate safety
//!
//! **One `send` is one attempt per request.** The first failing request aborts the rest of the
//! batch's requests, and `write_loop` retries the whole batch, re-sending any request that had
//! already succeeded.
//!
//! | Outcome | Result |
//! |---|---|
//! | 2xx | `Ok` (then acknowledgment, below, under `ack: true`) |
//! | `400` code 6 naming an object of the body (`invalid-event-number` in range), except the row below | that object dropped, counted `records.dropped{reason="invalid_event"}` with a throttled `invalid_event` diagnostic, and the rest resent once ([`after_invalid_event`]); a second code 6 is [`Fault::Permanent`] |
//! | `400` code 6 naming object 0 of a body over [`SPLUNK_CLOUD_BODY_CAP`] before compression | Splunk Cloud's answer to an oversize body: a body of several objects is split in two and each half sent, with a throttled `oversize` diagnostic; a half answered so again is [`Fault::Permanent`]. A body of one object is dropped, counted `records.dropped{reason="oversize"}` |
//! | 429, or 503 with code 9 or no HEC body, before any `/event` request of this `send` was accepted | [`Fault::Clean`] |
//! | the same after one was | [`Fault::Ambiguous`] |
//! | 408, any other 5xx | [`Fault::Ambiguous`] |
//! | 401, 403 | [`Fault::Permanent`], with a throttled `token_rejected` diagnostic |
//! | any other 3xx or 4xx (a code 6 with no or an out-of-range number included) | [`Fault::Permanent`], with a throttled `request_rejected` diagnostic quoting the first 256 bytes of the body |
//! | connect failure, before any `/event` request of this `send` was accepted | [`Fault::Clean`] |
//! | connect failure after one was (a 2xx, or a code 6 whose objects ahead count as delivered) | [`Fault::Ambiguous`] |
//! | any other transport error, timeout included | [`Fault::Ambiguous`] |
//!
//! `Clean` means Splunk didn't take the body. A connect failure, a `429` (codes 26 and 27), and a
//! `503` code 9 ("Server is busy") each say so ([`is_busy`]); a `500` code 8 may have indexed, and
//! a `408`, a timeout, or another `5xx` says nothing either way. `Clean` holds only while nothing
//! of the batch has reached Splunk: `write_loop` retries `Clean` under every posture, and a retry
//! re-sends the bodies already indexed, so once one was accepted each of these is `Ambiguous`
//! instead ([`after_delivery`]). A `Retry-After` header is ignored; `write_loop`'s backoff applies.
//!
//! One receiver breaks the "didn't take the body" reading: a `logit` `splunk_hec_in` older than
//! the fix that finishes a body once its first batch is delivered answers `503` code 9 after
//! delivering part of a multi-resource body, so a relay into one can deliver that part twice
//! (`docs/known-gaps.md`, "Splunk"). Splunk itself refuses before indexing.
//!
//! A drop leaves `sent_any` unset when nothing ahead of the dropped object was indexed (a code 6
//! at object 0, or a lone object over Splunk Cloud's cap), so a busy answer later in the same
//! `send` is still `Clean` and the whole batch is retried: a busy retry re-sends and re-counts a
//! dropped object in `records.dropped`; the record is never delivered twice. Marking the drop
//! as a delivery instead would make that busy answer `Ambiguous` and drop the rest of the batch
//! under the default posture.
//!
//! The code-6-at-object-0 test for an oversize body stands because Splunk Cloud answers a body
//! over its cap that way, not with `413`, and an object that can't be parsed at the head of a
//! body that large is far less likely than the cap. A body at or under the cap keeps the
//! drop-one rule.
//!
//! Every non-2xx other than 408, 429, and 5xx is counted `logit.output.requests.rejected{code}`,
//! `code` being the body's HEC code when Splunk documents it ([`code_tag`]), else `other`.
//! Redirects aren't followed ([`crate::http::build_client`] says why).
//!
//! [`SplunkHecOutput::duplicate_safe`] is **`false`**: Splunk indexes a resent event twice, and a
//! batch can span several requests, so a retry re-sends the ones that succeeded. The default
//! posture is at-most-once; `buffer: { delivery: at_least_once }` retries and accepts duplicates.
//!
//! ## Acknowledgment
//!
//! Under `ack: true`, each 2xx body's `ackId` is kept. Once every body of the batch is accepted,
//! the sink polls `POST {endpoint}/ack` with `{"acks":[…]}` after 0.5s, 1s, 2s, and then every
//! 5s, dropping each id answered `true`, until none is left (`Ok`) or `ack_timeout` has passed
//! since the last body was accepted (a last poll at the deadline, then [`Fault::Ambiguous`], so
//! `write_loop` may resend the batch). A poll that fails in transport, answers another non-2xx,
//! or answers a body that isn't an ack reply is retried on the same schedule.
//!
//! Two answers mean the token doesn't acknowledge: a 2xx with no `ackId`, and a poll answered
//! `400` code 14 (`ACK is disabled`). Either counts the request, or every id still pending, as
//! delivered, counted `logit.output.acks{result="unsupported"}` with a throttled
//! `ack_unsupported` diagnostic.
//!
//! ## Telemetry
//!
//! | Point | Meaning |
//! |---|---|
//! | `logit.output.requests{route, class}` | one per request; `route` is `event` or `ack`, `class` is [`crate::http::status_class`]'s, or `network_error` |
//! | `logit.output.request.duration{route}` | one timer per request |
//! | `logit.output.request.bytes{route}` | the body as sent, after compression |
//! | `logit.output.records` | records in a body Splunk accepted, and those ahead of a code-6 object |
//! | `logit.output.records.dropped{reason}` | `oversize` or `invalid_event`, as above |
//! | `logit.output.requests.rejected{code}` | one per rejected `/event` request, as above |
//! | `logit.output.acks{result}` | per `/event` request under `ack: true`: `acked`, `timeout`, or `unsupported` |
//!
//! Plus everything [`SplunkEncoder`] counts itself (`logit.output.metrics.skipped` and
//! `metrics.degraded` by `metric_kind` under `multi_value`, `metrics.normalized`, `tags.dropped`,
//! `events.skipped`, `spans.degraded`), which this sink doesn't repeat.

use crate::http::{
    build_client, classify_reqwest_error, error_read_bytes, read_body_prefix, redacted_snippet,
    status_class,
};
/// `tls:`: the shared `crate::tls` type, re-exported as the other sinks do.
pub use crate::tls::TlsClientSettings;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{Fault, Output};
use logit_proto::splunk::response::{
    encode_ack_request, parse_ack_reply, parse_reply, HecReply, HecStatus, SPLUNK_CLOUD_BODY_CAP,
};
use logit_proto::splunk::{ObjectMeta, SplunkEncoder};
use logit_proto::{MessageBuf, MultiValue};
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::time::Duration;

/// The default `timeout:` for one request.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The default `ack_timeout:` under `ack: true`, the `batchTimeout` a forwarder uses.
pub const DEFAULT_ACK_TIMEOUT: Duration = Duration::from_secs(30);

/// The default `max_body_bytes:`, the OpenTelemetry exporter's `max_content_length_logs`.
pub const DEFAULT_MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

const USER_AGENT: &str = concat!("logit/", env!("CARGO_PKG_VERSION"));
const CHANNEL_HEADER: &str = "x-splunk-request-channel";

/// The waits before each `/ack` poll; every later poll waits the last.
const ACK_BACKOFF: [Duration; 4] = [
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
];

/// How much of a 2xx or `/ack` reply to read: a HEC status body or an ack map for one batch's
/// requests is far smaller.
const REPLY_READ_BYTES: usize = 64 * 1024;

const REQUESTS: &str = "logit.output.requests";
const REQUEST_DURATION: &str = "logit.output.request.duration";
const REQUEST_BYTES: &str = "logit.output.request.bytes";
const RECORDS: &str = "logit.output.records";
const RECORDS_DROPPED: &str = "logit.output.records.dropped";
const REQUESTS_REJECTED: &str = "logit.output.requests.rejected";
const ACKS: &str = "logit.output.acks";

const ROUTE_EVENT: &str = "event";
const ROUTE_ACK: &str = "ack";

/// Whether request bodies are compressed. Mirrors `logit_config::SplunkCompression`, which
/// `logit-cli::pipeline::build_spec` translates, since this crate doesn't depend on
/// `logit-config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SplunkCompression {
    #[default]
    Gzip,
    None,
}

/// One object ready to send: its bytes and the records it carries.
type Object<'a> = (&'a [u8], usize);

/// What a `/event` request's non-failing answer was.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EventReply {
    /// A 2xx, with the `ackId` it carried when `ack` is on.
    Accepted { ack_id: Option<u64> },
    /// `400` code 6 naming object `n` of the body.
    InvalidEvent { n: u64 },
}

/// What one `/ack` poll said.
enum AckPoll {
    /// `(id, acked)` for the ids the reply named.
    Answered(Vec<(u64, bool)>),
    /// `400` code 14: the token doesn't acknowledge.
    Unsupported,
    /// Anything else: poll again.
    Retry,
}

/// What one body's send settled, when it didn't fail.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyOutcome {
    /// Accepted, or the objects left after a code-6 drop were.
    Done,
    /// Splunk Cloud answered it as over its cap, and it holds more than one object.
    OverCloudCap,
}

/// A body's length before compression: its objects concatenated with no separator.
fn body_len(objects: &[Object<'_>]) -> usize {
    objects.iter().map(|(bytes, _)| bytes.len()).sum()
}

/// Where to cut a body of two or more objects in two: after the first object at which half its
/// bytes are reached, and never at either end.
fn split_point(objects: &[Object<'_>]) -> usize {
    let total = body_len(objects);
    let mut bytes = 0;
    for (i, (object, _)) in objects.iter().enumerate() {
        bytes += object.len();
        if bytes * 2 >= total {
            return (i + 1).clamp(1, objects.len() - 1);
        }
    }
    objects.len() - 1
}

/// Which objects of a body to resend after Splunk answered `400` code 6 with
/// `invalid-event-number` `n`: the object to drop, and the range of objects to send again, or
/// `None` when `n` names no object of the body. It assumes Splunk indexed every object before
/// `n` and none after, which Splunk doesn't document: UNVERIFIED item 8 in
/// `docs/plans/splunk-relay.md`, which W5 settles against a real Splunk (ADR `splunk-hec-relay`,
/// decision 18). This function is the one place that assumption lives.
fn after_invalid_event(objects: usize, n: u64) -> Option<(usize, Range<usize>)> {
    let n = usize::try_from(n).ok().filter(|&n| n < objects)?;
    Some((n, n + 1..objects))
}

/// Whether a `/event` answer says Splunk didn't take the body: any `429` (codes 26 and 27), or a
/// `503` that is code 9 ("Server is busy") or carries no HEC body. It is [`Fault::Clean`], and
/// [`after_delivery`] makes it `Ambiguous` once a body of the `send` was accepted. A `500` code 8
/// may have indexed, and a `408`, `502`, `504`, or other `503` says nothing either way, so those
/// stay `Ambiguous`.
fn is_busy(status: u16, reply: Option<&HecReply>) -> bool {
    match status {
        429 => true,
        503 => reply.is_none_or(|reply| reply.code == HecStatus::SERVER_BUSY.code),
        _ => false,
    }
}

/// A failure of a later request in a `send` that already had a body accepted: a `Clean` fault
/// becomes [`Fault::Ambiguous`], since `write_loop` retries `Clean` under every posture and the
/// retry would index the accepted bodies twice (module doc's "Faults" table).
fn after_delivery(err: anyhow::Error, sent_any: bool) -> anyhow::Error {
    if sent_any && logit_pipeline::classify(&err) == Fault::Clean {
        err.context(Fault::Ambiguous)
    } else {
        err
    }
}

/// The `code` tag on `logit.output.requests.rejected`: the reply's HEC code when it is one
/// Splunk documents ([`HecStatus::ALL`]), else `other`, so the tag stays bounded whatever a
/// server answers.
fn code_tag(reply: Option<&HecReply>) -> &'static str {
    const TAGS: [&str; 29] = [
        "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16",
        "17", "18", "19", "20", "21", "22", "23", "24", "25", "26", "27", "28",
    ];
    match reply {
        Some(reply) if HecStatus::from_code(reply.code).is_some() => TAGS[usize::from(reply.code)],
        _ => "other",
    }
}

/// Cuts objects of the given sizes into consecutive runs of at most `max` bytes, in order. Every
/// size is at most `max`.
fn pack(sizes: impl IntoIterator<Item = usize>, max: usize) -> Vec<Range<usize>> {
    let mut bodies = Vec::new();
    let (mut start, mut bytes, mut end) = (0, 0usize, 0);
    for size in sizes {
        if end > start && bytes + size > max {
            bodies.push(start..end);
            (start, bytes) = (end, 0);
        }
        bytes += size;
        end += 1;
    }
    if end > start {
        bodies.push(start..end);
    }
    bodies
}

/// A random version-4 GUID, lowercase, the form Splunk takes as a channel.
fn channel_guid() -> String {
    let mut b = logit_core::trace::random_id_bytes::<16>();
    b[6] = (b[6] & 0x0f) | 0x40;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex: String = b.iter().map(|byte| format!("{byte:02x}")).collect();
    format!("{}-{}-{}-{}-{}", &hex[0..8], &hex[8..12], &hex[12..16], &hex[16..20], &hex[20..32])
}

/// Inline, not `spawn_blocking`: a body is at most `max_body_bytes`, which compresses in
/// milliseconds at the default.
fn gzip(raw: &[u8]) -> Bytes {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(raw).expect("writing to an in-memory Vec never fails");
    Bytes::from(e.finish().expect("finishing an in-memory encoder never fails"))
}

/// The HEC client (module doc).
///
/// Not `Debug`: it holds the token.
pub struct SplunkHecOutput {
    /// The base URL, trailing `/` dropped.
    endpoint: String,
    /// The bare token, only for scrubbing it from quoted response bodies.
    token: String,
    /// `Splunk <token>`, marked sensitive so `http`'s own `Debug` never prints it.
    authorization: HeaderValue,
    channel: HeaderValue,
    compression: SplunkCompression,
    ack: bool,
    ack_timeout: Duration,
    /// [`ACK_BACKOFF`]; a field so tests can run the schedule in milliseconds.
    ack_backoff: [Duration; 4],
    request_timeout: Duration,
    max_body_bytes: usize,
    /// Built by [`Output::bind`], or by the first `send` when nothing called it.
    client: Option<reqwest::Client>,
    /// Built by [`SplunkHecOutput::with_tls`]; `None` keeps `reqwest`'s default trust.
    tls: Option<rustls::ClientConfig>,
    multi_value: MultiValue,
    encoder: SplunkEncoder,
    /// Reused across batches; see [`MessageBuf`]'s `clear`.
    objects: MessageBuf<ObjectMeta>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl SplunkHecOutput {
    /// `endpoint` is the `/services/collector` base (rule 70). Fails when `token` can't be sent
    /// as a header value (a control character, say); the message never quotes it.
    pub fn new(endpoint: impl Into<String>, token: &str) -> anyhow::Result<Self> {
        let mut authorization =
            HeaderValue::from_str(&format!("Splunk {token}")).map_err(|_| {
                anyhow::anyhow!(
                    "splunk_hec_out: 'token' isn't a legal HTTP header value (it holds a control \
                 character or a non-ASCII byte)"
                )
            })?;
        authorization.set_sensitive(true);
        let endpoint = endpoint.into().trim_end_matches('/').to_string();
        Ok(Self {
            endpoint,
            token: token.to_string(),
            authorization,
            channel: HeaderValue::from_str(&channel_guid()).expect("a GUID is a legal header"),
            compression: SplunkCompression::default(),
            ack: false,
            ack_timeout: DEFAULT_ACK_TIMEOUT,
            ack_backoff: ACK_BACKOFF,
            request_timeout: DEFAULT_TIMEOUT,
            max_body_bytes: DEFAULT_MAX_BODY_BYTES,
            client: None,
            tls: None,
            multi_value: MultiValue::default(),
            encoder: SplunkEncoder::new(),
            objects: MessageBuf::default(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        })
    }

    pub fn with_compression(mut self, compression: SplunkCompression) -> Self {
        self.compression = compression;
        self
    }

    /// What a metric kind with more than one number does (`multi_value:`).
    pub fn with_multi_value(mut self, multi_value: MultiValue) -> Self {
        self.multi_value = multi_value;
        self.encoder = self.new_encoder();
        self
    }

    /// Whether to poll `/ack` (`ack:`), and for how long per batch (`ack_timeout:`).
    pub fn with_ack(mut self, ack: bool, timeout: Duration) -> Self {
        self.ack = ack;
        self.ack_timeout = timeout;
        self
    }

    /// Per-request timeout (`timeout:`).
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self.client = None;
        self
    }

    /// The largest body before compression (`max_body_bytes:`).
    pub fn with_max_body_bytes(mut self, max_body_bytes: usize) -> Self {
        self.max_body_bytes = max_body_bytes;
        self
    }

    /// Client TLS tuning (`tls:`) for an `https://` endpoint. A no-op when `settings` is empty.
    /// The files load and validate here, since `graph::resolve` never touches the filesystem.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        if settings.is_empty() {
            return Ok(self);
        }
        if settings.insecure_skip_verify {
            self.diag.warn(
                "tls.insecure_skip_verify is set -- the connection is encrypted, but this output \
                 will accept any certificate the peer presents, self-signed or otherwise",
            );
        }
        self.tls = Some(crate::tls::build_client_config(settings, base_dir)?);
        self.client = None;
        Ok(self)
    }

    /// Reaches the encoder too, which reports its own throttled diagnostics.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self.encoder = self.new_encoder();
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self.encoder = self.new_encoder();
        self
    }

    fn new_encoder(&self) -> SplunkEncoder {
        SplunkEncoder::new()
            .with_telemetry(self.telemetry.clone())
            .with_diagnostics(self.diag.clone())
            .with_multi_value(self.multi_value)
    }

    /// The client, built on first use when [`Output::bind`] wasn't called.
    fn client(&mut self) -> reqwest::Client {
        let (timeout, tls) = (self.request_timeout, self.tls.as_ref());
        self.client.get_or_insert_with(|| build_client(timeout, tls)).clone()
    }

    fn headers(&self, gzipped: bool) -> HeaderMap {
        let mut headers = HeaderMap::with_capacity(5);
        headers.insert(http::header::AUTHORIZATION, self.authorization.clone());
        headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if gzipped {
            headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static("gzip"));
        }
        headers.insert(http::header::USER_AGENT, HeaderValue::from_static(USER_AGENT));
        headers.insert(HeaderName::from_static(CHANNEL_HEADER), self.channel.clone());
        headers
    }

    /// One request, one attempt: the transport half every route shares. A transport error is
    /// counted and returned with its [`Fault`].
    async fn post(
        &mut self,
        route: &'static str,
        url: &str,
        body: Bytes,
        gzipped: bool,
    ) -> anyhow::Result<reqwest::Response> {
        let client = self.client();
        let tags = [("route", route)];
        let wire_len = body.len();
        let timer = self.telemetry.timer(REQUEST_DURATION);
        let result = client
            .post(url)
            .headers(self.headers(gzipped))
            .timeout(self.request_timeout)
            .body(body)
            .send()
            .await;
        timer.stop(&tags);
        self.telemetry.count(REQUEST_BYTES, wire_len as f64, &tags);
        match result {
            Ok(response) => {
                let class = status_class(response.status());
                self.telemetry.count(REQUESTS, 1.0, &[("route", route), ("class", class)]);
                Ok(response)
            }
            Err(err) => {
                self.telemetry.count(
                    REQUESTS,
                    1.0,
                    &[("route", route), ("class", "network_error")],
                );
                let fault = classify_reqwest_error(&err);
                Err(anyhow::Error::new(err).context(fault))
            }
        }
    }

    /// One `/event` request for `objects` (module doc's "Faults" table). A code 6 naming an
    /// object is returned for the caller to act on; every other non-2xx is an error.
    async fn post_event(&mut self, objects: &[Object<'_>]) -> anyhow::Result<EventReply> {
        let raw: Vec<u8> = objects.iter().flat_map(|(bytes, _)| bytes.iter().copied()).collect();
        let gzipped = self.compression == SplunkCompression::Gzip;
        let body = if gzipped { gzip(&raw) } else { Bytes::from(raw) };
        let url = format!("{}/event", self.endpoint);
        let response = self.post(ROUTE_EVENT, &url, body, gzipped).await?;
        let status = response.status();
        if status.is_success() {
            let records: usize = objects.iter().map(|(_, records)| records).sum();
            self.telemetry.count(RECORDS, records as f64, &[]);
            let ack_id = if self.ack {
                let body = read_body_prefix(response, REPLY_READ_BYTES).await;
                parse_reply(body.as_bytes()).and_then(|reply| reply.ack_id)
            } else {
                None
            };
            return Ok(EventReply::Accepted { ack_id });
        }
        let body = read_body_prefix(response, error_read_bytes(&self.token)).await;
        let reply = parse_reply(body.as_bytes());
        let snippet = redacted_snippet(&body, &self.token);
        let fault = match status.as_u16() {
            code if is_busy(code, reply.as_ref()) => Fault::Clean,
            408 | 500..=599 => Fault::Ambiguous,
            code => {
                self.telemetry.count(REQUESTS_REJECTED, 1.0, &[("code", code_tag(reply.as_ref()))]);
                if let (400, Some(HecReply { code: 6, invalid_event_number: Some(n), .. })) =
                    (code, &reply)
                {
                    return Ok(EventReply::InvalidEvent { n: *n });
                }
                if matches!(code, 401 | 403) {
                    self.diag.warn_throttled(
                        "token_rejected",
                        format_args!(
                            "Splunk refused the token: {url} answered {status}: {snippet} -- \
                             check 'token', and that it is enabled"
                        ),
                    );
                } else {
                    self.diag.warn_throttled(
                        "request_rejected",
                        format_args!("{url} answered {status}: {snippet}"),
                    );
                }
                Fault::Permanent
            }
        };
        Err(anyhow::anyhow!("splunk_hec_out: request to {url} failed ({status}): {snippet}"))
            .map_err(|err| err.context(fault))
    }

    /// Keeps a 2xx's `ackId` under `ack: true`; a 2xx without one means the token doesn't
    /// acknowledge (module doc's "Acknowledgment").
    fn keep_ack_id(&mut self, ack_id: Option<u64>, ack_ids: &mut Vec<u64>) {
        if !self.ack {
            return;
        }
        match ack_id {
            Some(id) => ack_ids.push(id),
            None => self.ack_unsupported(1, "a 2xx answer carried no ackId"),
        }
    }

    fn ack_unsupported(&mut self, requests: usize, why: &str) {
        self.telemetry.count(ACKS, requests as f64, &[("result", "unsupported")]);
        self.diag.warn_throttled(
            "ack_unsupported",
            format_args!(
                "'ack' is on, but {why}: the token doesn't acknowledge (indexer acknowledgment \
                 is off for it), so each accepted request counts as delivered"
            ),
        );
    }

    /// One body, split in two when Splunk Cloud answers it as over its cap, and each half's
    /// code-6 resend (module doc's "Faults" table). `sent_any` says whether an earlier request of
    /// this `send` was accepted, and is set once one of these is.
    async fn send_body(
        &mut self,
        objects: &[Object<'_>],
        ack_ids: &mut Vec<u64>,
        sent_any: &mut bool,
    ) -> anyhow::Result<()> {
        if self.send_once(objects, ack_ids, sent_any, true).await? == BodyOutcome::OverCloudCap {
            let mid = split_point(objects);
            self.diag.warn_throttled(
                "oversize",
                format_args!(
                    "Splunk refused a request of {} bytes as invalid (code 6) at object 0, its \
                     answer to a body over Splunk Cloud's {SPLUNK_CLOUD_BODY_CAP}-byte cap; \
                     resent it as two requests -- lower 'max_body_bytes' to \
                     {SPLUNK_CLOUD_BODY_CAP} or less",
                    body_len(objects)
                ),
            );
            for half in [&objects[..mid], &objects[mid..]] {
                self.send_once(half, ack_ids, sent_any, false).await?;
            }
        }
        Ok(())
    }

    /// One body and its code-6 resend. A code 6 naming object 0 of a body over
    /// [`SPLUNK_CLOUD_BODY_CAP`] is Splunk Cloud's oversize answer, not a bad object: a lone
    /// object is dropped as `oversize`; several are [`BodyOutcome::OverCloudCap`] for the caller
    /// to split when `may_split`, and [`Fault::Permanent`] otherwise.
    async fn send_once(
        &mut self,
        objects: &[Object<'_>],
        ack_ids: &mut Vec<u64>,
        sent_any: &mut bool,
        may_split: bool,
    ) -> anyhow::Result<BodyOutcome> {
        let reply = self.post_event(objects).await.map_err(|err| after_delivery(err, *sent_any))?;
        let n = match reply {
            EventReply::Accepted { ack_id } => {
                *sent_any = true;
                self.keep_ack_id(ack_id, ack_ids);
                return Ok(BodyOutcome::Done);
            }
            EventReply::InvalidEvent { n } => n,
        };
        if n == 0 && body_len(objects) > SPLUNK_CLOUD_BODY_CAP {
            return self.over_cloud_cap(objects, may_split);
        }
        let Some((bad, rest)) = after_invalid_event(objects.len(), n) else {
            let message = format!(
                "Splunk rejected a request of {} objects as invalid (code 6) at object {n}, \
                 which isn't one of them",
                objects.len()
            );
            self.diag.warn_throttled("request_rejected", &message);
            return Err(anyhow::anyhow!("splunk_hec_out: {message}"))
                .map_err(|err| err.context(Fault::Permanent));
        };
        // The objects ahead of `bad` count as indexed (`after_invalid_event`'s assumption).
        *sent_any |= bad > 0;
        let ahead: usize = objects[..bad].iter().map(|(_, records)| records).sum();
        self.telemetry.count(RECORDS, ahead as f64, &[]);
        let dropped = objects[bad].1;
        self.telemetry.count(RECORDS_DROPPED, dropped as f64, &[("reason", "invalid_event")]);
        self.diag.warn_throttled(
            "invalid_event",
            format_args!(
                "Splunk rejected object {bad} of a request as invalid (code 6); dropped it and \
                 resent the {} objects after it",
                rest.len()
            ),
        );
        if rest.is_empty() {
            return Ok(BodyOutcome::Done);
        }
        let reply =
            self.post_event(&objects[rest]).await.map_err(|err| after_delivery(err, *sent_any))?;
        match reply {
            EventReply::Accepted { ack_id } => {
                *sent_any = true;
                self.keep_ack_id(ack_id, ack_ids);
                Ok(BodyOutcome::Done)
            }
            EventReply::InvalidEvent { n } => Err(anyhow::anyhow!(
                "splunk_hec_out: Splunk rejected the resend after a dropped invalid object as \
                 invalid too (code 6, object {n})"
            ))
            .map_err(|err| err.context(Fault::Permanent)),
        }
    }

    /// Splunk Cloud's oversize answer to `objects` (`send_once`).
    fn over_cloud_cap(
        &mut self,
        objects: &[Object<'_>],
        may_split: bool,
    ) -> anyhow::Result<BodyOutcome> {
        let bytes = body_len(objects);
        if let [(_, records)] = objects {
            self.telemetry.count(RECORDS_DROPPED, *records as f64, &[("reason", "oversize")]);
            self.diag.warn_throttled(
                "oversize",
                format_args!(
                    "Splunk refused one HEC object of {bytes} bytes as invalid (code 6), its \
                     answer to a body over Splunk Cloud's {SPLUNK_CLOUD_BODY_CAP}-byte cap; \
                     dropped it"
                ),
            );
            return Ok(BodyOutcome::Done);
        }
        if may_split {
            return Ok(BodyOutcome::OverCloudCap);
        }
        let message = format!(
            "Splunk refused half of a split request, {bytes} bytes in {} objects, as invalid \
             (code 6) at object 0, its answer to a body over Splunk Cloud's \
             {SPLUNK_CLOUD_BODY_CAP}-byte cap -- lower 'max_body_bytes' to \
             {SPLUNK_CLOUD_BODY_CAP} or less",
            objects.len()
        );
        self.diag.warn_throttled("oversize", &message);
        Err(anyhow::anyhow!("splunk_hec_out: {message}"))
            .map_err(|err| err.context(Fault::Permanent))
    }

    /// One `/ack` poll for `pending`.
    async fn poll(&mut self, url: &str, pending: &[u64]) -> AckPoll {
        let body = Bytes::from(encode_ack_request(pending));
        let Ok(response) = self.post(ROUTE_ACK, url, body, false).await else {
            return AckPoll::Retry;
        };
        let status = response.status();
        let body = read_body_prefix(response, REPLY_READ_BYTES).await;
        if status.is_success() {
            return parse_ack_reply(body.as_bytes()).map_or(AckPoll::Retry, AckPoll::Answered);
        }
        match parse_reply(body.as_bytes()) {
            Some(reply) if status.as_u16() == 400 && reply.code == 14 => AckPoll::Unsupported,
            _ => AckPoll::Retry,
        }
    }

    /// Polls `/ack` until every id is acknowledged or `ack_timeout` passes (module doc's
    /// "Acknowledgment").
    async fn await_acks(&mut self, mut pending: Vec<u64>) -> anyhow::Result<()> {
        let deadline = tokio::time::Instant::now() + self.ack_timeout;
        let url = format!("{}/ack", self.endpoint);
        for step in 0.. {
            let wait = self.ack_backoff[usize::min(step, self.ack_backoff.len() - 1)];
            tokio::time::sleep_until((tokio::time::Instant::now() + wait).min(deadline)).await;
            match self.poll(&url, &pending).await {
                AckPoll::Answered(answers) => {
                    let before = pending.len();
                    pending.retain(|id| !answers.contains(&(*id, true)));
                    let acked = before - pending.len();
                    if acked > 0 {
                        self.telemetry.count(ACKS, acked as f64, &[("result", "acked")]);
                    }
                }
                AckPoll::Unsupported => {
                    self.ack_unsupported(
                        pending.len(),
                        "Splunk answered the poll 'ACK is disabled'",
                    );
                    return Ok(());
                }
                AckPoll::Retry => {}
            }
            if pending.is_empty() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                break;
            }
        }
        self.telemetry.count(ACKS, pending.len() as f64, &[("result", "timeout")]);
        self.diag.warn_throttled(
            "ack_timeout",
            format_args!(
                "Splunk hadn't acknowledged {} request(s) after {:?}; the batch may be resent",
                pending.len(),
                self.ack_timeout
            ),
        );
        Err(anyhow::anyhow!(
            "splunk_hec_out: {} request(s) not acknowledged within {:?}",
            pending.len(),
            self.ack_timeout
        ))
        .map_err(|err| err.context(Fault::Ambiguous))
    }

    /// Packs `objects` into bodies and sends them (module doc's "Requests").
    async fn send_objects(&mut self, objects: &MessageBuf<ObjectMeta>) -> anyhow::Result<()> {
        let mut sendable: Vec<Object<'_>> = Vec::with_capacity(objects.len());
        for (bytes, meta) in objects.iter_with() {
            if bytes.len() <= self.max_body_bytes {
                sendable.push((bytes, meta.records));
                continue;
            }
            self.telemetry.count(RECORDS_DROPPED, meta.records as f64, &[("reason", "oversize")]);
            self.diag.warn_throttled(
                "oversize",
                format_args!(
                    "dropped one HEC object of {} bytes, over 'max_body_bytes' ({})",
                    bytes.len(),
                    self.max_body_bytes
                ),
            );
        }
        let (mut ack_ids, mut sent_any) = (Vec::new(), false);
        for body in pack(sendable.iter().map(|(bytes, _)| bytes.len()), self.max_body_bytes) {
            self.send_body(&sendable[body], &mut ack_ids, &mut sent_any).await?;
        }
        if !ack_ids.is_empty() {
            self.await_acks(ack_ids).await?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl Output for SplunkHecOutput {
    /// Builds the HTTP client; idempotent. Nothing to open: the sink only connects outward.
    async fn bind(&mut self) -> anyhow::Result<()> {
        self.client();
        Ok(())
    }

    /// Every body sequentially, then acknowledgment; the first failure aborts the rest (module
    /// doc's "Faults, retries, and duplicate safety").
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let mut objects = std::mem::take(&mut self.objects);
        self.encoder.encode_objects(batch, &mut objects);
        let result = self.send_objects(&objects).await;
        self.objects = objects;
        result
    }

    /// `false`: the module doc's "Faults, retries, and duplicate safety" says why.
    fn duplicate_safe(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::{BodyExt, Full};
    use hyper::service::service_fn;
    use hyper_util::rt::TokioIo;
    use logit_core::interner::{intern, resolve};
    use logit_core::{
        AttrMap, BodyFormat, Event, Histogram, LogRecord, MetricKind, MetricRecord, Registry,
        Resource, Temporality, Value,
    };
    use logit_proto::splunk::response::{encode_ack_reply, encode_invalid_event, encode_success};
    use logit_proto::splunk::SplunkDecoder;
    use std::io::Read;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    const TOKEN: &str = "11111111-2222-3333-4444-555555555555";
    const TS: i64 = 1_700_000_000_000_000_000;

    // ---- a local collector that records every request ---------------------------------------

    #[derive(Debug, Clone)]
    struct Captured {
        path: String,
        headers: http::HeaderMap,
        /// Decompressed per `Content-Encoding`.
        body: Vec<u8>,
    }

    impl Captured {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers.get(name).and_then(|v| v.to_str().ok())
        }

        fn decode(&self) -> Vec<EventBatch> {
            SplunkDecoder::new().decode_events(&self.body, 0).expect("a HEC body")
        }
    }

    type Log = Arc<Mutex<Vec<Captured>>>;

    fn decompress(headers: &http::HeaderMap, body: &[u8]) -> Vec<u8> {
        match headers.get("content-encoding").and_then(|v| v.to_str().ok()) {
            None => body.to_vec(),
            Some("gzip") => {
                let mut out = Vec::new();
                flate2::read::GzDecoder::new(body).read_to_end(&mut out).unwrap();
                out
            }
            Some(other) => panic!("unexpected content-encoding {other}"),
        }
    }

    /// An HTTP/1.1 server recording each request and answering `respond(path, decoded body)`.
    async fn collector(
        respond: impl Fn(&str, &[u8]) -> (u16, String) + Send + Sync + 'static,
    ) -> (SocketAddr, Log) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let log: Log = Arc::default();
        let respond = Arc::new(respond);
        let task_log = log.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let (log, respond) = (task_log.clone(), respond.clone());
                tokio::spawn(async move {
                    let svc = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let (log, respond) = (log.clone(), respond.clone());
                        async move {
                            let path = req.uri().path().to_string();
                            let headers = req.headers().clone();
                            let raw = req.into_body().collect().await.unwrap().to_bytes();
                            let body = decompress(&headers, &raw);
                            let (status, text) = respond(&path, &body);
                            log.lock().unwrap().push(Captured { path, headers, body });
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::from(text)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), svc)
                        .await;
                });
            }
        });
        (addr, log)
    }

    fn success() -> (u16, String) {
        (200, String::from_utf8(encode_success(None)).unwrap())
    }

    async fn accepting() -> (SocketAddr, Log) {
        collector(|_, _| success()).await
    }

    fn sink(addr: SocketAddr) -> SplunkHecOutput {
        SplunkHecOutput::new(format!("http://{addr}/services/collector/"), TOKEN).unwrap()
    }

    fn metered(addr: SocketAddr) -> (Arc<Registry>, SplunkHecOutput) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("out", "splunk_hec_out", "sink");
        (registry, sink(addr).with_telemetry(telemetry))
    }

    /// A counter's total across every point matching `tags`.
    fn total(points: &[Event], metric: &str, tags: &[(&str, &str)]) -> f64 {
        let mut sum = 0.0;
        for event in points {
            if tags.iter().any(|(k, v)| event.attributes.get(k).and_then(Value::as_str) != Some(v))
            {
                continue;
            }
            for record in &event.metrics {
                if let MetricKind::Sum(s) = &record.kind {
                    if resolve(record.name) == metric {
                        sum += s.value;
                    }
                }
            }
        }
        sum
    }

    fn paths(log: &Log) -> Vec<String> {
        log.lock().unwrap().iter().map(|c| c.path.clone()).collect()
    }

    // ---- events ------------------------------------------------------------------------------

    fn batch(events: Vec<Event>) -> EventBatch {
        let mut resource = Resource::default();
        resource.attributes.insert("host.name", Value::str("web-1"));
        resource.attributes.insert("com.splunk.index", Value::str("main"));
        EventBatch { resource: Arc::new(resource), scope: None, events }
    }

    fn log_event(message: &str) -> Event {
        Event::log(
            TS,
            AttrMap::new(),
            LogRecord {
                message: Value::str(message),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn logs(n: usize) -> EventBatch {
        batch((0..n).map(|i| log_event(&format!("line {i}"))).collect())
    }

    fn messages(batches: &[EventBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| &b.events)
            .map(|e| e.log.as_ref().unwrap().message.as_str().unwrap().to_string())
            .collect()
    }

    // ---- the wire ----------------------------------------------------------------------------

    /// One request to `/event` with the HEC headers, and a body that decodes to the batch sent.
    #[tokio::test]
    async fn a_batch_posts_to_event_with_the_hec_headers() {
        let (addr, log) = accepting().await;
        let mut out = sink(addr);
        let b = logs(3);
        out.send(&b).await.unwrap();
        out.send(&b).await.unwrap();

        let captured = log.lock().unwrap().clone();
        assert_eq!(paths(&log), ["/services/collector/event", "/services/collector/event"]);
        let first = &captured[0];
        assert_eq!(first.header("authorization"), Some(format!("Splunk {TOKEN}").as_str()));
        assert_eq!(first.header("content-type"), Some("application/json"));
        assert_eq!(first.header("content-encoding"), Some("gzip"));
        assert_eq!(first.header("user-agent"), Some(USER_AGENT));
        let channel = first.header(CHANNEL_HEADER).expect("a channel on every request");
        assert_eq!(captured[1].header(CHANNEL_HEADER), Some(channel), "one channel per sink");
        assert_eq!(first.decode(), vec![b]);
    }

    /// `compression: none` sends the body as-is, with no `Content-Encoding`.
    #[tokio::test]
    async fn compression_none_sends_plain_json() {
        let (addr, log) = accepting().await;
        sink(addr).with_compression(SplunkCompression::None).send(&logs(1)).await.unwrap();
        let captured = log.lock().unwrap()[0].clone();
        assert_eq!(captured.header("content-encoding"), None);
        assert_eq!(messages(&captured.decode()), ["line 0"]);
    }

    /// The channel is a lowercase version-4 GUID, and differs per sink.
    #[test]
    fn the_channel_is_a_v4_guid() {
        let guid = channel_guid();
        let groups: Vec<&str> = guid.split('-').collect();
        assert_eq!(groups.iter().map(|g| g.len()).collect::<Vec<_>>(), [8, 4, 4, 4, 12]);
        assert!(guid.chars().all(|c| c == '-' || c.is_ascii_hexdigit() && !c.is_uppercase()));
        assert!(groups[2].starts_with('4'), "{guid}");
        assert!("89ab".contains(&groups[3][..1]), "{guid}");
        assert_ne!(channel_guid(), guid);
    }

    #[test]
    fn a_token_that_cant_be_a_header_fails_without_quoting_it() {
        let Err(err) = SplunkHecOutput::new("http://h/services/collector", "secret\ntoken") else {
            panic!("a newline can't go in a header value");
        };
        assert!(!format!("{err:#}").contains("secret"), "{err:#}");
    }

    // ---- bodies ------------------------------------------------------------------------------

    /// Objects pack greedily into bodies under the cap, in order, none over it.
    #[tokio::test]
    async fn a_batch_over_the_body_cap_splits_in_order() {
        let (addr, log) = accepting().await;
        let (registry, out) = metered(addr);
        let mut out = out.with_max_body_bytes(300).with_compression(SplunkCompression::None);
        let b = logs(10);
        out.send(&b).await.unwrap();

        let captured = log.lock().unwrap().clone();
        assert!(captured.len() > 1, "split across {} requests", captured.len());
        for c in &captured {
            assert!(c.body.len() <= 300, "{} bytes", c.body.len());
        }
        let sent: Vec<EventBatch> = captured.iter().flat_map(Captured::decode).collect();
        assert_eq!(messages(&sent), messages(&[b]));
        assert_eq!(total(&registry.drain(0), RECORDS, &[]), 10.0);
    }

    #[test]
    fn packing_is_greedy_and_ordered() {
        assert_eq!(pack([4, 4, 4, 9, 1], 10), [0..2, 2..3, 3..5]);
        assert_eq!(pack([10, 10], 10), [0..1, 1..2]);
        assert_eq!(pack([], 10), Vec::<Range<usize>>::new());
    }

    /// One object over the cap alone is dropped and counted; the rest still goes.
    #[tokio::test]
    async fn an_object_over_the_body_cap_is_dropped_as_oversize() {
        let (addr, log) = accepting().await;
        let (registry, out) = metered(addr);
        let mut out = out.with_max_body_bytes(1_000);
        let b = batch(vec![log_event("small"), log_event(&"x".repeat(2_000)), log_event("after")]);
        out.send(&b).await.unwrap();
        let captured = log.lock().unwrap().clone();
        assert_eq!(captured.len(), 1);
        assert_eq!(messages(&captured[0].decode()), ["small", "after"]);
        let points = registry.drain(0);
        assert_eq!(total(&points, RECORDS_DROPPED, &[("reason", "oversize")]), 1.0);
        assert_eq!(total(&points, RECORDS, &[]), 2.0);
    }

    /// `skip` drops a histogram, counted; `expand` writes the exporter's `_bucket` series.
    #[tokio::test]
    async fn multi_value_skip_counts_and_expand_writes_the_series() {
        let histogram = MetricKind::Histogram(Histogram {
            buckets: vec![(1.0, 2), (5.0, 3)],
            temporality: Temporality::Cumulative,
            sum: Some(9.0),
            min: None,
            max: None,
        });
        let b = batch(vec![Event::metric(
            TS,
            AttrMap::new(),
            MetricRecord::new(intern("latency"), histogram),
        )]);

        let (addr, log) = accepting().await;
        let (registry, mut out) = metered(addr);
        out.send(&b).await.unwrap();
        assert!(paths(&log).is_empty(), "nothing left to send");
        let points = registry.drain(0);
        assert_eq!(
            total(&points, "logit.output.metrics.skipped", &[("metric_kind", "histogram")]),
            1.0
        );

        let (addr, log) = accepting().await;
        let (registry, out) = metered(addr);
        out.with_multi_value(MultiValue::Expand).send(&b).await.unwrap();
        let body = String::from_utf8(log.lock().unwrap()[0].body.clone()).unwrap();
        assert!(body.contains(r#""metric_name:latency_bucket""#), "{body}");
        assert!(body.contains(r#""le":"+Inf""#), "{body}");
        let points = registry.drain(0);
        assert_eq!(
            total(&points, "logit.output.metrics.degraded", &[("metric_kind", "histogram")]),
            1.0
        );
    }

    // ---- responses ---------------------------------------------------------------------------

    async fn fault_for(status: u16, body: &str) -> Fault {
        let body = body.to_string();
        let (addr, _log) = collector(move |_, _| (status, body.clone())).await;
        let err = sink(addr).send(&logs(1)).await.unwrap_err();
        logit_pipeline::classify(&err)
    }

    #[tokio::test]
    async fn each_response_class_maps_to_its_fault() {
        let body = |code| String::from_utf8(encode_status_body(code, None)).unwrap();
        // Nothing of the `send` accepted yet: a busy answer says Splunk didn't take the body.
        for (status, body) in
            [(429, String::new()), (429, body(26)), (503, body(9)), (503, "".into())]
        {
            assert_eq!(fault_for(status, &body).await, Fault::Clean, "{status} {body}");
        }
        // Answers that may have indexed, or say nothing either way.
        for (status, body) in [
            (408, String::new()),
            (500, String::new()),
            (500, body(8)),
            (502, String::new()),
            (503, body(18)),
            (504, String::new()),
        ] {
            assert_eq!(fault_for(status, &body).await, Fault::Ambiguous, "{status} {body}");
        }
        for status in [301, 400, 401, 403, 404, 413] {
            assert_eq!(fault_for(status, "").await, Fault::Permanent, "{status}");
        }
        // A code 6 with no object number, or none in range, is permanent too.
        let code6 = String::from_utf8(encode_status_body(6, None)).unwrap();
        assert_eq!(fault_for(400, &code6).await, Fault::Permanent);
        let out_of_range = String::from_utf8(encode_status_body(6, Some(1))).unwrap();
        assert_eq!(fault_for(400, &out_of_range).await, Fault::Permanent);
    }

    fn encode_status_body(code: u16, n: Option<u64>) -> Vec<u8> {
        let status = HecStatus::from_code(code).unwrap();
        match n {
            Some(n) => encode_invalid_event(status, n),
            None => logit_proto::splunk::response::encode_status(status),
        }
    }

    /// Splunk Cloud 10.5.2605.9's answer to a `useACK` token's request without a channel.
    const CLOUD_CHANNEL_MISSING: &str = r#"{"text":"Data channel is missing. If you have multiple indexers, sticky session load balancers must be provisioned and client requests must be routed accordingly.","code":28}"#;

    /// A missing channel is permanent, whether Splunk Enterprise (code 10) or Splunk Cloud (code
    /// 28) says so.
    #[tokio::test]
    async fn a_missing_channel_is_permanent_on_either_code() {
        let enterprise = r#"{"text":"Data channel is missing","code":10}"#;
        assert_eq!(fault_for(400, enterprise).await, Fault::Permanent);
        assert_eq!(fault_for(400, CLOUD_CHANNEL_MISSING).await, Fault::Permanent);
    }

    /// A rejection is counted by its HEC code, bounded: an unknown code tags `other`.
    #[tokio::test]
    async fn a_rejection_is_counted_by_its_hec_code() {
        for (status, body, tag) in [
            (403, r#"{"text":"Invalid token","code":4}"#, "4"),
            (400, r#"{"text":"Incorrect index","code":7}"#, "7"),
            (400, r#"{"text":"Data channel is missing","code":10}"#, "10"),
            (400, CLOUD_CHANNEL_MISSING, "28"),
            (400, r#"{"text":"?","code":99}"#, "other"),
            (413, "Request Entity Too Large", "other"),
        ] {
            let (addr, _log) = collector(move |_, _| (status, body.to_string())).await;
            let (registry, mut out) = metered(addr);
            out.send(&logs(1)).await.unwrap_err();
            let points = registry.drain(0);
            assert_eq!(total(&points, REQUESTS_REJECTED, &[("code", tag)]), 1.0, "{body}");
            assert_eq!(
                total(&points, REQUESTS, &[("route", "event"), ("class", "4xx")]),
                1.0,
                "{body}"
            );
        }
    }

    /// A code 6 naming object 1 of 4 drops it and resends objects 2 and 3, and nothing else.
    #[tokio::test]
    async fn a_code_6_drops_that_object_and_resends_the_rest_once() {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let (addr, log) = collector(move |_, _| {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                (400, String::from_utf8(encode_status_body(6, Some(1))).unwrap())
            } else {
                success()
            }
        })
        .await;
        let (registry, mut out) = metered(addr);
        out.send(&logs(4)).await.expect("the resend is accepted");

        let captured = log.lock().unwrap().clone();
        assert_eq!(captured.len(), 2);
        assert_eq!(messages(&captured[0].decode()), ["line 0", "line 1", "line 2", "line 3"]);
        assert_eq!(messages(&captured[1].decode()), ["line 2", "line 3"]);
        let points = registry.drain(0);
        assert_eq!(total(&points, RECORDS_DROPPED, &[("reason", "invalid_event")]), 1.0);
        assert_eq!(total(&points, RECORDS, &[]), 3.0, "the one ahead, and the two resent");
        assert_eq!(total(&points, REQUESTS_REJECTED, &[("code", "6")]), 1.0);
    }

    /// A code 6 on the last object needs no resend.
    #[tokio::test]
    async fn a_code_6_on_the_last_object_sends_nothing_more() {
        let (addr, log) =
            collector(|_, _| (400, String::from_utf8(encode_status_body(6, Some(1))).unwrap()))
                .await;
        sink(addr).send(&logs(2)).await.expect("the object ahead counts as indexed");
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    /// A second code 6, on the resend, is permanent.
    #[tokio::test]
    async fn a_second_code_6_is_permanent() {
        let (addr, log) =
            collector(|_, _| (400, String::from_utf8(encode_status_body(6, Some(0))).unwrap()))
                .await;
        let err = sink(addr).send(&logs(3)).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
        let captured = log.lock().unwrap().clone();
        assert_eq!(captured.len(), 2, "one resend, not a third request");
        assert_eq!(messages(&captured[1].decode()), ["line 1", "line 2"]);
    }

    #[test]
    fn after_invalid_event_names_the_object_and_what_follows_it() {
        assert_eq!(after_invalid_event(4, 1), Some((1, 2..4)));
        assert_eq!(after_invalid_event(4, 3), Some((3, 4..4)));
        assert_eq!(after_invalid_event(4, 4), None);
        assert_eq!(after_invalid_event(4, u64::MAX), None);
    }

    /// A busy answer after an earlier body of the `send` was accepted is ambiguous: a `Clean`
    /// retry would index that body twice.
    #[tokio::test]
    async fn a_busy_answer_after_an_accepted_body_is_ambiguous() {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let (addr, log) = collector(move |_, _| {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                success()
            } else {
                (503, String::from_utf8(encode_status_body(9, None)).unwrap())
            }
        })
        .await;
        let mut out = sink(addr).with_max_body_bytes(300);
        let err = out.send(&logs(10)).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
        assert_eq!(log.lock().unwrap().len(), 2, "one body accepted, then a busy answer");
    }

    // ---- Splunk Cloud's oversize answer ------------------------------------------------------

    /// A collector answering as Splunk Cloud does: code 6 naming object 0 for a body over its
    /// cap, and success otherwise.
    async fn cloud_capped() -> (SocketAddr, Log) {
        collector(|_, body| {
            if body.len() > SPLUNK_CLOUD_BODY_CAP {
                (400, String::from_utf8(encode_status_body(6, Some(0))).unwrap())
            } else {
                success()
            }
        })
        .await
    }

    /// A sink whose `max_body_bytes` lets a body past Splunk Cloud's cap, uncompressed to keep
    /// the large bodies cheap.
    fn over_cap_sink(addr: SocketAddr) -> (Arc<Registry>, SplunkHecOutput) {
        let (registry, out) = metered(addr);
        let out = out
            .with_max_body_bytes(16 * 1024 * 1024)
            .with_compression(SplunkCompression::None)
            .with_diagnostics(Diagnostics::new("splunk"));
        (registry, out)
    }

    /// Log events of `bytes` bytes each, told apart by their first character.
    fn large_logs(sizes: &[usize]) -> EventBatch {
        batch(
            sizes
                .iter()
                .enumerate()
                .map(|(i, n)| log_event(&format!("{i}{}", "x".repeat(*n))))
                .collect(),
        )
    }

    /// A body of two objects over the cap is split, and both halves are delivered.
    #[tokio::test]
    async fn a_code_6_at_object_0_over_the_cloud_cap_splits_the_body() {
        let (addr, log) = cloud_capped().await;
        let (registry, mut out) = over_cap_sink(addr);
        out.send(&large_logs(&[3_000_000, 3_000_000])).await.expect("both halves accepted");

        let captured = log.lock().unwrap().clone();
        assert_eq!(captured.len(), 3, "the whole body, then each half");
        assert!(captured[0].body.len() > SPLUNK_CLOUD_BODY_CAP);
        assert_eq!(captured[1].decode()[0].events.len(), 1);
        assert_eq!(captured[2].decode()[0].events.len(), 1);
        let first = |c: &Captured| messages(&c.decode())[0].chars().next().unwrap();
        assert_eq!((first(&captured[1]), first(&captured[2])), ('0', '1'));
        let points = registry.drain(0);
        assert_eq!(total(&points, RECORDS, &[]), 2.0);
        assert_eq!(total(&points, RECORDS_DROPPED, &[]), 0.0);
        assert_eq!(out.diag.occurrences("oversize"), 1);
        assert_eq!(out.diag.occurrences("invalid_event"), 0);
    }

    /// One object over the cap is dropped as oversize, not as an invalid event.
    #[tokio::test]
    async fn a_code_6_at_object_0_over_the_cloud_cap_drops_a_lone_object_as_oversize() {
        let (addr, log) = cloud_capped().await;
        let (registry, mut out) = over_cap_sink(addr);
        out.send(&large_logs(&[6_000_000])).await.expect("the drop is counted, not a fault");

        assert_eq!(log.lock().unwrap().len(), 1, "nothing resent");
        let points = registry.drain(0);
        assert_eq!(total(&points, RECORDS_DROPPED, &[("reason", "oversize")]), 1.0);
        assert_eq!(total(&points, RECORDS_DROPPED, &[("reason", "invalid_event")]), 0.0);
        assert_eq!(total(&points, RECORDS, &[]), 0.0);
    }

    /// A half still over the cap after the one split is permanent.
    #[tokio::test]
    async fn a_half_still_over_the_cloud_cap_is_permanent() {
        let (addr, log) = cloud_capped().await;
        let (_registry, mut out) = over_cap_sink(addr);
        let err = out.send(&large_logs(&[3_000_000, 3_000_000, 3_000_000])).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent, "{err:#}");
        assert_eq!(log.lock().unwrap().len(), 2, "the whole body, then the first half only");
    }

    /// A code 6 naming object 0 of a body at or under the cap keeps the drop-one rule.
    #[tokio::test]
    async fn a_code_6_at_object_0_under_the_cloud_cap_drops_that_object() {
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = calls.clone();
        let (addr, log) = collector(move |_, _| {
            if seen.fetch_add(1, Ordering::SeqCst) == 0 {
                (400, String::from_utf8(encode_status_body(6, Some(0))).unwrap())
            } else {
                success()
            }
        })
        .await;
        let (registry, out) = metered(addr);
        let mut out = out.with_diagnostics(Diagnostics::new("splunk"));
        out.send(&logs(3)).await.expect("the resend is accepted");

        let captured = log.lock().unwrap().clone();
        assert_eq!(captured.len(), 2);
        assert_eq!(messages(&captured[1].decode()), ["line 1", "line 2"]);
        let points = registry.drain(0);
        assert_eq!(total(&points, RECORDS_DROPPED, &[("reason", "invalid_event")]), 1.0);
        assert_eq!(total(&points, RECORDS_DROPPED, &[("reason", "oversize")]), 0.0);
        assert_eq!(out.diag.occurrences("oversize"), 0);
    }

    #[test]
    fn a_body_splits_at_half_its_bytes_and_never_at_an_end() {
        let objects = |sizes: &[usize]| -> Vec<(Vec<u8>, usize)> {
            sizes.iter().map(|n| (vec![b'x'; *n], 1)).collect()
        };
        for (sizes, mid) in [
            (&[3, 3][..], 1),
            (&[1, 1, 1, 1][..], 2),
            (&[10, 1, 1][..], 1),
            (&[1, 1, 10][..], 2),
            (&[3, 3, 3][..], 2),
        ] {
            let owned = objects(sizes);
            let borrowed: Vec<Object<'_>> = owned.iter().map(|(b, r)| (b.as_slice(), *r)).collect();
            assert_eq!(split_point(&borrowed), mid, "{sizes:?}");
        }
    }

    /// The first failing body aborts the ones after it.
    #[tokio::test]
    async fn a_failing_body_aborts_the_rest_of_the_batch() {
        let (addr, log) = collector(|_, _| (503, String::new())).await;
        let mut out = sink(addr).with_max_body_bytes(300);
        out.send(&logs(10)).await.unwrap_err();
        assert_eq!(log.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn connect_refused_is_clean() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let err = sink(addr).send(&logs(1)).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    /// A collector that answers one request with `status` and `body`, closes the connection,
    /// and stops listening, so the next request's connect is refused.
    async fn answers_once(status: u16, body: String) -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
        let seen = bodies.clone();
        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            drop(listener);
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let body_start = loop {
                let n = stream.read(&mut chunk).await.unwrap();
                buf.extend_from_slice(&chunk[..n]);
                if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break i + 4;
                }
            };
            let head = String::from_utf8_lossy(&buf[..body_start]).to_ascii_lowercase();
            let length: usize = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .map(|v| v.trim().parse().unwrap())
                .unwrap();
            while buf.len() < body_start + length {
                let n = stream.read(&mut chunk).await.unwrap();
                buf.extend_from_slice(&chunk[..n]);
            }
            seen.lock().unwrap().push(buf[body_start..].to_vec());
            let response = format!(
                "HTTP/1.1 {status} X\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            stream.shutdown().await.unwrap();
        });
        (addr, bodies)
    }

    /// Once a body was accepted, a connect failure on the next is ambiguous, not clean: a
    /// `Clean` retry would index the first body twice.
    #[tokio::test]
    async fn a_connect_failure_after_an_accepted_body_is_ambiguous() {
        let (addr, bodies) = answers_once(200, r#"{"text":"Success","code":0}"#.into()).await;
        let mut out = sink(addr).with_max_body_bytes(300);
        let err = out.send(&logs(10)).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
        assert_eq!(bodies.lock().unwrap().len(), 1, "one body accepted, then refused");
    }

    /// The same after a code 6 whose objects ahead count as indexed: the resend's connect
    /// failure is ambiguous. With nothing ahead of the named object, it stays clean.
    #[tokio::test]
    async fn a_connect_failure_on_a_code_6_resend_is_ambiguous_once_objects_counted() {
        for (n, fault) in [(1, Fault::Ambiguous), (0, Fault::Clean)] {
            let body = String::from_utf8(encode_status_body(6, Some(n))).unwrap();
            let (addr, _bodies) = answers_once(400, body).await;
            let err = sink(addr).send(&logs(3)).await.unwrap_err();
            assert_eq!(logit_pipeline::classify(&err), fault, "n={n}: {err:#}");
        }
    }

    /// A code 6 naming no object of the body is permanent, with a `request_rejected` diagnostic.
    #[tokio::test]
    async fn a_code_6_out_of_range_is_diagnosed() {
        use tracing_subscriber::util::SubscriberInitExt as _;

        let body = String::from_utf8(encode_status_body(6, Some(5))).unwrap();
        let (addr, _log) = collector(move |_, _| (400, body.clone())).await;
        let captured = CapturedLogs::default();
        let guard = tracing_subscriber::fmt()
            .with_writer(captured.clone())
            .with_ansi(false)
            .finish()
            .set_default();
        let mut out = sink(addr).with_diagnostics(Diagnostics::new("splunk"));
        let err = out.send(&logs(2)).await.unwrap_err();
        drop(guard);
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
        let logged = String::from_utf8_lossy(&captured.0.lock().unwrap()).into_owned();
        assert!(logged.contains("at object 5, which isn't one of them"), "{logged}");
        assert_eq!(out.diag.occurrences("request_rejected"), 1);
    }

    // ---- acknowledgment ----------------------------------------------------------------------

    /// A collector that answers every `/event` with a fresh `ackId` from 1, and each `/ack` poll
    /// with `acked(poll number)` for every id asked.
    async fn acking(acked: impl Fn(usize) -> bool + Send + Sync + 'static) -> (SocketAddr, Log) {
        let next_id = Arc::new(AtomicUsize::new(1));
        let polls = Arc::new(AtomicUsize::new(0));
        collector(move |path, body| {
            if path.ends_with("/ack") {
                let n = polls.fetch_add(1, Ordering::SeqCst);
                let ids = logit_proto::splunk::response::parse_ack_request(body).unwrap();
                let answers: Vec<(u64, bool)> = ids.iter().map(|id| (*id, acked(n))).collect();
                (200, String::from_utf8(encode_ack_reply(&answers)).unwrap())
            } else {
                let id = next_id.fetch_add(1, Ordering::SeqCst) as u64;
                (200, String::from_utf8(encode_success(Some(id))).unwrap())
            }
        })
        .await
    }

    /// A sink under `ack: true` whose poll schedule is [`ACK_BACKOFF`] in milliseconds rather
    /// than seconds, so the tests run on real time: a paused clock would auto-advance past the
    /// request timeout while a request's socket I/O is in flight.
    fn acked_sink(addr: SocketAddr, registry: &Registry, ack_timeout: Duration) -> SplunkHecOutput {
        let mut out = sink(addr)
            .with_ack(true, ack_timeout)
            .with_max_body_bytes(300)
            .with_telemetry(registry.telemetry_for("out", "splunk_hec_out", "sink"));
        out.ack_backoff = ACK_BACKOFF.map(|wait| wait / 1_000);
        out
    }

    /// Ids answered `false` and then `true` are polled until acknowledged, every one asked for.
    #[tokio::test]
    async fn acks_answered_false_then_true_succeed() {
        let (addr, log) = acking(|poll| poll >= 2).await;
        let registry = Registry::new();
        let started = std::time::Instant::now();
        acked_sink(addr, &registry, Duration::from_secs(60))
            .send(&logs(10))
            .await
            .expect("acknowledged");
        let posts = paths(&log).iter().filter(|p| p.ends_with("/event")).count();
        assert!(posts > 1, "{posts} bodies");
        let polls: Vec<Captured> =
            log.lock().unwrap().iter().filter(|c| c.path.ends_with("/ack")).cloned().collect();
        assert_eq!(polls.len(), 3);
        assert!(polls[0].header(CHANNEL_HEADER).is_some(), "polls name the channel too");
        let asked = logit_proto::splunk::response::parse_ack_request(&polls[0].body).unwrap();
        assert_eq!(asked, (1..=posts as u64).collect::<Vec<_>>());
        assert!(started.elapsed() >= Duration::from_micros(3_500), "0.5, 1, and 2 ms waits");
        assert_eq!(total(&registry.drain(0), ACKS, &[("result", "acked")]), posts as f64);
    }

    /// Ids never acknowledged time out as an ambiguous fault, after a last poll at the deadline.
    #[tokio::test]
    async fn acks_that_never_arrive_time_out_ambiguous() {
        let (addr, log) = acking(|_| false).await;
        let registry = Registry::new();
        let started = std::time::Instant::now();
        let mut out = acked_sink(addr, &registry, Duration::from_millis(12));
        let err = out.send(&logs(1)).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(started.elapsed() >= Duration::from_millis(12));
        // At most 0.5, 1.5, 3.5, 8.5, and the deadline at 12; a slow poll can push the deadline
        // ahead of a scheduled one.
        let polls = paths(&log).iter().filter(|p| p.ends_with("/ack")).count();
        assert!((2..=5).contains(&polls), "{polls} polls");
        assert_eq!(total(&registry.drain(0), ACKS, &[("result", "timeout")]), 1.0);
    }

    /// A 2xx with no `ackId` is success, counted `unsupported`, and nothing is polled.
    #[tokio::test]
    async fn a_success_without_an_ack_id_counts_as_delivered() {
        let (addr, log) = accepting().await;
        let registry = Registry::new();
        acked_sink(addr, &registry, Duration::from_secs(60)).send(&logs(1)).await.unwrap();
        assert_eq!(paths(&log), ["/services/collector/event"]);
        assert_eq!(total(&registry.drain(0), ACKS, &[("result", "unsupported")]), 1.0);
    }

    /// A poll answered code 14 (`ACK is disabled`) is success, counted `unsupported`.
    #[tokio::test]
    async fn a_poll_answered_ack_disabled_counts_as_delivered() {
        let (addr, log) = collector(|path, _| {
            if path.ends_with("/ack") {
                (400, String::from_utf8(encode_status_body(14, None)).unwrap())
            } else {
                (200, String::from_utf8(encode_success(Some(1))).unwrap())
            }
        })
        .await;
        let registry = Registry::new();
        acked_sink(addr, &registry, Duration::from_secs(60)).send(&logs(1)).await.unwrap();
        assert_eq!(paths(&log).len(), 2);
        assert_eq!(total(&registry.drain(0), ACKS, &[("result", "unsupported")]), 1.0);
    }

    // ---- the token ---------------------------------------------------------------------------

    /// Collects rendered `tracing` output.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogs;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// A `403` and a `400` whose bodies echo the token are diagnosed and reported with it
    /// redacted.
    #[tokio::test]
    async fn the_token_never_reaches_a_diagnostic_or_an_error() {
        use tracing_subscriber::util::SubscriberInitExt as _;

        for status in [403, 400] {
            let (addr, _log) = collector(move |_, _| {
                (status, format!(r#"{{"text":"Invalid token {TOKEN}","code":4}}"#))
            })
            .await;
            let logs = CapturedLogs::default();
            let guard = tracing_subscriber::fmt()
                .with_writer(logs.clone())
                .with_ansi(false)
                .finish()
                .set_default();
            let mut out = sink(addr).with_diagnostics(Diagnostics::new("splunk"));
            let err = out.send(&batch(vec![log_event("x")])).await.unwrap_err();
            drop(guard);

            let logged = String::from_utf8_lossy(&logs.0.lock().unwrap()).into_owned();
            assert!(logged.contains("<redacted>"), "{logged}");
            assert!(!logged.contains(TOKEN), "{logged}");
            let message = format!("{err:#}");
            assert!(message.contains("<redacted>"), "{message}");
            assert!(!message.contains(TOKEN), "{message}");
            assert!(!format!("{err:?}").contains(TOKEN));
        }
    }

    #[test]
    fn splunk_hec_output_is_not_duplicate_safe() {
        assert!(!SplunkHecOutput::new("http://h/services/collector", TOKEN)
            .unwrap()
            .duplicate_safe());
    }
}
