//! InfluxDB 2.x line-protocol output: `POST /api/v2/write` with org/bucket query params and a
//! `Token` auth header. Matches the `influxdb` service seeded in `compose.yaml`.
//!
//! ## Destination resolution
//!
//! Each batch is one request, and its query parameters are resolved once per batch from the
//! config and the batch resource's private carriers, which `influxdb_in` sets from the request it
//! received ([`RESOURCE_ATTR_BUCKET`], [`RESOURCE_ATTR_ORG`], [`RESOURCE_ATTR_ORG_ID`]). A
//! carrier whose value is empty or isn't a string reads as absent, as `influxdb_in` reads an empty
//! query parameter. A retry resolves the same batch to the same parameters.
//!
//! - **`bucket`**: a configured `bucket:` is the fixed mode: every batch goes there and a carried
//!   bucket is ignored. With no `bucket:`, the carried mode: the carried bucket, else
//!   `default_bucket:`. With neither, the batch isn't sent: each of its metric records counts
//!   `logit.output.metrics.dropped{reason="no_bucket"}`, a throttled `no_bucket` diagnostic says
//!   why, and `send` returns `Ok`. Configuring both is a graph error.
//! - **`org`**: a configured `org:` is sent as `org=` alone, in both modes. A carried `orgID` is
//!   never sent beside it, because InfluxDB 2.7 reads `orgID` over `org` when a request has both,
//!   which would defeat the override. With no `org:`, the carried values are forwarded: `org=`
//!   from the carried name and `orgID=` from the carried ID, both when both were carried. A batch
//!   that carries neither is sent with `org=` from `default_org:`. With none of these, the request
//!   names no organization: InfluxDB 3 ignores it, and InfluxDB 2.x answers `400`
//!   `Please provide either orgID or org`, the `400` row below. Unlike a missing bucket, the sink
//!   sends that batch: the `400` is `Rejected`, and `write_loop` counts the drop as
//!   `logit.component.batches.dropped{reason="rejected"}`. Configuring both `org:` and
//!   `default_org:` is a graph error.
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
//! ## Concurrency
//!
//! `concurrency:` (default 1) bounds how many batches are in flight. At 1, [`InfluxDbOutput::send`]
//! makes each request in turn. Above 1, each batch's request runs in its own task through
//! [`crate::http::Slots`], and `write_loop` settles them oldest first: a fault on one batch
//! resends or drops that batch alone, under the same response classes as `send`. Batches can
//! land out of order. That's safe here: InfluxDB keeps the last write of a point by
//! `(measurement, tag set, timestamp)`, so two batches writing one point with different values
//! leave whichever landed later, the same outcome as a fan-in topology at concurrency 1.
//! Over TLS to a server that picks h2 through ALPN, the batches in flight share one connection as
//! concurrent streams; otherwise they open up to `concurrency` connections.
//! `logit.output.in_flight` reads the batches in flight, and `logit.component.send.duration`
//! times each batch's request from its send to its response.
//!
//! `tls:` tunes an `https://` url: a private `ca_file`, a client certificate, or
//! `insecure_skip_verify` ([`InfluxDbOutput::with_tls`]). Empty, the client keeps `reqwest`'s
//! default trust. The files reload without a restart, as every TLS sink's do.
//!
//! `idle_timeout:` (default `30s`) closes a pooled connection idle past it instead of reusing it,
//! so the sink closes an idle connection before the destination can ([`crate::http::build_client`];
//! `docs/adr/sink-request-concurrency.md`, decision 7).
//!
//! `connect_timeout:` (default `5s`) bounds a new connection's DNS lookup, TCP dial, and TLS
//! handshake together. A dial it ends sent nothing: `Clean`, counted `connect_error`
//! ([`crate::http::build_client`]). Keep it below the sink's fixed 10 s
//! request timeout, which otherwise ends a stalled dial first as a timeout.
//!
//! ## Response classes
//!
//! A non-2xx answer carries InfluxDB's error JSON (`code` and `message`; a parse error's message
//! quotes each offending line), which the error and the `send_failed` warning quote, read bounded
//! ([`crate::http::read_body_prefix`]). On `/api/v2/write` each documented status maps to one
//! `code`, so the status decides the class: no documented `400` names an unknown bucket or org,
//! and no documented `404` names the body. Every row is [`crate::http::classify_status`]'s except
//! a `403` or `404` naming a carried value. Which value a `403` or `404` names, and where that
//! value came from for this batch (see "Destination resolution"), decides its class:
//!
//! - A value from the config (`bucket:`, `default_bucket:`, `org:`, or `default_org:`) is the
//!   operator's, and every batch that uses it gets the same answer, so it's `Refused` and holds
//!   the queue.
//! - A value carried on the batch came from a relay's sender, so a missing or forbidden one is
//!   that batch's fault: it's `Rejected`, and holding on it would wedge the sink on one stray
//!   bucket. `splunk_hec_out` reads a code 7 for a carried index the same way
//!   ([`crate::splunk`], "Response classes").
//!
//! A `404`'s `message` names the bucket or the org. InfluxDB 2.x looks a bucket name up inside
//! the org the request names, so a `404` naming the bucket is `Rejected` when either the bucket
//! or the org was carried: a configured bucket missing from a carried org is that org's doing. A
//! `404` naming the org is `Rejected` only when the org was carried. A `403` names neither: a
//! bucket outside the token's scope and an org the token doesn't reach both produce it, so it's
//! `Rejected` when either was carried. A `401` is the token's, so every batch gets it. A batch
//! that carries no value, which is every batch when both `bucket:` and `org:` are configured,
//! reads every `403` and `404` as `Refused`. The bodies quoted are InfluxDB 2.7.12's, verified
//! against it and InfluxDB 3.8.0 Core.
//!
//! | Response | Class | Why | Evidence |
//! |---|---|---|---|
//! | `204` | `Ok` | every point written (OSS) or queued (Cloud) | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | `400` `invalid` (`unable to parse '<line>': <reason>`) | `Rejected` | malformed line protocol: "all data rejected", and a resend parses the same | [troubleshoot writes](https://docs.influxdata.com/influxdb/v2/write-data/troubleshoot/) |
//! | `400` `invalid` (`Please provide either orgID or org`) | `Rejected` | InfluxDB 2.x with no `org:` or `default_org:` configured and none carried; a resend names none either | verified against InfluxDB 2.7.12 |
//! | `401` `unauthorized` (`unauthorized access`; InfluxDB 3: `{"error": "the request was not authenticated"}`) | `Refused` | a missing or unknown token: every batch gets it, in both modes | [write API](https://docs.influxdata.com/influxdb/v2/api/write/), verified against InfluxDB 2.7.12 and 3.8.0 Core |
//! | `403` `forbidden` (`insufficient permissions for write`), bucket from `bucket:` or `default_bucket:` and org from `org:`, `default_org:`, or none | `Refused` | the token can't write the operator's bucket (a token scoped to another bucket or org, or a read-only one): every batch sent there gets it | `checkBucketWritePermissions` in [`http/write_handler.go`](https://github.com/influxdata/influxdb/blob/master/http/write_handler.go), verified against InfluxDB 2.7.12 |
//! | `403` `forbidden`, bucket or org carried | `Rejected` | the token can't write this batch's bucket, or doesn't reach its org (an all-access token for another org); a batch naming another may land | verified against InfluxDB 2.7.12 |
//! | `404` `not found` (`bucket "<name>" not found`, or `bucket not found` for an empty name), bucket from `bucket:` or `default_bucket:` and org from `org:`, `default_org:`, or none | `Refused` | the operator's bucket doesn't exist: every batch sent there gets it until the operator creates it | [write API](https://docs.influxdata.com/influxdb/v2/api/write/), `decodeWriteRequest` in [`http/write_handler.go`](https://github.com/influxdata/influxdb/blob/master/http/write_handler.go), verified against InfluxDB 2.7.12 |
//! | `404` `not found` naming the bucket, bucket or org carried | `Rejected` | this batch's bucket doesn't exist, or doesn't exist in this batch's org; a batch naming another may land. InfluxDB 3 creates a missing database instead of answering `404` | verified against InfluxDB 2.7.12 and 3.8.0 Core |
//! | `404` `not found` (`organization name "<name>" not found`, or `organization not found` for an `orgID`), org from `org:` or `default_org:` | `Refused` | the operator's org doesn't exist: every batch gets it | `decodeWriteRequest` in [`http/write_handler.go`](https://github.com/influxdata/influxdb/blob/master/http/write_handler.go), verified against InfluxDB 2.7.12 |
//! | `404` `not found` naming the org, org carried | `Rejected` | this batch's org doesn't exist; a batch naming another may land | verified against InfluxDB 2.7.12 |
//! | `404` with any other body, bucket or org carried | `Rejected` | the body names neither, and holding would wedge the sink on a value one sender chose | -- |
//! | `404` with any other body, nothing carried | `Refused` | every value came from the config | [`crate::http::classify_status`] |
//! | `408` | `Ambiguous` | the server or a proxy in front of it (nginx `client_body_timeout`, HAProxy `timeout http-request`) gave up waiting for the request to arrive, and RFC 9110 lets a client repeat it; InfluxDB doesn't document the status, so this is [`crate::http::classify_status`]'s class. A `408` that repeats every attempt holds the queue | [RFC 9110, 15.5.9](https://www.rfc-editor.org/rfc/rfc9110#name-408-request-timeout) |
//! | `413` | `Rejected` | the body is over the server's limit, "no data written"; a smaller batch would land | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | `422` `unprocessable entity` (`failure writing points to database`, a partial write such as points outside the bucket's retention) | `Rejected` | the server applied what it could and refused the rest for its content; a resend repeats the refusal and overwrites what landed | [write API](https://docs.influxdata.com/influxdb/v2/api/write/), [troubleshoot writes](https://docs.influxdata.com/influxdb/v2/write-data/troubleshoot/) |
//! | `429` `too many requests` (Cloud) | `Ambiguous` | a rate or quota limit, `Retry-After` set; the next attempt may land | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | `500` `internal error` | `Ambiguous` | "unexpected error writing points"; may have been applied | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | `503` `unavailable` | `Ambiguous` | the server can't take writes for now, `Retry-After` set | [write API](https://docs.influxdata.com/influxdb/v2/api/write/) |
//! | any other status | [`crate::http::classify_status`]'s class | undocumented on this route | -- |
//! | any `3xx` | `Rejected` | redirects are off: a moved endpoint is a config change | [`crate::http::build_client`] |
//! | connect failure, DNS failure | `Clean` | nothing left the process | [`crate::http::classify_reqwest_error`] |
//! | a dial `connect_timeout:` ended (the DNS lookup, TCP dial, and TLS handshake together) | `Clean` | nothing left the process | [`crate::http::classify_reqwest_error`]; `http::tests::a_stalled_handshake_ends_at_the_connect_timeout_as_connect_error`, `http::tests::a_dial_to_a_full_accept_queue_ends_at_the_connect_timeout_as_connect_error` |
//! | a request timeout after the connection opened, or a stalled dial the fixed 10 s request timeout ended first because `connect_timeout:` is at or above it; any other transport error | `Ambiguous` | the write may have reached the server | [`crate::http::classify_reqwest_error`] |
//!
//! `Retry-After` is ignored: `write_loop`'s backoff applies.

use crate::accounting::BatchAccounting;
use crate::http::{body_snippet, Exchange, Reply, Slots, ERROR_BODY_SNIPPET_BYTES};
use crate::Output;
use anyhow::Context;
use logit_core::{Diagnostics, EventBatch, Resource, Telemetry, Value};
use logit_pipeline::{BatchContext, DeliveryPosture, Fault, SeqId};
pub use logit_proto::influxdb::encode::{EncodeStats, InfluxLineEncoder};
use logit_proto::influxdb::{RESOURCE_ATTR_BUCKET, RESOURCE_ATTR_ORG, RESOURCE_ATTR_ORG_ID};
use logit_proto::Encoder;
use std::path::Path;
use std::time::Duration;

/// `tls:`: the shared `crate::tls` type, re-exported as the other sinks do.
pub use crate::tls::TlsClientSettings;

/// `reqwest` has no request timeout by default; without one, a server that accepts the connection
/// but never responds hangs `send`, and the pipeline worker driving it, forever.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Where a batch's bucket comes from (the module doc's "Destination resolution").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Destination {
    /// A configured `bucket:`: every batch goes here, and a carried bucket is ignored.
    Fixed(String),
    /// No `bucket:`: the batch's carried bucket, else `default_bucket`, else the batch is dropped.
    Carried { default_bucket: Option<String> },
}

impl Destination {
    /// The destination a config's `bucket:` and `default_bucket:` describe. Graph validation
    /// rejects both set; were they, `bucket` would win.
    pub fn from_config(bucket: Option<String>, default_bucket: Option<String>) -> Self {
        match bucket {
            Some(bucket) => Destination::Fixed(bucket),
            None => Destination::Carried { default_bucket },
        }
    }
}

/// Where a batch's organization comes from (the module doc's "Destination resolution").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Organization {
    /// A configured `org:`: sent as `org=` alone, and a carried org or org ID is ignored.
    Fixed(String),
    /// No `org:`: the batch's carried org and org ID, else `default_org`, else none.
    Carried { default_org: Option<String> },
}

impl Organization {
    /// The organization a config's `org:` and `default_org:` describe. Graph validation rejects
    /// both set; were they, `org` would win.
    pub fn from_config(org: Option<String>, default_org: Option<String>) -> Self {
        match org {
            Some(org) => Organization::Fixed(org),
            None => Organization::Carried { default_org },
        }
    }
}

/// One batch's resolved `/api/v2/write` query parameters, borrowed from the config and the batch
/// resource.
#[derive(Debug, PartialEq, Eq)]
struct WriteParams<'a> {
    org: Option<&'a str>,
    org_id: Option<&'a str>,
    bucket: &'a str,
    /// Whether `bucket` came from the batch's carrier, not `bucket:` or `default_bucket:`.
    bucket_carried: bool,
    /// Whether `org`/`org_id` came from the batch's carriers, not `org:` or `default_org:`.
    org_carried: bool,
}

impl WriteParams<'_> {
    fn carried(&self) -> Carried {
        Carried { bucket: self.bucket_carried, org: self.org_carried }
    }

    fn query(&self) -> Vec<(&str, &str)> {
        let mut query = Vec::with_capacity(4);
        if let Some(org) = self.org {
            query.push(("org", org));
        }
        if let Some(org_id) = self.org_id {
            query.push(("orgID", org_id));
        }
        query.push(("bucket", self.bucket));
        query.push(("precision", "ns"));
        query
    }
}

/// Which of a write's values came from the batch's carriers: all its answer's class needs once
/// the request has left the loop task.
#[derive(Debug, Clone, Copy)]
struct Carried {
    bucket: bool,
    org: bool,
}

impl Carried {
    /// The class of a non-2xx answer to a write (the module doc's "Response classes"). A `403`
    /// or `404` is the batch's own, `Rejected`, only when the value it names, or the org a
    /// bucket is looked up in, came from the batch's carriers; one naming only configured
    /// values hits every batch, so it holds, as [`crate::http::classify_status`] reads it.
    fn classify(self, status: reqwest::StatusCode, body: &str) -> Fault {
        let carried = match status.as_u16() {
            403 => self.bucket || self.org,
            404 => match NotFound::of(body) {
                NotFound::Bucket => self.bucket || self.org,
                NotFound::Org => self.org,
                NotFound::Other => self.bucket || self.org,
            },
            _ => false,
        };
        if carried {
            Fault::Rejected
        } else {
            crate::http::classify_status(status)
        }
    }
}

/// What a `404` says is missing, read from its error JSON's `message`.
#[derive(Debug, PartialEq, Eq)]
enum NotFound {
    /// `bucket "<name>" not found`, or `bucket not found` for an empty bucket.
    Bucket,
    /// `organization name "<name>" not found`, or `organization not found` for an org ID.
    Org,
    /// Any other body, a truncated or non-JSON one included.
    Other,
}

impl NotFound {
    fn of(body: &str) -> Self {
        let message = serde_json::from_str::<serde_json::Value>(body).ok().and_then(|json| {
            json.get("message").and_then(serde_json::Value::as_str).map(str::to_owned)
        });
        match message.as_deref() {
            Some(m) if m.starts_with("bucket ") && m.ends_with(" not found") => NotFound::Bucket,
            Some(m) if m.starts_with("organization ") && m.ends_with(" not found") => NotFound::Org,
            _ => NotFound::Other,
        }
    }
}

/// A carrier attribute's string value; a missing, empty, or non-string one is absent.
fn carried<'a>(resource: &'a Resource, key: &str) -> Option<&'a str> {
    resource.attributes.get(key).and_then(Value::as_str).filter(|value| !value.is_empty())
}

pub struct InfluxDbOutput {
    writer: Writer,
    encoder: InfluxLineEncoder,
    /// Layer-3 detail (`docs/design/internal-telemetry.md`): the response class, which
    /// `run_output`'s `logit.component.send.*` can't see inside one `send`. Ungated; the encoder's
    /// diagnostics are gated by `accounting` (`crate::accounting`).
    telemetry: Telemetry,
    /// The sink's own diagnostics: the `no_bucket` warning. A `no_bucket` drop returns `Ok`, a
    /// final send, so it's never repeated by a retry and needs no gate.
    diag: Diagnostics,
    /// The `send` path's batch accounting, at a concurrency of 1.
    accounting: BatchAccounting,
    /// The batches in flight above a concurrency of 1, each with its own accounting over
    /// `accounting`'s gate (the module doc's "Concurrency").
    slots: Slots<(Carried, Exchange)>,
    /// The operator's `idle_timeout:` ([`crate::http::build_client`]).
    idle_timeout: Duration,
    /// The operator's `connect_timeout:` ([`crate::http::build_client`]).
    connect_timeout: Duration,
    /// Built by [`InfluxDbOutput::with_tls`]; `None` keeps `reqwest`'s default trust.
    tls: Option<rustls::ClientConfig>,
}

/// Where and how a batch is written: everything a request needs from the config.
struct Writer {
    url: String,
    organization: Organization,
    destination: Destination,
    token: String,
    client: reqwest::Client,
    /// Per-request timeout: what `with_timeout` set, else [`DEFAULT_TIMEOUT`]. `send` makes one
    /// attempt, so there's no retry budget to clamp it against.
    request_timeout: Duration,
}

impl InfluxDbOutput {
    pub fn new(
        url: String,
        organization: Organization,
        destination: Destination,
        token: String,
    ) -> Self {
        let accounting = BatchAccounting::default();
        Self {
            writer: Writer {
                url,
                organization,
                destination,
                token,
                client: crate::http::build_client(
                    DEFAULT_TIMEOUT,
                    crate::http::DEFAULT_CONNECT_TIMEOUT,
                    crate::http::DEFAULT_IDLE_TIMEOUT,
                    None,
                ),
                request_timeout: DEFAULT_TIMEOUT,
            },
            encoder: InfluxLineEncoder::default(),
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
            slots: Slots::new(accounting.gate().clone()),
            accounting,
            idle_timeout: crate::http::DEFAULT_IDLE_TIMEOUT,
            connect_timeout: crate::http::DEFAULT_CONNECT_TIMEOUT,
            tls: None,
        }
    }

    /// Rebuilds the client from the timeouts and TLS config set so far.
    fn rebuild_client(&mut self) {
        self.writer.client = crate::http::build_client(
            self.writer.request_timeout,
            self.connect_timeout,
            self.idle_timeout,
            self.tls.as_ref(),
        );
    }

    /// Sets how many batches may be in flight at once (`concurrency:` in config, default 1).
    pub fn with_concurrency(mut self, concurrency: u32) -> Self {
        self.slots.set_concurrency(concurrency);
        self
    }

    /// How long a pooled connection may sit idle before it's closed instead of reused
    /// (`idle_timeout:` in config, default [`crate::http::DEFAULT_IDLE_TIMEOUT`]). Rebuilds the
    /// client.
    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self.rebuild_client();
        self
    }

    /// How long a new connection may take to open, DNS lookup, TCP dial, and TLS handshake
    /// together (`connect_timeout:` in config, default [`crate::http::DEFAULT_CONNECT_TIMEOUT`]).
    /// Rebuilds the client.
    pub fn with_connect_timeout(mut self, connect_timeout: Duration) -> Self {
        self.connect_timeout = connect_timeout;
        self.rebuild_client();
        self
    }

    /// Overrides the default 10s request timeout. Rebuilds the underlying HTTP client.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.writer.request_timeout = timeout;
        self.rebuild_client();
        self
    }

    /// Client TLS tuning (`tls:`) for an `https://` url. A no-op when `settings` is empty.
    ///
    /// Registers the files with `reloader` under this sink's diagnostics and telemetry as they are
    /// when this runs, so call it after `with_diagnostics` and `with_telemetry`.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
        reloader: &logit_pipeline::tls::TlsReloader,
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
        self.tls = Some(logit_pipeline::tls::build_client_config(
            settings,
            base_dir,
            reloader,
            &self.diag,
            &self.telemetry,
        )?);
        self.rebuild_client();
        Ok(self)
    }

    /// Attaches the encoder's diagnostics handle (per-metric encode failures), gated by this
    /// sink's batch accounting. Retry diagnostics come from `logit-pipeline`'s writer, not this
    /// sink.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.encoder = self.encoder.with_diagnostics(diag.gated(self.accounting.gate()));
        self.diag = diag;
        self
    }

    /// Attaches the layer-3 telemetry handle.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.slots.set_telemetry(telemetry.clone());
        self.telemetry = telemetry;
        self
    }
}

/// The encode half of one attempt, on the caller's task: the request to send and what its
/// answer's class needs, or `None` when there's nothing to send. Encode-side counts go through
/// `accounting`, so a retry of the same batch counts them once.
fn prepare(
    writer: &Writer,
    encoder: &mut InfluxLineEncoder,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
    accounting: &mut BatchAccounting,
    batch: &EventBatch,
) -> anyhow::Result<Option<(reqwest::RequestBuilder, Carried)>> {
    let Some(params) = resolve(&writer.organization, &writer.destination, &batch.resource) else {
        drop_no_bucket(telemetry, diag, batch);
        return Ok(None);
    };
    let (first, body) = accounting.encode(0, || encoder.encode(batch));
    let body = body?;
    // Before the empty-body return: a batch whose every line was unencodable still counts what
    // it dropped, omitted, or normalized, and an operator chasing a missing point needs to see
    // it.
    if first {
        emit_encode_stats(telemetry, encoder.stats());
    }
    if body.is_empty() {
        // Nothing had a line-protocol encoding (log- or span-only events, or every metric was
        // dropped or skipped). Not an error; nothing to write.
        return Ok(None);
    }
    if first {
        telemetry.count("logit.output.batch.bytes", body.len() as f64, &[]);
    }

    let write_url = format!("{}/api/v2/write", writer.url.trim_end_matches('/'));
    let request = writer
        .client
        .post(&write_url)
        .query(&params.query())
        .header("Authorization", format!("Token {}", writer.token))
        .header("Content-Type", "text/plain; charset=utf-8")
        .timeout(writer.request_timeout)
        .body(body);
    Ok(Some((request, params.carried())))
}

/// The answer half of one attempt, on the caller's task: counts the request and classifies its
/// answer as `.context(fault)` (the module doc's "Response classes").
fn settle(telemetry: &Telemetry, carried: Carried, exchange: Exchange) -> anyhow::Result<()> {
    telemetry.timing("logit.output.request.duration", exchange.took, &[]);
    telemetry.count("logit.output.requests", 1.0, &[("class", exchange.reply.class())]);
    match exchange.reply {
        Reply::Success { .. } => Ok(()),
        Reply::Failure { status, body, .. } => {
            let fault = carried.classify(status, &body);
            let text = body_snippet(&body, ERROR_BODY_SNIPPET_BYTES);
            Err(anyhow::anyhow!("InfluxDB write failed ({status}): {text}")).context(fault)
        }
        Reply::Transport(err) => {
            let fault = crate::http::classify_reqwest_error(&err);
            Err(anyhow::Error::new(err)).context(fault)
        }
    }
}

/// Counts and reports a batch [`resolve`] found no bucket for, which this sink drops unsent.
/// Public so a caller that encodes as this sink does without it counts the drop as it would.
pub fn drop_no_bucket(telemetry: &Telemetry, diag: &mut Diagnostics, batch: &EventBatch) {
    let metrics: usize = batch.events.iter().map(|event| event.metrics.len()).sum();
    if metrics > 0 {
        telemetry.count("logit.output.metrics.dropped", metrics as f64, &[("reason", "no_bucket")]);
        diag.warn_throttled(
            "no_bucket",
            format_args!(
                "dropped a batch of {metrics} metrics that names no bucket: no 'bucket' or \
                 'default_bucket' is configured and it didn't come from an influxdb_in"
            ),
        );
    }
}

/// The `org`, `orgID`, and `bucket` this sink writes a batch under `resource` to, or `None`
/// when it drops the batch for naming no bucket ([`drop_no_bucket`]). Public so a caller can
/// resolve as this sink does without it.
pub fn destination<'a>(
    organization: &'a Organization,
    destination: &'a Destination,
    resource: &'a Resource,
) -> Option<(Option<&'a str>, Option<&'a str>, &'a str)> {
    resolve(organization, destination, resource).map(|p| (p.org, p.org_id, p.bucket))
}

/// One batch's query parameters (the module doc's "Destination resolution"), or `None` for a
/// carried-mode batch with no bucket carried and no `default_bucket`.
fn resolve<'a>(
    organization: &'a Organization,
    destination: &'a Destination,
    resource: &'a Resource,
) -> Option<WriteParams<'a>> {
    let (bucket, bucket_carried) = match destination {
        Destination::Fixed(bucket) => (bucket.as_str(), false),
        Destination::Carried { default_bucket } => match carried(resource, RESOURCE_ATTR_BUCKET) {
            Some(bucket) => (bucket, true),
            None => (default_bucket.as_deref()?, false),
        },
    };
    let (org, org_id, org_carried) = match organization {
        Organization::Fixed(org) => (Some(org.as_str()), None, false),
        Organization::Carried { default_org } => {
            match (carried(resource, RESOURCE_ATTR_ORG), carried(resource, RESOURCE_ATTR_ORG_ID)) {
                (None, None) => (default_org.as_deref(), None, false),
                (org, org_id) => (org, org_id, true),
            }
        }
    };
    Some(WriteParams { org, org_id, bucket, bucket_carried, org_carried })
}

#[async_trait::async_trait]
impl Output for InfluxDbOutput {
    /// Arms this sink's batch accounting (`crate::accounting`), or under a window above 1 the
    /// batch's own slot.
    fn observe_batch(&mut self, _ctx: BatchContext, seq: SeqId) {
        if self.slots.window() > 1 {
            self.slots.observe(seq);
        } else {
            self.accounting.observe();
        }
    }

    fn observe_posture(&mut self, posture: DeliveryPosture) {
        self.accounting.observe_posture(posture);
        self.slots.observe_posture(posture);
    }

    /// One attempt, one request, no retry: retry timing belongs to `logit-pipeline`'s writer
    /// (`docs/adr/buffered-sink-delivery.md`). A final result (`Ok`, or a fault `write_loop`
    /// won't retry) disarms the batch accounting on every path, a batch that encoded to nothing
    /// included.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let prepared = prepare(
            &self.writer,
            &mut self.encoder,
            &self.telemetry,
            &mut self.diag,
            &mut self.accounting,
            batch,
        );
        let result = match prepared {
            Ok(Some((request, carried))) => {
                settle(&self.telemetry, carried, crate::http::exchange(request).await)
            }
            Ok(None) => Ok(()),
            Err(err) => Err(err),
        };
        self.accounting.finish(result)
    }

    fn window(&self) -> usize {
        self.slots.window()
    }

    fn send_duration_per_batch(&self) -> bool {
        true
    }

    fn answered_send_duration(&self) -> Option<Duration> {
        self.slots.answered()
    }

    /// Encodes `batch` into its slot and starts its request on a task of its own.
    async fn submit(
        &mut self,
        batch: &EventBatch,
        _ctx: BatchContext,
        seq: SeqId,
    ) -> anyhow::Result<()> {
        let (writer, encoder, telemetry, diag) =
            (&self.writer, &mut self.encoder, &self.telemetry, &mut self.diag);
        self.slots.submit(seq, |accounting| {
            let prepared = prepare(writer, encoder, telemetry, diag, accounting, batch)?;
            Ok(prepared.map(|(request, carried)| async move {
                (carried, crate::http::exchange(request).await)
            }))
        })
    }

    /// The oldest batch's answer, counted and classified as `send` does.
    async fn await_ack(&mut self) -> anyhow::Result<()> {
        let telemetry = &self.telemetry;
        self.slots.ack(|(carried, exchange)| settle(telemetry, carried, exchange)).await
    }

    /// Aborts every request still in flight: the write loop has stopped.
    async fn flush(&mut self) -> anyhow::Result<()> {
        self.slots.cancel();
        Ok(())
    }
}

/// Emits one batch's [`EncodeStats`] as `logit.output.*` counters, each only when non-zero so an
/// ordinary batch doesn't upsert a permanent zero series. Public so a caller that runs
/// [`InfluxLineEncoder`] without this sink counts what the sink would.
pub fn emit_encode_stats(telemetry: &Telemetry, stats: &EncodeStats) {
    const TAGS_NORMALIZED: &str = "logit.output.tags.normalized";
    const TAGS_OMITTED: &str = "logit.output.tags.omitted";
    const NORMALIZED: &str = "logit.output.metrics.normalized";
    const DROPPED: &str = "logit.output.metrics.dropped";
    const SKIPPED: &str = "logit.output.metrics.skipped";
    const DEGRADED: &str = "logit.output.metrics.degraded";
    let rows: [(usize, &str, (&str, &str)); 26] = [
        (stats.tags_normalized_multi_value, TAGS_NORMALIZED, ("reason", "multi_value")),
        (stats.tags_omitted_unrepresentable, TAGS_OMITTED, ("reason", "unrepresentable")),
        (stats.tags_omitted_empty, TAGS_OMITTED, ("reason", "empty")),
        (stats.tags_omitted_unencodable, TAGS_OMITTED, ("reason", "unencodable")),
        (stats.tags_omitted_reserved, TAGS_OMITTED, ("reason", "reserved")),
        (stats.metrics_normalized_carrier_mismatch, NORMALIZED, ("reason", "carrier_mismatch")),
        (stats.dropped_unencodable_value, DROPPED, ("reason", "unencodable_value")),
        (stats.dropped_unencodable_key, DROPPED, ("reason", "unencodable_key")),
        (stats.dropped_reserved_key, DROPPED, ("reason", "reserved_key")),
        (stats.dropped_oversize_value, DROPPED, ("reason", "oversize_value")),
        (stats.degraded_non_finite_component, DEGRADED, ("reason", "non_finite_component")),
        (stats.skipped_no_recorded_value, SKIPPED, ("reason", "no_recorded_value")),
        (stats.skipped_set_members, SKIPPED, ("metric_kind", "set_members")),
        (stats.skipped_exponential_histogram, SKIPPED, ("metric_kind", "exponential_histogram")),
        (stats.dropped_unresolved_gauge_delta, DROPPED, ("reason", "unresolved_gauge_delta")),
        (stats.dropped_comment_measurement, DROPPED, ("reason", "comment_measurement")),
        (stats.dropped_timestamp_overflow, DROPPED, ("reason", "timestamp_overflow")),
        (stats.degraded_distribution, DEGRADED, ("metric_kind", "distribution")),
        (stats.degraded_samples, DEGRADED, ("metric_kind", "samples")),
        (stats.degraded_histogram, DEGRADED, ("metric_kind", "histogram")),
        (stats.degraded_summary, DEGRADED, ("metric_kind", "summary")),
        (stats.degraded_set, DEGRADED, ("metric_kind", "set")),
        (stats.degraded_monotonic_delta_sum, DEGRADED, ("metric_kind", "monotonic_delta_sum")),
        (
            stats.degraded_non_monotonic_delta_sum,
            DEGRADED,
            ("metric_kind", "non_monotonic_delta_sum"),
        ),
        (stats.degraded_cumulative_sum, DEGRADED, ("metric_kind", "cumulative_sum")),
        (stats.skipped_no_metrics, "logit.output.events.skipped", ("reason", "no_metrics")),
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
        let (addr, count, _targets) = recording_server(responses).await;
        (addr, count)
    }

    /// [`canned_server`], also recording each request's target (`/api/v2/write?...`).
    async fn recording_server(
        responses: Vec<&'static str>,
    ) -> (
        std::net::SocketAddr,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
        std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    ) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let count = Arc::new(AtomicUsize::new(0));
        let targets = Arc::new(Mutex::new(Vec::new()));
        let (count_task, targets_task) = (count.clone(), targets.clone());
        tokio::spawn(async move {
            loop {
                let Ok((mut stream, _)) = listener.accept().await else { return };
                let i = count_task.fetch_add(1, Ordering::SeqCst);
                let response = responses.get(i).or(responses.last()).copied().unwrap_or("");
                // Read up to the end of the request line, so the client's write can't block and
                // the target is whole; the timeout keeps a silent client from wedging this task.
                let mut head = Vec::new();
                let mut buf = [0u8; 8192];
                while !head.windows(2).any(|w| w == b"\r\n") {
                    match tokio::time::timeout(Duration::from_millis(500), stream.read(&mut buf))
                        .await
                    {
                        Ok(Ok(n)) if n > 0 => head.extend_from_slice(&buf[..n]),
                        _ => break,
                    }
                }
                let line = String::from_utf8_lossy(&head);
                if let Some(target) = line.lines().next().and_then(|l| l.split(' ').nth(1)) {
                    targets_task.lock().unwrap().push(target.to_string());
                }
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.shutdown().await;
            }
        });
        (addr, count, targets)
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
        InfluxDbOutput::new(
            format!("http://{addr}"),
            Organization::Fixed("org".into()),
            Destination::Fixed("bucket".into()),
            "token".into(),
        )
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
        // The shape `influxd` 2.9.1 answers (ADR `influxdb-line-protocol-relay`'s "Verified
        // `influxd` behavior", item 11): no `line` field.
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

    /// With `bucket:` and `org:` configured nothing is carried, so a `404` whose body names
    /// neither value holds the queue, as the status-only table reads it.
    #[tokio::test]
    async fn a_404_with_an_unrecognized_body_and_nothing_carried_is_refused() {
        let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        assert_eq!(class_of(response).await.0, Fault::Refused, "empty body");
        let body = r#"{"code":"not found","message":"resource not found"}"#;
        let fault = class_of(influx_error("404 Not Found", body)).await.0;
        assert_eq!(fault, Fault::Refused, "a body that names neither value");
    }

    /// A `408` reads as `classify_status` does, `Ambiguous`: a proxy gave up on the request's
    /// arrival, and a resend can land.
    #[tokio::test]
    async fn a_408_request_timeout_is_ambiguous() {
        let request_timeout = "HTTP/1.1 408 Request Timeout\r\nContent-Length: 0\r\n\
                               Connection: close\r\n\r\n";
        assert_eq!(class_of(request_timeout).await.0, Fault::Ambiguous, "408");
    }

    /// A redirect is `Rejected` because the client follows none.
    #[tokio::test]
    async fn a_redirect_is_rejected() {
        let redirect = "HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/api/v2/write\r\n\
                        Content-Length: 0\r\nConnection: close\r\n\r\n";
        assert_eq!(class_of(redirect).await.0, Fault::Rejected, "302");
    }

    /// A name that fails to resolve never reaches a server, so it is `Clean`. The `.invalid` TLD
    /// (RFC 6761) never resolves; the timeout bounds a resolver that is slow to say so.
    #[tokio::test]
    async fn a_dns_failure_is_classified_as_a_clean_fault() {
        let mut output = InfluxDbOutput::new(
            "http://logit-test.invalid:8086".to_string(),
            Organization::Fixed("org".into()),
            Destination::Fixed("bucket".into()),
            "token".into(),
        )
        .with_timeout(Duration::from_secs(5));
        let err = output
            .send(&one_metric_batch())
            .await
            .expect_err("an unresolvable host should fail send");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{err:#}");
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
        let (addr, _held) = logit_pipeline::test_util::refused_addr();

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
            Organization::Fixed("org".into()),
            Destination::Fixed("bucket".into()),
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
            Organization::Fixed("org".into()),
            Destination::Fixed("bucket".into()),
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
            ("logit.output.metrics.dropped", ("reason", "unencodable_value")),
            ("logit.output.metrics.dropped", ("reason", "unencodable_key")),
            ("logit.output.metrics.normalized", ("reason", "carrier_mismatch")),
            ("logit.output.tags.omitted", ("reason", "unrepresentable")),
            ("logit.output.tags.omitted", ("reason", "unencodable")),
        ] {
            assert_eq!(probe.sum(name, &[label]), 1.0, "{name} {label:?}");
        }
        assert_eq!(probe.sum("logit.output.metrics.skipped", &[]), 0.0, "a lost metric is dropped");
        assert_eq!(probe.sum("logit.output.tags.dropped", &[]), 0.0, "a tag left off is omitted");
    }

    /// A record written with a non-finite component left out counts as degraded, not dropped.
    #[test]
    fn a_non_finite_component_is_emitted_as_degraded() {
        use logit_pipeline::test_util::TelemetryProbe;

        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "influxdb_out", "sink");
        let stats = EncodeStats { degraded_non_finite_component: 1, ..EncodeStats::default() };
        emit_encode_stats(&telemetry, &stats);
        let label = ("reason", "non_finite_component");
        assert_eq!(probe.sum("logit.output.metrics.degraded", &[label]), 1.0);
        assert_eq!(probe.sum("logit.output.metrics.dropped", &[]), 0.0);
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

        // A collapsed multi-value tag, a gauge delta dropped with its diagnostic, and an event
        // with nothing to write.
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
                Event::empty(0, AttrMap::new()),
            ])
        };
        const ENCODE_SIDE: [(&str, &[(&str, &str)]); 5] = [
            ("logit.output.tags.normalized", &[("reason", "multi_value")]),
            ("logit.component.diagnostics", &[("key", "gauge_delta_unresolved")]),
            ("logit.output.metrics.dropped", &[("reason", "unresolved_gauge_delta")]),
            ("logit.output.events.skipped", &[("reason", "no_metrics")]),
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

    /// Each event with nothing to write counts once, a log-only one and an empty one alike, and
    /// a counter beside them doesn't.
    #[test]
    fn a_metricless_event_is_counted() {
        use logit_pipeline::test_util::TelemetryProbe;

        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "influxdb_out", "sink");
        let mut encoder = InfluxLineEncoder::new();
        let mut log_only = Event::empty(0, AttrMap::new());
        log_only.log = Some(logit_core::LogRecord {
            message: Value::str("x"),
            severity: None,
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        });
        encoder
            .encode(&batch_with(vec![
                metric_event("hits", MetricKind::counter(1.0), &[]),
                log_only,
                Event::empty(0, AttrMap::new()),
            ]))
            .unwrap();
        emit_encode_stats(&telemetry, encoder.stats());
        assert_eq!(probe.sum("logit.output.events.skipped", &[("reason", "no_metrics")]), 2.0);
    }

    /// A record written as fields of another shape counts once under its kind, a sum under its
    /// shape, and one dropped when its line is written (a second point of a series at
    /// `i64::MAX - 1`, with no free timestamp left) counts its drop alone.
    #[test]
    fn a_record_written_as_another_shape_is_counted_by_kind() {
        use logit_core::{DdSketch, Histogram, HyperLogLog, Samples, Summary, Temporality};
        use logit_pipeline::test_util::TelemetryProbe;

        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "influxdb_out", "sink");
        let mut encoder = InfluxLineEncoder::new();
        let histogram = || {
            MetricKind::Histogram(Histogram {
                buckets: vec![(1.0, 1), (f64::INFINITY, 2)],
                temporality: Temporality::Delta,
                sum: Some(3.0),
                min: None,
                max: None,
            })
        };
        let summary = MetricKind::Summary(Summary {
            quantiles: vec![(0.5, 0.2)],
            count: 1,
            sum: 0.2,
            temporality: Temporality::Cumulative,
        });
        let sum = |temporality, monotonic| {
            MetricKind::Sum(logit_core::Sum {
                value: logit_core::MetricValue::F64(1.0),
                temporality,
                monotonic,
            })
        };
        let late = |name: &str, kind| {
            Event::metric(
                i64::MAX - 1,
                AttrMap::new(),
                MetricRecord::new(logit_core::interner::intern(name), kind),
            )
        };
        let mut sketch = DdSketch::new();
        sketch.add(1.0);
        encoder
            .encode(&batch_with(vec![
                metric_event("lat", MetricKind::Distribution(sketch), &[]),
                metric_event("timer", MetricKind::Samples(Samples::new([1.0])), &[]),
                metric_event("buckets", histogram(), &[]),
                metric_event("rpc", summary, &[]),
                metric_event("users", MetricKind::Set(HyperLogLog::default()), &[]),
                metric_event("hits", sum(Temporality::Delta, true), &[]),
                metric_event("level", sum(Temporality::Delta, false), &[]),
                metric_event("total", sum(Temporality::Cumulative, true), &[]),
                late("buckets", histogram()),
                late("buckets", histogram()),
                late("total", sum(Temporality::Cumulative, true)),
                late("total", sum(Temporality::Cumulative, true)),
            ]))
            .unwrap();
        emit_encode_stats(&telemetry, encoder.stats());
        let kinds = [
            ("distribution", 1.0),
            ("samples", 1.0),
            ("histogram", 2.0),
            ("summary", 1.0),
            ("set", 1.0),
            ("monotonic_delta_sum", 1.0),
            ("non_monotonic_delta_sum", 1.0),
            ("cumulative_sum", 2.0),
        ];
        for (kind, n) in kinds {
            let counted = probe.sum("logit.output.metrics.degraded", &[("metric_kind", kind)]);
            assert_eq!(counted, n, "{kind}");
        }
        let dropped = [("reason", "timestamp_overflow")];
        assert_eq!(probe.sum("logit.output.metrics.dropped", &dropped), 2.0);
    }

    /// A `Sum` written as a field of its measurement carrier's line counts its shape, as one on a
    /// line of its own does.
    #[test]
    fn a_sum_on_a_carrier_line_is_counted_by_shape() {
        use logit_pipeline::test_util::TelemetryProbe;

        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "influxdb_out", "sink");
        let mut encoder = InfluxLineEncoder::new();
        let mut event = metric_event(
            "cpu.total",
            MetricKind::Sum(logit_core::Sum {
                value: logit_core::MetricValue::F64(4.0),
                temporality: logit_core::Temporality::Cumulative,
                monotonic: true,
            }),
            &[],
        );
        event.attributes.insert(logit_proto::influxdb::ATTR_MEASUREMENT, Value::str("cpu"));
        let body = encoder.encode(&batch_with(vec![event])).unwrap();
        assert!(std::str::from_utf8(&body).unwrap().starts_with("cpu total=4"), "{body:?}");
        emit_encode_stats(&telemetry, encoder.stats());
        let kind = [("metric_kind", "cumulative_sum")];
        assert_eq!(probe.sum("logit.output.metrics.degraded", &kind), 1.0);
        assert_eq!(encoder.stats().metrics_normalized_carrier_mismatch, 0);
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
                ("logit.output.metrics.dropped", &[("reason", "unresolved_gauge_delta")]),
                ("logit.output.batch.bytes", &[]),
            ],
        )
        .await;
    }

    // -- Destination resolution (the module doc's section of that name) ----------------------

    /// A metric batch whose resource carries `carriers`.
    fn carried_batch(carriers: &[(&str, Value)]) -> EventBatch {
        let mut resource = Resource::default();
        for (key, value) in carriers {
            resource.attributes.insert(key, value.clone());
        }
        let mut batch = one_metric_batch();
        batch.resource = Arc::new(resource);
        batch
    }

    /// The request target `send` writes `batch` to, against a server answering `204`.
    async fn target_of(org: Organization, destination: Destination, batch: EventBatch) -> String {
        let (addr, count, targets) = recording_server(vec![RESP_204]).await;
        let mut output =
            InfluxDbOutput::new(format!("http://{addr}"), org, destination, "token".into());
        output.send(&batch).await.expect("a 204 should succeed");
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "one request");
        let targets = targets.lock().unwrap();
        targets.first().cloned().expect("the request target was recorded")
    }

    /// No `org:` and no `default_org:`: the batch's carried org, else none.
    const NO_ORG: Organization = Organization::Carried { default_org: None };

    fn fixed_org(org: &str) -> Organization {
        Organization::Fixed(org.into())
    }

    fn default_org(org: &str) -> Organization {
        Organization::Carried { default_org: Some(org.into()) }
    }

    const ALL_CARRIERS: [(&str, &str); 3] = [
        (RESOURCE_ATTR_ORG, "acme"),
        (RESOURCE_ATTR_ORG_ID, "0123456789abcdef"),
        (RESOURCE_ATTR_BUCKET, "metrics"),
    ];

    fn all_carriers() -> EventBatch {
        carried_batch(&ALL_CARRIERS.map(|(k, v)| (k, Value::str(v))))
    }

    #[tokio::test]
    async fn carried_mode_forwards_the_carried_bucket_org_and_org_id() {
        let target =
            target_of(NO_ORG, Destination::Carried { default_bucket: None }, all_carriers()).await;
        assert_eq!(
            target,
            "/api/v2/write?org=acme&orgID=0123456789abcdef&bucket=metrics&precision=ns"
        );
    }

    /// InfluxDB 2.7 reads `orgID` over `org`, so a carried ID sent beside the configured name
    /// would defeat it.
    #[tokio::test]
    async fn a_configured_org_replaces_a_carried_org_and_org_id() {
        let target = target_of(
            fixed_org("ops"),
            Destination::Carried { default_bucket: None },
            all_carriers(),
        )
        .await;
        assert_eq!(target, "/api/v2/write?org=ops&bucket=metrics&precision=ns");
    }

    #[tokio::test]
    async fn a_fixed_bucket_ignores_a_carried_bucket() {
        let batch = carried_batch(&[
            (RESOURCE_ATTR_ORG, Value::str("acme")),
            (RESOURCE_ATTR_BUCKET, Value::str("metrics")),
        ]);
        let target = target_of(NO_ORG, Destination::Fixed("fixed".into()), batch).await;
        assert_eq!(target, "/api/v2/write?org=acme&bucket=fixed&precision=ns");
    }

    #[tokio::test]
    async fn a_batch_with_no_carrier_goes_to_the_default_bucket_with_no_org() {
        let destination = Destination::Carried { default_bucket: Some("fallback".into()) };
        let target = target_of(NO_ORG, destination, one_metric_batch()).await;
        assert_eq!(target, "/api/v2/write?bucket=fallback&precision=ns");
    }

    #[tokio::test]
    async fn a_batch_with_no_carrier_goes_to_the_default_org() {
        let destination = Destination::Carried { default_bucket: Some("fallback".into()) };
        let target = target_of(default_org("ops"), destination, one_metric_batch()).await;
        assert_eq!(target, "/api/v2/write?org=ops&bucket=fallback&precision=ns");
    }

    #[tokio::test]
    async fn a_carried_org_wins_over_the_default_org() {
        let batch = carried_batch(&[(RESOURCE_ATTR_ORG, Value::str("acme"))]);
        let target = target_of(default_org("ops"), Destination::Fixed("b".into()), batch).await;
        assert_eq!(target, "/api/v2/write?org=acme&bucket=b&precision=ns");
    }

    /// A carried `orgID` names the org on its own, so `default_org` isn't sent beside it.
    #[tokio::test]
    async fn a_carried_org_id_alone_wins_over_the_default_org() {
        let batch = carried_batch(&[(RESOURCE_ATTR_ORG_ID, Value::str("0123456789abcdef"))]);
        let target = target_of(default_org("ops"), Destination::Fixed("b".into()), batch).await;
        assert_eq!(target, "/api/v2/write?orgID=0123456789abcdef&bucket=b&precision=ns");
    }

    #[tokio::test]
    async fn a_configured_org_wins_over_everything_carried() {
        let destination = Destination::Carried { default_bucket: None };
        let target = target_of(fixed_org("ops"), destination, all_carriers()).await;
        assert_eq!(target, "/api/v2/write?org=ops&bucket=metrics&precision=ns");
    }

    #[tokio::test]
    async fn no_org_configured_or_carried_sends_no_org_parameter() {
        let target = target_of(NO_ORG, Destination::Fixed("b".into()), one_metric_batch()).await;
        assert_eq!(target, "/api/v2/write?bucket=b&precision=ns");
    }

    /// A carrier that isn't a string, or is empty, reads as absent.
    #[tokio::test]
    async fn a_non_string_carrier_reads_as_absent() {
        let batch = carried_batch(&[
            (RESOURCE_ATTR_BUCKET, Value::I64(7)),
            (RESOURCE_ATTR_ORG_ID, Value::Bool(true)),
        ]);
        let destination = Destination::Carried { default_bucket: Some("fallback".into()) };
        let target = target_of(fixed_org("ops"), destination, batch).await;
        assert_eq!(target, "/api/v2/write?org=ops&bucket=fallback&precision=ns");
    }

    #[tokio::test]
    async fn an_empty_carrier_reads_as_absent() {
        let batch = carried_batch(&[
            (RESOURCE_ATTR_BUCKET, Value::str("")),
            (RESOURCE_ATTR_ORG, Value::str("")),
            (RESOURCE_ATTR_ORG_ID, Value::str("0123456789abcdef")),
        ]);
        let destination = Destination::Carried { default_bucket: Some("fallback".into()) };
        let target = target_of(NO_ORG, destination, batch).await;
        assert_eq!(target, "/api/v2/write?orgID=0123456789abcdef&bucket=fallback&precision=ns");
    }

    /// With no bucket carried and no `default_bucket`, nothing is sent and every metric record
    /// counts as dropped, with its diagnostic.
    #[tokio::test]
    async fn a_batch_with_no_bucket_is_dropped_counted_and_not_sent() {
        use logit_pipeline::test_util::TelemetryProbe;

        let (addr, count) = canned_server(vec![RESP_204]).await;
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "influxdb_out", "sink");
        let mut output = InfluxDbOutput::new(
            format!("http://{addr}"),
            fixed_org("ops"),
            Destination::Carried { default_bucket: None },
            "token".into(),
        )
        .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
        .with_telemetry(telemetry);
        let batch = carried_batch(&[(RESOURCE_ATTR_ORG, Value::str("acme"))]);
        let mut batch = batch;
        batch.events.push(metric_event("y", MetricKind::gauge(2.0), &[]));

        output.send(&batch).await.expect("a batch with no bucket is not a send error");

        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 0, "no request is made");
        let dropped = [("reason", "no_bucket")];
        assert_eq!(probe.sum("logit.output.metrics.dropped", &dropped), 2.0);
        assert_eq!(probe.sum("logit.component.diagnostics", &[("key", "no_bucket")]), 1.0);
        assert_eq!(probe.sum("logit.output.requests", &[]), 0.0);
    }

    /// The class `send` reads from one canned answer to `batch` under `org` and `destination`.
    async fn class_under(
        org: Organization,
        destination: Destination,
        batch: EventBatch,
        response: &'static str,
    ) -> Fault {
        let (addr, count) = canned_server(vec![response]).await;
        let mut output =
            InfluxDbOutput::new(format!("http://{addr}"), org, destination, "token".into());
        let err = output.send(&batch).await.expect_err("a non-2xx fails send");
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), 1, "one attempt");
        logit_pipeline::classify(&err)
    }

    /// The class of one canned answer to a batch whose bucket, org, and orgID are all carried.
    async fn carried_class_of(response: &'static str) -> Fault {
        class_under(NO_ORG, Destination::Carried { default_bucket: None }, all_carriers(), response)
            .await
    }

    const BUCKET_NOT_FOUND: &str =
        r#"{"code":"not found","message":"bucket \"metrics\" not found"}"#;
    const ORG_NOT_FOUND: &str =
        r#"{"code":"not found","message":"organization name \"ops\" not found"}"#;
    const FORBIDDEN: &str =
        r#"{"code":"forbidden","message":"insufficient permissions for write"}"#;

    #[tokio::test]
    async fn a_404_for_a_carried_bucket_is_rejected() {
        let response = influx_error("404 Not Found", BUCKET_NOT_FOUND);
        assert_eq!(carried_class_of(response).await, Fault::Rejected);
    }

    #[tokio::test]
    async fn a_404_for_a_carried_org_id_is_rejected() {
        let body = r#"{"code":"not found","message":"organization not found"}"#;
        assert_eq!(carried_class_of(influx_error("404 Not Found", body)).await, Fault::Rejected);
    }

    #[tokio::test]
    async fn a_403_for_a_carried_bucket_is_rejected() {
        let destination = Destination::Carried { default_bucket: None };
        let response = influx_error("403 Forbidden", FORBIDDEN);
        let fault = class_under(fixed_org("ops"), destination, all_carriers(), response).await;
        assert_eq!(fault, Fault::Rejected);
    }

    /// A `404` whose body names neither, with a value carried, doesn't hold the queue.
    #[tokio::test]
    async fn a_404_with_an_unrecognized_body_and_a_carried_value_is_rejected() {
        let response = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        assert_eq!(carried_class_of(response).await, Fault::Rejected);
    }

    /// The bucket is carried, but the org that isn't found is the configured one.
    #[tokio::test]
    async fn a_404_for_a_configured_org_beside_a_carried_bucket_is_refused() {
        let destination = Destination::Carried { default_bucket: None };
        let response = influx_error("404 Not Found", ORG_NOT_FOUND);
        let fault = class_under(fixed_org("ops"), destination, all_carriers(), response).await;
        assert_eq!(fault, Fault::Refused);
    }

    /// `default_bucket` is the operator's bucket, so a fault naming it holds as a fixed one does.
    /// The org is configured, so nothing in the request was carried.
    #[tokio::test]
    async fn a_404_or_403_for_the_default_bucket_is_refused() {
        let batch = || carried_batch(&[(RESOURCE_ATTR_ORG, Value::str("acme"))]);
        for (status, body) in [("404 Not Found", BUCKET_NOT_FOUND), ("403 Forbidden", FORBIDDEN)] {
            let destination = Destination::Carried { default_bucket: Some("fallback".into()) };
            let response = influx_error(status, body);
            let fault = class_under(fixed_org("ops"), destination, batch(), response).await;
            assert_eq!(fault, Fault::Refused, "{status}");
        }
    }

    /// `default_org` is the operator's org, so a fault that could concern it holds as a
    /// configured one does. The bucket is configured, so nothing in the request was carried.
    #[tokio::test]
    async fn a_404_or_403_under_the_default_org_is_refused() {
        let answers = [
            ("404 Not Found", ORG_NOT_FOUND),
            ("404 Not Found", BUCKET_NOT_FOUND),
            ("403 Forbidden", FORBIDDEN),
        ];
        for (status, body) in answers {
            let destination = Destination::Fixed("fixed".into());
            let response = influx_error(status, body);
            let fault =
                class_under(default_org("ops"), destination, one_metric_batch(), response).await;
            assert_eq!(fault, Fault::Refused, "{status} {body}");
        }
    }

    /// A carried org wins over `default_org`, so a fault that could concern it is the batch's.
    #[tokio::test]
    async fn a_404_or_403_for_a_carried_org_beside_a_default_org_is_rejected() {
        let batch = || carried_batch(&[(RESOURCE_ATTR_ORG, Value::str("acme"))]);
        for (status, body) in [("404 Not Found", ORG_NOT_FOUND), ("403 Forbidden", FORBIDDEN)] {
            let destination = Destination::Fixed("fixed".into());
            let response = influx_error(status, body);
            let fault = class_under(default_org("ops"), destination, batch(), response).await;
            assert_eq!(fault, Fault::Rejected, "{status}");
        }
    }

    /// An all-access token for one org answers `403` for a bucket in another, so a carried org
    /// makes a `403` the batch's even with a configured bucket.
    #[tokio::test]
    async fn a_403_for_a_carried_org_beside_a_configured_bucket_is_rejected() {
        let batch = carried_batch(&[(RESOURCE_ATTR_ORG, Value::str("acme"))]);
        let response = influx_error("403 Forbidden", FORBIDDEN);
        let fault = class_under(NO_ORG, Destination::Fixed("fixed".into()), batch, response).await;
        assert_eq!(fault, Fault::Rejected);
    }

    /// InfluxDB 2.x looks the bucket up inside the request's org, so a configured bucket missing
    /// from a carried org is the batch's.
    #[tokio::test]
    async fn a_404_for_a_configured_bucket_in_a_carried_org_is_rejected() {
        let batch = carried_batch(&[(RESOURCE_ATTR_ORG, Value::str("acme"))]);
        let response = influx_error("404 Not Found", BUCKET_NOT_FOUND);
        let fault = class_under(NO_ORG, Destination::Fixed("fixed".into()), batch, response).await;
        assert_eq!(fault, Fault::Rejected);
    }

    /// A bad token is the sink's whatever was carried.
    #[tokio::test]
    async fn a_carried_mode_401_unauthorized_is_refused() {
        let body = r#"{"code":"unauthorized","message":"unauthorized access"}"#;
        let response = influx_error("401 Unauthorized", body);
        assert_eq!(carried_class_of(response).await, Fault::Refused);
    }

    /// InfluxDB 3's `401` body.
    #[tokio::test]
    async fn an_influxdb_3_401_is_refused() {
        let body = r#"{"error": "the request was not authenticated"}"#;
        assert_eq!(class_of(influx_error("401 Unauthorized", body)).await.0, Fault::Refused);
    }

    /// InfluxDB 2.x answers a request naming no organization `400`.
    #[tokio::test]
    async fn a_400_naming_no_organization_is_rejected() {
        let body = r#"{"code":"invalid","message":"Please provide either orgID or org"}"#;
        assert_eq!(carried_class_of(influx_error("400 Bad Request", body)).await, Fault::Rejected);
    }

    // -- tls: ----------------------------------------------------------------------------------

    /// A receiver over TLS presenting `testdata/tls/server.pem`, answering every write `204`.
    /// Returns the receiver and a sink dialing it, tuned by `settings`.
    async fn tls_sink(
        client_ca: Option<&str>,
        settings: crate::tls::TlsClientSettings,
    ) -> (crate::http::held::Mux, InfluxDbOutput) {
        let server =
            crate::test_support::fixture_server_config("server", client_ca, &[b"http/1.1"]);
        let mux = crate::http::held::Mux::start(Some(server)).await;
        mux.release();
        let output = InfluxDbOutput::new(
            mux.url().to_string(),
            Organization::Fixed("org".into()),
            Destination::Fixed("bucket".into()),
            "token".into(),
        )
        .with_tls(
            &settings,
            &crate::test_support::testdata_dir(),
            &logit_pipeline::tls::TlsReloader::new(),
        )
        .expect("the fixture files load");
        (mux, output)
    }

    /// With the private CA in `ca_file`, the write completes over TLS.
    #[tokio::test]
    async fn a_ca_file_trusts_a_private_ca_receiver() {
        let settings = crate::test_support::tls_settings(|s| s.ca_file = Some("ca.pem".into()));
        let (mux, mut output) = tls_sink(None, settings).await;
        output.send(&one_metric_batch()).await.expect("the write should verify and land");
        assert_eq!(mux.carriers().len(), 1, "one request reached the receiver");
    }

    /// Without `ca_file`, `reqwest`'s default trust refuses the private CA's certificate.
    #[tokio::test]
    async fn no_tls_block_refuses_a_private_ca_receiver() {
        let (mux, mut output) = tls_sink(None, Default::default()).await;
        output.send(&one_metric_batch()).await.expect_err("the private CA isn't trusted");
        assert!(mux.carriers().is_empty(), "no request got through the handshake");
    }

    /// `insecure_skip_verify` accepts the same certificate without a CA.
    #[tokio::test]
    async fn insecure_skip_verify_accepts_a_private_ca_receiver() {
        let settings = crate::test_support::tls_settings(|s| s.insecure_skip_verify = true);
        let (mux, mut output) = tls_sink(None, settings).await;
        output.send(&one_metric_batch()).await.expect("verification is off");
        assert_eq!(mux.carriers().len(), 1);
    }

    /// A receiver requiring a client certificate admits the sink only with `cert_file` and
    /// `key_file` set.
    #[tokio::test]
    async fn a_client_certificate_satisfies_mutual_tls() {
        let with_cert = crate::test_support::tls_settings(|s| {
            s.ca_file = Some("ca.pem".into());
            s.cert_file = Some("client.pem".into());
            s.key_file = Some("client.key".into());
        });
        let (mux, mut output) = tls_sink(Some("ca"), with_cert).await;
        output.send(&one_metric_batch()).await.expect("the client certificate is admitted");
        assert_eq!(mux.carriers().len(), 1);

        let without_cert = crate::test_support::tls_settings(|s| s.ca_file = Some("ca.pem".into()));
        let (mux, mut output) = tls_sink(Some("ca"), without_cert).await;
        output.send(&one_metric_batch()).await.expect_err("no client certificate is refused");
        assert!(mux.carriers().is_empty());
    }

    /// The certificate in `server-named.pem` names only `logit.test`, so a client dialing
    /// `127.0.0.1` verifies it only against a configured `server_name`.
    #[tokio::test]
    async fn server_name_verifies_the_certificate_against_that_name() {
        let named = |server_name: Option<&str>| {
            crate::test_support::tls_settings(|s| {
                s.ca_file = Some("ca.pem".into());
                s.server_name = server_name.map(String::from);
            })
        };
        let server =
            || crate::test_support::fixture_server_config("server-named", None, &[b"http/1.1"]);

        let mux = crate::http::held::Mux::start(Some(server())).await;
        mux.release();
        let mut output = InfluxDbOutput::new(
            mux.url().to_string(),
            Organization::Fixed("org".into()),
            Destination::Fixed("bucket".into()),
            "token".into(),
        )
        .with_tls(
            &named(None),
            &crate::test_support::testdata_dir(),
            &logit_pipeline::tls::TlsReloader::new(),
        )
        .unwrap();
        output.send(&one_metric_batch()).await.expect_err("127.0.0.1 isn't in the certificate");
        assert!(mux.carriers().is_empty());

        let mux = crate::http::held::Mux::start(Some(server())).await;
        mux.release();
        let mut output = InfluxDbOutput::new(
            mux.url().to_string(),
            Organization::Fixed("org".into()),
            Destination::Fixed("bucket".into()),
            "token".into(),
        )
        .with_tls(
            &named(Some("logit.test")),
            &crate::test_support::testdata_dir(),
            &logit_pipeline::tls::TlsReloader::new(),
        )
        .unwrap();
        output.send(&one_metric_batch()).await.expect("logit.test is in the certificate");
        assert_eq!(mux.carriers().len(), 1);
    }

    /// `with_tls` survives the later timeout and idle-timeout setters, which rebuild the client.
    #[tokio::test]
    async fn the_tls_config_survives_a_client_rebuild() {
        let settings = crate::test_support::tls_settings(|s| s.ca_file = Some("ca.pem".into()));
        let (mux, output) = tls_sink(None, settings).await;
        let mut output =
            output.with_timeout(Duration::from_secs(5)).with_idle_timeout(Duration::from_secs(1));
        output.send(&one_metric_batch()).await.expect("the rebuilt client still trusts ca.pem");
        assert_eq!(mux.carriers().len(), 1);
    }

    /// A reloaded `ca_file` reaches the next request: building the sink registers its files with
    /// the reloader.
    #[tokio::test]
    async fn a_reloaded_ca_file_reaches_the_next_request() {
        use crate::test_support::{fixture_server_config, rewrite_tls_file, scratch_tls_files};
        let server = fixture_server_config("server-other", None, &[b"http/1.1"]);
        let mux = crate::http::held::Mux::start(Some(server)).await;
        mux.release();
        let dir = scratch_tls_files("influxdb-out-ca-reload", &[("ca.pem", "ca.pem")]);
        let reloader = logit_pipeline::tls::TlsReloader::new();
        let mut output = InfluxDbOutput::new(
            mux.url().to_string(),
            Organization::Fixed("org".into()),
            Destination::Fixed("bucket".into()),
            "token".into(),
        )
        .with_tls(
            &crate::test_support::tls_settings(|s| s.ca_file = Some("ca.pem".into())),
            &dir,
            &reloader,
        )
        .unwrap();
        output.send(&one_metric_batch()).await.expect_err("ca.pem didn't sign server-other");

        rewrite_tls_file(&dir, "ca.pem", "other-ca.pem");
        reloader.check_now();

        output.send(&one_metric_batch()).await.expect("the reloaded CA trusts the receiver");
    }

    // -- concurrency above 1 (`docs/adr/sink-request-concurrency.md`, decision 3) -----------

    mod concurrency {
        use super::*;
        use crate::http::held::{self, Subject};

        fn make(url: &str, concurrency: u32, telemetry: Telemetry) -> InfluxDbOutput {
            InfluxDbOutput::new(
                url.into(),
                Organization::Fixed("org".into()),
                Destination::Fixed("bucket".into()),
                "token".into(),
            )
            .with_timeout(Duration::from_secs(60))
            .with_concurrency(concurrency)
            .with_telemetry(telemetry)
        }

        /// Batch `n` writes measurement `m<n>` and carries an event with no metrics, which the
        /// encoder counts skipped.
        fn batch(n: u64) -> EventBatch {
            batch_with(vec![
                metric_event(&format!("m{n}"), MetricKind::counter(1.0), &[]),
                Event::empty(1_700_000_000_000_000_000, AttrMap::new()),
            ])
        }

        fn id_of(body: &[u8]) -> u64 {
            let text = String::from_utf8_lossy(body);
            let measurement = text.split([' ', ',']).next().unwrap_or_default();
            measurement.trim_start_matches('m').parse().expect("a body written by `batch`")
        }

        fn subject() -> Subject<InfluxDbOutput> {
            Subject {
                kind: "influxdb_out",
                make,
                batch,
                id_of,
                encoded: ("logit.output.events.skipped", &[("reason", "no_metrics")]),
                refuse: held::RESP_401,
            }
        }

        #[tokio::test]
        async fn n_requests_in_flight_answered_out_of_order() {
            held::n_requests_in_flight_answered_out_of_order(subject()).await;
        }

        #[tokio::test]
        async fn a_500_on_one_slot_is_resent_alone() {
            held::a_500_on_one_slot_is_resent_alone(subject()).await;
        }

        #[tokio::test]
        async fn a_400_on_one_slot_is_dropped_alone() {
            held::a_400_on_one_slot_is_dropped_alone(subject()).await;
        }

        #[tokio::test]
        async fn a_401_head_holds_the_fill_at_one_window() {
            held::a_401_head_holds_the_fill_at_one_window(subject()).await;
        }

        #[tokio::test]
        async fn at_most_once_drops_one_ambiguous_slot_alone() {
            held::at_most_once_drops_one_ambiguous_slot_alone(subject()).await;
        }

        #[tokio::test]
        async fn a_grace_cut_aborts_the_requests_in_flight() {
            held::a_grace_cut_aborts_the_requests_in_flight(subject()).await;
        }

        #[tokio::test]
        async fn a_batch_behind_a_retried_head_times_its_own_request() {
            held::a_batch_behind_a_retried_head_times_its_own_request(subject()).await;
        }
    }
}
