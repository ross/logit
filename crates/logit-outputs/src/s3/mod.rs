//! `s3_out`: archives batches to an S3 bucket, or an S3-compatible store, one object per time
//! and size window, each batch counted delivered only once its object is stored
//! (`docs/adr/s3-object-archive.md`).
//!
//! ## The window
//!
//! The sink collects through the window protocol (`docs/adr/collecting-sink-window.md`): it
//! reports a [`Output::window`] of `usize::MAX`, and [`Output::collecting`] returns the open
//! object's `max_age` deadline until the object is full. Every batch of an open or uploading
//! object stays reserved in the sink's store, uncommitted, until the object is stored.
//!
//! - **`submit` stages.** The first batch submitted into an empty object opens it and names its
//!   key; each batch is encoded, compressed, and hashed into the body as it arrives
//!   ([`StagedObject`]).
//! - **Full.** `collecting()` reads `None` once the staged batches' encoded bytes reach
//!   `max_bytes`, the compressed body reaches `part_size`, or the object holds the head's pinned
//!   batch count.
//! - **`await_ack` on the head completes the object**: it closes the body and sends one
//!   `PutObject`, and answers the head with the object's outcome. From then until each batch
//!   behind the head is answered, `window()` reads the answers owed, `collecting()` reads `None`,
//!   and each later `await_ack` answers `Ok` at once.
//! - **A fault covers the object.** A failed `PutObject`, or a credential lookup that failed
//!   before it, returns its fault without `HeadOnly`, so the loop resubmits from the head and
//!   the object is staged again under the same key.
//! - **An encode that fails** is the batch's own `Rejected` fault at the head. Past the head the
//!   failed batch was never staged, so the loop's unclassified stop completes the object without
//!   it, and the batch fails again at the head of the next object, where it's dropped alone.
//! - **The shutdown grace.** [`Output::reached_destination`] reads 0 until the `PutObject` is
//!   sent, so an at-most-once grace cut commits none of an object that never left the process.
//!
//! **W3a's limit: one `PutObject` per object.** An object closes once its compressed body reaches
//! `part_size`, where the multipart path will upload a part and keep the object open. Until that
//! path lands an object is at most about `part_size` plus one batch's compressed bytes, whatever
//! `max_bytes` says (`docs/known-gaps/sinks.md`).
//!
//! ## The key
//!
//! `{prefix}{partition}{object_time}-{store id}-{first seq}.{ext}` ([`key`]'s module doc). The
//! head pins `object_time` and, from the object's first close, its batch count, so every retry
//! of that head in this process writes the same key and holds no more batches than an attempt
//! that may still land; the count only falls (`docs/adr/s3-object-archive.md`, decision 3). A
//! key holds no configuration value but `prefix` and `partition`.
//!
//! ## Requests
//!
//! One `PutObject` per object, with a known `Content-Length`, the body's hex SHA-256 in
//! `x-amz-content-sha256`, signed with SigV4 header auth ([`sign`]'s module doc), and
//! `Content-Type` `application/gzip`, `application/zstd`, `application/x-ndjson`, or
//! `application/octet-stream`. No `Content-Encoding`: a client fetching the object would
//! decompress it in transit and hand on bytes whose name says they're compressed. `storage_class:`
//! sets `x-amz-storage-class`, and `sse:` sets `x-amz-server-side-encryption` with
//! `x-amz-server-side-encryption-aws-kms-key-id` and
//! `x-amz-server-side-encryption-bucket-key-enabled` under `aws_kms`. Redirects are off
//! ([`crate::http::build_client`]), and every request is bounded by `timeout`.
//!
//! Credentials come from `credentials:`, else `logit_aws::creds::Chain`. An error answer's body
//! is read bounded, to [`xml::ERROR_BODY_BYTES`], for its `<Code>` and `<Message>`, which the
//! error quotes with the bucket and the key, never a credential.
//!
//! **No encode-side counts.** The json and native encoders count nothing, so the sink holds no
//! `BatchAccounting` gate; every count below is per request or per object.
//!
//! ## Response classes
//!
//! S3 never interprets an object's content, so no answer is about one batch except a size, and
//! an object's size is set by the sink's configuration. Every answer the sink distinguishes is
//! listed, and an answer that reaches "any other `4xx`" costs one batch per object attempt: the
//! runtime drops the head alone, counted `batches.dropped{reason="rejected"}`, and the next
//! attempt uploads the rest again. This table is canonical; [ADR
//! `s3-object-archive`](../../../../docs/adr/s3-object-archive.md)'s decision 6 was its first copy.
//! A row the single-`PutObject` path can't receive is tested with the multipart path, W3b of
//! [the plan](../../../../docs/plans/s3-object-archive.md).
//!
//! | Answer | Class | Why | Evidence |
//! |---|---|---|---|
//! | `2xx` | delivered | the object is stored | `a_2xx_delivers_the_object` |
//! | `200` from `CompleteMultipartUpload` with an `<Error>` body | the class of its `<Code>` below | AWS documents the error-in-`200` case | W3b |
//! | `301` `PermanentRedirect`, `307` `TemporaryRedirect`, any other `3xx` | `Refused` | the wrong region or endpoint: the same answer for every object; the error carries `x-amz-bucket-region` | `a_3xx_is_refused_and_names_the_buckets_region` |
//! | `400` `RequestTimeout`, `IncompleteBody`, `BadDigest`, `XAmzContentSHA256Mismatch` | `Clean` | the body didn't arrive whole, so nothing was stored | `a_body_that_didnt_arrive_whole_is_clean` |
//! | `400` `ExpiredToken`, `TokenRefreshRequired` | `Clean`, after `Chain::forget` | the credentials expired; fresh ones will sign the retry | `an_expired_token_is_clean_and_forgets_the_chains_credentials` |
//! | `400` `EntityTooLarge`, `413` | `Refused` | a body is at most `part_size`, so the destination's limit is below it: configuration, not a batch | `a_body_too_large_is_refused` |
//! | `400` `InvalidArgument`, `InvalidToken`, `AuthorizationHeaderMalformed`, `InvalidBucketName`, `InvalidEncryptionAlgorithmError`, any `KMS.*`, `InvalidStorageClass`, `AccessControlListNotSupported`, `KeyTooLongError`, `InvalidRequest` | `Refused` | configuration: the same answer for every object | `a_configuration_code_is_refused` |
//! | `400` `InvalidPart`, `InvalidPartOrder`, `EntityTooSmall` | `Clean` | the sink built a bad upload; nothing is visible yet, so the object starts again | W3b |
//! | `403`, any code | `Refused` | credentials or policy | `a_403_is_refused` |
//! | `404` `NoSuchBucket`, and any other `404` | `Refused` | the bucket or endpoint is wrong | `a_404_is_refused` |
//! | `404` `NoSuchUpload` | `Clean` | the upload was aborted underneath the sink; the object starts again | W3b |
//! | `408` | `Ambiguous` | the shared default ([ADR `sink-fault-classes`](../../../../docs/adr/sink-fault-classes.md)'s "Amendment: 408 is `Ambiguous`") | `a_408_is_ambiguous` |
//! | `409` `OperationAborted` | `Clean` | a conflicting operation was in progress; nothing was stored | `a_409_operation_aborted_is_clean` |
//! | `411` `MissingContentLength` | `Refused` | a proxy or store that won't take the request's framing | `a_411_is_refused` |
//! | `429`; any `503` with an S3 XML `<Code>` (`SlowDown`, `ServiceUnavailable`, MinIO's `SlowDownWrite` and `SlowDownRead`) | `Clean` | the store refused the request before processing it; `PutObject` is atomic | `a_429_or_a_503_with_a_code_is_clean` |
//! | `503` with no S3 `<Code>` | `Ambiguous` | a load balancer's answer, which says nothing about the store: the shared default | `a_503_with_no_code_is_ambiguous` |
//! | `500` (`InternalError` or any other code), `502`, `504`, and any other `5xx` | `Ambiguous` | the request may have been applied; a retry rewrites the same key; the shared default | `a_5xx_is_ambiguous` |
//! | `501` `NotImplemented` | `Refused` | the store lacks a feature the sink uses | `a_501_is_refused` |
//! | any other `4xx` | `Rejected` | [ADR `sink-fault-classes`](../../../../docs/adr/sink-fault-classes.md)'s rule for an answer the sink can't attribute | `any_other_4xx_is_rejected` |
//! | connect failure; a dial `connect_timeout:` ended | `Clean` | nothing left the process: the shared HTTP driver's default ([`crate::http::classify_reqwest_error`]) | `a_refused_or_stalled_dial_is_clean` |
//! | request timeout, or another transport error after the connection opened | `Ambiguous` | the request may have been applied: the shared HTTP driver's default | `a_request_timeout_is_ambiguous` |
//!
//! A credential lookup that fails before any request is the chain's own fault: `Clean` when an
//! endpoint was unreachable or busy, `Refused` otherwise (`logit_aws::creds`' module doc).
//!
//! **Retries are the loop's.** Every retryable class restarts the object from its head through
//! the window protocol, holding no more than the head's pinned count; the sink retries no request
//! of its own. **Posture:** the default, `at_least_once`, because a retry rewrites the same key.
//! Under `at_most_once` an `Ambiguous` `PutObject` drops every batch of its object, which may be
//! stored.
//!
//! ## Telemetry
//!
//! `logit.output.requests{op, class}` per request, `op` `put_object` and `class` the HTTP sinks'
//! status bucket or `connect_error`/`network_error`; `logit.output.request.duration{op}`;
//! `logit.output.objects` per object stored; and `logit.output.bytes` per compressed byte sent,
//! resends included (`docs/design/internal-telemetry.md`). Loss is the runtime's to count.

mod key;
mod object;
mod sign;
#[cfg(test)]
mod tests;
mod xml;

pub use crate::tls::TlsClientSettings;
pub use object::{FinishedObject, StagedObject};

use crate::Output;
use anyhow::Context;
use key::Pin;
use logit_aws::creds::{Chain, Credentials};
use logit_aws::sigv4::AmzDate;
use logit_core::{Diagnostics, EventBatch, Telemetry};
use logit_pipeline::{BatchContext, Fault, SeqId};
use sign::Addressing;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

/// Mirrors `logit_config::default_s3_partition`.
pub const DEFAULT_PARTITION: &str = "dt=%Y-%m-%d/hour=%H/";
/// Mirrors `logit_config::default_s3_max_bytes`.
pub const DEFAULT_MAX_BYTES: u64 = 32 * 1024 * 1024;
/// Mirrors `logit_config::default_s3_max_age`.
pub const DEFAULT_MAX_AGE: Duration = Duration::from_secs(300);
/// Mirrors `logit_config::default_s3_part_size`.
pub const DEFAULT_PART_SIZE: u64 = 8 * 1024 * 1024;
/// Mirrors `logit_config::default_s3_timeout`.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// How an object is encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectFormat {
    Json,
    Native,
}

/// How an object is compressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectCompression {
    Gzip,
    Zstd,
    None,
}

/// Server-side encryption for each object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sse {
    Aes256,
    AwsKms { kms_key_id: Option<String>, bucket_key: Option<bool> },
}

/// Static credentials, from `credentials:`.
#[derive(Clone)]
pub struct StaticCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
}

/// Everything `s3_out` is configured with but TLS, which [`S3Output::with_tls`] adds.
pub struct S3Settings {
    pub bucket: String,
    pub region: String,
    pub endpoint: Option<String>,
    pub path_style: Option<bool>,
    pub prefix: String,
    pub partition: String,
    pub format: ObjectFormat,
    pub compression: ObjectCompression,
    pub max_bytes: u64,
    pub max_age: Duration,
    pub part_size: u64,
    pub storage_class: Option<String>,
    pub sse: Option<Sse>,
    /// `None` takes credentials from the AWS default chain.
    pub credentials: Option<StaticCredentials>,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
}

impl S3Settings {
    /// The defaults for `bucket` in `region`.
    pub fn new(bucket: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            path_style: None,
            prefix: String::new(),
            partition: DEFAULT_PARTITION.to_string(),
            format: ObjectFormat::Json,
            compression: ObjectCompression::Gzip,
            max_bytes: DEFAULT_MAX_BYTES,
            max_age: DEFAULT_MAX_AGE,
            part_size: DEFAULT_PART_SIZE,
            storage_class: None,
            sse: None,
            credentials: None,
            timeout: DEFAULT_TIMEOUT,
            connect_timeout: crate::http::DEFAULT_CONNECT_TIMEOUT,
            idle_timeout: crate::http::DEFAULT_IDLE_TIMEOUT,
        }
    }
}

/// Where the sink's credentials come from.
enum CredentialSource {
    Static(Arc<Credentials>),
    Chain(Chain),
}

/// The object being staged: its key and its `max_age` deadline.
struct OpenObject {
    key: String,
    deadline: tokio::time::Instant,
}

/// The `s3_out` sink.
pub struct S3Output {
    settings: S3Settings,
    addressing: Addressing,
    client: reqwest::Client,
    tls: Option<rustls::ClientConfig>,
    credentials: CredentialSource,
    object: StagedObject,
    open: Option<OpenObject>,
    pin: Option<Pin>,
    /// The head's `await_ack` closed the object and hasn't answered yet; set until it does, so a
    /// submit after a cancelled close opens a new object.
    closing: bool,
    /// The answers owed to the batches behind a stored object's head.
    owed: usize,
    /// The current object's `PutObject` was sent.
    visible: bool,
    /// The last head's time from its object's close to its answer.
    answered: Option<Duration>,
    /// The batch `observe_batch` last saw, for a direct `send`.
    observed: Option<SeqId>,
    /// Nanoseconds since the Unix epoch: the wall clock, or a test's.
    clock: fn() -> i64,
    telemetry: Telemetry,
    diag: Diagnostics,
}

fn wall_clock_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

impl S3Output {
    /// The sink `settings` describe. Fails on an `endpoint` that isn't a URL with a host, or a
    /// `partition` jiff can't format, which validation rejects first.
    pub fn new(settings: S3Settings) -> anyhow::Result<Self> {
        let addressing = Addressing::new(
            &settings.bucket,
            &settings.region,
            settings.endpoint.as_deref(),
            settings.path_style,
        )?;
        key::object_key(
            &settings.prefix,
            &settings.partition,
            0,
            SeqId { id: [0; 16], seq: 0 },
            "",
        )
        .map_err(|err| anyhow::anyhow!("s3_out 'partition' can't be formatted: {err}"))?;
        let credentials = match &settings.credentials {
            Some(c) => CredentialSource::Static(Arc::new(Credentials {
                access_key_id: c.access_key_id.clone(),
                secret_access_key: c.secret_access_key.clone(),
                session_token: c.session_token.clone(),
                expires: None,
                source: "the 'credentials' block".to_string(),
            })),
            None => CredentialSource::Chain(Chain::new(settings.timeout)),
        };
        let client = crate::http::build_client(
            settings.timeout,
            settings.connect_timeout,
            settings.idle_timeout,
            None,
        );
        Ok(Self {
            object: StagedObject::new(settings.format, settings.compression),
            settings,
            addressing,
            client,
            tls: None,
            credentials,
            open: None,
            pin: None,
            closing: false,
            owed: 0,
            visible: false,
            answered: None,
            observed: None,
            clock: wall_clock_nanos,
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
        })
    }

    /// Takes credentials from `chain` when `credentials:` is unset. Tests point it at a stand-in.
    pub fn with_credential_chain(mut self, chain: Chain) -> Self {
        if let CredentialSource::Chain(_) = self.credentials {
            self.credentials = CredentialSource::Chain(chain);
        }
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Client TLS tuning (`tls:`) for an `https://` store. A no-op when `settings` is empty.
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
        self.client = crate::http::build_client(
            self.settings.timeout,
            self.settings.connect_timeout,
            self.settings.idle_timeout,
            self.tls.as_ref(),
        );
        Ok(self)
    }

    #[cfg(test)]
    fn with_clock(mut self, clock: fn() -> i64) -> Self {
        self.clock = clock;
        self
    }

    /// Opens an object whose first batch is `head`, under the head's pin when it has one.
    fn open(&mut self, head: SeqId) -> anyhow::Result<()> {
        let now_ms = (self.clock)().div_euclid(1_000_000);
        let pin = match self.pin {
            Some(pin) if pin.head == head => pin,
            _ => Pin::new(head, now_ms),
        };
        self.pin = Some(pin);
        let ext = key::extension(self.settings.format, self.settings.compression);
        let key = key::object_key(
            &self.settings.prefix,
            &self.settings.partition,
            pin.object_time_ms,
            head,
            ext,
        )
        .map_err(|err| anyhow::anyhow!("s3_out couldn't format an object key: {err}"))
        .context(Fault::Refused)?;
        self.object.reset();
        self.open =
            Some(OpenObject { key, deadline: tokio::time::Instant::now() + self.settings.max_age });
        self.closing = false;
        self.owed = 0;
        self.visible = false;
        Ok(())
    }

    /// Whether the open object takes no more batches.
    fn full(&self) -> bool {
        let staged = self.object.batches();
        self.pin.is_some_and(|pin| pin.reached(staged))
            || self.object.encoded_bytes() >= self.settings.max_bytes
            || self.object.compressed_bytes() as u64 >= self.settings.part_size
    }

    async fn credentials(&self) -> anyhow::Result<Arc<Credentials>> {
        match &self.credentials {
            CredentialSource::Static(credentials) => Ok(Arc::clone(credentials)),
            CredentialSource::Chain(chain) => chain.credentials(&self.settings.region).await,
        }
    }

    /// The headers a `PutObject` of a body hashing to `sha256` sends and signs, `host` among
    /// them.
    fn put_headers(
        &self,
        credentials: &Credentials,
        date: &AmzDate,
        sha256: &str,
    ) -> Vec<(String, String)> {
        let content_type = match (self.settings.compression, self.settings.format) {
            (ObjectCompression::Gzip, _) => "application/gzip",
            (ObjectCompression::Zstd, _) => "application/zstd",
            (ObjectCompression::None, ObjectFormat::Json) => "application/x-ndjson",
            (ObjectCompression::None, ObjectFormat::Native) => "application/octet-stream",
        };
        let mut headers: Vec<(&str, String)> = vec![
            ("content-type", content_type.to_string()),
            ("host", self.addressing.host().to_string()),
            ("x-amz-content-sha256", sha256.to_string()),
            ("x-amz-date", date.datetime.clone()),
        ];
        if let Some(token) = &credentials.session_token {
            headers.push(("x-amz-security-token", token.clone()));
        }
        if let Some(class) = &self.settings.storage_class {
            headers.push(("x-amz-storage-class", class.clone()));
        }
        match &self.settings.sse {
            None => {}
            Some(Sse::Aes256) => headers.push(("x-amz-server-side-encryption", "AES256".into())),
            Some(Sse::AwsKms { kms_key_id, bucket_key }) => {
                headers.push(("x-amz-server-side-encryption", "aws:kms".into()));
                if let Some(id) = kms_key_id {
                    headers.push(("x-amz-server-side-encryption-aws-kms-key-id", id.clone()));
                }
                if let Some(on) = bucket_key {
                    headers
                        .push(("x-amz-server-side-encryption-bucket-key-enabled", on.to_string()));
                }
            }
        }
        headers.into_iter().map(|(name, value)| (name.to_string(), value)).collect()
    }

    /// One `PutObject` of `object` at `key`, classified by the module doc's "Response classes".
    async fn put_object(&mut self, key: &str, object: FinishedObject) -> anyhow::Result<()> {
        let credentials = self.credentials().await?;
        let date = AmzDate::from_unix_nanos((self.clock)());
        let (url, path) = self.addressing.object(key);
        let headers = self.put_headers(&credentials, &date, &object.sha256);
        let authorization = sign::authorization(
            &credentials,
            &self.settings.region,
            &date,
            "PUT",
            &path,
            &headers,
            &object.sha256,
        );
        let mut request = self
            .client
            .put(url)
            .timeout(self.settings.timeout)
            .header(reqwest::header::AUTHORIZATION, authorization);
        for (name, value) in headers.iter().filter(|(name, _)| name != "host") {
            request = request.header(name.as_str(), value.as_str());
        }
        let bytes = object.body.len();
        self.visible = true;
        let started = std::time::Instant::now();
        let result = request.body(object.body).send().await;
        // A failed dial sent nothing.
        if !result.as_ref().is_err_and(reqwest::Error::is_connect) {
            self.telemetry.count("logit.output.bytes", bytes as f64, &[]);
        }
        let op = [("op", "put_object")];
        let response = match result {
            Ok(response) => response,
            Err(err) => {
                let fault = crate::http::classify_reqwest_error(&err);
                self.telemetry.timing("logit.output.request.duration", started.elapsed(), &op);
                self.count_request(crate::http::transport_class(fault));
                return Err(anyhow::Error::new(err))
                    .with_context(|| self.failed("PutObject", key))
                    .context(fault);
            }
        };
        let status = response.status();
        self.telemetry.timing("logit.output.request.duration", started.elapsed(), &op);
        self.count_request(crate::http::status_class(status));
        if status.is_success() {
            return Ok(());
        }
        let region = response
            .headers()
            .get("x-amz-bucket-region")
            .and_then(|value| value.to_str().ok())
            .map(|region| crate::http::body_snippet(region, 64));
        let body = crate::http::read_body_bytes(response, xml::ERROR_BODY_BYTES).await;
        let code = xml::element(&body, "Code");
        let fault = classify(status.as_u16(), code.as_deref());
        if matches!(code.as_deref(), Some("ExpiredToken" | "TokenRefreshRequired")) {
            if let CredentialSource::Chain(chain) = &self.credentials {
                chain.forget(&credentials);
            }
        }
        let detail = match &code {
            Some(code) => {
                let message = xml::element(&body, "Message").unwrap_or_default();
                format!(
                    "{code}: {}",
                    crate::http::body_snippet(&message, crate::http::ERROR_BODY_SNIPPET_BYTES)
                )
            }
            None => crate::http::body_snippet(
                &String::from_utf8_lossy(&body),
                crate::http::ERROR_BODY_SNIPPET_BYTES,
            ),
        };
        let redirect = match (status.is_redirection(), region) {
            (true, Some(region)) => {
                format!(" -- the bucket is in region {region:?}; set 'region' to it")
            }
            _ => String::new(),
        };
        Err(anyhow::anyhow!("{} ({status}) {detail}{redirect}", self.failed("PutObject", key)))
            .context(fault)
    }

    fn failed(&self, op: &str, key: &str) -> String {
        format!("S3 {op} of {key:?} to bucket {:?} failed", self.settings.bucket)
    }

    fn count_request(&self, class: &'static str) {
        self.telemetry.count(
            "logit.output.requests",
            1.0,
            &[("op", "put_object"), ("class", class)],
        );
    }
}

/// The class of an S3 error answer: the module doc's "Response classes", by status and the
/// body's `<Code>`. Every row is here, the multipart rows included.
pub(crate) fn classify(status: u16, code: Option<&str>) -> Fault {
    let code = code.unwrap_or_default();
    match status {
        300..=399 => Fault::Refused,
        400 => match code {
            "RequestTimeout"
            | "IncompleteBody"
            | "BadDigest"
            | "XAmzContentSHA256Mismatch"
            | "ExpiredToken"
            | "TokenRefreshRequired"
            | "InvalidPart"
            | "InvalidPartOrder"
            | "EntityTooSmall" => Fault::Clean,
            "EntityTooLarge"
            | "InvalidArgument"
            | "InvalidToken"
            | "AuthorizationHeaderMalformed"
            | "InvalidBucketName"
            | "InvalidEncryptionAlgorithmError"
            | "InvalidStorageClass"
            | "AccessControlListNotSupported"
            | "KeyTooLongError"
            | "InvalidRequest" => Fault::Refused,
            kms if kms.starts_with("KMS.") => Fault::Refused,
            _ => Fault::Rejected,
        },
        403 | 411 | 413 | 501 => Fault::Refused,
        404 if code == "NoSuchUpload" => Fault::Clean,
        404 => Fault::Refused,
        408 => Fault::Ambiguous,
        409 if code == "OperationAborted" => Fault::Clean,
        429 => Fault::Clean,
        503 if !code.is_empty() => Fault::Clean,
        500..=599 => Fault::Ambiguous,
        _ => Fault::Rejected,
    }
}

#[async_trait::async_trait]
impl Output for S3Output {
    /// One object holding `batch` alone, for a caller outside the runtime, which takes the
    /// window path.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let seq = self.observed.unwrap_or(SeqId { id: [0; 16], seq: 0 });
        self.submit(batch, BatchContext::default(), seq).await?;
        self.await_ack().await
    }

    fn observe_batch(&mut self, _ctx: BatchContext, seq: SeqId) {
        self.observed = Some(seq);
    }

    fn window(&self) -> usize {
        if self.owed > 0 {
            self.owed
        } else {
            usize::MAX
        }
    }

    fn collecting(&self) -> Option<tokio::time::Instant> {
        if self.owed > 0 || self.closing || self.object.batches() == 0 || self.full() {
            return None;
        }
        self.open.as_ref().map(|open| open.deadline)
    }

    fn reached_destination(&self, left_the_process: usize) -> usize {
        if self.visible {
            left_the_process
        } else {
            0
        }
    }

    fn send_duration_per_batch(&self) -> bool {
        true
    }

    fn answered_send_duration(&self) -> Option<Duration> {
        self.answered
    }

    async fn submit(
        &mut self,
        batch: &EventBatch,
        _ctx: BatchContext,
        seq: SeqId,
    ) -> anyhow::Result<()> {
        if self.open.is_none() || self.closing || self.object.batches() == 0 {
            self.open(seq)?;
        }
        self.object
            .stage(batch)
            .map_err(|err| anyhow::anyhow!("s3_out couldn't encode a batch: {err}"))
            .context(Fault::Rejected)
    }

    async fn await_ack(&mut self) -> anyhow::Result<()> {
        if self.owed > 0 {
            self.owed -= 1;
            self.answered = None;
            if self.owed == 0 {
                self.open = None;
            }
            return Ok(());
        }
        let Some(open) = &self.open else {
            return Err(anyhow::anyhow!("s3_out has no object to complete")).context(Fault::Clean);
        };
        let key = open.key.clone();
        let staged = self.object.batches();
        self.closing = true;
        let closed_at = tokio::time::Instant::now();
        if let Some(pin) = &mut self.pin {
            pin.close_at(staged);
        }
        let object = self.object.finish();
        let result = self.put_object(&key, object).await;
        self.closing = false;
        match result {
            Ok(()) => {
                self.telemetry.count("logit.output.objects", 1.0, &[]);
                self.answered = Some(closed_at.elapsed());
                self.owed = staged.saturating_sub(1);
                if self.owed == 0 {
                    self.open = None;
                }
                Ok(())
            }
            Err(err) => {
                self.open = None;
                self.answered = None;
                Err(err)
            }
        }
    }
}
