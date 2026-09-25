//! `datadog_out`: sends a batch straight to Datadog's intake API, one `POST` per route the batch
//! needs. It mirrors `logit_inputs::datadog` (`datadog_in`), and the pair is a like-protocol relay
//! under [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md)
//! ([ADR `datadog-agent-and-intake-relay`](../../../docs/adr/datadog-agent-and-intake-relay.md),
//! decisions 2, 7, 9, and 10; [`docs/plans/datadog-relay.md`](../../../docs/plans/datadog-relay.md)
//! §2, §11, §12). Every body comes from [`DatadogEncoder`], whose module doc holds the mappings;
//! this module owns HTTP: which events go to which route, what isn't sent at all, how a route's
//! events are cut into requests, and what a response means.
//!
//! Like `prometheus_out`, this sink is outside the three encoder shapes: Datadog has several
//! routes per signal, each with its own body.
//!
//! ## Config
//!
//! ```yaml
//! kind: datadog_out
//! api_key: !env DD_API_KEY   # sent as DD-API-KEY; never logged
//! site: datadoghq.com        # default; the hosts are api., http-intake.logs., trace.agent.<site>
//! endpoints:                 # optional base URLs replacing the derived hosts, per intake
//!   api: http://127.0.0.1:8080
//!   logs: http://127.0.0.1:8080
//!   traces: http://127.0.0.1:8080
//! compression: gzip          # default; `none` sends every body uncompressed
//! timeout: 10s               # default; bounds one request
//! headers: {}                # extra headers; `!env` works on a value
//! tls: {}                    # tunes every https:// request
//! ```
//!
//! There are no per-sink host, service, source, or tag fields (ADR decision 10): the encoders read
//! them from each event's attributes and the batch resource, so an upstream `set` stamps them.
//! Graph rule 66 validates the block.
//!
//! ## Routes
//!
//! Each event is routed by predicate, in this order: APM stats ([`is_datadog_stats`]), a service
//! check ([`is_service_check`]), a Datadog event ([`is_datadog_event`]), then its metrics, its
//! log, and its span. A stats event goes to the stats route alone; any other event can feed
//! several routes (a log with metrics, a service check with later records).
//!
//! | What in the batch | Route (intake + path) | Body | `Content-Type` |
//! |---|---|---|---|
//! | metric records other than `Samples`/`Distribution` (a service check's record 0 excluded) | api `/api/v2/series` | [`DatadogEncoder::encode_series_v2_protobuf`] | `application/x-protobuf` |
//! | `Samples` | api `/api/v1/distribution_points` | [`DatadogEncoder::encode_distribution_points`] | `application/json` |
//! | `Distribution` | api `/api/beta/sketches` | [`DatadogEncoder::encode_sketches`] | `application/x-protobuf` |
//! | service checks | api `/api/v1/check_run` | [`DatadogEncoder::encode_service_checks`] | `application/json` |
//! | Datadog events | api `/api/v1/events`, one request per event | [`DatadogEncoder::encode_events`] under [`EventFormat::PublicV1`] | `application/json` |
//! | every other log | logs `/api/v2/logs` | [`DatadogEncoder::encode_logs`] | `application/json` |
//! | spans in a ready chunk (below) | traces `/api/v0.2/traces` | [`DatadogEncoder::encode_agent_payload`] | `application/x-protobuf` |
//! | APM stats | traces `/api/v0.2/stats` | [`DatadogEncoder::encode_stats_payload`] | `application/msgpack` |
//!
//! The routes go out in that order, sequentially. An encoder that returns `None` (nothing in its
//! events it can send, all skipped and counted by the encoder) means no request. Events use the
//! documented public route rather than the Agent's `/intake/` envelope, so one event is one
//! request.
//!
//! **Hosts.** Each intake is `https://api.<site>`, `https://http-intake.logs.<site>`, or
//! `https://trace.agent.<site>`, unless `endpoints` gives a base URL for it, to which the path is
//! appended (a trailing `/` on the base is dropped). Pointing all three at one `datadog_in` is how
//! the pair test works, and how a relay chain does.
//!
//! ## What is never sent
//!
//! **Stale points.** Datadog documents a window per route and discards data outside it, so before
//! encoding, relative to the send time, this sink drops and counts
//! `logit.output.records.dropped{reason="stale"}`, per record:
//!
//! | Route | Dropped when |
//! |---|---|
//! | series, distribution points, sketches | older than 1h, or more than 10 min in the future |
//! | logs, events | older than 18h |
//! | service checks | older than 10 min |
//! | traces, stats | never: neither route has a documented window |
//!
//! A `buffer.disk:` replaying after a long outage therefore sends only what is still inside these
//! windows. Anything older is counted `stale` and dropped at replay time, not delivered late.
//!
//! The series window is the documented one, and stricter than the intake, which stored older
//! points in a trial-org run (`docs/plans/datadog-relay.md`, "Verification"). That plan's
//! "Timestamp windows" section has what the intake stored and how it treats a point too far ahead.
//!
//! **Traces an Agent hasn't processed** (ADR decision 2). The intake's trace route expects what an
//! Agent sends: normalized, obfuscated, `_top_level`-marked spans, with the Agent's stats beside
//! them. [`trace_readiness`] decides per trace chunk from its root span: a root with the Agent's
//! `_top_level` mark goes out; a Datadog span without it is counted
//! `records.dropped{reason="needs_agent_processing"}`, and a span with no Datadog attribute at all
//! `records.dropped{reason="not_datadog_origin"}`, one per span. So `datadog_trace_in` must not
//! feed this sink directly: its spans are raw tracer output. Route them to `datadog_trace_out` and
//! a real Agent, and OTel spans to `otlp_out`.
//!
//! ## Size limits
//!
//! | Route | Entries per request | Uncompressed body | Body on the wire |
//! |---|---|---|---|
//! | series (points), distribution points and sketches (records; the series limits) | 10,000 | 5,242,880 B | 512,000 B |
//! | logs | 1,000 | 5,000,000 B | -- |
//! | events | 1 | -- | -- |
//! | traces | -- | 3,200,000 B | -- |
//! | service checks, stats | -- | -- | -- |
//!
//! A route's events are cut into requests by entry count first ([`split_encode`]; an event with
//! several records weighs as many entries). A request whose encoded body is over a byte limit is
//! bisected and each half re-encoded, down to one event, and a single event still over the limit
//! is dropped, counted `records.dropped{reason="oversize"}` for its entries, with a throttled
//! `oversize` diagnostic. A bisected event's encoder counters (`metrics.degraded`, say) count once
//! per encode attempt. Datadog's 1 MB per-log limit isn't enforced here: the intake truncates such
//! a log and still accepts it.
//!
//! The series wire limit is the intake's: a 512,180 B gzip body drew `413` ("limit=512 kB"). The
//! intake enforced none of the others at the sizes tried (distribution points: 1,052,533 B gzip
//! holding 150,000 values; logs: 5,252,247 B uncompressed), so distribution points and sketches
//! keep the series limits and logs the documented 5,000,000 B, which cost extra requests rather
//! than a `413`.
//!
//! ## The wire
//!
//! Headers are the operator's `headers:` with these `insert`ed over them, so a protocol-owned name
//! always wins (rule 66 also rejects one at config time):
//!
//! | Header | Value |
//! |---|---|
//! | `DD-API-KEY` | `api_key`, marked sensitive |
//! | `Content-Type` | per route, above |
//! | `Content-Encoding` | `gzip` under `compression: gzip`, except `deflate` (zlib-wrapped) on distribution points, which Datadog documents as deflate-only (the intake takes gzip and zlib there, and rejects raw deflate), and none on events, whose route answers any compressed body `400 Invalid JSON structure`; absent under `compression: none` |
//! | `User-Agent` | `logit/<version>` |
//!
//! The key never appears in a diagnostic or an error: a rejection body is read past the quoted
//! snippet size by the key's own length ([`error_read_bytes`]) so an echoed key is read whole and
//! replaced with `<redacted>` before the snippet is cut, and a trailing remnant of the key split
//! by the read limit itself is stripped after ([`strip_key_remnant`]).
//!
//! ## Faults, retries, and duplicate safety
//!
//! **One `send` is one attempt per request.** The first failing request aborts the rest of the
//! batch's requests, and `write_loop` retries the whole batch, re-sending any request that had
//! already succeeded (`otlp_out`'s rule). The outcome is classified with [`crate::http`]'s
//! helpers, with two additions for Datadog's documented retry set:
//!
//! | Outcome | Result |
//! |---|---|
//! | 2xx (logs answer `202`) | `Ok` |
//! | 408, 429, any 5xx | [`Fault::Ambiguous`] |
//! | 403 | [`Fault::Permanent`], with a throttled `api_key_rejected` diagnostic saying Datadog refused the key |
//! | 413 | [`Fault::Permanent`], and the request's entries counted `records.dropped{reason="oversize"}` |
//! | any other 3xx or 4xx | [`Fault::Permanent`], with a throttled `request_rejected` diagnostic quoting the first 256 bytes of the body |
//! | connect failure | [`Fault::Clean`] |
//! | any other transport error, timeout included | [`Fault::Ambiguous`] |
//!
//! Redirects aren't followed ([`crate::http::build_client`] says why).
//!
//! [`DatadogOutput::duplicate_safe`] is **`false`**: a batch spans several requests, so a retry
//! re-sends the ones that succeeded. A trial org was sent two resends: a resent series point was
//! stored once, the last write winning at its `(series, timestamp)`, and an identical log was
//! stored twice. Every other route (distribution points, sketches, events, checks, traces, stats)
//! is assumed to store a resend again until measured. So the default posture is at-most-once, and
//! a 5xx drops the batch; `buffer: { delivery: at_least_once }` retries and accepts those
//! duplicates instead.
//!
//! ## Telemetry
//!
//! | Point | Meaning |
//! |---|---|
//! | `logit.output.requests{route, class}` | one per request; `class` is [`crate::http::status_class`]'s, or `network_error` |
//! | `logit.output.request.duration{route}` | one timer per request |
//! | `logit.output.request.bytes{route}` | the body as sent, after compression |
//! | `logit.output.records{route}` | entries in a request Datadog accepted |
//! | `logit.output.records.dropped{route, reason}` | `stale`, `oversize`, `needs_agent_processing`, `not_datadog_origin`, as above |
//!
//! Plus everything [`DatadogEncoder`] counts itself (`logit.output.metrics.skipped`, including
//! `unsupported_kind`-style skips by `metric_kind`; `metrics.degraded`; `tags.dropped`;
//! `spans.degraded`; `stats.*`), which this sink doesn't repeat.

use crate::http::{
    body_snippet, build_client, classify_reqwest_error, read_body_prefix, split_encode,
    status_class, Caps, Encoded, ERROR_BODY_SNIPPET_BYTES,
};
/// `tls:`: the shared `crate::tls` type, re-exported as the other sinks do.
pub use crate::tls::TlsClientSettings;
use anyhow::Context;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use logit_core::{Diagnostics, EventBatch, MetricKind, Telemetry};
use logit_pipeline::{Fault, Output};
use logit_proto::datadog::events::EventFormat;
use logit_proto::datadog::{
    is_datadog_event, is_datadog_stats, is_service_check, trace_readiness, DatadogEncoder,
    TraceReadiness,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::time::Duration;

/// The default `site:`, Datadog's US1.
pub const DEFAULT_SITE: &str = "datadoghq.com";

/// The default `timeout:` for one request, the 10s `otlp_out` and `prometheus_out` use.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// The `User-Agent` on every request. Reserved in config (rule 66).
const USER_AGENT: &str = concat!("logit/", env!("CARGO_PKG_VERSION"));

const REQUESTS: &str = "logit.output.requests";
const REQUEST_DURATION: &str = "logit.output.request.duration";
const REQUEST_BYTES: &str = "logit.output.request.bytes";
const RECORDS: &str = "logit.output.records";
const RECORDS_DROPPED: &str = "logit.output.records.dropped";

const MINUTE: i64 = 60 * 1_000_000_000;
/// Datadog's series window: a point more than 1h old or 10 min ahead is rejected.
const METRIC_MAX_AGE: i64 = 60 * MINUTE;
const METRIC_MAX_AHEAD: i64 = 10 * MINUTE;
/// Logs (`/api/v2/logs`) and events (`date_happened`): 18h.
const LOG_MAX_AGE: i64 = 18 * 60 * MINUTE;
/// Service checks: 10 min.
const CHECK_MAX_AGE: i64 = 10 * MINUTE;

/// Whether request bodies are compressed. Mirrors `logit_config::DatadogCompression`, which
/// `logit-cli::pipeline::build_spec` translates, since this crate doesn't depend on
/// `logit-config`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DatadogCompression {
    /// gzip, except zlib-wrapped deflate on distribution points and no compression on events.
    #[default]
    Gzip,
    None,
}

/// Base URLs replacing the hosts `site` derives (`endpoints:` in config). Mirrors
/// `logit_config::DatadogEndpoints`.
#[derive(Debug, Clone, Default)]
pub struct DatadogEndpoints {
    pub api: Option<String>,
    pub logs: Option<String>,
    pub traces: Option<String>,
}

/// One of Datadog's three intake hosts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Intake {
    Api,
    Logs,
    Traces,
}

impl Intake {
    /// The host prefix before `.<site>`.
    fn prefix(self) -> &'static str {
        match self {
            Self::Api => "api",
            Self::Logs => "http-intake.logs",
            Self::Traces => "trace.agent",
        }
    }
}

/// A request's route: the module doc's "Routes" table, in send order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Route {
    Series,
    DistributionPoints,
    Sketches,
    CheckRun,
    Events,
    Logs,
    Traces,
    Stats,
}

const ROUTES: [Route; 8] = [
    Route::Series,
    Route::DistributionPoints,
    Route::Sketches,
    Route::CheckRun,
    Route::Events,
    Route::Logs,
    Route::Traces,
    Route::Stats,
];

impl Route {
    /// The `route` tag.
    fn name(self) -> &'static str {
        match self {
            Self::Series => "series",
            Self::DistributionPoints => "distribution_points",
            Self::Sketches => "sketches",
            Self::CheckRun => "check_run",
            Self::Events => "events",
            Self::Logs => "logs",
            Self::Traces => "traces",
            Self::Stats => "stats",
        }
    }

    fn intake(self) -> Intake {
        match self {
            Self::Logs => Intake::Logs,
            Self::Traces | Self::Stats => Intake::Traces,
            _ => Intake::Api,
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Series => "/api/v2/series",
            Self::DistributionPoints => "/api/v1/distribution_points",
            Self::Sketches => "/api/beta/sketches",
            Self::CheckRun => "/api/v1/check_run",
            Self::Events => "/api/v1/events",
            Self::Logs => "/api/v2/logs",
            Self::Traces => "/api/v0.2/traces",
            Self::Stats => "/api/v0.2/stats",
        }
    }

    fn content_type(self) -> &'static str {
        match self {
            Self::Series | Self::Sketches | Self::Traces => "application/x-protobuf",
            Self::Stats => "application/msgpack",
            _ => "application/json",
        }
    }

    /// The module doc's "Size limits" row.
    fn caps(self) -> Caps {
        match self {
            Self::Series | Self::DistributionPoints | Self::Sketches => {
                Caps { entries: 10_000, raw_bytes: 5_242_880, wire_bytes: 512_000 }
            }
            Self::Logs => Caps { entries: 1_000, raw_bytes: 5_000_000, wire_bytes: usize::MAX },
            Self::Events => Caps { entries: 1, ..Caps::UNBOUNDED },
            Self::Traces => Caps { raw_bytes: 3_200_000, ..Caps::UNBOUNDED },
            Self::CheckRun | Self::Stats => Caps::UNBOUNDED,
        }
    }

    fn body_encoding(self, compression: DatadogCompression) -> BodyEncoding {
        match (compression, self) {
            // The events route answers any compressed body `400 Invalid JSON structure`.
            (DatadogCompression::None, _) | (_, Self::Events) => BodyEncoding::Identity,
            (DatadogCompression::Gzip, Self::DistributionPoints) => BodyEncoding::Deflate,
            (DatadogCompression::Gzip, _) => BodyEncoding::Gzip,
        }
    }

    /// This route's body for `batch`, uncompressed, or `None` when the encoder finds nothing to
    /// send in it.
    fn encode(self, encoder: &mut DatadogEncoder, batch: &EventBatch) -> Option<Bytes> {
        match self {
            Self::Series => encoder.encode_series_v2_protobuf(batch),
            Self::DistributionPoints => encoder.encode_distribution_points(batch),
            Self::Sketches => encoder.encode_sketches(batch),
            Self::CheckRun => encoder.encode_service_checks(batch),
            // `batch` is one event here (the route's entry cap is 1), so this is one body.
            Self::Events => encoder.encode_events(batch, EventFormat::PublicV1).into_iter().next(),
            Self::Logs => encoder.encode_logs(batch),
            Self::Traces => encoder.encode_agent_payload(batch),
            Self::Stats => encoder.encode_stats_payload(batch),
        }
    }
}

/// A request body's `Content-Encoding`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BodyEncoding {
    Identity,
    Gzip,
    /// zlib-wrapped, what Datadog (and the Agent's `zlib` compressor) mean by `deflate`.
    Deflate,
}

impl BodyEncoding {
    fn header(self) -> Option<&'static str> {
        match self {
            Self::Identity => None,
            Self::Gzip => Some("gzip"),
            Self::Deflate => Some("deflate"),
        }
    }

    /// Inline, not `spawn_blocking`: a request is at most a few MiB, which compresses in
    /// milliseconds.
    fn apply(self, raw: Bytes) -> Bytes {
        let level = flate2::Compression::default();
        let out = match self {
            Self::Identity => return raw,
            Self::Gzip => {
                let mut e = flate2::write::GzEncoder::new(Vec::new(), level);
                e.write_all(&raw).expect("writing to an in-memory Vec never fails");
                e.finish()
            }
            Self::Deflate => {
                let mut e = flate2::write::ZlibEncoder::new(Vec::new(), level);
                e.write_all(&raw).expect("writing to an in-memory Vec never fails");
                e.finish()
            }
        };
        Bytes::from(out.expect("finishing an in-memory encoder never fails"))
    }
}

/// One event's place on one route: its index in the batch and how many entries it weighs there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Item {
    index: usize,
    weight: usize,
}

/// The events of `batch` named by `items`: the batch itself when that is every event, else a
/// copy sharing its resource and scope. `items` is in ascending index order with no repeats.
fn sub_batch<'a>(batch: &'a EventBatch, items: &[Item]) -> Cow<'a, EventBatch> {
    if items.len() == batch.events.len() {
        return Cow::Borrowed(batch);
    }
    Cow::Owned(EventBatch {
        resource: batch.resource.clone(),
        scope: batch.scope.clone(),
        events: items.iter().map(|item| batch.events[item.index].clone()).collect(),
    })
}

/// Wall-clock Unix nanoseconds, the "send time" the stale windows are measured from.
fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
}

/// How many bytes to read from a rejection body before cutting it to [`ERROR_BODY_SNIPPET_BYTES`]:
/// enough past the snippet size that a key echoed within the kept snippet is read whole, rather
/// than cut mid-key by the read limit itself, so [`DatadogOutput::redact`]'s whole-key match can
/// still catch it before the cut.
fn error_read_bytes(key: &str) -> usize {
    ERROR_BODY_SNIPPET_BYTES + key.len()
}

/// After `snippet` is cut to size, strips a trailing run of 4 or more bytes that is itself a
/// prefix of `key`. That run is what's left of a key split by [`error_read_bytes`]'s own read
/// limit, which [`DatadogOutput::redact`]'s whole-key match can't catch because the read never
/// captured the whole key.
fn strip_key_remnant(snippet: String, key: &str) -> String {
    let (content, ellipsis) = match snippet.strip_suffix("...") {
        Some(rest) => (rest, "..."),
        None => (snippet.as_str(), ""),
    };
    let max_run = content.len().min(key.len());
    for len in (4..=max_run).rev() {
        let cut = content.len() - len;
        if !content.is_char_boundary(cut) {
            continue;
        }
        if key.as_bytes().starts_with(&content.as_bytes()[cut..]) {
            return format!("{}{ellipsis}", &content[..cut]);
        }
    }
    snippet
}

/// The Datadog intake client (module doc).
///
/// Not `Debug`: it holds the API key.
pub struct DatadogOutput {
    /// `DD-API-KEY`, marked sensitive so `http`'s own `Debug` never prints it.
    api_key: HeaderValue,
    site: String,
    endpoints: DatadogEndpoints,
    compression: DatadogCompression,
    request_timeout: Duration,
    client: reqwest::Client,
    /// The operator's `headers:`, built once; the protocol's are inserted over a clone per
    /// request.
    headers: HeaderMap,
    /// Built by [`DatadogOutput::with_tls`]; `None` keeps `reqwest`'s default trust.
    tls: Option<rustls::ClientConfig>,
    encoder: DatadogEncoder,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl DatadogOutput {
    /// Fails when `api_key` can't be sent as a header value (a control character, say); the
    /// message never quotes it.
    pub fn new(api_key: &str) -> anyhow::Result<Self> {
        let mut api_key = HeaderValue::from_str(api_key).map_err(|_| {
            anyhow::anyhow!(
                "datadog_out: 'api_key' isn't a legal HTTP header value (it holds a control \
                 character or a non-ASCII byte)"
            )
        })?;
        api_key.set_sensitive(true);
        Ok(Self {
            api_key,
            site: DEFAULT_SITE.to_string(),
            endpoints: DatadogEndpoints::default(),
            compression: DatadogCompression::default(),
            request_timeout: DEFAULT_TIMEOUT,
            client: build_client(DEFAULT_TIMEOUT, None),
            headers: HeaderMap::new(),
            tls: None,
            encoder: DatadogEncoder::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        })
    }

    /// The Datadog site the three intake hosts derive from (`site:`).
    pub fn with_site(mut self, site: impl Into<String>) -> Self {
        self.site = site.into();
        self
    }

    /// Base URLs replacing the derived hosts (`endpoints:`).
    pub fn with_endpoints(mut self, endpoints: DatadogEndpoints) -> Self {
        self.endpoints = endpoints;
        self
    }

    pub fn with_compression(mut self, compression: DatadogCompression) -> Self {
        self.compression = compression;
        self
    }

    /// Per-request timeout (`timeout:`). The client is rebuilt so its default agrees with the
    /// per-request `.timeout(..)` that bounds each request.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self.client = build_client(timeout, self.tls.as_ref());
        self
    }

    /// The extra headers on every request (`headers:`). Fails on a name or value that isn't legal
    /// HTTP, and on two names that collide once case is normalized; rule 66 rejects the
    /// protocol's own names.
    pub fn with_headers(mut self, headers: &HashMap<String, String>) -> anyhow::Result<Self> {
        let mut map = HeaderMap::with_capacity(headers.len());
        for (name, value) in headers {
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .with_context(|| format!("datadog_out: {name:?} is not a legal header name"))?;
            let header_value = HeaderValue::from_str(value)
                .with_context(|| format!("datadog_out: header {name:?} has an invalid value"))?;
            if map.insert(header_name, header_value).is_some() {
                anyhow::bail!(
                    "datadog_out: header {name:?} collides with another entry in 'headers' once \
                     case is ignored -- HTTP header names are case-insensitive, so which value \
                     would actually be sent is undefined"
                );
            }
        }
        self.headers = map;
        Ok(self)
    }

    /// Client TLS tuning (`tls:`) for every `https://` request. A no-op when `settings` is
    /// empty. The files load and validate here, since `graph::resolve` never touches the
    /// filesystem.
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
        let cfg = crate::tls::build_client_config(settings, base_dir)?;
        self.client = build_client(self.request_timeout, Some(&cfg));
        self.tls = Some(cfg);
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

    fn new_encoder(&self) -> DatadogEncoder {
        DatadogEncoder::new()
            .with_telemetry(self.telemetry.clone())
            .with_diagnostics(self.diag.clone())
    }

    /// `route`'s URL: the `endpoints` base for its intake, else `https://<prefix>.<site>`.
    fn url(&self, route: Route) -> String {
        let base = match route.intake() {
            Intake::Api => &self.endpoints.api,
            Intake::Logs => &self.endpoints.logs,
            Intake::Traces => &self.endpoints.traces,
        };
        match base {
            Some(base) => format!("{}{}", base.trim_end_matches('/'), route.path()),
            None => format!("https://{}.{}{}", route.intake().prefix(), self.site, route.path()),
        }
    }

    fn dropped(&self, route: Route, reason: &'static str, n: usize) {
        if n > 0 {
            self.telemetry.count(
                RECORDS_DROPPED,
                n as f64,
                &[("route", route.name()), ("reason", reason)],
            );
        }
    }

    /// Each route's items for `batch` (module doc's "Routes"), indexed by `Route as usize`, after
    /// the stale filter and the trace readiness gate have dropped and counted what they drop.
    fn plan(&self, batch: &EventBatch, now: i64) -> [Vec<Item>; 8] {
        let resource = &batch.resource;
        let readiness = if batch.events.iter().any(|e| e.span.is_some()) {
            trace_readiness(batch)
        } else {
            Vec::new()
        };
        let mut routes: [Vec<Item>; 8] = Default::default();
        for (index, event) in batch.events.iter().enumerate() {
            let mut push = |route: Route, weight: usize| {
                routes[route as usize].push(Item { index, weight });
            };
            if is_datadog_stats(resource, event) {
                push(Route::Stats, 1);
                continue;
            }
            let age = now.saturating_sub(event.timestamp);
            let check = is_service_check(resource, event);
            if check {
                if age > CHECK_MAX_AGE {
                    self.dropped(Route::CheckRun, "stale", 1);
                } else {
                    push(Route::CheckRun, 1);
                }
            }
            let datadog_event = is_datadog_event(resource, event);
            if datadog_event {
                if age > LOG_MAX_AGE {
                    self.dropped(Route::Events, "stale", 1);
                } else {
                    push(Route::Events, 1);
                }
            }
            // A service check's record 0 is the check route's alone.
            let (mut series, mut samples, mut sketches) = (0, 0, 0);
            for record in &event.metrics[usize::from(check)..] {
                match record.kind {
                    MetricKind::Samples(_) => samples += 1,
                    MetricKind::Distribution(_) => sketches += 1,
                    _ => series += 1,
                }
            }
            let metric_stale =
                age > METRIC_MAX_AGE || event.timestamp.saturating_sub(now) > METRIC_MAX_AHEAD;
            for (route, n) in [
                (Route::Series, series),
                (Route::DistributionPoints, samples),
                (Route::Sketches, sketches),
            ] {
                if n == 0 {
                    continue;
                }
                if metric_stale {
                    self.dropped(route, "stale", n);
                } else {
                    push(route, n);
                }
            }
            if event.log.is_some() && !datadog_event {
                if age > LOG_MAX_AGE {
                    self.dropped(Route::Logs, "stale", 1);
                } else {
                    push(Route::Logs, 1);
                }
            }
            if event.span.is_some() {
                match readiness[index] {
                    Some(TraceReadiness::Ready) => push(Route::Traces, 1),
                    Some(other) => {
                        let reason = other.drop_reason().expect("only Ready has no reason");
                        self.dropped(Route::Traces, reason, 1);
                    }
                    None => unreachable!("trace_readiness gives every span event a verdict"),
                }
            }
        }
        routes
    }

    /// [`Output::send`] at a given send time, so tests can pin the stale windows' edges.
    async fn send_at(&mut self, batch: &EventBatch, now: i64) -> anyhow::Result<()> {
        let mut plan = self.plan(batch, now);
        for route in ROUTES {
            let items = std::mem::take(&mut plan[route as usize]);
            if items.is_empty() {
                continue;
            }
            let body_encoding = route.body_encoding(self.compression);
            let encoder = &mut self.encoder;
            let split = split_encode(
                &items,
                route.caps(),
                |item| item.weight,
                |chunk| {
                    let raw = route.encode(encoder, &sub_batch(batch, chunk))?;
                    Some(Encoded { raw_len: raw.len(), body: body_encoding.apply(raw), meta: () })
                },
            );
            for (item, raw_len, wire_len) in split.oversize {
                self.dropped(route, "oversize", item.weight);
                self.diag.warn_throttled(
                    "oversize",
                    format_args!(
                        "dropped one event on the {} route: it encodes to {raw_len} bytes \
                         ({wire_len} compressed), over the route's per-request limit",
                        route.name()
                    ),
                );
            }
            for (chunk, encoded) in split.requests {
                let entries = chunk.iter().map(|item| item.weight).sum();
                self.post(route, encoded, entries).await?;
            }
        }
        Ok(())
    }

    /// The operator's headers with the protocol's `insert`ed over them, so a protocol name
    /// always wins. One `.headers(..)` at the call site, never `RequestBuilder::header`, which
    /// appends and would undo that.
    fn request_headers(&self, route: Route) -> HeaderMap {
        let mut headers = self.headers.clone();
        headers.insert(HeaderName::from_static("dd-api-key"), self.api_key.clone());
        headers.insert(http::header::CONTENT_TYPE, HeaderValue::from_static(route.content_type()));
        match route.body_encoding(self.compression).header() {
            Some(encoding) => {
                headers.insert(http::header::CONTENT_ENCODING, HeaderValue::from_static(encoding));
            }
            None => {
                headers.remove(http::header::CONTENT_ENCODING);
            }
        }
        headers.insert(http::header::USER_AGENT, HeaderValue::from_static(USER_AGENT));
        headers
    }

    /// `text` with the API key replaced, for anything quoted from a response.
    fn redact(&self, text: &str) -> String {
        match self.api_key.to_str() {
            Ok(key) if !key.is_empty() => text.replace(key, "<redacted>"),
            _ => text.to_string(),
        }
    }

    /// One request, one attempt (module doc's "Faults, retries, and duplicate safety").
    async fn post(&mut self, route: Route, encoded: Encoded, entries: usize) -> anyhow::Result<()> {
        let url = self.url(route);
        let tags = [("route", route.name())];
        let wire_len = encoded.body.len();
        let timer = self.telemetry.timer(REQUEST_DURATION);
        let result = self
            .client
            .post(&url)
            .headers(self.request_headers(route))
            .timeout(self.request_timeout)
            .body(encoded.body)
            .send()
            .await;
        timer.stop(&tags);
        self.telemetry.count(REQUEST_BYTES, wire_len as f64, &tags);

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                self.telemetry.count(
                    REQUESTS,
                    1.0,
                    &[("route", route.name()), ("class", "network_error")],
                );
                let fault = classify_reqwest_error(&err);
                return Err(anyhow::Error::new(err)).context(fault);
            }
        };
        let status = response.status();
        self.telemetry.count(
            REQUESTS,
            1.0,
            &[("route", route.name()), ("class", status_class(status))],
        );
        if status.is_success() {
            self.telemetry.count(RECORDS, entries as f64, &tags);
            return Ok(());
        }
        // Bounded, and scrubbed of the key before it reaches a diagnostic or the error: the read
        // goes past the snippet size so a key that starts inside the kept snippet is read whole
        // and redacted before the cut, and a trailing remnant of the key split by the read limit
        // itself is stripped after.
        let key = self.api_key.to_str().unwrap_or_default();
        let body = read_body_prefix(response, error_read_bytes(key)).await;
        let snippet =
            strip_key_remnant(body_snippet(&self.redact(&body), ERROR_BODY_SNIPPET_BYTES), key);
        let fault = match status.as_u16() {
            408 | 429 | 500..=599 => Fault::Ambiguous,
            403 => {
                self.diag.warn_throttled(
                    "api_key_rejected",
                    format_args!(
                        "Datadog refused the API key: {url} answered 403 -- check 'api_key', and \
                         that 'site' is the one the key belongs to"
                    ),
                );
                Fault::Permanent
            }
            413 => {
                self.dropped(route, "oversize", entries);
                self.diag.warn_throttled(
                    "request_rejected",
                    format_args!("{url} answered 413, request too large: {snippet}"),
                );
                Fault::Permanent
            }
            _ => {
                self.diag.warn_throttled(
                    "request_rejected",
                    format_args!("{url} answered {status}: {snippet}"),
                );
                Fault::Permanent
            }
        };
        Err(anyhow::anyhow!(
            "datadog_out: {} request to {url} failed ({status}): {snippet}",
            route.name()
        ))
        .context(fault)
    }
}

#[async_trait::async_trait]
impl Output for DatadogOutput {
    /// One request per route the batch needs, sequentially; the first failure aborts the rest
    /// (module doc's "Faults, retries, and duplicate safety").
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        self.send_at(batch, now_nanos()).await
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
        AttrMap, BodyFormat, DdSketch, Event, LogRecord, MetricRecord, Registry, Resource, Samples,
        SpanKind, SpanRecord, SpanStatus, Value,
    };
    use logit_proto::datadog::events::ATTR_EVENT_TITLE;
    use logit_proto::datadog::service_checks::{
        ATTR_SERVICE_CHECK_NAME, ATTR_SERVICE_CHECK_STATUS,
    };
    use logit_proto::datadog::stats::{ATTR_BUCKET_DURATION, ATTR_STATS_NAME, METRIC_HITS};
    use logit_proto::datadog::{DatadogDecoder, ATTR_RESOURCE_NAME, METRIC_TOP_LEVEL};
    use std::io::Read;
    use std::net::SocketAddr;
    use std::sync::{Arc, Mutex};

    const KEY: &str = "0123456789abcdef0123456789abcdef";
    /// The send time every test pins, so each window's edge is exact.
    const NOW: i64 = 1_790_000_000_000_000_000;
    const HOUR: i64 = 60 * MINUTE;

    // ---- a local intake that records every request ------------------------------------------

    #[derive(Debug, Clone)]
    struct Captured {
        path: String,
        headers: http::HeaderMap,
        body: Vec<u8>,
    }

    impl Captured {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers.get(name).and_then(|v| v.to_str().ok())
        }

        /// The body, decompressed per its `Content-Encoding`.
        fn decoded(&self) -> Vec<u8> {
            let mut out = Vec::new();
            match self.header("content-encoding") {
                None => out.clone_from(&self.body),
                Some("gzip") => {
                    flate2::read::GzDecoder::new(&self.body[..]).read_to_end(&mut out).unwrap();
                }
                Some("deflate") => {
                    flate2::read::ZlibDecoder::new(&self.body[..]).read_to_end(&mut out).unwrap();
                }
                Some(other) => panic!("unexpected content-encoding {other}"),
            }
            out
        }
    }

    type Log = Arc<Mutex<Vec<Captured>>>;

    /// An HTTP/1.1 server recording each request and answering `respond(path)`.
    async fn intake(
        respond: impl Fn(&str) -> (u16, String) + Send + Sync + 'static,
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
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            log.lock().unwrap().push(Captured {
                                path: path.clone(),
                                headers,
                                body: body.to_vec(),
                            });
                            let (status, text) = respond(&path);
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

    async fn accepting() -> (SocketAddr, Log) {
        intake(|path| (if path == "/api/v2/logs" { 202 } else { 200 }, "{}".into())).await
    }

    fn sink(addr: SocketAddr) -> DatadogOutput {
        let base = format!("http://{addr}");
        DatadogOutput::new(KEY).unwrap().with_endpoints(DatadogEndpoints {
            api: Some(base.clone()),
            logs: Some(base.clone()),
            traces: Some(base),
        })
    }

    fn metered(addr: SocketAddr) -> (Arc<Registry>, DatadogOutput) {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("out", "datadog_out", "sink");
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
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn metric(ts: i64, kind: MetricKind) -> Event {
        Event::metric(ts, AttrMap::new(), MetricRecord::new(intern("m"), kind))
    }

    fn gauge(ts: i64) -> Event {
        metric(ts, MetricKind::Gauge(1.5))
    }

    fn sketch(ts: i64) -> Event {
        let mut s = DdSketch::new();
        s.add(1.0);
        s.add(20.0);
        metric(ts, MetricKind::Distribution(s))
    }

    fn log_record(message: &str) -> LogRecord {
        LogRecord {
            message: Value::str(message),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        }
    }

    fn log_event(ts: i64, message: &str) -> Event {
        Event::log(ts, AttrMap::new(), log_record(message))
    }

    fn datadog_event(ts: i64) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_EVENT_TITLE, Value::str("deploy"));
        Event::log(ts, attrs, log_record("v2 is out"))
    }

    fn service_check(ts: i64) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_SERVICE_CHECK_NAME, Value::str("app.ok"));
        attrs.insert(ATTR_SERVICE_CHECK_STATUS, Value::U64(0));
        Event::metric(ts, attrs, MetricRecord::new(intern("app.ok"), MetricKind::Gauge(0.0)))
    }

    fn stats(ts: i64) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert(ATTR_STATS_NAME, Value::str("http.request"));
        attrs.insert(ATTR_BUCKET_DURATION, Value::U64(10_000_000_000));
        Event::metric(ts, attrs, MetricRecord::new(intern(METRIC_HITS), MetricKind::counter(4.0)))
    }

    fn span(trace: u8, id: u8, parent: Option<u8>, attrs: &[(&str, Value)]) -> Event {
        let mut attributes = AttrMap::new();
        for (key, value) in attrs {
            attributes.insert(key, value.clone());
        }
        Event::span(
            NOW,
            attributes,
            SpanRecord {
                trace_id: [trace; 16],
                span_id: [id; 8],
                parent_span_id: parent.map(|p| [p; 8]),
                name: Value::str("op"),
                kind: SpanKind::Internal,
                status: SpanStatus::Unset,
                events: Vec::new(),
                links: Vec::new(),
                end_timestamp: NOW + 1_000,
                flags: 0,
                ext: None,
            },
        )
    }

    fn ready_span(trace: u8, id: u8) -> Event {
        span(
            trace,
            id,
            None,
            &[(ATTR_RESOURCE_NAME, Value::str("GET /")), (METRIC_TOP_LEVEL, Value::F64(1.0))],
        )
    }

    // ---- routes ------------------------------------------------------------------------------

    /// One event per route: each reaches its own path, once, with its own `Content-Type`, and a
    /// log carrying metrics feeds both routes.
    #[tokio::test]
    async fn every_kind_of_event_reaches_its_route() {
        let (addr, log) = accepting().await;
        let mut log_with_metric = log_event(NOW, "both");
        log_with_metric.metrics.push(MetricRecord::new(intern("n"), MetricKind::counter(1.0)));
        let b = batch(vec![
            gauge(NOW),
            metric(NOW, MetricKind::Samples(Samples::new([1.0, 2.0]))),
            sketch(NOW),
            service_check(NOW),
            datadog_event(NOW),
            log_with_metric,
            ready_span(1, 1),
            stats(NOW),
        ]);
        sink(addr).send_at(&b, NOW).await.expect("every route accepts");

        assert_eq!(
            paths(&log),
            [
                "/api/v2/series",
                "/api/v1/distribution_points",
                "/api/beta/sketches",
                "/api/v1/check_run",
                "/api/v1/events",
                "/api/v2/logs",
                "/api/v0.2/traces",
                "/api/v0.2/stats",
            ]
        );
        let captured = log.lock().unwrap().clone();
        let content_types: Vec<_> =
            captured.iter().map(|c| c.header("content-type").unwrap().to_string()).collect();
        assert_eq!(
            content_types,
            [
                "application/x-protobuf",
                "application/json",
                "application/x-protobuf",
                "application/json",
                "application/json",
                "application/json",
                "application/x-protobuf",
                "application/msgpack",
            ]
        );

        let mut decoder = DatadogDecoder::new();
        let series = decoder.decode_series_v2_protobuf(&captured[0].decoded(), NOW).unwrap();
        let names: Vec<_> =
            series.events.iter().map(|e| resolve(e.metrics[0].name).to_string()).collect();
        assert_eq!(names, ["m", "n"], "the gauge and the log's counter; not the check's record 0");
        let logs = decoder.decode_logs(&captured[5].decoded(), NOW).unwrap();
        assert_eq!(logs.events.len(), 1, "the Datadog event isn't a log");
        let events = decoder.decode_events(&captured[4].decoded(), NOW).unwrap();
        assert_eq!(events.events.len(), 1);
    }

    /// A batch with nothing any route sends makes no request.
    #[tokio::test]
    async fn an_empty_batch_sends_nothing() {
        let (addr, log) = accepting().await;
        sink(addr).send_at(&batch(Vec::new()), NOW).await.unwrap();
        assert!(paths(&log).is_empty());
    }

    /// Hosts derive from `site` unless `endpoints` replaces one, whose trailing `/` is dropped.
    #[test]
    fn urls_derive_from_the_site_unless_overridden() {
        let out = DatadogOutput::new(KEY).unwrap().with_site("datadoghq.eu").with_endpoints(
            DatadogEndpoints { logs: Some("http://relay:8080/dd/".into()), ..Default::default() },
        );
        assert_eq!(out.url(Route::Series), "https://api.datadoghq.eu/api/v2/series");
        assert_eq!(out.url(Route::Events), "https://api.datadoghq.eu/api/v1/events");
        assert_eq!(out.url(Route::Logs), "http://relay:8080/dd/api/v2/logs");
        assert_eq!(out.url(Route::Traces), "https://trace.agent.datadoghq.eu/api/v0.2/traces");
        assert_eq!(out.url(Route::Stats), "https://trace.agent.datadoghq.eu/api/v0.2/stats");
        let default = DatadogOutput::new(KEY).unwrap();
        assert_eq!(default.url(Route::Logs), "https://http-intake.logs.datadoghq.com/api/v2/logs");
    }

    // ---- the stale filter --------------------------------------------------------------------

    /// Each window keeps its edge and drops one nanosecond past it, counted per record.
    #[tokio::test]
    async fn the_stale_filter_keeps_each_windows_edge_and_drops_past_it() {
        let (addr, log) = accepting().await;
        let (registry, mut out) = metered(addr);
        let b = batch(vec![
            gauge(NOW - HOUR),
            gauge(NOW - HOUR - 1),
            gauge(NOW + 10 * MINUTE),
            gauge(NOW + 10 * MINUTE + 1),
            log_event(NOW - 18 * HOUR, "kept"),
            log_event(NOW - 18 * HOUR - 1, "dropped"),
            datadog_event(NOW - 18 * HOUR),
            datadog_event(NOW - 18 * HOUR - 1),
            service_check(NOW - 10 * MINUTE),
            service_check(NOW - 10 * MINUTE - 1),
            // Traces and stats have no window.
            span(9, 1, None, &[(METRIC_TOP_LEVEL, Value::F64(1.0))]),
            stats(NOW - 48 * HOUR),
        ]);
        out.send_at(&b, NOW).await.unwrap();

        let captured = log.lock().unwrap().clone();
        let mut decoder = DatadogDecoder::new();
        let body = |path: &str| -> Vec<Vec<u8>> {
            captured.iter().filter(|c| c.path == path).map(Captured::decoded).collect()
        };
        let series = decoder.decode_series_v2_protobuf(&body("/api/v2/series")[0], NOW).unwrap();
        assert_eq!(series.events.len(), 2, "the two in-window gauges");
        let logs = decoder.decode_logs(&body("/api/v2/logs")[0], NOW).unwrap();
        assert_eq!(logs.events.len(), 1);
        assert_eq!(body("/api/v1/events").len(), 1);
        let checks = decoder.decode_service_checks(&body("/api/v1/check_run")[0], NOW).unwrap();
        assert_eq!(checks.events.len(), 1);
        assert_eq!(body("/api/v0.2/traces").len(), 1);
        assert_eq!(body("/api/v0.2/stats").len(), 1);

        let points = registry.drain(0);
        for route in ["series", "logs", "events", "check_run"] {
            let dropped = total(&points, RECORDS_DROPPED, &[("route", route), ("reason", "stale")]);
            let expected = if route == "series" { 2.0 } else { 1.0 };
            assert_eq!(dropped, expected, "{route}");
        }
    }

    // ---- the readiness gate ------------------------------------------------------------------

    /// Only the Agent-processed chunk is sent; the raw tracer chunk and the OTel span are counted
    /// by reason, per span.
    #[tokio::test]
    async fn only_agent_processed_chunks_are_sent() {
        let (addr, log) = accepting().await;
        let (registry, mut out) = metered(addr);
        let raw = [(ATTR_RESOURCE_NAME, Value::str("GET /"))];
        let b = batch(vec![
            ready_span(1, 1),
            span(1, 2, Some(1), &raw),
            span(2, 3, None, &raw),
            span(2, 4, Some(3), &raw),
            span(3, 5, None, &[("http.route", Value::str("/"))]),
        ]);
        out.send_at(&b, NOW).await.unwrap();

        let captured = log.lock().unwrap().clone();
        assert_eq!(paths(&log), ["/api/v0.2/traces"]);
        let sent = DatadogDecoder::new().decode_agent_payload(&captured[0].decoded(), 0).unwrap();
        let ids: Vec<_> = sent[0].events.iter().map(|e| e.span.as_ref().unwrap().span_id).collect();
        assert_eq!(ids, [[1; 8], [2; 8]]);
        let points = registry.drain(0);
        let dropped =
            |reason| total(&points, RECORDS_DROPPED, &[("route", "traces"), ("reason", reason)]);
        assert_eq!(dropped("needs_agent_processing"), 2.0);
        assert_eq!(dropped("not_datadog_origin"), 1.0);
    }

    // ---- the splitter ------------------------------------------------------------------------

    /// 1,001 logs are two requests on the logs route's 1,000-entry cap.
    #[tokio::test]
    async fn a_route_over_its_entry_cap_sends_several_requests() {
        let (addr, log) = accepting().await;
        let (registry, mut out) = metered(addr);
        let b = batch((0..1_001).map(|i| log_event(NOW, &format!("line {i}"))).collect());
        out.send_at(&b, NOW).await.unwrap();
        let captured = log.lock().unwrap().clone();
        let counts: Vec<_> = captured
            .iter()
            .map(|c| DatadogDecoder::new().decode_logs(&c.decoded(), NOW).unwrap().events.len())
            .collect();
        assert_eq!(counts, [1_000, 1]);
        let points = registry.drain(0);
        assert_eq!(total(&points, RECORDS, &[("route", "logs")]), 1_001.0);
    }

    /// One log over the uncompressed cap is dropped and counted; the rest still goes.
    #[tokio::test]
    async fn a_single_event_over_the_byte_cap_is_dropped_as_oversize() {
        let (addr, log) = accepting().await;
        let (registry, mut out) = metered(addr);
        let huge = "x".repeat(5_000_001);
        let b = batch(vec![log_event(NOW, "small"), log_event(NOW, &huge)]);
        out.send_at(&b, NOW).await.unwrap();
        let captured = log.lock().unwrap().clone();
        assert_eq!(captured.len(), 1);
        let sent = DatadogDecoder::new().decode_logs(&captured[0].decoded(), NOW).unwrap();
        assert_eq!(sent.events.len(), 1);
        let points = registry.drain(0);
        assert_eq!(
            total(&points, RECORDS_DROPPED, &[("route", "logs"), ("reason", "oversize")]),
            1.0
        );
    }

    // ---- responses ---------------------------------------------------------------------------

    async fn fault_for(status: u16) -> Fault {
        let (addr, _log) = intake(move |_| (status, String::new())).await;
        let err = sink(addr).send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        logit_pipeline::classify(&err)
    }

    #[tokio::test]
    async fn each_response_class_maps_to_its_fault() {
        for status in [408, 429, 500, 503] {
            assert_eq!(fault_for(status).await, Fault::Ambiguous, "{status}");
        }
        for status in [301, 400, 403, 404, 413] {
            assert_eq!(fault_for(status).await, Fault::Permanent, "{status}");
        }
    }

    /// A logs `202` is success, and the first failing route aborts the ones after it.
    #[tokio::test]
    async fn a_failing_route_aborts_the_rest_of_the_batch() {
        let (addr, log) =
            intake(|path| (if path == "/api/v2/series" { 500 } else { 202 }, String::new())).await;
        let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
        sink(addr).send_at(&b, NOW).await.unwrap_err();
        assert_eq!(paths(&log), ["/api/v2/series"]);

        let (addr, log) = intake(|_| (202, String::new())).await;
        sink(addr).send_at(&b, NOW).await.expect("202 is success");
        assert_eq!(paths(&log), ["/api/v2/series", "/api/v2/logs"]);
    }

    #[tokio::test]
    async fn connect_refused_is_clean() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let err = sink(addr).send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    /// A 413 counts the request's entries oversize.
    #[tokio::test]
    async fn a_413_counts_the_requests_entries_oversize() {
        let (addr, _log) = intake(|_| (413, String::new())).await;
        let (registry, mut out) = metered(addr);
        out.send_at(&batch(vec![gauge(NOW), gauge(NOW)]), NOW).await.unwrap_err();
        let points = registry.drain(0);
        assert_eq!(
            total(&points, RECORDS_DROPPED, &[("route", "series"), ("reason", "oversize")]),
            2.0
        );
        assert_eq!(
            total(&points, REQUESTS, &[("route", "series"), ("class", "4xx")]),
            1.0,
            "the request is counted by its class too"
        );
    }

    // ---- the key -----------------------------------------------------------------------------

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

    /// The key goes out as `DD-API-KEY` and nowhere else: a `403` whose body echoes it is
    /// diagnosed and reported with the key redacted.
    #[tokio::test]
    async fn the_api_key_is_sent_and_never_logged() {
        use tracing_subscriber::util::SubscriberInitExt as _;

        let (addr, log) = intake(|_| (403, format!(r#"{{"errors":["bad key {KEY}"]}}"#))).await;
        let logs = CapturedLogs::default();
        let guard = tracing_subscriber::fmt()
            .with_writer(logs.clone())
            .with_ansi(false)
            .finish()
            .set_default();
        let mut out = sink(addr).with_diagnostics(Diagnostics::new("dd"));
        let err = out.send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        drop(guard);

        assert_eq!(log.lock().unwrap()[0].header("dd-api-key"), Some(KEY));
        let logged = String::from_utf8_lossy(&logs.0.lock().unwrap()).into_owned();
        assert!(logged.contains("Datadog refused the API key"), "{logged}");
        assert!(!logged.contains(KEY), "{logged}");
        let message = format!("{err:#}");
        assert!(message.contains("403") && message.contains("<redacted>"), "{message}");
        assert!(!message.contains(KEY), "{message}");
        assert!(!format!("{err:?}").contains(KEY));
    }

    /// A key echoed by a rejection body starting near the snippet's 256-byte cut is still read
    /// and redacted whole, because the read goes past the cut by the key's own length.
    #[tokio::test]
    async fn a_key_straddling_the_snippet_cut_is_fully_redacted() {
        let start = 240;
        let prefix = "x".repeat(start);
        let suffix = "y".repeat(300 - start - KEY.len());
        let body = format!("{prefix}{KEY}{suffix}");
        assert_eq!(body.len(), 300);

        let (addr, _log) = intake(move |_| (500, body.clone())).await;
        let err = sink(addr).send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        let message = format!("{err:#}");
        assert!(message.contains("<redacted>"), "{message}");
        assert!(!message.contains(KEY), "{message}");
    }

    /// A key can be split by the read limit itself, not only by the snippet cut, leaving a
    /// fragment `redact`'s whole-key match can't catch. `strip_key_remnant` bounds what such a
    /// fragment can leak to fewer than 4 bytes.
    #[tokio::test]
    async fn a_key_split_by_the_read_limit_leaks_no_more_than_a_few_bytes() {
        let start = error_read_bytes(KEY) - 3;
        let prefix = "x".repeat(start);
        let body = format!("{prefix}{KEY}");

        let (addr, _log) = intake(move |_| (500, body.clone())).await;
        let err = sink(addr).send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        let message = format!("{err:#}");
        assert!(!contains_key_run_longer_than(&message, KEY, 3), "{message}");
    }

    /// Whether `text` contains a contiguous run of more than `max_run` bytes that is itself a
    /// substring of `key`.
    fn contains_key_run_longer_than(text: &str, key: &str, max_run: usize) -> bool {
        let key = key.as_bytes();
        for len in (max_run + 1)..=key.len() {
            for start in 0..=(key.len() - len) {
                let window = std::str::from_utf8(&key[start..start + len]).unwrap();
                if text.contains(window) {
                    return true;
                }
            }
        }
        false
    }

    #[test]
    fn an_api_key_that_cant_be_a_header_fails_without_quoting_it() {
        let Err(err) = DatadogOutput::new("secret\nkey") else {
            panic!("a newline can't go in a header value");
        };
        assert!(!format!("{err:#}").contains("secret"), "{err:#}");
    }

    // ---- compression and headers -------------------------------------------------------------

    /// gzip on every route but distribution points, which get zlib deflate, and events, which go
    /// uncompressed; `none` sends every body as-is, with no `Content-Encoding`.
    #[tokio::test]
    async fn gzip_by_default_deflate_for_distribution_points_and_none_when_asked() {
        let b = batch(vec![
            gauge(NOW),
            metric(NOW, MetricKind::Samples(Samples::new([1.0]))),
            datadog_event(NOW),
        ]);

        let (addr, log) = accepting().await;
        sink(addr).send_at(&b, NOW).await.unwrap();
        let captured = log.lock().unwrap().clone();
        assert_eq!(captured[0].header("content-encoding"), Some("gzip"));
        assert_eq!(captured[1].path, "/api/v1/distribution_points");
        assert_eq!(captured[1].header("content-encoding"), Some("deflate"));
        let points = DatadogDecoder::new()
            .decode_distribution_points(&captured[1].decoded(), NOW)
            .expect("a zlib stream, not raw deflate");
        assert_eq!(points.events.len(), 1);
        assert_eq!(captured[2].path, "/api/v1/events");
        assert_eq!(captured[2].header("content-encoding"), None);
        DatadogDecoder::new().decode_events(&captured[2].body, NOW).expect("plain JSON");

        let (addr, log) = accepting().await;
        sink(addr).with_compression(DatadogCompression::None).send_at(&b, NOW).await.unwrap();
        for c in log.lock().unwrap().iter() {
            assert_eq!(c.header("content-encoding"), None, "{}", c.path);
        }
        let captured = log.lock().unwrap().clone();
        DatadogDecoder::new().decode_series_v2_protobuf(&captured[0].body, NOW).unwrap();
    }

    /// An operator header goes out; one spelled like a protocol header loses to it.
    #[tokio::test]
    async fn operator_headers_are_sent_under_the_protocols_own() {
        let (addr, log) = accepting().await;
        let mut out = sink(addr)
            .with_headers(&HashMap::from([
                ("X-Proxy-Token".to_string(), "t".to_string()),
                ("Content-Type".to_string(), "text/plain".to_string()),
                ("DD-API-KEY".to_string(), "not-the-key".to_string()),
                ("User-Agent".to_string(), "other".to_string()),
            ]))
            .unwrap();
        out.send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap();
        let captured = log.lock().unwrap()[0].clone();
        assert_eq!(captured.header("x-proxy-token"), Some("t"));
        assert_eq!(captured.header("content-type"), Some("application/x-protobuf"));
        assert_eq!(captured.header("dd-api-key"), Some(KEY));
        assert_eq!(captured.header("user-agent"), Some(USER_AGENT));
        assert_eq!(captured.headers.get_all("dd-api-key").iter().count(), 1);
    }

    #[test]
    fn datadog_output_is_not_duplicate_safe() {
        assert!(!DatadogOutput::new(KEY).unwrap().duplicate_safe());
    }
}
