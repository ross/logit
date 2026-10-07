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
//! The send time is read once per batch, in `observe_batch`, and every attempt at the batch
//! measures from it: staleness isn't monotonic in the clock (a point too far ahead becomes fresh),
//! so a clock read per attempt could drop a point on one attempt and send it on the next. An `Ok`
//! clears it, and the next `observe_batch` replaces it. A `send` with no `observe_batch` reads the
//! clock itself only when no earlier batch left a time behind: after a batch whose last attempt
//! failed, it reuses that batch's time (`docs/known-gaps/datadog.md`).
//!
//! A retryable fault retries until the batch is delivered or shutdown cuts it, bounded by the
//! sink's `buffer:`. Staleness is judged against the batch's one send time, so a batch held
//! through a long outage can reach the intake with a point past its window.
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
//! `oversize` diagnostic. The re-encodes of a bisection count nothing ([`split_encode`]), so an
//! event's encoder counters (`metrics.degraded`, say) count once per event, at its first encode
//! (that of the count-capped request it fell in), even within a single attempt; an event the
//! encoder degraded and the bisection then dropped counts under both. Datadog's 1 MB per-log
//! limit isn't enforced here: the intake truncates such a log and still accepts it.
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
//! snippet size by the key's own length ([`error_read_bytes`]) and scrubbed of it by
//! [`redacted_snippet`].
//!
//! ## Faults, retries, and duplicate safety
//!
//! **One `send` is one attempt per request, and each request's verdict stands on its own**
//! ([`Outcomes`]; `docs/adr/delivery-semantics.md`'s "Amendment: per-request verdicts
//! (2026-10-04)"). A request Datadog rejects is counted and the send goes on to the next request;
//! a refused key, a `Clean` failure, or an `Ambiguous` one stops the send, and `write_loop` retries
//! the whole batch, re-sending any request that had already succeeded. The send succeeds when any
//! request was accepted, and fails with the first rejection when every request was rejected.
//!
//! Datadog documents its intake statuses by HTTP code, the same on every route, with a free-text
//! body; no documented body code changes a status's meaning, so the status decides. `Rejected`
//! counts the request's entries `records.dropped{reason="rejected"}` (`oversize` for a `413`) with
//! a throttled `request_rejected` diagnostic; a `403` warns `api_key_rejected`, and any other
//! `Refused` warns `request_refused`. Each quotes the first 256 bytes of the body, scrubbed of the
//! key.
//!
//! | Response | Class | Why | Evidence |
//! |---|---|---|---|
//! | `2xx` (logs answer `202`) | `Ok` | accepted for processing | [send logs][logs-api] |
//! | `400` | `Rejected` | "Bad request (likely an issue in the payload formatting)": this body; the Agent drops it too | [send logs][logs-api], [Agent retry guide][agent-retry] |
//! | `401` | `Refused` | "Unauthorized (likely a missing API Key)": the key rides on every request | [send logs][logs-api] |
//! | `403` | `Refused` | "Permission issue (likely using an invalid API Key)", or a key sent to another `site`: one org-wide key on every route, so every request gets it; the Agent retries it after refreshing the key | [send logs][logs-api], the Agent's [`transaction.go`][agent-tx] |
//! | `404` | `Refused` | a route the configured base doesn't serve: a `site` or `endpoints:` override pointed at something that isn't the intake, the same for every batch that uses the route; the Agent's forwarder reschedules a `404` rather than drop it | the Agent's [`transaction.go`][agent-tx] |
//! | `405`, `407` | `Refused` | the path to the intake, not the batch | [`crate::http::classify_status`] |
//! | `408` | `Ambiguous` | "Request Timeout, request should be retried after some time" | [send logs][logs-api] |
//! | `413` | `Rejected` | "Payload too large": this body; a smaller one would land, and the Agent drops it too | [send logs][logs-api], [Agent retry guide][agent-retry] |
//! | `429` | `Ambiguous` | "Too Many Requests, request should be retried after some time", with `X-RateLimit-*` headers this sink doesn't read | [send logs][logs-api], [rate limits][rate-limits] |
//! | any `5xx`, `501` included | `Ambiguous` | "request should be retried after some time"; may have been applied | [send logs][logs-api] |
//! | any other `3xx` or `4xx` | `Rejected` | about this request; redirects are off ([`crate::http::build_client`]) | [Agent retry guide][agent-retry] |
//! | connect failure, before any request of this `send` was accepted | `Clean` | nothing left the process | -- |
//! | connect failure after one was | `Ambiguous` | Datadog holds part of the batch ([`crate::http::after_delivery`]) | -- |
//! | any other transport error, timeout included | `Ambiguous` | the request may have been applied | -- |
//!
//! `Clean` and `Refused` mean Datadog holds nothing of the batch, so once a request was accepted a
//! connect failure or a refusal on a later one is `Ambiguous` ([`crate::http::after_delivery`]):
//! a route answered `Refused` after another route was accepted retries the whole batch under
//! `at_least_once`, the accepted routes included, and drops it under `at_most_once`. That costs a
//! resend of the accepted routes on every retry, and is kept because a refusal on one route holds
//! for every batch: the routes share one org-wide key, so a `401` or `403` refuses them all, and a
//! `404` names a route the configured base will refuse next time too. Reading such a route as
//! `Rejected` instead would drop its records from every batch until the operator fixed the config,
//! the loss `Refused` exists to prevent. Under `at_least_once` a resent series point overwrites
//! and a resent log is stored again ("Delivery posture", below).
//!
//! [logs-api]: https://docs.datadoghq.com/api/latest/logs/#send-logs
//! [rate-limits]: https://docs.datadoghq.com/api/latest/rate-limits/
//! [agent-retry]: https://docs.datadoghq.com/agent/guide/agent-retry/
//! [agent-tx]: https://github.com/DataDog/datadog-agent/blob/main/comp/forwarder/defaultforwarder/transaction/transaction.go
//!
//! Redirects aren't followed ([`crate::http::build_client`] says why).
//!
//! **Delivery posture.** The default, `at_least_once` (`docs/adr/delivery-semantics.md`, item 5),
//! retries an `Ambiguous` attempt, and a batch spans several requests, so a retry re-sends the
//! ones that succeeded. A trial org was sent two resends: a resent series point was stored once,
//! the last write winning at its `(series, timestamp)`, and an identical log was stored twice.
//! The upstream `aggregate` `temporality: cumulative` remedy doesn't apply here: the series route
//! skips a cumulative `Sum`, since a Datadog `count` carries a per-interval value. Distribution
//! points, sketches, and APM stats have no remedy and are assumed to add on a resend, and events,
//! checks, and traces to be stored again, until measured. `buffer.delivery: at_most_once` drops
//! the batch instead.
//!
//! ## Telemetry
//!
//! | Point | Meaning |
//! |---|---|
//! | `logit.output.requests{route, class}` | one per request; `class` is [`crate::http::status_class`]'s, or `network_error` |
//! | `logit.output.request.duration{route}` | one timer per request |
//! | `logit.output.request.bytes{route}` | the body as sent, after compression, for a request that got an answer or failed after it may have left; not for a refused connection |
//! | `logit.output.records{route}` | entries in a request Datadog accepted |
//! | `logit.output.records.dropped{route, reason}` | `stale`, `oversize`, `needs_agent_processing`, `not_datadog_origin`, `rejected`, as above |
//!
//! Plus everything [`DatadogEncoder`] counts itself (`logit.output.metrics.skipped`, including
//! `unsupported_kind`-style skips by `metric_kind`; `metrics.degraded`; `tags.dropped`;
//! `spans.degraded`; `stats.*`), which this sink doesn't repeat.
//!
//! **Once per batch or per attempt** (ADR `sink-send-path-and-attempt-accounting`, decision 1).
//! What the plan and the encoder decide counts once per batch: `stale`, `needs_agent_processing`,
//! `not_datadog_origin`, an event too large to send alone, and the encoder's counters and
//! diagnostics, through handles gated by the sink's `BatchAccounting`. The plan is unit 0 and each
//! route its own unit, so a route an earlier attempt never reached counts on the attempt that
//! first encodes it. The transport counters, a `413`'s `oversize`, and a rejected request's
//! `rejected` count per attempt.

use crate::accounting::BatchAccounting;
use crate::http::classify_status;
use crate::http::{
    build_client, classify_reqwest_error, error_read_bytes, read_body_prefix, redacted_snippet,
    split_encode, status_class, Caps, Encoded, Outcomes,
};
/// `tls:`: the shared `crate::tls` type, re-exported as the other sinks do.
pub use crate::tls::TlsClientSettings;
use anyhow::Context;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use logit_core::{redact, Diagnostics, EventBatch, MetricKind, Telemetry};
use logit_pipeline::{BatchContext, Fault, Output, SeqId};
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

/// Where the sink reads its send time: [`now_nanos`], or a test's scripted clock.
type Clock = Box<dyn Fn() -> i64 + Send + Sync>;

/// [`plan`]'s result: each route's items, indexed by `Route as usize`, and what the stale filter
/// and the trace readiness gate dropped, which the sink counts once per batch.
#[derive(Debug, Default)]
struct Plan {
    routes: [Vec<Item>; 8],
    /// Records dropped `stale`, per route, indexed as `routes`.
    stale: [usize; 8],
    /// Spans the trace route drops, per [`TraceReadiness::drop_reason`].
    needs_agent_processing: usize,
    not_datadog_origin: usize,
}

/// Each route's items for `batch` (module doc's "Routes") at send time `now`, after the stale
/// filter and the trace readiness gate. Counts nothing: [`DatadogOutput::count_plan_drops`] counts
/// the drops, so the sink can skip them when a retry repeats the plan.
fn plan(batch: &EventBatch, now: i64) -> Plan {
    let resource = &batch.resource;
    let readiness = if batch.events.iter().any(|e| e.span.is_some()) {
        trace_readiness(batch)
    } else {
        Vec::new()
    };
    let mut plan = Plan::default();
    for (index, event) in batch.events.iter().enumerate() {
        let routes = &mut plan.routes;
        let mut push = |route: Route, weight: usize| {
            routes[route as usize].push(Item { index, weight });
        };
        if is_datadog_stats(resource, event) {
            push(Route::Stats, 1);
            continue;
        }
        let stale = &mut plan.stale;
        let age = now.saturating_sub(event.timestamp);
        let check = is_service_check(resource, event);
        if check {
            if age > CHECK_MAX_AGE {
                stale[Route::CheckRun as usize] += 1;
            } else {
                push(Route::CheckRun, 1);
            }
        }
        let datadog_event = is_datadog_event(resource, event);
        if datadog_event {
            if age > LOG_MAX_AGE {
                stale[Route::Events as usize] += 1;
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
                stale[route as usize] += n;
            } else {
                push(route, n);
            }
        }
        if event.log.is_some() && !datadog_event {
            if age > LOG_MAX_AGE {
                stale[Route::Logs as usize] += 1;
            } else {
                push(Route::Logs, 1);
            }
        }
        if event.span.is_some() {
            match readiness[index] {
                Some(TraceReadiness::Ready) => push(Route::Traces, 1),
                Some(TraceReadiness::NeedsAgentProcessing) => plan.needs_agent_processing += 1,
                Some(TraceReadiness::NotDatadogOrigin) => plan.not_datadog_origin += 1,
                None => unreachable!("trace_readiness gives every span event a verdict"),
            }
        }
    }
    plan
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
    /// Counts through views of `telemetry`/`diag` gated by `accounting`
    /// ([`DatadogOutput::new_encoder`]).
    encoder: DatadogEncoder,
    /// Ungated: the transport counters, the server's verdicts, and [`plan`]'s and the oversize
    /// drops, which [`DatadogOutput::attempt`] skips itself on a repeat encode.
    diag: Diagnostics,
    telemetry: Telemetry,
    accounting: BatchAccounting,
    /// The send time of the batch `observe_batch` last armed, read by every attempt at it so each
    /// reaches the same stale verdict; cleared by an `Ok`, replaced by the next `observe_batch`.
    /// `None` reads the clock per `send`.
    batch_now: Option<i64>,
    /// [`now_nanos`], or a test's scripted clock.
    clock: Clock,
    /// Replaces every route's [`Route::caps`], so a test can bisect a small body.
    #[cfg(test)]
    caps_override: Option<Caps>,
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
        let mut output = Self {
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
            accounting: BatchAccounting::default(),
            batch_now: None,
            clock: Box::new(now_nanos),
            #[cfg(test)]
            caps_override: None,
        };
        output.encoder = output.new_encoder();
        Ok(output)
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
        let cfg = logit_pipeline::tls::build_client_config(
            settings,
            base_dir,
            reloader,
            &self.diag,
            &self.telemetry,
        )?;
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

    /// The encoder, on views of this sink's handles gated by its batch accounting, so a retried
    /// batch counts the codec's drops once (`crate::accounting`). `new` and every builder that
    /// changes what the encoder holds call this, so no builder order leaves it ungated.
    fn new_encoder(&self) -> DatadogEncoder {
        let gate = self.accounting.gate();
        DatadogEncoder::new()
            .with_telemetry(self.telemetry.gated(gate))
            .with_diagnostics(self.diag.gated(gate))
    }

    /// Reads send times from `clock` instead of the wall clock.
    #[cfg(test)]
    fn with_clock(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.clock = Box::new(clock);
        self
    }

    /// Replaces every route's request limits with `caps`.
    #[cfg(test)]
    fn with_caps(mut self, caps: Caps) -> Self {
        self.caps_override = Some(caps);
        self
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

    /// Counts what [`plan`] dropped, per route and reason.
    fn count_plan_drops(&self, plan: &Plan) {
        for route in ROUTES {
            self.dropped(route, "stale", plan.stale[route as usize]);
        }
        self.dropped(Route::Traces, "needs_agent_processing", plan.needs_agent_processing);
        self.dropped(Route::Traces, "not_datadog_origin", plan.not_datadog_origin);
    }

    /// `route`'s request limits: [`Route::caps`], or a test's override.
    fn caps(&self, route: Route) -> Caps {
        #[cfg(test)]
        if let Some(caps) = self.caps_override {
            return caps;
        }
        route.caps()
    }

    /// One attempt at send time `now`: the plan, then each route's requests in turn. The plan is
    /// unit 0 of the batch accounting and route `r`'s `split_encode` unit `1 + r as u32`, each
    /// counting encode-side only on the batch's first encode of it. A route is encoded only once
    /// the routes before it were sent or rejected, so a route an earlier attempt never reached
    /// counts on the attempt that first encodes it.
    ///
    /// Each request's result is folded by [`Outcomes`]: a rejected request (counted by
    /// [`DatadogOutput::post`]) lets the attempt go on; a refused key, a `Clean` failure, or an
    /// `Ambiguous` one ends it. Once a request was accepted, a failure that would be `Clean` is
    /// `Ambiguous` ([`crate::http::after_delivery`]). That wraps `post`'s result rather than living
    /// in it, so `post`'s `requests{class}` and `request.bytes` follow the request's own fault: a
    /// refused connection counts no bytes whatever came before it.
    async fn attempt(&mut self, batch: &EventBatch, now: i64) -> anyhow::Result<()> {
        let (first, mut plan) = self.accounting.encode(0, || plan(batch, now));
        if first {
            self.count_plan_drops(&plan);
        }
        let gate = self.accounting.gate().clone();
        let mut outcomes = Outcomes::new();
        for route in ROUTES {
            let items = std::mem::take(&mut plan.routes[route as usize]);
            if items.is_empty() {
                continue;
            }
            let (body_encoding, caps) = (route.body_encoding(self.compression), self.caps(route));
            let encoder = &mut self.encoder;
            let (first, split) = self.accounting.encode(1 + route as u32, || {
                split_encode(
                    &items,
                    caps,
                    &gate,
                    |item| item.weight,
                    |chunk| {
                        let raw = route.encode(encoder, &sub_batch(batch, chunk))?;
                        Some(Encoded {
                            raw_len: raw.len(),
                            body: body_encoding.apply(raw),
                            meta: (),
                        })
                    },
                )
            });
            if first {
                for (item, raw_len, wire_len) in &split.oversize {
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
            }
            for (chunk, encoded) in split.requests {
                let entries = chunk.iter().map(|item| item.weight).sum();
                // `post` counts and diagnoses a rejection itself.
                outcomes.note(self.post(route, encoded, entries).await, |_| {})?;
            }
        }
        outcomes.finish()
    }

    /// One attempt at send time `now` ([`DatadogOutput::attempt`]). An `Ok` clears the batch's
    /// send time and disarms the batch accounting, a batch that sent nothing included.
    async fn send_at(&mut self, batch: &EventBatch, now: i64) -> anyhow::Result<()> {
        let result = self.attempt(batch, now).await;
        if result.is_ok() {
            self.batch_now = None;
        }
        self.accounting.finish(result)
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

    /// One request, one attempt (module doc's "Faults, retries, and duplicate safety").
    /// `request.bytes` counts a request that may have left: any answer, and any error but a
    /// [`Fault::Clean`] one, which never connected. A rejection counts the request's entries
    /// dropped here, `oversize` for a `413` and `rejected` for any other 3xx or 4xx that
    /// [`classify_status`] doesn't read as `Refused`.
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

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                let fault = classify_reqwest_error(&err);
                if fault != Fault::Clean {
                    self.telemetry.count(REQUEST_BYTES, wire_len as f64, &tags);
                }
                self.telemetry.count(
                    REQUESTS,
                    1.0,
                    &[("route", route.name()), ("class", "network_error")],
                );
                return Err(anyhow::Error::new(err)).context(fault);
            }
        };
        self.telemetry.count(REQUEST_BYTES, wire_len as f64, &tags);
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
        let url = redact::url(&url);
        // Bounded, and scrubbed of the key before it reaches a diagnostic or the error
        // ([`redacted_snippet`]).
        let key = self.api_key.to_str().unwrap_or_default();
        let body = read_body_prefix(response, error_read_bytes(key)).await;
        let snippet = redacted_snippet(&body, key);
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
                Fault::Refused
            }
            413 => {
                self.dropped(route, "oversize", entries);
                self.diag.warn_throttled(
                    "request_rejected",
                    format_args!(
                        "{url} answered 413, request too large, {entries} record(s) dropped: \
                         {snippet}"
                    ),
                );
                Fault::Rejected
            }
            _ => match classify_status(status) {
                Fault::Refused => {
                    self.diag.warn_throttled(
                        "request_refused",
                        format_args!("{url} answered {status}: {snippet}"),
                    );
                    Fault::Refused
                }
                other => {
                    self.dropped(route, "rejected", entries);
                    self.diag.warn_throttled(
                        "request_rejected",
                        format_args!(
                            "{url} answered {status}, {entries} record(s) dropped: {snippet}"
                        ),
                    );
                    other
                }
            },
        };
        let err = anyhow::anyhow!(
            "datadog_out: {} request to {url} failed ({status}): {snippet}",
            route.name()
        )
        .context(fault);
        Err(err)
    }
}

#[async_trait::async_trait]
impl Output for DatadogOutput {
    /// Arms this sink's batch accounting (`crate::accounting`) and fixes the batch's send time,
    /// so every attempt at it reaches the same stale verdict (module doc's "What is never sent").
    fn observe_batch(&mut self, _ctx: BatchContext, _seq: SeqId) {
        self.accounting.observe();
        self.batch_now = Some((self.clock)());
    }

    /// One or more requests per route the batch needs, sequentially; a rejected request is counted
    /// and the rest go on, and a refused key or a `Clean` or `Ambiguous` failure stops the send
    /// (module doc's "Faults, retries, and duplicate safety"). The send time is the one the last
    /// `observe_batch` fixed until an `Ok` clears it, else the clock's.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let now = self.batch_now.unwrap_or_else(|| (self.clock)());
        self.send_at(batch, now).await
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
        for status in [301, 400, 413] {
            assert_eq!(fault_for(status).await, Fault::Rejected, "{status}");
        }
        for status in [401, 403, 404] {
            assert_eq!(fault_for(status).await, Fault::Refused, "{status}");
        }
    }

    async fn fault_with(status: u16, body: &'static str) -> Fault {
        let (addr, _log) = intake(move |_| (status, body.into())).await;
        let err = sink(addr).send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        logit_pipeline::classify(&err)
    }

    // One test per row of the module doc's "Faults, retries, and duplicate safety" table.

    #[tokio::test]
    async fn a_400_bad_request_is_rejected() {
        let body = r#"{"errors":["Payload is not in the expected format"]}"#;
        assert_eq!(fault_with(400, body).await, Fault::Rejected);
    }

    #[tokio::test]
    async fn a_401_unauthorized_is_refused() {
        assert_eq!(fault_with(401, r#"{"errors":["Unauthorized"]}"#).await, Fault::Refused);
    }

    #[tokio::test]
    async fn a_403_forbidden_is_refused() {
        assert_eq!(fault_with(403, r#"{"errors":["Forbidden"]}"#).await, Fault::Refused);
    }

    #[tokio::test]
    async fn a_404_unserved_route_is_refused() {
        assert_eq!(fault_with(404, "404 page not found").await, Fault::Refused);
    }

    #[tokio::test]
    async fn a_405_or_407_is_refused() {
        for status in [405, 407] {
            assert_eq!(fault_with(status, "").await, Fault::Refused, "{status}");
        }
    }

    #[tokio::test]
    async fn a_408_request_timeout_is_ambiguous() {
        assert_eq!(fault_with(408, r#"{"errors":["Request Timeout"]}"#).await, Fault::Ambiguous);
    }

    #[tokio::test]
    async fn a_413_payload_too_large_is_rejected() {
        let body = r#"{"errors":["Request too large"]}"#;
        assert_eq!(fault_with(413, body).await, Fault::Rejected);
    }

    #[tokio::test]
    async fn a_429_too_many_requests_is_ambiguous() {
        let body = r#"{"errors":["Too many requests"]}"#;
        assert_eq!(fault_with(429, body).await, Fault::Ambiguous);
    }

    #[tokio::test]
    async fn any_5xx_is_ambiguous() {
        for status in [500, 501, 502, 503, 504] {
            assert_eq!(fault_with(status, "").await, Fault::Ambiguous, "{status}");
        }
    }

    #[tokio::test]
    async fn any_other_3xx_or_4xx_is_rejected() {
        for status in [302, 409, 415, 422] {
            assert_eq!(fault_with(status, "").await, Fault::Rejected, "{status}");
        }
    }

    /// A route refused after another was accepted retries the whole batch: `Ambiguous`, so
    /// `at_least_once` resends and `at_most_once` drops, never a `Clean`-style resend under both.
    #[tokio::test]
    async fn a_refused_route_after_an_accepted_route_is_ambiguous() {
        for status in [403, 404] {
            let (addr, log) = intake(move |path| {
                (if path == "/api/v2/series" { 202 } else { status }, String::new())
            })
            .await;
            let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
            let err = sink(addr).send_at(&b, NOW).await.unwrap_err();
            assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{status}: {err:#}");
            assert_eq!(paths(&log), ["/api/v2/series", "/api/v2/logs"], "{status}");
        }
    }

    /// A logs `202` is success, and an `Ambiguous` route aborts the ones after it: the retry
    /// resends the whole batch anyway.
    #[tokio::test]
    async fn an_ambiguous_route_aborts_the_rest_of_the_batch() {
        let (addr, log) =
            intake(|path| (if path == "/api/v2/series" { 500 } else { 202 }, String::new())).await;
        let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
        let err = sink(addr).send_at(&b, NOW).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
        assert_eq!(paths(&log), ["/api/v2/series"]);

        let (addr, log) = intake(|_| (202, String::new())).await;
        sink(addr).send_at(&b, NOW).await.expect("202 is success");
        assert_eq!(paths(&log), ["/api/v2/series", "/api/v2/logs"]);
    }

    /// A rejected route is counted and the routes after it are still sent; one accepted route
    /// makes the send `Ok`.
    #[tokio::test]
    async fn a_rejected_route_is_counted_and_the_rest_of_the_batch_is_sent() {
        let (addr, log) =
            intake(|path| (if path == "/api/v2/series" { 400 } else { 202 }, String::new())).await;
        let (registry, mut out) = metered(addr);
        let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
        out.send_at(&b, NOW).await.expect("the accepted logs route makes the send Ok");
        assert_eq!(paths(&log), ["/api/v2/series", "/api/v2/logs"]);
        let points = registry.drain(0);
        let rejected = [("route", "series"), ("reason", "rejected")];
        assert_eq!(total(&points, RECORDS_DROPPED, &rejected), 1.0);
        assert_eq!(total(&points, RECORDS, &[("route", "logs")]), 1.0);
        assert_eq!(total(&points, RECORDS, &[("route", "series")]), 0.0);
    }

    /// Every route rejected: each is counted, and the send fails `Rejected`.
    #[tokio::test]
    async fn every_route_rejected_fails_the_send_rejected() {
        let (addr, log) = intake(|_| (400, String::new())).await;
        let (registry, mut out) = metered(addr);
        let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
        let err = out.send_at(&b, NOW).await.unwrap_err();
        assert_eq!(err.downcast_ref::<Fault>(), Some(&Fault::Rejected), "{err:#}");
        assert!(format!("{err:#}").contains("series"), "the first rejection: {err:#}");
        assert_eq!(paths(&log), ["/api/v2/series", "/api/v2/logs"]);
        let points = registry.drain(0);
        for route in ["series", "logs"] {
            let rejected = [("route", route), ("reason", "rejected")];
            assert_eq!(total(&points, RECORDS_DROPPED, &rejected), 1.0, "{route}");
        }
    }

    /// A `403` refuses the key, which every route would refuse too: the send stops after one
    /// request, and nothing is counted rejected.
    #[tokio::test]
    async fn a_refused_key_stops_the_send_after_one_request() {
        let (addr, log) = intake(|_| (403, String::new())).await;
        let (registry, mut out) = metered(addr);
        let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
        let err = out.send_at(&b, NOW).await.unwrap_err();
        assert_eq!(err.downcast_ref::<Fault>(), Some(&Fault::Refused), "{err:#}");
        assert_eq!(paths(&log), ["/api/v2/series"]);
        assert_eq!(total(&registry.drain(0), RECORDS_DROPPED, &[]), 0.0);
    }

    /// A `413` counts `oversize` alone, never `rejected` as well.
    #[tokio::test]
    async fn a_413_is_not_also_counted_rejected() {
        let (addr, _log) = intake(|_| (413, String::new())).await;
        let (registry, mut out) = metered(addr);
        out.send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        let points = registry.drain(0);
        assert_eq!(total(&points, RECORDS_DROPPED, &[("reason", "rejected")]), 0.0);
        assert_eq!(total(&points, RECORDS_DROPPED, &[("reason", "oversize")]), 1.0);
    }

    #[tokio::test]
    async fn connect_refused_is_clean() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let err = sink(addr).send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    /// Once a route's request was accepted, a connect failure on the next is ambiguous: a
    /// `Clean` retry would resend the accepted series.
    #[tokio::test]
    async fn a_connect_failure_after_an_accepted_request_is_ambiguous() {
        let (addr, log) = crate::test_support::answers_once(202, Vec::new()).await;
        let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
        let err = sink(addr).send_at(&b, NOW).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous, "{err:#}");
        assert_eq!(crate::test_support::recorded_paths(&log.lock().unwrap()), ["/api/v2/series"]);
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
    /// fragment `redacted_snippet`'s whole-key match can't catch. Its remnant strip bounds what
    /// such a fragment can leak to fewer than 4 bytes.
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

    // ---- attempt accounting (ADR `sink-send-path-and-attempt-accounting`, decisions 2 and 3) --

    use crate::test_support::{
        assert_counted_once_per_batch, assert_direct_sends_count_after_an_empty_batch,
        at_least_once, at_most_once, bodies, fast_retry, http_recorder, per_path_recorder,
        recorded_paths, refused_addr, sum_of, sums_through_write_loop, Recorded, Reply, SumSeries,
        Sums,
    };
    use logit_core::{Sum, Temporality};
    use logit_pipeline::test_util::TelemetryProbe;
    use logit_pipeline::WriteLoopConfig;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const SERIES: &str = "/api/v2/series";
    const LOGS: &str = "/api/v2/logs";

    /// A non-monotonic delta sum, which the series codec skips and counts.
    fn non_monotonic_delta(ts: i64) -> Event {
        let sum = Sum { value: 1.0, temporality: Temporality::Delta, monotonic: false };
        metric(ts, MetricKind::Sum(sum))
    }

    /// A log with a `timestamp` attribute, which the logs codec drops and counts `reserved_key`.
    fn log_with_reserved_key(ts: i64, message: &str) -> Event {
        let mut event = log_event(ts, message);
        event.attributes.insert("timestamp", Value::str("t"));
        event
    }

    /// A sketch of 24 bins of 4e9 each: over a `Dogsketch`'s 2^20 k/n entries, so the sketches
    /// codec drops it, counts it, and diagnoses it.
    fn oversized_sketch(ts: i64) -> Event {
        let mut s = DdSketch::new();
        for i in 0..24 {
            s.add_count(f64::from(1u32 << i), 4.0e9);
        }
        metric(ts, MetricKind::Distribution(s))
    }

    /// Two of `plan`'s drops and a codec count on each of the series, sketches, and logs routes;
    /// the sketches route sends nothing, so the batch is one series and one logs request.
    fn encode_side_batch() -> EventBatch {
        batch(vec![
            gauge(NOW),
            non_monotonic_delta(NOW),
            gauge(NOW - 2 * HOUR),
            oversized_sketch(NOW),
            log_with_reserved_key(NOW, "kept"),
            span(3, 5, None, &[("http.route", Value::str("/"))]),
        ])
    }

    const ENCODE_SIDE: [SumSeries<'static>; 6] = [
        (RECORDS_DROPPED, &[("route", "series"), ("reason", "stale")]),
        (RECORDS_DROPPED, &[("route", "traces"), ("reason", "not_datadog_origin")]),
        ("logit.output.metrics.skipped", &[("metric_kind", "non_monotonic_delta_sum")]),
        ("logit.output.metrics.skipped", &[("reason", "oversized_sketch")]),
        ("logit.component.diagnostics", &[("key", "oversized_sketch")]),
        ("logit.output.tags.dropped", &[("reason", "reserved_key")]),
    ];

    /// Beyond `logit.output.requests`, what a retried batch counts once per attempt here.
    const PER_ATTEMPT: [SumSeries<'static>; 2] = [(REQUEST_BYTES, &[]), (RECORDS, &[])];

    /// What an accepting intake answers on `path`.
    fn accepted(path: &str) -> Reply {
        Reply::Answer(if path == LOGS { 202 } else { 200 }, b"{}".to_vec())
    }

    /// `503` on `path`'s first request, then [`accepted`].
    fn busy_once(path: &str, k: usize) -> Reply {
        if k == 0 {
            Reply::Answer(503, Vec::new())
        } else {
            accepted(path)
        }
    }

    /// A sink on `probe`'s handles, with uncompressed bodies and every send time [`NOW`].
    fn instrumented(addr: SocketAddr, probe: &TelemetryProbe) -> DatadogOutput {
        let telemetry = probe.telemetry("out", "datadog_out", "sink");
        sink(addr)
            .with_compression(DatadogCompression::None)
            .with_clock(|| NOW)
            .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry)
    }

    /// `batches` through the write loop under `config`, over the sink `build` makes, against a
    /// [`per_path_recorder`]; and the requests it received.
    async fn run_dd(
        script: impl Fn(&str, usize) -> Reply + Send + Sync + 'static,
        batches: Vec<EventBatch>,
        config: WriteLoopConfig,
        build: impl FnOnce(SocketAddr, &TelemetryProbe) -> DatadogOutput,
    ) -> (Sums, Vec<Recorded>) {
        let (addr, log) = per_path_recorder(script).await;
        let mut probe = TelemetryProbe::new();
        let mut output = build(addr, &probe);
        let sums =
            sums_through_write_loop(&mut output, &mut probe, "datadog_out", batches, config).await;
        let log = log.lock().unwrap().clone();
        (sums, log)
    }

    /// Every request to `path` sent the bytes of the single-attempt run's one request there.
    fn assert_resent_unchanged(single: &[Recorded], retried: &[Recorded], path: &str) {
        let first = &bodies(single, path)[0];
        for body in bodies(retried, path) {
            assert_eq!(&body, first, "{path}: a retry sends the first attempt's bytes");
        }
    }

    /// The logs route fails after the series route was sent: the retry re-sends both, and every
    /// route's encode-side counts, `plan`'s drops, and the codec diagnostic read as after one
    /// attempt, the logs route's included, which attempt 1 encoded before its request failed.
    #[tokio::test]
    async fn a_route_failing_after_another_was_sent_counts_every_routes_encode_side_once() {
        let batches = || vec![encode_side_batch()];
        let (single, one) =
            run_dd(|p, _| accepted(p), batches(), at_least_once(), instrumented).await;
        let script = |p: &str, k| if p == LOGS { busy_once(p, k) } else { accepted(p) };
        let (retried, log) = run_dd(script, batches(), at_least_once(), instrumented).await;

        assert_eq!(recorded_paths(&one), [SERIES, LOGS]);
        assert_eq!(recorded_paths(&log), [SERIES, LOGS, SERIES, LOGS], "two attempts");
        assert_eq!(sum_of(&retried, REQUESTS, &[("route", "series"), ("class", "2xx")]), 2.0);
        assert_eq!(sum_of(&retried, REQUESTS, &[("route", "logs"), ("class", "5xx")]), 1.0);
        assert_eq!(sum_of(&retried, REQUESTS, &[("route", "logs"), ("class", "2xx")]), 1.0);
        let series_records = sum_of(&single, RECORDS, &[("route", "series")]);
        assert_eq!(sum_of(&retried, RECORDS, &[("route", "series")]), 2.0 * series_records);
        assert_eq!(sum_of(&retried, RECORDS, &[("route", "logs")]), 1.0);
        let bytes = sum_of(&single, REQUEST_BYTES, &[("route", "series")]);
        assert_eq!(sum_of(&retried, REQUEST_BYTES, &[("route", "series")]), 2.0 * bytes);
        for path in [SERIES, LOGS] {
            assert_resent_unchanged(&one, &log, path);
        }
        assert_counted_once_per_batch(&single, &retried, &ENCODE_SIDE, &PER_ATTEMPT);
    }

    /// The series route fails first, so attempt 1 never encodes the sketches and logs routes:
    /// they count on attempt 2, their first encode, once.
    #[tokio::test]
    async fn routes_first_encoded_on_a_retry_count_their_encode_side_then() {
        let batches = || vec![encode_side_batch()];
        let (single, one) =
            run_dd(|p, _| accepted(p), batches(), at_least_once(), instrumented).await;
        let script = |p: &str, k| if p == SERIES { busy_once(p, k) } else { accepted(p) };
        let (retried, log) = run_dd(script, batches(), at_least_once(), instrumented).await;

        assert_eq!(recorded_paths(&log), [SERIES, SERIES, LOGS], "logs is reached on attempt 2");
        assert_eq!(sum_of(&retried, RECORDS, &[("route", "logs")]), 1.0);
        for path in [SERIES, LOGS] {
            assert_resent_unchanged(&one, &log, path);
        }
        assert_counted_once_per_batch(&single, &retried, &ENCODE_SIDE, &PER_ATTEMPT);
    }

    /// Caps small enough to bisect the logs of [`bisected_logs`]: two count-capped chunks of five
    /// and four, and in each, two logs fit a request, three don't, and the 2,000-byte log doesn't
    /// fit alone. The second chunk's encode follows the first chunk's bisection, so it runs with
    /// the gate as that bisection left it.
    const SMALL_CAPS: Caps = Caps { entries: 5, raw_bytes: 700, wire_bytes: usize::MAX };

    /// Eight 200-byte logs and one of 2,000 bytes in the middle, each with a `reserved_key`
    /// attribute the logs codec counts once per record it encodes.
    fn bisected_logs() -> EventBatch {
        let mut events: Vec<Event> = (0..8)
            .map(|i| log_with_reserved_key(NOW, &format!("{i}{}", "y".repeat(199))))
            .collect();
        events.insert(4, log_with_reserved_key(NOW, &"x".repeat(2_000)));
        batch(events)
    }

    /// Bisection re-encodes records a count-capped chunk already counted, and runs muted: each
    /// record's `reserved_key` drop counts once, on one attempt and on a retried one. The record
    /// dropped as oversize at the leaf counts `oversize` once, and its codec count too, from the
    /// count-capped chunk's encode: a record degraded by its codec and then dropped is reported
    /// under both counters.
    #[tokio::test]
    async fn bisection_counts_each_records_codec_counters_once_on_every_attempt() {
        let build = |addr, probe: &TelemetryProbe| instrumented(addr, probe).with_caps(SMALL_CAPS);
        let batches = || vec![bisected_logs()];
        let (single, one) = run_dd(|p, _| accepted(p), batches(), at_least_once(), build).await;
        let (retried, log) = run_dd(busy_once, batches(), at_least_once(), build).await;

        let requests = one.len();
        assert!(requests >= 4, "the nine logs bisect into several requests: {requests}");
        let sent: usize = one
            .iter()
            .map(|r| DatadogDecoder::new().decode_logs(&r.body, NOW).unwrap().events.len())
            .sum();
        assert_eq!(sent, 8, "every log but the oversize one, each once");
        assert_eq!(log.len(), requests + 1, "the first request failed, then all were resent");
        for sums in [&single, &retried] {
            assert_eq!(
                sum_of(sums, "logit.output.tags.dropped", &[("reason", "reserved_key")]),
                9.0
            );
            let oversize = [("route", "logs"), ("reason", "oversize")];
            assert_eq!(sum_of(sums, RECORDS_DROPPED, &oversize), 1.0);
            assert_eq!(sum_of(sums, "logit.component.diagnostics", &[("key", "oversize")]), 1.0);
        }
        let encode_side: [SumSeries<'static>; 3] = [
            ("logit.output.tags.dropped", &[("reason", "reserved_key")]),
            (RECORDS_DROPPED, &[("reason", "oversize")]),
            ("logit.component.diagnostics", &[("key", "oversize")]),
        ];
        assert_counted_once_per_batch(&single, &retried, &encode_side, &PER_ATTEMPT);
    }

    /// A scripted clock reading `t0` first and `t0 + 2 min` on every later read, and a count of
    /// its reads.
    fn stepping_clock(t0: i64) -> (impl Fn() -> i64 + Send + Sync + 'static, Arc<AtomicUsize>) {
        let reads = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&reads);
        let clock = move || {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                t0
            } else {
                t0 + 2 * MINUTE
            }
        };
        (clock, reads)
    }

    /// The gauges each series body sent, by timestamp.
    fn series_points(log: &[Recorded]) -> Vec<Vec<i64>> {
        bodies(log, SERIES)
            .iter()
            .map(|body| {
                let series = DatadogDecoder::new().decode_series_v2_protobuf(body, NOW).unwrap();
                series.events.iter().map(|e| e.timestamp).collect()
            })
            .collect()
    }

    /// A point 11 minutes ahead of the batch's send time is stale there, and fresh 2 minutes
    /// later. The send time is read once per batch, so every attempt drops it: it is never sent,
    /// and its `stale` drop counts once.
    #[tokio::test]
    async fn a_point_stale_at_the_batchs_send_time_is_dropped_on_every_attempt() {
        let (clock, reads) = stepping_clock(NOW);
        let ahead = NOW + 11 * MINUTE;
        let b = batch(vec![gauge(NOW), gauge(ahead)]);
        let build = |addr, probe: &TelemetryProbe| instrumented(addr, probe).with_clock(clock);
        let (sums, log) = run_dd(busy_once, vec![b], at_least_once(), build).await;

        assert_eq!(series_points(&log), [vec![NOW], vec![NOW]], "the ahead point is never sent");
        assert_eq!(
            sum_of(&sums, RECORDS_DROPPED, &[("route", "series"), ("reason", "stale")]),
            1.0
        );
        assert_eq!(reads.load(Ordering::SeqCst), 1, "one read for the batch");
    }

    /// An `Ok` clears the batch's send time: a later `send` with no `observe_batch` reads the
    /// clock again, so a point stale at the delivered batch's time and fresh at its own is sent.
    #[tokio::test]
    async fn a_direct_send_after_a_delivered_batch_reads_the_clock_again() {
        let (clock, reads) = stepping_clock(NOW);
        let (addr, log) = per_path_recorder(|p, _| accepted(p)).await;
        let mut probe = TelemetryProbe::new();
        let mut output = instrumented(addr, &probe).with_clock(clock);
        let batches = vec![batch(vec![gauge(NOW)])];
        sums_through_write_loop(&mut output, &mut probe, "datadog_out", batches, fast_retry())
            .await;
        let ahead = NOW + 11 * MINUTE;
        output.send(&batch(vec![gauge(ahead)])).await.unwrap();

        let log = log.lock().unwrap().clone();
        assert_eq!(series_points(&log), [vec![NOW], vec![ahead]]);
        assert_eq!(reads.load(Ordering::SeqCst), 2, "one read per batch");
    }

    /// Two batches through the write loop: the first's series request answers `first()` and the
    /// batch is dropped; the second holds one point 11 min ahead of `NOW`. [`stepping_clock`]
    /// reads `NOW` for the first batch and `NOW + 2 min` for the second. Against
    /// `METRIC_MAX_AHEAD` (10 min) the point is 11 min ahead of the first reading, stale, and 9
    /// min ahead of the second, fresh. `observe_batch` replaces the dropped batch's send time, so
    /// the point is sent, nothing is counted `stale`, and the clock is read once per batch.
    async fn assert_a_batch_after_a_dropped_one_reads_the_clock_again(
        first: fn() -> Reply,
        config: WriteLoopConfig,
    ) {
        let (clock, reads) = stepping_clock(NOW);
        let script = move |p: &str, k| if p == SERIES && k == 0 { first() } else { accepted(p) };
        let ahead = NOW + 11 * MINUTE;
        let batches = vec![batch(vec![gauge(NOW)]), batch(vec![gauge(ahead)])];
        let build = |addr, probe: &TelemetryProbe| instrumented(addr, probe).with_clock(clock);
        let (sums, log) = run_dd(script, batches, config, build).await;

        assert_eq!(sum_of(&sums, "logit.component.batches.dropped", &[]), 1.0);
        assert_eq!(sum_of(&sums, "logit.component.batches.delivered", &[]), 1.0);
        assert_eq!(series_points(&log), [vec![NOW], vec![ahead]], "the second point is sent");
        assert_eq!(sum_of(&sums, RECORDS_DROPPED, &[("reason", "stale")]), 0.0);
        assert_eq!(reads.load(Ordering::SeqCst), 2, "one read per batch");
    }

    /// The first batch's request is answered `503`, an ambiguous failure `at_most_once` drops.
    #[tokio::test]
    async fn a_batch_after_one_dropped_ambiguously_reads_the_clock_again() {
        let unavailable = || Reply::Answer(503, Vec::new());
        assert_a_batch_after_a_dropped_one_reads_the_clock_again(unavailable, at_most_once()).await;
    }

    /// The first batch's request is answered `400`, a rejection that drops it at once.
    #[tokio::test]
    async fn a_batch_after_one_rejected_reads_the_clock_again() {
        let rejected = || Reply::Answer(400, b"bad request".to_vec());
        assert_a_batch_after_a_dropped_one_reads_the_clock_again(rejected, fast_retry()).await;
    }

    /// Pins a documented corner (`docs/known-gaps/datadog.md`): after a batch whose last attempt
    /// failed, a `send` with no `observe_batch` reuses that batch's send time and finds the gate
    /// armed. The failed batch fixed `NOW`, so the direct send's point 11 min ahead is stale there
    /// and dropped (it would be fresh at the clock's next reading, `NOW + 2 min`), and its `stale`
    /// drop is muted, since the failed batch already encoded the plan's unit. Its `Ok` clears both,
    /// so the next direct send reads the clock and sends the same point.
    #[tokio::test]
    async fn a_direct_send_after_a_failed_batch_reuses_its_send_time_and_armed_gate() {
        let (clock, reads) = stepping_clock(NOW);
        let rejected_first = |p: &str, k| {
            if p == SERIES && k == 0 {
                Reply::Answer(400, Vec::new())
            } else {
                accepted(p)
            }
        };
        let (addr, log) = per_path_recorder(rejected_first).await;
        let mut probe = TelemetryProbe::new();
        let mut output = instrumented(addr, &probe).with_clock(clock);
        let batches = vec![batch(vec![gauge(NOW)])];
        let sums =
            sums_through_write_loop(&mut output, &mut probe, "datadog_out", batches, fast_retry())
                .await;
        assert_eq!(sum_of(&sums, "logit.component.batches.dropped", &[]), 1.0);

        let ahead = NOW + 11 * MINUTE;
        output.send(&batch(vec![gauge(ahead)])).await.unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 1, "the failed batch's time is reused");
        assert_eq!(series_points(&log.lock().unwrap()), [vec![NOW]], "the point is dropped");
        let sums: Sums =
            probe.poll().sums().map(|(n, t, v)| ((n.to_string(), t.to_vec()), v)).collect();
        assert_eq!(sum_of(&sums, RECORDS_DROPPED, &[("reason", "stale")]), 0.0, "and muted");

        output.send(&batch(vec![gauge(ahead)])).await.unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 2);
        assert_eq!(series_points(&log.lock().unwrap()), [vec![NOW], vec![ahead]]);
    }

    /// The gate re-arms per batch: a second batch counts as the first did.
    #[tokio::test]
    async fn a_second_datadog_batch_counts_its_encode_side_counters() {
        let batches = vec![encode_side_batch(), encode_side_batch()];
        let (sums, _) = run_dd(|p, _| accepted(p), batches, fast_retry(), instrumented).await;
        for (name, tags) in ENCODE_SIDE {
            assert_eq!(sum_of(&sums, name, tags), 2.0, "{name} {tags:?}");
        }
    }

    /// A batch whose logs request is answered `503` is dropped under `at_most_once`, and the next
    /// batch counts its encode-side counters.
    #[tokio::test]
    async fn a_datadog_batch_after_one_dropped_counts_encode_side() {
        let script = |p: &str, k| {
            if p == LOGS && k == 0 {
                Reply::Answer(503, Vec::new())
            } else {
                accepted(p)
            }
        };
        let batches = vec![encode_side_batch(), encode_side_batch()];
        let (sums, log) = run_dd(script, batches, at_most_once(), instrumented).await;
        assert_eq!(sum_of(&sums, "logit.component.batches.dropped", &[]), 1.0);
        assert_eq!(sum_of(&sums, "logit.component.batches.delivered", &[]), 1.0);
        assert_eq!(recorded_paths(&log), [SERIES, LOGS, SERIES, LOGS]);
        for (name, tags) in ENCODE_SIDE {
            assert_eq!(sum_of(&sums, name, tags), 2.0, "{name} {tags:?}");
        }
    }

    /// A batch whose every point is stale sends nothing and returns `Ok`, leaving the accounting
    /// disarmed, so later direct sends count.
    #[tokio::test]
    async fn datadog_direct_sends_after_a_batch_that_sent_nothing_count_every_time() {
        let (addr, log) = per_path_recorder(|p, _| accepted(p)).await;
        let mut probe = TelemetryProbe::new();
        let mut output = instrumented(addr, &probe);
        assert_direct_sends_count_after_an_empty_batch(
            &mut output,
            &mut probe,
            "datadog_out",
            batch(vec![gauge(NOW - 2 * HOUR)]),
            encode_side_batch,
            &ENCODE_SIDE,
        )
        .await;
        let log = log.lock().unwrap().clone();
        assert_eq!(recorded_paths(&log), [SERIES, LOGS, SERIES, LOGS], "the two direct sends");
    }

    /// One of the two builders that rebuild the encoder.
    #[derive(Clone, Copy, Debug)]
    enum Builder {
        Diagnostics,
        Telemetry,
    }

    /// Both orders of the two encoder-building builders, each after a first call with other
    /// handles, leave the encoder counting through gated views of the last handles.
    #[tokio::test]
    async fn every_datadog_builder_order_gates_the_encoder_on_the_final_handles() {
        use Builder::{Diagnostics as D, Telemetry as T};
        for order in [[D, T], [T, D]] {
            let decoy = Registry::new();
            let build = |addr: SocketAddr, probe: &TelemetryProbe| -> DatadogOutput {
                let other = decoy.telemetry_for("other", "datadog_out", "sink");
                let mut sink = sink(addr)
                    .with_compression(DatadogCompression::None)
                    .with_clock(|| NOW)
                    .with_telemetry(other.clone())
                    .with_diagnostics(Diagnostics::new("other").with_telemetry(other));
                let telemetry = probe.telemetry("out", "datadog_out", "sink");
                for builder in order {
                    sink = match builder {
                        Builder::Diagnostics => sink.with_diagnostics(
                            Diagnostics::new("out").with_telemetry(telemetry.clone()),
                        ),
                        Builder::Telemetry => sink.with_telemetry(telemetry.clone()),
                    };
                }
                sink
            };
            let batches = || vec![encode_side_batch()];
            let (single, _) = run_dd(|p, _| accepted(p), batches(), at_least_once(), build).await;
            let script = |p: &str, k| if p == LOGS { busy_once(p, k) } else { accepted(p) };
            let (retried, _) = run_dd(script, batches(), at_least_once(), build).await;
            assert_counted_once_per_batch(&single, &retried, &ENCODE_SIDE, &PER_ATTEMPT);
            let stale = decoy.drain(0).iter().map(|e| e.metrics.len()).sum::<usize>();
            assert_eq!(stale, 0, "{order:?}: nothing counts through a replaced handle");
        }
    }

    /// With no handle builders, the encoder's diagnostics share the sink's throttle and are gated
    /// too: a retried batch reports its codec diagnostic and its local oversize drop once.
    #[tokio::test]
    async fn a_datadog_sink_with_no_handle_builders_reports_each_encode_side_diagnostic_once() {
        let (addr, log) =
            per_path_recorder(|p, k| if p == LOGS { busy_once(p, k) } else { accepted(p) }).await;
        let mut probe = TelemetryProbe::new();
        let mut output = sink(addr)
            .with_compression(DatadogCompression::None)
            .with_clock(|| NOW)
            .with_caps(Caps { raw_bytes: 1_000, ..Caps::UNBOUNDED });
        let mut b = encode_side_batch();
        b.events.push(log_event(NOW, &"x".repeat(2_000)));
        sums_through_write_loop(&mut output, &mut probe, "datadog_out", vec![b], at_least_once())
            .await;
        assert_eq!(recorded_paths(&log.lock().unwrap()), [SERIES, LOGS, SERIES, LOGS]);
        assert_eq!(output.diag.occurrences("oversized_sketch"), 1);
        assert_eq!(output.diag.occurrences("oversize"), 1);
    }

    /// A `413` answered on a retry is the intake's verdict on that attempt: its entries are
    /// counted oversize and it is diagnosed, through the sink's ungated handles.
    #[tokio::test]
    async fn a_413_answered_on_a_retry_is_counted() {
        let script = |p: &str, k| match k {
            0 => Reply::Answer(503, Vec::new()),
            _ if p == SERIES => Reply::Answer(413, b"too large".to_vec()),
            _ => accepted(p),
        };
        let b = batch(vec![gauge(NOW), gauge(NOW)]);
        let (sums, log) = run_dd(script, vec![b], at_least_once(), instrumented).await;
        assert_eq!(recorded_paths(&log), [SERIES, SERIES]);
        assert_eq!(sum_of(&sums, "logit.component.batches.dropped", &[]), 1.0);
        let oversize = [("route", "series"), ("reason", "oversize")];
        assert_eq!(sum_of(&sums, RECORDS_DROPPED, &oversize), 2.0);
        let rejected = [("key", "request_rejected")];
        assert_eq!(sum_of(&sums, "logit.component.diagnostics", &rejected), 1.0);
    }

    /// The series route is rejected on both attempts and the logs route is busy on the first:
    /// each attempt's rejection is that attempt's verdict and counts again, the logs route, which
    /// attempt 1 reached past the rejection, counts its encode side once, and the batch is
    /// delivered on attempt 2.
    #[tokio::test]
    async fn a_rejection_answered_on_a_retry_is_counted_again() {
        let batches = || vec![encode_side_batch()];
        let (single, _) =
            run_dd(|p, _| accepted(p), batches(), at_least_once(), instrumented).await;
        let script = |p: &str, k| match p {
            SERIES => Reply::Answer(400, b"bad payload".to_vec()),
            _ => busy_once(p, k),
        };
        let (retried, log) = run_dd(script, batches(), at_least_once(), instrumented).await;

        assert_eq!(recorded_paths(&log), [SERIES, LOGS, SERIES, LOGS], "two attempts");
        assert_eq!(sum_of(&retried, "logit.component.batches.delivered", &[]), 1.0);
        let series_records = sum_of(&single, RECORDS, &[("route", "series")]);
        assert!(series_records > 0.0);
        let rejected = [("route", "series"), ("reason", "rejected")];
        assert_eq!(sum_of(&retried, RECORDS_DROPPED, &rejected), 2.0 * series_records);
        let diagnosed = [("key", "request_rejected")];
        assert_eq!(sum_of(&retried, "logit.component.diagnostics", &diagnosed), 2.0);
        let per_attempt = [
            PER_ATTEMPT[0],
            PER_ATTEMPT[1],
            (RECORDS_DROPPED, &[("reason", "rejected")][..]),
            ("logit.component.diagnostics", &[("key", "request_rejected")][..]),
        ];
        assert_counted_once_per_batch(&single, &retried, &ENCODE_SIDE, &per_attempt);
    }

    // ---- request.bytes -----------------------------------------------------------------------

    /// A refused connection sent nothing, so it counts no `request.bytes`, only its request.
    #[tokio::test]
    async fn a_refused_connection_counts_no_request_bytes() {
        let (registry, mut out) = metered(refused_addr().await);
        let err = out.send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
        let points = registry.drain(0);
        assert_eq!(total(&points, REQUEST_BYTES, &[]), 0.0);
        let refused = [("route", "series"), ("class", "network_error")];
        assert_eq!(total(&points, REQUESTS, &refused), 1.0);
    }

    /// A request that got an answer counts the body as sent, and so does one that timed out,
    /// which may have reached the intake.
    #[tokio::test]
    async fn an_answered_or_timed_out_request_counts_its_bytes() {
        let (addr, log) = accepting().await;
        let (registry, out) = metered(addr);
        let mut out = out.with_compression(DatadogCompression::None);
        out.send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap();
        let sent = log.lock().unwrap()[0].body.len() as f64;
        assert_eq!(total(&registry.drain(0), REQUEST_BYTES, &[("route", "series")]), sent);

        let (addr, _log) = http_recorder(|_, _, _| Reply::Hang).await;
        let (registry, out) = metered(addr);
        // The recorder never answers, so the timeout ends the request whatever its length: 100 ms
        // bounds only how long the test waits, and a loaded machine can't make it fire early.
        let mut out = out.with_timeout(Duration::from_millis(100));
        let err = out.send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        assert!(total(&registry.drain(0), REQUEST_BYTES, &[("route", "series")]) > 0.0);
    }
}
