//! `datadog_out`: sends a batch straight to Datadog's intake API, one `POST` per route the batch
//! needs. It mirrors `logit_inputs::datadog` (`datadog_in`), and the pair is a like-protocol relay
//! under [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md)
//! ([ADR `datadog-agent-and-intake-relay`](../../../docs/adr/datadog-agent-and-intake-relay.md),
//! decisions 2, 7, 9, and 10, and its "Verified destination behavior"). Every body comes from
//! [`DatadogEncoder`], whose module doc holds the mappings; this module owns HTTP: which events go to which route, what isn't sent at all, how a route's
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
//! concurrency: 1             # default; batches in flight, see "Concurrency"
//! headers: {}                # extra headers; `!env` works on a value
//! tls: {}                    # tunes every https:// request
//! histograms:                # how OTLP histograms are written, as Datadog's exporter's block
//!   mode: distributions      # default; or counters (`.bucket` counts) or nobuckets
//!   send_aggregation_metrics: false  # default; adds .count, .sum, .min, .max; nobuckets needs it
//! ```
//!
//! There are no per-sink host, service, source, or tag fields (ADR decision 10): the encoders read
//! them from each event's attributes and the batch resource, so an upstream `set` stamps them. A
//! series, sketch, log, event, or service check that carries no host gets the process hostname
//! ([`DatadogOutput::with_default_host`]). A trace or stats payload's hostname is its resource's
//! `host.name`. It gets the process hostname only when it has neither that nor a relaying
//! Agent's hostname: an Agent sends an empty one beside its own. Graph rule 66 validates the
//! block.
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
//! | metric records other than `Samples`, `Distribution`, and a delta histogram's sketch (a service check's record 0 excluded), and the `.bucket`, aggregation, and `.quantile` series a histogram or summary expands to | api `/api/v2/series` | [`DatadogEncoder::encode_series_v2_protobuf`] | `application/x-protobuf` |
//! | `Samples` | api `/api/v1/distribution_points` | [`DatadogEncoder::encode_distribution_points`] | `application/json` |
//! | `Distribution`, a delta `Histogram` under `histograms.mode: distributions`, and a delta `ExponentialHistogram` | api `/api/beta/sketches` | [`DatadogEncoder::encode_sketches`] | `application/x-protobuf` |
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
//! `logit.output.records.dropped{reason="stale"}`, per entry: on the series route, each series a
//! record would write and each record or point the encoder would skip or drop there (a cumulative
//! monotonic `Sum`, a summary's non-finite quantile); each distribution-points or sketch record;
//! each event on the other routes:
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
//! so a clock read per attempt could drop a point on one attempt and send it on the next. A final
//! result (`Ok`, or a fault `write_loop` won't retry under the sink's posture) clears it, and the
//! next `observe_batch` replaces it. A `send` with no `observe_batch` reads the clock itself only
//! when no earlier batch left a time behind: after a batch whose last attempt failed with a fault
//! `write_loop` would retry, it reuses that batch's time (`docs/known-gaps/datadog.md`).
//!
//! A retryable fault retries until the batch is delivered or shutdown cuts it, bounded by the
//! sink's `buffer:`. Staleness is judged against the batch's one send time, so a batch held
//! through a long outage can reach the intake with a point past its window.
//!
//! The series window is the documented one, and stricter than the intake, which stored older
//! points in a trial-org run. ADR `datadog-agent-and-intake-relay`'s "Timestamp windows and what
//! a `202` drops" has what the intake stored and how it treats a point too far ahead.
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
//! **A metric over Datadog's tag limit.** A series, distribution point, or sketch whose `tags`
//! list holds more than 100 strings is dropped by the intake, so this sink drops it first and
//! counts `records.dropped{reason="too_many_tags"}`, with a throttled `too_many_tags` diagnostic
//! naming the route and the first record's metric name and tag count. The count starts from the
//! list the encoder writes for every record of the event
//! (`logit_proto::datadog::series::metric_tag_count`): every rendered attribute, repeats included,
//! and not the host, device, or other carriers, which go in fields of their own. A trial org stored
//! 100 tags and dropped 101 on all three routes on 2026-10-08; `resources` didn't count. The series
//! route names such a drop in its `202` body (below), but the distribution-points and sketches
//! routes drop it silently, answering `202 {"status": "ok"}`, so only the sink can count it there.
//!
//! On the distribution-points and sketches routes the check is per event: every record carries
//! the event's list as it is, so one over the limit drops them all, one count per record. On the
//! series route it's per series, because a `.bucket` series adds its `lower_bound` and
//! `upper_bound` tags and a `.quantile` series its `quantile`
//! (`logit_proto::datadog::series::series_over_tag_limit`). Each series over the limit counts one,
//! the encoder leaves it off, and the event's other series still go out. An event whose own list
//! is over the limit has every series over it, so the per-event check isn't needed there. A record
//! the route writes nothing for (a cumulative monotonic `Sum`, say) has no series to count, and
//! the encoder counts its skip as usual.
//!
//! **Two points of one series in one second.** Datadog keeps one point per series per second:
//! of two in one request, the higher, for a `count` as for a gauge (a trial org, 2026-10-21), and
//! across requests the later write. So the plan finds, over the whole batch, the latest point of
//! each series and second, the later in batch order on a tie
//! (`logit_proto::datadog::series::SeriesDuplicates`; `logit_proto::datadog`'s encode table has
//! the series key), and the series encoder sends only that one. Each other point is counted once
//! per batch with the plan's drops: `logit.output.metrics.normalized{reason="duplicate_timestamp"}`
//! when it's a state equal to the point sent (`logit_proto::datadog::series`'s `same_point`: a
//! gauge or a cumulative kind, with the same value, wire type, unit, interval, source type, and
//! origin), since Datadog stores the same point, and otherwise
//! `logit.output.metrics.dropped{reason="duplicate_timestamp"}`, with a throttled
//! `duplicate_timestamp` diagnostic. Two equal delta counts or `rate`s (a `Gauge` flagged
//! `_logit.metrics.type: rate`) are two increments, so the one left off counts `dropped`. Each point left off comes out of its event's weight, so no request cut
//! or bisection splits a pair, and `logit.output.records` and
//! an `at_most_once` drop count only the points sent. Distribution points aren't collapsed:
//! Datadog keeps both. Two points of one series in one second in two batches are both sent, and
//! Datadog keeps the later, uncounted (`docs/known-gaps/datadog.md`). A `dedup_timestamps` with
//! `precision: 1s` as the last stage before this sink sums two counts or rates of one interval and
//! keeps the later gauge first.
//!
//! ## Size limits
//!
//! | Route | Entries per request | Uncompressed body | Body on the wire |
//! |---|---|---|---|
//! | series (series entries), distribution points and sketches (records; the series limits) | 10,000 | 5,242,880 B | 512,000 B |
//! | logs | 1,000 | 5,000,000 B | -- |
//! | events | 1 | -- | -- |
//! | traces | -- | 3,200,000 B | -- |
//! | service checks, stats | -- | -- | -- |
//!
//! A route's events are cut into requests by entry count first ([`split_encode`]; an event with
//! several records weighs as many entries, and on the series route a record weighs as many series
//! as the encoder writes for it: a histogram's `.bucket` and aggregation series, a summary's
//! finite quantiles, and none for a record it skips, from
//! `logit_proto::datadog::series::record_routes`, which shares the encoder's own count). The
//! delivered and drop counts below count the same entries. A request whose encoded body is over a byte limit is
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
//! **One `send`, several requests, retried per request.** A batch leaves as one or more requests
//! per route, a route cut into requests by [`split_encode`], sent in route order. Each request's
//! verdict stands on its own ([`Outcomes`]; `docs/adr/delivery-semantics.md`'s "Amendment:
//! per-request verdicts (2026-10-04)"):
//! - An accepted request is remembered for the batch, and a retry of the batch doesn't resend it.
//! - A `Rejected` request counts its entries dropped (below), is remembered too, and the send goes
//!   on to the next request.
//! - A refused key, a `Clean` failure, or an `Ambiguous` one stops the send with that request's
//!   own class, and the runtime retries or drops the batch on it: a retry sends only the requests
//!   not yet remembered.
//!
//! A send succeeds when any request of the batch was accepted, on this attempt or an earlier one,
//! and fails `Rejected` only when every request sent was rejected. A request is named by its place
//! in the attempt's send order, which is the same on every attempt because the plan and the cuts
//! depend only on the batch and the send time `observe_batch` fixed
//! ([`DatadogOutput::attempt`]). The memory lives beside the batch accounting
//! ([`crate::accounting::BatchAccounting::settle`]), one per batch in flight: `observe_batch`
//! clears it for each new batch, so it covers the retries of one batch and nothing else. A caller that never calls
//! `observe_batch` gets no memory, and each `send` sends every request. A disk-buffered batch
//! replayed after a restart is a new batch and sends every request again.
//!
//! The posture decides what a stop costs, and the accepted requests are never resent under either:
//! - `Refused` and `Clean` hold the batch under both postures, and each retry sends the
//!   remaining requests.
//! - `Ambiguous` under `at_least_once` retries the remaining requests; the failed one may arrive
//!   twice.
//! - `Ambiguous` under `at_most_once` drops the batch. The sink counts the entries of the failed
//!   request, of the rest of its route's unsent requests, and of every later route as
//!   `logit.output.records.dropped{route, reason="ambiguous_at_most_once"}`, from the posture the
//!   runtime hands it ([`Output::observe_posture`]); the runtime counts the batch. Every route is
//!   encoded before the first request, so a later route counts its requests' entries, not what
//!   its encode dropped and counted.
//!
//! **Concurrency.** `concurrency:` (default 1) bounds how many batches are in flight. At 1, `send`
//! makes each batch's requests in turn. Above 1, each batch's plan and requests are encoded on the
//! loop task, from the send time `observe_batch` fixed for that batch, and its requests run in
//! route order in its own task through [`crate::http::Slots`]; `write_loop` settles the batches
//! oldest first. A fault on one batch resends or drops that batch alone, under the table below,
//! and its resend skips the requests that batch already settled. Batches can land out of order: a
//! series point is the last to arrive per `(series, second)`, and the other routes store what
//! they receive as it arrives. Over TLS to a server that picks h2 through ALPN, the batches in
//! flight share one connection as concurrent streams; otherwise they open up to `concurrency`
//! connections. `logit.output.in_flight` reads the batches in flight, and
//! `logit.component.send.duration` times each batch's requests from the first send to the last
//! response.
//!
//! Datadog documents its intake statuses by HTTP code, the same on every route, with a free-text
//! body; no documented body code changes a status's meaning, so the status decides the class.
//! A series `2xx` body is read too, for what Datadog dropped from an accepted request: it's empty
//! for a clean request, and otherwise a protobuf whose field 1, `repeated string errors`, holds one
//! entry per series dropped ("Payload validation failed: series <metric> contains 1 data points
//! too far in the future", "... too many tags in series <metric>: limit=100 tags=101"), recorded
//! from a trial org on 2026-10-08. Each entry counts one record
//! `records.dropped{reason="rejected"}`, at most the request's entries, and comes out of
//! `records`, with a throttled `series_rejected` diagnostic
//! quoting the first entry. The verdict stays `Ok`: the rest of the request was stored, and a retry
//! would resend it. The read stops at 4 MiB, and an entry past it isn't counted. The
//! other routes' `2xx` bodies aren't read: distribution points and sketches answer
//! `{"status": "ok"}` whatever they drop, and what the check and logs routes drop from a `202` is
//! unmeasured (`docs/known-gaps/datadog.md`). `Rejected`
//! counts the request's entries `records.dropped{reason="rejected"}` (`oversize` for a `413`) with
//! a throttled `request_rejected` diagnostic; a `403` warns `api_key_rejected`, and any other
//! `Refused` warns `request_refused`. Each quotes the first 256 bytes of the body, scrubbed of the
//! key.
//!
//! | Response | Class | Why | Evidence |
//! |---|---|---|---|
//! | `2xx` (logs answer `202`) | `Ok` | accepted for processing; on the series route, each `errors` entry in the body counts one record `records.dropped{reason="rejected"}` | [send logs][logs-api]; a trial org's series `202` bodies, 2026-10-08 |
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
//! | connect failure | `Clean` | nothing of this request left the process; a retry skips the requests already accepted | -- |
//! | any other transport error, timeout included | `Ambiguous` | the request may have been applied | -- |
//!
//! A refusal holds for every batch: the routes share one org-wide key, so a `401` or `403` refuses
//! them all, and a `404` names a route the configured base will refuse next time too. So a route
//! answered `Refused` after another route was accepted stays `Refused` and holds the batch under
//! both postures, and each retry sends the refused route's requests alone. Reading such a route as
//! `Rejected` instead would drop its records from every batch until the operator fixed the config,
//! the loss `Refused` exists to prevent.
//!
//! [logs-api]: https://docs.datadoghq.com/api/latest/logs/#send-logs
//! [rate-limits]: https://docs.datadoghq.com/api/latest/rate-limits/
//! [agent-retry]: https://docs.datadoghq.com/agent/guide/agent-retry/
//! [agent-tx]: https://github.com/DataDog/datadog-agent/blob/main/comp/forwarder/defaultforwarder/transaction/transaction.go
//!
//! Redirects aren't followed ([`crate::http::build_client`] says why).
//!
//! **Delivery posture.** The default, `at_least_once` (`docs/adr/delivery-semantics.md`, item 5),
//! retries an `Ambiguous` attempt, which resends the request that drew the ambiguous answer and
//! may store it twice; a disk-buffered batch replayed after a restart resends every request. A
//! trial org was sent two resends: a resent series point was stored once, the last write winning
//! at its `(series, timestamp)`, and an identical log was stored twice. The upstream `aggregate`
//! `temporality: cumulative` remedy doesn't apply here: a Datadog `count` carries a per-interval
//! value, so the series route drops a cumulative monotonic `Sum`, and a `cumulative_to_delta`
//! upstream is what makes one sendable. Every series point,
//! a histogram's `.bucket` counts and a summary's quantiles included, overwrites. Distribution
//! points, sketches (a delta histogram's included), and APM stats have no remedy and are assumed
//! to add on a resend, and events, checks, and traces to be stored again, until measured. `buffer.delivery: at_most_once` drops the batch instead.
//!
//! ## Telemetry
//!
//! | Point | Meaning |
//! |---|---|
//! | `logit.output.requests{route, class}` | one per request; `class` is [`crate::http::status_class`]'s, or `network_error` |
//! | `logit.output.request.duration{route}` | one timer per request |
//! | `logit.output.request.bytes{route}` | the body as sent, after compression, for a request that got an answer or failed after it may have left; not for a refused connection |
//! | `logit.output.records{route}` | entries in a request Datadog accepted, less those its series `202` body names |
//! | `logit.output.records.dropped{route, reason}` | `stale`, `too_many_tags`, `oversize`, `needs_agent_processing`, `not_datadog_origin`, as above; `rejected`, a rejected request's entries or the series a `202` body names; `ambiguous_at_most_once`, the entries an at-most-once drop loses: the unsent requests' entries, every route's included |
//!
//! Plus everything [`DatadogEncoder`] counts itself (`logit.output.metrics.skipped` by
//! `metric_kind` or `reason`; `metrics.dropped`; `metrics.degraded`; `tags.omitted`;
//! `spans.degraded`; `stats.*`; and `events.skipped{reason="no_payload"}`, an event no route
//! carries, which the plan finds and counts through the encoder), which this sink doesn't repeat.
//!
//! **Once per batch or per attempt** (ADR `sink-send-path-and-attempt-accounting`, decision 1).
//! What the plan and the encoder decide counts once per batch: `stale`, `too_many_tags`,
//! `needs_agent_processing`, `not_datadog_origin`, an event too large to send alone, an event no
//! route carries (`events.skipped{reason="no_payload"}`), and the
//! encoder's counters and diagnostics, through handles gated by the sink's `BatchAccounting`. The
//! plan is unit 0 and each route its own unit, so a route an earlier attempt never reached counts
//! on the attempt that first encodes it. The transport counters, a series `202`'s `rejected`,
//! a `413`'s `oversize`, and a rejected request's `rejected` count per request sent: a settled request isn't
//! resent, so each counts once per batch for it, and a retry counts only the requests it sends.

use crate::accounting::BatchAccounting;
use crate::http::classify_status;
use crate::http::{
    build_client, classify_reqwest_error, error_read_bytes, read_body_bytes, read_body_prefix,
    read_varint, redacted_snippet, skip_field, split_encode, status_class, Caps, Encoded, Journal,
    Outcomes, Slots, DEFAULT_IDLE_TIMEOUT,
};
/// `tls:`: the shared `crate::tls` type, re-exported as the other sinks do.
pub use crate::tls::TlsClientSettings;
use anyhow::Context;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue};
use logit_core::interner::resolve;
use logit_core::{redact, Diagnostics, Event, EventBatch, Resource, Symbol, Telemetry};
use logit_pipeline::{BatchContext, DeliveryPosture, Fault, Output, SeqId};
use logit_proto::datadog::events::EventFormat;
use logit_proto::datadog::series::{
    metric_tag_count, record_routes, series_over_tag_limit, SeriesDuplicates, SeriesPlanned,
};
use logit_proto::datadog::{
    datadog_stats_name, is_datadog_event, is_datadog_stats, is_service_check, trace_readiness,
    DatadogEncoder, HistogramOptions, TraceReadiness, MAX_METRIC_TAGS,
};
use std::borrow::Cow;
use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
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

/// How much of a series `2xx` body to read for its `errors` (module doc's "Faults, retries, and
/// duplicate safety"): one entry per dropped series, each about 100 bytes plus the metric name, for
/// up to 10,000 series per request. Entries past it go uncounted.
const SERIES_ERRORS_READ_BYTES: usize = 4 * 1024 * 1024;

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

/// The entries `items` weigh on their route.
fn weight(items: &[Item]) -> usize {
    items.iter().map(|item| item.weight).sum()
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

/// [`plan`]'s result: each route's items, indexed by `Route as usize`, and what the stale filter,
/// the tag limit, and the trace readiness gate dropped, which the sink counts once per batch.
#[derive(Debug, Default)]
struct Plan {
    routes: [Vec<Item>; 8],
    /// Records dropped `stale`, per route, indexed as `routes`.
    stale: [usize; 8],
    /// Records dropped `too_many_tags`, per route, indexed as `routes`.
    too_many_tags: [usize; 8],
    /// Per route, the first such record's metric name and tag count, for the diagnostic.
    too_many_tags_first: [Option<(Symbol, usize)>; 8],
    /// Spans the trace route drops, per [`TraceReadiness::drop_reason`].
    needs_agent_processing: usize,
    not_datadog_origin: usize,
    /// The series points left off as duplicates of a later point of their series and second,
    /// planned over the whole batch so no request cut or bisection splits a pair.
    duplicates: Arc<SeriesDuplicates>,
    /// Events no route carries ([`Routing::unrouted`]).
    unrouted: usize,
}

/// Plans the series route's duplicates over the whole batch, after the stale filter and the tag
/// limit, and takes each event's left-off points out of its weight, so the request cuts and the
/// `records` count see only the points sent (module doc's "Two points of one series in one
/// second").
fn plan_duplicates(plan: &mut Plan, encoder: &DatadogEncoder, batch: &EventBatch) {
    let series = &mut plan.routes[Route::Series as usize];
    if series.len() < 2 && series.iter().all(|item| item.weight < 2) {
        return;
    }
    let events: Vec<usize> = series.iter().map(|item| item.index).collect();
    let duplicates = encoder.series_duplicates_in(batch, &events);
    for item in series.iter_mut() {
        item.weight = item.weight.saturating_sub(duplicates.of_event(item.index));
    }
    plan.duplicates = Arc::new(duplicates);
}

/// [`partition`]'s result: each route's items, indexed by `Route as usize`, and how many events
/// took no route, an event with no metric, log, or span.
struct Routing {
    routes: [Vec<Item>; 8],
    unrouted: usize,
}

/// Each route's items for `batch` (module doc's "Routes"), before any filter: the routing
/// alone, which [`plan`] filters and [`encode_routes`] encodes.
fn partition(batch: &EventBatch, histograms: HistogramOptions) -> Routing {
    let resource = &batch.resource;
    let mut routes: [Vec<Item>; 8] = Default::default();
    let mut unrouted = 0;
    let stats_name = datadog_stats_name();
    for (index, event) in batch.events.iter().enumerate() {
        let mut routed = false;
        let mut push = |route: Route, weight: usize| {
            routes[route as usize].push(Item { index, weight });
            routed = true;
        };
        if is_datadog_stats(resource, event, stats_name) {
            push(Route::Stats, 1);
            continue;
        }
        let check = is_service_check(resource, event);
        if check {
            push(Route::CheckRun, 1);
        }
        let datadog_event = is_datadog_event(resource, event);
        if datadog_event {
            push(Route::Events, 1);
        }
        // A service check's record 0 is the check route's alone. A record weighs as many series
        // entries as the encoder writes for it, a histogram's buckets and aggregations included.
        // The series route reads a record that writes none too, since it counts the skip.
        let (mut series, mut samples, mut sketches) = (0, 0, 0);
        let mut reads_series = false;
        for record in &event.metrics[usize::from(check)..] {
            let routes = record_routes(record, histograms);
            series += routes.series;
            reads_series |= routes.series_route;
            samples += usize::from(routes.distribution_points);
            sketches += usize::from(routes.sketch);
        }
        if reads_series {
            push(Route::Series, series);
        }
        for (route, n) in [(Route::DistributionPoints, samples), (Route::Sketches, sketches)] {
            if n > 0 {
                push(route, n);
            }
        }
        if event.log.is_some() && !datadog_event {
            push(Route::Logs, 1);
        }
        if event.span.is_some() {
            push(Route::Traces, 1);
        }
        unrouted += usize::from(!routed);
    }
    Routing { routes, unrouted }
}

/// Each route's items for `batch` at send time `now`: [`partition`]'s, after the stale filter,
/// the tag limit, and the trace readiness gate. Counts nothing:
/// [`DatadogOutput::count_plan_drops`] counts the drops, so the sink can skip them when a retry
/// repeats the plan.
fn plan(batch: &EventBatch, now: i64, histograms: HistogramOptions) -> Plan {
    let resource = &batch.resource;
    let readiness = if batch.events.iter().any(|e| e.span.is_some()) {
        trace_readiness(batch)
    } else {
        Vec::new()
    };
    let mut plan = Plan::default();
    let routing = partition(batch, histograms);
    plan.unrouted = routing.unrouted;
    for (route, items) in ROUTES.into_iter().zip(routing.routes) {
        let r = route as usize;
        for item in items {
            let event = &batch.events[item.index];
            let age = now.saturating_sub(event.timestamp);
            let kept = match route {
                Route::Stats => true,
                Route::CheckRun => age <= CHECK_MAX_AGE,
                Route::Events | Route::Logs => age <= LOG_MAX_AGE,
                Route::Series | Route::DistributionPoints | Route::Sketches => {
                    age <= METRIC_MAX_AGE && event.timestamp.saturating_sub(now) <= METRIC_MAX_AHEAD
                }
                Route::Traces => {
                    match readiness[item.index] {
                        Some(TraceReadiness::Ready) => plan.routes[r].push(item),
                        Some(TraceReadiness::NeedsAgentProcessing) => {
                            plan.needs_agent_processing += 1;
                        }
                        Some(TraceReadiness::NotDatadogOrigin) => plan.not_datadog_origin += 1,
                        None => unreachable!("trace_readiness gives every span event a verdict"),
                    }
                    continue;
                }
            };
            if !kept {
                plan.stale[r] += if route == Route::Series {
                    stale_series_entries(resource, event, histograms)
                } else {
                    item.weight
                };
                continue;
            }
            if route == Route::Series {
                // A series carries the event's tags and its own: a `.bucket` series its bounds and
                // a `.quantile` series its quantile. Each one over the limit is counted here and
                // left off by the encoder; the event's other series still go out.
                let base = metric_tag_count(resource, event);
                let check = usize::from(is_service_check(resource, event));
                let mut over = 0;
                for record in &event.metrics[check..] {
                    let (n, most) = series_over_tag_limit(record, histograms, base);
                    over += n;
                    let first = &mut plan.too_many_tags_first[r];
                    if n > 0 && first.is_none() {
                        *first = Some((record.name, most));
                    }
                }
                plan.too_many_tags[r] += over;
                plan.routes[r].push(Item { weight: item.weight - over, ..item });
                continue;
            }
            if matches!(route, Route::DistributionPoints | Route::Sketches) {
                // A distribution point and a sketch carry the event's `tags` list as it is.
                let tags = metric_tag_count(resource, event);
                if tags > MAX_METRIC_TAGS {
                    plan.too_many_tags[r] += item.weight;
                    let first = &mut plan.too_many_tags_first[r];
                    if first.is_none() {
                        let check = usize::from(is_service_check(resource, event));
                        let record = event.metrics[check..].iter().find(|r| {
                            let routes = record_routes(r, histograms);
                            match route {
                                Route::DistributionPoints => routes.distribution_points,
                                _ => routes.sketch,
                            }
                        });
                        *first = record.map(|r| (r.name, tags));
                    }
                    continue;
                }
            }
            plan.routes[r].push(item);
        }
    }
    plan
}

/// The entries a stale series item counts: what the encoder would have counted for a fresh copy,
/// each series a record writes and each skip or drop of the series route's own (a cumulative
/// monotonic `Sum`, a summary's non-finite quantile). A record only the sketches route writes
/// counts there, not here.
fn stale_series_entries(resource: &Resource, event: &Event, histograms: HistogramOptions) -> usize {
    let check = usize::from(is_service_check(resource, event));
    event.metrics[check..]
        .iter()
        .map(|record| record_routes(record, histograms))
        .map(|routes| routes.series + routes.series_skips)
        .sum()
}

/// One request [`encode_routes`] writes: the route's path, its `Content-Type`, and its body,
/// uncompressed.
#[derive(Debug, Clone)]
pub struct RouteRequest {
    pub path: &'static str,
    pub content_type: &'static str,
    pub body: Bytes,
}

/// The requests `batch` encodes to on each route, in route order: [`partition`]'s routing and
/// [`Route::encode`]'s bodies, one per route and one per event on the events route, with no
/// stale filter, tag limit, trace readiness gate, or size cut, which sit above the encoder. A
/// caller that needs the bytes this sink would send without its transport (the transit flow
/// driver) reads them here.
pub fn encode_routes(encoder: &mut DatadogEncoder, batch: &EventBatch) -> Vec<RouteRequest> {
    let mut out = Vec::new();
    let routing = partition(batch, encoder.histograms());
    encoder.events_without_payload(routing.unrouted);
    for (route, items) in ROUTES.into_iter().zip(routing.routes) {
        if items.is_empty() {
            continue;
        }
        let chunks: Vec<&[Item]> = match route {
            Route::Events => items.chunks(1).collect(),
            _ => vec![&items[..]],
        };
        for chunk in chunks {
            if let Some(body) = route.encode(encoder, &sub_batch(batch, chunk)) {
                out.push(RouteRequest {
                    path: route.path(),
                    content_type: route.content_type(),
                    body,
                });
            }
        }
    }
    out
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
    /// The operator's `idle_timeout:` ([`crate::http::build_client`]).
    idle_timeout: Duration,
    default_host: Option<Arc<str>>,
    histograms: HistogramOptions,
    /// Counts through views of `telemetry`/`diag` gated by `accounting`
    /// ([`DatadogOutput::new_encoder`]).
    encoder: DatadogEncoder,
    /// Ungated: the transport counters, the server's verdicts, and [`plan`]'s and the oversize
    /// drops, which [`DatadogOutput::attempt`] skips itself on a repeat encode.
    diag: Diagnostics,
    telemetry: Telemetry,
    accounting: BatchAccounting,
    /// The send time of the batch `observe_batch` last armed, read by every attempt at it so each
    /// reaches the same stale verdict; cleared by a final result
    /// ([`BatchAccounting::is_final`]), replaced by the next `observe_batch`.
    /// `None` reads the clock per `send`.
    batch_now: Option<i64>,
    /// [`now_nanos`], or a test's scripted clock.
    clock: Clock,
    /// Replaces every route's [`Route::caps`], so a test can bisect a small body.
    #[cfg(test)]
    caps_override: Option<Caps>,
    /// The batches in flight above a concurrency of 1, each with its own accounting over
    /// `accounting`'s gate and its own send time (the module doc's "Concurrency").
    slots: Slots<Attempted, Option<i64>>,
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
        let accounting = BatchAccounting::default();
        let mut output = Self {
            slots: Slots::new(accounting.gate().clone()),
            api_key,
            site: DEFAULT_SITE.to_string(),
            endpoints: DatadogEndpoints::default(),
            compression: DatadogCompression::default(),
            request_timeout: DEFAULT_TIMEOUT,
            client: build_client(DEFAULT_TIMEOUT, DEFAULT_IDLE_TIMEOUT, None),
            idle_timeout: DEFAULT_IDLE_TIMEOUT,
            headers: HeaderMap::new(),
            tls: None,
            default_host: None,
            histograms: HistogramOptions::default(),
            encoder: DatadogEncoder::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            accounting,
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

    /// The host of a series, sketch, log, event, or service check that carries none of its own,
    /// and of a trace or APM stats payload with no resource `host.name` and no relaying Agent's
    /// hostname: `logit run` passes the process hostname.
    pub fn with_default_host(mut self, host: impl Into<Arc<str>>) -> Self {
        self.default_host = Some(host.into());
        self.encoder = self.new_encoder();
        self
    }

    /// How a delta histogram is written (`histograms:`), as the Datadog exporter's setting of the
    /// same name.
    pub fn with_histograms(mut self, histograms: HistogramOptions) -> Self {
        self.histograms = histograms;
        self.encoder = self.new_encoder();
        self
    }

    pub fn with_compression(mut self, compression: DatadogCompression) -> Self {
        self.compression = compression;
        self
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
        self.client = build_client(self.request_timeout, idle_timeout, self.tls.as_ref());
        self
    }

    /// Per-request timeout (`timeout:`). The client is rebuilt so its default agrees with the
    /// per-request `.timeout(..)` that bounds each request.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self.client = build_client(timeout, self.idle_timeout, self.tls.as_ref());
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
        self.client = build_client(self.request_timeout, self.idle_timeout, Some(&cfg));
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
        self.slots.set_telemetry(telemetry.clone());
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
            .with_default_host(self.default_host.clone())
            .with_histograms(self.histograms)
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

    /// `route`'s request limits: [`Route::caps`], or a test's override.
    fn caps(&self, route: Route) -> Caps {
        #[cfg(test)]
        if let Some(caps) = self.caps_override {
            return caps;
        }
        route.caps()
    }

    /// What one attempt's requests need from this sink, owned so they can run in a task of their
    /// own.
    fn poster(&self) -> Poster {
        Poster {
            api_key: self.api_key.clone(),
            site: self.site.clone(),
            endpoints: self.endpoints.clone(),
            compression: self.compression,
            request_timeout: self.request_timeout,
            client: self.client.clone(),
            headers: self.headers.clone(),
            telemetry: self.telemetry.clone(),
        }
    }

    /// Every route's request limits, indexed by `Route as usize`.
    fn route_caps(&self) -> [Caps; 8] {
        ROUTES.map(|route| self.caps(route))
    }

    /// One attempt at send time `now`: [`prepare`], [`run`] inline, then the journal replayed,
    /// as a slot does above a concurrency of 1.
    async fn attempt(&mut self, batch: &EventBatch, now: i64) -> anyhow::Result<()> {
        let (poster, caps, compression) = (self.poster(), self.route_caps(), self.compression);
        let encoding = Encoding {
            caps,
            compression,
            encoder: &mut self.encoder,
            telemetry: &self.telemetry,
            diag: &mut self.diag,
        };
        match prepare(poster, encoding, &mut self.accounting, batch, now) {
            Some(attempt) => {
                let attempted = run(attempt).await;
                attempted.journal.replay(&mut self.diag, &mut self.accounting);
                attempted.result
            }
            None => Outcomes::new(self.accounting.any_accepted()).finish(),
        }
    }

    /// One attempt at send time `now` ([`DatadogOutput::attempt`]). A final result (`Ok`, or a
    /// fault `write_loop` won't retry) clears the batch's send time and disarms the batch
    /// accounting, a batch that sent nothing included.
    async fn send_at(&mut self, batch: &EventBatch, now: i64) -> anyhow::Result<()> {
        let result = self.attempt(batch, now).await;
        if self.accounting.is_final(&result) {
            self.batch_now = None;
        }
        self.accounting.finish(result)
    }

    /// `route`'s URL: the `endpoints` base for its intake, else `https://<prefix>.<site>`.
    #[cfg(test)]
    fn url(&self, route: Route) -> String {
        route_url(&self.site, &self.endpoints, route)
    }
}

/// `route`'s URL: the `endpoints` base for its intake, else `https://<prefix>.<site>`.
fn route_url(site: &str, endpoints: &DatadogEndpoints, route: Route) -> String {
    let base = match route.intake() {
        Intake::Api => &endpoints.api,
        Intake::Logs => &endpoints.logs,
        Intake::Traces => &endpoints.traces,
    };
    match base {
        Some(base) => format!("{}{}", base.trim_end_matches('/'), route.path()),
        None => format!("https://{}.{site}{}", route.intake().prefix(), route.path()),
    }
}

/// Counts `n` of `route`'s records dropped for `reason`, when there are any.
fn dropped(journal: &mut Journal, route: Route, reason: &'static str, n: usize) {
    if n > 0 {
        journal.count(RECORDS_DROPPED, n as f64, &[("route", route.name()), ("reason", reason)]);
    }
}

/// Whether `err` stops the send with a drop the sink counts `ambiguous_at_most_once`: an
/// `Ambiguous` fault under `at_most_once`, which `write_loop` doesn't retry.
fn drops_ambiguous(posture: DeliveryPosture, err: &anyhow::Error) -> bool {
    posture == DeliveryPosture::AtMostOnce && logit_pipeline::classify(err) == Fault::Ambiguous
}

/// One attempt's encode, on the loop task: the sink's encoder and ungated handles, and the
/// per-route limits.
struct Encoding<'a> {
    caps: [Caps; 8],
    compression: DatadogCompression,
    encoder: &'a mut DatadogEncoder,
    telemetry: &'a Telemetry,
    diag: &'a mut Diagnostics,
}

impl Encoding<'_> {
    fn dropped(&self, route: Route, reason: &'static str, n: usize) {
        if n > 0 {
            self.telemetry.count(
                RECORDS_DROPPED,
                n as f64,
                &[("route", route.name()), ("reason", reason)],
            );
        }
    }

    /// The batch's plan at send time `now`, unit 0 of `accounting`; what it dropped counts on the
    /// batch's first encode of it only.
    fn plan(&mut self, accounting: &mut BatchAccounting, batch: &EventBatch, now: i64) -> Plan {
        let histograms = self.encoder.histograms();
        let encoder = &*self.encoder;
        let (first, plan) = accounting.encode(0, || {
            let mut plan = plan(batch, now, histograms);
            plan_duplicates(&mut plan, encoder, batch);
            plan
        });
        if first {
            self.count_plan_drops(&plan);
        }
        plan
    }

    /// Counts what [`plan`] dropped, per route and reason, and the events it routed nowhere, and
    /// diagnoses the tag-limit drops.
    fn count_plan_drops(&mut self, plan: &Plan) {
        self.encoder.events_without_payload(plan.unrouted);
        for route in ROUTES {
            self.dropped(route, "stale", plan.stale[route as usize]);
            let n = plan.too_many_tags[route as usize];
            self.dropped(route, "too_many_tags", n);
            if let Some((name, tags)) = plan.too_many_tags_first[route as usize] {
                self.diag.warn_throttled(
                    "too_many_tags",
                    format_args!(
                        "dropped {n} record(s) on the {} route over Datadog's limit of \
                         {MAX_METRIC_TAGS} tags; the first, {}, carries {tags}",
                        route.name(),
                        resolve(name)
                    ),
                );
            }
        }
        self.dropped(Route::Traces, "needs_agent_processing", plan.needs_agent_processing);
        self.dropped(Route::Traces, "not_datadog_origin", plan.not_datadog_origin);
        self.encoder.count_duplicates(plan.duplicates.differing(), plan.duplicates.equal());
    }

    /// `route`'s requests for `items`, unit `1 + route as u32` of `accounting`: the oversize drops
    /// count on the batch's first encode of it only. Numbers each request from `next_request` and
    /// leaves out those the destination settled for this batch.
    fn route(
        &mut self,
        accounting: &mut BatchAccounting,
        route: Route,
        items: &[Item],
        batch: &EventBatch,
        duplicates: &Arc<SeriesDuplicates>,
        next_request: &mut u32,
    ) -> Vec<Posting> {
        let (body_encoding, caps) =
            (route.body_encoding(self.compression), self.caps[route as usize]);
        let gate = accounting.gate().clone();
        let encoder = &mut *self.encoder;
        let (first, split) = accounting.encode(1 + route as u32, || {
            split_encode(
                items,
                caps,
                &gate,
                |item| item.weight,
                |chunk| {
                    if route == Route::Series {
                        encoder.with_series_plan(Some(SeriesPlanned {
                            dropped: Arc::clone(duplicates),
                            origin: chunk.iter().map(|item| item.index).collect(),
                        }));
                    }
                    let raw = route.encode(encoder, &sub_batch(batch, chunk));
                    encoder.with_series_plan(None);
                    let raw = raw?;
                    Some(Encoded { raw_len: raw.len(), body: body_encoding.apply(raw), meta: () })
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
        let mut postings = Vec::with_capacity(split.requests.len());
        for (chunk, encoded) in split.requests {
            let request = *next_request;
            *next_request += 1;
            if !accounting.settled(request) {
                postings.push(Posting { request, route, entries: weight(&chunk), encoded });
            }
        }
        postings
    }
}

/// One request of an attempt: its number in the batch's send order, its route, the entries it
/// carries, and its body.
struct Posting {
    request: u32,
    route: Route,
    entries: usize,
    encoded: Encoded,
}

/// Everything a batch's requests need, cloned from the sink: the client is a handle on the
/// sink's own pool, and `telemetry` the ungated handle the attempt's [`Journal`] counts on.
struct Poster {
    api_key: HeaderValue,
    site: String,
    endpoints: DatadogEndpoints,
    compression: DatadogCompression,
    request_timeout: Duration,
    client: reqwest::Client,
    headers: HeaderMap,
    telemetry: Telemetry,
}

/// A stop [`post_all`] returns: the failed request's error and entries, and the requests after
/// it, unsent.
struct Stopped {
    err: anyhow::Error,
    route: Route,
    entries: usize,
    rest: Vec<Posting>,
}

/// Sends `postings` in order, each verdict folded into `outcomes`: an accepted or rejected request
/// is settled in `journal` and the next is sent; any other fault stops the run with the request's
/// own class.
async fn post_all(
    poster: &Poster,
    postings: Vec<Posting>,
    outcomes: &mut Outcomes,
    journal: &mut Journal,
) -> Result<(), Stopped> {
    let mut postings = postings.into_iter();
    while let Some(Posting { request, route, entries, encoded }) = postings.next() {
        // `post` counts and diagnoses a rejection itself.
        let result = poster.post(route, encoded, entries, journal).await;
        let accepted = result.is_ok();
        if let Err(err) = outcomes.note(result, |_| {}) {
            return Err(Stopped { err, route, entries, rest: postings.collect() });
        }
        journal.settle(request, accepted);
    }
    Ok(())
}

/// One attempt, every route encoded on the loop task before the first request ([`prepare`]).
struct SlotAttempt {
    poster: Poster,
    postings: Vec<Posting>,
    /// Whether an earlier attempt at the batch had a request accepted ([`Outcomes::new`]).
    accepted_before: bool,
    posture: DeliveryPosture,
}

/// What [`run`] did: the attempt's result and every count, warning, and settled request it
/// recorded, for [`DatadogOutput::await_ack`] to apply on the loop task.
struct Attempted {
    journal: Journal,
    result: anyhow::Result<()>,
}

/// The encode half of one attempt, on the loop task: the plan and every route's requests at send
/// time `now`, leaving out those the destination settled. The plan is unit 0 of `accounting` and
/// route `r`'s `split_encode` unit `1 + r as u32`, each counting encode-side only on the batch's
/// first encode of it. `None` when no request is left to send.
///
/// Requests are numbered `0, 1, 2, …` in send order across routes, one per `split_encode` chunk,
/// and a request the destination settled for this batch (accepted or rejected) is encoded again
/// but not resent. The number names the same request on every attempt only because the plan and
/// the cuts are pure functions of the batch and the send time `observe_batch` fixed: anything
/// that makes either depend on per-attempt state breaks the memory.
fn prepare(
    poster: Poster,
    mut encoding: Encoding<'_>,
    accounting: &mut BatchAccounting,
    batch: &EventBatch,
    now: i64,
) -> Option<SlotAttempt> {
    let mut plan = encoding.plan(accounting, batch, now);
    let duplicates = Arc::clone(&plan.duplicates);
    let mut postings = Vec::new();
    let mut next_request: u32 = 0;
    for route in ROUTES {
        let items = std::mem::take(&mut plan.routes[route as usize]);
        if !items.is_empty() {
            postings.extend(encoding.route(
                accounting,
                route,
                &items,
                batch,
                &duplicates,
                &mut next_request,
            ));
        }
    }
    if postings.is_empty() {
        return None;
    }
    Some(SlotAttempt {
        poster,
        postings,
        accepted_before: accounting.any_accepted(),
        posture: accounting.posture(),
    })
}

/// Every request of the attempt in order ([`post_all`]). Under `at_most_once` an `Ambiguous` stop
/// counts the failed request's entries and every unsent request's, per route,
/// `records.dropped{reason="ambiguous_at_most_once"}`: every route was encoded before the first
/// request, so each count is the entries its requests carry.
async fn run(attempt: SlotAttempt) -> Attempted {
    let SlotAttempt { poster, postings, accepted_before, posture } = attempt;
    let mut journal = Journal::new(poster.telemetry.clone());
    let mut outcomes = Outcomes::new(accepted_before);
    match post_all(&poster, postings, &mut outcomes, &mut journal).await {
        Ok(()) => Attempted { journal, result: outcomes.finish() },
        Err(stopped) => {
            if drops_ambiguous(posture, &stopped.err) {
                let mut lost = [0usize; 8];
                for posting in &stopped.rest {
                    lost[posting.route as usize] += posting.entries;
                }
                lost[stopped.route as usize] += stopped.entries;
                for later in ROUTES {
                    dropped(&mut journal, later, "ambiguous_at_most_once", lost[later as usize]);
                }
            }
            Attempted { journal, result: Err(stopped.err) }
        }
    }
}

impl Poster {
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

    /// One request, one attempt (module doc's "Faults, retries, and duplicate safety"), every
    /// count and warning recorded in `journal`. `request.bytes` counts a request that may have
    /// left: any answer, and any error but a [`Fault::Clean`] one, which never connected. A
    /// rejection counts the request's entries dropped here, `oversize` for a `413` and `rejected`
    /// for any other 3xx or 4xx that [`classify_status`] doesn't read as `Refused`. A series
    /// `2xx` counts the entries its body names `rejected` as well ([`series_errors`]), and takes
    /// them out of `records`.
    async fn post(
        &self,
        route: Route,
        encoded: Encoded,
        entries: usize,
        journal: &mut Journal,
    ) -> anyhow::Result<()> {
        let url = route_url(&self.site, &self.endpoints, route);
        let tags = [("route", route.name())];
        let wire_len = encoded.body.len();
        let started = std::time::Instant::now();
        let result = self
            .client
            .post(&url)
            .headers(self.request_headers(route))
            .timeout(self.request_timeout)
            .body(encoded.body)
            .send()
            .await;
        journal.timing(REQUEST_DURATION, started.elapsed(), &tags);

        let response = match result {
            Ok(response) => response,
            Err(err) => {
                let fault = classify_reqwest_error(&err);
                if fault != Fault::Clean {
                    journal.count(REQUEST_BYTES, wire_len as f64, &tags);
                }
                journal.count(
                    REQUESTS,
                    1.0,
                    &[("route", route.name()), ("class", "network_error")],
                );
                return Err(anyhow::Error::new(err)).context(fault);
            }
        };
        journal.count(REQUEST_BYTES, wire_len as f64, &tags);
        let status = response.status();
        journal.count(REQUESTS, 1.0, &[("route", route.name()), ("class", status_class(status))]);
        let key = self.api_key.to_str().unwrap_or_default();
        if status.is_success() {
            let rejected = if route == Route::Series {
                let body = read_body_bytes(response, SERIES_ERRORS_READ_BYTES).await;
                let errors = series_errors(&body);
                let rejected = errors.len().min(entries);
                if let Some(first) = errors.first() {
                    let first = redacted_snippet(&String::from_utf8_lossy(first), key);
                    dropped(journal, route, "rejected", rejected);
                    journal.warn(
                        "series_rejected",
                        format_args!(
                            "{} accepted the request but dropped {rejected} of {entries} \
                             series; the first: {first}",
                            redact::url(&url)
                        ),
                    );
                }
                rejected
            } else {
                0
            };
            journal.count(RECORDS, (entries - rejected) as f64, &tags);
            return Ok(());
        }
        let url = redact::url(&url);
        // Bounded, and scrubbed of the key before it reaches a diagnostic or the error
        // ([`redacted_snippet`]).
        let body = read_body_prefix(response, error_read_bytes(key)).await;
        let snippet = redacted_snippet(&body, key);
        let fault = match status.as_u16() {
            408 | 429 | 500..=599 => Fault::Ambiguous,
            403 => {
                journal.warn(
                    "api_key_rejected",
                    format_args!(
                        "Datadog refused the API key: {url} answered 403 -- check 'api_key', and \
                         that 'site' is the one the key belongs to"
                    ),
                );
                Fault::Refused
            }
            413 => {
                dropped(journal, route, "oversize", entries);
                journal.warn(
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
                    journal.warn(
                        "request_refused",
                        format_args!("{url} answered {status}: {snippet}"),
                    );
                    Fault::Refused
                }
                other => {
                    dropped(journal, route, "rejected", entries);
                    journal.warn(
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

/// The `errors` of a series `2xx` body: protobuf field 1, `repeated string`, one entry per series
/// the intake dropped. Not in the Agent's payload protos, so read by hand. An empty body is a clean
/// request; anything that stops parsing ends the list there, so garbage counts nothing, and an
/// entry the read limit cut short isn't counted.
fn series_errors(body: &[u8]) -> Vec<&[u8]> {
    let mut errors = Vec::new();
    let mut pos = 0;
    while pos < body.len() {
        let Some(key) = read_varint(body, &mut pos) else { break };
        let (field, wire_type) = (key >> 3, key & 0x7);
        if (field, wire_type) != (1, 2) {
            if skip_field(body, &mut pos, wire_type).is_none() {
                break;
            }
            continue;
        }
        let Some(entry) = read_varint(body, &mut pos)
            .and_then(|len| pos.checked_add(usize::try_from(len).ok()?))
            .and_then(|end| body.get(pos..end).map(|entry| (entry, end)))
        else {
            break;
        };
        errors.push(entry.0);
        pos = entry.1;
    }
    errors
}

#[async_trait::async_trait]
impl Output for DatadogOutput {
    /// Arms this sink's batch accounting (`crate::accounting`), which clears the requests the
    /// destination settled for the last batch, and fixes the batch's send time, so every attempt
    /// at it reaches the same stale verdict and cuts the same requests (module doc's "What is
    /// never sent"). Under a window above 1, both go to the batch's own slot.
    fn observe_batch(&mut self, _ctx: BatchContext, seq: SeqId) {
        let now = (self.clock)();
        if self.slots.window() > 1 {
            *self.slots.observe(seq) = Some(now);
        } else {
            self.accounting.observe();
            self.batch_now = Some(now);
        }
    }

    /// Records the posture, which decides whether an `Ambiguous` stop counts
    /// `ambiguous_at_most_once` and is final, disarming the batch accounting and clearing the
    /// batch's send time.
    fn observe_posture(&mut self, posture: DeliveryPosture) {
        self.accounting.observe_posture(posture);
        self.slots.observe_posture(posture);
    }

    /// One or more requests per route the batch needs, sequentially, skipping those the
    /// destination settled on an earlier attempt at the batch; a rejected request is counted and
    /// the rest go on, and a refused key or a `Clean` or `Ambiguous` failure stops the send
    /// (module doc's "Faults, retries, and duplicate safety"). The send time is the one the last
    /// `observe_batch` fixed until a final result clears it, else the clock's.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let now = self.batch_now.unwrap_or_else(|| (self.clock)());
        self.send_at(batch, now).await
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

    /// Plans and encodes every route of `batch` into its slot at the slot's send time, and
    /// starts its requests on a task of their own.
    async fn submit(
        &mut self,
        batch: &EventBatch,
        _ctx: BatchContext,
        seq: SeqId,
    ) -> anyhow::Result<()> {
        let (poster, caps, compression) = (self.poster(), self.route_caps(), self.compression);
        let clock = &self.clock;
        let encoding = Encoding {
            caps,
            compression,
            encoder: &mut self.encoder,
            telemetry: &self.telemetry,
            diag: &mut self.diag,
        };
        self.slots.submit_with(seq, |accounting, batch_now| {
            let now = *batch_now.get_or_insert_with(clock);
            Ok(prepare(poster, encoding, accounting, batch, now).map(run))
        })
    }

    /// The oldest batch's answer, counted and classified as `send` does.
    async fn await_ack(&mut self) -> anyhow::Result<()> {
        let diag = &mut self.diag;
        self.slots
            .ack_with(|attempted, accounting| {
                attempted.journal.replay(diag, accounting);
                attempted.result
            })
            .await
    }

    /// Aborts every request still in flight: the write loop has stopped.
    async fn flush(&mut self) -> anyhow::Result<()> {
        self.slots.cancel();
        Ok(())
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
        AttrMap, BodyFormat, DdSketch, Event, LogRecord, MetricKind, MetricRecord, Registry,
        Resource, Samples, SpanKind, SpanRecord, SpanStatus, Value,
    };
    use logit_proto::datadog::events::ATTR_EVENT_TITLE;
    use logit_proto::datadog::service_checks::{
        ATTR_SERVICE_CHECK_NAME, ATTR_SERVICE_CHECK_STATUS,
    };
    use logit_proto::datadog::stats::{ATTR_BUCKET_DURATION, ATTR_STATS_NAME, METRIC_HITS};
    use logit_proto::datadog::{DatadogDecoder, ATTR_RESOURCE_NAME, METRIC_TOP_LEVEL};
    use std::io::Read;
    use std::net::SocketAddr;
    use std::sync::Mutex;

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
                        sum += s.value.as_f64();
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
        metric(ts, MetricKind::gauge(1.5))
    }

    fn sketch(ts: i64) -> Event {
        let mut s = DdSketch::new();
        s.add(1.0);
        s.add(20.0);
        metric(ts, MetricKind::Distribution(s))
    }

    /// A delta histogram of three buckets and a sum.
    fn histogram(ts: i64) -> Event {
        metric(
            ts,
            MetricKind::Histogram(logit_core::Histogram {
                buckets: vec![(1.0, 2), (10.0, 5), (f64::INFINITY, 1)],
                temporality: Temporality::Delta,
                sum: Some(30.0),
                min: None,
                max: None,
            }),
        )
    }

    /// A histogram weighs what it writes on each route: its sketch alone under `distributions`,
    /// and under `counters` a series entry per bucket plus `.count` and `.sum` (it has no min or
    /// max), which the entries cap, the `records` count, and a `202` body's series errors count
    /// against.
    #[test]
    fn a_histogram_weighs_the_series_and_sketches_it_writes() {
        let weights = |options| {
            let routes = partition(&batch(vec![histogram(NOW)]), options).routes;
            (weight(&routes[Route::Series as usize]), weight(&routes[Route::Sketches as usize]))
        };
        assert_eq!(weights(HistogramOptions::default()), (0, 1));
        let counters = HistogramOptions {
            mode: logit_proto::datadog::HistogramMode::Counters,
            send_aggregation_metrics: true,
        };
        assert_eq!(weights(counters), (3 + 2, 0));
    }

    /// A stale series item counts its entries: each series a record writes, and one for a record
    /// that writes none, which never reaches the encoder to count its skip.
    #[test]
    fn a_stale_record_that_writes_no_series_still_counts() {
        let counters = HistogramOptions {
            mode: logit_proto::datadog::HistogramMode::Counters,
            send_aggregation_metrics: true,
        };
        let stale = NOW - 2 * HOUR;
        let sum_only = plan(&batch(vec![cumulative_sum(stale)]), NOW, HistogramOptions::default());
        assert_eq!(sum_only.stale[Route::Series as usize], 1);
        let mut h = histogram(stale);
        h.metrics.push(MetricRecord::new(
            intern("c"),
            MetricKind::Sum(Sum {
                value: logit_core::MetricValue::F64(1.0),
                temporality: Temporality::Cumulative,
                monotonic: true,
            }),
        ));
        let mixed = plan(&batch(vec![h]), NOW, counters);
        assert_eq!(mixed.stale[Route::Series as usize], 3 + 2 + 1);
    }

    /// Under the default `histograms:`, a stale delta histogram or exponential histogram counts on
    /// the sketches route alone, as a fresh one is written there alone; a stale summary counts each
    /// quantile, its non-finite one too, which a fresh copy counts dropped.
    #[test]
    fn a_stale_record_counts_once_on_the_route_that_writes_it() {
        let stale = NOW - 2 * HOUR;
        let exp = MetricKind::ExponentialHistogram(logit_core::ExpHistogram {
            scale: 0,
            zero_count: 0,
            zero_threshold: 0.0,
            positive: (0, vec![1, 2]),
            negative: (0, vec![]),
            temporality: Temporality::Delta,
            count: 3,
            sum: Some(5.0),
            min: None,
            max: None,
        });
        for event in [histogram(stale), metric(stale, exp)] {
            let p = plan(&batch(vec![event]), NOW, HistogramOptions::default());
            assert_eq!(p.stale[Route::Series as usize], 0);
            assert_eq!(p.stale[Route::Sketches as usize], 1);
        }
        let summary = |temporality| {
            MetricKind::Summary(logit_core::Summary {
                quantiles: vec![(0.5, 1.0), (0.9, f64::NAN)],
                count: 2,
                sum: 3.0,
                temporality,
            })
        };
        let cumulative = summary(logit_core::Temporality::Cumulative);
        let p = plan(&batch(vec![metric(stale, cumulative)]), NOW, HistogramOptions::default());
        assert_eq!(p.stale[Route::Series as usize], 2);
        // A delta summary's `.count` and `.sum` are two more series.
        let delta = summary(logit_core::Temporality::Delta);
        let p = plan(&batch(vec![metric(stale, delta)]), NOW, HistogramOptions::default());
        assert_eq!(p.stale[Route::Series as usize], 4);
    }

    /// A `.bucket` series carries its two bounds after the event's tags, so on an event with 99
    /// of them each bucket is over the limit: counted `too_many_tags` one per series, while the
    /// record's `.count` and `.sum` keep their place in the request.
    #[test]
    fn a_series_its_bounds_take_past_the_tag_limit_is_counted_alone() {
        let counters = HistogramOptions {
            mode: logit_proto::datadog::HistogramMode::Counters,
            send_aggregation_metrics: true,
        };
        let kind = histogram(NOW).metrics[0].kind.clone();
        let plan = plan(&batch(vec![tagged("h", kind, MAX_METRIC_TAGS - 1)]), NOW, counters);
        let r = Route::Series as usize;
        assert_eq!(plan.too_many_tags[r], 3);
        assert_eq!(weight(&plan.routes[r]), 2);
        assert_eq!(plan.too_many_tags_first[r], Some((intern("h"), MAX_METRIC_TAGS + 1)));
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
        Event::metric(ts, attrs, MetricRecord::new(intern("app.ok"), MetricKind::gauge(0.0)))
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

    // ---- the tag limit -----------------------------------------------------------------------

    /// A record named `name` of `kind` whose event carries `tags` distinct tags and a host, which
    /// goes in a wire field of its own rather than `tags`.
    fn tagged(name: &str, kind: MetricKind, tags: usize) -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert(logit_proto::datadog::ATTR_HOST_NAME, Value::str("h"));
        for i in 0..tags {
            attrs.insert(format!("k{i}").as_str(), Value::str("v"));
        }
        Event::metric(NOW, attrs, MetricRecord::new(intern(name), kind))
    }

    /// On each metrics route, a record with 100 tags is sent and one with 101 isn't, counted
    /// `too_many_tags` and diagnosed by its metric name, never a tag.
    #[tokio::test]
    async fn a_record_over_the_tag_limit_is_dropped_on_each_metrics_route() {
        let mut s = DdSketch::new();
        s.add(1.0);
        let routes: [(&str, &str, MetricKind); 3] = [
            ("series", "/api/v2/series", MetricKind::gauge(1.0)),
            (
                "distribution_points",
                "/api/v1/distribution_points",
                MetricKind::Samples(Samples::new([1.0])),
            ),
            ("sketches", "/api/beta/sketches", MetricKind::Distribution(s)),
        ];
        for (route, path, kind) in routes {
            let (addr, log) = accepting().await;
            let registry = Registry::new();
            let telemetry = registry.telemetry_for("out", "datadog_out", "sink");
            let mut out = sink(addr)
                .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry);
            let b = batch(vec![
                tagged("keep", kind.clone(), MAX_METRIC_TAGS),
                tagged("drop", kind.clone(), MAX_METRIC_TAGS + 1),
                tagged("keep2", kind, 3),
            ]);
            out.send_at(&b, NOW).await.unwrap();

            let captured = log.lock().unwrap().clone();
            assert_eq!(paths(&log), [path], "{route}");
            let body = captured[0].decoded();
            let mut decoder = DatadogDecoder::new();
            let sent = match route {
                "series" => decoder.decode_series_v2_protobuf(&body, NOW),
                "distribution_points" => decoder.decode_distribution_points(&body, NOW),
                _ => decoder.decode_sketches(&body, NOW),
            }
            .unwrap();
            let names: Vec<_> =
                sent.events.iter().map(|e| resolve(e.metrics[0].name).to_string()).collect();
            assert_eq!(names, ["keep", "keep2"], "{route}");

            let points = registry.drain(0);
            let tags = [("route", route), ("reason", "too_many_tags")];
            assert_eq!(total(&points, RECORDS_DROPPED, &tags), 1.0, "{route}");
            assert_eq!(total(&points, RECORDS, &[("route", route)]), 2.0, "{route}");
            let diag = [("key", "too_many_tags")];
            assert_eq!(total(&points, "logit.component.diagnostics", &diag), 1.0, "{route}");
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

    /// A route refused after another was accepted keeps its own class, `Refused`, so the batch
    /// holds under both postures, and each retry sends only the refused route.
    #[tokio::test]
    async fn a_refused_route_after_an_accepted_route_is_refused_and_retries_only_itself() {
        for status in [403, 404] {
            let (addr, log) = intake(move |path| {
                (if path == "/api/v2/series" { 202 } else { status }, String::new())
            })
            .await;
            let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
            let mut out = sink(addr).with_clock(|| NOW);
            out.observe_batch(BatchContext::default(), SeqId { id: [0; 16], seq: 1 });
            for _ in 0..2 {
                let err = out.send(&b).await.unwrap_err();
                assert_eq!(logit_pipeline::classify(&err), Fault::Refused, "{status}: {err:#}");
            }
            assert_eq!(
                paths(&log),
                ["/api/v2/series", "/api/v2/logs", "/api/v2/logs"],
                "{status}: the retry sends the refused route alone"
            );
        }
    }

    /// A logs `202` is success, and an `Ambiguous` route aborts the ones after it: the retry
    /// sends them.
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

    /// A connect failure after an accepted request keeps its own class, `Clean`: the retry
    /// resends only the logs request, so the accepted series isn't sent twice. Both attempts'
    /// errors name the logs URL, which only a retry that skipped the series request reaches.
    #[tokio::test]
    async fn a_connect_failure_after_an_accepted_request_is_clean_and_retries_only_itself() {
        let (addr, log) = crate::test_support::answers_once(202, Vec::new()).await;
        let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
        let mut out = sink(addr).with_clock(|| NOW);
        out.observe_batch(BatchContext::default(), SeqId { id: [0; 16], seq: 1 });
        for attempt in 0..2 {
            let err = out.send(&b).await.unwrap_err();
            assert_eq!(logit_pipeline::classify(&err), Fault::Clean, "{attempt}: {err:#}");
            assert!(format!("{err:#}").contains("/api/v2/logs"), "{attempt}: {err:#}");
        }
        assert_eq!(crate::test_support::recorded_paths(&log.lock().unwrap()), ["/api/v2/series"]);
    }

    /// A 413 counts the request's entries oversize.
    #[tokio::test]
    async fn a_413_counts_the_requests_entries_oversize() {
        let (addr, _log) = intake(|_| (413, String::new())).await;
        let (registry, mut out) = metered(addr);
        out.send_at(&batch(vec![gauge(NOW), gauge(NOW - 1_000_000_000)]), NOW).await.unwrap_err();
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

    // ---- a series 202's errors ---------------------------------------------------------------

    /// A series `202` body naming two series over the tag limit, recorded from a Datadog trial org
    /// on 2026-10-08 (`/api/v2/series` over protobuf, `Content-Type: application/x-protobuf`).
    /// Field 1, length `0x6e`, twice; the metric names are the recording's own.
    const SERIES_202_TWO_ERRORS: &[u8] = b"\
        \x0a\x6ePayload validation failed: too many tags in series \
        logit.w0202.1791476884.tags101_two.bad1: limit=100 tags=101\
        \x0a\x6ePayload validation failed: too many tags in series \
        logit.w0202.1791476884.tags101_two.bad2: limit=100 tags=102";

    /// The same run's body for one series with points too far in the future.
    const SERIES_202_FUTURE: &[u8] = b"\
        \x0a\x6ePayload validation failed: series logit.w0202.1791476884.future15 contains 1 \
        data points too far in the future";

    /// `records` and `records.dropped{reason="rejected"}` on the series route after sending
    /// `gauges` gauges against an intake answering every series request `202` with `body`.
    async fn series_202(gauges: usize, body: &'static [u8]) -> (f64, f64, Vec<Event>) {
        let (addr, _log) = per_path_recorder(move |_, _| Reply::Answer(202, body.to_vec())).await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("out", "datadog_out", "sink");
        let mut out = sink(addr)
            .with_diagnostics(Diagnostics::new("out").with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry);
        // A second apart, so no two are one series in one second.
        let b = batch((0..gauges as i64).map(|i| gauge(NOW - i * 1_000_000_000)).collect());
        out.send_at(&b, NOW).await.expect("a 202 is accepted whatever its body names");
        let points = registry.drain(0);
        let tags = [("route", "series")];
        let rejected =
            total(&points, RECORDS_DROPPED, &[("route", "series"), ("reason", "rejected")]);
        // Counted once, under `rejected` alone.
        assert_eq!(total(&points, RECORDS_DROPPED, &[]), rejected);
        (total(&points, RECORDS, &tags), rejected, points)
    }

    /// Each entry of a series `202`'s `errors` is one series Datadog dropped: counted
    /// `records.dropped{reason="rejected"}` and taken out of `records`, with the first entry
    /// quoted once.
    #[tokio::test]
    async fn a_series_202_naming_dropped_series_counts_them_rejected() {
        let (records, rejected, points) = series_202(3, SERIES_202_TWO_ERRORS).await;
        assert_eq!((records, rejected), (1.0, 2.0));
        let diag = [("key", "series_rejected")];
        assert_eq!(total(&points, "logit.component.diagnostics", &diag), 1.0);

        let (records, rejected, _) = series_202(3, SERIES_202_FUTURE).await;
        assert_eq!((records, rejected), (2.0, 1.0));
    }

    /// An empty body is a clean request, and a body that isn't the `errors` message counts
    /// nothing: every series counts as stored.
    #[tokio::test]
    async fn a_series_202_with_no_errors_counts_every_series() {
        for body in [&b""[..], b"{\"status\":\"ok\"}", b"\xff\xff\xff", b"\x0a\x6ePayload"] {
            let (records, rejected, points) = series_202(3, body).await;
            assert_eq!((records, rejected), (3.0, 0.0), "{body:?}");
            let diag = [("key", "series_rejected")];
            assert_eq!(total(&points, "logit.component.diagnostics", &diag), 0.0, "{body:?}");
        }
    }

    /// More entries than the request held series count no more than it held.
    #[tokio::test]
    async fn a_series_202_rejects_no_more_than_the_request_sent() {
        let (records, rejected, _) = series_202(1, SERIES_202_TWO_ERRORS).await;
        assert_eq!((records, rejected), (0.0, 1.0));
    }

    /// Unknown fields are skipped, so an entry after one still counts.
    #[test]
    fn series_errors_skips_unknown_fields() {
        let mut body = vec![0x10, 0x05, 0x1a, 0x02, b'h', b'i', 0x25, 0, 0, 0, 0];
        body.extend_from_slice(SERIES_202_FUTURE);
        assert_eq!(series_errors(&body).len(), 1);
        assert_eq!(series_errors(SERIES_202_TWO_ERRORS).len(), 2);
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
    ///
    /// A split fragment reaches the snippet only when the body opens with whitespace: the snippet
    /// is cut from the trimmed body, so trimming shifts the read limit's cut inside it. Without
    /// that, a key the read splits starts past the snippet's 256 bytes.
    #[tokio::test]
    async fn a_key_split_by_the_read_limit_leaks_no_more_than_a_few_bytes() {
        // `read_body_prefix` reads 4 bytes past `error_read_bytes`, so the read keeps the key's
        // first 7 bytes, and the trimmed body (filler plus fragment) fits in the snippet whole.
        let indent = " ".repeat(KEY.len() + 8);
        let filler = "x".repeat(error_read_bytes(KEY) + 4 - 7 - indent.len());
        let body = format!("{indent}{filler}{KEY}");

        let (addr, _log) = intake(move |_| (500, body.clone())).await;
        let err = sink(addr).send_at(&batch(vec![gauge(NOW)]), NOW).await.unwrap_err();
        // The message also names the request URL, whose ephemeral port can hold a run of KEY's
        // digits (`34563` holds `3456`), so only the response snippet after it is checked.
        let message = format!("{err:#}");
        let (_, snippet) = message.split_once("): ").expect("a response snippet");
        assert!(snippet.starts_with(&filler), "{message}");
        assert!(!contains_key_run_longer_than(snippet, KEY, 3), "{message}");
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

    /// A cumulative monotonic sum, which the series codec drops and counts.
    fn cumulative_sum(ts: i64) -> Event {
        let sum = Sum {
            value: logit_core::MetricValue::F64(1.0),
            temporality: Temporality::Cumulative,
            monotonic: true,
        };
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

    /// Three of `plan`'s drops and a codec count on each of the series, sketches, and logs routes;
    /// the sketches route sends nothing, so the batch is one series and one logs request.
    fn encode_side_batch() -> EventBatch {
        batch(vec![
            gauge(NOW),
            tagged("wide", MetricKind::gauge(1.0), MAX_METRIC_TAGS + 1),
            cumulative_sum(NOW),
            gauge(NOW - 2 * HOUR),
            oversized_sketch(NOW),
            log_with_reserved_key(NOW, "kept"),
            span(3, 5, None, &[("http.route", Value::str("/"))]),
            map_valued_gauge(),
            Event::empty(NOW, AttrMap::new()),
        ])
    }

    /// A gauge with a map-valued attribute, which has no Datadog tag form.
    fn map_valued_gauge() -> Event {
        let mut attrs = AttrMap::new();
        attrs.insert("nested", Value::Map(Box::new(AttrMap::new())));
        Event::metric(NOW, attrs, MetricRecord::new(intern("flags"), MetricKind::gauge(1.0)))
    }

    const ENCODE_SIDE: [SumSeries<'static>; 10] = [
        (RECORDS_DROPPED, &[("route", "series"), ("reason", "stale")]),
        (RECORDS_DROPPED, &[("route", "series"), ("reason", "too_many_tags")]),
        ("logit.component.diagnostics", &[("key", "too_many_tags")]),
        (RECORDS_DROPPED, &[("route", "traces"), ("reason", "not_datadog_origin")]),
        ("logit.output.metrics.dropped", &[("reason", "unresolved_cumulative")]),
        ("logit.output.metrics.dropped", &[("reason", "oversized_sketch")]),
        ("logit.component.diagnostics", &[("key", "oversized_sketch")]),
        ("logit.output.tags.omitted", &[("reason", "reserved_key")]),
        ("logit.output.tags.omitted", &[("reason", "unrepresentable")]),
        ("logit.output.events.skipped", &[("reason", "no_payload")]),
    ];

    /// A `samples` record at rate 0.8, which rounds to weight 1, counts `sample_rate_rounded`
    /// once per batch however many attempts its route takes, and one at rate 1 or 0.5 counts
    /// nothing under that reason.
    #[tokio::test]
    async fn a_rate_rounded_to_weight_one_counts_once_per_batch() {
        const DISTRIBUTION: &str = "/api/v1/distribution_points";
        let at = |rate: f64| {
            let mut samples = Samples::new([1.0, 2.0]);
            samples.sample_rate = rate;
            metric(NOW, MetricKind::Samples(samples))
        };
        let batches = || vec![batch(vec![at(0.8), at(1.0), at(0.5)])];
        let (single, _) =
            run_dd(|p, _| accepted(p), batches(), at_least_once(), instrumented).await;
        let (retried, log) = run_dd(busy_once, batches(), at_least_once(), instrumented).await;
        assert_eq!(recorded_paths(&log), [DISTRIBUTION, DISTRIBUTION]);
        let rounded = [("reason", "sample_rate_rounded")];
        assert_eq!(sum_of(&single, "logit.output.metrics.degraded", &rounded), 1.0);
        let encode_side: [SumSeries<'static>; 2] = [
            ("logit.output.metrics.degraded", &[("reason", "sample_rate_rounded")]),
            ("logit.output.metrics.degraded", &[("reason", "sample_rate_expanded")]),
        ];
        assert_counted_once_per_batch(&single, &retried, &encode_side, &PER_ATTEMPT);
    }

    /// A delta summary, written as quantile gauges and `count` points, counts
    /// `metrics.degraded{metric_kind="summary"}` once per batch however many attempts it takes.
    #[tokio::test]
    async fn a_delta_summary_counts_its_lesser_form_once_per_batch() {
        let summary = || {
            metric(
                NOW,
                MetricKind::Summary(logit_core::Summary {
                    quantiles: vec![(0.5, 1.0)],
                    count: 2,
                    sum: 3.0,
                    temporality: Temporality::Delta,
                }),
            )
        };
        let batches = || vec![batch(vec![summary()])];
        let (single, _) =
            run_dd(|p, _| accepted(p), batches(), at_least_once(), instrumented).await;
        let (retried, log) = run_dd(busy_once, batches(), at_least_once(), instrumented).await;
        assert_eq!(recorded_paths(&log), [SERIES, SERIES]);
        let kind = [("metric_kind", "summary")];
        assert_eq!(sum_of(&single, "logit.output.metrics.degraded", &kind), 1.0);
        let encode_side: [SumSeries<'static>; 1] =
            [("logit.output.metrics.degraded", &[("metric_kind", "summary")])];
        assert_counted_once_per_batch(&single, &retried, &encode_side, &PER_ATTEMPT);
    }

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

    /// The logs route fails after the series route was accepted: the retry sends the logs request
    /// alone, so the series route's requests, records, and bytes read as after one attempt, and
    /// nothing is counted dropped. Every route's encode-side counts, `plan`'s drops, and the codec
    /// diagnostic read as after one attempt too, the logs route's included, which attempt 1
    /// encoded before its request failed.
    #[tokio::test]
    async fn a_route_failing_after_another_was_accepted_resends_only_itself() {
        let batches = || vec![encode_side_batch()];
        let (single, one) =
            run_dd(|p, _| accepted(p), batches(), at_least_once(), instrumented).await;
        let script = |p: &str, k| if p == LOGS { busy_once(p, k) } else { accepted(p) };
        let (retried, log) = run_dd(script, batches(), at_least_once(), instrumented).await;

        assert_eq!(recorded_paths(&one), [SERIES, LOGS]);
        assert_eq!(recorded_paths(&log), [SERIES, LOGS, LOGS], "the retry sends logs alone");
        assert_eq!(sum_of(&retried, REQUESTS, &[("route", "series"), ("class", "2xx")]), 1.0);
        assert_eq!(sum_of(&retried, REQUESTS, &[("route", "logs"), ("class", "5xx")]), 1.0);
        assert_eq!(sum_of(&retried, REQUESTS, &[("route", "logs"), ("class", "2xx")]), 1.0);
        let series_records = sum_of(&single, RECORDS, &[("route", "series")]);
        assert!(series_records > 0.0);
        assert_eq!(sum_of(&retried, RECORDS, &[("route", "series")]), series_records);
        assert_eq!(sum_of(&retried, RECORDS, &[("route", "logs")]), 1.0);
        let bytes = sum_of(&single, REQUEST_BYTES, &[("route", "series")]);
        assert_eq!(sum_of(&retried, REQUEST_BYTES, &[("route", "series")]), bytes);
        let ambiguous = [("reason", "ambiguous_at_most_once")];
        assert_eq!(sum_of(&retried, RECORDS_DROPPED, &ambiguous), 0.0);
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
                sum_of(sums, "logit.output.tags.omitted", &[("reason", "reserved_key")]),
                9.0
            );
            let oversize = [("route", "logs"), ("reason", "oversize")];
            assert_eq!(sum_of(sums, RECORDS_DROPPED, &oversize), 1.0);
            assert_eq!(sum_of(sums, "logit.component.diagnostics", &[("key", "oversize")]), 1.0);
        }
        let encode_side: [SumSeries<'static>; 3] = [
            ("logit.output.tags.omitted", &[("reason", "reserved_key")]),
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

    /// A rejected batch is final, so a `send` after it with no `observe_batch` reads the clock
    /// and counts live: the point 11 min past `NOW` is fresh at the clock's second reading,
    /// `NOW + 2 min`, and the stale point beside it counts its `stale` drop.
    #[tokio::test]
    async fn a_direct_send_after_a_rejected_batch_reads_the_clock_and_counts() {
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
        output.send(&batch(vec![gauge(ahead), gauge(NOW - 2 * HOUR)])).await.unwrap();
        assert_eq!(reads.load(Ordering::SeqCst), 2, "the direct send reads the clock");
        assert_eq!(series_points(&log.lock().unwrap()), [vec![NOW], vec![ahead]]);
        let sums: Sums =
            probe.poll().sums().map(|(n, t, v)| ((n.to_string(), t.to_vec()), v)).collect();
        assert_eq!(sum_of(&sums, RECORDS_DROPPED, &[("reason", "stale")]), 1.0, "and counts");
    }

    /// Pins a documented corner (`docs/known-gaps/datadog.md`): after a batch whose last attempt
    /// failed with a fault `write_loop` would retry, a `send` with no `observe_batch` reuses that
    /// batch's send time and finds the gate armed. The failed batch fixed `NOW`, so the direct
    /// send's point 11 min ahead is stale there and dropped (it would be fresh at the clock's next
    /// reading, `NOW + 2 min`), and its `stale` drop is muted, since the failed batch already
    /// encoded the plan's unit. Its `Ok` clears both, so the next direct send reads the clock and
    /// sends the same point.
    #[tokio::test]
    async fn a_direct_send_after_a_retryable_failure_reuses_its_send_time_and_armed_gate() {
        let (clock, reads) = stepping_clock(NOW);
        let busy_first = |p: &str, k| {
            if p == SERIES && k == 0 {
                Reply::Answer(503, Vec::new())
            } else {
                accepted(p)
            }
        };
        let (addr, log) = per_path_recorder(busy_first).await;
        let mut probe = TelemetryProbe::new();
        let mut output = instrumented(addr, &probe).with_clock(clock);
        output.observe_batch(BatchContext::default(), SeqId { id: [0; 16], seq: 1 });
        let err = output.send(&batch(vec![gauge(NOW)])).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);

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
        assert_eq!(recorded_paths(&log.lock().unwrap()), [SERIES, LOGS, LOGS]);
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
        let b = batch(vec![gauge(NOW), gauge(NOW - 1_000_000_000)]);
        let (sums, log) = run_dd(script, vec![b], at_least_once(), instrumented).await;
        assert_eq!(recorded_paths(&log), [SERIES, SERIES]);
        assert_eq!(sum_of(&sums, "logit.component.batches.dropped", &[]), 1.0);
        let oversize = [("route", "series"), ("reason", "oversize")];
        assert_eq!(sum_of(&sums, RECORDS_DROPPED, &oversize), 2.0);
        let rejected = [("key", "request_rejected")];
        assert_eq!(sum_of(&sums, "logit.component.diagnostics", &rejected), 1.0);
    }

    /// The series route is rejected and the logs route is busy on the first attempt: the
    /// rejected request is remembered, so the retry sends logs alone and the rejection counts and
    /// diagnoses once. The logs route, which attempt 1 reached past the rejection, counts its
    /// encode side once, and the batch is delivered on attempt 2.
    #[tokio::test]
    async fn a_rejected_request_is_not_resent_and_counts_once() {
        let batches = || vec![encode_side_batch()];
        let (single, _) =
            run_dd(|p, _| accepted(p), batches(), at_least_once(), instrumented).await;
        let script = |p: &str, k| match p {
            SERIES => Reply::Answer(400, b"bad payload".to_vec()),
            _ => busy_once(p, k),
        };
        let (retried, log) = run_dd(script, batches(), at_least_once(), instrumented).await;

        assert_eq!(recorded_paths(&log), [SERIES, LOGS, LOGS], "the retry sends logs alone");
        assert_eq!(sum_of(&retried, "logit.component.batches.delivered", &[]), 1.0);
        let series_records = sum_of(&single, RECORDS, &[("route", "series")]);
        assert!(series_records > 0.0);
        let rejected = [("route", "series"), ("reason", "rejected")];
        assert_eq!(sum_of(&retried, RECORDS_DROPPED, &rejected), series_records);
        let diagnosed = [("key", "request_rejected")];
        assert_eq!(sum_of(&retried, "logit.component.diagnostics", &diagnosed), 1.0);
        let per_attempt = [
            PER_ATTEMPT[0],
            PER_ATTEMPT[1],
            (RECORDS_DROPPED, &[("reason", "rejected")][..]),
            ("logit.component.diagnostics", &[("key", "request_rejected")][..]),
        ];
        assert_counted_once_per_batch(&single, &retried, &ENCODE_SIDE, &per_attempt);
    }

    // ---- per-request retry memory --------------------------------------------------------------

    /// One entry per request, so each log is its own request on the logs route.
    const ONE_PER_REQUEST: Caps = Caps { entries: 1, ..Caps::UNBOUNDED };

    /// `n` logs, each its own message, so each request's body differs from the others'.
    fn numbered_logs(n: usize) -> Vec<Event> {
        (0..n).map(|i| log_event(NOW, &format!("log {i}"))).collect()
    }

    /// A sink on `probe`'s handles that sends one entry per request.
    fn one_per_request(addr: SocketAddr, probe: &TelemetryProbe) -> DatadogOutput {
        instrumented(addr, probe).with_caps(ONE_PER_REQUEST)
    }

    /// A `503` on the second of three logs requests: the retry resends that request alone and
    /// then sends the third, so the logs bodies read first, second, second, third.
    #[tokio::test]
    async fn a_retry_resends_only_the_failed_chunk_of_a_route() {
        let script = |p: &str, k| if p == LOGS && k == 1 { busy_once(p, 0) } else { accepted(p) };
        let b = batch(numbered_logs(3));
        let (sums, log) = run_dd(script, vec![b], at_least_once(), one_per_request).await;
        let sent = bodies(&log, LOGS);
        assert_eq!(sent.len(), 4, "three requests and one resend");
        assert_eq!(sent[1], sent[2], "the retry resends the failed request");
        assert!(sent[0] != sent[1] && sent[1] != sent[3] && sent[0] != sent[3]);
        assert_eq!(sum_of(&sums, RECORDS, &[("route", "logs")]), 3.0);
        assert_eq!(sum_of(&sums, RECORDS_DROPPED, &[]), 0.0);
    }

    /// A request index past the first 64-bit word of the accounting's memory: a `503` on the
    /// 67th of 70 logs requests resends that request alone.
    #[tokio::test]
    async fn a_retry_past_the_sixty_fourth_request_resends_only_the_failed_one() {
        let script = |p: &str, k| if p == LOGS && k == 66 { busy_once(p, 0) } else { accepted(p) };
        let b = batch(numbered_logs(70));
        let (sums, log) = run_dd(script, vec![b], at_least_once(), one_per_request).await;
        let sent = bodies(&log, LOGS);
        assert_eq!(sent.len(), 71, "70 requests and one resend");
        assert_eq!(sent[66], sent[67], "the retry resends the failed request");
        assert_eq!(sum_of(&sums, RECORDS, &[("route", "logs")]), 70.0);
    }

    /// A batch whose every request is rejected, across two attempts, is dropped `rejected`; one
    /// whose series request was accepted on the first attempt is delivered, though the only
    /// request of the second is rejected.
    #[tokio::test]
    async fn rejections_across_attempts_drop_the_batch_unless_a_request_was_accepted() {
        for (series, dropped) in [(400, 1.0), (200, 0.0)] {
            let script = move |p: &str, k| match (p, k) {
                (SERIES, _) => Reply::Answer(series, Vec::new()),
                (_, 0) => Reply::Answer(503, Vec::new()),
                _ => Reply::Answer(400, Vec::new()),
            };
            let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
            let (sums, log) = run_dd(script, vec![b], at_least_once(), instrumented).await;
            assert_eq!(recorded_paths(&log), [SERIES, LOGS, LOGS], "series {series}");
            let rejected = [("reason", "rejected")];
            let batches_dropped = sum_of(&sums, "logit.component.batches.dropped", &rejected);
            assert_eq!(batches_dropped, dropped, "series {series}");
            let delivered = sum_of(&sums, "logit.component.batches.delivered", &[]);
            assert_eq!(delivered, 1.0 - dropped, "series {series}");
        }
    }

    /// Under `at_most_once` an `Ambiguous` stop drops the batch and counts
    /// `ambiguous_at_most_once` for the failed request, the rest of its route, and every later
    /// route, and nothing for a request already accepted. Under `at_least_once` the same script
    /// is retried and delivered, counting nothing.
    #[tokio::test]
    async fn an_ambiguous_stop_at_most_once_counts_what_was_not_accepted() {
        // (failing path, its failing request, series counted, logs counted)
        let cases = [(SERIES, 1, 1.0, 3.0), (LOGS, 1, 0.0, 2.0)];
        for (path, request, series, logs) in cases {
            let script = move |p: &str, k| {
                if p == path && k == request {
                    Reply::Answer(503, Vec::new())
                } else {
                    accepted(p)
                }
            };
            let events = || {
                let mut events = vec![gauge(NOW), gauge(NOW - 1_000_000_000)];
                events.extend(numbered_logs(3));
                vec![batch(events)]
            };
            let (sums, _) = run_dd(script, events(), at_most_once(), one_per_request).await;
            let counted = |route| {
                let tags = [("route", route), ("reason", "ambiguous_at_most_once")];
                sum_of(&sums, RECORDS_DROPPED, &tags)
            };
            assert_eq!(counted("series"), series, "{path}");
            assert_eq!(counted("logs"), logs, "{path}");
            assert_eq!(sum_of(&sums, "logit.component.batches.dropped", &[]), 1.0, "{path}");

            let (sums, _) = run_dd(script, events(), at_least_once(), one_per_request).await;
            let ambiguous = [("reason", "ambiguous_at_most_once")];
            assert_eq!(sum_of(&sums, RECORDS_DROPPED, &ambiguous), 0.0, "{path}");
            assert_eq!(sum_of(&sums, "logit.component.batches.delivered", &[]), 1.0, "{path}");
        }
    }

    /// The memory is per batch: a second batch after a retried one sends every request again.
    #[tokio::test]
    async fn a_new_batch_resends_every_request() {
        let script = |p: &str, k| if p == LOGS { busy_once(p, k) } else { accepted(p) };
        let b = || batch(vec![gauge(NOW), log_event(NOW, "after")]);
        let (_, log) = run_dd(script, vec![b(), b()], at_least_once(), instrumented).await;
        assert_eq!(recorded_paths(&log), [SERIES, LOGS, LOGS, SERIES, LOGS]);
    }

    /// With no `observe_batch` nothing is remembered: each direct send is a new batch and sends
    /// every request.
    #[tokio::test]
    async fn direct_sends_with_no_observe_batch_resend_every_request() {
        let (addr, log) =
            per_path_recorder(|p, k| if p == LOGS { busy_once(p, k) } else { accepted(p) }).await;
        let mut out = sink(addr).with_clock(|| NOW);
        let b = batch(vec![gauge(NOW), log_event(NOW, "after")]);
        let err = out.send(&b).await.unwrap_err();
        assert_eq!(logit_pipeline::classify(&err), Fault::Ambiguous);
        out.send(&b).await.unwrap();
        assert_eq!(recorded_paths(&log.lock().unwrap()), [SERIES, LOGS, SERIES, LOGS]);
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

    // ---- duplicate timestamps ----------------------------------------------------------------

    /// Two gauges of one series in one second, the later reading the lower value, beside a point
    /// of another second: the later reading of the pair is sent and the earlier counted dropped,
    /// once for the batch though it took two attempts, and `records` counts only what was sent.
    #[tokio::test]
    async fn a_series_point_sharing_its_second_is_dropped_and_counted_once() {
        let b = batch(vec![
            metric(NOW, MetricKind::gauge(9.0)),
            metric(NOW + 500_000_000, MetricKind::gauge(2.0)),
            metric(NOW + 1_000_000_000, MetricKind::gauge(3.0)),
        ]);
        let (sums, log) = run_dd(busy_once, vec![b], at_least_once(), instrumented).await;
        let values: Vec<Vec<f64>> = bodies(&log, SERIES)
            .iter()
            .map(|body| {
                let decoded = DatadogDecoder::new().decode_series_v2_protobuf(body, NOW).unwrap();
                let value = |e: &Event| match e.metrics[0].kind {
                    MetricKind::Gauge(v) => v.as_f64(),
                    ref other => panic!("a gauge, got {other:?}"),
                };
                decoded.events.iter().map(value).collect()
            })
            .collect();
        assert_eq!(values, [vec![2.0, 3.0], vec![2.0, 3.0]], "the later reading is sent");
        let duplicate = [("reason", "duplicate_timestamp")];
        assert_eq!(sum_of(&sums, "logit.output.metrics.dropped", &duplicate), 1.0);
        assert_eq!(
            sum_of(&sums, "logit.component.diagnostics", &[("key", "duplicate_timestamp")]),
            1.0
        );
        assert_eq!(sum_of(&sums, RECORDS, &[("route", "series")]), 2.0);
    }

    /// Two equal points of one series in one second, as a relay or a resend writes them: one is
    /// sent, and the other counted `normalized` once for the batch, though it took two attempts,
    /// with no loss and no diagnostic.
    #[tokio::test]
    async fn an_equal_series_point_sharing_its_second_is_counted_normalized_once() {
        let b = batch(vec![
            metric(NOW, MetricKind::gauge(2.0)),
            metric(NOW + 500_000_000, MetricKind::gauge(2.0)),
        ]);
        let (sums, log) = run_dd(busy_once, vec![b], at_least_once(), instrumented).await;
        assert_eq!(bodies(&log, SERIES).len(), 2, "one request per attempt");
        assert_eq!(sent_series(&log), [("m".to_string(), 2.0), ("m".to_string(), 2.0)]);
        let duplicate = [("reason", "duplicate_timestamp")];
        assert_eq!(sum_of(&sums, "logit.output.metrics.normalized", &duplicate), 1.0);
        assert_eq!(sum_of(&sums, "logit.output.metrics.dropped", &duplicate), 0.0);
        assert_eq!(
            sum_of(&sums, "logit.component.diagnostics", &[("key", "duplicate_timestamp")]),
            0.0
        );
        assert_eq!(sum_of(&sums, RECORDS, &[("route", "series")]), 1.0);
    }

    /// Two equal delta counts of one series in one second are two increments: one is sent, and
    /// the other counted `dropped`, with the diagnostic, though the values are equal.
    #[tokio::test]
    async fn an_equal_count_sharing_its_second_is_counted_dropped() {
        let b = batch(vec![
            metric(NOW, MetricKind::counter(1.0)),
            metric(NOW + 500_000_000, MetricKind::counter(1.0)),
        ]);
        let (sums, _) = run_dd(|p, _| accepted(p), vec![b], at_least_once(), instrumented).await;
        let duplicate = [("reason", "duplicate_timestamp")];
        assert_eq!(sum_of(&sums, "logit.output.metrics.dropped", &duplicate), 1.0);
        assert_eq!(sum_of(&sums, "logit.output.metrics.normalized", &duplicate), 0.0);
        assert_eq!(
            sum_of(&sums, "logit.component.diagnostics", &[("key", "duplicate_timestamp")]),
            1.0
        );
        assert_eq!(sum_of(&sums, RECORDS, &[("route", "series")]), 1.0);
    }

    /// Two equal `rate` points of one series in one second are two increments over their
    /// interval: one is sent, and the other counted `dropped`.
    #[tokio::test]
    async fn an_equal_rate_sharing_its_second_is_counted_dropped() {
        let rate = |ts: i64| {
            let mut attributes = AttrMap::new();
            attributes.insert(logit_core::attrs::METRICS_TYPE, Value::str("rate"));
            Event::metric(ts, attributes, MetricRecord::new(intern("m"), MetricKind::gauge(0.5)))
        };
        let b = batch(vec![rate(NOW), rate(NOW + 500_000_000)]);
        let (sums, _) = run_dd(|p, _| accepted(p), vec![b], at_least_once(), instrumented).await;
        let duplicate = [("reason", "duplicate_timestamp")];
        assert_eq!(sum_of(&sums, "logit.output.metrics.dropped", &duplicate), 1.0);
        assert_eq!(sum_of(&sums, "logit.output.metrics.normalized", &duplicate), 0.0);
    }

    /// Datadog keeps both of two distribution points in one second, so nothing is dropped there.
    #[tokio::test]
    async fn distribution_points_sharing_a_second_are_all_sent() {
        let b = batch(vec![
            metric(NOW, MetricKind::Samples(Samples::new([1.0]))),
            metric(NOW, MetricKind::Samples(Samples::new([2.0]))),
        ]);
        let (sums, _) = run_dd(|p, _| accepted(p), vec![b], at_least_once(), instrumented).await;
        assert_eq!(sum_of(&sums, "logit.output.metrics.dropped", &[]), 0.0);
        assert_eq!(sum_of(&sums, RECORDS, &[("route", "distribution_points")]), 2.0);
    }

    /// Each series body's points as `(metric, value)`, in send order across the bodies.
    fn sent_series(log: &[Recorded]) -> Vec<(String, f64)> {
        bodies(log, SERIES)
            .iter()
            .flat_map(|body| {
                let decoded = DatadogDecoder::new().decode_series_v2_protobuf(body, NOW).unwrap();
                decoded.events.into_iter().map(|e| {
                    let value = match e.metrics[0].kind {
                        MetricKind::Gauge(v) => v.as_f64(),
                        ref other => panic!("a gauge, got {other:?}"),
                    };
                    (resolve(e.metrics[0].name).to_string(), value)
                })
            })
            .collect()
    }

    fn named(name: &str, ts: i64, value: f64) -> Event {
        Event::metric(ts, AttrMap::new(), MetricRecord::new(intern(name), MetricKind::gauge(value)))
    }

    /// A same-second pair four events apart, the rest other series: the pair, its later point, and
    /// the filler as `(metric, value)`.
    fn split_pair_batch() -> EventBatch {
        batch(vec![
            named("m", NOW, 9.0),
            named("a", NOW, 1.0),
            named("b", NOW, 1.0),
            named("c", NOW, 1.0),
            named("m", NOW + 500_000_000, 2.0),
        ])
    }

    fn assert_pair_collapsed_once(sums: &Sums, log: &[Recorded]) {
        let sent = sent_series(log);
        let m: Vec<f64> = sent.iter().filter(|(n, _)| n == "m").map(|(_, v)| *v).collect();
        assert_eq!(m, [2.0], "only the later point of the pair is sent: {sent:?}");
        let duplicate = [("reason", "duplicate_timestamp")];
        assert_eq!(sum_of(sums, "logit.output.metrics.dropped", &duplicate), 1.0);
        assert_eq!(sum_of(sums, RECORDS, &[("route", "series")]), sent.len() as f64);
    }

    /// A same-second pair that a request cut by entry count would split is collapsed before the
    /// cut: one point sent, one dropped, and `records` counting what was sent.
    #[tokio::test]
    async fn a_same_second_pair_split_by_the_entry_cut_is_collapsed_once() {
        let build = |addr, probe: &TelemetryProbe| {
            instrumented(addr, probe).with_caps(Caps { entries: 2, ..Caps::UNBOUNDED })
        };
        let (sums, log) =
            run_dd(|p, _| accepted(p), vec![split_pair_batch()], at_least_once(), build).await;
        assert!(bodies(&log, SERIES).len() > 1, "the cut makes several requests");
        assert_pair_collapsed_once(&sums, &log);
    }

    /// A same-second pair in one count-capped chunk whose body is over the byte cap, so the chunk
    /// is bisected down to single events: still one point sent and one dropped.
    #[tokio::test]
    async fn a_same_second_pair_split_by_bisection_is_collapsed_once() {
        let build = |addr, probe: &TelemetryProbe| {
            instrumented(addr, probe).with_caps(Caps { raw_bytes: 60, ..Caps::UNBOUNDED })
        };
        let (sums, log) =
            run_dd(|p, _| accepted(p), vec![split_pair_batch()], at_least_once(), build).await;
        let sizes: Vec<usize> = bodies(&log, SERIES).iter().map(Vec::len).collect();
        assert!(sizes.len() > 2, "the chunk was bisected: {sizes:?}");
        assert_pair_collapsed_once(&sums, &log);
    }

    /// An ambiguous failure under `at_most_once` counts the entries it loses, and a point already
    /// counted `duplicate_timestamp` isn't one of them.
    #[tokio::test]
    async fn an_ambiguous_at_most_once_drop_does_not_count_a_duplicate_twice() {
        let script = |p: &str, k| {
            if p == SERIES && k == 0 {
                Reply::Answer(503, Vec::new())
            } else {
                accepted(p)
            }
        };
        let build = |addr, probe: &TelemetryProbe| {
            instrumented(addr, probe).with_caps(Caps { entries: 1, ..Caps::UNBOUNDED })
        };
        let b = batch(vec![
            named("m", NOW, 9.0),
            named("m", NOW + 500_000_000, 2.0),
            named("a", NOW, 1.0),
            named("b", NOW, 1.0),
        ]);
        let (sums, _) = run_dd(script, vec![b], at_most_once(), build).await;
        let ambiguous = [("route", "series"), ("reason", "ambiguous_at_most_once")];
        assert_eq!(sum_of(&sums, RECORDS_DROPPED, &ambiguous), 3.0);
        let duplicate = [("reason", "duplicate_timestamp")];
        assert_eq!(sum_of(&sums, "logit.output.metrics.dropped", &duplicate), 1.0);
    }
}

#[cfg(test)]
mod concurrency {
    use super::*;
    use crate::http::held::{self, Answer, Held, Subject};
    use logit_core::interner::intern;
    use logit_core::{AttrMap, Event, MetricKind, MetricRecord, Resource, Value};
    use logit_proto::datadog::events::ATTR_EVENT_TITLE;

    /// The sink's clock and every event's timestamp, so no point is stale.
    const NOW: i64 = 1_790_000_000_000_000_000;

    fn make(url: &str, concurrency: u32, telemetry: Telemetry) -> DatadogOutput {
        DatadogOutput::new("key")
            .unwrap()
            .with_endpoints(DatadogEndpoints {
                api: Some(url.into()),
                logs: Some(url.into()),
                traces: Some(url.into()),
            })
            .with_compression(DatadogCompression::None)
            .with_timeout(Duration::from_secs(60))
            .with_concurrency(concurrency)
            .with_telemetry(telemetry)
            .with_clock(|| NOW)
    }

    fn batch_of(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    /// Batch `n` is one log marked `held::marker(n)` beside an event no route carries, which the
    /// encoder counts skipped.
    fn batch(n: u64) -> EventBatch {
        batch_of(vec![held::marker_log(NOW, n), Event::empty(NOW, AttrMap::new())])
    }

    fn subject() -> Subject<DatadogOutput> {
        Subject {
            kind: "datadog_out",
            make,
            batch,
            id_of: held::id_after_marker,
            encoded: ("logit.output.events.skipped", &[("reason", "no_payload")]),
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

    /// Batch `n` takes three routes, series, events, and logs, each request marked `held::marker(n)`.
    fn three_routes(n: u64) -> EventBatch {
        let gauge = MetricRecord::new(intern(&held::marker(n)), MetricKind::gauge(1.0));
        let mut event = held::marker_log(NOW, n);
        event.attributes.insert(ATTR_EVENT_TITLE, Value::str("deploy"));
        batch_of(vec![Event::metric(NOW, AttrMap::new(), gauge), event, held::marker_log(NOW, n)])
    }

    /// Batch 1's series request is rejected, its event accepted, and its logs answered `500`
    /// while batch 2 is held in another slot: the head's resubmit sends logs alone, skipping
    /// both settled requests, and counts the rejected series once. Both batches are held at
    /// once first, so a window of one can't pass.
    #[tokio::test]
    async fn a_resubmitted_head_skips_its_settled_requests_with_another_slot_in_flight() {
        let held = Held::start_by_path(held::id_after_marker, |batch, path, earlier| {
            match (batch, path, earlier) {
                (2, _, 0) => Answer::Hold,
                (1, "/api/v2/series", 0) => Answer::Now(held::RESP_400),
                (1, "/api/v2/logs", 2) => Answer::Hold,
                _ => Answer::Now(held::RESP_204),
            }
        })
        .await;
        let subject = Subject { batch: three_routes, ..subject() };
        let (mut output, mut probe, telemetry) = held::sink(&subject, &held, 2);
        let control = async {
            logit_pipeline::test_util::wait_until("both batches held", || held.held() == 2).await;
            held.release(1, held::RESP_500);
            logit_pipeline::test_util::wait_until("batch 1 resent behind a held batch 2", || {
                held.paths(1).len() >= 4 && held.held() == 1
            })
            .await;
            held.release(2, held::RESP_204);
        };
        let (totals, _) = held::run(
            &mut output,
            &mut probe,
            telemetry,
            vec![three_routes(1), three_routes(2)],
            DeliveryPosture::AtLeastOnce,
            std::future::pending(),
            control,
        )
        .await;
        let paths = ["/api/v2/series", "/api/v1/events", "/api/v2/logs"];
        assert_eq!(held.paths(1), [paths[0], paths[1], paths[2], paths[2]]);
        assert_eq!(held.paths(2), paths);
        assert_eq!(held::delivered(&totals), 2.0);
        let rejected = [("route", "series"), ("reason", "rejected")];
        assert_eq!(totals.sum(RECORDS_DROPPED, &rejected), 1.0);
    }

    /// Series answered, events held.
    fn events_held(batch: u64, path: &str, _: usize) -> Answer {
        match (batch, path) {
            (_, "/api/v1/events") => Answer::Hold,
            _ => Answer::Now(held::RESP_204),
        }
    }

    #[tokio::test]
    async fn a_grace_cut_counts_the_requests_already_answered_on_send() {
        let subject = Subject { batch: three_routes, ..subject() };
        held::a_grace_cut_counts_the_requests_already_answered(subject, 1, events_held).await;
    }

    #[tokio::test]
    async fn a_grace_cut_counts_the_requests_already_answered_in_a_slot() {
        let subject = Subject { batch: three_routes, ..subject() };
        held::a_grace_cut_counts_the_requests_already_answered(subject, 2, events_held).await;
    }
}
