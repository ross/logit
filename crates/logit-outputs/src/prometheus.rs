//! `prometheus_out`: the Prometheus sink, in one of two modes. An **exposition** endpoint a
//! Prometheus scrapes (`bind:`), or a **remote-write sender** that POSTs each batch to a receiver
//! (`endpoint:`). Exactly one is set; graph rule 56 rejects both and neither. It mirrors
//! `logit_inputs::prometheus` and is the fourth like-protocol pair under
//! [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md). Both wire syntaxes and the
//! model-to-families mapping live in `logit_proto::prometheus` ([`text::write`],
//! [`remote_write::encode`], [`events_to_families`]); nothing here knows what a sample line or a
//! `TimeSeries` looks like.
//!
//! **This module doc is the spec** (house convention; see [`crate::statsd`]'s). The authorities
//! are [ADR `prometheus-scrape-and-exposition`](../../../docs/adr/prometheus-scrape-and-exposition.md)
//! ("Exposition state and expiry", "Dialects and negotiation", "`Output::bind`") and
//! [ADR `prometheus-remote-write`](../../../docs/adr/prometheus-remote-write.md) ("Sender
//! behaviour: one request per batch, no retry in the sink", "Decode and encode work in timestamp
//! groups", "Timestamps: received without the marker, sent always").
//!
//! ## Config
//!
//! Registry mode, [`ExposeOutput`]:
//!
//! ```yaml
//! kind: prometheus_out
//! bind: "127.0.0.1:9464"   # loopback in every example -- see "Security posture"
//! path: /metrics           # default; must start with `/` (rule 41)
//! expire_after: 5m         # a series not updated within this window stops being exposed; 0s off
//! max_series: 100000       # hard cap, least-recently-updated evicted first; 0 rejected (rule 41)
//! ```
//!
//! Sender mode, [`RemoteWriteOutput`]:
//!
//! ```yaml
//! kind: prometheus_out
//! endpoint: "http://mimir:8080/api/v1/push"  # absolute http(s) URL, path included
//! version: 1                                 # default; 1 = prometheus.WriteRequest, 2 = io.prometheus.write.v2.Request
//! timeout: 10s                               # default; bounds one request
//! headers:                                   # e.g. a tenant header; !env works on a value
//!   X-Scope-OrgID: tenant-a
//! endpoint_tls: {}                           # https:// only -- see `TlsClientConfig`
//! ```
//!
//! Setting the other mode's field is a config error, not a no-op (rule 56). `buffer:` works in
//! both modes, as a sibling of `kind:`; it matters more in sender mode, the only one whose `send`
//! can fail.
//!
//! ## Dialect negotiation (registry mode)
//!
//! One endpoint, two dialects, chosen per request from the client's `Accept`:
//!
//! | Request | Response `Content-Type` |
//! |---|---|
//! | `Accept` contains `application/openmetrics-text` | `application/openmetrics-text; version=1.0.0; charset=utf-8` |
//! | anything else, including no `Accept` and a bare `*/*` | `text/plain; version=0.0.4; charset=utf-8` |
//!
//! Both strings come from [`Dialect::content_type`], so the scraper's `Accept` and the response's
//! `Content-Type` never drift apart. `Accept-Encoding: gzip` gets a gzipped body and
//! `Content-Encoding: gzip`; `gzip;q=0` doesn't.
//!
//! ## Routes (registry mode)
//!
//! | Request | Response |
//! |---|---|
//! | `GET`/`HEAD` on the configured `path` | `200`, the rendered registry -- `logit.output.scrapes{class="ok"}` |
//! | any method on any other path | `404` -- `logit.output.scrapes{class="not_found"}` |
//! | any other method on `path` | `405` + `Allow: GET, HEAD` -- `logit.output.scrapes{class="method"}` |
//!
//! `HEAD` builds the same body as `GET`, so `content-length` matches (RFC 9110 §9.3.2); hyper
//! drops the bytes. Path is matched before method, so an unknown path is a `404` whatever the
//! method.
//!
//! ## State: upsert, expiry, cardinality (registry mode)
//!
//! [`ExposeOutput::send`] converts the batch with [`events_to_families`] (the codec's
//! [`PrometheusEncoder`] counts every lossy path: delta temporality, `ExponentialHistogram`, an
//! unrepresentable label) and **upserts** each series into the registry, keyed by family name and
//! then by the rendered, sorted label set; latest value wins. These are cumulative-series
//! semantics: a scrape renders the current registry, so re-sending a batch rewrites the same
//! values.
//!
//! - `expire_after` (default 5m, Prometheus's staleness horizon): a series last updated longer ago
//!   is dropped, counted `logit.output.series.evicted{reason="expired"}`; `0s` disables expiry.
//!   The sweep runs after every `send` **and on every request**, so a quiet sink stops exposing
//!   stale series instead of freezing its last state.
//! - `max_series` (default 100000): a hard cap. Over it, the least-recently-updated series go
//!   first, counted `logit.output.series.evicted{reason="cardinality"}`: the same shape as
//!   `aggregate`'s `max_retained_series`.
//!
//! **Type conflicts.** The exposition grammar allows one `# TYPE` per family name, so a `gauge`
//! arriving for a name registered as a `counter` can't sit beside it. The new type wins: the
//! family's type is replaced, every series stored under the old type is evicted, and
//! `logit.output.metrics.type_conflict` is counted. Keeping the old type would drop live data in
//! favour of data that may already have expired.
//!
//! ## The wire (sender mode)
//!
//! One `send` is **one `POST`**, and a batch that produces no series sends **no request**, not an
//! empty `WriteRequest`. Four headers are `insert`ed over a clone of the operator's `headers:`, so
//! a protocol-owned name always wins (rule 56 also rejects one at config time; `otlp_out`'s HTTP
//! transport uses the same merge order):
//!
//! | Header | Value |
//! |---|---|
//! | `Content-Type` | [`remote_write::Version::content_type`] -- `application/x-protobuf;proto=prometheus.WriteRequest` for `version: 1`, `…;proto=io.prometheus.write.v2.Request` for `2` |
//! | `Content-Encoding` | `snappy` -- the Snappy **block** format (`snap::raw`), never the framed one |
//! | `X-Prometheus-Remote-Write-Version` | [`remote_write::Version::header_version`] -- `0.1.0` for 1.0 (the spec's own historical number), `2.0.0` for 2.0 |
//! | `User-Agent` | `logit/<version>` |
//!
//! No negotiation and no fallback between versions: the operator picks the one their receiver
//! speaks, as they pick an exposition dialect.
//!
//! **Timestamp partition, merged `TimeSeries`.** A remote-write `TimeSeries` is one label set and
//! N samples; a `Series` is one label set and one point. So [`RemoteWriteOutput::send`] partitions
//! the batch's events by `Event::timestamp` (ascending, stable within a timestamp), runs
//! [`events_to_families`] once per partition, and hands the groups to [`remote_write::encode`],
//! which merges identical label sets across groups into one `TimeSeries` with samples in
//! timestamp order. Five scrapes of one target in a batch become one `TimeSeries` with five
//! samples, which is what a receiver's in-order check expects. The partition also keeps a classic
//! histogram's `_bucket`/`_sum`/`_count` samples, which share one timestamp, in front of one
//! assembler at the far end.
//!
//! The encoder runs `with_timestamps_always(true)`, since the wire can't omit a timestamp and a
//! series without one would be skipped, and `with_stale_markers(true)`, so a record flagged
//! `FLAG_NO_RECORDED_VALUE` is written as Prometheus's stale marker (a NaN with bit pattern
//! `0x7ff0000000000002`) instead of skipped. That covers `Gauge`, `Sum`, and marker-untyped
//! records only: a flagged `Histogram`/`Summary`/sketch expands to several series and one flag
//! doesn't say which existed, so it stays skipped and counted (`docs/known-gaps.md`).
//!
//! ## Faults, retries and duplicate safety (sender mode)
//!
//! **One `send` is one attempt.** Retry is `write_loop`'s job
//! (`docs/adr/buffered-sink-delivery.md`); this sink classifies the outcome with
//! `.context(fault)`, by the table `otlp_out`'s HTTP transport uses, shared in [`crate::http`]:
//!
//! | Outcome | Result |
//! |---|---|
//! | 2xx | `Ok` |
//! | 429, any 5xx | [`Fault::Ambiguous`] -- the request reached the server and may have been partly applied |
//! | any 3xx, any other 4xx | [`Fault::Permanent`], with the status and the first 256 bytes of the response body in the message and in a throttled `remote_write_rejected` diagnostic: Prometheus's own `400` text names the offending series and is the only useful thing in the exchange |
//! | connect failure | [`Fault::Clean`] -- the destination provably never saw it |
//! | any other transport error, timeout included | [`Fault::Ambiguous`] |
//!
//! **Redirects aren't followed** ([`crate::http::build_client`] turns off `reqwest`'s default
//! `limited(10)`), which is why `3xx` is on that table. Remote-write defines no redirect, and
//! following one breaks the premise that the request went to the configured URL: a
//! `301`/`302`/`303` replays as a body-less `GET`, so a batch nothing wrote would be acked by
//! whatever answered, and a `307`/`308` would carry the operator's `headers:` (a tenant header,
//! an `Authorization` on a same-host scheme downgrade) to the `Location` host, past rule 56's
//! `https://` check. The redirect is reported against the configured URL, where the
//! misconfiguration is.
//!
//! **The rejection body is read bounded.** At most 256 bytes (plus a character's slack) leave the
//! socket, so a receiver answering `500` with an endless body costs a snippet per retry, not a
//! connection's worth of allocation ([`crate::http::read_body_prefix`]).
//!
//! [`RemoteWriteOutput::duplicate_safe`] is **`true`**. A sample's identity at a remote-write
//! receiver is `(label set, timestamp)`, so replaying an identical request is an idempotent
//! overwrite, never a double count. And `true` selects `DeliveryPosture::AtLeastOnce`
//! (`logit_pipeline::output`), the only posture that retries a `Fault::Ambiguous`: `false` would
//! turn every 5xx into a dropped batch, not make delivery safer.
//!
//! **Ordering is the topology's, not this sink's.** Samples go out in batch order and nothing
//! reorders across batches. Two upstream branches writing the same series can draw out-of-order
//! `400`s from a receiver with no out-of-order window; a single chain into one `prometheus_out`
//! can't.
//!
//! ## Telemetry
//!
//! Registry mode:
//!
//! | Point | Meaning |
//! |---|---|
//! | `logit.output.scrapes{class="ok"\|"not_found"\|"method"}` | one per HTTP request, by outcome. `ok` means the response was *rendered*, not acknowledged: a `Full<Bytes>` response offers no body-completion hook, so a connection dropped mid-write still counts `ok` |
//! | `logit.output.scrape.bytes` | response body bytes as rendered -- post-gzip when the client asked for it, so it measures transfer cost rather than exposition size. Counted when the body is built, same caveat as `ok` above |
//! | `logit.output.series` (gauge) | series held after each `send` |
//! | `logit.output.series.evicted{reason="expired"\|"cardinality"}` | see above |
//! | `logit.output.metrics.type_conflict` | see above |
//!
//! Sender mode:
//!
//! | Point | Meaning |
//! |---|---|
//! | `logit.output.requests{class="1xx"\|"2xx"\|"3xx"\|"4xx"\|"5xx"\|"other"\|"network_error"}` | one per request, in `otlp_out`'s vocabulary ([`crate::http::status_class`]). No `429` class: a 429 is a `4xx`, and splitting it out would contradict [`crate::http::is_retryable_http_status`], which picks the `Fault` from the same status. No `timeout` class: a timeout is a transport error, so `network_error`. No `signal` tag, unlike `otlp_out`: this sink has one signal |
//! | `logit.output.request.duration` | one timer per request issued, the spelling `graphite_out`, `collectd_out`, `syslog_out`, `statsd_out` and `influxdb_out` use |
//! | `logit.output.samples` | samples in the request body, counted by the codec ([`remote_write::encode_counted`]) rather than guessed from family counts: one `Series` is one sample for a gauge and several for a histogram. Mirrors `prometheus_in`'s `logit.input.samples`, so the two ends of a relay compare |
//!
//! Both modes also get everything the codec counts on the sink's [`PrometheusEncoder`]:
//! `logit.output.metrics.{skipped,degraded}`, `logit.output.labels.dropped`, and
//! `logit.output.labels.normalized{reason="multi_value"}`. That last one is a `Value::Array`
//! attribute (a repeated DogStatsD tag key relayed from `statsd_in`) collapsed to its last
//! representable element, since a Prometheus label set has no multi-value label. Unlike the other
//! `*.normalized` reasons in `docs/design/internal-telemetry.md`, it's **lossy**: the non-last
//! elements are discarded. It's `normalized` rather than `dropped` because the label survives.
//!
//! A delta `Sum` is the common skip in either mode: counted
//! `logit.output.metrics.skipped{metric_kind="delta_sum"}`, with a throttled
//! `delta_temporality_unresolved` diagnostic naming the fix, an `aggregate` with
//! `temporality: cumulative` in front of the sink. This sink never accumulates a delta itself;
//! remote-write carries cumulative series too, so it has no spelling for one either.
//!
//! **Registry mode counts both directions on one encoder.** `send` and a render share a single
//! [`PrometheusEncoder`], so `send`'s drops and a render's
//! `degraded{reason="exemplar_dropped"|"unit_not_suffix"}` from [`text::write_with`] add up as one
//! component's totals. The lock order that implies (encoder before registry) is on
//! [`ExposeOutput::encoder`]. Sender mode has one direction and `&mut self`, so its encoder is a
//! plain field.
//!
//! ## Security posture: no TLS, no auth on `bind:`
//!
//! Registry mode serves the **entire registry**, every label on every series, to anything that
//! connects to `bind:`, with no credential check and no transport encryption. That's the posture
//! [ADR `admin-readiness-endpoint`](../../../docs/adr/admin-readiness-endpoint.md) accepted for
//! `/readyz`, except the payload is a metric surface rather than a lifecycle word. So bind
//! loopback or pod-local (`127.0.0.1:9464`, as every shipped example does) and front it with
//! something that has TLS and auth. Exposing it off-host is an operator's explicit choice, not
//! an example's default. Tracked in `docs/known-gaps.md` next to `admin:`'s row.
//!
//! Sender mode is the opposite: it dials out, an `https://` endpoint gets TLS with the bundled
//! Mozilla roots by default, and `endpoint_tls:` adds a private CA, a client certificate for
//! mutual TLS, or `insecure_skip_verify` (which logs a startup warning). None of that gives the
//! exposition server TLS.

use crate::http::{
    body_snippet, build_client, classify_reqwest_error, is_retryable_http_status, read_body_prefix,
    status_class, ERROR_BODY_SNIPPET_BYTES,
};
/// The sender's `endpoint_tls:`: the shared `crate::tls` type, re-exported as the other sinks do.
pub use crate::tls::TlsClientSettings;
use anyhow::Context;
use bytes::Bytes;
use http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use logit_core::{Diagnostics, Event, EventBatch, Exemplar, Telemetry};
use logit_pipeline::{Fault, Output};
use logit_proto::prometheus::{
    events_to_families, remote_write, text, Dialect, FamilyType, MetricFamily, Point,
    PrometheusEncoder, Series,
};
use std::collections::{BTreeMap, HashMap};
use std::io::ErrorKind;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// The default `path:`, what a Prometheus scrape config assumes when `metrics_path` is unset.
pub const DEFAULT_PATH: &str = "/metrics";

/// The default `expire_after:`, Prometheus's staleness horizon.
pub const DEFAULT_EXPIRE_AFTER: Duration = Duration::from_secs(300);

/// The default `max_series:` cap.
pub const DEFAULT_MAX_SERIES: usize = 100_000;

/// Concurrent scrape connections, the bound `logit-cli`'s admin server uses: a handful of stuck
/// connections at worst, not an unbounded accept loop.
const MAX_CONCURRENT_CONNECTIONS: usize = 16;

/// How long a client has to send its request headers: the slowloris bound, tight because a
/// scrape request is a few hundred bytes. Hyper's knob, which needs the
/// [`hyper_util::rt::TokioTimer`] on the builder.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The floor for a whole connection, body included, well past Prometheus's default 10s
/// `scrape_timeout`. The body can be `max_series` series, and a shorter deadline would cut it
/// mid-write: a short read against a trusted `Content-Length`, already counted a success.
const RESPONSE_TIMEOUT_BASE: Duration = Duration::from_secs(30);

/// Added to [`RESPONSE_TIMEOUT_BASE`] per 1000 series of configured `max_series`, so raising the
/// cap raises the deadline. 40s total at the default.
const RESPONSE_TIMEOUT_PER_1K_SERIES: Duration = Duration::from_millis(100);

/// The accept loop's pause after an `accept()` failure that isn't one client's accident (fd
/// exhaustion, realistically). Without it a sustained one spins a core.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// [`RESPONSE_TIMEOUT_BASE`] plus [`RESPONSE_TIMEOUT_PER_1K_SERIES`] per 1000 of `max_series`,
/// saturating on an absurd cap.
fn response_timeout(max_series: usize) -> Duration {
    let thousands = u32::try_from(max_series / 1_000).unwrap_or(u32::MAX);
    RESPONSE_TIMEOUT_BASE.saturating_add(
        RESPONSE_TIMEOUT_PER_1K_SERIES.checked_mul(thousands).unwrap_or(Duration::MAX),
    )
}

/// The default `timeout:` for one remote-write request, the 10s `otlp_out` and `prometheus_in`'s
/// scrape use.
pub const DEFAULT_ENDPOINT_TIMEOUT: Duration = Duration::from_secs(10);

/// The `User-Agent` on every remote-write request. Reserved in config (rule 56): a receiver's
/// logs are often the only place a misbehaving sender is identified from.
const USER_AGENT: &str = concat!("logit/", env!("CARGO_PKG_VERSION"));

const SCRAPES: &str = "logit.output.scrapes";
/// Response body bytes as rendered (post-gzip if asked), counted when the body is built: a
/// `Full<Bytes>` has no completion hook, so a connection dropped mid-write still counts, here and
/// as `scrapes{class="ok"}`.
const SCRAPE_BYTES: &str = "logit.output.scrape.bytes";
const SERIES: &str = "logit.output.series";
const SERIES_EVICTED: &str = "logit.output.series.evicted";
const TYPE_CONFLICT: &str = "logit.output.metrics.type_conflict";

// Sender mode's three; the module doc's "Telemetry" has why `requests` has no `signal` tag.
const REQUESTS: &str = "logit.output.requests";
const REQUEST_DURATION: &str = "logit.output.request.duration";
/// Samples in the request body as the codec counted them: several per histogram `Series`.
const SAMPLES: &str = "logit.output.samples";

/// What reads "now" for the expiry sweep, a closure so tests can advance time by hand instead of
/// sleeping through a real `expire_after`.
type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// A series' rendered, sorted label set: the registry's per-family key. Rendered, not the
/// `AttrMap`, because sanitization is many-to-one (ADR "Names and sanitization") and the wire
/// series is what a scraper sees.
type LabelKey = Vec<(String, String)>;

/// One series' current value and when it last arrived; the label set is its map key.
#[derive(Debug, Clone)]
struct Stored {
    point: Point,
    timestamp: Option<i64>,
    created: Option<i64>,
    exemplars: Vec<Exemplar>,
    updated_at: Instant,
}

/// One family: one `# TYPE`, one metadata pair, and its series keyed by label set.
#[derive(Debug, Clone)]
struct StoredFamily {
    kind: FamilyType,
    help: Option<String>,
    unit: Option<String>,
    series: BTreeMap<LabelKey, Stored>,
}

/// What a scrape renders. `BTreeMap` at both levels, so iteration is already [`text::write`]'s
/// canonical order (families by name, series by label set).
#[derive(Debug, Default)]
struct Registry {
    families: BTreeMap<String, StoredFamily>,
}

impl Registry {
    fn len(&self) -> usize {
        self.families.values().map(|f| f.series.len()).sum()
    }

    /// Upserts one family's series, latest wins. A type change drops every series stored under
    /// the old type (module doc's "Type conflicts").
    fn upsert(&mut self, family: MetricFamily, now: Instant, telemetry: &Telemetry) {
        let MetricFamily { name, kind, help, unit, series } = family;
        let entry = self.families.entry(name).or_insert_with(|| StoredFamily {
            kind,
            help: None,
            unit: None,
            series: BTreeMap::new(),
        });
        if entry.kind != kind {
            entry.kind = kind;
            entry.series.clear();
            telemetry.count(TYPE_CONFLICT, 1.0, &[]);
        }
        // Latest wins for `# HELP`/`# UNIT` too: a scrape shows what the producer last said.
        entry.help = help;
        entry.unit = unit;
        for series in series {
            let Series { labels, point, timestamp, created, exemplars } = series;
            entry
                .series
                .insert(labels, Stored { point, timestamp, created, exemplars, updated_at: now });
        }
    }

    /// Drops every series last updated more than `expire_after` ago (zero disables expiry), and
    /// any family left empty, which would otherwise render as a bare `# TYPE`/`# HELP`.
    fn sweep(&mut self, expire_after: Duration, now: Instant, telemetry: &Telemetry) {
        if expire_after.is_zero() {
            return;
        }
        let mut evicted = 0u64;
        for family in self.families.values_mut() {
            family.series.retain(|_, stored| {
                let stale = now.saturating_duration_since(stored.updated_at) > expire_after;
                if stale {
                    evicted += 1;
                }
                !stale
            });
        }
        if evicted > 0 {
            self.families.retain(|_, family| !family.series.is_empty());
            telemetry.count(SERIES_EVICTED, evicted as f64, &[("reason", "expired")]);
        }
    }

    /// Evicts least-recently-updated series until at most `max_series` remain, in **one pass**
    /// however many go.
    ///
    /// Over the cap is the steady state the cap exists for, so this runs under load, holding the
    /// lock a render needs. It borrows `(updated_at, &name, &labels)` per series once,
    /// [`select_nth_unstable_by`](slice::select_nth_unstable_by) moves the `k` oldest to the front
    /// in O(N), and only those `k` keys are cloned. A `while len() > max` loop over `min_by` would
    /// make `k` allocating full scans per `send`, just when cardinality is being diagnosed.
    ///
    /// Ties on `updated_at` break on the series key (family name, then label set), so which of
    /// two same-batch series goes depends on the data, not `Instant` resolution or map order.
    fn enforce_cap(&mut self, max_series: usize, telemetry: &Telemetry) {
        let total = self.len();
        if total <= max_series {
            return;
        }
        let excess = total - max_series;

        let mut candidates: Vec<(Instant, &str, &LabelKey)> = Vec::with_capacity(total);
        for (name, family) in &self.families {
            for (labels, stored) in &family.series {
                candidates.push((stored.updated_at, name.as_str(), labels));
            }
        }
        // `1 <= excess <= total` here, so `excess - 1` indexes `candidates`.
        let order = |a: &(Instant, &str, &LabelKey), b: &(Instant, &str, &LabelKey)| {
            a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)).then_with(|| a.2.cmp(b.2))
        };
        candidates.select_nth_unstable_by(excess - 1, order);
        let doomed: Vec<(String, LabelKey)> = candidates[..excess]
            .iter()
            .map(|(_, name, labels)| ((*name).to_string(), (*labels).clone()))
            .collect();
        drop(candidates);

        for (name, labels) in doomed {
            if let Some(family) = self.families.get_mut(&name) {
                family.series.remove(&labels);
                if family.series.is_empty() {
                    self.families.remove(&name);
                }
            }
        }
        telemetry.count(SERIES_EVICTED, excess as f64, &[("reason", "cardinality")]);
    }

    /// The registry as the codec's family list, which [`text::write`] renders.
    fn families(&self) -> Vec<MetricFamily> {
        self.families
            .iter()
            .map(|(name, family)| MetricFamily {
                name: name.clone(),
                kind: family.kind,
                help: family.help.clone(),
                unit: family.unit.clone(),
                series: family
                    .series
                    .iter()
                    .map(|(labels, stored)| Series {
                        labels: labels.clone(),
                        point: stored.point.clone(),
                        timestamp: stored.timestamp,
                        created: stored.created,
                        exemplars: stored.exemplars.clone(),
                    })
                    .collect(),
            })
            .collect()
    }

    /// Renders through [`text::write_with`], not [`text::write`], so the writer's drops
    /// (`degraded{reason="exemplar_dropped"|"unit_not_suffix"}`) count on the encoder `send`
    /// uses. Post-sanitization family-name collisions are that encoder's to skip and count too.
    fn render(&self, dialect: Dialect, encoder: &mut PrometheusEncoder) -> Vec<u8> {
        let mut out = Vec::new();
        text::write_with(&self.families(), dialect, &mut out, encoder);
        out
    }
}

/// What a request handler needs, behind one `Arc` cloned per connection.
struct ServerState {
    registry: Arc<Mutex<Registry>>,
    /// The same encoder `send` uses, so its counters are the sink's totals. Lock order is on
    /// [`ExposeOutput::encoder`].
    encoder: Arc<Mutex<PrometheusEncoder>>,
    path: String,
    expire_after: Duration,
    /// The whole-connection deadline, [`response_timeout`] of `max_series` at bind time.
    response_timeout: Duration,
    telemetry: Telemetry,
    clock: Clock,
}

/// `prometheus_out` in the mode the config selected (see the module doc). One enum, not two
/// sinks, because it's one `kind:`, one `buffer:`, and one set of encoder counters; rule 56 makes
/// the choice unambiguous, so `build_spec` picks a variant and nothing branches again.
// `RemoteWriteOutput` is a few hundred bytes larger, but there's one per component, boxed as
// `Box<dyn Output>` at startup; boxing a variant would add an indirection to every `send`.
#[allow(clippy::large_enum_variant)]
pub enum PrometheusOutput {
    /// `bind:` -- the exposition endpoint.
    Expose(ExposeOutput),
    /// `endpoint:` -- the remote-write sender.
    Send(RemoteWriteOutput),
}

impl From<ExposeOutput> for PrometheusOutput {
    fn from(output: ExposeOutput) -> Self {
        PrometheusOutput::Expose(output)
    }
}

impl From<RemoteWriteOutput> for PrometheusOutput {
    fn from(output: RemoteWriteOutput) -> Self {
        PrometheusOutput::Send(output)
    }
}

/// Pure delegation: every contract, [`Output::duplicate_safe`]'s included, is the mode's.
#[async_trait::async_trait]
impl Output for PrometheusOutput {
    async fn bind(&mut self) -> anyhow::Result<()> {
        match self {
            PrometheusOutput::Expose(output) => output.bind().await,
            PrometheusOutput::Send(output) => output.bind().await,
        }
    }

    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        match self {
            PrometheusOutput::Expose(output) => output.send(batch).await,
            PrometheusOutput::Send(output) => output.send(batch).await,
        }
    }

    async fn flush(&mut self) -> anyhow::Result<()> {
        match self {
            PrometheusOutput::Expose(output) => output.flush().await,
            PrometheusOutput::Send(output) => output.flush().await,
        }
    }

    fn duplicate_safe(&self) -> bool {
        match self {
            PrometheusOutput::Expose(output) => output.duplicate_safe(),
            PrometheusOutput::Send(output) => output.duplicate_safe(),
        }
    }
}

/// The `bind:` mode: `logit_pipeline::Output` for the exposition endpoint.
pub struct ExposeOutput {
    bind: String,
    path: String,
    expire_after: Duration,
    max_series: usize,
    registry: Arc<Mutex<Registry>>,
    /// The address bound, once [`ExposeOutput::bind`] has run; how a `:0` test learns its port.
    local_addr: Option<SocketAddr>,
    /// The accept loop, which owns the [`TcpListener`]; aborting it closes the port. `Some` is
    /// also the "already bound" flag that makes [`Output::bind`] idempotent.
    server: Option<JoinHandle<()>>,
    /// Shared with the request handler: `send` converts through it, and a render counts
    /// [`text::write_with`]'s drops on it. **Lock order is encoder before registry**: `send`
    /// releases the encoder before taking the registry, and a render takes both in that order.
    encoder: Arc<Mutex<PrometheusEncoder>>,
    /// The encoder is rebuilt from this by the builders; the accept loop also reports its
    /// `prometheus_accept_failed` under it.
    diag: Diagnostics,
    telemetry: Telemetry,
    clock: Clock,
}

impl ExposeOutput {
    /// `bind` is `host:port`, resolved when [`Output::bind`] runs, not at config load.
    pub fn new(bind: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            path: DEFAULT_PATH.to_string(),
            expire_after: DEFAULT_EXPIRE_AFTER,
            max_series: DEFAULT_MAX_SERIES,
            registry: Arc::new(Mutex::new(Registry::default())),
            local_addr: None,
            server: None,
            encoder: Arc::new(Mutex::new(PrometheusEncoder::new())),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            clock: Arc::new(Instant::now),
        }
    }

    pub fn with_path(mut self, path: impl Into<String>) -> Self {
        self.path = path.into();
        self
    }

    /// `Duration::ZERO` disables expiry.
    pub fn with_expire_after(mut self, expire_after: Duration) -> Self {
        self.expire_after = expire_after;
        self
    }

    pub fn with_max_series(mut self, max_series: usize) -> Self {
        self.max_series = max_series;
        self
    }

    /// Reaches the codec's encoder, which reports the throttled
    /// `delta_temporality_unresolved`/`gauge_delta_unresolved` diagnostics, and the accept loop's
    /// `prometheus_accept_failed`.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self.rebuild_encoder();
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self.rebuild_encoder();
        self
    }

    /// Derives the encoder from `telemetry`/`diag`. Replacing it whole keeps `PrometheusEncoder`'s
    /// `mut self -> Self` builders usable; every caller is a builder that runs before `bind`, so
    /// the handler's `Arc` clone gets the finished encoder.
    fn rebuild_encoder(&mut self) {
        self.encoder = Arc::new(Mutex::new(
            PrometheusEncoder::new()
                .with_telemetry(self.telemetry.clone())
                .with_diagnostics(self.diag.clone()),
        ));
    }

    /// Overrides the sweep's "now" ([`Clock`]). Tests only: a public second clock would invite a
    /// sink whose `expire_after` means something else.
    #[cfg(test)]
    fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// The address bound, or `None` before [`Output::bind`] has run.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }
}

#[async_trait::async_trait]
impl Output for ExposeOutput {
    /// Opens the socket and starts serving. Idempotent, so the runtime's pre-spawn pass and
    /// `run_output`'s lazy call don't bind twice ([`Output::bind`]'s contract).
    ///
    /// Serving starts before the first batch, so a scraper polling a fresh `logit` gets an empty
    /// `200` rather than a connection refused it can't tell from a crash.
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.server.is_some() {
            return Ok(());
        }
        let listener = TcpListener::bind(&self.bind)
            .await
            .with_context(|| format!("binding prometheus_out on '{}'", self.bind))?;
        let local_addr = listener.local_addr().context("reading prometheus_out's bound address")?;
        // The `bound` lifecycle line every listener emits: this sink reaches `NodeState::Bound`
        // through the same pre-spawn pass. `local_addr`, not `self.bind`, so a `:0` shows the
        // real port.
        self.diag.info("bound", format_args!("serving {} on {local_addr}", self.path));
        self.local_addr = Some(local_addr);
        let state = Arc::new(ServerState {
            registry: Arc::clone(&self.registry),
            encoder: Arc::clone(&self.encoder),
            path: self.path.clone(),
            expire_after: self.expire_after,
            response_timeout: response_timeout(self.max_series),
            telemetry: self.telemetry.clone(),
            clock: Arc::clone(&self.clock),
        });
        self.server = Some(tokio::spawn(serve(listener, state, self.diag.clone())));
        Ok(())
    }

    /// Upserts the batch and brings the registry back within `expire_after`/`max_series`. Never
    /// fails: no I/O, only a lock and a `BTreeMap`.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let resource = batch.resource.as_ref();
        // Encoder before registry, and released first, so a scrape holding both never waits on
        // a conversion.
        let families = {
            let mut encoder = lock(&self.encoder);
            events_to_families(batch.events.iter().map(|event| (resource, event)), &mut encoder)
        };
        let now = (self.clock)();
        // One critical section, so a racing scrape never sees the registry over its cap or
        // holding expired series.
        let held = {
            let mut registry = lock(&self.registry);
            for family in families {
                registry.upsert(family, now, &self.telemetry);
            }
            registry.sweep(self.expire_after, now, &self.telemetry);
            registry.enforce_cap(self.max_series, &self.telemetry);
            registry.len()
        };
        self.telemetry.gauge(SERIES, held as f64, &[]);
        Ok(())
    }

    /// Stops serving by aborting the accept task, which owns the [`TcpListener`]; in-flight
    /// connections are bounded by [`response_timeout`]. Nothing is buffered: `send` commits to the
    /// registry before returning. [`Drop`] does the same for paths that never reach `flush`.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        Ok(())
    }

    /// `true`: `send` is an idempotent replace into an in-memory registry, so a redelivered batch
    /// changes nothing.
    fn duplicate_safe(&self) -> bool {
        true
    }
}

/// Aborts the accept loop on drop, not only on `flush`. On the startup-failure path (a later
/// component's `bind` fails) `run_with_telemetry` returns `RunError::Startup` and drops every spec
/// unflushed, which would leave the port held. Moot under the CLI, which exits; real in-process
/// and in tests. `logit-cli`'s admin listener has the same teardown.
impl Drop for ExposeOutput {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

/// The `endpoint:` mode: a stateless remote-write sender (module doc's "The wire" and "Faults,
/// retries and duplicate safety").
///
/// Nothing is retained between batches: no registry, sweep, cap, or lock. `send` takes
/// `&mut self`, so the encoder is a plain field.
pub struct RemoteWriteOutput {
    /// The absolute write URL. `reqwest` parses it per request; rule 56 has checked the scheme
    /// and authority.
    endpoint: String,
    version: remote_write::Version,
    request_timeout: Duration,
    client: reqwest::Client,
    /// The operator's `headers:`, built once; see [`RemoteWriteOutput::request_headers`].
    headers: HeaderMap,
    /// Built by [`RemoteWriteOutput::with_tls`] from `endpoint_tls:`. `None` keeps `reqwest`'s
    /// default trust (the bundled Mozilla roots).
    tls: Option<rustls::ClientConfig>,
    /// Rebuilt from `telemetry`/`diag` by the builders, as in [`ExposeOutput`].
    encoder: PrometheusEncoder,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl RemoteWriteOutput {
    /// `endpoint` is the receiver's absolute write URL, path included, resolved per request.
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            version: remote_write::Version::V1,
            request_timeout: DEFAULT_ENDPOINT_TIMEOUT,
            client: build_client(DEFAULT_ENDPOINT_TIMEOUT, None),
            headers: HeaderMap::new(),
            tls: None,
            encoder: new_sender_encoder(&Telemetry::default(), &Diagnostics::default()),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    /// Which remote-write message to send (`version:`). No negotiation, no fallback.
    pub fn with_version(mut self, version: remote_write::Version) -> Self {
        self.version = version;
        self
    }

    /// Per-request timeout (`timeout:`). The per-request `.timeout(..)` in `send` is what bounds
    /// a request; the client is rebuilt so its default agrees.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self.client = build_client(timeout, self.tls.as_ref());
        self
    }

    /// The extra headers on every request (`headers:`).
    ///
    /// Fails on a name or value that isn't legal HTTP (rule 56 rejects protocol-owned names; this
    /// catches illegal bytes and embedded newlines), and on two names that collide once case is
    /// normalized, which `HeaderMap::insert` would resolve by `HashMap` iteration order.
    pub fn with_headers(mut self, headers: &HashMap<String, String>) -> anyhow::Result<Self> {
        let mut map = HeaderMap::with_capacity(headers.len());
        for (name, value) in headers {
            let header_name = HeaderName::from_bytes(name.as_bytes())
                .with_context(|| format!("prometheus_out: {name:?} is not a legal header name"))?;
            let header_value = HeaderValue::from_str(value)
                .with_context(|| format!("prometheus_out: header {name:?} has an invalid value"))?;
            if map.insert(header_name, header_value).is_some() {
                anyhow::bail!(
                    "prometheus_out: header {name:?} collides with another entry in 'headers' \
                     once case is ignored -- HTTP header names are case-insensitive, so which \
                     value would actually be sent is undefined"
                );
            }
        }
        self.headers = map;
        Ok(self)
    }

    /// Client TLS tuning (`endpoint_tls:`): a private CA, a client certificate, or no
    /// verification. A no-op when `settings` is empty, since `reqwest` already does TLS for
    /// `https://`. Rule 56 checks the block's shape; the files load and validate here, since
    /// `graph::resolve` never touches the filesystem.
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
                "endpoint_tls.insecure_skip_verify is set -- the connection is encrypted, but \
                 this output will accept any certificate the peer presents, self-signed or \
                 otherwise",
            );
        }
        let cfg = crate::tls::build_client_config(settings, base_dir)?;
        self.client = build_client(self.request_timeout, Some(&cfg));
        self.tls = Some(cfg);
        Ok(self)
    }

    /// Reaches the codec's encoder, which reports the throttled
    /// `delta_temporality_unresolved`/`gauge_delta_unresolved` diagnostics, and this sink's
    /// `remote_write_rejected`.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self.encoder = new_sender_encoder(&self.telemetry, &self.diag);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self.encoder = new_sender_encoder(&self.telemetry, &self.diag);
        self
    }

    /// The batch's events grouped by `Event::timestamp`, ascending, stable within a group; the
    /// partition [`remote_write::encode`] merges back into one `TimeSeries` per label set. A
    /// `BTreeMap`, not a sort: a real batch has one or two distinct timestamps.
    fn partition(batch: &EventBatch) -> BTreeMap<i64, Vec<&Event>> {
        let mut groups: BTreeMap<i64, Vec<&Event>> = BTreeMap::new();
        for event in &batch.events {
            groups.entry(event.timestamp).or_default().push(event);
        }
        groups
    }

    /// The operator's headers with the protocol's four `insert`ed over them, so a protocol name
    /// always wins. `insert` and one `.headers(..)` at the call site, never `RequestBuilder`'s
    /// appending `.header(..)`, which would undo that (`otlp_out::send_http` has the same).
    fn request_headers(&self) -> HeaderMap {
        let mut headers = self.headers.clone();
        headers.insert(
            http::header::CONTENT_TYPE,
            HeaderValue::from_static(self.version.content_type()),
        );
        headers.insert(
            http::header::CONTENT_ENCODING,
            HeaderValue::from_static(remote_write::CONTENT_ENCODING_SNAPPY),
        );
        headers.insert(
            HeaderName::from_static(remote_write::HEADER_VERSION),
            HeaderValue::from_static(self.version.header_version()),
        );
        headers.insert(http::header::USER_AGENT, HeaderValue::from_static(USER_AGENT));
        headers
    }
}

/// The sender's encoder, built in one place so `new` and the builders can't drift (module doc's
/// "The wire"): `with_timestamps_always` because the wire can't omit a timestamp,
/// `with_stale_markers` because remote-write, unlike an exposition, can say "this series is gone".
fn new_sender_encoder(telemetry: &Telemetry, diag: &Diagnostics) -> PrometheusEncoder {
    PrometheusEncoder::new()
        .with_stale_markers(true)
        .with_timestamps_always(true)
        .with_telemetry(telemetry.clone())
        .with_diagnostics(diag.clone())
}

#[async_trait::async_trait]
impl Output for RemoteWriteOutput {
    /// Nothing to bind: this sink dials out per request.
    async fn bind(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// One batch, one request, one attempt (module doc's "The wire" and "Faults, retries and
    /// duplicate safety").
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let resource = batch.resource.as_ref();
        let groups: Vec<Vec<MetricFamily>> = Self::partition(batch)
            .into_values()
            .map(|events| {
                events_to_families(
                    events.into_iter().map(|event| (resource, event)),
                    &mut self.encoder,
                )
            })
            .collect();
        // No request for a batch that produced nothing: an empty `WriteRequest` is legal, but it
        // would count a `requests{class="2xx"}` an operator reads as a delivery.
        if groups.iter().all(Vec::is_empty) {
            return Ok(());
        }
        let (body, samples) =
            remote_write::encode_counted(&groups, self.version, &mut self.encoder);
        // Snappy block format (`snap::raw`), what both specs mean by `Content-Encoding: snappy`,
        // never the framed `snap::write`. Errors only past `u32::MAX`; reported, not unwrapped,
        // so an absurd batch fails alone.
        let compressed = snap::raw::Encoder::new()
            .compress_vec(&body)
            .context("snappy-compressing a remote-write request body")?;

        let request_timer = self.telemetry.timer(REQUEST_DURATION);
        let result = self
            .client
            .post(&self.endpoint)
            .headers(self.request_headers())
            .timeout(self.request_timeout)
            .body(compressed)
            .send()
            .await;
        drop(request_timer);

        match result {
            Ok(response) if response.status().is_success() => {
                self.telemetry.count(REQUESTS, 1.0, &[("class", status_class(response.status()))]);
                self.telemetry.count(SAMPLES, samples as f64, &[]);
                Ok(())
            }
            Ok(response) => {
                let status = response.status();
                self.telemetry.count(REQUESTS, 1.0, &[("class", status_class(status))]);
                let fault = if is_retryable_http_status(status) {
                    Fault::Ambiguous
                } else {
                    Fault::Permanent
                };
                // The body names the offending series in a Prometheus-style `400` (`out of order
                // sample`, a bad label): the only actionable part. Read bounded.
                let body = read_body_prefix(response, ERROR_BODY_SNIPPET_BYTES).await;
                let snippet = body_snippet(&body, ERROR_BODY_SNIPPET_BYTES);
                self.diag.warn_throttled(
                    "remote_write_rejected",
                    format_args!("remote-write to {} failed ({status}): {snippet}", self.endpoint),
                );
                Err(anyhow::anyhow!("remote-write failed ({status}): {snippet}")).context(fault)
            }
            Err(err) => {
                self.telemetry.count(REQUESTS, 1.0, &[("class", "network_error")]);
                let fault = classify_reqwest_error(&err);
                Err(anyhow::Error::new(err)).context(fault)
            }
        }
    }

    /// Nothing is buffered: `send` has issued its request, if any, before returning.
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// `true`. A sample's identity at a receiver is `(label set, timestamp)` and a retry
    /// re-encodes the same events, so a replay is an idempotent overwrite. `true` also selects
    /// `DeliveryPosture::AtLeastOnce` (`logit_pipeline::output`'s `from_duplicate_safe`), the only
    /// posture that retries a 5xx's `Fault::Ambiguous`; `false` would drop every 5xx batch.
    fn duplicate_safe(&self) -> bool {
        true
    }
}

/// Recovers a poisoned lock rather than propagating it, as `logit_core::Registry` does: a panic
/// under the registry or encoder lock still leaves a valid value, and the next `send` upserts
/// over the registry.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The accept loop, as in `logit-cli::admin::serve_on`: a task per connection, the permit taken
/// after accept so the kernel backlog absorbs a burst, and a per-connection timeout. Returns `()`:
/// the task is only aborted, never joined, so an `Err` would go unread.
async fn serve(listener: TcpListener, state: Arc<ServerState>, mut diag: Diagnostics) {
    let connection_limit = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _peer)) => stream,
            Err(err) => {
                // A failed `accept()` never ends the loop (see admin.rs). A client's accident
                // retries at once; anything else (fd pressure) pauses so it can't spin a core.
                diag.warn_throttled(
                    "prometheus_accept_failed",
                    format_args!("accepting a scrape connection failed: {err}"),
                );
                if !matches!(
                    err.kind(),
                    ErrorKind::ConnectionAborted
                        | ErrorKind::ConnectionReset
                        | ErrorKind::Interrupted
                ) {
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                }
                continue;
            }
        };
        let permit =
            connection_limit.clone().acquire_owned().await.expect("this semaphore is never closed");
        let state = Arc::clone(&state);
        let response_deadline = state.response_timeout;
        tokio::spawn(async move {
            let _permit = permit; // held for the connection's lifetime; released on drop
            let svc = service_fn(move |req| {
                let state = Arc::clone(&state);
                async move { handle(req, state) }
            });
            // Two deadlines: hyper's tight one on request headers (slowloris), and the outer one
            // on the whole connection, which scales with `max_series` to outlast the scraper's
            // `scrape_timeout`.
            let serve = hyper::server::conn::http1::Builder::new()
                .timer(hyper_util::rt::TokioTimer::new())
                .header_read_timeout(HEADER_READ_TIMEOUT)
                .serve_connection(TokioIo::new(stream), svc);
            let _ = tokio::time::timeout(response_deadline, serve).await;
        });
    }
}

fn handle(
    req: http::Request<hyper::body::Incoming>,
    state: Arc<ServerState>,
) -> Result<http::Response<Full<Bytes>>, std::convert::Infallible> {
    if req.uri().path() != state.path {
        state.telemetry.count(SCRAPES, 1.0, &[("class", "not_found")]);
        return Ok(text_response(StatusCode::NOT_FOUND, "not found"));
    }
    if !matches!(*req.method(), Method::GET | Method::HEAD) {
        state.telemetry.count(SCRAPES, 1.0, &[("class", "method")]);
        return Ok(http::Response::builder()
            .status(StatusCode::METHOD_NOT_ALLOWED)
            .header("allow", "GET, HEAD")
            .header("content-type", "text/plain; charset=utf-8")
            .body(Full::new(Bytes::from_static(b"method not allowed")))
            .expect("a well-formed response always builds"));
    }

    let dialect = negotiate(req.headers().get(http::header::ACCEPT).and_then(header_str));
    let gzip = accepts_gzip(req.headers().get(http::header::ACCEPT_ENCODING).and_then(header_str));

    // Sweep here too, so a quiet sink still expires series. One `Instant` compare per series;
    // both locks are released before the response is built.
    let body = {
        let mut encoder = lock(&state.encoder);
        let mut registry = lock(&state.registry);
        registry.sweep(state.expire_after, (state.clock)(), &state.telemetry);
        registry.render(dialect, &mut encoder)
    };

    let body = if gzip { gzip_encode(&body) } else { body };
    // Counted at render; see `SCRAPE_BYTES`.
    state.telemetry.count(SCRAPES, 1.0, &[("class", "ok")]);
    state.telemetry.count(SCRAPE_BYTES, body.len() as f64, &[]);

    let mut builder = http::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", dialect.content_type())
        // Operators are told to front this with a proxy; without `Vary` a cache may hand a
        // text-0.0.4 scraper an OpenMetrics or gzipped body.
        .header("vary", "Accept, Accept-Encoding");
    if gzip {
        builder = builder.header("content-encoding", "gzip");
    }
    Ok(builder.body(Full::new(Bytes::from(body))).expect("a well-formed response always builds"))
}

/// A header value as `&str`, or `None` if it isn't visible ASCII. An unreadable `Accept` counts
/// as absent (text 0.0.4): a scrape never fails over a negotiation header.
fn header_str(value: &http::HeaderValue) -> Option<&str> {
    value.to_str().ok()
}

/// OpenMetrics only when asked for by name; text 0.0.4 otherwise, as Prometheus's own server
/// does. A substring match, not an `Accept` parse: no real scraper weights the two dialects.
fn negotiate(accept: Option<&str>) -> Dialect {
    const OM: &str = "application/openmetrics-text";
    match accept {
        Some(value) if contains_ignore_ascii_case(value, OM) => Dialect::OpenMetrics1_0,
        _ => Dialect::Text0_0_4,
    }
}

fn contains_ignore_ascii_case(haystack: &str, needle: &str) -> bool {
    let (haystack, needle) = (haystack.as_bytes(), needle.as_bytes());
    haystack.windows(needle.len()).any(|window| window.eq_ignore_ascii_case(needle))
}

/// Whether `Accept-Encoding` offers `gzip` at a non-zero weight; `gzip;q=0` is a refusal (RFC
/// 9110 §12.5.3).
fn accepts_gzip(accept_encoding: Option<&str>) -> bool {
    let Some(value) = accept_encoding else { return false };
    value.split(',').any(|entry| {
        let mut parts = entry.split(';');
        let token = parts.next().unwrap_or("").trim();
        if !token.eq_ignore_ascii_case("gzip") {
            return false;
        }
        !parts.any(|param| {
            let param = param.trim();
            param.strip_prefix("q=").is_some_and(|q| q.trim().parse::<f32>() == Ok(0.0))
        })
    })
}

fn gzip_encode(body: &[u8]) -> Vec<u8> {
    use std::io::Write as _;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    // Writing into a `Vec` can't fail, so the handler gets no dead error branch.
    encoder.write_all(body).expect("writing into a Vec never fails");
    encoder.finish().expect("finishing a Vec-backed gzip stream never fails")
}

fn text_response(status: StatusCode, body: &'static str) -> http::Response<Full<Bytes>> {
    http::Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::from_static(body.as_bytes())))
        .expect("a well-formed response always builds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{
        interner::intern, AttrMap, Event, Histogram, MetricKind, MetricRecord, Resource, Sum,
        Summary, Temporality, Value,
    };
    use logit_proto::prometheus::{ATTR_TIMESTAMP, ATTR_TYPE, LABEL_INSTANCE};
    // The vendored prompb types, so a sender test asserts against the *message* a receiver would
    // decode rather than against this codec's own view of it (`crates/logit-proto/proto/`).
    use logit_proto::prometheus::generated::io::prometheus::write::v2 as pb2;
    use logit_proto::prometheus::generated::prometheus as pb1;
    use prost::Message as _;

    // -- fixtures ---------------------------------------------------------------------------

    fn metric_event(name: &str, kind: MetricKind, attrs: &[(&str, Value)]) -> Event {
        let mut map = AttrMap::new();
        for (key, value) in attrs {
            map.insert(key, value.clone());
        }
        Event::metric(0, map, MetricRecord::new(intern(name), kind))
    }

    fn batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn batch_with_resource(attrs: &[(&str, Value)], events: Vec<Event>) -> EventBatch {
        let mut resource = Resource::default();
        for (key, value) in attrs {
            resource.attributes.insert(key, value.clone());
        }
        EventBatch { resource: Arc::new(resource), scope: None, events }
    }

    fn cumulative_counter(value: f64) -> MetricKind {
        MetricKind::Sum(Sum { value, temporality: Temporality::Cumulative, monotonic: true })
    }

    /// One of every shape the exposition format carries, rendered by every byte-exact test: a
    /// cumulative counter whose model name lacks `_total`, a gauge, a cumulative histogram, a
    /// summary, an `info`-typed gauge, and a series with its own wire timestamp.
    fn fixture_batch() -> EventBatch {
        let mut histogram = MetricRecord::new(
            intern("latency_seconds"),
            MetricKind::Histogram(Histogram {
                buckets: vec![(0.5, 2), (1.0, 1), (f64::INFINITY, 1)],
                sum: Some(1.5),
                min: None,
                max: None,
                temporality: Temporality::Cumulative,
            }),
        );
        histogram.unit = Some(intern("seconds"));
        histogram.description = Some(intern("request latency"));

        let summary = MetricRecord::new(
            intern("gc_pause"),
            MetricKind::Summary(Summary {
                quantiles: vec![(0.5, 0.1), (0.99, 0.4)],
                count: 7,
                sum: 1.25,
            }),
        );

        batch_with_resource(
            &[(LABEL_INSTANCE, Value::str("node-a:9100"))],
            vec![
                metric_event("http_requests", cumulative_counter(5.0), &[]),
                metric_event("temperature", MetricKind::Gauge(21.5), &[]),
                Event::metric(0, AttrMap::new(), histogram),
                Event::metric(0, AttrMap::new(), summary),
                metric_event(
                    "build",
                    MetricKind::Gauge(1.0),
                    &[(ATTR_TYPE, Value::str("info")), ("version", Value::str("1.2.3"))],
                ),
                Event::metric(
                    1_700_000_000_500_000_000,
                    {
                        let mut map = AttrMap::new();
                        map.insert(ATTR_TIMESTAMP, Value::Bool(true));
                        map
                    },
                    MetricRecord::new(intern("stamped"), MetricKind::Gauge(3.0)),
                ),
            ],
        )
    }

    /// A bound sink plus the base URL to scrape it at.
    async fn bound(sink: ExposeOutput) -> (ExposeOutput, String) {
        let mut sink = sink;
        sink.bind().await.expect("binding an ephemeral port should succeed");
        let addr = sink.local_addr().expect("bind records the address");
        (sink, format!("http://{addr}"))
    }

    /// [`fixture_batch`] sent to a bound sink, with `expire_after: 0s` so nothing expires out from
    /// under a byte-exact assertion.
    async fn fixture_sink() -> (ExposeOutput, String) {
        let mut sink = ExposeOutput::new("127.0.0.1:0").with_expire_after(Duration::ZERO);
        sink.send(&fixture_batch()).await.expect("send never fails");
        bound(sink).await
    }

    async fn get(url: &str, headers: &[(&str, &str)]) -> reqwest::Response {
        let client = reqwest::Client::builder()
            // So a gzip body arrives as written, not inflated with `content-encoding` stripped.
            .no_gzip()
            .build()
            .expect("a default client always builds");
        let mut req = client.get(url);
        for (name, value) in headers {
            req = req.header(*name, *value);
        }
        req.send().await.expect("the scrape request should reach the sink")
    }

    const OM_ACCEPT: &str = "application/openmetrics-text;version=1.0.0,text/plain;q=0.5";

    // -- byte-exact exposition --------------------------------------------------------------

    /// The text 0.0.4 rendering of [`fixture_batch`], written out by hand from the ADR's rules:
    /// families sorted by name; `# HELP` before `# TYPE`; the counter's sample *and* family name
    /// carry `_total`; no `# UNIT` and no `_created` (text has neither); `info` renders as a
    /// `gauge` whose family and sample are both `build_info`; `instance` from the resource appears
    /// on every series; the stamped series carries integer milliseconds; no `# EOF`.
    const EXPECTED_TEXT: &str = concat!(
        "# TYPE build_info gauge\n",
        "build_info{instance=\"node-a:9100\",version=\"1.2.3\"} 1\n",
        "# TYPE gc_pause summary\n",
        "gc_pause{instance=\"node-a:9100\",quantile=\"0.5\"} 0.1\n",
        "gc_pause{instance=\"node-a:9100\",quantile=\"0.99\"} 0.4\n",
        "gc_pause_sum{instance=\"node-a:9100\"} 1.25\n",
        "gc_pause_count{instance=\"node-a:9100\"} 7\n",
        "# TYPE http_requests_total counter\n",
        "http_requests_total{instance=\"node-a:9100\"} 5\n",
        "# HELP latency_seconds request latency\n",
        "# TYPE latency_seconds histogram\n",
        "latency_seconds_bucket{instance=\"node-a:9100\",le=\"0.5\"} 2\n",
        "latency_seconds_bucket{instance=\"node-a:9100\",le=\"1\"} 3\n",
        "latency_seconds_bucket{instance=\"node-a:9100\",le=\"+Inf\"} 4\n",
        "latency_seconds_sum{instance=\"node-a:9100\"} 1.5\n",
        "latency_seconds_count{instance=\"node-a:9100\"} 4\n",
        "# TYPE stamped gauge\n",
        "stamped{instance=\"node-a:9100\"} 3 1700000000500\n",
        "# TYPE temperature gauge\n",
        "temperature{instance=\"node-a:9100\"} 21.5\n",
    );

    /// The OpenMetrics 1.0 rendering of the same registry: `# TYPE`, then `# UNIT`, then `# HELP`;
    /// the counter family loses its `_total` while the sample keeps it; `info` keeps its own type
    /// with an `_info`-suffixed sample; the timestamp is decimal seconds; a trailing `# EOF`.
    const EXPECTED_OPENMETRICS: &str = concat!(
        "# TYPE build info\n",
        "build_info{instance=\"node-a:9100\",version=\"1.2.3\"} 1\n",
        "# TYPE gc_pause summary\n",
        "gc_pause{instance=\"node-a:9100\",quantile=\"0.5\"} 0.1\n",
        "gc_pause{instance=\"node-a:9100\",quantile=\"0.99\"} 0.4\n",
        "gc_pause_sum{instance=\"node-a:9100\"} 1.25\n",
        "gc_pause_count{instance=\"node-a:9100\"} 7\n",
        "# TYPE http_requests counter\n",
        "http_requests_total{instance=\"node-a:9100\"} 5\n",
        "# TYPE latency_seconds histogram\n",
        "# UNIT latency_seconds seconds\n",
        "# HELP latency_seconds request latency\n",
        "latency_seconds_bucket{instance=\"node-a:9100\",le=\"0.5\"} 2\n",
        "latency_seconds_bucket{instance=\"node-a:9100\",le=\"1\"} 3\n",
        "latency_seconds_bucket{instance=\"node-a:9100\",le=\"+Inf\"} 4\n",
        "latency_seconds_sum{instance=\"node-a:9100\"} 1.5\n",
        "latency_seconds_count{instance=\"node-a:9100\"} 4\n",
        "# TYPE stamped gauge\n",
        "stamped{instance=\"node-a:9100\"} 3 1700000000.5\n",
        "# TYPE temperature gauge\n",
        "temperature{instance=\"node-a:9100\"} 21.5\n",
        "# EOF\n",
    );

    #[tokio::test]
    async fn a_scrape_with_no_accept_header_serves_text_0_0_4_byte_for_byte() {
        let (_sink, url) = fixture_sink().await;
        let response = get(&format!("{url}/metrics"), &[]).await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()[http::header::CONTENT_TYPE],
            "text/plain; version=0.0.4; charset=utf-8"
        );
        assert_eq!(
            response.headers()[http::header::VARY],
            "Accept, Accept-Encoding",
            "the representation varies on both, and a caching proxy has to know"
        );
        assert_eq!(response.text().await.unwrap(), EXPECTED_TEXT);
    }

    #[tokio::test]
    async fn a_scrape_asking_for_openmetrics_serves_openmetrics_byte_for_byte() {
        let (_sink, url) = fixture_sink().await;
        let response = get(&format!("{url}/metrics"), &[("accept", OM_ACCEPT)]).await;
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()[http::header::CONTENT_TYPE],
            "application/openmetrics-text; version=1.0.0; charset=utf-8"
        );
        assert_eq!(response.headers()[http::header::VARY], "Accept, Accept-Encoding");
        assert_eq!(response.text().await.unwrap(), EXPECTED_OPENMETRICS);
    }

    /// A bare `*/*` is not an OpenMetrics request -- it's what a client that doesn't care sends,
    /// and the format such a client can definitely read is text 0.0.4.
    #[tokio::test]
    async fn a_star_accept_header_serves_text_0_0_4() {
        let (_sink, url) = fixture_sink().await;
        let response = get(&format!("{url}/metrics"), &[("accept", "*/*")]).await;
        assert_eq!(response.text().await.unwrap(), EXPECTED_TEXT);
    }

    /// RFC 9110 §9.3.2: the same headers a `GET` would have sent, including `content-length`, and
    /// no body.
    #[tokio::test]
    async fn head_returns_the_get_headers_and_no_body() {
        let (_sink, url) = fixture_sink().await;
        let client = reqwest::Client::new();
        let response = client
            .head(format!("{url}/metrics"))
            .send()
            .await
            .expect("the HEAD request should reach the sink");
        assert_eq!(response.status(), 200);
        assert_eq!(
            response.headers()[http::header::CONTENT_LENGTH],
            EXPECTED_TEXT.len().to_string().as_str()
        );
        assert_eq!(
            response.headers()[http::header::VARY],
            "Accept, Accept-Encoding",
            "HEAD sends the headers a GET would, Vary included"
        );
        assert!(response.bytes().await.unwrap().is_empty(), "a HEAD response has no body");
    }

    #[tokio::test]
    async fn gzip_is_offered_only_when_asked_for_and_inflates_to_the_same_bytes() {
        use std::io::Read as _;
        let (_sink, url) = fixture_sink().await;
        let response =
            get(&format!("{url}/metrics"), &[("accept-encoding", "gzip, deflate")]).await;
        assert_eq!(response.headers()[http::header::CONTENT_ENCODING], "gzip");
        let compressed = response.bytes().await.unwrap();
        let mut inflated = String::new();
        flate2::read::GzDecoder::new(&compressed[..])
            .read_to_string(&mut inflated)
            .expect("the body should be a valid gzip stream");
        assert_eq!(inflated, EXPECTED_TEXT);

        let plain = get(&format!("{url}/metrics"), &[]).await;
        assert!(
            !plain.headers().contains_key(http::header::CONTENT_ENCODING),
            "a client that didn't ask for gzip must not get it"
        );
    }

    /// `gzip;q=0` is an explicit refusal, not an offer.
    #[tokio::test]
    async fn a_gzip_q_zero_accept_encoding_is_not_an_offer() {
        let (_sink, url) = fixture_sink().await;
        let response = get(&format!("{url}/metrics"), &[("accept-encoding", "gzip;q=0")]).await;
        assert!(!response.headers().contains_key(http::header::CONTENT_ENCODING));
        assert_eq!(response.text().await.unwrap(), EXPECTED_TEXT);
    }

    #[tokio::test]
    async fn another_path_is_a_404() {
        let (_sink, url) = fixture_sink().await;
        assert_eq!(get(&format!("{url}/nope"), &[]).await.status(), 404);
    }

    #[tokio::test]
    async fn a_custom_path_is_served_and_the_default_one_is_not() {
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_path("/exposed")
            .with_expire_after(Duration::ZERO);
        sink.send(&fixture_batch()).await.unwrap();
        let (_sink, url) = bound(sink).await;
        assert_eq!(get(&format!("{url}/exposed"), &[]).await.text().await.unwrap(), EXPECTED_TEXT);
        assert_eq!(get(&format!("{url}/metrics"), &[]).await.status(), 404);
    }

    #[tokio::test]
    async fn another_method_on_the_metrics_path_is_a_405_naming_the_allowed_ones() {
        let (_sink, url) = fixture_sink().await;
        let response = reqwest::Client::new()
            .post(format!("{url}/metrics"))
            .send()
            .await
            .expect("the POST should reach the sink");
        assert_eq!(response.status(), 405);
        assert_eq!(response.headers()[http::header::ALLOW], "GET, HEAD");
    }

    // -- state: upsert, expiry, cardinality, type conflict ----------------------------------

    /// A second delivery of a series replaces the first: no accumulation, no duplicate.
    #[tokio::test]
    async fn a_resent_series_replaces_its_stored_value_rather_than_adding_to_it() {
        let mut sink = ExposeOutput::new("127.0.0.1:0").with_expire_after(Duration::ZERO);
        sink.send(&batch(vec![metric_event("hits", cumulative_counter(1.0), &[])])).await.unwrap();
        sink.send(&batch(vec![metric_event("hits", cumulative_counter(9.0), &[])])).await.unwrap();
        let (_sink, url) = bound(sink).await;
        assert_eq!(
            get(&format!("{url}/metrics"), &[]).await.text().await.unwrap(),
            "# TYPE hits_total counter\nhits_total 9\n"
        );
    }

    /// At exactly `expire_after` a series is live; past it, it's gone and counted.
    #[tokio::test]
    async fn a_series_not_updated_within_expire_after_stops_being_exposed_and_is_counted() {
        let start = Instant::now();
        let now = Arc::new(Mutex::new(start));
        let clock_handle = Arc::clone(&now);
        let registry = logit_core::Registry::new();
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_expire_after(Duration::from_secs(60))
            .with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"))
            .with_clock(Arc::new(move || *lock(&clock_handle)));
        sink.send(&batch(vec![metric_event("hits", cumulative_counter(1.0), &[])])).await.unwrap();
        let (_sink, url) = bound(sink).await;

        *lock(&now) = start + Duration::from_secs(60);
        assert_eq!(
            get(&format!("{url}/metrics"), &[]).await.text().await.unwrap(),
            "# TYPE hits_total counter\nhits_total 1\n",
            "at exactly expire_after the series is still live"
        );

        *lock(&now) = start + Duration::from_secs(61);
        assert_eq!(
            get(&format!("{url}/metrics"), &[]).await.text().await.unwrap(),
            "",
            "past expire_after the series -- and its now-empty family -- is gone"
        );
        assert_eq!(counter(&registry, SERIES_EVICTED, "reason", "expired"), 1.0);
    }

    /// `0s` means "never expire", not "expire immediately".
    #[tokio::test]
    async fn expire_after_zero_disables_expiry_entirely() {
        let start = Instant::now();
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_expire_after(Duration::ZERO)
            .with_clock(Arc::new(move || start + Duration::from_secs(86_400)));
        sink.send(&batch(vec![metric_event("hits", cumulative_counter(1.0), &[])])).await.unwrap();
        let (_sink, url) = bound(sink).await;
        assert_eq!(
            get(&format!("{url}/metrics"), &[]).await.text().await.unwrap(),
            "# TYPE hits_total counter\nhits_total 1\n"
        );
    }

    /// The cap evicts the least recently updated series. The oldest is `id="z"`, last in map
    /// order, so evicting by iteration order would fail.
    #[tokio::test]
    async fn the_max_series_cap_evicts_the_least_recently_updated_series_and_counts_it() {
        let start = Instant::now();
        let now = Arc::new(Mutex::new(start));
        let clock_handle = Arc::clone(&now);
        let registry = logit_core::Registry::new();
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_expire_after(Duration::ZERO)
            .with_max_series(2)
            .with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"))
            .with_clock(Arc::new(move || *lock(&clock_handle)));

        sink.send(&batch(vec![metric_event(
            "hits",
            cumulative_counter(1.0),
            &[("id", Value::str("z"))],
        )]))
        .await
        .unwrap();
        *lock(&now) = start + Duration::from_secs(1);
        sink.send(&batch(vec![
            metric_event("hits", cumulative_counter(2.0), &[("id", Value::str("a"))]),
            metric_event("hits", cumulative_counter(3.0), &[("id", Value::str("b"))]),
        ]))
        .await
        .unwrap();

        let (_sink, url) = bound(sink).await;
        assert_eq!(
            get(&format!("{url}/metrics"), &[]).await.text().await.unwrap(),
            concat!(
                "# TYPE hits_total counter\n",
                "hits_total{id=\"a\"} 2\n",
                "hits_total{id=\"b\"} 3\n",
            ),
            "the oldest series goes even though it sorts last in the map"
        );
        assert_eq!(counter(&registry, SERIES_EVICTED, "reason", "cardinality"), 1.0);
    }

    /// Thousands of series, hundreds over the cap, evicted in one pass: pins which go (the
    /// oldest, then by key), not only how many.
    #[tokio::test]
    async fn a_batch_far_over_the_cap_evicts_exactly_the_oldest_series_in_one_pass() {
        const OLD: usize = 3_000;
        const NEW: usize = 500;
        const CAP: usize = 3_000;

        fn series(prefix: &str, count: usize) -> Vec<Event> {
            (0..count)
                .map(|i| {
                    metric_event(
                        "hits",
                        cumulative_counter(i as f64),
                        &[("id", Value::str(format!("{prefix}_{i:05}")))],
                    )
                })
                .collect()
        }

        let start = Instant::now();
        let now = Arc::new(Mutex::new(start));
        let clock_handle = Arc::clone(&now);
        let registry = logit_core::Registry::new();
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_expire_after(Duration::ZERO)
            .with_max_series(CAP)
            .with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"))
            .with_clock(Arc::new(move || *lock(&clock_handle)));

        // The `old_*` series fill the cap exactly, so nothing is evicted yet.
        sink.send(&batch(series("old", OLD))).await.unwrap();
        assert_eq!(counter(&registry, SERIES_EVICTED, "reason", "cardinality"), 0.0);

        // NEW fresh series take the total to CAP + NEW, and every `old_*` is older than them.
        *lock(&now) = start + Duration::from_secs(1);
        sink.send(&batch(series("new", NEW))).await.unwrap();
        assert_eq!(counter(&registry, SERIES_EVICTED, "reason", "cardinality"), NEW as f64);

        let (_sink, url) = bound(sink).await;
        let body = get(&format!("{url}/metrics"), &[]).await.text().await.unwrap();
        assert_eq!(body.lines().filter(|l| l.starts_with("hits_total{")).count(), CAP);
        // The `old_*` share one `updated_at`, so the key tie-break picks the NEW lowest keys.
        for i in 0..NEW {
            assert!(
                !body.contains(&format!("id=\"old_{i:05}\"")),
                "old_{i:05} is among the NEW oldest and should have been evicted"
            );
        }
        for i in NEW..OLD {
            assert!(
                body.contains(&format!("id=\"old_{i:05}\"")),
                "old_{i:05} should have survived"
            );
        }
        for i in 0..NEW {
            assert!(body.contains(&format!("id=\"new_{i:05}\"")), "new_{i:05} is the freshest");
        }
    }

    /// A gauge arriving for a counter's name replaces the family and its old series.
    #[tokio::test]
    async fn a_type_conflict_replaces_the_family_and_evicts_its_old_series() {
        let registry = logit_core::Registry::new();
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_expire_after(Duration::ZERO)
            .with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"));
        sink.send(&batch(vec![metric_event(
            "x",
            cumulative_counter(1.0),
            &[("id", Value::str("old"))],
        )]))
        .await
        .unwrap();
        sink.send(&batch(vec![metric_event("x", MetricKind::Gauge(2.0), &[])])).await.unwrap();

        let (_sink, url) = bound(sink).await;
        assert_eq!(
            get(&format!("{url}/metrics"), &[]).await.text().await.unwrap(),
            "# TYPE x gauge\nx 2\n",
            "the counter family, and every series under it, is replaced"
        );
        assert_eq!(counter(&registry, TYPE_CONFLICT, "component", "out"), 1.0);
    }

    /// The sink's encoder is wired to the component's `Telemetry`, so codec skips are counted.
    #[tokio::test]
    async fn a_delta_sum_is_skipped_and_counted_through_the_sinks_own_telemetry() {
        let registry = logit_core::Registry::new();
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_expire_after(Duration::ZERO)
            .with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"));
        sink.send(&batch(vec![metric_event(
            "hits",
            MetricKind::Sum(Sum { value: 3.0, temporality: Temporality::Delta, monotonic: true }),
            &[],
        )]))
        .await
        .expect("a skipped record is not a send failure");

        let (_sink, url) = bound(sink).await;
        assert_eq!(
            get(&format!("{url}/metrics"), &[]).await.text().await.unwrap(),
            "",
            "a delta Sum never reaches the exposition"
        );
        assert_eq!(
            counter(&registry, "logit.output.metrics.skipped", "metric_kind", "delta_sum"),
            1.0
        );
    }

    /// A render's drops count on the sink's encoder: one `_total` line fits one exemplar, so the
    /// second is dropped and counted. OpenMetrics, since text 0.0.4 drops exemplars uncounted by
    /// dialect.
    #[tokio::test]
    async fn a_render_that_has_to_drop_an_exemplar_counts_it_under_the_sinks_own_telemetry() {
        let registry = logit_core::Registry::new();
        let mut record = MetricRecord::new(intern("hits"), cumulative_counter(2.0));
        record.exemplars = vec![
            Exemplar { timestamp: 0, value: 1.0, trace: None, filtered_attributes: AttrMap::new() },
            Exemplar { timestamp: 0, value: 2.0, trace: None, filtered_attributes: AttrMap::new() },
        ];
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_expire_after(Duration::ZERO)
            .with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"));
        sink.send(&batch(vec![Event::metric(0, AttrMap::new(), record)])).await.unwrap();
        let (_sink, url) = bound(sink).await;

        let body = get(&format!("{url}/metrics"), &[("accept", OM_ACCEPT)]).await;
        let body = body.text().await.unwrap();
        assert_eq!(
            body.matches(" # {} ").count(),
            1,
            "exactly one exemplar reaches the wire: {body}"
        );
        assert_eq!(
            counter(&registry, "logit.output.metrics.degraded", "reason", "exemplar_dropped"),
            1.0,
            "the one that had no line to sit on is counted"
        );
    }

    // -- lifecycle --------------------------------------------------------------------------

    /// A second `bind` neither reopens the port nor starts a second accept loop.
    #[tokio::test]
    async fn binding_twice_is_a_no_op_that_keeps_the_first_address() {
        let mut sink = ExposeOutput::new("127.0.0.1:0");
        sink.bind().await.expect("the first bind should succeed");
        let first = sink.local_addr().expect("bind records the address");
        sink.bind().await.expect("a second bind must be a harmless no-op");
        assert_eq!(sink.local_addr(), Some(first));
    }

    #[tokio::test]
    async fn binding_an_address_already_in_use_fails_with_the_address_in_the_message() {
        let mut first = ExposeOutput::new("127.0.0.1:0");
        first.bind().await.unwrap();
        let addr = first.local_addr().unwrap();
        let err =
            ExposeOutput::new(addr.to_string()).bind().await.expect_err("the port is already held");
        assert!(err.to_string().contains(&addr.to_string()), "got: {err}");
    }

    /// `flush` closes the port.
    #[tokio::test]
    async fn flush_stops_the_server_and_closes_the_port() {
        let (mut sink, url) = fixture_sink().await;
        assert_eq!(get(&format!("{url}/metrics"), &[]).await.status(), 200);
        sink.flush().await.expect("flush never fails");
        let refused = reqwest::Client::new().get(format!("{url}/metrics")).send().await;
        assert!(refused.is_err(), "the port should be closed after flush, got {refused:?}");
    }

    /// Dropping a bound sink without `flush`, as the startup-failure path does, frees the port.
    #[tokio::test]
    async fn dropping_an_unflushed_bound_sink_closes_the_port() {
        let (sink, url) = bound(ExposeOutput::new("127.0.0.1:0")).await;
        assert_eq!(get(&format!("{url}/metrics"), &[]).await.status(), 200);

        drop(sink); // no flush, exactly as a startup failure would
                    // The abort takes effect asynchronously, so retry until connecting fails.
        let mut refused = None;
        for _ in 0..100 {
            tokio::task::yield_now().await;
            match reqwest::Client::new().get(format!("{url}/metrics")).send().await {
                Err(err) => {
                    refused = Some(err);
                    break;
                }
                Ok(_) => continue,
            }
        }
        assert!(refused.is_some(), "dropping the sink should have closed the port");
    }

    /// Before any delivery a scrape gets an empty `200`: "up, no data", not "not listening".
    #[tokio::test]
    async fn a_scrape_before_any_batch_arrives_is_an_empty_200() {
        let (_sink, url) = bound(ExposeOutput::new("127.0.0.1:0")).await;
        let response = get(&format!("{url}/metrics"), &[]).await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "");
    }

    #[tokio::test]
    async fn send_is_duplicate_safe_because_it_only_replaces_in_memory_state() {
        assert!(ExposeOutput::new("127.0.0.1:0").duplicate_safe());
    }

    // -- telemetry --------------------------------------------------------------------------

    #[tokio::test]
    async fn each_request_is_counted_by_outcome_class_with_the_bytes_it_served() {
        let registry = logit_core::Registry::new();
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_expire_after(Duration::ZERO)
            .with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"));
        sink.send(&batch(vec![metric_event("hits", cumulative_counter(1.0), &[])])).await.unwrap();
        let (_sink, url) = bound(sink).await;

        let body = get(&format!("{url}/metrics"), &[]).await.text().await.unwrap();
        get(&format!("{url}/nope"), &[]).await;
        reqwest::Client::new().post(format!("{url}/metrics")).send().await.unwrap();

        let drained = registry.drain(0);
        assert_eq!(tagged(&drained, SCRAPES, "class", "ok"), 1.0);
        assert_eq!(tagged(&drained, SCRAPES, "class", "not_found"), 1.0);
        assert_eq!(tagged(&drained, SCRAPES, "class", "method"), 1.0);
        assert_eq!(tagged(&drained, SCRAPE_BYTES, "component", "out"), body.len() as f64);
    }

    #[tokio::test]
    async fn the_series_gauge_reports_what_the_registry_holds_after_each_send() {
        let registry = logit_core::Registry::new();
        let mut sink = ExposeOutput::new("127.0.0.1:0")
            .with_expire_after(Duration::ZERO)
            .with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"));
        sink.send(&batch(vec![
            metric_event("hits", cumulative_counter(1.0), &[("id", Value::str("a"))]),
            metric_event("hits", cumulative_counter(2.0), &[("id", Value::str("b"))]),
        ]))
        .await
        .unwrap();
        let drained = registry.drain(0);
        let gauges: Vec<f64> = drained
            .iter()
            .flat_map(|e| &e.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == SERIES)
            .map(|m| match m.kind {
                MetricKind::Gauge(v) => v,
                ref other => panic!("{SERIES} must be a gauge, got {other:?}"),
            })
            .collect();
        assert_eq!(gauges, vec![2.0]);
    }

    // -- pure helpers -----------------------------------------------------------------------

    #[test]
    fn accept_negotiation_matches_the_module_docs_table() {
        assert_eq!(negotiate(None), Dialect::Text0_0_4);
        assert_eq!(negotiate(Some("*/*")), Dialect::Text0_0_4);
        assert_eq!(negotiate(Some("text/plain;version=0.0.4")), Dialect::Text0_0_4);
        assert_eq!(negotiate(Some(OM_ACCEPT)), Dialect::OpenMetrics1_0);
        assert_eq!(
            negotiate(Some("Application/OpenMetrics-Text")),
            Dialect::OpenMetrics1_0,
            "media types are case-insensitive"
        );
    }

    #[test]
    fn gzip_is_accepted_only_on_a_non_zero_weight_offer() {
        assert!(!accepts_gzip(None));
        assert!(accepts_gzip(Some("gzip")));
        assert!(accepts_gzip(Some("br, gzip;q=0.9")));
        assert!(accepts_gzip(Some("GZIP")));
        assert!(!accepts_gzip(Some("deflate")));
        assert!(!accepts_gzip(Some("gzip;q=0")));
        assert!(!accepts_gzip(Some("gzip;q=0.0")));
    }

    // -- assertion helpers ------------------------------------------------------------------

    /// Sums `name`'s counter points tagged `tag=value` in an already-drained event list.
    fn tagged(events: &[Event], name: &str, tag: &str, value: &str) -> f64 {
        events
            .iter()
            .filter(|e| e.attributes.get(tag).and_then(|v| v.as_str()) == Some(value))
            .flat_map(|e| &e.metrics)
            .filter(|m| logit_core::interner::resolve(m.name) == name)
            .map(|m| match m.kind {
                MetricKind::Sum(ref s) => s.value,
                MetricKind::Gauge(v) => v,
                ref other => panic!("{name} should be a counter or gauge, got {other:?}"),
            })
            .sum()
    }

    fn counter(registry: &logit_core::Registry, name: &str, tag: &str, value: &str) -> f64 {
        tagged(&registry.drain(0), name, tag, value)
    }

    // -- sender mode: a canned remote-write receiver ------------------------------------------

    /// One request as a canned receiver saw it, independent of the sink's code.
    #[derive(Debug, Clone)]
    struct CapturedRequest {
        method: Method,
        path: String,
        headers: http::HeaderMap,
        /// Still Snappy-compressed, as it arrived.
        body: Vec<u8>,
    }

    impl CapturedRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers.get(name).and_then(|v| v.to_str().ok())
        }

        /// The body decompressed as Snappy block format, so decoding at all proves the sink didn't
        /// use the framed format.
        fn decompressed(&self) -> Vec<u8> {
            snap::raw::Decoder::new()
                .decompress_vec(&self.body)
                .expect("the body should be a snappy block")
        }

        fn as_v1(&self) -> pb1::WriteRequest {
            pb1::WriteRequest::decode(self.decompressed().as_slice())
                .expect("the body should decode as prometheus.WriteRequest")
        }

        fn as_v2(&self) -> pb2::Request {
            pb2::Request::decode(self.decompressed().as_slice())
                .expect("the body should decode as io.prometheus.write.v2.Request")
        }
    }

    /// What a canned receiver answers with.
    #[derive(Clone, Copy)]
    enum Canned {
        /// A status, a fixed body, and an optional `Location` header.
        Fixed(StatusCode, &'static str, Option<&'static str>),
        /// A status whose body never ends, which only a bounded read survives.
        Endless(StatusCode),
    }

    type CannedBody = http_body_util::combinators::BoxBody<Bytes, std::convert::Infallible>;

    const ENDLESS_CHUNK: &[u8] = &[b'x'; 1024];

    /// A body that never ends: `poll_frame` always has another kilobyte. A bounded reader just
    /// drops the connection.
    struct EndlessBody;

    impl hyper::body::Body for EndlessBody {
        type Data = Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<hyper::body::Frame<Bytes>, Self::Error>>> {
            std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(Bytes::from_static(
                ENDLESS_CHUNK,
            )))))
        }
    }

    /// A real HTTP/1.1 receiver answering `canned` and recording every request, since these tests
    /// assert on the request's headers, path, and decoded body.
    async fn canned_receiver(
        status: StatusCode,
        body: &'static str,
    ) -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
        serve_canned(Canned::Fixed(status, body, None)).await
    }

    /// A receiver answering `status` with a `Location` and a body naming it, as an ingress's
    /// redirect page does. `location` is relative, so a client that followed it would come back
    /// here and show in the request count.
    async fn canned_redirect_receiver(
        status: StatusCode,
        location: &'static str,
    ) -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
        serve_canned(Canned::Fixed(
            status,
            "redirecting to somewhere this batch was never written",
            Some(location),
        ))
        .await
    }

    async fn canned_endless_receiver(
        status: StatusCode,
    ) -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
        serve_canned(Canned::Endless(status)).await
    }

    async fn serve_canned(canned: Canned) -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_task = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let seen_conn = Arc::clone(&seen_task);
                tokio::spawn(async move {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(stream),
                            service_fn(move |req| {
                                let seen = Arc::clone(&seen_conn);
                                async move {
                                    Ok::<_, std::convert::Infallible>(
                                        record(req, seen, canned).await,
                                    )
                                }
                            }),
                        )
                        .await;
                });
            }
        });
        (format!("http://{addr}/api/v1/write"), seen)
    }

    /// The TLS-wrapped twin of [`canned_receiver`], presenting `testdata/tls/server.pem`. ALPN
    /// offers `http/1.1` only, since this test server speaks `hyper::server::conn::http1`.
    async fn canned_tls_receiver(
        require_client_auth: bool,
    ) -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
        let acceptor =
            tokio_rustls::TlsAcceptor::from(http1_server_tls_config(require_client_auth));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_task = Arc::clone(&seen);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let acceptor = acceptor.clone();
                let seen_conn = Arc::clone(&seen_task);
                tokio::spawn(async move {
                    let Ok(tls_stream) = acceptor.accept(stream).await else { return };
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(
                            TokioIo::new(tls_stream),
                            service_fn(move |req| {
                                let seen = Arc::clone(&seen_conn);
                                async move {
                                    Ok::<_, std::convert::Infallible>(
                                        record(req, seen, Canned::Fixed(StatusCode::OK, "", None))
                                            .await,
                                    )
                                }
                            }),
                        )
                        .await;
                });
            }
        });
        (format!("https://{addr}/api/v1/write"), seen)
    }

    async fn record(
        req: http::Request<hyper::body::Incoming>,
        seen: Arc<Mutex<Vec<CapturedRequest>>>,
        canned: Canned,
    ) -> http::Response<CannedBody> {
        use http_body_util::BodyExt as _;
        let (parts, incoming) = req.into_parts();
        let collected = incoming.collect().await.map(|b| b.to_bytes()).unwrap_or_default();
        seen.lock().unwrap().push(CapturedRequest {
            method: parts.method,
            path: parts.uri.path().to_string(),
            headers: parts.headers,
            body: collected.to_vec(),
        });
        let (status, body, location): (_, CannedBody, _) = match canned {
            Canned::Fixed(status, body, location) => {
                (status, Full::new(Bytes::from_static(body.as_bytes())).boxed(), location)
            }
            Canned::Endless(status) => (status, CannedBody::new(EndlessBody), None),
        };
        let mut builder = http::Response::builder().status(status);
        if let Some(location) = location {
            builder = builder.header(http::header::LOCATION, location);
        }
        builder.body(body).expect("a well-formed response always builds")
    }

    /// `otlp.rs`'s own test-only server config, with ALPN narrowed to HTTP/1.1.
    fn http1_server_tls_config(require_client_auth: bool) -> Arc<rustls::ServerConfig> {
        use rustls_pki_types::pem::PemObject as _;
        use rustls_pki_types::{CertificateDer, PrivateKeyDer};
        let dir = testdata_tls_dir();
        let chain: Vec<CertificateDer<'static>> =
            CertificateDer::pem_file_iter(dir.join("server.pem"))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        let key = PrivateKeyDer::from_pem_file(dir.join("server.key")).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap();
        let mut cfg = if require_client_auth {
            let mut roots = rustls::RootCertStore::empty();
            let ca: Vec<CertificateDer<'static>> =
                CertificateDer::pem_file_iter(dir.join("ca.pem"))
                    .unwrap()
                    .collect::<Result<_, _>>()
                    .unwrap();
            roots.add_parsable_certificates(ca);
            let verifier =
                rustls::server::WebPkiClientVerifier::builder(Arc::new(roots)).build().unwrap();
            builder.with_client_cert_verifier(verifier).with_single_cert(chain, key).unwrap()
        } else {
            builder.with_no_client_auth().with_single_cert(chain, key).unwrap()
        };
        cfg.alpn_protocols = vec![b"http/1.1".to_vec()];
        Arc::new(cfg)
    }

    /// The repo root's `testdata/tls` (`testdata/tls/README.md`).
    fn testdata_tls_dir() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    fn sender(endpoint: &str) -> RemoteWriteOutput {
        RemoteWriteOutput::new(endpoint)
    }

    /// One cumulative counter at one timestamp -- the smallest batch that produces a request.
    fn counter_batch(timestamp: i64, value: f64) -> EventBatch {
        batch(vec![Event::metric(
            timestamp,
            AttrMap::new(),
            MetricRecord::new(intern("http_requests"), cumulative_counter(value)),
        )])
    }

    fn only(seen: &Arc<Mutex<Vec<CapturedRequest>>>) -> CapturedRequest {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 1, "expected exactly one request, got {}", seen.len());
        seen[0].clone()
    }

    // -- sender mode: the wire ----------------------------------------------------------------

    #[tokio::test]
    async fn a_batch_is_posted_to_the_configured_url_with_the_four_protocol_headers() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink = sender(&url);
        sink.send(&counter_batch(1_700_000_000_000_000_000, 5.0)).await.expect("2xx is Ok");

        let request = only(&seen);
        assert_eq!(request.method, Method::POST);
        assert_eq!(request.path, "/api/v1/write", "the endpoint's own path is used verbatim");
        assert_eq!(
            request.header("content-type"),
            Some("application/x-protobuf;proto=prometheus.WriteRequest")
        );
        assert_eq!(request.header("content-encoding"), Some("snappy"));
        assert_eq!(request.header("x-prometheus-remote-write-version"), Some("0.1.0"));
        assert_eq!(
            request.header("user-agent"),
            Some(concat!("logit/", env!("CARGO_PKG_VERSION")))
        );
    }

    #[tokio::test]
    async fn version_2_switches_the_content_type_the_version_header_and_the_message() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink = sender(&url).with_version(remote_write::Version::V2);
        sink.send(&counter_batch(1_700_000_000_000_000_000, 5.0)).await.expect("2xx is Ok");

        let request = only(&seen);
        assert_eq!(
            request.header("content-type"),
            Some("application/x-protobuf;proto=io.prometheus.write.v2.Request")
        );
        assert_eq!(request.header("x-prometheus-remote-write-version"), Some("2.0.0"));
        let decoded = request.as_v2();
        assert_eq!(decoded.symbols.first().map(String::as_str), Some(""), "symbols[0] is empty");
        assert_eq!(decoded.timeseries.len(), 1);
    }

    #[tokio::test]
    async fn an_operator_header_rides_along_and_a_protocol_owned_one_is_overridden() {
        // Rule 56 rejects this at config time; `with_headers` bypasses it to test the sink.
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink = sender(&url)
            .with_headers(&HashMap::from([
                ("X-Scope-OrgID".to_string(), "tenant-a".to_string()),
                ("content-type".to_string(), "text/plain".to_string()),
            ]))
            .expect("both names are lexically legal headers");
        sink.send(&counter_batch(1_700_000_000_000_000_000, 5.0)).await.expect("2xx is Ok");

        let request = only(&seen);
        assert_eq!(request.header("x-scope-orgid"), Some("tenant-a"));
        assert_eq!(
            request.header("content-type"),
            Some("application/x-protobuf;proto=prometheus.WriteRequest"),
            "a protocol-owned name is inserted over the operator's, never appended to it"
        );
    }

    #[tokio::test]
    async fn the_body_is_a_snappy_block_that_decodes_as_a_write_request() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink = sender(&url);
        sink.send(&counter_batch(1_700_000_000_500_000_000, 5.0)).await.expect("2xx is Ok");

        let decoded = only(&seen).as_v1();
        assert_eq!(decoded.timeseries.len(), 1);
        let series = &decoded.timeseries[0];
        let name = series
            .labels
            .iter()
            .find(|l| l.name == "__name__")
            .map(|l| l.value.as_str())
            .expect("every series carries __name__");
        assert_eq!(name, "http_requests_total", "a counter gains _total on the wire");
        assert_eq!(series.samples.len(), 1);
        assert_eq!(series.samples[0].value, 5.0);
        assert_eq!(
            series.samples[0].timestamp, 1_700_000_000_500,
            "nanoseconds truncate to milliseconds on the wire"
        );
    }

    /// Three events at three instants for one series become one `TimeSeries` with three samples
    /// in ascending order.
    #[tokio::test]
    async fn a_multi_timestamp_batch_becomes_one_timeseries_with_ordered_samples() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink = sender(&url);
        // Out of order in the batch: ascending order is the partition's doing.
        sink.send(&batch(vec![
            Event::metric(
                3_000_000_000,
                AttrMap::new(),
                MetricRecord::new(intern("http_requests"), cumulative_counter(3.0)),
            ),
            Event::metric(
                1_000_000_000,
                AttrMap::new(),
                MetricRecord::new(intern("http_requests"), cumulative_counter(1.0)),
            ),
            Event::metric(
                2_000_000_000,
                AttrMap::new(),
                MetricRecord::new(intern("http_requests"), cumulative_counter(2.0)),
            ),
        ]))
        .await
        .expect("2xx is Ok");

        let decoded = only(&seen).as_v1();
        assert_eq!(decoded.timeseries.len(), 1, "one label set is one TimeSeries");
        let samples = &decoded.timeseries[0].samples;
        assert_eq!(
            samples.iter().map(|s| (s.timestamp, s.value)).collect::<Vec<_>>(),
            vec![(1_000, 1.0), (2_000, 2.0), (3_000, 3.0)]
        );
    }

    /// A `FLAG_NO_RECORDED_VALUE` gauge goes out as a stale marker, not skipped as an exposition
    /// would.
    #[tokio::test]
    async fn a_flagged_gauge_is_sent_as_a_stale_nan_sample() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink = sender(&url);
        let mut record = MetricRecord::new(intern("temperature"), MetricKind::Gauge(0.0));
        record.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
        sink.send(&batch(vec![Event::metric(1_000_000_000, AttrMap::new(), record)]))
            .await
            .expect("2xx is Ok");

        let decoded = only(&seen).as_v1();
        assert_eq!(decoded.timeseries.len(), 1);
        let value = decoded.timeseries[0].samples[0].value;
        assert_eq!(
            value.to_bits(),
            logit_proto::prometheus::STALE_NAN_BITS,
            "the stale marker is a specific NaN payload, not any NaN"
        );
    }

    /// A delta `Sum` is skipped and counted in sender mode too, with the same named fix.
    #[tokio::test]
    async fn a_delta_sum_is_skipped_and_counted_with_no_request_left_to_send() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let registry = logit_core::Registry::new();
        let mut sink =
            sender(&url).with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"));
        sink.send(&batch(vec![Event::metric(
            1_000_000_000,
            AttrMap::new(),
            MetricRecord::new(
                intern("requests"),
                MetricKind::Sum(Sum {
                    value: 1.0,
                    temporality: Temporality::Delta,
                    monotonic: true,
                }),
            ),
        )]))
        .await
        .expect("a skipped record is not a failure");

        assert_eq!(
            counter(&registry, "logit.output.metrics.skipped", "metric_kind", "delta_sum"),
            1.0
        );
        assert!(seen.lock().unwrap().is_empty(), "nothing survived, so nothing was sent");
    }

    #[tokio::test]
    async fn an_empty_batch_sends_no_request_at_all() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink = sender(&url);
        sink.send(&batch(vec![])).await.expect("an empty batch is a no-op");
        assert!(seen.lock().unwrap().is_empty());
    }

    // -- sender mode: telemetry ----------------------------------------------------------------

    #[tokio::test]
    async fn a_successful_request_counts_its_class_its_duration_and_its_samples() {
        let (url, _seen) = canned_receiver(StatusCode::OK, "").await;
        let registry = logit_core::Registry::new();
        let mut sink =
            sender(&url).with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"));
        // Two series at two timestamps: four samples, not the family or series count (2).
        sink.send(&batch(vec![
            Event::metric(
                1_000_000_000,
                AttrMap::new(),
                MetricRecord::new(intern("a"), MetricKind::Gauge(1.0)),
            ),
            Event::metric(
                1_000_000_000,
                AttrMap::new(),
                MetricRecord::new(intern("b"), MetricKind::Gauge(2.0)),
            ),
            Event::metric(
                2_000_000_000,
                AttrMap::new(),
                MetricRecord::new(intern("a"), MetricKind::Gauge(3.0)),
            ),
            Event::metric(
                2_000_000_000,
                AttrMap::new(),
                MetricRecord::new(intern("b"), MetricKind::Gauge(4.0)),
            ),
        ]))
        .await
        .expect("2xx is Ok");

        let events = registry.drain(0);
        assert_eq!(tagged(&events, "logit.output.requests", "class", "2xx"), 1.0);
        assert_eq!(
            events
                .iter()
                .flat_map(|e| &e.metrics)
                .filter(|m| logit_core::interner::resolve(m.name) == "logit.output.samples")
                .map(|m| match m.kind {
                    MetricKind::Sum(ref s) => s.value,
                    ref other => panic!("samples should be a counter, got {other:?}"),
                })
                .sum::<f64>(),
            4.0
        );
        assert!(
            events.iter().flat_map(|e| &e.metrics).any(|m| {
                logit_core::interner::resolve(m.name) == "logit.output.request.duration"
            }),
            "one timer per request actually issued"
        );
    }

    // -- sender mode: fault classification -----------------------------------------------------

    #[tokio::test]
    async fn a_5xx_and_a_429_are_ambiguous_and_counted_by_class() {
        for (status, class) in
            [(StatusCode::INTERNAL_SERVER_ERROR, "5xx"), (StatusCode::TOO_MANY_REQUESTS, "4xx")]
        {
            let (url, _seen) = canned_receiver(status, "").await;
            let registry = logit_core::Registry::new();
            let mut sink = sender(&url).with_telemetry(registry.telemetry_for(
                "out",
                "prometheus_out",
                "sink",
            ));
            let err = sink
                .send(&counter_batch(1_000_000_000, 1.0))
                .await
                .expect_err("a rejection is an error");
            assert_eq!(
                logit_pipeline::classify(&err),
                Fault::Ambiguous,
                "{status}: the request reached the server and may have been partly applied"
            );
            assert_eq!(counter(&registry, "logit.output.requests", "class", class), 1.0);
        }
    }

    /// A `400` is permanent and its body, which names the offending series, is reported.
    #[tokio::test]
    async fn a_400_is_permanent_and_carries_the_response_body_in_its_message() {
        let (url, _seen) =
            canned_receiver(StatusCode::BAD_REQUEST, "out of order sample for series {x=\"1\"}")
                .await;
        let mut sink = sender(&url);
        let err =
            sink.send(&counter_batch(1_000_000_000, 1.0)).await.expect_err("a 400 is an error");
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
        assert!(logit_pipeline::is_explicitly_permanent(&err), "never retried under any posture");
        let message = format!("{err:#}");
        assert!(message.contains("400"), "got: {message}");
        assert!(message.contains("out of order sample"), "got: {message}");
    }

    /// A `500` with an endless body still classifies as a `5xx` promptly: the read stops.
    /// `crate::http`'s tests cover the cutting rules.
    #[tokio::test]
    async fn an_endless_rejection_body_is_read_only_as_far_as_the_snippet_needs() {
        let (url, _seen) = canned_endless_receiver(StatusCode::INTERNAL_SERVER_ERROR).await;
        // An unbounded read would fail this timeout rather than hang the test.
        let mut sink = sender(&url).with_timeout(Duration::from_secs(30));
        let outcome = tokio::time::timeout(
            Duration::from_secs(10),
            sink.send(&counter_batch(1_000_000_000, 1.0)),
        )
        .await
        .expect("the read is bounded by bytes, so it returns without waiting for the body to end");

        let err = outcome.expect_err("a 500 is an error");
        assert_eq!(
            logit_pipeline::classify(&err),
            Fault::Ambiguous,
            "a bounded read must not turn a 5xx into a transport timeout"
        );
        let message = format!("{err:#}");
        assert!(message.contains("500"), "got: {message}");
        assert!(message.contains("..."), "the snippet is ellipsised: {message}");
        assert!(
            message.len() < ERROR_BODY_SNIPPET_BYTES * 2,
            "the whole message stays log-line sized, got {} bytes",
            message.len()
        );
    }

    /// A `3xx` is a failure, not a followed redirect; the receiver's request count is the proof.
    #[tokio::test]
    async fn a_redirect_is_not_followed_and_is_a_permanent_fault() {
        let (url, seen) = canned_redirect_receiver(StatusCode::FOUND, "/api/v1/write").await;
        let registry = logit_core::Registry::new();
        let mut sink =
            sender(&url).with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"));
        let err = sink
            .send(&counter_batch(1_000_000_000, 1.0))
            .await
            .expect_err("a 3xx is not a delivery");

        assert_eq!(seen.lock().unwrap().len(), 1, "the redirect must not be followed");
        assert_eq!(logit_pipeline::classify(&err), Fault::Permanent);
        assert!(logit_pipeline::is_explicitly_permanent(&err), "never retried under any posture");
        let message = format!("{err:#}");
        assert!(message.contains("302"), "got: {message}");
        assert!(
            message.contains("redirecting to"),
            "the operator sees what the receiver said: {message}"
        );

        let events = registry.drain(0);
        assert_eq!(tagged(&events, "logit.output.requests", "class", "3xx"), 1.0);
        assert_eq!(
            events
                .iter()
                .flat_map(|e| &e.metrics)
                .filter(|m| logit_core::interner::resolve(m.name) == "logit.output.samples")
                .count(),
            0,
            "nothing was written, so nothing is counted as written"
        );
    }

    #[tokio::test]
    async fn a_refused_connection_is_clean() {
        // Bound and dropped, so (almost certainly) nothing listens there.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let registry = logit_core::Registry::new();
        let mut sink = sender(&format!("http://{addr}/api/v1/write"))
            .with_telemetry(registry.telemetry_for("out", "prometheus_out", "sink"));
        let err =
            sink.send(&counter_batch(1_000_000_000, 1.0)).await.expect_err("nothing is listening");
        assert_eq!(
            logit_pipeline::classify(&err),
            Fault::Clean,
            "a connect failure provably never reached a receiver"
        );
        assert_eq!(counter(&registry, "logit.output.requests", "class", "network_error"), 1.0);
    }

    // -- sender mode: TLS, lifecycle, posture --------------------------------------------------

    #[tokio::test]
    async fn an_https_endpoint_with_a_trusted_ca_file_delivers() {
        let (url, seen) = canned_tls_receiver(false).await;
        let mut sink = sender(&url)
            .with_tls(
                &TlsClientSettings { ca_file: Some("ca.pem".to_string()), ..Default::default() },
                &testdata_tls_dir(),
            )
            .expect("the fixture CA loads");
        sink.send(&counter_batch(1_000_000_000, 1.0)).await.expect("a trusted CA handshakes");
        assert_eq!(only(&seen).path, "/api/v1/write");
    }

    #[tokio::test]
    async fn an_https_endpoint_with_an_untrusted_ca_fails_cleanly() {
        let (url, _seen) = canned_tls_receiver(false).await;
        let mut sink = sender(&url)
            .with_tls(
                &TlsClientSettings {
                    ca_file: Some("other-ca.pem".to_string()),
                    ..Default::default()
                },
                &testdata_tls_dir(),
            )
            .expect("the other CA loads too -- it just doesn't sign this server");
        let err = sink
            .send(&counter_batch(1_000_000_000, 1.0))
            .await
            .expect_err("an untrusted CA should fail");
        assert_eq!(logit_pipeline::classify(&err), Fault::Clean);
    }

    #[tokio::test]
    async fn an_https_endpoint_requiring_a_client_certificate_gets_one() {
        let (url, seen) = canned_tls_receiver(true).await;
        let mut sink = sender(&url)
            .with_tls(
                &TlsClientSettings {
                    ca_file: Some("ca.pem".to_string()),
                    cert_file: Some("client.pem".to_string()),
                    key_file: Some("client.key".to_string()),
                    ..Default::default()
                },
                &testdata_tls_dir(),
            )
            .expect("the fixture client certificate loads");
        sink.send(&counter_batch(1_000_000_000, 1.0)).await.expect("mutual TLS handshakes");
        assert_eq!(only(&seen).path, "/api/v1/write");
    }

    #[tokio::test]
    async fn the_sender_binds_and_flushes_as_no_ops() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink = sender(&url);
        sink.bind().await.expect("there is nothing to bind");
        sink.flush().await.expect("there is nothing to flush");
        assert!(seen.lock().unwrap().is_empty(), "neither hook touches the network");
    }

    /// `true`, which selects `AtLeastOnce`, the only posture that retries a 5xx.
    #[test]
    fn the_sender_is_duplicate_safe_and_that_selects_at_least_once() {
        let sink = RemoteWriteOutput::new("http://mimir:8080/api/v1/push");
        assert!(sink.duplicate_safe());
        assert_eq!(
            logit_pipeline::DeliveryPosture::from_duplicate_safe(sink.duplicate_safe()),
            logit_pipeline::DeliveryPosture::AtLeastOnce
        );
    }

    #[test]
    fn a_header_name_that_is_not_a_legal_http_header_is_rejected_at_construction() {
        // `let ... else`, not `expect_err`, which would need the sink to be `Debug`.
        let Err(err) = RemoteWriteOutput::new("http://mimir:8080/api/v1/push")
            .with_headers(&HashMap::from([("bad header".to_string(), "x".to_string())]))
        else {
            panic!("a space is not legal in a header name");
        };
        assert!(format!("{err:#}").contains("not a legal header name"), "got: {err:#}");

        let Err(err) =
            RemoteWriteOutput::new("http://mimir:8080/api/v1/push").with_headers(&HashMap::from([
                ("X-A".to_string(), "1".to_string()),
                ("x-a".to_string(), "2".to_string()),
            ]))
        else {
            panic!("two names collide once case is normalized");
        };
        assert!(format!("{err:#}").contains("collides"), "got: {err:#}");
    }

    /// The mode enum delegates and decides nothing of its own.
    #[tokio::test]
    async fn the_mode_enum_delegates_to_the_selected_half() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink: PrometheusOutput = RemoteWriteOutput::new(&url).into();
        assert!(sink.duplicate_safe());
        sink.bind().await.expect("a sender has nothing to bind");
        sink.send(&counter_batch(1_000_000_000, 1.0)).await.expect("2xx is Ok");
        sink.flush().await.expect("a sender has nothing to flush");
        assert_eq!(only(&seen).method, Method::POST);

        let mut sink: PrometheusOutput = ExposeOutput::new("127.0.0.1:0").into();
        assert!(sink.duplicate_safe());
        sink.bind().await.expect("the exposition half binds");
        sink.send(&fixture_batch()).await.expect("the exposition half never fails a send");
        sink.flush().await.expect("the exposition half aborts its accept loop");
    }
}
