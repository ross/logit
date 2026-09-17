//! `prometheus_out`: the Prometheus sink, in either of its two shapes -- an **exposition**
//! endpoint a Prometheus scrapes (`bind:`), or a **remote-write sender** that POSTs each batch to
//! a receiver (`endpoint:`). Exactly one is set; graph rule 56 rejects both and neither. The
//! mirror of `logit_inputs::prometheus`, which is the same pair the other way round (scrape a
//! target, or receive remote-write on a bind), and the fourth like-protocol pair under
//! [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md). Both wire syntaxes and the
//! whole model<->families mapping live in `logit_proto::prometheus` ([`text::write`],
//! [`remote_write::encode`], [`events_to_families`]); nothing here knows what a sample line or a
//! `TimeSeries` looks like.
//!
//! **This module doc is the spec** (house convention, see [`crate::statsd`]'s). The authorities
//! are [ADR `prometheus-scrape-and-exposition`](../../../docs/adr/prometheus-scrape-and-exposition.md)
//! -- "Exposition state and expiry", "Dialects and negotiation", "`Output::bind`" -- and
//! [ADR `prometheus-remote-write`](../../../docs/adr/prometheus-remote-write.md) -- "Sender
//! behaviour: one request per batch, no retry in the sink", "Decode and encode work in timestamp
//! groups", "Timestamps: received without the marker, sent always".
//!
//! ## Config
//!
//! Registry mode -- [`ExposeOutput`]:
//!
//! ```yaml
//! kind: prometheus_out
//! bind: "127.0.0.1:9464"   # loopback in every example -- see "Security posture"
//! path: /metrics           # default
//! expire_after: 5m         # a series not updated within this window stops being exposed; 0s off
//! max_series: 100000       # hard cap; least-recently-updated evicted first
//! ```
//!
//! Sender mode -- [`RemoteWriteOutput`]:
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
//! A field of the mode that isn't set is a config error rather than a setting that silently does
//! nothing (rule 56). `buffer:` works unchanged on both -- it is a sibling of `kind:` on every
//! sink. It matters rather more in sender mode, which is the only one of the two whose `send` can
//! actually fail.
//!
//! ## Dialect negotiation (registry mode)
//!
//! One endpoint, two dialects, chosen per request from the client's own `Accept`:
//!
//! | Request | Response `Content-Type` |
//! |---|---|
//! | `Accept` contains `application/openmetrics-text` | `application/openmetrics-text; version=1.0.0; charset=utf-8` |
//! | anything else, including no `Accept` at all and a bare `*/*` | `text/plain; version=0.0.4; charset=utf-8` |
//!
//! Both spellings come from [`Dialect::content_type`], so the scraper's `Accept` and this
//! response's `Content-Type` are never two independent copies of the same string. `Accept-Encoding:
//! gzip` gets a gzip-compressed body and `Content-Encoding: gzip`; a `gzip;q=0` does not.
//!
//! ## Routes (registry mode)
//!
//! | Request | Response |
//! |---|---|
//! | `GET`/`HEAD` on the configured `path` | `200`, the rendered registry -- `logit.output.scrapes{class="ok"}` |
//! | any method on any other path | `404` -- `logit.output.scrapes{class="not_found"}` |
//! | any other method on `path` | `405` + `Allow: GET, HEAD` -- `logit.output.scrapes{class="method"}` |
//!
//! `HEAD` routes exactly like `GET` and builds the identical body, so `content-length` agrees with
//! what a `GET` would have returned (RFC 9110 §9.3.2) -- hyper suppresses the bytes themselves.
//! Path is matched before method, so an unknown path is a `404` regardless of method: "no such
//! resource" outranks "wrong verb for the resource you didn't ask for".
//!
//! ## State: upsert, expiry, cardinality (registry mode)
//!
//! [`ExposeOutput::send`] converts the batch with [`events_to_families`] (every lossy path --
//! delta temporality, `ExponentialHistogram`, an unrepresentable label -- is counted by the codec's
//! own [`PrometheusEncoder`], not here) and **upserts** each series into the registry: keyed by
//! family name, then by the series' rendered, sorted label set, latest value wins. That is
//! cumulative-series semantics, not a stream of deliveries: a scrape renders the current registry,
//! so re-sending the same batch simply writes the same values again.
//!
//! - `expire_after` (default 5m, Prometheus's own staleness horizon): a series whose last update is
//!   older than this is dropped, counted `logit.output.series.evicted{reason="expired"}`. `0s`
//!   disables expiry. The sweep runs after every `send` **and on every request**, so a sink that
//!   has gone quiet still stops exposing stale series rather than freezing its last state forever.
//! - `max_series` (default 100000): a hard cap. Over it, the least-recently-updated series is
//!   evicted first, counted `logit.output.series.evicted{reason="cardinality"}` -- the same
//!   least-recently-used shape `aggregate`'s `max_retained_gauge_series` uses for window state.
//!
//! **Type conflicts.** The exposition grammar allows exactly one `# TYPE` per family name, so a
//! record arriving as a `gauge` for a name already registered as a `counter` cannot be represented
//! alongside the old interpretation. The new type wins: the family's type is replaced, every series
//! already stored under the old type is evicted, and `logit.output.metrics.type_conflict` is
//! counted. Keeping the old type instead would mean silently dropping live data in favour of data
//! that may already have expired.
//!
//! ## The wire (sender mode)
//!
//! One `send` is **one `POST`**, and a batch that produces no series at all sends **no request** --
//! not an empty `WriteRequest`. Four headers are `insert`ed *over* a clone of the operator's
//! `headers:` map, so a protocol-owned name always wins whatever config said (rule 56 rejects one
//! at config time as well; this is the defense in depth behind it, and the same merge order
//! `otlp_out`'s HTTP transport uses):
//!
//! | Header | Value |
//! |---|---|
//! | `Content-Type` | [`remote_write::Version::content_type`] -- `application/x-protobuf;proto=prometheus.WriteRequest` for `version: 1`, `…;proto=io.prometheus.write.v2.Request` for `2` |
//! | `Content-Encoding` | `snappy` -- the Snappy **block** format (`snap::raw`), never the framed one |
//! | `X-Prometheus-Remote-Write-Version` | [`remote_write::Version::header_version`] -- `0.1.0` for 1.0 (the spec's own historical number), `2.0.0` for 2.0 |
//! | `User-Agent` | `logit/<version>` |
//!
//! There is no negotiation and no fallback between versions: the operator picks the one their
//! receiver speaks, exactly as they already pick an exposition dialect.
//!
//! **Timestamp partition → merged `TimeSeries`.** A remote-write `TimeSeries` is one label set and
//! N samples; a `Series` is one label set and one point. So [`RemoteWriteOutput::send`] partitions
//! the batch's events by `Event::timestamp` (ascending, stable within a timestamp), runs
//! [`events_to_families`] once per partition, and hands the whole list of groups to
//! [`remote_write::encode`], which merges identical label sets **across** groups into one
//! `TimeSeries` whose samples are in timestamp order. A batch holding five scrapes of one target
//! therefore becomes one `TimeSeries` with five samples, which is both what the format is for and
//! what a receiver's in-order check expects. The partition is also what keeps a classic
//! histogram's `_bucket`/`_sum`/`_count` samples -- which by construction share one timestamp --
//! in front of one assembler at the far end.
//!
//! The encoder runs with `with_timestamps_always(true)`, because the wire has no way to omit a
//! timestamp and a series without one would be silently skipped, and with
//! `with_stale_markers(true)`, so a record flagged `FLAG_NO_RECORDED_VALUE` is written as
//! Prometheus's own stale marker (a NaN with bit pattern `0x7ff0000000000002`) rather than skipped.
//! That last one covers `Gauge`, `Sum` and marker-untyped records only: a flagged
//! `Histogram`/`Summary`/sketch expands to several derived series and one flag says nothing about
//! which of them existed, so it stays skipped and counted (`docs/known-gaps.md`).
//!
//! ## Faults, retries and duplicate safety (sender mode)
//!
//! **One `send` is one attempt.** There is no retry loop here; retry is `write_loop`'s job
//! (`docs/adr/buffered-sink-delivery.md`) and this sink's whole contribution is classifying the
//! outcome, attached to the error with `.context(fault)` -- the identical table `otlp_out`'s HTTP
//! transport uses, shared with it as code in [`crate::http`] rather than copied:
//!
//! | Outcome | Result |
//! |---|---|
//! | 2xx | `Ok` |
//! | 429, any 5xx | [`Fault::Ambiguous`] -- the request reached the server and may have been partly applied |
//! | any other 4xx | [`Fault::Permanent`], with the status and the first 256 bytes of the response body in the message and in a throttled `remote_write_rejected` diagnostic: Prometheus's own `400` text names the offending series and is the only useful thing in the exchange |
//! | connect failure | [`Fault::Clean`] -- the destination provably never saw it |
//! | any other transport error, timeout included | [`Fault::Ambiguous`] |
//!
//! [`RemoteWriteOutput::duplicate_safe`] is **`true`**, and load-bearing rather than incidental. A
//! sample's identity at a remote-write receiver is `(label set, timestamp)`, so replaying an
//! identical request is an idempotent overwrite, never a double count -- which is what makes
//! `true` honest. And `true` is what selects `DeliveryPosture::AtLeastOnce`
//! (`logit_pipeline::output`), which is the *only* posture under which a `Fault::Ambiguous` is
//! retried at all: setting it `false` would not make delivery safer, it would silently turn every
//! 5xx into a dropped batch.
//!
//! **Ordering is the topology's, not this sink's.** Samples go out in batch order and nothing here
//! reorders across batches. Two upstream branches writing the same series can therefore draw
//! out-of-order `400`s from a receiver with no out-of-order window -- a property of the pipeline
//! that was built, not a bug in the sink. A single chain into one `prometheus_out` does not have
//! it.
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
//! | `logit.output.requests{class="1xx"\|"2xx"\|"3xx"\|"4xx"\|"5xx"\|"other"\|"network_error"}` | one per request. `otlp_out`'s vocabulary exactly ([`crate::http::status_class`]), with no `429` class of its own -- a 429 is a `4xx`, and splitting it out would contradict [`crate::http::is_retryable_http_status`], which reads the same status to pick the `Fault`. No `timeout` class either: a timeout is a transport error, so it lands in `network_error`. And no `signal` tag, which `otlp_out` does carry -- this sink has exactly one signal, and hard-coding a tag that never varies is noise |
//! | `logit.output.request.duration` | one timer per request actually issued -- the spelling `graphite_out`, `collectd_out`, `syslog_out`, `statsd_out` and `influxdb_out` already use |
//! | `logit.output.samples` | samples in the request body, counted by the codec ([`remote_write::encode_counted`]) rather than guessed from family counts: one `Series` is one sample for a gauge and several for a histogram. The mirror of `prometheus_in`'s own `logit.input.samples`, so the two ends of a remote-write relay are comparable |
//!
//! plus, in **both** modes, everything the codec counts on the [`PrometheusEncoder`] the sink hands
//! it: `logit.output.metrics.{skipped,degraded}`, `logit.output.labels.dropped`, and
//! `logit.output.labels.normalized{reason="multi_value"}` -- a `Value::Array` attribute (a repeated
//! DogStatsD tag key relayed in from `statsd_in`) collapsed to its last representable element,
//! since a Prometheus label set is a map and has no multi-value label. Unlike every other
//! `*.normalized` reason in `docs/design/internal-telemetry.md`, which report a
//! lossless-but-different rendering of the same information, that one is **lossy**: the non-last
//! elements are discarded, not re-spelled. It reads as `normalized` rather than `dropped` because
//! the label itself survives and the series still exposes. A delta `Sum` reaching either mode is
//! the common one -- skipped, counted `logit.output.metrics.skipped{metric_kind="delta_sum"}`, with
//! a throttled `delta_temporality_unresolved` diagnostic naming the fix (an `aggregate` with
//! `temporality: cumulative` in front of the sink). Remote-write does not change that: the format
//! carries cumulative series, so a delta `Sum` has no more of a spelling there than it has in an
//! exposition.
//!
//! **Both directions count under one encoder** in registry mode. `send` and a render share a single
//! [`PrometheusEncoder`], so the drops each side reaches -- `send`'s unrepresentable labels and
//! skipped kinds, a render's `degraded{reason="exemplar_dropped"|"unit_not_suffix"}` from
//! [`text::write_with`] -- add up as this component's totals rather than splitting across two
//! encoder identities for the same sink. The lock order that implies (encoder before registry) is
//! on [`ExposeOutput::encoder`]. Sender mode needs none of that: it has one direction and one
//! `&mut self`, so its encoder is a plain field.
//!
//! ## Security posture: no TLS, no auth on `bind:`
//!
//! The registry-mode server serves the **entire registry** -- every label on every series it
//! currently holds -- to anything that connects to `bind:`, with no credential check and no
//! transport encryption. That is the same posture
//! [ADR `admin-readiness-endpoint`](../../../docs/adr/admin-readiness-endpoint.md) accepted for
//! `/readyz`, with one difference that matters: the payload is a metric surface rather than a
//! lifecycle word. So: bind loopback or pod-local (`127.0.0.1:9464`, as every shipped example
//! does) and let something that does have TLS and auth front it. An operator who needs this
//! reachable from off-host is making that choice deliberately, not inheriting it from an example.
//! Tracked in `docs/known-gaps.md` next to `admin:`'s own row.
//!
//! Sender mode is the opposite posture and always has been: it dials out, an `https://` endpoint
//! gets real TLS with the bundled Mozilla roots by default, and `endpoint_tls:` tunes that -- a
//! private CA, a client certificate for mutual TLS, or (deliberately awkward to ask for)
//! `insecure_skip_verify`, which logs a startup warning. The receiver getting TLS does not
//! retroactively give the exposition server any.

use crate::http::{build_client, classify_reqwest_error, is_retryable_http_status, status_class};
/// The sender's `endpoint_tls:`, re-exported at this path the way `otlp_out`, `logit_out`,
/// `syslog_out` and `statsd_out` each re-export the one shared type.
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

/// The default `path:` -- what every Prometheus scrape config assumes when a target's own
/// `metrics_path` is left unset.
pub const DEFAULT_PATH: &str = "/metrics";

/// The default `expire_after:`, matching Prometheus's own staleness horizon -- see the module doc.
pub const DEFAULT_EXPIRE_AFTER: Duration = Duration::from_secs(300);

/// The default `max_series:` cap.
pub const DEFAULT_MAX_SERIES: usize = 100_000;

/// A scrape endpoint answers one cheap `GET` at a time -- the same bound `logit-cli`'s admin
/// server uses, and for the same reason: this caps the worst case at a handful of stuck
/// connections rather than an unbounded accept loop.
const MAX_CONCURRENT_CONNECTIONS: usize = 16;

/// How long a client has to finish sending its request headers. Hyper's own knob (it needs a timer
/// installed, hence the [`hyper_util::rt::TokioTimer`] on the builder), and the right place for the
/// slowloris bound that an all-encompassing connection deadline used to carry: a client that opens
/// a socket and dribbles -- or never finishes -- must not hold one of the 16 slots, and *that* is
/// cheap to bound tightly because a scrape request is a few hundred bytes of headers.
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// The floor for a whole connection, response body included. Deliberately well past Prometheus's
/// own default `scrape_timeout` of 10s: the response here is not a probe's few hundred bytes but up
/// to `max_series` series, and a deadline shorter than the scraper's own would drop hyper mid-body
/// and hand the client a short read against the `Content-Length` it already trusted -- while this
/// sink had already counted the scrape a success. `admin.rs`'s 5s is right for `admin.rs`'s payload;
/// it is not right here.
const RESPONSE_TIMEOUT_BASE: Duration = Duration::from_secs(30);

/// Added to [`RESPONSE_TIMEOUT_BASE`] per 1000 series of *configured capacity*, so a deployment that
/// raised `max_series` raises its own write deadline with it rather than having to know this
/// constant exists. At the default 100 000 cap that is 40s total.
const RESPONSE_TIMEOUT_PER_1K_SERIES: Duration = Duration::from_millis(100);

/// How long the accept loop pauses after an `accept()` failure that is not one client's own
/// accident -- fd exhaustion (`EMFILE`/`ENFILE`) being the realistic case, which neither clears
/// instantly nor persists forever. Without it a sustained one spins a core.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// [`RESPONSE_TIMEOUT_BASE`] plus [`RESPONSE_TIMEOUT_PER_1K_SERIES`] per 1000 of `max_series`,
/// saturating rather than panicking on an absurd configured cap.
fn response_timeout(max_series: usize) -> Duration {
    let thousands = u32::try_from(max_series / 1_000).unwrap_or(u32::MAX);
    RESPONSE_TIMEOUT_BASE.saturating_add(
        RESPONSE_TIMEOUT_PER_1K_SERIES.checked_mul(thousands).unwrap_or(Duration::MAX),
    )
}

/// The default `timeout:` for one remote-write request -- the same 10s `otlp_out` and
/// `prometheus_in`'s scrape both use for one HTTP request.
pub const DEFAULT_ENDPOINT_TIMEOUT: Duration = Duration::from_secs(10);

/// The `User-Agent` every remote-write request carries. Reserved in config (rule 56) so an
/// operator can't replace it: a receiver's own logs are frequently the only place a misbehaving
/// sender is identified from, and this is the identification.
const USER_AGENT: &str = concat!("logit/", env!("CARGO_PKG_VERSION"));

/// How much of a rejection body is worth carrying into an error message and a diagnostic. Enough
/// for Prometheus's own `400` text -- which names the offending series and why -- and short enough
/// that a receiver answering with an HTML error page doesn't fill a log line.
const ERROR_BODY_SNIPPET_BYTES: usize = 256;

const SCRAPES: &str = "logit.output.scrapes";
/// Response body bytes as *rendered* (post-gzip when the client asked for it), counted when the
/// body is built rather than when its last byte is acknowledged -- a `Full<Bytes>` response has no
/// body-completion hook to count from, so a connection dropped mid-write is still counted here and
/// still counted `scrapes{class="ok"}`.
const SCRAPE_BYTES: &str = "logit.output.scrape.bytes";
const SERIES: &str = "logit.output.series";
const SERIES_EVICTED: &str = "logit.output.series.evicted";
const TYPE_CONFLICT: &str = "logit.output.metrics.type_conflict";

// Sender mode's three. See the module doc's telemetry table for why `requests` carries no
// `signal` tag and no `429`/`timeout` class of its own.
const REQUESTS: &str = "logit.output.requests";
const REQUEST_DURATION: &str = "logit.output.request.duration";
/// Samples in the request body, as the codec counted them -- not families, and not series: one
/// `Series` is one sample for a gauge and several for a histogram.
const SAMPLES: &str = "logit.output.samples";

/// What reads "now" for the expiry sweep. A closure rather than a bare `Instant::now` so the
/// expiry and cardinality tests can advance time by hand instead of sleeping -- the sweep is the
/// one piece of this sink whose behaviour is a function of wall-clock elapsed time, and a test that
/// slept through a real `expire_after` would be both slow and flaky.
type Clock = Arc<dyn Fn() -> Instant + Send + Sync>;

/// The rendered, sorted label set of a series -- the registry's per-family key. Rendered, not the
/// event's own `AttrMap`: sanitization is many-to-one (ADR "Names and sanitization"), so two
/// distinct attribute sets can be one wire series, and the wire is what a scraper sees.
type LabelKey = Vec<(String, String)>;

/// One series' current value plus when it last arrived. The label set is the map key, not a field
/// here; [`StoredFamily::families`] rebuilds a [`Series`] from the pair at render time.
#[derive(Debug, Clone)]
struct Stored {
    point: Point,
    timestamp: Option<i64>,
    created: Option<i64>,
    exemplars: Vec<Exemplar>,
    updated_at: Instant,
}

/// One family: exactly one `# TYPE`, one metadata pair, and its series keyed by label set.
#[derive(Debug, Clone)]
struct StoredFamily {
    kind: FamilyType,
    help: Option<String>,
    unit: Option<String>,
    series: BTreeMap<LabelKey, Stored>,
}

/// The exposition state: what a scrape renders. `BTreeMap` at both levels, so a render is already
/// in the canonical order [`text::write`] wants (families by name, series by label set) with no
/// sort pass of its own.
#[derive(Debug, Default)]
struct Registry {
    families: BTreeMap<String, StoredFamily>,
}

impl Registry {
    fn len(&self) -> usize {
        self.families.values().map(|f| f.series.len()).sum()
    }

    /// One family's worth of series into the registry, latest wins. A type change replaces the
    /// family's type and drops every series stored under the old one -- see the module doc's
    /// "Type conflicts".
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
        // Latest wins for metadata too, not just for values: `# HELP`/`# UNIT` come from the
        // record's own `description`/`unit`, so a change here means the producer changed its mind,
        // and a scrape should see what the producer last said.
        entry.help = help;
        entry.unit = unit;
        for series in series {
            let Series { labels, point, timestamp, created, exemplars } = series;
            entry
                .series
                .insert(labels, Stored { point, timestamp, created, exemplars, updated_at: now });
        }
    }

    /// Drops every series whose last update is older than `expire_after` (a zero duration disables
    /// expiry entirely), plus any family left with no series at all -- an empty family would
    /// otherwise render as a bare `# TYPE`/`# HELP` pair with nothing under it.
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

    /// Evicts least-recently-updated series until at most `max_series` remain, in **one pass over
    /// the registry** regardless of how many have to go.
    ///
    /// Being over the cap is not a rare accident -- it is the steady state the cap exists for, so
    /// this runs under load, holding the lock a render also needs. Hence: borrow
    /// `(updated_at, &name, &labels)` for every series once (no allocation per candidate),
    /// [`select_nth_unstable_by`](slice::select_nth_unstable_by) to partition the `k` oldest into
    /// the front of that slice in O(N) without sorting the rest, clone only those `k` keys, and
    /// remove them. A `while len() > max` loop calling `min_by` instead would re-scan and
    /// re-allocate every candidate `k` times over -- at the default cap of 100 000 that is `k` full
    /// allocating scans per `send`, exactly when cardinality is the thing being diagnosed.
    ///
    /// The tie-break past `updated_at` is the series' own key (family name, then label set), so
    /// which of two series updated in the same batch goes is a function of the data rather than of
    /// `Instant` resolution or map iteration order.
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
        // `excess <= total` (`excess = total - max_series`) and `total > 0` here, so
        // `excess - 1` is a valid index into `candidates`.
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

    /// The registry as the codec's own family list -- the seam [`text::write`] renders from.
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

    /// Renders through [`text::write_with`], not the no-telemetry [`text::write`] convenience, so
    /// the writer's own drops land under this component's identity: an exemplar with no line left
    /// to sit on and a `# UNIT` whose unit doesn't suffix the family name are counted
    /// `logit.output.metrics.degraded{reason="exemplar_dropped"|"unit_not_suffix"}` by the same
    /// encoder `send` hands `events_to_families`. Family-name collisions after sanitization are
    /// that encoder's to skip and count too, never this registry's to special-case.
    fn render(&self, dialect: Dialect, encoder: &mut PrometheusEncoder) -> Vec<u8> {
        let mut out = Vec::new();
        text::write_with(&self.families(), dialect, &mut out, encoder);
        out
    }
}

/// Everything one request handler needs, behind one `Arc` so the accept loop clones a refcount per
/// connection rather than four fields.
struct ServerState {
    registry: Arc<Mutex<Registry>>,
    /// The *same* encoder `send` uses, not a second one built from the same handles: one component,
    /// one encoder identity, so `logit.output.metrics.degraded` reads as this sink's total however
    /// the drop was reached. See [`ExposeOutput::encoder`] for the lock ordering it implies.
    encoder: Arc<Mutex<PrometheusEncoder>>,
    path: String,
    expire_after: Duration,
    /// Derived from `max_series` once, at bind time, by [`response_timeout`] -- the whole-connection
    /// deadline, separate from hyper's own [`HEADER_READ_TIMEOUT`].
    response_timeout: Duration,
    telemetry: Telemetry,
    clock: Clock,
}

/// `prometheus_out`, in whichever of its two shapes the config selected -- see the module doc for
/// the whole spec. One enum rather than two unrelated sinks because it is one `kind:` with one
/// name in `docs/design/pipeline-graph.md`'s table, one `buffer:`, and one set of encoder
/// counters; graph rule 56 is what guarantees the choice is unambiguous, so `build_spec` picks a
/// variant and nothing downstream branches again.
// The two variants differ in size by a few hundred bytes (`RemoteWriteOutput` carries a
// `reqwest::Client` and a `HeaderMap`), which is what clippy is pointing at -- and it costs
// nothing here: exactly one of these exists per configured component, built once at startup and
// immediately boxed as `NodeSpec::Output`'s `Box<dyn Output>`. Boxing a variant to even them out
// would add an indirection to every `send` to save a one-off allocation of the larger size.
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

/// Pure delegation -- every method's contract, including [`Output::duplicate_safe`]'s, is the
/// selected mode's and is documented there. Nothing is decided at this level.
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

/// The `bind:` half: `logit_pipeline::Output` for the exposition endpoint -- see the module doc.
pub struct ExposeOutput {
    bind: String,
    path: String,
    expire_after: Duration,
    max_series: usize,
    registry: Arc<Mutex<Registry>>,
    /// The address actually bound, once [`ExposeOutput::bind`] has run -- and the reason
    /// `bind` is where the socket is opened rather than lazily on first request: a test binds
    /// `127.0.0.1:0` and reads the real port back from here.
    local_addr: Option<SocketAddr>,
    /// The accept loop. `Some` is also this sink's "already bound" flag, which is what makes
    /// [`Output::bind`] idempotent; the [`TcpListener`] itself is owned by that task, so aborting
    /// it (in [`Output::flush`]) closes the port.
    server: Option<JoinHandle<()>>,
    /// Shared with the request handler, because both sides of this sink encode: `send` converts a
    /// batch with `events_to_families`, and a render counts what [`text::write_with`] has to drop.
    /// **Lock order is encoder before registry**, the one order both paths take -- `send` releases
    /// the encoder before touching the registry, and a render holds both in that order.
    encoder: Arc<Mutex<PrometheusEncoder>>,
    /// The source of truth the encoder is rebuilt from by each builder below, and separately the
    /// handle the accept loop reports its own `accept_failed` under.
    diag: Diagnostics,
    telemetry: Telemetry,
    clock: Clock,
}

impl ExposeOutput {
    /// `bind` is `host:port`, resolved when [`Output::bind`] runs, not at config-load time -- the
    /// same `syslog_out`/`statsd_out` precedent for an address in config.
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

    /// `Duration::ZERO` disables expiry -- see the module doc.
    pub fn with_expire_after(mut self, expire_after: Duration) -> Self {
        self.expire_after = expire_after;
        self
    }

    pub fn with_max_series(mut self, max_series: usize) -> Self {
        self.max_series = max_series;
        self
    }

    /// Reaches the codec's encoder, which is what actually reports the throttled
    /// `delta_temporality_unresolved`/`gauge_delta_unresolved` diagnostics -- and the accept loop,
    /// whose own `accept_failed` is an independent throttle key.
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

    /// `telemetry`/`diag` are the source of truth; the encoder is derived from them. Replacing the
    /// whole encoder (rather than mutating through the lock) keeps
    /// `PrometheusEncoder`'s own `mut self -> Self` builders usable as written, and every call is a
    /// builder running before `bind` -- so the `Arc` the handler later clones always holds the
    /// finished article.
    fn rebuild_encoder(&mut self) {
        self.encoder = Arc::new(Mutex::new(
            PrometheusEncoder::new()
                .with_telemetry(self.telemetry.clone())
                .with_diagnostics(self.diag.clone()),
        ));
    }

    /// Overrides what the expiry sweep reads as "now" -- see [`Clock`]. Tests only: production has
    /// exactly one clock, and offering a second in the public API would invite a sink whose
    /// `expire_after` quietly means something else.
    #[cfg(test)]
    fn with_clock(mut self, clock: Clock) -> Self {
        self.clock = clock;
        self
    }

    /// The address actually bound, or `None` before [`Output::bind`] has run. `127.0.0.1:0` in
    /// config plus this is how a test learns which ephemeral port to scrape.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }
}

#[async_trait::async_trait]
impl Output for ExposeOutput {
    /// Opens the exposition socket and starts serving it. Idempotent (a second call sees
    /// `self.server` already `Some` and returns), which is what lets the runtime's pre-spawn pass
    /// and `run_output`'s own lazy call both happen without binding twice -- see
    /// [`Output::bind`]'s contract.
    ///
    /// The server starts here, before any batch has arrived, rather than on the first `send`: a
    /// scraper polling a freshly-started `logit` should get an empty `200` (nothing has been
    /// delivered yet) rather than a connection refused it cannot distinguish from a crash.
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.server.is_some() {
            return Ok(());
        }
        let listener = TcpListener::bind(&self.bind)
            .await
            .with_context(|| format!("binding prometheus_out on '{}'", self.bind))?;
        let local_addr = listener.local_addr().context("reading prometheus_out's bound address")?;
        // The same `bound` lifecycle line every listener emits from its own `bind`
        // (`logit_inputs::udp`, `logit_inputs::otlp`) -- a sink that listens advances to
        // `NodeState::Bound` through the same pre-spawn pass, so it must not do so silently.
        // `local_addr`, not `self.bind`: a configured `:0` is the one case where what was asked for
        // and what was opened differ, and the port actually listening is the useful one.
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

    /// Upserts the batch into the registry and brings it back within `expire_after`/`max_series`.
    /// Never fails: there is no I/O here at all, only a lock and a `BTreeMap`.
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let resource = batch.resource.as_ref();
        // Encoder first, registry second -- the one lock order both this and a render take. The
        // encoder is released here, before the registry is touched, so a scrape holding both never
        // waits on a conversion.
        let families = {
            let mut encoder = lock(&self.encoder);
            events_to_families(batch.events.iter().map(|event| (resource, event)), &mut encoder)
        };
        let now = (self.clock)();
        // One critical section for the whole batch: upsert, then bring the registry back within
        // both bounds, so a scrape racing this `send` never observes a registry that is over its
        // cap or still holding series past their `expire_after`.
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

    /// Stops serving. Aborting the accept task is the whole teardown: it owns the [`TcpListener`],
    /// so dropping its future closes the port, and each in-flight connection is already bounded by
    /// [`response_timeout`]. Nothing is buffered here to flush -- `send` has already committed every
    /// batch to the registry by the time it returns. [`Drop`] does the same thing, for the paths
    /// that never reach a graceful `flush` at all.
    async fn flush(&mut self) -> anyhow::Result<()> {
        if let Some(server) = self.server.take() {
            server.abort();
        }
        Ok(())
    }

    /// `true`: `send` is an idempotent replace into an in-memory registry and touches no network,
    /// so a redelivered batch writes the same values a second time and changes nothing. Unlike
    /// `statsd_out`, there is no counter at a destination to double.
    fn duplicate_safe(&self) -> bool {
        true
    }
}

/// Aborts the accept loop when the sink itself is dropped, not only when `flush` runs. `flush` is
/// the *graceful* teardown and the runtime calls it on every path it finishes a sink on -- but not on
/// the startup-failure path: if a later component's `bind` fails, `run_with_telemetry` returns
/// `RunError::Startup` and drops every spec without flushing anything, which would leave this
/// listener holding its port for the rest of the runtime's life. Moot under the CLI (the process is
/// exiting anyway) and real for in-process use and for any test that binds a sink and then fails
/// startup, so the listener gets the same non-`flush` teardown `logit-cli`'s own admin listener has.
///
/// `abort` rather than an await: `Drop` cannot be async, and abort is all `flush` does anyway -- the
/// task owns the `TcpListener`, so dropping its future closes the port.
impl Drop for ExposeOutput {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

/// The `endpoint:` half: a stateless remote-write sender -- see the module doc for the wire, the
/// `Fault` table, and why [`RemoteWriteOutput::duplicate_safe`] is `true`.
///
/// Nothing is retained between batches, which is what makes this the opposite shape from
/// [`ExposeOutput`]: no registry, no expiry sweep, no cardinality cap, no lock. `send` takes
/// `&mut self`, so the encoder is a plain field rather than the `Arc<Mutex<..>>` a shared request
/// handler forces on the other mode.
pub struct RemoteWriteOutput {
    /// The absolute write URL, path included. Never parsed here -- `reqwest` does that per
    /// request, and graph rule 56 has already checked the scheme and authority.
    endpoint: String,
    version: remote_write::Version,
    request_timeout: Duration,
    client: reqwest::Client,
    /// The operator's `headers:`, built into a `HeaderMap` once at construction rather than per
    /// request. The protocol's own four are inserted *over* a clone of this on each request --
    /// see [`RemoteWriteOutput::request_headers`].
    headers: HeaderMap,
    /// `Some` only once [`RemoteWriteOutput::with_tls`] has built one from `endpoint_tls:`;
    /// `None` leaves `reqwest`'s own default trust (the bundled Mozilla root set) in place, which
    /// is already correct for an ordinary `https://` receiver.
    tls: Option<rustls::ClientConfig>,
    /// Rebuilt from `telemetry`/`diag` by the builders below, exactly as [`ExposeOutput`] does --
    /// the encoder is derived state, and `PrometheusEncoder`'s own `mut self -> Self` builders
    /// stay usable as written that way.
    encoder: PrometheusEncoder,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl RemoteWriteOutput {
    /// `endpoint` is the receiver's absolute write URL, path included -- resolved at request time,
    /// never at config-load time, the same `otlp_out`/`syslog_out` precedent for an address in
    /// config.
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

    /// Which remote-write message this sender writes (`version:` in config). No negotiation and no
    /// fallback -- see the module doc.
    pub fn with_version(mut self, version: remote_write::Version) -> Self {
        self.version = version;
        self
    }

    /// Per-request timeout (`timeout:` in config). Rebuilds the client, since that is where
    /// `reqwest`'s own client-wide default lives; the per-request `.timeout(..)` in `send` is what
    /// actually bounds a request either way, so the two are kept in step rather than left to
    /// disagree.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self.client = build_client(timeout, self.tls.as_ref());
        self
    }

    /// The extra headers sent on every request (`headers:` in config). Fails if a name or value
    /// isn't a legal HTTP header (graph rule 56 rejects a protocol-owned name before construction
    /// ever sees it; this catches the lexical shape `graph` can't -- illegal bytes, embedded
    /// newlines), and if two names collide once `HeaderName` normalizes their case, which
    /// `HeaderMap::insert` would otherwise resolve by whichever of the two the `HashMap` happened
    /// to iterate last. Both are the same defense-in-depth relationship `otlp_out::with_headers`
    /// has with its own rule.
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

    /// Client-side TLS tuning (`endpoint_tls:` in config) -- a private CA, a client certificate
    /// for mutual TLS, or disabling verification entirely. A no-op if `settings` is empty:
    /// `reqwest` already defaults to a working TLS configuration (the bundled Mozilla root set)
    /// for an `https://` endpoint without this ever being called. Graph rule 56 rejects a
    /// non-empty block on a non-`https://` endpoint and requires `cert_file`/`key_file` together
    /// before this ever runs -- this method still loads and validates every file itself, since
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

    /// Reaches the codec's encoder, which is what reports the throttled
    /// `delta_temporality_unresolved`/`gauge_delta_unresolved` diagnostics -- and this sink's own
    /// `remote_write_rejected`, an independent throttle key.
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

    /// The batch's events grouped by `Event::timestamp`, ascending, stable within a group -- the
    /// partition [`remote_write::encode`] inverts back into one `TimeSeries` per label set. A
    /// `BTreeMap` rather than a sort: a real batch carries one or two distinct timestamps, so this
    /// is a couple of map lookups per event rather than a comparison sort over all of them, and
    /// ascending order comes out of the map rather than out of a sort key.
    fn partition(batch: &EventBatch) -> BTreeMap<i64, Vec<&Event>> {
        let mut groups: BTreeMap<i64, Vec<&Event>> = BTreeMap::new();
        for event in &batch.events {
            groups.entry(event.timestamp).or_default().push(event);
        }
        groups
    }

    /// The operator's headers with the protocol's own four `insert`ed over them, so a
    /// protocol-owned name always wins whatever `headers:` said. `insert` (not `RequestBuilder`'s
    /// append-semantics `.header(..)`), and one `.headers(..)` call at the call site, for exactly
    /// the reason `otlp_out::send_http` spells out: mixing the two would undo this guarantee.
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

/// One encoder configured the way this transport needs it, in one place so `new` and both
/// telemetry builders cannot drift: `with_timestamps_always` because the wire has no way to omit a
/// timestamp and a series without one would be silently skipped and counted; `with_stale_markers`
/// because remote-write *does* have a spelling for "this series is gone" and an exposition of the
/// same data does not. See the module doc's "The wire".
fn new_sender_encoder(telemetry: &Telemetry, diag: &Diagnostics) -> PrometheusEncoder {
    PrometheusEncoder::new()
        .with_stale_markers(true)
        .with_timestamps_always(true)
        .with_telemetry(telemetry.clone())
        .with_diagnostics(diag.clone())
}

#[async_trait::async_trait]
impl Output for RemoteWriteOutput {
    /// Nothing to bind: this sink dials out per request. `Ok(())` rather than the trait's default
    /// so the whole `Output` surface reads from one place in this file.
    async fn bind(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// One batch, one request -- see the module doc's "The wire" and "Faults, retries and
    /// duplicate safety". Exactly one attempt: retry is `write_loop`'s job.
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
        // No request at all for a batch that produced nothing -- a batch of logs reaching a
        // metrics sink, or one whose every record was skipped and counted. An empty `WriteRequest`
        // is a legal message, but sending one would turn a no-op into network traffic and into a
        // `requests{class="2xx"}` an operator would read as a delivery.
        if groups.iter().all(Vec::is_empty) {
            return Ok(());
        }
        let (body, samples) =
            remote_write::encode_counted(&groups, self.version, &mut self.encoder);
        // Snappy *block* format (`snap::raw`), which is what both specs mean by
        // `Content-Encoding: snappy` -- never the framed format `snap::write` produces.
        // Infallible in practice: `compress_vec` only errors on an input past `u32::MAX`, which a
        // batch cannot reach, but it is reported rather than unwrapped so an absurd one fails the
        // batch instead of the process.
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
                // The body, not just the status: a Prometheus-style `400` names the offending
                // series (`out of order sample`, `duplicate sample for timestamp`, a label that
                // failed validation), and that is the only actionable thing in the exchange.
                // Truncated, because a receiver under load can answer with a great deal of it.
                let body = response.text().await.unwrap_or_default();
                let snippet = body_snippet(&body);
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

    /// Nothing is buffered here: `send` has already issued (or deliberately not issued) its one
    /// request by the time it returns.
    async fn flush(&mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// `true`, and load-bearing -- see the module doc's "Faults, retries and duplicate safety".
    /// A sample's identity at a remote-write receiver is `(label set, timestamp)`, and this sink
    /// re-encodes a retried batch from the same events, so a replayed request is an idempotent
    /// overwrite rather than a second sample. And `true` is what selects
    /// `DeliveryPosture::AtLeastOnce` (`logit_pipeline::output`'s `from_duplicate_safe` and
    /// `is_retryable`), the only posture under which the `Fault::Ambiguous` a 5xx produces is
    /// retried at all: `false` here would silently turn every 5xx into a dropped batch.
    fn duplicate_safe(&self) -> bool {
        true
    }
}

/// The first [`ERROR_BODY_SNIPPET_BYTES`] of a rejection body, on a character boundary so the
/// result is always printable, with an ellipsis when anything was cut. A `String` is UTF-8 by
/// construction, so `floor_char_boundary`'s work is all this needs.
fn body_snippet(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.len() <= ERROR_BODY_SNIPPET_BYTES {
        return trimmed.to_string();
    }
    let mut end = ERROR_BODY_SNIPPET_BYTES;
    while end > 0 && !trimmed.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}...", &trimmed[..end])
}

/// Poisoning cannot lose data here -- a panic while holding this lock would have to come from
/// inside `BTreeMap`, and the registry is rebuilt by the next `send` regardless -- so a poisoned
/// lock is recovered rather than propagated, exactly as `logit_core::Registry` does with its own.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// The accept loop, mirroring `logit-cli::admin::serve_on`: one spawned task per connection,
/// permit acquired *after* accept so the kernel backlog absorbs a burst, and a per-connection
/// timeout. Returns nothing because no failure here has anywhere to go -- this task is only ever
/// aborted, never joined, so an `Err` would vanish unread.
async fn serve(listener: TcpListener, state: Arc<ServerState>, mut diag: Diagnostics) {
    let connection_limit = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _peer)) => stream,
            Err(err) => {
                // One failed `accept()` must never end this loop -- see admin.rs's own comment for
                // the full reasoning. A client's own accident is retried immediately; anything
                // else (fd pressure) gets a short pause first so it cannot spin a core.
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
            // Two deadlines, not one: hyper bounds how long request *headers* may take to
            // arrive (the slowloris case, cheap to bound tightly), while the outer timeout bounds
            // the whole connection including the body write, which scales with `max_series` and
            // must outlast the scraper's own `scrape_timeout`.
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

    // The sweep runs here as well as in `send` so a sink that has gone quiet still stops exposing
    // series past their `expire_after`, rather than freezing whatever it last held. Cheap: one
    // `Instant` comparison per series, under a lock nothing else contends for between scrapes.
    // Rendered under the same lock, then released before the response is built.
    let body = {
        let mut encoder = lock(&state.encoder);
        let mut registry = lock(&state.registry);
        registry.sweep(state.expire_after, (state.clock)(), &state.telemetry);
        registry.render(dialect, &mut encoder)
    };

    let body = if gzip { gzip_encode(&body) } else { body };
    // Counted here, when the body is *rendered*, not when the last byte reaches the client: the
    // response is a `Full<Bytes>`, which offers no body-completion hook to count from, so a
    // connection that dies mid-write still lands as `ok`. See `SCRAPE_BYTES`' own doc comment.
    state.telemetry.count(SCRAPES, 1.0, &[("class", "ok")]);
    state.telemetry.count(SCRAPE_BYTES, body.len() as f64, &[]);

    let mut builder = http::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", dialect.content_type())
        // The representation genuinely varies on both, and the module doc tells operators to front
        // this endpoint with a proxy -- without `Vary` a caching intermediary is entitled to hand a
        // text-0.0.4 scraper an OpenMetrics (or gzipped) body it never asked for.
        .header("vary", "Accept, Accept-Encoding");
    if gzip {
        builder = builder.header("content-encoding", "gzip");
    }
    Ok(builder.body(Full::new(Bytes::from(body))).expect("a well-formed response always builds"))
}

/// A header value as `&str`, or `None` for one that isn't valid ASCII -- an `Accept` this codec
/// cannot read is treated as absent (text 0.0.4), never as an error: a scrape must not fail over a
/// malformed negotiation header.
fn header_str(value: &http::HeaderValue) -> Option<&str> {
    value.to_str().ok()
}

/// OpenMetrics only when the client actually asks for it; text 0.0.4 for everything else,
/// including no header at all and a bare `*/*` -- the same default Prometheus's own server applies.
/// A substring match rather than a full `Accept` parse: `q` weights between the two exposition
/// dialects are not a thing any real scraper sends, and naming the type at all is the ask.
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

/// Whether `Accept-Encoding` offers `gzip` at a non-zero weight. `gzip;q=0` is an explicit refusal
/// (RFC 9110 §12.5.3), so it must not be read as an offer just because the token is present.
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
    // Both calls write into a `Vec`, which cannot fail; `expect` rather than a fallible signature
    // so the handler has no error branch that can never be taken.
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

    /// One of every shape the exposition format can carry, as the exposition-fixture batch every
    /// byte-exact test below renders: a cumulative counter whose model name lacks `_total`, a
    /// gauge, a cumulative histogram, a summary, an `info`-typed gauge, and a series carrying its
    /// own wire timestamp.
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

    /// A bound sink plus the base URL to scrape it at. `expire_after: 0s` unless a test says
    /// otherwise, so nothing expires out from under a byte-exact assertion.
    async fn bound(sink: ExposeOutput) -> (ExposeOutput, String) {
        let mut sink = sink;
        sink.bind().await.expect("binding an ephemeral port should succeed");
        let addr = sink.local_addr().expect("bind records the address");
        (sink, format!("http://{addr}"))
    }

    async fn fixture_sink() -> (ExposeOutput, String) {
        let mut sink = ExposeOutput::new("127.0.0.1:0").with_expire_after(Duration::ZERO);
        sink.send(&fixture_batch()).await.expect("send never fails");
        bound(sink).await
    }

    async fn get(url: &str, headers: &[(&str, &str)]) -> reqwest::Response {
        let client = reqwest::Client::builder()
            // Off, so a gzip body arrives exactly as this sink wrote it rather than being
            // transparently inflated (and its `content-encoding` stripped) by the client.
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

    /// Cumulative semantics: the second delivery of a series replaces the first, it does not
    /// accumulate and it does not appear twice.
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

    /// A hand-advanced clock, so this pins the `expire_after` boundary itself rather than waiting
    /// on one: at exactly the window the series is still live, past it it is gone and counted.
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

    /// The cap evicts the *least recently updated* series, not an arbitrary one. The oldest series
    /// is deliberately `id="z"`, which sorts **last** in the registry's `BTreeMap`: an
    /// implementation that evicted the first series in iteration order would keep `z` and drop `a`,
    /// so this distinguishes LRU from map order rather than passing on a coincidence.
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

    /// The regime the cap actually operates in: thousands of series, hundreds over the cap, evicted
    /// in **one** pass. Pins *which* ones go -- the oldest, by `updated_at` -- not just how many, so
    /// a one-pass rewrite cannot quietly evict the wrong half.
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

        // A later batch of NEW fresh series takes the total to CAP + NEW, so exactly NEW have to
        // go -- and every `old_*` is strictly older than every `new_*`.
        *lock(&now) = start + Duration::from_secs(1);
        sink.send(&batch(series("new", NEW))).await.unwrap();
        assert_eq!(counter(&registry, SERIES_EVICTED, "reason", "cardinality"), NEW as f64);

        let (_sink, url) = bound(sink).await;
        let body = get(&format!("{url}/metrics"), &[]).await.text().await.unwrap();
        assert_eq!(body.lines().filter(|l| l.starts_with("hits_total{")).count(), CAP);
        // Every `old_*` shares one `updated_at`, so the documented key tie-break decides between
        // them: the evicted set is exactly the NEW lowest `old_*` keys.
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

    /// One name cannot carry two `# TYPE` lines, so the newer type wins and the old series go --
    /// a gauge arriving for a name registered as a counter must not leave a counter behind.
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

    /// The codec owns every lossy path, and this proves the sink's own encoder is wired to the
    /// component's real `Telemetry` so those counters actually reach the pipeline.
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

    /// A render's own drops count under the sink's identity, not a throwaway encoder's: only one
    /// exemplar fits a counter's single `_total` line, so the second is dropped and counted. The
    /// drop happens in the *writer*, which is exactly what `text::write_with` exists to report --
    /// scraping as text 0.0.4 would drop both uncounted (a dialect choice, not a lossy mapping), so
    /// this asks for OpenMetrics.
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

    /// `Output::bind`'s idempotency obligation: the second call must not try to open the port
    /// again (which would fail, since the first call is holding it), and must not leave a second
    /// accept loop behind.
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

    /// `flush` is the sink's teardown, and this sink's teardown is "stop serving": the port must
    /// actually close, not merely stop being refreshed.
    #[tokio::test]
    async fn flush_stops_the_server_and_closes_the_port() {
        let (mut sink, url) = fixture_sink().await;
        assert_eq!(get(&format!("{url}/metrics"), &[]).await.status(), 200);
        sink.flush().await.expect("flush never fails");
        let refused = reqwest::Client::new().get(format!("{url}/metrics")).send().await;
        assert!(refused.is_err(), "the port should be closed after flush, got {refused:?}");
    }

    /// The startup-failure path in miniature: a bound sink that is *dropped* without a graceful
    /// `flush` -- which is exactly what `run_with_telemetry` does to every spec when a later
    /// component's `bind` fails -- must not leave its accept loop holding the port.
    #[tokio::test]
    async fn dropping_an_unflushed_bound_sink_closes_the_port() {
        let (sink, url) = bound(ExposeOutput::new("127.0.0.1:0")).await;
        assert_eq!(get(&format!("{url}/metrics"), &[]).await.status(), 200);

        drop(sink); // no flush, exactly as a startup failure would
                    // The abort has to be observed by the runtime before the listener is really gone; a
                    // scrape that still connects retries until it doesn't, rather than racing on one attempt.
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

    /// Nothing has been delivered yet, and a scraper polling a freshly-started process must be
    /// able to tell "up, no data" from "not listening".
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

    /// Sums `name`'s counter points carrying `tag=value` out of a *drained* event list -- for a
    /// test that drains once and then asks several questions of the result.
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

    /// One request as a canned receiver saw it -- enough to assert everything the wire contract
    /// promises without either side sharing code with the other.
    #[derive(Debug, Clone)]
    struct CapturedRequest {
        method: Method,
        path: String,
        headers: http::HeaderMap,
        /// Still Snappy-compressed, exactly as it arrived.
        body: Vec<u8>,
    }

    impl CapturedRequest {
        fn header(&self, name: &str) -> Option<&str> {
            self.headers.get(name).and_then(|v| v.to_str().ok())
        }

        /// The decompressed protobuf body -- Snappy *block* format, which is what both specs mean
        /// by `Content-Encoding: snappy`. A test asserting this decompresses at all is asserting
        /// the sink didn't reach for the framed encoder.
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

    /// A real HTTP/1.1 receiver answering `status` with `body` and recording every request it
    /// saw. A real server rather than a raw-socket canned response (`otlp.rs`'s pattern) because
    /// these assertions are about the *request*: the headers, the path, and a body this test then
    /// decompresses and prost-decodes.
    async fn canned_receiver(
        status: StatusCode,
        body: &'static str,
    ) -> (String, Arc<Mutex<Vec<CapturedRequest>>>) {
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
                                        record(req, seen, status, body).await,
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
                                        record(req, seen, StatusCode::OK, "").await,
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
        status: StatusCode,
        body: &'static str,
    ) -> http::Response<Full<Bytes>> {
        use http_body_util::BodyExt as _;
        let (parts, incoming) = req.into_parts();
        let collected = incoming.collect().await.map(|b| b.to_bytes()).unwrap_or_default();
        seen.lock().unwrap().push(CapturedRequest {
            method: parts.method,
            path: parts.uri.path().to_string(),
            headers: parts.headers,
            body: collected.to_vec(),
        });
        http::Response::builder()
            .status(status)
            .body(Full::new(Bytes::from_static(body.as_bytes())))
            .expect("a well-formed response always builds")
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

    /// `logit-outputs` lives at `crates/logit-outputs`; the fixtures live at the repo root's
    /// `testdata/tls` (`testdata/tls/README.md`) -- two levels up from `CARGO_MANIFEST_DIR`.
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
        // Rule 56 rejects `content-type` in `headers:` at config time -- this proves the
        // defense-in-depth guarantee directly, bypassing that rule via `with_headers`.
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

    /// The timestamp partition, inverted: three events at three instants for one series become
    /// **one** `TimeSeries` with three samples in ascending timestamp order, not three series.
    #[tokio::test]
    async fn a_multi_timestamp_batch_becomes_one_timeseries_with_ordered_samples() {
        let (url, seen) = canned_receiver(StatusCode::OK, "").await;
        let mut sink = sender(&url);
        // Deliberately out of order in the batch: ascending order is the partition's, not the
        // caller's.
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

    /// `with_stale_markers(true)`: a `FLAG_NO_RECORDED_VALUE` gauge is Prometheus's own stale
    /// marker on the wire, not a skipped series. The exposition path would skip and count it.
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

    /// Delta temporality has no spelling in remote-write either -- skipped and counted, with the
    /// same named fix the exposition path gives.
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
        // Two series at two timestamps: four samples, which is neither the family count (2) nor
        // the event count (4 records over 2 events) by accident.
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

    /// A `400` is permanent, and its body is the useful half of the exchange -- Prometheus names
    /// the offending series in it.
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

    /// The snippet is bounded: a receiver answering with a page of HTML must not become a log
    /// line of one.
    #[test]
    fn a_long_rejection_body_is_truncated_on_a_character_boundary() {
        let long = "é".repeat(400);
        let snippet = body_snippet(&long);
        assert!(snippet.len() <= ERROR_BODY_SNIPPET_BYTES + 3, "got {} bytes", snippet.len());
        assert!(snippet.ends_with("..."));
        assert_eq!(body_snippet("  short  "), "short", "trimmed, and not truncated");
    }

    #[tokio::test]
    async fn a_refused_connection_is_clean() {
        // Bound and immediately dropped, so the port is (almost certainly) unused and closed --
        // `influxdb.rs`'s own "nothing is listening" pattern.
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

    /// `true` is what selects `AtLeastOnce`, which is the only posture under which the
    /// `Fault::Ambiguous` a 5xx produces is retried -- see the module doc.
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
        // `let ... else`, not `expect_err`: the `Ok` side is a whole sink, which has no reason to
        // be `Debug` just so a test can name it.
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
