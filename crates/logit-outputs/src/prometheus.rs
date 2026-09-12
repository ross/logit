//! `prometheus_out`: a Prometheus/OpenMetrics **exposition** endpoint -- a sink that holds a
//! registry of current series and renders it on demand, rather than writing anything anywhere.
//! The mirror of `logit_inputs::prometheus` (scrape), and the fourth like-protocol pair under
//! [ADR `lossless-transit`](../../../docs/adr/lossless-transit.md). The wire syntax and the whole
//! model↔families mapping live in `logit_proto::prometheus`
//! ([`text::write`], [`events_to_families`]); nothing here knows what a sample line looks like.
//!
//! **This module doc is the spec** (house convention, see [`crate::statsd`]'s). The authority is
//! [ADR `prometheus-scrape-and-exposition`](../../../docs/adr/prometheus-scrape-and-exposition.md)
//! -- "Exposition state and expiry", "Dialects and negotiation", "`Output::bind`".
//!
//! ## Config
//!
//! ```yaml
//! kind: prometheus_out
//! bind: "127.0.0.1:9464"   # required; loopback in every example -- see "Security posture"
//! path: /metrics           # default
//! expire_after: 5m         # a series not updated within this window stops being exposed; 0s off
//! max_series: 100000       # hard cap; least-recently-updated evicted first
//! ```
//!
//! `buffer:` works unchanged -- it is a sibling of `kind:` on every sink, and this one is no
//! different (though a sink whose `send` never fails will never actually back one up).
//!
//! A future remote-write **sender** is an optional `endpoint:` on this same variant, mutually
//! exclusive with `bind:` by a graph rule -- purely additive, see the ADR's forward-compatibility
//! section. There is no `endpoint:` today.
//!
//! ## Dialect negotiation
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
//! ## Routes
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
//! ## State: upsert, expiry, cardinality
//!
//! [`PrometheusOutput::send`] converts the batch with [`events_to_families`] (every lossy path --
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
//! ## Telemetry
//!
//! | Point | Meaning |
//! |---|---|
//! | `logit.output.scrapes{class="ok"\|"not_found"\|"method"}` | one per HTTP request, by outcome. `ok` means the response was *rendered*, not acknowledged: a `Full<Bytes>` response offers no body-completion hook, so a connection dropped mid-write still counts `ok` |
//! | `logit.output.scrape.bytes` | response body bytes as rendered -- post-gzip when the client asked for it, so it measures transfer cost rather than exposition size. Counted when the body is built, same caveat as `ok` above |
//! | `logit.output.series` (gauge) | series held after each `send` |
//! | `logit.output.series.evicted{reason="expired"\|"cardinality"}` | see above |
//! | `logit.output.metrics.type_conflict` | see above |
//!
//! plus everything the codec counts on the [`PrometheusEncoder`] this sink hands it:
//! `logit.output.metrics.{skipped,degraded}` and `logit.output.labels.dropped`. A delta `Sum`
//! reaching this sink is the common one -- skipped, counted
//! `logit.output.metrics.skipped{metric_kind="delta_sum"}`, with a throttled
//! `delta_temporality_unresolved` diagnostic naming the fix (an `aggregate` with
//! `temporality: cumulative` in front of the sink).
//!
//! **Both directions count under one encoder.** `send` and a render share a single
//! [`PrometheusEncoder`], so the drops each side reaches -- `send`'s unrepresentable labels and
//! skipped kinds, a render's `degraded{reason="exemplar_dropped"|"unit_not_suffix"}` from
//! [`text::write_with`] -- add up as this component's totals rather than splitting across two
//! encoder identities for the same sink. The lock order that implies (encoder before registry) is
//! on [`PrometheusOutput::encoder`].
//!
//! ## Security posture: no TLS, no auth
//!
//! This server serves the **entire registry** -- every label on every series it currently holds --
//! to anything that connects to `bind:`, with no credential check and no transport encryption. That
//! is the same posture [ADR `admin-readiness-endpoint`](../../../docs/adr/admin-readiness-endpoint.md)
//! accepted for `/readyz`, with one difference that matters: `bind:` here is **required**, not
//! off-by-default, and the payload is a metric surface rather than a lifecycle word. So: bind
//! loopback or pod-local (`127.0.0.1:9464`, as every shipped example does) and let something that
//! does have TLS and auth front it. An operator who needs this reachable from off-host is making
//! that choice deliberately, not inheriting it from an example. Tracked in `docs/known-gaps.md`
//! next to `admin:`'s own row.

use anyhow::Context;
use bytes::Bytes;
use http::{Method, StatusCode};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use logit_core::{Diagnostics, EventBatch, Exemplar, Telemetry};
use logit_pipeline::Output;
use logit_proto::prometheus::{
    events_to_families, text, Dialect, FamilyType, MetricFamily, Point, PrometheusEncoder, Series,
};
use std::collections::BTreeMap;
use std::io::ErrorKind;
use std::net::SocketAddr;
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

const SCRAPES: &str = "logit.output.scrapes";
/// Response body bytes as *rendered* (post-gzip when the client asked for it), counted when the
/// body is built rather than when its last byte is acknowledged -- a `Full<Bytes>` response has no
/// body-completion hook to count from, so a connection dropped mid-write is still counted here and
/// still counted `scrapes{class="ok"}`.
const SCRAPE_BYTES: &str = "logit.output.scrape.bytes";
const SERIES: &str = "logit.output.series";
const SERIES_EVICTED: &str = "logit.output.series.evicted";
const TYPE_CONFLICT: &str = "logit.output.metrics.type_conflict";

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
    /// the drop was reached. See [`PrometheusOutput::encoder`] for the lock ordering it implies.
    encoder: Arc<Mutex<PrometheusEncoder>>,
    path: String,
    expire_after: Duration,
    /// Derived from `max_series` once, at bind time, by [`response_timeout`] -- the whole-connection
    /// deadline, separate from hyper's own [`HEADER_READ_TIMEOUT`].
    response_timeout: Duration,
    telemetry: Telemetry,
    clock: Clock,
}

/// `logit_pipeline::Output` for `prometheus_out` -- see the module doc for the whole spec.
pub struct PrometheusOutput {
    bind: String,
    path: String,
    expire_after: Duration,
    max_series: usize,
    registry: Arc<Mutex<Registry>>,
    /// The address actually bound, once [`PrometheusOutput::bind`] has run -- and the reason
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

impl PrometheusOutput {
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
impl Output for PrometheusOutput {
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
impl Drop for PrometheusOutput {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
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
    async fn bound(sink: PrometheusOutput) -> (PrometheusOutput, String) {
        let mut sink = sink;
        sink.bind().await.expect("binding an ephemeral port should succeed");
        let addr = sink.local_addr().expect("bind records the address");
        (sink, format!("http://{addr}"))
    }

    async fn fixture_sink() -> (PrometheusOutput, String) {
        let mut sink = PrometheusOutput::new("127.0.0.1:0").with_expire_after(Duration::ZERO);
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0").with_expire_after(Duration::ZERO);
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0");
        sink.bind().await.expect("the first bind should succeed");
        let first = sink.local_addr().expect("bind records the address");
        sink.bind().await.expect("a second bind must be a harmless no-op");
        assert_eq!(sink.local_addr(), Some(first));
    }

    #[tokio::test]
    async fn binding_an_address_already_in_use_fails_with_the_address_in_the_message() {
        let mut first = PrometheusOutput::new("127.0.0.1:0");
        first.bind().await.unwrap();
        let addr = first.local_addr().unwrap();
        let err = PrometheusOutput::new(addr.to_string())
            .bind()
            .await
            .expect_err("the port is already held");
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
        let (sink, url) = bound(PrometheusOutput::new("127.0.0.1:0")).await;
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
        let (_sink, url) = bound(PrometheusOutput::new("127.0.0.1:0")).await;
        let response = get(&format!("{url}/metrics"), &[]).await;
        assert_eq!(response.status(), 200);
        assert_eq!(response.text().await.unwrap(), "");
    }

    #[tokio::test]
    async fn send_is_duplicate_safe_because_it_only_replaces_in_memory_state() {
        assert!(PrometheusOutput::new("127.0.0.1:0").duplicate_safe());
    }

    // -- telemetry --------------------------------------------------------------------------

    #[tokio::test]
    async fn each_request_is_counted_by_outcome_class_with_the_bytes_it_served() {
        let registry = logit_core::Registry::new();
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
        let mut sink = PrometheusOutput::new("127.0.0.1:0")
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
}
