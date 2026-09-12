//! `prometheus_in`: scrapes Prometheus `/metrics` endpoints on an interval, the way Prometheus's
//! own server does. See [ADR `prometheus-scrape-and-exposition`](../../../docs/adr/prometheus-scrape-and-exposition.md)
//! for the full design; this module doc is the implementation's own spec.
//!
//! ## Config
//!
//! ```yaml
//! targets: ["http://node-exporter:9100/metrics"]   # required, non-empty, absolute http(s) URLs
//! interval: 15s        # scrape cadence; default 15s
//! timeout: 10s          # per-request timeout; default 10s
//! headers: {}           # optional extra request headers
//! tls: {}                # TlsClientConfig -- only meaningful when a target is https://
//! ```
//!
//! A future `bind:` field on the same `ComponentKind::PrometheusIn` variant (a remote-write
//! receiver) is planned as an additive, non-breaking change -- "exactly one of `targets`/`bind`"
//! would become a graph rule once it lands, not a new kind (see the ADR's "Remote-write forward
//! compatibility" section).
//!
//! ## Modeled on `internal.rs`
//!
//! Interval-driven, like [`crate::internal::InternalInput`]: [`Input::run`] owns a
//! `tokio::time::interval` ticker, swallows its immediate first tick (so the first real scrape
//! happens after one full `interval` has elapsed, not at t=0), and calls [`PrometheusInput::tick`]
//! -- a plain, non-trait method, directly testable without a runtime harness. Unlike `internal`,
//! there is no `bind` override: a scrape client has no socket of its own to open ahead of time.
//!
//! One deliberate difference from `internal.rs`: the ticker's missed-tick behavior is set to
//! `Delay`, not the default `Burst`. `internal`'s own tick never does network I/O, so `Burst`
//! (fire every missed tick back-to-back the moment a stall clears) is harmless there; this tick
//! awaits `sink.send` (bounded-channel backpressure) plus a per-target request timeout, so a
//! downstream stall lasting several intervals is ordinary here, and `Burst` would turn it into N
//! full scrape rounds fired in a row -- `Delay` resumes on a fixed cadence instead, matching
//! Prometheus's own scrape scheduler, which skips a missed scrape rather than bursting to catch up.
//!
//! ## Dialect negotiation
//!
//! Every request carries `Accept: application/openmetrics-text;version=1.0.0,text/plain;
//! version=0.0.4;q=0.5,*/*;q=0.1` and `User-Agent: logit/<CARGO_PKG_VERSION>` -- but, per the ADR,
//! this input never *forces* a dialect: which one a target actually sent is read back off the
//! response's own `Content-Type` via [`Dialect::from_content_type`] (`application/
//! openmetrics-text` selects OpenMetrics; anything else, including a missing header, is text
//! 0.0.4), so a target that ignores `Accept` entirely and always answers in text 0.0.4 still
//! decodes correctly.
//!
//! ## Resource identity
//!
//! Built once per target in [`PrometheusInput::new`], never per scrape: an unprefixed `instance`
//! (`host:port` -- port defaulted to 80/443 when the target URL omits one, exactly matching
//! Prometheus's own scrape) via [`logit_proto::prometheus::LABEL_INSTANCE`], and
//! [`logit_proto::prometheus::ATTR_TARGET`], the scrape URL with its userinfo (`user:pass@`, a
//! legitimate way to put HTTP basic-auth credentials in a scrape URL) and query string stripped
//! ([`redact_url`]) -- unlike `instance`, this attribute rides on every event this target
//! produces, reaching whatever sink the pipeline routes it to, so it must never carry a
//! credential in cleartext. Both ride on every batch this target ever produces, including its
//! synthetic metrics, which is what keeps two targets exposing the same exporter from colliding
//! once relayed onward.
//!
//! ## Synthetic scrape metrics
//!
//! Every tick, every target -- scrape failures included -- gets exactly three synthetic series
//! appended to its batch, on that target's resource, with no wire timestamp marker: `up`
//! (`Gauge(0|1)`), `scrape_duration_seconds` (`Gauge`, wall-clock seconds for that target's own
//! request), and `scrape_samples_scraped` (`Gauge`, the number of series successfully decoded --
//! `0` on any failure). See the ADR's "Synthetic scrape metrics" section for why these three, and
//! why they're always on.
//!
//! ## Counters
//!
//! `logit.input.scrapes{class}` -- one count per target per tick, `class` one of `2xx`/`4xx`/`5xx`/
//! `other` (HTTP response classes), `network_error`, `timeout`, `parse_error` (a 2xx response body
//! that failed to parse), or `oversize` (a response body that exceeded [`MAX_SCRAPE_BYTES`]).
//! `logit.input.scrape.duration` -- a timing sample per target per tick, recorded regardless of
//! outcome. `logit.input.samples` -- the number of series decoded, summed across every target
//! (`0` contributes nothing but is still a well-formed call). A scrape failure also reports
//! `Diagnostics::warn_throttled("scrape_failed", ..)`, with the failing target's [`redact_url`]ed
//! form in the message text only -- never a tag (a target URL isn't `&'static` and isn't safe to
//! intern per-target, `AGENTS.md`'s tag-cardinality convention) and never the raw URL, which may
//! carry a credential.

use crate::tls::apply_client_tls;
use crate::Input;
use http::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE, USER_AGENT};
use logit_core::interner::intern;
use logit_core::{
    AttrMap, Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::prometheus::{
    families_to_events, text, Dialect, PrometheusDecoder, ATTR_TARGET, LABEL_INSTANCE,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

/// Hard cap on one scrape response's body, read incrementally via [`reqwest::Response::chunk`] --
/// a hostile or misconfigured exporter can't grow this input's memory unboundedly. 32 MiB is
/// generous for even a very large `/metrics` page while still being a real bound; an exporter that
/// legitimately needs more is a config problem to fix at the source, not something to raise this
/// for silently.
const MAX_SCRAPE_BYTES: usize = 32 * 1024 * 1024;

/// Never forces a dialect (per the ADR's "Dialects and negotiation" section) -- this just tells a
/// target that speaks either that OpenMetrics is preferred, for a target that bothers to read
/// `Accept` at all. Which dialect a response is actually in is always read back off its own
/// `Content-Type`, never assumed from this header.
const ACCEPT_HEADER_VALUE: &str =
    "application/openmetrics-text;version=1.0.0,text/plain;version=0.0.4;q=0.5,*/*;q=0.1";

/// `logit/<CARGO_PKG_VERSION>` -- a compile-time constant, so sending it costs no per-request
/// allocation.
const USER_AGENT_VALUE: &str = concat!("logit/", env!("CARGO_PKG_VERSION"));

/// `crate::tls::TlsClientSettings`, re-exported at this path -- kept reachable as
/// `prometheus::TlsClientSettings` so `logit-cli::pipeline::build_spec` has one obvious path to
/// import, the same convention `otlp::TlsServerSettings` already follows.
pub use crate::tls::TlsClientSettings;

/// One configured scrape target: the URL to `GET`, its [`redact_url`]ed form (for anything that
/// isn't the request itself -- diagnostics text, `prometheus.target`), and the `Resource` every
/// batch built from it carries. Built once in [`PrometheusInput::new`] -- see this module's doc
/// comment.
#[derive(Clone)]
struct Target {
    url: String,
    redacted_url: String,
    resource: Arc<Resource>,
}

/// `host:port` of `url`'s authority, port defaulted to the scheme's well-known one (80/443) when
/// absent -- exactly what Prometheus's own `instance` label holds, and what
/// [`logit_proto::prometheus::LABEL_INSTANCE`] documents. Falls back to [`redact_url`]'s own
/// `index`-keyed placeholder if `url` doesn't parse -- graph validation's rule 40 only checks
/// scheme plus a non-empty authority (a cheap, `reqwest`-free approximation, since
/// `logit-pipeline` can't depend on it), not a full URL grammar, so a target like
/// `http://999.999.999.999/metrics` or `http://[::1/metrics` passes rule 40 but still fails
/// `reqwest::Url::parse` here -- a real path, not merely defensive. Never the raw `url` itself,
/// which may carry a `user:pass@` credential this fallback must not leak.
fn instance_of(index: usize, url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(parsed) => {
            let host = parsed.host_str().unwrap_or_default();
            match parsed.port_or_known_default() {
                Some(port) => format!("{host}:{port}"),
                None => host.to_string(),
            }
        }
        Err(_) => redact_url(index, url),
    }
}

/// `url` with its userinfo (`user:pass@`) and query string stripped -- what every place that
/// isn't the actual scrape request itself (the `prometheus.target` resource attribute, every
/// `scrape_failed` diagnostic) must use instead of the raw target. A scrape URL can legitimately
/// carry HTTP basic-auth credentials (`http://user:pass@host/metrics`) -- `reqwest` turns that
/// into an `Authorization` header and never puts it on the wire itself, but the raw `String` this
/// input was configured with still holds it in memory, and `prometheus.target` rides on every
/// event's resource, reaching whatever sink the pipeline is configured with (InfluxDB tags,
/// statsd tag sets, a forwarded OTLP resource, a stdout/file render) -- rendering the password in
/// cleartext into every one of them if not stripped first. The query string goes too, since a
/// bearer-token-in-query auth scheme (`?token=...`) is just as real a credential shape; the
/// fragment is *not* stripped -- `url` crate parsing means this also normalizes the result (the
/// host is lowercased, and a port matching the scheme's default is dropped), which is harmless
/// for an already-valid absolute URL.
///
/// Falls back to `<unparseable target #{index}>` -- never the raw `url`, and never a placeholder
/// shared across targets -- if `url` doesn't parse at all. Rule 40 (`logit-pipeline::graph`) only
/// approximates a real URL grammar (scheme plus non-empty authority), so a target like
/// `http://999.999.999.999/metrics`, `http://[::1/metrics`, `http://host:99999/metrics`, or a
/// host containing a space or an invalid percent-escape reaches this function despite passing
/// that check; `index` (this target's position in the configured `targets` list) is what keeps
/// two such targets from colliding onto the same `instance`/`prometheus.target` and silently
/// merging their series.
fn redact_url(index: usize, url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut parsed) => {
            // `Url::set_username`/`set_password` only fail for a URL kind that can't have
            // userinfo at all (`cannot-be-a-base`, e.g. `data:`) -- never true for an absolute
            // `http`/`https` URL, which is all rule 40 ever lets through; the `Result` is
            // discarded rather than propagated for exactly that reason.
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.to_string()
        }
        Err(_) => format!("<unparseable target #{index}>"),
    }
}

fn build_resource(index: usize, url: &str) -> Resource {
    let mut attributes = AttrMap::new();
    attributes.insert(LABEL_INSTANCE, Value::str(instance_of(index, url)));
    attributes.insert(ATTR_TARGET, Value::str(redact_url(index, url)));
    Resource { attributes, ..Default::default() }
}

fn synthetic_event(timestamp: i64, name: &'static str, value: f64) -> Event {
    Event::metric(
        timestamp,
        AttrMap::new(),
        MetricRecord::new(intern(name), MetricKind::Gauge(value)),
    )
}

fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// A coarse HTTP response-status bucket -- see `logit_outputs::otlp::status_class`'s identical
/// reasoning; duplicated rather than shared, same as that module's own note on why (independently
/// evolving crates). `1xx`/`3xx` fold into `other` -- a scrape response is never legitimately
/// either, so there's no value in a finer bucket for them.
fn status_class(status: reqwest::StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        2 => "2xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

/// The outcome of one target's scrape attempt -- what [`scrape_target`] returns, and what
/// [`PrometheusInput::tick`] classifies into a `logit.input.scrapes{class}` count and either a
/// decoded batch or a failed one.
enum ScrapeStatus {
    Ok { dialect: Dialect, body: Vec<u8> },
    Http(reqwest::StatusCode),
    NetworkError,
    Timeout,
    Oversize,
}

/// Scrapes one target: builds the request (extra `headers` first, then the fixed `Accept`/
/// `User-Agent` pair inserted after -- so they always win even if a misconfigured `headers` entry
/// named one of them, the same defense-in-depth `logit_outputs::otlp::OtlpOutput::send_http` uses
/// for `Content-Type`), reads the response body incrementally via [`reqwest::Response::chunk`]
/// capped at [`MAX_SCRAPE_BYTES`].
async fn scrape_target(
    client: reqwest::Client,
    url: String,
    headers: HeaderMap,
    timeout: Duration,
) -> ScrapeStatus {
    let mut request_headers = headers;
    request_headers.insert(ACCEPT, HeaderValue::from_static(ACCEPT_HEADER_VALUE));
    request_headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));

    let response = match client.get(&url).timeout(timeout).headers(request_headers).send().await {
        Ok(response) => response,
        Err(err) if err.is_timeout() => return ScrapeStatus::Timeout,
        Err(_) => return ScrapeStatus::NetworkError,
    };

    let status = response.status();
    if !status.is_success() {
        return ScrapeStatus::Http(status);
    }
    let dialect = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(Dialect::from_content_type)
        .unwrap_or(Dialect::Text0_0_4);

    let mut response = response;
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if body.len() + chunk.len() > MAX_SCRAPE_BYTES {
                    return ScrapeStatus::Oversize;
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(err) if err.is_timeout() => return ScrapeStatus::Timeout,
            Err(_) => return ScrapeStatus::NetworkError,
        }
    }
    ScrapeStatus::Ok { dialect, body }
}

pub struct PrometheusInput {
    targets: Vec<Target>,
    interval: Duration,
    timeout: Duration,
    headers: HeaderMap,
    client: reqwest::Client,
    /// Kept across ticks (not rebuilt per scrape) so the decoder's own throttled diagnostics
    /// (a malformed line, non-monotonic histogram buckets) accumulate their occurrence counts
    /// across the whole component's lifetime, the same as every other decoder-holding input.
    decoder: PrometheusDecoder,
    telemetry: Telemetry,
    diag: Diagnostics,
}

impl PrometheusInput {
    /// `targets` become this input's scrape list, each with a `Resource` built once here (see this
    /// module's doc comment's "Resource identity" section) -- never rebuilt per tick.
    pub fn new(targets: Vec<String>, interval: Duration) -> Self {
        let targets = targets
            .into_iter()
            .enumerate()
            .map(|(index, url)| {
                let resource = Arc::new(build_resource(index, &url));
                let redacted_url = redact_url(index, &url);
                Target { url, redacted_url, resource }
            })
            .collect();
        Self {
            targets,
            interval,
            timeout: Duration::from_secs(10),
            headers: HeaderMap::new(),
            client: reqwest::Client::new(),
            decoder: PrometheusDecoder::new(),
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
        }
    }

    /// Overrides the default 10s per-request timeout, applied per scrape via
    /// `RequestBuilder::timeout` -- unlike `otlp_out`'s client-wide timeout, this needs no client
    /// rebuild, so it composes with [`PrometheusInput::with_tls`] in either call order.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the extra headers sent on every scrape request (`headers:` in config). Fails on the
    /// same shape `otlp_out`'s `with_headers` does: an illegal header name/value, or two names
    /// colliding once HTTP's case-insensitivity is applied -- `logit-pipeline::graph::resolve`'s
    /// rule 40 rejects a header this input sets itself (`accept`, `user-agent`, and the other
    /// protocol-owned names) before construction ever sees it; this catches the lexical shape
    /// `graph` can't. [`scrape_target`] still inserts `Accept`/`User-Agent` *after* cloning these
    /// in, so they always win even if that rule were ever bypassed.
    pub fn with_headers(mut self, headers: &HashMap<String, String>) -> anyhow::Result<Self> {
        let mut map = HeaderMap::with_capacity(headers.len());
        for (name, value) in headers {
            let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|err| {
                anyhow::anyhow!("prometheus_in: {name:?} is not a legal header name: {err}")
            })?;
            let header_value = HeaderValue::from_str(value).map_err(|err| {
                anyhow::anyhow!("prometheus_in: header {name:?} has an invalid value: {err}")
            })?;
            if map.insert(header_name, header_value).is_some() {
                anyhow::bail!(
                    "prometheus_in: header {name:?} collides with another entry in 'headers' \
                     once case is ignored -- HTTP header names are case-insensitive, so which \
                     value would actually be sent is undefined"
                );
            }
        }
        self.headers = map;
        Ok(self)
    }

    /// Sets client-side TLS tuning (`tls:` in config) for any `https://` target -- a no-op if
    /// `settings` is empty. Built entirely on `reqwest`'s own PEM loaders
    /// ([`crate::tls::apply_client_tls`]); no `rustls` type appears in this crate's HTTP-client
    /// path.
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
                "tls.insecure_skip_verify is set -- the connection is encrypted, but this \
                 input will accept any certificate a scraped target presents, self-signed or \
                 otherwise",
            );
        }
        let builder = apply_client_tls(reqwest::Client::builder(), settings, base_dir)?;
        self.client = builder.build().map_err(|err| {
            anyhow::anyhow!("prometheus_in: building a TLS-configured client: {err}")
        })?;
        Ok(self)
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag.clone();
        self.decoder = self.decoder.with_diagnostics(diag);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry.clone();
        self.decoder = self.decoder.with_telemetry(telemetry);
        self
    }

    /// One scrape cycle: every target concurrently (a [`JoinSet`], since the number of targets is
    /// only known at runtime), then -- sequentially, back on this method's own task, so nothing
    /// here needs to synchronize concurrent access to `self.decoder`/`self.diag`/`self.telemetry`
    /// -- one `EventBatch` sent per target, decoded series plus the three synthetic metrics
    /// together. A non-trait method, directly callable from a test with a canned server and a
    /// bare `Fanout`, the same shape as `internal.rs`'s own `tick`.
    async fn tick(&mut self, sink: &Fanout) {
        let received_at = now_nanos();
        // A local clone of the target list, not `&self.targets` -- lets the loop below hold
        // `&mut self.decoder`/`&mut self.diag` at the same time as reading each target's own
        // fields, with no borrow-checker conflict over disjoint parts of `self`. Cheap: a handful
        // of `String` + `Arc<Resource>` clones per tick, not a hot path.
        let targets = self.targets.clone();

        let mut set = JoinSet::new();
        for (idx, target) in targets.iter().enumerate() {
            let client = self.client.clone();
            let url = target.url.clone();
            let headers = self.headers.clone();
            let timeout = self.timeout;
            let _handle = set.spawn(async move {
                let started = Instant::now();
                let status = scrape_target(client, url, headers, timeout).await;
                (idx, status, started.elapsed())
            });
        }

        let mut outcomes: Vec<Option<(ScrapeStatus, Duration)>> =
            (0..targets.len()).map(|_| None).collect();
        while let Some(joined) = set.join_next().await {
            // A `JoinError` only ever means the spawned task panicked -- nothing in
            // `scrape_target` does, so this is unreachable in practice; treated as a network
            // error (no batch, `up: 0`) rather than propagating the panic, matching this
            // component's "one target's failure can't take down the others" contract.
            if let Ok((idx, status, elapsed)) = joined {
                outcomes[idx] = Some((status, elapsed));
            }
        }

        for (target, outcome) in targets.iter().zip(outcomes) {
            let (status, elapsed) = outcome.unwrap_or((ScrapeStatus::NetworkError, Duration::ZERO));
            let (class, up, samples, mut events) = match status {
                ScrapeStatus::Ok { dialect, body } => {
                    match text::parse_with(&body, dialect, &mut self.decoder) {
                        Ok(families) => {
                            let events =
                                families_to_events(&families, received_at, &mut self.decoder);
                            let samples = events.len();
                            ("2xx", 1.0, samples, events)
                        }
                        Err(err) => {
                            self.diag.warn_throttled(
                                "scrape_failed",
                                format_args!(
                                    "prometheus_in: scraping {} succeeded but its response body \
                                     failed to parse: {err}",
                                    target.redacted_url
                                ),
                            );
                            ("parse_error", 0.0, 0, Vec::new())
                        }
                    }
                }
                ScrapeStatus::Http(status_code) => {
                    self.diag.warn_throttled(
                        "scrape_failed",
                        format_args!(
                            "prometheus_in: scraping {} returned HTTP {status_code}",
                            target.redacted_url
                        ),
                    );
                    (status_class(status_code), 0.0, 0, Vec::new())
                }
                ScrapeStatus::NetworkError => {
                    self.diag.warn_throttled(
                        "scrape_failed",
                        format_args!(
                            "prometheus_in: scraping {} failed: connection error",
                            target.redacted_url
                        ),
                    );
                    ("network_error", 0.0, 0, Vec::new())
                }
                ScrapeStatus::Timeout => {
                    self.diag.warn_throttled(
                        "scrape_failed",
                        format_args!("prometheus_in: scraping {} timed out", target.redacted_url),
                    );
                    ("timeout", 0.0, 0, Vec::new())
                }
                ScrapeStatus::Oversize => {
                    self.diag.warn_throttled(
                        "scrape_failed",
                        format_args!(
                            "prometheus_in: scraping {} exceeded the {MAX_SCRAPE_BYTES}-byte \
                             scrape limit",
                            target.redacted_url
                        ),
                    );
                    ("oversize", 0.0, 0, Vec::new())
                }
            };

            self.telemetry.count("logit.input.scrapes", 1.0, &[("class", class)]);
            self.telemetry.timing("logit.input.scrape.duration", elapsed, &[]);
            self.telemetry.count("logit.input.samples", samples as f64, &[]);

            events.push(synthetic_event(received_at, "up", up));
            events.push(synthetic_event(
                received_at,
                "scrape_duration_seconds",
                elapsed.as_secs_f64(),
            ));
            events.push(synthetic_event(received_at, "scrape_samples_scraped", samples as f64));

            sink.send(EventBatch { resource: target.resource.clone(), scope: None, events }).await;
        }
    }
}

#[async_trait::async_trait]
impl Input for PrometheusInput {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(self.interval);
        // `Delay`, not the default `Burst`: unlike `internal.rs`'s own tick (which never does
        // network I/O), a tick here awaits `sink.send` (bounded-channel backpressure) plus a
        // per-target request timeout, so a downstream stall lasting several intervals is
        // ordinary, not exceptional. `Burst` would fire every missed tick back-to-back the moment
        // the stall clears -- N full scrape rounds in a row, each target hit N times with
        // near-identical `received_at`, inflating `logit.input.scrapes`/`samples` and the
        // synthetic series. `Delay` instead resumes on a fixed cadence from whenever the last
        // tick actually completed, matching Prometheus's own scrape scheduler, which skips rather
        // than bursts.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Swallow the immediate first tick, `internal.rs`'s own pattern -- the first real scrape
        // happens after one full `interval` has elapsed, not at t=0.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            self.tick(&sink).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use hyper::{Request, Response};
    use hyper_util::rt::TokioIo;
    use logit_core::{MetricKind, Registry};
    use logit_pipeline::Delivered;
    use rustls_pki_types::pem::PemObject;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    /// What a canned connection does with each request it accepts -- one variant per scenario
    /// this module's tests need a server for. `Clone` so one value can back every connection a
    /// test's (usually single-request) server accepts.
    #[derive(Clone)]
    enum CannedResponse {
        Body {
            status: u16,
            content_type: Option<&'static str>,
            body: Bytes,
        },
        /// Accepts the connection and reads the request, but never writes a response -- what
        /// drives this input's own per-request timeout rather than a connection-level one.
        Hang,
    }

    /// A real `hyper` HTTP/1.1 server (`otlp_in`'s own server-side stack, `hyper::server::conn::
    /// http1` + `hyper_util::rt::TokioIo`) bound to an ephemeral port, answering every request
    /// with `response` and recording each request's headers. Returns the bound address and the
    /// captured-headers list.
    async fn canned_server(response: CannedResponse) -> (SocketAddr, Arc<Mutex<Vec<HeaderMap>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_task = captured.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let io = TokioIo::new(stream);
                let response = response.clone();
                let captured = captured_task.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: Request<Incoming>| {
                        let response = response.clone();
                        let captured = captured.clone();
                        async move {
                            captured.lock().unwrap().push(req.headers().clone());
                            match response {
                                CannedResponse::Hang => std::future::pending::<
                                    Result<Response<Full<Bytes>>, Infallible>,
                                >()
                                .await,
                                CannedResponse::Body { status, content_type, body } => {
                                    let mut builder = Response::builder().status(status);
                                    if let Some(ct) = content_type {
                                        builder = builder.header("content-type", ct);
                                    }
                                    Ok(builder.body(Full::new(body)).unwrap())
                                }
                            }
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(io, service)
                        .await;
                });
            }
        });
        (addr, captured)
    }

    fn text_0_0_4_body() -> Bytes {
        Bytes::from_static(
            b"# HELP scrape_target_up A test metric.\n\
              # TYPE scrape_target_up gauge\n\
              scrape_target_up 1\n",
        )
    }

    fn openmetrics_body() -> Bytes {
        Bytes::from_static(
            b"# TYPE scrape_target_requests counter\n\
              scrape_target_requests_total 7\n\
              # EOF\n",
        )
    }

    fn input_for(url: &str) -> PrometheusInput {
        PrometheusInput::new(vec![url.to_string()], Duration::from_secs(3600))
    }

    async fn recv_batch(rx: &mut mpsc::Receiver<Delivered>) -> EventBatch {
        match rx.try_recv().expect("expected a batch to have been sent") {
            Delivered::Owned(batch, _ctx) => batch,
            Delivered::Shared(shared, _ctx) => (*shared).clone(),
        }
    }

    fn synthetic_value(batch: &EventBatch, name: &str) -> f64 {
        batch
            .events
            .iter()
            .find_map(|e| {
                e.metrics.iter().find_map(|m| {
                    if logit_core::interner::resolve(m.name) != name {
                        return None;
                    }
                    match m.kind {
                        MetricKind::Gauge(v) => Some(v),
                        _ => None,
                    }
                })
            })
            .unwrap_or_else(|| panic!("expected a '{name}' synthetic metric in the batch"))
    }

    /// Reads one counter's value out of an already-drained event list -- `Registry::drain` empties
    /// its buffers on every call, so a test asserting on more than one counter must drain exactly
    /// once and look up every value from that same snapshot, not call this once per counter.
    fn counter_in(events: &[Event], metric: &str, tag: (&str, &str)) -> Option<f64> {
        events.iter().find_map(|e| {
            if e.attributes.get(tag.0).and_then(|v| v.as_str()) != Some(tag.1) {
                return None;
            }
            e.metrics.iter().find_map(|m| {
                if logit_core::interner::resolve(m.name) != metric {
                    return None;
                }
                match m.kind {
                    MetricKind::Sum(sum) => Some(sum.value),
                    _ => None,
                }
            })
        })
    }

    #[tokio::test]
    async fn a_successful_text_scrape_decodes_series_and_reports_up_one() {
        let (addr, _captured) = canned_server(CannedResponse::Body {
            status: 200,
            content_type: Some("text/plain; version=0.0.4"),
            body: text_0_0_4_body(),
        })
        .await;
        let mut input = input_for(&format!("http://{addr}/metrics"));
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        assert!(batch.events.iter().any(|e| e
            .metrics
            .iter()
            .any(|m| logit_core::interner::resolve(m.name) == "scrape_target_up")));
        assert_eq!(synthetic_value(&batch, "up"), 1.0);
        assert_eq!(synthetic_value(&batch, "scrape_samples_scraped"), 1.0);
    }

    #[tokio::test]
    async fn a_successful_openmetrics_scrape_is_parsed_via_its_content_type() {
        let (addr, _captured) = canned_server(CannedResponse::Body {
            status: 200,
            content_type: Some("application/openmetrics-text; version=1.0.0"),
            body: openmetrics_body(),
        })
        .await;
        let mut input = input_for(&format!("http://{addr}/metrics"));
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        assert!(batch.events.iter().any(|e| e
            .metrics
            .iter()
            .any(|m| logit_core::interner::resolve(m.name) == "scrape_target_requests_total")));
        assert_eq!(synthetic_value(&batch, "up"), 1.0);
    }

    #[tokio::test]
    async fn an_http_error_status_reports_up_zero() {
        let (addr, _captured) = canned_server(CannedResponse::Body {
            status: 500,
            content_type: None,
            body: Bytes::new(),
        })
        .await;
        let mut input = input_for(&format!("http://{addr}/metrics"));
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        assert_eq!(synthetic_value(&batch, "up"), 0.0);
        assert_eq!(synthetic_value(&batch, "scrape_samples_scraped"), 0.0);
    }

    #[tokio::test]
    async fn a_timeout_reports_up_zero() {
        let (addr, _captured) = canned_server(CannedResponse::Hang).await;
        let mut input =
            input_for(&format!("http://{addr}/metrics")).with_timeout(Duration::from_millis(100));
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        assert_eq!(synthetic_value(&batch, "up"), 0.0);
    }

    #[tokio::test]
    async fn an_oversize_body_is_aborted_and_reports_up_zero() {
        let oversize = Bytes::from(vec![b'a'; MAX_SCRAPE_BYTES + 1]);
        let (addr, _captured) =
            canned_server(CannedResponse::Body { status: 200, content_type: None, body: oversize })
                .await;
        let mut input = input_for(&format!("http://{addr}/metrics"));
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        assert_eq!(synthetic_value(&batch, "up"), 0.0);
    }

    #[tokio::test]
    async fn a_refused_connection_reports_up_zero() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let mut input = input_for(&format!("http://{addr}/metrics"));
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        assert_eq!(synthetic_value(&batch, "up"), 0.0);
    }

    // ---- TLS: a canned `tokio-rustls`-wrapped scrape target -- mirrors
    // `logit_outputs::otlp`'s own `canned_tls_http_server`/`test_server_tls_config` test pattern,
    // since `PrometheusInput::with_tls` is a client the same shape `OtlpOutput::with_tls` is. ----

    fn testdata_dir() -> std::path::PathBuf {
        // `logit-inputs` lives at `crates/logit-inputs`; the fixtures live at the repo root's
        // `testdata/tls` (`testdata/tls/README.md`) -- two levels up from `CARGO_MANIFEST_DIR`.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// Builds a `rustls::ServerConfig` presenting `testdata/tls/server.{pem,key}` -- no client
    /// certificate required, since `PrometheusInput` doesn't (yet) support mutual TLS.
    fn test_server_tls_config() -> Arc<rustls::ServerConfig> {
        let dir = testdata_dir();
        let chain: Vec<rustls_pki_types::CertificateDer<'static>> =
            rustls_pki_types::CertificateDer::pem_file_iter(dir.join("server.pem"))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        let key = rustls_pki_types::PrivateKeyDer::from_pem_file(dir.join("server.key")).unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let cfg = rustls::ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
        Arc::new(cfg)
    }

    /// A TLS-wrapped `canned_server`: replies with a fixed text-0.0.4 body over a real TLS
    /// handshake against `testdata/tls/server.pem`.
    async fn canned_tls_server() -> SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let acceptor = tokio_rustls::TlsAcceptor::from(test_server_tls_config());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let Ok(mut tls_stream) = acceptor.accept(stream).await else { continue };
                let mut buf = [0u8; 4096];
                let _ = tls_stream.read(&mut buf).await;
                let body = text_0_0_4_body();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\n\
                     Content-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = tls_stream.write_all(response.as_bytes()).await;
                let _ = tls_stream.write_all(&body).await;
                let _ = tls_stream.shutdown().await;
            }
        });
        addr
    }

    #[tokio::test]
    async fn a_trusted_ca_file_lets_an_https_scrape_succeed() {
        let addr = canned_tls_server().await;
        let mut input = input_for(&format!("https://{addr}/metrics"))
            .with_tls(
                &TlsClientSettings { ca_file: Some("ca.pem".to_string()), ..Default::default() },
                &testdata_dir(),
            )
            .expect("a well-formed tls: block should build fine");
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        assert_eq!(
            synthetic_value(&batch, "up"),
            1.0,
            "a trusted CA should let the scrape succeed"
        );
    }

    /// Alongside the success case above, proves `ca_file` is actually *honored* -- a CA that
    /// doesn't sign the server's leaf (`other-ca.pem` signs nothing here, `testdata/tls/
    /// README.md`) is rejected, not silently ignored. This does **not** exercise
    /// `apply_client_tls`'s `tls_built_in_root_certs(false)` call: `testdata/tls/server.pem` is
    /// signed by a private test CA that no bundled public root chains to either, so the handshake
    /// fails here whether or not the built-in roots are disabled -- a discriminating test would
    /// need a leaf the *bundled* roots would otherwise accept, which isn't reproducible offline.
    /// `tls_built_in_root_certs(false)`'s replacement guarantee (a configured `ca_file` trusted
    /// *instead of*, not *alongside*, the bundled Mozilla set) is documented behavior of the
    /// underlying `reqwest::ClientBuilder::tls_built_in_root_certs` call itself, taken on trust
    /// from its own doc comment rather than re-verified by a test here.
    #[tokio::test]
    async fn an_untrusted_ca_file_rejects_an_https_scrape() {
        let addr = canned_tls_server().await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("scrape", "prometheus_in", "listener");
        let mut input = input_for(&format!("https://{addr}/metrics"))
            .with_tls(
                &TlsClientSettings {
                    ca_file: Some("other-ca.pem".to_string()),
                    ..Default::default()
                },
                &testdata_dir(),
            )
            .expect("a well-formed tls: block should build fine")
            .with_telemetry(telemetry);
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        assert_eq!(synthetic_value(&batch, "up"), 0.0, "an untrusted CA should reject the scrape");
        // `up == 0` alone can't distinguish a TLS handshake failure from a timeout or a 5xx --
        // assert the actual class too, so this test only passes for the failure mode it names.
        let events = registry.drain(0);
        assert_eq!(
            counter_in(&events, "logit.input.scrapes", ("class", "network_error")),
            Some(1.0)
        );
    }

    #[tokio::test]
    async fn the_resource_carries_instance_and_prometheus_target() {
        let (addr, _captured) = canned_server(CannedResponse::Body {
            status: 200,
            content_type: Some("text/plain; version=0.0.4"),
            body: text_0_0_4_body(),
        })
        .await;
        let url = format!("http://{addr}/metrics");
        let mut input = input_for(&url);
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        assert_eq!(
            batch.resource.attributes.get("instance").and_then(|v| v.as_str()),
            Some(addr.to_string().as_str())
        );
        assert_eq!(
            batch.resource.attributes.get("prometheus.target").and_then(|v| v.as_str()),
            Some(url.as_str())
        );
    }

    /// The regression test for the credential-leak fix: `prometheus.target` rides on every event
    /// this target produces, reaching whatever sink the pipeline routes to (InfluxDB tags, statsd
    /// tag sets, a forwarded OTLP resource, a stdout render) -- it must never carry a scrape URL's
    /// userinfo or query string in cleartext, even though the *request itself* still needs and
    /// uses them (a scrape URL's `user:pass@` is a legitimate way to configure HTTP basic auth).
    #[tokio::test]
    async fn the_prometheus_target_attribute_strips_userinfo_and_query() {
        let (addr, captured) = canned_server(CannedResponse::Body {
            status: 200,
            content_type: Some("text/plain; version=0.0.4"),
            body: text_0_0_4_body(),
        })
        .await;
        let url = format!("http://user:pass@{addr}/metrics?token=x");
        let mut input = input_for(&url);
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let batch = recv_batch(&mut rx).await;
        let target =
            batch.resource.attributes.get("prometheus.target").and_then(|v| v.as_str()).unwrap();
        assert_eq!(target, format!("http://{addr}/metrics"), "got: {target}");
        assert!(!target.contains("pass"), "got: {target}");
        assert!(!target.contains("token"), "got: {target}");

        // The request itself still authenticates: `reqwest` turns the URL's userinfo into a real
        // `Authorization` header, which this redaction must not have broken.
        let headers = captured.lock().unwrap();
        let auth = headers[0].get(http::header::AUTHORIZATION).and_then(|v| v.to_str().ok());
        assert!(auth.is_some(), "expected an Authorization header from the URL's userinfo");
    }

    #[test]
    fn redact_url_strips_userinfo_and_query_but_keeps_the_path() {
        assert_eq!(
            redact_url(0, "http://user:pass@example.com:9100/metrics?token=x"),
            "http://example.com:9100/metrics"
        );
        assert_eq!(redact_url(0, "https://example.com/metrics"), "https://example.com/metrics");
    }

    #[test]
    fn redact_url_falls_back_to_an_index_keyed_placeholder_on_an_unparseable_url() {
        assert_eq!(redact_url(3, "not a url"), "<unparseable target #3>");
        assert_eq!(instance_of(3, "not a url"), "<unparseable target #3>");
        // Two malformed targets at different configured positions must not collapse onto the
        // same placeholder -- see `an_unparseable_targets_placeholder_is_keyed_by_index` for the
        // end-to-end version of this property (distinct `Resource`s, not just distinct strings).
        assert_ne!(redact_url(0, "not a url"), redact_url(1, "not a url"));
    }

    #[tokio::test]
    async fn the_accept_header_is_sent_on_every_scrape() {
        let (addr, captured) = canned_server(CannedResponse::Body {
            status: 200,
            content_type: Some("text/plain; version=0.0.4"),
            body: text_0_0_4_body(),
        })
        .await;
        let mut input = input_for(&format!("http://{addr}/metrics"));
        let (tx, mut _rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let headers = captured.lock().unwrap();
        let accept = headers[0].get(ACCEPT).and_then(|v| v.to_str().ok());
        assert_eq!(accept, Some(ACCEPT_HEADER_VALUE));
        let user_agent = headers[0].get(USER_AGENT).and_then(|v| v.to_str().ok());
        assert_eq!(user_agent, Some(USER_AGENT_VALUE));
    }

    #[tokio::test]
    async fn two_targets_each_get_their_own_batch_in_one_tick() {
        let (addr1, _c1) = canned_server(CannedResponse::Body {
            status: 200,
            content_type: Some("text/plain; version=0.0.4"),
            body: text_0_0_4_body(),
        })
        .await;
        let (addr2, _c2) = canned_server(CannedResponse::Body {
            status: 200,
            content_type: Some("text/plain; version=0.0.4"),
            body: text_0_0_4_body(),
        })
        .await;
        let mut input = PrometheusInput::new(
            vec![format!("http://{addr1}/metrics"), format!("http://{addr2}/metrics")],
            Duration::from_secs(3600),
        );
        let (tx, mut rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let first = recv_batch(&mut rx).await;
        let second = recv_batch(&mut rx).await;
        let mut instances: Vec<_> = [&first, &second]
            .iter()
            .map(|b| {
                b.resource.attributes.get("instance").and_then(|v| v.as_str()).unwrap().to_string()
            })
            .collect();
        instances.sort();
        let mut expected = vec![addr1.to_string(), addr2.to_string()];
        expected.sort();
        assert_eq!(instances, expected);
    }

    #[tokio::test]
    async fn telemetry_counts_scrapes_by_class_and_samples() {
        let (addr, _captured) = canned_server(CannedResponse::Body {
            status: 200,
            content_type: Some("text/plain; version=0.0.4"),
            body: text_0_0_4_body(),
        })
        .await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("scrape", "prometheus_in", "listener");
        let mut input = input_for(&format!("http://{addr}/metrics")).with_telemetry(telemetry);
        let (tx, mut _rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let events = registry.drain(0);
        assert_eq!(counter_in(&events, "logit.input.scrapes", ("class", "2xx")), Some(1.0));
        assert_eq!(counter_in(&events, "logit.input.samples", ("component", "scrape")), Some(1.0));
    }

    #[tokio::test]
    async fn telemetry_counts_a_5xx_response_under_the_5xx_class() {
        let (addr, _captured) = canned_server(CannedResponse::Body {
            status: 503,
            content_type: None,
            body: Bytes::new(),
        })
        .await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("scrape", "prometheus_in", "listener");
        let mut input = input_for(&format!("http://{addr}/metrics")).with_telemetry(telemetry);
        let (tx, mut _rx) = mpsc::channel(4);
        let fanout = Fanout::new(vec![tx]);

        input.tick(&fanout).await;

        let events = registry.drain(0);
        assert_eq!(counter_in(&events, "logit.input.scrapes", ("class", "5xx")), Some(1.0));
    }

    #[test]
    fn with_headers_rejects_a_case_insensitive_collision() {
        let headers = HashMap::from([
            ("X-Scope-OrgID".to_string(), "a".to_string()),
            ("x-scope-orgid".to_string(), "b".to_string()),
        ]);
        match input_for("http://example.com/metrics").with_headers(&headers) {
            Ok(_) => panic!("expected colliding headers to fail construction"),
            Err(err) => assert!(err.to_string().contains("collides"), "got: {err}"),
        }
    }

    #[test]
    fn with_headers_accepts_a_well_formed_custom_header() {
        let headers = HashMap::from([("X-Scope-OrgID".to_string(), "tenant-a".to_string())]);
        input_for("http://example.com/metrics")
            .with_headers(&headers)
            .expect("a well-formed header should be accepted");
    }

    #[test]
    fn instance_of_defaults_the_port_from_the_scheme() {
        assert_eq!(instance_of(0, "http://example.com/metrics"), "example.com:80");
        assert_eq!(instance_of(0, "https://example.com/metrics"), "example.com:443");
        assert_eq!(instance_of(0, "http://example.com:9100/metrics"), "example.com:9100");
    }

    /// The regression test for the placeholder-collision fix: rule 40's `is_absolute_http_url`
    /// only approximates a real URL grammar, so a config can carry more than one target that
    /// passes it but still fails `reqwest::Url::parse` (`redact_url`'s own doc comment lists the
    /// shapes) -- without the configured index folded into the placeholder, two such targets
    /// would build byte-identical `instance`/`prometheus.target` attributes and their `up=0`
    /// series would collapse onto one another downstream.
    #[test]
    fn two_unparseable_targets_get_distinct_resources() {
        let input = PrometheusInput::new(
            vec!["http://999.999.999.999/metrics".to_string(), "http://[::1/metrics".to_string()],
            Duration::from_secs(3600),
        );
        let instances: Vec<&str> = input
            .targets
            .iter()
            .map(|t| t.resource.attributes.get("instance").and_then(|v| v.as_str()).unwrap())
            .collect();
        assert_ne!(instances[0], instances[1], "got: {instances:?}");
        let target_attrs: Vec<&str> = input
            .targets
            .iter()
            .map(|t| {
                t.resource.attributes.get("prometheus.target").and_then(|v| v.as_str()).unwrap()
            })
            .collect();
        assert_ne!(target_attrs[0], target_attrs[1], "got: {target_attrs:?}");
    }
}
