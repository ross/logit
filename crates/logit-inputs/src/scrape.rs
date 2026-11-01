//! The scrape driver: an interval-driven HTTP client that fetches each configured target, hands
//! the body to a [`ScrapeDecoder`], and sends one batch per target per tick. `prometheus_in`'s
//! scrape mode runs on it; the decoder owns the page's format, and this module owns everything
//! else ([ADR `server-status-scrape-inputs`](../../../docs/adr/server-status-scrape-inputs.md)
//! section 2).
//!
//! ## The tick
//!
//! Interval-driven, like [`crate::internal::InternalInput`]: [`Input::run`] owns a
//! `tokio::time::interval` ticker, swallows its immediate first tick (so the first scrape happens
//! one full `interval` after start, not at t=0), and calls [`ScrapeInput::tick`], a plain method
//! testable without a runtime harness. There is no `bind` override: a scrape client has no socket
//! to open ahead of time.
//!
//! The ticker's missed-tick behavior is `Delay`, not the default `Burst`. `internal`'s tick does
//! no network I/O, so `Burst` (fire every missed tick back-to-back once a stall clears) is
//! harmless there. This tick awaits `sink.send` (bounded-channel backpressure) plus a per-target
//! request timeout, so a downstream stall lasting several intervals is ordinary, and `Burst` would
//! turn it into N full scrape rounds in a row. `Delay` resumes on a fixed cadence, matching
//! Prometheus's own scrape scheduler, which skips a missed scrape rather than bursting to catch up.
//!
//! Every target is fetched concurrently on a [`JoinSet`], each under the per-request timeout, and
//! every point carries the tick's timestamp, taken once before the fetches start. The batches are
//! then built and sent in target order on the ticking task, so the decoder needs no
//! synchronization and its throttled diagnostics accumulate across ticks.
//!
//! ## The request
//!
//! The operator's `headers:` go first, then the decoder's `Accept` and this driver's
//! `User-Agent: logit/<CARGO_PKG_VERSION>`, inserted last so they win over a `headers:` entry
//! naming either. Graph rule 40 rejects a `headers:` name the driver or the HTTP transport owns,
//! `accept-encoding` included, so this driver alone decides which content encodings a target may
//! answer in. The body is read incrementally through [`reqwest::Response::chunk`] under the
//! decoder's [`ScrapeDecoder::max_body_bytes`], so a misconfigured target can't grow this input's
//! memory unboundedly. Client TLS comes from `logit_pipeline::tls`, reloaded without a rebuilt
//! client, and offers h2 through ALPN on an `https://` target, with or without `scrape_tls:`.
//!
//! ## Synthetic scrape metrics
//!
//! Every tick, every target, scrape failures included, gets three synthetic series appended to
//! its batch, on that target's resource, with no `timestamped` flag: `up` (`Gauge(0|1)`),
//! `scrape_duration_seconds` (`Gauge`, wall-clock seconds for that target's request), and
//! `scrape_samples_scraped` (`Gauge`, the number of events the decoder produced, `0` on any
//! failure). `up` is `1` only for a `2xx` response the decoder accepted. A decode error sends no
//! decoded events.
//!
//! ## Counters
//!
//! `logit.input.scrapes{class}` -- one count per target per tick, `class` one of `2xx`/`4xx`/`5xx`/
//! `other` (HTTP response classes), `network_error`, `timeout`, `parse_error` (a 2xx response body
//! the decoder rejected), or `oversize` (a response body past the decoder's cap).
//! `logit.input.scrape.duration` -- a timing sample per target per tick, recorded regardless of
//! outcome. `logit.input.samples` -- the number of events decoded, summed across every target.
//! A scrape failure also reports `Diagnostics::warn_throttled("scrape_failed", ..)`, prefixed
//! with the decoder's kind name, with the target's [`redact_url`]ed form in the message text
//! only: never a tag (a target URL isn't `&'static` and isn't safe to intern per target,
//! `logit_core::telemetry::Tag`'s convention), and never the raw URL, which may carry a
//! credential.

use crate::Input;
use http::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE, USER_AGENT};
use logit_core::interner::intern;
use logit_core::Telemetry;
use logit_core::{
    AttrMap, Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Value,
};
use logit_pipeline::Fanout;
use std::collections::HashMap;
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

/// `logit/<CARGO_PKG_VERSION>`, a compile-time constant, so sending it allocates nothing.
pub(crate) const USER_AGENT_VALUE: &str = concat!("logit/", env!("CARGO_PKG_VERSION"));

/// `logit_pipeline::tls::TlsClientSettings`, re-exported beside this driver's builders.
pub use logit_pipeline::tls::TlsClientSettings;

/// What a scrape kind supplies to [`ScrapeInput`]: the page's format, and nothing about the
/// fetch. One value lives for the component's lifetime, so a decoder's throttled diagnostics and
/// counters accumulate across ticks.
pub trait ScrapeDecoder: Send {
    /// What [`ScrapeDecoder::decode`] rejects a body with. Its `Display` goes into the
    /// `scrape_failed` diagnostic after the redacted URL, so it must not carry the URL.
    type Error: Display;

    /// The component kind (`prometheus_in`), prefixed onto every message the driver writes.
    fn kind(&self) -> &'static str;

    /// The `Accept` header every request carries.
    fn accept(&self) -> &'static str;

    /// The largest body the driver reads; one byte more is the `oversize` class, with `up` at `0`.
    fn max_body_bytes(&self) -> usize;

    /// The `Resource` every batch from target `index` (its position in `scrape_targets`) carries,
    /// built once at construction. `url` is the configured URL, credential and all: a resource
    /// built from it goes through [`redact_url`] or reads only the authority and path.
    fn resource(&self, index: usize, url: &str) -> Resource;

    /// Attaches the component's diagnostics handle.
    fn set_diagnostics(&mut self, diag: Diagnostics);

    /// Attaches the component's telemetry handle.
    fn set_telemetry(&mut self, telemetry: Telemetry);

    /// Decodes one `2xx` body. `content_type` is the response's `Content-Type`, when it has one
    /// that is visible ASCII; `timestamp` is the tick's, in Unix nanoseconds, for every event.
    fn decode(
        &mut self,
        body: &[u8],
        content_type: Option<&str>,
        timestamp: i64,
    ) -> Result<Vec<Event>, Self::Error>;
}

/// One configured scrape target: the URL to `GET`, its [`redact_url`]ed form (for everything that
/// isn't the request itself), and the `Resource` every batch built from it carries. Built once in
/// [`ScrapeInput::with_decoder`].
#[derive(Clone)]
pub(crate) struct Target {
    url: String,
    redacted_url: String,
    pub(crate) resource: Arc<Resource>,
}

/// `url` with its userinfo (`user:pass@`) and query string stripped: what everything but the
/// scrape request itself (a decoder's resource, every `scrape_failed` diagnostic) uses. `reqwest`
/// turns `http://user:pass@host/metrics` into an `Authorization` header, but the configured
/// `String` still holds the password, and a resource attribute rides the batch to every sink the
/// pipeline routes to. A protocol sink leaves a private attribute off its tags, labels, and
/// fields, but a `stdio_out` or `file_out` render, a `kafka_out` record in its `json`, `msgpack`,
/// or `native` format, and the native hop carry it. The query string goes too, since
/// `?token=...` is as real a credential shape. The fragment is kept. Parsing also normalizes the
/// result (host lowercased, a default port dropped), which is harmless for a valid absolute URL.
///
/// Falls back to `<unparseable target #{index}>`, never the raw `url` and never a placeholder
/// shared across targets. Rule 40 (`logit-pipeline::graph`) only approximates a URL grammar
/// (scheme plus non-empty authority), so `http://999.999.999.999/metrics`,
/// `http://[::1/metrics`, `http://host:99999/metrics`, or a host with a space or an invalid
/// percent-escape reaches this function; `index` (the target's position in `scrape_targets`)
/// keeps two such targets from colliding onto one resource and merging their series.
pub(crate) fn redact_url(index: usize, url: &str) -> String {
    match reqwest::Url::parse(url) {
        Ok(mut parsed) => {
            // `Url::set_username`/`set_password` fail only for a URL that can't have userinfo
            // (`cannot-be-a-base`, e.g. `data:`), never an absolute `http`/`https` URL, which is
            // all rule 40 lets through.
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_query(None);
            parsed.to_string()
        }
        Err(_) => format!("<unparseable target #{index}>"),
    }
}

/// The resource a status-page kind gives target `index`: OTel semantic-convention `server.address`
/// (the URL's host, an IPv6 address without its brackets), `server.port` (an integer, defaulted
/// from the scheme), and `url.path` (the path without its query string, which may hold a
/// credential), so two targets behind one proxy at one host and port keep their series apart. A URL
/// `reqwest` can't parse gets [`redact_url`]'s placeholder as `server.address` and nothing else.
/// Neither `instance` nor `_logit.prometheus.target`: those are Prometheus's vocabulary
/// ([ADR `server-status-scrape-inputs`](../../../docs/adr/server-status-scrape-inputs.md)
/// section 4).
pub(crate) fn server_resource(index: usize, url: &str) -> Resource {
    let mut attributes = AttrMap::new();
    match reqwest::Url::parse(url) {
        Ok(parsed) => {
            let host = parsed.host_str().unwrap_or_default();
            let host = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
            attributes.insert(SERVER_ADDRESS, Value::str(host));
            if let Some(port) = parsed.port_or_known_default() {
                attributes.insert(SERVER_PORT, Value::I64(i64::from(port)));
            }
            attributes.insert(URL_PATH, Value::str(parsed.path()));
        }
        Err(_) => attributes.insert(SERVER_ADDRESS, Value::str(redact_url(index, url))),
    }
    Resource { attributes, ..Default::default() }
}

/// `server.address`, the scraped server's host.
pub const SERVER_ADDRESS: &str = "server.address";
/// `server.port`, the scraped server's port, an integer.
pub const SERVER_PORT: &str = "server.port";
/// `url.path`, the scraped page's path.
pub const URL_PATH: &str = "url.path";

fn synthetic_event(timestamp: i64, name: &'static str, value: f64) -> Event {
    Event::metric(
        timestamp,
        AttrMap::new(),
        MetricRecord::new(intern(name), MetricKind::gauge(value)),
    )
}

pub(crate) fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// A coarse HTTP response-status bucket, modelled on `logit_outputs::http::status_class` rather
/// than shared across crates, except that `1xx`/`3xx` fold into `other`: a scrape response is
/// never legitimately either.
fn status_class(status: reqwest::StatusCode) -> &'static str {
    match status.as_u16() / 100 {
        2 => "2xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

/// One target's fetch outcome, which [`ScrapeInput::tick`] classifies into a
/// `logit.input.scrapes{class}` count and a decoded or failed batch.
enum ScrapeStatus {
    Ok { content_type: Option<HeaderValue>, body: Vec<u8> },
    Http(reqwest::StatusCode),
    NetworkError,
    Timeout,
    Oversize,
}

/// What one fetch needs, cloned onto its task.
struct Request {
    client: reqwest::Client,
    url: String,
    headers: HeaderMap,
    accept: &'static str,
    timeout: Duration,
    max_body_bytes: usize,
}

/// Fetches one target. The fixed `Accept`/`User-Agent` pair is inserted after the extra
/// `headers`, so it wins even over a `headers` entry naming one of them, the same defense
/// `logit_outputs::otlp::OtlpOutput::send_http` uses for `Content-Type`.
async fn scrape_target(request: Request) -> ScrapeStatus {
    let Request { client, url, mut headers, accept, timeout, max_body_bytes } = request;
    headers.insert(ACCEPT, HeaderValue::from_static(accept));
    headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));

    let response = match client.get(&url).timeout(timeout).headers(headers).send().await {
        Ok(response) => response,
        Err(err) if err.is_timeout() => return ScrapeStatus::Timeout,
        Err(_) => return ScrapeStatus::NetworkError,
    };

    let status = response.status();
    if !status.is_success() {
        return ScrapeStatus::Http(status);
    }
    let content_type = response.headers().get(CONTENT_TYPE).cloned();

    let mut response = response;
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                if body.len() + chunk.len() > max_body_bytes {
                    return ScrapeStatus::Oversize;
                }
                body.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(err) if err.is_timeout() => return ScrapeStatus::Timeout,
            Err(_) => return ScrapeStatus::NetworkError,
        }
    }
    ScrapeStatus::Ok { content_type, body }
}

/// A scrape kind's [`Input`]: the target list, the ticker, the client, and the telemetry, over a
/// decoder `D` that reads the page.
pub struct ScrapeInput<D: ScrapeDecoder> {
    pub(crate) targets: Vec<Target>,
    interval: Duration,
    timeout: Duration,
    headers: HeaderMap,
    client: reqwest::Client,
    decoder: D,
    telemetry: Telemetry,
    diag: Diagnostics,
}

impl<D: ScrapeDecoder + Default> ScrapeInput<D> {
    /// [`ScrapeInput::with_decoder`] over `D::default()`.
    pub fn new(targets: Vec<String>, interval: Duration) -> Self {
        Self::with_decoder(D::default(), targets, interval)
    }
}

impl<D: ScrapeDecoder> ScrapeInput<D> {
    /// `targets` become the scrape list, each with the `Resource` `decoder` builds for it, once,
    /// here.
    pub fn with_decoder(decoder: D, targets: Vec<String>, interval: Duration) -> Self {
        let targets = targets
            .into_iter()
            .enumerate()
            .map(|(index, url)| {
                let resource = Arc::new(decoder.resource(index, &url));
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
            decoder,
            telemetry: Telemetry::default(),
            diag: Diagnostics::default(),
        }
    }

    /// Overrides the default 10s per-request timeout. Applied per scrape via
    /// `RequestBuilder::timeout`, not baked into the client, so it composes with
    /// [`ScrapeInput::with_tls`] in either call order.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the extra headers sent on every scrape request (`headers:` in config). Fails, as
    /// `otlp_out`'s `with_headers` does, on an illegal name or value, or two names that collide
    /// case-insensitively. Rule 40 already rejects a header this driver or the transport sets
    /// itself; this catches the lexical faults the graph can't, and [`scrape_target`] still
    /// inserts `Accept`/`User-Agent` last.
    pub fn with_headers(mut self, headers: &HashMap<String, String>) -> anyhow::Result<Self> {
        let kind = self.decoder.kind();
        let mut map = HeaderMap::with_capacity(headers.len());
        for (name, value) in headers {
            let header_name = HeaderName::from_bytes(name.as_bytes()).map_err(|err| {
                anyhow::anyhow!("{kind}: {name:?} is not a legal header name: {err}")
            })?;
            let header_value = HeaderValue::from_str(value).map_err(|err| {
                anyhow::anyhow!("{kind}: header {name:?} has an invalid value: {err}")
            })?;
            if map.insert(header_name, header_value).is_some() {
                anyhow::bail!(
                    "{kind}: header {name:?} collides with another entry in 'headers' \
                     once case is ignored -- HTTP header names are case-insensitive, so which \
                     value would actually be sent is undefined"
                );
            }
        }
        self.headers = map;
        Ok(self)
    }

    /// Sets client-side TLS (`scrape_tls:` in config) for any `https://` target; a no-op if
    /// `settings` is empty. The `rustls::ClientConfig` comes from
    /// `logit_pipeline::tls::build_client_config`, as every sink's does, so a rotated `ca_file` or
    /// client certificate reaches the next full handshake without a rebuilt client.
    ///
    /// Registers the files with `reloader` under this input's diagnostics and telemetry as they
    /// are when this runs, so call it after `with_diagnostics` and `with_telemetry`.
    pub fn with_tls(
        mut self,
        settings: &TlsClientSettings,
        base_dir: &Path,
        reloader: &logit_pipeline::tls::TlsReloader,
    ) -> anyhow::Result<Self> {
        if settings.is_empty() {
            return Ok(self);
        }
        let kind = self.decoder.kind();
        if settings.insecure_skip_verify {
            self.diag.warn(format_args!(
                "{kind}: scrape_tls.insecure_skip_verify is set -- the connection is encrypted, \
                 but this input will accept any certificate a scraped target presents, \
                 self-signed or otherwise"
            ));
        }
        let cfg = logit_pipeline::tls::build_client_config(
            settings,
            base_dir,
            reloader,
            &self.diag,
            &self.telemetry,
        )
        .map_err(|err| anyhow::anyhow!("{kind}: scrape_tls: {err:#}"))?;
        let cfg = logit_pipeline::tls::with_http_alpn(&cfg);
        self.client = reqwest::Client::builder()
            .use_preconfigured_tls(cfg)
            .build()
            .map_err(|err| anyhow::anyhow!("{kind}: building a TLS-configured client: {err}"))?;
        Ok(self)
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag.clone();
        self.decoder.set_diagnostics(diag);
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry.clone();
        self.decoder.set_telemetry(telemetry);
        self
    }

    /// One scrape cycle: every target fetched concurrently (a [`JoinSet`]), then one `EventBatch`
    /// per target, decoded events plus the three synthetic metrics, built and sent sequentially on
    /// this task so `self.decoder`/`self.diag`/`self.telemetry` need no synchronization. A plain
    /// method, callable from a test with a canned server and a bare `Fanout`, like `internal.rs`'s
    /// `tick`.
    pub(crate) async fn tick(&mut self, sink: &Fanout) {
        let received_at = now_nanos();
        let kind = self.decoder.kind();
        let accept = self.decoder.accept();
        let max_body_bytes = self.decoder.max_body_bytes();
        // Cloned, not borrowed, so the loop below can hold `&mut self.decoder`/`&mut self.diag`
        // while reading each target. A few `String` + `Arc<Resource>` clones per tick.
        let targets = self.targets.clone();

        let mut set = JoinSet::new();
        for (idx, target) in targets.iter().enumerate() {
            let request = Request {
                client: self.client.clone(),
                url: target.url.clone(),
                headers: self.headers.clone(),
                accept,
                timeout: self.timeout,
                max_body_bytes,
            };
            let _handle = set.spawn(async move {
                let started = Instant::now();
                let status = scrape_target(request).await;
                (idx, status, started.elapsed())
            });
        }

        let mut outcomes: Vec<Option<(ScrapeStatus, Duration)>> =
            (0..targets.len()).map(|_| None).collect();
        while let Some(joined) = set.join_next().await {
            // A `JoinError` means the task panicked, which `scrape_target` doesn't. It falls
            // through as a network error (`up: 0`) below rather than propagating: one target's
            // failure can't take down the others.
            if let Ok((idx, status, elapsed)) = joined {
                outcomes[idx] = Some((status, elapsed));
            }
        }

        for (target, outcome) in targets.iter().zip(outcomes) {
            let (status, elapsed) = outcome.unwrap_or((ScrapeStatus::NetworkError, Duration::ZERO));
            let (class, up, samples, mut events) = match status {
                ScrapeStatus::Ok { content_type, body } => {
                    let content_type = content_type.as_ref().and_then(|v| v.to_str().ok());
                    match self.decoder.decode(&body, content_type, received_at) {
                        Ok(events) => {
                            let samples = events.len();
                            ("2xx", 1.0, samples, events)
                        }
                        Err(err) => {
                            self.diag.warn_throttled(
                                "scrape_failed",
                                format_args!(
                                    "{kind}: scraping {} succeeded but its response body failed \
                                     to parse: {err}",
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
                            "{kind}: scraping {} returned HTTP {status_code}",
                            target.redacted_url
                        ),
                    );
                    (status_class(status_code), 0.0, 0, Vec::new())
                }
                ScrapeStatus::NetworkError => {
                    self.diag.warn_throttled(
                        "scrape_failed",
                        format_args!(
                            "{kind}: scraping {} failed: connection error",
                            target.redacted_url
                        ),
                    );
                    ("network_error", 0.0, 0, Vec::new())
                }
                ScrapeStatus::Timeout => {
                    self.diag.warn_throttled(
                        "scrape_failed",
                        format_args!("{kind}: scraping {} timed out", target.redacted_url),
                    );
                    ("timeout", 0.0, 0, Vec::new())
                }
                ScrapeStatus::Oversize => {
                    self.diag.warn_throttled(
                        "scrape_failed",
                        format_args!(
                            "{kind}: scraping {} exceeded the {max_body_bytes}-byte scrape limit",
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
impl<D: ScrapeDecoder> Input for ScrapeInput<D> {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(self.interval);
        // `Delay`, not `Burst`: a multi-interval downstream stall is ordinary here, and `Burst`
        // would then hit each target N times back-to-back with near-identical timestamps,
        // inflating `logit.input.scrapes`/`samples` and the synthetic series. The module doc's
        // "The tick" section has the rest.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Swallow the immediate first tick: the first scrape is one `interval` after start.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            self.tick(&sink).await;
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use bytes::Bytes;
    use http_body_util::Full;
    use hyper::body::Incoming;
    use hyper::service::service_fn;
    use hyper::{Request as HttpRequest, Response};
    use hyper_util::rt::TokioIo;
    use logit_pipeline::Delivered;
    use std::convert::Infallible;
    use std::net::SocketAddr;
    use std::sync::Mutex;
    use tokio::sync::mpsc;

    const STUB_KIND: &str = "stub_in";
    const STUB_ACCEPT: &str = "text/x-stub";
    const STUB_CAP: usize = 16;

    /// A decoder that turns any body into one event and rejects a body starting with `!`, so a
    /// test sees the driver's side of the seam alone.
    #[derive(Default)]
    struct StubDecoder;

    impl ScrapeDecoder for StubDecoder {
        type Error = &'static str;

        fn kind(&self) -> &'static str {
            STUB_KIND
        }

        fn accept(&self) -> &'static str {
            STUB_ACCEPT
        }

        fn max_body_bytes(&self) -> usize {
            STUB_CAP
        }

        fn resource(&self, index: usize, _url: &str) -> Resource {
            let mut attributes = AttrMap::new();
            attributes.insert("stub.index", Value::I64(index as i64));
            Resource { attributes, ..Default::default() }
        }

        fn set_diagnostics(&mut self, _diag: Diagnostics) {}

        fn set_telemetry(&mut self, _telemetry: Telemetry) {}

        fn decode(
            &mut self,
            body: &[u8],
            _content_type: Option<&str>,
            timestamp: i64,
        ) -> Result<Vec<Event>, Self::Error> {
            if body.starts_with(b"!") {
                return Err("stub rejects a leading '!'");
            }
            Ok(vec![synthetic_event(timestamp, "stub.decoded", 1.0)])
        }
    }

    /// A real `hyper` HTTP/1.1 server on an ephemeral port answering every request `200` with
    /// `body`. Returns the bound address and each request's captured headers.
    pub(crate) async fn canned_server(body: Bytes) -> (SocketAddr, Arc<Mutex<Vec<HeaderMap>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let captured_task = captured.clone();
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let body = body.clone();
                let captured = captured_task.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: HttpRequest<Incoming>| {
                        captured.lock().unwrap().push(req.headers().clone());
                        let body = body.clone();
                        async move { Ok::<_, Infallible>(Response::new(Full::new(body))) }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        (addr, captured)
    }

    /// The batch a tick already sent: `tick` returns after its send, so an empty channel is a
    /// failure, not a slow scrape.
    pub(crate) fn sent_batch(rx: &mut mpsc::Receiver<Delivered>) -> EventBatch {
        match rx.try_recv().expect("expected a batch to have been sent") {
            Delivered::Owned(batch, _ctx) => batch,
            Delivered::Shared(shared, _ctx) => (*shared).clone(),
        }
    }

    fn gauge(batch: &EventBatch, name: &str) -> Option<f64> {
        batch.events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match m.kind {
                MetricKind::Gauge(v) if logit_core::interner::resolve(m.name) == name => {
                    Some(v.as_f64())
                }
                _ => None,
            })
        })
    }

    async fn tick_once(url: &str) -> EventBatch {
        let mut input = ScrapeInput::<StubDecoder>::new(vec![url.to_string()], Duration::MAX);
        let (tx, mut rx) = mpsc::channel(4);
        input.tick(&Fanout::new(vec![tx])).await;
        sent_batch(&mut rx)
    }

    /// Self-log output rendered on this thread while the guard lives.
    #[derive(Clone, Default)]
    struct Captured(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for Captured {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl Captured {
        fn start() -> (Self, tracing::subscriber::DefaultGuard) {
            let captured = Captured::default();
            let writer = captured.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .finish();
            (captured, tracing::subscriber::set_default(subscriber))
        }

        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
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
    fn server_resource_takes_host_port_and_path_from_the_url() {
        let attrs = |url| server_resource(0, url).attributes;

        let a = attrs("http://user:pw@Status.Example:8080/a/nginx_status?token=secret");
        assert_eq!(a.get(SERVER_ADDRESS), Some(&Value::str("status.example")));
        assert_eq!(a.get(SERVER_PORT), Some(&Value::I64(8080)));
        assert_eq!(a.get(URL_PATH), Some(&Value::str("/a/nginx_status")));
        assert_eq!(a.len(), 3);

        assert_eq!(attrs("http://h/s").get(SERVER_PORT), Some(&Value::I64(80)));
        assert_eq!(attrs("https://h/s").get(SERVER_PORT), Some(&Value::I64(443)));

        let v6 = attrs("http://[::1]:8080/nginx_status");
        assert_eq!(v6.get(SERVER_ADDRESS), Some(&Value::str("::1")));
        assert_eq!(v6.get(SERVER_PORT), Some(&Value::I64(8080)));

        assert_ne!(
            server_resource(0, "http://proxy/a/nginx_status").attributes,
            server_resource(1, "http://proxy/b/nginx_status").attributes,
        );

        let bad = attrs("http://[::1/nginx_status");
        assert_eq!(bad.get(SERVER_ADDRESS), Some(&Value::str("<unparseable target #0>")));
        assert_eq!(bad.len(), 1);
    }

    #[tokio::test]
    async fn the_decoders_accept_value_reaches_the_request_and_its_resource_the_batch() {
        let (addr, captured) = canned_server(Bytes::from_static(b"ok")).await;

        let batch = tick_once(&format!("http://{addr}/status")).await;

        let headers = captured.lock().unwrap();
        assert_eq!(headers[0].get(ACCEPT).and_then(|v| v.to_str().ok()), Some(STUB_ACCEPT));
        assert_eq!(
            headers[0].get(USER_AGENT).and_then(|v| v.to_str().ok()),
            Some(USER_AGENT_VALUE)
        );
        assert_eq!(batch.resource.attributes.get("stub.index"), Some(&Value::I64(0)));
        assert_eq!(gauge(&batch, "stub.decoded"), Some(1.0));
        assert_eq!(gauge(&batch, "up"), Some(1.0));
        assert_eq!(gauge(&batch, "scrape_samples_scraped"), Some(1.0));
    }

    /// The read stops at the decoder's cap, not the driver's: a body at the cap decodes, and one
    /// byte past it is `oversize`.
    #[tokio::test]
    async fn the_decoders_body_cap_bounds_the_read() {
        let (at_cap, _) = canned_server(Bytes::from(vec![b'a'; STUB_CAP])).await;
        let (past_cap, _) = canned_server(Bytes::from(vec![b'a'; STUB_CAP + 1])).await;

        let batch = tick_once(&format!("http://{at_cap}/status")).await;
        assert_eq!(gauge(&batch, "up"), Some(1.0));

        let batch = tick_once(&format!("http://{past_cap}/status")).await;
        assert_eq!(gauge(&batch, "up"), Some(0.0));
        assert_eq!(gauge(&batch, "stub.decoded"), None);
    }

    #[tokio::test]
    async fn a_scrape_failed_diagnostic_names_the_decoders_kind() {
        let (addr, _) = canned_server(Bytes::from_static(b"!bad")).await;
        let (captured, _guard) = Captured::start();

        let batch = tick_once(&format!("http://{addr}/status")).await;

        assert_eq!(gauge(&batch, "up"), Some(0.0));
        let logs = captured.text();
        assert!(
            logs.contains(&format!("{STUB_KIND}: scraping http://{addr}/status succeeded"))
                && logs.contains("stub rejects a leading '!'"),
            "got: {logs}"
        );
    }

    #[test]
    fn every_headers_construction_error_names_the_decoders_kind() {
        let cases = [
            HashMap::from([("bad name".to_string(), "v".to_string())]),
            HashMap::from([("x-ok".to_string(), "bad\nvalue".to_string())]),
            HashMap::from([
                ("X-Scope-OrgID".to_string(), "a".to_string()),
                ("x-scope-orgid".to_string(), "b".to_string()),
            ]),
        ];
        for headers in cases {
            let input = ScrapeInput::<StubDecoder>::new(vec![], Duration::MAX);
            match input.with_headers(&headers) {
                Ok(_) => panic!("expected {headers:?} to fail construction"),
                Err(err) => {
                    assert!(err.to_string().starts_with(&format!("{STUB_KIND}: ")), "got: {err}")
                }
            }
        }
    }

    #[test]
    fn a_scrape_tls_construction_error_names_the_decoders_kind() {
        let dir = logit_pipeline::test_util::scratch_dir("scrape-tls-error");
        let settings =
            TlsClientSettings { ca_file: Some("missing.pem".to_string()), ..Default::default() };
        let input = ScrapeInput::<StubDecoder>::new(vec![], Duration::MAX);
        match input.with_tls(&settings, &dir, &logit_pipeline::tls::TlsReloader::new()) {
            Ok(_) => panic!("expected a missing ca_file to fail construction"),
            Err(err) => assert!(
                format!("{err:#}").starts_with(&format!("{STUB_KIND}: scrape_tls: ")),
                "got: {err:#}"
            ),
        }
    }
}
