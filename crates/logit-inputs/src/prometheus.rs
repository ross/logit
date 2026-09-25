//! `prometheus_in`: **two modes on one kind.** `scrape_targets:` scrapes Prometheus `/metrics`
//! endpoints on an interval; `bind:` is a Prometheus **remote-write receiver**, accepting 1.0 and
//! 2.0 requests on one listener. Exactly one of the two is configured: graph rule 55, which also
//! rejects a field belonging to the other mode rather than ignoring it. The designs are
//! [ADR `prometheus-scrape-and-exposition`](../../../docs/adr/prometheus-scrape-and-exposition.md)
//! and [ADR `prometheus-remote-write`](../../../docs/adr/prometheus-remote-write.md); this module
//! doc is the implementation's spec.
//!
//! ## Config
//!
//! ```yaml
//! # scrape mode
//! scrape_targets: ["http://node-exporter:9100/metrics"]   # non-empty, absolute http(s) URLs
//! interval: 15s         # scrape cadence; default 15s
//! timeout: 10s          # per-request timeout; default 10s
//! headers: {}           # optional extra request headers
//! scrape_tls: {}        # TlsClientConfig -- only meaningful when a target is https://
//! ```
//!
//! ```yaml
//! # bind mode (a remote-write receiver)
//! bind: "0.0.0.0:9090"  # host:port to accept remote-write POSTs on
//! path: /api/v1/write   # the one route POSTs are accepted on; default /api/v1/write
//! bind_tls: {}          # TlsServerConfig -- its presence turns TLS on
//! idle_timeout: 60s     # optional; omitted means no idle timeout
//! metadata_cache:       # what 1.0 metric types are remembered between requests
//!   max_families: 10000 #   0 turns the cache off
//!   ttl: 10m
//! ```
//!
//! `scrape_targets`, not `targets`: `Component.targets` (`docs/adr/target-components.md`) claims
//! the bare name at the flattened top level. The TLS keys are `scrape_tls`/`bind_tls`, never a
//! bare `tls`: each mode has its own TLS role (client TLS dialling out, server TLS terminating
//! in), and each key names the socket it governs.
//!
//! # Scrape mode
//!
//! ## Modeled on `internal.rs`
//!
//! Interval-driven, like [`crate::internal::InternalInput`]: [`Input::run`] owns a
//! `tokio::time::interval` ticker, swallows its immediate first tick (so the first scrape happens
//! one full `interval` after start, not at t=0), and calls [`PrometheusInput::tick`], a plain
//! method testable without a runtime harness. There is no `bind` override: a scrape client has no
//! socket to open ahead of time.
//!
//! The ticker's missed-tick behavior is `Delay`, not the default `Burst`. `internal`'s tick does
//! no network I/O, so `Burst` (fire every missed tick back-to-back once a stall clears) is
//! harmless there. This tick awaits `sink.send` (bounded-channel backpressure) plus a per-target
//! request timeout, so a downstream stall lasting several intervals is ordinary, and `Burst` would
//! turn it into N full scrape rounds in a row. `Delay` resumes on a fixed cadence, matching
//! Prometheus's own scrape scheduler, which skips a missed scrape rather than bursting to catch up.
//!
//! ## Dialect negotiation
//!
//! Every request carries `Accept: application/openmetrics-text;version=1.0.0,text/plain;
//! version=0.0.4;q=0.5,*/*;q=0.1` and `User-Agent: logit/<CARGO_PKG_VERSION>`, but this input
//! never *forces* a dialect: the one a target sent is read off the response's own `Content-Type`
//! via [`Dialect::from_content_type`] (`application/openmetrics-text` selects OpenMetrics;
//! anything else, including a missing header, is text 0.0.4). A target that ignores `Accept` and
//! always answers in text 0.0.4 still decodes.
//!
//! ## Resource identity
//!
//! Built once per target in [`PrometheusInput::new`], never per scrape:
//!
//! - an unprefixed `instance` ([`logit_proto::prometheus::LABEL_INSTANCE`]): `host:port`, the port
//!   defaulted to 80/443 when the target URL omits one, matching Prometheus's own scrape;
//! - [`logit_proto::prometheus::ATTR_TARGET`]: the scrape URL with its userinfo and query string
//!   stripped ([`redact_url`]). A scrape URL may carry a credential (`user:pass@` is a legitimate
//!   way to configure basic auth), and this attribute reaches whatever sink the pipeline routes
//!   to.
//!
//! Both ride on every batch the target produces, synthetic metrics included, which keeps two
//! targets exposing the same exporter from colliding once relayed onward.
//!
//! ## Synthetic scrape metrics
//!
//! Every tick, every target, scrape failures included, gets exactly three synthetic series
//! appended to its batch, on that target's resource, with no wire timestamp marker: `up`
//! (`Gauge(0|1)`), `scrape_duration_seconds` (`Gauge`, wall-clock seconds for that target's
//! request), and `scrape_samples_scraped` (`Gauge`, the number of series decoded, `0` on any
//! failure). The ADR's "Synthetic scrape metrics" section says why these three, and why they're
//! always on. **Bind mode synthesizes none of them**: a receiver performed no scrape, so there is
//! no `up` to report and no duration to measure.
//!
//! ## Counters
//!
//! `logit.input.scrapes{class}` -- one count per target per tick, `class` one of `2xx`/`4xx`/`5xx`/
//! `other` (HTTP response classes), `network_error`, `timeout`, `parse_error` (a 2xx response body
//! that failed to parse), or `oversize` (a response body that exceeded [`MAX_SCRAPE_BYTES`]).
//! `logit.input.scrape.duration` -- a timing sample per target per tick, recorded regardless of
//! outcome. `logit.input.samples` -- the number of series decoded, summed across every target.
//! A scrape failure also reports `Diagnostics::warn_throttled("scrape_failed", ..)` with the
//! target's [`redact_url`]ed form in the message text only: never a tag (a target URL isn't
//! `&'static` and isn't safe to intern per target, `logit_core::telemetry::Tag`'s convention),
//! and never the raw URL, which may carry a credential.
//!
//! # Bind mode: the remote-write receiver
//!
//! [`PrometheusReceiver`] is an HTTP listener: `bind:` opens a `TcpListener` ([`Input::bind`],
//! idempotent), `run` is an accept loop, and each connection is served by
//! [`hyper_util::server::conn::auto::Builder`], HTTP/1.1 and h2c off the same socket, since a
//! remote-write sender may speak either. Both wire versions are accepted on that one listener,
//! chosen **per request** from its own `Content-Type` with nothing to configure: the 2.0 spec
//! requires a 2.0-capable receiver to keep accepting 1.0, and a mixed-version fleet can then
//! share one receiver.
//!
//! ## Routes
//!
//! | Request | Response |
//! |---|---|
//! | `POST path`, `Content-Encoding: snappy` or `zstd`, recognised `Content-Type` | decode, then `204` |
//! | any other path | `404` |
//! | any other method on `path` | `405` + `Allow: POST` |
//! | missing or other `Content-Encoding`, unrecognised or missing `Content-Type` | `415` |
//! | a body, or its decompressed size, over [`MAX_REQUEST_BYTES`] | `413` |
//! | a body that stops arriving mid-upload, **when `idle_timeout:` is set** (it is off by default, and the stall bound is derived from it) | `408`, and the connection closes |
//! | Snappy, zstd, or protobuf failure, 2.0 symbol-table errors | `400`, `text/plain` reason |
//!
//! **`zstd` is the VictoriaMetrics remote write protocol**: a 1.0 request compressed with zstd,
//! which vmagent sends by default
//! ([ADR `victoriametrics-interop`](../../../docs/adr/victoriametrics-interop.md)). Any other
//! encoding stays `415` because vmagent downgrades to Snappy on a `415` or `400`, and on nothing
//! else. A zstd body under a 2.0 `Content-Type` is decoded like any other; only the sender's rule
//! 56 pairs zstd with 1.0.
//!
//! **`405` diverges from `otlp_in`**, which answers `404` for a non-`POST`. `prometheus_out`'s
//! exposition server answers `405` for a wrong method on `/metrics`, and this receiver matches
//! that sibling on the same kind pair rather than the unrelated input whose accept loop it
//! copied. A reader diffing the two inputs finds the reason here rather than a bug.
//!
//! **A missing `Content-Type` is a `415`, not a default.** `otlp_in` reads an absent type as
//! protobuf because every client predating its JSON support sent none. Both remote-write specs
//! require the header, so guessing 1.0 would turn a 2.0 sender's misconfiguration into a wall of
//! protobuf decode errors instead of the one status the spec has for it.
//!
//! **The `-Written` headers.** A 2.0 request's response carries
//! `X-Prometheus-Remote-Write-{Samples,Histograms,Exemplars}-Written`, on `4xx` as well as `2xx`
//! as 2.0 requires, reporting what this receiver stored: zeros on a rejection, and always `0`
//! histograms, since native histograms are skipped and counted rather than stored
//! (`docs/known-gaps.md`). A 1.0 request gets none: 1.0 defines none.
//!
//! *Samples-written is measured on the way out, not on the way in.* The codec's accepted count is
//! what the assembler took, and the model mapping that runs afterwards can still drop a whole
//! series (an empty histogram, a histogram whose bucket counts decrease). So the number reported
//! is every decoded series' wire samples minus those of each series the mapping dropped, both
//! measured by [`logit_proto::prometheus::remote_write::wire_samples`], which reads the
//! [`logit_proto::prometheus::Point`] and so still sees the `Option`s the wire had (a summary sent
//! as quantiles alone is two samples, not four). A request whose every series was dropped answers
//! `204` with `Samples-Written: 0` and sends no batch.
//!
//! ## What a decoded request becomes
//!
//! **One `EventBatch` per request**, with an **empty `Resource`** and `received_at` = now, built
//! by concatenating [`families_to_events`] over the decoded timestamp groups. A request that
//! decodes to no events sends no batch: a `204` and nothing downstream.
//!
//! **Labels stay labels.** `instance` and `job` arrive as ordinary labels (both optional per spec,
//! neither structurally distinguished on the wire) and stay ordinary event attributes, verbatim.
//! Nothing is lifted into `Resource` and no `prometheus.target` is stamped, the opposite of scrape
//! mode, because each mode knows something different: a scrape connected to the target it names,
//! while a receiver observed a TCP connection from a sender that may be relaying for thousands of
//! targets. Promoting a payload label to resource identity would invent structure the wire did
//! not carry and change the label set a remote-write → remote-write relay re-emits. An operator
//! who wants resource identity adds it with a downstream `set` component or Lua.
//!
//! **Timestamps and timestamp groups.** A remote-write `TimeSeries` is one label set and N
//! samples; a `Series` holds one point and one timestamp. So the codec partitions a request's
//! samples by timestamp and returns one family list per distinct timestamp in ascending order
//! (`logit_proto::prometheus::remote_write`'s own doc), and this receiver concatenates them into
//! one batch: N events per series, in timestamp order. `Event::timestamp` comes from the sample,
//! and the decoder runs with `with_timestamp_marker(false)`, so **no `prometheus.timestamp: true`
//! attribute is set**: that marker records a *producer's choice* to expose a timestamp on an
//! exposition line, and a transport that mandates one is not that choice. Setting it would make a
//! remote-write → exposition relay stamp an explicit timestamp on every line it writes, which no
//! scrape of the same data would have produced.
//!
//! ## Metadata cache
//!
//! **Why there is one.** Remote-write carries a family's type, `# HELP` and `# UNIT` as
//! *metadata*, and 1.0 puts it in `WriteRequest.metadata[]`, which Prometheus's own sender ships
//! in **separate requests** on its own schedule (`metadata_config`, by default once a minute)
//! rather than attached to the samples it describes. A receiver that remembers nothing sees, for
//! nearly every 1.0 request, flat series with no type anywhere in the message: every family
//! decodes as `unknown`, and `http_request_duration_seconds_bucket`/`_sum`/`_count` arrive as
//! three unrelated series instead of one histogram. Nothing is lost (the samples and labels are
//! exact, and a relay back out to remote-write is still a fixed point), but the model kinds are
//! flatter than the producer's, which is what the cache fixes.
//!
//! [`MetadataCache`] holds `family name -> (type, help, unit, last seen)`, seeded into every decode
//! ([`remote_write::decode_with`]) and learned from every request's own
//! [`Decoded::declarations`](remote_write::Decoded::declarations): 1.0's `metadata[]` and 2.0's
//! inline `Metadata` alike, so a mixed-version fleet fills one table and a 2.0 sender's
//! declarations type a 1.0 sender's series.
//!
//! **Precedence: the request, then the cache.** A declaration in the request being decoded always
//! wins, per family name; the cache answers only for a family that request said nothing about. A
//! sender that retypes a family retypes it immediately, however stale the remembered entry.
//!
//! **What a TTL expiry means.** An expired family stops being typed (its next samples decode as
//! `unknown` and its `_bucket`/`_sum`/`_count` series come apart again) until the sender's next
//! metadata request re-declares it. That is the bound working: a sender that has stopped writing
//! should stop costing memory, and a remembered type nothing has reasserted within the TTL is a
//! guess about a series that may no longer exist. The default 10m is an order of magnitude over
//! Prometheus's own metadata cadence, so a live sender has to miss ten refreshes running to lapse.
//!
//! **Bounds.** `max_families` caps the table; over it, the **least-recently-seen** family is
//! evicted first (ties broken by name, so it is a function of the data rather than of map order),
//! the same policy and one-pass shape as `prometheus_out`'s exposition `max_series:`. A family's
//! `# HELP` and `# UNIT` are each cut to [`MAX_METADATA_TEXT_BYTES`] as they are remembered,
//! counted `logit.input.metadata_cache.truncated`: the request cap bounds a request, not a table
//! that keeps things. `max_families: 0` is not a zero-size cache but no cache at all: nothing is
//! allocated, no lock is taken, and requests decode through the stateless
//! [`remote_write::decode`]. Rule 55 rejects `ttl: 0s`, the pointless version of that.
//!
//! **Whose table it is.** One per component, not one per sender: every peer that can `POST` to
//! this listener writes to the same table, is typed from it, and is evicted by the same
//! `last_seen` order. So a peer's declarations are visible to every other peer (the point, since
//! that is what lets a 2.0 sender type a 1.0 one), and a peer that declares a great many families
//! evicts everyone else's, counted `evicted{reason="cardinality"}` but not attributed. Repeated
//! faster than the victims' own metadata cadence, that keeps well-behaved senders permanently
//! untyped. The cache does not *drop* their samples (a remembered type gives way to a sample it
//! cannot place rather than rejecting it, [`remote_write::decode_with`]), but flat families are
//! what they get. Nothing here authenticates a sender, so this is the "Security posture" rule
//! again: do not point this listener at untrusted senders. An operator who has to, and would
//! rather have flat families than a table anyone can churn, sets `max_families: 0`, which turns
//! the sharing off along with the typing.
//!
//! **Concurrency, and what a request pays.** One `std::sync::Mutex` guards the table, taken to
//! build the seed and again to learn, **never held across the decode**: a connection's requests
//! are served concurrently and the decode is the expensive part. A blocking mutex parks the Tokio
//! worker thread of anyone waiting on it, so the held section is `O(1)` unless the table changed:
//!
//! - the seed handed to the codec is an `Arc` of the table in the codec's own shape, rebuilt only
//!   when the table's *contents* change; re-declaring what is already remembered (which
//!   Prometheus does every `send_interval`, from every shard) touches `last_seen` and nothing else;
//! - the expiry sweep is *checked* per request, not performed: the table carries the earliest
//!   instant at which any entry could go, so until then expiry costs one comparison;
//! - a request that declares nothing (nearly every 1.0 request) does not take the lock a second
//!   time.
//!
//! So a sample-only request pays a lock, a comparison and a refcount bump, and the passes that
//! scale with the table happen only when an entry is added, retyped or expired.
//!
//! ## Size, concurrency, and shutdown
//!
//! [`MAX_REQUEST_BYTES`] (4 MiB) bounds the **decompressed** body, through
//! [`logit_proto::prometheus::compression::decompress_bounded`]: Snappy's own `decompress_len`
//! before a byte is expanded, and for zstd the declared content size, the window size, and a
//! streaming decode that stops one byte past the cap. So a compression bomb is rejected rather
//! than inflated. [`MAX_CONCURRENT_CONNECTIONS`] bounds how many connections are served at once; past
//! it a connection is rejected, not queued (`logit.input.connections.rejected{reason="limit"}`).
//! [`HANDSHAKE_TIMEOUT`] bounds each connection's pre-request phase: its TLS accept on a TLS
//! listener, its first byte on a plaintext one. None of the three is a config field: the first
//! two are denial-of-service bounds rather than tuning knobs, and graph rule 45's
//! `handshake_timeout:` does not cover this kind.
//!
//! `idle_timeout:` closes a connection that sits with no request in flight, via the shared
//! tracker in [`crate::http`]; `otlp_in`'s module doc holds the reasoning (why the clock is at
//! the service rather than the socket, why it resets on request *completion*, and why the close is
//! `graceful_shutdown` plus a bounded grace rather than a drop). A request whose *body* stalls gets
//! the narrower per-frame bound instead and answers `408`, **derived from the same field, so it
//! exists only where `idle_timeout:` is set.** It is off by default, so a default `bind:` has no
//! bound on a half-uploaded request: it holds its [`MAX_CONCURRENT_CONNECTIONS`] permit until the
//! sender goes away. Set it on any listener a real fleet writes to
//! ([ADR `idle-connection-timeout`](../../../docs/adr/idle-connection-timeout.md)'s "recommend it
//! on wherever consistent traffic is expected"; `examples/prometheus-remote-write-receive.yaml`
//! ships a value). There is no listener-level graceful shutdown here or anywhere else in this
//! repo: shutdown is per connection.
//!
//! ## Security posture
//!
//! `bind_tls:` gives transport security and nothing else. **The receiver has no authentication of
//! any kind** (no bearer token, no basic auth, no mutual-TLS identity check beyond `rustls`
//! accepting a client certificate chain when `client_ca_file` is set), so anything that can reach
//! the socket can write series into the pipeline. `admin:` and `prometheus_out`'s exposition
//! `bind:` carry the same gap, tracked in `docs/known-gaps.md`: front it with something that
//! authenticates, or keep it on a trusted network.
//!
//! ## Counters
//!
//! `logit.input.writes{class}` -- one count per request, `class` one of `ok`, `not_found`,
//! `method`, `unsupported`, `oversize`, `timeout`, or `bad_request`. An `ok` count also carries
//! `encoding` (`snappy` or `zstd`), which is how to see whether a vmagent stayed on zstd. `logit.input.write.duration`
//! -- a timing sample per request, recorded regardless of outcome. `logit.input.samples` --
//! reused from scrape mode, counting the wire samples that reached the `Fanout`: every decoded
//! series' worth minus every series the model mapping then dropped, the number the `-Written`
//! header reports. The same number on purpose: a counter and a header disagreeing about one
//! request would be a puzzle with no right answer.
//!
//! `logit.input.metadata_cache.size` -- how many families are remembered, a gauge published
//! whenever the table changes (a transition, like `logit.input.connections`, not a per-request
//! restatement). `logit.input.metadata_cache.evicted{reason}` -- `expired` for a family whose `ttl`
//! ran out, `cardinality` for one pushed out of `max_families` by a newer one.
//! `logit.input.metadata_cache.replaced` -- one count per family a request retyped: the counter to
//! watch when a sender's model kinds look wrong, since a healthy fleet retypes almost nothing and
//! a steady stream here is two senders disagreeing about one family name.
//! `logit.input.metadata_cache.truncated` -- one count per help or unit string cut to
//! [`MAX_METADATA_TEXT_BYTES`] on its way into the table; the type is still remembered exactly, so
//! this bounds what one entry costs rather than what it types.
//!
//! The connection counters are `otlp_in`'s spelling verbatim
//! (`logit.input.connections{,.rejected,.closed}`), since this is the same accept loop. A rejected
//! request also reports `Diagnostics::warn_throttled("write_rejected", ..)` with the peer address
//! in the message text only, never a tag (a peer address isn't `&'static` and isn't safe to intern
//! per peer, `logit_core::telemetry::Tag`'s convention). The decoder's own
//! `logit.input.metrics.skipped{reason}` counts what it stepped over, native histograms included.

use crate::http::{
    body_read_error_message, collect_with_stall_bound, drive_with_idle, Activity, BodyReadError,
};
use crate::tls::apply_client_tls;
use crate::Input;
use bytes::Bytes;
use http::header::{HeaderMap, HeaderName, HeaderValue, ACCEPT, CONTENT_TYPE, USER_AGENT};
use http::{Method, StatusCode};
use http_body_util::{Full, Limited};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use logit_core::interner::intern;
use logit_core::{
    AttrMap, Diagnostics, Event, EventBatch, MetricKind, MetricRecord, Resource, Telemetry, Value,
};
use logit_pipeline::Fanout;
use logit_proto::prometheus::compression::{self, DecompressError, Encoding};
use logit_proto::prometheus::{
    families_to_events, families_to_events_with, remote_write, text, Dialect, FamilyType,
    MetricFamily, PrometheusDecoder, Series, ATTR_TARGET, LABEL_INSTANCE,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinSet;

// -------------------------------------------------------------------------------------------------
// Scrape mode: the interval-driven scrape client
// -------------------------------------------------------------------------------------------------

/// Hard cap on one scrape response's body, read incrementally via [`reqwest::Response::chunk`], so
/// a hostile or misconfigured exporter can't grow this input's memory unboundedly. 32 MiB is
/// generous for a very large `/metrics` page; an exporter that needs more is a problem to fix at
/// the source, not a reason to raise this.
const MAX_SCRAPE_BYTES: usize = 32 * 1024 * 1024;

/// A preference, never a forced dialect (the ADR's "Dialects and negotiation" section): the
/// dialect a response is in is always read off its own `Content-Type`, never assumed from this.
const ACCEPT_HEADER_VALUE: &str =
    "application/openmetrics-text;version=1.0.0,text/plain;version=0.0.4;q=0.5,*/*;q=0.1";

/// `logit/<CARGO_PKG_VERSION>`, a compile-time constant, so sending it allocates nothing.
const USER_AGENT_VALUE: &str = concat!("logit/", env!("CARGO_PKG_VERSION"));

/// `crate::tls::TlsClientSettings`, re-exported so `logit-cli::pipeline::build_spec` imports it
/// from this module, as it does `otlp::TlsServerSettings`.
pub use crate::tls::TlsClientSettings;

/// One configured scrape target: the URL to `GET`, its [`redact_url`]ed form (for everything that
/// isn't the request itself: diagnostics text, `prometheus.target`), and the `Resource` every
/// batch built from it carries. Built once in [`PrometheusInput::new`].
#[derive(Clone)]
struct Target {
    url: String,
    redacted_url: String,
    resource: Arc<Resource>,
}

/// `host:port` of `url`'s authority, the port defaulted to the scheme's (80/443) when absent: what
/// Prometheus's own `instance` label holds ([`logit_proto::prometheus::LABEL_INSTANCE`]).
///
/// Falls back to [`redact_url`]'s `index`-keyed placeholder if `url` doesn't parse, and that path
/// is reachable: rule 40 checks only scheme plus a non-empty authority (a `reqwest`-free
/// approximation, since `logit-pipeline` can't depend on it), so `http://999.999.999.999/metrics`
/// or `http://[::1/metrics` passes it and still fails `reqwest::Url::parse`. Never the raw `url`,
/// which may carry a `user:pass@` credential.
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

/// `url` with its userinfo (`user:pass@`) and query string stripped: what everything but the
/// scrape request itself (`prometheus.target`, every `scrape_failed` diagnostic) uses. `reqwest`
/// turns `http://user:pass@host/metrics` into an `Authorization` header, but the configured
/// `String` still holds the password, and `prometheus.target` reaches every sink the pipeline
/// routes to (InfluxDB tags, statsd tag sets, a forwarded OTLP resource, a stdout/file render).
/// The query string goes too, since `?token=...` is as real a credential shape. The fragment is
/// kept. Parsing also normalizes the result (host lowercased, a default port dropped), which is
/// harmless for a valid absolute URL.
///
/// Falls back to `<unparseable target #{index}>`, never the raw `url` and never a placeholder
/// shared across targets. Rule 40 (`logit-pipeline::graph`) only approximates a URL grammar
/// (scheme plus non-empty authority), so `http://999.999.999.999/metrics`,
/// `http://[::1/metrics`, `http://host:99999/metrics`, or a host with a space or an invalid
/// percent-escape reaches this function; `index` (the target's position in `scrape_targets`)
/// keeps two such targets from colliding onto one `instance`/`prometheus.target` and merging
/// their series.
fn redact_url(index: usize, url: &str) -> String {
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

/// One target's scrape outcome, which [`PrometheusInput::tick`] classifies into a
/// `logit.input.scrapes{class}` count and a decoded or failed batch.
enum ScrapeStatus {
    Ok { dialect: Dialect, body: Vec<u8> },
    Http(reqwest::StatusCode),
    NetworkError,
    Timeout,
    Oversize,
}

/// Scrapes one target, reading the body incrementally via [`reqwest::Response::chunk`] capped at
/// [`MAX_SCRAPE_BYTES`]. The fixed `Accept`/`User-Agent` pair is inserted after the extra
/// `headers`, so it wins even over a `headers` entry naming one of them, the same defense
/// `logit_outputs::otlp::OtlpOutput::send_http` uses for `Content-Type`.
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
    /// Kept across ticks so the decoder's throttled diagnostics (a malformed line, non-monotonic
    /// histogram buckets) accumulate occurrence counts over the component's lifetime.
    decoder: PrometheusDecoder,
    telemetry: Telemetry,
    diag: Diagnostics,
}

impl PrometheusInput {
    /// `targets` become the scrape list, each with a `Resource` built once here (this module's
    /// "Resource identity" section).
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

    /// Overrides the default 10s per-request timeout. Applied per scrape via
    /// `RequestBuilder::timeout`, not baked into the client, so it composes with
    /// [`PrometheusInput::with_tls`] in either call order.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the extra headers sent on every scrape request (`headers:` in config). Fails, as
    /// `otlp_out`'s `with_headers` does, on an illegal name or value, or two names that collide
    /// case-insensitively. Rule 40 already rejects a header this input sets itself (`accept`,
    /// `user-agent`, the other protocol-owned names); this catches the lexical faults the graph
    /// can't, and [`scrape_target`] still inserts `Accept`/`User-Agent` last.
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

    /// Sets client-side TLS (`scrape_tls:` in config) for any `https://` target; a no-op if
    /// `settings` is empty. Built on `reqwest`'s PEM loaders ([`crate::tls::apply_client_tls`]), so
    /// no `rustls` type appears in this crate's HTTP-client path.
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
                "scrape_tls.insecure_skip_verify is set -- the connection is encrypted, but \
                 this input will accept any certificate a scraped target presents, self-signed \
                 or otherwise",
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

    /// One scrape cycle: every target fetched concurrently (a [`JoinSet`]), then one `EventBatch`
    /// per target, decoded series plus the three synthetic metrics, built and sent sequentially on
    /// this task so `self.decoder`/`self.diag`/`self.telemetry` need no synchronization. A plain
    /// method, callable from a test with a canned server and a bare `Fanout`, like `internal.rs`'s
    /// `tick`.
    async fn tick(&mut self, sink: &Fanout) {
        let received_at = now_nanos();
        // Cloned, not borrowed, so the loop below can hold `&mut self.decoder`/`&mut self.diag`
        // while reading each target. A few `String` + `Arc<Resource>` clones per tick.
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
        // `Delay`, not `Burst`: a multi-interval downstream stall is ordinary here, and `Burst`
        // would then hit each target N times back-to-back with near-identical `received_at`,
        // inflating `logit.input.scrapes`/`samples` and the synthetic series. The module doc's
        // "Modeled on `internal.rs`" section has the rest.
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // Swallow the immediate first tick: the first scrape is one `interval` after start.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            self.tick(&sink).await;
        }
    }
}

// -------------------------------------------------------------------------------------------------
// Bind mode: the remote-write receiver
// -------------------------------------------------------------------------------------------------

/// Hard cap on one remote-write request's **decompressed** body, enforced by
/// [`compression::decompress_bounded`] without expanding past it (its module doc has the Snappy
/// check and zstd's three), so a compression bomb is rejected rather than inflated. Also the
/// compressed body's cap. The same number and hardcoded posture as `otlp_in`'s
/// cap: a denial-of-service bound, not a tuning knob. Prometheus's default
/// `max_samples_per_send` of 2000 puts a real request orders of magnitude under it, so an
/// operator who hits this has a misconfigured sender.
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;

/// Bounds the connections [`PrometheusReceiver`] serves at once, so [`MAX_REQUEST_BYTES`] bounds
/// the listener's worst case rather than one connection's. The same 1024 as `otlp_in`,
/// `logit_in` and `crate::tcp`'s listeners: no protocol reason to differ, and one figure for an
/// operator to learn. A connection past the cap is **rejected, not queued**, as on those.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// How long a connection has, per pre-request phase, before this listener releases its
/// [`MAX_CONCURRENT_CONNECTIONS`] permit: its TLS accept on a TLS listener, its first byte on a
/// plaintext one. The same 5s every other TCP listener here defaults to.
///
/// **Not an operator-facing field**, unlike `otlp_in`'s `handshake_timeout:`: `prometheus_in` has
/// no such field for graph rule 45 to check. A field can be added if a deployment needs one.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// `crate::tls::TlsServerSettings`, re-exported for the receiver's `bind_tls:` block: the
/// server-side twin of [`TlsClientSettings`] above, same convention as `otlp::TlsServerSettings`.
pub use crate::tls::TlsServerSettings;

const METADATA_CACHE_SIZE: &str = "logit.input.metadata_cache.size";
const METADATA_CACHE_EVICTED: &str = "logit.input.metadata_cache.evicted";
const METADATA_CACHE_REPLACED: &str = "logit.input.metadata_cache.replaced";
const METADATA_CACHE_TRUNCATED: &str = "logit.input.metadata_cache.truncated";

/// The longest `# HELP` or `# UNIT` text this receiver *remembers* for one family. Past it the
/// text is truncated on a `char` boundary, counted `logit.input.metadata_cache.truncated`.
///
/// The decode keeps whatever the request carried; this bounds only the copy that outlives the
/// request. Without it the cache's resident size is `max_families x` the *request* cap: 10 000
/// individually-legal metadata-only requests, each declaring one family with a multi-megabyte
/// help (a few KB once Snappy has seen the repeated bytes), would take the process down while
/// `metadata_cache.size` read a healthy 10 000. 1 KiB is an order of magnitude past the longest
/// `# HELP` any real exporter writes.
const MAX_METADATA_TEXT_BYTES: usize = 1024;

/// One remembered family declaration. The family's own name is the map key, not a field here.
///
/// `Arc<str>` rather than `String` so a seed rebuild (a second copy of the table for in-flight
/// requests to hold) shares this text instead of copying it: the cache and every live seed
/// generation cost one description each, not one per generation.
#[derive(Debug, Clone)]
struct CachedFamily {
    kind: FamilyType,
    help: Option<Arc<str>>,
    unit: Option<Arc<str>>,
    /// When a request last *declared* this family -- not when one last carried its samples. The
    /// TTL is a bound on how long a declaration is trusted, and a 1.0 sender re-declares on its own
    /// schedule regardless of how busy the series are.
    last_seen: Instant,
}

/// What the receiver remembers about metric types between requests (`metadata_cache:` in config):
/// the table and the two operations a request performs on it. This module's "Metadata cache"
/// section says why it exists and what its bounds mean.
///
/// Built only when the cache is on: `max_families: 0` leaves [`PrometheusReceiver::metadata_cache`]
/// `None`, and nothing here is allocated, locked or swept.
struct MetadataCache {
    /// Always `> 0` -- a zero cap is no cache at all, which is `None` one level up.
    max_families: usize,
    ttl: Duration,
    state: Mutex<CacheState>,
    /// Test-only: how many times the expiry sweep has walked the table. A sweep that expires
    /// nothing leaves no other trace, and "a sample-only request does not sweep" is the property
    /// [`CacheState::next_expiry`] exists for.
    #[cfg(test)]
    sweeps: std::sync::atomic::AtomicU64,
}

/// Everything behind the one lock. `families` is authoritative; `seed` is the same content in the
/// codec's shape, rebuilt only when `families` changes, so the common request (samples, no
/// declarations) hands the decoder a refcount bump rather than a copy of the table.
#[derive(Default)]
struct CacheState {
    families: HashMap<String, CachedFamily>,
    seed: Arc<remote_write::Declarations>,
    /// The earliest instant at which *any* entry could have expired, and `None` when the table is
    /// empty -- what lets a request that declares nothing skip the sweep entirely.
    ///
    /// A **lower bound**, never an over-estimate, which is the whole of its correctness: a sweep
    /// skipped because `now` has not reached this cannot have missed an expiry. Every sweep
    /// recomputes it; a learn only *lowers* it, because an entry whose `last_seen` moves forward
    /// can only expire later. Cap eviction may leave it early, which costs one sweep that finds
    /// nothing and recomputes.
    next_expiry: Option<Instant>,
}

impl CacheState {
    /// Recomputes [`CacheState::next_expiry`] from every entry still in the table. One pass, called
    /// only from a sweep, which has just made one anyway.
    fn recompute_expiry(&mut self, ttl: Duration) {
        self.next_expiry =
            self.families.values().filter_map(|family| family.last_seen.checked_add(ttl)).min();
    }

    /// Lowers [`CacheState::next_expiry`] to account for an entry that will expire at `now + ttl`.
    /// Never raises it: a stale-early watermark costs one sweep, a stale-late one loses an expiry.
    fn note_expiry(&mut self, now: Instant, ttl: Duration) {
        let Some(expires_at) = now.checked_add(ttl) else { return };
        self.next_expiry = Some(match self.next_expiry {
            Some(earliest) => earliest.min(expires_at),
            None => expires_at,
        });
    }

    /// The table in the codec's shape. Family names are copied (a `Declarations` owns its keys);
    /// the descriptions, the bulk of it, are `Arc` clones.
    fn rebuild_seed(&mut self) {
        let mut seed = remote_write::Declarations::default();
        for (name, family) in &self.families {
            seed.insert(name.as_str(), family.kind, family.help.clone(), family.unit.clone());
        }
        self.seed = Arc::new(seed);
    }
}

/// `text` bounded to [`MAX_METADATA_TEXT_BYTES`], cut on a `char` boundary, and whether it was cut.
fn bounded_text(text: &Option<Arc<str>>) -> (Option<Arc<str>>, bool) {
    let Some(text) = text else { return (None, false) };
    if text.len() <= MAX_METADATA_TEXT_BYTES {
        // The common path, and why this takes a reference: a description within the bound is
        // shared with the request that carried it, never copied.
        return (Some(Arc::clone(text)), false);
    }
    let mut end = MAX_METADATA_TEXT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    (Some(Arc::from(&text[..end])), true)
}

impl MetadataCache {
    fn new(max_families: usize, ttl: Duration) -> Self {
        MetadataCache {
            max_families,
            ttl,
            state: Mutex::new(CacheState::default()),
            #[cfg(test)]
            sweeps: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Expires what the TTL has run out on, then hands back the table to decode this request
    /// against. Counted `logit.input.metadata_cache.evicted{reason="expired"}`.
    ///
    /// Expiry is checked per request rather than on a timer (this input has no clock task, and a
    /// receiver nothing writes to has no memory to reclaim), but *checked*, not performed:
    /// [`CacheState::next_expiry`] says when the first entry could go, so until then this is one
    /// comparison and a refcount bump. A pass over the table per request would scale with what is
    /// remembered rather than with the request, the cost the seed is an `Arc` to avoid.
    fn seed(&self, now: Instant, telemetry: &Telemetry) -> Arc<remote_write::Declarations> {
        let mut state = self.lock();
        if state.next_expiry.is_some_and(|earliest| now > earliest) {
            self.sweep(&mut state, now, telemetry);
        }
        Arc::clone(&state.seed)
    }

    /// One pass, dropping every entry the TTL has run out on and recomputing the watermark from
    /// what is left. Allocation-free; the seed is rebuilt only if something expired.
    fn sweep(&self, state: &mut CacheState, now: Instant, telemetry: &Telemetry) {
        #[cfg(test)]
        self.sweeps.fetch_add(1, Ordering::Relaxed);
        let ttl = self.ttl;
        let mut expired = 0u64;
        state.families.retain(|_, family| {
            let stale = now.saturating_duration_since(family.last_seen) > ttl;
            expired += u64::from(stale);
            !stale
        });
        state.recompute_expiry(ttl);
        if expired > 0 {
            telemetry.count(METADATA_CACHE_EVICTED, expired as f64, &[("reason", "expired")]);
            state.rebuild_seed();
            telemetry.gauge(METADATA_CACHE_SIZE, state.families.len() as f64, &[]);
        }
    }

    /// Folds one request's declarations in (the newest statement about a family wins, and a
    /// *retype* is counted `logit.input.metadata_cache.replaced`), then evicts down to the cap.
    ///
    /// Returns before taking the lock when the request declared nothing, as nearly every 1.0
    /// request does: no entry to touch, and nothing can have grown past the cap.
    fn learn(
        &self,
        declarations: &remote_write::Declarations,
        now: Instant,
        telemetry: &Telemetry,
    ) {
        if declarations.is_empty() {
            return;
        }
        let mut state = self.lock();
        let mut replaced = 0u64;
        let mut truncated = 0u64;
        // Whether the *seed* has to be rebuilt, which re-declaring what is already remembered
        // does not require. Prometheus re-sends a family's metadata every `send_interval` from
        // every shard, so "identical to what is there" is the common case, and rebuilding for it
        // would copy the whole table under the lock once a minute per shard.
        let mut changed = false;
        for (name, declaration) in declarations.iter() {
            // Bounded on the way in: what is remembered outlives the request, and the request
            // cap does not bound a table that keeps entries.
            let (help, help_cut) = bounded_text(&declaration.help);
            let (unit, unit_cut) = bounded_text(&declaration.unit);
            truncated += u64::from(help_cut) + u64::from(unit_cut);
            match state.families.get_mut(name) {
                Some(existing) => {
                    let retyped = existing.kind != declaration.kind;
                    if retyped {
                        // Two senders disagreeing about one family name, or one that changed its
                        // mind. Keep the newest: the alternative types a live sender's series from
                        // a declaration nothing has repeated.
                        replaced += 1;
                    }
                    if retyped || existing.help != help || existing.unit != unit {
                        existing.kind = declaration.kind;
                        existing.help = help;
                        existing.unit = unit;
                        changed = true;
                    }
                    // Assigned only on a real change: an identical re-declaration (the common
                    // case) must leave the entry's `Arc`s where they are. The seed shares them and
                    // is not rebuilt for a no-op, so adopting the request's clones would leave the
                    // two holding equal strings in separate allocations.
                    //
                    // `last_seen` moves regardless: the TTL measures how long ago a sender last
                    // said this, and it just said it again.
                    existing.last_seen = now;
                }
                None => {
                    state.families.insert(
                        name.to_string(),
                        CachedFamily { kind: declaration.kind, help, unit, last_seen: now },
                    );
                    changed = true;
                }
            }
        }
        if replaced > 0 {
            telemetry.count(METADATA_CACHE_REPLACED, replaced as f64, &[]);
        }
        if truncated > 0 {
            telemetry.count(METADATA_CACHE_TRUNCATED, truncated as f64, &[]);
        }
        changed |= self.enforce_cap(&mut state, telemetry);
        // Every entry this touched expires at `now + ttl`, no earlier than the table's existing
        // watermark unless the table was empty, which is the case this is here for.
        state.note_expiry(now, self.ttl);
        if changed {
            state.rebuild_seed();
            telemetry.gauge(METADATA_CACHE_SIZE, state.families.len() as f64, &[]);
        }
    }

    /// Evicts least-recently-seen families until at most `max_families` remain, in **one pass over
    /// the table** however many go, for `prometheus_out`'s `Registry::enforce_cap` reason: over the
    /// cap is the steady state the cap exists for, so a `while len() > max` loop calling `min_by`
    /// would re-scan every candidate per eviction, just when cardinality is being diagnosed.
    ///
    /// The tie-break past `last_seen` is the family's name, so which of two families declared in
    /// one request goes is a function of the data, not of `Instant` resolution or map order.
    fn enforce_cap(&self, state: &mut CacheState, telemetry: &Telemetry) -> bool {
        let total = state.families.len();
        if total <= self.max_families {
            return false;
        }
        let excess = total - self.max_families;
        let mut candidates: Vec<(Instant, &str)> =
            state.families.iter().map(|(name, family)| (family.last_seen, name.as_str())).collect();
        // `excess <= total` and `total > max_families >= 1`, so `excess - 1` indexes `candidates`.
        candidates.select_nth_unstable(excess - 1);
        let doomed: Vec<String> =
            candidates[..excess].iter().map(|(_, name)| (*name).to_string()).collect();
        drop(candidates);

        for name in doomed {
            state.families.remove(&name);
        }
        telemetry.count(METADATA_CACHE_EVICTED, excess as f64, &[("reason", "cardinality")]);
        // The watermark may now point at an evicted entry (the oldest are the ones evicted),
        // which costs one sweep that finds nothing and recomputes it.
        true
    }

    /// The lock, unpoisoned. Nothing panics while it is held (map operations on owned data), and a
    /// receiver that stopped typing metrics because one request panicked would be a worse failure
    /// than the panic.
    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// `prometheus_in` in **bind mode**: a Prometheus remote-write receiver. The module doc has the
/// config, the routes table, and every mapping decision; this type is the accept loop and the
/// request handler that implement them.
pub struct PrometheusReceiver {
    bind: String,
    /// The one path this receiver answers `POST`s on. An `Arc<str>`: cloned per connection, never
    /// per request, and never re-allocated.
    path: Arc<str>,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Set by [`Input::bind`], taken by [`Input::run`]. `None` after a run, so a second run
    /// rebinds.
    listener: Option<tokio::net::TcpListener>,
    /// `None`, the default, means no idle timeout. See this module's "Size, concurrency, and
    /// shutdown" section.
    idle_timeout: Option<Duration>,
    /// The one state this receiver holds across requests; `None` under
    /// `metadata_cache: {max_families: 0}`, which keeps the stateless decode path. Shared by every
    /// connection, hence the `Arc`; see this module's "Metadata cache" section.
    metadata_cache: Option<Arc<MetadataCache>>,
    handshake_timeout: Duration,
    max_connections: usize,
    /// The empty `Resource` every batch this receiver builds carries, allocated once (this
    /// module's "Labels stay labels").
    resource: Arc<Resource>,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl PrometheusReceiver {
    /// `bind` is a `host:port`; `path` is the route `POST`s are accepted on (config's `path:`,
    /// defaulting to `/api/v1/write`).
    pub fn new(bind: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            bind: bind.into(),
            path: Arc::from(path.into()),
            tls: None,
            listener: None,
            idle_timeout: None,
            metadata_cache: None,
            handshake_timeout: HANDSHAKE_TIMEOUT,
            max_connections: MAX_CONCURRENT_CONNECTIONS,
            resource: Arc::new(Resource::default()),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    /// The address bound, once [`Input::bind`] has run, so a test can learn the OS-assigned port
    /// without a bind-drop-rebind race.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// Turns on TLS termination (`bind_tls:` in config). Both ALPN protocols the auto builder can
    /// serve are advertised, so a TLS client's negotiation picks what the plaintext path sniffs.
    pub fn with_bind_tls(
        mut self,
        settings: &TlsServerSettings,
        base_dir: &Path,
    ) -> anyhow::Result<Self> {
        let alpn: &[&[u8]] = &[b"h2", b"http/1.1"];
        self.tls = Some(Arc::new(crate::tls::build_server_config(settings, base_dir, alpn)?));
        Ok(self)
    }

    /// Bounds how long a connection may sit with no request in flight before this listener closes
    /// it (`idle_timeout:` in config; off when `None` or never called). Graph rule 53 rejects
    /// `Some(0s)`. Takes the `Option`, like every other listener's `with_idle_timeout`.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Turns on the metadata cache (`metadata_cache:` in config). `max_families == 0` is the
    /// operator's "off" and leaves this receiver on the stateless decode path. Graph rule 55
    /// rejects a zero `ttl`.
    pub fn with_metadata_cache(mut self, max_families: usize, ttl: Duration) -> Self {
        self.metadata_cache =
            (max_families > 0).then(|| Arc::new(MetadataCache::new(max_families, ttl)));
        self
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`], so the cap is reachable with two
    /// connections instead of 1025.
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Test-only override of [`HANDSHAKE_TIMEOUT`], so a test can watch a silent connection close
    /// without a multi-second sleep.
    #[cfg(test)]
    fn with_handshake_timeout(mut self, handshake_timeout: Duration) -> Self {
        self.handshake_timeout = handshake_timeout;
        self
    }
}

#[async_trait::async_trait]
impl Input for PrometheusReceiver {
    async fn bind(&mut self) -> anyhow::Result<()> {
        if self.listener.is_some() {
            return Ok(()); // idempotent, per `Input::bind`'s contract
        }
        let listener = tokio::net::TcpListener::bind(&self.bind).await?;
        self.diag.info("bound", format_args!("listening on {}", self.bind));
        self.listener = Some(listener);
        Ok(())
    }

    /// `otlp_in`'s accept loop: a permit per connection acquired before any TLS accept, the
    /// handshake (or plaintext first-byte peek) bounded inside the spawned task, and `hyper_util`'s
    /// auto builder serving HTTP/1.1 and h2c off one socket. Copied rather than shared because the
    /// telemetry, handler and dispatch are each listener's own; the idle machinery it hands off to
    /// is shared ([`crate::http`]).
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        self.bind().await?;
        let listener = self.listener.take().expect("bind() leaves a listener behind");
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        // `TlsAcceptor::from` wraps the `Arc<ServerConfig>`, so a per-connection clone is an `Arc`
        // clone, not a config rebuild.
        let tls_acceptor = self.tls.clone().map(tokio_rustls::TlsAcceptor::from);
        let live_connections = Arc::new(AtomicI64::new(0));
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;
        let metadata_cache = self.metadata_cache.clone();
        // `crate::tcp`'s accept-queue gauges: `logit.input.accept_queue.depth`/`.utilization`,
        // sampled before each accept and once a second while waiting for one.
        let mut accept_queue =
            crate::tcp::AcceptQueueSampler::new(self.telemetry.clone(), self.diag.clone());
        loop {
            let (stream, peer) = accept_queue.accept(&listener).await?;

            // Non-blocking (`try_acquire_owned`): at capacity the connection is closed rather than
            // queued behind a permit that may never come, and *before* any TLS accept, since
            // remote-write has no in-band "try later" to spend a handshake delivering. The sender
            // retries from its own queue, which is the protocol's flow control.
            let Ok(permit) = connection_limit.clone().try_acquire_owned() else {
                self.telemetry.count(
                    "logit.input.connections.rejected",
                    1.0,
                    &[("reason", "limit")],
                );
                drop(stream);
                continue;
            };

            let sink = sink.clone();
            let path = Arc::clone(&self.path);
            let mut diag = self.diag.clone();
            let telemetry = self.telemetry.clone();
            let tls_acceptor = tls_acceptor.clone();
            let live_connections = Arc::clone(&live_connections);
            let resource = Arc::clone(&self.resource);
            let metadata_cache = metadata_cache.clone();
            tokio::spawn(async move {
                let _permit = permit; // held for the connection's lifetime; released on drop

                // Published from the read-modify-write's return value, not a separate `load`:
                // `Telemetry::gauge` is last-write-wins per key, so two tasks interleaving an add
                // and a load would leave the stale one published until the next transition.
                let live = live_connections.fetch_add(1, Ordering::Relaxed) + 1;
                telemetry.gauge("logit.input.connections", live as f64, &[]);

                let result = match tls_acceptor {
                    // No first-byte peek on this arm: `acceptor.accept` already waits on this
                    // connection's first bytes under the same budget.
                    Some(acceptor) => {
                        match tokio::time::timeout(handshake_timeout, acceptor.accept(stream)).await
                        {
                            Ok(Ok(tls_stream)) => {
                                serve_write_connection(
                                    TokioIo::new(tls_stream),
                                    peer,
                                    path,
                                    resource,
                                    metadata_cache,
                                    sink,
                                    telemetry.clone(),
                                    diag.clone(),
                                    idle_timeout,
                                    handshake_timeout,
                                )
                                .await
                            }
                            Ok(Err(err)) => Err(format!("TLS handshake failed: {err}")),
                            Err(_elapsed) => Err(format!(
                                "TLS handshake did not complete within {handshake_timeout:?}"
                            )),
                        }
                    }
                    // The plaintext arm's budget. `peek` is `recv(..., MSG_PEEK)`: it consumes
                    // nothing, so the auto builder's `ReadVersion` sniff still sees a pristine
                    // stream. A clean close before the first byte (`Ok(0)`) is a TCP health
                    // check, not a fault, as in `otlp_in` and `crate::tcp`.
                    None => {
                        let first_byte =
                            tokio::time::timeout(handshake_timeout, stream.peek(&mut [0u8; 1]))
                                .await;
                        match first_byte {
                            Ok(Ok(0)) => Ok(()),
                            Ok(Ok(_)) => {
                                serve_write_connection(
                                    TokioIo::new(stream),
                                    peer,
                                    path,
                                    resource,
                                    metadata_cache,
                                    sink,
                                    telemetry.clone(),
                                    diag.clone(),
                                    idle_timeout,
                                    handshake_timeout,
                                )
                                .await
                            }
                            Ok(Err(err)) => Err(format!("waiting for a first byte failed: {err}")),
                            Err(_elapsed) => {
                                Err(format!("no first byte received within {handshake_timeout:?}"))
                            }
                        }
                    }
                };

                let live = live_connections.fetch_sub(1, Ordering::Relaxed) - 1;
                telemetry.gauge("logit.input.connections", live as f64, &[]);

                // One connection's I/O error shouldn't be fatal to the listener or its siblings --
                // only `TcpListener::accept` failing in `run`'s own loop is.
                if let Err(err) = result {
                    diag.warn_throttled("connection_error", err);
                }
            });
        }
    }
}

/// Serves one accepted (and, on a TLS listener, handshaken) connection to completion. Generic
/// over the IO type so the plaintext and TLS cases share everything below `run`'s
/// `tls_acceptor` branch.
#[allow(clippy::too_many_arguments)]
async fn serve_write_connection<IO>(
    io: IO,
    peer: SocketAddr,
    path: Arc<str>,
    resource: Arc<Resource>,
    metadata_cache: Option<Arc<MetadataCache>>,
    sink: Fanout,
    telemetry: Telemetry,
    diag: Diagnostics,
    idle_timeout: Option<Duration>,
    grace: Duration,
) -> Result<(), String>
where
    IO: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    // One tracker per connection: the service stamps it as requests start and finish, the driver
    // below reads it. The body-frame stall bound is `idle_timeout` too, so a connection with no
    // idle bound gets no per-frame one either.
    let activity = Arc::new(Activity::new());
    let svc = service_fn({
        let activity = Arc::clone(&activity);
        let (sink, telemetry, diag) = (sink.clone(), telemetry.clone(), diag.clone());
        let (path, resource) = (Arc::clone(&path), Arc::clone(&resource));
        move |req| {
            // `enter` here, not inside the returned future: hyper calls the service as soon as a
            // request head is parsed, so the in-flight count rises then, not at first poll.
            let in_flight = activity.enter();
            let (sink, telemetry, diag) = (sink.clone(), telemetry.clone(), diag.clone());
            let (path, resource) = (Arc::clone(&path), Arc::clone(&resource));
            let metadata_cache = metadata_cache.clone();
            let activity = Arc::clone(&activity);
            async move {
                let _in_flight = in_flight;
                handle_write(
                    req,
                    &path,
                    resource,
                    metadata_cache.as_deref(),
                    peer,
                    sink,
                    telemetry,
                    diag,
                    &activity,
                    idle_timeout,
                )
                .await
            }
        }
    });
    // Bound to a local: `auto::Connection` borrows its builder, so a temporary would not live long
    // enough to be held across `drive_with_idle`'s loop.
    let builder = auto::Builder::new(TokioExecutor::new());
    let conn = builder.serve_connection(io, svc);
    drive_with_idle(
        conn,
        |conn| conn.graceful_shutdown(),
        &activity,
        idle_timeout,
        grace,
        &telemetry,
    )
    .await
}

/// One request, timed and counted: every exit from [`write_response`], early rejections included,
/// contributes one `logit.input.writes{class}` count (plus `encoding` on `ok`) and one
/// `logit.input.write.duration` timing.
#[allow(clippy::too_many_arguments)]
async fn handle_write(
    req: http::Request<Incoming>,
    path: &str,
    resource: Arc<Resource>,
    metadata_cache: Option<&MetadataCache>,
    peer: SocketAddr,
    sink: Fanout,
    telemetry: Telemetry,
    diag: Diagnostics,
    activity: &Activity,
    stall: Option<Duration>,
) -> Result<http::Response<Full<Bytes>>, std::convert::Infallible> {
    let started = Instant::now();
    let (class, encoding, response) = write_response(
        req,
        path,
        resource,
        metadata_cache,
        peer,
        &sink,
        &telemetry,
        diag,
        activity,
        stall,
    )
    .await;
    match encoding {
        Some(encoding) => {
            let tags = [("class", class), ("encoding", encoding.as_str())];
            telemetry.count("logit.input.writes", 1.0, &tags);
        }
        None => telemetry.count("logit.input.writes", 1.0, &[("class", class)]),
    }
    telemetry.timing("logit.input.write.duration", started.elapsed(), &[]);
    Ok(response)
}

/// The routes table in this module's doc comment, in order, returning the
/// `logit.input.writes{class}` label alongside the response, and on `ok` the encoding the request
/// arrived in.
#[allow(clippy::too_many_arguments)]
async fn write_response(
    req: http::Request<Incoming>,
    path: &str,
    resource: Arc<Resource>,
    metadata_cache: Option<&MetadataCache>,
    peer: SocketAddr,
    sink: &Fanout,
    telemetry: &Telemetry,
    mut diag: Diagnostics,
    activity: &Activity,
    stall: Option<Duration>,
) -> (&'static str, Option<Encoding>, http::Response<Full<Bytes>>) {
    if req.uri().path() != path {
        return ("not_found", None, text_response(None, StatusCode::NOT_FOUND, "not found"));
    }
    // `405 + Allow: POST` where `otlp_in` answers `404`: this matches `prometheus_out`'s
    // exposition server (the module doc's "Routes" section).
    if req.method() != Method::POST {
        let mut response = text_response(
            None,
            StatusCode::METHOD_NOT_ALLOWED,
            "only POST is accepted on a remote-write endpoint",
        );
        response.headers_mut().insert(http::header::ALLOW, HeaderValue::from_static("POST"));
        return ("method", None, response);
    }

    // Both specs mandate Snappy *block* compression on every request, with no identity mode, so a
    // missing header is as unusable as a wrong one. `zstd` is the VictoriaMetrics variant; the
    // `415` for anything else is what vmagent's downgrade to Snappy keys on (the module doc's
    // "Routes").
    let header = header_str(req.headers(), http::header::CONTENT_ENCODING);
    let Some(encoding) = Encoding::from_header(header) else {
        let message = format!(
            "unsupported Content-Encoding {header:?} -- this receiver accepts '{}' (Snappy block \
             format) and '{}'",
            compression::CONTENT_ENCODING_SNAPPY,
            compression::CONTENT_ENCODING_ZSTD
        );
        diag.warn_throttled(
            "write_rejected",
            format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
        );
        return (
            "unsupported",
            None,
            text_response(None, StatusCode::UNSUPPORTED_MEDIA_TYPE, &message),
        );
    };
    // The version comes from this request's own `Content-Type`. An absent header is `""`, which no
    // version claims, so it is a `415` rather than a 1.0 default (the module doc's "Routes").
    let content_type = header_str(req.headers(), CONTENT_TYPE);
    let Some(version) = remote_write::Version::from_content_type(content_type) else {
        let message = format!(
            "unsupported Content-Type {content_type:?} -- this receiver accepts {:?} (1.0) and \
             {:?} (2.0)",
            remote_write::Version::V1.content_type(),
            remote_write::Version::V2.content_type()
        );
        diag.warn_throttled(
            "write_rejected",
            format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
        );
        return (
            "unsupported",
            None,
            text_response(None, StatusCode::UNSUPPORTED_MEDIA_TYPE, &message),
        );
    };

    // The `-Written` helpers take an `Option<Version>` because the rejections above precede
    // knowing it; from here on it is known.
    let seen = Some(version);

    let limited = Limited::new(req.into_body(), MAX_REQUEST_BYTES);
    let compressed = match collect_with_stall_bound(limited, stall).await {
        Ok(bytes) => bytes,
        // A body that stopped arriving is the sender's clock, not its size. `drive_with_idle`
        // applies no deadline while a request is in flight, so without this a half-uploaded
        // request would hold its permit forever; the connection closes once this response is out.
        //
        // Reachable only where `idle_timeout:` is set, since `stall` is derived from it; a
        // default `bind:` has no stall bound (the module doc's "Routes").
        Err(BodyReadError::Stalled(stall)) => {
            activity.request_close();
            let message = format!("request body stalled for {stall:?}");
            diag.warn_throttled(
                "write_rejected",
                format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
            );
            return ("timeout", None, text_response(seen, StatusCode::REQUEST_TIMEOUT, &message));
        }
        Err(BodyReadError::Failed(err)) => {
            let message = body_read_error_message(err.as_ref());
            diag.warn_throttled(
                "write_rejected",
                format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
            );
            return (
                "oversize",
                None,
                text_response(seen, StatusCode::PAYLOAD_TOO_LARGE, &message),
            );
        }
    };

    // Bounded by `MAX_REQUEST_BYTES` without expanding past it, so a compression bomb is
    // rejected, never inflated.
    let body = match compression::decompress_bounded(encoding, &compressed, MAX_REQUEST_BYTES) {
        Ok(body) => body,
        Err(err) => {
            let message = err.to_string();
            diag.warn_throttled(
                "write_rejected",
                format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
            );
            let (class, status) = match err {
                DecompressError::TooLarge { .. } => ("oversize", StatusCode::PAYLOAD_TOO_LARGE),
                DecompressError::Malformed { .. } => ("bad_request", StatusCode::BAD_REQUEST),
            };
            return (class, None, text_response(seen, status, &message));
        }
    };

    // Built per request: requests are served concurrently, so a shared `&mut` decoder would need a
    // lock on the hot path, and nothing is lost, since throttled diagnostics accumulate in the
    // shared `Diagnostics` and telemetry is a clone of one registry handle.
    // `with_timestamp_marker(false)`: every remote-write sample carries a timestamp, so its
    // presence is no producer choice worth recording.
    let mut decoder = PrometheusDecoder::new()
        .with_timestamp_marker(false)
        .with_telemetry(telemetry.clone())
        .with_diagnostics(diag.clone());
    // The cache's lock is taken for the seed and again below to learn, never held across the
    // decode (the module doc's "Metadata cache").
    let seeded = metadata_cache.map(|cache| cache.seed(Instant::now(), telemetry));
    let result = match &seeded {
        Some(seed) => remote_write::decode_with(&body, version, &mut decoder, seed),
        None => remote_write::decode(&body, version, &mut decoder),
    };
    let decoded = match result {
        Ok(decoded) => decoded,
        Err(err) => {
            let message = err.to_string();
            diag.warn_throttled(
                "write_rejected",
                format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
            );
            return ("bad_request", None, text_response(seen, StatusCode::BAD_REQUEST, &message));
        }
    };
    // Learned from what *this* request declared, never from the seed: a cache that refreshed
    // entries from its own memory would never let one expire. After the `400` above, so a
    // malformed request teaches nothing.
    if let Some(cache) = metadata_cache {
        cache.learn(&decoded.declarations, Instant::now(), telemetry);
    }

    // One batch per request, on an empty `Resource`, concatenating the timestamp groups in
    // ascending order (the module doc's "Timestamps and timestamp groups").
    //
    // The kept count is built here, not read off `Decoded::samples`, which is what the
    // *assembler* accepted: the model mapping below can still drop a series with no model kind
    // (an empty histogram, decreasing cumulative bucket counts). `-Written` reports what was
    // kept (the module doc's "The `-Written` headers").
    let received_at = now_nanos();
    let mut total: u64 = 0;
    let mut dropped: u64 = 0;
    let mut events = Vec::new();
    for group in &decoded.groups {
        for family in group {
            for series in &family.series {
                total += remote_write::wire_samples(family.kind, series, version);
            }
        }
        events.extend(families_to_events_with(
            group,
            received_at,
            &mut decoder,
            &mut |family: &MetricFamily, series: &Series| {
                dropped += remote_write::wire_samples(family.kind, series, version);
            },
        ));
    }
    // Both sides walk the same `decoded.groups`, so this cannot underflow; `saturating_sub` only
    // so a bug there is not a panic.
    let written = total.saturating_sub(dropped);
    telemetry.count("logit.input.samples", written as f64, &[]);
    if !events.is_empty() {
        // **Before** the response is built, as in `otlp_in`: channel backpressure delays the
        // `204` and the sender's queue throttles, remote-write's own flow-control model.
        sink.send_reserved(EventBatch { resource, scope: None, events }).await;
    }
    ("ok", Some(encoding), no_content(seen, written, decoded.exemplars))
}

/// `""` for an absent or non-ASCII header: either way it says nothing usable, and the caller's
/// message quotes what came back.
fn header_str(headers: &HeaderMap, name: http::header::HeaderName) -> &str {
    headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("")
}

/// `204 No Content`, plus 2.0's `-Written` report of what this receiver stored: `samples` is the
/// wire samples of the series that reached the fanout, never the assembler's accepted total.
/// Native histograms are skipped, so the histogram count is always `0`.
fn no_content(
    version: Option<remote_write::Version>,
    samples: u64,
    exemplars: u64,
) -> http::Response<Full<Bytes>> {
    let mut builder = http::Response::builder().status(StatusCode::NO_CONTENT);
    builder = with_written_headers(builder, version, samples, exemplars);
    builder.body(Full::new(Bytes::new())).expect("a well-formed response always builds")
}

fn text_response(
    version: Option<remote_write::Version>,
    status: StatusCode,
    message: &str,
) -> http::Response<Full<Bytes>> {
    let mut builder =
        http::Response::builder().status(status).header(CONTENT_TYPE, "text/plain; charset=utf-8");
    builder = with_written_headers(builder, version, 0, 0);
    builder
        .body(Full::new(Bytes::copy_from_slice(message.as_bytes())))
        .expect("a well-formed response always builds")
}

/// 2.0 requires the three `-Written` headers on `4xx` as well as `2xx`, so a sender can tell a
/// partially-applied write from one that stored nothing; a rejection stored nothing, hence the
/// zeros. A 1.0 request, or one rejected before its version was known, gets none.
fn with_written_headers(
    builder: http::response::Builder,
    version: Option<remote_write::Version>,
    samples: u64,
    exemplars: u64,
) -> http::response::Builder {
    if version != Some(remote_write::Version::V2) {
        return builder;
    }
    builder
        .header(remote_write::HEADER_SAMPLES_WRITTEN, samples.to_string())
        .header(remote_write::HEADER_HISTOGRAMS_WRITTEN, "0")
        .header(remote_write::HEADER_EXEMPLARS_WRITTEN, exemplars.to_string())
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

    /// What a canned server does with each request. `Clone` so one value backs every connection
    /// the server accepts.
    #[derive(Clone)]
    enum CannedResponse {
        Body {
            status: u16,
            content_type: Option<&'static str>,
            body: Bytes,
        },
        /// Reads the request but never responds, driving this input's per-request timeout.
        Hang,
    }

    /// A real `hyper` HTTP/1.1 server on an ephemeral port, answering every request with
    /// `response`. Returns the bound address and each request's captured headers.
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

    /// Reads one counter out of an already-drained event list. `Registry::drain` empties its
    /// buffers, so a test asserting on several counters drains once and reads them all from that
    /// snapshot.
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

    /// [`counter_in`] for a gauge, which that helper does not match (a counter read as a gauge
    /// would hide a spelling bug).
    fn gauge_in(events: &[Event], metric: &str, tag: (&str, &str)) -> Option<f64> {
        events.iter().find_map(|e| {
            if e.attributes.get(tag.0).and_then(|v| v.as_str()) != Some(tag.1) {
                return None;
            }
            e.metrics.iter().find_map(|m| {
                if logit_core::interner::resolve(m.name) != metric {
                    return None;
                }
                match m.kind {
                    MetricKind::Gauge(value) => Some(value),
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

    // ---- TLS: a canned `tokio-rustls`-wrapped scrape target ----

    fn testdata_dir() -> std::path::PathBuf {
        // The repo root's `testdata/tls` (`testdata/tls/README.md`), two levels up.
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    /// A `rustls::ServerConfig` presenting `testdata/tls/server.{pem,key}`, requiring no client
    /// certificate.
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

    /// A TLS-wrapped `canned_server` replying with a fixed text-0.0.4 body.
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

    /// `ca_file` is honored: a CA that doesn't sign the server's leaf (`other-ca.pem`,
    /// `testdata/tls/README.md`) fails the handshake. This does **not** exercise
    /// `tls_built_in_root_certs(false)`: no bundled root chains to the private test CA either, so
    /// the handshake fails regardless, and a discriminating leaf isn't reproducible offline. That
    /// `ca_file` replaces rather than joins the bundled set is taken on trust from `reqwest`'s
    /// `ClientBuilder::tls_built_in_root_certs` doc.
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
        // `up == 0` alone can't tell a handshake failure from a timeout or a 5xx.
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

    /// `prometheus.target` never carries a scrape URL's userinfo or query string, while the request
    /// itself still authenticates with them.
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

        // `reqwest` turns the URL's userinfo into an `Authorization` header, which redaction must
        // not break.
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
        // Two malformed targets must not share a placeholder; the end-to-end version is
        // `an_unparseable_targets_placeholder_is_keyed_by_index`.
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

    /// Two targets that pass rule 40 but fail `reqwest::Url::parse` (`redact_url`'s doc lists the
    /// shapes) get distinct `instance`/`prometheus.target` values, so their `up=0` series don't
    /// collapse downstream.
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

    // ---- bind mode: the remote-write receiver -------------------------------------------------
    //
    // One row of the routes table per test, both wire versions, over a real socket with
    // hand-written HTTP/1.1 (`otlp_in`'s `post_raw` shape), which lets a test hold a keep-alive
    // connection open across requests, as the idle-timeout and backpressure cases need.

    use logit_proto::prometheus::{
        FamilyType, MetricFamily, Point, PrometheusEncoder, Series, ATTR_TIMESTAMP, ATTR_TYPE,
    };

    /// A bound receiver plus its address. `Input::bind` makes the port live before `run` is
    /// spawned, so there is no bind-drop-rebind race.
    async fn bound_receiver(path: &str) -> (PrometheusReceiver, String) {
        let mut receiver = PrometheusReceiver::new("127.0.0.1:0", path);
        receiver.bind().await.expect("binding an ephemeral port should succeed");
        let addr = receiver.local_addr().expect("bind() leaves a real address behind").to_string();
        (receiver, addr)
    }

    /// Spawns `receiver`'s accept loop and returns the `Fanout` receiving end. `capacity` `1` with
    /// nothing draining it parks a handler inside `Fanout::send`.
    fn spawn_receiver(
        mut receiver: PrometheusReceiver,
        capacity: usize,
    ) -> mpsc::Receiver<Delivered> {
        let (tx, rx) = mpsc::channel(capacity);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move { receiver.run(sink).await });
        rx
    }

    /// Through `snap` directly rather than the receiver's own `compression` module, as an
    /// independent sender would.
    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).expect("compressing a test body never fails")
    }

    /// One remote-write request body: protobuf through the codec, then Snappy **block**
    /// compression, as a real sender puts it on the wire.
    fn request_body(groups: &[Vec<MetricFamily>], version: remote_write::Version) -> Vec<u8> {
        let mut encoder = PrometheusEncoder::new();
        snappy(&remote_write::encode(groups, version, &mut encoder))
    }

    fn gauge_family(name: &str, label: (&str, &str), value: f64, timestamp: i64) -> MetricFamily {
        let mut family = MetricFamily::new(name, FamilyType::Gauge);
        family.series.push(Series {
            labels: vec![(label.0.to_string(), label.1.to_string())],
            point: Point::Gauge(value),
            timestamp: Some(timestamp),
            created: None,
            exemplars: Vec::new(),
        });
        family
    }

    /// Unix nanoseconds on a whole-millisecond boundary: remote-write timestamps are milliseconds,
    /// so anything finer would come back truncated.
    fn millis(ms: i64) -> i64 {
        ms * 1_000_000
    }

    /// The protocol headers a well-formed request carries, for [`post_raw`]. `Connection: close`
    /// so `read_to_end` ends with the response; [`keep_alive_write_headers`] omits it.
    fn write_headers(version: remote_write::Version) -> String {
        format!("{}Connection: close\r\n", keep_alive_write_headers(version))
    }

    /// [`write_headers`] without `Connection: close`, so HTTP/1.1's keep-alive default applies and
    /// only the *listener* can end the connection.
    fn keep_alive_write_headers(version: remote_write::Version) -> String {
        format!(
            "Content-Type: {}\r\nContent-Encoding: {}\r\n{}: {}\r\nUser-Agent: test\r\n",
            version.content_type(),
            compression::CONTENT_ENCODING_SNAPPY,
            remote_write::HEADER_VERSION,
            version.header_version()
        )
    }

    async fn post_raw(addr: &str, path: &str, headers: &str, body: &[u8]) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{headers}\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf)).await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    /// `post_raw` for a well-formed request of `version`.
    async fn post_write(
        addr: &str,
        path: &str,
        version: remote_write::Version,
        body: &[u8],
    ) -> String {
        post_raw(addr, path, &write_headers(version), body).await
    }

    /// Reads one response head (through the blank line) off a keep-alive connection, where
    /// `read_to_end` would block until the connection ends. `None` on the deadline, so the
    /// backpressure test can assert that no response has arrived *yet*.
    async fn read_head<S: tokio::io::AsyncRead + Unpin>(
        stream: &mut S,
        within: Duration,
    ) -> Option<String> {
        use tokio::io::AsyncReadExt;
        let deadline = tokio::time::Instant::now() + within;
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            let read = tokio::time::timeout_at(deadline, stream.read(&mut byte)).await.ok()?;
            match read {
                Ok(0) | Err(_) => return None,
                Ok(n) => head.extend_from_slice(&byte[..n]),
            }
        }
        Some(String::from_utf8_lossy(&head).into_owned())
    }

    async fn expect_closed<S: tokio::io::AsyncRead + Unpin>(stream: &mut S, what: &str) {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 1];
        let result = tokio::time::timeout(Duration::from_secs(2), stream.read(&mut buf))
            .await
            .unwrap_or_else(|_| panic!("{what}: expected a close within 2s"));
        match result {
            Ok(n) => assert_eq!(n, 0, "{what}: expected a close, got a byte"),
            // A close with bytes still unread in the peer's receive queue is an RST, not a FIN.
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(err) => panic!("{what}: read failed outright: {err}"),
        }
    }

    fn gauge_value_of(batch: &EventBatch, name: &str) -> Option<f64> {
        batch.events.iter().find_map(|e| {
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
    }

    #[tokio::test]
    async fn bind_makes_the_port_live_before_run_and_a_second_bind_is_a_no_op() {
        let mut receiver = PrometheusReceiver::new("127.0.0.1:0", "/api/v1/write");
        assert_eq!(receiver.local_addr(), None, "no address before bind()");
        receiver.bind().await.expect("binding an ephemeral port should succeed");
        let addr = receiver.local_addr().expect("bind() should leave a real address behind");

        // Connects with `run` never having been spawned -- the socket is live from `bind` alone.
        tokio::net::TcpStream::connect(addr)
            .await
            .expect("the port should already be accepting connections after bind() alone");

        receiver.bind().await.expect("a second bind() is idempotent, per Input::bind's contract");
        assert_eq!(receiver.local_addr(), Some(addr), "and must not have rebound to a new port");
    }

    #[tokio::test]
    async fn a_1_0_request_answers_204_and_reaches_the_fanout() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 7.0, millis(1_700_000_000_000))]],
            remote_write::Version::V1,
        );

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;

        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        // 1.0 defines no `-Written` headers, so none are sent.
        assert!(
            !response.to_ascii_lowercase().contains(remote_write::HEADER_SAMPLES_WRITTEN),
            "got: {response}"
        );
        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(gauge_value_of(&batch, "queue_depth"), Some(7.0));
    }

    #[tokio::test]
    async fn a_2_0_request_answers_204_with_the_written_headers() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 7.0, millis(1_700_000_000_000))]],
            remote_write::Version::V2,
        );

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V2, &body).await;

        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        let lowered = response.to_ascii_lowercase();
        assert!(
            lowered.contains(&format!("{}: 1", remote_write::HEADER_SAMPLES_WRITTEN)),
            "got: {response}"
        );
        // Always 0: native histograms are skipped and counted, never stored.
        assert!(
            lowered.contains(&format!("{}: 0", remote_write::HEADER_HISTOGRAMS_WRITTEN)),
            "got: {response}"
        );
        assert!(
            lowered.contains(&format!("{}: 0", remote_write::HEADER_EXEMPLARS_WRITTEN)),
            "got: {response}"
        );
        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(gauge_value_of(&batch, "queue_depth"), Some(7.0));
    }

    #[tokio::test]
    async fn a_post_to_another_path_is_404() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 7.0, millis(1))]],
            remote_write::Version::V1,
        );

        let response = post_write(&addr, "/write", remote_write::Version::V1, &body).await;

        assert!(response.starts_with("HTTP/1.1 404"), "got: {response}");
    }

    /// A wrong method is `405` + `Allow: POST`, matching `prometheus_out`, not `otlp_in`'s `404`.
    #[tokio::test]
    async fn a_non_post_on_the_write_path_is_405_with_an_allow_header() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);

        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let request =
            format!("GET /api/v1/write HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf)).await;
        let response = String::from_utf8_lossy(&buf).into_owned();

        assert!(response.starts_with("HTTP/1.1 405"), "got: {response}");
        assert!(response.to_ascii_lowercase().contains("allow: post"), "got: {response}");
    }

    #[tokio::test]
    async fn a_request_without_snappy_content_encoding_is_415() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        let headers = format!(
            "Content-Type: {}\r\nContent-Encoding: gzip\r\nConnection: close\r\n",
            remote_write::Version::V1.content_type()
        );

        let response = post_raw(&addr, "/api/v1/write", &headers, b"whatever").await;

        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
        assert!(response.contains("gzip"), "the message names what arrived, got: {response}");
        assert!(
            response.contains("'snappy'") && response.contains("'zstd'"),
            "the message names both accepted encodings, got: {response}"
        );
    }

    // -- zstd, the VictoriaMetrics remote write protocol --------------------------------------

    /// [`write_headers`] with `Content-Encoding: zstd`, as vmagent sends by default.
    fn zstd_write_headers() -> String {
        format!(
            "Content-Type: {}\r\nContent-Encoding: {}\r\n{}: {}\r\nConnection: close\r\n",
            remote_write::Version::V1.content_type(),
            compression::CONTENT_ENCODING_ZSTD,
            remote_write::HEADER_VERSION,
            remote_write::Version::V1.header_version()
        )
    }

    /// A zstd frame header with no content checksum: a 4-byte content size when `content_size`
    /// is set, else none, and `window` as the window-descriptor byte (`0x50` is 1 MiB).
    fn zstd_frame_header(content_size: Option<u32>, window: u8) -> Vec<u8> {
        let mut header = vec![0x28, 0xb5, 0x2f, 0xfd];
        match content_size {
            Some(size) => {
                header.extend([0x80, window]);
                header.extend(size.to_le_bytes());
            }
            None => header.extend([0x00, window]),
        }
        header
    }

    #[tokio::test]
    async fn a_zstd_request_is_decoded_and_counted_by_encoding() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver.with_telemetry(telemetry);
        let mut rx = spawn_receiver(receiver, 4);
        let mut encoder = PrometheusEncoder::new();
        let protobuf = remote_write::encode(
            &[vec![gauge_family("queue_depth", ("job", "api"), 7.0, millis(1))]],
            remote_write::Version::V1,
            &mut encoder,
        );
        let body = compression::compress(Encoding::Zstd, &protobuf).unwrap();

        let response = post_raw(&addr, "/api/v1/write", &zstd_write_headers(), &body).await;

        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(batch.events.len(), 1);
        assert_eq!(logit_core::interner::resolve(batch.events[0].metrics[0].name), "queue_depth");
        let events = registry.drain(0);
        let ok_zstd = events.iter().any(|e| {
            e.attributes.get("class").and_then(|v| v.as_str()) == Some("ok")
                && e.attributes.get("encoding").and_then(|v| v.as_str()) == Some("zstd")
                && e.metrics
                    .iter()
                    .any(|m| logit_core::interner::resolve(m.name) == "logit.input.writes")
        });
        assert!(ok_zstd, "logit.input.writes{{class=ok,encoding=zstd}} should be counted");
    }

    /// Guard 1: a frame declaring more than the cap is refused on its header alone. The frame
    /// has no blocks at all, so a `413` rather than a `400` proves nothing was decoded.
    #[tokio::test]
    async fn a_zstd_body_declaring_more_than_the_cap_is_413_before_decoding() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        let declared = MAX_REQUEST_BYTES as u32 + 1;
        let header_only = zstd_frame_header(Some(declared), 0x50);

        let response = post_raw(&addr, "/api/v1/write", &zstd_write_headers(), &header_only).await;

        assert!(response.starts_with("HTTP/1.1 413"), "got: {response}");
        assert!(response.contains(&format!("would be {declared} bytes")), "got: {response}");
    }

    /// Guard 3: no declared size and a 1 MiB window, then 33 RLE blocks of 128 KiB each: 132
    /// bytes on the wire that inflate past 4 MiB.
    #[tokio::test]
    async fn an_undeclared_zstd_body_that_inflates_past_the_cap_is_413() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        let block_size: u32 = 128 * 1024;
        let blocks = MAX_REQUEST_BYTES / block_size as usize + 1;
        let mut bomb = zstd_frame_header(None, 0x50);
        for i in 0..blocks {
            // An RLE block: type 1, `block_size` copies of the one byte that follows.
            let header = (block_size << 3) | (1 << 1) | u32::from(i + 1 == blocks);
            bomb.extend(&header.to_le_bytes()[..3]);
            bomb.push(0);
        }
        assert!(bomb.len() < 256, "the bomb is {} bytes", bomb.len());

        let response = post_raw(&addr, "/api/v1/write", &zstd_write_headers(), &bomb).await;

        assert!(response.starts_with("HTTP/1.1 413"), "got: {response}");
    }

    #[tokio::test]
    async fn a_corrupt_zstd_body_is_400() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);

        let response =
            post_raw(&addr, "/api/v1/write", &zstd_write_headers(), b"not zstd at all").await;

        assert!(response.starts_with("HTTP/1.1 400"), "got: {response}");
        assert!(response.contains("invalid zstd body"), "got: {response}");
    }

    /// A Snappy body labelled `zstd` is a `400`, one of the two statuses vmagent downgrades on.
    #[tokio::test]
    async fn a_snappy_body_labelled_zstd_is_400() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 7.0, millis(1))]],
            remote_write::Version::V1,
        );

        let response = post_raw(&addr, "/api/v1/write", &zstd_write_headers(), &body).await;

        assert!(response.starts_with("HTTP/1.1 400"), "got: {response}");
    }

    /// Both specs mandate Snappy, so there is no identity fallback for a missing header to mean.
    #[tokio::test]
    async fn a_request_with_no_content_encoding_at_all_is_415() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        let headers = format!(
            "Content-Type: {}\r\nConnection: close\r\n",
            remote_write::Version::V1.content_type()
        );

        let response = post_raw(&addr, "/api/v1/write", &headers, b"whatever").await;

        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
    }

    #[tokio::test]
    async fn a_request_with_an_unrecognised_content_type_is_415() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        let headers = format!(
            "Content-Type: application/x-protobuf;proto=some.other.Message\r\n\
             Content-Encoding: {}\r\nConnection: close\r\n",
            compression::CONTENT_ENCODING_SNAPPY
        );

        let response = post_raw(&addr, "/api/v1/write", &headers, &snappy(b"")).await;

        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
        assert!(response.contains("some.other.Message"), "got: {response}");
    }

    /// An absent `Content-Type` is a `415`, not a guessed version (unlike `otlp_in`'s protobuf
    /// default).
    #[tokio::test]
    async fn a_request_with_no_content_type_is_415() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        let headers = format!(
            "Content-Encoding: {}\r\nConnection: close\r\n",
            compression::CONTENT_ENCODING_SNAPPY
        );

        let response = post_raw(&addr, "/api/v1/write", &headers, &snappy(b"")).await;

        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
    }

    /// The compression-bomb bound: Snappy's declared decompressed size is checked against
    /// `MAX_REQUEST_BYTES` *before* a byte is expanded.
    #[tokio::test]
    async fn a_body_that_would_decompress_over_the_cap_is_413() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        // Highly compressible, so only the declared decompressed length can catch it.
        let bomb = snappy(&vec![0u8; MAX_REQUEST_BYTES + 1024]);
        assert!(
            bomb.len() < MAX_REQUEST_BYTES,
            "the compressed body must itself fit under the cap"
        );

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V1, &bomb).await;

        assert!(response.starts_with("HTTP/1.1 413"), "got: {response}");
    }

    #[tokio::test]
    async fn a_malformed_snappy_body_is_400() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);

        let response =
            post_write(&addr, "/api/v1/write", remote_write::Version::V1, b"not snappy at all")
                .await;

        assert!(response.starts_with("HTTP/1.1 400"), "got: {response}");
    }

    /// A body that decompresses but is not protobuf (a truncated varint) is a `400`. Sent as 2.0,
    /// so this also pins the zero `-Written` report 2.0 requires on a `4xx`.
    ///
    /// A *valid 1.0* body under a 2.0 `Content-Type` is also a `CodecError`
    /// (`logit_proto::prometheus::remote_write`'s module doc) -- see
    /// `a_valid_body_of_the_other_version_is_400_and_counted_bad_request`.
    #[tokio::test]
    async fn a_body_that_is_not_the_promised_message_is_400() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        // `0x08` is field 1, varint -- with no varint after it.
        let truncated = snappy(b"\x08");

        let response =
            post_write(&addr, "/api/v1/write", remote_write::Version::V2, &truncated).await;

        assert!(response.starts_with("HTTP/1.1 400"), "got: {response}");
        // 2.0 wants the `-Written` report on a 4xx too: zeros, since nothing was stored.
        assert!(
            response
                .to_ascii_lowercase()
                .contains(&format!("{}: 0", remote_write::HEADER_SAMPLES_WRITTEN)),
            "got: {response}"
        );
    }

    /// A *valid* body of one version, posted under the other version's `Content-Type`, is also a
    /// `400`: `logit_proto::prometheus::remote_write::decode_v1`/`decode_v2` refuse a non-empty
    /// body that decodes to an empty message, since 1.0 and 2.0 field numbers don't overlap.
    #[tokio::test]
    async fn a_valid_body_of_the_other_version_is_400_and_counted_bad_request() {
        for (sent, claimed) in [
            (remote_write::Version::V1, remote_write::Version::V2),
            (remote_write::Version::V2, remote_write::Version::V1),
        ] {
            let (receiver, addr) = bound_receiver("/api/v1/write").await;
            let registry = Registry::new();
            let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
            let receiver = receiver.with_telemetry(telemetry);
            let mut rx = spawn_receiver(receiver, 4);
            let groups = vec![vec![gauge_family(
                "queue_depth",
                ("job", "api"),
                7.0,
                millis(1_700_000_000_000),
            )]];
            let body = request_body(&groups, sent);

            let response = post_write(&addr, "/api/v1/write", claimed, &body).await;

            assert!(
                response.starts_with("HTTP/1.1 400"),
                "{sent:?} as {claimed:?} got: {response}"
            );
            if claimed == remote_write::Version::V2 {
                // 2.0 wants the `-Written` report on a 4xx too: zeros, since nothing was stored.
                assert!(
                    response
                        .to_ascii_lowercase()
                        .contains(&format!("{}: 0", remote_write::HEADER_SAMPLES_WRITTEN)),
                    "got: {response}"
                );
            }
            assert!(
                rx.try_recv().is_err(),
                "{sent:?} as {claimed:?}: nothing should reach the fanout"
            );

            let events = registry.drain(0);
            assert_eq!(
                counter_in(&events, "logit.input.writes", ("class", "bad_request")),
                Some(1.0),
                "{sent:?} as {claimed:?}"
            );
        }
    }

    /// Three samples of one series become **one** batch of three events in ascending timestamp
    /// order, none carrying the `prometheus.timestamp` marker.
    #[tokio::test]
    async fn a_multi_timestamp_request_becomes_one_ordered_batch_with_no_timestamp_marker() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let mut rx = spawn_receiver(receiver, 4);
        let groups: Vec<Vec<MetricFamily>> =
            [1_700_000_000_000i64, 1_700_000_015_000, 1_700_000_030_000]
                .into_iter()
                .enumerate()
                .map(|(i, ms)| {
                    vec![gauge_family("queue_depth", ("job", "api"), i as f64, millis(ms))]
                })
                .collect();
        let body = request_body(&groups, remote_write::Version::V1);

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");

        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(batch.events.len(), 3, "one batch, one event per sample");
        let timestamps: Vec<i64> = batch.events.iter().map(|e| e.timestamp).collect();
        assert_eq!(
            timestamps,
            vec![millis(1_700_000_000_000), millis(1_700_000_015_000), millis(1_700_000_030_000)],
            "ascending, straight from the wire"
        );
        for event in &batch.events {
            assert!(
                event.attributes.get(ATTR_TIMESTAMP).is_none(),
                "no timestamp marker on a transport that mandates timestamps"
            );
        }
        assert!(rx.try_recv().is_err(), "three timestamps are still one request, so one batch");
    }

    /// Labels stay labels: `job`/`instance` ride as ordinary event attributes and the batch's
    /// `Resource` is empty.
    #[tokio::test]
    async fn the_batch_carries_an_empty_resource_and_labels_stay_labels() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("instance", "node-1:9100"), 3.0, millis(1))]],
            remote_write::Version::V1,
        );

        post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;

        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(*batch.resource, Resource::default(), "no resource identity is invented");
        let event = &batch.events[0];
        assert_eq!(
            event.attributes.get("instance").and_then(|v| v.as_str()),
            Some("node-1:9100"),
            "instance stays an ordinary label"
        );
    }

    /// With **no** metadata cache, a 1.0 request carrying only samples (Prometheus ships
    /// `MetricMetadata` separately) decodes as `Unknown` families with exact samples. The cache
    /// tests below are this test with a table behind it.
    #[tokio::test]
    async fn a_1_0_request_without_metadata_decodes_as_unknown_families() {
        use logit_proto::prometheus::generated::prometheus as pb1;
        use prost::Message;

        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let mut rx = spawn_receiver(receiver, 4);
        // Hand-built: `remote_write::encode` always writes a `metadata[]` entry, and the shape
        // under test carries none. Labels sorted by byte order, as both specs require.
        let request = pb1::WriteRequest {
            timeseries: vec![pb1::TimeSeries {
                labels: vec![
                    pb1::Label { name: "__name__".to_string(), value: "queue_depth".to_string() },
                    pb1::Label { name: "job".to_string(), value: "api".to_string() },
                ],
                samples: vec![pb1::Sample { value: 11.0, timestamp: 1_700_000_000_000 }],
                exemplars: Vec::new(),
                histograms: Vec::new(),
            }],
            metadata: Vec::new(),
        };
        let body = snappy(&request.encode_to_vec());

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");

        let batch = recv_batch_async(&mut rx).await;
        let event = &batch.events[0];
        assert_eq!(gauge_value_of(&batch, "queue_depth"), Some(11.0), "the sample is exact");
        assert_eq!(
            event.attributes.get(ATTR_TYPE).and_then(|v| v.as_str()),
            Some("unknown"),
            "with no metadata in the request, the family is untyped"
        );
    }

    /// An empty request is a `204` and nothing downstream, not an empty batch.
    #[tokio::test]
    async fn an_empty_request_answers_204_and_sends_no_batch() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(&[], remote_write::Version::V1);

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;

        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        assert!(rx.try_recv().is_err(), "no batch for a request that decoded to nothing");
    }

    /// The batch reaches the `Fanout` **before** the response is built, so a full downstream
    /// delays the `204` and the sender's own queue throttles.
    #[tokio::test]
    async fn backpressure_delays_the_204_until_the_channel_drains() {
        use tokio::io::AsyncWriteExt;
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        // Capacity 1, nothing draining: the first batch fills it and the second parks.
        let mut rx = spawn_receiver(receiver, 1);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 1.0, millis(1))]],
            remote_write::Version::V1,
        );

        let first = post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;
        assert!(first.starts_with("HTTP/1.1 204"), "got: {first}");

        let mut blocked = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let request = format!(
            "POST /api/v1/write HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{}\r\n",
            body.len(),
            write_headers(remote_write::Version::V1)
        );
        blocked.write_all(request.as_bytes()).await.unwrap();
        blocked.write_all(&body).await.unwrap();

        assert!(
            read_head(&mut blocked, Duration::from_millis(300)).await.is_none(),
            "the 204 must not be written while the batch is still parked in Fanout::send"
        );

        recv_batch_async(&mut rx).await; // drain the first batch; the parked send completes
        let head = read_head(&mut blocked, Duration::from_secs(5))
            .await
            .expect("the 204 arrives once the downstream drains");
        assert!(head.starts_with("HTTP/1.1 204"), "got: {head}");
        recv_batch_async(&mut rx).await;
    }

    /// A histogram whose cumulative bucket counts *decrease* is dropped by `families_to_events`
    /// after the assembler accepted its samples, so `Samples-Written` is `0` and
    /// `logit.input.samples` counts nothing: the report follows the events, not the assembler.
    #[tokio::test]
    async fn a_request_whose_only_series_is_dropped_reports_zero_samples_written() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver.with_telemetry(telemetry);
        let mut rx = spawn_receiver(receiver, 4);

        let mut family = MetricFamily::new("request_seconds", FamilyType::Histogram);
        family.series.push(Series {
            labels: vec![("job".to_string(), "api".to_string())],
            // Cumulative counts must never decrease; `5` after `9` makes this series unmappable.
            point: Point::Histogram {
                buckets: vec![(0.5, 9), (f64::INFINITY, 5)],
                sum: None,
                count: 5,
            },
            timestamp: Some(millis(1_700_000_000_000)),
            created: None,
            exemplars: Vec::new(),
        });
        let body = request_body(&[vec![family]], remote_write::Version::V2);

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V2, &body).await;

        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        assert!(
            response
                .to_ascii_lowercase()
                .contains(&format!("{}: 0", remote_write::HEADER_SAMPLES_WRITTEN)),
            "the header reports what was kept, which is nothing -- got: {response}"
        );
        assert!(rx.try_recv().is_err(), "a dropped series leaves no events, so no batch is sent");

        let events = registry.drain(0);
        assert_eq!(counter_in(&events, "logit.input.writes", ("class", "ok")), Some(1.0));
        assert_eq!(
            counter_in(&events, "logit.input.samples", ("component", "receive")),
            Some(0.0),
            "and the counter agrees with the header"
        );
        // The decoder's skip counter is where the loss is visible.
        assert_eq!(
            counter_in(&events, "logit.input.metrics.skipped", ("reason", "non_monotonic_buckets")),
            Some(1.0)
        );
    }

    /// The other half: one classic-histogram series spelled as several wire samples reports all
    /// of them, so the count is not "count events".
    #[tokio::test]
    async fn a_kept_histogram_reports_every_wire_sample_it_was_spelled_as() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let mut rx = spawn_receiver(receiver, 4);

        let mut family = MetricFamily::new("request_seconds", FamilyType::Histogram);
        family.series.push(Series {
            labels: vec![("job".to_string(), "api".to_string())],
            // Two `_bucket` samples, a `_sum` and a `_count`: four on the wire, one event here.
            point: Point::Histogram {
                buckets: vec![(0.5, 3), (f64::INFINITY, 7)],
                sum: Some(1.25),
                count: 7,
            },
            timestamp: Some(millis(1_700_000_000_000)),
            created: None,
            exemplars: Vec::new(),
        });
        let body = request_body(&[vec![family]], remote_write::Version::V2);

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V2, &body).await;

        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        assert!(
            response
                .to_ascii_lowercase()
                .contains(&format!("{}: 4", remote_write::HEADER_SAMPLES_WRITTEN)),
            "two buckets plus _sum plus _count -- got: {response}"
        );
        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(batch.events.len(), 1, "four wire samples, one model series");
    }

    /// A summary sent as quantiles alone (no `_sum`, no `_count`) reports two samples. A count from
    /// the built `MetricRecord` would say four: `logit_core::Summary`'s `sum`/`count` are plain
    /// numbers, while the codec's `Point::Summary` keeps the `Option`s `wire_samples` reads.
    #[tokio::test]
    async fn a_quantiles_only_summary_reports_only_its_quantiles() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver.with_telemetry(telemetry);
        let mut rx = spawn_receiver(receiver, 4);

        let mut family = MetricFamily::new("request_seconds", FamilyType::Summary);
        family.series.push(Series {
            labels: vec![("job".to_string(), "api".to_string())],
            point: Point::Summary {
                quantiles: vec![(0.5, 1.0), (0.9, 2.0)],
                sum: None,
                count: None,
            },
            timestamp: Some(millis(1_700_000_000_000)),
            created: None,
            exemplars: Vec::new(),
        });
        let body = request_body(&[vec![family]], remote_write::Version::V2);

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V2, &body).await;

        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        assert!(
            response
                .to_ascii_lowercase()
                .contains(&format!("{}: 2", remote_write::HEADER_SAMPLES_WRITTEN)),
            "two quantiles and nothing else -- got: {response}"
        );
        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(batch.events.len(), 1);
        let events = registry.drain(0);
        assert_eq!(counter_in(&events, "logit.input.samples", ("component", "receive")), Some(2.0));
    }

    /// 1.0 spells a created timestamp as a `_created` sample of its own, which this receiver keeps
    /// (as `Series::created`), so both samples are reported. Hand-built: `remote_write::encode`
    /// drops `created` on 1.0, which has no field for it.
    #[tokio::test]
    async fn a_1_0_created_sample_is_reported_as_a_sample_it_kept() {
        use logit_proto::prometheus::generated::prometheus as pb1;
        use prost::Message;

        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver.with_telemetry(telemetry);
        let mut rx = spawn_receiver(receiver, 4);

        let series = |name: &str, value: f64| pb1::TimeSeries {
            labels: vec![pb1::Label { name: "__name__".to_string(), value: name.to_string() }],
            samples: vec![pb1::Sample { value, timestamp: 1_700_000_000_000 }],
            exemplars: Vec::new(),
            histograms: Vec::new(),
        };
        let request = pb1::WriteRequest {
            timeseries: vec![
                series("requests_total", 7.0),
                series("requests_created", 1_699_000_000.0),
            ],
            // A `_created` sample means something only once the family is declared a counter;
            // without metadata `requests_created` is another untyped series.
            metadata: vec![pb1::MetricMetadata {
                r#type: pb1::metric_metadata::MetricType::Counter as i32,
                metric_family_name: "requests".to_string(),
                help: String::new(),
                unit: String::new(),
            }],
        };
        let body = snappy(&request.encode_to_vec());

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");

        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(batch.events.len(), 1, "one series, whatever it was spelled as");
        assert_eq!(
            batch.events[0].metrics[0].start_timestamp, 1_699_000_000_000_000_000,
            "the `_created` sample became the record's start timestamp"
        );
        let events = registry.drain(0);
        assert_eq!(
            counter_in(&events, "logit.input.samples", ("component", "receive")),
            Some(2.0),
            "the value sample and the `_created` sample were both kept"
        );
    }

    /// The routes table's `408` row: `drive_with_idle` applies no deadline while a request is in
    /// flight, so a peer that stops mid-body would otherwise hold its permit forever.
    #[tokio::test]
    async fn a_body_that_stops_arriving_is_408_and_closes_the_connection() {
        use tokio::io::AsyncWriteExt;
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver
            .with_telemetry(telemetry)
            // The per-frame body bound is derived from `idle_timeout`.
            .with_idle_timeout(Some(Duration::from_millis(100)))
            // The grace `drive_with_idle` gives hyper to write the 408 out and close.
            .with_handshake_timeout(Duration::from_millis(200));
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 1.0, millis(1))]],
            remote_write::Version::V1,
        );

        // A `Content-Length` promising the whole body, then half of it and silence. Sent
        // **keep-alive**, so only the listener's `Activity::request_close` can end the connection.
        let mut stalled = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let head = format!(
            "POST /api/v1/write HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{}\r\n",
            body.len(),
            keep_alive_write_headers(remote_write::Version::V1)
        );
        stalled.write_all(head.as_bytes()).await.unwrap();
        stalled.write_all(&body[..body.len() / 2]).await.unwrap();

        // `read_to_end` completing is the close: it reads the response and proves the socket
        // went away.
        let mut buf = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            tokio::time::timeout(Duration::from_secs(5), stalled.read_to_end(&mut buf))
                .await
                .expect("a stalled request body should be answered and closed within 5s")
                .expect("reading the response should not fail outright");
        }
        let response = String::from_utf8_lossy(&buf);
        assert!(response.starts_with("HTTP/1.1 408"), "got: {response}");
        assert!(response.contains("stalled"), "the message should say what happened: {response}");

        assert!(rx.try_recv().is_err(), "a half-uploaded request produces no batch");
        let events = registry.drain(0);
        assert_eq!(counter_in(&events, "logit.input.writes", ("class", "timeout")), Some(1.0));
    }

    #[tokio::test]
    async fn telemetry_counts_writes_by_class_and_samples() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver.with_telemetry(telemetry);
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 7.0, millis(1))]],
            remote_write::Version::V1,
        );

        post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;
        recv_batch_async(&mut rx).await;
        post_write(&addr, "/nowhere", remote_write::Version::V1, &body).await;

        let events = registry.drain(0);
        assert_eq!(counter_in(&events, "logit.input.writes", ("class", "ok")), Some(1.0));
        assert_eq!(counter_in(&events, "logit.input.writes", ("class", "not_found")), Some(1.0));
        assert_eq!(
            counter_in(&events, "logit.input.samples", ("component", "receive")),
            Some(1.0),
            "the receiver reuses scrape mode's own samples counter"
        );
    }

    #[tokio::test]
    async fn a_remote_write_request_over_tls_reaches_the_fanout() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let receiver = receiver
            .with_bind_tls(
                &TlsServerSettings {
                    cert_file: "server.pem".to_string(),
                    key_file: "server.key".to_string(),
                    client_ca_file: None,
                },
                &testdata_dir(),
            )
            .expect("a well-formed bind_tls: block should build fine");
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 5.0, millis(1))]],
            remote_write::Version::V1,
        );

        let connector = test_tls_connector();
        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let mut tls_stream = connector.connect(server_name, stream).await.unwrap();
        let request = format!(
            "POST /api/v1/write HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{}\r\n",
            body.len(),
            write_headers(remote_write::Version::V1)
        );
        tls_stream.write_all(request.as_bytes()).await.unwrap();
        tls_stream.write_all(&body).await.unwrap();
        let mut buf = Vec::new();
        let _ =
            tokio::time::timeout(Duration::from_secs(5), tls_stream.read_to_end(&mut buf)).await;
        let response = String::from_utf8_lossy(&buf).into_owned();

        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(gauge_value_of(&batch, "queue_depth"), Some(5.0));
    }

    /// A keep-alive connection that finished its write and went quiet gives its permit back. Under
    /// `with_max_connections(1)` the follow-up request is served only if it did; the close is
    /// counted, never diagnosed. `otlp_in` has the same test over the same `crate::http` helpers.
    #[tokio::test]
    async fn an_idle_keep_alive_connection_is_closed_and_releases_its_permit() {
        use tokio::io::AsyncWriteExt;
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let diag = Diagnostics::new("prometheus_in").with_telemetry(telemetry.clone());
        let listener_diag = diag.clone();
        let receiver = receiver
            .with_telemetry(telemetry)
            .with_diagnostics(diag)
            .with_max_connections(1)
            .with_idle_timeout(Some(Duration::from_millis(100)));
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 1.0, millis(1))]],
            remote_write::Version::V1,
        );

        // One complete write, keep-alive, so only the idle clock can end the connection.
        let mut keep_alive = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let request = format!(
            "POST /api/v1/write HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nContent-Type: \
             {}\r\nContent-Encoding: {}\r\n\r\n",
            body.len(),
            remote_write::Version::V1.content_type(),
            compression::CONTENT_ENCODING_SNAPPY
        );
        keep_alive.write_all(request.as_bytes()).await.unwrap();
        keep_alive.write_all(&body).await.unwrap();
        let head = read_head(&mut keep_alive, Duration::from_secs(5))
            .await
            .expect("the keep-alive write is answered");
        assert!(head.starts_with("HTTP/1.1 204"), "got: {head}");
        recv_batch_async(&mut rx).await;

        expect_closed(&mut keep_alive, "a keep-alive connection quiet past its idle_timeout").await;

        let drained = registry.drain(0);
        assert_eq!(
            counter_in(&drained, "logit.input.connections.closed", ("reason", "idle")),
            Some(1.0),
            "an idle close is counted"
        );
        assert_eq!(
            listener_diag.occurrences("connection_error"),
            0,
            "and never diagnosed -- an idle close returns Ok(())"
        );

        // Under `with_max_connections(1)` this can only be answered if the permit came back.
        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;
        assert!(response.starts_with("HTTP/1.1 204"), "permit came back, got: {response}");
        recv_batch_async(&mut rx).await;
        drop(keep_alive);
    }

    /// A connection that connects and says nothing is closed by the plaintext first-byte bound.
    #[tokio::test]
    async fn a_silent_connection_releases_its_permit_after_the_handshake_timeout() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let receiver =
            receiver.with_max_connections(1).with_handshake_timeout(Duration::from_millis(100));
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 1.0, millis(1))]],
            remote_write::Version::V1,
        );

        let silent = tokio::net::TcpStream::connect(&addr).await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;

        let response = post_write(&addr, "/api/v1/write", remote_write::Version::V1, &body).await;
        assert!(response.starts_with("HTTP/1.1 204"), "permit came back, got: {response}");
        recv_batch_async(&mut rx).await;
        drop(silent);
    }

    /// `recv_batch`, awaiting: the receiver answers on a spawned task, so a batch may land after
    /// the response is read.
    async fn recv_batch_async(rx: &mut mpsc::Receiver<Delivered>) -> EventBatch {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("a batch should arrive within 5s")
            .expect("the channel should still be open");
        match delivered {
            Delivered::Owned(batch, _ctx) => batch,
            Delivered::Shared(shared, _ctx) => (*shared).clone(),
        }
    }

    /// A `tokio-rustls` client trusting `testdata/tls/ca.pem`, presenting no client certificate.
    fn test_tls_connector() -> tokio_rustls::TlsConnector {
        let dir = testdata_dir();
        let mut roots = rustls::RootCertStore::empty();
        let ca: Vec<rustls_pki_types::CertificateDer<'static>> =
            rustls_pki_types::CertificateDer::pem_file_iter(dir.join("ca.pem"))
                .unwrap()
                .collect::<Result<_, _>>()
                .unwrap();
        roots.add_parsable_certificates(ca);
        let cfg = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
        tokio_rustls::TlsConnector::from(Arc::new(cfg))
    }

    // ---- bind mode: the metadata cache --------------------------------------------------------
    //
    // The 1.0 shape: one request declares a family and carries no samples, later requests carry
    // its samples and declare nothing. `remote_write::encode` produces neither half alone (it
    // writes a `metadata[]` entry for every family it has series for), so these are hand-built.

    use logit_proto::prometheus::generated::io::prometheus::write::v2 as pb2;
    use logit_proto::prometheus::generated::prometheus as pb1;
    use prost::Message as _;

    /// A 1.0 request that declares `foo` and carries nothing else, as Prometheus's sender writes on
    /// its `metadata_config` schedule.
    fn v1_metadata_only(kind: pb1::metric_metadata::MetricType, help: &str, unit: &str) -> Vec<u8> {
        let request = pb1::WriteRequest {
            timeseries: Vec::new(),
            metadata: vec![pb1::MetricMetadata {
                r#type: kind as i32,
                metric_family_name: "foo".to_string(),
                help: help.to_string(),
                unit: unit.to_string(),
            }],
        };
        snappy(&request.encode_to_vec())
    }

    fn v1_sample(name: &str, extra: Option<(&str, &str)>, value: f64) -> pb1::TimeSeries {
        // Labels sorted by byte order, as both specs require of a sender: `__name__` first here
        // because `_` (0x5f) sorts below every lowercase letter.
        let mut labels = vec![pb1::Label { name: "__name__".to_string(), value: name.to_string() }];
        if let Some((key, value)) = extra {
            labels.push(pb1::Label { name: key.to_string(), value: value.to_string() });
        }
        pb1::TimeSeries {
            labels,
            samples: vec![pb1::Sample { value, timestamp: 1_700_000_000_000 }],
            exemplars: Vec::new(),
            histograms: Vec::new(),
        }
    }

    /// The three flat series one classic histogram is spelled as, with no metadata at all.
    fn v1_histogram_samples_only() -> Vec<u8> {
        let request = pb1::WriteRequest {
            timeseries: vec![
                v1_sample("foo_bucket", Some(("le", "+Inf")), 7.0),
                v1_sample("foo_count", None, 7.0),
                v1_sample("foo_sum", None, 2.5),
            ],
            metadata: Vec::new(),
        };
        snappy(&request.encode_to_vec())
    }

    /// The one histogram record a decoded `foo` family becomes, or `None` if the request decoded
    /// to something flatter.
    fn histogram_record(batch: &EventBatch) -> Option<&MetricRecord> {
        batch.events.iter().flat_map(|e| e.metrics.iter()).find(|m| {
            logit_core::interner::resolve(m.name) == "foo"
                && matches!(m.kind, MetricKind::Histogram(_))
        })
    }

    /// A metadata-only request teaches the receiver what `foo` is, and the samples-only request
    /// that follows decodes as one typed `Histogram` with its help and unit, not three `unknown`
    /// series.
    #[tokio::test]
    async fn a_metadata_only_request_types_the_samples_only_request_that_follows() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver
            .with_telemetry(telemetry)
            .with_metadata_cache(10_000, Duration::from_secs(600));
        let mut rx = spawn_receiver(receiver, 4);

        let declare = v1_metadata_only(
            pb1::metric_metadata::MetricType::Histogram,
            "Request duration.",
            "seconds",
        );
        let response =
            post_write(&addr, "/api/v1/write", remote_write::Version::V1, &declare).await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        assert!(rx.try_recv().is_err(), "a metadata-only request carries no samples to send");

        let samples = v1_histogram_samples_only();
        let response =
            post_write(&addr, "/api/v1/write", remote_write::Version::V1, &samples).await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");

        let batch = recv_batch_async(&mut rx).await;
        let record = histogram_record(&batch).expect("the cached type assembles one histogram");
        assert_eq!(
            record.description.map(logit_core::interner::resolve),
            Some("Request duration."),
            "help is remembered too, not just the type"
        );
        assert_eq!(record.unit.map(logit_core::interner::resolve), Some("seconds"));
        assert_eq!(
            batch.events.len(),
            1,
            "three wire series, one model record: {:#?}",
            batch.events
        );
        // No `prometheus.type` marker: `MetricKind::Histogram` *is* the type, and the marker
        // exists only for the family types the model has no kind of its own for.
        assert_eq!(batch.events[0].attributes.get(ATTR_TYPE), None);

        let events = registry.drain(0);
        assert_eq!(
            gauge_in(&events, "logit.input.metadata_cache.size", ("component", "receive")),
            Some(1.0)
        );
    }

    /// `max_families: 0` is no cache: the same pair of requests decodes flat, and nothing is
    /// counted.
    #[tokio::test]
    async fn a_disabled_metadata_cache_leaves_the_stateless_decode_path_alone() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver =
            receiver.with_telemetry(telemetry).with_metadata_cache(0, Duration::from_secs(600));
        let mut rx = spawn_receiver(receiver, 4);

        let declare = v1_metadata_only(
            pb1::metric_metadata::MetricType::Histogram,
            "Request duration.",
            "seconds",
        );
        post_write(&addr, "/api/v1/write", remote_write::Version::V1, &declare).await;
        let samples = v1_histogram_samples_only();
        post_write(&addr, "/api/v1/write", remote_write::Version::V1, &samples).await;

        let batch = recv_batch_async(&mut rx).await;
        assert!(histogram_record(&batch).is_none(), "nothing was remembered, so nothing assembled");
        assert_eq!(batch.events.len(), 3, "three unrelated flat series: {:#?}", batch.events);
        for event in &batch.events {
            assert_eq!(event.attributes.get(ATTR_TYPE).and_then(|v| v.as_str()), Some("unknown"));
        }

        let events = registry.drain(0);
        assert_eq!(
            gauge_in(&events, "logit.input.metadata_cache.size", ("component", "receive")),
            None,
            "a cache that does not exist reports nothing"
        );
    }

    /// A request's own metadata beats the remembered entry, per family name, and the retype is
    /// counted.
    #[tokio::test]
    async fn a_request_declaration_overrides_a_cached_one() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver
            .with_telemetry(telemetry)
            .with_metadata_cache(10_000, Duration::from_secs(600));
        let mut rx = spawn_receiver(receiver, 4);

        let declare =
            v1_metadata_only(pb1::metric_metadata::MetricType::Histogram, "A histogram.", "");
        post_write(&addr, "/api/v1/write", remote_write::Version::V1, &declare).await;

        // Now a request that says `foo` is a gauge and carries a bare `foo` sample. The cached
        // histogram would have typed it `unknown` (a seeded type gives way to a sample it cannot
        // place rather than rejecting it), so the `gauge` here is the request's own word.
        let retype = pb1::WriteRequest {
            timeseries: vec![v1_sample("foo", None, 3.0)],
            metadata: vec![pb1::MetricMetadata {
                r#type: pb1::metric_metadata::MetricType::Gauge as i32,
                metric_family_name: "foo".to_string(),
                help: "A gauge, actually.".to_string(),
                unit: String::new(),
            }],
        };
        let response = post_write(
            &addr,
            "/api/v1/write",
            remote_write::Version::V1,
            &snappy(&retype.encode_to_vec()),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");

        let batch = recv_batch_async(&mut rx).await;
        // A `MetricKind::Gauge` named `foo` -- the request's type, not the remembered one, and
        // not the `Unknown` the seed would have given way to on its own.
        assert_eq!(gauge_value_of(&batch, "foo"), Some(3.0));
        assert!(histogram_record(&batch).is_none());

        let events = registry.drain(0);
        assert_eq!(
            counter_in(&events, "logit.input.metadata_cache.replaced", ("component", "receive")),
            Some(1.0),
            "the retype is counted once"
        );
    }

    /// 2.0's inline `Metadata` types a 1.0 sender's series: a 2.0 series declares the base family
    /// its type implies (`foo_bucket` under `HISTOGRAM` declares `foo`), the key a 1.0
    /// `metadata[]` entry uses.
    #[tokio::test]
    async fn a_2_0_request_fills_the_cache_for_a_later_1_0_sender() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let receiver = receiver.with_metadata_cache(10_000, Duration::from_secs(600));
        let mut rx = spawn_receiver(receiver, 4);

        let v2 = pb2::Request {
            symbols: vec![
                String::new(),
                "__name__".to_string(),
                "foo_bucket".to_string(),
                "le".to_string(),
                "+Inf".to_string(),
                "Request duration.".to_string(),
                "seconds".to_string(),
            ],
            timeseries: vec![pb2::TimeSeries {
                labels_refs: vec![1, 2, 3, 4],
                samples: vec![pb2::Sample {
                    value: 1.0,
                    timestamp: 1_700_000_000_000,
                    start_timestamp: 0,
                }],
                metadata: Some(pb2::Metadata {
                    r#type: pb2::metadata::MetricType::Histogram as i32,
                    help_ref: 5,
                    unit_ref: 6,
                }),
                ..Default::default()
            }],
        };
        let response = post_write(
            &addr,
            "/api/v1/write",
            remote_write::Version::V2,
            &snappy(&v2.encode_to_vec()),
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");
        let _ = recv_batch_async(&mut rx).await; // the 2.0 request's own (bucket-only) family

        // Now a 1.0 sender's samples-only write of the same family.
        let samples = v1_histogram_samples_only();
        let response =
            post_write(&addr, "/api/v1/write", remote_write::Version::V1, &samples).await;
        assert!(response.starts_with("HTTP/1.1 204"), "got: {response}");

        let batch = recv_batch_async(&mut rx).await;
        let record = histogram_record(&batch).expect("the 2.0 declaration typed the 1.0 request");
        assert_eq!(
            record.description.map(logit_core::interner::resolve),
            Some("Request duration.")
        );
        assert_eq!(record.unit.map(logit_core::interner::resolve), Some("seconds"));
    }

    // ---- the table itself, against a clock a test owns ----------------------------------------
    //
    // `Instant` is a parameter of `seed`/`learn`, so expiry and eviction are testable without
    // sleeping through a real TTL.

    fn declarations(entries: &[(&str, FamilyType)]) -> remote_write::Declarations {
        let mut declarations = remote_write::Declarations::default();
        for (name, kind) in entries {
            declarations.insert(*name, *kind, None, None);
        }
        declarations
    }

    fn seeded_names(seed: &remote_write::Declarations) -> Vec<&str> {
        let mut names: Vec<&str> = seed.iter().map(|(name, _)| name).collect();
        names.sort_unstable();
        names
    }

    /// A family nothing has re-declared within the TTL stops being typed (its next samples come
    /// back `unknown`), and the drop is counted.
    #[test]
    fn a_cached_family_expires_once_its_ttl_has_run_out() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let cache = MetadataCache::new(10, Duration::from_secs(600));
        let start = Instant::now();

        cache.learn(&declarations(&[("foo", FamilyType::Histogram)]), start, &telemetry);
        assert_eq!(seeded_names(&cache.seed(start, &telemetry)), ["foo"]);

        // One second inside the window, then one past it.
        assert_eq!(
            seeded_names(&cache.seed(start + Duration::from_secs(599), &telemetry)),
            ["foo"]
        );
        let seed = cache.seed(start + Duration::from_secs(601), &telemetry);
        assert!(seed.is_empty(), "the declaration is no longer trusted");

        let events = registry.drain(0);
        assert_eq!(
            counter_in(&events, "logit.input.metadata_cache.evicted", ("reason", "expired")),
            Some(1.0)
        );
        assert_eq!(
            gauge_in(&events, "logit.input.metadata_cache.size", ("component", "receive")),
            Some(0.0)
        );
    }

    /// Over the cap the **least-recently-seen** family goes, and "seen" means last declared: a
    /// family re-declared by a later request outlives one that was inserted after it but never
    /// mentioned again.
    #[test]
    fn the_cardinality_cap_evicts_the_least_recently_seen_family() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let cache = MetadataCache::new(2, Duration::from_secs(600));
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);

        cache.learn(&declarations(&[("a", FamilyType::Counter)]), at(0), &telemetry);
        cache.learn(&declarations(&[("b", FamilyType::Counter)]), at(1), &telemetry);
        // `a` is refreshed, so `b` is now the oldest thing in the table.
        cache.learn(&declarations(&[("a", FamilyType::Counter)]), at(2), &telemetry);
        cache.learn(&declarations(&[("c", FamilyType::Counter)]), at(3), &telemetry);

        assert_eq!(seeded_names(&cache.seed(at(3), &telemetry)), ["a", "c"]);
        let events = registry.drain(0);
        assert_eq!(
            counter_in(&events, "logit.input.metadata_cache.evicted", ("reason", "cardinality")),
            Some(1.0)
        );
        assert_eq!(
            gauge_in(&events, "logit.input.metadata_cache.size", ("component", "receive")),
            Some(2.0)
        );
    }

    /// One request declaring several families over the cap evicts them in one pass, tie-broken by
    /// family name rather than hash order.
    #[test]
    fn one_over_cap_request_evicts_in_a_single_pass_and_ties_break_by_name() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let cache = MetadataCache::new(2, Duration::from_secs(600));
        let now = Instant::now();

        cache.learn(
            &declarations(&[
                ("a", FamilyType::Counter),
                ("b", FamilyType::Counter),
                ("c", FamilyType::Counter),
                ("d", FamilyType::Counter),
            ]),
            now,
            &telemetry,
        );

        assert_eq!(seeded_names(&cache.seed(now, &telemetry)), ["c", "d"]);
        let events = registry.drain(0);
        assert_eq!(
            counter_in(&events, "logit.input.metadata_cache.evicted", ("reason", "cardinality")),
            Some(2.0),
            "two evictions, counted once"
        );
    }

    /// Re-declaring a family the same way is not a retype: a 1.0 sender repeating itself every
    /// minute must not move `replaced`.
    #[test]
    fn relearning_the_same_declaration_is_not_counted_as_a_replacement() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let cache = MetadataCache::new(10, Duration::from_secs(600));
        let start = Instant::now();

        cache.learn(&declarations(&[("foo", FamilyType::Counter)]), start, &telemetry);
        cache.learn(
            &declarations(&[("foo", FamilyType::Counter)]),
            start + Duration::from_secs(60),
            &telemetry,
        );
        assert_eq!(
            counter_in(
                &registry.drain(0),
                "logit.input.metadata_cache.replaced",
                ("component", "receive")
            ),
            None,
            "a sender repeating itself is not a retype"
        );

        // And the repeat *did* refresh the entry: it survives a TTL measured from the first.
        assert_eq!(
            seeded_names(&cache.seed(start + Duration::from_secs(620), &telemetry)),
            ["foo"],
            "last_seen moved with the second declaration"
        );
    }

    /// A federating Prometheus declares relayed series `UNKNOWN` ("no type given"), which must not
    /// overwrite a remembered `HISTOGRAM`, or the real sender's `_bucket`/`_sum`/`_count` would
    /// come apart on every federated write. `UNKNOWN` is not learned (`remote_write::decode_v1`).
    #[tokio::test]
    async fn an_unknown_declaration_cannot_replace_a_cached_type() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver
            .with_telemetry(telemetry)
            .with_metadata_cache(10_000, Duration::from_secs(600));
        let mut rx = spawn_receiver(receiver, 4);

        let declare = v1_metadata_only(
            pb1::metric_metadata::MetricType::Histogram,
            "Request duration.",
            "seconds",
        );
        post_write(&addr, "/api/v1/write", remote_write::Version::V1, &declare).await;

        // The federating sender's write: `foo` UNKNOWN, with a bare `foo` sample of its own.
        let federated = pb1::WriteRequest {
            timeseries: vec![v1_sample("foo", None, 1.0)],
            metadata: vec![pb1::MetricMetadata {
                r#type: pb1::metric_metadata::MetricType::Unknown as i32,
                metric_family_name: "foo".to_string(),
                help: String::new(),
                unit: String::new(),
            }],
        };
        post_write(
            &addr,
            "/api/v1/write",
            remote_write::Version::V1,
            &snappy(&federated.encode_to_vec()),
        )
        .await;
        // Its own sample survives: the remembered histogram gives way rather than rejecting it.
        let batch = recv_batch_async(&mut rx).await;
        assert_eq!(gauge_value_of(&batch, "foo"), Some(1.0));

        // And the real sender's next write is still assembled as the histogram it is.
        let samples = v1_histogram_samples_only();
        post_write(&addr, "/api/v1/write", remote_write::Version::V1, &samples).await;
        let batch = recv_batch_async(&mut rx).await;
        assert!(histogram_record(&batch).is_some(), "{:#?}", batch.events);

        let events = registry.drain(0);
        assert_eq!(
            counter_in(&events, "logit.input.metadata_cache.replaced", ("component", "receive")),
            None,
            "an UNKNOWN entry is not learned, so it cannot retype anything"
        );
        assert_eq!(
            counter_in(&events, "logit.input.metrics.degraded", ("reason", "seed_mismatch")),
            Some(1.0),
            "the bare `foo` sample is the one the remembered histogram gave way to"
        );
    }

    /// A remembered description is cut to [`MAX_METADATA_TEXT_BYTES`] on a `char` boundary, and the
    /// cut is counted.
    #[test]
    fn a_remembered_description_is_bounded_and_cut_on_a_char_boundary() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let cache = MetadataCache::new(10, Duration::from_secs(600));
        let now = Instant::now();

        // A multi-byte char straddling the cut, so a naive slice would panic and a byte-wise one
        // would produce invalid UTF-8.
        let help: String = "é".repeat(MAX_METADATA_TEXT_BYTES);
        let mut declarations = remote_write::Declarations::default();
        declarations.insert(
            "foo",
            FamilyType::Counter,
            Some(Arc::from(help.as_str())),
            Some(Arc::from("seconds")),
        );
        cache.learn(&declarations, now, &telemetry);

        let seed = cache.seed(now, &telemetry);
        let (_, remembered) = seed.iter().next().expect("one family");
        let kept = remembered.help.as_deref().expect("the description survives, shortened");
        assert!(kept.len() <= MAX_METADATA_TEXT_BYTES, "{} bytes", kept.len());
        assert!(kept.len() > MAX_METADATA_TEXT_BYTES - 4, "cut at the boundary, not far short");
        assert!(help.starts_with(kept), "a prefix of what the sender sent");
        // A short unit alongside it is untouched, and shares the request's own allocation.
        assert_eq!(remembered.unit.as_deref(), Some("seconds"));

        assert_eq!(
            counter_in(
                &registry.drain(0),
                "logit.input.metadata_cache.truncated",
                ("component", "receive")
            ),
            Some(1.0),
            "one string cut, not one entry"
        );
    }

    /// An identical re-declaration (the common write) does not rebuild the seed: the seed handed
    /// out is the *same* `Arc`, the only way to observe that no rebuild happened.
    #[test]
    fn re_declaring_what_is_already_remembered_does_not_rebuild_the_seed() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let cache = MetadataCache::new(10, Duration::from_secs(600));
        let start = Instant::now();
        let declaration = declarations(&[("foo", FamilyType::Counter)]);

        cache.learn(&declaration, start, &telemetry);
        let first = cache.seed(start, &telemetry);

        cache.learn(&declaration, start + Duration::from_secs(60), &telemetry);
        let second = cache.seed(start + Duration::from_secs(60), &telemetry);
        assert!(Arc::ptr_eq(&first, &second), "nothing changed, so nothing was rebuilt");

        // A declaration that really says something new does rebuild.
        cache.learn(
            &declarations(&[("foo", FamilyType::Gauge)]),
            start + Duration::from_secs(120),
            &telemetry,
        );
        let third = cache.seed(start + Duration::from_secs(120), &telemetry);
        assert!(!Arc::ptr_eq(&second, &third), "a retype is a change");
    }

    /// On a no-op re-declaration the **entry** keeps its `Arc<str>`s too, rather than adopting the
    /// request's clones; otherwise cache and un-rebuilt seed would hold equal strings in separate
    /// allocations. Its own builder, because `declarations()` above inserts `help: None`, leaving
    /// no per-entry `Arc<str>` to observe.
    #[test]
    fn re_declaring_what_is_already_remembered_keeps_the_entrys_own_arc() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let cache = MetadataCache::new(10, Duration::from_secs(600));
        let start = Instant::now();

        let described = |help: &str| {
            let mut declarations = remote_write::Declarations::default();
            declarations.insert("foo", FamilyType::Counter, Some(Arc::from(help)), None);
            declarations
        };

        cache.learn(&described("Total requests."), start, &telemetry);
        let seed = cache.seed(start, &telemetry);
        let seeded_help = seed
            .iter()
            .find(|(name, _)| *name == "foo")
            .and_then(|(_, declaration)| declaration.help.clone())
            .expect("the entry was learned with a help string");

        // A *fresh* `Arc` with the same text, as a second identical request brings.
        cache.learn(&described("Total requests."), start + Duration::from_secs(60), &telemetry);
        let after = cache.seed(start + Duration::from_secs(60), &telemetry);
        assert!(Arc::ptr_eq(&seed, &after), "nothing changed, so nothing was rebuilt");

        let entry_help = cache
            .lock()
            .families
            .get("foo")
            .and_then(|family| family.help.clone())
            .expect("still remembered");
        assert!(
            Arc::ptr_eq(&entry_help, &seeded_help),
            "a no-op re-declaration must leave the entry's own Arc in place -- the un-rebuilt seed \
             is still sharing it"
        );

        // And a help string that really differs is adopted, seed rebuilt with it.
        cache.learn(&described("Requests served."), start + Duration::from_secs(120), &telemetry);
        let changed = cache.seed(start + Duration::from_secs(120), &telemetry);
        assert!(!Arc::ptr_eq(&after, &changed), "new help text is a change");
        assert_eq!(
            changed
                .iter()
                .find(|(name, _)| *name == "foo")
                .and_then(|(_, declaration)| declaration.help.clone())
                .as_deref(),
            Some("Requests served.")
        );
    }

    /// The expiry sweep is checked per request, not performed: no walk until the earliest entry
    /// could have expired. Only the test-only sweep counter can see this.
    #[test]
    fn a_request_before_the_watermark_does_not_sweep() {
        use std::sync::atomic::Ordering;

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let cache = MetadataCache::new(10, Duration::from_secs(600));
        let start = Instant::now();

        // An empty table has no earliest expiry at all, so not even the first request sweeps.
        let _ = cache.seed(start, &telemetry);
        assert_eq!(cache.sweeps.load(Ordering::Relaxed), 0);

        cache.learn(&declarations(&[("foo", FamilyType::Counter)]), start, &telemetry);
        for seconds in [1, 60, 599, 600] {
            let _ = cache.seed(start + Duration::from_secs(seconds), &telemetry);
        }
        assert_eq!(cache.sweeps.load(Ordering::Relaxed), 0, "nothing could have expired yet");

        // Past it, once, and the recomputed watermark is `None`, the table now being empty.
        let seed = cache.seed(start + Duration::from_secs(601), &telemetry);
        assert_eq!(cache.sweeps.load(Ordering::Relaxed), 1);
        assert!(seed.is_empty());
        let _ = cache.seed(start + Duration::from_secs(3_600), &telemetry);
        assert_eq!(cache.sweeps.load(Ordering::Relaxed), 1, "an empty table has nothing to sweep");
    }
}
