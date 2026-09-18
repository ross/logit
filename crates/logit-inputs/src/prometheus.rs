//! `prometheus_in`: **two modes on one kind.** `scrape_targets:` scrapes Prometheus `/metrics`
//! endpoints on an interval, the way Prometheus's own server does; `bind:` is a Prometheus
//! **remote-write receiver**, accepting 1.0 and 2.0 requests on one listener. Exactly one of the
//! two is configured -- graph rule 55, which also rejects a field belonging to the other mode
//! rather than ignoring it. See [ADR `prometheus-scrape-and-exposition`](../../../docs/adr/prometheus-scrape-and-exposition.md)
//! and [ADR `prometheus-remote-write`](../../../docs/adr/prometheus-remote-write.md) for the full
//! designs; this module doc is the implementation's own spec.
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
//! Named `scrape_targets`, not `targets` -- `Component.targets` (`docs/adr/
//! target-components.md`) claims the bare name at the flattened top level. The TLS keys are
//! `scrape_tls`/`bind_tls`, never a bare `tls`: this kind has a TLS-shaped role on each side --
//! client TLS dialling out, server TLS terminating in -- and each key names the socket it governs.
//!
//! # Scrape mode
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
//! why they're always on. **Bind mode synthesizes none of them**: a receiver performed no scrape,
//! so there is no `up` to report and nothing whose duration to measure.
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
//!
//! # Bind mode: the remote-write receiver
//!
//! [`PrometheusReceiver`] is an HTTP listener, not a client: `bind:` opens a `TcpListener`
//! ([`Input::bind`], idempotent), `run` is an accept loop, and each accepted connection is served
//! by [`hyper_util::server::conn::auto::Builder`] -- HTTP/1.1 and h2c off the same socket, since a
//! remote-write sender may be either. Both wire versions are accepted on that one listener,
//! chosen **per request** from its own `Content-Type` with nothing to configure: the 2.0 spec
//! requires a 2.0-capable receiver to keep accepting 1.0, and a `logit` receiver is worth pointing
//! a mixed-version fleet at.
//!
//! ## Routes
//!
//! | Request | Response |
//! |---|---|
//! | `POST path`, `Content-Encoding: snappy`, recognised `Content-Type` | decode, then `204` |
//! | any other path | `404` |
//! | any other method on `path` | `405` + `Allow: POST` |
//! | missing or other `Content-Encoding`, unrecognised or missing `Content-Type` | `415` |
//! | a body, or a Snappy `decompress_len`, over [`MAX_REQUEST_BYTES`] | `413` |
//! | a body that stops arriving mid-upload | `408`, and the connection closes |
//! | Snappy or protobuf failure, 2.0 symbol-table errors | `400`, `text/plain` reason |
//!
//! **`405` is a deliberate divergence from `otlp_in`**, which answers `404` for a non-`POST`.
//! `prometheus_out`'s exposition server already answers `405` for a wrong method on `/metrics`,
//! and this receiver lives on the same kind pair, so it matches its sibling rather than the
//! unrelated input whose accept loop it copied. A reader diffing the two inputs finds the reason
//! here rather than a bug.
//!
//! **A missing `Content-Type` is a `415`, not a default.** `otlp_in` treats an absent type as
//! protobuf, deliberately, because every client predating its JSON support sent none. Remote-write
//! has no such history: both specs require the header, so guessing 1.0 would turn a 2.0 sender's
//! misconfiguration into a wall of protobuf decode errors instead of the one status the spec has
//! for exactly this.
//!
//! **The `-Written` headers.** A 2.0 request's response carries
//! `X-Prometheus-Remote-Write-{Samples,Histograms,Exemplars}-Written`, on `4xx` as well as `2xx`
//! as 2.0 requires, reporting what this receiver actually stored -- zeros on a rejection, and
//! always `0` histograms, since native histograms are skipped and counted rather than stored
//! (`docs/known-gaps.md`). A 1.0 request gets none: 1.0 defines none.
//!
//! *Samples-written is measured on the way out, not on the way in.* The codec's own accepted count
//! is a statement about what the assembler took, and the model mapping that runs afterwards can
//! still drop a whole series (an empty histogram, a histogram whose bucket counts decrease). So
//! the number reported is every decoded series' wire samples minus the ones belonging to a series
//! that mapping dropped, both measured by [`logit_proto::prometheus::remote_write::wire_samples`]
//! -- which reads the [`logit_proto::prometheus::Point`], and so still sees the `Option`s the wire
//! had (a summary sent as quantiles alone is two samples, not four). A request whose every series
//! was dropped answers `204` with `Samples-Written: 0` and sends no batch, which is the honest
//! report of having stored nothing.
//!
//! ## What a decoded request becomes
//!
//! **One `EventBatch` per request**, with an **empty `Resource`** and `received_at` = now, built
//! by concatenating [`families_to_events`] over the decoded timestamp groups. A request that
//! decodes to no events at all sends no batch -- a `204` and nothing downstream.
//!
//! **Labels stay labels.** `instance` and `job` arrive as ordinary labels -- both are optional per
//! spec, neither is structurally distinguished on the wire -- and stay ordinary event attributes,
//! verbatim. Nothing is lifted into `Resource` and no `prometheus.target` is stamped. This is the
//! opposite of what scrape mode does, and the difference is a fact about what each mode knows: a
//! scrape connected to the target it names, while a receiver observed a TCP connection from a
//! sender that may be relaying for thousands of targets. Promoting a payload label to resource
//! identity would invent structure the wire did not carry, and would silently change the label set
//! a remote-write → remote-write relay re-emits. An operator who wants resource identity gets it
//! from a downstream `set` component or Lua, which is where a claim about *this* deployment's
//! topology belongs.
//!
//! **Timestamps and timestamp groups.** A remote-write `TimeSeries` is one label set and N
//! samples; a `Series` holds one point and one timestamp. So the codec partitions a request's
//! samples by timestamp and returns one family list per distinct timestamp in ascending order
//! (`logit_proto::prometheus::remote_write`'s own doc), and this receiver concatenates them into
//! one batch -- N events per series, in timestamp order. `Event::timestamp` comes from the sample,
//! and the decoder runs with `with_timestamp_marker(false)` so **no `prometheus.timestamp: true`
//! attribute is set**: that marker records a *producer's choice* to expose a timestamp on an
//! exposition line, and a transport that mandates one is not that choice. Setting it would make a
//! remote-write → exposition relay stamp an explicit timestamp on every line it writes, which no
//! scrape of the same data would have produced.
//!
//! ## Metadata cache
//!
//! **Why there is one at all.** Remote-write carries a family's type, `# HELP` and `# UNIT` as
//! *metadata*, and 1.0 puts it in `WriteRequest.metadata[]` -- which Prometheus's own sender ships
//! in **separate requests**, on its own schedule (`metadata_config`, by default once a minute),
//! rather than attached to the samples it describes. A receiver that remembers nothing therefore
//! sees, for nearly every 1.0 request, a bag of flat series with no type anywhere in the message:
//! every family decodes as `unknown`, and `http_request_duration_seconds_bucket`/`_sum`/`_count`
//! arrive as three unrelated series instead of one histogram. Nothing is lost -- the samples and
//! labels are exact, and a relay back out to remote-write is still a fixed point -- but the model
//! kinds are flatter than the producer's, which is what the cache fixes.
//!
//! [`MetadataCache`] holds `family name -> (type, help, unit, last seen)`, seeded into every decode
//! ([`remote_write::decode_with`]) and learned from every request's own
//! [`Decoded::declarations`](remote_write::Decoded::declarations) -- 1.0's `metadata[]` and 2.0's
//! inline `Metadata` alike, so a mixed-version fleet fills one table and a 2.0 sender's
//! declarations type a 1.0 sender's series.
//!
//! **Precedence: the request, then the cache.** A declaration in the request being decoded always
//! wins, per family name; the cache answers only for a family that request said nothing about. A
//! sender that retypes a family retypes it immediately, however stale the remembered entry.
//!
//! **What a TTL expiry means.** An expired family stops being typed -- its next samples decode as
//! `unknown` and its `_bucket`/`_sum`/`_count` series come apart again -- until the sender's next
//! metadata request re-declares it. That is the bound working, not a fault: a sender that has
//! stopped writing should stop costing memory, and a remembered type nothing has reasserted within
//! the TTL is a guess about a series that may no longer exist. The default 10m is an order of
//! magnitude over Prometheus's own metadata cadence, so a live sender has to miss ten refreshes
//! running to lapse.
//!
//! **Bounds.** `max_families` caps the table; over it, the **least-recently-seen** family is
//! evicted first (ties broken by name, so it is a function of the data rather than of map order),
//! the same policy and the same one-pass shape `prometheus_out`'s exposition `max_series:` uses.
//! `max_families: 0` is not a zero-size cache but no cache at all: nothing is allocated, no lock is
//! taken, and requests decode through the stateless [`remote_write::decode`] exactly as they did
//! before this existed. Rule 55 rejects `ttl: 0s`, which would be the pointless version of that.
//!
//! **Concurrency, and what a request actually pays.** One `std::sync::Mutex` guards the table,
//! taken to build the seed and taken again to learn, **never held across the decode** -- a
//! connection's requests are served concurrently and the decode is the expensive part. A blocking
//! mutex parks the Tokio worker thread of anyone waiting on it, so the rule here is that the held
//! section is `O(1)` unless something really changed:
//!
//! - the seed handed to the codec is an `Arc` of the table in the codec's own shape, rebuilt only
//!   when the table's *contents* change -- re-declaring what is already remembered (which is what
//!   Prometheus does every `send_interval`, from every shard) touches `last_seen` and nothing else;
//! - the expiry sweep is *checked* per request, not performed: the table carries the earliest
//!   instant at which any entry could go, so until then expiry costs one comparison;
//! - a request that declares nothing -- nearly every 1.0 request -- does not take the lock a second
//!   time at all.
//!
//! So a sample-only request pays a lock, a comparison and a refcount bump, and the passes that
//! scale with what is remembered happen only when an entry is really added, retyped or expired.
//!
//! ## Size, concurrency, and shutdown
//!
//! [`MAX_REQUEST_BYTES`] (4 MiB) bounds the **decompressed** body, checked against Snappy's own
//! `decompress_len` before a byte is expanded, so a compression bomb is rejected rather than
//! inflated. [`MAX_CONCURRENT_CONNECTIONS`] bounds how many connections are served at once --
//! past it a connection is rejected, not queued (`logit.input.connections.rejected{reason="limit"}`).
//! [`HANDSHAKE_TIMEOUT`] bounds each connection's pre-request phase: its TLS accept on a TLS
//! listener, its first byte on a plaintext one. None of the three is a config field; the first two
//! are denial-of-service bounds rather than tuning knobs, and the third is a constant here because
//! graph rule 45's `handshake_timeout:` does not cover this kind.
//!
//! `idle_timeout:` closes a connection that sits with no request in flight, via the shared
//! tracker in [`crate::http`] -- `otlp_in`'s module doc holds the reasoning (why the clock is at
//! the service rather than the socket, why it resets on request *completion*, and why the close is
//! `graceful_shutdown` plus a bounded grace rather than a drop). A request whose *body* stalls
//! gets the narrower per-frame bound instead and answers `408`. There is no listener-level
//! graceful shutdown here or anywhere else in this repo: shutdown is per connection.
//!
//! ## Security posture
//!
//! `bind_tls:` gives transport security and nothing else. **The receiver has no authentication of
//! any kind** -- no bearer token, no basic auth, no mutual-TLS identity check beyond `rustls`
//! accepting a client certificate chain when `client_ca_file` is set -- so anything that can reach
//! the socket can write series into the pipeline. The same gap `admin:` and `prometheus_out`'s
//! exposition `bind:` already carry, tracked in `docs/known-gaps.md`: front it with something that
//! authenticates, or keep it on a trusted network.
//!
//! ## Counters
//!
//! `logit.input.writes{class}` -- one count per request, `class` one of `ok`, `not_found`,
//! `method`, `unsupported`, `oversize`, `timeout`, or `bad_request`. `logit.input.write.duration`
//! -- a timing sample per request, recorded regardless of outcome. `logit.input.samples` --
//! reused from scrape mode, counting the wire samples that actually reached the `Fanout` -- every
//! decoded series' worth minus every series the model mapping then dropped, exactly the number the
//! `-Written` header reports. Deliberately the same number: a counter and a header disagreeing
//! about one request would be a puzzle with no right answer. The connection
//! counters are `otlp_in`'s spelling verbatim (`logit.input.connections{,.rejected,.closed}`),
//! `logit.input.metadata_cache.size` -- how many families are remembered, a gauge published
//! whenever the table changes (a transition, like `logit.input.connections`, not a per-request
//! restatement). `logit.input.metadata_cache.evicted{reason}` -- `expired` for a family whose `ttl`
//! ran out, `cardinality` for one pushed out of `max_families` by a newer one.
//! `logit.input.metadata_cache.replaced` -- one count per family a request retyped, which is the
//! counter to watch when a sender's model kinds look wrong: a healthy fleet retypes almost nothing,
//! and a steady stream here is two senders disagreeing about one family name. The connection
//! counters are `otlp_in`'s spelling verbatim (`logit.input.connections{,.rejected,.closed}`),
//! since this is the same accept loop. A rejected request also reports
//! `Diagnostics::warn_throttled("write_rejected", ..)` with the peer address in the message text
//! only -- never a tag (a peer address isn't `&'static` and isn't safe to intern per-peer,
//! `AGENTS.md`'s tag-cardinality convention). The decoder's own
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

    /// Sets client-side TLS tuning (`scrape_tls:` in config) for any `https://` target -- a
    /// no-op if
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

// -------------------------------------------------------------------------------------------------
// Bind mode: the remote-write receiver
// -------------------------------------------------------------------------------------------------

/// Hard cap on one remote-write request's **decompressed** body -- checked against Snappy's own
/// `decompress_len`, read out of the block header, *before* a byte is expanded, so a compression
/// bomb is rejected rather than inflated. The same number and the same hardcoded-not-configurable
/// posture as `otlp_in`'s own cap (`crates/logit-inputs/src/otlp.rs`): a denial-of-service bound is
/// not a workload tuning knob, and Prometheus's default `max_samples_per_send` of 2000 puts a real
/// request orders of magnitude under it. An operator who hits this has a misconfigured sender.
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;

/// Bounds the number of connections [`PrometheusReceiver`] serves at once, so the per-request cap
/// [`MAX_REQUEST_BYTES`] bounds this listener's whole worst case rather than one connection's. The
/// same 1024 `otlp_in`, `logit_in` and `syslog_in` use -- there is no protocol reason for a
/// remote-write receiver to differ, and one shared figure is one thing for an operator to learn. A
/// connection past the cap is **rejected, not queued**, exactly as on those three.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// How long a connection has, per pre-request phase, before this listener gives up on it and
/// releases its [`MAX_CONCURRENT_CONNECTIONS`] permit: its TLS accept on a TLS listener, its first
/// byte on a plaintext one. The same 5s every other TCP listener here defaults to.
///
/// **Not an operator-facing field**, unlike `otlp_in`'s `handshake_timeout:` -- graph rule 45
/// enumerates the kinds that carry one and `prometheus_in` is not among them. A constant rather
/// than a silently-ignored config key; a field can be added if a deployment ever needs one.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// `crate::tls::TlsServerSettings`, re-exported at this path -- the receiver's `bind_tls:` block,
/// the server-side twin of this module's [`TlsClientSettings`] re-export. Same convention
/// `otlp::TlsServerSettings` already follows, so `logit-cli::pipeline::build_spec` has one obvious
/// path to import per socket.
pub use crate::tls::TlsServerSettings;

const METADATA_CACHE_SIZE: &str = "logit.input.metadata_cache.size";
const METADATA_CACHE_EVICTED: &str = "logit.input.metadata_cache.evicted";
const METADATA_CACHE_REPLACED: &str = "logit.input.metadata_cache.replaced";
const METADATA_CACHE_TRUNCATED: &str = "logit.input.metadata_cache.truncated";

/// The longest `# HELP` or `# UNIT` text this receiver will *remember* for one family. Past it the
/// text is truncated on a `char` boundary and the cut is counted
/// `logit.input.metadata_cache.truncated`.
///
/// Not a statement about what a description may be -- the decode keeps whatever the request
/// carried, and this bounds only the copy that outlives the request. Without it the cache's
/// resident size is `max_families x` the *request* cap: nothing else on the path bounds a
/// description, so 10 000 individually-legal metadata-only requests, each declaring one family
/// with a multi-megabyte help (a few KB once Snappy has seen the repeated bytes), would take the
/// process down while `metadata_cache.size` read a healthy 10 000. 1 KiB is an order of magnitude
/// past the longest `# HELP` any real exporter writes.
const MAX_METADATA_TEXT_BYTES: usize = 1024;

/// One remembered family declaration. The family's own name is the map key, not a field here.
///
/// `Arc<str>` rather than `String` so that rebuilding the seed -- which happens whenever the table
/// changes and produces a whole second copy of it for in-flight requests to hold -- shares this
/// text instead of copying it. The cache and every seed generation alive at once then cost one
/// description each, not one per generation.
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

/// What the receiver remembers about metric types between requests -- `metadata_cache:` in config.
/// See this module's "Metadata cache" doc section for why it exists and what its bounds mean; this
/// type is the table and the two operations a request performs on it.
///
/// Built only when the cache is on: `max_families: 0` leaves [`PrometheusReceiver::metadata_cache`]
/// `None`, and nothing here is allocated, locked or swept.
struct MetadataCache {
    /// Always `> 0` -- a zero cap is no cache at all, which is `None` one level up.
    max_families: usize,
    ttl: Duration,
    state: Mutex<CacheState>,
    /// Test-only: how many times the expiry sweep has actually walked the table. Not otherwise
    /// observable -- a sweep that expires nothing leaves behind no counter and no rebuild -- and
    /// "a sample-only request does not sweep" is the property [`CacheState::next_expiry`] exists
    /// for.
    #[cfg(test)]
    sweeps: std::sync::atomic::AtomicU64,
}

/// Everything behind the one lock. `families` is authoritative; `seed` is the same content in the
/// codec's own shape, rebuilt only when `families` changes, so the overwhelmingly common request --
/// samples, no declarations -- hands the decoder a refcount bump rather than a copy of every
/// remembered family.
#[derive(Default)]
struct CacheState {
    families: HashMap<String, CachedFamily>,
    seed: Arc<remote_write::Declarations>,
    /// The earliest instant at which *any* entry could have expired, and `None` when the table is
    /// empty -- what lets a request that declares nothing skip the sweep entirely.
    ///
    /// A **lower bound**, never an over-estimate, which is the whole of its correctness: a sweep
    /// skipped because `now` has not reached this cannot have missed an expiry. Every sweep
    /// recomputes it exactly; a learn only ever *lowers* it, because an entry whose `last_seen`
    /// moves forward can only expire later than this said it would. Cap eviction may leave it
    /// early, which costs one sweep that finds nothing and recomputes.
    next_expiry: Option<Instant>,
}

impl CacheState {
    /// Recomputes [`CacheState::next_expiry`] exactly, from every entry still in the table. One
    /// pass, and only ever from a sweep -- which has just made one anyway.
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

    /// The table in the codec's own shape. The family names are copied (a `Declarations` owns its
    /// keys); the descriptions, which are the bulk of it, are `Arc` clones.
    fn rebuild_seed(&mut self) {
        let mut seed = remote_write::Declarations::default();
        for (name, family) in &self.families {
            seed.insert(name.as_str(), family.kind, family.help.clone(), family.unit.clone());
        }
        self.seed = Arc::new(seed);
    }
}

/// `text` bounded to [`MAX_METADATA_TEXT_BYTES`], cut on a `char` boundary so the result is still a
/// string rather than a broken one, and whether it had to cut at all.
fn bounded_text(text: &Option<Arc<str>>) -> (Option<Arc<str>>, bool) {
    let Some(text) = text else { return (None, false) };
    if text.len() <= MAX_METADATA_TEXT_BYTES {
        // The overwhelmingly common path, and the reason this takes a reference: a description
        // within the bound is shared with the request that carried it, never copied.
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
    /// Expiry is checked per request rather than on a timer of its own -- this input has no clock
    /// task, and a receiver nothing is writing to has nothing to spend memory on either way -- but
    /// it is *checked*, not performed: [`CacheState::next_expiry`] says when the first entry could
    /// possibly go, so until then this is one comparison and a refcount bump. A pass over the whole
    /// table on every request would be a cost that scales with what is remembered rather than with
    /// the request, which is exactly the shape the seed is an `Arc` to avoid.
    fn seed(&self, now: Instant, telemetry: &Telemetry) -> Arc<remote_write::Declarations> {
        let mut state = self.lock();
        if state.next_expiry.is_some_and(|earliest| now > earliest) {
            self.sweep(&mut state, now, telemetry);
        }
        Arc::clone(&state.seed)
    }

    /// One pass, dropping every entry the TTL has run out on and recomputing the watermark from
    /// what is left. Allocation-free; the rebuild below it happens only if something actually went.
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

    /// Folds one request's own declarations in -- the newest statement about a family wins, and a
    /// *retype* is counted `logit.input.metadata_cache.replaced` -- then evicts back down to the
    /// cap.
    ///
    /// Returns before taking the lock when the request declared nothing, which is nearly every 1.0
    /// request: there is no entry to touch, nothing can have grown past the cap, and the alternative
    /// is a lock per request to learn an empty table.
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
        // Whether the *seed* has to be rebuilt -- which a re-declaration of what is already
        // remembered does not. Prometheus re-sends a family's metadata every `send_interval` from
        // every shard, so "identical to what is already there" is the common case, and rebuilding
        // for it would copy the whole table under the lock once a minute per shard.
        let mut changed = false;
        for (name, declaration) in declarations.iter() {
            // Bounded on the way in, not on the way out: what is remembered is what outlives the
            // request, and the request's own cap does not bound a table that keeps entries.
            let (help, help_cut) = bounded_text(&declaration.help);
            let (unit, unit_cut) = bounded_text(&declaration.unit);
            truncated += u64::from(help_cut) + u64::from(unit_cut);
            match state.families.get_mut(name) {
                Some(existing) => {
                    if existing.kind != declaration.kind {
                        // Two senders disagreeing about one family name, or one that changed its
                        // mind. Either way the newest statement is the one to keep -- the alternative
                        // is typing a live sender's series from a declaration nothing has repeated.
                        replaced += 1;
                        changed = true;
                    } else if existing.help != help || existing.unit != unit {
                        changed = true;
                    }
                    existing.kind = declaration.kind;
                    existing.help = help;
                    existing.unit = unit;
                    // Touched whether or not anything else moved: the TTL measures how long ago a
                    // sender last said this, and it just said it again.
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
        // Every entry this touched expires at `now + ttl` at the latest, which can only be later
        // than whatever the table already held -- unless it was empty, which is the case this is
        // here for.
        state.note_expiry(now, self.ttl);
        if changed {
            state.rebuild_seed();
            telemetry.gauge(METADATA_CACHE_SIZE, state.families.len() as f64, &[]);
        }
    }

    /// Evicts least-recently-seen families until at most `max_families` remain, in **one pass over
    /// the table** however many have to go -- `prometheus_out`'s `Registry::enforce_cap` one crate
    /// over, for its reasoning: being over the cap is the steady state the cap exists for, so a
    /// `while len() > max` loop calling `min_by` would re-scan and re-allocate every candidate once
    /// per eviction, exactly when cardinality is what is being diagnosed.
    ///
    /// The tie-break past `last_seen` is the family's own name, so which of two families declared
    /// in one request goes is a function of the data rather than of `Instant` resolution or map
    /// iteration order.
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
        // The watermark may now point at an entry that is gone -- the oldest are exactly the ones
        // evicted -- which costs one sweep that finds nothing and recomputes it.
        true
    }

    /// The lock, unpoisoned. Nothing here can panic while it is held -- the body is map operations
    /// on owned data -- and a receiver that stopped remembering metric types because one request
    /// panicked would be a worse failure than the one that caused it.
    fn lock(&self) -> std::sync::MutexGuard<'_, CacheState> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

/// `prometheus_in` in **bind mode**: a Prometheus remote-write receiver. See this module's doc
/// comment for the config table, the routes table, and every mapping decision; this type is the
/// accept loop and the request handler that implement them.
pub struct PrometheusReceiver {
    bind: String,
    /// The one path this receiver answers `POST`s on. An `Arc<str>` because every connection's
    /// service closure captures it and every request compares against it -- cloned per connection,
    /// never per request, and never re-allocated.
    path: Arc<str>,
    tls: Option<Arc<rustls::ServerConfig>>,
    /// Set by [`Input::bind`], taken back out by [`Input::run`] -- `Input::bind`'s own contract.
    /// `None` after a run, so a second run rebinds.
    listener: Option<tokio::net::TcpListener>,
    /// `None` -- the default -- means no idle timeout at all. See this module's "Shutdown and
    /// connection lifetime" doc section.
    idle_timeout: Option<Duration>,
    /// The one piece of state this receiver holds across requests, and `None` unless an operator
    /// asked for it -- `metadata_cache: {max_families: 0}`, and every config predating the field,
    /// leaves the stateless decode path exactly as it was. Shared by every connection, hence the
    /// `Arc`; see this module's "Metadata cache" doc section.
    metadata_cache: Option<Arc<MetadataCache>>,
    handshake_timeout: Duration,
    max_connections: usize,
    /// The empty `Resource` every batch this receiver builds carries, allocated once. A receiver
    /// observed a TCP connection from a sender that may be relaying for thousands of targets, so
    /// it has no target identity of its own to stamp -- this module's "Labels stay labels" doc
    /// section.
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

    /// The address actually bound, once [`Input::bind`] has run -- lets a test learn the
    /// OS-assigned port without a bind-drop-rebind race, exactly as `otlp_in`'s own does.
    pub fn local_addr(&self) -> Option<SocketAddr> {
        self.listener.as_ref().and_then(|l| l.local_addr().ok())
    }

    /// Turns on TLS termination for this listener (`bind_tls:` in config). Both ALPN protocols the
    /// auto builder can serve are advertised, so a TLS client's own negotiation picks the same one
    /// the plaintext path would otherwise have to sniff.
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
    /// it -- `idle_timeout:` in config, and off (`None`) when never called. Graph rule 53 rejects
    /// `Some(0s)` before it can reach here. Takes the `Option` rather than a bare `Duration`, like
    /// every other listener's own `with_idle_timeout`.
    pub fn with_idle_timeout(mut self, idle_timeout: Option<Duration>) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    /// Turns on the metadata cache -- `metadata_cache:` in config, whose defaults this takes
    /// verbatim. `max_families == 0` is the operator's "off", and leaves this receiver on the
    /// stateless decode path rather than building a table it would never put anything in. Graph
    /// rule 55 rejects a zero `ttl` before it can reach here.
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

    /// Test-only override of [`MAX_CONCURRENT_CONNECTIONS`] -- opening 1025 real connections to
    /// exercise the cap would be slow and flaky; this makes it reachable with two. `otlp_in`'s own
    /// test-only override, one listener over.
    #[cfg(test)]
    fn with_max_connections(mut self, max_connections: usize) -> Self {
        self.max_connections = max_connections;
        self
    }

    /// Test-only override of [`HANDSHAKE_TIMEOUT`], so a test can watch a silent connection
    /// actually be closed without a multi-second sleep.
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

    /// `otlp_in`'s accept loop, one listener over: a permit per connection acquired before any TLS
    /// accept, the handshake (or the plaintext first-byte peek) bounded inside the spawned task,
    /// and `hyper_util`'s auto builder serving HTTP/1.1 and h2c off the same socket. Copied rather
    /// than shared because the loop's telemetry, its handler and its dispatch are each listener's
    /// own; what *is* shared is the idle machinery it hands off to ([`crate::http`]).
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        self.bind().await?;
        let listener = self.listener.take().expect("bind() leaves a listener behind");
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(self.max_connections));
        // Built once outside the loop -- `TlsAcceptor::from` just wraps the `Arc<ServerConfig>`,
        // so cloning it per connection is an `Arc` clone, not a config rebuild.
        let tls_acceptor = self.tls.clone().map(tokio_rustls::TlsAcceptor::from);
        let live_connections = Arc::new(AtomicI64::new(0));
        let handshake_timeout = self.handshake_timeout;
        let idle_timeout = self.idle_timeout;
        let metadata_cache = self.metadata_cache.clone();
        loop {
            let (stream, peer) = listener.accept().await?;

            // Non-blocking (`try_acquire_owned`): at capacity the connection is closed immediately
            // rather than queued behind a permit that may never come, and closed *before* any TLS
            // accept -- remote-write has no in-band "try later" of its own to deliver, so there is
            // nothing to say and no reason to spend a handshake saying it. A sender that gets its
            // connection closed retries on its own queue, which is the protocol's own flow control.
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

                // Published from the read-modify-write's own return value, not a separate `load`:
                // `Telemetry::gauge` is last-write-wins per key, so two tasks interleaving an add
                // and a load would leave the stale one published until the next transition.
                let live = live_connections.fetch_add(1, Ordering::Relaxed) + 1;
                telemetry.gauge("logit.input.connections", live as f64, &[]);

                let result = match tls_acceptor {
                    // No first-byte peek on this arm: `acceptor.accept` is already waiting on this
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
                    // The plaintext arm's equivalent budget. `peek` is `recv(..., MSG_PEEK)`: it
                    // waits for the first byte to be *available* and consumes nothing, so the auto
                    // builder's own `ReadVersion` sniff still sees a pristine stream. A *clean*
                    // close before the first byte (`Ok(0)`) is a TCP health check, not a fault --
                    // the same call `otlp_in` and `crate::tcp` make.
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

/// Serves one already-accepted (and, on a TLS listener, already-handshaken) connection to
/// completion -- generic over the IO type so the plaintext and TLS cases share every line below
/// `run`'s own `tls_acceptor` branch, exactly as `otlp_in`'s twin does.
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
    // One tracker per connection, shared between the service (which stamps it as requests start
    // and finish) and the driver below (which reads it). The body-frame stall bound is
    // `idle_timeout` too: a connection with no idle bound configured gets no per-frame one either.
    let activity = Arc::new(Activity::new());
    let svc = service_fn({
        let activity = Arc::clone(&activity);
        let (sink, telemetry, diag) = (sink.clone(), telemetry.clone(), diag.clone());
        let (path, resource) = (Arc::clone(&path), Arc::clone(&resource));
        move |req| {
            // `enter` here rather than inside the returned future: hyper calls the service the
            // moment a request head is parsed, so the in-flight count rises then, not whenever the
            // future first happens to be polled.
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

/// One request, timed and counted. The outcome classification lives in [`write_response`]; this
/// wrapper exists so that *every* exit from it -- including the early rejections -- contributes
/// exactly one `logit.input.writes{class}` count and one `logit.input.write.duration` timing,
/// the way `tick`'s own `logit.input.scrapes`/`scrape.duration` pair does per scrape.
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
    let (class, response) = write_response(
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
    telemetry.count("logit.input.writes", 1.0, &[("class", class)]);
    telemetry.timing("logit.input.write.duration", started.elapsed(), &[]);
    Ok(response)
}

/// The routes table in this module's doc comment, in order, returning the
/// `logit.input.writes{class}` label alongside the response.
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
) -> (&'static str, http::Response<Full<Bytes>>) {
    if req.uri().path() != path {
        return ("not_found", text_response(None, StatusCode::NOT_FOUND, "not found"));
    }
    // `405 + Allow: POST`, where `otlp_in` answers `404` for a wrong method. Deliberate: this
    // receiver's sibling on the same kind pair -- `prometheus_out`'s exposition server -- already
    // answers `405` on `/metrics`, and matching it is more useful to an operator than matching the
    // unrelated input this accept loop was copied from.
    if req.method() != Method::POST {
        let mut response = text_response(
            None,
            StatusCode::METHOD_NOT_ALLOWED,
            "only POST is accepted on a remote-write endpoint",
        );
        response.headers_mut().insert(http::header::ALLOW, HeaderValue::from_static("POST"));
        return ("method", response);
    }

    // Both specs mandate Snappy *block* compression on every request; there is no identity mode to
    // fall back to, so a missing header is as unusable as a wrong one.
    let encoding = header_str(req.headers(), http::header::CONTENT_ENCODING);
    if !encoding.eq_ignore_ascii_case(remote_write::CONTENT_ENCODING_SNAPPY) {
        let message = format!(
            "unsupported Content-Encoding {encoding:?} -- remote-write is always \
             '{}' (block format)",
            remote_write::CONTENT_ENCODING_SNAPPY
        );
        diag.warn_throttled(
            "write_rejected",
            format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
        );
        return ("unsupported", text_response(None, StatusCode::UNSUPPORTED_MEDIA_TYPE, &message));
    }
    // Which version the body is, per request, from the request's own `Content-Type` -- no
    // configuration. An absent header is `""`, which no version claims, so it lands here rather
    // than defaulting to 1.0 the way `otlp_in` defaults an absent type to protobuf: 1.0's own spec
    // requires the header, and guessing would turn a 2.0 sender's misconfiguration into a wall of
    // protobuf decode errors instead of the `415` the spec has for exactly this.
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
        return ("unsupported", text_response(None, StatusCode::UNSUPPORTED_MEDIA_TYPE, &message));
    };

    // The helpers that attach 2.0's `-Written` headers take an `Option<Version>`, because the
    // rejections above happen before a version is known at all; from here on it is always known.
    let seen = Some(version);

    let limited = Limited::new(req.into_body(), MAX_REQUEST_BYTES);
    let compressed = match collect_with_stall_bound(limited, stall).await {
        Ok(bytes) => bytes,
        // A body that stopped arriving is the sender's clock, not its size. `drive_with_idle`
        // applies no deadline while a request is in flight, so without this bound a half-uploaded
        // request would hold its connection-limit permit forever; the connection closes once this
        // response is out rather than waiting for the whole-connection deadline.
        Err(BodyReadError::Stalled(stall)) => {
            activity.request_close();
            let message = format!("request body stalled for {stall:?}");
            diag.warn_throttled(
                "write_rejected",
                format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
            );
            return ("timeout", text_response(seen, StatusCode::REQUEST_TIMEOUT, &message));
        }
        Err(BodyReadError::Failed(err)) => {
            let message = body_read_error_message(err.as_ref());
            diag.warn_throttled(
                "write_rejected",
                format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
            );
            return ("oversize", text_response(seen, StatusCode::PAYLOAD_TOO_LARGE, &message));
        }
    };

    // The decompressed size is read out of the Snappy block header and checked *before* a byte is
    // expanded -- a compression bomb is rejected, never inflated.
    let declared = match snap::raw::decompress_len(&compressed) {
        Ok(declared) => declared,
        Err(err) => {
            let message = format!("invalid snappy body: {err}");
            diag.warn_throttled(
                "write_rejected",
                format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
            );
            return ("bad_request", text_response(seen, StatusCode::BAD_REQUEST, &message));
        }
    };
    if declared > MAX_REQUEST_BYTES {
        let message = format!(
            "decompressed request would be {declared} bytes, over the {MAX_REQUEST_BYTES}-byte \
             limit"
        );
        diag.warn_throttled(
            "write_rejected",
            format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
        );
        return ("oversize", text_response(seen, StatusCode::PAYLOAD_TOO_LARGE, &message));
    }
    let body = match snap::raw::Decoder::new().decompress_vec(&compressed) {
        Ok(body) => body,
        Err(err) => {
            let message = format!("invalid snappy body: {err}");
            diag.warn_throttled(
                "write_rejected",
                format_args!("prometheus_in: rejecting a request from {peer}: {message}"),
            );
            return ("bad_request", text_response(seen, StatusCode::BAD_REQUEST, &message));
        }
    };

    // Built per request rather than kept on the receiver: a connection's requests are served
    // concurrently, so a shared `&mut` decoder would need a lock on the hot path for no gain --
    // the decoder's own throttled diagnostics accumulate through the shared `Diagnostics` counts
    // regardless of how many decoders exist, and its telemetry is a clone of one registry handle.
    // `with_timestamp_marker(false)` is the one non-default: every remote-write sample carries a
    // timestamp, so its presence is no producer choice worth recording.
    let mut decoder = PrometheusDecoder::new()
        .with_timestamp_marker(false)
        .with_telemetry(telemetry.clone())
        .with_diagnostics(diag.clone());
    // The cache's lock is taken to build the seed and taken again below to learn, never held
    // across the decode -- a connection's requests are served concurrently and the decode is the
    // expensive part. With no cache configured this is the stateless `decode` verbatim.
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
            return ("bad_request", text_response(seen, StatusCode::BAD_REQUEST, &message));
        }
    };
    // Learned from what *this* request declared, never from the seed: a cache that refreshed
    // entries out of its own memory would never let one expire. A malformed request teaches
    // nothing, which is why this sits after the `400` above rather than beside the seed.
    if let Some(cache) = metadata_cache {
        cache.learn(&decoded.declarations, Instant::now(), telemetry);
    }

    // One batch per request, on an empty `Resource`, built by concatenating the timestamp groups
    // in ascending order -- this module's "Timestamps and timestamp groups" doc section.
    //
    // The samples-kept count is built here rather than read off `Decoded::samples`, because that
    // is the count the *assembler* accepted and the model mapping below runs after it: a series
    // whose point has no model kind (an empty histogram, a histogram whose cumulative bucket
    // counts decrease) is dropped here, having already been counted there. `-Written` is the one
    // thing 2.0 defines as a report of what the receiver *kept*, so it is every decoded series'
    // wire samples minus the ones belonging to series this mapping then dropped -- both sides
    // measured by the codec's own `wire_samples`, which reads the `Point` and so still sees the
    // `Option`s the wire had.
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
    // `saturating_sub` only because a panic is a poor way to learn that the callback fired for a
    // series the loop above did not count -- it cannot, since both walk the same `decoded.groups`.
    let written = total.saturating_sub(dropped);
    telemetry.count("logit.input.samples", written as f64, &[]);
    if !events.is_empty() {
        // **Before** the response is built, the ordering `otlp_in` uses: channel backpressure
        // delays the `204` and the sender's own queue throttles, which is remote-write's own
        // flow-control model working as designed rather than a stalled receiver.
        sink.send(EventBatch { resource, scope: None, events }).await;
    }
    ("ok", no_content(seen, written, decoded.exemplars))
}

/// `""` for an absent or non-ASCII header -- both are "this header said nothing this receiver can
/// use", and the caller's message quotes whatever came back.
fn header_str(headers: &HeaderMap, name: http::header::HeaderName) -> &str {
    headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or("")
}

/// `204 No Content`, plus 2.0's `-Written` report of what this receiver actually stored --
/// `samples` is the wire samples of the series that reached the fanout, never the assembler's own
/// accepted total. Native histograms are skipped, so the histogram count is always `0` -- an
/// honest report, not a placeholder.
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
/// partially-applied write from one that stored nothing. A rejection stored nothing, hence the
/// zeros; a 1.0 request (or one rejected before its version was even known) gets no headers at
/// all, since 1.0 defines none.
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

    /// [`counter_in`] for a gauge -- `Telemetry::gauge` records a `MetricKind::Gauge`, which that
    /// helper deliberately does not match (a counter read as a gauge would hide a spelling bug).
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

    // ---- bind mode: the remote-write receiver -------------------------------------------------
    //
    // One row of this module's routes table per test, both wire versions, driven over a real
    // socket with hand-written HTTP/1.1 -- `otlp_in`'s own `post_raw` shape, which needs no HTTP
    // client crate and lets a test hold a keep-alive connection open across requests (what the
    // idle-timeout and backpressure cases below are actually about).

    use logit_proto::prometheus::{
        FamilyType, MetricFamily, Point, PrometheusEncoder, Series, ATTR_TIMESTAMP, ATTR_TYPE,
    };

    /// A bound receiver plus the address it is listening on. `Input::bind` makes the port live
    /// before `run` is ever spawned, so there is no bind-drop-rebind race to lose.
    async fn bound_receiver(path: &str) -> (PrometheusReceiver, String) {
        let mut receiver = PrometheusReceiver::new("127.0.0.1:0", path);
        receiver.bind().await.expect("binding an ephemeral port should succeed");
        let addr = receiver.local_addr().expect("bind() leaves a real address behind").to_string();
        (receiver, addr)
    }

    /// Spawns `receiver`'s accept loop and returns the `Fanout` receiving end. `capacity` is the
    /// channel bound -- `1` with nothing draining it is how the backpressure test parks a handler
    /// inside `Fanout::send`.
    fn spawn_receiver(
        mut receiver: PrometheusReceiver,
        capacity: usize,
    ) -> mpsc::Receiver<Delivered> {
        let (tx, rx) = mpsc::channel(capacity);
        let sink = Fanout::new(vec![tx]);
        tokio::spawn(async move { receiver.run(sink).await });
        rx
    }

    fn snappy(body: &[u8]) -> Vec<u8> {
        snap::raw::Encoder::new().compress_vec(body).expect("compressing a test body never fails")
    }

    /// One remote-write request body: protobuf through W2's codec, then Snappy **block**
    /// compression -- exactly what a real sender puts on the wire.
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

    /// Unix nanoseconds on a whole-millisecond boundary -- remote-write timestamps are
    /// milliseconds, so anything finer would come back truncated and make an assertion about
    /// equality a statement about rounding instead.
    fn millis(ms: i64) -> i64 {
        ms * 1_000_000
    }

    /// The protocol headers a well-formed request carries, ready to splice into [`post_raw`].
    /// `Connection: close` so [`post_raw`]'s `read_to_end` terminates on the response rather than
    /// on a keep-alive connection's own lifetime -- a test that needs the connection kept alive
    /// uses [`keep_alive_write_headers`] instead.
    fn write_headers(version: remote_write::Version) -> String {
        format!("{}Connection: close\r\n", keep_alive_write_headers(version))
    }

    /// [`write_headers`] without the `Connection: close`, so HTTP/1.1's own default (keep-alive)
    /// applies and the *listener* is the only thing that can end the connection.
    fn keep_alive_write_headers(version: remote_write::Version) -> String {
        format!(
            "Content-Type: {}\r\nContent-Encoding: {}\r\n{}: {}\r\nUser-Agent: test\r\n",
            version.content_type(),
            remote_write::CONTENT_ENCODING_SNAPPY,
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

    /// Reads exactly one response head (through the blank line) off a keep-alive connection --
    /// `read_to_end` would block until the *connection* ends, which is the thing some of these
    /// tests are holding open on purpose. `None` on the deadline rather than a panic, so the
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

    /// The documented divergence from `otlp_in`, which answers `404` for a wrong method: this
    /// receiver matches its sibling `prometheus_out`'s exposition server instead.
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
            remote_write::CONTENT_ENCODING_SNAPPY
        );

        let response = post_raw(&addr, "/api/v1/write", &headers, &snappy(b"")).await;

        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
        assert!(response.contains("some.other.Message"), "got: {response}");
    }

    /// `otlp_in` reads an absent `Content-Type` as protobuf, for compatibility with clients that
    /// predate its JSON support. Remote-write has no such history and both specs require the
    /// header, so an absent one is the `415` the spec has for exactly this rather than a guess.
    #[tokio::test]
    async fn a_request_with_no_content_type_is_415() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        let headers = format!(
            "Content-Encoding: {}\r\nConnection: close\r\n",
            remote_write::CONTENT_ENCODING_SNAPPY
        );

        let response = post_raw(&addr, "/api/v1/write", &headers, &snappy(b"")).await;

        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
    }

    /// The compression-bomb bound: the decompressed size is read out of the Snappy block header
    /// and compared against `MAX_REQUEST_BYTES` *before* a byte is expanded, so this is rejected
    /// without the receiver ever holding the 5 MiB it would have become.
    #[tokio::test]
    async fn a_body_that_would_decompress_over_the_cap_is_413() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        // Highly compressible, so the *compressed* body stays far under the cap and only the
        // declared decompressed length can catch it -- which is the check under test.
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

    /// A body that decompresses fine but is not protobuf at all: a truncated varint, which no
    /// message can be. Sent as 2.0 so this also pins the `-Written` report a rejection owes a 2.0
    /// sender -- zeros, on a `4xx`, which is exactly what 2.0 asks for.
    ///
    /// A *valid 1.0* body under a 2.0 `Content-Type` is the more realistic mistake, and on this
    /// branch's base it still answers `204` with nothing stored (proto3 field numbers overlap
    /// enough for it to decode as an empty 2.0 `Request`). `rw/w2`'s own follow-up
    /// `fix(proto): bound remote-write decode and place its exemplars honestly` makes that a
    /// `CodecError`; once the lead syncs this stack onto it, add the case here -- a `V1`
    /// `request_body` posted with `Version::V2`'s headers, asserting `400`.
    #[tokio::test]
    async fn a_body_that_is_not_the_promised_message_is_400() {
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let _rx = spawn_receiver(receiver, 4);
        // `0x08` is field 1, varint -- with no varint after it.
        let truncated = snappy(b"\x08");

        let response =
            post_write(&addr, "/api/v1/write", remote_write::Version::V2, &truncated).await;

        assert!(response.starts_with("HTTP/1.1 400"), "got: {response}");
        // 2.0 wants the `-Written` report on a 4xx too -- zeros, because nothing was stored.
        assert!(
            response
                .to_ascii_lowercase()
                .contains(&format!("{}: 0", remote_write::HEADER_SAMPLES_WRITTEN)),
            "got: {response}"
        );
    }

    /// The timestamp-group rule end to end: one request carrying three samples of one series
    /// becomes **one** batch of three events, in ascending timestamp order, and none of them
    /// carries the `prometheus.timestamp` marker -- remote-write mandates a timestamp, so its
    /// presence is not the producer choice that marker records.
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
    /// `Resource` is empty. A receiver never touched the target those labels name, so it has no
    /// resource identity of its own to stamp -- the opposite call scrape mode makes, deliberately.
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

    /// What a receiver with **no** metadata cache does with Prometheus's own 1.0 sender, which
    /// ships `MetricMetadata` in *separate* requests: a request carrying only samples decodes as
    /// `Unknown` families. The samples themselves are exact. `metadata_cache:` is what closes
    /// this -- see the cache's own tests below, which are this test with a table behind it.
    #[tokio::test]
    async fn a_1_0_request_without_metadata_decodes_as_unknown_families() {
        use logit_proto::prometheus::generated::prometheus as pb1;
        use prost::Message;

        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let mut rx = spawn_receiver(receiver, 4);
        // Hand-built rather than round-tripped through `remote_write::encode`, which always writes
        // a `metadata[]` entry -- the shape under test is precisely the one that carries none.
        // Labels are sorted by byte order, as both specs require of a sender.
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

    /// An empty request is a `204` and nothing downstream -- a real sender's heartbeat write
    /// should not manufacture an empty batch for every component below this one to walk.
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
    /// delays the `204` and the sender's own queue throttles -- remote-write's flow-control model
    /// working as designed, which is only true if the ordering is this way round.
    #[tokio::test]
    async fn backpressure_delays_the_204_until_the_channel_drains() {
        use tokio::io::AsyncWriteExt;
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        // Capacity 1, nothing draining: the first request's batch fills it and the second's
        // `Fanout::send` parks.
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

    /// A histogram whose cumulative bucket counts *decrease* is not a cumulative histogram at all,
    /// so `families_to_events` drops the whole series after the assembler has already accepted its
    /// samples. The report has to follow the events, not the assembler: nothing reached the
    /// `Fanout`, so `Samples-Written` is `0` and `logit.input.samples` counts nothing -- otherwise
    /// the one header 2.0 defines as "what the receiver kept" would be claiming three samples this
    /// receiver threw away.
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
        // The decoder's own skip counter is where the loss is visible, which is the point of it.
        assert_eq!(
            counter_in(&events, "logit.input.metrics.skipped", ("reason", "non_monotonic_buckets")),
            Some(1.0)
        );
    }

    /// A well-formed classic histogram, for the other half of the same property: a single series
    /// that the wire spelled as several samples reports all of them, so the fix above is not
    /// simply "count events".
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

    /// A summary sent as quantiles alone -- no `_sum`, no `_count` -- is two wire samples, and the
    /// report has to say two. This is the case a count derived from the built `MetricRecord` could
    /// not get right: `logit_core::Summary`'s `sum`/`count` are plain numbers, so the model has
    /// forgotten that the wire omitted them, while the codec's `Point::Summary` still carries the
    /// `Option`s and `wire_samples` reads them.
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

    /// 1.0 spells a created timestamp as a `_created` sample of its own, which the codec counts and
    /// this receiver keeps (as `Series::created`) -- so both of the request's samples are reported.
    /// Hand-built, because `remote_write::encode` drops `created` on 1.0, 1.0 having no field for
    /// it: the shape under test only exists on the wire.
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
            // A `_created` sample only means anything once something says the family is a counter;
            // without metadata `requests_created` is just another untyped series.
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

    /// The `408` row of the routes table, and the whole reason `collect_with_stall_bound` was
    /// hoisted: `drive_with_idle` applies no deadline while a request is in flight, so a peer that
    /// sends a head and then stops mid-body would otherwise hold its connection-limit permit
    /// forever. `otlp_in`'s own stalled-body test, one listener over.
    #[tokio::test]
    async fn a_body_that_stops_arriving_is_408_and_closes_the_connection() {
        use tokio::io::AsyncWriteExt;
        let (receiver, addr) = bound_receiver("/api/v1/write").await;
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("receive", "prometheus_in", "listener");
        let receiver = receiver
            .with_telemetry(telemetry)
            // The per-frame body bound is `idle_timeout` too -- a listener with no idle bound
            // configured gets no per-frame one either.
            .with_idle_timeout(Some(Duration::from_millis(100)))
            // The grace `drive_with_idle` gives hyper to write the 408 out and close.
            .with_handshake_timeout(Duration::from_millis(200));
        let mut rx = spawn_receiver(receiver, 4);
        let body = request_body(
            &[vec![gauge_family("queue_depth", ("job", "api"), 1.0, millis(1))]],
            remote_write::Version::V1,
        );

        // A `Content-Length` promising the whole body, then half of it and silence. Sent
        // **keep-alive** -- no `Connection: close` -- so nothing but this listener's own
        // `Activity::request_close` can end the connection, which is what makes the close below an
        // assertion about the code under test rather than about the request this test wrote.
        let mut stalled = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let head = format!(
            "POST /api/v1/write HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{}\r\n",
            body.len(),
            keep_alive_write_headers(remote_write::Version::V1)
        );
        stalled.write_all(head.as_bytes()).await.unwrap();
        stalled.write_all(&body[..body.len() / 2]).await.unwrap();

        // `read_to_end` *completing* is the close: `Activity::request_close` ends the connection
        // once the 408 is out rather than leaving it to the whole-connection deadline, so this
        // both reads the response and proves the socket went away. `otlp_in`'s own shape.
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

    /// `otlp_in`'s idle-timeout test one listener over, and the reason the helpers were hoisted
    /// rather than copied: a pooled keep-alive connection that finished its write and went quiet
    /// gives its connection-cap permit back instead of holding it forever. Proven under
    /// `with_max_connections(1)`, so the follow-up request can only be served if the permit
    /// genuinely came back -- and the close is counted, never diagnosed.
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

        // One complete write, keep-alive, so the connection settles idle inside hyper with the
        // first-byte peek long behind it -- the idle clock is the only thing that can end it.
        let mut keep_alive = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let request = format!(
            "POST /api/v1/write HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\nContent-Type: \
             {}\r\nContent-Encoding: {}\r\n\r\n",
            body.len(),
            remote_write::Version::V1.content_type(),
            remote_write::CONTENT_ENCODING_SNAPPY
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

    /// A connection that completes its TCP connect and then says nothing must not pin a
    /// connection-cap permit forever -- the plaintext arm's first-byte peek is what bounds it.
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

    /// `recv_batch`'s awaiting twin -- the receiver answers on a spawned task, so a batch may not
    /// have landed by the time the response has been read.
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

    /// A `tokio-rustls` client trusting `testdata/tls/ca.pem`, presenting no client certificate --
    /// `otlp_in`'s own `tls_connector` for the no-mTLS case.
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
    // The 1.0 shape these are all about: one request declares a family and carries no samples,
    // later requests carry its samples and declare nothing. `remote_write::encode` never produces
    // either half on its own -- it writes a `metadata[]` entry for every family it has series for
    // -- so the wire messages here are hand-built, exactly as `a_1_0_request_without_metadata_…`
    // builds its own.

    use logit_proto::prometheus::generated::io::prometheus::write::v2 as pb2;
    use logit_proto::prometheus::generated::prometheus as pb1;
    use prost::Message as _;

    /// A 1.0 request that declares `foo` and carries nothing else -- what Prometheus's sender
    /// writes on its `metadata_config` schedule.
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

    /// The whole point of the feature, end to end over a socket: a metadata-only request teaches
    /// the receiver what `foo` is, and the samples-only request that follows -- carrying no
    /// metadata whatsoever, which is every request a real 1.0 sender writes -- decodes as one
    /// typed `Histogram` with its help and unit, instead of three unrelated `unknown` series.
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

    /// `max_families: 0` is not a zero-size cache but no cache: the receiver stays on the
    /// stateless decode path, so the same pair of requests decodes exactly as it did before this
    /// feature existed -- and nothing is counted, since there is nothing there to count.
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

    /// A request's own metadata beats the remembered entry, per family name: the sender said `foo`
    /// is a gauge *now*, however long the receiver has been treating it as a histogram -- and the
    /// retype is counted, since a steady stream of them is two senders disagreeing about one name.
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

    /// 2.0's inline `Metadata` fills the same table, so a fleet migrating version by version gets
    /// its 1.0 senders typed by its 2.0 ones -- the family a 2.0 series declares is the base its
    /// type implies (`foo_bucket` under `HISTOGRAM` declares `foo`), which is exactly the key a
    /// 1.0 `metadata[]` entry uses.
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
    // `Instant` is a parameter of `seed`/`learn` rather than read inside them, so the expiry and
    // eviction rules are testable without sleeping through a real TTL -- `prometheus_out`'s
    // exposition registry takes the same shape for the same reason.

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

    /// A family nothing has re-declared within the TTL stops being typed -- its next samples come
    /// back `unknown` -- and the drop is counted. That is the bound working: a remembered type
    /// nothing has reasserted is a guess about a series that may no longer exist.
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

    /// One request declaring several families over the cap evicts them all in one pass, and the
    /// tie-break past `last_seen` is the family's own name -- so which of two families declared in
    /// the same request survives is a function of the data, not of hash order.
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

    /// Re-declaring a family the same way is not a retype: `replaced` is the counter an operator
    /// watches when model kinds look wrong, and a 1.0 sender repeating itself once a minute
    /// forever must not move it.
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

    /// The federation case the cache must not make worse: a Prometheus relaying someone else's
    /// series declares them `UNKNOWN`, which is the metadata enum's "no type given". That must not
    /// overwrite a remembered `HISTOGRAM` -- if it did, the real sender's `_bucket`/`_sum`/`_count`
    /// would come apart every time the federating one wrote. `UNKNOWN` is not learned at all
    /// (`remote_write::decode_v1`), so there is nothing to replace with.
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
        // Its own sample survives -- the remembered histogram gives way rather than rejecting it.
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

    /// What is remembered is what outlives the request, and the 4 MiB request cap does not bound a
    /// table that keeps entries: without this, 10 000 individually-legal metadata-only requests
    /// each carrying a multi-megabyte `# HELP` would take the process down while
    /// `metadata_cache.size` read a healthy 10 000. The description is cut to
    /// [`MAX_METADATA_TEXT_BYTES`] on a `char` boundary -- so the survivor is still a string -- and
    /// the cut is counted.
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

    /// Prometheus re-sends a family's metadata every `send_interval`, from every shard, so
    /// "identical to what is already remembered" is the common write. Rebuilding the seed for it
    /// would copy the whole table under the lock on every one -- so the seed handed out must be the
    /// *same* `Arc`, which is the only way to observe that no rebuild happened.
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

    /// The expiry sweep is checked per request, not performed: until the earliest entry could
    /// possibly have expired there is nothing to find, and walking the table anyway would be a
    /// per-request cost that scales with what is remembered. Only the sweep's own counter can see
    /// this -- one that finds nothing leaves no counter and no rebuild behind.
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

        // Past it, once -- and the watermark it recomputes is `None`, the table now being empty.
        let seed = cache.seed(start + Duration::from_secs(601), &telemetry);
        assert_eq!(cache.sweeps.load(Ordering::Relaxed), 1);
        assert!(seed.is_empty());
        let _ = cache.seed(start + Duration::from_secs(3_600), &telemetry);
        assert_eq!(cache.sweeps.load(Ordering::Relaxed), 1, "an empty table has nothing to sweep");
    }
}
