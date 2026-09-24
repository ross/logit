//! Config types for `logit`.
//!
//! Every type here derives `Serialize`, `Deserialize`, and `JsonSchema` together, so the published
//! JSON Schema (`logit schema`, `schema/logit.schema.json`) cannot drift from what the binary
//! accepts (ADR `config-yaml-jsonschema`). YAML parsing belongs to `logit-cli`; this crate only
//! defines the shape. Every `///` on a config type or field renders into that schema as the
//! field's description, so write them for an operator configuring `logit`.
//!
//! Config is one flat graph of named [`Component`]s. A component's `sources` name the other
//! components it reads from; its `type`-tagged [`ComponentKind`] fixes its arity (a listener has
//! none, a sink has at least one and is never itself a source, a transform has both). Resolving
//! that graph (cycle detection, arity checks, topological ordering) is `logit-pipeline`'s job,
//! not this crate's.

use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct Config {
    #[serde(default)]
    #[schemars(schema_with = "non_empty_components_schema")]
    pub components: HashMap<String, Component>,
    /// The readiness/liveness HTTP endpoint. Off unless `bind` is set. Process-level: one admin
    /// server per `logit run`, not per component.
    #[serde(default)]
    pub admin: AdminConfig,
}

/// The `admin:` block. Every field defaults, so omitting the block leaves the admin server off.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct AdminConfig {
    /// `host:port` to serve `/readyz` and `/healthz` on. Omitted (the default) means off. No TLS:
    /// bind this to loopback or a pod-local address, not to a network beyond the process's own.
    pub bind: Option<String>,
}

fn non_empty_components_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = HashMap::<String, Component>::json_schema(generator);
    if let Schema::Object(schema) = &mut schema {
        schema.object().min_properties = Some(1);
    }
    schema
}

/// One node in the pipeline's component graph. `sources` names the components this one reads
/// events from: empty for a listener, required for everything else. Which arity is legal depends
/// on `type` and is checked by `logit validate`, not by this schema.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Component {
    #[serde(default)]
    pub sources: Vec<String>,
    /// The `target` components this component directs events into, in slot order. Legal only on
    /// `lua`/`lua_file`; a `route` derives its targets from its `routes:` values, and a non-empty
    /// list on any other kind is rejected. Every id must name a `target` component, and none may
    /// repeat.
    #[serde(default)]
    pub targets: Vec<String>,
    /// Per-sink delivery buffer. Meaningful only on a sink: a non-default block on any other kind
    /// is rejected.
    #[serde(default)]
    pub buffer: BufferConfig,
    /// Per-listener receive queue and batch assembly. A datagram listener (`collectd_in`, and
    /// `statsd_in`/`syslog_in`/`graphite_in` under `transport: udp`) accepts every field; the same
    /// three under `transport: tcp`, `tail_in`, and `docker_in` accept only the batch-assembly
    /// fields and `shutdown_grace`; a non-default block on any other kind (`otlp_in`, `logit_in`,
    /// `prometheus_in`, `internal`, and `generate_in` included) is rejected.
    #[serde(default)]
    pub receive: ReceiveConfig,
    #[serde(flatten)]
    pub kind: ComponentKind,
}

/// One `kv_metrics` entry: a metric `name`, an optional source `field`, and an optional `unit`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MetricSpec {
    /// The metric's measurement name. Must be non-empty.
    pub name: String,
    /// The attribute to read this metric's value from. Omitted means "+1 per event" for a counter
    /// or "set to 1" for a gauge; a distribution entry must name one. The name is literal:
    /// `field: http.status` reads the attribute named `http.status`, never a `status` key nested
    /// under `http`. Nested fields are not addressable.
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// One value a `set` component stamps onto an attribute or a resource attribute. YAML's own
/// scalar types decide the variant, so `resource: {service.name: nginx, retries: 3, ratio: 0.5,
/// sampled: true}` reads as written, with no type tag needed.
// `I64` must stay ordered before `F64`: serde tries untagged variants in declaration order and
// keeps the first that parses, and `3` parses as both, so the reverse order would turn every
// whole-number scalar into a float. `Bool`/`Str` never also parse as a number, so their position
// doesn't matter.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum SetValue {
    Bool(bool),
    I64(i64),
    F64(f64),
    Str(String),
}

/// A rewrite `keep_values` applies to a field's value before testing it against `allow`. A list,
/// so further steps can be added beside `lower`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NormalizeStep {
    /// ASCII-lowercase a string value, bytewise. Not Unicode case folding.
    Lower,
}

/// What `json` does with a message that is not valid UTF-8. `reject` (the default) fails the
/// parse: the event passes through untouched with one throttled `parse_failure` diagnostic.
/// `replace` retries the failed parse on a copy with every invalid sequence replaced by U+FFFD;
/// a valid line's cost is unchanged. Use `replace` for nginx's `escape=json`, which passes high
/// bytes (0x80 and above) through raw, so one client with a Latin-1 `User-Agent` would otherwise
/// lose the whole access line.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum JsonInvalidUtf8 {
    #[default]
    Reject,
    Replace,
}

/// Which top-level attributes `flatten` expands: the keyword `all` or `none`, or a list of
/// literal top-level attribute names. A named entry is an attribute name, never a path into a
/// nested value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum FlattenFields {
    Keyword(FlattenKeyword),
    Named(Vec<String>),
}

/// The blanket modes of `FlattenFields`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlattenKeyword {
    /// Expand every nested attribute (or resource attribute). The default for `attributes`.
    All,
    /// Expand nothing. The default for `resource`.
    None,
}

/// One field's clamp, under `keep_values`' `resource`/`attributes` maps.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct ValueAllowList {
    /// Rewrites applied in order before the `allow` test, and written back to the event when the
    /// result is allowed. Empty (the default) means no normalization. A repeated step is rejected.
    #[serde(default)]
    pub normalize: Vec<NormalizeStep>,
    /// The permitted values. Comparison coerces across numeric representations: a configured
    /// `200` matches an integer, float, or string `200`. Must be non-empty, and every number must
    /// be finite. Under `normalize: [lower]`, a string literal here must already be
    /// ASCII-lowercase, or it could never match.
    pub allow: Vec<SetValue>,
    /// What a value outside `allow` becomes. Absent (the default) removes the attribute instead.
    /// The same literal rules as `allow` apply: a number must be finite, and under
    /// `normalize: [lower]` a string must be ASCII-lowercase.
    #[serde(default)]
    pub other: Option<SetValue>,
}

/// One entry of `http_access`'s `routes` list: either a `builtin` set, or a `match` regex with
/// the literal `route` it assigns. Never both, and never a mix; `logit validate` names which half
/// is missing or which key is extra. `match` is tested unanchored (unless the pattern anchors
/// itself) against the capped `url.path`; `route` is written verbatim, never from a capture
/// group, so every `http.route` value comes from config.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HttpRouteRule {
    /// A named built-in route set, expanded in place at this position in the list. A set may
    /// appear once.
    #[serde(default)]
    pub builtin: Option<HttpRouteSet>,
    /// A regex over the capped `url.path`, compiled by `logit validate`. Must be non-empty.
    #[serde(default, rename = "match")]
    pub pattern: Option<String>,
    /// The literal `http.route` value a matching `match` assigns. Must be non-empty.
    #[serde(default)]
    pub route: Option<String>,
}

/// The built-in route sets an `HttpRouteRule` can name, each mapping to one fixed route value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HttpRouteSet {
    /// Static files by extension (`.css`, `.js`, `.png`, `.woff2`, ...): `/{asset}`. Not
    /// `json`/`xml`/`txt`/`csv`, which are routinely API responses.
    Assets,
    /// `/.well-known/*`, `robots.txt`, `favicon.ico`, sitemaps and their kin: `/{well-known}`.
    WellKnown,
    /// Health, readiness, and status endpoints (`/healthz`, `/readyz`, `/metrics`,
    /// `/nginx_status`, ...): `/{probe}`.
    Probes,
}

/// One entry of `http_access`'s `user_agent_rules`: a regex over the uncapped
/// `user_agent.original` (the identifying token of a spoofed UA is often at its tail) and the
/// literal `user_agent.class` it assigns. Rules are tried in order, before the built-in table. A
/// `class` of `crawler` or `scanner` also writes `user_agent.synthetic.type: bot`, as the
/// built-in classes do. `match` is compiled by `logit validate`; an empty `match` or `class` is
/// rejected.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct UserAgentRule {
    #[serde(rename = "match")]
    pub pattern: String,
    pub class: String,
}

/// `http_access`'s `forwarded` block. When present, `client.address` is overwritten with the
/// first hop of `http.request.header.x-forwarded-for`. All-or-nothing: there is no trusted-proxy
/// list or hop count. `trust: false` is rejected; omit the block instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ForwardedConfig {
    #[serde(default = "default_true")]
    pub trust: bool,
}

/// `ForwardedConfig::trust`'s default.
fn default_true() -> bool {
    true
}

/// Every field `http_access` caps, with its default character limit. A `max_length` key must name
/// one of these. Limits are characters, not bytes: a cap cuts at a char boundary, so a capped
/// value stays valid UTF-8.
pub const CAPPED_FIELDS: &[(&str, usize)] = &[
    ("url.path", 256),
    ("url.query", 256),
    ("user_agent.original", 256),
    ("http.request.header.referer", 256),
    ("server.address", 253),
    ("client.address", 128),
    ("network.peer.address", 128),
    ("http.request.header.x-forwarded-for", 128),
    ("upstream.address", 128),
    ("user.name", 128),
    ("http.request.method_original", 32),
    ("cache.status", 32),
    ("url.scheme", 16),
    ("network.protocol.version", 8),
    ("http.termination_state", 8),
];

/// Which metric kind a `GenerateMetric` produces: `sum` for a counter, `gauge` for a level, and
/// `distribution` for the sketch-merging path (emitted as raw samples, the shape a real listener
/// produces, never pre-sketched).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GenerateMetricKind {
    /// A delta counter. The default, and the cheapest shape to generate.
    #[default]
    Sum,
    Gauge,
    Distribution,
}

/// The metric `generate_in` stamps onto every event it generates, when `event.metric` is set.
/// One metric per event: a scenario that needs more uses a `set`/`kv_metrics` stage downstream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct GenerateMetric {
    /// The metric's name, a template like every other `generate_in` string field, with one
    /// narrowing: only `{seq%N}`, never a bare `{seq}`. A metric name is interned and never
    /// freed, so an unbounded name would leak one entry per generated event. `{seq%N}` is how a
    /// scenario generates a wide metric-name cardinality. Required and non-empty.
    pub name: String,
    /// Which metric kind to produce. Defaults to `sum`.
    #[serde(default)]
    pub kind: GenerateMetricKind,
    /// The value carried on every generated point. Constant, so the number measures the pipeline
    /// rather than the generator's arithmetic. Defaults to `1`. Must be finite.
    #[serde(default = "default_generate_metric_value")]
    pub value: f64,
}

/// The event template `generate_in` renders per generated event. Every field defaults, so an
/// omitted `event:` block generates the cheapest event there is: a timestamp, the configured
/// resource, and nothing else, which is what a "runtime floor" scenario measures.
///
/// `log`, every value in `attributes`, and `metric.name` are templates: `{seq}` renders the
/// generator's 0-based event counter and `{seq%N}` renders it modulo `N`, the knob that gives a
/// scenario a chosen attribute or series cardinality. `{{`/`}}` write a literal brace, which a
/// JSON log body needs. An unknown placeholder is rejected. A field with no placeholder is
/// rendered once and shared by every event, so use placeholders for cardinality, not decoration:
/// each one costs a rendering and a copy per event.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct GenerateEvent {
    /// The generated log body, as a template. Omitted means the event carries no log record (a
    /// metrics-only scenario).
    pub log: Option<String>,
    /// Event attributes: literal keys, templated values. An empty key is rejected.
    pub attributes: std::collections::BTreeMap<String, String>,
    /// The metric to stamp on every event. Omitted means the event carries no metrics (a
    /// logs-only scenario).
    pub metric: Option<GenerateMetric>,
}

/// Which OTLP transport a component speaks. Both carry identical protobuf payloads and differ
/// only in framing and endpoint shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    /// OTLP/HTTP, protobuf body, one POST per signal. The default, and what an
    /// `http://host:4318`-shaped endpoint implies.
    #[default]
    Http,
    /// Unary gRPC over HTTP/2: plaintext for an `http://`/`grpc://` endpoint, TLS for `https://`.
    /// `otlp_out`'s `tls:` block tunes the TLS case.
    Grpc,
}

/// Client-side TLS tuning for a sink or scrape client. On `otlp_out`, `prometheus_out`, and
/// `prometheus_in`, TLS itself is selected by the endpoint's `https://` scheme and this block
/// only tunes an already-TLS connection: a non-default block under a plain `http://`/`grpc://`
/// endpoint is rejected rather than ignored. On `logit_out`, `syslog_out`, and `statsd_out`,
/// whose endpoint is a bare `host:port`, the block's presence is what turns TLS on, so even an
/// empty `tls: {}` means TLS with the bundled Mozilla roots.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TlsClientConfig {
    /// PEM bundle of CA certificates to trust instead of the bundled Mozilla root set. A relative
    /// path resolves against the config file's directory. A plain string, so `!env` works on it.
    #[serde(default)]
    pub ca_file: Option<String>,
    /// Client certificate chain (PEM) presented for mutual TLS. Requires `key_file`.
    #[serde(default)]
    pub cert_file: Option<String>,
    /// Private key (PEM, PKCS#8/PKCS#1/SEC1) for `cert_file`. Requires `cert_file`.
    #[serde(default)]
    pub key_file: Option<String>,
    /// Disables server-certificate verification. The connection is still encrypted but accepts
    /// any certificate the peer presents, self-signed or otherwise, and `logit` logs a warning at
    /// startup. For a throwaway or pre-production endpoint only. Rejected together with
    /// `ca_file`.
    #[serde(default)]
    pub insecure_skip_verify: bool,
}

impl TlsClientConfig {
    /// `true` if every field is at its default, which is how validation tells a set `tls:` block
    /// from an omitted one.
    pub fn is_empty(&self) -> bool {
        self.ca_file.is_none()
            && self.cert_file.is_none()
            && self.key_file.is_none()
            && !self.insecure_skip_verify
    }
}

/// Server-side TLS for a listener. Its presence turns TLS on for that listener and makes it
/// required: there is no plaintext fallback. A relative path resolves against the config file's
/// directory, and every field accepts `!env`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TlsServerConfig {
    /// Certificate chain (PEM) this listener presents to every client.
    pub cert_file: String,
    /// Private key (PEM, PKCS#8/PKCS#1/SEC1) for `cert_file`.
    pub key_file: String,
    /// PEM bundle of CAs. When set, every connecting client must present a certificate chaining
    /// to one of them (mutual TLS). Absent, any client is accepted once the TLS handshake
    /// completes.
    #[serde(default)]
    pub client_ca_file: Option<String>,
}

/// Whether `otlp_out` gzips its request bodies, on both transports (HTTP `Content-Encoding:
/// gzip`; gRPC's per-message compressed flag plus `grpc-encoding: gzip`). Default `none`. This
/// only compresses what `otlp_out` sends; it never advertises accepting a compressed response.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OtlpCompression {
    #[default]
    None,
    Gzip,
}

/// A signal an event's payload may carry, in OTLP's vocabulary. `traces` corresponds to the
/// event's span.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Signal {
    Logs,
    Metrics,
    Traces,
}

/// `has_signal`'s matching rule. `any_of` forwards an event carrying at least one listed signal,
/// untouched, even if it also carries an unlisted one. `only` also requires the event carry
/// nothing outside `signals`: a mixed event with an unlisted signal is dropped, not trimmed
/// (`keep_signals` trims; `has_signal` never mutates an event).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum MatchMode {
    #[default]
    AnyOf,
    Only,
}

/// What one `route` reads from each event. Written `by: {provenance: origin}`, `by: {attribute:
/// stream}`, or `by: {resource: service.name}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RouteBy {
    Provenance(ProvenanceField),
    Attribute(String),
    Resource(String),
}

/// Which half of a batch's provenance a `route` switches on: `origin` is the component that
/// created the batch, `previous` the one that most recently handled it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceField {
    Origin,
    Previous,
}

/// What `sample` hashes to reach its keep/drop verdict. Written `key: trace_id`, `key:
/// {attribute: request_id}`, or `key: {resource: service.name}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SampleKey {
    /// The event's application trace id: the span's `trace_id` if it carries a span, else the log
    /// record's trace reference (what `trace_context` or `otlp_in` sets). Hashed as its 32
    /// lowercase hex characters, so it agrees with `{attribute: ...}` on the same id left as a
    /// hex string.
    TraceId,
    /// A top-level event attribute, named literally (never a path).
    Attribute(String),
    /// A resource attribute: every event of one resource gets the same verdict.
    Resource(String),
}

/// What `sample` does with an event its configured `key:` isn't on: no span or log trace
/// reference, no such attribute, or a null, array, or map value there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SampleMissing {
    /// Draw at `rate` as if no key were configured (the default).
    #[default]
    Random,
    /// Forward it.
    Keep,
    /// Drop it.
    Drop,
}

/// `sample`'s `always_keep:` override: an event carrying the named field, and with `value:`
/// carrying it with that value, is kept whatever the rate. Name one of `attribute`/`resource`,
/// never both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SampleOverride {
    /// A top-level event attribute to look for, named literally.
    #[serde(default)]
    pub attribute: Option<String>,
    /// A resource attribute to look for, named literally.
    #[serde(default)]
    pub resource: Option<String>,
    /// The value the field must carry, compared under `has_attributes`' equality rules (numeric
    /// coercion across integer/float/string, none for booleans: `value: true` does not match a
    /// logfmt `"true"` string). Absent means any value. Must be finite.
    #[serde(default)]
    pub value: Option<SetValue>,
}

/// Per-signal HTTP path overrides for `otlp_out` (`paths:` in config). An omitted field uses the
/// OTLP-standard default (`/v1/logs`, `/v1/metrics`, `/v1/traces`). A path prefix belongs on
/// `endpoint` itself: `endpoint: http://host/otlp` already yields `/otlp/v1/logs`. `protocol:
/// http` only; gRPC method names are fixed by the service definitions, so a non-empty `paths:`
/// under `protocol: grpc` is rejected.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct OtlpPaths {
    #[serde(default)]
    pub logs: Option<String>,
    #[serde(default)]
    pub metrics: Option<String>,
    #[serde(default)]
    pub traces: Option<String>,
}

impl OtlpPaths {
    /// `true` if every field is `None`.
    pub fn is_empty(&self) -> bool {
        self.logs.is_none() && self.metrics.is_none() && self.traces.is_none()
    }
}

/// `internal`'s `span_sample_rate` default, taken from `logit-core` so the two crates can't
/// drift. The one place `logit-config` depends on `logit-core`.
fn default_span_sample_rate() -> f64 {
    logit_core::DEFAULT_SPAN_SAMPLE_RATE
}

/// A component's kind, tagged by `type` in config. Every protocol kind is suffixed `_in`/`_out`,
/// so a listener and a sink for one protocol never collide on a tag; transform kinds take no
/// suffix.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ComponentKind {
    /// statsd / DogStatsD-style tagged metrics, over UDP (the default) or TCP.
    ///
    /// Under `transport: tcp` a message is one LF-delimited line; there is no `framing:` field
    /// and no octet-counted alternative, because a statsd line may begin with a digit. A line
    /// longer than 64 KiB is dropped and counted (`logit.input.frames.dropped{reason="oversize"}`);
    /// the connection stays open and the next line still decodes.
    ///
    /// `tls:` turns TLS on and makes it required: there is no plaintext fallback on a TLS
    /// listener. It applies to `transport: tcp` only; `tls:` under `transport: udp` is rejected.
    /// Plain statsd clients have no TLS of their own, so this is for a `logit`-to-`logit` or
    /// stunnel-shaped relay hop.
    ///
    /// A TCP listener has no receive queue (the connection's own flow control is the
    /// backpressure), so `receive:`'s queue fields (`max_datagrams`, `max_bytes`, `overflow`,
    /// `receive_buffer_bytes`, `read_batch`) are rejected on one. Its batch-assembly fields
    /// (`batch_max_events`, `batch_max_bytes`, `batch_flush_interval`) and `shutdown_grace` apply
    /// per connection: N live connections can hold up to N times `batch_max_events` in flight.
    StatsdIn {
        bind: String,
        #[serde(default)]
        transport: StatsdTransport,
        /// Terminates TLS on this listener when present; plaintext when omitted. Requires
        /// `transport: tcp`.
        #[serde(default)]
        tls: Option<TlsServerConfig>,
        /// How long one connection has, per pre-message phase, before this listener closes it
        /// and frees its connection-cap slot: the TLS accept when `tls:` is set, then the wait for
        /// the connection's first byte. Each phase gets its own budget, so a silent TLS
        /// connection costs up to twice this value. Defaults to `5s`; `0s` is rejected.
        /// `transport: tcp` only: a non-default value under `transport: udp` is rejected.
        ///
        /// Not an idle timeout. Once a connection has sent its first byte, the gap before its
        /// next line is bounded by `idle_timeout` if set, and unbounded otherwise.
        #[serde(default = "default_handshake_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        handshake_timeout: Duration,
        /// How long one connection may stay quiet before this listener closes it and frees its
        /// connection-cap slot. Off unless set: with no value, a connection that sent one line
        /// and then went silent holds its slot indefinitely. `0s` is rejected; omit the field to
        /// disable. `transport: tcp` only: any value under `transport: udp` is rejected.
        ///
        /// Recommended wherever steady traffic is expected: a connection quiet for longer than
        /// this is an anomaly (a dead peer, a half-open socket, a slow-loris), and closing it
        /// returns the slot. Set it well above the sender's longest normal gap (several
        /// `batch_flush_interval`s, say). Leave it unset for sparse or bursty senders, and think
        /// twice on plaintext transports, where the sender cannot detect the close before its
        /// next write. A statsd client flushing on a fixed interval is the easy case; one that
        /// only emits when its process sees traffic is not.
        ///
        /// The clock runs only while this listener is waiting on the peer's socket, and resets on
        /// bytes read from the peer and on this listener handing an accumulated batch downstream.
        /// Time blocked on a full downstream never counts, so a stalled pipeline cannot make a
        /// busy connection look idle.
        ///
        /// An idle close is policy, not a fault: complete buffered frames are flushed downstream
        /// first (`logit.component.receive.flushed{reason="closed"}`), a buffered partial frame is
        /// counted `logit.input.frames.dropped{reason="truncated"}`, and the close is counted
        /// `logit.input.connections.closed{reason="idle"}`, never diagnosed as a
        /// `connection_error`.
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        idle_timeout: Option<Duration>,
    },
    /// collectd's binary `network` plugin protocol over UDP.
    ///
    /// `bind` is an ordinary `host:port`; collectd's own default port is `25826`. When the
    /// address is a multicast group (collectd's defaults are `239.192.74.66` and
    /// `ff18::efc0:4a42`), the listener sets `SO_REUSEADDR`, binds the unspecified address on
    /// that port, and joins the group on the default interface. There is no `multicast:` field;
    /// the address says it. Such a listener also accepts unicast datagrams sent to that port from
    /// any source: joining a group is additive, not a filter.
    ///
    /// `types_db` names zero or more collectd `types.db` files (relative paths resolve against
    /// the config file's directory), read at startup and merged in order, later files overriding
    /// earlier ones. They supply data-source names only: a value list whose type resolves with a
    /// matching data-source count and kinds is named `<plugin>.<type>.<ds_name>` rather than
    /// `<plugin>.<type>.<i>` (a single-data-source list is `<plugin>.<type>` either way). A
    /// missing or unparseable file fails startup. Names never change what `collectd_out` puts
    /// back on the wire; it re-encodes from the `collectd.*` attributes. collectd's own
    /// `types.db` is GPL-licensed and not shipped with `logit`; point this at the installed copy.
    CollectdIn {
        bind: String,
        #[serde(default)]
        types_db: Vec<PathBuf>,
    },
    /// Graphite/Carbon metric ingress: carbon's plaintext line protocol or its pickle batch
    /// protocol, over TCP or UDP.
    ///
    /// `bind` is an ordinary `host:port`; carbon's own plaintext port is `2003` and its pickle
    /// port is `2004`. There is no `prefix:`/`template:` field and no `graphite.*` attribute
    /// namespace: a datapoint's dotted path is the metric name and its `;k=v` tags are event
    /// attributes, so a `lua`/`set` stage that renames the metric renames the wire path.
    ///
    /// `transport: tcp` (the default, carbon's own) runs an accept loop with no receive queue
    /// (TCP's own flow control is the backpressure), so only `receive:`'s batch-assembly and
    /// `shutdown_grace` fields apply to it. `transport: udp` runs the datagram listener and takes
    /// the whole `receive:` block. `protocol: pickle` requires `transport: tcp`: carbon's 4-byte
    /// length prefix has no meaning in a self-delimiting datagram.
    ///
    /// `tls:` turns TLS on and makes it required: there is no plaintext fallback on a TLS
    /// listener. It applies to `transport: tcp` only; `tls:` under `transport: udp` is rejected.
    /// Carbon senders have no TLS of their own, so this is for a `logit`-to-`logit` or
    /// stunnel-shaped relay hop.
    GraphiteIn {
        bind: String,
        #[serde(default)]
        transport: GraphiteTransport,
        #[serde(default)]
        protocol: GraphiteProtocol,
        /// Terminates TLS on this listener when present; plaintext when omitted. Requires
        /// `transport: tcp`.
        #[serde(default)]
        tls: Option<TlsServerConfig>,
        /// How long one connection has, per pre-message phase, before this listener closes it
        /// and frees its connection-cap slot: the TLS accept when `tls:` is set, then the wait for
        /// the connection's first byte. Each phase gets its own budget, so a silent TLS
        /// connection costs up to twice this value. Defaults to `5s`; `0s` is rejected.
        /// `transport: tcp` only: a non-default value under `transport: udp` is rejected.
        ///
        /// Not an idle timeout. Once a connection has sent its first byte, the gap before its
        /// next datapoint is bounded by `idle_timeout` if set, and unbounded otherwise.
        #[serde(default = "default_handshake_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        handshake_timeout: Duration,
        /// How long one connection may stay quiet before this listener closes it and frees its
        /// connection-cap slot. Off unless set: with no value, a connection that sent one
        /// datapoint and then went silent holds its slot indefinitely. `0s` is rejected; omit the
        /// field to disable. `transport: tcp` only: any value under `transport: udp` is rejected.
        ///
        /// Recommended wherever steady traffic is expected: a connection quiet for longer than
        /// this is an anomaly (a dead peer, a half-open socket, a slow-loris), and closing it
        /// returns the slot. Set it well above the sender's longest normal gap (several
        /// `batch_flush_interval`s, say). Leave it unset for sparse or bursty senders, and think
        /// twice on plaintext transports, where the sender cannot detect the close before its
        /// next write. A carbon relay that flushes once a minute wants a value well above that
        /// minute.
        ///
        /// The clock runs only while this listener is waiting on the peer's socket, and resets on
        /// bytes read from the peer and on this listener handing an accumulated batch downstream.
        /// Time blocked on a full downstream never counts, so a stalled pipeline cannot make a
        /// busy connection look idle.
        ///
        /// An idle close is policy, not a fault: complete buffered frames are flushed downstream
        /// first (`logit.component.receive.flushed{reason="closed"}`), a buffered partial frame is
        /// counted `logit.input.frames.dropped{reason="truncated"}`, and the close is counted
        /// `logit.input.connections.closed{reason="idle"}`, never diagnosed as a
        /// `connection_error`.
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        idle_timeout: Option<Duration>,
        /// The longest plaintext line this listener assembles before dropping it and draining to
        /// the next newline (counted once as `logit.input.frames.dropped{reason="oversize"}`; the
        /// line after it still decodes). A byte-count string (`"8192"`, `"16KiB"`). Defaults to
        /// `"8192"`, past any real tagged path while keeping one hostile connection from growing
        /// an unbounded read buffer. `0` is rejected. TCP plaintext only; a UDP datagram is its
        /// own frame.
        #[serde(default = "default_graphite_max_line_bytes", with = "human_bytes")]
        #[schemars(with = "String")]
        max_line_bytes: u64,
        /// The largest pickle frame this listener accepts. A frame declaring more closes the
        /// connection (`logit.input.frames.dropped{reason="oversize"}`, diagnostic
        /// `framing_error`): a length-framed stream has no resync point. A byte-count string.
        /// Defaults to `"1MiB"`, the bound carbon's own pickle receiver inherits from Twisted, so
        /// this listener refuses the frames carbon would. Must be within `1024..=16MiB`.
        /// `protocol: pickle` only.
        #[serde(default = "default_graphite_max_frame_bytes", with = "human_bytes")]
        #[schemars(with = "String")]
        max_frame_bytes: u64,
    },
    /// RFC 3164 / RFC 5424 syslog, over UDP (the default) or TCP. RFC 5424 STRUCTURED-DATA is
    /// parsed into `syslog.sd` either way.
    ///
    /// Under `transport: tcp` each connection's framing is detected from its first byte and
    /// latched for that connection's life: an ASCII digit means RFC 6587 octet-counting
    /// (`MSG-LEN SP MSG`, what `syslog_out` emits, and the only framing that can carry a message
    /// containing a newline); anything else means LF-delimited framing (rsyslog's `omfwd`
    /// default). There is no `framing:` field. A frame over 64 KiB, or a malformed octet count,
    /// closes that one connection.
    ///
    /// `tls:` turns TLS on and makes it required: there is no plaintext fallback on a TLS
    /// listener. It applies to `transport: tcp` only (RFC 5425); `tls:` under `transport: udp` is
    /// rejected.
    ///
    /// A TCP listener has no receive queue (the connection's own flow control is the
    /// backpressure), so `receive:`'s queue fields (`max_datagrams`, `max_bytes`, `overflow`,
    /// `receive_buffer_bytes`, `read_batch`) are rejected on one. Its batch-assembly fields
    /// (`batch_max_events`, `batch_max_bytes`, `batch_flush_interval`) and `shutdown_grace` apply
    /// per connection: N live connections can hold up to N times `batch_max_events` in flight.
    SyslogIn {
        bind: String,
        #[serde(default)]
        transport: SyslogTransport,
        /// Terminates TLS on this listener when present; plaintext when omitted. Requires
        /// `transport: tcp`.
        #[serde(default)]
        tls: Option<TlsServerConfig>,
        /// How long one connection has, per pre-message phase, before this listener closes it
        /// and frees its connection-cap slot: the TLS accept when `tls:` is set, then the wait for
        /// the connection's first byte. Each phase gets its own budget, so a silent TLS
        /// connection costs up to twice this value. Defaults to `5s`; `0s` is rejected.
        /// `transport: tcp` only: a non-default value under `transport: udp` is rejected.
        ///
        /// Not an idle timeout. Once a connection has sent its first byte, the gap before its
        /// next frame is bounded by `idle_timeout` if set, and unbounded otherwise.
        #[serde(default = "default_handshake_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        handshake_timeout: Duration,
        /// How long one connection may stay quiet before this listener closes it and frees its
        /// connection-cap slot. Off unless set: with no value, a connection that sent one frame
        /// and then went silent holds its slot indefinitely. `0s` is rejected; omit the field to
        /// disable. `transport: tcp` only: any value under `transport: udp` is rejected.
        ///
        /// Recommended wherever steady traffic is expected: a connection quiet for longer than
        /// this is an anomaly (a dead peer, a half-open socket, a slow-loris), and closing it
        /// returns the slot. Set it well above the sender's longest normal gap (several
        /// `batch_flush_interval`s, say). Leave it unset for sparse or bursty senders, and think
        /// twice on plaintext transports: a plaintext syslog sender has no ack to lose a message
        /// against and may not notice the close until after it has written one.
        ///
        /// The clock runs only while this listener is waiting on the peer's socket, and resets on
        /// bytes read from the peer and on this listener handing an accumulated batch downstream.
        /// Time blocked on a full downstream never counts, so a stalled pipeline cannot make a
        /// busy connection look idle.
        ///
        /// An idle close is policy, not a fault: complete buffered frames are flushed downstream
        /// first (`logit.component.receive.flushed{reason="closed"}`), a buffered partial frame is
        /// counted `logit.input.frames.dropped{reason="truncated"}`, and the close is counted
        /// `logit.input.connections.closed{reason="idle"}`, never diagnosed as a
        /// `connection_error`.
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        idle_timeout: Option<Duration>,
    },
    /// OpenTelemetry Protocol (logs, metrics, and/or traces) over OTLP/HTTP (protobuf or JSON
    /// body) or OTLP/gRPC.
    OtlpIn {
        bind: String,
        #[serde(default)]
        protocol: OtlpProtocol,
        /// Terminates TLS on this listener, under either `protocol`, when present; plaintext when
        /// omitted.
        #[serde(default)]
        tls: Option<TlsServerConfig>,
        /// How long one connection has, per pre-request phase, before this listener closes it
        /// and frees its connection-cap slot: the TLS accept when `tls:` is set, and on a
        /// plaintext listener the wait for its first byte. Defaults to `5s`; `0s` is rejected.
        ///
        /// Not an idle timeout. Once a connection has produced one byte, the quiet gaps between
        /// requests are bounded by `idle_timeout` if set, and by nothing otherwise.
        ///
        /// Also the grace period an idle close gives the HTTP server to shut the connection down
        /// before it is dropped.
        #[serde(default = "default_handshake_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        handshake_timeout: Duration,
        /// How long one connection may sit with no request in flight before this listener closes
        /// it and frees its connection-cap slot. Off unless set: with no value, a connection that
        /// sent one byte and then went silent holds its slot indefinitely. `0s` is rejected; omit
        /// the field to disable.
        ///
        /// Recommended wherever steady traffic is expected: a connection quiet for longer than
        /// this is an anomaly (a dead peer, a half-open socket, a slow-loris), and closing it
        /// returns the slot. An OTLP exporter pools its connection between exports, so set this
        /// well above the export interval; a conformant exporter reconnects on its own, so a
        /// close between exports costs it a reconnect, not a batch.
        ///
        /// The clock runs only while no request is in flight and resets when a request completes,
        /// so time a handler spends blocked on a full downstream never counts. A request head
        /// that takes longer than this to arrive on an idle keep-alive connection closes it. A
        /// request body that stalls mid-upload is bounded per frame by the same value, answered
        /// with `408` (`protocol: http`) or `grpc-status: 4` (`protocol: grpc`), and then closed.
        ///
        /// An idle close is policy, not a fault: the server is asked to shut the connection down
        /// gracefully, given `handshake_timeout` to do it, so a response already in flight still
        /// goes out. The close is counted `logit.input.connections.closed{reason="idle"}`, never
        /// diagnosed as a `connection_error`.
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        idle_timeout: Option<Duration>,
    },
    /// Tails one or more files as a log source, one line per event; rotation-, truncation-, and
    /// checkpoint-aware. `paths` entries are absolute paths; a `*` is permitted only in the final
    /// path component (`/var/log/app/*.log`) and matches any run of non-`/` characters. An empty
    /// list or entry is rejected.
    TailIn {
        paths: Vec<String>,
        #[serde(flatten)]
        tail: TailOptions,
    },
    /// Tails Docker's json-file container logs (`<root>/<id>/<id>-json.log`) and stamps
    /// per-container resource attributes read from the sibling `config.v2.json`. No docker
    /// socket and no HTTP client. Takes the same tailing options as `tail_in`.
    DockerIn {
        /// The Docker daemon's container-state directory. Defaults to
        /// `/var/lib/docker/containers`, right for stock native-Linux Docker only; rootless
        /// Docker, Podman, and Docker Desktop use a different layout or log format. Must be
        /// non-empty.
        #[serde(default = "default_docker_root")]
        root: String,
        /// Container names (the `docker ps` name, without a leading `/`) or id prefixes (at
        /// least 12 hex characters) to follow. A container not named here is never tailed, even
        /// if it exists under `root`, so a mistyped name is a silent no-op rather than every
        /// container on the host. Required unless `discover: true`; an empty or duplicate entry
        /// is rejected.
        #[serde(default)]
        containers: Vec<String>,
        /// Follow every container under `root`, including ones that appear after startup,
        /// instead of only what `containers` names. `containers` may still be listed to document
        /// intent but has no filtering effect once this is set.
        #[serde(default)]
        discover: bool,
        /// Container label keys to stamp as `container.label.<key>` resource attributes. Empty by
        /// default: a label's value is your data, and every key here becomes a permanent entry in
        /// the process-wide attribute interner, so this is an opt-in list, never "all labels". An
        /// empty entry is rejected.
        #[serde(default)]
        labels: Vec<String>,
        #[serde(flatten)]
        tail: TailOptions,
    },
    /// The native `logit`-to-`logit` protocol: one TCP (optionally TLS) listener accepting many
    /// connections, each handshaking with `Hello`/`HelloAck` and acknowledging every frame.
    LogitIn {
        bind: String,
        /// Terminates TLS on this listener when present; plaintext when omitted.
        #[serde(default)]
        tls: Option<TlsServerConfig>,
        /// Caps the size (before and after decompression alike) of one frame this listener
        /// accepts, echoed to every connecting client in `HelloAck`. A byte-count string.
        /// Defaults to 64 MiB, which is also the ceiling; `0` or a larger value is rejected.
        #[serde(default, with = "human_bytes::option")]
        #[schemars(with = "Option<String>")]
        max_frame_bytes: Option<u64>,
        /// How long one connection has, per pre-`Hello` phase, before this listener closes it
        /// and frees its connection-cap slot: the TLS accept when `tls:` is set, then the `Hello`
        /// read itself. Each phase gets its own budget, so a TLS connection that sends no `Hello`
        /// costs up to twice this value. Defaults to `5s`; `0s` is rejected.
        ///
        /// Not an idle timeout. Once a connection is handshaken, the gap before its next data
        /// frame is bounded by `idle_timeout` if set, and unbounded otherwise.
        #[serde(default = "default_handshake_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        handshake_timeout: Duration,
        /// How long one handshaken connection may stay quiet before this listener closes it and
        /// frees its connection-cap slot. Off unless set: with no value, a connection that sent
        /// one frame and then went silent holds its slot indefinitely. `0s` is rejected; omit the
        /// field to disable.
        ///
        /// Recommended wherever steady traffic is expected: a connection quiet for longer than
        /// this is an anomaly (a dead peer, a half-open socket, a slow-loris), and closing it
        /// returns the slot. Set it well above the sender's longest normal gap (several of the
        /// peer's flush intervals, say); leave it unset for sparse or bursty senders.
        ///
        /// The clock runs only while this listener is waiting on the peer's socket, and resets
        /// when the handshake completes and on every `Ack` this listener writes. A peer waiting
        /// for an ack a slow downstream is delaying is not idle, so time blocked on a full
        /// downstream never counts. A frame body that stops arriving part-way is bounded by this
        /// value per read, not in total, so a large frame that keeps making progress is never cut
        /// off.
        ///
        /// An idle close is policy, not a fault: it is counted
        /// `logit.input.connections.closed{reason="idle"}`, never diagnosed as a
        /// `connection_error`. The peer is told: this listener writes `Reject{GOING_AWAY, "idle
        /// for <dur>"}` before closing, and `logit_out` probes a pooled connection for that before
        /// reusing it, so a `logit_out -> logit_in` pair reconnects rather than losing a batch.
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        idle_timeout: Option<Duration>,
    },
    /// `logit` observing itself: drains every component's buffered self-telemetry on `interval`
    /// and emits it as ordinary events into the graph. At most one per config.
    Internal {
        /// The drain cadence for every component's buffered points, and the sampling tick for
        /// process-level gauges (interner size, uptime). Should divide evenly into any downstream
        /// `aggregate` interval, or the two windows beat against each other. `0s` is rejected.
        #[serde(with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        interval: Duration,
        /// Fraction of traces whose internal spans are kept, `0.0..=1.0`, decided per `trace_id`
        /// the same way at every node, so a kept trace is kept at every hop. Defaults to `0.1`:
        /// spans cost one per node visit per batch, where metric points coalesce between drains.
        /// `0.0` turns spans off; `1.0` keeps everything, for a demo or debugging config that
        /// wants full traces. Distinct from the `sample` transform, which samples your
        /// application's events by a hashed key.
        #[serde(default = "default_span_sample_rate")]
        span_sample_rate: f64,
        /// Which of `logit`'s own self-log events are captured into the pipeline as ordinary log
        /// events: `warn` (the default) and `error` by severity, or `off`, which installs no
        /// capturing layer at all.
        #[serde(default)]
        logs: InternalLogs,
    },

    /// Inline Lua source (a YAML block scalar in practice).
    Lua {
        script: String,
        /// Runs this component's `flush()`, if the script defines one, on this interval. Omitted
        /// means the component never ticks, the same as a script with no `flush()`. `0s` is
        /// rejected.
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        interval: Option<Duration>,
    },
    /// A `.lua` file path, relative to the config file. Takes the same `interval` as `lua`.
    LuaFile {
        lua_file: String,
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        interval: Option<Duration>,
    },
    /// The windowed aggregator (counters, gauges, sets, distributions). Flushes every `interval`;
    /// `0s` is rejected.
    Aggregate {
        #[serde(with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        interval: Duration,
        /// Whether each window's emitted `Sum`/`Histogram` is that window's own increment
        /// (`delta`, the default: tumbling, self-contained) or a running total since the series
        /// was first seen (`cumulative`, what OTLP and Prometheus carry, and what
        /// `prometheus_out` requires).
        #[serde(default)]
        temporality: AggregateTemporality,
        /// How many consecutive windows a series with no new data is retained past its last
        /// update. Lets a relative gauge adjustment arriving in a later window resolve against
        /// the value last held, and under `temporality: cumulative` keeps a `Sum`/`Histogram`'s
        /// running total alive across the window boundary. Defaults to `5`. `0` disables
        /// retention (every series is drained every window) and is rejected together with
        /// `temporality: cumulative`, which would otherwise emit each window's delta labelled as
        /// a cumulative total.
        #[serde(default = "default_series_retention")]
        series_retention: u32,
        /// A hard cap on how many series may be retained across this component's window at once;
        /// a cardinality guard, not a tuning knob. `series_retention` alone bounds only how long
        /// one series survives, so a stream of never-repeating series names would otherwise hold
        /// unboundedly many. Defaults to `10000`. Least-recently-updated series are evicted first.
        /// Must be at least `1` under `temporality: cumulative`.
        #[serde(default = "default_max_retained_series")]
        max_retained_series: usize,
        /// Whether a raw samples series (statsd `ms`/`h`/`d`) absorbs into this window as a
        /// sketch (`sketch`, the default: error-bounded quantiles, no raw values retained) or
        /// keeps its raw observations for the whole window (`samples`), falling back to a sketch,
        /// counted, when `max_samples_per_series` is exceeded or an incoming record's
        /// `sample_rate` disagrees with the series' first one. `samples` is the lossless option:
        /// a downstream `logit` re-sketching with a different accuracy target, or a sink that
        /// wants the individual values, only has that choice under `samples`.
        #[serde(default)]
        distributions: Distributions,
        /// A hard cap on how many raw values one series may retain in one window before
        /// `distributions: samples` falls back to sketching what it holds; a memory guard, not a
        /// tuning knob. Defaults to `1000`. Meaningless under `distributions: sketch`.
        #[serde(default = "default_max_samples_per_series")]
        max_samples_per_series: usize,
        /// Whether a raw set-members series (statsd `s`) absorbs into this window as a
        /// HyperLogLog cardinality estimate (`estimate`, the default) or keeps its exact,
        /// deduplicated member set for the whole window (`members`), falling back to an estimate,
        /// counted, when `max_set_members_per_series` is exceeded. `members` is the lossless
        /// option: an exact member count or list only survives the window under it.
        #[serde(default)]
        sets: Sets,
        /// A hard cap on how many distinct members one series may retain in one window before
        /// `sets: members` falls back to an estimate; a memory guard, not a tuning knob. Defaults
        /// to `1000`. Meaningless under `sets: estimate`.
        #[serde(default = "default_max_set_members_per_series")]
        max_set_members_per_series: usize,
    },
    /// Parses a log record's message as JSON, merging the resulting key/values into the event's
    /// attributes. A failed parse passes the event through untouched.
    Json {
        /// Skip everything before the first `{` and parse from there, for lines with a non-JSON
        /// prefix (`2026-08-29 INFO {"a":1}`). Off by default: the whole line is the JSON.
        #[serde(default)]
        skip_to_brace: bool,
        /// What to do with a message that is not valid UTF-8: `reject` (the default: the parse
        /// fails and the event passes through untouched) or `replace` (retry on a copy with every
        /// invalid sequence replaced by U+FFFD).
        #[serde(default)]
        invalid_utf8: JsonInvalidUtf8,
    },
    /// Splits a log record's message as one CSV row, merging the named columns into the event's
    /// attributes. Columns are positional, declared in config; there is no header-row mode, and
    /// every field stays a string.
    Csv {
        /// Attribute names for each field, left to right. Required and non-empty; an empty entry
        /// or a duplicate entry is rejected (a duplicate would silently overwrite the earlier
        /// column on every event).
        columns: Vec<String>,
        /// The field separator, one ASCII character. `,` by default; `"\t"` (double-quoted, so
        /// YAML resolves the escape) for TSV, or `;`/`|`. Rejected: `"` (the quote character,
        /// which this parser reads as field framing), `\n`/`\r` (already consumed as line
        /// framing), and any non-ASCII character.
        #[serde(default = "default_csv_delimiter")]
        delimiter: char,
    },
    /// Turns attributes already on an event (typically merged there by `json`) into metrics on
    /// that same event. A missing or non-numeric attribute is a silent skip for that entry. At
    /// least one of `counters`/`gauges`/`distributions` must be non-empty. There is no `tags:`
    /// field: every metrics sink already reads `event.attributes`, so tag selection is `keep`'s
    /// job.
    KvMetrics {
        #[serde(default)]
        counters: Vec<MetricSpec>,
        #[serde(default)]
        gauges: Vec<MetricSpec>,
        #[serde(default)]
        distributions: Vec<MetricSpec>,
    },
    /// Retains only the named attributes, dropping the rest: an allowlist, so a new field
    /// appearing in a log format later cannot silently become a new tag dimension. Place it
    /// before `aggregate`, whose series key includes every attribute, to bound series cardinality
    /// and per-window memory. An empty `fields` list is legal and drops every attribute.
    Keep { fields: Vec<String> },
    /// Drops the named attributes, keeping the rest.
    Remove { fields: Vec<String> },
    /// Stamps constant values onto every event's attributes and/or the batch's resource. A
    /// configured value overwrites whatever the wire carried under the same key. At least one of
    /// `resource`/`attributes` must be non-empty.
    Set {
        /// Applied once per batch, to the batch's resource, which every event in the batch
        /// shares.
        #[serde(default)]
        resource: std::collections::BTreeMap<String, SetValue>,
        /// Applied per event, to `event.attributes`.
        #[serde(default)]
        attributes: std::collections::BTreeMap<String, SetValue>,
    },
    /// Lifts an application trace/span reference off an event's attributes onto its log record,
    /// for the common "my JSON log body already has a `trace.id` field" case, and with a `span:`
    /// block also turns that log line into a real span on the same event: an access log's ids
    /// plus its own start/end/duration become a span whose start is the event's timestamp. Reads
    /// the well-known attribute names by default (`traceparent`, `trace.id`, `trace.flags`,
    /// `span.id`, `span.parent_id`, `span.name`, `span.kind`, `span.status`,
    /// `span.start`/`span.end`/`span.duration` and their unit-suffixed forms); `trace_id`,
    /// `span_id`, and `flags` rename the three id sources. A successful lift overwrites any trace
    /// reference the log already had. An event with no log, or with missing or unparseable
    /// attributes, passes through untouched; it is never an error.
    TraceContext {
        /// The attribute holding a 32-character hex trace id. Defaults to `trace.id`; an empty
        /// string is rejected. A `traceparent` attribute supplies the trace id when this one is
        /// absent.
        #[serde(default = "default_trace_id_field")]
        trace_id: String,
        /// The attribute holding this line's own 16-character hex span id. Defaults to `span.id`;
        /// `null` disables the lookup, and an empty string is rejected. An absent attribute means
        /// "no span id", not a skip (unless a `span:` block needs one); only a present but
        /// unparseable value is an error.
        #[serde(default = "default_span_id_field")]
        span_id: Option<String>,
        /// The attribute holding the W3C trace flags (0-255, decimal, never hex). Defaults to
        /// `trace.flags`; `null` disables the lookup, and an empty string is rejected. A
        /// `traceparent` attribute supplies the flags when this one is absent.
        #[serde(default = "default_flags_field")]
        flags: Option<String>,
        /// Keep the source attributes after a successful lift instead of removing them (the
        /// default). Removal matters for OTLP-native backends: Loki turns log attributes into
        /// structured metadata under their own names, so a leftover `trace_id` attribute would
        /// collide with the native one. With `span:`, every convention attribute consumed
        /// (`traceparent`, `span.parent_id`, `span.name`, `span.kind`, `span.status`, and the
        /// timing fields) is removed too.
        #[serde(default)]
        keep_source: bool,
        /// Opt in to minting a span from the lifted ids plus the event's `span.start`/`span.end`/
        /// `span.duration` attributes (any two). Absent (the default) means a log-only lift.
        #[serde(default)]
        span: Option<SpanLiftConfig>,
    },
    /// Multiplies named numeric attributes by a constant factor, in place: unit conversion
    /// (nginx's `request_time` in seconds to milliseconds, say) without a Lua script. A missing
    /// or non-numeric attribute is a silent skip for that field, never a dropped event.
    Scale {
        /// Attribute name to multiplication factor. At least one entry is required; an empty name
        /// or a non-finite factor is rejected.
        fields: std::collections::BTreeMap<String, f64>,
    },
    /// Forwards an event carrying a wanted signal and drops the rest: `signals: [traces]` ahead
    /// of a traces-only sink, say, fed from a source whose events also carry metrics. Never
    /// mutates a forwarded event: under the default `mode: any_of`, an event carrying a listed
    /// signal is forwarded as it arrived, unlisted signals included. `mode: only` also requires
    /// the event carry nothing outside `signals`. Place `keep_signals`/`drop_signals` ahead of
    /// this instead if unwanted payloads must be stripped rather than tolerated. `signals` may
    /// not be empty.
    HasSignal {
        signals: Vec<Signal>,
        #[serde(default)]
        mode: MatchMode,
    },
    /// Retains only the listed signals' payloads on every event, clearing the rest: an allowlist,
    /// `keep`'s relationship to `remove`. Unlike `has_signal`, this mutates: an event carrying a
    /// log and derived metrics loses the metrics under `signals: [logs]` but keeps the log. An
    /// event left with no payload is dropped. `signals` may not be empty (that would drop every
    /// event) and may not name all three signals (a no-op).
    KeepSignals { signals: Vec<Signal> },
    /// Clears the listed signals' payloads on every event, keeping the rest: the mirror of
    /// `keep_signals`. An event left with no payload is dropped. `signals` may not be empty (a
    /// no-op) and may not name all three signals (that would drop every event).
    DropSignals { signals: Vec<Signal> },
    /// Forwards an event whose batch resource and/or own attributes match every configured pair,
    /// dropping the rest. The config is `set`'s: `resource:`/`attributes:` maps of the same
    /// literals, so this matches on what `set` can stamp. Never mutates a forwarded event.
    ///
    /// A map is a conjunction: every pair listed, across both maps, must match; there is no
    /// `or`. A configured key the event or resource doesn't carry never matches, and "key present
    /// with any value" is not expressible. Values coerce across numeric representations
    /// (`status: 200` matches an integer, float, or string `200`), but a boolean never coerces,
    /// and two strings are never compared numerically. At least one of `resource`/`attributes`
    /// must be non-empty, every key must be non-empty, and every numeric value must be finite.
    HasAttributes {
        #[serde(default)]
        resource: std::collections::BTreeMap<String, SetValue>,
        #[serde(default)]
        attributes: std::collections::BTreeMap<String, SetValue>,
    },
    /// Drops an event whose batch resource and/or own attributes match every configured pair,
    /// forwarding the rest: the complement of `has_attributes` on the same config, taken as a
    /// whole, not per pair. An event matching some but not all configured pairs is forwarded,
    /// and so is one that never carried a configured key; read it as "this event isn't one of
    /// the ones told to drop", the else-branch of a `has_attributes` fan-out. Same matching rules
    /// and validation as `has_attributes`.
    DropAttributes {
        #[serde(default)]
        resource: std::collections::BTreeMap<String, SetValue>,
        #[serde(default)]
        attributes: std::collections::BTreeMap<String, SetValue>,
    },
    /// Clamps attribute (and/or resource-attribute) values to a per-field allow-list: `keep`'s
    /// value-side sibling, for a tag whose valid set you know but the producer doesn't enforce
    /// (a `Host` header against a handful of real vhosts, say). A value not in a field's `allow`
    /// becomes that field's `other`, or is removed if `other` is absent. Never drops an event. An
    /// attribute the event doesn't carry is a silent no-op for that field, never a stamp. At
    /// least one of `resource`/`attributes` must be non-empty, and no field name may be empty.
    KeepValues {
        /// Applied once per batch, to the batch's resource.
        #[serde(default)]
        resource: std::collections::BTreeMap<String, ValueAllowList>,
        /// Applied per event, to `event.attributes`.
        #[serde(default)]
        attributes: std::collections::BTreeMap<String, ValueAllowList>,
    },
    /// Forwards an event whose batch's `origin`/`previous` match a configured list: `origin` is
    /// the component that created the batch and `previous` the one that most recently handled
    /// it, not event data. Never mutates a forwarded event.
    ///
    /// Each field is a list of alternatives, OR'd within the field; the two fields AND together
    /// when both are configured. An empty list means "not checked" for that field. At least one
    /// of `origin`/`previous` must be non-empty, and no entry may be an empty string.
    HasProvenance {
        #[serde(default)]
        origin: Vec<String>,
        #[serde(default)]
        previous: Vec<String>,
    },
    /// Drops an event whose batch's `origin`/`previous` match a configured list, forwarding the
    /// rest: the complement of `has_provenance` on the same config, taken as a whole, not per
    /// field. A batch matching only one of two configured fields is forwarded, and so is one that
    /// never carried a configured value; read it as "this batch isn't one of the ones told to
    /// drop". Same matching rules and validation as `has_provenance`.
    DropProvenance {
        #[serde(default)]
        origin: Vec<String>,
        #[serde(default)]
        previous: Vec<String>,
    },
    /// Equality-only routing: one key read per event, one target per matching value. There is no
    /// predicate language; use `lua` for a condition. An event whose key is absent, or whose
    /// value no route names, is unrouted: it goes to this component's ordinary consumers, or is
    /// dropped and counted (`logit.component.events.dropped{reason="unrouted"}`) if it has none.
    Route {
        by: RouteBy,
        /// Value to target id. Several values may name one target. The router's edges derive
        /// from these values, so there is no separate `targets:` list. Must be non-empty, with no
        /// empty key or value, and every value must name a `target` component.
        routes: std::collections::BTreeMap<String, String>,
    },
    /// Parses a log record's message as logfmt (`level=info msg="hello world" dur=3ms`), merging
    /// the resulting key/values into the event's attributes. Additive, and a failed parse passes
    /// the event through, like `json`.
    Logfmt {
        /// Treat a token with no `=` as a boolean-true flag (`cached` becomes `cached: true`),
        /// the Heroku logfmt convention. Off by default: a bareword promotes an arbitrary input
        /// token into attribute-key position, and every attribute key is interned into a
        /// process-global table that never shrinks, so a timestamp-prefixed line (`2026/09/07
        /// 12:00:00 level=info ...`) would leak two never-repeating entries per line. Turn it on
        /// only for a source that emits flags.
        #[serde(default)]
        bare_keys: bool,
    },
    /// Parses a log record's message as literal `key<kv_sep>value` pairs separated by `pair_sep`
    /// (`a=1&b=2`, `a: 1, b: 2`), with no quoting and no escapes, unlike `logfmt`. Both
    /// separators are required and must differ; an empty separator, or a `kv_sep` that contains
    /// `pair_sep`, is rejected.
    Kv {
        /// Separator between one pair and the next. Whitespace around each key and value is
        /// trimmed, so `", "` and `","` behave the same on `a=1, b=2`.
        pair_sep: String,
        /// Separator between a key and its value, within one pair. The first occurrence in a
        /// segment splits it, so `a=b=c` yields `a` -> `b=c`.
        kv_sep: String,
        /// Treat a token with no `kv_sep` as a boolean-true flag. Off by default, for the same
        /// interner reason as `logfmt`'s `bare_keys`.
        #[serde(default)]
        bare_keys: bool,
    },
    /// Matches a pattern against a log message (or, with `field:`, a named attribute), turning
    /// every named capture group (`(?P<name>...)` or `(?<name>...)`) into an attribute of that
    /// name; an unnamed group is grouping/alternation only. The pattern is compiled by `logit
    /// validate`, so an invalid pattern, or one with no named capture group, is a validation
    /// error rather than a run-time surprise.
    Regex {
        /// The pattern. Every named capture group becomes an attribute; first match only. A
        /// non-matching line, or one with no `field` attribute, passes through unchanged with no
        /// diagnostic, only the `logit.transform.matched{,.skipped}` counters.
        pattern: String,
        /// The attribute to match against, instead of the log message. Absent (the default) reads
        /// the log message, like `json`; an empty name is rejected. Use it when an earlier `json`
        /// has already lifted the text to match into an attribute.
        #[serde(default)]
        field: Option<String>,
    },
    /// Rewrites every event it sees into a measurement of that event's shape (attribute and
    /// nested-map counts, key/value byte lengths, value types, metric and span widths) and, on
    /// its `interval`, emits per-batch and cumulative measurements (events and resource/scope
    /// attributes per batch; distinct keys, distinct key-sets, and the share of events the most
    /// common one and five key-sets carry). The original payload is dropped, so place this on its
    /// own branch of a fan-out, never in the flow it measures.
    ///
    /// It emits counts and lengths only: never an attribute key, an attribute value, a log body,
    /// or a metric name from an observed event, in any metric, tag, diagnostic, or telemetry
    /// point. That is what lets its output leave an environment the traffic itself can't.
    ///
    /// Distribution-shaped quantities go out as raw samples; put an `aggregate` downstream to
    /// summarize them.
    Shape {
        /// How often the per-batch and cumulative measurements are emitted. Defaults to `10s`;
        /// `0s` is rejected. The per-event measurements ride out on the events themselves and
        /// never wait for this.
        #[serde(with = "humantime_serde_duration", default = "default_shape_interval")]
        #[schemars(with = "String")]
        interval: Duration,
        /// Whether the batch's resource is forwarded (`keep`) or replaced with an empty one
        /// (`drop`, the default). The batch's scope always passes through: it names an
        /// instrumentation library rather than carrying payload.
        #[serde(default)]
        resource: ShapeResource,
        /// A hard cap on the distinct top-level attribute keys tracked since start; a memory
        /// guard, not a tuning knob. Defaults to `4096`; `0` is rejected. Past it a new key is
        /// counted as overflow rather than tracked, and `logit.shape.tracking_overflow` goes to
        /// `1`.
        #[serde(default = "default_max_tracked_keys")]
        max_tracked_keys: usize,
        /// A hard cap on the distinct top-level key-sets tracked since start. Same guard and
        /// overflow behavior as `max_tracked_keys`. Defaults to `4096`; `0` is rejected.
        #[serde(default = "default_max_tracked_keysets")]
        max_tracked_keysets: usize,
    },
    /// Rewrites a nested map or array attribute into flat, dot-joined keys: `{"foo": {"key":
    /// "bar"}}` becomes `foo.key = "bar"`, `{"tags": ["a","b"]}` becomes `tags.0`/`tags.1`, and
    /// the two compose (`{"items": [{"name": "x"}]}` becomes `items.0.name`). `influxdb_out`,
    /// `statsd_out`, `prometheus_out`, `graphite_out`, and `collectd_out` each drop a nested
    /// attribute outright, so this is how nested JSON/OTLP data becomes a tag on any of them.
    /// Never drops an event, and never removes an attribute that wasn't itself nested. Last write
    /// wins on a key collision, silently.
    Flatten {
        /// Which top-level attributes to expand. `all` (the default) expands every nested
        /// attribute, the useful default for a source whose keys you don't control (a Kubernetes
        /// label map, an OTLP `KvlistValue`). A named list holds literal attribute names, never
        /// paths. Rejected if both this and `resource` are `none`, or if a named list is empty or
        /// contains an empty or duplicate name.
        #[serde(default = "default_flatten_attributes")]
        attributes: FlattenFields,
        /// Also expand the batch's resource attributes, under the same rules, once per batch.
        /// `none` by default: a resource is a small, mostly operator-declared identity map that
        /// is rarely nested. Scope attributes are never touched.
        #[serde(default = "default_flatten_resource")]
        resource: FlattenFields,
        /// Whether an array expands by index (`index`, the default: `tags.0`) or is left as a leaf
        /// and written back whole at its path (`skip`), for a source whose arrays are data rather
        /// than structure. Under `skip` a top-level array attribute is left untouched.
        #[serde(default)]
        arrays: FlattenArrays,
    },
    /// Normalizes a web server's access line, logged under raw OTel semconv attribute names, into
    /// its conformant form: composites (`http.request.line`, `url.original`) decomposed into
    /// whichever atomic fields are absent, numerics coerced to integers (`"000"` becomes `0`),
    /// durations in any unit spelling (`_ms`, `_us`, unsuffixed nanoseconds) converted to `_s`,
    /// an unknown method rewritten to `_OTHER` with the raw value kept as
    /// `http.request.method_original`, `HTTP/` stripped off the protocol version, semconv's
    /// sensitive `url.query` values redacted, every free-text field capped and
    /// control-byte-cleaned, plus a small, bounded derived set: `user_agent.class`,
    /// `http.route`, `error.type`, `span.name`, `span.status`, and the `span.duration_s` mirror
    /// `trace_context` resolves a span from. Place it between `json` and `trace_context`. Every
    /// canonical name is also accepted with each `.` spelled `-` (`url-path`,
    /// `http-request-header-x-forwarded-for`), for an emitter whose key grammar forbids dots
    /// (HAProxy's `%{+json}o`); the dotted spelling wins when both are present.
    ///
    /// Best-effort per field, never all-or-nothing and never a dropped event: a value that
    /// doesn't parse is left as it arrived and counted, while every other field is still
    /// normalized. An absent field produces nothing: no default `url.scheme`, no invented route.
    /// Every field is optional, and a bare `type: http_access` is meaningful (the built-in
    /// user-agent table and default caps still apply).
    HttpAccess {
        /// Ordered rules classifying the capped `url.path` into `http.route`, first match wins.
        /// Each is either a named built-in set or a regex paired with a literal route value,
        /// never a capture, so the route set stays bounded by construction.
        #[serde(default)]
        routes: Vec<HttpRouteRule>,
        /// The `http.route` written when no rule matches. Absent (the default) writes no route,
        /// and `span.name` is the method alone. An empty string is rejected.
        #[serde(default)]
        route_other: Option<String>,
        /// Extra user-agent classes, tried in order before the built-in
        /// scanner/tool/crawler/browser table. The built-in table can be pre-empted, never
        /// disabled.
        #[serde(default)]
        user_agent_rules: Vec<UserAgentRule>,
        /// Per-field limits, in characters (not bytes), overriding the built-in cap on a
        /// free-text field such as `url.path` (256) or `client.address` (128). A key must name a
        /// field `http_access` caps (`logit validate` lists them), and a limit of `0` is rejected.
        #[serde(default)]
        max_length: std::collections::BTreeMap<String, usize>,
        /// Extra `url.query` keys whose values are replaced with `REDACTED`, beyond semconv's
        /// seven (`AWSAccessKeyId`, `Signature`, `sig`, `X-Goog-Signature`, `X-Amz-Signature`,
        /// `X-Amz-Credential`, `X-Amz-Security-Token`). Matched ASCII-case-insensitively. An empty
        /// entry is rejected.
        #[serde(default)]
        redact_query: Vec<String>,
        /// Present only to opt in to overwriting `client.address` from the first hop of
        /// `http.request.header.x-forwarded-for`. Off by default, since the header is
        /// client-supplied.
        #[serde(default)]
        forwarded: Option<ForwardedConfig>,
    },
    /// Keeps a fraction of events, consistently. With `key:` set, the key's value is hashed
    /// (XXH64, seed 0, over a fixed canonical byte form; a frozen cross-version contract) and
    /// compared against `rate`, so every event sharing a key (every span and log of one trace,
    /// under `key: trace_id`) gets the same verdict in every `logit` process that sees it, with
    /// nothing propagated between them. With no `key:`, each event is an independent draw.
    /// `always_keep:` pins flagged events through regardless. Never mutates an event.
    Sample {
        /// Fraction of events (or of keys) kept, `0.0..=1.0`. `1` is rejected (a no-op), and `0`
        /// is rejected unless `always_keep` is set, in which case only the flagged events are
        /// kept.
        rate: f64,
        /// What to hash. Absent means an independent draw per event. An empty field name is
        /// rejected.
        #[serde(default)]
        key: Option<SampleKey>,
        /// What happens to an event the configured `key:` isn't on. Only meaningful with `key:`,
        /// and rejected without it. Absent means `random`.
        #[serde(default)]
        missing: Option<SampleMissing>,
        /// Events carrying this field (optionally with this value) are kept unconditionally,
        /// before the key is looked at. Per leg: nothing is propagated to other samplers.
        #[serde(default)]
        always_keep: Option<SampleOverride>,
    },
    // `filter`/`rename`/`throttle`/`dedup` are retired, not unimplemented (ADR
    // `routing-by-condition-is-lua`): each is expressible as a `lua` component, and referencing
    // one is a deserialization error naming the valid kinds. `sample` came back as a native kind
    // because consistent, keyed sampling is the one thing a `lua` component can't express.
    /// InfluxDB 2.x line protocol over HTTP, with bounded output retry.
    // Renamed explicitly: `rename_all = "snake_case"` alone would tag this `influx_db_out`.
    #[serde(rename = "influxdb_out")]
    InfluxDbOut {
        url: String,
        org: String,
        bucket: String,
        /// The API token. A plain string: write `!env INFLUXDB_TOKEN` to pull it from the
        /// environment rather than inlining it. `!env` works on `url`/`org`/`bucket` too.
        token: String,
    },
    /// OTLP logs, metrics, and traces over OTLP/HTTP or OTLP/gRPC. TLS is selected by
    /// `endpoint`'s `https://` scheme.
    OtlpOut {
        endpoint: String,
        #[serde(default)]
        protocol: OtlpProtocol,
        /// Extra headers sent on every export request, under either `protocol`: `X-Scope-OrgID`
        /// for a multi-tenant Loki/Mimir/Grafana Cloud target, say. A value is a plain string, so
        /// `!env` works on it, which is how to carry an `Authorization: Bearer …` token without
        /// inlining it. A name the protocol owns (`content-type`, `content-length`,
        /// `content-encoding`, `host`, `te`, `transfer-encoding`, `connection`, any `grpc-*`
        /// header, or an HTTP/2 pseudo-header starting with `:`) is rejected, as are two keys
        /// naming the same header once case is ignored.
        #[serde(default)]
        headers: HashMap<String, String>,
        /// Per-signal HTTP path overrides. `protocol: http` only; a non-empty value under
        /// `protocol: grpc` is rejected.
        #[serde(default)]
        paths: OtlpPaths,
        /// Gzips request bodies on both transports. Defaults to `none`; most receivers (the OTel
        /// Collector's included) accept both, so change this only for a bandwidth-constrained
        /// link.
        #[serde(default)]
        compression: OtlpCompression,
        /// Tunes TLS on an `https://` endpoint. A non-default block under a plain
        /// `http://`/`grpc://` endpoint is rejected.
        #[serde(default)]
        tls: TlsClientConfig,
    },
    /// The native `logit`-to-`logit` protocol, the mirror of `logit_in`: one TCP (optionally TLS)
    /// connection, one native frame per batch, one `Ack` before that batch counts as delivered.
    LogitOut {
        /// `host:port`. Resolved at connect time, never at config-load time: a peer that isn't up
        /// yet is not a config error.
        endpoint: String,
        /// Offered in this sink's `Hello`; the peer may negotiate it down to `none` if it doesn't
        /// support `lz4`.
        #[serde(default)]
        compression: Compression,
        /// Turns on TLS for this connection when present, and makes it required: a bare
        /// `host:port` has no scheme to select TLS from, so even an empty `tls: {}` means TLS
        /// with the bundled Mozilla roots.
        #[serde(default)]
        tls: Option<TlsClientConfig>,
        /// Connect, handshake, and per-batch ack-wait timeout, one knob for all three. Defaults
        /// to `10s`.
        #[serde(default = "default_logit_out_request_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        request_timeout: Duration,
    },
    /// A human-facing debug sink: writes every event as a readable text block to stdout (the
    /// default), stderr, or a file. The dev loop for seeing a whole pipeline's output without a
    /// real backend.
    StdioOut {
        #[serde(default)]
        target: StdioTarget,
        /// Which encoder writes through this sink: `human` (the default) is the readable text;
        /// `native` is `logit`'s own wire format.
        #[serde(default)]
        format: StreamFormat,
        /// Per-frame compression under `format: native`. A non-`none` value under `format:
        /// human` is rejected.
        #[serde(default)]
        compression: Compression,
    },
    /// A rotating file sink: size- and/or calendar-interval-triggered rotation with
    /// logrotate-style numbered-suffix retention. Renders the same human-readable text
    /// `stdio_out` does by default, or `logit`'s native wire format under `format: native`.
    FileOut {
        /// The active file. A relative path resolves against the config file's directory.
        path: String,
        #[serde(default)]
        rotate: RotateConfig,
        /// Which encoder writes through this sink: `human` (the default) or `native`.
        #[serde(default)]
        format: StreamFormat,
        /// Per-frame compression under `format: native`. A non-`none` value under `format:
        /// human` is rejected.
        #[serde(default)]
        compression: Compression,
    },
    /// RFC 3164 / RFC 5424 syslog egress over UDP or TCP, the mirror of `syslog_in` and a real
    /// relay: header fields round-trip from an event's `syslog.*` attributes when present,
    /// falling back to `facility`, `hostname`, and `app_name` only when an event carries none
    /// (one that never passed through `syslog_in`, say).
    SyslogOut {
        /// `host:port`. Resolved at connect/bind time, never at config-load time: a destination
        /// that isn't up yet is not a config error.
        endpoint: String,
        #[serde(default)]
        transport: SyslogTransport,
        /// Which syslog dialect to emit. `rfc5424` (the default) carries an unambiguous RFC 3339
        /// timestamp; `rfc3164`'s TIMESTAMP has no year and no timezone, so a receiver has to
        /// guess both.
        #[serde(default)]
        format: SyslogFormat,
        /// PRI facility used only when the event carries no `syslog.facility` attribute. Defaults
        /// to `local0`.
        #[serde(default)]
        facility: SyslogFacility,
        /// HOSTNAME fallback, used only when the event carries no `syslog.hostname` attribute
        /// (one that never passed through `syslog_in`, say). No default, so a relayed line's
        /// origin is never silently overwritten.
        #[serde(default)]
        hostname: Option<String>,
        /// APP-NAME fallback, used only when the event carries no `syslog.tag` attribute. No
        /// default.
        #[serde(default)]
        app_name: Option<String>,
        /// Bounds one encoded message (PRI + header + MSG). A byte-count string. Defaults to
        /// `"8192"`, Grafana Alloy's syslog receiver default, rather than RFC 3164's traditional
        /// 1024, which would truncate a JSON-bodied message on every modern relay chain.
        #[serde(default = "default_max_message_bytes", with = "human_bytes")]
        #[schemars(with = "String")]
        max_message_bytes: u64,
        /// TCP only, ignored for UDP. How long a connect attempt (including a reconnect after a
        /// dropped connection) may take before `send` reports a failure. Also bounds the TLS
        /// handshake under `tls:`, as a separate phase, so a TLS connect can take up to twice
        /// this value. Defaults to `5s`.
        #[serde(default = "default_syslog_connect_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        connect_timeout: Duration,
        /// Turns on TLS for this connection when present (RFC 5425, syslog over TLS over TCP),
        /// and makes it required: a bare `host:port` has no scheme to select TLS from, so even an
        /// empty `tls: {}` means TLS with the bundled Mozilla roots. `transport: tcp` only;
        /// `tls:` under `transport: udp` is rejected.
        #[serde(default)]
        tls: Option<TlsClientConfig>,
        /// Opt-in RFC 5424 STRUCTURED-DATA element built from an event's own non-`syslog.*`
        /// attributes. Absent (the default) emits no such element; a `syslog.sd` attribute
        /// (round-tripped from `syslog_in`) still renders regardless. Ignored under `format:
        /// rfc3164`, which has no STRUCTURED-DATA field.
        #[serde(default)]
        structured_data: Option<SyslogStructuredData>,
    },
    /// statsd / DogStatsD egress over UDP or TCP, the mirror of `statsd_in` and a real relay:
    /// names, values, and tags round-trip through the decoder on the other end.
    StatsdOut {
        /// `host:port`. Resolved at connect/bind time, never at config-load time.
        endpoint: String,
        #[serde(default)]
        transport: StatsdTransport,
        /// Which statsd dialect to emit. `dogstatsd` (the default) includes the `|#tag:value,...`
        /// segment; `statsd` omits it for a plain-statsd receiver that would reject it.
        #[serde(default)]
        format: StatsdFormat,
        /// Encodes a relative gauge adjustment (statsd's `+n`/`-n` syntax) natively as a signed
        /// value, instead of dropping it with a `gauge_delta_unresolved` diagnostic. Off by
        /// default: a delta reaching any sink usually means the pipeline is missing an
        /// `aggregate`, so this is an opt-in relay behavior.
        #[serde(default)]
        relative_gauges: bool,
        /// Bounds one UDP datagram's worth of packed lines (several statsd lines newline-joined
        /// per send), not a single line's length. A byte-count string. Defaults to `"1432"`, the
        /// statsd and DogStatsD client default: a 1500-byte MTU minus IPv4/UDP headers minus
        /// headroom for VXLAN/IPsec encapsulation, where a larger datagram would silently
        /// fragment or fail `EMSGSIZE`. `0` is rejected. Ignored for `transport: tcp`.
        #[serde(default = "default_statsd_max_packet_bytes", with = "human_bytes")]
        #[schemars(with = "String")]
        max_packet_bytes: u64,
        /// TCP only, ignored for UDP. How long a connect attempt (including a reconnect after a
        /// dropped connection) may take before `send` reports a failure. Also bounds the TLS
        /// handshake under `tls:`, as a separate phase, so a TLS connect can take up to twice
        /// this value. Defaults to `5s`.
        #[serde(default = "default_statsd_connect_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        connect_timeout: Duration,
        /// Turns on TLS for this connection when present, and makes it required: a bare
        /// `host:port` has no scheme to select TLS from, so even an empty `tls: {}` means TLS
        /// with the bundled Mozilla roots. `transport: tcp` only; `tls:` under `transport: udp`
        /// is rejected. No statsd client speaks TLS, so this is for a `logit`-to-`logit` or
        /// stunnel-shaped relay hop.
        #[serde(default)]
        tls: Option<TlsClientConfig>,
    },
    /// collectd binary `network` plugin egress, the mirror of `collectd_in` and a real relay:
    /// identity, values, and kinds round-trip through the decoder on the other end. UDP only:
    /// collectd's `network` plugin has no TCP mode.
    CollectdOut {
        /// `host:port`. Resolved at send time, never at config-load time.
        endpoint: String,
        /// Bounds one UDP datagram's worth of packed value lists, not a single list's length. A
        /// byte-count string. Defaults to `"1452"`, collectd's own `MaxPacketSize` default (a
        /// 1500-byte MTU minus IPv4 and UDP headers minus headroom). Must be within
        /// `1024..=65535`, collectd's own range: above it no UDP datagram can carry the result,
        /// so every send would fail `EMSGSIZE` and be counted as a per-datagram drop.
        #[serde(default = "default_collectd_max_packet_bytes", with = "human_bytes")]
        #[schemars(with = "String")]
        max_packet_bytes: u64,
        /// Used only when an event carries neither `collectd.host` nor `host.name` (one that
        /// never passed through `collectd_in`, say). No default, so a relayed list's origin is
        /// never silently overwritten. With nothing configured and nothing on the event, the list
        /// is dropped and counted (`logit.output.metrics.skipped{reason="no_host"}`): collectd's
        /// receiver rejects an empty host.
        #[serde(default)]
        hostname: Option<String>,
    },
    /// Carbon plaintext or pickle egress, the mirror of `graphite_in` and a real relay: path,
    /// tags, value, and timestamp round-trip through the decoder on the other end. Both
    /// transports are supported; `protocol: pickle` requires `transport: tcp`.
    GraphiteOut {
        /// `host:port`. Resolved at send time, never at config-load time.
        endpoint: String,
        #[serde(default)]
        transport: GraphiteTransport,
        #[serde(default)]
        protocol: GraphiteProtocol,
        /// Whether to render event attributes as carbon tags. `carbon` (the default) writes
        /// `;name=value` in ascending name order; `drop` is the escape hatch for a pre-1.1
        /// Graphite, whose whisper backend would otherwise take the `;` into a directory name.
        #[serde(default)]
        tags: GraphiteTags,
        /// What to do with a metric kind carbon's one-number-per-datapoint wire cannot carry.
        /// `skip` (the default) drops the record, counted; `expand` renders one dotted sub-path
        /// per component value, counted as degraded.
        #[serde(default)]
        multi_value: GraphiteMultiValue,
        /// Bounds one UDP datagram's worth of packed plaintext lines (several lines
        /// newline-joined per send), not a single line's length. A byte-count string. Defaults to
        /// `"1432"`, the same commodity-Ethernet figure `statsd_out` uses. `0` is rejected.
        /// Ignored under `transport: tcp`.
        #[serde(default = "default_graphite_max_packet_bytes", with = "human_bytes")]
        #[schemars(with = "String")]
        max_packet_bytes: u64,
        /// The longest pickle payload this sink packs into one length-prefixed frame. A
        /// byte-count string. Defaults to `"1MiB"`, the bound carbon's own pickle receiver
        /// enforces, so a relay never writes a frame the far end would refuse. Must be within
        /// `1024..=16MiB`.
        #[serde(default = "default_graphite_max_frame_bytes", with = "human_bytes")]
        #[schemars(with = "String")]
        max_frame_bytes: u64,
        /// TCP only, ignored for UDP. How long a connect attempt may take before `send` reports a
        /// failure. Defaults to `5s`; `0s` is rejected.
        #[serde(default = "default_graphite_connect_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        connect_timeout: Duration,
    },
    /// Prometheus metrics, in one of two modes chosen by which field is set. `scrape_targets:`
    /// scrapes `/metrics` endpoints on `interval`, the way Prometheus's own server does, parsing
    /// whichever text dialect (Prometheus text 0.0.4 or OpenMetrics 1.0) each target's response
    /// declares in its `Content-Type`, and synthesizes `up`, `scrape_duration_seconds`, and
    /// `scrape_samples_scraped` per target per scrape. `bind:` receives remote-write, accepting
    /// 1.0 and 2.0 requests on one listener. Set exactly one of the two; a non-default field
    /// belonging to the other mode is rejected rather than ignored. A receiver synthesizes no
    /// `up`/`scrape_*` series, and holds one piece of cross-request state: `metadata_cache`.
    PrometheusIn {
        /// Scrape mode: absolute `http://`/`https://` URLs with a non-empty host. Non-empty
        /// selects scrape mode.
        #[serde(default)]
        scrape_targets: Vec<String>,
        /// Scrape cadence. Defaults to `15s`; `0s` is rejected. Scrape mode only: a non-default
        /// value alongside `bind:` is rejected.
        #[serde(default = "default_prometheus_scrape_interval", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        interval: Duration,
        /// Per-request timeout. Defaults to `10s`; `0s` is rejected. Scrape mode only.
        #[serde(default = "default_prometheus_scrape_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        timeout: Duration,
        /// Extra headers sent on every scrape request. A name this input reserves (`accept`,
        /// `user-agent`, `content-type`, `content-length`, `content-encoding`, `host`, `te`,
        /// `transfer-encoding`, `connection`, or an HTTP/2 pseudo-header starting with `:`) is
        /// rejected, as are two keys naming the same header once case is ignored. Scrape mode
        /// only.
        #[serde(default)]
        headers: HashMap<String, String>,
        /// Client-side TLS tuning for `https://` scrape targets. A non-default block with no
        /// `https://` target is rejected. Scrape mode only. Prefixed `scrape_` because this kind
        /// has two TLS roles: client TLS for outbound scrapes here, server TLS for the receiver in
        /// `bind_tls`.
        #[serde(default)]
        scrape_tls: TlsClientConfig,
        /// Receiver mode: `host:port` to accept Prometheus remote-write requests on. Set selects
        /// receiver mode. Both wire versions are accepted on the one listener, chosen per request
        /// from its `Content-Type`.
        #[serde(default)]
        bind: Option<String>,
        /// The path the receiver answers `POST`s on; anything else is a `404`. Defaults to
        /// `/api/v1/write`, where every remote-write sender points by convention, and must start
        /// with `/`. Receiver mode only.
        #[serde(default = "default_prometheus_write_path")]
        path: String,
        /// Server-side TLS for the receiver's listener; its presence turns TLS on. Receiver mode
        /// only. Transport security only: the receiver has no authentication, so a listener
        /// reachable from an untrusted network belongs behind something that does.
        #[serde(default)]
        bind_tls: Option<TlsServerConfig>,
        /// How long a receiver connection may sit with no request in flight before it is closed
        /// and its connection-cap slot freed. Omitted (the default) means no idle timeout; `0s` is
        /// rejected. Receiver mode only.
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        idle_timeout: Option<Duration>,
        /// What the receiver remembers about metric types between requests, so a Prometheus 1.0
        /// sender's series decode as typed families. Receiver mode only.
        #[serde(default)]
        metadata_cache: MetadataCacheConfig,
    },
    /// A synthetic event source for load testing. No socket and no decoder: it renders a
    /// declarative `event:` template as fast as `count`/`rate` allow, so a scenario measures the
    /// runtime and the components under test rather than a generator process and a kernel
    /// socket buffer. There is no shape preset: the template plus an ordinary
    /// `json`/`regex`/`kv_metrics` stage downstream composes any shape a scenario needs.
    GenerateIn {
        /// Total events to generate, after which the input exits and the process shuts down
        /// cleanly. Exact: the last batch is short rather than rounded up, so a scenario's derived
        /// events/s and CPU-per-event count what was produced. Omitted means unbounded (a soak
        /// run, or one a profiler attaches to). `0` is rejected.
        #[serde(default)]
        count: Option<u64>,
        /// Events per generated batch, the same unit a real listener's batch assembly produces.
        /// Defaults to `100`; `0` is rejected. A bigger batch amortizes the per-batch runtime cost
        /// (channel send, telemetry point, span) over more events, so a scenario measuring a
        /// per-batch cost lowers it rather than raising `count`.
        #[serde(default = "default_generate_batch")]
        batch: usize,
        /// Target events per second, paced against the wall clock so the average rate holds over
        /// a run. Above roughly a thousand batches per second the sleep granularity makes it
        /// bursty within a millisecond, but the average is still right. Omitted means
        /// unthrottled: generate as fast as downstream backpressure allows, what a throughput
        /// scenario wants. `0` is rejected.
        #[serde(default)]
        rate: Option<u64>,
        /// What each generated event carries. Every field defaults, so an omitted block generates
        /// a bare timestamped event.
        #[serde(default)]
        event: GenerateEvent,
        /// Resource attributes for every generated event: literal keys, templated values
        /// (`{seq}`/`{seq%N}`, the same substitution `event:` uses). An empty key is rejected.
        ///
        /// A resource is batch-level, not per event, so in resource position `seq` is the batch
        /// ordinal (0, 1, 2, ...), not the event counter. That is the feature: the event counter
        /// advances by `batch` each batch, so `{seq%10}` over it under `batch: 100` would render
        /// `0` forever. Over the ordinal, `resource: { host: "h{seq%10}" }` means ten distinct
        /// resources cycling one per batch, what a scenario measuring resource grouping wants,
        /// costing one attribute map per batch and nothing per event. An all-literal resource is
        /// built once at startup and shared by every batch.
        #[serde(default)]
        resource: std::collections::BTreeMap<String, String>,
    },
    /// Prometheus metrics, in one of two sink modes chosen by which field is set. `bind:` serves
    /// an exposition endpoint: a registry of current series rendered on demand, in whichever text
    /// dialect the scraping client's `Accept` negotiates. `endpoint:` sends remote-write: each
    /// batch is POSTed to a remote-write receiver, with no retry in the sink. Set exactly one of
    /// the two; a non-default field belonging to the other mode is rejected rather than ignored.
    /// Delta metrics are skipped and counted; put an `aggregate` with `temporality: cumulative`
    /// upstream.
    PrometheusOut {
        /// `host:port` to serve the exposition on: registry mode. Bound when the pipeline starts,
        /// so an address already in use is a startup failure rather than a scrape that answers
        /// nothing.
        ///
        /// There is no TLS and no auth on this endpoint, and it serves every label of every
        /// series the registry holds to anything that connects: bind loopback or pod-local
        /// (`127.0.0.1:9464`) and front it with something that has both.
        #[serde(default)]
        bind: Option<String>,
        /// The HTTP path the exposition is served on; any other path is a `404`. Defaults to
        /// `/metrics`, what a Prometheus scrape config assumes when `metrics_path` is unset, and
        /// must start with `/`. Registry mode only.
        #[serde(default = "default_prometheus_path")]
        path: String,
        /// A series not updated within this window is dropped from the registry and stops being
        /// exposed. Defaults to `5m`, Prometheus's own staleness horizon. `0s` disables expiry,
        /// leaving `max_series` as the only bound. Registry mode only.
        #[serde(default = "default_prometheus_expire_after", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        expire_after: Duration,
        /// A hard cap on distinct series held in the registry; over it, the least-recently-updated
        /// series is evicted to admit a new one, counted
        /// `logit.output.series.evicted{reason="cardinality"}`. Defaults to `100000`; `0` is
        /// rejected. Registry mode only.
        #[serde(default = "default_prometheus_max_series")]
        max_series: usize,
        /// An absolute `http://`/`https://` remote-write URL, path included (typically
        /// `/api/v1/write`): sender mode. Never resolved at config-load time, so a receiver that
        /// isn't up yet is not a config error.
        #[serde(default)]
        endpoint: Option<String>,
        /// Which remote-write protocol version this sender writes, `1` or `2`. Defaults to `1`,
        /// what every deployed receiver accepts. There is no negotiation and no fallback: pick the
        /// version the receiver speaks. Sender mode only.
        // Serde reads and writes `RemoteWriteVersion` as the integer, so the schema must say so;
        // the derived variant-name schema would publish a spelling (`"V1"`) config never accepts.
        #[serde(default)]
        #[schemars(with = "u8")]
        version: RemoteWriteVersion,
        /// How each remote-write request body is compressed. `snappy`, the default, is the Snappy
        /// block format both remote-write specs mandate, and every receiver accepts it. `zstd` is
        /// the VictoriaMetrics remote write protocol: the same 1.0 request compressed with zstd
        /// instead, which VictoriaMetrics, vmagent, and `logit`'s own `prometheus_in` accept and
        /// Prometheus and Mimir reject. There is no negotiation and no fallback: a receiver that
        /// rejects `zstd` fails every batch, so pick what the receiver accepts. `zstd` needs
        /// `version: 1`. Sender mode only.
        #[serde(default)]
        compression: RemoteWriteCompression,
        /// Per-request timeout on the remote-write POST. Defaults to `10s`; `0s` is rejected.
        /// Sender mode only.
        #[serde(
            default = "default_prometheus_endpoint_timeout",
            with = "humantime_serde_duration"
        )]
        #[schemars(with = "String")]
        timeout: Duration,
        /// Extra headers sent on every remote-write request: `X-Scope-OrgID` for a multi-tenant
        /// Mimir, say. A value is a plain string, so `!env` works on it, which is how to carry an
        /// `Authorization: Bearer …` token without inlining it. A name the protocol owns
        /// (`content-type`, `content-encoding`, `content-length`,
        /// `x-prometheus-remote-write-version`, `user-agent`) or an HTTP/2 pseudo-header starting
        /// with `:` is rejected, as are two keys naming the same header once case is ignored.
        /// Sender mode only.
        #[serde(default)]
        headers: HashMap<String, String>,
        /// Client-side TLS tuning for an `https://` `endpoint:`. A non-default block under a plain
        /// `http://` endpoint is rejected. Sender mode only. Prefixed `endpoint_`, matching
        /// `prometheus_in`'s `scrape_tls`/`bind_tls`, because this kind has two modes.
        #[serde(default)]
        endpoint_tls: TlsClientConfig,
    },
    /// A sink that drops everything, as cheaply as the runtime allows; the sink end of the
    /// load-test harness. It measures everything upstream of a sink without a real one's
    /// encoder, socket, or filesystem in the number, while the runtime's own telemetry still
    /// counts what it received. No fields: a scenario that wants an encoder in the measurement
    /// uses `file_out` to `/dev/null` instead.
    NullOut {},
    /// A named destination a router directs events into. No fields and no `sources:`: a target
    /// is fed by a router that names it, never by naming anything itself, and must be directed
    /// to by at least one router. Downstream components read it like any other component, by
    /// listing it in their own `sources:`.
    Target {},
}

/// Which remote-write protobuf message `prometheus_out`'s `endpoint:` sender writes, spelled as
/// the integer the specs are numbered by: `version: 1` or `version: 2`. Defaults to `1`, which
/// every deployed receiver accepts; 2.0 support is still uneven.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(try_from = "u8", into = "u8")]
pub enum RemoteWriteVersion {
    /// Remote-write 1.0 (`prometheus.WriteRequest`).
    #[default]
    V1,
    /// Remote-write 2.0 (`io.prometheus.write.v2.Request`): symbol table, inline metadata,
    /// created timestamps.
    V2,
}

impl TryFrom<u8> for RemoteWriteVersion {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(RemoteWriteVersion::V1),
            2 => Ok(RemoteWriteVersion::V2),
            other => {
                Err(format!("unknown remote-write version {other} -- only 1 and 2 are defined"))
            }
        }
    }
}

impl From<RemoteWriteVersion> for u8 {
    fn from(version: RemoteWriteVersion) -> u8 {
        match version {
            RemoteWriteVersion::V1 => 1,
            RemoteWriteVersion::V2 => 2,
        }
    }
}

/// How `prometheus_out`'s `endpoint:` sender compresses a request body. Defaults to `snappy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RemoteWriteCompression {
    /// Snappy block compression, what both remote-write specs mandate.
    #[default]
    Snappy,
    /// zstd, the VictoriaMetrics remote write protocol. Remote-write 1.0 only.
    Zstd,
}

/// `prometheus_out`'s `path` default. `pub` so graph validation can tell a set registry-mode
/// field from a defaulted one.
pub fn default_prometheus_path() -> String {
    "/metrics".to_string()
}

/// What `prometheus_in`'s remote-write receiver remembers about metric types across requests
/// (`metadata_cache:`), so a Prometheus 1.0 sender's writes decode as typed families.
///
/// 1.0 carries a family's type, `# HELP`, and `# UNIT` in `WriteRequest.metadata[]`, and
/// Prometheus's own sender ships those in separate requests on their own schedule (by default
/// once a minute) rather than with the samples they describe. A receiver that remembers nothing
/// sees, for nearly every request, flat series with no type anywhere: every family decodes as
/// `unknown`, and `http_request_duration_seconds_bucket`/`_sum`/`_count` arrive as three
/// unrelated series instead of one histogram. Nothing is lost (samples and labels are exact, and
/// a relay back out to remote-write is still a fixed point), but the kinds are flatter than the
/// producer's.
///
/// So the receiver keeps a table of family name to (type, help, unit), fed by every declaration
/// any request carries that names a type (1.0's `metadata[]` and 2.0's inline metadata alike,
/// so a mixed fleet fills one table; an `UNKNOWN`/`UNSPECIFIED` entry declares nothing and is
/// not learned) and consulted for a family whose own request declared nothing. The request
/// always wins: a sender that retypes a family retypes it immediately. 2.0 senders need none of
/// this, and neither does a 1.0 sender that attaches metadata to its own writes.
///
/// It is one table per component, shared by every sender that can reach the listener. That lets
/// a 2.0 sender's declarations type a 1.0 sender's series, and it equally means a peer that
/// declares a great many families evicts other peers' entries, leaving well-behaved senders
/// untyped until their next metadata write. The receiver authenticates no one, so do not point
/// it at untrusted senders. `max_families: 0` turns the table off along with the typing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MetadataCacheConfig {
    /// How many families the receiver remembers at once. Over the cap the least-recently-seen
    /// entry is evicted first, counted
    /// `logit.input.metadata_cache.evicted{reason="cardinality"}`. Defaults to `10000`, a
    /// generous ceiling on distinct families (not series): a large Prometheus scrapes tens of
    /// thousands of series across low thousands of families.
    ///
    /// An entry is a family name plus its `# HELP` and `# UNIT` text, each bounded at 1 KiB as
    /// remembered (longer text is truncated and counted `logit.input.metadata_cache.truncated`),
    /// so the resident bound is roughly `max_families x (name + 2 KiB)`: about 20 MiB at the
    /// default, and far less in practice. The family name is the sender's and is not bounded
    /// here.
    ///
    /// `0` turns the cache off: nothing is remembered, nothing is swept, and 1.0 requests decode
    /// as a stateless receiver's do. That is the setting for a pure-2.0 fleet, or for one where
    /// the extra state is not wanted.
    #[serde(default = "default_metadata_cache_max_families")]
    pub max_families: usize,
    /// How long a family is remembered after the last request that declared it. Defaults to
    /// `10m`, ten times Prometheus's own default metadata cadence, so a sender has to miss ten
    /// refreshes running before its types lapse. An expired entry is dropped, counted
    /// `logit.input.metadata_cache.evicted{reason="expired"}`, and the families it typed decode
    /// as `unknown` again until the sender's next metadata request. `0s` is rejected;
    /// `max_families: 0` is how the cache is turned off.
    #[serde(default = "default_metadata_cache_ttl", with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub ttl: Duration,
}

impl Default for MetadataCacheConfig {
    fn default() -> Self {
        MetadataCacheConfig {
            max_families: default_metadata_cache_max_families(),
            ttl: default_metadata_cache_ttl(),
        }
    }
}

/// `MetadataCacheConfig::max_families`' default. `pub` for graph validation.
pub fn default_metadata_cache_max_families() -> usize {
    10_000
}

/// `MetadataCacheConfig::ttl`'s default. `pub` for graph validation.
pub fn default_metadata_cache_ttl() -> Duration {
    Duration::from_secs(600)
}

/// `prometheus_in`'s `path` default: the route every remote-write sender and receiver uses by
/// convention. `pub` for graph validation.
pub fn default_prometheus_write_path() -> String {
    "/api/v1/write".to_string()
}

fn default_generate_batch() -> usize {
    100
}

fn default_generate_metric_value() -> f64 {
    1.0
}

/// `prometheus_out`'s `expire_after` default. `pub` for graph validation.
pub fn default_prometheus_expire_after() -> Duration {
    Duration::from_secs(300)
}

/// `prometheus_out`'s `max_series` default. `pub` for graph validation.
pub fn default_prometheus_max_series() -> usize {
    100_000
}

/// `prometheus_out`'s `timeout` default, the same 10s `otlp_out` uses for one HTTP request.
/// `pub` for graph validation.
pub fn default_prometheus_endpoint_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_max_message_bytes() -> u64 {
    8192
}

fn default_docker_root() -> String {
    "/var/lib/docker/containers".to_string()
}

/// `internal`'s `logs` field: which of `logit`'s own self-log events are captured into the
/// pipeline as ordinary log events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InternalLogs {
    /// Capture `warn` and `error` events. The default.
    #[default]
    Warn,
    /// Capture only `error` events.
    Error,
    /// Install no capturing layer at all.
    Off,
}

/// Where a tailed file starts reading the first time it is seen, when no checkpoint entry names
/// it. A checkpoint entry always wins, and a file discovered after startup always starts at
/// `beginning` regardless of this setting (a file that didn't exist yet has nothing to skip).

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReadFrom {
    /// Skip whatever the file already holds and tail only new lines. The default.
    #[default]
    End,
    /// Replay the file's entire existing content, then continue tailing.
    Beginning,
}

/// How a tailed source notices new lines and new or removed files.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum WatchMode {
    /// `inotify` where available (Linux only), falling back to `poll` if it can't be set up (an
    /// exhausted `fs.inotify.max_user_instances`, say). The default.
    #[default]
    Auto,
    /// Always `inotify`: a startup error on a non-Linux build or if `inotify` can't be set up,
    /// rather than a silent fallback.
    Inotify,
    /// Always the `poll_interval` tick, even on Linux. Higher latency (new data waits up to
    /// `poll_interval`), but works over filesystems (some network or FUSE mounts) where
    /// `inotify` events don't reliably fire.
    Poll,
}

/// Options shared by `tail_in` and `docker_in`, written directly on the component rather than
/// under a sub-block. An unrecognized field here is silently ignored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
pub struct TailOptions {
    /// Where read offsets are persisted, so a restart resumes instead of replaying or skipping.
    /// Omitted (the default) means no checkpoint: every restart re-applies `read_from` to every
    /// file as if newly discovered. A relative path resolves against the config file's
    /// directory. Must be unique per component. Each write goes through `<checkpoint_path>.tmp`
    /// beside it, so two `tail_in`/`docker_in` components sharing a path, or one whose path is
    /// another's `.tmp`, are rejected. A checkpoint that exists but can't be read replays every
    /// file from its beginning, whatever `read_from` says.
    #[serde(default)]
    pub checkpoint_path: Option<String>,
    #[serde(default)]
    pub read_from: ReadFrom,
    #[serde(default)]
    pub watch: WatchMode,
    /// The read/rescan cadence, used as-is under `watch: poll` and as a reconciliation pass under
    /// `watch: inotify`/`auto` (catching a rename, a rotation, or an event `inotify` missed).
    /// Never disabled. Defaults to `1s`; `0s` is rejected.
    #[serde(default = "default_poll_interval", with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub poll_interval: Duration,
    /// How long a dirty checkpoint may sit before being flushed to disk, in addition to a flush
    /// on every file close and at shutdown. Not "every line": a checkpoint only bounds how much
    /// a crash can replay, and replay is safe for an at-least-once pipeline. Defaults to `5s`;
    /// `0s` is rejected.
    #[serde(default = "default_checkpoint_interval", with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub checkpoint_interval: Duration,
    /// A line longer than this is dropped whole (not truncated) and diagnosed, so a downstream
    /// JSON parser never sees a value that looks well-formed but isn't the real line. A
    /// byte-count string. Defaults to `"1MiB"`; `0` is rejected.
    #[serde(default = "default_max_line_bytes", with = "human_bytes")]
    #[schemars(with = "String")]
    pub max_line_bytes: u64,
}

impl Default for TailOptions {
    fn default() -> Self {
        Self {
            checkpoint_path: None,
            read_from: ReadFrom::default(),
            watch: WatchMode::default(),
            poll_interval: default_poll_interval(),
            checkpoint_interval: default_checkpoint_interval(),
            max_line_bytes: default_max_line_bytes(),
        }
    }
}

fn default_poll_interval() -> Duration {
    Duration::from_secs(1)
}

fn default_checkpoint_interval() -> Duration {
    Duration::from_secs(5)
}

fn default_max_line_bytes() -> u64 {
    1024 * 1024
}

/// `csv`'s `delimiter` default.
fn default_csv_delimiter() -> char {
    ','
}

/// `shape`'s `resource` field: whether the batch resource a `shape` sees is forwarded or
/// replaced. `drop` is the default because a resource's attributes are observed values like any
/// other; `keep` opts that identity into flowing downstream in exchange for a per-service
/// breakdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ShapeResource {
    /// Substitute an empty resource, so no resource attribute value flows out. The default.
    #[default]
    Drop,
    /// Forward the incoming resource unchanged.
    Keep,
}

/// `shape`'s `interval` default: ten seconds, matching the reference `aggregate` window.
fn default_shape_interval() -> Duration {
    Duration::from_secs(10)
}

/// `shape`'s `max_tracked_keys` default. `logit-transforms`' `DEFAULT_MAX_TRACKED_KEYS` mirrors
/// it for a direct `Shape::new` caller.
fn default_max_tracked_keys() -> usize {
    4096
}

/// `shape`'s `max_tracked_keysets` default.
fn default_max_tracked_keysets() -> usize {
    4096
}

/// `flatten`'s `arrays` field: whether an array expands by index or is treated as a leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FlattenArrays {
    /// Expand by index (`tags.0`, `tags.1`). The default.
    #[default]
    Index,
    /// Leave the array as a leaf, written back whole at its path.
    Skip,
}

/// `flatten`'s `attributes` default: every nested attribute.
fn default_flatten_attributes() -> FlattenFields {
    FlattenFields::Keyword(FlattenKeyword::All)
}

/// `flatten`'s `resource` default: nothing.
fn default_flatten_resource() -> FlattenFields {
    FlattenFields::Keyword(FlattenKeyword::None)
}

/// `aggregate`'s `distributions` field: whether a raw samples series (statsd `ms`/`h`/`d`)
/// absorbs as a sketch or keeps its raw values for the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Distributions {
    /// Sketch every value on absorb; no raw samples survive the window. The default: bounded
    /// memory however many samples a series sees.
    #[default]
    Sketch,
    /// Keep raw values for the whole window (bounded by `max_samples_per_series`), only sketching
    /// on overflow or a sample-rate mismatch.
    Samples,
}

/// `aggregate`'s `sets` field: whether a raw set-members series (statsd `s`) absorbs as a
/// HyperLogLog estimate or keeps its exact member set for the window.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Sets {
    /// Insert every member into a HyperLogLog on absorb; no exact member set survives the window.
    /// The default: bounded memory however many distinct members a series sees.
    #[default]
    Estimate,
    /// Keep the exact, deduplicated member set for the whole window (bounded by
    /// `max_set_members_per_series`), only falling back to an estimate on overflow.
    Members,
}

/// `aggregate`'s `temporality` field: what a flushed `Sum`/`Histogram` means.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AggregateTemporality {
    /// Every window emits its own increment and the accumulator resets at each flush: tumbling
    /// windows, what an InfluxDB/statsd-shaped consumer expects. The default.
    #[default]
    Delta,
    /// A `Sum`/`Histogram` accumulator survives the flush and keeps summing, so every window
    /// emits the running total since the series was first seen, stamped with that first-seen
    /// time as its start timestamp. Required by `prometheus_out`, which skips delta records.
    /// A running total lives only as long as `series_retention` and `max_retained_series` keep
    /// its series, so both must be at least `1`.
    Cumulative,
}

/// `aggregate`'s `series_retention` default. Retention is on by default; `0` is an explicit
/// opt-out.
fn default_series_retention() -> u32 {
    5
}

/// `aggregate`'s `max_retained_series` default.
fn default_max_retained_series() -> usize {
    10_000
}

/// `aggregate`'s `max_samples_per_series` default, on the order of
/// `logit_core::Samples::MAX_WEIGHT`.
fn default_max_samples_per_series() -> usize {
    1000
}

/// `aggregate`'s `max_set_members_per_series` default.
fn default_max_set_members_per_series() -> usize {
    1000
}

/// Mirrors `logit_outputs::syslog::DEFAULT_CONNECT_TIMEOUT`. `logit-outputs` depends on this
/// crate, never the reverse, so the two are kept in sync by hand.
fn default_syslog_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

/// The one `handshake_timeout` default shared by every TCP listener kind. Mirrors
/// `logit_inputs::tcp::HANDSHAKE_TIMEOUT` and `logit_inputs::logit::HANDSHAKE_TIMEOUT`, kept in
/// sync by hand. `pub` so graph validation can tell a set value from a defaulted one.
pub fn default_handshake_timeout() -> Duration {
    Duration::from_secs(5)
}

/// Mirrors `logit_outputs::logit::DEFAULT_TIMEOUT`, kept in sync by hand.
fn default_logit_out_request_timeout() -> Duration {
    Duration::from_secs(10)
}

/// Mirrors `logit_outputs::statsd::DEFAULT_MAX_PACKET_BYTES`, kept in sync by hand.
fn default_statsd_max_packet_bytes() -> u64 {
    1432
}

/// Mirrors `logit_outputs::statsd::DEFAULT_CONNECT_TIMEOUT`, kept in sync by hand.
fn default_statsd_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

/// Mirrors `logit_proto::collectd::DEFAULT_MAX_PACKET_BYTES`, kept in sync by hand.
fn default_collectd_max_packet_bytes() -> u64 {
    1452
}

/// Mirrors `logit_proto::graphite::DEFAULT_MAX_LINE_BYTES`, kept in sync by hand.
fn default_graphite_max_line_bytes() -> u64 {
    8192
}

/// Mirrors `logit_proto::graphite::DEFAULT_MAX_FRAME_BYTES` (Twisted's
/// `Int32StringReceiver.MAX_LENGTH`, which carbon's pickle receiver inherits), kept in sync by
/// hand. Shared by `graphite_in`'s and `graphite_out`'s `max_frame_bytes`.
fn default_graphite_max_frame_bytes() -> u64 {
    1 << 20
}

/// Mirrors `logit_proto::graphite::DEFAULT_MAX_PACKET_BYTES`, kept in sync by hand.
fn default_graphite_max_packet_bytes() -> u64 {
    1432
}

/// `graphite_out`'s `connect_timeout` default, matching `statsd_out`'s and `syslog_out`'s.
fn default_graphite_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

/// `prometheus_in`'s `interval` default, Prometheus's own default scrape interval. `pub` so
/// graph validation can tell a set value from a defaulted one.
pub fn default_prometheus_scrape_interval() -> Duration {
    Duration::from_secs(15)
}

/// `prometheus_in`'s `timeout` default, matching `otlp_out`'s. `pub` for graph validation.
pub fn default_prometheus_scrape_timeout() -> Duration {
    Duration::from_secs(10)
}

fn default_trace_id_field() -> String {
    "trace.id".to_string()
}

fn default_span_id_field() -> Option<String> {
    Some("span.id".to_string())
}

fn default_flags_field() -> Option<String> {
    Some("trace.flags".to_string())
}

/// `trace_context`'s `span:` block: the defaults a minted span falls back on when the event's
/// own `span.name`/`span.kind` attributes are absent, plus two knobs that aren't per-event data.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpanLiftConfig {
    /// Mint a fresh span id when the `span_id` attribute is absent, instead of skipping the
    /// event (`skipped{reason="span_id"}`). Off by default: `logit` never invents identity for
    /// data it didn't produce unless asked. A missing trace id is never minted.
    #[serde(default)]
    pub mint_id: bool,
    /// The span name when the event carries no `span.name` attribute. Defaults to
    /// `http.request`; an empty string is rejected, because OTLP requires a span name.
    #[serde(default = "default_span_name")]
    pub name: String,
    /// The span kind when the event carries no `span.kind` attribute. Defaults to `server`,
    /// since an access log line is a server span.
    #[serde(default)]
    pub kind: SpanKindConfig,
    /// A resolved start or end further than this from the event's receipt time is rejected
    /// (`skipped{reason="skew"}`) rather than written, so one sender with a badly wrong clock
    /// can't write spans years away and poison a trace store. Defaults to `1h`; `0s` is
    /// rejected.
    #[serde(default = "default_max_skew", with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub max_skew: Duration,
}

impl Default for SpanLiftConfig {
    fn default() -> Self {
        SpanLiftConfig {
            mint_id: false,
            name: default_span_name(),
            kind: SpanKindConfig::default(),
            max_skew: default_max_skew(),
        }
    }
}

fn default_span_name() -> String {
    "http.request".to_string()
}

fn default_max_skew() -> Duration {
    Duration::from_secs(3600)
}

/// OTLP's span kinds, as config vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SpanKindConfig {
    Internal,
    #[default]
    Server,
    Client,
    Producer,
    Consumer,
}

/// The transport `syslog_in` listens on and `syslog_out` sends over. `udp` (the default) is what
/// nginx's `syslog:` writer and most senders speak; a datagram sent before the receiver is up is
/// lost. `tcp` is the reliable, framed transport, and what `tls:` (RFC 5425) needs underneath
/// it. `syslog_out` always emits octet-counted frames; `syslog_in` accepts either RFC 6587
/// framing, detected per connection.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyslogTransport {
    #[default]
    Udp,
    Tcp,
}

/// Which syslog dialect `syslog_out` emits. `rfc5424` is the default.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyslogFormat {
    Rfc3164,
    #[default]
    Rfc5424,
}

/// `syslog_out`'s opt-in extra RFC 5424 STRUCTURED-DATA element. `sd_id` must be a valid
/// `SD-NAME` (RFC 5424 section 6.3.2: 1 to 32 printable US-ASCII characters excluding `=`, SP,
/// `]`, `"`) containing exactly one `@`, a private-enterprise-number-qualified id such as
/// `myapp@12345`. No default PEN is shipped: RFC 5424's `32473` example is documentation only,
/// so registering a PEN with IANA, or reusing one you hold, is your decision. `logit run` rejects
/// an invalid id at startup; `logit validate` doesn't check it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SyslogStructuredData {
    pub sd_id: String,
}

/// `statsd_in`'s and `statsd_out`'s transport. `udp` (the default) matches classic statsd and
/// DogStatsD clients; `tcp` is the reliable, framed transport, and what `tls:` needs underneath
/// it. A TCP message is one LF-delimited line in both directions.
// Its own enum rather than a shared one: schemars publishes a type's name into the schema's
// `$defs`, so sharing would document this transport by pointing at a syslog-named type.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StatsdTransport {
    #[default]
    Udp,
    Tcp,
}

/// Which statsd dialect `statsd_out` emits. `dogstatsd` (the default) includes the tag segment.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StatsdFormat {
    #[default]
    Dogstatsd,
    Statsd,
}

/// `graphite_in`'s and `graphite_out`'s transport. `tcp` (the default) matches carbon's own
/// default listener; `udp` is carbon's other plaintext mode. `protocol: pickle` under
/// `transport: udp` is rejected.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GraphiteTransport {
    #[default]
    Tcp,
    Udp,
}

/// Which carbon wire protocol `graphite_in`/`graphite_out` speaks: `plaintext` (`path[;k=v...]
/// value timestamp`, one line per datapoint, carbon's port 2003) or `pickle` (a 4-byte
/// big-endian length prefix then a pickled `[(path, (timestamp, value)), ...]`, carbon's port
/// 2004). `pickle` requires `transport: tcp`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GraphiteProtocol {
    #[default]
    Plaintext,
    Pickle,
}

/// Whether `graphite_out` renders attributes as carbon `;k=v` tags (`carbon`, the default) or
/// drops them (`drop`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GraphiteTags {
    #[default]
    Carbon,
    Drop,
}

/// What `graphite_out` does with a metric kind carbon's one-number-per-datapoint wire cannot
/// carry: drop it, counted (`skip`, the default), or render one dotted sub-path per component
/// value (`expand`).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum GraphiteMultiValue {
    #[default]
    Skip,
    Expand,
}

/// The syslog PRI facility, named rather than a bare `0..=23` integer so a typo is a config error
/// rather than a silently wrong PRI. Ordered to match the standard facility codes (`kern` is 0).
/// `local0` is the default.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyslogFacility {
    Kern,
    User,
    Mail,
    Daemon,
    Auth,
    Syslog,
    Lpr,
    News,
    Uucp,
    Cron,
    Authpriv,
    Ftp,
    Ntp,
    Security,
    Console,
    SolarisCron,
    #[default]
    Local0,
    Local1,
    Local2,
    Local3,
    Local4,
    Local5,
    Local6,
    Local7,
}

impl SyslogFacility {
    pub fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Where `stdio_out` writes, as a plain scalar (`target: stdout`): `stdout` and `stderr` are
/// matched as keywords first, and anything else is a file path. A relative path resolves against
/// the config file's directory, not the process's working directory.
///
/// Two consequences: a file named `stdout` or `stderr` next to the config is reachable only as
/// `./stdout`, and a typo like `stdrr` silently becomes a file path rather than a config error
/// (visible as soon as the wrongly named file appears next to the config).
// `Serialize`/`Deserialize`/`JsonSchema` are hand-rolled: a derived `#[serde(untagged)]` matches
// a unit variant's shape ("absent/null"), never the literal string `"stdout"`, so every value
// would fall through to `Path`. `JsonSchema` delegates to `String`'s schema, since every value
// this type accepts is a string.
#[derive(Debug, Default, Clone, PartialEq)]
pub enum StdioTarget {
    #[default]
    Stdout,
    Stderr,
    Path(String),
}

impl Serialize for StdioTarget {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let s = match self {
            StdioTarget::Stdout => "stdout",
            StdioTarget::Stderr => "stderr",
            StdioTarget::Path(path) => path.as_str(),
        };
        serializer.serialize_str(s)
    }
}

impl<'de> Deserialize<'de> for StdioTarget {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        Ok(match s.as_str() {
            "stdout" => StdioTarget::Stdout,
            "stderr" => StdioTarget::Stderr,
            _ => StdioTarget::Path(s),
        })
    }
}

impl JsonSchema for StdioTarget {
    fn schema_name() -> String {
        "StdioTarget".to_string()
    }

    fn json_schema(generator: &mut SchemaGenerator) -> Schema {
        String::json_schema(generator)
    }
}

/// The largest `rotate.max_files` graph validation accepts. It counts the active file, so 1000
/// keeps 999 rotated files: about 2.7 years of daily files, or 41 days of hourly ones. Every
/// rotation stats and renames each retained file, so a larger value makes each rotation slower
/// without a use a count-based policy needs.
pub const MAX_ROTATE_FILES: u32 = 1000;

/// `file_out`'s rotation policy. At least one of `max_bytes`/`interval` must be set; a config
/// that would never rotate is rejected (use `stdio_out` for an unrotated file). `max_files`
/// counts every file `file_out` maintains, active plus rotated, so `max_files * max_bytes` reads
/// as a disk budget.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct RotateConfig {
    /// Rotate once the active file reaches this size. A quoted byte-count string (`"64MiB"` or
    /// `"134217728"`). Omitted (the default) means size never triggers a rotation; `0` is
    /// rejected.
    #[serde(with = "human_bytes::option")]
    #[schemars(with = "Option<String>")]
    pub max_bytes: Option<u64>,
    /// Rotate on this UTC calendar boundary. Omitted (the default) means the calendar never
    /// triggers a rotation.
    #[serde(default)]
    pub interval: Option<RotateInterval>,
    /// Files to keep, active plus rotated. Defaults to `5`. Must be between `1` and `1000`
    /// (`MAX_ROTATE_FILES`): 1000 keeps 999 rotated files, about 2.7 years of daily files or 41
    /// days of hourly ones.
    pub max_files: u32,
}

impl Default for RotateConfig {
    fn default() -> Self {
        Self { max_bytes: None, interval: None, max_files: default_max_files() }
    }
}

fn default_max_files() -> u32 {
    5
}

/// Which calendar boundary `file_out` rotates on, in UTC, never the host's local zone. A calendar
/// period rather than a duration, so a daily file doesn't drift against the wall clock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RotateInterval {
    Hourly,
    Daily,
}

/// Which encoder a stream sink (`stdio_out`/`file_out`) writes through: `human` (the default) is
/// the readable text render; `native` is `logit`'s own wire format, in which every frame is
/// independently decodable, which is what a rotated-away file needs.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StreamFormat {
    #[default]
    Human,
    Native,
}

/// Per-frame compression for `logit`'s native wire format. There is no `zstd` variant: the native
/// codec rejects it on both encode and decode.
// Mirrors `logit_proto::frame::Compression`; `logit-config` must not depend on `logit-proto`, and
// `logit-cli` converts between the two.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Compression {
    #[default]
    None,
    Lz4,
}

/// Per-sink delivery buffer. Meaningful only on a sink; a non-default block on any other kind is
/// rejected. Every field defaults, so the block is never required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct BufferConfig {
    /// Batches the queue may hold ahead of the sink. Defaults to `1024`; `0` is rejected.
    pub max_batches: usize,
    /// Byte bound on the buffer's estimated heap footprint, checked alongside `max_batches`,
    /// whichever trips first. A quoted byte-count string (`"64MiB"` or `"134217728"`); an
    /// unquoted number is rejected. Defaults to `"64MiB"`; `0` is rejected.
    #[serde(with = "human_bytes")]
    #[schemars(with = "String")]
    pub max_bytes: u64,
    /// What happens once both bounds are full. Defaults to `block`.
    pub overflow: OverflowPolicy,
    /// Whether re-delivering an already-delivered batch is acceptable for this sink's
    /// destination. Omitted (the default) derives it from the sink kind; set it to override for
    /// this component.
    #[serde(default)]
    pub delivery: Option<DeliveryPosture>,
    /// Hard ceiling on the total time spent retrying one batch, across every attempt and backoff
    /// sleep. Defaults to `60s`.
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub retry_budget: Duration,
    /// Cap on the exponential backoff between retry attempts. Defaults to `10s`.
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub retry_max_delay: Duration,
    /// How long the sink keeps draining after a shutdown signal before being cancelled. Defaults
    /// to `5s`.
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub shutdown_grace: Duration,
    /// Disk-backed durable buffering, opt-in. Omitted (the default) keeps the in-memory queue.
    /// Present, it replaces that queue with a disk spool at `disk.path`; `max_batches`/`max_bytes`
    /// are rejected at anything but their defaults alongside it, since the disk bound replaces
    /// the in-memory one.
    #[serde(default)]
    pub disk: Option<DiskBufferConfig>,
}

impl Default for BufferConfig {
    fn default() -> Self {
        Self {
            max_batches: 1024,
            max_bytes: 64 * 1024 * 1024,
            overflow: OverflowPolicy::Block,
            delivery: None,
            retry_budget: Duration::from_secs(60),
            retry_max_delay: Duration::from_secs(10),
            shutdown_grace: Duration::from_secs(5),
            disk: None,
        }
    }
}

/// Disk-backed durable buffering for one sink's delivery queue, opt-in via `buffer.disk:`. Its
/// presence turns disk backing on for that sink. `path` is required.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiskBufferConfig {
    /// The spool directory. A relative path resolves against the config file's directory. Two
    /// sinks may not share one.
    pub path: String,
    /// Bound on the sum of on-disk segment sizes; replaces `buffer.max_bytes`'s role. A
    /// byte-count string. Defaults to `"1GiB"`; `0` is rejected.
    #[serde(default = "default_disk_max_bytes")]
    #[serde(with = "human_bytes")]
    #[schemars(with = "String")]
    pub max_bytes: u64,
    /// A soft rotation trigger, not a hard cap: the active segment rotates once it already
    /// exceeds this, so a single record larger than it still lands whole in a fresh segment. A
    /// byte-count string. Defaults to `"64MiB"`; `0`, or a value above `max_bytes`, is rejected.
    #[serde(default = "default_segment_bytes")]
    #[serde(with = "human_bytes")]
    #[schemars(with = "String")]
    pub segment_bytes: u64,
    /// Per-frame compression of the spooled frames. Defaults to `none`.
    #[serde(default)]
    pub compression: Compression,
    /// How often the read cursor is persisted during ordinary operation; also forced on segment
    /// rotation and at shutdown. Defaults to `1s`.
    #[serde(default = "default_disk_checkpoint_interval")]
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub checkpoint_interval: Duration,
}

fn default_disk_max_bytes() -> u64 {
    1024 * 1024 * 1024
}

fn default_segment_bytes() -> u64 {
    64 * 1024 * 1024
}

fn default_disk_checkpoint_interval() -> Duration {
    Duration::from_secs(1)
}

/// What a sink's delivery queue, or a datagram listener's receive queue, does once both its
/// bounds are full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    /// Wait for room.
    Block,
    /// Evict the oldest queued item to admit the new one.
    DropOldest,
    /// Discard the arriving item.
    DropNewest,
}

/// Whether re-delivering an already-delivered batch is acceptable for a sink's destination.
/// `at_least_once` retries as aggressively as fault classification allows and risks a
/// duplicate; `at_most_once` is the conservative posture.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPosture {
    AtLeastOnce,
    AtMostOnce,
}

/// Per-listener receive queue and datagram-to-batch assembly. A datagram listener (`collectd_in`,
/// and `statsd_in`/`syslog_in`/`graphite_in` under `transport: udp`) accepts every field; the
/// same three under `transport: tcp`, `tail_in`, and `docker_in` accept only the batch-assembly
/// fields (`batch_max_events`, `batch_max_bytes`, `batch_flush_interval`) and `shutdown_grace`;
/// a non-default block on any other kind is rejected. Every field defaults, so the block is never
/// required. The defaults batch: `batch_max_events: 1000` and `batch_flush_interval: 100ms` mean
/// a default-configured listener amortizes datagrams into batches (up to 1000 events, or up to
/// 100ms of added latency before a send). For one send per datagram with no added latency, set
/// `batch_max_events: 1`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct ReceiveConfig {
    /// Datagrams the read half may hold ahead of the decode half. Defaults to `10000`; `0` is
    /// rejected.
    pub max_datagrams: usize,
    /// Byte bound on the queue (undecoded datagram bytes), checked alongside `max_datagrams`,
    /// whichever trips first. A byte-count string. Defaults to `"32MiB"`; `0` is rejected.
    #[serde(with = "human_bytes")]
    #[schemars(with = "String")]
    pub max_bytes: u64,
    /// What happens once both bounds are full. Defaults to `drop_oldest`, unlike `buffer:`'s
    /// `block`: blocking a UDP reader backpressures the kernel, which discards the datagram into
    /// a counter this process never reads, so `block` relocates loss out of view rather than
    /// preventing it.
    pub overflow: OverflowPolicy,
    /// Events to accumulate across datagrams before one send downstream. Defaults to `1000`; `0`
    /// is rejected. `1` means one send per datagram, since the accumulator flushes on a bound
    /// reached or exceeded and never splits a single decode's output.
    pub batch_max_events: usize,
    /// Byte bound on an accumulated batch, checked alongside `batch_max_events`. A byte-count
    /// string. Defaults to `"1MiB"`; `0` is rejected.
    #[serde(with = "human_bytes")]
    #[schemars(with = "String")]
    pub batch_max_bytes: u64,
    /// Longest an accumulated batch waits before being sent regardless of size. Defaults to
    /// `100ms`. `0s` disables the timer (`batch_max_events` and `batch_max_bytes` are then the
    /// only triggers) and is not rejected.
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub batch_flush_interval: Duration,
    /// `SO_RCVBUF`, requested at bind. A byte-count string. Omitted (the default) leaves the
    /// kernel default alone. The kernel clamps a request above `net.core.rmem_max` (212992 on
    /// most stock kernels), and `logit` logs a warning at startup when the kernel grants less
    /// than was requested.
    #[serde(with = "human_bytes::option")]
    #[schemars(with = "Option<String>")]
    pub receive_buffer_bytes: Option<u64>,
    /// How long a listener keeps draining after a shutdown signal before being cancelled.
    /// Defaults to `5s`, matching `buffer.shutdown_grace`, so both ends of the pipeline drain on
    /// the same number.
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub shutdown_grace: Duration,
    /// Datagrams one `recvmmsg(2)` call may return, and how many the decode half takes off the
    /// receive queue at a time: one knob for both ends of one queue. Defaults to `64`; `0` is
    /// rejected, and so is anything above `1024`. `1` is one datagram per syscall.
    ///
    /// Linux only, in effect: every other target keeps a one-datagram-per-read loop and ignores
    /// this field, which still parses and validates everywhere so one config stays portable. The
    /// decode-side batch it also sets applies everywhere.
    ///
    /// The read half owns one slab of `read_batch` x 65,507-byte slots per listener: 4 MiB of
    /// address space at the default, 64 MiB at the ceiling. Only the pages a datagram is written
    /// into are faulted in, so the resident cost tracks real datagram sizes. A shutdown landing
    /// mid-push drops whatever the read half was holding, uncounted: up to `read_batch`
    /// datagrams, on the shutdown path only. A `read_batch` larger than `max_datagrams` is legal:
    /// the batch is admitted item by item under `overflow`.
    pub read_batch: usize,
}

/// Defaults derived from established UDP listeners' own tuning (Telegraf, gostatsd, DogStatsD,
/// rsyslog, syslog-ng); ADR `decoupled-listener-io` has the derivation.
impl Default for ReceiveConfig {
    fn default() -> Self {
        Self {
            max_datagrams: 10_000,
            max_bytes: 32 * 1024 * 1024,
            overflow: OverflowPolicy::DropOldest,
            batch_max_events: 1_000,
            batch_max_bytes: 1024 * 1024,
            batch_flush_interval: Duration::from_millis(100),
            receive_buffer_bytes: None,
            shutdown_grace: Duration::from_secs(5),
            read_batch: default_read_batch(),
        }
    }
}

/// `receive.read_batch`'s default, as a function so graph validation compares against the same
/// value this struct is built from. 64 is in the range Telegraf, rsyslog's `imudp`, and gostatsd
/// default their equivalent knob to, and the measured sweet spot against this codebase's own
/// decode and queue costs.
pub const fn default_read_batch() -> usize {
    64
}

/// The ceiling graph validation enforces on `receive.read_batch`. 1024 is `UIO_MAXIOV`'s number,
/// but the ceiling is `logit`'s own choice, not a kernel limit: the kernel clamps no `recvmmsg`
/// `vlen`. What it bounds is the per-listener receive slab and the shutdown-path loss, both of
/// which grow linearly with it. `pub` so `logit_pipeline::graph` imports it;
/// `logit_inputs::udp::MAX_READ_BATCH` is an unavoidable second copy, since `logit-inputs` does
/// not depend on this crate.
pub const MAX_READ_BATCH: usize = 1024;

/// A human-readable byte-size codec (`134217728`, `64MiB`, `128KiB`, `1GiB`) for every byte-count
/// field. Binary (1024-based) units only. String-only in both directions: it always serializes as
/// a quoted decimal-integer string (`"134217728"`), never a unit suffix and never a bare number,
/// matching the `#[schemars(with = "String")]` schema on every field that uses it; a human can
/// still write `"64MiB"` on the way in.
mod human_bytes {
    use serde::{Deserializer, Serializer};

    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    /// String-only, both directions. Accepting a bare integer on input, or emitting one on output,
    /// would contradict the published `"type": "string"` schema, so a bare number is rejected
    /// rather than silently accepted.
    pub fn serialize<S: Serializer>(bytes: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&bytes.to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        struct Visitor;
        impl serde::de::Visitor<'_> for Visitor {
            type Value = u64;

            fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("a byte count string, e.g. \"134217728\" or \"64MiB\"")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<u64, E> {
                parse(v).map_err(E::custom)
            }
        }
        d.deserialize_str(Visitor)
    }

    fn parse(raw: &str) -> Result<u64, String> {
        let raw = raw.trim();
        let split_at = raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len());
        let (num, unit) = raw.split_at(split_at);
        if num.is_empty() {
            return Err(format!("expected a byte count, e.g. 134217728 or 64MiB, got '{raw}'"));
        }
        let n: u64 = num.parse().map_err(|e| format!("invalid byte count '{num}': {e}"))?;
        let multiplier = match unit.trim() {
            "" | "B" => 1,
            "KiB" => KIB,
            "MiB" => MIB,
            "GiB" => GIB,
            other => {
                return Err(format!("unknown byte-size unit '{other}' (expected B/KiB/MiB/GiB)"))
            }
        };
        n.checked_mul(multiplier).ok_or_else(|| format!("byte count '{raw}' overflows u64"))
    }

    #[cfg(test)]
    mod tests {
        use serde::Deserialize;

        #[derive(Deserialize)]
        struct Wrapper {
            #[serde(with = "super")]
            bytes: u64,
        }

        fn parse_json(json: &str) -> Result<u64, String> {
            serde_json::from_str::<Wrapper>(&format!(r#"{{"bytes": {json}}}"#))
                .map(|w| w.bytes)
                .map_err(|e| e.to_string())
        }

        #[test]
        fn a_bare_unquoted_integer_is_rejected_not_silently_accepted() {
            // String-only, both directions: a bare integer would contradict the published
            // schema's `"type": "string"` claim.
            let err = parse_json("134217728").unwrap_err();
            assert!(err.contains("byte count string"), "got: {err}");
        }

        #[test]
        fn round_trips_a_quoted_bare_integer() {
            assert_eq!(parse_json(r#""134217728""#).unwrap(), 134_217_728);
        }

        #[test]
        fn round_trips_kib() {
            assert_eq!(parse_json(r#""128KiB""#).unwrap(), 128 * 1024);
        }

        #[test]
        fn round_trips_mib() {
            assert_eq!(parse_json(r#""64MiB""#).unwrap(), 64 * 1024 * 1024);
        }

        #[test]
        fn round_trips_gib() {
            assert_eq!(parse_json(r#""1GiB""#).unwrap(), 1024 * 1024 * 1024);
        }

        #[test]
        fn rejects_garbage_with_a_clear_error_not_a_panic() {
            let err = parse_json(r#""not-a-size""#).unwrap_err();
            assert!(err.contains("byte count") || err.contains("unit"), "got: {err}");
        }

        #[test]
        fn rejects_a_negative_number() {
            // Quoted, since input is string-only; an unquoted `-5` is rejected as the wrong JSON
            // type (`an_unquoted_negative_number_is_also_rejected`). `-` isn't an ASCII digit, so
            // `parse` sees an empty numeric prefix and reports "expected a byte count".
            let err = parse_json(r#""-5""#).unwrap_err();
            assert!(err.contains("expected a byte count"), "got: {err}");
        }

        #[test]
        fn an_unquoted_negative_number_is_also_rejected() {
            let err = parse_json("-5").unwrap_err();
            assert!(err.contains("byte count string"), "got: {err}");
        }

        #[test]
        fn serialize_emits_a_string_matching_the_published_schema() {
            // Matches `#[schemars(with = "String")]`'s claim in both directions.
            #[derive(serde::Serialize)]
            struct W {
                #[serde(with = "super")]
                bytes: u64,
            }
            let json = serde_json::to_string(&W { bytes: 64 * 1024 * 1024 }).unwrap();
            assert_eq!(json, r#"{"bytes":"67108864"}"#);
        }
    }

    /// The same codec for `Option<u64>` fields, where `None` means "unset" rather than a byte
    /// count of zero. A nested module because `#[serde(with = "...")]` on an `Option<u64>` field
    /// calls this module's functions with `Option<u64>`. `None` serializes as null, `Some` as the
    /// parent codec's quoted string.
    pub mod option {
        use serde::{Deserializer, Serializer};

        pub fn serialize<S: Serializer>(bytes: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
            match bytes {
                Some(bytes) => super::serialize(bytes, s),
                None => s.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
            struct Visitor;
            impl<'de> serde::de::Visitor<'de> for Visitor {
                type Value = Option<u64>;

                fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str("a byte count string (e.g. \"64MiB\") or null")
                }

                fn visit_none<E: serde::de::Error>(self) -> Result<Option<u64>, E> {
                    Ok(None)
                }

                fn visit_unit<E: serde::de::Error>(self) -> Result<Option<u64>, E> {
                    Ok(None)
                }

                fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Option<u64>, D::Error> {
                    super::deserialize(d).map(Some)
                }
            }
            d.deserialize_option(Visitor)
        }

        #[cfg(test)]
        mod tests {
            #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize)]
            struct Wrapper {
                #[serde(default, with = "super")]
                bytes: Option<u64>,
            }

            #[test]
            fn round_trips_none_as_null() {
                let w: Wrapper = serde_json::from_str(r#"{"bytes": null}"#).unwrap();
                assert_eq!(w.bytes, None);
                assert_eq!(serde_json::to_string(&w).unwrap(), r#"{"bytes":null}"#);
            }

            #[test]
            fn an_omitted_field_defaults_to_none() {
                let w: Wrapper = serde_json::from_str("{}").unwrap();
                assert_eq!(w.bytes, None);
            }

            #[test]
            fn round_trips_a_quoted_size_as_some() {
                let w: Wrapper = serde_json::from_str(r#"{"bytes": "8MiB"}"#).unwrap();
                assert_eq!(w.bytes, Some(8 * 1024 * 1024));
                assert_eq!(serde_json::to_string(&w).unwrap(), r#"{"bytes":"8388608"}"#);
            }

            #[test]
            fn an_unquoted_number_is_still_rejected_when_present() {
                let err = serde_json::from_str::<Wrapper>(r#"{"bytes": 8388608}"#).unwrap_err();
                assert!(err.to_string().contains("byte count string"), "got: {err}");
            }
        }
    }
}

/// Minimal `humantime`-flavored `(de)serialize` for `Duration` fields (`10s`, `1m`, ...), so
/// config keeps human-readable durations without an external crate for one helper.
/// TODO: replace with the `humantime-serde` crate once the crate list is finalized.
mod humantime_serde_duration {
    use super::*;
    use serde::{de::Error as _, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(d: &Duration, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format!("{}s", d.as_secs_f64()))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Duration, D::Error> {
        let raw = String::deserialize(d)?;
        parse(&raw).map_err(D::Error::custom)
    }

    fn parse(raw: &str) -> Result<Duration, String> {
        let (num, unit) = raw.trim().split_at(
            raw.trim()
                .find(|c: char| !c.is_ascii_digit() && c != '.')
                .ok_or_else(|| "expected a number followed by a unit, e.g. 10s".to_string())?,
        );
        let n: f64 = num.parse().map_err(|e| format!("{e}"))?;
        let secs = match unit {
            "ms" => n / 1000.0,
            "s" => n,
            "m" => n * 60.0,
            "h" => n * 3600.0,
            other => return Err(format!("unknown duration unit '{other}'")),
        };
        Ok(Duration::from_secs_f64(secs))
    }

    /// The same codec for `Option<Duration>` fields. A nested module because `#[serde(with =
    /// "...")]` on an `Option<Duration>` field calls this module's functions with
    /// `Option<Duration>`.
    pub mod option {
        use super::*;

        pub fn serialize<S: Serializer>(d: &Option<Duration>, s: S) -> Result<S::Ok, S::Error> {
            match d {
                Some(d) => super::serialize(d, s),
                None => s.serialize_none(),
            }
        }

        pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Duration>, D::Error> {
            let raw: Option<String> = Option::deserialize(d)?;
            raw.map(|raw| parse(&raw).map_err(D::Error::custom)).transpose()
        }
    }
}

/// Generate the published JSON Schema for [`Config`]. Backs the `logit schema` CLI command; a
/// workspace test compares its output with the committed `schema/logit.schema.json` and fails if
/// it is stale.
pub fn json_schema() -> schemars::schema::RootSchema {
    schemars::schema_for!(Config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn committed_schema_is_current() {
        let generated = format!("{}\n", serde_json::to_string_pretty(&json_schema()).unwrap());
        let committed = include_str!("../../../schema/logit.schema.json");
        assert_eq!(
            committed, generated,
            "schema/logit.schema.json is stale; run ./script/schema and commit the result"
        );
    }

    // Deserialized via `serde_json` rather than YAML (`logit-cli` owns YAML parsing, and this
    // crate has no YAML dependency): both are self-describing, so this exercises the same
    // tagged-enum disambiguation the real deserializer does.

    #[test]
    fn lua_component_without_interval_deserializes() {
        let component: Component =
            serde_json::from_str(r#"{"type": "lua", "sources": ["in"], "script": "return event"}"#)
                .unwrap();
        assert_eq!(component.sources, vec!["in".to_string()]);
        match component.kind {
            ComponentKind::Lua { script, interval } => {
                assert_eq!(script, "return event");
                assert_eq!(interval, None);
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn lua_component_with_interval_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "lua", "sources": ["in"], "script": "return event", "interval": "10s"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Lua { interval, .. } => {
                assert_eq!(interval, Some(Duration::from_secs(10)));
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn lua_file_component_with_interval_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "lua_file", "sources": ["in"], "lua_file": "x.lua", "interval": "1m"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::LuaFile { lua_file, interval } => {
                assert_eq!(lua_file, "x.lua");
                assert_eq!(interval, Some(Duration::from_secs(60)));
            }
            other => panic!("expected LuaFile, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_component_with_interval_deserializes() {
        let component: Component =
            serde_json::from_str(r#"{"type": "aggregate", "sources": ["in"], "interval": "10s"}"#)
                .unwrap();
        match component.kind {
            ComponentKind::Aggregate {
                interval,
                temporality,
                series_retention,
                max_retained_series,
                distributions,
                max_samples_per_series,
                sets,
                max_set_members_per_series,
            } => {
                assert_eq!(interval, Duration::from_secs(10));
                // Every field but `interval` defaults.
                assert_eq!(temporality, AggregateTemporality::Delta);
                assert_eq!(series_retention, 5);
                assert_eq!(max_retained_series, 10_000);
                assert_eq!(distributions, Distributions::Sketch);
                assert_eq!(max_samples_per_series, 1000);
                assert_eq!(sets, Sets::Estimate);
                assert_eq!(max_set_members_per_series, 1000);
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_component_can_override_series_retention() {
        let component: Component = serde_json::from_str(
            r#"{"type": "aggregate", "sources": ["in"], "interval": "10s", "series_retention": 0, "max_retained_series": 100}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Aggregate { series_retention, max_retained_series, .. } => {
                assert_eq!(series_retention, 0);
                assert_eq!(max_retained_series, 100);
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    /// `temporality: cumulative` parses, `snake_case` like every other config enum.
    #[test]
    fn aggregate_component_parses_cumulative_temporality() {
        let component: Component = serde_json::from_str(
            r#"{"type": "aggregate", "sources": ["in"], "interval": "10s", "temporality": "cumulative"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Aggregate { temporality, series_retention, .. } => {
                assert_eq!(temporality, AggregateTemporality::Cumulative);
                assert_eq!(series_retention, 5, "retention keeps its own default under cumulative");
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    /// The four raw-retention fields' defaults, isolated from the retention fields so a change to
    /// one set can't mask a regression in the other.
    #[test]
    fn aggregate_component_defaults_to_sketch_and_estimate_with_1000_caps() {
        let component: Component =
            serde_json::from_str(r#"{"type": "aggregate", "sources": ["in"], "interval": "10s"}"#)
                .unwrap();
        match component.kind {
            ComponentKind::Aggregate {
                distributions,
                max_samples_per_series,
                sets,
                max_set_members_per_series,
                ..
            } => {
                assert_eq!(distributions, Distributions::Sketch);
                assert_eq!(max_samples_per_series, 1000);
                assert_eq!(sets, Sets::Estimate);
                assert_eq!(max_set_members_per_series, 1000);
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn aggregate_component_parses_all_four_w2_fields() {
        let component: Component = serde_json::from_str(
            r#"{
                "type": "aggregate",
                "sources": ["in"],
                "interval": "10s",
                "distributions": "samples",
                "max_samples_per_series": 42,
                "sets": "members",
                "max_set_members_per_series": 7
            }"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Aggregate {
                distributions,
                max_samples_per_series,
                sets,
                max_set_members_per_series,
                ..
            } => {
                assert_eq!(distributions, Distributions::Samples);
                assert_eq!(max_samples_per_series, 42);
                assert_eq!(sets, Sets::Members);
                assert_eq!(max_set_members_per_series, 7);
            }
            other => panic!("expected Aggregate, got {other:?}"),
        }
    }

    #[test]
    fn internal_component_with_interval_deserializes() {
        let component: Component =
            serde_json::from_str(r#"{"type": "internal", "interval": "10s"}"#).unwrap();
        assert!(component.sources.is_empty());
        match component.kind {
            ComponentKind::Internal { interval, .. } => {
                assert_eq!(interval, Duration::from_secs(10));
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    /// `span_sample_rate` is optional, defaulting to `logit_core::DEFAULT_SPAN_SAMPLE_RATE` (0.1).
    #[test]
    fn internal_without_span_sample_rate_defaults_to_one_tenth() {
        let component: Component =
            serde_json::from_str(r#"{"type": "internal", "interval": "10s"}"#).unwrap();
        match component.kind {
            ComponentKind::Internal { span_sample_rate, .. } => {
                assert_eq!(span_sample_rate, 0.1);
                assert_eq!(span_sample_rate, logit_core::DEFAULT_SPAN_SAMPLE_RATE);
            }
            other => panic!("expected Internal, got {other:?}"),
        }
    }

    #[test]
    fn json_component_without_skip_to_brace_defaults_to_false() {
        let component: Component =
            serde_json::from_str(r#"{"type": "json", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::Json { skip_to_brace, invalid_utf8 } => {
                assert!(!skip_to_brace);
                assert_eq!(invalid_utf8, JsonInvalidUtf8::Reject, "strict by default");
            }
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn json_component_with_skip_to_brace_deserializes() {
        let component: Component =
            serde_json::from_str(r#"{"type": "json", "sources": ["in"], "skip_to_brace": true}"#)
                .unwrap();
        match component.kind {
            ComponentKind::Json { skip_to_brace, .. } => assert!(skip_to_brace),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn json_component_invalid_utf8_replace_deserializes_and_round_trips() {
        let component: Component = serde_json::from_str(
            r#"{"type": "json", "sources": ["in"], "invalid_utf8": "replace"}"#,
        )
        .unwrap();
        match &component.kind {
            ComponentKind::Json { invalid_utf8, .. } => {
                assert_eq!(*invalid_utf8, JsonInvalidUtf8::Replace)
            }
            other => panic!("expected Json, got {other:?}"),
        }
        let json = serde_json::to_string(&component).unwrap();
        assert!(json.contains(r#""invalid_utf8":"replace""#), "round-trips as snake_case: {json}");
        let err = serde_json::from_str::<Component>(
            r#"{"type": "json", "sources": ["in"], "invalid_utf8": "lossy"}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("reject") && err.contains("replace"), "names the choices: {err}");
    }

    /// `structured_data` is optional, defaulting to `None` rather than inventing an `sd_id`.
    #[test]
    fn syslog_out_without_structured_data_defaults_to_none() {
        let component: Component =
            serde_json::from_str(r#"{"type": "syslog_out", "endpoint": "127.0.0.1:514"}"#).unwrap();
        match component.kind {
            ComponentKind::SyslogOut { structured_data, .. } => {
                assert_eq!(structured_data, None);
            }
            other => panic!("expected SyslogOut, got {other:?}"),
        }
    }

    #[test]
    fn syslog_out_structured_data_parses() {
        let component: Component = serde_json::from_str(
            r#"{"type": "syslog_out", "endpoint": "127.0.0.1:514", "structured_data": {"sd_id": "myapp@12345"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::SyslogOut { structured_data, .. } => {
                assert_eq!(
                    structured_data,
                    Some(SyslogStructuredData { sd_id: "myapp@12345".to_string() })
                );
            }
            other => panic!("expected SyslogOut, got {other:?}"),
        }
    }

    /// `tls:` is optional: absent means plaintext.
    #[test]
    fn syslog_out_without_tls_defaults_to_none() {
        let component: Component =
            serde_json::from_str(r#"{"type": "syslog_out", "endpoint": "127.0.0.1:514"}"#).unwrap();
        match component.kind {
            ComponentKind::SyslogOut { tls, .. } => assert_eq!(tls, None),
            other => panic!("expected SyslogOut, got {other:?}"),
        }
    }

    /// Presence turns TLS on, so an empty `tls: {}` is meaningful: TLS with the bundled Mozilla
    /// roots.
    #[test]
    fn syslog_out_tls_parses_and_an_empty_block_is_distinct_from_absent() {
        let component: Component = serde_json::from_str(
            r#"{"type": "syslog_out", "endpoint": "relay:6514", "transport": "tcp", "tls": {}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::SyslogOut { tls, .. } => {
                assert_eq!(tls, Some(TlsClientConfig::default()));
                assert!(tls.unwrap().is_empty(), "an empty block still means TLS is on");
            }
            other => panic!("expected SyslogOut, got {other:?}"),
        }

        let component: Component = serde_json::from_str(
            r#"{"type": "syslog_out", "endpoint": "relay:6514", "transport": "tcp",
                "tls": {"ca_file": "ca.pem", "cert_file": "client.pem", "key_file": "client.key"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::SyslogOut { tls, .. } => {
                let tls = tls.expect("a set tls: block parses");
                assert_eq!(tls.ca_file.as_deref(), Some("ca.pem"));
                assert_eq!(tls.cert_file.as_deref(), Some("client.pem"));
                assert_eq!(tls.key_file.as_deref(), Some("client.key"));
                assert!(!tls.insecure_skip_verify);
            }
            other => panic!("expected SyslogOut, got {other:?}"),
        }
    }

    /// `filter`/`rename`/`throttle`/`dedup` are retired kinds: a config referencing one is a
    /// deserialization error naming the valid kinds, not graph validation's "not implemented".
    #[test]
    fn a_retired_kind_is_a_deserialization_error_not_an_unimplemented_kind() {
        let err = serde_json::from_str::<Component>(r#"{"type": "filter", "sources": ["in"]}"#)
            .unwrap_err();
        assert!(err.to_string().contains("unknown variant"), "got: {err}");
    }

    #[test]
    fn kv_metrics_component_round_trips_through_deserialization() {
        let component: Component = serde_json::from_str(
            r#"{"type": "kv_metrics", "sources": ["in"],
                "counters": [{"name": "nginx.requests"},
                             {"name": "nginx.bytes_sent", "field": "body_bytes_sent"}],
                "distributions": [{"name": "nginx.request_time", "field": "request_time",
                                    "unit": "s"}]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::KvMetrics { counters, gauges, distributions } => {
                assert_eq!(counters.len(), 2);
                assert_eq!(counters[0].name, "nginx.requests");
                assert_eq!(counters[0].field, None);
                assert_eq!(counters[1].field, Some("body_bytes_sent".to_string()));
                assert!(gauges.is_empty());
                assert_eq!(distributions.len(), 1);
                assert_eq!(distributions[0].unit, Some("s".to_string()));
            }
            other => panic!("expected KvMetrics, got {other:?}"),
        }
    }

    #[test]
    fn kv_metrics_component_defaults_every_list_to_empty() {
        let component: Component =
            serde_json::from_str(r#"{"type": "kv_metrics", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::KvMetrics { counters, gauges, distributions } => {
                assert!(counters.is_empty());
                assert!(gauges.is_empty());
                assert!(distributions.is_empty());
            }
            other => panic!("expected KvMetrics, got {other:?}"),
        }
    }

    #[test]
    fn keep_component_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "keep", "sources": ["in"], "fields": ["status", "method"]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Keep { fields } => {
                assert_eq!(fields, vec!["status".to_string(), "method".to_string()]);
            }
            other => panic!("expected Keep, got {other:?}"),
        }
    }

    #[test]
    fn remove_component_deserializes_with_multiple_fields() {
        let component: Component = serde_json::from_str(
            r#"{"type": "remove", "sources": ["in"], "fields": ["client_ip", "user_agent"]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Remove { fields } => {
                assert_eq!(fields, vec!["client_ip".to_string(), "user_agent".to_string()]);
            }
            other => panic!("expected Remove, got {other:?}"),
        }
    }

    #[test]
    fn set_component_deserializes_resource_and_attributes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "set", "sources": ["in"],
                "resource": {"service.name": "nginx"},
                "attributes": {"env": "prod", "tier": 3}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Set { resource, attributes } => {
                assert_eq!(resource.get("service.name"), Some(&SetValue::Str("nginx".to_string())));
                assert_eq!(attributes.get("env"), Some(&SetValue::Str("prod".to_string())));
                assert_eq!(attributes.get("tier"), Some(&SetValue::I64(3)));
            }
            other => panic!("expected Set, got {other:?}"),
        }
    }

    /// Pins `SetValue`'s untagged-variant order: a whole-number scalar decodes as `I64`, not
    /// `F64`, and a quoted number stays `Str`.
    #[test]
    fn set_value_untagged_variant_selection() {
        let component: Component = serde_json::from_str(
            r#"{"type": "set", "sources": ["in"], "attributes":
                {"s": "nginx", "i": 3, "f": 1.5, "b": true, "q": "3"}}"#,
        )
        .unwrap();
        let ComponentKind::Set { attributes, .. } = component.kind else {
            panic!("expected Set");
        };
        assert_eq!(attributes.get("s"), Some(&SetValue::Str("nginx".to_string())));
        assert_eq!(attributes.get("i"), Some(&SetValue::I64(3)), "a whole number must stay I64");
        assert_eq!(attributes.get("f"), Some(&SetValue::F64(1.5)));
        assert_eq!(attributes.get("b"), Some(&SetValue::Bool(true)));
        assert_eq!(
            attributes.get("q"),
            Some(&SetValue::Str("3".to_string())),
            "a quoted number must stay a string, not be coerced into I64"
        );
    }

    #[test]
    fn trace_context_component_deserializes_with_the_convention_defaults() {
        let component: Component =
            serde_json::from_str(r#"{"type": "trace_context", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::TraceContext { trace_id, span_id, flags, keep_source, span } => {
                assert_eq!(trace_id, "trace.id");
                assert_eq!(span_id, Some("span.id".to_string()));
                assert_eq!(flags, Some("trace.flags".to_string()));
                assert!(!keep_source);
                assert_eq!(span, None, "span lifting is opt-in");
            }
            other => panic!("expected TraceContext, got {other:?}"),
        }
    }

    #[test]
    fn trace_context_component_null_disables_an_optional_lookup() {
        let component: Component = serde_json::from_str(
            r#"{"type": "trace_context", "sources": ["in"], "span_id": null, "flags": null}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::TraceContext { span_id, flags, .. } => {
                assert_eq!(span_id, None);
                assert_eq!(flags, None);
            }
            other => panic!("expected TraceContext, got {other:?}"),
        }
    }

    #[test]
    fn trace_context_component_deserializes_every_field() {
        let component: Component = serde_json::from_str(
            r#"{"type": "trace_context", "sources": ["in"], "trace_id": "trace_id",
                "span_id": "span_id", "flags": "trace_flags", "keep_source": true,
                "span": {"mint_id": true, "name": "proxy", "kind": "client", "max_skew": "30s"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::TraceContext { trace_id, span_id, flags, keep_source, span } => {
                assert_eq!(trace_id, "trace_id");
                assert_eq!(span_id, Some("span_id".to_string()));
                assert_eq!(flags, Some("trace_flags".to_string()));
                assert!(keep_source);
                assert_eq!(
                    span,
                    Some(SpanLiftConfig {
                        mint_id: true,
                        name: "proxy".to_string(),
                        kind: SpanKindConfig::Client,
                        max_skew: Duration::from_secs(30),
                    })
                );
            }
            other => panic!("expected TraceContext, got {other:?}"),
        }
    }

    #[test]
    fn trace_context_empty_span_block_takes_every_default() {
        let component: Component =
            serde_json::from_str(r#"{"type": "trace_context", "sources": ["in"], "span": {}}"#)
                .unwrap();
        let ComponentKind::TraceContext { span, .. } = component.kind else {
            panic!("expected TraceContext");
        };
        let span = span.expect("span block present");
        assert_eq!(span, SpanLiftConfig::default());
        assert!(!span.mint_id);
        assert_eq!(span.name, "http.request");
        assert_eq!(span.kind, SpanKindConfig::Server);
        assert_eq!(span.max_skew, Duration::from_secs(3600));
    }

    #[test]
    fn trace_context_span_block_rejects_unknown_fields() {
        let err = serde_json::from_str::<Component>(
            r#"{"type": "trace_context", "sources": ["in"], "span": {"nme": "x"}}"#,
        )
        .unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    #[test]
    fn scale_component_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "scale", "sources": ["in"], "fields": {"request_time": 1000.0}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Scale { fields } => {
                assert_eq!(fields.get("request_time"), Some(&1000.0));
            }
            other => panic!("expected Scale, got {other:?}"),
        }
    }

    #[test]
    fn has_signal_component_deserializes_with_mode_defaulting_to_any_of() {
        let component: Component = serde_json::from_str(
            r#"{"type": "has_signal", "sources": ["in"], "signals": ["traces"]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::HasSignal { signals, mode } => {
                assert_eq!(signals, vec![Signal::Traces]);
                assert_eq!(mode, MatchMode::AnyOf);
            }
            other => panic!("expected HasSignal, got {other:?}"),
        }
    }

    #[test]
    fn has_signal_component_can_override_mode_to_only() {
        let component: Component = serde_json::from_str(
            r#"{"type": "has_signal", "sources": ["in"], "signals": ["traces"], "mode": "only"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::HasSignal { mode, .. } => assert_eq!(mode, MatchMode::Only),
            other => panic!("expected HasSignal, got {other:?}"),
        }
    }

    #[test]
    fn keep_signals_component_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "keep_signals", "sources": ["in"], "signals": ["logs"]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::KeepSignals { signals } => assert_eq!(signals, vec![Signal::Logs]),
            other => panic!("expected KeepSignals, got {other:?}"),
        }
    }

    #[test]
    fn drop_signals_component_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "drop_signals", "sources": ["in"], "signals": ["metrics"]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::DropSignals { signals } => assert_eq!(signals, vec![Signal::Metrics]),
            other => panic!("expected DropSignals, got {other:?}"),
        }
    }

    #[test]
    fn has_attributes_component_deserializes_with_both_maps() {
        let component: Component = serde_json::from_str(
            r#"{"type": "has_attributes", "sources": ["in"],
                "resource": {"service.name": "nginx"}, "attributes": {"status": 200}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::HasAttributes { resource, attributes } => {
                assert_eq!(resource.get("service.name"), Some(&SetValue::Str("nginx".to_string())));
                assert_eq!(attributes.get("status"), Some(&SetValue::I64(200)));
            }
            other => panic!("expected HasAttributes, got {other:?}"),
        }
    }

    #[test]
    fn drop_attributes_component_deserializes_with_only_attributes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "drop_attributes", "sources": ["in"], "attributes": {"stream": "debug"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::DropAttributes { resource, attributes } => {
                assert!(resource.is_empty(), "resource must default to empty");
                assert_eq!(attributes.get("stream"), Some(&SetValue::Str("debug".to_string())));
            }
            other => panic!("expected DropAttributes, got {other:?}"),
        }
    }

    #[test]
    fn keep_values_component_deserializes_full_form() {
        let component: Component = serde_json::from_str(
            r#"{"type": "keep_values", "sources": ["in"],
                "attributes": {"host": {"normalize": ["lower"],
                                         "allow": ["static.local", "proxy.local"],
                                         "other": "other"}}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::KeepValues { resource, attributes } => {
                assert!(resource.is_empty(), "resource must default to empty");
                let host = attributes.get("host").expect("host field configured");
                assert_eq!(host.normalize, vec![NormalizeStep::Lower]);
                assert_eq!(
                    host.allow,
                    vec![
                        SetValue::Str("static.local".to_string()),
                        SetValue::Str("proxy.local".to_string())
                    ]
                );
                assert_eq!(host.other, Some(SetValue::Str("other".to_string())));
            }
            other => panic!("expected KeepValues, got {other:?}"),
        }
    }

    #[test]
    fn keep_values_normalize_and_other_default_to_empty_and_none() {
        let component: Component = serde_json::from_str(
            r#"{"type": "keep_values", "sources": ["in"],
                "attributes": {"status": {"allow": [200]}}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::KeepValues { attributes, .. } => {
                let status = attributes.get("status").expect("status field configured");
                assert!(status.normalize.is_empty(), "normalize must default to empty");
                assert_eq!(status.allow, vec![SetValue::I64(200)], "a whole number must stay I64");
                assert!(status.other.is_none(), "other must default to None");
            }
            other => panic!("expected KeepValues, got {other:?}"),
        }
    }

    #[test]
    fn keep_values_resource_field_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "keep_values", "sources": ["in"],
                "resource": {"env": {"allow": ["prod", "staging"]}}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::KeepValues { resource, attributes } => {
                assert!(attributes.is_empty(), "attributes must default to empty");
                assert!(resource.contains_key("env"));
            }
            other => panic!("expected KeepValues, got {other:?}"),
        }
    }

    #[test]
    fn shape_component_deserializes_with_every_field_defaulted() {
        let component: Component =
            serde_json::from_str(r#"{"type": "shape", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::Shape { interval, resource, max_tracked_keys, max_tracked_keysets } => {
                assert_eq!(interval, Duration::from_secs(10));
                assert_eq!(resource, ShapeResource::Drop, "counts-only is the default");
                assert_eq!(max_tracked_keys, 4096);
                assert_eq!(max_tracked_keysets, 4096);
            }
            other => panic!("expected Shape, got {other:?}"),
        }
    }

    #[test]
    fn shape_component_deserializes_full_form() {
        let component: Component = serde_json::from_str(
            r#"{"type": "shape", "sources": ["in"], "interval": "30s", "resource": "keep",
                "max_tracked_keys": 128, "max_tracked_keysets": 64}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Shape { interval, resource, max_tracked_keys, max_tracked_keysets } => {
                assert_eq!(interval, Duration::from_secs(30));
                assert_eq!(resource, ShapeResource::Keep);
                assert_eq!(max_tracked_keys, 128);
                assert_eq!(max_tracked_keysets, 64);
            }
            other => panic!("expected Shape, got {other:?}"),
        }
    }

    #[test]
    fn shape_resource_uses_snake_case() {
        assert_eq!(
            serde_json::from_str::<ShapeResource>(r#""drop""#).unwrap(),
            ShapeResource::Drop
        );
        assert_eq!(
            serde_json::from_str::<ShapeResource>(r#""keep""#).unwrap(),
            ShapeResource::Keep
        );
        assert!(serde_json::from_str::<ShapeResource>(r#""Keep""#).is_err());
    }

    #[test]
    fn normalize_step_uses_snake_case() {
        let step: NormalizeStep = serde_json::from_str(r#""lower""#).unwrap();
        assert_eq!(step, NormalizeStep::Lower);
        assert_eq!(serde_json::to_string(&NormalizeStep::Lower).unwrap(), r#""lower""#);
    }

    #[test]
    fn flatten_component_deserializes_with_every_field_defaulted() {
        let component: Component =
            serde_json::from_str(r#"{"type": "flatten", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::Flatten { attributes, resource, arrays } => {
                assert_eq!(attributes, FlattenFields::Keyword(FlattenKeyword::All));
                assert_eq!(resource, FlattenFields::Keyword(FlattenKeyword::None));
                assert_eq!(arrays, FlattenArrays::Index);
            }
            other => panic!("expected Flatten, got {other:?}"),
        }
    }

    #[test]
    fn flatten_component_deserializes_full_form() {
        let component: Component = serde_json::from_str(
            r#"{"type": "flatten", "sources": ["in"], "attributes": ["http", "k8s.labels"],
                "resource": "all", "arrays": "skip"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Flatten { attributes, resource, arrays } => {
                assert_eq!(
                    attributes,
                    FlattenFields::Named(vec!["http".to_string(), "k8s.labels".to_string()])
                );
                assert_eq!(resource, FlattenFields::Keyword(FlattenKeyword::All));
                assert_eq!(arrays, FlattenArrays::Skip);
            }
            other => panic!("expected Flatten, got {other:?}"),
        }
    }

    #[test]
    fn sample_component_deserializes_with_only_a_rate() {
        let component: Component =
            serde_json::from_str(r#"{"type": "sample", "sources": ["in"], "rate": 0.25}"#).unwrap();
        match component.kind {
            ComponentKind::Sample { rate, key, missing, always_keep } => {
                assert_eq!(rate, 0.25);
                assert_eq!(key, None);
                assert_eq!(missing, None);
                assert_eq!(always_keep, None);
            }
            other => panic!("expected Sample, got {other:?}"),
        }
    }

    #[test]
    fn sample_key_reads_each_shape() {
        let key: SampleKey = serde_json::from_str(r#""trace_id""#).unwrap();
        assert_eq!(key, SampleKey::TraceId);
        let key: SampleKey = serde_json::from_str(r#"{"attribute": "request_id"}"#).unwrap();
        assert_eq!(key, SampleKey::Attribute("request_id".to_string()));
        let key: SampleKey = serde_json::from_str(r#"{"resource": "service.name"}"#).unwrap();
        assert_eq!(key, SampleKey::Resource("service.name".to_string()));
        assert_eq!(serde_json::to_string(&SampleKey::TraceId).unwrap(), r#""trace_id""#);
        assert!(serde_json::from_str::<SampleKey>(r#""span_id""#).is_err());
    }

    #[test]
    fn sample_missing_reads_each_mode() {
        for (text, mode) in [
            (r#""random""#, SampleMissing::Random),
            (r#""keep""#, SampleMissing::Keep),
            (r#""drop""#, SampleMissing::Drop),
        ] {
            assert_eq!(serde_json::from_str::<SampleMissing>(text).unwrap(), mode);
        }
        assert_eq!(SampleMissing::default(), SampleMissing::Random);
    }

    #[test]
    fn sample_component_deserializes_full_form() {
        let component: Component = serde_json::from_str(
            r#"{"type": "sample", "sources": ["in"], "rate": 0.1, "key": "trace_id",
                "missing": "drop",
                "always_keep": {"attribute": "sampling.keep", "value": true}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Sample { rate, key, missing, always_keep } => {
                assert_eq!(rate, 0.1);
                assert_eq!(key, Some(SampleKey::TraceId));
                assert_eq!(missing, Some(SampleMissing::Drop));
                let always_keep = always_keep.unwrap();
                assert_eq!(always_keep.attribute.as_deref(), Some("sampling.keep"));
                assert_eq!(always_keep.resource, None);
                // A YAML/JSON `true` stays a `Bool`, not a string.
                assert_eq!(always_keep.value, Some(SetValue::Bool(true)));
            }
            other => panic!("expected Sample, got {other:?}"),
        }
    }

    #[test]
    fn sample_override_without_a_value_matches_any_value() {
        let o: SampleOverride = serde_json::from_str(r#"{"resource": "debug"}"#).unwrap();
        assert_eq!(o.resource.as_deref(), Some("debug"));
        assert_eq!(o.attribute, None);
        assert_eq!(o.value, None);
    }

    #[test]
    fn sample_override_rejects_an_unknown_field() {
        let err = serde_json::from_str::<SampleOverride>(r#"{"attribute": "a", "equals": 1}"#)
            .unwrap_err();
        assert!(err.to_string().contains("unknown field"), "got: {err}");
    }

    #[test]
    fn http_access_component_deserializes_with_every_field_defaulted() {
        let component: Component =
            serde_json::from_str(r#"{"type": "http_access", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::HttpAccess {
                routes,
                route_other,
                user_agent_rules,
                max_length,
                redact_query,
                forwarded,
            } => {
                assert!(routes.is_empty());
                assert_eq!(route_other, None);
                assert!(user_agent_rules.is_empty());
                assert!(max_length.is_empty());
                assert!(redact_query.is_empty());
                assert_eq!(forwarded, None);
            }
            other => panic!("expected HttpAccess, got {other:?}"),
        }
    }

    #[test]
    fn http_access_component_deserializes_full_form() {
        let component: Component = serde_json::from_str(
            r#"{
                "type": "http_access",
                "sources": ["in"],
                "routes": [
                    {"builtin": "probes"},
                    {"builtin": "well_known"},
                    {"match": "^/api/v1/users/\\d+$", "route": "/api/v1/users/{id}"}
                ],
                "route_other": "/{other}",
                "user_agent_rules": [{"match": "MyMonitor/", "class": "tool"}],
                "max_length": {"url.path": 512},
                "redact_query": ["token"],
                "forwarded": {}
            }"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::HttpAccess {
                routes,
                route_other,
                user_agent_rules,
                max_length,
                redact_query,
                forwarded,
            } => {
                assert_eq!(
                    routes,
                    vec![
                        HttpRouteRule {
                            builtin: Some(HttpRouteSet::Probes),
                            ..HttpRouteRule::default()
                        },
                        HttpRouteRule {
                            builtin: Some(HttpRouteSet::WellKnown),
                            ..HttpRouteRule::default()
                        },
                        HttpRouteRule {
                            builtin: None,
                            pattern: Some(r"^/api/v1/users/\d+$".to_string()),
                            route: Some("/api/v1/users/{id}".to_string()),
                        },
                    ]
                );
                assert_eq!(route_other.as_deref(), Some("/{other}"));
                assert_eq!(
                    user_agent_rules,
                    vec![UserAgentRule {
                        pattern: "MyMonitor/".to_string(),
                        class: "tool".to_string()
                    }]
                );
                assert_eq!(max_length.get("url.path"), Some(&512));
                assert_eq!(redact_query, vec!["token".to_string()]);
                assert_eq!(
                    forwarded,
                    Some(ForwardedConfig { trust: true }),
                    "an empty forwarded block defaults trust to true"
                );
            }
            other => panic!("expected HttpAccess, got {other:?}"),
        }
    }

    /// Both halves of a route rule parse into the one flat struct; deciding which shape it is
    /// (and rejecting a mix) is graph validation's job, so the error can name the key.
    #[test]
    fn http_route_rule_accepts_a_mixed_shape_for_rule_60_to_reject() {
        let rule: HttpRouteRule =
            serde_json::from_str(r#"{"builtin": "assets", "match": "x"}"#).unwrap();
        assert_eq!(rule.builtin, Some(HttpRouteSet::Assets));
        assert_eq!(rule.pattern.as_deref(), Some("x"));
        assert_eq!(rule.route, None);
    }

    #[test]
    fn http_route_rule_rejects_an_unknown_key() {
        let err = serde_json::from_str::<HttpRouteRule>(r#"{"matches": "x"}"#).unwrap_err();
        assert!(err.to_string().contains("matches"), "{err}");
    }

    #[test]
    fn http_route_set_uses_snake_case() {
        assert_eq!(
            serde_json::from_str::<HttpRouteSet>(r#""well_known""#).unwrap(),
            HttpRouteSet::WellKnown
        );
        assert_eq!(serde_json::to_string(&HttpRouteSet::Assets).unwrap(), r#""assets""#);
        assert!(serde_json::from_str::<HttpRouteSet>(r#""WellKnown""#).is_err());
    }

    #[test]
    fn user_agent_rule_requires_both_fields_and_rejects_unknown_keys() {
        assert!(serde_json::from_str::<UserAgentRule>(r#"{"match": "x"}"#).is_err());
        assert!(serde_json::from_str::<UserAgentRule>(r#"{"match": "x", "class": "c", "k": 1}"#)
            .is_err());
    }

    #[test]
    fn http_access_round_trips_through_serde() {
        let kind = ComponentKind::HttpAccess {
            routes: vec![HttpRouteRule {
                builtin: Some(HttpRouteSet::Assets),
                ..HttpRouteRule::default()
            }],
            route_other: Some("/{other}".to_string()),
            user_agent_rules: vec![],
            max_length: std::collections::BTreeMap::from([("user.name".to_string(), 16)]),
            redact_query: vec![],
            forwarded: Some(ForwardedConfig { trust: true }),
        };
        let json = serde_json::to_string(&kind).unwrap();
        let back: ComponentKind = serde_json::from_str(&json).unwrap();
        assert_eq!(serde_json::to_string(&back).unwrap(), json);
    }

    #[test]
    fn capped_fields_are_unique_and_nonzero() {
        let mut seen = std::collections::HashSet::new();
        for (field, cap) in CAPPED_FIELDS {
            assert!(seen.insert(*field), "{field} listed twice");
            assert!(*cap > 0, "{field} has a zero default cap");
        }
    }

    #[test]
    fn flatten_fields_keyword_none_deserializes() {
        let fields: FlattenFields = serde_json::from_str(r#""none""#).unwrap();
        assert_eq!(fields, FlattenFields::Keyword(FlattenKeyword::None));
    }

    #[test]
    fn flatten_fields_rejects_an_unknown_keyword() {
        assert!(serde_json::from_str::<FlattenFields>(r#""everything""#).is_err());
    }

    #[test]
    fn flatten_arrays_uses_snake_case() {
        assert_eq!(
            serde_json::from_str::<FlattenArrays>(r#""index""#).unwrap(),
            FlattenArrays::Index
        );
        assert_eq!(
            serde_json::from_str::<FlattenArrays>(r#""skip""#).unwrap(),
            FlattenArrays::Skip
        );
        assert!(serde_json::from_str::<FlattenArrays>(r#""Skip""#).is_err());
    }

    #[test]
    fn has_attributes_a_whole_number_value_stays_i64() {
        let component: Component = serde_json::from_str(
            r#"{"type": "has_attributes", "sources": ["in"], "attributes": {"status": 200}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::HasAttributes { attributes, .. } => {
                assert_eq!(attributes.get("status"), Some(&SetValue::I64(200)), "not F64");
            }
            other => panic!("expected HasAttributes, got {other:?}"),
        }
    }

    /// `types_db` is optional; a `collectd_in` with only a `bind` gets index-named records.
    #[test]
    fn collectd_in_component_defaults_types_db_to_empty() {
        let component: Component =
            serde_json::from_str(r#"{"type": "collectd_in", "bind": "0.0.0.0:25826"}"#).unwrap();
        match component.kind {
            ComponentKind::CollectdIn { bind, types_db } => {
                assert_eq!(bind, "0.0.0.0:25826");
                assert!(types_db.is_empty(), "types_db defaults to no files at all");
            }
            other => panic!("expected CollectdIn, got {other:?}"),
        }
    }

    #[test]
    fn collectd_in_component_parses_a_types_db_list_in_order() {
        let component: Component = serde_json::from_str(
            r#"{"type": "collectd_in", "bind": "239.192.74.66:25826",
                "types_db": ["/usr/share/collectd/types.db", "local-types.db"]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::CollectdIn { bind, types_db } => {
                assert_eq!(bind, "239.192.74.66:25826", "a multicast group is an ordinary bind");
                assert_eq!(
                    types_db,
                    vec![
                        PathBuf::from("/usr/share/collectd/types.db"),
                        PathBuf::from("local-types.db"),
                    ],
                    "order matters: a later file overrides an earlier one"
                );
            }
            other => panic!("expected CollectdIn, got {other:?}"),
        }
    }

    /// Every `graphite_in` field but `bind` is optional, and the defaults are carbon's own: TCP
    /// plaintext, an 8 KiB line bound, a 1 MiB frame bound, no TLS, and the shared 5s
    /// `handshake_timeout`.
    #[test]
    fn graphite_in_component_defaults_to_tcp_plaintext_with_carbons_bounds() {
        let component: Component =
            serde_json::from_str(r#"{"type": "graphite_in", "bind": "0.0.0.0:2003"}"#).unwrap();
        match component.kind {
            ComponentKind::GraphiteIn {
                bind,
                transport,
                protocol,
                tls,
                handshake_timeout,
                idle_timeout,
                max_line_bytes,
                max_frame_bytes,
            } => {
                assert_eq!(bind, "0.0.0.0:2003");
                assert_eq!(transport, GraphiteTransport::Tcp);
                assert_eq!(protocol, GraphiteProtocol::Plaintext);
                assert_eq!(tls, None, "plaintext unless a tls: block says otherwise");
                assert_eq!(handshake_timeout, Duration::from_secs(5));
                assert_eq!(idle_timeout, None, "opt-in -- no idle timeout unless asked for");
                assert_eq!(max_line_bytes, 8192);
                assert_eq!(max_frame_bytes, 1 << 20);
            }
            other => panic!("expected GraphiteIn, got {other:?}"),
        }
    }

    /// `tls:` and `handshake_timeout:` round-trip on a `graphite_in`; graph validation rejects
    /// both on `transport: udp`.
    #[test]
    fn graphite_in_component_parses_tls_and_handshake_timeout() {
        let component: Component = serde_json::from_str(
            r#"{"type": "graphite_in", "bind": "0.0.0.0:2003",
                "handshake_timeout": "2s",
                "tls": {"cert_file": "server.pem", "key_file": "server.key",
                        "client_ca_file": "ca.pem"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::GraphiteIn { tls: Some(tls), handshake_timeout, .. } => {
                assert_eq!(tls.cert_file, "server.pem");
                assert_eq!(tls.key_file, "server.key");
                assert_eq!(tls.client_ca_file, Some("ca.pem".to_string()));
                assert_eq!(handshake_timeout, Duration::from_secs(2));
            }
            other => panic!("expected GraphiteIn with tls set, got {other:?}"),
        }
    }

    /// Both enums are `snake_case` on the wire; pickle over TCP is the only legal pickle
    /// combination.
    #[test]
    fn graphite_in_component_parses_snake_case_transport_and_protocol() {
        let component: Component = serde_json::from_str(
            r#"{"type": "graphite_in", "bind": "0.0.0.0:2004",
                "transport": "tcp", "protocol": "pickle"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::GraphiteIn { transport, protocol, .. } => {
                assert_eq!(transport, GraphiteTransport::Tcp);
                assert_eq!(protocol, GraphiteProtocol::Pickle);
            }
            other => panic!("expected GraphiteIn, got {other:?}"),
        }

        let udp: Component = serde_json::from_str(
            r#"{"type": "graphite_in", "bind": "0.0.0.0:2003", "transport": "udp"}"#,
        )
        .unwrap();
        match udp.kind {
            ComponentKind::GraphiteIn { transport, protocol, .. } => {
                assert_eq!(transport, GraphiteTransport::Udp);
                assert_eq!(protocol, GraphiteProtocol::Plaintext);
            }
            other => panic!("expected GraphiteIn, got {other:?}"),
        }
    }

    /// Both byte bounds go through `human_bytes`, so `"16KiB"` and a bare `"16384"` are the same
    /// setting.
    #[test]
    fn graphite_in_component_parses_human_byte_sizes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "graphite_in", "bind": "0.0.0.0:2003",
                "max_line_bytes": "16KiB", "max_frame_bytes": "2MiB"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::GraphiteIn { max_line_bytes, max_frame_bytes, .. } => {
                assert_eq!(max_line_bytes, 16 * 1024);
                assert_eq!(max_frame_bytes, 2 * 1024 * 1024);
            }
            other => panic!("expected GraphiteIn, got {other:?}"),
        }

        let plain: Component = serde_json::from_str(
            r#"{"type": "graphite_in", "bind": "0.0.0.0:2003", "max_line_bytes": "16384"}"#,
        )
        .unwrap();
        match plain.kind {
            ComponentKind::GraphiteIn { max_line_bytes, .. } => {
                assert_eq!(max_line_bytes, 16 * 1024, "a bare digit string is bytes");
            }
            other => panic!("expected GraphiteIn, got {other:?}"),
        }
    }

    #[test]
    fn component_with_no_sources_defaults_to_empty() {
        let component: Component =
            serde_json::from_str(r#"{"type": "statsd_in", "bind": "0.0.0.0:8125"}"#).unwrap();
        assert!(component.sources.is_empty());
        assert!(matches!(component.kind, ComponentKind::StatsdIn { .. }));
    }

    #[test]
    fn sink_component_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["enrich"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN"}"#,
        )
        .unwrap();
        assert_eq!(component.sources, vec!["enrich".to_string()]);
        assert!(matches!(component.kind, ComponentKind::InfluxDbOut { .. }));
    }

    #[test]
    fn otlp_out_without_protocol_defaults_to_http() {
        let component: Component = serde_json::from_str(
            r#"{"type": "otlp_out", "sources": ["in"], "endpoint": "http://tempo:4318"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::OtlpOut { endpoint, protocol, headers, paths, compression, tls } => {
                assert_eq!(endpoint, "http://tempo:4318");
                assert_eq!(protocol, OtlpProtocol::Http);
                assert!(headers.is_empty());
                assert!(paths.is_empty());
                assert_eq!(compression, OtlpCompression::None);
                assert_eq!(tls, TlsClientConfig::default());
            }
            other => panic!("expected OtlpOut, got {other:?}"),
        }
    }

    #[test]
    fn otlp_out_compression_defaults_to_none_and_can_be_set_to_gzip() {
        let component: Component = serde_json::from_str(
            r#"{"type": "otlp_out", "sources": ["in"], "endpoint": "http://loki:3100",
                "compression": "gzip"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::OtlpOut { compression, .. } => {
                assert_eq!(compression, OtlpCompression::Gzip);
            }
            other => panic!("expected OtlpOut, got {other:?}"),
        }
    }

    #[test]
    fn otlp_out_headers_default_to_empty_and_can_be_set() {
        let component: Component = serde_json::from_str(
            r#"{"type": "otlp_out", "sources": ["in"], "endpoint": "http://tempo:4318",
                "headers": {"X-Scope-OrgID": "tenant-a"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::OtlpOut { headers, .. } => {
                assert_eq!(headers.get("X-Scope-OrgID"), Some(&"tenant-a".to_string()));
            }
            other => panic!("expected OtlpOut, got {other:?}"),
        }
    }

    #[test]
    fn otlp_out_paths_default_to_empty_and_can_be_set() {
        let component: Component = serde_json::from_str(
            r#"{"type": "otlp_out", "sources": ["in"], "endpoint": "http://loki:3100",
                "paths": {"logs": "/otlp/v1/logs"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::OtlpOut { paths, .. } => {
                assert_eq!(paths.logs, Some("/otlp/v1/logs".to_string()));
                assert_eq!(paths.metrics, None);
                assert_eq!(paths.traces, None);
            }
            other => panic!("expected OtlpOut, got {other:?}"),
        }
    }

    /// The bare shape (UDP, no TLS) keeps deserializing with the optional fields behind
    /// `#[serde(default)]`.
    #[test]
    fn syslog_in_defaults_to_udp_with_no_tls() {
        let component: Component =
            serde_json::from_str(r#"{"type": "syslog_in", "bind": "0.0.0.0:5514"}"#).unwrap();
        match component.kind {
            ComponentKind::SyslogIn { bind, transport, tls, handshake_timeout, idle_timeout } => {
                assert_eq!(bind, "0.0.0.0:5514");
                assert_eq!(transport, SyslogTransport::Udp);
                assert_eq!(tls, None);
                assert_eq!(handshake_timeout, Duration::from_secs(5));
                assert_eq!(idle_timeout, None, "opt-in -- no idle timeout unless asked for");
            }
            other => panic!("expected SyslogIn, got {other:?}"),
        }
    }

    #[test]
    fn syslog_in_with_transport_tcp_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "syslog_in", "bind": "0.0.0.0:5514", "transport": "tcp"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::SyslogIn { transport, tls, .. } => {
                assert_eq!(transport, SyslogTransport::Tcp);
                assert_eq!(tls, None, "transport alone must not imply TLS");
            }
            other => panic!("expected SyslogIn, got {other:?}"),
        }
    }

    #[test]
    fn syslog_in_with_tls_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "syslog_in", "bind": "0.0.0.0:6514", "transport": "tcp",
                "tls": {"cert_file": "server.pem", "key_file": "server.key",
                        "client_ca_file": "ca.pem"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::SyslogIn { transport, tls: Some(tls), .. } => {
                assert_eq!(transport, SyslogTransport::Tcp);
                assert_eq!(tls.cert_file, "server.pem");
                assert_eq!(tls.key_file, "server.key");
                assert_eq!(tls.client_ca_file, Some("ca.pem".to_string()));
            }
            other => panic!("expected SyslogIn with tls set, got {other:?}"),
        }
    }

    #[test]
    fn otlp_in_with_protocol_grpc_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "otlp_in", "bind": "0.0.0.0:4317", "protocol": "grpc"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::OtlpIn { bind, protocol, tls, handshake_timeout, idle_timeout } => {
                assert_eq!(bind, "0.0.0.0:4317");
                assert_eq!(protocol, OtlpProtocol::Grpc);
                assert_eq!(tls, None);
                assert_eq!(handshake_timeout, Duration::from_secs(5));
                assert_eq!(idle_timeout, None, "opt-in -- no idle timeout unless asked for");
            }
            other => panic!("expected OtlpIn, got {other:?}"),
        }
    }

    #[test]
    fn otlp_out_tls_defaults_to_empty_and_can_be_set() {
        let component: Component = serde_json::from_str(
            r#"{"type": "otlp_out", "sources": ["in"], "endpoint": "https://tempo:4317",
                "protocol": "grpc",
                "tls": {"ca_file": "ca.pem", "cert_file": "client.pem", "key_file": "client.key"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::OtlpOut { tls, .. } => {
                assert_eq!(tls.ca_file, Some("ca.pem".to_string()));
                assert_eq!(tls.cert_file, Some("client.pem".to_string()));
                assert_eq!(tls.key_file, Some("client.key".to_string()));
                assert!(!tls.insecure_skip_verify);
            }
            other => panic!("expected OtlpOut, got {other:?}"),
        }
    }

    #[test]
    fn otlp_in_tls_defaults_to_none_and_can_be_set() {
        let component: Component = serde_json::from_str(
            r#"{"type": "otlp_in", "bind": "0.0.0.0:4317", "protocol": "grpc",
                "tls": {"cert_file": "server.pem", "key_file": "server.key"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::OtlpIn { tls: Some(tls), .. } => {
                assert_eq!(tls.cert_file, "server.pem");
                assert_eq!(tls.key_file, "server.key");
                assert_eq!(tls.client_ca_file, None);
            }
            other => panic!("expected OtlpIn with tls set, got {other:?}"),
        }
    }

    #[test]
    fn logit_in_defaults_tls_to_none_and_max_frame_bytes_to_none() {
        let component: Component =
            serde_json::from_str(r#"{"type": "logit_in", "bind": "0.0.0.0:5140"}"#).unwrap();
        match component.kind {
            ComponentKind::LogitIn {
                bind,
                tls,
                max_frame_bytes,
                handshake_timeout,
                idle_timeout,
            } => {
                assert_eq!(bind, "0.0.0.0:5140");
                assert_eq!(tls, None);
                assert_eq!(max_frame_bytes, None);
                assert_eq!(handshake_timeout, Duration::from_secs(5));
                assert_eq!(idle_timeout, None, "opt-in -- no idle timeout unless asked for");
            }
            other => panic!("expected LogitIn, got {other:?}"),
        }
    }

    #[test]
    fn logit_in_with_tls_and_max_frame_bytes_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "logit_in", "bind": "0.0.0.0:5140",
                "tls": {"cert_file": "server.pem", "key_file": "server.key"},
                "max_frame_bytes": "32MiB"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::LogitIn { tls: Some(tls), max_frame_bytes, .. } => {
                assert_eq!(tls.cert_file, "server.pem");
                assert_eq!(tls.key_file, "server.key");
                assert_eq!(max_frame_bytes, Some(32 * 1024 * 1024));
            }
            other => panic!("expected LogitIn with tls and max_frame_bytes set, got {other:?}"),
        }
    }

    /// `handshake_timeout` is one field on three listener kinds behind one shared default; a typo
    /// in any one attribute copy would otherwise show up only as a silently defaulted value.
    #[test]
    fn handshake_timeout_parses_on_all_three_tcp_listeners() {
        let syslog: Component = serde_json::from_str(
            r#"{"type": "syslog_in", "bind": "0.0.0.0:6514", "transport": "tcp",
                "handshake_timeout": "2s"}"#,
        )
        .unwrap();
        match syslog.kind {
            ComponentKind::SyslogIn { handshake_timeout, .. } => {
                assert_eq!(handshake_timeout, Duration::from_secs(2));
            }
            other => panic!("expected SyslogIn, got {other:?}"),
        }

        let logit: Component = serde_json::from_str(
            r#"{"type": "logit_in", "bind": "0.0.0.0:5140", "handshake_timeout": "500ms"}"#,
        )
        .unwrap();
        match logit.kind {
            ComponentKind::LogitIn { handshake_timeout, .. } => {
                assert_eq!(handshake_timeout, Duration::from_millis(500));
            }
            other => panic!("expected LogitIn, got {other:?}"),
        }

        let otlp: Component = serde_json::from_str(
            r#"{"type": "otlp_in", "bind": "0.0.0.0:4317", "handshake_timeout": "1m"}"#,
        )
        .unwrap();
        match otlp.kind {
            ComponentKind::OtlpIn { handshake_timeout, .. } => {
                assert_eq!(handshake_timeout, Duration::from_secs(60));
            }
            other => panic!("expected OtlpIn, got {other:?}"),
        }
    }

    /// `idle_timeout` is opt-in on every listener that has one: absent means no idle timeout. A
    /// `#[serde(default)]` typo on any one kind would silently change that, so all are checked.
    #[test]
    fn idle_timeout_defaults_to_none_on_every_tcp_listener() {
        let syslog: Component = serde_json::from_str(
            r#"{"type": "syslog_in", "bind": "0.0.0.0:6514", "transport": "tcp"}"#,
        )
        .unwrap();
        match syslog.kind {
            ComponentKind::SyslogIn { idle_timeout, .. } => assert_eq!(idle_timeout, None),
            other => panic!("expected SyslogIn, got {other:?}"),
        }

        let graphite: Component =
            serde_json::from_str(r#"{"type": "graphite_in", "bind": "0.0.0.0:2003"}"#).unwrap();
        match graphite.kind {
            ComponentKind::GraphiteIn { idle_timeout, .. } => assert_eq!(idle_timeout, None),
            other => panic!("expected GraphiteIn, got {other:?}"),
        }

        let statsd: Component = serde_json::from_str(
            r#"{"type": "statsd_in", "bind": "0.0.0.0:8125", "transport": "tcp"}"#,
        )
        .unwrap();
        match statsd.kind {
            ComponentKind::StatsdIn { idle_timeout, .. } => assert_eq!(idle_timeout, None),
            other => panic!("expected StatsdIn, got {other:?}"),
        }

        let logit: Component =
            serde_json::from_str(r#"{"type": "logit_in", "bind": "0.0.0.0:5140"}"#).unwrap();
        match logit.kind {
            ComponentKind::LogitIn { idle_timeout, .. } => assert_eq!(idle_timeout, None),
            other => panic!("expected LogitIn, got {other:?}"),
        }

        let otlp: Component =
            serde_json::from_str(r#"{"type": "otlp_in", "bind": "0.0.0.0:4318"}"#).unwrap();
        match otlp.kind {
            ComponentKind::OtlpIn { idle_timeout, .. } => assert_eq!(idle_timeout, None),
            other => panic!("expected OtlpIn, got {other:?}"),
        }
    }

    /// The twin of [`handshake_timeout_parses_on_all_three_tcp_listeners`] for the `Option`
    /// codec, which is a separate module: one humantime string per kind.
    #[test]
    fn idle_timeout_parses_on_every_tcp_listener() {
        let syslog: Component = serde_json::from_str(
            r#"{"type": "syslog_in", "bind": "0.0.0.0:6514", "transport": "tcp",
                "idle_timeout": "5m"}"#,
        )
        .unwrap();
        match syslog.kind {
            ComponentKind::SyslogIn { idle_timeout, .. } => {
                assert_eq!(idle_timeout, Some(Duration::from_secs(300)));
            }
            other => panic!("expected SyslogIn, got {other:?}"),
        }

        let graphite: Component = serde_json::from_str(
            r#"{"type": "graphite_in", "bind": "0.0.0.0:2003", "idle_timeout": "90s"}"#,
        )
        .unwrap();
        match graphite.kind {
            ComponentKind::GraphiteIn { idle_timeout, .. } => {
                assert_eq!(idle_timeout, Some(Duration::from_secs(90)));
            }
            other => panic!("expected GraphiteIn, got {other:?}"),
        }

        let statsd: Component = serde_json::from_str(
            r#"{"type": "statsd_in", "bind": "0.0.0.0:8125", "transport": "tcp",
                "idle_timeout": "500ms"}"#,
        )
        .unwrap();
        match statsd.kind {
            ComponentKind::StatsdIn { idle_timeout, .. } => {
                assert_eq!(idle_timeout, Some(Duration::from_millis(500)));
            }
            other => panic!("expected StatsdIn, got {other:?}"),
        }

        let logit: Component = serde_json::from_str(
            r#"{"type": "logit_in", "bind": "0.0.0.0:5140", "idle_timeout": "10m"}"#,
        )
        .unwrap();
        match logit.kind {
            ComponentKind::LogitIn { idle_timeout, .. } => {
                assert_eq!(idle_timeout, Some(Duration::from_secs(600)));
            }
            other => panic!("expected LogitIn, got {other:?}"),
        }

        let otlp: Component = serde_json::from_str(
            r#"{"type": "otlp_in", "bind": "0.0.0.0:4318", "idle_timeout": "10m"}"#,
        )
        .unwrap();
        match otlp.kind {
            ComponentKind::OtlpIn { idle_timeout, .. } => {
                assert_eq!(idle_timeout, Some(Duration::from_secs(600)));
            }
            other => panic!("expected OtlpIn, got {other:?}"),
        }
    }

    #[test]
    fn logit_out_defaults_compression_tls_and_request_timeout() {
        let component: Component = serde_json::from_str(
            r#"{"type": "logit_out", "sources": ["in"], "endpoint": "central:5140"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::LogitOut { endpoint, compression, tls, request_timeout } => {
                assert_eq!(endpoint, "central:5140");
                assert_eq!(compression, Compression::None);
                assert_eq!(tls, None);
                assert_eq!(request_timeout, Duration::from_secs(10));
            }
            other => panic!("expected LogitOut, got {other:?}"),
        }
    }

    #[test]
    fn logit_out_with_compression_tls_and_request_timeout_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "logit_out", "sources": ["in"], "endpoint": "central:5140",
                "compression": "lz4",
                "tls": {"insecure_skip_verify": true},
                "request_timeout": "30s"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::LogitOut { compression, tls: Some(tls), request_timeout, .. } => {
                assert_eq!(compression, Compression::Lz4);
                assert!(tls.insecure_skip_verify);
                assert_eq!(tls.ca_file, None);
                assert_eq!(request_timeout, Duration::from_secs(30));
            }
            other => panic!("expected LogitOut with tls set, got {other:?}"),
        }
    }

    #[test]
    fn statsd_out_defaults_to_dogstatsd_over_udp_with_relative_gauges_off() {
        let component: Component = serde_json::from_str(
            r#"{"type": "statsd_out", "sources": ["in"], "endpoint": "127.0.0.1:8125"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::StatsdOut {
                endpoint,
                transport,
                format,
                relative_gauges,
                max_packet_bytes,
                connect_timeout,
                tls,
            } => {
                assert_eq!(endpoint, "127.0.0.1:8125");
                assert_eq!(transport, StatsdTransport::Udp);
                assert_eq!(format, StatsdFormat::Dogstatsd);
                assert!(!relative_gauges);
                assert_eq!(max_packet_bytes, 1432);
                assert_eq!(connect_timeout, Duration::from_secs(5));
                assert_eq!(tls, None);
            }
            other => panic!("expected StatsdOut, got {other:?}"),
        }
    }

    /// The easiest thing here to get silently wrong: `rename_all = "snake_case"` on a variant
    /// spelled `DogStatsd` would yield `dog_statsd`, not `dogstatsd`.
    #[test]
    fn the_dogstatsd_format_variant_deserializes_from_the_single_word_dogstatsd_not_dog_statsd() {
        let component: Component = serde_json::from_str(
            r#"{"type": "statsd_out", "sources": ["in"], "endpoint": "127.0.0.1:8125",
                "format": "dogstatsd"}"#,
        )
        .unwrap();
        assert!(matches!(
            component.kind,
            ComponentKind::StatsdOut { format: StatsdFormat::Dogstatsd, .. }
        ));

        let result: Result<Component, _> = serde_json::from_str(
            r#"{"type": "statsd_out", "sources": ["in"], "endpoint": "127.0.0.1:8125",
                "format": "dog_statsd"}"#,
        );
        assert!(result.is_err(), "\"dog_statsd\" must not be accepted alongside \"dogstatsd\"");
    }

    #[test]
    fn statsd_out_max_packet_bytes_accepts_a_human_byte_string_and_defaults_to_1432() {
        let component: Component = serde_json::from_str(
            r#"{"type": "statsd_out", "sources": ["in"], "endpoint": "127.0.0.1:8125",
                "max_packet_bytes": "8KiB"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::StatsdOut { max_packet_bytes, .. } => {
                assert_eq!(max_packet_bytes, 8 * 1024);
            }
            other => panic!("expected StatsdOut, got {other:?}"),
        }
    }

    #[test]
    fn collectd_out_needs_only_an_endpoint_and_defaults_max_packet_bytes_and_hostname() {
        let component: Component = serde_json::from_str(
            r#"{"type": "collectd_out", "sources": ["in"], "endpoint": "127.0.0.1:25826"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::CollectdOut { endpoint, max_packet_bytes, hostname } => {
                assert_eq!(endpoint, "127.0.0.1:25826");
                assert_eq!(max_packet_bytes, 1452, "collectd's own MaxPacketSize default");
                assert_eq!(hostname, None);
            }
            other => panic!("expected CollectdOut, got {other:?}"),
        }
    }

    #[test]
    fn collectd_out_max_packet_bytes_accepts_a_human_byte_string() {
        let component: Component = serde_json::from_str(
            r#"{"type": "collectd_out", "sources": ["in"], "endpoint": "127.0.0.1:25826",
                "max_packet_bytes": "8KiB"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::CollectdOut { max_packet_bytes, .. } => {
                assert_eq!(max_packet_bytes, 8 * 1024);
            }
            other => panic!("expected CollectdOut, got {other:?}"),
        }
    }

    /// `hostname:` is optional and config-supplied: not an OS-hostname read and not a literal
    /// `"logit"` placeholder.
    #[test]
    fn collectd_out_hostname_is_optional_and_config_supplied() {
        let component: Component = serde_json::from_str(
            r#"{"type": "collectd_out", "sources": ["in"], "endpoint": "127.0.0.1:25826",
                "hostname": "logit-relay"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::CollectdOut { hostname, .. } => {
                assert_eq!(hostname, Some("logit-relay".to_string()));
            }
            other => panic!("expected CollectdOut, got {other:?}"),
        }
    }

    #[test]
    fn graphite_out_needs_only_an_endpoint_and_defaults_everything_else() {
        let component: Component = serde_json::from_str(
            r#"{"type": "graphite_out", "sources": ["in"], "endpoint": "127.0.0.1:2003"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::GraphiteOut {
                endpoint,
                transport,
                protocol,
                tags,
                multi_value,
                max_packet_bytes,
                max_frame_bytes,
                connect_timeout,
            } => {
                assert_eq!(endpoint, "127.0.0.1:2003");
                assert_eq!(transport, GraphiteTransport::Tcp, "carbon's own default listener");
                assert_eq!(protocol, GraphiteProtocol::Plaintext);
                assert_eq!(tags, GraphiteTags::Carbon);
                assert_eq!(multi_value, GraphiteMultiValue::Skip);
                assert_eq!(max_packet_bytes, 1432);
                assert_eq!(max_frame_bytes, 1 << 20, "Twisted's Int32StringReceiver.MAX_LENGTH");
                assert_eq!(connect_timeout, Duration::from_secs(5));
            }
            other => panic!("expected GraphiteOut, got {other:?}"),
        }
    }

    #[test]
    fn graphite_out_enums_deserialize_snake_case() {
        let component: Component = serde_json::from_str(
            r#"{"type": "graphite_out", "sources": ["in"], "endpoint": "127.0.0.1:2004",
                "transport": "udp", "protocol": "pickle", "tags": "drop",
                "multi_value": "expand"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::GraphiteOut { transport, protocol, tags, multi_value, .. } => {
                assert_eq!(transport, GraphiteTransport::Udp);
                assert_eq!(protocol, GraphiteProtocol::Pickle);
                assert_eq!(tags, GraphiteTags::Drop);
                assert_eq!(multi_value, GraphiteMultiValue::Expand);
            }
            other => panic!("expected GraphiteOut, got {other:?}"),
        }
    }

    #[test]
    fn graphite_out_byte_and_duration_fields_accept_human_strings() {
        let component: Component = serde_json::from_str(
            r#"{"type": "graphite_out", "sources": ["in"], "endpoint": "127.0.0.1:2003",
                "max_packet_bytes": "8KiB", "max_frame_bytes": "2MiB",
                "connect_timeout": "10s"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::GraphiteOut {
                max_packet_bytes,
                max_frame_bytes,
                connect_timeout,
                ..
            } => {
                assert_eq!(max_packet_bytes, 8 * 1024);
                assert_eq!(max_frame_bytes, 2 * 1024 * 1024);
                assert_eq!(connect_timeout, Duration::from_secs(10));
            }
            other => panic!("expected GraphiteOut, got {other:?}"),
        }
    }

    #[test]
    fn prometheus_out_needs_only_bind_and_defaults_path_expiry_and_the_series_cap() {
        let component: Component = serde_json::from_str(
            r#"{"type": "prometheus_out", "sources": ["in"], "bind": "127.0.0.1:9464"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::PrometheusOut {
                bind,
                path,
                expire_after,
                max_series,
                endpoint,
                version,
                compression,
                timeout,
                headers,
                endpoint_tls,
            } => {
                assert_eq!(bind.as_deref(), Some("127.0.0.1:9464"));
                assert_eq!(path, "/metrics");
                assert_eq!(
                    expire_after,
                    Duration::from_secs(300),
                    "Prometheus's own staleness horizon"
                );
                assert_eq!(max_series, 100_000);
                assert_eq!(endpoint, None, "registry mode sets no sender field");
                assert_eq!(version, RemoteWriteVersion::V1);
                assert_eq!(compression, RemoteWriteCompression::Snappy);
                assert_eq!(timeout, Duration::from_secs(10));
                assert!(headers.is_empty());
                assert_eq!(endpoint_tls, TlsClientConfig::default());
            }
            other => panic!("expected PrometheusOut, got {other:?}"),
        }
    }

    #[test]
    fn prometheus_out_expire_after_accepts_a_humantime_string_and_zero_to_disable_expiry() {
        for (text, expected) in [("30s", Duration::from_secs(30)), ("0s", Duration::ZERO)] {
            let component: Component = serde_json::from_str(&format!(
                r#"{{"type": "prometheus_out", "sources": ["in"], "bind": "127.0.0.1:9464",
                     "expire_after": "{text}"}}"#
            ))
            .unwrap();
            match component.kind {
                ComponentKind::PrometheusOut { expire_after, .. } => {
                    assert_eq!(expire_after, expected)
                }
                other => panic!("expected PrometheusOut, got {other:?}"),
            }
        }
    }

    #[test]
    fn prometheus_out_path_and_max_series_are_settable() {
        let component: Component = serde_json::from_str(
            r#"{"type": "prometheus_out", "sources": ["in"], "bind": "127.0.0.1:9464",
                "path": "/exposed", "max_series": 25}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::PrometheusOut { path, max_series, .. } => {
                assert_eq!(path, "/exposed");
                assert_eq!(max_series, 25);
            }
            other => panic!("expected PrometheusOut, got {other:?}"),
        }
    }

    /// Neither mode field is required by serde; which of `bind:`/`endpoint:` is set is graph
    /// validation's business, since "exactly one of two fields" is not a shape serde can state.
    /// Both absent deserializes and is rejected by `logit validate` with a message naming both
    /// fields.
    #[test]
    fn prometheus_out_with_neither_mode_field_deserializes_and_is_left_to_rule_56() {
        let component: Component =
            serde_json::from_str(r#"{"type": "prometheus_out", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::PrometheusOut { bind, endpoint, .. } => {
                assert_eq!(bind, None);
                assert_eq!(endpoint, None);
            }
            other => panic!("expected PrometheusOut, got {other:?}"),
        }
    }

    #[test]
    fn prometheus_out_reads_the_sender_mode_fields() {
        let component: Component = serde_json::from_str(
            r#"{"type": "prometheus_out", "sources": ["in"],
                "endpoint": "https://mimir:8080/api/v1/push", "version": 2, "compression": "zstd",
                "timeout": "30s",
                "headers": {"X-Scope-OrgID": "tenant-a"},
                "endpoint_tls": {"ca_file": "ca.pem"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::PrometheusOut {
                bind,
                endpoint,
                version,
                compression,
                timeout,
                headers,
                endpoint_tls,
                ..
            } => {
                assert_eq!(bind, None);
                assert_eq!(endpoint.as_deref(), Some("https://mimir:8080/api/v1/push"));
                assert_eq!(version, RemoteWriteVersion::V2);
                assert_eq!(compression, RemoteWriteCompression::Zstd);
                assert_eq!(timeout, Duration::from_secs(30));
                assert_eq!(headers.get("X-Scope-OrgID").map(String::as_str), Some("tenant-a"));
                assert_eq!(endpoint_tls.ca_file.as_deref(), Some("ca.pem"));
            }
            other => panic!("expected PrometheusOut, got {other:?}"),
        }
    }

    /// `version:` is the integer both specs are numbered by, and an integer that names no spec is
    /// a deserialization error rather than a silent fallback to `1`.
    #[test]
    fn prometheus_out_version_is_an_integer_and_rejects_anything_but_1_or_2() {
        for (text, expected) in [("1", RemoteWriteVersion::V1), ("2", RemoteWriteVersion::V2)] {
            let component: Component = serde_json::from_str(&format!(
                r#"{{"type": "prometheus_out", "sources": ["in"],
                     "endpoint": "http://mimir:8080/api/v1/push", "version": {text}}}"#
            ))
            .unwrap();
            match component.kind {
                ComponentKind::PrometheusOut { version, .. } => assert_eq!(version, expected),
                other => panic!("expected PrometheusOut, got {other:?}"),
            }
        }
        for text in ["0", "3", "\"v1\""] {
            let result: Result<Component, _> = serde_json::from_str(&format!(
                r#"{{"type": "prometheus_out", "sources": ["in"],
                     "endpoint": "http://mimir:8080/api/v1/push", "version": {text}}}"#
            ));
            assert!(result.is_err(), "version: {text} should be rejected");
        }
    }

    #[test]
    fn statsd_out_connect_timeout_accepts_a_humantime_string_and_defaults_to_five_seconds() {
        let component: Component = serde_json::from_str(
            r#"{"type": "statsd_out", "sources": ["in"], "endpoint": "127.0.0.1:8125",
                "transport": "tcp", "connect_timeout": "30s"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::StatsdOut { transport, connect_timeout, .. } => {
                assert_eq!(transport, StatsdTransport::Tcp);
                assert_eq!(connect_timeout, Duration::from_secs(30));
            }
            other => panic!("expected StatsdOut, got {other:?}"),
        }
    }

    /// `tls:` is optional, as on `syslog_out`: absent means plaintext, and presence (even an empty
    /// block) means TLS with the bundled Mozilla roots.
    #[test]
    fn statsd_out_tls_defaults_to_none_and_an_empty_block_is_distinct_from_absent() {
        let component: Component = serde_json::from_str(
            r#"{"type": "statsd_out", "sources": ["in"], "endpoint": "127.0.0.1:8125"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::StatsdOut { tls, .. } => assert_eq!(tls, None),
            other => panic!("expected StatsdOut, got {other:?}"),
        }

        let component: Component = serde_json::from_str(
            r#"{"type": "statsd_out", "sources": ["in"], "endpoint": "relay:8125",
                "transport": "tcp", "tls": {}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::StatsdOut { tls, .. } => {
                assert_eq!(tls, Some(TlsClientConfig::default()));
                assert!(tls.unwrap().is_empty(), "an empty block still means TLS is on");
            }
            other => panic!("expected StatsdOut, got {other:?}"),
        }

        let component: Component = serde_json::from_str(
            r#"{"type": "statsd_out", "sources": ["in"], "endpoint": "relay:8125",
                "transport": "tcp",
                "tls": {"ca_file": "ca.pem", "cert_file": "client.pem", "key_file": "client.key"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::StatsdOut { tls, .. } => {
                let tls = tls.expect("a set tls: block parses");
                assert_eq!(tls.ca_file.as_deref(), Some("ca.pem"));
                assert_eq!(tls.cert_file.as_deref(), Some("client.pem"));
                assert_eq!(tls.key_file.as_deref(), Some("client.key"));
                assert!(!tls.insecure_skip_verify);
            }
            other => panic!("expected StatsdOut, got {other:?}"),
        }
    }

    #[test]
    fn zero_interval_deserializes_fine_left_for_validation_to_reject() {
        // The codec has no opinion on zero; graph validation rejects a zero flush interval.
        let component: Component =
            serde_json::from_str(r#"{"type": "lua", "script": "x", "interval": "0s"}"#).unwrap();
        match component.kind {
            ComponentKind::Lua { interval, .. } => assert_eq!(interval, Some(Duration::ZERO)),
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn negative_interval_is_rejected_by_the_codec() {
        let result: Result<Component, _> =
            serde_json::from_str(r#"{"type": "lua", "script": "x", "interval": "-5s"}"#);
        assert!(result.is_err(), "a negative duration should not silently parse");
    }

    #[test]
    fn interval_round_trips_through_serialize_then_deserialize() {
        let original = Component {
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            buffer: BufferConfig::default(),
            receive: ReceiveConfig::default(),
            kind: ComponentKind::Lua {
                script: "x".to_string(),
                interval: Some(Duration::from_secs(30)),
            },
        };
        let json = serde_json::to_string(&original).unwrap();
        let round_tripped: Component = serde_json::from_str(&json).unwrap();
        match round_tripped.kind {
            ComponentKind::Lua { interval, .. } => {
                assert_eq!(interval, Some(Duration::from_secs(30)));
            }
            other => panic!("expected Lua, got {other:?}"),
        }
    }

    #[test]
    fn stdio_out_defaults_target_to_stdout_when_omitted() {
        let component: Component =
            serde_json::from_str(r#"{"type": "stdio_out", "sources": ["in"]}"#).unwrap();
        match component.kind {
            ComponentKind::StdioOut { target, format, compression } => {
                assert_eq!(target, StdioTarget::Stdout);
                assert_eq!(format, StreamFormat::Human);
                assert_eq!(compression, Compression::None);
            }
            other => panic!("expected StdioOut, got {other:?}"),
        }
    }

    #[test]
    fn stdio_out_target_stdout_deserializes() {
        let target: StdioTarget = serde_json::from_str(r#""stdout""#).unwrap();
        assert_eq!(target, StdioTarget::Stdout);
    }

    #[test]
    fn stdio_out_target_stderr_deserializes() {
        let target: StdioTarget = serde_json::from_str(r#""stderr""#).unwrap();
        assert_eq!(target, StdioTarget::Stderr);
    }

    #[test]
    fn stdio_out_target_anything_else_is_a_path() {
        let target: StdioTarget = serde_json::from_str(r#""/var/log/logit.log""#).unwrap();
        assert_eq!(target, StdioTarget::Path("/var/log/logit.log".to_string()));
    }

    #[test]
    fn file_out_requires_a_path_but_defaults_its_rotate_block() {
        let component: Component = serde_json::from_str(
            r#"{"type": "file_out", "sources": ["in"], "path": "/var/log/logit/events.log"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::FileOut { path, rotate, format, compression } => {
                assert_eq!(path, "/var/log/logit/events.log");
                assert_eq!(rotate, RotateConfig::default());
                assert_eq!(format, StreamFormat::Human);
                assert_eq!(compression, Compression::None);
            }
            other => panic!("expected FileOut, got {other:?}"),
        }
    }

    #[test]
    fn file_out_without_a_path_is_a_clear_deserialize_error() {
        let result: Result<Component, _> =
            serde_json::from_str(r#"{"type": "file_out", "sources": ["in"]}"#);
        assert!(result.is_err());
    }

    #[test]
    fn rotate_config_defaults_to_no_triggers_and_five_max_files() {
        let rotate = RotateConfig::default();
        assert_eq!(rotate.max_bytes, None);
        assert_eq!(rotate.interval, None);
        assert_eq!(rotate.max_files, 5);
    }

    #[test]
    fn a_fully_specified_rotate_block_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "file_out", "sources": ["in"], "path": "events.log",
                "rotate": {"max_bytes": "64MiB", "interval": "daily", "max_files": 3}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::FileOut { rotate, .. } => {
                assert_eq!(rotate.max_bytes, Some(64 * 1024 * 1024));
                assert_eq!(rotate.interval, Some(RotateInterval::Daily));
                assert_eq!(rotate.max_files, 3);
            }
            other => panic!("expected FileOut, got {other:?}"),
        }
    }

    #[test]
    fn each_rotate_interval_variant_deserializes() {
        for (raw, expected) in
            [("hourly", RotateInterval::Hourly), ("daily", RotateInterval::Daily)]
        {
            let interval: RotateInterval = serde_json::from_str(&format!(r#""{raw}""#)).unwrap();
            assert_eq!(interval, expected);
        }
    }

    #[test]
    fn stream_format_defaults_to_human() {
        assert_eq!(StreamFormat::default(), StreamFormat::Human);
    }

    #[test]
    fn each_stream_format_variant_deserializes() {
        for (raw, expected) in [("human", StreamFormat::Human), ("native", StreamFormat::Native)] {
            let format: StreamFormat = serde_json::from_str(&format!(r#""{raw}""#)).unwrap();
            assert_eq!(format, expected);
        }
    }

    #[test]
    fn compression_defaults_to_none() {
        assert_eq!(Compression::default(), Compression::None);
    }

    #[test]
    fn each_compression_variant_deserializes() {
        for (raw, expected) in [("none", Compression::None), ("lz4", Compression::Lz4)] {
            let compression: Compression = serde_json::from_str(&format!(r#""{raw}""#)).unwrap();
            assert_eq!(compression, expected);
        }
    }

    #[test]
    fn a_zstd_compression_value_is_a_clear_deserialize_error() {
        // `zstd` is not a `Compression` variant: `logit_proto::native` rejects it on both encode
        // and decode.
        let result: Result<Compression, _> = serde_json::from_str(r#""zstd""#);
        assert!(result.is_err());
    }

    #[test]
    fn stdio_out_with_format_native_and_compression_lz4_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "stdio_out", "sources": ["in"], "format": "native", "compression": "lz4"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::StdioOut { format, compression, .. } => {
                assert_eq!(format, StreamFormat::Native);
                assert_eq!(compression, Compression::Lz4);
            }
            other => panic!("expected StdioOut, got {other:?}"),
        }
    }

    #[test]
    fn file_out_with_format_native_and_compression_lz4_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "file_out", "sources": ["in"], "path": "events.log",
                "rotate": {"max_bytes": "1MiB"}, "format": "native", "compression": "lz4"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::FileOut { format, compression, .. } => {
                assert_eq!(format, StreamFormat::Native);
                assert_eq!(compression, Compression::Lz4);
            }
            other => panic!("expected FileOut, got {other:?}"),
        }
    }

    #[test]
    fn unknown_type_tag_is_a_clear_error() {
        let result: Result<Component, _> = serde_json::from_str(r#"{"type": "nonsense"}"#);
        assert!(result.is_err());
    }

    #[test]
    fn component_with_no_buffer_block_defaults_to_bufferconfig_default() {
        let component: Component = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["in"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN"}"#,
        )
        .unwrap();
        assert_eq!(component.buffer, BufferConfig::default());
    }

    #[test]
    fn an_empty_buffer_block_deserializes_to_every_default() {
        let component: Component = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["in"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN", "buffer": {}}"#,
        )
        .unwrap();
        assert_eq!(component.buffer, BufferConfig::default());
    }

    #[test]
    fn a_fully_specified_buffer_block_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["in"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN",
                "buffer": {"max_batches": 4096, "max_bytes": "128MiB", "overflow": "drop_oldest",
                           "delivery": "at_least_once", "retry_budget": "120s",
                           "retry_max_delay": "20s", "shutdown_grace": "10s"}}"#,
        )
        .unwrap();
        assert_eq!(component.buffer.max_batches, 4096);
        assert_eq!(component.buffer.max_bytes, 128 * 1024 * 1024);
        assert_eq!(component.buffer.overflow, OverflowPolicy::DropOldest);
        assert_eq!(component.buffer.delivery, Some(DeliveryPosture::AtLeastOnce));
        assert_eq!(component.buffer.retry_budget, Duration::from_secs(120));
        assert_eq!(component.buffer.retry_max_delay, Duration::from_secs(20));
        assert_eq!(component.buffer.shutdown_grace, Duration::from_secs(10));
        assert_eq!(component.buffer.disk, None);
    }

    #[test]
    fn a_fully_specified_disk_block_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["in"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN",
                "buffer": {"disk": {"path": "spool", "max_bytes": "2GiB",
                           "segment_bytes": "128MiB", "compression": "lz4",
                           "checkpoint_interval": "5s"}}}"#,
        )
        .unwrap();
        let disk = component.buffer.disk.expect("disk block should be present");
        assert_eq!(disk.path, "spool");
        assert_eq!(disk.max_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(disk.segment_bytes, 128 * 1024 * 1024);
        assert_eq!(disk.compression, Compression::Lz4);
        assert_eq!(disk.checkpoint_interval, Duration::from_secs(5));
    }

    #[test]
    fn a_disk_block_with_only_path_defaults_every_other_field() {
        let component: Component = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["in"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN",
                "buffer": {"disk": {"path": "spool"}}}"#,
        )
        .unwrap();
        let disk = component.buffer.disk.expect("disk block should be present");
        assert_eq!(disk.path, "spool");
        assert_eq!(disk.max_bytes, 1024 * 1024 * 1024);
        assert_eq!(disk.segment_bytes, 64 * 1024 * 1024);
        assert_eq!(disk.compression, Compression::None);
        assert_eq!(disk.checkpoint_interval, Duration::from_secs(1));
    }

    #[test]
    fn a_disk_block_missing_path_is_rejected() {
        let result: Result<Component, _> = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["in"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN",
                "buffer": {"disk": {}}}"#,
        );
        assert!(result.is_err(), "a disk block with no path should be rejected");
    }

    #[test]
    fn an_unknown_field_under_disk_is_rejected() {
        let result: Result<Component, _> = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["in"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN",
                "buffer": {"disk": {"path": "spool", "bogus_field": 1}}}"#,
        );
        assert!(result.is_err(), "an unknown disk field should be rejected");
    }

    #[test]
    fn each_overflow_variant_deserializes() {
        for (raw, expected) in [
            ("block", OverflowPolicy::Block),
            ("drop_oldest", OverflowPolicy::DropOldest),
            ("drop_newest", OverflowPolicy::DropNewest),
        ] {
            let overflow: OverflowPolicy = serde_json::from_str(&format!(r#""{raw}""#)).unwrap();
            assert_eq!(overflow, expected);
        }
    }

    #[test]
    fn each_delivery_posture_variant_deserializes() {
        for (raw, expected) in [
            ("at_least_once", DeliveryPosture::AtLeastOnce),
            ("at_most_once", DeliveryPosture::AtMostOnce),
        ] {
            let posture: DeliveryPosture = serde_json::from_str(&format!(r#""{raw}""#)).unwrap();
            assert_eq!(posture, expected);
        }
    }

    #[test]
    fn an_unknown_field_under_buffer_is_rejected() {
        let result: Result<Component, _> = serde_json::from_str(
            r#"{"type": "influxdb_out", "sources": ["in"], "url": "http://localhost:8086",
                "org": "org", "bucket": "bucket", "token": "TOKEN",
                "buffer": {"bogus_field": 1}}"#,
        );
        assert!(result.is_err(), "an unknown buffer field should be rejected");
    }

    #[test]
    fn component_with_no_receive_block_defaults_to_receiveconfig_default() {
        let component: Component =
            serde_json::from_str(r#"{"type": "statsd_in", "bind": "0.0.0.0:8125"}"#).unwrap();
        assert_eq!(component.receive, ReceiveConfig::default());
    }

    #[test]
    fn an_empty_receive_block_deserializes_to_every_default() {
        let component: Component =
            serde_json::from_str(r#"{"type": "statsd_in", "bind": "0.0.0.0:8125", "receive": {}}"#)
                .unwrap();
        assert_eq!(component.receive, ReceiveConfig::default());
    }

    #[test]
    fn a_fully_specified_receive_block_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "statsd_in", "bind": "0.0.0.0:8125",
                "receive": {"max_datagrams": 4096, "max_bytes": "16MiB", "overflow": "block",
                            "batch_max_events": 500, "batch_max_bytes": "512KiB",
                            "batch_flush_interval": "250ms", "receive_buffer_bytes": "8MiB",
                            "shutdown_grace": "10s"}}"#,
        )
        .unwrap();
        assert_eq!(component.receive.max_datagrams, 4096);
        assert_eq!(component.receive.max_bytes, 16 * 1024 * 1024);
        assert_eq!(component.receive.overflow, OverflowPolicy::Block);
        assert_eq!(component.receive.batch_max_events, 500);
        assert_eq!(component.receive.batch_max_bytes, 512 * 1024);
        assert_eq!(component.receive.batch_flush_interval, Duration::from_millis(250));
        assert_eq!(component.receive.receive_buffer_bytes, Some(8 * 1024 * 1024));
        assert_eq!(component.receive.shutdown_grace, Duration::from_secs(10));
    }

    #[test]
    fn each_receive_overflow_variant_deserializes() {
        // `each_overflow_variant_deserializes` covers `OverflowPolicy` itself; this confirms
        // `receive.overflow` wires to it.
        for (raw, expected) in [
            ("block", OverflowPolicy::Block),
            ("drop_oldest", OverflowPolicy::DropOldest),
            ("drop_newest", OverflowPolicy::DropNewest),
        ] {
            let component: Component = serde_json::from_str(&format!(
                r#"{{"type": "statsd_in", "bind": "0.0.0.0:8125",
                    "receive": {{"overflow": "{raw}"}}}}"#
            ))
            .unwrap();
            assert_eq!(component.receive.overflow, expected);
        }
    }

    #[test]
    fn receive_buffer_bytes_omitted_stays_none() {
        let component: Component =
            serde_json::from_str(r#"{"type": "statsd_in", "bind": "0.0.0.0:8125", "receive": {}}"#)
                .unwrap();
        assert_eq!(component.receive.receive_buffer_bytes, None);
    }

    #[test]
    fn receive_buffer_bytes_explicit_null_stays_none() {
        let component: Component = serde_json::from_str(
            r#"{"type": "statsd_in", "bind": "0.0.0.0:8125",
                "receive": {"receive_buffer_bytes": null}}"#,
        )
        .unwrap();
        assert_eq!(component.receive.receive_buffer_bytes, None);
    }

    #[test]
    fn an_unknown_field_under_receive_is_rejected() {
        let result: Result<Component, _> = serde_json::from_str(
            r#"{"type": "statsd_in", "bind": "0.0.0.0:8125",
                "receive": {"bogus_field": 1}}"#,
        );
        assert!(result.is_err(), "an unknown receive field should be rejected");
    }

    #[test]
    fn components_schema_requires_at_least_one_entry() {
        let schema = json_schema();
        let components = schema
            .schema
            .object
            .expect("config should be an object")
            .properties
            .remove("components")
            .expect("config should define components");
        let Schema::Object(components) = components else {
            panic!("components should have an object schema");
        };
        assert_eq!(
            components.object.expect("components should be an object").min_properties,
            Some(1)
        );
    }

    #[test]
    fn prometheus_in_requires_only_scrape_targets_and_defaults_the_rest() {
        let component: Component = serde_json::from_str(
            r#"{"type": "prometheus_in", "scrape_targets": ["http://node-exporter:9100/metrics"]}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::PrometheusIn {
                scrape_targets,
                interval,
                timeout,
                headers,
                scrape_tls,
                bind,
                path,
                bind_tls,
                idle_timeout,
                metadata_cache,
            } => {
                assert_eq!(scrape_targets, vec!["http://node-exporter:9100/metrics".to_string()]);
                assert_eq!(interval, Duration::from_secs(15));
                assert_eq!(timeout, Duration::from_secs(10));
                assert!(headers.is_empty());
                assert_eq!(scrape_tls, TlsClientConfig::default());
                // The receiver half all defaults away, so a scrape config is untouched by it, and
                // graph validation reads these defaults.
                assert_eq!(bind, None);
                assert_eq!(path, "/api/v1/write");
                assert_eq!(bind_tls, None);
                assert_eq!(idle_timeout, None);
                assert_eq!(metadata_cache, MetadataCacheConfig::default());
                assert_eq!(metadata_cache.max_families, 10_000);
                assert_eq!(metadata_cache.ttl, Duration::from_secs(600));
            }
            other => panic!("expected PrometheusIn, got {other:?}"),
        }
    }

    /// The cache's two bounds are independent, and `0` families is legal (it turns the cache off)
    /// where `0s` is not; graph validation, not serde, rejects the latter.
    #[test]
    fn prometheus_in_metadata_cache_deserializes_each_bound_on_its_own() {
        let cache = |json: &str| -> MetadataCacheConfig {
            let component: Component = serde_json::from_str(&format!(
                r#"{{"type": "prometheus_in", "bind": "0.0.0.0:9090", "metadata_cache": {json}}}"#
            ))
            .unwrap();
            match component.kind {
                ComponentKind::PrometheusIn { metadata_cache, .. } => metadata_cache,
                other => panic!("expected PrometheusIn, got {other:?}"),
            }
        };

        assert_eq!(
            cache(r#"{"max_families": 250, "ttl": "90s"}"#),
            MetadataCacheConfig { max_families: 250, ttl: Duration::from_secs(90) }
        );
        // Each field defaults on its own, so setting one never silently resets the other.
        assert_eq!(
            cache(r#"{"max_families": 0}"#),
            MetadataCacheConfig { max_families: 0, ttl: Duration::from_secs(600) }
        );
        assert_eq!(
            cache(r#"{"ttl": "1h"}"#),
            MetadataCacheConfig { max_families: 10_000, ttl: Duration::from_secs(3600) }
        );
        assert_eq!(cache("{}"), MetadataCacheConfig::default());
    }

    /// Denies unknown fields: a misspelled key would otherwise deserialize to the defaults, so the
    /// cap written would be ignored and mode validation would see a defaulted block, letting the
    /// typo pass under `scrape_targets:`.
    #[test]
    fn prometheus_in_metadata_cache_rejects_a_misspelled_key() {
        let err = serde_json::from_str::<Component>(
            r#"{"type": "prometheus_in", "bind": "0.0.0.0:9090",
                "metadata_cache": {"max_familes": 500}}"#,
        )
        .expect_err("a misspelled key must not deserialize to the defaults");
        assert!(err.to_string().contains("max_familes"), "got: {err}");
    }

    #[test]
    fn prometheus_in_interval_timeout_headers_and_scrape_tls_can_all_be_set() {
        let component: Component = serde_json::from_str(
            r#"{"type": "prometheus_in", "scrape_targets": ["https://node-exporter:9100/metrics"],
                "interval": "30s", "timeout": "5s",
                "headers": {"X-Scope-OrgID": "tenant-a"},
                "scrape_tls": {"ca_file": "ca.pem"}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::PrometheusIn { interval, timeout, headers, scrape_tls, .. } => {
                assert_eq!(interval, Duration::from_secs(30));
                assert_eq!(timeout, Duration::from_secs(5));
                assert_eq!(headers.get("X-Scope-OrgID"), Some(&"tenant-a".to_string()));
                assert_eq!(scrape_tls.ca_file, Some("ca.pem".to_string()));
            }
            other => panic!("expected PrometheusIn, got {other:?}"),
        }
    }

    #[test]
    fn prometheus_in_rejects_an_empty_scrape_targets_list_at_deserialize_time_only_if_required() {
        // `scrape_targets` has a `#[serde(default)]`, so an omitted or empty list both
        // deserialize here; graph validation is what rejects an empty list.
        let component: Component =
            serde_json::from_str(r#"{"type": "prometheus_in", "scrape_targets": []}"#).unwrap();
        match component.kind {
            ComponentKind::PrometheusIn { scrape_targets, .. } => {
                assert!(scrape_targets.is_empty())
            }
            other => panic!("expected PrometheusIn, got {other:?}"),
        }
    }

    #[test]
    fn generate_in_defaults_everything_but_the_batch_size() {
        let component: Component = serde_json::from_str(r#"{"type": "generate_in"}"#).unwrap();
        match component.kind {
            ComponentKind::GenerateIn { count, batch, rate, event, resource } => {
                assert_eq!(count, None, "omitted count means unbounded");
                assert_eq!(batch, 100);
                assert_eq!(rate, None, "omitted rate means unthrottled");
                assert_eq!(event, GenerateEvent::default());
                assert!(resource.is_empty());
            }
            other => panic!("expected GenerateIn, got {other:?}"),
        }
    }

    #[test]
    fn generate_in_reads_every_field_including_the_event_template() {
        let component: Component = serde_json::from_str(
            r#"{"type": "generate_in", "count": 2000000, "batch": 500, "rate": 50000,
                "resource": {"service.name": "web"},
                "event": {
                  "log": "{{\"path\":\"/x/{seq%50}\"}}",
                  "attributes": {"host": "web-{seq%10}"},
                  "metric": {"name": "requests", "kind": "distribution", "value": 2.5}
                }}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::GenerateIn { count, batch, rate, event, resource } => {
                assert_eq!(count, Some(2_000_000));
                assert_eq!(batch, 500);
                assert_eq!(rate, Some(50_000));
                assert_eq!(resource.get("service.name"), Some(&"web".to_string()));
                // Still the raw template text, doubled braces and all; unescaping is
                // `logit_core::template::parse`'s job.
                assert_eq!(event.log.as_deref(), Some(r#"{{"path":"/x/{seq%50}"}}"#));
                assert_eq!(event.attributes.get("host"), Some(&"web-{seq%10}".to_string()));
                assert_eq!(
                    event.metric,
                    Some(GenerateMetric {
                        name: "requests".to_string(),
                        kind: GenerateMetricKind::Distribution,
                        value: 2.5,
                    })
                );
            }
            other => panic!("expected GenerateIn, got {other:?}"),
        }
    }

    /// A `metric:` block names only what it has to; `sum` at `1` is the counter shape the
    /// defaults are chosen around.
    #[test]
    fn a_generate_metric_defaults_to_a_sum_of_one() {
        let component: Component = serde_json::from_str(
            r#"{"type": "generate_in", "event": {"metric": {"name": "requests"}}}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::GenerateIn { event, .. } => {
                assert_eq!(
                    event.metric,
                    Some(GenerateMetric {
                        name: "requests".to_string(),
                        kind: GenerateMetricKind::Sum,
                        value: 1.0,
                    })
                );
            }
            other => panic!("expected GenerateIn, got {other:?}"),
        }
    }

    #[test]
    fn generate_in_rejects_an_unknown_field_inside_its_event_block() {
        let err = serde_json::from_str::<Component>(
            r#"{"type": "generate_in", "event": {"logs": "oops"}}"#,
        )
        .expect_err("`deny_unknown_fields` should catch a misspelled template field");
        assert!(err.to_string().contains("unknown field `logs`"), "got: {err}");
    }

    #[test]
    fn null_out_deserializes_with_nothing_but_its_sources() {
        let component: Component =
            serde_json::from_str(r#"{"type": "null_out", "sources": ["gen"]}"#).unwrap();
        assert_eq!(component.sources, vec!["gen".to_string()]);
        match component.kind {
            ComponentKind::NullOut {} => {}
            other => panic!("expected NullOut, got {other:?}"),
        }
    }

    #[test]
    fn target_deserializes_with_no_fields() {
        let component: Component = serde_json::from_str(r#"{"type": "target"}"#).unwrap();
        assert!(component.sources.is_empty());
        match component.kind {
            ComponentKind::Target {} => {}
            other => panic!("expected Target, got {other:?}"),
        }
    }

    #[test]
    fn route_deserializes_with_by_provenance_origin() {
        let component: Component = serde_json::from_str(
            r#"{
                "type": "route",
                "sources": ["central_in"],
                "by": {"provenance": "origin"},
                "routes": {"host": "host_stream", "app": "app_stream"}
            }"#,
        )
        .unwrap();
        assert_eq!(component.sources, vec!["central_in".to_string()]);
        match component.kind {
            ComponentKind::Route { by, routes } => {
                assert_eq!(by, RouteBy::Provenance(ProvenanceField::Origin));
                assert_eq!(
                    routes,
                    std::collections::BTreeMap::from([
                        ("host".to_string(), "host_stream".to_string()),
                        ("app".to_string(), "app_stream".to_string()),
                    ])
                );
            }
            other => panic!("expected Route, got {other:?}"),
        }
    }

    #[test]
    fn route_by_attribute_and_resource_deserialize() {
        let component: Component = serde_json::from_str(
            r#"{
                "type": "route",
                "sources": ["in"],
                "by": {"attribute": "stream"},
                "routes": {"host": "host_stream"}
            }"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Route { by, .. } => {
                assert_eq!(by, RouteBy::Attribute("stream".to_string()));
            }
            other => panic!("expected Route, got {other:?}"),
        }

        let component: Component = serde_json::from_str(
            r#"{
                "type": "route",
                "sources": ["in"],
                "by": {"resource": "service.name"},
                "routes": {"web": "web_stream"}
            }"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::Route { by, .. } => {
                assert_eq!(by, RouteBy::Resource("service.name".to_string()));
            }
            other => panic!("expected Route, got {other:?}"),
        }
    }

    #[test]
    fn component_targets_default_to_empty() {
        let component: Component =
            serde_json::from_str(r#"{"type": "null_out", "sources": ["gen"]}"#).unwrap();
        assert!(component.targets.is_empty());
    }

    #[test]
    fn a_lua_component_parses_its_targets_list() {
        let component: Component = serde_json::from_str(
            r#"{"type": "lua", "script": "return event", "targets": ["a", "b"]}"#,
        )
        .unwrap();
        assert_eq!(component.targets, vec!["a".to_string(), "b".to_string()]);
    }

    /// `statsd_in`'s optional fields all default and all round-trip when set: the bare
    /// `{"type": "statsd_in", "bind": ...}` shape keeps deserializing, and a `transport: tcp`
    /// listener carries a `tls:` block and a `handshake_timeout`.
    #[test]
    fn statsd_in_round_trips_transport_tls_and_handshake_timeout() {
        let bare: Component =
            serde_json::from_str(r#"{"type": "statsd_in", "bind": "0.0.0.0:8125"}"#).unwrap();
        match bare.kind {
            ComponentKind::StatsdIn { bind, transport, tls, handshake_timeout, idle_timeout } => {
                assert_eq!(bind, "0.0.0.0:8125");
                assert_eq!(transport, StatsdTransport::Udp, "classic statsd stays the default");
                assert_eq!(tls, None);
                assert_eq!(handshake_timeout, Duration::from_secs(5));
                assert_eq!(idle_timeout, None, "opt-in -- no idle timeout unless asked for");
            }
            other => panic!("expected StatsdIn, got {other:?}"),
        }

        let full: Component = serde_json::from_str(
            r#"{"type": "statsd_in", "bind": "0.0.0.0:8125", "transport": "tcp",
                "tls": {"cert_file": "server.pem", "key_file": "server.key",
                        "client_ca_file": "ca.pem"},
                "handshake_timeout": "2s"}"#,
        )
        .unwrap();
        match full.kind {
            ComponentKind::StatsdIn { transport, tls: Some(tls), handshake_timeout, .. } => {
                assert_eq!(transport, StatsdTransport::Tcp);
                assert_eq!(tls.cert_file, "server.pem");
                assert_eq!(tls.key_file, "server.key");
                assert_eq!(tls.client_ca_file, Some("ca.pem".to_string()));
                assert_eq!(handshake_timeout, Duration::from_secs(2));
            }
            other => panic!("expected StatsdIn with tls set, got {other:?}"),
        }

        // `transport` alone must not imply TLS.
        let plaintext_tcp: Component = serde_json::from_str(
            r#"{"type": "statsd_in", "bind": "0.0.0.0:8125", "transport": "tcp"}"#,
        )
        .unwrap();
        match plaintext_tcp.kind {
            ComponentKind::StatsdIn { transport, tls, .. } => {
                assert_eq!(transport, StatsdTransport::Tcp);
                assert_eq!(tls, None);
            }
            other => panic!("expected StatsdIn, got {other:?}"),
        }
    }
}
