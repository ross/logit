//! Config types for `logit`.
//!
//! Every type here derives both `Deserialize` and `JsonSchema` together (ADR 0003) so the
//! published JSON Schema (`logit schema`, `schema/logit.schema.json`) can never drift from what
//! the binary actually accepts. YAML parsing itself (via a maintained `serde_yaml` fork, per
//! ADR 0003) belongs to `logit-cli`, not here -- this crate only defines the shape.
//!
//! Config is one flat graph of named [`Component`]s (ADR 0009,
//! `docs/design/pipeline-graph.md`) -- there is no separate inputs/outputs/pipelines split. A
//! component's `sources` name the other components it reads from; its `type`-tagged
//! [`ComponentKind`] fixes its arity (a listener has none, a sink has at least one and is never
//! itself a source, a transform has both). Resolving that graph into something runnable -- cycle
//! detection, arity checks, topological ordering -- is `logit-cli`/`logit-pipeline`'s job, not
//! this crate's; this crate only defines the shape serde and `schemars` need to agree on.

use schemars::{gen::SchemaGenerator, schema::Schema, JsonSchema};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Config {
    #[serde(default)]
    #[schemars(schema_with = "non_empty_components_schema")]
    pub components: HashMap<String, Component>,
}

fn non_empty_components_schema(generator: &mut SchemaGenerator) -> Schema {
    let mut schema = HashMap::<String, Component>::json_schema(generator);
    if let Schema::Object(schema) = &mut schema {
        schema.object().min_properties = Some(1);
    }
    schema
}

/// One node in the pipeline's component graph. `sources` names the other components this one
/// reads events from -- empty for a listener, required for everything else (enforced at
/// validation time, not in this schema: which arity is legal depends on `kind`, not something a
/// blanket `minItems` on this shared field can express). See `docs/design/pipeline-graph.md` for
/// the full arity table and validation rules.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct Component {
    #[serde(default)]
    pub sources: Vec<String>,
    /// Per-sink delivery buffer (`docs/adr/0021-buffered-sink-delivery.md`). Meaningful only on a
    /// sink -- graph validation (`crates/logit-pipeline/src/graph.rs`) rejects a non-default value
    /// on any other kind. A sibling field of `kind`, not nested inside every sink
    /// `ComponentKind` variant, so a future fifth sink kind costs nothing extra here.
    #[serde(default)]
    pub buffer: BufferConfig,
    /// Per-listener receive queue and batching (`docs/adr/0026-decoupled-listener-io.md`).
    /// Meaningful only on a datagram listener -- graph validation rejects a non-default value on
    /// any other kind, `internal` included. A sibling field of `kind`, mirroring `buffer`'s own
    /// placement.
    #[serde(default)]
    pub receive: ReceiveConfig,
    #[serde(flatten)]
    pub kind: ComponentKind,
}

/// One `kv_metrics` entry: a metric `name`, an optional source `field`, and an optional `unit`.
/// See `ComponentKind::KvMetrics` and `docs/adr/0014-kv-metrics-semantics.md`.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub struct MetricSpec {
    /// The metric's measurement name. An empty name is rejected at graph-validation time --
    /// `influxdb_out` requires a non-empty measurement to encode a metric line.
    pub name: String,
    /// The attribute to read this metric's value from. Omitted means "+1 per event" for a
    /// counter or "set to 1" for a gauge; a distribution entry with no `field` is rejected at
    /// graph-validation time (a distribution of nothing is meaningless). Names an attribute
    /// literally -- `field: http.status` means the attribute literally named `http.status`, never
    /// a `status` key nested under `http` in a `Value::Map`; nested fields are not addressable.
    #[serde(default)]
    pub field: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
}

/// Which OTLP transport a component speaks -- both `otlp_in` and `otlp_out` carry identical
/// protobuf payloads (`crates/logit-proto/src/otlp`), differing only in framing and endpoint
/// shape (`docs/adr/0024-hand-rolled-grpc-over-hyper.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OtlpProtocol {
    /// OTLP/HTTP, protobuf body, one POST per signal. Default: it's what an
    /// `http://host:4318`-shaped endpoint already implies, and a bare `endpoint: http://tempo:4318`
    /// would be silently wrong under any other default.
    #[default]
    Http,
    /// Unary gRPC over plaintext HTTP/2 -- `crates/logit-outputs/src/otlp.rs`'s hand-rolled gRPC
    /// transport has no TLS support at all (`docs/adr/0024-hand-rolled-grpc-over-hyper.md`), so an
    /// `otlp_out` component's `endpoint` under this protocol is rejected at construction time if
    /// it's written with an `https://` scheme, rather than silently exporting in plaintext.
    Grpc,
}

/// `ComponentKind::Internal`'s `span_sample_rate` default when a config omits it -- re-exported
/// from `logit-core` (not restated as a bare literal here) so the two crates can never drift
/// apart on what "the default" actually is. This is also the one place `logit-config` depends on
/// `logit-core` at all: a small, deliberately narrow edge (one `pub const`), not a general
/// dependency on the event model this crate otherwise has no business needing.
fn default_span_sample_rate() -> f64 {
    logit_core::DEFAULT_SPAN_SAMPLE_RATE
}

/// A component's kind, tagged by `type` in config. Every protocol kind is suffixed `_in`/`_out`
/// uniformly (`docs/design/pipeline-graph.md`'s naming rationale) so a listener and a sink for the
/// same protocol never collide on one tag value; transform kinds take no suffix, since there's
/// only one direction for a transform to be.
#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ComponentKind {
    /// statsd / DogStatsD-style tagged metrics over UDP.
    StatsdIn {
        bind: String,
    },
    /// RFC 3164 / RFC 5424 syslog over UDP. **Not** TCP, despite this doc comment's old claim --
    /// `crates/logit-inputs/src/syslog.rs`'s own module doc has always said UDP-only (nginx's
    /// `syslog:` writer is UDP-only, so a TCP accept loop would buy this listener nothing;
    /// `docs/known-gaps.md`'s "syslog TCP and structured data" entry tracks it as future,
    /// additive work). `syslog_out` (the egress side, `docs/adr/0022-syslog-output.md`) supports
    /// both UDP and TCP -- that asymmetry is deliberate, not a sign this needs fixing to match.
    SyslogIn {
        bind: String,
    },
    /// OpenTelemetry Protocol (logs, metrics, and/or traces).
    OtlpIn {
        bind: String,
        #[serde(default)]
        protocol: OtlpProtocol,
    },
    /// Tail one or more files as a log source, rotation- and checkpoint-aware.
    FileTail {
        paths: Vec<String>,
        #[serde(default)]
        checkpoint_path: Option<String>,
    },
    /// The native logit-to-logit protocol (`docs/design/wire-protocol.md`).
    LogitIn {
        bind: String,
    },
    /// `logit` talking about itself: drains every component's buffered self-telemetry points on
    /// `interval` and emits them as ordinary events into the graph, same as any other listener.
    /// Named for the source, not the signal it emits today -- free to grow logs and spans later
    /// without a rename. See `docs/design/internal-telemetry.md` and
    /// `docs/adr/0018-internal-telemetry-as-pipeline-events.md`.
    Internal {
        /// Both the drain cadence for every component's buffered points and the sampling tick for
        /// this component's own process-level gauges (interner size, uptime). Should divide
        /// evenly into any downstream `aggregate` interval, or the two windows beat against each
        /// other.
        #[serde(with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        interval: Duration,
        /// Fraction of traces whose internal spans are kept, `0.0..=1.0`, decided per-`trace_id`
        /// the same way at every node -- a kept trace is kept at every hop, never partially. Below
        /// `1.0` by default: span volume is a different shape than metric volume (one span per
        /// node-visit per batch, where a metric point coalesces between drains). `0.0` turns spans
        /// off entirely; `1.0` keeps everything -- e.g. a demo or debugging config that wants full
        /// traces rather than a representative sample would set this explicitly. Named
        /// `span_sample_rate`, not `sample_rate` -- there is already a `ComponentKind::Sample`
        /// transform, and `internal` may grow other sampling knobs later. See
        /// `docs/adr/0025-internal-span-emission-and-deterministic-sampling.md`.
        #[serde(default = "default_span_sample_rate")]
        span_sample_rate: f64,
    },

    /// Inline Lua source (a YAML block scalar in practice). See `docs/design/lua-api.md`.
    Lua {
        script: String,
        /// Runs this component's `flush()`, if the script defines one, on this interval
        /// (`docs/design/lua-api.md`'s flush contract). Omitted -- the common case -- means the
        /// component never ticks, same as a script with no `flush()` at all.
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        interval: Option<Duration>,
    },
    /// A `.lua` file path, relative to the config file.
    LuaFile {
        lua_file: String,
        #[serde(default, with = "humantime_serde_duration::option")]
        #[schemars(with = "Option<String>")]
        interval: Option<Duration>,
    },
    /// The stateful aggregator (counters/gauges/sets/distributions). Runs `flush()` on
    /// `interval`; see `docs/adr/0008-aggregation-window-semantics.md`.
    Aggregate {
        #[serde(with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        interval: Duration,
    },
    /// Parses a log record's message as JSON, merging the resulting key/values into the event's
    /// attributes. See `docs/adr/0010-json-parsing-into-attributes.md`.
    Json {
        /// Skip everything before the first `{` and parse from there -- for lines with a
        /// non-JSON prefix (`2026-08-29 INFO {"a":1}`). Off by default: the whole line is
        /// assumed to be the JSON data.
        #[serde(default)]
        skip_to_brace: bool,
    },
    /// Turns attributes already on an event (typically merged there by `json`) into metrics on
    /// that same event. See `docs/adr/0014-kv-metrics-semantics.md` for the skip rules, the
    /// numeric coercion rules, and why there is deliberately no `tags:` field here -- tag
    /// selection is `Keep`'s job, since every metrics sink already reads `event.attributes`.
    KvMetrics {
        #[serde(default)]
        counters: Vec<MetricSpec>,
        #[serde(default)]
        gauges: Vec<MetricSpec>,
        #[serde(default)]
        distributions: Vec<MetricSpec>,
    },
    /// Retains only the named attributes, dropping the rest -- an allowlist, not just a denylist:
    /// a new field appearing in a log format later must not be able to silently become a new
    /// tag dimension on a metrics sink. Place this *before* `aggregate` in a pipeline --
    /// `aggregate`'s `SeriesKey` includes the whole of `event.attributes`, so pruning first is
    /// what keeps series cardinality and per-window memory bounded. An empty `fields` list is
    /// legal and means "drop every attribute."
    Keep {
        fields: Vec<String>,
    },
    /// Drops the named attributes, keeping the rest.
    Remove {
        fields: Vec<String>,
    },
    // The rest of the built-in native transforms -- not implemented yet (`logit-transforms`),
    // carried over as unimplemented `ComponentKind` variants so config referencing one gets a
    // clear "not implemented yet" at validation time rather than a deserialization error.
    Logfmt,
    Kv,
    Regex {
        pattern: String,
    },
    Csv,
    Rename {
        from: String,
        to: String,
    },
    Filter {
        r#where: String,
    },
    Sample {
        rate: f64,
    },
    Throttle {
        limit: u64,
        #[serde(with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        window: Duration,
    },
    Dedup {
        key: String,
    },

    /// `rename`d explicitly: `rename_all = "snake_case"` alone would tag this `influx_db_out`
    /// (a word break at the embedded capital `Db`), not `influxdb_out` as published in
    /// `docs/design/pipeline-graph.md` and every example config.
    #[serde(rename = "influxdb_out")]
    InfluxDbOut {
        url: String,
        org: String,
        bucket: String,
        /// A plain string field like any other -- give it `!env INFLUXDB_TOKEN` in config to
        /// pull it from the environment (`crates/logit-cli/src/config.rs`) rather than inlining
        /// it. No env-specific field of its own: `!env` works on any field on any component, so
        /// `url`/`org`/`bucket` (just as deployment-specific) can use it too.
        token: String,
    },
    OtlpOut {
        endpoint: String,
        #[serde(default)]
        protocol: OtlpProtocol,
    },
    /// The native logit-to-logit protocol (`docs/design/wire-protocol.md`).
    LogitOut {
        endpoint: String,
    },
    /// A general-purpose, human-facing debug sink: dumps every event's details as a readable text
    /// block to stdout (default), stderr, or a file -- the dev loop for seeing a whole pipeline's
    /// output without standing up a real backend like InfluxDB.
    StdioOut {
        #[serde(default)]
        target: StdioTarget,
    },
    /// RFC 3164 / RFC 5424 syslog egress over UDP or TCP -- the mirror of `SyslogIn`, and a real
    /// relay: header fields round-trip from an event's `syslog.*` attributes when present,
    /// falling back to the defaults below only when an event carries none (e.g. one that never
    /// passed through `syslog_in`). See `docs/adr/0022-syslog-output.md`.
    SyslogOut {
        /// `host:port`. Resolved at connect/bind time, never at config-load time -- a `syslog_out`
        /// pointed at a destination that isn't up yet is not a config error (`!env` still applies
        /// like any other string field, ADR 0011).
        endpoint: String,
        #[serde(default)]
        transport: SyslogTransport,
        /// RFC 3164 carries no year and no timezone in its TIMESTAMP, so a receiver has to guess
        /// both -- `rfc5424`'s unambiguous RFC 3339 timestamp is the better default; `syslog_in`
        /// already parses both dialects, so emitting both is parity, not new scope.
        #[serde(default)]
        format: SyslogFormat,
        /// PRI facility used only when the event carries no `syslog.facility` attribute.
        #[serde(default)]
        facility: SyslogFacility,
        /// HOSTNAME/APP-NAME fallbacks, used only when the event carries no `syslog.hostname`/
        /// `syslog.tag` attribute -- e.g. an event that never passed through `syslog_in`. Omitted
        /// entirely (rather than a literal `logit` default) so a relayed line's origin is never
        /// silently overwritten with something that looks like a config mistake.
        #[serde(default)]
        hostname: Option<String>,
        #[serde(default)]
        app_name: Option<String>,
        /// Bounds one encoded message (PRI + header + MSG). Defaults to 8192, matching Grafana
        /// Alloy's `loki.source.syslog` `max_message_length` default -- the receiver the demo
        /// stack points this at -- rather than RFC 3164 §4.1's traditional 1024, which would
        /// truncate a JSON-bodied message on every modern relay chain. A string via
        /// [`human_bytes`], exactly like `BufferConfig::max_bytes`.
        #[serde(default = "default_max_message_bytes", with = "human_bytes")]
        #[schemars(with = "String")]
        max_message_bytes: u64,
        /// TCP only, ignored for UDP. How long a connect attempt (including a reconnect after a
        /// dropped connection) is allowed to take before `send` reports it as a failure.
        #[serde(default = "default_syslog_connect_timeout", with = "humantime_serde_duration")]
        #[schemars(with = "String")]
        connect_timeout: Duration,
    },
}

fn default_max_message_bytes() -> u64 {
    8192
}

/// Mirrors `logit_outputs::syslog::DEFAULT_CONNECT_TIMEOUT` -- can't reference it directly
/// (`logit-outputs` depends on `logit-config`, never the reverse), so keep the two in sync by
/// hand if this ever changes.
fn default_syslog_connect_timeout() -> Duration {
    Duration::from_secs(5)
}

/// `syslog_out`'s transport. UDP (the default) mirrors `syslog_in` and needs no ordering
/// guarantee against the receiver's startup -- a fire-and-forget `send_to` before the receiver is
/// up just loses that line, the same honest limit `syslog_in`'s own UDP intake accepts on the way
/// in. TCP is what makes `Fault` classification (`docs/adr/0021-buffered-sink-delivery.md`)
/// meaningful for this sink: a connect failure is unambiguously `Fault::Clean`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyslogTransport {
    #[default]
    Udp,
    Tcp,
}

/// Which syslog dialect `syslog_out` emits. See `SyslogOut::format`'s doc comment for why
/// `rfc5424` is the default.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SyslogFormat {
    Rfc3164,
    #[default]
    Rfc5424,
}

/// The syslog PRI facility, named rather than a bare `0..=23` integer so schemars publishes a
/// real enumeration and a typo is a config error rather than a silently-wrong PRI. Ordered to
/// match the standard facility codes (`kern` is 0); `as_u8` reads the discriminant back out.
/// `local0` defaults, matching `demo/hello/app.py`'s own `PRI = 134` (facility 16), so the demo
/// round-trips its own PRI unchanged even before `syslog.facility` attribute precedence applies.
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

/// Where `stdio_out` writes: config keeps this a plain scalar (`target: stdout`), not a tagged
/// object -- `stdout` and `stderr` are matched as keywords first, and anything else is treated as
/// a file path.
///
/// `Serialize`/`Deserialize`/`JsonSchema` are all hand-rolled rather than derived: a derived
/// `#[serde(untagged)]` dispatches each candidate variant against the input's *shape*, and a
/// fieldless (unit) variant's shape is "absent/null", not "any string that happens to match the
/// variant's name" -- so a plain `#[derive(Deserialize)]` here would never actually match the
/// literal string `"stdout"` against the `Stdout` variant, and every value (including `"stdout"`
/// itself) would silently fall through to `Path`. Matching a string against the two known
/// keywords first, `Path` as the explicit fallback, has to be written by hand instead --  and
/// `JsonSchema` follows suit (delegating straight to `String`'s schema) rather than letting a
/// derive describe the shape the broken derived (de)serializer *would* have accepted: every value
/// this type actually accepts is a string, `stdout`/`stderr` included, so that's the schema ADR
/// 0003 needs published, not an artifact of what a derive would guess from the variants.
///
/// A relative `Path` is resolved against the config file's own directory (`crates/logit-cli/src/
/// pipeline.rs::build_spec`, mirroring how `LuaFile { lua_file, .. }` resolves its script path) --
/// not the process's current working directory, so "next to the config" below is literal.
///
/// Two consequences worth knowing, both accepted rather than worked around:
/// - A file literally named `stdout` (or `stderr`) next to the config is unreachable this way --
///   write `./stdout` in config to target it instead.
/// - A typo like `stdrr` silently becomes a file path rather than a config error. That's the price
///   of the one-field shape; it's visible immediately in practice, since the (wrongly-named) file
///   appears next to the config the moment an event is written.
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

/// Per-sink delivery buffer (`docs/adr/0021-buffered-sink-delivery.md`). Meaningful only on a
/// sink; graph validation (`crates/logit-pipeline/src/graph.rs`) rejects a non-default value on
/// any other kind. Every field defaults, so an omitted `buffer:` block is exactly today's
/// behavior.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct BufferConfig {
    pub max_batches: usize,
    /// Byte bound on the buffer's estimated heap footprint (`EventBatch::estimated_heap_bytes`),
    /// checked alongside `max_batches` -- whichever trips first. A quoted string in YAML --
    /// `"64MiB"` or a plain `"134217728"` -- via the [`human_bytes`] codec below, string-only in
    /// both directions to match this field's published schema exactly (an unquoted number is
    /// rejected, not silently accepted).
    #[serde(with = "human_bytes")]
    #[schemars(with = "String")]
    pub max_bytes: u64,
    pub overflow: OverflowPolicy,
    /// `None` -- the default -- means "derive from the sink's own `duplicate_safe()` fact"
    /// (`docs/adr/0021-buffered-sink-delivery.md`'s three-layer posture design). `Some(_)`
    /// overrides that default for this component specifically.
    #[serde(default)]
    pub delivery: Option<DeliveryPosture>,
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub retry_budget: Duration,
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub retry_max_delay: Duration,
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub shutdown_grace: Duration,
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
        }
    }
}

/// What a sink's `SinkQueue` does once both its bounds (`max_batches`/`max_bytes`) are full.
/// `logit-config`'s own copy -- `logit-config` must not depend on `logit-pipeline`
/// (`docs/design/pipeline-graph.md`'s crate layout), so `crates/logit-cli/src/pipeline.rs`
/// converts this to `logit_pipeline::OverflowPolicy` when building a `NodeSpec::Output`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OverflowPolicy {
    Block,
    DropOldest,
    DropNewest,
}

/// Whether re-delivering an already-delivered batch is safe for a sink's destination -- see
/// `BufferConfig::delivery`. `logit-config`'s own copy, for the same crate-layout reason as
/// [`OverflowPolicy`]; converted to `logit_pipeline::DeliveryPosture` in
/// `crates/logit-cli/src/pipeline.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryPosture {
    AtLeastOnce,
    AtMostOnce,
}

/// Per-listener receive queue and datagram-\>batch assembly
/// (`docs/adr/0026-decoupled-listener-io.md`). Meaningful only on a datagram listener (today
/// `statsd_in`/`syslog_in`); graph validation (`crates/logit-pipeline/src/graph.rs`) rejects a
/// non-default value on any other kind, including `internal` (a listener by role, but one with no
/// socket, no queue, and no decoder). Flat, following [`BufferConfig`]'s own
/// `retry_budget`/`retry_max_delay` precedent rather than nesting a `batch:` sub-block -- two
/// levels of optional-with-defaults is harder to scan in YAML than a flat prefix. Every field
/// defaults, so a `receive:` block is never required -- **but an omitted block is not byte-for-
/// byte the pre-ADR-0026 behavior**, and isn't meant to be: `batch_max_events: 1_000` and
/// `batch_flush_interval: 100ms` mean a default-configured listener amortizes datagrams into
/// batches (up to 1000 events, or up to 100ms of added latency before a send) rather than sending
/// one batch per datagram immediately, matching what every established UDP listener researched
/// for ADR 0026 does out of the box. A deployment that genuinely needs the old one-send-per-
/// datagram, no-added-latency behavior gets it back explicitly with `batch_max_events: 1`, not by
/// omitting `receive:`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields, default)]
pub struct ReceiveConfig {
    /// Datagrams the read half may hold ahead of the decode half.
    pub max_datagrams: usize,
    /// Byte bound on the queue (undecoded datagram bytes, not `estimated_heap_bytes`), checked
    /// alongside `max_datagrams` -- whichever trips first.
    #[serde(with = "human_bytes")]
    #[schemars(with = "String")]
    pub max_bytes: u64,
    /// What happens once both bounds are full. Defaults to `drop_oldest`, **deliberately unlike
    /// `buffer:`'s `block`**: blocking a sink's producer backpressures an in-process drain that
    /// can wait, while blocking a UDP reader backpressures the kernel, which cannot -- the kernel
    /// just discards the datagram into a counter this process never reads. `block` relocates loss
    /// out of view rather than preventing it; every mature UDP listener (syslog-ng, rsyslog,
    /// Telegraf, gostatsd) treats this the same way.
    pub overflow: OverflowPolicy,
    /// Events to accumulate across datagrams before one send downstream. `1` means one send per
    /// datagram -- the behavior before ADR 0026 -- since the accumulator flushes on a bound
    /// *reached or exceeded* and never splits a single decode's output. `0` is rejected as an
    /// impossible bound (graph rule 18, the twin of rule 15's `buffer.max_batches: 0` check).
    pub batch_max_events: usize,
    #[serde(with = "human_bytes")]
    #[schemars(with = "String")]
    pub batch_max_bytes: u64,
    /// Longest an accumulated batch waits before being sent regardless of size. `0s` disables the
    /// timer entirely (the two bounds above are then the only trigger) -- unlike the count
    /// bounds, zero here is a meaningful setting, not an impossible one, and is not rejected.
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub batch_flush_interval: Duration,
    /// `SO_RCVBUF`, requested at bind. `None` -- the default -- leaves the kernel default alone:
    /// a nonzero default here would exceed most stock kernels' `net.core.rmem_max` (212992 B) and
    /// warn on every first run, training operators to ignore the one warning that matters.
    #[serde(with = "human_bytes::option")]
    #[schemars(with = "Option<String>")]
    pub receive_buffer_bytes: Option<u64>,
    /// How long a listener keeps draining a cooperative shutdown before being cancelled by drop
    /// (`docs/adr/0026-decoupled-listener-io.md`, revising ADR 0013's unconditional cancel-by-drop
    /// into a bounded one). Matches `buffer.shutdown_grace`'s default so both ends of the
    /// pipeline drain on the same number.
    #[serde(with = "humantime_serde_duration")]
    #[schemars(with = "String")]
    pub shutdown_grace: Duration,
}

/// Defaults, justified against established UDP listeners' own tuning figures -- see
/// `docs/adr/0026-decoupled-listener-io.md` for the full numeric derivation (Telegraf, gostatsd,
/// DogStatsD, rsyslog, syslog-ng).
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
        }
    }
}

/// A human-readable byte-size codec (`134217728`, `64MiB`, `128KiB`, `1GiB`) for `BufferConfig::
/// max_bytes`, mirroring `humantime_serde_duration`'s shape below -- hand-rolled rather than a new
/// crate dependency, consistent with that module's own reasoning. Binary (1024-based) units only,
/// matching `max_bytes`'s own doc comment and the way this codebase already sizes buffers
/// (`SinkQueueConfig::default`'s `64 * 1024 * 1024`). Always serializes back out as a quoted
/// decimal-integer string (e.g. `"134217728"`), never a unit suffix and never a bare (unquoted)
/// number -- string-only in both directions, exactly like `humantime_serde_duration` below,
/// matching `#[schemars(with = "String")]`'s published schema exactly; a human can still write
/// `"64MiB"` on the way in.
mod human_bytes {
    use serde::{Deserializer, Serializer};

    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;

    /// String-only, both directions -- deliberately, not just permissively. An earlier version
    /// accepted a bare YAML/JSON integer on input (via `deserialize_any`) while always
    /// serializing as an integer, which contradicted `#[schemars(with = "String")]`'s published
    /// claim in *both* directions at once: the generated schema said `"type": "string"` while a
    /// real config's serialized form was always a bare number, and a schema-strict validator
    /// would separately reject the bare-integer input form `logit` itself accepted -- two
    /// distinct violations of ADR 0003's "the schema can't drift from what the binary accepts"
    /// contract, not one. Consistently string-only (a quoted `"134217728"` or `"64MiB"`, always
    /// serialized the same way) matches the published schema exactly, with no asymmetry to
    /// reason about.
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
            // String-only, deliberately, both directions -- see this module's own doc comment.
            // An earlier version accepted this via `deserialize_any`, which contradicted the
            // published schema's `"type": "string"` claim.
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
            // Quoted, since input is string-only now -- an unquoted `-5` is rejected as the
            // wrong JSON type entirely (see `an_unquoted_negative_number_is_also_rejected`
            // below), not because it's negative specifically. `-` isn't an ASCII digit, so
            // `parse`'s digit-scan sees an empty numeric prefix and reports the same "expected a
            // byte count" shape it would for any other non-numeric-looking string.
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
            // Matches `#[schemars(with = "String")]`'s claim exactly, both directions -- see
            // this module's own doc comment for why an earlier bare-integer form was a real
            // ADR-0003 schema-drift bug, not just a style choice.
            #[derive(serde::Serialize)]
            struct W {
                #[serde(with = "super")]
                bytes: u64,
            }
            let json = serde_json::to_string(&W { bytes: 64 * 1024 * 1024 }).unwrap();
            assert_eq!(json, r#"{"bytes":"67108864"}"#);
        }
    }

    /// The same codec, for `Option<u64>` fields (`#[serde(default, with =
    /// "human_bytes::option")]`) -- used by `ReceiveConfig::receive_buffer_bytes`
    /// (`docs/adr/0026-decoupled-listener-io.md`), where `None` means "leave the kernel default
    /// alone" rather than a byte count of zero. Mirrors `humantime_serde_duration::option`'s
    /// shape exactly: a nested module because `#[serde(with = "...")]` on an `Option<u64>` field
    /// calls *this* module's `serialize`/`deserialize` with `Option<u64>`, not the parent's `u64`
    /// ones. Still string-only in both directions -- `None` serializes as JSON/YAML null, `Some`
    /// as the same quoted string the parent codec produces.
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
/// config keeps human-readable durations without pulling in a full external crate for one helper.
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

    /// The same codec, for `Option<Duration>` fields (`#[serde(default, with =
    /// "humantime_serde_duration::option")]`) -- used by the Lua component kinds' optional
    /// `interval`. A nested module because `#[serde(with = "...")]` on an `Option<Duration>`
    /// field calls *this* module's `serialize`/`deserialize` with `Option<Duration>`, not the
    /// parent's `Duration` ones.
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

/// Generate the published JSON Schema for [`Config`]. Backs the `logit schema` CLI command
/// (ADR 0003) -- CI regenerates `schema/logit.schema.json` from this and fails if it's stale.
pub fn json_schema() -> schemars::schema::RootSchema {
    schemars::schema_for!(Config)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Deserialized via `serde_json` rather than the YAML this crate is actually fed through
    // `logit-cli` (deliberately not a dependency here -- see the crate doc comment): JSON and
    // YAML are both self-describing formats, so this exercises the same tagged-enum
    // disambiguation the real deserializer does.

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
            ComponentKind::Aggregate { interval } => assert_eq!(interval, Duration::from_secs(10)),
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

    /// `span_sample_rate` is optional, defaulting to `logit_core::DEFAULT_SPAN_SAMPLE_RATE`
    /// (0.1) -- an `internal` component that predates this field (every shipped config before
    /// this PR) still deserializes, with spans sampled at a tenth rather than silently disabled
    /// or silently kept at full volume.
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
            ComponentKind::Json { skip_to_brace } => assert!(!skip_to_brace),
            other => panic!("expected Json, got {other:?}"),
        }
    }

    #[test]
    fn json_component_with_skip_to_brace_deserializes() {
        let component: Component =
            serde_json::from_str(r#"{"type": "json", "sources": ["in"], "skip_to_brace": true}"#)
                .unwrap();
        match component.kind {
            ComponentKind::Json { skip_to_brace } => assert!(skip_to_brace),
            other => panic!("expected Json, got {other:?}"),
        }
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
            ComponentKind::OtlpOut { endpoint, protocol } => {
                assert_eq!(endpoint, "http://tempo:4318");
                assert_eq!(protocol, OtlpProtocol::Http);
            }
            other => panic!("expected OtlpOut, got {other:?}"),
        }
    }

    #[test]
    fn otlp_in_with_protocol_grpc_deserializes() {
        let component: Component = serde_json::from_str(
            r#"{"type": "otlp_in", "bind": "0.0.0.0:4317", "protocol": "grpc"}"#,
        )
        .unwrap();
        match component.kind {
            ComponentKind::OtlpIn { bind, protocol } => {
                assert_eq!(bind, "0.0.0.0:4317");
                assert_eq!(protocol, OtlpProtocol::Grpc);
            }
            other => panic!("expected OtlpIn, got {other:?}"),
        }
    }

    #[test]
    fn zero_interval_deserializes_fine_left_for_validation_to_reject() {
        // The codec itself has no opinion on zero -- graph validation (`logit-pipeline`) is where
        // a zero flush interval is actually rejected (it would spin the flush loop).
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
            ComponentKind::StdioOut { target } => assert_eq!(target, StdioTarget::Stdout),
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
        // Same enum as `buffer.overflow` (`OverflowPolicy`) -- `each_overflow_variant_deserializes`
        // above already covers the type itself; this just confirms `receive.overflow` actually
        // wires to it.
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
}
