//! Pure resolution and validation of a [`Config`] into a [`Graph`]: no channels, threads, or
//! tokio, so every rule is unit-testable. `logit run`, `logit validate`, and `logit graph` all
//! build on [`resolve`]'s output.
//!
//! # Validation rules
//!
//! This list is canonical; `docs/design/pipeline-graph.md`'s "Validation" section mirrors it. Each
//! rule is enforced at its `// Rule N` label in [`resolve`], or in the helper its entry names. The
//! numbers are identifiers, not execution order: [`resolve`] runs its checks in source order, which
//! interleaves them (49's first clause runs inside 6 and 50 inside 7; 47, 51, 48, and 49's last
//! clause right after 7; 26-28 before 19; 52 after 44; 53 between 45 and 46). A new rule takes the
//! next number.
//!
//! One principle recurs, cited by number: a config that can only be a no-op, a black hole, or an
//! impossible bound (`0` for a count or duration) is an error, and so is a setting that would be
//! silently ignored.
//!
//! 1. A config with no components.
//! 2. A `sources` id that names no defined component.
//! 3. A component listing itself as a source: a special case of 5, with its own message.
//! 4. A repeated id in one `sources` list: that source's `Fanout` would hold two senders into one
//!    inbox and deliver every batch twice.
//! 5. A cycle, through `sources` or router -> target edges: with bounded channels it deadlocks.
//!    The error names one concrete cycle, never a component merely downstream of it.
//! 6. Wrong arity for the kind's [`Role`]: a listener or `target` with `sources`, a transform or
//!    sink without, or a sink named as another component's source.
//! 7. A non-sink component with no consumer: it would run for nothing (50 exempts routers).
//! 8. A kind `is_implemented` doesn't list.
//! 9. A zero `interval` on a kind that has one (the `interval` fn's table): it would flush
//!    continuously.
//! 10. A `kv_metrics` with `counters`, `gauges`, and `distributions` all empty: a no-op.
//! 11. A `kv_metrics` distribution with no `field`, or any entry with an empty `name`: a
//!     distribution of nothing is meaningless, and `influxdb_out` can't encode a nameless metric
//!     (`docs/adr/kv-metrics-semantics.md`).
//! 12. A `set` with neither `resource` nor `attributes`, or an empty key in either: a no-op, or a
//!     key that could never name a real attribute, so `has_attributes` (36), which shares `set`'s
//!     config shape, never meets a key `set` stamped that its own rule rejects
//!     (`docs/adr/operator-declared-resource-attributes.md`).
//! 13. More than one `internal`: each would drain, and so split, the one process-wide telemetry
//!     registry.
//! 14. A non-default `buffer:` on a non-sink: only a sink has a delivery queue
//!     (`docs/adr/buffered-sink-delivery.md`).
//! 15. A sink's `buffer.max_batches` or `buffer.max_bytes` of `0`: no batch could ever be queued.
//! 16. An `internal` `span_sample_rate` that is non-finite or outside `[0, 1]`: a typo, not a value
//!     to clamp (NaN would keep every span).
//! 17. A non-default `receive:` outside a datagram, stream, or tail listener (explicit predicates,
//!     not [`Role`], so `internal`/`generate_in` are rejected too); on a stream or tail listener,
//!     which has no receive queue, a queue field or `read_batch`, by name.
//! 18. A `receive.max_datagrams`/`max_bytes`/`read_batch` (datagram listeners) or
//!     `batch_max_events`/`batch_max_bytes` (all three drivers) of `0`. `batch_flush_interval: 0s`
//!     is legal ("no flush timer"); 57 owns `read_batch`'s upper end.
//! 19. A `trace_context` with an empty `trace_id`, `span_id`, or `flags` field name: it could never
//!     match an attribute (`null`, not `""`, disables an optional lookup)
//!     (`docs/adr/log-record-trace-context.md`).
//! 20. A `scale` with no `fields`, an empty field name, or a non-finite factor
//!     (`docs/adr/scale-transform.md`).
//! 21. An empty `signals:` on `has_signal`/`keep_signals`/`drop_signals`, or all three signals on
//!     `keep_signals`/`drop_signals`. Which shape is the black hole and which the no-op is
//!     opposite between those two, so each message names the right one
//!     (`docs/adr/signal-filtering-components.md`).
//! 22. An `otlp_out` `headers:` name that is empty, `:`-prefixed, `grpc-*`, in
//!     `RESERVED_OTLP_HEADERS`, or a case-insensitive duplicate: the transport sets those itself,
//!     and which duplicate is sent is undefined.
//! 23. A non-empty `otlp_out` `paths:` under `protocol: grpc`: gRPC method names are fixed by the
//!     OTLP service definitions.
//! 24. An `otlp_out` `tls:` with one of `cert_file`/`key_file` alone, with `insecure_skip_verify`
//!     and `ca_file` together, or under a non-`https://` endpoint, where the scheme selects TLS and
//!     the block would do nothing (`docs/adr/otlp-tls-and-pooled-grpc-client.md`).
//! 25. A `trace_context` `span:` with an empty `name` (OTLP requires one) or a `max_skew` of `0s`
//!     (every span would be skewed) (`docs/adr/trace-context-span-lifting.md`).
//! 26. A `tail_in` with no `paths`, an empty entry, or a `*` outside the final path component,
//!     which the tail matcher never expands (`check_tail_glob`).
//! 27. A `docker_in` with no `containers` and no `discover: true` (it would tail nothing), an empty
//!     `containers`/`labels` entry, a duplicate `containers` entry, or an empty `root`.
//! 28. A `tail_in`/`docker_in` `poll_interval`, `checkpoint_interval`, or `max_line_bytes` of `0`:
//!     a busy loop, a checkpoint write every tick, or every line dropped. 26-28 are
//!     `docs/adr/file-tailing-and-docker-json-logs.md`'s.
//! 29. A `file_out` whose `rotate:` sets neither `max_bytes` nor `interval` (use `stdio_out` for an
//!     unrotated file), a `rotate.max_bytes`/`max_files` of `0`, or a `max_files` above
//!     `logit_config::MAX_ROTATE_FILES` (`docs/adr/rotating-file-output.md`).
//! 30. A `kv` with an empty `pair_sep`/`kv_sep`, `pair_sep == kv_sep`, or a `kv_sep` containing
//!     `pair_sep`: since `pair_sep` splits first, each is a certain no-op or garbage
//!     (`docs/adr/logfmt-and-kv-parsing.md`). `logfmt` needs no rule: its one field is a `bool`.
//! 31. A `regex` with an empty `field`, or a `pattern` that fails to compile or has no named
//!     capture group. Compiling here makes a bad pattern a `logit validate` error
//!     (`docs/adr/regex-transform.md`).
//! 32. A `csv` with no `columns`, an empty column name, a duplicate one (the later would overwrite
//!     the earlier), or a `delimiter` that is `"`, `\n`, `\r`, or non-ASCII
//!     (`docs/adr/csv-positional-columns.md`).
//! 33. A `stdio_out`/`file_out` `compression:` other than `none` outside `format: native`, where it
//!     would do nothing (`docs/adr/file-output-native-format.md`).
//! 34. A `logit_out` `tls:` failing 24's two consistency checks (no scheme check: the endpoint is a
//!     bare `host:port`); a `logit_in` `max_frame_bytes` of `0` or above
//!     `MAX_SANE_UNCOMPRESSED_LEN`, which the frame reader enforces regardless.
//! 35. A `buffer.disk:` alongside a non-default `buffer.max_batches`/`max_bytes` (disk replaces
//!     them), a disk bound of `0` or with `segment_bytes > max_bytes`, or two sinks sharing a
//!     literal `disk.path` (`docs/adr/disk-backed-sink-buffer.md`).
//! 36. A `has_attributes`/`drop_attributes` with nothing configured, an empty key, or a non-finite
//!     value. Zero pairs match vacuously, so `has_*` is the no-op and `drop_*` the black hole, the
//!     inverse of 21 (`docs/adr/attribute-filtering-components.md`).
//! 37. A `has_provenance`/`drop_provenance` with neither `origin` nor `previous`, or an empty or
//!     repeated entry. Oriented like 36, not 21: an empty field is left out of the match
//!     (`docs/adr/provenance-filtering-components.md`).
//! 38. A `statsd_out`/`collectd_out`/`graphite_out` `max_packet_bytes` of `0`, and a `collectd_out`
//!     value outside `1024..=65535`: above it every send fails `EMSGSIZE` while reporting success
//!     (`docs/adr/collectd-binary-relay.md`).
//! 39. A `temporality: cumulative` `aggregate` with `series_retention` or `max_retained_series` of
//!     `0`: nothing survives a flush, so each window's increment would be labeled a running total
//!     (`docs/adr/aggregation-window-semantics.md`).
//! 40. A scrape-mode `prometheus_in` with a target that isn't an absolute `http(s)://` URL
//!     (`is_absolute_http_url`), `timeout: 0s`, a `scrape_tls:` failing 24's checks or with no
//!     `https://` target, or 22's header faults against `RESERVED_PROMETHEUS_HEADERS`.
//! 41. A registry-mode `prometheus_out` `path` not starting with `/` (every scrape would 404) or
//!     `max_series: 0` (every series evicted on arrival)
//!     (`docs/adr/prometheus-scrape-and-exposition.md`).
//! 42. A `generate_in` `count`/`batch`/`rate` of `0`, an empty metric name or attribute/resource
//!     key, a non-finite metric value, or a placeholder other than `{seq}`/`{seq%N}`, with only
//!     `{seq%N}` in the interned metric name (`check_generate_template`).
//! 43. A `tls:` on a UDP `syslog_in`/`graphite_in`/`statsd_in`: DTLS is out of scope, so it could
//!     never take effect. One rule for every listener: a new one adds a match arm
//!     (`docs/adr/syslog-tcp-ingress-and-tls.md`).
//! 44. A `syslog_out` `tls:` failing 24's two consistency checks, or under `transport: udp`, since
//!     RFC 5425 is TLS over TCP (`docs/adr/syslog-tcp-ingress-and-tls.md`).
//! 45. A `handshake_timeout` of `0s` on any kind that has one (either transport), or a non-default
//!     one on a UDP `syslog_in`/`graphite_in`/`statsd_in`, which has no handshake. Compared against
//!     `default_handshake_timeout`, so the default stays legal everywhere
//!     (`docs/adr/syslog-tcp-ingress-and-tls.md`).
//! 46. A `graphite_in`/`graphite_out` `protocol: pickle` off TCP (its length prefix means nothing
//!     in a datagram), a `max_line_bytes`/`max_frame_bytes`/`connect_timeout` of `0`, or a
//!     `max_frame_bytes` outside `GRAPHITE_FRAME_BYTES_RANGE`
//!     (`docs/adr/graphite-carbon-relay.md`).
//! 47. A non-empty `targets:` on anything but `lua`/`lua_file`: a `route`'s targets are its
//!     `routes:` values, and no other kind can direct an event (`docs/adr/target-components.md`).
//! 48. A router target id that is unresolved, the router itself, or not a `target` (the message
//!     names the `sources:` fix), or a repeated `lua` `targets:` id. A `route` may map many values
//!     onto one target.
//! 49. A `target` with `sources` (checked in 6), or one no router directs to: its consumers would
//!     wait forever.
//! 50. Not a rejection: 7 exempts a router, whose unrouted events are dropped and counted
//!     (`logit.component.events.dropped{reason="unrouted"}`).
//! 51. A `route` with no `routes:`, an empty key or value, or an empty `by:` attribute/resource
//!     key. Runs before 48, so an empty value isn't reported as an unknown target.
//! 52. A `statsd_out` `tls:` failing 44's three checks, messages verbatim
//!     (`docs/adr/statsd-output.md`). Sink TLS rules are one per sink (24/34/44/52), since each
//!     also checks its own block.
//! 53. An `idle_timeout` of `0s` (omit it to disable), or any `idle_timeout` on a UDP
//!     `syslog_in`/`graphite_in`/`statsd_in`. It's an `Option`, so there is no default to exempt
//!     (`docs/adr/idle-connection-timeout.md`).
//! 54. A `keep_values` with nothing configured, an empty field name, an empty `allow` (that's
//!     `set`/`remove`), a non-finite literal, a non-lowercase `Str` under `normalize: [lower]`, or
//!     a repeated `normalize` step (`docs/adr/value-allowlist-cardinality-clamp.md`).
//! 55. A `prometheus_in` with both or neither of `scrape_targets`/`bind`, a non-default field of
//!     the other mode, a bind-mode `path` not starting with `/`, or `metadata_cache.ttl: 0s` with
//!     `max_families > 0` (`docs/adr/prometheus-remote-write.md`).
//! 56. A `prometheus_out` with both or neither of `bind`/`endpoint`, a non-default field of the
//!     other mode, `version: 2` with `compression: zstd` (2.0 mandates Snappy), or a sender fault
//!     in 40's shape: a non-absolute `endpoint`, `timeout: 0s`, a reserved or colliding header, or
//!     a bad `endpoint_tls` (`docs/adr/prometheus-remote-write.md`,
//!     `docs/adr/victoriametrics-interop.md`).
//! 57. A datagram listener's `receive.read_batch` above `MAX_READ_BATCH`: the kernel doesn't clamp
//!     `recvmmsg`'s `vlen`, so this bounds the receive slab and the shutdown-path loss
//!     (`docs/adr/udp-intake-batching-and-socket-visibility.md`).
//! 58. A `shape` `max_tracked_keys`/`max_tracked_keysets` of `0`: nothing would be tracked, and
//!     there is no "table off" spelling; remove the component instead
//!     (`docs/adr/shape-observer-component.md`).
//! 59. A `flatten` with `attributes: none` and `resource: none`, an empty named list (write
//!     `none` or `all`), or an empty or repeated field name (`docs/adr/flatten-transform.md`).
//! 60. An `http_access` `match` that is empty or invalid, an empty `route`/`class`/`route_other`/
//!     `redact_query`, a `routes` entry not `builtin` xor `match` + `route`, a repeated `builtin`,
//!     a bad `max_length`, or `forwarded: {trust: false}`
//!     (`docs/adr/http-access-normalization.md`).
//! 61. A `sample` `rate` non-finite, outside `[0, 1]`, `1`, or `0` without `always_keep`; an empty
//!     field name; an `always_keep` naming both or neither side, or with a non-finite value; or
//!     `missing:` without `key:` (`docs/adr/consistent-sampling-component.md`).
//! 62. Two `tail_in`/`docker_in` components sharing a literal `checkpoint_path`, or one whose
//!     `checkpoint_path` is another's `<checkpoint_path>.tmp`: each would overwrite, or truncate
//!     and rename away, the other's offsets (`docs/adr/file-tailing-and-docker-json-logs.md`).
//! 63. A `datadog_in` with an empty `bind`, or an `api_keys` entry that is empty or has leading or
//!     trailing whitespace: it could never match a request's `DD-API-KEY`. Its zero
//!     `handshake_timeout`/`idle_timeout` are rules 45/53's
//!     (`docs/adr/datadog-agent-and-intake-relay.md`).
//! 64. A `datadog_trace_in` with neither `bind` nor `socket`, an empty `bind`, a `socket` that
//!     isn't an absolute path, or `tls` without `bind`: the Unix socket has no TLS. Its zero
//!     `handshake_timeout`/`idle_timeout` are rules 45/53's
//!     (`docs/adr/datadog-agent-and-intake-relay.md`).
//!
//! Not validated: that a `by: {provenance: ..}` route key names a component in this graph. Like
//! 37's ids, it may name a component relayed from another process. Nor is `keep`'s empty `fields`:
//! "drop every attribute" is a real operation, unlike 21's "drop every event".
//!
//! Sink reachability needs no rule: by 2 + 5 + 7, every acyclic chain ends, and only at a sink.

use logit_config::{
    default_handshake_timeout, default_prometheus_scrape_interval,
    default_prometheus_scrape_timeout, default_prometheus_write_path, BufferConfig, Component,
    ComponentKind, Compression, Config, GraphiteProtocol, GraphiteTransport, MetadataCacheConfig,
    ReceiveConfig, StatsdTransport, StreamFormat, SyslogTransport, MAX_READ_BATCH,
};
use logit_proto::frame::MAX_SANE_UNCOMPRESSED_LEN;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::time::Duration;

/// Rule 46's bound on a `graphite_in`/`graphite_out` `max_frame_bytes`. Below 1024 no real carbon
/// pickle batch fits; above 16 MiB a frame's *declared* length is a larger allocation than any
/// sender has reason to ask for. The default (`logit_proto::graphite::DEFAULT_MAX_FRAME_BYTES`,
/// Twisted's `MAX_LENGTH`, 1 MiB) sits inside. One const, so both kinds check the same range.
const GRAPHITE_FRAME_BYTES_RANGE: std::ops::RangeInclusive<u64> = 1024..=16 * 1024 * 1024;

/// A component's arity class, fixed by its `kind` (`docs/design/pipeline-graph.md`'s arity
/// table) -- never derived from topology, so a typo'd source reference can't silently reclassify
/// a component instead of producing a clear error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Listener,
    Transform,
    Sink,
    /// A `target`: no sources, fed by direction from a router (`docs/adr/target-components.md`).
    Target,
}

impl Role {
    /// A stable, lowercase name for this role, stamped on `logit.component.*` telemetry points
    /// (`docs/design/internal-telemetry.md`).
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Listener => "listener",
            Role::Transform => "transform",
            Role::Sink => "sink",
            Role::Target => "target",
        }
    }
}

/// The arity class a kind belongs to. Public so `logit graph` (`logit-cli`) can style nodes
/// straight off a `Config`, and so render even a config that fails validation
/// (`docs/design/pipeline-graph.md`'s "`logit graph`" section).
pub fn role(kind: &ComponentKind) -> Role {
    use ComponentKind::*;
    match kind {
        StatsdIn { .. }
        | CollectdIn { .. }
        | GraphiteIn { .. }
        | SyslogIn { .. }
        | OtlpIn { .. }
        | DatadogIn { .. }
        | DatadogTraceIn { .. }
        | TailIn { .. }
        | DockerIn { .. }
        | LogitIn { .. }
        | Internal { .. }
        | PrometheusIn { .. }
        | GenerateIn { .. } => Role::Listener,
        Lua { .. }
        | LuaFile { .. }
        | Aggregate { .. }
        | Json { .. }
        | Csv { .. }
        | KvMetrics { .. }
        | Keep { .. }
        | Remove { .. }
        | Set { .. }
        | TraceContext { .. }
        | Scale { .. }
        | HasSignal { .. }
        | KeepSignals { .. }
        | DropSignals { .. }
        | HasAttributes { .. }
        | DropAttributes { .. }
        | HasProvenance { .. }
        | DropProvenance { .. }
        | KeepValues { .. }
        | Logfmt { .. }
        | Kv { .. }
        | Regex { .. }
        | Shape { .. }
        | Flatten { .. }
        | HttpAccess { .. }
        | Sample { .. }
        | Route { .. } => Role::Transform,
        InfluxDbOut { .. }
        | OtlpOut { .. }
        | LogitOut { .. }
        | StdioOut { .. }
        | FileOut { .. }
        | SyslogOut { .. }
        | StatsdOut { .. }
        | CollectdOut { .. }
        | GraphiteOut { .. }
        | PrometheusOut { .. }
        | NullOut { .. } => Role::Sink,
        Target { .. } => Role::Target,
    }
}

/// The config `type` tag this kind deserializes from, stamped on `logit.component.*` telemetry
/// points (`docs/design/internal-telemetry.md`). Hand-written rather than derived from `Serialize`,
/// which would round-trip a whole `Component`; like [`role`] and `is_implemented`, it needs an arm
/// for every new `ComponentKind` variant.
pub fn kind_name(kind: &ComponentKind) -> &'static str {
    use ComponentKind::*;
    match kind {
        StatsdIn { .. } => "statsd_in",
        CollectdIn { .. } => "collectd_in",
        GraphiteIn { .. } => "graphite_in",
        SyslogIn { .. } => "syslog_in",
        OtlpIn { .. } => "otlp_in",
        DatadogIn { .. } => "datadog_in",
        DatadogTraceIn { .. } => "datadog_trace_in",
        TailIn { .. } => "tail_in",
        DockerIn { .. } => "docker_in",
        LogitIn { .. } => "logit_in",
        Internal { .. } => "internal",
        PrometheusIn { .. } => "prometheus_in",
        GenerateIn { .. } => "generate_in",
        Lua { .. } => "lua",
        LuaFile { .. } => "lua_file",
        Aggregate { .. } => "aggregate",
        Json { .. } => "json",
        Csv { .. } => "csv",
        KvMetrics { .. } => "kv_metrics",
        Keep { .. } => "keep",
        Remove { .. } => "remove",
        Set { .. } => "set",
        TraceContext { .. } => "trace_context",
        Scale { .. } => "scale",
        HasSignal { .. } => "has_signal",
        KeepSignals { .. } => "keep_signals",
        DropSignals { .. } => "drop_signals",
        HasAttributes { .. } => "has_attributes",
        DropAttributes { .. } => "drop_attributes",
        HasProvenance { .. } => "has_provenance",
        DropProvenance { .. } => "drop_provenance",
        KeepValues { .. } => "keep_values",
        Logfmt { .. } => "logfmt",
        Kv { .. } => "kv",
        Regex { .. } => "regex",
        Shape { .. } => "shape",
        Flatten { .. } => "flatten",
        HttpAccess { .. } => "http_access",
        Sample { .. } => "sample",
        Route { .. } => "route",
        InfluxDbOut { .. } => "influxdb_out",
        OtlpOut { .. } => "otlp_out",
        LogitOut { .. } => "logit_out",
        StdioOut { .. } => "stdio_out",
        FileOut { .. } => "file_out",
        SyslogOut { .. } => "syslog_out",
        StatsdOut { .. } => "statsd_out",
        CollectdOut { .. } => "collectd_out",
        GraphiteOut { .. } => "graphite_out",
        PrometheusOut { .. } => "prometheus_out",
        NullOut { .. } => "null_out",
        Target { .. } => "target",
    }
}

/// Every router -> target edge one component declares, with the route key that produced it (`None`
/// for a `lua`/`lua_file` `targets:` entry, whose key is the script's `event:to("..")`). Keeps
/// duplicates: this is the *edge* list, where [`targets_of`] is the slot list, so a `route` mapping
/// two values onto one target appears here twice and there once. Any other kind has no edges; rule
/// 47 rejects `targets:` on one.
///
/// Public for the same reason as [`role`]: `logit graph` (`logit-cli`'s `dot.rs`) renders these
/// edges off a raw `Config`.
pub fn target_edges(component: &Component) -> Vec<(Option<&str>, &str)> {
    match &component.kind {
        ComponentKind::Route { routes, .. } => {
            routes.iter().map(|(key, target)| (Some(key.as_str()), target.as_str())).collect()
        }
        ComponentKind::Lua { .. } | ComponentKind::LuaFile { .. } => {
            component.targets.iter().map(|target| (None, target.as_str())).collect()
        }
        _ => Vec::new(),
    }
}

/// The slot-ordered, de-duplicated `target` ids one component directs events into: a `route`'s
/// `routes:` values in key order (first occurrence wins), a `lua`/`lua_file`'s `targets:` as
/// written, nothing for any other kind. This order is what a router's slot index means downstream
/// (the node runtime's `Vec<Fanout>`, a Lua worker's `event:to("..")` table), so it is derived here
/// once (`docs/adr/target-components.md`).
///
/// A `route` ignores `Component.targets`; rule 47 rejects one written on it.
///
/// Public for the same reason as [`target_edges`].
pub fn targets_of(component: &Component) -> Vec<&str> {
    let mut slots: Vec<&str> = Vec::new();
    for (_, target) in target_edges(component) {
        if !slots.contains(&target) {
            slots.push(target);
        }
    }
    slots
}

/// Rule 8's list: the `ComponentKind`s the runtime can build.
fn is_implemented(kind: &ComponentKind) -> bool {
    matches!(
        kind,
        ComponentKind::StatsdIn { .. }
            | ComponentKind::CollectdIn { .. }
            | ComponentKind::GraphiteIn { .. }
            | ComponentKind::SyslogIn { .. }
            | ComponentKind::OtlpIn { .. }
            | ComponentKind::DatadogIn { .. }
            | ComponentKind::DatadogTraceIn { .. }
            | ComponentKind::TailIn { .. }
            | ComponentKind::DockerIn { .. }
            | ComponentKind::Internal { .. }
            | ComponentKind::PrometheusIn { .. }
            | ComponentKind::Lua { .. }
            | ComponentKind::LuaFile { .. }
            | ComponentKind::Aggregate { .. }
            | ComponentKind::Json { .. }
            | ComponentKind::Csv { .. }
            | ComponentKind::KvMetrics { .. }
            | ComponentKind::Keep { .. }
            | ComponentKind::Remove { .. }
            | ComponentKind::Set { .. }
            | ComponentKind::TraceContext { .. }
            | ComponentKind::Scale { .. }
            | ComponentKind::HasSignal { .. }
            | ComponentKind::KeepSignals { .. }
            | ComponentKind::DropSignals { .. }
            | ComponentKind::HasAttributes { .. }
            | ComponentKind::DropAttributes { .. }
            | ComponentKind::HasProvenance { .. }
            | ComponentKind::DropProvenance { .. }
            | ComponentKind::KeepValues { .. }
            | ComponentKind::Logfmt { .. }
            | ComponentKind::Kv { .. }
            | ComponentKind::Regex { .. }
            | ComponentKind::Shape { .. }
            | ComponentKind::Flatten { .. }
            | ComponentKind::HttpAccess { .. }
            | ComponentKind::Sample { .. }
            | ComponentKind::InfluxDbOut { .. }
            | ComponentKind::OtlpOut { .. }
            | ComponentKind::StdioOut { .. }
            | ComponentKind::FileOut { .. }
            | ComponentKind::SyslogOut { .. }
            | ComponentKind::LogitIn { .. }
            | ComponentKind::LogitOut { .. }
            | ComponentKind::StatsdOut { .. }
            | ComponentKind::CollectdOut { .. }
            | ComponentKind::GraphiteOut { .. }
            | ComponentKind::PrometheusOut { .. }
            | ComponentKind::GenerateIn { .. }
            | ComponentKind::NullOut { .. }
            | ComponentKind::Target { .. }
            | ComponentKind::Route { .. }
    )
}

/// Rule 9's table: `Some` for a kind with an `interval` field, set or defaulted; `None` for a kind
/// without one, or a `lua`/`lua_file` that left it unset. Neither `None` case ever flushes, so rule
/// 9 has nothing to reject.
fn interval(kind: &ComponentKind) -> Option<Duration> {
    match kind {
        ComponentKind::Lua { interval, .. } | ComponentKind::LuaFile { interval, .. } => *interval,
        ComponentKind::Aggregate { interval, .. }
        | ComponentKind::Internal { interval, .. }
        | ComponentKind::Shape { interval, .. }
        | ComponentKind::PrometheusIn { interval, .. } => Some(*interval),
        _ => None,
    }
}

/// `true` if `signals` names all three of `Logs`/`Metrics`/`Traces`: rule 21's test for
/// `keep_signals`/`drop_signals`.
fn names_all_three(signals: &[logit_config::Signal]) -> bool {
    signals.contains(&logit_config::Signal::Logs)
        && signals.contains(&logit_config::Signal::Metrics)
        && signals.contains(&logit_config::Signal::Traces)
}

pub struct ResolvedComponent {
    pub sources: Vec<String>,
    pub consumers: Vec<String>,
    /// The `target` components this one directs events into, in slot order ([`targets_of`], owned).
    /// Empty for a non-router; once rules 47-49 pass, every id names a defined `target`, once.
    pub targets: Vec<String>,
    pub kind: ComponentKind,
    /// Per-sink delivery buffer config (`docs/adr/buffered-sink-delivery.md`). Rule 14 guarantees
    /// it is [`BufferConfig::default`] on any non-sink.
    pub buffer: BufferConfig,
    /// Per-listener receive config (`docs/adr/decoupled-listener-io.md`). Rule 17 guarantees it is
    /// [`ReceiveConfig::default`] on anything but a datagram, stream, or tail listener.
    pub receive: ReceiveConfig,
}

impl ResolvedComponent {
    pub fn role(&self) -> Role {
        role(&self.kind)
    }

    pub fn kind_name(&self) -> &'static str {
        kind_name(&self.kind)
    }
}

pub struct Graph {
    pub components: HashMap<String, ResolvedComponent>,
    /// Listener-first order, a byproduct of rule 5's cycle check. Only tests read it: the node
    /// runtime creates every inbox up front, and `logit graph` renders from the raw `Config`.
    pub topological_order: Vec<String>,
}

/// Non-gRPC header names `otlp_out`'s HTTP transport sets itself, or that HTTP/1.1 connection
/// management reserves (`crates/logit-outputs/src/otlp.rs`'s `send_http`); rule 22 compares them
/// case-insensitively. Every `grpc-*` name is reserved by prefix in `resolve` instead: gRPC defines
/// a whole namespace (`grpc-encoding`, `grpc-trace-bin`, ...), and a fixed list would miss one the
/// transport starts setting later.
const RESERVED_OTLP_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "content-encoding",
    "host",
    "te",
    "transfer-encoding",
    "connection",
];

/// Header names `prometheus_in`'s scrape client sets itself (rule 40): `Accept` (dialect
/// negotiation) and `User-Agent` (`crates/logit-inputs/src/prometheus.rs`'s `scrape_target`), plus
/// [`RESERVED_OTLP_HEADERS`]'s connection-management names. A scrape sends no body, so
/// `content-type`/`content-encoding` are listed only to keep the two lists parallel.
const RESERVED_PROMETHEUS_HEADERS: &[&str] = &[
    "accept",
    "user-agent",
    "content-type",
    "content-length",
    "content-encoding",
    "host",
    "te",
    "transfer-encoding",
    "connection",
];

/// Header names `prometheus_out`'s remote-write sender sets itself (rule 56): the four protocol
/// headers `RemoteWriteOutput::send` inserts over the operator's map
/// (`crates/logit-outputs/src/prometheus.rs`), plus `content-length`, which `reqwest` sets
/// (`docs/adr/prometheus-remote-write.md`'s "Sender behaviour"). No connection-management names:
/// unlike the other two lists' users, this sender neither hand-frames a request nor negotiates a
/// dialect. Compared case-insensitively.
const RESERVED_REMOTE_WRITE_HEADERS: &[&str] = &[
    "content-type",
    "content-encoding",
    "content-length",
    "x-prometheus-remote-write-version",
    "user-agent",
];

/// Rules 40 and 56's URL check: an absolute `http://`/`https://` URL with a non-empty authority.
/// Hand-rolled because this crate doesn't depend on `reqwest`/`url`
/// (`docs/design/pipeline-graph.md`'s "Crate layout"), so it catches a typo'd scheme or a bare
/// `host:port` but not everything `reqwest::Url::parse` rejects (a `999.999.999.999` host, an
/// unbalanced IPv6 `[`, a port past `u16::MAX`). `crates/logit-inputs/src/prometheus.rs` does the
/// real parse, and keys an unparseable target's placeholder `Resource` by its configured index so
/// two never collide.
fn is_absolute_http_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    let Some(rest) = lower.strip_prefix("http://").or_else(|| lower.strip_prefix("https://"))
    else {
        return false;
    };
    !rest.split(['/', '?', '#']).next().unwrap_or("").is_empty()
}

pub fn resolve(config: Config) -> anyhow::Result<Graph> {
    let Config { components, .. } = config;

    // Rule 1: at least one component.
    if components.is_empty() {
        anyhow::bail!("config defines no components");
    }

    // Rules 2 + 3 + 4: every source resolves, no self-reference, no repeated source. A repeated
    // source would push the same consumer into `consumers` twice below, so its `Fanout` would
    // deliver every batch twice.
    for (id, component) in &components {
        let mut seen = std::collections::HashSet::with_capacity(component.sources.len());
        for source in &component.sources {
            if source == id {
                anyhow::bail!("component '{id}' lists itself as a source");
            }
            if !components.contains_key(source) {
                anyhow::bail!("component '{id}' references unknown source '{source}'");
            }
            if !seen.insert(source) {
                anyhow::bail!("component '{id}' lists source '{source}' more than once");
            }
        }
    }

    // Invert `sources` into each component's outbound consumer list.
    let mut consumers: HashMap<String, Vec<String>> =
        components.keys().map(|id| (id.clone(), Vec::new())).collect();
    for (id, component) in &components {
        for source in &component.sources {
            consumers.get_mut(source).expect("validated above").push(id.clone());
        }
    }

    // Rule 5: no cycles. The Kahn pass also yields the order `Graph::topological_order` publishes.
    let topological_order = topological_order(&components)?;

    // Rule 6: arity per kind.
    for (id, component) in &components {
        match role(&component.kind) {
            Role::Listener if !component.sources.is_empty() => {
                anyhow::bail!("component '{id}' is a listener and cannot declare sources");
            }
            Role::Transform if component.sources.is_empty() => {
                anyhow::bail!("component '{id}' is a transform and requires at least one source");
            }
            // Rule 49's first clause: a `target` is fed by a router that names it, so it names
            // nothing itself.
            Role::Target if !component.sources.is_empty() => {
                anyhow::bail!(
                    "component '{id}' is a target and cannot declare sources -- a target is fed \
                     by a router that names it"
                );
            }
            Role::Sink => {
                if component.sources.is_empty() {
                    anyhow::bail!("component '{id}' is a sink and requires at least one source");
                }
                if !consumers.get(id).is_some_and(Vec::is_empty) {
                    anyhow::bail!(
                        "component '{id}' is a sink and cannot be listed as a source of another \
                         component"
                    );
                }
            }
            _ => {}
        }
    }

    // Rule 7: every non-sink component, `target`s included, needs at least one consumer.
    //
    // Rule 50: a router (non-empty `targets_of`) is exempt. Its consumers get only its unrouted
    // events, and a router with none drops and counts those
    // (`logit.component.events.dropped{reason="unrouted"}`), so it isn't a silent black hole.
    for (id, component) in &components {
        if role(&component.kind) != Role::Sink
            && consumers.get(id).is_none_or(Vec::is_empty)
            && targets_of(component).is_empty()
        {
            anyhow::bail!("component '{id}' has no consumers -- nothing reads what it produces");
        }
    }

    // Rule 47: `targets:` is `lua`/`lua_file`-only, rule 14's shape. A `route`'s targets are its
    // `routes:` values, and no other kind can direct an event at all.
    for (id, component) in &components {
        if !component.targets.is_empty()
            && !matches!(component.kind, ComponentKind::Lua { .. } | ComponentKind::LuaFile { .. })
        {
            anyhow::bail!(
                "component '{id}': 'targets' is only meaningful on a lua/lua_file component (a \
                 route's targets are its routes: values)"
            );
        }
    }

    // Rule 51: `route`'s own shape. Runs before rule 48, which reads the same `routes:` values as
    // target ids, so an empty value is reported as empty rather than as an unknown target `''`.
    for (id, component) in &components {
        if let ComponentKind::Route { by, routes } = &component.kind {
            if routes.is_empty() {
                anyhow::bail!(
                    "component '{id}': a route with no 'routes' configured can only ever be a \
                     no-op"
                );
            }
            if routes.keys().any(|value| value.is_empty()) {
                anyhow::bail!(
                    "component '{id}': a route 'routes' key must not be empty -- it could never \
                     match a real value"
                );
            }
            if routes.values().any(|target| target.is_empty()) {
                anyhow::bail!(
                    "component '{id}': a route 'routes' value must not be empty -- it could \
                     never name a real target"
                );
            }
            let by_key = match by {
                logit_config::RouteBy::Attribute(key) => Some(("attribute", key)),
                logit_config::RouteBy::Resource(key) => Some(("resource", key)),
                logit_config::RouteBy::Provenance(_) => None,
            };
            if let Some((field, key)) = by_key {
                if key.is_empty() {
                    anyhow::bail!(
                        "component '{id}': a route 'by: {{{field}: ..}}' key name must not be \
                         empty -- it could never name a real {field} key"
                    );
                }
            }
        }
    }

    // Rule 48: every router -> target reference resolves, isn't the router itself, and names a
    // `target`; a `lua`/`lua_file` may not repeat one. Unresolved is checked first, so a typo is
    // reported as a typo. Directing at an ordinary component is a `sources:` entry on the wrong
    // side of the edge (`docs/adr/component-graph-configuration.md`'s "named outlets"), so the
    // message names that fix. Only keyless edges are checked for repeats: a `route` mapping several
    // values onto one target is the many-to-one the kind exists for, and `targets_of` collapses it
    // to one slot.
    for (id, component) in &components {
        let mut seen = std::collections::HashSet::new();
        for (key, target) in target_edges(component) {
            let Some(referenced) = components.get(target) else {
                anyhow::bail!("component '{id}' references unknown target '{target}'");
            };
            if target == id.as_str() {
                anyhow::bail!("component '{id}' directs at itself as a target");
            }
            if role(&referenced.kind) != Role::Target {
                anyhow::bail!(
                    "component '{id}': target '{target}' is a {}, not a target -- a router may \
                     only direct at a 'target' kind; did you mean to list '{id}' in the \
                     'sources' of '{target}'?",
                    kind_name(&referenced.kind)
                );
            }
            if key.is_none() && !seen.insert(target) {
                anyhow::bail!(
                    "component '{id}' lists target '{target}' more than once -- two fanouts into \
                     the same target would deliver every routed batch to it twice"
                );
            }
        }
    }

    // Rule 49's last clause: a `target` no router directs to would leave its consumers waiting
    // forever.
    let directed_to: BTreeSet<&str> = components.values().flat_map(targets_of).collect();
    for (id, component) in &components {
        if role(&component.kind) == Role::Target && !directed_to.contains(id.as_str()) {
            anyhow::bail!(
                "component '{id}': is a target that no router directs to -- its consumers would \
                 wait on it forever"
            );
        }
    }

    // Rule 8: kind implemented.
    for (id, component) in &components {
        if !is_implemented(&component.kind) {
            anyhow::bail!("component '{id}': kind {:?} is not implemented yet", component.kind);
        }
    }

    // Rule 9: no zero-length flush interval.
    for (id, component) in &components {
        if interval(&component.kind) == Some(Duration::ZERO) {
            anyhow::bail!(
                "component '{id}': a flush interval of 0s would flush continuously -- use a \
                 positive duration"
            );
        }
    }

    // Rule 10: a `kv_metrics` with nothing configured is a no-op.
    for (id, component) in &components {
        if let ComponentKind::KvMetrics { counters, gauges, distributions } = &component.kind {
            if counters.is_empty() && gauges.is_empty() && distributions.is_empty() {
                anyhow::bail!(
                    "component '{id}': a kv_metrics with no counters, gauges, or distributions \
                     configured can only ever be a no-op"
                );
            }
            // Rule 11: a distribution needs a `field`, and every entry a `name` (`influxdb_out`
            // can't encode a metric with no measurement name).
            if distributions.iter().any(|m| m.field.is_none()) {
                anyhow::bail!(
                    "component '{id}': a kv_metrics distribution entry requires a 'field' -- a \
                     distribution of nothing is meaningless"
                );
            }
            if counters.iter().chain(gauges).chain(distributions).any(|m| m.name.is_empty()) {
                anyhow::bail!(
                    "component '{id}': a kv_metrics counter, gauge, or distribution entry \
                     requires a non-empty 'name' -- influxdb_out cannot encode a metric with no \
                     measurement name"
                );
            }
        }
    }

    // Rule 12: a `set` with nothing configured is a no-op, and an empty key could never name a real
    // attribute. Rejecting it here means `has_attributes` (rule 36), which shares `set`'s config
    // shape and rejects an empty key itself, never meets one `set` stamped.
    for (id, component) in &components {
        if let ComponentKind::Set { resource, attributes } = &component.kind {
            if resource.is_empty() && attributes.is_empty() {
                anyhow::bail!(
                    "component '{id}': a set with neither 'resource' nor 'attributes' \
                     configured can only ever be a no-op"
                );
            }
            if resource.keys().chain(attributes.keys()).any(|key| key.is_empty()) {
                anyhow::bail!(
                    "component '{id}': a set key must not be empty -- it could never name a \
                     real attribute"
                );
            }
        }
    }

    // Rule 13: at most one `internal` component.
    let internal_ids: Vec<&String> = components
        .iter()
        .filter(|(_, c)| matches!(c.kind, ComponentKind::Internal { .. }))
        .map(|(id, _)| id)
        .collect();
    if internal_ids.len() > 1 {
        let mut ids: Vec<&str> = internal_ids.iter().map(|s| s.as_str()).collect();
        ids.sort_unstable();
        anyhow::bail!(
            "config defines more than one 'internal' component ({}) -- each would drain (and so \
             split) the same process-wide telemetry",
            ids.join(", ")
        );
    }

    // Rule 14: `buffer:` is sink-only; a non-default value elsewhere is a misplaced block.
    for (id, component) in &components {
        if component.buffer != BufferConfig::default() && role(&component.kind) != Role::Sink {
            anyhow::bail!(
                "component '{id}': 'buffer' is only meaningful on a sink, but '{id}' is a {}",
                role(&component.kind).as_str()
            );
        }
    }

    // Rule 15: `max_batches: 0` or `max_bytes: 0` makes every push overflow, even into an empty
    // queue. `SinkQueue::push` tolerates that at runtime rather than hanging, but a sink that can
    // never queue a batch is a config mistake.
    for (id, component) in &components {
        if role(&component.kind) == Role::Sink {
            if component.buffer.max_batches == 0 {
                anyhow::bail!(
                    "component '{id}': 'buffer.max_batches' must be at least 1 -- 0 means no \
                     batch can ever be queued"
                );
            }
            if component.buffer.max_bytes == 0 {
                anyhow::bail!(
                    "component '{id}': 'buffer.max_bytes' must be at least 1 -- 0 means no batch \
                     can ever be queued"
                );
            }
        }
    }

    // Rule 16: `internal`'s `span_sample_rate` must be finite and within `[0, 1]`.
    // `trace_is_sampled` (`crates/logit-core/src/telemetry.rs`) treats NaN as "keep everything",
    // which a typo shouldn't get, and no value outside `[0, 1]` has a sensible reading.
    for (id, component) in &components {
        if let ComponentKind::Internal { span_sample_rate, .. } = &component.kind {
            if !span_sample_rate.is_finite() {
                anyhow::bail!(
                    "component '{id}': 'span_sample_rate' must be a finite number, got {span_sample_rate}"
                );
            }
            if !(0.0..=1.0).contains(span_sample_rate) {
                anyhow::bail!(
                    "component '{id}': 'span_sample_rate' must be between 0.0 and 1.0, got {span_sample_rate}"
                );
            }
        }
    }

    // Rule 17: `receive:` belongs to a datagram, stream, or tail listener only, tested by those
    // predicates rather than `Role::Listener` so `internal`/`generate_in` are rejected. A stream or
    // tail listener has no receive queue, so the queue fields and `read_batch` are rejected there
    // by name, and the error says which field doesn't apply.
    for (id, component) in &components {
        if component.receive == ReceiveConfig::default() {
            continue;
        }
        if is_datagram_listener(&component.kind) {
            continue;
        }
        if is_tail_listener(&component.kind) || is_stream_listener(&component.kind) {
            let default = ReceiveConfig::default();
            let queue_only_field = if component.receive.max_datagrams != default.max_datagrams {
                Some("max_datagrams")
            } else if component.receive.max_bytes != default.max_bytes {
                Some("max_bytes")
            } else if component.receive.overflow != default.overflow {
                Some("overflow")
            } else if component.receive.receive_buffer_bytes != default.receive_buffer_bytes {
                Some("receive_buffer_bytes")
            } else if component.receive.read_batch != default.read_batch {
                Some("read_batch")
            } else {
                None
            };
            if let Some(field) = queue_only_field {
                // The reason is the actionable half, and it differs: a tail listener's buffer is
                // the file, a stream listener's is the peer's send window.
                if is_stream_listener(&component.kind) {
                    anyhow::bail!(
                        "component '{id}': 'receive.{field}' is only meaningful on a datagram \
                         listener (collectd_in, or a UDP statsd_in, syslog_in or graphite_in) -- \
                         a stream listener has no receive queue; the connection's own flow \
                         control is the backpressure. Only receive.batch_max_events, \
                         batch_max_bytes, batch_flush_interval, and shutdown_grace apply (per \
                         connection)"
                    );
                }
                anyhow::bail!(
                    "component '{id}': 'receive.{field}' is only meaningful on a datagram \
                     listener (collectd_in, or a UDP statsd_in, syslog_in or graphite_in) -- a \
                     tail listener has no receive queue; \
                     only receive.batch_max_events, batch_max_bytes, batch_flush_interval, and \
                     shutdown_grace apply"
                );
            }
            continue;
        }
        anyhow::bail!(
            "component '{id}': 'receive' is only meaningful on a datagram, stream or tail \
             listener (statsd_in, collectd_in, syslog_in, graphite_in, tail_in, docker_in), but \
             '{id}' is a {}",
            role(&component.kind).as_str()
        );
    }

    // Rule 18: rule 15's twin for receive-side batch assembly. `batch_flush_interval: 0s` isn't
    // checked: it means "no timer". The queue bounds are datagram-only (rule 17).
    for (id, component) in &components {
        if is_datagram_listener(&component.kind) {
            if component.receive.max_datagrams == 0 {
                anyhow::bail!(
                    "component '{id}': 'receive.max_datagrams' must be at least 1 -- 0 means no \
                     datagram can ever be queued"
                );
            }
            if component.receive.max_bytes == 0 {
                anyhow::bail!(
                    "component '{id}': 'receive.max_bytes' must be at least 1 -- 0 means no \
                     datagram can ever be queued"
                );
            }
            if component.receive.read_batch == 0 {
                anyhow::bail!(
                    "component '{id}': 'receive.read_batch' must be at least 1 -- 0 means no \
                     datagram could ever be read off the socket"
                );
            }
        }
        if is_datagram_listener(&component.kind)
            || is_tail_listener(&component.kind)
            || is_stream_listener(&component.kind)
        {
            if component.receive.batch_max_events == 0 {
                anyhow::bail!(
                    "component '{id}': 'receive.batch_max_events' must be at least 1 -- 0 means \
                     no datagram could ever be accumulated"
                );
            }
            if component.receive.batch_max_bytes == 0 {
                anyhow::bail!(
                    "component '{id}': 'receive.batch_max_bytes' must be at least 1 -- 0 means no \
                     datagram could ever be accumulated"
                );
            }
        }
    }

    // Rule 26: `tail_in`'s `paths`: at least one, none empty, and `*` only in the final path
    // component (`check_tail_glob`).
    for (id, component) in &components {
        if let ComponentKind::TailIn { paths, .. } = &component.kind {
            if paths.is_empty() {
                anyhow::bail!("component '{id}': 'paths' must name at least one file");
            }
            for path in paths {
                if path.is_empty() {
                    anyhow::bail!("component '{id}': 'paths' has an empty entry");
                }
                check_tail_glob(id, path)?;
            }
        }
    }

    // Rule 27: `docker_in`'s shape. No `containers` and no `discover` would tail nothing (rule 7's
    // black hole, invisible to arity). An empty `containers`/`labels` entry can never match, a
    // repeated `containers` entry is a mistake, and an empty `root` would resolve to the working
    // directory.
    for (id, component) in &components {
        if let ComponentKind::DockerIn { root, containers, discover, labels, .. } = &component.kind
        {
            if containers.is_empty() && !discover {
                anyhow::bail!(
                    "component '{id}': 'containers' must name at least one container, or \
                     'discover: true' must be set -- otherwise this listener would tail nothing"
                );
            }
            if root.is_empty() {
                anyhow::bail!("component '{id}': 'root' must not be empty");
            }
            let mut seen = std::collections::HashSet::new();
            for name in containers {
                if name.is_empty() {
                    anyhow::bail!("component '{id}': 'containers' has an empty entry");
                }
                if !seen.insert(name.as_str()) {
                    anyhow::bail!("component '{id}': 'containers' has a duplicate entry '{name}'");
                }
            }
            for key in labels {
                if key.is_empty() {
                    anyhow::bail!("component '{id}': 'labels' has an empty entry");
                }
            }
        }
    }

    // Rule 28: a tail listener's knobs must be positive: `0` would busy-loop (`poll_interval`),
    // checkpoint every tick (`checkpoint_interval`), or drop every line (`max_line_bytes`).
    for (id, component) in &components {
        let tail_options = match &component.kind {
            ComponentKind::TailIn { tail, .. } => Some(tail),
            ComponentKind::DockerIn { tail, .. } => Some(tail),
            _ => None,
        };
        if let Some(tail) = tail_options {
            if tail.poll_interval.is_zero() {
                anyhow::bail!(
                    "component '{id}': 'poll_interval' must be greater than 0s -- 0 would \
                     busy-loop"
                );
            }
            if tail.checkpoint_interval.is_zero() {
                anyhow::bail!(
                    "component '{id}': 'checkpoint_interval' must be greater than 0s -- 0 would \
                     write the checkpoint on every tick"
                );
            }
            if tail.max_line_bytes == 0 {
                anyhow::bail!(
                    "component '{id}': 'max_line_bytes' must be greater than 0 -- 0 would drop \
                     every line"
                );
            }
        }
    }

    // Rule 19: an empty `trace_context` field name could never name an attribute. `span_id`/`flags`
    // are disabled with `null`; `""` there is a typo, not an opt-out.
    for (id, component) in &components {
        if let ComponentKind::TraceContext { trace_id, span_id, flags, .. } = &component.kind {
            if trace_id.is_empty() {
                anyhow::bail!(
                    "component '{id}': a trace_context with an empty 'trace_id' field name can \
                     only ever be a no-op"
                );
            }
            for (field, value) in [("span_id", span_id), ("flags", flags)] {
                if value.as_deref() == Some("") {
                    anyhow::bail!(
                        "component '{id}': a trace_context with an empty '{field}' field name \
                         could never match an attribute -- use null to disable the lookup"
                    );
                }
            }
        }
    }

    // Rule 20: `scale`. No `fields` is a no-op, an empty field name can't match (rule 19's
    // reasoning), and a non-finite factor only produces values `numeric`
    // (`crates/logit-transforms/src/lib.rs`) rejects downstream.
    for (id, component) in &components {
        if let ComponentKind::Scale { fields } = &component.kind {
            if fields.is_empty() {
                anyhow::bail!(
                    "component '{id}': a scale with no 'fields' configured can only ever be a \
                     no-op"
                );
            }
            if fields.keys().any(|field| field.is_empty()) {
                anyhow::bail!(
                    "component '{id}': a scale field name must not be empty -- it could never \
                     match a real attribute"
                );
            }
            if fields.values().any(|factor| !factor.is_finite()) {
                anyhow::bail!("component '{id}': every scale factor must be a finite number");
            }
        }
    }

    // Rule 21: `has_signal`/`keep_signals`/`drop_signals` need a non-empty `signals:`, and
    // `keep_signals`/`drop_signals` may not name all three. For an allowlist, empty keeps nothing
    // (black hole) and all three keeps everything (no-op); a denylist is the mirror. Both are
    // rejected, but each message must name the right failure. `has_signal` naming all three stays
    // legal: under `mode: only` it forwards anything with a payload.
    for (id, component) in &components {
        match &component.kind {
            ComponentKind::HasSignal { signals, .. } => {
                if signals.is_empty() {
                    anyhow::bail!(
                        "component '{id}': 'signals' must name at least one signal -- an empty \
                         list can only ever drop every event"
                    );
                }
            }
            ComponentKind::KeepSignals { signals } => {
                if signals.is_empty() {
                    anyhow::bail!(
                        "component '{id}': 'signals' must name at least one signal -- an empty \
                         list keeps nothing, dropping every event"
                    );
                }
                if names_all_three(signals) {
                    anyhow::bail!(
                        "component '{id}': 'signals' names all three signals -- that keeps \
                         everything, a no-op that forwards every event untouched"
                    );
                }
            }
            ComponentKind::DropSignals { signals } => {
                if signals.is_empty() {
                    anyhow::bail!(
                        "component '{id}': 'signals' must name at least one signal -- an empty \
                         list drops nothing, a no-op that forwards every event untouched"
                    );
                }
                if names_all_three(signals) {
                    anyhow::bail!(
                        "component '{id}': 'signals' names all three signals -- that drops \
                         everything, dropping every event"
                    );
                }
            }
            _ => {}
        }
    }

    // Rule 22: an `otlp_out` header the transport sets itself (`grpc-*` by prefix; see
    // `RESERVED_OTLP_HEADERS`), or two names equal ignoring case, which would collide into one
    // `HeaderMap` entry (`OtlpOutput::with_headers`) with an unpredictable winner.
    for (id, component) in &components {
        if let ComponentKind::OtlpOut { headers, .. } = &component.kind {
            let mut seen_lowercase = BTreeSet::new();
            for name in headers.keys() {
                if name.is_empty() {
                    anyhow::bail!("component '{id}': 'headers' has an empty header name");
                }
                if name.starts_with(':') {
                    anyhow::bail!(
                        "component '{id}': 'headers' names {name:?} -- an HTTP/2 pseudo-header \
                         (starting with ':') can't be set as a custom header"
                    );
                }
                let lowercase = name.to_ascii_lowercase();
                if lowercase.starts_with("grpc-")
                    || RESERVED_OTLP_HEADERS.contains(&lowercase.as_str())
                {
                    anyhow::bail!(
                        "component '{id}': 'headers' names {name:?}, which this protocol sets \
                         itself -- it can't be overridden"
                    );
                }
                if !seen_lowercase.insert(lowercase) {
                    anyhow::bail!(
                        "component '{id}': 'headers' names {name:?}, which differs only in \
                         case from another entry -- HTTP header names are case-insensitive, so \
                         which value would actually be sent is undefined"
                    );
                }
            }
        }
    }

    // Rule 23: `otlp_out`'s `paths:` is HTTP-only: gRPC method names are fixed by the `.proto`
    // service definitions, so `paths:` under `protocol: grpc` is rejected, not ignored.
    for (id, component) in &components {
        if let ComponentKind::OtlpOut { protocol, paths, .. } = &component.kind {
            if *protocol == logit_config::OtlpProtocol::Grpc && !paths.is_empty() {
                anyhow::bail!(
                    "component '{id}': 'paths' has no effect under 'protocol: grpc' -- gRPC \
                     method names are fixed by the OTLP service definitions, not a mount point \
                     'paths' can move"
                );
            }
        }
    }

    // Rule 24: `otlp_out`'s `tls:`. `cert_file` or `key_file` alone is a typo, not half an mTLS
    // config; `insecure_skip_verify` with `ca_file` is contradictory; and under a non-`https://`
    // endpoint the block does nothing, since the scheme selects TLS
    // (`docs/adr/otlp-tls-and-pooled-grpc-client.md`).
    for (id, component) in &components {
        if let ComponentKind::OtlpOut { endpoint, tls, .. } = &component.kind {
            if tls.cert_file.is_some() != tls.key_file.is_some() {
                anyhow::bail!(
                    "component '{id}': 'tls.cert_file' and 'tls.key_file' must both be set for \
                     mutual TLS, or both omitted -- one alone can't be used"
                );
            }
            if tls.insecure_skip_verify && tls.ca_file.is_some() {
                anyhow::bail!(
                    "component '{id}': 'tls.insecure_skip_verify' and 'tls.ca_file' can't both \
                     be set -- 'insecure_skip_verify' trusts any certificate, which makes a \
                     specific trusted CA meaningless"
                );
            }
            if !tls.is_empty() && !endpoint.to_ascii_lowercase().starts_with("https://") {
                anyhow::bail!(
                    "component '{id}': 'tls' is set, but 'endpoint' ({endpoint:?}) isn't \
                     'https://' -- TLS is selected by the endpoint's scheme, so a 'tls:' block \
                     here would have no effect"
                );
            }
        }
    }

    // Rule 25: `trace_context`'s `span:` (`docs/adr/trace-context-span-lifting.md`). OTLP requires
    // a span name, and a zero `max_skew` rejects every span as skewed.
    for (id, component) in &components {
        if let ComponentKind::TraceContext { span: Some(span), .. } = &component.kind {
            if span.name.is_empty() {
                anyhow::bail!(
                    "component '{id}': a trace_context 'span.name' default can't be empty -- \
                     OTLP requires every span to have a name"
                );
            }
            if span.max_skew.is_zero() {
                anyhow::bail!(
                    "component '{id}': a trace_context 'span.max_skew' of 0s would reject every \
                     span as skewed"
                );
            }
        }
    }

    // Rule 29: `file_out`'s `rotate:`. With neither trigger it never rotates, which is what
    // `stdio_out` is for, so the message points there. `max_bytes: 0`/`max_files: 0` are impossible
    // bounds, and `max_files` above `MAX_ROTATE_FILES` makes every rotation a syscall storm.
    for (id, component) in &components {
        if let ComponentKind::FileOut { path, rotate, .. } = &component.kind {
            if rotate.max_bytes.is_none() && rotate.interval.is_none() {
                anyhow::bail!(
                    "component '{id}': 'file_out' needs at least one of 'rotate.max_bytes' or \
                     'rotate.interval' -- for an unrotated file, use 'stdio_out' with \
                     'target: {path}'"
                );
            }
            if rotate.max_bytes == Some(0) {
                anyhow::bail!(
                    "component '{id}': 'rotate.max_bytes' must be at least 1 -- 0 means every \
                     batch would rotate"
                );
            }
            if rotate.max_files == 0 {
                anyhow::bail!(
                    "component '{id}': 'rotate.max_files' must be at least 1 -- 0 would delete \
                     the file it just rotated"
                );
            }
            if rotate.max_files > logit_config::MAX_ROTATE_FILES {
                anyhow::bail!(
                    "component '{id}': 'rotate.max_files' must be at most {} -- every rotation \
                     renames each retained file",
                    logit_config::MAX_ROTATE_FILES
                );
            }
        }
    }

    // Rule 30: `kv`'s separators. An empty one splits between every character, identical ones split
    // every segment away from its own separator, and a `kv_sep` containing `pair_sep` never
    // survives the `pair_sep` split, which runs first.
    for (id, component) in &components {
        if let ComponentKind::Kv { pair_sep, kv_sep, .. } = &component.kind {
            if pair_sep.is_empty() {
                anyhow::bail!("component '{id}': a kv 'pair_sep' must not be empty");
            }
            if kv_sep.is_empty() {
                anyhow::bail!("component '{id}': a kv 'kv_sep' must not be empty");
            }
            if pair_sep == kv_sep {
                anyhow::bail!(
                    "component '{id}': a kv 'pair_sep' and 'kv_sep' must differ -- identical \
                     separators mean every segment is split away from its own separator, so no \
                     line could ever produce a pair"
                );
            }
            if kv_sep.contains(pair_sep.as_str()) {
                anyhow::bail!(
                    "component '{id}': a kv 'kv_sep' must not contain 'pair_sep' -- it could \
                     never appear intact inside a segment, since the 'pair_sep' split runs first"
                );
            }
        }
    }

    // Rule 31: `regex`. An empty `field` can't match (rule 19's reasoning); a pattern that fails to
    // compile or has no named group is caught here rather than by `build_spec` at startup. The
    // compiled `Regex` is dropped: `build_spec` rebuilds from the raw `ComponentKind`, like every
    // kind. A duplicate group name needs no check; the `regex` crate rejects it.
    for (id, component) in &components {
        if let ComponentKind::Regex { pattern, field } = &component.kind {
            if field.as_deref() == Some("") {
                anyhow::bail!(
                    "component '{id}': a regex with an empty 'field' name could never match an \
                     attribute -- omit 'field' to match the log message instead"
                );
            }
            let re = ::regex::Regex::new(pattern).map_err(|err| {
                anyhow::anyhow!("component '{id}': 'pattern' is not a valid regex: {err}")
            })?;
            if !re.capture_names().skip(1).any(|n| n.is_some()) {
                anyhow::bail!(
                    "component '{id}': a regex whose 'pattern' declares no named capture group \
                     can only ever be a no-op -- name the groups you want as attributes, e.g. \
                     (?P<status>\\d+)"
                );
            }
        }
    }

    // Rule 32: `csv` (`docs/adr/csv-positional-columns.md`). No `columns` is a no-op, an empty name
    // is no usable attribute, and a duplicate name would let the later field overwrite the earlier
    // on every event (rule 4's reasoning, applied to columns). `"` is the quote character this
    // parser frames fields with, and `\n`/`\r` are line framing every input already consumes.
    for (id, component) in &components {
        if let ComponentKind::Csv { columns, delimiter } = &component.kind {
            if columns.is_empty() {
                anyhow::bail!(
                    "component '{id}': a csv with no 'columns' configured can only ever be a no-op"
                );
            }
            if columns.iter().any(|c| c.is_empty()) {
                anyhow::bail!(
                    "component '{id}': a csv column name must not be empty -- it could never be \
                     a useful attribute name"
                );
            }
            let mut seen = std::collections::HashSet::with_capacity(columns.len());
            for column in columns {
                if !seen.insert(column.as_str()) {
                    anyhow::bail!(
                        "component '{id}': 'columns' names '{column}' twice -- the later field \
                         would silently overwrite the earlier one, leaving one column unreachable"
                    );
                }
            }
            if !delimiter.is_ascii() {
                anyhow::bail!("component '{id}': 'delimiter' must be a single ASCII character");
            }
            if matches!(delimiter, '"' | '\n' | '\r') {
                anyhow::bail!(
                    "component '{id}': 'delimiter' must not be {delimiter:?} -- '\"' is the \
                     quote character and '\\n'/'\\r' are line framing every input already \
                     consumes"
                );
            }
        }
    }

    // Rule 33: `compression:` does nothing outside `format: native`
    // (`docs/adr/file-output-native-format.md`).
    for (id, component) in &components {
        let stream_format = match &component.kind {
            ComponentKind::StdioOut { format, compression, .. }
            | ComponentKind::FileOut { format, compression, .. } => Some((*format, *compression)),
            _ => None,
        };
        if let Some((format, compression)) = stream_format {
            if format != StreamFormat::Native && compression != Compression::None {
                anyhow::bail!(
                    "component '{id}': 'compression' only applies under 'format: native'"
                );
            }
        }
    }

    // Rule 34: `logit_out`'s `tls:` gets rule 24's two consistency checks but no scheme check: its
    // `endpoint` is a bare `host:port`, so `tls:`'s presence alone turns TLS on. `logit_in`'s
    // `max_frame_bytes` may be neither `0` nor above `MAX_SANE_UNCOMPRESSED_LEN` (64 MiB), which
    // `read_frame`/`read_frame_with_header` enforce whatever a listener configures.
    for (id, component) in &components {
        if let ComponentKind::LogitOut { tls: Some(tls), .. } = &component.kind {
            if tls.cert_file.is_some() != tls.key_file.is_some() {
                anyhow::bail!(
                    "component '{id}': 'tls.cert_file' and 'tls.key_file' must both be set for \
                     mutual TLS, or both omitted -- one alone can't be used"
                );
            }
            if tls.insecure_skip_verify && tls.ca_file.is_some() {
                anyhow::bail!(
                    "component '{id}': 'tls.insecure_skip_verify' and 'tls.ca_file' can't both \
                     be set -- 'insecure_skip_verify' trusts any certificate, which makes a \
                     specific trusted CA meaningless"
                );
            }
        }
        if let ComponentKind::LogitIn { max_frame_bytes: Some(max_frame_bytes), .. } =
            &component.kind
        {
            if *max_frame_bytes == 0 {
                anyhow::bail!(
                    "component '{id}': 'max_frame_bytes' of 0 is impossible, not just small"
                );
            }
            if *max_frame_bytes > MAX_SANE_UNCOMPRESSED_LEN as u64 {
                anyhow::bail!(
                    "component '{id}': 'max_frame_bytes' ({max_frame_bytes}) is over the \
                     {MAX_SANE_UNCOMPRESSED_LEN}-byte ceiling the native frame format itself \
                     enforces"
                );
            }
        }
    }

    // Rule 35: `buffer.disk:` (`docs/adr/disk-backed-sink-buffer.md`). Disk replaces the in-memory
    // bound rather than sizing alongside it, so a non-default `max_batches`/`max_bytes` beside it
    // would be ignored. Zero disk bounds are impossible. Two sinks sharing a literal `disk.path`
    // would corrupt each other's spool; `DiskQueue::open`'s exclusive lock catches an aliased path
    // (`./spool` vs `spool`), since this function never resolves paths. Sorted as `(path, id)` so
    // entries sharing a path are adjacent.
    let mut disk_paths: Vec<(&str, &str)> = Vec::new();
    for (id, component) in &components {
        let Some(disk) = &component.buffer.disk else { continue };
        if component.buffer.max_batches != BufferConfig::default().max_batches
            || component.buffer.max_bytes != BufferConfig::default().max_bytes
        {
            anyhow::bail!(
                "component '{id}': 'buffer.max_batches'/'buffer.max_bytes' are ignored once \
                 'buffer.disk' is set -- disk replaces the in-memory bound rather than sizing \
                 alongside it; tune 'buffer.disk.max_bytes' instead"
            );
        }
        if disk.segment_bytes == 0 {
            anyhow::bail!(
                "component '{id}': 'buffer.disk.segment_bytes' must be at least 1 -- 0 means no \
                 record could ever be written"
            );
        }
        if disk.max_bytes == 0 {
            anyhow::bail!(
                "component '{id}': 'buffer.disk.max_bytes' must be at least 1 -- 0 means no \
                 record could ever be written"
            );
        }
        if disk.segment_bytes > disk.max_bytes {
            anyhow::bail!(
                "component '{id}': 'buffer.disk.segment_bytes' ({}) must not exceed \
                 'buffer.disk.max_bytes' ({}) -- a single segment could never fit the overall \
                 bound",
                disk.segment_bytes,
                disk.max_bytes
            );
        }
        disk_paths.push((disk.path.as_str(), id.as_str()));
    }
    disk_paths.sort_unstable();
    for pair in disk_paths.windows(2) {
        if pair[0].0 == pair[1].0 {
            anyhow::bail!(
                "components '{}' and '{}' both set 'buffer.disk.path' to '{}' -- two sinks \
                 sharing one spool directory would corrupt each other's records",
                pair[0].1,
                pair[1].1,
                pair[0].0
            );
        }
    }

    // Rule 36: `has_attributes`/`drop_attributes` (`docs/adr/attribute-filtering-components.md`).
    // The maps are conjunctions, so zero pairs match every event: `has_attributes` forwards all
    // (no-op) and `drop_attributes` drops all (black hole), the inverse of rule 21's list of
    // alternatives. An empty key can't name an attribute; a non-finite value never compares equal
    // under `crate::attributes`' coercing matcher. The same key in `resource:` and `attributes:` is
    // legal: one addresses the batch, the other the event.
    for (id, component) in &components {
        let (kind_name, resource, attributes) = match &component.kind {
            ComponentKind::HasAttributes { resource, attributes } => {
                ("has_attributes", resource, attributes)
            }
            ComponentKind::DropAttributes { resource, attributes } => {
                ("drop_attributes", resource, attributes)
            }
            _ => continue,
        };

        if resource.is_empty() && attributes.is_empty() {
            if kind_name == "has_attributes" {
                anyhow::bail!(
                    "component '{id}': a has_attributes with neither 'resource' nor \
                     'attributes' configured matches every event -- a no-op that forwards \
                     every event untouched"
                );
            } else {
                anyhow::bail!(
                    "component '{id}': a drop_attributes with neither 'resource' nor \
                     'attributes' configured matches every event -- and so can only ever drop \
                     every one of them"
                );
            }
        }

        for (map_name, map) in [("resource", resource), ("attributes", attributes)] {
            if map.keys().any(|key| key.is_empty()) {
                anyhow::bail!(
                    "component '{id}': a {kind_name} '{map_name}' key must not be empty -- it \
                     could never name a real attribute"
                );
            }
            if map
                .values()
                .any(|value| matches!(value, logit_config::SetValue::F64(f) if !f.is_finite()))
            {
                anyhow::bail!(
                    "component '{id}': every {kind_name} '{map_name}' value must be a finite \
                     number -- a non-finite value never compares equal to anything, so that \
                     entry could never match"
                );
            }
        }
    }

    // Rule 37: `has_provenance`/`drop_provenance` (`docs/adr/provenance-filtering-components.md`).
    // Oriented like rule 36, not rule 21, although each field is a list of alternatives: an empty
    // `origin:`/`previous:` leaves that field out of the match (vacuously true) rather than
    // matching zero alternatives, and the two AND'd fields decide. So with both empty
    // `has_provenance` is the no-op and `drop_provenance` the black hole. An empty entry can't name
    // a component id; a repeated one is a copy-paste mistake (rule 4's reasoning).
    for (id, component) in &components {
        let (kind_name, origin, previous) = match &component.kind {
            ComponentKind::HasProvenance { origin, previous } => {
                ("has_provenance", origin, previous)
            }
            ComponentKind::DropProvenance { origin, previous } => {
                ("drop_provenance", origin, previous)
            }
            _ => continue,
        };

        if origin.is_empty() && previous.is_empty() {
            if kind_name == "has_provenance" {
                anyhow::bail!(
                    "component '{id}': a has_provenance with neither 'origin' nor 'previous' \
                     configured matches every batch -- a no-op that forwards every event \
                     untouched"
                );
            } else {
                anyhow::bail!(
                    "component '{id}': a drop_provenance with neither 'origin' nor 'previous' \
                     configured matches every batch -- and so can only ever drop every event"
                );
            }
        }

        for (field_name, list) in [("origin", origin), ("previous", previous)] {
            if list.iter().any(|entry| entry.is_empty()) {
                anyhow::bail!(
                    "component '{id}': a {kind_name} '{field_name}' entry must not be empty -- \
                     it could never name a real component id"
                );
            }
            let mut seen = std::collections::HashSet::with_capacity(list.len());
            if let Some(dup) = list.iter().find(|entry| !seen.insert(entry.as_str())) {
                anyhow::bail!(
                    "component '{id}': a {kind_name} '{field_name}' entry ('{dup}') is repeated \
                     -- almost certainly a copy-paste mistake, since a repeated alternative \
                     changes nothing about what matches"
                );
            }
        }
    }

    // Rule 38: `max_packet_bytes: 0` is an impossible bound on all three sinks. `collectd_out` also
    // requires collectd's own `MaxPacketSize` range (`docs/adr/collectd-binary-relay.md`): above
    // the UDP payload ceiling every datagram fails `EMSGSIZE`, which `collectd_out` counts as a
    // per-datagram drop, not a `Fault`, so it would report `requests{class="ok"}` while delivering
    // nothing. `statsd_out` starts a new datagram at the cap and `graphite_out` counts an oversize
    // one `oversize_datagram`; neither ADR claims a range, so both keep only the zero check.
    for (id, component) in &components {
        if matches!(
            &component.kind,
            ComponentKind::StatsdOut { max_packet_bytes: 0, .. }
                | ComponentKind::CollectdOut { max_packet_bytes: 0, .. }
                | ComponentKind::GraphiteOut { max_packet_bytes: 0, .. }
        ) {
            anyhow::bail!(
                "component '{id}': max_packet_bytes: 0 would drop every metric line -- use a \
                 positive byte size"
            );
        }
        if let ComponentKind::CollectdOut { max_packet_bytes, .. } = &component.kind {
            if !(1024..=65535).contains(max_packet_bytes) {
                anyhow::bail!(
                    "component '{id}': max_packet_bytes: {max_packet_bytes} is outside \
                     1024..=65535 -- collectd's own MaxPacketSize range; a value above 65535 \
                     packs datagrams no UDP socket can send (every send would fail EMSGSIZE, \
                     silently reported as requests{{class=\"ok\"}}) and a value below 1024 is \
                     narrower than collectd itself allows"
                );
            }
        }
    }

    // Rule 39: a cumulative `aggregate` needs both retention bounds non-zero, or every window's
    // `Sum`/`Histogram` is its own increment labeled `Cumulative`: a wrong number for
    // `prometheus_out`, which reads it as a running total.
    for (id, component) in &components {
        if let ComponentKind::Aggregate {
            temporality, series_retention, max_retained_series, ..
        } = &component.kind
        {
            if *temporality == logit_config::AggregateTemporality::Cumulative
                && (*series_retention == 0 || *max_retained_series == 0)
            {
                anyhow::bail!(
                    "component '{id}': temporality: cumulative requires series_retention >= 1 \
                     (a count of windows) and max_retained_series >= 1 -- with either at 0 no \
                     series survives a flush, so every window would emit its own increment \
                     labelled as a cumulative total"
                );
            }
        }
    }

    // Rule 40: a scrape-mode `prometheus_in`'s scrape settings. Gated on scrape mode; rule 55 owns
    // the mode itself, including neither mode set.
    for (id, component) in &components {
        if let ComponentKind::PrometheusIn {
            scrape_targets,
            timeout,
            headers,
            scrape_tls: tls,
            ..
        } = &component.kind
        {
            if scrape_targets.is_empty() {
                continue; // bind mode, or no mode at all -- rule 55's, either way
            }
            for target in scrape_targets {
                if !is_absolute_http_url(target) {
                    anyhow::bail!(
                        "component '{id}': 'scrape_targets' entry {target:?} isn't an absolute \
                         'http://' or 'https://' URL"
                    );
                }
            }
            if timeout.is_zero() {
                anyhow::bail!(
                    "component '{id}': 'timeout: 0s' would fail every scrape immediately -- use \
                     a positive duration"
                );
            }
            if tls.cert_file.is_some() != tls.key_file.is_some() {
                anyhow::bail!(
                    "component '{id}': 'scrape_tls.cert_file' and 'scrape_tls.key_file' must \
                     both be set for mutual TLS, or both omitted -- one alone can't be used"
                );
            }
            if tls.insecure_skip_verify && tls.ca_file.is_some() {
                anyhow::bail!(
                    "component '{id}': 'scrape_tls.insecure_skip_verify' and \
                     'scrape_tls.ca_file' can't both be set -- 'insecure_skip_verify' trusts any \
                     certificate, which makes a specific trusted CA meaningless"
                );
            }
            let any_https =
                scrape_targets.iter().any(|t| t.to_ascii_lowercase().starts_with("https://"));
            if !tls.is_empty() && !any_https {
                anyhow::bail!(
                    "component '{id}': 'scrape_tls' is set, but no 'scrape_targets' entry is \
                     'https://' -- TLS is selected per-target by its own scheme, so a \
                     'scrape_tls:' block here would have no effect"
                );
            }
            let mut seen_lowercase = BTreeSet::new();
            for name in headers.keys() {
                if name.is_empty() {
                    anyhow::bail!("component '{id}': 'headers' has an empty header name");
                }
                if name.starts_with(':') {
                    anyhow::bail!(
                        "component '{id}': 'headers' names {name:?} -- an HTTP/2 pseudo-header \
                         (starting with ':') can't be set as a custom header"
                    );
                }
                let lowercase = name.to_ascii_lowercase();
                if RESERVED_PROMETHEUS_HEADERS.contains(&lowercase.as_str()) {
                    anyhow::bail!(
                        "component '{id}': 'headers' names {name:?}, which this input sets \
                         itself -- it can't be overridden"
                    );
                }
                if !seen_lowercase.insert(lowercase) {
                    anyhow::bail!(
                        "component '{id}': 'headers' names {name:?}, which differs only in \
                         case from another entry -- HTTP header names are case-insensitive, so \
                         which value would actually be sent is undefined"
                    );
                }
            }
        }
    }

    // Rule 41: a registry-mode `prometheus_out` `path:` must be absolute, since a request URI's
    // path always is (a relative one would 404 every scrape), and `max_series: 0` would evict every
    // series on arrival. Registry mode only: rule 56 rejects a non-default value of either in
    // sender mode, and one gate per rule keeps the two from racing for the same config.
    for (id, component) in &components {
        let ComponentKind::PrometheusOut { bind: Some(_), path, max_series, .. } = &component.kind
        else {
            continue;
        };
        if !path.starts_with('/') {
            anyhow::bail!(
                "component '{id}': prometheus_out path '{path}' must start with '/' -- a request \
                 URI's path always does, so this one could never be scraped"
            );
        }
        if *max_series == 0 {
            anyhow::bail!(
                "component '{id}': max_series: 0 would evict every series as soon as it arrived \
                 -- use a positive count"
            );
        }
    }

    // Rule 42: a `generate_in`'s bounds and templates. `0` for a count means "generate nothing"
    // (`None` spells unbounded/unthrottled). The template checks keep a mistyped placeholder from
    // silently collapsing a scenario's cardinality. As with rule 31's `regex`, the parse result is
    // dropped; `build_spec` re-derives from the raw `ComponentKind`.
    for (id, component) in &components {
        let ComponentKind::GenerateIn { count, batch, rate, event, resource } = &component.kind
        else {
            continue;
        };
        if *count == Some(0) {
            anyhow::bail!(
                "component '{id}': count: 0 would generate nothing -- omit 'count' for an \
                 unbounded run, or name a positive number of events"
            );
        }
        if *batch == 0 {
            anyhow::bail!(
                "component '{id}': batch: 0 would generate nothing -- use a positive batch size"
            );
        }
        if *rate == Some(0) {
            anyhow::bail!(
                "component '{id}': rate: 0 would generate nothing -- omit 'rate' to generate as \
                 fast as backpressure allows"
            );
        }
        if let Some(log) = &event.log {
            check_generate_template(id, "event.log", log, Rendering::Copied)?;
        }
        for (key, value) in &event.attributes {
            if key.is_empty() {
                anyhow::bail!(
                    "component '{id}': 'event.attributes' has an empty key -- an attribute with \
                     no name could never be read back"
                );
            }
            check_generate_template(
                id,
                &format!("event.attributes.{key}"),
                value,
                Rendering::Copied,
            )?;
        }
        for (key, value) in resource {
            if key.is_empty() {
                anyhow::bail!(
                    "component '{id}': 'resource' has an empty key -- a resource attribute with \
                     no name could never be read back"
                );
            }
            check_generate_template(id, &format!("resource.{key}"), value, Rendering::Copied)?;
        }
        if let Some(metric) = &event.metric {
            if metric.name.is_empty() {
                anyhow::bail!(
                    "component '{id}': 'event.metric.name' is empty -- a generated metric needs a \
                     name, the same reason rule 11 requires one of every kv_metrics entry"
                );
            }
            if !metric.value.is_finite() {
                anyhow::bail!(
                    "component '{id}': 'event.metric.value' must be a finite number, got {}",
                    metric.value
                );
            }
            check_generate_template(id, "event.metric.name", &metric.name, Rendering::Interned)?;
        }
    }

    // Rule 43: a listener's `tls:` needs a stream transport. DTLS is out of scope
    // (`docs/adr/syslog-tcp-ingress-and-tls.md`'s "Alternatives considered"), so `tls:` under UDP
    // could never take effect, and running a connection the operator meant encrypted in the clear
    // is the worst outcome (rule 24's reasoning). One rule for every such listener: a new
    // stream-capable listener adds a match arm, not a rule number.
    for (id, component) in &components {
        let tls_on_a_datagram_transport = match &component.kind {
            ComponentKind::SyslogIn { transport, tls: Some(_), .. } => {
                *transport == SyslogTransport::Udp
            }
            ComponentKind::GraphiteIn { transport, tls: Some(_), .. } => {
                *transport == GraphiteTransport::Udp
            }
            ComponentKind::StatsdIn { transport, tls: Some(_), .. } => {
                *transport == StatsdTransport::Udp
            }
            _ => false,
        };
        if tls_on_a_datagram_transport {
            anyhow::bail!(
                "component '{id}': 'tls:' needs 'transport: tcp' -- TLS is defined over a byte \
                 stream, and DTLS is out of scope"
            );
        }
    }

    // Rule 44: `syslog_out`'s `tls:`. Rule 34's two checks, messages verbatim so one grep finds
    // every sink making them: like `logit_out`, it dials a bare `host:port`. Its own third check:
    // syslog over TLS is RFC 5425, TLS over TCP, so `tls:` under `transport: udp` is rejected.
    // `SyslogOutput::with_tls` re-checks that one, since `resolve` isn't its only possible caller.
    for (id, component) in &components {
        let ComponentKind::SyslogOut { tls: Some(tls), transport, .. } = &component.kind else {
            continue;
        };
        if tls.cert_file.is_some() != tls.key_file.is_some() {
            anyhow::bail!(
                "component '{id}': 'tls.cert_file' and 'tls.key_file' must both be set for \
                 mutual TLS, or both omitted -- one alone can't be used"
            );
        }
        if tls.insecure_skip_verify && tls.ca_file.is_some() {
            anyhow::bail!(
                "component '{id}': 'tls.insecure_skip_verify' and 'tls.ca_file' can't both be \
                 set -- 'insecure_skip_verify' trusts any certificate, which makes a specific \
                 trusted CA meaningless"
            );
        }
        if *transport == logit_config::SyslogTransport::Udp {
            anyhow::bail!("component '{id}': DTLS is out of scope; 'tls:' needs 'transport: tcp'");
        }
    }

    // Rule 52: `statsd_out`'s `tls:`: rule 44's three checks, messages verbatim. Its own rule
    // rather than an arm of 44's loop, since sink TLS rules are one per sink (24/34/44/52).
    // `StatsdOutput::with_tls` re-checks the UDP case, since `resolve` isn't its only possible
    // caller.
    for (id, component) in &components {
        let ComponentKind::StatsdOut { tls: Some(tls), transport, .. } = &component.kind else {
            continue;
        };
        if tls.cert_file.is_some() != tls.key_file.is_some() {
            anyhow::bail!(
                "component '{id}': 'tls.cert_file' and 'tls.key_file' must both be set for \
                 mutual TLS, or both omitted -- one alone can't be used"
            );
        }
        if tls.insecure_skip_verify && tls.ca_file.is_some() {
            anyhow::bail!(
                "component '{id}': 'tls.insecure_skip_verify' and 'tls.ca_file' can't both be \
                 set -- 'insecure_skip_verify' trusts any certificate, which makes a specific \
                 trusted CA meaningless"
            );
        }
        if *transport == StatsdTransport::Udp {
            anyhow::bail!("component '{id}': DTLS is out of scope; 'tls:' needs 'transport: tcp'");
        }
    }

    // Rule 45: `handshake_timeout` must be non-zero on every kind that has one: no TLS accept,
    // first-byte read, or `Hello` read completes in zero time, so every connection would close on
    // accept. A UDP `syslog_in`/`graphite_in`/`statsd_in` has no connection to hand shake, so a set
    // value there is rejected (rule 33's shape). Only a non-default value counts as set, so the
    // default stays legal under UDP. `otlp_in`, `datadog_in`, and `datadog_trace_in` get only the
    // zero check: the budget also bounds a plaintext connection's first-byte wait
    // (`crates/logit-inputs/src/otlp.rs`'s "Handshake timeout"), so it is live with or without
    // `tls:`.
    for (id, component) in &components {
        let handshake_timeout = match &component.kind {
            ComponentKind::SyslogIn { handshake_timeout, .. }
            | ComponentKind::GraphiteIn { handshake_timeout, .. }
            | ComponentKind::StatsdIn { handshake_timeout, .. }
            | ComponentKind::LogitIn { handshake_timeout, .. }
            | ComponentKind::OtlpIn { handshake_timeout, .. }
            | ComponentKind::DatadogIn { handshake_timeout, .. }
            | ComponentKind::DatadogTraceIn { handshake_timeout, .. } => *handshake_timeout,
            _ => continue,
        };
        if handshake_timeout.is_zero() {
            anyhow::bail!(
                "component '{id}': 'handshake_timeout' must be greater than 0s -- 0 would close \
                 every connection before its handshake could start"
            );
        }
        if handshake_timeout == default_handshake_timeout() {
            continue; // a defaulted value is not a set one -- see this rule's comment
        }
        let (kind_name, datagram) = match &component.kind {
            ComponentKind::SyslogIn { transport, .. } => {
                ("syslog_in", *transport == SyslogTransport::Udp)
            }
            ComponentKind::GraphiteIn { transport, .. } => {
                ("graphite_in", *transport == GraphiteTransport::Udp)
            }
            ComponentKind::StatsdIn { transport, .. } => {
                ("statsd_in", *transport == StatsdTransport::Udp)
            }
            _ => continue,
        };
        if datagram {
            anyhow::bail!(
                "component '{id}': 'handshake_timeout' needs 'transport: tcp' -- a UDP \
                 {kind_name} has no connection to hand shake, so the value could never take effect"
            );
        }
    }

    // Rule 53: `idle_timeout`, rule 45's checks one field over
    // (`docs/adr/idle-connection-timeout.md`), with two differences. The field is an `Option` whose
    // absence means "no idle timeout", so every `Some` is set: the UDP check rejects any value, and
    // the zero message says to omit the field. And `0s` is impossible because a connection is idle
    // whenever the listener awaits its next byte. `logit_in`, `otlp_in`, `datadog_in`,
    // `datadog_trace_in`, and `prometheus_in` have no datagram transport, so their arms pass
    // `false`; a scrape-mode `prometheus_in`'s value is rule 55's wrong-mode check.
    for (id, component) in &components {
        let (kind_name, idle_timeout, datagram) = match &component.kind {
            ComponentKind::SyslogIn { idle_timeout, transport, .. } => {
                ("syslog_in", *idle_timeout, *transport == SyslogTransport::Udp)
            }
            ComponentKind::GraphiteIn { idle_timeout, transport, .. } => {
                ("graphite_in", *idle_timeout, *transport == GraphiteTransport::Udp)
            }
            ComponentKind::StatsdIn { idle_timeout, transport, .. } => {
                ("statsd_in", *idle_timeout, *transport == StatsdTransport::Udp)
            }
            ComponentKind::LogitIn { idle_timeout, .. } => ("logit_in", *idle_timeout, false),
            ComponentKind::OtlpIn { idle_timeout, .. } => ("otlp_in", *idle_timeout, false),
            ComponentKind::DatadogIn { idle_timeout, .. } => ("datadog_in", *idle_timeout, false),
            ComponentKind::DatadogTraceIn { idle_timeout, .. } => {
                ("datadog_trace_in", *idle_timeout, false)
            }
            ComponentKind::PrometheusIn { idle_timeout, .. } => {
                ("prometheus_in", *idle_timeout, false)
            }
            _ => continue,
        };
        let Some(idle_timeout) = idle_timeout else { continue };
        if idle_timeout.is_zero() {
            anyhow::bail!(
                "component '{id}': 'idle_timeout' must be greater than 0s -- omit the field to \
                 disable the idle timeout"
            );
        }
        if datagram {
            anyhow::bail!(
                "component '{id}': 'idle_timeout' needs 'transport: tcp' -- a UDP {kind_name} has \
                 no connection to time out, so the value could never take effect"
            );
        }
    }

    // Rule 46: `graphite_in`/`graphite_out` (`docs/adr/graphite-carbon-relay.md`). Carbon's pickle
    // wire is a 4-byte big-endian length prefix per batch (Twisted's `Int32StringReceiver`), which
    // means nothing in a self-delimiting datagram, so pickle over UDP could only mis-frame. The
    // zero checks are impossible bounds; `GRAPHITE_FRAME_BYTES_RANGE` holds the range's reasoning.
    for (id, component) in &components {
        match &component.kind {
            ComponentKind::GraphiteIn {
                transport,
                protocol,
                max_line_bytes,
                max_frame_bytes,
                ..
            } => {
                if *protocol == GraphiteProtocol::Pickle && *transport != GraphiteTransport::Tcp {
                    anyhow::bail!(
                        "component '{id}': protocol: pickle requires transport: tcp -- carbon \
                         frames a pickle batch with a 4-byte big-endian length prefix (Twisted's \
                         Int32StringReceiver), which has no meaning in a datagram that already \
                         delimits itself"
                    );
                }
                if *max_line_bytes == 0 {
                    anyhow::bail!(
                        "component '{id}': max_line_bytes: 0 would skip every plaintext line -- \
                         use a positive byte size"
                    );
                }
                if *max_frame_bytes == 0 {
                    anyhow::bail!(
                        "component '{id}': max_frame_bytes: 0 would refuse every pickle frame -- \
                         use a positive byte size"
                    );
                }
                if !GRAPHITE_FRAME_BYTES_RANGE.contains(max_frame_bytes) {
                    anyhow::bail!(
                        "component '{id}': max_frame_bytes: {max_frame_bytes} is outside \
                         1024..=16777216 -- below 1024 no real carbon pickle batch fits, and \
                         above 16MiB a frame's declared length is a larger allocation than any \
                         sender has a reason to ask for"
                    );
                }
            }
            ComponentKind::GraphiteOut {
                transport,
                protocol,
                max_frame_bytes,
                connect_timeout,
                ..
            } => {
                if *protocol == GraphiteProtocol::Pickle && *transport != GraphiteTransport::Tcp {
                    anyhow::bail!(
                        "component '{id}': protocol: pickle requires transport: tcp -- carbon's \
                         length-prefixed pickle framing has no meaning in a datagram"
                    );
                }
                if *max_frame_bytes == 0 {
                    anyhow::bail!(
                        "component '{id}': max_frame_bytes: 0 could never fit even an empty \
                         pickle frame -- use a positive byte size"
                    );
                }
                if !GRAPHITE_FRAME_BYTES_RANGE.contains(max_frame_bytes) {
                    anyhow::bail!(
                        "component '{id}': max_frame_bytes: {max_frame_bytes} is outside \
                         1024..=16777216 -- below 1024 no real carbon pickle batch fits, and \
                         above 16MiB a frame's declared length is a larger allocation than any \
                         sender has a reason to ask for"
                    );
                }
                if connect_timeout.is_zero() {
                    anyhow::bail!(
                        "component '{id}': connect_timeout: 0s could never establish a TCP \
                         connection -- use a positive duration"
                    );
                }
            }
            _ => {}
        }
    }

    // Rule 54: `keep_values` (`docs/adr/value-allowlist-cardinality-clamp.md`), shaped like `set`
    // and checked like rule 12, plus: an empty `allow` clamps every value, which `set`/`remove`
    // already express, so the message names them; a non-finite `F64` never compares equal under
    // `value_matches` (rule 36's reasoning); and under `normalize: [lower]` a `Str` literal that
    // isn't ASCII-lowercase could never be produced, and is rejected rather than lowercased so what
    // validates is what the operator wrote. A repeated step is a no-op; an empty `normalize:` is
    // the default.
    for (id, component) in &components {
        if let ComponentKind::KeepValues { resource, attributes } = &component.kind {
            if resource.is_empty() && attributes.is_empty() {
                anyhow::bail!(
                    "component '{id}': a keep_values with neither 'resource' nor 'attributes' \
                     configured can only ever be a no-op"
                );
            }
            for (map_name, map) in [("resource", resource), ("attributes", attributes)] {
                for (field, allow_list) in map {
                    if field.is_empty() {
                        anyhow::bail!(
                            "component '{id}': a keep_values '{map_name}' key must not be empty \
                             -- it could never name a real attribute"
                        );
                    }
                    if allow_list.allow.is_empty() {
                        anyhow::bail!(
                            "component '{id}': keep_values '{map_name}.{field}' has an empty \
                             'allow' list -- that clamps every value unconditionally, which is \
                             what 'set' (with 'other:') or 'remove' (without it) already do"
                        );
                    }
                    let lower = allow_list.normalize.contains(&logit_config::NormalizeStep::Lower);
                    let mut seen_steps = std::collections::HashSet::new();
                    for step in &allow_list.normalize {
                        if !seen_steps.insert(step) {
                            anyhow::bail!(
                                "component '{id}': keep_values '{map_name}.{field}' repeats a \
                                 'normalize' step -- a duplicate can only ever be a no-op"
                            );
                        }
                    }
                    for literal in allow_list.allow.iter().chain(allow_list.other.iter()) {
                        match literal {
                            logit_config::SetValue::F64(f) if !f.is_finite() => {
                                anyhow::bail!(
                                    "component '{id}': every keep_values '{map_name}.{field}' \
                                     value must be a finite number -- a non-finite value never \
                                     compares equal to anything, so that entry could never match"
                                );
                            }
                            logit_config::SetValue::Str(s)
                                if lower && s.chars().any(|c| c.is_ascii_uppercase()) =>
                            {
                                anyhow::bail!(
                                    "component '{id}': keep_values '{map_name}.{field}' has \
                                     'normalize: [lower]' but the literal '{s}' isn't already \
                                     ASCII-lowercase -- a 'lower' step could never produce it"
                                );
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    // Rule 55: a `prometheus_in` is a scrape client (`scrape_targets:`) or a remote-write receiver
    // (`bind:`), exactly one (`docs/adr/prometheus-remote-write.md`). A non-default field of the
    // other mode is rejected (rules 45/53's shape). Comparing against defaults is what keeps
    // `interval` defaulted in bind mode, so rule 9 needs no bind-mode carve-out.
    for (id, component) in &components {
        let ComponentKind::PrometheusIn {
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
        } = &component.kind
        else {
            continue;
        };
        match (scrape_targets.is_empty(), bind.is_some()) {
            (false, true) => anyhow::bail!(
                "component '{id}': 'scrape_targets' and 'bind' are the two modes of a \
                 prometheus_in -- a scrape client and a remote-write receiver -- so exactly one \
                 of them belongs on a component, not both"
            ),
            (true, false) => anyhow::bail!(
                "component '{id}': a prometheus_in needs either 'scrape_targets' (scrape targets \
                 on an interval) or 'bind' (a remote-write receiver) -- with neither it would \
                 never produce an event"
            ),
            _ => {}
        }
        if bind.is_some() {
            // Rule 41's check, for its reason: a request URI's path is always absolute.
            if !path.starts_with('/') {
                anyhow::bail!(
                    "component '{id}': prometheus_in path '{path}' must start with '/' -- a \
                     request URI's path always does, so this one could never be written to"
                );
            }
            let wrong = if *interval != default_prometheus_scrape_interval() {
                Some("interval")
            } else if *timeout != default_prometheus_scrape_timeout() {
                Some("timeout")
            } else if !headers.is_empty() {
                Some("headers")
            } else if !scrape_tls.is_empty() {
                Some("scrape_tls")
            } else {
                None
            };
            if let Some(wrong) = wrong {
                anyhow::bail!(
                    "component '{id}': '{wrong}' configures an outbound scrape, and this \
                     prometheus_in has 'bind' set -- a receiver performs no scrape, so the value \
                     could never take effect"
                );
            }
            // Rule 9's reasoning: a zero `ttl` expires every entry as it is written, yet the
            // receiver would still sweep and lock per request. `max_families: 0` turns the cache
            // off, so `ttl` is unchecked there.
            if metadata_cache.max_families > 0 && metadata_cache.ttl.is_zero() {
                anyhow::bail!(
                    "component '{id}': 'metadata_cache.ttl' is 0s, so every remembered metric \
                     type would expire before the next request could use it -- set a positive \
                     duration, or 'metadata_cache: {{max_families: 0}}' to turn the cache off"
                );
            }
        } else {
            let wrong = if *path != default_prometheus_write_path() {
                Some("path")
            } else if bind_tls.is_some() {
                Some("bind_tls")
            } else if idle_timeout.is_some() {
                Some("idle_timeout")
            } else if *metadata_cache != MetadataCacheConfig::default() {
                Some("metadata_cache")
            } else {
                None
            };
            if let Some(wrong) = wrong {
                anyhow::bail!(
                    "component '{id}': '{wrong}' configures the remote-write receiver, and this \
                     prometheus_in has 'scrape_targets' set -- a scrape client binds nothing and \
                     reads a '# TYPE' line in every response it scrapes, so the value could never \
                     take effect"
                );
            }
        }
    }

    // Rule 56: `prometheus_out` is a registry (`bind:`) or a remote-write sender (`endpoint:`),
    // exactly one (`docs/adr/prometheus-remote-write.md`). A non-default field of the other mode is
    // rejected (rules 45/53's shape); the sender's own fields get rule 40's checks. Rule 41 owns
    // the registry's `path`/`max_series`.
    for (id, component) in &components {
        let ComponentKind::PrometheusOut {
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
        } = &component.kind
        else {
            continue;
        };
        match (bind, endpoint) {
            (Some(_), Some(_)) => anyhow::bail!(
                "component '{id}': 'bind' and 'endpoint' are the two modes of prometheus_out and \
                 can't both be set -- 'bind' serves an exposition a Prometheus scrapes, \
                 'endpoint' writes to a remote-write receiver; use two components for both"
            ),
            (None, None) => anyhow::bail!(
                "component '{id}': prometheus_out needs exactly one of 'bind' (serve an \
                 exposition) or 'endpoint' (write to a remote-write receiver) -- with neither it \
                 has nowhere to put anything it's sent"
            ),
            (Some(_), None) => {
                // Registry mode: sender-only fields must be at their defaults, compared against
                // `logit_config`'s default fns so a moved default can't disagree with this rule.
                if *version != logit_config::RemoteWriteVersion::default() {
                    anyhow::bail!(
                        "component '{id}': 'version' selects the remote-write protocol version \
                         and only means anything with 'endpoint' -- a prometheus_out with 'bind' \
                         serves an exposition, which has no wire version to pick"
                    );
                }
                if *compression != logit_config::RemoteWriteCompression::default() {
                    anyhow::bail!(
                        "component '{id}': 'compression' selects how a remote-write request body \
                         is compressed and only means anything with 'endpoint' -- a \
                         prometheus_out with 'bind' serves an exposition, which sends no request \
                         bodies"
                    );
                }
                if *timeout != logit_config::default_prometheus_endpoint_timeout() {
                    anyhow::bail!(
                        "component '{id}': 'timeout' bounds one remote-write request and only \
                         means anything with 'endpoint' -- a prometheus_out with 'bind' issues no \
                         requests of its own"
                    );
                }
                if !headers.is_empty() {
                    anyhow::bail!(
                        "component '{id}': 'headers' are sent on a remote-write request and only \
                         mean anything with 'endpoint' -- a prometheus_out with 'bind' answers \
                         requests rather than making them"
                    );
                }
                if !endpoint_tls.is_empty() {
                    anyhow::bail!(
                        "component '{id}': 'endpoint_tls' tunes the TLS of a remote-write request \
                         and only means anything with 'endpoint' -- a prometheus_out with 'bind' \
                         serves plaintext HTTP and has no client TLS at all (see the ADR's \
                         'Security posture')"
                    );
                }
            }
            (None, Some(endpoint)) => {
                // Sender mode: registry-only fields at their defaults, then the sender's own
                // checks.
                if path != &logit_config::default_prometheus_path() {
                    anyhow::bail!(
                        "component '{id}': 'path' is the path this sink *serves* an exposition on \
                         and only means anything with 'bind' -- a remote-write 'endpoint' carries \
                         its own path, so put it there"
                    );
                }
                if *expire_after != logit_config::default_prometheus_expire_after() {
                    anyhow::bail!(
                        "component '{id}': 'expire_after' expires series out of the exposition \
                         registry and only means anything with 'bind' -- a remote-write sender \
                         holds no series between batches"
                    );
                }
                if *max_series != logit_config::default_prometheus_max_series() {
                    anyhow::bail!(
                        "component '{id}': 'max_series' caps the exposition registry and only \
                         means anything with 'bind' -- a remote-write sender holds no series \
                         between batches"
                    );
                }
                if !is_absolute_http_url(endpoint) {
                    anyhow::bail!(
                        "component '{id}': 'endpoint' {endpoint:?} isn't an absolute 'http://' or \
                         'https://' URL -- give the receiver's full write URL, path included \
                         (typically '/api/v1/write')"
                    );
                }
                if timeout.is_zero() {
                    anyhow::bail!(
                        "component '{id}': 'timeout: 0s' would fail every remote-write request \
                         immediately -- use a positive duration"
                    );
                }
                if *version == logit_config::RemoteWriteVersion::V2
                    && *compression == logit_config::RemoteWriteCompression::Zstd
                {
                    anyhow::bail!(
                        "component '{id}': 'compression: zstd' needs 'version: 1' -- remote-write \
                         2.0 mandates Snappy, and zstd is the VictoriaMetrics variant of 1.0; use \
                         'compression: snappy' with 'version: 2'"
                    );
                }
                // Rule 40's header checks, against this sink's own reserved list.
                let mut seen_lowercase = BTreeSet::new();
                for name in headers.keys() {
                    if name.is_empty() {
                        anyhow::bail!("component '{id}': 'headers' has an empty header name");
                    }
                    if name.starts_with(':') {
                        anyhow::bail!(
                            "component '{id}': 'headers' names {name:?} -- an HTTP/2 \
                             pseudo-header (starting with ':') can't be set as a custom header"
                        );
                    }
                    let lowercase = name.to_ascii_lowercase();
                    if RESERVED_REMOTE_WRITE_HEADERS.contains(&lowercase.as_str()) {
                        anyhow::bail!(
                            "component '{id}': 'headers' names {name:?}, which this output sets \
                             itself -- it can't be overridden"
                        );
                    }
                    if !seen_lowercase.insert(lowercase) {
                        anyhow::bail!(
                            "component '{id}': 'headers' names {name:?}, which differs only in \
                             case from another entry -- HTTP header names are case-insensitive, \
                             so which value would actually be sent is undefined"
                        );
                    }
                }
                // Rules 24/34/44/52's two consistency checks, on this sink's block.
                if endpoint_tls.cert_file.is_some() != endpoint_tls.key_file.is_some() {
                    anyhow::bail!(
                        "component '{id}': 'endpoint_tls.cert_file' and 'endpoint_tls.key_file' \
                         must both be set for mutual TLS, or both omitted -- one alone can't be \
                         used"
                    );
                }
                if endpoint_tls.insecure_skip_verify && endpoint_tls.ca_file.is_some() {
                    anyhow::bail!(
                        "component '{id}': 'endpoint_tls.insecure_skip_verify' and \
                         'endpoint_tls.ca_file' can't both be set -- 'insecure_skip_verify' \
                         trusts any certificate, which makes a specific trusted CA meaningless"
                    );
                }
                // Rule 40's scheme check: the endpoint's scheme selects TLS, so a block under
                // `http://` would be ignored.
                if !endpoint_tls.is_empty()
                    && !endpoint.to_ascii_lowercase().starts_with("https://")
                {
                    anyhow::bail!(
                        "component '{id}': 'endpoint_tls' is set, but 'endpoint' isn't 'https://' \
                         -- TLS is selected by the endpoint's own scheme, so an 'endpoint_tls:' \
                         block here would have no effect"
                    );
                }
            }
        }
    }

    // Rule 57: a datagram listener's `read_batch` may not exceed `MAX_READ_BATCH`
    // (`docs/adr/udp-intake-batching-and-socket-visibility.md`). `read_batch` is `recvmmsg(2)`'s
    // `vlen`, and 1024 is `UIO_MAXIOV`'s value, but the ceiling is `logit`'s: `UIO_MAXIOV` bounds
    // `msg_iovlen` within one `msghdr` (`__copy_msghdr`, `net/socket.c`), which this path sets to
    // 1, and `do_recvmmsg` loops `while (datagrams < vlen)` with no clamp (only `__sys_sendmmsg`
    // clamps a `vlen`). A larger value would be honored; the ceiling bounds the per-listener slab
    // (`read_batch x 65,507` bytes) and the shutdown-path loss.
    //
    // Rule 18 owns `read_batch: 0`. A `read_batch` above `max_datagrams` is legal: `push_many`
    // evicts or blocks per item, as `push` would. Only datagram listeners are checked, since rule
    // 17 has rejected a non-default `read_batch` everywhere else.
    for (id, component) in &components {
        if is_datagram_listener(&component.kind) && component.receive.read_batch > MAX_READ_BATCH {
            anyhow::bail!(
                "component '{id}': 'receive.read_batch' is {} -- at most {MAX_READ_BATCH} \
                 (UIO_MAXIOV's number: the ceiling logit puts on this listener's receive slab and \
                 on how many datagrams a shutdown can discard mid-push)",
                component.receive.read_batch
            );
        }
    }

    // Rule 58: a `shape` cap of `0` tracks nothing, so its cumulative gauges would read `0` (and
    // `tracking_overflow` `1`) forever (`docs/adr/shape-observer-component.md`). There is no "table
    // off" spelling: those gauges are half of what `shape` is for.
    for (id, component) in &components {
        if let ComponentKind::Shape { max_tracked_keys, max_tracked_keysets, .. } = &component.kind
        {
            for (field, value) in [
                ("max_tracked_keys", *max_tracked_keys),
                ("max_tracked_keysets", *max_tracked_keysets),
            ] {
                if value == 0 {
                    anyhow::bail!(
                        "component '{id}': shape '{field}' is 0 -- nothing would ever be tracked, \
                         so every cumulative gauge would read 0 and 'logit.shape.tracking_overflow' \
                         1 forever. Remove the component instead of capping it to nothing"
                    );
                }
            }
        }
    }

    // Rule 59: `flatten` (`docs/adr/flatten-transform.md`). Both fields `none` is a no-op; an empty
    // named list meant `none` or `all`, so the message names both; an empty name can't match (rule
    // 19's reasoning) and a repeated one is a no-op. `attributes: all`, the default, is legal.
    for (id, component) in &components {
        if let ComponentKind::Flatten { attributes, resource, .. } = &component.kind {
            let selects_nothing = |fields: &logit_config::FlattenFields| {
                matches!(
                    fields,
                    logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::None)
                )
            };
            if selects_nothing(attributes) && selects_nothing(resource) {
                anyhow::bail!(
                    "component '{id}': a flatten with 'attributes: none' and 'resource: none' \
                     can only ever be a no-op"
                );
            }
            for (field_name, fields) in [("attributes", attributes), ("resource", resource)] {
                if let logit_config::FlattenFields::Named(names) = fields {
                    if names.is_empty() {
                        anyhow::bail!(
                            "component '{id}': flatten '{field_name}' is an empty list -- write \
                             'none' to mean nothing, or 'all' to mean every nested attribute"
                        );
                    }
                    let mut seen = std::collections::HashSet::new();
                    for name in names {
                        if name.is_empty() {
                            anyhow::bail!(
                                "component '{id}': a flatten '{field_name}' entry must not be \
                                 empty -- it could never name a real attribute"
                            );
                        }
                        if !seen.insert(name.as_str()) {
                            anyhow::bail!(
                                "component '{id}': flatten '{field_name}' repeats '{name}' -- a \
                                 duplicate entry can only ever be a no-op"
                            );
                        }
                    }
                }
            }
        }
    }

    // Rule 60: `http_access` (`docs/adr/http-access-normalization.md`). Every pattern is compiled
    // here (rule 31's reasoning), and the empties are rules 19/20/54's. A route rule's shape is
    // checked here rather than by serde because an untagged enum's error names no key, which is why
    // `HttpRouteRule` is one flat struct. No "nothing configured" check: a bare `http_access` still
    // normalizes with the built-in tables.
    for (id, component) in &components {
        if let ComponentKind::HttpAccess {
            routes,
            route_other,
            user_agent_rules,
            max_length,
            redact_query,
            forwarded,
        } = &component.kind
        {
            let compile = |what: &str, pattern: &str| -> anyhow::Result<()> {
                if pattern.is_empty() {
                    anyhow::bail!(
                        "component '{id}': an http_access {what} 'match' must not be empty -- it \
                         would match everything and hide every rule after it"
                    );
                }
                ::regex::Regex::new(pattern).map(drop).map_err(|err| {
                    anyhow::anyhow!(
                        "component '{id}': http_access {what} 'match' {pattern:?} is not a valid \
                         regex: {err}"
                    )
                })
            };
            let mut builtins = std::collections::HashSet::new();
            for (index, rule) in routes.iter().enumerate() {
                let logit_config::HttpRouteRule { builtin, pattern, route } = rule;
                match (builtin, pattern, route) {
                    (Some(set), None, None) => {
                        if !builtins.insert(*set) {
                            anyhow::bail!(
                                "component '{id}': http_access routes[{index}] repeats 'builtin: \
                                 {}' -- the second can never match anything the first didn't",
                                match set {
                                    logit_config::HttpRouteSet::Assets => "assets",
                                    logit_config::HttpRouteSet::WellKnown => "well_known",
                                    logit_config::HttpRouteSet::Probes => "probes",
                                }
                            );
                        }
                    }
                    (Some(_), Some(_), _) => anyhow::bail!(
                        "component '{id}': http_access routes[{index}] has both 'builtin' and \
                         'match' -- a rule is exactly one of 'builtin', or 'match' with 'route'"
                    ),
                    (Some(_), None, Some(_)) => anyhow::bail!(
                        "component '{id}': http_access routes[{index}] has both 'builtin' and \
                         'route' -- a built-in set carries its own route value; drop 'route'"
                    ),
                    (None, Some(pattern), Some(route)) => {
                        compile(&format!("routes[{index}]"), pattern)?;
                        if route.is_empty() {
                            anyhow::bail!(
                                "component '{id}': http_access routes[{index}] 'route' must not \
                                 be empty -- it would write an empty http.route"
                            );
                        }
                    }
                    (None, Some(_), None) => anyhow::bail!(
                        "component '{id}': http_access routes[{index}] has 'match' but no \
                         'route' -- name the literal http.route a matching path gets"
                    ),
                    (None, None, Some(_)) => anyhow::bail!(
                        "component '{id}': http_access routes[{index}] has 'route' but no \
                         'match' -- add the regex over url.path that selects it"
                    ),
                    (None, None, None) => anyhow::bail!(
                        "component '{id}': http_access routes[{index}] is empty -- a rule is \
                         exactly one of 'builtin', or 'match' with 'route'"
                    ),
                }
            }
            if route_other.as_deref() == Some("") {
                anyhow::bail!(
                    "component '{id}': http_access 'route_other' must not be empty -- omit it to \
                     write no route for an unmatched path"
                );
            }
            for (index, rule) in user_agent_rules.iter().enumerate() {
                compile(&format!("user_agent_rules[{index}]"), &rule.pattern)?;
                if rule.class.is_empty() {
                    anyhow::bail!(
                        "component '{id}': http_access user_agent_rules[{index}] 'class' must not \
                         be empty -- it would write an empty user_agent.class"
                    );
                }
            }
            for (field, limit) in max_length {
                if !logit_config::CAPPED_FIELDS.iter().any(|(name, _)| name == field) {
                    let valid: Vec<&str> =
                        logit_config::CAPPED_FIELDS.iter().map(|(name, _)| *name).collect();
                    anyhow::bail!(
                        "component '{id}': http_access 'max_length' names '{field}', which \
                         http_access never caps -- valid keys: {}",
                        valid.join(", ")
                    );
                }
                if *limit == 0 {
                    anyhow::bail!(
                        "component '{id}': http_access 'max_length' for '{field}' is 0 -- that \
                         would empty the field on every event; remove the attribute downstream \
                         instead"
                    );
                }
            }
            if redact_query.iter().any(String::is_empty) {
                anyhow::bail!(
                    "component '{id}': an http_access 'redact_query' entry must not be empty -- \
                     it could never name a real query key"
                );
            }
            if forwarded.is_some_and(|f| !f.trust) {
                anyhow::bail!(
                    "component '{id}': http_access 'forwarded: {{trust: false}}' is the default \
                     -- omit the block instead"
                );
            }
        }
    }

    // Rule 61: `sample` (`docs/adr/consistent-sampling-component.md`). The rate range is rule 16's,
    // since `sampling::keep` shares `trace_is_sampled`'s NaN-keeps-everything fallback. `rate: 1`
    // keeps everything and `rate: 0` alone keeps nothing; `rate: 0` with `always_keep` is the "only
    // flagged events" mode. A non-finite override value never compares equal under `value_matches`
    // (rules 36/54); `missing:` means nothing without `key:`.
    for (id, component) in &components {
        if let ComponentKind::Sample { rate, key, missing, always_keep } = &component.kind {
            if !rate.is_finite() {
                anyhow::bail!(
                    "component '{id}': sample 'rate' must be a finite number, got {rate}"
                );
            }
            if !(0.0..=1.0).contains(rate) {
                anyhow::bail!(
                    "component '{id}': sample 'rate' must be between 0.0 and 1.0, got {rate}"
                );
            }
            if *rate == 1.0 {
                anyhow::bail!(
                    "component '{id}': a sample with 'rate: 1' keeps every event and can only \
                     ever be a no-op -- remove the component instead"
                );
            }
            if *rate == 0.0 && always_keep.is_none() {
                anyhow::bail!(
                    "component '{id}': a sample with 'rate: 0' and no 'always_keep' drops every \
                     event -- that is what 'null_out' does; add 'always_keep' to keep only flagged \
                     events"
                );
            }
            match key {
                Some(
                    logit_config::SampleKey::Attribute(name)
                    | logit_config::SampleKey::Resource(name),
                ) if name.is_empty() => {
                    anyhow::bail!(
                        "component '{id}': a sample 'key' field name must not be empty -- it \
                         could never name a real attribute"
                    );
                }
                None if missing.is_some() => {
                    anyhow::bail!(
                        "component '{id}': sample 'missing' only applies with 'key' -- without \
                         a key there is nothing to be missing"
                    );
                }
                _ => {}
            }
            if let Some(always_keep) = always_keep {
                let field = match (&always_keep.attribute, &always_keep.resource) {
                    (Some(_), Some(_)) => anyhow::bail!(
                        "component '{id}': sample 'always_keep' names both 'attribute' and \
                         'resource' -- it takes exactly one"
                    ),
                    (None, None) => anyhow::bail!(
                        "component '{id}': sample 'always_keep' needs one of 'attribute' or \
                         'resource'"
                    ),
                    (Some(name), None) | (None, Some(name)) => name,
                };
                if field.is_empty() {
                    anyhow::bail!(
                        "component '{id}': a sample 'always_keep' field name must not be empty \
                         -- it could never name a real attribute"
                    );
                }
                if let Some(logit_config::SetValue::F64(v)) = &always_keep.value {
                    if !v.is_finite() {
                        anyhow::bail!(
                            "component '{id}': sample 'always_keep.value' must be finite, got \
                             {v} -- a non-finite value can never match anything"
                        );
                    }
                }
            }
        }
    }

    // Rule 62: a tail listener's `checkpoint_path`
    // (`docs/adr/file-tailing-and-docker-json-logs.md`). Two listeners sharing one would overwrite
    // each other's offsets on every write, and each would resume from whichever wrote last. One
    // whose path is another's tmp path (`crate::atomic_write::tmp_path`) would have its checkpoint
    // truncated and renamed away by every write of the other, then load as missing and skip to
    // `read_from`. Literal paths only, like rule 35's `disk.path`. Sorted as `(path, id)` so
    // entries sharing a path are adjacent.
    let mut checkpoint_paths: Vec<(&str, &str)> = components
        .iter()
        .filter_map(|(id, component)| match &component.kind {
            ComponentKind::TailIn { tail, .. } | ComponentKind::DockerIn { tail, .. } => {
                tail.checkpoint_path.as_deref().map(|path| (path, id.as_str()))
            }
            _ => None,
        })
        .collect();
    checkpoint_paths.sort_unstable();
    for pair in checkpoint_paths.windows(2) {
        if pair[0].0 == pair[1].0 {
            anyhow::bail!(
                "components '{}' and '{}' both set 'checkpoint_path' to '{}' -- two tailing \
                 listeners sharing one checkpoint would overwrite each other's offsets",
                pair[0].1,
                pair[1].1,
                pair[0].0
            );
        }
    }
    for &(path, id) in &checkpoint_paths {
        let tmp = crate::atomic_write::tmp_path(std::path::Path::new(path));
        let tmp = tmp.to_string_lossy();
        let clash = checkpoint_paths
            .binary_search_by(|&(other, _)| other.cmp(tmp.as_ref()))
            .ok()
            .map(|at| checkpoint_paths[at]);
        if let Some((other_path, other_id)) = clash {
            anyhow::bail!(
                "component '{other_id}' sets 'checkpoint_path' to '{other_path}', which is the tmp \
                 file component '{id}' writes beside its own 'checkpoint_path' '{path}' -- every \
                 checkpoint write of '{id}' would truncate and rename away the checkpoint of \
                 '{other_id}'"
            );
        }
    }

    // Rule 63: `datadog_in` (`docs/adr/datadog-agent-and-intake-relay.md`). An empty `bind` names
    // no socket. An empty `api_keys` entry could never match, since an Agent always sends a key;
    // nor could one with leading or trailing whitespace, which HTTP strips from a header value, a
    // typo `!env` makes easy with a key file's trailing newline. An empty list is the "accept any
    // key" setting, not an error. `handshake_timeout: 0s` is rule 45's and `idle_timeout: 0s` rule
    // 53's, as on `otlp_in`.
    for (id, component) in &components {
        if let ComponentKind::DatadogIn { bind, api_keys, .. } = &component.kind {
            if bind.trim().is_empty() {
                anyhow::bail!(
                    "component '{id}': datadog_in 'bind' must not be empty -- give the \
                     'host:port' to listen on"
                );
            }
            if api_keys.iter().any(String::is_empty) {
                anyhow::bail!(
                    "component '{id}': a datadog_in 'api_keys' entry must not be empty -- it \
                     could never match a request's DD-API-KEY; omit 'api_keys' to accept any key"
                );
            }
            if api_keys.iter().any(|key| key.trim() != key) {
                anyhow::bail!(
                    "component '{id}': a datadog_in 'api_keys' entry has leading or trailing \
                     whitespace, which HTTP strips from the DD-API-KEY header, so it could never \
                     match -- check the value (a key file's trailing newline, say)"
                );
            }
        }
    }

    // Rule 64: `datadog_trace_in` (`docs/adr/datadog-agent-and-intake-relay.md`). It serves a TCP
    // listener, a Unix socket, or both, so naming neither leaves nothing to listen on, and an
    // empty `bind` names no socket, as rule 63 says for `datadog_in`. A relative `socket` would
    // resolve against whatever directory `logit` was started in, which a tracer's
    // `DD_TRACE_AGENT_URL=unix:///...` can't follow. `tls` terminates on the TCP listener only, so
    // without `bind` it could never take effect. The timeouts are rules 45/53's.
    for (id, component) in &components {
        if let ComponentKind::DatadogTraceIn { bind, socket, tls, .. } = &component.kind {
            if bind.is_none() && socket.is_none() {
                anyhow::bail!(
                    "component '{id}': datadog_trace_in needs 'bind', 'socket', or both -- give \
                     the 'host:port' (the Agent's is ':8126') or the Unix socket path to listen on"
                );
            }
            if bind.as_deref().is_some_and(|bind| bind.trim().is_empty()) {
                anyhow::bail!(
                    "component '{id}': datadog_trace_in 'bind' must not be empty -- give the \
                     'host:port' to listen on, or omit it to serve only 'socket'"
                );
            }
            if let Some(socket) = socket {
                if !std::path::Path::new(socket).is_absolute() {
                    anyhow::bail!(
                        "component '{id}': datadog_trace_in 'socket' must be an absolute path, \
                         got '{socket}' -- tracers name it as unix:///<path>"
                    );
                }
            }
            if tls.is_some() && bind.is_none() {
                anyhow::bail!(
                    "component '{id}': datadog_trace_in 'tls' needs 'bind' -- TLS terminates on \
                     the TCP listener, and the Unix socket is always plaintext"
                );
            }
        }
    }

    let mut resolved = HashMap::with_capacity(components.len());
    for (id, component) in components {
        // Slot order is fixed here, once (see [`targets_of`]).
        let node_targets: Vec<String> =
            targets_of(&component).into_iter().map(String::from).collect();
        let Component { sources, buffer, receive, kind, targets: _ } = component;
        let node_consumers = consumers.remove(&id).unwrap_or_default();
        resolved.insert(
            id,
            ResolvedComponent {
                sources,
                consumers: node_consumers,
                targets: node_targets,
                kind,
                buffer,
                receive,
            },
        );
    }

    Ok(Graph { components: resolved, topological_order })
}

/// Rules 17/18/57's datagram predicate: the kinds the UDP listener driver backs
/// (`logit-inputs::udp::UdpListener`, `docs/adr/decoupled-listener-io.md`). An explicit list, not
/// [`Role`], so a new listener kind rejects `receive:` until it is wired to a driver.
fn is_datagram_listener(kind: &ComponentKind) -> bool {
    matches!(
        kind,
        ComponentKind::CollectdIn { .. }
            // UDP only: under TCP these run on the stream driver, which has no `ReceiveQueue` (see
            // [`is_stream_listener`]).
            | ComponentKind::SyslogIn { transport: SyslogTransport::Udp, .. }
            | ComponentKind::GraphiteIn { transport: GraphiteTransport::Udp, .. }
            | ComponentKind::StatsdIn { transport: StatsdTransport::Udp, .. }
    )
}

/// Rules 17/18's stream predicate: the kinds on the shared stream driver
/// (`logit_inputs::tcp::TcpListener`), which are `syslog_in`/`graphite_in`/`statsd_in` under TCP.
/// Such a listener assembles batches per connection but has no receive queue: TCP flow control is
/// the backpressure, so a blocked `Fanout::send` stops the socket being read and the peer's window
/// closes. Batch bounds are per connection, so N connections can hold N × `batch_max_events` in
/// flight (`docs/adr/syslog-tcp-ingress-and-tls.md`). An explicit list, like
/// [`is_datagram_listener`].
fn is_stream_listener(kind: &ComponentKind) -> bool {
    matches!(
        kind,
        ComponentKind::SyslogIn { transport: SyslogTransport::Tcp, .. }
            | ComponentKind::GraphiteIn { transport: GraphiteTransport::Tcp, .. }
            | ComponentKind::StatsdIn { transport: StatsdTransport::Tcp, .. }
    )
}

/// Rules 17/18/28's tail predicate: the kinds the file-tailing driver backs
/// (`logit_inputs::tail::Tailer`, `docs/adr/file-tailing-and-docker-json-logs.md`). The tailed file
/// is its own durable buffer, so there is no receive queue. An explicit list, like
/// [`is_datagram_listener`].
fn is_tail_listener(kind: &ComponentKind) -> bool {
    matches!(kind, ComponentKind::TailIn { .. } | ComponentKind::DockerIn { .. })
}

/// Whether `name` is a placeholder `generate_in` substitutes: `seq` (the 0-based event counter) or
/// `seq%N` with `N >= 1` (that counter modulo `N`).
///
/// A bare-`&str` predicate so `logit-inputs`' `Template::compile` resolver, which can't depend on
/// `logit-config` (`docs/design/pipeline-graph.md`'s "Crate layout"), can mirror it by eye. `N`
/// must be ASCII digits only: `u64::from_str` would also accept `+5`, a second spelling of `5`. `N
/// == 0` is rejected here rather than dividing by zero at render time.
fn generate_var_is_valid(name: &str) -> bool {
    if name == "seq" {
        return true;
    }
    match name.strip_prefix("seq%") {
        Some(modulus) => {
            !modulus.is_empty()
                && modulus.bytes().all(|byte| byte.is_ascii_digit())
                && modulus.parse::<u64>().is_ok_and(|modulus| modulus >= 1)
        }
        None => false,
    }
}

/// What becomes of one `generate_in` template's rendering, which is what decides whether an
/// *unbounded* placeholder is legal in it.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Rendering {
    /// Copied onto the event and freed with it: `event.log`, an attribute value, a resource
    /// value. An unbounded `{seq}` costs one allocation per render and nothing beyond it.
    Copied,
    /// Interned: `event.metric.name`. `logit_core::interner` is monotonic -- a `Symbol` is never
    /// removed (`docs/design/memory.md` §4) -- so an unbounded set of renderings is an unbounded,
    /// process-lifetime leak rather than a cardinality knob.
    Interned,
}

/// Rule 42's per-field template check: it must parse, and every placeholder in it must be one
/// [`generate_var_is_valid`] recognizes -- plus, for an [`Rendering::Interned`] field, must be
/// *bounded*. `field` is the dotted config path (`event.log`, `resource.service.name`) so the
/// error names what to go and fix.
fn check_generate_template(
    id: &str,
    field: &str,
    raw: &str,
    rendering: Rendering,
) -> anyhow::Result<()> {
    let template = logit_core::template::parse(raw)
        .map_err(|err| anyhow::anyhow!("component '{id}': '{field}' is not a template: {err}"))?;
    for var in template.vars() {
        if !generate_var_is_valid(var) {
            anyhow::bail!(
                "component '{id}': '{field}' names the placeholder '{{{var}}}', which generate_in \
                 doesn't substitute -- only '{{seq}}' and '{{seq%N}}' (N at least 1)"
            );
        }
        if rendering == Rendering::Interned && var == "seq" {
            anyhow::bail!(
                "component '{id}': '{field}' may not use '{{seq}}' -- a metric name is interned \
                 for the life of the process, so an unbounded one would intern a fresh name per \
                 generated event. Use '{{seq%N}}' for a bounded set of N names"
            );
        }
    }
    Ok(())
}

/// Rule 26's glob check: `*` is allowed only in `path`'s final `/`-separated component, the only
/// one `logit_inputs::tail::pattern::PathPattern` treats as a pattern; an earlier one would never
/// match.
fn check_tail_glob(id: &str, path: &str) -> anyhow::Result<()> {
    let Some((parent, _last)) = path.rsplit_once('/') else {
        // No `/` at all isn't this rule's concern: an absolute path is a deployment convention, not
        // a validation rule.
        return Ok(());
    };
    if parent.contains('*') {
        anyhow::bail!(
            "component '{id}': 'paths' entry {path:?} uses '*' outside the final path \
             component -- only a trailing '<dir>/<prefix>*<suffix>' pattern is supported"
        );
    }
    Ok(())
}

/// Rule 5: Kahn's algorithm over the component graph. Returns a listener-first order, or an error
/// naming one concrete cycle.
fn topological_order(components: &HashMap<String, Component>) -> anyhow::Result<Vec<String>> {
    // Two edge kinds: a `sources` entry and a router -> target direction
    // (`docs/adr/target-components.md`). Both carry data, so both count toward indegree, which is
    // what catches `router -> target -> .. -> router`. `incoming` is kept alongside indegree
    // because the cycle-recovery walk traverses both kinds backwards, and a target has no
    // `sources`.
    //
    // An id naming no component is skipped: rule 2 has rejected an unresolved `sources` entry, and
    // an unresolved or self-directed target is rule 48's, which runs after this, and whose message
    // is clearer than a cycle report.
    let mut incoming: HashMap<&str, Vec<&str>> =
        components.keys().map(|id| (id.as_str(), Vec::new())).collect();
    let mut outgoing: HashMap<&str, Vec<&str>> =
        components.keys().map(|id| (id.as_str(), Vec::new())).collect();
    for (id, c) in components {
        for producer in c.sources.iter().map(String::as_str) {
            if components.contains_key(producer) {
                incoming.get_mut(id.as_str()).expect("every id is in incoming").push(producer);
                outgoing.get_mut(producer).expect("checked above").push(id.as_str());
            }
        }
        for target in targets_of(c) {
            if components.contains_key(target) && target != id.as_str() {
                incoming.get_mut(target).expect("checked above").push(id.as_str());
                outgoing.get_mut(id.as_str()).expect("every id is in outgoing").push(target);
            }
        }
    }
    let mut indegree: HashMap<&str, usize> =
        incoming.iter().map(|(&id, sources)| (id, sources.len())).collect();

    let mut ready: Vec<&str> =
        indegree.iter().filter(|(_, &deg)| deg == 0).map(|(&id, _)| id).collect();
    ready.sort_unstable();
    let mut queue: VecDeque<&str> = ready.into();

    let mut order = Vec::with_capacity(components.len());
    while let Some(id) = queue.pop_front() {
        order.push(id.to_string());
        let mut newly_ready: Vec<&str> = Vec::new();
        for &next in &outgoing[id] {
            let deg = indegree.get_mut(next).expect("every id is in indegree");
            *deg -= 1;
            if *deg == 0 {
                newly_ready.push(next);
            }
        }
        newly_ready.sort_unstable();
        queue.extend(newly_ready);
    }

    if order.len() != components.len() {
        // Residual indegree marks the cycle *and* everything downstream of it. Naming that set
        // would blame downstream victims, so walk `incoming` backwards within it to one real cycle.
        // Every stuck node has a stuck source, so the walk can't dead-end and must revisit a node
        // within `stuck.len()` steps.
        let stuck: BTreeSet<&str> =
            indegree.iter().filter(|(_, &deg)| deg > 0).map(|(&id, _)| id).collect();
        let mut path: Vec<&str> = Vec::new();
        let mut seen: HashMap<&str, usize> = HashMap::new();
        let mut current = *stuck.iter().next().expect("order.len() < components.len()");
        let start = loop {
            if let Some(&at) = seen.get(current) {
                break at;
            }
            seen.insert(current, path.len());
            path.push(current);
            current = incoming[current]
                .iter()
                .copied()
                .filter(|s| stuck.contains(s))
                .min()
                .expect("a stuck node always has a stuck source");
        };
        // `path` was built walking against the flow (consumer -> source); reverse the cycle
        // portion so the message reads as data flow. The discarded prefix `path[..start]` is the
        // tail that led into the cycle, not part of it.
        let mut cycle: Vec<&str> = path[start..].to_vec();
        cycle.reverse();
        // Rotate so the smallest id leads, making the message independent of where the walk
        // started.
        let min_idx = cycle.iter().enumerate().min_by_key(|(_, id)| *id).map(|(i, _)| i).expect(
            "cycle is non-empty: the loop above always pushes at least one node before repeating",
        );
        cycle.rotate_left(min_idx);
        anyhow::bail!("component graph has a cycle: {} -> {}", cycle.join(" -> "), cycle[0]);
    }
    Ok(order)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap as Map;

    fn cfg(components: Vec<(&str, Vec<&str>, ComponentKind)>) -> Config {
        let mut map = Map::new();
        for (id, sources, kind) in components {
            map.insert(
                id.to_string(),
                Component {
                    sources: sources.into_iter().map(String::from).collect(),
                    targets: Vec::new(),
                    buffer: BufferConfig::default(),
                    receive: ReceiveConfig::default(),
                    kind,
                },
            );
        }
        Config { components: map, ..Default::default() }
    }

    /// Same as [`cfg`], but with an explicit `buffer` on one component -- for rule 14's tests.
    fn cfg_with_buffer(components: Vec<(&str, Vec<&str>, ComponentKind, BufferConfig)>) -> Config {
        let mut map = Map::new();
        for (id, sources, kind, buffer) in components {
            map.insert(
                id.to_string(),
                Component {
                    sources: sources.into_iter().map(String::from).collect(),
                    targets: Vec::new(),
                    buffer,
                    receive: ReceiveConfig::default(),
                    kind,
                },
            );
        }
        Config { components: map, ..Default::default() }
    }

    /// Same as [`cfg`], but with an explicit `receive` on one component, for rules 17/18's tests.
    fn cfg_with_receive(
        components: Vec<(&str, Vec<&str>, ComponentKind, ReceiveConfig)>,
    ) -> Config {
        let mut map = Map::new();
        for (id, sources, kind, receive) in components {
            map.insert(
                id.to_string(),
                Component {
                    sources: sources.into_iter().map(String::from).collect(),
                    targets: Vec::new(),
                    buffer: BufferConfig::default(),
                    receive,
                    kind,
                },
            );
        }
        Config { components: map, ..Default::default() }
    }

    /// One raw `Component`, for tests that call
    /// [`targets_of`]/[`target_edges`]/[`topological_order`] directly rather than through
    /// [`resolve`].
    fn component(sources: Vec<&str>, targets: Vec<&str>, kind: ComponentKind) -> Component {
        Component {
            sources: sources.into_iter().map(String::from).collect(),
            targets: targets.into_iter().map(String::from).collect(),
            buffer: BufferConfig::default(),
            receive: ReceiveConfig::default(),
            kind,
        }
    }

    /// The `components` map [`topological_order`] takes, from `(id, sources, targets, kind)`
    /// tuples -- `cfg`'s shape with a `targets:` list, since `cfg` always builds an empty one.
    fn components_map(
        components: Vec<(&str, Vec<&str>, Vec<&str>, ComponentKind)>,
    ) -> HashMap<String, Component> {
        components
            .into_iter()
            .map(|(id, sources, targets, kind)| (id.to_string(), component(sources, targets, kind)))
            .collect()
    }

    /// [`cfg`] for a config whose components carry `targets:`.
    fn cfg_with_targets(components: Vec<(&str, Vec<&str>, Vec<&str>, ComponentKind)>) -> Config {
        Config { components: components_map(components), ..Default::default() }
    }

    fn listener() -> ComponentKind {
        ComponentKind::StatsdIn {
            bind: "127.0.0.1:0".to_string(),
            transport: StatsdTransport::default(),
            tls: None,
            handshake_timeout: default_handshake_timeout(),
            idle_timeout: None,
        }
    }

    fn target() -> ComponentKind {
        ComponentKind::Target {}
    }

    fn keep(fields: Vec<&str>) -> ComponentKind {
        ComponentKind::Keep { fields: fields.into_iter().map(String::from).collect() }
    }

    fn route(by: logit_config::RouteBy, routes: &[(&str, &str)]) -> ComponentKind {
        ComponentKind::Route {
            by,
            routes: routes
                .iter()
                .map(|(value, target)| (value.to_string(), target.to_string()))
                .collect(),
        }
    }

    fn by_attribute(key: &str) -> logit_config::RouteBy {
        logit_config::RouteBy::Attribute(key.to_string())
    }

    fn tail_in(paths: Vec<&str>) -> ComponentKind {
        ComponentKind::TailIn {
            paths: paths.into_iter().map(String::from).collect(),
            tail: logit_config::TailOptions::default(),
        }
    }

    fn docker_in(containers: Vec<&str>, discover: bool) -> ComponentKind {
        ComponentKind::DockerIn {
            root: "/var/lib/docker/containers".to_string(),
            containers: containers.into_iter().map(String::from).collect(),
            discover,
            labels: Vec::new(),
            tail: logit_config::TailOptions::default(),
        }
    }

    fn lua() -> ComponentKind {
        ComponentKind::Lua { script: "".to_string(), interval: None }
    }

    fn json() -> ComponentKind {
        ComponentKind::Json { skip_to_brace: false, invalid_utf8: Default::default() }
    }

    fn logfmt() -> ComponentKind {
        ComponentKind::Logfmt { bare_keys: false }
    }

    fn kv(pair_sep: &str, kv_sep: &str) -> ComponentKind {
        ComponentKind::Kv {
            pair_sep: pair_sep.to_string(),
            kv_sep: kv_sep.to_string(),
            bare_keys: false,
        }
    }

    fn metric_spec(name: &str, field: Option<&str>) -> logit_config::MetricSpec {
        logit_config::MetricSpec {
            name: name.to_string(),
            field: field.map(String::from),
            unit: None,
        }
    }

    fn sink() -> ComponentKind {
        ComponentKind::InfluxDbOut {
            url: "http://localhost:8086".to_string(),
            org: "org".to_string(),
            bucket: "bucket".to_string(),
            token: "TOKEN".to_string(),
        }
    }

    fn statsd_out(max_packet_bytes: u64) -> ComponentKind {
        ComponentKind::StatsdOut {
            endpoint: "127.0.0.1:8125".to_string(),
            transport: logit_config::StatsdTransport::default(),
            format: logit_config::StatsdFormat::default(),
            relative_gauges: false,
            max_packet_bytes,
            connect_timeout: Duration::from_secs(5),
            tls: None,
        }
    }

    fn collectd_out(max_packet_bytes: u64) -> ComponentKind {
        ComponentKind::CollectdOut {
            endpoint: "127.0.0.1:25826".to_string(),
            max_packet_bytes,
            hostname: None,
        }
    }

    fn graphite_out(
        transport: GraphiteTransport,
        protocol: GraphiteProtocol,
        max_packet_bytes: u64,
        max_frame_bytes: u64,
        connect_timeout: Duration,
    ) -> ComponentKind {
        ComponentKind::GraphiteOut {
            endpoint: "127.0.0.1:2003".to_string(),
            transport,
            protocol,
            tags: logit_config::GraphiteTags::default(),
            multi_value: logit_config::GraphiteMultiValue::default(),
            max_packet_bytes,
            max_frame_bytes,
            connect_timeout,
        }
    }

    /// A scrape-mode `prometheus_in` with every other field at its default -- the shape rules 40
    /// and 55 both read. `prometheus_in_bind` below is its receiver-mode twin.
    fn prometheus_in(targets: Vec<&str>) -> ComponentKind {
        ComponentKind::PrometheusIn {
            scrape_targets: targets.into_iter().map(String::from).collect(),
            interval: default_prometheus_scrape_interval(),
            timeout: default_prometheus_scrape_timeout(),
            headers: Map::new(),
            scrape_tls: logit_config::TlsClientConfig::default(),
            bind: None,
            path: default_prometheus_write_path(),
            bind_tls: None,
            idle_timeout: None,
            metadata_cache: MetadataCacheConfig::default(),
        }
    }

    fn prometheus_in_bind(bind: &str) -> ComponentKind {
        ComponentKind::PrometheusIn {
            scrape_targets: Vec::new(),
            interval: default_prometheus_scrape_interval(),
            timeout: default_prometheus_scrape_timeout(),
            headers: Map::new(),
            scrape_tls: logit_config::TlsClientConfig::default(),
            bind: Some(bind.to_string()),
            path: default_prometheus_write_path(),
            bind_tls: None,
            idle_timeout: None,
            metadata_cache: MetadataCacheConfig::default(),
        }
    }

    fn prometheus_in_with_headers(targets: Vec<&str>, headers: Vec<(&str, &str)>) -> ComponentKind {
        let mut kind = prometheus_in(targets);
        if let ComponentKind::PrometheusIn { headers: slot, .. } = &mut kind {
            *slot = headers.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        }
        kind
    }

    fn prometheus_in_with_tls(
        targets: Vec<&str>,
        tls: logit_config::TlsClientConfig,
    ) -> ComponentKind {
        let mut kind = prometheus_in(targets);
        if let ComponentKind::PrometheusIn { scrape_tls, .. } = &mut kind {
            *scrape_tls = tls;
        }
        kind
    }

    fn file_out(rotate: logit_config::RotateConfig) -> ComponentKind {
        ComponentKind::FileOut {
            path: "events.log".to_string(),
            rotate,
            format: StreamFormat::default(),
            compression: Compression::default(),
        }
    }

    fn file_out_with_format(format: StreamFormat, compression: Compression) -> ComponentKind {
        ComponentKind::FileOut {
            path: "events.log".to_string(),
            rotate: logit_config::RotateConfig {
                max_bytes: Some(1024),
                interval: None,
                max_files: 5,
            },
            format,
            compression,
        }
    }

    fn stdio_out_with_format(format: StreamFormat, compression: Compression) -> ComponentKind {
        ComponentKind::StdioOut {
            target: logit_config::StdioTarget::default(),
            format,
            compression,
        }
    }

    /// The perf harness's generator, at its own defaults: unbounded, `batch: 100`, unthrottled,
    /// and no event template at all -- the cheapest event `generate_in` can produce.
    fn generate_in() -> ComponentKind {
        generate_in_full(None, 100, None, logit_config::GenerateEvent::default(), Vec::new())
    }

    /// Rule 42's three count bounds, with an otherwise-default generator.
    fn generate_in_with_counts(
        count: Option<u64>,
        batch: usize,
        rate: Option<u64>,
    ) -> ComponentKind {
        generate_in_full(count, batch, rate, logit_config::GenerateEvent::default(), Vec::new())
    }

    /// Rule 42's template and metric checks, with otherwise-default counts.
    fn generate_in_with_event(event: logit_config::GenerateEvent) -> ComponentKind {
        generate_in_full(None, 100, None, event, Vec::new())
    }

    /// Rule 42's `resource` key/template checks.
    fn generate_in_with_resource(resource: Vec<(&str, &str)>) -> ComponentKind {
        generate_in_full(None, 100, None, logit_config::GenerateEvent::default(), resource)
    }

    fn generate_in_full(
        count: Option<u64>,
        batch: usize,
        rate: Option<u64>,
        event: logit_config::GenerateEvent,
        resource: Vec<(&str, &str)>,
    ) -> ComponentKind {
        ComponentKind::GenerateIn {
            count,
            batch,
            rate,
            event,
            resource: resource.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
        }
    }

    /// A [`logit_config::GenerateEvent`] spelled out field by field, so a rule-42 test reads as
    /// the config it rejects.
    fn generate_event(
        log: Option<&str>,
        attributes: Vec<(&str, &str)>,
        metric: Option<logit_config::GenerateMetric>,
    ) -> logit_config::GenerateEvent {
        logit_config::GenerateEvent {
            log: log.map(String::from),
            attributes: attributes
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            metric,
        }
    }

    fn generate_metric(name: &str, value: f64) -> logit_config::GenerateMetric {
        logit_config::GenerateMetric {
            name: name.to_string(),
            kind: logit_config::GenerateMetricKind::default(),
            value,
        }
    }

    fn null_out() -> ComponentKind {
        ComponentKind::NullOut {}
    }

    /// `Result::expect_err` needs `Debug` on the `Ok` side, and `Graph` isn't `Debug` (it embeds
    /// `ComponentKind`, which isn't either). `logit-cli::pipeline` has its own helper for the same
    /// reason.
    fn expect_err(config: Config) -> String {
        match resolve(config) {
            Ok(_) => panic!("expected resolution to fail"),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn empty_config_is_rejected() {
        let err = expect_err(cfg(vec![]));
        assert!(err.contains("no components"), "got: {err}");
    }

    #[test]
    fn unknown_source_is_rejected() {
        let err = expect_err(cfg(vec![("out", vec!["missing"], sink())]));
        assert!(err.contains("unknown source 'missing'"), "got: {err}");
    }

    #[test]
    fn self_reference_is_rejected() {
        let err = expect_err(cfg(vec![("a", vec!["a"], lua())]));
        assert!(err.contains("lists itself as a source"), "got: {err}");
    }

    /// Rule 4: a repeated source would give that source's `Fanout` two senders into one inbox,
    /// doubling every batch.
    #[test]
    fn duplicate_source_within_one_component_is_rejected() {
        let err =
            expect_err(cfg(vec![("in", vec![], listener()), ("out", vec!["in", "in"], sink())]));
        assert!(err.contains("lists source 'in' more than once"), "got: {err}");
    }

    #[test]
    fn two_node_cycle_is_rejected() {
        let err = expect_err(cfg(vec![("a", vec!["b"], lua()), ("b", vec!["a"], lua())]));
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn longer_cycle_is_rejected() {
        let err = expect_err(cfg(vec![
            ("a", vec!["c"], lua()),
            ("b", vec!["a"], lua()),
            ("c", vec!["b"], lua()),
        ]));
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn a_cycle_is_reported_as_a_concrete_path() {
        let err = expect_err(cfg(vec![
            ("a", vec!["c"], lua()),
            ("b", vec!["a"], lua()),
            ("c", vec!["b"], lua()),
        ]));
        assert!(err.contains("cycle: a -> b -> c -> a"), "got: {err}");
    }

    /// Rule 5's error names only the cycle, not `out`, which is merely downstream of it (residual
    /// indegree marks both).
    #[test]
    fn a_cycle_error_does_not_name_components_downstream_of_it() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("a", vec!["in", "b"], lua()),
            ("b", vec!["a"], lua()),
            ("out", vec!["b"], sink()),
        ]));
        assert!(err.contains("cycle: a -> b -> a"), "got: {err}");
        assert!(!err.contains("out"), "got: {err}");
        assert!(!err.contains("in"), "got: {err}");
    }

    #[test]
    fn listener_with_sources_is_rejected() {
        let err =
            expect_err(cfg(vec![("in", vec!["other"], listener()), ("other", vec![], listener())]));
        assert!(err.contains("listener") && err.contains("cannot declare sources"), "got: {err}");
    }

    #[test]
    fn sink_named_as_another_components_source_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], sink()),
            ("other", vec!["out"], lua()),
        ]));
        assert!(err.contains("is a sink and cannot be listed as a source"), "got: {err}");
    }

    #[test]
    fn transform_with_no_consumers_is_rejected() {
        let err = expect_err(cfg(vec![("in", vec![], listener()), ("orphan", vec!["in"], lua())]));
        assert!(err.contains("no consumers"), "got: {err}");
    }

    #[test]
    fn listener_with_no_consumers_is_rejected() {
        let err = expect_err(cfg(vec![("in", vec![], listener())]));
        assert!(err.contains("no consumers"), "got: {err}");
    }

    #[test]
    fn a_json_component_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("parse", vec!["in"], json()),
            ("out", vec!["parse"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["parse"].role(), Role::Transform);
    }

    #[test]
    fn a_logfmt_component_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("parse", vec!["in"], logfmt()),
            ("out", vec!["parse"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["parse"].role(), Role::Transform);
    }

    #[test]
    fn a_kv_component_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("parse", vec!["in"], kv("&", "=")),
            ("out", vec!["parse"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["parse"].role(), Role::Transform);
    }

    #[test]
    fn kv_with_an_empty_pair_sep_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("parse", vec!["in"], kv("", "=")),
            ("out", vec!["parse"], sink()),
        ]));
        assert!(err.contains("pair_sep") && err.contains("empty"), "got: {err}");
    }

    #[test]
    fn kv_with_an_empty_kv_sep_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("parse", vec!["in"], kv("&", "")),
            ("out", vec!["parse"], sink()),
        ]));
        assert!(err.contains("kv_sep") && err.contains("empty"), "got: {err}");
    }

    #[test]
    fn kv_with_identical_separators_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("parse", vec!["in"], kv("=", "=")),
            ("out", vec!["parse"], sink()),
        ]));
        assert!(err.contains("must differ"), "got: {err}");
    }

    #[test]
    fn kv_with_a_kv_sep_containing_pair_sep_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("parse", vec!["in"], kv("=", "==")),
            ("out", vec!["parse"], sink()),
        ]));
        assert!(err.contains("must not contain"), "got: {err}");
    }

    #[test]
    fn zero_interval_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "agg",
                vec!["in"],
                ComponentKind::Aggregate {
                    interval: Duration::ZERO,
                    temporality: logit_config::AggregateTemporality::default(),
                    series_retention: 5,
                    max_retained_series: 10_000,
                    distributions: logit_config::Distributions::default(),
                    max_samples_per_series: 1000,
                    sets: logit_config::Sets::default(),
                    max_set_members_per_series: 1000,
                },
            ),
            ("out", vec!["agg"], sink()),
        ]));
        assert!(err.contains("0s"), "got: {err}");
    }

    #[test]
    fn a_well_formed_chain_resolves() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("enrich", vec!["in"], lua()),
            ("out", vec!["enrich"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.topological_order, vec!["in", "enrich", "out"]);
        assert_eq!(graph.components["in"].role(), Role::Listener);
        assert_eq!(graph.components["enrich"].role(), Role::Transform);
        assert_eq!(graph.components["out"].role(), Role::Sink);
        assert_eq!(graph.components["in"].consumers, vec!["enrich"]);
    }

    /// A sink fed by two independent branches resolves: sharing needs no rule.
    #[test]
    fn a_sink_shared_by_two_branches_is_accepted() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("branch_a", vec!["in"], lua()),
            ("branch_b", vec!["in"], lua()),
            ("out", vec!["branch_a", "branch_b"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["in"].consumers.len(), 2);
        assert_eq!(graph.components["out"].sources.len(), 2);
    }

    #[test]
    fn a_kv_metrics_with_no_lists_configured_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "derive",
                vec!["in"],
                ComponentKind::KvMetrics {
                    counters: vec![],
                    gauges: vec![],
                    distributions: vec![],
                },
            ),
            ("out", vec!["derive"], sink()),
        ]));
        assert!(err.contains("no-op"), "got: {err}");
    }

    #[test]
    fn a_kv_metrics_distribution_with_no_field_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "derive",
                vec!["in"],
                ComponentKind::KvMetrics {
                    counters: vec![],
                    gauges: vec![],
                    distributions: vec![metric_spec("nginx.request_time", None)],
                },
            ),
            ("out", vec!["derive"], sink()),
        ]));
        assert!(err.contains("distribution entry requires a 'field'"), "got: {err}");
    }

    #[test]
    fn a_kv_metrics_counter_with_an_empty_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "derive",
                vec!["in"],
                ComponentKind::KvMetrics {
                    counters: vec![metric_spec("", None)],
                    gauges: vec![],
                    distributions: vec![],
                },
            ),
            ("out", vec!["derive"], sink()),
        ]));
        assert!(err.contains("non-empty 'name'"), "got: {err}");
    }

    #[test]
    fn a_kv_metrics_gauge_with_an_empty_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "derive",
                vec!["in"],
                ComponentKind::KvMetrics {
                    counters: vec![],
                    gauges: vec![metric_spec("", Some("status"))],
                    distributions: vec![],
                },
            ),
            ("out", vec!["derive"], sink()),
        ]));
        assert!(err.contains("non-empty 'name'"), "got: {err}");
    }

    #[test]
    fn a_kv_metrics_distribution_with_an_empty_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "derive",
                vec!["in"],
                ComponentKind::KvMetrics {
                    counters: vec![],
                    gauges: vec![],
                    distributions: vec![metric_spec("", Some("request_time"))],
                },
            ),
            ("out", vec!["derive"], sink()),
        ]));
        assert!(err.contains("non-empty 'name'"), "got: {err}");
    }

    #[test]
    fn a_has_signal_with_an_empty_signals_list_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasSignal { signals: vec![], mode: logit_config::MatchMode::AnyOf },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(err.contains("must name at least one signal"), "got: {err}");
    }

    #[test]
    fn a_keep_signals_with_an_empty_signals_list_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("filter", vec!["in"], ComponentKind::KeepSignals { signals: vec![] }),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(err.contains("must name at least one signal"), "got: {err}");
        assert!(
            err.contains("keeps nothing"),
            "a keep_signals with an empty list is the black-hole shape (drops every event), \
             not the no-op shape -- got: {err}"
        );
    }

    #[test]
    fn a_drop_signals_naming_all_three_signals_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::DropSignals {
                    signals: vec![
                        logit_config::Signal::Logs,
                        logit_config::Signal::Metrics,
                        logit_config::Signal::Traces,
                    ],
                },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(err.contains("names all three signals"), "got: {err}");
        assert!(
            err.contains("drops everything"),
            "a drop_signals naming all three signals is the black-hole shape (drops every \
             event), not the no-op shape -- got: {err}"
        );
    }

    #[test]
    fn a_keep_signals_naming_all_three_signals_is_rejected_as_a_no_op_not_a_black_hole() {
        // The inverse of the drop_signals case above: for keep_signals (an allowlist), naming
        // all three signals keeps everything -- a no-op, not the "drop every event" black hole.
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::KeepSignals {
                    signals: vec![
                        logit_config::Signal::Logs,
                        logit_config::Signal::Metrics,
                        logit_config::Signal::Traces,
                    ],
                },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(err.contains("names all three signals"), "got: {err}");
        assert!(
            err.contains("keeps everything") && err.contains("no-op"),
            "a keep_signals naming all three signals is the no-op shape (keeps every event \
             untouched), not the black-hole shape -- got: {err}"
        );
    }

    #[test]
    fn a_drop_signals_with_an_empty_signals_list_is_rejected_as_a_no_op_not_a_black_hole() {
        // The inverse of the keep_signals-empty case: for drop_signals (a denylist), an empty
        // list drops nothing -- a no-op, not the "drop every event" black hole.
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("filter", vec!["in"], ComponentKind::DropSignals { signals: vec![] }),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(err.contains("must name at least one signal"), "got: {err}");
        assert!(
            err.contains("drops nothing") && err.contains("no-op"),
            "a drop_signals with an empty list is the no-op shape (forwards every event \
             untouched), not the black-hole shape -- got: {err}"
        );
    }

    #[test]
    fn a_has_signal_naming_all_three_signals_resolves_fine() {
        // Unlike `keep_signals`/`drop_signals`, `has_signal` naming all three signals is a real,
        // permissive filter under `mode: only` ("forward anything with a payload"), not a no-op.
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasSignal {
                    signals: vec![
                        logit_config::Signal::Logs,
                        logit_config::Signal::Metrics,
                        logit_config::Signal::Traces,
                    ],
                    mode: logit_config::MatchMode::Only,
                },
            ),
            ("out", vec!["filter"], sink()),
        ]))
        .expect("should resolve");
    }

    #[test]
    fn a_set_with_an_empty_key_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "tag",
                vec!["in"],
                ComponentKind::Set {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "".to_string(),
                        logit_config::SetValue::Str("x".to_string()),
                    )]),
                },
            ),
            ("out", vec!["tag"], sink()),
        ]));
        assert!(err.contains("key must not be empty"), "got: {err}");
    }

    #[test]
    fn a_has_attributes_with_neither_map_configured_is_rejected_as_a_no_op() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasAttributes {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::new(),
                },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(
            err.contains("no-op") && err.contains("forwards every event untouched"),
            "a has_attributes with nothing configured is the no-op shape (matches every event), \
             not the black-hole shape -- got: {err}"
        );
    }

    #[test]
    fn a_drop_attributes_with_neither_map_configured_is_rejected_as_a_black_hole() {
        // The inverse of the has_attributes case above: nothing configured matches every event
        // too, but for drop_attributes that means dropping every one of them.
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::DropAttributes {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::new(),
                },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(
            err.contains("drop every one of them"),
            "a drop_attributes with nothing configured is the black-hole shape (drops every \
             event), not the no-op shape -- got: {err}"
        );
    }

    #[test]
    fn a_has_provenance_with_neither_field_configured_is_rejected_as_a_no_op() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasProvenance { origin: vec![], previous: vec![] },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(
            err.contains("no-op") && err.contains("forwards every event untouched"),
            "a has_provenance with nothing configured is the no-op shape (matches every batch), \
             not the black-hole shape -- got: {err}"
        );
    }

    #[test]
    fn a_drop_provenance_with_neither_field_configured_is_rejected_as_a_black_hole() {
        // The inverse of the has_provenance case above: nothing configured matches every batch
        // too, but for drop_provenance that means dropping every one of them.
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::DropProvenance { origin: vec![], previous: vec![] },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(
            err.contains("drop every event"),
            "a drop_provenance with nothing configured is the black-hole shape (drops every \
             event), not the no-op shape -- got: {err}"
        );
    }

    #[test]
    fn a_has_provenance_with_an_empty_origin_entry_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasProvenance { origin: vec!["".to_string()], previous: vec![] },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(err.contains("entry must not be empty"), "got: {err}");
    }

    #[test]
    fn a_drop_provenance_with_a_duplicate_previous_entry_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::DropProvenance {
                    origin: vec![],
                    previous: vec!["parse_json".to_string(), "parse_json".to_string()],
                },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(err.contains("is repeated"), "got: {err}");
    }

    #[test]
    fn a_has_provenance_with_only_an_origin_list_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasProvenance {
                    origin: vec!["nginx_in".to_string(), "syslog_in".to_string()],
                    previous: vec![],
                },
            ),
            ("out", vec!["filter"], sink()),
        ]))
        .unwrap();
    }

    #[test]
    fn a_has_attributes_with_an_empty_key_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasAttributes {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "".to_string(),
                        logit_config::SetValue::Str("x".to_string()),
                    )]),
                },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(err.contains("key must not be empty"), "got: {err}");
    }

    #[test]
    fn a_drop_attributes_with_a_non_finite_value_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::DropAttributes {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "ratio".to_string(),
                        logit_config::SetValue::F64(f64::NAN),
                    )]),
                },
            ),
            ("out", vec!["filter"], sink()),
        ]));
        assert!(err.contains("must be a finite number"), "got: {err}");
    }

    #[test]
    fn a_has_attributes_with_only_a_resource_map_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasAttributes {
                    resource: std::collections::BTreeMap::from([(
                        "service.name".to_string(),
                        logit_config::SetValue::Str("nginx".to_string()),
                    )]),
                    attributes: std::collections::BTreeMap::new(),
                },
            ),
            ("out", vec!["filter"], sink()),
        ]))
        .expect("should resolve");
    }

    #[test]
    fn a_has_attributes_with_only_an_attributes_map_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasAttributes {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "stream".to_string(),
                        logit_config::SetValue::Str("a".to_string()),
                    )]),
                },
            ),
            ("out", vec!["filter"], sink()),
        ]))
        .expect("should resolve");
    }

    #[test]
    fn the_same_key_in_both_maps_resolves_fine() {
        // resource: and attributes: address different objects, so a shared key name is legal.
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "filter",
                vec!["in"],
                ComponentKind::HasAttributes {
                    resource: std::collections::BTreeMap::from([(
                        "stream".to_string(),
                        logit_config::SetValue::Str("a".to_string()),
                    )]),
                    attributes: std::collections::BTreeMap::from([(
                        "stream".to_string(),
                        logit_config::SetValue::Str("a".to_string()),
                    )]),
                },
            ),
            ("out", vec!["filter"], sink()),
        ]))
        .expect("should resolve");
    }

    fn otlp_out_with_headers(headers: Vec<(&str, &str)>) -> ComponentKind {
        ComponentKind::OtlpOut {
            endpoint: "http://localhost:4318".to_string(),
            protocol: logit_config::OtlpProtocol::Http,
            headers: headers.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            paths: logit_config::OtlpPaths::default(),
            compression: logit_config::OtlpCompression::default(),
            tls: logit_config::TlsClientConfig::default(),
        }
    }

    fn otlp_out_with_paths(
        protocol: logit_config::OtlpProtocol,
        paths: logit_config::OtlpPaths,
    ) -> ComponentKind {
        ComponentKind::OtlpOut {
            endpoint: "http://localhost:4318".to_string(),
            protocol,
            headers: Map::new(),
            paths,
            compression: logit_config::OtlpCompression::default(),
            tls: logit_config::TlsClientConfig::default(),
        }
    }

    fn otlp_out_with_tls(endpoint: &str, tls: logit_config::TlsClientConfig) -> ComponentKind {
        ComponentKind::OtlpOut {
            endpoint: endpoint.to_string(),
            protocol: logit_config::OtlpProtocol::Grpc,
            headers: Map::new(),
            paths: logit_config::OtlpPaths::default(),
            compression: logit_config::OtlpCompression::default(),
            tls,
        }
    }

    #[test]
    fn an_otlp_out_with_a_reserved_header_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_headers(vec![("Content-Type", "text/plain")])),
        ]));
        assert!(err.contains("sets itself"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_a_reserved_header_name_is_rejected_case_insensitively() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_headers(vec![("GRPC-ENCODING", "gzip")])),
        ]));
        assert!(err.contains("sets itself"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_a_grpc_header_not_on_the_fixed_reserved_list_is_still_rejected() {
        // `grpc-*` is reserved by prefix, so a gRPC header the transport never sets
        // (`grpc-trace-bin`) is still rejected.
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_headers(vec![("grpc-trace-bin", "x")])),
        ]));
        assert!(err.contains("sets itself"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_two_headers_differing_only_in_case_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                otlp_out_with_headers(vec![
                    ("X-Scope-OrgID", "tenant-a"),
                    ("x-scope-orgid", "tenant-b"),
                ]),
            ),
        ]));
        assert!(err.contains("differs only in case"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_an_empty_header_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_headers(vec![("", "tenant-a")])),
        ]));
        assert!(err.contains("empty header name"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_a_pseudo_header_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_headers(vec![(":method", "POST")])),
        ]));
        assert!(err.contains("pseudo-header"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_a_custom_header_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_headers(vec![("X-Scope-OrgID", "tenant-a")])),
        ]))
        .expect("should resolve");
    }

    #[test]
    fn an_otlp_out_with_paths_under_grpc_is_rejected() {
        let paths = logit_config::OtlpPaths {
            logs: Some("/otlp/v1/logs".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_paths(logit_config::OtlpProtocol::Grpc, paths)),
        ]));
        assert!(err.contains("no effect under 'protocol: grpc'"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_paths_under_http_resolves_fine() {
        let paths = logit_config::OtlpPaths {
            logs: Some("/otlp/v1/logs".to_string()),
            ..Default::default()
        };
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_paths(logit_config::OtlpProtocol::Http, paths)),
        ]))
        .expect("should resolve");
    }

    #[test]
    fn an_otlp_out_with_no_paths_under_grpc_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                otlp_out_with_paths(
                    logit_config::OtlpProtocol::Grpc,
                    logit_config::OtlpPaths::default(),
                ),
            ),
        ]))
        .expect("should resolve");
    }

    #[test]
    fn an_otlp_out_with_a_full_tls_block_under_https_resolves_fine() {
        let tls = logit_config::TlsClientConfig {
            ca_file: Some("ca.pem".to_string()),
            cert_file: Some("client.pem".to_string()),
            key_file: Some("client.key".to_string()),
            insecure_skip_verify: false,
        };
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_tls("https://tempo:4317", tls)),
        ]))
        .expect("should resolve");
    }

    #[test]
    fn an_otlp_out_with_cert_file_but_no_key_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            cert_file: Some("client.pem".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_tls("https://tempo:4317", tls)),
        ]));
        assert!(err.contains("cert_file") && err.contains("key_file"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_key_file_but_no_cert_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            key_file: Some("client.key".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_tls("https://tempo:4317", tls)),
        ]));
        assert!(err.contains("cert_file") && err.contains("key_file"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_insecure_skip_verify_and_ca_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            ca_file: Some("ca.pem".to_string()),
            insecure_skip_verify: true,
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_tls("https://tempo:4317", tls)),
        ]));
        assert!(err.contains("insecure_skip_verify") && err.contains("ca_file"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_a_tls_block_under_a_plaintext_endpoint_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            ca_file: Some("ca.pem".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], otlp_out_with_tls("grpc://tempo:4317", tls)),
        ]));
        assert!(err.contains("'tls' is set") && err.contains("https://"), "got: {err}");
    }

    #[test]
    fn an_otlp_out_with_no_tls_block_under_a_plaintext_endpoint_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                otlp_out_with_tls("grpc://tempo:4317", logit_config::TlsClientConfig::default()),
            ),
        ]))
        .expect("should resolve");
    }

    #[test]
    fn a_kv_metrics_with_only_a_counter_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "derive",
                vec!["in"],
                ComponentKind::KvMetrics {
                    counters: vec![metric_spec("nginx.requests", None)],
                    gauges: vec![],
                    distributions: vec![],
                },
            ),
            ("out", vec!["derive"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["derive"].role(), Role::Transform);
    }

    #[test]
    fn keep_and_remove_resolve_as_transforms() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("keep", vec!["in"], ComponentKind::Keep { fields: vec!["status".to_string()] }),
            (
                "remove",
                vec!["keep"],
                ComponentKind::Remove { fields: vec!["client_ip".to_string()] },
            ),
            ("out", vec!["remove"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["keep"].role(), Role::Transform);
        assert_eq!(graph.components["remove"].role(), Role::Transform);
    }

    #[test]
    fn a_set_with_neither_map_configured_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "identity",
                vec!["in"],
                ComponentKind::Set {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::new(),
                },
            ),
            ("out", vec!["identity"], sink()),
        ]));
        assert!(err.contains("no-op"), "got: {err}");
    }

    #[test]
    fn a_set_with_only_resource_configured_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "identity",
                vec!["in"],
                ComponentKind::Set {
                    resource: std::collections::BTreeMap::from([(
                        "service.name".to_string(),
                        logit_config::SetValue::Str("nginx".to_string()),
                    )]),
                    attributes: std::collections::BTreeMap::new(),
                },
            ),
            ("out", vec!["identity"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["identity"].role(), Role::Transform);
    }

    #[test]
    fn a_trace_context_with_an_empty_trace_id_field_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "trace",
                vec!["in"],
                ComponentKind::TraceContext {
                    trace_id: String::new(),
                    span_id: None,
                    flags: None,
                    keep_source: false,
                    span: None,
                },
            ),
            ("out", vec!["trace"], sink()),
        ]));
        assert!(err.contains("no-op"), "got: {err}");
    }

    #[test]
    fn a_trace_context_with_an_empty_optional_field_name_is_rejected() {
        for (span_id, flags) in [(Some(String::new()), None), (None, Some(String::new()))] {
            let err = expect_err(cfg(vec![
                ("in", vec![], listener()),
                (
                    "trace",
                    vec!["in"],
                    ComponentKind::TraceContext {
                        trace_id: "trace.id".to_string(),
                        span_id,
                        flags,
                        keep_source: false,
                        span: None,
                    },
                ),
                ("out", vec!["trace"], sink()),
            ]));
            assert!(err.contains("use null"), "got: {err}");
        }
    }

    #[test]
    fn a_trace_context_with_a_trace_id_field_name_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "trace",
                vec!["in"],
                ComponentKind::TraceContext {
                    trace_id: "trace_id".to_string(),
                    span_id: None,
                    flags: None,
                    keep_source: false,
                    span: Some(logit_config::SpanLiftConfig::default()),
                },
            ),
            ("out", vec!["trace"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["trace"].role(), Role::Transform);
    }

    #[test]
    fn a_trace_context_span_block_with_an_empty_name_or_zero_skew_is_rejected() {
        let empty_name = logit_config::SpanLiftConfig {
            name: String::new(),
            ..logit_config::SpanLiftConfig::default()
        };
        let zero_skew = logit_config::SpanLiftConfig {
            max_skew: std::time::Duration::ZERO,
            ..logit_config::SpanLiftConfig::default()
        };
        for (span, needle) in [(empty_name, "requires every span"), (zero_skew, "0s")] {
            let err = expect_err(cfg(vec![
                ("in", vec![], listener()),
                (
                    "trace",
                    vec!["in"],
                    ComponentKind::TraceContext {
                        trace_id: "trace.id".to_string(),
                        span_id: None,
                        flags: None,
                        keep_source: false,
                        span: Some(span),
                    },
                ),
                ("out", vec!["trace"], sink()),
            ]));
            assert!(err.contains(needle), "got: {err}");
        }
    }

    #[test]
    fn a_scale_with_no_fields_configured_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "scale",
                vec!["in"],
                ComponentKind::Scale { fields: std::collections::BTreeMap::new() },
            ),
            ("out", vec!["scale"], sink()),
        ]));
        assert!(err.contains("no-op"), "got: {err}");
    }

    #[test]
    fn a_scale_with_an_empty_field_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "scale",
                vec!["in"],
                ComponentKind::Scale {
                    fields: std::collections::BTreeMap::from([(String::new(), 1000.0)]),
                },
            ),
            ("out", vec!["scale"], sink()),
        ]));
        assert!(err.contains("empty"), "got: {err}");
    }

    #[test]
    fn a_scale_with_a_non_finite_factor_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "scale",
                vec!["in"],
                ComponentKind::Scale {
                    fields: std::collections::BTreeMap::from([(
                        "request_time".to_string(),
                        f64::NAN,
                    )]),
                },
            ),
            ("out", vec!["scale"], sink()),
        ]));
        assert!(err.contains("finite"), "got: {err}");
    }

    #[test]
    fn a_scale_with_a_field_configured_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "scale",
                vec!["in"],
                ComponentKind::Scale {
                    fields: std::collections::BTreeMap::from([(
                        "request_time".to_string(),
                        1000.0,
                    )]),
                },
            ),
            ("out", vec!["scale"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["scale"].role(), Role::Transform);
    }

    /// Builds a [`logit_config::ValueAllowList`] for rule 54's tests -- `normalize`/`other`
    /// default the way the real config's `#[serde(default)]` fields do.
    fn allow_list(
        normalize: Vec<logit_config::NormalizeStep>,
        allow: Vec<logit_config::SetValue>,
        other: Option<logit_config::SetValue>,
    ) -> logit_config::ValueAllowList {
        logit_config::ValueAllowList { normalize, allow, other }
    }

    #[test]
    fn a_keep_values_with_neither_map_configured_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::new(),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]));
        assert!(err.contains("no-op"), "got: {err}");
    }

    #[test]
    fn a_keep_values_with_an_empty_field_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        String::new(),
                        allow_list(
                            vec![],
                            vec![logit_config::SetValue::Str("x".to_string())],
                            None,
                        ),
                    )]),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]));
        assert!(err.contains("empty"), "got: {err}");
    }

    #[test]
    fn a_keep_values_with_an_empty_allow_list_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "host".to_string(),
                        allow_list(vec![], vec![], None),
                    )]),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]));
        assert!(err.contains("'allow'"), "got: {err}");
        assert!(err.contains("set"), "message should name the alternative -- got: {err}");
    }

    #[test]
    fn a_keep_values_with_a_non_finite_allow_value_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "ratio".to_string(),
                        allow_list(vec![], vec![logit_config::SetValue::F64(f64::NAN)], None),
                    )]),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]));
        assert!(err.contains("finite"), "got: {err}");
    }

    #[test]
    fn a_keep_values_with_a_non_finite_other_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "ratio".to_string(),
                        allow_list(
                            vec![],
                            vec![logit_config::SetValue::F64(1.0)],
                            Some(logit_config::SetValue::F64(f64::INFINITY)),
                        ),
                    )]),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]));
        assert!(err.contains("finite"), "got: {err}");
    }

    #[test]
    fn a_keep_values_with_a_non_lowercase_allow_literal_under_lower_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "host".to_string(),
                        allow_list(
                            vec![logit_config::NormalizeStep::Lower],
                            vec![logit_config::SetValue::Str("Static.Local".to_string())],
                            None,
                        ),
                    )]),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]));
        assert!(err.contains("Static.Local"), "got: {err}");
        assert!(err.contains("lower"), "got: {err}");
    }

    #[test]
    fn a_keep_values_with_a_non_lowercase_other_under_lower_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "host".to_string(),
                        allow_list(
                            vec![logit_config::NormalizeStep::Lower],
                            vec![logit_config::SetValue::Str("static.local".to_string())],
                            Some(logit_config::SetValue::Str("Other".to_string())),
                        ),
                    )]),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]));
        assert!(err.contains("Other"), "got: {err}");
    }

    #[test]
    fn a_keep_values_with_a_duplicate_normalize_step_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "host".to_string(),
                        allow_list(
                            vec![
                                logit_config::NormalizeStep::Lower,
                                logit_config::NormalizeStep::Lower,
                            ],
                            vec![logit_config::SetValue::Str("static.local".to_string())],
                            None,
                        ),
                    )]),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]));
        assert!(err.contains("normalize"), "got: {err}");
    }

    #[test]
    fn a_keep_values_with_an_empty_normalize_list_is_legal() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "host".to_string(),
                        allow_list(
                            vec![],
                            vec![logit_config::SetValue::Str("static.local".to_string())],
                            None,
                        ),
                    )]),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]))
        .expect("an empty normalize list means no normalization, not a config error");
        assert_eq!(graph.components["kv"].role(), Role::Transform);
    }

    #[test]
    fn a_keep_values_with_a_field_configured_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "kv",
                vec!["in"],
                ComponentKind::KeepValues {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "host".to_string(),
                        allow_list(
                            vec![logit_config::NormalizeStep::Lower],
                            vec![logit_config::SetValue::Str("static.local".to_string())],
                            Some(logit_config::SetValue::Str("other".to_string())),
                        ),
                    )]),
                },
            ),
            ("out", vec!["kv"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["kv"].role(), Role::Transform);
    }

    /// A `shape` with everything defaulted -- the whole config an operator writes in the common
    /// case (`docs/adr/shape-observer-component.md`).
    fn shape_defaults() -> ComponentKind {
        ComponentKind::Shape {
            interval: Duration::from_secs(10),
            resource: logit_config::ShapeResource::Drop,
            max_tracked_keys: 4096,
            max_tracked_keysets: 4096,
        }
    }

    #[test]
    fn a_shape_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("tap", vec!["in"], shape_defaults()),
            ("out", vec!["tap"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["tap"].role(), Role::Transform);
        assert_eq!(graph.components["tap"].kind_name(), "shape");
    }

    /// Rule 9 reaches `shape` through [`interval`] -- the same gate `aggregate` and `internal`
    /// pass through, not a second zero check of its own.
    #[test]
    fn a_shape_with_a_zero_interval_is_rejected() {
        let mut kind = shape_defaults();
        if let ComponentKind::Shape { interval, .. } = &mut kind {
            *interval = Duration::ZERO;
        }
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("tap", vec!["in"], kind),
            ("out", vec!["tap"], sink()),
        ]));
        assert!(err.contains("interval"), "{err}");
    }

    /// Rule 58, key half.
    #[test]
    fn a_shape_with_a_zero_key_cap_is_rejected() {
        let mut kind = shape_defaults();
        if let ComponentKind::Shape { max_tracked_keys, .. } = &mut kind {
            *max_tracked_keys = 0;
        }
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("tap", vec!["in"], kind),
            ("out", vec!["tap"], sink()),
        ]));
        assert!(err.contains("max_tracked_keys"), "{err}");
    }

    /// Rule 58, key-set half.
    #[test]
    fn a_shape_with_a_zero_keyset_cap_is_rejected() {
        let mut kind = shape_defaults();
        if let ComponentKind::Shape { max_tracked_keysets, .. } = &mut kind {
            *max_tracked_keysets = 0;
        }
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("tap", vec!["in"], kind),
            ("out", vec!["tap"], sink()),
        ]));
        assert!(err.contains("max_tracked_keysets"), "{err}");
    }

    /// A bare `sample` at `rate` -- no key, no `missing`, no override.
    fn sample_at(rate: f64) -> ComponentKind {
        ComponentKind::Sample { rate, key: None, missing: None, always_keep: None }
    }

    /// An `always_keep` override on `attribute: sampling.keep`, any value.
    fn keep_flag() -> logit_config::SampleOverride {
        logit_config::SampleOverride {
            attribute: Some("sampling.keep".to_string()),
            resource: None,
            value: None,
        }
    }

    fn resolve_sample(kind: ComponentKind) -> anyhow::Result<Graph> {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("sampled", vec!["in"], kind),
            ("out", vec!["sampled"], sink()),
        ]))
    }

    fn sample_err(kind: ComponentKind) -> String {
        expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("sampled", vec!["in"], kind),
            ("out", vec!["sampled"], sink()),
        ]))
    }

    #[test]
    fn a_bare_sample_resolves_as_a_transform() {
        let graph = resolve_sample(sample_at(0.5)).expect("should resolve");
        assert_eq!(graph.components["sampled"].role(), Role::Transform);
        assert_eq!(graph.components["sampled"].kind_name(), "sample");
    }

    /// Rule 61: `rate: 0` with an override is the "only flagged events" mode.
    #[test]
    fn a_sample_at_rate_zero_with_always_keep_is_accepted() {
        let kind = ComponentKind::Sample {
            rate: 0.0,
            key: Some(logit_config::SampleKey::TraceId),
            missing: Some(logit_config::SampleMissing::Drop),
            always_keep: Some(keep_flag()),
        };
        resolve_sample(kind).expect("rate 0 with always_keep keeps flagged events");
    }

    /// Rule 61: range, NaN.
    #[test]
    fn a_sample_with_a_non_finite_rate_is_rejected() {
        let err = sample_err(sample_at(f64::NAN));
        assert!(err.contains("finite"), "got: {err}");
    }

    /// Rule 61: range, out of `[0, 1]`.
    #[test]
    fn a_sample_with_a_rate_outside_zero_to_one_is_rejected() {
        for rate in [-0.1, 1.5] {
            let err = sample_err(sample_at(rate));
            assert!(err.contains("between 0.0 and 1.0"), "rate {rate}, got: {err}");
        }
    }

    /// Rule 61: `rate: 1` is a no-op.
    #[test]
    fn a_sample_at_rate_one_is_rejected() {
        let err = sample_err(sample_at(1.0));
        assert!(err.contains("no-op"), "got: {err}");
    }

    /// Rule 61: `rate: 0` alone drops everything.
    #[test]
    fn a_sample_at_rate_zero_without_always_keep_is_rejected() {
        let err = sample_err(sample_at(0.0));
        assert!(err.contains("null_out"), "got: {err}");
    }

    /// Rule 61: empty key field names.
    #[test]
    fn a_sample_with_an_empty_key_name_is_rejected() {
        for key in [
            logit_config::SampleKey::Attribute(String::new()),
            logit_config::SampleKey::Resource(String::new()),
        ] {
            let kind = ComponentKind::Sample {
                rate: 0.5,
                key: Some(key),
                missing: None,
                always_keep: None,
            };
            let err = sample_err(kind);
            assert!(err.contains("'key' field name must not be empty"), "got: {err}");
        }
    }

    /// Rule 61: `missing:` without `key:`.
    #[test]
    fn a_sample_with_missing_but_no_key_is_rejected() {
        let kind = ComponentKind::Sample {
            rate: 0.5,
            key: None,
            missing: Some(logit_config::SampleMissing::Keep),
            always_keep: None,
        };
        let err = sample_err(kind);
        assert!(err.contains("'missing' only applies with 'key'"), "got: {err}");
    }

    /// Rule 61: `always_keep` naming both, or neither.
    #[test]
    fn a_sample_override_must_name_exactly_one_field() {
        let both = logit_config::SampleOverride {
            attribute: Some("a".to_string()),
            resource: Some("b".to_string()),
            value: None,
        };
        let neither = logit_config::SampleOverride { attribute: None, resource: None, value: None };
        for (o, expected) in [(both, "both"), (neither, "needs one of")] {
            let kind =
                ComponentKind::Sample { rate: 0.5, key: None, missing: None, always_keep: Some(o) };
            let err = sample_err(kind);
            assert!(err.contains(expected), "got: {err}");
        }
    }

    /// Rule 61: an empty override field name.
    #[test]
    fn a_sample_override_with_an_empty_field_name_is_rejected() {
        let o = logit_config::SampleOverride {
            attribute: None,
            resource: Some(String::new()),
            value: None,
        };
        let kind =
            ComponentKind::Sample { rate: 0.5, key: None, missing: None, always_keep: Some(o) };
        let err = sample_err(kind);
        assert!(err.contains("'always_keep' field name must not be empty"), "got: {err}");
    }

    /// Rule 61: a non-finite override value can never match.
    #[test]
    fn a_sample_override_with_a_non_finite_value_is_rejected() {
        let mut o = keep_flag();
        o.value = Some(logit_config::SetValue::F64(f64::INFINITY));
        let kind =
            ComponentKind::Sample { rate: 0.5, key: None, missing: None, always_keep: Some(o) };
        let err = sample_err(kind);
        assert!(err.contains("must be finite"), "got: {err}");
    }

    /// A `datadog_in` with every optional field at its default, the shape rule 63 reads.
    fn datadog_in(bind: &str, api_keys: Vec<&str>) -> ComponentKind {
        ComponentKind::DatadogIn {
            bind: bind.to_string(),
            tls: None,
            api_keys: api_keys.into_iter().map(String::from).collect(),
            handshake_timeout: default_handshake_timeout(),
            idle_timeout: None,
        }
    }

    fn datadog_in_err(kind: ComponentKind) -> String {
        expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]))
    }

    #[test]
    fn a_datadog_in_with_or_without_api_keys_resolves() {
        for keys in [vec![], vec!["0123456789abcdef", "fedcba9876543210"]] {
            resolve(cfg(vec![
                ("in", vec![], datadog_in("0.0.0.0:8080", keys)),
                ("out", vec!["in"], sink()),
            ]))
            .expect("a datadog_in with a bind and non-empty keys (or none) is valid");
        }
    }

    /// Rule 63: an empty `bind` names no socket.
    #[test]
    fn a_datadog_in_with_an_empty_bind_is_rejected() {
        let err = datadog_in_err(datadog_in("", vec![]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'bind' must not be empty"), "got: {err}");
    }

    /// Rule 63: an empty key could never match; the message names the accept-any spelling.
    #[test]
    fn a_datadog_in_with_an_empty_api_key_is_rejected() {
        let err = datadog_in_err(datadog_in("0.0.0.0:8080", vec!["good-key", ""]));
        assert!(err.contains("'api_keys' entry must not be empty"), "got: {err}");
        assert!(err.contains("omit 'api_keys'"), "got: {err}");
    }

    /// Rule 63: HTTP strips a header value's surrounding whitespace, so this key never matches.
    #[test]
    fn a_datadog_in_api_key_with_surrounding_whitespace_is_rejected() {
        let err = datadog_in_err(datadog_in("0.0.0.0:8080", vec!["key\n"]));
        assert!(err.contains("leading or trailing whitespace"), "got: {err}");
    }

    /// Rules 45 and 53 cover `datadog_in`'s two timeouts, as they do `otlp_in`'s.
    #[test]
    fn a_datadog_in_with_a_zero_handshake_or_idle_timeout_is_rejected() {
        let mut kind = datadog_in("0.0.0.0:8080", vec![]);
        if let ComponentKind::DatadogIn { handshake_timeout, .. } = &mut kind {
            *handshake_timeout = Duration::ZERO;
        }
        let err = datadog_in_err(kind);
        assert!(err.contains("'handshake_timeout' must be greater than 0s"), "got: {err}");

        let mut kind = datadog_in("0.0.0.0:8080", vec![]);
        if let ComponentKind::DatadogIn { idle_timeout, .. } = &mut kind {
            *idle_timeout = Some(Duration::ZERO);
        }
        let err = datadog_in_err(kind);
        assert!(err.contains("'idle_timeout' must be greater than 0s"), "got: {err}");
    }

    /// A `datadog_trace_in` with every optional field at its default, the shape rule 64 reads.
    fn datadog_trace_in(bind: Option<&str>, socket: Option<&str>) -> ComponentKind {
        ComponentKind::DatadogTraceIn {
            bind: bind.map(String::from),
            socket: socket.map(String::from),
            tls: None,
            handshake_timeout: default_handshake_timeout(),
            idle_timeout: None,
        }
    }

    #[test]
    fn a_datadog_trace_in_with_a_bind_a_socket_or_both_resolves() {
        for (bind, socket) in [
            (Some("127.0.0.1:8126"), None),
            (None, Some("/var/run/datadog/apm.socket")),
            (Some("127.0.0.1:8126"), Some("/var/run/datadog/apm.socket")),
        ] {
            resolve(cfg(vec![
                ("in", vec![], datadog_trace_in(bind, socket)),
                ("out", vec!["in"], sink()),
            ]))
            .expect("a datadog_trace_in with a bind, a socket, or both is valid");
        }
    }

    /// Rule 64: neither listener leaves nothing to serve.
    #[test]
    fn a_datadog_trace_in_with_neither_bind_nor_socket_is_rejected() {
        let err = datadog_in_err(datadog_trace_in(None, None));
        assert!(err.contains("needs 'bind', 'socket', or both"), "got: {err}");
    }

    /// Rule 64: an empty `bind` names no socket, as rule 63 says for `datadog_in`.
    #[test]
    fn a_datadog_trace_in_with_an_empty_bind_is_rejected() {
        let err = datadog_in_err(datadog_trace_in(Some(" "), Some("/tmp/apm.socket")));
        assert!(err.contains("'bind' must not be empty"), "got: {err}");
    }

    /// Rule 64: a relative socket path depends on the working directory.
    #[test]
    fn a_datadog_trace_in_with_a_relative_socket_is_rejected() {
        let err = datadog_in_err(datadog_trace_in(None, Some("apm.socket")));
        assert!(err.contains("'socket' must be an absolute path"), "got: {err}");
    }

    /// Rule 64: TLS terminates on the TCP listener only.
    #[test]
    fn a_datadog_trace_in_with_tls_and_no_bind_is_rejected() {
        let mut kind = datadog_trace_in(None, Some("/tmp/apm.socket"));
        if let ComponentKind::DatadogTraceIn { tls, .. } = &mut kind {
            *tls = Some(logit_config::TlsServerConfig {
                cert_file: "server.pem".into(),
                key_file: "server.key".into(),
                client_ca_file: None,
            });
        }
        let err = datadog_in_err(kind);
        assert!(err.contains("'tls' needs 'bind'"), "got: {err}");
    }

    /// Rules 45 and 53 cover `datadog_trace_in`'s two timeouts.
    #[test]
    fn a_datadog_trace_in_with_a_zero_handshake_or_idle_timeout_is_rejected() {
        let mut kind = datadog_trace_in(Some("127.0.0.1:8126"), None);
        if let ComponentKind::DatadogTraceIn { handshake_timeout, .. } = &mut kind {
            *handshake_timeout = Duration::ZERO;
        }
        let err = datadog_in_err(kind);
        assert!(err.contains("'handshake_timeout' must be greater than 0s"), "got: {err}");

        let mut kind = datadog_trace_in(Some("127.0.0.1:8126"), None);
        if let ComponentKind::DatadogTraceIn { idle_timeout, .. } = &mut kind {
            *idle_timeout = Some(Duration::ZERO);
        }
        let err = datadog_in_err(kind);
        assert!(err.contains("'idle_timeout' must be greater than 0s"), "got: {err}");
    }

    /// A `flatten` with everything defaulted -- `attributes: all`, `resource: none`,
    /// `arrays: index`.
    fn flatten_defaults() -> ComponentKind {
        ComponentKind::Flatten {
            attributes: logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::All),
            resource: logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::None),
            arrays: logit_config::FlattenArrays::Index,
        }
    }

    #[test]
    fn a_flatten_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("flat", vec!["in"], flatten_defaults()),
            ("out", vec!["flat"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["flat"].role(), Role::Transform);
        assert_eq!(graph.components["flat"].kind_name(), "flatten");
    }

    #[test]
    fn a_flatten_with_no_fields_configured_is_accepted() {
        // The default -- `attributes: all`, `resource: none` -- selects something (every nested
        // attribute), so it must not trip the "neither selects anything" rejection below.
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("flat", vec!["in"], flatten_defaults()),
            ("out", vec!["flat"], sink()),
        ]))
        .expect("attributes: all is not a no-op");
    }

    #[test]
    fn a_flatten_with_both_fields_set_to_none_is_rejected() {
        let kind = ComponentKind::Flatten {
            attributes: logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::None),
            resource: logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::None),
            arrays: logit_config::FlattenArrays::Index,
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("flat", vec!["in"], kind),
            ("out", vec!["flat"], sink()),
        ]));
        assert!(err.contains("no-op"), "got: {err}");
    }

    #[test]
    fn a_flatten_with_an_empty_named_list_is_rejected() {
        let kind = ComponentKind::Flatten {
            attributes: logit_config::FlattenFields::Named(vec![]),
            resource: logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::None),
            arrays: logit_config::FlattenArrays::Index,
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("flat", vec!["in"], kind),
            ("out", vec!["flat"], sink()),
        ]));
        assert!(err.contains("empty list"), "got: {err}");
    }

    #[test]
    fn a_flatten_with_an_empty_field_name_is_rejected() {
        let kind = ComponentKind::Flatten {
            attributes: logit_config::FlattenFields::Named(vec![String::new()]),
            resource: logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::None),
            arrays: logit_config::FlattenArrays::Index,
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("flat", vec!["in"], kind),
            ("out", vec!["flat"], sink()),
        ]));
        assert!(err.contains("must not be empty"), "got: {err}");
    }

    #[test]
    fn a_flatten_with_a_duplicate_field_name_is_rejected() {
        let kind = ComponentKind::Flatten {
            attributes: logit_config::FlattenFields::Named(vec![
                "http".to_string(),
                "http".to_string(),
            ]),
            resource: logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::None),
            arrays: logit_config::FlattenArrays::Index,
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("flat", vec!["in"], kind),
            ("out", vec!["flat"], sink()),
        ]));
        assert!(err.contains("repeats"), "got: {err}");
    }

    /// A bare `http_access` -- every field defaulted.
    fn http_access_defaults() -> ComponentKind {
        ComponentKind::HttpAccess {
            routes: vec![],
            route_other: None,
            user_agent_rules: vec![],
            max_length: std::collections::BTreeMap::new(),
            redact_query: vec![],
            forwarded: None,
        }
    }

    /// Runs `edit` against a default `http_access`, resolves it in an `in -> http -> out`
    /// chain, and returns the rule-60 error.
    fn http_access_err(edit: impl FnOnce(&mut ComponentKind)) -> String {
        let mut kind = http_access_defaults();
        edit(&mut kind);
        expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("http", vec!["in"], kind),
            ("out", vec!["http"], sink()),
        ]))
    }

    fn route_rule(
        builtin: Option<logit_config::HttpRouteSet>,
        pattern: Option<&str>,
        route: Option<&str>,
    ) -> logit_config::HttpRouteRule {
        logit_config::HttpRouteRule {
            builtin,
            pattern: pattern.map(str::to_string),
            route: route.map(str::to_string),
        }
    }

    fn set_routes(routes: Vec<logit_config::HttpRouteRule>) -> impl FnOnce(&mut ComponentKind) {
        move |kind| {
            if let ComponentKind::HttpAccess { routes: r, .. } = kind {
                *r = routes;
            }
        }
    }

    /// Rule 60 has no "nothing configured" clause: a bare `http_access` is meaningful.
    #[test]
    fn a_bare_http_access_validates_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("http", vec!["in"], http_access_defaults()),
            ("out", vec!["http"], sink()),
        ]))
        .expect("a bare http_access is not a no-op");
        assert_eq!(graph.components["http"].role(), Role::Transform);
        assert_eq!(graph.components["http"].kind_name(), "http_access");
    }

    #[test]
    fn a_fully_configured_http_access_validates() {
        use logit_config::{ForwardedConfig, HttpRouteSet, UserAgentRule};
        let kind = ComponentKind::HttpAccess {
            routes: vec![
                route_rule(Some(HttpRouteSet::Probes), None, None),
                route_rule(Some(HttpRouteSet::Assets), None, None),
                route_rule(None, Some("^/$"), Some("/")),
            ],
            route_other: Some("/{other}".to_string()),
            user_agent_rules: vec![UserAgentRule {
                pattern: "MyMonitor/".to_string(),
                class: "tool".to_string(),
            }],
            max_length: std::collections::BTreeMap::from([("url.path".to_string(), 512)]),
            redact_query: vec!["token".to_string()],
            forwarded: Some(ForwardedConfig { trust: true }),
        };
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("http", vec!["in"], kind),
            ("out", vec!["http"], sink()),
        ]))
        .expect("should resolve");
    }

    /// Rule 60: an uncompilable `routes` match.
    #[test]
    fn an_http_access_route_with_an_uncompilable_match_is_rejected() {
        let err = http_access_err(set_routes(vec![route_rule(None, Some("(unclosed"), Some("/"))]));
        assert!(err.contains("routes[0]") && err.contains("not a valid regex"), "{err}");
    }

    /// Rule 60: an uncompilable `user_agent_rules` match.
    #[test]
    fn an_http_access_user_agent_rule_with_an_uncompilable_match_is_rejected() {
        let err = http_access_err(|kind| {
            if let ComponentKind::HttpAccess { user_agent_rules, .. } = kind {
                user_agent_rules.push(logit_config::UserAgentRule {
                    pattern: "[z-a]".to_string(),
                    class: "tool".to_string(),
                });
            }
        });
        assert!(err.contains("user_agent_rules[0]") && err.contains("not a valid regex"), "{err}");
    }

    /// Rule 60: an empty `match`.
    #[test]
    fn an_http_access_empty_match_is_rejected() {
        let err = http_access_err(set_routes(vec![route_rule(None, Some(""), Some("/"))]));
        assert!(err.contains("'match' must not be empty"), "{err}");
    }

    /// Rule 60: an empty `route`.
    #[test]
    fn an_http_access_empty_route_is_rejected() {
        let err = http_access_err(set_routes(vec![route_rule(None, Some("^/$"), Some(""))]));
        assert!(err.contains("'route' must not be empty"), "{err}");
    }

    /// Rule 60: an empty `class`.
    #[test]
    fn an_http_access_empty_class_is_rejected() {
        let err = http_access_err(|kind| {
            if let ComponentKind::HttpAccess { user_agent_rules, .. } = kind {
                user_agent_rules.push(logit_config::UserAgentRule {
                    pattern: "x".to_string(),
                    class: String::new(),
                });
            }
        });
        assert!(err.contains("'class' must not be empty"), "{err}");
    }

    /// Rule 60: an empty `route_other`.
    #[test]
    fn an_http_access_empty_route_other_is_rejected() {
        let err = http_access_err(|kind| {
            if let ComponentKind::HttpAccess { route_other, .. } = kind {
                *route_other = Some(String::new());
            }
        });
        assert!(err.contains("'route_other' must not be empty"), "{err}");
    }

    /// Rule 60: an empty `redact_query` entry.
    #[test]
    fn an_http_access_empty_redact_query_entry_is_rejected() {
        let err = http_access_err(|kind| {
            if let ComponentKind::HttpAccess { redact_query, .. } = kind {
                redact_query.push(String::new());
            }
        });
        assert!(err.contains("'redact_query' entry must not be empty"), "{err}");
    }

    /// Rule 60: `builtin` and `match` together.
    #[test]
    fn an_http_access_route_with_builtin_and_match_is_rejected() {
        let err = http_access_err(set_routes(vec![route_rule(
            Some(logit_config::HttpRouteSet::Assets),
            Some("x"),
            Some("/x"),
        )]));
        assert!(err.contains("both 'builtin' and 'match'"), "{err}");
    }

    /// Rule 60: `builtin` and `route` together.
    #[test]
    fn an_http_access_route_with_builtin_and_route_is_rejected() {
        let err = http_access_err(set_routes(vec![route_rule(
            Some(logit_config::HttpRouteSet::Assets),
            None,
            Some("/x"),
        )]));
        assert!(err.contains("both 'builtin' and 'route'"), "{err}");
    }

    /// Rule 60: `match` without `route`.
    #[test]
    fn an_http_access_route_with_match_but_no_route_is_rejected() {
        let err = http_access_err(set_routes(vec![route_rule(None, Some("^/$"), None)]));
        assert!(err.contains("has 'match' but no 'route'"), "{err}");
    }

    /// Rule 60: `route` without `match`.
    #[test]
    fn an_http_access_route_with_route_but_no_match_is_rejected() {
        let err = http_access_err(set_routes(vec![route_rule(None, None, Some("/"))]));
        assert!(err.contains("has 'route' but no 'match'"), "{err}");
    }

    /// Rule 60: an entry with nothing in it.
    #[test]
    fn an_http_access_empty_route_rule_is_rejected() {
        let err = http_access_err(set_routes(vec![route_rule(None, None, None)]));
        assert!(err.contains("routes[0] is empty"), "{err}");
    }

    /// Rule 60: a repeated `builtin` set.
    #[test]
    fn an_http_access_repeated_builtin_is_rejected() {
        let err = http_access_err(set_routes(vec![
            route_rule(Some(logit_config::HttpRouteSet::Assets), None, None),
            route_rule(None, Some("^/$"), Some("/")),
            route_rule(Some(logit_config::HttpRouteSet::Assets), None, None),
        ]));
        assert!(err.contains("routes[2] repeats 'builtin: assets'"), "{err}");
    }

    /// Rule 60: a `max_length` key `http_access` never caps; the error lists the valid keys.
    #[test]
    fn an_http_access_max_length_for_an_uncapped_field_is_rejected() {
        let err = http_access_err(|kind| {
            if let ComponentKind::HttpAccess { max_length, .. } = kind {
                max_length.insert("url.paths".to_string(), 10);
            }
        });
        assert!(err.contains("'url.paths'") && err.contains("valid keys: url.path,"), "{err}");
    }

    /// Rule 60: a `max_length` of `0`.
    #[test]
    fn an_http_access_zero_max_length_is_rejected() {
        let err = http_access_err(|kind| {
            if let ComponentKind::HttpAccess { max_length, .. } = kind {
                max_length.insert("url.path".to_string(), 0);
            }
        });
        assert!(err.contains("'url.path' is 0"), "{err}");
    }

    /// Rule 60: `forwarded: {trust: false}`, the default spelled out.
    #[test]
    fn an_http_access_forwarded_trust_false_is_rejected() {
        let err = http_access_err(|kind| {
            if let ComponentKind::HttpAccess { forwarded, .. } = kind {
                *forwarded = Some(logit_config::ForwardedConfig { trust: false });
            }
        });
        assert!(err.contains("omit the block instead"), "{err}");
    }

    #[test]
    fn a_regex_with_a_named_capture_resolves() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "regex",
                vec!["in"],
                ComponentKind::Regex {
                    pattern: r"status=(?P<status>\d+)".to_string(),
                    field: None,
                },
            ),
            ("out", vec!["regex"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["regex"].role(), Role::Transform);
    }

    #[test]
    fn a_regex_with_a_field_naming_an_attribute_resolves() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "regex",
                vec!["in"],
                ComponentKind::Regex {
                    pattern: r"status=(?P<status>\d+)".to_string(),
                    field: Some("message".to_string()),
                },
            ),
            ("out", vec!["regex"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["regex"].role(), Role::Transform);
    }

    #[test]
    fn a_regex_with_an_invalid_pattern_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "regex",
                vec!["in"],
                ComponentKind::Regex { pattern: "(?P<a>".to_string(), field: None },
            ),
            ("out", vec!["regex"], sink()),
        ]));
        assert!(err.contains("not a valid regex"), "got: {err}");
    }

    #[test]
    fn a_regex_with_no_named_capture_groups_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "regex",
                vec!["in"],
                ComponentKind::Regex { pattern: r"(\d+)".to_string(), field: None },
            ),
            ("out", vec!["regex"], sink()),
        ]));
        assert!(err.contains("no-op"), "got: {err}");
    }

    #[test]
    fn a_regex_with_a_duplicate_named_capture_group_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "regex",
                vec!["in"],
                ComponentKind::Regex { pattern: "(?P<a>x)(?P<a>y)".to_string(), field: None },
            ),
            ("out", vec!["regex"], sink()),
        ]));
        assert!(err.contains("not a valid regex"), "got: {err}");
    }

    #[test]
    fn a_regex_with_an_empty_field_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "regex",
                vec!["in"],
                ComponentKind::Regex {
                    pattern: r"status=(?P<status>\d+)".to_string(),
                    field: Some(String::new()),
                },
            ),
            ("out", vec!["regex"], sink()),
        ]));
        assert!(err.contains("could never match"), "got: {err}");
    }

    #[test]
    fn a_csv_with_no_columns_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("csv", vec!["in"], ComponentKind::Csv { columns: vec![], delimiter: ',' }),
            ("out", vec!["csv"], sink()),
        ]));
        assert!(err.contains("no-op"), "got: {err}");
    }

    #[test]
    fn a_csv_with_an_empty_column_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "csv",
                vec!["in"],
                ComponentKind::Csv {
                    columns: vec!["a".to_string(), String::new()],
                    delimiter: ',',
                },
            ),
            ("out", vec!["csv"], sink()),
        ]));
        assert!(err.contains("empty"), "got: {err}");
    }

    #[test]
    fn a_csv_with_a_duplicate_column_name_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "csv",
                vec!["in"],
                ComponentKind::Csv {
                    columns: vec!["a".to_string(), "b".to_string(), "a".to_string()],
                    delimiter: ',',
                },
            ),
            ("out", vec!["csv"], sink()),
        ]));
        assert!(err.contains("twice"), "got: {err}");
    }

    #[test]
    fn a_csv_with_a_quote_delimiter_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "csv",
                vec!["in"],
                ComponentKind::Csv { columns: vec!["a".to_string()], delimiter: '"' },
            ),
            ("out", vec!["csv"], sink()),
        ]));
        assert!(err.contains("delimiter"), "got: {err}");
    }

    #[test]
    fn a_csv_with_a_newline_delimiter_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "csv",
                vec!["in"],
                ComponentKind::Csv { columns: vec!["a".to_string()], delimiter: '\n' },
            ),
            ("out", vec!["csv"], sink()),
        ]));
        assert!(err.contains("delimiter"), "got: {err}");
    }

    #[test]
    fn a_csv_with_a_non_ascii_delimiter_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "csv",
                vec!["in"],
                ComponentKind::Csv { columns: vec!["a".to_string()], delimiter: 'é' },
            ),
            ("out", vec!["csv"], sink()),
        ]));
        assert!(err.contains("ASCII"), "got: {err}");
    }

    #[test]
    fn a_csv_with_columns_configured_resolves_as_a_transform() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "csv",
                vec!["in"],
                ComponentKind::Csv {
                    columns: vec!["remote_addr".to_string(), "status".to_string()],
                    delimiter: ',',
                },
            ),
            ("out", vec!["csv"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["csv"].role(), Role::Transform);
        assert_eq!(graph.components["csv"].kind_name(), "csv");
    }

    #[test]
    fn a_csv_with_a_tab_delimiter_resolves() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "csv",
                vec!["in"],
                ComponentKind::Csv { columns: vec!["a".to_string()], delimiter: '\t' },
            ),
            ("out", vec!["csv"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["csv"].role(), Role::Transform);
    }

    #[test]
    fn the_signal_components_resolve_as_transforms_with_the_right_kind_names() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "has_signal",
                vec!["in"],
                ComponentKind::HasSignal {
                    signals: vec![logit_config::Signal::Traces],
                    mode: logit_config::MatchMode::AnyOf,
                },
            ),
            (
                "keep_signals",
                vec!["has_signal"],
                ComponentKind::KeepSignals { signals: vec![logit_config::Signal::Logs] },
            ),
            (
                "drop_signals",
                vec!["keep_signals"],
                ComponentKind::DropSignals { signals: vec![logit_config::Signal::Metrics] },
            ),
            ("out", vec!["drop_signals"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["has_signal"].role(), Role::Transform);
        assert_eq!(graph.components["keep_signals"].role(), Role::Transform);
        assert_eq!(graph.components["drop_signals"].role(), Role::Transform);
        assert_eq!(kind_name(&graph.components["has_signal"].kind), "has_signal");
        assert_eq!(kind_name(&graph.components["keep_signals"].kind), "keep_signals");
        assert_eq!(kind_name(&graph.components["drop_signals"].kind), "drop_signals");
    }

    #[test]
    fn the_attribute_components_resolve_as_transforms_with_the_right_kind_names() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "has_attributes",
                vec!["in"],
                ComponentKind::HasAttributes {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "stream".to_string(),
                        logit_config::SetValue::Str("a".to_string()),
                    )]),
                },
            ),
            (
                "drop_attributes",
                vec!["has_attributes"],
                ComponentKind::DropAttributes {
                    resource: std::collections::BTreeMap::new(),
                    attributes: std::collections::BTreeMap::from([(
                        "debug".to_string(),
                        logit_config::SetValue::Bool(true),
                    )]),
                },
            ),
            ("out", vec!["drop_attributes"], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["has_attributes"].role(), Role::Transform);
        assert_eq!(graph.components["drop_attributes"].role(), Role::Transform);
        assert_eq!(kind_name(&graph.components["has_attributes"].kind), "has_attributes");
        assert_eq!(kind_name(&graph.components["drop_attributes"].kind), "drop_attributes");
    }

    #[test]
    fn diamond_fan_out_fan_in_resolves() {
        let graph = resolve(cfg(vec![
            ("in", vec![], listener()),
            ("left", vec!["in"], lua()),
            ("right", vec!["in"], lua()),
            ("out", vec!["left", "right"], sink()),
        ]))
        .expect("should resolve");
        let order = graph.topological_order;
        assert_eq!(order[0], "in");
        assert_eq!(order[3], "out");
        assert!(order[1..3].contains(&"left".to_string()));
        assert!(order[1..3].contains(&"right".to_string()));
    }

    fn internal() -> ComponentKind {
        internal_with_rate(logit_core::DEFAULT_SPAN_SAMPLE_RATE)
    }

    fn internal_with_rate(span_sample_rate: f64) -> ComponentKind {
        ComponentKind::Internal {
            interval: Duration::from_secs(10),
            span_sample_rate,
            logs: logit_config::InternalLogs::default(),
        }
    }

    #[test]
    fn kind_name_matches_the_configs_own_type_tag() {
        assert_eq!(kind_name(&listener()), "statsd_in");
        assert_eq!(kind_name(&internal()), "internal");
        assert_eq!(kind_name(&sink()), "influxdb_out");
    }

    /// A valid router -> target config resolves (rules 47-51).
    #[test]
    fn a_target_and_its_router_resolve() {
        let graph = resolve(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["t"], lua()),
            ("t", vec![], vec![], target()),
            ("out", vec!["t", "r"], vec![], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["t"].role(), Role::Target);
        assert_eq!(graph.components["r"].targets, vec!["t".to_string()]);
        let at = |id: &str| {
            graph.topological_order.iter().position(|other| other == id).expect("placed")
        };
        assert!(
            at("r") < at("t"),
            "the router must sort before its target: {:?}",
            graph.topological_order
        );
    }

    #[test]
    fn a_route_component_and_its_target_resolve() {
        let graph = resolve(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec![], route(by_attribute("stream"), &[("host", "t")])),
            ("t", vec![], vec![], target()),
            ("out", vec!["r", "t"], vec![], sink()),
        ]))
        .expect("should resolve");
        assert_eq!(graph.components["r"].role(), Role::Transform);
        assert_eq!(graph.components["t"].role(), Role::Target);
        assert_eq!(graph.components["r"].targets, vec!["t".to_string()]);
        let at = |id: &str| {
            graph.topological_order.iter().position(|other| other == id).expect("placed")
        };
        assert!(
            at("r") < at("t"),
            "the router must sort before its target: {:?}",
            graph.topological_order
        );
    }

    // Rules 47-51 (`docs/adr/target-components.md`).

    #[test]
    fn targets_on_a_non_router_kind_is_rejected() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("k", vec!["in"], vec!["t"], keep(vec!["a"])),
            ("r", vec!["in"], vec!["t"], lua()),
            ("t", vec![], vec![], target()),
            ("out", vec!["t", "k", "r"], vec![], sink()),
        ]));
        assert!(err.contains("'k'") && err.contains("'targets' is only meaningful"), "got: {err}");
    }

    #[test]
    fn targets_on_a_route_is_rejected() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["t"], route(by_attribute("stream"), &[("host", "t")])),
            ("t", vec![], vec![], target()),
            ("out", vec!["t"], vec![], sink()),
        ]));
        assert!(
            err.contains("'r'") && err.contains("a route's targets are its routes: values"),
            "got: {err}"
        );
    }

    #[test]
    fn a_target_ref_must_resolve() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["typo"], lua()),
            ("out", vec!["r"], vec![], sink()),
        ]));
        assert!(err.contains("unknown target 'typo'"), "got: {err}");
    }

    /// A router directing at an ordinary component is a `sources:` entry written on the wrong
    /// side of the edge, so the message says so rather than only naming the kind.
    #[test]
    fn a_target_ref_must_name_a_target_kind() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["k"], lua()),
            ("k", vec!["in"], vec![], keep(vec!["a"])),
            ("out", vec!["r", "k"], vec![], sink()),
        ]));
        assert!(err.contains("target 'k' is a keep, not a target"), "got: {err}");
        assert!(err.contains("'sources'"), "got: {err}");
    }

    #[test]
    fn a_router_may_not_target_itself() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["r"], lua()),
            ("out", vec!["r"], vec![], sink()),
        ]));
        assert!(err.contains("'r' directs at itself as a target"), "got: {err}");
    }

    #[test]
    fn a_duplicate_lua_target_is_rejected() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["t", "t"], lua()),
            ("t", vec![], vec![], target()),
            ("out", vec!["t"], vec![], sink()),
        ]));
        assert!(err.contains("lists target 't' more than once"), "got: {err}");
    }

    /// Many-to-one is what `routes:` is for, so it is legal and collapses to one slot.
    #[test]
    fn a_many_to_one_route_map_is_legal_and_collapses_to_one_slot() {
        let router = component(
            vec!["in"],
            vec![],
            route(by_attribute("stream"), &[("host", "t"), ("node", "t"), ("app", "other")]),
        );
        // `routes:` is a `BTreeMap`, so slot order follows the *key* order: app, host, node.
        assert_eq!(targets_of(&router), vec!["other", "t"]);
    }

    #[test]
    fn a_target_may_not_declare_sources() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["t"], lua()),
            ("t", vec!["in"], vec![], target()),
            ("out", vec!["t"], vec![], sink()),
        ]));
        assert!(err.contains("'t' is a target and cannot declare sources"), "got: {err}");
    }

    #[test]
    fn a_target_needs_a_consumer() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["t"], lua()),
            ("t", vec![], vec![], target()),
            ("out", vec!["r"], vec![], sink()),
        ]));
        assert!(err.contains("'t' has no consumers"), "got: {err}");
    }

    #[test]
    fn a_target_needs_a_directing_router() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("k", vec!["in"], vec![], keep(vec!["a"])),
            ("t", vec![], vec![], target()),
            ("out", vec!["k", "t"], vec![], sink()),
        ]));
        assert!(err.contains("'t': is a target that no router directs to"), "got: {err}");
    }

    /// Rule 50: a router whose every event is routed needs no ordinary consumer, so rule 7 doesn't
    /// reject it.
    #[test]
    fn a_router_with_targets_but_no_consumers_resolves() {
        let graph = resolve(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["t"], lua()),
            ("t", vec![], vec![], target()),
            ("out", vec!["t"], vec![], sink()),
        ]))
        .expect("rule 50 should let a targets-only router resolve");
        assert!(graph.components["r"].consumers.is_empty(), "r has no ordinary consumers");
        assert_eq!(graph.components["r"].targets, vec!["t".to_string()]);
    }

    #[test]
    fn a_route_with_empty_routes_is_rejected() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec![], route(by_attribute("stream"), &[])),
            ("out", vec!["r"], vec![], sink()),
        ]));
        assert!(err.contains("no 'routes' configured can only ever be a no-op"), "got: {err}");
    }

    #[test]
    fn a_route_with_an_empty_routes_key_is_rejected() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec![], route(by_attribute("stream"), &[("", "t")])),
            ("t", vec![], vec![], target()),
            ("out", vec!["t"], vec![], sink()),
        ]));
        assert!(err.contains("a route 'routes' key must not be empty"), "got: {err}");
    }

    /// Rule 51 runs before rule 48, so this reads as an empty value rather than an unknown target
    /// `''`.
    #[test]
    fn a_route_with_an_empty_routes_value_is_rejected() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec![], route(by_attribute("stream"), &[("host", "")])),
            ("out", vec!["r"], vec![], sink()),
        ]));
        assert!(err.contains("a route 'routes' value must not be empty"), "got: {err}");
    }

    #[test]
    fn a_route_by_attribute_with_an_empty_key_name_is_rejected() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec![], route(by_attribute(""), &[("host", "t")])),
            ("t", vec![], vec![], target()),
            ("out", vec!["t"], vec![], sink()),
        ]));
        assert!(err.contains("'by: {attribute: ..}' key name must not be empty"), "got: {err}");
    }

    /// Rule 5: router -> target edges count, so a loop closed through a target is a cycle.
    #[test]
    fn a_cycle_through_a_target_is_detected() {
        let err = expect_err(cfg_with_targets(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in", "x"], vec!["t"], lua()),
            ("t", vec![], vec![], target()),
            ("x", vec!["t"], vec![], keep(vec!["a"])),
        ]));
        assert_eq!(err, "component graph has a cycle: r -> t -> x -> r");
    }

    #[test]
    fn topological_order_places_a_target_after_its_router() {
        let components = components_map(vec![
            ("in", vec![], vec![], listener()),
            ("r", vec!["in"], vec!["t"], lua()),
            ("t", vec![], vec![], target()),
            ("out", vec!["t"], vec![], sink()),
        ]);
        let order = topological_order(&components).expect("acyclic");
        assert_eq!(order, vec!["in", "r", "t", "out"]);
    }

    /// Fan-in at a target counts both inbound edges, so the target sorts after both routers.
    #[test]
    fn a_target_fed_by_two_routers_has_indegree_two() {
        let components = components_map(vec![
            ("in", vec![], vec![], listener()),
            ("r1", vec!["in"], vec!["t"], lua()),
            ("r2", vec!["in"], vec!["t"], lua()),
            ("t", vec![], vec![], target()),
            ("out", vec!["t"], vec![], sink()),
        ]);
        let order = topological_order(&components).expect("acyclic");
        let at = |id: &str| order.iter().position(|other| other == id).expect("every id is placed");
        assert_eq!(order.len(), 5);
        assert!(at("t") > at("r1") && at("t") > at("r2"), "got: {order:?}");
    }

    #[test]
    fn target_edges_keeps_the_route_key_and_duplicates() {
        let router = component(
            vec!["in"],
            vec![],
            route(by_attribute("stream"), &[("host", "t"), ("node", "t")]),
        );
        assert_eq!(target_edges(&router), vec![(Some("host"), "t"), (Some("node"), "t")]);
        // A `lua` target list has no route key: the destination is chosen in the script.
        let script = component(vec!["in"], vec!["a", "b"], lua());
        assert_eq!(target_edges(&script), vec![(None, "a"), (None, "b")]);
        assert_eq!(targets_of(&script), vec!["a", "b"]);
    }

    #[test]
    fn internal_resolves_as_a_listener() {
        let graph = resolve(cfg(vec![("self", vec![], internal()), ("out", vec!["self"], sink())]))
            .expect("should resolve");
        assert_eq!(graph.components["self"].role(), Role::Listener);
    }

    #[test]
    fn a_second_internal_component_is_rejected() {
        let err = expect_err(cfg(vec![
            ("self", vec![], internal()),
            ("self2", vec![], internal()),
            ("out", vec!["self", "self2"], sink()),
        ]));
        assert!(err.contains("more than one 'internal' component"), "got: {err}");
    }

    #[test]
    fn internal_with_zero_interval_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "self",
                vec![],
                ComponentKind::Internal {
                    interval: Duration::ZERO,
                    span_sample_rate: logit_core::DEFAULT_SPAN_SAMPLE_RATE,
                    logs: logit_config::InternalLogs::default(),
                },
            ),
            ("out", vec!["self"], sink()),
        ]));
        assert!(err.contains("flush interval of 0s"), "got: {err}");
    }

    #[test]
    fn resolve_rejects_a_span_sample_rate_above_one() {
        let err = expect_err(cfg(vec![
            ("self", vec![], internal_with_rate(1.5)),
            ("out", vec!["self"], sink()),
        ]));
        assert!(
            err.contains("span_sample_rate") && err.contains("between 0.0 and 1.0"),
            "got: {err}"
        );
    }

    #[test]
    fn resolve_rejects_a_span_sample_rate_below_zero() {
        let err = expect_err(cfg(vec![
            ("self", vec![], internal_with_rate(-0.1)),
            ("out", vec!["self"], sink()),
        ]));
        assert!(
            err.contains("span_sample_rate") && err.contains("between 0.0 and 1.0"),
            "got: {err}"
        );
    }

    #[test]
    fn resolve_rejects_a_span_sample_rate_that_is_not_finite() {
        let err = expect_err(cfg(vec![
            ("self", vec![], internal_with_rate(f64::NAN)),
            ("out", vec!["self"], sink()),
        ]));
        assert!(err.contains("span_sample_rate") && err.contains("finite"), "got: {err}");
    }

    fn non_default_buffer() -> BufferConfig {
        BufferConfig { max_batches: 4096, ..BufferConfig::default() }
    }

    #[test]
    fn a_non_default_buffer_on_a_listener_is_rejected() {
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), non_default_buffer()),
            ("out", vec!["in"], sink(), BufferConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'buffer' is only meaningful on a sink"), "got: {err}");
    }

    #[test]
    fn a_non_default_buffer_on_a_transform_is_rejected() {
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            (
                "agg",
                vec!["in"],
                ComponentKind::Aggregate {
                    interval: Duration::from_secs(10),
                    temporality: logit_config::AggregateTemporality::default(),
                    series_retention: 5,
                    max_retained_series: 10_000,
                    distributions: logit_config::Distributions::default(),
                    max_samples_per_series: 1000,
                    sets: logit_config::Sets::default(),
                    max_set_members_per_series: 1000,
                },
                non_default_buffer(),
            ),
            ("out", vec!["agg"], sink(), BufferConfig::default()),
        ]));
        assert!(err.contains("'agg'"), "got: {err}");
        assert!(err.contains("'buffer' is only meaningful on a sink"), "got: {err}");
    }

    #[test]
    fn a_non_default_buffer_on_a_lua_component_is_rejected() {
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("enrich", vec!["in"], lua(), non_default_buffer()),
            ("out", vec!["enrich"], sink(), BufferConfig::default()),
        ]));
        assert!(err.contains("'enrich'"), "got: {err}");
        assert!(err.contains("'buffer' is only meaningful on a sink"), "got: {err}");
    }

    #[test]
    fn a_non_default_buffer_on_a_sink_validates_fine() {
        let graph = resolve(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out", vec!["in"], sink(), non_default_buffer()),
        ]))
        .expect("a buffer block on a sink should validate fine");
        assert_eq!(graph.components["out"].buffer.max_batches, 4096);
    }

    #[test]
    fn a_sinks_buffer_with_zero_max_batches_is_rejected() {
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out", vec!["in"], sink(), BufferConfig { max_batches: 0, ..BufferConfig::default() }),
        ]));
        assert!(err.contains("'out'"), "got: {err}");
        assert!(err.contains("max_batches"), "got: {err}");
    }

    #[test]
    fn a_sinks_buffer_with_zero_max_bytes_is_rejected() {
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out", vec!["in"], sink(), BufferConfig { max_bytes: 0, ..BufferConfig::default() }),
        ]));
        assert!(err.contains("'out'"), "got: {err}");
        assert!(err.contains("max_bytes"), "got: {err}");
    }

    #[test]
    fn a_default_buffer_on_a_non_sink_validates_fine() {
        // An explicit but all-default `buffer: {}` is indistinguishable from an omitted block; rule
        // 14 rejects only a non-default value.
        let graph = resolve(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("enrich", vec!["in"], lua(), BufferConfig::default()),
            ("out", vec!["enrich"], sink(), BufferConfig::default()),
        ]))
        .expect("a default buffer block on a non-sink should validate fine");
        assert_eq!(graph.components["enrich"].buffer, BufferConfig::default());
    }

    fn non_default_receive() -> ReceiveConfig {
        ReceiveConfig { max_datagrams: 4096, ..ReceiveConfig::default() }
    }

    #[test]
    fn a_non_default_receive_on_a_transform_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            ("in", vec![], listener(), ReceiveConfig::default()),
            (
                "agg",
                vec!["in"],
                ComponentKind::Aggregate {
                    interval: Duration::from_secs(10),
                    temporality: logit_config::AggregateTemporality::default(),
                    series_retention: 5,
                    max_retained_series: 10_000,
                    distributions: logit_config::Distributions::default(),
                    max_samples_per_series: 1000,
                    sets: logit_config::Sets::default(),
                    max_set_members_per_series: 1000,
                },
                non_default_receive(),
            ),
            ("out", vec!["agg"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'agg'"), "got: {err}");
        assert!(
            err.contains("'receive' is only meaningful on a datagram, stream or tail listener"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_default_receive_on_a_sink_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            ("in", vec![], listener(), ReceiveConfig::default()),
            ("out", vec!["in"], sink(), non_default_receive()),
        ]));
        assert!(err.contains("'out'"), "got: {err}");
        assert!(
            err.contains("'receive' is only meaningful on a datagram, stream or tail listener"),
            "got: {err}"
        );
    }

    /// Rule 17: `internal` is a listener by role but has no socket, queue, or decoder, so
    /// `receive:` on it is rejected like on a sink or transform.
    #[test]
    fn a_non_default_receive_on_internal_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            ("in", vec![], listener(), ReceiveConfig::default()),
            ("self", vec![], internal(), non_default_receive()),
            ("out", vec!["in", "self"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'self'"), "got: {err}");
        assert!(
            err.contains("'receive' is only meaningful on a datagram, stream or tail listener"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_default_receive_on_a_datagram_listener_validates_fine() {
        let graph = resolve(cfg_with_receive(vec![
            ("in", vec![], listener(), non_default_receive()),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("a receive block on a datagram listener should validate fine");
        assert_eq!(graph.components["in"].receive.max_datagrams, 4096);
    }

    #[test]
    fn a_default_receive_on_a_non_listener_validates_fine() {
        // An explicit but all-default `receive: {}` is indistinguishable from an omitted block;
        // rule 17 rejects only a non-default value.
        let graph = resolve(cfg_with_receive(vec![
            ("in", vec![], listener(), ReceiveConfig::default()),
            ("enrich", vec!["in"], lua(), ReceiveConfig::default()),
            ("out", vec!["enrich"], sink(), ReceiveConfig::default()),
        ]))
        .expect("a default receive block on a non-listener should validate fine");
        assert_eq!(graph.components["enrich"].receive, ReceiveConfig::default());
    }

    #[test]
    fn a_listeners_receive_with_zero_max_datagrams_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                listener(),
                ReceiveConfig { max_datagrams: 0, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("max_datagrams"), "got: {err}");
    }

    #[test]
    fn a_listeners_receive_with_zero_max_bytes_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            ("in", vec![], listener(), ReceiveConfig { max_bytes: 0, ..ReceiveConfig::default() }),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("max_bytes"), "got: {err}");
    }

    #[test]
    fn a_listeners_receive_with_zero_batch_max_events_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                listener(),
                ReceiveConfig { batch_max_events: 0, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("batch_max_events"), "got: {err}");
    }

    #[test]
    fn a_listeners_receive_with_zero_batch_max_bytes_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                listener(),
                ReceiveConfig { batch_max_bytes: 0, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("batch_max_bytes"), "got: {err}");
    }

    /// Rule 18's `read_batch` arm: `0` means `recvmmsg`'s `vlen` is zero, which reads nothing,
    /// forever -- the same impossible bound the four above are.
    #[test]
    fn a_listeners_receive_with_zero_read_batch_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            ("in", vec![], listener(), ReceiveConfig { read_batch: 0, ..ReceiveConfig::default() }),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("read_batch"), "got: {err}");
    }

    /// Rule 57: a `read_batch` above `MAX_READ_BATCH` is rejected, with `UIO_MAXIOV` named in the
    /// message as the number's source.
    #[test]
    fn a_listeners_read_batch_above_uio_maxiov_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                listener(),
                ReceiveConfig { read_batch: MAX_READ_BATCH + 1, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("read_batch"), "got: {err}");
        assert!(err.contains("UIO_MAXIOV"), "got: {err}");
    }

    /// The boundary itself is legal: `MAX_READ_BATCH` is the largest `read_batch` this rule
    /// accepts, not the first one it rejects.
    #[test]
    fn a_listeners_read_batch_of_exactly_uio_maxiov_validates_fine() {
        let graph = resolve(cfg_with_receive(vec![
            (
                "in",
                vec![],
                listener(),
                ReceiveConfig { read_batch: MAX_READ_BATCH, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("1024 is the ceiling, not the first rejected value");
        assert_eq!(graph.components["in"].receive.read_batch, MAX_READ_BATCH);
    }

    /// A `read_batch` above `max_datagrams` is legal: `push_many` handles a batch larger than the
    /// whole queue (rule 57).
    #[test]
    fn a_read_batch_larger_than_max_datagrams_validates_fine() {
        let graph = resolve(cfg_with_receive(vec![
            (
                "in",
                vec![],
                listener(),
                ReceiveConfig { read_batch: 64, max_datagrams: 4, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("a read batch bigger than the queue is legal -- push_many handles it per item");
        assert_eq!(graph.components["in"].receive.read_batch, 64);
        assert_eq!(graph.components["in"].receive.max_datagrams, 4);
    }

    /// Rule 17's queue-only list has to include `read_batch`: it sizes one `recvmmsg` read and
    /// the matching `pop_many` off a receive queue a stream listener does not have.
    #[test]
    fn a_read_batch_on_a_tcp_syslog_in_is_rejected_naming_the_field() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                syslog_in(SyslogTransport::Tcp, false),
                ReceiveConfig { read_batch: 16, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'receive.read_batch'"), "got: {err}");
        assert!(err.contains("a stream listener has no receive queue"), "got: {err}");
    }

    /// The default is the same 64 `UdpListenerConfig::default` carries. They live in different
    /// crates (`logit-inputs` doesn't depend on `logit-config`), so only a test holds them
    /// together.
    #[test]
    fn the_default_read_batch_is_sixty_four() {
        assert_eq!(ReceiveConfig::default().read_batch, 64);
        assert_eq!(logit_config::default_read_batch(), 64);
        assert_eq!(MAX_READ_BATCH, 1024);
    }

    /// Unlike the four count/byte bounds above, `batch_flush_interval: 0s` is a meaningful
    /// setting ("no flush timer") -- rule 18 must not reject it.
    #[test]
    fn a_listeners_receive_with_a_zero_batch_flush_interval_validates_fine() {
        let graph = resolve(cfg_with_receive(vec![
            (
                "in",
                vec![],
                listener(),
                ReceiveConfig { batch_flush_interval: Duration::ZERO, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("a zero batch_flush_interval should validate fine -- it means 'no timer'");
        assert_eq!(graph.components["in"].receive.batch_flush_interval, Duration::ZERO);
    }

    // -- syslog_in: transports, rules 17/18/43 --------------------------------------------------

    /// A `syslog_in` on either transport, with or without a `tls:` block -- the three-way shape
    /// rules 17/18/43 all key on (`docs/adr/syslog-tcp-ingress-and-tls.md`).
    fn syslog_in(transport: SyslogTransport, tls: bool) -> ComponentKind {
        syslog_in_with_handshake_timeout(transport, tls, default_handshake_timeout())
    }

    /// [`syslog_in`] with rule 45's knob exposed -- both the zero case and the
    /// set-but-ignored-under-UDP case need to name it.
    fn syslog_in_with_handshake_timeout(
        transport: SyslogTransport,
        tls: bool,
        handshake_timeout: Duration,
    ) -> ComponentKind {
        ComponentKind::SyslogIn {
            bind: "127.0.0.1:0".to_string(),
            transport,
            tls: tls.then(|| logit_config::TlsServerConfig {
                cert_file: "server.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
            handshake_timeout,
            idle_timeout: None,
        }
    }

    /// [`syslog_in`] with rule 53's knob exposed instead -- the `Option` that rule reads.
    fn syslog_in_with_idle_timeout(
        transport: SyslogTransport,
        idle_timeout: Option<Duration>,
    ) -> ComponentKind {
        ComponentKind::SyslogIn {
            bind: "127.0.0.1:0".to_string(),
            transport,
            tls: None,
            handshake_timeout: default_handshake_timeout(),
            idle_timeout,
        }
    }

    /// Rule 43: DTLS is out of scope, so a `tls:` block under `transport: udp` could never take
    /// effect -- rejected rather than silently ignored.
    #[test]
    fn tls_on_a_udp_syslog_in_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], syslog_in(SyslogTransport::Udp, true)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'tls:' needs 'transport: tcp'"), "got: {err}");
        assert!(err.contains("DTLS"), "got: {err}");
    }

    /// Rule 43's other side: TLS over TCP is RFC 5425.
    #[test]
    fn tls_on_a_tcp_syslog_in_validates_fine() {
        resolve(cfg(vec![
            ("in", vec![], syslog_in(SyslogTransport::Tcp, true)),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a TLS-terminating TCP syslog_in is the RFC 5425 shape");
    }

    /// Rule 43 doesn't touch a plaintext TCP listener.
    #[test]
    fn a_plaintext_tcp_syslog_in_validates_fine() {
        resolve(cfg(vec![
            ("in", vec![], syslog_in(SyslogTransport::Tcp, false)),
            ("out", vec!["in"], sink()),
        ]))
        .expect("TCP without TLS is a perfectly ordinary syslog listener");
    }

    /// Rule 17: a TCP `syslog_in` has no receive queue, so a queue field is rejected by name, with
    /// the reason in the message.
    #[test]
    fn a_receive_queue_field_on_a_tcp_syslog_in_is_rejected_naming_the_field() {
        let err = expect_err(cfg_with_receive(vec![
            ("in", vec![], syslog_in(SyslogTransport::Tcp, false), non_default_receive()),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'receive.max_datagrams'"), "got: {err}");
        assert!(err.contains("a stream listener has no receive queue"), "got: {err}");
        assert!(err.contains("flow control"), "got: {err}");
    }

    /// The batch-assembly half of rule 17 still applies on TCP -- per connection, which is what
    /// the config field's own doc comment warns about.
    #[test]
    fn a_receive_batch_override_on_a_tcp_syslog_in_validates_fine() {
        let graph = resolve(cfg_with_receive(vec![
            (
                "in",
                vec![],
                syslog_in(SyslogTransport::Tcp, false),
                ReceiveConfig { batch_max_events: 1, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("batch_max_events is one of the fields a stream listener may override");
        assert_eq!(graph.components["in"].receive.batch_max_events, 1);
    }

    /// A UDP `syslog_in` keeps its queue fields.
    #[test]
    fn a_receive_queue_field_on_a_udp_syslog_in_still_validates_fine() {
        let graph = resolve(cfg_with_receive(vec![
            ("in", vec![], syslog_in(SyslogTransport::Udp, false), non_default_receive()),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("a UDP syslog_in is still a datagram listener with a real receive queue");
        assert_eq!(graph.components["in"].receive.max_datagrams, 4096);
    }

    /// Rule 18 reaches a TCP `syslog_in` through `is_stream_listener`: with `batch_max_events: 0`
    /// it would accumulate forever.
    #[test]
    fn a_zero_batch_max_events_on_a_tcp_syslog_in_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                syslog_in(SyslogTransport::Tcp, false),
                ReceiveConfig { batch_max_events: 0, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'receive.batch_max_events' must be at least 1"), "got: {err}");
    }

    // -- tail_in / docker_in ------------------------------------------------------------------

    #[test]
    fn a_receive_batch_override_on_a_tail_listener_validates_fine() {
        let graph = resolve(cfg_with_receive(vec![
            (
                "in",
                vec![],
                tail_in(vec!["/var/log/app.log"]),
                ReceiveConfig { batch_max_events: 1, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("batch_max_events is one of the fields a tail listener may override");
        assert_eq!(graph.components["in"].receive.batch_max_events, 1);
    }

    #[test]
    fn a_receive_queue_field_on_a_tail_listener_is_rejected_naming_the_field() {
        let err = expect_err(cfg_with_receive(vec![
            ("in", vec![], tail_in(vec!["/var/log/app.log"]), non_default_receive()),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'receive.max_datagrams'"), "got: {err}");
        assert!(err.contains("has no receive queue"), "got: {err}");
    }

    #[test]
    fn a_zero_batch_max_events_on_a_tail_listener_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                tail_in(vec!["/var/log/app.log"]),
                ReceiveConfig { batch_max_events: 0, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("batch_max_events"), "got: {err}");
    }

    #[test]
    fn tail_in_with_no_paths_is_rejected() {
        let err =
            expect_err(cfg(vec![("in", vec![], tail_in(vec![])), ("out", vec!["in"], sink())]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'paths' must name at least one file"), "got: {err}");
    }

    #[test]
    fn tail_in_with_an_empty_paths_entry_is_rejected() {
        let err =
            expect_err(cfg(vec![("in", vec![], tail_in(vec![""])), ("out", vec!["in"], sink())]));
        assert!(err.contains("'paths' has an empty entry"), "got: {err}");
    }

    #[test]
    fn tail_in_with_a_star_in_a_directory_component_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], tail_in(vec!["/var/*/app.log"])),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("outside the final path component"), "got: {err}");
    }

    #[test]
    fn tail_in_with_a_trailing_star_validates_fine() {
        resolve(cfg(vec![
            ("in", vec![], tail_in(vec!["/var/log/app/*.log"])),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a '*' in only the final path component should validate fine");
    }

    #[test]
    fn docker_in_with_explicit_containers_validates_fine() {
        resolve(cfg(vec![
            ("in", vec![], docker_in(vec!["nginx"], false)),
            ("out", vec!["in"], sink()),
        ]))
        .expect("an explicit non-empty containers list should validate fine");
    }

    #[test]
    fn docker_in_with_discover_and_no_containers_validates_fine() {
        resolve(cfg(vec![("in", vec![], docker_in(vec![], true)), ("out", vec!["in"], sink())]))
            .expect("discover: true alone should validate fine");
    }

    #[test]
    fn docker_in_with_no_containers_and_no_discover_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], docker_in(vec![], false)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("must name at least one container"), "got: {err}");
    }

    #[test]
    fn docker_in_with_an_empty_containers_entry_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], docker_in(vec![""], false)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'containers' has an empty entry"), "got: {err}");
    }

    #[test]
    fn docker_in_with_a_duplicate_containers_entry_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], docker_in(vec!["nginx", "nginx"], false)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("duplicate entry 'nginx'"), "got: {err}");
    }

    #[test]
    fn docker_in_with_an_empty_root_is_rejected() {
        let mut kind = docker_in(vec!["nginx"], false);
        if let ComponentKind::DockerIn { root, .. } = &mut kind {
            *root = String::new();
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("'root' must not be empty"), "got: {err}");
    }

    #[test]
    fn docker_in_with_an_empty_labels_entry_is_rejected() {
        let mut kind = docker_in(vec!["nginx"], false);
        if let ComponentKind::DockerIn { labels, .. } = &mut kind {
            labels.push(String::new());
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("'labels' has an empty entry"), "got: {err}");
    }

    #[test]
    fn a_zero_poll_interval_on_docker_in_is_rejected() {
        let mut kind = docker_in(vec!["nginx"], false);
        if let ComponentKind::DockerIn { tail, .. } = &mut kind {
            tail.poll_interval = Duration::ZERO;
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("'poll_interval' must be greater than 0s"), "got: {err}");
    }

    #[test]
    fn a_zero_poll_interval_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                ComponentKind::TailIn {
                    paths: vec!["/var/log/app.log".to_string()],
                    tail: logit_config::TailOptions {
                        poll_interval: Duration::ZERO,
                        ..logit_config::TailOptions::default()
                    },
                },
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'poll_interval' must be greater than 0s"), "got: {err}");
    }

    #[test]
    fn a_zero_checkpoint_interval_on_tail_in_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                ComponentKind::TailIn {
                    paths: vec!["/var/log/app.log".to_string()],
                    tail: logit_config::TailOptions {
                        checkpoint_interval: Duration::ZERO,
                        ..logit_config::TailOptions::default()
                    },
                },
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'checkpoint_interval' must be greater than 0s"), "got: {err}");
    }

    #[test]
    fn a_zero_max_line_bytes_on_tail_in_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                ComponentKind::TailIn {
                    paths: vec!["/var/log/app.log".to_string()],
                    tail: logit_config::TailOptions {
                        max_line_bytes: 0,
                        ..logit_config::TailOptions::default()
                    },
                },
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'max_line_bytes' must be greater than 0"), "got: {err}");
    }

    #[test]
    fn file_out_with_neither_rotate_trigger_set_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], file_out(logit_config::RotateConfig::default())),
        ]));
        assert!(err.contains("'out'"), "got: {err}");
        assert!(
            err.contains("needs at least one of 'rotate.max_bytes' or 'rotate.interval'"),
            "got: {err}"
        );
        assert!(err.contains("'target: events.log'"), "got: {err}");
    }

    #[test]
    fn file_out_with_max_bytes_alone_validates_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                file_out(logit_config::RotateConfig {
                    max_bytes: Some(1024),
                    interval: None,
                    max_files: 5,
                }),
            ),
        ]))
        .expect("max_bytes alone should validate fine");
    }

    #[test]
    fn file_out_with_interval_alone_validates_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                file_out(logit_config::RotateConfig {
                    max_bytes: None,
                    interval: Some(logit_config::RotateInterval::Daily),
                    max_files: 5,
                }),
            ),
        ]))
        .expect("interval alone should validate fine");
    }

    #[test]
    fn file_out_with_zero_max_bytes_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                file_out(logit_config::RotateConfig {
                    max_bytes: Some(0),
                    interval: None,
                    max_files: 5,
                }),
            ),
        ]));
        assert!(err.contains("'rotate.max_bytes' must be at least 1"), "got: {err}");
    }

    #[test]
    fn file_out_with_zero_max_files_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                file_out(logit_config::RotateConfig {
                    max_bytes: Some(1024),
                    interval: None,
                    max_files: 0,
                }),
            ),
        ]));
        assert!(err.contains("'rotate.max_files' must be at least 1"), "got: {err}");
    }

    #[test]
    fn file_out_with_max_files_over_the_ceiling_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                file_out(logit_config::RotateConfig {
                    max_bytes: Some(1024),
                    interval: None,
                    max_files: logit_config::MAX_ROTATE_FILES + 1,
                }),
            ),
        ]));
        assert!(err.contains("'out'"), "got: {err}");
        assert!(err.contains("'rotate.max_files' must be at most 1000"), "got: {err}");
    }

    #[test]
    fn file_out_with_max_files_at_the_ceiling_is_accepted() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                file_out(logit_config::RotateConfig {
                    max_bytes: Some(1024),
                    interval: None,
                    max_files: logit_config::MAX_ROTATE_FILES,
                }),
            ),
        ]))
        .expect("max_files at the ceiling should validate fine");
    }

    #[test]
    fn file_out_with_compression_set_under_the_default_human_format_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], file_out_with_format(StreamFormat::Human, Compression::Lz4)),
        ]));
        assert!(err.contains("'compression' only applies under 'format: native'"), "got: {err}");
    }

    #[test]
    fn stdio_out_with_compression_set_under_the_default_human_format_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], stdio_out_with_format(StreamFormat::Human, Compression::Lz4)),
        ]));
        assert!(err.contains("'compression' only applies under 'format: native'"), "got: {err}");
    }

    #[test]
    fn file_out_with_format_native_and_compression_lz4_validates_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], file_out_with_format(StreamFormat::Native, Compression::Lz4)),
        ]))
        .expect("format: native with compression: lz4 should validate fine");
    }

    #[test]
    fn stdio_out_with_format_native_and_no_compression_validates_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], stdio_out_with_format(StreamFormat::Native, Compression::None)),
        ]))
        .expect("format: native with the default compression should validate fine");
    }

    fn logit_out_with_tls(tls: Option<logit_config::TlsClientConfig>) -> ComponentKind {
        ComponentKind::LogitOut {
            endpoint: "central:5140".to_string(),
            compression: Compression::None,
            tls,
            request_timeout: Duration::from_secs(10),
        }
    }

    fn logit_in_with_max_frame_bytes(max_frame_bytes: Option<u64>) -> ComponentKind {
        ComponentKind::LogitIn {
            bind: "0.0.0.0:5140".to_string(),
            tls: None,
            max_frame_bytes,
            handshake_timeout: default_handshake_timeout(),
            idle_timeout: None,
        }
    }

    /// Rule 45's `logit_in` shape -- the only field that rule looks at.
    fn logit_in_with_handshake_timeout(handshake_timeout: Duration) -> ComponentKind {
        ComponentKind::LogitIn {
            bind: "0.0.0.0:5140".to_string(),
            tls: None,
            max_frame_bytes: None,
            handshake_timeout,
            idle_timeout: None,
        }
    }

    /// Rule 53's `logit_in` shape. `logit_in` is TCP by construction, so only the zero check can
    /// fire.
    fn logit_in_with_idle_timeout(idle_timeout: Option<Duration>) -> ComponentKind {
        ComponentKind::LogitIn {
            bind: "0.0.0.0:5140".to_string(),
            tls: None,
            max_frame_bytes: None,
            handshake_timeout: default_handshake_timeout(),
            idle_timeout,
        }
    }

    /// Rule 45's `otlp_in` shape -- likewise.
    fn otlp_in_with_handshake_timeout(handshake_timeout: Duration) -> ComponentKind {
        ComponentKind::OtlpIn {
            bind: "0.0.0.0:4317".to_string(),
            protocol: logit_config::OtlpProtocol::Http,
            tls: None,
            handshake_timeout,
            idle_timeout: None,
        }
    }

    /// [`otlp_in_with_handshake_timeout`] with rule 53's knob exposed instead. `otlp_in` has no
    /// datagram transport, so there is no `transport` parameter.
    fn otlp_in_with_idle_timeout(idle_timeout: Option<Duration>) -> ComponentKind {
        ComponentKind::OtlpIn {
            bind: "0.0.0.0:4317".to_string(),
            protocol: logit_config::OtlpProtocol::Http,
            tls: None,
            handshake_timeout: default_handshake_timeout(),
            idle_timeout,
        }
    }

    // ---- Rule 44: `syslog_out`'s `tls:` block -------------------------------------------------

    fn syslog_out_with_tls(
        transport: logit_config::SyslogTransport,
        tls: Option<logit_config::TlsClientConfig>,
    ) -> ComponentKind {
        ComponentKind::SyslogOut {
            endpoint: "relay:6514".to_string(),
            transport,
            format: logit_config::SyslogFormat::default(),
            facility: logit_config::SyslogFacility::default(),
            hostname: None,
            app_name: None,
            max_message_bytes: 8192,
            connect_timeout: Duration::from_secs(5),
            structured_data: None,
            tls,
        }
    }

    #[test]
    fn a_syslog_out_with_cert_file_but_no_key_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            cert_file: Some("client.pem".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], syslog_out_with_tls(logit_config::SyslogTransport::Tcp, Some(tls))),
        ]));
        assert!(err.contains("cert_file") && err.contains("key_file"), "got: {err}");
    }

    #[test]
    fn a_syslog_out_with_insecure_skip_verify_and_ca_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            ca_file: Some("ca.pem".to_string()),
            insecure_skip_verify: true,
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], syslog_out_with_tls(logit_config::SyslogTransport::Tcp, Some(tls))),
        ]));
        assert!(err.contains("insecure_skip_verify") && err.contains("ca_file"), "got: {err}");
    }

    /// Rule 44's third check, which rule 34 lacks: syslog over TLS is RFC 5425, TLS over TCP, so
    /// `tls:` under `transport: udp` is rejected.
    #[test]
    fn a_syslog_out_with_tls_under_transport_udp_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                syslog_out_with_tls(
                    logit_config::SyslogTransport::Udp,
                    Some(logit_config::TlsClientConfig::default()),
                ),
            ),
        ]));
        assert!(err.contains("DTLS") && err.contains("transport: tcp"), "got: {err}");
    }

    // ---- Rule 52: `statsd_out`'s `tls:` block ---------------------------------------------------

    fn statsd_out_with_tls(
        transport: StatsdTransport,
        tls: Option<logit_config::TlsClientConfig>,
    ) -> ComponentKind {
        ComponentKind::StatsdOut {
            endpoint: "relay:8125".to_string(),
            transport,
            format: logit_config::StatsdFormat::default(),
            relative_gauges: false,
            max_packet_bytes: 1432,
            connect_timeout: Duration::from_secs(5),
            tls,
        }
    }

    #[test]
    fn a_statsd_out_with_cert_file_but_no_key_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            cert_file: Some("client.pem".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], statsd_out_with_tls(StatsdTransport::Tcp, Some(tls))),
        ]));
        assert!(err.contains("cert_file") && err.contains("key_file"), "got: {err}");
    }

    #[test]
    fn a_statsd_out_with_insecure_skip_verify_and_ca_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            ca_file: Some("ca.pem".to_string()),
            insecure_skip_verify: true,
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], statsd_out_with_tls(StatsdTransport::Tcp, Some(tls))),
        ]));
        assert!(err.contains("insecure_skip_verify") && err.contains("ca_file"), "got: {err}");
    }

    /// Rule 52's third check, rule 44's verbatim: `tls:` under `transport: udp` is rejected.
    #[test]
    fn a_statsd_out_with_tls_under_transport_udp_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                statsd_out_with_tls(
                    StatsdTransport::Udp,
                    Some(logit_config::TlsClientConfig::default()),
                ),
            ),
        ]));
        assert!(err.contains("DTLS") && err.contains("transport: tcp"), "got: {err}");
    }

    /// A consistent `tls:` block on TCP resolves, so the rejections above are specific.
    #[test]
    fn a_statsd_out_with_a_consistent_tls_block_over_tcp_resolves() {
        let tls = logit_config::TlsClientConfig {
            ca_file: Some("ca.pem".to_string()),
            cert_file: Some("client.pem".to_string()),
            key_file: Some("client.key".to_string()),
            insecure_skip_verify: false,
        };
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], statsd_out_with_tls(StatsdTransport::Tcp, Some(tls))),
        ]))
        .expect("a mutual-TLS statsd_out over tcp is a legal config");
    }

    // ---- Rule 45: `handshake_timeout` on the five TCP listeners ---------------------------------

    /// Rule 45: `0s` would close every connection on accept. One test per kind, since each variant
    /// carries its own field.
    #[test]
    fn a_zero_handshake_timeout_is_rejected_on_a_tcp_syslog_in() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                syslog_in_with_handshake_timeout(SyslogTransport::Tcp, false, Duration::ZERO),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("handshake_timeout") && err.contains("0s"), "got: {err}");
    }

    #[test]
    fn a_zero_handshake_timeout_is_rejected_on_a_logit_in() {
        let err = expect_err(cfg(vec![
            ("in", vec![], logit_in_with_handshake_timeout(Duration::ZERO)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("handshake_timeout") && err.contains("0s"), "got: {err}");
    }

    #[test]
    fn a_zero_handshake_timeout_is_rejected_on_an_otlp_in() {
        let err = expect_err(cfg(vec![
            ("in", vec![], otlp_in_with_handshake_timeout(Duration::ZERO)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("handshake_timeout") && err.contains("0s"), "got: {err}");
    }

    /// Rule 45: a non-default `handshake_timeout` on a UDP `syslog_in` could never take effect.
    #[test]
    fn a_non_default_handshake_timeout_under_transport_udp_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                syslog_in_with_handshake_timeout(
                    SyslogTransport::Udp,
                    false,
                    Duration::from_secs(30),
                ),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("handshake_timeout") && err.contains("transport: tcp"), "got: {err}");
    }

    /// Rule 45 has no context check for `otlp_in`: its budget also bounds a plaintext connection's
    /// first-byte wait (`crates/logit-inputs/src/otlp.rs`'s "peek, not a read"), so a value is live
    /// without `tls:`.
    #[test]
    fn a_non_default_handshake_timeout_on_a_plaintext_otlp_in_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], otlp_in_with_handshake_timeout(Duration::from_secs(30))),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a plaintext otlp_in with a real handshake_timeout should resolve");
    }

    /// The same value on a TLS-terminating `otlp_in` resolves too.
    #[test]
    fn a_non_default_handshake_timeout_on_a_tls_otlp_in_resolves_fine() {
        let kind = ComponentKind::OtlpIn {
            bind: "0.0.0.0:4317".to_string(),
            protocol: logit_config::OtlpProtocol::Http,
            tls: Some(logit_config::TlsServerConfig {
                cert_file: "server.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
            handshake_timeout: Duration::from_secs(30),
            idle_timeout: None,
        };
        resolve(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]))
            .expect("a TLS otlp_in with a real handshake_timeout should resolve");
    }

    /// A UDP `syslog_in` at the default `handshake_timeout` resolves. Deserializes a real config,
    /// so it exercises the `serde` defaulting path rule 45's comparison against
    /// [`default_handshake_timeout`] must agree with.
    #[test]
    fn a_udp_syslog_in_at_the_default_handshake_timeout_resolves_fine() {
        let component: logit_config::Component =
            serde_json::from_str(r#"{"type": "syslog_in", "bind": "127.0.0.1:0"}"#)
                .expect("should deserialize");
        resolve(cfg(vec![("in", vec![], component.kind), ("out", vec!["in"], sink())]))
            .expect("a defaulted handshake_timeout under UDP should resolve");
    }

    /// A non-default value under `transport: tcp` resolves.
    #[test]
    fn a_non_default_handshake_timeout_on_a_tcp_syslog_in_resolves_fine() {
        resolve(cfg(vec![
            (
                "in",
                vec![],
                syslog_in_with_handshake_timeout(
                    SyslogTransport::Tcp,
                    true,
                    Duration::from_secs(30),
                ),
            ),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a TCP syslog_in with a real handshake_timeout should resolve");
    }

    // ---- Rule 53: `idle_timeout` on a TCP listener ----------------------------------------------
    //
    // Rule 45's tests one field over, except that `idle_timeout` is an `Option`, so *any* value
    // under `transport: udp` is rejected, not only a non-default one.

    /// Rule 53: `0s` would close every connection the moment it paused, so it is rejected with a
    /// message saying to omit the field instead. One test per kind with a `transport`.
    #[test]
    fn a_zero_idle_timeout_is_rejected_on_a_syslog_in() {
        let err = expect_err(cfg(vec![
            ("in", vec![], syslog_in_with_idle_timeout(SyslogTransport::Tcp, Some(Duration::ZERO))),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("idle_timeout") && err.contains("omit the field"), "got: {err}");
    }

    #[test]
    fn a_zero_idle_timeout_is_rejected_on_a_graphite_in() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                graphite_in_with_idle_timeout(GraphiteTransport::Tcp, Some(Duration::ZERO)),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("idle_timeout") && err.contains("omit the field"), "got: {err}");
    }

    #[test]
    fn a_zero_idle_timeout_is_rejected_on_a_statsd_in() {
        let err = expect_err(cfg(vec![
            ("in", vec![], statsd_in_with_idle_timeout(StatsdTransport::Tcp, Some(Duration::ZERO))),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("idle_timeout") && err.contains("omit the field"), "got: {err}");
    }

    /// The zero check on `logit_in`, TCP by construction.
    #[test]
    fn a_zero_idle_timeout_is_rejected_on_a_logit_in() {
        let err = expect_err(cfg(vec![
            ("in", vec![], logit_in_with_idle_timeout(Some(Duration::ZERO))),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("idle_timeout") && err.contains("omit the field"), "got: {err}");
    }

    /// The zero check on `otlp_in`, with the identical message: it has no datagram transport
    /// either.
    #[test]
    fn a_zero_idle_timeout_is_rejected_on_an_otlp_in() {
        let err = expect_err(cfg(vec![
            ("in", vec![], otlp_in_with_idle_timeout(Some(Duration::ZERO))),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("idle_timeout") && err.contains("omit the field"), "got: {err}");
    }

    /// Rule 53: any `idle_timeout` on a UDP listener is rejected, since it has no connection to
    /// time out. One test per kind, since each reads its own `transport`.
    #[test]
    fn a_set_idle_timeout_under_transport_udp_is_rejected_on_a_syslog_in() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                syslog_in_with_idle_timeout(SyslogTransport::Udp, Some(Duration::from_secs(300))),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(
            err.contains("idle_timeout")
                && err.contains("transport: tcp")
                && err.contains("syslog_in"),
            "got: {err}"
        );
    }

    #[test]
    fn a_set_idle_timeout_under_transport_udp_is_rejected_on_a_graphite_in() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                graphite_in_with_idle_timeout(
                    GraphiteTransport::Udp,
                    Some(Duration::from_secs(300)),
                ),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(
            err.contains("idle_timeout")
                && err.contains("transport: tcp")
                && err.contains("graphite_in"),
            "got: {err}"
        );
    }

    #[test]
    fn a_set_idle_timeout_under_transport_udp_is_rejected_on_a_statsd_in() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                statsd_in_with_idle_timeout(StatsdTransport::Udp, Some(Duration::from_secs(300))),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(
            err.contains("idle_timeout")
                && err.contains("transport: tcp")
                && err.contains("statsd_in"),
            "got: {err}"
        );
    }

    /// A real value on a connection-oriented transport resolves, on every kind.
    #[test]
    fn a_set_idle_timeout_on_a_tcp_listener_resolves_fine() {
        for kind in [
            syslog_in_with_idle_timeout(SyslogTransport::Tcp, Some(Duration::from_secs(300))),
            graphite_in_with_idle_timeout(GraphiteTransport::Tcp, Some(Duration::from_secs(300))),
            statsd_in_with_idle_timeout(StatsdTransport::Tcp, Some(Duration::from_secs(300))),
            logit_in_with_idle_timeout(Some(Duration::from_secs(300))),
            otlp_in_with_idle_timeout(Some(Duration::from_secs(300))),
        ] {
            resolve(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]))
                .expect("a TCP listener with a real idle_timeout should resolve");
        }
    }

    /// A UDP config that omits `idle_timeout` resolves. Deserializes a real config, so it exercises
    /// the `#[serde(default)]` path rule 53's `Option` match must agree with.
    #[test]
    fn a_udp_syslog_in_with_no_idle_timeout_resolves_fine() {
        let component: logit_config::Component =
            serde_json::from_str(r#"{"type": "syslog_in", "bind": "127.0.0.1:0"}"#)
                .expect("should deserialize");
        resolve(cfg(vec![("in", vec![], component.kind), ("out", vec!["in"], sink())]))
            .expect("an absent idle_timeout under UDP should resolve");
    }

    #[test]
    fn a_syslog_out_with_a_consistent_tls_block_over_tcp_validates_fine() {
        let tls = logit_config::TlsClientConfig {
            ca_file: Some("ca.pem".to_string()),
            cert_file: Some("client.pem".to_string()),
            key_file: Some("client.key".to_string()),
            ..Default::default()
        };
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], syslog_out_with_tls(logit_config::SyslogTransport::Tcp, Some(tls))),
        ]))
        .expect("a paired cert_file/key_file over TCP should validate fine");
    }

    #[test]
    fn a_logit_out_with_cert_file_but_no_key_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            cert_file: Some("client.pem".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], logit_out_with_tls(Some(tls))),
        ]));
        assert!(err.contains("cert_file") && err.contains("key_file"), "got: {err}");
    }

    #[test]
    fn a_logit_out_with_insecure_skip_verify_and_ca_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            ca_file: Some("ca.pem".to_string()),
            insecure_skip_verify: true,
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], logit_out_with_tls(Some(tls))),
        ]));
        assert!(err.contains("insecure_skip_verify") && err.contains("ca_file"), "got: {err}");
    }

    #[test]
    fn a_logit_out_with_a_consistent_tls_block_validates_fine() {
        let tls = logit_config::TlsClientConfig {
            cert_file: Some("client.pem".to_string()),
            key_file: Some("client.key".to_string()),
            ..Default::default()
        };
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], logit_out_with_tls(Some(tls))),
        ]))
        .expect("a paired cert_file/key_file should validate fine");
    }

    #[test]
    fn a_logit_in_with_max_frame_bytes_zero_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], logit_in_with_max_frame_bytes(Some(0))),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("max_frame_bytes"), "got: {err}");
    }

    #[test]
    fn a_logit_in_with_max_frame_bytes_over_the_sanity_cap_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                logit_in_with_max_frame_bytes(Some(MAX_SANE_UNCOMPRESSED_LEN as u64 + 1)),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("max_frame_bytes"), "got: {err}");
    }

    #[test]
    fn a_logit_in_with_no_max_frame_bytes_or_a_sane_one_validates_fine() {
        resolve(cfg(vec![
            ("in", vec![], logit_in_with_max_frame_bytes(None)),
            ("out", vec!["in"], sink()),
        ]))
        .expect("an omitted max_frame_bytes should validate fine");
        resolve(cfg(vec![
            ("in", vec![], logit_in_with_max_frame_bytes(Some(MAX_SANE_UNCOMPRESSED_LEN as u64))),
            ("out", vec!["in"], sink()),
        ]))
        .expect("max_frame_bytes at exactly the sanity cap should validate fine");
    }

    #[test]
    fn kind_name_and_role_are_implemented_for_logit_in_and_logit_out() {
        let kind_in = logit_in_with_max_frame_bytes(None);
        assert_eq!(kind_name(&kind_in), "logit_in");
        assert_eq!(role(&kind_in), Role::Listener);
        let kind_out = logit_out_with_tls(None);
        assert_eq!(kind_name(&kind_out), "logit_out");
        assert_eq!(role(&kind_out), Role::Sink);
    }

    fn disk_buffer(path: &str) -> BufferConfig {
        BufferConfig {
            disk: Some(logit_config::DiskBufferConfig {
                path: path.to_string(),
                max_bytes: 1024 * 1024 * 1024,
                segment_bytes: 64 * 1024 * 1024,
                compression: Compression::None,
                checkpoint_interval: std::time::Duration::from_secs(1),
            }),
            ..BufferConfig::default()
        }
    }

    #[test]
    fn a_disk_buffer_with_a_non_default_max_batches_is_rejected() {
        let mut buffer = disk_buffer("spool");
        buffer.max_batches = 4096;
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out", vec!["in"], sink(), buffer),
        ]));
        assert!(err.contains("are ignored once 'buffer.disk' is set"), "got: {err}");
    }

    #[test]
    fn a_disk_buffer_with_a_non_default_max_bytes_is_rejected() {
        let mut buffer = disk_buffer("spool");
        buffer.max_bytes = 1;
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out", vec!["in"], sink(), buffer),
        ]));
        assert!(err.contains("are ignored once 'buffer.disk' is set"), "got: {err}");
    }

    #[test]
    fn a_disk_buffer_with_zero_segment_bytes_is_rejected() {
        let mut buffer = disk_buffer("spool");
        buffer.disk.as_mut().unwrap().segment_bytes = 0;
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out", vec!["in"], sink(), buffer),
        ]));
        assert!(err.contains("'buffer.disk.segment_bytes' must be at least 1"), "got: {err}");
    }

    #[test]
    fn a_disk_buffer_with_zero_max_bytes_is_rejected() {
        let mut buffer = disk_buffer("spool");
        buffer.disk.as_mut().unwrap().max_bytes = 0;
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out", vec!["in"], sink(), buffer),
        ]));
        assert!(err.contains("'buffer.disk.max_bytes' must be at least 1"), "got: {err}");
    }

    #[test]
    fn a_disk_buffer_whose_segment_bytes_exceeds_max_bytes_is_rejected() {
        let mut buffer = disk_buffer("spool");
        {
            let disk = buffer.disk.as_mut().unwrap();
            disk.max_bytes = 1024;
            disk.segment_bytes = 2048;
        }
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out", vec!["in"], sink(), buffer),
        ]));
        assert!(
            err.contains("'buffer.disk.segment_bytes'") && err.contains("must not exceed"),
            "got: {err}"
        );
    }

    #[test]
    fn two_sinks_sharing_a_literal_disk_path_are_rejected() {
        let err = expect_err(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out1", vec!["in"], sink(), disk_buffer("shared")),
            ("out2", vec!["in"], sink(), disk_buffer("shared")),
        ]));
        assert!(err.contains("'out1'") && err.contains("'out2'"), "got: {err}");
        assert!(err.contains("both set 'buffer.disk.path' to 'shared'"), "got: {err}");
    }

    #[test]
    fn two_sinks_with_distinct_disk_paths_validate_fine() {
        resolve(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out1", vec!["in"], sink(), disk_buffer("one")),
            ("out2", vec!["in"], sink(), disk_buffer("two")),
        ]))
        .expect("distinct disk paths should validate fine");
    }

    fn tail_in_checkpointing_to(checkpoint_path: &str) -> ComponentKind {
        ComponentKind::TailIn {
            paths: vec!["/var/log/app.log".to_string()],
            tail: logit_config::TailOptions {
                checkpoint_path: Some(checkpoint_path.to_string()),
                ..logit_config::TailOptions::default()
            },
        }
    }

    fn docker_in_checkpointing_to(checkpoint_path: &str) -> ComponentKind {
        ComponentKind::DockerIn {
            root: "/var/lib/docker/containers".to_string(),
            containers: vec!["nginx".to_string()],
            discover: false,
            labels: Vec::new(),
            tail: logit_config::TailOptions {
                checkpoint_path: Some(checkpoint_path.to_string()),
                ..logit_config::TailOptions::default()
            },
        }
    }

    /// Rule 62: two tailing listeners writing one checkpoint would overwrite each other's
    /// offsets every interval, and each would resume from whichever wrote last.
    #[test]
    fn two_tailing_listeners_sharing_a_checkpoint_path_are_rejected() {
        let err = expect_err(cfg(vec![
            ("logs", vec![], tail_in_checkpointing_to("state/tail.json")),
            ("containers", vec![], docker_in_checkpointing_to("state/tail.json")),
            ("out", vec!["logs", "containers"], sink()),
        ]));
        assert!(err.contains("'containers'") && err.contains("'logs'"), "got: {err}");
        assert!(err.contains("both set 'checkpoint_path' to 'state/tail.json'"), "got: {err}");

        let err = expect_err(cfg(vec![
            ("a", vec![], tail_in_checkpointing_to("tail.json")),
            ("b", vec![], tail_in_checkpointing_to("tail.json")),
            ("out", vec!["a", "b"], sink()),
        ]));
        assert!(err.contains("both set 'checkpoint_path' to 'tail.json'"), "got: {err}");
    }

    /// Rule 62: `a.json`'s every write creates (truncating) `a.json.tmp` and renames it away, so a
    /// second listener checkpointing to `a.json.tmp` would find its checkpoint gone at restart,
    /// load it as missing, and skip to `read_from`.
    #[test]
    fn a_checkpoint_path_equal_to_another_components_tmp_path_is_rejected() {
        for (first, second) in [("a", "b"), ("b", "a")] {
            let err = expect_err(cfg(vec![
                (first, vec![], tail_in_checkpointing_to("state/a.json")),
                (second, vec![], docker_in_checkpointing_to("state/a.json.tmp")),
                ("out", vec!["a", "b"], sink()),
            ]));
            assert!(
                err.contains(&format!("component '{second}'"))
                    && err.contains(&format!("component '{first}'")),
                "got: {err}"
            );
            assert!(
                err.contains("'state/a.json.tmp'") && err.contains("'state/a.json'"),
                "got: {err}"
            );
            assert!(err.contains("tmp file"), "got: {err}");
        }
    }

    /// Rule 62 compares literal paths only; distinct ones, and listeners with no checkpoint, pass.
    #[test]
    fn tailing_listeners_with_distinct_or_no_checkpoint_paths_validate_fine() {
        resolve(cfg(vec![
            ("a", vec![], tail_in_checkpointing_to("state/a.json")),
            ("b", vec![], tail_in_checkpointing_to("state/a.yaml")),
            ("c", vec![], tail_in(vec!["/var/log/c.log"])),
            ("d", vec![], tail_in(vec!["/var/log/d.log"])),
            ("out", vec!["a", "b", "c", "d"], sink()),
        ]))
        .expect("distinct checkpoint paths, or none, should validate fine");
    }

    #[test]
    fn a_disk_buffer_at_every_default_but_path_validates_fine() {
        resolve(cfg_with_buffer(vec![
            ("in", vec![], listener(), BufferConfig::default()),
            ("out", vec!["in"], sink(), disk_buffer("spool")),
        ]))
        .expect("a disk buffer with only path set should validate fine");
    }

    #[test]
    fn kind_name_and_role_are_implemented_for_file_out() {
        let kind = file_out(logit_config::RotateConfig {
            max_bytes: Some(1024),
            interval: None,
            max_files: 5,
        });
        assert_eq!(kind_name(&kind), "file_out");
        assert_eq!(role(&kind), Role::Sink);
    }

    #[test]
    fn kind_name_and_role_are_implemented_for_tail_in_and_docker_in() {
        assert_eq!(kind_name(&tail_in(vec!["/x"])), "tail_in");
        assert_eq!(role(&tail_in(vec!["/x"])), Role::Listener);
        assert_eq!(kind_name(&docker_in(vec!["x"], false)), "docker_in");
        assert_eq!(role(&docker_in(vec!["x"], false)), Role::Listener);
    }

    #[test]
    fn statsd_out_is_a_sink_and_is_implemented() {
        let kind = statsd_out(1432);
        assert_eq!(kind_name(&kind), "statsd_out");
        assert_eq!(role(&kind), Role::Sink);
        resolve(cfg(vec![("in", vec![], listener()), ("out", vec!["in"], statsd_out(1432))]))
            .expect("a well-formed statsd_out should resolve fine");
    }

    #[test]
    fn a_statsd_out_with_no_sources_is_rejected() {
        let err = expect_err(cfg(vec![("out", vec![], statsd_out(1432))]));
        assert!(err.contains("'out'") && err.contains("sink"), "got: {err}");
    }

    /// Rule 38: `max_packet_bytes: 0` would drop every metric line.
    #[test]
    fn a_zero_max_packet_bytes_is_rejected() {
        let err =
            expect_err(cfg(vec![("in", vec![], listener()), ("out", vec!["in"], statsd_out(0))]));
        assert!(err.contains("'out'") && err.contains("max_packet_bytes: 0"), "got: {err}");
    }

    #[test]
    fn collectd_out_is_a_sink_and_is_implemented() {
        let kind = collectd_out(1452);
        assert_eq!(kind_name(&kind), "collectd_out");
        assert_eq!(role(&kind), Role::Sink);
        resolve(cfg(vec![("in", vec![], listener()), ("out", vec!["in"], collectd_out(1452))]))
            .expect("a well-formed collectd_out should resolve fine");
    }

    #[test]
    fn a_collectd_out_with_no_sources_is_rejected() {
        let err = expect_err(cfg(vec![("out", vec![], collectd_out(1452))]));
        assert!(err.contains("'out'") && err.contains("sink"), "got: {err}");
    }

    /// Rule 38: `collectd_out`'s `max_packet_bytes: 0`.
    #[test]
    fn a_zero_max_packet_bytes_is_rejected_for_collectd_out_too() {
        let err =
            expect_err(cfg(vec![("in", vec![], listener()), ("out", vec!["in"], collectd_out(0))]));
        assert!(err.contains("'out'") && err.contains("max_packet_bytes: 0"), "got: {err}");
    }

    /// Rule 38's `collectd_out` range check, below collectd's own `MaxPacketSize` minimum.
    #[test]
    fn a_max_packet_bytes_below_1024_is_rejected_for_collectd_out() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], collectd_out(1023)),
        ]));
        assert!(err.contains("'out'") && err.contains("1024..=65535"), "got: {err}");
    }

    /// Rule 38's `collectd_out` range check, above `u16::MAX`: every send would fail `EMSGSIZE`
    /// while reporting success.
    #[test]
    fn a_max_packet_bytes_above_65535_is_rejected_for_collectd_out() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], collectd_out(65536)),
        ]));
        assert!(err.contains("'out'") && err.contains("1024..=65535"), "got: {err}");
    }

    /// Both ends of `1024..=65535` are legal.
    #[test]
    fn max_packet_bytes_at_either_bound_is_accepted_for_collectd_out() {
        for bound in [1024u64, 65535] {
            resolve(cfg(vec![
                ("in", vec![], listener()),
                ("out", vec!["in"], collectd_out(bound)),
            ]))
            .unwrap_or_else(|err| panic!("bound {bound} should resolve fine, got: {err}"));
        }
    }

    #[test]
    fn graphite_out_is_a_sink_and_is_implemented() {
        let kind = graphite_out(
            GraphiteTransport::Tcp,
            GraphiteProtocol::Plaintext,
            1432,
            1 << 20,
            Duration::from_secs(5),
        );
        assert_eq!(kind_name(&kind), "graphite_out");
        assert_eq!(role(&kind), Role::Sink);
        resolve(cfg(vec![("in", vec![], listener()), ("out", vec!["in"], kind)]))
            .expect("a well-formed graphite_out should resolve fine");
    }

    #[test]
    fn a_graphite_out_with_no_sources_is_rejected() {
        let err = expect_err(cfg(vec![(
            "out",
            vec![],
            graphite_out(
                GraphiteTransport::Tcp,
                GraphiteProtocol::Plaintext,
                1432,
                1 << 20,
                Duration::from_secs(5),
            ),
        )]));
        assert!(err.contains("'out'") && err.contains("sink"), "got: {err}");
    }

    /// Rule 46: pickle over UDP is rejected -- Twisted's length-prefixed framing has no meaning
    /// in a datagram.
    #[test]
    fn a_pickle_graphite_out_on_udp_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                graphite_out(
                    GraphiteTransport::Udp,
                    GraphiteProtocol::Pickle,
                    1432,
                    1 << 20,
                    Duration::from_secs(5),
                ),
            ),
        ]));
        assert!(
            err.contains("'out'") && err.contains("protocol: pickle requires transport: tcp"),
            "got: {err}"
        );
    }

    /// Pickle over TCP is fine -- only the UDP combination is rejected.
    #[test]
    fn a_pickle_graphite_out_on_tcp_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                graphite_out(
                    GraphiteTransport::Tcp,
                    GraphiteProtocol::Pickle,
                    1432,
                    1 << 20,
                    Duration::from_secs(5),
                ),
            ),
        ]))
        .expect("pickle over tcp should resolve fine");
    }

    /// Rule 38: `graphite_out`'s `max_packet_bytes: 0`.
    #[test]
    fn a_zero_max_packet_bytes_graphite_out_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                graphite_out(
                    GraphiteTransport::Tcp,
                    GraphiteProtocol::Plaintext,
                    0,
                    1 << 20,
                    Duration::from_secs(5),
                ),
            ),
        ]));
        assert!(err.contains("'out'") && err.contains("max_packet_bytes: 0"), "got: {err}");
    }

    /// Rule 46: `max_frame_bytes` outside `1024..=16 MiB` is rejected, both below and above.
    #[test]
    fn a_max_frame_bytes_outside_its_range_is_rejected() {
        for bad in [0u64, 1023, 16 * 1024 * 1024 + 1] {
            let err = expect_err(cfg(vec![
                ("in", vec![], listener()),
                (
                    "out",
                    vec!["in"],
                    graphite_out(
                        GraphiteTransport::Tcp,
                        GraphiteProtocol::Plaintext,
                        1432,
                        bad,
                        Duration::from_secs(5),
                    ),
                ),
            ]));
            assert!(err.contains("'out'") && err.contains("max_frame_bytes"), "got: {err}");
        }
    }

    /// Both ends of `1024..=16 MiB` are legal.
    #[test]
    fn max_frame_bytes_at_either_bound_is_accepted_for_graphite_out() {
        for bound in [1024u64, 16 * 1024 * 1024] {
            resolve(cfg(vec![
                ("in", vec![], listener()),
                (
                    "out",
                    vec!["in"],
                    graphite_out(
                        GraphiteTransport::Tcp,
                        GraphiteProtocol::Plaintext,
                        1432,
                        bound,
                        Duration::from_secs(5),
                    ),
                ),
            ]))
            .unwrap_or_else(|err| panic!("bound {bound} should resolve fine, got: {err}"));
        }
    }

    /// Rule 46: a zero `connect_timeout` could never establish a TCP connection.
    #[test]
    fn a_zero_connect_timeout_graphite_out_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "out",
                vec!["in"],
                graphite_out(
                    GraphiteTransport::Tcp,
                    GraphiteProtocol::Plaintext,
                    1432,
                    1 << 20,
                    Duration::ZERO,
                ),
            ),
        ]));
        assert!(err.contains("'out'") && err.contains("connect_timeout: 0s"), "got: {err}");
    }

    /// An `aggregate` with the given temporality and retention bounds -- rule 39's fixture.
    fn aggregate(
        temporality: logit_config::AggregateTemporality,
        series_retention: u32,
        max_retained_series: usize,
    ) -> ComponentKind {
        ComponentKind::Aggregate {
            interval: Duration::from_secs(10),
            temporality,
            series_retention,
            max_retained_series,
            distributions: logit_config::Distributions::default(),
            max_samples_per_series: 1000,
            sets: logit_config::Sets::default(),
            max_set_members_per_series: 1000,
        }
    }

    /// Rule 39: cumulative with no retention would label each window's own increment a running
    /// total.
    #[test]
    fn a_cumulative_aggregate_with_zero_series_retention_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            (
                "agg",
                vec!["in"],
                aggregate(logit_config::AggregateTemporality::Cumulative, 0, 10_000),
            ),
            ("out", vec!["agg"], sink()),
        ]));
        assert!(err.contains("'agg'") && err.contains("temporality: cumulative"), "got: {err}");
    }

    /// The cap half of rule 39: a zero `max_retained_series` evicts every survivor immediately,
    /// which is the same failure as never retaining at all.
    #[test]
    fn a_cumulative_aggregate_with_a_zero_retained_series_cap_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("agg", vec!["in"], aggregate(logit_config::AggregateTemporality::Cumulative, 5, 0)),
            ("out", vec!["agg"], sink()),
        ]));
        assert!(err.contains("max_retained_series"), "got: {err}");
    }

    /// Rule 39 is scoped to `cumulative`: in `delta` mode `series_retention: 0` is the documented
    /// opt-out to strictly tumbling windows.
    #[test]
    fn a_delta_aggregate_with_zero_series_retention_is_accepted() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("agg", vec!["in"], aggregate(logit_config::AggregateTemporality::Delta, 0, 0)),
            ("out", vec!["agg"], sink()),
        ]))
        .expect("delta mode without retention is the pre-existing default behavior");
    }

    /// A well-formed cumulative `aggregate` resolves.
    #[test]
    fn a_cumulative_aggregate_with_both_bounds_set_resolves() {
        resolve(cfg(vec![
            ("in", vec![], listener()),
            (
                "agg",
                vec!["in"],
                aggregate(logit_config::AggregateTemporality::Cumulative, 5, 10_000),
            ),
            ("out", vec!["agg"], sink()),
        ]))
        .expect("cumulative with both retention bounds set should resolve");
    }

    // ---- rule 40: prometheus_in --------------------------------------------------------------

    #[test]
    fn prometheus_in_is_a_listener_and_is_implemented() {
        let kind = prometheus_in(vec!["http://node-exporter:9100/metrics"]);
        assert_eq!(kind_name(&kind), "prometheus_in");
        assert_eq!(role(&kind), Role::Listener);
        resolve(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]))
            .expect("a well-formed prometheus_in should resolve fine");
    }

    /// With no targets and no `bind:`, rule 55's neither-mode check rejects the config, naming the
    /// missing field.
    #[test]
    fn a_prometheus_in_with_neither_scrape_targets_nor_bind_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], prometheus_in(vec![])),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'in'") && err.contains("'scrape_targets'"), "got: {err}");
        assert!(err.contains("'bind'"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_target_missing_a_scheme_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], prometheus_in(vec!["node-exporter:9100/metrics"])),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("absolute"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_target_with_an_empty_authority_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], prometheus_in(vec!["http:///metrics"])),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("absolute"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_with_an_https_target_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], prometheus_in(vec!["https://node-exporter:9100/metrics"])),
            ("out", vec!["in"], sink()),
        ]))
        .expect("an https:// target should resolve fine");
    }

    #[test]
    fn a_prometheus_in_with_zero_interval_is_rejected() {
        let mut kind = prometheus_in(vec!["http://node-exporter:9100/metrics"]);
        if let ComponentKind::PrometheusIn { interval, .. } = &mut kind {
            *interval = Duration::ZERO;
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("interval"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_with_zero_timeout_is_rejected() {
        let mut kind = prometheus_in(vec!["http://node-exporter:9100/metrics"]);
        if let ComponentKind::PrometheusIn { timeout, .. } = &mut kind {
            *timeout = Duration::ZERO;
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("'timeout: 0s'"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_tls_block_under_an_all_http_target_list_is_rejected() {
        let tls =
            logit_config::TlsClientConfig { insecure_skip_verify: true, ..Default::default() };
        let err = expect_err(cfg(vec![
            ("in", vec![], prometheus_in_with_tls(vec!["http://node-exporter:9100/metrics"], tls)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("no 'scrape_targets' entry is 'https://'"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_tls_block_with_one_https_target_resolves_fine() {
        let tls =
            logit_config::TlsClientConfig { insecure_skip_verify: true, ..Default::default() };
        resolve(cfg(vec![
            (
                "in",
                vec![],
                prometheus_in_with_tls(
                    vec!["http://a:9100/metrics", "https://b:9100/metrics"],
                    tls,
                ),
            ),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a tls: block with at least one https:// target should resolve fine");
    }

    #[test]
    fn a_prometheus_in_tls_cert_file_without_a_key_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            cert_file: Some("client.pem".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], prometheus_in_with_tls(vec!["https://node-exporter:9100/metrics"], tls)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("cert_file") && err.contains("key_file"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_tls_key_file_without_a_cert_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            key_file: Some("client.key".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], prometheus_in_with_tls(vec!["https://node-exporter:9100/metrics"], tls)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("cert_file") && err.contains("key_file"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_tls_insecure_skip_verify_with_a_ca_file_is_rejected() {
        let tls = logit_config::TlsClientConfig {
            insecure_skip_verify: true,
            ca_file: Some("ca.pem".to_string()),
            ..Default::default()
        };
        let err = expect_err(cfg(vec![
            ("in", vec![], prometheus_in_with_tls(vec!["https://node-exporter:9100/metrics"], tls)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("insecure_skip_verify") && err.contains("ca_file"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_tls_cert_and_key_file_together_resolve_fine() {
        let tls = logit_config::TlsClientConfig {
            cert_file: Some("client.pem".to_string()),
            key_file: Some("client.key".to_string()),
            ..Default::default()
        };
        resolve(cfg(vec![
            ("in", vec![], prometheus_in_with_tls(vec!["https://node-exporter:9100/metrics"], tls)),
            ("out", vec!["in"], sink()),
        ]))
        .expect("cert_file and key_file set together should resolve fine");
    }

    #[test]
    fn a_prometheus_in_header_this_input_sets_itself_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                prometheus_in_with_headers(
                    vec!["http://node-exporter:9100/metrics"],
                    vec![("Accept", "text/plain")],
                ),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("sets itself"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_with_two_headers_differing_only_in_case_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                prometheus_in_with_headers(
                    vec!["http://node-exporter:9100/metrics"],
                    vec![("X-Scope-OrgID", "a"), ("x-scope-orgid", "b")],
                ),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("differs only in case"), "got: {err}");
    }

    #[test]
    fn a_prometheus_in_with_a_custom_header_resolves_fine() {
        resolve(cfg(vec![
            (
                "in",
                vec![],
                prometheus_in_with_headers(
                    vec!["http://node-exporter:9100/metrics"],
                    vec![("X-Scope-OrgID", "tenant-a")],
                ),
            ),
            ("out", vec!["in"], sink()),
        ]))
        .expect("should resolve");
    }

    /// Rule 17 rejects `receive:` on `prometheus_in`: a listener by role, but on none of the three
    /// drivers.
    #[test]
    fn a_non_default_receive_on_prometheus_in_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                prometheus_in(vec!["http://node-exporter:9100/metrics"]),
                non_default_receive(),
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(
            err.contains("'receive' is only meaningful on a datagram, stream or tail listener"),
            "got: {err}"
        );
    }

    // ---- rule 55: prometheus_in's two modes (docs/adr/prometheus-remote-write.md) --------------

    #[test]
    fn a_bind_mode_prometheus_in_resolves_fine() {
        let kind = prometheus_in_bind("0.0.0.0:9090");
        assert_eq!(kind_name(&kind), "prometheus_in");
        assert_eq!(role(&kind), Role::Listener);
        resolve(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]))
            .expect("a bind-mode prometheus_in should resolve fine");
    }

    /// A bind-mode `prometheus_in` resolves: rule 40's scrape checks are gated on scrape mode.
    #[test]
    fn rule_40_does_not_fire_on_a_bind_mode_prometheus_in() {
        let mut kind = prometheus_in_bind("0.0.0.0:9090");
        // Rule 40's `timeout: 0s` and `scrape_tls`-without-https checks are scrape statements;
        // neither may fire here. The defaults are what rule 55 requires in bind mode anyway, so
        // this asserts the gate, not a carve-out.
        if let ComponentKind::PrometheusIn { path, .. } = &mut kind {
            *path = "/write".to_string();
        }
        resolve(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]))
            .expect("rule 40 must not run over a bind-mode prometheus_in");
    }

    #[test]
    fn a_prometheus_in_with_both_scrape_targets_and_bind_is_rejected() {
        let mut kind = prometheus_in(vec!["http://node-exporter:9100/metrics"]);
        if let ComponentKind::PrometheusIn { bind, .. } = &mut kind {
            *bind = Some("0.0.0.0:9090".to_string());
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("exactly one of them"), "got: {err}");
    }

    /// Rule 55: a bind-mode `path` must start with `/` (rule 41's check), or every write would 404.
    #[test]
    fn a_bind_mode_prometheus_in_with_a_relative_path_is_rejected() {
        let mut kind = prometheus_in_bind("0.0.0.0:9090");
        if let ComponentKind::PrometheusIn { path, .. } = &mut kind {
            *path = "api/v1/write".to_string();
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("must start with '/'"), "got: {err}");
    }

    #[test]
    fn a_bind_mode_prometheus_in_with_an_empty_path_is_rejected() {
        let mut kind = prometheus_in_bind("0.0.0.0:9090");
        if let ComponentKind::PrometheusIn { path, .. } = &mut kind {
            *path = String::new();
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("must start with '/'"), "got: {err}");
    }

    #[test]
    fn a_scrape_only_field_alongside_bind_is_rejected() {
        for (field, mutate) in scrape_only_mutations() {
            let mut kind = prometheus_in_bind("0.0.0.0:9090");
            mutate(&mut kind);
            let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
            assert!(err.contains(&format!("'{field}'")), "for {field}, got: {err}");
            assert!(err.contains("a receiver performs no scrape"), "for {field}, got: {err}");
        }
    }

    #[test]
    fn a_bind_only_field_alongside_scrape_targets_is_rejected() {
        for (field, mutate) in bind_only_mutations() {
            let mut kind = prometheus_in(vec!["http://node-exporter:9100/metrics"]);
            mutate(&mut kind);
            let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
            assert!(err.contains(&format!("'{field}'")), "for {field}, got: {err}");
            assert!(err.contains("a scrape client binds nothing"), "for {field}, got: {err}");
        }
    }

    /// `interval` keeps its default in bind mode, so rule 9 needs no carve-out and rule 55 sees a
    /// defaulted value.
    #[test]
    fn bind_mode_leaves_interval_at_its_default_and_rule_9_stays_satisfied() {
        let mut kind = prometheus_in_bind("0.0.0.0:9090");
        if let ComponentKind::PrometheusIn { interval, .. } = &mut kind {
            *interval = Duration::ZERO;
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        // Rule 9's generic flush-interval check, reached before rule 55's wrong-mode one -- which
        // is the point: nothing about bind mode is carved out of it.
        assert!(err.contains("a flush interval of 0s"), "got: {err}");
    }

    /// Rule 53 reaches a bind-mode `prometheus_in`. In scrape mode rule 55 rejects the field
    /// instead (`a_bind_only_field_alongside_scrape_targets_is_rejected`).
    #[test]
    fn a_zero_idle_timeout_on_a_bind_mode_prometheus_in_is_rejected() {
        let mut kind = prometheus_in_bind("0.0.0.0:9090");
        if let ComponentKind::PrometheusIn { idle_timeout, .. } = &mut kind {
            *idle_timeout = Some(Duration::ZERO);
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("'idle_timeout' must be greater than 0s"), "got: {err}");
    }

    #[test]
    fn a_positive_idle_timeout_on_a_bind_mode_prometheus_in_resolves_fine() {
        let mut kind = prometheus_in_bind("0.0.0.0:9090");
        if let ComponentKind::PrometheusIn { idle_timeout, .. } = &mut kind {
            *idle_timeout = Some(Duration::from_secs(60));
        }
        resolve(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]))
            .expect("a bind-mode prometheus_in may carry an idle_timeout");
    }

    /// One non-default value per scrape-only field, named as rule 55's message spells it.
    #[allow(clippy::type_complexity)]
    fn scrape_only_mutations() -> Vec<(&'static str, Box<dyn Fn(&mut ComponentKind)>)> {
        vec![
            (
                "interval",
                Box::new(|kind: &mut ComponentKind| {
                    if let ComponentKind::PrometheusIn { interval, .. } = kind {
                        *interval = Duration::from_secs(30);
                    }
                }) as Box<dyn Fn(&mut ComponentKind)>,
            ),
            (
                "timeout",
                Box::new(|kind: &mut ComponentKind| {
                    if let ComponentKind::PrometheusIn { timeout, .. } = kind {
                        *timeout = Duration::from_secs(30);
                    }
                }),
            ),
            (
                "headers",
                Box::new(|kind: &mut ComponentKind| {
                    if let ComponentKind::PrometheusIn { headers, .. } = kind {
                        headers.insert("X-Scope-OrgID".to_string(), "tenant-a".to_string());
                    }
                }),
            ),
            (
                "scrape_tls",
                Box::new(|kind: &mut ComponentKind| {
                    if let ComponentKind::PrometheusIn { scrape_tls, .. } = kind {
                        scrape_tls.ca_file = Some("ca.pem".to_string());
                    }
                }),
            ),
        ]
    }

    /// The same, one field over.
    #[allow(clippy::type_complexity)]
    fn bind_only_mutations() -> Vec<(&'static str, Box<dyn Fn(&mut ComponentKind)>)> {
        vec![
            (
                "path",
                Box::new(|kind: &mut ComponentKind| {
                    if let ComponentKind::PrometheusIn { path, .. } = kind {
                        *path = "/write".to_string();
                    }
                }) as Box<dyn Fn(&mut ComponentKind)>,
            ),
            (
                "bind_tls",
                Box::new(|kind: &mut ComponentKind| {
                    if let ComponentKind::PrometheusIn { bind_tls, .. } = kind {
                        *bind_tls = Some(logit_config::TlsServerConfig {
                            cert_file: "server.pem".to_string(),
                            key_file: "server.key".to_string(),
                            client_ca_file: None,
                        });
                    }
                }),
            ),
            (
                "idle_timeout",
                Box::new(|kind: &mut ComponentKind| {
                    if let ComponentKind::PrometheusIn { idle_timeout, .. } = kind {
                        *idle_timeout = Some(Duration::from_secs(60));
                    }
                }),
            ),
            (
                "metadata_cache",
                Box::new(|kind: &mut ComponentKind| {
                    if let ComponentKind::PrometheusIn { metadata_cache, .. } = kind {
                        metadata_cache.max_families = 250;
                    }
                }),
            ),
        ]
    }

    /// Rule 55: a non-default `metadata_cache` in scrape mode is rejected (a scrape client reads `#
    /// TYPE` in every response); the default resolves.
    #[test]
    fn a_default_metadata_cache_alongside_scrape_targets_resolves() {
        resolve(cfg(vec![
            ("in", vec![], prometheus_in(vec!["http://localhost:9100/metrics"])),
            ("out", vec!["in"], sink()),
        ]))
        .expect("an untouched metadata_cache is not a wrong-mode value");
    }

    /// Rule 55: `metadata_cache.ttl: 0s` is rejected, except with `max_families: 0`, which turns
    /// the cache off; the message names that spelling.
    #[test]
    fn a_zero_metadata_cache_ttl_on_a_bind_mode_prometheus_in_is_rejected() {
        let mut kind = prometheus_in_bind("0.0.0.0:9090");
        if let ComponentKind::PrometheusIn { metadata_cache, .. } = &mut kind {
            metadata_cache.ttl = Duration::ZERO;
        }
        let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
        assert!(err.contains("'metadata_cache.ttl' is 0s"), "got: {err}");
        assert!(err.contains("max_families: 0"), "the message names the off switch: {err}");
    }

    #[test]
    fn a_disabled_metadata_cache_on_a_bind_mode_prometheus_in_resolves_fine() {
        for ttl in [Duration::from_secs(600), Duration::ZERO] {
            let mut kind = prometheus_in_bind("0.0.0.0:9090");
            if let ComponentKind::PrometheusIn { metadata_cache, .. } = &mut kind {
                metadata_cache.max_families = 0;
                // With no cache there is nothing for a `ttl` to govern, so even `0s` -- which the
                // check above rejects on its own -- is not a setting that could do nothing.
                metadata_cache.ttl = ttl;
            }
            resolve(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]))
                .expect("'max_families: 0' is how an operator turns the cache off");
        }
    }

    // ---- collectd_in (docs/adr/collectd-binary-relay.md) --------------------------------------

    fn collectd_in(types_db: Vec<&str>) -> ComponentKind {
        ComponentKind::CollectdIn {
            bind: "0.0.0.0:25826".to_string(),
            types_db: types_db.into_iter().map(std::path::PathBuf::from).collect(),
        }
    }

    #[test]
    fn a_collectd_in_resolves_as_an_implemented_listener() {
        let graph =
            resolve(cfg(vec![("in", vec![], collectd_in(vec![])), ("out", vec!["in"], sink())]))
                .expect("a collectd_in should resolve");
        assert_eq!(graph.components["in"].role(), Role::Listener);
        assert_eq!(graph.components["in"].kind_name(), "collectd_in");
    }

    #[test]
    fn a_collectd_in_with_types_db_paths_resolves() {
        resolve(cfg(vec![
            ("in", vec![], collectd_in(vec!["/usr/share/collectd/types.db", "local.db"])),
            ("out", vec!["in"], sink()),
        ]))
        .expect("types_db paths are resolved at build time, not here");
    }

    /// `collectd_in` runs on the shared UDP listener driver, so rule 17 must let a `receive:`
    /// block through -- the property `is_datagram_listener` exists to carry.
    #[test]
    fn a_non_default_receive_on_collectd_in_is_allowed() {
        let graph = resolve(cfg_with_receive(vec![
            ("in", vec![], collectd_in(vec![]), non_default_receive()),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("collectd_in is a datagram listener, so receive: applies to it");
        assert_eq!(graph.components["in"].receive.max_datagrams, 4096);
    }

    /// Rule 18's zero-bound check reaches `collectd_in` through the same predicate.
    #[test]
    fn a_zero_receive_bound_on_collectd_in_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                collectd_in(vec![]),
                ReceiveConfig { max_datagrams: 0, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'receive.max_datagrams' must be at least 1"), "got: {err}");
    }

    // ---- graphite_in + rule 46 (docs/adr/graphite-carbon-relay.md) ----------------------------

    fn graphite_in(transport: GraphiteTransport, protocol: GraphiteProtocol) -> ComponentKind {
        graphite_in_sized(transport, protocol, 8192, 1 << 20)
    }

    /// The same with both size bounds spelled out -- an enum variant has no functional-record-
    /// update syntax, so rule 46's bound tests take this rather than `..graphite_in(..)`.
    fn graphite_in_sized(
        transport: GraphiteTransport,
        protocol: GraphiteProtocol,
        max_line_bytes: u64,
        max_frame_bytes: u64,
    ) -> ComponentKind {
        graphite_in_full(
            transport,
            protocol,
            max_line_bytes,
            max_frame_bytes,
            false,
            default_handshake_timeout(),
        )
    }

    /// [`graphite_in`] with rules 43's and 45's knobs, `tls:` and `handshake_timeout`, exposed.
    fn graphite_in_full(
        transport: GraphiteTransport,
        protocol: GraphiteProtocol,
        max_line_bytes: u64,
        max_frame_bytes: u64,
        tls: bool,
        handshake_timeout: Duration,
    ) -> ComponentKind {
        ComponentKind::GraphiteIn {
            bind: "0.0.0.0:2003".to_string(),
            transport,
            protocol,
            tls: tls.then(|| logit_config::TlsServerConfig {
                cert_file: "server.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
            handshake_timeout,
            idle_timeout: None,
            max_line_bytes,
            max_frame_bytes,
        }
    }

    /// [`graphite_in`] with rule 53's knob exposed -- the `Option` that rule reads.
    fn graphite_in_with_idle_timeout(
        transport: GraphiteTransport,
        idle_timeout: Option<Duration>,
    ) -> ComponentKind {
        ComponentKind::GraphiteIn {
            bind: "0.0.0.0:2003".to_string(),
            transport,
            protocol: GraphiteProtocol::Plaintext,
            tls: None,
            handshake_timeout: default_handshake_timeout(),
            idle_timeout,
            max_line_bytes: 8192,
            max_frame_bytes: 1 << 20,
        }
    }

    /// Rule 43 over `graphite_in`: the same message a `syslog_in` gets.
    #[test]
    fn tls_on_a_udp_graphite_in_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "in",
                vec![],
                graphite_in_full(
                    GraphiteTransport::Udp,
                    GraphiteProtocol::Plaintext,
                    8192,
                    1 << 20,
                    true,
                    default_handshake_timeout(),
                ),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'tls:' needs 'transport: tcp'"), "got: {err}");
        assert!(err.contains("DTLS"), "got: {err}");
    }

    /// Rule 43's other side for carbon: a TLS-terminating TCP `graphite_in` resolves.
    #[test]
    fn tls_on_a_tcp_graphite_in_validates_fine() {
        resolve(cfg(vec![
            (
                "in",
                vec![],
                graphite_in_full(
                    GraphiteTransport::Tcp,
                    GraphiteProtocol::Plaintext,
                    8192,
                    1 << 20,
                    true,
                    default_handshake_timeout(),
                ),
            ),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a TLS-terminating TCP graphite_in is legal");
    }

    /// Rule 45 over `graphite_in`: `0s` is impossible on either transport, and a non-default value
    /// is set-but-ignored under `transport: udp`. The TCP case with a real value validates.
    #[test]
    fn rule_45_covers_a_graphite_in_handshake_timeout() {
        let zero = expect_err(cfg(vec![
            (
                "in",
                vec![],
                graphite_in_full(
                    GraphiteTransport::Tcp,
                    GraphiteProtocol::Plaintext,
                    8192,
                    1 << 20,
                    false,
                    Duration::ZERO,
                ),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(zero.contains("'handshake_timeout' must be greater than 0s"), "got: {zero}");

        let on_udp = expect_err(cfg(vec![
            (
                "in",
                vec![],
                graphite_in_full(
                    GraphiteTransport::Udp,
                    GraphiteProtocol::Plaintext,
                    8192,
                    1 << 20,
                    false,
                    Duration::from_secs(2),
                ),
            ),
            ("out", vec!["in"], sink()),
        ]));
        assert!(on_udp.contains("needs 'transport: tcp'"), "got: {on_udp}");
        assert!(on_udp.contains("UDP graphite_in"), "got: {on_udp}");

        resolve(cfg(vec![
            (
                "in",
                vec![],
                graphite_in_full(
                    GraphiteTransport::Tcp,
                    GraphiteProtocol::Plaintext,
                    8192,
                    1 << 20,
                    false,
                    Duration::from_secs(2),
                ),
            ),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a non-default handshake_timeout on a TCP graphite_in is what the field is for");
    }

    /// A UDP `graphite_in` at the default `handshake_timeout` resolves (rule 45's defaulted-value
    /// early return).
    #[test]
    fn a_defaulted_handshake_timeout_on_a_udp_graphite_in_is_fine() {
        resolve(cfg(vec![
            ("in", vec![], graphite_in(GraphiteTransport::Udp, GraphiteProtocol::Plaintext)),
            ("out", vec!["in"], sink()),
        ]))
        .expect("an untouched handshake_timeout must not make transport: udp a config error");
    }

    #[test]
    fn a_graphite_in_resolves_as_an_implemented_listener() {
        let kind = graphite_in(GraphiteTransport::Tcp, GraphiteProtocol::Plaintext);
        assert_eq!(kind_name(&kind), "graphite_in");
        assert_eq!(role(&kind), Role::Listener);
        let graph = resolve(cfg(vec![
            ("in", vec![], graphite_in(GraphiteTransport::Tcp, GraphiteProtocol::Plaintext)),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a graphite_in should resolve");
        assert_eq!(graph.components["in"].role(), Role::Listener);
        assert_eq!(graph.components["in"].kind_name(), "graphite_in");
    }

    /// Rule 6's arity table: a listener has no `sources` of its own, whichever transport it runs.
    #[test]
    fn a_graphite_in_with_sources_is_rejected() {
        let err = expect_err(cfg(vec![
            ("first", vec![], graphite_in(GraphiteTransport::Udp, GraphiteProtocol::Plaintext)),
            ("in", vec!["first"], graphite_in(GraphiteTransport::Tcp, GraphiteProtocol::Plaintext)),
            ("out", vec!["in", "first"], sink()),
        ]));
        assert!(err.contains("'in'") && err.contains("listener"), "got: {err}");
    }

    /// Rule 46's headline check: carbon's pickle wire is a length-prefixed stream framing, which a
    /// self-delimiting datagram has no use for.
    #[test]
    fn a_graphite_in_with_pickle_over_udp_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], graphite_in(GraphiteTransport::Udp, GraphiteProtocol::Pickle)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("protocol: pickle requires transport: tcp"), "got: {err}");
    }

    #[test]
    fn a_graphite_in_with_pickle_over_tcp_resolves() {
        resolve(cfg(vec![
            ("in", vec![], graphite_in(GraphiteTransport::Tcp, GraphiteProtocol::Pickle)),
            ("out", vec!["in"], sink()),
        ]))
        .expect("pickle over TCP is carbon's own port-2004 listener");
    }

    /// Rule 46's impossible bounds, the shape rules 9/15/18/38 share: `0` means *nothing can ever
    /// get through*, which is a config error rather than a small setting.
    #[test]
    fn a_graphite_in_with_a_zero_size_bound_is_rejected() {
        for (field, kind) in [
            (
                "max_line_bytes",
                graphite_in_sized(GraphiteTransport::Tcp, GraphiteProtocol::Plaintext, 0, 1 << 20),
            ),
            (
                "max_frame_bytes",
                graphite_in_sized(GraphiteTransport::Tcp, GraphiteProtocol::Pickle, 8192, 0),
            ),
        ] {
            let err = expect_err(cfg(vec![("in", vec![], kind), ("out", vec!["in"], sink())]));
            assert!(err.contains(&format!("{field}: 0")), "got: {err}");
        }
    }

    /// Rule 46's range: both out-of-range sides are rejected, and the default resolves.
    #[test]
    fn a_graphite_in_max_frame_bytes_is_bounded() {
        for out_of_range in [1023u64, 16 * 1024 * 1024 + 1] {
            let err = expect_err(cfg(vec![
                (
                    "in",
                    vec![],
                    graphite_in_sized(
                        GraphiteTransport::Tcp,
                        GraphiteProtocol::Pickle,
                        8192,
                        out_of_range,
                    ),
                ),
                ("out", vec!["in"], sink()),
            ]));
            assert!(err.contains("outside 1024..=16777216"), "got: {err}");
        }
        for in_range in [1024u64, 1 << 20, 16 * 1024 * 1024] {
            resolve(cfg(vec![
                (
                    "in",
                    vec![],
                    graphite_in_sized(
                        GraphiteTransport::Tcp,
                        GraphiteProtocol::Pickle,
                        8192,
                        in_range,
                    ),
                ),
                ("out", vec!["in"], sink()),
            ]))
            .unwrap_or_else(|e| panic!("{in_range} is inside the range: {e}"));
        }
    }

    /// A UDP `graphite_in` is a datagram listener, so the whole `receive:` block applies to it.
    #[test]
    fn a_non_default_receive_on_a_udp_graphite_in_is_allowed() {
        let graph = resolve(cfg_with_receive(vec![
            (
                "in",
                vec![],
                graphite_in(GraphiteTransport::Udp, GraphiteProtocol::Plaintext),
                non_default_receive(),
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("a udp graphite_in is a datagram listener, so receive: applies to it");
        assert_eq!(graph.components["in"].receive.max_datagrams, 4096);
    }

    /// Rule 17: a TCP `graphite_in` has no receive queue, so a queue field is rejected by name.
    #[test]
    fn a_queue_bounding_receive_field_on_a_tcp_graphite_in_is_rejected_by_name() {
        for (field, receive) in [
            ("max_datagrams", ReceiveConfig { max_datagrams: 4096, ..ReceiveConfig::default() }),
            ("max_bytes", ReceiveConfig { max_bytes: 1024, ..ReceiveConfig::default() }),
            (
                "overflow",
                ReceiveConfig {
                    overflow: logit_config::OverflowPolicy::Block,
                    ..ReceiveConfig::default()
                },
            ),
            (
                "receive_buffer_bytes",
                ReceiveConfig { receive_buffer_bytes: Some(1 << 20), ..ReceiveConfig::default() },
            ),
        ] {
            let err = expect_err(cfg_with_receive(vec![
                (
                    "in",
                    vec![],
                    graphite_in(GraphiteTransport::Tcp, GraphiteProtocol::Plaintext),
                    receive,
                ),
                ("out", vec!["in"], sink(), ReceiveConfig::default()),
            ]));
            assert!(err.contains(&format!("'receive.{field}'")), "got: {err}");
            assert!(err.contains("a stream listener has no receive queue"), "got: {err}");
        }
    }

    /// Rule 17: batch assembly and `shutdown_grace` apply on a TCP listener, per connection.
    #[test]
    fn a_batch_assembly_receive_field_on_a_tcp_graphite_in_is_allowed() {
        let graph = resolve(cfg_with_receive(vec![
            (
                "in",
                vec![],
                graphite_in(GraphiteTransport::Tcp, GraphiteProtocol::Plaintext),
                ReceiveConfig {
                    batch_max_events: 42,
                    shutdown_grace: Duration::from_secs(9),
                    ..ReceiveConfig::default()
                },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("a tcp graphite_in assembles batches, so those receive: fields apply");
        assert_eq!(graph.components["in"].receive.batch_max_events, 42);
        assert_eq!(graph.components["in"].receive.shutdown_grace, Duration::from_secs(9));
    }

    /// Rule 18's zero-bound check reaches a TCP `graphite_in` through `is_stream_listener`, and a
    /// UDP one through `is_datagram_listener`.
    #[test]
    fn a_zero_batch_bound_on_a_graphite_in_is_rejected_on_both_transports() {
        for transport in [GraphiteTransport::Tcp, GraphiteTransport::Udp] {
            let err = expect_err(cfg_with_receive(vec![
                (
                    "in",
                    vec![],
                    graphite_in(transport, GraphiteProtocol::Plaintext),
                    ReceiveConfig { batch_max_events: 0, ..ReceiveConfig::default() },
                ),
                ("out", vec!["in"], sink(), ReceiveConfig::default()),
            ]));
            assert!(
                err.contains("'receive.batch_max_events' must be at least 1"),
                "{transport:?}: got: {err}"
            );
        }
    }

    // ---- rule 41: prometheus_out --------------------------------------------------------------

    /// A registry-mode `prometheus_out` -- `bind:` set, every sender-only field defaulted, which
    /// is what rule 56 requires of one.
    fn prometheus_out(path: &str, max_series: usize) -> ComponentKind {
        ComponentKind::PrometheusOut {
            bind: Some("127.0.0.1:9464".to_string()),
            path: path.to_string(),
            expire_after: Duration::from_secs(300),
            max_series,
            endpoint: None,
            version: logit_config::RemoteWriteVersion::default(),
            compression: logit_config::RemoteWriteCompression::default(),
            timeout: logit_config::default_prometheus_endpoint_timeout(),
            headers: HashMap::new(),
            endpoint_tls: logit_config::TlsClientConfig::default(),
        }
    }

    /// A sender-mode `prometheus_out` -- `endpoint:` set, every registry-only field defaulted.
    fn prometheus_remote_write(endpoint: &str) -> ComponentKind {
        ComponentKind::PrometheusOut {
            bind: None,
            path: logit_config::default_prometheus_path(),
            expire_after: logit_config::default_prometheus_expire_after(),
            max_series: logit_config::default_prometheus_max_series(),
            endpoint: Some(endpoint.to_string()),
            version: logit_config::RemoteWriteVersion::default(),
            compression: logit_config::RemoteWriteCompression::default(),
            timeout: logit_config::default_prometheus_endpoint_timeout(),
            headers: HashMap::new(),
            endpoint_tls: logit_config::TlsClientConfig::default(),
        }
    }

    #[test]
    fn prometheus_out_is_a_sink_and_is_implemented() {
        let kind = prometheus_out("/metrics", 100_000);
        assert_eq!(kind_name(&kind), "prometheus_out");
        assert_eq!(role(&kind), Role::Sink);
        resolve(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], prometheus_out("/metrics", 100_000)),
        ]))
        .expect("a well-formed prometheus_out should resolve fine");
    }

    #[test]
    fn a_prometheus_out_with_no_sources_is_rejected() {
        let err = expect_err(cfg(vec![("out", vec![], prometheus_out("/metrics", 100_000))]));
        assert!(err.contains("'out'") && err.contains("sink"), "got: {err}");
    }

    /// Rule 41: a request URI's path is always absolute, so a relative or empty `path:` could never
    /// be scraped -- every request would 404 against an endpoint that looks configured.
    #[test]
    fn a_prometheus_out_path_that_does_not_start_with_a_slash_is_rejected() {
        for path in ["metrics", ""] {
            let err = expect_err(cfg(vec![
                ("in", vec![], listener()),
                ("out", vec!["in"], prometheus_out(path, 100_000)),
            ]));
            assert!(err.contains("'out'") && err.contains("must start with '/'"), "got: {err}");
        }
    }

    /// Rule 41: `max_series: 0` is rule 38's impossible bound in another shape.
    #[test]
    fn a_zero_prometheus_out_max_series_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("out", vec!["in"], prometheus_out("/metrics", 0)),
        ]));
        assert!(err.contains("'out'") && err.contains("max_series: 0"), "got: {err}");
    }

    // ---- rule 56: prometheus_out's two modes ---------------------------------------------------

    /// The shape every wrong-mode test below uses: one sender-mode `prometheus_out` with one field
    /// mutated, resolved behind a listener.
    fn remote_write_cfg(mutate: impl FnOnce(&mut ComponentKind)) -> Config {
        let mut kind = prometheus_remote_write("http://mimir:8080/api/v1/push");
        mutate(&mut kind);
        cfg(vec![("in", vec![], listener()), ("out", vec!["in"], kind)])
    }

    /// The registry-mode twin of [`remote_write_cfg`].
    fn expose_cfg(mutate: impl FnOnce(&mut ComponentKind)) -> Config {
        let mut kind = prometheus_out("/metrics", 100_000);
        mutate(&mut kind);
        cfg(vec![("in", vec![], listener()), ("out", vec!["in"], kind)])
    }

    #[test]
    fn a_prometheus_out_in_sender_mode_resolves() {
        resolve(remote_write_cfg(|_| {}))
            .expect("an endpoint-mode prometheus_out should resolve fine");
    }

    #[test]
    fn a_prometheus_out_with_both_bind_and_endpoint_is_rejected() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { bind, .. } = kind else { unreachable!() };
            *bind = Some("127.0.0.1:9464".to_string());
        }));
        assert!(err.contains("'out'") && err.contains("can't both be set"), "got: {err}");
    }

    #[test]
    fn a_prometheus_out_with_neither_bind_nor_endpoint_is_rejected() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { endpoint, .. } = kind else { unreachable!() };
            *endpoint = None;
        }));
        assert!(err.contains("'out'") && err.contains("exactly one of"), "got: {err}");
    }

    /// Rule 56's wrong-mode half, sender side: a registry-only field set alongside `endpoint:`
    /// would silently do nothing, so it fails startup naming itself instead (rules 45/53's shape).
    #[test]
    fn registry_only_fields_are_rejected_alongside_an_endpoint() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { path, .. } = kind else { unreachable!() };
            *path = "/exposed".to_string();
        }));
        assert!(err.contains("'out'") && err.contains("'path'"), "got: {err}");

        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { expire_after, .. } = kind else { unreachable!() };
            *expire_after = Duration::from_secs(30);
        }));
        assert!(err.contains("'out'") && err.contains("'expire_after'"), "got: {err}");

        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { max_series, .. } = kind else { unreachable!() };
            *max_series = 25;
        }));
        assert!(err.contains("'out'") && err.contains("'max_series'"), "got: {err}");
    }

    /// The other direction: a sender-only field set alongside `bind:`.
    #[test]
    fn sender_only_fields_are_rejected_alongside_a_bind() {
        let err = expect_err(expose_cfg(|kind| {
            let ComponentKind::PrometheusOut { version, .. } = kind else { unreachable!() };
            *version = logit_config::RemoteWriteVersion::V2;
        }));
        assert!(err.contains("'out'") && err.contains("'version'"), "got: {err}");

        let err = expect_err(expose_cfg(|kind| {
            let ComponentKind::PrometheusOut { compression, .. } = kind else { unreachable!() };
            *compression = logit_config::RemoteWriteCompression::Zstd;
        }));
        assert!(err.contains("'out'") && err.contains("'compression'"), "got: {err}");

        let err = expect_err(expose_cfg(|kind| {
            let ComponentKind::PrometheusOut { timeout, .. } = kind else { unreachable!() };
            *timeout = Duration::from_secs(30);
        }));
        assert!(err.contains("'out'") && err.contains("'timeout'"), "got: {err}");

        let err = expect_err(expose_cfg(|kind| {
            let ComponentKind::PrometheusOut { headers, .. } = kind else { unreachable!() };
            headers.insert("X-Scope-OrgID".to_string(), "tenant-a".to_string());
        }));
        assert!(err.contains("'out'") && err.contains("'headers'"), "got: {err}");

        let err = expect_err(expose_cfg(|kind| {
            let ComponentKind::PrometheusOut { endpoint_tls, .. } = kind else { unreachable!() };
            endpoint_tls.insecure_skip_verify = true;
        }));
        assert!(err.contains("'out'") && err.contains("'endpoint_tls'"), "got: {err}");
    }

    /// A *default* sender-only value stays legal under `bind:` -- rule 45's reason for comparing
    /// against the config crate's own default rather than rejecting the field's presence.
    #[test]
    fn a_defaulted_sender_field_is_fine_under_bind() {
        resolve(expose_cfg(|kind| {
            let ComponentKind::PrometheusOut { timeout, version, .. } = kind else {
                unreachable!()
            };
            *timeout = logit_config::default_prometheus_endpoint_timeout();
            *version = logit_config::RemoteWriteVersion::default();
        }))
        .expect("defaults are legal in either mode");
        resolve(expose_cfg(|kind| {
            let ComponentKind::PrometheusOut { compression, .. } = kind else { unreachable!() };
            *compression = logit_config::RemoteWriteCompression::Snappy;
        }))
        .expect("an explicit default compression is legal under bind");
    }

    /// Remote-write 2.0 mandates Snappy, so `zstd` pairs only with `version: 1`.
    #[test]
    fn zstd_compression_with_version_2_is_rejected() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { version, compression, .. } = kind else {
                unreachable!()
            };
            *version = logit_config::RemoteWriteVersion::V2;
            *compression = logit_config::RemoteWriteCompression::Zstd;
        }));
        assert!(
            err.contains("'out'") && err.contains("'compression: zstd' needs 'version: 1'"),
            "got: {err}"
        );
    }

    /// `zstd` with `version: 1` and `snappy` with `version: 2` are both legal senders.
    #[test]
    fn zstd_with_version_1_and_snappy_with_version_2_both_resolve() {
        resolve(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { compression, .. } = kind else { unreachable!() };
            *compression = logit_config::RemoteWriteCompression::Zstd;
        }))
        .expect("zstd with version: 1 is the VictoriaMetrics wire");
        resolve(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { version, compression, .. } = kind else {
                unreachable!()
            };
            *version = logit_config::RemoteWriteVersion::V2;
            *compression = logit_config::RemoteWriteCompression::Snappy;
        }))
        .expect("snappy with version: 2 is the 2.0 wire");
    }

    /// Rule 41's checks are registry-mode only: a sender-mode component never reaches them, and
    /// rule 56 rejects a non-default `max_series` there.
    #[test]
    fn rule_41_does_not_fire_on_a_sender_mode_prometheus_out() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { max_series, .. } = kind else { unreachable!() };
            *max_series = 0;
        }));
        assert!(
            err.contains("'max_series'") && !err.contains("max_series: 0"),
            "rule 56 should name the field as wrong-mode, not rule 41's bound: {err}"
        );
    }

    #[test]
    fn a_prometheus_out_endpoint_that_is_not_an_absolute_http_url_is_rejected() {
        for bad in ["mimir:8080/api/v1/write", "/api/v1/write", "ftp://mimir/api/v1/write", ""] {
            let err = expect_err(remote_write_cfg(|kind| {
                let ComponentKind::PrometheusOut { endpoint, .. } = kind else { unreachable!() };
                *endpoint = Some(bad.to_string());
            }));
            assert!(
                err.contains("'out'") && err.contains("absolute 'http://'"),
                "{bad:?}: got: {err}"
            );
        }
    }

    #[test]
    fn a_zero_prometheus_out_endpoint_timeout_is_rejected() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { timeout, .. } = kind else { unreachable!() };
            *timeout = Duration::ZERO;
        }));
        assert!(err.contains("'out'") && err.contains("'timeout: 0s'"), "got: {err}");
    }

    #[test]
    fn a_reserved_remote_write_header_is_rejected_whatever_its_case() {
        for name in [
            "content-type",
            "Content-Encoding",
            "content-length",
            "X-Prometheus-Remote-Write-Version",
            "USER-AGENT",
        ] {
            let err = expect_err(remote_write_cfg(|kind| {
                let ComponentKind::PrometheusOut { headers, .. } = kind else { unreachable!() };
                headers.insert(name.to_string(), "x".to_string());
            }));
            assert!(
                err.contains("'out'") && err.contains("which this output sets itself"),
                "{name}: got: {err}"
            );
        }
    }

    #[test]
    fn an_empty_or_pseudo_remote_write_header_name_is_rejected() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { headers, .. } = kind else { unreachable!() };
            headers.insert(String::new(), "x".to_string());
        }));
        assert!(err.contains("empty header name"), "got: {err}");

        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { headers, .. } = kind else { unreachable!() };
            headers.insert(":authority".to_string(), "x".to_string());
        }));
        assert!(err.contains("pseudo-header"), "got: {err}");
    }

    #[test]
    fn two_remote_write_headers_differing_only_in_case_are_rejected() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { headers, .. } = kind else { unreachable!() };
            headers.insert("X-Scope-OrgID".to_string(), "a".to_string());
            headers.insert("x-scope-orgid".to_string(), "b".to_string());
        }));
        assert!(err.contains("differs only in") && err.contains("case"), "got: {err}");
    }

    #[test]
    fn an_inconsistent_endpoint_tls_block_is_rejected() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { endpoint, endpoint_tls, .. } = kind else {
                unreachable!()
            };
            *endpoint = Some("https://mimir:8080/api/v1/push".to_string());
            endpoint_tls.cert_file = Some("client.pem".to_string());
        }));
        assert!(err.contains("must both be set for mutual TLS"), "got: {err}");

        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { endpoint, endpoint_tls, .. } = kind else {
                unreachable!()
            };
            *endpoint = Some("https://mimir:8080/api/v1/push".to_string());
            endpoint_tls.insecure_skip_verify = true;
            endpoint_tls.ca_file = Some("ca.pem".to_string());
        }));
        assert!(err.contains("can't both be set"), "got: {err}");
    }

    /// Rule 40's third TLS check in this kind's spelling: a *scheme* check, not a mode check.
    #[test]
    fn an_endpoint_tls_block_under_a_plaintext_endpoint_is_rejected() {
        let err = expect_err(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { endpoint_tls, .. } = kind else { unreachable!() };
            endpoint_tls.ca_file = Some("ca.pem".to_string());
        }));
        assert!(err.contains("'out'") && err.contains("would have no effect"), "got: {err}");

        resolve(remote_write_cfg(|kind| {
            let ComponentKind::PrometheusOut { endpoint, endpoint_tls, .. } = kind else {
                unreachable!()
            };
            *endpoint = Some("https://mimir:8080/api/v1/push".to_string());
            endpoint_tls.ca_file = Some("ca.pem".to_string());
        }))
        .expect("an endpoint_tls block under an https endpoint is exactly what it is for");
    }

    // ---- rule 42: generate_in / null_out ------------------------------------------------------

    #[test]
    fn generate_in_is_a_listener_and_null_out_is_a_sink_and_both_are_implemented() {
        assert_eq!(kind_name(&generate_in()), "generate_in");
        assert_eq!(role(&generate_in()), Role::Listener);
        assert_eq!(kind_name(&null_out()), "null_out");
        assert_eq!(role(&null_out()), Role::Sink);
    }

    /// The perf harness's minimal scenario: a finite generator straight into a sink that drops
    /// everything, with nothing else in the graph at all.
    #[test]
    fn a_generate_in_and_a_null_out_alone_resolve() {
        let graph = resolve(cfg(vec![
            ("gen", vec![], generate_in_with_counts(Some(2_000_000), 100, Some(50_000))),
            ("sink", vec!["gen"], null_out()),
        ]))
        .expect("a finite generator into a null sink is the whole perf scenario shape");
        assert_eq!(graph.components["gen"].role(), Role::Listener);
        assert_eq!(graph.components["sink"].kind_name(), "null_out");
    }

    /// Rule 6: `generate_in` is a listener, so it may not declare sources.
    #[test]
    fn a_generate_in_with_sources_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], listener()),
            ("gen", vec!["in"], generate_in()),
            ("sink", vec!["gen"], null_out()),
        ]));
        assert!(err.contains("'gen'") && err.contains("listener"), "got: {err}");
    }

    /// Rule 6 again, from the sink side.
    #[test]
    fn a_null_out_with_no_sources_is_rejected() {
        let err = expect_err(cfg(vec![("sink", vec![], null_out())]));
        assert!(err.contains("'sink'") && err.contains("sink"), "got: {err}");
    }

    #[test]
    fn a_zero_generate_in_count_is_rejected() {
        let err = expect_err(cfg(vec![
            ("gen", vec![], generate_in_with_counts(Some(0), 100, None)),
            ("sink", vec!["gen"], null_out()),
        ]));
        assert!(err.contains("'gen'") && err.contains("count: 0"), "got: {err}");
    }

    #[test]
    fn a_zero_generate_in_batch_is_rejected() {
        let err = expect_err(cfg(vec![
            ("gen", vec![], generate_in_with_counts(None, 0, None)),
            ("sink", vec!["gen"], null_out()),
        ]));
        assert!(err.contains("'gen'") && err.contains("batch: 0"), "got: {err}");
    }

    #[test]
    fn a_zero_generate_in_rate_is_rejected() {
        let err = expect_err(cfg(vec![
            ("gen", vec![], generate_in_with_counts(None, 100, Some(0))),
            ("sink", vec!["gen"], null_out()),
        ]));
        assert!(err.contains("'gen'") && err.contains("rate: 0"), "got: {err}");
    }

    /// Omitting `count`/`rate` is how unbounded and unthrottled are spelled -- the check above is
    /// about `0` specifically, not about the field being absent.
    #[test]
    fn an_unbounded_unthrottled_generate_in_resolves() {
        resolve(cfg(vec![
            ("gen", vec![], generate_in_with_counts(None, 100, None)),
            ("sink", vec!["gen"], null_out()),
        ]))
        .expect("omitted count/rate mean unbounded and unthrottled, not zero");
    }

    #[test]
    fn an_empty_generate_in_metric_name_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "gen",
                vec![],
                generate_in_with_event(generate_event(
                    None,
                    vec![],
                    Some(generate_metric("", 1.0)),
                )),
            ),
            ("sink", vec!["gen"], null_out()),
        ]));
        assert!(
            err.contains("'gen'") && err.contains("'event.metric.name' is empty"),
            "got: {err}"
        );
    }

    #[test]
    fn a_non_finite_generate_in_metric_value_is_rejected() {
        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let err = expect_err(cfg(vec![
                (
                    "gen",
                    vec![],
                    generate_in_with_event(generate_event(
                        None,
                        vec![],
                        Some(generate_metric("requests", value)),
                    )),
                ),
                ("sink", vec!["gen"], null_out()),
            ]));
            assert!(err.contains("'gen'") && err.contains("must be a finite number"), "got: {err}");
        }
    }

    #[test]
    fn an_empty_generate_in_attribute_key_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "gen",
                vec![],
                generate_in_with_event(generate_event(None, vec![("", "web-1")], None)),
            ),
            ("sink", vec!["gen"], null_out()),
        ]));
        assert!(
            err.contains("'gen'") && err.contains("'event.attributes' has an empty key"),
            "got: {err}"
        );
    }

    #[test]
    fn an_empty_generate_in_resource_key_is_rejected() {
        let err = expect_err(cfg(vec![
            ("gen", vec![], generate_in_with_resource(vec![("", "web")])),
            ("sink", vec!["gen"], null_out()),
        ]));
        assert!(err.contains("'gen'") && err.contains("'resource' has an empty key"), "got: {err}");
    }

    /// Rule 42's parse check, reached through each of the four templated config paths in turn --
    /// a malformed template must be caught wherever it is written, not just in `event.log`.
    #[test]
    fn a_malformed_generate_in_template_is_rejected_in_every_templated_field() {
        let cases: Vec<(ComponentKind, &str)> = vec![
            (generate_in_with_event(generate_event(Some("host-{seq"), vec![], None)), "event.log"),
            (
                generate_in_with_event(generate_event(None, vec![("host", "web-{}")], None)),
                "event.attributes.host",
            ),
            (
                generate_in_with_event(generate_event(
                    None,
                    vec![],
                    Some(generate_metric("requests}", 1.0)),
                )),
                "event.metric.name",
            ),
            (generate_in_with_resource(vec![("service.name", "{seq")]), "resource.service.name"),
        ];
        for (kind, field) in cases {
            let err =
                expect_err(cfg(vec![("gen", vec![], kind), ("sink", vec!["gen"], null_out())]));
            assert!(
                err.contains("'gen'") && err.contains(field) && err.contains("is not a template"),
                "got: {err}"
            );
        }
    }

    /// Rule 42: a placeholder `generate_in` can't substitute is rejected, not rendered literally or
    /// as nothing.
    #[test]
    fn an_unknown_generate_in_placeholder_is_rejected() {
        for name in ["seg", "SEQ", "seq%", "seq%0", "seq%x", "seq%-1", "seq-1", "hostname", "seq "]
        {
            let err = expect_err(cfg(vec![
                (
                    "gen",
                    vec![],
                    generate_in_with_event(generate_event(
                        Some(&format!("host-{{{name}}}")),
                        vec![],
                        None,
                    )),
                ),
                ("sink", vec!["gen"], null_out()),
            ]));
            assert!(
                err.contains("'gen'") && err.contains("doesn't substitute"),
                "{name}: got: {err}"
            );
        }
    }

    /// Rule 42: a bare `{seq}` in the interned metric name is rejected; it would leak one interned
    /// name per generated event.
    #[test]
    fn a_bare_seq_in_a_generate_in_metric_name_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "gen",
                vec![],
                generate_in_with_event(generate_event(
                    None,
                    vec![],
                    Some(generate_metric("requests.{seq}", 1.0)),
                )),
            ),
            ("sink", vec!["gen"], null_out()),
        ]));
        assert!(
            err.contains("'gen'")
                && err.contains("'event.metric.name' may not use '{seq}'")
                && err.contains("interned for the life of the process"),
            "got: {err}"
        );
    }

    /// The bounded form stays legal there: `N` bounds the interner's growth.
    #[test]
    fn a_bounded_seq_modulus_in_a_generate_in_metric_name_resolves() {
        resolve(cfg(vec![
            (
                "gen",
                vec![],
                generate_in_with_event(generate_event(
                    None,
                    vec![],
                    Some(generate_metric("requests.{seq%100}", 1.0)),
                )),
            ),
            ("sink", vec!["gen"], null_out()),
        ]))
        .expect("a bounded metric-name modulus is the point of the feature");
    }

    /// ...and the same bare `{seq}` stays legal everywhere it is *copied* onto the event rather
    /// than interned, which is every other templated field: the copy frees with its event.
    #[test]
    fn a_bare_seq_is_accepted_outside_a_generate_in_metric_name() {
        resolve(cfg(vec![
            (
                "gen",
                vec![],
                generate_in_full(
                    None,
                    100,
                    None,
                    generate_event(Some("n={seq}"), vec![("n", "{seq}")], None),
                    vec![("shard", "{seq}")],
                ),
            ),
            ("sink", vec!["gen"], null_out()),
        ]))
        .expect("a copied rendering is freed with its event, so an unbounded seq is fine");
    }

    /// Rule 42: `{seq%+5}` is rejected; `N` is ASCII digits only, so `{seq%5}` has one spelling.
    #[test]
    fn a_seq_modulus_with_a_leading_plus_is_rejected() {
        let err = expect_err(cfg(vec![
            (
                "gen",
                vec![],
                generate_in_with_event(generate_event(Some("host-{seq%+5}"), vec![], None)),
            ),
            ("sink", vec!["gen"], null_out()),
        ]));
        assert!(err.contains("'gen'") && err.contains("doesn't substitute"), "got: {err}");
    }

    /// The other side of the same rule: the two names `generate_in` does substitute, in every
    /// templated field, alongside the `{{`/`}}` escape a JSON log body needs.
    #[test]
    fn the_seq_placeholders_and_escaped_braces_resolve_in_every_templated_field() {
        resolve(cfg(vec![
            (
                "gen",
                vec![],
                generate_in_full(
                    Some(1000),
                    10,
                    Some(100),
                    generate_event(
                        Some(r#"{{"path":"/x/{seq%50}","n":{seq}}}"#),
                        vec![("host", "web-{seq%10}")],
                        Some(generate_metric("requests.{seq%4}", 1.0)),
                    ),
                    vec![("service.name", "web-{seq%2}")],
                ),
            ),
            ("sink", vec!["gen"], null_out()),
        ]))
        .expect("{seq} and {seq%N} are exactly what generate_in substitutes");
    }

    /// Rule 17: `generate_in` is a listener by role, but it has no socket, no queue, and no
    /// decoder -- the same reason `internal` rejects a `receive:` block.
    #[test]
    fn a_non_default_receive_on_generate_in_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            ("gen", vec![], generate_in(), non_default_receive()),
            ("sink", vec!["gen"], null_out(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'gen'"), "got: {err}");
        assert!(
            err.contains("'receive' is only meaningful on a datagram, stream or tail listener"),
            "got: {err}"
        );
    }

    /// A graph may be disconnected: the harness's `native-relay` scenario puts both ends of a
    /// `logit_out`/`logit_in` hop in one config as two chains. Rules 2/5/7 hold per chain.
    #[test]
    fn a_logit_out_and_logit_in_in_one_graph_resolve() {
        let relay_out = ComponentKind::LogitOut {
            endpoint: "127.0.0.1:19001".to_string(),
            compression: Compression::None,
            tls: None,
            request_timeout: Duration::from_secs(10),
        };
        let relay_in = ComponentKind::LogitIn {
            bind: "127.0.0.1:19001".to_string(),
            tls: None,
            max_frame_bytes: None,
            handshake_timeout: default_handshake_timeout(),
            idle_timeout: None,
        };
        let graph = resolve(cfg(vec![
            ("gen", vec![], generate_in_with_counts(Some(2_000_000), 100, None)),
            ("relay_out", vec!["gen"], relay_out),
            ("relay_in", vec![], relay_in),
            ("sink", vec!["relay_in"], null_out()),
        ]))
        .expect("two disconnected chains in one graph are a perfectly good pipeline");
        assert_eq!(graph.topological_order.len(), 4);
        assert!(graph.components["relay_out"].consumers.is_empty());
        assert_eq!(graph.components["sink"].sources, vec!["relay_in".to_string()]);
    }

    // ---- `statsd_in`'s stream transport: rules 43/45/17 -----------------------------------------

    /// A `statsd_in` with `transport`/`tls:`/`handshake_timeout` spelled out -- an enum variant
    /// has no functional-record-update syntax, so every case below goes through this.
    fn statsd_in_full(
        transport: StatsdTransport,
        tls: bool,
        handshake_timeout: Duration,
    ) -> ComponentKind {
        ComponentKind::StatsdIn {
            bind: "127.0.0.1:0".to_string(),
            transport,
            tls: tls.then(|| logit_config::TlsServerConfig {
                cert_file: "server.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
            handshake_timeout,
            idle_timeout: None,
        }
    }

    /// [`statsd_in_full`] with rule 53's knob exposed instead -- the `Option` that rule reads.
    fn statsd_in_with_idle_timeout(
        transport: StatsdTransport,
        idle_timeout: Option<Duration>,
    ) -> ComponentKind {
        ComponentKind::StatsdIn {
            bind: "127.0.0.1:0".to_string(),
            transport,
            tls: None,
            handshake_timeout: default_handshake_timeout(),
            idle_timeout,
        }
    }

    /// Rule 43 over `statsd_in`: a `tls:` block under UDP is rejected.
    #[test]
    fn a_udp_statsd_in_with_tls_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], statsd_in_full(StatsdTransport::Udp, true, default_handshake_timeout())),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'tls:' needs 'transport: tcp'"), "got: {err}");
        assert!(err.contains("DTLS"), "got: {err}");
    }

    /// Rule 43's other side for statsd: a TLS-terminating TCP `statsd_in` resolves.
    #[test]
    fn a_tcp_statsd_in_with_tls_resolves_fine() {
        resolve(cfg(vec![
            ("in", vec![], statsd_in_full(StatsdTransport::Tcp, true, default_handshake_timeout())),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a TLS-terminating TCP statsd_in is legal");
    }

    /// Rule 45 over `statsd_in`: `0s` is rejected on either transport, a non-default value under
    /// UDP is rejected, and a real value under TCP resolves.
    #[test]
    fn a_non_default_handshake_timeout_on_a_udp_statsd_in_is_rejected() {
        let zero = expect_err(cfg(vec![
            ("in", vec![], statsd_in_full(StatsdTransport::Tcp, false, Duration::ZERO)),
            ("out", vec!["in"], sink()),
        ]));
        assert!(zero.contains("'handshake_timeout' must be greater than 0s"), "got: {zero}");

        let on_udp = expect_err(cfg(vec![
            ("in", vec![], statsd_in_full(StatsdTransport::Udp, false, Duration::from_secs(2))),
            ("out", vec!["in"], sink()),
        ]));
        assert!(on_udp.contains("needs 'transport: tcp'"), "got: {on_udp}");
        assert!(on_udp.contains("UDP statsd_in"), "got: {on_udp}");

        resolve(cfg(vec![
            ("in", vec![], statsd_in_full(StatsdTransport::Tcp, false, Duration::from_secs(2))),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a non-default handshake_timeout on a TCP statsd_in is what the field is for");

        // And the *default* value is not a set one, on either transport -- the defaulted-value
        // early return in rule 45. `listener()` above is a bare UDP `statsd_in`, so every other
        // test in this module depends on this holding.
        resolve(cfg(vec![
            (
                "in",
                vec![],
                statsd_in_full(StatsdTransport::Udp, false, default_handshake_timeout()),
            ),
            ("out", vec!["in"], sink()),
        ]))
        .expect("a UDP statsd_in that never mentions handshake_timeout must keep resolving");
    }

    /// Rule 17: a TCP `statsd_in` has no receive queue, so a queue field is rejected by name, while
    /// the UDP arm keeps its own.
    #[test]
    fn a_tcp_statsd_in_rejects_receive_max_datagrams() {
        let err = expect_err(cfg_with_receive(vec![
            (
                "in",
                vec![],
                statsd_in_full(StatsdTransport::Tcp, false, default_handshake_timeout()),
                non_default_receive(),
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'in'"), "got: {err}");
        assert!(err.contains("'receive.max_datagrams'"), "got: {err}");
        assert!(err.contains("a stream listener has no receive queue"), "got: {err}");
        assert!(err.contains("UDP statsd_in"), "got: {err}");

        let graph = resolve(cfg_with_receive(vec![
            (
                "in",
                vec![],
                statsd_in_full(StatsdTransport::Udp, false, default_handshake_timeout()),
                non_default_receive(),
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("a UDP statsd_in is still a datagram listener with a real receive queue");
        assert_eq!(graph.components["in"].receive.max_datagrams, 4096);

        // The batch-assembly half still applies on TCP, per connection (rule 17's split).
        let graph = resolve(cfg_with_receive(vec![
            (
                "in",
                vec![],
                statsd_in_full(StatsdTransport::Tcp, false, default_handshake_timeout()),
                ReceiveConfig { batch_max_events: 1, ..ReceiveConfig::default() },
            ),
            ("out", vec!["in"], sink(), ReceiveConfig::default()),
        ]))
        .expect("batch_max_events is one of the fields a stream listener may override");
        assert_eq!(graph.components["in"].receive.batch_max_events, 1);
    }
}
