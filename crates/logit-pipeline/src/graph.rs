//! Pure resolution and validation of a [`Config`] into a [`Graph`]. No channels, no threads, no
//! tokio -- mirrors how `apply_transforms` in the pre-graph `logit-cli::pipeline` was kept pure
//! specifically for unit-testability. `logit run`, `logit validate`, and `logit graph` are all
//! just different things layered on top of this one function's output.
//!
//! Validation rules, in order (`docs/design/pipeline-graph.md`):
//! 1. At least one component.
//! 2. Every `sources` id resolves to a defined component.
//! 3. No self-reference.
//! 4. No duplicate source within one component's `sources` list.
//! 5. No cycles.
//! 6. Arity per kind (listener: no sources; transform/sink: at least one).
//! 7. Every non-sink component has at least one consumer.
//! 8. Kind is implemented.
//! 9. No zero-length `interval` on a kind that has one.
//! 10. A `kv_metrics` with counters, gauges, and distributions all empty is rejected -- it can
//!     only ever be a no-op, the same silent-black-hole failure rule 7 exists to catch.
//! 11. A `kv_metrics` distribution entry with no `field` is rejected -- a distribution of nothing
//!     is meaningless (`docs/adr/kv-metrics-semantics.md`).
//! 12. A `kv_metrics` counter, gauge, or distribution entry with an empty `name` is rejected -- the
//!     implemented `influxdb_out` sink can't encode a metric with no measurement name (Influx line
//!     protocol requires one), so this must be caught here rather than surfacing as a runtime sink
//!     failure the first time such an event arrives.
//! 13. At most one `internal` component -- two would each drain (and so split) the same
//!     process-wide telemetry `Registry`, silently halving whichever one a downstream consumer
//!     happened not to be reading from rather than failing clearly.
//! 14. A non-default `buffer:` block on a non-sink component is rejected -- `buffer:`
//!     (`docs/adr/buffered-sink-delivery.md`) configures a sink's delivery queue, which only a
//!     sink has, so a listener or transform carrying one is almost certainly a misplaced block
//!     rather than a meaningful setting silently ignored.
//! 15. A sink's `buffer.max_batches` or `buffer.max_bytes` of `0` is rejected -- an impossible
//!     bound (no batch could ever be queued) rather than a small one.
//! 16. `internal`'s `span_sample_rate` must be finite and within `[0, 1]` -- a config error, not
//!     something to clamp silently.
//! 17. A non-default `receive:` block is rejected on any kind that is not a datagram listener
//!     (today `statsd_in`/`syslog_in`) or a tail listener (`tail_in`/`docker_in`) --
//!     `receive:` (`docs/adr/decoupled-listener-io.md`) configures a listener's receive-side
//!     batch assembly, and a datagram listener's socket-side receive queue on top of that. A
//!     tail listener has no such queue (the tailed file is its own durable buffer), so it may
//!     only set `receive`'s batch-assembly/shutdown-grace fields -- a queue-bounding field
//!     (`max_datagrams`, `max_bytes`, `overflow`, `receive_buffer_bytes`) is rejected by name on
//!     one. Deliberately **not** `role(&kind) != Role::Listener`: `internal` is a listener by
//!     role but has no socket, no queue, and no decoder, so `receive:` on it would be a
//!     silently-ignored setting -- exactly what this rule exists to catch on the sink side (rule
//!     14). A future listener kind rejects `receive:` until it is actually wired to one of these
//!     two drivers.
//! 18. A datagram listener's `receive.max_datagrams` or `receive.max_bytes` of `0` is rejected;
//!     a datagram or tail listener's `receive.batch_max_events` or `receive.batch_max_bytes` of
//!     `0` is rejected -- each an impossible bound, the twin of rule 15.
//!     `receive.batch_flush_interval: 0s` is **not** rejected: it means "no flush timer," a
//!     meaningful setting, unlike the count bounds.
//! 19. A `trace_context` with an empty `trace_id`, `span_id`, or `flags` field name is rejected
//!     -- it could never name a real attribute, so that lookup can only ever be a no-op, the same
//!     reasoning rules 10-12 already apply to `kv_metrics`/`set` (`null`, not `""`, is how an
//!     optional lookup is disabled).
//! 20. A `scale` with an empty `fields` map, an empty field name, or a non-finite factor is
//!     rejected (`docs/adr/scale-transform.md`).
//! 21. An empty `signals:` list on `has_signal`, `keep_signals`, or `drop_signals` is rejected.
//!     `keep_signals`/`drop_signals` additionally reject naming all three signals. Which of the
//!     two shapes is the silent black hole (rule 7's "no consumer" failure, recast here as "no
//!     event ever gets through") and which is the no-op (every event forwarded untouched) is
//!     *opposite* between the two kinds -- an allowlist naming nothing keeps nothing (black
//!     hole), naming everything keeps everything (no-op); a denylist is the mirror. Both shapes
//!     are rejected either way, but the error message names the right one. `keep`'s empty
//!     `fields` list stays legal by contrast -- "drop every attribute" is a real operation,
//!     "drop every event" is not. See `docs/adr/signal-filtering-components.md`.
//! 22. An `otlp_out` `headers:` entry naming an empty string, an HTTP/2 pseudo-header (starting
//!     with `:`), any `grpc-*` header, or another header the wire transport itself sets
//!     (`content-type`, etc. -- see `RESERVED_OTLP_HEADERS`) is rejected, case-insensitively --
//!     almost certainly a config mistake, not a meaningful override. Two entries naming the same
//!     header once case is ignored (HTTP header names are case-insensitive) are also rejected --
//!     which value would actually be sent is otherwise undefined.
//! 23. A non-empty `otlp_out` `paths:` under `protocol: grpc` is rejected -- gRPC method names
//!     are fixed by the OTLP service definitions, not a mount point `paths` can move, so silently
//!     ignoring it would be a worse failure mode than a clear error.
//! 24. An `otlp_out` `tls:` block must be internally consistent (`cert_file`/`key_file`
//!     together, no `insecure_skip_verify` alongside `ca_file`) and is rejected under a
//!     non-`https://` endpoint, where it would have no effect
//!     (`docs/adr/otlp-tls-and-pooled-grpc-client.md`).
//! 25. A `trace_context` `span:` block with an empty `name` (OTLP requires a span name) or a
//!     `max_skew` of `0s` (an impossible window -- every span would be rejected as skewed) is
//!     rejected (`docs/adr/trace-context-span-lifting.md`).
//! 26. A `tail_in` with an empty `paths`, an empty `paths` entry, or a `*` outside the final
//!     path component is rejected (`docs/adr/file-tailing-and-docker-json-logs.md`).
//! 27. A `docker_in` with an empty `containers` and no `discover: true`, an empty `containers`/
//!     `labels` entry, a duplicate `containers` entry, or an empty `root` is rejected
//!     (`docs/adr/file-tailing-and-docker-json-logs.md`).
//! 28. A `tail_in`/`docker_in` with a `poll_interval`, `checkpoint_interval`, or `max_line_bytes`
//!     of `0` is rejected -- each would busy-loop, thrash the checkpoint file, or drop every
//!     line, the same "0 is impossible" reasoning as rule 9.
//! 29. A `kv` with an empty `pair_sep` or `kv_sep`, with `pair_sep == kv_sep`, or with a `kv_sep`
//!     that *contains* `pair_sep`, is rejected. An empty separator makes splitting yield a
//!     boundary between every character; identical separators mean every segment is split away
//!     from its own separator, so no line could ever produce a pair; and a `kv_sep` containing
//!     `pair_sep` can never appear intact inside a segment, since the `pair_sep` split runs
//!     first -- each is a certain no-op or a certain garbage result, catchable at `logit
//!     validate` time.
//! 30. A `regex` `pattern` that doesn't compile, or that declares no named capture group, is
//!     rejected -- and so is an empty `field` name. The pattern is compiled here, not deferred
//!     to `build_spec`, so an invalid one is a `logit validate` error rather than a run-time
//!     surprise. The compiled `Regex` is then dropped and rebuilt in `build_spec`, matching how
//!     every other kind re-derives from its raw `ComponentKind` -- one `Regex::new` at process
//!     start is not worth inventing a mechanism for. A pattern with no named group could only
//!     ever be a no-op; an empty `field` name could never match a real attribute. A duplicate
//!     capture-group name needs no separate check -- the `regex` crate rejects it at compile
//!     time already.
//! 31. A `csv` with an empty `columns` list, an empty column name, or a duplicate column name is
//!     rejected, as is a `delimiter` that is `"` (RFC 4180's quote character), `\n`/`\r`
//!     (already consumed as line framing by every input), or non-ASCII. The empty-list and
//!     empty-name clauses are the "can only ever be a no-op" rule again; the duplicate clause is
//!     the "a repeated entry silently doubles rather than erroring" rule applied to columns
//!     instead of sources.
//! 40. (Numbers 32-39 belong to other components' rules that landed after this list's numbering
//!     already drifted from the code, per the note on rule 12 above -- left unnumbered here rather
//!     than renumbered, so a rule referenced elsewhere by its own PR keeps the number it was given
//!     there.) A `prometheus_in` `targets` must be non-empty, and every entry must parse as an
//!     absolute `http://`/`https://` URL with a non-empty authority -- `logit-pipeline` doesn't
//!     depend on `reqwest`/`url` (`docs/design/pipeline-graph.md`'s crate layout), so this is a
//!     small hand-rolled scheme/authority check, not a real URL parse. A `tls:` block must be
//!     internally consistent -- `cert_file`/`key_file` set together, no `insecure_skip_verify`
//!     alongside `ca_file` -- the same two checks rule 24 makes for `otlp_out`'s own `tls:` block
//!     (and rule 34 for `logit_out`'s) -- and is rejected outright unless at least one target is
//!     `https://` (the same "would have no effect" reasoning as rule 24's third check).
//!     `timeout: 0s` is rejected (the same "0 is impossible" reasoning as rule 9's `interval`).
//!     `headers` may not name a header this input sets itself (`accept`, `user-agent`, `host`,
//!     `content-length`, `te`, `transfer-encoding`, `connection`, an empty name, or an HTTP/2
//!     pseudo-header starting with `:`), checked case-insensitively, and no two entries may
//!     collide once case is ignored -- the same shape rule 22 already checks for `otlp_out`.
//!
//! Sink reachability from a listener needs no separate rule -- it's implied by 2 + 5 + 7: every
//! acyclic chain of sourced components terminates somewhere, and every non-terminal component in
//! it is required (by 7) to have a consumer, so the chain can only terminate at a sink.

use logit_config::{
    BufferConfig, Component, ComponentKind, Compression, Config, ReceiveConfig, StreamFormat,
};
use logit_proto::frame::MAX_SANE_UNCOMPRESSED_LEN;
use std::collections::{BTreeSet, HashMap, VecDeque};
use std::time::Duration;

/// A component's arity class, fixed by its `kind` (`docs/design/pipeline-graph.md`'s arity
/// table) -- never derived from topology, so a typo'd source reference can't silently reclassify
/// a component instead of producing a clear error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    Listener,
    Transform,
    Sink,
}

impl Role {
    /// A stable, lowercase name for this role -- used to stamp `logit.component.*` telemetry
    /// points with which arity class produced them (`docs/design/internal-telemetry.md`).
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Listener => "listener",
            Role::Transform => "transform",
            Role::Sink => "sink",
        }
    }
}

/// The arity class a kind belongs to. Public so `logit graph` (`logit-cli`) can style nodes by
/// role directly off a `Config`, without needing a fully-resolved `Graph` -- useful precisely
/// because it lets `logit graph` render *something* even for a config that fails validation
/// (`docs/design/pipeline-graph.md`'s "`logit graph`" section).
pub fn role(kind: &ComponentKind) -> Role {
    use ComponentKind::*;
    match kind {
        StatsdIn { .. }
        | SyslogIn { .. }
        | OtlpIn { .. }
        | TailIn { .. }
        | DockerIn { .. }
        | LogitIn { .. }
        | Internal { .. }
        | PrometheusIn { .. } => Role::Listener,
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
        | Logfmt { .. }
        | Kv { .. }
        | Regex { .. } => Role::Transform,
        InfluxDbOut { .. }
        | OtlpOut { .. }
        | LogitOut { .. }
        | StdioOut { .. }
        | FileOut { .. }
        | SyslogOut { .. }
        | StatsdOut { .. } => Role::Sink,
    }
}

/// A stable, human-readable name for this kind -- exactly the config `type` tag it deserializes
/// from. Not derived from `Serialize` (that would round-trip a whole `Component`, not just name a
/// variant) -- alongside [`role`], this is the one other place that must be kept in sync with a
/// new `ComponentKind` variant landing ("the kind already knows its own arity",
/// `docs/design/pipeline-graph.md`, extended here to naming). Used to stamp `logit.component.*`
/// telemetry points with which kind produced them (`docs/design/internal-telemetry.md`) --
/// `logit-cli::pipeline::build_spec` is the one caller.
pub fn kind_name(kind: &ComponentKind) -> &'static str {
    use ComponentKind::*;
    match kind {
        StatsdIn { .. } => "statsd_in",
        SyslogIn { .. } => "syslog_in",
        OtlpIn { .. } => "otlp_in",
        TailIn { .. } => "tail_in",
        DockerIn { .. } => "docker_in",
        LogitIn { .. } => "logit_in",
        Internal { .. } => "internal",
        PrometheusIn { .. } => "prometheus_in",
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
        Logfmt { .. } => "logfmt",
        Kv { .. } => "kv",
        Regex { .. } => "regex",
        InfluxDbOut { .. } => "influxdb_out",
        OtlpOut { .. } => "otlp_out",
        LogitOut { .. } => "logit_out",
        StdioOut { .. } => "stdio_out",
        FileOut { .. } => "file_out",
        SyslogOut { .. } => "syslog_out",
        StatsdOut { .. } => "statsd_out",
    }
}

/// The single source of truth for which `ComponentKind`s the runtime can actually build --
/// mirrors the pre-graph `require_implemented_input`/`require_implemented_output`/
/// `require_implemented_transform` trio, now unified over one enum.
fn is_implemented(kind: &ComponentKind) -> bool {
    matches!(
        kind,
        ComponentKind::StatsdIn { .. }
            | ComponentKind::SyslogIn { .. }
            | ComponentKind::OtlpIn { .. }
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
            | ComponentKind::Logfmt { .. }
            | ComponentKind::Kv { .. }
            | ComponentKind::Regex { .. }
            | ComponentKind::InfluxDbOut { .. }
            | ComponentKind::OtlpOut { .. }
            | ComponentKind::StdioOut { .. }
            | ComponentKind::FileOut { .. }
            | ComponentKind::SyslogOut { .. }
            | ComponentKind::LogitIn { .. }
            | ComponentKind::LogitOut { .. }
            | ComponentKind::StatsdOut { .. }
    )
}

/// `Some(interval)` for a kind with an `interval` field, `Aggregate`'s always populated,
/// `Lua`/`LuaFile`'s only when set. `None` either means no `interval` field on this kind, or a
/// `Lua`/`LuaFile` component that left it unset -- both are "never flushes", so rule 9 treats
/// them the same: nothing to reject.
fn interval(kind: &ComponentKind) -> Option<Duration> {
    match kind {
        ComponentKind::Lua { interval, .. } | ComponentKind::LuaFile { interval, .. } => *interval,
        ComponentKind::Aggregate { interval, .. }
        | ComponentKind::Internal { interval, .. }
        | ComponentKind::PrometheusIn { interval, .. } => Some(*interval),
        _ => None,
    }
}

/// `true` if `signals` names all three of `Logs`/`Metrics`/`Traces` -- rule 19's shared test for
/// `keep_signals`/`drop_signals`'s two opposite black-hole shapes.
fn names_all_three(signals: &[logit_config::Signal]) -> bool {
    signals.contains(&logit_config::Signal::Logs)
        && signals.contains(&logit_config::Signal::Metrics)
        && signals.contains(&logit_config::Signal::Traces)
}

pub struct ResolvedComponent {
    pub sources: Vec<String>,
    pub consumers: Vec<String>,
    pub kind: ComponentKind,
    /// Per-sink delivery buffer config (`docs/adr/buffered-sink-delivery.md`). Validated as
    /// sink-only by [`resolve`] (rule 14); meaningless on any other role, so a non-sink component's
    /// value here is always [`BufferConfig::default`] once resolution has succeeded.
    pub buffer: BufferConfig,
    /// Per-listener receive queue/batching config (`docs/adr/decoupled-listener-io.md`).
    /// Validated as datagram-listener-only by [`resolve`] (rule 17); meaningless on any other
    /// kind, so its value here is always [`ReceiveConfig::default`] once resolution has succeeded.
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
    /// Listener-first, sink-last ("produce before consume") order. Used by `logit graph` for
    /// deterministic output; the node runtime doesn't need it -- every component's inbox channel
    /// is created up front, independent of build order, so there's nothing dependency-ordering
    /// actually has to protect there.
    pub topological_order: Vec<String>,
}

/// Non-gRPC header names `otlp_out`'s HTTP transport sets itself, or that HTTP/1.1's own
/// connection-management semantics reserve regardless of transport
/// (`crates/logit-outputs/src/otlp.rs`'s `send_http`). Checked case-insensitively, matching
/// HTTP's own header-name semantics. Every `grpc-*` name is reserved too -- checked separately,
/// by prefix, in `resolve` -- since the gRPC wire protocol defines a whole namespace of them
/// (`grpc-encoding`, `grpc-status`, `grpc-trace-bin`, ...), not just the handful
/// `crates/logit-outputs/src/otlp.rs`'s `grpc_roundtrip` happens to set today; a fixed list here
/// would silently stop covering a `grpc-*` header this project starts setting later.
const RESERVED_OTLP_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "content-encoding",
    "host",
    "te",
    "transfer-encoding",
    "connection",
];

/// Header names `prometheus_in`'s scrape client sets itself (rule 40) --
/// `crates/logit-inputs/src/prometheus.rs`'s `scrape_target` unconditionally sends `Accept` (the
/// dialect-negotiation header) and `User-Agent`, in addition to the same connection-management
/// names `RESERVED_OTLP_HEADERS` already reserves for `otlp_out` (this listener never sends a
/// body, so `content-type`/`content-encoding` aren't actually load-bearing here, but naming them
/// too costs nothing and keeps this list's shape recognizable next to that one).
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

/// Rule 40's URL check: `targets` must be absolute `http://`/`https://` URLs with a non-empty
/// authority. `logit-pipeline` doesn't depend on `reqwest`/`url` (`docs/design/pipeline-graph.md`'s
/// crate layout keeps this crate free of any concrete protocol's dependencies), so this is a small
/// hand-rolled scheme/authority check rather than a real URL parse -- good enough to catch a typo'd
/// scheme or a bare `host:port` with none at all, which is what this rule exists for; the real
/// parse (`reqwest::Url::parse`) happens once more, harmlessly, in `crates/logit-inputs/src/
/// prometheus.rs` itself when building each target's resource.
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

    if components.is_empty() {
        anyhow::bail!("config defines no components");
    }

    // Rules 2 + 3 + 4: every source resolves, no self-reference, no duplicate source within one
    // component's `sources` list. The last of these matters beyond tidiness: a duplicate would
    // otherwise push the same consumer id into `consumers` twice below, so that source's `Fanout`
    // would hold two live `Sender` clones pointing at the same inbox and deliver every batch to
    // it twice -- a repeated source id would silently double telemetry (and, through an
    // `aggregate` component, double every aggregated count) rather than being rejected as the
    // config typo it almost certainly is.
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

    // Rule 5: cycle detection, via Kahn's algorithm -- its natural byproduct is also the
    // listener-first topological order `Graph::topological_order` publishes.
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

    // Rule 7: every non-sink component needs at least one consumer.
    for (id, component) in &components {
        if role(&component.kind) != Role::Sink && consumers.get(id).is_none_or(Vec::is_empty) {
            anyhow::bail!("component '{id}' has no consumers -- nothing reads what it produces");
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

    // Rules 10 + 11: `kv_metrics`-specific validation. Neither is a generic arity/interval check,
    // so each gets its own loop rather than folding into rules 6/9 above.
    for (id, component) in &components {
        if let ComponentKind::KvMetrics { counters, gauges, distributions } = &component.kind {
            if counters.is_empty() && gauges.is_empty() && distributions.is_empty() {
                anyhow::bail!(
                    "component '{id}': a kv_metrics with no counters, gauges, or distributions \
                     configured can only ever be a no-op"
                );
            }
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

    // Rule 12: `set`-specific validation -- neither map configured can only ever be a no-op,
    // exactly the `kv_metrics` rule above, for the same reason. An empty key in either map could
    // never name a real attribute -- rule 19/20's reasoning, applied here too so `has_attributes`/
    // `drop_attributes` (rule 36) can claim their own empty-key rejection actually bounds what
    // `set` can stamp: `has_attributes`' config is `set`'s config, and this keeps that true.
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

    // Rule 14: `buffer:` is a sink-only concept -- a non-default value on any other role is
    // almost certainly a misplaced block, not a setting that would be silently honored.
    for (id, component) in &components {
        if component.buffer != BufferConfig::default() && role(&component.kind) != Role::Sink {
            anyhow::bail!(
                "component '{id}': 'buffer' is only meaningful on a sink, but '{id}' is a {}",
                role(&component.kind).as_str()
            );
        }
    }

    // Rule 15: `max_batches: 0` or `max_bytes: 0` is an impossible bound, not a small one -- it
    // makes every push overflow unconditionally, even against an empty queue, with nothing a
    // concurrent commit could ever do to free room (`SinkQueue::push`'s "impossible to ever fit"
    // check tolerates this at runtime rather than hanging, but a config that can never accept a
    // single batch is a mistake worth catching here, not something to silently degrade around).
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

    // Rule 16: `internal`'s `span_sample_rate` must be finite and within `[0, 1]` -- a config
    // error, not something to clamp silently. `trace_is_sampled` (`crates/logit-core/src/
    // telemetry.rs`) treats NaN as "keep everything," which would be a surprising thing to get
    // from a typo (`span_sample_rate: tru` parsing as a string coerced to NaN, say) rather than a
    // deliberate "sample everything" choice; a value above 1 or below 0 is unambiguously a
    // mistake, since neither has a sensible "keep more/less than everything" reading.
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

    // Rule 17: `receive:` is a datagram- or tail-listener-only concept -- see this module's own
    // doc comment on why this checks dedicated predicates rather than `role() == Role::Listener`
    // (which would wrongly also permit `internal`). A tail listener has no receive *queue* (the
    // tailed file is its own durable buffer), so it may only set the batch-assembly/shutdown-
    // grace fields `receive:` also carries -- the queue-bounding fields
    // (`max_datagrams`/`max_bytes`/`overflow`/`receive_buffer_bytes`) stay datagram-only and are
    // named individually here, not just rejected as "any non-default field", so the error points
    // at exactly what doesn't apply rather than making an operator guess.
    for (id, component) in &components {
        if component.receive == ReceiveConfig::default() {
            continue;
        }
        if is_datagram_listener(&component.kind) {
            continue;
        }
        if is_tail_listener(&component.kind) {
            let default = ReceiveConfig::default();
            let queue_only_field = if component.receive.max_datagrams != default.max_datagrams {
                Some("max_datagrams")
            } else if component.receive.max_bytes != default.max_bytes {
                Some("max_bytes")
            } else if component.receive.overflow != default.overflow {
                Some("overflow")
            } else if component.receive.receive_buffer_bytes != default.receive_buffer_bytes {
                Some("receive_buffer_bytes")
            } else {
                None
            };
            if let Some(field) = queue_only_field {
                anyhow::bail!(
                    "component '{id}': 'receive.{field}' is only meaningful on a datagram \
                     listener (statsd_in, syslog_in) -- a tail listener has no receive queue; \
                     only receive.batch_max_events, batch_max_bytes, batch_flush_interval, and \
                     shutdown_grace apply"
                );
            }
            continue;
        }
        anyhow::bail!(
            "component '{id}': 'receive' is only meaningful on a datagram or tail listener \
             (statsd_in, syslog_in, tail_in, docker_in), but '{id}' is a {}",
            role(&component.kind).as_str()
        );
    }

    // Rule 18: the twin of rule 15, for a listener's receive-side batch assembly -- `0` on any
    // of the four count/byte bounds is an impossible bound, never a small one.
    // `batch_flush_interval: 0s` is deliberately not checked here: zero there means "no timer,"
    // a meaningful setting. `max_datagrams`/`max_bytes` (the receive *queue*'s own bounds) are
    // datagram-listener-only, since a tail listener has no such queue (rule 17).
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
        }
        if is_datagram_listener(&component.kind) || is_tail_listener(&component.kind) {
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

    // Rule 26: `tail_in`'s `paths` -- at least one, none empty, and a `*` (this driver's only
    // wildcard) permitted only in the final path component. An unrestricted `*` (e.g.
    // `/var/*/app.log`) would make the same glob match a moving set of *directories*, not just
    // files, which this driver's minimal matcher doesn't attempt to reason about.
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

    // Rule 27: `docker_in`'s `containers`/`discover`/`root`/`labels` shape. `containers` empty
    // and `discover` unset would silently tail nothing -- the same black-hole reasoning rule 7
    // exists to catch, just not derivable from arity alone here. No empty entry in `containers`/
    // `labels` (an empty string can never match a real container or a real label key), no
    // duplicate `containers` entry (a repeated selector is always a config mistake, never
    // meaningful), and `root` must be non-empty (an empty path would resolve to the process's own
    // working directory, almost certainly not intended).
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

    // Rule 28: a tail listener's timing knobs must be positive -- `0s` on either would busy-loop
    // (`poll_interval`) or write the checkpoint on every single tick (`checkpoint_interval`), the
    // same "0 is impossible, not just small" reasoning as rule 9's flush interval.
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

    // Rule 19: `trace_context`-specific validation -- an empty field name could never name a
    // real attribute, so that lookup can only ever be a no-op, the same reasoning rules 10-12
    // already apply to `kv_metrics`/`set`. `span_id`/`flags` are disabled with `null`, never
    // `""` -- an empty string there is a typo, not an opt-out.
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

    // Rule 20: `scale`-specific validation -- an empty `fields` map can only ever be a no-op, the
    // same reasoning rules 10-12/19 already apply to `kv_metrics`/`set`/`trace_context`; an empty
    // field name could never match a real attribute for the same reason rule 19 rejects one on
    // `trace_context`; a non-finite factor would only ever produce values `numeric` then rejects
    // downstream (`crates/logit-transforms/src/lib.rs::numeric`), which is a confusing way to
    // learn about what's almost certainly a config typo.
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

    // Rule 21: `has_signal`/`keep_signals`/`drop_signals` need a non-empty `signals:`. For
    // `keep_signals`/`drop_signals`, an empty or all-three list is *always* rejected, but which
    // of the two is the silent-black-hole shape (rule 7's "no consumer" failure, here recast as
    // "no event ever gets through") and which is the no-op (every event forwarded completely
    // untouched, so the component is pointless) is *opposite* between the two kinds -- an
    // allowlist that names nothing keeps nothing (black hole), one that names everything keeps
    // everything (no-op); a denylist is the mirror. Both shapes are rejected either way (a no-op
    // component is exactly as much a config mistake as a black hole one), but the message must
    // say which is which, or it tells the operator the wrong thing happened.
    // `has_signal` naming all three signals is left alone: under `mode: only` that's a real,
    // if permissive, "forward anything with a payload" filter, not a no-op.
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

    // Rule 22: an `otlp_out` `headers:` entry may not name a header the protocol itself sets
    // (see `RESERVED_OTLP_HEADERS`'s own doc comment for why `grpc-*` is a prefix check here,
    // not a fixed list), and no two entries may name the same header once case is ignored --
    // HTTP header names are case-insensitive, so e.g. `X-Scope-OrgID` and `x-scope-orgid` in the
    // same `headers:` block would silently collide into one `HeaderMap` entry
    // (`OtlpOutput::with_headers`) with no way to predict which value wins.
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

    // Rule 23: `otlp_out`'s `paths:` is HTTP-only -- gRPC method names are fixed by the `.proto`
    // service definitions, not a mount point an operator can move, so a non-empty `paths:` under
    // `protocol: grpc` is rejected rather than silently ignored (the same instinct as rule 14's
    // `buffer:` on a non-sink, and rule 17's `receive:` on a non-datagram listener).
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

    // Rule 24: `otlp_out`'s `tls:` block. `cert_file`/`key_file` must be set together -- a lone
    // one is almost certainly a typo, not a deliberate half-configured mTLS. `insecure_skip_verify`
    // together with `ca_file` is contradictory -- "trust this CA" and "trust nothing, verify
    // nothing" can't both be meant. And a non-empty `tls:` under a plain `http://`/`grpc://`
    // endpoint is rejected outright, the same instinct as rule 14's `buffer:` on a non-sink and
    // rule 23's `paths:` under `protocol: grpc` -- TLS is selected by the endpoint's scheme
    // (`docs/adr/otlp-tls-and-pooled-grpc-client.md`), so a `tls:` block with nothing to tune
    // would otherwise be silently ignored rather than caught as a likely mistake.
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

    // Rule 25: `trace_context`'s `span:` block (`docs/adr/trace-context-span-lifting.md`). An
    // empty default `name` would mint spans OTLP requires a name for, and a `max_skew` of zero
    // rejects every span as skewed -- an impossible window, the same instinct as rule 9's
    // zero-length `interval` and rule 15's zero-sized buffer.
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

    // Rule 29: `file_out`'s `rotate:` block. Neither trigger set would silently never rotate at
    // all -- the same "would silently do nothing" reasoning rule 7/27 already apply, just not
    // derivable from arity alone here; `stdio_out` already covers the never-rotate case on
    // purpose, so this rejects rather than treats it as a quiet no-op. `max_bytes: 0`/
    // `max_files: 0` are each an impossible bound, the same "0 is impossible, not just small"
    // instinct as rule 9/15/18/28.
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
        }
    }

    // Rule 30: `kv`'s separators. An empty `pair_sep` or `kv_sep` makes splitting yield a
    // boundary between every character; `pair_sep == kv_sep` means every segment is split away
    // from its own separator, so no line could ever produce a pair; and a `kv_sep` that
    // *contains* `pair_sep` can never appear intact inside a segment, since the `pair_sep` split
    // always runs first -- each shape is a certain no-op or a certain garbage result, catchable
    // here rather than surfacing as silently-wrong output at runtime.
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

    // Rule 31: `regex`-specific validation -- an empty `field` name could never match a real
    // attribute for the same reason rule 19 rejects one on `trace_context`; a pattern that
    // doesn't compile, or declares no named capture group, can only ever be a no-op (or worse, a
    // run-time surprise) if left for `build_spec` to discover.
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

    // Rule 32: a `csv`'s `columns`/`delimiter` shape (`docs/adr/csv-positional-columns.md`). An
    // empty `columns` list can only ever be a no-op, the same reasoning rules 10-12/19/20 already
    // apply elsewhere; an empty column name could never be a useful attribute name, the same
    // reasoning as rule 20's empty scale field name; a duplicate column name would let the later
    // field silently overwrite the earlier one on every event, leaving one configured column
    // permanently unreachable -- the "a repeated entry silently doubles rather than erroring" rule
    // applied to columns instead of sources (rule 4). `delimiter` must be a single ASCII
    // character, and not `"` (RFC 4180's quote character, which this parser reads as field
    // framing, not data) or `\n`/`\r` (already consumed as line framing by every input).
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

    // Rule 33: `stdio_out`/`file_out`'s `compression:` only means anything under `format:
    // native` -- under the default `format: human`, a non-`none` value would silently do
    // nothing, the same reasoning rule 29 already applies to `rotate:`'s own triggers
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

    // Rule 34: `logit_out`'s `tls:` block must be internally consistent -- `cert_file`/`key_file`
    // together, `insecure_skip_verify` and `ca_file` contradictory -- mirroring rule 24's first
    // two checks. No scheme-based check the way rule 24's third one has: `logit_out`'s `endpoint`
    // is a bare `host:port` (the `syslog_out` shape), so `tls:`'s mere presence is the only signal
    // available, and it always turns TLS on -- there's no "wrong scheme" case to catch. And
    // `logit_in`'s `max_frame_bytes`, when set, must be a real, sane bound: `0` could never accept
    // a single frame (the same "0 is impossible, not just small" instinct as rules 9/15/18/28),
    // and anything over `logit_proto::frame::MAX_SANE_UNCOMPRESSED_LEN` (64 MiB) exceeds what
    // `read_frame`/`read_frame_with_header` themselves ever accept regardless of what a listener
    // configures.
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

    // Rule 35: `buffer.disk:`'s shape (`docs/adr/disk-backed-sink-buffer.md`). Disk *replaces*
    // memory for that sink, not a tier sized alongside it, so `max_batches`/`max_bytes` staying
    // at their defaults while `disk:` is set would silently ignore whichever one an operator
    // actually meant to tune -- the same "a knob that would silently do nothing is a config
    // error" reasoning as rule 33. `segment_bytes`/`max_bytes` of `0` are impossible bounds, the
    // same instinct as rule 15's `buffer.max_batches: 0`. Two sinks sharing a literal `disk.path`
    // would corrupt each other's spool; `DiskQueue::open`'s own exclusive lock also catches an
    // *aliased* path (`./spool` vs `spool`) this literal-string check can't see, since this
    // function never resolves a path against the config's base directory.
    // `(path, id)`, not `(id, path)` -- sorted so two entries sharing a path become adjacent
    // regardless of which component id happens to sort first.
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

    // Rule 36: `has_attributes`/`drop_attributes`-specific validation
    // (`docs/adr/attribute-filtering-components.md`). Neither map configured is rejected on both
    // kinds, the same "can only ever be a no-op" instinct as rule 12 -- but note the black-hole/
    // no-op assignment is *inverted* from rule 21: there the allowlist (`keep_signals`) is the
    // black hole and the denylist the no-op, because `signals:` is a list of alternatives.
    // `resource:`/`attributes:` is a map of conjunctions instead, so a conjunction over zero pairs
    // is vacuously true -- `has_attributes` with nothing configured matches *every* event (a
    // no-op, forwarding everything untouched) and `drop_attributes` with nothing configured is
    // therefore its exact complement, matching every event too, but that means dropping every one
    // of them (a black hole). An empty key could never name a real attribute (rule 12/19/20's
    // reasoning). A non-finite value can never compare equal to anything under
    // `crate::attributes`' coercing matcher, so an entry holding one could never match -- the same
    // "would only ever produce a value `numeric` then rejects" reasoning rule 20 applies to
    // `scale`'s factors. The same key appearing in both `resource:` and `attributes:` is
    // deliberately *not* rejected -- they address different objects (the batch vs. the event), so
    // that config is meaningful, not a mistake.
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

    // Rule 37: `has_provenance`/`drop_provenance`-specific validation
    // (`docs/adr/provenance-filtering-components.md`). Same "can only ever be a no-op" instinct
    // as rule 36, and the empty-config black-hole/no-op assignment lines up with rule 36's, not
    // rule 21's -- despite each field's *contents* being a list of alternatives, the same shape
    // `signals:` has. The difference is what "empty" means at the *field*, not the list: an empty
    // `origin:`/`previous:` means "this field isn't part of the match" (vacuously true, so it
    // never narrows what matches), exactly like `has_attributes`' empty `resource:`/`attributes:`
    // map -- not "match against zero alternatives" (vacuously false), which is what makes
    // `has_signal`'s family the inverted case. Two independently-omittable AND'd fields, each an
    // OR internally, is `has_attributes`' top-level shape with `has_signal`'s per-field shape
    // nested inside it -- and it's the *top* level that decides this assignment. So: both fields
    // empty means `has_provenance` matches every batch (a no-op, forwarding everything untouched)
    // and `drop_provenance`, its exact complement, therefore drops every one of them (a black
    // hole). An empty string entry could never name a real component id (rule 12/19/20/36's
    // reasoning); a duplicate entry within one list is almost certainly a copy-paste typo, the
    // same instinct rule 4 already applies to a repeated `sources` entry.
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

    // Rule 38: `statsd_out`'s `max_packet_bytes: 0` is rejected the same way rule 15's
    // `max_batches`/`max_bytes: 0` is -- an impossible bound (every line would overflow it and be
    // dropped whole), not a small one.
    for (id, component) in &components {
        if let ComponentKind::StatsdOut { max_packet_bytes: 0, .. } = &component.kind {
            anyhow::bail!(
                "component '{id}': max_packet_bytes: 0 would drop every metric line -- use a \
                 positive byte size"
            );
        }
    }

    // Rule 40: `prometheus_in`'s `targets`/`timeout`/`tls`/`headers` -- see this module's own doc
    // comment for the full rule text.
    for (id, component) in &components {
        if let ComponentKind::PrometheusIn { targets, timeout, headers, tls, .. } = &component.kind
        {
            if targets.is_empty() {
                anyhow::bail!(
                    "component '{id}': 'targets' must name at least one scrape URL -- an empty \
                     list would never scrape anything"
                );
            }
            for target in targets {
                if !is_absolute_http_url(target) {
                    anyhow::bail!(
                        "component '{id}': 'targets' entry {target:?} isn't an absolute \
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
            let any_https = targets.iter().any(|t| t.to_ascii_lowercase().starts_with("https://"));
            if !tls.is_empty() && !any_https {
                anyhow::bail!(
                    "component '{id}': 'tls' is set, but no 'targets' entry is 'https://' -- \
                     TLS is selected per-target by its own scheme, so a 'tls:' block here would \
                     have no effect"
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

    let mut resolved = HashMap::with_capacity(components.len());
    for (id, component) in components {
        let Component { sources, buffer, receive, kind } = component;
        let node_consumers = consumers.remove(&id).unwrap_or_default();
        resolved.insert(
            id,
            ResolvedComponent { sources, consumers: node_consumers, kind, buffer, receive },
        );
    }

    Ok(Graph { components: resolved, topological_order })
}

/// The predicate rule 17 needs: which `ComponentKind`s the UDP listener driver
/// (`docs/adr/decoupled-listener-io.md`, `logit-inputs::udp::UdpListener`) actually backs.
/// Kept explicit rather than derived from [`Role`] -- see rule 17's own doc comment -- so a new
/// listener kind rejects `receive:` until it is actually wired to that driver.
fn is_datagram_listener(kind: &ComponentKind) -> bool {
    matches!(kind, ComponentKind::StatsdIn { .. } | ComponentKind::SyslogIn { .. })
}

/// The predicate rules 17/18/28 need: which `ComponentKind`s the file-tailing driver
/// (`docs/adr/file-tailing-and-docker-json-logs.md`, `logit_inputs::tail::Tailer`) backs. A tail
/// listener has no receive *queue* at all -- the tailed file is its own durable buffer, so only
/// `receive`'s batch-assembly and shutdown-grace fields apply to it, never the queue-bounding
/// ones a datagram listener's socket needs (`max_datagrams`, `max_bytes`, `overflow`,
/// `receive_buffer_bytes`). Kept explicit, alongside [`is_datagram_listener`], rather than
/// derived from [`Role`] -- the same reasoning: a future listener kind rejects `receive:` until
/// it is actually wired to one of these two drivers.
///
fn is_tail_listener(kind: &ComponentKind) -> bool {
    matches!(kind, ComponentKind::TailIn { .. } | ComponentKind::DockerIn { .. })
}

/// Rule 26: rejects a `*` anywhere in `path` except its final `/`-separated component --
/// `logit_inputs::tail::pattern::PathPattern`'s matcher only ever treats the last component as a
/// pattern, so a wildcard earlier (`/var/*/app.log`) would silently never match anything rather
/// than doing what its author probably meant.
fn check_tail_glob(id: &str, path: &str) -> anyhow::Result<()> {
    let Some((parent, _last)) = path.rsplit_once('/') else {
        // No `/` at all isn't a path this driver can use either way, but that's not this rule's
        // job to say -- an absolute-path requirement is a deployment convention this driver
        // trusts the operator on, not something graph validation enforces.
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

/// Kahn's algorithm over the `sources` edges (a source's data flows *into* the component that
/// names it, so indegree is `sources.len()`). Returns a listener-first order, or a cycle error
/// naming one concrete cycle recovered from the components still unresolved once no more
/// zero-indegree nodes remain -- that residual set is the cycle *and* everything downstream of
/// it (a node fed by a cycle never reaches indegree 0 either), so it is walked back down to a
/// single cycle rather than reported as-is.
fn topological_order(components: &HashMap<String, Component>) -> anyhow::Result<Vec<String>> {
    let mut indegree: HashMap<&str, usize> =
        components.iter().map(|(id, c)| (id.as_str(), c.sources.len())).collect();
    let mut outgoing: HashMap<&str, Vec<&str>> =
        components.keys().map(|id| (id.as_str(), Vec::new())).collect();
    for (id, c) in components {
        for source in &c.sources {
            if let Some(out) = outgoing.get_mut(source.as_str()) {
                out.push(id.as_str());
            }
        }
    }

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
        // Residual indegree marks the cycle *and* everything downstream of it -- a node fed by a
        // cycle never reaches indegree 0 either. Naming that whole set would blame components
        // that are merely downstream victims, so walk `sources` backwards inside it to recover
        // one real cycle instead. Every stuck node has a stuck source (that's what non-zero
        // residual indegree means), so the walk can't dead-end, and it must revisit a node within
        // `stuck.len()` steps.
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
            current = components[current]
                .sources
                .iter()
                .map(String::as_str)
                .filter(|s| stuck.contains(s))
                .min()
                .expect("a stuck node always has a stuck source");
        };
        // `path` was built walking against the flow (consumer -> source); reverse the cycle
        // portion so the message reads as data flow. The discarded prefix `path[..start]` is the
        // tail that led into the cycle, not part of it.
        let mut cycle: Vec<&str> = path[start..].to_vec();
        cycle.reverse();
        // Rotate so the lexicographically smallest id leads -- a cosmetic step only (any
        // rotation names the same cycle), but it keeps the message independent of which stuck
        // node the backward walk happened to start from.
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
                    buffer,
                    receive: ReceiveConfig::default(),
                    kind,
                },
            );
        }
        Config { components: map, ..Default::default() }
    }

    /// Same as [`cfg`], but with an explicit `receive` on one component -- for rules 16/17's
    /// tests.
    fn cfg_with_receive(
        components: Vec<(&str, Vec<&str>, ComponentKind, ReceiveConfig)>,
    ) -> Config {
        let mut map = Map::new();
        for (id, sources, kind, receive) in components {
            map.insert(
                id.to_string(),
                Component {
                    sources: sources.into_iter().map(String::from).collect(),
                    buffer: BufferConfig::default(),
                    receive,
                    kind,
                },
            );
        }
        Config { components: map, ..Default::default() }
    }

    fn listener() -> ComponentKind {
        ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() }
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
        ComponentKind::Json { skip_to_brace: false }
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
        }
    }

    fn prometheus_in(targets: Vec<&str>) -> ComponentKind {
        ComponentKind::PrometheusIn {
            targets: targets.into_iter().map(String::from).collect(),
            interval: Duration::from_secs(15),
            timeout: Duration::from_secs(10),
            headers: Map::new(),
            tls: logit_config::TlsClientConfig::default(),
        }
    }

    fn prometheus_in_with_headers(targets: Vec<&str>, headers: Vec<(&str, &str)>) -> ComponentKind {
        ComponentKind::PrometheusIn {
            targets: targets.into_iter().map(String::from).collect(),
            interval: Duration::from_secs(15),
            timeout: Duration::from_secs(10),
            headers: headers.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            tls: logit_config::TlsClientConfig::default(),
        }
    }

    fn prometheus_in_with_tls(
        targets: Vec<&str>,
        tls: logit_config::TlsClientConfig,
    ) -> ComponentKind {
        ComponentKind::PrometheusIn {
            targets: targets.into_iter().map(String::from).collect(),
            interval: Duration::from_secs(15),
            timeout: Duration::from_secs(10),
            headers: Map::new(),
            tls,
        }
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

    /// `Graph` isn't `Debug` (it embeds `ComponentKind`, which isn't either), so
    /// `Result::expect_err` -- which needs `Debug` on the `Ok` side to format its panic message --
    /// doesn't work here. Same reason `logit-cli::pipeline` has its own `expect_err` helper.
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

    /// A repeated source id would otherwise push the same consumer into `consumers` twice, giving
    /// that source's `Fanout` two live `Sender` clones pointing at the same inbox -- silently
    /// doubling every batch delivered, not a cosmetic issue.
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

    /// The regression this exists for: residual indegree marks the cycle *and* everything
    /// downstream of it, since a node fed by a cycle never reaches indegree 0 either. The message
    /// must name only the cycle, not `out`, which is merely a downstream victim.
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
                    gauge_retention: 5,
                    max_retained_gauge_series: 10_000,
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

    /// The headline regression test: today's `validate_semantics` rejects an input/output
    /// referenced by more than one pipeline outright. A sink with two independent upstream
    /// branches must now be *accepted*.
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
        // resource: and attributes: address different objects, so a shared key name is
        // meaningful configuration, not a mistake -- deliberately not rejected.
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
        // Regression guard: RESERVED_OTLP_HEADERS deliberately no longer lists every gRPC header
        // name individually -- any `grpc-*` header is reserved by prefix, not by exact match, so
        // a header this project doesn't itself set (e.g. `grpc-trace-bin`, part of the gRPC wire
        // protocol but never used by `crates/logit-outputs/src/otlp.rs`) is still rejected.
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
                    gauge_retention: 5,
                    max_retained_gauge_series: 10_000,
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
        // An explicitly-written but all-default `buffer: {}` on a non-sink is indistinguishable
        // from an omitted block -- rule 14 only rejects a genuinely *non-default* value.
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
                    gauge_retention: 5,
                    max_retained_gauge_series: 10_000,
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
            err.contains("'receive' is only meaningful on a datagram or tail listener"),
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
            err.contains("'receive' is only meaningful on a datagram or tail listener"),
            "got: {err}"
        );
    }

    /// The reason rule 17 checks a dedicated predicate rather than `role() == Role::Listener`:
    /// `internal` is a listener by role but has no socket, no queue, and no decoder, so a
    /// `receive:` block on it must be rejected just as clearly as on a sink or a transform.
    #[test]
    fn a_non_default_receive_on_internal_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            ("in", vec![], listener(), ReceiveConfig::default()),
            ("self", vec![], internal(), non_default_receive()),
            ("out", vec!["in", "self"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'self'"), "got: {err}");
        assert!(
            err.contains("'receive' is only meaningful on a datagram or tail listener"),
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
        // An explicitly-written but all-default `receive: {}` is indistinguishable from an
        // omitted block -- rule 17 only rejects a genuinely *non-default* value.
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
        ComponentKind::LogitIn { bind: "0.0.0.0:5140".to_string(), tls: None, max_frame_bytes }
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

    /// Rule 38: `max_packet_bytes: 0` would drop every metric line -- an impossible bound, not a
    /// small one, the same shape as rule 15's `buffer.max_batches`/`max_bytes: 0`.
    #[test]
    fn a_zero_max_packet_bytes_is_rejected() {
        let err =
            expect_err(cfg(vec![("in", vec![], listener()), ("out", vec!["in"], statsd_out(0))]));
        assert!(err.contains("'out'") && err.contains("max_packet_bytes: 0"), "got: {err}");
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

    #[test]
    fn a_prometheus_in_with_empty_targets_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], prometheus_in(vec![])),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("'in'") && err.contains("'targets'"), "got: {err}");
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
        assert!(err.contains("no 'targets' entry is 'https://'"), "got: {err}");
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

    /// `receive:` stays rejected on `prometheus_in` via rule 17's explicit allowlist -- it's a
    /// listener by role, but not one of the two drivers (`is_datagram_listener`/
    /// `is_tail_listener`) rule 17 actually wires `receive:` to, so a non-default block on it is
    /// caught the same way `internal`'s own is.
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
            err.contains("'receive' is only meaningful on a datagram or tail listener"),
            "got: {err}"
        );
    }
}
