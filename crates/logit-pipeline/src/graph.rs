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
//!     (today `statsd_in`/`syslog_in`) -- `receive:` (`docs/adr/decoupled-listener-io.md`)
//!     configures a listener's socket-side receive queue, which only a datagram listener has.
//!     Deliberately **not** `role(&kind) != Role::Listener`: `internal` is a listener by role but
//!     has no socket, no queue, and no decoder, so `receive:` on it would be a silently-ignored
//!     setting -- exactly what this rule exists to catch on the sink side (rule 14). A future
//!     listener kind rejects `receive:` until it is actually wired to the UDP driver.
//! 18. A datagram listener's `receive.max_datagrams`, `receive.max_bytes`,
//!     `receive.batch_max_events`, or `receive.batch_max_bytes` of `0` is rejected -- an
//!     impossible bound, the twin of rule 15.
//!     `receive.batch_flush_interval: 0s` is **not** rejected: it means "no flush timer," a
//!     meaningful setting, unlike the count bounds.
//! 19. An empty `signals:` list on `has_signal`, `keep_signals`, or `drop_signals` is rejected.
//!     `keep_signals`/`drop_signals` additionally reject naming all three signals. Which of the
//!     two shapes is the silent black hole (rule 7's "no consumer" failure, recast here as "no
//!     event ever gets through") and which is the no-op (every event forwarded untouched) is
//!     *opposite* between the two kinds -- an allowlist naming nothing keeps nothing (black
//!     hole), naming everything keeps everything (no-op); a denylist is the mirror. Both shapes
//!     are rejected either way, but the error message names the right one. `keep`'s empty
//!     `fields` list stays legal by contrast -- "drop every attribute" is a real operation,
//!     "drop every event" is not. See `docs/adr/signal-filtering-components.md`.
//! 20. An `otlp_out` `headers:` entry naming an empty string, an HTTP/2 pseudo-header (starting
//!     with `:`), or a header the wire transport itself sets (`content-type`, `grpc-encoding`,
//!     etc. -- see `RESERVED_OTLP_HEADERS`) is rejected, case-insensitively -- almost certainly a
//!     config mistake, not a meaningful override.
//!
//! Sink reachability from a listener needs no separate rule -- it's implied by 2 + 5 + 7: every
//! acyclic chain of sourced components terminates somewhere, and every non-terminal component in
//! it is required (by 7) to have a consumer, so the chain can only terminate at a sink.

use logit_config::{BufferConfig, Component, ComponentKind, Config, ReceiveConfig};
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
        | FileTail { .. }
        | LogitIn { .. }
        | Internal { .. } => Role::Listener,
        Lua { .. }
        | LuaFile { .. }
        | Aggregate { .. }
        | Json { .. }
        | KvMetrics { .. }
        | Keep { .. }
        | Remove { .. }
        | HasSignal { .. }
        | KeepSignals { .. }
        | DropSignals { .. }
        | Logfmt
        | Kv
        | Regex { .. }
        | Csv
        | Rename { .. }
        | Filter { .. }
        | Sample { .. }
        | Throttle { .. }
        | Dedup { .. } => Role::Transform,
        InfluxDbOut { .. }
        | OtlpOut { .. }
        | LogitOut { .. }
        | StdioOut { .. }
        | SyslogOut { .. } => Role::Sink,
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
        FileTail { .. } => "file_tail",
        LogitIn { .. } => "logit_in",
        Internal { .. } => "internal",
        Lua { .. } => "lua",
        LuaFile { .. } => "lua_file",
        Aggregate { .. } => "aggregate",
        Json { .. } => "json",
        KvMetrics { .. } => "kv_metrics",
        Keep { .. } => "keep",
        Remove { .. } => "remove",
        HasSignal { .. } => "has_signal",
        KeepSignals { .. } => "keep_signals",
        DropSignals { .. } => "drop_signals",
        Logfmt => "logfmt",
        Kv => "kv",
        Regex { .. } => "regex",
        Csv => "csv",
        Rename { .. } => "rename",
        Filter { .. } => "filter",
        Sample { .. } => "sample",
        Throttle { .. } => "throttle",
        Dedup { .. } => "dedup",
        InfluxDbOut { .. } => "influxdb_out",
        OtlpOut { .. } => "otlp_out",
        LogitOut { .. } => "logit_out",
        StdioOut { .. } => "stdio_out",
        SyslogOut { .. } => "syslog_out",
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
            | ComponentKind::Internal { .. }
            | ComponentKind::Lua { .. }
            | ComponentKind::LuaFile { .. }
            | ComponentKind::Aggregate { .. }
            | ComponentKind::Json { .. }
            | ComponentKind::KvMetrics { .. }
            | ComponentKind::Keep { .. }
            | ComponentKind::Remove { .. }
            | ComponentKind::HasSignal { .. }
            | ComponentKind::KeepSignals { .. }
            | ComponentKind::DropSignals { .. }
            | ComponentKind::InfluxDbOut { .. }
            | ComponentKind::OtlpOut { .. }
            | ComponentKind::StdioOut { .. }
            | ComponentKind::SyslogOut { .. }
    )
}

/// `Some(interval)` for a kind with an `interval` field, `Aggregate`'s always populated,
/// `Lua`/`LuaFile`'s only when set. `None` either means no `interval` field on this kind, or a
/// `Lua`/`LuaFile` component that left it unset -- both are "never flushes", so rule 9 treats
/// them the same: nothing to reject.
fn interval(kind: &ComponentKind) -> Option<Duration> {
    match kind {
        ComponentKind::Lua { interval, .. } | ComponentKind::LuaFile { interval, .. } => *interval,
        ComponentKind::Aggregate { interval, .. } | ComponentKind::Internal { interval, .. } => {
            Some(*interval)
        }
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

/// Header names `otlp_out`'s two wire transports set themselves -- `content-type` and the
/// hand-rolled gRPC framing's own `grpc-*`/`te`/`connection`/etc. headers
/// (`crates/logit-outputs/src/otlp.rs`'s `send_http`/`grpc_roundtrip`). A config `headers:` entry
/// naming one of these is almost certainly a config mistake, not a meaningful override -- checked
/// case-insensitively, matching HTTP's own header-name semantics. `OtlpOutput::with_headers`
/// guards the same thing in code, as defense in depth; this is what gives an operator a clear,
/// component-scoped error at config-load time instead.
const RESERVED_OTLP_HEADERS: &[&str] = &[
    "content-type",
    "content-length",
    "content-encoding",
    "host",
    "te",
    "transfer-encoding",
    "connection",
    "grpc-encoding",
    "grpc-accept-encoding",
    "grpc-timeout",
    "grpc-status",
    "grpc-message",
];

pub fn resolve(config: Config) -> anyhow::Result<Graph> {
    let Config { components } = config;

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

    // Rule 17: `receive:` is a datagram-listener-only concept -- see this module's own doc
    // comment on why this checks a dedicated predicate rather than `role() == Role::Listener`
    // (which would wrongly also permit `internal`).
    for (id, component) in &components {
        if component.receive != ReceiveConfig::default() && !is_datagram_listener(&component.kind) {
            anyhow::bail!(
                "component '{id}': 'receive' is only meaningful on a datagram listener \
                 (statsd_in, syslog_in), but '{id}' is a {}",
                role(&component.kind).as_str()
            );
        }
    }

    // Rule 18: the twin of rule 15, for a listener's receive queue -- `0` on any of the four
    // count/byte bounds is an impossible bound, never a small one. `batch_flush_interval: 0s` is
    // deliberately not checked here: zero there means "no timer," a meaningful setting.
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

    // Rule 19: `has_signal`/`keep_signals`/`drop_signals` need a non-empty `signals:`. For
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

    // Rule 20: an `otlp_out` `headers:` entry may not name a header the protocol itself sets --
    // see `RESERVED_OTLP_HEADERS`'s own doc comment.
    for (id, component) in &components {
        if let ComponentKind::OtlpOut { headers, .. } = &component.kind {
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
                if RESERVED_OTLP_HEADERS.contains(&name.to_ascii_lowercase().as_str()) {
                    anyhow::bail!(
                        "component '{id}': 'headers' names {name:?}, which this protocol sets \
                         itself -- it can't be overridden"
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
        Config { components: map }
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
        Config { components: map }
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
        Config { components: map }
    }

    fn listener() -> ComponentKind {
        ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() }
    }

    fn lua() -> ComponentKind {
        ComponentKind::Lua { script: "".to_string(), interval: None }
    }

    fn json() -> ComponentKind {
        ComponentKind::Json { skip_to_brace: false }
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
    fn unimplemented_kind_is_rejected() {
        let err = expect_err(cfg(vec![
            ("in", vec![], ComponentKind::LogitIn { bind: "127.0.0.1:0".to_string() }),
            ("out", vec!["in"], sink()),
        ]));
        assert!(err.contains("not implemented yet"), "got: {err}");
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

    fn otlp_out_with_headers(headers: Vec<(&str, &str)>) -> ComponentKind {
        ComponentKind::OtlpOut {
            endpoint: "http://localhost:4318".to_string(),
            protocol: logit_config::OtlpProtocol::Http,
            headers: headers.into_iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
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
        ComponentKind::Internal { interval: Duration::from_secs(10), span_sample_rate }
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
                },
                non_default_receive(),
            ),
            ("out", vec!["agg"], sink(), ReceiveConfig::default()),
        ]));
        assert!(err.contains("'agg'"), "got: {err}");
        assert!(err.contains("'receive' is only meaningful on a datagram listener"), "got: {err}");
    }

    #[test]
    fn a_non_default_receive_on_a_sink_is_rejected() {
        let err = expect_err(cfg_with_receive(vec![
            ("in", vec![], listener(), ReceiveConfig::default()),
            ("out", vec!["in"], sink(), non_default_receive()),
        ]));
        assert!(err.contains("'out'"), "got: {err}");
        assert!(err.contains("'receive' is only meaningful on a datagram listener"), "got: {err}");
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
        assert!(err.contains("'receive' is only meaningful on a datagram listener"), "got: {err}");
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
}
