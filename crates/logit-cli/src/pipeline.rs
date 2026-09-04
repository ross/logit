//! `logit run`: resolves a config's component graph (`docs/design/pipeline-graph.md`,
//! `docs/adr/component-graph-configuration.md`) into a runnable [`NodeSpec`] per component
//! and hands it to `logit_pipeline::run`. See `docs/OVERVIEW.md` for the shape (`logit` as
//! sidecar, host agent, or central aggregator is all just this, differing only by config).
//!
//! This module is now just the *registry* -- graph resolution/validation
//! (`logit_pipeline::graph`) and the node runtime (`logit_pipeline::run`) both live in
//! `logit-pipeline`; what's left here is turning one component's `ComponentKind` into the boxed
//! implementation the runtime actually runs, which is exactly the "kind → impl" mapping this
//! project has always kept in one place (previously `build_input`/`build_output`).

use crate::config;
use anyhow::Context;
use logit_config::{BufferConfig, Config, StdioTarget};
use logit_core::{Diagnostics, Registry, Telemetry};
use logit_inputs::internal::InternalInput;
use logit_inputs::otlp::{OtlpInput, OtlpTransport as OtlpInTransport};
use logit_inputs::statsd::StatsdInput;
use logit_inputs::syslog::SyslogInput;
use logit_outputs::influxdb::InfluxDbOutput;
use logit_outputs::otlp::{
    OtlpCompression as OtlpOutCompression, OtlpOutput, OtlpTransport as OtlpOutTransport,
    SignalPaths,
};
use logit_outputs::stdio::StdioOutput;
use logit_outputs::syslog::{SyslogEncoder, SyslogOutput};
use logit_pipeline::graph::{self, ResolvedComponent};
use logit_pipeline::{InputRuntimeConfig, NodeSpec, RetryConfig, SinkQueueConfig, WriteLoopConfig};
use logit_transforms::{
    Aggregator, DropSignals as DropSignalsTransform, HasSignal as HasSignalTransform, JsonParser,
    Keep as KeepTransform, KeepSignals as KeepSignalsTransform, KvMetrics as KvMetricsTransform,
    MatchMode as TransformMatchMode, Remove as RemoveTransform, Set as SetTransform, SignalSet,
    TraceContext as TraceContextTransform,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Loads `path`, resolves its component graph, and runs it until the first component fails or a
/// shutdown signal is received.
///
/// Every listener/sink task loops forever in normal operation (a listener keeps listening, a
/// sink keeps draining its inbox), so in the happy path this simply never returns -- matching
/// "this is a service," not "this is a batch job." SIGTERM/SIGINT (Ctrl-C on non-Unix) triggers a
/// graceful drain: every listener stops, which closes its downstream inboxes normally and
/// triggers each node's existing close-time flush (`logit_pipeline::run_with_shutdown`,
/// `crates/logit-pipeline/src/runtime.rs`) -- so an in-flight `aggregate` window is emitted rather
/// than lost. A second signal before that drain finishes exits immediately (exit code 130): a
/// wedged drain must stay killable by the same signal that started it, which matters once an
/// unattended restart policy is the thing waiting on this process to actually exit.
pub async fn run_pipelines(path: PathBuf) -> anyhow::Result<()> {
    // An unset `!env` variable (a missing token, most likely) fails here, before anything starts
    // listening.
    let config = config::load(&path)?;
    let base_dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
    let (graph, specs, telemetry) = prepare(config, base_dir)?;

    // Independent listener from the one `run_with_shutdown` races internally (below) -- multiple
    // concurrent listeners on the same signal kind are supported and all get notified, so this
    // doesn't compete with or consume the first one. Aborted once `run_with_shutdown` returns so
    // it doesn't linger if shutdown never happens.
    let kill_switch = tokio::spawn(async {
        shutdown_signal().await;
        shutdown_signal().await;
        std::process::exit(130);
    });

    let result =
        logit_pipeline::run_with_telemetry(graph, specs, telemetry, shutdown_signal()).await;
    kill_switch.abort();
    result
}

/// A resolved `Graph` plus one built `NodeSpec` and one [`Telemetry`] handle per component --
/// [`prepare`]'s return type, factored out purely to keep clippy's `type_complexity` lint happy.
type Prepared = (graph::Graph, HashMap<String, NodeSpec>, HashMap<String, Telemetry>);

/// Resolves a config into a `Graph`, one built `NodeSpec` per component, and one [`Telemetry`]
/// handle per component -- the shared setup between [`run_pipelines`] and [`run_config`] (the
/// latter used directly by tests below, which don't need shutdown wiring).
///
/// The telemetry map is empty (every handle [`Telemetry::default`], the disabled no-op) unless
/// `config` contains an `internal` component, in which case a single process-wide [`Registry`] is
/// built and shared by every component -- one live handle per component id, reused for both its
/// own instrumentation (`build_spec`, layer 3) and the node runtime's uniform instrumentation
/// (`logit_pipeline::run_with_telemetry`, layer 2), so both land in the same buffer and drain
/// together. See `docs/design/internal-telemetry.md`.
fn prepare(config: Config, base_dir: PathBuf) -> anyhow::Result<Prepared> {
    let graph = graph::resolve(config)?;

    // The rate comes off the config's own `internal` component (graph rule 13 already
    // guarantees at most one), rather than always calling `Registry::new`'s default -- an operator
    // who set `span_sample_rate` explicitly (`demo/logit.yaml`'s `1.0`, say) would otherwise have
    // their choice silently ignored.
    let internal_span_sample_rate = graph.components.values().find_map(|c| match &c.kind {
        logit_config::ComponentKind::Internal { span_sample_rate, .. } => Some(*span_sample_rate),
        _ => None,
    });
    let registry: Option<Arc<Registry>> =
        internal_span_sample_rate.map(Registry::with_span_sampling);

    // Sorted, not raw `HashMap` iteration order: a startup failure (a missing lua_file) should be
    // reproducible across runs, not depend on hash-seed-driven iteration order -- two
    // independently-broken components should always report the same one first. Also what makes
    // `Registry::drain`'s output order reproducible, incidentally -- components register with the
    // registry in this same order.
    let mut ids: Vec<&String> = graph.components.keys().collect();
    ids.sort();

    let mut specs: HashMap<String, NodeSpec> = HashMap::with_capacity(graph.components.len());
    let mut telemetry: HashMap<String, Telemetry> = HashMap::with_capacity(graph.components.len());
    for id in ids {
        let component = &graph.components[id];
        let (spec, component_telemetry) = build_spec(id, component, &base_dir, registry.as_ref())
            .with_context(|| format!("component '{id}'"))?;
        specs.insert(id.clone(), spec);
        telemetry.insert(id.clone(), component_telemetry);
    }

    Ok((graph, specs, telemetry))
}

/// Waits for one SIGTERM or SIGINT (Ctrl-C on non-Unix, where `SignalKind` doesn't exist). Each
/// call installs its own independent listener -- see `run_pipelines`, which calls this three
/// times (the graceful-shutdown trigger, plus twice more for the kill switch) and relies on all
/// three being notified independently on the same signal.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut terminate = signal(SignalKind::terminate()).expect("installing a SIGTERM handler");
        let mut interrupt = signal(SignalKind::interrupt()).expect("installing a SIGINT handler");
        tokio::select! {
            _ = terminate.recv() => {}
            _ = interrupt.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Test-only: [`run_pipelines`] minus shutdown handling and the on-disk `path`, so tests can drive
/// an in-memory `Config` directly without a signal handler racing their assertions.
#[cfg(test)]
async fn run_config(config: Config, base_dir: PathBuf) -> anyhow::Result<()> {
    let (graph, specs, telemetry) = prepare(config, base_dir)?;
    logit_pipeline::run_with_telemetry(graph, specs, telemetry, std::future::pending()).await
}

/// The same checks `logit run` needs before spawning anything, exposed for `logit validate` to
/// share -- so the two commands can never again disagree about whether a config is acceptable.
/// Takes `Config` by value (graph resolution consumes it) rather than by reference: `logit
/// validate` has no further use for the config afterward either.
pub fn validate_semantics(config: Config) -> anyhow::Result<()> {
    graph::resolve(config)?;
    Ok(())
}

/// Turns one resolved component's kind into the boxed implementation the node runtime actually
/// runs. The single source of truth for which `ComponentKind`s this binary can build --
/// `graph::resolve` already rejected every kind `is_implemented` doesn't recognize (rule 8), so
/// the fallback arm below is unreachable in practice, not a silent gap.
///
/// `id` attaches a [`Diagnostics`] to every component that emits one
/// (`docs/adr/service-lifecycle-and-output-retry.md`) via each kind's own `with_diagnostics`
/// builder -- not a constructor parameter, so none of the ~60 existing tests across these four
/// kinds needed to change.
///
/// `registry` is `Some` only when the config being built contains an `internal` component
/// (`prepare` below) -- every component gets a [`Telemetry`] handle from it either way
/// (`Telemetry::default()`, the disabled no-op, when `registry` is `None`), attached to its own
/// `Diagnostics` (so every existing `warn_throttled` call becomes a metric for free) and, for the
/// two kinds instrumented as a worked example (`statsd_in`, `influxdb_out`), to the component
/// itself. See `docs/design/internal-telemetry.md`.
fn build_spec(
    id: &str,
    component: &ResolvedComponent,
    base_dir: &Path,
    registry: Option<&Arc<Registry>>,
) -> anyhow::Result<(NodeSpec, Telemetry)> {
    use logit_config::ComponentKind::*;
    // Never moved into a match arm below (every arm clones instead) -- kept alive to return
    // alongside `spec`, so `prepare` can hand this exact handle to the node runtime too
    // (`logit_pipeline::run_with_telemetry`), landing layer 2 and layer 3 in the same buffer.
    let telemetry: Telemetry = registry
        .map(|r| r.telemetry_for(id, component.kind_name(), component.role().as_str()))
        .unwrap_or_default();
    let spec = match &component.kind {
        StatsdIn { bind } => NodeSpec::Input(
            Box::new(
                StatsdInput::new(bind.clone())
                    .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                    .with_telemetry(telemetry.clone())
                    .with_receive(receive_config(&component.receive)),
            ),
            input_runtime_config(&component.receive),
        ),
        SyslogIn { bind } => NodeSpec::Input(
            Box::new(
                SyslogInput::new(bind.clone())
                    .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                    .with_telemetry(telemetry.clone())
                    .with_receive(receive_config(&component.receive)),
            ),
            input_runtime_config(&component.receive),
        ),
        OtlpIn { bind, protocol } => NodeSpec::Input(
            Box::new(
                OtlpInput::new(bind.clone(), otlp_in_transport(*protocol))
                    .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                    .with_telemetry(telemetry.clone()),
            ),
            input_runtime_config(&component.receive),
        ),
        // `span_sample_rate` is read by `prepare` (above) to build the `Registry` itself, not
        // here -- by the time `build_spec` runs, the `Registry` this handle points at already has
        // it baked in.
        Internal { interval, span_sample_rate: _ } => {
            let registry = registry
                .cloned()
                .expect("graph::resolve's rule 13 guarantees a Registry whenever an 'internal' component does");
            NodeSpec::Input(
                Box::new(
                    InternalInput::new(*interval, registry)
                        .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                        .with_telemetry(telemetry.clone()),
                ),
                input_runtime_config(&component.receive),
            )
        }

        Lua { script, interval } => NodeSpec::Lua { script: script.clone(), interval: *interval },
        LuaFile { lua_file, interval } => {
            let script_path = base_dir.join(lua_file);
            let script = std::fs::read_to_string(&script_path)
                .with_context(|| format!("reading lua_file {}", script_path.display()))?;
            NodeSpec::Lua { script, interval: *interval }
        }
        Aggregate { interval, gauge_retention, max_retained_gauge_series } => {
            NodeSpec::Transform(Box::new(
                Aggregator::new(*interval)
                    .with_gauge_retention(*gauge_retention, *max_retained_gauge_series)
                    .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                    .with_telemetry(telemetry.clone()),
            ))
        }
        Json { skip_to_brace } => NodeSpec::Transform(Box::new(
            JsonParser::new(*skip_to_brace)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone())),
        )),
        KvMetrics { counters, gauges, distributions } => NodeSpec::Transform(Box::new(
            KvMetricsTransform::new(
                to_metric_specs(counters),
                to_metric_specs(gauges),
                to_metric_specs(distributions),
            )
            .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry.clone()),
        )),
        Keep { fields } => NodeSpec::Transform(Box::new(
            KeepTransform::new(fields.clone()).with_telemetry(telemetry.clone()),
        )),
        Remove { fields } => NodeSpec::Transform(Box::new(
            RemoveTransform::new(fields.clone()).with_telemetry(telemetry.clone()),
        )),
        Set { resource, attributes } => NodeSpec::Transform(Box::new(
            SetTransform::new(to_set_pairs(resource), to_set_pairs(attributes))
                .with_telemetry(telemetry.clone()),
        )),
        TraceContext { trace_id, span_id, flags, keep_source } => NodeSpec::Transform(Box::new(
            TraceContextTransform::new(
                trace_id.clone(),
                span_id.clone(),
                flags.clone(),
                *keep_source,
            )
            .with_telemetry(telemetry.clone()),
        )),
        HasSignal { signals, mode } => NodeSpec::Transform(Box::new(
            HasSignalTransform::new(to_signal_set(signals), to_match_mode(*mode))
                .with_telemetry(telemetry.clone()),
        )),
        KeepSignals { signals } => NodeSpec::Transform(Box::new(
            KeepSignalsTransform::new(to_signal_set(signals)).with_telemetry(telemetry.clone()),
        )),
        DropSignals { signals } => NodeSpec::Transform(Box::new(
            DropSignalsTransform::new(to_signal_set(signals)).with_telemetry(telemetry.clone()),
        )),

        InfluxDbOut { url, org, bucket, token } => NodeSpec::Output(
            Box::new(
                InfluxDbOutput::new(url.clone(), org.clone(), bucket.clone(), token.clone())
                    .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                    .with_telemetry(telemetry.clone()),
            ),
            queue_config(&component.buffer),
            write_config(&component.buffer),
        ),
        OtlpOut { endpoint, protocol, headers, paths, compression } => {
            let output = OtlpOutput::new(endpoint.clone(), otlp_out_transport(*protocol))?
                .with_headers(headers)?
                .with_paths(to_signal_paths(paths))
                .with_compression(to_otlp_compression(*compression))
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone());
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer),
                write_config(&component.buffer),
            )
        }
        StdioOut { target } => {
            let output = match target {
                StdioTarget::Stdout => StdioOutput::stdout(),
                StdioTarget::Stderr => StdioOutput::stderr(),
                // Resolved against `base_dir` (the config file's own directory), exactly as
                // `LuaFile { lua_file, .. }` resolves its script path above -- `Path::join`
                // leaves an already-absolute `path` untouched, so this is correct whether `path`
                // is relative or absolute. Without it, a relative target resolves against the
                // process's current working directory instead, which for `logit run
                // /etc/logit/config.yaml` run from an unrelated directory silently writes
                // somewhere other than "next to the config" (what this kind's own doc comment
                // promises).
                StdioTarget::Path(path) => StdioOutput::open_path(base_dir.join(path))?,
            };
            NodeSpec::Output(
                Box::new(output.with_telemetry(telemetry.clone())),
                queue_config(&component.buffer),
                write_config(&component.buffer),
            )
        }

        SyslogOut {
            endpoint,
            transport,
            format,
            facility,
            hostname,
            app_name,
            max_message_bytes,
            connect_timeout,
        } => {
            // Eager for UDP (a bad local bind is a config error, `StdioOutput::open_path`'s
            // precedent) -- requires an active tokio runtime, which holds here since `build_spec`
            // only ever runs from inside `logit run`'s `runtime.block_on` (`main.rs`), never from
            // `validate`/`graph`. Lazy for TCP -- see `logit_outputs::syslog::Conn`'s doc comment.
            let mut output = match transport {
                logit_config::SyslogTransport::Udp => SyslogOutput::udp(endpoint.clone())?,
                logit_config::SyslogTransport::Tcp => {
                    SyslogOutput::tcp(endpoint.clone(), *connect_timeout)
                }
            };
            let mut encoder = SyslogEncoder::new(syslog_format(*format), facility.as_u8())
                .with_max_message_bytes(*max_message_bytes as usize);
            if let Some(hostname) = hostname {
                encoder = encoder.with_hostname(hostname.clone());
            }
            if let Some(app_name) = app_name {
                encoder = encoder.with_app_name(app_name.clone());
            }
            output = output
                .with_encoder(encoder)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone());
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer),
                write_config(&component.buffer),
            )
        }

        other => unreachable!("graph::resolve already rejected any unimplemented kind: {other:?}"),
    };
    Ok((spec, telemetry))
}

/// Builds a sink's `SinkQueueConfig` from its `BufferConfig` (`docs/adr/buffered-sink-delivery.md`,
/// workstream F) -- the sole place `logit_config::OverflowPolicy` is converted to
/// `logit_pipeline::OverflowPolicy`, since neither config nor pipeline crate can see both types
/// without violating the dependency direction (`logit-pipeline` depends on `logit-config`, never
/// the reverse; `docs/design/pipeline-graph.md`'s crate layout).
fn queue_config(buffer: &BufferConfig) -> SinkQueueConfig {
    SinkQueueConfig {
        max_batches: buffer.max_batches,
        max_bytes: buffer.max_bytes,
        overflow: overflow_policy(buffer.overflow),
    }
}

/// Builds a sink's `WriteLoopConfig` from its `BufferConfig`. `base_delay` (the initial backoff)
/// is deliberately not exposed in `BufferConfig` -- only `retry_budget`/`retry_max_delay` are
/// operator-tunable for now -- so it keeps `RetryConfig::default()`'s value.
fn write_config(buffer: &BufferConfig) -> WriteLoopConfig {
    WriteLoopConfig {
        retry: RetryConfig {
            total_budget: buffer.retry_budget,
            base_delay: RetryConfig::default().base_delay,
            max_delay: buffer.retry_max_delay,
        },
        shutdown_grace: buffer.shutdown_grace,
        delivery_override: buffer.delivery.map(delivery_posture),
    }
}

/// Translates config's `OtlpProtocol` into `logit-inputs`'s own copy of the same two-value
/// choice -- `logit-inputs` doesn't depend on `logit-config` (`docs/design/pipeline-graph.md`'s
/// crate layout), the same reason `overflow_policy`/`delivery_posture` exist just below.
fn otlp_in_transport(protocol: logit_config::OtlpProtocol) -> OtlpInTransport {
    match protocol {
        logit_config::OtlpProtocol::Http => OtlpInTransport::Http,
        logit_config::OtlpProtocol::Grpc => OtlpInTransport::Grpc,
    }
}

/// The `logit-outputs` mirror of [`otlp_in_transport`].
fn otlp_out_transport(protocol: logit_config::OtlpProtocol) -> OtlpOutTransport {
    match protocol {
        logit_config::OtlpProtocol::Http => OtlpOutTransport::Http,
        logit_config::OtlpProtocol::Grpc => OtlpOutTransport::Grpc,
    }
}

/// Translates config's `OtlpCompression` into `logit-outputs`'s own copy of the same choice --
/// same reasoning as `otlp_out_transport`.
fn to_otlp_compression(compression: logit_config::OtlpCompression) -> OtlpOutCompression {
    match compression {
        logit_config::OtlpCompression::None => OtlpOutCompression::None,
        logit_config::OtlpCompression::Gzip => OtlpOutCompression::Gzip,
    }
}

fn overflow_policy(cfg: logit_config::OverflowPolicy) -> logit_pipeline::OverflowPolicy {
    match cfg {
        logit_config::OverflowPolicy::Block => logit_pipeline::OverflowPolicy::Block,
        logit_config::OverflowPolicy::DropOldest => logit_pipeline::OverflowPolicy::DropOldest,
        logit_config::OverflowPolicy::DropNewest => logit_pipeline::OverflowPolicy::DropNewest,
    }
}

/// Builds a UDP listener's `UdpListenerConfig` from its `ReceiveConfig`
/// (`docs/adr/decoupled-listener-io.md`) -- the receive-side mirror of `queue_config`/
/// `write_config` above.
fn receive_config(receive: &logit_config::ReceiveConfig) -> logit_inputs::udp::UdpListenerConfig {
    logit_inputs::udp::UdpListenerConfig {
        max_datagrams: receive.max_datagrams,
        max_bytes: receive.max_bytes,
        overflow: overflow_policy(receive.overflow),
        receive_buffer_bytes: receive.receive_buffer_bytes,
        batch_max_events: receive.batch_max_events,
        batch_max_bytes: receive.batch_max_bytes,
        batch_flush_interval: receive.batch_flush_interval,
        shutdown_grace: receive.shutdown_grace,
    }
}

/// Builds any listener's `InputRuntimeConfig` from its `ReceiveConfig` -- safe to call
/// unconditionally for every `NodeSpec::Input` arm, including `internal`: graph validation's rule
/// 16 already guarantees a non-datagram-listener's `receive` is `ReceiveConfig::default()` by the
/// time a resolved `Graph` reaches `build_spec`, so `internal` always gets `shutdown_grace:
/// ReceiveConfig::default().shutdown_grace` here (5s today, not `Duration::ZERO`) regardless of
/// what any `receive:` block would otherwise say. That's harmless, not just unused, only because
/// `InternalInput` never overrides `Input::run_until_shutdown`: the default impl's own `select!`
/// always resolves at t=shutdown against a non-overriding input, so `run_input`'s grace backstop
/// -- built from this value -- never gets a chance to matter. If `internal` ever gains a
/// cooperative drain of its own, this stops being a harmless default and needs its own
/// `receive.shutdown_grace`-shaped knob rather than inheriting whatever `ReceiveConfig::default`
/// happens to say.
fn input_runtime_config(receive: &logit_config::ReceiveConfig) -> InputRuntimeConfig {
    InputRuntimeConfig { shutdown_grace: receive.shutdown_grace }
}

fn delivery_posture(cfg: logit_config::DeliveryPosture) -> logit_pipeline::DeliveryPosture {
    match cfg {
        logit_config::DeliveryPosture::AtLeastOnce => logit_pipeline::DeliveryPosture::AtLeastOnce,
        logit_config::DeliveryPosture::AtMostOnce => logit_pipeline::DeliveryPosture::AtMostOnce,
    }
}

/// The sole place `logit_config::SyslogFormat` crosses into `logit_outputs::syslog::Format` --
/// `logit-outputs` never depends on `logit-config` (`docs/design/pipeline-graph.md`'s crate
/// layout), mirroring `overflow_policy`/`delivery_posture` above.
fn syslog_format(cfg: logit_config::SyslogFormat) -> logit_outputs::syslog::Format {
    match cfg {
        logit_config::SyslogFormat::Rfc3164 => logit_outputs::syslog::Format::Rfc3164,
        logit_config::SyslogFormat::Rfc5424 => logit_outputs::syslog::Format::Rfc5424,
    }
}

/// Converts config's `Vec<Signal>` (`logit-config`, which `logit-transforms` deliberately doesn't
/// depend on -- `docs/design/pipeline-graph.md`'s crate layout) into the transform crate's
/// boolean-flags `SignalSet`.
fn to_signal_set(signals: &[logit_config::Signal]) -> SignalSet {
    let mut set = SignalSet::default();
    for signal in signals {
        match signal {
            logit_config::Signal::Logs => set.logs = true,
            logit_config::Signal::Metrics => set.metrics = true,
            logit_config::Signal::Traces => set.traces = true,
        }
    }
    set
}

fn to_match_mode(mode: logit_config::MatchMode) -> TransformMatchMode {
    match mode {
        logit_config::MatchMode::AnyOf => TransformMatchMode::AnyOf,
        logit_config::MatchMode::Only => TransformMatchMode::Only,
    }
}

/// Converts config's `OtlpPaths` (`logit-config`, which `logit-outputs` deliberately doesn't
/// depend on -- `docs/design/pipeline-graph.md`'s crate layout) into the output crate's own
/// identically-shaped `SignalPaths`.
fn to_signal_paths(paths: &logit_config::OtlpPaths) -> SignalPaths {
    SignalPaths {
        logs: paths.logs.clone(),
        metrics: paths.metrics.clone(),
        traces: paths.traces.clone(),
    }
}

/// Converts config's `MetricSpec` (`logit-config`, which `logit-transforms` deliberately doesn't
/// depend on -- `docs/design/pipeline-graph.md`'s crate layout) into the transform crate's own
/// identically-shaped type.
fn to_metric_specs(specs: &[logit_config::MetricSpec]) -> Vec<logit_transforms::MetricSpec> {
    specs
        .iter()
        .map(|s| logit_transforms::MetricSpec {
            name: s.name.clone(),
            field: s.field.clone(),
            unit: s.unit.clone(),
        })
        .collect()
}

/// Converts `logit-config`'s `SetValue` map (`ComponentKind::Set`'s `resource`/`attributes`
/// fields) into the plain `(String, logit_core::Value)` pairs `logit_transforms::Set::new` takes
/// -- `logit-transforms` doesn't depend on `logit-config` (`docs/design/pipeline-graph.md`'s crate
/// layout), same reasoning as [`to_metric_specs`] above. A `BTreeMap` iterates in key order, which
/// is why `Set`'s own tests don't need to assert an order beyond "whatever `AttrMap`'s sorted
/// `Symbol` order ends up being" -- the interning happens once, at construction, inside `Set::new`.
fn to_set_pairs(
    values: &std::collections::BTreeMap<String, logit_config::SetValue>,
) -> Vec<(String, logit_core::Value)> {
    values
        .iter()
        .map(|(k, v)| {
            let value = match v {
                logit_config::SetValue::Bool(b) => logit_core::Value::Bool(*b),
                logit_config::SetValue::I64(i) => logit_core::Value::I64(*i),
                logit_config::SetValue::F64(f) => logit_core::Value::F64(*f),
                logit_config::SetValue::Str(s) => logit_core::Value::str(s.clone()),
            };
            (k.clone(), value)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_config::{Component, ComponentKind};
    use std::collections::HashMap as Map;
    use std::time::Duration;

    fn statsd_in() -> Component {
        Component {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
        }
    }

    fn influxdb_out(sources: Vec<&str>) -> Component {
        Component {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: sources.into_iter().map(String::from).collect(),
            kind: ComponentKind::InfluxDbOut {
                url: "http://localhost:8086".to_string(),
                org: "org".to_string(),
                bucket: "bucket".to_string(),
                token: "test-token".to_string(),
            },
        }
    }

    fn config(components: Vec<(&str, Component)>) -> Config {
        let mut map = Map::new();
        for (id, component) in components {
            map.insert(id.to_string(), component);
        }
        Config { components: map }
    }

    #[test]
    fn validate_semantics_rejects_an_empty_config() {
        let err = validate_semantics(config(vec![])).expect_err("expected an error");
        assert!(err.to_string().contains("no components"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_a_listener_with_no_consumers() {
        let err =
            validate_semantics(config(vec![("in", statsd_in())])).expect_err("expected an error");
        assert!(err.to_string().contains("no consumers"), "got: {err}");
    }

    #[test]
    fn validate_semantics_accepts_a_well_formed_config() {
        let cfg = config(vec![("in", statsd_in()), ("out", influxdb_out(vec!["in"]))]);
        assert!(validate_semantics(cfg).is_ok());
    }

    /// The headline regression test at the CLI layer, mirroring `logit-pipeline::graph`'s: a sink
    /// shared by two upstream branches is accepted now, where the pre-graph `validate_semantics`
    /// rejected any output referenced by more than one pipeline outright.
    #[test]
    fn validate_semantics_accepts_a_sink_shared_by_two_branches() {
        let cfg = config(vec![
            ("in", statsd_in()),
            (
                "branch_a",
                Component {
                    buffer: logit_config::BufferConfig::default(),
                    receive: logit_config::ReceiveConfig::default(),
                    sources: vec!["in".to_string()],
                    kind: ComponentKind::Lua { script: "".to_string(), interval: None },
                },
            ),
            (
                "branch_b",
                Component {
                    buffer: logit_config::BufferConfig::default(),
                    receive: logit_config::ReceiveConfig::default(),
                    sources: vec!["in".to_string()],
                    kind: ComponentKind::Lua { script: "".to_string(), interval: None },
                },
            ),
            ("out", influxdb_out(vec!["branch_a", "branch_b"])),
        ]);
        assert!(validate_semantics(cfg).is_ok());
    }

    #[tokio::test]
    async fn run_config_reports_a_missing_lua_file_clearly() {
        let cfg = config(vec![
            ("in", statsd_in()),
            (
                "enrich",
                Component {
                    buffer: logit_config::BufferConfig::default(),
                    receive: logit_config::ReceiveConfig::default(),
                    sources: vec!["in".to_string()],
                    kind: ComponentKind::LuaFile {
                        lua_file: "does-not-exist.lua".to_string(),
                        interval: None,
                    },
                },
            ),
            ("out", influxdb_out(vec!["enrich"])),
        ]);
        let err = run_config(cfg, PathBuf::new()).await.expect_err("expected an error");
        assert!(format!("{err:?}").contains("does-not-exist.lua"), "got: {err:?}");
    }

    #[test]
    fn prepare_builds_no_registry_and_only_disabled_handles_without_an_internal_component() {
        let cfg = config(vec![("in", statsd_in()), ("out", influxdb_out(vec!["in"]))]);
        let (_, _, telemetry) = prepare(cfg, PathBuf::new()).unwrap();
        assert_eq!(telemetry.len(), 2);
        assert!(
            telemetry.values().all(|t| !t.is_enabled()),
            "no config-level 'internal' component should mean every handle stays disabled"
        );
    }

    #[test]
    fn prepare_wires_a_live_registry_when_config_has_an_internal_component() {
        let cfg = config(vec![
            (
                "self",
                Component {
                    buffer: logit_config::BufferConfig::default(),
                    receive: logit_config::ReceiveConfig::default(),
                    sources: vec![],
                    kind: ComponentKind::Internal {
                        interval: Duration::from_secs(10),
                        span_sample_rate: logit_core::DEFAULT_SPAN_SAMPLE_RATE,
                    },
                },
            ),
            ("out", influxdb_out(vec!["self"])),
        ]);
        let (_, _, telemetry) = prepare(cfg, PathBuf::new()).unwrap();
        assert!(
            telemetry.values().all(|t| t.is_enabled()),
            "an 'internal' component should give every component a live telemetry handle"
        );
    }

    #[test]
    fn build_spec_builds_an_internal_input() {
        let registry = Registry::new();
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Internal {
                interval: Duration::from_secs(10),
                span_sample_rate: logit_core::DEFAULT_SPAN_SAMPLE_RATE,
            },
        };
        let (spec, telemetry) =
            build_spec("self", &component, Path::new(""), Some(&registry)).unwrap();
        assert!(matches!(spec, NodeSpec::Input(..)));
        assert!(telemetry.is_enabled());
    }

    #[test]
    fn build_spec_builds_an_aggregate_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Aggregate {
                interval: Duration::from_secs(10),
                gauge_retention: 5,
                max_retained_gauge_series: 10_000,
            },
        };
        assert!(matches!(
            build_spec("windowed", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    /// `token` is a plain field now (no more `token_env` indirection, no more `std::env::var`
    /// here) -- an unset `!env` variable is caught earlier, at `config::load` time, not here.
    #[test]
    fn build_spec_builds_an_influxdb_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::InfluxDbOut {
                url: "http://localhost:8086".to_string(),
                org: "org".to_string(),
                bucket: "bucket".to_string(),
                token: "test-token".to_string(),
            },
        };
        assert!(matches!(
            build_spec("out", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    #[test]
    fn build_spec_builds_an_otlp_input() {
        for protocol in [logit_config::OtlpProtocol::Http, logit_config::OtlpProtocol::Grpc] {
            let component = ResolvedComponent {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec![],
                consumers: vec!["out".to_string()],
                kind: ComponentKind::OtlpIn { bind: "127.0.0.1:0".to_string(), protocol },
            };
            assert!(
                matches!(
                    build_spec("in", &component, Path::new(""), None).unwrap().0,
                    NodeSpec::Input(..)
                ),
                "protocol {protocol:?}"
            );
        }
    }

    #[test]
    fn build_spec_builds_an_otlp_sink() {
        for protocol in [logit_config::OtlpProtocol::Http, logit_config::OtlpProtocol::Grpc] {
            let component = ResolvedComponent {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                consumers: vec![],
                kind: ComponentKind::OtlpOut {
                    endpoint: "http://localhost:4318".to_string(),
                    protocol,
                    headers: HashMap::new(),
                    paths: logit_config::OtlpPaths::default(),
                    compression: logit_config::OtlpCompression::default(),
                },
            };
            assert!(
                matches!(
                    build_spec("out", &component, Path::new(""), None).unwrap().0,
                    NodeSpec::Output(_, _, _)
                ),
                "protocol {protocol:?}"
            );
        }
    }

    /// `OtlpOutput::new`'s https-under-grpc guard (`crates/logit-outputs/src/otlp.rs`) surfaces
    /// through `build_spec` as a clear config error, not a panic or a silently-downgraded
    /// connection -- `NodeSpec` isn't `Debug` (see `build_spec_reports_a_clear_path_naming_error_
    /// for_an_unopenable_stdio_target`'s comment for why this can't use `expect_err` directly).
    #[test]
    fn build_spec_rejects_an_otlp_sink_with_https_under_grpc() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::OtlpOut {
                endpoint: "https://tempo:4317".to_string(),
                protocol: logit_config::OtlpProtocol::Grpc,
                headers: HashMap::new(),
                paths: logit_config::OtlpPaths::default(),
                compression: logit_config::OtlpCompression::default(),
            },
        };
        let err = match build_spec("out", &component, Path::new(""), None) {
            Ok(_) => {
                panic!("expected build_spec to reject an https:// endpoint under protocol: grpc")
            }
            Err(err) => err,
        };
        assert!(format!("{err:?}").contains("https"), "got: {err:?}");
    }

    /// The wiring this workstream adds: a non-default `buffer:` on the component actually reaches
    /// the built `NodeSpec::Output`'s `SinkQueueConfig`/`WriteLoopConfig`, not just
    /// `SinkQueueConfig::default()`/`WriteLoopConfig::default()` as before.
    #[test]
    fn build_spec_wires_a_non_default_buffer_config_into_the_sink_queue_and_write_loop() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig {
                max_batches: 4096,
                max_bytes: 128 * 1024 * 1024,
                overflow: logit_config::OverflowPolicy::DropOldest,
                delivery: Some(logit_config::DeliveryPosture::AtLeastOnce),
                retry_budget: Duration::from_secs(120),
                retry_max_delay: Duration::from_secs(20),
                shutdown_grace: Duration::from_secs(10),
            },
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::InfluxDbOut {
                url: "http://localhost:8086".to_string(),
                org: "org".to_string(),
                bucket: "bucket".to_string(),
                token: "test-token".to_string(),
            },
        };
        let NodeSpec::Output(_, queue_config, write_config) =
            build_spec("out", &component, Path::new(""), None).unwrap().0
        else {
            panic!("expected NodeSpec::Output");
        };
        assert_eq!(queue_config.max_batches, 4096);
        assert_eq!(queue_config.max_bytes, 128 * 1024 * 1024);
        assert_eq!(queue_config.overflow, logit_pipeline::OverflowPolicy::DropOldest);
        assert_eq!(write_config.retry.total_budget, Duration::from_secs(120));
        assert_eq!(write_config.retry.max_delay, Duration::from_secs(20));
        assert_eq!(
            write_config.retry.base_delay,
            logit_pipeline::RetryConfig::default().base_delay,
            "base_delay is not config-exposed -- always the default"
        );
        assert_eq!(write_config.shutdown_grace, Duration::from_secs(10));
        assert_eq!(
            write_config.delivery_override,
            Some(logit_pipeline::DeliveryPosture::AtLeastOnce)
        );
    }

    #[test]
    fn build_spec_builds_a_json_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Json { skip_to_brace: true },
        };
        assert!(matches!(
            build_spec("parse", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    fn stdio_out_component(target: StdioTarget) -> ResolvedComponent {
        ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::StdioOut { target },
        }
    }

    #[test]
    fn build_spec_builds_a_stdio_sink_for_stdout() {
        let component = stdio_out_component(StdioTarget::Stdout);
        assert!(matches!(
            build_spec("tap", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    #[test]
    fn build_spec_builds_a_stdio_sink_for_stderr() {
        let component = stdio_out_component(StdioTarget::Stderr);
        assert!(matches!(
            build_spec("tap", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    #[test]
    fn build_spec_builds_a_stdio_sink_for_a_file_path() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-build-spec-stdio-out-test-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let component = stdio_out_component(StdioTarget::Path(path.display().to_string()));
        assert!(matches!(
            build_spec("tap", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));

        std::fs::remove_file(&path).ok();
    }

    /// A *relative* `target:` path must resolve against the config file's own directory
    /// (`base_dir`), exactly as `LuaFile`'s `lua_file` already does -- not against the process's
    /// current working directory, which for `logit run` invoked from an unrelated directory would
    /// silently write somewhere other than "next to the config", contradicting `StdioTarget`'s own
    /// doc comment.
    #[test]
    fn build_spec_resolves_a_relative_stdio_target_against_the_config_base_dir() {
        let base_dir = std::env::temp_dir()
            .join(format!("logit-build-spec-stdio-base-dir-{}", std::process::id()));
        std::fs::create_dir_all(&base_dir).expect("base_dir should be creatable");
        let relative = "relative-debug.log";
        let expected_path = base_dir.join(relative);
        let _ = std::fs::remove_file(&expected_path);

        let component = stdio_out_component(StdioTarget::Path(relative.to_string()));
        assert!(matches!(
            build_spec("tap", &component, &base_dir, None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
        assert!(
            expected_path.exists(),
            "expected the relative target to be created inside base_dir ({}), not the process cwd",
            base_dir.display()
        );

        std::fs::remove_file(&expected_path).ok();
        std::fs::remove_dir(&base_dir).ok();
    }

    #[test]
    fn build_spec_reports_a_clear_path_naming_error_for_an_unopenable_stdio_target() {
        // `NodeSpec` isn't `Debug` (it embeds trait objects), so `Result::expect_err` -- which
        // needs `Debug` on the `Ok` side to format its panic message -- doesn't work here. Same
        // reason `logit-pipeline::graph`'s tests have their own `expect_err` helper.
        let path = std::env::temp_dir().join("logit-build-spec-no-such-dir").join("x.log");
        let component = stdio_out_component(StdioTarget::Path(path.display().to_string()));
        let err = match build_spec("tap", &component, Path::new(""), None) {
            Ok(_) => panic!("expected build_spec to fail for an unopenable path"),
            Err(err) => err,
        };
        assert!(format!("{err:?}").contains(&path.display().to_string()), "got: {err:?}");
    }

    #[test]
    fn build_spec_builds_a_kv_metrics_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::KvMetrics {
                counters: vec![logit_config::MetricSpec {
                    name: "hits".to_string(),
                    field: None,
                    unit: None,
                }],
                gauges: vec![],
                distributions: vec![],
            },
        };
        assert!(matches!(
            build_spec("derive", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_keep_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Keep { fields: vec!["status".to_string()] },
        };
        assert!(matches!(
            build_spec("keep", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_remove_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Remove { fields: vec!["client_ip".to_string()] },
        };
        assert!(matches!(
            build_spec("remove", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_set_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Set {
                resource: std::collections::BTreeMap::from([(
                    "service.name".to_string(),
                    logit_config::SetValue::Str("nginx".to_string()),
                )]),
                attributes: std::collections::BTreeMap::new(),
            },
        };
        assert!(matches!(
            build_spec("identity", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    /// Unlike `build_spec_builds_a_set_transform` above, this actually runs the built transform
    /// against an event rather than only checking the `NodeSpec` variant -- specifically to catch
    /// a swapped-argument-order regression (`trace_id`/`span_id`/`flags`/`keep_source` all being
    /// the same shape of value at the call site makes that an easy mistake to introduce silently).
    #[test]
    fn build_spec_builds_a_working_trace_context_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::TraceContext {
                trace_id: "tid".to_string(),
                span_id: Some("sid".to_string()),
                flags: None,
                keep_source: true,
            },
        };
        let NodeSpec::Transform(mut transform) =
            build_spec("trace", &component, Path::new(""), None).unwrap().0
        else {
            panic!("expected a Transform node");
        };

        let mut attrs = logit_core::AttrMap::new();
        attrs.insert("tid", logit_core::Value::str("ab".repeat(16)));
        attrs.insert("sid", logit_core::Value::str("cd".repeat(8)));
        let event = logit_core::Event::log(
            0,
            attrs,
            logit_core::LogRecord {
                message: logit_core::Value::str("msg"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
            },
        );
        let resource = Arc::new(logit_core::Resource::default());
        let out = transform.process(&resource, event).expect("should forward the event");
        let trace = out.log.expect("log should survive").trace.expect("trace should be lifted");
        assert_eq!(trace.trace_id, [0xab; 16]);
        assert_eq!(trace.span_id, Some([0xcd; 8]));
        assert!(
            out.attributes.get("tid").is_some(),
            "keep_source: true should retain the attribute"
        );
    }

    #[test]
    fn build_spec_builds_a_has_signal_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::HasSignal {
                signals: vec![logit_config::Signal::Traces],
                mode: logit_config::MatchMode::AnyOf,
            },
        };
        assert!(matches!(
            build_spec("has_signal", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_keep_signals_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::KeepSignals { signals: vec![logit_config::Signal::Logs] },
        };
        assert!(matches!(
            build_spec("keep_signals", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_drop_signals_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::DropSignals { signals: vec![logit_config::Signal::Metrics] },
        };
        assert!(matches!(
            build_spec("drop_signals", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }
}
