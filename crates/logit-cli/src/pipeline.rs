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
use logit_inputs::docker::{ContainerFilter, DockerInput};
use logit_inputs::internal::InternalInput;
use logit_inputs::logit::LogitInput;
use logit_inputs::otlp::{OtlpInput, OtlpTransport as OtlpInTransport};
use logit_inputs::prometheus::PrometheusInput;
use logit_inputs::statsd::StatsdInput;
use logit_inputs::syslog::SyslogInput;
use logit_inputs::tail::TailInput;
use logit_outputs::file::{RotateInterval as OutputRotateInterval, RotatePolicy};
use logit_outputs::influxdb::InfluxDbOutput;
use logit_outputs::logit::LogitOutput;
use logit_outputs::otlp::{
    OtlpCompression as OtlpOutCompression, OtlpOutput, OtlpTransport as OtlpOutTransport,
    SignalPaths,
};
use logit_outputs::statsd::{StatsdEncoder, StatsdOutput};
use logit_outputs::stdio::StreamOutput;
use logit_outputs::syslog::{SyslogEncoder, SyslogOutput};
use logit_pipeline::graph::{self, ResolvedComponent};
use logit_pipeline::{
    DiskQueueConfig, InputRuntimeConfig, NodeSpec, Readiness, RetryConfig, RunError,
    SinkQueueConfig, SinkStoreConfig, WriteLoopConfig,
};
use logit_proto::frame::Compression as NativeCompression;
use logit_transforms::{
    AggregateTemporality as TransformTemporality, Aggregator, CsvParser,
    Distributions as TransformDistributions, DropAttributes as DropAttributesTransform,
    DropProvenance as DropProvenanceTransform, DropSignals as DropSignalsTransform,
    HasAttributes as HasAttributesTransform, HasProvenance as HasProvenanceTransform,
    HasSignal as HasSignalTransform, JsonParser, Keep as KeepTransform,
    KeepSignals as KeepSignalsTransform, Kv as KvTransform, KvMetrics as KvMetricsTransform,
    Logfmt as LogfmtTransform, MatchMode as TransformMatchMode, RegexParser,
    Remove as RemoveTransform, Scale as ScaleTransform, Set as SetTransform, Sets as TransformSets,
    SignalSet, SpanLift, TraceContext as TraceContextTransform,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The operator-chosen id, live [`Registry`], and `logs` threshold of a config's `internal`
/// component -- what [`run_pipelines`] needs to [`logit_core::TelemetryLayer::activate`] it
/// (`docs/plans/operator-surface.md`, workstream D). `None` means no `internal` component, so no
/// `Registry` was built and the layer is never activated.
pub struct InternalInfo {
    pub registry: Arc<Registry>,
    pub id: String,
    pub logs: logit_config::InternalLogs,
}

/// `InternalInfo::logs` -> the threshold [`logit_core::TelemetryLayer::activate`] takes, or
/// `None` for `off` (which means: never activate the layer at all).
fn severity_for_logs(logs: logit_config::InternalLogs) -> Option<logit_core::Severity> {
    match logs {
        logit_config::InternalLogs::Off => None,
        logit_config::InternalLogs::Warn => Some(logit_core::Severity::Warn),
        logit_config::InternalLogs::Error => Some(logit_core::Severity::Error),
    }
}

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
///
/// `telemetry_layer` is `main`'s already-`.init()`-ed `TelemetryLayer` handle
/// (`docs/plans/operator-surface.md`, workstream D) -- installed inactive, before any of this
/// ran (there is no stable API to add a layer to an already-`.init()`-ed subscriber), and
/// activated here, once the config's own `internal` component (if any) is known.
pub async fn run_pipelines(
    path: PathBuf,
    telemetry_layer: logit_core::TelemetryLayer,
) -> Result<(), RunError> {
    // An unset `!env` variable (a missing token, most likely) fails here, before anything starts
    // listening.
    let config = config::load(&path).map_err(RunError::Startup)?;

    // `docs/plans/operator-surface.md`'s stable lifecycle event names, for log-based alerting.
    // Logged before `prepare` (which can still reject the config -- an empty graph, an unknown
    // source, a cycle) so a config that never gets that far still leaves a `starting` line behind
    // naming what was attempted; `config.components` (the raw, unresolved map) is what's on hand
    // at this point, not yet the resolved `Graph`.
    tracing::info!(
        target: "logit",
        config = %path.display(),
        components = config.components.len(),
        version = env!("CARGO_PKG_VERSION"),
        "starting"
    );

    let admin_bind = config.admin.bind.clone();
    let base_dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
    let (graph, specs, telemetry, internal) =
        prepare(config, base_dir).map_err(RunError::Startup)?;

    if let Some(info) = &internal {
        if let Some(threshold) = severity_for_logs(info.logs) {
            telemetry_layer.activate(info.registry.clone(), threshold, info.id.clone());
        }
    }

    // Independent listener from the one `run_with_shutdown` races internally (below) -- multiple
    // concurrent listeners on the same signal kind are supported and all get notified, so this
    // doesn't compete with or consume the first one. Aborted once `run_with_shutdown` returns so
    // it doesn't linger if shutdown never happens.
    let kill_switch = tokio::spawn(async {
        shutdown_signal().await;
        shutdown_signal().await;
        std::process::exit(130);
    });

    // `admin.bind` set: bind its listener *synchronously*, here, before `run_with_telemetry` ever
    // starts -- a bind failure is `RunError::Startup`, the same "fail before anything else spawns"
    // guarantee `Input::bind`'s own pre-pass gives every ordinary listener. Not set: the
    // `Readiness::disabled()` placeholder every test and every config without an `admin:` block
    // already uses.
    let (readiness, admin_server) = match admin_bind {
        Some(bind) => {
            let listener = tokio::net::TcpListener::bind(&bind)
                .await
                .with_context(|| format!("admin: binding '{bind}'"))
                .map_err(RunError::Startup)?;
            let (readiness, readiness_rx) = Readiness::channel();
            // Deliberately *not* given a shutdown listener of its own. The drain that a signal
            // starts is exactly the window `/readyz` has to answer `503 draining` in -- several
            // seconds of sink flush and listener grace (`buffer.shutdown_grace`,
            // `receive.shutdown_grace`) during which an orchestrator must be told "stop routing
            // here, I am still finishing", not handed a refused connection it cannot tell from a
            // crash. `abort()` below, once `run_with_telemetry` has already returned, is the sole
            // teardown.
            (readiness, Some(tokio::spawn(crate::admin::serve_on(listener, readiness_rx))))
        }
        None => (Readiness::disabled(), None),
    };

    let result =
        logit_pipeline::run_with_telemetry(graph, specs, telemetry, readiness, shutdown_signal())
            .await;
    kill_switch.abort();
    if let Some(admin_server) = admin_server {
        admin_server.abort();
    }
    match &result {
        Ok(()) => tracing::info!(target: "logit", code = 0, "exiting"),
        Err(err) => {
            tracing::error!(target: "logit", code = err.exit_code(), reason = %err, "exiting")
        }
    }
    result
}

/// A resolved `Graph`, one built `NodeSpec` and one [`Telemetry`] handle per component, and the
/// config's own [`InternalInfo`] if it has an `internal` component -- [`prepare`]'s return type,
/// factored out purely to keep clippy's `type_complexity` lint happy. The config's `admin` block
/// is deliberately not in here: [`run_pipelines`] clones it off the `Config` before handing the
/// config to [`prepare`], which consumes it.
type PrepareResult =
    (graph::Graph, HashMap<String, NodeSpec>, HashMap<String, Telemetry>, Option<InternalInfo>);

/// Resolves a config into a `Graph`, one built `NodeSpec` per component, one [`Telemetry`] handle
/// per component, and the config's [`InternalInfo`] if it has an `internal` component -- the
/// shared setup between [`run_pipelines`] and [`run_config`] (the latter used directly by tests
/// below, which don't need shutdown wiring).
///
/// The telemetry map is empty (every handle [`Telemetry::default`], the disabled no-op) unless
/// `config` contains an `internal` component, in which case a single process-wide [`Registry`] is
/// built and shared by every component -- one live handle per component id, reused for both its
/// own instrumentation (`build_spec`, layer 3) and the node runtime's uniform instrumentation
/// (`logit_pipeline::run_with_telemetry`, layer 2), so both land in the same buffer and drain
/// together. See `docs/design/internal-telemetry.md`.
fn prepare(config: Config, base_dir: PathBuf) -> anyhow::Result<PrepareResult> {
    let graph = graph::resolve(config)?;

    // Read off the config's own `internal` component (graph rule 13 already guarantees at most
    // one), rather than always calling `Registry::new`'s default -- an operator who set
    // `span_sample_rate` explicitly (`demo/logit.yaml`'s `1.0`, say) would otherwise have their
    // choice silently ignored.
    let internal_component = graph.components.iter().find_map(|(id, c)| match &c.kind {
        logit_config::ComponentKind::Internal { span_sample_rate, logs, .. } => {
            Some((id.clone(), *span_sample_rate, *logs))
        }
        _ => None,
    });
    let registry: Option<Arc<Registry>> =
        internal_component.as_ref().map(|(_, rate, _)| Registry::with_span_sampling(*rate));

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

    let internal = match (internal_component, registry) {
        (Some((id, _, logs)), Some(registry)) => Some(InternalInfo { registry, id, logs }),
        _ => None,
    };

    Ok((graph, specs, telemetry, internal))
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
    let (graph, specs, telemetry, _internal) = prepare(config, base_dir)?;
    logit_pipeline::run_with_telemetry(
        graph,
        specs,
        telemetry,
        Readiness::disabled(),
        std::future::pending(),
    )
    .await
    .map_err(RunError::into_inner)
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
        OtlpIn { bind, protocol, tls } => {
            let mut input = OtlpInput::new(bind.clone(), otlp_in_transport(*protocol))
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone());
            if let Some(tls) = tls {
                input = input.with_tls(&to_tls_server_settings(tls), base_dir)?;
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        PrometheusIn { targets, interval, timeout, headers, tls } => {
            let input = PrometheusInput::new(targets.clone(), *interval)
                .with_timeout(*timeout)
                .with_headers(headers)?
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                .with_tls(&to_input_tls_client_settings(tls), base_dir)?;
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        LogitIn { bind, tls, max_frame_bytes } => {
            let mut input = LogitInput::new(bind.clone())
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone());
            if let Some(tls) = tls {
                input = input.with_tls(&to_tls_server_settings(tls), base_dir)?;
            }
            if let Some(max_frame_bytes) = max_frame_bytes {
                input = input.with_max_frame_bytes(*max_frame_bytes as u32);
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        TailIn { paths, tail } => {
            let paths = paths.iter().map(|p| base_dir.join(p)).collect();
            NodeSpec::Input(
                Box::new(
                    TailInput::new(paths, tail_config(tail, &component.receive, base_dir))
                        .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                        .with_telemetry(telemetry.clone()),
                ),
                input_runtime_config(&component.receive),
            )
        }
        DockerIn { root, containers, discover, labels, tail } => {
            let root = base_dir.join(root);
            let filter = ContainerFilter::new(containers.clone(), *discover);
            NodeSpec::Input(
                Box::new(
                    DockerInput::new(
                        root,
                        filter,
                        labels.clone(),
                        tail_config(tail, &component.receive, base_dir),
                    )
                    .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                    .with_telemetry(telemetry.clone()),
                ),
                input_runtime_config(&component.receive),
            )
        }
        // `span_sample_rate`/`logs` are both read by `prepare` (above) -- the former to build the
        // `Registry` itself (already baked into the handle this arm receives), the latter to
        // decide whether `main::init_logging` stacks a `TelemetryLayer` at all. Neither is this
        // arm's concern.
        Internal { interval, span_sample_rate: _, logs: _ } => {
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
        Aggregate {
            interval,
            temporality,
            series_retention,
            max_retained_series,
            distributions,
            max_samples_per_series,
            sets,
            max_set_members_per_series,
        } => NodeSpec::Transform(Box::new(
            Aggregator::new(*interval)
                .with_temporality(to_temporality(*temporality))
                .with_series_retention(*series_retention, *max_retained_series)
                .with_distributions(to_distributions(*distributions), *max_samples_per_series)
                .with_sets(to_sets(*sets), *max_set_members_per_series)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone()),
        )),
        Json { skip_to_brace } => NodeSpec::Transform(Box::new(
            JsonParser::new(*skip_to_brace)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone())),
        )),
        Csv { columns, delimiter } => NodeSpec::Transform(Box::new(
            CsvParser::new(columns.clone(), *delimiter as u8)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone()),
        )),
        Logfmt { bare_keys } => NodeSpec::Transform(Box::new(
            LogfmtTransform::new(*bare_keys)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone()),
        )),
        Kv { pair_sep, kv_sep, bare_keys } => NodeSpec::Transform(Box::new(
            KvTransform::new(pair_sep.clone(), kv_sep.clone(), *bare_keys)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone()),
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
        TraceContext { trace_id, span_id, flags, keep_source, span } => {
            let mut transform = TraceContextTransform::new(
                trace_id.clone(),
                span_id.clone(),
                flags.clone(),
                *keep_source,
            )
            .with_telemetry(telemetry.clone());
            if let Some(span) = span {
                transform = transform.with_span(to_span_lift(span));
            }
            NodeSpec::Transform(Box::new(transform))
        }
        Scale { fields } => NodeSpec::Transform(Box::new(
            ScaleTransform::new(fields.iter().map(|(k, v)| (k.clone(), *v)).collect())
                .with_telemetry(telemetry.clone()),
        )),
        // The `?` here is unreachable in practice: `graph::resolve`'s rule 29 already compiled
        // this exact pattern successfully, so `RegexParser::new` can only fail on a pattern
        // validation let through -- which it doesn't.
        Regex { pattern, field } => NodeSpec::Transform(Box::new(
            RegexParser::new(pattern, field.as_deref())?.with_telemetry(telemetry.clone()),
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
        HasAttributes { resource, attributes } => NodeSpec::Transform(Box::new(
            HasAttributesTransform::new(to_set_pairs(resource), to_set_pairs(attributes))
                .with_telemetry(telemetry.clone()),
        )),
        DropAttributes { resource, attributes } => NodeSpec::Transform(Box::new(
            DropAttributesTransform::new(to_set_pairs(resource), to_set_pairs(attributes))
                .with_telemetry(telemetry.clone()),
        )),
        // No conversion helper needed here, unlike `to_set_pairs`/`to_signal_set`:
        // `ComponentKind::HasProvenance`'s fields are already the plain `Vec<String>`
        // `HasProvenanceTransform::new` takes -- interning happens inside the transform itself
        // (`crate::provenance::Matcher::new`), not at the config boundary.
        HasProvenance { origin, previous } => NodeSpec::Transform(Box::new(
            HasProvenanceTransform::new(origin.clone(), previous.clone())
                .with_telemetry(telemetry.clone()),
        )),
        DropProvenance { origin, previous } => NodeSpec::Transform(Box::new(
            DropProvenanceTransform::new(origin.clone(), previous.clone())
                .with_telemetry(telemetry.clone()),
        )),

        InfluxDbOut { url, org, bucket, token } => NodeSpec::Output(
            Box::new(
                InfluxDbOutput::new(url.clone(), org.clone(), bucket.clone(), token.clone())
                    .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                    .with_telemetry(telemetry.clone()),
            ),
            queue_config(&component.buffer, base_dir),
            write_config(&component.buffer),
        ),
        OtlpOut { endpoint, protocol, headers, paths, compression, tls } => {
            let output = OtlpOutput::new(endpoint.clone(), otlp_out_transport(*protocol))?
                .with_headers(headers)?
                .with_paths(to_signal_paths(paths))
                .with_compression(to_otlp_compression(*compression))
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                .with_tls(&to_tls_client_settings(tls), base_dir)?;
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer, base_dir),
                write_config(&component.buffer),
            )
        }
        LogitOut { endpoint, compression, tls, request_timeout } => {
            let mut output = LogitOutput::new(endpoint.clone())
                .with_compression(to_native_compression(*compression))
                .with_timeout(*request_timeout)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone());
            if let Some(tls) = tls {
                // `logit_outputs::logit::TlsClientSettings` and `logit_outputs::otlp::
                // TlsClientSettings` are the same type (`logit_outputs::tls::TlsClientSettings`,
                // re-exported at both paths) -- `to_tls_client_settings` already builds it.
                output = output.with_tls(&to_tls_client_settings(tls), base_dir)?;
            }
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer, base_dir),
                write_config(&component.buffer),
            )
        }
        StdioOut { target, format, compression } => {
            let output = match target {
                StdioTarget::Stdout => StreamOutput::stdout(),
                StdioTarget::Stderr => StreamOutput::stderr(),
                // Resolved against `base_dir` (the config file's own directory), exactly as
                // `LuaFile { lua_file, .. }` resolves its script path above -- `Path::join`
                // leaves an already-absolute `path` untouched, so this is correct whether `path`
                // is relative or absolute. Without it, a relative target resolves against the
                // process's current working directory instead, which for `logit run
                // /etc/logit/config.yaml` run from an unrelated directory silently writes
                // somewhere other than "next to the config" (what this kind's own doc comment
                // promises).
                StdioTarget::Path(path) => StreamOutput::open_path(base_dir.join(path))?,
            };
            let output = output.with_format(to_stream_encoder(*format, *compression));
            NodeSpec::Output(
                Box::new(output.with_telemetry(telemetry.clone())),
                queue_config(&component.buffer, base_dir),
                write_config(&component.buffer),
            )
        }
        FileOut { path, rotate, format, compression } => {
            // Resolved against `base_dir`, exactly as `StdioTarget::Path` above.
            let output = StreamOutput::rotating(base_dir.join(path), to_rotate_policy(rotate))?
                .with_format(to_stream_encoder(*format, *compression))
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone());
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer, base_dir),
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
            structured_data,
        } => {
            // Eager for UDP (a bad local bind is a config error, `StreamOutput::open_path`'s
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
            if let Some(structured_data) = structured_data {
                encoder =
                    encoder.with_structured_data(structured_data.sd_id.clone()).with_context(
                        || format!("component {id:?}: syslog_out structured_data.sd_id is invalid"),
                    )?;
            }
            output = output
                .with_encoder(encoder)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone());
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer, base_dir),
                write_config(&component.buffer),
            )
        }

        StatsdOut {
            endpoint,
            transport,
            format,
            relative_gauges,
            max_packet_bytes,
            connect_timeout,
        } => {
            // Eager for UDP, lazy for TCP -- same reasoning as `SyslogOut` above.
            let output = match transport {
                logit_config::StatsdTransport::Udp => StatsdOutput::udp(endpoint.clone())?,
                logit_config::StatsdTransport::Tcp => {
                    StatsdOutput::tcp(endpoint.clone(), *connect_timeout)
                }
            };
            let encoder =
                StatsdEncoder::new(statsd_format(*format)).with_relative_gauges(*relative_gauges);
            let output = output
                .with_encoder(encoder)
                .with_max_packet_bytes(*max_packet_bytes as usize)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone());
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer, base_dir),
                write_config(&component.buffer),
            )
        }
    };
    Ok((spec, telemetry))
}

/// Builds a sink's `SinkStoreConfig` from its `BufferConfig` (`docs/adr/buffered-sink-delivery.md`
/// / `docs/adr/disk-backed-sink-buffer.md`) -- the sole place `logit_config::OverflowPolicy` is
/// converted to `logit_pipeline::OverflowPolicy`, since neither config nor pipeline crate can see
/// both types without violating the dependency direction (`logit-pipeline` depends on
/// `logit-config`, never the reverse; `docs/design/pipeline-graph.md`'s crate layout).
/// `buffer.disk` present selects `SinkStoreConfig::Disk`; `path` is resolved against `base_dir`
/// exactly like `StdioTarget::Path`/`FileOut::path` above.
fn queue_config(buffer: &BufferConfig, base_dir: &Path) -> SinkStoreConfig {
    match &buffer.disk {
        None => SinkStoreConfig::Memory(SinkQueueConfig {
            max_batches: buffer.max_batches,
            max_bytes: buffer.max_bytes,
            overflow: overflow_policy(buffer.overflow),
        }),
        Some(disk) => SinkStoreConfig::Disk(DiskQueueConfig {
            dir: base_dir.join(&disk.path),
            max_bytes: disk.max_bytes,
            segment_bytes: disk.segment_bytes,
            overflow: overflow_policy(buffer.overflow),
            compression: to_native_compression(disk.compression),
            checkpoint_interval: disk.checkpoint_interval,
        }),
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

/// The sole place `logit_config::RotateConfig` crosses into `logit_outputs::file::RotatePolicy`
/// -- `logit-outputs` never depends on `logit-config` (`docs/design/pipeline-graph.md`'s crate
/// layout), the same reason `overflow_policy`/`delivery_posture`/`syslog_format` exist.
fn to_rotate_policy(cfg: &logit_config::RotateConfig) -> RotatePolicy {
    RotatePolicy {
        max_bytes: cfg.max_bytes,
        interval: cfg.interval.map(to_rotate_interval),
        max_files: cfg.max_files,
    }
}

fn to_rotate_interval(interval: logit_config::RotateInterval) -> OutputRotateInterval {
    match interval {
        logit_config::RotateInterval::Hourly => OutputRotateInterval::Hourly,
        logit_config::RotateInterval::Daily => OutputRotateInterval::Daily,
    }
}

/// The sole place `logit_config::StreamFormat`/`Compression` cross into
/// `logit_outputs::stdio::StreamEncoder` -- same crate-layout reason as `to_rotate_policy` above.
/// `compression` is read regardless of `format`; graph rule 33 already guarantees it's `none`
/// whenever `format` isn't `native`, so ignoring it under `Human` here is never a silent
/// behavior change, just dead weight `resolve` already rejected.
fn to_stream_encoder(
    format: logit_config::StreamFormat,
    compression: logit_config::Compression,
) -> logit_outputs::stdio::StreamEncoder {
    match format {
        logit_config::StreamFormat::Human => logit_outputs::stdio::StreamEncoder::human(),
        logit_config::StreamFormat::Native => {
            logit_outputs::stdio::StreamEncoder::native(to_native_compression(compression))
        }
    }
}

fn to_native_compression(compression: logit_config::Compression) -> NativeCompression {
    match compression {
        logit_config::Compression::None => NativeCompression::None,
        logit_config::Compression::Lz4 => NativeCompression::Lz4,
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
/// 17 already guarantees a non-datagram-, non-tail-listener's `receive` is `ReceiveConfig::
/// default()` by the time a resolved `Graph` reaches `build_spec`, so `internal` always gets
/// `shutdown_grace: ReceiveConfig::default().shutdown_grace` here (5s today, not
/// `Duration::ZERO`) regardless of what any `receive:` block would otherwise say. For `internal`
/// that's harmless, not just unused, only because `InternalInput` never overrides `Input::
/// run_until_shutdown`: the default impl's own `select!` always resolves at t=shutdown against a
/// non-overriding input, so `run_input`'s grace backstop -- built from this value -- never gets a
/// chance to matter. If `internal` ever gains a cooperative drain of its own, this stops being a
/// harmless default and needs its own `receive.shutdown_grace`-shaped knob rather than inheriting
/// whatever `ReceiveConfig::default` happens to say.
///
/// `tail_in`/`docker_in` and, now, `logit_in` are the listeners where this value is genuinely
/// load-bearing rather than incidentally harmless: `TailInput` (`crates/logit-inputs/src/tail/
/// driver.rs`) overrides `run_until_shutdown` to flush every tracked file's accumulator and
/// write a final checkpoint, and `LogitInput` (`crates/logit-inputs/src/logit.rs`) overrides it
/// to close every idle connection with `Reject{GOING_AWAY}` -- either drain must fit inside
/// `shutdown_grace` or `run_input`'s backstop cancels it by drop, losing whatever it hadn't
/// flushed/closed yet. `logit_in` falls under rule 17's non-datagram, non-tail bucket, so unlike
/// `tail_in`/`docker_in` it always gets the fixed 5s default here -- there is no
/// `receive:`-shaped knob to override it with (`docs/known-gaps.md` tracks this as the one
/// currently un-tunable case).
fn input_runtime_config(receive: &logit_config::ReceiveConfig) -> InputRuntimeConfig {
    InputRuntimeConfig { shutdown_grace: receive.shutdown_grace }
}

/// Builds a tailing listener's `TailConfig` from its `TailOptions` plus the shared `receive:`
/// block (`docs/adr/file-tailing-and-docker-json-logs.md`) -- the tail-side mirror of
/// `receive_config` above. `checkpoint_path` is resolved against `base_dir` when relative,
/// exactly like `StdioTarget::Path`/`LuaFile { lua_file, .. }` resolve their own paths.
fn tail_config(
    tail: &logit_config::TailOptions,
    receive: &logit_config::ReceiveConfig,
    base_dir: &Path,
) -> logit_inputs::tail::TailConfig {
    logit_inputs::tail::TailConfig {
        checkpoint_path: tail.checkpoint_path.as_ref().map(|p| base_dir.join(p)),
        read_from: match tail.read_from {
            logit_config::ReadFrom::Beginning => logit_inputs::tail::ReadFrom::Beginning,
            logit_config::ReadFrom::End => logit_inputs::tail::ReadFrom::End,
        },
        watch: match tail.watch {
            logit_config::WatchMode::Auto => logit_inputs::tail::WatchMode::Auto,
            logit_config::WatchMode::Inotify => logit_inputs::tail::WatchMode::Inotify,
            logit_config::WatchMode::Poll => logit_inputs::tail::WatchMode::Poll,
        },
        poll_interval: tail.poll_interval,
        checkpoint_interval: tail.checkpoint_interval,
        max_line_bytes: tail.max_line_bytes as usize,
        batching: logit_inputs::tail::TailBatching {
            max_events: receive.batch_max_events,
            max_bytes: receive.batch_max_bytes,
            flush_interval: receive.batch_flush_interval,
            shutdown_grace: receive.shutdown_grace,
        },
    }
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

/// The sole place `logit_config::StatsdFormat` crosses into `logit_outputs::statsd::Format` --
/// same reasoning as [`syslog_format`].
fn statsd_format(cfg: logit_config::StatsdFormat) -> logit_outputs::statsd::Format {
    match cfg {
        logit_config::StatsdFormat::Dogstatsd => logit_outputs::statsd::Format::DogStatsd,
        logit_config::StatsdFormat::Statsd => logit_outputs::statsd::Format::Statsd,
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

/// Converts config's `span:` block into the transform crate's `SpanLift` -- the same
/// config-vocabulary-to-transform-type mapping `to_signal_set` does, `SpanKindConfig` to
/// `logit_core::SpanKind` included.
fn to_span_lift(span: &logit_config::SpanLiftConfig) -> SpanLift {
    use logit_config::SpanKindConfig;
    SpanLift {
        mint_id: span.mint_id,
        name: span.name.clone(),
        kind: match span.kind {
            SpanKindConfig::Internal => logit_core::SpanKind::Internal,
            SpanKindConfig::Server => logit_core::SpanKind::Server,
            SpanKindConfig::Client => logit_core::SpanKind::Client,
            SpanKindConfig::Producer => logit_core::SpanKind::Producer,
            SpanKindConfig::Consumer => logit_core::SpanKind::Consumer,
        },
        max_skew: span.max_skew,
    }
}

fn to_match_mode(mode: logit_config::MatchMode) -> TransformMatchMode {
    match mode {
        logit_config::MatchMode::AnyOf => TransformMatchMode::AnyOf,
        logit_config::MatchMode::Only => TransformMatchMode::Only,
    }
}

/// `logit_config::Distributions` -> `logit_transforms::Distributions` -- `logit-transforms`
/// deliberately doesn't depend on `logit-config` (`docs/design/pipeline-graph.md`'s crate
/// layout), so this wiring boundary maps every config enum `aggregate` is configured by, the same
/// `to_match_mode` precedent above.
fn to_distributions(mode: logit_config::Distributions) -> TransformDistributions {
    match mode {
        logit_config::Distributions::Sketch => TransformDistributions::Sketch,
        logit_config::Distributions::Samples => TransformDistributions::Samples,
    }
}

/// `logit_config::Sets` -> `logit_transforms::Sets` -- see [`to_distributions`]'s doc comment.
fn to_sets(mode: logit_config::Sets) -> TransformSets {
    match mode {
        logit_config::Sets::Estimate => TransformSets::Estimate,
        logit_config::Sets::Members => TransformSets::Members,
    }
}

/// `logit_config::AggregateTemporality` -> `logit_transforms::AggregateTemporality` -- see
/// [`to_distributions`]'s doc comment for why this mapping exists at all.
fn to_temporality(mode: logit_config::AggregateTemporality) -> TransformTemporality {
    match mode {
        logit_config::AggregateTemporality::Delta => TransformTemporality::Delta,
        logit_config::AggregateTemporality::Cumulative => TransformTemporality::Cumulative,
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

/// Converts config's `TlsClientConfig` (`logit-config`, which `logit-outputs` deliberately
/// doesn't depend on -- `docs/design/pipeline-graph.md`'s crate layout) into the output crate's
/// own identically-shaped `TlsClientSettings`.
fn to_tls_client_settings(
    tls: &logit_config::TlsClientConfig,
) -> logit_outputs::otlp::TlsClientSettings {
    logit_outputs::otlp::TlsClientSettings {
        ca_file: tls.ca_file.clone(),
        cert_file: tls.cert_file.clone(),
        key_file: tls.key_file.clone(),
        insecure_skip_verify: tls.insecure_skip_verify,
    }
}

/// The `logit-inputs` mirror of [`to_tls_client_settings`].
fn to_tls_server_settings(
    tls: &logit_config::TlsServerConfig,
) -> logit_inputs::otlp::TlsServerSettings {
    logit_inputs::otlp::TlsServerSettings {
        cert_file: tls.cert_file.clone(),
        key_file: tls.key_file.clone(),
        client_ca_file: tls.client_ca_file.clone(),
    }
}

/// [`to_tls_client_settings`]'s `logit-inputs` counterpart -- `prometheus_in` is a client, not a
/// sink, so it needs `logit_inputs::prometheus::TlsClientSettings` (built on `reqwest`, per that
/// module's own doc comment) rather than `logit_outputs::otlp::TlsClientSettings`.
fn to_input_tls_client_settings(
    tls: &logit_config::TlsClientConfig,
) -> logit_inputs::prometheus::TlsClientSettings {
    logit_inputs::prometheus::TlsClientSettings {
        ca_file: tls.ca_file.clone(),
        cert_file: tls.cert_file.clone(),
        key_file: tls.key_file.clone(),
        insecure_skip_verify: tls.insecure_skip_verify,
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
///
/// Also the conversion for `has_attributes`/`drop_attributes` (`ComponentKind::HasAttributes`/
/// `DropAttributes`), whose `resource`/`attributes` fields are the identical `SetValue` map shape
/// -- reused unchanged, not reimplemented, so the two config surfaces cannot drift apart on what a
/// `SetValue` becomes.
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
    use logit_config::{Component, ComponentKind, InternalLogs};
    use std::collections::HashMap as Map;
    use std::time::Duration;

    #[test]
    fn every_internal_logs_threshold_is_at_or_above_warn() {
        for logs in [InternalLogs::Warn, InternalLogs::Error, InternalLogs::Off] {
            if let Some(severity) = severity_for_logs(logs) {
                assert!(severity >= logit_core::Severity::Warn, "{logs:?} maps below warn");
            }
        }
    }

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
        Config { components: map, ..Default::default() }
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
        let (_, _, telemetry, _) = prepare(cfg, PathBuf::new()).unwrap();
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
                        logs: logit_config::InternalLogs::default(),
                    },
                },
            ),
            ("out", influxdb_out(vec!["self"])),
        ]);
        let (_, _, telemetry, _) = prepare(cfg, PathBuf::new()).unwrap();
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
                logs: logit_config::InternalLogs::default(),
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
                temporality: logit_config::AggregateTemporality::default(),
                series_retention: 5,
                max_retained_series: 10_000,
                distributions: logit_config::Distributions::default(),
                max_samples_per_series: 1000,
                sets: logit_config::Sets::default(),
                max_set_members_per_series: 1000,
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
                kind: ComponentKind::OtlpIn {
                    bind: "127.0.0.1:0".to_string(),
                    protocol,
                    tls: None,
                },
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
    fn build_spec_builds_a_prometheus_input() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::PrometheusIn {
                targets: vec!["http://127.0.0.1:0/metrics".to_string()],
                interval: Duration::from_secs(15),
                timeout: Duration::from_secs(10),
                headers: HashMap::new(),
                tls: logit_config::TlsClientConfig::default(),
            },
        };
        assert!(matches!(
            build_spec("in", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    #[test]
    fn build_spec_builds_a_tail_input_with_receive_batching_wired() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig {
                batch_max_events: 42,
                ..logit_config::ReceiveConfig::default()
            },
            sources: vec![],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::TailIn {
                paths: vec!["/var/log/app.log".to_string()],
                tail: logit_config::TailOptions::default(),
            },
        };
        assert!(matches!(
            build_spec("in", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    #[test]
    fn build_spec_builds_a_docker_input_with_filter_and_labels_wired() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::DockerIn {
                root: "/var/lib/docker/containers".to_string(),
                containers: vec!["nginx".to_string()],
                discover: false,
                labels: vec!["team".to_string()],
                tail: logit_config::TailOptions::default(),
            },
        };
        assert!(matches!(
            build_spec("in", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    /// `tail_config` is where `checkpoint_path`, `receive:`'s batching fields, and every other
    /// `TailOptions` field actually turn into a `logit_inputs::tail::TailConfig` -- `build_spec`'s
    /// own `TailIn` arm just calls it, and `NodeSpec::Input` boxes the result as `dyn Input`, with
    /// no way to inspect what's inside from the outside. So the conversion logic is exercised
    /// directly here, the same way `queue_config`/`write_config`/`receive_config` already are
    /// implicitly through their own callers -- these are this function's only tests.
    #[test]
    fn tail_config_resolves_a_relative_checkpoint_path_against_base_dir() {
        let tail = logit_config::TailOptions {
            checkpoint_path: Some("state/tail.checkpoint".to_string()),
            ..logit_config::TailOptions::default()
        };
        let cfg =
            tail_config(&tail, &logit_config::ReceiveConfig::default(), Path::new("/etc/logit"));
        assert_eq!(cfg.checkpoint_path, Some(PathBuf::from("/etc/logit/state/tail.checkpoint")));
    }

    #[test]
    fn tail_config_leaves_an_absolute_checkpoint_path_untouched() {
        let tail = logit_config::TailOptions {
            checkpoint_path: Some("/var/lib/logit/tail.checkpoint".to_string()),
            ..logit_config::TailOptions::default()
        };
        let cfg =
            tail_config(&tail, &logit_config::ReceiveConfig::default(), Path::new("/etc/logit"));
        assert_eq!(cfg.checkpoint_path, Some(PathBuf::from("/var/lib/logit/tail.checkpoint")));
    }

    #[test]
    fn tail_config_defaults_to_no_checkpoint() {
        let cfg = tail_config(
            &logit_config::TailOptions::default(),
            &logit_config::ReceiveConfig::default(),
            Path::new("/etc/logit"),
        );
        assert_eq!(cfg.checkpoint_path, None);
    }

    #[test]
    fn tail_config_wires_batching_from_receive() {
        let receive = logit_config::ReceiveConfig {
            batch_max_events: 250,
            batch_max_bytes: 1_000_000,
            batch_flush_interval: Duration::from_millis(250),
            shutdown_grace: Duration::from_secs(7),
            ..logit_config::ReceiveConfig::default()
        };
        let cfg = tail_config(&logit_config::TailOptions::default(), &receive, Path::new(""));
        assert_eq!(cfg.batching.max_events, 250);
        assert_eq!(cfg.batching.max_bytes, 1_000_000);
        assert_eq!(cfg.batching.flush_interval, Duration::from_millis(250));
        assert_eq!(cfg.batching.shutdown_grace, Duration::from_secs(7));
    }

    #[test]
    fn tail_config_converts_read_from_watch_mode_and_the_remaining_tail_options() {
        let tail = logit_config::TailOptions {
            read_from: logit_config::ReadFrom::Beginning,
            watch: logit_config::WatchMode::Inotify,
            poll_interval: Duration::from_millis(500),
            checkpoint_interval: Duration::from_secs(9),
            max_line_bytes: 2048,
            ..logit_config::TailOptions::default()
        };
        let cfg = tail_config(&tail, &logit_config::ReceiveConfig::default(), Path::new(""));
        assert_eq!(cfg.read_from, logit_inputs::tail::ReadFrom::Beginning);
        assert_eq!(cfg.watch, logit_inputs::tail::WatchMode::Inotify);
        assert_eq!(cfg.poll_interval, Duration::from_millis(500));
        assert_eq!(cfg.checkpoint_interval, Duration::from_secs(9));
        assert_eq!(cfg.max_line_bytes, 2048);
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
                    tls: logit_config::TlsClientConfig::default(),
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

    /// An `https://` endpoint under `protocol: grpc` used to be rejected outright (the hand-rolled
    /// gRPC client had no TLS support at all); it's now the normal way to ask for gRPC-over-TLS
    /// (`docs/adr/otlp-tls-and-pooled-grpc-client.md`) and `build_spec` builds it like any other
    /// endpoint.
    #[test]
    fn build_spec_builds_an_otlp_sink_with_https_under_grpc() {
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
                tls: logit_config::TlsClientConfig::default(),
            },
        };
        assert!(matches!(
            build_spec("out", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    /// `logit-cli`'s own home for `testdata/tls`'s fixtures -- `crates/logit-cli` is two levels
    /// under the repo root, same as every other crate's `testdata_dir()` test helper.
    fn testdata_tls_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    #[test]
    fn build_spec_wires_a_tls_client_config_into_an_otlp_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::OtlpOut {
                endpoint: "https://localhost:4318".to_string(),
                protocol: logit_config::OtlpProtocol::Http,
                headers: HashMap::new(),
                paths: logit_config::OtlpPaths::default(),
                compression: logit_config::OtlpCompression::default(),
                tls: logit_config::TlsClientConfig {
                    ca_file: Some("ca.pem".to_string()),
                    ..Default::default()
                },
            },
        };
        assert!(matches!(
            build_spec("out", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    /// `build_spec` (via `OtlpOutput::with_tls`) is where a bad `tls.ca_file` path actually loads
    /// the file and fails -- `graph::resolve`'s rule 22 never touches the filesystem, so it can't
    /// catch this (`docs/deploying.md`'s TLS section documents that `logit validate` doesn't
    /// either, since `validate_semantics` only runs `graph::resolve`).
    #[test]
    fn build_spec_reports_a_missing_tls_ca_file_clearly() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::OtlpOut {
                endpoint: "https://localhost:4318".to_string(),
                protocol: logit_config::OtlpProtocol::Http,
                headers: HashMap::new(),
                paths: logit_config::OtlpPaths::default(),
                compression: logit_config::OtlpCompression::default(),
                tls: logit_config::TlsClientConfig {
                    ca_file: Some("does-not-exist.pem".to_string()),
                    ..Default::default()
                },
            },
        };
        let err = match build_spec("out", &component, &testdata_tls_dir(), None) {
            Ok(_) => panic!("expected a missing tls.ca_file to fail build_spec"),
            Err(err) => err,
        };
        assert!(format!("{err:?}").contains("does-not-exist.pem"), "got: {err:?}");
    }

    #[test]
    fn build_spec_wires_a_tls_server_config_into_an_otlp_input() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::OtlpIn {
                bind: "127.0.0.1:0".to_string(),
                protocol: logit_config::OtlpProtocol::Http,
                tls: Some(logit_config::TlsServerConfig {
                    cert_file: "server.pem".to_string(),
                    key_file: "server.key".to_string(),
                    client_ca_file: None,
                }),
            },
        };
        assert!(matches!(
            build_spec("in", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    #[test]
    fn build_spec_builds_a_logit_input() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::LogitIn {
                bind: "127.0.0.1:0".to_string(),
                tls: None,
                max_frame_bytes: None,
            },
        };
        assert!(matches!(
            build_spec("in", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    #[test]
    fn build_spec_wires_tls_and_max_frame_bytes_into_a_logit_input() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::LogitIn {
                bind: "127.0.0.1:0".to_string(),
                tls: Some(logit_config::TlsServerConfig {
                    cert_file: "server.pem".to_string(),
                    key_file: "server.key".to_string(),
                    client_ca_file: None,
                }),
                max_frame_bytes: Some(32 * 1024 * 1024),
            },
        };
        assert!(matches!(
            build_spec("in", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    #[test]
    fn build_spec_builds_a_logit_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::LogitOut {
                endpoint: "central:5140".to_string(),
                compression: logit_config::Compression::Lz4,
                tls: None,
                request_timeout: Duration::from_secs(10),
            },
        };
        assert!(matches!(
            build_spec("out", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    #[test]
    fn build_spec_wires_a_tls_client_config_into_a_logit_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::LogitOut {
                endpoint: "central:5140".to_string(),
                compression: logit_config::Compression::None,
                tls: Some(logit_config::TlsClientConfig {
                    ca_file: Some("ca.pem".to_string()),
                    ..Default::default()
                }),
                request_timeout: Duration::from_secs(10),
            },
        };
        assert!(matches!(
            build_spec("out", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
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
                disk: None,
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
        let NodeSpec::Output(_, store_config, write_config) =
            build_spec("out", &component, Path::new(""), None).unwrap().0
        else {
            panic!("expected NodeSpec::Output");
        };
        let SinkStoreConfig::Memory(queue_config) = store_config else {
            panic!("expected SinkStoreConfig::Memory, buffer.disk was None");
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
    fn build_spec_wires_a_disk_buffer_into_a_sinkstoreconfig_disk_with_the_path_resolved_against_base_dir(
    ) {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig {
                disk: Some(logit_config::DiskBufferConfig {
                    path: "spool".to_string(),
                    max_bytes: 2 * 1024 * 1024 * 1024,
                    segment_bytes: 128 * 1024 * 1024,
                    compression: logit_config::Compression::Lz4,
                    checkpoint_interval: Duration::from_secs(5),
                }),
                ..logit_config::BufferConfig::default()
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
        let NodeSpec::Output(_, store_config, _) =
            build_spec("out", &component, Path::new("/etc/logit"), None).unwrap().0
        else {
            panic!("expected NodeSpec::Output");
        };
        let SinkStoreConfig::Disk(disk_config) = store_config else {
            panic!("expected SinkStoreConfig::Disk, buffer.disk was Some");
        };
        assert_eq!(disk_config.dir, Path::new("/etc/logit/spool"));
        assert_eq!(disk_config.max_bytes, 2 * 1024 * 1024 * 1024);
        assert_eq!(disk_config.segment_bytes, 128 * 1024 * 1024);
        assert_eq!(disk_config.compression, NativeCompression::Lz4);
        assert_eq!(disk_config.checkpoint_interval, Duration::from_secs(5));
    }

    #[test]
    fn build_spec_builds_a_logfmt_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Logfmt { bare_keys: false },
        };
        assert!(matches!(
            build_spec("parse", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_kv_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Kv {
                pair_sep: "&".to_string(),
                kv_sep: "=".to_string(),
                bare_keys: false,
            },
        };
        assert!(matches!(
            build_spec("parse", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
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
        stdio_out_component_with_format(
            target,
            logit_config::StreamFormat::default(),
            logit_config::Compression::default(),
        )
    }

    fn stdio_out_component_with_format(
        target: StdioTarget,
        format: logit_config::StreamFormat,
        compression: logit_config::Compression,
    ) -> ResolvedComponent {
        ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::StdioOut { target, format, compression },
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

    fn file_out_component(path: &str, rotate: logit_config::RotateConfig) -> ResolvedComponent {
        file_out_component_with_format(
            path,
            rotate,
            logit_config::StreamFormat::default(),
            logit_config::Compression::default(),
        )
    }

    fn file_out_component_with_format(
        path: &str,
        rotate: logit_config::RotateConfig,
        format: logit_config::StreamFormat,
        compression: logit_config::Compression,
    ) -> ResolvedComponent {
        ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec![],
            kind: ComponentKind::FileOut { path: path.to_string(), rotate, format, compression },
        }
    }

    fn size_rotate_config() -> logit_config::RotateConfig {
        logit_config::RotateConfig { max_bytes: Some(1024), interval: None, max_files: 5 }
    }

    #[test]
    fn build_spec_builds_a_file_out_sink() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("logit-build-spec-file-out-test-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let component = file_out_component(&path.display().to_string(), size_rotate_config());
        assert!(matches!(
            build_spec("tap", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn build_spec_builds_a_file_out_sink_with_format_native_and_compression_lz4() {
        let dir = std::env::temp_dir();
        let path =
            dir.join(format!("logit-build-spec-file-out-native-test-{}.log", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let component = file_out_component_with_format(
            &path.display().to_string(),
            size_rotate_config(),
            logit_config::StreamFormat::Native,
            logit_config::Compression::Lz4,
        );
        assert!(matches!(
            build_spec("tap", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));

        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn build_spec_builds_a_stdio_sink_for_stdout_with_format_native() {
        let component = stdio_out_component_with_format(
            StdioTarget::Stdout,
            logit_config::StreamFormat::Native,
            logit_config::Compression::None,
        );
        assert!(matches!(
            build_spec("tap", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    /// A *relative* `path:` must resolve against the config file's own directory (`base_dir`),
    /// exactly as `StdioTarget::Path` already does -- see
    /// `build_spec_resolves_a_relative_stdio_target_against_the_config_base_dir` above.
    #[test]
    fn build_spec_resolves_a_relative_file_out_path_against_the_config_base_dir() {
        let base_dir = std::env::temp_dir()
            .join(format!("logit-build-spec-file-out-base-dir-{}", std::process::id()));
        std::fs::create_dir_all(&base_dir).expect("base_dir should be creatable");
        let relative = "relative-events.log";
        let expected_path = base_dir.join(relative);
        let _ = std::fs::remove_file(&expected_path);

        let component = file_out_component(relative, size_rotate_config());
        assert!(matches!(
            build_spec("tap", &component, &base_dir, None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
        assert!(
            expected_path.exists(),
            "expected the relative path to be created inside base_dir ({}), not the process cwd",
            base_dir.display()
        );

        std::fs::remove_file(&expected_path).ok();
        std::fs::remove_dir(&base_dir).ok();
    }

    #[test]
    fn build_spec_reports_a_clear_path_naming_error_for_an_unopenable_file_out_target() {
        let path = std::env::temp_dir().join("logit-build-spec-file-out-no-such-dir").join("x.log");
        let component = file_out_component(&path.display().to_string(), size_rotate_config());
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
                span: None,
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
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
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

    /// The `span:` block reaches the transform: a config-vocabulary `kind: client` and the
    /// default `name` land on the minted `SpanRecord`, and the event's timestamp becomes the
    /// lifted start -- proving `to_span_lift`'s mapping, not just that the variant builds.
    #[test]
    fn build_spec_builds_a_span_lifting_trace_context_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::TraceContext {
                trace_id: "trace.id".to_string(),
                span_id: Some("span.id".to_string()),
                flags: None,
                keep_source: false,
                span: Some(logit_config::SpanLiftConfig {
                    kind: logit_config::SpanKindConfig::Client,
                    ..logit_config::SpanLiftConfig::default()
                }),
            },
        };
        let NodeSpec::Transform(mut transform) =
            build_spec("trace", &component, Path::new(""), None).unwrap().0
        else {
            panic!("expected a Transform node");
        };

        let mut attrs = logit_core::AttrMap::new();
        attrs.insert("trace.id", logit_core::Value::str("ab".repeat(16)));
        attrs.insert("span.id", logit_core::Value::str("cd".repeat(8)));
        attrs.insert("span.start", logit_core::Value::I64(1_725_000_000_000_000_000));
        attrs.insert("span.duration_ms", logit_core::Value::I64(5));
        let event = logit_core::Event::log(
            1_725_000_000_001_000_000,
            attrs,
            logit_core::LogRecord {
                message: logit_core::Value::str("msg"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let resource = Arc::new(logit_core::Resource::default());
        let out = transform.process(&resource, event).expect("should forward the event");
        let span = out.span.expect("a span should be minted");
        assert_eq!(span.kind, logit_core::SpanKind::Client);
        assert_eq!(span.name.as_str(), Some("http.request"));
        assert_eq!(out.timestamp, 1_725_000_000_000_000_000);
        assert_eq!(span.end_timestamp, 1_725_000_000_005_000_000);
    }

    /// Like `build_spec_builds_a_working_trace_context_transform` above, runs the built transform
    /// against an event rather than only checking the `NodeSpec` variant -- proving the
    /// `BTreeMap<String, f64>` config shape actually reaches `Scale::new` as the expected factor.
    #[test]
    fn build_spec_builds_a_working_scale_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Scale {
                fields: std::collections::BTreeMap::from([("request_time".to_string(), 1000.0)]),
            },
        };
        let NodeSpec::Transform(mut transform) =
            build_spec("scale", &component, Path::new(""), None).unwrap().0
        else {
            panic!("expected a Transform node");
        };

        let mut attrs = logit_core::AttrMap::new();
        attrs.insert("request_time", logit_core::Value::F64(0.012));
        let event = logit_core::Event::log(
            0,
            attrs,
            logit_core::LogRecord {
                message: logit_core::Value::str("msg"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        );
        let resource = Arc::new(logit_core::Resource::default());
        let out = transform.process(&resource, event).expect("should forward the event");
        match out.attributes.get("request_time") {
            Some(logit_core::Value::F64(v)) => assert!((v - 12.0).abs() < 1e-9, "got {v}"),
            other => panic!("expected a scaled F64, got {other:?}"),
        }
    }

    #[test]
    fn build_spec_builds_a_regex_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Regex {
                pattern: r"status=(?P<status>\d+)".to_string(),
                field: None,
            },
        };
        assert!(matches!(
            build_spec("regex", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
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

    #[test]
    fn build_spec_builds_a_has_attributes_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::HasAttributes {
                resource: std::collections::BTreeMap::new(),
                attributes: std::collections::BTreeMap::from([(
                    "stream".to_string(),
                    logit_config::SetValue::Str("a".to_string()),
                )]),
            },
        };
        assert!(matches!(
            build_spec("has_attributes", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_drop_attributes_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::DropAttributes {
                resource: std::collections::BTreeMap::from([(
                    "service.name".to_string(),
                    logit_config::SetValue::Str("nginx".to_string()),
                )]),
                attributes: std::collections::BTreeMap::new(),
            },
        };
        assert!(matches!(
            build_spec("drop_attributes", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_has_provenance_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::HasProvenance {
                origin: vec!["nginx_in".to_string()],
                previous: vec![],
            },
        };
        assert!(matches!(
            build_spec("has_provenance", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_drop_provenance_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            consumers: vec!["out".to_string()],
            kind: ComponentKind::DropProvenance {
                origin: vec![],
                previous: vec!["logit_in".to_string()],
            },
        };
        assert!(matches!(
            build_spec("drop_provenance", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }
}
