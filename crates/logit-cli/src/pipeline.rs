//! `logit run`: resolves a config's component graph (`docs/design/pipeline-graph.md`) into a
//! [`NodeSpec`] per component and hands them to `logit_pipeline::run`.
//!
//! Only the kind → implementation registry lives here: graph resolution/validation
//! (`logit_pipeline::graph`) and the node runtime are in `logit-pipeline`. The free functions
//! below are where each config type crosses into its implementation's own type: `logit-inputs`
//! and `logit-outputs` don't depend on `logit-config` (`docs/design/pipeline-graph.md`'s "Crate
//! layout"), and `logit-transforms` and `logit-pipeline` (`OverflowPolicy`, `DeliveryPosture`)
//! keep their own types.
//!
//! Two check layers: `graph::resolve`, which [`validate_semantics`] runs for `logit validate`, and
//! `build_spec`, which only `logit run` reaches. `build_spec` is where a referenced file is read
//! (TLS material, `lua_file`, `types_db`), a UDP sink binds its local socket, and a check with no
//! graph rule runs (a syslog `sd_id`), so those fail only at `run`.

use crate::config;
use anyhow::Context;
use logit_config::{BufferConfig, Config, StdioTarget};
use logit_core::{Diagnostics, Registry, Telemetry};
use logit_inputs::collectd::CollectdInput;
use logit_inputs::datadog::DatadogInput;
use logit_inputs::datadog_trace::DatadogTraceInput;
use logit_inputs::docker::{ContainerFilter, DockerInput};
use logit_inputs::generate::{GenerateInput, GenerateMetricKind};
use logit_inputs::graphite::GraphiteInput;
use logit_inputs::internal::InternalInput;
use logit_inputs::logit::LogitInput;
use logit_inputs::otlp::{OtlpInput, OtlpTransport as OtlpInTransport};
use logit_inputs::prometheus::{PrometheusInput, PrometheusReceiver};
use logit_inputs::splunk::SplunkHecInput;
use logit_inputs::statsd::StatsdInput;
use logit_inputs::syslog::SyslogInput;
use logit_inputs::tail::TailInput;
use logit_outputs::collectd::CollectdOutput;
use logit_outputs::datadog::{
    DatadogCompression as DatadogOutCompression, DatadogEndpoints as DatadogOutEndpoints,
    DatadogOutput,
};
use logit_outputs::datadog_trace::{
    DatadogTraceCompression as DatadogTraceOutCompression, DatadogTraceOutput, TracerApiForm,
};
use logit_outputs::file::{RotateInterval as OutputRotateInterval, RotatePolicy};
use logit_outputs::graphite::{GraphiteOutput, Transport as GraphiteOutTransport};
use logit_outputs::influxdb::InfluxDbOutput;
use logit_outputs::logit::LogitOutput;
use logit_outputs::null::NullOutput;
use logit_outputs::otlp::{
    OtlpCompression as OtlpOutCompression, OtlpOutput, OtlpTransport as OtlpOutTransport,
    SignalPaths,
};
use logit_outputs::prometheus::{ExposeOutput, PrometheusOutput, RemoteWriteOutput};
use logit_outputs::splunk::{
    SplunkCompression as SplunkOutCompression, SplunkHecOutput,
    DEFAULT_ACK_TIMEOUT as SPLUNK_DEFAULT_ACK_TIMEOUT,
};
use logit_outputs::statsd::{StatsdEncoder, StatsdOutput};
use logit_outputs::stdio::StreamOutput;
use logit_outputs::syslog::{SyslogEncoder, SyslogOutput};
use logit_pipeline::graph::{self, ResolvedComponent};
use logit_pipeline::{
    DiskQueueConfig, Input, InputRuntimeConfig, NodeSpec, Readiness, RetryConfig, RunError,
    SinkQueueConfig, SinkStoreConfig, WriteLoopConfig,
};
use logit_proto::collectd::{CollectdEncoder, TypesDb};
use logit_proto::frame::Compression as NativeCompression;
use logit_proto::graphite::{
    GraphiteEncoder, MultiValue as GraphiteWireMultiValue, Protocol as GraphiteWireProtocol,
    Tags as GraphiteWireTags,
};
use logit_transforms::{
    AggregateTemporality as TransformTemporality, Aggregator, Arrays as TransformArrays, CsvParser,
    Distributions as TransformDistributions, DropAttributes as DropAttributesTransform,
    DropProvenance as DropProvenanceTransform, DropSignals as DropSignalsTransform,
    Fields as TransformFields, Flatten as FlattenTransform,
    HasAttributes as HasAttributesTransform, HasProvenance as HasProvenanceTransform,
    HasSignal as HasSignalTransform, HttpAccess as HttpAccessTransform, HttpAccessConfig,
    IdFormat as TraceIdFormatTransform, InvalidUtf8 as TransformInvalidUtf8, JsonParser,
    Keep as KeepTransform, KeepSignals as KeepSignalsTransform, KeepValues as KeepValuesTransform,
    Kv as KvTransform, KvMetrics as KvMetricsTransform, Logfmt as LogfmtTransform,
    MatchMode as TransformMatchMode, Normalize as TransformNormalize, RegexParser,
    Remove as RemoveTransform, Route as RouteTransform, Sample as SampleTransform,
    Scale as ScaleTransform, Set as SetTransform, Sets as TransformSets, Shape as ShapeTransform,
    SignalSet, SpanLift, TraceContext as TraceContextTransform,
};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A config's `internal` component: what [`run_pipelines`] needs to
/// [`logit_core::TelemetryLayer::activate`] the layer. Absent when there's no `internal`, in which
/// case no `Registry` is built and the layer is never activated.
pub struct InternalInfo {
    pub registry: Arc<Registry>,
    pub id: String,
    pub logs: logit_config::InternalLogs,
}

/// `InternalInfo::logs` -> the threshold [`logit_core::TelemetryLayer::activate`] takes; `None`
/// for `off`, which never activates the layer.
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
/// SIGTERM/SIGINT (Ctrl-C on non-Unix) starts a graceful drain: every listener stops, closing its
/// downstream inboxes and triggering each node's close-time flush, so an in-flight `aggregate`
/// window is emitted rather than lost. A second signal before the drain finishes exits at once
/// with code 130, so a wedged drain stays killable by the signal that started it.
///
/// Every failure before the pipeline reports ready is `RunError::Startup` (exit 1); after, it's a
/// runtime failure (exit 2). See `docs/deploying.md`'s "Probes and exit codes".
///
/// `telemetry_layer` is `main`'s handle, installed inactive by `init_logging` and activated here
/// once the config's `internal` component is known.
pub async fn run_pipelines(
    path: PathBuf,
    telemetry_layer: logit_core::TelemetryLayer,
) -> Result<(), RunError> {
    let config = config::load(&path).map_err(RunError::Startup)?;

    // `starting`/`exiting` are stable lifecycle event names for log-based alerting
    // (`docs/deploying.md`'s "Self-logging"). Logged before `prepare`, which can still reject the
    // config, so a config that fails resolution still leaves a `starting` line.
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

    // Every concurrent listener on a signal kind is notified, so this doesn't consume the one
    // `run_with_telemetry` races on. Aborted once that returns.
    let kill_switch = tokio::spawn(async {
        shutdown_signal().await;
        shutdown_signal().await;
        std::process::exit(130);
    });

    // The admin listener binds here, before `run_with_telemetry` spawns anything, so a bind
    // failure is `RunError::Startup`: the guarantee `Input::bind`'s pre-pass gives every listener.
    let (readiness, admin_server) = match admin_bind {
        Some(bind) => {
            let listener = tokio::net::TcpListener::bind(&bind)
                .await
                .with_context(|| format!("admin: binding '{bind}'"))
                .map_err(RunError::Startup)?;
            let (readiness, readiness_rx) = Readiness::channel();
            // No shutdown listener of its own: the drain a signal starts is the window `/readyz`
            // answers `503 draining` in, and a refused connection then looks like a crash. The
            // `abort()` below, after `run_with_telemetry` returns, is the only teardown.
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

/// [`prepare`]'s return type, named for clippy's `type_complexity`.
type PrepareResult =
    (graph::Graph, HashMap<String, NodeSpec>, HashMap<String, Telemetry>, Option<InternalInfo>);

/// Resolves a config into a `Graph`, a built `NodeSpec` and a [`Telemetry`] handle per component,
/// and the config's [`InternalInfo`].
///
/// Every handle is [`Telemetry::default`] (a no-op) unless `config` has an `internal` component.
/// Then one process-wide [`Registry`] is built, and each component's handle serves both its own
/// instrumentation (`build_spec`, layer 3) and the node runtime's (layer 2), so both drain from
/// one buffer. See `docs/design/internal-telemetry.md`.
fn prepare(config: Config, base_dir: PathBuf) -> anyhow::Result<PrepareResult> {
    let graph = graph::resolve(config)?;

    // Graph rule 13 allows at most one `internal`; its `span_sample_rate` configures the registry.
    let internal_component = graph.components.iter().find_map(|(id, c)| match &c.kind {
        logit_config::ComponentKind::Internal { span_sample_rate, logs, .. } => {
            Some((id.clone(), *span_sample_rate, *logs))
        }
        _ => None,
    });
    let registry: Option<Arc<Registry>> =
        internal_component.as_ref().map(|(_, rate, _)| Registry::with_span_sampling(*rate));

    // Sorted, so with two broken components the same one reports first on every run, and
    // components register with the `Registry` (and so drain) in a stable order.
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

/// Waits for one SIGTERM or SIGINT (Ctrl-C on non-Unix). Each call installs its own listener, and
/// `run_pipelines` relies on its three calls each being notified.
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

/// [`run_pipelines`] for an in-memory `Config`, with no signal handler to race a test.
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

/// The graph checks `logit run` makes before building anything, shared with `logit validate`.
///
/// Doesn't call `build_spec`, so its checks (a referenced file, a syslog `sd_id`) pass here and
/// fail only at `run`.
pub fn validate_semantics(config: Config) -> anyhow::Result<()> {
    graph::resolve(config)?;
    Ok(())
}

/// Builds the boxed implementation the node runtime runs for one resolved component.
///
/// The match is exhaustive with no fallback arm: adding a `ComponentKind` variant doesn't compile
/// until it has an arm here. Graph rule 8 rejects any kind `is_implemented` doesn't list before
/// this runs.
///
/// `id` names the component's [`Diagnostics`] (`docs/adr/service-lifecycle-and-output-retry.md`).
/// `registry` is `Some` only when the config has an `internal` component; otherwise every
/// [`Telemetry`] handle is the no-op default. The handle goes on a component's `Diagnostics`, so
/// every `warn_throttled` call is also a metric, and on the component itself where it records
/// points of its own. See `docs/design/internal-telemetry.md`.
fn build_spec(
    id: &str,
    component: &ResolvedComponent,
    base_dir: &Path,
    registry: Option<&Arc<Registry>>,
) -> anyhow::Result<(NodeSpec, Telemetry)> {
    use logit_config::ComponentKind::*;
    // Every arm clones this rather than moving it: it's returned too, so the node runtime's layer 2
    // points land in the same buffer as the component's layer 3.
    let telemetry: Telemetry = registry
        .map(|r| r.telemetry_for(id, component.kind_name(), component.role().as_str()))
        .unwrap_or_default();
    let spec = match &component.kind {
        // The transport picks the constructor and the `receive:` translation: a TCP listener has
        // no receive queue, so it takes `tcp_receive_config`, not `receive_config` (graph rule 17).
        // `tls:` is TCP-only: rules 43 and 65 reject it elsewhere, and `with_tls` refuses it again.
        StatsdIn { bind, transport, tls, handshake_timeout, idle_timeout } => {
            let mut input = match transport {
                logit_config::StatsdTransport::Udp => {
                    StatsdInput::new(bind.clone()).with_receive(receive_config(&component.receive))
                }
                logit_config::StatsdTransport::Tcp => StatsdInput::tcp(bind.clone())
                    .with_tcp_receive(tcp_receive_config(&component.receive)),
                // The Unix transports take the same two translations: `unix` is a datagram
                // listener, `unix_stream` a stream one (rules 17 and 65).
                logit_config::StatsdTransport::Unix => {
                    StatsdInput::unix(bind).with_receive(receive_config(&component.receive))
                }
                logit_config::StatsdTransport::UnixStream => StatsdInput::unix_stream(bind)
                    .with_tcp_receive(tcp_receive_config(&component.receive)),
            }
            .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry.clone())
            // No-ops under UDP, where rules 45 and 53 reject a value.
            .with_handshake_timeout(*handshake_timeout)
            .with_idle_timeout(*idle_timeout);
            if let Some(tls) = tls {
                input = input.with_tls(&to_tls_server_settings(tls), base_dir)?;
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        // `types_db` paths resolve against the config file's directory, like every path here, and
        // are read at startup: a bad file stops the process before it reports ready rather than
        // leaving a listener with no data-source names.
        CollectdIn { bind, types_db } => {
            let mut input = CollectdInput::new(bind.clone())
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                .with_receive(receive_config(&component.receive));
            if !types_db.is_empty() {
                let paths: Vec<PathBuf> = types_db.iter().map(|p| base_dir.join(p)).collect();
                let loaded = TypesDb::load(&paths)
                    .with_context(|| format!("component '{id}': loading types_db"))?;
                input = input.with_types_db(Arc::new(loaded));
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        // `GraphiteInput` picks its own driver from `transport`, so `with_receive` is safe on
        // either: rule 17 rejects the queue fields under TCP, leaving only the batch-assembly half
        // the stream driver reads. The two timeouts are no-ops under UDP (rules 45 and 53 reject a
        // value there); `tls:` is TCP-only (rule 43), and `with_tls` refuses it again.
        GraphiteIn {
            bind,
            transport,
            protocol,
            tls,
            handshake_timeout,
            idle_timeout,
            max_line_bytes,
            max_frame_bytes,
        } => {
            let mut input = GraphiteInput::new(
                bind.clone(),
                graphite_transport(*transport),
                graphite_protocol(*protocol),
            )
            .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry.clone())
            .with_receive(receive_config(&component.receive))
            .with_max_line_bytes(*max_line_bytes as usize)
            .with_max_frame_bytes(*max_frame_bytes as usize)
            .with_handshake_timeout(*handshake_timeout)
            .with_idle_timeout(*idle_timeout);
            if let Some(tls) = tls {
                input = input.with_tls(&to_tls_server_settings(tls), base_dir)?;
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        // The `StatsdIn` arm's shape (`docs/adr/syslog-tcp-ingress-and-tls.md`).
        SyslogIn { bind, transport, tls, handshake_timeout, idle_timeout } => {
            let mut input = match transport {
                logit_config::SyslogTransport::Udp => {
                    SyslogInput::new(bind.clone()).with_receive(receive_config(&component.receive))
                }
                logit_config::SyslogTransport::Tcp => SyslogInput::tcp(bind.clone())
                    .with_tcp_receive(tcp_receive_config(&component.receive)),
            }
            .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
            .with_telemetry(telemetry.clone())
            // No-ops under UDP, where rules 45 and 53 reject a value.
            .with_handshake_timeout(*handshake_timeout)
            .with_idle_timeout(*idle_timeout);
            if let Some(tls) = tls {
                input = input.with_tls(&to_tls_server_settings(tls), base_dir)?;
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        OtlpIn { bind, protocol, tls, handshake_timeout, idle_timeout } => {
            let mut input = OtlpInput::new(bind.clone(), otlp_in_transport(*protocol))
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                // `OtlpInput` reads `handshake_timeout` twice: as the pre-request budget and as
                // the grace an idle close gives `hyper` (`docs/adr/idle-connection-timeout.md`).
                .with_handshake_timeout(*handshake_timeout)
                .with_idle_timeout(*idle_timeout);
            if let Some(tls) = tls {
                input = input.with_tls(&to_tls_server_settings(tls), base_dir)?;
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        DatadogIn { bind, tls, api_keys, handshake_timeout, idle_timeout } => {
            let mut input = DatadogInput::new(bind.clone())
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                // Read twice, as on `otlp_in`: the pre-request budget and an idle close's grace.
                .with_handshake_timeout(*handshake_timeout)
                .with_idle_timeout(*idle_timeout)
                .with_api_keys(api_keys.clone());
            if let Some(tls) = tls {
                input = input.with_tls(&to_tls_server_settings(tls), base_dir)?;
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        SplunkHecIn { bind, tls, tokens, max_request_bytes, handshake_timeout, idle_timeout } => {
            let mut input = SplunkHecInput::new(bind.clone())
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                // Read twice, as on `otlp_in`: the pre-request budget and an idle close's grace.
                .with_handshake_timeout(*handshake_timeout)
                .with_idle_timeout(*idle_timeout)
                .with_tokens(tokens.clone())
                // Saturates on a 32-bit target: a cap past the address space is no cap.
                .with_max_request_bytes(usize::try_from(*max_request_bytes).unwrap_or(usize::MAX));
            if let Some(tls) = tls {
                input = input.with_tls(&to_tls_server_settings(tls), base_dir)?;
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        // Graph rule 64 guarantees at least one of `bind`/`socket`, and `tls` only with `bind`.
        DatadogTraceIn { bind, socket, tls, handshake_timeout, idle_timeout } => {
            let mut input = DatadogTraceInput::new()
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                // Read twice, as on `otlp_in`: the pre-request budget and an idle close's grace.
                .with_handshake_timeout(*handshake_timeout)
                .with_idle_timeout(*idle_timeout);
            if let Some(bind) = bind {
                input = input.with_bind(bind.clone());
            }
            if let Some(socket) = socket {
                input = input.with_socket(socket);
            }
            if let Some(tls) = tls {
                input = input.with_tls(&to_tls_server_settings(tls), base_dir)?;
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }
        // Graph rule 55 guarantees exactly one of `scrape_targets`/`bind`, so this dispatches on
        // `bind`. The modes are two types, not one enum: a scrape client and an HTTP listener
        // share no field or builder, and `NodeSpec::Input` boxes either.
        PrometheusIn {
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
            let input: Box<dyn Input + Send> = match bind {
                Some(bind) => {
                    let mut receiver = PrometheusReceiver::new(bind.clone(), path.clone())
                        .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                        .with_telemetry(telemetry.clone())
                        .with_idle_timeout(*idle_timeout)
                        // Unconditional: the receiver reads `max_families: 0` as off, and rule 55
                        // rejects a zero `ttl`.
                        .with_metadata_cache(metadata_cache.max_families, metadata_cache.ttl);
                    if let Some(bind_tls) = bind_tls {
                        receiver =
                            receiver.with_bind_tls(&to_tls_server_settings(bind_tls), base_dir)?;
                    }
                    Box::new(receiver)
                }
                None => Box::new(
                    PrometheusInput::new(scrape_targets.clone(), *interval)
                        .with_timeout(*timeout)
                        .with_headers(headers)?
                        .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                        .with_telemetry(telemetry.clone())
                        .with_tls(&to_input_tls_client_settings(scrape_tls), base_dir)?,
                ),
            };
            NodeSpec::Input(input, input_runtime_config(&component.receive))
        }
        LogitIn { bind, tls, max_frame_bytes, handshake_timeout, idle_timeout } => {
            let mut input = LogitInput::new(bind.clone())
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                .with_handshake_timeout(*handshake_timeout)
                .with_idle_timeout(*idle_timeout);
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
        // `prepare` reads `span_sample_rate` to build the `Registry`, and `logs` decides whether
        // `run_pipelines` activates the `TelemetryLayer`.
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

        // Rule 42 has already parsed every template and checked its placeholders, so these `?`s
        // don't fire. They stay errors, not `expect`s, so a rule and resolver that drift apart fail
        // startup instead of panicking.
        GenerateIn { count, batch, rate, event, resource } => {
            let mut input = GenerateInput::new(*count, *batch)
                .with_rate(*rate)
                .with_resource(resource.clone())?
                .with_diagnostics(Diagnostics::new(id))
                .with_telemetry(telemetry.clone());
            if let Some(log) = &event.log {
                input = input.with_log(parse_generate_template(id, "event.log", log)?)?;
            }
            for (key, value) in &event.attributes {
                let field = format!("event.attributes.{key}");
                input = input.with_attribute(key, parse_generate_template(id, &field, value)?)?;
            }
            if let Some(metric) = &event.metric {
                input = input.with_metric(
                    parse_generate_template(id, "event.metric.name", &metric.name)?,
                    generate_metric_kind(metric.kind),
                    metric.value,
                )?;
            }
            NodeSpec::Input(Box::new(input), input_runtime_config(&component.receive))
        }

        // The runtime reads a Lua router's `targets:` off the `ResolvedComponent`, not
        // `NodeSpec::Lua` (`docs/adr/target-components.md`).
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
        Json { skip_to_brace, invalid_utf8 } => NodeSpec::Transform(Box::new(
            JsonParser::new(*skip_to_brace)
                .with_invalid_utf8(to_invalid_utf8(*invalid_utf8))
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
        TraceContext { format, trace_id, span_id, flags, trace_id_high, keep_source, span } => {
            let (trace_id, span_id, flags) = format.resolve_fields(trace_id, span_id, flags);
            let id_format = match format {
                logit_config::TraceIdFormat::Otel => TraceIdFormatTransform::Otel,
                logit_config::TraceIdFormat::Datadog => {
                    TraceIdFormatTransform::Datadog { trace_id_high: trace_id_high.clone() }
                }
            };
            let mut transform = TraceContextTransform::new(trace_id, span_id, flags, *keep_source)
                .with_format(id_format)
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
        // Rule 31 has already compiled this pattern, so the `?` doesn't fire.
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
        KeepValues { resource, attributes } => NodeSpec::Transform(Box::new(
            KeepValuesTransform::new(to_allow_lists(resource), to_allow_lists(attributes))
                .with_telemetry(telemetry.clone()),
        )),
        // `with_name(id)` becomes the `tap` tag, so two `shape` taps feeding one `aggregate` stay
        // distinct series (`docs/adr/shape-observer-component.md`).
        Shape { interval, resource, max_tracked_keys, max_tracked_keysets } => {
            NodeSpec::Transform(Box::new(
                ShapeTransform::new(*interval)
                    .with_resource_kept(matches!(resource, logit_config::ShapeResource::Keep))
                    .with_caps(*max_tracked_keys, *max_tracked_keysets)
                    .with_name(id)
                    .with_telemetry(telemetry.clone()),
            ))
        }
        Flatten { attributes, resource, arrays } => NodeSpec::Transform(Box::new(
            FlattenTransform::new(
                to_flatten_fields(attributes),
                to_flatten_fields(resource),
                to_flatten_arrays(*arrays),
            )
            .with_telemetry(telemetry.clone()),
        )),
        // Rule 60 has compiled every configured pattern and `http_access`'s tests compile the
        // built-in ones, so the `?` doesn't fire.
        HttpAccess {
            routes,
            route_other,
            user_agent_rules,
            max_length,
            redact_query,
            forwarded,
        } => NodeSpec::Transform(Box::new(
            HttpAccessTransform::new(to_http_access_config(
                routes,
                route_other,
                user_agent_rules,
                max_length,
                redact_query,
                *forwarded,
            ))?
            .with_telemetry(telemetry.clone())
            .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone())),
        )),
        // Rule 61 has already checked every field; the transform never fails to build.
        Sample { rate, key, missing, always_keep } => NodeSpec::Transform(Box::new(
            SampleTransform::new(
                *rate,
                key.as_ref().map(to_sample_key),
                to_sample_missing(*missing),
                always_keep.as_ref().map(to_sample_override),
            )
            .with_telemetry(telemetry.clone()),
        )),
        // The config's `Vec<String>`s pass straight through; the transform interns them.
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
        DatadogOut { api_key, site, endpoints, compression, timeout, headers, tls } => {
            let output = DatadogOutput::new(api_key)?
                .with_site(site.clone())
                .with_endpoints(DatadogOutEndpoints {
                    api: endpoints.api.clone(),
                    logs: endpoints.logs.clone(),
                    traces: endpoints.traces.clone(),
                })
                .with_compression(match compression {
                    logit_config::DatadogCompression::Gzip => DatadogOutCompression::Gzip,
                    logit_config::DatadogCompression::None => DatadogOutCompression::None,
                })
                .with_timeout(*timeout)
                .with_headers(headers)?
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                .with_tls(&to_tls_client_settings(tls), base_dir)?;
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer, base_dir),
                write_config(&component.buffer),
            )
        }
        DatadogTraceOut { endpoint, socket, version, compression, timeout, headers, tls } => {
            // Rule 67 has already required one of `endpoint`/`socket`, not both.
            let output = match (endpoint, socket) {
                (Some(endpoint), _) => DatadogTraceOutput::http(endpoint.clone()),
                (None, Some(socket)) => DatadogTraceOutput::unix(socket),
                (None, None) => anyhow::bail!("datadog_trace_out needs 'endpoint' or 'socket'"),
            };
            let output = output
                .with_version(match version {
                    logit_config::DatadogTraceVersion::V04 => TracerApiForm::V04,
                    logit_config::DatadogTraceVersion::V07 => TracerApiForm::V07,
                })
                .with_compression(match compression {
                    logit_config::DatadogTraceCompression::None => DatadogTraceOutCompression::None,
                    logit_config::DatadogTraceCompression::Gzip => DatadogTraceOutCompression::Gzip,
                })
                .with_timeout(*timeout)
                .with_headers(headers)?
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone())
                .with_tls(&to_tls_client_settings(tls), base_dir)?;
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer, base_dir),
                write_config(&component.buffer),
            )
        }
        SplunkHecOut {
            endpoint,
            token,
            compression,
            multi_value,
            ack,
            ack_timeout,
            timeout,
            tls,
            max_body_bytes,
        } => {
            let output = SplunkHecOutput::new(endpoint.clone(), token)?
                .with_compression(splunk_compression(*compression))
                .with_multi_value(splunk_multi_value(*multi_value))
                // Rule 70 allows `ack_timeout` only with `ack`; the default applies under `ack`.
                .with_ack(*ack, ack_timeout.unwrap_or(SPLUNK_DEFAULT_ACK_TIMEOUT))
                .with_timeout(*timeout)
                // Saturates on a 32-bit target: a cap past the address space is no cap.
                .with_max_body_bytes(usize::try_from(*max_body_bytes).unwrap_or(usize::MAX))
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
                // Both `logit` and `otlp` re-export `logit_outputs::tls::TlsClientSettings`.
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
                // Relative to the config file's directory, not the working directory, as
                // `StdioTarget` documents; `Path::join` leaves an absolute path untouched.
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
            // Relative to the config file's directory, as `StdioTarget::Path` is.
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
            tls,
        } => {
            // Eager for UDP, so a bad local bind is a startup error; that needs a tokio runtime,
            // which `logit run` always has here. Lazy for TCP (`logit_outputs::syslog::Conn`).
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
            // TCP only (RFC 5425; rule 44 rejects `tls:` under UDP). After `with_diagnostics`, so
            // the `insecure_skip_verify` warning lands on this component's diagnostics.
            if let (logit_config::SyslogTransport::Tcp, Some(tls)) = (transport, tls) {
                output = output.with_tls(&to_tls_client_settings(tls), base_dir)?;
            }
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
            tls,
        } => {
            // Eager for UDP, lazy for the Unix and stream transports. A Unix datagram send's wait
            // on a full receiver is bounded by `connect_timeout`.
            let output = match transport {
                logit_config::StatsdTransport::Udp => StatsdOutput::udp(endpoint.clone())?,
                logit_config::StatsdTransport::Tcp => {
                    StatsdOutput::tcp(endpoint.clone(), *connect_timeout)
                }
                logit_config::StatsdTransport::Unix => {
                    StatsdOutput::unix_datagram(endpoint.clone(), *connect_timeout)
                }
                logit_config::StatsdTransport::UnixStream => {
                    StatsdOutput::unix_stream(endpoint.clone(), *connect_timeout)
                }
            };
            let encoder =
                StatsdEncoder::new(statsd_format(*format)).with_relative_gauges(*relative_gauges);
            let mut output = output
                .with_encoder(encoder)
                .with_max_packet_bytes(*max_packet_bytes as usize)
                .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                .with_telemetry(telemetry.clone());
            // TCP only (rule 52), after `with_diagnostics`, as `SyslogOut`.
            if let (logit_config::StatsdTransport::Tcp, Some(tls)) = (transport, tls) {
                output = output.with_tls(&to_tls_client_settings(tls), base_dir)?;
            }
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer, base_dir),
                write_config(&component.buffer),
            )
        }

        CollectdOut { endpoint, max_packet_bytes, hostname } => {
            // Eager UDP bind, as `SyslogOut`; collectd's `network` protocol has no TCP mode.
            let output = CollectdOutput::udp(endpoint.clone())?;
            let mut encoder = CollectdEncoder::new();
            if let Some(hostname) = hostname {
                encoder = encoder.with_hostname(hostname.clone());
            }
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

        GraphiteOut {
            endpoint,
            transport,
            protocol,
            tags,
            multi_value,
            max_packet_bytes,
            max_frame_bytes,
            connect_timeout,
        } => {
            // Eager for UDP, lazy for TCP, as `SyslogOut`.
            let output = match graphite_out_transport(*transport) {
                GraphiteOutTransport::Udp => GraphiteOutput::udp(endpoint.clone())?,
                GraphiteOutTransport::Tcp => {
                    GraphiteOutput::tcp(endpoint.clone(), *connect_timeout)
                }
            };
            let encoder = GraphiteEncoder::new()
                .with_protocol(graphite_out_protocol(*protocol))
                .with_tags(graphite_tags(*tags))
                .with_multi_value(graphite_multi_value(*multi_value))
                .with_max_frame_bytes(*max_frame_bytes as usize);
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

        PrometheusOut {
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
            // Graph rule 56 guarantees exactly one mode field; `PrometheusOutput` carries the
            // choice as a variant from here on.
            let output: PrometheusOutput = match (bind, endpoint) {
                (Some(bind), _) => {
                    // Not bound here: the runtime's pre-spawn `Output::bind` pass opens it, so an
                    // address in use is a startup failure naming this component.
                    ExposeOutput::new(bind.clone())
                        .with_path(path.clone())
                        .with_expire_after(*expire_after)
                        .with_max_series(*max_series)
                        .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                        .with_telemetry(telemetry.clone())
                        .into()
                }
                (None, Some(endpoint)) => RemoteWriteOutput::new(endpoint.clone())
                    .with_version(to_remote_write_version(*version))
                    .with_compression(to_remote_write_encoding(*compression))
                    .with_timeout(*timeout)
                    .with_diagnostics(Diagnostics::new(id).with_telemetry(telemetry.clone()))
                    .with_telemetry(telemetry.clone())
                    .with_headers(headers)?
                    .with_tls(&to_tls_client_settings(endpoint_tls), base_dir)?
                    .into(),
                // Unreachable behind rule 56. An error, not a panic: `build_spec` is callable
                // without `graph::resolve` having run.
                (None, None) => anyhow::bail!(
                    "component '{id}': prometheus_out needs exactly one of 'bind' or 'endpoint'"
                ),
            };
            NodeSpec::Output(
                Box::new(output),
                queue_config(&component.buffer, base_dir),
                write_config(&component.buffer),
            )
        }

        // A load-test sink (`docs/adr/load-test-harness.md`) that still honours `buffer:`, disk
        // included, like every other sink.
        NullOut {} => NodeSpec::Output(
            Box::new(NullOutput),
            queue_config(&component.buffer, base_dir),
            write_config(&component.buffer),
        ),

        // Nothing to build (`docs/adr/target-components.md`'s "Runtime: a target is a zero-cost
        // alias"): the runtime's pre-spawn pass gives it a `Fanout`. `NodeSpec::Target` keeps the
        // registry at one spec per component.
        Target {} => NodeSpec::Target,

        // `Route::new` resolves each `routes:` value to a slot in `component.targets` once (rules
        // 48 and 51 guarantee each resolves). No `with_telemetry`: `route` records no layer-3
        // points (`logit_transforms::route`'s module doc).
        Route { by, routes } => {
            NodeSpec::Router(Box::new(RouteTransform::new(by.clone(), routes, &component.targets)))
        }
    };
    Ok((spec, telemetry))
}

/// A sink's `SinkStoreConfig` from its `BufferConfig` (`docs/adr/buffered-sink-delivery.md`,
/// `docs/adr/disk-backed-sink-buffer.md`). `buffer.disk` selects `SinkStoreConfig::Disk`, its
/// `path` relative to the config file's directory.
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

/// A sink's `WriteLoopConfig` from its `BufferConfig`. `base_delay` isn't config-exposed, so it
/// keeps `RetryConfig::default()`'s value.
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

/// Config's `OtlpProtocol` into `logit-inputs`'s own copy (see the module doc).
fn otlp_in_transport(protocol: logit_config::OtlpProtocol) -> OtlpInTransport {
    match protocol {
        logit_config::OtlpProtocol::Http => OtlpInTransport::Http,
        logit_config::OtlpProtocol::Grpc => OtlpInTransport::Grpc,
    }
}

/// Parses one `generate_in` template, naming its config path (`event.log`,
/// `event.attributes.host`) in any error. Rule 42 has already parsed it (the `GenerateIn` arm).
fn parse_generate_template(
    id: &str,
    field: &str,
    raw: &str,
) -> anyhow::Result<logit_core::template::Template> {
    logit_core::template::parse(raw)
        .map_err(|err| anyhow::anyhow!("component '{id}': '{field}' is not a template: {err}"))
}

/// Config's `GenerateMetricKind` into `logit-inputs`'s own copy.
fn generate_metric_kind(kind: logit_config::GenerateMetricKind) -> GenerateMetricKind {
    match kind {
        logit_config::GenerateMetricKind::Sum => GenerateMetricKind::Sum,
        logit_config::GenerateMetricKind::Gauge => GenerateMetricKind::Gauge,
        logit_config::GenerateMetricKind::Distribution => GenerateMetricKind::Distribution,
    }
}

/// The `logit-outputs` mirror of [`otlp_in_transport`].
fn otlp_out_transport(protocol: logit_config::OtlpProtocol) -> OtlpOutTransport {
    match protocol {
        logit_config::OtlpProtocol::Http => OtlpOutTransport::Http,
        logit_config::OtlpProtocol::Grpc => OtlpOutTransport::Grpc,
    }
}

/// Config's `OtlpCompression` into `logit-outputs`'s own copy.
fn to_otlp_compression(compression: logit_config::OtlpCompression) -> OtlpOutCompression {
    match compression {
        logit_config::OtlpCompression::None => OtlpOutCompression::None,
        logit_config::OtlpCompression::Gzip => OtlpOutCompression::Gzip,
    }
}

/// Config's `RotateConfig` into `logit_outputs::file::RotatePolicy`.
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

/// Config's `StreamFormat`/`Compression` into a `StreamEncoder`. `compression` is ignored under
/// `Human`, where graph rule 33 guarantees it's `none`.
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

/// A UDP listener's `UdpListenerConfig` from its `ReceiveConfig`
/// (`docs/adr/decoupled-listener-io.md`).
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
        read_batch: receive.read_batch,
    }
}

/// A TCP listener's `TcpListenerConfig` from the same `receive:` block
/// (`docs/adr/syslog-tcp-ingress-and-tls.md`).
///
/// The queue fields don't cross: a TCP listener has no receive queue, flow control being its
/// backpressure, and graph rule 17 rejects them. The batching fields apply per connection.
fn tcp_receive_config(
    receive: &logit_config::ReceiveConfig,
) -> logit_inputs::tcp::TcpListenerConfig {
    logit_inputs::tcp::TcpListenerConfig {
        batch_max_events: receive.batch_max_events,
        batch_max_bytes: receive.batch_max_bytes,
        batch_flush_interval: receive.batch_flush_interval,
        shutdown_grace: receive.shutdown_grace,
    }
}

/// Any listener's `InputRuntimeConfig` from its `ReceiveConfig`; safe on every `NodeSpec::Input`
/// arm.
///
/// `shutdown_grace` bounds each listener's `run_until_shutdown` before `run_input`'s backstop
/// cancels it by drop. It matters where that's overridden: `TailInput` flushes every file and
/// writes a final checkpoint, `LogitInput` closes idle connections with `Reject{GOING_AWAY}`, and
/// `InternalInput` drains its buffered points once more. Graph rule 17 forces a non-datagram,
/// non-tail listener's `receive` to the default, so `logit_in` and `internal` always get
/// `ReceiveConfig::default()`'s 5s, with no knob (`docs/known-gaps.md`).
fn input_runtime_config(receive: &logit_config::ReceiveConfig) -> InputRuntimeConfig {
    InputRuntimeConfig { shutdown_grace: receive.shutdown_grace }
}

/// A tailing listener's `TailConfig` from its `TailOptions` plus the `receive:` block, whose
/// `batch_*` fields and `shutdown_grace` become `TailBatching`
/// (`docs/adr/file-tailing-and-docker-json-logs.md`). A relative `checkpoint_path` resolves
/// against the config file's directory.
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

/// Config's `SyslogFormat` into `logit_outputs::syslog::Format`.
fn syslog_format(cfg: logit_config::SyslogFormat) -> logit_outputs::syslog::Format {
    match cfg {
        logit_config::SyslogFormat::Rfc3164 => logit_outputs::syslog::Format::Rfc3164,
        logit_config::SyslogFormat::Rfc5424 => logit_outputs::syslog::Format::Rfc5424,
    }
}

/// Config's `StatsdFormat` into `logit_outputs::statsd::Format`.
fn statsd_format(cfg: logit_config::StatsdFormat) -> logit_outputs::statsd::Format {
    match cfg {
        logit_config::StatsdFormat::Dogstatsd => logit_outputs::statsd::Format::DogStatsd,
        logit_config::StatsdFormat::Statsd => logit_outputs::statsd::Format::Statsd,
    }
}

/// Config's `GraphiteTransport` into `logit_inputs::graphite::Transport`.
fn graphite_transport(cfg: logit_config::GraphiteTransport) -> logit_inputs::graphite::Transport {
    match cfg {
        logit_config::GraphiteTransport::Tcp => logit_inputs::graphite::Transport::Tcp,
        logit_config::GraphiteTransport::Udp => logit_inputs::graphite::Transport::Udp,
    }
}

/// Config's `GraphiteProtocol` into `logit_proto::graphite::Protocol` for `graphite_in`. Graph
/// rule 46 keeps `pickle` off UDP.
fn graphite_protocol(cfg: logit_config::GraphiteProtocol) -> logit_proto::graphite::Protocol {
    match cfg {
        logit_config::GraphiteProtocol::Plaintext => logit_proto::graphite::Protocol::Plaintext,
        logit_config::GraphiteProtocol::Pickle => logit_proto::graphite::Protocol::Pickle,
    }
}

/// Config's `GraphiteTransport` into `graphite_out`'s transport, a different type from
/// [`graphite_transport`]'s, hence the `graphite_out_` prefix.
fn graphite_out_transport(cfg: logit_config::GraphiteTransport) -> GraphiteOutTransport {
    match cfg {
        logit_config::GraphiteTransport::Udp => GraphiteOutTransport::Udp,
        logit_config::GraphiteTransport::Tcp => GraphiteOutTransport::Tcp,
    }
}

/// Config's `GraphiteProtocol` into `logit_proto::graphite::Protocol` for `graphite_out`.
fn graphite_out_protocol(cfg: logit_config::GraphiteProtocol) -> GraphiteWireProtocol {
    match cfg {
        logit_config::GraphiteProtocol::Plaintext => GraphiteWireProtocol::Plaintext,
        logit_config::GraphiteProtocol::Pickle => GraphiteWireProtocol::Pickle,
    }
}

/// Config's `GraphiteTags` into `logit_proto::graphite::Tags`.
fn graphite_tags(cfg: logit_config::GraphiteTags) -> GraphiteWireTags {
    match cfg {
        logit_config::GraphiteTags::Carbon => GraphiteWireTags::Carbon,
        logit_config::GraphiteTags::Drop => GraphiteWireTags::Drop,
    }
}

/// Config's `GraphiteMultiValue` into `logit_proto::graphite::MultiValue`.
fn graphite_multi_value(cfg: logit_config::GraphiteMultiValue) -> GraphiteWireMultiValue {
    match cfg {
        logit_config::GraphiteMultiValue::Skip => GraphiteWireMultiValue::Skip,
        logit_config::GraphiteMultiValue::Expand => GraphiteWireMultiValue::Expand,
    }
}

/// Config's `SplunkCompression` into `logit_outputs::splunk::SplunkCompression`.
fn splunk_compression(cfg: logit_config::SplunkCompression) -> SplunkOutCompression {
    match cfg {
        logit_config::SplunkCompression::Gzip => SplunkOutCompression::Gzip,
        logit_config::SplunkCompression::None => SplunkOutCompression::None,
    }
}

/// Config's `SplunkMultiValue` into `logit_proto::MultiValue`.
fn splunk_multi_value(cfg: logit_config::SplunkMultiValue) -> logit_proto::MultiValue {
    match cfg {
        logit_config::SplunkMultiValue::Skip => logit_proto::MultiValue::Skip,
        logit_config::SplunkMultiValue::Expand => logit_proto::MultiValue::Expand,
    }
}

/// Config's `Vec<Signal>` into the transform's boolean-flags `SignalSet`.
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

/// Config's `span:` block into the transform's `SpanLift`.
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

/// `logit_config::Distributions` -> `logit_transforms::Distributions`.
fn to_distributions(mode: logit_config::Distributions) -> TransformDistributions {
    match mode {
        logit_config::Distributions::Sketch => TransformDistributions::Sketch,
        logit_config::Distributions::Samples => TransformDistributions::Samples,
    }
}

/// `logit_config::Sets` -> `logit_transforms::Sets`.
fn to_sets(mode: logit_config::Sets) -> TransformSets {
    match mode {
        logit_config::Sets::Estimate => TransformSets::Estimate,
        logit_config::Sets::Members => TransformSets::Members,
    }
}

/// `logit_config::AggregateTemporality` -> `logit_transforms::AggregateTemporality`.
fn to_temporality(mode: logit_config::AggregateTemporality) -> TransformTemporality {
    match mode {
        logit_config::AggregateTemporality::Delta => TransformTemporality::Delta,
        logit_config::AggregateTemporality::Cumulative => TransformTemporality::Cumulative,
    }
}

/// Config's `OtlpPaths` into `logit-outputs`'s identically shaped `SignalPaths`.
fn to_signal_paths(paths: &logit_config::OtlpPaths) -> SignalPaths {
    SignalPaths {
        logs: paths.logs.clone(),
        metrics: paths.metrics.clone(),
        traces: paths.traces.clone(),
    }
}

/// Config's `TlsClientConfig` into `logit-outputs`'s identically shaped `TlsClientSettings`.
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

/// Config's `version: 1 | 2` into the codec's own `remote_write::Version`.
fn to_remote_write_version(
    version: logit_config::RemoteWriteVersion,
) -> logit_proto::prometheus::remote_write::Version {
    use logit_proto::prometheus::remote_write::Version;
    match version {
        logit_config::RemoteWriteVersion::V1 => Version::V1,
        logit_config::RemoteWriteVersion::V2 => Version::V2,
    }
}

/// Config's `compression: snappy | zstd` into the codec's own `compression::Encoding`.
fn to_remote_write_encoding(
    compression: logit_config::RemoteWriteCompression,
) -> logit_proto::prometheus::compression::Encoding {
    use logit_proto::prometheus::compression::Encoding;
    match compression {
        logit_config::RemoteWriteCompression::Snappy => Encoding::Snappy,
        logit_config::RemoteWriteCompression::Zstd => Encoding::Zstd,
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

/// [`to_tls_client_settings`] for `prometheus_in`'s scrape client, whose `reqwest`-based type is
/// `logit-inputs`'s own.
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

/// Config's `MetricSpec` into the transform's identically shaped type.
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

/// A `SetValue` map into the `(String, logit_core::Value)` pairs `set`, `has_attributes`, and
/// `drop_attributes` take. One conversion for all three, so they can't disagree on what a
/// `SetValue` becomes.
fn to_set_pairs(
    values: &std::collections::BTreeMap<String, logit_config::SetValue>,
) -> Vec<(String, logit_core::Value)> {
    values.iter().map(|(k, v)| (k.clone(), to_set_value(v))).collect()
}

/// One `SetValue`, shared by every caller so none maps a literal differently.
fn to_set_value(v: &logit_config::SetValue) -> logit_core::Value {
    match v {
        logit_config::SetValue::Bool(b) => logit_core::Value::Bool(*b),
        logit_config::SetValue::I64(i) => logit_core::Value::I64(*i),
        logit_config::SetValue::F64(f) => logit_core::Value::F64(*f),
        logit_config::SetValue::Str(s) => logit_core::Value::str(s.clone()),
    }
}

/// `keep_values`' `ValueAllowList` map into the `ClampConfig` tuples `KeepValues::new` takes.
fn to_allow_lists(
    values: &std::collections::BTreeMap<String, logit_config::ValueAllowList>,
) -> Vec<logit_transforms::ClampConfig> {
    values
        .iter()
        .map(|(field, allow_list)| {
            let normalize = allow_list
                .normalize
                .iter()
                .map(|step| match step {
                    logit_config::NormalizeStep::Lower => TransformNormalize::Lower,
                })
                .collect();
            let allow = allow_list.allow.iter().map(to_set_value).collect();
            let other = allow_list.other.as_ref().map(to_set_value);
            (field.clone(), normalize, allow, other)
        })
        .collect()
}

/// `flatten`'s `FlattenFields` into the `logit_transforms::Fields` `Flatten::new` takes.
fn to_flatten_fields(fields: &logit_config::FlattenFields) -> TransformFields {
    match fields {
        logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::All) => {
            TransformFields::All
        }
        logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::None) => {
            TransformFields::None
        }
        logit_config::FlattenFields::Named(names) => TransformFields::Named(names.clone()),
    }
}

/// `http_access`'s fields into the `HttpAccessConfig` `HttpAccess::new` takes.
///
/// `max_length` is resolved here, `logit_config::CAPPED_FIELDS`' defaults with the config's
/// overrides applied, so the transform gets one complete cap list and never knows the defaults
/// (`docs/adr/http-access-normalization.md`).
fn to_http_access_config(
    routes: &[logit_config::HttpRouteRule],
    route_other: &Option<String>,
    user_agent_rules: &[logit_config::UserAgentRule],
    max_length: &std::collections::BTreeMap<String, usize>,
    redact_query: &[String],
    forwarded: Option<logit_config::ForwardedConfig>,
) -> HttpAccessConfig {
    let routes = routes
        .iter()
        .map(|rule| match (rule.builtin, &rule.pattern, &rule.route) {
            (Some(set), _, _) => logit_transforms::RouteRule::Builtin(match set {
                logit_config::HttpRouteSet::Assets => logit_transforms::RouteSet::Assets,
                logit_config::HttpRouteSet::WellKnown => logit_transforms::RouteSet::WellKnown,
                logit_config::HttpRouteSet::Probes => logit_transforms::RouteSet::Probes,
            }),
            // Rule 60 guarantees every non-builtin rule carries both halves.
            (None, pattern, route) => logit_transforms::RouteRule::Pattern {
                pattern: pattern.clone().unwrap_or_default(),
                route: route.clone().unwrap_or_default(),
            },
        })
        .collect();
    let user_agent_rules = user_agent_rules
        .iter()
        .map(|rule| logit_transforms::UaRule {
            pattern: rule.pattern.clone(),
            class: rule.class.clone(),
        })
        .collect();
    let max_length = logit_config::CAPPED_FIELDS
        .iter()
        .map(|(field, default)| {
            (field.to_string(), max_length.get(*field).copied().unwrap_or(*default))
        })
        .collect();
    HttpAccessConfig {
        routes,
        route_other: route_other.clone(),
        user_agent_rules,
        max_length,
        redact_query: redact_query.to_vec(),
        trust_forwarded: forwarded.is_some_and(|f| f.trust),
    }
}

/// `json`'s `invalid_utf8` into the `logit_transforms::InvalidUtf8` `JsonParser` takes.
fn to_invalid_utf8(mode: logit_config::JsonInvalidUtf8) -> TransformInvalidUtf8 {
    match mode {
        logit_config::JsonInvalidUtf8::Reject => TransformInvalidUtf8::Reject,
        logit_config::JsonInvalidUtf8::Replace => TransformInvalidUtf8::Replace,
    }
}

/// `flatten`'s `FlattenArrays` into `logit_transforms::Arrays`.
fn to_flatten_arrays(arrays: logit_config::FlattenArrays) -> TransformArrays {
    match arrays {
        logit_config::FlattenArrays::Index => TransformArrays::Index,
        logit_config::FlattenArrays::Skip => TransformArrays::Skip,
    }
}

/// `sample`'s `SampleKey` into `logit_transforms::SampleKey`.
fn to_sample_key(key: &logit_config::SampleKey) -> logit_transforms::SampleKey {
    match key {
        logit_config::SampleKey::TraceId => logit_transforms::SampleKey::TraceId,
        logit_config::SampleKey::Attribute(name) => {
            logit_transforms::SampleKey::Attribute(name.clone())
        }
        logit_config::SampleKey::Resource(name) => {
            logit_transforms::SampleKey::Resource(name.clone())
        }
    }
}

/// `missing:` absent means `random` (`docs/adr/consistent-sampling-component.md`); the config
/// keeps it an `Option` only so rule 61 can see "set without `key:`".
fn to_sample_missing(
    missing: Option<logit_config::SampleMissing>,
) -> logit_transforms::SampleMissing {
    match missing.unwrap_or_default() {
        logit_config::SampleMissing::Random => logit_transforms::SampleMissing::Random,
        logit_config::SampleMissing::Keep => logit_transforms::SampleMissing::Keep,
        logit_config::SampleMissing::Drop => logit_transforms::SampleMissing::Drop,
    }
}

/// Converts `always_keep:`, folding rule 61's "exactly one of `attribute`/`resource`" into
/// `SampleField`'s two variants. The literal goes through [`to_set_value`], so `always_keep`
/// reads a YAML scalar as `set`/`has_attributes` do.
fn to_sample_override(o: &logit_config::SampleOverride) -> logit_transforms::SampleOverride {
    let field = match (&o.attribute, &o.resource) {
        (Some(name), _) => logit_transforms::SampleField::Attribute(name.clone()),
        (None, Some(name)) => logit_transforms::SampleField::Resource(name.clone()),
        (None, None) => unreachable!("graph rule 61 requires one of attribute/resource"),
    };
    logit_transforms::SampleOverride { field, value: o.value.as_ref().map(to_set_value) }
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
            targets: Vec::new(),
            kind: ComponentKind::StatsdIn {
                bind: "127.0.0.1:0".to_string(),
                transport: logit_config::StatsdTransport::default(),
                tls: None,
                handshake_timeout: logit_config::default_handshake_timeout(),
                idle_timeout: None,
            },
        }
    }

    fn influxdb_out(sources: Vec<&str>) -> Component {
        Component {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: sources.into_iter().map(String::from).collect(),
            targets: Vec::new(),
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

    /// A sink shared by two upstream branches is valid.
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
                    targets: Vec::new(),
                    kind: ComponentKind::Lua { script: "".to_string(), interval: None },
                },
            ),
            (
                "branch_b",
                Component {
                    buffer: logit_config::BufferConfig::default(),
                    receive: logit_config::ReceiveConfig::default(),
                    sources: vec!["in".to_string()],
                    targets: Vec::new(),
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
                    targets: Vec::new(),
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
                    targets: Vec::new(),
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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

    /// `token` is a plain field; an unset `!env` variable fails earlier, in `config::load`.
    #[test]
    fn build_spec_builds_an_influxdb_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
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

    /// `#[tokio::test]`: `CollectdOutput::udp` binds a local UDP socket eagerly, which needs a
    /// runtime.
    #[tokio::test]
    async fn build_spec_builds_a_collectd_sink_and_wires_a_configured_hostname_into_its_encoder() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec![],
            kind: ComponentKind::CollectdOut {
                endpoint: "127.0.0.1:25826".to_string(),
                max_packet_bytes: 1452,
                hostname: Some("logit-relay".to_string()),
            },
        };
        assert!(matches!(
            build_spec("out", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    fn graphite_out_kind(
        transport: logit_config::GraphiteTransport,
        protocol: logit_config::GraphiteProtocol,
    ) -> ComponentKind {
        ComponentKind::GraphiteOut {
            endpoint: "127.0.0.1:2003".to_string(),
            transport,
            protocol,
            tags: logit_config::GraphiteTags::default(),
            multi_value: logit_config::GraphiteMultiValue::default(),
            max_packet_bytes: 1432,
            max_frame_bytes: 1 << 20,
            connect_timeout: Duration::from_secs(5),
        }
    }

    /// `#[tokio::test]`: `GraphiteOutput::udp` binds eagerly, as `CollectdOutput::udp` does.
    #[tokio::test]
    async fn build_spec_builds_a_graphite_udp_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec![],
            kind: graphite_out_kind(
                logit_config::GraphiteTransport::Udp,
                logit_config::GraphiteProtocol::Plaintext,
            ),
        };
        assert!(matches!(
            build_spec("out", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    /// `GraphiteOutput::tcp` touches no socket at construction, so this needs no runtime.
    #[test]
    fn build_spec_builds_a_graphite_tcp_sink_without_binding_eagerly() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec![],
            kind: graphite_out_kind(
                logit_config::GraphiteTransport::Tcp,
                logit_config::GraphiteProtocol::Pickle,
            ),
        };
        assert!(matches!(
            build_spec("out", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    /// No runtime: `build_spec` must not bind; `Output::bind`'s pre-spawn pass does.
    #[test]
    fn build_spec_builds_a_prometheus_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec![],
            kind: ComponentKind::PrometheusOut {
                bind: Some("127.0.0.1:0".to_string()),
                path: "/metrics".to_string(),
                expire_after: Duration::from_secs(300),
                max_series: 100_000,
                endpoint: None,
                version: logit_config::RemoteWriteVersion::default(),
                compression: logit_config::RemoteWriteCompression::default(),
                timeout: logit_config::default_prometheus_endpoint_timeout(),
                headers: HashMap::new(),
                endpoint_tls: logit_config::TlsClientConfig::default(),
            },
        };
        assert!(matches!(
            build_spec("out", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    /// `endpoint:` builds the remote-write sender; nothing is dialed, it connects per request.
    #[test]
    fn build_spec_builds_a_prometheus_remote_write_sink() {
        for (version, compression) in [
            (logit_config::RemoteWriteVersion::V1, logit_config::RemoteWriteCompression::Snappy),
            (logit_config::RemoteWriteVersion::V1, logit_config::RemoteWriteCompression::Zstd),
            (logit_config::RemoteWriteVersion::V2, logit_config::RemoteWriteCompression::Snappy),
        ] {
            let component = ResolvedComponent {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                consumers: vec![],
                kind: ComponentKind::PrometheusOut {
                    bind: None,
                    path: logit_config::default_prometheus_path(),
                    expire_after: logit_config::default_prometheus_expire_after(),
                    max_series: logit_config::default_prometheus_max_series(),
                    endpoint: Some("http://mimir:8080/api/v1/push".to_string()),
                    version,
                    compression,
                    timeout: Duration::from_secs(30),
                    headers: HashMap::from([("X-Scope-OrgID".to_string(), "tenant-a".to_string())]),
                    endpoint_tls: logit_config::TlsClientConfig::default(),
                },
            };
            assert!(
                matches!(
                    build_spec("out", &component, Path::new(""), None).unwrap().0,
                    NodeSpec::Output(_, _, _)
                ),
                "version {version:?}, compression {compression:?}"
            );
        }
    }

    #[test]
    fn build_spec_builds_a_null_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec![],
            kind: ComponentKind::NullOut {},
        };
        assert!(matches!(
            build_spec("out", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Output(_, _, _)
        ));
    }

    #[test]
    fn build_spec_builds_a_target() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Target {},
        };
        assert!(matches!(
            build_spec("t", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Target
        ));
    }

    #[test]
    fn build_spec_builds_a_route_router() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: vec!["host_stream".to_string(), "app_stream".to_string()],
            consumers: vec![],
            kind: ComponentKind::Route {
                by: logit_config::RouteBy::Attribute("stream".to_string()),
                routes: [
                    ("host".to_string(), "host_stream".to_string()),
                    ("app".to_string(), "app_stream".to_string()),
                ]
                .into_iter()
                .collect(),
            },
        };
        assert!(matches!(
            build_spec("r", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Router(_)
        ));
    }

    #[test]
    fn build_spec_builds_a_sample_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Sample {
                rate: 0.1,
                key: Some(logit_config::SampleKey::TraceId),
                missing: None,
                always_keep: Some(logit_config::SampleOverride {
                    attribute: None,
                    resource: Some("debug".to_string()),
                    value: Some(logit_config::SetValue::Bool(true)),
                }),
            },
        };
        assert!(matches!(
            build_spec("sampled", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    #[test]
    fn build_spec_builds_an_otlp_input() {
        for protocol in [logit_config::OtlpProtocol::Http, logit_config::OtlpProtocol::Grpc] {
            let component = ResolvedComponent {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec![],
                targets: Vec::new(),
                consumers: vec!["out".to_string()],
                kind: ComponentKind::OtlpIn {
                    bind: "127.0.0.1:0".to_string(),
                    protocol,
                    tls: None,
                    handshake_timeout: Duration::from_secs(5),
                    idle_timeout: None,
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

    /// A fully populated `generate_in` builds, covering the arm's four template `?`s.
    #[test]
    fn build_spec_builds_a_generate_input() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::GenerateIn {
                count: Some(1000),
                batch: 100,
                rate: Some(50_000),
                event: logit_config::GenerateEvent {
                    log: Some("path=/x/{seq%50}".to_string()),
                    attributes: std::collections::BTreeMap::from([(
                        "host".to_string(),
                        "web-{seq%10}".to_string(),
                    )]),
                    metric: Some(logit_config::GenerateMetric {
                        name: "requests".to_string(),
                        kind: logit_config::GenerateMetricKind::Distribution,
                        value: 1.0,
                    }),
                },
                resource: std::collections::BTreeMap::from([(
                    "service.name".to_string(),
                    "web".to_string(),
                )]),
            },
        };
        assert!(matches!(
            build_spec("gen", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    #[test]
    fn build_spec_builds_a_prometheus_input() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::PrometheusIn {
                scrape_targets: vec!["http://127.0.0.1:0/metrics".to_string()],
                interval: Duration::from_secs(15),
                timeout: Duration::from_secs(10),
                headers: HashMap::new(),
                scrape_tls: logit_config::TlsClientConfig::default(),
                bind: None,
                path: "/api/v1/write".to_string(),
                bind_tls: None,
                idle_timeout: None,
                metadata_cache: logit_config::MetadataCacheConfig::default(),
            },
        };
        assert!(matches!(
            build_spec("in", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    /// `bind:` instead of `scrape_targets:` builds the remote-write receiver.
    #[test]
    fn build_spec_builds_a_prometheus_receiver() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::PrometheusIn {
                scrape_targets: Vec::new(),
                interval: Duration::from_secs(15),
                timeout: Duration::from_secs(10),
                headers: HashMap::new(),
                scrape_tls: logit_config::TlsClientConfig::default(),
                bind: Some("127.0.0.1:0".to_string()),
                path: "/api/v1/write".to_string(),
                bind_tls: None,
                idle_timeout: Some(Duration::from_secs(60)),
                metadata_cache: logit_config::MetadataCacheConfig::default(),
            },
        };
        assert!(matches!(
            build_spec("in", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    fn collectd_component(types_db: Vec<PathBuf>) -> ResolvedComponent {
        ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig {
                max_datagrams: 4242,
                ..logit_config::ReceiveConfig::default()
            },
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::CollectdIn { bind: "127.0.0.1:0".to_string(), types_db },
        }
    }

    #[test]
    fn build_spec_builds_a_collectd_input_with_receive_wired() {
        let component = collectd_component(vec![]);
        let (spec, _telemetry) = build_spec("in", &component, Path::new(""), None).unwrap();
        assert!(matches!(spec, NodeSpec::Input(..)));
    }

    /// A relative `types_db` path resolves against `base_dir` and is read; a missing one is a
    /// startup error naming it.
    #[test]
    fn build_spec_loads_a_collectd_types_db_relative_to_base_dir() {
        let dir = std::env::temp_dir().join(format!("logit-collectd-spec-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("types.db"), "load a:GAUGE:U:U, b:GAUGE:U:U\n").unwrap();

        let component = collectd_component(vec![PathBuf::from("types.db")]);
        let (spec, _telemetry) = build_spec("in", &component, &dir, None)
            .expect("a types_db under base_dir should load");
        assert!(matches!(spec, NodeSpec::Input(..)));

        let missing = collectd_component(vec![PathBuf::from("absent.db")]);
        let err = match build_spec("in", &missing, &dir, None) {
            Ok(_) => panic!("a missing types_db must fail startup, not warn"),
            Err(err) => err.to_string(),
        };
        assert!(err.contains("loading types_db"), "got: {err}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn graphite_component(
        transport: logit_config::GraphiteTransport,
        protocol: logit_config::GraphiteProtocol,
    ) -> ResolvedComponent {
        graphite_component_with_tls(transport, protocol, None)
    }

    fn graphite_component_with_tls(
        transport: logit_config::GraphiteTransport,
        protocol: logit_config::GraphiteProtocol,
        tls: Option<logit_config::TlsServerConfig>,
    ) -> ResolvedComponent {
        ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig {
                batch_max_events: 4242,
                ..logit_config::ReceiveConfig::default()
            },
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::GraphiteIn {
                bind: "127.0.0.1:0".to_string(),
                transport,
                protocol,
                tls,
                handshake_timeout: Duration::from_secs(5),
                idle_timeout: None,
                max_line_bytes: 8192,
                max_frame_bytes: 1 << 20,
            },
        }
    }

    /// A TCP `graphite_in` loads its TLS cert. The missing-cert half is what pins the `with_tls`
    /// call: the positive half passes without it. Rule 43 never reads the file.
    #[test]
    fn build_spec_builds_a_tls_graphite_input() {
        use logit_config::{GraphiteProtocol, GraphiteTransport};
        let component = graphite_component_with_tls(
            GraphiteTransport::Tcp,
            GraphiteProtocol::Plaintext,
            Some(logit_config::TlsServerConfig {
                cert_file: "server.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
        );
        assert!(matches!(
            build_spec("in", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Input(..)
        ));

        let missing = graphite_component_with_tls(
            GraphiteTransport::Tcp,
            GraphiteProtocol::Plaintext,
            Some(logit_config::TlsServerConfig {
                cert_file: "does-not-exist.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
        );
        let err = match build_spec("in", &missing, &testdata_tls_dir(), None) {
            Ok(_) => panic!("expected a missing tls.cert_file to fail build_spec"),
            Err(err) => format!("{err:?}"),
        };
        assert!(err.contains("tls.cert_file"), "got: {err}");
        assert!(err.contains("does-not-exist.pem"), "got: {err}");
    }

    /// Every `transport`/`protocol` pair rule 46 permits builds, with a `receive:` block on each.
    #[test]
    fn build_spec_builds_a_graphite_input_for_every_permitted_transport_and_protocol() {
        use logit_config::{GraphiteProtocol, GraphiteTransport};
        for (transport, protocol) in [
            (GraphiteTransport::Tcp, GraphiteProtocol::Plaintext),
            (GraphiteTransport::Tcp, GraphiteProtocol::Pickle),
            (GraphiteTransport::Udp, GraphiteProtocol::Plaintext),
        ] {
            let component = graphite_component(transport, protocol);
            let (spec, _telemetry) = build_spec("in", &component, Path::new(""), None)
                .unwrap_or_else(|e| panic!("{transport:?}/{protocol:?} should build: {e}"));
            match spec {
                NodeSpec::Input(input, runtime) => {
                    assert_eq!(
                        runtime.shutdown_grace,
                        logit_config::ReceiveConfig::default().shutdown_grace,
                        "{transport:?}: shutdown_grace comes from the same receive: block"
                    );
                    drop(input);
                }
                _other => panic!("{transport:?}/{protocol:?} should have built a NodeSpec::Input"),
            }
        }
    }

    /// The two converters map each arm correctly; a mismap would surface nowhere else.
    #[test]
    fn graphite_converters_map_every_variant() {
        use logit_config::{GraphiteProtocol, GraphiteTransport};
        assert_eq!(
            graphite_transport(GraphiteTransport::Tcp),
            logit_inputs::graphite::Transport::Tcp
        );
        assert_eq!(
            graphite_transport(GraphiteTransport::Udp),
            logit_inputs::graphite::Transport::Udp
        );
        assert_eq!(
            graphite_protocol(GraphiteProtocol::Plaintext),
            logit_proto::graphite::Protocol::Plaintext
        );
        assert_eq!(
            graphite_protocol(GraphiteProtocol::Pickle),
            logit_proto::graphite::Protocol::Pickle
        );
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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

    /// `tail_config` maps every `TailOptions` field and `receive:`'s batching fields. Tested
    /// directly because `NodeSpec::Input` boxes the result opaquely.
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
                targets: Vec::new(),
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

    /// An `https://` endpoint under `protocol: grpc` builds gRPC over TLS
    /// (`docs/adr/otlp-tls-and-pooled-grpc-client.md`).
    #[test]
    fn build_spec_builds_an_otlp_sink_with_https_under_grpc() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
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

    /// The repo's `testdata/tls` fixtures, two levels up.
    fn testdata_tls_dir() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/tls")
    }

    #[test]
    fn build_spec_wires_a_tls_client_config_into_an_otlp_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
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

    /// A bad `tls.ca_file` first fails in `build_spec`: rule 24 never reads the file, so `logit
    /// validate` passes it (`docs/deploying.md`'s "`logit validate` as a preflight").
    #[test]
    fn build_spec_reports_a_missing_tls_ca_file_clearly() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
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
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::OtlpIn {
                bind: "127.0.0.1:0".to_string(),
                protocol: logit_config::OtlpProtocol::Http,
                tls: Some(logit_config::TlsServerConfig {
                    cert_file: "server.pem".to_string(),
                    key_file: "server.key".to_string(),
                    client_ca_file: None,
                }),
                handshake_timeout: Duration::from_secs(5),
                idle_timeout: None,
            },
        };
        assert!(matches!(
            build_spec("in", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    /// A `syslog_in` at either transport, with or without TLS.
    fn syslog_in_component(
        transport: logit_config::SyslogTransport,
        tls: Option<logit_config::TlsServerConfig>,
    ) -> ResolvedComponent {
        ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::SyslogIn {
                bind: "127.0.0.1:0".to_string(),
                transport,
                tls,
                handshake_timeout: Duration::from_secs(5),
                idle_timeout: None,
            },
        }
    }

    #[test]
    fn build_spec_builds_a_tcp_syslog_input() {
        let component = syslog_in_component(logit_config::SyslogTransport::Tcp, None);
        assert!(matches!(
            build_spec("in", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    /// The TLS arm loads `testdata/tls/server.{pem,key}`.
    #[test]
    fn build_spec_wires_a_tls_server_config_into_a_tcp_syslog_input() {
        let component = syslog_in_component(
            logit_config::SyslogTransport::Tcp,
            Some(logit_config::TlsServerConfig {
                cert_file: "server.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
        );
        assert!(matches!(
            build_spec("in", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    /// A missing cert fails the build: this, not the positive test above, pins the `with_tls`
    /// call. Rule 43 never reads the file.
    #[test]
    fn build_spec_reports_a_missing_syslog_tls_cert_file_clearly() {
        let component = syslog_in_component(
            logit_config::SyslogTransport::Tcp,
            Some(logit_config::TlsServerConfig {
                cert_file: "does-not-exist.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
        );
        let err = match build_spec("in", &component, &testdata_tls_dir(), None) {
            Ok(_) => panic!("expected a missing tls.cert_file to fail build_spec"),
            Err(err) => err,
        };
        let err = format!("{err:?}");
        assert!(err.contains("tls.cert_file"), "got: {err}");
        assert!(err.contains("does-not-exist.pem"), "got: {err}");
    }

    /// The default transport still builds the UDP listener, `receive:` and all.
    #[test]
    fn build_spec_builds_a_udp_syslog_input() {
        let component = syslog_in_component(logit_config::SyslogTransport::Udp, None);
        assert!(matches!(
            build_spec("in", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    /// `tcp_receive_config` carries the batching and shutdown fields, not the queue ones.
    #[test]
    fn tcp_receive_config_carries_only_the_batch_and_shutdown_fields() {
        let receive = logit_config::ReceiveConfig {
            max_datagrams: 4096,
            batch_max_events: 7,
            batch_max_bytes: 99,
            batch_flush_interval: Duration::from_millis(25),
            shutdown_grace: Duration::from_secs(3),
            ..logit_config::ReceiveConfig::default()
        };
        let cfg = tcp_receive_config(&receive);
        assert_eq!(cfg.batch_max_events, 7);
        assert_eq!(cfg.batch_max_bytes, 99);
        assert_eq!(cfg.batch_flush_interval, Duration::from_millis(25));
        assert_eq!(cfg.shutdown_grace, Duration::from_secs(3));
    }

    #[test]
    fn build_spec_builds_a_logit_input() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::LogitIn {
                bind: "127.0.0.1:0".to_string(),
                tls: None,
                max_frame_bytes: None,
                handshake_timeout: Duration::from_secs(5),
                idle_timeout: None,
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
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::LogitIn {
                bind: "127.0.0.1:0".to_string(),
                tls: Some(logit_config::TlsServerConfig {
                    cert_file: "server.pem".to_string(),
                    key_file: "server.key".to_string(),
                    client_ca_file: None,
                }),
                max_frame_bytes: Some(32 * 1024 * 1024),
                handshake_timeout: Duration::from_secs(5),
                idle_timeout: None,
            },
        };
        assert!(matches!(
            build_spec("in", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    // ---- `handshake_timeout` reaches each of the three listeners -------------------------------
    //
    // A boxed `dyn Input` has no field to read back, so each test asserts a behavior only the call
    // produces: a 50ms budget closes a silent connection within the 1s read, where the 5s default
    // wouldn't.

    /// A free loopback port, bound and released: a boxed `Input` can't report its `local_addr`.
    async fn free_port() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().to_string()
    }

    /// Spawns a built `NodeSpec::Input` and asserts it closes a silent connection within 1s.
    async fn assert_closes_a_silent_connection(spec: NodeSpec, addr: &str) {
        let NodeSpec::Input(mut input, _) = spec else { panic!("expected NodeSpec::Input") };
        tokio::spawn(async move { input.run(logit_pipeline::Fanout::new(vec![])).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut silent = tokio::net::TcpStream::connect(addr).await.unwrap();
        let mut buf = [0u8; 1];
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::io::AsyncReadExt::read(&mut silent, &mut buf),
        )
        .await
        .expect("a configured 50ms handshake_timeout should close a silent connection within 1s");
        match result {
            Ok(n) => assert_eq!(n, 0, "expected a close, got a byte"),
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(err) => panic!("read failed outright: {err}"),
        }
    }

    #[tokio::test]
    async fn build_spec_wires_handshake_timeout_into_a_tcp_syslog_input() {
        let addr = free_port().await;
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::SyslogIn {
                bind: addr.clone(),
                transport: logit_config::SyslogTransport::Tcp,
                tls: None,
                handshake_timeout: Duration::from_millis(50),
                idle_timeout: None,
            },
        };
        let spec = build_spec("in", &component, Path::new(""), None).unwrap().0;
        assert_closes_a_silent_connection(spec, &addr).await;
    }

    #[tokio::test]
    async fn build_spec_wires_handshake_timeout_into_a_logit_input() {
        let addr = free_port().await;
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::LogitIn {
                bind: addr.clone(),
                tls: None,
                max_frame_bytes: None,
                handshake_timeout: Duration::from_millis(50),
                idle_timeout: None,
            },
        };
        let spec = build_spec("in", &component, Path::new(""), None).unwrap().0;
        assert_closes_a_silent_connection(spec, &addr).await;
    }

    /// `otlp_in`'s knob bounds only the TLS accept, so this needs a real `tls:` block.
    #[tokio::test]
    async fn build_spec_wires_handshake_timeout_into_an_otlp_input() {
        let addr = free_port().await;
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::OtlpIn {
                bind: addr.clone(),
                protocol: logit_config::OtlpProtocol::Http,
                tls: Some(logit_config::TlsServerConfig {
                    cert_file: "server.pem".to_string(),
                    key_file: "server.key".to_string(),
                    client_ca_file: None,
                }),
                handshake_timeout: Duration::from_millis(50),
                idle_timeout: None,
            },
        };
        let spec = build_spec("in", &component, &testdata_tls_dir(), None).unwrap().0;
        assert_closes_a_silent_connection(spec, &addr).await;
    }

    // ---- `idle_timeout` reaches each TCP listener that honours it -------------------------------
    //
    // As above, one phase later: a 50ms `idle_timeout` closes a client gone quiet after one frame.
    // `handshake_timeout` stays at its 5s default, so only the idle clock can close within 1s.

    /// Spawns a built `NodeSpec::Input`, sends `wire`, and asserts the server closes the connection
    /// within 1s of it going quiet.
    async fn assert_closes_a_quiet_connection(spec: NodeSpec, addr: &str, wire: &[u8]) {
        let NodeSpec::Input(mut input, _) = spec else { panic!("expected NodeSpec::Input") };
        tokio::spawn(async move { input.run(logit_pipeline::Fanout::new(vec![])).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let mut quiet = tokio::net::TcpStream::connect(addr).await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut quiet, wire).await.unwrap();

        let mut buf = [0u8; 1];
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            tokio::io::AsyncReadExt::read(&mut quiet, &mut buf),
        )
        .await
        .expect("a configured 50ms idle_timeout should close a quiet connection within 1s");
        match result {
            Ok(n) => assert_eq!(n, 0, "expected a close, got a byte"),
            Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
            Err(err) => panic!("read failed outright: {err}"),
        }
    }

    #[tokio::test]
    async fn build_spec_wires_idle_timeout_into_a_tcp_syslog_input() {
        let addr = free_port().await;
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::SyslogIn {
                bind: addr.clone(),
                transport: logit_config::SyslogTransport::Tcp,
                tls: None,
                handshake_timeout: logit_config::default_handshake_timeout(),
                idle_timeout: Some(Duration::from_millis(50)),
            },
        };
        let spec = build_spec("in", &component, Path::new(""), None).unwrap().0;
        assert_closes_a_quiet_connection(spec, &addr, b"<13>hello\n").await;
    }

    #[tokio::test]
    async fn build_spec_wires_idle_timeout_into_a_tcp_graphite_input() {
        let addr = free_port().await;
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::GraphiteIn {
                bind: addr.clone(),
                transport: logit_config::GraphiteTransport::Tcp,
                protocol: logit_config::GraphiteProtocol::Plaintext,
                tls: None,
                handshake_timeout: logit_config::default_handshake_timeout(),
                idle_timeout: Some(Duration::from_millis(50)),
                max_line_bytes: 8192,
                max_frame_bytes: 1 << 20,
            },
        };
        let spec = build_spec("in", &component, Path::new(""), None).unwrap().0;
        assert_closes_a_quiet_connection(spec, &addr, b"some.path 1 1700000000\n").await;
    }

    #[tokio::test]
    async fn build_spec_wires_idle_timeout_into_a_tcp_statsd_input() {
        let addr = free_port().await;
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::StatsdIn {
                bind: addr.clone(),
                transport: logit_config::StatsdTransport::Tcp,
                tls: None,
                handshake_timeout: logit_config::default_handshake_timeout(),
                idle_timeout: Some(Duration::from_millis(50)),
            },
        };
        let spec = build_spec("in", &component, Path::new(""), None).unwrap().0;
        assert_closes_a_quiet_connection(spec, &addr, b"some.counter:1|c\n").await;
    }

    /// `logit_in` arms the idle clock after its handshake and closes with `Reject{GOING_AWAY}`,
    /// which only the 50ms idle clock can write within 1s.
    #[tokio::test]
    async fn build_spec_wires_idle_timeout_into_a_logit_input() {
        use logit_proto::frame;
        use logit_proto::native::{self, control};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let addr = free_port().await;
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::LogitIn {
                bind: addr.clone(),
                tls: None,
                max_frame_bytes: None,
                handshake_timeout: logit_config::default_handshake_timeout(),
                idle_timeout: Some(Duration::from_millis(50)),
            },
        };
        let NodeSpec::Input(mut input, _) =
            build_spec("in", &component, Path::new(""), None).unwrap().0
        else {
            panic!("expected NodeSpec::Input")
        };
        tokio::spawn(async move { input.run(logit_pipeline::Fanout::new(vec![])).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        /// Reads and decodes one control frame. Hand-rolled: a real `logit_out` would reconnect
        /// and hide the close under test.
        async fn read_control_frame(stream: &mut tokio::net::TcpStream) -> control::ControlMessage {
            let mut header_buf = [0u8; frame::HEADER_LEN];
            stream.read_exact(&mut header_buf).await.unwrap();
            let mut header_bytes = bytes::Bytes::copy_from_slice(&header_buf);
            let header = frame::FrameHeader::read(&mut header_bytes).unwrap();
            let mut body = vec![0u8; header.compressed_len as usize];
            stream.read_exact(&mut body).await.unwrap();
            let mut full = Vec::with_capacity(frame::HEADER_LEN + body.len());
            full.extend_from_slice(&header_buf);
            full.extend_from_slice(&body);
            let mut full = bytes::Bytes::from(full);
            let (_header, mut payload) = frame::read_frame_with_header(&mut full).unwrap();
            control::ControlMessage::decode(&mut payload).unwrap()
        }

        let mut client = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let hello = control::Hello {
            version: control::PROTOCOL_VERSION,
            codecs: vec![native::CODEC_NATIVE_V1],
            compressions: vec![0],
            max_frame_bytes: frame::MAX_SANE_UNCOMPRESSED_LEN,
            window: 1,
        };
        let framed = frame::write_frame_with_flags(
            0,
            NativeCompression::None,
            frame::FLAG_CONTROL,
            &hello.encode(),
        )
        .unwrap();
        client.write_all(&framed).await.unwrap();
        assert!(
            matches!(read_control_frame(&mut client).await, control::ControlMessage::HelloAck(_)),
            "the handshake itself must succeed"
        );

        // Quiet from here; nothing else on this listener writes to the peer.
        let reject = tokio::time::timeout(Duration::from_secs(1), read_control_frame(&mut client))
            .await
            .expect("a configured 50ms idle_timeout should close a quiet connection within 1s");
        match reject {
            control::ControlMessage::Reject(reject) => {
                assert_eq!(reject.code, control::REJECT_GOING_AWAY);
                assert!(reject.message.contains("idle for"), "got: {}", reject.message);
            }
            other => panic!("expected Reject{{GOING_AWAY}}, got {other:?}"),
        }
    }

    /// `otlp_in`: one byte clears its first-byte peek, and its idle clock runs from the connection,
    /// so that byte and silence is what `idle_timeout` closes. A complete request would be
    /// answered instead (`docs/adr/idle-connection-timeout.md`).
    #[tokio::test]
    async fn build_spec_wires_idle_timeout_into_an_otlp_input() {
        let addr = free_port().await;
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::OtlpIn {
                bind: addr.clone(),
                protocol: logit_config::OtlpProtocol::Http,
                tls: None,
                handshake_timeout: logit_config::default_handshake_timeout(),
                idle_timeout: Some(Duration::from_millis(50)),
            },
        };
        let spec = build_spec("in", &component, Path::new(""), None).unwrap().0;
        assert_closes_a_quiet_connection(spec, &addr, b"P").await;
    }

    #[test]
    fn build_spec_builds_a_logit_sink() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
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
            targets: Vec::new(),
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

    /// A non-default `buffer:` reaches the built sink's `SinkQueueConfig`/`WriteLoopConfig`.
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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

    /// A `null_out` behind `buffer.disk` (the perf harness's `buffered` scenario) gets a disk
    /// `SinkStoreConfig`.
    #[test]
    fn build_spec_builds_a_null_sink_behind_a_disk_buffer_and_resolves_a_disk_sinkstoreconfig() {
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
            targets: Vec::new(),
            consumers: vec![],
            kind: ComponentKind::NullOut {},
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
    }

    #[test]
    fn build_spec_builds_a_logfmt_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
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
            targets: Vec::new(),
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
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Json { skip_to_brace: true, invalid_utf8: Default::default() },
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
            targets: Vec::new(),
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

    /// A relative `target:` path resolves against `base_dir`, not the working directory.
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
        // Not `expect_err`: `NodeSpec` isn't `Debug`.
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
            targets: Vec::new(),
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

    /// A relative `path:` resolves against `base_dir`, as `StdioTarget::Path` does.
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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

    /// Runs the built transform, to catch swapped `trace_id`/`span_id`/`flags` arguments, which
    /// share a type.
    #[test]
    fn build_spec_builds_a_working_trace_context_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::TraceContext {
                format: logit_config::TraceIdFormat::Otel,
                trace_id: Some("tid".to_string()),
                span_id: Some(Some("sid".to_string())),
                flags: Some(None),
                trace_id_high: None,
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
        let mut event = event;
        assert!(transform.process(&resource, &mut event), "should forward the event");
        let trace = event.log.expect("log should survive").trace.expect("trace should be lifted");
        assert_eq!(trace.trace_id, [0xab; 16]);
        assert_eq!(trace.span_id, Some([0xcd; 8]));
        assert!(
            event.attributes.get("tid").is_some(),
            "keep_source: true should retain the attribute"
        );
    }

    /// `to_span_lift`'s mapping: `kind: client` and the default `name` reach the minted span.
    #[test]
    fn build_spec_builds_a_span_lifting_trace_context_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::TraceContext {
                format: logit_config::TraceIdFormat::Otel,
                trace_id: None,
                span_id: None,
                flags: Some(None),
                trace_id_high: None,
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
        let mut event = event;
        assert!(transform.process(&resource, &mut event), "should forward the event");
        let span = event.span.expect("a span should be minted");
        assert_eq!(span.kind, logit_core::SpanKind::Client);
        assert_eq!(span.name.as_str(), Some("http.request"));
        assert_eq!(event.timestamp, 1_725_000_000_000_000_000);
        assert_eq!(span.end_timestamp, 1_725_000_000_005_000_000);
    }

    /// `format: datadog` reaches the transform with its `dd.*` defaults and `trace_id_high`.
    #[test]
    fn build_spec_builds_a_datadog_trace_context_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::TraceContext {
                format: logit_config::TraceIdFormat::Datadog,
                trace_id: None,
                span_id: None,
                flags: None,
                trace_id_high: Some("_dd.p.tid".to_string()),
                keep_source: false,
                span: None,
            },
        };
        let NodeSpec::Transform(mut transform) =
            build_spec("trace", &component, Path::new(""), None).unwrap().0
        else {
            panic!("expected a Transform node");
        };

        let mut attrs = logit_core::AttrMap::new();
        attrs.insert("dd.trace_id", logit_core::Value::str("1311768467750121234"));
        attrs.insert("dd.span_id", logit_core::Value::str("42"));
        attrs.insert("_dd.p.tid", logit_core::Value::str("64de8e2b00000000"));
        let mut event = logit_core::Event::log(
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
        assert!(transform.process(&resource, &mut event), "should forward the event");
        let trace = event.log.expect("log should survive").trace.expect("trace should be lifted");
        assert_eq!(logit_core::trace::to_hex(&trace.trace_id), "64de8e2b0000000012345678abcdef12");
        assert_eq!(trace.span_id, Some(42_u64.to_be_bytes()));
        assert!(event.attributes.is_empty(), "all three consumed: {:?}", event.attributes);
    }

    /// Runs the built transform: the configured factor reaches `Scale::new`.
    #[test]
    fn build_spec_builds_a_working_scale_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
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
        let mut event = event;
        assert!(transform.process(&resource, &mut event), "should forward the event");
        match event.attributes.get("request_time") {
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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
            targets: Vec::new(),
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
    fn build_spec_builds_a_keep_values_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::KeepValues {
                resource: std::collections::BTreeMap::new(),
                attributes: std::collections::BTreeMap::from([(
                    "host".to_string(),
                    logit_config::ValueAllowList {
                        normalize: vec![logit_config::NormalizeStep::Lower],
                        allow: vec![logit_config::SetValue::Str("static.local".to_string())],
                        other: Some(logit_config::SetValue::Str("other".to_string())),
                    },
                )]),
            },
        };
        assert!(matches!(
            build_spec("keep_values", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Transform(_)
        ));
    }

    /// Runs the built transform: `FlattenFields`/`FlattenArrays` reach `Flatten::new`.
    #[test]
    fn build_spec_builds_a_working_flatten_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Flatten {
                attributes: logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::All),
                resource: logit_config::FlattenFields::Keyword(logit_config::FlattenKeyword::None),
                arrays: logit_config::FlattenArrays::Index,
            },
        };
        let NodeSpec::Transform(mut transform) =
            build_spec("flat", &component, Path::new(""), None).unwrap().0
        else {
            panic!("expected a Transform node");
        };

        let mut attrs = logit_core::AttrMap::new();
        let mut nested = logit_core::AttrMap::new();
        nested.insert("key", logit_core::Value::str("bar"));
        attrs.insert("foo", logit_core::Value::Map(Box::new(nested)));
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
        let mut event = event;
        assert!(transform.process(&resource, &mut event), "should forward the event");
        assert_eq!(
            event.attributes.get("foo.key"),
            Some(&logit_core::Value::str("bar")),
            "attributes: all (the default) should have expanded the nested attribute"
        );
    }

    /// Runs the built transform: routes, `route_other`, a `max_length` override, and `forwarded`
    /// reach it, and `CAPPED_FIELDS` defaults fill the rest.
    #[test]
    fn build_spec_builds_a_working_http_access_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::HttpAccess {
                routes: vec![
                    logit_config::HttpRouteRule {
                        builtin: Some(logit_config::HttpRouteSet::Assets),
                        ..Default::default()
                    },
                    logit_config::HttpRouteRule {
                        builtin: None,
                        pattern: Some("^/api/".to_string()),
                        route: Some("/api".to_string()),
                    },
                ],
                route_other: Some("/{other}".to_string()),
                user_agent_rules: vec![],
                max_length: std::collections::BTreeMap::from([("user.name".to_string(), 3)]),
                redact_query: vec![],
                forwarded: Some(logit_config::ForwardedConfig { trust: true }),
            },
        };
        let NodeSpec::Transform(mut transform) =
            build_spec("http", &component, Path::new(""), None).unwrap().0
        else {
            panic!("expected a Transform node");
        };

        let mut attrs = logit_core::AttrMap::new();
        attrs.insert("http.request.line", logit_core::Value::str("GET /api/x HTTP/1.1"));
        attrs.insert("http.response.status_code", logit_core::Value::str("503"));
        attrs.insert("user.name", logit_core::Value::str("alexandra"));
        attrs.insert("user_agent.original", logit_core::Value::str("x".repeat(300)));
        attrs.insert("http-request-header-x-forwarded-for", logit_core::Value::str("192.0.2.1"));
        let mut event = logit_core::Event::log(
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
        assert!(transform.process(&resource, &mut event), "should forward the event");
        let get = |key: &str| event.attributes.get(key).cloned();
        assert_eq!(get("http.route"), Some(logit_core::Value::str("/api")));
        assert_eq!(get("span.name"), Some(logit_core::Value::str("GET /api")));
        assert_eq!(get("http.response.status_code"), Some(logit_core::Value::I64(503)));
        assert_eq!(get("span.status"), Some(logit_core::Value::str("error")));
        assert_eq!(get("user.name"), Some(logit_core::Value::str("ale")), "the override");
        assert_eq!(
            get("user_agent.original").and_then(|v| v.as_str().map(str::len)),
            Some(256),
            "the CAPPED_FIELDS default for every field not overridden"
        );
        assert_eq!(get("client.address"), Some(logit_core::Value::str("192.0.2.1")), "trusted");
    }

    /// The component id becomes `shape`'s `tap` tag.
    #[test]
    fn build_spec_builds_a_shape_transform_carrying_its_own_id() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::Shape {
                interval: Duration::from_secs(10),
                resource: logit_config::ShapeResource::Drop,
                max_tracked_keys: 4096,
                max_tracked_keysets: 4096,
            },
        };
        let (spec, _) = build_spec("tap_a", &component, Path::new(""), None).unwrap();
        let NodeSpec::Transform(mut transform) = spec else {
            panic!("shape is a transform");
        };
        let resource = Arc::new(logit_core::Resource::default());
        let mut event = logit_core::Event::empty(0, logit_core::AttrMap::new());
        assert!(transform.process(&resource, &mut event));
        assert_eq!(
            event.attributes.get("tap").and_then(|v| v.as_str()),
            Some("tap_a"),
            "the registry's component id becomes the tap tag"
        );
    }

    #[test]
    fn build_spec_builds_a_has_provenance_transform() {
        let component = ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
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
            targets: Vec::new(),
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

    /// A `statsd_in` at either transport, with or without TLS.
    fn statsd_in_component(
        transport: logit_config::StatsdTransport,
        tls: Option<logit_config::TlsServerConfig>,
    ) -> ResolvedComponent {
        ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec![],
            targets: Vec::new(),
            consumers: vec!["out".to_string()],
            kind: ComponentKind::StatsdIn {
                bind: "127.0.0.1:0".to_string(),
                transport,
                tls,
                handshake_timeout: Duration::from_secs(5),
                idle_timeout: None,
            },
        }
    }

    #[test]
    fn build_spec_builds_a_tcp_statsd_input() {
        let component = statsd_in_component(logit_config::StatsdTransport::Tcp, None);
        assert!(matches!(
            build_spec("in", &component, Path::new(""), None).unwrap().0,
            NodeSpec::Input(..)
        ));
    }

    /// The TLS arm loads its cert; the missing-file half pins the `with_tls` call.
    #[test]
    fn build_spec_builds_a_tls_statsd_input() {
        let component = statsd_in_component(
            logit_config::StatsdTransport::Tcp,
            Some(logit_config::TlsServerConfig {
                cert_file: "server.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
        );
        assert!(matches!(
            build_spec("in", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Input(..)
        ));

        let missing = statsd_in_component(
            logit_config::StatsdTransport::Tcp,
            Some(logit_config::TlsServerConfig {
                cert_file: "does-not-exist.pem".to_string(),
                key_file: "server.key".to_string(),
                client_ca_file: None,
            }),
        );
        let err = match build_spec("in", &missing, &testdata_tls_dir(), None) {
            Ok(_) => panic!("expected a missing tls.cert_file to fail build_spec"),
            Err(err) => err,
        };
        let err = format!("{err:?}");
        assert!(err.contains("tls.cert_file"), "got: {err}");
        assert!(err.contains("does-not-exist.pem"), "got: {err}");
    }

    fn statsd_out_component(
        transport: logit_config::StatsdTransport,
        tls: Option<logit_config::TlsClientConfig>,
    ) -> ResolvedComponent {
        ResolvedComponent {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources: vec!["in".to_string()],
            targets: Vec::new(),
            consumers: vec![],
            kind: ComponentKind::StatsdOut {
                endpoint: "127.0.0.1:8125".to_string(),
                transport,
                format: logit_config::StatsdFormat::default(),
                relative_gauges: false,
                max_packet_bytes: 1432,
                connect_timeout: Duration::from_secs(5),
                tls,
            },
        }
    }

    /// A TCP `statsd_out` loads its CA; the missing-file half pins the `with_tls` call.
    #[test]
    fn build_spec_builds_a_tls_statsd_output() {
        let component = statsd_out_component(
            logit_config::StatsdTransport::Tcp,
            Some(logit_config::TlsClientConfig {
                ca_file: Some("ca.pem".to_string()),
                ..Default::default()
            }),
        );
        assert!(matches!(
            build_spec("out", &component, &testdata_tls_dir(), None).unwrap().0,
            NodeSpec::Output(..)
        ));

        let missing = statsd_out_component(
            logit_config::StatsdTransport::Tcp,
            Some(logit_config::TlsClientConfig {
                ca_file: Some("does-not-exist.pem".to_string()),
                ..Default::default()
            }),
        );
        let err = match build_spec("out", &missing, &testdata_tls_dir(), None) {
            Ok(_) => panic!("expected a missing tls.ca_file to fail build_spec"),
            Err(err) => err,
        };
        let err = format!("{err:?}");
        assert!(err.contains("tls.ca_file"), "got: {err}");
        assert!(err.contains("does-not-exist.pem"), "got: {err}");
    }
}
