//! `logit run`: resolves a config's named inputs/transforms/outputs into running pipelines and
//! drives them until one fails. See `docs/OVERVIEW.md` for the shape (`logit` as sidecar, host
//! agent, or central aggregator is all just this, differing only by config) and
//! `docs/design/lua-api.md` for the transform-chain contract this wires up.

use anyhow::Context;
use logit_config::{Config, InputConfig, OutputConfig, TransformConfig};
use logit_core::{Event, EventBatch};
use logit_inputs::statsd::StatsdInput;
use logit_inputs::Input;
use logit_outputs::influxdb::InfluxDbOutput;
use logit_outputs::Output;
use logit_script::{ProcessOutcome, ScriptWorker};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

/// Bounded channel capacity between an input/worker/output stage. Small and arbitrary -- just
/// enough to smooth out bursts without unbounded memory growth; revisit with real numbers once
/// there's a reason to.
const CHANNEL_CAPACITY: usize = 64;

/// Loads `path`, resolves every pipeline it defines, and runs them until the first task fails.
///
/// Every input/output task loops forever in normal operation (an input keeps listening, an output
/// keeps draining its channel), so in the happy path this simply never returns -- matching "this
/// is a service," not "this is a batch job." There's no graceful-shutdown handling yet (no
/// installed Ctrl-C handler, no drain of in-flight events on exit) -- Ctrl-C falls through to the
/// OS default (immediate termination), same as any other long-running process with no handler.
/// Worth a real look once there's actual in-flight state worth draining; there isn't yet.
pub async fn run_pipelines(path: PathBuf) -> anyhow::Result<()> {
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("reading config file {}", path.display()))?;
    let config: Config = serde_norway::from_str(&raw)
        .with_context(|| format!("parsing config file {}", path.display()))?;
    let base_dir = path.parent().map(Path::to_path_buf).unwrap_or_default();
    run_config(config, base_dir).await
}

/// The rest of [`run_pipelines`], split out so it's callable with an in-memory `Config` --
/// specifically so the validation this function does can be tested directly, without needing a
/// real file on disk or (since validation happens before anything is resolved) any real sockets
/// or HTTP calls either.
async fn run_config(config: Config, base_dir: PathBuf) -> anyhow::Result<()> {
    // Every check `logit validate` also needs to make -- empty pipelines/inputs/outputs, unknown
    // or double-claimed names, unimplemented kinds -- lives in `validate_semantics` so the two
    // paths can't silently disagree about what's acceptable (see its doc comment).
    validate_semantics(&config)?;

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    for (pipeline_name, pipeline) in &config.pipelines {
        // Outputs first, so the worker thread below can be handed their senders directly.
        let mut output_txs = Vec::with_capacity(pipeline.outputs.len());
        for output_name in &pipeline.outputs {
            let output_config = config.outputs.get(output_name).with_context(|| {
                format!("pipeline '{pipeline_name}' references unknown output '{output_name}'")
            })?;
            let mut output =
                build_output(output_config).with_context(|| format!("output '{output_name}'"))?;
            let (tx, mut rx) = mpsc::channel::<EventBatch>(CHANNEL_CAPACITY);
            let output_name = output_name.clone();
            tasks.spawn(async move {
                // `InfluxDbOutput::send` (and any other Output) already isolates per-event
                // encoding failures internally -- an `Err` reaching here means the whole write
                // failed (timeout, connection refused, a rejected request). Propagating via `?`
                // rather than logging and continuing means a destination that's persistently
                // broken (a bad token, a dead host) actually ends `logit run` with a real error,
                // matching this module's documented "runs until the first task fails" contract --
                // the alternative is reporting healthy forever while silently dropping every
                // batch. There's no retry/backoff to soften this with yet (output buffering is
                // still just a trait, no implementation -- logit-proto/src/buffer.rs); that's
                // where a real answer to "don't die on one transient blip" belongs, not here.
                while let Some(batch) = rx.recv().await {
                    output.send(batch).await.with_context(|| format!("output '{output_name}'"))?;
                }
                Ok(())
            });
            output_txs.push(tx);
        }

        // This pipeline's script *sources* -- not `ScriptWorker`s yet. `ScriptWorker` opts out of
        // both `Send` and `Sync` (`docs/design/lua-api.md`'s concurrency section), so it can't be
        // *moved* into the dedicated thread below at all, let alone held across an `.await`;
        // `ScriptWorker::new` has to run on that thread itself, not here. Any builtin transform is
        // a config-time error -- none are implemented yet (not just `aggregate`), so this states
        // current capability honestly rather than special-casing one variant.
        let mut script_sources = Vec::with_capacity(pipeline.transforms.len());
        for transform in &pipeline.transforms {
            let source = match transform {
                TransformConfig::Lua { lua } => lua.clone(),
                TransformConfig::LuaFile { lua_file } => {
                    let script_path = base_dir.join(lua_file);
                    std::fs::read_to_string(&script_path)
                        .with_context(|| format!("reading lua_file {}", script_path.display()))?
                }
                TransformConfig::Builtin(builtin) => {
                    anyhow::bail!(
                        "pipeline '{pipeline_name}': builtin transform {builtin:?} is not \
                         implemented yet"
                    );
                }
            };
            script_sources.push(source);
        }

        // Fail startup on a bad script, not silently later -- matches `ScriptWorker::new`'s own
        // "fail at load time" contract, just applied one level up. `oneshot::Sender::send` is
        // synchronous (no `.await` needed), so the worker thread can report back over it directly.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

        let (batch_tx, batch_rx) = mpsc::channel::<EventBatch>(CHANNEL_CAPACITY);
        let worker_pipeline_name = pipeline_name.clone();
        std::thread::Builder::new()
            .name(format!("logit-pipeline-{pipeline_name}"))
            .spawn(move || {
                run_pipeline_worker(
                    worker_pipeline_name,
                    script_sources,
                    ready_tx,
                    batch_rx,
                    output_txs,
                )
            })
            .with_context(|| format!("spawning worker thread for pipeline '{pipeline_name}'"))?;
        match ready_rx.await {
            Ok(Ok(())) => {}
            Ok(Err(message)) => anyhow::bail!("pipeline '{pipeline_name}': {message}"),
            Err(_) => anyhow::bail!(
                "pipeline '{pipeline_name}': worker thread exited before reporting ready"
            ),
        }

        for input_name in &pipeline.inputs {
            let input_config = config.inputs.get(input_name).with_context(|| {
                format!("pipeline '{pipeline_name}' references unknown input '{input_name}'")
            })?;
            let mut input =
                build_input(input_config).with_context(|| format!("input '{input_name}'"))?;
            let tx = batch_tx.clone();
            let input_name = input_name.clone();
            tasks.spawn(async move {
                input.run(tx).await.with_context(|| format!("input '{input_name}'"))
            });
        }
    }

    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

/// Semantic checks a `Config` must pass beyond what serde/the generated schema already enforce --
/// non-empty pipelines/inputs/outputs, no unknown or double-claimed input/output names, no
/// unimplemented input/output/transform kind. Shared by `logit validate` and `run_config` (called
/// first thing, before resolving or spawning anything) so the two can never again disagree about
/// whether a config is acceptable -- `validate` silently passing a config `run` rejects was a real
/// review finding on this same module, not a hypothetical.
///
/// Deliberately stops short of what `run_config` checks *after* this: it doesn't read
/// `token_env` from the environment or try to load Lua source, since either would change
/// `logit validate`'s contract from "is this config structurally valid" to "could I run this right
/// now" -- a bigger, separate decision from the empty-collection gap this closes.
pub fn validate_semantics(config: &Config) -> anyhow::Result<()> {
    if config.pipelines.is_empty() {
        anyhow::bail!("config defines no pipelines");
    }

    // A named input/output claimed by more than one pipeline is a config-time error, not
    // silently-wrong behavior (e.g. two pipelines both trying to bind the same UDP port). Real
    // fan-out/fan-in support is a legitimate future need, but nothing today's configs need.
    let mut claimed_inputs: HashSet<&str> = HashSet::new();
    let mut claimed_outputs: HashSet<&str> = HashSet::new();

    for (pipeline_name, pipeline) in &config.pipelines {
        // `PipelineConfig.inputs`/`outputs` are `minItems: 1` in the generated schema (a
        // documentation-level match to this check -- schemars' `length` attribute doesn't add
        // runtime validation of its own, so this is still the check that actually enforces it).
        // Left unenforced: `outputs: []` would have `run_pipeline_worker` silently discard every
        // transformed batch forever (its `output_txs.split_last()` is `None`) while the input
        // keeps consuming -- telemetry in, nothing out, no error anywhere. `inputs: []` would spawn
        // no input tasks, so the pipeline's `batch_tx` drops at the end of that loop iteration,
        // closing the worker thread's channel almost immediately -- if it's the only pipeline,
        // `run_pipelines` can return `Ok(())` having done nothing at all.
        if pipeline.inputs.is_empty() {
            anyhow::bail!(
                "pipeline '{pipeline_name}' has no inputs -- it would never receive any events"
            );
        }
        if pipeline.outputs.is_empty() {
            anyhow::bail!(
                "pipeline '{pipeline_name}' has no outputs -- any events it processes would be \
                 silently discarded"
            );
        }

        for output_name in &pipeline.outputs {
            if !claimed_outputs.insert(output_name) {
                anyhow::bail!(
                    "output '{output_name}' is referenced by more than one pipeline -- not yet supported"
                );
            }
            let output_config = config.outputs.get(output_name).with_context(|| {
                format!("pipeline '{pipeline_name}' references unknown output '{output_name}'")
            })?;
            require_implemented_output(output_config)?;
        }

        for transform in &pipeline.transforms {
            if let TransformConfig::Builtin(builtin) = transform {
                anyhow::bail!(
                    "pipeline '{pipeline_name}': builtin transform {builtin:?} is not \
                     implemented yet"
                );
            }
        }

        for input_name in &pipeline.inputs {
            if !claimed_inputs.insert(input_name) {
                anyhow::bail!(
                    "input '{input_name}' is referenced by more than one pipeline -- not yet supported"
                );
            }
            let input_config = config.inputs.get(input_name).with_context(|| {
                format!("pipeline '{pipeline_name}' references unknown input '{input_name}'")
            })?;
            require_implemented_input(input_config)?;
        }
    }

    Ok(())
}

/// The single source of truth for which `InputConfig` kinds `build_input` can actually construct
/// -- split out so `validate_semantics` can reject the same unimplemented kinds `logit run` does
/// without the side effects (env reads, socket/HTTP setup) real construction can carry.
fn require_implemented_input(config: &InputConfig) -> anyhow::Result<()> {
    match config {
        InputConfig::Statsd { .. } => Ok(()),
        other => anyhow::bail!("input kind {other:?} is not implemented yet"),
    }
}

fn build_input(config: &InputConfig) -> anyhow::Result<Box<dyn Input + Send>> {
    require_implemented_input(config)?;
    let InputConfig::Statsd { bind } = config else {
        unreachable!("require_implemented_input already rejected every other kind");
    };
    Ok(Box::new(StatsdInput { bind: bind.clone() }))
}

/// See [`require_implemented_input`] -- same reasoning, for outputs. `build_output` alone reads
/// `token_env` from the environment; this doesn't, so `validate_semantics` can use it without
/// changing `logit validate`'s contract.
fn require_implemented_output(config: &OutputConfig) -> anyhow::Result<()> {
    match config {
        OutputConfig::InfluxDb { .. } => Ok(()),
        other => anyhow::bail!("output kind {other:?} is not implemented yet"),
    }
}

fn build_output(config: &OutputConfig) -> anyhow::Result<Box<dyn Output + Send>> {
    require_implemented_output(config)?;
    let OutputConfig::InfluxDb { url, org, bucket, token_env } = config else {
        unreachable!("require_implemented_output already rejected every other kind");
    };
    let token = std::env::var(token_env).with_context(|| {
        format!("environment variable '{token_env}' (referenced by output config) is not set")
    })?;
    Ok(Box::new(InfluxDbOutput::new(url.clone(), org.clone(), bucket.clone(), token)))
}

/// Runs `events` through `workers` in order (`docs/design/lua-api.md`'s `process()` contract:
/// `Emit`/`EmitMany`/`Drop`). A script error is logged and treated as a drop for that one event,
/// not an abort of the whole batch -- matches this project's established "one bad item doesn't
/// take down everything alongside it" policy (`logit-inputs::statsd`, `logit-outputs::influxdb`).
/// A pure function (no channels/threads) specifically so this logic is directly unit-testable.
fn apply_transforms(
    pipeline_name: &str,
    workers: &[ScriptWorker],
    events: Vec<Event>,
) -> Vec<Event> {
    let mut events = events;
    for worker in workers {
        let mut next = Vec::with_capacity(events.len());
        for event in events {
            match worker.process(event) {
                Ok(ProcessOutcome::Emit(e)) => next.push(*e),
                Ok(ProcessOutcome::EmitMany(es)) => next.extend(es),
                Ok(ProcessOutcome::Drop) => {}
                Err(err) => {
                    // TODO: route through a proper diagnostics facility once one exists, instead
                    // of stderr -- same gap noted in logit-inputs::statsd/logit-outputs::influxdb.
                    eprintln!("pipeline '{pipeline_name}': script error: {err}");
                }
            }
        }
        events = next;
    }
    events
}

/// Builds this pipeline's `Vec<ScriptWorker>` and owns them for the pipeline's lifetime, all on a
/// dedicated OS thread. `ScriptWorker` opts out of both `Send` and `Sync`
/// (`docs/design/lua-api.md`'s concurrency section, enforced in `logit-script` via a `PhantomData`
/// marker) -- it can't be *moved* into a thread at all, so construction has to happen here, not
/// on the caller's side with the result handed in. `blocking_recv`/`blocking_send` bridge to and
/// from the async world on either side, exactly what they're for from a plain thread like this.
///
/// Reports success/failure of the initial script loads over `ready_tx` before entering the receive
/// loop, so a bad script fails `logit run` at startup instead of silently running a broken
/// pipeline that only ever logs errors into the void.
fn run_pipeline_worker(
    pipeline_name: String,
    script_sources: Vec<String>,
    ready_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
    mut batch_rx: mpsc::Receiver<EventBatch>,
    output_txs: Vec<mpsc::Sender<EventBatch>>,
) {
    let workers: Vec<ScriptWorker> = match script_sources
        .iter()
        .map(|source| ScriptWorker::new(source))
        .collect::<Result<_, _>>()
    {
        Ok(workers) => workers,
        Err(err) => {
            // The receiver may already be gone if `run_pipelines` bailed for an unrelated reason
            // first; nothing useful to do with that here.
            let _ = ready_tx.send(Err(format!("loading a transform script: {err}")));
            return;
        }
    };
    // Ignoring a send failure for the same reason as above.
    let _ = ready_tx.send(Ok(()));

    while let Some(batch) = batch_rx.blocking_recv() {
        let events = apply_transforms(&pipeline_name, &workers, batch.events);
        if events.is_empty() {
            continue;
        }
        let out_batch = EventBatch { resource: batch.resource, events };
        // Clone for every output but the last, so the final send can move the batch instead.
        if let Some((last, rest)) = output_txs.split_last() {
            for tx in rest {
                let _ = tx.blocking_send(out_batch.clone());
            }
            let _ = last.blocking_send(out_batch);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_config::{BuiltinTransformConfig, PipelineConfig};
    use logit_core::{interner::intern, AttrMap, MetricKind, MetricRecord, Payload};
    use std::collections::HashMap;

    fn influxdb_output_config() -> OutputConfig {
        OutputConfig::InfluxDb {
            url: "http://localhost:8086".to_string(),
            org: "org".to_string(),
            bucket: "bucket".to_string(),
            token_env: "LOGIT_TEST_DEFINITELY_UNSET_TOKEN_VAR".to_string(),
        }
    }

    #[tokio::test]
    async fn pipeline_with_no_inputs_is_rejected() {
        // Otherwise: no input tasks get spawned, this pipeline's batch_tx drops at the end of the
        // loop iteration, and (if it's the only pipeline) run_pipelines can return Ok(()) having
        // done nothing at all -- no error, no sign anything is wrong.
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig { inputs: vec![], transforms: vec![], outputs: vec!["out".to_string()] },
        );
        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), influxdb_output_config());
        let config = Config { inputs: HashMap::new(), outputs, pipelines };

        let err = expect_err(run_config(config, PathBuf::new()).await);
        assert!(err.to_string().contains("no inputs"), "got: {err}");
    }

    #[tokio::test]
    async fn pipeline_with_no_outputs_is_rejected() {
        // Otherwise: run_pipeline_worker's output_txs.split_last() is None, so every transformed
        // batch is silently discarded forever while the input keeps consuming -- telemetry in,
        // nothing out, no error anywhere.
        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), InputConfig::Statsd { bind: "127.0.0.1:0".to_string() });
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig { inputs: vec!["in".to_string()], transforms: vec![], outputs: vec![] },
        );
        let config = Config { inputs, outputs: HashMap::new(), pipelines };

        let err = expect_err(run_config(config, PathBuf::new()).await);
        assert!(err.to_string().contains("no outputs"), "got: {err}");
    }

    /// The exact function `logit validate` calls (`main.rs`'s `Command::Validate` arm) -- there's
    /// no CLI-subprocess test harness in this workspace to exercise that arm any more directly,
    /// so testing `validate_semantics` itself is testing what `validate` actually runs.
    #[test]
    fn validate_semantics_rejects_no_pipelines() {
        let config =
            Config { inputs: HashMap::new(), outputs: HashMap::new(), pipelines: HashMap::new() };
        let err = expect_err(validate_semantics(&config));
        assert!(err.to_string().contains("no pipelines"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_empty_inputs() {
        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), influxdb_output_config());
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig { inputs: vec![], transforms: vec![], outputs: vec!["out".to_string()] },
        );
        let config = Config { inputs: HashMap::new(), outputs, pipelines };
        let err = expect_err(validate_semantics(&config));
        assert!(err.to_string().contains("no inputs"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_empty_outputs() {
        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), InputConfig::Statsd { bind: "127.0.0.1:0".to_string() });
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig { inputs: vec!["in".to_string()], transforms: vec![], outputs: vec![] },
        );
        let config = Config { inputs, outputs: HashMap::new(), pipelines };
        let err = expect_err(validate_semantics(&config));
        assert!(err.to_string().contains("no outputs"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_unknown_output_reference() {
        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), InputConfig::Statsd { bind: "127.0.0.1:0".to_string() });
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig {
                inputs: vec!["in".to_string()],
                transforms: vec![],
                outputs: vec!["missing".to_string()],
            },
        );
        let config = Config { inputs, outputs: HashMap::new(), pipelines };
        let err = expect_err(validate_semantics(&config));
        assert!(err.to_string().contains("unknown output 'missing'"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_unknown_input_reference() {
        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), influxdb_output_config());
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig {
                inputs: vec!["missing".to_string()],
                transforms: vec![],
                outputs: vec!["out".to_string()],
            },
        );
        let config = Config { inputs: HashMap::new(), outputs, pipelines };
        let err = expect_err(validate_semantics(&config));
        assert!(err.to_string().contains("unknown input 'missing'"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_output_claimed_by_two_pipelines() {
        let mut inputs = HashMap::new();
        inputs.insert("in1".to_string(), InputConfig::Statsd { bind: "127.0.0.1:0".to_string() });
        inputs.insert("in2".to_string(), InputConfig::Statsd { bind: "127.0.0.1:0".to_string() });
        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), influxdb_output_config());
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p1".to_string(),
            PipelineConfig {
                inputs: vec!["in1".to_string()],
                transforms: vec![],
                outputs: vec!["out".to_string()],
            },
        );
        pipelines.insert(
            "p2".to_string(),
            PipelineConfig {
                inputs: vec!["in2".to_string()],
                transforms: vec![],
                outputs: vec!["out".to_string()],
            },
        );
        let config = Config { inputs, outputs, pipelines };
        let err = expect_err(validate_semantics(&config));
        assert!(err.to_string().contains("more than one pipeline"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_unimplemented_output_kind() {
        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), InputConfig::Statsd { bind: "127.0.0.1:0".to_string() });
        let mut outputs = HashMap::new();
        outputs.insert(
            "out".to_string(),
            OutputConfig::Otlp { endpoint: "http://localhost:4317".to_string() },
        );
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig {
                inputs: vec!["in".to_string()],
                transforms: vec![],
                outputs: vec!["out".to_string()],
            },
        );
        let config = Config { inputs, outputs, pipelines };
        let err = expect_err(validate_semantics(&config));
        assert!(err.to_string().contains("not implemented yet"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_unimplemented_input_kind() {
        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), InputConfig::Otlp { bind: "0.0.0.0:4317".to_string() });
        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), influxdb_output_config());
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig {
                inputs: vec!["in".to_string()],
                transforms: vec![],
                outputs: vec!["out".to_string()],
            },
        );
        let config = Config { inputs, outputs, pipelines };
        let err = expect_err(validate_semantics(&config));
        assert!(err.to_string().contains("not implemented yet"), "got: {err}");
    }

    #[test]
    fn validate_semantics_rejects_builtin_transform() {
        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), InputConfig::Statsd { bind: "127.0.0.1:0".to_string() });
        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), influxdb_output_config());
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig {
                inputs: vec!["in".to_string()],
                transforms: vec![TransformConfig::Builtin(BuiltinTransformConfig::Json)],
                outputs: vec!["out".to_string()],
            },
        );
        let config = Config { inputs, outputs, pipelines };
        let err = expect_err(validate_semantics(&config));
        assert!(err.to_string().contains("not implemented yet"), "got: {err}");
    }

    #[test]
    fn validate_semantics_accepts_a_well_formed_config() {
        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), InputConfig::Statsd { bind: "127.0.0.1:0".to_string() });
        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), influxdb_output_config());
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig {
                inputs: vec!["in".to_string()],
                transforms: vec![],
                outputs: vec!["out".to_string()],
            },
        );
        let config = Config { inputs, outputs, pipelines };
        assert!(validate_semantics(&config).is_ok());
    }

    fn counter_event(name: &str, value: f64) -> Event {
        Event {
            timestamp: 0,
            attributes: AttrMap::new(),
            payload: Payload::Metric(MetricRecord {
                name: intern(name),
                kind: MetricKind::Counter(value),
                unit: None,
            }),
        }
    }

    /// `Box<dyn Input + Send>`/`Box<dyn Output + Send>` aren't `Debug`, so `Result::unwrap_err`
    /// (which needs `Debug` on the `Ok` side to format its panic message) doesn't work here --
    /// same reason `logit-script` has its own `expect_err` helper.
    fn expect_err<T>(result: anyhow::Result<T>) -> anyhow::Error {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(err) => err,
        }
    }

    #[test]
    fn build_input_rejects_unimplemented_kinds() {
        let err = expect_err(build_input(&InputConfig::Otlp { bind: "0.0.0.0:4317".to_string() }));
        assert!(err.to_string().contains("not implemented yet"));
    }

    #[test]
    fn build_output_rejects_unimplemented_kinds() {
        let err = expect_err(build_output(&OutputConfig::Otlp {
            endpoint: "http://localhost:4317".to_string(),
        }));
        assert!(err.to_string().contains("not implemented yet"));
    }

    #[test]
    fn build_output_reports_missing_token_env_clearly() {
        let config = OutputConfig::InfluxDb {
            url: "http://localhost:8086".to_string(),
            org: "org".to_string(),
            bucket: "bucket".to_string(),
            token_env: "LOGIT_TEST_DEFINITELY_UNSET_TOKEN_VAR".to_string(),
        };
        let err = expect_err(build_output(&config));
        assert!(err.to_string().contains("LOGIT_TEST_DEFINITELY_UNSET_TOKEN_VAR"));
    }

    #[test]
    fn apply_transforms_chains_scripts_in_order() {
        let w1 = ScriptWorker::new(
            r#"function process(event) event.attributes.stage1 = "yes" return event end"#,
        )
        .unwrap();
        let w2 = ScriptWorker::new(
            r#"function process(event) event.attributes.stage2 = "yes" return event end"#,
        )
        .unwrap();
        let events = apply_transforms("test", &[w1, w2], vec![counter_event("hits", 1.0)]);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].attributes.get("stage1").and_then(|v| v.as_str()), Some("yes"));
        assert_eq!(events[0].attributes.get("stage2").and_then(|v| v.as_str()), Some("yes"));
    }

    #[test]
    fn apply_transforms_respects_drop() {
        let w = ScriptWorker::new("function process(event) return nil end").unwrap();
        let events = apply_transforms("test", &[w], vec![counter_event("hits", 1.0)]);
        assert!(events.is_empty());
    }

    #[test]
    fn apply_transforms_respects_fan_out() {
        let w =
            ScriptWorker::new("function process(event) return {event, event:clone()} end").unwrap();
        let events = apply_transforms("test", &[w], vec![counter_event("hits", 1.0)]);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn apply_transforms_with_no_workers_is_a_passthrough() {
        let events = apply_transforms("test", &[], vec![counter_event("hits", 1.0)]);
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn apply_transforms_logs_and_drops_on_script_error() {
        let w = ScriptWorker::new(r#"function process(event) error("boom") end"#).unwrap();
        let events = apply_transforms("test", &[w], vec![counter_event("hits", 1.0)]);
        assert!(events.is_empty());
    }
}
