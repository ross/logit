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

    if config.pipelines.is_empty() {
        anyhow::bail!("config defines no pipelines");
    }

    // A named input/output claimed by more than one pipeline is a config-time error, not
    // silently-wrong behavior (e.g. two pipelines both trying to bind the same UDP port). Real
    // fan-out/fan-in support is a legitimate future need, but nothing today's configs need.
    let mut claimed_inputs: HashSet<String> = HashSet::new();
    let mut claimed_outputs: HashSet<String> = HashSet::new();
    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();

    for (pipeline_name, pipeline) in &config.pipelines {
        // Outputs first, so the worker thread below can be handed their senders directly.
        let mut output_txs = Vec::with_capacity(pipeline.outputs.len());
        for output_name in &pipeline.outputs {
            if !claimed_outputs.insert(output_name.clone()) {
                anyhow::bail!(
                    "output '{output_name}' is referenced by more than one pipeline -- not yet supported"
                );
            }
            let output_config = config.outputs.get(output_name).with_context(|| {
                format!("pipeline '{pipeline_name}' references unknown output '{output_name}'")
            })?;
            let mut output =
                build_output(output_config).with_context(|| format!("output '{output_name}'"))?;
            let (tx, mut rx) = mpsc::channel::<EventBatch>(CHANNEL_CAPACITY);
            let output_name = output_name.clone();
            tasks.spawn(async move {
                while let Some(batch) = rx.recv().await {
                    if let Err(err) = output.send(batch).await {
                        // TODO: route through a proper diagnostics facility once one exists,
                        // instead of stderr -- same gap noted in logit-inputs::statsd and
                        // logit-outputs::influxdb.
                        eprintln!("output '{output_name}': {err:#}");
                    }
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
            if !claimed_inputs.insert(input_name.clone()) {
                anyhow::bail!(
                    "input '{input_name}' is referenced by more than one pipeline -- not yet supported"
                );
            }
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

fn build_input(config: &InputConfig) -> anyhow::Result<Box<dyn Input + Send>> {
    match config {
        InputConfig::Statsd { bind } => Ok(Box::new(StatsdInput { bind: bind.clone() })),
        other => anyhow::bail!("input kind {other:?} is not implemented yet"),
    }
}

fn build_output(config: &OutputConfig) -> anyhow::Result<Box<dyn Output + Send>> {
    match config {
        OutputConfig::InfluxDb { url, org, bucket, token_env } => {
            let token = std::env::var(token_env).with_context(|| {
                format!(
                    "environment variable '{token_env}' (referenced by output config) is not set"
                )
            })?;
            Ok(Box::new(InfluxDbOutput::new(url.clone(), org.clone(), bucket.clone(), token)))
        }
        other => anyhow::bail!("output kind {other:?} is not implemented yet"),
    }
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
    use logit_core::{interner::intern, AttrMap, MetricKind, MetricRecord, Payload};

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
