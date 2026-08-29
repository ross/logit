//! `logit run`: resolves a config's named inputs/transforms/outputs into running pipelines and
//! drives them until one fails. See `docs/OVERVIEW.md` for the shape (`logit` as sidecar, host
//! agent, or central aggregator is all just this, differing only by config) and
//! `docs/design/lua-api.md` for the transform-chain contract this wires up.

use anyhow::Context;
use logit_config::{BuiltinTransformConfig, Config, InputConfig, OutputConfig, TransformConfig};
use logit_core::{Event, EventBatch, Resource};
use logit_inputs::statsd::StatsdInput;
use logit_inputs::Input;
use logit_outputs::influxdb::InfluxDbOutput;
use logit_outputs::Output;
use logit_script::{ProcessOutcome, ScriptWorker};
use logit_transforms::Aggregator;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
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

        // This pipeline's transform *specs* -- not built `Stage`s yet. `ScriptWorker` opts out of
        // both `Send` and `Sync` (`docs/design/lua-api.md`'s concurrency section), so it can't be
        // *moved* into the dedicated thread below at all, let alone held across an `.await`;
        // `ScriptWorker::new` (and building an `Aggregator`, which is `Send` but has to sit
        // alongside `ScriptWorker`s in the same ordered chain) has to happen on that thread
        // itself, not here. `require_implemented_transform` -- called by `validate_semantics`
        // above -- already rejects any transform that doesn't fit one of the three arms below, so
        // the fallback arm is unreachable, not a silent gap.
        let mut transform_specs = Vec::with_capacity(pipeline.transforms.len());
        for transform in &pipeline.transforms {
            let spec = match transform {
                TransformConfig::Lua { lua, interval } => {
                    TransformSpec::Lua { source: lua.clone(), flush_interval: *interval }
                }
                TransformConfig::LuaFile { lua_file, interval } => {
                    let script_path = base_dir.join(lua_file);
                    let source = std::fs::read_to_string(&script_path)
                        .with_context(|| format!("reading lua_file {}", script_path.display()))?;
                    TransformSpec::Lua { source, flush_interval: *interval }
                }
                TransformConfig::Builtin(BuiltinTransformConfig::Aggregate { interval }) => {
                    TransformSpec::Aggregate { interval: *interval }
                }
                TransformConfig::Builtin(_) => {
                    unreachable!("validate_semantics already rejected every unimplemented builtin")
                }
            };
            transform_specs.push(spec);
        }

        // Fail startup on a bad script, not silently later -- matches `ScriptWorker::new`'s own
        // "fail at load time" contract, just applied one level up. `oneshot::Sender::send` is
        // synchronous (no `.await` needed), so the worker thread can report back over it directly.
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel::<Result<(), String>>();

        // `Handle::current()` must be called from inside the runtime -- true here since
        // `run_config` is itself running as an async task on it -- and can then be used to drive
        // timers from the plain OS thread below via `Handle::block_on`, which is legal off the
        // runtime's own worker threads as long as it's never called from *within* an async
        // context (an `.await` on this same runtime). That's exactly the shape here: a bare
        // `std::thread`, not a tokio task.
        let runtime = tokio::runtime::Handle::current();

        let (batch_tx, batch_rx) = mpsc::channel::<EventBatch>(CHANNEL_CAPACITY);
        let worker_pipeline_name = pipeline_name.clone();
        std::thread::Builder::new()
            .name(format!("logit-pipeline-{pipeline_name}"))
            .spawn(move || {
                run_pipeline_worker(
                    worker_pipeline_name,
                    transform_specs,
                    ready_tx,
                    batch_rx,
                    output_txs,
                    runtime,
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
            require_implemented_transform(transform)?;
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

/// The single source of truth for which transform stages `run_config`'s spec-building loop can
/// actually build -- `Lua`/`LuaFile` (any interval) and the `aggregate` builtin; every other
/// builtin isn't implemented yet. Split out for the same reason as
/// [`require_implemented_input`]/[`require_implemented_output`]: `validate_semantics` and
/// `run_config` used to reject builtin transforms in two separate places with two separate
/// copies of the message -- precisely the shape of bug that produced those two helpers.
///
/// Also where a zero flush interval is rejected, for either kind of stage: the hand-rolled
/// duration codec (`logit-config`) happily accepts `0s`, but `run_pipeline_worker`'s flush
/// schedule treats "due now" as "due again immediately" for a zero interval, which would spin the
/// worker thread. This is pure config validation (no env reads, no Lua compilation), so it
/// belongs here, not only checked once a pipeline actually starts.
fn require_implemented_transform(transform: &TransformConfig) -> anyhow::Result<()> {
    match transform {
        TransformConfig::Lua { interval, .. } | TransformConfig::LuaFile { interval, .. } => {
            require_nonzero_interval(*interval)
        }
        TransformConfig::Builtin(BuiltinTransformConfig::Aggregate { interval }) => {
            require_nonzero_interval(Some(*interval))
        }
        TransformConfig::Builtin(builtin) => {
            anyhow::bail!("builtin transform {builtin:?} is not implemented yet")
        }
    }
}

fn require_nonzero_interval(interval: Option<Duration>) -> anyhow::Result<()> {
    if interval == Some(Duration::ZERO) {
        anyhow::bail!("a flush interval of 0s would flush continuously -- use a positive duration");
    }
    Ok(())
}

/// A Send-able description of one transform stage, built in `run_config` (where `base_dir` and the
/// filesystem are reachable) and handed to the worker thread, which turns each into a [`Stage`].
/// `ScriptWorker` is `!Send`, so as before, only plain data crosses the thread boundary --
/// construction happens on the worker thread itself.
enum TransformSpec {
    Lua { source: String, flush_interval: Option<Duration> },
    Aggregate { interval: Duration },
}

/// One stage of a pipeline's transform chain, built from a [`TransformSpec`] on the worker
/// thread. A plain enum, not `Box<dyn Trait>`: the whole chain lives on one OS thread with no
/// `Send`/object-safety pressure pushing toward dynamic dispatch, matching how `build_input`/
/// `build_output` already dispatch on config kind.
enum Stage {
    Lua { worker: ScriptWorker, flush_interval: Option<Duration> },
    Aggregate(Aggregator),
}

impl Stage {
    fn build(spec: TransformSpec) -> Result<Self, logit_script::ScriptError> {
        Ok(match spec {
            TransformSpec::Lua { source, flush_interval } => {
                Stage::Lua { worker: ScriptWorker::new(&source)?, flush_interval }
            }
            TransformSpec::Aggregate { interval } => Stage::Aggregate(Aggregator::new(interval)),
        })
    }

    /// `None` means this stage never flushes -- a Lua stage with no configured `interval`, same
    /// as a script with no `flush()` at all. Every `Aggregate` stage has one; it's the only way
    /// to construct one (see `TransformSpec::Aggregate`, `require_implemented_transform`'s
    /// zero-interval rejection).
    fn flush_interval(&self) -> Option<Duration> {
        match self {
            Stage::Lua { flush_interval, .. } => *flush_interval,
            Stage::Aggregate(agg) => Some(agg.interval()),
        }
    }
}

/// Runs `events` through `stages` in order. A Lua stage follows `docs/design/lua-api.md`'s
/// `process()` contract (`Emit`/`EmitMany`/`Drop`); a script error is logged and treated as a drop
/// for that one event, not an abort of the whole batch -- matches this project's established "one
/// bad item doesn't take down everything alongside it" policy (`logit-inputs::statsd`,
/// `logit-outputs::influxdb`). An `Aggregate` stage accumulates what it can and passes everything
/// else through untouched (logs, spans, and metric kinds with no defined merge rule -- never
/// silently dropped).
///
/// `resource` identifies which of an `Aggregate` stage's per-resource windows `events` belongs to;
/// see [`Aggregator::process`]. `&mut [Stage]` (not `&[ScriptWorker]` as before `Aggregate`
/// existed): an aggregator mutates state per event, unlike a Lua stage's `&self` `process`.
fn apply_transforms(
    pipeline_name: &str,
    stages: &mut [Stage],
    resource: &Arc<Resource>,
    events: Vec<Event>,
) -> Vec<Event> {
    let mut events = events;
    for stage in stages.iter_mut() {
        let mut next = Vec::with_capacity(events.len());
        match stage {
            Stage::Lua { worker, .. } => {
                for event in events {
                    match worker.process(event) {
                        Ok(ProcessOutcome::Emit(e)) => next.push(*e),
                        Ok(ProcessOutcome::EmitMany(es)) => next.extend(es),
                        Ok(ProcessOutcome::Drop) => {}
                        Err(err) => {
                            // TODO: route through a proper diagnostics facility once one exists,
                            // instead of stderr -- same gap noted in
                            // logit-inputs::statsd/logit-outputs::influxdb.
                            eprintln!("pipeline '{pipeline_name}': script error: {err}");
                        }
                    }
                }
            }
            Stage::Aggregate(agg) => {
                for event in events {
                    if let Some(passed_through) = agg.process(resource, event) {
                        next.push(passed_through);
                    }
                }
            }
        }
        events = next;
    }
    events
}

/// Flushes stage `index` (an aggregate window tick or a Lua `flush()` tick) and runs whatever it
/// emits through every later stage in the chain (`index + 1..`) via [`apply_transforms`] -- a
/// flushed event is not exempt from downstream enrichment/filtering, it's a source of events into
/// the rest of the chain like any other. `split_at_mut` is what makes "mutate stage `index` while
/// also mutating the stages after it" expressible as two disjoint `&mut` slices.
///
/// `fallback_resource` is used only for a Lua stage's `flush()`, which -- unlike a normal batch,
/// or an `Aggregate` stage's own per-resource windows -- has no resource of its own to stamp its
/// emitted events with.
fn flush_stage(
    pipeline_name: &str,
    stages: &mut [Stage],
    index: usize,
    now: i64,
    fallback_resource: &Arc<Resource>,
) -> Vec<(Arc<Resource>, Vec<Event>)> {
    let (head, tail) = stages.split_at_mut(index + 1);
    let groups: Vec<(Arc<Resource>, Vec<Event>)> = match &mut head[index] {
        Stage::Lua { worker, .. } => match worker.flush() {
            Ok(events) if events.is_empty() => Vec::new(),
            Ok(events) => vec![(fallback_resource.clone(), events)],
            Err(err) => {
                eprintln!("pipeline '{pipeline_name}': script flush error: {err}");
                Vec::new()
            }
        },
        Stage::Aggregate(agg) => agg.flush(now),
    };
    groups
        .into_iter()
        .map(|(resource, events)| {
            let events = apply_transforms(pipeline_name, tail, &resource, events);
            (resource, events)
        })
        .filter(|(_, events)| !events.is_empty())
        .collect()
}

/// Sends `events` to every output, cloning the batch for all but the last send so the final one
/// can move it instead. A no-op if `events` is empty (nothing to send, and an empty `EventBatch`
/// would be a pointless wakeup for every output task).
fn send_batch(
    output_txs: &[mpsc::Sender<EventBatch>],
    resource: Arc<Resource>,
    events: Vec<Event>,
) {
    if events.is_empty() {
        return;
    }
    let out_batch = EventBatch { resource, events };
    if let Some((last, rest)) = output_txs.split_last() {
        for tx in rest {
            let _ = tx.blocking_send(out_batch.clone());
        }
        let _ = last.blocking_send(out_batch);
    }
}

fn now_unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// Flushes every stage whose deadline has passed, in chain order, sending whatever each one
/// (and, transitively, the stages after it) emits. Each due stage's deadline advances by whole
/// interval steps from where it was -- not from `now` -- so the schedule doesn't drift, and a
/// stall long enough to miss several ticks collapses into one flush rather than a burst of
/// catch-up flushes for a tumbling-window aggregator, which would otherwise emit one real window
/// followed by several empty ones.
fn flush_due_stages(
    pipeline_name: &str,
    stages: &mut [Stage],
    next_flush: &mut [Option<tokio::time::Instant>],
    fallback_resource: &Arc<Resource>,
    output_txs: &[mpsc::Sender<EventBatch>],
) {
    let now_instant = tokio::time::Instant::now();
    for i in 0..stages.len() {
        let Some(deadline) = next_flush[i] else { continue };
        if deadline > now_instant {
            continue;
        }
        let groups = flush_stage(pipeline_name, stages, i, now_unix_nanos(), fallback_resource);
        for (resource, events) in groups {
            send_batch(output_txs, resource, events);
        }
        let interval = stages[i]
            .flush_interval()
            .expect("next_flush[i] is only ever Some for a stage with an interval");
        let mut next = deadline;
        while next <= now_instant {
            next += interval;
        }
        next_flush[i] = Some(next);
    }
}

/// Builds this pipeline's `Vec<Stage>` and owns them for the pipeline's lifetime, all on a
/// dedicated OS thread. `ScriptWorker` opts out of both `Send` and `Sync`
/// (`docs/design/lua-api.md`'s concurrency section, enforced in `logit-script` via a `PhantomData`
/// marker) -- it can't be *moved* into a thread at all, so construction has to happen here, not
/// on the caller's side with the result handed in. `blocking_recv`/`blocking_send` bridge to and
/// from the async world on either side, exactly what they're for from a plain thread like this.
///
/// Reports success/failure of the initial script loads over `ready_tx` before entering the receive
/// loop, so a bad script fails `logit run` at startup instead of silently running a broken
/// pipeline that only ever logs errors into the void.
///
/// When no stage has a flush interval, the loop is exactly what it was before flush ticks
/// existed: a plain `blocking_recv` with no timer involvement at all. Otherwise, `runtime` (a
/// `Handle` to the multi-thread runtime `logit run` builds -- a `current_thread` runtime can't
/// drive timers from `Handle::block_on`, but this project's only doesn't apply) drives
/// `tokio::time::timeout` around the receive so a due flush interrupts a wait instead of being
/// starved by it. Due stages are additionally checked at the *top* of every loop iteration,
/// unconditionally: `timeout` polls its inner future first, so under sustained load (where
/// `batch_rx.recv()` is always immediately ready) its `Elapsed` branch would never fire on its
/// own, and a flush would never happen. As long as batches keep arriving, the loop keeps
/// revisiting the top and re-checking -- so a due flush still fires on the very next iteration
/// after its deadline passes, regardless of how busy the channel is.
fn run_pipeline_worker(
    pipeline_name: String,
    transform_specs: Vec<TransformSpec>,
    ready_tx: tokio::sync::oneshot::Sender<Result<(), String>>,
    mut batch_rx: mpsc::Receiver<EventBatch>,
    output_txs: Vec<mpsc::Sender<EventBatch>>,
    runtime: tokio::runtime::Handle,
) {
    let mut stages: Vec<Stage> =
        match transform_specs.into_iter().map(Stage::build).collect::<Result<_, _>>() {
            Ok(stages) => stages,
            Err(err) => {
                // The receiver may already be gone if `run_pipelines` bailed for an unrelated
                // reason first; nothing useful to do with that here.
                let _ = ready_tx.send(Err(format!("loading a transform script: {err}")));
                return;
            }
        };
    // Ignoring a send failure for the same reason as above.
    let _ = ready_tx.send(Ok(()));

    let mut next_flush: Vec<Option<tokio::time::Instant>> = stages
        .iter()
        .map(|stage| stage.flush_interval().map(|interval| tokio::time::Instant::now() + interval))
        .collect();

    // Used only as the resource a Lua stage's flush() stamps its emitted events with, before any
    // real batch has arrived to take one from -- see `flush_stage`'s doc comment. Overwritten by
    // every real batch below.
    let mut last_resource = Arc::new(Resource::default());

    loop {
        flush_due_stages(&pipeline_name, &mut stages, &mut next_flush, &last_resource, &output_txs);

        let earliest_deadline = next_flush.iter().flatten().min().copied();
        let batch = match earliest_deadline {
            None => batch_rx.blocking_recv(),
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
                // The `async` block matters, not just style: `tokio::time::timeout` builds its
                // `Sleep` eagerly, and `Sleep` construction needs a runtime context, which this
                // plain thread doesn't have outside of `block_on`. Deferring construction to
                // inside the block (only polled once `block_on` has entered that context) is what
                // makes this legal rather than an immediate panic.
                match runtime.block_on(async { tokio::time::timeout(wait, batch_rx.recv()).await })
                {
                    Ok(batch) => batch,
                    Err(_elapsed) => continue,
                }
            }
        };
        let Some(batch) = batch else {
            // Inbound channel closed (every input for this pipeline finished, or `run_config`
            // bailed and dropped its side) -- flush once more so the in-flight window isn't
            // silently lost, then exit. This is not a substitute for real graceful shutdown
            // (Ctrl-C still falls through to the OS default, same as before): it only runs when
            // every sender is already gone, which today's inputs never do on their own.
            flush_all_stages(&pipeline_name, &mut stages, &last_resource, &output_txs);
            return;
        };
        last_resource = batch.resource.clone();
        let events = apply_transforms(&pipeline_name, &mut stages, &batch.resource, batch.events);
        send_batch(&output_txs, batch.resource, events);
    }
}

fn flush_all_stages(
    pipeline_name: &str,
    stages: &mut [Stage],
    fallback_resource: &Arc<Resource>,
    output_txs: &[mpsc::Sender<EventBatch>],
) {
    let now = now_unix_nanos();
    for i in 0..stages.len() {
        // Only a stage with a flush contract has anything to drain; skip the rest so a pipeline
        // with no aggregate/flush-bearing stage shuts down exactly like it did before flush ticks
        // existed -- no wasted flush() calls into every Lua stage on every exit.
        if stages[i].flush_interval().is_none() {
            continue;
        }
        let groups = flush_stage(pipeline_name, stages, i, now, fallback_resource);
        for (resource, events) in groups {
            send_batch(output_txs, resource, events);
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

    fn lua_stage(source: &str) -> Stage {
        Stage::Lua { worker: ScriptWorker::new(source).unwrap(), flush_interval: None }
    }

    fn default_resource() -> Arc<Resource> {
        Arc::new(logit_core::Resource::default())
    }

    #[test]
    fn apply_transforms_chains_scripts_in_order() {
        let mut stages = vec![
            lua_stage(
                r#"function process(event) event.attributes.stage1 = "yes" return event end"#,
            ),
            lua_stage(
                r#"function process(event) event.attributes.stage2 = "yes" return event end"#,
            ),
        ];
        let events = apply_transforms(
            "test",
            &mut stages,
            &default_resource(),
            vec![counter_event("hits", 1.0)],
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].attributes.get("stage1").and_then(|v| v.as_str()), Some("yes"));
        assert_eq!(events[0].attributes.get("stage2").and_then(|v| v.as_str()), Some("yes"));
    }

    #[test]
    fn apply_transforms_respects_drop() {
        let mut stages = vec![lua_stage("function process(event) return nil end")];
        let events = apply_transforms(
            "test",
            &mut stages,
            &default_resource(),
            vec![counter_event("hits", 1.0)],
        );
        assert!(events.is_empty());
    }

    #[test]
    fn apply_transforms_respects_fan_out() {
        let mut stages =
            vec![lua_stage("function process(event) return {event, event:clone()} end")];
        let events = apply_transforms(
            "test",
            &mut stages,
            &default_resource(),
            vec![counter_event("hits", 1.0)],
        );
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn apply_transforms_with_no_stages_is_a_passthrough() {
        let events = apply_transforms(
            "test",
            &mut [],
            &default_resource(),
            vec![counter_event("hits", 1.0)],
        );
        assert_eq!(events.len(), 1);
    }

    #[test]
    fn apply_transforms_logs_and_drops_on_script_error() {
        let mut stages = vec![lua_stage(r#"function process(event) error("boom") end"#)];
        let events = apply_transforms(
            "test",
            &mut stages,
            &default_resource(),
            vec![counter_event("hits", 1.0)],
        );
        assert!(events.is_empty());
    }

    #[test]
    fn apply_transforms_consumes_a_counter_in_an_aggregate_stage() {
        let mut stages = vec![Stage::Aggregate(Aggregator::new(Duration::from_secs(10)))];
        let events = apply_transforms(
            "test",
            &mut stages,
            &default_resource(),
            vec![counter_event("hits", 1.0)],
        );
        assert!(events.is_empty(), "an aggregatable counter should be absorbed, not passed on");
    }

    #[test]
    fn apply_transforms_passes_a_log_through_an_aggregate_stage() {
        let mut stages = vec![Stage::Aggregate(Aggregator::new(Duration::from_secs(10)))];
        let log = Event {
            timestamp: 0,
            attributes: AttrMap::new(),
            payload: Payload::Log(logit_core::LogRecord {
                message: logit_core::Value::str("hi"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
            }),
        };
        let events = apply_transforms("test", &mut stages, &default_resource(), vec![log]);
        assert_eq!(events.len(), 1, "a log has nothing to aggregate into and should pass through");
    }

    #[test]
    fn flush_stage_runs_flushed_events_through_later_stages() {
        // aggregate -> lua stage that tags every event it sees as flushed=yes. The tag should
        // appear on the aggregate's flushed output, proving a flush isn't exempt from the rest
        // of the chain.
        let mut stages = vec![
            Stage::Aggregate(Aggregator::new(Duration::from_secs(10))),
            lua_stage(
                r#"function process(event) event.attributes.flushed = "yes" return event end"#,
            ),
        ];
        let resource = default_resource();
        apply_transforms("test", &mut stages, &resource, vec![counter_event("hits", 1.0)]);

        let groups = flush_stage("test", &mut stages, 0, 100, &resource);
        assert_eq!(groups.len(), 1);
        let (_, events) = &groups[0];
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].attributes.get("flushed").and_then(|v| v.as_str()), Some("yes"));
    }

    #[test]
    fn flush_stage_of_the_last_stage_has_no_downstream() {
        let mut stages = vec![Stage::Aggregate(Aggregator::new(Duration::from_secs(10)))];
        let resource = default_resource();
        apply_transforms("test", &mut stages, &resource, vec![counter_event("hits", 1.0)]);

        let groups = flush_stage("test", &mut stages, 0, 100, &resource);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].1.len(), 1);
    }

    #[test]
    fn require_implemented_transform_accepts_aggregate_and_lua() {
        assert!(require_implemented_transform(&TransformConfig::Builtin(
            BuiltinTransformConfig::Aggregate { interval: Duration::from_secs(10) }
        ))
        .is_ok());
        assert!(require_implemented_transform(&TransformConfig::Lua {
            lua: "".into(),
            interval: None
        })
        .is_ok());
    }

    #[test]
    fn require_implemented_transform_rejects_other_builtins() {
        let err = expect_err(require_implemented_transform(&TransformConfig::Builtin(
            BuiltinTransformConfig::Json,
        )));
        assert!(err.to_string().contains("not implemented yet"));
    }

    #[test]
    fn require_implemented_transform_rejects_a_zero_aggregate_interval() {
        let err = expect_err(require_implemented_transform(&TransformConfig::Builtin(
            BuiltinTransformConfig::Aggregate { interval: Duration::ZERO },
        )));
        assert!(err.to_string().contains("0s"), "got: {err}");
    }

    #[test]
    fn require_implemented_transform_rejects_a_zero_lua_flush_interval() {
        let err = expect_err(require_implemented_transform(&TransformConfig::Lua {
            lua: "".into(),
            interval: Some(Duration::ZERO),
        }));
        assert!(err.to_string().contains("0s"), "got: {err}");
    }

    #[test]
    fn validate_semantics_accepts_an_aggregate_transform() {
        let mut inputs = HashMap::new();
        inputs.insert("in".to_string(), InputConfig::Statsd { bind: "127.0.0.1:0".to_string() });
        let mut outputs = HashMap::new();
        outputs.insert("out".to_string(), influxdb_output_config());
        let mut pipelines = HashMap::new();
        pipelines.insert(
            "p".to_string(),
            PipelineConfig {
                inputs: vec!["in".to_string()],
                transforms: vec![TransformConfig::Builtin(BuiltinTransformConfig::Aggregate {
                    interval: Duration::from_secs(10),
                })],
                outputs: vec!["out".to_string()],
            },
        );
        let config = Config { inputs, outputs, pipelines };
        assert!(validate_semantics(&config).is_ok());
    }
}
