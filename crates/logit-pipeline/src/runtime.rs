//! The node runtime: turns a resolved [`Graph`] plus one built implementation per component
//! (a [`NodeSpec`]) into running tasks/threads, wired together with per-component [`Fanout`]s.
//! See `docs/design/pipeline-graph.md`'s "Runtime model" and "Thread model" sections.
//!
//! Every component's inbox channel is created before any node is spawned, so spawn order doesn't
//! matter: a `Fanout` is cloned `Sender`s into inboxes that already exist.
//!
//! Every `select!` and `timeout` a node runs, here and in the listeners and sinks, is a row of
//! `docs/design/pipeline-graph.md`'s "Cancellation points" table: what each losing arm drops, and
//! why that loses nothing or what counts it.

use crate::fanout::{BatchContext, Delivered, TraceContext};
use crate::graph::{Graph, Role};
use crate::output::{classify, is_head_only, is_retryable, DeliveryPosture, Fault, HeadOnly};
#[cfg(test)]
use crate::queue::{SinkQueue, SinkQueueConfig};
use crate::queue::{SinkStore, SinkStoreConfig, StoreItem};
use crate::readiness::{NodeState, Phase};
use crate::router::{Destination, Router, RouterScratch};
use crate::{Edge, Fanout, Input, InputRuntimeConfig, Output, Readiness, Transform};
use anyhow::Context;
use logit_core::{Diagnostics, Event, EventBatch, Resource, Scope, SpanKind, Telemetry};
use logit_proto::native::SeqId;
use logit_script::{Heartbeat, ProcessOutcome, ScriptWorker};
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

/// The gauge `write_loop` sets to 1 while its head has failed and a retry is pending or in
/// flight, and to 0 once that head is delivered or dropped (`docs/adr/sink-fault-classes.md`, "A
/// hold is announced"). `batches.dropped` stops moving while a head holds, so this is what an
/// alert reads.
const RETRYING_GAUGE: &str = "logit.component.retrying";

/// The least time between two `retrying` lines for one held head: [`WriteLoopConfig`]'s
/// default `retrying_log_interval`.
const RETRYING_LOG_INTERVAL: Duration = Duration::from_secs(60);

/// Bounded channel capacity between two graph nodes. Small and arbitrary: enough to smooth bursts
/// without unbounded memory growth. Not tuned against measurements.
const CHANNEL_CAPACITY: usize = 64;

/// One component's built implementation, keyed by id and handed to [`run`]. The registry
/// (`logit-cli`) decides which variant a `ComponentKind` becomes; this crate only runs it.
pub enum NodeSpec {
    /// A listener and its runtime knobs. Production call sites derive `shutdown_grace` from the
    /// listener's `receive:` block through `logit-cli`'s `input_runtime_config`, so a config
    /// that omits `receive:` gets `ReceiveConfig::default()`'s 5 s, not
    /// `InputRuntimeConfig::default()`'s `Duration::ZERO`, which only tests reach. The
    /// difference matters: at `ZERO` the backstop arm in `run_input` is ready the instant
    /// shutdown fires, so an overriding listener that still has anything to drain is cancelled
    /// by drop. See `docs/adr/decoupled-listener-io.md`.
    Input(Box<dyn Input + Send>, InputRuntimeConfig),
    /// The sink, its queue (in memory or disk-backed; see `SinkStoreConfig`), and its retry
    /// backoff cap and shutdown grace (`WriteLoopConfig`). Production builds these from the
    /// component's `buffer:` block, falling back to the defaults when it's omitted.
    Output(Box<dyn Output + Send>, SinkStoreConfig, WriteLoopConfig),
    Transform(Box<dyn Transform + Send>),
    /// A [`Router`] node (`docs/adr/target-components.md`): directs each event to its own
    /// outbound `Fanout` ([`Destination::Forward`]) or to one of the slot-ordered target
    /// `Fanout`s the runtime clones in from `ResolvedComponent::targets`.
    Router(Box<dyn Router + Send>),
    /// A `target`: nothing is spawned for it, and it holds nothing.
    ///
    /// The variant exists so the registry stays one spec per component (a missing spec is a
    /// startup error). A target is a zero-cost alias, not a node: no task, no inbox, no channel.
    /// Its behavior is the one `Fanout` [`run_with_telemetry`]'s pre-spawn pass builds under the
    /// target's id and clones into each of its routers. See `docs/adr/target-components.md`'s
    /// "Runtime: a target is a zero-cost alias".
    Target,
    /// Built on its own thread, not by the caller: `ScriptWorker` is `!Send`
    /// (`docs/design/lua-api.md`'s concurrency section). Carries no `targets`: the Lua spawn arm
    /// resolves both the VM's name -> slot table and its target `Fanout`s from
    /// `ResolvedComponent::targets`, as the `Router` arm does, so slot order has one source.
    Lua {
        script: String,
        interval: Option<Duration>,
        runtime: LuaRuntimeConfig,
    },
}

/// Why the pipeline stopped, at the granularity `logit`'s exit-code table needs
/// (`docs/deploying.md`): a startup failure is the operator's config or environment (exit 1); a
/// runtime failure happened after the process reported `Ready` (exit 2).
///
/// Doesn't implement `std::error::Error`: anyhow's blanket `From` would then nest this wrapper's
/// `Debug` above the inner error's context chain, changing every message the tests assert on.
/// [`RunError::into_inner`] returns the original `anyhow::Error` unwrapped.
#[derive(Debug)]
pub enum RunError {
    Startup(anyhow::Error),
    Runtime(anyhow::Error),
}

impl RunError {
    /// `1` or `2`. `0` (clean exit) and `130` (the second-signal kill) belong to `main`.
    pub fn exit_code(&self) -> i32 {
        match self {
            RunError::Startup(_) => 1,
            RunError::Runtime(_) => 2,
        }
    }

    pub fn error(&self) -> &anyhow::Error {
        match self {
            RunError::Startup(err) | RunError::Runtime(err) => err,
        }
    }

    pub fn into_inner(self) -> anyhow::Error {
        match self {
            RunError::Startup(err) | RunError::Runtime(err) => err,
        }
    }
}

impl std::fmt::Display for RunError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.error().fmt(f)
    }
}

/// Builds every component's inbox and `Fanout`, then spawns each as a tokio task (listeners,
/// sinks, `Transform`-trait nodes) or a dedicated OS thread (Lua nodes), and runs until the first
/// one fails. No shutdown signal -- see [`run_with_shutdown`] for graceful shutdown.
pub async fn run(graph: Graph, specs: HashMap<String, NodeSpec>) -> anyhow::Result<()> {
    run_with_shutdown(graph, specs, std::future::pending()).await
}

/// Same as [`run`], but resolving `shutdown` stops every listener so its downstream inboxes close
/// normally, which triggers the close-time flush cascade (a node flushes once when its inbox
/// closes; see `run_transform`/`run_lua`). No `Input` implementation needs to know about it.
///
/// The mechanism: `Input::run` takes its `Fanout` by value, so when a listener returns or its
/// future is dropped, the last `Sender` into each downstream inbox goes with it, and those inboxes
/// close. The `FiniteInput` tests prove the cascade.
pub async fn run_with_shutdown(
    graph: Graph,
    specs: HashMap<String, NodeSpec>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    run_with_telemetry(graph, specs, HashMap::new(), Readiness::disabled(), shutdown)
        .await
        .map_err(RunError::into_inner)
}

/// Same as [`run_with_shutdown`], with a per-component [`Telemetry`] handle and a [`Readiness`]
/// handle. A component with no `telemetry` entry gets [`Telemetry::default`], the disabled handle;
/// `run`/`run_with_shutdown` pass an empty map and [`Readiness::disabled()`]. See
/// `docs/design/internal-telemetry.md`.
///
/// Returns [`RunError`] so a caller can tell a startup failure from a runtime one without parsing
/// a message.
pub async fn run_with_telemetry(
    graph: Graph,
    specs: HashMap<String, NodeSpec>,
    telemetry: HashMap<String, Telemetry>,
    readiness: Readiness,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<(), RunError> {
    run_with_options(graph, specs, telemetry, readiness, shutdown, RunOptions::default()).await
}

/// Process-level settings for [`run_with_options`]. The default is [`run_with_telemetry`]'s
/// behavior.
#[derive(Debug, Default, Clone, Copy)]
pub struct RunOptions {
    /// How long the runtime waits between `shutdown` resolving and telling the nodes to drain.
    /// Readiness reports draining for the whole wait, and every listener keeps running. Applies
    /// only when the phase was `Ready` at the signal.
    pub shutdown_delay: Duration,
}

/// Same as [`run_with_telemetry`], with [`RunOptions`].
pub async fn run_with_options(
    graph: Graph,
    mut specs: HashMap<String, NodeSpec>,
    mut telemetry: HashMap<String, Telemetry>,
    readiness: Readiness,
    shutdown: impl Future<Output = ()> + Send + 'static,
    options: RunOptions,
) -> Result<(), RunError> {
    // Sorted so a startup failure (an unbindable port, a bad Lua script) names the same component
    // every time. `readiness.begin` publishes the full ordered id list before anything binds, so
    // a probe arriving mid-startup sees every id.
    //
    // `begin` must run before the shutdown driver is spawned: that task's first act is
    // `readiness.draining()`, and an already-resolved `shutdown` (`std::future::ready(())`) could
    // run it before this line, since no `.await` separates the spawn from here.
    let mut ids: Vec<String> = graph.components.keys().cloned().collect();
    ids.sort();
    readiness.begin(&ids);

    // A `watch`, not a `oneshot`, because every listener needs its own receiver clone.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // The join loop keeps `shutdown_tx` to trigger shutdown on the first task error. Both may
    // `send(true)`; `watch::Sender::send` is idempotent.
    let shutdown_tx_for_driver = shutdown_tx.clone();
    // Set by whichever of the driver or the join loop's first-error branch starts the drain
    // first (`OnceLock::set` ignores later calls); read at the end for the `drain complete` log.
    let drain_started: Arc<std::sync::OnceLock<tokio::time::Instant>> = Arc::default();
    let drain_started_for_driver = drain_started.clone();
    let readiness_for_driver = readiness.clone();
    let delay = options.shutdown_delay;
    let shutdown_driver = tokio::spawn(async move {
        shutdown.await;
        tracing::info!(target: "logit", "shutdown signal received");
        // A process that never reported `Ready` was never in an endpoint set, so there is no
        // traffic to move away and the delay would only hold its ports during a failed rollout.
        // Read before `draining()` overwrites the phase.
        let delay = if readiness_for_driver.snapshot().phase == Phase::Ready {
            delay
        } else {
            Duration::ZERO
        };
        // Before the nodes are told, so `/readyz` stops routing traffic here the instant the
        // signal arrives, not partway through the drain. A no-op if a node already failed: a
        // SIGTERM after a failure must not paper over it.
        readiness_for_driver.draining();
        // The delay is the window an orchestrator withdraws this process from its endpoints while
        // every listener still accepts and reads; readiness already reports draining. Every
        // grace timer starts at the send below, after it. A node error during the delay starts
        // the drain from the join loop instead, which makes this task's late `set` and `send`
        // no-ops; a run that ends on its own aborts this task mid-sleep. Skipped when zero,
        // because `sleep(ZERO)` still yields to the timer.
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        if drain_started_for_driver.set(tokio::time::Instant::now()).is_ok() && !delay.is_zero() {
            tracing::info!(target: "logit", delay = ?delay, "shutdown delay elapsed");
        }
        let _ = shutdown_tx_for_driver.send(true);
    });

    // Every batch any node dropped for shutdown, summed by `count_shutdown_drop` so the `drain
    // complete` log can say whether the drain was clean.
    let shutdown_dropped_batches = Arc::new(AtomicU64::new(0));

    // Every listener and sink binds before any channel exists or task spawns, so a bind failure
    // fails startup with nothing else running. Sequential and sorted, so "which one failed" never
    // depends on join order. Sinks are here because `prometheus_out` opens a listening socket,
    // which fails like an input (address in use), not like a sink whose destination isn't up yet;
    // `Output::bind` defaults to a no-op (`docs/adr/prometheus-scrape-and-exposition.md`).
    for id in &ids {
        let bound = match specs.get_mut(id) {
            Some(NodeSpec::Input(input, _)) => input.bind().await,
            Some(NodeSpec::Output(output, _, _)) => output.bind().await,
            _ => continue,
        };
        bound.map_err(|err| RunError::Startup(err.context(format!("component '{id}'"))))?;
        readiness.set_node(id, NodeState::Bound);
    }

    let mut edges: HashMap<String, Edge> = HashMap::with_capacity(ids.len());
    let mut inboxes: HashMap<String, mpsc::Receiver<Delivered>> = HashMap::with_capacity(ids.len());
    for id in &ids {
        // No channel for a `target`: it declares no `sources:` and nothing may name one as a
        // source (rule 49). A target is a name for its routers' outbound edges; the pass below
        // builds its one `Fanout` directly onto its consumers' inboxes.
        if graph.components.get(id).is_some_and(|c| c.role() == Role::Target) {
            continue;
        }
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        // Captured here, before the spawn loop's `telemetry.remove`, so a producer sorted after
        // its consumer still finds the consumer's handle.
        let consumer = telemetry.get(id).cloned().unwrap_or_default();
        edges.insert(id.clone(), Edge::new(tx).with_telemetry(consumer));
        inboxes.insert(id.clone(), rx);
    }

    // One `Fanout` per `target`, built before the spawn loop: `ids` is sorted, so a router can
    // sort ahead of the targets it directs at (`fixtures/fan-out-central.yaml` does), and its
    // spawn arm needs them built. Each carries the target's own id and telemetry handle, so
    // `Fanout::stamp` writes the target's id into `previous`
    // (`docs/adr/batch-provenance-on-delivered.md`) and the producer metrics (`batches.sent`,
    // `events.sent`, `send.blocked.duration`, `events.dropped{reason="closed_consumer"}`) appear
    // under the target's id, giving per-stream volume with no new metric.
    let mut target_fanouts: HashMap<String, Fanout> = HashMap::new();
    for id in &ids {
        let component = graph.components.get(id).expect("id came from this graph");
        if component.role() != Role::Target {
            continue;
        }
        let fanout =
            Fanout::from_edges(component.consumers.iter().map(|c| edges[c].clone()).collect())
                .with_component(id)
                .with_telemetry(telemetry.get(id).cloned().unwrap_or_default());
        target_fanouts.insert(id.clone(), fanout);
        // Never `Running`/`Finished`/`Failed`: those transitions all come from a `JoinSet` entry,
        // and a target has no task to have one (`NodeState::Alias`'s own doc comment).
        readiness.set_node(id, NodeState::Alias);
    }

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();
    // Task id -> component id, so the join loop can name a failing or panicking task in
    // `readiness`. A Lua node's entry is its `watch_lua_thread` task, since a `std::thread` can't
    // be a `JoinSet` member.
    let mut node_ids: HashMap<tokio::task::Id, String> = HashMap::with_capacity(ids.len());
    // For Lua nodes only (see `run_lua`); `Handle::current()` needs the async context we're in.
    let runtime_handle = tokio::runtime::Handle::current();

    for id in ids {
        let component = graph.components.get(&id).expect("id came from this graph");
        // Skipped before anything is taken out of `telemetry`/`specs`/`inboxes`: a target has no
        // inbox, and its `NodeSpec::Target` goes unused.
        if component.role() == Role::Target {
            continue;
        }
        let node_telemetry = telemetry.remove(&id).unwrap_or_default();
        let fanout =
            Fanout::from_edges(component.consumers.iter().map(|c| edges[c].clone()).collect())
                .with_component(&id)
                .with_telemetry(node_telemetry.clone());
        let inbox = inboxes.remove(&id).expect("an inbox was created for every id above");
        let spec = specs
            .remove(&id)
            .with_context(|| format!("no implementation registered for component '{id}'"))
            .map_err(RunError::Startup)?;

        match spec {
            NodeSpec::Input(input, input_config) => {
                // A listener has no sources (arity rule), so nothing ever sends into its inbox.
                drop(inbox);
                let handle = tasks.spawn(run_input(
                    id.clone(),
                    input,
                    fanout,
                    shutdown_rx.clone(),
                    input_config.shutdown_grace,
                ));
                node_ids.insert(handle.id(), id.clone());
                readiness.set_node(&id, NodeState::Running);
            }
            NodeSpec::Output(output, store_config, write_config) => {
                let handle = tasks.spawn(run_output(
                    id.clone(),
                    output,
                    inbox,
                    node_telemetry,
                    store_config,
                    write_config,
                    shutdown_rx.clone(),
                    shutdown_dropped_batches.clone(),
                ));
                node_ids.insert(handle.id(), id.clone());
                readiness.set_node(&id, NodeState::Running);
            }
            NodeSpec::Transform(transform) => {
                let handle = tasks.spawn(run_transform(transform, inbox, fanout, node_telemetry));
                node_ids.insert(handle.id(), id.clone());
                readiness.set_node(&id, NodeState::Running);
            }
            NodeSpec::Router(router) => {
                // Slot order is `ResolvedComponent::targets`' order, derived only in
                // `graph::targets_of`, so `Destination::To(n)` means `routes[n]` here and in the
                // Lua name -> slot table alike. Cloning a `Fanout` clones its `Sender`s, so two
                // routers directing at one target fan in there like `sources:` fan-in.
                let routes = resolve_target_fanouts(&id, &component.targets, &target_fanouts);
                let handle = tasks.spawn(run_router(router, inbox, fanout, routes, node_telemetry));
                node_ids.insert(handle.id(), id.clone());
                readiness.set_node(&id, NodeState::Running);
            }
            NodeSpec::Target => {
                // Reachable only if a caller registered `NodeSpec::Target` for a non-target
                // component (nothing checks spec kind against config kind here); the `Role::Target`
                // guard above skips real targets. Doing nothing beats panicking.
            }
            NodeSpec::Lua { script, interval, runtime } => {
                // A Lua node is a router too: the ids in `component.targets` become
                // `event:to("..")`'s name -> slot table in the VM, resolved from the same
                // pre-spawn map as the `Router` arm, so `Destination::To(n)` and the script's n-th
                // target id agree.
                let targets = component.targets.clone();
                let target_routes =
                    resolve_target_fanouts(&id, &component.targets, &target_fanouts);
                let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();
                let (done_tx, done_rx) = oneshot::channel::<Result<(), String>>();
                let handle = runtime_handle.clone();
                let thread_id = id.clone();
                let heartbeat = Arc::new(Heartbeat::new());
                let io: SharedLuaIo = Arc::new(std::sync::Mutex::new(Some(LuaIo {
                    inbox,
                    fanout,
                    target_fanouts: target_routes,
                })));
                let watcher_telemetry = node_telemetry.clone();
                let watcher_diag =
                    Diagnostics::new(id.clone()).with_telemetry(node_telemetry.clone());
                let thread_heartbeat = heartbeat.clone();
                let thread_io = io.clone();
                let thread_dropped = shutdown_dropped_batches.clone();
                let max_memory = runtime.max_memory;
                let verdict_spacing = runtime.min_verdict_spacing;
                std::thread::Builder::new()
                    .name(format!("logit-{id}"))
                    // Script recursion through C frames (a `string.gsub` callback a few hundred
                    // deep) overflows the 2 MiB default and aborts the process. Virtual: pages are
                    // committed only when touched.
                    .stack_size(8 << 20)
                    .spawn(move || {
                        run_lua(
                            thread_id,
                            script,
                            interval,
                            targets,
                            ready_tx,
                            done_tx,
                            thread_io,
                            thread_heartbeat,
                            max_memory,
                            verdict_spacing,
                            node_telemetry,
                            thread_dropped,
                            handle,
                        )
                    })
                    .with_context(|| format!("spawning thread for component '{id}'"))
                    .map_err(RunError::Startup)?;
                match ready_rx.await {
                    Ok(Ok(())) => {
                        // Spawned only after the ready handshake, so a load failure never leaves a
                        // watcher behind. `done_tx` buffers its one message, so a thread that dies
                        // between reporting ready and this spawn is still observed. `Running` is
                        // set before the spawn so it can never overwrite the watcher's `Stalled`.
                        readiness.set_node(&id, NodeState::Running);
                        let watcher = tasks.spawn(watch_lua_thread(
                            id.clone(),
                            done_rx,
                            heartbeat,
                            io,
                            runtime,
                            shutdown_rx.clone(),
                            readiness.clone(),
                            watcher_telemetry,
                            watcher_diag,
                            shutdown_dropped_batches.clone(),
                        ));
                        node_ids.insert(watcher.id(), id.clone());
                    }
                    Ok(Err(message)) => {
                        return Err(RunError::Startup(anyhow::anyhow!(
                            "component '{id}': {message}"
                        )))
                    }
                    Err(_) => {
                        return Err(RunError::Startup(anyhow::anyhow!(
                            "component '{id}': thread exited before reporting ready"
                        )))
                    }
                }
            }
        }
    }

    // Every `Fanout` holds its own `Edge` clones; these are construction scaffolding. Each edge
    // holds a `Sender`, so left alive, each is an extra `Sender` on every channel, no inbox ever
    // closes, the shutdown cascade never fires, and `run` hangs.
    drop(edges);
    // Same rule for targets: each router holds its own clone of a target's `Fanout`. Left alive,
    // these keep every target consumer's inbox open and `run` hangs past the target.
    // `a_router_exiting_closes_its_targets_consumers_inboxes` pins it under a timeout.
    drop(target_fanouts);

    // A no-op if a node has already failed (`Readiness::ready`).
    readiness.ready();
    tracing::info!(target: "logit", "ready");

    // On the first error, trigger the same shutdown SIGTERM drives, so every other task drains
    // gracefully (`docs/adr/buffered-sink-delivery.md`) instead of being aborted with a healthy
    // sibling's buffered work. Keep joining until every task exits; keep only the first error,
    // since later ones are usually cascades from it.
    let mut result: Result<(), RunError> = Ok(());
    while let Some(joined) = tasks.join_next_with_id().await {
        let (task_id, outcome) = match joined {
            Ok((task_id, Ok(()))) => {
                if let Some(id) = node_ids.get(&task_id) {
                    readiness.set_node(id, NodeState::Finished);
                }
                continue;
            }
            Ok((task_id, Err(err))) => (task_id, err),
            Err(join_err) => {
                let task_id = join_err.id();
                (task_id, anyhow::Error::from(join_err))
            }
        };
        // Every failing node is marked (`/readyz`'s `degraded` means any node failed); `result`
        // keeps only the first failure.
        if let Some(id) = node_ids.get(&task_id) {
            readiness.set_node(id, NodeState::Failed);
        }
        readiness.failed();
        if result.is_ok() {
            result = Err(RunError::Runtime(outcome));
            let _ = drain_started.set(tokio::time::Instant::now());
            let _ = shutdown_tx.send(true);
        }
    }
    // Every node has exited; `shutdown` may never resolve (`run` passes `pending()`).
    shutdown_driver.abort();

    // Unset means every node finished on its own, with no shutdown or failure: no drain to report.
    if let Some(started) = drain_started.get() {
        let dropped = shutdown_dropped_batches.load(std::sync::atomic::Ordering::Relaxed);
        if dropped > 0 {
            tracing::warn!(
                target: "logit",
                duration = ?started.elapsed(),
                batches_dropped = dropped,
                "drain complete"
            );
        } else {
            tracing::info!(target: "logit", duration = ?started.elapsed(), "drain complete");
        }
    }
    result
}

/// Drives one listener via [`Input::run_until_shutdown`], racing it against a grace-delayed
/// backstop rather than `shutdown` itself (`docs/adr/decoupled-listener-io.md`).
///
/// The default `run_until_shutdown` resolves the instant `shutdown` fires. Racing it against
/// `shutdown` too would make both arms ready at once, with no way to let an overriding
/// implementation finish draining. Racing against [`shutdown_grace_expired`] instead means the
/// default (or any override that finishes within the grace) always wins, adding no latency; only
/// an override still working at the deadline is cancelled by drop, a loss now bounded by
/// `shutdown_grace`.
///
/// `biased`, input arm first; `unconstrained` backstop: see `docs/design/pipeline-graph.md`'s
/// "Cancellation points".
async fn run_input(
    id: String,
    mut input: Box<dyn Input + Send>,
    fanout: Fanout,
    mut shutdown: watch::Receiver<bool>,
    shutdown_grace: Duration,
) -> anyhow::Result<()> {
    let deadline = std::sync::OnceLock::new();
    tokio::select! {
        biased;
        result = input.run_until_shutdown(fanout, shutdown.clone())
            => result.with_context(|| format!("component '{id}'")),
        () = tokio::task::unconstrained(
            shutdown_grace_expired(&mut shutdown, &deadline, shutdown_grace),
        ) => Ok(()),
    }
}

/// A sink node's drain-and-deliver pair, decoupled through a [`SinkStore`]
/// (`docs/adr/buffered-sink-delivery.md`): [`drain_inbox`] moves every `Delivered` off the inbox
/// into the store as fast as its bounds allow, while [`write_loop`] delivers from it
/// independently, so a slow or backing-off `Output::send` doesn't stall the inbox.
///
/// Both run in this one task, and neither fails: a sink's delivery never ends the run
/// (`docs/adr/sink-fault-classes.md`, "The process never exits for a sink"), so only `bind` and
/// opening the store return `Err`. `write_loop` only borrows `output` so that this function can
/// run the final drain-and-flush itself, after `drain` can no longer push anything (see
/// `finish_and_flush`).
#[allow(clippy::too_many_arguments)]
async fn run_output(
    id: String,
    mut output: Box<dyn Output + Send>,
    mut inbox: mpsc::Receiver<Delivered>,
    telemetry: Telemetry,
    store_config: SinkStoreConfig,
    write_config: WriteLoopConfig,
    shutdown: watch::Receiver<bool>,
    shutdown_dropped_batches: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    let diag = Diagnostics::new(id.clone()).with_telemetry(telemetry.clone());
    // Idempotent (`Output::bind`'s contract): a no-op after `run_with_telemetry`'s pre-spawn
    // pass, and what lets a unit test spawn `run_output` directly.
    output.bind().await.with_context(|| format!("component '{id}'"))?;
    // A `Disk` store opens or recovers a spool directory and can fail (bad path, permissions,
    // another process holding the lock).
    let store = Arc::new(
        SinkStore::open(store_config, telemetry.clone(), diag.clone())
            .with_context(|| format!("component '{id}'"))?,
    );

    // `drain_inbox` borrows `inbox` rather than owning it, so dropping an abandoned `drain`
    // leaves its unread batches in the channel for the sweep below to count. The batch it was
    // pushing when dropped is left in `in_hand`, for the same sweep.
    let in_hand = InHand::default();
    let mut drain =
        Box::pin(drain_inbox(&mut inbox, Arc::clone(&store), telemetry.clone(), &in_hand));
    let mut write = Box::pin(write_loop(
        id.clone(),
        output.as_mut(),
        Arc::clone(&store),
        telemetry.clone(),
        write_config,
        shutdown,
        &shutdown_dropped_batches,
    ));

    // Not `tokio::join!`: `write_loop` can return early (shutdown grace expiring) while `inbox`
    // stays open, as it does under every real listener. `drain_inbox` can't learn its consumer
    // gave up, and under `Block` would park forever pushing into a queue nothing drains, hanging
    // this task and `run`.
    //
    // If `write` finishes first, a still-pending `drain` is dropped and never polled again; the
    // sweep below counts what it left in `inbox`. (When `write` finished by draining to
    // closed-and-empty, `drain` already closed the queue, so it's already done.) If `drain`
    // finishes first, its inbox closed normally and `write_loop` still has the queue's tail.
    //
    // `write` holds `output`'s mutable borrow until dropped, and `finish_and_flush` needs it back.
    // The flag exists for the borrow checker: it tracks the move `write.await` makes per arm, so
    // dropping `write` unconditionally after the `select!` doesn't typecheck.
    let write_finished = tokio::select! {
        () = &mut write => true,
        () = &mut drain => false,
    };
    if write_finished {
        drop(write);
    } else {
        write.await;
    }

    // From here nothing else pushes into `store`, so `finish_and_flush`'s snapshot is final.
    drop(drain);

    // Close `store` before the sweep below pushes into it. Otherwise, under `overflow: block`
    // against a full disk spool, the sweep's `store.push(..).await` waits on `not_full` forever,
    // since nothing left running would notify it. Once closed, `DiskQueue::push` accepts
    // over-bound instead of blocking, so the sweep still drops nothing (`SinkStore::finish`'s
    // "a disk-backed sink drops nothing at shutdown") and may briefly exceed `disk.max_bytes`, by
    // at most the channel's capacity plus the one batch `in_hand` held, reclaimed on the next
    // `open`. The `Memory` path never
    // pushes in the sweep, and `SinkStore::finish`'s memory drain uses `commit()`, which
    // ignores `closed`.
    store.close();

    // Closed before the sweep, so a producer that lands after it fails upstream as
    // `closed_consumer` instead of completing a send into a channel nothing reads again. Left
    // open, a producer parked on the full channel (an upstream `aggregate` or Lua node's
    // close-time flush) takes the capacity the sweep frees and sends while `finish_and_flush`
    // awaits, and the batch dies with `inbox`, neither received nor dropped.
    inbox.close();

    // An abandoned `drain` may leave batches that never reached `store`, so `finish_and_flush`
    // can't see them: the one its dropped `store.push` held (`in_hand`, first, since it arrived
    // first), then any still in `inbox`. A `Disk` store persists them (it drops nothing at
    // shutdown); a `Memory` store counts and diagnoses them as dropped.
    let mut abandoned_batches: u64 = 0;
    let mut abandoned_events: u64 = 0;
    // Not counted `received` here: `drain_inbox` counted it before parking it in `in_hand`.
    let parked = in_hand.lock().unwrap_or_else(|p| p.into_inner()).take();
    if let Some((batch, ctx)) = parked {
        abandoned_batches += 1;
        abandoned_events += batch.events.len() as u64;
        if matches!(store.as_ref(), SinkStore::Disk(_)) {
            store.push((batch, ctx)).await;
        }
    }
    // `recv` on a closed channel returns `None` once the buffer is empty and every reserved
    // `Permit` is released, so a batch sent through a permit `Fanout::send_with_deadline`
    // reserved before the close still lands here. The bound covers only `recv`, never a disk
    // `push`, so a cancelled wait loses nothing already taken.
    let sweep_deadline = tokio::time::Instant::now() + SWEEP_DRAIN_TIMEOUT;
    while let Ok(Some(delivered)) = tokio::time::timeout_at(sweep_deadline, inbox.recv()).await {
        let ctx = delivered.batch_context();
        let batch = unwrap_batch_arc(delivered);
        let events = batch.events.len() as u64;
        telemetry.count("logit.component.batches.received", 1.0, &[]);
        telemetry.count("logit.component.events.received", events as f64, &[]);
        abandoned_batches += 1;
        abandoned_events += events;
        if matches!(store.as_ref(), SinkStore::Disk(_)) {
            store.push((batch, ctx)).await;
        }
    }
    if abandoned_batches > 0 {
        if matches!(store.as_ref(), SinkStore::Disk(_)) {
            diag.warn(format_args!(
                "{abandoned_batches} batch(es) ({abandoned_events} event(s)) not yet in this \
                 sink's disk spool when it stopped -- appended to it instead of being dropped"
            ));
        } else {
            count_shutdown_drop(
                &telemetry,
                &shutdown_dropped_batches,
                abandoned_batches,
                abandoned_events,
            );
            diag.warn(format_args!(
                "{abandoned_batches} batch(es) ({abandoned_events} event(s)) never handed to \
                 this sink's delivery queue when it stopped"
            ));
        }
    }

    finish_and_flush(&diag, &store, &telemetry, output.as_mut(), &shutdown_dropped_batches).await;

    Ok(())
}

/// How long `run_output`'s shutdown sweep waits on a closed inbox for upstream permit holders to
/// send. A permit still unreleased past it is the same uncounted case [`REVOKE_DRAIN_TIMEOUT`]
/// documents for a revoked Lua inbox.
const SWEEP_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

/// Counts `batches` batches (`events` events) dropped for shutdown: the only site that counts
/// `batches.dropped`/`events.dropped{reason="shutdown"}` and the only one that adds to
/// `drain_total`, the sum the `drain complete` log reports (`run_with_telemetry`). Callers:
/// `run_output`'s sweep, [`finish_and_flush`], [`write_loop`]'s grace-cut send, and
/// [`revoke_lua_io`].
fn count_shutdown_drop(telemetry: &Telemetry, drain_total: &AtomicU64, batches: u64, events: u64) {
    if batches == 0 {
        return;
    }
    drain_total.fetch_add(batches, std::sync::atomic::Ordering::Relaxed);
    telemetry.count("logit.component.batches.dropped", batches as f64, &[("reason", "shutdown")]);
    telemetry.count("logit.component.events.dropped", events as f64, &[("reason", "shutdown")]);
}

/// Moves every `Delivered` batch off `inbox` into `store` as fast as `store.push`'s bounds allow,
/// independent of `write_loop`'s current delivery attempt, then closes `store` once `inbox`
/// closes. That close is how `write_loop`'s `store.peek()` learns "closed and empty".
///
/// Allocation: `Delivered::Owned` costs one `Arc::new`; `Delivered::Shared` is a move
/// (`crates/logit-bench/tests/allocations.rs`, `docs/design/memory.md`).
///
/// Borrows `inbox` so that dropping this future (when `write_loop` gives up first) leaves unread
/// batches in the channel for `run_output`'s abandoned-inbox sweep. The batch being pushed sits
/// in `in_hand` until its `store.push` returns, so dropping this future mid-push (parked on a
/// full `overflow: block` store, say) leaves it there for the same sweep, instead of losing it
/// uncounted. A push that returned has either queued the batch or counted it dropped, and
/// `in_hand` is cleared in the same poll, so the sweep never handles a batch twice.
///
/// `pub` only so `logit-bench`'s allocation tests can drive this hop directly.
pub async fn drain_inbox(
    inbox: &mut mpsc::Receiver<Delivered>,
    store: Arc<SinkStore>,
    telemetry: Telemetry,
    in_hand: &InHand,
) {
    while let Some(delivered) = inbox.recv().await {
        sample_inbox_depth(inbox, &telemetry);
        // Read before `unwrap_batch_arc` consumes `delivered`. The store carries it with the
        // batch so `write_loop`'s sink span has a parent and `Output::observe_batch` gets the
        // batch's provenance.
        let ctx = delivered.batch_context();
        let batch = unwrap_batch_arc(delivered);
        telemetry.count("logit.component.batches.received", 1.0, &[]);
        telemetry.count("logit.component.events.received", batch.events.len() as f64, &[]);
        *in_hand.lock().unwrap_or_else(|p| p.into_inner()) = Some((Arc::clone(&batch), ctx));
        store.push((batch, ctx)).await;
        *in_hand.lock().unwrap_or_else(|p| p.into_inner()) = None;
    }
    store.close();
}

/// Records `logit.component.inbox.batches`, how many batches are still waiting in `inbox`, under
/// the node's own `telemetry`. Called after each successful receive, so the gauge reads the
/// backlog a node leaves behind it as it works; a node parked in a full downstream send records
/// nothing new until it receives again.
fn sample_inbox_depth(inbox: &mpsc::Receiver<Delivered>, telemetry: &Telemetry) {
    if telemetry.is_enabled() {
        telemetry.gauge("logit.component.inbox.batches", inbox.len() as f64, &[]);
    }
}

/// The batch [`drain_inbox`] is pushing into its store, if a push is in progress. `run_output`
/// owns it and sweeps it at shutdown.
pub type InHand = std::sync::Mutex<Option<(Arc<EventBatch>, BatchContext)>>;

/// Converts a `Delivered` into the store's `Arc<EventBatch>`: one `Arc::new` for
/// `Delivered::Owned`, a move for `Delivered::Shared`.
///
/// Discards the `BatchContext`; callers read it with `Delivered::batch_context` first
/// (`docs/design/pipeline-graph.md`'s "Trace context propagation" section).
fn unwrap_batch_arc(delivered: Delivered) -> Arc<EventBatch> {
    match delivered {
        Delivered::Owned(batch, _ctx) => Arc::new(batch),
        Delivered::Shared(shared, _ctx) => shared,
    }
}

/// Backoff schedule for every sink's delivery in [`write_loop`]
/// (`docs/adr/sink-fault-classes.md`, "A retryable fault retries until it succeeds"). A retry
/// has no end other than success or the shutdown grace; the sink's `buffer:` bounds what queues
/// behind a held head.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Backoff after attempt `n` is `base_delay * 2^(n-1)`, capped at `max_delay`. No jitter:
    /// one writer per sink, not a fleet.
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self { base_delay: Duration::from_millis(200), max_delay: Duration::from_secs(10) }
    }
}

/// Delivery config for `write_loop`; the queue itself is `SinkStoreConfig`'s.
#[derive(Debug, Clone, Copy)]
pub struct WriteLoopConfig {
    pub retry: RetryConfig,
    /// Caps `write_loop`'s drain time after shutdown fires, measured from the first signal (not
    /// reset per batch), so a down sink can't hang exit (`docs/adr/buffered-sink-delivery.md`).
    pub shutdown_grace: Duration,
    /// Overrides the sink's `Output::default_posture()`; `None` uses it. Set from
    /// `logit-config::BufferConfig::delivery`.
    pub delivery_override: Option<DeliveryPosture>,
    /// The least time between two `retrying` lines while one head keeps failing. Not
    /// config-exposed; a test sets it small.
    pub retrying_log_interval: Duration,
}

impl Default for WriteLoopConfig {
    fn default() -> Self {
        Self {
            retry: RetryConfig::default(),
            shutdown_grace: Duration::from_secs(5),
            delivery_override: None,
            retrying_log_interval: RETRYING_LOG_INTERVAL,
        }
    }
}

/// A Lua node's stall and wedge thresholds, read by [`watch_lua_thread`], and its memory cap, read
/// by [`run_lua_loop`] (`docs/adr/lua-runaway-script-bounds.md`). Only `max_memory` is
/// config-exposed.
#[derive(Debug, Clone, Copy)]
pub struct LuaRuntimeConfig {
    /// How long the thread may sit inside one `process()`/`flush()` with its [`Heartbeat`]
    /// unchanged before the node reads [`NodeState::Stalled`]. The node reads `Running` again
    /// on the next change.
    pub stall_after: Duration,
    /// Once shutdown has begun, how long the thread may sit inside a call with its heartbeat
    /// unchanged before the watcher revokes its I/O and fails the node, measured from the later
    /// of the signal and the last change. A node already `Stalled` is measured from its last
    /// change alone, which with the defaults means the watcher's next tick after the signal.
    ///
    /// Shorter than a sink's `WriteLoopConfig::shutdown_grace` (5 s): revoking the node closes
    /// its downstream inboxes, and a downstream `aggregate`'s close-time flush must reach the sink
    /// before that sink's `write_loop` stops draining.
    pub shutdown_grace: Duration,
    /// The component's `max_memory`: the VM bytes over which, after the full collections
    /// [`MemoryVerdict`] runs, the node fails. `None` is no limit. The one config-exposed field.
    pub max_memory: Option<usize>,
    /// The least time between two forced `max_memory` verdicts; the spacing is also at least ten
    /// times the last verdict's own duration. Not config-exposed.
    pub min_verdict_spacing: Duration,
}

impl Default for LuaRuntimeConfig {
    fn default() -> Self {
        Self {
            stall_after: Duration::from_secs(10),
            shutdown_grace: Duration::from_secs(2),
            max_memory: None,
            min_verdict_spacing: MIN_VERDICT_SPACING,
        }
    }
}

/// What one batch's delivery, through however many retries, ended in.
enum Delivery {
    Delivered,
    /// Never delivered, and not worth retrying: `fault` is `Rejected`, or `Ambiguous` under
    /// at-most-once. The caller commits and counts it. `err` is the attempt's error, carrying the
    /// destination's text for the drop diagnostic.
    Dropped {
        fault: Fault,
        err: anyhow::Error,
    },
    /// The shutdown grace deadline had passed before an attempt, so none was started. The
    /// batch never left the process; the caller leaves it uncommitted and counts nothing.
    GraceExpired,
}

/// Announces a head that keeps failing (`docs/adr/sink-fault-classes.md`, "A hold is
/// announced"): [`RETRYING_GAUGE`] at 1 from its first retryable failure, and an error line on
/// that failure and then at most once per `interval` while the head keeps failing, each carrying
/// the class, the destination's text, how long the head has been held, the failure count, and the
/// queued count. Paced by time, not by failure count, so an operator tailing the log during an
/// outage sees a line every `interval` however long it lasts. [`Retrying::settle`] ends it when
/// the head is delivered or dropped; dropping the value settles too, so every `write_loop` exit
/// leaves the gauge at 0.
struct Retrying {
    telemetry: Telemetry,
    diag: Diagnostics,
    interval: Duration,
    /// Failed attempts at the current head, retryable ones only.
    failures: u64,
    /// When the current head first failed, and when its last line was written.
    since: Option<tokio::time::Instant>,
    last_line: Option<tokio::time::Instant>,
}

impl Retrying {
    fn new(telemetry: Telemetry, diag: Diagnostics, interval: Duration) -> Self {
        Self { telemetry, diag, interval, failures: 0, since: None, last_line: None }
    }

    /// Records a retryable failure of the head, before the backoff that precedes its retry.
    fn failed(&mut self, fault: Fault, err: &anyhow::Error, queued: usize) {
        let now = tokio::time::Instant::now();
        self.failures += 1;
        let since = *self.since.get_or_insert(now);
        if self.failures == 1 {
            self.telemetry.gauge(RETRYING_GAUGE, 1.0, &[]);
        }
        if self.last_line.is_some_and(|last| now.duration_since(last) < self.interval) {
            return;
        }
        self.last_line = Some(now);
        self.diag.error(
            "retrying",
            format_args!(
                "send failed ({fault}): {}; retrying until it succeeds (held {:?}, failure {}, \
                 {queued} batch(es) queued)",
                destination_text(err),
                now.duration_since(since),
                self.failures
            ),
        );
    }

    /// Whether the current head failed at least once.
    fn is_retrying(&self) -> bool {
        self.failures > 0
    }

    /// Ends the current head's announcement: it was delivered or dropped.
    fn settle(&mut self) {
        if self.failures > 0 {
            self.failures = 0;
            self.since = None;
            self.last_line = None;
            self.telemetry.gauge(RETRYING_GAUGE, 0.0, &[]);
        }
    }
}

impl Drop for Retrying {
    fn drop(&mut self) {
        self.settle();
    }
}

/// Attempts to deliver `batch` via `output.send`, retrying per `posture`/[`is_retryable`] until
/// it succeeds or a failure isn't retryable. [`write_loop`]'s path for a head that starts with a
/// window of 1, nothing submitted, and nothing observed ahead of it; [`deliver_window`] is the
/// other. An attempt runs under no runtime timeout: the sink bounds it (`Output::send`'s
/// contract), and the shutdown grace cuts it (`write_loop`'s `DeliverStep`).
///
/// Before every attempt, the first and each one after a backoff, a `grace_deadline` already
/// anchored and reached returns [`Delivery::GraceExpired`] with no send started. [`write_loop`]
/// polls this future before its grace arm, so without the check a send started on a deadline
/// already past would be cut off on its first poll and read as ambiguous.
///
/// `sending` is `true` only while an `output.send` call is in flight: set immediately before the
/// attempt's await and cleared as soon as it returns, before classification and before any
/// backoff sleep. So when [`write_loop`] drops this future for the shutdown grace, `true` means
/// `send` was polled at least once and hadn't completed. A grace that lands during a backoff
/// sleep leaves it `false`.
#[allow(clippy::too_many_arguments)]
async fn deliver_with_retry(
    output: &mut (dyn Output + Send),
    batch: &EventBatch,
    store: &SinkStore,
    posture: DeliveryPosture,
    retry: &RetryConfig,
    telemetry: &Telemetry,
    retrying: &mut Retrying,
    grace_deadline: &std::sync::OnceLock<tokio::time::Instant>,
    sending: &mut bool,
) -> Delivery {
    // Graph rule 15 rejects a zero `max_delay`, which would retry with no pause.
    debug_assert!(!retry.max_delay.is_zero(), "a zero retry max delay");
    let mut attempt: u32 = 0;
    loop {
        if grace_deadline.get().is_some_and(|&due| tokio::time::Instant::now() >= due) {
            return Delivery::GraceExpired;
        }
        attempt = attempt.saturating_add(1);
        let timer = telemetry.timer("logit.component.send.duration");
        *sending = true;
        let result = output.send(batch).await;
        *sending = false;
        drop(timer);

        let Err(err) = result else {
            return Delivery::Delivered;
        };
        telemetry.count("logit.component.errors", 1.0, &[]);
        let fault = classify(&err);
        if !is_retryable(fault, posture) {
            return Delivery::Dropped { fault, err };
        }
        retrying.failed(fault, &err, store.queued());
        telemetry.count("logit.component.retries", 1.0, &[]);
        tokio::time::sleep(backoff_for(retry, attempt)).await;
    }
}

/// [`write_loop`]'s record of its window over the store's head (`docs/adr/native-hop-send-window.md`,
/// decision 4). It outlives each [`deliver_window`] call, so a call the grace cuts off leaves it
/// for `write_loop` to read.
#[derive(Debug, Default)]
struct InFlight {
    /// Batches submitted and not yet acknowledged: always the store's first `outstanding` items.
    /// A fault resets it to 0, since the sink dropped its connection.
    outstanding: usize,
    /// The store's first `observed` items have had `Output::observe_batch` called. A commit
    /// lowers it by one; a fault doesn't reset it, so a resubmitted batch isn't observed again.
    /// Never below `outstanding`.
    observed: usize,
    /// `true` while an `Output::submit` call is in flight.
    submitting: bool,
    /// Set by [`deliver_window`] before it returns `Delivery::Dropped`: how many of the store's
    /// first items the fault left with an unknown outcome, the head included.
    at_fault: usize,
}

impl InFlight {
    /// How many of the store's first items have left the process, wholly or in part, and are
    /// unacknowledged: what a grace cut leaves with an unknown outcome.
    fn left_the_process(&self) -> usize {
        self.outstanding + usize::from(self.submitting)
    }
}

/// Delivers the store's head when [`write_loop`] doesn't take the `send` path: the sink's window
/// is above 1, or batches are submitted or observed past the head
/// (`docs/adr/native-hop-send-window.md`, decision 4). Fills the window from the store with
/// `Output::submit`, then waits for the head's
/// acknowledgment with `Output::await_ack`, retrying the round per `posture` until the head is
/// acknowledged or a fault isn't retryable. `state.outstanding` may be above 0 on entry, from
/// earlier heads' fills.
///
/// - **Fill.** While `state.outstanding` is below `min(output.window(), store.max_in_flight())`,
///   it submits the next item, observing it first if it hasn't been. `peek_at` returning `None`
///   ends the fill.
/// - **No submit or acknowledgment wait has a runtime timeout**: the sink bounds every write and
///   every acknowledgment wait itself (the `Output::window` contract).
/// - **A submit failure at the head** is the round's fault. **A failure past the head classifies
///   nothing**: the fill stops and the acknowledgments already owed are read.
/// - **A failed `await_ack`** sets `state.outstanding` to 0 and is the round's fault, covering
///   every item that was outstanding. One marked [`crate::HeadOnly`] covers the head alone and
///   leaves `state.outstanding` counting it, for `write_loop`'s commit to lower; the items behind
///   it stay submitted.
///
/// The shutdown grace is the one thing that cuts a round short (`write_loop`'s `DeliverStep`).
/// Before returning `Delivery::Dropped` it has set `state.at_fault`; `write_loop` commits and
/// counts the drops. Telemetry per round matches [`deliver_with_retry`]'s per attempt: one
/// `send.duration` sample, one `errors` per failed round, and one `retries` per retry. As there,
/// a grace deadline already reached before a round returns [`Delivery::GraceExpired`] with
/// nothing new submitted.
#[allow(clippy::too_many_arguments)]
async fn deliver_window(
    output: &mut (dyn Output + Send),
    store: &SinkStore,
    head: &StoreItem,
    posture: DeliveryPosture,
    retry: &RetryConfig,
    telemetry: &Telemetry,
    retrying: &mut Retrying,
    grace_deadline: &std::sync::OnceLock<tokio::time::Instant>,
    state: &mut InFlight,
) -> Delivery {
    debug_assert!(!retry.max_delay.is_zero(), "a zero retry max delay");
    let mut attempt: u32 = 0;
    loop {
        if grace_deadline.get().is_some_and(|&due| tokio::time::Instant::now() >= due) {
            return Delivery::GraceExpired;
        }
        attempt = attempt.saturating_add(1);
        let timer = telemetry.timer("logit.component.send.duration");
        let result = window_round(output, store, head, state).await;
        drop(timer);

        let Err(err) = result else {
            return Delivery::Delivered;
        };
        telemetry.count("logit.component.errors", 1.0, &[]);
        let fault = classify(&err);
        if !is_retryable(fault, posture) {
            return Delivery::Dropped { fault, err };
        }
        retrying.failed(fault, &err, store.queued());
        telemetry.count("logit.component.retries", 1.0, &[]);
        tokio::time::sleep(backoff_for(retry, attempt)).await;
    }
}

/// One [`deliver_window`] round: the fill, then the head's `await_ack`. `Ok` means the head was
/// acknowledged. Every `Err` has set `state.at_fault`.
async fn window_round(
    output: &mut (dyn Output + Send),
    store: &SinkStore,
    head: &StoreItem,
    state: &mut InFlight,
) -> anyhow::Result<()> {
    loop {
        let window = output.window().min(store.max_in_flight()).max(1);
        if state.outstanding >= window {
            break;
        }
        let position = state.outstanding;
        // The head is held already, and `peek_at(0)` would return the same item.
        let item = match position {
            0 => Some(head.clone()),
            _ => store.peek_at(position).await,
        };
        let Some((batch, ctx, seq)) = item else {
            break;
        };
        if position >= state.observed {
            output.observe_batch(ctx, seq);
            state.observed = position + 1;
        }
        state.submitting = true;
        let result = output.submit(&batch, ctx, seq).await;
        state.submitting = false;
        match result {
            Ok(()) => state.outstanding += 1,
            Err(err) if position == 0 => {
                state.at_fault = 1;
                return Err(err);
            }
            // Past the head: the sink keeps its connection for the acknowledgments already owed.
            Err(_) => break,
        }
    }
    debug_assert!(state.outstanding > 0, "the fill submits the head or returns its fault");
    let err = match output.await_ack().await {
        Ok(()) => return Ok(()),
        Err(err) => err,
    };
    // The head alone failed and the sink kept the rest in flight: `write_loop` commits the head
    // and lowers `outstanding` past it, as for a delivered head.
    if is_head_only(&err) {
        debug_assert_eq!(classify(&err), Fault::Rejected, "only a Rejected head settles alone");
        state.at_fault = 1;
        return Err(err);
    }
    state.at_fault = state.outstanding;
    state.outstanding = 0;
    Err(err)
}

/// At shutdown, the store's first `n` items left the process and their outcome is unknown: the
/// `Fault::Ambiguous` case. Under at-most-once this commits each, counts it a shutdown drop, and
/// warns. Under at-least-once it leaves them reserved: `SinkStore::finish` counts them for
/// `Memory`, and `Disk` replays them on its next open.
fn drop_cut_off_at_shutdown(
    store: &SinkStore,
    n: usize,
    posture: DeliveryPosture,
    telemetry: &Telemetry,
    shutdown_dropped: &AtomicU64,
    diag: &Diagnostics,
) {
    if n == 0 || is_retryable(Fault::Ambiguous, posture) {
        return;
    }
    let mut batches = 0u64;
    let mut events = 0u64;
    for _ in 0..n {
        if let Some((batch, ..)) = store.commit() {
            batches += 1;
            events += batch.events.len() as u64;
        }
    }
    count_shutdown_drop(telemetry, shutdown_dropped, batches, events);
    if batches > 0 {
        diag.warn(format_args!(
            "{batches} batch(es) cut off mid-send at shutdown were dropped: the destination may \
             have taken them, and at-most-once delivery never replays them"
        ));
    }
}

/// The backoff before retry attempt `attempt + 1`: `base_delay` doubled `attempt - 1` times with
/// `saturating_mul`, stopping once at or past `max_delay`. Correct for any `base_delay`/`max_delay`
/// pair, which a single `base_delay * 2u32.pow(shift)` with a fixed shift cap isn't. The 128
/// bound matters only for a zero `base_delay`, which never grows; a nonzero one saturates
/// `Duration` in under 100 doublings.
fn backoff_for(retry: &RetryConfig, attempt: u32) -> Duration {
    let mut backoff = retry.base_delay;
    for _ in 0..attempt.saturating_sub(1).min(128) {
        if backoff >= retry.max_delay {
            break;
        }
        backoff = backoff.saturating_mul(2);
    }
    backoff.min(retry.max_delay)
}

/// A [`Fault`] as the `&'static str` `SpanGuard::tag` needs, avoiding `Display`'s allocation.
fn fault_tag(fault: Fault) -> &'static str {
    match fault {
        Fault::Clean => "clean",
        Fault::Ambiguous => "ambiguous",
        Fault::Rejected => "rejected",
        Fault::Refused => "refused",
    }
}

/// `err`'s chain as `{err:#}` prints it, minus the links that are only a [`Fault`]'s class or a
/// [`HeadOnly`] marker: the line that prints it names the class once itself. A sink attaches both
/// as anyhow context, and that link's concrete type is anyhow's internal `ContextError`, which no
/// `dyn Error` downcast matches ([`classify`]'s doc), so the links are matched by their text.
fn destination_text(err: &anyhow::Error) -> String {
    const CLASSES: [Fault; 4] = [Fault::Clean, Fault::Ambiguous, Fault::Rejected, Fault::Refused];
    let head_only = HeadOnly.to_string();
    let mut text = String::new();
    for link in err.chain() {
        let link = link.to_string();
        if CLASSES.iter().any(|class| fault_tag(*class) == link) || link == head_only {
            continue;
        }
        if !text.is_empty() {
            text.push_str(": ");
        }
        text.push_str(&link);
    }
    text
}

/// The `reason` a write-loop drop counts under. Only two faults drop: `Rejected` under either
/// posture, and `Ambiguous` under at-most-once (`docs/design/internal-telemetry.md`, "Sinks:
/// `SinkStore`").
fn drop_reason(fault: Fault) -> &'static str {
    debug_assert!(matches!(fault, Fault::Rejected | Fault::Ambiguous), "{fault} never drops");
    match fault {
        Fault::Ambiguous => "ambiguous_at_most_once",
        Fault::Clean | Fault::Rejected | Fault::Refused => "rejected",
    }
}

/// Resolves `grace` after the first poll that sees `shutdown` fired, never before it fires.
///
/// The deadline is anchored at that first poll after the signal, not at the signal instant.
/// Every caller runs this under `tokio::task::unconstrained` in a `select!` it re-polls on each
/// wake, which keeps that poll within a few wakes of the signal: a `select!` polls no arm once
/// the task's coop budget is spent, and `write_loop` runs inside `run_output`'s unbiased
/// `select!` with `drain_inbox`, which is polled first on half the wakes and can spend it.
///
/// `deadline` persists across calls (one per `write_loop` iteration), so the window is not reset
/// per batch, and `deliver_with_retry` reads it to start no attempt past it. It's set
/// synchronously when `wait_for` resolves, so a call dropped after that poll (one that loses a
/// `select!` race) keeps the anchor and the next call waits out the remainder. A call never
/// polled after the signal anchors nothing. Cancellation-safe.
async fn shutdown_grace_expired(
    shutdown: &mut watch::Receiver<bool>,
    deadline: &std::sync::OnceLock<tokio::time::Instant>,
    grace: Duration,
) {
    if deadline.get().is_none() {
        // An error means the sender is gone; treat it as shutdown firing rather than hang.
        let _ = shutdown.wait_for(|&due| due).await;
        let _ = deadline.set(tokio::time::Instant::now() + grace);
    }
    let due = *deadline.get().expect("set above if it was unset");
    tokio::time::sleep_until(due).await;
}

/// Finalizes what `store` still holds, counting and logging anything dropped, then calls
/// `output.flush()` once. `run_output` calls it on every exit path of `write_loop`, and only after
/// nothing can push into `store` any more.
///
/// Must not run inside `write_loop`: committing wakes a producer blocked on `not_full`, so a
/// concurrent `drain_inbox` could push a batch after the queue was seen empty and while `flush()`
/// was pending, and that batch would be lost uncounted when `drain_inbox` was cancelled.
///
/// `SinkStore::finish` supplies the counts; for `Disk` they're always `(0, 0)`, since a
/// disk-backed sink keeps everything still queued.
async fn finish_and_flush(
    diag: &Diagnostics,
    store: &SinkStore,
    telemetry: &Telemetry,
    output: &mut (dyn Output + Send),
    shutdown_dropped: &AtomicU64,
) {
    let (dropped_batches, dropped_events) = store.finish().await;
    if dropped_batches > 0 {
        count_shutdown_drop(telemetry, shutdown_dropped, dropped_batches, dropped_events);
        // Unthrottled: fires at most once per `run_output`.
        diag.warn(format_args!(
            "{dropped_batches} batch(es) ({dropped_events} event(s)) still queued when this sink \
             stopped, undelivered"
        ));
    }
    if let Err(err) = output.flush().await {
        telemetry.count("logit.component.errors", 1.0, &[("reason", "flush")]);
        diag.warn(format_args!("flush failed: {err}"));
    }
}

/// Delivers from `store`'s head until `store.peek()` returns `None` (closed and empty) or shutdown
/// grace expires. The posture is `write_config.delivery_override`, else `output.default_posture()`
/// (`docs/adr/delivery-semantics.md`, item 5).
///
/// Each head goes one of two ways (`docs/adr/native-hop-send-window.md`, decision 4). With
/// nothing submitted or observed past the store's head and `output.window()` at most 1, it is
/// observed and delivered through [`deliver_with_retry`] and `output.send`, one batch at a time.
/// Otherwise [`deliver_window`] keeps up to the window submitted ahead of it, carrying the
/// [`InFlight`] counts from head to head. The `observed` term keeps a batch observed in an earlier
/// window off `send`, which would observe it again and, for `logit_out`, send it under the
/// wrong pending sequence.
///
/// A retryable failure holds the head and retries it until it succeeds or the shutdown grace
/// cuts it, announced through [`Retrying`]. A batch that isn't worth retrying is committed,
/// counted under [`drop_reason`], and warned about. Nothing a sink answers ends the loop early
/// or the pipeline (`docs/adr/sink-fault-classes.md`).
///
/// Never drains the queue or calls `output.flush()`; [`finish_and_flush`] does, and says why.
/// Returns when shutdown grace expires: an incomplete drain on shutdown isn't a failure.
/// A send the grace cuts off mid-flight is `Fault::Ambiguous`, decided by [`is_retryable`]:
/// under at-least-once it stays queued, under at-most-once it's committed and counted through
/// [`count_shutdown_drop`] into `shutdown_dropped`
/// (`docs/adr/shutdown-accounting-and-cancellation-safety.md`, decision 3). Under a window, so is
/// every batch submitted and unacknowledged, and one whose submit was cut off.
pub(crate) async fn write_loop(
    id: String,
    output: &mut (dyn Output + Send),
    store: Arc<SinkStore>,
    telemetry: Telemetry,
    write_config: WriteLoopConfig,
    mut shutdown: watch::Receiver<bool>,
    shutdown_dropped: &AtomicU64,
) {
    let posture = write_config.delivery_override.unwrap_or_else(|| output.default_posture());
    output.observe_posture(posture);
    let mut diag = Diagnostics::new(id).with_telemetry(telemetry.clone());
    let mut retrying =
        Retrying::new(telemetry.clone(), diag.clone(), write_config.retrying_log_interval);

    let mut last_success: Option<tokio::time::Instant> = None;
    // A `OnceLock`, not an `Option`: the grace arm sets it while `deliver_with_retry`, in the
    // same `select!`, reads it.
    let shutdown_deadline = std::sync::OnceLock::new();
    // Turns a stream of failures into two edge events: `degraded` on the first drop, `recovered`
    // on the next success after a drop or a retried failure.
    let mut degraded = false;
    let mut in_flight = InFlight::default();

    loop {
        // Each `select!` reduces to a plain enum so no handler arm touches `output`: the
        // `deliver_with_retry` future already borrows it mutably, and the borrow checker rejects
        // a second overlapping borrow inside the macro.
        enum NextBatch {
            Batch(Arc<EventBatch>, BatchContext, SeqId),
            Closed,
            ShutdownExpired,
        }
        // Unbiased; `unconstrained` grace arm: see `docs/design/pipeline-graph.md`'s
        // "Cancellation points".
        let next = tokio::select! {
            batch = store.peek() => match batch {
                Some((batch, ctx, seq)) => NextBatch::Batch(batch, ctx, seq),
                None => NextBatch::Closed,
            },
            () = tokio::task::unconstrained(shutdown_grace_expired(
                &mut shutdown,
                &shutdown_deadline,
                write_config.shutdown_grace,
            )) => NextBatch::ShutdownExpired,
        };
        let (batch, ctx, seq) = match next {
            NextBatch::Batch(batch, ctx, seq) => (batch, ctx, seq),
            NextBatch::Closed => break, // queue closed and empty: nothing left to deliver.
            NextBatch::ShutdownExpired => {
                // Batches a window left submitted and unacknowledged are cut off here as they
                // are mid-round: the previous head's acknowledgment can win the deliver arm on
                // the wake the grace expires, and this unbiased `select!` can then pick the grace.
                // No submit is in progress between rounds.
                drop_cut_off_at_shutdown(
                    &store,
                    in_flight.outstanding,
                    posture,
                    &telemetry,
                    shutdown_dropped,
                    &diag,
                );
                return;
            }
        };

        // The sink span, the only one that carries `SpanStatus::Error` and a fault tag
        // (`docs/adr/internal-span-emission-and-deterministic-sampling.md`). Its `child()` context
        // is its own identity and goes nowhere else: a sink has nothing downstream to propagate to.
        let span_ctx = ctx.trace.child();
        let mut span = telemetry.span(
            "deliver",
            SpanKind::Client,
            span_ctx.trace_id,
            span_ctx.span_id,
            Some(ctx.trace.span_id),
        );
        span.events(batch.events.len() as u64);

        enum DeliverStep {
            Outcome(Delivery),
            ShutdownExpired,
        }
        // The fast path's: `true` while `output.send` is in flight.
        let mut sending = false;
        let fast = in_flight.outstanding == 0 && in_flight.observed == 0 && output.window() <= 1;
        // `biased`, deliver arm first; `unconstrained` grace arm: see
        // `docs/design/pipeline-graph.md`'s "Cancellation points".
        let step = if fast {
            // Once per batch, before its first attempt. `logit_out` carries provenance and the
            // store's sequence across the wire from it (`docs/adr/batch-provenance-on-delivered.md`,
            // `docs/adr/native-hop-identity-and-sequence.md`), and a sink with encode-side
            // counters arms its per-batch accounting
            // (`docs/adr/sink-send-path-and-attempt-accounting.md`, decision 2). `deliver_window`
            // observes the batches it submits itself.
            output.observe_batch(ctx, seq);
            tokio::select! {
                biased;
                outcome = deliver_with_retry(
                    output,
                    &batch,
                    &store,
                    posture,
                    &write_config.retry,
                    &telemetry,
                    &mut retrying,
                    &shutdown_deadline,
                    &mut sending,
                ) => DeliverStep::Outcome(outcome),
                () = tokio::task::unconstrained(shutdown_grace_expired(
                    &mut shutdown,
                    &shutdown_deadline,
                    write_config.shutdown_grace,
                )) => DeliverStep::ShutdownExpired,
            }
        } else {
            let head = (Arc::clone(&batch), ctx, seq);
            tokio::select! {
                biased;
                outcome = deliver_window(
                    output,
                    &store,
                    &head,
                    posture,
                    &write_config.retry,
                    &telemetry,
                    &mut retrying,
                    &shutdown_deadline,
                    &mut in_flight,
                ) => DeliverStep::Outcome(outcome),
                () = tokio::task::unconstrained(shutdown_grace_expired(
                    &mut shutdown,
                    &shutdown_deadline,
                    write_config.shutdown_grace,
                )) => DeliverStep::ShutdownExpired,
            }
        };
        let outcome = match step {
            DeliverStep::Outcome(outcome) => outcome,
            DeliverStep::ShutdownExpired => {
                // What left the process unacknowledged: on the fast path the one send in flight,
                // under a window every batch submitted and one whose submit was cut off.
                let cut_off =
                    if fast { usize::from(sending) } else { in_flight.left_the_process() };
                if cut_off > 0 {
                    span.error();
                    span.tag("fault", fault_tag(Fault::Ambiguous));
                    drop_cut_off_at_shutdown(
                        &store,
                        cut_off,
                        posture,
                        &telemetry,
                        shutdown_dropped,
                        &diag,
                    );
                }
                // Any batch still reserved here is benign: `SinkStore::finish` commits and
                // counts it for `Memory`, and persists the read cursor at the head for `Disk`, so
                // it replays on the next open.
                return;
            }
        };

        match outcome {
            // No attempt started: left uncommitted for `finish_and_flush`, like a grace expiry
            // between batches. Batches already submitted under a window left the process, and
            // are treated as a grace cut treats them.
            Delivery::GraceExpired => {
                let cut_off = in_flight.outstanding;
                if cut_off > 0 {
                    span.error();
                    span.tag("fault", fault_tag(Fault::Ambiguous));
                    drop_cut_off_at_shutdown(
                        &store,
                        cut_off,
                        posture,
                        &telemetry,
                        shutdown_dropped,
                        &diag,
                    );
                }
                return;
            }
            Delivery::Delivered => {
                store.commit();
                in_flight.outstanding = in_flight.outstanding.saturating_sub(1);
                in_flight.observed = in_flight.observed.saturating_sub(1);
                telemetry.count("logit.component.batches.delivered", 1.0, &[]);
                telemetry.count("logit.component.events.delivered", batch.events.len() as f64, &[]);
                last_success = Some(tokio::time::Instant::now());
                if degraded || retrying.is_retrying() {
                    degraded = false;
                    diag.info("recovered", "delivery succeeded after a prior failure");
                }
                retrying.settle();
            }
            Delivery::Dropped { fault, err } => {
                retrying.settle();
                // Every dropped round leaves nothing outstanding but one whose head alone failed
                // (`Output::await_ack`'s `HeadOnly`), which leaves the head and those behind it.
                debug_assert!(
                    in_flight.outstanding == 0 || is_head_only(&err),
                    "a dropped round leaves nothing out"
                );
                in_flight.outstanding = in_flight.outstanding.saturating_sub(1);
                // The head, then, under at-most-once, every other batch an `Ambiguous` fault
                // left with an unknown outcome: each is as ambiguous as the head, and
                // at-most-once never resends one. A `Rejected` fault drops only the head; the rest
                // are submitted again.
                let also_in_flight =
                    if !fast && fault == Fault::Ambiguous && !is_retryable(fault, posture) {
                        in_flight.at_fault.saturating_sub(1)
                    } else {
                        0
                    };
                let mut dropped_batches = 1u64;
                let mut dropped_events = batch.events.len() as u64;
                store.commit();
                in_flight.observed = in_flight.observed.saturating_sub(1);
                for _ in 0..also_in_flight {
                    if let Some((other, ..)) = store.commit() {
                        dropped_batches += 1;
                        dropped_events += other.events.len() as u64;
                    }
                    in_flight.observed = in_flight.observed.saturating_sub(1);
                }
                span.error();
                span.tag("fault", fault_tag(fault));
                let reason = drop_reason(fault);
                telemetry.count(
                    "logit.component.batches.dropped",
                    dropped_batches as f64,
                    &[("reason", reason)],
                );
                telemetry.count(
                    "logit.component.events.dropped",
                    dropped_events as f64,
                    &[("reason", reason)],
                );
                let since_success = last_success
                    .map(|t| format!("{:?} ago", t.elapsed()))
                    .unwrap_or_else(|| "never".to_string());
                diag.warn_throttled(
                    "send_failed",
                    format_args!(
                        "batch dropped after a {fault} send failure: {} (last successful \
                         delivery: {since_success})",
                        destination_text(&err)
                    ),
                );
                if !degraded {
                    degraded = true;
                    diag.warn("degraded");
                }
            }
        }
    }
}

/// The receive counts and one `Output::send` call with its `send.duration` sample and error
/// count, for `logit-bench`'s allocation tests and benches to measure that hop in isolation. Call
/// it from a `current_thread` runtime with no `tokio::spawn`.
///
/// Not on the runtime's delivery path, and not `run_output`'s per-batch body: `drain_inbox`
/// counts receipt, and `write_loop` calls `output.observe_batch` once and then `output.send`
/// through `deliver_with_retry`, which retries on the error's `Fault`. This calls no
/// `observe_batch`, so a sink's per-batch accounting stays unarmed and counts every call.
pub async fn send_batch(
    id: &str,
    output: &mut (dyn Output + Send),
    delivered: &Delivered,
    telemetry: &Telemetry,
) -> anyhow::Result<()> {
    let batch: &EventBatch = match delivered {
        Delivered::Owned(batch, _ctx) => batch,
        Delivered::Shared(shared, _ctx) => shared,
    };
    telemetry.count("logit.component.batches.received", 1.0, &[]);
    telemetry.count("logit.component.events.received", batch.events.len() as f64, &[]);

    let timer = telemetry.timer("logit.component.send.duration");
    let result = output.send(batch).await;
    drop(timer);
    if result.is_err() {
        telemetry.count("logit.component.errors", 1.0, &[]);
    }
    result.with_context(|| format!("component '{id}'"))
}

/// A `Transform`-trait node's loop: races its inbox against its flush deadline, if it has one,
/// with `tokio::time::timeout`. `run_lua` has the same shape through `Handle::block_on`. Flushes
/// once more when the inbox closes.
async fn run_transform(
    mut transform: Box<dyn Transform + Send>,
    mut inbox: mpsc::Receiver<Delivered>,
    fanout: Fanout,
    telemetry: Telemetry,
) -> anyhow::Result<()> {
    let mut next_flush =
        transform.flush_interval().map(|interval| tokio::time::Instant::now() + interval);

    loop {
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                run_flush(&mut *transform, &fanout, &telemetry).await;
                let interval = transform
                    .flush_interval()
                    .expect("next_flush is only ever Some for a transform with an interval");
                next_flush = Some(advance_flush_deadline(deadline, now_instant, interval));
            }
        }

        let batch = match next_flush {
            None => inbox.recv().await,
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
                match tokio::time::timeout(wait, inbox.recv()).await {
                    Ok(batch) => batch,
                    Err(_elapsed) => continue,
                }
            }
        };
        let Some(batch) = batch else {
            // Inbox closed: flush once more so an in-flight window isn't lost, then exit.
            if next_flush.is_some() {
                run_flush(&mut *transform, &fanout, &telemetry).await;
            }
            return Ok(());
        };
        sample_inbox_depth(&inbox, &telemetry);
        // Read before `unwrap_batch` consumes `batch`. Everything this call emits comes from this
        // one batch, so it's the unambiguous parent (`TraceContext`); `run_flush` has no single
        // parent and mints a root. `parent.provenance` passes through unchanged, for
        // `Fanout::stamp` to rewrite `previous` while keeping `origin`.
        let parent = batch.batch_context();
        // `observe_batch_context` lets a flush-bearing transform (`Aggregator`) link this batch
        // as a contributor to its next flush. `observe_provenance` hands `has_provenance`/
        // `drop_provenance` the batch's provenance to read in `process`. Both are no-ops for
        // other transforms.
        transform.observe_batch_context(parent.trace);
        transform.observe_provenance(parent.provenance);
        // Minted here, not in `Fanout`, because this node's span covers `process_batch` and the
        // send, and the span's `span_id` must equal the outgoing `Delivered`'s
        // (`docs/adr/internal-span-emission-and-deterministic-sampling.md`).
        let ctx = BatchContext { trace: parent.trace.child(), provenance: parent.provenance };
        let mut span = telemetry.span(
            "process",
            SpanKind::Internal,
            ctx.trace.trace_id,
            ctx.trace.span_id,
            Some(parent.trace.span_id),
        );
        let batch = unwrap_batch(batch);
        if let Some(out) = process_batch(&mut *transform, batch, &telemetry) {
            span.events(out.events.len() as u64);
            fanout.send_with_own_context(out, ctx).await;
        }
    }
}

/// The per-batch body of `run_transform`: telemetry accounting plus `Transform::process` over
/// every event in place, dropping what it absorbs. `Vec::retain_mut` over the batch's own
/// `events`, so a forwarded event never moves and the survivors need no new allocation.
///
/// `pub` so `crates/logit-bench/tests/allocations.rs` can measure the real path
/// (`docs/design/memory.md` §7).
pub fn process_batch(
    transform: &mut (dyn Transform + Send),
    batch: EventBatch,
    telemetry: &Telemetry,
) -> Option<EventBatch> {
    telemetry.count("logit.component.batches.received", 1.0, &[]);
    telemetry.count("logit.component.events.received", batch.events.len() as f64, &[]);

    // `map_resource` runs before any event reaches `process`. `None` (the common case) moves the
    // incoming `Arc` through with no clone; `Some` replaces it for `process` and the output.
    // `scope` passes through unchanged and is also handed to `observe_scope`, so a flush-bearing
    // transform (`Aggregator`) can stamp its flush with it (`FlushOutput`).
    let EventBatch { resource, scope, mut events } = batch;
    transform.observe_scope(scope.clone());
    let resource = transform.map_resource(&resource).unwrap_or(resource);

    let process_timer = telemetry.timer("logit.component.process.duration");
    let before = events.len();
    events.retain_mut(|event| transform.process(&resource, event));
    // Inside the timer: work a transform defers to `end_batch` is still this node's per-batch
    // cost (`docs/design/internal-telemetry.md`).
    transform.end_batch();
    drop(process_timer);
    let absorbed = (before - events.len()) as u64;
    if absorbed > 0 {
        telemetry.count(
            "logit.component.events.dropped",
            absorbed as f64,
            &[("reason", "absorbed")],
        );
    }
    if events.is_empty() {
        None
    } else {
        Some(EventBatch { resource, scope, events })
    }
}

/// A [`Router`] node's loop: `run_transform` minus the flush-deadline race, since no router
/// flushes (`crate::router`). Per incoming batch it mints one child context and records one
/// `"process"` span, then sends one batch per non-empty destination under that same context: one
/// batch is one hop however many ways it forks, as with `Fanout`
/// (`docs/design/pipeline-graph.md`'s "Trace context propagation").
///
/// Provenance passes through; each destination's `Fanout::stamp` rewrites `previous` to its own
/// id (the target's, or this router's for the forward edge) and keeps `origin`. That's all of
/// `docs/adr/target-components.md`'s "`previous` downstream of a target is the target's id".
async fn run_router(
    mut router: Box<dyn Router + Send>,
    mut inbox: mpsc::Receiver<Delivered>,
    fanout: Fanout,
    targets: Vec<Fanout>,
    telemetry: Telemetry,
) -> anyhow::Result<()> {
    // Reused for the node's life; `RouterScratch` documents the allocations that saves.
    let mut scratch = RouterScratch::new(targets.len());

    while let Some(batch) = inbox.recv().await {
        sample_inbox_depth(&inbox, &telemetry);
        // Every partition comes from this one batch: the unambiguous parent, as in
        // `run_transform`.
        let parent = batch.batch_context();
        router.observe_batch_context(parent.trace);
        router.observe_provenance(parent.provenance);
        // Minted here so the span and every outgoing `Delivered` share one `span_id`; every
        // destination gets the identical context.
        let ctx = BatchContext { trace: parent.trace.child(), provenance: parent.provenance };
        let mut span = telemetry.span(
            "process",
            SpanKind::Internal,
            ctx.trace.trace_id,
            ctx.trace.span_id,
            Some(parent.trace.span_id),
        );
        let batch = unwrap_batch(batch);
        // `observe_scope` and `map_resource` run inside `route_batch`, as in `process_batch`, so
        // the allocation suite measuring `route_batch` measures the node's real path.
        let partitions = route_batch(&mut *router, &mut scratch, batch, &telemetry);
        span.events(partitions.iter().map(|(_, batch)| batch.events.len() as u64).sum());

        for (slot, out) in partitions {
            // Slot 0 is this router's own outbound edge; slot n + 1 is `Destination::To(n)`.
            let destination = match slot {
                0 => &fanout,
                n => match targets.get(n - 1) {
                    Some(fanout) => fanout,
                    // Unreachable: `route_batch` never hands back a slot this scratch -- sized
                    // from `targets` itself -- wasn't built for.
                    None => continue,
                },
            };
            // `Fanout::deliver` returns early on zero consumers and counts nothing, so unrouted
            // events must be counted here. A router with targets and no ordinary consumers is
            // legal (rule 50); its forward partition is the events no route claimed.
            if slot == 0 && destination.is_empty() {
                telemetry.count(
                    "logit.component.events.dropped",
                    out.events.len() as f64,
                    &[("reason", "unrouted")],
                );
                continue;
            }
            destination.send_with_own_context(out, ctx).await;
        }
    }
    Ok(())
}

/// The per-batch body of `run_router`: telemetry accounting, `Router::route` over every event, and
/// one outgoing [`EventBatch`] per destination that received at least one event. `pub` so
/// `crates/logit-bench/tests/allocations.rs` measures the real path.
///
/// Four passes, because `Router::route` borrows its event (returning an `Event` through an enum
/// would memcpy the batch), so every verdict is recorded before anything moves:
///
/// 1. **Route.** One `Destination` per event, appended to `scratch.marks`.
/// 2. **Count.** `scratch.counts[d]` from those marks.
/// 3. **Reserve.** `reserve_exact(count)` on each destination with a nonzero count. The previous
///    batch's `std::mem::take` left every `scratch.dests` entry at capacity 0, so this is that
///    destination's only allocation.
/// 4. **Move.** Each event is pushed into its exactly sized buffer: no regrowth, no second copy.
///
/// Allocations: `1 + (destinations that received at least one event)` per batch, zero per event
/// (`docs/adr/target-components.md`; pinned by
/// `route_batch_two_targets_costs_one_vec_per_used_destination`). The `1` is the returned `Vec`,
/// sized `used` up front. `scratch.marks`/`scratch.counts` amortize to zero once grown to the
/// node's largest batch, `resource`/`scope` are refcount bumps, and an unused destination
/// allocates nothing.
///
/// An out-of-range `Destination::To(n)` is a bug in the `Router` impl: a `debug_assert!` in debug
/// builds, unrouted with a warning in release. A resolved graph never produces one.
pub fn route_batch(
    router: &mut (dyn Router + Send),
    scratch: &mut RouterScratch,
    batch: EventBatch,
    telemetry: &Telemetry,
) -> Vec<(usize, EventBatch)> {
    telemetry.count("logit.component.batches.received", 1.0, &[]);
    telemetry.count("logit.component.events.received", batch.events.len() as f64, &[]);

    // As in `process_batch`: `scope` passes through to every partition and is offered to
    // `observe_scope`; `map_resource`'s `None` (both shipped routers) moves the `Arc`, no clone.
    let scope = batch.scope.clone();
    router.observe_scope(scope.clone());
    let resource = router.map_resource(&batch.resource).unwrap_or(batch.resource);

    let process_timer = telemetry.timer("logit.component.process.duration");
    let slots = scratch.dests.len();
    scratch.marks.clear();
    scratch.marks.reserve(batch.events.len());
    scratch.counts.clear();
    scratch.counts.resize(slots, 0);

    // Pass 1: route, borrowing.
    for event in &batch.events {
        let mark = match router.route(&resource, event) {
            Destination::To(n) if usize::from(n) + 1 >= slots => {
                debug_assert!(
                    false,
                    "router returned Destination::To({n}) with only {} target slot(s)",
                    slots - 1
                );
                tracing::warn!(
                    target: "logit",
                    slot = n,
                    targets = slots - 1,
                    "router returned an out-of-range target slot; treating the event as unrouted"
                );
                Destination::Forward
            }
            mark => mark,
        };
        scratch.marks.push(mark);
    }

    // Pass 2: count.
    for mark in &scratch.marks {
        scratch.counts[slot_of(*mark)] += 1;
    }

    // Pass 3: reserve exactly, and only where something is going.
    let mut used = 0usize;
    for (dest, count) in scratch.dests.iter_mut().zip(&scratch.counts) {
        if *count > 0 {
            dest.reserve_exact(*count);
            used += 1;
        }
    }

    // Pass 4: move. A router never absorbs (`crate::router`: no `Destination::Drop`).
    for (event, mark) in batch.events.into_iter().zip(&scratch.marks) {
        scratch.dests[slot_of(*mark)].push(event);
    }
    drop(process_timer);

    let mut out = Vec::with_capacity(used);
    for (slot, dest) in scratch.dests.iter_mut().enumerate() {
        if dest.is_empty() {
            continue;
        }
        // Leaves a capacity-0 `Vec` for the next batch's `reserve_exact` (`RouterScratch`).
        let events = std::mem::take(dest);
        out.push((slot, EventBatch { resource: resource.clone(), scope: scope.clone(), events }));
    }
    out
}

/// The slot-ordered target `Fanout`s one routing node (`NodeSpec::Router`, or `NodeSpec::Lua`
/// with `targets:`) owns, cloned from [`run_with_telemetry`]'s pre-spawn map.
///
/// Slot order is `ResolvedComponent::targets`' order, derived only in `graph::targets_of`, so
/// `Destination::To(n)` and the n-th id in a Lua component's `targets:` name the same target.
///
/// # Panics
///
/// If a target id has no `Fanout`; graph rules 48/49 rule that out.
fn resolve_target_fanouts(
    id: &str,
    targets: &[String],
    built: &HashMap<String, Fanout>,
) -> Vec<Fanout> {
    targets
        .iter()
        .map(|target| {
            built.get(target.as_str()).cloned().unwrap_or_else(|| {
                panic!(
                    "component '{id}': target '{target}' has no Fanout -- rules 48/49 guarantee \
                     every target reference resolves to a defined `target`"
                )
            })
        })
        .collect()
}

/// `Destination` -> index into [`RouterScratch`]'s per-destination buffers: slot 0 is the router's
/// own outbound edge, slot `n + 1` is `Destination::To(n)`.
fn slot_of(destination: Destination) -> usize {
    match destination {
        Destination::Forward => 0,
        Destination::To(n) => usize::from(n) + 1,
    }
}

/// Runs one flush, for both `run_transform`'s deadline tick and its close-time flush. Timed as one
/// call however many resource groups it yields.
///
/// Mints one fresh root before `transform.flush(now)` and sends every resource group under it as
/// siblings: one flush is one unit of work, not one hop per group. A root, not a parent, because a
/// flush is *n*-to-1 over the batches absorbed since the last tick, with no single correct parent
/// (`TraceContext`; `docs/known-gaps/telemetry.md`'s internal-spans entry;
/// `docs/adr/internal-span-emission-and-deterministic-sampling.md`). The contributing-context
/// links `Transform::flush` returns per event are unioned onto the flush span, bounded by
/// `MAX_LINKS_PER_SPAN`.
async fn run_flush(transform: &mut (dyn Transform + Send), fanout: &Fanout, telemetry: &Telemetry) {
    // Empty provenance, for the same reason as the fresh root: a flush has no single upstream
    // batch. `Fanout::stamp` fills both `origin` and `previous` with this component's id, as for
    // a listener's first send (`docs/adr/batch-provenance-on-delivered.md`).
    let ctx: BatchContext = TraceContext::new_root().into();
    let mut span =
        telemetry.span("flush", SpanKind::Internal, ctx.trace.trace_id, ctx.trace.span_id, None);

    let timer = telemetry.timer("logit.component.flush.duration");
    let flushed = transform.flush(now_unix_nanos());
    drop(timer);

    let mut total_events: u64 = 0;
    for (resource, scope, events_with_links) in flushed {
        if events_with_links.is_empty() {
            continue;
        }
        telemetry.count("logit.component.flush.events", events_with_links.len() as f64, &[]);
        total_events += events_with_links.len() as u64;
        let mut events = Vec::with_capacity(events_with_links.len());
        for (event, links) in events_with_links {
            span.links(links);
            events.push(event);
        }
        // `Aggregator` groups by `(resource, scope)` value, so each flushed group carries the
        // one scope all its series share; dropping it would lose scope on
        // `otlp_in -> aggregate -> otlp_out`.
        fanout.send_with_own_context(EventBatch { resource, scope, events }, ctx).await;
    }
    span.events(total_events);
}

/// A Lua node's loop, on its own OS thread (`ScriptWorker` is `!Send`). Same shape as
/// `run_transform`, but the thread has no async context: `runtime` supplies one through
/// `Handle::block_on`, which is legal because this never runs on a runtime worker thread or
/// inside another `.await`. For the same reason it sends with `fanout.send_blocking`.
///
/// A Lua `flush()` runs in a root context (`docs/adr/lua-flush-root-context.md`). Before every
/// `flush()`, the script's four batch-scoped globals are reset: `trace` to the fresh root the
/// emission goes out under, `provenance` to this component as both `origin` and `previous`,
/// `resource` to empty, and `scope` to none. A script that writes `resource` or `scope` in
/// `flush()` gives the emission that identity (see `flush_now`). Nothing carries over from the
/// last processed batch.
///
/// A Lua node is also a router (`docs/adr/target-components.md`): `targets` is the component's
/// `targets:` in slot order, handed to the VM as `event:to("..")`'s name -> slot table, and
/// `target_fanouts` is the matching `Fanout`s. One incoming batch splits into at most
/// `target_fanouts.len() + 1` outgoing batches (slot 0 is the node's own edge) under one
/// `BatchContext`, as in `run_router`.
///
/// Two handshakes report to `run_with_telemetry`: `ready_tx` carries the script-load outcome (a
/// failure is `RunError::Startup`), and `done_tx` the post-ready outcome, `Err` on a panic or on
/// the VM staying over `max_memory` (see [`MemoryVerdict`]).
/// [`watch_lua_thread`] awaits `done_rx` as the node's `JoinSet` entry, so the join loop treats a
/// Lua exit like any task's. The loop runs under `catch_unwind` so a panic becomes a message, not
/// a dropped sender; `AssertUnwindSafe` because `ScriptWorker` holds `Lua` and `Rc<RefCell>`s and
/// nothing is used after the unwind. A script's own `process()`/`flush()` errors are logged and
/// counted in [`run_lua_loop`] and never end the node. `done_tx` sends only after the [`LuaIo`]
/// is taken out of `io` and dropped, so the downstream cascade is already underway when the
/// watcher resolves.
///
/// `io` is shared with the watcher so it can revoke the node's channels without the thread's
/// cooperation (see [`LuaIo`]); `heartbeat` is how the watcher tells a working script from a
/// stuck one (see [`watch_lua_thread`]).
#[allow(clippy::too_many_arguments)]
fn run_lua(
    id: String,
    script: String,
    configured_interval: Option<Duration>,
    targets: Vec<String>,
    ready_tx: oneshot::Sender<Result<(), String>>,
    done_tx: oneshot::Sender<Result<(), String>>,
    io: SharedLuaIo,
    heartbeat: Arc<Heartbeat>,
    max_memory: Option<usize>,
    verdict_spacing: Duration,
    telemetry: Telemetry,
    shutdown_dropped: Arc<AtomicU64>,
    runtime: tokio::runtime::Handle,
) {
    let worker = match ScriptWorker::new(&script)
        .and_then(|w| w.with_telemetry(telemetry.clone()))
        .map(|w| w.with_component(&id))
        .map(|w| w.with_targets(&targets))
        .map(|w| w.with_heartbeat(heartbeat.clone()))
    {
        Ok(worker) => worker,
        Err(err) => {
            // The receiver is gone only if `run` already bailed; nothing to do.
            let _ = ready_tx.send(Err(format!("loading a transform script: {err}")));
            return;
        }
    };
    let _ = ready_tx.send(Ok(()));
    worker.set_memory_cap(max_memory);

    // Built here because the registry can't attach one to a `ScriptWorker` it never constructs.
    // Cloned so the panic report below has one after the loop's copy moves into the closure.
    let diag = Diagnostics::new(id.clone()).with_telemetry(telemetry.clone());
    let reporter = diag.clone();

    // The same `Symbol` `Fanout::with_component` interned for this node's edge.
    let me = logit_core::interner::intern(&id);

    let loop_io = io.clone();
    let heartbeat_for_report = heartbeat.clone();
    let (sweep_telemetry, sweep_runtime) = (telemetry.clone(), runtime.clone());
    let verdict = max_memory.map(|cap| MemoryVerdict::new(cap, verdict_spacing));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        run_lua_loop(
            worker,
            me,
            configured_interval,
            loop_io,
            heartbeat,
            verdict,
            telemetry,
            runtime,
            diag,
        )
    }));
    // A panic unwinds out of a script call with the busy bit still set; left set, a watcher tick
    // after shutdown could report a wedge before it reads the panic from `done_rx`.
    heartbeat_for_report.leave();
    // Dropped here, after a return or a panic alike, so the downstream cascade is underway
    // before `done_tx` reports. `None` already if the watcher revoked it. A loop that failed the
    // node left its inbox open with batches still queued, so those are swept and counted, as
    // the watcher's revocation counts them. Counted `reason="shutdown"` whether or not a signal
    // came first: the node is leaving the graph, and its failure starts the drain if none has.
    let leftover = lock_io(&io).take();
    let failed = matches!(outcome, Ok(Err(_)));
    match leftover {
        Some(io) if failed => {
            sweep_runtime.block_on(revoke_lua_io(io, &sweep_telemetry, &shutdown_dropped))
        }
        leftover => drop(leftover),
    }
    let panicked = outcome.is_err();
    let report = thread_outcome(outcome);
    if let (true, Err(message)) = (panicked, &report) {
        reporter.error("thread_panicked", format_args!("{message}"));
    }
    // The receiver is gone only if `run` already returned for an unrelated reason; nothing to do.
    let _ = done_tx.send(report);
}

/// A Lua thread's post-ready outcome as a message: a loop's own failure (`max_memory`) as it
/// returned it, a panic prefixed `thread panicked:`. A panic payload is a `&str` for a literal
/// `panic!`, a `String` for a formatted one, and anything for `panic_any`, hence the fallback.
fn thread_outcome(result: std::thread::Result<Result<(), String>>) -> Result<(), String> {
    match result {
        Ok(returned) => returned,
        Err(payload) => {
            let message = if let Some(s) = payload.downcast_ref::<&str>() {
                (*s).to_string()
            } else if let Some(s) = payload.downcast_ref::<String>() {
                s.clone()
            } else {
                "non-string panic payload".to_string()
            };
            Err(format!("thread panicked: {message}"))
        }
    }
}

/// A Lua node's channels: its inbox, its own edge, and its target edges.
///
/// Held in a [`SharedLuaIo`] the thread and its watcher share, so the watcher can revoke them
/// from a thread wedged inside a script call: dropping the `LuaIo` closes every downstream inbox
/// (each drains on its own grace, flushing its own window) and fails every upstream send as
/// `closed_consumer`, with nothing aborted (`docs/adr/lua-runaway-script-bounds.md`).
///
/// The thread locks it only around a receive and around a send, never while its heartbeat is
/// busy, so the watcher's `try_lock` on a busy node always succeeds unless the thread is between
/// `leave()` and the lock, and a revoked node finds `None` the next time it looks and exits.
struct LuaIo {
    inbox: mpsc::Receiver<Delivered>,
    fanout: Fanout,
    target_fanouts: Vec<Fanout>,
}

type SharedLuaIo = Arc<std::sync::Mutex<Option<LuaIo>>>;

/// Locks `io`, recovering from poison: the guarded value is an `Option` of channels, left
/// consistent whatever a panicking holder was doing.
fn lock_io(io: &SharedLuaIo) -> std::sync::MutexGuard<'_, Option<LuaIo>> {
    io.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// How long [`revoke_lua_io`] waits for upstream permit holders to finish sending into a revoked
/// inbox before it stops counting.
const REVOKE_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

/// Drops a wedged node's channels, counting what its inbox still held as
/// `batches.dropped`/`events.dropped{reason="shutdown"}` under the node's own id through
/// [`count_shutdown_drop`], as `run_output`
/// counts an abandoned sink inbox. Dropping a `Receiver` destroys its buffered batches, and no
/// `Fanout` counts them: the upstream sends already succeeded.
///
/// The outbound edges go first, so the downstream cascade starts at once. `close` fails every
/// later send (counted `closed_consumer` upstream) but not one whose `Permit` was reserved
/// before it, which `Fanout::send_with_deadline` can hold across an await; so the sweep receives
/// until `recv` returns `None`, which it does once the buffer is empty and every permit is
/// released. A permit still unreleased after [`REVOKE_DRAIN_TIMEOUT`] (its holder blocked on
/// another consumer) stops the wait, and a batch it sends later is destroyed uncounted.
async fn revoke_lua_io(io: LuaIo, telemetry: &Telemetry, shutdown_dropped: &AtomicU64) {
    let LuaIo { mut inbox, fanout, target_fanouts } = io;
    drop((fanout, target_fanouts));
    inbox.close();
    let mut batches: u64 = 0;
    let mut events: u64 = 0;
    let _ = tokio::time::timeout(REVOKE_DRAIN_TIMEOUT, async {
        while let Some(delivered) = inbox.recv().await {
            batches += 1;
            events += unwrap_batch_arc(delivered).events.len() as u64;
        }
    })
    .await;
    count_shutdown_drop(telemetry, shutdown_dropped, batches, events);
}

/// A Lua node's `JoinSet` entry: waits on the thread's `done` report (see `run_lua`) and watches
/// its [`Heartbeat`] (`docs/adr/lua-runaway-script-bounds.md`).
///
/// - Busy and unchanged for `stall_after`: the node reads [`NodeState::Stalled`] and
///   `script_stalled` is logged once; the next change sets `Running` and logs `script_resumed`.
/// - After shutdown, busy and unchanged for `shutdown_grace` measured from the later of the
///   signal and the last change: the node is wedged. A node already `Stalled` is measured from
///   its last change alone (with the defaults, the first tick after the signal). The watcher
///   drops its [`LuaIo`] and returns `Err`, and the join loop fails the run as it would for any
///   node. The thread is left running; `main`'s exit reclaims it.
///
/// A loop that keeps constructing events with `Event.new` advances the heartbeat and is never
/// stalled or wedged; a loop of refused calls is not progress. Telling a constructing loop from
/// a large `flush()` would take a time limit, which `docs/adr/lua-runaway-script-bounds.md`
/// declines.
///
/// A node that isn't busy is never stalled or wedged: a thread parked sending into a full inbox is
/// backpressure, and the sink's own grace unparks it.
///
/// `shutdown` only arms the wedge rule; it never resolves this future, because shutdown reaches
/// the thread through its inbox closing, and resolving on it would end the node before its
/// close-time flush. The `Err(_)` arm on `done_rx` is defensive; `run_lua` always sends after
/// `catch_unwind`.
#[allow(clippy::too_many_arguments)]
async fn watch_lua_thread(
    id: String,
    mut done_rx: oneshot::Receiver<Result<(), String>>,
    heartbeat: Arc<Heartbeat>,
    io: SharedLuaIo,
    config: LuaRuntimeConfig,
    mut shutdown: watch::Receiver<bool>,
    readiness: Readiness,
    telemetry: Telemetry,
    mut diag: Diagnostics,
    shutdown_dropped: Arc<AtomicU64>,
) -> anyhow::Result<()> {
    // A quarter of the shorter threshold, so either verdict lands within 25% of its own bound.
    let period = (config.stall_after.min(config.shutdown_grace) / 4).max(Duration::from_millis(10));
    let mut ticker = tokio::time::interval(period);
    // A late tick is observed late, not replayed in a burst that would read one pause as several.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut last_value = heartbeat.read();
    let mut last_change = tokio::time::Instant::now();
    let mut stalled = false;
    let mut shutdown_at = shutdown.borrow().then(tokio::time::Instant::now);
    // A closed `watch` (its sender dropped) can't signal again; stop polling it.
    let mut shutdown_open = true;

    loop {
        tokio::select! {
            // `done_rx` first: see `docs/design/pipeline-graph.md`'s "Cancellation points".
            biased;
            outcome = &mut done_rx => {
                return match outcome {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(message)) => Err(anyhow::anyhow!("component '{id}': {message}")),
                    Err(_) => {
                        Err(anyhow::anyhow!("component '{id}': thread exited without reporting"))
                    }
                };
            }
            changed = shutdown.changed(), if shutdown_at.is_none() && shutdown_open => {
                match changed {
                    Ok(()) if *shutdown.borrow_and_update() => {
                        shutdown_at = Some(tokio::time::Instant::now());
                    }
                    Ok(()) => {}
                    Err(_) => shutdown_open = false,
                }
            }
            _ = ticker.tick() => {
                let now = tokio::time::Instant::now();
                let value = heartbeat.read();
                if value != last_value {
                    last_value = value;
                    last_change = now;
                    if stalled {
                        stalled = false;
                        readiness.set_node(&id, NodeState::Running);
                        diag.info("script_resumed", "the script is making progress again");
                    }
                    continue;
                }
                if !Heartbeat::is_busy(value) {
                    continue;
                }
                let quiet = now.duration_since(last_change);
                if !stalled && quiet >= config.stall_after {
                    stalled = true;
                    readiness.set_node(&id, NodeState::Stalled);
                    diag.warn_throttled(
                        "script_stalled",
                        format_args!(
                            "inside process()/flush() with no progress for {quiet:?}; \
                             /readyz reports stalled until it resumes"
                        ),
                    );
                }
                let Some(signalled) = shutdown_at else {
                    continue;
                };
                // A stalled node's quiet time counts from its last progress, before the signal
                // included; anything else gets the full grace from the signal or its last
                // progress, whichever is later.
                let since = if stalled { last_change } else { signalled.max(last_change) };
                if now.duration_since(since) < config.shutdown_grace {
                    continue;
                }
                // Not blocking: the thread holds the lock only while not busy, so a failed
                // `try_lock` means it left the call since `read()` above; look again next tick.
                // Scoped so the guard is gone before the sweep's `.await`: a `std` guard held
                // across it would make this future `!Send`.
                let revoked = {
                    let mut guard = match io.try_lock() {
                        Ok(guard) => guard,
                        Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
                        Err(std::sync::TryLockError::WouldBlock) => continue,
                    };
                    // The thread can leave the call between `read()` and the lock; that is
                    // progress.
                    if heartbeat.read() != last_value {
                        continue;
                    }
                    guard.take()
                };
                if let Some(io) = revoked {
                    revoke_lua_io(io, &telemetry, &shutdown_dropped).await;
                }
                let elapsed = now.duration_since(signalled);
                return Err(anyhow::anyhow!(
                    "component '{id}': still inside process()/flush() {elapsed:?} after shutdown \
                     began; exiting without it"
                ));
            }
        }
    }
}

/// The most full collections one `max_memory` verdict runs
/// (`ScriptWorker::collect_until_under`).
const MAX_VERDICT_PASSES: usize = 8;

/// The default [`LuaRuntimeConfig::min_verdict_spacing`].
const MIN_VERDICT_SPACING: Duration = Duration::from_secs(1);

/// A Lua node's `max_memory` check, run after each batch's send and each `flush()`'s
/// (`docs/adr/lua-runaway-script-bounds.md`, decision 3).
///
/// A VM over the cap runs full collections until it is under or a pass stops freeing much, and
/// fails the node if it is still over. The collection runs with the heartbeat idle, after the
/// send, so the batch that crossed the cap has already gone downstream.
///
/// Forced verdicts are rate-limited to one per [`LuaRuntimeConfig::min_verdict_spacing`] or ten
/// times the last
/// one's duration, whichever is longer; an over-cap reading between them is skipped. A cap under
/// about twice the script's working set would otherwise force a full collection on nearly every
/// batch, since the incremental collector lets garbage reach that much before a cycle ends. A
/// skipped reading defers the verdict to the end of the window ([`MemoryVerdict::deferred_until`]),
/// which [`run_lua_loop`] wakes for even with no batch arriving, so a node left over the cap by
/// its last batch still fails within the window. When the inbox closes first, the pending verdict
/// runs then, rate limit or not ([`MemoryVerdict::check_at_close`]): a deferral never outlives the
/// node.
struct MemoryVerdict {
    cap: usize,
    spacing: Duration,
    next_allowed: Option<std::time::Instant>,
    /// An over-cap reading was skipped and no verdict has run since.
    deferred: bool,
}

impl MemoryVerdict {
    fn new(cap: usize, spacing: Duration) -> Self {
        Self { cap, spacing, next_allowed: None, deferred: false }
    }

    /// When a skipped verdict is due, if one is pending.
    fn deferred_until(&self) -> Option<std::time::Instant> {
        self.next_allowed.filter(|_| self.deferred)
    }

    /// `Err` with the node's failure message, already logged as `memory_limit_exceeded`, when
    /// the VM stays over the cap; a failed collection fails the node with its own message.
    fn check(
        &mut self,
        worker: &ScriptWorker,
        telemetry: &Telemetry,
        diag: &Diagnostics,
    ) -> Result<(), String> {
        self.run(worker, telemetry, diag, false)
    }

    /// The inbox has closed: a deferred verdict runs now, rate limit or not, since no later wake
    /// is coming. Without one pending, the last `check` already decided.
    fn check_at_close(
        &mut self,
        worker: &ScriptWorker,
        telemetry: &Telemetry,
        diag: &Diagnostics,
    ) -> Result<(), String> {
        match self.deferred {
            true => self.run(worker, telemetry, diag, true),
            false => Ok(()),
        }
    }

    fn run(
        &mut self,
        worker: &ScriptWorker,
        telemetry: &Telemetry,
        diag: &Diagnostics,
        bypass_limit: bool,
    ) -> Result<(), String> {
        if worker.used_memory() <= self.cap {
            self.deferred = false;
            return Ok(());
        }
        let started = std::time::Instant::now();
        if !bypass_limit && self.next_allowed.is_some_and(|next| started < next) {
            self.deferred = true;
            return Ok(());
        }
        self.deferred = false;
        telemetry.count("logit.script.vm.gc.forced", 1.0, &[]);
        let verdict = worker
            .collect_until_under(self.cap, MAX_VERDICT_PASSES)
            .map_err(|err| format!("a full garbage collection of the Lua VM failed: {err}"))?;
        let took = started.elapsed();
        telemetry.timing("logit.script.vm.gc.duration", took, &[]);
        telemetry.gauge("logit.script.vm.memory", verdict.used as f64, &[]);
        self.next_allowed = Some(started + self.spacing.max(took * 10));
        if verdict.used <= self.cap {
            return Ok(());
        }
        let message = format!(
            "Lua VM holds {} bytes after {} full collections, over max_memory {}",
            verdict.used, verdict.passes, self.cap
        );
        diag.error("memory_limit_exceeded", &message);
        Err(message)
    }
}

/// What one `flush_now` in [`run_lua_loop`] left the loop to do.
enum FlushOutcome {
    Continue,
    /// The watcher revoked `io`; the loop exits cleanly.
    Revoked,
    /// The node failed (see [`MemoryVerdict`]); the loop returns this message.
    Failed(String),
}

/// The loop half of [`run_lua`]. Takes everything by value so `catch_unwind` has nothing borrowed.
/// Returns once `inbox` closes, after a last `flush()` if the component has an interval, or once
/// the watcher has revoked `io` (see [`LuaIo`]). Returns `Err` only when `verdict` fails the node.
///
/// The heartbeat is busy only inside a script call: `enter()` before each `process()` and around
/// `flush()`, `leave()` before anything is sent. A thread parked in a send is therefore never
/// read as stalled, and never holds `io`'s lock while busy.
#[allow(clippy::too_many_arguments)]
fn run_lua_loop(
    worker: ScriptWorker,
    me: logit_core::Symbol,
    configured_interval: Option<Duration>,
    io: SharedLuaIo,
    heartbeat: Arc<Heartbeat>,
    mut verdict: Option<MemoryVerdict>,
    telemetry: Telemetry,
    runtime: tokio::runtime::Handle,
    mut diag: Diagnostics,
) -> Result<(), String> {
    let mut next_flush = configured_interval.map(|interval| tokio::time::Instant::now() + interval);
    // A `flush()`'s root (see `run_lua`): one empty resource shared by every tick (an `Arc`
    // clone, not an allocation), and this node's id as both halves of the provenance. That's
    // the value `Fanout::stamp` would fill in on this node's edge, so the script reads what the
    // batch will carry; `stamp` is idempotent over it. An event marked for a `target` gets
    // `previous` rewritten to the target's id there, and keeps this `origin`.
    let root_resource = Arc::new(Resource::default());
    let flush_provenance = logit_core::Provenance { origin: Some(me), previous: Some(me) };

    // The same reused per-destination buffers a native `Router` uses (`RouterScratch`), shared
    // by the batch path and `flush_now`. Only `dests` is used: a script returns its verdict with
    // each event, so there's no route-then-count pass as in `route_batch`. Sized once from the
    // target count, which revocation never changes.
    let mut scratch =
        RouterScratch::new(lock_io(&io).as_ref().map_or(0, |io| io.target_fanouts.len()));

    // Mints its own root and records the `flush` span, as `run_flush` does: a Lua `flush()` has
    // no single parent batch (`docs/adr/lua-flush-root-context.md`). The script's batch-scoped
    // globals are reset to that root first, so `flush()` reads what its emission goes out as.
    // The `max_memory` verdict runs after the send.
    //
    // `diag`, `scratch`, and `verdict` are parameters, not captures: the loop body also borrows
    // them mutably, and a capture would hold the borrow for the closure's lifetime.
    let flush_now = |diag: &mut Diagnostics,
                     worker: &ScriptWorker,
                     scratch: &mut RouterScratch,
                     verdict: &mut Option<MemoryVerdict>| {
        let ctx = BatchContext { trace: TraceContext::new_root(), provenance: flush_provenance };
        let mut span = telemetry.span(
            "flush",
            SpanKind::Internal,
            ctx.trace.trace_id,
            ctx.trace.span_id,
            None,
        );
        // The same four setters as the batch path below, fed the root.
        if let Err(err) = worker.set_trace_context(ctx.trace.trace_id, ctx.trace.span_id) {
            diag.warn_throttled(
                "trace_context_error",
                format_args!("setting trace context failed: {err}"),
            );
        }
        worker.set_provenance(ctx.provenance);
        worker.set_resource(&root_resource);
        worker.set_scope(&None);

        let timer = telemetry.timer("logit.component.flush.duration");
        // The same `now_unix_nanos()` `run_flush` hands `Transform::flush`, so a flush-driven
        // `Event.new{timestamp = now, ..}` is stamped like an aggregate window
        // (`docs/adr/lua-event-constructor.md`).
        heartbeat.enter();
        let result = worker.flush(now_unix_nanos());
        heartbeat.leave();
        worker.reset_memory_trip();
        drop(timer);
        worker.expire_registry_values();
        // A script that wrote `resource` in `flush()` gives the emission that identity; `None`
        // keeps the empty root (an `Arc` clone, no allocation).
        let resource = worker.take_resource().unwrap_or_else(|| root_resource.clone());
        // `Some` only if written, so an untouched `scope` stays the root's `None`.
        let scope = worker.take_scope();
        // Sampled on flush too, whatever the outcome: a script that grows only in `flush()`
        // while the input is idle would otherwise leave this gauge silent when it matters.
        telemetry.gauge("logit.script.vm.memory", worker.used_memory() as f64, &[]);
        match result {
            Ok(events) if !events.is_empty() => {
                telemetry.count("logit.component.flush.events", events.len() as f64, &[]);
                // A flushed event honors its mark like any other (`docs/adr/target-components.md`).
                // It's typically an `event:clone()` stashed in `process()`, and `clone` copies the
                // mark and the target table, so `e:to("x")` resolves from `flush()`.
                for (event, mark) in events {
                    let slot = lua_slot_of(mark, scratch.dests.len());
                    scratch.dests[slot].push(event);
                }
                let guard = lock_io(&io);
                let Some(io) = guard.as_ref() else {
                    drop(guard);
                    count_revoked(&mut scratch.dests, &telemetry);
                    return FlushOutcome::Revoked;
                };
                let sent = send_lua_partitions(
                    &mut scratch.dests,
                    &io.fanout,
                    &io.target_fanouts,
                    &resource,
                    &scope,
                    ctx,
                    &telemetry,
                );
                span.events(sent);
            }
            Ok(_) => {}
            Err(err) => {
                telemetry.count("logit.component.errors", 1.0, &[("reason", "flush")]);
                diag.error("flush_error", format_args!("script flush error: {err}"));
                span.error();
            }
        }
        match verdict.as_mut().map(|v| v.check(worker, &telemetry, diag)) {
            Some(Err(message)) => FlushOutcome::Failed(message),
            _ => FlushOutcome::Continue,
        }
    };

    loop {
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                match flush_now(&mut diag, &worker, &mut scratch, &mut verdict) {
                    FlushOutcome::Continue => {}
                    FlushOutcome::Revoked => return Ok(()),
                    FlushOutcome::Failed(message) => return Err(message),
                }
                let interval = configured_interval
                    .expect("next_flush is only ever Some for a component with an interval");
                next_flush = Some(advance_flush_deadline(deadline, now_instant, interval));
            }
        }
        // A verdict the rate limit skipped, now due (see `MemoryVerdict`).
        if let Some(verdict) = verdict.as_mut() {
            if verdict.deferred_until().is_some_and(|due| due <= std::time::Instant::now()) {
                verdict.check(&worker, &telemetry, &diag)?;
            }
        }

        // Wakes for whichever of the next flush and a deferred verdict comes first.
        let flush_wait =
            next_flush.map(|due| due.saturating_duration_since(tokio::time::Instant::now()));
        let verdict_wait = verdict
            .as_ref()
            .and_then(MemoryVerdict::deferred_until)
            .map(|due| due.saturating_duration_since(std::time::Instant::now()));
        let wait = match (flush_wait, verdict_wait) {
            (Some(flush), Some(verdict)) => Some(flush.min(verdict)),
            (flush, verdict) => flush.or(verdict),
        };

        // The lock is held across the wait. The heartbeat is idle here, and the watcher only
        // takes the lock from a busy node, so the two never contend.
        let batch = {
            let mut guard = lock_io(&io);
            let Some(io) = guard.as_mut() else {
                return Ok(());
            };
            match wait {
                None => {
                    let batch = io.inbox.blocking_recv();
                    if batch.is_some() {
                        sample_inbox_depth(&io.inbox, &telemetry);
                    }
                    batch
                }
                Some(wait) => {
                    // The `async` block is required: `tokio::time::timeout` builds its `Sleep`
                    // eagerly, which panics outside a runtime context. Inside the block it's
                    // built only once `block_on` has entered one.
                    match runtime
                        .block_on(async { tokio::time::timeout(wait, io.inbox.recv()).await })
                    {
                        Ok(batch) => {
                            if batch.is_some() {
                                sample_inbox_depth(&io.inbox, &telemetry);
                            }
                            batch
                        }
                        Err(_elapsed) => continue,
                    }
                }
            }
        };
        let Some(batch) = batch else {
            if next_flush.is_some() {
                match flush_now(&mut diag, &worker, &mut scratch, &mut verdict) {
                    FlushOutcome::Continue => {}
                    FlushOutcome::Revoked => return Ok(()),
                    FlushOutcome::Failed(message) => return Err(message),
                }
            }
            if let Some(verdict) = verdict.as_mut() {
                verdict.check_at_close(&worker, &telemetry, &diag)?;
            }
            return Ok(());
        };
        // As in `run_transform`: this batch is the unambiguous parent of everything emitted
        // below, provenance passes through, and the context is minted once here so the span's
        // `span_id` matches the outgoing `Delivered`'s.
        let parent = batch.batch_context();
        let ctx = BatchContext { trace: parent.trace.child(), provenance: parent.provenance };
        let mut span = telemetry.span(
            "process",
            SpanKind::Internal,
            ctx.trace.trace_id,
            ctx.trace.span_id,
            Some(parent.trace.span_id),
        );
        // Exposes `trace` to `process()`. Effectively infallible (the registry-held table is
        // independent of the script's `trace` global), so an error is logged, not fatal.
        if let Err(err) = worker.set_trace_context(parent.trace.trace_id, parent.trace.span_id) {
            diag.warn_throttled(
                "trace_context_error",
                format_args!("setting trace context failed: {err}"),
            );
        }
        worker.set_provenance(parent.provenance);
        let batch = unwrap_batch(batch);
        // Set on every batch, even one whose events all error, so a previous batch's write to
        // `resource` or `scope` never leaks into this one.
        worker.set_resource(&batch.resource);
        worker.set_scope(&batch.scope);
        telemetry.count("logit.component.batches.received", 1.0, &[]);
        telemetry.count("logit.component.events.received", batch.events.len() as f64, &[]);

        let process_timer = telemetry.timer("logit.component.process.duration");
        // One allocation: the previous batch's `std::mem::take` left slot 0 at capacity 0, and
        // slot 0 (this node's own edge) gets every event when there are no `targets:`, and most
        // events otherwise. A target slot grows normally: a script returns its verdict with the
        // event, so there's no count to reserve from, unlike `route_batch`.
        scratch.dests[0].reserve_exact(batch.events.len());
        let mut dropped: u64 = 0;
        let mut errors: u64 = 0;
        let slots = scratch.dests.len();
        for event in batch.events {
            heartbeat.enter();
            let outcome = worker.process(event);
            worker.reset_memory_trip();
            match outcome {
                Ok(ProcessOutcome::Emit(e, mark)) => {
                    scratch.dests[lua_slot_of(mark, slots)].push(*e);
                    telemetry.count("logit.script.events.emitted", 1.0, &[("outcome", "emit")]);
                }
                Ok(ProcessOutcome::EmitMany(es)) => {
                    telemetry.count(
                        "logit.script.events.emitted",
                        es.len() as f64,
                        &[("outcome", "emit_many")],
                    );
                    // Each event in a `return {a, b}` carries its own mark.
                    for (event, mark) in es {
                        scratch.dests[lua_slot_of(mark, slots)].push(event);
                    }
                }
                Ok(ProcessOutcome::Drop) => dropped += 1,
                Err(err) => {
                    errors += 1;
                    diag.warn_throttled("script_error", format_args!("script error: {err}"));
                }
            }
        }
        heartbeat.leave();
        drop(process_timer);
        worker.expire_registry_values();
        // Once per batch: how a script leaking VM-side state becomes visible
        // (`docs/design/internal-telemetry.md`).
        telemetry.gauge("logit.script.vm.memory", worker.used_memory() as f64, &[]);
        if dropped > 0 {
            telemetry.count(
                "logit.component.events.dropped",
                dropped as f64,
                &[("reason", "script_drop")],
            );
        }
        if errors > 0 {
            telemetry.count("logit.component.errors", errors as f64, &[("reason", "process")]);
            // Any script error, even mixed with successful emits, marks the span failed at the
            // call site whose error path fired, as `write_loop` does. Without it, a batch whose
            // every event errored would record a successful zero-event span.
            span.error();
        }
        // A `resource` write from any `process()` call in this batch; `None` moves the incoming
        // `Arc` through with no clone, like `map_resource` in `process_batch`.
        let resource = worker.take_resource().unwrap_or(batch.resource);
        // Unwritten falls back to the batch's own scope, which may itself be `None`.
        let scope = worker.take_scope().or_else(|| batch.scope.clone());
        // One send per non-empty destination, all under the one `ctx`, as in `run_router`.
        let guard = lock_io(&io);
        let Some(io) = guard.as_ref() else {
            drop(guard);
            count_revoked(&mut scratch.dests, &telemetry);
            return Ok(());
        };
        let sent = send_lua_partitions(
            &mut scratch.dests,
            &io.fanout,
            &io.target_fanouts,
            &resource,
            &scope,
            ctx,
            &telemetry,
        );
        drop(guard);
        if sent > 0 {
            span.events(sent);
        }
        if let Some(verdict) = verdict.as_mut() {
            verdict.check(&worker, &telemetry, &diag)?;
        }
    }
}

/// Counts what a revoked node produced but can no longer send as
/// `events.dropped{reason="closed_consumer"}`: its consumers are gone, as they would be for a
/// `Fanout` sending into closed inboxes.
fn count_revoked(dests: &mut [Vec<Event>], telemetry: &Telemetry) {
    let lost: usize = dests.iter_mut().map(|d| std::mem::take(d).len()).sum();
    if lost > 0 {
        telemetry.count(
            "logit.component.events.dropped",
            lost as f64,
            &[("reason", "closed_consumer")],
        );
    }
}

/// An `event:to(..)` mark -> index into `run_lua`'s per-destination buffers, numbered as
/// [`slot_of`] numbers a [`Destination`]: 0 is the node's own edge, `n + 1` is target slot `n`.
///
/// An out-of-range mark can't happen: `event:to(id)` resolves against the same `targets:` list
/// that sized the buffers, and an unknown id is a script error (`TargetTable` in
/// `crates/logit-script/src/proxy.rs`). Debug builds assert; release treats it as unrouted, as
/// `route_batch` does.
fn lua_slot_of(mark: Option<u16>, slots: usize) -> usize {
    let Some(target) = mark else {
        return 0;
    };
    let slot = usize::from(target) + 1;
    debug_assert!(
        slot < slots,
        "a script marked an event for target slot {target} with only {} target(s) configured",
        slots - 1
    );
    match slot < slots {
        true => slot,
        false => 0,
    }
}

/// Sends a Lua node's partition: one `EventBatch` per non-empty buffer, all under the caller's one
/// [`BatchContext`], each buffer taken with `std::mem::take` (see [`RouterScratch`]). Returns the
/// number of events handed over, for the caller's span. Used by both `run_lua` paths.
///
/// Counts an unconsumed forward partition as `events.dropped{reason="unrouted"}`, as
/// `run_router` does, since `Fanout::deliver` counts nothing on zero consumers.
fn send_lua_partitions(
    dests: &mut [Vec<Event>],
    fanout: &Fanout,
    target_fanouts: &[Fanout],
    resource: &Arc<Resource>,
    scope: &Option<Arc<Scope>>,
    ctx: BatchContext,
    telemetry: &Telemetry,
) -> u64 {
    let mut sent = 0u64;
    for (slot, dest) in dests.iter_mut().enumerate() {
        if dest.is_empty() {
            continue;
        }
        sent += dest.len() as u64;
        let events = std::mem::take(dest);
        // Slot 0 is this component's own outbound edge; slot n + 1 is target slot n.
        let destination = match slot {
            0 => fanout,
            n => match target_fanouts.get(n - 1) {
                Some(fanout) => fanout,
                // Unreachable: `lua_slot_of` never produces a slot these buffers -- sized from
                // `target_fanouts` itself -- weren't built for.
                None => continue,
            },
        };
        if slot == 0 && destination.is_empty() {
            telemetry.count(
                "logit.component.events.dropped",
                events.len() as f64,
                &[("reason", "unrouted")],
            );
            continue;
        }
        destination.send_blocking_with_own_context(
            EventBatch { resource: resource.clone(), scope: scope.clone(), events },
            ctx,
        );
    }
    sent
}

/// Turns the channel payload back into an owned `EventBatch` for `Transform::process` or
/// `ScriptWorker::process`, which mutate or consume owned events. `run_output` never calls this:
/// it keeps the batch as an `Arc` (`unwrap_batch_arc`) and `Output::send` borrows it
/// (`docs/adr/arc-eventbatch-copy-on-write.md`).
///
/// `Delivered::Owned` (a single-consumer edge) is free. `Delivered::Shared` (a fan-out) uses
/// `Arc::try_unwrap`, which avoids a clone only when this is the last strong reference: a
/// best-effort saving, not a guarantee that one branch pays nothing, since two branches unwrapping
/// concurrently can both see a count above 1 and both clone. An `Output` sibling doesn't make it
/// deterministic: `drain_inbox` moves the `Arc` into the sink's store, where a `Memory` store
/// holds it until `write_loop` commits the batch after delivery (a `Disk` store drops it once the
/// record is written), so the unwrap here is free only if that sink got there first.
/// `crates/logit-bench/tests/allocations.rs` pins both outcomes. Either way a branch's
/// copy is independent before it can be mutated
/// (`a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch`).
///
/// `pub` for `crates/logit-bench/tests/allocations.rs`.
///
/// Discards the `BatchContext`; call [`Delivered::batch_context`] first when it's needed as a
/// parent.
pub fn unwrap_batch(batch: Delivered) -> EventBatch {
    match batch {
        Delivered::Owned(batch, _ctx) => batch,
        Delivered::Shared(shared, _ctx) => {
            Arc::try_unwrap(shared).unwrap_or_else(|shared| (*shared).clone())
        }
    }
}

fn now_unix_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as i64
}

/// Returns the first point on `deadline`'s interval cadence strictly after `now`. Computing the
/// remainder makes this constant-time even when a very small interval has missed billions of
/// ticks. If the platform cannot represent the cadence's next instant, fall back to the smallest
/// representable useful delay rather than overflowing or leaving the deadline due forever.
///
/// [`crate::accumulator::BatchAccumulator`] reuses it for its own interval flush.
pub(crate) fn advance_flush_deadline(
    deadline: tokio::time::Instant,
    now: tokio::time::Instant,
    interval: Duration,
) -> tokio::time::Instant {
    debug_assert!(deadline <= now);
    debug_assert!(!interval.is_zero());

    let remainder_nanos = now.duration_since(deadline).as_nanos() % interval.as_nanos();
    let remainder = Duration::new(
        (remainder_nanos / 1_000_000_000) as u64,
        (remainder_nanos % 1_000_000_000) as u32,
    );
    let until_next = if remainder.is_zero() { interval } else { interval - remainder };
    now.checked_add(until_next).or_else(|| now.checked_add(Duration::from_nanos(1))).unwrap_or(now)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fanout::TraceContext;
    use crate::graph;
    use crate::queue::OverflowPolicy;
    use crate::readiness::Phase;
    use crate::test_util::{wait_until, TelemetryProbe, Totals, RECV_TIMEOUT};
    use logit_config::{Component, ComponentKind, Config};
    use logit_core::{AttrMap, Event, MetricKind, Provenance, Registry, SpanLink, SpanStatus};
    use std::collections::HashMap as Map;

    #[test]
    fn advancing_a_missed_flush_deadline_is_constant_time_and_preserves_cadence() {
        let deadline = tokio::time::Instant::from_std(std::time::Instant::now());
        let interval = Duration::from_nanos(7);
        let now = deadline + Duration::from_secs(2) + Duration::from_nanos(5);

        let next = advance_flush_deadline(deadline, now, interval);

        assert!(next > now);
        assert_eq!(next.duration_since(deadline).as_nanos() % interval.as_nanos(), 0);

        let nanosecond_interval = Duration::from_nanos(1);
        let next = advance_flush_deadline(
            deadline,
            deadline + Duration::from_secs(2),
            nanosecond_interval,
        );
        assert_eq!(next, deadline + Duration::from_secs(2) + nanosecond_interval);
    }

    struct RecordingOutput {
        tx: std::sync::mpsc::Sender<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Output for RecordingOutput {
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            // Cloned so the test can inspect it after `send` returns.
            let _ = self.tx.send(batch.clone());
            Ok(())
        }
    }

    struct OneShotInput {
        batch: Option<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Input for OneShotInput {
        async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
            if let Some(batch) = self.batch.take() {
                sink.send(batch).await;
            }
            // Idle like a real listener, keeping the graph alive.
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    fn counter_event(name: &str, value: f64) -> Event {
        use logit_core::{interner::intern, AttrMap, MetricKind, MetricRecord};
        Event::metric(
            0,
            AttrMap::new(),
            MetricRecord::new(intern(name), MetricKind::counter(value)),
        )
    }

    #[tokio::test]
    async fn a_lua_node_processes_events_end_to_end_through_the_graph() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "enrich".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Lua {
                    script: r#"function process(event) event.attributes.tagged = "yes" return event end"#
                        .to_string(),
                    interval: None,
                    max_memory: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["enrich".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::InfluxDbOut {
                    url: "http://localhost:8086".to_string(),
                    org: "org".to_string(),
                    bucket: "bucket".to_string(),
                    token: "TOKEN".to_string(),
                },
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(OneShotInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "enrich".to_string(),
            NodeSpec::Lua {
                script:
                    r#"function process(event) event.attributes.tagged = "yes" return event end"#
                        .to_string(),
                interval: None,
                runtime: LuaRuntimeConfig::default(),
            },
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        tokio::spawn(run(g, specs));

        let received = tokio::task::spawn_blocking(move || {
            result_rx.recv_timeout(Duration::from_secs(5)).expect("should receive a batch")
        })
        .await
        .expect("blocking task should not panic");

        assert_eq!(received.events.len(), 1);
        assert_eq!(
            received.events[0].attributes.get("tagged").and_then(|v| v.as_str()),
            Some("yes")
        );
    }

    /// Sends one batch, then returns (unlike `OneShotInput`, which idles).
    struct FiniteInput {
        batch: Option<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Input for FiniteInput {
        async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
            if let Some(batch) = self.batch.take() {
                sink.send(batch).await;
            }
            Ok(())
        }
    }

    /// `run` drops its scaffolding `edges`, so inboxes close and `run` returns after the input.
    #[tokio::test]
    async fn run_returns_once_the_only_input_finishes_instead_of_hanging() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::InfluxDbOut {
                    url: "http://localhost:8086".to_string(),
                    org: "org".to_string(),
                    bucket: "bucket".to_string(),
                    token: "TOKEN".to_string(),
                },
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let received = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the batch should have reached the output before shutdown");
        assert_eq!(received.events.len(), 1);
    }

    /// Appends an `extra` metric to every event.
    struct MutatingTransform;

    impl Transform for MutatingTransform {
        fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
            use logit_core::{interner::intern, MetricKind, MetricRecord};
            event.metrics.push(MetricRecord::new(intern("extra"), MetricKind::counter(1.0)));
            true
        }
    }

    /// Substitutes the resource on every batch, like `logit-transforms::Set`.
    struct ResourceMappingTransform {
        replacement: Arc<Resource>,
    }

    impl Transform for ResourceMappingTransform {
        fn process(&mut self, resource: &Arc<Resource>, _event: &mut Event) -> bool {
            // `process_batch` must pass the mapped resource, not the original.
            assert!(Arc::ptr_eq(resource, &self.replacement));
            true
        }

        fn map_resource(&mut self, _resource: &Arc<Resource>) -> Option<Arc<Resource>> {
            Some(self.replacement.clone())
        }
    }

    /// A `Some` from `map_resource` replaces the resource for `process` and the outgoing batch.
    #[test]
    fn process_batch_uses_map_resources_substituted_resource_for_the_outgoing_batch() {
        let mut attrs = AttrMap::new();
        attrs.insert("service.name", "mapped");
        let replacement = Arc::new(Resource { attributes: attrs, ..Default::default() });
        let mut transform = ResourceMappingTransform { replacement: replacement.clone() };

        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        };
        let telemetry = Registry::new().telemetry_for("x", "x", "x");

        let out = process_batch(&mut transform, batch, &telemetry).expect("one event should pass");
        assert!(
            Arc::ptr_eq(&out.resource, &replacement),
            "the outgoing batch must carry map_resource's substituted Arc"
        );
    }

    /// `map_resource`'s `None` default passes the incoming `Arc` through, no clone.
    #[test]
    fn process_batch_with_no_map_resource_override_passes_the_incoming_arc_through_unchanged() {
        let mut transform = MutatingTransform;
        let resource = Arc::new(Resource::default());
        let batch = EventBatch {
            resource: resource.clone(),
            scope: None,
            events: vec![Event::empty(0, AttrMap::new())],
        };
        let telemetry = Registry::new().telemetry_for("x", "x", "x");

        let out = process_batch(&mut transform, batch, &telemetry).expect("one event should pass");
        assert!(Arc::ptr_eq(&out.resource, &resource), "the default map_resource must be a no-op");
    }

    /// Branch isolation (`docs/adr/multi-payload-events.md`) through a real two-branch fan-out.
    #[tokio::test]
    async fn a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        // `Json` is an arity placeholder; the `MutatingTransform` spec is what runs.
        components.insert(
            "branch_a".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Json {
                    skip_to_brace: false,
                    invalid_utf8: Default::default(),
                },
            },
        );
        components.insert(
            "sink_a".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["branch_a".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        components.insert(
            "sink_b".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (tx_a, rx_a) = std::sync::mpsc::channel();
        let (tx_b, rx_b) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert("branch_a".to_string(), NodeSpec::Transform(Box::new(MutatingTransform)));
        specs.insert(
            "sink_a".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: tx_a }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );
        specs.insert(
            "sink_b".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: tx_b }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let received_a =
            rx_a.recv_timeout(Duration::from_secs(1)).expect("sink_a should receive a batch");
        let received_b =
            rx_b.recv_timeout(Duration::from_secs(1)).expect("sink_b should receive a batch");

        assert_eq!(
            received_a.events[0].metrics.len(),
            2,
            "branch_a's own mutation should be visible on its own branch"
        );
        assert_eq!(
            received_b.events[0].metrics.len(),
            1,
            "branch_a's mutation must not leak onto sink_b's independent copy of the same \
             upstream event"
        );
    }

    fn influxdb_out() -> ComponentKind {
        ComponentKind::InfluxDbOut {
            url: "http://localhost:8086".to_string(),
            org: "org".to_string(),
            bucket: "bucket".to_string(),
            token: "TOKEN".to_string(),
        }
    }

    /// Stands in for `Aggregator` (this crate can't depend on `logit-transforms`): absorbs every
    /// event and emits them only from `flush`.
    struct WindowingTransform {
        interval: Duration,
        buffered: Vec<Event>,
    }

    impl Transform for WindowingTransform {
        fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
            // `Event` has no `Default`, so `mem::replace`; `retain_mut` drops the husk.
            self.buffered.push(std::mem::replace(event, Event::empty(0, AttrMap::new())));
            false
        }

        fn flush_interval(&self) -> Option<Duration> {
            Some(self.interval)
        }

        fn flush(
            &mut self,
            _now: i64,
        ) -> Vec<(Arc<Resource>, Option<Arc<Scope>>, Vec<(Event, Vec<SpanLink>)>)> {
            if self.buffered.is_empty() {
                return Vec::new();
            }
            let events =
                std::mem::take(&mut self.buffered).into_iter().map(|e| (e, Vec::new())).collect();
            vec![(Arc::new(Resource::default()), None, events)]
        }
    }

    /// `OneShotInput` that signals `sent` once its batch is enqueued downstream.
    struct SignalingInput {
        batch: Option<EventBatch>,
        sent: Option<oneshot::Sender<()>>,
    }

    #[async_trait::async_trait]
    impl Input for SignalingInput {
        async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
            if let Some(batch) = self.batch.take() {
                sink.send(batch).await;
            }
            if let Some(tx) = self.sent.take() {
                let _ = tx.send(());
            }
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    /// Shutdown mid-window flushes the buffered window through to the sink before exit.
    #[tokio::test]
    async fn run_with_shutdown_flushes_an_in_flight_window_before_exiting() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "windowed".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Aggregate {
                    interval: Duration::from_secs(3600),
                    temporality: logit_config::AggregateTemporality::default(),
                    series_retention: 5,
                    max_retained_series: 10_000,
                    distributions: logit_config::Distributions::default(),
                    max_samples_per_series: 1000,
                    sets: logit_config::Sets::default(),
                    max_set_members_per_series: 1000,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["windowed".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::InfluxDbOut {
                    url: "http://localhost:8086".to_string(),
                    org: "org".to_string(),
                    bucket: "bucket".to_string(),
                    token: "TOKEN".to_string(),
                },
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        let (sent_tx, sent_rx) = oneshot::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(SignalingInput { batch: Some(batch), sent: Some(sent_tx) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "windowed".to_string(),
            NodeSpec::Transform(Box::new(WindowingTransform {
                interval: Duration::from_secs(3600),
                buffered: Vec::new(),
            })),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task = tokio::spawn(run_with_shutdown(g, specs, async move {
            let _ = shutdown_rx.await;
        }));

        sent_rx.await.expect("input should signal its batch was enqueued");
        shutdown_tx.send(()).expect("shutdown receiver should still be alive");

        tokio::time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("run_with_shutdown should return promptly once shutdown fires")
            .expect("task should not panic")
            .expect("run_with_shutdown should complete without error");

        let received = result_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the flushed window should have reached the output before exit");
        assert_eq!(received.events.len(), 1);
    }

    // -- `Input::run_until_shutdown` and `run_input`'s grace backstop
    //    (`docs/adr/decoupled-listener-io.md`). --

    /// Never returns on its own; only shutdown stops it.
    struct ForeverInput;

    #[async_trait::async_trait]
    impl Input for ForeverInput {
        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves")
        }
    }

    /// A cooperative listener: on shutdown, drains for `drain_for`, sends `batch`, and returns.
    struct DrainingInput {
        drain_for: Duration,
        batch: Option<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Input for DrainingInput {
        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves")
        }

        async fn run_until_shutdown(
            &mut self,
            sink: Fanout,
            mut shutdown: watch::Receiver<bool>,
        ) -> anyhow::Result<()> {
            let _ = shutdown.wait_for(|&due| due).await;
            tokio::time::sleep(self.drain_for).await;
            if let Some(batch) = self.batch.take() {
                sink.send(batch).await;
            }
            Ok(())
        }
    }

    /// Errors immediately.
    struct ErrInput;

    #[async_trait::async_trait]
    impl Input for ErrInput {
        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            anyhow::bail!("boom")
        }
    }

    /// The grace-delayed backstop adds no latency to an input using the default
    /// `run_until_shutdown`.
    #[tokio::test(start_paused = true)]
    async fn a_non_overriding_input_returns_at_the_instant_shutdown_fires_not_after_the_grace() {
        let (tx, _rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let huge_grace = Duration::from_secs(3600);

        let handle = tokio::spawn(run_input(
            "in".to_string(),
            Box::new(ForeverInput),
            fanout,
            shutdown_rx,
            huge_grace,
        ));

        // Let the spawned task start and park inside the default `run_until_shutdown`'s `select!`.
        tokio::task::yield_now().await;
        let before = tokio::time::Instant::now();
        shutdown_tx.send(true).expect("receiver should still be alive");

        let result = tokio::time::timeout(Duration::from_secs(1), handle)
            .await
            .expect("must resolve promptly -- waiting out the 1-hour grace would time out here")
            .expect("task should not panic");
        assert!(result.is_ok());
        assert_eq!(
            tokio::time::Instant::now().duration_since(before),
            Duration::ZERO,
            "the default impl must resolve at the instant shutdown fires, adding no latency"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_overriding_input_draining_within_its_grace_completes_and_delivers() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        let handle = tokio::spawn(run_input(
            "in".to_string(),
            Box::new(DrainingInput { drain_for: Duration::from_secs(2), batch: Some(batch) }),
            fanout,
            shutdown_rx,
            Duration::from_secs(10), // grace comfortably longer than the 2s drain
        ));

        tokio::task::yield_now().await;
        shutdown_tx.send(true).expect("receiver should still be alive");

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("should resolve once the 2s drain completes, well before the 10s grace")
            .expect("task should not panic");
        assert!(result.is_ok());

        let delivered = rx.recv().await.expect("the drained batch should have reached the fanout");
        assert_eq!(unwrap_batch(delivered).events.len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn an_overriding_input_that_never_finishes_is_cancelled_at_exactly_the_grace_deadline() {
        let (tx, _rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let grace = Duration::from_secs(5);

        let handle = tokio::spawn(run_input(
            "in".to_string(),
            Box::new(DrainingInput { drain_for: Duration::from_secs(3600), batch: None }),
            fanout,
            shutdown_rx,
            grace,
        ));

        tokio::task::yield_now().await;
        let before = tokio::time::Instant::now();
        shutdown_tx.send(true).expect("receiver should still be alive");

        let result = tokio::time::timeout(Duration::from_secs(30), handle)
            .await
            .expect("must be cancelled at the 5s grace, not wait out the 1-hour drain")
            .expect("task should not panic");
        assert!(result.is_ok(), "grace expiry resolves Ok, matching write_loop's own convention");
        assert_eq!(tokio::time::Instant::now().duration_since(before), grace);
    }

    /// Dropping the default impl's `run` future drops its `Fanout`, closing every downstream inbox.
    #[tokio::test(start_paused = true)]
    async fn the_default_impls_dropped_run_future_still_closes_every_downstream_inbox() {
        let (tx, mut rx) = mpsc::channel::<Delivered>(1);
        let fanout = Fanout::new(vec![tx]);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(run_input(
            "in".to_string(),
            Box::new(ForeverInput),
            fanout,
            shutdown_rx,
            Duration::from_secs(60),
        ));
        tokio::task::yield_now().await;
        shutdown_tx.send(true).expect("receiver should still be alive");
        handle.await.expect("task should not panic").expect("should complete without error");

        assert!(
            rx.recv().await.is_none(),
            "the downstream inbox should observe every sender dropped and close"
        );
    }

    #[tokio::test]
    async fn an_input_erroring_before_any_shutdown_still_propagates_with_its_component_context() {
        let (tx, _rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        let err = run_input(
            "bad".to_string(),
            Box::new(ErrInput),
            fanout,
            shutdown_rx,
            Duration::from_secs(60),
        )
        .await
        .expect_err("should propagate the input's own error");
        let message = format!("{err:#}");
        assert!(message.contains("component 'bad'"), "got: {message}");
        assert!(message.contains("boom"), "got: {message}");
    }

    /// A single-consumer `Fanout` delivers `Delivered::Owned`, never an `Arc`.
    #[tokio::test]
    async fn a_single_consumer_fanout_delivers_the_batch_owned_with_no_arc_involved() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        fanout.send(batch).await;

        let received = rx.recv().await.expect("should receive");
        assert!(
            matches!(received, Delivered::Owned(_, _)),
            "a single-consumer edge should never wrap the batch in an Arc"
        );
    }

    /// A fan-out's `Arc` becomes unwrappable without a clone only once every sibling handle drops.
    #[tokio::test]
    async fn a_shared_batchs_arc_is_uniquely_held_only_once_every_sibling_handle_is_dropped() {
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx_a, tx_b]);
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        fanout.send(batch).await;

        let Delivered::Shared(shared_a, _ctx) = rx_a.recv().await.expect("a should receive") else {
            panic!("a fan-out of two consumers should share, not own")
        };
        let Delivered::Shared(shared_b, _ctx) = rx_b.recv().await.expect("b should receive") else {
            panic!("a fan-out of two consumers should share, not own")
        };

        assert_eq!(Arc::strong_count(&shared_a), 2, "both branches still hold their own handle");

        drop(shared_b);

        assert_eq!(
            Arc::strong_count(&shared_a),
            1,
            "once the sibling branch drops its handle, this one is uniquely held"
        );
        assert!(
            Arc::try_unwrap(shared_a).is_ok(),
            "try_unwrap should now succeed with no clone -- this is the property the whole \
             design rests on"
        );
    }

    /// Listener, transform, and sink each get the uniform metric set from the runtime alone.
    #[tokio::test]
    async fn run_with_telemetry_records_the_uniform_metric_set_for_every_node_kind() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "xform".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Json {
                    skip_to_brace: false,
                    invalid_utf8: Default::default(),
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["xform".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert("xform".to_string(), NodeSpec::Transform(Box::new(MutatingTransform)));
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "xform", "out"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, telemetry, Readiness::disabled(), std::future::pending()),
        )
        .await
        .expect("should not hang")
        .expect("should complete without error");

        result_rx.recv_timeout(Duration::from_secs(1)).expect("output should receive the batch");

        let events = registry.drain(0);
        let value = |name: &str, component: &str| -> Option<f64> {
            events.iter().find_map(|e| {
                if e.attributes.get("component").and_then(|v| v.as_str()) != Some(component) {
                    return None;
                }
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s) if logit_core::interner::resolve(m.name) == name => {
                        Some(s.value)
                    }
                    _ => None,
                })
            })
        };

        assert_eq!(
            value("logit.component.batches.sent", "in"),
            Some(1.0),
            "the listener's own Fanout should record what it sent"
        );
        assert_eq!(value("logit.component.events.sent", "in"), Some(1.0));
        assert_eq!(value("logit.component.batches.received", "xform"), Some(1.0));
        assert_eq!(value("logit.component.events.received", "xform"), Some(1.0));
        assert_eq!(value("logit.component.batches.received", "out"), Some(1.0));
        assert_eq!(value("logit.component.events.received", "out"), Some(1.0));
    }

    /// A Lua node reports VM memory and tags `return {a, b}` emits as `outcome="emit_many"`.
    #[tokio::test]
    async fn run_lua_records_vm_memory_and_emit_outcome() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "enrich".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Lua {
                    script: "function process(event) return {event, event:clone()} end".to_string(),
                    interval: None,
                    max_memory: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["enrich".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "enrich".to_string(),
            NodeSpec::Lua {
                script: "function process(event) return {event, event:clone()} end".to_string(),
                interval: None,
                runtime: LuaRuntimeConfig::default(),
            },
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "enrich", "out"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, telemetry, Readiness::disabled(), std::future::pending()),
        )
        .await
        .expect("should not hang")
        .expect("should complete without error");

        let received =
            result_rx.recv_timeout(Duration::from_secs(1)).expect("output should receive a batch");
        assert_eq!(received.events.len(), 2, "one event in should fan out to two events out");

        let events = registry.drain(0);
        let value = |name: &str, tag: Option<(&str, &str)>| -> Option<f64> {
            events.iter().find_map(|e| {
                if e.attributes.get("component").and_then(|v| v.as_str()) != Some("enrich") {
                    return None;
                }
                if let Some((k, v)) = tag {
                    if e.attributes.get(k).and_then(|v2| v2.as_str()) != Some(v) {
                        return None;
                    }
                }
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s) if logit_core::interner::resolve(m.name) == name => {
                        Some(s.value)
                    }
                    _ => None,
                })
            })
        };

        assert_eq!(value("logit.script.events.emitted", Some(("outcome", "emit_many"))), Some(2.0));

        let vm_memory = events.iter().find_map(|e| {
            if e.attributes.get("component").and_then(|v| v.as_str()) != Some("enrich") {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Gauge(v)
                    if logit_core::interner::resolve(m.name) == "logit.script.vm.memory" =>
                {
                    Some(*v)
                }
                _ => None,
            })
        });
        assert!(vm_memory.is_some_and(|v| v > 0.0), "a loaded Lua VM should report nonzero memory");
    }

    /// A `resource` write in `process()` reaches the outgoing batch.
    #[tokio::test]
    async fn run_lua_process_writing_resource_re_stamps_the_outgoing_batch() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        let script = r#"
            function process(event)
                resource["service.name"] = "web"
                return event
            end
        "#
        .to_string();
        components.insert(
            "enrich".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Lua {
                    script: script.clone(),
                    interval: None,
                    max_memory: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["enrich".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "enrich".to_string(),
            NodeSpec::Lua { script, interval: None, runtime: LuaRuntimeConfig::default() },
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "enrich", "out"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, telemetry, Readiness::disabled(), std::future::pending()),
        )
        .await
        .expect("should not hang")
        .expect("should complete without error");

        let received =
            result_rx.recv_timeout(Duration::from_secs(1)).expect("output should receive a batch");
        assert_eq!(
            received.resource.attributes.get("service.name"),
            Some(&logit_core::Value::str("web")),
            "a resource write inside process() must reach the outgoing batch"
        );
    }

    /// A `scope` write in `process()` reaches the outgoing batch; `resource` is left unchanged.
    #[tokio::test]
    async fn run_lua_process_writing_scope_re_stamps_the_outgoing_batch() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        let script = r#"
            function process(event)
                scope.name = "nginx-otel-module"
                return event
            end
        "#
        .to_string();
        components.insert(
            "enrich".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Lua {
                    script: script.clone(),
                    interval: None,
                    max_memory: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["enrich".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "enrich".to_string(),
            NodeSpec::Lua { script, interval: None, runtime: LuaRuntimeConfig::default() },
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "enrich", "out"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, telemetry, Readiness::disabled(), std::future::pending()),
        )
        .await
        .expect("should not hang")
        .expect("should complete without error");

        let received =
            result_rx.recv_timeout(Duration::from_secs(1)).expect("output should receive a batch");
        let scope =
            received.scope.as_ref().expect("a scope write inside process() must produce a scope");
        assert_eq!(
            scope.name.as_ref(),
            b"nginx-otel-module",
            "a scope write inside process() must reach the outgoing batch"
        );
        assert!(
            received.resource.attributes.is_empty(),
            "a script that never touches resource must leave it unchanged"
        );
    }

    /// The drained `process` span for `component_id`; panics listing `events` if absent.
    fn find_process_span<'a>(events: &'a [Event], component_id: &str) -> &'a Event {
        events
            .iter()
            .find(|e| {
                e.span.is_some()
                    && span_op(e) == Some("process")
                    && e.attributes.get("component").and_then(|v| v.as_str()) == Some(component_id)
            })
            .unwrap_or_else(|| panic!("no process span for '{component_id}' in: {events:?}"))
    }

    /// A batch whose every event errors marks the Lua process span `SpanStatus::Error`.
    #[tokio::test]
    async fn run_lua_marks_its_process_span_as_error_when_every_event_in_the_batch_errors() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "enrich".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Lua {
                    script: "function process(event) error('boom') end".to_string(),
                    interval: None,
                    max_memory: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["enrich".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, _result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0), counter_event("hits", 2.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "enrich".to_string(),
            NodeSpec::Lua {
                script: "function process(event) error('boom') end".to_string(),
                interval: None,
                runtime: LuaRuntimeConfig::default(),
            },
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        // Keep every span, not the default 10%.
        let registry = Registry::with_span_sampling(1.0);
        let telemetry: HashMap<String, Telemetry> = ["in", "enrich", "out"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, telemetry, Readiness::disabled(), std::future::pending()),
        )
        .await
        .expect("should not hang")
        .expect("should complete without error");

        let events = registry.drain(0);
        let span_event = find_process_span(&events, "enrich");
        let record = span_event.span.as_ref().expect("span record");
        assert_eq!(
            record.status,
            SpanStatus::Error,
            "a batch where every event errored must not drain as a successful span"
        );
    }

    /// A batch mixing successes and script errors still marks the process span `Error`.
    #[tokio::test]
    async fn run_lua_marks_its_process_span_as_error_on_a_mixed_batch_of_successes_and_failures() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        let script = "n = 0\n\
                       function process(event)\n\
                       \x20 n = n + 1\n\
                       \x20 if n % 2 == 0 then error('boom') end\n\
                       \x20 return event\n\
                       end"
        .to_string();
        components.insert(
            "enrich".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Lua {
                    script: script.clone(),
                    interval: None,
                    max_memory: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["enrich".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0), counter_event("hits", 2.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "enrich".to_string(),
            NodeSpec::Lua { script, interval: None, runtime: LuaRuntimeConfig::default() },
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::with_span_sampling(1.0);
        let telemetry: HashMap<String, Telemetry> = ["in", "enrich", "out"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, telemetry, Readiness::disabled(), std::future::pending()),
        )
        .await
        .expect("should not hang")
        .expect("should complete without error");

        let received =
            result_rx.recv_timeout(Duration::from_secs(1)).expect("output should receive a batch");
        assert_eq!(received.events.len(), 1, "only the non-erroring event should survive");

        let events = registry.drain(0);
        let span_event = find_process_span(&events, "enrich");
        let record = span_event.span.as_ref().expect("span record");
        assert_eq!(
            record.status,
            SpanStatus::Error,
            "a mixed success/error batch must not drain as a successful span"
        );
    }

    /// `flush_now` samples VM memory even when no batch ever arrived.
    #[tokio::test]
    async fn a_flush_with_no_batch_ever_received_still_records_vm_memory() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "windowed".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: ComponentKind::Lua {
                    script: "function process(event) return event end".to_string(),
                    interval: Some(Duration::from_secs(3600)),
                    max_memory: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["windowed".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(FiniteInput { batch: None }), InputRuntimeConfig::default()),
        );
        specs.insert(
            "windowed".to_string(),
            NodeSpec::Lua {
                script: "function process(event) return event end".to_string(),
                interval: Some(Duration::from_secs(3600)),
                runtime: LuaRuntimeConfig::default(),
            },
        );
        let (result_tx, _result_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "windowed", "out"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, telemetry, Readiness::disabled(), std::future::pending()),
        )
        .await
        .expect("should not hang")
        .expect("should complete without error");

        let events = registry.drain(0);
        let vm_memory = events.iter().find_map(|e| {
            if e.attributes.get("component").and_then(|v| v.as_str()) != Some("windowed") {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Gauge(v)
                    if logit_core::interner::resolve(m.name) == "logit.script.vm.memory" =>
                {
                    Some(*v)
                }
                _ => None,
            })
        });
        assert!(
            vm_memory.is_some_and(|v| v > 0.0),
            "the close-time flush should have sampled VM memory even with no batch ever received"
        );
    }

    // -----------------------------------------------------------------------------------------
    // `run_output`'s drain/write split (`docs/adr/buffered-sink-delivery.md`)
    // -----------------------------------------------------------------------------------------

    /// One-shot gate: `wait()` blocks until `open()`, race-free whichever runs first.
    #[derive(Clone)]
    struct Gate(Arc<GateState>);

    struct GateState {
        open: std::sync::atomic::AtomicBool,
        notify: tokio::sync::Notify,
    }

    impl Gate {
        fn new() -> Self {
            Self(Arc::new(GateState {
                open: std::sync::atomic::AtomicBool::new(false),
                notify: tokio::sync::Notify::new(),
            }))
        }

        async fn wait(&self) {
            loop {
                let notified = self.0.notify.notified();
                if self.0.open.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
                notified.await;
            }
        }

        fn open(&self) {
            self.0.open.store(true, std::sync::atomic::Ordering::Release);
            self.0.notify.notify_waiters();
        }
    }

    /// A sink whose `send` doesn't resolve until its [`Gate`] opens.
    struct SlowOutput {
        gate: Gate,
        // Tokio, not `std::sync::mpsc`: a blocking receive would starve the pipeline task on the
        // single-threaded test runtime.
        delivered: tokio::sync::mpsc::UnboundedSender<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Output for SlowOutput {
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            self.gate.wait().await;
            let _ = self.delivered.send(batch.clone());
            Ok(())
        }
    }

    /// A sink whose `send` always fails, with an unclassified error.
    struct FailingOutput;

    #[async_trait::async_trait]
    impl Output for FailingOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            anyhow::bail!("simulated unclassified send failure")
        }
    }

    /// Sends every batch in order, then idles forever, so the test controls teardown.
    struct BurstInput {
        batches: Vec<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Input for BurstInput {
        async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
            for batch in self.batches.drain(..) {
                sink.send(batch).await;
            }
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    /// [`BurstInput`] that returns once every batch is sent.
    struct FiniteBurstInput {
        batches: Vec<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Input for FiniteBurstInput {
        async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
            for batch in self.batches.drain(..) {
                sink.send(batch).await;
            }
            Ok(())
        }
    }

    fn gauge_value(events: &[Event], component: &str, name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            if e.attributes.get("component").and_then(|v| v.as_str()) != Some(component) {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Gauge(v) if logit_core::interner::resolve(m.name) == name => Some(*v),
                _ => None,
            })
        })
    }

    /// A stuck `send` doesn't stop the inbox draining into the queue (`buffer.batches` shows it).
    #[tokio::test(start_paused = true)]
    async fn a_slow_sinks_send_in_flight_does_not_stop_its_inbox_from_draining_into_the_queue() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let batches: Vec<EventBatch> = (0..5)
            .map(|i| EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", i as f64)],
            })
            .collect();

        let gate = Gate::new();
        let (delivered_tx, mut delivered_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(BurstInput { batches }), InputRuntimeConfig::default()),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(SlowOutput { gate: gate.clone(), delivered: delivered_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig {
                    max_batches: 100,
                    max_bytes: u64::MAX,
                    overflow: OverflowPolicy::Block,
                }),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::new();
        let mut telemetry: HashMap<String, Telemetry> = HashMap::new();
        telemetry.insert("out".to_string(), registry.telemetry_for("out", "x", "sink"));

        let run_task = tokio::spawn(run_with_telemetry(
            g,
            specs,
            telemetry,
            Readiness::disabled(),
            std::future::pending(),
        ));

        // Wait until the queue holds more than the one batch stuck in `send`.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let queued = gauge_value(&registry.drain(0), "out", "logit.component.buffer.batches");
            if queued.unwrap_or(0.0) > 1.0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for more than one batch to be queued while send was gated"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        gate.open();

        for i in 0..5 {
            let received = tokio::time::timeout(Duration::from_secs(5), delivered_rx.recv())
                .await
                .expect("every batch should eventually be delivered once the gate opens")
                .expect("the channel should not have closed");
            match &received.events[0].metrics[0].kind {
                MetricKind::Sum(s) => {
                    assert_eq!(s.value, i as f64, "batches should still be delivered in order")
                }
                other => panic!("expected Sum, got {other:?}"),
            }
        }

        run_task.abort();
    }

    /// An isolated unclassified send failure drops the batch; `run` still completes `Ok`.
    #[tokio::test]
    async fn a_single_isolated_send_failure_no_longer_ends_run_the_batch_is_dropped_instead() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(FailingOutput),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should not hang")
            .expect(
                "a single dropped batch should no longer end run with an error -- it should \
                 complete normally once the input (and therefore the queue) is exhausted",
            );
    }

    /// A sink failing with unclassified errors doesn't end `run` while its input stays live.
    #[tokio::test]
    async fn a_permanently_failing_sink_with_a_live_input_does_not_end_run() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(BurstInput { batches: vec![batch] }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(FailingOutput),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let run_task = tokio::spawn(run(g, specs));
        let abort_handle = run_task.abort_handle();
        let outcome = tokio::time::timeout(Duration::from_millis(200), run_task).await;
        assert!(
            outcome.is_err(),
            "run should still be running -- a permanently failing sink with no sustained \
             60s-window trip must not end the pipeline just because its input stays live"
        );
        abort_handle.abort();
    }

    /// Every batch queued before the inbox closes is delivered before `run_output` resolves.
    #[tokio::test]
    async fn inbox_close_drains_the_queues_tail_before_run_output_resolves() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["in".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let batches: Vec<EventBatch> = (0..5)
            .map(|i| EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", i as f64)],
            })
            .collect();

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(FiniteBurstInput { batches }), InputRuntimeConfig::default()),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should not hang")
            .expect("run should complete without error");

        let received: Vec<EventBatch> = result_rx.try_iter().collect();
        assert_eq!(
            received.len(),
            5,
            "every batch sent before the inbox closed should still have been delivered"
        );
    }

    // -----------------------------------------------------------------------------------------
    // `write_loop`'s retry/posture/failure-handling/shutdown-grace logic
    // (`docs/adr/buffered-sink-delivery.md`)
    // -----------------------------------------------------------------------------------------

    /// Fails with `fault` for its first `fail_times` sends, then succeeds; records each attempt.
    struct FaultyOutput {
        fault: Fault,
        fail_times: u32,
        attempts: Arc<std::sync::atomic::AtomicU32>,
        attempt_times: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
        attempted: mpsc::UnboundedSender<()>,
        flushed: Arc<std::sync::atomic::AtomicBool>,
    }

    #[async_trait::async_trait]
    impl Output for FaultyOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            let n = self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.attempt_times.lock().unwrap().push(tokio::time::Instant::now());
            let _ = self.attempted.send(());
            if n < self.fail_times {
                return Err(anyhow::anyhow!("simulated {:?} failure", self.fault))
                    .context(self.fault);
            }
            Ok(())
        }

        async fn flush(&mut self) -> anyhow::Result<()> {
            self.flushed.store(true, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    struct FaultyOutputHandles {
        attempts: Arc<std::sync::atomic::AtomicU32>,
        attempt_times: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
        attempted: mpsc::UnboundedReceiver<()>,
        flushed: Arc<std::sync::atomic::AtomicBool>,
    }

    fn faulty_output(fault: Fault, fail_times: u32) -> (FaultyOutput, FaultyOutputHandles) {
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let attempt_times = Arc::new(std::sync::Mutex::new(Vec::new()));
        let flushed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (attempted_tx, attempted_rx) = mpsc::unbounded_channel();
        (
            FaultyOutput {
                fault,
                fail_times,
                attempts: attempts.clone(),
                attempt_times: attempt_times.clone(),
                attempted: attempted_tx,
                flushed: flushed.clone(),
            },
            FaultyOutputHandles { attempts, attempt_times, attempted: attempted_rx, flushed },
        )
    }

    fn one_event_batch(value: f64) -> Arc<EventBatch> {
        Arc::new(EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", value)],
        })
    }

    fn fast_retry_config() -> RetryConfig {
        RetryConfig { base_delay: Duration::from_millis(1), max_delay: Duration::from_millis(5) }
    }

    /// Runs `write_loop` over a closed queue holding `batches`, with no shutdown, under
    /// `posture` set as the operator's override.
    async fn run_write_loop_to_completion(
        mut output: FaultyOutput,
        batches: Vec<Arc<EventBatch>>,
        retry: RetryConfig,
        posture: DeliveryPosture,
    ) {
        let batches = batches.into_iter().map(Arc::unwrap_or_clone).collect();
        let write_config = WriteLoopConfig {
            retry,
            shutdown_grace: Duration::from_secs(5),
            delivery_override: Some(posture),
            ..WriteLoopConfig::default()
        };
        crate::test_util::drive_write_loop(
            &mut output,
            batches,
            write_config,
            Telemetry::default(),
        )
        .await;
    }

    /// `backoff_for` over `base > max`, `base == max`, and attempts from 1 to `u32::MAX`: the
    /// doubling stops at `max_delay` and never overflows.
    #[test]
    fn backoff_for_doubles_from_base_and_is_capped_at_max_for_every_attempt() {
        let ms = Duration::from_millis;
        let retry = |base, max| RetryConfig { base_delay: base, max_delay: max };
        let cases = [
            // (base, max, attempt, backoff)
            (ms(100), ms(1000), 1, ms(100)),
            (ms(100), ms(1000), 2, ms(200)),
            (ms(100), ms(1000), 4, ms(800)),
            (ms(100), ms(1000), 5, ms(1000)),
            (ms(100), ms(1000), 128, ms(1000)),
            (ms(100), ms(1000), u32::MAX, ms(1000)),
            (ms(100), ms(100), 1, ms(100)),
            (ms(100), ms(100), 2, ms(100)),
            (ms(100), ms(100), 128, ms(100)),
            (ms(100), ms(100), u32::MAX, ms(100)),
            (ms(500), ms(200), 1, ms(200)),
            (ms(500), ms(200), 2, ms(200)),
            (ms(500), ms(200), 128, ms(200)),
            (ms(500), ms(200), u32::MAX, ms(200)),
            (Duration::from_secs(u64::MAX / 2), Duration::MAX, u32::MAX, Duration::MAX),
        ];
        for (base, max, attempt, expected) in cases {
            assert_eq!(
                backoff_for(&retry(base, max), attempt),
                expected,
                "base {base:?}, max {max:?}, attempt {attempt}"
            );
        }
    }

    /// Per attempt: one `send.duration` sample, and per failed attempt one `errors`, and one
    /// `retries` when another attempt follows.
    #[tokio::test]
    async fn every_attempt_records_one_send_duration_sample_and_every_retry_one_error() {
        use DeliveryPosture::{AtLeastOnce, AtMostOnce};
        // (fault, failures before a success, delivery posture, attempts, retries)
        let cases = [
            (Fault::Clean, 0, AtMostOnce, 1, 0),
            (Fault::Clean, 1, AtMostOnce, 2, 1),
            (Fault::Clean, 3, AtMostOnce, 4, 3),
            (Fault::Ambiguous, 3, AtLeastOnce, 4, 3),
            // Dropped on its first failure: an error, but no retry follows.
            (Fault::Ambiguous, u32::MAX, AtMostOnce, 1, 0),
            (Fault::Rejected, u32::MAX, AtLeastOnce, 1, 0),
            (Fault::Refused, 3, AtMostOnce, 4, 3),
        ];
        for (fault, fail_times, posture, attempts, retries) in cases {
            let label = format!("{fault:?} x{fail_times}, {posture:?}");
            let (mut output, handles) = faulty_output(fault, fail_times);
            let mut probe = TelemetryProbe::new();
            let config = WriteLoopConfig {
                retry: fast_retry_config(),
                delivery_override: Some(posture),
                ..WriteLoopConfig::default()
            };
            crate::test_util::drive_write_loop(
                &mut output,
                vec![Arc::unwrap_or_clone(one_event_batch(1.0))],
                config,
                probe.telemetry("out", "influxdb_out", "sink"),
            )
            .await;
            assert_eq!(
                handles.attempts.load(std::sync::atomic::Ordering::SeqCst),
                attempts,
                "{label}"
            );
            let totals = probe.poll();
            let errors = u32::min(fail_times, attempts);
            assert_eq!(totals.sum("logit.component.errors", &[]), f64::from(errors), "{label}");
            assert_eq!(totals.sum("logit.component.retries", &[]), f64::from(retries), "{label}");
            let samples: usize = totals
                .events
                .iter()
                .flat_map(|e| e.metrics.iter())
                .filter(|m| {
                    logit_core::interner::resolve(m.name) == "logit.component.send.duration"
                })
                .map(|m| match &m.kind {
                    MetricKind::Distribution(sketch) => sketch.count(),
                    other => panic!("send.duration is a distribution, got {other:?}"),
                })
                .sum();
            assert_eq!(samples, attempts as usize, "{label}");
        }
    }

    /// A `send` that never returns.
    struct HangingOutput;

    #[async_trait::async_trait]
    impl Output for HangingOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            std::future::pending().await
        }
    }

    /// An attempt runs under no runtime timeout: a send that never returns stays in flight until
    /// the shutdown grace cuts it, and the cut send is `Ambiguous`, so under at-most-once it's
    /// committed and counted `shutdown`.
    #[tokio::test(start_paused = true)]
    async fn a_send_runs_until_the_grace_cuts_it_and_the_cut_send_is_ambiguous() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "influxdb_out", "sink");
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        store.push((one_event_batch(1.0), TraceContext::default().into())).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let grace = Duration::from_millis(500);
        let write_config = WriteLoopConfig {
            retry: fast_retry_config(),
            shutdown_grace: grace,
            delivery_override: Some(DeliveryPosture::AtMostOnce),
            ..WriteLoopConfig::default()
        };
        let store_for_task = Arc::clone(&store);
        let shutdown_dropped = Arc::new(AtomicU64::new(0));
        let dropped_for_task = Arc::clone(&shutdown_dropped);
        let handle = tokio::spawn(async move {
            write_loop(
                "out".to_string(),
                &mut HangingOutput,
                store_for_task,
                telemetry,
                write_config,
                shutdown_rx,
                &dropped_for_task,
            )
            .await;
        });

        // An hour of virtual time sizes the negative window: nothing cuts the send short.
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert!(!handle.is_finished(), "no runtime timeout ends an attempt");
        assert_eq!(probe.sum("logit.component.errors", &[]), 0.0);

        let signalled = tokio::time::Instant::now();
        shutdown_tx.send(true).expect("receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("write_loop returns once the grace expires")
            .expect("the task should not panic");
        assert!(signalled.elapsed() >= grace, "the grace, and nothing sooner, cut the send");
        assert_eq!(
            probe.sum("logit.component.batches.dropped", &[("reason", "shutdown")]),
            1.0,
            "the cut send is Ambiguous, which at-most-once drops"
        );
        assert_eq!(shutdown_dropped.load(std::sync::atomic::Ordering::Relaxed), 1);
        assert!(store.commit().is_none(), "committed, not left queued");
    }

    async fn assert_clean_fault_retries_and_eventually_delivers(posture: DeliveryPosture) {
        let (output, handles) = faulty_output(Fault::Clean, 2);
        run_write_loop_to_completion(
            output,
            vec![one_event_batch(1.0)],
            fast_retry_config(),
            posture,
        )
        .await;
        assert_eq!(
            handles.attempts.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "should fail twice then succeed on the 3rd attempt"
        );
    }

    #[tokio::test]
    async fn a_clean_fault_is_retried_and_eventually_delivered_under_at_most_once() {
        assert_clean_fault_retries_and_eventually_delivers(DeliveryPosture::AtMostOnce).await;
    }

    #[tokio::test]
    async fn a_clean_fault_is_retried_and_eventually_delivered_under_at_least_once() {
        assert_clean_fault_retries_and_eventually_delivers(DeliveryPosture::AtLeastOnce).await;
    }

    #[tokio::test]
    async fn an_ambiguous_fault_is_dropped_immediately_under_at_most_once_with_no_retry() {
        let (output, handles) = faulty_output(Fault::Ambiguous, u32::MAX);
        run_write_loop_to_completion(
            output,
            vec![one_event_batch(1.0)],
            fast_retry_config(),
            DeliveryPosture::AtMostOnce,
        )
        .await;
        assert_eq!(
            handles.attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "an Ambiguous fault under AtMostOnce must be dropped after exactly one attempt, no retry"
        );
    }

    #[tokio::test]
    async fn an_ambiguous_fault_is_retried_under_at_least_once_and_eventually_delivered() {
        let (output, handles) = faulty_output(Fault::Ambiguous, 2);
        run_write_loop_to_completion(
            output,
            vec![one_event_batch(1.0)],
            fast_retry_config(),
            DeliveryPosture::AtLeastOnce,
        )
        .await;
        assert_eq!(handles.attempts.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    /// A sink that declares no posture: `FaultyOutput` with the trait's `default_posture`.
    struct DeclaresNothing(FaultyOutput);

    #[async_trait::async_trait]
    impl Output for DeclaresNothing {
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            self.0.send(batch).await
        }
    }

    /// A sink that declares `AtMostOnce` as its own default, as `statsd_out` does.
    struct DeclaresAtMostOnce(FaultyOutput);

    #[async_trait::async_trait]
    impl Output for DeclaresAtMostOnce {
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            self.0.send(batch).await
        }

        fn default_posture(&self) -> DeliveryPosture {
            DeliveryPosture::AtMostOnce
        }
    }

    /// Runs `write_loop` over one batch under `delivery_override` and returns how many attempts
    /// `handles` saw and how many batches `write_loop` counted dropped.
    async fn attempts_and_drops<O: Output + Send>(
        output: &mut O,
        handles: &FaultyOutputHandles,
        delivery_override: Option<DeliveryPosture>,
    ) -> (u32, f64) {
        let mut probe = TelemetryProbe::new();
        let config =
            WriteLoopConfig { retry: fast_retry_config(), delivery_override, ..Default::default() };
        crate::test_util::drive_write_loop(
            output,
            vec![Arc::unwrap_or_clone(one_event_batch(1.0))],
            config,
            probe.telemetry("out", "influxdb_out", "sink"),
        )
        .await;
        let dropped = probe.poll().sum("logit.component.batches.dropped", &[]);
        (handles.attempts.load(std::sync::atomic::Ordering::SeqCst), dropped)
    }

    /// A sink declaring `AtMostOnce` that records each posture `write_loop` hands it.
    struct RecordsPosture(DeclaresAtMostOnce, Vec<DeliveryPosture>);

    #[async_trait::async_trait]
    impl Output for RecordsPosture {
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            self.0.send(batch).await
        }

        fn default_posture(&self) -> DeliveryPosture {
            self.0.default_posture()
        }

        fn observe_posture(&mut self, posture: DeliveryPosture) {
            self.1.push(posture);
        }
    }

    /// `write_loop` hands the sink the posture it resolved, once: the override when set, else
    /// the sink's own default.
    #[tokio::test(start_paused = true)]
    async fn write_loop_hands_the_sink_its_resolved_posture_once() {
        for (delivery_override, want) in [
            (None, DeliveryPosture::AtMostOnce),
            (Some(DeliveryPosture::AtLeastOnce), DeliveryPosture::AtLeastOnce),
        ] {
            let (output, handles) = faulty_output(Fault::Clean, 0);
            let mut output = RecordsPosture(DeclaresAtMostOnce(output), Vec::new());
            attempts_and_drops(&mut output, &handles, delivery_override).await;
            assert_eq!(output.1, [want], "override: {delivery_override:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_fault_is_retried_by_default_for_a_sink_that_declares_nothing() {
        let (output, handles) = faulty_output(Fault::Ambiguous, 2);
        let mut output = DeclaresNothing(output);
        assert_eq!(output.default_posture(), DeliveryPosture::AtLeastOnce);
        let (attempts, dropped) = attempts_and_drops(&mut output, &handles, None).await;
        assert_eq!(attempts, 3, "two Ambiguous failures retried, then delivered");
        assert_eq!(dropped, 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn buffer_delivery_at_most_once_drops_an_ambiguous_fault_on_a_sink_that_declares_nothing()
    {
        let (output, handles) = faulty_output(Fault::Ambiguous, 2);
        let mut output = DeclaresNothing(output);
        let (attempts, dropped) =
            attempts_and_drops(&mut output, &handles, Some(DeliveryPosture::AtMostOnce)).await;
        assert_eq!(attempts, 1, "the override drops the batch after its one attempt");
        assert_eq!(dropped, 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_sink_declaring_at_most_once_drops_an_ambiguous_fault_by_default() {
        let (output, handles) = faulty_output(Fault::Ambiguous, 2);
        let mut output = DeclaresAtMostOnce(output);
        let (attempts, dropped) = attempts_and_drops(&mut output, &handles, None).await;
        assert_eq!(attempts, 1, "the sink's own default drops the batch after one attempt");
        assert_eq!(dropped, 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn buffer_delivery_at_least_once_overrides_a_sink_declaring_at_most_once() {
        let (output, handles) = faulty_output(Fault::Ambiguous, 2);
        let mut output = DeclaresAtMostOnce(output);
        let (attempts, dropped) =
            attempts_and_drops(&mut output, &handles, Some(DeliveryPosture::AtLeastOnce)).await;
        assert_eq!(attempts, 3, "the override retries past the sink's own default");
        assert_eq!(dropped, 0.0);
    }

    /// Runs one batch through `write_loop` against `faulty_output(fault, fail_times)` under
    /// `posture` and returns the attempts, the retries, and the batches delivered and dropped.
    async fn outcome_of(
        fault: Fault,
        fail_times: u32,
        posture: DeliveryPosture,
    ) -> (u32, f64, f64, Totals) {
        let (mut output, handles) = faulty_output(fault, fail_times);
        let mut probe = TelemetryProbe::new();
        let config = WriteLoopConfig {
            retry: fast_retry_config(),
            delivery_override: Some(posture),
            ..WriteLoopConfig::default()
        };
        crate::test_util::drive_write_loop(
            &mut output,
            vec![Arc::unwrap_or_clone(one_event_batch(1.0))],
            config,
            probe.telemetry("out", "influxdb_out", "sink"),
        )
        .await;
        let totals = probe.poll().clone();
        let attempts = handles.attempts.load(std::sync::atomic::Ordering::SeqCst);
        let retries = totals.sum("logit.component.retries", &[]);
        let delivered = totals.sum("logit.component.batches.delivered", &[]);
        (attempts, retries, delivered, totals)
    }

    /// A `Rejected` batch is dropped on its first attempt under either posture, counted
    /// `rejected`, and never announced as a hold.
    #[tokio::test(start_paused = true)]
    async fn a_rejected_fault_is_never_retried_under_either_posture() {
        for posture in [DeliveryPosture::AtMostOnce, DeliveryPosture::AtLeastOnce] {
            let (attempts, retries, delivered, totals) =
                outcome_of(Fault::Rejected, u32::MAX, posture).await;
            assert_eq!(attempts, 1, "{posture:?}");
            assert_eq!(retries, 0.0, "{posture:?}");
            assert_eq!(delivered, 0.0, "{posture:?}");
            assert_eq!(
                totals.sum("logit.component.batches.dropped", &[("reason", "rejected")]),
                1.0,
                "{posture:?}"
            );
            assert_eq!(totals.gauge(RETRYING_GAUGE, &[]), None, "{posture:?}: never a hold");
        }
    }

    /// A `Refused` fault applied nothing, so it retries under either posture until it succeeds.
    #[tokio::test(start_paused = true)]
    async fn a_refused_fault_is_retried_under_either_posture_until_it_succeeds() {
        for posture in [DeliveryPosture::AtMostOnce, DeliveryPosture::AtLeastOnce] {
            let (attempts, retries, delivered, totals) =
                outcome_of(Fault::Refused, 3, posture).await;
            assert_eq!(attempts, 4, "{posture:?}");
            assert_eq!(retries, 3.0, "{posture:?}");
            assert_eq!(delivered, 1.0, "{posture:?}");
            assert_eq!(totals.sum("logit.component.batches.dropped", &[]), 0.0, "{posture:?}");
            assert_eq!(
                totals.gauge(RETRYING_GAUGE, &[]),
                Some(0.0),
                "{posture:?}: raised on the first failure, lowered on delivery"
            );
        }
    }

    /// An `Ambiguous` drop under at-most-once counts under its own reason, apart from a rejection.
    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_drop_under_at_most_once_counts_its_own_reason() {
        let (_, _, _, totals) =
            outcome_of(Fault::Ambiguous, u32::MAX, DeliveryPosture::AtMostOnce).await;
        assert_eq!(
            totals.sum("logit.component.batches.dropped", &[("reason", "ambiguous_at_most_once")]),
            1.0
        );
        assert_eq!(totals.sum("logit.component.batches.dropped", &[("reason", "rejected")]), 0.0);
    }

    /// Backoff between attempts doubles: 100, 200, 400, 800 ms.
    #[tokio::test(start_paused = true)]
    async fn backoff_between_retry_attempts_follows_the_configured_doubling_schedule() {
        let (output, handles) = faulty_output(Fault::Clean, 4);
        let retry = RetryConfig {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
        };
        run_write_loop_to_completion(
            output,
            vec![one_event_batch(1.0)],
            retry,
            DeliveryPosture::AtMostOnce,
        )
        .await;

        let times = handles.attempt_times.lock().unwrap();
        assert_eq!(times.len(), 5, "4 failed attempts plus the successful 5th");
        let deltas: Vec<Duration> = times.windows(2).map(|w| w[1].duration_since(w[0])).collect();
        assert_eq!(
            deltas,
            vec![
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400),
                Duration::from_millis(800),
            ]
        );
    }

    /// A retryable fault has no budget: the head retries until it succeeds, however many
    /// attempts that takes, and is delivered once.
    #[tokio::test(start_paused = true)]
    async fn a_retryable_fault_retries_until_it_succeeds_with_no_budget() {
        const FAILURES: u32 = 200;
        let (attempts, retries, delivered, totals) =
            outcome_of(Fault::Ambiguous, FAILURES, DeliveryPosture::AtLeastOnce).await;
        assert_eq!(attempts, FAILURES + 1);
        assert_eq!(retries, f64::from(FAILURES));
        assert_eq!(delivered, 1.0, "delivered once");
        assert_eq!(totals.sum("logit.component.batches.dropped", &[]), 0.0);
    }

    /// What [`SwitchOutput`] answers: `None` succeeds, `Some(fault)` fails with that class.
    type Verdict = Arc<std::sync::Mutex<Option<Fault>>>;

    /// Answers every `send` with a shared, switchable [`Verdict`], with a destination's text per
    /// class. Records each attempted batch's counter value.
    struct SwitchOutput {
        verdict: Verdict,
        sent: Arc<std::sync::Mutex<Vec<f64>>>,
        times: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
        attempted: mpsc::UnboundedSender<()>,
    }

    struct SwitchHandles {
        verdict: Verdict,
        sent: Arc<std::sync::Mutex<Vec<f64>>>,
        /// When each attempt in `sent` was made.
        times: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
        attempted: mpsc::UnboundedReceiver<()>,
    }

    impl SwitchHandles {
        fn set(&self, verdict: Option<Fault>) {
            *self.verdict.lock().unwrap() = verdict;
        }
        fn sent(&self) -> Vec<f64> {
            self.sent.lock().unwrap().clone()
        }
        /// Waits for `n` more attempts.
        async fn attempts(&mut self, n: usize) {
            for i in 0..n {
                tokio::time::timeout(RECV_TIMEOUT, self.attempted.recv())
                    .await
                    .unwrap_or_else(|_| panic!("attempt {i} of {n} never happened"))
                    .expect("the output is alive");
            }
        }
    }

    fn switch_output(verdict: Option<Fault>) -> (SwitchOutput, SwitchHandles) {
        let verdict = Arc::new(std::sync::Mutex::new(verdict));
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let times = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (attempted_tx, attempted_rx) = mpsc::unbounded_channel();
        (
            SwitchOutput {
                verdict: Arc::clone(&verdict),
                sent: Arc::clone(&sent),
                times: Arc::clone(&times),
                attempted: attempted_tx,
            },
            SwitchHandles { verdict, sent, times, attempted: attempted_rx },
        )
    }

    #[async_trait::async_trait]
    impl Output for SwitchOutput {
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            self.sent.lock().unwrap().push(counter_value_of(batch));
            self.times.lock().unwrap().push(tokio::time::Instant::now());
            let _ = self.attempted.send(());
            let verdict = *self.verdict.lock().unwrap();
            match verdict {
                None => Ok(()),
                Some(Fault::Refused) => {
                    Err(anyhow::anyhow!("401: token is invalid")).context(Fault::Refused)
                }
                Some(Fault::Rejected) => {
                    Err(anyhow::anyhow!("400: unable to parse line 3")).context(Fault::Rejected)
                }
                Some(fault) => Err(anyhow::anyhow!("{fault} failure")).context(fault),
            }
        }
    }

    /// The `retrying` line's pacing in [`hold_write_config`].
    const HOLD_LOG_INTERVAL: Duration = Duration::from_secs(10);

    /// A backoff from 10 ms to 1 s, so a minute of virtual time is tens of attempts, and a
    /// `retrying` line at most every [`HOLD_LOG_INTERVAL`].
    fn hold_write_config() -> WriteLoopConfig {
        WriteLoopConfig {
            retry: RetryConfig {
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_secs(1),
            },
            retrying_log_interval: HOLD_LOG_INTERVAL,
            ..WriteLoopConfig::default()
        }
    }

    /// How many self-log lines for component `id` contain `text`.
    fn log_lines(id: &str, text: &str) -> usize {
        let logs = String::from_utf8_lossy(&global_logs().0.lock().unwrap()).into_owned();
        let component = format!("component={id}");
        logs.lines().filter(|l| l.contains(&component) && l.contains(text)).count()
    }

    /// A `write_loop` over `output` and a fresh memory store bounded by `queue`, spawned under
    /// component `id`. The store, the probe it counts into, the shutdown sender, and the task.
    fn spawn_switch_write_loop(
        id: &'static str,
        mut output: SwitchOutput,
        queue: SinkQueueConfig,
    ) -> (Arc<SinkStore>, TelemetryProbe, watch::Sender<bool>, tokio::task::JoinHandle<()>) {
        global_logs();
        let probe = TelemetryProbe::new();
        let telemetry = probe.telemetry(id, "influxdb_out", "sink");
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(queue, telemetry.clone())));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let store_for_task = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            write_loop(
                id.to_string(),
                &mut output,
                store_for_task,
                telemetry,
                hold_write_config(),
                shutdown_rx,
                &AtomicU64::new(0),
            )
            .await;
        });
        (store, probe, shutdown_tx, handle)
    }

    async fn push_counter(store: &SinkStore, value: f64) {
        store.push((one_event_batch(value), TraceContext::default().into())).await;
    }

    /// A `Rejected` batch is committed on the attempt that got the verdict and counted
    /// `rejected`, and the throttled `send_failed` line carries the class and the destination's
    /// text. Nothing behind it waits.
    #[tokio::test(start_paused = true)]
    async fn a_rejected_batch_drops_at_once_with_the_destinations_text() {
        let (output, handles) = switch_output(Some(Fault::Rejected));
        let (store, mut probe, _shutdown, handle) =
            spawn_switch_write_loop("rejected_drop", output, SinkQueueConfig::default());
        push_counter(&store, 1.0).await;
        push_counter(&store, 2.0).await;
        store.close();
        tokio::time::timeout(RECV_TIMEOUT, handle)
            .await
            .expect("write_loop drains the closed queue")
            .expect("the task should not panic");

        assert_eq!(handles.sent(), vec![1.0, 2.0], "each attempted once");
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.component.batches.dropped", &[("reason", "rejected")]), 2.0);
        assert_eq!(totals.sum("logit.component.events.dropped", &[("reason", "rejected")]), 2.0);
        assert_eq!(totals.sum("logit.component.retries", &[]), 0.0);
        assert_eq!(totals.gauge(RETRYING_GAUGE, &[]), None, "a drop is never a hold");
        assert!(
            log_lines("rejected_drop", "rejected send failure: 400: unable to parse line 3") >= 1,
            "the drop line names the class and the destination's text"
        );
    }

    /// A `Refused` head holds under `overflow: block`: the queue fills behind it, the next push
    /// waits, nothing is dropped, and the gauge reads 1. Once the destination accepts, the held
    /// head and the queue behind it deliver in order and the gauge returns to 0.
    #[tokio::test(start_paused = true)]
    async fn a_refused_head_holds_while_a_blocking_queue_fills_then_delivers_on_recovery() {
        let (output, mut handles) = switch_output(Some(Fault::Refused));
        let queue = SinkQueueConfig { max_batches: 2, ..SinkQueueConfig::default() };
        let (store, mut probe, _shutdown, handle) =
            spawn_switch_write_loop("refused_block", output, queue);
        push_counter(&store, 1.0).await;
        handles.attempts(3).await;
        probe.wait_for("the hold's gauge", |t| t.gauge(RETRYING_GAUGE, &[]) == Some(1.0)).await;
        push_counter(&store, 2.0).await;
        let store_for_push = Arc::clone(&store);
        let blocked = tokio::spawn(async move { push_counter(&store_for_push, 3.0).await });

        // A minute of virtual time sizes the negative window: the head holds the whole time.
        tokio::time::sleep(Duration::from_secs(60)).await;
        assert!(!blocked.is_finished(), "a full blocking queue makes the next push wait");
        assert_eq!(store.queued(), 2, "the held head and the batch behind it");
        assert!(handles.sent().iter().all(|&v| v == 1.0), "only the head is attempted");
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.component.batches.dropped", &[]), 0.0, "a hold drops nothing");
        assert_eq!(totals.gauge(RETRYING_GAUGE, &[]), Some(1.0));
        assert!(!handle.is_finished(), "a hold never ends write_loop");

        handles.set(None);
        tokio::time::timeout(RECV_TIMEOUT, blocked)
            .await
            .expect("the waiting push completes once the queue moves")
            .expect("the push task should not panic");
        store.close();
        tokio::time::timeout(RECV_TIMEOUT, handle)
            .await
            .expect("write_loop drains the closed queue")
            .expect("the task should not panic");

        let sent = handles.sent();
        let failures = sent.iter().filter(|&&v| v == 1.0).count() - 1;
        assert_eq!(&sent[failures..], &[1.0, 2.0, 3.0], "in order, the held head first");
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 3.0);
        assert_eq!(totals.sum("logit.component.batches.dropped", &[]), 0.0);
        assert_eq!(totals.gauge(RETRYING_GAUGE, &[]), Some(0.0), "lowered on delivery");
        // A line on the first failure, then on the first failure at least an interval after the
        // last line, each naming the class and the text. A failure is recorded at its attempt's
        // instant: the send returns at once.
        let times = handles.times.lock().unwrap().clone();
        let mut paced = 0;
        let mut last: Option<tokio::time::Instant> = None;
        for &at in &times[..failures] {
            if last.is_none_or(|last| at.duration_since(last) >= HOLD_LOG_INTERVAL) {
                paced += 1;
                last = Some(at);
            }
        }
        assert!(paced >= 6, "a minute of hold at a 10 s interval: {paced} lines");
        assert_eq!(
            log_lines("refused_block", "send failed (refused): 401: token is invalid; retrying"),
            paced,
            "{failures} failures"
        );
        assert_eq!(log_lines("refused_block", "(held 0ns, failure 1, "), 1, "the first failure");
        assert_eq!(log_lines("refused_block", "delivery succeeded after a prior failure"), 1);
    }

    /// The `retrying` line is written on a head's first failure, then at most once per interval
    /// however many failures fall between, with how long the head has been held. A new head
    /// starts its own pacing.
    #[tokio::test(start_paused = true)]
    async fn the_retrying_line_is_paced_by_time_not_by_failure_count() {
        global_logs();
        let interval = Duration::from_secs(10);
        let mut retrying =
            Retrying::new(Telemetry::default(), Diagnostics::new("retry_pace"), interval);
        let err = anyhow::anyhow!("503: busy").context(Fault::Ambiguous);
        let lines = || log_lines("retry_pace", "send failed (ambiguous): 503: busy; retrying");

        retrying.failed(Fault::Ambiguous, &err, 1);
        assert_eq!(lines(), 1, "the first failure");
        for _ in 0..9 {
            tokio::time::advance(Duration::from_secs(1)).await;
            for _ in 0..100 {
                retrying.failed(Fault::Ambiguous, &err, 1);
            }
        }
        assert_eq!(lines(), 1, "900 failures inside one interval write nothing");
        tokio::time::advance(Duration::from_secs(1)).await;
        retrying.failed(Fault::Ambiguous, &err, 1);
        assert_eq!(lines(), 2, "an interval after the last line");
        assert_eq!(log_lines("retry_pace", "(held 10s, failure 902, 1 batch(es) queued)"), 1);

        retrying.settle();
        retrying.failed(Fault::Ambiguous, &err, 1);
        assert_eq!(lines(), 3, "a new head's first failure");
        assert_eq!(log_lines("retry_pace", "(held 0ns, failure 1, 1 batch(es) queued)"), 2);
    }

    /// A `Refused` head holds under `overflow: drop_oldest` too: it's reserved, so the queue
    /// evicts the oldest batch behind it, and the held head delivers on recovery.
    #[tokio::test(start_paused = true)]
    async fn a_refused_head_holds_while_a_drop_oldest_queue_evicts_behind_it() {
        let (output, mut handles) = switch_output(Some(Fault::Refused));
        let queue = SinkQueueConfig {
            max_batches: 2,
            overflow: OverflowPolicy::DropOldest,
            ..SinkQueueConfig::default()
        };
        let (store, mut probe, _shutdown, handle) =
            spawn_switch_write_loop("refused_evict", output, queue);
        push_counter(&store, 1.0).await;
        handles.attempts(2).await;
        probe.wait_for("the hold's gauge", |t| t.gauge(RETRYING_GAUGE, &[]) == Some(1.0)).await;
        push_counter(&store, 2.0).await;
        push_counter(&store, 3.0).await;
        let totals = probe.poll();
        assert_eq!(
            totals.sum("logit.component.batches.dropped", &[("reason", "overflow_oldest")]),
            1.0,
            "batch 2 evicted, the held head kept"
        );
        assert_eq!(totals.gauge(RETRYING_GAUGE, &[]), Some(1.0));

        handles.set(None);
        store.close();
        tokio::time::timeout(RECV_TIMEOUT, handle)
            .await
            .expect("write_loop drains the closed queue")
            .expect("the task should not panic");
        let sent = handles.sent();
        assert_eq!(&sent[sent.len() - 2..], &[1.0, 3.0]);
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 2.0);
        assert_eq!(totals.sum("logit.component.batches.dropped", &[("reason", "rejected")]), 0.0);
        assert_eq!(totals.gauge(RETRYING_GAUGE, &[]), Some(0.0));
    }

    /// Shutdown during a hold: the grace ends the retry, and the held head and the queue behind
    /// it are left for `finish_and_flush`, which counts them `shutdown` for a memory sink and
    /// leaves them spooled for a disk sink.
    async fn shutdown_while_held(store_config: SinkStoreConfig) -> Totals {
        let (output, mut handles) = switch_output(Some(Fault::Refused));
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let registry = Registry::new();
        let write_config =
            WriteLoopConfig { shutdown_grace: Duration::from_secs(5), ..hold_write_config() };
        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            registry.telemetry_for("out", "influxdb_out", "sink"),
            store_config,
            write_config,
            shutdown_rx,
            Arc::new(AtomicU64::new(0)),
        ));
        for value in [1.0, 2.0, 3.0] {
            inbox_tx.send(counter_batch(value)).await.unwrap();
        }
        handles.attempts(2).await;
        let mut totals = Totals::default();
        wait_until("the hold's gauge", || {
            totals.fold(registry.drain(0));
            totals.gauge(RETRYING_GAUGE, &[("component", "out")]) == Some(1.0)
        })
        .await;

        let signalled = tokio::time::Instant::now();
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(10), run)
            .await
            .expect("run_output must not stop responding")
            .expect("the task must not panic")
            .expect("shutdown during a hold ends run_output with Ok");
        assert!(
            signalled.elapsed() < write_config.shutdown_grace + Duration::from_secs(1),
            "the grace ends the hold"
        );
        drop(inbox_tx);
        assert!(handles.sent().iter().all(|&v| v == 1.0), "only the held head was attempted");
        totals.fold(registry.drain(0));
        assert_eq!(totals.gauge(RETRYING_GAUGE, &[("component", "out")]), Some(0.0));
        assert_eq!(
            totals.sum(
                "logit.component.batches.dropped",
                &[("component", "out"), ("reason", "rejected")]
            ),
            0.0
        );
        totals
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_hold_counts_a_memory_sinks_held_batches_shutdown() {
        let totals = shutdown_while_held(SinkStoreConfig::Memory(SinkQueueConfig::default())).await;
        assert_eq!(
            totals.sum(
                "logit.component.batches.dropped",
                &[("component", "out"), ("reason", "shutdown")]
            ),
            3.0,
            "the held head and the two batches behind it"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_a_hold_leaves_a_disk_sinks_held_batches_spooled_for_replay() {
        let dir = crate::disk_queue::test_support::scratch_dir("hold-shutdown");
        let totals =
            shutdown_while_held(SinkStoreConfig::Disk(disk_store_config(&dir, u64::MAX))).await;
        assert_eq!(
            totals.sum(
                "logit.component.batches.dropped",
                &[("component", "out"), ("reason", "shutdown")]
            ),
            0.0
        );
        assert_eq!(reopen_and_drain(&dir).await, vec![1.0, 2.0, 3.0], "replayed on the next open");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A sink stuck retrying still returns `Ok` within `shutdown_grace`, leaving its batch queued.
    #[tokio::test(start_paused = true)]
    async fn shutdown_grace_expiry_ends_write_loop_promptly_leaving_the_remainder_for_run_output() {
        let (mut output, _handles) = faulty_output(Fault::Clean, u32::MAX);

        let telemetry = Telemetry::default();
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        store.push((one_event_batch(1.0), TraceContext::default().into())).await;
        // Left open: grace must cut delivery off even while more could arrive.

        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                base_delay: Duration::from_millis(30),
                max_delay: Duration::from_millis(30),
            },
            shutdown_grace: Duration::from_millis(500),
            delivery_override: None,
            ..WriteLoopConfig::default()
        };
        let store_for_task = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            write_loop(
                "out".to_string(),
                &mut output,
                store_for_task,
                telemetry,
                write_config,
                shutdown_rx,
                &AtomicU64::new(0),
            )
            .await
        });

        // Let a couple of retry attempts happen first.
        tokio::time::sleep(Duration::from_millis(90)).await;
        shutdown_tx.send(true).expect("receiver should still be alive");

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("write_loop should return within shutdown_grace, not hang")
            .expect("the task should not panic");

        // Draining and flushing are `finish_and_flush`'s job, so the batch is still queued.
        assert!(
            store.commit().is_some(),
            "write_loop must leave the undelivered batch for run_output to account for, not drop it silently itself"
        );
    }

    /// A batch pushed as shutdown grace expires is counted, not lost, and `flush` runs once.
    #[tokio::test(start_paused = true)]
    async fn run_output_flushes_exactly_once_and_never_loses_a_batch_racing_shutdown_grace() {
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (output, mut handles) = faulty_output(Fault::Clean, u32::MAX);
        let flushed = Arc::clone(&handles.flushed);
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            shutdown_grace: Duration::from_millis(100),
            delivery_override: None,
            ..WriteLoopConfig::default()
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            Telemetry::default(),
            SinkStoreConfig::Memory(SinkQueueConfig::default()),
            write_config,
            shutdown_rx,
            Arc::new(AtomicU64::new(0)),
        ));

        // Fails forever, so write_loop is mid-retry when shutdown fires.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", 1.0)],
                },
                TraceContext::new_root().into(),
            ))
            .await
            .expect("receiver should still be alive");
        handles.attempted.recv().await.expect("the first attempt should have happened");

        shutdown_tx.send(true).expect("receiver should still be alive");
        // At the grace boundary, this push races the final drain. The test holds `inbox_tx`
        // itself because a real listener would already be cancelled here.
        tokio::time::sleep(Duration::from_millis(100)).await;
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", 2.0)],
                },
                TraceContext::new_root().into(),
            ))
            .await
            .expect("receiver should still be alive");
        drop(inbox_tx); // let drain_inbox finish naturally once it gets to check its inbox again

        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run_output should not hang")
            .expect("task should not panic")
            .expect("shutdown-grace expiry should end run_output with Ok, not Err");

        assert!(
            flushed.load(std::sync::atomic::Ordering::SeqCst),
            "output.flush() should have been called exactly once, on the ordinary run_output exit path"
        );
    }

    /// A batch left unread in the inbox when `write_loop` gives up is counted as dropped.
    ///
    /// Batch 1 fills the one-slot queue and is reserved by the failing retry loop; batch 2 is
    /// received by `drain_inbox` and parked in `push`, left in `in_hand` when the future is
    /// abandoned; batch 3 stays in the channel. The sweep counts batches 2 and 3.
    #[tokio::test(start_paused = true)]
    async fn a_batch_still_sitting_in_the_inbox_when_write_loop_gives_up_is_counted_not_silently_lost(
    ) {
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (output, mut handles) = faulty_output(Fault::Clean, u32::MAX);
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            shutdown_grace: Duration::from_millis(100),
            delivery_override: None,
            ..WriteLoopConfig::default()
        };
        let store_config = SinkStoreConfig::Memory(SinkQueueConfig {
            max_batches: 1,
            max_bytes: u64::MAX,
            overflow: OverflowPolicy::Block,
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");

        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            telemetry,
            store_config,
            write_config,
            shutdown_rx,
            Arc::new(AtomicU64::new(0)),
        ));

        // Batch 1: fills the queue's one slot and is reserved by the retry loop.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", 1.0)],
                },
                TraceContext::new_root().into(),
            ))
            .await
            .expect("receiver should still be alive");
        handles.attempted.recv().await.expect("the first attempt should have happened");

        // Batch 2: received by drain_inbox, which then blocks forever in `queue.push`.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", 2.0)],
                },
                TraceContext::new_root().into(),
            ))
            .await
            .expect("receiver should still be alive");
        // Let drain_inbox block on batch 2 first, so batch 3 is the one left in the channel.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Batch 3: stays unread in the channel.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", 3.0)],
                },
                TraceContext::new_root().into(),
            ))
            .await
            .expect("receiver should still be alive");

        shutdown_tx.send(true).expect("receiver should still be alive");
        drop(inbox_tx);

        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run_output should not hang")
            .expect("task should not panic")
            .expect("shutdown-grace expiry should end run_output with Ok, not Err");

        let dropped_for_shutdown = Totals::of(registry.drain(0)).sum(
            "logit.component.batches.dropped",
            &[("component", "out"), ("reason", "shutdown")],
        );

        assert_eq!(
            dropped_for_shutdown, 3.0,
            "batch 1 (left reserved in the queue), batch 2 (parked in the abandoned push), and \
             batch 3 (left in the inbox) must each be counted as dropped, never lost uncounted"
        );
    }

    fn counter_value_of(batch: &EventBatch) -> f64 {
        match batch.events[0].metrics.iter().next().map(|m| &m.kind) {
            Some(MetricKind::Sum(s)) => s.value,
            other => panic!("expected exactly one counter metric, got {other:?}"),
        }
    }

    /// `run_output`'s shutdown sweep doesn't hang pushing into a full, `Block` disk spool.
    #[tokio::test]
    async fn a_disk_backed_sinks_shutdown_sweep_does_not_hang_pushing_into_a_full_spool() {
        let dir = crate::disk_queue::test_support::scratch_dir("shutdown-sweep-full-spool");

        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (output, mut handles) = faulty_output(Fault::Clean, u32::MAX);
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            shutdown_grace: Duration::from_millis(100),
            delivery_override: None,
            ..WriteLoopConfig::default()
        };

        // Every counter batch encodes to the same length, so this sizes the spool to one record.
        let sample_batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 0.0)],
        };
        let one_record_len = crate::disk_queue::test_support::encoded_record_len(
            &sample_batch,
            logit_core::Provenance::default(),
        );

        let store_config = SinkStoreConfig::Disk(crate::disk_queue::DiskQueueConfig {
            dir: dir.clone(),
            max_bytes: one_record_len,
            segment_bytes: one_record_len * 10, // no rotation needed for this scenario
            overflow: OverflowPolicy::Block,
            compression: logit_proto::frame::Compression::None,
            checkpoint_interval: Duration::from_secs(3600),
        });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "influxdb_out", "sink");

        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            telemetry,
            store_config,
            write_config,
            shutdown_rx,
            Arc::new(AtomicU64::new(0)),
        ));

        // Batch 1: fills the spool and is reserved by the retry loop.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", 1.0)],
                },
                TraceContext::new_root().into(),
            ))
            .await
            .expect("receiver should still be alive");
        handles.attempted.recv().await.expect("the first attempt should have happened");

        // Batch 2: received by drain_inbox, which then blocks forever in `queue.push`.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", 2.0)],
                },
                TraceContext::new_root().into(),
            ))
            .await
            .expect("receiver should still be alive");
        // `drain_inbox` counts a batch received before its push, which parks on the full
        // spool; waiting for batch 2's count keeps batch 3 in the channel.
        probe
            .wait_for("batch 2 received by drain_inbox", |t| {
                t.sum("logit.component.batches.received", &[]) >= 2.0
            })
            .await;

        // Batch 3: stays unread in the channel for the sweep.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", 3.0)],
                },
                TraceContext::new_root().into(),
            ))
            .await
            .expect("receiver should still be alive");

        shutdown_tx.send(true).expect("receiver should still be alive");
        drop(inbox_tx);

        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run_output should not hang")
            .expect("task should not panic")
            .expect("shutdown-grace expiry should end run_output with Ok, not Err");

        let dropped_for_shutdown = probe.sum(
            "logit.component.batches.dropped",
            &[("component", "out"), ("reason", "shutdown")],
        );
        assert_eq!(
            dropped_for_shutdown, 0.0,
            "a disk-backed sink drops nothing at shutdown -- everything still queued at shutdown \
             must survive, not be counted dropped"
        );

        // Every batch survives, in order: batch 2, parked in the abandoned `push`, too.
        let reopened = crate::disk_queue::DiskQueue::open(
            crate::disk_queue::DiskQueueConfig {
                dir: dir.clone(),
                max_bytes: u64::MAX,
                segment_bytes: one_record_len * 10,
                overflow: OverflowPolicy::Block,
                compression: logit_proto::frame::Compression::None,
                checkpoint_interval: Duration::from_secs(3600),
            },
            logit_core::Telemetry::default(),
            Diagnostics::new("test"),
        )
        .unwrap();
        reopened.close();
        let mut spooled = Vec::new();
        while let Some((batch, ..)) = reopened.peek().await {
            spooled.push(counter_value_of(&batch));
            reopened.commit().unwrap();
        }
        assert_eq!(spooled, vec![1.0, 2.0, 3.0]);

        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // Every `run_output` exit path accounts for every batch (DISK-09)
    // -----------------------------------------------------------------------------------------

    /// Sleeps `delay` per send, then fails with `fail` if set, else succeeds and counts it.
    struct PacedOutput {
        delay: Duration,
        fail: Option<Fault>,
        delivered: Arc<std::sync::atomic::AtomicU64>,
    }

    #[async_trait::async_trait]
    impl Output for PacedOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            tokio::time::sleep(self.delay).await;
            match self.fail {
                Some(fault) => Err(anyhow::anyhow!("simulated {fault:?} failure")).context(fault),
                None => {
                    self.delivered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Ok(())
                }
            }
        }
    }

    fn counter_batch(value: f64) -> Delivered {
        Delivered::Owned(
            EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", value)],
            },
            TraceContext::new_root().into(),
        )
    }

    /// The on-disk length of one [`counter_batch`] record.
    fn one_counter_record_len() -> u64 {
        let Delivered::Owned(batch, _) = counter_batch(0.0) else { unreachable!() };
        crate::disk_queue::test_support::encoded_record_len(
            &batch,
            logit_core::Provenance::default(),
        )
    }

    fn disk_store_config(
        dir: &std::path::Path,
        max_bytes: u64,
    ) -> crate::disk_queue::DiskQueueConfig {
        crate::disk_queue::DiskQueueConfig {
            dir: dir.to_path_buf(),
            max_bytes,
            segment_bytes: 2 * one_counter_record_len(),
            overflow: OverflowPolicy::Block,
            compression: logit_proto::frame::Compression::None,
            checkpoint_interval: Duration::from_secs(3600),
        }
    }

    /// Reopens the spool at `dir` and drains it, returning each batch's counter value in order.
    async fn reopen_and_drain(dir: &std::path::Path) -> Vec<f64> {
        let reopened = crate::disk_queue::DiskQueue::open(
            disk_store_config(dir, u64::MAX),
            Telemetry::default(),
            Diagnostics::new("test"),
        )
        .unwrap();
        reopened.close();
        let mut spooled = Vec::new();
        while let Some((batch, ..)) = reopened.peek().await {
            spooled.push(counter_value_of(&batch));
            reopened.commit().unwrap();
        }
        spooled
    }

    fn slow_retry_write_config(grace: Duration) -> WriteLoopConfig {
        WriteLoopConfig {
            retry: RetryConfig {
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            shutdown_grace: grace,
            delivery_override: None,
            ..WriteLoopConfig::default()
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum ExitPath {
        /// The inbox closes while a slow sink is still delivering: `drain_inbox` finishes first.
        DrainFirst,
        /// The sink fails forever; shutdown grace expires with the store full and a push parked.
        GraceExpiry,
        /// The sink never completes a send; shutdown grace cuts the in-flight send off with the
        /// store full and a push parked.
        GraceCutsInFlightSend,
        /// [`ExitPath::GraceCutsInFlightSend`], cut during a submit past the head under a window:
        /// the head is submitted and the second batch's submit never finishes. A sink with no
        /// window never completes its send, as on that path.
        GraceCutsInFlightSubmit,
        /// The sink rejects every batch (`Fault::Rejected`): each is dropped at once, and
        /// `write_loop` sees closed and empty.
        Rejected,
        /// The inbox closes and the sink delivers everything: `write_loop` sees closed and empty.
        ClosedAndEmpty,
    }

    /// The sink's side of `docs/adr/shutdown-accounting-and-cancellation-safety.md`'s decision 1,
    /// read from telemetry: `received == delivered + Σ dropped{reason} + spooled`, with the
    /// `drain complete` total equal to `dropped{reason="shutdown"}`.
    #[tokio::test(start_paused = true)]
    async fn every_run_output_exit_path_reconciles_received_against_delivered_dropped_and_spooled()
    {
        let output = |path: ExitPath, delivered: &Arc<AtomicU64>| {
            let paced = |delay, fail| {
                Box::new(PacedOutput { delay, fail, delivered: Arc::clone(delivered) })
                    as Box<dyn Output + Send>
            };
            match path {
                ExitPath::DrainFirst => paced(Duration::from_millis(10), None),
                ExitPath::GraceExpiry => paced(Duration::from_millis(10), Some(Fault::Clean)),
                ExitPath::GraceCutsInFlightSend | ExitPath::GraceCutsInFlightSubmit => {
                    Box::new(NeverOutput)
                }
                ExitPath::Rejected => paced(Duration::from_millis(10), Some(Fault::Rejected)),
                ExitPath::ClosedAndEmpty => paced(Duration::ZERO, None),
            }
        };
        reconcile_every_exit_path(output, 1.0).await;
    }

    /// [`every_run_output_exit_path_reconciles_received_against_delivered_dropped_and_spooled`]
    /// with a windowed sink: each path's `send` behavior moves to `await_ack`, and a grace cut
    /// leaves both batches the small store holds submitted and unacknowledged.
    #[tokio::test(start_paused = true)]
    async fn every_run_output_exit_path_reconciles_with_a_window_in_flight() {
        let output = |path: ExitPath, delivered: &Arc<AtomicU64>| {
            let windowed = |delay, ack: AckScript, submit_delays| {
                let (mut output, log) = windowed_output((4, 4));
                output.ack_delay = delay;
                output.ack_script = ack;
                output.submit_delays = submit_delays;
                output.delivered = Arc::clone(delivered);
                drop(log);
                Box::new(output) as Box<dyn Output + Send>
            };
            // On a grace cut, the head's 1 ms submit holds the fill until `drain_inbox` has
            // pushed the second batch: a disk store reads and writes on blocking threads, so the
            // fill could otherwise find it not yet readable, stop at the head, and leave one
            // batch cut off instead of two. Pending blocking I/O holds the paused clock, so the
            // delay can't fire before that push completes.
            let head = (1, Duration::from_millis(1));
            match path {
                ExitPath::DrainFirst => {
                    windowed(Duration::from_millis(10), AckScript::Ok, Vec::new())
                }
                ExitPath::GraceExpiry => {
                    windowed(Duration::from_millis(10), AckScript::Always(Fault::Clean), Vec::new())
                }
                ExitPath::GraceCutsInFlightSend => {
                    windowed(Duration::ZERO, AckScript::Hang, vec![head])
                }
                ExitPath::GraceCutsInFlightSubmit => windowed(
                    Duration::ZERO,
                    AckScript::Hang,
                    vec![head, (2, Duration::from_secs(3600))],
                ),
                ExitPath::Rejected => windowed(
                    Duration::from_millis(10),
                    AckScript::Always(Fault::Rejected),
                    Vec::new(),
                ),
                ExitPath::ClosedAndEmpty => windowed(Duration::ZERO, AckScript::Ok, Vec::new()),
            }
        };
        reconcile_every_exit_path(output, 2.0).await;
    }

    /// The matrix behind both exit-path tests. `cut_off` is how many batches a grace cut leaves
    /// with an unknown outcome on a small store: what a disk store under at-most-once counts as
    /// shutdown drops.
    async fn reconcile_every_exit_path(
        make_output: impl Fn(ExitPath, &Arc<AtomicU64>) -> Box<dyn Output + Send>,
        cut_off: f64,
    ) {
        const SENT: u64 = 6;
        for path in [
            ExitPath::DrainFirst,
            ExitPath::GraceExpiry,
            ExitPath::GraceCutsInFlightSend,
            ExitPath::GraceCutsInFlightSubmit,
            ExitPath::Rejected,
            ExitPath::ClosedAndEmpty,
        ] {
            for posture in [DeliveryPosture::AtLeastOnce, DeliveryPosture::AtMostOnce] {
                for disk in [false, true] {
                    let at = format!("{path:?}, {posture:?}, disk={disk}");
                    let dir = crate::disk_queue::test_support::scratch_dir("exit-path-reconcile");
                    // Two batches fill the store on the paths that leave some undelivered, so
                    // the rest wait in `drain_inbox`'s parked push and in the inbox.
                    let small = matches!(
                        path,
                        ExitPath::GraceExpiry
                            | ExitPath::GraceCutsInFlightSend
                            | ExitPath::GraceCutsInFlightSubmit
                    );
                    let store_config = match (disk, small) {
                        (true, true) => SinkStoreConfig::Disk(disk_store_config(
                            &dir,
                            2 * one_counter_record_len(),
                        )),
                        (true, false) => SinkStoreConfig::Disk(disk_store_config(&dir, u64::MAX)),
                        (false, true) => SinkStoreConfig::Memory(SinkQueueConfig {
                            max_batches: 2,
                            max_bytes: u64::MAX,
                            overflow: OverflowPolicy::Block,
                        }),
                        (false, false) => SinkStoreConfig::Memory(SinkQueueConfig::default()),
                    };
                    let double_delivered = Arc::new(AtomicU64::new(0));
                    let output = make_output(path, &double_delivered);
                    let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
                    let (shutdown_tx, shutdown_rx) = watch::channel(false);
                    let registry = Registry::new();
                    let drain_total = Arc::new(AtomicU64::new(0));
                    let run = tokio::spawn(run_output(
                        "out".to_string(),
                        output,
                        inbox_rx,
                        registry.telemetry_for("out", "influxdb_out", "sink"),
                        store_config,
                        WriteLoopConfig {
                            delivery_override: Some(posture),
                            ..slow_retry_write_config(Duration::from_millis(100))
                        },
                        shutdown_rx,
                        Arc::clone(&drain_total),
                    ));

                    for value in 1..=SENT {
                        inbox_tx.send(counter_batch(value as f64)).await.unwrap();
                    }
                    // Lets `drain_inbox` fill the store and park on its next push.
                    tokio::time::sleep(Duration::from_millis(5)).await;
                    match path {
                        ExitPath::DrainFirst | ExitPath::Rejected | ExitPath::ClosedAndEmpty => {
                            drop(inbox_tx);
                        }
                        ExitPath::GraceExpiry
                        | ExitPath::GraceCutsInFlightSend
                        | ExitPath::GraceCutsInFlightSubmit => {
                            shutdown_tx.send(true).unwrap();
                            drop(inbox_tx);
                        }
                    }

                    let result = tokio::time::timeout(Duration::from_secs(600), run)
                        .await
                        .unwrap_or_else(|_| panic!("{at}: run_output stopped responding"))
                        .expect("the task must not panic");
                    assert!(result.is_ok(), "{at}: exit result {result:?}");
                    drop(shutdown_tx);

                    let totals = Totals::of(registry.drain(0));
                    let count = |name: &str| totals.sum(name, &[("component", "out")]);
                    let received = count("logit.component.batches.received");
                    let delivered = count("logit.component.batches.delivered");
                    let rejected = totals.sum(
                        "logit.component.batches.dropped",
                        &[("component", "out"), ("reason", "rejected")],
                    );
                    let shutdown = totals.sum(
                        "logit.component.batches.dropped",
                        &[("component", "out"), ("reason", "shutdown")],
                    );
                    let spooled =
                        if disk { reopen_and_drain(&dir).await.len() as f64 } else { 0.0 };
                    assert_eq!(received, SENT as f64, "{at}: every batch sent is received");
                    assert_eq!(
                        received,
                        delivered + rejected + shutdown + spooled,
                        "{at}: received == delivered ({delivered}) + rejected ({rejected}) \
                         + shutdown ({shutdown}) + spooled ({spooled})"
                    );
                    assert_eq!(
                        drain_total.load(std::sync::atomic::Ordering::Relaxed) as f64,
                        shutdown,
                        "{at}: drain complete's total is the shutdown drop count"
                    );
                    assert_eq!(
                        delivered,
                        double_delivered.load(std::sync::atomic::Ordering::SeqCst) as f64,
                        "{at}: batches.delivered agrees with the sink's own count"
                    );
                    // A disk sink drops for shutdown only what the grace cut off, and only under
                    // at-most-once. On `GraceExpiry`, each attempt is a 10 ms send and a 10 ms
                    // backoff from the first push, and the deadline lands 105 ms in, during the
                    // send started at 100 ms.
                    if disk {
                        let cut_off = match path {
                            ExitPath::GraceExpiry
                            | ExitPath::GraceCutsInFlightSend
                            | ExitPath::GraceCutsInFlightSubmit => cut_off,
                            ExitPath::DrainFirst
                            | ExitPath::Rejected
                            | ExitPath::ClosedAndEmpty => 0.0,
                        };
                        let expected = match posture {
                            DeliveryPosture::AtMostOnce => cut_off,
                            DeliveryPosture::AtLeastOnce => 0.0,
                        };
                        assert_eq!(shutdown, expected, "{at}");
                    }
                    match path {
                        ExitPath::DrainFirst | ExitPath::ClosedAndEmpty => {
                            assert_eq!(delivered, SENT as f64, "{at}")
                        }
                        ExitPath::GraceExpiry
                        | ExitPath::GraceCutsInFlightSend
                        | ExitPath::GraceCutsInFlightSubmit => {
                            assert_eq!(delivered, 0.0, "{at}")
                        }
                        ExitPath::Rejected => {
                            assert_eq!(delivered, 0.0, "{at}");
                            assert_eq!(rejected, SENT as f64, "{at}: each dropped at once");
                        }
                    }
                    std::fs::remove_dir_all(&dir).ok();
                }
            }
        }
    }

    // -----------------------------------------------------------------------------------------
    // A window of batches in flight (`docs/adr/native-hop-send-window.md`, decision 4)
    // -----------------------------------------------------------------------------------------

    /// One call [`WindowedOutput`] saw, by the batch's counter value (`1.0` is the first pushed,
    /// which the memory store also numbers 1).
    #[derive(Debug, Clone, PartialEq)]
    enum Call {
        /// `observe_batch`, by the sequence number the store gave the batch.
        Observe(u64),
        /// `send`, the fast path.
        Send(u64),
        /// `submit(batch, _, seq)`: `(value, seq, in_flight)`, `in_flight` the batches the sink
        /// held unacknowledged when it was called.
        Submit(u64, u64, usize),
        /// An `await_ack` that delivered this batch.
        Ack(u64),
    }

    /// What every `await_ack` with a batch outstanding does, after `ack_delay`.
    #[derive(Debug, Clone)]
    enum AckScript {
        Ok,
        /// One result per call, `None` delivering; `Ok` once the list runs out.
        Calls(std::collections::VecDeque<Option<Fault>>),
        /// Fails while this batch is the oldest outstanding; delivers any other.
        Head(u64, Fault),
        /// Fails this batch alone, `Rejected` marked `HeadOnly`, as `logit_out` does for a
        /// rejected `Ack`: pops it and keeps the connection and the batches behind it.
        HeadOnly(u64),
        Always(Fault),
        Hang,
    }

    /// A windowed sink with the contract `logit_out` has: `window()` reads `windows.0` until a
    /// submission connects it and `windows.1` after, and every dropped connection goes back to
    /// `windows.0`. A failed `submit` with nothing in flight drops the connection; with batches in
    /// flight it marks it broken, failing every later submit until the acks already owed are
    /// read. A failed or cancelled `await_ack` drops it with every batch in flight. `send` is
    /// `submit` then `await_ack`.
    struct WindowedOutput {
        windows: (usize, usize),
        connected: bool,
        broken: bool,
        /// Batches submitted and not acknowledged, oldest first.
        in_flight: std::collections::VecDeque<u64>,
        /// The `seq` the last `observe_batch` saw, for `send`.
        pending_seq: Option<SeqId>,
        /// One result per `submit` call, `None` succeeding; success once the list runs out.
        submit_script: std::collections::VecDeque<Option<Fault>>,
        /// Every submit of this batch fails with this fault.
        submit_fails: Option<(u64, Fault)>,
        /// Every submit of each listed batch takes its duration before it writes.
        submit_delays: Vec<(u64, Duration)>,
        ack_script: AckScript,
        ack_delay: Duration,
        log: Arc<std::sync::Mutex<Vec<Call>>>,
        /// The most batches in flight at once.
        peak: Arc<std::sync::atomic::AtomicUsize>,
        delivered: Arc<AtomicU64>,
    }

    struct WindowedLog {
        log: Arc<std::sync::Mutex<Vec<Call>>>,
        peak: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl WindowedLog {
        fn calls(&self) -> Vec<Call> {
            self.log.lock().unwrap().clone()
        }
        fn submits(&self) -> Vec<(u64, usize)> {
            self.calls()
                .into_iter()
                .filter_map(|c| match c {
                    Call::Submit(v, _, in_flight) => Some((v, in_flight)),
                    _ => None,
                })
                .collect()
        }
        fn count(&self, call: &Call) -> usize {
            self.calls().iter().filter(|c| *c == call).count()
        }
    }

    fn windowed_output(windows: (usize, usize)) -> (WindowedOutput, WindowedLog) {
        let log = Arc::new(std::sync::Mutex::new(Vec::new()));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        (
            WindowedOutput {
                windows,
                connected: false,
                broken: false,
                in_flight: std::collections::VecDeque::new(),
                pending_seq: None,
                submit_script: std::collections::VecDeque::new(),
                submit_fails: None,
                submit_delays: Vec::new(),
                ack_script: AckScript::Ok,
                ack_delay: Duration::ZERO,
                log: Arc::clone(&log),
                peak: Arc::clone(&peak),
                delivered: Arc::new(AtomicU64::new(0)),
            },
            WindowedLog { log, peak },
        )
    }

    impl WindowedOutput {
        fn record(&self, call: Call) {
            self.log.lock().unwrap().push(call);
        }

        fn drop_connection(&mut self) {
            self.in_flight.clear();
            self.connected = false;
            self.broken = false;
        }

        fn submit_value(&mut self, value: u64) -> anyhow::Result<()> {
            let scripted = self.submit_script.pop_front().flatten();
            let fault = match self.submit_fails {
                Some((v, fault)) if v == value => Some(fault),
                _ if self.broken => Some(Fault::Clean),
                _ => scripted,
            };
            if let Some(fault) = fault {
                if self.in_flight.is_empty() {
                    self.drop_connection();
                } else {
                    self.broken = true;
                }
                return Err(anyhow::anyhow!("simulated {fault:?} submit")).context(fault);
            }
            self.connected = true;
            self.in_flight.push_back(value);
            self.peak.fetch_max(self.in_flight.len(), std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    fn value_of(batch: &EventBatch) -> u64 {
        counter_value_of(batch) as u64
    }

    #[async_trait::async_trait]
    impl Output for WindowedOutput {
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            self.record(Call::Send(value_of(batch)));
            self.submit_value(value_of(batch))?;
            self.await_ack().await
        }

        fn observe_batch(&mut self, _ctx: BatchContext, seq: SeqId) {
            self.record(Call::Observe(seq.seq));
            self.pending_seq = Some(seq);
        }

        fn window(&self) -> usize {
            if self.connected {
                self.windows.1
            } else {
                self.windows.0
            }
        }

        async fn submit(
            &mut self,
            batch: &EventBatch,
            _ctx: BatchContext,
            seq: SeqId,
        ) -> anyhow::Result<()> {
            let value = value_of(batch);
            self.record(Call::Submit(value, seq.seq, self.in_flight.len()));
            if let Some(&(_, delay)) = self.submit_delays.iter().find(|(v, _)| *v == value) {
                tokio::time::sleep(delay).await;
            }
            self.submit_value(value)
        }

        async fn await_ack(&mut self) -> anyhow::Result<()> {
            let Some(&head) = self.in_flight.front() else {
                return Ok(());
            };
            // A cancelled wait drops the connection, as the contract says.
            struct DropOnCancel<'a>(Option<&'a mut WindowedOutput>);
            impl Drop for DropOnCancel<'_> {
                fn drop(&mut self) {
                    if let Some(output) = self.0.take() {
                        output.drop_connection();
                    }
                }
            }
            let mut guard = DropOnCancel(Some(self));
            let output = guard.0.as_deref_mut().expect("set above");
            tokio::time::sleep(output.ack_delay).await;
            if matches!(output.ack_script, AckScript::HeadOnly(v) if v == head) {
                let output = guard.0.take().expect("set above");
                output.in_flight.pop_front();
                return Err(anyhow::anyhow!("simulated rejected head {head}"))
                    .context(Fault::Rejected)
                    .context(HeadOnly);
            }
            let fault = match &mut output.ack_script {
                AckScript::Ok | AckScript::HeadOnly(_) => None,
                AckScript::Calls(calls) => calls.pop_front().flatten(),
                AckScript::Head(v, fault) => (*v == head).then_some(*fault),
                AckScript::Always(fault) => Some(*fault),
                AckScript::Hang => std::future::pending().await,
            };
            let output = guard.0.take().expect("set above");
            if let Some(fault) = fault {
                output.drop_connection();
                return Err(anyhow::anyhow!("simulated {fault:?} ack")).context(fault);
            }
            output.in_flight.pop_front();
            if output.in_flight.is_empty() && output.broken {
                output.drop_connection();
            }
            output.record(Call::Ack(head));
            output.delivered.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    /// Runs `write_loop` until it has drained batches `1..=n`, pushed into a memory store of
    /// `max_batches` while it runs and then closed, under `posture`.
    async fn drive_windowed(
        output: &mut WindowedOutput,
        n: u64,
        max_batches: usize,
        posture: DeliveryPosture,
    ) -> Totals {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("out", "logit_out", "sink");
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig { max_batches, max_bytes: u64::MAX, overflow: OverflowPolicy::Block },
            telemetry.clone(),
        )));
        let producer = async {
            for value in 1..=n {
                store.push((one_event_batch(value as f64), BatchContext::default())).await;
            }
            store.close();
        };
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let config = WriteLoopConfig {
            retry: fast_retry_config(),
            delivery_override: Some(posture),
            ..WriteLoopConfig::default()
        };
        let shutdown_dropped = AtomicU64::new(0);
        let write = write_loop(
            "out".to_string(),
            output,
            Arc::clone(&store),
            telemetry,
            config,
            shutdown_rx,
            &shutdown_dropped,
        );
        tokio::time::timeout(Duration::from_secs(60), async { tokio::join!(producer, write) })
            .await
            .expect("write_loop drains a closed store");
        Totals::of(registry.drain(0))
    }

    /// Batches the write loop dropped for a send's outcome, under either drop reason.
    fn sent_failed(totals: &Totals) -> f64 {
        totals.sum("logit.component.batches.dropped", &[("reason", "rejected")])
            + totals.sum("logit.component.batches.dropped", &[("reason", "ambiguous_at_most_once")])
    }

    #[tokio::test(start_paused = true)]
    async fn a_window_has_every_batch_submitted_before_the_first_ack() {
        let (mut output, log) = windowed_output((4, 4));
        output.ack_delay = Duration::from_millis(10);
        let totals = drive_windowed(&mut output, 3, 1024, DeliveryPosture::AtLeastOnce).await;
        assert_eq!(
            log.calls(),
            vec![
                Call::Observe(1),
                Call::Submit(1, 1, 0),
                Call::Observe(2),
                Call::Submit(2, 2, 1),
                Call::Observe(3),
                Call::Submit(3, 3, 2),
                Call::Ack(1),
                Call::Ack(2),
                Call::Ack(3),
            ]
        );
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 3.0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_window_of_one_calls_send_and_never_submit() {
        let (mut output, log) = windowed_output((1, 1));
        let totals = drive_windowed(&mut output, 3, 1024, DeliveryPosture::AtLeastOnce).await;
        assert_eq!(
            log.calls(),
            vec![
                Call::Observe(1),
                Call::Send(1),
                Call::Ack(1),
                Call::Observe(2),
                Call::Send(2),
                Call::Ack(2),
                Call::Observe(3),
                Call::Send(3),
                Call::Ack(3),
            ]
        );
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 3.0);
    }

    /// `logit_out`'s shape: a window of 1 until the first connection, then 4. The first head
    /// goes through `send`, which connects; the rest are submitted.
    #[tokio::test(start_paused = true)]
    async fn a_sink_whose_window_opens_after_its_first_connection_sends_the_first_head_and_submits_the_rest(
    ) {
        let (mut output, log) = windowed_output((1, 4));
        let totals = drive_windowed(&mut output, 3, 1024, DeliveryPosture::AtLeastOnce).await;
        assert_eq!(
            log.calls(),
            vec![
                Call::Observe(1),
                Call::Send(1),
                Call::Ack(1),
                Call::Observe(2),
                Call::Submit(2, 2, 0),
                Call::Observe(3),
                Call::Submit(3, 3, 1),
                Call::Ack(2),
                Call::Ack(3),
            ]
        );
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 3.0);
    }

    #[tokio::test(start_paused = true)]
    async fn observe_batch_runs_once_per_batch_under_a_window_across_resubmits() {
        let (mut output, log) = windowed_output((4, 4));
        output.ack_script = AckScript::Calls([Some(Fault::Ambiguous)].into());
        let totals = drive_windowed(&mut output, 3, 1024, DeliveryPosture::AtLeastOnce).await;
        for value in 1..=3 {
            assert_eq!(log.count(&Call::Observe(value)), 1, "batch {value} observed once");
            let submits = log.submits().iter().filter(|(v, _)| *v == value).count();
            assert_eq!(submits, 2, "batch {value} submitted, then resubmitted after the fault");
        }
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 3.0);
    }

    /// Four batches in flight; the first is acknowledged, then the wait for the second fails.
    fn ack_then_fault(fault: Fault) -> (WindowedOutput, WindowedLog) {
        let (mut output, log) = windowed_output((4, 4));
        output.ack_script = AckScript::Calls([None, Some(fault)].into());
        (output, log)
    }

    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_await_mid_window_under_at_least_once_resubmits_every_unacked_batch_in_order(
    ) {
        let (mut output, log) = ack_then_fault(Fault::Ambiguous);
        let totals = drive_windowed(&mut output, 4, 1024, DeliveryPosture::AtLeastOnce).await;
        assert_eq!(
            log.submits(),
            vec![(1, 0), (2, 1), (3, 2), (4, 3), (2, 0), (3, 1), (4, 2)],
            "the unacknowledged 2, 3, 4 go out again, in order, from position 0"
        );
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 4.0);
        assert_eq!(sent_failed(&totals), 0.0);
        assert_eq!(totals.sum("logit.component.retries", &[]), 1.0);
    }

    #[tokio::test(start_paused = true)]
    async fn an_ambiguous_await_mid_window_under_at_most_once_drops_and_counts_every_unacked_batch()
    {
        let (mut output, log) = ack_then_fault(Fault::Ambiguous);
        let totals = drive_windowed(&mut output, 4, 1024, DeliveryPosture::AtMostOnce).await;
        assert_eq!(log.submits(), vec![(1, 0), (2, 1), (3, 2), (4, 3)], "nothing resubmitted");
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 1.0);
        assert_eq!(sent_failed(&totals), 3.0, "2, 3, and 4 are each as ambiguous as the head");
        assert_eq!(
            totals.sum("logit.component.events.dropped", &[("reason", "ambiguous_at_most_once")]),
            3.0
        );
        assert_eq!(totals.sum("logit.component.retries", &[]), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_clean_await_mid_window_resubmits_under_both_postures() {
        for posture in [DeliveryPosture::AtLeastOnce, DeliveryPosture::AtMostOnce] {
            let (mut output, log) = ack_then_fault(Fault::Clean);
            let totals = drive_windowed(&mut output, 4, 1024, posture).await;
            assert_eq!(
                log.submits(),
                vec![(1, 0), (2, 1), (3, 2), (4, 3), (2, 0), (3, 1), (4, 2)],
                "{posture:?}"
            );
            assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 4.0, "{posture:?}");
            assert_eq!(sent_failed(&totals), 0.0, "{posture:?}");
        }
    }

    /// The third submit fails with two batches in flight: the sink keeps its connection,
    /// broken, and `write_loop` reads the two acks owed before anything is submitted again.
    /// Nothing is classified, so nothing counts as an error.
    #[tokio::test(start_paused = true)]
    async fn a_submit_failure_past_the_head_stops_the_fill_and_drains_the_acks_already_owed() {
        let (mut output, log) = windowed_output((4, 4));
        output.submit_script = [None, None, Some(Fault::Clean)].into();
        let totals = drive_windowed(&mut output, 4, 1024, DeliveryPosture::AtLeastOnce).await;
        assert_eq!(
            log.calls(),
            vec![
                Call::Observe(1),
                Call::Submit(1, 1, 0),
                Call::Observe(2),
                Call::Submit(2, 2, 1),
                Call::Observe(3),
                Call::Submit(3, 3, 2),
                Call::Ack(1),
                // Still broken: the fill stops again, and the last ack owed is read.
                Call::Submit(3, 3, 1),
                Call::Ack(2),
                // Connection dropped once drained; 3 and 4 go out on the next one.
                Call::Submit(3, 3, 0),
                Call::Observe(4),
                Call::Submit(4, 4, 1),
                Call::Ack(3),
                Call::Ack(4),
            ]
        );
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 4.0);
        assert_eq!(totals.sum("logit.component.errors", &[]), 0.0);
        assert_eq!(totals.sum("logit.component.retries", &[]), 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_rejected_submit_past_the_head_drops_that_batch_only_once_it_is_the_head() {
        let (mut output, log) = windowed_output((4, 4));
        output.submit_fails = Some((2, Fault::Rejected));
        let totals = drive_windowed(&mut output, 3, 1024, DeliveryPosture::AtLeastOnce).await;
        assert_eq!(
            log.calls(),
            vec![
                Call::Observe(1),
                Call::Submit(1, 1, 0),
                Call::Observe(2),
                Call::Submit(2, 2, 1),
                Call::Ack(1),
                // At the head now: its own fault, dropped with no retry.
                Call::Submit(2, 2, 0),
                Call::Observe(3),
                Call::Submit(3, 3, 0),
                Call::Ack(3),
            ]
        );
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 2.0);
        assert_eq!(sent_failed(&totals), 1.0);
        assert_eq!(totals.sum("logit.component.errors", &[]), 1.0, "only the head's failure");
    }

    #[tokio::test(start_paused = true)]
    async fn the_window_never_exceeds_a_memory_stores_max_batches() {
        let (mut output, log) = windowed_output((8, 8));
        output.ack_delay = Duration::from_millis(1);
        let totals = drive_windowed(&mut output, 6, 2, DeliveryPosture::AtLeastOnce).await;
        assert!(log.submits().iter().all(|&(_, in_flight)| in_flight < 2), "{:?}", log.submits());
        assert_eq!(log.peak.load(std::sync::atomic::Ordering::SeqCst), 2);
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 6.0);
    }

    /// The head's ack fails `Rejected` with three batches in flight, under either posture. Only
    /// the head is dropped: the batches behind it go out again and are delivered.
    #[tokio::test(start_paused = true)]
    async fn a_rejected_head_drops_only_the_head() {
        for posture in [DeliveryPosture::AtLeastOnce, DeliveryPosture::AtMostOnce] {
            let (mut output, log) = windowed_output((4, 4));
            output.ack_script = AckScript::Head(1, Fault::Rejected);
            let totals = drive_windowed(&mut output, 3, 1024, posture).await;
            assert_eq!(
                totals.sum("logit.component.batches.dropped", &[("reason", "rejected")]),
                1.0,
                "{posture:?}"
            );
            assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 2.0, "{posture:?}");
            assert_eq!(log.count(&Call::Ack(2)), 1, "{posture:?}");
            assert_eq!(log.count(&Call::Ack(3)), 1, "{posture:?}");
            assert_eq!(
                log.count(&Call::Observe(2)),
                1,
                "{posture:?}: a resubmit never observes again"
            );
        }
    }

    /// The head's ack fails `Rejected` marked `HeadOnly`, under either posture: only the head is
    /// dropped, and the batches behind it stay submitted and are delivered with no resubmit and
    /// no retry (`Output::await_ack`'s doc).
    #[tokio::test(start_paused = true)]
    async fn a_head_only_rejection_drops_the_head_and_keeps_the_rest_in_flight() {
        for posture in [DeliveryPosture::AtLeastOnce, DeliveryPosture::AtMostOnce] {
            let (mut output, log) = windowed_output((4, 4));
            output.ack_delay = Duration::from_millis(10);
            output.ack_script = AckScript::HeadOnly(2);
            let totals = drive_windowed(&mut output, 4, 1024, posture).await;
            assert_eq!(
                log.calls(),
                vec![
                    Call::Observe(1),
                    Call::Submit(1, 1, 0),
                    Call::Observe(2),
                    Call::Submit(2, 2, 1),
                    Call::Observe(3),
                    Call::Submit(3, 3, 2),
                    Call::Observe(4),
                    Call::Submit(4, 4, 3),
                    Call::Ack(1),
                    Call::Ack(3),
                    Call::Ack(4),
                ],
                "{posture:?}"
            );
            assert_eq!(
                totals.sum("logit.component.batches.dropped", &[("reason", "rejected")]),
                1.0,
                "{posture:?}"
            );
            assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 3.0, "{posture:?}");
            assert_eq!(totals.sum("logit.component.retries", &[]), 0.0, "{posture:?}");
            assert_eq!(totals.sum("logit.component.errors", &[]), 1.0, "{posture:?}");
        }
    }

    /// A head-only rejection off a disk spool commits the head, so a restart doesn't replay it,
    /// and the batches behind it are delivered.
    #[tokio::test(start_paused = true)]
    async fn a_disk_sink_commits_a_head_only_rejection_so_it_never_replays() {
        let dir = crate::disk_queue::test_support::scratch_dir("head-only-is-committed");
        let (mut output, log) = windowed_output((4, 4));
        output.ack_delay = Duration::from_millis(10);
        output.ack_script = AckScript::HeadOnly(2);
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let registry = Registry::new();
        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            registry.telemetry_for("out", "logit_out", "sink"),
            SinkStoreConfig::Disk(disk_store_config(&dir, u64::MAX)),
            slow_retry_write_config(Duration::from_secs(5)),
            shutdown_rx,
            Arc::new(AtomicU64::new(0)),
        ));
        for value in [1.0, 2.0, 3.0] {
            inbox_tx.send(counter_batch(value)).await.unwrap();
        }
        drop(inbox_tx);

        tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("run_output must not stop responding")
            .expect("the task must not panic")
            .expect("a rejection doesn't fail the sink");

        let totals = Totals::of(registry.drain(0));
        assert_eq!(
            totals.sum(
                "logit.component.batches.dropped",
                &[("component", "out"), ("reason", "rejected")]
            ),
            1.0
        );
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 2.0);
        assert_eq!(log.count(&Call::Ack(1)), 1);
        assert_eq!(log.count(&Call::Ack(3)), 1);
        assert!(
            reopen_and_drain(&dir).await.is_empty(),
            "the rejected head is committed, so a restart doesn't replay it"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A slowly draining receiver: the submit past the head takes a minute while the head's
    /// acknowledgment is already there to read. No runtime timeout cuts that submit, so the head
    /// is delivered, not dropped as `Ambiguous`.
    #[tokio::test(start_paused = true)]
    async fn a_slow_submit_past_the_head_is_never_cut_and_the_acknowledged_head_delivers() {
        const SLOW: Duration = Duration::from_secs(60);
        let (mut output, log) = windowed_output((4, 4));
        output.submit_delays = vec![(2, SLOW)];
        let start = tokio::time::Instant::now();
        let totals = drive_windowed(&mut output, 2, 1024, DeliveryPosture::AtMostOnce).await;
        assert!(start.elapsed() >= SLOW, "the submit ran on");
        assert_eq!(
            log.calls(),
            vec![
                Call::Observe(1),
                Call::Submit(1, 1, 0),
                Call::Observe(2),
                Call::Submit(2, 2, 1),
                Call::Ack(1),
                Call::Ack(2),
            ]
        );
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 2.0);
        assert_eq!(sent_failed(&totals), 0.0);
        assert_eq!(totals.sum("logit.component.errors", &[]), 0.0);
    }

    /// Three batches submitted; the head's acknowledgment and the grace deadline land on the
    /// same instant, so the `biased` deliver arm delivers the head and two stay outstanding. The
    /// loop-top `select!` that follows is unbiased with both arms ready: the grace arm returns
    /// from it, and the store's arm runs `deliver_window`, whose grace check returns
    /// `GraceExpired`. Repeated so both arms are taken; each must apply the cut-off rule.
    #[tokio::test(start_paused = true)]
    async fn a_grace_that_wins_the_loop_top_select_with_frames_outstanding_still_applies_the_cut_off_rule(
    ) {
        let grace = Duration::from_millis(100);
        for posture in [DeliveryPosture::AtMostOnce, DeliveryPosture::AtLeastOnce] {
            for round in 0..16 {
                let at = format!("{posture:?}, round {round}");
                let (mut output, log) = windowed_output((4, 4));
                output.ack_delay = grace;
                let registry = Registry::new();
                let telemetry = registry.telemetry_for("out", "logit_out", "sink");
                let store = Arc::new(SinkStore::Memory(SinkQueue::new(
                    SinkQueueConfig::default(),
                    telemetry.clone(),
                )));
                for value in 1..=3 {
                    store.push((one_event_batch(value as f64), BatchContext::default())).await;
                }
                let (shutdown_tx, shutdown_rx) = watch::channel(false);
                shutdown_tx.send(true).unwrap();
                let shutdown_dropped = AtomicU64::new(0);
                let config = WriteLoopConfig {
                    retry: fast_retry_config(),
                    shutdown_grace: grace,
                    delivery_override: Some(posture),
                    ..WriteLoopConfig::default()
                };
                write_loop(
                    "out".to_string(),
                    &mut output,
                    Arc::clone(&store),
                    telemetry,
                    config,
                    shutdown_rx,
                    &shutdown_dropped,
                )
                .await;
                assert_eq!(log.submits(), vec![(1, 0), (2, 1), (3, 2)], "{at}");
                assert_eq!(log.count(&Call::Ack(1)), 1, "{at}: the head's ack won its wake");
                let totals = Totals::of(registry.drain(0));
                assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 1.0, "{at}");
                let shutdown =
                    totals.sum("logit.component.batches.dropped", &[("reason", "shutdown")]);
                let left = store.finish().await;
                match posture {
                    DeliveryPosture::AtMostOnce => {
                        assert_eq!(shutdown, 2.0, "{at}: both outstanding batches, counted");
                        assert_eq!(left, (0, 0), "{at}: committed, so a disk store can't replay");
                    }
                    DeliveryPosture::AtLeastOnce => {
                        assert_eq!(shutdown, 0.0, "{at}");
                        assert_eq!(left, (2, 2), "{at}: left reserved, for `finish` to count");
                    }
                }
            }
        }
    }

    /// The head and the second batch are submitted; the third's submit is still writing when the
    /// grace cuts it. That batch may have reached the receiver too, so it counts with the two
    /// outstanding.
    #[tokio::test(start_paused = true)]
    async fn a_grace_cut_during_a_submit_past_the_head_counts_that_batch_too_under_at_most_once() {
        for posture in [DeliveryPosture::AtMostOnce, DeliveryPosture::AtLeastOnce] {
            let (mut output, log) = windowed_output((4, 4));
            output.submit_delays = vec![(3, Duration::from_secs(3600))];
            let registry = Registry::new();
            let telemetry = registry.telemetry_for("out", "logit_out", "sink");
            let store = Arc::new(SinkStore::Memory(SinkQueue::new(
                SinkQueueConfig::default(),
                telemetry.clone(),
            )));
            for value in 1..=3 {
                store.push((one_event_batch(value as f64), BatchContext::default())).await;
            }
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            shutdown_tx.send(true).unwrap();
            let shutdown_dropped = AtomicU64::new(0);
            let config = WriteLoopConfig {
                retry: fast_retry_config(),
                shutdown_grace: Duration::from_millis(100),
                delivery_override: Some(posture),
                ..WriteLoopConfig::default()
            };
            write_loop(
                "out".to_string(),
                &mut output,
                Arc::clone(&store),
                telemetry,
                config,
                shutdown_rx,
                &shutdown_dropped,
            )
            .await;
            assert_eq!(log.submits(), vec![(1, 0), (2, 1), (3, 2)], "{posture:?}");
            assert_eq!(log.count(&Call::Ack(1)), 0, "{posture:?}: cut before any ack");
            let totals = Totals::of(registry.drain(0));
            let shutdown = totals.sum("logit.component.batches.dropped", &[("reason", "shutdown")]);
            let left = store.finish().await;
            match posture {
                DeliveryPosture::AtMostOnce => {
                    assert_eq!(shutdown, 3.0, "two outstanding and the one mid-submit");
                    assert_eq!(left, (0, 0), "all three committed");
                }
                DeliveryPosture::AtLeastOnce => {
                    assert_eq!(shutdown, 0.0, "nothing committed before `finish`");
                    assert_eq!(left, (3, 3), "left reserved, for `finish` to count");
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_grace_cut_with_a_window_in_flight_counts_every_outstanding_batch_under_at_most_once_and_leaves_them_under_at_least_once(
    ) {
        for posture in [DeliveryPosture::AtMostOnce, DeliveryPosture::AtLeastOnce] {
            let (mut output, log) = windowed_output((4, 4));
            output.ack_script = AckScript::Hang;
            let mut probe = TelemetryProbe::new();
            let telemetry = probe.telemetry("out", "logit_out", "sink");
            let store = Arc::new(SinkStore::Memory(SinkQueue::new(
                SinkQueueConfig::default(),
                telemetry.clone(),
            )));
            for value in 1..=3 {
                store.push((one_event_batch(value as f64), BatchContext::default())).await;
            }
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            shutdown_tx.send(true).unwrap();
            let shutdown_dropped = AtomicU64::new(0);
            let config = WriteLoopConfig {
                retry: fast_retry_config(),
                shutdown_grace: Duration::from_millis(100),
                delivery_override: Some(posture),
                ..WriteLoopConfig::default()
            };
            write_loop(
                "out".to_string(),
                &mut output,
                Arc::clone(&store),
                telemetry,
                config,
                shutdown_rx,
                &shutdown_dropped,
            )
            .await;
            assert_eq!(log.submits(), vec![(1, 0), (2, 1), (3, 2)], "{posture:?}");
            let shutdown = probe.sum("logit.component.batches.dropped", &[("reason", "shutdown")]);
            let left = store.finish().await;
            match posture {
                DeliveryPosture::AtMostOnce => {
                    assert_eq!(shutdown, 3.0, "every outstanding batch, counted");
                    assert_eq!(shutdown_dropped.load(std::sync::atomic::Ordering::SeqCst), 3);
                    assert_eq!(left, (0, 0), "committed");
                }
                DeliveryPosture::AtLeastOnce => {
                    assert_eq!(shutdown, 0.0, "{posture:?}");
                    assert_eq!(left, (3, 3), "left reserved, for `finish` to count");
                }
            }
        }
    }

    /// The amendment to the fast path: a submit fails past the head, the acks owed are read, and
    /// the sink drops its connection, so `window()` reads 1 with batch 3 observed and not yet
    /// delivered. Batch 3 must go out through `submit` with its own sequence, not `send`, and
    /// must not be observed again.
    #[tokio::test(start_paused = true)]
    async fn a_head_observed_in_an_earlier_window_is_submitted_with_its_own_seq_when_the_window_falls_to_one(
    ) {
        let (mut output, log) = windowed_output((1, 4));
        // `send`'s submit for batch 1, then 2, then 3 fails past the head.
        output.submit_script = [None, None, Some(Fault::Clean)].into();
        let totals = drive_windowed(&mut output, 4, 1024, DeliveryPosture::AtLeastOnce).await;
        assert_eq!(
            log.calls(),
            vec![
                Call::Observe(1),
                Call::Send(1),
                Call::Ack(1),
                Call::Observe(2),
                Call::Submit(2, 2, 0),
                Call::Observe(3),
                Call::Submit(3, 3, 1),
                Call::Ack(2),
                // `window()` reads 1 here: 3 is the head, observed, and goes through `submit`.
                Call::Submit(3, 3, 0),
                Call::Observe(4),
                Call::Submit(4, 4, 1),
                Call::Ack(3),
                Call::Ack(4),
            ]
        );
        assert_eq!(log.count(&Call::Send(3)), 0);
        assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 4.0);
    }

    /// Batch 1 fills a one-record `Block` spool and is reserved by a failing sink; batch 2 is
    /// parked in `drain_inbox`'s push when shutdown grace expires and `drain_inbox` is dropped.
    /// The inbox is empty, so the sweep has only the parked batch, and must spool it.
    #[tokio::test]
    async fn a_batch_parked_in_a_blocked_push_when_the_drain_is_abandoned_is_spooled_by_the_sweep()
    {
        let dir = crate::disk_queue::test_support::scratch_dir("parked-push-spooled");
        let output = PacedOutput {
            delay: Duration::from_millis(10),
            fail: Some(Fault::Clean),
            delivered: Arc::new(AtomicU64::new(0)),
        };
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut probe = TelemetryProbe::new();
        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            probe.telemetry("out", "influxdb_out", "sink"),
            SinkStoreConfig::Disk(disk_store_config(&dir, one_counter_record_len())),
            slow_retry_write_config(Duration::from_millis(100)),
            shutdown_rx,
            Arc::new(AtomicU64::new(0)),
        ));

        inbox_tx.send(counter_batch(1.0)).await.unwrap();
        inbox_tx.send(counter_batch(2.0)).await.unwrap();
        // `drain_inbox` counts a batch received before its push, which parks on the full
        // spool.
        probe
            .wait_for("batch 2 parked in the push", |t| {
                t.sum("logit.component.batches.received", &[]) >= 2.0
            })
            .await;
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run_output must not stop responding")
            .expect("the task must not panic")
            .expect("shutdown-grace expiry ends run_output with Ok");
        drop(inbox_tx);

        assert_eq!(
            probe.sum(
                "logit.component.batches.dropped",
                &[("component", "out"), ("reason", "shutdown")]
            ),
            0.0
        );
        assert_eq!(reopen_and_drain(&dir).await, vec![1.0, 2.0]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Decision 7 of `docs/adr/durable-checkpoint-writes-and-fault-injection.md`: a batch the
    /// destination rejects is committed off a disk spool, counted `rejected`, and never replayed
    /// after a restart.
    #[tokio::test(start_paused = true)]
    async fn a_disk_sink_commits_a_rejected_batch_so_it_never_replays() {
        let dir = crate::disk_queue::test_support::scratch_dir("dropped-is-committed");
        let output = PacedOutput {
            delay: Duration::from_millis(10),
            fail: Some(Fault::Rejected),
            delivered: Arc::new(AtomicU64::new(0)),
        };
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let registry = Registry::new();
        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            registry.telemetry_for("out", "influxdb_out", "sink"),
            SinkStoreConfig::Disk(disk_store_config(&dir, u64::MAX)),
            slow_retry_write_config(Duration::from_secs(5)),
            shutdown_rx,
            Arc::new(AtomicU64::new(0)),
        ));
        inbox_tx.send(counter_batch(1.0)).await.unwrap();
        inbox_tx.send(counter_batch(2.0)).await.unwrap();
        drop(inbox_tx); // the store closes, and ends empty once both are dropped

        tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("run_output must not stop responding")
            .expect("the task must not panic")
            .expect("a rejection doesn't fail the sink");

        assert_eq!(
            Totals::of(registry.drain(0)).sum(
                "logit.component.batches.dropped",
                &[("component", "out"), ("reason", "rejected")]
            ),
            2.0
        );
        assert!(
            reopen_and_drain(&dir).await.is_empty(),
            "a rejected batch is committed, so a restart doesn't replay it"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // `run_with_telemetry`'s join loop: drain every task on the first error instead of aborting
    // (`docs/adr/buffered-sink-delivery.md`)
    // -----------------------------------------------------------------------------------------

    // -----------------------------------------------------------------------------------------
    // Shutdown accounting (`docs/adr/shutdown-accounting-and-cancellation-safety.md`)
    // -----------------------------------------------------------------------------------------

    /// The `fault` tags on every `deliver` span in `events` that carries one.
    fn deliver_fault_tags(events: &[Event]) -> Vec<(SpanStatus, String)> {
        span_events(events)
            .filter(|e| span_op(e) == Some("deliver"))
            .filter_map(|e| {
                let fault = e.attributes.get("fault").and_then(|v| v.as_str())?;
                Some((e.span.as_ref().expect("a span event").status, fault.to_string()))
            })
            .collect()
    }

    /// Runs `run_output` over a `NeverOutput` under `posture`, sends `batches` batches, lets the
    /// first one's send go in flight, then signals shutdown. Returns the registry's telemetry and
    /// the `drain complete` total.
    async fn run_never_delivering_sink(
        store_config: SinkStoreConfig,
        posture: DeliveryPosture,
        batches: u64,
    ) -> (Totals, u64) {
        let registry = Registry::with_span_sampling(1.0);
        let drain_total = Arc::new(AtomicU64::new(0));
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(NeverOutput),
            inbox_rx,
            registry.telemetry_for("out", "influxdb_out", "sink"),
            store_config,
            WriteLoopConfig {
                delivery_override: Some(posture),
                ..slow_retry_write_config(Duration::from_millis(100))
            },
            shutdown_rx,
            Arc::clone(&drain_total),
        ));
        for value in 1..=batches {
            inbox_tx.send(counter_batch(value as f64)).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(5)).await; // the first send is in flight
        shutdown_tx.send(true).unwrap();
        drop(inbox_tx);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run_output ends within its grace")
            .expect("the task must not panic")
            .expect("grace expiry is not a failure");
        (Totals::of(registry.drain(0)), drain_total.load(std::sync::atomic::Ordering::Relaxed))
    }

    #[tokio::test(start_paused = true)]
    async fn a_send_cut_off_by_shutdown_grace_is_committed_and_counted_under_at_most_once() {
        for disk in [false, true] {
            let dir = crate::disk_queue::test_support::scratch_dir("grace-cut-at-most-once");
            let store_config = match disk {
                true => SinkStoreConfig::Disk(disk_store_config(&dir, u64::MAX)),
                false => SinkStoreConfig::Memory(SinkQueueConfig::default()),
            };
            let (totals, drain_total) =
                run_never_delivering_sink(store_config, DeliveryPosture::AtMostOnce, 1).await;

            assert_eq!(
                totals.sum(
                    "logit.component.batches.dropped",
                    &[("component", "out"), ("reason", "shutdown")]
                ),
                1.0,
                "disk={disk}"
            );
            assert_eq!(drain_total, 1, "disk={disk}");
            assert_eq!(
                deliver_fault_tags(&totals.events),
                vec![(SpanStatus::Error, "ambiguous".to_string())],
                "disk={disk}: the cut-off send's span is an ambiguous error"
            );
            if disk {
                assert!(
                    reopen_and_drain(&dir).await.is_empty(),
                    "a batch committed at the grace cut never replays"
                );
            }
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_send_cut_off_by_shutdown_grace_stays_queued_for_replay_under_at_least_once() {
        for disk in [false, true] {
            let dir = crate::disk_queue::test_support::scratch_dir("grace-cut-at-least-once");
            let store_config = match disk {
                true => SinkStoreConfig::Disk(disk_store_config(&dir, u64::MAX)),
                false => SinkStoreConfig::Memory(SinkQueueConfig::default()),
            };
            let (totals, drain_total) =
                run_never_delivering_sink(store_config, DeliveryPosture::AtLeastOnce, 1).await;

            assert_eq!(
                deliver_fault_tags(&totals.events),
                vec![(SpanStatus::Error, "ambiguous".to_string())],
                "disk={disk}"
            );
            if disk {
                assert_eq!(
                    totals.sum(
                        "logit.component.batches.dropped",
                        &[("component", "out"), ("reason", "shutdown")]
                    ),
                    0.0
                );
                assert_eq!(drain_total, 0);
                assert_eq!(reopen_and_drain(&dir).await, vec![1.0], "the batch replays");
            } else {
                // Left uncommitted, so `SinkStore::finish` counts it: once, not twice.
                assert_eq!(
                    totals.sum(
                        "logit.component.batches.dropped",
                        &[("component", "out"), ("reason", "shutdown")]
                    ),
                    1.0
                );
                assert_eq!(drain_total, 1);
            }
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    /// The head a grace-cut delivery leaves reserved, and the batch behind it, are each counted
    /// once by `finish_and_flush`.
    #[tokio::test(start_paused = true)]
    async fn a_head_left_reserved_by_a_grace_cut_delivery_is_dropped_and_counted_by_finish() {
        let (totals, drain_total) = run_never_delivering_sink(
            SinkStoreConfig::Memory(SinkQueueConfig::default()),
            DeliveryPosture::AtLeastOnce,
            2,
        )
        .await;
        assert_eq!(totals.sum("logit.component.batches.received", &[("component", "out")]), 2.0);
        assert_eq!(
            totals.sum(
                "logit.component.batches.dropped",
                &[("component", "out"), ("reason", "shutdown")]
            ),
            2.0
        );
        assert_eq!(
            totals.sum(
                "logit.component.events.dropped",
                &[("component", "out"), ("reason", "shutdown")]
            ),
            2.0
        );
        assert_eq!(drain_total, 2);
    }

    /// A grace that lands in the backoff sleep after a clean failure cuts off no send, so even
    /// under at-most-once the batch stays queued for `finish_and_flush` and carries no
    /// ambiguous fault.
    #[tokio::test(start_paused = true)]
    async fn a_grace_expiring_during_backoff_after_a_clean_failure_leaves_the_batch_uncommitted_under_at_most_once(
    ) {
        let (mut output, mut handles) = faulty_output(Fault::Clean, u32::MAX);
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        store.push((one_event_batch(1.0), TraceContext::new_root().into())).await;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                base_delay: Duration::from_secs(10),
                max_delay: Duration::from_secs(10),
            },
            shutdown_grace: Duration::from_millis(100),
            delivery_override: Some(DeliveryPosture::AtMostOnce),
            ..WriteLoopConfig::default()
        };
        let drain_total = Arc::new(AtomicU64::new(0));
        let store_for_task = Arc::clone(&store);
        let total_for_task = Arc::clone(&drain_total);
        let handle = tokio::spawn(async move {
            write_loop(
                "out".to_string(),
                &mut output,
                store_for_task,
                telemetry,
                write_config,
                shutdown_rx,
                &total_for_task,
            )
            .await
        });
        handles.attempted.recv().await.expect("the first attempt happened");
        shutdown_tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("write_loop ends within its grace")
            .unwrap();

        assert_eq!(handles.attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(store.commit().is_some(), "the batch is still queued for finish_and_flush");
        let totals = Totals::of(registry.drain(0));
        assert_eq!(
            totals.sum(
                "logit.component.batches.dropped",
                &[("component", "out"), ("reason", "shutdown")]
            ),
            0.0
        );
        assert_eq!(drain_total.load(std::sync::atomic::Ordering::Relaxed), 0);
        assert!(deliver_fault_tags(&totals.events).is_empty(), "no send was cut off: no fault tag");
    }

    /// Completes its send at `at`, and counts it.
    struct DeadlineOutput {
        at: tokio::time::Instant,
    }

    #[async_trait::async_trait]
    impl Output for DeadlineOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            tokio::time::sleep_until(self.at).await;
            Ok(())
        }
    }

    /// A send resolving at the grace deadline wakes in the same timer turn as the grace arm.
    /// Deterministic only because `write_loop`'s deliver `select!` is biased toward the send;
    /// unbiased, about half of these iterations would read it as cut off.
    #[tokio::test(start_paused = true)]
    async fn a_send_that_completes_in_the_same_wake_as_the_grace_deadline_is_counted_delivered() {
        let grace = Duration::from_millis(100);
        for iteration in 0..16 {
            let registry = Registry::new();
            let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
            let store = Arc::new(SinkStore::Memory(SinkQueue::new(
                SinkQueueConfig::default(),
                telemetry.clone(),
            )));
            store.push((one_event_batch(1.0), TraceContext::new_root().into())).await;
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            shutdown_tx.send(true).unwrap();
            let signalled = tokio::time::Instant::now();
            let mut output = DeadlineOutput { at: signalled + grace };
            let write_config = WriteLoopConfig {
                shutdown_grace: grace,
                delivery_override: Some(DeliveryPosture::AtMostOnce),
                ..WriteLoopConfig::default()
            };
            let drain_total = AtomicU64::new(0);
            write_loop(
                "out".to_string(),
                &mut output,
                Arc::clone(&store),
                telemetry,
                write_config,
                shutdown_rx,
                &drain_total,
            )
            .await;

            assert_eq!(tokio::time::Instant::now(), signalled + grace, "iteration {iteration}");
            let totals = Totals::of(registry.drain(0));
            assert_eq!(
                totals.sum("logit.component.batches.delivered", &[("component", "out")]),
                1.0,
                "iteration {iteration}"
            );
            assert_eq!(
                totals.sum(
                    "logit.component.batches.dropped",
                    &[("component", "out"), ("reason", "shutdown")]
                ),
                0.0,
                "iteration {iteration}"
            );
            assert_eq!(drain_total.load(std::sync::atomic::Ordering::Relaxed), 0);
            assert!(store.commit().is_none(), "iteration {iteration}: delivered and committed");
        }
    }

    /// Resolves its first send at `first_at` (failing it with `Fault::Clean` when `first_fails`),
    /// and never completes a later one, so a later attempt that starts is visible as a cut-off
    /// send.
    struct FirstThenNeverOutput {
        first_at: tokio::time::Instant,
        first_fails: bool,
        attempts: Arc<std::sync::atomic::AtomicU32>,
    }

    #[async_trait::async_trait]
    impl Output for FirstThenNeverOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            if self.attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                std::future::pending::<()>().await;
            }
            tokio::time::sleep_until(self.first_at).await;
            match self.first_fails {
                true => Err(anyhow::anyhow!("simulated clean failure")).context(Fault::Clean),
                false => Ok(()),
            }
        }
    }

    /// Runs `run_output` under at-most-once with shutdown already signalled, so the grace
    /// deadline is anchored at its first poll, sends `batches` batches, and waits for it to end.
    async fn run_with_grace_anchored_at_start(
        output: FirstThenNeverOutput,
        store_config: SinkStoreConfig,
        retry: RetryConfig,
        grace: Duration,
        batches: u64,
    ) -> Totals {
        let registry = Registry::with_span_sampling(1.0);
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(true);
        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            registry.telemetry_for("out", "influxdb_out", "sink"),
            store_config,
            WriteLoopConfig {
                retry,
                shutdown_grace: grace,
                delivery_override: Some(DeliveryPosture::AtMostOnce),
                ..WriteLoopConfig::default()
            },
            shutdown_rx,
            Arc::new(AtomicU64::new(0)),
        ));
        for value in 1..=batches {
            inbox_tx.send(counter_batch(value as f64)).await.unwrap();
        }
        drop(inbox_tx);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run_output ends within its grace")
            .expect("the task must not panic")
            .expect("grace expiry is not a failure");
        drop(shutdown_tx);
        Totals::of(registry.drain(0))
    }

    /// The first send completes in the grace deadline's wake. The second batch then either loses
    /// `NextBatch` to the grace arm or enters the deliver step with the deadline already past;
    /// either way no send starts, so under at-most-once it's neither cut off nor committed.
    /// Repeated because `NextBatch` is unbiased: each route is taken about half the time.
    #[tokio::test(start_paused = true)]
    async fn a_batch_queued_behind_a_send_that_completes_at_the_grace_deadline_is_not_started_and_stays_uncommitted(
    ) {
        let grace = Duration::from_millis(100);
        for iteration in 0..16 {
            for disk in [false, true] {
                let at = format!("iteration {iteration}, disk={disk}");
                let dir = crate::disk_queue::test_support::scratch_dir("queued-behind-deadline");
                let store_config = match disk {
                    true => SinkStoreConfig::Disk(disk_store_config(&dir, u64::MAX)),
                    false => SinkStoreConfig::Memory(SinkQueueConfig::default()),
                };
                let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
                let output = FirstThenNeverOutput {
                    first_at: tokio::time::Instant::now() + grace,
                    first_fails: false,
                    attempts: Arc::clone(&attempts),
                };
                let totals = run_with_grace_anchored_at_start(
                    output,
                    store_config,
                    fast_retry_config(),
                    grace,
                    2,
                )
                .await;

                assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1, "{at}");
                assert_eq!(
                    totals.sum("logit.component.batches.delivered", &[("component", "out")]),
                    1.0,
                    "{at}"
                );
                assert!(deliver_fault_tags(&totals.events).is_empty(), "{at}: nothing was cut off");
                if disk {
                    assert_eq!(
                        totals.sum(
                            "logit.component.batches.dropped",
                            &[("component", "out"), ("reason", "shutdown")]
                        ),
                        0.0,
                        "{at}"
                    );
                    assert_eq!(reopen_and_drain(&dir).await, vec![2.0], "{at}: batch 2 replays");
                } else {
                    // `finish`'s count, the only one: batch 2 was never committed by the cut.
                    assert_eq!(
                        totals.sum(
                            "logit.component.batches.dropped",
                            &[("component", "out"), ("reason", "shutdown")]
                        ),
                        1.0,
                        "{at}"
                    );
                }
                std::fs::remove_dir_all(&dir).ok();
            }
        }
    }

    /// A clean failure's backoff ends at the grace deadline, in the same wake as the grace arm.
    /// The deliver arm is polled first, and the pre-attempt check keeps it from starting a second
    /// attempt that the grace would then read as cut off.
    #[tokio::test(start_paused = true)]
    async fn a_backoff_ending_at_the_grace_deadline_does_not_start_another_attempt() {
        let grace = Duration::from_millis(100);
        let dir = crate::disk_queue::test_support::scratch_dir("backoff-at-deadline");
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let output = FirstThenNeverOutput {
            first_at: tokio::time::Instant::now(),
            first_fails: true,
            attempts: Arc::clone(&attempts),
        };
        let retry = RetryConfig { base_delay: grace, max_delay: grace };
        let totals = run_with_grace_anchored_at_start(
            output,
            SinkStoreConfig::Disk(disk_store_config(&dir, u64::MAX)),
            retry,
            grace,
            1,
        )
        .await;

        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1, "no second attempt");
        assert_eq!(
            totals.sum(
                "logit.component.batches.dropped",
                &[("component", "out"), ("reason", "shutdown")]
            ),
            0.0
        );
        assert!(deliver_fault_tags(&totals.events).is_empty(), "nothing was cut off");
        assert_eq!(reopen_and_drain(&dir).await, vec![1.0], "the batch replays");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// The `batches_dropped` field of the last `drain complete` line in the global capture.
    fn logged_drain_complete_batches_dropped() -> Option<u64> {
        let text = String::from_utf8_lossy(&global_logs().0.lock().unwrap()).into_owned();
        let line = text.lines().rev().find(|l| l.contains("drain complete"))?;
        let value = line.split("batches_dropped=").nth(1)?;
        value.split_whitespace().next()?.parse().ok()
    }

    /// Six batches into a two-batch store whose sink never delivers, under at-most-once: the
    /// grace-cut send (1), `finish` (1), and the sweep (the parked push and the three in the inbox)
    /// all reach `drain complete`.
    #[tokio::test(start_paused = true)]
    async fn drain_complete_reports_every_batch_dropped_for_shutdown_including_those_finish_drops()
    {
        global_logs();
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components
            .insert("out".to_string(), plain_component(vec!["in".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(BurstInput { batches: (0..6).map(|_| counter_batch_of(1)).collect() }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(NeverOutput),
                SinkStoreConfig::Memory(SinkQueueConfig {
                    max_batches: 2,
                    max_bytes: u64::MAX,
                    overflow: OverflowPolicy::Block,
                }),
                WriteLoopConfig {
                    shutdown_grace: Duration::from_millis(100),
                    delivery_override: Some(DeliveryPosture::AtMostOnce),
                    ..WriteLoopConfig::default()
                },
            ),
        );
        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "out"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();
        let (readiness, _rx) = Readiness::channel();
        tokio::time::timeout(
            Duration::from_secs(60),
            run_with_telemetry(
                g,
                specs,
                telemetry,
                readiness,
                tokio::time::sleep(Duration::from_secs(1)),
            ),
        )
        .await
        .expect("the run ends within the sink's grace")
        .expect("a grace-cut drain is not a failure");

        let totals = Totals::of(registry.drain(0));
        let dropped = totals.sum("logit.component.batches.dropped", &[("reason", "shutdown")]);
        assert_eq!(dropped, 6.0, "every batch is dropped for shutdown");
        assert_eq!(logged_drain_complete_batches_dropped(), Some(6));
        let text = String::from_utf8_lossy(&global_logs().0.lock().unwrap()).into_owned();
        for site in ["cut off mid-send", "still queued when this sink", "never handed to"] {
            assert!(
                text.lines().any(|l| l.contains("component=out") && l.contains(site)),
                "the {site:?} site dropped something"
            );
        }
    }

    /// A sink whose `send` never completes and whose `flush` reports it started, then waits for
    /// the test to release it.
    struct GatedFlushOutput {
        flushing: mpsc::UnboundedSender<()>,
        release: Arc<tokio::sync::Notify>,
    }

    #[async_trait::async_trait]
    impl Output for GatedFlushOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves")
        }

        async fn flush(&mut self) -> anyhow::Result<()> {
            let _ = self.flushing.send(());
            self.release.notified().await;
            Ok(())
        }
    }

    /// Batch 1 is in flight, batch 2 parked in `drain_inbox`'s push, batch 3 fills the
    /// one-slot inbox, and the producer is parked sending batch 4 when the grace expires. Were
    /// the inbox left open, the sweep taking batch 3 would let batch 4 land while `flush` is
    /// pending, and it would die with the `Receiver`, received by no one and dropped by no one.
    #[tokio::test(start_paused = true)]
    async fn a_batch_sent_into_the_inbox_after_the_sweep_began_is_counted_not_silently_lost() {
        const SENT: usize = 5;
        let registry = Registry::new();
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(1);
        let producer = Fanout::new(vec![inbox_tx])
            .with_component("up")
            .with_telemetry(registry.telemetry_for("up", "x", "x"));
        let (flushing_tx, mut flushing_rx) = mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(GatedFlushOutput { flushing: flushing_tx, release: Arc::clone(&release) }),
            inbox_rx,
            registry.telemetry_for("out", "influxdb_out", "sink"),
            SinkStoreConfig::Memory(SinkQueueConfig {
                max_batches: 1,
                max_bytes: u64::MAX,
                overflow: OverflowPolicy::Block,
            }),
            slow_retry_write_config(Duration::from_millis(100)),
            shutdown_rx,
            Arc::new(AtomicU64::new(0)),
        ));
        let produce = tokio::spawn(async move {
            for _ in 0..SENT {
                producer.send(counter_batch_of(1)).await;
            }
        });

        tokio::time::sleep(Duration::from_millis(5)).await; // the producer parks on batch 4
        assert!(!produce.is_finished(), "the producer is parked on the full inbox");
        shutdown_tx.send(true).unwrap();
        flushing_rx.recv().await.expect("run_output reaches flush");
        // The producer gets every chance to land a send while `flush` is pending.
        tokio::time::sleep(Duration::from_millis(10)).await;
        release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run_output ends")
            .unwrap()
            .expect("grace expiry is Ok");
        produce.await.unwrap();

        let totals = Totals::of(registry.drain(0));
        let sent = totals.sum("logit.component.batches.sent", &[("component", "up")]);
        let refused = totals.sum(
            "logit.component.events.dropped",
            &[("component", "up"), ("reason", "closed_consumer")],
        );
        let received = totals.sum("logit.component.batches.received", &[("component", "out")]);
        let delivered = totals.sum("logit.component.batches.delivered", &[("component", "out")]);
        let dropped = totals.sum("logit.component.batches.dropped", &[("component", "out")]);
        assert_eq!(sent, SENT as f64);
        assert!(refused > 0.0, "the parked send fails upstream once the inbox closes");
        assert_eq!(sent, received + refused, "every batch sent is received or refused upstream");
        assert_eq!(received, delivered + dropped, "every batch received is accounted for");
    }

    /// Waits for shutdown, then sends into its sink forever, never returning.
    struct BusyAfterShutdownInput;

    #[async_trait::async_trait]
    impl Input for BusyAfterShutdownInput {
        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves")
        }

        async fn run_until_shutdown(
            &mut self,
            sink: Fanout,
            mut shutdown: watch::Receiver<bool>,
        ) -> anyhow::Result<()> {
            let _ = shutdown.wait_for(|&due| due).await;
            loop {
                sink.send(counter_batch_of(1)).await;
            }
        }
    }

    /// Each send spends coop budget, so every poll of the input exhausts it and returns
    /// `Pending`. The grace arm polled after it must still fire. Paused, with the clock advanced
    /// by hand: a runtime that never idles never auto-advances.
    #[tokio::test(start_paused = true)]
    async fn an_input_that_burns_its_coop_budget_after_the_signal_is_still_cancelled_at_the_grace_deadline(
    ) {
        // Deep enough that the input never parks on capacity: it runs out of budget first.
        let (tx, mut rx) = mpsc::channel::<Delivered>(4096);
        let consumer = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let grace = Duration::from_millis(100);
        let handle = tokio::spawn(run_input(
            "in".to_string(),
            Box::new(BusyAfterShutdownInput),
            Fanout::new(vec![tx]),
            shutdown_rx,
            grace,
        ));
        tokio::task::yield_now().await;
        shutdown_tx.send(true).unwrap();
        let signalled = tokio::time::Instant::now();
        for _ in 0..100 {
            if handle.is_finished() {
                break;
            }
            tokio::time::advance(Duration::from_millis(10)).await;
        }
        assert!(handle.is_finished(), "the backstop never fired against a busy input");
        handle.await.unwrap().expect("grace expiry is Ok");
        assert!(tokio::time::Instant::now().duration_since(signalled) >= grace);
        consumer.await.unwrap();
    }

    /// Waits for shutdown, sleeps `grace`, then fails: ready in the same wake as the backstop.
    struct ErrAtGraceInput {
        grace: Duration,
    }

    #[async_trait::async_trait]
    impl Input for ErrAtGraceInput {
        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves")
        }

        async fn run_until_shutdown(
            &mut self,
            _sink: Fanout,
            mut shutdown: watch::Receiver<bool>,
        ) -> anyhow::Result<()> {
            let _ = shutdown.wait_for(|&due| due).await;
            tokio::time::sleep(self.grace).await;
            anyhow::bail!("failed while draining")
        }
    }

    /// Unbiased, about half of these iterations would discard the error.
    #[tokio::test(start_paused = true)]
    async fn an_input_error_at_the_grace_deadline_is_never_swallowed_by_the_backstop() {
        let grace = Duration::from_millis(100);
        for iteration in 0..32 {
            let (tx, _rx) = mpsc::channel(1);
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let handle = tokio::spawn(run_input(
                "in".to_string(),
                Box::new(ErrAtGraceInput { grace }),
                Fanout::new(vec![tx]),
                shutdown_rx,
                grace,
            ));
            tokio::task::yield_now().await;
            shutdown_tx.send(true).unwrap();
            let err = handle
                .await
                .unwrap()
                .expect_err(&format!("iteration {iteration}: the input's error is returned"));
            assert!(format!("{err:#}").contains("failed while draining"), "{err:#}");
        }
    }

    /// Polls `future` once with a no-op waker.
    fn poll_once<F: Future>(future: F) -> std::task::Poll<F::Output> {
        let mut future = std::pin::pin!(future);
        future.as_mut().poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
    }

    #[tokio::test(start_paused = true)]
    async fn a_shutdown_grace_expired_call_polled_after_the_signal_then_dropped_keeps_its_anchor() {
        let grace = Duration::from_secs(1);
        let (shutdown_tx, mut shutdown) = watch::channel(false);
        let deadline = std::sync::OnceLock::new();
        shutdown_tx.send(true).unwrap();
        let first_poll = tokio::time::Instant::now();
        assert!(poll_once(shutdown_grace_expired(&mut shutdown, &deadline, grace)).is_pending());
        assert_eq!(deadline.get(), Some(&(first_poll + grace)));

        tokio::time::advance(Duration::from_millis(400)).await;
        shutdown_grace_expired(&mut shutdown, &deadline, grace).await;
        assert_eq!(tokio::time::Instant::now(), first_poll + grace);
    }

    /// The anchor is the first poll that sees the signal, not the signal: a call last polled
    /// before it, and dropped unpolled after it, leaves no deadline.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_grace_expired_call_never_polled_after_the_signal_anchors_nothing() {
        let grace = Duration::from_secs(1);
        let (shutdown_tx, mut shutdown) = watch::channel(false);
        let deadline = std::sync::OnceLock::new();
        {
            let mut call = std::pin::pin!(shutdown_grace_expired(&mut shutdown, &deadline, grace));
            let waker = std::task::Waker::noop();
            assert!(call.as_mut().poll(&mut std::task::Context::from_waker(waker)).is_pending());
            shutdown_tx.send(true).unwrap();
            tokio::time::advance(Duration::from_millis(400)).await;
        }
        assert_eq!(deadline.get(), None, "no poll saw the signal");

        let polled = tokio::time::Instant::now();
        shutdown_grace_expired(&mut shutdown, &deadline, grace).await;
        assert_eq!(tokio::time::Instant::now(), polled + grace);
    }

    /// Forwards each batch the test sends on `rx`; returns once the test drops the sender.
    struct ChannelInput {
        rx: mpsc::UnboundedReceiver<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Input for ChannelInput {
        async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
            while let Some(batch) = self.rx.recv().await {
                sink.send(batch).await;
            }
            Ok(())
        }
    }

    /// A sink holding a `Refused` head ends nothing: a healthy sibling delivers its queued
    /// batches and new ones, and the run keeps going.
    #[tokio::test(start_paused = true)]
    async fn a_healthy_sink_keeps_delivering_while_a_sibling_sink_holds_a_refused_head() {
        let mut components = Map::new();
        components.insert(
            "bad_in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "bad".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["bad_in".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        components.insert(
            "good_in".to_string(),
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
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            },
        );
        components.insert(
            "good".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["good_in".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (bad_tx, bad_rx) = mpsc::unbounded_channel();
        let (good_tx, good_rx) = mpsc::unbounded_channel();
        let (bad_output, mut bad_handles) = faulty_output(Fault::Refused, u32::MAX);
        let gate = Gate::new();
        let (delivered_tx, mut delivered_rx) = mpsc::unbounded_channel();

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "bad_in".to_string(),
            NodeSpec::Input(Box::new(ChannelInput { rx: bad_rx }), InputRuntimeConfig::default()),
        );
        specs.insert(
            "bad".to_string(),
            NodeSpec::Output(
                Box::new(bad_output),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );
        specs.insert(
            "good_in".to_string(),
            NodeSpec::Input(Box::new(ChannelInput { rx: good_rx }), InputRuntimeConfig::default()),
        );
        specs.insert(
            "good".to_string(),
            NodeSpec::Output(
                Box::new(SlowOutput { gate: gate.clone(), delivered: delivered_tx }),
                SinkStoreConfig::Memory(SinkQueueConfig {
                    max_batches: 100,
                    max_bytes: u64::MAX,
                    overflow: OverflowPolicy::Block,
                }),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Arc::new(Registry::new());
        let mut telemetry: HashMap<String, Telemetry> = HashMap::new();
        telemetry.insert("good".to_string(), registry.telemetry_for("good", "x", "sink"));
        telemetry.insert("bad".to_string(), registry.telemetry_for("bad", "x", "sink"));

        let mut run_task = tokio::spawn(run_with_telemetry(
            g,
            specs,
            telemetry,
            Readiness::disabled(),
            std::future::pending(),
        ));

        // Queue three batches on "good" while its delivery is gated shut.
        for i in 0..3 {
            good_tx
                .send(EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", i as f64)],
                })
                .expect("good_in's receiver should still be alive");
        }
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let queued = gauge_value(&registry.drain(0), "good", "logit.component.buffer.batches");
            if queued.unwrap_or(0.0) >= 3.0 {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for good's batches to queue"
            );
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Hold "bad"'s head: every attempt is refused.
        bad_tx
            .send(EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", 1.0)],
            })
            .expect("bad_in's receiver should still be alive");
        for attempt in 0..3 {
            bad_handles.attempted.recv().await.unwrap_or_else(|| panic!("bad's attempt {attempt}"));
        }
        let mut probe = TelemetryProbe::with_registry(Arc::clone(&registry));
        probe
            .wait_for("bad to hold its head", |t| {
                t.gauge(RETRYING_GAUGE, &[("component", "bad")]) == Some(1.0)
            })
            .await;

        gate.open();
        for i in 0..3 {
            let received = tokio::time::timeout(Duration::from_secs(5), delivered_rx.recv())
                .await
                .expect("good's queued batches should be delivered while bad holds")
                .expect("the channel should not have closed");
            match &received.events[0].metrics[0].kind {
                MetricKind::Sum(s) => {
                    assert_eq!(s.value, i as f64, "batches should still be delivered in order")
                }
                other => panic!("expected Sum, got {other:?}"),
            }
        }

        // Good takes a new batch while bad keeps retrying, and the run goes on.
        good_tx
            .send(EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", 3.0)],
            })
            .expect("good_in's receiver should still be alive");
        tokio::time::timeout(Duration::from_secs(5), delivered_rx.recv())
            .await
            .expect("good keeps delivering new batches")
            .expect("the channel should not have closed");
        bad_handles.attempted.recv().await.expect("bad keeps retrying");
        assert!(
            tokio::time::timeout(Duration::from_secs(60), &mut run_task).await.is_err(),
            "a sink holding its head must not end the run"
        );
        assert_eq!(
            probe.poll().sum("logit.component.batches.dropped", &[("component", "bad")]),
            0.0,
            "a hold drops nothing"
        );
        run_task.abort();
    }

    /// Fails `delay` after it starts, ignoring the shutdown signal until then, as a listener
    /// still draining at shutdown would.
    struct DelayedErrInput {
        delay: Duration,
    }

    #[async_trait::async_trait]
    impl Input for DelayedErrInput {
        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            tokio::time::sleep(self.delay).await;
            anyhow::bail!("failed after {:?}", self.delay)
        }

        async fn run_until_shutdown(
            &mut self,
            sink: Fanout,
            _shutdown: watch::Receiver<bool>,
        ) -> anyhow::Result<()> {
            self.run(sink).await
        }
    }

    /// Both listeners fail: `bad1` at 60 s and `bad2` at 61 s, inside the shutdown grace that
    /// `bad1`'s failure starts. The join loop returns `bad1`'s error, and `bad2`'s later one
    /// doesn't overwrite it.
    #[tokio::test(start_paused = true)]
    async fn run_with_telemetry_returns_the_first_failure_not_a_later_cascading_one() {
        let mut components = Map::new();
        components.insert("bad1".to_string(), plain_component(vec![], statsd_in()));
        components.insert("bad2".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "out".to_string(),
            plain_component(vec!["bad1".to_string(), "bad2".to_string()], influxdb_out()),
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        // The grace keeps `bad1`'s failure-triggered shutdown from cancelling `bad2` before it
        // fails.
        let grace = InputRuntimeConfig { shutdown_grace: Duration::from_secs(3600) };
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "bad1".to_string(),
            NodeSpec::Input(Box::new(DelayedErrInput { delay: Duration::from_secs(60) }), grace),
        );
        specs.insert(
            "bad2".to_string(),
            NodeSpec::Input(Box::new(DelayedErrInput { delay: Duration::from_secs(61) }), grace),
        );
        let (tx, _out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let run_task = tokio::spawn(run(g, specs));

        // On the same virtual clock, so the timeout must exceed both delays.
        let result = tokio::time::timeout(Duration::from_secs(120), run_task)
            .await
            .expect(
                "run should not hang -- it must still terminate in bounded time once every \
                      task has finished, not abort-then-hang or hang forever",
            )
            .expect("task should not panic");
        let err = result.expect_err("a failing node should still end run with an error");
        assert!(
            err.to_string().contains("bad1"),
            "the returned error should be bad1's, the first failure recorded, got: {err}"
        );
        assert!(
            !err.to_string().contains("bad2"),
            "bad2's later, cascading failure must not overwrite the first recorded error, got: {err}"
        );
    }

    // -- `Input::bind`, readiness, exit codes (docs/plans/operator-surface.md) --

    /// Fails every `bind()`; `run()` must never be reached.
    struct FailingBindInput;

    #[async_trait::async_trait]
    impl Input for FailingBindInput {
        async fn bind(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("cannot bind")
        }
        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            unreachable!("bind() fails first; run() must never be called")
        }
    }

    fn statsd_in() -> ComponentKind {
        ComponentKind::StatsdIn {
            bind: "127.0.0.1:0".to_string(),
            transport: logit_config::StatsdTransport::default(),
            tls: None,
            handshake_timeout: logit_config::default_handshake_timeout(),
            idle_timeout: None,
            max_connections: logit_config::default_max_connections(),
            peer: false,
            proxy_protocol: false,
            socket_mode: None,
        }
    }

    fn plain_component(sources: Vec<String>, kind: ComponentKind) -> Component {
        Component {
            buffer: logit_config::BufferConfig::default(),
            receive: logit_config::ReceiveConfig::default(),
            sources,
            targets: Vec::new(),
            kind,
        }
    }

    #[tokio::test]
    async fn a_failing_bind_returns_startup_and_spawns_nothing() {
        let mut components = Map::new();
        components.insert("a_in".to_string(), plain_component(vec![], statsd_in()));
        components
            .insert("out".to_string(), plain_component(vec!["a_in".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (tx, rx) = std::sync::mpsc::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "a_in".to_string(),
            NodeSpec::Input(Box::new(FailingBindInput), InputRuntimeConfig::default()),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let err = run_with_telemetry(
            g,
            specs,
            HashMap::new(),
            Readiness::disabled(),
            std::future::pending(),
        )
        .await
        .expect_err("a bind failure must fail the whole run");
        assert!(matches!(err, RunError::Startup(_)), "a bind failure is a startup failure");
        assert!(
            err.to_string().contains("a_in"),
            "the error should name the failing component: {err}"
        );
        assert!(
            rx.try_recv().is_err(),
            "the sink must never have been spawned -- nothing should have reached it"
        );
    }

    /// The sink mirror of [`FailingBindInput`].
    struct FailingBindOutput {
        tx: std::sync::mpsc::Sender<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Output for FailingBindOutput {
        async fn bind(&mut self) -> anyhow::Result<()> {
            anyhow::bail!("cannot bind")
        }
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            let _ = self.tx.send(batch.clone());
            unreachable!("bind() fails first; send() must never be called")
        }
    }

    #[tokio::test]
    async fn a_failing_output_bind_returns_startup_and_spawns_nothing() {
        let mut components = Map::new();
        components.insert("a_in".to_string(), plain_component(vec![], statsd_in()));
        components
            .insert("out".to_string(), plain_component(vec!["a_in".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (tx, rx) = std::sync::mpsc::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "a_in".to_string(),
            NodeSpec::Input(Box::new(ForeverInput), InputRuntimeConfig::default()),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(FailingBindOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let err = run_with_telemetry(
            g,
            specs,
            HashMap::new(),
            Readiness::disabled(),
            std::future::pending(),
        )
        .await
        .expect_err("a sink bind failure must fail the whole run");
        assert!(matches!(err, RunError::Startup(_)), "a bind failure is a startup failure");
        assert!(
            err.to_string().contains("out"),
            "the error should name the failing component: {err}"
        );
        assert!(
            rx.try_recv().is_err(),
            "nothing should have been spawned -- the sink must never have seen a batch"
        );
    }

    /// With two unbindable inputs, the first by sorted id is always the one reported.
    #[tokio::test]
    async fn the_first_failing_bind_by_sorted_id_is_the_one_reported() {
        let mut components = Map::new();
        components.insert("a_in".to_string(), plain_component(vec![], statsd_in()));
        components.insert("z_in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "out".to_string(),
            plain_component(vec!["a_in".to_string(), "z_in".to_string()], influxdb_out()),
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "a_in".to_string(),
            NodeSpec::Input(Box::new(FailingBindInput), InputRuntimeConfig::default()),
        );
        specs.insert(
            "z_in".to_string(),
            NodeSpec::Input(Box::new(FailingBindInput), InputRuntimeConfig::default()),
        );

        let err = run(g, specs).await.expect_err("both inputs fail to bind");
        assert!(err.to_string().contains("a_in"), "the sorted-first id should be named: {err}");
    }

    /// Phase reaches `Ready` after startup, then `Draining` when `shutdown` resolves. Uses
    /// `wait_for`, not a `changed()` sequence, since `watch` coalesces updates.
    #[tokio::test(start_paused = true)]
    async fn phase_reaches_ready_then_draining_on_a_normal_run() {
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components
            .insert("out".to_string(), plain_component(vec!["in".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (readiness, mut rx) = Readiness::channel();
        assert_eq!(rx.borrow().phase, Phase::Starting);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(ForeverInput), InputRuntimeConfig::default()),
        );
        let (tx, _out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task =
            tokio::spawn(run_with_telemetry(g, specs, HashMap::new(), readiness, async {
                let _ = shutdown_rx.await;
            }));

        rx.wait_for(|s| s.phase == Phase::Ready).await.expect("readiness channel should stay open");
        assert_eq!(
            rx.borrow().components.get("in"),
            Some(&NodeState::Running),
            "every component should be Running once Ready"
        );
        assert_eq!(rx.borrow().components.get("out"), Some(&NodeState::Running));

        let _ = shutdown_tx.send(());
        rx.wait_for(|s| s.phase == Phase::Draining)
            .await
            .expect("readiness channel should stay open");

        tokio::time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("run_with_telemetry should not hang once shutdown fires")
            .expect("task should not panic")
            .expect("a clean shutdown should end run_with_telemetry with Ok");
    }

    // -- `RunOptions::shutdown_delay` --

    /// Reports the instant its shutdown receiver fires, then returns. With `bind_gate` set, its
    /// `bind` reports entry and then waits for the gate, holding the run in `Phase::Starting`.
    struct StopRecordingInput {
        stopped: Option<oneshot::Sender<tokio::time::Instant>>,
        bind_gate: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
    }

    #[async_trait::async_trait]
    impl Input for StopRecordingInput {
        async fn bind(&mut self) -> anyhow::Result<()> {
            if let Some((entered, gate)) = self.bind_gate.take() {
                let _ = entered.send(());
                let _ = gate.await;
            }
            Ok(())
        }

        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves")
        }

        async fn run_until_shutdown(
            &mut self,
            _sink: Fanout,
            mut shutdown: watch::Receiver<bool>,
        ) -> anyhow::Result<()> {
            let _ = shutdown.wait_for(|&due| due).await;
            if let Some(tx) = self.stopped.take() {
                let _ = tx.send(tokio::time::Instant::now());
            }
            Ok(())
        }
    }

    /// Fails once `poke` fires.
    struct PokedErrInput {
        poke: Option<oneshot::Receiver<()>>,
    }

    #[async_trait::async_trait]
    impl Input for PokedErrInput {
        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            if let Some(poke) = self.poke.take() {
                let _ = poke.await;
            }
            anyhow::bail!("poked")
        }
    }

    /// Two listeners into one sink: `in`, built by the caller, and `steady`, which stops only on
    /// the shutdown `watch`, so the run can't end before that is sent.
    fn delay_graph(input: Box<dyn Input + Send>) -> (Graph, HashMap<String, NodeSpec>) {
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert("steady".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "out".to_string(),
            plain_component(vec!["in".to_string(), "steady".to_string()], influxdb_out()),
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert("in".to_string(), NodeSpec::Input(input, InputRuntimeConfig::default()));
        specs.insert(
            "steady".to_string(),
            NodeSpec::Input(Box::new(ForeverInput), InputRuntimeConfig::default()),
        );
        let (tx, _out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );
        (g, specs)
    }

    /// Resolves `shutdown` once the run is `Ready`, checks that readiness flips to `Draining` at
    /// the same instant, and returns how long after the signal the listener was told to stop.
    async fn listener_stop_after_signal(options: RunOptions) -> Duration {
        let (stopped_tx, stopped_rx) = oneshot::channel();
        let (g, specs) = delay_graph(Box::new(StopRecordingInput {
            stopped: Some(stopped_tx),
            bind_gate: None,
        }));
        let (readiness, mut rx) = Readiness::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task = tokio::spawn(run_with_options(
            g,
            specs,
            HashMap::new(),
            readiness,
            async {
                let _ = shutdown_rx.await;
            },
            options,
        ));
        rx.wait_for(|s| s.phase == Phase::Ready).await.expect("readiness channel should stay open");

        let signalled_at = tokio::time::Instant::now();
        let _ = shutdown_tx.send(());
        rx.wait_for(|s| s.phase == Phase::Draining)
            .await
            .expect("readiness channel should stay open");
        assert_eq!(
            tokio::time::Instant::now(),
            signalled_at,
            "readiness should report draining at the signal, before any delay"
        );

        let stopped_at = stopped_rx.await.expect("the listener should be told to stop");
        tokio::time::timeout(Duration::from_secs(5), run_task)
            .await
            .expect("the run should end once its listener stops")
            .expect("task should not panic")
            .expect("a clean shutdown should end the run with Ok");
        stopped_at.duration_since(signalled_at)
    }

    /// The paused clock advances only to the next timer deadline, so the gap equals the delay to
    /// the nanosecond.
    #[tokio::test(start_paused = true)]
    async fn a_shutdown_delay_keeps_listeners_running_and_reports_draining_until_it_elapses() {
        let delay = Duration::from_secs(30);
        let gap = listener_stop_after_signal(RunOptions { shutdown_delay: delay }).await;
        assert_eq!(gap, delay);
    }

    #[tokio::test(start_paused = true)]
    async fn the_default_run_options_stop_listeners_at_the_instant_of_the_signal() {
        let gap = listener_stop_after_signal(RunOptions::default()).await;
        assert_eq!(gap, Duration::ZERO);
    }

    /// The signal lands while `in`'s bind holds the run in `Starting`, so the delay is skipped:
    /// the listener stops at the signal's instant on the paused clock, not 30 s later.
    #[tokio::test(start_paused = true)]
    async fn a_signal_before_ready_skips_the_shutdown_delay() {
        let (stopped_tx, stopped_rx) = oneshot::channel();
        let (entered_tx, entered_rx) = oneshot::channel();
        let (gate_tx, gate_rx) = oneshot::channel();
        let (g, specs) = delay_graph(Box::new(StopRecordingInput {
            stopped: Some(stopped_tx),
            bind_gate: Some((entered_tx, gate_rx)),
        }));
        let (readiness, mut rx) = Readiness::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task = tokio::spawn(run_with_options(
            g,
            specs,
            HashMap::new(),
            readiness,
            async {
                let _ = shutdown_rx.await;
            },
            RunOptions { shutdown_delay: Duration::from_secs(30) },
        ));
        entered_rx.await.expect("the gated bind should start");
        assert_eq!(rx.borrow().phase, Phase::Starting);

        let signalled_at = tokio::time::Instant::now();
        let _ = shutdown_tx.send(());
        rx.wait_for(|s| s.phase == Phase::Draining)
            .await
            .expect("readiness channel should stay open");
        let _ = gate_tx.send(());

        let stopped_at = stopped_rx.await.expect("the listener should be told to stop");
        assert_eq!(stopped_at.duration_since(signalled_at), Duration::ZERO);
        run_task
            .await
            .expect("task should not panic")
            .expect("a clean shutdown should end the run with Ok");
    }

    /// A node failing mid-delay sends the shutdown itself, so the drain doesn't wait out the delay.
    #[tokio::test(start_paused = true)]
    async fn a_node_failing_during_the_shutdown_delay_starts_the_drain_at_once() {
        let delay = Duration::from_secs(3600);
        let (poke_tx, poke_rx) = oneshot::channel();
        let (g, specs) = delay_graph(Box::new(PokedErrInput { poke: Some(poke_rx) }));
        let (readiness, mut rx) = Readiness::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task = tokio::spawn(run_with_options(
            g,
            specs,
            HashMap::new(),
            readiness,
            async {
                let _ = shutdown_rx.await;
            },
            RunOptions { shutdown_delay: delay },
        ));
        rx.wait_for(|s| s.phase == Phase::Ready).await.expect("readiness channel should stay open");

        let signalled_at = tokio::time::Instant::now();
        let _ = shutdown_tx.send(());
        rx.wait_for(|s| s.phase == Phase::Draining)
            .await
            .expect("readiness channel should stay open");
        let _ = poke_tx.send(());

        let err = run_task
            .await
            .expect("task should not panic")
            .expect_err("the poked listener's error should fail the run");
        assert!(matches!(err, RunError::Runtime(_)), "{err}");
        assert!(
            signalled_at.elapsed() < delay,
            "the run ended {:?} after the signal, waiting out the delay",
            signalled_at.elapsed()
        );
    }

    /// A listener failing after `Ready` sets `Phase::Failed` and returns `RunError::Runtime`.
    #[tokio::test]
    async fn a_listener_failing_after_ready_flips_failed_and_returns_runtime() {
        let mut components = Map::new();
        components.insert("err_in".to_string(), plain_component(vec![], statsd_in()));
        components
            .insert("out".to_string(), plain_component(vec!["err_in".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (readiness, rx) = Readiness::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "err_in".to_string(),
            NodeSpec::Input(Box::new(ErrInput), InputRuntimeConfig::default()),
        );
        let (tx, _out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let err = run_with_telemetry(g, specs, HashMap::new(), readiness, std::future::pending())
            .await
            .expect_err("ErrInput should fail the run");
        assert!(matches!(err, RunError::Runtime(_)), "a post-ready failure is a runtime failure");
        assert!(err.to_string().contains("err_in"), "the error should name err_in: {err}");

        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.phase, Phase::Failed);
        assert_eq!(snapshot.components.get("err_in"), Some(&NodeState::Failed));
    }

    /// A Lua thread panicking after `Ready` fails the run like a task: `Runtime`, others drained.
    #[tokio::test]
    async fn a_lua_thread_panicking_after_ready_flips_failed_and_returns_runtime() {
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "enrich".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua {
                    script: "function process(event) return event end".to_string(),
                    interval: None,
                    max_memory: None,
                },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["enrich".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (readiness, rx) = Readiness::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(ForeverInput), InputRuntimeConfig::default()),
        );
        specs.insert(
            "enrich".to_string(),
            NodeSpec::Lua {
                script: "function process(event) return event end".to_string(),
                // The panic vector: script errors are non-fatal, but `advance_flush_deadline`
                // panics on a zero interval on the first loop iteration. Graph rule 9 rejects a
                // zero interval from config, so only the spec carries it. If a runtime guard
                // lands, find a new vector rather than relaxing the assertions.
                interval: Some(Duration::ZERO),
                runtime: LuaRuntimeConfig::default(),
            },
        );
        let (tx, _out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let err = tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, HashMap::new(), readiness, std::future::pending()),
        )
        .await
        .expect("a dead Lua thread must end the run, not leave it hanging")
        .expect_err("the panicking Lua thread should fail the run");
        assert!(matches!(err, RunError::Runtime(_)), "a post-ready failure is a runtime failure");
        let message = err.to_string();
        assert!(message.contains("enrich"), "the error should name enrich: {message}");
        assert!(
            message.contains("panicked"),
            "the error should say the thread panicked: {message}"
        );

        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.phase, Phase::Failed);
        assert_eq!(snapshot.components.get("enrich"), Some(&NodeState::Failed));
        assert_eq!(
            snapshot.components.get("in"),
            Some(&NodeState::Finished),
            "the listener should have been drained by the failure-triggered shutdown, not aborted"
        );
        assert_eq!(
            snapshot.components.get("out"),
            Some(&NodeState::Finished),
            "the sink should have drained once the dead node's fanout closed its inbox"
        );
    }

    /// A Lua node whose inbox closes normally reaches `NodeState::Finished`.
    #[tokio::test]
    async fn a_lua_node_finishing_on_its_own_reaches_finished() {
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "enrich".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua {
                    script: "function process(event) return event end".to_string(),
                    interval: None,
                    max_memory: None,
                },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["enrich".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (readiness, rx) = Readiness::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "enrich".to_string(),
            NodeSpec::Lua {
                script: "function process(event) return event end".to_string(),
                interval: None,
                runtime: LuaRuntimeConfig::default(),
            },
        );
        let (tx, out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, HashMap::new(), readiness, std::future::pending()),
        )
        .await
        .expect("a finite input should let the whole graph drain and the run end")
        .expect("a clean run should end with Ok");

        let received =
            out_rx.try_recv().expect("the batch should have flowed through the Lua node");
        assert_eq!(received.events.len(), 1);
        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.phase, Phase::Ready, "nothing failed and nothing signalled a drain");
        assert_eq!(snapshot.components.get("in"), Some(&NodeState::Finished));
        assert_eq!(
            snapshot.components.get("enrich"),
            Some(&NodeState::Finished),
            "the Lua node's own exit is now observed via its watcher task"
        );
        assert_eq!(snapshot.components.get("out"), Some(&NodeState::Finished));
    }

    /// Every panic payload shape (`&str`, `String`, other) becomes a "thread panicked" message.
    #[test]
    fn thread_outcome_reports_a_panic_payload_as_a_message() {
        assert_eq!(thread_outcome(Ok(Ok(()))), Ok(()));
        // A loop's own failure passes through unprefixed: it isn't a panic.
        assert_eq!(thread_outcome(Ok(Err("over".to_string()))), Err("over".to_string()));

        let literal = std::panic::catch_unwind(|| panic!("boom"));
        assert_eq!(thread_outcome(literal), Err("thread panicked: boom".to_string()));

        let formatted = std::panic::catch_unwind(|| {
            let n = 7;
            panic!("slot {n} out of range")
        });
        assert_eq!(
            thread_outcome(formatted),
            Err("thread panicked: slot 7 out of range".to_string())
        );

        let opaque = std::panic::catch_unwind(|| std::panic::panic_any(42u8));
        assert_eq!(
            thread_outcome(opaque),
            Err("thread panicked: non-string panic payload".to_string())
        );
    }

    // -- `watch_lua_thread` against a hand-driven heartbeat. No Lua thread, so paused time is
    //    safe here; the tests further down that run a real Lua thread use real time.

    /// A watcher's inputs besides `done_rx`, kept alive by the test so nothing closes under it.
    struct WatcherRig {
        heartbeat: Arc<Heartbeat>,
        io: SharedLuaIo,
        /// The sending half of the `io`'s inbox, to observe revocation (`is_closed`).
        inbox_tx: mpsc::Sender<Delivered>,
        shutdown_tx: watch::Sender<bool>,
        readiness: Readiness,
        rx: watch::Receiver<crate::readiness::PipelineState>,
    }

    fn watcher_rig() -> WatcherRig {
        let (inbox_tx, inbox) = mpsc::channel(1);
        let io = Arc::new(std::sync::Mutex::new(Some(LuaIo {
            inbox,
            fanout: Fanout::new(Vec::new()),
            target_fanouts: Vec::new(),
        })));
        let (shutdown_tx, _) = watch::channel(false);
        let (readiness, rx) = Readiness::channel();
        readiness.begin(&["enrich".to_string()]);
        readiness.set_node("enrich", NodeState::Running);
        WatcherRig {
            heartbeat: Arc::new(Heartbeat::new()),
            io,
            inbox_tx,
            shutdown_tx,
            readiness,
            rx,
        }
    }

    fn spawn_watcher(
        rig: &WatcherRig,
        done_rx: oneshot::Receiver<Result<(), String>>,
        config: LuaRuntimeConfig,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        spawn_watcher_with_telemetry(rig, done_rx, config, Telemetry::default())
    }

    fn spawn_watcher_with_telemetry(
        rig: &WatcherRig,
        done_rx: oneshot::Receiver<Result<(), String>>,
        config: LuaRuntimeConfig,
        telemetry: Telemetry,
    ) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        tokio::spawn(watch_lua_thread(
            "enrich".to_string(),
            done_rx,
            rig.heartbeat.clone(),
            rig.io.clone(),
            config,
            rig.shutdown_tx.subscribe(),
            rig.readiness.clone(),
            telemetry,
            Diagnostics::new("enrich"),
            Arc::new(AtomicU64::new(0)),
        ))
    }

    fn enrich_state(rig: &WatcherRig) -> Option<NodeState> {
        rig.rx.borrow().components.get("enrich").copied()
    }

    /// A clean report is `Ok`; a panic report or a dropped sender is an error naming the node.
    #[tokio::test(start_paused = true)]
    async fn watch_lua_thread_maps_each_outcome() {
        let rig = watcher_rig();
        let config = LuaRuntimeConfig::default();

        let (tx, rx) = oneshot::channel();
        tx.send(Ok(())).expect("receiver alive");
        spawn_watcher(&rig, rx, config).await.unwrap().expect("a clean report is Ok");

        let (tx, rx) = oneshot::channel();
        tx.send(Err("thread panicked: boom".to_string())).expect("receiver alive");
        let err =
            spawn_watcher(&rig, rx, config).await.unwrap().expect_err("a panic report is an error");
        assert_eq!(err.to_string(), "component 'enrich': thread panicked: boom");

        let (tx, rx) = oneshot::channel::<Result<(), String>>();
        drop(tx);
        let err = spawn_watcher(&rig, rx, config)
            .await
            .unwrap()
            .expect_err("a dropped sender is an error, not a silent Ok");
        assert!(
            err.to_string().contains("without reporting"),
            "the defensive arm should say what happened: {err}"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_busy_heartbeat_that_stops_advancing_is_stalled_and_resumes() {
        let rig = watcher_rig();
        let config = LuaRuntimeConfig {
            stall_after: Duration::from_millis(100),
            shutdown_grace: Duration::from_millis(100),
            ..Default::default()
        };
        let (done_tx, done_rx) = oneshot::channel();
        let watcher = spawn_watcher(&rig, done_rx, config);

        rig.heartbeat.enter();
        // Advancing more often than `stall_after` keeps it running however long the call lasts.
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_millis(60)).await;
            rig.heartbeat.tick();
        }
        assert_eq!(enrich_state(&rig), Some(NodeState::Running), "progress is never a stall");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(enrich_state(&rig), Some(NodeState::Stalled));
        assert!(rig.rx.borrow().has_stalled_node());

        rig.heartbeat.tick();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(enrich_state(&rig), Some(NodeState::Running), "one change clears the stall");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert_eq!(enrich_state(&rig), Some(NodeState::Stalled), "and a new pause stalls again");

        assert!(lock_io(&rig.io).is_some(), "a stall without shutdown never revokes I/O");
        done_tx.send(Ok(())).unwrap();
        watcher.await.unwrap().expect("a clean report after a stall is still Ok");
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_heartbeat_is_never_stalled() {
        let rig = watcher_rig();
        let config = LuaRuntimeConfig {
            stall_after: Duration::from_millis(50),
            shutdown_grace: Duration::from_millis(50),
            ..Default::default()
        };
        let (done_tx, done_rx) = oneshot::channel();
        let watcher = spawn_watcher(&rig, done_rx, config);

        rig.heartbeat.enter();
        rig.heartbeat.leave();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(enrich_state(&rig), Some(NodeState::Running));

        // Idle through shutdown too: a node parked outside a script call is never blamed.
        rig.shutdown_tx.send(true).unwrap();
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(enrich_state(&rig), Some(NodeState::Running));
        assert!(!watcher.is_finished(), "an idle node is never wedged");
        assert!(!rig.inbox_tx.is_closed(), "an idle node's I/O is never revoked");

        done_tx.send(Ok(())).unwrap();
        watcher.await.unwrap().expect("Ok");
    }

    #[tokio::test(start_paused = true)]
    async fn a_wedged_node_after_shutdown_has_its_io_revoked_and_fails() {
        let rig = watcher_rig();
        let config = LuaRuntimeConfig {
            stall_after: Duration::from_secs(10),
            shutdown_grace: Duration::from_millis(200),
            ..Default::default()
        };
        let (_done_tx, done_rx) = oneshot::channel();
        let watcher = spawn_watcher(&rig, done_rx, config);

        rig.heartbeat.enter();
        rig.shutdown_tx.send(true).unwrap();
        // Progress after the signal moves the deadline: the grace runs from the later of the two.
        tokio::time::sleep(Duration::from_millis(150)).await;
        rig.heartbeat.tick();
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!watcher.is_finished(), "300 ms after shutdown, but 150 ms after progress");
        assert!(!rig.inbox_tx.is_closed());

        let err = tokio::time::timeout(Duration::from_secs(1), watcher)
            .await
            .expect("a wedged node fails within its grace")
            .unwrap()
            .expect_err("a wedge is an error");
        let message = err.to_string();
        assert!(message.contains("component 'enrich'"), "{message}");
        assert!(message.contains("still inside process()/flush()"), "{message}");
        assert!(lock_io(&rig.io).is_none(), "the watcher took the node's I/O");
        assert!(rig.inbox_tx.is_closed(), "and dropping it closed the node's inbox");
    }

    /// A send whose permit was reserved before revocation still lands after `close`, and must be
    /// drained and counted, not destroyed with the `Receiver`.
    #[tokio::test(start_paused = true)]
    async fn a_permit_reserved_before_revocation_is_drained_and_counted() {
        let rig = watcher_rig();
        let config = LuaRuntimeConfig {
            stall_after: Duration::from_secs(10),
            shutdown_grace: Duration::from_millis(100),
            ..Default::default()
        };
        let registry = Registry::new();
        let (_done_tx, done_rx) = oneshot::channel();
        let watcher = spawn_watcher_with_telemetry(
            &rig,
            done_rx,
            config,
            registry.telemetry_for("enrich", "x", "x"),
        );
        let permit = rig.inbox_tx.reserve().await.expect("the inbox is open and empty");

        rig.heartbeat.enter();
        rig.shutdown_tx.send(true).unwrap();
        // Past the grace, so the watcher has revoked and is waiting on the outstanding permit.
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!watcher.is_finished(), "the sweep waits for a reserved permit");
        permit.send(counter_batch(1.0));

        watcher.await.unwrap().expect_err("a wedge is an error");
        let totals = Totals::of(registry.drain(0));
        assert_eq!(
            totals.sum(
                "logit.component.events.dropped",
                &[("component", "enrich"), ("reason", "shutdown")]
            ),
            1.0,
            "the batch sent through the pre-reserved permit is counted"
        );
        assert!(rig.inbox_tx.is_closed());
    }

    #[tokio::test(start_paused = true)]
    async fn a_node_already_stalled_at_shutdown_is_revoked_on_the_next_tick() {
        let rig = watcher_rig();
        let config = LuaRuntimeConfig {
            stall_after: Duration::from_millis(100),
            shutdown_grace: Duration::from_secs(2),
            ..Default::default()
        };
        let (_done_tx, done_rx) = oneshot::channel();
        let watcher = spawn_watcher(&rig, done_rx, config);

        rig.heartbeat.enter();
        tokio::time::sleep(Duration::from_secs(3)).await;
        assert_eq!(enrich_state(&rig), Some(NodeState::Stalled));
        assert!(!watcher.is_finished(), "no shutdown yet, so a stall alone never revokes");

        let signalled = tokio::time::Instant::now();
        rig.shutdown_tx.send(true).unwrap();
        let err = tokio::time::timeout(Duration::from_secs(5), watcher)
            .await
            .expect("a stalled node is revoked")
            .unwrap()
            .expect_err("a wedge is an error");
        let waited = signalled.elapsed();
        assert!(
            waited <= Duration::from_millis(100),
            "revoked {waited:?} after the signal; its quiet time already exceeded the grace"
        );
        assert!(err.to_string().contains("still inside process()/flush()"), "{err}");
        assert!(rig.inbox_tx.is_closed());
    }

    // -- A Lua node's stall and wedge path against a real Lua thread
    //    (`docs/adr/lua-runaway-script-bounds.md`). Real time only: a paused runtime auto-advances
    //    its clock whenever every task is idle, which it is while the Lua thread works on its own
    //    OS thread, so the watcher would see minutes pass in a few real milliseconds and report
    //    spurious stalls. A spinning `while true do end` thread outlives its test; nextest runs
    //    each test in its own process, which reclaims it.

    const SPIN_ON_SECOND_EVENT: &str = r#"
        local seen = 0
        function process(event)
            seen = seen + 1
            if seen > 1 then
                while true do end
            end
            return event
        end
    "#;

    fn one_counter_batch(name: &str) -> EventBatch {
        EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event(name, 1.0)],
        }
    }

    fn lua_spec(script: &str, interval: Option<Duration>, runtime: LuaRuntimeConfig) -> NodeSpec {
        NodeSpec::Lua { script: script.to_string(), interval, runtime }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_infinite_loop_script_is_reported_stalled_and_degrades_readyz() {
        let script = "function process(event) while true do end end";
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "enrich".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua { script: script.to_string(), interval: None, max_memory: None },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["enrich".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(OneShotInput { batch: Some(one_counter_batch("hits")) }),
                InputRuntimeConfig::default(),
            ),
        );
        let runtime = LuaRuntimeConfig {
            stall_after: Duration::from_millis(50),
            shutdown_grace: Duration::from_millis(100),
            ..Default::default()
        };
        specs.insert("enrich".to_string(), lua_spec(script, None, runtime));
        let (tx, _out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let (readiness, mut rx) = Readiness::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task =
            tokio::spawn(run_with_telemetry(g, specs, HashMap::new(), readiness, async move {
                let _ = shutdown_rx.await;
            }));

        tokio::time::timeout(
            Duration::from_secs(10),
            rx.wait_for(|s| s.components.get("enrich") == Some(&NodeState::Stalled)),
        )
        .await
        .expect("a spinning script must be reported stalled")
        .expect("readiness sender alive");
        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.phase, Phase::Ready, "a stall moves no phase");
        assert!(snapshot.has_stalled_node(), "which `/readyz` turns into `503 stalled`");

        shutdown_tx.send(()).unwrap();
        let err = tokio::time::timeout(Duration::from_secs(10), run_task)
            .await
            .expect("a wedged script must not hang shutdown")
            .unwrap()
            .expect_err("a wedged script fails the run");
        assert!(matches!(err, RunError::Runtime(_)));
        assert!(err.to_string().contains("enrich"), "{err}");
    }

    /// Feeds `before` at start; once shutdown fires, waits until `readiness` reports node
    /// `after_failed` as [`NodeState::Failed`] and sends `after`.
    struct WedgeFeedInput {
        before: Vec<EventBatch>,
        readiness: watch::Receiver<crate::readiness::PipelineState>,
        after_failed: &'static str,
        after: Option<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Input for WedgeFeedInput {
        async fn run(&mut self, _sink: Fanout) -> anyhow::Result<()> {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves")
        }

        async fn run_until_shutdown(
            &mut self,
            sink: Fanout,
            mut shutdown: watch::Receiver<bool>,
        ) -> anyhow::Result<()> {
            for batch in self.before.drain(..) {
                sink.send(batch).await;
            }
            let _ = shutdown.wait_for(|&due| due).await;
            let failed = self.after_failed;
            let _ = self
                .readiness
                .wait_for(|s| s.components.get(failed) == Some(&NodeState::Failed))
                .await;
            if let Some(batch) = self.after.take() {
                sink.send(batch).await;
            }
            Ok(())
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_with_a_wedged_script_revokes_its_io_and_returns_runtime_naming_it() {
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "enrich".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua {
                    script: SPIN_ON_SECOND_EVENT.to_string(),
                    interval: None,
                    max_memory: None,
                },
            ),
        );
        components.insert(
            "windowed".to_string(),
            plain_component(
                vec!["enrich".to_string()],
                ComponentKind::Aggregate {
                    interval: Duration::from_secs(3600),
                    temporality: logit_config::AggregateTemporality::default(),
                    series_retention: 5,
                    max_retained_series: 10_000,
                    distributions: logit_config::Distributions::default(),
                    max_samples_per_series: 1000,
                    sets: logit_config::Sets::default(),
                    max_set_members_per_series: 1000,
                },
            ),
        );
        components.insert(
            "out".to_string(),
            plain_component(vec!["windowed".to_string()], influxdb_out()),
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (readiness, rx) = Readiness::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(WedgeFeedInput {
                    // The first passes through into the window; the second wedges the script.
                    before: vec![one_counter_batch("kept"), one_counter_batch("wedge")],
                    // The join loop marks the node failed only after the watcher's revocation
                    // closed its inbox, so this send meets a revoked inbox.
                    readiness: rx.clone(),
                    after_failed: "enrich",
                    after: Some(one_counter_batch("late")),
                }),
                InputRuntimeConfig { shutdown_grace: Duration::from_secs(5) },
            ),
        );
        // Default sink graces: the Lua grace must be short enough that the window flushed after
        // revocation still beats the sink's default 5 s grace. With `stall_after` below the grace,
        // the node is revoked once its quiet time after shutdown reaches 1 s.
        let runtime = LuaRuntimeConfig {
            stall_after: Duration::from_millis(50),
            shutdown_grace: Duration::from_secs(1),
            ..Default::default()
        };
        specs.insert("enrich".to_string(), lua_spec(SPIN_ON_SECOND_EVENT, None, runtime));
        // The `aggregate` stand-in: holds everything until its close-time flush.
        specs.insert(
            "windowed".to_string(),
            NodeSpec::Transform(Box::new(WindowingTransform {
                interval: Duration::from_secs(3600),
                buffered: Vec::new(),
            })),
        );
        let (tx, out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let mut probe = TelemetryProbe::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "enrich", "windowed", "out"]
            .into_iter()
            .map(|id| (id.to_string(), probe.telemetry(id, "x", "x")))
            .collect();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task =
            tokio::spawn(run_with_telemetry(g, specs, telemetry, readiness, async move {
                let _ = shutdown_rx.await;
            }));

        // Shut down once the script is spinning on the second event, so the wedge is certain: a
        // preemption during the first can read as a passing stall.
        probe
            .wait_for("the script stalled on the second event", |t| {
                t.sum("logit.component.events.received", &[("component", "enrich")]) >= 2.0
                    && rx.borrow().components.get("enrich") == Some(&NodeState::Stalled)
            })
            .await;
        let started = std::time::Instant::now();
        shutdown_tx.send(()).unwrap();

        let err = tokio::time::timeout(Duration::from_secs(10), run_task)
            .await
            .expect("a wedged script must not hang shutdown")
            .unwrap()
            .expect_err("a wedged script fails the run");
        let elapsed = started.elapsed();
        // The Lua node's 1 s grace, counted from its last progress, is most of the run. 4 s is
        // under the input's and the sink's 5 s graces, so a run that waited out either fails here.
        assert!(elapsed < Duration::from_secs(4), "the run took {elapsed:?} after shutdown");
        assert!(matches!(err, RunError::Runtime(_)), "a wedge is a runtime failure: {err:?}");
        let message = err.to_string();
        assert!(message.contains("component 'enrich'"), "{message}");
        assert!(message.contains("still inside process()/flush()"), "{message}");

        let flushed: Vec<EventBatch> = out_rx.try_iter().collect();
        let names: Vec<&str> = flushed
            .iter()
            .flat_map(|b| b.events.iter())
            .flat_map(|e| e.metrics.iter())
            .map(|m| logit_core::interner::resolve(m.name))
            .collect();
        assert_eq!(names, ["kept"], "the downstream window's close-time flush reached the sink");

        assert_eq!(
            probe.sum(
                "logit.component.events.dropped",
                &[("component", "in"), ("reason", "closed_consumer")]
            ),
            1.0,
            "the send after revocation meets a closed inbox and is counted"
        );

        let snapshot = rx.borrow().clone();
        assert_eq!(snapshot.phase, Phase::Failed);
        assert_eq!(snapshot.components.get("enrich"), Some(&NodeState::Failed));
        assert_eq!(snapshot.components.get("windowed"), Some(&NodeState::Finished));
        assert_eq!(snapshot.components.get("out"), Some(&NodeState::Finished));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_progressing_flush_emitting_many_events_is_never_stalled() {
        let script = r#"
            function process(event) return nil end
            function flush(now)
                local out = {}
                for i = 1, 100000 do
                    out[i] = Event.new{timestamp = now, attributes = {i = i}}
                end
                return out
            end
        "#;
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "enrich".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua {
                    script: script.to_string(),
                    interval: Some(Duration::from_secs(3600)),
                    max_memory: None,
                },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["enrich".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        // Finishes at once, so the close-time `flush()` is the only work the node does.
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(one_counter_batch("hits")) }),
                InputRuntimeConfig::default(),
            ),
        );
        // Well above scheduling noise on a loaded test run; the flush takes over a second in a
        // debug build. The bound is not asserted, so a faster machine can't fail the test.
        let runtime = LuaRuntimeConfig {
            stall_after: Duration::from_millis(200),
            shutdown_grace: Duration::from_millis(50),
            ..Default::default()
        };
        specs.insert(
            "enrich".to_string(),
            lua_spec(script, Some(Duration::from_secs(3600)), runtime),
        );
        let (tx, out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> =
            [("enrich".to_string(), registry.telemetry_for("enrich", "x", "x"))].into();
        let (readiness, mut rx) = Readiness::channel();
        // Records every state the node passes through, so a stall that clears again is seen.
        let seen_stalled = tokio::spawn(async move {
            let mut seen = false;
            while rx.changed().await.is_ok() {
                seen |=
                    rx.borrow_and_update().components.get("enrich") == Some(&NodeState::Stalled);
            }
            seen
        });

        tokio::time::timeout(
            Duration::from_secs(60),
            run_with_telemetry(g, specs, telemetry, readiness, std::future::pending()),
        )
        .await
        .expect("the flush finishes")
        .expect("a progressing flush is a clean run");

        let emitted: usize = out_rx.try_iter().map(|b| b.events.len()).sum();
        assert_eq!(emitted, 100_000);
        assert!(!seen_stalled.await.unwrap(), "a flush making progress was reported stalled");
        assert_eq!(
            Totals::of(registry.drain(0)).sum(
                "logit.component.diagnostics",
                &[("component", "enrich"), ("key", "script_stalled")]
            ),
            0.0
        );
    }

    /// Batches a wedged node never read are destroyed with its inbox on revocation; each must be
    /// counted `dropped{reason="shutdown"}`, so the upstream's `sent` reconciles against the
    /// node's `received` plus those drops.
    #[tokio::test(flavor = "multi_thread")]
    async fn batches_queued_in_a_revoked_inbox_are_counted_not_silently_lost() {
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "enrich".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua {
                    script: SPIN_ON_SECOND_EVENT.to_string(),
                    interval: None,
                    max_memory: None,
                },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["enrich".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        // Under `CHANNEL_CAPACITY`, so every send completes: the first passes, the second wedges
        // the script, and the rest wait in its inbox.
        const SENT: usize = 20;
        let batches: Vec<EventBatch> = (0..SENT).map(|_| one_counter_batch("hits")).collect();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(BurstInput { batches }), InputRuntimeConfig::default()),
        );
        let runtime = LuaRuntimeConfig {
            stall_after: Duration::from_millis(50),
            shutdown_grace: Duration::from_millis(100),
            ..Default::default()
        };
        specs.insert("enrich".to_string(), lua_spec(SPIN_ON_SECOND_EVENT, None, runtime));
        let (tx, out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let mut probe = TelemetryProbe::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "enrich", "out"]
            .into_iter()
            .map(|id| (id.to_string(), probe.telemetry(id, "x", "x")))
            .collect();
        let (readiness, rx) = Readiness::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task =
            tokio::spawn(run_with_telemetry(g, specs, telemetry, readiness, async move {
                let _ = shutdown_rx.await;
            }));

        // Every batch is sent, and the node is stalled on the second event rather than on a
        // preemption during the first.
        probe
            .wait_for("every batch sent and the script stalled on the second event", |t| {
                t.sum("logit.component.batches.sent", &[("component", "in")]) >= SENT as f64
                    && t.sum("logit.component.events.received", &[("component", "enrich")]) >= 2.0
                    && rx.borrow().components.get("enrich") == Some(&NodeState::Stalled)
            })
            .await;
        shutdown_tx.send(()).unwrap();
        let err = tokio::time::timeout(Duration::from_secs(10), run_task)
            .await
            .expect("a wedged script must not hang shutdown")
            .unwrap()
            .expect_err("a wedged script fails the run");
        assert!(err.to_string().contains("component 'enrich'"), "{err}");
        let totals = probe.poll();

        let sent = totals.sum("logit.component.events.sent", &[("component", "in")]);
        let received = totals.sum("logit.component.events.received", &[("component", "enrich")]);
        let revoked = totals.sum(
            "logit.component.events.dropped",
            &[("component", "enrich"), ("reason", "shutdown")],
        );
        let revoked_batches = totals.sum(
            "logit.component.batches.dropped",
            &[("component", "enrich"), ("reason", "shutdown")],
        );
        let delivered: usize = out_rx.try_iter().map(|b| b.events.len()).sum();
        assert_eq!(sent, SENT as f64);
        assert_eq!(received, 2.0, "the passed event and the one the script is stuck on");
        assert_eq!(revoked, (SENT - 2) as f64, "every queued event is counted, none lost");
        assert_eq!(revoked_batches, (SENT - 2) as f64);
        assert_eq!(sent, received + revoked, "the upstream's sends reconcile");
        // Of what the node received, one event was delivered and one is held by the wedged call,
        // which never returns it.
        assert_eq!(delivered, 1);
    }

    /// Pins an accepted residual (`docs/adr/lua-runaway-script-bounds.md`): each `Event.new`
    /// advances the heartbeat, so a script constructing and discarding events in a loop reads as
    /// progress however long it runs, the same as a large `flush()`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_loop_that_keeps_constructing_events_is_progress_not_a_stall() {
        let script = r#"
            function process(event)
                for i = 1, 200000 do
                    local discarded = Event.new{timestamp = "1", attributes = {i = i}}
                end
                return event
            end
        "#;
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "enrich".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua { script: script.to_string(), interval: None, max_memory: None },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["enrich".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(one_counter_batch("hits")) }),
                InputRuntimeConfig::default(),
            ),
        );
        // Over 10x the worst heartbeat gap measured under CPU load (18 ms); the loop takes over a
        // second in a debug build. The bound is not asserted, so a faster machine can't fail the
        // test.
        let runtime = LuaRuntimeConfig {
            stall_after: Duration::from_millis(200),
            shutdown_grace: Duration::from_millis(20),
            ..Default::default()
        };
        specs.insert("enrich".to_string(), lua_spec(script, None, runtime));
        let (tx, out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let (readiness, mut rx) = Readiness::channel();
        let seen_stalled = tokio::spawn(async move {
            let mut seen = false;
            while rx.changed().await.is_ok() {
                seen |=
                    rx.borrow_and_update().components.get("enrich") == Some(&NodeState::Stalled);
            }
            seen
        });

        tokio::time::timeout(
            Duration::from_secs(60),
            run_with_telemetry(g, specs, HashMap::new(), readiness, std::future::pending()),
        )
        .await
        .expect("the loop finishes")
        .expect("a constructing loop is a clean run");

        assert_eq!(out_rx.try_iter().map(|b| b.events.len()).sum::<usize>(), 1);
        assert!(!seen_stalled.await.unwrap(), "a loop constructing events was reported stalled");
    }

    // -- `max_memory` (`docs/adr/lua-runaway-script-bounds.md`, decision 3). Real time: each
    //    runs a real Lua thread, and the verdict's rate limit reads the wall clock.

    /// Rendered self-log output from every thread.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<std::sync::Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedLogs {
        type Writer = CapturedLogs;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// A process-wide capture: a Lua node logs from its own OS thread, which a thread-local
    /// `set_default` never sees. Callers tell their lines apart by component id.
    fn global_logs() -> &'static CapturedLogs {
        static LOGS: std::sync::OnceLock<CapturedLogs> = std::sync::OnceLock::new();
        LOGS.get_or_init(|| {
            let logs = CapturedLogs::default();
            let subscriber =
                tracing_subscriber::fmt().with_writer(logs.clone()).with_ansi(false).finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
            logs
        })
    }

    /// Sends `batches` one every `every`, then returns.
    struct PacedInput {
        batches: Vec<EventBatch>,
        every: Duration,
    }

    #[async_trait::async_trait]
    impl Input for PacedInput {
        async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
            for batch in self.batches.drain(..) {
                sink.send(batch).await;
                tokio::time::sleep(self.every).await;
            }
            Ok(())
        }
    }

    fn counter_batch_of(n: usize) -> EventBatch {
        EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: (0..n).map(|_| counter_event("hits", 1.0)).collect(),
        }
    }

    /// What one `in -> <id> (lua) -> out` run under `max_memory` left behind.
    struct MaxMemoryRun {
        id: &'static str,
        result: Result<(), RunError>,
        telemetry: Totals,
        delivered: usize,
        elapsed: Duration,
        state: Option<NodeState>,
    }

    impl MaxMemoryRun {
        /// Self-log lines this run's Lua node wrote under diagnostic `key`.
        fn logged(&self, key: &str) -> usize {
            let text = String::from_utf8_lossy(&global_logs().0.lock().unwrap()).into_owned();
            let component = format!("component={}", self.id);
            text.lines().filter(|l| l.contains(&component) && l.contains(key)).count()
        }

        fn counter(&self, component: &str, name: &str, tag: Option<(&str, &str)>) -> f64 {
            let mut tags = vec![("component", component)];
            tags.extend(tag);
            self.telemetry.sum(name, &tags)
        }

        fn gc_forced(&self) -> f64 {
            self.counter(self.id, "logit.script.vm.gc.forced", None)
        }
    }

    async fn run_under_max_memory(
        id: &'static str,
        script: &str,
        interval: Option<Duration>,
        max_memory: usize,
        spacing: Duration,
        input: Box<dyn Input + Send>,
    ) -> MaxMemoryRun {
        global_logs();
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            id.to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua {
                    script: script.to_string(),
                    interval,
                    max_memory: Some(max_memory as u64),
                },
            ),
        );
        components.insert("out".to_string(), plain_component(vec![id.to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert("in".to_string(), NodeSpec::Input(input, InputRuntimeConfig::default()));
        let runtime = LuaRuntimeConfig {
            max_memory: Some(max_memory),
            min_verdict_spacing: spacing,
            ..Default::default()
        };
        specs.insert(id.to_string(), lua_spec(script, interval, runtime));
        let (tx, out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = ["in", id, "out"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();
        let (readiness, rx) = Readiness::channel();
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(60),
            run_with_telemetry(g, specs, telemetry, readiness, std::future::pending()),
        )
        .await
        .expect("the run ends on its own");
        let elapsed = started.elapsed();
        let state = rx.borrow().components.get(id).cloned();
        MaxMemoryRun {
            id,
            result,
            telemetry: Totals::of(registry.drain(0)),
            delivered: out_rx.try_iter().map(|b| b.events.len()).sum(),
            elapsed,
            state,
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_lua_node_over_max_memory_fails_the_run_as_runtime_naming_it() {
        // A unique ~1 KiB Lua string kept per event: ~2.5 MiB per batch, so the first batch ends
        // under the 4 MiB cap and the second ends over it with live data alone, and the first
        // verdict fails while the last two batches wait in the inbox.
        const BATCHES: usize = 4;
        const PER_BATCH: usize = 2500;
        let script = r#"
            kept = {}
            function process(event)
                kept[#kept + 1] = string.rep(string.format("%10d", #kept), 100)
                return event
            end
        "#;
        // Never finishes by itself: only the failure ends the run.
        let input =
            BurstInput { batches: (0..BATCHES).map(|_| counter_batch_of(PER_BATCH)).collect() };
        let run = run_under_max_memory(
            "mem_retains",
            script,
            None,
            4 << 20,
            MIN_VERDICT_SPACING,
            Box::new(input),
        )
        .await;

        let err = run.result.as_ref().expect_err("10 MB retained against a 4 MiB cap fails");
        assert!(matches!(err, RunError::Runtime(_)), "a memory failure is a runtime one: {err:?}");
        let message = err.to_string();
        assert!(message.contains("component 'mem_retains'"), "{message}");
        assert!(message.contains("over max_memory 4194304"), "{message}");
        assert!(!message.contains("panicked"), "{message}");
        assert_eq!(run.logged("memory_limit_exceeded"), 1);
        assert_eq!(run.logged("thread_panicked"), 0, "a memory failure isn't a panic");
        assert_eq!(run.state, Some(NodeState::Failed));

        let received = run.counter(run.id, "logit.component.events.received", None);
        let swept =
            run.counter(run.id, "logit.component.events.dropped", Some(("reason", "shutdown")));
        let refused = run.counter(
            "in",
            "logit.component.events.dropped",
            Some(("reason", "closed_consumer")),
        );
        assert!(received > 0.0 && received < (BATCHES * PER_BATCH) as f64, "{received}");
        // The batch that crossed the cap was sent before the verdict, so everything the script
        // returned reached the sink.
        assert_eq!(run.delivered as f64, received);
        assert!(swept > 0.0, "batches queued behind the failure are counted");
        assert_eq!(
            received + swept + refused,
            (BATCHES * PER_BATCH) as f64,
            "every event is delivered or counted"
        );
        // The `max_memory` failure's inbox sweep reaches `drain complete` like any shutdown drop.
        assert_eq!(
            logged_drain_complete_batches_dropped().map(|n| n as f64),
            Some(run.telemetry.sum("logit.component.batches.dropped", &[("reason", "shutdown")])),
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn garbage_over_max_memory_is_collected_before_the_node_is_failed() {
        // ~100 KiB live; each call leaves a ~4 MiB table that becomes garbage at return, so the
        // reading after every batch is over a 2 MiB cap until a full collection runs.
        let script = r#"
            live = {}
            for i = 1, 100 do live[i] = string.rep(string.format("%10d", i), 100) end
            function process(event)
                local t = {}
                for i = 1, 4000 do t[i] = string.rep(string.format("%10d", i), 100) end
                return event
            end
        "#;
        let input = FiniteBurstInput { batches: (0..100).map(|_| counter_batch_of(1)).collect() };
        let run = run_under_max_memory(
            "mem_garbage",
            script,
            None,
            2 << 20,
            MIN_VERDICT_SPACING,
            Box::new(input),
        )
        .await;

        run.result.as_ref().expect("garbage is collected, not counted against the cap");
        assert_eq!(run.delivered, 100);
        assert_eq!(run.logged("memory_limit_exceeded"), 0);
        let forced = run.gc_forced();
        assert!(forced >= 1.0, "the first over-cap reading forces a collection");
        let allowed = run.elapsed.as_secs_f64().ceil() + 1.0;
        assert!(forced <= allowed, "{forced} forced collections in {:?}", run.elapsed);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn max_memory_is_checked_after_flush_too() {
        // `process()` keeps nothing; each `flush()` keeps 10k ~1 KiB strings.
        let script = r#"
            kept = {}
            function process(event) return nil end
            function flush(now)
                for i = 1, 10000 do kept[#kept + 1] = string.rep(string.format("%10d", #kept), 100) end
            end
        "#;
        // An interval tick: the input never finishes, so only the tick's verdict ends the run.
        let input = BurstInput { batches: vec![counter_batch_of(1)] };
        let run = run_under_max_memory(
            "mem_tick",
            script,
            Some(Duration::from_millis(50)),
            4 << 20,
            MIN_VERDICT_SPACING,
            Box::new(input),
        )
        .await;
        let err = run.result.as_ref().expect_err("a tick's flush() over the cap fails the node");
        assert!(matches!(err, RunError::Runtime(_)), "{err:?}");
        assert!(err.to_string().contains("component 'mem_tick'"), "{err}");
        assert_eq!(run.logged("memory_limit_exceeded"), 1);
        assert_eq!(run.logged("thread_panicked"), 0);

        // The close-time flush: the input finishes, and the last `flush()` crosses the cap.
        let input = FiniteInput { batch: Some(counter_batch_of(1)) };
        let run = run_under_max_memory(
            "mem_close",
            script,
            Some(Duration::from_secs(3600)),
            4 << 20,
            MIN_VERDICT_SPACING,
            Box::new(input),
        )
        .await;
        let err = run.result.as_ref().expect_err("the close-time flush() is checked too");
        assert!(err.to_string().contains("over max_memory"), "{err}");
        assert_eq!(run.logged("memory_limit_exceeded"), 1);
        assert_eq!(run.state, Some(NodeState::Failed));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_memory_verdict_is_rate_limited() {
        // Each call leaves ~1 MiB of garbage; the cap sits 512 KiB above the VM's collected
        // size, so every batch ends over it and passes once collected.
        let script = r#"
            function process(event)
                local t = {}
                for i = 1, 1000 do t[i] = string.rep(string.format("%10d", i), 100) end
                return event
            end
        "#;
        let base = {
            let w =
                ScriptWorker::new(script).unwrap().with_telemetry(Telemetry::default()).unwrap();
            w.collect_until_under(0, 8).unwrap().used
        };
        // Unlimited, each of the 50 batches would force a collection. A 1 s spacing allows one per
        // started second of the run plus the close-time one, about two in ~0.5 s. `allowed`
        // comes from the measured run, so a slow run only raises it.
        let input = PacedInput {
            batches: (0..50).map(|_| counter_batch_of(1)).collect(),
            every: Duration::from_millis(10),
        };
        let run = run_under_max_memory(
            "mem_paced",
            script,
            None,
            base + (512 << 10),
            MIN_VERDICT_SPACING,
            Box::new(input),
        )
        .await;

        run.result.as_ref().expect("every verdict collects back under the cap");
        let forced = run.gc_forced();
        let allowed = run.elapsed.as_secs_f64().ceil() + 1.0;
        assert!(forced >= 1.0, "every batch ends over the cap");
        assert!(
            forced <= allowed,
            "50 over-cap batches in {:?} forced {forced} collections",
            run.elapsed
        );
    }

    /// A reading the rate limit skipped is not forgotten when no batch follows it: the loop
    /// wakes when the window ends and runs the verdict then.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_skipped_verdict_runs_once_the_window_ends_with_no_batch_arriving() {
        // The first batch leaves ~4 MiB of garbage (its verdict passes and opens a window); the
        // second keeps ~4 MiB, over the cap inside that window.
        let script = r#"
            kept = {}
            calls = 0
            function process(event)
                calls = calls + 1
                local t = calls == 1 and {} or kept
                for i = 1, 4000 do t[#t + 1] = string.rep(string.format("%10d", i), 100) end
                return event
            end
        "#;
        // Two batches, then silence: the input never finishes.
        let input = BurstInput { batches: vec![counter_batch_of(1), counter_batch_of(1)] };
        let run = run_under_max_memory(
            "mem_deferred",
            script,
            None,
            2 << 20,
            MIN_VERDICT_SPACING,
            Box::new(input),
        )
        .await;

        let err = run.result.as_ref().expect_err("the deferred verdict fails the node");
        assert!(err.to_string().contains("component 'mem_deferred'"), "{err}");
        assert_eq!(run.delivered, 2);
        assert_eq!(run.gc_forced(), 2.0, "the first batch's verdict, then the deferred one");
        assert!(
            run.elapsed >= MIN_VERDICT_SPACING.mul_f64(0.9),
            "the second verdict waits out the window: {:?}",
            run.elapsed
        );
    }

    /// An inbox that closes inside the rate-limit window doesn't take a deferred verdict with it:
    /// the verdict runs at close, with or without a close-time `flush()`.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_deferred_verdict_still_runs_when_the_inbox_closes_inside_the_window() {
        // The first batch leaves ~6 MiB of garbage (its verdict passes and opens a window); the
        // second keeps ~8 MiB, over the 4 MiB cap inside that window; then the input finishes.
        let script = r#"
            kept = {}
            calls = 0
            function process(event)
                calls = calls + 1
                local t, n = kept, 8000
                if calls == 1 then t, n = {}, 6000 end
                for i = 1, n do t[#t + 1] = string.rep(string.format("%10d", i), 100) end
                return event
            end
            function flush(now) end
        "#;
        // A window far past the bound below, so a run that waited it out can't pass, while two
        // forced collections over ~14 MiB in a debug build under load stay well inside the bound.
        const WINDOW: Duration = Duration::from_secs(60);
        for (id, interval) in
            [("mem_close_plain", None), ("mem_close_flush", Some(Duration::from_secs(3600)))]
        {
            let input =
                FiniteBurstInput { batches: vec![counter_batch_of(1), counter_batch_of(1)] };
            let run =
                run_under_max_memory(id, script, interval, 4 << 20, WINDOW, Box::new(input)).await;

            let err = run.result.as_ref().expect_err("the deferred verdict runs at close");
            assert!(matches!(err, RunError::Runtime(_)), "{id}: {err:?}");
            assert!(err.to_string().contains(&format!("component '{id}'")), "{err}");
            assert!(err.to_string().contains("over max_memory 4194304"), "{err}");
            assert_eq!(run.delivered, 2, "{id}");
            assert_eq!(run.gc_forced(), 2.0, "{id}: the first batch's verdict, then the close one");
            assert!(run.elapsed < Duration::from_secs(10), "{id}: no wait for the window");
            assert_eq!(run.state, Some(NodeState::Failed), "{id}");
        }
    }

    /// Never completes a send, so its store fills and its inbox backs up.
    struct NeverOutput;

    #[async_trait::async_trait]
    impl Output for NeverOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            std::future::pending::<()>().await;
            unreachable!("pending() never resolves")
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_lua_node_blocked_on_a_full_sink_inbox_unparks_within_the_sinks_grace_and_returns_ok()
    {
        let script = "function process(event) return event end";
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "enrich".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua { script: script.to_string(), interval: None, max_memory: None },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["enrich".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let batches: Vec<EventBatch> = (0..200).map(|_| one_counter_batch("hits")).collect();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(BurstInput { batches }), InputRuntimeConfig::default()),
        );
        // A grace below the sink's: if a parked send were read as a wedge, it would fire. Both
        // thresholds are over 10x the worst heartbeat gap measured under CPU load (18 ms), so a
        // preempted `process()` call is never read as a stall or a wedge.
        let runtime = LuaRuntimeConfig {
            stall_after: Duration::from_millis(250),
            shutdown_grace: Duration::from_millis(250),
            ..Default::default()
        };
        specs.insert("enrich".to_string(), lua_spec(script, None, runtime));
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(NeverOutput),
                SinkStoreConfig::Memory(SinkQueueConfig {
                    max_batches: 2,
                    max_bytes: u64::MAX,
                    overflow: OverflowPolicy::Block,
                }),
                WriteLoopConfig { shutdown_grace: Duration::from_secs(1), ..Default::default() },
            ),
        );

        let mut probe = TelemetryProbe::new();
        let telemetry: HashMap<String, Telemetry> = ["enrich", "out"]
            .into_iter()
            .map(|id| (id.to_string(), probe.telemetry(id, "x", "x")))
            .collect();
        let (readiness, rx) = Readiness::channel();
        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task =
            tokio::spawn(run_with_telemetry(g, specs, telemetry, readiness, async move {
                let _ = shutdown_rx.await;
            }));

        // The Lua node has sent enough to fill the sink's store and inbox, so its next send parks.
        probe
            .wait_for("the Lua node filled the sink", |t| {
                t.sum("logit.component.batches.sent", &[("component", "enrich")]) >= 66.0
            })
            .await;
        // A negative window: parked, outside any script call, for 4x `stall_after`, which a
        // parked send misread as a stall would have crossed.
        tokio::time::sleep(Duration::from_secs(1)).await;
        assert_eq!(rx.borrow().components.get("enrich"), Some(&NodeState::Running));

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), run_task)
            .await
            .expect("the sink's grace unparks the Lua node")
            .unwrap()
            .expect("a node parked by backpressure is never wedged");
        assert_eq!(
            probe.sum(
                "logit.component.diagnostics",
                &[("component", "enrich"), ("key", "script_stalled")]
            ),
            0.0
        );
        assert_eq!(rx.borrow().components.get("enrich"), Some(&NodeState::Finished));
    }

    /// A sink spec over [`NeverOutput`] whose store holds two batches, so its inbox fills soon
    /// after.
    fn stuck_sink() -> NodeSpec {
        NodeSpec::Output(
            Box::new(NeverOutput),
            SinkStoreConfig::Memory(SinkQueueConfig {
                max_batches: 2,
                max_bytes: u64::MAX,
                overflow: OverflowPolicy::Block,
            }),
            WriteLoopConfig { shutdown_grace: Duration::from_millis(100), ..Default::default() },
        )
    }

    /// Runs `g` under `specs` with a probe handle for every id in `ids`, returning the probe and
    /// the run's task, which the test aborts once it has read what it needs.
    fn run_probed(
        g: Graph,
        specs: HashMap<String, NodeSpec>,
        ids: &[&str],
    ) -> (TelemetryProbe, tokio::task::JoinHandle<Result<(), RunError>>) {
        let probe = TelemetryProbe::new();
        let telemetry: HashMap<String, Telemetry> =
            ids.iter().map(|id| (id.to_string(), probe.telemetry(id, "x", "x"))).collect();
        let run = tokio::spawn(run_with_telemetry(
            g,
            specs,
            telemetry,
            Readiness::disabled(),
            std::future::pending(),
        ));
        (probe, run)
    }

    #[tokio::test]
    async fn a_full_sink_inbox_in_a_fan_out_is_counted_as_inbox_full_on_that_sink() {
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        for sink in ["stuck", "good"] {
            components
                .insert(sink.to_string(), plain_component(vec!["in".to_string()], influxdb_out()));
        }
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (feed, rx) = mpsc::unbounded_channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(ChannelInput { rx }), InputRuntimeConfig::default()),
        );
        specs.insert("stuck".to_string(), stuck_sink());
        let (tx, _good_rx) = std::sync::mpsc::channel();
        specs.insert(
            "good".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let (mut probe, run) = run_probed(g, specs, &["in", "stuck", "good"]);
        // One batch at a time, each waited on until `good` has it, so `good`'s inbox never holds
        // more than one and only `stuck`'s can fill.
        let stuck_full =
            |t: &Totals| t.sum("logit.component.inbox.full", &[("component", "stuck")]);
        for k in 1..=200 {
            feed.send(one_counter_batch("hits")).expect("the input is running");
            let totals = probe
                .wait_for("good to receive the batch, or stuck's inbox to fill", |t| {
                    stuck_full(t) > 0.0
                        || t.sum("logit.component.batches.received", &[("component", "good")])
                            >= f64::from(k)
                })
                .await;
            if stuck_full(totals) > 0.0 {
                break;
            }
        }
        run.abort();

        let totals = probe.poll();
        assert!(stuck_full(totals) > 0.0, "stuck's inbox filled within 200 batches");
        assert_eq!(totals.sum("logit.component.inbox.full", &[("component", "good")]), 0.0);
        assert_eq!(
            totals.sum("logit.component.inbox.full", &[("component", "in")]),
            0.0,
            "never on the producer"
        );
    }

    /// `aa_out` sorts before `zz_in`, so the spawn loop takes `aa_out`'s handle out of the
    /// telemetry map before it builds `zz_in`'s `Fanout`. The edge must already hold it.
    #[tokio::test]
    async fn a_consumer_sorted_before_its_producer_still_counts_its_inbox_full() {
        let mut components = Map::new();
        components.insert("zz_in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "aa_out".to_string(),
            plain_component(vec!["zz_in".to_string()], influxdb_out()),
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let batches: Vec<EventBatch> = (0..200).map(|_| one_counter_batch("hits")).collect();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "zz_in".to_string(),
            NodeSpec::Input(Box::new(BurstInput { batches }), InputRuntimeConfig::default()),
        );
        specs.insert("aa_out".to_string(), stuck_sink());

        let (mut probe, run) = run_probed(g, specs, &["zz_in", "aa_out"]);
        probe
            .wait_for("aa_out's inbox.full", |t| {
                t.sum("logit.component.inbox.full", &[("component", "aa_out")]) > 0.0
            })
            .await;
        run.abort();
    }

    #[tokio::test]
    async fn inbox_depth_is_sampled_on_receive() {
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        // `Json` is an arity placeholder; the `MutatingTransform` spec is what runs.
        components.insert(
            "xform".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Json { skip_to_brace: false, invalid_utf8: Default::default() },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["xform".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let batches: Vec<EventBatch> = (0..3).map(|_| one_counter_batch("hits")).collect();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(BurstInput { batches }), InputRuntimeConfig::default()),
        );
        specs.insert("xform".to_string(), NodeSpec::Transform(Box::new(MutatingTransform)));
        let (tx, _out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let (mut probe, run) = run_probed(g, specs, &["in", "xform", "out"]);
        let depth = "logit.component.inbox.batches";
        probe
            .wait_for("an inbox depth sample from xform and out", |t| {
                t.has(depth, &[("component", "xform")]) && t.has(depth, &[("component", "out")])
            })
            .await;
        run.abort();

        let totals = probe.totals();
        assert!(totals.gauge(depth, &[("component", "xform")]).is_some(), "a gauge, not a sum");
        assert!(totals.gauge(depth, &[("component", "out")]).is_some(), "a gauge, not a sum");
        assert!(!totals.has(depth, &[("component", "in")]), "a listener's inbox is never fed");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_later_script_failing_to_load_returns_startup_promptly() {
        let good = "function process(event) return event end";
        let bad = "function process(event) return event";
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "a_ok".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua { script: good.to_string(), interval: None, max_memory: None },
            ),
        );
        components.insert(
            "b_bad".to_string(),
            plain_component(
                vec!["a_ok".to_string()],
                ComponentKind::Lua { script: bad.to_string(), interval: None, max_memory: None },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["b_bad".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(ForeverInput), InputRuntimeConfig::default()),
        );
        // `a_ok` sorts first, so its thread is running when `b_bad` fails to load. The early
        // return drops every `Sender` into `a_ok`'s inbox, so that thread exits on its own.
        specs.insert("a_ok".to_string(), lua_spec(good, None, LuaRuntimeConfig::default()));
        specs.insert("b_bad".to_string(), lua_spec(bad, None, LuaRuntimeConfig::default()));
        let (tx, _out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let started = std::time::Instant::now();
        let err = tokio::time::timeout(
            Duration::from_secs(10),
            run_with_telemetry(
                g,
                specs,
                HashMap::new(),
                Readiness::disabled(),
                std::future::pending(),
            ),
        )
        .await
        .expect("a load failure must not hang")
        .expect_err("a script that doesn't parse fails startup");
        assert!(started.elapsed() < Duration::from_secs(2), "took {:?}", started.elapsed());
        assert!(matches!(err, RunError::Startup(_)), "a load failure is a startup failure");
        assert!(err.to_string().contains("b_bad"), "the error names the failing script: {err}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_interval_tick_runs_flush_through_the_elapsed_branch() {
        let script = r#"
            function process(event) return event end
            function flush(now) return {Event.new{timestamp = now, attributes = {tick = true}}} end
        "#;
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components.insert(
            "enrich".to_string(),
            plain_component(
                vec!["in".to_string()],
                ComponentKind::Lua {
                    script: script.to_string(),
                    interval: Some(Duration::from_millis(50)),
                    max_memory: None,
                },
            ),
        );
        components
            .insert("out".to_string(), plain_component(vec!["enrich".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        // Sends nothing and never closes the inbox, so only the timeout branch can flush.
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(ForeverInput), InputRuntimeConfig::default()),
        );
        specs.insert(
            "enrich".to_string(),
            lua_spec(script, Some(Duration::from_millis(50)), LuaRuntimeConfig::default()),
        );
        let (tx, out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
        let run_task = tokio::spawn(run_with_telemetry(
            g,
            specs,
            HashMap::new(),
            Readiness::disabled(),
            async move {
                let _ = shutdown_rx.await;
            },
        ));

        let received = tokio::task::spawn_blocking(move || {
            out_rx.recv_timeout(Duration::from_secs(10)).map(|b| b.events.len())
        })
        .await
        .unwrap()
        .expect("an interval tick's flush() emission reaches the sink");
        assert_eq!(received, 1);

        shutdown_tx.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(10), run_task)
            .await
            .expect("shutdown completes")
            .unwrap()
            .expect("a clean run");
    }

    /// A sink whose destination refuses every batch holds its head and never ends `run`: exit
    /// code 2 is for a listener or Lua failure
    /// (`a_listener_failing_after_ready_flips_failed_and_returns_runtime`).
    #[tokio::test(start_paused = true)]
    async fn a_sustained_sink_failure_never_ends_run() {
        let mut components = Map::new();
        components.insert("bad_in".to_string(), plain_component(vec![], statsd_in()));
        components
            .insert("bad".to_string(), plain_component(vec!["bad_in".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (bad_tx, bad_rx) = mpsc::unbounded_channel();
        let (bad_output, mut bad_handles) = faulty_output(Fault::Refused, u32::MAX);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "bad_in".to_string(),
            NodeSpec::Input(Box::new(ChannelInput { rx: bad_rx }), InputRuntimeConfig::default()),
        );
        specs.insert(
            "bad".to_string(),
            NodeSpec::Output(
                Box::new(bad_output),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let mut run_task = tokio::spawn(run(g, specs));

        bad_tx
            .send(EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", 1.0)],
            })
            .expect("bad_in's receiver should still be alive");
        bad_handles.attempted.recv().await.expect("bad's first attempt should have happened");
        // Ten minutes of virtual time sizes the negative window: the head keeps being retried.
        let started = tokio::time::Instant::now();
        while started.elapsed() < Duration::from_secs(600) {
            bad_handles.attempted.recv().await.expect("bad keeps retrying");
        }
        assert!(
            tokio::time::timeout(Duration::from_secs(60), &mut run_task).await.is_err(),
            "a sink that never delivers must not end the run"
        );
        run_task.abort();
    }

    /// Exit codes match `docs/deploying.md`: 1 for startup, 2 for runtime.
    #[test]
    fn run_error_exit_codes() {
        assert_eq!(RunError::Startup(anyhow::anyhow!("x")).exit_code(), 1);
        assert_eq!(RunError::Runtime(anyhow::anyhow!("x")).exit_code(), 2);
    }

    /// A full run under receiver-less `Readiness::disabled()` doesn't panic.
    #[tokio::test]
    async fn readiness_disabled_never_panics_across_a_full_run() {
        let mut components = Map::new();
        components.insert("in".to_string(), plain_component(vec![], statsd_in()));
        components
            .insert("out".to_string(), plain_component(vec!["in".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(OneShotInput { batch: None }), InputRuntimeConfig::default()),
        );
        let (tx, _out_rx) = std::sync::mpsc::channel();
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        );

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let run_task = tokio::spawn(run_with_shutdown(g, specs, async {
            let _ = shutdown_rx.await;
        }));
        tokio::time::sleep(Duration::from_millis(10)).await;
        let _ = shutdown_tx.send(());
        run_task.await.expect("task should not panic").expect("clean shutdown should be Ok");
    }

    /// `run_transform` sends its emission as a [`TraceContext::child`] of the incoming batch.
    #[tokio::test]
    async fn run_transform_propagates_the_incoming_context_as_a_child() {
        let (in_tx, in_rx) = mpsc::channel(1);
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![out_tx]);
        let transform: Box<dyn Transform + Send> = Box::new(MutatingTransform);

        let parent = TraceContext::new_root();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };
        in_tx.send(Delivered::Owned(batch, parent.into())).await.expect("inbox should accept");
        drop(in_tx); // close the inbox so run_transform returns once it's drained

        run_transform(transform, in_rx, fanout, Telemetry::default())
            .await
            .expect("should complete without error");

        let received = out_rx.recv().await.expect("should receive").context();
        assert_eq!(
            received.trace_id, parent.trace_id,
            "the emitted batch should stay on the same trace as the one that produced it"
        );
        assert_ne!(
            received.span_id, parent.span_id,
            "the emission is its own hop -- it should mint a fresh span id, not reuse the parent's"
        );
    }

    /// A flush mints a fresh root, not the context of either absorbed batch.
    #[tokio::test]
    async fn run_transform_flush_mints_a_fresh_root_not_either_absorbed_batchs_context() {
        let (in_tx, in_rx) = mpsc::channel(2);
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![out_tx]);
        let transform: Box<dyn Transform + Send> = Box::new(WindowingTransform {
            interval: Duration::from_secs(3600),
            buffered: Vec::new(),
        });

        let ctx_a = TraceContext::new_root();
        let ctx_b = TraceContext::new_root();
        let batch = || EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };
        in_tx.send(Delivered::Owned(batch(), ctx_a.into())).await.expect("inbox should accept");
        in_tx.send(Delivered::Owned(batch(), ctx_b.into())).await.expect("inbox should accept");
        drop(in_tx); // close the inbox -- triggers the close-time flush that emits both, absorbed

        run_transform(transform, in_rx, fanout, Telemetry::default())
            .await
            .expect("should complete without error");

        let flushed = out_rx.recv().await.expect("the close-time flush should emit").context();
        assert_ne!(flushed.trace_id, ctx_a.trace_id);
        assert_ne!(flushed.trace_id, ctx_b.trace_id);
    }

    // -------------------------------------------------------------------------------------------
    // Spans
    // -------------------------------------------------------------------------------------------

    fn span_events(events: &[Event]) -> impl Iterator<Item = &Event> {
        events.iter().filter(|e| e.span.is_some())
    }

    fn span_op(event: &Event) -> Option<&str> {
        event.attributes.get("logit.node.op").and_then(|v| v.as_str())
    }

    /// One `Internal` `"process"` span per batch, parented on the incoming batch's context.
    #[tokio::test]
    async fn run_transform_records_one_span_per_incoming_batch_parented_on_that_batchs_context() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("t", "keep", "transform");
        let (in_tx, in_rx) = mpsc::channel(1);
        let (out_tx, out_rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![out_tx]);
        let transform: Box<dyn Transform + Send> = Box::new(MutatingTransform);

        let parent = TraceContext::new_root();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };
        in_tx.send(Delivered::Owned(batch, parent.into())).await.expect("inbox should accept");
        drop(in_tx);

        run_transform(transform, in_rx, fanout, telemetry)
            .await
            .expect("should complete without error");
        drop(out_rx);

        let events = registry.drain(0);
        let span_event = span_events(&events)
            .find(|e| span_op(e) == Some("process"))
            .expect("a process span should be recorded");
        let record = span_event.span.as_ref().expect("span record");
        assert_eq!(record.trace_id, parent.trace_id);
        assert_eq!(record.parent_span_id, Some(parent.span_id));
        assert_eq!(record.kind, SpanKind::Internal);
    }

    /// A span records the node's visit even when every event is absorbed.
    #[tokio::test]
    async fn a_transform_that_absorbs_every_event_still_records_a_span() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("agg", "aggregate", "transform");
        let (in_tx, in_rx) = mpsc::channel(1);
        let (out_tx, out_rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![out_tx]);
        let transform: Box<dyn Transform + Send> = Box::new(WindowingTransform {
            interval: Duration::from_secs(3600),
            buffered: Vec::new(),
        });

        let parent = TraceContext::new_root();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };
        in_tx.send(Delivered::Owned(batch, parent.into())).await.expect("inbox should accept");
        drop(in_tx);

        run_transform(transform, in_rx, fanout, telemetry)
            .await
            .expect("should complete without error");
        drop(out_rx);

        let events = registry.drain(0);
        assert!(
            span_events(&events).any(|e| span_op(e) == Some("process")),
            "an absorbing transform should still record a process span, got: {events:?}"
        );
    }

    /// One emission fanned out to two consumers records one span, not one per branch.
    #[tokio::test]
    async fn a_fan_out_records_exactly_one_span_not_one_per_branch() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("t", "keep", "transform");
        let (in_tx, in_rx) = mpsc::channel(1);
        let (out_tx_a, out_rx_a) = mpsc::channel(1);
        let (out_tx_b, out_rx_b) = mpsc::channel(1);
        let fanout = Fanout::new(vec![out_tx_a, out_tx_b]);
        let transform: Box<dyn Transform + Send> = Box::new(MutatingTransform);

        let parent = TraceContext::new_root();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            scope: None,
            events: vec![counter_event("hits", 1.0)],
        };
        in_tx.send(Delivered::Owned(batch, parent.into())).await.expect("inbox should accept");
        drop(in_tx);

        run_transform(transform, in_rx, fanout, telemetry)
            .await
            .expect("should complete without error");
        drop(out_rx_a);
        drop(out_rx_b);

        let events = registry.drain(0);
        let process_spans: Vec<&Event> =
            span_events(&events).filter(|e| span_op(e) == Some("process")).collect();
        assert_eq!(
            process_spans.len(),
            1,
            "one fan-out emission should record exactly one span, got {process_spans:?}"
        );
    }

    /// Flushes two resource groups per call, like `Aggregator`'s per-resource windowing.
    struct MultiGroupFlushTransform {
        links_for_first_group: Vec<SpanLink>,
    }

    impl Transform for MultiGroupFlushTransform {
        fn process(&mut self, _resource: &Arc<Resource>, _event: &mut Event) -> bool {
            false
        }

        fn flush_interval(&self) -> Option<Duration> {
            Some(Duration::from_secs(3600))
        }

        fn flush(
            &mut self,
            _now: i64,
        ) -> Vec<(Arc<Resource>, Option<Arc<Scope>>, Vec<(Event, Vec<SpanLink>)>)> {
            vec![
                (
                    Arc::new(Resource::default()),
                    None,
                    vec![(counter_event("a", 1.0), self.links_for_first_group.clone())],
                ),
                (Arc::new(Resource::default()), None, vec![(counter_event("b", 1.0), Vec::new())]),
            ]
        }
    }

    #[tokio::test]
    async fn run_flush_attaches_the_links_the_transform_produced_to_one_flush_span() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("agg", "aggregate", "transform");
        let (out_tx, out_rx) = mpsc::channel(2);
        let fanout = Fanout::new(vec![out_tx]);
        let link = SpanLink {
            trace_id: [7; 16],
            span_id: [7; 8],
            attributes: logit_core::AttrMap::new(),
            flags: 0,
            trace_state: None,
            dropped_attributes_count: 0,
        };
        let mut transform = MultiGroupFlushTransform { links_for_first_group: vec![link.clone()] };

        run_flush(&mut transform, &fanout, &telemetry).await;
        drop(out_rx);

        let events = registry.drain(0);
        let span_event =
            span_events(&events).find(|e| span_op(e) == Some("flush")).expect("a flush span");
        let record = span_event.span.as_ref().expect("span record");
        assert_eq!(record.links.len(), 1, "the one link the transform produced");
        assert_eq!(record.links[0].trace_id, link.trace_id);
        assert_eq!(record.links[0].span_id, link.span_id);
    }

    #[tokio::test]
    async fn run_flush_sends_every_resource_group_under_one_root_context() {
        let (out_tx, mut out_rx) = mpsc::channel(2);
        let fanout = Fanout::new(vec![out_tx]);
        let mut transform = MultiGroupFlushTransform { links_for_first_group: Vec::new() };

        run_flush(&mut transform, &fanout, &Telemetry::default()).await;

        let a = out_rx.recv().await.expect("the first group should send").context();
        let b = out_rx.recv().await.expect("the second group should send").context();
        assert_eq!(a, b, "both resource groups from one flush should share the identical context");
    }

    /// Flushes one group carrying `scope`, like `Aggregator`'s `(resource, scope)` grouping.
    struct ScopedFlushTransform {
        scope: Arc<Scope>,
    }

    impl Transform for ScopedFlushTransform {
        fn process(&mut self, _resource: &Arc<Resource>, _event: &mut Event) -> bool {
            false
        }

        fn flush_interval(&self) -> Option<Duration> {
            Some(Duration::from_secs(3600))
        }

        fn flush(
            &mut self,
            _now: i64,
        ) -> Vec<(Arc<Resource>, Option<Arc<Scope>>, Vec<(Event, Vec<SpanLink>)>)> {
            vec![(
                Arc::new(Resource::default()),
                Some(self.scope.clone()),
                vec![(counter_event("a", 1.0), Vec::new())],
            )]
        }
    }

    #[tokio::test]
    async fn run_flush_carries_the_transforms_scope_onto_the_outgoing_batch() {
        let (out_tx, mut out_rx) = mpsc::channel(2);
        let fanout = Fanout::new(vec![out_tx]);
        let scope =
            Arc::new(Scope { name: bytes::Bytes::from_static(b"scope-x"), ..Scope::default() });
        let mut transform = ScopedFlushTransform { scope: scope.clone() };

        run_flush(&mut transform, &fanout, &Telemetry::default()).await;

        let delivered = out_rx.recv().await.expect("the group should send");
        let batch = match delivered {
            Delivered::Owned(batch, _) => batch,
            Delivered::Shared(batch, _) => (*batch).clone(),
        };
        assert_eq!(
            batch.scope,
            Some(scope),
            "run_flush must carry the transform's own scope through"
        );
    }

    /// A failed delivery records a `Client` sink span with `Error` status and a `fault` tag.
    #[tokio::test]
    async fn write_loop_records_a_sink_span_with_error_status_and_a_fault_tag_on_a_failed_delivery()
    {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let (mut output, _handles) = faulty_output(Fault::Rejected, u32::MAX);
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        store.push((one_event_batch(1.0), TraceContext::new_root().into())).await;
        store.close();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        write_loop(
            "out".to_string(),
            &mut output,
            store,
            telemetry.clone(),
            WriteLoopConfig { retry: fast_retry_config(), ..WriteLoopConfig::default() },
            shutdown_rx,
            &AtomicU64::new(0),
        )
        .await;

        let events = registry.drain(0);
        let span_event =
            span_events(&events).find(|e| span_op(e) == Some("deliver")).expect("a sink span");
        let record = span_event.span.as_ref().expect("span record");
        assert_eq!(record.status, SpanStatus::Error);
        assert_eq!(record.kind, SpanKind::Client);
        assert_eq!(span_event.attributes.get("fault").and_then(|v| v.as_str()), Some("rejected"));
    }

    /// The sink span is parented on the context the batch was queued with.
    #[tokio::test]
    async fn write_loop_records_a_sink_span_parented_on_the_incoming_batchs_context() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let (mut output, _handles) = faulty_output(Fault::Clean, 0);
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        let parent = TraceContext::new_root();
        store.push((one_event_batch(1.0), parent.into())).await;
        store.close();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        write_loop(
            "out".to_string(),
            &mut output,
            store,
            telemetry.clone(),
            WriteLoopConfig::default(),
            shutdown_rx,
            &AtomicU64::new(0),
        )
        .await;

        let events = registry.drain(0);
        let span_event =
            span_events(&events).find(|e| span_op(e) == Some("deliver")).expect("a sink span");
        let record = span_event.span.as_ref().expect("span record");
        assert_eq!(record.trace_id, parent.trace_id);
        assert_eq!(record.parent_span_id, Some(parent.span_id));
        assert_eq!(record.status, SpanStatus::Ok);
    }

    // -------------------------------------------------------------------------------------------
    // Routers and targets (`docs/adr/target-components.md`)
    // -------------------------------------------------------------------------------------------

    /// Maps one attribute's value to a target slot, else [`Destination::Forward`]; a local
    /// stand-in for `logit-transforms::Route`.
    struct SplitByAttr {
        key: String,
        values: Vec<(String, u16)>,
    }

    impl SplitByAttr {
        fn new(key: &str, values: &[(&str, u16)]) -> Self {
            Self {
                key: key.to_string(),
                values: values.iter().map(|(v, slot)| ((*v).to_string(), *slot)).collect(),
            }
        }
    }

    impl Router for SplitByAttr {
        fn route(&mut self, _resource: &Arc<Resource>, event: &Event) -> Destination {
            let Some(value) = event.attributes.get(&self.key).and_then(|v| v.as_str()) else {
                return Destination::Forward;
            };
            self.values
                .iter()
                .find(|(candidate, _)| candidate == value)
                .map_or(Destination::Forward, |(_, slot)| Destination::To(*slot))
        }
    }

    /// Passes events through and reports each batch's [`Provenance`], which a sink never sees.
    struct RecordProvenance {
        tx: std::sync::mpsc::Sender<Provenance>,
    }

    impl Transform for RecordProvenance {
        fn process(&mut self, _resource: &Arc<Resource>, _event: &mut Event) -> bool {
            true
        }

        fn observe_provenance(&mut self, provenance: Provenance) {
            let _ = self.tx.send(provenance);
        }
    }

    fn tagged_event(name: &str, stream: Option<&str>) -> Event {
        let mut event = counter_event(name, 1.0);
        if let Some(stream) = stream {
            event.attributes.insert("stream", stream);
        }
        event
    }

    fn stream_tag(event: &Event) -> Option<&str> {
        event.attributes.get("stream").and_then(|v| v.as_str())
    }

    fn symbol_name(symbol: Option<logit_core::Symbol>) -> Option<String> {
        symbol.map(|s| logit_core::interner::resolve(s).to_string())
    }

    /// Builds a resolved [`Graph`] from `(id, sources, targets, kind)` tuples without
    /// `graph::resolve`, producing the `ResolvedComponent`s it would.
    ///
    /// A router here is a `lua`-kind component with `targets:`, handed a `NodeSpec::Router` (spec
    /// kind and config kind are independent at this layer). A target must be a real
    /// `ComponentKind::Target {}`: `Role::Target` is what the runtime's target passes key off.
    fn routed_graph(nodes: Vec<(&str, Vec<&str>, Vec<&str>, ComponentKind)>) -> Graph {
        let consumers: HashMap<String, Vec<String>> = nodes
            .iter()
            .map(|(id, ..)| {
                let list = nodes
                    .iter()
                    .filter(|(_, sources, _, _)| sources.contains(id))
                    .map(|(consumer, ..)| (*consumer).to_string())
                    .collect();
                ((*id).to_string(), list)
            })
            .collect();

        let mut components: HashMap<String, graph::ResolvedComponent> = HashMap::new();
        for (id, sources, targets, kind) in nodes {
            components.insert(
                id.to_string(),
                graph::ResolvedComponent {
                    sources: sources.into_iter().map(String::from).collect(),
                    consumers: consumers[id].clone(),
                    targets: targets.into_iter().map(String::from).collect(),
                    kind,
                    buffer: logit_config::BufferConfig::default(),
                    receive: logit_config::ReceiveConfig::default(),
                },
            );
        }
        let mut topological_order: Vec<String> = components.keys().cloned().collect();
        topological_order.sort();
        Graph { components, topological_order }
    }

    fn recording_sink(tx: std::sync::mpsc::Sender<EventBatch>) -> NodeSpec {
        NodeSpec::Output(
            Box::new(RecordingOutput { tx }),
            SinkStoreConfig::Memory(SinkQueueConfig::default()),
            WriteLoopConfig::default(),
        )
    }

    fn one_batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    /// A router partitions one batch across two targets and its own edge, with no duplicates.
    #[tokio::test]
    async fn a_router_partitions_a_batch_across_two_targets() {
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a", "b"],
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            ("b", vec![], vec![], ComponentKind::Target {}),
            ("sink_a", vec!["a"], vec![], influxdb_out()),
            ("sink_b", vec!["b"], vec![], influxdb_out()),
            ("sink_fwd", vec!["split"], vec![], influxdb_out()),
        ]);

        let (tx_a, rx_a) = std::sync::mpsc::channel();
        let (tx_b, rx_b) = std::sync::mpsc::channel();
        let (tx_fwd, rx_fwd) = std::sync::mpsc::channel();

        let batch = one_batch(vec![
            tagged_event("hits_a", Some("a")),
            tagged_event("hits_none", None),
            tagged_event("hits_b", Some("b")),
            tagged_event("hits_a2", Some("a")),
            tagged_event("hits_other", Some("zzz")),
        ]);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "split".to_string(),
            NodeSpec::Router(Box::new(SplitByAttr::new("stream", &[("a", 0), ("b", 1)]))),
        );
        // As the registry does; the run works with or without them.
        specs.insert("a".to_string(), NodeSpec::Target);
        specs.insert("b".to_string(), NodeSpec::Target);
        specs.insert("sink_a".to_string(), recording_sink(tx_a));
        specs.insert("sink_b".to_string(), recording_sink(tx_b));
        specs.insert("sink_fwd".to_string(), recording_sink(tx_fwd));

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let received_a = rx_a.recv_timeout(Duration::from_secs(1)).expect("sink_a gets a batch");
        let received_b = rx_b.recv_timeout(Duration::from_secs(1)).expect("sink_b gets a batch");
        let received_fwd =
            rx_fwd.recv_timeout(Duration::from_secs(1)).expect("sink_fwd gets a batch");

        assert_eq!(
            received_a.events.iter().map(stream_tag).collect::<Vec<_>>(),
            vec![Some("a"), Some("a")],
            "target a's consumer should see exactly the stream=a events, in order"
        );
        assert_eq!(received_b.events.iter().map(stream_tag).collect::<Vec<_>>(), vec![Some("b")],);
        assert_eq!(
            received_fwd.events.iter().map(stream_tag).collect::<Vec<_>>(),
            vec![None, Some("zzz")],
            "an absent key and an unmatched value are both unrouted, not routed to slot 0's target"
        );
        assert!(rx_a.recv_timeout(Duration::from_millis(50)).is_err(), "no duplicate batch");
        assert!(rx_b.recv_timeout(Duration::from_millis(50)).is_err(), "no duplicate batch");
        assert!(rx_fwd.recv_timeout(Duration::from_millis(50)).is_err(), "no duplicate batch");
    }

    /// When no route claims anything, the whole batch reaches the router's ordinary consumer.
    #[tokio::test]
    async fn unrouted_events_reach_the_routers_ordinary_consumers() {
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            ("sink_a", vec!["a"], vec![], influxdb_out()),
            ("sink_fwd", vec!["split"], vec![], influxdb_out()),
        ]);

        let (tx_a, rx_a) = std::sync::mpsc::channel();
        let (tx_fwd, rx_fwd) = std::sync::mpsc::channel();
        let batch = one_batch(vec![
            tagged_event("one", None),
            tagged_event("two", Some("nothing_routes_this")),
        ]);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "split".to_string(),
            NodeSpec::Router(Box::new(SplitByAttr::new("stream", &[("a", 0)]))),
        );
        specs.insert("sink_a".to_string(), recording_sink(tx_a));
        specs.insert("sink_fwd".to_string(), recording_sink(tx_fwd));

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let forwarded =
            rx_fwd.recv_timeout(Duration::from_secs(1)).expect("sink_fwd gets the whole batch");
        assert_eq!(forwarded.events.len(), 2);
        assert!(
            rx_a.recv_timeout(Duration::from_millis(50)).is_err(),
            "target a claimed nothing, so its consumer must see no batch at all"
        );
    }

    /// With no ordinary consumers, unrouted events are counted as dropped and the run terminates.
    #[tokio::test]
    async fn unrouted_events_are_counted_when_a_router_has_no_ordinary_consumers() {
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            ("sink_a", vec!["a"], vec![], influxdb_out()),
        ]);

        let (tx_a, rx_a) = std::sync::mpsc::channel();
        let batch = one_batch(vec![
            tagged_event("routed", Some("a")),
            tagged_event("unrouted_one", None),
            tagged_event("unrouted_two", Some("zzz")),
        ]);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "split".to_string(),
            NodeSpec::Router(Box::new(SplitByAttr::new("stream", &[("a", 0)]))),
        );
        specs.insert("a".to_string(), NodeSpec::Target);
        specs.insert("sink_a".to_string(), recording_sink(tx_a));

        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "split", "a", "sink_a"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, telemetry, Readiness::disabled(), std::future::pending()),
        )
        .await
        .expect("a router whose forward partition has nowhere to go must not hang the run")
        .expect("run should complete without error");

        let routed = rx_a.recv_timeout(Duration::from_secs(1)).expect("sink_a gets the routed one");
        assert_eq!(routed.events.len(), 1);

        let events = registry.drain(0);
        let dropped = events
            .iter()
            .filter(|e| e.attributes.get("component").and_then(|v| v.as_str()) == Some("split"))
            .filter(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("unrouted"))
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name)
                            == "logit.component.events.dropped" =>
                    {
                        Some(s.value)
                    }
                    _ => None,
                })
            });
        assert_eq!(
            dropped,
            Some(2.0),
            "both unrouted events should be counted under the router, got: {events:?}"
        );
    }

    /// Downstream of a target, `previous` is the target's id and `origin` is still the listener.
    #[tokio::test]
    async fn previous_downstream_of_a_target_is_the_targets_id_and_origin_is_untouched() {
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            (
                "watcher",
                vec!["a"],
                vec![],
                ComponentKind::Json { skip_to_brace: false, invalid_utf8: Default::default() },
            ),
        ]);

        let (tx, rx) = std::sync::mpsc::channel();
        let batch = one_batch(vec![tagged_event("routed", Some("a"))]);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "split".to_string(),
            NodeSpec::Router(Box::new(SplitByAttr::new("stream", &[("a", 0)]))),
        );
        specs.insert("a".to_string(), NodeSpec::Target);
        specs.insert("watcher".to_string(), NodeSpec::Transform(Box::new(RecordProvenance { tx })));

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let provenance =
            rx.recv_timeout(Duration::from_secs(1)).expect("the target's consumer sees a batch");
        assert_eq!(
            symbol_name(provenance.origin).as_deref(),
            Some("in"),
            "origin still names the listener that created the batch"
        );
        assert_eq!(
            symbol_name(provenance.previous).as_deref(),
            Some("a"),
            "previous is the target's id, not the router's -- the target Fanout did the stamping"
        );
    }

    /// A router batch forked to three destinations records one span.
    #[tokio::test]
    async fn one_incoming_batch_forks_into_one_span_however_many_destinations() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("split", "route", "transform");
        let (in_tx, in_rx) = mpsc::channel(1);
        let (fwd_tx, fwd_rx) = mpsc::channel(1);
        let (a_tx, a_rx) = mpsc::channel(1);
        let (b_tx, b_rx) = mpsc::channel(1);

        let parent = TraceContext::new_root();
        let batch = one_batch(vec![
            tagged_event("one", Some("a")),
            tagged_event("two", Some("b")),
            tagged_event("three", None),
        ]);
        in_tx.send(Delivered::Owned(batch, parent.into())).await.expect("inbox should accept");
        drop(in_tx);

        run_router(
            Box::new(SplitByAttr::new("stream", &[("a", 0), ("b", 1)])),
            in_rx,
            Fanout::new(vec![fwd_tx]),
            vec![Fanout::new(vec![a_tx]), Fanout::new(vec![b_tx])],
            telemetry,
        )
        .await
        .expect("run_router should return Ok once its inbox closes");
        drop(fwd_rx);
        drop(a_rx);
        drop(b_rx);

        let events = registry.drain(0);
        let process_spans: Vec<&Event> =
            span_events(&events).filter(|e| span_op(e) == Some("process")).collect();
        assert_eq!(
            process_spans.len(),
            1,
            "three destinations, one incoming batch, one span -- got {process_spans:?}"
        );
        let record = process_spans[0].span.as_ref().expect("span record");
        assert_eq!(record.trace_id, parent.trace_id);
        assert_eq!(record.parent_span_id, Some(parent.span_id));
        assert_eq!(record.kind, SpanKind::Internal);
        assert_eq!(
            process_spans[0].attributes.get("events"),
            Some(&logit_core::Value::I64(3)),
            "the one span counts every event routed, across every destination"
        );
    }

    /// The shutdown cascade reaches past targets (`drop(target_fanouts)`); a hang is the failure.
    #[tokio::test]
    async fn a_router_exiting_closes_its_targets_consumers_inboxes() {
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a", "b"],
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            ("b", vec![], vec![], ComponentKind::Target {}),
            ("sink_a", vec!["a"], vec![], influxdb_out()),
            ("sink_b", vec!["b"], vec![], influxdb_out()),
        ]);

        let (tx_a, rx_a) = std::sync::mpsc::channel();
        let (tx_b, rx_b) = std::sync::mpsc::channel();
        let batch = one_batch(vec![tagged_event("a1", Some("a")), tagged_event("b1", Some("b"))]);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "split".to_string(),
            NodeSpec::Router(Box::new(SplitByAttr::new("stream", &[("a", 0), ("b", 1)]))),
        );
        specs.insert("a".to_string(), NodeSpec::Target);
        specs.insert("b".to_string(), NodeSpec::Target);
        specs.insert("sink_a".to_string(), recording_sink(tx_a));
        specs.insert("sink_b".to_string(), recording_sink(tx_b));

        let (readiness, _rx) = Readiness::channel();
        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, HashMap::new(), readiness.clone(), std::future::pending()),
        )
        .await
        .expect("the shutdown cascade must reach past both targets -- a hang here is the bug")
        .expect("run should complete without error");

        rx_a.recv_timeout(Duration::from_secs(1)).expect("sink_a should have been delivered to");
        rx_b.recv_timeout(Duration::from_secs(1)).expect("sink_b should have been delivered to");

        let snapshot = readiness.snapshot();
        assert_eq!(
            snapshot.components.get("a"),
            Some(&NodeState::Alias),
            "a target has no task, so it never leaves Alias"
        );
        assert_eq!(snapshot.components.get("b"), Some(&NodeState::Alias));
        assert_eq!(
            snapshot.components.get("sink_a"),
            Some(&NodeState::Finished),
            "a sink reaching Finished is its inbox having closed and its queue having drained"
        );
        assert_eq!(snapshot.components.get("sink_b"), Some(&NodeState::Finished));
    }

    /// Two routers directing at one target both deliver to its consumer.
    #[tokio::test]
    async fn two_routers_directing_at_one_target_both_deliver() {
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "split_a",
                vec!["in"],
                vec!["shared"],
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
            ),
            (
                "split_b",
                vec!["in"],
                vec!["shared"],
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
            ),
            ("shared", vec![], vec![], ComponentKind::Target {}),
            ("sink", vec!["shared"], vec![], influxdb_out()),
        ]);

        let (tx, rx) = std::sync::mpsc::channel();
        let batch =
            one_batch(vec![tagged_event("from_a", Some("a")), tagged_event("from_b", Some("b"))]);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        // Each router claims one event for `shared`; the other is dropped as unrouted.
        specs.insert(
            "split_a".to_string(),
            NodeSpec::Router(Box::new(SplitByAttr::new("stream", &[("a", 0)]))),
        );
        specs.insert(
            "split_b".to_string(),
            NodeSpec::Router(Box::new(SplitByAttr::new("stream", &[("b", 0)]))),
        );
        specs.insert("shared".to_string(), NodeSpec::Target);
        specs.insert("sink".to_string(), recording_sink(tx));

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let mut delivered: Vec<String> = Vec::new();
        while let Ok(batch) = rx.recv_timeout(Duration::from_secs(1)) {
            for event in &batch.events {
                delivered.push(
                    event
                        .metrics
                        .first()
                        .map(|m| logit_core::interner::resolve(m.name).to_string())
                        .expect("a counter event"),
                );
            }
        }
        delivered.sort();
        assert_eq!(
            delivered,
            vec!["from_a".to_string(), "from_b".to_string()],
            "both routers' claims should land at the one shared target's consumer"
        );
    }

    /// `route_batch` yields slot-ordered, non-empty partitions and leaves every buffer capacity 0.
    #[test]
    fn route_batch_partitions_into_slot_order_and_leaves_its_scratch_empty_for_reuse() {
        let mut router = SplitByAttr::new("stream", &[("a", 0), ("b", 1)]);
        let mut scratch = RouterScratch::new(2);
        assert_eq!(scratch.destinations(), 3, "Forward plus one slot per target");
        let telemetry = Telemetry::default();

        // Warm-up: gives every buffer an allocation to be taken away.
        let warmed = route_batch(
            &mut router,
            &mut scratch,
            one_batch(vec![
                tagged_event("w_a", Some("a")),
                tagged_event("w_b", Some("b")),
                tagged_event("w_none", None),
            ]),
            &telemetry,
        );
        assert_eq!(warmed.len(), 3);
        assert!(
            scratch.dests.iter().all(|dest| dest.is_empty() && dest.capacity() == 0),
            "every buffer must be handed out by mem::take, leaving a capacity-0 Vec behind"
        );

        let partitions = route_batch(
            &mut router,
            &mut scratch,
            one_batch(vec![
                tagged_event("one", Some("b")),
                tagged_event("two", None),
                tagged_event("three", Some("b")),
            ]),
            &telemetry,
        );

        let shape: Vec<(usize, Vec<Option<&str>>)> = partitions
            .iter()
            .map(|(slot, batch)| (*slot, batch.events.iter().map(stream_tag).collect()))
            .collect();
        assert_eq!(
            shape,
            vec![(0, vec![None]), (2, vec![Some("b"), Some("b")])],
            "slot 0 is Forward, slot 2 is To(1); slot 1 received nothing so it yields no partition"
        );
        assert!(
            scratch.dests.iter().all(|dest| dest.is_empty() && dest.capacity() == 0),
            "the scratch must come back empty and capacity-free for the next batch"
        );
    }

    // -------------------------------------------------------------------------------------------
    // Lua routers (`docs/adr/target-components.md`)
    // -------------------------------------------------------------------------------------------

    /// Sends each batch in turn, then returns.
    struct FiniteBatches {
        batches: Vec<EventBatch>,
    }

    #[async_trait::async_trait]
    impl Input for FiniteBatches {
        async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
            for batch in self.batches.drain(..) {
                sink.send(batch).await;
            }
            Ok(())
        }
    }

    /// The ADR's example: `event:to(..)` per stream, `return event` otherwise.
    const SPLIT_SCRIPT: &str = r#"
        function process(event)
            if event.attributes.stream == "a" then
                return event:to("a")
            elseif event.attributes.stream == "b" then
                return event:to("b")
            end
            return event
        end
    "#;

    /// A Lua router's `event:to(..)` marks split one batch across two targets and its own edge.
    #[tokio::test]
    async fn a_lua_router_splits_a_batch_two_ways() {
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a", "b"],
                ComponentKind::Lua {
                    script: SPLIT_SCRIPT.to_string(),
                    interval: None,
                    max_memory: None,
                },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            ("b", vec![], vec![], ComponentKind::Target {}),
            ("sink_a", vec!["a"], vec![], influxdb_out()),
            ("sink_b", vec!["b"], vec![], influxdb_out()),
            ("sink_fwd", vec!["split"], vec![], influxdb_out()),
        ]);

        let (tx_a, rx_a) = std::sync::mpsc::channel();
        let (tx_b, rx_b) = std::sync::mpsc::channel();
        let (tx_fwd, rx_fwd) = std::sync::mpsc::channel();

        let batch = one_batch(vec![
            tagged_event("hits_a", Some("a")),
            tagged_event("hits_none", None),
            tagged_event("hits_b", Some("b")),
            tagged_event("hits_a2", Some("a")),
            tagged_event("hits_other", Some("zzz")),
        ]);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "split".to_string(),
            NodeSpec::Lua {
                script: SPLIT_SCRIPT.to_string(),
                interval: None,
                runtime: LuaRuntimeConfig::default(),
            },
        );
        specs.insert("a".to_string(), NodeSpec::Target);
        specs.insert("b".to_string(), NodeSpec::Target);
        specs.insert("sink_a".to_string(), recording_sink(tx_a));
        specs.insert("sink_b".to_string(), recording_sink(tx_b));
        specs.insert("sink_fwd".to_string(), recording_sink(tx_fwd));

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let received_a = rx_a.recv_timeout(Duration::from_secs(1)).expect("sink_a gets a batch");
        let received_b = rx_b.recv_timeout(Duration::from_secs(1)).expect("sink_b gets a batch");
        let received_fwd =
            rx_fwd.recv_timeout(Duration::from_secs(1)).expect("sink_fwd gets a batch");

        assert_eq!(
            received_a.events.iter().map(stream_tag).collect::<Vec<_>>(),
            vec![Some("a"), Some("a")],
            "target a's consumer should see exactly the events the script marked for it, in order"
        );
        assert_eq!(received_b.events.iter().map(stream_tag).collect::<Vec<_>>(), vec![Some("b")]);
        assert_eq!(
            received_fwd.events.iter().map(stream_tag).collect::<Vec<_>>(),
            vec![None, Some("zzz")],
            "an event the script never marked is unrouted, not routed to slot 0's target"
        );
        assert!(rx_a.recv_timeout(Duration::from_millis(50)).is_err(), "no duplicate batch");
        assert!(rx_b.recv_timeout(Duration::from_millis(50)).is_err(), "no duplicate batch");
        assert!(rx_fwd.recv_timeout(Duration::from_millis(50)).is_err(), "no duplicate batch");
    }

    /// An unmarked event reaches the Lua router's ordinary consumer.
    #[tokio::test]
    async fn an_unmarked_event_reaches_the_lua_routers_ordinary_consumers() {
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a", "b"],
                ComponentKind::Lua {
                    script: SPLIT_SCRIPT.to_string(),
                    interval: None,
                    max_memory: None,
                },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            ("b", vec![], vec![], ComponentKind::Target {}),
            ("sink_a", vec!["a"], vec![], influxdb_out()),
            ("sink_fwd", vec!["split"], vec![], influxdb_out()),
        ]);

        let (tx_a, rx_a) = std::sync::mpsc::channel();
        let (tx_fwd, rx_fwd) = std::sync::mpsc::channel();
        let batch =
            one_batch(vec![tagged_event("one", None), tagged_event("two", Some("nothing"))]);

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(batch) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "split".to_string(),
            NodeSpec::Lua {
                script: SPLIT_SCRIPT.to_string(),
                interval: None,
                runtime: LuaRuntimeConfig::default(),
            },
        );
        specs.insert("a".to_string(), NodeSpec::Target);
        specs.insert("b".to_string(), NodeSpec::Target);
        specs.insert("sink_a".to_string(), recording_sink(tx_a));
        specs.insert("sink_fwd".to_string(), recording_sink(tx_fwd));

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let forwarded =
            rx_fwd.recv_timeout(Duration::from_secs(1)).expect("sink_fwd gets the whole batch");
        assert_eq!(forwarded.events.len(), 2);
        assert!(
            rx_a.recv_timeout(Duration::from_millis(50)).is_err(),
            "the script marked nothing for target a, so its consumer must see no batch at all"
        );
    }

    /// `event:to` an unknown id is a counted script error; the node keeps running.
    #[tokio::test]
    async fn an_unknown_target_in_lua_counts_a_script_error_and_does_not_kill_the_node() {
        let script = r#"
            function process(event)
                if event.attributes.stream == "bad" then
                    return event:to("nope")
                end
                return event
            end
        "#;
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua { script: script.to_string(), interval: None, max_memory: None },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            ("sink_a", vec!["a"], vec![], influxdb_out()),
            ("sink_fwd", vec!["split"], vec![], influxdb_out()),
        ]);

        let (tx_a, _rx_a) = std::sync::mpsc::channel();
        let (tx_fwd, rx_fwd) = std::sync::mpsc::channel();

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteBatches {
                    batches: vec![
                        one_batch(vec![
                            tagged_event("boom", Some("bad")),
                            tagged_event("fine", None),
                        ]),
                        one_batch(vec![tagged_event("later", None)]),
                    ],
                }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "split".to_string(),
            NodeSpec::Lua {
                script: script.to_string(),
                interval: None,
                runtime: LuaRuntimeConfig::default(),
            },
        );
        specs.insert("a".to_string(), NodeSpec::Target);
        specs.insert("sink_a".to_string(), recording_sink(tx_a));
        specs.insert("sink_fwd".to_string(), recording_sink(tx_fwd));

        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = ["in", "split", "a", "sink_a", "sink_fwd"]
            .into_iter()
            .map(|id| (id.to_string(), registry.telemetry_for(id, "x", "x")))
            .collect();

        tokio::time::timeout(
            Duration::from_secs(5),
            run_with_telemetry(g, specs, telemetry, Readiness::disabled(), std::future::pending()),
        )
        .await
        .expect("a script error must not park or kill the node")
        .expect("run should complete without error");

        let mut forwarded: Vec<String> = Vec::new();
        while let Ok(batch) = rx_fwd.recv_timeout(Duration::from_millis(500)) {
            for event in &batch.events {
                forwarded.push(
                    event
                        .metrics
                        .first()
                        .map(|m| logit_core::interner::resolve(m.name).to_string())
                        .expect("a counter event"),
                );
            }
        }
        assert_eq!(
            forwarded,
            vec!["fine".to_string(), "later".to_string()],
            "the erroring event is lost, but its batch-mate and the whole next batch still flow"
        );

        let events = registry.drain(0);
        let errors = events
            .iter()
            .filter(|e| e.attributes.get("component").and_then(|v| v.as_str()) == Some("split"))
            .filter(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("process"))
            .find_map(|e| {
                e.metrics.iter().find_map(|m| match &m.kind {
                    MetricKind::Sum(s)
                        if logit_core::interner::resolve(m.name) == "logit.component.errors" =>
                    {
                        Some(s.value)
                    }
                    _ => None,
                })
            });
        assert_eq!(
            errors,
            Some(1.0),
            "the unknown target should be counted as an ordinary script error, got: {events:?}"
        );
    }

    /// A `flush()`-emitted `event:clone()` honors its `event:to(..)` mark.
    #[tokio::test]
    async fn lua_flush_output_honours_marks() {
        let script = r#"
            local pending = nil
            function process(event)
                pending = event:clone()
                return nil
            end
            function flush()
                if pending then
                    local e = pending
                    pending = nil
                    return {e:to("a")}
                end
                return {}
            end
        "#;
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "windowed",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua {
                    script: script.to_string(),
                    interval: Some(Duration::from_secs(3600)),
                    max_memory: None,
                },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            ("sink_a", vec!["a"], vec![], influxdb_out()),
            ("sink_fwd", vec!["windowed"], vec![], influxdb_out()),
        ]);

        let (tx_a, rx_a) = std::sync::mpsc::channel();
        let (tx_fwd, rx_fwd) = std::sync::mpsc::channel();

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput {
                    batch: Some(one_batch(vec![tagged_event("stashed", None)])),
                }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "windowed".to_string(),
            NodeSpec::Lua {
                script: script.to_string(),
                interval: Some(Duration::from_secs(3600)),
                runtime: LuaRuntimeConfig::default(),
            },
        );
        specs.insert("a".to_string(), NodeSpec::Target);
        specs.insert("sink_a".to_string(), recording_sink(tx_a));
        specs.insert("sink_fwd".to_string(), recording_sink(tx_fwd));

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let flushed =
            rx_a.recv_timeout(Duration::from_secs(1)).expect("the flushed event should reach a");
        assert_eq!(flushed.events.len(), 1);
        assert!(
            rx_fwd.recv_timeout(Duration::from_millis(50)).is_err(),
            "the flush marked its event for a, so the component's own consumer sees nothing"
        );
    }

    // -----------------------------------------------------------------------------------------
    // A Lua `flush()` runs in a root context (`docs/adr/lua-flush-root-context.md`)
    // -----------------------------------------------------------------------------------------

    /// `RecordProvenance` that also reports each batch's [`TraceContext`].
    struct RecordDelivered {
        ctx_tx: std::sync::mpsc::Sender<TraceContext>,
        prov_tx: std::sync::mpsc::Sender<Provenance>,
    }

    impl Transform for RecordDelivered {
        fn process(&mut self, _resource: &Arc<Resource>, _event: &mut Event) -> bool {
            true
        }

        fn observe_batch_context(&mut self, ctx: TraceContext) {
            let _ = self.ctx_tx.send(ctx);
        }

        fn observe_provenance(&mut self, provenance: Provenance) {
            let _ = self.prov_tx.send(provenance);
        }
    }

    fn hex_id(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn attr_str<'a>(event: &'a Event, key: &str) -> Option<&'a str> {
        event.attributes.get(key).and_then(|v| v.as_str())
    }

    /// Runs `in -> windowed(script) -> watcher -> out` over `input`; returns index-aligned
    /// observed `(context, provenance)` pairs and sink batches.
    async fn run_lua_flush_probe(
        script: &str,
        input: EventBatch,
    ) -> (Vec<(TraceContext, Provenance)>, Vec<EventBatch>) {
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "windowed",
                vec!["in"],
                vec![],
                ComponentKind::Lua {
                    script: script.to_string(),
                    interval: Some(Duration::from_secs(3600)),
                    max_memory: None,
                },
            ),
            (
                "watcher",
                vec!["windowed"],
                vec![],
                ComponentKind::Lua { script: String::new(), interval: None, max_memory: None },
            ),
            ("out", vec!["watcher"], vec![], influxdb_out()),
        ]);

        let (ctx_tx, ctx_rx) = std::sync::mpsc::channel();
        let (prov_tx, prov_rx) = std::sync::mpsc::channel();
        let (out_tx, out_rx) = std::sync::mpsc::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput { batch: Some(input) }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "windowed".to_string(),
            NodeSpec::Lua {
                script: script.to_string(),
                interval: Some(Duration::from_secs(3600)),
                runtime: LuaRuntimeConfig::default(),
            },
        );
        specs.insert(
            "watcher".to_string(),
            NodeSpec::Transform(Box::new(RecordDelivered { ctx_tx, prov_tx })),
        );
        specs.insert("out".to_string(), recording_sink(out_tx));

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let contexts: Vec<TraceContext> = ctx_rx.try_iter().collect();
        let provenances: Vec<Provenance> = prov_rx.try_iter().collect();
        assert_eq!(contexts.len(), provenances.len(), "one context and one provenance per batch");
        let batches: Vec<EventBatch> = out_rx.try_iter().collect();
        assert_eq!(batches.len(), contexts.len(), "the watcher and the sink see the same batches");
        (contexts.into_iter().zip(provenances).collect(), batches)
    }

    /// Passes each event through and re-emits a clone from `flush()`, tagging both with what the
    /// script saw of `trace`/`provenance`.
    const FLUSH_PROBE_SCRIPT: &str = r#"
        local pending = nil
        local function stamp(e, phase)
            e.attributes["phase"] = phase
            e.attributes["seen_trace_id"] = trace.trace_id
            e.attributes["seen_span_id"] = trace.span_id
            e.attributes["seen_origin"] = provenance.origin
            e.attributes["seen_previous"] = provenance.previous
            e.attributes["seen_component"] = provenance.component
            return e
        end
        function process(event)
            pending = event:clone()
            return stamp(event, "process")
        end
        function flush()
            if pending then
                local e = pending
                pending = nil
                return {stamp(e, "flush")}
            end
            return {}
        end
    "#;

    fn upstream_batch() -> EventBatch {
        let mut resource = Resource::default();
        resource.attributes.insert("service.name", "upstream");
        EventBatch {
            resource: Arc::new(resource),
            scope: Some(Arc::new(Scope {
                name: "upstream-lib".into(),
                version: "1".into(),
                attributes: AttrMap::new(),
                dropped_attributes_count: 0,
                schema_url: None,
            })),
            events: vec![tagged_event("stashed", None)],
        }
    }

    /// A flushed batch has an empty resource and no scope, not the last processed batch's.
    #[tokio::test]
    async fn lua_flush_resource_and_scope_start_empty_not_last_seen() {
        let (_, batches) = run_lua_flush_probe(FLUSH_PROBE_SCRIPT, upstream_batch()).await;
        let [processed, flushed] = batches.as_slice() else {
            panic!("expected the process-path batch then the flushed batch, got {batches:?}");
        };
        assert_eq!(attr_str(&processed.events[0], "phase"), Some("process"));
        assert_eq!(
            processed.resource.attributes.get("service.name").and_then(|v| v.as_str()),
            Some("upstream"),
            "the process path keeps the incoming batch's resource"
        );
        assert!(processed.scope.is_some(), "the process path keeps the incoming batch's scope");

        assert_eq!(attr_str(&flushed.events[0], "phase"), Some("flush"));
        assert!(
            flushed.resource.attributes.is_empty(),
            "a flush runs in a root context: empty resource, got {:?}",
            flushed.resource
        );
        assert!(flushed.scope.is_none(), "a flush runs in a root context: no scope");
    }

    /// A `resource`/`scope` write inside `flush()` reaches the flushed batch.
    #[tokio::test]
    async fn lua_flush_resource_and_scope_written_inside_flush_are_honoured() {
        let script = r#"
            local pending = nil
            function process(event)
                pending = event:clone()
                return nil
            end
            function flush()
                if pending then
                    local e = pending
                    pending = nil
                    resource["service.name"] = "from-flush"
                    scope.name = "from-flush-lib"
                    return {e}
                end
                return {}
            end
        "#;
        let (_, batches) = run_lua_flush_probe(script, upstream_batch()).await;
        let [flushed] = batches.as_slice() else {
            panic!("process() drops, so only the flushed batch should arrive, got {batches:?}");
        };
        assert_eq!(
            flushed.resource.attributes.get("service.name").and_then(|v| v.as_str()),
            Some("from-flush")
        );
        let scope = flushed.scope.as_ref().expect("the scope written in flush() should be carried");
        assert_eq!(&scope.name[..], b"from-flush-lib");
    }

    /// `flush()` sees this component as `origin` and `previous`; `process()` sees the batch's.
    #[tokio::test]
    async fn lua_flush_sees_its_own_component_as_origin_and_previous() {
        let (observed, batches) = run_lua_flush_probe(FLUSH_PROBE_SCRIPT, upstream_batch()).await;
        let [processed, flushed] = batches.as_slice() else {
            panic!("expected the process-path batch then the flushed batch, got {batches:?}");
        };

        let seen = &processed.events[0];
        assert_eq!(attr_str(seen, "seen_origin"), Some("in"), "process() sees the listener");
        assert_eq!(attr_str(seen, "seen_previous"), Some("in"));
        assert_eq!(attr_str(seen, "seen_component"), Some("windowed"));

        let seen = &flushed.events[0];
        assert_eq!(attr_str(seen, "seen_origin"), Some("windowed"), "a flush is its own origin");
        assert_eq!(attr_str(seen, "seen_previous"), Some("windowed"));
        assert_eq!(attr_str(seen, "seen_component"), Some("windowed"));
        let (_, stamped) = &observed[1];
        assert_eq!(symbol_name(stamped.origin).as_deref(), Some("windowed"));
        assert_eq!(symbol_name(stamped.previous).as_deref(), Some("windowed"));
    }

    /// `flush()` sees the fresh root its batch is sent under, not the last batch's context.
    #[tokio::test]
    async fn lua_flush_sees_the_fresh_root_trace_context_it_is_sent_under() {
        let (observed, batches) = run_lua_flush_probe(FLUSH_PROBE_SCRIPT, upstream_batch()).await;
        let [(process_ctx, _), (flush_ctx, _)] = observed.as_slice() else {
            panic!("expected two observed contexts, got {}", observed.len());
        };
        let [processed, flushed] = batches.as_slice() else {
            panic!("expected the process-path batch then the flushed batch, got {batches:?}");
        };

        // Process path: the script reads the incoming context; the outgoing batch is its child.
        let seen = &processed.events[0];
        assert_eq!(attr_str(seen, "seen_trace_id"), Some(hex_id(&process_ctx.trace_id).as_str()));
        assert_ne!(attr_str(seen, "seen_span_id"), Some(hex_id(&process_ctx.span_id).as_str()));

        // Flush: the script read the root the batch went out under.
        let seen = &flushed.events[0];
        assert_eq!(attr_str(seen, "seen_trace_id"), Some(hex_id(&flush_ctx.trace_id).as_str()));
        assert_eq!(attr_str(seen, "seen_span_id"), Some(hex_id(&flush_ctx.span_id).as_str()));
        assert_ne!(
            flush_ctx.trace_id, process_ctx.trace_id,
            "a flush is a new root, unrelated to the batch that fed it"
        );
    }

    /// A flushed event sent through a target keeps this node as `origin`, the target as `previous`.
    #[tokio::test]
    async fn lua_flush_through_a_target_keeps_this_node_as_origin_and_the_target_as_previous() {
        let script = r#"
            local pending = nil
            function process(event)
                pending = event:clone()
                return nil
            end
            function flush()
                if pending then
                    local e = pending
                    pending = nil
                    return {e:to("a")}
                end
                return {}
            end
        "#;
        let g = routed_graph(vec![
            (
                "in",
                vec![],
                vec![],
                ComponentKind::StatsdIn {
                    bind: "127.0.0.1:0".to_string(),
                    transport: logit_config::StatsdTransport::default(),
                    tls: None,
                    handshake_timeout: logit_config::default_handshake_timeout(),
                    idle_timeout: None,
                    max_connections: logit_config::default_max_connections(),
                    peer: false,
                    proxy_protocol: false,
                    socket_mode: None,
                },
            ),
            (
                "windowed",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua {
                    script: script.to_string(),
                    interval: Some(Duration::from_secs(3600)),
                    max_memory: None,
                },
            ),
            ("a", vec![], vec![], ComponentKind::Target {}),
            (
                "watcher",
                vec!["a"],
                vec![],
                ComponentKind::Json { skip_to_brace: false, invalid_utf8: Default::default() },
            ),
        ]);

        let (tx, rx) = std::sync::mpsc::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(FiniteInput {
                    batch: Some(one_batch(vec![tagged_event("stashed", None)])),
                }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "windowed".to_string(),
            NodeSpec::Lua {
                script: script.to_string(),
                interval: Some(Duration::from_secs(3600)),
                runtime: LuaRuntimeConfig::default(),
            },
        );
        specs.insert("a".to_string(), NodeSpec::Target);
        specs.insert("watcher".to_string(), NodeSpec::Transform(Box::new(RecordProvenance { tx })));

        tokio::time::timeout(Duration::from_secs(5), run(g, specs))
            .await
            .expect("run should return once the only input finishes, not hang forever")
            .expect("run should complete without error");

        let provenance = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("the flushed batch should reach the target's consumer");
        assert_eq!(
            symbol_name(provenance.origin).as_deref(),
            Some("windowed"),
            "the flushing node stays the origin -- the target never overwrites an already-set one"
        );
        assert_eq!(
            symbol_name(provenance.previous).as_deref(),
            Some("a"),
            "the target's Fanout rewrote previous, exactly as it does on the process() path"
        );
    }
}
