//! The node runtime: turns a resolved [`Graph`] plus one built implementation per component
//! (a [`NodeSpec`]) into running tasks/threads, wired together with per-component [`Fanout`]s.
//! See `docs/design/pipeline-graph.md`'s "Runtime model" and "Thread model" sections.
//!
//! Every component's inbox channel is created before any node is spawned, so spawn order doesn't
//! matter: a `Fanout` is cloned `Sender`s into inboxes that already exist.

use crate::fanout::{BatchContext, Delivered, TraceContext};
use crate::graph::{Graph, Role};
use crate::output::{classify, is_explicitly_permanent, is_retryable, DeliveryPosture, Fault};
#[cfg(test)]
use crate::queue::{SinkQueue, SinkQueueConfig};
use crate::queue::{SinkStore, SinkStoreConfig};
use crate::readiness::NodeState;
use crate::router::{Destination, Router, RouterScratch};
use crate::{Fanout, Input, InputRuntimeConfig, Output, Readiness, Transform};
use anyhow::Context;
use logit_core::{Diagnostics, Event, EventBatch, Resource, Scope, SpanKind, Telemetry};
use logit_script::{ProcessOutcome, ScriptWorker};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

/// How long permanent (`Fault::Permanent`) send failures may repeat, with no intervening
/// successful delivery, before `write_loop` returns `Err`, ending `run_output` and the whole
/// pipeline. A misconfigured sink (bad token, bad bucket) still fails loudly enough for a
/// restart-policy supervisor to notice; one malformed batch can't kill a healthy pipeline. Not
/// config-exposed: `logit_config::BufferConfig` doesn't surface it. See
/// `docs/adr/buffered-sink-delivery.md`'s "Failure handling" section.
const PERMANENT_FAILURE_WINDOW: Duration = Duration::from_secs(60);

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
    /// shutdown fires, so `select!`'s random rotation cancels the listener by drop about half
    /// the time. See `docs/adr/decoupled-listener-io.md`.
    Input(Box<dyn Input + Send>, InputRuntimeConfig),
    /// The sink, its queue (in memory or disk-backed; see `SinkStoreConfig`), and its retry
    /// budget and shutdown grace (`WriteLoopConfig`). Production builds these from the
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
    mut specs: HashMap<String, NodeSpec>,
    mut telemetry: HashMap<String, Telemetry>,
    readiness: Readiness,
    shutdown: impl Future<Output = ()> + Send + 'static,
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
    let shutdown_driver = tokio::spawn(async move {
        shutdown.await;
        tracing::info!(target: "logit", "shutdown signal received");
        // Before the nodes are told, so `/readyz` stops routing traffic here the instant the
        // signal arrives, not partway through the drain. A no-op if a node already failed: a
        // SIGTERM after a failure must not paper over it.
        readiness_for_driver.draining();
        let _ = drain_started_for_driver.set(tokio::time::Instant::now());
        let _ = shutdown_tx_for_driver.send(true);
    });

    // Batches abandoned in any sink's inbox because shutdown grace expired before `write_loop`
    // drained them, summed so the `drain complete` log can say whether the drain was clean.
    let shutdown_dropped_batches = Arc::new(std::sync::atomic::AtomicU64::new(0));

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

    let mut senders: HashMap<String, mpsc::Sender<Delivered>> = HashMap::with_capacity(ids.len());
    let mut inboxes: HashMap<String, mpsc::Receiver<Delivered>> = HashMap::with_capacity(ids.len());
    for id in &ids {
        // No channel for a `target`: it declares no `sources:` and nothing may name one as a
        // source (rule 49). A target is a name for its routers' outbound edges; the pass below
        // builds its one `Fanout` directly onto its consumers' inboxes.
        if graph.components.get(id).is_some_and(|c| c.role() == Role::Target) {
            continue;
        }
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        senders.insert(id.clone(), tx);
        inboxes.insert(id.clone(), rx);
    }

    // One `Fanout` per `target`, built before the spawn loop: `ids` is sorted, so a router can
    // sort ahead of the targets it directs at (`examples/fan-out-central.yaml` does), and its
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
        let fanout = Fanout::new(component.consumers.iter().map(|c| senders[c].clone()).collect())
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
        let fanout = Fanout::new(component.consumers.iter().map(|c| senders[c].clone()).collect())
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
            NodeSpec::Lua { script, interval } => {
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
                std::thread::Builder::new()
                    .name(format!("logit-{id}"))
                    .spawn(move || {
                        run_lua(
                            thread_id,
                            script,
                            interval,
                            targets,
                            ready_tx,
                            done_tx,
                            inbox,
                            fanout,
                            target_routes,
                            node_telemetry,
                            handle,
                        )
                    })
                    .with_context(|| format!("spawning thread for component '{id}'"))
                    .map_err(RunError::Startup)?;
                match ready_rx.await {
                    Ok(Ok(())) => {
                        // Spawned only after the ready handshake, so a load failure never leaves a
                        // watcher behind. `done_tx` buffers its one message, so a thread that dies
                        // between reporting ready and this spawn is still observed.
                        let watcher = tasks.spawn(watch_lua_thread(id.clone(), done_rx));
                        node_ids.insert(watcher.id(), id.clone());
                        readiness.set_node(&id, NodeState::Running);
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

    // Every `Fanout` holds its own `Sender` clones; these are construction scaffolding. Left
    // alive, each is an extra `Sender` on every channel, so no inbox ever closes, the shutdown
    // cascade never fires, and `run` hangs.
    drop(senders);
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
async fn run_input(
    id: String,
    mut input: Box<dyn Input + Send>,
    fanout: Fanout,
    mut shutdown: watch::Receiver<bool>,
    shutdown_grace: Duration,
) -> anyhow::Result<()> {
    let mut deadline: Option<tokio::time::Instant> = None;
    tokio::select! {
        result = input.run_until_shutdown(fanout, shutdown.clone())
            => result.with_context(|| format!("component '{id}'")),
        () = shutdown_grace_expired(&mut shutdown, &mut deadline, shutdown_grace) => Ok(()),
    }
}

/// A sink node's drain-and-deliver pair, decoupled through a [`SinkStore`]
/// (`docs/adr/buffered-sink-delivery.md`): [`drain_inbox`] moves every `Delivered` off the inbox
/// into the store as fast as its bounds allow, while [`write_loop`] delivers from it
/// independently, so a slow or backing-off `Output::send` doesn't stall the inbox.
///
/// Both run in this one task, so `write_loop`'s `Err` (`drain_inbox` never fails) is what the
/// `JoinSet` sees. `write_loop` only borrows `output` so that this function can run the final
/// drain-and-flush itself, after `drain` can no longer push anything (see `finish_and_flush`).
#[allow(clippy::too_many_arguments)]
async fn run_output(
    id: String,
    mut output: Box<dyn Output + Send>,
    mut inbox: mpsc::Receiver<Delivered>,
    telemetry: Telemetry,
    store_config: SinkStoreConfig,
    write_config: WriteLoopConfig,
    shutdown: watch::Receiver<bool>,
    shutdown_dropped_batches: Arc<std::sync::atomic::AtomicU64>,
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
    ));

    // Not `tokio::join!`: `write_loop` can return early (a permanent failure, or shutdown grace
    // expiring) while `inbox` stays open, as it does under every real listener. `drain_inbox`
    // can't learn its consumer gave up, and under `Block` would park forever pushing into a queue
    // nothing drains, hanging this task and `run`.
    //
    // If `write` finishes first, a still-pending `drain` is dropped and never polled again; the
    // sweep below counts what it left in `inbox`. (When `write` finished by draining to
    // closed-and-empty, `drain` already closed the queue, so it's already done.) If `drain`
    // finishes first, its inbox closed normally and `write_loop` still has the queue's tail.
    //
    // `write` holds `output`'s mutable borrow until dropped, and `finish_and_flush` needs it back.
    // The `Option` exists for the borrow checker: it tracks the move `write.await` makes per arm,
    // so dropping `write` unconditionally after the `select!` doesn't typecheck.
    let already_finished = tokio::select! {
        result = &mut write => Some(result),
        () = &mut drain => None,
    };
    let write_result = match already_finished {
        Some(result) => {
            drop(write);
            result
        }
        None => write.await,
    };

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

    // An abandoned `drain` may leave batches that never reached `store`, so `finish_and_flush`
    // can't see them: the one its dropped `store.push` held (`in_hand`, first, since it arrived
    // first), then any still in `inbox`. A `Disk` store persists them (it drops nothing at
    // shutdown); a `Memory` store counts and diagnoses them as dropped. `try_recv` never waits.
    let mut abandoned_batches: u64 = 0;
    let mut abandoned_events: u64 = 0;
    let mut parked = in_hand.lock().unwrap_or_else(|p| p.into_inner()).take();
    loop {
        let (batch, ctx) = match parked.take() {
            Some(item) => item,
            None => match inbox.try_recv() {
                Ok(delivered) => {
                    let ctx = delivered.batch_context();
                    (unwrap_batch_arc(delivered), ctx)
                }
                Err(_) => break,
            },
        };
        abandoned_batches += 1;
        abandoned_events += batch.events.len() as u64;
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
            shutdown_dropped_batches
                .fetch_add(abandoned_batches, std::sync::atomic::Ordering::Relaxed);
            telemetry.count(
                "logit.component.batches.dropped",
                abandoned_batches as f64,
                &[("reason", "shutdown")],
            );
            telemetry.count(
                "logit.component.events.dropped",
                abandoned_events as f64,
                &[("reason", "shutdown")],
            );
            diag.warn(format_args!(
                "{abandoned_batches} batch(es) ({abandoned_events} event(s)) never handed to \
                 this sink's delivery queue when it stopped"
            ));
        }
    }

    finish_and_flush(&diag, &store, &telemetry, output.as_mut()).await;

    write_result
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

/// Retry budget for every sink's delivery in [`write_loop`] (`docs/adr/buffered-sink-delivery.md`).
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Hard ceiling on time spent on one batch, across every attempt and backoff sleep. Each
    /// batch gets a fresh budget from its first attempt.
    pub total_budget: Duration,
    /// Backoff after attempt `n` is `base_delay * 2^(n-1)`, capped at `max_delay` and clamped to
    /// what's left of `total_budget`. No jitter: one writer per sink, not a fleet.
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        // Long enough to ride out a destination restart: the sink queue absorbs the stall, so it
        // doesn't reach the listener (`docs/adr/buffered-sink-delivery.md`).
        Self {
            total_budget: Duration::from_secs(60),
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(10),
        }
    }
}

/// Delivery config for `write_loop`; the queue itself is `SinkStoreConfig`'s.
#[derive(Debug, Clone, Copy)]
pub struct WriteLoopConfig {
    pub retry: RetryConfig,
    /// Caps `write_loop`'s drain time after shutdown fires, measured from the first signal (not
    /// reset per batch), so a down sink can't hang exit (`docs/adr/buffered-sink-delivery.md`).
    pub shutdown_grace: Duration,
    /// Overrides the delivery posture derived from `output.duplicate_safe()`; `None` uses it.
    /// Set from `logit-config::BufferConfig::delivery`.
    pub delivery_override: Option<DeliveryPosture>,
}

impl Default for WriteLoopConfig {
    fn default() -> Self {
        Self {
            retry: RetryConfig::default(),
            shutdown_grace: Duration::from_secs(5),
            delivery_override: None,
        }
    }
}

/// What one batch's delivery attempt (through however many retries its budget allows) ended in.
enum Delivery {
    Delivered,
    /// Never delivered: `fault` wasn't retryable, or the budget ran out. The caller commits and
    /// counts it. `explicit_permanent` is narrower than `fault == Fault::Permanent`: true only
    /// when the sink attached `Fault::Permanent` itself, not when `classify` defaulted to it.
    /// Only `write_loop`'s fatal streak uses it.
    Dropped {
        fault: Fault,
        explicit_permanent: bool,
    },
}

/// Attempts to deliver `batch` via `output.send`, retrying per `posture`/[`is_retryable`] until
/// either it succeeds, a failure isn't retryable, or `retry.total_budget` (a fresh budget for this
/// call) is exhausted.
///
/// Every attempt, including the first, runs under `tokio::time::timeout` of the remaining budget:
/// a sink's own timeout (`InfluxDbOutput`'s 10 s HTTP timeout) can exceed the budget, which would
/// otherwise go unenforced until that attempt gave up. A timeout is `Fault::Ambiguous` (the
/// destination may have received the request), never `Permanent`.
async fn deliver_with_retry(
    output: &mut (dyn Output + Send),
    batch: &EventBatch,
    posture: DeliveryPosture,
    retry: &RetryConfig,
    telemetry: &Telemetry,
) -> Delivery {
    let deadline = tokio::time::Instant::now() + retry.total_budget;
    let mut attempt: u32 = 0;
    loop {
        attempt += 1;
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let timer = telemetry.timer("logit.component.send.duration");
        let result = tokio::time::timeout(remaining, output.send(batch)).await;
        drop(timer);

        let err = match result {
            Ok(Ok(())) => return Delivery::Delivered,
            Ok(Err(err)) => err,
            Err(_elapsed) => {
                anyhow::anyhow!("send attempt exceeded the remaining retry budget ({remaining:?})")
                    .context(Fault::Ambiguous)
            }
        };
        telemetry.count("logit.component.errors", 1.0, &[]);
        let fault = classify(&err);
        let explicit_permanent = is_explicitly_permanent(&err);
        if !is_retryable(fault, posture) {
            return Delivery::Dropped { fault, explicit_permanent };
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            return Delivery::Dropped { fault, explicit_permanent };
        }
        let backoff = backoff_for(retry, attempt).min(deadline.saturating_duration_since(now));
        telemetry.count("logit.component.retries", 1.0, &[]);
        tokio::time::sleep(backoff).await;
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
        Fault::Permanent => "permanent",
    }
}

/// Resolves `grace` after `shutdown` first fires, never before it fires.
///
/// `deadline` persists across calls (one per `write_loop` iteration), anchoring the window to the
/// first signal rather than resetting per batch. It's set synchronously when `wait_for` resolves,
/// so it sticks even if this call then loses a `select!` race and is dropped before its
/// `sleep_until` completes; the next call waits out the remainder. Cancellation-safe.
async fn shutdown_grace_expired(
    shutdown: &mut watch::Receiver<bool>,
    deadline: &mut Option<tokio::time::Instant>,
    grace: Duration,
) {
    if deadline.is_none() {
        // An error means the sender is gone; treat it as shutdown firing rather than hang.
        let _ = shutdown.wait_for(|&due| due).await;
        *deadline = Some(tokio::time::Instant::now() + grace);
    }
    tokio::time::sleep_until(deadline.expect("just set above if it was None")).await;
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
) {
    let (dropped_batches, dropped_events) = store.finish().await;
    if dropped_batches > 0 {
        telemetry.count(
            "logit.component.batches.dropped",
            dropped_batches as f64,
            &[("reason", "shutdown")],
        );
        telemetry.count(
            "logit.component.events.dropped",
            dropped_events as f64,
            &[("reason", "shutdown")],
        );
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

/// Delivers from `store`'s head, one batch at a time, until `store.peek()` returns `None` (closed
/// and empty) or shutdown grace expires. The posture is `write_config.delivery_override`, else
/// `output.duplicate_safe()`'s default (`docs/adr/buffered-sink-delivery.md`).
///
/// A failed batch is committed, counted, and warned about; the pipeline keeps running. The one
/// exception: a run of nothing but explicitly classified `Fault::Permanent` outcomes
/// ([`is_explicitly_permanent`]) lasting [`PERMANENT_FAILURE_WINDOW`] returns `Err`, ending the
/// pipeline. A success, a budget-exhausted `Clean`/`Ambiguous` fault, or an unclassified error
/// that only defaulted to `Permanent` resets the streak: only a positively identified config
/// error counts.
///
/// Never drains the queue or calls `output.flush()`; [`finish_and_flush`] does, and says why.
/// Returns `Ok(())` when shutdown grace expires: an incomplete drain on shutdown isn't a failure.
async fn write_loop(
    id: String,
    output: &mut (dyn Output + Send),
    store: Arc<SinkStore>,
    telemetry: Telemetry,
    write_config: WriteLoopConfig,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let posture = write_config
        .delivery_override
        .unwrap_or_else(|| DeliveryPosture::from_duplicate_safe(output.duplicate_safe()));
    let mut diag = Diagnostics::new(id.clone()).with_telemetry(telemetry.clone());

    let mut last_success: Option<tokio::time::Instant> = None;
    let mut permanent_streak_since: Option<tokio::time::Instant> = None;
    let mut shutdown_deadline: Option<tokio::time::Instant> = None;
    // Turns a stream of `send_failed` warnings into two edge events: `degraded` on the first
    // failure, `recovered` on the next success.
    let mut degraded = false;

    loop {
        // Each `select!` reduces to a plain enum so no handler arm touches `output`: the
        // `deliver_with_retry` future already borrows it mutably, and the borrow checker rejects
        // a second overlapping borrow inside the macro.
        enum NextBatch {
            Batch(Arc<EventBatch>, BatchContext),
            Closed,
            ShutdownExpired,
        }
        let next = tokio::select! {
            batch = store.peek() => match batch {
                Some((batch, ctx)) => NextBatch::Batch(batch, ctx),
                None => NextBatch::Closed,
            },
            () = shutdown_grace_expired(&mut shutdown, &mut shutdown_deadline, write_config.shutdown_grace) => {
                NextBatch::ShutdownExpired
            }
        };
        let (batch, ctx) = match next {
            NextBatch::Batch(batch, ctx) => (batch, ctx),
            NextBatch::Closed => break, // queue closed and empty: nothing left to deliver.
            NextBatch::ShutdownExpired => return Ok(()),
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

        // Once per batch, covering all its retries. `logit_out` uses it to carry provenance
        // across the wire (`docs/adr/batch-provenance-on-delivered.md`); a no-op elsewhere.
        output.observe_batch(ctx);

        enum DeliverStep {
            Outcome(Delivery),
            ShutdownExpired,
        }
        let step = tokio::select! {
            outcome = deliver_with_retry(output, &batch, posture, &write_config.retry, &telemetry) => {
                DeliverStep::Outcome(outcome)
            }
            () = shutdown_grace_expired(&mut shutdown, &mut shutdown_deadline, write_config.shutdown_grace) => {
                DeliverStep::ShutdownExpired
            }
        };
        let outcome = match step {
            DeliverStep::Outcome(outcome) => outcome,
            DeliverStep::ShutdownExpired => return Ok(()),
        };

        match outcome {
            Delivery::Delivered => {
                store.commit();
                last_success = Some(tokio::time::Instant::now());
                permanent_streak_since = None;
                if degraded {
                    degraded = false;
                    diag.info("recovered", "delivery succeeded after a prior failure");
                }
            }
            Delivery::Dropped { fault, explicit_permanent } => {
                store.commit();
                span.error();
                span.tag("fault", fault_tag(fault));
                telemetry.count(
                    "logit.component.batches.dropped",
                    1.0,
                    &[("reason", "send_failed")],
                );
                telemetry.count(
                    "logit.component.events.dropped",
                    batch.events.len() as f64,
                    &[("reason", "send_failed")],
                );
                let since_success = last_success
                    .map(|t| format!("{:?} ago", t.elapsed()))
                    .unwrap_or_else(|| "never".to_string());
                diag.warn_throttled(
                    "send_failed",
                    format_args!(
                        "batch dropped after a {fault} send failure (last successful delivery: \
                         {since_success})"
                    ),
                );
                if !degraded {
                    degraded = true;
                    diag.warn("degraded");
                }

                if explicit_permanent {
                    let now = tokio::time::Instant::now();
                    let since = *permanent_streak_since.get_or_insert(now);
                    if now.duration_since(since) >= PERMANENT_FAILURE_WINDOW {
                        return Err(anyhow::anyhow!(
                            "permanent send failures for at least {PERMANENT_FAILURE_WINDOW:?} \
                             with no successful delivery"
                        ))
                        .with_context(|| format!("component '{id}'"));
                    }
                } else {
                    // Anything short of an explicit config error breaks the streak (see this
                    // function's doc).
                    permanent_streak_since = None;
                }
            }
        }
    }
    Ok(())
}

/// Telemetry accounting plus one `Output::send` call, for `logit-bench`'s allocation tests and
/// benches to measure that hop in isolation. Call it from a `current_thread` runtime with no
/// `tokio::spawn`.
///
/// Not on the runtime's delivery path: `write_loop` calls `output.send` through
/// `deliver_with_retry`, which needs the error's `Fault` for retry decisions.
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
/// (`TraceContext`; `docs/known-gaps.md`'s internal-spans entry;
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
/// failure is `RunError::Startup`), and `done_tx` the post-ready outcome, `Err` only on a panic.
/// [`watch_lua_thread`] awaits `done_rx` as the node's `JoinSet` entry, so the join loop treats a
/// Lua exit like any task's. The loop runs under `catch_unwind` so a panic becomes a message, not
/// a dropped sender; `AssertUnwindSafe` because `ScriptWorker` holds `Lua` and `Rc<RefCell>`s and
/// nothing is used after the unwind. A script's own `process()`/`flush()` errors are logged and
/// counted in [`run_lua_loop`] and never end the node. `done_tx` sends only after the closure
/// drops `inbox` and the `Fanout`s, so the downstream cascade is already underway when the
/// watcher resolves.
#[allow(clippy::too_many_arguments)]
fn run_lua(
    id: String,
    script: String,
    configured_interval: Option<Duration>,
    targets: Vec<String>,
    ready_tx: oneshot::Sender<Result<(), String>>,
    done_tx: oneshot::Sender<Result<(), String>>,
    inbox: mpsc::Receiver<Delivered>,
    fanout: Fanout,
    target_fanouts: Vec<Fanout>,
    telemetry: Telemetry,
    runtime: tokio::runtime::Handle,
) {
    let worker = match ScriptWorker::new(&script)
        .and_then(|w| w.with_telemetry(telemetry.clone()))
        .map(|w| w.with_component(&id))
        .map(|w| w.with_targets(&targets))
    {
        Ok(worker) => worker,
        Err(err) => {
            // The receiver is gone only if `run` already bailed; nothing to do.
            let _ = ready_tx.send(Err(format!("loading a transform script: {err}")));
            return;
        }
    };
    let _ = ready_tx.send(Ok(()));

    // Built here because the registry can't attach one to a `ScriptWorker` it never constructs.
    // Cloned so the panic report below has one after the loop's copy moves into the closure.
    let diag = Diagnostics::new(id.clone()).with_telemetry(telemetry.clone());
    let reporter = diag.clone();

    // The same `Symbol` `Fanout::with_component` interned for this node's edge.
    let me = logit_core::interner::intern(&id);

    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        run_lua_loop(
            worker,
            me,
            configured_interval,
            inbox,
            fanout,
            target_fanouts,
            telemetry,
            runtime,
            diag,
        )
    }));
    let report = thread_outcome(outcome);
    if let Err(message) = &report {
        reporter.error("thread_panicked", format_args!("{message}"));
    }
    // The receiver is gone only if `run` already returned for an unrelated reason; nothing to do.
    let _ = done_tx.send(report);
}

/// A Lua thread's post-ready outcome as a message. A panic payload is a `&str` for a literal
/// `panic!`, a `String` for a formatted one, and anything for `panic_any`, hence the fallback.
fn thread_outcome(result: std::thread::Result<()>) -> Result<(), String> {
    match result {
        Ok(()) => Ok(()),
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

/// A Lua node's `JoinSet` entry: waits on the thread's `done` report (see `run_lua`).
///
/// Doesn't watch `shutdown`: shutdown reaches the thread through the cascade closing its inbox,
/// and racing `shutdown` here would resolve before the thread's close-time flush finished. The
/// `Err(_)` arm is defensive; `run_lua` always sends after `catch_unwind`.
async fn watch_lua_thread(
    id: String,
    done_rx: oneshot::Receiver<Result<(), String>>,
) -> anyhow::Result<()> {
    match done_rx.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(message)) => Err(anyhow::anyhow!("component '{id}': {message}")),
        Err(_) => Err(anyhow::anyhow!("component '{id}': thread exited without reporting")),
    }
}

/// The loop half of [`run_lua`]. Takes everything by value so `catch_unwind` has nothing borrowed.
/// Returns once `inbox` closes, after a last `flush()` if the component has an interval.
#[allow(clippy::too_many_arguments)]
fn run_lua_loop(
    worker: ScriptWorker,
    me: logit_core::Symbol,
    configured_interval: Option<Duration>,
    mut inbox: mpsc::Receiver<Delivered>,
    fanout: Fanout,
    target_fanouts: Vec<Fanout>,
    telemetry: Telemetry,
    runtime: tokio::runtime::Handle,
    mut diag: Diagnostics,
) {
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
    // each event, so there's no route-then-count pass as in `route_batch`.
    let mut scratch = RouterScratch::new(target_fanouts.len());

    // Mints its own root and records the `flush` span, as `run_flush` does: a Lua `flush()` has
    // no single parent batch (`docs/adr/lua-flush-root-context.md`). The script's batch-scoped
    // globals are reset to that root first, so `flush()` reads what its emission goes out as.
    //
    // `diag` and `scratch` are parameters, not captures: the loop body also borrows both mutably,
    // and a capture would hold the borrow for the closure's lifetime.
    let flush_now = |diag: &mut Diagnostics,
                     worker: &ScriptWorker,
                     fanout: &Fanout,
                     target_fanouts: &[Fanout],
                     scratch: &mut RouterScratch| {
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
        let result = worker.flush(now_unix_nanos());
        drop(timer);
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
                let sent = send_lua_partitions(
                    &mut scratch.dests,
                    fanout,
                    target_fanouts,
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
    };

    loop {
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                flush_now(&mut diag, &worker, &fanout, &target_fanouts, &mut scratch);
                let interval = configured_interval
                    .expect("next_flush is only ever Some for a component with an interval");
                next_flush = Some(advance_flush_deadline(deadline, now_instant, interval));
            }
        }

        let batch = match next_flush {
            None => inbox.blocking_recv(),
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
                // The `async` block is required: `tokio::time::timeout` builds its `Sleep` eagerly,
                // which panics outside a runtime context. Inside the block it's built only once
                // `block_on` has entered one.
                match runtime.block_on(async { tokio::time::timeout(wait, inbox.recv()).await }) {
                    Ok(batch) => batch,
                    Err(_elapsed) => continue,
                }
            }
        };
        let Some(batch) = batch else {
            if next_flush.is_some() {
                flush_now(&mut diag, &worker, &fanout, &target_fanouts, &mut scratch);
            }
            return;
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
            match worker.process(event) {
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
        drop(process_timer);
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
        let sent = send_lua_partitions(
            &mut scratch.dests,
            &fanout,
            &target_fanouts,
            &resource,
            &scope,
            ctx,
            &telemetry,
        );
        if sent > 0 {
            span.events(sent);
        }
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

    /// `run` drops its scaffolding `senders`, so inboxes close and `run` returns after the input.
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
                kind: ComponentKind::Lua { script: script.clone(), interval: None },
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
        specs.insert("enrich".to_string(), NodeSpec::Lua { script, interval: None });
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
                kind: ComponentKind::Lua { script: script.clone(), interval: None },
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
        specs.insert("enrich".to_string(), NodeSpec::Lua { script, interval: None });
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
                kind: ComponentKind::Lua { script: script.clone(), interval: None },
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
        specs.insert("enrich".to_string(), NodeSpec::Lua { script, interval: None });
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
            anyhow::bail!("simulated permanent send failure")
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
        duplicate_safe: bool,
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

        fn duplicate_safe(&self) -> bool {
            self.duplicate_safe
        }
    }

    struct FaultyOutputHandles {
        attempts: Arc<std::sync::atomic::AtomicU32>,
        attempt_times: Arc<std::sync::Mutex<Vec<tokio::time::Instant>>>,
        attempted: mpsc::UnboundedReceiver<()>,
        flushed: Arc<std::sync::atomic::AtomicBool>,
    }

    fn faulty_output(
        fault: Fault,
        fail_times: u32,
        duplicate_safe: bool,
    ) -> (FaultyOutput, FaultyOutputHandles) {
        let attempts = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let attempt_times = Arc::new(std::sync::Mutex::new(Vec::new()));
        let flushed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (attempted_tx, attempted_rx) = mpsc::unbounded_channel();
        (
            FaultyOutput {
                fault,
                fail_times,
                duplicate_safe,
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
        RetryConfig {
            total_budget: Duration::from_secs(5),
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(5),
        }
    }

    /// Runs `write_loop` over a closed queue holding `batches`, with no shutdown.
    async fn run_write_loop_to_completion(
        mut output: FaultyOutput,
        batches: Vec<Arc<EventBatch>>,
        retry: RetryConfig,
    ) -> anyhow::Result<()> {
        let telemetry = Telemetry::default();
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        for batch in batches {
            store.push((batch, TraceContext::default().into())).await;
        }
        store.close();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let write_config = WriteLoopConfig {
            retry,
            shutdown_grace: Duration::from_secs(5),
            delivery_override: None,
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            write_loop("out".to_string(), &mut output, store, telemetry, write_config, shutdown_rx),
        )
        .await
        .expect("write_loop should not hang")
    }

    async fn assert_clean_fault_retries_and_eventually_delivers(duplicate_safe: bool) {
        let (output, handles) = faulty_output(Fault::Clean, 2, duplicate_safe);
        run_write_loop_to_completion(output, vec![one_event_batch(1.0)], fast_retry_config())
            .await
            .expect("a Clean fault should always eventually be retried into success");
        assert_eq!(
            handles.attempts.load(std::sync::atomic::Ordering::SeqCst),
            3,
            "should fail twice then succeed on the 3rd attempt"
        );
    }

    #[tokio::test]
    async fn a_clean_fault_is_retried_and_eventually_delivered_under_at_most_once() {
        assert_clean_fault_retries_and_eventually_delivers(false).await;
    }

    #[tokio::test]
    async fn a_clean_fault_is_retried_and_eventually_delivered_under_at_least_once() {
        assert_clean_fault_retries_and_eventually_delivers(true).await;
    }

    #[tokio::test]
    async fn an_ambiguous_fault_is_dropped_immediately_under_at_most_once_with_no_retry() {
        let (output, handles) = faulty_output(Fault::Ambiguous, u32::MAX, false);
        run_write_loop_to_completion(output, vec![one_event_batch(1.0)], fast_retry_config())
            .await
            .expect("a dropped batch under AtMostOnce should not end write_loop with an error");
        assert_eq!(
            handles.attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "an Ambiguous fault under AtMostOnce must be dropped after exactly one attempt, no retry"
        );
    }

    #[tokio::test]
    async fn an_ambiguous_fault_is_retried_under_at_least_once_and_eventually_delivered() {
        let (output, handles) = faulty_output(Fault::Ambiguous, 2, true);
        run_write_loop_to_completion(output, vec![one_event_batch(1.0)], fast_retry_config())
            .await
            .expect("an Ambiguous fault under AtLeastOnce should retry into success");
        assert_eq!(handles.attempts.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    async fn assert_permanent_fault_is_never_retried(duplicate_safe: bool) {
        let (output, handles) = faulty_output(Fault::Permanent, u32::MAX, duplicate_safe);
        run_write_loop_to_completion(output, vec![one_event_batch(1.0)], fast_retry_config())
            .await
            .expect("a single permanent failure should not itself trip the failure window");
        assert_eq!(
            handles.attempts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "a Permanent fault must never be retried, regardless of posture"
        );
    }

    #[tokio::test]
    async fn a_permanent_fault_is_never_retried_under_at_most_once() {
        assert_permanent_fault_is_never_retried(false).await;
    }

    #[tokio::test]
    async fn a_permanent_fault_is_never_retried_under_at_least_once() {
        assert_permanent_fault_is_never_retried(true).await;
    }

    /// Backoff between attempts doubles: 100, 200, 400, 800 ms.
    #[tokio::test(start_paused = true)]
    async fn backoff_between_retry_attempts_follows_the_configured_doubling_schedule() {
        let (output, handles) = faulty_output(Fault::Clean, 4, false);
        let retry = RetryConfig {
            total_budget: Duration::from_secs(60),
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(1),
        };
        run_write_loop_to_completion(output, vec![one_event_batch(1.0)], retry)
            .await
            .expect("should eventually deliver");

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

    /// A retryable fault that exhausts its budget drops the batch and `write_loop` continues.
    #[tokio::test(start_paused = true)]
    async fn budget_exhaustion_on_a_retryable_fault_drops_the_batch_and_write_loop_continues() {
        let (output, handles) = faulty_output(Fault::Ambiguous, u32::MAX, true);
        let retry = RetryConfig {
            total_budget: Duration::from_millis(50),
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(10),
        };
        let result = run_write_loop_to_completion(
            output,
            vec![one_event_batch(1.0), one_event_batch(2.0)],
            retry,
        )
        .await;
        assert!(
            result.is_ok(),
            "budget exhaustion on a retryable fault must never end write_loop with Err, got {result:?}"
        );
        assert!(
            handles.attempts.load(std::sync::atomic::Ordering::SeqCst) > 2,
            "both batches should have been retried more than once each before their budgets ran out"
        );
    }

    /// Explicit `Permanent` failures spanning `PERMANENT_FAILURE_WINDOW`, idle gap included, end
    /// `write_loop` with `Err`.
    #[tokio::test(start_paused = true)]
    async fn sustained_permanent_failures_end_write_loop_once_the_failure_window_elapses() {
        let (output, mut handles) = faulty_output(Fault::Permanent, u32::MAX, false);
        let telemetry = Telemetry::default();
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        store.push((one_event_batch(1.0), TraceContext::default().into())).await;

        let store_for_task = Arc::clone(&store);
        // `write_loop` borrows `output`; moving it into the block makes the future `'static`.
        let handle = tokio::spawn(async move {
            let mut output = output;
            write_loop(
                "out".to_string(),
                &mut output,
                store_for_task,
                telemetry,
                WriteLoopConfig::default(),
                shutdown_rx,
            )
            .await
        });

        handles.attempted.recv().await.expect("the first permanent failure should have happened");

        // An idle gap doesn't reset the streak.
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW).await;

        store.push((one_event_batch(2.0), TraceContext::default().into())).await;
        store.close();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("write_loop should not hang")
            .expect("the task should not panic");
        assert!(
            result.is_err(),
            "permanent failures spanning the whole window with no success should end write_loop \
             with Err"
        );
    }

    /// One scripted outcome per `send` (`None` succeeds), repeating the last entry once exhausted.
    struct ScriptedOutput {
        script: Vec<Option<Fault>>,
        index: usize,
        attempted: mpsc::UnboundedSender<()>,
    }

    #[async_trait::async_trait]
    impl Output for ScriptedOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            let step = self.script[self.index.min(self.script.len() - 1)];
            if self.index + 1 < self.script.len() {
                self.index += 1;
            }
            let _ = self.attempted.send(());
            match step {
                None => Ok(()),
                Some(fault) => Err(anyhow::anyhow!("scripted failure")).context(fault),
            }
        }
    }

    /// A success resets the permanent-failure streak.
    #[tokio::test(start_paused = true)]
    async fn a_success_inside_the_window_resets_the_permanent_failure_streak() {
        let telemetry = Telemetry::default();
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (attempted_tx, mut attempted_rx) = mpsc::unbounded_channel();
        let output = ScriptedOutput {
            script: vec![Some(Fault::Permanent), None, Some(Fault::Permanent)],
            index: 0,
            attempted: attempted_tx,
        };

        // attempt 1: Permanent -- sets streak_since
        store.push((one_event_batch(1.0), TraceContext::default().into())).await;

        let store_for_task = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            let mut output = output;
            write_loop(
                "out".to_string(),
                &mut output,
                store_for_task,
                telemetry,
                WriteLoopConfig::default(),
                shutdown_rx,
            )
            .await
        });
        attempted_rx.recv().await.expect("attempt 1 (failing) should have happened");

        // Past the window, but the next batch succeeds before it's checked again.
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW * 2).await;
        // attempt 2: success -- resets the streak
        store.push((one_event_batch(2.0), TraceContext::default().into())).await;
        attempted_rx.recv().await.expect("attempt 2 (succeeding) should have happened");

        // attempt 3: Permanent again -- a fresh streak
        store.push((one_event_batch(3.0), TraceContext::default().into())).await;
        attempted_rx.recv().await.expect("attempt 3 (failing again) should have happened");
        store.close();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("write_loop should not hang")
            .expect("the task should not panic");
        assert!(
            result.is_ok(),
            "a success inside the window should reset the permanent-failure streak, so a fresh \
             isolated failure right after must not immediately trip Err"
        );
    }

    /// Always fails with no `Fault` attached, like a bare I/O error.
    struct AlwaysUnclassifiedFailure {
        attempted: mpsc::UnboundedSender<()>,
    }

    #[async_trait::async_trait]
    impl Output for AlwaysUnclassifiedFailure {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            let _ = self.attempted.send(());
            Err(anyhow::anyhow!("some bare I/O error, no Fault attached"))
        }
    }

    /// An unclassified error, though non-retryable, never counts toward the failure window.
    #[tokio::test(start_paused = true)]
    async fn an_unclassified_error_never_trips_the_permanent_failure_window() {
        let telemetry = Telemetry::default();
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (attempted_tx, mut attempted_rx) = mpsc::unbounded_channel();
        let output = AlwaysUnclassifiedFailure { attempted: attempted_tx };

        store.push((one_event_batch(1.0), TraceContext::default().into())).await;
        let store_for_task = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            let mut output = output;
            write_loop(
                "out".to_string(),
                &mut output,
                store_for_task,
                telemetry,
                WriteLoopConfig::default(),
                shutdown_rx,
            )
            .await
        });
        attempted_rx.recv().await.expect("the first attempt should have happened");

        tokio::time::sleep(PERMANENT_FAILURE_WINDOW * 2).await;
        store.push((one_event_batch(2.0), TraceContext::default().into())).await;
        attempted_rx.recv().await.expect("a later attempt should have happened");
        store.close();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("write_loop should not hang")
            .expect("the task should not panic");
        assert!(
            result.is_ok(),
            "an unclassified error must never trip the sustained-permanent-failure exit window, \
             no matter how long it repeats"
        );
    }

    /// Always fails: `Ambiguous` for the batch valued 2.0, else `Permanent`, stable across retries.
    struct FaultByBatchValue {
        attempted: mpsc::UnboundedSender<()>,
    }

    #[async_trait::async_trait]
    impl Output for FaultByBatchValue {
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            let _ = self.attempted.send(());
            let value = match &batch.events[0].metrics[0].kind {
                MetricKind::Sum(s) => s.value,
                other => panic!("expected Sum, got {other:?}"),
            };
            let fault = if value == 2.0 { Fault::Ambiguous } else { Fault::Permanent };
            Err(anyhow::anyhow!("simulated failure for batch {value}")).context(fault)
        }

        fn duplicate_safe(&self) -> bool {
            true // AtLeastOnce, so Ambiguous is retryable at all.
        }
    }

    /// A budget-exhausted `Ambiguous` drop resets the permanent-failure streak like a success.
    #[tokio::test(start_paused = true)]
    async fn a_budget_exhausted_ambiguous_drop_resets_the_permanent_failure_streak_like_success_does(
    ) {
        let telemetry = Telemetry::default();
        let store = Arc::new(SinkStore::Memory(SinkQueue::new(
            SinkQueueConfig::default(),
            telemetry.clone(),
        )));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (attempted_tx, mut attempted_rx) = mpsc::unbounded_channel();
        let output = FaultByBatchValue { attempted: attempted_tx };
        // Short, so batch 2's budget exhausts quickly.
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                total_budget: Duration::from_millis(50),
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            ..WriteLoopConfig::default()
        };

        // Permanent -- sets streak_since
        store.push((one_event_batch(1.0), TraceContext::default().into())).await;
        let store_for_task = Arc::clone(&store);
        let handle = tokio::spawn(async move {
            let mut output = output;
            write_loop(
                "out".to_string(),
                &mut output,
                store_for_task,
                telemetry,
                write_config,
                shutdown_rx,
            )
            .await
        });
        attempted_rx.recv().await.expect("attempt 1 (Permanent) should have happened");

        // Past the window; batch 2 then exhausts its 50 ms budget and is dropped. The 200 ms
        // sleep guarantees that drop is committed before batch 3 is pushed.
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW * 2).await;
        store.push((one_event_batch(2.0), TraceContext::default().into())).await;
        attempted_rx.recv().await.expect("batch 2's first attempt should have happened");
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Permanent again -- must be a fresh streak
        store.push((one_event_batch(3.0), TraceContext::default().into())).await;
        attempted_rx.recv().await.expect("attempt 3 (Permanent) should have happened");
        store.close();

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("write_loop should not hang")
            .expect("the task should not panic");
        assert!(
            result.is_ok(),
            "a budget-exhausted Ambiguous drop should reset the permanent-failure streak, so a \
             fresh isolated Permanent failure right after must not immediately trip Err"
        );
    }

    /// A sink stuck retrying still returns `Ok` within `shutdown_grace`, leaving its batch queued.
    #[tokio::test(start_paused = true)]
    async fn shutdown_grace_expiry_ends_write_loop_promptly_leaving_the_remainder_for_run_output() {
        let (mut output, _handles) = faulty_output(Fault::Clean, u32::MAX, false);

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
                total_budget: Duration::from_secs(3600), // "stuck retrying forever", relatively
                base_delay: Duration::from_millis(30),
                max_delay: Duration::from_millis(30),
            },
            shutdown_grace: Duration::from_millis(500),
            delivery_override: None,
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
            )
            .await
        });

        // Let a couple of retry attempts happen first.
        tokio::time::sleep(Duration::from_millis(90)).await;
        shutdown_tx.send(true).expect("receiver should still be alive");

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("write_loop should return within shutdown_grace, not hang")
            .expect("the task should not panic");
        assert!(result.is_ok(), "shutdown-grace expiry should end write_loop with Ok, not Err");

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
        let (output, mut handles) = faulty_output(Fault::Clean, u32::MAX, false);
        let flushed = Arc::clone(&handles.flushed);
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                total_budget: Duration::from_secs(3600),
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            shutdown_grace: Duration::from_millis(100),
            delivery_override: None,
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
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
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
        let (output, mut handles) = faulty_output(Fault::Clean, u32::MAX, false);
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                total_budget: Duration::from_secs(3600),
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            shutdown_grace: Duration::from_millis(100),
            delivery_override: None,
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
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
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

        let dropped_for_shutdown: f64 = registry
            .drain(0)
            .iter()
            .filter(|e| e.attributes.get("component").and_then(|v| v.as_str()) == Some("out"))
            .filter(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("shutdown"))
            .flat_map(|e| e.metrics.iter())
            .filter(|m| logit_core::interner::resolve(m.name) == "logit.component.batches.dropped")
            .filter_map(|m| match &m.kind {
                MetricKind::Sum(s) => Some(s.value),
                _ => None,
            })
            .sum();

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
        let (output, mut handles) = faulty_output(Fault::Clean, u32::MAX, false);
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                total_budget: Duration::from_secs(3600),
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            shutdown_grace: Duration::from_millis(100),
            delivery_override: None,
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
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
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
        tokio::time::sleep(Duration::from_millis(50)).await;

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

        let dropped_for_shutdown: f64 = registry
            .drain(0)
            .iter()
            .filter(|e| e.attributes.get("component").and_then(|v| v.as_str()) == Some("out"))
            .filter(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some("shutdown"))
            .flat_map(|e| e.metrics.iter())
            .filter(|m| logit_core::interner::resolve(m.name) == "logit.component.batches.dropped")
            .filter_map(|m| match &m.kind {
                MetricKind::Sum(s) => Some(s.value),
                _ => None,
            })
            .sum();
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
        while let Some((batch, _)) = reopened.peek().await {
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

        fn duplicate_safe(&self) -> bool {
            true
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
        while let Some((batch, _)) = reopened.peek().await {
            spooled.push(counter_value_of(&batch));
            reopened.commit().unwrap();
        }
        spooled
    }

    /// Sums `logit.component.batches.dropped` for component `out` under `reason`.
    fn batches_dropped(events: &[logit_core::Event], reason: &str) -> f64 {
        events
            .iter()
            .filter(|e| e.attributes.get("component").and_then(|v| v.as_str()) == Some("out"))
            .filter(|e| e.attributes.get("reason").and_then(|v| v.as_str()) == Some(reason))
            .flat_map(|e| e.metrics.iter())
            .filter(|m| logit_core::interner::resolve(m.name) == "logit.component.batches.dropped")
            .filter_map(|m| match &m.kind {
                MetricKind::Sum(s) => Some(s.value),
                _ => None,
            })
            .sum()
    }

    fn slow_retry_write_config(total_budget: Duration, grace: Duration) -> WriteLoopConfig {
        WriteLoopConfig {
            retry: RetryConfig {
                total_budget,
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            shutdown_grace: grace,
            delivery_override: None,
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum ExitPath {
        /// The inbox closes while a slow sink is still delivering: `drain_inbox` finishes first.
        DrainFirst,
        /// The sink fails forever; shutdown grace expires with the store full and a push parked.
        GraceExpiry,
        /// The sink fails permanently for `PERMANENT_FAILURE_WINDOW`: `write_loop` returns `Err`
        /// with the store full and a push parked.
        PermanentError,
        /// The inbox closes and the sink delivers everything: `write_loop` sees closed and empty.
        ClosedAndEmpty,
    }

    #[tokio::test(start_paused = true)]
    async fn every_run_output_exit_path_reconciles_received_against_delivered_dropped_and_spooled()
    {
        const SENT: u64 = 6;
        for path in [
            ExitPath::DrainFirst,
            ExitPath::GraceExpiry,
            ExitPath::PermanentError,
            ExitPath::ClosedAndEmpty,
        ] {
            for disk in [false, true] {
                let at = format!("{path:?}, disk={disk}");
                let dir = crate::disk_queue::test_support::scratch_dir("exit-path-reconcile");
                // Two batches fill the store on the paths that leave some undelivered, so the
                // rest wait in `drain_inbox`'s parked push and in the inbox.
                let small = matches!(path, ExitPath::GraceExpiry | ExitPath::PermanentError);
                let store_config = match (disk, small) {
                    (true, true) => {
                        SinkStoreConfig::Disk(disk_store_config(&dir, 2 * one_counter_record_len()))
                    }
                    (true, false) => SinkStoreConfig::Disk(disk_store_config(&dir, u64::MAX)),
                    (false, true) => SinkStoreConfig::Memory(SinkQueueConfig {
                        max_batches: 2,
                        max_bytes: u64::MAX,
                        overflow: OverflowPolicy::Block,
                    }),
                    (false, false) => SinkStoreConfig::Memory(SinkQueueConfig::default()),
                };
                let delivered = Arc::new(std::sync::atomic::AtomicU64::new(0));
                let (delay, fail) = match path {
                    ExitPath::DrainFirst => (Duration::from_millis(10), None),
                    ExitPath::GraceExpiry => (Duration::from_millis(10), Some(Fault::Clean)),
                    ExitPath::PermanentError => (Duration::from_secs(20), Some(Fault::Permanent)),
                    ExitPath::ClosedAndEmpty => (Duration::ZERO, None),
                };
                let output = PacedOutput { delay, fail, delivered: Arc::clone(&delivered) };
                let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
                let (shutdown_tx, shutdown_rx) = watch::channel(false);
                let registry = Registry::new();
                let run = tokio::spawn(run_output(
                    "out".to_string(),
                    Box::new(output),
                    inbox_rx,
                    registry.telemetry_for("out", "influxdb_out", "sink"),
                    store_config,
                    slow_retry_write_config(Duration::from_secs(3600), Duration::from_millis(100)),
                    shutdown_rx,
                    Arc::new(std::sync::atomic::AtomicU64::new(0)),
                ));

                for value in 1..=SENT {
                    inbox_tx.send(counter_batch(value as f64)).await.unwrap();
                }
                // Lets `drain_inbox` fill the store and park on its next push.
                tokio::time::sleep(Duration::from_millis(5)).await;
                // `PermanentError` keeps the inbox open, as a live listener would: only the
                // permanent streak ends it.
                let held_open = match path {
                    ExitPath::DrainFirst | ExitPath::ClosedAndEmpty => {
                        drop(inbox_tx);
                        None
                    }
                    ExitPath::GraceExpiry => {
                        shutdown_tx.send(true).unwrap();
                        drop(inbox_tx);
                        None
                    }
                    ExitPath::PermanentError => Some(inbox_tx),
                };

                let result = tokio::time::timeout(Duration::from_secs(600), run)
                    .await
                    .unwrap_or_else(|_| panic!("{at}: run_output stopped responding"))
                    .expect("the task must not panic");
                assert_eq!(
                    result.is_err(),
                    matches!(path, ExitPath::PermanentError),
                    "{at}: exit result {result:?}"
                );
                drop(held_open);
                drop(shutdown_tx);

                let events = registry.drain(0);
                let delivered = delivered.load(std::sync::atomic::Ordering::SeqCst) as f64;
                let send_failed = batches_dropped(&events, "send_failed");
                let shutdown = batches_dropped(&events, "shutdown");
                let spooled = if disk { reopen_and_drain(&dir).await.len() as f64 } else { 0.0 };
                assert_eq!(
                    SENT as f64,
                    delivered + send_failed + shutdown + spooled,
                    "{at}: received == delivered ({delivered}) + send_failed ({send_failed}) + \
                     shutdown ({shutdown}) + spooled ({spooled})"
                );
                if disk {
                    assert_eq!(shutdown, 0.0, "{at}: a disk-backed sink drops nothing at shutdown");
                }
                match path {
                    ExitPath::DrainFirst | ExitPath::ClosedAndEmpty => {
                        assert_eq!(delivered, SENT as f64, "{at}")
                    }
                    ExitPath::GraceExpiry => assert_eq!(delivered, 0.0, "{at}"),
                    ExitPath::PermanentError => {
                        assert!(send_failed >= 4.0 && send_failed < SENT as f64, "{at}")
                    }
                }
                std::fs::remove_dir_all(&dir).ok();
            }
        }
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
            delivered: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        };
        let (inbox_tx, inbox_rx) = mpsc::channel::<Delivered>(64);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let registry = Registry::new();
        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            registry.telemetry_for("out", "influxdb_out", "sink"),
            SinkStoreConfig::Disk(disk_store_config(&dir, one_counter_record_len())),
            slow_retry_write_config(Duration::from_secs(3600), Duration::from_millis(100)),
            shutdown_rx,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
        ));

        inbox_tx.send(counter_batch(1.0)).await.unwrap();
        inbox_tx.send(counter_batch(2.0)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await; // batch 2 parks in the push
        shutdown_tx.send(true).unwrap();

        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run_output must not stop responding")
            .expect("the task must not panic")
            .expect("shutdown-grace expiry ends run_output with Ok");
        drop(inbox_tx);

        assert_eq!(batches_dropped(&registry.drain(0), "shutdown"), 0.0);
        assert_eq!(reopen_and_drain(&dir).await, vec![1.0, 2.0]);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Decision 7 of `docs/adr/durable-checkpoint-writes-and-fault-injection.md`: a batch the
    /// sink drops once its retry budget runs out is committed off a disk spool, counted
    /// `send_failed`, and never replayed after a restart.
    #[tokio::test(start_paused = true)]
    async fn a_disk_sink_commits_a_batch_dropped_after_its_retry_budget_so_it_never_replays() {
        let dir = crate::disk_queue::test_support::scratch_dir("dropped-is-committed");
        let output = PacedOutput {
            delay: Duration::from_millis(10),
            fail: Some(Fault::Clean),
            delivered: Arc::new(std::sync::atomic::AtomicU64::new(0)),
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
            slow_retry_write_config(Duration::from_secs(1), Duration::from_secs(5)),
            shutdown_rx,
            Arc::new(std::sync::atomic::AtomicU64::new(0)),
        ));
        inbox_tx.send(counter_batch(1.0)).await.unwrap();
        inbox_tx.send(counter_batch(2.0)).await.unwrap();
        drop(inbox_tx); // the store closes, and ends empty once both are dropped

        tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("run_output must not stop responding")
            .expect("the task must not panic")
            .expect("budget-exhausted drops don't fail the sink");

        assert_eq!(batches_dropped(&registry.drain(0), "send_failed"), 2.0);
        assert!(
            reopen_and_drain(&dir).await.is_empty(),
            "a batch dropped after its retry budget is committed, so a restart doesn't replay it"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    // -----------------------------------------------------------------------------------------
    // `run_with_telemetry`'s join loop: drain every task on the first error instead of aborting
    // (`docs/adr/buffered-sink-delivery.md`)
    // -----------------------------------------------------------------------------------------

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

    /// When one sink fails, a healthy sibling still delivers its queued batches before exit.
    #[tokio::test(start_paused = true)]
    async fn a_healthy_sinks_buffered_batches_are_still_delivered_after_a_sibling_sink_trips_the_permanent_failure_window(
    ) {
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
        let (bad_output, mut bad_handles) = faulty_output(Fault::Permanent, u32::MAX, false);
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
                // The gate stays shut past bad's 60 s window; a default budget or grace would
                // time this send out first.
                WriteLoopConfig {
                    retry: RetryConfig {
                        total_budget: Duration::from_secs(3600),
                        ..RetryConfig::default()
                    },
                    shutdown_grace: Duration::from_secs(3600),
                    delivery_override: None,
                },
            ),
        );

        let registry = Registry::new();
        let mut telemetry: HashMap<String, Telemetry> = HashMap::new();
        telemetry.insert("good".to_string(), registry.telemetry_for("good", "x", "sink"));

        let run_task = tokio::spawn(run_with_telemetry(
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

        // Trip "bad"'s permanent-failure window, ending its task with Err.
        bad_tx
            .send(EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", 1.0)],
            })
            .expect("bad_in's receiver should still be alive");
        bad_handles.attempted.recv().await.expect("bad's first attempt should have happened");
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW).await;
        bad_tx
            .send(EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", 2.0)],
            })
            .expect("bad_in's receiver should still be alive");
        bad_handles
            .attempted
            .recv()
            .await
            .expect("bad's second (window-tripping) attempt should have happened");

        // Lets the join loop observe bad's failure and fire shutdown.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Opened only after shutdown fired, so good's delivery can't have finished earlier.
        gate.open();

        for i in 0..3 {
            let received = tokio::time::timeout(Duration::from_secs(5), delivered_rx.recv())
                .await
                .expect("good's already-queued batches should still be delivered, not aborted")
                .expect("the channel should not have closed");
            match &received.events[0].metrics[0].kind {
                MetricKind::Sum(s) => {
                    assert_eq!(s.value, i as f64, "batches should still be delivered in order")
                }
                other => panic!("expected Sum, got {other:?}"),
            }
        }

        let result = tokio::time::timeout(Duration::from_secs(10), run_task)
            .await
            .expect("run_with_telemetry should not hang once every task has actually finished")
            .expect("task should not panic");
        let err =
            result.expect_err("bad's sustained permanent failures should still end run with Err");
        assert!(err.to_string().contains("bad"), "the returned error should be bad's, got: {err}");
    }

    /// Always fails `Fault::Permanent`; the second `send` first sleeps for `delay`. A `delay`
    /// past the retry budget times that attempt out as `Fault::Ambiguous` instead.
    struct DelayedSecondFailureOutput {
        delay: Duration,
        calls: Arc<std::sync::atomic::AtomicU32>,
    }

    #[async_trait::async_trait]
    impl Output for DelayedSecondFailureOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            let n = self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if n == 1 {
                tokio::time::sleep(self.delay).await;
            }
            Err(anyhow::anyhow!("simulated permanent failure #{n}")).context(Fault::Permanent)
        }
    }

    /// The join loop returns `bad1`'s failure, which trips the window at 60 s. `bad2`'s 61 s
    /// delay exceeds the default 60 s retry budget, so its second batch drops as `Ambiguous`,
    /// resetting its streak: `bad2` never fails, and the `!contains("bad2")` check holds trivially.
    #[tokio::test(start_paused = true)]
    async fn run_with_telemetry_returns_the_first_failure_not_a_later_cascading_one() {
        let mut components = Map::new();
        components.insert(
            "bad1_in".to_string(),
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
            },
        );
        components.insert(
            "bad1".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["bad1_in".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        components.insert(
            "bad2_in".to_string(),
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
            },
        );
        components.insert(
            "bad2".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                receive: logit_config::ReceiveConfig::default(),
                sources: vec!["bad2_in".to_string()],
                targets: Vec::new(),
                kind: influxdb_out(),
            },
        );
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (bad1_tx, bad1_rx) = mpsc::unbounded_channel();
        let (bad2_tx, bad2_rx) = mpsc::unbounded_channel();

        // So one sink's failure-triggered shutdown doesn't cut the other's delay short.
        let generous_grace = WriteLoopConfig {
            retry: RetryConfig::default(),
            shutdown_grace: Duration::from_secs(3600),
            delivery_override: None,
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "bad1_in".to_string(),
            NodeSpec::Input(Box::new(ChannelInput { rx: bad1_rx }), InputRuntimeConfig::default()),
        );
        specs.insert(
            "bad1".to_string(),
            NodeSpec::Output(
                Box::new(DelayedSecondFailureOutput {
                    delay: PERMANENT_FAILURE_WINDOW,
                    calls: Arc::new(std::sync::atomic::AtomicU32::new(0)),
                }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                generous_grace,
            ),
        );
        specs.insert(
            "bad2_in".to_string(),
            NodeSpec::Input(Box::new(ChannelInput { rx: bad2_rx }), InputRuntimeConfig::default()),
        );
        specs.insert(
            "bad2".to_string(),
            NodeSpec::Output(
                Box::new(DelayedSecondFailureOutput {
                    delay: PERMANENT_FAILURE_WINDOW + Duration::from_secs(1),
                    calls: Arc::new(std::sync::atomic::AtomicU32::new(0)),
                }),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                generous_grace,
            ),
        );

        // Senders dropped up front, so both inputs finish on their own.
        for tx in [&bad1_tx, &bad2_tx] {
            for i in 0..2 {
                tx.send(EventBatch {
                    resource: Arc::new(Resource::default()),
                    scope: None,
                    events: vec![counter_event("hits", i as f64)],
                })
                .expect("receiver should still be alive");
            }
        }
        drop(bad1_tx);
        drop(bad2_tx);

        let run_task = tokio::spawn(run(g, specs));

        // On the same virtual clock, so the timeout must exceed both delays.
        let result = tokio::time::timeout(PERMANENT_FAILURE_WINDOW * 2, run_task)
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
        assert_eq!(thread_outcome(Ok(())), Ok(()));

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

    /// A clean report is `Ok`; a panic report or a dropped sender is an error naming the node.
    #[tokio::test]
    async fn watch_lua_thread_maps_each_outcome() {
        let (tx, rx) = oneshot::channel();
        tx.send(Ok(())).expect("receiver alive");
        watch_lua_thread("enrich".to_string(), rx).await.expect("a clean report is Ok");

        let (tx, rx) = oneshot::channel();
        tx.send(Err("thread panicked: boom".to_string())).expect("receiver alive");
        let err = watch_lua_thread("enrich".to_string(), rx)
            .await
            .expect_err("a panic report is an error");
        assert_eq!(err.to_string(), "component 'enrich': thread panicked: boom");

        let (tx, rx) = oneshot::channel::<Result<(), String>>();
        drop(tx);
        let err = watch_lua_thread("enrich".to_string(), rx)
            .await
            .expect_err("a dropped sender is an error, not a silent Ok");
        assert!(
            err.to_string().contains("without reporting"),
            "the defensive arm should say what happened: {err}"
        );
    }

    /// A sustained permanent sink failure ends `run` with an error naming the sink.
    #[tokio::test(start_paused = true)]
    async fn a_sustained_permanent_sink_failure_returns_runtime_not_startup() {
        let mut components = Map::new();
        components.insert("bad_in".to_string(), plain_component(vec![], statsd_in()));
        components
            .insert("bad".to_string(), plain_component(vec!["bad_in".to_string()], influxdb_out()));
        let g =
            graph::resolve(Config { components, ..Default::default() }).expect("should resolve");

        let (bad_tx, bad_rx) = mpsc::unbounded_channel();
        let (bad_output, mut bad_handles) = faulty_output(Fault::Permanent, u32::MAX, false);

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

        let run_task = tokio::spawn(run(g, specs));

        bad_tx
            .send(EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", 1.0)],
            })
            .expect("bad_in's receiver should still be alive");
        bad_handles.attempted.recv().await.expect("bad's first attempt should have happened");
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW).await;
        bad_tx
            .send(EventBatch {
                resource: Arc::new(Resource::default()),
                scope: None,
                events: vec![counter_event("hits", 2.0)],
            })
            .expect("bad_in's receiver should still be alive");
        bad_handles
            .attempted
            .recv()
            .await
            .expect("bad's second (window-tripping) attempt should have happened");
        drop(bad_tx);

        // `run` flattens `RunError`; `run_error_exit_codes` covers the typed variant.
        let result = tokio::time::timeout(PERMANENT_FAILURE_WINDOW * 2, run_task)
            .await
            .expect("run should not hang")
            .expect("task should not panic");
        let err = result.expect_err("a sustained permanent failure should still end run with Err");
        assert!(err.to_string().contains("bad"), "the error should name bad, got: {err}");
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
        let (mut output, _handles) = faulty_output(Fault::Permanent, u32::MAX, false);
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
        )
        .await
        .expect("one permanent failure alone should not end write_loop");

        let events = registry.drain(0);
        let span_event =
            span_events(&events).find(|e| span_op(e) == Some("deliver")).expect("a sink span");
        let record = span_event.span.as_ref().expect("span record");
        assert_eq!(record.status, SpanStatus::Error);
        assert_eq!(record.kind, SpanKind::Client);
        assert_eq!(span_event.attributes.get("fault").and_then(|v| v.as_str()), Some("permanent"));
    }

    /// The sink span is parented on the context the batch was queued with.
    #[tokio::test]
    async fn write_loop_records_a_sink_span_parented_on_the_incoming_batchs_context() {
        let registry = Registry::with_span_sampling(1.0);
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");
        let (mut output, _handles) = faulty_output(Fault::Clean, 0, false);
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
        )
        .await
        .expect("a successful delivery should not end write_loop with an error");

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
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a", "b"],
                ComponentKind::Lua { script: String::new(), interval: None },
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
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua { script: String::new(), interval: None },
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
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua { script: String::new(), interval: None },
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
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua { script: String::new(), interval: None },
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
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a", "b"],
                ComponentKind::Lua { script: String::new(), interval: None },
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
                },
            ),
            (
                "split_a",
                vec!["in"],
                vec!["shared"],
                ComponentKind::Lua { script: String::new(), interval: None },
            ),
            (
                "split_b",
                vec!["in"],
                vec!["shared"],
                ComponentKind::Lua { script: String::new(), interval: None },
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
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a", "b"],
                ComponentKind::Lua { script: SPLIT_SCRIPT.to_string(), interval: None },
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
            NodeSpec::Lua { script: SPLIT_SCRIPT.to_string(), interval: None },
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
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a", "b"],
                ComponentKind::Lua { script: SPLIT_SCRIPT.to_string(), interval: None },
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
            NodeSpec::Lua { script: SPLIT_SCRIPT.to_string(), interval: None },
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
                },
            ),
            (
                "split",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua { script: script.to_string(), interval: None },
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
            NodeSpec::Lua { script: script.to_string(), interval: None },
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
                },
            ),
            (
                "windowed",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua {
                    script: script.to_string(),
                    interval: Some(Duration::from_secs(3600)),
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
            NodeSpec::Lua { script: script.to_string(), interval: Some(Duration::from_secs(3600)) },
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
                },
            ),
            (
                "windowed",
                vec!["in"],
                vec![],
                ComponentKind::Lua {
                    script: script.to_string(),
                    interval: Some(Duration::from_secs(3600)),
                },
            ),
            (
                "watcher",
                vec!["windowed"],
                vec![],
                ComponentKind::Lua { script: String::new(), interval: None },
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
            NodeSpec::Lua { script: script.to_string(), interval: Some(Duration::from_secs(3600)) },
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
                },
            ),
            (
                "windowed",
                vec!["in"],
                vec!["a"],
                ComponentKind::Lua {
                    script: script.to_string(),
                    interval: Some(Duration::from_secs(3600)),
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
            NodeSpec::Lua { script: script.to_string(), interval: Some(Duration::from_secs(3600)) },
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
