//! The node runtime: turns a resolved [`Graph`] plus one built implementation per component
//! (a [`NodeSpec`]) into running tasks/threads, wired together with per-component [`Fanout`]s.
//! See `docs/design/pipeline-graph.md`'s "Runtime model" and "Thread model" sections.
//!
//! Every component gets one inbox channel, created up front for the whole graph before any node
//! is spawned -- unlike the design doc's "build in reverse topological order" framing, this
//! doesn't actually need dependency ordering: a `Fanout` is just cloned `Sender`s into inboxes
//! that already exist by construction, regardless of which node gets spawned first.

use crate::fanout::Delivered;
use crate::graph::Graph;
use crate::output::{classify, is_explicitly_permanent, is_retryable, DeliveryPosture, Fault};
use crate::sink_queue::{SinkQueue, SinkQueueConfig};
use crate::{Fanout, Input, Output, Transform};
use anyhow::Context;
use logit_core::{Diagnostics, EventBatch, Resource, Telemetry};
use logit_script::{ProcessOutcome, ScriptWorker};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

/// How long permanent (`Fault::Permanent`) send failures may repeat, with no intervening
/// successful delivery, before `write_loop` gives up and returns `Err` -- ending `run_output` and
/// therefore the whole pipeline, exactly as an unclassified failure did before this workstream. A
/// genuinely misconfigured sink (bad token, bad bucket) still fails loudly enough for a
/// restart-policy supervisor to notice; one malformed batch cannot kill an otherwise-healthy
/// pipeline. Fixed, not config-exposed -- workstream F's `logit_config::BufferConfig` deliberately
/// does not surface this window; revisit if a real deployment ever needs to tune it.
/// See `docs/adr/0021-buffered-sink-delivery.md`'s "Failure handling" section.
const PERMANENT_FAILURE_WINDOW: Duration = Duration::from_secs(60);

/// Bounded channel capacity between two graph nodes. Small and arbitrary -- just enough to smooth
/// out bursts without unbounded memory growth; revisit with real numbers once there's a reason to.
const CHANNEL_CAPACITY: usize = 64;

/// One component's built implementation, keyed by id and handed to [`run`]. Which variant a
/// `ComponentKind` becomes is the registry's job (`logit-cli`), not this crate's -- this crate
/// only knows how to *run* each variant once built.
pub enum NodeSpec {
    Input(Box<dyn Input + Send>),
    /// The sink's own `SinkQueue` bounds/overflow policy (see `sink_queue.rs`) plus its retry
    /// budget and shutdown grace (see `RetryConfig`/`WriteLoopConfig`). Production call sites
    /// (`logit-cli::pipeline::build_spec`) build these from the component's own
    /// `logit_config::BufferConfig` (`queue_config`/`write_config` there), defaulting to
    /// `SinkQueueConfig::default()`/`WriteLoopConfig::default()` only when a config omits its
    /// `buffer:` block; a test can pass whatever config it needs to exercise (e.g. a tiny
    /// `max_batches` to force overflow behavior deterministically, or a short `total_budget`/
    /// `shutdown_grace` to keep a retry/shutdown test fast).
    Output(Box<dyn Output + Send>, SinkQueueConfig, WriteLoopConfig),
    Transform(Box<dyn Transform + Send>),
    /// Built here, not by the caller: `ScriptWorker` is `!Send` (`docs/design/lua-api.md`'s
    /// concurrency section), so it can't be constructed anywhere but the dedicated thread it
    /// will live on.
    Lua {
        script: String,
        interval: Option<Duration>,
    },
}

/// Builds every component's inbox and `Fanout`, then spawns each as a tokio task (listeners,
/// sinks, `Transform`-trait nodes) or a dedicated OS thread (Lua nodes), and runs until the first
/// one fails. No shutdown signal -- see [`run_with_shutdown`] for graceful shutdown.
pub async fn run(graph: Graph, specs: HashMap<String, NodeSpec>) -> anyhow::Result<()> {
    run_with_shutdown(graph, specs, std::future::pending()).await
}

/// Same as [`run`], but resolving `shutdown` closes every listener's inbound channel *normally*
/// instead of the process just dying -- which is enough to trigger the existing close-time flush
/// cascade (a node flushes once when its own inbox closes, `run_transform`/`run_lua` below), with
/// no change needed to any `Input` implementation.
///
/// The mechanism: `Input::run` takes its `Fanout` by value, so racing `input.run(fanout)` against
/// `shutdown` and letting `shutdown` win *drops* that future -- and with it, the last `Fanout` (and
/// therefore the last `Sender`) into every one of that listener's downstream inboxes. Those inboxes
/// then observe every sender gone and close, exactly as they do today when a listener returns
/// `Ok(())` on its own (`FiniteInput` in this module's tests proves that cascade already works).
pub async fn run_with_shutdown(
    graph: Graph,
    specs: HashMap<String, NodeSpec>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    run_with_telemetry(graph, specs, HashMap::new(), shutdown).await
}

/// Same as [`run_with_shutdown`], but with a per-component [`Telemetry`] handle attached to every
/// node -- `run`/`run_with_shutdown` are thin wrappers over this with an empty map, which is what
/// makes them (and every existing caller and test) cost nothing new: a component with no entry
/// gets [`Telemetry::default`], the disabled handle, same as if this function never existed. Built
/// by `logit-cli::pipeline::prepare` only when a config's `internal` component asks for a live
/// `Registry` (`docs/design/internal-telemetry.md`).
pub async fn run_with_telemetry(
    graph: Graph,
    mut specs: HashMap<String, NodeSpec>,
    mut telemetry: HashMap<String, Telemetry>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    // A `watch` (not a `oneshot`) because every listener needs its own clone of the receiver, and
    // `watch::Receiver` is `Clone` where `oneshot::Receiver` is not. Driven from a spawned task
    // rather than shared directly so this function doesn't need to name `shutdown`'s own type in
    // more than one place.
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // Cloned before the move below so the join loop still has a handle to trigger shutdown on
    // the first task error -- see the loop's own comment further down. `watch::Sender::send` is
    // idempotent (it just overwrites the latest value and notifies watchers), so it's harmless if
    // both this clone and `shutdown_tx_for_driver` (via `shutdown` resolving on its own, e.g. a
    // real SIGTERM under `run_with_shutdown`) end up calling `send(true)`.
    let shutdown_tx_for_driver = shutdown_tx.clone();
    let shutdown_driver = tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_tx_for_driver.send(true);
    });

    let ids: Vec<String> = graph.components.keys().cloned().collect();

    let mut senders: HashMap<String, mpsc::Sender<Delivered>> = HashMap::with_capacity(ids.len());
    let mut inboxes: HashMap<String, mpsc::Receiver<Delivered>> = HashMap::with_capacity(ids.len());
    for id in &ids {
        let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
        senders.insert(id.clone(), tx);
        inboxes.insert(id.clone(), rx);
    }

    let mut tasks: JoinSet<anyhow::Result<()>> = JoinSet::new();
    // Needed only for Lua nodes -- see `run_lua`'s doc comment. `Handle::current()` requires an
    // async context, true here since `run` is itself running as a task on this runtime.
    let runtime_handle = tokio::runtime::Handle::current();

    for id in ids {
        let component = graph.components.get(&id).expect("id came from this graph");
        let node_telemetry = telemetry.remove(&id).unwrap_or_default();
        let fanout = Fanout::new(component.consumers.iter().map(|c| senders[c].clone()).collect())
            .with_telemetry(node_telemetry.clone());
        let inbox = inboxes.remove(&id).expect("an inbox was created for every id above");
        let spec = specs
            .remove(&id)
            .with_context(|| format!("no implementation registered for component '{id}'"))?;

        match spec {
            NodeSpec::Input(input) => {
                // A listener's own inbox is never written to (arity rule: a listener has no
                // sources, so nothing ever names it as a source and sends into it) -- nothing
                // reads it either. A listener's own send-side telemetry (batches/events sent,
                // send-blocked duration) comes from `fanout` above, already attached -- nothing
                // further to instrument here.
                drop(inbox);
                tasks.spawn(run_input(id, input, fanout, shutdown_rx.clone()));
            }
            NodeSpec::Output(output, queue_config, write_config) => {
                tasks.spawn(run_output(
                    id,
                    output,
                    inbox,
                    node_telemetry,
                    queue_config,
                    write_config,
                    shutdown_rx.clone(),
                ));
            }
            NodeSpec::Transform(transform) => {
                tasks.spawn(run_transform(transform, inbox, fanout, node_telemetry));
            }
            NodeSpec::Lua { script, interval } => {
                let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();
                let handle = runtime_handle.clone();
                let thread_id = id.clone();
                std::thread::Builder::new()
                    .name(format!("logit-{id}"))
                    .spawn(move || {
                        run_lua(
                            thread_id,
                            script,
                            interval,
                            ready_tx,
                            inbox,
                            fanout,
                            node_telemetry,
                            handle,
                        )
                    })
                    .with_context(|| format!("spawning thread for component '{id}'"))?;
                match ready_rx.await {
                    Ok(Ok(())) => {}
                    Ok(Err(message)) => anyhow::bail!("component '{id}': {message}"),
                    Err(_) => {
                        anyhow::bail!("component '{id}': thread exited before reporting ready")
                    }
                }
            }
        }
    }

    // Every consumer's `Fanout` already holds its own clone of the `Sender`s it needs -- this
    // map's own clones are construction-only scaffolding. Left alive, they'd each be one extra
    // outstanding `Sender` on every channel for the rest of `run`, so a channel would never
    // observe every real sender dropped and close -- the shutdown cascade (an inbox closing,
    // triggering that node's close-time flush and exit) could never fire, and `run` would hang
    // forever waiting on tasks that are themselves waiting on inboxes that can never close.
    drop(senders);

    // On the first error (from either arm below), record it and trigger the same shutdown signal
    // SIGTERM already drives -- every remaining task then gets the graceful-shutdown treatment it
    // already knows how to handle (a listener's inbox closes normally, cascading through to
    // `write_loop`'s shutdown-grace drain, `docs/adr/0021-buffered-sink-delivery.md`) instead of
    // being aborted mid-flight by dropping `tasks` early, which would silently discard a healthy
    // sibling's buffered, not-yet-delivered work. Keep `join_next`ing until every task has actually
    // exited (the loop condition, unchanged) rather than breaking -- only the *first* error is kept
    // (a later, cascading error from a task that's now shutting down because of the first one must
    // not overwrite it), but a second error is still observed and discarded here rather than
    // aborting the loop.
    let mut result: anyhow::Result<()> = Ok(());
    while let Some(joined) = tasks.join_next().await {
        let outcome = match joined {
            Ok(Ok(())) => continue,
            Ok(Err(err)) => err,
            Err(join_err) => join_err.into(),
        };
        if result.is_ok() {
            result = Err(outcome);
            let _ = shutdown_tx.send(true);
        }
    }
    // Whether `shutdown` ever resolved on its own or not (in `run`'s case, it's `pending()` and
    // never will), this task has nothing left to do once every node has exited -- the join loop
    // above has already observed that (its `while` condition only becomes `false` once every task
    // has actually finished, whether that happened via `shutdown` or via the error path above
    // flipping `shutdown_tx` itself), so abort rather than leave it parked forever holding its own
    // clone of `shutdown_tx`.
    shutdown_driver.abort();
    result
}

async fn run_input(
    id: String,
    mut input: Box<dyn Input + Send>,
    fanout: Fanout,
    mut shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    tokio::select! {
        result = input.run(fanout) => result.with_context(|| format!("component '{id}'")),
        // `wait_for` checks the current value before waiting on a change, so this resolves
        // immediately if `shutdown` was already flipped before this task started racing it --
        // unlike `changed()`, which only fires on a transition and could otherwise miss a
        // shutdown that landed between this receiver's creation and this select.
        _ = shutdown.wait_for(|&due| due) => Ok(()),
    }
}

/// A sink node's drain-and-deliver pair, decoupled through a [`SinkQueue`]
/// (`docs/adr/0021-buffered-sink-delivery.md`): [`drain_inbox`] moves every `Delivered` off this
/// component's inbox into the queue as fast as the queue's own bounds allow, while [`write_loop`]
/// delivers from the queue independently -- so a slow or backing-off `Output::send` no longer
/// stops this sink's own inbox from moving batches into its (deeper, byte-bounded) queue, the way
/// the single inline loop this replaced did.
///
/// The two run as one task, joined here rather than each spawned separately, so this function's
/// `Err` (from `write_loop`; `drain_inbox` never fails) is what `run_with_telemetry`'s `JoinSet`
/// sees. `write_loop` no longer owns `output` for its whole lifetime -- it only ever borrows it,
/// via `output.as_mut()` -- specifically so this function can perform the final drain-and-flush
/// itself, *after* `drain` can no longer push anything new, rather than `write_loop` doing it from
/// inside a race it cannot see the other half of (a real bug an earlier version of this split had:
/// see `finish_and_flush`'s doc comment).
async fn run_output(
    id: String,
    mut output: Box<dyn Output + Send>,
    mut inbox: mpsc::Receiver<Delivered>,
    telemetry: Telemetry,
    queue_config: SinkQueueConfig,
    write_config: WriteLoopConfig,
    shutdown: watch::Receiver<bool>,
) -> anyhow::Result<()> {
    let queue = Arc::new(SinkQueue::new(queue_config, telemetry.clone()));
    let diag = Diagnostics::new(id.clone()).with_telemetry(telemetry.clone());

    // `inbox` is now owned by *this* function, not moved into `drain_inbox` -- `drain_inbox`
    // only ever borrows it (`&mut inbox`). This is what makes the abandoned-inbox accounting
    // below possible: dropping a future that merely borrowed `inbox` releases the borrow without
    // touching the channel itself, so whatever `drain_inbox` never got around to `recv()`-ing
    // stays right where it was, in `inbox`'s own buffer, for this function to still see and count.
    let mut drain = Box::pin(drain_inbox(&mut inbox, Arc::clone(&queue), telemetry.clone()));
    let mut write = Box::pin(write_loop(
        id.clone(),
        output.as_mut(),
        Arc::clone(&queue),
        telemetry.clone(),
        write_config,
        shutdown,
    ));

    // `tokio::join!` would wait for *both* futures every time, which is wrong here: `write_loop`
    // can return early (a permanent send failure, or shutdown grace expiring) while `inbox` is
    // still open, e.g. every real listener, which never closes its sender on its own. `drain_inbox`
    // has no way to learn that its consumer gave up, so it would keep pulling from `inbox` (and,
    // under `Block`, eventually park forever pushing into a queue nothing commits from any more)
    // -- hanging this task, and therefore `run`, indefinitely.
    //
    // `select!` fixes this without any new cancellation signal: whichever future finishes first
    // wins.
    // - `write` finishes first: either it drained to closed-and-empty (only possible once `drain`
    //   has already run `queue.close()` -- by construction `drain` is then already `Ready`, so no
    //   work is lost by not polling it again), it bailed with a fatal error, or shutdown grace
    //   expired. Either way, if `drain` is still pending, it is dropped below -- `Box::pin`ned
    //   locally, never polled again after this point. Dropping it does **not** drop `inbox` any
    //   more (see the field comment above): whatever was still sitting in `inbox`'s own buffer,
    //   never `recv()`-ed by the abandoned `drain`, is swept up and counted just below, instead of
    //   silently vanishing along with the receiver the way an owned-`inbox` `drain_inbox` used to.
    // - `drain` finishes first (its inbox closed normally): `write_loop` hasn't necessarily
    //   finished draining the queue's tail yet, so wait for it.
    // `write` (a `Pin<Box<dyn Future>>`) holds `output`'s mutable borrow until it is dropped, and
    // this function needs that borrow released before it can reclaim `output` for
    // `finish_and_flush` below. The two arms consume `write` differently -- the first only
    // *polls* it via `&mut write`, leaving the outer binding to be dropped explicitly once we
    // know which arm fired; the second calls `write.await` on the owned binding directly, which
    // fully consumes (and so drops) it as part of driving it to completion. Routed through an
    // intermediate `Option` (rather than dropping `write` unconditionally after the `select!`)
    // because the borrow checker tracks the move `write.await` performs per-arm -- referencing
    // `write` after the `select!` regardless of which arm ran does not typecheck even though only
    // one arm's move ever actually happens at runtime.
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

    // Only now, with `write` finished (and dropped) and `drain` either already finished or about
    // to be dropped (never polled again once this local variable goes out of scope), can nothing
    // further be pushed into `queue` -- so this snapshot is genuinely final. See
    // `finish_and_flush`.
    drop(drain);

    // A `drain` abandoned mid-flight (the `write`-finishes-first case above) may leave batches
    // sitting in `inbox`'s own buffer -- accepted by the channel but never `recv()`-ed, since
    // `drain_inbox`'s loop never got back around to pulling them out before this function stopped
    // polling it. Those batches never reached `queue` at all, so `finish_and_flush` below (which
    // only ever sees what's *in* `queue`) cannot count them; without this sweep they would vanish
    // with no `batches.dropped` count and no diagnostic, unlike every other drop path this
    // workstream instruments. `try_recv` is non-blocking and exits as soon as `inbox` reports
    // empty (or disconnected, the ordinary case when `drain` already ran `inbox` dry on its own),
    // so this never waits for a sender that may never come.
    let mut abandoned_batches: u64 = 0;
    let mut abandoned_events: u64 = 0;
    while let Ok(delivered) = inbox.try_recv() {
        let batch = unwrap_batch_arc(delivered);
        abandoned_batches += 1;
        abandoned_events += batch.events.len() as u64;
    }
    if abandoned_batches > 0 {
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
            "{abandoned_batches} batch(es) ({abandoned_events} event(s)) still in this sink's \
             inbox, never handed to its delivery queue, when this sink stopped"
        ));
    }

    finish_and_flush(&diag, &queue, &telemetry, output.as_mut()).await;

    write_result
}

/// Moves every `Delivered` batch off `inbox` into `queue`, as fast as `queue.push` (governed by
/// its own bounds/overflow policy) allows -- entirely independent of how long `write_loop`'s
/// current delivery attempt is taking. `Delivered::Owned` costs one `Arc::new` here (previously
/// zero on this path -- a real, measured, and accepted cost, see
/// `crates/logit-bench/tests/allocations.rs` and `docs/design/memory.md`); `Delivered::Shared` is
/// already an `Arc`, so this is just a move, no clone. Closes `queue` once `inbox` itself closes,
/// which is what lets `write_loop`'s `queue.peek()` loop discover "closed and empty" and return --
/// no separate close-detection logic needed on that side.
///
/// Takes `&mut inbox`, not an owned receiver -- `run_output` retains ownership specifically so
/// dropping this future (when `write_loop` gives up first, see `run_output`'s `select!`) releases
/// only the borrow, not the channel itself, leaving whatever this loop hadn't yet `recv()`-ed
/// still sitting in `inbox`'s buffer for `run_output`'s own abandoned-inbox sweep to find and
/// count, instead of vanishing along with a dropped owned `Receiver`.
///
/// `pub` (rather than crate-private, like every other node-loop function here) purely so
/// `logit-bench`'s allocation tests can drive it directly, isolating exactly this hop's cost --
/// see `crates/logit-bench/tests/allocations.rs`.
pub async fn drain_inbox(
    inbox: &mut mpsc::Receiver<Delivered>,
    queue: Arc<SinkQueue>,
    telemetry: Telemetry,
) {
    while let Some(delivered) = inbox.recv().await {
        let batch = unwrap_batch_arc(delivered);
        telemetry.count("logit.component.batches.received", 1.0, &[]);
        telemetry.count("logit.component.events.received", batch.events.len() as f64, &[]);
        queue.push(batch).await;
    }
    queue.close();
}

/// Shared by [`drain_inbox`] and `run_output`'s abandoned-inbox sweep: `Delivered::Owned` costs
/// one `Arc::new` (previously zero on this path -- see `drain_inbox`'s own doc comment);
/// `Delivered::Shared` is already an `Arc`, so this is just a move.
///
/// Discards `delivered`'s `TraceContext` -- the sink/output path doesn't propagate trace context
/// yet (`Output::send` still takes `&EventBatch`, not `&Delivered`; see `unwrap_batch`'s own doc
/// comment and `docs/design/pipeline-graph.md`'s "Trace context propagation" section).
fn unwrap_batch_arc(delivered: Delivered) -> Arc<EventBatch> {
    match delivered {
        Delivered::Owned(batch, _ctx) => Arc::new(batch),
        Delivered::Shared(shared, _ctx) => shared,
    }
}

/// Retry budget for [`write_loop`]'s generic `deliver_with_retry`, driving every sink -- moved
/// here from `logit-outputs::influxdb::RetryPolicy`, which owned its own retry loop before
/// `docs/adr/0021-buffered-sink-delivery.md`; now every sink gets retry for free instead of
/// reimplementing it.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Hard ceiling on total time spent retrying one batch, across every attempt and every
    /// backoff sleep combined. Checked before each backoff sleep, so the budget is a hard ceiling
    /// regardless of how many attempts fit inside it. A fresh budget starts the moment a batch is
    /// first attempted -- not shared across batches.
    pub total_budget: Duration,
    /// Backoff after attempt `n` is `base_delay * 2^(n-1)`, capped at `max_delay` and further
    /// clamped to whatever's left of `total_budget`. No jitter: there's exactly one writer per
    /// `SinkQueue`, not a fleet thundering-herding a shared endpoint.
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        // Widened from ADR 0013's ~5s default: a stall here no longer reaches the drain loop or
        // the listener behind it (docs/adr/0021-buffered-sink-delivery.md), since the queue
        // absorbs it instead -- so a much larger budget, enough to ride out a real destination
        // restart, is now affordable.
        Self {
            total_budget: Duration::from_secs(60),
            base_delay: Duration::from_millis(200),
            max_delay: Duration::from_secs(10),
        }
    }
}

/// Config `write_loop` needs beyond `SinkQueueConfig` (which governs the queue `drain_inbox`
/// pushes into, not delivery itself). Config-file exposure of these fields is workstream F.
#[derive(Debug, Clone, Copy)]
pub struct WriteLoopConfig {
    pub retry: RetryConfig,
    /// Once the shutdown signal fires, `write_loop`'s remaining allowed drain time is capped at
    /// this, measured from the moment shutdown first fired (not reset per batch) -- so a
    /// permanently-down sink can't hang process exit indefinitely under SIGTERM. See
    /// `docs/adr/0021-buffered-sink-delivery.md`'s "shutdown grace" section.
    pub shutdown_grace: Duration,
    /// Overrides the delivery posture `write_loop` would otherwise derive from
    /// `output.duplicate_safe()` (`docs/adr/0021-buffered-sink-delivery.md`'s three-layer posture
    /// design: sink fact -> runtime default -> this config override). `None` -- the default --
    /// means "use the derived default"; workstream F's `logit-config::BufferConfig::delivery`
    /// is what sets this per component.
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
    /// Never delivered -- either `fault` wasn't retryable under the resolved posture, or it was
    /// but the retry budget ran out first. Either way the caller commits the batch off the queue
    /// and counts it. `explicit_permanent` is a *narrower* fact than `fault == Fault::Permanent`:
    /// it's true only when the sink itself attached `Fault::Permanent`, never when `classify`
    /// merely defaulted to it for an unclassified error -- only the caller's fatal-streak logic
    /// (see `write_loop`) needs this distinction; retry decisions already went through
    /// `is_retryable` using the (possibly defaulted) `fault` alone.
    Dropped {
        fault: Fault,
        explicit_permanent: bool,
    },
}

/// Attempts to deliver `batch` via `output.send`, retrying per `posture`/[`is_retryable`] until
/// either it succeeds, a failure isn't retryable, or `retry.total_budget` (a fresh budget for this
/// call) is exhausted. Moved here from `logit-outputs::influxdb::InfluxDbOutput::send`'s own retry
/// loop (`docs/adr/0021-buffered-sink-delivery.md`) -- every sink gets this for free now, driven by
/// its own `Fault` classification and `duplicate_safe` fact rather than reimplementing the loop.
///
/// **Every attempt, including the first, is raced against the remaining budget** via
/// `tokio::time::timeout` -- `retry.total_budget`'s own doc comment already promises a "hard
/// ceiling on total time spent... across every attempt," but a single un-raced `output.send` could
/// blow straight through it: a sink's own internal timeout (e.g. `InfluxDbOutput`'s 10s HTTP
/// client timeout) can be far larger than a configured `retry_budget`, so nothing would actually
/// enforce the budget until the loop got back around to checking it *after* that one attempt
/// finally gave up on its own. A timeout here is classified `Fault::Ambiguous` (the destination
/// may have received the request before this gave up waiting on the response) -- never
/// `Permanent`, since giving up early says nothing about whether the request was valid.
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

/// The backoff before retry attempt `attempt + 1`: `base_delay` doubled `attempt - 1` times via
/// repeated `saturating_mul`, stopping early once it's already at or past `max_delay` -- correct
/// for *any* `base_delay`/`max_delay` pair, not just `RetryConfig::default`'s. Moved here from
/// `logit-outputs::influxdb` verbatim (`docs/adr/0021-buffered-sink-delivery.md`) -- it was never
/// InfluxDB-specific, just historically homed on the one sink that had a retry loop at all. See
/// that module's history for why the loop is bounded at 128 iterations and why a single
/// `base_delay * 2u32.pow(shift)` with a fixed shift cap doesn't work for every `base_delay`/
/// `max_delay` pair.
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

/// Resolves once `shutdown`'s grace period has fully elapsed -- never before `shutdown` fires at
/// all, and never more than `grace` after it does. `deadline` is `&mut` so it persists across
/// repeated calls (once per `write_loop` iteration): the grace window is anchored to the instant
/// shutdown first fired, not reset by racing this again for a later batch. Setting `*deadline`
/// happens as a plain synchronous step the moment `shutdown.wait_for` resolves -- so it takes
/// effect even if this particular call loses a `tokio::select!` race and gets dropped before its
/// own `sleep_until` resolves; the next call sees `deadline` already `Some` and skips straight to
/// waiting out whatever's left of it.
async fn shutdown_grace_expired(
    shutdown: &mut watch::Receiver<bool>,
    deadline: &mut Option<tokio::time::Instant>,
    grace: Duration,
) {
    if deadline.is_none() {
        // An error here means the sender side is already gone -- treat that the same as shutdown
        // having just fired, rather than hanging forever waiting for a signal that will never
        // come.
        let _ = shutdown.wait_for(|&due| due).await;
        *deadline = Some(tokio::time::Instant::now() + grace);
    }
    tokio::time::sleep_until(deadline.expect("just set above if it was None")).await;
}

/// Commits (drops) whatever `queue` still holds, counting and logging anything found, then
/// flushes `output` exactly once -- called from `run_output` only, only after nothing can push
/// into `queue` any more (`drain_inbox` has either already finished naturally or been dropped).
///
/// **This must not run from inside `write_loop`.** An earlier version did exactly that, on
/// shutdown-grace expiry: it drained the queue to empty, then `await`ed `output.flush()` -- but
/// `queue.commit()` wakes any producer blocked on `not_full`, so a concurrent `drain_inbox` could
/// push a *new* batch into the queue while `flush()` was still pending, land uncounted (this
/// function had already seen the queue go empty and moved on), and then get silently dropped when
/// `write_loop` returned and `run_output`'s `select!` cancelled `drain_inbox` -- no delivery, no
/// `reason="shutdown"` accounting, nothing. Running this only after `run_output` has already
/// ensured `drain_inbox` can push no more closes that gap: there is no longer any window between
/// "queue observed empty" and "flush called" for a producer to slip through.
///
/// This is also what makes `flush()` fire on the *ordinary* completion path too, not just on
/// shutdown: `run_output` calls this unconditionally after `write_loop` returns, whether that was
/// via the queue draining to closed-and-empty on its own, a fatal error, or shutdown grace
/// expiring -- `Output::flush`'s own contract ("called once after the last batch") doesn't carve
/// out an exception for the happy path, so this doesn't either. In the ordinary case the queue is
/// already empty here, so `dropped_batches` stays `0` and nothing but `flush()` itself happens.
async fn finish_and_flush(
    diag: &Diagnostics,
    queue: &SinkQueue,
    telemetry: &Telemetry,
    output: &mut (dyn Output + Send),
) {
    let mut dropped_batches: u64 = 0;
    let mut dropped_events: u64 = 0;
    while let Some(batch) = queue.commit() {
        dropped_batches += 1;
        dropped_events += batch.events.len() as u64;
    }
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
        // Unthrottled: this fires at most once per `run_output` (shutdown happens once), so
        // `warn_throttled`'s occurrence-count limiting -- built for a hot-path flood -- would be
        // pointless machinery here. Goes through `Diagnostics`, not a bare `eprintln!`, so it's
        // still attributed and still mirrored into telemetry (`docs/known-gaps.md`'s closed
        // `eprintln!` entry), same as every other diagnostic in this codebase.
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

/// Delivers from `queue`'s head, one batch at a time, until `queue.peek()` returns `None` (closed
/// and empty -- see [`SinkQueue::peek`]) or shutdown grace expires. Per batch: attempt delivery via
/// [`deliver_with_retry`], per the posture resolved from `write_config.delivery_override` (config,
/// workstream F) falling back to `output.duplicate_safe()`'s derived default
/// (`docs/adr/0021-buffered-sink-delivery.md`). On success, commit and reset the permanent-failure
/// streak. On failure (not retryable, or retryable but the budget ran out), commit anyway (the
/// process no longer exits on an ordinary sink failure), count and warn -- *except*: a run of
/// nothing but *explicitly classified* `Fault::Permanent` outcomes (see
/// [`is_explicitly_permanent`]), with no successful delivery anywhere in between, for
/// [`PERMANENT_FAILURE_WINDOW`], still ends this function with `Err` (and therefore `run_output`,
/// and therefore the whole pipeline) -- a genuinely misconfigured sink still fails loudly enough
/// for a restart-policy supervisor to notice. Both a budget-exhausted `Clean`/`Ambiguous` fault
/// *and* an unclassified error that merely defaulted to `Permanent` are a different failure mode (a
/// destination that's merely slow/down, or a sink that hasn't opted into `Fault` classification at
/// all, neither of which is a positively-identified configuration error) and reset the streak
/// exactly like a success would -- the streak only ever accumulates across a run of nothing *but*
/// the sink explicitly saying "this is a config error," never merely "nothing else was retryable."
///
/// Does **not** drain the queue or call `output.flush()` itself on any exit path -- that's
/// `run_output`'s job, after this function returns (see [`finish_and_flush`]'s doc comment for
/// why it must happen there and not here). `shutdown`: once it flips, this function's remaining
/// allowed time is capped at `write_config.shutdown_grace` from that point (see
/// [`shutdown_grace_expired`]) -- on expiry this returns `Ok(())` immediately, not an error: an
/// incomplete drain on shutdown is expected behavior, not a pipeline failure.
async fn write_loop(
    id: String,
    output: &mut (dyn Output + Send),
    queue: Arc<SinkQueue>,
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

    loop {
        // Two-step, rather than touching `output` from inside either `select!`'s handler arms:
        // one arm below (`deliver_with_retry`) already borrows `output` mutably for the future
        // itself, and a handler block running `output` too would be a second overlapping mutable
        // borrow as far as the borrow checker's concerned even though only one future is ever
        // actually driven to completion. Reducing each `select!` to a plain enum keeps every
        // `output` access outside the macro, in the `match` below, where there's no ambiguity.
        enum NextBatch {
            Batch(Arc<EventBatch>),
            Closed,
            ShutdownExpired,
        }
        let next = tokio::select! {
            batch = queue.peek() => match batch {
                Some(batch) => NextBatch::Batch(batch),
                None => NextBatch::Closed,
            },
            () = shutdown_grace_expired(&mut shutdown, &mut shutdown_deadline, write_config.shutdown_grace) => {
                NextBatch::ShutdownExpired
            }
        };
        let batch = match next {
            NextBatch::Batch(batch) => batch,
            NextBatch::Closed => break, // queue closed and empty: nothing left to deliver.
            NextBatch::ShutdownExpired => return Ok(()),
        };

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
                queue.commit();
                last_success = Some(tokio::time::Instant::now());
                permanent_streak_since = None;
            }
            Delivery::Dropped { fault, explicit_permanent } => {
                queue.commit();
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
                    // Anything short of an explicit configuration-error classification -- a
                    // budget-exhausted Clean/Ambiguous fault (a destination that's merely
                    // slow/down), or an unclassified error that only defaulted to Permanent --
                    // breaks the streak exactly like a success would: the exit condition is a
                    // sustained run of nothing *but* explicitly-identified configuration errors,
                    // not merely "nothing else was retryable."
                    permanent_streak_since = None;
                }
            }
        }
    }
    Ok(())
}

/// Telemetry accounting plus one `Output::send` call -- factored out (rather than left inline) for
/// the same reason [`process_batch`] below is: so it can be measured directly in
/// `crates/logit-bench/tests/allocations.rs`/`benches/pipeline.rs`, with no channel or the rest of
/// the node runtime involved. Unlike `process_batch` this stays `async`, because `Output::send`
/// itself is; call it from a `current_thread` runtime with no `tokio::spawn` to keep it measurable
/// the same way `fanout.rs`'s own tests already are.
///
/// No caller in this crate any more -- `run_output`'s real delivery path now goes through
/// `deliver_with_retry`/`write_loop` (`docs/adr/0021-buffered-sink-delivery.md`), which calls
/// `output.send` directly so it can classify the resulting `Fault` and drive retry/posture
/// decisions from it; `send_batch`'s all-or-nothing `anyhow::Result` return doesn't fit that. Kept
/// as its own `pub` function purely so the bench/allocations harness can still measure this exact
/// "one send, with telemetry" hop in isolation.
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

/// A `Transform`-trait node's loop: races its inbox against its own flush deadline (if it has
/// one), exactly the shape `run_lua` below uses for a Lua node with `interval` set -- but as a
/// plain tokio task, this one can use `tokio::time::timeout` directly with no `Handle::block_on`
/// indirection, since it's already running inside the async runtime.
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
            // Inbox closed: flush once more so an in-flight window isn't silently lost, then exit.
            if next_flush.is_some() {
                run_flush(&mut *transform, &fanout, &telemetry).await;
            }
            return Ok(());
        };
        // Read before `unwrap_batch` consumes `batch` -- this call's entire emission (whatever
        // survives `process_batch`, however many events it started from) traces back to this one
        // incoming batch, so it's the unambiguous parent (`TraceContext`'s own doc comment,
        // `crates/logit-pipeline/src/fanout.rs`). `run_flush` below has no such single parent and
        // deliberately doesn't do this.
        let parent = batch.context();
        // Lets a flush-bearing transform (only `Aggregator` today) record this batch as a
        // contributor to whatever it's about to absorb from it -- the flush-side linking
        // `TraceContext`'s doc comment and `docs/known-gaps.md`'s internal-spans entry describe.
        // A no-op for every other transform.
        transform.observe_batch_context(parent);
        let batch = unwrap_batch(batch);
        if let Some(out) = process_batch(&mut *transform, batch, &telemetry) {
            fanout.send_with_context(out, parent).await;
        }
    }
}

/// The per-batch body of `run_transform`'s loop above: telemetry accounting plus feeding every
/// event through `Transform::process`, collecting what survives. Factored out (rather than left
/// inline) so `crates/logit-bench/tests/allocations.rs` can measure the real code path directly,
/// instead of a hand-written replica -- the same "call it directly" approach
/// `docs/design/memory.md` §7 already uses for every other stage, applied to the node runtime for
/// the first time. `run_transform` is the only caller in this crate; `pub` is for the bench.
pub fn process_batch(
    transform: &mut (dyn Transform + Send),
    batch: EventBatch,
    telemetry: &Telemetry,
) -> Option<EventBatch> {
    telemetry.count("logit.component.batches.received", 1.0, &[]);
    telemetry.count("logit.component.events.received", batch.events.len() as f64, &[]);

    let process_timer = telemetry.timer("logit.component.process.duration");
    let mut out = Vec::with_capacity(batch.events.len());
    let mut absorbed: u64 = 0;
    for event in batch.events {
        match transform.process(&batch.resource, event) {
            Some(event) => out.push(event),
            None => absorbed += 1,
        }
    }
    drop(process_timer);
    if absorbed > 0 {
        telemetry.count(
            "logit.component.events.dropped",
            absorbed as f64,
            &[("reason", "absorbed")],
        );
    }
    if out.is_empty() {
        None
    } else {
        Some(EventBatch { resource: batch.resource, events: out })
    }
}

/// Shared by `run_transform`'s two flush call sites (the deadline tick and the close-time flush).
/// Timed as one call even when it yields several `(resource, events)` groups -- `flush`'s own
/// per-resource windowing (`docs/adr/0008-aggregation-window-semantics.md`) is internal to the
/// transform, not something this timing needs to break out further.
///
/// **Sends via plain `fanout.send` (a fresh trace root), not `send_with_context`.** A flushed
/// batch is built from however many incoming batches `Transform::process` absorbed since the last
/// tick -- an *n*-to-1 relationship, not the 1-to-1 the non-flush path (`process_batch`'s caller,
/// above) propagates a real parent for. `TraceContext`'s own doc comment
/// (`crates/logit-pipeline/src/fanout.rs`) and `docs/known-gaps.md`'s internal-spans entry both
/// track this as a deliberate, open gap -- not something this function is wrong to skip.
async fn run_flush(transform: &mut (dyn Transform + Send), fanout: &Fanout, telemetry: &Telemetry) {
    let timer = telemetry.timer("logit.component.flush.duration");
    let flushed = transform.flush(now_unix_nanos());
    drop(timer);
    for (resource, events_with_links) in flushed {
        if !events_with_links.is_empty() {
            telemetry.count("logit.component.flush.events", events_with_links.len() as f64, &[]);
            // The links aren't attached to anything yet -- nothing turns a (context, node, batch)
            // tuple into a real SpanRecord-carrying Event (docs/known-gaps.md's internal-spans
            // entry, item 2, still open). Discarded here on purpose; this is where they'll attach
            // once that exists.
            let events = events_with_links.into_iter().map(|(event, _links)| event).collect();
            fanout.send(EventBatch { resource, events }).await;
        }
    }
}

/// A Lua node's loop, on its own dedicated OS thread (`ScriptWorker` is `!Send`). Structurally
/// the same as `run_transform` above, but `tokio::time::timeout` needs an async context this
/// plain thread doesn't have on its own -- `runtime` (a `Handle` to the multi-thread runtime
/// `run` was called from) supplies one via `Handle::block_on`, legal here since this never runs
/// on the runtime's own worker threads and never nests inside another `.await`. `fanout.send`
/// becomes `fanout.send_blocking` for the same reason: no `.await` available outside `block_on`.
///
/// Unlike `Transform::flush`, a Lua `flush()` has no resource of its own to stamp its emitted
/// events with (`docs/adr/0008-aggregation-window-semantics.md`) -- `last_resource` tracks
/// whichever resource this component most recently saw on a real batch, defaulting to a fresh one
/// if none has arrived yet.
#[allow(clippy::too_many_arguments)]
fn run_lua(
    id: String,
    script: String,
    configured_interval: Option<Duration>,
    ready_tx: oneshot::Sender<Result<(), String>>,
    mut inbox: mpsc::Receiver<Delivered>,
    fanout: Fanout,
    telemetry: Telemetry,
    runtime: tokio::runtime::Handle,
) {
    let worker = match ScriptWorker::new(&script).and_then(|w| w.with_telemetry(telemetry.clone()))
    {
        Ok(worker) => worker,
        Err(err) => {
            // The receiver may already be gone if `run` bailed for an unrelated reason first;
            // nothing useful to do with that here.
            let _ = ready_tx.send(Err(format!("loading a transform script: {err}")));
            return;
        }
    };
    let _ = ready_tx.send(Ok(()));

    let mut next_flush = configured_interval.map(|interval| tokio::time::Instant::now() + interval);
    let mut last_resource = Arc::new(Resource::default());

    // Sends via plain `fanout.send_blocking` below (a fresh trace root), same reasoning as
    // `run_flush`'s own doc comment above: a Lua `flush()`'s emission has no single incoming
    // batch to call its parent -- worse than `Transform::flush`, even, since there's no
    // accumulator here at all to eventually attribute it to (`docs/known-gaps.md`'s
    // internal-spans entry, and the existing resource-stamping gap this same imprecision already
    // has: `last_resource` above is the identical shape of approximation, just for `Resource`
    // instead of `TraceContext`).
    let flush_now = |worker: &ScriptWorker, resource: &Arc<Resource>, fanout: &Fanout| {
        let timer = telemetry.timer("logit.component.flush.duration");
        let result = worker.flush();
        drop(timer);
        // Sampled here too, not only after a batch (below) -- `ScriptWorker::used_memory`'s own
        // doc comment names accumulation *across `flush()` calls* as exactly the leak shape this
        // metric exists to catch. A script whose only growth happens in `flush()` (nothing new
        // arriving on the inbox between ticks) would otherwise leave `logit.script.vm.memory`
        // frozen or absent for as long as the input stays idle -- the metric would go silent right
        // when it matters. Sampled unconditionally, including the empty and error outcomes below:
        // the VM's memory doesn't care whether `flush()` had anything to emit.
        telemetry.gauge("logit.script.vm.memory", worker.used_memory() as f64, &[]);
        match result {
            Ok(events) if !events.is_empty() => {
                telemetry.count("logit.component.flush.events", events.len() as f64, &[]);
                fanout.send_blocking(EventBatch { resource: resource.clone(), events });
            }
            Ok(_) => {}
            Err(err) => {
                telemetry.count("logit.component.errors", 1.0, &[("reason", "flush")]);
                eprintln!("component '{id}': script flush error: {err}");
            }
        }
    };

    loop {
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                flush_now(&worker, &last_resource, &fanout);
                let interval = configured_interval
                    .expect("next_flush is only ever Some for a component with an interval");
                next_flush = Some(advance_flush_deadline(deadline, now_instant, interval));
            }
        }

        let batch = match next_flush {
            None => inbox.blocking_recv(),
            Some(deadline) => {
                let wait = deadline.saturating_duration_since(tokio::time::Instant::now());
                // The `async` block matters, not just style: `tokio::time::timeout` builds its
                // `Sleep` eagerly, and `Sleep` construction needs a runtime context, which this
                // plain thread doesn't have outside of `block_on`. Deferring construction to
                // inside the block (only polled once `block_on` has entered that context) is what
                // makes this legal rather than an immediate panic.
                match runtime.block_on(async { tokio::time::timeout(wait, inbox.recv()).await }) {
                    Ok(batch) => batch,
                    Err(_elapsed) => continue,
                }
            }
        };
        let Some(batch) = batch else {
            if next_flush.is_some() {
                flush_now(&worker, &last_resource, &fanout);
            }
            return;
        };
        // Read before `unwrap_batch` consumes `batch` -- same reasoning as `run_transform`'s
        // non-flush path (`crates/logit-pipeline/src/fanout.rs`'s `TraceContext` doc comment):
        // this call's entire emission traces back to this one incoming batch. `flush_now` above
        // has no such single parent and deliberately doesn't do this.
        let parent = batch.context();
        // Lets the script's own `process()` read `trace.trace_id`/`trace.span_id`
        // (`crates/logit-script/src/trace.rs`) -- essentially infallible in practice (the
        // registry-held table is independent of whatever a script does to the `trace` global),
        // logged rather than treated as fatal on the off chance it isn't.
        if let Err(err) = worker.set_trace_context(parent.trace_id, parent.span_id) {
            eprintln!("component '{id}': setting trace context failed: {err}");
        }
        let batch = unwrap_batch(batch);
        last_resource = batch.resource.clone();
        telemetry.count("logit.component.batches.received", 1.0, &[]);
        telemetry.count("logit.component.events.received", batch.events.len() as f64, &[]);

        let process_timer = telemetry.timer("logit.component.process.duration");
        let mut out = Vec::with_capacity(batch.events.len());
        let mut dropped: u64 = 0;
        let mut errors: u64 = 0;
        for event in batch.events {
            match worker.process(event) {
                Ok(ProcessOutcome::Emit(e)) => {
                    out.push(*e);
                    telemetry.count("logit.script.events.emitted", 1.0, &[("outcome", "emit")]);
                }
                Ok(ProcessOutcome::EmitMany(es)) => {
                    telemetry.count(
                        "logit.script.events.emitted",
                        es.len() as f64,
                        &[("outcome", "emit_many")],
                    );
                    out.extend(es);
                }
                Ok(ProcessOutcome::Drop) => dropped += 1,
                Err(err) => {
                    errors += 1;
                    eprintln!("component '{id}': script error: {err}");
                }
            }
        }
        drop(process_timer);
        // Sampled once per batch, not per event: the strongest single candidate found for
        // observing a stateful script leaking VM-side state (`docs/design/internal-telemetry.md`)
        // -- otherwise invisible until the process's own memory visibly grows.
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
        }
        if !out.is_empty() {
            fanout.send_blocking_with_context(
                EventBatch { resource: batch.resource, events: out },
                parent,
            );
        }
    }
}

/// Turns the channel payload back into an owned `EventBatch`, right before handing it to a
/// `Transform`/`ScriptWorker::process` -- called from `run_transform`/`run_lua` only. `run_output`
/// (above) never calls this: `Output::send` takes `&EventBatch`, so it borrows straight out of the
/// `Delivered` instead, which is what actually realizes the fan-out saving for an `Output` branch
/// (`docs/adr/0016-arc-eventbatch-copy-on-write.md`'s "Round two"). `Transform`/`ScriptWorker`
/// still need to mutate or consume an *owned* `Event`, so this unwrap can't be skipped for them.
///
/// `Delivered::Owned` (a single-consumer edge) is already the owned batch: no `Arc` was ever
/// involved, so this is free. `Delivered::Shared` (a real fan-out) unwraps via `Arc::try_unwrap`,
/// which succeeds with no clone whenever this is the only remaining strong reference -- in
/// practice, whichever branch happens to drop its own reference last at runtime. That is a
/// best-effort saving over always cloning, not a guarantee that exactly one branch pays nothing:
/// nothing about `Fanout::send` privileges one branch's handle over another's, and two branches
/// racing to unwrap concurrently can both still observe a strong count above 1 and both fall back
/// to cloning.
///
/// **An `Output` sibling on the same fan-out doesn't change this into a guarantee either way --
/// it's still genuinely racy, just against a different clock than it used to be.** Before
/// `docs/adr/0021-buffered-sink-delivery.md` split `run_output` into `drain_inbox`/`write_loop`,
/// the `Output` branch held its `Delivered` handle for the full duration of `output.send`, which
/// typically does real I/O -- slower than a `Transform`'s local processing, making a clone (cost
/// 6) the likelier practical outcome even though a free unwrap (cost 1) was reachable. That's no
/// longer the shape: `drain_inbox` drops its `Delivered` the moment it matches it -- immediately on
/// receipt, before even `queue.push`, entirely decoupled from how long the paired `write_loop`'s
/// `output.send` takes. So whether *this* function's unwrap succeeds for a `Transform`/Lua sibling
/// now comes down to whether `drain_inbox`'s near-instant match-and-drop wins the race against this
/// call actually running, which -- both being cheap, local work with no I/O on either side -- is
/// close to a coin flip biased toward the `Output` side finishing first, not the slow-I/O-bound
/// race the old shape had. Both outcomes are still genuinely reachable (see
/// `fanout_send_mixed_output_and_transform_consumers[_when_output_finishes_first]`,
/// `crates/logit-bench/tests/allocations.rs` -- those tests measure `Fanout::send` directly, not
/// through `run_output`, so their numbers are unaffected by this shift; only the *practical
/// likelihood* of each outcome in the real running pipeline changed). Either outcome keeps
/// isolation intact: a sibling branch's copy is always independent before it can be mutated
/// (`a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch` below pins this).
///
/// `pub` (rather than crate-private) for `crates/logit-bench/tests/allocations.rs`, which needs
/// to measure this allocation-relevant path directly rather than reconstruct it.
///
/// Discards `batch`'s `TraceContext` -- call [`Delivered::context`] first if the caller needs it
/// as a parent for whatever it goes on to emit (`run_transform`/`run_lua`'s non-flush paths do).
/// Kept out of this function's own return type deliberately: every existing caller before
/// propagation landed just wanted the `EventBatch`, and `context()` costs nothing extra to call
/// separately (it's a `Copy` read, not a consuming one).
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
fn advance_flush_deadline(
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
    use crate::sink_queue::OverflowPolicy;
    use logit_config::{Component, ComponentKind, Config};
    use logit_core::{Event, MetricKind, Registry, SpanLink};
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
            // `Output::send` only ever borrows (`docs/adr/0016-arc-eventbatch-copy-on-write.md`);
            // this test double clones onto its own plain `std::sync::mpsc` channel purely so the
            // assertion side of each test can inspect what arrived after this async fn returns.
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
            // A real listener loops forever; for this test, just idle so `run`'s JoinSet has
            // something to keep alive until the assertion side has what it needs.
            std::future::pending::<()>().await;
            Ok(())
        }
    }

    fn counter_event(name: &str, value: f64) -> Event {
        use logit_core::{interner::intern, AttrMap, MetricKind, MetricRecord};
        Event::metric(
            0,
            AttrMap::new(),
            MetricRecord { name: intern(name), kind: MetricKind::Counter(value), unit: None },
        )
    }

    #[tokio::test]
    async fn a_lua_node_processes_events_end_to_end_through_the_graph() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "enrich".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
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
                sources: vec!["enrich".to_string()],
                kind: ComponentKind::InfluxDbOut {
                    url: "http://localhost:8086".to_string(),
                    org: "org".to_string(),
                    bucket: "bucket".to_string(),
                    token: "TOKEN".to_string(),
                },
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(OneShotInput { batch: Some(batch) })),
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
                SinkQueueConfig::default(),
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

    /// Unlike `OneShotInput` above (which idles forever after sending, to keep the graph alive
    /// for that test's assertion), this returns as soon as it's sent its one batch -- a real
    /// listener that has genuinely finished.
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

    /// Regression test: `run`'s internal `senders` map used to keep one extra `Sender` clone
    /// alive, for every channel, for `run`'s entire lifetime -- so a downstream inbox could never
    /// observe every real sender dropped and close, the shutdown cascade could never fire, and
    /// `run` hung forever even after its only input had genuinely finished.
    #[tokio::test]
    async fn run_returns_once_the_only_input_finishes_instead_of_hanging() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
                kind: ComponentKind::InfluxDbOut {
                    url: "http://localhost:8086".to_string(),
                    org: "org".to_string(),
                    bucket: "bucket".to_string(),
                    token: "TOKEN".to_string(),
                },
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(FiniteInput { batch: Some(batch) })),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkQueueConfig::default(),
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

    /// A native `Transform` that mutates every event it sees by appending an extra metric --
    /// standing in for any real transform (a Lua enrichment stage, `kv_metrics`, ...) that changes
    /// an event on its way through one branch of a fan-out.
    struct MutatingTransform;

    impl Transform for MutatingTransform {
        fn process(&mut self, _resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
            use logit_core::{interner::intern, MetricKind, MetricRecord};
            event.metrics.push(MetricRecord {
                name: intern("extra"),
                kind: MetricKind::Counter(1.0),
                unit: None,
            });
            Some(event)
        }
    }

    /// Operationalizes branch isolation (docs/adr/0012-multi-payload-events.md). Since
    /// docs/adr/0016-arc-eventbatch-copy-on-write.md, `Fanout` no longer deep-clones eagerly at
    /// send time -- a real fan-out hands every branch its own `Delivered::Shared` handle onto one
    /// `Arc`, and the clone (if any) happens lazily, at `unwrap_batch`, right before a branch's own
    /// node can touch the batch at all. Whichever branch doesn't win `Arc::try_unwrap` gets a real,
    /// independent deep clone at that point, so no branch ever mutates a batch another branch still
    /// holds a handle to -- a mutation one branch of a fan-out makes is still never visible on a
    /// sibling branch's copy of the same upstream event, even now that `Event` can carry several
    /// payloads at once. Proven against a real two-branch fan-out (one listener feeding a
    /// mutating transform on one branch and a sink directly on the other -- exactly the "one
    /// listener, two independently-processed downstream chains" shape ADR 0009 exists to make an
    /// ordinary config, not an edge case), not just asserted in a design doc.
    #[tokio::test]
    async fn a_mutation_on_one_fan_out_branch_is_invisible_to_the_sibling_branch() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        // `Json` is only a graph-arity placeholder here -- the runtime doesn't check that a
        // component's `NodeSpec` implementation matches what its `ComponentKind` says, so
        // `branch_a`'s actual behavior below comes entirely from the `MutatingTransform` NodeSpec.
        components.insert(
            "branch_a".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
                kind: ComponentKind::Json { skip_to_brace: false },
            },
        );
        components.insert(
            "sink_a".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["branch_a".to_string()],
                kind: influxdb_out(),
            },
        );
        components.insert(
            "sink_b".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let (tx_a, rx_a) = std::sync::mpsc::channel();
        let (tx_b, rx_b) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(FiniteInput { batch: Some(batch) })),
        );
        specs.insert("branch_a".to_string(), NodeSpec::Transform(Box::new(MutatingTransform)));
        specs.insert(
            "sink_a".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: tx_a }),
                SinkQueueConfig::default(),
                WriteLoopConfig::default(),
            ),
        );
        specs.insert(
            "sink_b".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: tx_b }),
                SinkQueueConfig::default(),
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

    /// A local fake `Transform`, standing in for `logit-transforms::Aggregator` -- this crate
    /// can't depend on `logit-transforms` (`docs/design/pipeline-graph.md`'s "Crate layout": the
    /// dependency runs the other way). Absorbs every event it's given (`process` always returns
    /// `None`) and only ever emits them from `flush`, exactly the shape needed to prove a
    /// shutdown-triggered close-time flush actually drains what's buffered.
    struct WindowingTransform {
        interval: Duration,
        buffered: Vec<Event>,
    }

    impl Transform for WindowingTransform {
        fn process(&mut self, _resource: &Arc<Resource>, event: Event) -> Option<Event> {
            self.buffered.push(event);
            None
        }

        fn flush_interval(&self) -> Option<Duration> {
            Some(self.interval)
        }

        fn flush(&mut self, _now: i64) -> Vec<(Arc<Resource>, Vec<(Event, Vec<SpanLink>)>)> {
            if self.buffered.is_empty() {
                return Vec::new();
            }
            let events =
                std::mem::take(&mut self.buffered).into_iter().map(|e| (e, Vec::new())).collect();
            vec![(Arc::new(Resource::default()), events)]
        }
    }

    /// Like `OneShotInput`, but also signals `sent` once its one batch has been handed to
    /// `sink.send` -- so a test can wait for "the batch is definitely enqueued downstream" before
    /// triggering shutdown, rather than relying on timing.
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

    /// The SIGTERM-mid-window proof, without any real OS signal: `run_with_shutdown`'s `shutdown`
    /// future is driven by a plain oneshot the test controls directly. Fired only once the input
    /// confirms its batch is enqueued (via `SignalingInput`/`sent_rx` above) -- not because the
    /// ordering would otherwise be wrong (`mpsc::Receiver::recv` still drains whatever's already
    /// buffered before observing every sender gone), but so this test exercises exactly the
    /// "in-flight window, then shutdown" sequence its name promises, not "shutdown that happens to
    /// race a send."
    #[tokio::test]
    async fn run_with_shutdown_flushes_an_in_flight_window_before_exiting() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "windowed".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
                kind: ComponentKind::Aggregate {
                    interval: Duration::from_secs(3600),
                    gauge_retention: 5,
                    max_retained_gauge_series: 10_000,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["windowed".to_string()],
                kind: ComponentKind::InfluxDbOut {
                    url: "http://localhost:8086".to_string(),
                    org: "org".to_string(),
                    bucket: "bucket".to_string(),
                    token: "TOKEN".to_string(),
                },
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };

        let (sent_tx, sent_rx) = oneshot::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(SignalingInput { batch: Some(batch), sent: Some(sent_tx) })),
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
                SinkQueueConfig::default(),
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

    /// The property `unwrap_batch` (and the whole `Arc<EventBatch>` design,
    /// docs/adr/0016-arc-eventbatch-copy-on-write.md) rests on for a single-consumer edge: `Fanout`
    /// with exactly one consumer never touches an `Arc` at all, so what arrives at the other end is
    /// `Delivered::Owned` -- proving the fast path (item 1 of the PR #33 review) actually takes,
    /// not just that the code happens to also be correct if it didn't.
    #[tokio::test]
    async fn a_single_consumer_fanout_delivers_the_batch_owned_with_no_arc_involved() {
        let (tx, mut rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };

        fanout.send(batch).await;

        let received = rx.recv().await.expect("should receive");
        assert!(
            matches!(received, Delivered::Owned(_, _)),
            "a single-consumer edge should never wrap the batch in an Arc"
        );
    }

    /// The property `Arc::try_unwrap` at each consumption point actually depends on: a real
    /// fan-out's `Arc` reaches strong count 1 -- and so becomes unwrappable with no clone -- only
    /// once every sibling handle has been dropped. Demonstrated deterministically (no concurrent
    /// consumers racing each other) rather than asserted as a property of the design, since the PR
    /// review that asked for this test found that race is real: two branches unwrapping
    /// concurrently can both still observe strong count 2 and both fall back to cloning. This test
    /// pins the mechanics `unwrap_batch`'s fallback correctly handles either way, not the timing.
    #[tokio::test]
    async fn a_shared_batchs_arc_is_uniquely_held_only_once_every_sibling_handle_is_dropped() {
        let (tx_a, mut rx_a) = mpsc::channel(1);
        let (tx_b, mut rx_b) = mpsc::channel(1);
        let fanout = Fanout::new(vec![tx_a, tx_b]);
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
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

    /// Proves the whole point of layer 2 (`docs/design/internal-telemetry.md`): a listener, a
    /// `Transform`, and an `Output` each produce the uniform in/out metric set with zero code of
    /// their own, purely from `run_with_telemetry` attaching a handle per node. The listener's
    /// send-side numbers come entirely from its `Fanout` (`FiniteInput` itself never touches
    /// telemetry); the transform's and output's come from `run_transform`/`run_output`.
    #[tokio::test]
    async fn run_with_telemetry_records_the_uniform_metric_set_for_every_node_kind() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "xform".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
                kind: ComponentKind::Json { skip_to_brace: false },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["xform".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(FiniteInput { batch: Some(batch) })),
        );
        specs.insert("xform".to_string(), NodeSpec::Transform(Box::new(MutatingTransform)));
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkQueueConfig::default(),
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
            run_with_telemetry(g, specs, telemetry, std::future::pending()),
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
                    MetricKind::Counter(v) if logit_core::interner::resolve(m.name) == name => {
                        Some(*v)
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

    /// The Lua-specific layer-3 additions (`docs/design/internal-telemetry.md`): VM memory is
    /// visible, and a fan-out script (`return {a, b}`, `ProcessOutcome::EmitMany`) is
    /// distinguishable from a plain 1:1 script by outcome, not just by the aggregate event count
    /// `Fanout` already reports.
    #[tokio::test]
    async fn run_lua_records_vm_memory_and_emit_outcome() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "enrich".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
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
                sources: vec!["enrich".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(FiniteInput { batch: Some(batch) })),
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
                SinkQueueConfig::default(),
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
            run_with_telemetry(g, specs, telemetry, std::future::pending()),
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
                    MetricKind::Counter(v) if logit_core::interner::resolve(m.name) == name => {
                        Some(*v)
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

    /// A stateful script whose only growth happens inside `flush()` (nothing new arriving on the
    /// inbox between ticks) must not leave `logit.script.vm.memory` frozen or absent -- that's
    /// exactly the leak shape `ScriptWorker::used_memory`'s own doc comment names. Proven with no
    /// batch ever sent at all: `FiniteInput { batch: None }` finishes immediately, closing this
    /// Lua node's inbox and triggering the same close-time flush a real flush-interval tick would
    /// (`next_flush.is_some()` is all that's required, regardless of whether a deadline actually
    /// elapsed) -- so if `flush_now` didn't sample memory, this test would see no gauge at all.
    #[tokio::test]
    async fn a_flush_with_no_batch_ever_received_still_records_vm_memory() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "windowed".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
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
                sources: vec!["windowed".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert("in".to_string(), NodeSpec::Input(Box::new(FiniteInput { batch: None })));
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
                SinkQueueConfig::default(),
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
            run_with_telemetry(g, specs, telemetry, std::future::pending()),
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
    // `run_output`'s drain/write split (`docs/adr/0021-buffered-sink-delivery.md`)
    // -----------------------------------------------------------------------------------------

    /// A minimal one-shot gate for tests: `wait()` blocks until `open()` is called, from anywhere,
    /// any time relative to `wait()` -- race-free via the same "register `notified()` before
    /// checking state" idiom `SinkQueue` itself relies on (`sink_queue.rs`).
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

    /// A sink whose `send` doesn't resolve until a test-controlled [`Gate`] opens. Used to prove
    /// the point of the drain/write split: the inbox keeps draining into the `SinkQueue` while a
    /// delivery attempt is stuck, instead of the two being coupled the way the single inline loop
    /// this replaced was.
    struct SlowOutput {
        gate: Gate,
        // A tokio channel, not `std::sync::mpsc` -- this test's `run_with_telemetry` task keeps
        // running concurrently with the assertion side under a single-threaded test runtime, so
        // blocking that thread on a std-mpsc receive would starve the very task the test is
        // waiting on. `.recv().await` yields instead of blocking.
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

    /// A sink whose `send` always fails -- for pinning that a permanent failure still ends
    /// `run_output` (and therefore `run`) with an error, unchanged from before this split.
    struct FailingOutput;

    #[async_trait::async_trait]
    impl Output for FailingOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            anyhow::bail!("simulated permanent send failure")
        }
    }

    /// Sends every batch in `batches`, in order, then idles forever -- a burst producer standing
    /// in for a real listener that already has several batches ready before a slow sink can keep
    /// up. Unlike [`FiniteInput`], deliberately never returns, so the test controls exactly when
    /// the graph is torn down (via aborting the spawned `run` task) rather than racing a listener
    /// that finishes on its own.
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

    /// Like [`BurstInput`], but returns once every batch is sent -- a real listener that has
    /// genuinely finished, exactly like [`FiniteInput`] but with more than one batch.
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

    /// The property this whole workstream exists to deliver
    /// (`docs/adr/0021-buffered-sink-delivery.md`, `docs/plans/0004-buffered-sink-delivery.md`
    /// section C): a slow/backing-off `Output::send` no longer stops its own component's inbox
    /// from draining. Proven directly: while the sink's very first delivery attempt is parked on a
    /// gate, several more batches sent right behind it still make it off the inbox and into the
    /// `SinkQueue` -- observable via `logit.component.buffer.batches`, since nothing else exposes
    /// the queue's depth to a test driving a full graph rather than `SinkQueue` directly. Uses
    /// paused time so the polling loop below never depends on real wall-clock timing to be
    /// deterministic.
    #[tokio::test(start_paused = true)]
    async fn a_slow_sinks_send_in_flight_does_not_stop_its_inbox_from_draining_into_the_queue() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let batches: Vec<EventBatch> = (0..5)
            .map(|i| EventBatch {
                resource: Arc::new(Resource::default()),
                events: vec![counter_event("hits", i as f64)],
            })
            .collect();

        let gate = Gate::new();
        let (delivered_tx, mut delivered_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert("in".to_string(), NodeSpec::Input(Box::new(BurstInput { batches })));
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(SlowOutput { gate: gate.clone(), delivered: delivered_tx }),
                SinkQueueConfig {
                    max_batches: 100,
                    max_bytes: u64::MAX,
                    overflow: OverflowPolicy::Block,
                },
                WriteLoopConfig::default(),
            ),
        );

        let registry = Registry::new();
        let mut telemetry: HashMap<String, Telemetry> = HashMap::new();
        telemetry.insert("out".to_string(), registry.telemetry_for("out", "x", "sink"));

        let run_task =
            tokio::spawn(run_with_telemetry(g, specs, telemetry, std::future::pending()));

        // Poll (under paused time, so this never depends on real wall-clock passing) until the
        // queue's own depth gauge shows more than the one batch currently stuck inside
        // `output.send` -- proof the drain side kept moving batches into the queue instead of
        // waiting on the gated send.
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
                MetricKind::Counter(v) => {
                    assert_eq!(*v, i as f64, "batches should still be delivered in order")
                }
                other => panic!("expected Counter, got {other:?}"),
            }
        }

        run_task.abort();
    }

    /// **Behavior change from before this workstream, deliberate**
    /// (`docs/adr/0021-buffered-sink-delivery.md`'s "Failure handling" section): an isolated,
    /// unclassified send failure (defaults to `Fault::Permanent`, see `output::classify`) used to
    /// end `run` outright the moment it happened. It no longer does -- `write_loop` drops the
    /// batch, counts it, and moves on; only a *sustained* run of nothing but `Permanent` failures
    /// for the whole `PERMANENT_FAILURE_WINDOW` (pinned directly against `write_loop`,
    /// `sustained_permanent_failures_end_write_loop_once_the_failure_window_elapses` above) still
    /// ends the process. This test is the full-graph-level twin of that: with `FiniteInput`
    /// closing its sender right after its one batch (so the sink's queue drains to closed-and-
    /// empty right after the single drop), `run` now completes with `Ok(())`, not an error.
    #[tokio::test]
    async fn a_single_isolated_send_failure_no_longer_ends_run_the_batch_is_dropped_instead() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(FiniteInput { batch: Some(batch) })),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(FailingOutput),
                SinkQueueConfig::default(),
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

    /// The mirror of `a_single_isolated_send_failure_no_longer_ends_run_the_batch_is_dropped_instead`
    /// with a still-live input (`BurstInput` never closes its sender, unlike `FiniteInput`): a
    /// permanently failing sink no longer takes the whole pipeline down just because it keeps
    /// failing -- `run` simply keeps running (dropping every batch), exactly as intended for "one
    /// malformed batch cannot kill an otherwise-healthy pipeline." Asserted by showing `run`
    /// does *not* complete within a bounded window, rather than asserting an `Err` it no longer
    /// promptly returns.
    #[tokio::test]
    async fn a_permanently_failing_sink_with_a_live_input_does_not_end_run() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(Box::new(BurstInput { batches: vec![batch] })),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(FailingOutput),
                SinkQueueConfig::default(),
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

    /// A batch pushed just before the inbox closes must still reach the sink: `drain_inbox`
    /// closes the queue only once its own inbox is exhausted, and `write_loop`'s `queue.peek()`
    /// loop only stops once the queue is both closed *and* empty -- so the joined `run_output`
    /// future can't resolve while any of the tail is still undelivered. Exercised with several
    /// batches, not just one, so a bug that only drains the last-committed item wouldn't slip
    /// through.
    #[tokio::test]
    async fn inbox_close_drains_the_queues_tail_before_run_output_resolves() {
        let mut components = Map::new();
        components.insert(
            "in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["in".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let batches: Vec<EventBatch> = (0..5)
            .map(|i| EventBatch {
                resource: Arc::new(Resource::default()),
                events: vec![counter_event("hits", i as f64)],
            })
            .collect();

        let (result_tx, result_rx) = std::sync::mpsc::channel();
        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert("in".to_string(), NodeSpec::Input(Box::new(FiniteBurstInput { batches })));
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(RecordingOutput { tx: result_tx }),
                SinkQueueConfig::default(),
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
    // (`docs/adr/0021-buffered-sink-delivery.md`, workstream D)
    // -----------------------------------------------------------------------------------------

    /// A sink whose `send` fails with a `fault`-tagged error for its first `fail_times` calls,
    /// then succeeds forever after (`fail_times == u32::MAX` never succeeds) -- the fake every
    /// test below drives `write_loop` against directly. Each attempt is signaled on `attempted`
    /// (a test can `.recv().await` it to observe exactly when an attempt happened, race-free) and
    /// its instant recorded on `attempt_times`, so a test can assert the backoff schedule between
    /// attempts. `flushed` records whether `Output::flush` was ever called.
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

    /// Drives `write_loop` directly against a fresh, already-closed `SinkQueue` holding exactly
    /// `batches`, with a default (fast) retry config and a shutdown signal that never fires.
    /// Returns `write_loop`'s own result -- most tests below only care about `attempts`/
    /// `attempt_times`/`flushed`, read from the handles passed in separately.
    async fn run_write_loop_to_completion(
        mut output: FaultyOutput,
        batches: Vec<Arc<EventBatch>>,
        retry: RetryConfig,
    ) -> anyhow::Result<()> {
        let telemetry = Telemetry::default();
        let queue = Arc::new(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone()));
        for batch in batches {
            queue.push(batch).await;
        }
        queue.close();
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let write_config = WriteLoopConfig {
            retry,
            shutdown_grace: Duration::from_secs(5),
            delivery_override: None,
        };
        tokio::time::timeout(
            Duration::from_secs(5),
            write_loop("out".to_string(), &mut output, queue, telemetry, write_config, shutdown_rx),
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

    /// Pins the exact doubling sequence under a real (paused) clock: 100ms, 200ms, 400ms, 800ms
    /// between 5 attempts (4 failures then a success), matching `base_delay * 2^(attempt-1)`
    /// capped at `max_delay` (here, high enough never to clamp).
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

    /// A retryable (`Ambiguous`, under `AtLeastOnce`) fault that never succeeds is dropped once
    /// its own `total_budget` runs out -- `write_loop` continues to the next batch rather than
    /// returning `Err`, and the permanent-failure streak is completely untouched by this (a
    /// budget-exhausted `Clean`/`Ambiguous` drop is a "destination slow/down" failure mode, not a
    /// "misconfigured" one).
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

    /// The ~60s permanent-failure-window exit: sustained `Fault::Permanent` outcomes with no
    /// intervening success cause `write_loop` to return `Err` once the window elapses. A gap with
    /// nothing happening in between (simulated here by the queue sitting empty while `write_loop`
    /// waits) does not itself reset anything -- only a *successful* delivery would.
    #[tokio::test(start_paused = true)]
    async fn sustained_permanent_failures_end_write_loop_once_the_failure_window_elapses() {
        let (output, mut handles) = faulty_output(Fault::Permanent, u32::MAX, false);
        let telemetry = Telemetry::default();
        let queue = Arc::new(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone()));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);

        queue.push(one_event_batch(1.0)).await;

        let queue_for_task = Arc::clone(&queue);
        // `write_loop` borrows `output` (it no longer owns it -- `run_output` does, normally);
        // `tokio::spawn` needs a `'static` future, so `output` moves into this async block and
        // the `&mut` borrow it passes to `write_loop` lives entirely inside that block's own
        // stack frame, not tied to this test function's.
        let handle = tokio::spawn(async move {
            let mut output = output;
            write_loop(
                "out".to_string(),
                &mut output,
                queue_for_task,
                telemetry,
                WriteLoopConfig::default(),
                shutdown_rx,
            )
            .await
        });

        handles.attempted.recv().await.expect("the first permanent failure should have happened");

        // Nothing else happens for the rest of the window -- the queue sits empty and write_loop
        // just waits, exactly like `an intervening gap with no success does not reset anything`
        // above documents.
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW).await;

        queue.push(one_event_batch(2.0)).await;
        queue.close();

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

    /// A sink whose `send` outcome is driven by a fixed script -- `Ok(())` or an `Err` tagged with
    /// the given `Fault`, one entry consumed per call, repeating the script's last entry forever
    /// once exhausted. More flexible than [`FaultyOutput`]'s simpler "fail N times then always
    /// succeed" shape, for a test that needs a genuine fail/succeed/fail pattern.
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

    /// A single success anywhere inside the window resets the streak: `Permanent`, then a
    /// success, then well past where the *original* window would have tripped, one more isolated
    /// `Permanent` failure -- if the reset hadn't happened, "now - streak_since" would already be
    /// far past the window the instant that third failure lands, tripping `Err` immediately. It
    /// must not: the reset means this third failure starts a brand new, still-fresh streak.
    #[tokio::test(start_paused = true)]
    async fn a_success_inside_the_window_resets_the_permanent_failure_streak() {
        let telemetry = Telemetry::default();
        let queue = Arc::new(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone()));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (attempted_tx, mut attempted_rx) = mpsc::unbounded_channel();
        let output = ScriptedOutput {
            script: vec![Some(Fault::Permanent), None, Some(Fault::Permanent)],
            index: 0,
            attempted: attempted_tx,
        };

        queue.push(one_event_batch(1.0)).await; // attempt 1: Permanent -- sets streak_since

        let queue_for_task = Arc::clone(&queue);
        let handle = tokio::spawn(async move {
            let mut output = output;
            write_loop(
                "out".to_string(),
                &mut output,
                queue_for_task,
                telemetry,
                WriteLoopConfig::default(),
                shutdown_rx,
            )
            .await
        });
        attempted_rx.recv().await.expect("attempt 1 (failing) should have happened");

        // Well past where the window would trip -- but the *next* batch succeeds, which must
        // reset the streak before the window is ever checked again.
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW * 2).await;
        queue.push(one_event_batch(2.0)).await; // attempt 2: success -- resets the streak
        attempted_rx.recv().await.expect("attempt 2 (succeeding) should have happened");

        queue.push(one_event_batch(3.0)).await; // attempt 3: Permanent again -- a fresh streak
        attempted_rx.recv().await.expect("attempt 3 (failing again) should have happened");
        queue.close();

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

    /// A sink whose `send` always fails with no `Fault` attached at all -- e.g. `StdioOutput`'s
    /// bare I/O errors. `classify` still defaults this to `Permanent` for retry purposes (never
    /// retry an error the sink didn't recognize), but it must never be mistaken for a positively
    /// identified configuration error that should end the process.
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

    /// The review finding this guards: an unclassified error defaults to non-retryable (correct),
    /// but that must not also make it count toward the sustained-permanent-failure exit window --
    /// a sink that never opted into `Fault` classification at all failing forever is a very
    /// different situation from `InfluxDbOutput` explicitly identifying a bad token forever, and
    /// only the latter should ever end the process.
    #[tokio::test(start_paused = true)]
    async fn an_unclassified_error_never_trips_the_permanent_failure_window() {
        let telemetry = Telemetry::default();
        let queue = Arc::new(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone()));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (attempted_tx, mut attempted_rx) = mpsc::unbounded_channel();
        let output = AlwaysUnclassifiedFailure { attempted: attempted_tx };

        queue.push(one_event_batch(1.0)).await;
        let queue_for_task = Arc::clone(&queue);
        let handle = tokio::spawn(async move {
            let mut output = output;
            write_loop(
                "out".to_string(),
                &mut output,
                queue_for_task,
                telemetry,
                WriteLoopConfig::default(),
                shutdown_rx,
            )
            .await
        });
        attempted_rx.recv().await.expect("the first attempt should have happened");

        // Well past the window, with nothing but this unclassified failure the whole time.
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW * 2).await;
        queue.push(one_event_batch(2.0)).await;
        attempted_rx.recv().await.expect("a later attempt should have happened");
        queue.close();

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

    /// A sink whose `Fault` depends on the batch's own content (its single counter's value) --
    /// deterministic across however many retries a single batch takes, unlike `ScriptedOutput`
    /// (whose script advances per *call*, not per logical batch, so it can't represent "retry
    /// this one batch several times with the same fault" at all).
    struct FaultByBatchValue {
        attempted: mpsc::UnboundedSender<()>,
    }

    #[async_trait::async_trait]
    impl Output for FaultByBatchValue {
        async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
            let _ = self.attempted.send(());
            let value = match &batch.events[0].metrics[0].kind {
                MetricKind::Counter(v) => *v,
                other => panic!("expected Counter, got {other:?}"),
            };
            let fault = if value == 2.0 { Fault::Ambiguous } else { Fault::Permanent };
            Err(anyhow::anyhow!("simulated failure for batch {value}")).context(fault)
        }

        fn duplicate_safe(&self) -> bool {
            true // AtLeastOnce, so Ambiguous is retryable at all.
        }
    }

    /// The other half of the same review finding: a budget-exhausted `Ambiguous` drop is a
    /// different failure mode than an explicit configuration error (a destination that's merely
    /// slow/down, not misconfigured) and must reset the permanent-failure streak exactly like a
    /// success would -- not merely leave it untouched. `Permanent`, then `Ambiguous` (retried,
    /// budget exhausted, dropped) for well past the window, then one more isolated `Permanent` --
    /// if the reset hadn't happened, that third failure would find `permanent_streak_since`
    /// already far in the past and trip `Err` immediately; it must not.
    #[tokio::test(start_paused = true)]
    async fn a_budget_exhausted_ambiguous_drop_resets_the_permanent_failure_streak_like_success_does(
    ) {
        let telemetry = Telemetry::default();
        let queue = Arc::new(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone()));
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let (attempted_tx, mut attempted_rx) = mpsc::unbounded_channel();
        let output = FaultByBatchValue { attempted: attempted_tx };
        // A short retry budget so batch 2's Ambiguous failures exhaust quickly rather than
        // actually taking PERMANENT_FAILURE_WINDOW of real retrying.
        let write_config = WriteLoopConfig {
            retry: RetryConfig {
                total_budget: Duration::from_millis(50),
                base_delay: Duration::from_millis(10),
                max_delay: Duration::from_millis(10),
            },
            ..WriteLoopConfig::default()
        };

        queue.push(one_event_batch(1.0)).await; // Permanent -- sets streak_since
        let queue_for_task = Arc::clone(&queue);
        let handle = tokio::spawn(async move {
            let mut output = output;
            write_loop(
                "out".to_string(),
                &mut output,
                queue_for_task,
                telemetry,
                write_config,
                shutdown_rx,
            )
            .await
        });
        attempted_rx.recv().await.expect("attempt 1 (Permanent) should have happened");

        // Well past where the *original* streak would have tripped -- batch 2 (Ambiguous)
        // retries within its short (50ms) budget, exhausts it, and gets dropped; that drop must
        // reset the streak despite happening long after streak_since was first set. A 200ms sleep
        // (comfortably longer than the 50ms budget, and free under start_paused) guarantees the
        // retry loop has already given up and committed batch 2 before batch 3 is pushed --
        // simpler and less brittle than trying to count exactly how many retries it took.
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW * 2).await;
        queue.push(one_event_batch(2.0)).await;
        attempted_rx.recv().await.expect("batch 2's first attempt should have happened");
        tokio::time::sleep(Duration::from_millis(200)).await;

        queue.push(one_event_batch(3.0)).await; // Permanent again -- must be a fresh streak
        attempted_rx.recv().await.expect("attempt 3 (Permanent) should have happened");
        queue.close();

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

    /// Shutdown grace: a sink that's permanently stuck retrying (a `Clean` fault, retried
    /// indefinitely under a huge `total_budget`) still causes `write_loop` to return within
    /// `shutdown_grace` once the shutdown signal fires -- counting the drop with
    /// `reason="shutdown"` and calling `Output::flush`.
    #[tokio::test(start_paused = true)]
    async fn shutdown_grace_expiry_ends_write_loop_promptly_leaving_the_remainder_for_run_output() {
        let (mut output, _handles) = faulty_output(Fault::Clean, u32::MAX, false);

        let telemetry = Telemetry::default();
        let queue = Arc::new(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone()));
        queue.push(one_event_batch(1.0)).await;
        // Deliberately left open (not closed) -- shutdown grace must cut delivery off even while
        // the queue could still receive more, not just once it's known to be exhausted.

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
        let queue_for_task = Arc::clone(&queue);
        let handle = tokio::spawn(async move {
            write_loop(
                "out".to_string(),
                &mut output,
                queue_for_task,
                telemetry,
                write_config,
                shutdown_rx,
            )
            .await
        });

        // Let a couple of retry attempts actually happen before shutdown fires.
        tokio::time::sleep(Duration::from_millis(90)).await;
        shutdown_tx.send(true).expect("receiver should still be alive");

        let result = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("write_loop should return within shutdown_grace, not hang")
            .expect("the task should not panic");
        assert!(result.is_ok(), "shutdown-grace expiry should end write_loop with Ok, not Err");

        // write_loop no longer drains or flushes on shutdown-grace expiry itself -- that's
        // run_output's job (see finish_and_flush's doc comment for why it must happen there, not
        // here), exercised end to end by
        // `run_output_flushes_exactly_once_and_never_loses_a_batch_racing_shutdown_grace` below.
        // Confirm the batch is still exactly where write_loop left it: untouched, not silently
        // dropped by write_loop itself.
        assert!(
            queue.commit().is_some(),
            "write_loop must leave the undelivered batch for run_output to account for, not drop it silently itself"
        );
    }

    /// The exact race a review finding named: `finish_and_flush` must run only after `drain_inbox`
    /// can no longer push anything new, or a batch that lands in the gap between "queue observed
    /// empty" and "flush called" is silently lost -- no delivery, no `reason=shutdown` accounting.
    /// Drives `run_output` directly against a hand-fed `Delivered` channel (bypassing any real
    /// `Input`/listener) specifically so the producer side survives past the moment shutdown
    /// fires -- a real listener's task is cancelled by `run_input`'s own shutdown race almost
    /// immediately (see `run_with_telemetry_returns_the_first_failure_not_a_later_cascading_one`'s
    /// doc comment for the same lesson learned elsewhere), which would close `drain_inbox`'s inbox
    /// well before `write_loop`'s shutdown-grace timer ever expires, and this race needs
    /// `drain_inbox` to still be pushable exactly when grace expires.
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
            SinkQueueConfig::default(),
            write_config,
            shutdown_rx,
        ));

        // One batch, permanently failing to send -- write_loop will be mid-retry when shutdown
        // fires.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    events: vec![counter_event("hits", 1.0)],
                },
                TraceContext::new_root(),
            ))
            .await
            .expect("receiver should still be alive");
        handles.attempted.recv().await.expect("the first attempt should have happened");

        shutdown_tx.send(true).expect("receiver should still be alive");
        // Right at the shutdown-grace boundary: this batch's push races finish_and_flush's final
        // drain. Before the fix, a push landing here could slip past the snapshot and be dropped
        // when run_output returned, uncounted. `inbox_tx` is still held by this test (not by any
        // cancelled listener task), so this send is exactly the race the fix closes.
        tokio::time::sleep(Duration::from_millis(100)).await;
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    events: vec![counter_event("hits", 2.0)],
                },
                TraceContext::new_root(),
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

    /// A review finding, adjacent to the flush/shutdown race the test above closes: when
    /// `write_loop` gives up first (shutdown-grace expiry here; a permanent-failure-window trip
    /// is the other way this happens) while `drain_inbox` is still mid-flight, `run_output`'s
    /// `select!` drops the abandoned `drain` future -- and `drain_inbox` used to *own* its
    /// `inbox: mpsc::Receiver`, so dropping it also destroyed the channel, silently discarding
    /// whatever was still sitting in its buffer (accepted by `send`, never yet `recv()`-ed) with
    /// no `batches.dropped` count and no diagnostic, unlike every other drop path this workstream
    /// instruments. `drain_inbox` now borrows `inbox` (`&mut`) instead of owning it, so
    /// `run_output` can retain it, sweep whatever `drain_inbox` never got around to, and count it.
    ///
    /// Setup: `max_batches: 1` under `Block` means batch 1 fills the queue and is immediately
    /// `peek()`-reserved by `write_loop`'s endless-failure retry loop. Batch 2, sent right behind
    /// it, is pulled off the channel by `drain_inbox` but then blocks forever inside
    /// `queue.push()` -- a separate, narrower gap this fix does not close, since that batch is
    /// already out of the channel and held in the abandoned future's own suspended stack by the
    /// time `drain` is dropped (see this test's final assertion, which documents that residual
    /// loss rather than papering over it). Batch 3, sent only once `drain_inbox` is confirmed
    /// stuck on batch 2's push, can only ever sit in `inbox`'s own buffer, genuinely
    /// un-`recv()`-ed -- exactly the case this fix closes.
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
        let queue_config = SinkQueueConfig {
            max_batches: 1,
            max_bytes: u64::MAX,
            overflow: OverflowPolicy::Block,
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let registry = Registry::new();
        let telemetry = registry.telemetry_for("out", "influxdb_out", "sink");

        let run = tokio::spawn(run_output(
            "out".to_string(),
            Box::new(output),
            inbox_rx,
            telemetry,
            queue_config,
            write_config,
            shutdown_rx,
        ));

        // Batch 1: drained into the queue (filling its one slot), then peeked -- and so
        // reserved -- by write_loop's endless-failure retry loop.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    events: vec![counter_event("hits", 1.0)],
                },
                TraceContext::new_root(),
            ))
            .await
            .expect("receiver should still be alive");
        handles.attempted.recv().await.expect("the first attempt should have happened");

        // Batch 2: drain_inbox receives it, then blocks forever inside `queue.push` -- the queue
        // is full and its one slot is reserved, so there is nothing a concurrent commit could
        // ever free.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    events: vec![counter_event("hits", 2.0)],
                },
                TraceContext::new_root(),
            ))
            .await
            .expect("receiver should still be alive");
        // Let drain_inbox actually reach and block on that push before sending batch 3 -- if
        // batch 3 raced ahead of batch 2, it could be the one that gets stuck instead, proving
        // nothing about the case this fix targets.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Batch 3: drain_inbox is already stuck on batch 2's push, so this one can only ever sit
        // in `inbox`'s own channel buffer, genuinely un-`recv()`-ed -- exactly the case this fix
        // closes.
        inbox_tx
            .send(Delivered::Owned(
                EventBatch {
                    resource: Arc::new(Resource::default()),
                    events: vec![counter_event("hits", 3.0)],
                },
                TraceContext::new_root(),
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
                MetricKind::Counter(v) => Some(*v),
                _ => None,
            })
            .sum();

        assert_eq!(
            dropped_for_shutdown, 2.0,
            "batch 1 (left reserved in the queue) and batch 3 (left in the inbox, never drained \
             into the queue at all) must both be counted as dropped -- before this fix, batch 3 \
             would vanish uncounted, leaving this at 1.0 instead of 2.0. (Batch 2, stuck inside \
             an abandoned in-flight `queue.push()`, is a separate, narrower residual gap this fix \
             does not close, and is deliberately not counted here either.)"
        );
    }

    // -----------------------------------------------------------------------------------------
    // `run_with_telemetry`'s join loop: drain every task on the first error instead of aborting
    // (`docs/plans/0004-buffered-sink-delivery.md` workstream E,
    // `docs/adr/0021-buffered-sink-delivery.md`)
    // -----------------------------------------------------------------------------------------

    /// An `Input` fully driven by the test: every batch handed to the paired `UnboundedSender`
    /// is forwarded to `sink` as soon as it arrives, in order -- unlike `BurstInput`/
    /// `FiniteBurstInput` above (whose batch list is fixed at construction time), this lets a
    /// test stagger exactly when each batch reaches a component. The join-loop tests below need
    /// that control to trigger `write_loop`'s permanent-failure-window trip at a precise instant
    /// rather than all at once. Ends (closing its `Fanout`, and therefore its consumer's inbox,
    /// exactly like `FiniteInput`) once the test drops its sender -- or, same as every other
    /// `Input` here, once `run_input`'s own race against the shutdown signal picks the shutdown
    /// branch first.
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

    /// The property this workstream exists to deliver: `run_with_telemetry`'s join loop no
    /// longer aborts every other task the instant one task fails. Two independent sinks, each
    /// fed by its own live input: `bad` is a sink that trips `write_loop`'s sustained-permanent-
    /// failure-window path (`PERMANENT_FAILURE_WINDOW`, workstream D) and ends with `Err`; `good`
    /// is a healthy sink whose delivery is held shut (via a `Gate`) with several batches already
    /// sitting in its `SinkQueue`, still undelivered, at the exact moment `bad` fails. Under the
    /// old `break`-and-drop-the-`JoinSet` behavior, dropping the `JoinSet` at that instant would
    /// abort `good`'s task mid-drain, discarding those buffered batches outright. This test
    /// proves that no longer happens: only once `bad`'s failure has fired the shared shutdown
    /// signal does the test open `good`'s gate, and `good`'s already-queued batches are still
    /// delivered through the ordinary path before `run_with_telemetry` returns.
    #[tokio::test(start_paused = true)]
    async fn a_healthy_sinks_buffered_batches_are_still_delivered_after_a_sibling_sink_trips_the_permanent_failure_window(
    ) {
        let mut components = Map::new();
        components.insert(
            "bad_in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "bad".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["bad_in".to_string()],
                kind: influxdb_out(),
            },
        );
        components.insert(
            "good_in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "good".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["good_in".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let (bad_tx, bad_rx) = mpsc::unbounded_channel();
        let (good_tx, good_rx) = mpsc::unbounded_channel();
        let (bad_output, mut bad_handles) = faulty_output(Fault::Permanent, u32::MAX, false);
        let gate = Gate::new();
        let (delivered_tx, mut delivered_rx) = mpsc::unbounded_channel();

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert("bad_in".to_string(), NodeSpec::Input(Box::new(ChannelInput { rx: bad_rx })));
        specs.insert(
            "bad".to_string(),
            NodeSpec::Output(
                Box::new(bad_output),
                SinkQueueConfig::default(),
                WriteLoopConfig::default(),
            ),
        );
        specs
            .insert("good_in".to_string(), NodeSpec::Input(Box::new(ChannelInput { rx: good_rx })));
        specs.insert(
            "good".to_string(),
            NodeSpec::Output(
                Box::new(SlowOutput { gate: gate.clone(), delivered: delivered_tx }),
                SinkQueueConfig {
                    max_batches: 100,
                    max_bytes: u64::MAX,
                    overflow: OverflowPolicy::Block,
                },
                // Generous shutdown grace AND retry budget: this test deliberately holds "good"'s
                // gate shut past bad's own PERMANENT_FAILURE_WINDOW trip (60s+), and every attempt
                // is now raced against its own retry budget (`deliver_with_retry`'s "impossible to
                // ever fit" fix) -- a default 60s budget would time this send out as `Ambiguous`
                // before the gate ever opens, which is not what this test is proving. Neither
                // bound should matter to what's actually under test here.
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

        let run_task =
            tokio::spawn(run_with_telemetry(g, specs, telemetry, std::future::pending()));

        // Queue three batches on "good" while its delivery is gated shut -- they land in its
        // SinkQueue, undelivered, exactly the state a healthy sibling can be in when another
        // node fails.
        for i in 0..3 {
            good_tx
                .send(EventBatch {
                    resource: Arc::new(Resource::default()),
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

        // Trip "bad"'s sustained-permanent-failure-window path, ending its write_loop (and
        // therefore its task) with Err -- mirrors
        // `sustained_permanent_failures_end_write_loop_once_the_failure_window_elapses` above,
        // just driven through the full graph rather than against `write_loop` directly.
        bad_tx
            .send(EventBatch {
                resource: Arc::new(Resource::default()),
                events: vec![counter_event("hits", 1.0)],
            })
            .expect("bad_in's receiver should still be alive");
        bad_handles.attempted.recv().await.expect("bad's first attempt should have happened");
        tokio::time::sleep(PERMANENT_FAILURE_WINDOW).await;
        bad_tx
            .send(EventBatch {
                resource: Arc::new(Resource::default()),
                events: vec![counter_event("hits", 2.0)],
            })
            .expect("bad_in's receiver should still be alive");
        bad_handles
            .attempted
            .recv()
            .await
            .expect("bad's second (window-tripping) attempt should have happened");

        // A short (paused-clock) sleep, far shorter than "good"'s 3600s shutdown grace above, so
        // there's no risk of it accidentally expiring: this just gives every already-runnable
        // task (bad's task completing, run_with_telemetry's join loop observing that and firing
        // shutdown, bad_in/good_in reacting to shutdown) room to actually run before the test
        // moves on -- none of that needs any further time to elapse, just scheduling.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Only now open "good"'s gate -- proving its delivery wasn't already finished (and so
        // trivially safe from the old abort-on-first-error bug) before shutdown fired.
        gate.open();

        for i in 0..3 {
            let received = tokio::time::timeout(Duration::from_secs(5), delivered_rx.recv())
                .await
                .expect("good's already-queued batches should still be delivered, not aborted")
                .expect("the channel should not have closed");
            match &received.events[0].metrics[0].kind {
                MetricKind::Counter(v) => {
                    assert_eq!(*v, i as f64, "batches should still be delivered in order")
                }
                other => panic!("expected Counter, got {other:?}"),
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

    /// An `Output` whose `send` always fails `Fault::Permanent`, but delays before returning its
    /// *second* call's error by `delay` -- used to control exactly when a sink's
    /// sustained-permanent-failure-window trips (its first call sets `write_loop`'s streak clock,
    /// its second call's failure is what gets checked against it) without needing any `Input` to
    /// stay alive past its own immediate completion. The delay lives inside `Output::send` itself,
    /// which `write_loop`'s outer `shutdown_grace` select doesn't preempt -- so as long as a
    /// node's `shutdown_grace` is generous, this keeps working normally even after a sibling
    /// node's failure has already fired the shared shutdown signal.
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

    /// `run_with_telemetry`'s join loop keeps the *first* recorded error, not whichever task
    /// happens to finish last. Two independent sinks, `bad1`/`bad2`, each fed two batches
    /// up front by an input that finishes immediately on its own (so neither depends on
    /// surviving past the other's shutdown-triggering failure); each sink's own second `send`
    /// call is what trips its sustained-permanent-failure-window (`PERMANENT_FAILURE_WINDOW`,
    /// workstream D), and `DelayedSecondFailureOutput`'s internal delay is what controls *when*
    /// that happens -- `bad1`'s trips at exactly the window, `bad2`'s one second later -- so
    /// `bad1`'s task is guaranteed to complete (and be recorded by the join loop) strictly before
    /// `bad2`'s, deterministically, with no reliance on scheduling order between two
    /// simultaneously-ready tasks.
    #[tokio::test(start_paused = true)]
    async fn run_with_telemetry_returns_the_first_failure_not_a_later_cascading_one() {
        let mut components = Map::new();
        components.insert(
            "bad1_in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "bad1".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["bad1_in".to_string()],
                kind: influxdb_out(),
            },
        );
        components.insert(
            "bad2_in".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "bad2".to_string(),
            Component {
                buffer: logit_config::BufferConfig::default(),
                sources: vec!["bad2_in".to_string()],
                kind: influxdb_out(),
            },
        );
        let g = graph::resolve(Config { components }).expect("should resolve");

        let (bad1_tx, bad1_rx) = mpsc::unbounded_channel();
        let (bad2_tx, bad2_rx) = mpsc::unbounded_channel();

        // Generous shutdown grace on both -- this test wants each sink's own internal delay
        // (inside `DelayedSecondFailureOutput::send`) to run to completion normally, not get cut
        // short just because the *other* sink's failure already fired shutdown in the meantime.
        let generous_grace = WriteLoopConfig {
            retry: RetryConfig::default(),
            shutdown_grace: Duration::from_secs(3600),
            delivery_override: None,
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs
            .insert("bad1_in".to_string(), NodeSpec::Input(Box::new(ChannelInput { rx: bad1_rx })));
        specs.insert(
            "bad1".to_string(),
            NodeSpec::Output(
                Box::new(DelayedSecondFailureOutput {
                    delay: PERMANENT_FAILURE_WINDOW,
                    calls: Arc::new(std::sync::atomic::AtomicU32::new(0)),
                }),
                SinkQueueConfig::default(),
                generous_grace,
            ),
        );
        specs
            .insert("bad2_in".to_string(), NodeSpec::Input(Box::new(ChannelInput { rx: bad2_rx })));
        specs.insert(
            "bad2".to_string(),
            NodeSpec::Output(
                Box::new(DelayedSecondFailureOutput {
                    delay: PERMANENT_FAILURE_WINDOW + Duration::from_secs(1),
                    calls: Arc::new(std::sync::atomic::AtomicU32::new(0)),
                }),
                SinkQueueConfig::default(),
                generous_grace,
            ),
        );

        // Both batches for both sinks, sent and the senders dropped immediately -- each
        // `ChannelInput` sees its channel close right away and finishes on its own (`Ok(())`)
        // with no dependency on shutdown timing at all.
        for tx in [&bad1_tx, &bad2_tx] {
            for i in 0..2 {
                tx.send(EventBatch {
                    resource: Arc::new(Resource::default()),
                    events: vec![counter_event("hits", i as f64)],
                })
                .expect("receiver should still be alive");
            }
        }
        drop(bad1_tx);
        drop(bad2_tx);

        let run_task = tokio::spawn(run(g, specs));

        // Paused clock: both sinks' internal delays (60s and 61s) elapse in virtual time while
        // this just waits for the task to actually finish -- no real wall-clock cost, but the
        // timeout itself must still exceed both delays or it trips first against the same
        // virtual clock.
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

    /// `run_transform`'s non-flush path (`MutatingTransform`, which always returns `Some`) reads
    /// the incoming batch's `TraceContext` and sends its own emission as a
    /// [`TraceContext::child`] of it -- the propagation this PR's "Inherited context" follow-up
    /// implements for the two straightforward node kinds. Tests `run_transform` directly (a
    /// private fn in this module, not through the full graph) because nothing surfaces a
    /// propagated context past this crate yet: `Output::send` still takes `&EventBatch`, not
    /// `&Delivered`, so an end-to-end test through `run` has nowhere to observe it -- see
    /// `docs/design/pipeline-graph.md`'s "Trace context propagation" section.
    #[tokio::test]
    async fn run_transform_propagates_the_incoming_context_as_a_child() {
        let (in_tx, in_rx) = mpsc::channel(1);
        let (out_tx, mut out_rx) = mpsc::channel(1);
        let fanout = Fanout::new(vec![out_tx]);
        let transform: Box<dyn Transform + Send> = Box::new(MutatingTransform);

        let parent = TraceContext::new_root();
        let batch = EventBatch {
            resource: Arc::new(Resource::default()),
            events: vec![counter_event("hits", 1.0)],
        };
        in_tx.send(Delivered::Owned(batch, parent)).await.expect("inbox should accept");
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

    /// The other half of the same story: `run_transform`'s flush path (`WindowingTransform`,
    /// which only ever emits from `flush`, never `process`) has no single incoming batch to
    /// attribute a close-time flush to -- deliberately mints a fresh root rather than picking one
    /// of however many batches it absorbed, per `TraceContext`'s own doc comment
    /// (`crates/logit-pipeline/src/fanout.rs`) and `docs/known-gaps.md`'s internal-spans entry.
    /// Two absorbed batches on two different traces prove the point directly: neither survives
    /// into the flushed context.
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
            events: vec![counter_event("hits", 1.0)],
        };
        in_tx.send(Delivered::Owned(batch(), ctx_a)).await.expect("inbox should accept");
        in_tx.send(Delivered::Owned(batch(), ctx_b)).await.expect("inbox should accept");
        drop(in_tx); // close the inbox -- triggers the close-time flush that emits both, absorbed

        run_transform(transform, in_rx, fanout, Telemetry::default())
            .await
            .expect("should complete without error");

        let flushed = out_rx.recv().await.expect("the close-time flush should emit").context();
        assert_ne!(flushed.trace_id, ctx_a.trace_id);
        assert_ne!(flushed.trace_id, ctx_b.trace_id);
    }
}
