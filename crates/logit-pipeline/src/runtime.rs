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
use crate::{Fanout, Input, Output, Transform};
use anyhow::Context;
use logit_core::{EventBatch, Resource, Telemetry};
use logit_script::{ProcessOutcome, ScriptWorker};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinSet;

/// Bounded channel capacity between two graph nodes. Small and arbitrary -- just enough to smooth
/// out bursts without unbounded memory growth; revisit with real numbers once there's a reason to.
const CHANNEL_CAPACITY: usize = 64;

/// One component's built implementation, keyed by id and handed to [`run`]. Which variant a
/// `ComponentKind` becomes is the registry's job (`logit-cli`), not this crate's -- this crate
/// only knows how to *run* each variant once built.
pub enum NodeSpec {
    Input(Box<dyn Input + Send>),
    Output(Box<dyn Output + Send>),
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
    let shutdown_driver = tokio::spawn(async move {
        shutdown.await;
        let _ = shutdown_tx.send(true);
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
            NodeSpec::Output(output) => {
                tasks.spawn(run_output(id, output, inbox, node_telemetry));
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

    let mut result: anyhow::Result<()> = Ok(());
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                result = Err(err);
                break;
            }
            Err(join_err) => {
                result = Err(join_err.into());
                break;
            }
        }
    }
    // Whether `shutdown` ever resolved or not (in `run`'s case, it's `pending()` and never will),
    // this task has nothing left to do once every node has exited -- abort rather than leave it
    // parked forever holding `shutdown_tx`.
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

/// `Output::send` takes `&EventBatch` (`docs/adr/0016-arc-eventbatch-copy-on-write.md`), so this is
/// the one node kind that never needs [`unwrap_batch`] at all: a `Delivered::Owned` batch is
/// already there to borrow, and a `Delivered::Shared(Arc<EventBatch>)` is borrowed straight through
/// the `Arc` (`&Arc<EventBatch>` derefs to `&EventBatch`) with no `Arc::try_unwrap`, no clone, ever
/// -- regardless of how many sibling branches still hold their own handle to the same batch. This
/// is what actually delivers the "every read-only sink branch pays one atomic, never a clone"
/// saving `docs/design/memory.md` §8 item 4 originally recommended; `run_transform`/`run_lua` below
/// still call `unwrap_batch`, because `Transform::process`/`ScriptWorker::process` need an owned
/// `Event` to mutate or consume.
async fn run_output(
    id: String,
    mut output: Box<dyn Output + Send>,
    mut inbox: mpsc::Receiver<Delivered>,
    telemetry: Telemetry,
) -> anyhow::Result<()> {
    while let Some(delivered) = inbox.recv().await {
        send_batch(&id, &mut *output, &delivered, &telemetry).await?;
    }
    Ok(())
}

/// The per-batch body of `run_output`'s loop above: telemetry accounting plus one `Output::send`
/// call. Factored out (rather than left inline) for the same reason [`process_batch`] below is --
/// so it can be measured directly in `crates/logit-bench/tests/allocations.rs`/`benches/pipeline.rs`,
/// with no channel or the rest of the node runtime involved. Unlike `process_batch` this stays
/// `async`, because `Output::send` itself is; call it from a `current_thread` runtime with no
/// `tokio::spawn` to keep it measurable the same way `fanout.rs`'s own tests already are.
/// `run_output` is the only caller in this crate; `pub` is for the bench/test.
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
    for (resource, events) in flushed {
        if !events.is_empty() {
            telemetry.count("logit.component.flush.events", events.len() as f64, &[]);
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
/// it's still genuinely racy, just against a different clock.** `run_output` (above) drops its own
/// `Delivered` the moment `output.send` returns, immediately before its next `inbox.recv().await`
/// -- it does not hold the handle for the rest of its loop, and it never itself calls
/// `try_unwrap`. So whether *this* function's unwrap succeeds for a `Transform`/Lua sibling comes
/// down to real tokio scheduling: whichever happens first, `Output`'s task completing its `send`
/// and dropping its handle, or this call actually running. If `Output` finishes first, this
/// succeeds for free (1 allocation total for the whole fan-out send -- see
/// `fanout_send_mixed_output_and_transform_consumers_when_output_finishes_first`,
/// `crates/logit-bench/tests/allocations.rs`); if `Output` is still mid-`send` (plausible, even
/// likely, since `Output::send` typically does real I/O against a network or file, which tends to
/// be slower than a `Transform`'s local processing), this fails and clones (`Delivered::Owned`
/// and `Delivered::Shared`'s handling is the same as it would be with no `Output` sibling at all --
/// see `fanout_send_mixed_output_and_transform_consumers`, same file). Either outcome keeps
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
    use logit_config::{Component, ComponentKind, Config};
    use logit_core::{Event, MetricKind, Registry};
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
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "enrich".to_string(),
            Component {
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
            NodeSpec::Output(Box::new(RecordingOutput { tx: result_tx })),
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
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
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
            NodeSpec::Output(Box::new(RecordingOutput { tx: result_tx })),
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
                sources: vec!["in".to_string()],
                kind: ComponentKind::Json { skip_to_brace: false },
            },
        );
        components.insert(
            "sink_a".to_string(),
            Component { sources: vec!["branch_a".to_string()], kind: influxdb_out() },
        );
        components.insert(
            "sink_b".to_string(),
            Component { sources: vec!["in".to_string()], kind: influxdb_out() },
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
        specs
            .insert("sink_a".to_string(), NodeSpec::Output(Box::new(RecordingOutput { tx: tx_a })));
        specs
            .insert("sink_b".to_string(), NodeSpec::Output(Box::new(RecordingOutput { tx: tx_b })));

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

        fn flush(&mut self, _now: i64) -> Vec<(Arc<Resource>, Vec<Event>)> {
            if self.buffered.is_empty() {
                return Vec::new();
            }
            vec![(Arc::new(Resource::default()), std::mem::take(&mut self.buffered))]
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
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "windowed".to_string(),
            Component {
                sources: vec!["in".to_string()],
                kind: ComponentKind::Aggregate { interval: Duration::from_secs(3600) },
            },
        );
        components.insert(
            "out".to_string(),
            Component {
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
            NodeSpec::Output(Box::new(RecordingOutput { tx: result_tx })),
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
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "xform".to_string(),
            Component {
                sources: vec!["in".to_string()],
                kind: ComponentKind::Json { skip_to_brace: false },
            },
        );
        components.insert(
            "out".to_string(),
            Component { sources: vec!["xform".to_string()], kind: influxdb_out() },
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
            NodeSpec::Output(Box::new(RecordingOutput { tx: result_tx })),
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
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "enrich".to_string(),
            Component {
                sources: vec!["in".to_string()],
                kind: ComponentKind::Lua {
                    script: "function process(event) return {event, event:clone()} end".to_string(),
                    interval: None,
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component { sources: vec!["enrich".to_string()], kind: influxdb_out() },
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
            NodeSpec::Output(Box::new(RecordingOutput { tx: result_tx })),
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
                sources: vec![],
                kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
            },
        );
        components.insert(
            "windowed".to_string(),
            Component {
                sources: vec!["in".to_string()],
                kind: ComponentKind::Lua {
                    script: "function process(event) return event end".to_string(),
                    interval: Some(Duration::from_secs(3600)),
                },
            },
        );
        components.insert(
            "out".to_string(),
            Component { sources: vec!["windowed".to_string()], kind: influxdb_out() },
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
            NodeSpec::Output(Box::new(RecordingOutput { tx: result_tx })),
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
