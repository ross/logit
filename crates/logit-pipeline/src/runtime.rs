//! The node runtime: turns a resolved [`Graph`] plus one built implementation per component
//! (a [`NodeSpec`]) into running tasks/threads, wired together with per-component [`Fanout`]s.
//! See `docs/design/pipeline-graph.md`'s "Runtime model" and "Thread model" sections.
//!
//! Every component gets one inbox channel, created up front for the whole graph before any node
//! is spawned -- unlike the design doc's "build in reverse topological order" framing, this
//! doesn't actually need dependency ordering: a `Fanout` is just cloned `Sender`s into inboxes
//! that already exist by construction, regardless of which node gets spawned first.

use crate::graph::Graph;
use crate::{Fanout, Input, Output, Transform};
use anyhow::Context;
use logit_core::{EventBatch, Resource};
use logit_script::{ProcessOutcome, ScriptWorker};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
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
/// one fails.
pub async fn run(graph: Graph, mut specs: HashMap<String, NodeSpec>) -> anyhow::Result<()> {
    let ids: Vec<String> = graph.components.keys().cloned().collect();

    let mut senders: HashMap<String, mpsc::Sender<EventBatch>> = HashMap::with_capacity(ids.len());
    let mut inboxes: HashMap<String, mpsc::Receiver<EventBatch>> =
        HashMap::with_capacity(ids.len());
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
        let fanout = Fanout::new(component.consumers.iter().map(|c| senders[c].clone()).collect());
        let inbox = inboxes.remove(&id).expect("an inbox was created for every id above");
        let spec = specs
            .remove(&id)
            .with_context(|| format!("no implementation registered for component '{id}'"))?;

        match spec {
            NodeSpec::Input(input) => {
                // A listener's own inbox is never written to (arity rule: a listener has no
                // sources, so nothing ever names it as a source and sends into it) -- nothing
                // reads it either.
                drop(inbox);
                tasks.spawn(run_input(id, input, fanout));
            }
            NodeSpec::Output(output) => {
                tasks.spawn(run_output(id, output, inbox));
            }
            NodeSpec::Transform(transform) => {
                tasks.spawn(run_transform(transform, inbox, fanout));
            }
            NodeSpec::Lua { script, interval } => {
                let (ready_tx, ready_rx) = oneshot::channel::<Result<(), String>>();
                let handle = runtime_handle.clone();
                let thread_id = id.clone();
                std::thread::Builder::new()
                    .name(format!("logit-{id}"))
                    .spawn(move || {
                        run_lua(thread_id, script, interval, ready_tx, inbox, fanout, handle)
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

    while let Some(result) = tasks.join_next().await {
        result??;
    }
    Ok(())
}

async fn run_input(
    id: String,
    mut input: Box<dyn Input + Send>,
    fanout: Fanout,
) -> anyhow::Result<()> {
    input.run(fanout).await.with_context(|| format!("component '{id}'"))
}

async fn run_output(
    id: String,
    mut output: Box<dyn Output + Send>,
    mut inbox: mpsc::Receiver<EventBatch>,
) -> anyhow::Result<()> {
    while let Some(batch) = inbox.recv().await {
        output.send(batch).await.with_context(|| format!("component '{id}'"))?;
    }
    Ok(())
}

/// A `Transform`-trait node's loop: races its inbox against its own flush deadline (if it has
/// one), exactly the shape `run_lua` below uses for a Lua node with `interval` set -- but as a
/// plain tokio task, this one can use `tokio::time::timeout` directly with no `Handle::block_on`
/// indirection, since it's already running inside the async runtime.
async fn run_transform(
    mut transform: Box<dyn Transform + Send>,
    mut inbox: mpsc::Receiver<EventBatch>,
    fanout: Fanout,
) -> anyhow::Result<()> {
    let mut next_flush =
        transform.flush_interval().map(|interval| tokio::time::Instant::now() + interval);

    loop {
        if let Some(deadline) = next_flush {
            let now_instant = tokio::time::Instant::now();
            if deadline <= now_instant {
                for (resource, events) in transform.flush(now_unix_nanos()) {
                    if !events.is_empty() {
                        fanout.send(EventBatch { resource, events }).await;
                    }
                }
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
                for (resource, events) in transform.flush(now_unix_nanos()) {
                    if !events.is_empty() {
                        fanout.send(EventBatch { resource, events }).await;
                    }
                }
            }
            return Ok(());
        };

        let mut out = Vec::with_capacity(batch.events.len());
        for event in batch.events {
            if let Some(event) = transform.process(&batch.resource, event) {
                out.push(event);
            }
        }
        if !out.is_empty() {
            fanout.send(EventBatch { resource: batch.resource, events: out }).await;
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
fn run_lua(
    id: String,
    script: String,
    configured_interval: Option<Duration>,
    ready_tx: oneshot::Sender<Result<(), String>>,
    mut inbox: mpsc::Receiver<EventBatch>,
    fanout: Fanout,
    runtime: tokio::runtime::Handle,
) {
    let worker = match ScriptWorker::new(&script) {
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

    let flush_now =
        |worker: &ScriptWorker, resource: &Arc<Resource>, fanout: &Fanout| match worker.flush() {
            Ok(events) if !events.is_empty() => {
                fanout.send_blocking(EventBatch { resource: resource.clone(), events });
            }
            Ok(_) => {}
            Err(err) => eprintln!("component '{id}': script flush error: {err}"),
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
        last_resource = batch.resource.clone();

        let mut out = Vec::with_capacity(batch.events.len());
        for event in batch.events {
            match worker.process(event) {
                Ok(ProcessOutcome::Emit(e)) => out.push(*e),
                Ok(ProcessOutcome::EmitMany(es)) => out.extend(es),
                Ok(ProcessOutcome::Drop) => {}
                Err(err) => eprintln!("component '{id}': script error: {err}"),
            }
        }
        if !out.is_empty() {
            fanout.send_blocking(EventBatch { resource: batch.resource, events: out });
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
    use crate::graph;
    use logit_config::{Component, ComponentKind, Config};
    use logit_core::Event;
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
        async fn send(&mut self, batch: EventBatch) -> anyhow::Result<()> {
            let _ = self.tx.send(batch);
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
        use logit_core::{interner::intern, AttrMap, MetricKind, MetricRecord, Payload};
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
}
