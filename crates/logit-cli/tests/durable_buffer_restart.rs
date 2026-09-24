//! End to end, ADR `disk-backed-sink-buffer`'s core claim: a disk-backed sink buffer survives a
//! process death with no shutdown signal and, on restart, delivers every batch pushed before it,
//! in order, losing nothing. Aborting the whole `run` task stands in for `SIGKILL`. The graph and
//! its `NodeSpec`s are built through `logit_pipeline`'s public API rather than parsed from YAML,
//! because the sink under test has to fail on command, which no real `ComponentKind` can express.

use logit_config::{BufferConfig, Component, ComponentKind, Config, ReceiveConfig};
use logit_core::{AttrMap, Event, EventBatch, MetricKind, Registry, Resource, Telemetry, Value};
use logit_pipeline::graph;
use logit_pipeline::{
    DiskQueueConfig, Fanout, Input, InputRuntimeConfig, NodeSpec, Output, OverflowPolicy,
    Readiness, SinkStoreConfig, WriteLoopConfig, SINK_QUEUE_METRICS,
};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// A distinguishable batch: one log event whose message names its own position in the sequence,
/// so delivery order and duplication are both directly checkable from what a sink recorded.
fn batch(marker: usize) -> EventBatch {
    let mut attrs = AttrMap::new();
    attrs.insert("marker", Value::str(marker.to_string()));
    EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![Event::empty(marker as i64, attrs)],
    }
}

fn marker_of(batch: &EventBatch) -> usize {
    batch.events[0].attributes.get("marker").and_then(Value::as_str).unwrap().parse().unwrap()
}

/// Sends each of `batches` once, then hangs forever; the test aborts `run` rather than letting it
/// finish. `gap` is the sleep between sends: `Duration::ZERO` sends one burst, and a nonzero gap
/// lets delivery keep pace with ingest, which parks the reader at the end of the active segment
/// before each rotation (what
/// `a_disk_backed_sink_keeps_delivering_across_a_rotation_once_the_reader_has_caught_up` needs).
struct BurstThenHangInput {
    batches: Vec<EventBatch>,
    gap: Duration,
}

#[async_trait::async_trait]
impl Input for BurstThenHangInput {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        for batch in self.batches.drain(..) {
            sink.send(batch).await;
            if self.gap > Duration::ZERO {
                tokio::time::sleep(self.gap).await;
            }
        }
        std::future::pending::<()>().await;
        unreachable!("pending() never resolves")
    }
}

/// Attaches `Fault::Clean`, as every real sink's `send` does
/// (`docs/adr/buffered-sink-delivery.md`). A `Clean` fault is retried under every delivery posture
/// with no duplicate risk, since the destination never saw the batch.
trait ContextFault<T> {
    fn context_fault(self) -> anyhow::Result<T>;
}
impl<T> ContextFault<T> for anyhow::Result<T> {
    fn context_fault(self) -> anyhow::Result<T> {
        use anyhow::Context;
        self.context(logit_pipeline::Fault::Clean)
    }
}

/// Records every attempt as `(marker, succeeded)`. The first `succeed_first_n_attempts` calls
/// succeed and every later one fails. `write_loop` is single-in-flight, so while earlier calls
/// succeed the Nth call is batch N's only attempt: one instance simulates some batches delivered,
/// the next stuck retrying, and the rest never attempted.
struct RecordingOutput {
    attempts: Arc<Mutex<Vec<(usize, bool)>>>,
    attempt_count: Arc<AtomicU64>,
    succeed_first_n_attempts: u64,
}

#[async_trait::async_trait]
impl Output for RecordingOutput {
    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let marker = marker_of(batch);
        let idx = self.attempt_count.fetch_add(1, Ordering::SeqCst);
        let ok = idx < self.succeed_first_n_attempts;
        self.attempts.lock().unwrap().push((marker, ok));
        if ok {
            Ok(())
        } else {
            Err(anyhow::anyhow!("simulated failure")).context_fault()
        }
    }

    fn duplicate_safe(&self) -> bool {
        // At-least-once: the posture that retries and redelivers, which surviving a restart needs.
        true
    }
}

/// The most recent `Gauge` point named `name` in one drain's worth of telemetry events -- a gauge
/// is last-write-wins within a drain (`Telemetry::gauge`), so each drain carries at most one.
fn latest_gauge(events: &[Event], name: &str) -> Option<f64> {
    events.iter().rev().find_map(|e| {
        e.metrics.iter().rev().find_map(|m| match &m.kind {
            MetricKind::Gauge(v) if logit_core::interner::resolve(m.name) == name => Some(*v),
            _ => None,
        })
    })
}

fn graph_and_topology(disk_dir: std::path::PathBuf) -> (graph::Graph, DiskQueueConfig) {
    let mut components = HashMap::new();
    components.insert(
        "in".to_string(),
        Component {
            buffer: BufferConfig::default(),
            receive: ReceiveConfig::default(),
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
            buffer: BufferConfig::default(),
            receive: ReceiveConfig::default(),
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
    let graph = graph::resolve(Config { components, ..Default::default() })
        .expect("topology should resolve");
    let disk_config = DiskQueueConfig {
        dir: disk_dir,
        max_bytes: 64 * 1024 * 1024,
        segment_bytes: 1024, // small: forces real rotation across a handful of tiny batches
        overflow: OverflowPolicy::Block,
        compression: logit_proto::frame::Compression::None,
        checkpoint_interval: Duration::from_millis(20),
    };
    (graph, disk_config)
}

#[tokio::test]
async fn a_disk_backed_sink_survives_a_simulated_sigkill_and_redelivers_only_what_it_must() {
    let dir = std::env::temp_dir().join(format!(
        "logit-durable-buffer-restart-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let spool_dir = dir.join("spool");

    const TOTAL: usize = 40;
    const SUCCEED_FIRST_RUN: u64 = 10;

    // --- Run 1: push everything, let the first 10 succeed and commit, then jam retrying the
    // 11th forever (a huge retry budget), and kill the whole runtime with no shutdown signal.
    let run1_attempts = Arc::new(Mutex::new(Vec::new()));
    {
        let (graph, disk_config) = graph_and_topology(spool_dir.clone());
        let batches: Vec<EventBatch> = (0..TOTAL).map(batch).collect();

        let output = RecordingOutput {
            attempts: Arc::clone(&run1_attempts),
            attempt_count: Arc::new(AtomicU64::new(0)),
            succeed_first_n_attempts: SUCCEED_FIRST_RUN,
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(BurstThenHangInput { batches, gap: Duration::ZERO }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(output),
                SinkStoreConfig::Disk(disk_config),
                WriteLoopConfig {
                    retry: logit_pipeline::RetryConfig {
                        total_budget: Duration::from_secs(3600),
                        base_delay: Duration::from_millis(5),
                        max_delay: Duration::from_millis(5),
                    },
                    ..WriteLoopConfig::default()
                },
            ),
        );

        // No shutdown signal: aborting this task drops the whole `run` future, taking every node
        // task and its `DiskQueue` down mid-flight, so nothing runs `finish()`.
        //
        // When to kill is decided by observation, not a clock. A batch is durable only once
        // `DiskQueue::push` has written it, so "every batch was pushed before the crash" has to be
        // established: on a loaded CI disk, where each segment rotation `fsync`s twice, 40 pushes
        // don't always fit a fixed wait. The sink's `logit.component.buffer.batches` gauge
        // (`SINK_QUEUE_METRICS.depth`, pushed minus committed) reading `TOTAL - SUCCEED_FIRST_RUN`
        // says all 40 are on disk and the first 10 committed.
        let registry = Registry::new();
        let telemetry: HashMap<String, Telemetry> = HashMap::from([(
            "out".to_string(),
            registry.telemetry_for("out", "influxdb_out", "sink"),
        )]);
        let run = tokio::spawn(logit_pipeline::run_with_telemetry(
            graph,
            specs,
            telemetry,
            Readiness::disabled(),
            std::future::pending(),
        ));
        let expected_depth = (TOTAL as u64 - SUCCEED_FIRST_RUN) as f64;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        let mut depth = None;
        loop {
            // `drain` consumes the pending points, and a gauge is last-write-wins per drain, so
            // the most recent drain that carried one holds the current value.
            if let Some(v) = latest_gauge(&registry.drain(0), SINK_QUEUE_METRICS.depth) {
                depth = Some(v);
            }
            let jammed = run1_attempts
                .lock()
                .unwrap()
                .iter()
                .any(|(marker, ok)| *marker == SUCCEED_FIRST_RUN as usize && !*ok);
            if (depth == Some(expected_depth) && jammed) || tokio::time::Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            depth,
            Some(expected_depth),
            "all {TOTAL} batches should be spooled ({SUCCEED_FIRST_RUN} of them committed) before \
             the kill"
        );
        assert!(!run.is_finished(), "run should still be going (in should be hanging) when killed");
        run.abort();
        // Let the abort land before reopening the spool: the aborted task still holds
        // the `DiskQueue` (and its exclusive lock file) until its future is dropped.
        let joined = tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("the aborted run should be torn down promptly");
        assert!(
            joined.is_err_and(|err| err.is_cancelled()),
            "run should have been aborted, not finished on its own"
        );
    }
    let run1_successes: Vec<usize> = run1_attempts
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, ok)| *ok)
        .map(|(marker, _)| *marker)
        .collect();
    assert_eq!(
        run1_successes,
        (0..SUCCEED_FIRST_RUN as usize).collect::<Vec<_>>(),
        "the first {SUCCEED_FIRST_RUN} batches should have succeeded, in order, before the kill"
    );

    // --- Run 2: a fresh graph over the same spool directory, this time always succeeding.
    let run2_attempts = Arc::new(Mutex::new(Vec::new()));
    {
        let (graph, disk_config) = graph_and_topology(spool_dir.clone());
        let output = RecordingOutput {
            attempts: Arc::clone(&run2_attempts),
            attempt_count: Arc::new(AtomicU64::new(0)),
            succeed_first_n_attempts: u64::MAX, // succeeds immediately, every time
        };

        let mut specs: HashMap<String, NodeSpec> = HashMap::new();
        specs.insert(
            "in".to_string(),
            NodeSpec::Input(
                Box::new(BurstThenHangInput { batches: vec![], gap: Duration::ZERO }),
                InputRuntimeConfig::default(),
            ),
        );
        specs.insert(
            "out".to_string(),
            NodeSpec::Output(
                Box::new(output),
                SinkStoreConfig::Disk(disk_config),
                WriteLoopConfig::default(),
            ),
        );

        // `in` sends nothing and hangs -- shutdown closes it, which is what lets `run` finish
        // once `out` has drained the spool to empty.
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let run = tokio::spawn(logit_pipeline::run_with_shutdown(graph, specs, async move {
            let mut rx = shutdown_rx;
            let _ = rx.wait_for(|&fired| fired).await;
        }));
        // Poll until the last marker is delivered, then fire shutdown; `run` never returns on its
        // own, since nothing closes `in`'s Fanout. Wait on the last marker, not a delivery count:
        // how many deliveries run 2 makes depends on where run 1's cursor was last checkpointed.
        // `checkpoint_interval` is time-gated, so all 10 commits may land before the first
        // checkpoint and the whole spool replay, which the at-most-twice assertion below allows.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            let last_delivered =
                run2_attempts.lock().unwrap().iter().any(|(marker, _)| *marker == TOTAL - 1);
            if last_delivered || tokio::time::Instant::now() > deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let _ = shutdown_tx.send(true);
        tokio::time::timeout(Duration::from_secs(5), run)
            .await
            .expect("run should finish once shutdown fires")
            .expect("the task should not panic")
            .expect("run should complete without error");
    }
    let run2_successes: Vec<usize> = run2_attempts
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, ok)| *ok)
        .map(|(marker, _)| *marker)
        .collect();

    // Across both runs, every batch succeeded at least once and at most twice (once per run, if
    // the crash landed between commit and checkpoint). Run 2 delivers in marker order because the
    // spool is FIFO.
    let mut counts = HashMap::new();
    for &marker in run1_successes.iter().chain(run2_successes.iter()) {
        *counts.entry(marker).or_insert(0u32) += 1;
    }
    for marker in 0..TOTAL {
        let count = counts.get(&marker).copied().unwrap_or(0);
        assert!(count >= 1, "batch {marker} was never delivered at all");
        assert!(count <= 2, "batch {marker} was delivered {count} times, expected at most 2");
    }
    let mut sorted = run2_successes.clone();
    sorted.sort_unstable();
    assert_eq!(run2_successes, sorted, "run 2's deliveries must be in FIFO order");

    std::fs::remove_dir_all(&dir).ok();
}

/// A reader that catches up to the writer while its segment is still active keeps delivering once
/// that segment rotates away. `gap` lets delivery keep pace with ingest, parking the reader at
/// `read_offset == len` of the active segment before each rotation: the integration-level
/// counterpart of `disk_queue.rs`'s
/// `the_cursor_rolls_forward_when_a_segment_the_reader_caught_up_to_later_rotates_away`.
#[tokio::test]
async fn a_disk_backed_sink_keeps_delivering_across_a_rotation_once_the_reader_has_caught_up() {
    let dir = std::env::temp_dir().join(format!(
        "logit-durable-buffer-rotation-{}-{}",
        std::process::id(),
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let spool_dir = dir.join("spool");

    const TOTAL: usize = 12;
    let gap = Duration::from_millis(20);

    // A couple of small records per segment -- small enough that this handful of batches forces
    // several real rotations.
    let (graph, mut disk_config) = graph_and_topology(spool_dir.clone());
    disk_config.segment_bytes = 256;

    let attempts = Arc::new(Mutex::new(Vec::new()));
    let output = RecordingOutput {
        attempts: Arc::clone(&attempts),
        attempt_count: Arc::new(AtomicU64::new(0)),
        succeed_first_n_attempts: u64::MAX, // always succeeds
    };

    let batches: Vec<EventBatch> = (0..TOTAL).map(batch).collect();

    let mut specs: HashMap<String, NodeSpec> = HashMap::new();
    specs.insert(
        "in".to_string(),
        NodeSpec::Input(
            Box::new(BurstThenHangInput { batches, gap }),
            InputRuntimeConfig::default(),
        ),
    );
    specs.insert(
        "out".to_string(),
        NodeSpec::Output(
            Box::new(output),
            SinkStoreConfig::Disk(disk_config),
            WriteLoopConfig::default(),
        ),
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let run = tokio::spawn(logit_pipeline::run_with_shutdown(graph, specs, async move {
        let mut rx = shutdown_rx;
        let _ = rx.wait_for(|&fired| fired).await;
    }));

    // Poll for every delivery rather than sleeping a fixed guess; a reader that stalls after a
    // rotation runs out the deadline instead.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if attempts.lock().unwrap().len() >= TOTAL || tokio::time::Instant::now() > deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let _ = shutdown_tx.send(true);
    tokio::time::timeout(Duration::from_secs(5), run)
        .await
        .expect("run should finish once shutdown fires")
        .expect("the task should not panic")
        .expect("run should complete without error");

    let delivered: Vec<usize> =
        attempts.lock().unwrap().iter().map(|(marker, _)| *marker).collect();
    assert_eq!(
        delivered,
        (0..TOTAL).collect::<Vec<_>>(),
        "all {TOTAL} markers should have been delivered, in order, exactly once -- pre-fix, the \
         reader would park forever on `not_empty` the first time it caught up mid-active-segment \
         and that segment later rotated away"
    );

    std::fs::remove_dir_all(&dir).ok();
}
