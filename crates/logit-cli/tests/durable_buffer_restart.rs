//! End-to-end proof of `docs/adr/disk-backed-sink-buffer.md`'s core claim: a disk-backed sink
//! buffer survives a process death with no shutdown signal (the closest in-process analogue of
//! `SIGKILL` -- dropping the whole runtime future rather than racing a `watch` signal) and, on
//! restart, delivers every batch pushed before the drop, in order, losing nothing. Modelled on
//! `otlp_round_trip.rs`, the only other integration test in this crate: builds a graph and its
//! `NodeSpec`s directly via `logit_pipeline`'s public API rather than parsing a YAML config, since
//! the sink under test needs to fail on command, which no real `ComponentKind` can express.

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
        events: vec![Event::empty(marker as i64, attrs)],
    }
}

fn marker_of(batch: &EventBatch) -> usize {
    batch.events[0].attributes.get("marker").and_then(Value::as_str).unwrap().parse().unwrap()
}

/// Sends every batch in `batches` once, then hangs forever -- the test aborts the whole `run`
/// task rather than ever letting it finish, simulating `SIGKILL`: no shutdown signal, no chance
/// for anything downstream to flush or checkpoint on its own initiative.
///
/// `gap`: how long to sleep between each `sink.send(...)` call -- `Duration::ZERO` sends the
/// whole burst as fast as possible (this file's original scenario); a nonzero gap gives delivery
/// time to keep pace with ingest, which is what parks the reader at the end of the active segment
/// before each rotation -- the F1 scenario `a_disk_backed_sink_keeps_delivering_across_a_rotation_once_the_reader_has_caught_up`
/// below needs.
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

/// `anyhow::Context` isn't in scope by default for a bare `Err` -- attaching `Fault::Clean` is
/// exactly what every real sink's own `send` does (`docs/adr/buffered-sink-delivery.md`); a
/// `Clean` fault is retried under every delivery posture with zero duplicate risk, since the
/// destination provably never saw the batch.
trait ContextFault<T> {
    fn context_fault(self) -> anyhow::Result<T>;
}
impl<T> ContextFault<T> for anyhow::Result<T> {
    fn context_fault(self) -> anyhow::Result<T> {
        use anyhow::Context;
        self.context(logit_pipeline::Fault::Clean)
    }
}

/// Records every attempt (`(marker, succeeded)`) it's asked to make. Succeeds on the first
/// `succeed_first_n_attempts` calls made against this instance (regardless of which batch --
/// `write_loop` is strictly single-in-flight, so the Nth call is always batch N's first and only
/// attempt as long as every earlier one succeeded) and fails every call after that, forever --
/// which is what lets one instance simulate "some batches already delivered, the next one stuck
/// retrying, the rest never even attempted."
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
        // At-least-once: this test's whole point is proving batches survive a restart, which
        // requires the posture that actually retries/redelivers rather than giving up.
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
            kind: ComponentKind::StatsdIn { bind: "127.0.0.1:0".to_string() },
        },
    );
    components.insert(
        "out".to_string(),
        Component {
            buffer: BufferConfig::default(),
            receive: ReceiveConfig::default(),
            sources: vec!["in".to_string()],
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

        // No shutdown signal at all -- aborting this task drops the whole `run` future outright,
        // taking every node task (and the `DiskQueue` inside them) down with it mid-flight. This
        // is the closest in-process analogue of `SIGKILL`: nothing gets a chance to run
        // `finish()`.
        //
        // *When* to kill is decided by observation, not a clock. A batch is only durable once
        // `DiskQueue::push` has written it; anything still in the input's loop or the sink's
        // inbox at the kill is lost by design, so this test's premise ("every batch was pushed
        // before the crash") has to be established, not assumed. A fixed 500ms wait used to be
        // that assumption, and on a loaded CI disk (each of run 1's ~3 segment rotations
        // `fsync`s twice) 40 pushes did not always fit -- the batches that had not reached the
        // spool yet were then reported as "never delivered". The sink's own
        // `logit.component.buffer.batches` gauge (`SINK_QUEUE_METRICS.depth`: pushed minus
        // committed) reading exactly `TOTAL - SUCCEED_FIRST_RUN` is the precise statement that
        // all 40 are on disk and the first 10 are committed.
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
        // Let the abort actually land before reopening the spool: the aborted task still holds
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
        // Poll until the spool is drained rather than sleeping a fixed guess -- `run` itself
        // never returns on its own here (nothing closes `in`'s Fanout), so this drives shutdown
        // once delivery has caught up. "Drained" is the *last* marker having been delivered,
        // not a count of deliveries: how many run 2 has to make depends on where run 1's cursor
        // was last checkpointed (`checkpoint_interval` is time-gated, so all 10 commits may have
        // landed before the first checkpoint and the whole spool replays -- the at-most-twice
        // assertion below allows exactly that), and stopping after a fixed count used to cut the
        // drain short in that case.
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

    // Every batch pushed in run 1 must have succeeded at least once across the two runs
    // combined, at most twice (once in each run, if the crash landed between commit and
    // checkpoint), and in nondecreasing marker order within each run (the spool is strictly
    // FIFO) -- concatenating the two runs' successes preserves overall chronological order,
    // since every run-1 success happened before every run-2 one.
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

/// End-to-end proof of F1: a reader that catches up to the writer *while* its segment is still
/// active must not stall forever once that segment later rotates away. `gap` between each
/// `sink.send` gives delivery time to keep pace with ingest, which is exactly what parks the
/// reader at `read_offset == len` of the still-active segment before the next rotation --
/// reproducing the bug scenario at the integration level, not just the unit level
/// (`disk_queue.rs`'s own `the_cursor_rolls_forward_when_a_segment_the_reader_caught_up_to_later_rotates_away`).
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

    // Poll for every batch to have been delivered rather than sleeping a fixed guess -- pre-fix,
    // this would never reach TOTAL and the loop would run out the deadline instead.
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
