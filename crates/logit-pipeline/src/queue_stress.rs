//! Randomized concurrency stress for [`BoundedQueue`] and [`DiskQueue`] (NET-06, NET-07, RT-07,
//! and DISK-08 in `docs/plans/critical-sections-inventory.md`): random producers, consumers,
//! cancellations, and close timing, checked against a ledger that every handed item must
//! reconcile with. The method, and why not loom or shuttle, is decision 5 of
//! `docs/adr/shutdown-accounting-and-cancellation-safety.md`.
//!
//! Each seed is one scenario, replayable with `LOGIT_QUEUE_STRESS_SEED=<n>`. The thread schedule
//! isn't replayable, so a failure prints its seed and scenario to rerun under the same
//! configuration, not the same interleaving. That is also why this is a seeded loop and not a
//! `proptest`: shrinking needs a failure that reproduces.
//!
//! `BoundedQueue` runs every producer and consumer as its own task on a four-worker runtime, a
//! stronger contract than production needs, where each queue's two halves share one task.
//! `DiskQueue` runs its producer and consumer as two futures joined in one task, the production
//! shape, because its `commit` and `evict_oldest` are safe only there (the same-task contract on
//! [`DiskQueue`]).
//!
//! What a cancelled operation leaves:
//!
//! - A `BoundedQueue::push` cut off is never admitted; nothing counts it, so the ledger does.
//! - A `BoundedQueue::push_many` cut off after its first poll keeps its admitted prefix and
//!   counts the rest `reason="shutdown"`. One dropped before its first poll leaves every item in
//!   the caller's `Vec`, held by the caller.
//! - A `DiskQueue::push` cut off is not queued. Its write may already be on a blocking thread,
//!   and the next push truncates it; the one exception is a trailing push nothing wrote after,
//!   whose record `finish` flushes and the next open replays.
//! - A cut-off `pop`, `pop_many`, or `peek` removes and reserves nothing.

use std::collections::HashSet;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::disk_queue::test_support::{
    batch, config, ctx, encoded_record_len, marker_of, metric_sum, scratch_dir,
};
use crate::disk_queue::{DiskQueue, DiskQueueConfig};
use crate::queue::SINK_QUEUE_METRICS;
use crate::queue::{BoundedQueue, OverflowPolicy, QueueConfig, QueueMetrics, Queued};
use logit_core::{Diagnostics, Event, Provenance, Registry, Telemetry};

/// Real time, not paused: the scenarios run on a multi-thread runtime.
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(30);

/// SplitMix64: enough randomness for scheduling choices, with no dependency.
struct SplitMix64(u64);

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0x6c6f_6769_745f_7173)
    }
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1000) < per_mille
    }
    fn pick<T: Copy>(&mut self, from: &[T]) -> T {
        from[self.below(from.len() as u64) as usize]
    }
}

/// `0..n`, or the one seed `LOGIT_QUEUE_STRESS_SEED` names.
fn seeds(n: u64) -> Vec<u64> {
    match std::env::var("LOGIT_QUEUE_STRESS_SEED") {
        Ok(seed) => vec![seed.parse().expect("LOGIT_QUEUE_STRESS_SEED must be a u64")],
        Err(_) => (0..n).collect(),
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap()
}

/// One progress counter per task, printed when a scenario times out.
struct Progress {
    names: Vec<String>,
    counts: Vec<AtomicU64>,
}

impl Progress {
    fn new(names: Vec<String>) -> Arc<Self> {
        let counts = names.iter().map(|_| AtomicU64::new(0)).collect();
        Arc::new(Self { names, counts })
    }
    fn tick(&self, task: usize) {
        self.counts[task].fetch_add(1, Ordering::Relaxed);
    }
}

impl fmt::Display for Progress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (name, count) in self.names.iter().zip(&self.counts) {
            write!(f, "{name}={} ", count.load(Ordering::Relaxed))?;
        }
        Ok(())
    }
}

/// Runs `scenario` under [`SCENARIO_TIMEOUT`], panicking with the seed, the scenario, and every
/// task's progress if it doesn't finish.
fn run_with_timeout<F: Future<Output = ()>>(
    rt: &tokio::runtime::Runtime,
    seed: u64,
    desc: &str,
    progress: &Progress,
    scenario: F,
) {
    rt.block_on(async {
        if tokio::time::timeout(SCENARIO_TIMEOUT, scenario).await.is_err() {
            panic!(
                "seed {seed} ({desc}) did not finish in {SCENARIO_TIMEOUT:?}: a task is parked \
                 with nothing left to wake it. Progress: {progress}"
            );
        }
    });
}

/// Returns `Pending` `n` times, waking itself each time: a cancellation point finer than a timer.
struct YieldN(u32);

impl Future for YieldN {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 == 0 {
            return Poll::Ready(());
        }
        self.0 -= 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// How an operation may be cut off. `select!` polls its branches in random order, so a canceller
/// that is ready at once can also win before the operation is ever polled.
#[derive(Clone, Copy, Debug)]
enum Cut {
    Never,
    Yields(u32),
    SleepZero,
    Micros(u64),
    /// Built and dropped without a poll.
    Unpolled,
}

fn pick_cut(rng: &mut SplitMix64, cancel_per_mille: u64) -> Cut {
    if !rng.chance(cancel_per_mille) {
        return Cut::Never;
    }
    match rng.below(10) {
        0..=4 => Cut::Yields(rng.below(4) as u32),
        5 | 6 => Cut::SleepZero,
        7 => Cut::Micros(rng.below(1500)),
        _ => Cut::Unpolled,
    }
}

/// `Some(output)` if `op` completed, `None` if `cut` cut it off.
async fn race<F: Future>(op: F, cut: Cut) -> Option<F::Output> {
    match cut {
        Cut::Never => Some(op.await),
        Cut::Unpolled => {
            drop(op);
            None
        }
        Cut::Yields(n) => tokio::select! {
            out = op => Some(out),
            () = YieldN(n) => None,
        },
        Cut::SleepZero => tokio::select! {
            out = op => Some(out),
            () = tokio::time::sleep(Duration::ZERO) => None,
        },
        Cut::Micros(us) => tokio::select! {
            out = op => Some(out),
            () = tokio::time::sleep(Duration::from_micros(us)) => None,
        },
    }
}

#[derive(Clone, Copy, Debug)]
enum Close {
    AfterProducers,
    AfterMicros(u64),
    Immediately,
}

fn pick_close(rng: &mut SplitMix64) -> Close {
    match rng.below(3) {
        0 => Close::AfterProducers,
        1 => Close::AfterMicros(rng.below(3000)),
        _ => Close::Immediately,
    }
}

// ---------------------------------------------------------------------------------------------
// BoundedQueue
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Debug)]
struct StressItem {
    producer: u64,
    seq: u64,
    weight: u64,
    units: u64,
}

impl Queued for StressItem {
    fn weight(&self) -> u64 {
        self.weight
    }
    fn units(&self) -> u64 {
        self.units
    }
}

static STRESS_METRICS: QueueMetrics = QueueMetrics {
    depth: "stress.queue.depth",
    bytes: "stress.queue.bytes",
    utilization: "stress.queue.utilization",
    push_blocked: "stress.queue.push.blocked.duration",
    items_dropped: "stress.queue.items.dropped",
    units_dropped: "stress.queue.units.dropped",
};

const DROP_REASONS: [&str; 3] = ["overflow_oldest", "overflow_newest", "shutdown"];

fn stress_queue(config: QueueConfig) -> (Arc<Registry>, Arc<BoundedQueue<StressItem>>) {
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("stress", "input", "source");
    (registry, Arc::new(BoundedQueue::with_metrics(config, &STRESS_METRICS, telemetry)))
}

#[derive(Debug)]
struct BoundedScenario {
    config: QueueConfig,
    producers: Vec<u64>,
    consumers: usize,
    cancel_per_mille: u64,
    never_fits: bool,
    /// The heaviest ordinary item. Zero-drop mode keeps it within `max_weight`: a heavier item can
    /// never fit, and `Block` evicts to admit it.
    max_item_weight: u64,
    close: Close,
    zero_drop: bool,
}

impl BoundedScenario {
    fn pick(rng: &mut SplitMix64) -> Self {
        let zero_drop = rng.chance(250);
        let max_items = rng.pick(&[1usize, 2, 3, 8, 64]);
        let max_weight = if rng.chance(500) { u64::MAX } else { 3 + rng.below(38) };
        let overflow = if zero_drop {
            OverflowPolicy::Block
        } else {
            rng.pick(&[
                OverflowPolicy::Block,
                OverflowPolicy::DropOldest,
                OverflowPolicy::DropNewest,
            ])
        };
        let producers = (0..1 + rng.below(3)).map(|_| rng.below(401)).collect();
        let consumers = 1 + rng.below(2) as usize;
        let cancel_per_mille = if zero_drop || rng.chance(333) { 0 } else { 1 + rng.below(300) };
        Self {
            config: QueueConfig { max_items, max_weight, overflow },
            producers,
            consumers,
            cancel_per_mille,
            never_fits: !zero_drop && rng.chance(500),
            max_item_weight: if zero_drop { max_weight.min(8) } else { 8 },
            close: if zero_drop { Close::AfterProducers } else { pick_close(rng) },
            zero_drop,
        }
    }
}

/// What one producer handed over, and what of that it knows was not admitted.
#[derive(Default)]
struct ProducerReport {
    handed: (u64, u64),
    /// Single pushes cut off before admission: `(seq, units)`.
    cancelled: Vec<(u64, u64)>,
    /// Items an unpolled `push_many` left in this producer's `Vec`: `(seq, units)`.
    held: Vec<(u64, u64)>,
}

/// What a producer draws its items and cancellations from.
#[derive(Clone, Copy)]
struct ProducerMix {
    cancel_per_mille: u64,
    never_fits: bool,
    max_item_weight: u64,
}

/// Producer `producer` reports its progress as task `producer`.
async fn bounded_producer(
    q: Arc<BoundedQueue<StressItem>>,
    producer: u64,
    count: u64,
    scenario_seed: u64,
    mix: ProducerMix,
    progress: Arc<Progress>,
) -> ProducerReport {
    let ProducerMix { cancel_per_mille, never_fits, max_item_weight } = mix;
    let task = producer as usize;
    let mut rng = SplitMix64::new(scenario_seed.wrapping_mul(31).wrapping_add(producer));
    let mut report = ProducerReport::default();
    let mut seq = 0;
    while seq < count {
        progress.tick(task);
        if rng.chance(150) {
            tokio::task::yield_now().await;
        }
        let batch_len = if rng.chance(500) { 1 + rng.below(8).min(count - seq - 1) } else { 0 };
        let make = |seq: u64, rng: &mut SplitMix64| {
            let weight = if never_fits && rng.chance(20) {
                u64::MAX
            } else {
                rng.below(max_item_weight + 1)
            };
            StressItem { producer, seq, weight, units: 1 + rng.below(4) }
        };
        let cut = pick_cut(&mut rng, cancel_per_mille);
        if batch_len == 0 {
            let item = make(seq, &mut rng);
            report.handed.0 += 1;
            report.handed.1 += item.units;
            let units = item.units;
            if race(q.push(item), cut).await.is_none() {
                report.cancelled.push((seq, units));
            }
            seq += 1;
        } else {
            let mut items: Vec<StressItem> =
                (seq..seq + batch_len).map(|s| make(s, &mut rng)).collect();
            report.handed.0 += batch_len;
            report.handed.1 += items.iter().map(|i| i.units).sum::<u64>();
            let _ = race(q.push_many(&mut items), cut).await;
            report.held.extend(items.drain(..).map(|i| (i.seq, i.units)));
            seq += batch_len;
        }
    }
    report
}

async fn bounded_consumer(
    q: Arc<BoundedQueue<StressItem>>,
    use_peek: bool,
    seed: u64,
    cancel_per_mille: u64,
    progress: Arc<Progress>,
    task: usize,
) -> Vec<StressItem> {
    let mut rng = SplitMix64::new(seed);
    let mut got = Vec::new();
    loop {
        progress.tick(task);
        if rng.chance(150) {
            tokio::task::yield_now().await;
        }
        let cut = pick_cut(&mut rng, cancel_per_mille);
        match rng.below(if use_peek { 3 } else { 2 }) {
            0 => match race(q.pop(), cut).await {
                Some(Some(item)) => got.push(item),
                Some(None) => break,
                None => {}
            },
            1 => {
                let max = 1 + rng.below(8) as usize;
                let mut out = Vec::new();
                match race(q.pop_many(&mut out, max), cut).await {
                    Some(0) => break,
                    Some(n) => {
                        assert_eq!(n, out.len());
                        assert!(n <= max);
                        got.append(&mut out);
                    }
                    None => assert!(out.is_empty(), "a cut-off pop_many removed items"),
                }
            }
            _ => match race(q.peek(), cut).await {
                Some(Some(peeked)) => {
                    let committed = q.commit().expect("a peeked head is still there to commit");
                    assert_eq!(
                        (committed.producer, committed.seq),
                        (peeked.producer, peeked.seq),
                        "commit must remove the item peek reserved"
                    );
                    got.push(committed);
                }
                Some(None) => break,
                None => {}
            },
        }
    }
    got
}

fn check_increasing(items: &[StressItem], what: &str) {
    let mut last: std::collections::HashMap<u64, u64> = std::collections::HashMap::new();
    for item in items {
        if let Some(&prev) = last.get(&item.producer) {
            assert!(
                item.seq > prev,
                "{what}: producer {}'s seq {} after {prev}; FIFO order broken",
                item.producer,
                item.seq
            );
        }
        last.insert(item.producer, item.seq);
    }
}

fn dropped(events: &[Event], metric: &str, reason: &str) -> u64 {
    metric_sum(events, metric, Some(("reason", reason))) as u64
}

fn bounded_scenario(rt: &tokio::runtime::Runtime, seed: u64) {
    let mut rng = SplitMix64::new(seed);
    let scenario = BoundedScenario::pick(&mut rng);
    let desc = format!("{scenario:?}");
    let mut names: Vec<String> =
        (0..scenario.producers.len()).map(|p| format!("producer{p}")).collect();
    names.extend((0..scenario.consumers).map(|c| format!("consumer{c}")));
    let progress = Progress::new(names);
    let (registry, q) = stress_queue(scenario.config);

    let mut outcome = None;
    run_with_timeout(rt, seed, &desc, &progress, async {
        let consumers: Vec<_> = (0..scenario.consumers)
            .map(|c| {
                tokio::spawn(bounded_consumer(
                    Arc::clone(&q),
                    scenario.consumers == 1,
                    seed.wrapping_mul(7).wrapping_add(c as u64 + 100),
                    scenario.cancel_per_mille,
                    Arc::clone(&progress),
                    scenario.producers.len() + c,
                ))
            })
            .collect();
        let producers: Vec<_> = scenario
            .producers
            .iter()
            .enumerate()
            .map(|(p, &count)| {
                tokio::spawn(bounded_producer(
                    Arc::clone(&q),
                    p as u64,
                    count,
                    seed,
                    ProducerMix {
                        cancel_per_mille: scenario.cancel_per_mille,
                        never_fits: scenario.never_fits,
                        max_item_weight: scenario.max_item_weight,
                    },
                    Arc::clone(&progress),
                ))
            })
            .collect();
        let closer = match scenario.close {
            Close::Immediately => {
                q.close();
                None
            }
            Close::AfterMicros(us) => {
                let q = Arc::clone(&q);
                Some(tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_micros(us)).await;
                    q.close();
                }))
            }
            Close::AfterProducers => None,
        };
        let mut reports = Vec::new();
        for producer in producers {
            reports.push(producer.await.unwrap_or_else(|e| panic!("seed {seed} ({desc}): {e}")));
        }
        if let Close::AfterProducers = scenario.close {
            q.close();
        }
        if let Some(closer) = closer {
            closer.await.unwrap();
        }
        let mut got = Vec::new();
        for consumer in consumers {
            got.push(consumer.await.unwrap_or_else(|e| panic!("seed {seed} ({desc}): {e}")));
        }
        outcome = Some((reports, got));
    });
    let (reports, got) = outcome.expect("the scenario finished");

    // Everything still queued, through `commit`, whose last call also refreshes the gauges with
    // nothing else running.
    let mut remaining = Vec::new();
    while let Some(item) = q.commit() {
        remaining.push(item);
    }
    let events = registry.drain(0);
    let fail = |msg: String| -> ! { panic!("seed {seed} ({desc}): {msg}") };

    // Identity: nothing twice, nothing that was cut off or left with the caller.
    let mut not_admitted = HashSet::new();
    for (p, report) in reports.iter().enumerate() {
        for &(seq, _) in report.cancelled.iter().chain(&report.held) {
            not_admitted.insert((p as u64, seq));
        }
    }
    let mut seen = HashSet::new();
    for item in got.iter().flatten().chain(&remaining) {
        let key = (item.producer, item.seq);
        if !seen.insert(key) {
            fail(format!("{key:?} was delivered or left queued twice"));
        }
        if not_admitted.contains(&key) {
            fail(format!("{key:?} was cut off before admission but reached the queue"));
        }
        if item.seq >= scenario.producers[item.producer as usize] {
            fail(format!("{key:?} was never handed over"));
        }
    }
    for (c, items) in got.iter().enumerate() {
        check_increasing(items, &format!("seed {seed} consumer {c}"));
    }
    check_increasing(&remaining, &format!("seed {seed} remaining"));

    // Counts: every handed item is popped, still queued, counted dropped, or known not admitted.
    let sum = |it: &mut dyn Iterator<Item = u64>| it.sum::<u64>();
    let handed = (
        sum(&mut reports.iter().map(|r| r.handed.0)),
        sum(&mut reports.iter().map(|r| r.handed.1)),
    );
    let delivered = (
        (got.iter().map(Vec::len).sum::<usize>() + remaining.len()) as u64,
        sum(&mut got.iter().flatten().chain(&remaining).map(|i| i.units)),
    );
    let known = (
        not_admitted.len() as u64,
        sum(&mut reports.iter().flat_map(|r| r.cancelled.iter().chain(&r.held)).map(|x| x.1)),
    );
    let drops = (
        DROP_REASONS.iter().map(|r| dropped(&events, STRESS_METRICS.items_dropped, r)).sum::<u64>(),
        DROP_REASONS.iter().map(|r| dropped(&events, STRESS_METRICS.units_dropped, r)).sum::<u64>(),
    );
    let accounted = (delivered.0 + known.0 + drops.0, delivered.1 + known.1 + drops.1);
    if handed != accounted {
        fail(format!(
            "handed (items, units) {handed:?} != popped+remaining {delivered:?} + cut off or held \
             {known:?} + dropped {drops:?}"
        ));
    }
    if metric_sum(&events, STRESS_METRICS.items_dropped, None) as u64 != drops.0 {
        fail("an items_dropped count carries a reason outside the expected three".into());
    }
    if scenario.zero_drop && drops != (0, 0) {
        fail(format!("zero-drop mode dropped {drops:?}"));
    }

    // Gauges: the final `commit` wrote last, with nothing queued.
    for gauge in [STRESS_METRICS.depth, STRESS_METRICS.bytes, STRESS_METRICS.utilization] {
        let value = metric_sum(&events, gauge, None);
        if value != 0.0 {
            fail(format!("{gauge} reads {value} on an empty queue"));
        }
    }
}

#[test]
fn bounded_queue_under_random_producers_consumers_cancellation_and_close_loses_and_duplicates_nothing(
) {
    let rt = runtime();
    for seed in seeds(64) {
        bounded_scenario(&rt, seed);
    }
}

#[test]
#[ignore = "long stress mode: 20,000 seeds"]
fn bounded_queue_long_stress() {
    let rt = runtime();
    for seed in seeds(20_000) {
        bounded_scenario(&rt, seed);
    }
}

/// `close()` wakes every parked waiter on both sides: three `Block` pushers (`push` and
/// `push_many`) on a full queue, then `pop`, `pop_many`, and `peek` on an empty one.
#[test]
fn a_close_while_every_producer_and_consumer_is_parked_wakes_all_of_them() {
    let rt = runtime();
    rt.block_on(async {
        let item = |seq: u64| StressItem { producer: 0, seq, weight: 1, units: 1 };
        let config =
            QueueConfig { max_items: 2, max_weight: u64::MAX, overflow: OverflowPolicy::Block };
        let (registry, q) = stress_queue(config);
        q.push(item(0)).await;
        q.push(item(1)).await;
        let pushers = vec![
            tokio::spawn({
                let q = Arc::clone(&q);
                async move { q.push(item(2)).await }
            }),
            tokio::spawn({
                let q = Arc::clone(&q);
                async move {
                    let mut items = vec![item(3), item(4), item(5)];
                    q.push_many(&mut items).await;
                    assert!(items.is_empty());
                }
            }),
            tokio::spawn({
                let q = Arc::clone(&q);
                async move { q.push(item(6)).await }
            }),
        ];
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(pushers.iter().all(|p| !p.is_finished()), "every pusher parks on a full queue");
        q.close();
        for pusher in pushers {
            tokio::time::timeout(Duration::from_secs(5), pusher)
                .await
                .expect("close() must wake a parked pusher")
                .unwrap();
        }
        let mut remaining = 0u64;
        while q.commit().is_some() {
            remaining += 1;
        }
        let evicted = dropped(&registry.drain(0), STRESS_METRICS.items_dropped, "overflow_oldest");
        assert_eq!(remaining + evicted, 7, "every item is queued or counted evicted");

        let (_registry, q) = stress_queue(config);
        let pop = tokio::spawn({
            let q = Arc::clone(&q);
            async move { q.pop().await.is_none() }
        });
        let pop_many = tokio::spawn({
            let q = Arc::clone(&q);
            async move { q.pop_many(&mut Vec::new(), 4).await == 0 }
        });
        let peek = tokio::spawn({
            let q = Arc::clone(&q);
            async move { q.peek().await.is_none() }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let consumers = [pop, pop_many, peek];
        assert!(consumers.iter().all(|c| !c.is_finished()), "every consumer parks on empty");
        q.close();
        for consumer in consumers {
            let closed_and_empty = tokio::time::timeout(Duration::from_secs(5), consumer)
                .await
                .expect("close() must wake a parked consumer")
                .unwrap();
            assert!(closed_and_empty, "a woken consumer reports closed and empty");
        }
    });
}

// ---------------------------------------------------------------------------------------------
// DiskQueue
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
struct DiskScenario {
    max_bytes: u64,
    segment_bytes: u64,
    overflow: OverflowPolicy,
    count: u64,
    cancel_per_mille: u64,
    close: Close,
}

impl DiskScenario {
    fn pick(rng: &mut SplitMix64, one: u64) -> Self {
        Self {
            max_bytes: rng.pick(&[2 * one, 3 * one, 8 * one, u64::MAX]),
            segment_bytes: rng.pick(&[one, 3 * one, 1024 * 1024]),
            overflow: rng.pick(&[
                OverflowPolicy::Block,
                OverflowPolicy::DropOldest,
                OverflowPolicy::DropNewest,
            ]),
            count: rng.below(61),
            cancel_per_mille: if rng.chance(333) { 0 } else { 1 + rng.below(300) },
            close: pick_close(rng),
        }
    }
}

/// Fixed width, so every record is the same length.
fn disk_marker(seq: u64) -> String {
    format!("{seq:06}")
}

fn disk_seq(batch: &logit_core::EventBatch) -> u64 {
    marker_of(batch).parse().expect("a stress marker")
}

async fn drain_disk(q: &DiskQueue) -> Vec<u64> {
    q.close();
    let mut out = Vec::new();
    while let Some((batch, _)) = q.peek().await {
        out.push(disk_seq(&batch));
        q.commit().expect("a peeked head is still there to commit");
    }
    out
}

fn disk_scenario(rt: &tokio::runtime::Runtime, seed: u64) {
    let mut rng = SplitMix64::new(seed);
    let one = encoded_record_len(&batch(&disk_marker(0)), Provenance::default());
    let scenario = DiskScenario::pick(&mut rng, one);
    let desc = format!("{scenario:?}");
    let progress = Progress::new(vec!["producer".into(), "consumer".into()]);
    let dir = scratch_dir("queue-stress");
    let cfg = DiskQueueConfig {
        max_bytes: scenario.max_bytes,
        segment_bytes: scenario.segment_bytes,
        overflow: scenario.overflow,
        ..config(dir.clone())
    };
    let registry = Registry::new();
    let telemetry = registry.telemetry_for("stress", "output", "sink");
    let q = DiskQueue::open(cfg.clone(), telemetry, Diagnostics::new("stress")).unwrap();

    let mut outcome = None;
    run_with_timeout(rt, seed, &desc, &progress, async {
        let mut producer_rng = SplitMix64::new(seed.wrapping_mul(31).wrapping_add(1));
        let mut consumer_rng = SplitMix64::new(seed.wrapping_mul(31).wrapping_add(2));
        if let Close::Immediately = scenario.close {
            q.close();
        }
        let producer = async {
            let mut cancelled = Vec::new();
            for seq in 0..scenario.count {
                progress.tick(0);
                if producer_rng.chance(150) {
                    tokio::task::yield_now().await;
                }
                let cut = pick_cut(&mut producer_rng, scenario.cancel_per_mille);
                if race(q.push((batch(&disk_marker(seq)), ctx())), cut).await.is_none() {
                    cancelled.push(seq);
                }
            }
            if let Close::AfterProducers = scenario.close {
                q.close();
            }
            cancelled
        };
        let consumer = async {
            let mut delivered = Vec::new();
            loop {
                progress.tick(1);
                if consumer_rng.chance(150) {
                    tokio::task::yield_now().await;
                }
                let cut = pick_cut(&mut consumer_rng, scenario.cancel_per_mille);
                match race(q.peek(), cut).await {
                    Some(Some((peeked, _))) => {
                        let (committed, _) = q.commit().expect("a peeked head is there to commit");
                        assert_eq!(disk_seq(&committed), disk_seq(&peeked));
                        delivered.push(disk_seq(&committed));
                    }
                    Some(None) => break,
                    None => {}
                }
            }
            delivered
        };
        let closer = async {
            if let Close::AfterMicros(us) = scenario.close {
                tokio::time::sleep(Duration::from_micros(us)).await;
                q.close();
            }
        };
        let (cancelled, delivered, ()) = tokio::join!(producer, consumer, closer);
        q.finish().await;
        outcome = Some((cancelled, delivered));
    });
    let (cancelled, delivered) = outcome.expect("the scenario finished");
    drop(q);
    let events = registry.drain(0);

    let reopened = DiskQueue::open(cfg, Telemetry::default(), Diagnostics::new("stress")).unwrap();
    let replayed = rt.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, drain_disk(&reopened))
            .await
            .unwrap_or_else(|_| panic!("seed {seed} ({desc}): draining the reopened spool hung"))
    });
    drop(reopened);
    std::fs::remove_dir_all(&dir).ok();
    let fail = |msg: String| -> ! { panic!("seed {seed} ({desc}): {msg}") };

    let cancelled_set: HashSet<u64> = cancelled.iter().copied().collect();
    if let Some(seq) = delivered.iter().find(|s| cancelled_set.contains(s)) {
        fail(format!("cut-off push {seq} was delivered live"));
    }
    let order: Vec<u64> = delivered.iter().chain(&replayed).copied().collect();
    if let Some(pair) = order.windows(2).find(|w| w[0] >= w[1]) {
        fail(format!("delivered then replayed out of push order, or twice: {pair:?} in {order:?}"));
    }
    let reappeared: Vec<u64> =
        replayed.iter().copied().filter(|s| cancelled_set.contains(s)).collect();
    if reappeared.len() > 1 || reappeared.first().is_some_and(|s| replayed.last() != Some(s)) {
        fail(format!(
            "only the trailing cut-off push may reappear after reopen, got {reappeared:?} in \
             {replayed:?}"
        ));
    }
    if dropped(&events, SINK_QUEUE_METRICS.items_dropped, "disk_corrupt") != 0 {
        fail("a record read back corrupt".into());
    }
    let drops = metric_sum(&events, SINK_QUEUE_METRICS.items_dropped, None) as u64;
    let handed = scenario.count - cancelled.len() as u64;
    let accounted = delivered.len() as u64 + drops + (replayed.len() - reappeared.len()) as u64;
    if handed != accounted {
        fail(format!(
            "handed minus cut off {handed} != delivered {} + dropped {drops} + replayed {} \
             (less {} reappeared)",
            delivered.len(),
            replayed.len(),
            reappeared.len()
        ));
    }
}

#[test]
fn disk_queue_under_a_random_producer_consumer_and_close_loses_and_duplicates_nothing() {
    let rt = runtime();
    for seed in seeds(16) {
        disk_scenario(&rt, seed);
    }
}

#[test]
#[ignore = "long stress mode: 1,000 seeds"]
fn disk_queue_long_stress() {
    let rt = runtime();
    for seed in seeds(1_000) {
        disk_scenario(&rt, seed);
    }
}
