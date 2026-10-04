//! Shared test helpers for every crate's tests: a named wait, an accumulating telemetry reader, a
//! channel receive under one timeout, socket close checks, a bind-first input spawn, the runtime's
//! own sink write loop over a queue of batches ([`drive_write_loop`]), and a unique scratch
//! directory. The rules they encode are in `docs/adr/test-timing-and-observables.md`.
//!
//! Compiled for this crate's own tests and, elsewhere, only through the dev-only `test-util`
//! feature; `script/lint` fails if `logit-cli`'s release graph enables it.
//!
//! Facts a caller needs:
//!
//! - [`wait_until`] and [`TelemetryProbe::wait_for`] poll every [`POLL_INTERVAL`] for up to
//!   [`RECV_TIMEOUT`] and then panic with `timed out after 5s waiting for {what}`. Name `what`
//!   after the post-state being waited on, so the panic says which step never happened.
//! - [`Registry::drain`] is destructive and a gauge clears on drain. Once a test uses a
//!   [`TelemetryProbe`], every read of that registry goes through the probe: a direct
//!   `registry.drain(0)` elsewhere in the test takes points the probe never sees.
//! - Under `#[tokio::test(start_paused = true)]` the clock jumps forward whenever the runtime is
//!   idle, so a wait on work done by a plain OS thread (a Lua VM, the kernel) reaches its
//!   deadline before that work finishes. Use these waits there only for work driven by the
//!   runtime's own tasks. A pending `spawn_blocking` task, which every `tokio::fs` call is, holds
//!   the clock instead; a read that never waits, such as the disk store's `peek_at`, still races
//!   that task's push on either clock.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use logit_core::telemetry::{Registry, Telemetry};
use logit_core::{interner, Event, EventBatch, MetricKind, Value};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;

use crate::runtime::unwrap_batch;
use crate::{Delivered, Fanout, Input};

/// The ceiling on every positive wait: a batch, a close, a telemetry post-state.
pub const RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// How often [`wait_until`] and [`TelemetryProbe::wait_for`] re-check their condition.
pub const POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Polls `cond` every [`POLL_INTERVAL`] until it holds, panicking after [`RECV_TIMEOUT`].
pub async fn wait_until(what: &str, cond: impl FnMut() -> bool) {
    wait_until_within(what, RECV_TIMEOUT, cond).await;
}

/// [`wait_until`] under a ceiling the caller sets, for a wait whose cost is known to exceed
/// [`RECV_TIMEOUT`] (tens of fsyncs on a loaded disk, a spool replay). The call site's comment
/// says what the ceiling covers.
pub async fn wait_until_within(what: &str, ceiling: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + ceiling;
    while !cond() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out after {ceiling:?} waiting for {what}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// One telemetry series: a metric name and its sorted `(key, value)` attributes, the
/// `component`/`kind`/`role` ones [`Registry::telemetry_for`] stamps included.
type SeriesKey = (String, Vec<(String, String)>);

/// Running totals over any number of drained telemetry batches. A `Sum` point adds to its
/// series, a `Gauge` point replaces its series' value, and every other kind is recorded as seen.
///
/// Readers match tags as a subset: `&[]` matches every series of a name, and
/// `&[("reason", "shutdown")]` matches every series carrying that tag, whatever else it carries.
#[derive(Debug, Default, Clone)]
pub struct Totals {
    /// Every event folded in, in arrival order, for assertions on something the totals don't
    /// keep (a log line, a distribution).
    pub events: Vec<Event>,
    sums: BTreeMap<SeriesKey, f64>,
    gauges: BTreeMap<SeriesKey, f64>,
    seen: BTreeSet<SeriesKey>,
}

impl Totals {
    /// Totals over one drain.
    pub fn of(events: Vec<Event>) -> Self {
        let mut totals = Self::default();
        totals.fold(events);
        totals
    }

    /// Adds one drain's points to the running totals.
    pub fn fold(&mut self, events: Vec<Event>) {
        for event in &events {
            let mut tags: Vec<(String, String)> = event
                .attributes
                .iter()
                .map(|(key, value)| (interner::resolve(key).to_string(), render(value)))
                .collect();
            tags.sort();
            for metric in event.metrics.iter() {
                let key = (interner::resolve(metric.name).to_string(), tags.clone());
                match &metric.kind {
                    MetricKind::Sum(sum) => *self.sums.entry(key.clone()).or_default() += sum.value,
                    MetricKind::Gauge(v) => {
                        self.gauges.insert(key.clone(), *v);
                    }
                    _ => {}
                }
                self.seen.insert(key);
            }
        }
        self.events.extend(events);
    }

    /// The total of every `Sum` series named `name` whose tags include `tags`; `0.0` if none.
    pub fn sum(&self, name: &str, tags: &[(&str, &str)]) -> f64 {
        self.sums.iter().filter(|(key, _)| matches(key, name, tags)).map(|(_, v)| v).sum()
    }

    /// Every `Sum` series: its name, its sorted tags, and its total. For comparing whole runs.
    pub fn sums(&self) -> impl Iterator<Item = (&str, &[(String, String)], f64)> {
        self.sums.iter().map(|((name, tags), v)| (name.as_str(), tags.as_slice(), *v))
    }

    /// Whether any point of any kind named `name` with tags including `tags` has been folded.
    pub fn has(&self, name: &str, tags: &[(&str, &str)]) -> bool {
        self.seen.iter().any(|key| matches(key, name, tags))
    }

    /// The last value of the one `Gauge` series named `name` whose tags include `tags`, or `None`
    /// if no such series has been folded.
    ///
    /// # Panics
    /// If more than one series matches: a gauge from two series has no single value, so the
    /// caller names more tags.
    pub fn gauge(&self, name: &str, tags: &[(&str, &str)]) -> Option<f64> {
        let mut found = self.gauges.iter().filter(|(key, _)| matches(key, name, tags));
        let (_, value) = found.next()?;
        let rest: Vec<_> = found.map(|(key, _)| &key.1).collect();
        assert!(
            rest.is_empty(),
            "gauge {name} with tags {tags:?} matches more than one series; also {rest:?}"
        );
        Some(*value)
    }
}

fn matches(key: &SeriesKey, name: &str, tags: &[(&str, &str)]) -> bool {
    key.0 == name && tags.iter().all(|(k, v)| key.1.iter().any(|(tk, tv)| tk == k && tv == v))
}

/// A tag value as a string for matching. Telemetry tags are always strings; the other arms keep
/// an event from elsewhere readable.
fn render(value: &Value) -> String {
    match value {
        Value::Bool(b) => b.to_string(),
        Value::I64(v) => v.to_string(),
        Value::U64(v) => v.to_string(),
        Value::F64(v) => v.to_string(),
        other => other.as_str().map_or_else(|| format!("{other:?}"), str::to_string),
    }
}

/// A [`Registry`] and the running [`Totals`] of every drain taken from it.
pub struct TelemetryProbe {
    registry: Arc<Registry>,
    totals: Totals,
}

impl Default for TelemetryProbe {
    fn default() -> Self {
        Self::new()
    }
}

impl TelemetryProbe {
    /// A probe over a fresh [`Registry`].
    pub fn new() -> Self {
        Self::with_registry(Registry::new())
    }

    /// A probe over a registry the test built elsewhere, such as the one handed to
    /// `run_with_telemetry`.
    pub fn with_registry(registry: Arc<Registry>) -> Self {
        Self { registry, totals: Totals::default() }
    }

    pub fn registry(&self) -> &Arc<Registry> {
        &self.registry
    }

    /// A handle for component `id`, as [`Registry::telemetry_for`].
    pub fn telemetry(&self, id: &str, kind: &'static str, role: &'static str) -> Telemetry {
        self.registry.telemetry_for(id, kind, role)
    }

    /// Drains the registry into the totals and returns them.
    pub fn poll(&mut self) -> &Totals {
        self.totals.fold(self.registry.drain(0));
        &self.totals
    }

    /// The totals as of the last poll, without draining.
    pub fn totals(&self) -> &Totals {
        &self.totals
    }

    /// [`Totals::sum`] after a [`TelemetryProbe::poll`].
    pub fn sum(&mut self, name: &str, tags: &[(&str, &str)]) -> f64 {
        self.poll().sum(name, tags)
    }

    /// [`Totals::gauge`] after a [`TelemetryProbe::poll`].
    pub fn gauge(&mut self, name: &str, tags: &[(&str, &str)]) -> Option<f64> {
        self.poll().gauge(name, tags)
    }

    /// Polls until `cond` holds over the totals, on [`wait_until`]'s schedule and panic.
    pub async fn wait_for(&mut self, what: &str, mut cond: impl FnMut(&Totals) -> bool) -> &Totals {
        wait_until(what, || cond(self.poll())).await;
        &self.totals
    }
}

/// A [`Fanout`] with one consumer, and that consumer's receiver.
pub fn fanout_channel(capacity: usize) -> (Fanout, mpsc::Receiver<Delivered>) {
    let (tx, rx) = mpsc::channel(capacity);
    (Fanout::new(vec![tx]), rx)
}

/// The next batch on `rx`, panicking after [`RECV_TIMEOUT`] or on a closed channel.
pub async fn recv_batch(rx: &mut mpsc::Receiver<Delivered>) -> EventBatch {
    let delivered = tokio::time::timeout(RECV_TIMEOUT, rx.recv())
        .await
        .unwrap_or_else(|_| panic!("no batch delivered within {RECV_TIMEOUT:?}"))
        .expect("the fanout channel closed before a batch arrived");
    unwrap_batch(delivered)
}

/// Receives batches until at least `n` events have arrived, and returns them all in arrival
/// order. Each batch waits up to [`RECV_TIMEOUT`].
pub async fn recv_events(rx: &mut mpsc::Receiver<Delivered>, n: usize) -> Vec<Event> {
    let mut events = Vec::new();
    while events.len() < n {
        events.extend(recv_batch(rx).await.events);
    }
    events
}

/// Asserts no batch arrives on `rx` within `window`. A closed channel passes: nothing more can
/// arrive. `window` is a negative window, sized per the ADR's first rule.
pub async fn assert_no_batch(rx: &mut mpsc::Receiver<Delivered>, window: Duration, what: &str) {
    if let Ok(Some(delivered)) = tokio::time::timeout(window, rx.recv()).await {
        let events = unwrap_batch(delivered).events.len();
        panic!("{what}: expected no batch within {window:?}, got one of {events} events");
    }
}

/// Reads one byte, expecting the peer to have closed instead, within [`RECV_TIMEOUT`].
pub async fn expect_closed<S: AsyncRead + Unpin>(stream: &mut S, what: &str) {
    let mut buf = [0u8; 1];
    let result = tokio::time::timeout(RECV_TIMEOUT, stream.read(&mut buf))
        .await
        .unwrap_or_else(|_| panic!("{what}: expected a close within {RECV_TIMEOUT:?}"));
    match result {
        Ok(n) => assert_eq!(n, 0, "{what}: expected a close, got a byte"),
        // A close with bytes still unread in the peer's receive queue is an RST, not a FIN
        // (Linux `tcp_close`), and a read after an RST is `ECONNRESET`: still a close.
        Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => {}
        Err(err) => panic!("{what}: read failed outright: {err}"),
    }
}

/// Asserts a connection stays open for `window`: a peer that never writes leaves the read
/// blocked, while a closed one returns `Ok(0)` or `ECONNRESET` at once. Scheduler lag only
/// lengthens the window.
pub async fn expect_still_open<S: AsyncRead + Unpin>(stream: &mut S, window: Duration, what: &str) {
    let mut buf = [0u8; 1];
    match tokio::time::timeout(window, stream.read(&mut buf)).await {
        Err(_elapsed) => {}
        Ok(Ok(0)) => panic!("{what}: expected the connection to still be open, got a close"),
        Ok(Ok(n)) => panic!("{what}: expected no bytes, got {n}"),
        Ok(Err(err)) => panic!("{what}: expected the connection to still be open, got {err}"),
    }
}

/// An input running on its own task, from [`spawn_input`].
pub struct Running {
    pub shutdown: watch::Sender<bool>,
    pub handle: JoinHandle<anyhow::Result<()>>,
}

impl Running {
    /// Signals shutdown and waits up to [`RECV_TIMEOUT`] for a clean exit, so a hang fails the
    /// test rather than the suite.
    pub async fn stop(self) {
        let _ = self.shutdown.send(true);
        tokio::time::timeout(RECV_TIMEOUT, self.handle)
            .await
            .unwrap_or_else(|_| panic!("the input did not shut down within {RECV_TIMEOUT:?}"))
            .expect("the input task panicked")
            .expect("the input exited with an error");
    }
}

/// Binds `input`, then runs it until shutdown on its own task. Binding first means its sockets
/// and watches exist when this returns, so the test can connect or write at once.
pub async fn spawn_input<I: Input + Send + 'static>(mut input: I, sink: Fanout) -> Running {
    input.bind().await.expect("bind should succeed");
    let (shutdown, rx) = watch::channel(false);
    let handle = tokio::spawn(async move { input.run_until_shutdown(sink, rx).await });
    Running { shutdown, handle }
}

/// Runs the runtime's own `write_loop` over `output` until a closed in-memory queue holding
/// `batches` is drained, with no shutdown. `telemetry` is the runtime's
/// handle for the component, so pass one from the registry the sink counts into to read both.
///
/// Every batch goes through `write_loop`'s real `Output::observe_batch` call site and
/// `deliver_with_retry`, so a test sees the per-attempt and per-batch counts a running pipeline
/// produces. Panics if the loop is still running after [`RECV_TIMEOUT`]: a retryable fault
/// retries until it succeeds, so `output` must succeed or fail with a fault that drops.
pub async fn drive_write_loop<O: crate::Output + Send>(
    output: &mut O,
    batches: Vec<EventBatch>,
    config: crate::WriteLoopConfig,
    telemetry: Telemetry,
) {
    let store = Arc::new(crate::SinkStore::Memory(crate::SinkQueue::new(
        crate::SinkQueueConfig::default(),
        telemetry.clone(),
    )));
    for batch in batches {
        store.push((Arc::new(batch), crate::TraceContext::default().into())).await;
    }
    store.close();
    let (_shutdown_tx, shutdown_rx) = watch::channel(false);
    tokio::time::timeout(
        RECV_TIMEOUT,
        crate::runtime::write_loop(
            "out".to_string(),
            output,
            store,
            telemetry,
            config,
            shutdown_rx,
            &AtomicU64::new(0),
        ),
    )
    .await
    .unwrap_or_else(|_| panic!("write_loop still running after {RECV_TIMEOUT:?}"))
}

static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

/// A new, empty directory at `{tmp}/logit-test-{label}-{pid}-{seq}`. A leftover directory at that
/// path, from an earlier process that had the same pid, is removed first.
pub fn scratch_dir(label: &str) -> PathBuf {
    let seq = SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("logit-test-{label}-{}-{seq}", std::process::id()));
    match std::fs::remove_dir_all(&dir) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => panic!("removing leftover scratch dir {}: {err}", dir.display()),
    }
    std::fs::create_dir_all(&dir).expect("create scratch dir");
    dir
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_sum_accumulates_across_drains() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("in", "statsd_in", "listener");
        telemetry.count("logit.input.events", 2.0, &[]);
        assert_eq!(probe.sum("logit.input.events", &[]), 2.0);
        telemetry.count("logit.input.events", 3.0, &[]);
        assert_eq!(probe.sum("logit.input.events", &[]), 5.0);
    }

    #[test]
    fn a_gauge_survives_an_empty_drain() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("in", "tail_in", "listener");
        telemetry.gauge("logit.input.files.open", 3.0, &[]);
        let mut totals = Totals::of(registry.drain(0));
        assert_eq!(totals.gauge("logit.input.files.open", &[]), Some(3.0));

        let empty = registry.drain(0);
        assert!(empty.is_empty(), "the gauge cleared on the first drain");
        totals.fold(empty);
        assert_eq!(totals.gauge("logit.input.files.open", &[]), Some(3.0));

        telemetry.gauge("logit.input.files.open", 0.0, &[]);
        totals.fold(registry.drain(0));
        assert_eq!(totals.gauge("logit.input.files.open", &[]), Some(0.0));
    }

    #[test]
    fn tags_match_as_a_subset() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("out", "influxdb_out", "sink");
        telemetry.count("logit.output.batches.dropped", 1.0, &[("reason", "shutdown")]);
        telemetry.count("logit.output.batches.dropped", 4.0, &[("reason", "overflow")]);
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.batches.dropped", &[]), 5.0);
        assert_eq!(totals.sum("logit.output.batches.dropped", &[("reason", "overflow")]), 4.0);
        assert_eq!(
            totals.sum(
                "logit.output.batches.dropped",
                &[("reason", "shutdown"), ("component", "out"), ("role", "sink")]
            ),
            1.0
        );
        assert_eq!(totals.sum("logit.output.batches.dropped", &[("reason", "other")]), 0.0);
        assert!(totals.has("logit.output.batches.dropped", &[("kind", "influxdb_out")]));
        assert!(!totals.has("logit.output.batches.dropped", &[("kind", "statsd_out")]));
    }

    #[test]
    #[should_panic(expected = "matches more than one series")]
    fn a_gauge_matching_two_series_panics() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("in", "tail_in", "listener");
        telemetry.gauge("logit.input.files.open", 1.0, &[("path", "a")]);
        telemetry.gauge("logit.input.files.open", 2.0, &[("path", "b")]);
        probe.gauge("logit.input.files.open", &[]);
    }

    #[tokio::test(start_paused = true)]
    #[should_panic(expected = "timed out after 5s waiting for x")]
    async fn wait_until_panics_with_what_it_waited_for() {
        wait_until("x", || false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn wait_for_sees_a_count_made_between_polls() {
        let mut probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("in", "statsd_in", "listener");
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            telemetry.count("logit.input.events", 1.0, &[]);
        });
        let totals = probe.wait_for("one event", |t| t.sum("logit.input.events", &[]) >= 1.0).await;
        assert_eq!(totals.sum("logit.input.events", &[]), 1.0);
    }

    #[test]
    fn scratch_dirs_are_distinct_and_empty() {
        let a = scratch_dir("test-util");
        let b = scratch_dir("test-util");
        assert_ne!(a, b);
        assert_eq!(std::fs::read_dir(&a).unwrap().count(), 0);
        std::fs::remove_dir_all(a).unwrap();
        std::fs::remove_dir_all(b).unwrap();
    }
}
