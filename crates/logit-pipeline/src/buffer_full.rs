//! [`FullWatch`]: the self-log lines and the blocked-push gauge for a bounded queue that fills, a
//! sink's buffer (in memory or on disk) or a datagram listener's receive queue. During an outage
//! the edge's telemetry may not reach its backend, so these lines are the live signal
//! (`docs/adr/buffer-full-diagnostics.md`).
//!
//! An episode opens when a push finds no room: a `block` push starts waiting, or an
//! `overflow_oldest`/`overflow_newest` drop sheds an item. It closes once a consumer-side change
//! leaves the queue's utilization below `recover_below` with no push waiting. The lines:
//!
//! - `buffer_full` (warn): when an episode opens, then at most once per [`REPEAT_INTERVAL`] while
//!   it stays open, with the episode's drop counts or blocked time so far. One clock covers every
//!   episode of one queue, so a queue whose single item fills more than `1 - recover_below` of a
//!   bound, and so opens and closes an episode per item, still writes at most one warn line a
//!   minute. An episode that opens inside that minute stays unannounced until the clock allows a
//!   line; if it closes first, its counts ride on the next `buffer_full` line.
//! - `buffer_recovered` (warn): when an announced episode closes, with its duration and totals.
//! - `buffer_full_at_shutdown` (warn): when the queue's owner finishes it with an announced
//!   episode still open.
//! - `buffer_shed_unannounced` (warn): when the owner finishes it and the episodes no line
//!   announced shed anything, the open one and those whose counts were waiting to ride on the
//!   next `buffer_full`. No later line would carry them, so without it that loss would show only
//!   in the `*.dropped` counters. Episodes that only held intake write nothing.
//!
//! Every line is `warn` so a reader who sees one finds its pair: `internal`'s default `logs: warn`
//! captures only `warn` and above, and a pipeline that got `buffer_full` alone would never see
//! the episode end.
//!
//! On a sink's buffer, a `buffer_full` line written while the write loop has as many batches in
//! flight as its concurrency allows also names that limit and the last send's duration
//! ([`SendLimit`]): the round trip, not the destination's refusal, is what bounds the sink, and
//! raising `concurrency:` is the remedy (`docs/adr/sink-request-concurrency.md`, decision 2).
//!
//! The watch also keeps the store-pressure signal a collecting sink's window closes on
//! (`docs/adr/collecting-sink-window.md`, decision 3). It's raised from the moment a push finds no
//! room, as an episode opens, and also by a push accepted over the bound with nothing evictable,
//! which opens no episode. It clears once a change leaves utilization below 1 with no push
//! waiting, not at `recover_below`.
//!
//! None of them increments `logit.component.diagnostics`: like `write_loop`'s `retrying`, they
//! are paced by time, never suppressed by count. The `*.dropped` counters stay the record of
//! loss; these lines sit beside them.

use crate::queue::OverflowPolicy;
use logit_core::{Diagnostics, Telemetry};
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::futures::Notified;
use tokio::time::Instant;

/// The least time between two `buffer_full` lines for one queue.
pub(crate) const REPEAT_INTERVAL: Duration = Duration::from_secs(60);

/// How often a waiting push re-writes the blocked gauge. `ComponentBuffer::drain` takes every
/// point, so a gauge written once shows in one `internal` drain window and then vanishes. Half the
/// shortest `internal` interval any shipped config uses (`1s`), so each window of that length or
/// longer holds a write.
pub(crate) const GAUGE_REFRESH: Duration = Duration::from_millis(500);

/// How one kind of queue names itself and its bounds in its lines, in the operator's config
/// vocabulary.
pub struct FullLabels {
    /// What the line calls the queue: `buffer` or `receive queue`.
    pub what: &'static str,
    /// The config path of the item-count bound, such as `buffer.max_batches`.
    pub items_bound: &'static str,
    /// The config path of the byte bound, such as `buffer.max_bytes`.
    pub weight_bound: &'static str,
    /// The plural noun for items, such as `batch(es)`.
    pub items: &'static str,
    /// The plural noun for an item's units, such as `event(s)`.
    pub units: &'static str,
}

/// Which bound a push found no room under.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Bound {
    Items,
    Weight,
}

/// Bounds and policy one [`FullWatch`] reports against.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WatchConfig {
    pub policy: OverflowPolicy,
    pub recover_below: f64,
    pub max_items: u64,
    pub max_weight: u64,
}

/// What a sink's write loop is waiting on, for the `buffer_full` line's cause: how many batches
/// it may have in flight at once, how many it has, and how long the last one took from its send
/// to its answer. `write_loop` writes it; the [`FullWatch`] reads it when it writes a line. A
/// queue no write loop drains (a listener's receive queue) leaves it at zero, which adds nothing.
#[derive(Debug, Default)]
pub(crate) struct SendLimit {
    limit: AtomicUsize,
    in_flight: AtomicUsize,
    /// The last answered send's duration in nanoseconds, 0 before the first.
    last_send_nanos: AtomicU64,
}

impl SendLimit {
    /// Records the loop's limit and the batches it's waiting on. A loop backing off before a
    /// retry waits on none: the `retrying` line names that cause.
    pub(crate) fn set(&self, limit: usize, in_flight: usize) {
        self.limit.store(limit, Ordering::Relaxed);
        self.in_flight.store(in_flight, Ordering::Relaxed);
    }

    /// Records one answered send's duration, its answer a success or a failure.
    pub(crate) fn answered(&self, took: Duration) {
        let nanos = u64::try_from(took.as_nanos()).unwrap_or(u64::MAX).max(1);
        self.last_send_nanos.store(nanos, Ordering::Relaxed);
    }

    /// The `buffer_full` line's suffix when every batch the limit allows is in flight.
    pub(crate) fn cause(&self) -> Option<String> {
        let limit = self.limit.load(Ordering::Relaxed);
        if limit == 0 || self.in_flight.load(Ordering::Relaxed) < limit {
            return None;
        }
        Some(match self.last_send_nanos.load(Ordering::Relaxed) {
            0 => format!("; sending at its concurrency of {limit}, no send answered yet"),
            nanos => format!(
                "; sending at its concurrency of {limit}, the last send took {}",
                fmt_duration(Duration::from_nanos(nanos))
            ),
        })
    }
}

/// Totals for one episode, or for the unannounced episodes since the last `buffer_full` line.
#[derive(Debug, Default, Clone, Copy)]
struct Totals {
    episodes: u64,
    items: u64,
    units: u64,
    blocked: Duration,
}

impl Totals {
    fn add(&mut self, other: &Totals) {
        self.episodes += other.episodes;
        self.items += other.items;
        self.units += other.units;
        self.blocked += other.blocked;
    }
}

#[derive(Debug)]
struct Episode {
    started: Instant,
    bound: Bound,
    totals: Totals,
    /// The `reason` of the latest drop, which names what the queue sheds.
    shed: Option<&'static str>,
    /// Set while a push waits; its time joins `totals.blocked` when the last waiter leaves.
    blocked_since: Option<Instant>,
    announced: bool,
}

impl Episode {
    fn blocked_so_far(&self, now: Instant) -> Duration {
        self.totals.blocked + self.blocked_since.map_or(Duration::ZERO, |s| now - s)
    }
}

#[derive(Debug, Default)]
struct State {
    episode: Option<Episode>,
    last_warn: Option<Instant>,
    unannounced: Totals,
}

/// One queue's full-buffer episode, its lines, and its blocked-push gauge. Shared by the
/// producer and the consumer through `&self`.
pub(crate) struct FullWatch {
    telemetry: Telemetry,
    diag: Diagnostics,
    labels: &'static FullLabels,
    blocked_gauge: &'static str,
    config: WatchConfig,
    /// Mirrors `state.episode.is_some()`, so the consumer's per-pop check costs one atomic load
    /// while no episode is open.
    open: AtomicBool,
    /// Pushes waiting for room. Production queues have one producer, so this is 0 or 1.
    waiters: AtomicUsize,
    state: Mutex<State>,
    send_limit: SendLimit,
    /// The store-pressure signal (the module doc).
    pressure: AtomicBool,
    /// Woken, every waiter, when `pressure` goes from clear to raised.
    pressure_raised: tokio::sync::Notify,
}

/// Held by a push for as long as it waits for room. Dropping it, on room, close, or the push
/// future's cancellation, lowers the blocked gauge and adds the wait to the episode.
pub(crate) struct Waiting<'a> {
    watch: &'a FullWatch,
}

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let watch = self.watch;
        if watch.waiters.fetch_sub(1, Ordering::AcqRel) != 1 {
            return;
        }
        watch.telemetry.gauge(watch.blocked_gauge, 0.0, &[]);
        let now = Instant::now();
        let mut state = watch.lock();
        if let Some(episode) = state.episode.as_mut() {
            if let Some(since) = episode.blocked_since.take() {
                episode.totals.blocked += now - since;
            }
        }
    }
}

/// A self-log line: its key and message. Every line of an episode is `warn`, so `internal`'s
/// default `logs: warn` captures the opening and the close of one.
struct Line(&'static str, String);

impl FullWatch {
    pub(crate) fn new(
        config: WatchConfig,
        labels: &'static FullLabels,
        blocked_gauge: &'static str,
        telemetry: Telemetry,
        diag: Diagnostics,
    ) -> Self {
        Self {
            telemetry,
            diag,
            labels,
            blocked_gauge,
            config,
            open: AtomicBool::new(false),
            waiters: AtomicUsize::new(0),
            state: Mutex::new(State::default()),
            send_limit: SendLimit::default(),
            pressure: AtomicBool::new(false),
            pressure_raised: tokio::sync::Notify::new(),
        }
    }

    /// The write loop's side of the `buffer_full` line's cause.
    pub(crate) fn send_limit(&self) -> &SendLimit {
        &self.send_limit
    }

    /// Whether the store-pressure signal is raised.
    pub(crate) fn under_pressure(&self) -> bool {
        self.pressure.load(Ordering::Acquire)
    }

    /// Resolves once the store-pressure signal is next raised. Created before a check of
    /// [`FullWatch::under_pressure`], it can't miss a raise that lands after that check.
    pub(crate) fn pressure_raised(&self) -> Notified<'_> {
        self.pressure_raised.notified()
    }

    /// Records a push accepted over a bound with nothing evictable: it raises the pressure signal
    /// and opens no episode.
    pub(crate) fn over_bound(&self) {
        self.raise_pressure();
    }

    /// Clears the pressure signal once `ratio`, the queue's utilization after a change, is below 1
    /// with no push waiting.
    pub(crate) fn ease_pressure(&self, ratio: f64) {
        if ratio < 1.0
            && self.waiters.load(Ordering::Acquire) == 0
            && self.pressure.load(Ordering::Acquire)
        {
            self.pressure.store(false, Ordering::Release);
        }
    }

    fn raise_pressure(&self) {
        if !self.pressure.swap(true, Ordering::AcqRel) {
            self.pressure_raised.notify_waiters();
        }
    }

    pub(crate) fn set_diagnostics(&mut self, diag: Diagnostics) {
        self.diag = diag;
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Records one item an overflow policy shed, `reason` being `overflow_oldest` or
    /// `overflow_newest`. Opens an episode if none is open.
    pub(crate) fn dropped(&self, reason: &'static str, units: u64, bound: Bound) {
        self.raise_pressure();
        let now = Instant::now();
        let line = {
            let mut state = self.lock();
            let episode = self.open_episode(&mut state, now, bound);
            episode.totals.items += 1;
            episode.totals.units = episode.totals.units.saturating_add(units);
            episode.shed = Some(reason);
            self.due_warn(&mut state, now)
        };
        self.emit(line);
    }

    /// Marks a push as waiting for room under `bound`, until the returned guard drops. Raises the
    /// blocked gauge and opens an episode if none is open.
    pub(crate) fn waiting(&self, bound: Bound) -> Waiting<'_> {
        if self.waiters.fetch_add(1, Ordering::AcqRel) == 0 {
            self.telemetry.gauge(self.blocked_gauge, 1.0, &[]);
        }
        self.raise_pressure();
        let now = Instant::now();
        let line = {
            let mut state = self.lock();
            let episode = self.open_episode(&mut state, now, bound);
            episode.blocked_since.get_or_insert(now);
            self.due_warn(&mut state, now)
        };
        self.emit(line);
        Waiting { watch: self }
    }

    /// Awaits `notified`, or at most [`GAUGE_REFRESH`]. On that tick it re-writes the blocked
    /// gauge, which a drain removes, and writes a `buffer_full` repeat if one is due, so a push
    /// that waits for hours keeps the gauge at 1 in every drain window and still logs once a
    /// minute. A tick changes nothing else: the caller's loop re-checks the queue as it does after
    /// any wakeup. The deadline is always [`GAUGE_REFRESH`] ahead of now, never a past due time,
    /// so a repeat that isn't written can't make the loop spin. With no tokio runtime in context
    /// (a test polling by hand), it awaits `notified` alone.
    pub(crate) async fn wait(&self, notified: Notified<'_>) {
        if tokio::runtime::Handle::try_current().is_err() {
            notified.await;
            return;
        }
        let deadline = Instant::now() + GAUGE_REFRESH;
        if tokio::time::timeout_at(deadline, notified).await.is_err() {
            if self.waiters.load(Ordering::Acquire) > 0 {
                self.telemetry.gauge(self.blocked_gauge, 1.0, &[]);
            }
            let now = Instant::now();
            let line = self.due_warn(&mut self.lock(), now);
            self.emit(line);
        }
    }

    /// Checks the consumer side's new utilization, `max(items ratio, bytes ratio)`, and closes
    /// the episode once it is below `recover_below` with no push waiting. Clears the pressure
    /// signal below 1 ([`FullWatch::ease_pressure`]). Two atomic loads while no episode is open
    /// and no pressure is raised.
    pub(crate) fn utilization(&self, ratio: f64) {
        self.ease_pressure(ratio);
        if !self.open.load(Ordering::Acquire)
            || ratio >= self.config.recover_below
            || self.waiters.load(Ordering::Acquire) > 0
        {
            return;
        }
        let now = Instant::now();
        let line = {
            let mut state = self.lock();
            let Some(episode) = state.episode.take() else { return };
            self.open.store(false, Ordering::Release);
            if episode.announced {
                Some(Line(
                    "buffer_recovered",
                    format!(
                        "{} below {}% of its bounds after {}; overflow {} {}",
                        self.labels.what,
                        fmt_percent(self.config.recover_below),
                        fmt_duration(now - episode.started),
                        self.config.policy.as_str(),
                        self.what_happened(&episode, now, true),
                    ),
                ))
            } else {
                let mut totals = episode.totals;
                totals.blocked = episode.blocked_so_far(now);
                totals.episodes = 1;
                state.unannounced.add(&totals);
                None
            }
        };
        self.emit(line);
    }

    /// Closes an open episode at shutdown, after the last push. Writes
    /// `buffer_full_at_shutdown` if the episode was announced, else
    /// `buffer_shed_unannounced` if the episodes no line announced shed anything: the open one,
    /// and any that closed inside the throttle's minute and were waiting to ride on a later
    /// `buffer_full`. Without it, a queue that sheds within a minute of its last line and then
    /// shuts down would report that loss in its `*.dropped` counters alone.
    pub(crate) fn finish(&self) {
        let now = Instant::now();
        let line = {
            let mut state = self.lock();
            let open = state.episode.take();
            self.open.store(false, Ordering::Release);
            let mut unannounced = std::mem::take(&mut state.unannounced);
            match open {
                Some(episode) if episode.announced => Some(Line(
                    "buffer_full_at_shutdown",
                    format!(
                        "{} still full at shutdown after {}; overflow {} {}",
                        self.labels.what,
                        fmt_duration(now - episode.started),
                        self.config.policy.as_str(),
                        self.what_happened(&episode, now, true),
                    ),
                )),
                open => {
                    let still_open = open.is_some();
                    if let Some(episode) = open {
                        let mut totals = episode.totals;
                        totals.blocked = episode.blocked_so_far(now);
                        totals.episodes = 1;
                        unannounced.add(&totals);
                    }
                    (unannounced.items > 0).then(|| {
                        Line(
                            "buffer_shed_unannounced",
                            format!(
                                "{} full in {} episode(s) no earlier line reported{}; overflow \
                                 {} {}",
                                self.labels.what,
                                unannounced.episodes,
                                if still_open { ", the last still open at shutdown" } else { "" },
                                self.config.policy.as_str(),
                                self.totals_text(&unannounced),
                            ),
                        )
                    })
                }
            }
        };
        self.emit(line);
    }

    fn open_episode<'s>(
        &self,
        state: &'s mut State,
        now: Instant,
        bound: Bound,
    ) -> &'s mut Episode {
        let episode = state.episode.get_or_insert_with(|| {
            self.open.store(true, Ordering::Release);
            Episode {
                started: now,
                bound,
                totals: Totals::default(),
                shed: None,
                blocked_since: None,
                announced: false,
            }
        });
        episode
    }

    /// The `buffer_full` line, if one is due: an episode is open and no line was written in the
    /// last [`REPEAT_INTERVAL`]. Marks it written.
    fn due_warn(&self, state: &mut State, now: Instant) -> Option<Line> {
        if state.last_warn.is_some_and(|last| now - last < REPEAT_INTERVAL) {
            return None;
        }
        let episode = state.episode.as_ref()?;
        let mut msg = String::new();
        let bound = self.describe_bound(episode.bound);
        let elapsed = now - episode.started;
        if !episode.announced && elapsed.is_zero() {
            let _ = write!(
                msg,
                "{} full ({bound}); overflow {} {}",
                self.labels.what,
                self.config.policy.as_str(),
                self.what_happened(episode, now, false)
            );
        } else {
            let _ = write!(
                msg,
                "{} {} for {} ({bound}); overflow {} {}",
                self.labels.what,
                if episode.announced { "still full" } else { "full" },
                fmt_duration(elapsed),
                self.config.policy.as_str(),
                self.what_happened(episode, now, true)
            );
        }
        if let Some(cause) = self.send_limit.cause() {
            msg.push_str(&cause);
        }
        let earlier = std::mem::take(&mut state.unannounced);
        if earlier.episodes > 0 {
            let _ = write!(
                msg,
                "; {} shorter episode(s) since the last line {}",
                earlier.episodes,
                self.totals_text(&earlier)
            );
        }
        state.last_warn = Some(now);
        if let Some(episode) = state.episode.as_mut() {
            episode.announced = true;
        }
        Some(Line("buffer_full", msg))
    }

    /// What the episode did: `is holding intake` or `is shedding ...` as it opens, and its totals
    /// afterward.
    fn what_happened(&self, episode: &Episode, now: Instant, totals: bool) -> String {
        if !totals {
            return match episode.shed {
                Some("overflow_oldest") => format!("is shedding the oldest {}", self.labels.items),
                Some(_) => format!("is shedding new {}", self.labels.items),
                None => "is holding intake".to_string(),
            };
        }
        let mut t = episode.totals;
        t.blocked = episode.blocked_so_far(now);
        self.totals_text(&t)
    }

    fn totals_text(&self, t: &Totals) -> String {
        let mut parts = Vec::with_capacity(2);
        if t.items > 0 {
            parts.push(format!(
                "shed {} {} ({} {})",
                t.items, self.labels.items, t.units, self.labels.units
            ));
        }
        if !t.blocked.is_zero() || t.items == 0 {
            parts.push(format!("held intake for {}", fmt_duration(t.blocked)));
        }
        parts.join(" and ")
    }

    fn describe_bound(&self, bound: Bound) -> String {
        match bound {
            Bound::Items => format!("{} {}", self.labels.items_bound, self.config.max_items),
            Bound::Weight => {
                format!("{} {}", self.labels.weight_bound, fmt_bytes(self.config.max_weight))
            }
        }
    }

    fn emit(&self, line: Option<Line>) {
        if let Some(Line(key, msg)) = line {
            self.diag.warn_keyed(key, msg);
        }
    }
}

/// A byte count in the form a config writes it: `64MiB`, `1GiB`, or a plain number.
pub(crate) fn fmt_bytes(bytes: u64) -> String {
    const KIB: u64 = 1024;
    const MIB: u64 = KIB * 1024;
    const GIB: u64 = MIB * 1024;
    match bytes {
        0 => "0".to_string(),
        b if b % GIB == 0 => format!("{}GiB", b / GIB),
        b if b % MIB == 0 => format!("{}MiB", b / MIB),
        b if b % KIB == 0 => format!("{}KiB", b / KIB),
        b => b.to_string(),
    }
}

/// A fraction as a percentage with at most four decimals and no float noise: `0.57` reads `57`,
/// not `56.99999999999999`.
fn fmt_percent(fraction: f64) -> String {
    let percent = (fraction * 100.0 * 10_000.0).round() / 10_000.0;
    format!("{percent}")
}

/// `4h15m0s`, `2m30s`, `45s`, or `250ms` under a second.
fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs();
    if secs == 0 {
        return format!("{}ms", d.as_millis());
    }
    let (h, m, s) = (secs / 3600, secs / 60 % 60, secs % 60);
    if h > 0 {
        format!("{h}h{m}m{s}s")
    } else if m > 0 {
        format!("{m}m{s}s")
    } else {
        format!("{s}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::capture_lines as capture;

    static LABELS: FullLabels = FullLabels {
        what: "buffer",
        items_bound: "buffer.max_batches",
        weight_bound: "buffer.max_bytes",
        items: "batch(es)",
        units: "event(s)",
    };
    const GAUGE: &str = "logit.component.buffer.push.blocked";

    fn watch(policy: OverflowPolicy, telemetry: Telemetry) -> FullWatch {
        FullWatch::new(
            WatchConfig { policy, recover_below: 0.75, max_items: 8, max_weight: 64 * 1024 * 1024 },
            &LABELS,
            GAUGE,
            telemetry,
            Diagnostics::new("w"),
        )
    }

    #[test]
    fn bytes_and_durations_read_as_an_operator_writes_them() {
        assert_eq!(fmt_bytes(64 * 1024 * 1024), "64MiB");
        assert_eq!(fmt_bytes(1024 * 1024 * 1024), "1GiB");
        assert_eq!(fmt_bytes(1000), "1000");
        assert_eq!(fmt_bytes(2048), "2KiB");
        assert_eq!(fmt_duration(Duration::from_millis(250)), "250ms");
        assert_eq!(fmt_duration(Duration::from_secs(45)), "45s");
        assert_eq!(fmt_duration(Duration::from_secs(150)), "2m30s");
        assert_eq!(fmt_duration(Duration::from_secs(15_300)), "4h15m0s");
    }

    /// With every batch the write loop may send at once in flight, the line names the limit and
    /// the last send's duration; below the limit, or in a backoff, it names neither.
    #[tokio::test]
    async fn a_full_line_names_the_concurrency_only_when_every_send_is_in_flight() {
        for (limit, in_flight, answered, cause) in [
            (1, 1, Some(200), Some("; sending at its concurrency of 1, the last send took 200ms")),
            (8, 8, Some(1500), Some("; sending at its concurrency of 8, the last send took 1s")),
            (1, 1, None, Some("; sending at its concurrency of 1, no send answered yet")),
            (8, 7, Some(200), None),
            (1, 0, Some(200), None),
            (0, 0, None, None),
        ] {
            let (lines, _guard) = capture();
            let w = watch(OverflowPolicy::DropNewest, Telemetry::default());
            w.send_limit().set(limit, in_flight);
            if let Some(ms) = answered {
                w.send_limit().answered(Duration::from_millis(ms));
            }
            w.dropped("overflow_newest", 1, Bound::Items);
            let full = lines.with_key("buffer_full");
            let base = "buffer full (buffer.max_batches 8); overflow drop_newest is shedding new \
                        batch(es)";
            assert_eq!(
                full[0].1,
                format!("{base}{}", cause.unwrap_or_default()),
                "{limit}/{in_flight}"
            );
        }
    }

    /// `internal`'s default `logs: warn` captures only `warn` and above, so every line of an
    /// episode must be `warn` or the pipeline sees an episode open and never close.
    #[tokio::test]
    async fn every_line_of_an_episode_logs_at_warn() {
        let (lines, _guard) = capture();
        let w = watch(OverflowPolicy::DropNewest, Telemetry::default());
        w.dropped("overflow_newest", 1, Bound::Items);
        w.utilization(0.5);
        let closing = watch(OverflowPolicy::DropNewest, Telemetry::default());
        closing.dropped("overflow_newest", 1, Bound::Items);
        closing.finish();
        for key in ["buffer_full", "buffer_recovered", "buffer_full_at_shutdown"] {
            let found = lines.with_key(key);
            assert!(!found.is_empty(), "{key} was not written");
            assert!(found.iter().all(|(level, _)| *level == tracing::Level::WARN), "{found:?}");
        }
    }

    /// An episode the throttle left unannounced, open at shutdown or closed and waiting to ride on
    /// a later line, writes `buffer_shed_unannounced` from `finish` when it shed anything; one that
    /// only held intake writes nothing.
    #[tokio::test(start_paused = true)]
    async fn finish_reports_what_unannounced_episodes_shed() {
        let (lines, _guard) = capture();
        let w = watch(OverflowPolicy::DropNewest, Telemetry::default());
        w.dropped("overflow_newest", 3, Bound::Items);
        w.utilization(0.5);
        // Inside the minute: two episodes no line announces, the second still open at shutdown.
        w.dropped("overflow_newest", 4, Bound::Items);
        w.utilization(0.5);
        w.dropped("overflow_newest", 5, Bound::Items);
        w.finish();
        assert!(lines.with_key("buffer_full_at_shutdown").is_empty());
        let shed = lines.with_key("buffer_shed_unannounced");
        assert_eq!(
            shed,
            vec![(
                tracing::Level::WARN,
                "buffer full in 2 episode(s) no earlier line reported, the last still open at \
                 shutdown; overflow drop_newest shed 2 batch(es) (9 event(s))"
                    .to_string()
            )]
        );

        // Closed and unannounced, nothing open at shutdown.
        let closed = watch(OverflowPolicy::DropOldest, Telemetry::default());
        closed.dropped("overflow_oldest", 1, Bound::Items);
        closed.utilization(0.5);
        closed.dropped("overflow_oldest", 2, Bound::Items);
        closed.utilization(0.5);
        closed.finish();
        let shed = lines.with_key("buffer_shed_unannounced");
        assert_eq!(shed.len(), 2);
        assert_eq!(
            shed[1].1,
            "buffer full in 1 episode(s) no earlier line reported; overflow drop_oldest shed 1 \
             batch(es) (2 event(s))"
        );

        // An unannounced episode that only held intake lost nothing.
        let held = watch(OverflowPolicy::Block, Telemetry::default());
        held.dropped("overflow_newest", 1, Bound::Items);
        held.utilization(0.5);
        drop(held.waiting(Bound::Items));
        held.finish();
        assert_eq!(lines.with_key("buffer_shed_unannounced").len(), 2);
    }

    /// `0.57 * 100.0` is `56.99999999999999` in `f64`; the line reads `57%`.
    #[tokio::test]
    async fn the_recovered_line_prints_recover_below_without_float_noise() {
        let (lines, _guard) = capture();
        let mut w = watch(OverflowPolicy::DropNewest, Telemetry::default());
        w.config.recover_below = 0.57;
        w.dropped("overflow_newest", 1, Bound::Items);
        w.utilization(0.5);
        let recovered = lines.with_key("buffer_recovered");
        assert_eq!(recovered.len(), 1);
        assert!(
            recovered[0].1.starts_with("buffer below 57% of its bounds after "),
            "{recovered:?}"
        );
        assert_eq!(fmt_percent(0.755), "75.5");
    }

    /// A queue whose one item fills more than a quarter of a bound opens and closes an episode
    /// per item. However many it opens in a minute, it writes one `buffer_full` line, and the
    /// closed, unannounced ones' counts ride on the next.
    #[tokio::test(start_paused = true)]
    async fn episodes_that_flap_inside_a_minute_write_one_warn_line_and_carry_their_counts() {
        let (lines, _guard) = capture();
        let w = watch(OverflowPolicy::DropNewest, Telemetry::default());
        w.dropped("overflow_newest", 3, Bound::Weight);
        w.utilization(0.5);
        for _ in 0..10 {
            w.dropped("overflow_newest", 3, Bound::Weight);
            w.utilization(0.5);
        }
        assert_eq!(lines.with_key("buffer_full").len(), 1, "one line in the first minute");
        assert_eq!(lines.with_key("buffer_recovered").len(), 1, "only the announced episode");

        tokio::time::advance(REPEAT_INTERVAL).await;
        w.dropped("overflow_newest", 3, Bound::Weight);
        let full = lines.with_key("buffer_full");
        assert_eq!(full.len(), 2);
        assert_eq!(
            full[1].1,
            "buffer full (buffer.max_bytes 64MiB); overflow drop_newest is shedding new batch(es); \
             10 shorter episode(s) since the last line shed 10 batch(es) (30 event(s))"
        );
    }
}
