//! `has_stale`/`drop_stale`: filter events by their age against the wall clock, so replayed
//! backlog never reaches `aggregate` or another consumer that keeps state across events
//! (`docs/adr/stale-event-filtering.md`).
//!
//! **The instant judged** is the span's `end_timestamp` when the event has a span and that field
//! is non-zero, else `event.timestamp`. An instant of 0 is unknown: the event is never stale,
//! `drop_stale` forwards it, and `has_stale` discards it.
//!
//! **Stale** means `now - instant > max_age` (an age equal to `max_age` is fresh), or, with
//! `max_ahead` set, `instant - now > max_ahead`. `now` is the wall clock, read once per batch on
//! the first event and cleared in `end_batch`.
//!
//! **The pair partitions every event**: `drop_stale` forwards what `has_stale` discards, given the
//! same bounds. Payloads are never stripped.
//!
//! **Accounting.** The runtime counts each discard as `logit.component.events.discarded`. Both
//! kinds add `logit.transform.stale.exceeded{bound}` per stale event and
//! `logit.transform.stale.unjudged` per unknown-time event, emitted per batch and only when
//! nonzero. `drop_stale` also warns through the throttled diagnostic `stale_event`, naming the
//! observed age and the bound crossed.

use logit_core::{Diagnostics, Event, Resource, Telemetry};
use logit_pipeline::Transform;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type Clock = Box<dyn Fn() -> i64 + Send + Sync>;

fn wall_clock_nanos() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

fn nanos(d: Duration) -> i64 {
    i64::try_from(d.as_nanos()).unwrap_or(i64::MAX)
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Verdict {
    Fresh,
    Unjudged,
    TooOld { age: i64 },
    TooFarAhead { ahead: i64 },
}

/// The judgment and per-batch state shared by [`HasStale`] and [`DropStale`].
struct Judge {
    max_age: i64,
    max_ahead: Option<i64>,
    clock: Clock,
    /// Read lazily on a batch's first event, cleared by `end_batch`.
    now: Option<i64>,
    telemetry: Telemetry,
    exceeded_age: u64,
    exceeded_ahead: u64,
    unjudged: u64,
}

impl Judge {
    fn new(max_age: Duration, max_ahead: Option<Duration>) -> Self {
        Self {
            max_age: nanos(max_age),
            max_ahead: max_ahead.map(nanos),
            clock: Box::new(wall_clock_nanos),
            now: None,
            telemetry: Telemetry::default(),
            exceeded_age: 0,
            exceeded_ahead: 0,
            unjudged: 0,
        }
    }

    fn judge(&mut self, event: &Event) -> Verdict {
        let instant = match &event.span {
            Some(span) if span.end_timestamp != 0 => span.end_timestamp,
            _ => event.timestamp,
        };
        if instant == 0 {
            self.unjudged += 1;
            return Verdict::Unjudged;
        }
        let now = match self.now {
            Some(now) => now,
            None => {
                let now = (self.clock)();
                self.now = Some(now);
                now
            }
        };
        let age = i128::from(now) - i128::from(instant);
        if age > i128::from(self.max_age) {
            self.exceeded_age += 1;
            return Verdict::TooOld { age: i64::try_from(age).unwrap_or(i64::MAX) };
        }
        if let Some(max_ahead) = self.max_ahead {
            let ahead = -age;
            if ahead > i128::from(max_ahead) {
                self.exceeded_ahead += 1;
                return Verdict::TooFarAhead { ahead: i64::try_from(ahead).unwrap_or(i64::MAX) };
            }
        }
        Verdict::Fresh
    }

    fn end_batch(&mut self) {
        self.now = None;
        for (bound, n) in
            [("max_age", &mut self.exceeded_age), ("max_ahead", &mut self.exceeded_ahead)]
        {
            if *n > 0 {
                self.telemetry.count(
                    "logit.transform.stale.exceeded",
                    *n as f64,
                    &[("bound", bound)],
                );
                *n = 0;
            }
        }
        if self.unjudged > 0 {
            self.telemetry.count("logit.transform.stale.unjudged", self.unjudged as f64, &[]);
            self.unjudged = 0;
        }
    }
}

/// Forwards a stale event and discards the rest. See the module doc.
pub struct HasStale {
    judge: Judge,
}

impl HasStale {
    /// `max_ahead` of `None` leaves the future bound off.
    pub fn new(max_age: Duration, max_ahead: Option<Duration>) -> Self {
        Self { judge: Judge::new(max_age, max_ahead) }
    }

    /// Counts `logit.transform.stale.*` into `telemetry`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.judge.telemetry = telemetry;
        self
    }

    /// Replaces the wall clock, which reads Unix nanoseconds; for tests.
    pub fn with_clock(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.judge.clock = Box::new(clock);
        self
    }
}

impl Transform for HasStale {
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        matches!(self.judge.judge(event), Verdict::TooOld { .. } | Verdict::TooFarAhead { .. })
    }

    fn end_batch(&mut self) {
        self.judge.end_batch();
    }
}

/// Forwards a fresh or unjudged event and discards the stale ones. See the module doc.
pub struct DropStale {
    judge: Judge,
    diag: Diagnostics,
}

impl DropStale {
    /// `max_ahead` of `None` leaves the future bound off.
    pub fn new(max_age: Duration, max_ahead: Option<Duration>) -> Self {
        Self { judge: Judge::new(max_age, max_ahead), diag: Diagnostics::default() }
    }

    /// Counts `logit.transform.stale.*` into `telemetry`.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.judge.telemetry = telemetry;
        self
    }

    /// Routes the `stale_event` warning through `diag`.
    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Replaces the wall clock, which reads Unix nanoseconds; for tests.
    pub fn with_clock(mut self, clock: impl Fn() -> i64 + Send + Sync + 'static) -> Self {
        self.judge.clock = Box::new(clock);
        self
    }
}

impl Transform for DropStale {
    fn process(&mut self, _resource: &Arc<Resource>, event: &mut Event) -> bool {
        match self.judge.judge(event) {
            Verdict::Fresh | Verdict::Unjudged => true,
            Verdict::TooOld { age } => {
                self.diag.warn_throttled(
                    "stale_event",
                    format_args!(
                        "discarding an event {}s old, past max_age ({}s)",
                        age / 1_000_000_000,
                        self.judge.max_age / 1_000_000_000
                    ),
                );
                false
            }
            Verdict::TooFarAhead { ahead } => {
                self.diag.warn_throttled(
                    "stale_event",
                    format_args!(
                        "discarding an event {}s in the future, past max_ahead ({}s)",
                        ahead / 1_000_000_000,
                        self.judge.max_ahead.unwrap_or(0) / 1_000_000_000
                    ),
                );
                false
            }
        }
    }

    fn end_batch(&mut self) {
        self.judge.end_batch();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{
        AttrMap, BodyFormat, EventBatch, LogRecord, Registry, SpanKind, SpanRecord, SpanStatus,
        Value,
    };
    use logit_pipeline::runtime::process_batch;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const NOW: i64 = 1_000_000 * 1_000_000_000;
    const MAX_AGE: Duration = Duration::from_secs(300);
    const MAX_AHEAD: Duration = Duration::from_secs(60);

    fn secs(s: i64) -> i64 {
        s * 1_000_000_000
    }

    fn log_at(timestamp: i64) -> Event {
        Event::log(
            timestamp,
            AttrMap::new(),
            LogRecord {
                message: Value::str("msg"),
                severity: None,
                body_format: BodyFormat::Raw,
                trace: None,
                event_name: None,
                observed_timestamp: 0,
                dropped_attributes_count: 0,
            },
        )
    }

    fn span_event(start: i64, end: i64) -> Event {
        Event::span(
            start,
            AttrMap::new(),
            SpanRecord {
                trace_id: [0; 16],
                span_id: [0; 8],
                parent_span_id: None,
                name: Value::str("span"),
                kind: SpanKind::Internal,
                status: SpanStatus::Unset,
                events: vec![],
                links: vec![],
                end_timestamp: end,
                flags: 0,
                ext: None,
            },
        )
    }

    fn resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    fn has(max_ahead: Option<Duration>) -> HasStale {
        HasStale::new(MAX_AGE, max_ahead).with_clock(|| NOW)
    }

    fn drop_(max_ahead: Option<Duration>) -> DropStale {
        DropStale::new(MAX_AGE, max_ahead).with_clock(|| NOW)
    }

    fn keeps(t: &mut impl Transform, ev: Event) -> bool {
        let mut ev = ev;
        t.process(&resource(), &mut ev)
    }

    fn counter(events: &[Event], name: &str, tag: Option<(&str, &str)>) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                logit_core::MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name) == name
                        && tag.is_none_or(|(k, v)| e.attributes.get(k) == Some(&Value::str(v))) =>
                {
                    Some(sum.value.as_f64())
                }
                _ => None,
            })
        })
    }

    #[test]
    fn a_fresh_event_is_forwarded_by_drop_and_discarded_by_has() {
        let ev = || log_at(NOW - secs(10));
        assert!(keeps(&mut drop_(None), ev()));
        assert!(!keeps(&mut has(None), ev()));
    }

    #[test]
    fn a_stale_event_is_discarded_by_drop_and_forwarded_by_has() {
        let ev = || log_at(NOW - secs(301));
        assert!(!keeps(&mut drop_(None), ev()));
        assert!(keeps(&mut has(None), ev()));
    }

    #[test]
    fn an_age_equal_to_max_age_is_fresh() {
        let ev = || log_at(NOW - secs(300));
        assert!(keeps(&mut drop_(None), ev()));
        assert!(!keeps(&mut has(None), ev()));
        let ev = || log_at(NOW - secs(300) - 1);
        assert!(!keeps(&mut drop_(None), ev()));
    }

    #[test]
    fn a_future_event_is_fresh_without_max_ahead() {
        let ev = || log_at(NOW + secs(3600));
        assert!(keeps(&mut drop_(None), ev()));
        assert!(!keeps(&mut has(None), ev()));
    }

    #[test]
    fn a_future_event_past_max_ahead_is_stale_and_equal_is_fresh() {
        let past = || log_at(NOW + secs(61));
        assert!(!keeps(&mut drop_(Some(MAX_AHEAD)), past()));
        assert!(keeps(&mut has(Some(MAX_AHEAD)), past()));
        let equal = || log_at(NOW + secs(60));
        assert!(keeps(&mut drop_(Some(MAX_AHEAD)), equal()));
        assert!(!keeps(&mut has(Some(MAX_AHEAD)), equal()));
    }

    #[test]
    fn a_span_is_judged_by_its_end() {
        let ev = || span_event(NOW - secs(1000), NOW - secs(5));
        assert!(keeps(&mut drop_(None), ev()), "start is stale but end is fresh");
        assert!(!keeps(&mut has(None), ev()));
        let ev = || span_event(NOW - secs(5), NOW - secs(1000));
        assert!(!keeps(&mut drop_(None), ev()), "end is stale");
    }

    #[test]
    fn a_span_with_no_end_falls_back_to_the_event_timestamp() {
        assert!(!keeps(&mut drop_(None), span_event(NOW - secs(1000), 0)));
        assert!(keeps(&mut drop_(None), span_event(NOW - secs(5), 0)));
    }

    #[test]
    fn a_zero_timestamp_is_unjudged() {
        assert!(keeps(&mut drop_(Some(MAX_AHEAD)), log_at(0)));
        assert!(!keeps(&mut has(Some(MAX_AHEAD)), log_at(0)));
    }

    #[test]
    fn the_pair_partitions_a_mixed_batch() {
        let stamps = [
            0,
            NOW,
            NOW - secs(300),
            NOW - secs(301),
            NOW - secs(86_400),
            NOW + secs(60),
            NOW + secs(61),
            i64::MIN + 1,
            i64::MAX,
        ];
        let mut d = drop_(Some(MAX_AHEAD));
        let mut h = has(Some(MAX_AHEAD));
        for t in stamps {
            let kept = keeps(&mut d, log_at(t));
            let archived = keeps(&mut h, log_at(t));
            assert_ne!(kept, archived, "timestamp {t} went through both or neither");
        }
    }

    #[test]
    fn the_clock_is_read_once_per_batch() {
        let reads = Arc::new(AtomicUsize::new(0));
        let counting = reads.clone();
        let mut d = DropStale::new(MAX_AGE, None).with_clock(move || {
            counting.fetch_add(1, Ordering::SeqCst);
            NOW
        });
        for _ in 0..2 {
            for _ in 0..3 {
                keeps(&mut d, log_at(NOW));
            }
            d.end_batch();
        }
        assert_eq!(reads.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn a_batch_of_unjudged_events_reads_no_clock() {
        let reads = Arc::new(AtomicUsize::new(0));
        let counting = reads.clone();
        let mut d = DropStale::new(MAX_AGE, None).with_clock(move || {
            counting.fetch_add(1, Ordering::SeqCst);
            NOW
        });
        keeps(&mut d, log_at(0));
        assert_eq!(reads.load(Ordering::SeqCst), 0);
    }

    fn mixed_batch() -> EventBatch {
        EventBatch {
            resource: resource(),
            scope: None,
            events: vec![
                log_at(NOW),
                log_at(NOW - secs(1000)),
                log_at(NOW - secs(2000)),
                log_at(NOW + secs(500)),
                log_at(0),
            ],
        }
    }

    #[test]
    fn the_exceeded_and_unjudged_counters_carry_the_bound() {
        for kind in ["drop_stale", "has_stale"] {
            let registry = Registry::new();
            let telemetry = registry.telemetry_for("filter", kind, "transform");
            let mut t: Box<dyn Transform + Send> = if kind == "drop_stale" {
                Box::new(drop_(Some(MAX_AHEAD)).with_telemetry(telemetry.clone()))
            } else {
                Box::new(has(Some(MAX_AHEAD)).with_telemetry(telemetry.clone()))
            };
            process_batch(&mut *t, mixed_batch(), &mut Vec::new(), &telemetry);
            let events = registry.drain(0);
            assert_eq!(
                counter(&events, "logit.transform.stale.exceeded", Some(("bound", "max_age"))),
                Some(2.0),
                "{kind}"
            );
            assert_eq!(
                counter(&events, "logit.transform.stale.exceeded", Some(("bound", "max_ahead"))),
                Some(1.0),
                "{kind}"
            );
            assert_eq!(
                counter(&events, "logit.transform.stale.unjudged", None),
                Some(1.0),
                "{kind}"
            );
        }
    }

    #[test]
    fn the_stale_counters_are_absent_when_zero() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("filter", "drop_stale", "transform");
        let mut d = drop_(None).with_telemetry(telemetry.clone());
        let batch = EventBatch { resource: resource(), scope: None, events: vec![log_at(NOW)] };
        process_batch(&mut d, batch, &mut Vec::new(), &telemetry);
        let events = registry.drain(0);
        assert_eq!(counter(&events, "logit.transform.stale.exceeded", None), None);
        assert_eq!(counter(&events, "logit.transform.stale.unjudged", None), None);
    }

    #[test]
    fn a_stale_event_counts_as_discarded_not_dropped() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("filter", "drop_stale", "transform");
        let mut d = drop_(None).with_telemetry(telemetry.clone());
        let batch = EventBatch {
            resource: resource(),
            scope: None,
            events: vec![log_at(NOW), log_at(NOW - secs(1000))],
        };
        let out = process_batch(&mut d, batch, &mut Vec::new(), &telemetry)
            .expect("the fresh event survives");
        assert_eq!(out.events.len(), 1);

        let events = registry.drain(0);
        assert_eq!(counter(&events, "logit.component.events.discarded", None), Some(1.0));
        assert_eq!(counter(&events, "logit.component.events.dropped", None), None);
    }

    #[test]
    fn drop_stale_counts_a_stale_event_diagnostic() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("filter", "drop_stale", "transform");
        let diag = Diagnostics::new("filter").with_telemetry(telemetry);
        let mut d = drop_(None).with_diagnostics(diag);
        assert!(!keeps(&mut d, log_at(NOW - secs(1000))));

        let events = registry.drain(0);
        assert_eq!(
            counter(&events, "logit.component.diagnostics", Some(("key", "stale_event"))),
            Some(1.0)
        );
    }

    #[test]
    fn has_stale_emits_no_diagnostic() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("filter", "has_stale", "transform");
        let mut h = has(None).with_telemetry(telemetry);
        assert!(keeps(&mut h, log_at(NOW - secs(1000))));
        assert_eq!(counter(&registry.drain(0), "logit.component.diagnostics", None), None);
    }

    #[test]
    fn the_pair_partitions_a_batch_through_process_batch() {
        let (max_age, max_ahead) = (MAX_AGE, Some(MAX_AHEAD));
        let registry = Registry::new();
        let t_drop = registry.telemetry_for("d", "drop_stale", "transform");
        let t_has = registry.telemetry_for("h", "has_stale", "transform");
        let mut d = DropStale::new(max_age, max_ahead).with_clock(|| NOW);
        let mut h = HasStale::new(max_age, max_ahead).with_clock(|| NOW);
        let kept = process_batch(&mut d, mixed_batch(), &mut Vec::new(), &t_drop)
            .map_or(0, |b| b.events.len());
        let archived = process_batch(&mut h, mixed_batch(), &mut Vec::new(), &t_has)
            .map_or(0, |b| b.events.len());
        assert_eq!(kept + archived, 5);
        assert_eq!((kept, archived), (2, 3));
    }
}
