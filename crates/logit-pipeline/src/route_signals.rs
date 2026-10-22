//! `route_signals`: sends each signal an event carries to that signal's `target`, and under
//! `strip: true` splits a mixed event into one part per target
//! ([ADR `route-signals`](../../../docs/adr/route-signals.md)). Not a [`crate::Router`]: a router
//! never mutates an event, and a split moves payloads out of one. The node loop is
//! `crate::runtime::run_route_signals`; the per-batch body is [`route_signals_batch`].
//!
//! **No ordinary edge.** Every event goes to targets or is discarded, so a mark never names
//! [`Destinations::FORWARD`] (graph rule 92 keeps it out of every `sources:`). An empty mark is a
//! discard: an event carrying none of the listed signals counts as
//! `logit.component.events.discarded`.
//!
//! **`none`.** An event that arrives with no payload takes `signals.none`'s target, whole, as one
//! part, and is discarded when `none` is unset. The test reads the signals carried on arrival, so
//! an event emptied only by stripping its unlisted signals is still discarded. The InfluxDB codec
//! decodes a point whose fields are all strings to such an event (`logit_proto::influxdb`'s module
//! doc), and `influxdb_out` writes it.
//!
//! **Strip semantics, `strip: true`:**
//!
//! - A signal `signals:` leaves out has its payload cleared from every event before routing,
//!   counted `logit.transform.payloads.stripped{signal}` per slot cleared, as `keep_signals` does.
//! - An event goes out as at most one part per target, in arrival order within each target.
//!   Signals that share a target stay on one part. An event's metric records move whole: they are
//!   one signal.
//! - Each part holds the original's timestamp and its attributes, `_logit.*` carriers included.
//!   The part for the highest slot is the original event, keeping its `AttrMap`; every other part
//!   clones it, and its payloads are moved out of the original, never cloned.
//! - A log's `TraceRef` lives on the log, so a metrics or span part doesn't carry it: a `sample`
//!   keyed on `trace_id` downstream reads a metrics part as missing the key.
//! - Every part of one batch shares its resource `Arc` and scope, and goes out under the node's
//!   one child context and one span. There is no shared-`Arc` path: each part is its own event.
//! - Each part past an event's first counts as `logit.component.events.copied`, so
//!   `events.received + events.copied − events.discarded` is the events across every partition
//!   sent.
//!
//! **`strip: false`:** each target receives the whole event, unlisted payloads riding along, as
//! [`crate::route_batch`] sends a multi-destination mark: cloned into each slot but the highest,
//! or, when every event in the batch carries one multi-target mark, the whole batch behind one
//! shared `Arc`. Only an event carrying no listed signal is discarded, and nothing is stripped.

use crate::router::{signal_index, signal_masks, Destinations, RouterScratch};
use crate::runtime::{count_and_reserve, take_partitions, Partition};
use logit_config::SignalTargets;
use logit_core::{Event, EventBatch, MetricList, Telemetry};
use std::sync::Arc;

/// `payloads.stripped`'s `signal` tag per [`signal_index`] bit.
const SIGNAL_NAMES: [&str; 3] = ["logs", "metrics", "traces"];

/// One `route_signals` component's routing table, resolved once from its config.
pub struct SignalRoutes {
    /// Each signal's destination slot (`n + 1` for target `n`), `None` for an unlisted signal, in
    /// [`signal_index`] bit order.
    slots: [Option<usize>; 3],
    /// [`signal_masks`]' table, with an empty mark for a carried set naming no listed signal, and
    /// entry 0, an event that arrived with no payload, `signals.none`'s target or empty.
    masks: [Destinations; 8],
    /// The [`signal_index`] bits of the listed signals.
    listed: usize,
    strip: bool,
    targets: usize,
}

impl SignalRoutes {
    /// Resolves each `signals:` id to its slot in `targets` (`ResolvedComponent::targets`).
    ///
    /// Panics if an id isn't in `targets` or there are more than [`Destinations::MAX_TARGETS`];
    /// graph rules 48 and 92 rule both out for a resolved graph.
    pub fn new(signals: &SignalTargets, strip: bool, targets: &[String]) -> Self {
        assert!(
            targets.len() <= Destinations::MAX_TARGETS,
            "route_signals has {} targets, over the {} rule 92 allows",
            targets.len(),
            Destinations::MAX_TARGETS
        );
        let slot_of = |id: &str| {
            targets.iter().position(|t| t == id).unwrap_or_else(|| {
                panic!(
                    "route_signals target '{id}' is not among its targets {targets:?} -- rules \
                     48/92 should have guaranteed this resolves"
                )
            })
        };
        let mut slots = [None; 3];
        let mut per_signal = [Destinations::NONE; 3];
        let mut listed = 0usize;
        let payload_signals = [&signals.logs, &signals.metrics, &signals.traces];
        for (bit, id) in payload_signals.into_iter().enumerate() {
            let Some(id) = id else { continue };
            let n = slot_of(id);
            slots[bit] = Some(n + 1);
            per_signal[bit] = Destinations::target(n as u16);
            listed |= 1 << bit;
        }
        let none = signals
            .none
            .as_deref()
            .map_or(Destinations::NONE, |id| Destinations::target(slot_of(id) as u16));
        Self { slots, masks: signal_masks(per_signal, none), listed, strip, targets: targets.len() }
    }

    /// How many targets the table addresses: the length of the node's slot-ordered target
    /// `Fanout`s, and [`RouterScratch::new`]'s argument.
    pub fn targets(&self) -> usize {
        self.targets
    }

    /// The part of `event` bound for `slot`: the payloads of the signals mapped there, moved out
    /// of `event`, with its timestamp and a clone of its attributes.
    fn split_off(&self, event: &mut Event, slot: usize) -> Event {
        let here = |bit: usize| self.slots[bit] == Some(slot);
        Event {
            timestamp: event.timestamp,
            attributes: event.attributes.clone(),
            log: if here(0) { event.log.take() } else { None },
            metrics: if here(1) { std::mem::take(&mut event.metrics) } else { MetricList::new() },
            span: if here(2) { event.span.take() } else { None },
        }
    }
}

/// Clears the payloads whose [`signal_index`] bits are set in `unlisted`, tallying each in
/// `stripped`.
fn strip_unlisted(event: &mut Event, unlisted: usize, stripped: &mut [u64; 3]) {
    if unlisted & 1 != 0 {
        event.log = None;
        stripped[0] += 1;
    }
    if unlisted & 2 != 0 {
        event.metrics = MetricList::new();
        stripped[1] += 1;
    }
    if unlisted & 4 != 0 {
        event.span = None;
        stripped[2] += 1;
    }
}

/// The per-batch body of `run_route_signals`: one outgoing [`Partition`] per target that receives
/// at least one event, in slot order; never slot 0. `pub` so `crates/logit-bench/tests/allocations.rs`
/// measures the real path.
///
/// The passes are [`crate::route_batch`]'s. Pass 1 reads each event's carried signals, strips
/// unlisted payloads under `strip: true`, and records its mark. Passes 2 and 3 count and
/// `reserve_exact` per used target. Pass 4 moves each event into the highest slot its mark names
/// and, for every other slot, either moves that slot's payloads into a new part
/// (`strip: true`) or clones the event (`strip: false`). An empty mark drops the event there.
///
/// Allocations per batch: 1 for the returned `Vec` plus 1 per target used, plus, under
/// `strip: true`, each extra part's attribute clone, and, under `strip: false`, each event clone;
/// 2 on `strip: false`'s shared path; 0 when every event is discarded. Pinned in
/// `crates/logit-bench/tests/allocations.rs`.
pub fn route_signals_batch(
    routes: &SignalRoutes,
    scratch: &mut RouterScratch,
    batch: EventBatch,
    telemetry: &Telemetry,
) -> Vec<(usize, Partition)> {
    telemetry.count("logit.component.batches.received", 1.0, &[]);
    telemetry.count("logit.component.events.received", batch.events.len() as f64, &[]);
    debug_assert_eq!(scratch.destinations(), routes.targets + 1, "scratch sized for this table");

    let EventBatch { resource, scope, mut events } = batch;
    let process_timer = telemetry.timer("logit.component.process.duration");
    scratch.marks.clear();
    scratch.marks.reserve(events.len());
    scratch.counts.clear();
    scratch.counts.resize(scratch.dests.len(), 0);

    // Pass 1: strip, then mark. The mark comes from the signals carried before the strip, which
    // gives the same mark: an unlisted signal adds no slot.
    let mut stripped = [0u64; 3];
    let mut discarded = 0u64;
    for event in &mut events {
        let carried = signal_index(event);
        if routes.strip && carried & !routes.listed != 0 {
            strip_unlisted(event, carried & !routes.listed, &mut stripped);
        }
        let mark = routes.masks[carried];
        discarded += u64::from(mark.is_empty());
        scratch.marks.push(mark);
    }
    for (count, signal) in stripped.iter().zip(SIGNAL_NAMES) {
        if *count > 0 {
            telemetry.count(
                "logit.transform.payloads.stripped",
                *count as f64,
                &[("signal", signal)],
            );
        }
    }
    if discarded > 0 {
        telemetry.count("logit.component.events.discarded", discarded as f64, &[]);
    }

    // `route_batch`'s shared path, for `strip: false` only: a split event has no whole to share.
    if !routes.strip {
        if let Some(&first) = scratch.marks.first() {
            if first.len() > 1 && scratch.marks.iter().all(|mark| *mark == first) {
                drop(process_timer);
                telemetry.count(
                    "logit.component.events.copied",
                    ((first.len() - 1) * events.len()) as f64,
                    &[],
                );
                let shared = Arc::new(EventBatch { resource, scope, events });
                return first
                    .slots()
                    .map(|slot| (slot, Partition::Shared(shared.clone())))
                    .collect();
            }
        }
    }

    let (copies, used) = count_and_reserve(scratch);

    // Pass 4: move, splitting or cloning for every slot but the highest.
    for (mut event, mark) in events.into_iter().zip(&scratch.marks) {
        let Some(last) = mark.highest_slot() else { continue };
        for slot in mark.without_slot(last).slots() {
            let part = match routes.strip {
                true => routes.split_off(&mut event, slot),
                false => event.clone(),
            };
            scratch.dests[slot].push(part);
        }
        scratch.dests[last].push(event);
    }
    drop(process_timer);
    if copies > 0 {
        telemetry.count("logit.component.events.copied", copies as f64, &[]);
    }

    take_partitions(scratch, used, &resource, &scope)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{TelemetryProbe, Totals};
    use logit_core::interner::intern;
    use logit_core::{
        AttrMap, BodyFormat, LogRecord, MetricKind, MetricRecord, Resource, SpanKind, SpanRecord,
        SpanStatus, Value,
    };

    fn log_record(message: &str) -> LogRecord {
        LogRecord {
            message: Value::str(message),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        }
    }

    fn metric_record() -> MetricRecord {
        MetricRecord::new(intern("m"), MetricKind::counter(1.0))
    }

    fn span_record() -> SpanRecord {
        SpanRecord {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: Value::str("op"),
            kind: SpanKind::Internal,
            status: SpanStatus::Unset,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: 0,
            flags: 0,
            ext: None,
        }
    }

    /// An event tagged `id`, carrying the payloads `signals` names (`l`, `m`, `t`).
    fn event(id: &str, signals: &str) -> Event {
        let mut attributes = AttrMap::new();
        attributes.insert("id", Value::str(id));
        attributes.insert("_logit.statsd.type", Value::str("c"));
        Event {
            timestamp: 7,
            attributes,
            log: signals.contains('l').then(|| log_record(id)),
            metrics: if signals.contains('m') {
                MetricList::from_iter([metric_record(), metric_record()])
            } else {
                MetricList::new()
            },
            span: signals.contains('t').then(span_record),
        }
    }

    /// `(id, carried signals)` per event, the signals as `event`'s letters.
    fn shape(event: &Event) -> (String, String) {
        let id = event.attributes.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let mut signals = String::new();
        if event.log.is_some() {
            signals.push('l');
        }
        if !event.metrics.is_empty() {
            signals.push('m');
        }
        if event.span.is_some() {
            signals.push('t');
        }
        (id, signals)
    }

    fn batch(events: Vec<Event>) -> EventBatch {
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events }
    }

    fn signals(logs: Option<&str>, metrics: Option<&str>, traces: Option<&str>) -> SignalTargets {
        SignalTargets {
            logs: logs.map(String::from),
            metrics: metrics.map(String::from),
            traces: traces.map(String::from),
            none: None,
        }
    }

    fn with_none(mut signals: SignalTargets, none: &str) -> SignalTargets {
        signals.none = Some(none.to_string());
        signals
    }

    fn targets(ids: &[&str]) -> Vec<String> {
        ids.iter().map(|s| s.to_string()).collect()
    }

    /// One partition as `(slot, shared, [(id, signals)])`.
    type Shape = (usize, bool, Vec<(String, String)>);

    /// Routes `events` and returns each partition's [`Shape`] with the batch's telemetry.
    fn run(routes: &SignalRoutes, events: Vec<Event>) -> (Vec<Shape>, Totals) {
        let mut scratch = RouterScratch::new(routes.targets());
        let probe = TelemetryProbe::new();
        let telemetry = probe.telemetry("by_signal", "route_signals", "transform");
        let partitions = route_signals_batch(routes, &mut scratch, batch(events), &telemetry);
        assert!(
            scratch.dests.iter().all(|dest| dest.is_empty() && dest.capacity() == 0),
            "the scratch must come back empty and capacity-free for the next batch"
        );
        let out = partitions
            .iter()
            .map(|(slot, out)| {
                let shared = matches!(out, Partition::Shared(_));
                (*slot, shared, out.events.iter().map(shape).collect())
            })
            .collect();
        (out, Totals::of(probe.registry().drain(0)))
    }

    fn parts(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs.iter().map(|(id, s)| (id.to_string(), s.to_string())).collect()
    }

    #[test]
    fn strip_splits_a_mixed_event_into_one_part_per_target() {
        let routes = SignalRoutes::new(
            &signals(Some("logs_t"), Some("metrics_t"), Some("traces_t")),
            true,
            &targets(&["logs_t", "metrics_t", "traces_t"]),
        );
        let (out, totals) = run(&routes, vec![event("a", "lmt"), event("b", "m")]);
        assert_eq!(
            out,
            vec![
                (1, false, parts(&[("a", "l")])),
                (2, false, parts(&[("a", "m"), ("b", "m")])),
                (3, false, parts(&[("a", "t")])),
            ]
        );
        assert_eq!(totals.sum("logit.component.events.received", &[]), 2.0);
        assert_eq!(totals.sum("logit.component.events.copied", &[]), 2.0, "parts − 1 per event");
        assert_eq!(totals.sum("logit.component.events.discarded", &[]), 0.0);
    }

    #[test]
    fn every_part_keeps_the_timestamp_attributes_and_whole_metric_list() {
        let routes = SignalRoutes::new(
            &signals(Some("logs_t"), Some("metrics_t"), None),
            true,
            &targets(&["logs_t", "metrics_t"]),
        );
        let mut scratch = RouterScratch::new(routes.targets());
        let out = route_signals_batch(
            &routes,
            &mut scratch,
            batch(vec![event("a", "lm")]),
            &Telemetry::default(),
        );
        let [(1, logs), (2, metrics)] = out.as_slice() else { panic!("expected two parts") };
        for part in [&logs.events[0], &metrics.events[0]] {
            assert_eq!(part.timestamp, 7);
            assert_eq!(
                part.attributes.get("_logit.statsd.type").and_then(|v| v.as_str()),
                Some("c"),
                "a private carrier rides on every part"
            );
        }
        assert_eq!(metrics.events[0].metrics.len(), 2, "metric records move as one signal");
        assert!(logs.events[0].metrics.is_empty());
    }

    #[test]
    fn with_every_signal_unset_every_event_is_discarded() {
        let routes = SignalRoutes::new(&signals(None, None, None), true, &targets(&[]));
        let (out, totals) = run(&routes, vec![event("a", "lmt"), event("b", "l"), event("c", "")]);
        assert!(out.is_empty());
        assert_eq!(totals.sum("logit.component.events.received", &[]), 3.0);
        assert_eq!(totals.sum("logit.component.events.discarded", &[]), 3.0);
    }

    #[test]
    fn signals_sharing_a_target_stay_on_one_part() {
        let routes = SignalRoutes::new(
            &signals(Some("events_t"), Some("metrics_t"), Some("events_t")),
            true,
            &targets(&["events_t", "metrics_t"]),
        );
        let (out, totals) = run(&routes, vec![event("a", "lmt"), event("b", "lt")]);
        assert_eq!(
            out,
            vec![(1, false, parts(&[("a", "lt"), ("b", "lt")])), (2, false, parts(&[("a", "m")]))]
        );
        assert_eq!(totals.sum("logit.component.events.copied", &[]), 1.0);
    }

    #[test]
    fn strip_clears_and_counts_an_unlisted_payload() {
        let routes =
            SignalRoutes::new(&signals(Some("logs_t"), None, None), true, &targets(&["logs_t"]));
        let (out, totals) = run(&routes, vec![event("a", "lmt"), event("b", "l")]);
        assert_eq!(out, vec![(1, false, parts(&[("a", "l"), ("b", "l")]))]);
        assert_eq!(totals.sum("logit.transform.payloads.stripped", &[("signal", "metrics")]), 1.0);
        assert_eq!(totals.sum("logit.transform.payloads.stripped", &[("signal", "traces")]), 1.0);
        assert_eq!(totals.sum("logit.transform.payloads.stripped", &[("signal", "logs")]), 0.0);
        assert_eq!(totals.sum("logit.component.events.copied", &[]), 0.0);
    }

    #[test]
    fn an_event_with_no_listed_signal_is_discarded_never_dropped() {
        let routes =
            SignalRoutes::new(&signals(Some("logs_t"), None, None), true, &targets(&["logs_t"]));
        let (out, totals) = run(&routes, vec![event("a", "m"), event("b", ""), event("c", "l")]);
        assert_eq!(out, vec![(1, false, parts(&[("c", "l")]))]);
        assert_eq!(totals.sum("logit.component.events.discarded", &[]), 2.0);
        assert_eq!(totals.sum("logit.transform.payloads.stripped", &[("signal", "metrics")]), 1.0);
        assert!(
            totals.sums().all(|(name, _, _)| !name.ends_with(".dropped")),
            "a discard the config names is never *.dropped"
        );
    }

    #[test]
    fn with_all_three_listed_only_an_empty_event_is_discarded() {
        let routes =
            SignalRoutes::new(&signals(Some("t"), Some("t"), Some("t")), true, &targets(&["t"]));
        let (out, totals) = run(&routes, vec![event("a", ""), event("b", "lmt")]);
        assert_eq!(out, vec![(1, false, parts(&[("b", "lmt")]))]);
        assert_eq!(totals.sum("logit.component.events.discarded", &[]), 1.0);
        assert_eq!(totals.sum("logit.transform.payloads.stripped", &[]), 0.0);
    }

    #[test]
    fn a_batch_with_every_event_discarded_yields_no_partition() {
        let routes = SignalRoutes::new(
            &signals(None, None, Some("traces_t")),
            true,
            &targets(&["traces_t"]),
        );
        let (out, totals) = run(&routes, vec![event("a", "m"), event("b", "l")]);
        assert!(out.is_empty(), "got {out:?}");
        assert_eq!(totals.sum("logit.component.events.discarded", &[]), 2.0);
    }

    #[test]
    fn strip_never_shares_a_uniformly_split_batch() {
        let routes = SignalRoutes::new(
            &signals(Some("logs_t"), Some("metrics_t"), None),
            true,
            &targets(&["logs_t", "metrics_t"]),
        );
        let (out, _) = run(&routes, vec![event("a", "lm"), event("b", "lm")]);
        assert_eq!(
            out,
            vec![
                (1, false, parts(&[("a", "l"), ("b", "l")])),
                (2, false, parts(&[("a", "m"), ("b", "m")])),
            ]
        );
    }

    #[test]
    fn without_strip_each_target_gets_the_whole_event() {
        let routes = SignalRoutes::new(
            &signals(Some("logs_t"), Some("metrics_t"), None),
            false,
            &targets(&["logs_t", "metrics_t"]),
        );
        let (out, totals) = run(&routes, vec![event("a", "lmt"), event("b", "m"), event("c", "t")]);
        assert_eq!(
            out,
            vec![
                (1, false, parts(&[("a", "lmt")])),
                (2, false, parts(&[("a", "lmt"), ("b", "m")])),
            ],
            "an unlisted span rides along; an event with only an unlisted signal is discarded"
        );
        assert_eq!(totals.sum("logit.component.events.copied", &[]), 1.0);
        assert_eq!(totals.sum("logit.component.events.discarded", &[]), 1.0);
        assert_eq!(totals.sum("logit.transform.payloads.stripped", &[]), 0.0);
    }

    #[test]
    fn without_strip_a_uniformly_multi_target_batch_is_shared() {
        let routes = SignalRoutes::new(
            &signals(Some("logs_t"), Some("metrics_t"), None),
            false,
            &targets(&["logs_t", "metrics_t"]),
        );
        let mut scratch = RouterScratch::new(routes.targets());
        let out = route_signals_batch(
            &routes,
            &mut scratch,
            batch(vec![event("a", "lm"), event("b", "lm")]),
            &Telemetry::default(),
        );
        let [(1, Partition::Shared(first)), (2, Partition::Shared(second))] = out.as_slice() else {
            panic!("expected slots 1 and 2 sharing one batch");
        };
        assert!(Arc::ptr_eq(first, second));
    }

    #[test]
    fn a_uniform_single_target_batch_moves_untouched() {
        let routes = SignalRoutes::new(
            &signals(None, Some("metrics_t"), None),
            true,
            &targets(&["metrics_t"]),
        );
        let (out, totals) = run(&routes, vec![event("a", "m"), event("b", "m")]);
        assert_eq!(out, vec![(1, false, parts(&[("a", "m"), ("b", "m")]))]);
        assert_eq!(totals.sum("logit.component.events.copied", &[]), 0.0);
    }

    #[test]
    fn none_alone_routes_an_empty_event_and_discards_the_rest() {
        for strip in [true, false] {
            let routes = SignalRoutes::new(
                &with_none(signals(None, None, None), "none_t"),
                strip,
                &targets(&["none_t"]),
            );
            let (out, totals) = run(&routes, vec![event("a", ""), event("b", "m"), event("c", "")]);
            assert_eq!(out, vec![(1, false, parts(&[("a", ""), ("c", "")]))], "strip: {strip}");
            assert_eq!(totals.sum("logit.component.events.discarded", &[]), 1.0, "strip: {strip}");
            assert_eq!(totals.sum("logit.component.events.copied", &[]), 0.0, "strip: {strip}");
        }
    }

    #[test]
    fn none_sharing_the_metrics_target_routes_both_there_once() {
        for strip in [true, false] {
            let routes = SignalRoutes::new(
                &with_none(signals(Some("logs_t"), Some("metrics_t"), None), "metrics_t"),
                strip,
                &targets(&["logs_t", "metrics_t"]),
            );
            let (out, totals) =
                run(&routes, vec![event("a", ""), event("b", "m"), event("c", "l")]);
            assert_eq!(
                out,
                vec![(1, false, parts(&[("c", "l")])), (2, false, parts(&[("a", ""), ("b", "m")])),],
                "strip: {strip}"
            );
            assert_eq!(totals.sum("logit.component.events.received", &[]), 3.0);
            assert_eq!(
                totals.sum("logit.component.events.discarded", &[]),
                0.0,
                "routed, not discarded"
            );
            assert_eq!(totals.sum("logit.component.events.copied", &[]), 0.0, "no copies");
        }
    }

    #[test]
    fn an_event_emptied_by_strip_is_discarded_not_routed_to_none() {
        let routes = SignalRoutes::new(
            &with_none(signals(Some("logs_t"), None, None), "none_t"),
            true,
            &targets(&["logs_t", "none_t"]),
        );
        let (out, totals) = run(&routes, vec![event("a", "m"), event("b", "")]);
        assert_eq!(out, vec![(2, false, parts(&[("b", "")]))]);
        assert_eq!(totals.sum("logit.component.events.discarded", &[]), 1.0);
        assert_eq!(totals.sum("logit.transform.payloads.stripped", &[("signal", "metrics")]), 1.0);
    }

    #[test]
    fn without_strip_an_event_with_only_unlisted_signals_is_discarded_not_routed_to_none() {
        let routes = SignalRoutes::new(
            &with_none(signals(Some("logs_t"), None, None), "none_t"),
            false,
            &targets(&["logs_t", "none_t"]),
        );
        let (out, totals) = run(&routes, vec![event("a", "mt"), event("b", "")]);
        assert_eq!(out, vec![(2, false, parts(&[("b", "")]))]);
        assert_eq!(totals.sum("logit.component.events.discarded", &[]), 1.0);
    }
}
