//! The built-in `aggregate` transform: a stateful, tumbling-window metric aggregator.
//!
//! Windowing/merge semantics are recorded in `docs/adr/0008-aggregation-window-semantics.md` and
//! come from `docs/design/data-model.md`'s mergeable-metric-kinds design: `Counter` sums, `Gauge`
//! keeps the value with the latest source timestamp, `Distribution` merges via `DdSketch::merge`
//! (this is that method's first real caller anywhere in the codebase). `Set`/`Histogram`/`Summary`
//! have no defined merge rule here yet (`Set` specifically is blocked on `HyperLogLog` still being a
//! method-less stub) and pass through untouched rather than being dropped -- this project's
//! consistent stance on data it doesn't know how to handle correctly. Since an event can now carry
//! a log and/or a span alongside its metrics (docs/adr/0012-multi-payload-events.md), pass-through
//! is per *metric*, not per *event*: this stage absorbs every mergeable metric off an event and
//! forwards whatever's left -- the unmergeable metrics, plus any log/span -- rather than treating
//! "can't merge one metric" as a reason to forward the whole event untouched.

use logit_core::interner::Symbol;
use logit_core::{
    AttrMap, Diagnostics, Event, MetricKind, MetricRecord, Resource, SpanLink, Telemetry, Value,
};
use logit_pipeline::{FlushOutput, TraceContext, Transform};
use smallvec::SmallVec;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Duration;

/// A small, bounded set of the distinct `TraceContext`s that have contributed to one series since
/// the last flush -- reasonable/best-effort, not exhaustive. `SpanLink` (`crates/logit-core/src/span.rs`)
/// already exists in the data model for exactly this shape (OTel's answer to "this span was
/// influenced by several others, not descended from one"), per
/// `docs/adr/0020-trace-context-propagation-on-delivered.md`. Same "cap gates insertion only,
/// already-seen is always free to re-observe, drop-and-count past it" shape
/// `ComponentBuffer::upsert` already uses (`crates/logit-core/src/telemetry.rs`) -- the one
/// precedent in this codebase for a bounded, drop-and-counted set.
///
/// Per-series, not shared across a whole resource group or the whole `Aggregator`: a link belongs
/// to the specific series whose flush would become a span, and attributing it to unrelated series
/// in the same window would be exactly the "silently wrong" shape ADR 0020 rejected when it
/// considered (and rejected) picking an arbitrary parent for a flush.
#[derive(Default)]
struct ContributingContexts {
    /// Inline capacity 1: most series see exactly one contributing source, so this stays free
    /// (`docs/adr/0017-minimize-allocations-over-event-size.md`'s policy) until a genuinely
    /// fanned-in series needs more.
    seen: SmallVec<[TraceContext; 1]>,
    dropped: u64,
}

/// Caps how many distinct contexts one series tracks between flushes. A fixed constant, not
/// configurable, matching `docs/known-gaps.md`'s stance on `MAX_KEYS_PER_COMPONENT` -- revisit if
/// a legitimate series ever needs more than this many distinct sources tracked at once.
const MAX_CONTRIBUTING_CONTEXTS_PER_SERIES: usize = 8;

impl ContributingContexts {
    /// Records `ctx` as a contributor, unless it's already tracked (free to re-observe) or the cap
    /// is already full (dropped and counted, never silently grown).
    fn observe(&mut self, ctx: TraceContext) {
        if self.seen.contains(&ctx) {
            return;
        }
        if self.seen.len() >= MAX_CONTRIBUTING_CONTEXTS_PER_SERIES {
            self.dropped += 1;
            return;
        }
        self.seen.push(ctx);
    }

    /// Consumes the tracked set into the `SpanLink`s a flush would attach to whatever span
    /// eventually represents this series (not built yet -- `docs/known-gaps.md`'s internal-spans
    /// entry, item 2), plus how many distinct contexts the cap rejected.
    fn into_links(self) -> (Vec<SpanLink>, u64) {
        let links = self
            .seen
            .into_iter()
            .map(|ctx| SpanLink {
                trace_id: ctx.trace_id,
                span_id: ctx.span_id,
                attributes: AttrMap::new(),
            })
            .collect();
        (links, self.dropped)
    }
}

/// One tumbling-window aggregator, owned by one pipeline stage. `process` accumulates what it can
/// and passes everything else straight through; `flush` drains every non-gauge accumulator and
/// every gauge series past its retention window, resetting each to empty -- state does not carry
/// across flushes for those. **A gauge series is the one exception**, per `gauge_retention`: see
/// `docs/adr/0008-aggregation-window-semantics.md`'s "gauge series carry across the window
/// boundary" amendment for the full design and why gauges specifically (not counters) get this.
pub struct Aggregator {
    interval: Duration,
    groups: Vec<ResourceGroup>,
    diag: Diagnostics,
    telemetry: Telemetry,
    /// The most recent batch's `TraceContext`, per `observe_batch_context` -- rolling state, not
    /// reset by `flush` (it isn't part of any one window). Mirrors `run_lua`'s `last_resource`
    /// precedent (`crates/logit-pipeline/src/runtime.rs`): default until the first batch arrives.
    current_batch_context: TraceContext,
    /// How many consecutive *idle* windows (no update at all) a gauge series is retained past its
    /// last update, so a delta in window N+1 can still resolve against window N's final absolute
    /// value. `0` (the default, matching `Aggregator::new`) reproduces today's strictly-tumbling
    /// behavior exactly -- no gauge series ever survives a flush. Set via
    /// [`Aggregator::with_gauge_retention`]. See the ADR 0008 amendment.
    gauge_retention: u32,
    /// Hard cap on how many gauge series may be retained across all resource groups at once -- a
    /// DoS/cardinality guard, not a tuning knob. `gauge_retention` alone bounds only the *tail*
    /// (how long one series survives); this bounds the *peak* (how many can exist retained at
    /// once), which a sustained stream of never-repeating series names would otherwise blow past
    /// regardless of how short the retention window is. Least-recently-updated series are evicted
    /// first once this is exceeded. Meaningless while `gauge_retention` is `0`.
    max_retained_gauge_series: usize,
}

struct ResourceGroup {
    resource: Arc<Resource>,
    series: HashMap<SeriesKey, SeriesState>,
}

/// One series' accumulated value, paired with which batches contributed to it since the last
/// flush.
struct SeriesState {
    accumulator: Accumulator,
    contexts: ContributingContexts,
    /// Consecutive flushes this series has survived with **no** update at all -- reset to 0 the
    /// moment any event touches it again. Only ever incremented for a retained (gauge,
    /// `gauge_retention > 0`) series; a non-gauge series never survives a flush to have this
    /// matter. Compared against `Aggregator::gauge_retention` at flush to decide eviction.
    idle_windows: u32,
    /// Whether any event touched this series since the last flush. An explicit field, not derived
    /// from `contexts.seen` being non-empty -- that happens to correlate (`observe` fires on
    /// exactly the successful merges that also flip this), but coupling this to a set built for a
    /// different purpose (span linking) is fragile: a future change to `ContributingContexts`
    /// that stops recording on some merge path would silently break retention's idea of "was this
    /// updated" too. A `bool` costs nothing extra on an already heap-allocated struct.
    updated_this_window: bool,
}

enum Accumulator {
    Counter(f64),
    /// `at` is the source event's timestamp, used to pick the last-write-wins value -- not the
    /// window's timestamp, which doesn't exist until flush.
    Gauge {
        value: f64,
        at: i64,
    },
    Distribution(logit_core::DdSketch),
}

impl Aggregator {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            groups: Vec::new(),
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
            current_batch_context: TraceContext::default(),
            gauge_retention: 0,
            max_retained_gauge_series: 0,
        }
    }

    /// Enables cross-flush gauge retention -- see the `gauge_retention`/`max_retained_gauge_series`
    /// field doc comments and the ADR 0008 amendment. Matches the existing
    /// `with_diagnostics`/`with_telemetry` builder shape, so `Aggregator::new(interval)`'s
    /// signature stays unchanged and every existing caller (including `crates/logit-bench`'s
    /// fixture) compiles unchanged, defaulting to `0` -- strictly tumbling, exactly today's
    /// behavior.
    pub fn with_gauge_retention(mut self, retention: u32, max_retained: usize) -> Self {
        self.gauge_retention = retention;
        self.max_retained_gauge_series = max_retained;
        self
    }

    /// Records `ctx` as the context of the batch about to be `process`ed -- see
    /// `Transform::observe_batch_context`'s doc comment for why this is per-batch, not per-event.
    pub fn observe_batch_context(&mut self, ctx: TraceContext) {
        self.current_batch_context = ctx;
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    /// Attaches a telemetry handle -- see `flush`'s `logit.transform.series.active`/
    /// `.resource.groups` gauges.
    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Absorbs every mergeable metric (`Counter`/`Gauge`/`Distribution`) off `event` into this
    /// aggregator's window state, and forwards whatever's left -- unmergeable metric kinds, a
    /// kind conflict with an already-accumulating series, and/or a log or span, if the event
    /// carries any (docs/adr/0012-multi-payload-events.md). `None` only when nothing at all
    /// remains on the event; a pure log/span event (no metrics at all) never touches window
    /// state, matching the zero-cost pass-through this had before an event could carry more than
    /// one payload.
    ///
    /// Grouped by `resource` *value*, not `Arc` identity: two inputs that each build their own
    /// `Arc::new(Resource::default())` describe the same (empty) origin and should aggregate
    /// together, not be split into separate windows because they happen to be different
    /// allocations. One input's batches do share one `Arc` in practice (see
    /// `crates/logit-inputs/src/statsd.rs`), so the common case is a single linear-scan group.
    pub fn process(&mut self, resource: &Arc<Resource>, mut event: Event) -> Option<Event> {
        if event.metrics.is_empty() {
            return Some(event);
        }

        // Read once, not once per metric -- `observe_batch_context` fires once per incoming
        // batch, so every metric on every event of that batch shares the same value.
        let ctx = self.current_batch_context;

        // Taken as an owned list, not filtered in place with `retain`: a `retain` closure would
        // need `&event.attributes`/`&event.timestamp` at the same time `self.group_for(resource)`
        // needs `&mut self`, and those two borrows can't coexist. Owning the metrics up front
        // makes them independent of `event` for the rest of this loop; anything not absorbed is
        // pushed back at the end, in its original relative order.
        let metrics = std::mem::take(&mut event.metrics);
        for record in metrics {
            // No merge rule defined for these (docs/design/data-model.md) -- leave them on the
            // event rather than absorbing or dropping them. `GaugeDelta` is *not* here -- it has
            // a real resolution below, unlike these three (docs/adr/0024-relative-gauge-adjustments.md).
            if matches!(
                record.kind,
                MetricKind::Set(_) | MetricKind::Histogram { .. } | MetricKind::Summary { .. }
            ) {
                event.metrics.push(record);
                continue;
            }

            let key = SeriesKey {
                name: record.name,
                unit: record.unit,
                attributes: event.attributes.clone(),
            };
            let group = self.group_for(resource);
            let entry = group.series.entry(key);
            // Whether this metric opened a brand-new series -- not derivable from the
            // accumulator's `at`/`value` afterward, since a genuine `Gauge(0.0)` at `at:
            // i64::MIN` looks identical to an unseeded delta's result. Only meaningful for
            // `GaugeDelta` below; a fresh `Counter`/`Distribution` series has no equivalent
            // "resolved against a placeholder" hazard, since there's no prior value to have
            // wanted.
            let was_vacant = matches!(entry, std::collections::hash_map::Entry::Vacant(_));
            let state = entry.or_insert_with(|| SeriesState {
                accumulator: Accumulator::new_for(&record.kind),
                contexts: ContributingContexts::default(),
                idle_windows: 0,
                updated_this_window: false,
            });
            let accumulated = match (&mut state.accumulator, &record.kind) {
                (Accumulator::Counter(sum), MetricKind::Counter(v)) => {
                    *sum += v;
                    true
                }
                (Accumulator::Gauge { value, at }, MetricKind::Gauge(v)) => {
                    // Last-write-wins by timestamp (docs/design/data-model.md): a later-or-equal
                    // source timestamp replaces the held value. Equal timestamps favor whichever
                    // arrives second -- arbitrary but deterministic given actual processing
                    // order. Two gauges of the same series inside one event share `event
                    // .timestamp`, so this tiebreak is what decides between them too.
                    if event.timestamp >= *at {
                        *value = *v;
                        *at = event.timestamp;
                    }
                    true
                }
                (Accumulator::Gauge { value, .. }, MetricKind::GaugeDelta(d)) => {
                    // Asymmetric on purpose (docs/adr/0024-relative-gauge-adjustments.md,
                    // verbatim there): a delta applies to the running value in *arrival* order
                    // and never advances `at` -- note the `..`. Mixing "deltas in arrival order"
                    // with "absolutes by last-write-wins" is undefined the moment they interleave
                    // unless one of the two rules is pinned independently of the other's
                    // tiebreak; leaving `at` untouched here is what keeps an absolute's LWW rule
                    // meaningful regardless of how many deltas land between two absolutes.
                    *value += d;
                    true
                }
                (Accumulator::Distribution(sketch), MetricKind::Distribution(incoming)) => {
                    sketch.merge(incoming);
                    true
                }
                // A series already accumulating under one kind (e.g. it started as a counter)
                // just saw a metric of a different kind under the same name/unit/tags (e.g. a
                // gauge). No correct merge exists for that -- leave this one metric on the event
                // rather than silently dropping it or corrupting the existing accumulator with a
                // type-punned value. Per-metric now, not per-event: a sibling metric on the same
                // event that *does* merge cleanly is still absorbed. A `GaugeDelta` against a
                // `Counter`/`Distribution` series lands here too, same as `Gauge` always has.
                _ => false,
            };
            if accumulated {
                // Only on an actual merge -- a kind-conflicted metric didn't touch this series'
                // accumulator, so it shouldn't be recorded as one of its contributors either.
                state.contexts.observe(ctx);
                // An explicit flag, not derived from `contexts.seen` -- see `SeriesState`'s field
                // doc comment for why. `flush` resets this to `false` for every series it retains.
                state.updated_this_window = true;
                if was_vacant && matches!(record.kind, MetricKind::GaugeDelta(_)) {
                    // A delta that opened a brand-new series resolved against 0.0 (statsd's own
                    // rule for an unseeded gauge) -- correct per spec, but indistinguishable from
                    // a real 0.0 in the emitted number, so this is counted and reported rather
                    // than left silent.
                    self.telemetry.count("logit.transform.gauge.delta.unseeded", 1.0, &[]);
                    self.diag.warn_throttled(
                        "gauge_delta_unseeded",
                        format_args!(
                            "gauge delta for '{}' opened a new series and resolved against 0.0 \
                             -- no prior absolute value seen for this series",
                            logit_core::interner::resolve(record.name)
                        ),
                    );
                }
            }
            if !accumulated {
                self.diag.warn_throttled(
                    "kind_conflict",
                    format_args!(
                        "metric '{}' has a kind that conflicts with an already-accumulating \
                         series under the same name/unit/tags -- forwarding it untouched",
                        logit_core::interner::resolve(record.name)
                    ),
                );
                event.metrics.push(record);
            }
        }

        if event.metrics.is_empty() && event.log.is_none() && event.span.is_none() {
            None
        } else {
            Some(event)
        }
    }

    fn group_for(&mut self, resource: &Arc<Resource>) -> &mut ResourceGroup {
        if let Some(i) = self.groups.iter().position(|g| g.resource.as_ref() == resource.as_ref()) {
            &mut self.groups[i]
        } else {
            self.groups.push(ResourceGroup { resource: resource.clone(), series: HashMap::new() });
            self.groups.last_mut().expect("just pushed")
        }
    }

    /// One window's worth of series becomes one emitted event each, stamped with `now` and paired
    /// with the `SpanLink`s `ContributingContexts::into_links` built for it. **Every non-gauge
    /// accumulator is still removed unconditionally** -- tumbling, exactly as before this method
    /// gained retention. A gauge series with `gauge_retention > 0` instead survives into the next
    /// window, subject to `max_retained_gauge_series`: see the `gauge_retention`/
    /// `max_retained_gauge_series` field doc comments and
    /// `docs/adr/0008-aggregation-window-semantics.md`'s amendment for the full design.
    /// `current_batch_context` is *not* reset here -- it isn't part of any one window (see its own
    /// field doc comment).
    pub fn flush(&mut self, now: i64) -> FlushOutput {
        // Sampled before any series is touched below -- the peak-of-window value, at the one
        // point this aggregator already visits every series it holds. `aggregate`'s own
        // `SeriesKey` includes an event's whole attribute set (this module's doc comment), so an
        // un-pruned high-cardinality attribute reaching it shows up here first -- see
        // `docs/design/internal-telemetry.md` and `crate::keep`'s own module doc, which already
        // warns about exactly this failure mode.
        //
        // `.active` keeps its documented meaning -- series that received data *this* window --
        // rather than silently growing to include every retained-but-idle series too, which would
        // break its existing high-cardinality early-warning use. `.retained` is the new, separate
        // count for those.
        let mut active_series: usize = 0;
        let mut retained_series: usize = 0;
        for group in &self.groups {
            for state in group.series.values() {
                if state.updated_this_window {
                    active_series += 1;
                } else {
                    retained_series += 1;
                }
            }
        }
        self.telemetry.gauge("logit.transform.series.active", active_series as f64, &[]);
        self.telemetry.gauge("logit.transform.series.retained", retained_series as f64, &[]);
        self.telemetry.gauge("logit.transform.resource.groups", self.groups.len() as f64, &[]);

        // Every gauge series this flush decided to keep, not yet placed back into its group --
        // the cardinality cap below needs to see the *global* candidate set (across every
        // resource group) before any of them are final, since the cap is a whole-`Aggregator`
        // bound, not a per-group one. Each group's own `series` map is emptied via `mem::take`
        // below (not the whole `self.groups` Vec) specifically so surviving series can be
        // re-inserted straight back into their original group afterward, with no need to also
        // rebuild a parallel `resources`/`events_per_group` Vec pair just to remember which
        // group each one came from -- that would cost three extra allocations on the always-taken
        // default (`gauge_retention: 0`) path for no benefit, since nothing in that path ever
        // populates `survivors` at all.
        let mut survivors: Vec<(usize, SeriesKey, SeriesState)> = Vec::new();
        let mut total_dropped_links: u64 = 0;
        let mut evicted_idle: u64 = 0;
        let mut result = Vec::new();

        for (gi, group) in self.groups.iter_mut().enumerate() {
            let series = std::mem::take(&mut group.series);
            let mut events = Vec::new();
            for (key, mut state) in series {
                let is_gauge = matches!(state.accumulator, Accumulator::Gauge { .. });
                if state.updated_this_window {
                    let (links, dropped) = std::mem::take(&mut state.contexts).into_links();
                    total_dropped_links += dropped;

                    if is_gauge && self.gauge_retention > 0 {
                        // Retained: read the current value without consuming the accumulator
                        // (`Gauge`'s fields are plain `Copy` types, so this is free) rather than
                        // `into_kind()`, which would require cloning the whole accumulator just
                        // to keep a copy of it around afterward. `key.attributes` is cloned here
                        // -- and *only* here, not on the tumbling path below -- because `key`
                        // itself has to survive to become this series' map key again.
                        let value = match state.accumulator {
                            Accumulator::Gauge { value, .. } => value,
                            _ => unreachable!("is_gauge guards this"),
                        };
                        events.push((
                            Event::metric(
                                now,
                                key.attributes.clone(),
                                MetricRecord {
                                    name: key.name,
                                    kind: MetricKind::Gauge(value),
                                    unit: key.unit,
                                },
                            ),
                            links,
                        ));
                        // A retained gauge keeps `value` but resets `at` to `i64::MIN`: LWW is a
                        // within-window tiebreak, and retention must not promote it to a
                        // cross-window ordering guarantee -- an ordinary absolute gauge in the
                        // next window with an earlier source timestamp than this window's winner
                        // must still be accepted, not silently dropped by a stale `at`.
                        if let Accumulator::Gauge { at, .. } = &mut state.accumulator {
                            *at = i64::MIN;
                        }
                        state.updated_this_window = false;
                        state.idle_windows = 0;
                        survivors.push((gi, key, state));
                    } else {
                        // Not retained: consume the accumulator directly, exactly as before
                        // retention existed -- zero-cost for `Distribution` (the sketch's backing
                        // `Vec`s move rather than being cloned).
                        let kind = state.accumulator.into_kind();
                        events.push((
                            Event::metric(
                                now,
                                key.attributes,
                                MetricRecord { name: key.name, kind, unit: key.unit },
                            ),
                            links,
                        ));
                    }
                } else {
                    // A previously-retained, still-idle gauge series (only reachable when
                    // `gauge_retention > 0` -- nothing else survives to see an unupdated flush).
                    // Emits nothing this window -- not a repeat of last window's value, not a
                    // zero -- which is the whole point of retention: silence, not noise, for a
                    // gauge nobody touched.
                    state.contexts = ContributingContexts::default(); // never carried, even empty
                    state.idle_windows += 1;
                    if state.idle_windows < self.gauge_retention {
                        survivors.push((gi, key, state));
                    } else {
                        evicted_idle += 1;
                    }
                }
            }
            // Guarded on `events.is_empty()` *after* building, not on whether the group's series
            // were empty *before*: a group can now hold only retained-but-idle gauges (zero
            // events this window) without its series map being empty, and the old "skip an
            // empty-series group" guard would have let that same shape through as an empty batch.
            if !events.is_empty() {
                result.push((group.resource.clone(), events));
            }
        }

        // Cardinality cap: a hard bound on *all* retained gauge series at once, evicting the
        // least-recently-updated (highest `idle_windows`) first once exceeded. `gauge_retention`
        // alone bounds only how long one series survives; without this, a stream of C
        // never-repeating series names per window would hold C * gauge_retention series forever,
        // regardless of how short the retention window is.
        let mut evicted_cardinality: u64 = 0;
        if survivors.len() > self.max_retained_gauge_series {
            let excess = survivors.len() - self.max_retained_gauge_series;
            // Stable sort: ties (e.g. several series retained fresh this same flush, all at
            // `idle_windows == 0`) keep their relative order rather than picking an eviction
            // victim nondeterministically among equally-idle series.
            survivors.sort_by_key(|(_, _, state)| std::cmp::Reverse(state.idle_windows));
            survivors.drain(0..excess);
            evicted_cardinality = excess as u64;
        }

        // Re-insert the survivors into their original group's now-empty `series` map, then drop
        // any group left with none: every non-gauge series was always removed above; every gauge
        // series was either not retained, idle-evicted, or just cardinality-evicted -- a
        // counters-only resource group disappears from `self.groups` exactly as it always has,
        // tumbling or not.
        for (gi, key, state) in survivors {
            self.groups[gi].series.insert(key, state);
        }
        self.groups.retain(|g| !g.series.is_empty());

        if total_dropped_links > 0 {
            self.telemetry.count(
                "logit.transform.links.dropped",
                total_dropped_links as f64,
                &[("reason", "cardinality")],
            );
        }
        if evicted_idle > 0 {
            self.telemetry.count(
                "logit.transform.series.evicted",
                evicted_idle as f64,
                &[("reason", "idle")],
            );
        }
        if evicted_cardinality > 0 {
            self.telemetry.count(
                "logit.transform.series.evicted",
                evicted_cardinality as f64,
                &[("reason", "cardinality")],
            );
            // Never silent: hitting the cap means a later delta against an evicted series
            // resolves against 0.0 and produces a wrong-looking number, same as an unseeded
            // delta's own diagnostic just below it in spirit.
            self.diag.warn_throttled(
                "gauge_retention_full",
                format_args!(
                    "gauge retention cap ({}) exceeded; evicted {evicted_cardinality} least-\
                     recently-updated series -- a later delta against an evicted series will \
                     resolve against 0.0",
                    self.max_retained_gauge_series
                ),
            );
        }

        result
    }
}

/// `Aggregator`'s existing inherent methods already match `Transform`'s contract exactly (a
/// deliberate match, not a coincidence -- see `crate::Transform`'s doc comment): this impl is
/// pure delegation, no reshaping needed.
impl Transform for Aggregator {
    fn process(&mut self, resource: &Arc<Resource>, event: Event) -> Option<Event> {
        Aggregator::process(self, resource, event)
    }

    fn observe_batch_context(&mut self, ctx: TraceContext) {
        Aggregator::observe_batch_context(self, ctx)
    }

    fn flush_interval(&self) -> Option<Duration> {
        Some(self.interval())
    }

    fn flush(&mut self, now: i64) -> FlushOutput {
        Aggregator::flush(self, now)
    }
}

impl Accumulator {
    fn new_for(kind: &MetricKind) -> Self {
        match kind {
            MetricKind::Counter(_) => Accumulator::Counter(0.0),
            // `Gauge` and `GaugeDelta` share one accumulator -- they're not a kind conflict, just
            // two different ways to update the same running value (`docs/adr/
            // 0024-relative-gauge-adjustments.md`). This makes `new_for` non-injective on
            // purpose: two different `MetricKind`s map to the same `Accumulator` variant, which
            // would otherwise be easy to miss given the `unreachable!()` arm below makes the rest
            // of this mapping look total-and-one-to-one.
            MetricKind::Gauge(_) | MetricKind::GaugeDelta(_) => {
                Accumulator::Gauge { value: 0.0, at: i64::MIN }
            }
            MetricKind::Distribution(_) => Accumulator::Distribution(logit_core::DdSketch::new()),
            MetricKind::Set(_) | MetricKind::Histogram { .. } | MetricKind::Summary { .. } => {
                unreachable!("process() never creates an accumulator for a pass-through kind")
            }
        }
    }

    fn into_kind(self) -> MetricKind {
        match self {
            Accumulator::Counter(sum) => MetricKind::Counter(sum),
            Accumulator::Gauge { value, .. } => MetricKind::Gauge(value),
            Accumulator::Distribution(sketch) => MetricKind::Distribution(sketch),
        }
    }
}

/// A metric series' identity: name, unit, and attribute set. Used as a `HashMap` key, which
/// `AttrMap`/`Value` can't be directly -- neither implements `Eq`/`Hash` (`Value::F64` has no
/// total order). Projects `f64` through `to_bits()` for both comparison and hashing instead, so
/// `NaN` keys consistently with itself (`Eq`'s reflexivity requires `a == a`) rather than the
/// IEEE-754 "NaN != NaN" that would otherwise make an aggregation key grow without bound.
struct SeriesKey {
    name: Symbol,
    unit: Option<Symbol>,
    attributes: AttrMap,
}

impl PartialEq for SeriesKey {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.unit == other.unit
            && self.attributes.len() == other.attributes.len()
            && self
                .attributes
                .iter()
                .zip(other.attributes.iter())
                .all(|((k1, v1), (k2, v2))| k1 == k2 && value_key_eq(v1, v2))
    }
}

impl Eq for SeriesKey {}

impl Hash for SeriesKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.unit.hash(state);
        // `AttrMap::iter()` yields sorted-by-`Symbol` order (see its doc comment), so this is
        // stable regardless of insertion order -- two events with the same tags added in a
        // different order still hash and key identically.
        for (k, v) in self.attributes.iter() {
            k.hash(state);
            hash_value(v, state);
        }
    }
}

fn value_key_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => true,
        (Value::Bool(a), Value::Bool(b)) => a == b,
        (Value::I64(a), Value::I64(b)) => a == b,
        (Value::U64(a), Value::U64(b)) => a == b,
        (Value::F64(a), Value::F64(b)) => a.to_bits() == b.to_bits(),
        (Value::Bytes(a), Value::Bytes(b)) => a == b,
        (Value::Str(a), Value::Str(b)) => a == b,
        (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
        (Value::Array(a), Value::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| value_key_eq(a, b))
        }
        (Value::Map(a), Value::Map(b)) => {
            a.len() == b.len()
                && a.iter()
                    .zip(b.iter())
                    .all(|((k1, v1), (k2, v2))| k1 == k2 && value_key_eq(v1, v2))
        }
        _ => false,
    }
}

fn hash_value<H: Hasher>(v: &Value, state: &mut H) {
    // Hash the variant first (by discriminant-ish tag) so e.g. an empty `Array` and an empty
    // `Map` don't collide.
    match v {
        Value::Null => 0u8.hash(state),
        Value::Bool(b) => {
            1u8.hash(state);
            b.hash(state);
        }
        Value::I64(n) => {
            2u8.hash(state);
            n.hash(state);
        }
        Value::U64(n) => {
            3u8.hash(state);
            n.hash(state);
        }
        Value::F64(n) => {
            4u8.hash(state);
            n.to_bits().hash(state);
        }
        Value::Bytes(b) => {
            5u8.hash(state);
            b.hash(state);
        }
        Value::Str(s) => {
            6u8.hash(state);
            s.hash(state);
        }
        Value::Timestamp(t) => {
            7u8.hash(state);
            t.hash(state);
        }
        Value::Array(items) => {
            8u8.hash(state);
            items.len().hash(state);
            for item in items {
                hash_value(item, state);
            }
        }
        Value::Map(map) => {
            9u8.hash(state);
            map.len().hash(state);
            for (k, v) in map.iter() {
                k.hash(state);
                hash_value(v, state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::intern;

    fn metric_event(name: &str, kind: MetricKind, timestamp: i64) -> Event {
        Event::metric(
            timestamp,
            AttrMap::new(),
            MetricRecord { name: intern(name), kind, unit: None },
        )
    }

    fn metric_event_with_tags(
        name: &str,
        kind: MetricKind,
        timestamp: i64,
        tags: &[(&str, &str)],
    ) -> Event {
        let mut event = metric_event(name, kind, timestamp);
        for (k, v) in tags {
            event.attributes.insert(k, *v);
        }
        event
    }

    fn default_resource() -> Arc<Resource> {
        Arc::new(Resource::default())
    }

    /// Most existing assertions below don't care about the per-event `SpanLink` set `flush` now
    /// returns alongside each `Event` (`Transform::flush`'s doc comment) -- this flattens it away
    /// so those assertions keep the same shape they had before flush-side linking landed. Tests
    /// that *do* care about links call `agg.flush(...)` directly instead (see the
    /// `contributing_context`-prefixed tests below).
    fn flush_events(agg: &mut Aggregator, now: i64) -> Vec<(Arc<Resource>, Vec<Event>)> {
        agg.flush(now)
            .into_iter()
            .map(|(resource, events)| {
                (resource, events.into_iter().map(|(event, _links)| event).collect())
            })
            .collect()
    }

    fn kind_of(event: &Event) -> &MetricKind {
        &event
            .metrics
            .first()
            .unwrap_or_else(|| panic!("expected a metric on event {event:?}"))
            .kind
    }

    fn counter_value(kind: &MetricKind) -> f64 {
        match kind {
            MetricKind::Counter(v) => *v,
            other => panic!("expected Counter, got {other:?}"),
        }
    }

    #[test]
    fn counters_sum_within_a_window() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg
            .process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0))
            .is_none());
        assert!(agg
            .process(&resource, metric_event("hits", MetricKind::Counter(2.0), 1))
            .is_none());
        assert!(agg
            .process(&resource, metric_event("hits", MetricKind::Counter(3.0), 2))
            .is_none());

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed.len(), 1);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 6.0);
        assert_eq!(events[0].timestamp, 100);
    }

    #[test]
    fn gauge_keeps_the_value_with_the_latest_source_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        // Deliberately out of arrival order: the later-timestamped value (5) arrives first.
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(5.0), 50));
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(1.0), 10));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 5.0, "should keep the value stamped at t=50"),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn distributions_merge_via_ddsketch() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for v in [10.0, 20.0, 30.0, 40.0, 50.0] {
            let mut sketch = logit_core::DdSketch::new();
            sketch.add(v);
            agg.process(&resource, metric_event("latency", MetricKind::Distribution(sketch), 0));
        }

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Distribution(sketch) => {
                assert_eq!(sketch.count(), 5);
                let median = sketch.quantile(0.5).expect("merged sketch has a median");
                assert!((median - 30.0).abs() < 5.0, "median should be near 30, got {median}");
            }
            other => panic!("expected Distribution, got {other:?}"),
        }
    }

    #[test]
    fn a_second_flush_after_the_first_emits_nothing() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));

        assert_eq!(agg.flush(100).len(), 1, "first flush should emit the window");
        assert!(agg.flush(200).is_empty(), "tumbling: state resets, second flush is empty");
    }

    #[test]
    fn logs_and_spans_pass_through_untouched() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let log = Event::log(
            0,
            AttrMap::new(),
            logit_core::LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
            },
        );
        let passed = agg.process(&resource, log);
        assert!(passed.is_some(), "a log event should pass through, not be absorbed");
        assert!(agg.flush(100).is_empty(), "nothing should have been accumulated");
    }

    #[test]
    fn set_histogram_and_summary_pass_through_untouched() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        for kind in [
            MetricKind::Set(logit_core::HyperLogLog::default()),
            MetricKind::Histogram { buckets: vec![(10.0, 1)] },
            MetricKind::Summary { quantiles: vec![(0.5, 1.0)] },
        ] {
            let event = metric_event("m", kind, 0);
            assert!(
                agg.process(&resource, event).is_some(),
                "a kind with no defined merge rule should pass through"
            );
        }
        assert!(agg.flush(100).is_empty());
    }

    /// Guards `process`'s pass-through `matches!` directly, not just by implication
    /// (`docs/adr/0024-relative-gauge-adjustments.md`): a `GaugeDelta` must be absorbed, not
    /// forwarded, now that workstream B gives it a real resolution -- the opposite of what
    /// workstream A's version of this test pinned. A comment alone on the `matches!` wouldn't
    /// catch a future change that silently defeated the whole feature by adding `GaugeDelta`
    /// back to that list; a test does.
    #[test]
    fn gauge_delta_is_not_in_the_pass_through_matches() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let event = metric_event("temp", MetricKind::GaugeDelta(5.0), 0);
        assert!(agg.process(&resource, event).is_none(), "a GaugeDelta-only event should absorb");
    }

    /// A delta opening a brand-new series resolves against 0.0 (statsd's own rule for an
    /// unseeded gauge) and fires the `gauge_delta_unseeded` counter -- correct per spec, but
    /// indistinguishable from a real 0.0 in the emitted number unless reported.
    #[test]
    fn a_delta_into_an_empty_window_resolves_against_zero_and_fires_the_unseeded_counter() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        assert!(agg
            .process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 0))
            .is_none());

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 5.0, "unseeded delta resolves against 0.0"),
            other => panic!("expected Gauge, got {other:?}"),
        }

        let drained = registry.drain(0);
        let unseeded = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Counter(v)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.gauge.delta.unseeded" =>
                {
                    Some(*v)
                }
                _ => None,
            })
        });
        assert_eq!(unseeded, Some(1.0), "the unseeded delta should be counted");
    }

    #[test]
    fn absolute_then_delta_adds_to_the_absolute() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 1));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 15.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// The delta is subsumed by the later absolute, not added underneath it -- an absolute always
    /// *replaces* the running value, never adds to it.
    #[test]
    fn delta_then_absolute_is_subsumed_by_the_absolute() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 0));
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 1));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 10.0, "the absolute should win outright"),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// Pins the `..` on `at`: a delta must never advance the last-write-wins timestamp. Sequence:
    /// an absolute at t=50 sets `at` to 50; a delta at t=60 must apply (arrival order) but must
    /// leave `at` at 50; a second absolute, also stamped t=50, then arrives -- it only wins the
    /// `event.timestamp >= *at` tiebreak if `at` is still 50. If the delta had incorrectly
    /// advanced `at` to 60, this final absolute (50 >= 60 is false) would be silently dropped and
    /// the flushed value would stay 15, not become 99 -- so asserting 99 is what actually pins
    /// this, not just an assertion that *some* value came out.
    #[test]
    fn a_delta_never_advances_the_last_write_wins_timestamp() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 50));
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 60));
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(99.0), 50));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 99.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn two_deltas_accumulate() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 1));
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(-3.0), 2));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 12.0),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    /// A `GaugeDelta` against an already-accumulating `Counter` series is a real kind conflict --
    /// the same `_ => false` / `kind_conflict` path a `Gauge` vs. `Counter` conflict always took.
    #[test]
    fn gauge_delta_against_a_counter_series_is_a_kind_conflict_and_is_forwarded() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg.process(&resource, metric_event("m", MetricKind::Counter(1.0), 0)).is_none());

        let conflicting = metric_event("m", MetricKind::GaugeDelta(5.0), 0);
        let passed = agg.process(&resource, conflicting);
        assert!(passed.is_some(), "the conflicting delta should be forwarded, not absorbed");
        assert!(matches!(passed.unwrap().metrics[0].kind, MetricKind::GaugeDelta(v) if v == 5.0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(counter_value(kind_of(&events[0])), 1.0, "the counter should be untouched");
    }

    /// `into_kind` always emits `MetricKind::Gauge`, never `MetricKind::GaugeDelta` -- a delta
    /// never survives `aggregate`, however it entered.
    #[test]
    fn a_gauge_delta_never_survives_aggregate() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert!(
            !matches!(kind_of(&events[0]), MetricKind::GaugeDelta(_)),
            "GaugeDelta must never be the kind of a flushed event"
        );
        assert!(matches!(kind_of(&events[0]), MetricKind::Gauge(_)));
    }

    #[test]
    fn distinct_tag_sets_stay_distinct_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event_with_tags("hits", MetricKind::Counter(1.0), 0, &[("host", "a")]),
        );
        agg.process(
            &resource,
            metric_event_with_tags("hits", MetricKind::Counter(1.0), 0, &[("host", "b")]),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 2, "different tag values should be different series");
    }

    #[test]
    fn same_tags_in_different_insertion_order_collide_into_one_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.process(
            &resource,
            metric_event_with_tags(
                "hits",
                MetricKind::Counter(1.0),
                0,
                &[("host", "a"), ("env", "prod")],
            ),
        );
        agg.process(
            &resource,
            metric_event_with_tags(
                "hits",
                MetricKind::Counter(1.0),
                0,
                &[("env", "prod"), ("host", "a")],
            ),
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "AttrMap keeps sorted order regardless of insertion order");
        assert_eq!(counter_value(kind_of(&events[0])), 2.0);
    }

    #[test]
    fn different_resources_do_not_fold_together() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let mut resource_a = Resource::default();
        resource_a.attributes.insert("host", "a");
        let mut resource_b = Resource::default();
        resource_b.attributes.insert("host", "b");

        agg.process(&Arc::new(resource_a), metric_event("hits", MetricKind::Counter(1.0), 0));
        agg.process(&Arc::new(resource_b), metric_event("hits", MetricKind::Counter(1.0), 0));

        let flushed = agg.flush(100);
        assert_eq!(flushed.len(), 2, "distinct resources should produce distinct batches");
    }

    #[test]
    fn nan_attribute_value_keys_stably_across_events() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut e1 = metric_event("hits", MetricKind::Counter(1.0), 0);
        e1.attributes.insert("score", f64::NAN);
        let mut e2 = metric_event("hits", MetricKind::Counter(1.0), 0);
        e2.attributes.insert("score", f64::NAN);

        agg.process(&resource, e1);
        agg.process(&resource, e2);

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "two NaN-tagged events should key into the same series");
        assert_eq!(counter_value(kind_of(&events[0])), 2.0);
    }

    #[test]
    fn a_kind_conflict_on_one_series_is_forwarded_not_dropped_or_a_panic() {
        // Same name, same (empty) tags, different kinds -- e.g. a misconfigured statsd source
        // sending both `foo:1|c` and `foo:1|g`. There's no correct merge, so this must forward
        // the conflicting event rather than panic (this exact shape used to hit an `unreachable!`)
        // or silently corrupt the counter accumulator already in progress.
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg.process(&resource, metric_event("m", MetricKind::Counter(1.0), 0)).is_none());
        let conflicting = metric_event("m", MetricKind::Gauge(5.0), 0);
        let passed = agg.process(&resource, conflicting);
        assert!(passed.is_some(), "the conflicting event should be forwarded, not absorbed");

        // The counter accumulator should be untouched by the conflicting event.
        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    /// The headline test for the multi-payload model (docs/adr/0012-multi-payload-events.md): an
    /// event carrying both a log and a counter has the counter absorbed into window state while
    /// the log is still forwarded -- pass-through is per metric now, not per event.
    #[test]
    fn a_log_event_carrying_a_counter_has_the_counter_absorbed_and_the_log_forwarded() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = Event::log(
            0,
            AttrMap::new(),
            logit_core::LogRecord {
                message: Value::str("hello"),
                severity: None,
                body_format: logit_core::BodyFormat::Raw,
            },
        );
        event.metrics.push(MetricRecord {
            name: intern("hits"),
            kind: MetricKind::Counter(1.0),
            unit: None,
        });

        let passed = agg.process(&resource, event).expect("the log half should be forwarded");
        assert!(passed.metrics.is_empty(), "the counter should have been absorbed");
        assert_eq!(
            passed.log.as_ref().expect("the log should still be present").message,
            Value::str("hello")
        );

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    #[test]
    fn a_mixed_metric_event_absorbs_the_mergeable_ones_and_keeps_the_rest() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("hits", MetricKind::Counter(1.0), 0);
        event.metrics.push(MetricRecord {
            name: intern("sizes"),
            kind: MetricKind::Histogram { buckets: vec![(10.0, 1)] },
            unit: None,
        });

        let passed =
            agg.process(&resource, event).expect("the histogram should survive as the remainder");
        assert_eq!(passed.metrics.len(), 1, "only the unmergeable histogram should remain");
        assert!(matches!(passed.metrics[0].kind, MetricKind::Histogram { .. }));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 1.0);
    }

    #[test]
    fn a_metric_only_event_fully_absorbed_returns_none() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("a", MetricKind::Counter(1.0), 0);
        event.metrics.push(MetricRecord {
            name: intern("b"),
            kind: MetricKind::Counter(2.0),
            unit: None,
        });

        assert!(
            agg.process(&resource, event).is_none(),
            "an event with nothing left to forward should still return None"
        );
    }

    #[test]
    fn two_metrics_of_the_same_series_on_one_event_sum_into_one_flushed_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let mut event = metric_event("hits", MetricKind::Counter(1.0), 0);
        event.metrics.push(MetricRecord {
            name: intern("hits"),
            kind: MetricKind::Counter(2.0),
            unit: None,
        });

        assert!(agg.process(&resource, event).is_none());

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1, "both metrics should key into the same series");
        assert_eq!(counter_value(kind_of(&events[0])), 3.0);
    }

    #[test]
    fn a_kind_conflict_leaves_only_the_conflicting_metric_on_the_event() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        assert!(agg.process(&resource, metric_event("m", MetricKind::Counter(1.0), 0)).is_none());

        let mut event = metric_event("m", MetricKind::Counter(1.0), 0);
        event.metrics.push(MetricRecord {
            name: intern("m"),
            kind: MetricKind::Gauge(5.0),
            unit: None,
        });

        let passed =
            agg.process(&resource, event).expect("the conflicting gauge should be forwarded");
        assert_eq!(passed.metrics.len(), 1, "the absorbed counter should not also be forwarded");
        assert!(matches!(passed.metrics[0].kind, MetricKind::Gauge(v) if v == 5.0));

        let flushed = flush_events(&mut agg, 100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        assert_eq!(counter_value(kind_of(&events[0])), 2.0, "both counters should have merged");
    }

    /// Two batches, two different `TraceContext`s, both contributing to the same series --
    /// `flush` should link both, per `ContributingContexts`' whole point.
    #[test]
    fn flush_links_every_distinct_context_that_contributed_to_a_series() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();

        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));

        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));

        let flushed = agg.flush(100);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        let (_, links) = &events[0];
        assert_eq!(links.len(), 2, "both contributing traces should be linked");
        let trace_ids: Vec<[u8; 16]> = links.iter().map(|l| l.trace_id).collect();
        assert!(trace_ids.contains(&ctx_a.trace_id));
        assert!(trace_ids.contains(&ctx_b.trace_id));
    }

    /// The same context observed twice (e.g. two events from the same batch touching the same
    /// series) shouldn't produce two links -- `ContributingContexts::observe`'s `contains` check.
    #[test]
    fn repeat_events_under_the_same_context_dont_duplicate_a_link() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        agg.observe_batch_context(TraceContext::new_root());
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));

        let flushed = agg.flush(100);
        let (_, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "the same context observed twice should still be one link");
    }

    /// Nine distinct contributing contexts on one series: the cap (`MAX_CONTRIBUTING_CONTEXTS_PER_SERIES`,
    /// 8) admits the first 8, the 9th is dropped and counted -- the same "bound and count the
    /// drop, never silently grow" shape `ComponentBuffer::upsert`'s own cardinality cap uses.
    #[test]
    fn a_series_fed_by_more_than_the_cap_drops_and_counts_the_rest() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        for _ in 0..9 {
            agg.observe_batch_context(TraceContext::new_root());
            agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));
        }

        let flushed = agg.flush(100);
        let (_, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 8, "capped at MAX_CONTRIBUTING_CONTEXTS_PER_SERIES");

        let drained = registry.drain(0);
        let dropped = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Counter(v)
                    if logit_core::interner::resolve(m.name) == "logit.transform.links.dropped" =>
                {
                    Some(*v)
                }
                _ => None,
            })
        });
        assert_eq!(dropped, Some(1.0), "the 9th distinct context should be dropped and counted");
    }

    /// Tumbling, not sliding (ADR 0008): a series' contributing-context set resets with the rest
    /// of its accumulator state at flush, exactly like `a_second_flush_after_the_first_emits_nothing`
    /// already pins for the accumulated value itself.
    #[test]
    fn contributing_contexts_reset_after_a_flush() {
        let mut agg = Aggregator::new(Duration::from_secs(10));
        let resource = default_resource();
        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));
        agg.flush(100); // first window's links discarded along with its accumulator

        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));

        let flushed = agg.flush(200);
        let (_, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "tumbling: the first window's context shouldn't carry over");
        assert_eq!(
            links[0].trace_id, ctx_b.trace_id,
            "only the second window's context should be linked"
        );
    }

    // Takes already-drained `events`, not a `&Registry` -- `Registry::drain` is consuming (it
    // empties every buffer via `mem::take`), so calling it once per assertion in the same test
    // would make every assertion after the first see an already-emptied registry.
    fn gauge_value(events: &[Event], name: &str) -> Option<f64> {
        events.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Gauge(v) if logit_core::interner::resolve(m.name) == name => Some(*v),
                _ => None,
            })
        })
    }

    #[test]
    fn flush_records_active_series_and_resource_group_counts() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);
        let resource = default_resource();

        agg.process(&resource, metric_event("a", MetricKind::Counter(1.0), 0));
        agg.process(&resource, metric_event("b", MetricKind::Counter(1.0), 0));
        agg.flush(100);

        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.series.active"), Some(2.0));
        assert_eq!(gauge_value(&events, "logit.transform.resource.groups"), Some(1.0));
    }

    #[test]
    fn flush_with_nothing_accumulated_records_zero_series_and_zero_groups() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);

        agg.flush(100);

        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.series.active"), Some(0.0));
        assert_eq!(gauge_value(&events, "logit.transform.resource.groups"), Some(0.0));
    }

    #[test]
    fn flush_sums_series_across_multiple_resource_groups() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_telemetry(telemetry);

        let mut resource_a = logit_core::AttrMap::new();
        resource_a.insert("host", "a");
        let resource_a = Arc::new(Resource { attributes: resource_a });
        let mut resource_b = logit_core::AttrMap::new();
        resource_b.insert("host", "b");
        let resource_b = Arc::new(Resource { attributes: resource_b });

        agg.process(&resource_a, metric_event("a", MetricKind::Counter(1.0), 0));
        agg.process(&resource_b, metric_event("b", MetricKind::Counter(1.0), 0));
        agg.process(&resource_b, metric_event("c", MetricKind::Counter(1.0), 0));
        agg.flush(100);

        let events = registry.drain(0);
        assert_eq!(gauge_value(&events, "logit.transform.series.active"), Some(3.0));
        assert_eq!(gauge_value(&events, "logit.transform.resource.groups"), Some(2.0));
    }

    // -----------------------------------------------------------------------------------------
    // Workstream C: gauge series retention across the window boundary
    // (docs/adr/0008-aggregation-window-semantics.md's amendment)
    // -----------------------------------------------------------------------------------------

    #[test]
    fn a_delta_in_the_next_window_resolves_against_the_previous_windows_final_value() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_gauge_retention(5, 100);
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        let flushed = flush_events(&mut agg, 100);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 10.0),
            other => panic!("expected Gauge, got {other:?}"),
        }

        // Window 2 sees only a delta, no absolute -- it must resolve against window 1's value.
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 150));
        let flushed = flush_events(&mut agg, 200);
        assert_eq!(flushed.len(), 1);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => {
                assert_eq!(*v, 15.0, "the delta should resolve against window 1's final value")
            }
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn a_retained_idle_gauge_emits_nothing_that_window() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_gauge_retention(5, 100);
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        assert_eq!(flush_events(&mut agg, 100).len(), 1, "window 1 emits the gauge");

        // Window 2: nothing touches "conns" at all -- not a repeat of 10.0, not a 0.0, nothing.
        let flushed = agg.flush(200);
        assert!(flushed.is_empty(), "an idle retained gauge must emit nothing that window");
    }

    #[test]
    fn an_idle_gauge_is_evicted_after_gauge_retention_windows_and_a_later_delta_resolves_against_zero(
    ) {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_gauge_retention(2, 100)
            .with_telemetry(telemetry);
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        assert_eq!(agg.flush(100).len(), 1, "window 1: emits, idle_windows resets to 0");
        assert!(agg.flush(200).is_empty(), "window 2: idle_windows -> 1, still under retention 2");
        assert!(agg.flush(300).is_empty(), "window 3: idle_windows -> 2, now evicted");

        // A delta after eviction opens a brand-new series, resolving against 0.0.
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(5.0), 350));
        let flushed = flush_events(&mut agg, 400);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 5.0, "should resolve against 0.0 post-eviction"),
            other => panic!("expected Gauge, got {other:?}"),
        }

        let drained = registry.drain(0);
        let unseeded = drained.iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Counter(v)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.gauge.delta.unseeded" =>
                {
                    Some(*v)
                }
                _ => None,
            })
        });
        assert_eq!(unseeded, Some(1.0), "the post-eviction delta should count as unseeded");
    }

    /// `gauge_retention: 0` must reproduce today's exact tumbling behavior byte-for-byte -- the
    /// migration story for anyone not opting into retention (which is every existing config,
    /// since it's the field's default).
    #[test]
    fn gauge_retention_zero_reproduces_the_strictly_tumbling_output() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_gauge_retention(0, 0);
        let resource = default_resource();
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(5.0), 50));
        agg.process(&resource, metric_event("temp", MetricKind::Gauge(1.0), 10));

        let flushed = flush_events(&mut agg, 100);
        assert_eq!(flushed.len(), 1);
        let (_, events) = &flushed[0];
        assert_eq!(events.len(), 1);
        match kind_of(&events[0]) {
            MetricKind::Gauge(v) => assert_eq!(*v, 5.0, "should keep the value stamped at t=50"),
            other => panic!("expected Gauge, got {other:?}"),
        }

        assert!(
            agg.flush(200).is_empty(),
            "gauge_retention: 0 must not retain anything across flushes, exactly like today"
        );
    }

    /// The regression test that matters most (per the plan): retaining `Gauge { value, at }`
    /// verbatim would retain `at` too, so an ordinary absolute gauge in window 2 with an earlier
    /// source timestamp than window 1's winner would be silently dropped by the last-write-wins
    /// rule (`event.timestamp` compared against `at`) -- a new failure class that grows with
    /// retention depth. A retained gauge must reset `at` to `i64::MIN`: LWW is a within-window
    /// tiebreak, and retention must not promote it to a cross-window ordering guarantee.
    #[test]
    fn an_absolute_gauge_in_the_next_window_with_an_earlier_timestamp_is_still_accepted() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_gauge_retention(5, 100);
        let resource = default_resource();
        // Window 1's winner is stamped at t=500.
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 500));
        flush_events(&mut agg, 1000);

        // Window 2: an absolute gauge stamped at t=1 -- far earlier than window 1's `at` (500).
        // If retention had carried `at` across the boundary, this would fail `>= at` and be
        // silently dropped.
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(99.0), 1));
        let flushed = flush_events(&mut agg, 2000);
        match kind_of(&flushed[0].1[0]) {
            MetricKind::Gauge(v) => assert_eq!(
                *v, 99.0,
                "an earlier-timestamped absolute must still win against a reset `at`"
            ),
            other => panic!("expected Gauge, got {other:?}"),
        }
    }

    #[test]
    fn a_counter_series_does_not_survive_its_window_even_with_gauge_retention_enabled() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_gauge_retention(5, 100);
        let resource = default_resource();
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));
        assert_eq!(flush_events(&mut agg, 100).len(), 1);
        assert!(
            agg.flush(200).is_empty(),
            "a counter series must never survive a flush, retention enabled or not"
        );
    }

    #[test]
    fn a_counters_only_resource_group_disappears_from_groups() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_gauge_retention(5, 100)
            .with_telemetry(telemetry);
        let resource = default_resource();
        agg.process(&resource, metric_event("hits", MetricKind::Counter(1.0), 0));
        agg.flush(100);
        // A second flush's own `resource.groups` sample reflects state as of right before it --
        // i.e. right after the first flush pruned the now-empty counters-only group.
        agg.flush(200);

        let events = registry.drain(0);
        assert_eq!(
            gauge_value(&events, "logit.transform.resource.groups"),
            Some(0.0),
            "a counters-only resource group should disappear from `groups` after its flush"
        );
    }

    #[test]
    fn the_cardinality_cap_evicts_and_fires_series_evicted_cardinality() {
        let registry = logit_core::Registry::new();
        let telemetry = registry.telemetry_for("windowed", "aggregate", "transform");
        let mut agg = Aggregator::new(Duration::from_secs(10))
            .with_gauge_retention(5, 2)
            .with_telemetry(telemetry);
        let resource = default_resource();
        for i in 0..3 {
            agg.process(&resource, metric_event(&format!("g{i}"), MetricKind::Gauge(i as f64), 0));
        }
        // 3 fresh gauge series, all wanting retention, but the cap is 2 -- one must be evicted.
        agg.flush(100);

        let events = registry.drain(0);
        let evicted_cardinality = events.iter().find_map(|e| {
            if e.attributes.get("reason").and_then(|v| v.as_str()) != Some("cardinality") {
                return None;
            }
            e.metrics.iter().find_map(|m| match &m.kind {
                MetricKind::Counter(v)
                    if logit_core::interner::resolve(m.name)
                        == "logit.transform.series.evicted" =>
                {
                    Some(*v)
                }
                _ => None,
            })
        });
        assert_eq!(evicted_cardinality, Some(1.0), "exactly one series should exceed the cap");
    }

    /// `ContributingContexts`' doc comment scopes it to "since the last flush" -- a carried-over
    /// context on a retained gauge would produce the silently-wrong `SpanLink` parent ADR 0020
    /// rejected. `mem::take`n every flush unconditionally, retained or not.
    #[test]
    fn contexts_are_never_carried_across_a_flush_even_for_a_retained_gauge() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_gauge_retention(5, 100);
        let resource = default_resource();
        let ctx_a = TraceContext::new_root();
        agg.observe_batch_context(ctx_a);
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));

        let flushed = agg.flush(100);
        let (_, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(links.len(), 1, "window 1 links its one contributing context");

        // Window 2: the series is retained-idle, contributing nothing -- if its context leaked
        // forward, a *later* series sharing the same key would incorrectly inherit ctx_a's link.
        // Touch it again with a different context and confirm only the new one is linked.
        let ctx_b = TraceContext::new_root();
        agg.observe_batch_context(ctx_b);
        agg.process(&resource, metric_event("conns", MetricKind::GaugeDelta(1.0), 150));
        let flushed = agg.flush(200);
        let (_, events) = &flushed[0];
        let (_, links) = &events[0];
        assert_eq!(
            links.len(),
            1,
            "only ctx_b should be linked -- ctx_a must not have carried over"
        );
        assert_eq!(links[0].trace_id, ctx_b.trace_id);
    }

    /// A group holding only retained-but-idle gauges emits zero events; `flush` must not send an
    /// empty `(resource, events)` batch downstream for it.
    #[test]
    fn flush_emits_no_empty_resource_events_group() {
        let mut agg = Aggregator::new(Duration::from_secs(10)).with_gauge_retention(5, 100);
        let resource = default_resource();
        agg.process(&resource, metric_event("conns", MetricKind::Gauge(10.0), 0));
        agg.flush(100); // retains "conns", idle from here on

        // Window 2: "conns" is retained-idle (no event); nothing else touches this resource.
        let flushed = agg.flush(200);
        assert!(
            flushed.iter().all(|(_, events)| !events.is_empty()),
            "flush must never emit a (resource, events) pair with an empty events list"
        );
        assert!(flushed.is_empty(), "the only group present here has nothing to emit at all");
    }
}
