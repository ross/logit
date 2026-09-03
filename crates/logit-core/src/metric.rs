use crate::interner::Symbol;

#[derive(Debug, Clone)]
pub struct MetricRecord {
    pub name: Symbol,
    pub kind: MetricKind,
    pub unit: Option<Symbol>,
}

/// Metric kinds are chosen to be *mergeable*: the split-collection topology (`docs/OVERVIEW.md`)
/// means two edge nodes' aggregates may need combining downstream, and that has to be exact where
/// the math allows it (`Counter`, `Gauge`, `Set`) and correctly error-bounded where it can't
/// (`Distribution`). See `docs/design/data-model.md`.
#[derive(Debug, Clone)]
pub enum MetricKind {
    Counter(f64),
    Gauge(f64),
    /// A *relative* adjustment to a gauge's previous value (statsd/DogStatsD's leading `+`/`-`
    /// syntax, `crates/logit-inputs/src/statsd.rs`) -- **unresolved**. This variant must never
    /// reach a sink; it is resolved into an ordinary [`MetricKind::Gauge`] by the `aggregate`
    /// transform (`crates/logit-transforms/src/aggregate.rs`), which is the only component that
    /// carries the running gauge value a delta needs to apply against. See
    /// `docs/adr/relative-gauge-adjustments.md`.
    GaugeDelta(f64),
    Set(HyperLogLog),
    Distribution(DdSketch),
    /// Fixed-bucket histogram, e.g. a Prometheus-style scrape input. Each `(bound, count)` pair is
    /// that bucket's own count, not a cumulative running total up to `bound` -- safe to state
    /// plainly since nothing in this codebase produces a `Histogram` yet (`crates/logit-proto/src/
    /// otlp/metrics.rs` is the first, decoding OTLP's `HistogramDataPoint`/`ExponentialHistogramDataPoint`,
    /// both of which are per-bucket on the wire too).
    Histogram {
        buckets: Vec<(f64, u64)>,
    },
    /// Pre-computed quantiles, e.g. some scrape inputs report these directly.
    Summary {
        quantiles: Vec<(f64, f64)>,
    },
}

/// A mergeable quantile sketch, wrapping `sketches_ddsketch::DDSketch` (per
/// `docs/design/data-model.md` -- merges with a guaranteed relative-error bound, unlike naive
/// percentile-of-percentiles, which is load-bearing for the split-collection topology in
/// `docs/OVERVIEW.md`).
#[derive(Clone)]
pub struct DdSketch(sketches_ddsketch::DDSketch);

// `sketches_ddsketch::DDSketch` doesn't implement `Debug`; summarize instead of deriving.
impl std::fmt::Debug for DdSketch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DdSketch").field("count", &self.0.count()).finish()
    }
}

impl DdSketch {
    pub fn new() -> Self {
        Self(sketches_ddsketch::DDSketch::new(sketches_ddsketch::Config::defaults()))
    }

    pub fn add(&mut self, value: f64) {
        self.0.add(value);
    }

    /// Adds `value` as `count` weighted samples -- e.g. what a sampled statsd timing/histogram
    /// line needs to extrapolate `100|ms|@0.1` into ten samples rather than one
    /// (`crates/logit-inputs/src/statsd.rs`). Delegates directly to
    /// `sketches_ddsketch::DDSketch::add_with_count`, which computes the target bin once and
    /// increments its stored count by `count` in constant time -- not a loop calling `add`
    /// `count` times, and not a single-bucket sketch `merge`-d in via binary doubling either:
    /// both would cost real, avoidable work (an O(count) loop, or O(log count) allocations for
    /// the merge alternative) that `add_with_count` doesn't pay. Same zero-additional-allocation
    /// property either way -- the bin `Vec` is allocated once, the first time any sample ever
    /// lands in this sketch -- but O(1) instead of O(count) in CPU cost, which matters because
    /// `count` can be attacker-influenced (a sampled statsd line's extrapolated weight). `count
    /// == 0` is a no-op (`add_with_count`'s own contract).
    pub fn add_weighted(&mut self, value: f64, count: u64) {
        self.0.add_with_count(value, count);
    }

    /// Merges `other` into `self`. Every `DdSketch` in this codebase is built with
    /// `Config::defaults()` (via [`DdSketch::new`]), so the mismatched-config failure case this
    /// can't-actually-happen -- if that stops being true, this needs a real `Result`.
    pub fn merge(&mut self, other: &DdSketch) {
        self.0.merge(&other.0).expect("DdSketch configs always match (Config::defaults())");
    }

    pub fn quantile(&self, q: f64) -> Option<f64> {
        self.0.quantile(q).ok().flatten()
    }

    pub fn count(&self) -> usize {
        self.0.count()
    }
}

impl Default for DdSketch {
    fn default() -> Self {
        Self::new()
    }
}

/// Placeholder for a mergeable cardinality estimator (candidate: `cardinality-estimator`). Merges
/// (unions) exactly by construction, which `Set` needs for the same distributed-aggregation
/// reason `Distribution` needs `DdSketch`. See `docs/design/data-model.md`. Still a stub as of
/// the statsd decoder (`crates/logit-inputs/src/statsd.rs`): its `s` (set) metric type returns a
/// decode error rather than silently losing data until this is wired up.
#[derive(Debug, Clone, Default)]
pub struct HyperLogLog {
    // TODO: wrap a real HLL implementation.
    _todo: (),
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `add_weighted(v, 1)` is the `count == 1` case a sample-rate-1 statsd line always takes --
    /// it must be indistinguishable from the plain `add(v)` path it replaces there.
    #[test]
    fn add_weighted_with_count_one_matches_plain_add() {
        let mut weighted = DdSketch::new();
        weighted.add_weighted(42.0, 1);

        let mut plain = DdSketch::new();
        plain.add(42.0);

        assert_eq!(weighted.count(), plain.count());
        assert_eq!(weighted.quantile(0.5), plain.quantile(0.5));
        assert_eq!(weighted.quantile(0.99), plain.quantile(0.99));
    }

    /// `add_weighted(v, 100)` extrapolates one sample into a hundred identical ones -- `count()`
    /// reports the extrapolated population, and every quantile (not just the median) lands within
    /// `DdSketch`'s documented 1% relative-error bound of `v`, since all 100 samples fall in the
    /// same bucket. Not *exactly* `v`: DDSketch is a bucketed approximation by construction --
    /// `quantile` returns a bucket boundary estimate, not the stored value -- so even a sketch fed
    /// nothing but identical samples doesn't round-trip them exactly.
    #[test]
    fn add_weighted_with_large_count_extrapolates_count_and_every_quantile() {
        let mut sketch = DdSketch::new();
        sketch.add_weighted(7.5, 100);

        assert_eq!(sketch.count(), 100);
        for q in [0.0, 0.1, 0.5, 0.9, 0.99, 1.0] {
            let value = sketch.quantile(q).expect("quantile should be present");
            let relative_error = (value - 7.5).abs() / 7.5;
            assert!(
                relative_error <= 0.01,
                "quantile({q}) = {value} is more than 1% away from the true value 7.5"
            );
        }
    }

    /// `add_weighted(v, 0)` must be a true no-op -- the clamp in `statsd.rs` never produces a
    /// zero weight, but the method's own contract should hold regardless of the caller.
    #[test]
    fn add_weighted_with_zero_count_is_a_no_op() {
        let mut sketch = DdSketch::new();
        sketch.add_weighted(1.0, 0);
        assert_eq!(sketch.count(), 0);
        assert_eq!(sketch.quantile(0.5), None);
    }

    /// A weighted add still respects `Config::defaults()`'s documented 1% relative-accuracy bound
    /// (`sketches_ddsketch::Config::defaults()`: alpha = 0.01) -- extrapolating via repeated `add`
    /// must not degrade the sketch's error guarantee versus the same number of genuine samples.
    #[test]
    fn add_weighted_quantile_stays_within_the_configured_relative_error_bound() {
        let mut sketch = DdSketch::new();
        sketch.add_weighted(200.0, 50);

        let q = sketch.quantile(0.5).expect("quantile should be present");
        let relative_error = (q - 200.0).abs() / 200.0;
        assert!(
            relative_error <= 0.01,
            "quantile {q} is more than 1% away from the true value 200.0"
        );
    }
}
