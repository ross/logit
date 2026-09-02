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
    /// `docs/adr/0024-relative-gauge-adjustments.md`.
    GaugeDelta(f64),
    Set(HyperLogLog),
    Distribution(DdSketch),
    /// Fixed-bucket histogram, e.g. a Prometheus-style scrape input.
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
