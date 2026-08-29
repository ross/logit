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

/// Placeholder for a mergeable quantile sketch (chosen: `sketches-ddsketch`, per
/// `docs/design/data-model.md` -- merges with a guaranteed relative-error bound, unlike naive
/// percentile-of-percentiles). Wraps the real crate's type once the `aggregate` processor is
/// implemented; kept as a stub here so the core event model doesn't carry that dependency before
/// anything uses it.
#[derive(Debug, Clone, Default)]
pub struct DdSketch {
    // TODO: wrap `sketches_ddsketch::DDSketch`.
    _todo: (),
}

/// Placeholder for a mergeable cardinality estimator (candidate: `cardinality-estimator`). Merges
/// (unions) exactly by construction, which `Set` needs for the same distributed-aggregation
/// reason `Distribution` needs `DdSketch`. See `docs/design/data-model.md`.
#[derive(Debug, Clone, Default)]
pub struct HyperLogLog {
    // TODO: wrap a real HLL implementation.
    _todo: (),
}
