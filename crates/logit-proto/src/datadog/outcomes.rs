//! What the decoders and encoders count for a metric or APM stats record they don't carry
//! whole. A call site names an outcome, and the outcome names its counter and tag, so a fault
//! can't be counted `skipped` or a by-design skip `dropped` by a typo at the call site
//! ([ADR `telemetry-loss-vocabulary`](../../../../docs/adr/telemetry-loss-vocabulary.md)).
//!
//! A malformed or unsupported input is `dropped`: a different sender or a different `logit`
//! would have delivered it. Nothing to carry (a `null` value, an empty sketch, a record flagged
//! no-value) is `skipped`, and so is a kind the series route has no form for.

const INPUT_METRICS_DROPPED: &str = "logit.input.metrics.dropped";
const INPUT_METRICS_SKIPPED: &str = "logit.input.metrics.skipped";
const INPUT_STATS_DROPPED: &str = "logit.input.stats.dropped";
const INPUT_STATS_SKIPPED: &str = "logit.input.stats.skipped";
const OUTPUT_METRICS_DROPPED: &str = "logit.output.metrics.dropped";
const OUTPUT_METRICS_SKIPPED: &str = "logit.output.metrics.skipped";

/// A series point, sketch, or service check the decoder didn't turn into an event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MetricIn {
    BadSeries,
    BadPoint,
    NonFiniteValue,
    BadSketch,
    LegacyDistribution,
    Malformed,
    NoName,
    InvalidStatus,
    NullValue,
    EmptyDistribution,
    EmptySketch,
}

impl MetricIn {
    #[cfg(test)]
    const ALL: [MetricIn; 11] = [
        MetricIn::BadSeries,
        MetricIn::BadPoint,
        MetricIn::NonFiniteValue,
        MetricIn::BadSketch,
        MetricIn::LegacyDistribution,
        MetricIn::Malformed,
        MetricIn::NoName,
        MetricIn::InvalidStatus,
        MetricIn::NullValue,
        MetricIn::EmptyDistribution,
        MetricIn::EmptySketch,
    ];

    /// The `reason` tag, also the diagnostic key where one is raised.
    pub(super) fn reason(self) -> &'static str {
        match self {
            MetricIn::BadSeries => "bad_series",
            MetricIn::BadPoint => "bad_point",
            MetricIn::NonFiniteValue => "non_finite_value",
            MetricIn::BadSketch => "bad_sketch",
            MetricIn::LegacyDistribution => "legacy_distribution",
            MetricIn::Malformed => "malformed",
            MetricIn::NoName => "no_name",
            MetricIn::InvalidStatus => "invalid_status",
            MetricIn::NullValue => "null_value",
            MetricIn::EmptyDistribution => "empty_distribution",
            MetricIn::EmptySketch => "empty_sketch",
        }
    }

    pub(super) fn metric(self) -> &'static str {
        match self {
            MetricIn::NullValue | MetricIn::EmptyDistribution | MetricIn::EmptySketch => {
                INPUT_METRICS_SKIPPED
            }
            MetricIn::BadSeries
            | MetricIn::BadPoint
            | MetricIn::NonFiniteValue
            | MetricIn::BadSketch
            | MetricIn::LegacyDistribution
            | MetricIn::Malformed
            | MetricIn::NoName
            | MetricIn::InvalidStatus => INPUT_METRICS_DROPPED,
        }
    }
}

/// An element of an APM stats payload the decoder didn't turn into a stats event.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum StatsIn {
    MalformedPayload,
    MalformedBucket,
    MalformedGroup,
    Interpolation,
    BadSketch,
    EmptyBucket,
}

impl StatsIn {
    #[cfg(test)]
    const ALL: [StatsIn; 6] = [
        StatsIn::MalformedPayload,
        StatsIn::MalformedBucket,
        StatsIn::MalformedGroup,
        StatsIn::Interpolation,
        StatsIn::BadSketch,
        StatsIn::EmptyBucket,
    ];

    pub(super) fn reason(self) -> &'static str {
        match self {
            StatsIn::MalformedPayload => "malformed_payload",
            StatsIn::MalformedBucket => "malformed_bucket",
            StatsIn::MalformedGroup => "malformed_group",
            StatsIn::Interpolation => "interpolation",
            StatsIn::BadSketch => "bad_sketch",
            StatsIn::EmptyBucket => "empty_bucket",
        }
    }

    pub(super) fn metric(self) -> &'static str {
        match self {
            StatsIn::EmptyBucket => INPUT_STATS_SKIPPED,
            StatsIn::MalformedPayload
            | StatsIn::MalformedBucket
            | StatsIn::MalformedGroup
            | StatsIn::Interpolation
            | StatsIn::BadSketch => INPUT_STATS_DROPPED,
        }
    }
}

/// A metric record an encoder didn't send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MetricOut {
    /// A kind the series route has no form for, named by [`logit_core::MetricKind::name`] or
    /// the temporality it can't carry (`cumulative_sum`, `non_monotonic_delta_sum`).
    Kind(&'static str),
    /// A relative gauge nothing upstream resolved to an absolute value.
    GaugeDelta,
    NoRecordedValue,
    EmptySketch,
    NonFiniteValue,
    OversizedSketch,
    /// A histogram bucket whose upper bound keys below its lower bound.
    NonMonotonicBounds,
    /// An exponential histogram outside the Agent sketch's key range.
    SketchRange,
    InvalidStatus,
}

impl MetricOut {
    #[cfg(test)]
    const ALL: [MetricOut; 9] = [
        MetricOut::Kind("summary"),
        MetricOut::GaugeDelta,
        MetricOut::NoRecordedValue,
        MetricOut::EmptySketch,
        MetricOut::NonFiniteValue,
        MetricOut::OversizedSketch,
        MetricOut::NonMonotonicBounds,
        MetricOut::SketchRange,
        MetricOut::InvalidStatus,
    ];

    /// The counter and its one tag.
    pub(super) fn counter(self) -> (&'static str, (&'static str, &'static str)) {
        match self {
            MetricOut::Kind(kind) => (OUTPUT_METRICS_SKIPPED, ("metric_kind", kind)),
            MetricOut::GaugeDelta => (OUTPUT_METRICS_DROPPED, ("reason", "unresolved_gauge_delta")),
            MetricOut::NoRecordedValue => (OUTPUT_METRICS_SKIPPED, ("reason", "no_recorded_value")),
            MetricOut::EmptySketch => (OUTPUT_METRICS_SKIPPED, ("reason", "empty_sketch")),
            MetricOut::NonFiniteValue => (OUTPUT_METRICS_DROPPED, ("reason", "non_finite_value")),
            MetricOut::OversizedSketch => (OUTPUT_METRICS_DROPPED, ("reason", "oversized_sketch")),
            MetricOut::NonMonotonicBounds => {
                (OUTPUT_METRICS_DROPPED, ("reason", "non_monotonic_bounds"))
            }
            MetricOut::SketchRange => (OUTPUT_METRICS_DROPPED, ("reason", "sketch_range")),
            MetricOut::InvalidStatus => (OUTPUT_METRICS_DROPPED, ("reason", "invalid_status")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every outcome against the ADR's migration record: a fault is `dropped`, and nothing to
    /// carry or no wire form is `skipped`.
    #[test]
    fn every_outcome_counts_under_the_vocabulary_s_name() {
        let skipped_in = ["null_value", "empty_distribution", "empty_sketch"];
        for outcome in MetricIn::ALL {
            let want = if skipped_in.contains(&outcome.reason()) {
                INPUT_METRICS_SKIPPED
            } else {
                INPUT_METRICS_DROPPED
            };
            assert_eq!(outcome.metric(), want, "{outcome:?}");
        }
        for outcome in StatsIn::ALL {
            let want = if outcome == StatsIn::EmptyBucket {
                INPUT_STATS_SKIPPED
            } else {
                INPUT_STATS_DROPPED
            };
            assert_eq!(outcome.metric(), want, "{outcome:?}");
        }
        let want_out = [
            (OUTPUT_METRICS_SKIPPED, ("metric_kind", "summary")),
            (OUTPUT_METRICS_DROPPED, ("reason", "unresolved_gauge_delta")),
            (OUTPUT_METRICS_SKIPPED, ("reason", "no_recorded_value")),
            (OUTPUT_METRICS_SKIPPED, ("reason", "empty_sketch")),
            (OUTPUT_METRICS_DROPPED, ("reason", "non_finite_value")),
            (OUTPUT_METRICS_DROPPED, ("reason", "oversized_sketch")),
            (OUTPUT_METRICS_DROPPED, ("reason", "non_monotonic_bounds")),
            (OUTPUT_METRICS_DROPPED, ("reason", "sketch_range")),
            (OUTPUT_METRICS_DROPPED, ("reason", "invalid_status")),
        ];
        for (outcome, want) in MetricOut::ALL.into_iter().zip(want_out) {
            assert_eq!(outcome.counter(), want, "{outcome:?}");
        }
        // A `dropped` counter never carries `metric_kind`.
        for outcome in MetricOut::ALL {
            let (metric, (key, _)) = outcome.counter();
            assert!(!(metric == OUTPUT_METRICS_DROPPED && key == "metric_kind"), "{outcome:?}");
        }
    }

    /// Each reason names one outcome, so a diagnostic key or a counter tag reads back to it.
    #[test]
    fn every_input_reason_is_distinct() {
        let mut reasons: Vec<_> = MetricIn::ALL.iter().map(|o| o.reason()).collect();
        reasons.sort_unstable();
        reasons.dedup();
        assert_eq!(reasons.len(), MetricIn::ALL.len());
        let mut reasons: Vec<_> = StatsIn::ALL.iter().map(|o| o.reason()).collect();
        reasons.sort_unstable();
        reasons.dedup();
        assert_eq!(reasons.len(), StatsIn::ALL.len());
    }
}
