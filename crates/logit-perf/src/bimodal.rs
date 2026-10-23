//! Whether a scenario's repeats split into two modes of CPU µs/event, so `run` and `compare` stop
//! reporting a pooled median that describes neither.
//!
//! A tokio runtime can land a workload in two execution modes from one run to the next, on
//! identical work and one binary, and their CPU per event can differ by 2×. The median of a mix of
//! the two moves with the mix, not the code, so gating on it is a coin toss.
//!
//! The test is exact one-dimensional two-means: sort the values, try every split point, and keep
//! the split with the smallest within-cluster sum of squares. The split is bimodal when both
//! clusters hold at least two repeats and the gap between them, the upper cluster's minimum less
//! the lower cluster's maximum, exceeds the wider cluster's range. One outlier is never a mode:
//! it's a cluster of one. Fewer than [`MIN_REPEATS`] repeats can't support the call either way.

use serde::{Deserialize, Serialize};

/// The fewest repeats the test runs on; below it the verdict is [`Verdict::TooFew`].
pub const MIN_REPEATS: usize = 5;

/// One mode of a bimodal scenario.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Cluster {
    /// Repeats in this mode.
    pub size: usize,
    /// Their median CPU µs/event.
    pub median: f64,
}

/// A bimodal scenario's two modes.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Clusters {
    pub lower: Cluster,
    pub upper: Cluster,
}

impl Clusters {
    /// `3 repeats at 0.512 and 2 at 1.031 us/event`.
    pub fn describe(&self) -> String {
        format!(
            "{} repeats at {:.3} and {} at {:.3} us/event",
            self.lower.size, self.lower.median, self.upper.size, self.upper.median
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Verdict {
    TooFew,
    Unimodal,
    Bimodal(Clusters),
}

impl Verdict {
    /// The results file's `bimodal`: `null` for too few repeats, never `false`.
    pub fn flag(&self) -> Option<bool> {
        match self {
            Verdict::TooFew => None,
            Verdict::Unimodal => Some(false),
            Verdict::Bimodal(_) => Some(true),
        }
    }

    pub fn clusters(&self) -> Option<Clusters> {
        match self {
            Verdict::Bimodal(clusters) => Some(*clusters),
            _ => None,
        }
    }
}

pub fn detect(values: &[f64]) -> Verdict {
    if values.len() < MIN_REPEATS {
        return Verdict::TooFew;
    }
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);

    // `split` is the upper cluster's first index; both sides are non-empty.
    let split = (1..sorted.len())
        .min_by(|&a, &b| {
            let cost = |k: usize| sum_of_squares(&sorted[..k]) + sum_of_squares(&sorted[k..]);
            cost(a).total_cmp(&cost(b))
        })
        .expect("at least MIN_REPEATS values give at least one split");
    let (lower, upper) = sorted.split_at(split);
    if lower.len() < 2 || upper.len() < 2 {
        return Verdict::Unimodal;
    }
    let range = |cluster: &[f64]| cluster[cluster.len() - 1] - cluster[0];
    let gap = upper[0] - lower[lower.len() - 1];
    if gap > range(lower).max(range(upper)) {
        Verdict::Bimodal(Clusters {
            lower: Cluster { size: lower.len(), median: median_sorted(lower) },
            upper: Cluster { size: upper.len(), median: median_sorted(upper) },
        })
    } else {
        Verdict::Unimodal
    }
}

fn sum_of_squares(cluster: &[f64]) -> f64 {
    let mean = cluster.iter().sum::<f64>() / cluster.len() as f64;
    cluster.iter().map(|v| (v - mean) * (v - mean)).sum()
}

fn median_sorted(sorted: &[f64]) -> f64 {
    let n = sorted.len();
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn two_separated_modes_are_bimodal() {
        let verdict = detect(&[1.02, 0.50, 1.05, 0.52, 0.51, 1.00]);
        assert_eq!(
            verdict,
            Verdict::Bimodal(Clusters {
                lower: Cluster { size: 3, median: 0.51 },
                upper: Cluster { size: 3, median: 1.02 },
            })
        );
        assert_eq!(verdict.flag(), Some(true));
    }

    #[test]
    fn a_two_against_three_split_is_bimodal() {
        let clusters = detect(&[0.80, 0.40, 0.82, 0.41, 0.81]).clusters().expect("bimodal");
        assert_eq!((clusters.lower.size, clusters.upper.size), (2, 3));
        assert!((clusters.lower.median - 0.405).abs() < 1e-12, "{clusters:?}");
        assert_eq!(clusters.upper.median, 0.81);
    }

    #[test]
    fn an_even_spread_is_unimodal() {
        let verdict = detect(&[1.0, 1.1, 1.2, 1.3, 1.4, 1.5]);
        assert_eq!(verdict, Verdict::Unimodal);
        assert_eq!(verdict.flag(), Some(false));
    }

    #[test]
    fn a_single_outlier_is_not_a_mode() {
        assert_eq!(detect(&[0.50, 0.51, 0.52, 0.50, 1.40]), Verdict::Unimodal);
        assert_eq!(detect(&[0.10, 0.50, 0.51, 0.52, 0.50]), Verdict::Unimodal);
    }

    #[test]
    fn a_gap_no_wider_than_a_cluster_is_unimodal() {
        // Gap 0.20, upper range 0.30.
        assert_eq!(detect(&[1.0, 1.05, 1.25, 1.40, 1.55]), Verdict::Unimodal);
    }

    #[test]
    fn fewer_than_five_repeats_is_no_verdict() {
        let verdict = detect(&[0.4, 0.4, 0.8, 0.8]);
        assert_eq!(verdict, Verdict::TooFew);
        assert_eq!(verdict.flag(), None);
    }
}
