//! The JSON schema `logit-perf run` writes to `perf/results/<utc-ts>-<sha>[-label].json` and
//! `logit-perf compare` reads back (docs/plans/load-test-harness.md's "Harness" section).
//!
//! `Sample` is one repeat's derived numbers; `ScenarioReport` holds every repeat plus a per-field
//! `median`/`min` across them, computed independently per field (never "the repeat with the
//! median wall time, in full") -- simple, and exactly what `compare.rs` needs to gate on.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::time::Duration;

/// The commit the binary under test was built from, and whether the working tree had uncommitted
/// changes at the time -- so a results file is never silently ambiguous about what it measured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitInfo {
    pub sha: String,
    pub dirty: bool,
}

/// One `logit-perf run` invocation: the environment it ran in, plus every scenario it measured.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunReport {
    pub git: GitInfo,
    /// RFC 3339, always UTC (a trailing `Z`), second precision.
    pub timestamp: String,
    pub hostname: String,
    /// `/proc/cpuinfo`'s `model name` field, verbatim.
    pub cpu_model: String,
    pub nproc: usize,
    /// `rustc -V`'s output, verbatim (trimmed).
    pub rustc: String,
    /// The `--profile` the binary under test was built with (`release` by default).
    pub profile: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    pub scenarios: BTreeMap<String, ScenarioReport>,
}

/// One scenario's results across every `--repeat`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScenarioReport {
    /// The scenario's configured `generate_in.count`, copied in so a results file is
    /// self-describing without re-reading the scenario YAML it came from.
    pub count: u64,
    pub repeats: Vec<Sample>,
    pub median: Sample,
    pub min: Sample,
}

/// The numbers derived from one repeat's wall time and `wait4` rusage.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    pub wall_s: f64,
    pub user_s: f64,
    pub sys_s: f64,
    pub max_rss_bytes: u64,
    pub events_per_s: f64,
    /// `(user_s + sys_s) * 1e6 / count` -- the headline regression signal
    /// (docs/adr/load-test-harness.md): far less noisy than wall time on a shared box, since it
    /// doesn't care how many other processes were competing for the CPU during the run.
    pub cpu_us_per_event: f64,
}

impl Sample {
    pub fn from_usage(count: u64, wall: Duration, user: Duration, sys: Duration, max_rss_bytes: u64) -> Self {
        let wall_s = wall.as_secs_f64();
        let cpu_s = user.as_secs_f64() + sys.as_secs_f64();
        let count_f = count as f64;
        Sample {
            wall_s,
            user_s: user.as_secs_f64(),
            sys_s: sys.as_secs_f64(),
            max_rss_bytes,
            events_per_s: count_f / wall_s,
            cpu_us_per_event: cpu_s * 1_000_000.0 / count_f,
        }
    }
}

/// The statistical median of `values` -- the average of the two middle elements for an even
/// count, matching every other "median" in this file (`median_sample`'s per-field reduction).
fn median_f64(values: &[f64]) -> f64 {
    let mut sorted: Vec<f64> = values.to_vec();
    sorted.sort_by(|a, b| a.total_cmp(b));
    let n = sorted.len();
    if n == 0 {
        return f64::NAN;
    }
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0
    }
}

fn median_u64(values: &[u64]) -> u64 {
    let mut sorted: Vec<u64> = values.to_vec();
    sorted.sort_unstable();
    let n = sorted.len();
    if n == 0 {
        return 0;
    }
    if n % 2 == 1 {
        sorted[n / 2]
    } else {
        // Integer average of the two middle elements, rounding down -- a peak-RSS median doesn't
        // need fractional-byte precision.
        (sorted[n / 2 - 1] + sorted[n / 2]) / 2
    }
}

fn min_f64(values: &[f64]) -> f64 {
    values.iter().copied().fold(f64::INFINITY, f64::min)
}

fn min_u64(values: &[u64]) -> u64 {
    values.iter().copied().min().unwrap_or(0)
}

/// Builds the per-field median [`Sample`] across `samples`. Each field's median is computed
/// independently of the others -- the result is not, and isn't meant to be, any single repeat
/// that actually ran; it's a per-metric summary, exactly what `compare.rs` diffs.
pub fn median_sample(samples: &[Sample]) -> Sample {
    Sample {
        wall_s: median_f64(&samples.iter().map(|s| s.wall_s).collect::<Vec<_>>()),
        user_s: median_f64(&samples.iter().map(|s| s.user_s).collect::<Vec<_>>()),
        sys_s: median_f64(&samples.iter().map(|s| s.sys_s).collect::<Vec<_>>()),
        max_rss_bytes: median_u64(&samples.iter().map(|s| s.max_rss_bytes).collect::<Vec<_>>()),
        events_per_s: median_f64(&samples.iter().map(|s| s.events_per_s).collect::<Vec<_>>()),
        cpu_us_per_event: median_f64(&samples.iter().map(|s| s.cpu_us_per_event).collect::<Vec<_>>()),
    }
}

/// Builds the per-field minimum [`Sample`] across `samples`, the same independent-per-field way
/// [`median_sample`] does.
pub fn min_sample(samples: &[Sample]) -> Sample {
    Sample {
        wall_s: min_f64(&samples.iter().map(|s| s.wall_s).collect::<Vec<_>>()),
        user_s: min_f64(&samples.iter().map(|s| s.user_s).collect::<Vec<_>>()),
        sys_s: min_f64(&samples.iter().map(|s| s.sys_s).collect::<Vec<_>>()),
        max_rss_bytes: min_u64(&samples.iter().map(|s| s.max_rss_bytes).collect::<Vec<_>>()),
        events_per_s: min_f64(&samples.iter().map(|s| s.events_per_s).collect::<Vec<_>>()),
        cpu_us_per_event: min_f64(&samples.iter().map(|s| s.cpu_us_per_event).collect::<Vec<_>>()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(wall_s: f64, cpu_us_per_event: f64, max_rss_bytes: u64) -> Sample {
        Sample {
            wall_s,
            user_s: wall_s / 2.0,
            sys_s: wall_s / 2.0,
            max_rss_bytes,
            events_per_s: 1.0 / wall_s,
            cpu_us_per_event,
        }
    }

    #[test]
    fn median_of_an_odd_number_of_samples_is_the_middle_value() {
        let samples = vec![sample(3.0, 30.0, 300), sample(1.0, 10.0, 100), sample(2.0, 20.0, 200)];
        let median = median_sample(&samples);
        assert_eq!(median.wall_s, 2.0);
        assert_eq!(median.cpu_us_per_event, 20.0);
        assert_eq!(median.max_rss_bytes, 200);
    }

    #[test]
    fn median_of_an_even_number_of_samples_averages_the_two_middle_values() {
        let samples = vec![sample(1.0, 10.0, 100), sample(2.0, 20.0, 200), sample(3.0, 30.0, 300), sample(4.0, 40.0, 400)];
        let median = median_sample(&samples);
        assert_eq!(median.wall_s, 2.5);
        assert_eq!(median.cpu_us_per_event, 25.0);
        assert_eq!(median.max_rss_bytes, 250);
    }

    #[test]
    fn min_picks_the_smallest_value_per_field_independently() {
        let samples = vec![sample(3.0, 5.0, 900), sample(1.0, 40.0, 100)];
        let min = min_sample(&samples);
        // wall_s's minimum (1.0) and cpu_us_per_event's minimum (5.0) come from different
        // repeats -- `min_sample` never claims the result describes one real run.
        assert_eq!(min.wall_s, 1.0);
        assert_eq!(min.cpu_us_per_event, 5.0);
        assert_eq!(min.max_rss_bytes, 100);
    }

    #[test]
    fn single_repeat_median_and_min_both_equal_that_repeat() {
        let samples = vec![sample(1.5, 12.0, 150)];
        assert_eq!(median_sample(&samples), samples[0]);
        assert_eq!(min_sample(&samples), samples[0]);
    }

    #[test]
    fn sample_from_usage_derives_events_per_s_and_cpu_us_per_event() {
        let sample = Sample::from_usage(
            1_000_000,
            Duration::from_secs(2),
            Duration::from_millis(1_500),
            Duration::from_millis(500),
            123 * 1024,
        );
        assert_eq!(sample.wall_s, 2.0);
        assert_eq!(sample.events_per_s, 500_000.0);
        // (1.5s + 0.5s) * 1e6us / 1_000_000 events = 2.0 us/event.
        assert_eq!(sample.cpu_us_per_event, 2.0);
        assert_eq!(sample.max_rss_bytes, 123 * 1024);
    }

    #[test]
    fn run_report_round_trips_through_json() {
        let mut scenarios = BTreeMap::new();
        let repeats = vec![sample(1.0, 10.0, 100), sample(1.2, 11.0, 110)];
        scenarios.insert(
            "passthrough".to_string(),
            ScenarioReport {
                count: 5_000_000,
                median: median_sample(&repeats),
                min: min_sample(&repeats),
                repeats,
            },
        );
        let report = RunReport {
            git: GitInfo { sha: "abc123".to_string(), dirty: false },
            timestamp: "2026-09-12T00:00:00Z".to_string(),
            hostname: "devbox".to_string(),
            cpu_model: "Some CPU".to_string(),
            nproc: 8,
            rustc: "rustc 1.98.1".to_string(),
            profile: "release".to_string(),
            label: Some("baseline".to_string()),
            scenarios,
        };

        let json = serde_json::to_string(&report).unwrap();
        let round_tripped: RunReport = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, report);
    }

    #[test]
    fn run_report_label_is_omitted_from_json_when_absent() {
        let report = RunReport {
            git: GitInfo { sha: "abc123".to_string(), dirty: true },
            timestamp: "2026-09-12T00:00:00Z".to_string(),
            hostname: "devbox".to_string(),
            cpu_model: "Some CPU".to_string(),
            nproc: 4,
            rustc: "rustc 1.98.1".to_string(),
            profile: "release".to_string(),
            label: None,
            scenarios: BTreeMap::new(),
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("label"), "{json}");
    }
}
