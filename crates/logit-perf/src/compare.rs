//! `logit-perf compare a.json b.json`: per-scenario percent deltas on medians between two results
//! files, gated on a regression threshold (docs/plans/load-test-harness.md's "Harness" section).
//!
//! `b` is read as "after" and `a` as "before" -- a positive `events_per_s` delta or a negative
//! `cpu_us_per_event`/`max_rss_bytes` delta is an improvement; the opposite direction on
//! `events_per_s` or `cpu_us_per_event` past `--threshold` percent is a regression. `max_rss_bytes`
//! is reported unconditionally but only gates the exit code when `--rss-threshold` is given
//! (docs/plans/load-test-harness.md: "RSS reported, gated only with `--rss-threshold`").

use crate::result::{RunReport, Sample};
use std::collections::BTreeSet;

/// One scenario's before/after comparison.
#[derive(Debug, Clone, PartialEq)]
pub struct ScenarioComparison {
    pub name: String,
    /// `None` when the scenario is present in only one of the two files -- listed, not compared
    /// (docs/plans/load-test-harness.md's W5 row).
    pub deltas: Option<Deltas>,
}

/// Percent change from `a`'s median to `b`'s median, `(b - a) / a * 100.0`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Deltas {
    pub events_per_s_pct: f64,
    pub cpu_us_per_event_pct: f64,
    pub max_rss_bytes_pct: f64,
}

impl Deltas {
    fn between(a: &Sample, b: &Sample) -> Self {
        Deltas {
            events_per_s_pct: pct_change(a.events_per_s, b.events_per_s),
            cpu_us_per_event_pct: pct_change(a.cpu_us_per_event, b.cpu_us_per_event),
            max_rss_bytes_pct: pct_change(a.max_rss_bytes as f64, b.max_rss_bytes as f64),
        }
    }

    /// A throughput drop or a CPU-per-event rise past `threshold_pct` (of `a`'s value) is a
    /// regression on its own. `rss_threshold_pct`, when given, adds RSS growth past it as a third
    /// gating reason.
    fn is_regression(&self, threshold_pct: f64, rss_threshold_pct: Option<f64>) -> bool {
        let events_regressed = self.events_per_s_pct < -threshold_pct;
        let cpu_regressed = self.cpu_us_per_event_pct > threshold_pct;
        let rss_regressed =
            rss_threshold_pct.is_some_and(|rss_threshold| self.max_rss_bytes_pct > rss_threshold);
        events_regressed || cpu_regressed || rss_regressed
    }
}

fn pct_change(a: f64, b: f64) -> f64 {
    if a == 0.0 {
        if b == 0.0 {
            0.0
        } else {
            f64::INFINITY
        }
    } else {
        (b - a) / a * 100.0
    }
}

/// The full outcome of one `compare` invocation.
#[derive(Debug, Clone, PartialEq)]
pub struct CompareReport {
    pub scenarios: Vec<ScenarioComparison>,
    /// `hostname`/`cpu_model` differ between the two files -- a warning, not a failure
    /// (docs/plans/load-test-harness.md: cross-machine wall noise is why CPU/event is the actual
    /// gate, not a reason to refuse the comparison outright).
    pub environment_warning: Option<String>,
}

impl CompareReport {
    /// Whether any compared scenario regressed past the given thresholds -- what `main.rs` maps
    /// to the process exit code.
    pub fn has_regression(&self, threshold_pct: f64, rss_threshold_pct: Option<f64>) -> bool {
        self.scenarios.iter().any(|scenario| {
            scenario
                .deltas
                .is_some_and(|deltas| deltas.is_regression(threshold_pct, rss_threshold_pct))
        })
    }
}

/// Builds the comparison between two [`RunReport`]s. Never fails: a scenario present in only one
/// file is reported as such rather than treated as an error, and an environment mismatch is a
/// warning carried on the report, not a `Result::Err`.
pub fn compare(a: &RunReport, b: &RunReport) -> CompareReport {
    let names: BTreeSet<&String> = a.scenarios.keys().chain(b.scenarios.keys()).collect();
    let scenarios = names
        .into_iter()
        .map(|name| {
            let deltas = match (a.scenarios.get(name), b.scenarios.get(name)) {
                (Some(a_scenario), Some(b_scenario)) => {
                    Some(Deltas::between(&a_scenario.median, &b_scenario.median))
                }
                _ => None,
            };
            ScenarioComparison { name: name.clone(), deltas }
        })
        .collect();

    let environment_warning = if a.hostname != b.hostname || a.cpu_model != b.cpu_model {
        Some(format!(
            "comparing across different environments: {} ({}) vs {} ({}) -- wall-clock deltas \
             may reflect the machines, not the code; CPU us/event is the more trustworthy signal",
            a.hostname, a.cpu_model, b.hostname, b.cpu_model
        ))
    } else {
        None
    };

    CompareReport { scenarios, environment_warning }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::{GitInfo, ScenarioReport};
    use std::collections::BTreeMap;

    fn sample(events_per_s: f64, cpu_us_per_event: f64, max_rss_bytes: u64) -> Sample {
        Sample {
            wall_s: 1.0,
            user_s: 0.5,
            sys_s: 0.5,
            max_rss_bytes,
            events_per_s,
            cpu_us_per_event,
        }
    }

    fn scenario_report(
        events_per_s: f64,
        cpu_us_per_event: f64,
        max_rss_bytes: u64,
    ) -> ScenarioReport {
        let sample = sample(events_per_s, cpu_us_per_event, max_rss_bytes);
        ScenarioReport { count: 1_000_000, repeats: vec![sample], median: sample, min: sample }
    }

    fn report(
        hostname: &str,
        cpu_model: &str,
        scenarios: BTreeMap<String, ScenarioReport>,
    ) -> RunReport {
        RunReport {
            git: GitInfo { sha: "sha".to_string(), dirty: false },
            timestamp: "2026-09-12T00:00:00Z".to_string(),
            hostname: hostname.to_string(),
            cpu_model: cpu_model.to_string(),
            nproc: 8,
            rustc: "rustc 1.98.1".to_string(),
            profile: "release".to_string(),
            label: None,
            scenarios,
        }
    }

    #[test]
    fn identical_files_show_zero_delta_and_no_regression() {
        let mut scenarios = BTreeMap::new();
        scenarios.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.0, 1024));
        let a = report("host", "cpu", scenarios.clone());
        let b = report("host", "cpu", scenarios);

        let cmp = compare(&a, &b);
        assert_eq!(cmp.scenarios.len(), 1);
        let deltas = cmp.scenarios[0].deltas.unwrap();
        assert_eq!(deltas.events_per_s_pct, 0.0);
        assert_eq!(deltas.cpu_us_per_event_pct, 0.0);
        assert!(!cmp.has_regression(5.0, None));
    }

    #[test]
    fn a_throughput_drop_past_threshold_is_a_regression() {
        let mut before = BTreeMap::new();
        before.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.0, 1024));
        let mut after = BTreeMap::new();
        after.insert("passthrough".to_string(), scenario_report(800_000.0, 2.0, 1024)); // -20%

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert!(cmp.has_regression(5.0, None));
        assert!(!cmp.has_regression(25.0, None), "a 20% drop should not trip a 25% threshold");
    }

    #[test]
    fn a_cpu_per_event_rise_past_threshold_is_a_regression() {
        let mut before = BTreeMap::new();
        before.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.0, 1024));
        let mut after = BTreeMap::new();
        after.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.5, 1024)); // +25%

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert!(cmp.has_regression(10.0, None));
    }

    #[test]
    fn an_improvement_is_never_a_regression() {
        let mut before = BTreeMap::new();
        before.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.0, 1024));
        let mut after = BTreeMap::new();
        after.insert("passthrough".to_string(), scenario_report(1_500_000.0, 1.0, 512)); // faster, cheaper, smaller

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert!(!cmp.has_regression(0.0, Some(0.0)));
    }

    #[test]
    fn rss_growth_only_gates_when_a_threshold_is_given() {
        let mut before = BTreeMap::new();
        before.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.0, 1024));
        let mut after = BTreeMap::new();
        after.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.0, 2048)); // +100% RSS

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert!(!cmp.has_regression(5.0, None), "RSS must not gate without --rss-threshold");
        assert!(cmp.has_regression(5.0, Some(50.0)), "RSS should gate once a threshold is given");
    }

    #[test]
    fn a_scenario_in_only_one_file_is_listed_not_compared() {
        let mut before = BTreeMap::new();
        before.insert("only_in_a".to_string(), scenario_report(1.0, 1.0, 1));
        let after = BTreeMap::new();

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert_eq!(cmp.scenarios.len(), 1);
        assert_eq!(cmp.scenarios[0].name, "only_in_a");
        assert!(cmp.scenarios[0].deltas.is_none());
        assert!(!cmp.has_regression(0.0, Some(0.0)), "an uncompared scenario never regresses");
    }

    #[test]
    fn a_hostname_or_cpu_mismatch_warns_but_does_not_fail() {
        let scenarios = BTreeMap::new();
        let cmp = compare(
            &report("box-a", "cpu-a", scenarios.clone()),
            &report("box-b", "cpu-a", scenarios),
        );
        assert!(cmp.environment_warning.is_some());
    }

    #[test]
    fn matching_environments_have_no_warning() {
        let scenarios = BTreeMap::new();
        let cmp = compare(
            &report("box-a", "cpu-a", scenarios.clone()),
            &report("box-a", "cpu-a", scenarios),
        );
        assert!(cmp.environment_warning.is_none());
    }
}
