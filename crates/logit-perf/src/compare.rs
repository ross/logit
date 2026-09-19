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
    /// (docs/plans/load-test-harness.md's W5 row); see [`Presence`] for which file it's in then.
    pub deltas: Option<Deltas>,
    pub presence: Presence,
}

/// Which of the two compared files a scenario appears in. `Both` is the only case `deltas` is
/// ever `Some` for; the other two exist so the caller can name the actual file the lone entry
/// came from, rather than a bare "only in one file".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Presence {
    Both,
    BeforeOnly,
    AfterOnly,
}

/// Percent change from `a`'s median to `b`'s median, `(b - a) / a * 100.0`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Deltas {
    pub events_per_s_pct: f64,
    pub cpu_us_per_event_pct: f64,
    pub max_rss_bytes_pct: f64,
    /// `None` when either side's median `startup_s` is itself `None` (no repeat on that side ever
    /// observed a `ready` line) -- a delta needs both ends, and there is nothing to warn about
    /// when one is missing.
    pub startup_s_pct: Option<f64>,
    /// The change in datagram drop rate, in **percentage points** (not a percent change): a
    /// baseline that dropped 3.0% of datagrams against an "after" that drops 1.2% gives `-1.8`.
    ///
    /// Percentage points rather than a relative percentage because the quantity is already a rate,
    /// and a relative delta on a small rate is nearly all noise -- 0.1% to 0.2% reads as "+100%"
    /// and means almost nothing, while "+0.1 points" is exactly as much as it is.
    ///
    /// `None` unless both sides are real-socket scenarios carrying a `UdpSample`. **Reported,
    /// never gated**: see [`Deltas::is_regression`].
    pub drop_rate_points: Option<f64>,
}

impl Deltas {
    fn between(a: &Sample, b: &Sample) -> Self {
        Deltas {
            events_per_s_pct: pct_change(a.events_per_s, b.events_per_s),
            cpu_us_per_event_pct: pct_change(a.cpu_us_per_event, b.cpu_us_per_event),
            max_rss_bytes_pct: pct_change(a.max_rss_bytes as f64, b.max_rss_bytes as f64),
            startup_s_pct: match (a.startup_s, b.startup_s) {
                (Some(a), Some(b)) => Some(pct_change(a, b)),
                _ => None,
            },
            drop_rate_points: match (a.udp, b.udp) {
                (Some(a), Some(b)) => Some(100.0 * (b.drop_rate() - a.drop_rate())),
                _ => None,
            },
        }
    }

    /// A throughput drop or a CPU-per-event rise past `threshold_pct` (of `a`'s value) is a
    /// regression on its own. `rss_threshold_pct`, when given, adds RSS growth past it as a third
    /// gating reason. `pub`: `main.rs`'s table printer calls this directly to mark which rows
    /// tripped the threshold, the same verdict [`CompareReport::has_regression`] gates the exit
    /// code on.
    ///
    /// `startup_s_pct` and `drop_rate_points` deliberately never participate here -- see
    /// [`Deltas::startup_regressed`] and [`Deltas::drop_rate_points`]. A UDP scenario's drop rate
    /// is a property of how hard the harness chose to push it, tuned on purpose into a lossy
    /// regime (ADR `udp-intake-batching-and-socket-visibility`); gating on it would fail a run for
    /// being configured the way it was meant to be. It is printed because a *change* in it between
    /// two runs of the same spec is exactly what `push_many`/`recvmmsg` are supposed to move.
    pub fn is_regression(&self, threshold_pct: f64, rss_threshold_pct: Option<f64>) -> bool {
        let events_regressed = self.events_per_s_pct < -threshold_pct;
        let cpu_regressed = self.cpu_us_per_event_pct > threshold_pct;
        let rss_regressed =
            rss_threshold_pct.is_some_and(|rss_threshold| self.max_rss_bytes_pct > rss_threshold);
        events_regressed || cpu_regressed || rss_regressed
    }

    /// Whether startup time rose by more than `threshold_pct` -- `main.rs` prints a warning for
    /// this, but it is never folded into [`Deltas::is_regression`] or
    /// [`CompareReport::has_regression`]: startup is spawn -> ready, process bring-up rather than
    /// the graph's own per-event cost, so a regression here is worth a human's attention without
    /// failing a `compare --threshold` gate meant for throughput/CPU/RSS.
    pub fn startup_regressed(&self, threshold_pct: f64) -> bool {
        self.startup_s_pct.is_some_and(|pct| pct > threshold_pct)
    }
}

/// An effective pacing rate for the warning above -- "unpaced" reads better than a bare `None`,
/// and distinguishes a spec with no `rate:` from one whose results predate the field.
fn render_rate(rate: Option<u64>) -> String {
    rate.map(|rate| rate.to_string()).unwrap_or_else(|| "unpaced/unrecorded".to_string())
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
    /// Every reason this comparison might be apples-to-oranges: a `hostname`/`cpu_model`
    /// mismatch (docs/plans/load-test-harness.md: cross-machine wall noise is why CPU/event is
    /// the actual gate, not a reason to refuse the comparison outright), a `profile`/`rustc`
    /// mismatch, or a per-scenario `count` mismatch (its events/s and CPU us/event denominators
    /// differ, so a delta between them conflates a real change with a different workload size).
    /// Warnings, never failures -- `main.rs` prints each one but still runs the comparison.
    pub warnings: Vec<String>,
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
/// file is reported as such rather than treated as an error, and every mismatch that makes the
/// comparison less trustworthy is collected into `warnings` rather than a `Result::Err`.
pub fn compare(a: &RunReport, b: &RunReport) -> CompareReport {
    let mut warnings = Vec::new();

    if a.hostname != b.hostname || a.cpu_model != b.cpu_model {
        warnings.push(format!(
            "comparing across different environments: {} ({}) vs {} ({}) -- wall-clock deltas \
             may reflect the machines, not the code; CPU us/event is the more trustworthy signal",
            a.hostname, a.cpu_model, b.hostname, b.cpu_model
        ));
    }
    if a.profile != b.profile {
        warnings.push(format!(
            "comparing different build profiles: `{}` vs `{}`",
            a.profile, b.profile
        ));
    }
    if a.rustc != b.rustc {
        warnings
            .push(format!("comparing different rustc versions: `{}` vs `{}`", a.rustc, b.rustc));
    }
    // Caught the exact hazard a multi-source VM session ran into by hand (`docs/adr/disposable-
    // azure-perf-vm.md`): two source trees extracted around the same time against one shared
    // `CARGO_TARGET_DIR` let cargo's mtime fingerprinting treat the second as unchanged, so a
    // "delta" would have been measured against a byte-identical binary under a different label. A
    // warning, not a refusal -- a docs-only diff between two refs legitimately produces this too.
    if let (Some(a_bin), Some(b_bin)) = (&a.binary, &b.binary) {
        if a_bin.sha256 == b_bin.sha256 {
            let short: String = a_bin.sha256.chars().take(12).collect();
            warnings.push(format!(
                "both sides measured the identical binary (sha256 {short}…) -- a delta between \
                 these two results measures nothing"
            ));
        }
    }

    let names: BTreeSet<&String> = a.scenarios.keys().chain(b.scenarios.keys()).collect();
    let scenarios = names
        .into_iter()
        .map(|name| match (a.scenarios.get(name), b.scenarios.get(name)) {
            (Some(a_scenario), Some(b_scenario)) => {
                if a_scenario.count != b_scenario.count {
                    warnings.push(format!(
                        "scenario `{name}`: count differs ({} vs {}) -- events/s and CPU us/event \
                         may not be comparable",
                        a_scenario.count, b_scenario.count
                    ));
                }
                // A driven scenario read at two different `--rate-scale`s is two different
                // operating points, and a real-socket pipeline's CPU/event moves with where on the
                // load curve it sat. Nothing else in a results file would say so, so this is the
                // one place it can be caught.
                let (a_rate, b_rate) = (
                    a_scenario.median.udp.and_then(|udp| udp.effective_rate),
                    b_scenario.median.udp.and_then(|udp| udp.effective_rate),
                );
                if a_rate != b_rate {
                    warnings.push(format!(
                        "scenario `{name}`: the two runs were paced differently ({} vs {} \
                         datagrams/s) -- they are different operating points on the load curve, \
                         not a before and after",
                        render_rate(a_rate),
                        render_rate(b_rate),
                    ));
                }
                ScenarioComparison {
                    name: name.clone(),
                    deltas: Some(Deltas::between(&a_scenario.median, &b_scenario.median)),
                    presence: Presence::Both,
                }
            }
            (Some(_), None) => ScenarioComparison {
                name: name.clone(),
                deltas: None,
                presence: Presence::BeforeOnly,
            },
            (None, Some(_)) => ScenarioComparison {
                name: name.clone(),
                deltas: None,
                presence: Presence::AfterOnly,
            },
            (None, None) => unreachable!("name came from the union of both maps' own keys"),
        })
        .collect();

    CompareReport { scenarios, warnings }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::{GitInfo, ScenarioReport};
    use std::collections::BTreeMap;

    fn sample(events_per_s: f64, cpu_us_per_event: f64, max_rss_bytes: u64) -> Sample {
        sample_with_startup(events_per_s, cpu_us_per_event, max_rss_bytes, Some(0.1))
    }

    fn sample_with_startup(
        events_per_s: f64,
        cpu_us_per_event: f64,
        max_rss_bytes: u64,
        startup_s: Option<f64>,
    ) -> Sample {
        Sample {
            wall_s: 1.0,
            startup_s,
            user_s: 0.5,
            sys_s: 0.5,
            max_rss_bytes,
            events_per_s,
            cpu_us_per_event,
            udp: None,
        }
    }

    /// A driven scenario's report, dropping `kernel_dropped` of 10,000 datagrams.
    fn driven_report(
        events_per_s: f64,
        cpu_us_per_event: f64,
        kernel_dropped: u64,
    ) -> ScenarioReport {
        let mut sample = sample(events_per_s, cpu_us_per_event, 1024);
        sample.udp = Some(crate::result::UdpSample {
            sent_datagrams: 10_000,
            sent_lines: 10_000,
            received_datagrams: 10_000 - kernel_dropped,
            reads: 10_000 - kernel_dropped,
            kernel_dropped,
            queue_dropped: 0,
            events_delivered: 10_000 - kernel_dropped,
            send_errors: 0,
            kernel_rcvbuf_utilization_max: 0.9,
            effective_rate: Some(90_000),
        });
        ScenarioReport { count: 10_000, repeats: vec![sample], median: sample, min: sample }
    }

    fn scenario_report(
        events_per_s: f64,
        cpu_us_per_event: f64,
        max_rss_bytes: u64,
    ) -> ScenarioReport {
        let sample = sample(events_per_s, cpu_us_per_event, max_rss_bytes);
        ScenarioReport { count: 1_000_000, repeats: vec![sample], median: sample, min: sample }
    }

    fn scenario_report_with_startup(
        events_per_s: f64,
        cpu_us_per_event: f64,
        max_rss_bytes: u64,
        startup_s: Option<f64>,
    ) -> ScenarioReport {
        let sample = sample_with_startup(events_per_s, cpu_us_per_event, max_rss_bytes, startup_s);
        ScenarioReport { count: 1_000_000, repeats: vec![sample], median: sample, min: sample }
    }

    fn report(
        hostname: &str,
        cpu_model: &str,
        scenarios: BTreeMap<String, ScenarioReport>,
    ) -> RunReport {
        RunReport {
            git: GitInfo { sha: Some("sha".to_string()), dirty: Some(false) },
            timestamp: "2026-09-12T00:00:00Z".to_string(),
            hostname: hostname.to_string(),
            cpu_model: cpu_model.to_string(),
            nproc: 8,
            rustc: "rustc 1.98.1".to_string(),
            profile: "release".to_string(),
            label: None,
            box_state: None,
            binary: None,
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
        assert_eq!(cmp.scenarios[0].presence, Presence::BeforeOnly);
        assert!(!cmp.has_regression(0.0, Some(0.0)), "an uncompared scenario never regresses");
    }

    #[test]
    fn a_scenario_only_in_the_after_file_is_marked_after_only() {
        let before = BTreeMap::new();
        let mut after = BTreeMap::new();
        after.insert("only_in_b".to_string(), scenario_report(1.0, 1.0, 1));

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert_eq!(cmp.scenarios[0].presence, Presence::AfterOnly);
    }

    #[test]
    fn a_hostname_or_cpu_mismatch_warns_but_does_not_fail() {
        let scenarios = BTreeMap::new();
        let cmp = compare(
            &report("box-a", "cpu-a", scenarios.clone()),
            &report("box-b", "cpu-a", scenarios),
        );
        assert!(cmp.warnings.iter().any(|w| w.contains("different environments")));
    }

    #[test]
    fn a_profile_mismatch_warns() {
        let scenarios = BTreeMap::new();
        let mut b = report("box-a", "cpu-a", scenarios.clone());
        b.profile = "dev".to_string();
        let cmp = compare(&report("box-a", "cpu-a", scenarios), &b);
        assert!(cmp.warnings.iter().any(|w| w.contains("build profiles")));
    }

    #[test]
    fn a_rustc_mismatch_warns() {
        let scenarios = BTreeMap::new();
        let mut b = report("box-a", "cpu-a", scenarios.clone());
        b.rustc = "rustc 1.99.0".to_string();
        let cmp = compare(&report("box-a", "cpu-a", scenarios), &b);
        assert!(cmp.warnings.iter().any(|w| w.contains("rustc versions")));
    }

    #[test]
    fn a_per_scenario_count_mismatch_warns() {
        let mut before = BTreeMap::new();
        before.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.0, 1024));
        let mut after = BTreeMap::new();
        let mut after_scenario = scenario_report(1_000_000.0, 2.0, 1024);
        after_scenario.count = 2_000_000;
        after.insert("passthrough".to_string(), after_scenario);

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert!(cmp.warnings.iter().any(|w| w.contains("count differs")));
    }

    #[test]
    fn a_startup_regression_is_reported_in_the_delta_but_never_gates_the_verdict() {
        let mut before = BTreeMap::new();
        before.insert(
            "passthrough".to_string(),
            scenario_report_with_startup(1_000_000.0, 2.0, 1024, Some(0.010)),
        );
        let mut after = BTreeMap::new();
        after.insert(
            "passthrough".to_string(),
            // Ten times slower to reach `ready` -- a huge regression by any threshold.
            scenario_report_with_startup(1_000_000.0, 2.0, 1024, Some(0.100)),
        );

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        let deltas = cmp.scenarios[0].deltas.unwrap();
        assert!((deltas.startup_s_pct.unwrap() - 900.0).abs() < 1e-9);
        assert!(deltas.startup_regressed(5.0));
        assert!(
            !cmp.has_regression(5.0, None),
            "startup must never gate the overall verdict, however large the regression"
        );
    }

    #[test]
    fn startup_delta_is_none_when_either_side_never_observed_a_ready_line() {
        let mut before = BTreeMap::new();
        before.insert(
            "passthrough".to_string(),
            scenario_report_with_startup(1_000_000.0, 2.0, 1024, None),
        );
        let mut after = BTreeMap::new();
        after.insert(
            "passthrough".to_string(),
            scenario_report_with_startup(1_000_000.0, 2.0, 1024, Some(0.05)),
        );

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        let deltas = cmp.scenarios[0].deltas.unwrap();
        assert_eq!(deltas.startup_s_pct, None);
        assert!(!deltas.startup_regressed(0.0), "no delta means nothing to warn about");
    }

    #[test]
    fn a_drop_rate_improvement_is_reported_in_percentage_points_and_never_gates() {
        let mut before = BTreeMap::new();
        before.insert("udp-statsd".to_string(), driven_report(1_000_000.0, 2.0, 300)); // 3.0%
        let mut after = BTreeMap::new();
        after.insert("udp-statsd".to_string(), driven_report(1_000_000.0, 2.0, 120)); // 1.2%

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        let deltas = cmp.scenarios[0].deltas.unwrap();
        assert!((deltas.drop_rate_points.unwrap() + 1.8).abs() < 1e-9);
        assert!(!cmp.has_regression(5.0, None));
    }

    #[test]
    fn a_drop_rate_that_got_worse_is_still_never_a_regression_on_its_own() {
        let mut before = BTreeMap::new();
        before.insert("udp-statsd".to_string(), driven_report(1_000_000.0, 2.0, 0));
        let mut after = BTreeMap::new();
        after.insert("udp-statsd".to_string(), driven_report(1_000_000.0, 2.0, 5_000)); // 50%

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        let deltas = cmp.scenarios[0].deltas.unwrap();
        assert!((deltas.drop_rate_points.unwrap() - 50.0).abs() < 1e-9);
        assert!(
            !cmp.has_regression(0.0, Some(0.0)),
            "the drop rate is reported, never folded into the verdict"
        );
    }

    #[test]
    fn a_generated_scenario_has_no_drop_rate_delta_at_all() {
        let mut before = BTreeMap::new();
        before.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.0, 1024));
        let mut after = BTreeMap::new();
        after.insert("passthrough".to_string(), scenario_report(1_000_000.0, 2.0, 1024));

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert_eq!(cmp.scenarios[0].deltas.unwrap().drop_rate_points, None);
    }

    #[test]
    fn comparing_a_driven_run_against_a_generated_one_reports_no_drop_delta() {
        let mut before = BTreeMap::new();
        before.insert("udp-statsd".to_string(), scenario_report(1_000_000.0, 2.0, 1024));
        let mut after = BTreeMap::new();
        after.insert("udp-statsd".to_string(), driven_report(1_000_000.0, 2.0, 300));

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert_eq!(cmp.scenarios[0].deltas.unwrap().drop_rate_points, None);
    }

    #[test]
    fn two_driven_runs_paced_differently_are_warned_about() {
        let mut before = BTreeMap::new();
        before.insert("udp-statsd".to_string(), driven_report(1_000_000.0, 2.0, 300));
        let mut after = BTreeMap::new();
        let mut derated = driven_report(1_000_000.0, 2.0, 0);
        // What `--rate-scale 0.25` / `--verify` would have produced.
        derated.median.udp.as_mut().unwrap().effective_rate = Some(22_500);
        derated.repeats[0].udp.as_mut().unwrap().effective_rate = Some(22_500);
        after.insert("udp-statsd".to_string(), derated);

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert!(
            cmp.warnings.iter().any(|w| w.contains("paced differently") && w.contains("22500")),
            "{:?}",
            cmp.warnings
        );
    }

    #[test]
    fn two_driven_runs_at_the_same_pace_are_not_warned_about() {
        let mut before = BTreeMap::new();
        before.insert("udp-statsd".to_string(), driven_report(1_000_000.0, 2.0, 300));
        let mut after = BTreeMap::new();
        after.insert("udp-statsd".to_string(), driven_report(1_000_000.0, 2.0, 120));

        let cmp = compare(&report("h", "c", before), &report("h", "c", after));
        assert!(
            !cmp.warnings.iter().any(|w| w.contains("paced differently")),
            "{:?}",
            cmp.warnings
        );
    }

    fn binary(sha256: &str) -> crate::result::BinaryInfo {
        crate::result::BinaryInfo {
            path: "/repo/target/release/logit".to_string(),
            sha256: sha256.to_string(),
            source_ref: None,
            source_sha: None,
            built_at: None,
        }
    }

    #[test]
    fn identical_binary_sha256_on_both_sides_warns() {
        let scenarios = BTreeMap::new();
        let mut a = report("box-a", "cpu-a", scenarios.clone());
        a.binary = Some(binary(&"a".repeat(64)));
        let mut b = report("box-a", "cpu-a", scenarios);
        b.binary = Some(binary(&"a".repeat(64)));

        let cmp = compare(&a, &b);
        assert!(cmp.warnings.iter().any(|w| w.contains("identical binary")), "{:?}", cmp.warnings);
    }

    #[test]
    fn different_binary_sha256s_do_not_warn() {
        let scenarios = BTreeMap::new();
        let mut a = report("box-a", "cpu-a", scenarios.clone());
        a.binary = Some(binary(&"a".repeat(64)));
        let mut b = report("box-a", "cpu-a", scenarios);
        b.binary = Some(binary(&"b".repeat(64)));

        let cmp = compare(&a, &b);
        assert!(!cmp.warnings.iter().any(|w| w.contains("identical binary")), "{:?}", cmp.warnings);
    }

    #[test]
    fn missing_binary_info_on_either_side_does_not_warn() {
        let scenarios = BTreeMap::new();
        let mut a = report("box-a", "cpu-a", scenarios.clone());
        a.binary = Some(binary(&"a".repeat(64)));
        let b = report("box-a", "cpu-a", scenarios); // no binary block -- an old results file

        let cmp = compare(&a, &b);
        assert!(!cmp.warnings.iter().any(|w| w.contains("identical binary")), "{:?}", cmp.warnings);
    }

    #[test]
    fn matching_environments_have_no_warning() {
        let scenarios = BTreeMap::new();
        let cmp = compare(
            &report("box-a", "cpu-a", scenarios.clone()),
            &report("box-a", "cpu-a", scenarios),
        );
        assert!(cmp.warnings.is_empty());
    }
}
