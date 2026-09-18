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
/// Both fields are `None` (rendered as JSON `null`) rather than a confident-looking default when
/// `git` itself couldn't answer -- a git-worktree checkout's dev container is one real case this
/// happens in (`run.rs::git_info`'s doc), and a silent `false`/`"unknown"` string would read as a
/// real answer instead of a gap.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GitInfo {
    pub sha: Option<String>,
    pub dirty: Option<bool>,
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
    /// What the box's power/thermal policy was while this ran. Absent in every results file
    /// written before it existed, and any field of it may be absent on a machine or container
    /// that doesn't expose it -- see [`BoxState`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub box_state: Option<BoxState>,
    pub scenarios: BTreeMap<String, ScenarioReport>,
}

/// The CPU frequency policy and power source a run was taken under, read best-effort from sysfs.
///
/// Recorded because it is the single largest source of unexplained movement in these numbers, and
/// a results file that doesn't carry it can't be told apart from one that does: a `powersave`
/// governor or a run on battery can move CPU µs/event by tens of percent with nothing in the code
/// having changed (`perf/load/README.md`'s "Tuning", and `docs/design/performance.md`). Every
/// field is `Option` and read with a plain file read that is allowed to fail -- a container or a
/// non-Linux host that exposes none of this records `null`s rather than refusing to run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BoxState {
    /// `/sys/devices/system/cpu/cpu0/cpufreq/scaling_governor` -- `performance` or `powersave` on
    /// an `amd_pstate`/`intel_pstate` box. Visible inside this repo's dev container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scaling_governor: Option<String>,
    /// `…/cpu0/cpufreq/energy_performance_preference` -- the finer knob underneath the governor
    /// (`performance`, `balance_performance`, `balance_power`, `power`). Also visible in the
    /// container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub energy_performance_preference: Option<String>,
    /// `/sys/firmware/acpi/platform_profile` -- the firmware-level profile, where a machine has
    /// one. Verified **absent** on this repo's own dev box, hence very much optional.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_profile: Option<String>,
    /// Whether a mains supply is online, from the first `/sys/class/power_supply/*` whose `type`
    /// is `Mains`. Found by scanning rather than by name: it is `ACAD` on this box, `AC` or
    /// `ADP1` on others.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_ac_power: Option<bool>,
}

impl BoxState {
    /// The reasons this run's numbers should be read with suspicion, in words -- empty when the
    /// box looks like somewhere a measurement can be taken.
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        if self.scaling_governor.as_deref() == Some("powersave") {
            warnings.push(
                "the CPU governor is `powersave` -- CPU µs/event is not comparable with a run \
                 taken under `performance`, and a driven scenario's drop rate least of all"
                    .to_string(),
            );
        }
        if self.on_ac_power == Some(false) {
            warnings.push(
                "this box is on battery -- expect every absolute number to be depressed, and the \
                 amount to drift as the run goes on"
                    .to_string(),
            );
        }
        warnings
    }
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
    /// Wall time from the process's own `ready` line to the completion line --
    /// `crate::run::spawn_and_measure`'s doc has the mechanism. Falls back to spawn -> completion
    /// (with a printed warning) on the rare repeat where `ready` was never observed; `startup_s`
    /// distinguishes that case from a real, fast startup.
    pub wall_s: f64,
    /// Spawn -> the process's own `ready` line: `tracing::info!(target: "logit", "ready")`, logged
    /// once the bind pass has opened every listener's socket and every node has been spawned
    /// (`crates/logit-pipeline/src/runtime.rs`). `None` (rendered as JSON `null`, matching this
    /// crate's other "the source couldn't answer" fields -- `GitInfo`'s doc has the same
    /// reasoning) when that line never arrived before the completion line, rather than a
    /// confident-looking `0.0`.
    pub startup_s: Option<f64>,
    pub user_s: f64,
    pub sys_s: f64,
    pub max_rss_bytes: u64,
    pub events_per_s: f64,
    /// `(user_s + sys_s) * 1e6 / count` -- the headline regression signal
    /// (docs/adr/load-test-harness.md): far less noisy than wall time on a shared box, since it
    /// doesn't care how many other processes were competing for the CPU during the run.
    pub cpu_us_per_event: f64,
    /// The socket-side half of a real-socket (`Workload::Driven`) repeat -- absent for every
    /// generator-driven scenario, and absent from every results file written before ADR
    /// `udp-intake-batching-and-socket-visibility` existed. `#[serde(default)]` plus
    /// `skip_serializing_if` is the same optional-field shape [`Sample::startup_s`] already
    /// established, and is what keeps an old results file loadable by a new `compare`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub udp: Option<UdpSample>,
}

/// What a real-socket repeat's datagrams actually did: how many were sent, how many the listener
/// saw, and where the difference went.
///
/// Every field here exists because a UDP scenario is the first one in this codebase where "events
/// produced" and "events measured" can honestly differ. `events_delivered` is the denominator
/// [`Sample::events_per_s`] and [`Sample::cpu_us_per_event`] are computed over -- never
/// `sent_datagrams` or `sent_lines`, which would understate the true per-event cost by exactly the
/// drop rate, in precisely the regime this scenario family's baseline is tuned into.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct UdpSample {
    /// Datagrams `crate::load`'s sender got the kernel to accept.
    pub sent_datagrams: u64,
    /// statsd lines inside those datagrams -- the load spec's own count, not a decode.
    pub sent_lines: u64,
    /// `logit.input.datagrams`: what the listener actually read off the socket.
    pub received_datagrams: u64,
    /// `logit.input.reads`: read syscalls the listener made. `received_datagrams / reads` is the
    /// **mean fill** of one `recvmmsg(2)` batch -- the number that says whether `receive.read_batch`
    /// is the constraint or an irrelevance (`docs/deploying.md`'s "Listener intake").
    ///
    /// `#[serde(default)]` because a results file written before `logit.input.reads` existed has
    /// no such key, and a `compare` against one must still load -- the same optional-field shape
    /// [`Sample::startup_s`] established. `0` there means "not recorded", which is also what a
    /// non-UDP run would report, and [`UdpSample::mean_fill`] returns `None` for it rather than
    /// dividing by it.
    #[serde(default)]
    pub reads: u64,
    /// `logit.input.kernel.drops`: discarded by the kernel before `recv_from` could return them.
    /// On loopback `sent == received + this`, which `crate::run`'s self-check asserts.
    pub kernel_dropped: u64,
    /// `logit.component.datagrams.dropped{reason=overflow_*}`: `ReceiveQueue` eviction -- loss
    /// `logit` chose and counted itself, downstream of the kernel's.
    pub queue_dropped: u64,
    /// `logit.component.events.received` at the deepest node -- the denominator.
    pub events_delivered: u64,
    /// Retryable `sendmmsg` failures the sender absorbed (`ENOBUFS`, a stale `ECONNREFUSED`).
    /// Non-zero here doesn't mean a datagram was lost -- each one was retried -- but a large
    /// number means the sender was fighting the local send path rather than measuring the
    /// receiver.
    pub send_errors: u64,
    /// The high-water mark of `logit.input.receive_buffer.utilization` across the run. 1.0 is not
    /// "nearly full": it is exactly the point at which the kernel begins dropping, so a baseline
    /// sitting just under it is the regime this scenario family is tuned for.
    pub kernel_rcvbuf_utilization_max: f64,
    /// Datagrams/s the blast was actually paced at, after `--rate-scale`/`--verify` -- not the
    /// spec's own `rate:`, which is what it would have been unscaled. `None` for an unpaced spec,
    /// and for any results file written before the flag existed.
    ///
    /// Recorded because two runs of the same scenario at different scales are not comparable, and
    /// nothing else in the file would say so: `compare` warns when these differ.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effective_rate: Option<u64>,
}

impl UdpSample {
    /// Datagrams per read syscall, or `None` when the read counter was not recorded (a results
    /// file from before it existed). Not a rate and not comparable across scenarios with different
    /// datagram sizes -- it is only ever read against the `read_batch` that produced it.
    pub fn mean_fill(&self) -> Option<f64> {
        (self.reads > 0).then(|| self.received_datagrams as f64 / self.reads as f64)
    }

    /// Datagrams lost anywhere, as a fraction of those sent -- kernel and receive-queue drops
    /// together, since someone watching data loss cares that it happened, not which side of the
    /// socket boundary it happened on. `0.0` for a run that sent nothing, rather than a `NaN` that
    /// would poison every median it lands in.
    pub fn drop_rate(&self) -> f64 {
        if self.sent_datagrams == 0 {
            return 0.0;
        }
        (self.kernel_dropped + self.queue_dropped) as f64 / self.sent_datagrams as f64
    }
}

impl Sample {
    pub fn from_usage(
        count: u64,
        startup: Option<Duration>,
        wall: Duration,
        user: Duration,
        sys: Duration,
        max_rss_bytes: u64,
    ) -> Self {
        let wall_s = wall.as_secs_f64();
        let cpu_s = user.as_secs_f64() + sys.as_secs_f64();
        let count_f = count as f64;
        Sample {
            wall_s,
            startup_s: startup.map(|d| d.as_secs_f64()),
            user_s: user.as_secs_f64(),
            sys_s: sys.as_secs_f64(),
            max_rss_bytes,
            events_per_s: count_f / wall_s,
            cpu_us_per_event: cpu_s * 1_000_000.0 / count_f,
            udp: None,
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

/// [`median_f64`] over just the repeats that actually observed a `ready` line -- `None` only when
/// *none* of them did, matching [`Sample::startup_s`]'s own "the source couldn't answer" meaning
/// rather than folding a missing repeat in as if it measured zero.
fn median_f64_opt(values: &[Option<f64>]) -> Option<f64> {
    let present: Vec<f64> = values.iter().filter_map(|v| *v).collect();
    (!present.is_empty()).then(|| median_f64(&present))
}

/// [`min_f64`] over just the repeats that actually observed a `ready` line -- same reasoning as
/// [`median_f64_opt`].
fn min_f64_opt(values: &[Option<f64>]) -> Option<f64> {
    let present: Vec<f64> = values.iter().filter_map(|v| *v).collect();
    (!present.is_empty()).then(|| min_f64(&present))
}

/// Builds the per-field median [`Sample`] across `samples`. Each field's median is computed
/// independently of the others -- the result is not, and isn't meant to be, any single repeat
/// that actually ran; it's a per-metric summary, exactly what `compare.rs` diffs.
pub fn median_sample(samples: &[Sample]) -> Sample {
    Sample {
        wall_s: median_f64(&samples.iter().map(|s| s.wall_s).collect::<Vec<_>>()),
        startup_s: median_f64_opt(&samples.iter().map(|s| s.startup_s).collect::<Vec<_>>()),
        user_s: median_f64(&samples.iter().map(|s| s.user_s).collect::<Vec<_>>()),
        sys_s: median_f64(&samples.iter().map(|s| s.sys_s).collect::<Vec<_>>()),
        max_rss_bytes: median_u64(&samples.iter().map(|s| s.max_rss_bytes).collect::<Vec<_>>()),
        events_per_s: median_f64(&samples.iter().map(|s| s.events_per_s).collect::<Vec<_>>()),
        cpu_us_per_event: median_f64(
            &samples.iter().map(|s| s.cpu_us_per_event).collect::<Vec<_>>(),
        ),
        udp: reduce_udp(samples, median_u64, median_f64),
    }
}

/// Builds the per-field minimum [`Sample`] across `samples`, the same independent-per-field way
/// [`median_sample`] does.
pub fn min_sample(samples: &[Sample]) -> Sample {
    Sample {
        wall_s: min_f64(&samples.iter().map(|s| s.wall_s).collect::<Vec<_>>()),
        startup_s: min_f64_opt(&samples.iter().map(|s| s.startup_s).collect::<Vec<_>>()),
        user_s: min_f64(&samples.iter().map(|s| s.user_s).collect::<Vec<_>>()),
        sys_s: min_f64(&samples.iter().map(|s| s.sys_s).collect::<Vec<_>>()),
        max_rss_bytes: min_u64(&samples.iter().map(|s| s.max_rss_bytes).collect::<Vec<_>>()),
        events_per_s: min_f64(&samples.iter().map(|s| s.events_per_s).collect::<Vec<_>>()),
        cpu_us_per_event: min_f64(&samples.iter().map(|s| s.cpu_us_per_event).collect::<Vec<_>>()),
        udp: reduce_udp(samples, min_u64, min_f64),
    }
}

/// Reduces every [`UdpSample`] field independently with the caller's own reducers -- the same
/// field-wise treatment the rest of [`Sample`] gets, and for the same reason: the summary is a
/// per-metric picture, not any one repeat.
///
/// `None` unless *every* repeat carried a `UdpSample`. A mixed set can only come from a results
/// file somebody has edited or merged by hand, and summarizing a subset of repeats as if it were
/// all of them would quietly report a drop rate computed over the wrong denominator.
fn reduce_udp(
    samples: &[Sample],
    reduce_u64: fn(&[u64]) -> u64,
    reduce_f64: fn(&[f64]) -> f64,
) -> Option<UdpSample> {
    let udp: Vec<UdpSample> = samples.iter().filter_map(|sample| sample.udp).collect();
    if udp.is_empty() || udp.len() != samples.len() {
        return None;
    }
    let field_u64 =
        |get: fn(&UdpSample) -> u64| reduce_u64(&udp.iter().map(get).collect::<Vec<_>>());
    Some(UdpSample {
        sent_datagrams: field_u64(|u| u.sent_datagrams),
        sent_lines: field_u64(|u| u.sent_lines),
        received_datagrams: field_u64(|u| u.received_datagrams),
        reads: field_u64(|u| u.reads),
        kernel_dropped: field_u64(|u| u.kernel_dropped),
        queue_dropped: field_u64(|u| u.queue_dropped),
        events_delivered: field_u64(|u| u.events_delivered),
        send_errors: field_u64(|u| u.send_errors),
        kernel_rcvbuf_utilization_max: reduce_f64(
            &udp.iter().map(|u| u.kernel_rcvbuf_utilization_max).collect::<Vec<_>>(),
        ),
        // Config, not a measurement: every repeat of one scenario ran at the same pace, so there
        // is nothing to reduce. Carried through only when they all agree, so a hand-merged file
        // reporting two different paces as one number is impossible.
        effective_rate: udp
            .iter()
            .all(|u| u.effective_rate == udp[0].effective_rate)
            .then(|| udp[0].effective_rate)
            .flatten(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(wall_s: f64, cpu_us_per_event: f64, max_rss_bytes: u64) -> Sample {
        Sample {
            wall_s,
            startup_s: Some(wall_s / 10.0),
            user_s: wall_s / 2.0,
            sys_s: wall_s / 2.0,
            max_rss_bytes,
            events_per_s: 1.0 / wall_s,
            cpu_us_per_event,
            udp: None,
        }
    }

    fn udp(sent: u64, kernel_dropped: u64, delivered: u64, utilization: f64) -> UdpSample {
        UdpSample {
            sent_datagrams: sent,
            sent_lines: sent,
            received_datagrams: sent - kernel_dropped,
            reads: sent - kernel_dropped,
            kernel_dropped,
            queue_dropped: 0,
            events_delivered: delivered,
            send_errors: 0,
            kernel_rcvbuf_utilization_max: utilization,
            effective_rate: Some(100_000),
        }
    }

    #[test]
    fn median_of_an_odd_number_of_samples_is_the_middle_value() {
        let samples = vec![sample(3.0, 30.0, 300), sample(1.0, 10.0, 100), sample(2.0, 20.0, 200)];
        let median = median_sample(&samples);
        assert_eq!(median.wall_s, 2.0);
        assert_eq!(median.startup_s, Some(0.2));
        assert_eq!(median.cpu_us_per_event, 20.0);
        assert_eq!(median.max_rss_bytes, 200);
    }

    #[test]
    fn startup_s_median_ignores_repeats_that_never_saw_a_ready_line() {
        let mut missing_ready = sample(2.0, 20.0, 200);
        missing_ready.startup_s = None;
        let samples = vec![sample(1.0, 10.0, 100), missing_ready, sample(3.0, 30.0, 300)];
        // Only the two repeats with a real startup_s (0.1, 0.3) participate -- their median, not a
        // three-way median that would treat the missing one as if it measured zero.
        assert_eq!(median_sample(&samples).startup_s, Some(0.2));
    }

    #[test]
    fn startup_s_median_is_none_when_no_repeat_observed_a_ready_line() {
        let mut a = sample(1.0, 10.0, 100);
        a.startup_s = None;
        let mut b = sample(2.0, 20.0, 200);
        b.startup_s = None;
        assert_eq!(median_sample(&[a, b]).startup_s, None);
    }

    #[test]
    fn startup_s_min_ignores_repeats_that_never_saw_a_ready_line() {
        let mut missing_ready = sample(1.0, 10.0, 100); // startup_s would be 0.1, the smallest
        missing_ready.startup_s = None;
        let samples = vec![missing_ready, sample(2.0, 20.0, 200)];
        assert_eq!(min_sample(&samples).startup_s, Some(0.2));
    }

    #[test]
    fn median_of_an_even_number_of_samples_averages_the_two_middle_values() {
        let samples = vec![
            sample(1.0, 10.0, 100),
            sample(2.0, 20.0, 200),
            sample(3.0, 30.0, 300),
            sample(4.0, 40.0, 400),
        ];
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
            Some(Duration::from_millis(300)),
            Duration::from_secs(2),
            Duration::from_millis(1_500),
            Duration::from_millis(500),
            123 * 1024,
        );
        assert_eq!(sample.wall_s, 2.0);
        assert_eq!(sample.startup_s, Some(0.3));
        assert_eq!(sample.events_per_s, 500_000.0);
        // (1.5s + 0.5s) * 1e6us / 1_000_000 events = 2.0 us/event.
        assert_eq!(sample.cpu_us_per_event, 2.0);
        assert_eq!(sample.max_rss_bytes, 123 * 1024);
    }

    #[test]
    fn sample_from_usage_records_no_startup_when_ready_was_never_observed() {
        let sample = Sample::from_usage(
            1_000_000,
            None,
            Duration::from_secs(2),
            Duration::from_millis(1_500),
            Duration::from_millis(500),
            123 * 1024,
        );
        assert_eq!(sample.startup_s, None);
    }

    #[test]
    fn run_report_round_trips_through_json() {
        let mut scenarios = BTreeMap::new();
        // `wall_s` values chosen so every derived field is exactly representable in binary
        // (1.0, 2.0, 0.5, ...) -- serde_json's default float parser (without its
        // `float_roundtrip` feature, not enabled here) isn't guaranteed to recover every f64 bit
        // for bit from its shortest decimal rendering, only ones like these; the point of this
        // test is the JSON *shape* round-tripping, not pinning that parser behavior.
        let repeats = vec![sample(1.0, 10.0, 100), sample(2.0, 12.0, 110)];
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
            git: GitInfo { sha: Some("abc123".to_string()), dirty: Some(false) },
            timestamp: "2026-09-12T00:00:00Z".to_string(),
            hostname: "devbox".to_string(),
            cpu_model: "Some CPU".to_string(),
            nproc: 8,
            rustc: "rustc 1.98.1".to_string(),
            profile: "release".to_string(),
            label: Some("baseline".to_string()),
            box_state: None,
            scenarios,
        };

        let json = serde_json::to_string(&report).unwrap();
        let round_tripped: RunReport = serde_json::from_str(&json).unwrap();
        assert_eq!(round_tripped, report);
    }

    #[test]
    fn udp_fields_are_reduced_independently_like_every_other_field() {
        let mut a = sample(1.0, 10.0, 100);
        a.udp = Some(udp(1_000, 10, 990, 0.80));
        let mut b = sample(2.0, 20.0, 200);
        b.udp = Some(udp(1_000, 30, 970, 0.95));
        let mut c = sample(3.0, 30.0, 300);
        c.udp = Some(udp(1_000, 20, 980, 0.60));

        let median = median_sample(&[a, b, c]).udp.expect("every repeat carried one");
        assert_eq!(median.kernel_dropped, 20);
        assert_eq!(median.events_delivered, 980);
        assert!((median.kernel_rcvbuf_utilization_max - 0.80).abs() < 1e-9);

        let min = min_sample(&[a, b, c]).udp.expect("every repeat carried one");
        assert_eq!(min.kernel_dropped, 10);
        assert_eq!(min.events_delivered, 970);
    }

    #[test]
    fn a_generated_scenarios_samples_summarize_with_no_udp_block_at_all() {
        let samples = vec![sample(1.0, 10.0, 100), sample(2.0, 20.0, 200)];
        assert_eq!(median_sample(&samples).udp, None);
        assert_eq!(min_sample(&samples).udp, None);
    }

    #[test]
    fn a_partial_set_of_udp_samples_summarizes_to_none_rather_than_a_subset() {
        let mut a = sample(1.0, 10.0, 100);
        a.udp = Some(udp(1_000, 10, 990, 0.8));
        let b = sample(2.0, 20.0, 200); // no udp block
        assert_eq!(median_sample(&[a, b]).udp, None);
    }

    #[test]
    fn drop_rate_counts_both_kinds_of_loss_and_never_divides_by_zero() {
        let mut both = udp(1_000, 10, 980, 0.9);
        both.queue_dropped = 10;
        assert!((both.drop_rate() - 0.02).abs() < 1e-12);
        assert_eq!(udp(0, 0, 0, 0.0).drop_rate(), 0.0);
    }

    #[test]
    fn the_effective_rate_survives_a_summary_only_when_every_repeat_agrees() {
        let mut a = sample(1.0, 10.0, 100);
        a.udp = Some(udp(1_000, 10, 990, 0.8));
        let mut b = sample(2.0, 20.0, 200);
        b.udp = Some(udp(1_000, 10, 990, 0.8));
        assert_eq!(median_sample(&[a, b]).udp.unwrap().effective_rate, Some(100_000));

        // A hand-merged file mixing two paces must not report either of them as if it were both.
        let mut c = sample(3.0, 30.0, 300);
        let mut mixed = udp(1_000, 10, 990, 0.8);
        mixed.effective_rate = Some(25_000);
        c.udp = Some(mixed);
        assert_eq!(median_sample(&[a, c]).udp.unwrap().effective_rate, None);
    }

    #[test]
    fn a_udp_sample_written_before_the_rate_scale_flag_existed_still_loads() {
        // Exactly the `udp` block this harness wrote before `--rate-scale`: no `effective_rate`.
        let json = r#"{
            "sent_datagrams": 100, "sent_lines": 100, "received_datagrams": 99,
            "kernel_dropped": 1, "queue_dropped": 0, "events_delivered": 99,
            "send_errors": 0, "kernel_rcvbuf_utilization_max": 0.5
        }"#;
        let udp: UdpSample = serde_json::from_str(json).expect("an older udp block must load");
        assert_eq!(udp.effective_rate, None);
        assert_eq!(udp.events_delivered, 99);
    }

    #[test]
    fn a_results_file_written_before_udp_samples_existed_still_loads() {
        // Exactly the JSON the pre-ADR harness wrote for one repeat: no `udp` key anywhere.
        let json = r#"{
            "wall_s": 2.0, "startup_s": 0.2, "user_s": 1.0, "sys_s": 1.0,
            "max_rss_bytes": 1024, "events_per_s": 500.0, "cpu_us_per_event": 4.0
        }"#;
        let sample: Sample = serde_json::from_str(json).expect("an old sample must still load");
        assert_eq!(sample.udp, None);
        assert_eq!(sample.cpu_us_per_event, 4.0);
    }

    #[test]
    fn a_sample_with_no_udp_block_omits_the_key_entirely() {
        let json = serde_json::to_string(&sample(1.0, 2.0, 3)).unwrap();
        assert!(!json.contains("udp"), "{json}");
    }

    #[test]
    fn run_report_label_is_omitted_from_json_when_absent() {
        let report = RunReport {
            git: GitInfo { sha: Some("abc123".to_string()), dirty: Some(true) },
            timestamp: "2026-09-12T00:00:00Z".to_string(),
            hostname: "devbox".to_string(),
            cpu_model: "Some CPU".to_string(),
            nproc: 4,
            rustc: "rustc 1.98.1".to_string(),
            profile: "release".to_string(),
            label: None,
            box_state: None,
            scenarios: BTreeMap::new(),
        };
        let json = serde_json::to_string(&report).unwrap();
        assert!(!json.contains("label"), "{json}");
    }
}
