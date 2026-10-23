//! Per-thread CPU accounting for the spawned child, read from `/proc/PID/task/*` while it runs.
//!
//! A tokio multi-thread runtime can run one workload in modes that cost very different CPU per
//! event: every worker busy, or one worker doing the work while the rest idle, with events moving
//! between cores or staying on one. `perf` would tell them apart, but it needs
//! `perf_event_paranoid` below 2. These reads need nothing: the busiest thread's share of run time,
//! how many threads ran, and how often they migrated identify the mode from the results file.
//!
//! Each sweep reads, per task, `comm`, `schedstat` (run time, run-queue wait, timeslices), `stat`'s
//! field 39 (the CPU it last ran on), and `sched`'s `se.nr_migrations` when the kernel exposes it
//! (`CONFIG_SCHED_DEBUG`). Without `sched`, migrations are counted as changes of last CPU between
//! sweeps, which undercounts: a thread that moves and comes back within one interval counts zero.
//! [`ThreadSummary::migrations_source`] says which a summary used.
//!
//! **A thread's numbers are its last successful sweep.** Its task directory vanishes when it
//! exits, so each thread's last [`INTERVAL`] of run time is missing, and so is everything after
//! the final sweep, the shutdown drain included. These numbers describe a run's shape; `wait4`'s
//! rusage stays the CPU total.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

/// How often the sampler sweeps every task.
pub const INTERVAL: Duration = Duration::from_millis(200);

/// How many threads [`ThreadSummary::top`] lists.
const TOP: usize = 5;

/// `/proc/PID/task/TID/schedstat`: three counters since the thread started.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchedStat {
    pub run_ns: u64,
    pub wait_ns: u64,
    pub timeslices: u64,
}

pub fn parse_schedstat(text: &str) -> Option<SchedStat> {
    let mut fields = text.split_whitespace().map(|f| f.parse::<u64>().ok());
    Some(SchedStat {
        run_ns: fields.next()??,
        wait_ns: fields.next()??,
        timeslices: fields.next()??,
    })
}

/// The two `stat` fields the sampler needs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatFields {
    /// Field 22, in clock ticks since boot: tells a reused pid apart from the child.
    pub start_time: u64,
    /// Field 39: the CPU the thread last ran on.
    pub last_cpu: u32,
}

/// Parses `stat` from after the last `)`, since field 2, the thread name in parentheses, may itself
/// contain spaces and parentheses.
pub fn parse_stat(text: &str) -> Option<StatFields> {
    let rest = &text[text.rfind(')')? + 1..];
    // `rest` starts at field 3, so field N is index N - 3.
    let fields: Vec<&str> = rest.split_whitespace().collect();
    Some(StatFields {
        start_time: fields.get(22 - 3)?.parse().ok()?,
        last_cpu: fields.get(39 - 3)?.parse().ok()?,
    })
}

/// `se.nr_migrations` from `/proc/PID/task/TID/sched`.
pub fn parse_nr_migrations(text: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        (key.trim() == "se.nr_migrations").then(|| value.trim().parse().ok()).flatten()
    })
}

/// Where a summary's migration counts came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MigrationSource {
    /// Every thread's count is the kernel's `se.nr_migrations`.
    Sched,
    /// No thread's `sched` was readable; counts are last-CPU changes between sweeps.
    LastCpuChanges,
    /// Some threads had `sched` and some didn't.
    Mixed,
}

/// One thread's numbers at its last successful sweep.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadRow {
    /// `comm`, verbatim, which the kernel cuts to 15 bytes: `tokio-rt-worker` for a tokio worker,
    /// `logit-<id>` for a Lua thread, `logit` for the main thread.
    pub name: String,
    pub tid: u32,
    pub run_ms: f64,
    /// Time spent runnable but waiting for a CPU.
    pub wait_ms: f64,
    pub migrations: u64,
    pub last_cpu: u32,
}

/// One repeat's per-thread accounting, summarized.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreadSummary {
    /// Sweeps that read the child's task list.
    pub sweeps: u64,
    /// Threads that ran at all; a thread that never ran has no row anywhere.
    pub threads_ran: usize,
    /// `threads_ran` by thread name, so a worker count reads off one key.
    pub ran_by_name: BTreeMap<String, usize>,
    pub run_ms_total: f64,
    /// The busiest thread's run time over `run_ms_total`, 0 to 1: near 1 means one thread did the
    /// work, near `1 / threads_ran` means it was spread evenly.
    pub busiest_share: f64,
    pub migrations_total: u64,
    pub migrations_source: MigrationSource,
    pub run_queue_wait_ms_total: f64,
    /// The busiest threads by run time, at most five.
    pub top: Vec<ThreadRow>,
}

impl ThreadSummary {
    /// `, 3 threads ran (busiest 62%), 41 migrations, 1.2 ms run-queue wait` for a repeat's line.
    pub fn describe(&self) -> String {
        format!(
            ", {} threads ran (busiest {:.0}%), {} migrations{}, {:.1} ms run-queue wait",
            self.threads_ran,
            100.0 * self.busiest_share,
            self.migrations_total,
            match self.migrations_source {
                MigrationSource::Sched => "",
                MigrationSource::LastCpuChanges => " (CPU changes seen)",
                MigrationSource::Mixed => " (partly CPU changes seen)",
            },
            self.run_queue_wait_ms_total,
        )
    }

    /// The top threads as indented table lines.
    pub fn table(&self, indent: &str) -> Vec<String> {
        let mut lines = vec![format!(
            "{indent}{:<18} {:>8} {:>10} {:>9} {:>6} {:>4}",
            "thread", "tid", "run ms", "wait ms", "migr", "cpu"
        )];
        for row in &self.top {
            lines.push(format!(
                "{indent}{:<18} {:>8} {:>10.1} {:>9.1} {:>6} {:>4}",
                row.name, row.tid, row.run_ms, row.wait_ms, row.migrations, row.last_cpu
            ));
        }
        lines
    }
}

/// One thread as the sampler last saw it.
#[derive(Debug, Clone)]
struct Track {
    name: String,
    sched: SchedStat,
    last_cpu: u32,
    /// `se.nr_migrations` at the last sweep that could read it.
    nr_migrations: Option<u64>,
    /// Last-CPU changes between sweeps, the fallback count.
    cpu_changes: u64,
}

/// The sweeps' accumulated state for one child.
#[derive(Debug, Default)]
pub struct Sampler {
    pid: u32,
    /// The child's start time at the first sweep; a later mismatch is a reused pid.
    start_time: Option<u64>,
    threads: BTreeMap<u32, Track>,
    sweeps: u64,
}

impl Sampler {
    pub fn new(pid: u32) -> Sampler {
        Sampler { pid, ..Sampler::default() }
    }

    /// Reads every task once. Returns `false` once the child is gone (reaped, or its pid reused),
    /// after which no sweep can add anything. A thread that exits mid-sweep keeps its previous
    /// numbers.
    pub fn sweep(&mut self) -> bool {
        let base = format!("/proc/{}", self.pid);
        let Some(leader) = read(&format!("{base}/stat")).as_deref().and_then(parse_stat) else {
            return false;
        };
        match self.start_time {
            Some(start) if start != leader.start_time => return false,
            Some(_) => {}
            None => self.start_time = Some(leader.start_time),
        }
        let Ok(entries) = fs::read_dir(format!("{base}/task")) else { return false };
        for entry in entries.flatten() {
            let Some(tid) = entry.file_name().to_str().and_then(|s| s.parse::<u32>().ok()) else {
                continue;
            };
            let dir = format!("{base}/task/{tid}");
            let Some(sched) =
                read(&format!("{dir}/schedstat")).as_deref().and_then(parse_schedstat)
            else {
                continue;
            };
            let Some(stat) = read(&format!("{dir}/stat")).as_deref().and_then(parse_stat) else {
                continue;
            };
            let name = read(&format!("{dir}/comm")).map(|c| c.trim_end_matches('\n').to_string());
            let nr_migrations =
                read(&format!("{dir}/sched")).as_deref().and_then(parse_nr_migrations);
            self.record(tid, name, sched, stat.last_cpu, nr_migrations);
        }
        self.sweeps += 1;
        true
    }

    /// Folds one thread's reads into its track.
    fn record(
        &mut self,
        tid: u32,
        name: Option<String>,
        sched: SchedStat,
        last_cpu: u32,
        nr_migrations: Option<u64>,
    ) {
        match self.threads.get_mut(&tid) {
            Some(track) => {
                if track.last_cpu != last_cpu {
                    track.cpu_changes += 1;
                }
                if let Some(name) = name {
                    track.name = name;
                }
                track.sched = sched;
                track.last_cpu = last_cpu;
                if nr_migrations.is_some() {
                    track.nr_migrations = nr_migrations;
                }
            }
            None => {
                self.threads.insert(
                    tid,
                    Track {
                        name: name.unwrap_or_else(|| "?".to_string()),
                        sched,
                        last_cpu,
                        nr_migrations,
                        cpu_changes: 0,
                    },
                );
            }
        }
    }

    pub fn summary(&self) -> ThreadSummary {
        let ran: Vec<(&u32, &Track)> =
            self.threads.iter().filter(|(_, track)| track.sched.run_ns > 0).collect();
        let mut ran_by_name = BTreeMap::new();
        for (_, track) in &ran {
            *ran_by_name.entry(track.name.clone()).or_insert(0) += 1;
        }
        let run_ns_total: u64 = ran.iter().map(|(_, t)| t.sched.run_ns).sum();
        let busiest = ran.iter().map(|(_, t)| t.sched.run_ns).max().unwrap_or(0);
        let from_sched = ran.iter().filter(|(_, t)| t.nr_migrations.is_some()).count();
        let migrations = |track: &Track| track.nr_migrations.unwrap_or(track.cpu_changes);

        let mut rows: Vec<ThreadRow> = ran
            .iter()
            .map(|(tid, track)| ThreadRow {
                name: track.name.clone(),
                tid: **tid,
                run_ms: ns_to_ms(track.sched.run_ns),
                wait_ms: ns_to_ms(track.sched.wait_ns),
                migrations: migrations(track),
                last_cpu: track.last_cpu,
            })
            .collect();
        rows.sort_by(|a, b| b.run_ms.total_cmp(&a.run_ms).then(a.tid.cmp(&b.tid)));
        rows.truncate(TOP);

        ThreadSummary {
            sweeps: self.sweeps,
            threads_ran: ran.len(),
            ran_by_name,
            run_ms_total: ns_to_ms(run_ns_total),
            busiest_share: if run_ns_total == 0 {
                0.0
            } else {
                busiest as f64 / run_ns_total as f64
            },
            migrations_total: ran.iter().map(|(_, t)| migrations(t)).sum(),
            migrations_source: if from_sched == ran.len() {
                MigrationSource::Sched
            } else if from_sched == 0 {
                MigrationSource::LastCpuChanges
            } else {
                MigrationSource::Mixed
            },
            run_queue_wait_ms_total: ns_to_ms(ran.iter().map(|(_, t)| t.sched.wait_ns).sum()),
            top: rows,
        }
    }
}

fn ns_to_ms(ns: u64) -> f64 {
    ns as f64 / 1_000_000.0
}

/// A `/proc` file's text, or `None` when the thread or process is gone.
fn read(path: &str) -> Option<String> {
    fs::read_to_string(path).ok()
}

/// A sampler running on its own thread for one child. Dropping it without [`Running::finish`]
/// stops the thread at its next wake-up.
pub struct Running {
    wake: mpsc::Sender<()>,
    handle: JoinHandle<ThreadSummary>,
}

impl Running {
    /// Sweeps now rather than at the next interval, so the last sweep before a SIGTERM lands as
    /// late as it can.
    pub fn sweep_now(&self) {
        let _ = self.wake.send(());
    }

    /// Stops sampling and returns the summary. Call it once the child is reaped: the sampler keeps
    /// sweeping until the child is gone or this is called.
    pub fn finish(self) -> Option<ThreadSummary> {
        drop(self.wake);
        self.handle.join().ok()
    }
}

/// Starts sweeping `pid` every [`INTERVAL`] until it's gone or the returned handle finishes.
pub fn start(pid: u32) -> Running {
    let (wake, woken) = mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        let mut sampler = Sampler::new(pid);
        while sampler.sweep() {
            match woken.recv_timeout(INTERVAL) {
                Ok(()) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }
        sampler.summary()
    });
    Running { wake, handle }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A real `/proc/self/stat` line, its `comm` replaced with one holding spaces and parentheses.
    const STAT: &str = "1410307 (a (b) c) R 1410302 1410307 1410302 0 -1 4194304 94 0 0 0 0 0 0 0 \
        20 0 1 0 21549880 236335104 490 18446744073709551615 94395875500032 94395875521313 \
        140727190700096 0 0 0 0 0 0 0 0 0 17 6 0 0 0 0 0 94395875539440 94395875541096 \
        94396317020160 140727190707398 140727190707418 140727190707418 140727190712299 0\n";

    const SCHED: &str = "cat (1410303, #threads: 1)\n\
        -------------------------------------------------------------------\n\
        se.exec_start                                :     215339161.864468\n\
        se.sum_exec_runtime                          :             0.443358\n\
        se.nr_migrations                             :                   17\n\
        nr_switches                                  :                    1\n";

    #[test]
    fn stat_fields_are_counted_from_after_the_last_parenthesis() {
        assert_eq!(parse_stat(STAT), Some(StatFields { start_time: 21549880, last_cpu: 6 }));
    }

    #[test]
    fn a_truncated_stat_line_parses_to_none() {
        assert_eq!(parse_stat("1 (x) R 1 2 3"), None);
        assert_eq!(parse_stat("no parenthesis"), None);
    }

    #[test]
    fn schedstat_reads_its_three_counters() {
        assert_eq!(
            parse_schedstat("39514 18636 3\n"),
            Some(SchedStat { run_ns: 39514, wait_ns: 18636, timeslices: 3 })
        );
        assert_eq!(parse_schedstat("1 2\n"), None);
    }

    #[test]
    fn sched_yields_nr_migrations_and_a_missing_line_yields_none() {
        assert_eq!(parse_nr_migrations(SCHED), Some(17));
        assert_eq!(parse_nr_migrations("se.exec_start : 1.0\n"), None);
    }

    fn stat(run_ns: u64, wait_ns: u64) -> SchedStat {
        SchedStat { run_ns, wait_ns, timeslices: 1 }
    }

    #[test]
    fn without_sched_migrations_are_counted_as_last_cpu_changes() {
        let mut sampler = Sampler::new(1);
        let worker = Some("tokio-rt-worker".to_string());
        sampler.record(10, worker.clone(), stat(1_000_000, 0), 2, None);
        sampler.record(10, worker.clone(), stat(2_000_000, 0), 3, None);
        sampler.record(10, worker.clone(), stat(3_000_000, 500_000), 3, None);
        sampler.record(10, worker, stat(4_000_000, 500_000), 2, None);
        let summary = sampler.summary();
        assert_eq!(summary.migrations_total, 2);
        assert_eq!(summary.migrations_source, MigrationSource::LastCpuChanges);
        assert_eq!(summary.top[0].run_ms, 4.0);
        assert_eq!(summary.top[0].wait_ms, 0.5);
        assert_eq!(summary.top[0].last_cpu, 2);
    }

    #[test]
    fn the_summary_counts_threads_that_ran_and_the_busiest_ones_share() {
        let mut sampler = Sampler::new(1);
        sampler.record(1, Some("logit".into()), stat(0, 0), 0, Some(0));
        sampler.record(2, Some("tokio-rt-worker".into()), stat(6_000_000, 1_000_000), 1, Some(4));
        sampler.record(3, Some("tokio-rt-worker".into()), stat(2_000_000, 0), 2, Some(1));
        sampler.record(4, Some("logit-lua".into()), stat(2_000_000, 0), 3, Some(0));
        // A thread that vanished keeps its last numbers: nothing here removes tid 4.
        let summary = sampler.summary();
        assert_eq!(summary.threads_ran, 3);
        assert_eq!(summary.ran_by_name.get("tokio-rt-worker"), Some(&2));
        assert_eq!(summary.ran_by_name.get("logit"), None);
        assert_eq!(summary.run_ms_total, 10.0);
        assert!((summary.busiest_share - 0.6).abs() < 1e-12);
        assert_eq!(summary.migrations_total, 5);
        assert_eq!(summary.migrations_source, MigrationSource::Sched);
        assert_eq!(summary.run_queue_wait_ms_total, 1.0);
        assert_eq!(summary.top.iter().map(|r| r.tid).collect::<Vec<_>>(), vec![2, 3, 4]);
    }

    #[test]
    fn top_lists_at_most_five_threads() {
        let mut sampler = Sampler::new(1);
        for tid in 1..=8 {
            sampler.record(tid, Some("t".into()), stat(u64::from(tid) * 1_000, 0), 0, Some(0));
        }
        let summary = sampler.summary();
        assert_eq!(summary.top.len(), 5);
        assert_eq!(summary.top[0].tid, 8);
    }

    #[test]
    fn sweeping_this_process_finds_this_thread() {
        let mut sampler = Sampler::new(std::process::id());
        assert!(sampler.sweep());
        let summary = sampler.summary();
        assert_eq!(summary.sweeps, 1);
        assert!(summary.threads_ran >= 1, "{summary:?}");
    }

    #[test]
    fn a_pid_that_does_not_exist_ends_the_sampler() {
        // Above the kernel's `pid_max` ceiling (2^22), so no process can hold it.
        assert!(!Sampler::new(u32::MAX).sweep());
    }
}
