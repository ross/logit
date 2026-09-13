//! `wait4`-based process reaping: blocks until a spawned child exits and reports exactly that
//! child's resource usage in the same syscall (docs/adr/load-test-harness.md's "measure the real
//! binary" decision).
//!
//! Deliberately `wait4`, never `libc::getrusage(RUSAGE_CHILDREN)`: that call aggregates *every*
//! child this process has ever reaped -- in `logit-perf run`, that would include the `cargo
//! build` step already run in the same process before the first scenario spawns, silently
//! folding a multi-second compile into the first repeat's CPU time. `wait4` is attributed by pid,
//! so it can only ever report the one child it reaped.

#[cfg(not(target_os = "linux"))]
compile_error!(
    "crates/logit-perf/src/rusage.rs hard-codes ru_maxrss as kibibytes, true on Linux but not \
     POSIX-guaranteed (BSD reports bytes there instead) -- see Usage::max_rss_bytes's doc. This \
     harness only ever runs in this project's Linux dev container / CI image \
     (docs/adr/containerized-development.md), so this guard exists to fail a build loudly rather \
     than silently mis-scale RSS on some other target."
);

use std::io;
use std::time::Duration;

/// One spawned child's resource usage, plus the wall-clock elapsed the caller measured around it.
/// `wait4` itself has no notion of wall time -- only the caller, watching for a scenario's own
/// completion signal (the `generation complete` log line), knows when that clock should have
/// started and stopped, so it's threaded in here as a parameter rather than derived.
#[derive(Debug, Clone, Copy)]
pub struct Usage {
    pub wall: Duration,
    pub user: Duration,
    pub sys: Duration,
    /// Peak resident set size, in bytes. `wait4`'s `ru_maxrss` is **kibibytes on Linux** -- POSIX
    /// leaves the unit unspecified and BSD historically reports bytes there instead. This harness
    /// only ever runs in this project's Linux dev container / CI image
    /// (docs/adr/containerized-development.md), so the KiB-to-bytes conversion below is
    /// unconditional, not `cfg`-gated on `target_os`.
    pub max_rss_bytes: u64,
    /// The raw wait status `wait4` reported. Decode with `libc::WIFEXITED`/`WEXITSTATUS` (or
    /// [`Usage::exit_code`]) rather than reading it directly.
    pub status: i32,
}

impl Usage {
    /// `Some(code)` if the child exited normally (`WIFEXITED`), `None` if it was killed by a
    /// signal instead -- the harness's own SIGTERM-after-settle path is expected to end in a
    /// clean exit (the same graceful-shutdown cascade `logit ready`'s drain test exercises), not
    /// a signal death, so `None` here is always a failure worth reporting loudly.
    pub fn exit_code(&self) -> Option<i32> {
        libc::WIFEXITED(self.status).then(|| libc::WEXITSTATUS(self.status))
    }

    /// `Some(signal)` if the child was killed by one (`WIFSIGNALED`) -- the complement of
    /// [`Usage::exit_code`]. The caller needs this to tell its *own* SIGTERM apart from any other
    /// signal death: `perf record` (the only wrapper this harness runs `logit` under) forwards
    /// SIGTERM to its workload, waits for it, finalizes `perf.data`, and then dies by that same
    /// signal itself, which is a completed capture rather than a failure.
    pub fn termination_signal(&self) -> Option<i32> {
        libc::WIFSIGNALED(self.status).then(|| libc::WTERMSIG(self.status))
    }
}

/// Blocks until `pid` exits, reaping it and its resource usage in one call. `pid` must name a
/// direct child of this process that hasn't already been reaped (`std::process::Child::wait`
/// would race it).
pub fn wait4(pid: libc::pid_t, wall: Duration) -> io::Result<Usage> {
    let mut status: libc::c_int = 0;
    // `rusage` has no `Default`; every field is a plain integer or `timeval`, all valid as all
    // zero bytes, and `wait4` overwrites every field it defines before returning success.
    let mut rusage: libc::rusage = unsafe { std::mem::zeroed() };

    // Retried on EINTR: a signal delivered to this process (e.g. this harness's own process
    // group receiving Ctrl-C, or any other handler-bearing signal) can interrupt a blocking
    // `wait4` before the child has actually exited -- that's not a real failure, just this
    // syscall's ordinary contract, so it's retried rather than surfaced as an error.
    loop {
        // SAFETY: `pid` names a live child of this process per the caller's contract above, and
        // `&mut status`/`&mut rusage` are valid, correctly-sized, uniquely-owned out-parameters
        // for the duration of this call -- exactly what `wait4(2)` requires. The call blocks this
        // thread until the child exits (no `WNOHANG`); it does not touch any other process's
        // state.
        let ret = unsafe { libc::wait4(pid, &mut status, 0, &mut rusage) };
        if ret >= 0 {
            break;
        }
        let err = io::Error::last_os_error();
        if err.kind() == io::ErrorKind::Interrupted {
            continue;
        }
        return Err(err);
    }

    let user = timeval_to_duration(rusage.ru_utime);
    let sys = timeval_to_duration(rusage.ru_stime);
    // `ru_maxrss` is `c_long` (KiB on Linux); a negative value would mean the kernel reported
    // nonsense, not a real usage this harness should silently swallow, but that value is
    // unreachable in practice on this project's supported platform -- clamp rather than panic.
    let max_rss_kib = rusage.ru_maxrss.max(0) as u64;

    Ok(Usage { wall, user, sys, max_rss_bytes: max_rss_kib * 1024, status })
}

fn timeval_to_duration(tv: libc::timeval) -> Duration {
    Duration::new(tv.tv_sec.max(0) as u64, (tv.tv_usec.clamp(0, 999_999) as u32) * 1000)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timeval_to_duration_converts_microseconds_to_nanoseconds() {
        let tv = libc::timeval { tv_sec: 2, tv_usec: 500_000 };
        assert_eq!(timeval_to_duration(tv), Duration::new(2, 500_000_000));
    }

    #[test]
    fn usage_exit_code_reads_a_normal_exit_status() {
        // Fork-free: build the same `status` encoding `wait4` would report for "exited 0" by
        // reusing `libc::WIFEXITED`/`WEXITSTATUS`'s own encoding, which on Linux is
        // `(code & 0xff) << 8`.
        let status = 0 << 8;
        let usage = Usage {
            wall: Duration::ZERO,
            user: Duration::ZERO,
            sys: Duration::ZERO,
            max_rss_bytes: 0,
            status,
        };
        assert_eq!(usage.exit_code(), Some(0));
    }

    #[test]
    fn usage_exit_code_reads_a_nonzero_exit_status() {
        let status = 3 << 8;
        let usage = Usage {
            wall: Duration::ZERO,
            user: Duration::ZERO,
            sys: Duration::ZERO,
            max_rss_bytes: 0,
            status,
        };
        assert_eq!(usage.exit_code(), Some(3));
    }

    #[test]
    fn usage_exit_code_is_none_for_a_signal_death() {
        // SIGKILL (9), no core dump, no WIFEXITED bit set -- the low 7 bits carry the signal.
        let status = 9;
        let usage = Usage {
            wall: Duration::ZERO,
            user: Duration::ZERO,
            sys: Duration::ZERO,
            max_rss_bytes: 0,
            status,
        };
        assert_eq!(usage.exit_code(), None);
    }

    #[test]
    fn usage_termination_signal_names_the_signal_and_is_none_for_a_normal_exit() {
        let signalled = Usage {
            wall: Duration::ZERO,
            user: Duration::ZERO,
            sys: Duration::ZERO,
            max_rss_bytes: 0,
            status: libc::SIGTERM,
        };
        assert_eq!(signalled.termination_signal(), Some(libc::SIGTERM));

        let exited = Usage { status: 0 << 8, ..signalled };
        assert_eq!(exited.termination_signal(), None);
        assert_eq!(exited.exit_code(), Some(0));
    }
}
