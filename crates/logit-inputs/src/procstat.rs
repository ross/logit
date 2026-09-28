//! Whole-process facts read from `/proc/self`, for `internal`'s `logit.process.*` metrics
//! (`crate::internal::ProcessSampler`, `docs/adr/process-level-metrics.md`).
//!
//! **Read as bytes, never as UTF-8.** `/proc/self/stat`'s second field, `comm`, is up to 15
//! arbitrary non-NUL bytes in parentheses: it can hold spaces, `)`, or invalid UTF-8, so
//! `read_to_string` can fail and a whitespace split can shift every later field. [`parse_stat`]
//! splits at the last `)`; the tokens after it start at field 3 (`state`), so `utime` (field 14) is
//! token 11 and `stime` (field 15) token 12, both in [`USER_HZ`] ticks. `/proc/self` resolves to
//! the thread-group id, so these are totals over every thread.
//!
//! **`/proc/self/fd` lists one descriptor too many.** Reading the directory holds an fd open on it,
//! and that fd appears in its own listing; [`open_fds`] subtracts it.
//!
//! **A soft limit of `unlimited`** is [`open_files_limit`]'s `Ok(None)`, not an error.
//!
//! **`VmRSS` can lag** the true resident size by a little: the kernel batches per-thread RSS
//! counters before folding them into the process total.
//!
//! **Linux-only, reported through [`Unavailable`].** Each reader has a non-Linux twin with the same
//! signature that returns [`Unavailable::NotLinux`], so a caller needs no `cfg`.

// Off Linux, only the tests call the parsers.
#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::fmt;
use std::io;

/// Linux's `USER_HZ`, the unit of `/proc/self/stat`'s `utime`/`stime`.
///
/// A constant rather than a `sysconf(_SC_CLK_TCK)` call, which would be `unsafe` `libc`: the kernel
/// fixes `USER_HZ` at 100 in `include/asm-generic/param.h`, and glibc's `sysconf(_SC_CLK_TCK)`
/// returns the kernel-supplied `AT_CLKTCK`, which is that value on every architecture `logit`
/// builds for. A Linux test pins it against `sysconf`.
pub(crate) const USER_HZ: u64 = 100;

/// Why a `/proc/self` read reported nothing. A caller stops asking after the first one: none of
/// these causes clears up later in the same process.
#[derive(Debug)]
pub(crate) enum Unavailable {
    /// Not a Linux build. The non-Linux twins' only answer.
    #[cfg_attr(
        target_os = "linux",
        allow(dead_code, reason = "built only by the non-Linux twins")
    )]
    NotLinux,
    /// The read failed: a sandbox hiding the file, or a missing procfs mount.
    Io(io::Error),
    /// The file read, but not in the shape this module parses. Names what was missing.
    Malformed(&'static str),
}

impl fmt::Display for Unavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotLinux => f.write_str("procfs is Linux-only"),
            Self::Io(err) => write!(f, "reading /proc/self failed: {err}"),
            Self::Malformed(what) => write!(f, "unexpected /proc/self contents: {what}"),
        }
    }
}

/// The two `/proc/self/status` lines this module reads.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Status {
    /// `VmRSS:`, converted from kB to bytes.
    pub(crate) resident_bytes: u64,
    /// `Threads:`.
    pub(crate) threads: u64,
}

/// Cumulative CPU time since process start, in [`USER_HZ`] ticks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct CpuTicks {
    /// `utime`, field 14.
    pub(crate) user: u64,
    /// `stime`, field 15.
    pub(crate) system: u64,
}

/// Resident size and thread count, from `/proc/self/status`.
#[cfg(target_os = "linux")]
pub(crate) fn status() -> Result<Status, Unavailable> {
    parse_status(&std::fs::read("/proc/self/status").map_err(Unavailable::Io)?)
}

/// Non-Linux twin of [`status`].
#[cfg(not(target_os = "linux"))]
pub(crate) fn status() -> Result<Status, Unavailable> {
    Err(Unavailable::NotLinux)
}

/// User and system CPU ticks, from `/proc/self/stat`.
#[cfg(target_os = "linux")]
pub(crate) fn cpu_ticks() -> Result<CpuTicks, Unavailable> {
    parse_stat(&std::fs::read("/proc/self/stat").map_err(Unavailable::Io)?)
}

/// Non-Linux twin of [`cpu_ticks`].
#[cfg(not(target_os = "linux"))]
pub(crate) fn cpu_ticks() -> Result<CpuTicks, Unavailable> {
    Err(Unavailable::NotLinux)
}

/// Open descriptors, counted from `/proc/self/fd` less the one the listing holds.
#[cfg(target_os = "linux")]
pub(crate) fn open_fds() -> Result<u64, Unavailable> {
    let mut entries = 0u64;
    for entry in std::fs::read_dir("/proc/self/fd").map_err(Unavailable::Io)? {
        entry.map_err(Unavailable::Io)?;
        entries += 1;
    }
    Ok(entries.saturating_sub(1))
}

/// Non-Linux twin of [`open_fds`].
#[cfg(not(target_os = "linux"))]
pub(crate) fn open_fds() -> Result<u64, Unavailable> {
    Err(Unavailable::NotLinux)
}

/// The soft `Max open files` limit from `/proc/self/limits`; `None` when it's `unlimited`.
#[cfg(target_os = "linux")]
pub(crate) fn open_files_limit() -> Result<Option<u64>, Unavailable> {
    parse_limits(&std::fs::read("/proc/self/limits").map_err(Unavailable::Io)?)
}

/// Non-Linux twin of [`open_files_limit`].
#[cfg(not(target_os = "linux"))]
pub(crate) fn open_files_limit() -> Result<Option<u64>, Unavailable> {
    Err(Unavailable::NotLinux)
}

/// Parses `/proc/self/status`: `VmRSS:` (kB) and `Threads:`. A kernel thread has no `VmRSS:` line,
/// so its absence is `Malformed` rather than zero.
pub(crate) fn parse_status(bytes: &[u8]) -> Result<Status, Unavailable> {
    let mut resident_kb = None;
    let mut threads = None;
    for line in bytes.split(|&b| b == b'\n') {
        if let Some(rest) = line.strip_prefix(b"VmRSS:") {
            // `VmRSS:	    1392 kB`: the kernel always prints kB here.
            resident_kb = first_number(rest);
        } else if let Some(rest) = line.strip_prefix(b"Threads:") {
            threads = first_number(rest);
        }
    }
    Ok(Status {
        resident_bytes: resident_kb
            .ok_or(Unavailable::Malformed("no VmRSS line"))?
            .saturating_mul(1024),
        threads: threads.ok_or(Unavailable::Malformed("no Threads line"))?,
    })
}

/// Parses `/proc/self/stat` into `utime`/`stime`, splitting at the last `)` so `comm` can't shift
/// the fields.
pub(crate) fn parse_stat(bytes: &[u8]) -> Result<CpuTicks, Unavailable> {
    let close = bytes
        .iter()
        .rposition(|&b| b == b')')
        .ok_or(Unavailable::Malformed("no ')' closing comm in stat"))?;
    let mut fields = bytes[close + 1..].split(u8::is_ascii_whitespace).filter(|f| !f.is_empty());
    let user = fields.nth(11).and_then(parse_u64);
    let system = fields.next().and_then(parse_u64);
    match (user, system) {
        (Some(user), Some(system)) => Ok(CpuTicks { user, system }),
        _ => Err(Unavailable::Malformed("stat has no numeric utime/stime")),
    }
}

/// Parses `/proc/self/limits`' `Max open files` row. The kernel prints each row as
/// `"%-25s %-20s %-20s %-10s"`, so the soft limit is the first token after the 25-byte name.
pub(crate) fn parse_limits(bytes: &[u8]) -> Result<Option<u64>, Unavailable> {
    const NAME: &[u8] = b"Max open files";
    let row = bytes
        .split(|&b| b == b'\n')
        .find(|line| line.starts_with(NAME))
        .ok_or(Unavailable::Malformed("no Max open files row in limits"))?;
    let soft = row
        .get(25..)
        .and_then(|rest| rest.split(u8::is_ascii_whitespace).find(|f| !f.is_empty()))
        .ok_or(Unavailable::Malformed("Max open files row has no soft limit"))?;
    if soft == b"unlimited" {
        return Ok(None);
    }
    parse_u64(soft)
        .map(Some)
        .ok_or(Unavailable::Malformed("Max open files soft limit is not a number"))
}

fn first_number(rest: &[u8]) -> Option<u64> {
    rest.split(u8::is_ascii_whitespace).find(|f| !f.is_empty()).and_then(parse_u64)
}

fn parse_u64(field: &[u8]) -> Option<u64> {
    std::str::from_utf8(field).ok()?.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A long-running `sh`'s `/proc/<pid>/stat` from the dev container: `utime` 58, `stime` 1.
    const STAT: &[u8] = b"1 (sh) S 0 1 1 0 -1 4194560 1093 0 0 0 58 1 0 0 20 0 1 0 7608957 2654208 382 18446744073709551615 94461341679616 94461341756345 140735530456080 0 0 0 0 0 65538 1 0 0 17 3 0 0 0 0 0 94461341785648 94461341790784 94461747449856 140735530458789 140735530458914 140735530458914 140735530459116 0\n";

    /// `cat /proc/self/status` from the dev container, truncated after the lines parsed.
    const STATUS: &[u8] = b"Name:\tcat\nUmask:\t0022\nState:\tR (running)\nTgid:\t8\nNgid:\t0\nPid:\t8\nPPid:\t1\nTracerPid:\t0\nUid:\t1000\t1000\t1000\t1000\nGid:\t1000\t1000\t1000\t1000\nFDSize:\t64\nGroups:\t1000 \nNStgid:\t8\nNSpid:\t8\nNSpgid:\t1\nNSsid:\t1\nKthread:\t0\nVmPeak:\t    2640 kB\nVmSize:\t    2640 kB\nVmLck:\t       0 kB\nVmPin:\t       0 kB\nVmHWM:\t    1392 kB\nVmRSS:\t    1392 kB\nRssAnon:\t     100 kB\nRssFile:\t    1292 kB\nRssShmem:\t       0 kB\nVmData:\t     360 kB\nVmStk:\t     132 kB\nVmExe:\t      20 kB\nVmLib:\t    1528 kB\nVmPTE:\t      48 kB\nVmSwap:\t       0 kB\nHugetlbPages:\t       0 kB\nCoreDumping:\t0\nTHP_enabled:\t1\nuntag_mask:\t0xffffffffffffffff\nThreads:\t1\nSigQ:\t1/253927\n";

    /// `cat /proc/self/limits` from the dev container.
    const LIMITS: &str = "\
Limit                     Soft Limit           Hard Limit           Units
Max cpu time              unlimited            unlimited            seconds
Max file size             unlimited            unlimited            bytes
Max data size             unlimited            unlimited            bytes
Max stack size            8388608              unlimited            bytes
Max core file size        unlimited            unlimited            bytes
Max resident set          unlimited            unlimited            bytes
Max processes             unlimited            unlimited            processes
Max open files            1024                 524288               files
Max locked memory         8388608              8388608              bytes
Max address space         unlimited            unlimited            bytes
Max file locks            unlimited            unlimited            locks
Max pending signals       253927               253927               signals
Max msgqueue size         819200               819200               bytes
Max nice priority         0                    0
Max realtime priority     0                    0
Max realtime timeout      unlimited            unlimited            us
";

    #[test]
    fn stat_yields_utime_and_stime() {
        assert_eq!(parse_stat(STAT).unwrap(), CpuTicks { user: 58, system: 1 });
    }

    #[test]
    fn a_comm_holding_parens_spaces_and_non_utf8_does_not_shift_the_fields() {
        let mut stat = b"1 (".to_vec();
        stat.extend_from_slice(b") (\xff x");
        stat.extend_from_slice(&STAT[b"1 (sh".len()..]);
        assert!(std::str::from_utf8(&stat).is_err(), "the fixture must hold invalid UTF-8");
        assert_eq!(parse_stat(&stat).unwrap(), CpuTicks { user: 58, system: 1 });
    }

    #[test]
    fn a_truncated_stat_is_malformed() {
        let truncated = &STAT[..b"1 (sh) S 0 1 1 0 -1 4194560 1093 0 0 0 58".len()];
        assert!(matches!(parse_stat(truncated), Err(Unavailable::Malformed(_))));
        assert!(matches!(parse_stat(b"no parens at all"), Err(Unavailable::Malformed(_))));
    }

    #[test]
    fn status_yields_resident_bytes_and_threads() {
        assert_eq!(
            parse_status(STATUS).unwrap(),
            Status { resident_bytes: 1392 * 1024, threads: 1 }
        );
    }

    #[test]
    fn status_without_vmrss_is_malformed() {
        let status = std::str::from_utf8(STATUS).expect("the fixture is UTF-8");
        let without = status.replace("VmRSS:\t    1392 kB\n", "");
        assert_ne!(without, status, "the replacement must match the fixture");
        assert!(matches!(parse_status(without.as_bytes()), Err(Unavailable::Malformed(_))));
    }

    #[test]
    fn limits_yields_the_soft_open_files_limit() {
        assert_eq!(parse_limits(LIMITS.as_bytes()).unwrap(), Some(1024));
    }

    #[test]
    fn an_unlimited_open_files_limit_is_none() {
        let unlimited =
            LIMITS.replace("Max open files            1024", "Max open files            unlimited");
        assert_ne!(unlimited, LIMITS, "the replacement must match the fixture");
        assert_eq!(parse_limits(unlimited.as_bytes()).unwrap(), None);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn user_hz_matches_sysconf() {
        // SAFETY: sysconf takes an integer and touches no memory.
        let clk_tck = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        assert_eq!(clk_tck, USER_HZ as libc::c_long);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn live_reads_are_plausible() {
        let status = status().expect("status");
        assert!(status.resident_bytes > 0);
        assert!(status.threads >= 1);
        cpu_ticks().expect("stat");
        assert!(open_fds().expect("fd") >= 3, "stdin, stdout, and stderr at least");
        open_files_limit().expect("limits");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn opening_a_file_raises_open_fds() {
        let before = open_fds().expect("fd");
        let file = std::fs::File::open("/proc/self/status").expect("open");
        let during = open_fds().expect("fd");
        drop(file);
        // The count is process-wide; under a shared-process runner another test's descriptors move it.
        assert!(during > before, "opening a file must raise the count: {before} -> {during}");
    }
}
