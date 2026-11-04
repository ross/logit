//! The soft open-files limit raise `logit run` makes at startup: `RLIMIT_NOFILE`'s soft limit goes
//! up to its hard limit (`docs/adr/open-files-limit-raise.md`).
//!
//! Every stream listener defaults to `max_connections: 1024`, the same number as a stock systemd
//! service's soft limit, so the cap would not bind before descriptors ran out. Linux only; the
//! startup warning in `pipeline.rs` reads the same limit from `/proc/self/limits` after this runs.

/// What the raise did, for the caller to log once its self-log layer is active.
#[derive(Debug, PartialEq)]
pub enum Raise {
    /// No raise was needed, or none was possible and nothing was wrong enough to report.
    Unchanged,
    /// The soft limit went from `from` to `to`.
    Raised { from: u64, to: u64 },
    /// The raise failed; the soft limit is as it was.
    Failed(String),
}

/// The soft limit to set, or `None` to leave it. `None` in a limit is `RLIM_INFINITY`.
///
/// An infinite hard limit is capped at `nr_open`, the kernel's per-process descriptor ceiling,
/// because `setrlimit` fails with `EPERM` for a soft limit above it.
fn target_soft(soft: Option<u64>, hard: Option<u64>, nr_open: Option<u64>) -> Option<u64> {
    let current = soft?;
    let target = hard.or(nr_open)?;
    (current < target).then_some(target)
}

/// Raises the soft `RLIMIT_NOFILE` to the hard limit, never failing the caller.
#[cfg(target_os = "linux")]
pub fn raise_soft_limit() -> Raise {
    use rustix::process::{getrlimit, setrlimit, Resource, Rlimit};

    let Rlimit { current: soft, maximum: hard } = getrlimit(Resource::Nofile);
    let nr_open = std::fs::read_to_string("/proc/sys/fs/nr_open")
        .ok()
        .and_then(|text| text.trim().parse::<u64>().ok());
    if hard.is_none() && nr_open.is_none() {
        return Raise::Failed(
            "the hard open-files limit is unlimited and /proc/sys/fs/nr_open is unreadable, so \
             there is no ceiling to raise the soft limit to"
                .into(),
        );
    }
    let (Some(from), Some(to)) = (soft, target_soft(soft, hard, nr_open)) else {
        return Raise::Unchanged;
    };
    match setrlimit(Resource::Nofile, Rlimit { current: Some(to), maximum: hard }) {
        Ok(()) => Raise::Raised { from, to },
        Err(err) => Raise::Failed(format!("setrlimit(RLIMIT_NOFILE) to {to} failed: {err}")),
    }
}

/// Non-Linux twin of [`raise_soft_limit`].
#[cfg(not(target_os = "linux"))]
pub fn raise_soft_limit() -> Raise {
    Raise::Unchanged
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn target_soft_raises_to_the_hard_limit() {
        assert_eq!(target_soft(Some(1024), Some(524_288), Some(1_048_576)), Some(524_288));
        assert_eq!(target_soft(Some(1024), Some(524_288), None), Some(524_288));
    }

    #[test]
    fn target_soft_leaves_a_soft_limit_already_at_the_hard_limit() {
        assert_eq!(target_soft(Some(4096), Some(4096), Some(1_048_576)), None);
    }

    #[test]
    fn target_soft_caps_an_unlimited_hard_limit_at_nr_open() {
        assert_eq!(target_soft(Some(1024), None, Some(1_048_576)), Some(1_048_576));
        assert_eq!(target_soft(Some(1_048_576), None, Some(1_048_576)), None);
    }

    #[test]
    fn target_soft_leaves_the_limit_when_nr_open_is_unreadable() {
        assert_eq!(target_soft(Some(1024), None, None), None);
    }

    #[test]
    fn target_soft_leaves_an_unlimited_soft_limit() {
        assert_eq!(target_soft(None, None, Some(1_048_576)), None);
    }

    /// Calls the real `setrlimit`. `RLIMIT_NOFILE` is process-wide and the test lowers it first, so
    /// it runs only under nextest's process-per-test mode (as `logit-inputs`' `tcp.rs` EMFILE test
    /// does) and returns early in libtest's shared process.
    #[cfg(target_os = "linux")]
    #[test]
    fn raise_soft_limit_sets_the_soft_limit_to_the_hard_limit() {
        use rustix::process::{getrlimit, setrlimit, Resource, Rlimit};

        if std::env::var("NEXTEST_EXECUTION_MODE").as_deref() != Ok("process-per-test") {
            eprintln!("skipped: lowers RLIMIT_NOFILE, so it needs nextest's process-per-test mode");
            return;
        }
        let Rlimit { maximum, .. } = getrlimit(Resource::Nofile);
        let lowered = 512;
        if maximum.is_some_and(|hard| hard <= lowered) {
            eprintln!("skipped: the hard limit {maximum:?} leaves nothing to raise");
            return;
        }
        setrlimit(Resource::Nofile, Rlimit { current: Some(lowered), maximum }).unwrap();

        let outcome = raise_soft_limit();

        let now = getrlimit(Resource::Nofile);
        match outcome {
            Raise::Raised { from, to } => {
                assert_eq!(from, lowered);
                assert_eq!(now.current, Some(to));
                assert!(to > lowered);
                if let Some(hard) = maximum {
                    assert_eq!(to, hard, "a finite hard limit is the target");
                }
            }
            other => panic!("expected a raise from {lowered}, got {other:?}"),
        }
        assert_eq!(now.maximum, maximum, "the hard limit is untouched");
        assert_eq!(raise_soft_limit(), Raise::Unchanged, "a second call has nothing to raise");
    }
}
