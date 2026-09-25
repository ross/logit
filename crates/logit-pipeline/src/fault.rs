//! A fault-injection seam for the filesystem operations that decide what survives a crash: the
//! disk spool, the tail checkpoint, and `file_out` rotation
//! (`docs/adr/durable-checkpoint-writes-and-fault-injection.md`).
//!
//! Each such operation is preceded by [`check`] (or wrapped in [`fault_io!`]), naming a [`Point`]
//! (a call site and the [`Op`] about to run), a path the caller already holds, and an `arg` (a
//! segment sequence number, or 0).
//!
//! **Release builds pay nothing.** Without the `fault-injection` feature (and outside this
//! crate's own tests), `check` is an `#[inline(always)]` `Ok(())`. With it, a disarmed `check` is
//! one relaxed atomic load and allocates nothing, which is all the allocation pins in
//! `crates/logit-bench/tests/allocations.rs` see when a workspace test build unifies the feature
//! on. `script/lint` fails if `logit-cli`'s normal dependency graph enables the feature.
//!
//! **Test API** (feature on only). [`scope`] arms rules for every path under one directory and
//! disarms them when the returned [`Scope`] drops. Rules live in one global registry, not a
//! thread-local, because tokio runs blocking file operations on its own threads. A unique scratch
//! directory per test keeps concurrent tests in one process apart; `cargo nextest` runs each test
//! in its own process anyway. Matching is lexical ([`Path::starts_with`], component by
//! component): a component under test must be given paths spelled under the directory the test
//! armed.
//!
//! **Crashes freeze rather than panic.** [`Scope::crash_at`] makes the operation at that point
//! fail without running, and every later `check` under the scope fail too, until
//! [`Scope::revive`]. The test then drops the component, revives, reopens, and asserts. Provided
//! every mutating operation goes through `check`, what is on disk is exactly what a `kill -9` at
//! that point would leave, with no panic unwinding through tokio and no poisoned mutex.
//!
//! **Limitation.** `check` runs before an operation starts. A `tokio::fs::File` write already
//! handed to a blocking thread (its `poll_write` returned `Ready` and the future was dropped)
//! still lands after a freeze; the seam can't recall it.

use std::io;
use std::path::Path;

/// The filesystem operation a [`Point`] names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Op {
    Create,
    Open,
    Write,
    Flush,
    SetLen,
    SyncFile,
    SyncDir,
    Rename,
    Unlink,
}

/// One injectable operation: the call site (one of [`sites`]) and the operation it's about to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Point {
    pub site: &'static str,
    pub op: Op,
}

impl Point {
    pub const fn new(site: &'static str, op: Op) -> Self {
        Self { site, op }
    }
}

/// The call sites [`Point::site`] names.
pub mod sites {
    /// A disk spool segment file (`segment-<seq>.lgit`).
    pub const SPOOL_SEGMENT: &str = "spool.segment";
    /// The disk spool's `cursor.json`, written through `atomic_write`.
    pub const SPOOL_CURSOR: &str = "spool.cursor";
    /// The disk spool's directory: its creation, its lock file, and its `fsync`.
    pub const SPOOL_DIR: &str = "spool.dir";
    /// A `tail_in`/`docker_in` checkpoint file.
    pub const TAIL_CHECKPOINT: &str = "tail.checkpoint";
    /// `file_out`'s active file.
    pub const FILE_OUT_ACTIVE: &str = "file_out.active";
    /// `file_out`'s `.rotating` staging file.
    pub const FILE_OUT_STAGING: &str = "file_out.staging";
    /// `file_out`'s retained `.N` files.
    pub const FILE_OUT_RETAINED: &str = "file_out.retained";
}

/// Runs `$op` (an expression of type `io::Result<T>`, which may contain `.await`) unless
/// [`check`] injects a failure at `$point` for `$path`, in which case it returns that error
/// without evaluating `$op`.
#[macro_export]
macro_rules! fault_io {
    ($point:expr, $path:expr, $arg:expr, $op:expr) => {
        match $crate::fault::check($point, $path, $arg) {
            ::std::result::Result::Ok(()) => $op,
            ::std::result::Result::Err(err) => ::std::result::Result::Err(err),
        }
    };
}

/// Returns an injected error if an armed [`Scope`] covering `path` says `point` fails now, and
/// `Ok(())` otherwise. Call it immediately before the operation it names.
#[cfg(not(any(test, feature = "fault-injection")))]
#[inline(always)]
pub fn check(_point: Point, _path: &Path, _arg: u64) -> io::Result<()> {
    Ok(())
}

/// Returns an injected error if an armed [`Scope`] covering `path` says `point` fails now, and
/// `Ok(())` otherwise. Call it immediately before the operation it names.
#[cfg(any(test, feature = "fault-injection"))]
#[inline]
pub fn check(point: Point, path: &Path, arg: u64) -> io::Result<()> {
    if !armed::ARMED.load(std::sync::atomic::Ordering::Relaxed) {
        return Ok(());
    }
    armed::check_armed(point, path, arg)
}

#[cfg(any(test, feature = "fault-injection"))]
pub use armed::{scope, Hit, Scope};

/// The errnos tests inject most. An injected error is `io::Error::from_raw_os_error(errno)`, so
/// code that inspects `raw_os_error()` (the spool's `ENOSPC` check) sees it as real.
#[cfg(any(test, feature = "fault-injection"))]
pub mod errno {
    pub const EIO: i32 = 5;
    pub const EACCES: i32 = 13;
    pub const ENOSPC: i32 = 28;
    pub const EROFS: i32 = 30;
}

#[cfg(any(test, feature = "fault-injection"))]
mod armed {
    use super::{errno, Point};
    use std::io;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Mutex, MutexGuard};

    /// True while any scope has a rule, is recording, or is frozen. The only thing a disarmed
    /// [`super::check`] reads.
    pub(super) static ARMED: AtomicBool = AtomicBool::new(false);

    static REGISTRY: Mutex<Registry> = Mutex::new(Registry { next_id: 0, scopes: Vec::new() });

    /// One operation a recording [`Scope`] let through, in the order `check` saw them.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Hit {
        pub point: Point,
        pub path: PathBuf,
        pub arg: u64,
    }

    enum Action {
        Fail { errno: i32 },
        FailNth { n: u64, errno: i32 },
        CrashAt { n: u64 },
    }

    struct Rule {
        point: Point,
        action: Action,
        seen: u64,
    }

    struct ScopeState {
        id: u64,
        dir: PathBuf,
        rules: Vec<Rule>,
        recording: bool,
        hits: Vec<Hit>,
        crashed: bool,
    }

    impl ScopeState {
        fn is_armed(&self) -> bool {
            !self.rules.is_empty() || self.recording || self.crashed
        }
    }

    struct Registry {
        next_id: u64,
        scopes: Vec<ScopeState>,
    }

    impl Registry {
        fn get(&mut self, id: u64) -> &mut ScopeState {
            self.scopes.iter_mut().find(|s| s.id == id).expect("a live Scope's state is registered")
        }

        fn rearm(&self) {
            ARMED.store(self.scopes.iter().any(ScopeState::is_armed), Ordering::Relaxed);
        }
    }

    /// Tolerates poisoning: a test that panicked while holding the lock left consistent rules.
    fn registry() -> MutexGuard<'static, Registry> {
        REGISTRY.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    #[cold]
    #[inline(never)]
    pub(super) fn check_armed(point: Point, path: &Path, arg: u64) -> io::Result<()> {
        let mut registry = registry();
        for scope in registry.scopes.iter_mut().filter(|s| path.starts_with(&s.dir)) {
            if scope.crashed {
                return Err(io::Error::from_raw_os_error(errno::EIO));
            }
            for rule in scope.rules.iter_mut().filter(|r| r.point == point) {
                rule.seen += 1;
                match rule.action {
                    Action::Fail { errno } => return Err(io::Error::from_raw_os_error(errno)),
                    Action::FailNth { n, errno } if rule.seen == n => {
                        return Err(io::Error::from_raw_os_error(errno));
                    }
                    Action::CrashAt { n } if rule.seen == n => {
                        scope.crashed = true;
                        return Err(io::Error::from_raw_os_error(errno::EIO));
                    }
                    Action::FailNth { .. } | Action::CrashAt { .. } => {}
                }
            }
            if scope.recording {
                scope.hits.push(Hit { point, path: path.to_path_buf(), arg });
            }
        }
        Ok(())
    }

    /// Opens a fault scope over every path under `dir`. It injects nothing until a rule is added,
    /// and removes its rules, recorded hits, and frozen state when dropped.
    pub fn scope(dir: impl Into<PathBuf>) -> Scope {
        let mut registry = registry();
        let id = registry.next_id;
        registry.next_id += 1;
        registry.scopes.push(ScopeState {
            id,
            dir: dir.into(),
            rules: Vec::new(),
            recording: false,
            hits: Vec::new(),
            crashed: false,
        });
        Scope { id }
    }

    /// A drop guard over one directory's fault rules. See the module doc.
    pub struct Scope {
        id: u64,
    }

    impl Scope {
        fn add(&self, point: Point, action: Action) -> &Self {
            let mut registry = registry();
            registry.get(self.id).rules.push(Rule { point, action, seen: 0 });
            registry.rearm();
            self
        }

        /// Fails every `point` under this scope with `errno`.
        pub fn fail(&self, point: Point, errno: i32) -> &Self {
            self.add(point, Action::Fail { errno })
        }

        /// Fails only the `n`th (1-based) `point` under this scope with `errno`.
        pub fn fail_nth(&self, point: Point, n: u64, errno: i32) -> &Self {
            self.add(point, Action::FailNth { n, errno })
        }

        /// Freezes the scope at the `n`th (1-based) `point`: that operation and every later one
        /// under the scope fails with `EIO` without running, until [`Scope::revive`].
        pub fn crash_at(&self, point: Point, n: u64) -> &Self {
            self.add(point, Action::CrashAt { n })
        }

        /// Records every operation this scope lets through, for [`Scope::hits`].
        pub fn record(&self) -> &Self {
            let mut registry = registry();
            registry.get(self.id).recording = true;
            registry.rearm();
            self
        }

        /// The operations let through since [`Scope::record`], in order. An operation a rule
        /// failed, or a freeze stopped, isn't a hit.
        pub fn hits(&self) -> Vec<Hit> {
            registry().get(self.id).hits.clone()
        }

        /// Whether a [`Scope::crash_at`] point has fired and the scope is frozen.
        pub fn crashed(&self) -> bool {
            registry().get(self.id).crashed
        }

        /// Unfreezes the scope. A crash point that already fired doesn't fire again.
        pub fn revive(&self) {
            let mut registry = registry();
            registry.get(self.id).crashed = false;
            registry.rearm();
        }
    }

    impl Drop for Scope {
        fn drop(&mut self) {
            let mut registry = registry();
            registry.scopes.retain(|s| s.id != self.id);
            registry.rearm();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk_queue::test_support::scratch_dir;

    const EVERY_OP: [Op; 9] = [
        Op::Create,
        Op::Open,
        Op::Write,
        Op::Flush,
        Op::SetLen,
        Op::SyncFile,
        Op::SyncDir,
        Op::Rename,
        Op::Unlink,
    ];

    const EVERY_SITE: [&str; 7] = [
        sites::SPOOL_SEGMENT,
        sites::SPOOL_CURSOR,
        sites::SPOOL_DIR,
        sites::TAIL_CHECKPOINT,
        sites::FILE_OUT_ACTIVE,
        sites::FILE_OUT_STAGING,
        sites::FILE_OUT_RETAINED,
    ];

    const POINT: Point = Point::new(sites::SPOOL_SEGMENT, Op::SyncFile);

    fn errno_of(result: io::Result<()>) -> Option<i32> {
        result.err().and_then(|err| err.raw_os_error())
    }

    #[test]
    fn a_disarmed_seam_passes_every_point_through() {
        let dir = scratch_dir("fault-disarmed");
        // A scope with no rules arms nothing either.
        let _scope = scope(&dir);
        for site in EVERY_SITE {
            for op in EVERY_OP {
                assert!(check(Point::new(site, op), &dir.join("file"), 7).is_ok(), "{site} {op:?}");
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_armed_failure_fires_only_under_its_scope_directory() {
        let dir = scratch_dir("fault-scoped");
        let sibling =
            dir.with_file_name(format!("{}-sibling", dir.file_name().unwrap().to_string_lossy()));
        let scope = scope(&dir);
        scope.fail(POINT, errno::ENOSPC);

        assert_eq!(errno_of(check(POINT, &dir.join("segment"), 0)), Some(errno::ENOSPC));
        assert_eq!(errno_of(check(POINT, &dir, 0)), Some(errno::ENOSPC), "the directory itself");
        // A string prefix that isn't a path prefix is outside the scope.
        assert!(check(POINT, &sibling.join("segment"), 0).is_ok());
        assert!(check(Point::new(sites::SPOOL_SEGMENT, Op::Write), &dir.join("segment"), 0).is_ok());
        assert!(check(Point::new(sites::SPOOL_CURSOR, Op::SyncFile), &dir.join("c"), 0).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn fail_nth_fires_once_on_the_nth_hit() {
        let dir = scratch_dir("fault-nth");
        let scope = scope(&dir);
        scope.fail_nth(POINT, 3, errno::EIO);

        let path = dir.join("segment");
        let results: Vec<_> = (0..5).map(|_| errno_of(check(POINT, &path, 0))).collect();
        assert_eq!(results, vec![None, None, Some(errno::EIO), None, None]);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_crash_freezes_every_later_operation_under_the_scope_until_revived() {
        let dir = scratch_dir("fault-crash");
        let outside = scratch_dir("fault-crash-outside");
        let scope = scope(&dir);
        scope.crash_at(POINT, 2).record();

        let path = dir.join("segment");
        assert!(check(POINT, &path, 0).is_ok());
        assert!(!scope.crashed());
        assert!(check(POINT, &path, 0).is_err(), "the crash point itself fails");
        assert!(scope.crashed());
        for op in EVERY_OP {
            let point = Point::new(sites::SPOOL_CURSOR, op);
            assert!(check(point, &dir.join("cursor.json"), 0).is_err(), "{op:?} while frozen");
            assert!(check(point, &outside.join("cursor.json"), 0).is_ok(), "{op:?} outside");
        }
        assert_eq!(scope.hits().len(), 1, "only the operation before the crash ran");

        scope.revive();
        assert!(!scope.crashed());
        assert!(check(POINT, &path, 0).is_ok(), "a fired crash point doesn't fire again");
        std::fs::remove_dir_all(&dir).ok();
        std::fs::remove_dir_all(&outside).ok();
    }

    #[test]
    fn dropping_a_scope_disarms_it() {
        let dir = scratch_dir("fault-drop");
        let path = dir.join("segment");
        {
            let scope = scope(&dir);
            scope.crash_at(POINT, 1).fail(Point::new(sites::SPOOL_DIR, Op::SyncDir), errno::EROFS);
            assert!(check(POINT, &path, 0).is_err());
        }
        assert!(check(POINT, &path, 0).is_ok());
        assert!(check(Point::new(sites::SPOOL_DIR, Op::SyncDir), &dir, 0).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }
}
