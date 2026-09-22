//! The wake source a [`crate::tail::driver::Tailer`] races against its own poll tick.
//! [`Watcher::Poll`] never resolves on its own ([`Watcher::next_wake`] is `std::future::pending`)
//! -- the driver's own `poll_interval` tick is its only wake source, and racing a future that
//! never completes against it costs nothing. On Linux, [`Watcher::Inotify`] wraps a real
//! `inotify` file descriptor (`inotify::InotifyWatcher`, below) for near-immediate wakeups;
//! `poll_interval` still runs underneath it as reconciliation (a missed rename, an
//! `IN_Q_OVERFLOW`, anything `inotify` didn't report). See
//! `docs/adr/file-tailing-and-docker-json-logs.md` and
//! `docs/adr/docker-container-identity-and-minimal-watches.md`.
//!
//! Two kinds of watch, deliberately kept apart rather than sharing one mask: a directory watch
//! ([`Watcher::watch_dir`]) only ever needs to know something *appeared or departed* underneath
//! it (`DIR_MASK`) -- `docker_in` watches exactly one of these, `root` itself, since a container's
//! own state directory is a direct child of it. A file watch ([`Watcher::watch_file`]) only ever
//! needs to know its own content changed (`FILE_MASK`, just `IN_MODIFY`) -- one per file a
//! [`crate::tail::driver::Tailer`] actually has open. Nothing is watched beyond those two kinds:
//! a container this listener isn't tailing gets no watch at all, and a write to a tailed file
//! never triggers the directory-level rescan a `Wake::Discover` does.

use super::WatchMode;
use logit_core::Diagnostics;
use std::path::{Path, PathBuf};

/// Something changed. `Discover` names a directory watch's own wake -- a path appeared, departed,
/// or (nameless) the directory itself changed -- and is what triggers a full `scan`. `Data` names
/// a file watch's own wake -- that exact tracked file was written to -- and triggers nothing more
/// than draining that one file; see `crate::tail::driver::Tailer::run_until_shutdown`'s `select!`
/// match. `Overflow` means the kernel's `inotify` event queue overflowed and some events were
/// lost -- unreachable under [`Watcher::Poll`], which has no queue to overflow; the driver
/// responds to it with a full `scan` rather than trying to reconstruct which specific paths were
/// missed. `Dead` is the wake source itself giving up: the fd is unusable and this watcher will
/// never wake again, so the driver diagnoses it once (`watch_error`) and carries on with its own
/// `poll_interval` tick alone -- emitted at most once per watcher, after which
/// [`Watcher::next_wake`] parks forever rather than spinning on a fd that cannot recover.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Wake {
    Discover(PathBuf),
    Data(PathBuf),
    Overflow,
    Dead(String),
}

/// Identifies one file watch for a later [`Watcher::unwatch`] call. Opaque outside this module --
/// `crate::tail::driver::TrackedFile` just holds one and hands it back, never inspects it. `Copy`
/// so a `Tailer` can hold it in a plain field alongside the file it names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WatchId(i32);

#[derive(Debug)]
pub(crate) enum Watcher {
    Poll,
    #[cfg(target_os = "linux")]
    Inotify(inotify::InotifyWatcher),
}

impl Watcher {
    /// `Poll` always succeeds. `Inotify` fails startup outright on setup failure (including on a
    /// non-Linux build) -- an operator who explicitly asked for the low-latency path should know
    /// immediately if it isn't available, not silently get polling instead. `Auto` tries
    /// `inotify` first and falls back to `Poll` (diagnosed `watch_error`) on any setup failure,
    /// since it's the "best available" mode by definition.
    pub fn new(mode: WatchMode, diag: &mut Diagnostics) -> anyhow::Result<Self> {
        Self::new_inner(mode, diag, make_inotify)
    }

    /// Test-only seam: same as [`Watcher::new`], but with the `inotify` constructor injected --
    /// lets a test force `Auto`'s fallback path without needing to actually exhaust a real
    /// `fs.inotify.max_user_instances` limit.
    #[cfg(test)]
    fn new_with(
        mode: WatchMode,
        diag: &mut Diagnostics,
        make_inotify: impl FnOnce() -> anyhow::Result<PlatformInotify>,
    ) -> anyhow::Result<Self> {
        Self::new_inner(mode, diag, make_inotify)
    }

    fn new_inner(
        mode: WatchMode,
        diag: &mut Diagnostics,
        make_inotify: impl FnOnce() -> anyhow::Result<PlatformInotify>,
    ) -> anyhow::Result<Self> {
        match mode {
            WatchMode::Poll => Ok(Watcher::Poll),
            WatchMode::Inotify => wrap_inotify(make_inotify()?),
            WatchMode::Auto => match make_inotify() {
                Ok(w) => wrap_inotify(w),
                Err(err) => {
                    diag.warn_throttled(
                        "watch_error",
                        format!("inotify setup failed, falling back to polling: {err}"),
                    );
                    Ok(Watcher::Poll)
                }
            },
        }
    }

    pub fn watch_dir(&mut self, dir: &Path) -> std::io::Result<()> {
        match self {
            Watcher::Poll => Ok(()),
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.watch_dir(dir),
        }
    }

    /// Called by `Tailer::reconcile_watches` for a directory that was armed on an earlier scan and
    /// is no longer reached by any pattern's `dir()`.
    ///
    /// **Unreachable today, deliberately kept.** `Tailer::patterns` is assigned once in
    /// `Tailer::new` and never mutated, so the set `reconcile_watches` computes is the same on
    /// every scan and its removal loop always iterates an empty difference. It stays because
    /// `reconcile_watches` is only *correct* with it -- a pattern set that ever becomes mutable
    /// (a reloadable config, a `docker_in` that widens its watch set again) would otherwise leak
    /// a kernel watch per dropped directory -- and because it is what `watch_dir`'s own stale-`wd`
    /// replacement is the mirror of. Covered by `unwatch_dir_releases_both_indexes_and_allows_a_
    /// rearm` below rather than by any production call path.
    pub fn unwatch_dir(&mut self, dir: &Path) {
        match self {
            Watcher::Poll => {}
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.unwatch_dir(dir),
        }
    }

    /// Watches one file's own content changes. `Ok(None)` under [`Watcher::Poll`] (nothing to
    /// watch with); an `Err` is non-fatal to the caller -- the file is still tailed, just without
    /// a low-latency data wake, falling fully back to `poll_interval` for it exactly as
    /// `watch: poll` always does -- but it is *not* swallowed here: `Tailer::open_tracked`
    /// diagnoses it (`watch_error`, via [`watch_error_message`]), because the difference between
    /// "this file wakes promptly" and "this file waits for the poll tick" is otherwise invisible
    /// to an operator, and the most likely cause at scale (`ENOSPC` --
    /// `fs.inotify.max_user_watches`) is a host setting only a diagnostic would ever point at.
    /// Called once, when `Tailer::open_tracked` opens the file.
    pub fn watch_file(&mut self, path: &Path) -> std::io::Result<Option<WatchId>> {
        match self {
            Watcher::Poll => Ok(None),
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.watch_file(path).map(Some),
        }
    }

    /// Releases a file watch [`Watcher::watch_file`] returned -- called once, when the file it
    /// names stops being tracked (closed, rotated away, de-selected), regardless of reason.
    pub fn unwatch(&mut self, id: WatchId) {
        match self {
            Watcher::Poll => {}
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.unwatch(id),
        }
    }

    pub async fn next_wake(&mut self) -> Wake {
        match self {
            Watcher::Poll => std::future::pending().await,
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.next_wake().await,
        }
    }

    /// Test-only: asserting `Auto`'s fallback behavior, and that a successful `inotify` setup
    /// actually took the `Inotify` branch rather than silently landing on `Poll` regardless.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_inotify(&self) -> bool {
        match self {
            Watcher::Poll => false,
            #[cfg(target_os = "linux")]
            Watcher::Inotify(_) => true,
        }
    }

    /// Test-only: the raw `inotify` fd, so a leak test can cross-check this watcher's own
    /// bookkeeping against the kernel's (`/proc/self/fdinfo/<fd>` carries one `inotify wd:` line
    /// per *live* watch -- the external count `logit.input.watch.watches` deliberately isn't,
    /// see `Tailer::scan`). `None` under [`Watcher::Poll`], which has no fd.
    #[cfg(all(test, target_os = "linux"))]
    pub fn inotify_fd(&self) -> Option<std::os::fd::RawFd> {
        match self {
            Watcher::Poll => None,
            Watcher::Inotify(w) => Some(w.raw_fd()),
        }
    }

    /// Test-only companion to [`Watcher::inotify_fd`]: how many watch descriptors this watcher
    /// believes it holds.
    #[cfg(all(test, target_os = "linux"))]
    pub fn tracked_watch_count(&self) -> usize {
        match self {
            Watcher::Poll => 0,
            Watcher::Inotify(w) => w.tracked_watch_count(),
        }
    }
}

/// One operator-facing line for a failed `inotify_add_watch`, shared by the directory
/// (`Tailer::reconcile_watches`) and file (`Tailer::open_tracked`) call sites so both read the
/// same way under the one `watch_error` diagnostic key.
///
/// `ENOSPC` gets a pointer at the host setting behind it, because the errno alone ("No space left
/// on device") reads as a full disk and is not: `inotify_new_watch()` returns it from
/// `if (!inc_inotify_watches(group->inotify_data.ucounts))` when the per-user watch limit is
/// reached (`inotify_add_watch(2)` ERRORS: "The user limit on the total number of inotify watches
/// was reached"). The limit is no longer the flat 8192 of folklore -- since Linux 5.11 (commit
/// `92890123749b`, "inotify: Increase default inotify.max_user_watches limit to 1048576")
/// `inotify_user_setup()` sizes it from lowmem, `watches_max = clamp(watches_max, 8192UL,
/// 1048576UL)` over roughly 1% of `si.totalram - si.totalhigh` -- so the number to look at is the
/// running host's own `/proc/sys/fs/inotify/max_user_watches`, not a remembered constant.
pub(crate) fn watch_error_message(path: &Path, err: &std::io::Error) -> String {
    let base = format!("{}: {err}", path.display());
    #[cfg(target_os = "linux")]
    if err.raw_os_error() == Some(libc::ENOSPC) {
        return format!(
            "{base} -- the per-user inotify watch limit is exhausted; raise \
             fs.inotify.max_user_watches (its default is memory-derived since Linux 5.11, as low \
             as 8192)"
        );
    }
    base
}

// The platform-specific `inotify` constructor type `Watcher::new`/`new_with` are generic over --
// `InotifyWatcher` itself on Linux, and an uninhabited stand-in everywhere else (never
// constructed, since `make_inotify` on a non-Linux build always returns `Err` before producing
// one -- see `make_inotify` below).
#[cfg(target_os = "linux")]
type PlatformInotify = inotify::InotifyWatcher;
#[cfg(not(target_os = "linux"))]
type PlatformInotify = std::convert::Infallible;

#[cfg(target_os = "linux")]
fn make_inotify() -> anyhow::Result<PlatformInotify> {
    inotify::InotifyWatcher::new()
}

#[cfg(not(target_os = "linux"))]
fn make_inotify() -> anyhow::Result<PlatformInotify> {
    anyhow::bail!("watch: inotify is only available on Linux")
}

#[cfg(target_os = "linux")]
fn wrap_inotify(w: PlatformInotify) -> anyhow::Result<Watcher> {
    Ok(Watcher::Inotify(w))
}

#[cfg(not(target_os = "linux"))]
fn wrap_inotify(w: PlatformInotify) -> anyhow::Result<Watcher> {
    match w {} // `PlatformInotify` is uninhabited here -- nothing ever reaches this call
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn poll_mode_always_succeeds_and_never_reports_inotify() {
        let mut diag = Diagnostics::new("test");
        let watcher = Watcher::new_with(WatchMode::Poll, &mut diag, || {
            panic!("poll mode should never construct an inotify backend")
        })
        .expect("poll mode should always succeed");
        assert!(!watcher.is_inotify());
    }

    // `AsyncFd::new` (inside `InotifyWatcher::new`) needs a live reactor.
    #[tokio::test]
    async fn auto_uses_inotify_when_setup_succeeds() {
        let mut diag = Diagnostics::new("test");
        let watcher = Watcher::new_with(WatchMode::Auto, &mut diag, || {
            Ok(inotify::InotifyWatcher::new().expect("real inotify should be available here"))
        })
        .expect("auto should succeed when inotify setup succeeds");
        assert!(watcher.is_inotify());
    }

    #[test]
    fn auto_falls_back_to_poll_when_inotify_setup_fails() {
        let mut diag = Diagnostics::new("test");
        let watcher = Watcher::new_with(WatchMode::Auto, &mut diag, || {
            anyhow::bail!("simulated inotify_init1 failure")
        })
        .expect("auto should still succeed by falling back to poll");
        assert!(!watcher.is_inotify(), "auto should have fallen back to Poll");
    }

    /// A failed file watch reaches the caller as an `Err`, not as a `None` indistinguishable from
    /// `watch: poll`'s "there was never anything to watch with" -- that difference is the whole
    /// reason `Tailer::open_tracked` can diagnose it. `ENOENT` is the deterministic trigger here;
    /// the one that actually bites in production is `ENOSPC` against `fs.inotify.
    /// max_user_watches`, which no test can provoke without changing a host-wide sysctl.
    #[tokio::test]
    async fn watch_file_surfaces_its_error_rather_than_swallowing_it() {
        let mut diag = Diagnostics::new("test");
        let mut watcher = Watcher::new(WatchMode::Inotify, &mut diag)
            .expect("inotify should be available in the dev container");
        let err = watcher
            .watch_file(Path::new("/nonexistent/logit-test/app.log"))
            .expect_err("a missing file must not watch successfully");
        assert_eq!(err.raw_os_error(), Some(libc::ENOENT), "got: {err}");

        // And the operator-facing line names the path plus the errno.
        let message = watch_error_message(Path::new("/nonexistent/logit-test/app.log"), &err);
        assert!(message.contains("/nonexistent/logit-test/app.log"), "got: {message}");
        assert!(message.contains("No such file"), "got: {message}");

        // `Poll` has nothing to watch with, which is not an error.
        let mut poll = Watcher::Poll;
        assert!(poll.watch_file(Path::new("/nonexistent/logit-test/app.log")).unwrap().is_none());
    }

    /// `ENOSPC`'s own message, built directly -- the errno alone reads as a full disk, which is
    /// the wrong thing for an operator to go looking at.
    #[test]
    fn an_enospc_watch_error_points_at_the_watch_limit() {
        let err = std::io::Error::from_raw_os_error(libc::ENOSPC);
        let message = watch_error_message(Path::new("/var/log/app.log"), &err);
        assert!(message.contains("max_user_watches"), "got: {message}");
    }

    #[test]
    fn inotify_mode_fails_startup_when_setup_fails() {
        let mut diag = Diagnostics::new("test");
        let err = Watcher::new_with(WatchMode::Inotify, &mut diag, || {
            anyhow::bail!("simulated inotify_init1 failure")
        })
        .expect_err("explicit inotify mode must not silently fall back");
        assert!(err.to_string().contains("simulated"), "got: {err}");
    }
}

/// A hand-rolled `inotify` backend: `libc` calls confined to this module, no `notify` crate
/// (license-blocked by `deny.toml` -- see the ADR's Alternatives). A directory watch and a file
/// watch are both plain `inotify_add_watch` calls against one shared fd, distinguished only by
/// mask and by what `parse_events` does with their wake (`WatchTarget`, private to this
/// submodule) -- there is nothing `inotify`-specific about the split itself, it's the same "watch
/// exactly what you'd act on" design `watch.rs`'s own module doc describes.
#[cfg(target_os = "linux")]
mod inotify {
    use super::{Wake, WatchId};
    use std::collections::{HashMap, VecDeque};
    use std::ffi::{CString, OsStr};
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Path, PathBuf};
    use tokio::io::unix::AsyncFd;

    /// One `read()` off the inotify fd -- generously larger than any single burst of events this
    /// driver's own watches (`root`, plus one file per currently-tailed container) would ever
    /// produce at once; a burst larger than this is exactly what `IN_Q_OVERFLOW` (and this
    /// driver's full-rescan response to it) exists to handle.
    ///
    /// Its *lower* bound is load-bearing for liveness, not just for throughput, which is what the
    /// assertion below pins. `inotify_read` refuses an event that doesn't fit the caller's buffer
    /// rather than truncating it (`get_one_event`: `if (event_size > count) return
    /// ERR_PTR(-EINVAL);`), and surfaces that as `EINVAL` whenever nothing has been copied yet --
    /// a *persistent* error, since the same oversized event is still at the head of the queue on
    /// the next read. A buffer below `sizeof(struct inotify_event) + NAME_MAX + 1` therefore turns
    /// one long filename into a read that can never succeed; `next_wake` treats that as fatal to
    /// the wake source (`Wake::Dead`) rather than looping on it, but the buffer is what keeps the
    /// condition unreachable in the first place.
    const EVENT_BUF_BYTES: usize = 64 * 1024;

    /// `NAME_MAX + 1`, the largest name the kernel can pad an event out to -- `libc` exposes no
    /// `NAME_MAX` constant, and the value is 255 on every Linux filesystem this could tail.
    const MAX_EVENT_NAME_BYTES: usize = 256;

    const _: () = assert!(
        EVENT_BUF_BYTES >= std::mem::size_of::<libc::inotify_event>() + MAX_EVENT_NAME_BYTES,
        "a read buffer below one maximum-size event makes `read(2)` fail with EINVAL forever"
    );

    // The kernel's `struct inotify_event` is four 4-byte fields (`wd`, `mask`, `cookie`, `len`)
    // followed by a flexible `name[]` member that is *not* part of the Rust type -- `parse_events`
    // walks the name itself, out of the same buffer, at `header_len` past each event's start.
    // `ptr::read_unaligned` below is sound for any bit pattern of those four fields, but only if
    // the Rust type really is those four fields in that order with no padding, so pin it: a
    // `libc` bump that reshaped the struct would otherwise silently reinterpret every event.
    const _: () = assert!(std::mem::size_of::<libc::inotify_event>() == 16);
    const _: () = assert!(std::mem::align_of::<libc::inotify_event>() == 4);
    const _: () = assert!(std::mem::offset_of!(libc::inotify_event, wd) == 0);
    const _: () = assert!(std::mem::offset_of!(libc::inotify_event, mask) == 4);
    const _: () = assert!(std::mem::offset_of!(libc::inotify_event, cookie) == 8);
    const _: () = assert!(std::mem::offset_of!(libc::inotify_event, len) == 12);

    /// A directory watch's mask: appearance and departure only, no content events. `root` is the
    /// only directory `docker_in` ever watches with this -- Docker's per-container state
    /// directories are direct children of it, so `IN_CREATE`/`IN_DELETE` alone catch a container
    /// arriving or leaving without needing to also watch what's written inside it.
    /// `IN_DELETE_SELF` covers the watched directory itself disappearing; `IN_MOVE_SELF` covers
    /// the other way it can stop being the directory at this path -- a rename of the directory
    /// itself, after which the watch stays perfectly valid on an inode nobody is looking for any
    /// more, with no `IN_IGNORED` to mark it (`fsnotify_move` just calls `fsnotify_inode(source,
    /// FS_MOVE_SELF)`; nothing destroys the mark. The shape `notify`#555 records. A rename across
    /// filesystems is not this case at all -- that is a copy plus an unlink, so it arrives as
    /// `IN_DELETE_SELF` + `IN_IGNORED`). Both arrive nameless -- `fsnotify_inode` passes `NULL`
    /// for both `dir` and `name`, and `inotify_handle_inode_event` explicitly masks `IN_ISDIR`
    /// back out of them ("inotify never reported IN_ISDIR with those events") -- so
    /// `parse_events`' existing nameless-`Dir` arm reports them as the directory changing and the
    /// driver's `scan` re-arms by path.
    ///
    /// `IN_ONLYDIR` is a guard rather than an event: a pattern whose `dir()` is a regular file
    /// (`paths: [/var/log/app.log/*.log]`) would otherwise register a permanently silent watch
    /// that looks healthy. With it, that config mistake is an `ENOTDIR` at the syscall, which
    /// `Tailer::reconcile_watches` diagnoses.
    const DIR_MASK: u32 = libc::IN_CREATE
        | libc::IN_MOVED_TO
        | libc::IN_MOVED_FROM
        | libc::IN_DELETE
        | libc::IN_DELETE_SELF
        | libc::IN_MOVE_SELF
        | libc::IN_ONLYDIR;

    /// A file watch's mask: content changes only. One registered per file a
    /// `crate::tail::driver::Tailer` actually has open -- a write to any *other* file (an
    /// unselected container's log, a rotated-away `.1`) produces no event on this watch, and
    /// therefore no `scan`. A file's own deletion doesn't need a bit here: the kernel always
    /// emits `IN_IGNORED` when a watched inode goes away, regardless of the requested mask -- see
    /// `parse_events`.
    const FILE_MASK: u32 = libc::IN_MODIFY;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum WatchTarget {
        Dir,
        File,
    }

    #[derive(Debug)]
    pub(crate) struct InotifyWatcher {
        fd: AsyncFd<OwnedFd>,
        /// Watch descriptor -> the path it watches and which kind it is, so a raw event (which
        /// only carries a wd) can be turned back into the right [`Wake`] variant. Purged on
        /// `IN_IGNORED` as well as on an explicit `unwatch`/`unwatch_dir` -- see `parse_events`.
        watches: HashMap<i32, (PathBuf, WatchTarget)>,
        /// Directory watches only -- the reverse index, so re-arming a path that already has a
        /// watch can tell "the same inode, same `wd`" from "a *new* inode at that path, new `wd`
        /// and a stale one to release" (`watch_dir`). File watches are never indexed here: a
        /// rotation legitimately reuses one path under a new inode, and each generation gets its
        /// own watch.
        ///
        /// Kept honest in both directions or it is worse than useless -- an entry that outlives
        /// its `wd` used to make `watch_dir` short-circuit on a watch the kernel had already
        /// invalidated. Every removal path purges both maps together: `unwatch_dir`,
        /// `parse_events`' `IN_IGNORED` and `IN_MOVE_SELF` arms, and `watch_dir`'s own
        /// stale-`wd` replacement.
        by_path: HashMap<PathBuf, i32>,
        buf: Vec<u8>,
        /// One `read()` can (and often does) carry more than one event -- drained one at a time
        /// by [`InotifyWatcher::next_wake`] before this reads again.
        pending: VecDeque<Wake>,
        /// Set once, by the first unrecoverable failure of the fd itself (see
        /// [`InotifyWatcher::read_once`]). After it, `next_wake` parks forever instead of reading
        /// again: tokio only clears a fd's cached readiness on a `WouldBlock`
        /// (`AsyncFdReadyGuard::try_io`, tokio 1.53.1), so looping on a fd that fails any other
        /// way would spin *without ever yielding*: `AsyncFd::readable()` resolves through
        /// `Registration::readiness`, which -- unlike `Registration::poll_ready` and
        /// `Registration::async_io` -- carries no `coop::poll_proceed` budget check to break such
        /// a loop up, and `try_io` itself is synchronous. That takes the driver's poll, flush and
        /// checkpoint ticks down with it, which is the one way a defect in this module could cost
        /// data rather than latency. Parking instead leaves the listener exactly where
        /// `watch: poll` always is.
        dead: bool,
    }

    impl InotifyWatcher {
        pub fn new() -> anyhow::Result<Self> {
            Self::with_fd(open_inotify()?)
        }

        /// Test-only seam: same as [`InotifyWatcher::new`], but with the fd-opening step
        /// injected, so `Watcher::new_with`'s own test seam can force this to fail without
        /// touching a real OS limit.
        /// `pub(super)`, not `pub(crate)`: this is a test-only seam for `Watcher::new_with`'s own
        /// tests (in the parent module) as well as this module's own -- nothing outside `watch`
        /// should ever construct one with a fake fd.
        #[cfg(test)]
        pub(super) fn with_init(
            open: impl FnOnce() -> io::Result<OwnedFd>,
        ) -> anyhow::Result<Self> {
            Self::with_fd(open()?)
        }

        fn with_fd(raw: OwnedFd) -> anyhow::Result<Self> {
            let fd = AsyncFd::new(raw)?;
            Ok(Self {
                fd,
                watches: HashMap::new(),
                by_path: HashMap::new(),
                buf: vec![0u8; EVENT_BUF_BYTES],
                pending: VecDeque::new(),
                dead: false,
            })
        }

        /// Test-only: see [`super::Watcher::inotify_fd`].
        #[cfg(test)]
        pub(super) fn raw_fd(&self) -> std::os::fd::RawFd {
            self.fd.get_ref().as_raw_fd()
        }

        /// Test-only: see [`super::Watcher::tracked_watch_count`].
        #[cfg(test)]
        pub(super) fn tracked_watch_count(&self) -> usize {
            self.watches.len()
        }

        /// `inotify_add_watch` against `path` with `mask`, wrapped once so [`InotifyWatcher::
        /// watch_dir`] and [`InotifyWatcher::watch_file`] share the one `unsafe` call site.
        fn add_watch(&self, path: &Path, mask: u32) -> io::Result<i32> {
            let cpath = CString::new(path.as_os_str().as_bytes()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte")
            })?;
            // SAFETY: `self.fd`'s inner fd is a valid, open inotify instance for the life of
            // `self`; `cpath` is a valid NUL-terminated C string that outlives this call.
            let wd = unsafe {
                libc::inotify_add_watch(self.fd.get_ref().as_raw_fd(), cpath.as_ptr(), mask)
            };
            if wd < 0 {
                return Err(io::Error::last_os_error());
            }
            Ok(wd)
        }

        /// `inotify_rm_watch` against `wd`, ignoring the result -- shared by every caller that
        /// already removed its own bookkeeping and doesn't need to know whether the kernel had
        /// already invalidated the watch first (a redundant removal, e.g. after `IN_DELETE_SELF`
        /// or `IN_IGNORED`, returns `EINVAL`, not worth surfacing as an error).
        fn rm_watch(&self, wd: i32) {
            // SAFETY: `self.fd`'s inner fd is valid; `wd` was returned by a prior successful
            // `inotify_add_watch` on this same fd, or is already stale (harmless per above).
            unsafe {
                libc::inotify_rm_watch(self.fd.get_ref().as_raw_fd(), wd);
            }
        }

        /// Arms a directory watch, idempotently. Called for **every** directory the patterns
        /// reach on **every** `scan`, not just for newly-appearing ones -- that repetition is
        /// what makes the watch set self-healing, and it is only affordable because the kernel
        /// makes the repeat call a no-op on the common path: the mark is looked up by *inode*
        /// (`inotify_update_existing_watch`'s `fsnotify_find_inode_mark(inode, group)`), so
        /// re-adding on a live directory returns that same mark's `wd` and, with `IN_MASK_ADD`
        /// absent, rewrites its mask under `spin_lock(&fsn_mark->lock)`. Re-arming with the
        /// identical mask therefore leaves `old_mask == new_mask`, which skips even the
        /// `fsnotify_recalc_mask` branch: no event window, no `IN_IGNORED`, no second kernel
        /// watch. One `inotify_add_watch(2)` per pattern directory per scan is the whole cost,
        /// and every caller has exactly one such directory (`tail_in`'s `paths:` parent,
        /// `docker_in`'s `root` -- `PathPattern::dir`, one per pattern, and neither kind
        /// configures more than one).
        ///
        /// There is deliberately **no** "already in `by_path`, skip the syscall" short-circuit.
        /// That is what used to make a directory deleted and recreated -- or renamed away and
        /// replaced -- unrecoverable: the reverse index still held the dead `wd`, so the re-arm
        /// returned `Ok(())` without ever asking the kernel, and discovery in that directory
        /// silently stayed at `poll_interval` for the life of the process.
        ///
        /// A `wd` that differs from the one this path had means a *new inode* now answers to it.
        /// The previous one is released here: if it died on its own (the directory was deleted)
        /// the `inotify_rm_watch` is a harmless `EINVAL`, and if it did not (the directory was
        /// renamed away, which leaves the watch valid on the moved inode) this is what stops it
        /// reporting activity under a name it no longer has.
        pub fn watch_dir(&mut self, dir: &Path) -> io::Result<()> {
            let wd = self.add_watch(dir, DIR_MASK)?;
            if let Some(stale) = self.by_path.insert(dir.to_path_buf(), wd) {
                if stale != wd {
                    self.watches.remove(&stale);
                    self.rm_watch(stale);
                }
            }
            self.watches.insert(wd, (dir.to_path_buf(), WatchTarget::Dir));
            Ok(())
        }

        pub fn watch_file(&mut self, path: &Path) -> io::Result<WatchId> {
            // No `by_path` dedup here, deliberately: a caller (`Tailer::open_tracked`) registers
            // a file watch exactly once, when it opens the file, and calls `unwatch` exactly
            // once, when it stops tracking it -- deduping by path would paper over a caller bug
            // rather than serve a real need, and would be actively wrong across a rotation, where
            // the same path is legitimately watched under a new inode.
            let wd = self.add_watch(path, FILE_MASK)?;
            self.watches.insert(wd, (path.to_path_buf(), WatchTarget::File));
            Ok(WatchId(wd))
        }

        pub fn unwatch(&mut self, id: WatchId) {
            self.watches.remove(&id.0);
            self.rm_watch(id.0);
        }

        pub fn unwatch_dir(&mut self, dir: &Path) {
            let Some(wd) = self.by_path.remove(dir) else { return };
            self.watches.remove(&wd);
            self.rm_watch(wd);
        }

        /// Cancel-safe: the only suspension point is `readable().await` (plus the terminal
        /// `pending()`), and everything between a successful `read` and the parsed events landing
        /// in `self.pending` is synchronous, so a dropped future can never lose an event this
        /// already took off the fd.
        pub async fn next_wake(&mut self) -> Wake {
            loop {
                if let Some(wake) = self.pending.pop_front() {
                    return wake;
                }
                if self.dead {
                    // Already reported, once, as the `Wake::Dead` that set this. Park rather than
                    // read again: see the field's own doc comment for why looping here would
                    // wedge the driver's task instead of merely busying it.
                    return std::future::pending().await;
                }
                match self.read_once().await {
                    Ok(()) => {}
                    Err(reason) => {
                        self.dead = true;
                        return Wake::Dead(reason);
                    }
                }
            }
        }

        /// Waits for readiness, takes one `read(2)` off the fd, and parses whatever it yielded
        /// into `self.pending`. `Ok(())` covers both a successful read and a stale-readiness
        /// `WouldBlock` -- the caller loops. `Err(reason)` means the fd itself is no longer
        /// usable and never will be: reading again could not make progress, so the caller retires
        /// the wake source instead of retrying it.
        ///
        /// All three `Err` cases are believed unreachable against a real `inotify` fd --
        /// `readable()` errs only when the tokio runtime is shutting down; `EINVAL` needs a
        /// buffer smaller than one event (see `EVENT_BUF_BYTES`), `EINTR` is excluded by
        /// `IN_NONBLOCK`, `EFAULT` by the buffer being a live `Vec`; and a `0` return is
        /// pre-2.6.21 behaviour that modern kernels replaced with `EINVAL`. They are handled as
        /// fatal anyway because the alternative shape (swallow and loop) is not a spin but a
        /// hang: tokio clears a fd's cached readiness only on `WouldBlock`.
        async fn read_once(&mut self) -> Result<(), String> {
            let n = {
                let mut guard = match self.fd.readable().await {
                    Ok(guard) => guard,
                    Err(err) => return Err(format!("waiting on the inotify fd: {err}")),
                };
                let ptr = self.buf.as_mut_ptr();
                let cap = self.buf.len();
                // SAFETY: `ptr`/`cap` describe `self.buf`'s own live allocation, valid for
                // writes of up to `cap` bytes; `inner` is the same fd `readable()` just reported
                // ready, read non-blocking (`IN_NONBLOCK`, set at `open_inotify`).
                let read = guard.try_io(|inner| {
                    // SAFETY: as described above this `try_io` call -- `ptr`/`cap` describe
                    // `self.buf`'s own live allocation, valid for writes of up to `cap` bytes;
                    // `inner` is the same fd `readable()` just reported ready, read non-blocking.
                    let n =
                        unsafe { libc::read(inner.as_raw_fd(), ptr.cast::<libc::c_void>(), cap) };
                    if n < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                });
                match read {
                    Ok(Ok(0)) => {
                        return Err("the inotify fd reported end-of-file".to_string());
                    }
                    Ok(Ok(n)) => n,
                    Ok(Err(err)) => return Err(format!("reading the inotify fd: {err}")),
                    // Readiness was stale; loop back to `readable().await`. This is the *only*
                    // arm tokio treats as "not ready after all" -- `try_io` clears the cached
                    // readiness bit here and nowhere else.
                    Err(_would_block) => return Ok(()),
                }
            };
            let mut release = Vec::new();
            parse_events(
                &self.buf[..n],
                &mut self.watches,
                &mut self.by_path,
                &mut release,
                &mut self.pending,
            );
            // Watches `parse_events` decided this instance should stop holding but could not
            // release itself (it is a pure function over the buffer, with no fd) -- today, a
            // directory renamed out from under its own watch.
            for wd in release {
                self.rm_watch(wd);
            }
            Ok(())
        }
    }

    fn open_inotify() -> io::Result<OwnedFd> {
        // SAFETY: `inotify_init1` takes only flag arguments and returns either a valid,
        // exclusively-owned fd or `-1` on error -- no preconditions beyond that.
        let raw = unsafe { libc::inotify_init1(libc::IN_NONBLOCK | libc::IN_CLOEXEC) };
        if raw < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `raw` was just returned by `inotify_init1` above -- a valid fd this process
        // exclusively owns and hasn't handed to anything else yet.
        Ok(unsafe { OwnedFd::from_raw_fd(raw) })
    }

    /// Decodes every complete `inotify_event` in `buf` (as delivered by one `read()` off the fd)
    /// into a [`Wake`], appended to `out` in order. Pure and independent of a live fd -- `read`'s
    /// job (above) is entirely "make a live fd look like a byte buffer"; this is everything past
    /// that, and is what the unit tests below exercise directly.
    ///
    /// Takes the watch bookkeeping by `&mut`, which every caller reads as immutable everywhere
    /// else: this is the one place that *removes* an entry the kernel has already invalidated,
    /// rather than waiting for the driver's own close path to call `unwatch`. Two events do that:
    ///
    /// - `IN_IGNORED` -- the watch is gone (the inode was deleted, unmounted, or explicitly
    ///   removed). It is structurally the last event for that `wd`: `inotify_freeing_mark` →
    ///   `inotify_ignored_and_remove_idr` sets `i_mark->wd = -1`, after which
    ///   `inotify_handle_inode_event` emits nothing more for it. Purging here is what keeps both
    ///   maps from growing one dead entry per rotation, and -- for a directory -- what lets the
    ///   next `scan` re-arm the path at all.
    /// - `IN_MOVE_SELF` on a directory -- the watch is *not* gone, but the inode behind it no
    ///   longer answers to the path this instance knows it by, and the kernel sends no
    ///   `IN_IGNORED` for a rename. The `wd` is dropped from both maps and pushed onto `release`
    ///   for the caller to `inotify_rm_watch` (this function has no fd), so the `Wake::Discover`
    ///   it also emits finds a clean slate to re-arm into.
    ///
    /// Note what this purge is *not* about: `wd` values are not small recycled integers. The
    /// kernel allocates them with `idr_alloc_cyclic(idr, i_mark, 1, 0, GFP_NOWAIT)` (cyclic since
    /// v3.10, commit `a66c04b4534f`; before that a `*last_wd + 1` cursor that never wrapped at
    /// all), so reuse needs a process to cycle the whole `1..INT_MAX` range -- the caveat
    /// `inotify(7)`'s BUGS section describes, and not a thing a tailer reaches. The purge earns
    /// its place by keeping the maps bounded and `by_path` honest, not by racing a recycled `wd`.
    fn parse_events(
        buf: &[u8],
        watches: &mut HashMap<i32, (PathBuf, WatchTarget)>,
        by_path: &mut HashMap<PathBuf, i32>,
        release: &mut Vec<i32>,
        out: &mut VecDeque<Wake>,
    ) {
        let header_len = std::mem::size_of::<libc::inotify_event>();
        let mut i = 0;
        while i + header_len <= buf.len() {
            // SAFETY: the kernel guarantees `buf[i..]` holds at least one complete
            // `inotify_event` header (checked by the loop condition) plus `event.len` bytes of
            // name immediately after it (checked below before slicing). `read_unaligned` is used
            // deliberately -- nothing guarantees `buf`'s allocation aligns `inotify_event`'s
            // first field (`c_int`) at this offset once more than one event has been consumed.
            let event: libc::inotify_event = unsafe {
                std::ptr::read_unaligned(buf[i..].as_ptr().cast::<libc::inotify_event>())
            };
            let name_len = event.len as usize;
            // `checked_add`, not `i + header_len + name_len`: `event.len` is a `u32` widened to
            // `usize`, so on a 32-bit target the sum can wrap past `buf.len()` and turn the guard
            // below into a permission to slice out of bounds. The kernel itself can never produce
            // such a `len` (`round_event_name_len` caps it at `roundup(NAME_MAX + 1, 16)` = 272),
            // but the SAFETY comment above claims "never read past `buf`" unconditionally, and
            // this is what makes that true for any bytes at all.
            let Some(name_end) = i.checked_add(header_len).and_then(|h| h.checked_add(name_len))
            else {
                break;
            };
            if name_end > buf.len() {
                break; // a truncated trailing event -- shouldn't happen, but never read past buf
            }

            if event.mask & libc::IN_Q_OVERFLOW != 0 {
                out.push_back(Wake::Overflow);
            } else if event.mask & libc::IN_IGNORED != 0 {
                // The kernel has already invalidated this watch descriptor -- rotation cleanup
                // deleting an old file, an operator deleting a log directly, or an explicit
                // `inotify_rm_watch` this process itself issued (which also produces this event,
                // redundantly with the bookkeeping `unwatch`/`unwatch_dir` already did -- removing
                // an already-absent key here is a no-op). No `Wake` for it: whatever needed to
                // notice the underlying file or directory is gone already does, via `root`'s own
                // `IN_DELETE` or the driver's ordinary stale-tracking.
                //
                // Tested separately rather than folded into the `else if` chain below, because it
                // never arrives ORed with an event bit: the kernel emits it as
                // `inotify_handle_inode_event(fsn_mark, FS_IN_IGNORED, NULL, NULL, NULL, 0)` -- a
                // bare constant, no inode, no name -- and `event_compare` refuses to merge
                // anything into an already-queued ignore.
                if let Some((_, WatchTarget::Dir)) = watches.remove(&event.wd) {
                    // Every path that resolved to this now-dead `wd`, not just one: two spellings
                    // of the same directory (a symlink, a `.` component) alias one inode and
                    // therefore share its `wd`.
                    by_path.retain(|_, held| *held != event.wd);
                }
            } else if let Some((path, target)) = watches.get(&event.wd).cloned() {
                match target {
                    WatchTarget::File => out.push_back(Wake::Data(path)),
                    WatchTarget::Dir if name_len > 0 => {
                        let name_bytes = &buf[i + header_len..name_end];
                        // `inotify_event`'s `name` is NUL-padded out to a multiple of
                        // `sizeof(struct inotify_event)` (`round_event_name_len`'s
                        // `roundup(name_len + 1, sizeof(struct inotify_event))`), not exactly
                        // `strlen`-sized -- trim at the first NUL rather than trusting `event.len`
                        // as the name's real length.
                        let end =
                            name_bytes.iter().position(|&b| b == 0).unwrap_or(name_bytes.len());
                        let name = OsStr::from_bytes(&name_bytes[..end]);
                        out.push_back(Wake::Discover(path.join(name)));
                    }
                    WatchTarget::Dir => {
                        // No name -- `IN_DELETE_SELF` or `IN_MOVE_SELF` on the watched directory
                        // itself (the kernel strips `IN_ISDIR` from both, so neither ever carries
                        // one). Reported as the directory changing, which is accurate: something
                        // about it did.
                        out.push_back(Wake::Discover(path));
                        if event.mask & libc::IN_MOVE_SELF != 0 {
                            // Unlike a deletion, a rename leaves the watch perfectly valid -- on
                            // an inode that is no longer this path. Drop it here and let the
                            // `scan` the `Wake` above triggers re-arm whatever is at the path
                            // now; holding on would keep reporting the moved-away directory's
                            // activity under a name it no longer has.
                            watches.remove(&event.wd);
                            by_path.retain(|_, held| *held != event.wd);
                            release.push(event.wd);
                        }
                    }
                }
            }
            // else: an event on a watch descriptor this instance no longer knows about (already
            // unwatched) -- ignored, not an error.

            i = name_end;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// The kernel's own name padding, reproduced exactly (`round_event_name_len`, v6.12
        /// `fs/notify/inotify/inotify_user.c`):
        ///
        /// ```c
        /// static int round_event_name_len(struct fsnotify_event *fsn_event)
        /// {
        ///     struct inotify_event_info *event;
        ///     event = INOTIFY_E(fsn_event);
        ///     if (!event->name_len)
        ///         return 0;
        ///     return roundup(event->name_len + 1, sizeof(struct inotify_event));
        /// }
        /// ```
        ///
        /// So a nameless event has `len == 0` -- not 4, not 16 -- and a named one is padded to a
        /// multiple of **16**, never 4. Getting this wrong in a fixture is not cosmetic: it is
        /// what decides whether a multi-event buffer's second event starts where the code thinks
        /// it does, and it is why `parse_events_reports_the_wd_of_every_event_in_a_multi_event_
        /// buffer` can tell a correct advance from one that forgets `name_len`.
        fn kernel_name_len(name: &str) -> usize {
            let header_len = std::mem::size_of::<libc::inotify_event>();
            if name.is_empty() {
                0
            } else {
                (name.len() + 1).div_ceil(header_len) * header_len
            }
        }

        /// Builds one raw `inotify_event` (header + NUL-padded name) exactly as the kernel would
        /// write it, for feeding to `parse_events` without a real fd.
        fn raw_event(wd: i32, mask: u32, name: &str) -> Vec<u8> {
            let header_len = std::mem::size_of::<libc::inotify_event>();
            let name_bytes = name.as_bytes();
            let padded_len = kernel_name_len(name);
            let event = libc::inotify_event { wd, mask, cookie: 0, len: padded_len as u32 };
            let mut buf = vec![0u8; header_len + padded_len];
            // SAFETY: `buf[..header_len]` is exactly `size_of::<inotify_event>()` bytes, freshly
            // allocated and about to be fully overwritten by this copy.
            unsafe {
                std::ptr::write_unaligned(buf.as_mut_ptr().cast::<libc::inotify_event>(), event);
            }
            buf[header_len..header_len + name_bytes.len()].copy_from_slice(name_bytes);
            buf
        }

        /// One raw event with a hand-chosen `len`, for the malformed cases the kernel could never
        /// produce but `parse_events` must still walk without panicking.
        fn raw_event_with_len(wd: i32, mask: u32, len: u32, trailing: usize) -> Vec<u8> {
            let header_len = std::mem::size_of::<libc::inotify_event>();
            let event = libc::inotify_event { wd, mask, cookie: 0, len };
            let mut buf = vec![0u8; header_len + trailing];
            // SAFETY: as `raw_event` above -- `buf[..header_len]` is exactly one header's worth
            // of freshly allocated bytes, fully overwritten by this copy.
            unsafe {
                std::ptr::write_unaligned(buf.as_mut_ptr().cast::<libc::inotify_event>(), event);
            }
            buf
        }

        /// `parse_events` with the two bookkeeping maps and the release list a caller would pass,
        /// returning everything a test might want to assert on.
        fn parse(
            buf: &[u8],
            watches: &mut HashMap<i32, (PathBuf, WatchTarget)>,
            by_path: &mut HashMap<PathBuf, i32>,
        ) -> (VecDeque<Wake>, Vec<i32>) {
            let mut out = VecDeque::new();
            let mut release = Vec::new();
            parse_events(buf, watches, by_path, &mut release, &mut out);
            (out, release)
        }

        /// The common shape: one watch, no reverse index worth caring about.
        fn parse_wakes(
            buf: &[u8],
            watches: &mut HashMap<i32, (PathBuf, WatchTarget)>,
        ) -> VecDeque<Wake> {
            parse(buf, watches, &mut HashMap::new()).0
        }

        #[test]
        fn parse_events_fixture_pads_names_the_way_the_kernel_does() {
            // A nameless event is a bare 16-byte header with `len == 0` -- the kernel does not
            // pad "no name" out to anything.
            let nameless = raw_event(7, libc::IN_MODIFY, "");
            assert_eq!(nameless.len(), 16);
            assert_eq!(u32::from_ne_bytes(nameless[12..16].try_into().unwrap()), 0);

            // A name is padded to a multiple of `sizeof(struct inotify_event)` == 16, always
            // including at least one NUL: 6 bytes of "abc123" plus its terminator round up to 16.
            let named = raw_event(7, libc::IN_CREATE, "abc123");
            assert_eq!(named.len(), 32);
            assert_eq!(u32::from_ne_bytes(named[12..16].try_into().unwrap()), 16);
            assert_eq!(&named[16..22], b"abc123");
            assert!(named[22..].iter().all(|&b| b == 0), "the pad must be NUL, not garbage");

            // 16 characters + a terminator is 17, which rounds to 32.
            assert_eq!(kernel_name_len("0123456789abcdef"), 32);
        }

        #[test]
        fn parse_events_reports_a_dir_watch_create_as_discover_joined_with_the_name() {
            let dir = PathBuf::from("/var/lib/docker/containers");
            let mut watches = HashMap::from([(7, (dir.clone(), WatchTarget::Dir))]);
            let buf = raw_event(7, libc::IN_CREATE, "abc123");

            let out = parse_wakes(&buf, &mut watches);

            assert_eq!(out, VecDeque::from([Wake::Discover(dir.join("abc123"))]));
        }

        #[test]
        fn parse_events_reports_a_file_watch_modify_as_data_ignoring_any_name() {
            let path = PathBuf::from("/var/lib/docker/containers/abc/abc-json.log");
            let mut watches = HashMap::from([(7, (path.clone(), WatchTarget::File))]);
            // A file watch's own events never carry a name in practice, but even if one somehow
            // did, `Data` always names the watched file itself, never a joined child path.
            let buf = raw_event(7, libc::IN_MODIFY, "");

            let out = parse_wakes(&buf, &mut watches);

            assert_eq!(out, VecDeque::from([Wake::Data(path)]));
        }

        #[test]
        fn parse_events_reports_q_overflow() {
            let mut watches =
                HashMap::from([(7, (PathBuf::from("/var/log/app"), WatchTarget::Dir))]);
            // IN_Q_OVERFLOW events carry wd == -1 and no name.
            let buf = raw_event(-1, libc::IN_Q_OVERFLOW, "");

            let out = parse_wakes(&buf, &mut watches);

            assert_eq!(out, VecDeque::from([Wake::Overflow]));
        }

        /// `wd == -1` without the overflow bit: the kernel's overflow event is the only thing
        /// that carries that descriptor, so anything else bearing it resolves to no watch and is
        /// dropped rather than treated as an overflow.
        #[test]
        fn parse_events_ignores_a_minus_one_watch_descriptor_without_the_overflow_bit() {
            let mut watches =
                HashMap::from([(7, (PathBuf::from("/var/log/app"), WatchTarget::Dir))]);
            let buf = raw_event(-1, libc::IN_CREATE, "app.log");

            let out = parse_wakes(&buf, &mut watches);

            assert!(out.is_empty(), "got: {out:?}");
            assert_eq!(watches.len(), 1, "an unknown wd must not disturb the map");
        }

        #[test]
        fn parse_events_ignores_an_event_on_an_unknown_watch_descriptor() {
            let mut watches =
                HashMap::from([(7, (PathBuf::from("/var/log/app"), WatchTarget::Dir))]);
            let buf = raw_event(99, libc::IN_CREATE, "app.log");

            let out = parse_wakes(&buf, &mut watches);

            assert!(
                out.is_empty(),
                "an event for a watch this instance doesn't know should be dropped, not panic"
            );
        }

        #[test]
        fn parse_events_on_a_nameless_dir_event_reports_the_directory_itself() {
            let dir = PathBuf::from("/var/log/app");
            let mut watches = HashMap::from([(7, (dir.clone(), WatchTarget::Dir))]);
            let buf = raw_event(7, libc::IN_DELETE_SELF, "");

            let out = parse_wakes(&buf, &mut watches);

            assert_eq!(out, VecDeque::from([Wake::Discover(dir)]));
        }

        #[test]
        fn parse_events_purges_an_ignored_watch_and_emits_no_wake_for_it() {
            let path = PathBuf::from("/var/lib/docker/containers/abc/abc-json.log");
            let mut watches = HashMap::from([(7, (path, WatchTarget::File))]);
            let buf = raw_event(7, libc::IN_IGNORED, "");

            let out = parse_wakes(&buf, &mut watches);

            assert!(out.is_empty(), "IN_IGNORED carries no actionable wake of its own");
            assert!(
                watches.is_empty(),
                "the watch descriptor must be purged immediately, not left for the driver's own \
                 close path to notice -- otherwise both maps grow one dead entry per rotation"
            );
        }

        /// The half the reverse index used to miss. A directory watch's `IN_IGNORED` has to clear
        /// `by_path` too, or the next `scan`'s re-arm looks at a stale entry for a watch the
        /// kernel has already invalidated -- which is exactly how a deleted-and-recreated log
        /// directory used to lose its watch permanently.
        #[test]
        fn parse_events_purges_both_indexes_for_an_ignored_directory_watch() {
            let dir = PathBuf::from("/var/log/app");
            let alias = PathBuf::from("/srv/app/logs"); // a second spelling of the same inode
            let mut watches = HashMap::from([(7, (dir.clone(), WatchTarget::Dir))]);
            let mut by_path = HashMap::from([(dir, 7), (alias, 7)]);
            let buf = raw_event(7, libc::IN_IGNORED, "");

            let (out, release) = parse(&buf, &mut watches, &mut by_path);

            assert!(out.is_empty());
            assert!(watches.is_empty(), "the forward index must be purged");
            assert!(
                by_path.is_empty(),
                "every path that resolved to the dead wd must go, aliases included: {by_path:?}"
            );
            assert!(release.is_empty(), "the kernel already released this one");
        }

        /// A file watch's `IN_IGNORED` must not touch `by_path` -- file watches are never indexed
        /// there, and a directory that happens to be keyed elsewhere in the map has nothing to do
        /// with this descriptor.
        #[test]
        fn parse_events_leaves_the_reverse_index_alone_for_an_ignored_file_watch() {
            let dir = PathBuf::from("/var/log/app");
            let mut watches = HashMap::from([
                (3, (dir.clone(), WatchTarget::Dir)),
                (7, (dir.join("app.log"), WatchTarget::File)),
            ]);
            let mut by_path = HashMap::from([(dir, 3)]);
            let buf = raw_event(7, libc::IN_IGNORED, "");

            let (out, _) = parse(&buf, &mut watches, &mut by_path);

            assert!(out.is_empty());
            assert_eq!(watches.len(), 1, "only the file's own entry goes");
            assert_eq!(by_path.len(), 1, "the directory's reverse entry is untouched");
        }

        /// `IN_MOVE_SELF` is the one event that invalidates this instance's *knowledge* of a
        /// watch without invalidating the watch: the kernel sends no `IN_IGNORED` for a rename,
        /// and the descriptor stays live on the moved-away inode. Both indexes drop it and the
        /// caller is handed the `wd` to `inotify_rm_watch`, while the `Wake::Discover` drives the
        /// `scan` that re-arms whatever is at the path now.
        #[test]
        fn parse_events_on_a_moved_directory_purges_it_and_asks_for_its_release() {
            let dir = PathBuf::from("/var/log/app");
            let mut watches = HashMap::from([(7, (dir.clone(), WatchTarget::Dir))]);
            let mut by_path = HashMap::from([(dir.clone(), 7)]);
            let buf = raw_event(7, libc::IN_MOVE_SELF, "");

            let (out, release) = parse(&buf, &mut watches, &mut by_path);

            assert_eq!(out, VecDeque::from([Wake::Discover(dir)]));
            assert!(watches.is_empty(), "the moved-away watch must not keep reporting");
            assert!(by_path.is_empty());
            assert_eq!(
                release,
                vec![7],
                "the kernel watch is still live; the caller must remove it"
            );
        }

        /// A *file* watch never carries `IN_MOVE_SELF` (it isn't in `FILE_MASK`), but if one
        /// somehow arrived it must stay an ordinary data wake rather than silently dropping the
        /// file's watch.
        #[test]
        fn parse_events_does_not_release_a_file_watch_on_a_move_self_bit() {
            let path = PathBuf::from("/var/log/app/app.log");
            let mut watches = HashMap::from([(7, (path.clone(), WatchTarget::File))]);
            let buf = raw_event(7, libc::IN_MOVE_SELF, "");

            let (out, release) = parse(&buf, &mut watches, &mut HashMap::new());

            assert_eq!(out, VecDeque::from([Wake::Data(path)]));
            assert_eq!(watches.len(), 1);
            assert!(release.is_empty());
        }

        /// `IN_IGNORED` is tested before any other bit, so a mask carrying both is a purge and
        /// nothing else. The kernel never ORs them (`inotify_ignored_and_remove_idr` passes the
        /// bare `FS_IN_IGNORED` constant with no inode and no name, and `event_compare` refuses
        /// to merge anything into a queued ignore), but the precedence is worth pinning: the
        /// alternative reading -- emit the wake *and* purge -- would name a path this instance
        /// has just stopped watching.
        #[test]
        fn parse_events_treats_ignored_ored_with_another_bit_as_a_purge() {
            let dir = PathBuf::from("/var/log/app");
            let mut watches = HashMap::from([(7, (dir.clone(), WatchTarget::Dir))]);
            let mut by_path = HashMap::from([(dir, 7)]);
            let buf = raw_event(7, libc::IN_IGNORED | libc::IN_DELETE_SELF, "");

            let (out, _) = parse(&buf, &mut watches, &mut by_path);

            assert!(out.is_empty(), "got: {out:?}");
            assert!(watches.is_empty());
            assert!(by_path.is_empty());
        }

        #[test]
        fn parse_events_does_not_deduplicate_a_burst_of_modifies_on_one_file() {
            let path = PathBuf::from("/var/lib/docker/containers/abc/abc-json.log");
            let mut watches = HashMap::from([(7, (path.clone(), WatchTarget::File))]);
            let mut buf = raw_event(7, libc::IN_MODIFY, "");
            buf.extend(raw_event(7, libc::IN_MODIFY, ""));
            buf.extend(raw_event(7, libc::IN_MODIFY, ""));

            let out = parse_wakes(&buf, &mut watches);

            // Not de-duplicated by `parse_events` itself -- `Tailer::drain`'s own round-robin
            // already drains a file fully on any one wake, so a second, third, ... `Data(path)`
            // queued right behind the first costs nothing but wake up to an already-drained file.
            // (The *kernel* does coalesce identical consecutive unread events, which is what
            // keeps a real burst small; this fixture is one it would never actually produce.)
            assert_eq!(
                out,
                VecDeque::from([
                    Wake::Data(path.clone()),
                    Wake::Data(path.clone()),
                    Wake::Data(path)
                ])
            );
        }

        /// The advance past each event has to include its name, and only a buffer whose *first*
        /// event carries one can tell `i += header + name_len` from `i += header_len`: with a
        /// nameless first event the two are identical, and with a single named event the mutated
        /// loop simply exits early on the right answer. Three events -- named, nameless, named --
        /// so every offset in the walk is load-bearing.
        #[test]
        fn parse_events_decodes_every_event_of_a_mixed_named_and_nameless_buffer() {
            let dir = PathBuf::from("/var/log/app");
            let file = dir.join("app.log");
            let mut watches = HashMap::from([
                (3, (dir.clone(), WatchTarget::Dir)),
                (7, (file.clone(), WatchTarget::File)),
            ]);
            let mut buf = raw_event(3, libc::IN_CREATE, "abc123");
            buf.extend(raw_event(7, libc::IN_MODIFY, ""));
            buf.extend(raw_event(3, libc::IN_MOVED_TO, "def"));

            let out = parse_wakes(&buf, &mut watches);

            assert_eq!(
                out,
                VecDeque::from([
                    Wake::Discover(dir.join("abc123")),
                    Wake::Data(file),
                    Wake::Discover(dir.join("def")),
                ])
            );
        }

        /// The name-fit guard, exercised as a guard: a complete event followed by a named one
        /// whose padding was cut short yields exactly the first wake and no panic. The same
        /// buffer at its full length yields both -- otherwise this would pass for the wrong
        /// reason (a decoder that dropped the second event unconditionally).
        #[test]
        fn parse_events_stops_at_a_truncated_trailing_event() {
            let dir = PathBuf::from("/var/log/app");
            let mut watches = HashMap::from([(3, (dir.clone(), WatchTarget::Dir))]);
            let mut whole = raw_event(3, libc::IN_CREATE, "first");
            whole.extend(raw_event(3, libc::IN_CREATE, "second"));

            let out = parse_wakes(&whole, &mut watches);
            assert_eq!(out.len(), 2, "the intact buffer must decode both events");

            let truncated = &whole[..whole.len() - 2];
            let out = parse_wakes(truncated, &mut watches);
            assert_eq!(
                out,
                VecDeque::from([Wake::Discover(dir.join("first"))]),
                "a trailing event whose name doesn't fit is dropped, not read past"
            );
        }

        /// The header-fit guard's `<=` boundary, from both sides: a buffer that ends exactly on
        /// an event boundary decodes everything in it, and one byte of a further header decodes
        /// no more. (The fixture only reaches this boundary because it pads the way the kernel
        /// does -- a 4-byte pad would never land a nameless event's end on `buf.len()`.)
        #[test]
        fn parse_events_decodes_an_event_that_exactly_fills_the_buffer() {
            let dir = PathBuf::from("/var/log/app");
            let mut watches = HashMap::from([(3, (dir.clone(), WatchTarget::Dir))]);
            let exact = raw_event(3, libc::IN_DELETE_SELF, "");
            assert_eq!(exact.len(), std::mem::size_of::<libc::inotify_event>());

            let out = parse_wakes(&exact, &mut watches);
            assert_eq!(out, VecDeque::from([Wake::Discover(dir.clone())]));

            let mut plus_one_byte = exact.clone();
            plus_one_byte.push(0);
            let out = parse_wakes(&plus_one_byte, &mut watches);
            assert_eq!(
                out,
                VecDeque::from([Wake::Discover(dir)]),
                "a partial trailing header decodes nothing extra"
            );

            let out = parse_wakes(&exact[..exact.len() - 1], &mut watches);
            assert!(out.is_empty(), "one byte short of a header decodes nothing at all");
        }

        /// `event.len` is a `u32` widened to `usize`; on a 32-bit target `i + 16 + len` can wrap
        /// past `buf.len()` and turn the fit check into a green light for an out-of-bounds slice.
        /// `checked_add` is what makes the SAFETY comment's "never read past `buf`" hold for any
        /// bytes at all, and this pins it on every target: the event is discarded, nothing panics.
        #[test]
        fn parse_events_discards_an_event_whose_len_would_overflow() {
            let dir = PathBuf::from("/var/log/app");
            let mut watches = HashMap::from([(3, (dir, WatchTarget::Dir))]);
            for len in [u32::MAX, u32::MAX - 15, i32::MAX as u32, 1 << 31] {
                let buf = raw_event_with_len(3, libc::IN_CREATE, len, 8);
                let out = parse_wakes(&buf, &mut watches);
                assert!(out.is_empty(), "len {len} should decode nothing, got {out:?}");
            }
        }

        /// A `len` the kernel could never emit (not a multiple of 16) walks the rest of the
        /// buffer misaligned, which is garbage in and garbage out -- but it must stay
        /// memory-safe, terminate, and never resolve a descriptor this instance doesn't hold.
        #[test]
        fn parse_events_never_panics_on_a_len_that_is_not_a_multiple_of_sixteen() {
            let dir = PathBuf::from("/var/log/app");
            for len in 1u32..48 {
                let mut watches = HashMap::from([(3, (dir.clone(), WatchTarget::Dir))]);
                let mut buf = raw_event_with_len(3, libc::IN_CREATE, len, len as usize);
                buf.extend(raw_event(3, libc::IN_CREATE, "second"));
                let (out, _) = parse(&buf, &mut watches, &mut HashMap::new());
                for wake in &out {
                    assert!(
                        matches!(wake, Wake::Discover(p) if p.starts_with(&dir))
                            || *wake == Wake::Overflow,
                        "len {len} produced a wake for something this instance never watched: \
                         {wake:?}"
                    );
                }
            }
        }

        /// A seeded mutation sweep in the shape of `crates/logit-proto/tests/robustness.rs`:
        /// every single-byte truncation of a valid multi-event buffer, plus a few thousand
        /// single-bit flips. `parse_events` walks kernel-supplied bytes, so this is not an
        /// untrusted-input parser in the network sense -- but it is the one piece of this module
        /// that does manual offset arithmetic over an unaligned struct read, and the `with_init`
        /// seam can feed it bytes from a fd that isn't an inotify instance at all. The contract:
        /// never panic, always terminate, and never emit a wake naming a watch that isn't in the
        /// map.
        #[test]
        fn parse_events_survives_seeded_truncation_and_bit_flips() {
            // Hand-rolled, seeded, reproducible -- no RNG crate exists in this workspace, and
            // this is the same `Lcg` `logit-proto`'s robustness suite uses.
            struct Lcg(u64);
            impl Lcg {
                fn next_u64(&mut self) -> u64 {
                    self.0 =
                        self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                    self.0
                }
            }

            let dir = PathBuf::from("/var/log/app");
            let file = dir.join("app.log");
            let watches = HashMap::from([
                (3, (dir.clone(), WatchTarget::Dir)),
                (7, (file.clone(), WatchTarget::File)),
            ]);
            let mut valid = raw_event(3, libc::IN_CREATE, "abc123");
            valid.extend(raw_event(7, libc::IN_MODIFY, ""));
            valid.extend(raw_event(3, libc::IN_DELETE, "abc123"));
            valid.extend(raw_event(3, libc::IN_MOVE_SELF, ""));

            let check = |buf: &[u8], label: &str| {
                let mut watches = watches.clone();
                let mut by_path = HashMap::from([(dir.clone(), 3)]);
                let (out, release) = parse(buf, &mut watches, &mut by_path);
                for wake in &out {
                    match wake {
                        Wake::Overflow => {}
                        Wake::Data(p) => assert_eq!(p, &file, "{label}: {wake:?}"),
                        Wake::Discover(p) => {
                            assert!(p.starts_with(&dir), "{label}: {wake:?}")
                        }
                        Wake::Dead(_) => panic!("{label}: parse_events never reports Dead"),
                    }
                }
                for wd in release {
                    assert_eq!(wd, 3, "{label}: released a descriptor it never held");
                }
            };

            for len in 0..valid.len() {
                check(&valid[..len], &format!("truncated to {len}"));
            }

            // Miri walks every one of these at interpreter speed; a hundredth of the iterations
            // still covers every byte of the fixture several times over.
            let flips = if cfg!(miri) { 40 } else { 4_000 };
            let mut rng = Lcg(0x005E_ED10_71F7);
            for i in 0..flips {
                let mut buf = valid.clone();
                let byte = (rng.next_u64() % buf.len() as u64) as usize;
                let bit = (rng.next_u64() % 8) as u32;
                buf[byte] ^= 1 << bit;
                check(&buf, &format!("flip {i} at byte {byte} bit {bit}"));
            }

            // A length field inflated far past the buffer, at every event boundary.
            let header_len = std::mem::size_of::<libc::inotify_event>();
            let mut offset = 0;
            while offset + header_len <= valid.len() {
                for len in [u32::MAX, u32::MAX - 1, 1 << 30, valid.len() as u32 + 1] {
                    let mut buf = valid.clone();
                    buf[offset + 12..offset + 16].copy_from_slice(&len.to_ne_bytes());
                    check(&buf, &format!("len {len} at offset {offset}"));
                }
                offset += header_len;
            }
        }

        #[tokio::test]
        async fn inotify_watcher_watch_dir_wakes_discover_on_a_child_file_created() {
            let dir = crate::tail::test_support::scratch_dir("inotify-wake-dir");
            let mut watcher =
                InotifyWatcher::new().expect("inotify should be available in the dev container");
            watcher.watch_dir(&dir).expect("watch_dir should succeed");

            let path = dir.join("app.log");
            std::fs::write(&path, b"hello\n").unwrap();

            let wake = tokio::time::timeout(std::time::Duration::from_secs(5), watcher.next_wake())
                .await
                .expect("should wake within 5s");
            assert_eq!(wake, Wake::Discover(path));

            std::fs::remove_dir_all(&dir).ok();
        }

        #[tokio::test]
        async fn inotify_watcher_watch_file_wakes_data_on_its_own_write() {
            let dir = crate::tail::test_support::scratch_dir("inotify-wake-file");
            let path = dir.join("app.log");
            std::fs::write(&path, b"first\n").unwrap();

            let mut watcher =
                InotifyWatcher::new().expect("inotify should be available in the dev container");
            watcher.watch_file(&path).expect("watch_file should succeed");

            // Not `IN_CREATE` -- the file already exists, so only content changes should wake
            // this watch. `OpenOptions::append` avoids `O_TRUNC`, which fires no `IN_MODIFY` of
            // its own to conflate with the appended write below.
            use std::io::Write;
            std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap()
                .write_all(b"more\n")
                .unwrap();

            let wake = tokio::time::timeout(std::time::Duration::from_secs(5), watcher.next_wake())
                .await
                .expect("should wake within 5s");
            assert_eq!(wake, Wake::Data(path));

            std::fs::remove_dir_all(&dir).ok();
        }

        /// Re-arming a live directory is cheap *and* stable: the kernel looks a mark up by inode,
        /// so the second `inotify_add_watch` returns the same descriptor and replaces an
        /// identical mask, with no second watch and no bookkeeping churn. This is what makes
        /// `Tailer::reconcile_watches` calling `watch_dir` on every scan affordable.
        #[tokio::test]
        async fn rearming_a_live_directory_returns_the_same_watch_descriptor() {
            let dir = crate::tail::test_support::scratch_dir("inotify-rearm-live");
            let mut watcher = InotifyWatcher::new().expect("inotify should be available");
            watcher.watch_dir(&dir).expect("first arm");
            let first = watcher.by_path[&dir];
            watcher.watch_dir(&dir).expect("second arm");

            assert_eq!(watcher.by_path[&dir], first, "same inode, same wd");
            assert_eq!(watcher.watches.len(), 1, "no second entry for the same directory");

            std::fs::remove_dir_all(&dir).ok();
        }

        /// The recreate case, at the watcher's own level: a directory replaced by a new inode at
        /// the same path gets a *new* descriptor, and the stale one is dropped from both indexes
        /// rather than left to make the next re-arm a no-op.
        #[tokio::test]
        async fn rearming_a_replaced_directory_swaps_the_watch_descriptor() {
            let dir = crate::tail::test_support::scratch_dir("inotify-rearm-replaced");
            let mut watcher = InotifyWatcher::new().expect("inotify should be available");
            watcher.watch_dir(&dir).expect("first arm");
            let first = watcher.by_path[&dir];

            std::fs::remove_dir_all(&dir).unwrap();
            std::fs::create_dir_all(&dir).unwrap();
            watcher.watch_dir(&dir).expect("re-arm on the new inode");

            let second = watcher.by_path[&dir];
            assert_ne!(second, first, "a new inode must get its own descriptor");
            assert_eq!(
                watcher.watches.len(),
                1,
                "the stale descriptor must not be left behind: {:?}",
                watcher.watches
            );
            assert!(watcher.watches.contains_key(&second));

            std::fs::remove_dir_all(&dir).ok();
        }

        /// A failed `inotify_add_watch` is surfaced, not swallowed -- the errno is what
        /// `Tailer`'s `watch_error` diagnostic carries. Two deterministic triggers: a path that
        /// doesn't exist (`ENOENT`, the "log directory isn't there yet" case), and a path that is
        /// a regular file (`ENOTDIR`, courtesy of `IN_ONLYDIR` -- without that bit this would
        /// succeed and register a permanently silent watch).
        #[tokio::test]
        async fn watch_dir_surfaces_enoent_and_enotdir() {
            let dir = crate::tail::test_support::scratch_dir("inotify-dir-errors");
            let mut watcher = InotifyWatcher::new().expect("inotify should be available");

            let missing = dir.join("not-created-yet");
            let err = watcher.watch_dir(&missing).expect_err("a missing directory must error");
            assert_eq!(err.raw_os_error(), Some(libc::ENOENT), "got: {err}");

            let regular = dir.join("app.log");
            std::fs::write(&regular, b"").unwrap();
            let err = watcher.watch_dir(&regular).expect_err("a regular file must error");
            assert_eq!(err.raw_os_error(), Some(libc::ENOTDIR), "got: {err}");

            assert!(watcher.watches.is_empty(), "a failed arm must record nothing");
            assert!(watcher.by_path.is_empty());

            std::fs::remove_dir_all(&dir).ok();
        }

        /// `unwatch_dir` has no production caller today (`Tailer::patterns` never changes, so
        /// `reconcile_watches`' removal loop always iterates an empty difference) -- this is what
        /// keeps it honest anyway: both indexes go, and the path can be armed again afterwards.
        #[tokio::test]
        async fn unwatch_dir_releases_both_indexes_and_allows_a_rearm() {
            let dir = crate::tail::test_support::scratch_dir("inotify-unwatch-dir");
            let mut watcher = InotifyWatcher::new().expect("inotify should be available");
            watcher.watch_dir(&dir).expect("arm");
            assert_eq!(watcher.watches.len(), 1);

            watcher.unwatch_dir(&dir);
            assert!(watcher.watches.is_empty());
            assert!(watcher.by_path.is_empty());

            watcher.watch_dir(&dir).expect("re-arm after an explicit unwatch");
            assert_eq!(watcher.watches.len(), 1);

            std::fs::remove_dir_all(&dir).ok();
        }

        #[test]
        fn with_init_surfaces_the_open_failure_rather_than_panicking() {
            let err =
                InotifyWatcher::with_init(|| Err(io::Error::from(io::ErrorKind::PermissionDenied)))
                    .expect_err(
                        "a failing open should surface as an error, not construct a watcher",
                    );
            assert!(err.to_string().contains("permission"), "got: {err}");
        }

        /// The one liveness property that isn't about inotify semantics at all: a fd that reads
        /// as broken must retire the wake source, once, rather than loop on it. tokio clears a
        /// fd's cached readiness only on `WouldBlock` (`AsyncFdReadyGuard::try_io`), and
        /// `AsyncFd::readable()`'s path has no cooperative-budget check, so a "swallow it and try
        /// again" arm here would spin *without yielding* -- taking the driver's poll, flush and
        /// checkpoint ticks down with it, which is the one way an inotify defect in this module
        /// could cost data rather than latency.
        ///
        /// Driven through `with_init` over the read end of a pipe whose write end is already
        /// closed: `read(2)` on it returns `0` deterministically, which is the same dead-end this
        /// arm handles for a real inotify fd (a `0` return, or any error other than would-block).
        #[tokio::test]
        async fn a_watcher_whose_fd_reads_as_broken_dies_once_and_then_parks() {
            let mut fds = [0 as libc::c_int; 2];
            // SAFETY: `pipe2` writes exactly two fds into the array it is given and returns -1 on
            // failure without touching it; `fds` is a live, correctly sized local.
            let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
            assert_eq!(rc, 0, "pipe2: {}", io::Error::last_os_error());
            // SAFETY: both fds were just returned by `pipe2` above -- valid, exclusively owned by
            // this process, and not yet handed to anything else.
            let (read_end, write_end) =
                unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
            drop(write_end); // every subsequent read on `read_end` is an immediate EOF

            let mut watcher =
                InotifyWatcher::with_init(move || Ok(read_end)).expect("with_init should succeed");

            let wake = tokio::time::timeout(std::time::Duration::from_secs(5), watcher.next_wake())
                .await
                .expect("a broken fd must be reported promptly, not hang");
            let Wake::Dead(reason) = wake else { panic!("expected Wake::Dead, got {wake:?}") };
            assert!(reason.contains("end-of-file"), "got: {reason}");

            // Reported once. From here the arm simply never resolves again -- the driver's own
            // poll tick is the listener's wake source, exactly as under `watch: poll`.
            let again =
                tokio::time::timeout(std::time::Duration::from_millis(250), watcher.next_wake())
                    .await;
            assert!(again.is_err(), "a dead watcher must park, not report again: {again:?}");
        }
    }
}
