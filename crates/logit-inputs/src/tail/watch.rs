//! The wake source a [`crate::tail::driver::Tailer`] races against its poll tick.
//!
//! [`Watcher::Poll`] never resolves, leaving `poll_interval` as the only wake. On Linux,
//! [`Watcher::Inotify`] wraps an `inotify` fd for near-immediate wakes, with `poll_interval` still
//! running as reconciliation for anything `inotify` missed (an `IN_Q_OVERFLOW`, a network or FUSE
//! mount). See `docs/adr/file-tailing-and-docker-json-logs.md`'s "Wake source: poll always,
//! `inotify` as a lower-latency addition", which records the kernel facts this module relies on,
//! and `docs/adr/docker-container-identity-and-minimal-watches.md`.
//!
//! Two kinds of watch with separate masks. A directory watch ([`Watcher::watch_dir`], `DIR_MASK`)
//! reports entries appearing or departing; each pattern has one (`docker_in`'s is `root`). A file
//! watch ([`Watcher::watch_file`], `FILE_MASK`) reports only content changes, one per open file.
//! An untailed file has no watch, and a write to a tailed file never causes a rescan.

use super::WatchMode;
use logit_core::Diagnostics;
use std::path::{Path, PathBuf};

/// What a watcher woke for; `crate::tail::driver::Tailer::run_until_shutdown` handles each.
///
/// - `Discover`: a directory watch saw a path appear or depart, or (nameless) the directory itself
///   was deleted or moved. The driver rescans.
/// - `Data`: a tracked file's content changed. The driver checks only that file for truncation.
/// - `Overflow`: the kernel's event queue overflowed and events were lost. The driver rescans.
/// - `Dead`: the fd is unusable. Emitted at most once per watcher, after which
///   [`Watcher::next_wake`] parks forever; the driver diagnoses `watch_error` and polls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Wake {
    Discover(PathBuf),
    Data(PathBuf),
    Overflow,
    Dead(String),
}

/// Identifies one file watch for a later [`Watcher::unwatch`]. Opaque outside this module.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WatchId(i32);

#[derive(Debug)]
pub(crate) enum Watcher {
    Poll,
    #[cfg(target_os = "linux")]
    Inotify(inotify::InotifyWatcher),
}

impl Watcher {
    /// `Poll` always succeeds. `Inotify` fails on any setup failure, including a non-Linux build,
    /// so an explicit request for the low-latency path never degrades unnoticed. `Auto` falls
    /// back to `Poll`, diagnosed `watch_error`.
    pub fn new(mode: WatchMode, diag: &mut Diagnostics) -> anyhow::Result<Self> {
        Self::new_inner(mode, diag, make_inotify)
    }

    /// [`Watcher::new`] with the `inotify` constructor injected, so a test can force a setup
    /// failure.
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

    /// Releases a directory watch no pattern reaches any more (`Tailer::reconcile_watches`).
    ///
    /// **Unreachable today, but kept.** The patterns never change, so the difference is always
    /// empty. If they ever become mutable, `reconcile_watches` needs this to avoid leaking a
    /// kernel watch per dropped directory. Covered by a unit test only.
    pub fn unwatch_dir(&mut self, dir: &Path) {
        match self {
            Watcher::Poll => {}
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.unwatch_dir(dir),
        }
    }

    /// Watches one file's content changes; `Ok(None)` under [`Watcher::Poll`].
    ///
    /// An `Err` must reach the caller rather than become `None`: `Tailer::open_tracked` diagnoses
    /// it `watch_error`, since the file falling back to `poll_interval` is otherwise
    /// invisible, and the likely cause at scale is `ENOSPC` against
    /// `fs.inotify.max_user_watches`.
    pub fn watch_file(&mut self, path: &Path) -> std::io::Result<Option<WatchId>> {
        match self {
            Watcher::Poll => Ok(None),
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.watch_file(path).map(Some),
        }
    }

    /// Releases a file watch when its file stops being tracked, for any reason.
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

    /// Whether this is the `Inotify` backend, for tests.
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn is_inotify(&self) -> bool {
        match self {
            Watcher::Poll => false,
            #[cfg(target_os = "linux")]
            Watcher::Inotify(_) => true,
        }
    }

    /// The raw `inotify` fd, so a test can count live kernel watches in `/proc/self/fdinfo/<fd>`
    /// (one `inotify wd:` line each). `None` under [`Watcher::Poll`].
    #[cfg(all(test, target_os = "linux"))]
    pub fn inotify_fd(&self) -> Option<std::os::fd::RawFd> {
        match self {
            Watcher::Poll => None,
            Watcher::Inotify(w) => Some(w.raw_fd()),
        }
    }

    /// How many watch descriptors this watcher believes it holds, for tests.
    #[cfg(all(test, target_os = "linux"))]
    pub fn tracked_watch_count(&self) -> usize {
        match self {
            Watcher::Poll => 0,
            Watcher::Inotify(w) => w.tracked_watch_count(),
        }
    }
}

/// The diagnostic text for a failed `inotify_add_watch`, shared by the directory
/// (`watch_dir_error`) and file (`watch_error`) call sites.
///
/// `ENOSPC` ("No space left on device") gets a pointer at the host setting, because it reads as a
/// full disk: `inotify_add_watch(2)` returns it when the per-user watch limit is reached
/// (`inotify_new_watch`'s `inc_inotify_watches` check). Since Linux 5.11 (commit `92890123749b`)
/// `inotify_user_setup()` derives that limit from memory, clamped to `8192..=1048576`, so the
/// number to check is the host's `/proc/sys/fs/inotify/max_user_watches`.
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

// What `make_inotify` builds: `InotifyWatcher` on Linux, and an uninhabited type elsewhere, where
// `make_inotify` always fails.
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
    match w {} // uninhabited here: unreachable
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

    /// A failed file watch is an `Err`, not `Poll`'s `None`. `ENOENT` stands in for production's
    /// `ENOSPC`, which a test can't provoke without a host-wide sysctl.
    #[tokio::test]
    async fn watch_file_surfaces_its_error_rather_than_swallowing_it() {
        let mut diag = Diagnostics::new("test");
        let mut watcher = Watcher::new(WatchMode::Inotify, &mut diag)
            .expect("inotify should be available in the dev container");
        let err = watcher
            .watch_file(Path::new("/nonexistent/logit-test/app.log"))
            .expect_err("a missing file must not watch successfully");
        assert_eq!(err.raw_os_error(), Some(libc::ENOENT), "got: {err}");

        // The diagnostic names the path and the errno.
        let message = watch_error_message(Path::new("/nonexistent/logit-test/app.log"), &err);
        assert!(message.contains("/nonexistent/logit-test/app.log"), "got: {message}");
        assert!(message.contains("No such file"), "got: {message}");

        // `Poll` has nothing to watch with, which is not an error.
        let mut poll = Watcher::Poll;
        assert!(poll.watch_file(Path::new("/nonexistent/logit-test/app.log")).unwrap().is_none());
    }

    /// `ENOSPC` points at the watch limit, not a full disk.
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

/// A hand-rolled `inotify` backend with every `libc` call confined here, instead of the `notify`
/// crate (`docs/adr/file-tailing-and-docker-json-logs.md`'s "Alternatives considered"). Its
/// `unsafe` sites are verified out of CI (`docs/adr/out-of-ci-unsafe-verification.md`). Directory
/// and file watches share one fd and differ only by mask and `WatchTarget`.
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

    /// One `read()` off the inotify fd: far more than one burst from these watches produces. A
    /// larger backlog is what `IN_Q_OVERFLOW` and the full rescan handle.
    ///
    /// The *lower* bound is a liveness requirement, asserted below. The kernel never truncates an
    /// event: one that doesn't fit fails the read with `EINVAL` when nothing was copied yet, and
    /// stays at the head of the queue, so every later read fails too. Below
    /// `sizeof(struct inotify_event) + NAME_MAX + 1`, one long filename would kill the wake source.
    const EVENT_BUF_BYTES: usize = 64 * 1024;

    /// `NAME_MAX + 1`. `libc` has no `NAME_MAX`; it's 255 on every Linux filesystem.
    const MAX_EVENT_NAME_BYTES: usize = 256;

    const _: () = assert!(
        EVENT_BUF_BYTES >= std::mem::size_of::<libc::inotify_event>() + MAX_EVENT_NAME_BYTES,
        "a read buffer below one maximum-size event makes `read(2)` fail with EINVAL forever"
    );

    // `struct inotify_event` is four 4-byte fields (`wd`, `mask`, `cookie`, `len`) then a
    // flexible `name[]` the Rust type omits; `parse_events` reads the name from the buffer at
    // `header_len` past each event. `read_unaligned` is sound for any bit pattern only if the Rust
    // type is those four fields, in order, unpadded, so a `libc` change that reshaped it fails
    // the build here.
    const _: () = assert!(std::mem::size_of::<libc::inotify_event>() == 16);
    const _: () = assert!(std::mem::align_of::<libc::inotify_event>() == 4);
    const _: () = assert!(std::mem::offset_of!(libc::inotify_event, wd) == 0);
    const _: () = assert!(std::mem::offset_of!(libc::inotify_event, mask) == 4);
    const _: () = assert!(std::mem::offset_of!(libc::inotify_event, cookie) == 8);
    const _: () = assert!(std::mem::offset_of!(libc::inotify_event, len) == 12);

    /// A directory watch's mask: entries appearing and departing, no content events.
    ///
    /// `IN_DELETE_SELF` catches the directory being deleted. `IN_MOVE_SELF` catches it being
    /// renamed, which leaves the watch valid on the moved inode with no `IN_IGNORED` to say so (a
    /// rename across filesystems arrives as `IN_DELETE_SELF` + `IN_IGNORED` instead). Both arrive
    /// nameless and without `IN_ISDIR`, so `parse_events` reports the directory itself and the
    /// driver's `scan` re-arms by path.
    ///
    /// `IN_ONLYDIR` is a guard, not an event: a pattern whose `dir()` is a regular file would
    /// otherwise get a silent watch that looks healthy. With it, that's an `ENOTDIR` that
    /// `Tailer::reconcile_watches` diagnoses.
    const DIR_MASK: u32 = libc::IN_CREATE
        | libc::IN_MOVED_TO
        | libc::IN_MOVED_FROM
        | libc::IN_DELETE
        | libc::IN_DELETE_SELF
        | libc::IN_MOVE_SELF
        | libc::IN_ONLYDIR;

    /// A file watch's mask: content changes only. A truncation, including an `O_TRUNC` open, also
    /// raises `IN_MODIFY` (`notify_change` → `fsnotify_change` on `ATTR_SIZE`). Deletion needs no
    /// bit: the kernel emits `IN_IGNORED` when a watched inode goes away, whatever the mask.
    const FILE_MASK: u32 = libc::IN_MODIFY;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum WatchTarget {
        Dir,
        File,
    }

    #[derive(Debug)]
    pub(crate) struct InotifyWatcher {
        fd: AsyncFd<OwnedFd>,
        /// `wd` to the watched path and kind, to turn a raw event into a [`Wake`]. Purged on
        /// `IN_IGNORED` as well as by `unwatch`/`unwatch_dir`.
        watches: HashMap<i32, (PathBuf, WatchTarget)>,
        /// Directory path to `wd`, so `watch_dir` can tell a re-arm of the same inode from a new
        /// inode at the path whose stale `wd` must be released. Files aren't indexed: a rotation
        /// puts a new inode, with its own watch, at the same path.
        ///
        /// Every removal purges both maps together (`unwatch_dir`, `parse_events`' `IN_IGNORED`
        /// and `IN_MOVE_SELF` arms, `watch_dir`'s stale-`wd` release). An entry outliving its `wd`
        /// is worse than none.
        by_path: HashMap<PathBuf, i32>,
        buf: Vec<u8>,
        /// Parsed wakes from one `read()`, handed out one per [`InotifyWatcher::next_wake`].
        pending: VecDeque<Wake>,
        /// Set by the fd's first unrecoverable failure ([`InotifyWatcher::read_once`]); after
        /// it, `next_wake` parks forever.
        ///
        /// Retrying would spin without yielding: tokio clears cached readiness only on
        /// `WouldBlock` (`AsyncFdReadyGuard::try_io`, tokio 1.53.1), and `AsyncFd::readable()`
        /// goes through `Registration::readiness`, which has no `coop` budget check. That would
        /// stall the driver's poll, flush, and checkpoint ticks, the one way this module could
        /// cost data rather than latency. Parking leaves the listener as `watch: poll` would be.
        dead: bool,
    }

    impl InotifyWatcher {
        pub fn new() -> anyhow::Result<Self> {
            Self::with_fd(open_inotify()?)
        }

        /// [`InotifyWatcher::new`] with the fd injected, so a test can supply a failing open or
        /// a fd that isn't an inotify instance.
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

        /// See [`super::Watcher::inotify_fd`].
        #[cfg(test)]
        pub(super) fn raw_fd(&self) -> std::os::fd::RawFd {
            self.fd.get_ref().as_raw_fd()
        }

        /// See [`super::Watcher::tracked_watch_count`].
        #[cfg(test)]
        pub(super) fn tracked_watch_count(&self) -> usize {
            self.watches.len()
        }

        /// `inotify_add_watch(2)`: the one `unsafe` call site both watch kinds share.
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

        /// `inotify_rm_watch(2)`, ignoring the result: removing a watch the kernel already
        /// invalidated (after `IN_IGNORED`) returns a harmless `EINVAL`.
        fn rm_watch(&self, wd: i32) {
            // SAFETY: `self.fd`'s inner fd is valid; `wd` was returned by a prior successful
            // `inotify_add_watch` on this same fd, or is already stale (harmless per above).
            unsafe {
                libc::inotify_rm_watch(self.fd.get_ref().as_raw_fd(), wd);
            }
        }

        /// Arms a directory watch, idempotently; called for every pattern directory on every
        /// `scan`.
        ///
        /// A repeat on a live directory is a kernel no-op: the mark is found by inode, so it
        /// returns the same `wd`, and an identical mask changes nothing (no event, no
        /// `IN_IGNORED`, no second watch).
        ///
        /// **Never skip the syscall because `by_path` has the path.** After a delete-and-recreate
        /// or a rename-and-replace, `by_path` holds a dead or moved `wd`, and only asking the
        /// kernel finds the new inode. A different `wd` means a new inode: the old one is
        /// released, a harmless `EINVAL` if it died, and otherwise what stops a renamed-away
        /// directory reporting under its old name.
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
            // No dedup by path: the caller watches each opened file once and unwatches it once,
            // and a rotation puts a new inode, needing its own watch, at the same path.
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

        /// Cancel-safe: the only await points are `readable()` and the terminal `pending()`, and
        /// a successful read reaches `self.pending` synchronously, so dropping the future never
        /// loses an event already read.
        pub async fn next_wake(&mut self) -> Wake {
            loop {
                if let Some(wake) = self.pending.pop_front() {
                    return wake;
                }
                if self.dead {
                    // Already reported as `Wake::Dead`. Park; see `dead` for why not retry.
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

        /// Waits for readiness, takes one `read(2)`, and parses it into `self.pending`.
        ///
        /// `Ok(())` is a read or a stale-readiness `WouldBlock` (`EAGAIN`); the caller loops.
        /// `Err` means the fd can never make progress, and the caller retires the wake source.
        /// Every `Err` should be unreachable on a real inotify fd: `readable()` fails only during
        /// runtime shutdown, `EINVAL` needs a buffer below one event (`EVENT_BUF_BYTES`), a
        /// non-blocking read can't be interrupted (`EINTR`), `EFAULT` can't happen with a live
        /// `Vec`, and a `0` return predates Linux 2.6.21. They're fatal anyway, because looping
        /// on them would hang (see `dead`).
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
                    // Stale readiness: the only case where `try_io` clears the cached bit, so
                    // the next `readable()` really waits.
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
            // `parse_events` has no fd, so it hands back the watches to release (a directory
            // renamed away).
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

    /// Decodes every complete `inotify_event` from one `read()` into [`Wake`]s, in order. Pure,
    /// with no fd, so tests (and miri) exercise it directly.
    ///
    /// Also the one place that removes watches the kernel invalidated, rather than waiting for
    /// the driver to `unwatch`:
    ///
    /// - `IN_IGNORED`: the watch is gone (inode deleted, unmounted, or removed), and it's the last
    ///   event for that `wd` (`inotify_ignored_and_remove_idr` sets the mark's `wd` to -1).
    ///   Purging keeps the maps from growing an entry per rotation and lets the next `scan`
    ///   re-arm a directory.
    /// - `IN_MOVE_SELF` on a directory: the watch is valid but on an inode no longer at this path,
    ///   and no `IN_IGNORED` follows. Dropped from both maps and pushed onto `release` for the
    ///   caller to `inotify_rm_watch`.
    ///
    /// The purge isn't about `wd` reuse: the kernel allocates them cyclically over `1..INT_MAX`.
    ///
    /// After a malformed event the rest of the buffer is discarded, not resynchronized
    /// (`docs/known-gaps.md`); a real inotify fd never produces one.
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
            // `checked_add`: on a 32-bit target `i + header_len + name_len` can wrap and pass the
            // bounds check below. The kernel caps `len` at 272, but this keeps the slice in
            // bounds for any bytes at all.
            let Some(name_end) = i.checked_add(header_len).and_then(|h| h.checked_add(name_len))
            else {
                break;
            };
            if name_end > buf.len() {
                break; // truncated trailing event: never read past `buf`
            }

            if event.mask & libc::IN_Q_OVERFLOW != 0 {
                out.push_back(Wake::Overflow);
            } else if event.mask & libc::IN_IGNORED != 0 {
                // The kernel invalidated this `wd`: a deleted file or directory, or our own
                // `inotify_rm_watch` (whose bookkeeping is already gone, making this a no-op). No
                // `Wake`: the parent's `IN_DELETE` or the driver's rescan notices the loss.
                //
                // Tested before any other bit. The kernel never ORs it with one (it queues a bare
                // `FS_IN_IGNORED`, and `event_compare` merges nothing into it), but if it did, a
                // wake would name a path this instance just stopped watching.
                if let Some((_, WatchTarget::Dir)) = watches.remove(&event.wd) {
                    // Every path with this `wd`: two spellings of a directory (a symlink, a `.`
                    // component) share one.
                    by_path.retain(|_, held| *held != event.wd);
                }
            } else if let Some((path, target)) = watches.get(&event.wd).cloned() {
                match target {
                    WatchTarget::File => out.push_back(Wake::Data(path)),
                    WatchTarget::Dir if name_len > 0 => {
                        let name_bytes = &buf[i + header_len..name_end];
                        // `len` includes NUL padding to a multiple of 16, so trim at the first
                        // NUL.
                        let end =
                            name_bytes.iter().position(|&b| b == 0).unwrap_or(name_bytes.len());
                        let name = OsStr::from_bytes(&name_bytes[..end]);
                        out.push_back(Wake::Discover(path.join(name)));
                    }
                    WatchTarget::Dir => {
                        // Nameless: `IN_DELETE_SELF` or `IN_MOVE_SELF` on the directory itself.
                        out.push_back(Wake::Discover(path));
                        if event.mask & libc::IN_MOVE_SELF != 0 {
                            // A rename leaves the watch valid on an inode no longer at this
                            // path. Drop it; the `scan` this wake triggers re-arms the path.
                            watches.remove(&event.wd);
                            by_path.retain(|_, held| *held != event.wd);
                            release.push(event.wd);
                        }
                    }
                }
            }
            // Otherwise the `wd` was already unwatched: ignored.

            i = name_end;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// The kernel's name padding (`round_event_name_len`, v6.12
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
        /// A nameless event has `len == 0`, and a named one is padded to a multiple of **16**.
        /// A fixture that pads differently puts a multi-event buffer's later events where the
        /// kernel never would, and can hide a decoder that forgets to skip the name.
        fn kernel_name_len(name: &str) -> usize {
            let header_len = std::mem::size_of::<libc::inotify_event>();
            if name.is_empty() {
                0
            } else {
                (name.len() + 1).div_ceil(header_len) * header_len
            }
        }

        /// One raw `inotify_event` (header plus NUL-padded name) as the kernel writes it.
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

        /// One raw event with an arbitrary `len`, for malformed cases the kernel never produces.
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

        /// `parse_events`, returning the wakes and the release list.
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

        /// `parse` with an empty reverse index, returning only the wakes.
        fn parse_wakes(
            buf: &[u8],
            watches: &mut HashMap<i32, (PathBuf, WatchTarget)>,
        ) -> VecDeque<Wake> {
            parse(buf, watches, &mut HashMap::new()).0
        }

        #[test]
        fn parse_events_fixture_pads_names_the_way_the_kernel_does() {
            // A nameless event is a bare 16-byte header with `len == 0`.
            let nameless = raw_event(7, libc::IN_MODIFY, "");
            assert_eq!(nameless.len(), 16);
            assert_eq!(u32::from_ne_bytes(nameless[12..16].try_into().unwrap()), 0);

            // A name is padded to a multiple of 16 with at least one NUL: "abc123" plus NUL is 16.
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
            // `Data` names the watched file itself, never a joined child path.
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

        /// `wd == -1` without the overflow bit resolves to no watch and is dropped.
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

        /// A directory's `IN_IGNORED` clears `by_path` too, aliases included.
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

        /// A file watch's `IN_IGNORED` leaves `by_path` alone; files are never indexed there.
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

        /// A moved directory is reported, dropped from both indexes, and handed back for
        /// `inotify_rm_watch`, since its watch is still live.
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

        /// `IN_MOVE_SELF` on a file watch (not in `FILE_MASK`) stays a data wake, not a release.
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

        /// `IN_IGNORED` ORed with another bit is only a purge, never also a wake.
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

            // Not deduplicated: `drain` reads the file fully on the first wake, so the rest find
            // nothing. The kernel coalesces identical consecutive unread events, so it never
            // produces this fixture.
            assert_eq!(
                out,
                VecDeque::from([
                    Wake::Data(path.clone()),
                    Wake::Data(path.clone()),
                    Wake::Data(path)
                ])
            );
        }

        /// The advance past each event includes its name. Named, nameless, named, so only a
        /// correct advance decodes all three.
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

        /// A trailing event whose name is cut short is dropped without panicking; the intact
        /// buffer decodes both, so the test can't pass by always dropping the second.
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

        /// The header-fit check's `<=` boundary, from both sides.
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

        /// A `len` whose offset sum would overflow is discarded, on every target.
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

        /// A `len` that isn't a multiple of 16 decodes garbage but never panics or names an
        /// unwatched path.
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

        /// Every truncation and a few thousand bit flips of a valid buffer never panic, always
        /// terminate, and never name a watch outside the map.
        #[test]
        fn parse_events_survives_seeded_truncation_and_bit_flips() {
            // No RNG crate in the workspace; the same `Lcg` as
            // `crates/logit-proto/tests/robustness.rs`.
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

            // Miri is slow; 40 flips still cover every byte of the fixture several times.
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

            // Append rather than `std::fs::write`, whose `O_TRUNC` raises an `IN_MODIFY` of its
            // own.
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

        /// Re-arming a live directory returns the same `wd` and adds no second entry.
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

        /// A directory recreated at the same path gets a new `wd`, and the stale one is dropped.
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

        /// `watch_dir` surfaces `ENOENT` for a missing path and `ENOTDIR` (from `IN_ONLYDIR`)
        /// for a regular file, recording nothing.
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

        /// `unwatch_dir` (no production caller) clears both indexes and allows a re-arm.
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

        /// A broken fd yields one `Wake::Dead`, then parks (see `InotifyWatcher::dead`). Uses a
        /// pipe whose write end is closed, so `read(2)` returns `0` deterministically.
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
            drop(write_end); // every read on `read_end` is now an immediate EOF

            let mut watcher =
                InotifyWatcher::with_init(move || Ok(read_end)).expect("with_init should succeed");

            let wake = tokio::time::timeout(std::time::Duration::from_secs(5), watcher.next_wake())
                .await
                .expect("a broken fd must be reported promptly, not hang");
            let Wake::Dead(reason) = wake else { panic!("expected Wake::Dead, got {wake:?}") };
            assert!(reason.contains("end-of-file"), "got: {reason}");

            // Reported once; from here it never resolves again.
            let again =
                tokio::time::timeout(std::time::Duration::from_millis(250), watcher.next_wake())
                    .await;
            assert!(again.is_err(), "a dead watcher must park, not report again: {again:?}");
        }
    }
}
