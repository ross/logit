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
/// missed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Wake {
    Discover(PathBuf),
    Data(PathBuf),
    Overflow,
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

    /// Called by `Tailer::reconcile_watches` whenever a directory a pattern's `dir()` used to
    /// reach no longer is. In practice today that's only ever `root` itself going away.
    pub fn unwatch_dir(&mut self, dir: &Path) {
        match self {
            Watcher::Poll => {}
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.unwatch_dir(dir),
        }
    }

    /// Watches one file's own content changes. `None` under [`Watcher::Poll`] (nothing to watch
    /// with) and on a failed `inotify_add_watch` -- both are non-fatal: the file is still tailed,
    /// just without a low-latency data wake, falling fully back to `poll_interval` for it exactly
    /// as `watch: poll` always does. Called once, when `Tailer::open_tracked` opens the file.
    pub fn watch_file(&mut self, path: &Path) -> Option<WatchId> {
        match self {
            Watcher::Poll => None,
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.watch_file(path).ok(),
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
    const EVENT_BUF_BYTES: usize = 64 * 1024;

    /// A directory watch's mask: appearance and departure only, no content events. `root` is the
    /// only directory `docker_in` ever watches with this -- Docker's per-container state
    /// directories are direct children of it, so `IN_CREATE`/`IN_DELETE` alone catch a container
    /// arriving or leaving without needing to also watch what's written inside it.
    /// `IN_DELETE_SELF` covers the watched directory itself disappearing.
    const DIR_MASK: u32 = libc::IN_CREATE
        | libc::IN_MOVED_TO
        | libc::IN_MOVED_FROM
        | libc::IN_DELETE
        | libc::IN_DELETE_SELF;

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
        /// Directory watches only -- the "already watching this path" short-circuit `watch_dir`
        /// uses. File watches never dedupe by path (a rotation reuses the path under a new inode,
        /// which must always get its own fresh watch), so they have no entry here.
        by_path: HashMap<PathBuf, i32>,
        buf: Vec<u8>,
        /// One `read()` can (and often does) carry more than one event -- drained one at a time
        /// by [`InotifyWatcher::next_wake`] before this reads again.
        pending: VecDeque<Wake>,
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
            })
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

        pub fn watch_dir(&mut self, dir: &Path) -> io::Result<()> {
            if self.by_path.contains_key(dir) {
                return Ok(()); // already watching -- inotify_add_watch on the same path is a
                               // harmless no-op too, but this skips the syscall entirely
            }
            let wd = self.add_watch(dir, DIR_MASK)?;
            self.watches.insert(wd, (dir.to_path_buf(), WatchTarget::Dir));
            self.by_path.insert(dir.to_path_buf(), wd);
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

        pub async fn next_wake(&mut self) -> Wake {
            loop {
                if let Some(wake) = self.pending.pop_front() {
                    return wake;
                }
                let mut guard = match self.fd.readable().await {
                    Ok(guard) => guard,
                    // The fd itself is broken (extremely unlikely -- would mean the kernel
                    // closed it out from under this process); nothing left to wait on.
                    Err(_) => return std::future::pending().await,
                };
                let ptr = self.buf.as_mut_ptr();
                let cap = self.buf.len();
                // SAFETY: `ptr`/`cap` describe `self.buf`'s own live allocation, valid for
                // writes of up to `cap` bytes; `inner` is the same fd `readable()` just reported
                // ready, read non-blocking (`IN_NONBLOCK`, set at `open_inotify`).
                let read = guard.try_io(|inner| {
                    let n =
                        unsafe { libc::read(inner.as_raw_fd(), ptr.cast::<libc::c_void>(), cap) };
                    if n < 0 {
                        Err(io::Error::last_os_error())
                    } else {
                        Ok(n as usize)
                    }
                });
                match read {
                    Ok(Ok(n)) => {
                        parse_events(&self.buf[..n], &mut self.watches, &mut self.pending);
                    }
                    Ok(Err(_)) => {} // a read error other than would-block -- try again next wake
                    Err(_would_block) => {} // readiness was stale; loop back to `readable().await`
                }
            }
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
    /// that, and is what the unit tests below exercise directly. Takes `watches` by `&mut` (not
    /// `&`), which every caller reads as immutable everywhere else: this is the one place that
    /// mutates it, purging an `IN_IGNORED`'d watch descriptor's entry immediately rather than
    /// waiting for the driver's own close path to call `unwatch` -- `wd` values are small
    /// integers the kernel reuses, so a stale entry left behind risks a later, unrelated watch
    /// being misattributed to this one.
    fn parse_events(
        buf: &[u8],
        watches: &mut HashMap<i32, (PathBuf, WatchTarget)>,
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
            if i + header_len + name_len > buf.len() {
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
                watches.remove(&event.wd);
            } else if let Some((path, target)) = watches.get(&event.wd) {
                match target {
                    WatchTarget::File => out.push_back(Wake::Data(path.clone())),
                    WatchTarget::Dir if name_len > 0 => {
                        let name_bytes = &buf[i + header_len..i + header_len + name_len];
                        // `inotify_event`'s `name` is NUL-padded to a 4-byte boundary, not
                        // exactly `strlen`-sized -- trim at the first NUL rather than trusting
                        // `event.len` as the name's real length.
                        let end =
                            name_bytes.iter().position(|&b| b == 0).unwrap_or(name_bytes.len());
                        let name = OsStr::from_bytes(&name_bytes[..end]);
                        out.push_back(Wake::Discover(path.join(name)));
                    }
                    WatchTarget::Dir => {
                        // No name -- e.g. `IN_DELETE_SELF` on the watched directory itself.
                        // Reported as the directory changing, which is accurate: something about
                        // it did.
                        out.push_back(Wake::Discover(path.clone()));
                    }
                }
            }
            // else: an event on a watch descriptor this instance no longer knows about (already
            // unwatched) -- ignored, not an error.

            i += header_len + name_len;
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        /// Builds one raw `inotify_event` (header + NUL-padded name) exactly as the kernel would
        /// write it, for feeding to `parse_events` without a real fd.
        fn raw_event(wd: i32, mask: u32, name: &str) -> Vec<u8> {
            let header_len = std::mem::size_of::<libc::inotify_event>();
            let name_bytes = name.as_bytes();
            // The kernel NUL-pads `name` to a multiple of 4 (`sizeof(struct inotify_event)`'s own
            // alignment), always including at least one NUL terminator.
            let padded_len = (name_bytes.len() + 1).div_ceil(4) * 4;
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

        #[test]
        fn parse_events_reports_a_dir_watch_create_as_discover_joined_with_the_name() {
            let dir = PathBuf::from("/var/lib/docker/containers");
            let mut watches = HashMap::from([(7, (dir.clone(), WatchTarget::Dir))]);
            let buf = raw_event(7, libc::IN_CREATE, "abc123");

            let mut out = VecDeque::new();
            parse_events(&buf, &mut watches, &mut out);

            assert_eq!(out, VecDeque::from([Wake::Discover(dir.join("abc123"))]));
        }

        #[test]
        fn parse_events_reports_a_file_watch_modify_as_data_ignoring_any_name() {
            let path = PathBuf::from("/var/lib/docker/containers/abc/abc-json.log");
            let mut watches = HashMap::from([(7, (path.clone(), WatchTarget::File))]);
            // A file watch's own events never carry a name in practice, but even if one somehow
            // did, `Data` always names the watched file itself, never a joined child path.
            let buf = raw_event(7, libc::IN_MODIFY, "");

            let mut out = VecDeque::new();
            parse_events(&buf, &mut watches, &mut out);

            assert_eq!(out, VecDeque::from([Wake::Data(path)]));
        }

        #[test]
        fn parse_events_reports_q_overflow() {
            let mut watches =
                HashMap::from([(7, (PathBuf::from("/var/log/app"), WatchTarget::Dir))]);
            // IN_Q_OVERFLOW events carry wd == -1 and no name.
            let buf = raw_event(-1, libc::IN_Q_OVERFLOW, "");

            let mut out = VecDeque::new();
            parse_events(&buf, &mut watches, &mut out);

            assert_eq!(out, VecDeque::from([Wake::Overflow]));
        }

        #[test]
        fn parse_events_ignores_an_event_on_an_unknown_watch_descriptor() {
            let mut watches =
                HashMap::from([(7, (PathBuf::from("/var/log/app"), WatchTarget::Dir))]);
            let buf = raw_event(99, libc::IN_CREATE, "app.log");

            let mut out = VecDeque::new();
            parse_events(&buf, &mut watches, &mut out);

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

            let mut out = VecDeque::new();
            parse_events(&buf, &mut watches, &mut out);

            assert_eq!(out, VecDeque::from([Wake::Discover(dir)]));
        }

        #[test]
        fn parse_events_purges_an_ignored_watch_and_emits_no_wake_for_it() {
            let path = PathBuf::from("/var/lib/docker/containers/abc/abc-json.log");
            let mut watches = HashMap::from([(7, (path, WatchTarget::File))]);
            let buf = raw_event(7, libc::IN_IGNORED, "");

            let mut out = VecDeque::new();
            parse_events(&buf, &mut watches, &mut out);

            assert!(out.is_empty(), "IN_IGNORED carries no actionable wake of its own");
            assert!(
                watches.is_empty(),
                "the watch descriptor must be purged immediately, not left for the driver's own \
                 close path to notice -- the kernel can reuse the same wd number for an unrelated \
                 watch"
            );
        }

        #[test]
        fn parse_events_collapses_a_burst_of_modifies_on_one_file_into_one_pending_wake_each() {
            let path = PathBuf::from("/var/lib/docker/containers/abc/abc-json.log");
            let mut watches = HashMap::from([(7, (path.clone(), WatchTarget::File))]);
            let mut buf = raw_event(7, libc::IN_MODIFY, "");
            buf.extend(raw_event(7, libc::IN_MODIFY, ""));
            buf.extend(raw_event(7, libc::IN_MODIFY, ""));

            let mut out = VecDeque::new();
            parse_events(&buf, &mut watches, &mut out);

            // Not de-duplicated by `parse_events` itself -- `Tailer::drain`'s own round-robin
            // already drains a file fully on any one wake, so a second, third, ... `Data(path)`
            // queued right behind the first costs nothing but wake up to an already-drained file.
            assert_eq!(
                out,
                VecDeque::from([
                    Wake::Data(path.clone()),
                    Wake::Data(path.clone()),
                    Wake::Data(path)
                ])
            );
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

        #[test]
        fn with_init_surfaces_the_open_failure_rather_than_panicking() {
            let err =
                InotifyWatcher::with_init(|| Err(io::Error::from(io::ErrorKind::PermissionDenied)))
                    .expect_err(
                        "a failing open should surface as an error, not construct a watcher",
                    );
            assert!(err.to_string().contains("permission"), "got: {err}");
        }
    }
}
