//! The wake source a [`crate::tail::driver::Tailer`] races against its own poll tick.
//! [`Watcher::Poll`] never resolves on its own ([`Watcher::next_wake`] is `std::future::pending`)
//! -- the driver's own `poll_interval` tick is its only wake source, and racing a future that
//! never completes against it costs nothing. On Linux, [`Watcher::Inotify`] wraps a real
//! `inotify` file descriptor (`inotify::InotifyWatcher`, below) for near-immediate wakeups;
//! `poll_interval` still runs underneath it as reconciliation (a missed rename, an
//! `IN_Q_OVERFLOW`, anything `inotify` didn't report). See
//! `docs/adr/file-tailing-and-docker-json-logs.md`.

use super::WatchMode;
use logit_core::Diagnostics;
use std::path::{Path, PathBuf};

/// Something changed under a watched directory. `Overflow` means the kernel's `inotify` event
/// queue overflowed and some events were lost -- unreachable under [`Watcher::Poll`], which has
/// no queue to overflow; the driver responds to it with a full `scan` rather than trying to
/// reconstruct which specific paths were missed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Wake {
    Changed(PathBuf),
    Overflow,
}

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

    /// Unused until a caller ever needs to stop watching a directory mid-run (`docker_in`'s
    /// container churn, landing with `docker_in` itself) -- present now so `Tailer` has a stable
    /// method to call once it does.
    #[allow(dead_code)]
    pub fn unwatch_dir(&mut self, dir: &Path) {
        match self {
            Watcher::Poll => {}
            #[cfg(target_os = "linux")]
            Watcher::Inotify(w) => w.unwatch_dir(dir),
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
/// (license-blocked by `deny.toml` -- see the ADR's Alternatives). Watches whole directories, not
/// individual files (matching [`crate::tail::pattern::PathPattern`]'s own "scan a directory, match
/// names within it" shape) -- a new file appearing, one being written to, and one disappearing all
/// surface as events on the directory's own watch descriptor.
#[cfg(target_os = "linux")]
mod inotify {
    use super::Wake;
    use std::collections::{HashMap, VecDeque};
    use std::ffi::{CString, OsStr};
    use std::io;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Path, PathBuf};
    use tokio::io::unix::AsyncFd;

    /// One `read()` off the inotify fd -- generously larger than any single burst of events this
    /// driver's own directories (a handful of log files, or Docker's per-host container count)
    /// would ever produce at once; a burst larger than this is exactly what `IN_Q_OVERFLOW` (and
    /// this driver's full-rescan response to it) exists to handle.
    const EVENT_BUF_BYTES: usize = 64 * 1024;

    /// Every event this driver's directories actually need: content changes and appearance
    /// (`IN_MODIFY`, `IN_CLOSE_WRITE`), a file arriving by creation or by rename
    /// (`IN_CREATE`, `IN_MOVED_TO`), and departure by either means (`IN_DELETE`, `IN_MOVED_FROM`),
    /// plus the watched directory itself disappearing (`IN_DELETE_SELF`) -- rotation losing the
    /// old inode, a new file arriving, or a container's whole log directory going away are all
    /// covered by this set; the driver reacts to any of them the same way, with a full `scan`.
    const WATCH_MASK: u32 = libc::IN_MODIFY
        | libc::IN_CREATE
        | libc::IN_MOVED_TO
        | libc::IN_MOVED_FROM
        | libc::IN_DELETE
        | libc::IN_DELETE_SELF
        | libc::IN_CLOSE_WRITE;

    #[derive(Debug)]
    pub(crate) struct InotifyWatcher {
        fd: AsyncFd<OwnedFd>,
        /// Watch descriptor -> the directory it watches, so a raw event (which only carries a wd)
        /// can be turned back into a full path.
        watches: HashMap<i32, PathBuf>,
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

        pub fn watch_dir(&mut self, dir: &Path) -> io::Result<()> {
            if self.by_path.contains_key(dir) {
                return Ok(()); // already watching -- inotify_add_watch on the same path is a
                               // harmless no-op too, but this skips the syscall entirely
            }
            let cpath = CString::new(dir.as_os_str().as_bytes()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "path contains a NUL byte")
            })?;
            // SAFETY: `self.fd`'s inner fd is a valid, open inotify instance for the life of
            // `self`; `cpath` is a valid NUL-terminated C string that outlives this call.
            let wd = unsafe {
                libc::inotify_add_watch(self.fd.get_ref().as_raw_fd(), cpath.as_ptr(), WATCH_MASK)
            };
            if wd < 0 {
                return Err(io::Error::last_os_error());
            }
            self.watches.insert(wd, dir.to_path_buf());
            self.by_path.insert(dir.to_path_buf(), wd);
            Ok(())
        }

        pub fn unwatch_dir(&mut self, dir: &Path) {
            let Some(wd) = self.by_path.remove(dir) else { return };
            self.watches.remove(&wd);
            // SAFETY: `self.fd`'s inner fd is valid; `wd` was returned by a prior successful
            // `inotify_add_watch` on this same fd. A redundant removal (the kernel already
            // dropped this watch itself, e.g. after `IN_DELETE_SELF`) just returns `EINVAL`,
            // which this ignores -- not worth surfacing as an error.
            unsafe {
                libc::inotify_rm_watch(self.fd.get_ref().as_raw_fd(), wd);
            }
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
                    Ok(Ok(n)) => parse_events(&self.buf[..n], &self.watches, &mut self.pending),
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
    /// that, and is what the unit tests below exercise directly.
    fn parse_events(buf: &[u8], watches: &HashMap<i32, PathBuf>, out: &mut VecDeque<Wake>) {
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
            } else if let Some(dir) = watches.get(&event.wd) {
                if name_len > 0 {
                    let name_bytes = &buf[i + header_len..i + header_len + name_len];
                    // `inotify_event`'s `name` is NUL-padded to a 4-byte boundary, not exactly
                    // `strlen`-sized -- trim at the first NUL rather than trusting `event.len`
                    // as the name's real length.
                    let end = name_bytes.iter().position(|&b| b == 0).unwrap_or(name_bytes.len());
                    let name = OsStr::from_bytes(&name_bytes[..end]);
                    out.push_back(Wake::Changed(dir.join(name)));
                } else {
                    // No name -- e.g. `IN_DELETE_SELF` on the watched directory itself. Reported
                    // as the directory changing, which is accurate: something about it did.
                    out.push_back(Wake::Changed(dir.clone()));
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
        fn parse_events_decodes_a_create_and_a_modify_with_names() {
            let dir = PathBuf::from("/var/log/app");
            let watches = HashMap::from([(7, dir.clone())]);
            let mut buf = raw_event(7, libc::IN_CREATE, "app.log");
            buf.extend(raw_event(7, libc::IN_MODIFY, "app.log"));

            let mut out = VecDeque::new();
            parse_events(&buf, &watches, &mut out);

            assert_eq!(
                out,
                VecDeque::from([
                    Wake::Changed(dir.join("app.log")),
                    Wake::Changed(dir.join("app.log")),
                ])
            );
        }

        #[test]
        fn parse_events_reports_q_overflow() {
            let watches = HashMap::from([(7, PathBuf::from("/var/log/app"))]);
            // IN_Q_OVERFLOW events carry wd == -1 and no name.
            let buf = raw_event(-1, libc::IN_Q_OVERFLOW, "");

            let mut out = VecDeque::new();
            parse_events(&buf, &watches, &mut out);

            assert_eq!(out, VecDeque::from([Wake::Overflow]));
        }

        #[test]
        fn parse_events_ignores_an_event_on_an_unknown_watch_descriptor() {
            let watches = HashMap::from([(7, PathBuf::from("/var/log/app"))]);
            let buf = raw_event(99, libc::IN_CREATE, "app.log");

            let mut out = VecDeque::new();
            parse_events(&buf, &watches, &mut out);

            assert!(
                out.is_empty(),
                "an event for a watch this instance doesn't know should be dropped, not panic"
            );
        }

        #[test]
        fn parse_events_on_a_nameless_event_reports_the_directory_itself() {
            let dir = PathBuf::from("/var/log/app");
            let watches = HashMap::from([(7, dir.clone())]);
            let buf = raw_event(7, libc::IN_DELETE_SELF, "");

            let mut out = VecDeque::new();
            parse_events(&buf, &watches, &mut out);

            assert_eq!(out, VecDeque::from([Wake::Changed(dir)]));
        }

        #[tokio::test]
        async fn inotify_watcher_wakes_on_a_child_file_write() {
            let dir = crate::tail::test_support::scratch_dir("inotify-wake");
            let mut watcher =
                InotifyWatcher::new().expect("inotify should be available in the dev container");
            watcher.watch_dir(&dir).expect("watch_dir should succeed");

            let path = dir.join("app.log");
            std::fs::write(&path, b"hello\n").unwrap();

            let wake = tokio::time::timeout(std::time::Duration::from_secs(5), watcher.next_wake())
                .await
                .expect("should wake within 5s");
            assert_eq!(wake, Wake::Changed(path));

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
