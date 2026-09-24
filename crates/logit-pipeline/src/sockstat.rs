//! Per-socket kernel counters, read straight off a file descriptor this process already owns.
//!
//! **`getsockopt`, not procfs.** `/proc/net/udp[6]`'s `drops` column means parsing the whole
//! netns's socket table and matching our socket by address, or by inode when `SO_REUSEADDR` or a
//! multicast bind lets several sockets share one. `SO_MEMINFO` (Linux 4.12, `net/core/sock.c`'s
//! `sk_get_meminfo`) answers for one fd in one O(1) syscall, and returns the receive buffer's fill
//! alongside the drop counter. `SK_MEMINFO_DROPS` is the same `sk->sk_drops` procfs prints.
//!
//! **Why this crate.** Outputs may need these readings too, and must not depend on
//! `logit-inputs`; `logit-core` does no I/O. See
//! `docs/adr/udp-intake-batching-and-socket-visibility.md`, "`sockstat` lives in
//! `logit-pipeline`". This module only reads counters off a raw fd; opening sockets, setting
//! `SO_RCVBUF`, and reading datagrams stay in `logit-inputs::udp`/`tcp`.
//!
//! **Linux-only, reported through [`Unavailable`].** Each function has a non-Linux twin with an
//! identical signature, so a caller needs no `cfg`. On an `Err`, a caller should report once and
//! stop asking (the counters won't appear later on the same socket), quoting the real `errno` the
//! error carries.

/// The file descriptor these functions read: `std::os::fd::RawFd`, or `c_int` where that doesn't
/// exist (Windows), so the non-Linux twins keep the same signatures.
#[cfg(unix)]
pub type RawFd = std::os::fd::RawFd;
/// See the `cfg(unix)` twin directly above.
#[cfg(not(unix))]
pub type RawFd = std::ffi::c_int;

/// The raw descriptor behind `socket`, for [`meminfo`]/[`listen_queue`]. `None` on a platform with
/// no `RawFd`; it exists so that `cfg` lives here rather than in every caller.
#[cfg(unix)]
pub fn fd_of<S: std::os::fd::AsRawFd>(socket: &S) -> Option<RawFd> {
    Some(socket.as_raw_fd())
}

/// Non-unix twin of [`fd_of`].
#[cfg(not(unix))]
pub fn fd_of<S>(_socket: &S) -> Option<RawFd> {
    None
}

// ---- compile-time tripwires over the two kernel ABIs this module reads --------------------------

/// Both readings index a kernel struct by a constant `libc` supplies, and a wrong index is
/// invisible at runtime: a moved `SK_MEMINFO_*` index reads a different counter (`BACKLOG` and
/// `OPTMEM` are 0 on a fresh socket, like `DROPS`), and a moved `tcp_info` field reads a
/// neighboring `u32`. A `libc` bump that renumbers any of it fails the build here.
///
/// The values are kernel UAPI: `include/uapi/linux/sock_diag.h` (v6.12) numbers `SK_MEMINFO_*`
/// from 0 in the order below, `SK_MEMINFO_VARS` still 9; `include/uapi/linux/tcp.h` (v6.12) opens
/// `struct tcp_info` with eight `__u8`s and four `__u32`s, putting `tcpi_state` at 0,
/// `tcpi_unacked` at 24, and `tcpi_sacked` at 28 on every architecture. [`listen_queue`]'s
/// hardcoded `TCP_LISTEN = 10` (from `include/net/tcp_states.h`, which starts at
/// `TCP_ESTABLISHED = 1`) can't be asserted: `libc` has no Linux `TCP_LISTEN`.
///
/// Before a `libc` bump: its gnu binding omits the kernel's eighth `__u8`
/// (`tcpi_delivery_rate_app_limited`/`tcpi_fastopen_client_fail`) and lets `repr(C)` pad in its
/// place, while the musl binding names it. The offsets hold either way, and these asserts keep a
/// reconciliation of the two from moving them.
#[cfg(target_os = "linux")]
const _: () = {
    assert!(libc::SK_MEMINFO_RMEM_ALLOC == 0);
    assert!(libc::SK_MEMINFO_RCVBUF == 1);
    assert!(libc::SK_MEMINFO_WMEM_ALLOC == 2);
    assert!(libc::SK_MEMINFO_SNDBUF == 3);
    assert!(libc::SK_MEMINFO_DROPS == 8);
    // `meminfo`'s buffer is `[u32; 9]` (`SK_MEMINFO_VARS` slots); every index read must fit.
    assert!(std::mem::size_of::<[u32; 9]>() == 36);
    assert!((libc::SK_MEMINFO_DROPS as usize) < 9);

    assert!(std::mem::offset_of!(libc::tcp_info, tcpi_state) == 0);
    assert!(std::mem::offset_of!(libc::tcp_info, tcpi_unacked) == 24);
    assert!(std::mem::offset_of!(libc::tcp_info, tcpi_sacked) == 28);
    // `parse_listen_queue`'s length check covers `tcpi_sacked` alone, so it must be the later.
    assert!(
        std::mem::offset_of!(libc::tcp_info, tcpi_unacked)
            < std::mem::offset_of!(libc::tcp_info, tcpi_sacked)
    );
};

/// Why a per-socket counter read reported nothing.
///
/// A caller stops sampling on the first failure, since no reachable cause is transient, but only
/// [`Unavailable::is_unsupported_option`] justifies telling the operator the kernel is too old. An
/// `EBADF` (a closed or reused descriptor) is a bug in `logit`, not a property of the machine.
#[derive(Debug)]
pub enum Unavailable {
    /// [`fd_of`] had no descriptor: a platform with no `RawFd`.
    NoDescriptor,
    /// Not a Linux build. The non-Linux twins' only answer.
    NotLinux,
    /// The `getsockopt` failed, and this is what the kernel said. `ENOPROTOOPT` is the
    /// kernel-is-too-old case (and the one a sandbox like gVisor gives for an option it does not
    /// implement); `EBADF`/`ENOTSOCK` mean the descriptor is wrong, not the kernel.
    Syscall(std::io::Error),
    /// The option succeeded but the kernel wrote back fewer bytes than the fields read need.
    /// Defensive: no kernel with the option produces this (see `parse_meminfo`/
    /// `parse_listen_queue`).
    ShortReply {
        /// What the kernel wrote back through `optlen`.
        len: usize,
        /// What the fields being read need.
        needed: usize,
    },
    /// [`listen_queue`] only: the socket is not in `LISTEN`, so `tcpi_unacked`/`tcpi_sacked` are
    /// real segment counters rather than the accept queue. Carries the `tcpi_state` seen.
    NotListening {
        /// The `tcp_info` state byte, per `include/net/tcp_states.h` (`TCP_ESTABLISHED` is 1,
        /// `TCP_LISTEN` is 10).
        state: u8,
    },
}

impl Unavailable {
    /// Whether the option itself was refused: the only cause for which "this kernel is too old"
    /// is fair to tell an operator. Every other cause is about this one fd.
    pub fn is_unsupported_option(&self) -> bool {
        match self {
            Self::NotLinux => true,
            #[cfg(target_os = "linux")]
            Self::Syscall(err) => err.raw_os_error() == Some(libc::ENOPROTOOPT),
            _ => false,
        }
    }
}

impl std::fmt::Display for Unavailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoDescriptor => f.write_str("this platform exposes no raw file descriptor"),
            Self::NotLinux => f.write_str("these counters are Linux-only"),
            Self::Syscall(err) => write!(f, "getsockopt failed: {err}"),
            Self::ShortReply { len, needed } => write!(
                f,
                "the kernel returned {len} bytes, fewer than the {needed} these fields occupy"
            ),
            Self::NotListening { state } => {
                write!(f, "the socket is in TCP state {state}, not LISTEN (10)")
            }
        }
    }
}

impl std::error::Error for Unavailable {}

/// One socket's kernel-side memory accounting, as `SO_MEMINFO` reports it.
///
/// Every field is a byte count except [`Self::drops`], a packet count. All are this socket's own
/// numbers, not a netns- or interface-wide total, so they attribute to one component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SockMeminfo {
    /// `sk->sk_rmem_alloc`: bytes charged to this socket's receive queue. Not payload bytes: the
    /// kernel charges each packet's `skb->truesize`, `sk_buff` included, so small datagrams cost
    /// several hundred bytes apiece. Compare it only against [`Self::rcvbuf`].
    pub rmem_alloc: u32,
    /// `sk->sk_rcvbuf`: the ceiling `rmem_alloc` is tested against. It is the doubled value Linux
    /// stores for a `SO_RCVBUF` request (`sk_rcvbuf = max(2 * requested, SOCK_MIN_RCVBUF)`), the
    /// same number `getsockopt(SO_RCVBUF)` returns: comparable with
    /// `logit.input.receive_buffer.bytes`, not with what the operator asked for.
    pub rcvbuf: u32,
    /// `sk->sk_wmem_alloc`: bytes charged to this socket's send side. Nearly always 0 when sampled
    /// on a UDP sender, since a datagram is charged and uncharged inside one `sendmsg`; that is why
    /// `logit` counts send errors instead (`docs/known-gaps.md`). Unread today; kept for a TCP
    /// consumer.
    pub wmem_alloc: u32,
    /// `sk->sk_sndbuf`, the send-side twin of [`Self::rcvbuf`], doubled the same way.
    pub sndbuf: u32,
    /// `sk->sk_drops`: packets lost because the kernel could not charge them to `rmem_alloc` (plus,
    /// on UDP, rarer causes such as a failed checksum or a filter verdict). Free-running and
    /// wrapping (an `atomic_t` read into a `u32`, never reset), so feed samples through
    /// [`DropCounter::delta`]. Equal to `/proc/net/udp[6]`'s `drops` column, so
    /// `logit.input.kernel.drops` can be checked by hand.
    pub drops: u32,
}

impl SockMeminfo {
    /// `rmem_alloc / rcvbuf`: the ratio the kernel evaluates when it decides to drop
    /// (`__udp_enqueue_schedule_skb`, `net/ipv4/udp.c`). 1.0 is where drops begin, not a warning
    /// level.
    ///
    /// Three plausible ratios are wrong: `rmem_alloc` over the requested `receive_buffer_bytes`
    /// (reads about 2x high, since Linux doubles the request); queued payload over `rcvbuf` (reads
    /// low, since payload is far smaller than `truesize`, and never reaches 1.0 while dropping);
    /// and a `SO_RCVBUF` getsockopt over payload, which has both errors. Both numbers come from one
    /// `SO_MEMINFO` read.
    ///
    /// **It reads above 1.0 when saturated; don't clamp it.** The kernel admits a datagram when
    /// the charged total is at or below `sk_rcvbuf`, then charges its whole `truesize` on top, so
    /// a full queue holds `sk_rcvbuf` plus one datagram and reads above 1.0 for as long as the
    /// listener is overloaded. A test must not assert an upper bound of 1.0 against a real socket
    /// under load.
    ///
    /// Two kernel generations spell the rule differently; check both before "correcting" this
    /// against one tree. Through v6.6, back to v5.10, `__udp_enqueue_schedule_skb` charges first
    /// and re-tests against a raised ceiling:
    ///
    /// ```text
    ///     rmem = atomic_add_return(size, &sk->sk_rmem_alloc);
    ///     if (rmem > (size + (unsigned int)sk->sk_rcvbuf))
    ///             goto uncharge_drop;
    /// ```
    ///
    /// so a packet is given back only if the pre-charge total was already over. By v6.12 the test
    /// is that pre-charge comparison, with the charge after it:
    ///
    /// ```text
    ///     rmem = atomic_read(&sk->sk_rmem_alloc);
    ///     rcvbuf = READ_ONCE(sk->sk_rcvbuf);
    ///     if (rmem > rcvbuf)
    ///             goto drop;
    ///     ...
    ///     atomic_add(size, &sk->sk_rmem_alloc);
    /// ```
    ///
    /// (v6.12's `uncharge_drop` label is reached only when `udp_rmem_schedule` fails.) Both admit
    /// on the same condition and settle at up to `rcvbuf + truesize`.
    ///
    /// `None` when `rcvbuf` is 0, which no kernel reports for a live socket.
    pub fn receive_utilization(&self) -> Option<f64> {
        (self.rcvbuf > 0).then(|| f64::from(self.rmem_alloc) / f64::from(self.rcvbuf))
    }
}

/// Reads `SO_MEMINFO` for `fd`. `Err` if the option is unsupported (Linux before 4.12, or a
/// non-Linux build), if `fd` is not a socket, or if the reply is too short for
/// `SK_MEMINFO_DROPS`; the caller should report once and stop sampling.
///
/// `SO_MEMINFO` is in the generic `sock_getsockopt`, so it succeeds for any socket (an `AF_UNIX`
/// fd returns real numbers). The only kind error is "not a socket".
#[cfg(target_os = "linux")]
pub fn meminfo(fd: RawFd) -> Result<SockMeminfo, Unavailable> {
    let mut raw = [0u32; 9];
    let mut len = std::mem::size_of_val(&raw) as libc::socklen_t;
    // SAFETY: `raw` is a live, exclusively-borrowed `[u32; 9]`, so the pointer is valid for
    // writes of exactly the `len` bytes named alongside it; `len` is a live `socklen_t` the
    // kernel may both read and overwrite. `getsockopt` imposes no other precondition -- an
    // invalid or non-socket `fd` is reported as `-1`/`EBADF`, not undefined behavior.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_MEMINFO,
            raw.as_mut_ptr().cast::<libc::c_void>(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(Unavailable::Syscall(std::io::Error::last_os_error()));
    }
    parse_meminfo(&raw, len)
}

/// [`meminfo`]'s reply, without the syscall: the indexing and the length check, split out so they
/// are unit-tested.
///
/// **The length check is defensive.** `sock.c`'s `SO_MEMINFO` case does
/// `len = min_t(unsigned int, len, sizeof(meminfo))` and writes the truncated length back, never
/// `EINVAL`. `SK_MEMINFO_VARS` has been 9 since v4.12 and UAPI enums only append, so any kernel
/// with the option returns the 36 bytes asked for; a future kernel that grows the enum truncates
/// its copy to 36, and the nine indices stay right.
#[cfg(target_os = "linux")]
fn parse_meminfo(raw: &[u32; 9], len: libc::socklen_t) -> Result<SockMeminfo, Unavailable> {
    let needed = (libc::SK_MEMINFO_DROPS as usize + 1) * std::mem::size_of::<u32>();
    if (len as usize) < needed {
        return Err(Unavailable::ShortReply { len: len as usize, needed });
    }
    Ok(SockMeminfo {
        rmem_alloc: raw[libc::SK_MEMINFO_RMEM_ALLOC as usize],
        rcvbuf: raw[libc::SK_MEMINFO_RCVBUF as usize],
        wmem_alloc: raw[libc::SK_MEMINFO_WMEM_ALLOC as usize],
        sndbuf: raw[libc::SK_MEMINFO_SNDBUF as usize],
        drops: raw[libc::SK_MEMINFO_DROPS as usize],
    })
}

/// Non-Linux twin of [`meminfo`]: `SO_MEMINFO` is Linux-only.
#[cfg(not(target_os = "linux"))]
pub fn meminfo(_fd: RawFd) -> Result<SockMeminfo, Unavailable> {
    Err(Unavailable::NotLinux)
}

/// The accept queue of a listening TCP socket: `(depth, backlog)`. `depth` is completed
/// connections waiting for `accept()`; `backlog` is the `listen(2)` ceiling, already clamped to
/// `net.core.somaxconn` (`__sys_listen_socket`, `net/socket.c`, clamps before `__inet_listen_sk`
/// stores `sk_max_ack_backlog`). `Err` for a non-Linux build, a failed `getsockopt`, a reply too
/// short for both fields, or a socket not in `LISTEN`.
///
/// **`depth` can exceed `backlog` by one.** `sk_acceptq_is_full` (`include/net/sock.h`, v6.12) is
/// `sk_ack_backlog > sk_max_ack_backlog`, strictly greater (the kernel comment cites commit
/// 64a146513f8f), and `sk_acceptq_added` increments after the check. A `listen(1)` socket settles
/// at depth 2, so a consumer must not clamp the ratio: the kernel starts refusing above 1.0.
///
/// Don't pass an `IPPROTO_MPTCP` descriptor (`logit` never opens one): `TCP_INFO` there reports
/// the msk, whose `sk_ack_backlog` is not the queue an accept loop drains.
///
/// **Why `TCP_INFO` reports an accept queue.** For a listener, `tcp_get_info` (`net/ipv4/tcp.c`)
/// aliases two fields that mean something else on an established connection:
///
/// ```text
///     if (info->tcpi_state == TCP_LISTEN) {
///             /* listeners aliased fields :
///              * tcpi_unacked -> Number of children ready for accept()
///              * tcpi_sacked  -> max backlog
///              */
///             info->tcpi_unacked = READ_ONCE(sk->sk_ack_backlog);
///             info->tcpi_sacked  = READ_ONCE(sk->sk_max_ack_backlog);
///             return;
///     }
/// ```
///
/// `ss -lt` prints the same pair as a listener's `Recv-Q`/`Send-Q`. The aliasing holds only in
/// `LISTEN`, so this checks `tcpi_state` itself: on any other socket the fields are segment
/// counts, and reporting them as an accept queue would be wrong, not unavailable.
#[cfg(target_os = "linux")]
pub fn listen_queue(fd: RawFd) -> Result<(u32, u32), Unavailable> {
    // SAFETY: `tcp_info` is a plain C struct of integers with no padding invariants and no
    // niches, so an all-zero bit pattern is a valid value of it; the kernel overwrites whatever
    // prefix of it this call fills, and the length check below is what stops us reading a field
    // it did not.
    let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
    let mut len = std::mem::size_of::<libc::tcp_info>() as libc::socklen_t;
    // SAFETY: `info` is a live, exclusively-borrowed `tcp_info`, so the pointer is valid for
    // writes of exactly the `len` bytes named alongside it; `len` is a live `socklen_t` the
    // kernel may both read and overwrite. An invalid or non-TCP `fd` is reported as `-1`.
    let rc = unsafe {
        libc::getsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_INFO,
            std::ptr::from_mut(&mut info).cast::<libc::c_void>(),
            &mut len,
        )
    };
    if rc != 0 {
        return Err(Unavailable::Syscall(std::io::Error::last_os_error()));
    }
    parse_listen_queue(&info, len)
}

/// [`listen_queue`]'s reply, without the syscall: the length check, the state check, and the two
/// aliased fields, split out so they are unit-tested.
///
/// **The length check is defensive.** `do_tcp_getsockopt` truncates its copy and writes the
/// length back, so a build with a newer `tcp_info` than the kernel's gets a short reply. But the
/// `LISTEN` aliasing dates to v2.6.24, when `struct tcp_info` was already 104 bytes and it only
/// grows, so no kernel with the aliasing returns fewer than the 32 bytes checked.
///
/// **The state check is not defensive.** A listener that is `shutdown(SHUT_RD)` leaves `LISTEN`
/// (`inet_shutdown` -> `tcp_disconnect` -> `TCP_CLOSE`). If graceful listener drain is ever
/// built, the sampler's latch would treat that as permanent.
#[cfg(target_os = "linux")]
fn parse_listen_queue(
    info: &libc::tcp_info,
    len: libc::socklen_t,
) -> Result<(u32, u32), Unavailable> {
    /// `TCP_LISTEN` from `include/net/tcp_states.h`, which numbers from `TCP_ESTABLISHED = 1`.
    /// `libc` defines it only for the Hurd. Stable UAPI: `ss` and `netstat` depend on it.
    const TCP_LISTEN: u8 = 10;

    let filled = std::mem::offset_of!(libc::tcp_info, tcpi_sacked) + std::mem::size_of::<u32>();
    if (len as usize) < filled {
        return Err(Unavailable::ShortReply { len: len as usize, needed: filled });
    }
    if info.tcpi_state != TCP_LISTEN {
        return Err(Unavailable::NotListening { state: info.tcpi_state });
    }
    Ok((info.tcpi_unacked, info.tcpi_sacked))
}

/// Non-Linux twin of [`listen_queue`]: `TCP_INFO`'s listener aliasing is Linux behavior.
#[cfg(not(target_os = "linux"))]
pub fn listen_queue(_fd: RawFd) -> Result<(u32, u32), Unavailable> {
    Err(Unavailable::NotLinux)
}

/// Turns the free-running, wrapping [`SockMeminfo::drops`] into the per-interval delta a counter
/// metric wants.
///
/// **The first sample reports its absolute value, not a baseline.** `logit` opens every socket it
/// samples, and `sk_drops` is zero at birth, so the first value is a real delta. Discarding it
/// would lose the drops a listener is most likely to suffer: those between `Input::bind` and the
/// first sample, while the process is still starting and nothing reads the socket.
///
/// A caller sampling a socket it didn't create (an inherited fd, systemd socket activation) would
/// attribute that socket's whole history to its first interval, and must not reuse this type
/// as-is.
///
/// `wrapping_sub` gives the true delta across a wrap as long as fewer than 2^32 drops occur
/// between samples, which at a one-second interval no socket reaches.
#[derive(Debug, Clone, Copy, Default)]
pub struct DropCounter {
    /// `None` until the first [`Self::delta`] call, and treated as the socket's birth value of
    /// zero, not "no baseline yet" (see the type doc).
    last: Option<u32>,
}

impl DropCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of drops since the previous sample, or since the socket was created on the
    /// first call.
    pub fn delta(&mut self, current: u32) -> u64 {
        let previous = self.last.replace(current).unwrap_or(0);
        u64::from(current.wrapping_sub(previous))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_first_sample_reports_its_absolute_value() {
        let mut counter = DropCounter::new();
        assert_eq!(
            counter.delta(7),
            7,
            "a socket logit opened itself had 0 drops at birth, so the first reading is a real \
             delta from a known zero -- not a baseline to be thrown away"
        );
        assert_eq!(counter.delta(7), 0, "and the second sample of the same value adds nothing");
    }

    #[test]
    fn a_first_sample_of_zero_reports_zero() {
        let mut counter = DropCounter::new();
        assert_eq!(counter.delta(0), 0);
    }

    #[test]
    fn successive_samples_report_the_difference() {
        let mut counter = DropCounter::new();
        assert_eq!(counter.delta(10), 10);
        assert_eq!(counter.delta(25), 15);
        assert_eq!(counter.delta(25), 0);
        assert_eq!(counter.delta(26), 1);
    }

    #[test]
    fn a_wrap_past_u32_max_reports_the_true_delta_not_a_huge_one() {
        let mut counter = DropCounter::new();
        assert_eq!(counter.delta(u32::MAX - 2), u64::from(u32::MAX - 2));
        assert_eq!(
            counter.delta(2),
            5,
            "u32::MAX-2 -> ... -> u32::MAX -> 0 -> 1 -> 2 is five drops; a plain subtraction \
             would have reported a nonsense negative or a near-4-billion spike"
        );
    }

    /// `meminfo` reads real counters off a bound UDP socket.
    #[cfg(target_os = "linux")]
    #[test]
    fn meminfo_reads_a_real_bound_udp_socket() {
        use std::os::fd::AsRawFd;

        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("should bind loopback");
        let info = meminfo(socket.as_raw_fd())
            .expect("SO_MEMINFO should be readable on a socket this process just opened");
        assert!(info.rcvbuf > 0, "a live socket always has a receive-buffer ceiling: {info:?}");
        // Nothing else reads the send-side pair. `sndbuf` is always nonzero and `wmem_alloc` is 0
        // on a socket that has sent nothing, so a swap of their indices shows here.
        assert!(info.sndbuf > 0, "a live socket always has a send-buffer ceiling: {info:?}");
        assert_eq!(info.wmem_alloc, 0, "this socket has sent nothing: {info:?}");
        assert_eq!(info.drops, 0, "nothing has been sent to this socket yet");
        assert_eq!(
            info.receive_utilization(),
            Some(f64::from(info.rmem_alloc) / f64::from(info.rcvbuf)),
            "utilization is exactly the kernel's own rmem_alloc/rcvbuf pair"
        );
    }

    /// A non-socket fd reports `ENOTSOCK`, not an unsupported option.
    #[cfg(target_os = "linux")]
    #[test]
    fn meminfo_of_a_non_socket_fd_reports_enotsock() {
        use std::os::fd::AsRawFd;

        let file = std::fs::File::open("/proc/self/status").expect("procfs should be readable");
        let err = meminfo(file.as_raw_fd()).expect_err("a file is not a socket");
        assert!(
            matches!(&err, Unavailable::Syscall(e) if e.raw_os_error() == Some(libc::ENOTSOCK)),
            "the caller must be told it handed over a non-socket, not that its kernel is old: \
             {err:?}"
        );
        assert!(
            !err.is_unsupported_option(),
            "ENOTSOCK is a bug in the caller, so it must not be reported as a missing option"
        );
    }

    /// A real listener reports depth 0 and a nonzero backlog (`TcpListener::bind` sets one).
    #[cfg(target_os = "linux")]
    #[test]
    fn listen_queue_reads_a_real_tcp_listener() {
        use std::os::fd::AsRawFd;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("should bind loopback");
        let (depth, backlog) = listen_queue(listener.as_raw_fd())
            .expect("TCP_INFO should be readable on a listening socket");
        assert_eq!(depth, 0, "nothing has connected, so nothing is waiting to be accepted");
        assert!(backlog > 0, "a listening socket always has a backlog ceiling, got {backlog}");
    }

    /// A connected socket reports `NotListening` with the state it saw.
    #[cfg(target_os = "linux")]
    #[test]
    fn listen_queue_of_a_connected_socket_reports_the_state_it_saw() {
        use std::os::fd::AsRawFd;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("should bind loopback");
        let addr = listener.local_addr().expect("a bound listener has an address");
        let client = std::net::TcpStream::connect(addr).expect("loopback connect should succeed");
        let (accepted, _peer) = listener.accept().expect("the connection should be accepted");

        for (what, fd) in [("client", client.as_raw_fd()), ("accepted", accepted.as_raw_fd())] {
            let err = listen_queue(fd).expect_err("neither side is a listening socket");
            assert!(
                matches!(err, Unavailable::NotListening { state: 1 }),
                "the {what} side is TCP_ESTABLISHED (1), and the caller is told so: {err:?}"
            );
        }
    }

    /// `getsockopt(IPPROTO_TCP, TCP_INFO)` fails outright on a UDP socket.
    #[cfg(target_os = "linux")]
    #[test]
    fn listen_queue_of_a_udp_socket_reports_the_syscall_failure() {
        use std::os::fd::AsRawFd;

        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("should bind loopback");
        let err = listen_queue(socket.as_raw_fd()).expect_err("UDP has no TCP_INFO");
        assert!(matches!(err, Unavailable::Syscall(_)), "{err:?}");
    }

    /// The reply parsers, including branches no real kernel reaches. `script/unsafe-check miri`
    /// runs this module by path, so a test here must not need a real fd.
    mod reply_parsing {
        use super::*;

        /// Every `SO_MEMINFO` index read maps to a distinct sentinel, so a swapped or off-by-one
        /// index fails.
        #[cfg(target_os = "linux")]
        #[test]
        fn a_full_length_meminfo_reply_is_read_field_by_field() {
            // Index:       0    1    2    3    4    5    6    7    8
            let raw: [u32; 9] = [10, 11, 12, 13, 14, 15, 16, 17, 18];
            let info = parse_meminfo(&raw, 36).expect("36 bytes is the full nine-field reply");
            assert_eq!(
                info,
                SockMeminfo {
                    rmem_alloc: 10, // SK_MEMINFO_RMEM_ALLOC
                    rcvbuf: 11,     // SK_MEMINFO_RCVBUF
                    wmem_alloc: 12, // SK_MEMINFO_WMEM_ALLOC
                    sndbuf: 13,     // SK_MEMINFO_SNDBUF
                    drops: 18,      // SK_MEMINFO_DROPS -- *not* BACKLOG (17) or OPTMEM (16)
                }
            );
        }

        /// A reply shorter than `(SK_MEMINFO_DROPS + 1) * 4 = 36` bytes is refused.
        #[cfg(target_os = "linux")]
        #[test]
        fn a_meminfo_reply_short_of_the_drop_counter_is_refused() {
            let raw = [0u32; 9];
            assert!(parse_meminfo(&raw, 36).is_ok(), "the exact length the kernel returns");
            for len in [35, 32, 0] {
                let err = parse_meminfo(&raw, len)
                    .expect_err("a reply too short for the drop counter must be refused");
                assert!(
                    matches!(err, Unavailable::ShortReply { len: got, needed: 36 } if got == len as usize),
                    "a {len}-byte reply must be refused, not read past: {err:?}"
                );
            }
        }

        /// `tcp_info`'s two aliased fields, and the three ways the parse can refuse them.
        #[cfg(target_os = "linux")]
        #[test]
        fn the_listen_queue_is_read_only_from_a_listening_socket_with_both_fields_filled() {
            // SAFETY: `tcp_info` is a plain C struct of integers with no niches, so an all-zero bit
            // pattern is a valid value of it -- the same reasoning `listen_queue` states before its
            // own `getsockopt`.
            let mut info: libc::tcp_info = unsafe { std::mem::zeroed() };
            info.tcpi_state = 10; // TCP_LISTEN
            info.tcpi_unacked = 3; // depth
            info.tcpi_sacked = 5; // backlog

            assert_eq!(
                parse_listen_queue(&info, 32).expect("32 bytes reaches the end of tcpi_sacked"),
                (3, 5),
                "depth is tcpi_unacked and the ceiling is tcpi_sacked, in that order"
            );
            let err = parse_listen_queue(&info, 31)
                .expect_err("31 bytes stops one byte inside tcpi_sacked");
            assert!(matches!(err, Unavailable::ShortReply { len: 31, needed: 32 }), "{err:?}");

            // 1 is TCP_ESTABLISHED, 7 is TCP_CLOSE (a `shutdown()` listener), 9 and 11 are
            // TCP_LISTEN's neighbors.
            for state in [0u8, 1, 7, 9, 11] {
                info.tcpi_state = state;
                let err = parse_listen_queue(&info, 32)
                    .expect_err("only LISTEN aliases those fields onto the accept queue");
                assert!(
                    matches!(err, Unavailable::NotListening { state: s } if s == state),
                    "{err:?}"
                );
            }
        }

        /// The one failure an operator must never be told is their kernel's fault.
        #[cfg(target_os = "linux")]
        #[test]
        fn only_a_refused_option_is_reported_as_an_unsupported_one() {
            let refused =
                Unavailable::Syscall(std::io::Error::from_raw_os_error(libc::ENOPROTOOPT));
            assert!(
                refused.is_unsupported_option(),
                "ENOPROTOOPT really is 'this kernel lacks it'"
            );
            assert!(Unavailable::NotLinux.is_unsupported_option());

            for other in [
                Unavailable::Syscall(std::io::Error::from_raw_os_error(libc::EBADF)),
                Unavailable::Syscall(std::io::Error::from_raw_os_error(libc::ENOTSOCK)),
                Unavailable::NoDescriptor,
                Unavailable::ShortReply { len: 0, needed: 36 },
                Unavailable::NotListening { state: 1 },
            ] {
                assert!(
                    !other.is_unsupported_option(),
                    "a stale descriptor is a bug in logit, not an old kernel: {other:?}"
                );
            }
        }
    }
}
