//! Per-socket kernel counters, read straight off a file descriptor this process already owns.
//!
//! **Why `getsockopt` and not procfs.** The obvious way to learn how many datagrams the kernel
//! dropped on a UDP listener is `/proc/net/udp[6]`'s `drops` column, which is what every
//! operator-facing guide in the field tells you to read. In-process that turns out to be the worse
//! tool: it means reading and parsing a file whose length is the whole netns's socket table, then
//! matching *our* socket in it by local address (and, with `SO_REUSEADDR` or a multicast bind,
//! by inode, since several sockets legitimately share one address). `SO_MEMINFO` (Linux 4.12,
//! `net/core/sock.c`'s `sk_get_meminfo`) answers the same question about one specific fd in one
//! O(1) syscall, with no ambiguity about which socket it described -- and returns the receive
//! buffer's fill level alongside the drop counter, which procfs's `tx_queue:rx_queue` column only
//! approximates. `SK_MEMINFO_DROPS` is the same `sk->sk_drops` that procfs's `drops` column
//! prints, so the two agree by construction.
//!
//! **Crate placement.** `logit-core`, not `logit-inputs`: `logit-outputs` is the foreseeable
//! second consumer (a UDP sink's own `sk_drops` and send-buffer fill), and nothing here knows
//! anything about a listener, a datagram, or a pipeline -- it is a thin, typed reading of four
//! `getsockopt` options, in the same spirit as the rest of this crate's platform-free helpers.
//!
//! **Everything here is Linux-only, and says so by returning `None`.** Each function has a
//! non-Linux twin with an identical signature that reports nothing, so a caller never needs a
//! `cfg` of its own -- the same discipline `crates/logit-inputs/src/tail/watch.rs` uses for
//! inotify. A caller that sees `None` should report once and stop asking (the counters will not
//! start existing later on the same socket).

/// The file descriptor these functions read. `std::os::fd::RawFd` where it exists; its underlying
/// `c_int` where it does not (Windows), purely so the non-Linux twins below keep byte-for-byte the
/// same signatures as the real ones rather than vanishing from the API on some platform.
#[cfg(unix)]
pub type RawFd = std::os::fd::RawFd;
/// See the `cfg(unix)` twin directly above.
#[cfg(not(unix))]
pub type RawFd = std::ffi::c_int;

/// The raw descriptor behind `socket`, for handing to [`meminfo`]/[`listen_queue`] -- `None` on a
/// platform that has no `RawFd` to take one from, which is the only reason this exists rather than
/// callers writing `socket.as_raw_fd()` inline. Keeping it here means the `cfg` lives in this
/// module, with the rest of the platform knowledge, instead of being copied into every caller.
#[cfg(unix)]
pub fn fd_of<S: std::os::fd::AsRawFd>(socket: &S) -> Option<RawFd> {
    Some(socket.as_raw_fd())
}

/// Non-unix twin of [`fd_of`] -- see the `cfg(unix)` version directly above.
#[cfg(not(unix))]
pub fn fd_of<S>(_socket: &S) -> Option<RawFd> {
    None
}

/// One socket's kernel-side memory accounting, as `SO_MEMINFO` reports it.
///
/// Every field is a byte count except [`Self::drops`], which is a packet count. All five are the
/// kernel's own numbers for *this* socket, not a netns-wide or interface-wide total -- which is
/// exactly what makes them attributable to one `logit` component.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SockMeminfo {
    /// `sk->sk_rmem_alloc`: bytes currently charged to this socket's receive queue. **Not** the
    /// sum of the queued datagrams' payloads -- the kernel charges each packet's `skb->truesize`,
    /// which includes the `sk_buff` itself and its shared info, so a queue of small datagrams
    /// charges several hundred bytes apiece. Compare it only against [`Self::rcvbuf`], never
    /// against a payload figure (see [`SockMeminfo::receive_utilization`]).
    pub rmem_alloc: u32,
    /// `sk->sk_rcvbuf`: the ceiling `rmem_alloc` is tested against. This is the **doubled** value
    /// Linux stores for a `SO_RCVBUF` request (`sock_setsockopt` does
    /// `sk_rcvbuf = max(2 * requested, SOCK_MIN_RCVBUF)`), and it is the same number
    /// `getsockopt(SO_RCVBUF)` returns -- so it is directly comparable with
    /// `logit.input.receive_buffer.bytes`, and *not* with what an operator asked for.
    pub rcvbuf: u32,
    /// `sk->sk_wmem_alloc`: bytes charged to this socket's send side. For a UDP sender this is
    /// ~always 0 when sampled -- a datagram is charged and uncharged inside one `sendmsg` -- which
    /// is why `logit` gauges the receive side and counts send *errors* instead
    /// (`docs/known-gaps.md`). Carried here because the option returns it anyway and a TCP or
    /// disk-spooled consumer would want it.
    pub wmem_alloc: u32,
    /// `sk->sk_sndbuf`, the send-side twin of [`Self::rcvbuf`], doubled the same way.
    pub sndbuf: u32,
    /// `sk->sk_drops`: packets this socket lost because the kernel could not charge them to
    /// `rmem_alloc` (plus, on a UDP socket, a handful of rarer causes -- a failed checksum, a
    /// filter verdict). Free-running and **wrapping**: it is an `atomic_t` read into a `u32`, and
    /// nothing ever resets it for the life of the socket. Feed successive samples through
    /// [`DropCounter::delta`] rather than subtracting them by hand.
    ///
    /// This is byte-for-byte the `drops` column of `/proc/net/udp[6]`, which is what makes a
    /// `logit.input.kernel.drops` total checkable against procfs by hand.
    pub drops: u32,
}

impl SockMeminfo {
    /// `rmem_alloc / rcvbuf` -- the kernel's *own* comparable pair, and precisely the ratio it
    /// evaluates when it decides to drop: `__udp_enqueue_schedule_skb` (`net/ipv4/udp.c`) charges
    /// the incoming `skb->truesize` to `sk_rmem_alloc` and discards the packet if the result
    /// exceeds `sk_rcvbuf`. So 1.0 here is not "nearly full, keep an eye on it" -- it is the exact
    /// point drops begin.
    ///
    /// The pairing matters more than it looks. Three plausible-looking ratios are all *wrong*:
    /// `rmem_alloc` over the operator's requested `receive_buffer_bytes` (off by Linux's factor
    /// of two, so it reads ~2x high and hits 1.0 while the buffer is half empty); queued *payload*
    /// bytes over `rcvbuf` (payload is far smaller than the charged `truesize`, so it reads low
    /// and never reaches 1.0 even while the socket is dropping); and anything mixing a `SO_RCVBUF`
    /// getsockopt with a payload numerator, which gets both errors at once. Taking both numbers
    /// from the same `SO_MEMINFO` read is what rules all three out.
    ///
    /// `None` when `rcvbuf` is 0 -- no kernel reports that for a live socket, but the division is
    /// not this function's to guess at.
    pub fn receive_utilization(&self) -> Option<f64> {
        (self.rcvbuf > 0).then(|| f64::from(self.rmem_alloc) / f64::from(self.rcvbuf))
    }
}

/// Reads `SO_MEMINFO` for `fd`. `None` if the option is unsupported (Linux older than 4.12, or a
/// non-Linux build), if `fd` is not a socket, or if the kernel returned fewer fields than
/// `SK_MEMINFO_DROPS` needs -- in every one of those cases the counters simply do not exist, and
/// the caller should report once and stop sampling rather than retry.
#[cfg(target_os = "linux")]
pub fn meminfo(fd: RawFd) -> Option<SockMeminfo> {
    // `SK_MEMINFO_VARS` (`include/uapi/linux/sock_diag.h`) has been 9 since `SK_MEMINFO_DROPS`
    // was added in 4.12, and the kernel truncates its copy to whatever length we pass, writing
    // the truncated length back through `optlen` -- so a shorter reply is a real "this kernel
    // has fewer fields", not a buffer we under-sized.
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
        return None;
    }
    let needed = (libc::SK_MEMINFO_DROPS as usize + 1) * std::mem::size_of::<u32>();
    if (len as usize) < needed {
        return None;
    }
    Some(SockMeminfo {
        rmem_alloc: raw[libc::SK_MEMINFO_RMEM_ALLOC as usize],
        rcvbuf: raw[libc::SK_MEMINFO_RCVBUF as usize],
        wmem_alloc: raw[libc::SK_MEMINFO_WMEM_ALLOC as usize],
        sndbuf: raw[libc::SK_MEMINFO_SNDBUF as usize],
        drops: raw[libc::SK_MEMINFO_DROPS as usize],
    })
}

/// Non-Linux twin of [`meminfo`] -- `SO_MEMINFO` is a Linux-only socket option.
#[cfg(not(target_os = "linux"))]
pub fn meminfo(_fd: RawFd) -> Option<SockMeminfo> {
    None
}

/// The accept queue of a **listening** TCP socket: `(depth, backlog)`, where `depth` is how many
/// completed connections are waiting for an `accept()` right now and `backlog` is the ceiling the
/// `listen(2)` call set. `None` for a non-Linux build, a failed `getsockopt`, a reply too short to
/// contain both fields, or a socket that is not in `LISTEN`.
///
/// **Why `TCP_INFO` reports an accept queue at all.** Both numbers are read out of fields that
/// mean something else entirely on an established connection. `tcp_get_info` (`net/ipv4/tcp.c`)
/// fills `tcpi_state` and then, for a listener, takes an early return that deliberately aliases
/// two of them -- the kernel's own comment there reads:
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
/// `sk_ack_backlog`/`sk_max_ack_backlog` are the same pair `ss -lt` prints as a listening socket's
/// `Recv-Q`/`Send-Q`, which is how an operator can check this against the shell. Because the
/// aliasing holds *only* in `LISTEN`, this function verifies `tcpi_state` itself rather than
/// trusting its caller: on an established socket those fields are real unacked/SACKed segment
/// counts, and reporting them as an accept queue would be silently wrong rather than merely
/// unavailable.
#[cfg(target_os = "linux")]
pub fn listen_queue(fd: RawFd) -> Option<(u32, u32)> {
    /// `TCP_LISTEN` from the kernel's `include/net/tcp_states.h` enum. Not in `libc` for Linux
    /// (only the Hurd module defines a `TCP_LISTEN`), and part of a stable UAPI enum -- `ss`,
    /// `netstat` and every `tcp_info` consumer depend on these values not moving.
    const TCP_LISTEN: u8 = 10;

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
        return None;
    }
    // The kernel copies `min(len, its own sizeof(tcp_info))` bytes and writes that length back, so
    // a build whose `tcp_info` is newer/larger than the running kernel's gets a short reply. Both
    // fields this reads sit early in a struct that has only ever been appended to, but check
    // rather than assume: reading past what was filled would report a zeroed field as a real one.
    let filled = std::mem::offset_of!(libc::tcp_info, tcpi_sacked) + std::mem::size_of::<u32>();
    if (len as usize) < filled {
        return None;
    }
    if info.tcpi_state != TCP_LISTEN {
        return None;
    }
    Some((info.tcpi_unacked, info.tcpi_sacked))
}

/// Non-Linux twin of [`listen_queue`] -- `TCP_INFO`'s listener aliasing is a Linux behavior.
#[cfg(not(target_os = "linux"))]
pub fn listen_queue(_fd: RawFd) -> Option<(u32, u32)> {
    None
}

/// Turns the free-running, wrapping [`SockMeminfo::drops`] into the per-interval delta a counter
/// metric wants.
///
/// **The first sample reports its absolute value, it does not merely establish a baseline.** The
/// tempting default -- return 0 the first time, so a counter sampled off a socket somebody else
/// opened does not spike on startup -- would be wrong for the only sockets `logit` samples: it
/// opens every one of them itself, at bind, and a socket's `sk_drops` is zero at birth. So the
/// first sample's absolute value *is* a real delta, measured from a known zero, and discarding it
/// would silently lose exactly the drops that a listener is most likely to suffer -- the ones in
/// the window between `Input::bind` opening the socket and the run loop's first sample, while the
/// process is still binding its other listeners and building its pipeline, with traffic already
/// arriving at a socket nothing is reading yet.
///
/// The cost of the choice, stated plainly: a future caller that samples a socket it did *not*
/// open from scratch (an inherited fd, systemd socket activation) would attribute that socket's
/// entire history to its first interval. Nothing in `logit` does that today, and such a caller
/// should not reuse this type as-is.
///
/// Wrapping is handled by `wrapping_sub`, which is correct for any real interval: `sk_drops` is a
/// 32-bit free-running counter, so a wrap between two samples yields the true delta as long as
/// fewer than 2^32 drops occurred in between -- at a sampling interval of a second, that is a
/// packet rate no socket reaches.
#[derive(Debug, Clone, Copy, Default)]
pub struct DropCounter {
    /// `None` until the first [`Self::delta`] call. Read the type doc above before "fixing" the
    /// first-sample behavior: `None` is deliberately treated as the socket's birth value of zero,
    /// not as "no baseline yet, report nothing."
    last: Option<u32>,
}

impl DropCounter {
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of drops since the previous sample -- or, on the first call, since the socket
    /// was created. See the type doc for why those are the same thing here.
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

    /// The kernel counters actually come back on a real socket -- the one thing a pure unit test
    /// of [`DropCounter`] cannot tell us, and the assumption every gauge built on
    /// [`meminfo`] rests on.
    #[cfg(target_os = "linux")]
    #[test]
    fn meminfo_reads_a_real_bound_udp_socket() {
        use std::os::fd::AsRawFd;

        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("should bind loopback");
        let info = meminfo(socket.as_raw_fd())
            .expect("SO_MEMINFO should be readable on a socket this process just opened");
        assert!(info.rcvbuf > 0, "a live socket always has a receive-buffer ceiling: {info:?}");
        assert_eq!(info.drops, 0, "nothing has been sent to this socket yet");
        assert_eq!(
            info.receive_utilization(),
            Some(f64::from(info.rmem_alloc) / f64::from(info.rcvbuf)),
            "utilization is exactly the kernel's own rmem_alloc/rcvbuf pair"
        );
    }

    /// A non-socket fd must report nothing rather than garbage -- the `None` path every caller's
    /// "report once and stop sampling" handling depends on.
    #[cfg(target_os = "linux")]
    #[test]
    fn meminfo_of_a_non_socket_fd_is_none() {
        use std::os::fd::AsRawFd;

        let file = std::fs::File::open("/proc/self/status").expect("procfs should be readable");
        assert_eq!(meminfo(file.as_raw_fd()), None);
    }

    /// The `TCP_INFO` listener aliasing, against a real listener: `std::net::TcpListener::bind`
    /// asks for a backlog of its own, so the maximum must come back nonzero, and nothing has
    /// connected yet so the depth must be 0.
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

    /// The state check [`listen_queue`] makes rather than trusting its caller: on a connected
    /// socket `tcpi_unacked`/`tcpi_sacked` are real segment counters, so reporting them as an
    /// accept queue would be wrong rather than merely unavailable.
    #[cfg(target_os = "linux")]
    #[test]
    fn listen_queue_of_a_connected_socket_is_none() {
        use std::os::fd::AsRawFd;

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("should bind loopback");
        let addr = listener.local_addr().expect("a bound listener has an address");
        let client = std::net::TcpStream::connect(addr).expect("loopback connect should succeed");
        let (accepted, _peer) = listener.accept().expect("the connection should be accepted");

        assert_eq!(listen_queue(client.as_raw_fd()), None, "the client side is ESTABLISHED");
        assert_eq!(listen_queue(accepted.as_raw_fd()), None, "so is the accepted side");
    }

    /// A UDP socket is not TCP at all -- `getsockopt(IPPROTO_TCP, TCP_INFO)` fails outright there.
    #[cfg(target_os = "linux")]
    #[test]
    fn listen_queue_of_a_udp_socket_is_none() {
        use std::os::fd::AsRawFd;

        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("should bind loopback");
        assert_eq!(listen_queue(socket.as_raw_fd()), None);
    }
}
