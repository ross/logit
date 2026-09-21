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
//! **Crate placement.** `logit-pipeline`, and the reasoning is worth keeping because two other
//! crates look like better fits until you check them. Not `logit-inputs`, where the only caller
//! lives today: `logit-outputs` is the foreseeable second consumer (a UDP sink's own `sk_drops`,
//! a TCP sink's send-side fill), and an output crate must not depend on an input crate to read a
//! socket counter. Not `logit-core` either, which is where an earlier draft put it for exactly
//! that reason -- that crate's own doc says "no I/O, no pipeline, no protocol codecs live here,"
//! and `docs/design/pipeline-graph.md`'s "Crate layout" section leans on that sentence when it
//! puts socket-level mechanics outside it; a raw `getsockopt` and a `libc` dependency are the
//! first things that would have contradicted both. `logit-pipeline` is where the generic,
//! protocol-free machinery already lives (`BoundedQueue`, `BatchAccumulator`, `Fanout`), both
//! impl crates already depend on it, and it already performs real I/O without claiming otherwise
//! (`disk_queue.rs` writes and fsyncs segment files). Nothing here knows about a listener, a
//! datagram or a node -- it is a thin, typed reading of two `getsockopt` options over a raw fd.
//!
//! The division of labour with `logit-inputs` is the same one this crate draws everywhere else:
//! fd-level *readings* that any component could want are here; opening the socket, setting
//! `SO_RCVBUF` on it and reading datagrams off it stay in `logit-inputs::udp`/`tcp`.
//!
//! **Everything here is Linux-only, and says so by returning [`Unavailable`].** Each function has
//! a non-Linux twin with an identical signature that reports nothing, so a caller never needs a
//! `cfg` of its own -- the same discipline `crates/logit-inputs/src/tail/watch.rs` uses for
//! inotify. A caller that sees an `Err` should report once and stop asking (the counters will not
//! start existing later on the same socket) -- but it should report *what it was told*, which is
//! why the error carries the real `errno` rather than collapsing to a bare `None`.

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

// ---- compile-time tripwires over the two kernel ABIs this module reads --------------------------

/// Both readings below index into a kernel struct by a constant `libc` supplies, and neither
/// mistake is observable at runtime: a `SK_MEMINFO_*` index that moved would read a *different*
/// counter (`BACKLOG` and `OPTMEM` are both 0 on a fresh socket, exactly like `DROPS`), and a
/// `tcp_info` field that moved would read a neighbouring `u32`. A `libc` bump that reorders,
/// renames or re-numbers any of it is a **build failure** here instead.
///
/// The values are the kernel's, not `libc`'s, and they are UAPI:
/// `include/uapi/linux/sock_diag.h` (verified at v6.12) numbers the `SK_MEMINFO_*` enum from 0 in
/// the order below with `SK_MEMINFO_VARS` still 9; `include/uapi/linux/tcp.h` (v6.12) opens
/// `struct tcp_info` with eight `__u8`s and four `__u32`s, putting `tcpi_state` at 0,
/// `tcpi_unacked` at 24 and `tcpi_sacked` at 28 on every architecture (all fields are fixed-width
/// and naturally aligned). The `TCP_LISTEN = 10` that [`listen_queue`] hardcodes is the tenth
/// entry of `include/net/tcp_states.h`'s enum, which starts at `TCP_ESTABLISHED = 1`; it cannot be
/// asserted here because `libc` has no Linux `TCP_LISTEN` to compare against -- that absence is
/// why the constant is written out at all.
///
/// Worth knowing before a `libc` bump: the two bindings already disagree about `tcp_info`'s field
/// *count* while agreeing on its layout. `libc`'s gnu binding omits the kernel's eighth `__u8`
/// (`tcpi_delivery_rate_app_limited`/`tcpi_fastopen_client_fail`) and lets `repr(C)` insert an
/// alignment byte in its place; the musl binding names it. The offsets below hold either way, and
/// asserting them is what keeps a reconciliation of the two from moving anything silently.
#[cfg(target_os = "linux")]
const _: () = {
    assert!(libc::SK_MEMINFO_RMEM_ALLOC == 0);
    assert!(libc::SK_MEMINFO_RCVBUF == 1);
    assert!(libc::SK_MEMINFO_WMEM_ALLOC == 2);
    assert!(libc::SK_MEMINFO_SNDBUF == 3);
    assert!(libc::SK_MEMINFO_DROPS == 8);
    // The buffer `meminfo` passes is `[u32; 9]` -- `SK_MEMINFO_VARS` worth of slots, and the
    // largest index read above has to be one of them.
    assert!(std::mem::size_of::<[u32; 9]>() == 36);
    assert!((libc::SK_MEMINFO_DROPS as usize) < 9);

    assert!(std::mem::offset_of!(libc::tcp_info, tcpi_state) == 0);
    assert!(std::mem::offset_of!(libc::tcp_info, tcpi_unacked) == 24);
    assert!(std::mem::offset_of!(libc::tcp_info, tcpi_sacked) == 28);
    // `listen_queue` reads `tcpi_unacked` and `tcpi_sacked` as a pair, so the earlier of the two
    // must really be the earlier one: its length check is written in terms of `tcpi_sacked` alone.
    assert!(
        std::mem::offset_of!(libc::tcp_info, tcpi_unacked)
            < std::mem::offset_of!(libc::tcp_info, tcpi_sacked)
    );
};

/// Why a per-socket counter read reported nothing.
///
/// The point of the type is the `errno`. Both readings below latch their caller off for good on
/// the first failure -- correct, because none of the reachable causes is transient -- but "stop
/// asking" and "tell the operator this kernel is too old" are different claims, and only one of
/// them is ever checked. An `EBADF` (a closed or reused descriptor: the one cause that is a real
/// bug in `logit` rather than a property of the machine) used to be reported as "SO_MEMINFO needs
/// Linux 4.12 or newer".
#[derive(Debug)]
pub enum Unavailable {
    /// [`fd_of`] had no descriptor to hand back -- a platform with no `RawFd` at all.
    NoDescriptor,
    /// Not a Linux build. The non-Linux twins' only answer.
    NotLinux,
    /// The `getsockopt` failed, and this is what the kernel said. `ENOPROTOOPT` is the
    /// kernel-is-too-old case (and the one a sandbox like gVisor gives for an option it does not
    /// implement); `EBADF`/`ENOTSOCK` mean the descriptor is wrong, not the kernel.
    Syscall(std::io::Error),
    /// The option succeeded but the kernel wrote back fewer bytes than the fields read out of the
    /// reply occupy. Defensive: see the note on [`meminfo`]/[`listen_queue`] for why no kernel
    /// that has the option at all can produce this.
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
    /// Whether the failure was the option itself being refused -- the **only** cause for which
    /// "this kernel is too old" is a fair thing to put in front of an operator. Everything else
    /// (a bad descriptor, a non-socket, a socket in the wrong state) is about this one fd.
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
    /// **It can read above 1.0, and that is not a bug, a race, or a sampling artefact -- it is
    /// where a saturated queue settles.** The kernel's admission rule is "is the queue *already*
    /// over the ceiling?", not "would this packet put it over": a datagram is accepted whenever
    /// the currently-charged total is at or below `sk_rcvbuf`, and its whole `truesize` is then
    /// charged on top. So a full receive queue legitimately holds `sk_rcvbuf` plus one datagram's
    /// `truesize`, and reads above 1.0 for as long as that datagram stays queued -- which, on the
    /// listener this exists to report on, is the entire time it is overloaded.
    ///
    /// Two kernel generations spell that same rule differently, which is worth knowing before
    /// "correcting" any of this against one tree. `__udp_enqueue_schedule_skb`
    /// (`net/ipv4/udp.c`), through v6.6 and back through v5.10, charges first and re-tests
    /// afterwards against a *raised* ceiling:
    ///
    /// ```text
    ///     rmem = atomic_add_return(size, &sk->sk_rmem_alloc);
    ///     if (rmem > (size + (unsigned int)sk->sk_rcvbuf))
    ///             goto uncharge_drop;
    /// ```
    ///
    /// -- so a packet is given back only if the pre-charge total was already over. By v6.12 the
    /// ceiling test is that pre-charge comparison directly, with the charge after it and no
    /// re-test at all:
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
    /// (v6.12 keeps an `uncharge_drop` label, but it is now reached only when `udp_rmem_schedule`
    /// fails, not from the ceiling test.) Both admit on exactly the same condition and both settle
    /// at up to `rcvbuf + truesize`; the older one merely has a brief extra window in which even
    /// more is charged before being given back.
    ///
    /// Values over 1.0 therefore mean "saturated and dropping," which is exactly what they look
    /// like; a caller must not clamp them, and a test must not assert an upper bound of 1.0
    /// against a real socket under load.
    ///
    /// `None` when `rcvbuf` is 0 -- no kernel reports that for a live socket, but the division is
    /// not this function's to guess at.
    pub fn receive_utilization(&self) -> Option<f64> {
        (self.rcvbuf > 0).then(|| f64::from(self.rmem_alloc) / f64::from(self.rcvbuf))
    }
}

/// Reads `SO_MEMINFO` for `fd`. `Err` if the option is unsupported (Linux older than 4.12, or a
/// non-Linux build), if `fd` is not a socket, or if the kernel returned fewer fields than
/// `SK_MEMINFO_DROPS` needs -- in every one of those cases the counters simply do not exist, and
/// the caller should report once (quoting the [`Unavailable`]) and stop sampling rather than
/// retry.
///
/// `SO_MEMINFO` lives in the generic `sock_getsockopt`, so it succeeds for *any* socket, not only
/// a UDP one -- an `AF_UNIX` fd returns that socket's real numbers. There is no "wrong kind of
/// socket" error to expect here, only "not a socket at all".
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

/// [`meminfo`]'s reply, without the syscall: the indexing and the length check, which is the whole
/// of what can be got wrong here and none of what a real kernel will ever exercise.
///
/// **The length check is defensive and has never been observed to fire.** `sock.c`'s `SO_MEMINFO`
/// case does `len = min_t(unsigned int, len, sizeof(meminfo))` and writes the truncated length
/// back, never `EINVAL`; `SK_MEMINFO_VARS` has been 9 since `SO_MEMINFO` was added in v4.12 and
/// UAPI enums only append, so the 36 bytes asked for are the 36 bytes any kernel with the option
/// returns. A future kernel that grows the enum truncates *its* copy to our 36 and the nine
/// stable indices are still right. So the check is there to make a wrong answer impossible rather
/// than to handle a case anybody has seen -- which is exactly why it is split out and unit-tested
/// here instead of left unreachable.
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

/// Non-Linux twin of [`meminfo`] -- `SO_MEMINFO` is a Linux-only socket option.
#[cfg(not(target_os = "linux"))]
pub fn meminfo(_fd: RawFd) -> Result<SockMeminfo, Unavailable> {
    Err(Unavailable::NotLinux)
}

/// The accept queue of a **listening** TCP socket: `(depth, backlog)`, where `depth` is how many
/// completed connections are waiting for an `accept()` right now and `backlog` is the ceiling the
/// `listen(2)` call set (already clamped to `net.core.somaxconn`: `__sys_listen_socket`,
/// `net/socket.c`, does that before `__inet_listen_sk` stores `sk_max_ack_backlog`). `Err` for a
/// non-Linux build, a failed `getsockopt`, a reply too short to contain both fields, or a socket
/// that is not in `LISTEN`.
///
/// **`depth` can legitimately exceed `backlog`, by exactly one.** `sk_acceptq_is_full`
/// (`include/net/sock.h`, v6.12) is `sk_ack_backlog > sk_max_ack_backlog` -- strictly greater,
/// with a kernel comment pointing at commit 64a146513f8f for why it is not `>=` -- and the
/// increment (`sk_acceptq_added`, from `inet_csk_reqsk_queue_add`) happens after the check, with
/// no second test. So a `listen(1)` socket settles at a depth of 2. A consumer must not clamp the
/// ratio, and the point at which the kernel starts refusing is *above* 1.0, not at it.
///
/// Do not hand this an `IPPROTO_MPTCP` descriptor (`logit` never opens one): `TCP_INFO` there is
/// forwarded to the msk, whose `sk_ack_backlog` is not the queue an accept loop drains.
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

/// [`listen_queue`]'s reply, without the syscall: the length check, the state check and the two
/// aliased fields, split out for the same reason [`parse_meminfo`] is -- so the branches a real
/// kernel never takes are still tested.
///
/// **The length check is defensive and unreachable in practice.** `do_tcp_getsockopt` truncates
/// its copy the same way `SO_MEMINFO` does and writes the length back, so a build whose
/// `tcp_info` is newer than the running kernel's gets a short reply -- but the `LISTEN` aliasing
/// this function exists for is present in `tcp_get_info` as far back as v2.6.24, where
/// `struct tcp_info` was already 104 bytes, and the struct has only ever been appended to. No
/// kernel that aliases those fields at all can return fewer than the 32 bytes checked for.
///
/// **The state check is not defensive.** On anything but a listener those two fields are real
/// unacked/SACKed segment counts, so reporting them as an accept queue would be silently wrong
/// rather than merely unavailable. It fires for a real, if unreached, case: a listener that has
/// been `shutdown(SHUT_RD)` leaves `LISTEN` (`inet_shutdown` -> `tcp_disconnect` -> `TCP_CLOSE`),
/// which is worth remembering if graceful listener drain is ever built -- the sampler's latch
/// would take it as permanent.
#[cfg(target_os = "linux")]
fn parse_listen_queue(
    info: &libc::tcp_info,
    len: libc::socklen_t,
) -> Result<(u32, u32), Unavailable> {
    /// `TCP_LISTEN` from the kernel's `include/net/tcp_states.h` enum, which numbers from
    /// `TCP_ESTABLISHED = 1`. Not in `libc` for Linux (only the Hurd module defines a
    /// `TCP_LISTEN`), and part of a stable UAPI enum -- `ss`, `netstat` and every `tcp_info`
    /// consumer depend on these values not moving.
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

/// Non-Linux twin of [`listen_queue`] -- `TCP_INFO`'s listener aliasing is a Linux behavior.
#[cfg(not(target_os = "linux"))]
pub fn listen_queue(_fd: RawFd) -> Result<(u32, u32), Unavailable> {
    Err(Unavailable::NotLinux)
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
        // The send-side pair is read by nothing in the workspace yet -- it is shipped for a sink
        // consumer -- so without this the `SK_MEMINFO_WMEM_ALLOC`/`SK_MEMINFO_SNDBUF` indices
        // could be swapped (or point anywhere) and every test would still pass. `sndbuf` is a
        // ceiling and is always nonzero; `wmem_alloc` is 0 on a socket that has sent nothing,
        // which is what makes the swap detectable.
        assert!(info.sndbuf > 0, "a live socket always has a send-buffer ceiling: {info:?}");
        assert_eq!(info.wmem_alloc, 0, "this socket has sent nothing: {info:?}");
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

    /// A UDP socket is not TCP at all -- `getsockopt(IPPROTO_TCP, TCP_INFO)` fails outright there.
    #[cfg(target_os = "linux")]
    #[test]
    fn listen_queue_of_a_udp_socket_reports_the_syscall_failure() {
        use std::os::fd::AsRawFd;

        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("should bind loopback");
        let err = listen_queue(socket.as_raw_fd()).expect_err("UDP has no TCP_INFO");
        assert!(matches!(err, Unavailable::Syscall(_)), "{err:?}");
    }

    // ---- the reply parsers, where the branches no real kernel reaches live --------------------
    //
    // Both length checks below are unreachable on any kernel that supports the option at all (see
    // `parse_meminfo`/`parse_listen_queue`'s own docs), which is exactly why they -- and the two
    // constants they are written in terms of -- were free to be wrong before this.

    /// Every index read out of the `SO_MEMINFO` reply, pinned to a distinct sentinel so a swapped
    /// or off-by-one index cannot pass. The real-socket test above can only ever see zeros in
    /// four of the five slots.
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

    /// The `needed` constant is `(SK_MEMINFO_DROPS + 1) * 4 = 36`, and both an off-by-one in it
    /// and the deletion of the check itself are visible here.
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
        let err =
            parse_listen_queue(&info, 31).expect_err("31 bytes stops one byte inside tcpi_sacked");
        assert!(matches!(err, Unavailable::ShortReply { len: 31, needed: 32 }), "{err:?}");

        // Every other TCP state means those fields are real segment counters. 1 is
        // TCP_ESTABLISHED, 7 is TCP_CLOSE (what a `shutdown()` listener becomes), 11 is
        // TCP_CLOSING -- the neighbour on the other side of TCP_LISTEN.
        for state in [0u8, 1, 7, 9, 11] {
            info.tcpi_state = state;
            let err = parse_listen_queue(&info, 32)
                .expect_err("only LISTEN aliases those fields onto the accept queue");
            assert!(matches!(err, Unavailable::NotListening { state: s } if s == state), "{err:?}");
        }
    }

    /// The one failure an operator must never be told is their kernel's fault.
    #[cfg(target_os = "linux")]
    #[test]
    fn only_a_refused_option_is_reported_as_an_unsupported_one() {
        let refused = Unavailable::Syscall(std::io::Error::from_raw_os_error(libc::ENOPROTOOPT));
        assert!(refused.is_unsupported_option(), "ENOPROTOOPT really is 'this kernel lacks it'");
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
