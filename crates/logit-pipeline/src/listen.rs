//! The TCP bind every stream listener shares, so a listener-level socket option such as
//! `reuse_port:` is set in one place.
//!
//! - **Same defaults as `tokio::net::TcpListener::bind`.** `SO_REUSEADDR` on unix and a backlog
//!   of 1024, mio's own, so a listener moved onto this helper sees no change beyond the options it
//!   asks for.
//! - **`SO_REUSEPORT` is all-or-nothing per port.** Every socket bound to the port must set it and
//!   belong to the same effective UID, or the later bind fails with `EADDRINUSE`. The kernel then
//!   picks one listener per incoming connection by a hash of its 4-tuple.
//! - **A closing listener drops its accept queue.** A connection the kernel already queued on a
//!   listener that closes is reset, not handed to a sibling, unless the host sets
//!   `net.ipv4.tcp_migrate_req`.

use std::io;
use std::net::SocketAddr;

/// Socket options a listener applies before `bind(2)`.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BindOptions {
    /// Set `SO_REUSEPORT`, so several sockets can bind the same address at once.
    pub reuse_port: bool,
}

/// The listen backlog, mio's default for `TcpListener::bind`.
const BACKLOG: u32 = 1024;

/// Resolves `addr` and binds a listening TCP socket on the first candidate that binds, returning
/// the last candidate's error when none does.
pub async fn bind_tcp(addr: &str, opts: BindOptions) -> io::Result<tokio::net::TcpListener> {
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host(addr).await?.collect();
    bind_tcp_first(&addrs, opts)
}

/// [`bind_tcp`] after resolution, split out so the fallthrough is testable against a hand-built
/// address list.
fn bind_tcp_first(addrs: &[SocketAddr], opts: BindOptions) -> io::Result<tokio::net::TcpListener> {
    let mut last_err = None;
    for &addr in addrs {
        match bind_tcp_one(addr, opts) {
            Ok(listener) => return Ok(listener),
            Err(err) => last_err = Some(err),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        io::Error::new(io::ErrorKind::InvalidInput, "could not resolve to any address")
    }))
}

/// Creates, configures, binds, and listens on one TCP socket at `addr`.
fn bind_tcp_one(addr: SocketAddr, opts: BindOptions) -> io::Result<tokio::net::TcpListener> {
    let socket = if addr.is_ipv4() {
        tokio::net::TcpSocket::new_v4()?
    } else {
        tokio::net::TcpSocket::new_v6()?
    };
    #[cfg(unix)]
    socket.set_reuseaddr(true)?;
    if opts.reuse_port {
        set_reuse_port(&socket)?;
    }
    socket.bind(addr)?;
    socket.listen(BACKLOG)
}

#[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))))]
fn set_reuse_port(socket: &tokio::net::TcpSocket) -> io::Result<()> {
    socket.set_reuseport(true)
}

#[cfg(not(all(
    unix,
    not(any(target_os = "solaris", target_os = "illumos", target_os = "cygwin"))
)))]
fn set_reuse_port(_socket: &tokio::net::TcpSocket) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "SO_REUSEPORT is not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const REUSE: BindOptions = BindOptions { reuse_port: true };

    async fn ephemeral(opts: BindOptions) -> (tokio::net::TcpListener, SocketAddr) {
        let listener = bind_tcp("127.0.0.1:0", opts).await.expect("should bind loopback");
        let addr = listener.local_addr().expect("a bound listener has an address");
        (listener, addr)
    }

    #[tokio::test]
    async fn a_second_bind_without_reuse_port_is_refused() {
        let (_held, addr) = ephemeral(BindOptions::default()).await;
        let err = bind_tcp(&addr.to_string(), BindOptions::default())
            .await
            .expect_err("the port is held");
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "got {err}");
    }

    #[tokio::test]
    async fn two_reuse_port_binds_share_one_port() {
        let (_first, addr) = ephemeral(REUSE).await;
        let second = bind_tcp(&addr.to_string(), REUSE).await.expect("both set SO_REUSEPORT");
        assert_eq!(second.local_addr().unwrap(), addr);
    }

    #[tokio::test]
    async fn a_reuse_port_bind_against_an_unflagged_holder_is_refused() {
        let (_held, addr) = ephemeral(BindOptions::default()).await;
        let err = bind_tcp(&addr.to_string(), REUSE)
            .await
            .expect_err("the holder didn't set SO_REUSEPORT");
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "got {err}");
    }

    #[tokio::test]
    async fn bind_falls_through_to_a_later_candidate() {
        let (_held, occupied) = ephemeral(BindOptions::default()).await;
        let free: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = bind_tcp_first(&[occupied, free], BindOptions::default())
            .expect("the second candidate is free");
        assert_ne!(listener.local_addr().unwrap(), occupied);
    }

    #[tokio::test]
    async fn every_candidate_failing_reports_the_last_error() {
        let (_held, occupied) = ephemeral(BindOptions::default()).await;
        let err = bind_tcp_first(&[occupied], BindOptions::default()).expect_err("held");
        assert_eq!(err.kind(), io::ErrorKind::AddrInUse, "got {err}");
    }

    #[test]
    fn no_candidates_reports_tokios_resolution_error() {
        let err = bind_tcp_first(&[], BindOptions::default()).expect_err("nothing to bind");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(err.to_string(), "could not resolve to any address");
    }

    #[tokio::test]
    async fn a_hostname_is_resolved() {
        let listener = bind_tcp("localhost:0", BindOptions::default())
            .await
            .expect("localhost resolves to a loopback address");
        assert!(listener.local_addr().unwrap().ip().is_loopback());
    }

    /// The kernel clamps `listen()`'s backlog to `net.core.somaxconn`, so the recorded backlog is
    /// the smaller of the two.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn the_backlog_matches_tokios_own_bind() {
        use std::os::fd::AsRawFd;

        let somaxconn: u32 = std::fs::read_to_string("/proc/sys/net/core/somaxconn")
            .expect("somaxconn should be readable")
            .trim()
            .parse()
            .expect("somaxconn is an integer");
        let (listener, _) = ephemeral(BindOptions::default()).await;
        let (depth, backlog) = crate::sockstat::listen_queue(listener.as_raw_fd())
            .expect("TCP_INFO should be readable on a listening socket");
        assert_eq!(depth, 0);
        assert_eq!(backlog, BACKLOG.min(somaxconn));
    }
}
