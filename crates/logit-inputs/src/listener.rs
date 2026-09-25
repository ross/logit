//! Bookkeeping every connection-oriented listener shares: the `logit.input.connections` gauge and
//! the accept-error posture (`docs/design/internal-telemetry.md`).
//!
//! The gauge is a drop guard ([`LiveConnection`]) rather than an increment and a decrement around
//! a connection's serving future, so every way a connection task ends, a panic included, brings
//! it back down. tokio runs a task's `poll` under `catch_unwind` and drops the task's future on a
//! panic, which runs the guard's `Drop`; no build profile sets `panic = "abort"`.
//!
//! An accept loop hands every `accept()` error to [`absorb_accept_error`] and ends only on the one
//! it returns. Classifying per error, rather than wrapping the accept call, lets the same helper
//! follow `AcceptQueueSampler::accept` and `UnixListener::accept` unchanged.

use logit_core::{Diagnostics, Telemetry};
use std::io;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// One listener's count of connections holding a permit, and the handle it publishes through.
/// Cloning shares the count, so a listener with more than one accept loop
/// (`datadog_trace_in`'s TCP and Unix sockets) reports one gauge.
#[derive(Clone)]
pub(crate) struct LiveConnections {
    count: Arc<AtomicI64>,
    telemetry: Telemetry,
}

impl LiveConnections {
    pub(crate) fn new(telemetry: Telemetry) -> Self {
        Self { count: Arc::new(AtomicI64::new(0)), telemetry }
    }

    /// Counts one connection in and publishes the new value. The count comes back down when the
    /// returned guard drops.
    pub(crate) fn enter(&self) -> LiveConnection {
        let live = self.count.fetch_add(1, Ordering::Relaxed) + 1;
        self.publish(live);
        LiveConnection(self.clone())
    }

    /// The current count, for a test that holds the handle rather than a `Registry`.
    #[cfg(test)]
    pub(crate) fn count(&self) -> i64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Publishes from the read-modify-write's return value, never a separate `load`:
    /// `Telemetry::gauge` is last-write-wins per key, so two tasks interleaving a change and a load
    /// would leave the stale value published until the next transition.
    fn publish(&self, live: i64) {
        self.telemetry.gauge("logit.input.connections", live as f64, &[]);
    }
}

/// One live connection, held for as long as its task runs. See [`LiveConnections::enter`].
pub(crate) struct LiveConnection(LiveConnections);

impl Drop for LiveConnection {
    fn drop(&mut self) {
        let live = self.0.count.fetch_sub(1, Ordering::Relaxed) - 1;
        self.0.publish(live);
    }
}

/// The pause after a `Resource` or `Other` accept error, so a sustained one (fd exhaustion) can't
/// spin a core. The value `prometheus_out`'s exposition server and the admin server use.
pub(crate) const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// What a failed `accept()` says about the listening socket. The posture is
/// `docs/adr/untrusted-input-bounds.md`'s "An accept error is classified, not propagated";
/// [`classify_accept_error`] holds the table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AcceptErrorClass {
    /// One connection's failure. Retry at once.
    Connection,
    /// The process or kernel is out of something. Back off, then continue.
    Resource,
    /// The listening socket itself is unusable. End the listener.
    Fatal,
    /// Anything unrecognized. Back off, then continue.
    Other,
}

impl AcceptErrorClass {
    /// The `reason` tag on `logit.input.accept.errors`.
    fn reason(self) -> &'static str {
        match self {
            Self::Connection => "connection",
            Self::Resource => "resource",
            Self::Fatal => "fatal",
            Self::Other => "other",
        }
    }
}

/// Classifies one `accept()` error:
///
/// | Class | errno | Portable `ErrorKind` |
/// |---|---|---|
/// | `Connection` | `ECONNABORTED`, `ECONNRESET`, `EINTR`, `EPERM`, `EPROTO`, `EHOSTDOWN`, `ENONET`, `EHOSTUNREACH`, `EOPNOTSUPP`, `ENETDOWN`, `ENETUNREACH` | `ConnectionAborted`, `ConnectionReset`, `Interrupted`, `HostUnreachable`, `NetworkDown`, `NetworkUnreachable`, `PermissionDenied` |
/// | `Resource` | `EMFILE`, `ENFILE`, `ENOBUFS`, `ENOMEM` | `OutOfMemory` |
/// | `Fatal` | `EBADF`, `EINVAL`, `ENOTSOCK`, `EFAULT` | `InvalidInput`, and tokio's runtime-shutdown error |
/// | `Other` | anything else | anything else |
///
/// The `Connection` row follows `man 2 accept`: Linux passes a new socket's pending network errors
/// (`ENETDOWN`, `EPROTO`, `EHOSTDOWN`, `ENONET`, `EHOSTUNREACH`, `EOPNOTSUPP`, `ENETUNREACH`)
/// through `accept`, and a server should treat them like `EAGAIN` and retry. `EPERM`
/// is a firewall rule refusing that one connection.
///
/// tokio retries only `WouldBlock` inside `accept` and returns everything else, so every class
/// here reaches the caller. The errno column applies on Linux only, where `libc` is a dependency;
/// elsewhere an error is classified by its `ErrorKind` alone.
pub(crate) fn classify_accept_error(err: &io::Error) -> AcceptErrorClass {
    use io::ErrorKind;
    match err.kind() {
        ErrorKind::ConnectionAborted
        | ErrorKind::ConnectionReset
        | ErrorKind::Interrupted
        | ErrorKind::HostUnreachable
        | ErrorKind::NetworkDown
        | ErrorKind::NetworkUnreachable
        | ErrorKind::PermissionDenied => return AcceptErrorClass::Connection,
        ErrorKind::OutOfMemory => return AcceptErrorClass::Resource,
        ErrorKind::InvalidInput => return AcceptErrorClass::Fatal,
        _ => {}
    }
    if is_runtime_shutting_down(err) {
        return AcceptErrorClass::Fatal;
    }
    #[cfg(target_os = "linux")]
    if let Some(errno) = err.raw_os_error() {
        match errno {
            libc::ECONNABORTED
            | libc::ECONNRESET
            | libc::EINTR
            | libc::EPERM
            | libc::EPROTO
            | libc::EHOSTDOWN
            | libc::ENONET
            | libc::EHOSTUNREACH
            | libc::EOPNOTSUPP
            | libc::ENETDOWN
            | libc::ENETUNREACH => return AcceptErrorClass::Connection,
            libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM => {
                return AcceptErrorClass::Resource;
            }
            libc::EBADF | libc::EINVAL | libc::ENOTSOCK | libc::EFAULT => {
                return AcceptErrorClass::Fatal;
            }
            _ => {}
        }
    }
    AcceptErrorClass::Other
}

/// tokio 1.x's error for I/O on a runtime that is shutting down (`runtime::io::registration`'s
/// `gone`): `ErrorKind::Other` carrying this message and no errno. No accept on this runtime can
/// succeed again, so it ends the listener.
fn is_runtime_shutting_down(err: &io::Error) -> bool {
    const RUNTIME_SHUTTING_DOWN: &str = "A Tokio 1.x context was found, but it is being shutdown.";
    err.kind() == io::ErrorKind::Other
        && err.raw_os_error().is_none()
        && err.get_ref().is_some_and(|inner| inner.to_string() == RUNTIME_SHUTTING_DOWN)
}

/// Absorbs one `accept()` error per [`classify_accept_error`]: counts
/// `logit.input.accept.errors{reason}`, diagnoses it under the throttled key `accept_error`, and
/// sleeps [`ACCEPT_ERROR_BACKOFF`] for `Resource` and `Other`. Returns the error only for `Fatal`,
/// which the accept loop propagates; on `Ok` the loop accepts again.
///
/// The count and the diagnostic run before the first `.await`, so a caller racing this against
/// shutdown in a `biased` `tokio::select!`, this arm first, still records the error.
pub(crate) async fn absorb_accept_error(
    err: io::Error,
    telemetry: &Telemetry,
    diag: &mut Diagnostics,
) -> io::Result<()> {
    let class = classify_accept_error(&err);
    telemetry.count("logit.input.accept.errors", 1.0, &[("reason", class.reason())]);
    diag.warn_throttled(
        "accept_error",
        format_args!("accepting a connection failed ({} error): {err}", class.reason()),
    );
    match class {
        AcceptErrorClass::Connection => Ok(()),
        AcceptErrorClass::Resource | AcceptErrorClass::Other => {
            tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
            Ok(())
        }
        AcceptErrorClass::Fatal => Err(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::{MetricKind, Registry};

    fn gauge(registry: &Registry) -> Option<f64> {
        registry.drain(0).iter().find_map(|e| {
            e.metrics.iter().find_map(|m| match m.kind {
                MetricKind::Gauge(v)
                    if logit_core::interner::resolve(m.name) == "logit.input.connections" =>
                {
                    Some(v)
                }
                _ => None,
            })
        })
    }

    #[tokio::test]
    async fn a_panicking_connection_task_still_returns_the_gauge_to_zero() {
        let registry = Registry::new();
        let live = LiveConnections::new(registry.telemetry_for("in", "otlp_in", "listener"));

        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let handle = tokio::spawn({
            let live = live.clone();
            async move {
                let _connection = live.enter();
                entered_tx.send(()).unwrap();
                tokio::task::yield_now().await;
                panic!("a connection task panicking mid-serve");
            }
        });
        entered_rx.await.unwrap();
        assert_eq!(gauge(&registry), Some(1.0), "counted in while the task runs");

        assert!(handle.await.unwrap_err().is_panic(), "the task ended in a panic");
        assert_eq!(gauge(&registry), Some(0.0), "the unwind dropped the guard");
    }

    #[test]
    fn clones_share_one_count() {
        let registry = Registry::new();
        let live = LiveConnections::new(registry.telemetry_for("in", "otlp_in", "listener"));
        let first = live.enter();
        let second = live.clone().enter();
        assert_eq!(gauge(&registry), Some(2.0));
        drop(first);
        drop(second);
        assert_eq!(gauge(&registry), Some(0.0));
    }

    #[test]
    fn accept_errors_are_classified_by_errno() {
        use AcceptErrorClass::{Connection, Fatal, Other, Resource};
        #[cfg(target_os = "linux")]
        let by_errno = [
            (libc::ECONNABORTED, Connection),
            (libc::ECONNRESET, Connection),
            (libc::EINTR, Connection),
            (libc::EPERM, Connection),
            (libc::EPROTO, Connection),
            (libc::EHOSTDOWN, Connection),
            (libc::ENONET, Connection),
            (libc::EHOSTUNREACH, Connection),
            (libc::EOPNOTSUPP, Connection),
            (libc::ENETDOWN, Connection),
            (libc::ENETUNREACH, Connection),
            (libc::EMFILE, Resource),
            (libc::ENFILE, Resource),
            (libc::ENOBUFS, Resource),
            (libc::ENOMEM, Resource),
            (libc::EBADF, Fatal),
            (libc::EINVAL, Fatal),
            (libc::ENOTSOCK, Fatal),
            (libc::EFAULT, Fatal),
            (libc::EIO, Other),
            (libc::ELOOP, Other),
        ];
        #[cfg(target_os = "linux")]
        for (errno, class) in by_errno {
            let err = io::Error::from_raw_os_error(errno);
            assert_eq!(classify_accept_error(&err), class, "errno {errno}: {err}");
        }

        let by_kind = [
            (io::ErrorKind::ConnectionAborted, Connection),
            (io::ErrorKind::ConnectionReset, Connection),
            (io::ErrorKind::Interrupted, Connection),
            (io::ErrorKind::HostUnreachable, Connection),
            (io::ErrorKind::NetworkDown, Connection),
            (io::ErrorKind::NetworkUnreachable, Connection),
            (io::ErrorKind::PermissionDenied, Connection),
            (io::ErrorKind::OutOfMemory, Resource),
            (io::ErrorKind::InvalidInput, Fatal),
            (io::ErrorKind::TimedOut, Other),
            (io::ErrorKind::Other, Other),
        ];
        for (kind, class) in by_kind {
            assert_eq!(classify_accept_error(&io::Error::from(kind)), class, "{kind:?}");
        }

        let shutting_down =
            io::Error::other("A Tokio 1.x context was found, but it is being shutdown.");
        assert_eq!(classify_accept_error(&shutting_down), Fatal);
        assert_eq!(classify_accept_error(&io::Error::other("something else")), Other);
    }

    fn accept_errors(registry: &Registry, reason: &str) -> Option<f64> {
        registry.drain(0).iter().find_map(|e| {
            if e.attributes.get("reason").and_then(|v| v.as_str()) != Some(reason) {
                return None;
            }
            e.metrics.iter().find_map(|m| match m.kind {
                MetricKind::Sum(sum)
                    if logit_core::interner::resolve(m.name) == "logit.input.accept.errors" =>
                {
                    Some(sum.value)
                }
                _ => None,
            })
        })
    }

    #[tokio::test]
    async fn only_a_fatal_accept_error_is_returned_and_every_class_is_counted() {
        let registry = Registry::new();
        let telemetry = registry.telemetry_for("in", "otlp_in", "listener");
        let mut diag = Diagnostics::new("in");

        let reset = io::Error::from(io::ErrorKind::ConnectionReset);
        absorb_accept_error(reset, &telemetry, &mut diag).await.unwrap();
        assert_eq!(accept_errors(&registry, "connection"), Some(1.0));

        let fatal = io::Error::from(io::ErrorKind::InvalidInput);
        let returned = absorb_accept_error(fatal, &telemetry, &mut diag).await.unwrap_err();
        assert_eq!(returned.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(accept_errors(&registry, "fatal"), Some(1.0));

        let started = std::time::Instant::now();
        let other = io::Error::other("something else");
        absorb_accept_error(other, &telemetry, &mut diag).await.unwrap();
        assert!(started.elapsed() >= ACCEPT_ERROR_BACKOFF, "an `Other` error backs off");
        assert_eq!(accept_errors(&registry, "other"), Some(1.0));

        assert_eq!(diag.occurrences("accept_error"), 3);
    }
}
