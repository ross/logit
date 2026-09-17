//! Shared connection-level plumbing for this crate's `hyper`-based listeners -- the idle-timeout
//! tracker ([`Activity`], [`InFlight`]) and the connection driver that acts on it
//! ([`drive_with_idle`]).
//!
//! Hoisted verbatim out of `crate::otlp` (`docs/plans/prometheus-remote-write.md` W3), where these
//! three landed with `otlp_in`'s `idle_timeout:` field, once `prometheus_in`'s remote-write
//! receiver became a second HTTP listener needing exactly the same semantics. Nothing here is
//! OTLP-specific and nothing here changed in the move; `otlp_in`'s module doc (its "Idle timeout"
//! section) is still where the *reasoning* lives -- why the clock is tracked at the service rather
//! than around the socket, why it resets on request completion rather than on bytes, and the
//! pinned-hyper evidence behind the `graceful_shutdown`-then-bounded-grace-then-drop close
//! sequence. This module doc deliberately points there rather than duplicating it, so there is one
//! copy to keep true.
//!
//! What did *not* hoist is `serve_connection`: it dispatches on `otlp_in`'s own `protocol:`
//! (HTTP via [`hyper_util::server::conn::auto`], gRPC via `hyper::server::conn::http2`) and wires
//! in that input's own handlers, so it is a listener's own code rather than shared plumbing. A
//! second listener builds its own service and its own connection future and hands the result to
//! [`drive_with_idle`], which is the actual seam.

use logit_core::Telemetry;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// One connection's idle state, shared between its service and [`drive_with_idle`] -- the
/// service-level tracker `crate::otlp`'s "Idle timeout" doc section explains, and deliberately not
/// a timer wrapped around the socket.
pub(crate) struct Activity {
    /// Requests hyper has handed this connection's service and not yet had a response from.
    /// While it is non-zero there is no idle deadline at all: the connection is not quiet, it is
    /// working (and the work may be a `Fanout::send` parked on a full downstream, which must
    /// never look like a silent peer -- `docs/adr/idle-connection-timeout.md`'s reset rule).
    in_flight: AtomicUsize,
    /// When the last request finished, i.e. when the clock was last re-armed. A plain
    /// `std::sync::Mutex` and never held across an await: the critical section is one `Instant`
    /// read or write.
    last_progress: Mutex<tokio::time::Instant>,
    /// Set by a handler whose request body stalled: close this connection as soon as its
    /// response is out, rather than leaving it to the idle deadline.
    close_after: AtomicBool,
    /// Wakes [`drive_with_idle`] whenever any of the three above changed, so a request
    /// completing re-arms the deadline and a `close_after` is acted on promptly rather than at
    /// the next deadline.
    changed: tokio::sync::Notify,
}

impl Activity {
    pub(crate) fn new() -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            last_progress: Mutex::new(tokio::time::Instant::now()),
            close_after: AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        }
    }

    /// Marks one request as started, returning the guard whose `Drop` marks it finished.
    pub(crate) fn enter(self: &Arc<Self>) -> InFlight {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        InFlight(Arc::clone(self))
    }

    fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    fn last_progress(&self) -> tokio::time::Instant {
        *self.last_progress.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Stamps "the connection just finished something" -- the only thing that re-arms the clock.
    fn stamp_progress(&self) {
        // `into_inner` on poison rather than `unwrap`: this mutex only ever holds an `Instant`,
        // so a poisoned one still holds a usable value, and this runs inside [`InFlight::drop`],
        // where panicking a second time during an unwind would abort the process.
        let mut last = self.last_progress.lock().unwrap_or_else(PoisonError::into_inner);
        *last = tokio::time::Instant::now();
    }

    /// The body-stall path's "close this connection once my response is out".
    pub(crate) fn request_close(&self) {
        self.close_after.store(true, Ordering::SeqCst);
        self.changed.notify_one();
    }

    fn close_requested(&self) -> bool {
        self.close_after.load(Ordering::SeqCst)
    }
}

/// Held for one request's lifetime by the service wrapper a listener builds around its handler
/// (`crate::otlp`'s `serve_connection`, `crate::prometheus`'s own). A guard rather
/// than a pair of calls around the handler so that every way out of a handler -- an early
/// `return` on a 415, a `?`, a panic unwinding through it -- still decrements the count and
/// re-arms the clock. Stamping progress *here*, when the handler has returned, is what keeps time
/// spent blocked in `Fanout::send` from ever counting against the peer.
pub(crate) struct InFlight(Arc<Activity>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.stamp_progress();
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_one();
    }
}

/// Polls one hyper connection future to completion, closing it if [`Activity`] says it has been
/// idle for `idle` (or if a handler asked for a close after a stalled body). `shutdown` is the
/// connection's own `graceful_shutdown`, passed in because `auto::Connection` and
/// `http2::Connection` share the signature (`self: Pin<&mut Self>`) but no trait.
///
/// With `idle: None` this is `conn.await` and nothing else -- the pre-`idle_timeout` path,
/// unchanged. Otherwise the connection is raced against its own idle deadline; see `crate::otlp`'s
/// "Idle timeout" doc section for the semantics and the hyper evidence behind the close sequence.
///
/// **`conn` is polled the whole time, including while waiting for an in-flight request to
/// finish.** For h1 a handler's future is polled *inside* this connection future, so pausing it
/// to wait on `changed` alone would stall the very request being waited on -- a deadlock, since
/// only that request finishing can send the notification.
pub(crate) async fn drive_with_idle<C, E>(
    conn: C,
    shutdown: impl FnOnce(Pin<&mut C>),
    activity: &Activity,
    idle: Option<std::time::Duration>,
    grace: std::time::Duration,
    telemetry: &Telemetry,
) -> Result<(), String>
where
    C: Future<Output = Result<(), E>>,
    E: std::fmt::Display,
{
    let mut conn = std::pin::pin!(conn);
    let Some(idle) = idle else {
        return conn.await.map_err(|e| e.to_string());
    };

    loop {
        if activity.in_flight() > 0 {
            // Working, so no deadline applies -- but keep polling, and wake when the count
            // changes so the deadline can be re-armed from the instant that request finished.
            tokio::select! {
                result = conn.as_mut() => return result.map_err(|e| e.to_string()),
                () = activity.changed.notified() => continue,
            }
        }
        if activity.close_requested() {
            break;
        }
        // `checked_add` because `last_progress + idle` can overflow for an absurd (but legal)
        // `idle_timeout`, and rule 53 caps nothing above `0s`; the fallback never arrives.
        let deadline =
            activity.last_progress().checked_add(idle).unwrap_or_else(crate::tcp::far_future);
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::select! {
            result = conn.as_mut() => return result.map_err(|e| e.to_string()),
            // Both arms loop back round rather than deciding anything here: the deadline is
            // recomputed from the *current* `last_progress` at the top, so a request that
            // finished while this slept simply moves the deadline out instead of closing.
            () = tokio::time::sleep_until(deadline) => continue,
            () = activity.changed.notified() => continue,
        }
    }

    // Idle (or a stalled body asked for this). Ask hyper to close, give it `grace` to do so, and
    // then drop the connection whatever that returned -- `graceful_shutdown` alone leaves three
    // real cases parked, and the pre-sniff `ReadVersion` resolves `Err("Cancelled")` rather than
    // `Ok(())`, which is why the result is deliberately discarded (`crate::otlp`'s "Idle timeout"
    // doc section). Returning from here is the drop: the socket closes with the pinned future.
    shutdown(conn.as_mut());
    loop {
        if tokio::time::timeout(grace, conn.as_mut()).await.is_ok() {
            break;
        }
        if activity.in_flight() == 0 {
            // Nothing in flight, so the drop costs nothing: this is the case the grace exists
            // for (a `KA::Busy` head, a cancelled pre-sniff, an h2 still handshaking).
            break;
        }
        // A request *started* inside the grace window and its handler has not returned -- most
        // likely parked in `Fanout::send` on a full downstream. Dropping now would discard a
        // batch that never reached the fanout, which is precisely the backpressure-causes-loss
        // outcome this whole feature is built to avoid, so the request is waited out instead:
        // `conn` keeps being polled (on h1 the handler's own future is polled inside it) until
        // the count falls back to zero, and then the grace runs again so the response reaches
        // the wire. A stalled body is still bounded by its own per-frame timeout, and a
        // connection with nothing in flight is closed immediately, so no misbehaving peer can
        // hold this open by staying silent -- only by continuing to be served.
        let mut connection_finished = false;
        while activity.in_flight() > 0 {
            tokio::select! {
                _ = conn.as_mut() => {
                    connection_finished = true;
                    break;
                }
                () = activity.changed.notified() => {}
            }
        }
        if connection_finished {
            break;
        }
    }
    telemetry.count("logit.input.connections.closed", 1.0, &[("reason", "idle")]);
    Ok(())
}
