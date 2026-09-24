//! Connection-level plumbing shared by this crate's `hyper`-based listeners (`otlp_in` and
//! `prometheus_in`'s remote-write receiver): the idle-timeout tracker ([`Activity`],
//! [`InFlight`]), the connection driver that acts on it ([`drive_with_idle`]), and the bounded
//! request-body read.
//!
//! The reasoning lives in `crate::otlp`'s module doc, "Idle timeout" section, and only there: why
//! the clock is tracked at the service rather than around the socket, why it resets on request
//! completion rather than on bytes, and the pinned-hyper evidence behind the
//! `graceful_shutdown`-then-bounded-grace-then-drop close sequence.
//!
//! `serve_connection` is not shared: each listener dispatches on its own protocol (`otlp_in` uses
//! [`hyper_util::server::conn::auto`] for HTTP and `hyper::server::conn::http2` for gRPC) and wires
//! in its own handlers. It builds its own service and connection future and hands them to
//! [`drive_with_idle`], which is the seam.

use bytes::{Bytes, BytesMut};
use http_body_util::{BodyExt, Limited};
use hyper::body::Incoming;
use logit_core::Telemetry;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// One connection's idle state, shared between its service and [`drive_with_idle`].
///
/// A service-level tracker, not a timer around the socket (`crate::otlp`'s "Idle timeout" doc
/// section).
pub(crate) struct Activity {
    /// Requests hyper has handed this connection's service and not yet had a response from.
    /// While it is non-zero there is no idle deadline: the connection is working, and the work may
    /// be a `Fanout::send` parked on a full downstream, which must never look like a silent peer
    /// (`docs/adr/idle-connection-timeout.md`'s reset rule).
    in_flight: AtomicUsize,
    /// When the last request finished, which is when the clock was last re-armed. A
    /// `std::sync::Mutex`, never held across an await: the critical section is one `Instant` read
    /// or write.
    last_progress: Mutex<tokio::time::Instant>,
    /// Set by a handler whose request body stalled: close this connection as soon as its
    /// response is out, rather than leaving it to the idle deadline.
    close_after: AtomicBool,
    /// Wakes [`drive_with_idle`] when any of the three above changes, so a finished request
    /// re-arms the deadline and a `close_after` is acted on now rather than at the next deadline.
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

/// Held for one request's lifetime by the service wrapper a listener builds around its handler.
///
/// A guard rather than a pair of calls so that every way out of a handler (an early `return`, a
/// `?`, a panic unwinding through it) still decrements the count and re-arms the clock. Stamping
/// progress on drop, after the handler has returned, is what keeps time blocked in
/// `Fanout::send` from counting against the peer.
pub(crate) struct InFlight(Arc<Activity>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.stamp_progress();
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        self.0.changed.notify_one();
    }
}

/// Polls one hyper connection future to completion, closing it once [`Activity`] has been idle
/// for `idle` or a handler asked for a close after a stalled body.
///
/// `shutdown` is the connection's own `graceful_shutdown`, passed in because `auto::Connection`
/// and `http2::Connection` share the signature (`self: Pin<&mut Self>`) but no trait. With
/// `idle: None` this is `conn.await`. The close sequence's semantics and hyper evidence are in
/// `crate::otlp`'s "Idle timeout" doc section.
///
/// **`conn` is polled the whole time, including while waiting for an in-flight request to
/// finish.** For h1 a handler's future is polled *inside* this connection future, so waiting on
/// `changed` alone would deadlock: only that request finishing can send the notification.
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
            // Working, so no deadline applies. Keep polling, and wake when the count changes so
            // the deadline re-arms from the instant that request finished.
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
            // Both arms loop rather than deciding here: the deadline is recomputed from the
            // current `last_progress` at the top, so a request that finished during the sleep
            // moves the deadline out instead of closing.
            () = tokio::time::sleep_until(deadline) => continue,
            () = activity.changed.notified() => continue,
        }
    }

    // Idle, or a stalled body asked for this. Ask hyper to close, give it `grace`, then drop the
    // connection whatever that returned: `graceful_shutdown` alone leaves three cases parked, and
    // the pre-sniff `ReadVersion` resolves `Err("Cancelled")` rather than `Ok(())`, so the result
    // is discarded (`crate::otlp`'s "Idle timeout" doc section). Returning is the drop: the socket
    // closes with the pinned future.
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
        // A request started inside the grace window and its handler has not returned, most
        // likely parked in `Fanout::send` on a full downstream. Dropping now would discard a
        // batch that never reached the fanout (backpressure causing loss), so wait it out: keep
        // polling `conn` (on h1 the handler's future is polled inside it) until the count is
        // zero, then run the grace again so the response reaches the wire. A stalled body is
        // still bounded by its per-frame timeout and an empty connection closes immediately, so
        // a peer can hold this open only by continuing to be served, never by staying silent.
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

/// Why reading a request body stopped short.
///
/// Distinguished so the caller answers `408`/gRPC `DEADLINE_EXCEEDED` for a body that stopped
/// arriving, and `413` only for [`Self::Failed`].
pub(crate) enum BodyReadError {
    /// No frame of the body arrived within the per-frame bound.
    Stalled(std::time::Duration),
    /// Anything [`Limited`] reports: over the listener's `MAX_REQUEST_BYTES`, a client vanishing
    /// mid-upload, a reset h2 stream. Render it with [`body_read_error_message`].
    Failed(Box<dyn std::error::Error + Send + Sync>),
}

/// `Limited::collect` with a per-frame stall bound: the body half of `crate::otlp`'s "Idle
/// timeout" doc section.
///
/// The bound is per *frame*, never a total: a large body that keeps arriving in pieces is making
/// progress, however long it takes in aggregate (the distinction `logit_in`'s per-`read` body
/// bound also draws). With `stall: None` this behaves as `limited.collect().await`, including
/// which errors reach [`body_read_error_message`].
pub(crate) async fn collect_with_stall_bound(
    mut body: Limited<Incoming>,
    stall: Option<std::time::Duration>,
) -> Result<Bytes, BodyReadError> {
    // Frames are accumulated rather than concatenated as they arrive so the common single-frame
    // body is handed on without a copy, as `Collected::to_bytes` does.
    let mut frames: Vec<Bytes> = Vec::new();
    loop {
        let next = match stall {
            Some(stall) => match tokio::time::timeout(stall, body.frame()).await {
                Ok(next) => next,
                Err(_elapsed) => return Err(BodyReadError::Stalled(stall)),
            },
            None => body.frame().await,
        };
        let Some(frame) = next else { break };
        let frame = frame.map_err(BodyReadError::Failed)?;
        // Trailers on a request body are legal and carry nothing this input reads; dropping them
        // is what `Collected::to_bytes` does too.
        if let Ok(data) = frame.into_data() {
            frames.push(data);
        }
    }
    Ok(match frames.len() {
        0 => Bytes::new(),
        1 => frames.pop().expect("length checked just above"),
        _ => {
            let mut joined = BytesMut::with_capacity(frames.iter().map(Bytes::len).sum());
            for frame in frames {
                joined.extend_from_slice(&frame);
            }
            joined.freeze()
        }
    })
}

/// Turns a [`Limited`] read failure into a response message that doesn't overclaim.
///
/// `Limited`'s `Error` covers *any* failure reading the body, not only exceeding the size limit: a
/// client disconnecting mid-upload, malformed chunked encoding, or an HTTP/2 stream reset surface
/// the same way. Oversize is recognized by a `LengthLimitError` in the error chain (`Limited`
/// wraps it when the limit trips and otherwise forwards the body's own error). Callers respond
/// `413`/`RESOURCE_EXHAUSTED` either way, having no better status for a body that never
/// finished; only the message distinguishes the two.
pub(crate) fn body_read_error_message(
    err: &(dyn std::error::Error + Send + Sync + 'static),
) -> String {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cause {
        if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() {
            return "request exceeds the maximum allowed size".to_string();
        }
        cause = e.source();
    }
    format!("failed reading the request body (not necessarily oversized): {err}")
}
