//! Connection-level plumbing shared by this crate's `hyper`-based listeners (`otlp_in`,
//! `prometheus_in`'s remote-write receiver, `datadog_in`, and `datadog_trace_in`): the connection
//! builders that pin hyper's HTTP/2 settings ([`auto_builder`], [`h2_builder`]), the idle-timeout
//! tracker ([`Activity`], [`InFlight`]), the connection driver that acts on it
//! ([`drive_with_idle`]), and the bounded request-body read ([`collect_with_stall_bound`], which
//! holds one buffer per body however many reads it arrives in). All four listeners build and
//! read through these. The two Datadog listeners also share their request helpers here:
//! `Content-Encoding` and `Content-Type` parsing, bounded decompression, Datadog's JSON response
//! shapes, and the deadline-bounded delivery.
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
use hyper::server::conn::http2;
use hyper_util::rt::TokioExecutor;
use hyper_util::server::conn::auto;
use logit_core::Telemetry;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// The most concurrent HTTP/2 streams one connection may open: hyper 1.11.1's own server default,
/// pinned here so a hyper upgrade cannot move it.
///
/// A listener's worst case is `MAX_CONCURRENT_CONNECTIONS × MAX_CONCURRENT_STREAMS × 2 ×
/// MAX_REQUEST_BYTES`: a compressed body and its decompressed copy on every stream of every
/// connection. For `otlp_in` that is 1024 × 200 × 2 × 4 MiB = 1.6 TiB, a bound on what peers could
/// make the process try to allocate, not a memory budget
/// (`docs/adr/untrusted-input-bounds.md`'s "HTTP and gRPC listeners" section).
pub(crate) const MAX_CONCURRENT_STREAMS: u32 = 200;

/// Stream resets a peer may cause before they are accepted, after which h2 sends `GOAWAY`: h2
/// 0.4.19's `DEFAULT_REMOTE_RESET_STREAM_MAX`, which hyper applies when this is left unset. The
/// rapid-reset (CVE-2023-44487) bound.
const MAX_PENDING_ACCEPT_RESET_STREAMS: usize = 20;

/// The `SETTINGS_MAX_HEADER_LIST_SIZE` advertised: hyper 1.11.1's server default of 16 KiB.
const MAX_HEADER_LIST_SIZE: u32 = 16 * 1024;

/// The HTTP/1.1-and-h2c builder every `auto` listener serves through, with the h2 settings
/// above set explicitly. `otlp_in`'s HTTP transport, `prometheus_in`'s receiver, `datadog_in`,
/// and `datadog_trace_in` build here.
pub(crate) fn auto_builder() -> auto::Builder<TokioExecutor> {
    let mut builder = auto::Builder::new(TokioExecutor::new());
    builder
        .http2()
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_pending_accept_reset_streams(MAX_PENDING_ACCEPT_RESET_STREAMS)
        .max_header_list_size(MAX_HEADER_LIST_SIZE);
    builder
}

/// The HTTP/2-only builder, for `otlp_in`'s gRPC transport, with the same settings as
/// [`auto_builder`].
pub(crate) fn h2_builder() -> http2::Builder<TokioExecutor> {
    let mut builder = http2::Builder::new(TokioExecutor::new());
    builder
        .max_concurrent_streams(MAX_CONCURRENT_STREAMS)
        .max_pending_accept_reset_streams(MAX_PENDING_ACCEPT_RESET_STREAMS)
        .max_header_list_size(MAX_HEADER_LIST_SIZE);
    builder
}

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
    // connection whatever that returned. `graceful_shutdown` alone leaves two cases parked, an h1
    // head stopped mid-way (`KA::Busy`) and an h2 connection still handshaking, and those spend
    // the grace. A pre-sniff `ReadVersion` does not: `graceful_shutdown` cancels it and the first
    // poll below resolves at once to `Err("Cancelled")`, which is why the result is discarded
    // (`crate::otlp`'s "Idle timeout" doc section). Returning is the drop: the socket closes with
    // the pinned future.
    shutdown(conn.as_mut());
    loop {
        if tokio::time::timeout(grace, conn.as_mut()).await.is_ok() {
            break;
        }
        if activity.in_flight() == 0 {
            // Nothing in flight, so the drop costs nothing: this is the case the grace exists
            // for (a `KA::Busy` head, an h2 still handshaking).
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
/// bound also draws). With `stall: None` no frame is timed, and the same errors reach
/// [`body_read_error_message`].
///
/// **One buffer, not one per frame.** A body of one frame is returned as that frame, with no
/// copy. From the second frame on, every frame is copied into one growing [`BytesMut`] and
/// dropped. Keeping the frames instead would keep hyper's read buffers: on h1 a frame is a slice
/// of the connection's read buffer, and hyper allocates a fresh buffer behind it while the frame
/// is alive, so a body arriving in small segments (a slow WAN client, one MSS per read) held
/// several times its own size until it ended. Trailers are dropped: nothing here reads them.
pub(crate) async fn collect_with_stall_bound(
    mut body: Limited<Incoming>,
    stall: Option<std::time::Duration>,
) -> Result<Bytes, BodyReadError> {
    let mut first: Option<Bytes> = None;
    let mut joined: Option<BytesMut> = None;
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
        let Ok(data) = frame.into_data() else { continue };
        if let Some(joined) = joined.as_mut() {
            joined.extend_from_slice(&data);
        } else if let Some(held) = first.take() {
            let mut buf = BytesMut::with_capacity(held.len() + data.len());
            buf.extend_from_slice(&held);
            buf.extend_from_slice(&data);
            joined = Some(buf);
        } else {
            first = Some(data);
        }
    }
    Ok(match (joined, first) {
        (Some(joined), _) => joined.freeze(),
        (None, Some(first)) => first,
        (None, None) => Bytes::new(),
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
    if is_length_limit(err) {
        return "request exceeds the maximum allowed size".to_string();
    }
    format!("failed reading the request body (not necessarily oversized): {err}")
}

/// Whether a [`Limited`] read failure is the size limit tripping, recognized as
/// [`body_read_error_message`] recognizes it. `datadog_in` counts the two cases under different
/// reasons.
pub(crate) fn is_length_limit(err: &(dyn std::error::Error + Send + Sync + 'static)) -> bool {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cause {
        if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() {
            return true;
        }
        cause = e.source();
    }
    false
}

// -------------------------------------------------------------------------------------------------
// Request helpers shared by `datadog_in` and `datadog_trace_in`
// -------------------------------------------------------------------------------------------------

/// A request's declared `Content-Encoding`. Each listener decides which of these it accepts:
/// `datadog_in` all four, `datadog_trace_in` identity and gzip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Encoding {
    Identity,
    Gzip,
    Deflate,
    Zstd,
}

impl Encoding {
    /// Matched case-insensitively, since HTTP content codings are (RFC 9110 §8.4.1). `Err`
    /// carries what was sent, for the `415` message.
    pub(crate) fn from_headers(headers: &http::HeaderMap) -> Result<Self, String> {
        let Some(value) = headers.get(http::header::CONTENT_ENCODING) else {
            return Ok(Self::Identity);
        };
        let value = value.to_str().unwrap_or("").trim();
        if value.is_empty() || value.eq_ignore_ascii_case("identity") {
            Ok(Self::Identity)
        } else if value.eq_ignore_ascii_case("gzip") {
            Ok(Self::Gzip)
        } else if value.eq_ignore_ascii_case("deflate") {
            Ok(Self::Deflate)
        } else if value.eq_ignore_ascii_case("zstd") {
            Ok(Self::Zstd)
        } else {
            Err(value.to_string())
        }
    }
}

/// Why [`decompress`] failed: `413` versus `400`, as `otlp_in` distinguishes them.
pub(crate) enum DecompressError {
    TooLarge,
    Malformed(String),
}

/// Decompresses `body` under `encoding`, bounded to `cap` bytes of output. An identity body is
/// returned as-is: the caller has already capped it at its compressed limit, which no caller's
/// decompressed cap is below.
pub(crate) fn decompress(
    encoding: Encoding,
    body: Bytes,
    cap: usize,
) -> Result<Bytes, DecompressError> {
    match encoding {
        Encoding::Identity => Ok(body),
        Encoding::Gzip => bounded_read(flate2::read::GzDecoder::new(&body[..]), cap, "gzip"),
        Encoding::Deflate => {
            bounded_read(flate2::read::ZlibDecoder::new(&body[..]), cap, "deflate")
        }
        Encoding::Zstd => match crate::zstd::decompress(&body, cap) {
            Ok(out) => Ok(Bytes::from(out)),
            Err(crate::zstd::ZstdError::TooLarge) => Err(DecompressError::TooLarge),
            Err(crate::zstd::ZstdError::Malformed(message)) => {
                Err(DecompressError::Malformed(message))
            }
        },
    }
}

/// `otlp_in`'s `inflate`: `Read::take` allows one byte past `cap`, so an input inflating to
/// `cap + 1` is caught rather than truncated to fit.
fn bounded_read(
    reader: impl std::io::Read,
    cap: usize,
    name: &str,
) -> Result<Bytes, DecompressError> {
    use std::io::Read;
    let mut out = Vec::new();
    reader
        .take(cap as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|err| DecompressError::Malformed(format!("invalid {name} body: {err}")))?;
    if out.len() > cap {
        return Err(DecompressError::TooLarge);
    }
    Ok(Bytes::from(out))
}

/// A request's `Content-Type` media type, as far as the Datadog listeners tell bodies apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MediaType {
    /// `application/x-protobuf` or `application/protobuf`.
    Protobuf,
    /// `application/msgpack`, `application/x-msgpack`, or `application/vnd.msgpack`.
    Msgpack,
    /// `application/json` or `text/json`.
    Json,
    /// Anything else, or no `Content-Type` at all.
    Other,
}

/// Sniffs `Content-Type`, matched case-insensitively and ignoring parameters (`; charset=...`).
pub(crate) fn media_type(headers: &http::HeaderMap) -> MediaType {
    let Some(value) = headers.get(http::header::CONTENT_TYPE) else {
        return MediaType::Other;
    };
    let media = value.to_str().unwrap_or("").split(';').next().unwrap_or("").trim();
    let is = |name: &str| media.eq_ignore_ascii_case(name);
    if is("application/x-protobuf") || is("application/protobuf") {
        MediaType::Protobuf
    } else if is("application/msgpack")
        || is("application/x-msgpack")
        || is("application/vnd.msgpack")
    {
        MediaType::Msgpack
    } else if is("application/json") || is("text/json") {
        MediaType::Json
    } else {
        MediaType::Other
    }
}

/// `Content-Length`, when present and a number. An unparseable one is left for hyper, which
/// rejects it before the handler runs.
pub(crate) fn declared_length(headers: &http::HeaderMap) -> Option<u64> {
    headers.get(http::header::CONTENT_LENGTH)?.to_str().ok()?.trim().parse().ok()
}

/// Wall-clock now in Unix nanoseconds: a request's `received_at`.
pub(crate) fn now_nanos() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// A response with `Content-Type: application/json` and `body`.
pub(crate) fn json_response(
    status: http::StatusCode,
    body: Bytes,
) -> http::Response<http_body_util::Full<Bytes>> {
    http::Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, "application/json")
        .body(http_body_util::Full::new(body))
        .expect("a well-formed response always builds")
}

/// Datadog's error shape: `{"status":"error","errors":[message]}`.
pub(crate) fn error_response(
    status: http::StatusCode,
    message: &str,
) -> http::Response<http_body_util::Full<Bytes>> {
    // Formatted rather than built as a `serde_json::Value`, whose map would sort `errors` first.
    let message = serde_json::to_string(message).expect("a string always serializes");
    json_response(status, Bytes::from(format!(r#"{{"status":"error","errors":[{message}]}}"#)))
}

/// Sends `batches` in order under one deadline, `busy_after` from now: the bounded wait both
/// Datadog listeners answer `503` after (`crate::datadog`'s "Backpressure" section). Each batch
/// reaches every consumer or none ([`logit_pipeline::Fanout::send_with_deadline`]). `Err` carries
/// how many were not delivered, the timed-out one included.
pub(crate) async fn deliver_with_deadline(
    sink: &logit_pipeline::Fanout,
    batches: Vec<logit_core::EventBatch>,
    busy_after: std::time::Duration,
) -> Result<(), usize> {
    let deadline = tokio::time::Instant::now() + busy_after;
    let total = batches.len();
    for (sent, batch) in batches.into_iter().enumerate() {
        if sink.send_with_deadline(batch, deadline).await.is_err() {
            return Err(total - sent);
        }
    }
    Ok(())
}
