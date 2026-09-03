//! OTLP input -- accepts logs, metrics, and traces over either OTLP/HTTP (protobuf-over-POST) or
//! OTLP/gRPC, selected by `protocol` in config (`logit_config::OtlpProtocol`). See
//! `docs/adr/hand-rolled-grpc-over-hyper.md` for why the gRPC server is a ~200-line hand-rolled
//! `hyper::server::conn::http2` service rather than `tonic`, and why that's budgeted as roughly
//! half of this whole PR's effort -- HTTP/2 trailers, per-method routing, and gRPC status-code
//! framing are all things `tonic` gives away for free and this input has to build by hand.
//!
//! **One listener, one accept loop, one handler per connection.** [`Input::run`] binds a single
//! `TcpListener` and `tokio::spawn`s a handler per accepted connection -- [`Fanout`] is already
//! `Clone`, which is the whole mechanism that lets an arbitrary number of concurrent connections
//! each hold their own handle to the same downstream sink. HTTP connections are served by
//! [`hyper_util::server::conn::auto::Builder`], which transparently handles both HTTP/1.1 (what a
//! `curl`/most OTel HTTP exporters speak) and h2c (prior-knowledge HTTP/2 without TLS, what
//! `curl --http2-prior-knowledge` and some exporters prefer); gRPC connections are served by
//! [`hyper::server::conn::http2::Builder`] directly, since gRPC *is* HTTP/2 -- there's no h1
//! fallback to auto-detect.
//!
//! **This is the first listener with real backpressure to its source.** Every other input here is
//! UDP (statsd, syslog): a slow downstream just means the kernel silently drops datagrams. TCP
//! (both OTLP transports) has no such escape hatch -- a slow `sink.send(batch).await` blocks the
//! handler, which stalls reading the next request off that connection, which the client
//! eventually feels as its own write blocking. Correct for a reliable protocol (an OTLP exporter
//! is expected to retry/buffer on its own timeout, not silently lose data), and worth knowing
//! going in: `docs/design/pipeline-graph.md`'s backpressure section.
//!
//! **Compression is not supported.** `Content-Encoding: gzip` (HTTP) and `grpc-encoding: gzip`
//! (gRPC) are both explicitly rejected (`415`/`grpc-status: 12`) rather than silently
//! mishandled -- decompressing untrusted input is real, security-relevant surface (a zip-bomb-
//! shaped request), and `flate2` isn't a dependency yet. The OTel *collector*'s own default OTLP
//! exporter sends gzip, so pointing a real collector at this input fails on day one even though
//! `otlp_out -> otlp_in` (this PR's own round-trip tests, which never set `grpc-encoding`/
//! `Content-Encoding`) works fine. Recorded as a known gap; revisit with `flate2` if it bites.
//!
//! **Size and concurrency limits.** `MAX_REQUEST_BYTES` (4 MiB) matches the OTel collector's own
//! default `max_recv_msg_size`; a request over that is rejected (`413`/`grpc-status: 8`,
//! `RESOURCE_EXHAUSTED`) before it can grow an unbounded buffer. That bounds one connection's
//! worst case, not the listener's as a whole -- `MAX_CONCURRENT_CONNECTIONS` bounds how many
//! connections `run` serves at once, so total worst-case memory stays a real (if generous) number
//! rather than unbounded.
//!
//! **The response's `partial_success` is always empty on a successful decode.** OTLP's own
//! `Export*ServiceResponse.partial_success` exists to report *which* records within an otherwise-
//! accepted request were rejected -- but `logit_proto::SignalDecoder::decode_signal` doesn't
//! return a per-call skip/reject count today (it only counts skips against its own
//! `logit.input.metrics.skipped{metric_kind, reason}` telemetry, `logit-proto`'s `otlp::metrics`
//! module doc); there's nothing for this input to echo back into the wire response yet. A fully
//! malformed request (bad protobuf, an out-of-range span id) still fails the *whole* request
//! (`400`/`grpc-status: 3`), which is the one case this input's response does reflect correctly.
//! Threading a real per-call count through would be a `SignalDecoder` API change, out of this PR's
//! scope -- tracked in `docs/known-gaps.md`.

use crate::Input;
use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use logit_core::{Diagnostics, Telemetry};
use logit_pipeline::Fanout;
use logit_proto::otlp::OtlpDecoder;
use logit_proto::{Signal, SignalDecoder};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context as TaskContext, Poll};
use tokio::net::TcpListener;

/// Matches the OTel collector's own default `max_recv_msg_size` -- see this module's doc comment.
const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;

/// Bounds the number of connections [`Input::run`] serves concurrently -- without this, the
/// per-request cap [`MAX_REQUEST_BYTES`] bounds only *one* connection's worst case, and this is
/// the first listener in this codebase (every other one is UDP, with no concept of a
/// "connection" at all) where an unbounded number of them can each be holding that much. 1024 is
/// the same order of magnitude `logit_pipeline::SinkQueueConfig::default`'s `max_batches` already
/// uses elsewhere in this codebase for "a generous but real bound, not unlimited" -- worst case
/// `1024 * MAX_REQUEST_BYTES` = 4 GiB in flight, not unbounded. Not (yet) operator-tunable; revisit
/// as a config field if a real deployment needs a different number.
const MAX_CONCURRENT_CONNECTIONS: usize = 1024;

/// Which OTLP wire transport this listener accepts. See `logit_outputs::otlp::OtlpTransport`'s
/// identical doc comment -- same reasoning, mirrored independently rather than shared, since this
/// crate doesn't depend on `logit-config` any more than `logit-outputs` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpTransport {
    Http,
    Grpc,
}

pub struct OtlpInput {
    bind: String,
    transport: OtlpTransport,
    diag: Diagnostics,
    telemetry: Telemetry,
}

impl OtlpInput {
    pub fn new(bind: impl Into<String>, transport: OtlpTransport) -> Self {
        Self {
            bind: bind.into(),
            transport,
            diag: Diagnostics::default(),
            telemetry: Telemetry::default(),
        }
    }

    pub fn with_diagnostics(mut self, diag: Diagnostics) -> Self {
        self.diag = diag;
        self
    }

    pub fn with_telemetry(mut self, telemetry: Telemetry) -> Self {
        self.telemetry = telemetry;
        self
    }
}

#[async_trait::async_trait]
impl Input for OtlpInput {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        let listener = TcpListener::bind(&self.bind).await?;
        // Bounds this input's worst-case memory the same way `MAX_REQUEST_BYTES` bounds one
        // request's -- see [`MAX_CONCURRENT_CONNECTIONS`]'s own doc comment for the reasoning and
        // the resulting worst case.
        let connection_limit = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
        loop {
            let (stream, _peer) = listener.accept().await?;
            // Acquired *after* `accept`, not before: the kernel's own accept backlog still
            // absorbs a burst of new connections while every permit is held, so a connection
            // isn't refused outright at the cap -- its handler just doesn't start (and doesn't
            // read a single byte, so it can't yet be holding any of `MAX_REQUEST_BYTES`) until an
            // earlier connection finishes and its permit is released back (on drop, at the end of
            // the spawned task below). This is real backpressure to the accept loop itself: the
            // next `accept().await` above doesn't run until this acquire resolves, so the
            // listener stops draining its backlog at all once the backlog itself fills, same
            // shape as this crate's other listeners eventually blocking on a full downstream
            // `Fanout` (`docs/design/pipeline-graph.md`'s backpressure section).
            let permit = connection_limit
                .clone()
                .acquire_owned()
                .await
                .expect("this semaphore is never closed");
            let io = TokioIo::new(stream);
            let sink = sink.clone();
            let transport = self.transport;
            let mut diag = self.diag.clone();
            let telemetry = self.telemetry.clone();
            tokio::spawn(async move {
                let _permit = permit; // held for the connection's lifetime; released on drop
                let result = match transport {
                    OtlpTransport::Http => {
                        let svc = service_fn(move |req| {
                            handle_http(req, sink.clone(), telemetry.clone())
                        });
                        auto::Builder::new(TokioExecutor::new())
                            .serve_connection(io, svc)
                            .await
                            .map_err(|e| e.to_string())
                    }
                    OtlpTransport::Grpc => {
                        let svc = service_fn(move |req| {
                            handle_grpc(req, sink.clone(), telemetry.clone())
                        });
                        hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                            .serve_connection(io, svc)
                            .await
                            .map_err(|e| e.to_string())
                    }
                };
                // One connection's I/O error (a client disconnecting mid-request, a malformed
                // TLS-looking preamble on a plaintext port, ...) shouldn't be fatal to the
                // listener or its sibling connections -- only `TcpListener::accept` failing in
                // `run`'s own loop is.
                if let Err(err) = result {
                    diag.warn_throttled("connection_error", err);
                }
            });
        }
    }
}

async fn handle_http(
    req: http::Request<Incoming>,
    sink: Fanout,
    telemetry: Telemetry,
) -> Result<http::Response<Full<Bytes>>, std::convert::Infallible> {
    if req.method() != Method::POST {
        return Ok(text_response(StatusCode::NOT_FOUND, "not found"));
    }
    let Some(signal) = route_path(req.uri().path()) else {
        return Ok(text_response(StatusCode::NOT_FOUND, "not found"));
    };
    if let Some(ct) = req.headers().get("content-type") {
        let ct = ct.to_str().unwrap_or("");
        let ct = ct.split(';').next().unwrap_or("").trim();
        if !ct.is_empty() && ct != "application/x-protobuf" && ct != "application/protobuf" {
            return Ok(text_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                &format!(
                    "OTLP/JSON is not supported; send protobuf as application/x-protobuf (got \
                     {ct:?})"
                ),
            ));
        }
    }
    // `identity` is the legal, standard way to say "not compressed" -- rejecting on the header's
    // mere *presence* would 415 a client that explicitly (if redundantly) declares no compression,
    // not just one that actually compressed its body. Mirrors the gRPC handler's `grpc-encoding`
    // check just below.
    if let Some(enc) = req.headers().get("content-encoding") {
        if enc.as_bytes() != b"identity" {
            return Ok(text_response(
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "compressed OTLP/HTTP requests are not supported -- see this module's doc comment",
            ));
        }
    }

    let limited = Limited::new(req.into_body(), MAX_REQUEST_BYTES);
    let bytes = match limited.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => {
            return Ok(text_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &body_read_error_message(err.as_ref()),
            ))
        }
    };

    let mut decoder = OtlpDecoder::new().with_telemetry(telemetry);
    match decoder.decode_signal(signal, bytes) {
        Ok(batches) => {
            for batch in batches {
                sink.send(batch).await;
            }
            Ok(http::Response::builder()
                .status(StatusCode::OK)
                .header("content-type", "application/x-protobuf")
                .body(Full::new(Bytes::from(export_response(0, ""))))
                .expect("a well-formed response always builds"))
        }
        Err(err) => Ok(text_response(StatusCode::BAD_REQUEST, &err.to_string())),
    }
}

async fn handle_grpc(
    req: http::Request<Incoming>,
    sink: Fanout,
    telemetry: Telemetry,
) -> Result<http::Response<GrpcBody>, std::convert::Infallible> {
    if req.method() != Method::POST {
        return Ok(grpc_response(12, "only POST is supported", None));
    }
    let path = req.uri().path();
    let Some(signal) = [Signal::Logs, Signal::Metrics, Signal::Traces]
        .into_iter()
        .find(|s| s.grpc_method() == path)
    else {
        return Ok(grpc_response(12, &format!("unknown method {path}"), None));
    };
    if let Some(enc) = req.headers().get("grpc-encoding") {
        if enc.as_bytes() != b"identity" {
            return Ok(grpc_response(
                12,
                "compressed gRPC requests are not supported -- see this module's doc comment",
                None,
            ));
        }
    }

    let limited = Limited::new(req.into_body(), MAX_REQUEST_BYTES);
    let framed = match limited.collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(err) => return Ok(grpc_response(8, &body_read_error_message(err.as_ref()), None)),
    };
    let Some(payload) = grpc_unframe(&framed) else {
        return Ok(grpc_response(3, "malformed gRPC message frame", None));
    };

    let mut decoder = OtlpDecoder::new().with_telemetry(telemetry);
    match decoder.decode_signal(signal, Bytes::copy_from_slice(payload)) {
        Ok(batches) => {
            for batch in batches {
                sink.send(batch).await;
            }
            Ok(grpc_response(0, "", Some(export_response(0, ""))))
        }
        Err(err) => Ok(grpc_response(3, &err.to_string(), None)),
    }
}

/// Turns a [`Limited`] read failure into a response message that doesn't overclaim. `Limited`'s
/// `Error` covers *any* failure reading the body, not just exceeding `MAX_REQUEST_BYTES` -- a
/// client disconnecting mid-upload, malformed chunked encoding, or an HTTP/2 stream reset all
/// surface the same way. Distinguished via `LengthLimitError`'s presence in the error chain
/// (`Limited` wraps the real cause when the limit trips, and otherwise forwards the underlying
/// body's own error untouched) rather than assumed from the mere fact that `collect` failed --
/// callers still respond `413`/`RESOURCE_EXHAUSTED` either way (there's no better status for "the
/// request body never finished," and this is not the place to teach every HTTP/gRPC client the
/// difference), but the message itself says which actually happened.
fn body_read_error_message(err: &(dyn std::error::Error + Send + Sync + 'static)) -> String {
    let mut cause: Option<&(dyn std::error::Error + 'static)> = Some(err);
    while let Some(e) = cause {
        if e.downcast_ref::<http_body_util::LengthLimitError>().is_some() {
            return "request exceeds the maximum allowed size".to_string();
        }
        cause = e.source();
    }
    format!("failed reading the request body (not necessarily oversized): {err}")
}

/// Matches an OTLP/HTTP path (`/v1/logs`, `/v1/metrics`, `/v1/traces`) to its [`Signal`].
fn route_path(path: &str) -> Option<Signal> {
    [Signal::Logs, Signal::Metrics, Signal::Traces].into_iter().find(|s| s.path() == path)
}

fn text_response(status: StatusCode, message: &str) -> http::Response<Full<Bytes>> {
    http::Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::copy_from_slice(message.as_bytes())))
        .expect("a well-formed response always builds")
}

/// Builds a gRPC response: `200` status, the framed `payload` (empty when `None`) as one data
/// frame, and `grpc-status`/`grpc-message` as trailers -- always via a real trailers frame after
/// the body, never a Trailers-Only (headers-only) response, which keeps this server's shape
/// uniform for every outcome including an immediate rejection (an unknown method, say) rather than
/// needing a second response-building path for that case.
fn grpc_response(status: u32, message: &str, payload: Option<Vec<u8>>) -> http::Response<GrpcBody> {
    let mut trailers = HeaderMap::new();
    trailers.insert(
        "grpc-status",
        HeaderValue::from_str(&status.to_string())
            .expect("a decimal number is a valid header value"),
    );
    if !message.is_empty() {
        trailers.insert(
            "grpc-message",
            HeaderValue::from_str(message).unwrap_or_else(|_| HeaderValue::from_static("error")),
        );
    }
    let framed = grpc_frame(&payload.unwrap_or_default());
    let body = GrpcBody { data: Some(Bytes::from(framed)), trailers: Some(trailers) };
    http::Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/grpc+proto")
        .header("grpc-accept-encoding", "identity")
        .body(body)
        .expect("a well-formed response always builds")
}

/// A response body that yields exactly one data frame, then one trailers frame, then ends -- the
/// shape every unary gRPC response takes on the wire: a single framed message (possibly
/// zero-length, for an error response with no payload), followed by the `grpc-status` trailer.
/// `http_body_util::Full` can't express this -- it has no trailers concept at all -- so this is
/// hand-rolled directly against [`hyper::body::Body`], the same "small enough to write by hand"
/// call this whole transport makes (`docs/adr/hand-rolled-grpc-over-hyper.md`).
struct GrpcBody {
    data: Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl hyper::body::Body for GrpcBody {
    type Data = Bytes;
    type Error = std::convert::Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        if let Some(data) = self.data.take() {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        if let Some(trailers) = self.trailers.take() {
            return Poll::Ready(Some(Ok(Frame::trailers(trailers))));
        }
        Poll::Ready(None)
    }
}

/// Frames `payload` as one unary gRPC message. See
/// `logit_outputs::otlp::grpc_frame`'s identical doc comment -- duplicated, not shared: these are
/// two independent crates, and this one function is a handful of lines.
fn grpc_frame(payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(5 + payload.len());
    buf.push(0u8);
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

/// The mirror of [`grpc_frame`]. See `logit_outputs::otlp::grpc_unframe`'s identical doc comment.
fn grpc_unframe(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() < 5 || bytes[0] != 0 {
        return None;
    }
    let len = u32::from_be_bytes(bytes[1..5].try_into().expect("checked len >= 5 above")) as usize;
    bytes.get(5..5 + len)
}

fn write_varint(buf: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            buf.push(byte);
            break;
        }
        buf.push(byte | 0x80);
    }
}

/// Builds an `Export*ServiceResponse`'s bytes by hand -- see
/// `logit_outputs::otlp::parse_partial_success`'s doc comment for why the wire shape is identical
/// across all three signals and why neither side generates the collector-service types for it.
/// Empty when `rejected == 0` and `error_message` is empty: proto3's own "an unset field reads
/// back as its default" rule already makes an all-default message serialize to zero bytes, so a
/// fully successful response is simply an empty body -- exactly what every response this input
/// sends today looks like (see this module's doc comment on why `rejected` is always `0` for now).
fn export_response(rejected: i64, error_message: &str) -> Vec<u8> {
    if rejected == 0 && error_message.is_empty() {
        return Vec::new();
    }
    let mut sub = Vec::new();
    if rejected != 0 {
        sub.push(0x08); // field 1, varint
        write_varint(&mut sub, rejected as u64);
    }
    if !error_message.is_empty() {
        sub.push(0x12); // field 2, length-delimited
        write_varint(&mut sub, error_message.len() as u64);
        sub.extend_from_slice(error_message.as_bytes());
    }
    let mut out = Vec::new();
    out.push(0x0a); // field 1 (partial_success), length-delimited
    write_varint(&mut out, sub.len() as u64);
    out.extend_from_slice(&sub);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::mpsc;

    async fn bound_input(transport: OtlpTransport) -> (String, OtlpInput) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        (addr.to_string(), OtlpInput::new(addr.to_string(), transport))
    }

    fn fanout_into_channel() -> (Fanout, mpsc::Receiver<logit_pipeline::Delivered>) {
        let (tx, rx) = mpsc::channel(16);
        (Fanout::new(vec![tx]), rx)
    }

    async fn recv_batch(
        rx: &mut mpsc::Receiver<logit_pipeline::Delivered>,
    ) -> logit_core::EventBatch {
        let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("should receive within 5s")
            .expect("channel should still be open");
        logit_pipeline::unwrap_batch(delivered)
    }

    // ---- HTTP: raw-socket protocol tests ----

    async fn post_raw(addr: &str, path: &str, headers: &str, body: &[u8]) -> String {
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let request = format!(
            "POST {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Length: {}\r\n{headers}\r\n",
            body.len()
        );
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
        let mut buf = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(2), stream.read_to_end(&mut buf)).await;
        String::from_utf8_lossy(&buf).into_owned()
    }

    #[tokio::test]
    async fn a_protobuf_post_to_v1_traces_reaches_the_fanout_as_an_event_batch() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let batch = logit_core::EventBatch {
            resource: std::sync::Arc::new(logit_core::Resource::default()),
            events: vec![logit_core::Event::span(
                1,
                logit_core::AttrMap::new(),
                logit_core::SpanRecord {
                    trace_id: [9; 16],
                    span_id: [8; 8],
                    parent_span_id: None,
                    name: logit_core::Value::str("s"),
                    kind: logit_core::SpanKind::Internal,
                    status: logit_core::SpanStatus::Ok,
                    events: Vec::new(),
                    links: Vec::new(),
                    end_timestamp: 2,
                },
            )],
        };
        let payloads = logit_proto::SignalEncoder::encode_signals(&mut encoder, &batch).unwrap();
        let (_, body) = payloads.into_iter().find(|(s, _)| *s == Signal::Traces).unwrap();

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");

        let received = recv_batch(&mut rx).await;
        assert_eq!(received.events.len(), 1);
        assert!(received.events[0].span.is_some());
    }

    #[tokio::test]
    async fn a_json_content_type_is_rejected_with_415_and_a_clear_message() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/json\r\nConnection: close\r\n",
            b"{}",
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
        assert!(response.contains("OTLP/JSON is not supported"), "got: {response}");
    }

    /// `identity` is the standard, legal way to declare "not compressed" -- it must not be
    /// mistaken for "compressed, unsupported." Regression guard for rejecting on the header's mere
    /// *presence* rather than its value.
    #[tokio::test]
    async fn a_content_encoding_of_identity_is_accepted_not_rejected() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nContent-Encoding: identity\r\n\
             Connection: close\r\n",
            &[],
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");
    }

    #[tokio::test]
    async fn a_gzip_content_encoding_is_rejected_with_415() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nContent-Encoding: gzip\r\n\
             Connection: close\r\n",
            &[],
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 415"), "got: {response}");
    }

    #[tokio::test]
    async fn a_body_over_the_size_cap_is_rejected_with_413() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let body = vec![0u8; MAX_REQUEST_BYTES + 1];
        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &body,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 413"), "got: {response}");
    }

    #[tokio::test]
    async fn garbage_protobuf_over_http_returns_400() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &[0xff, 0xff, 0xff],
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 400"), "got: {response}");
    }

    #[tokio::test]
    async fn a_request_with_two_resource_spans_produces_two_batches_on_the_fanout() {
        let (addr, mut input) = bound_input(OtlpTransport::Http).await;
        let (sink, mut rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        fn span_batch(host: &str, trace_byte: u8) -> logit_core::EventBatch {
            let mut resource = logit_core::Resource::default();
            resource.attributes.insert("host", host);
            logit_core::EventBatch {
                resource: std::sync::Arc::new(resource),
                events: vec![logit_core::Event::span(
                    1,
                    logit_core::AttrMap::new(),
                    logit_core::SpanRecord {
                        trace_id: [trace_byte; 16],
                        span_id: [trace_byte; 8],
                        parent_span_id: None,
                        name: logit_core::Value::str("s"),
                        kind: logit_core::SpanKind::Internal,
                        status: logit_core::SpanStatus::Ok,
                        events: Vec::new(),
                        links: Vec::new(),
                        end_timestamp: 2,
                    },
                )],
            }
        }

        let mut encoder = logit_proto::otlp::OtlpEncoder::new();
        let bytes_a =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &span_batch("a", 1)).unwrap();
        let bytes_b =
            logit_proto::SignalEncoder::encode_signals(&mut encoder, &span_batch("b", 2)).unwrap();

        // Combine the two single-resource requests into one two-ResourceSpans request by simple
        // byte concatenation, not by decoding and re-encoding through a generated type (`logit-
        // proto`'s `generated` module is `pub(crate)`, unreachable from here). This works because
        // `TracesData`'s only field is `repeated ResourceSpans resource_spans = 1` -- each of
        // `bytes_a`/`bytes_b` is a complete encoding of *one* such occurrence, and concatenating
        // two complete, self-delimited protobuf field occurrences is indistinguishable on the wire
        // from a single message that had both all along (the same "concatenation of encoded
        // messages is a valid merge" property `logit-proto`'s own two-`ResourceSpans` test relies
        // on, just exercised externally here instead of via the generated types directly).
        let mut combined_bytes = Vec::new();
        combined_bytes.extend_from_slice(&bytes_a[0].1);
        combined_bytes.extend_from_slice(&bytes_b[0].1);

        let response = post_raw(
            &addr,
            "/v1/traces",
            "Content-Type: application/x-protobuf\r\nConnection: close\r\n",
            &combined_bytes,
        )
        .await;
        assert!(response.starts_with("HTTP/1.1 200"), "got: {response}");

        let first = recv_batch(&mut rx).await;
        let second = recv_batch(&mut rx).await;
        assert_eq!(first.resource.attributes.get("host").and_then(|v| v.as_str()), Some("a"));
        assert_eq!(second.resource.attributes.get("host").and_then(|v| v.as_str()), Some("b"));
    }

    // ---- gRPC: garbage-frame status test ----

    #[tokio::test]
    async fn garbage_protobuf_returns_grpc_status_three() {
        let (addr, mut input) = bound_input(OtlpTransport::Grpc).await;
        let (sink, _rx) = fanout_into_channel();
        tokio::spawn(async move { input.run(sink).await });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // `OtlpOutput` always sends well-formed frames -- to exercise "garbage protobuf" this
        // drives the raw framing directly instead, over a real HTTP/2 client connection.
        let stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http2::Builder::new(TokioExecutor::new())
            .handshake(io)
            .await
            .unwrap();
        tokio::spawn(conn);

        let mut framed = vec![0u8, 0, 0, 0, 3];
        framed.extend_from_slice(&[0xff, 0xff, 0xff]);
        let req = http::Request::builder()
            .method(Method::POST)
            .uri(Signal::Traces.grpc_method())
            .header("content-type", "application/grpc+proto")
            .header("te", "trailers")
            .body(Full::new(Bytes::from(framed)))
            .unwrap();
        let res = sender.send_request(req).await.unwrap();
        let collected = res.into_body().collect().await.unwrap();
        let trailers = collected.trailers().expect("should carry trailers");
        assert_eq!(trailers.get("grpc-status").unwrap().to_str().unwrap(), "3");
    }
}
