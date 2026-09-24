//! The readiness/liveness HTTP endpoint (docs/adr/admin-readiness-endpoint.md).
//!
//! Two routes, `GET /readyz` and `GET /healthz`, and nothing else: no `/metrics` (ADR
//! `internal-telemetry-as-pipeline-events` rejects it), no config dump. HTTP/1.1 only, no TLS:
//! it's a loopback/pod-local endpoint (`docs/deploying.md`). The accept loop has
//! `logit_inputs::otlp::OtlpInput::run`'s shape (permit-gated, one task per connection) at a
//! smaller scale.

use bytes::Bytes;
use http::{Method, StatusCode};
use http_body_util::Full;
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use logit_pipeline::readiness::{NodeState, Phase, PipelineState};
use std::io::ErrorKind;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;

/// Far below `otlp_in`'s 1024: a probe is one cheap GET, and this bounds the worst case to a
/// handful of stuck connections.
const MAX_CONCURRENT_CONNECTIONS: usize = 16;

/// Accept-to-response budget per connection, so a hung client or port scanner can't pin a
/// connection slot forever. Generous for a same-host probe.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// Pause after a process-wide `accept()` failure (fd exhaustion, `EMFILE`/`ENFILE`): long enough
/// that a sustained one can't spin a core, short enough not to delay a probe once it clears.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Serves `/readyz`/`/healthz` off `readiness` on `listener` for the life of the process.
///
/// Takes a bound listener, not an address, so `run_pipelines` binds before spawning anything and
/// maps a failure to `RunError::Startup`, as `Input::bind`'s pre-pass does. The caller aborts this
/// task once the pipeline returns, and that's the only teardown: a shutdown signal must not close
/// the port, because the drain it starts is the window `/readyz` answers `503 draining` in, and a
/// closed port then looks to an orchestrator like a crash.
///
/// Returns `()`: the caller only aborts this task, never joins it, so an `Err` would go unread.
pub async fn serve_on(listener: TcpListener, readiness: watch::Receiver<PipelineState>) {
    let connection_limit = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _peer)) => stream,
            Err(err) => {
                // A failed `accept()` never ends this loop: nothing awaits this task, so returning
                // would take the endpoint down unreported and put a healthy process into a restart
                // loop under any orchestrator polling `/readyz`. `ConnectionAborted`/
                // `ConnectionReset`/`Interrupted` are one client's accident and retry at once;
                // anything else (fd exhaustion, realistically) is process-wide and gets
                // `ACCEPT_ERROR_BACKOFF` first, or a readable-but-failing listener spins hot.
                tracing::warn!(target: "logit", error = %err, "admin: accept failed");
                if !matches!(
                    err.kind(),
                    ErrorKind::ConnectionAborted
                        | ErrorKind::ConnectionReset
                        | ErrorKind::Interrupted
                ) {
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                }
                continue;
            }
        };

        // Acquired after accept, as in `otlp_in`: while every permit is held, the kernel's
        // backlog absorbs a burst instead of the connection being refused.
        let permit =
            connection_limit.clone().acquire_owned().await.expect("this semaphore is never closed");
        let readiness = readiness.clone();
        tokio::spawn(async move {
            let _permit = permit; // held for the connection's lifetime; released on drop
            let svc = service_fn(move |req| handle(req, readiness.clone()));
            let serve = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), svc);
            let _ = tokio::time::timeout(CONNECTION_TIMEOUT, serve).await;
        });
    }
}

async fn handle(
    req: http::Request<hyper::body::Incoming>,
    readiness: watch::Receiver<PipelineState>,
) -> Result<http::Response<Full<Bytes>>, std::convert::Infallible> {
    // `HEAD` routes like `GET` and gets the full body: hyper's HTTP/1 server drops a HEAD
    // response's body bytes but derives `content-length` from the body it's handed, which is
    // RFC 9110 §9.3.2's "same header fields". An empty body would drop the header instead.
    if !matches!(*req.method(), Method::GET | Method::HEAD) {
        return Ok(text_response(StatusCode::NOT_FOUND, "not found"));
    }
    let json = req.uri().query().is_some_and(|q| q.split('&').any(|kv| kv == "format=json"));

    Ok(match req.uri().path() {
        "/readyz" => readyz_response(&readiness.borrow(), json),
        "/healthz" => healthz_response(json),
        _ => text_response(StatusCode::NOT_FOUND, "not found"),
    })
}

/// `Phase` -> `/readyz`'s status and wire word, which differs from `Phase::as_str()` (`ready` ->
/// `ok`, `failed` -> `degraded`): the wire word is an HTTP concern, `Phase`'s is the runtime's.
fn readyz_wire(phase: Phase) -> (StatusCode, &'static str) {
    match phase {
        Phase::Ready => (StatusCode::OK, "ok"),
        Phase::Starting => (StatusCode::SERVICE_UNAVAILABLE, "starting"),
        Phase::Draining => (StatusCode::SERVICE_UNAVAILABLE, "draining"),
        Phase::Failed => (StatusCode::SERVICE_UNAVAILABLE, "degraded"),
    }
}

fn readyz_response(snapshot: &PipelineState, json: bool) -> http::Response<Full<Bytes>> {
    let (status, word) = readyz_wire(snapshot.phase);
    if json {
        json_response(status, snapshot_json(word, snapshot))
    } else {
        text_response(status, word)
    }
}

/// Always `200 ok`: proves only that the tokio runtime can answer. Pipeline state is `/readyz`'s.
fn healthz_response(json: bool) -> http::Response<Full<Bytes>> {
    if json {
        json_response(StatusCode::OK, serde_json::json!({"status": "ok"}))
    } else {
        text_response(StatusCode::OK, "ok")
    }
}

fn snapshot_json(status: &str, snapshot: &PipelineState) -> serde_json::Value {
    let since_nanos = snapshot
        .since
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    let components: serde_json::Map<String, serde_json::Value> = snapshot
        .components
        .iter()
        .map(|(id, state)| (id.clone(), serde_json::Value::String(node_state_wire(*state))))
        .collect();
    serde_json::json!({
        "status": status,
        "since": logit_core::time::format_rfc3339_utc(since_nanos),
        "components": components,
    })
}

fn node_state_wire(state: NodeState) -> String {
    state.as_str().to_string()
}

fn text_response(status: StatusCode, body: &str) -> http::Response<Full<Bytes>> {
    http::Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(Full::new(Bytes::copy_from_slice(body.as_bytes())))
        .expect("a well-formed response always builds")
}

fn json_response(status: StatusCode, body: serde_json::Value) -> http::Response<Full<Bytes>> {
    http::Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("a well-formed response always builds")
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_pipeline::Readiness;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;

    #[test]
    fn readyz_wire_matches_the_spec_table() {
        assert_eq!(readyz_wire(Phase::Ready), (StatusCode::OK, "ok"));
        assert_eq!(readyz_wire(Phase::Starting), (StatusCode::SERVICE_UNAVAILABLE, "starting"));
        assert_eq!(readyz_wire(Phase::Draining), (StatusCode::SERVICE_UNAVAILABLE, "draining"));
        assert_eq!(readyz_wire(Phase::Failed), (StatusCode::SERVICE_UNAVAILABLE, "degraded"));
    }

    #[test]
    fn snapshot_json_carries_status_since_and_components() {
        let (readiness, _rx) = Readiness::channel();
        readiness.begin(&["a".to_string()]);
        readiness.set_node("a", NodeState::Running);
        let snapshot = readiness.snapshot();

        let value = snapshot_json("ok", &snapshot);
        assert_eq!(value["status"], "ok");
        assert_eq!(value["components"]["a"], "running");
        assert!(value["since"].as_str().unwrap().ends_with('Z'), "since should be RFC3339 UTC");
    }

    // The tests below go over real TCP: `hyper::body::Incoming` has no public constructor, so
    // `handle` can't be called directly.

    /// Sends a raw HTTP/1.1 request; returns (status code, headers block, body). The raw-socket
    /// idiom of `logit-inputs`' otlp tests (`post_raw`): `logit-cli` has no HTTP client of its own.
    async fn request_raw(addr: &str, method: &str, path: &str) -> (u16, String, String) {
        let mut stream = TcpStream::connect(addr).await.expect("the listener is already bound");
        let request =
            format!("{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).await.unwrap();
        let text = String::from_utf8_lossy(&buf).into_owned();
        let mut parts = text.splitn(2, "\r\n\r\n");
        let head = parts.next().unwrap_or("").to_string();
        let body = parts.next().unwrap_or("").to_string();
        let code = head
            .lines()
            .next()
            .unwrap_or("")
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        (code, head, body)
    }

    struct Server {
        addr: String,
        readiness: Readiness,
        handle: tokio::task::JoinHandle<()>,
    }

    async fn spawn_server() -> Server {
        // Bound before spawning, as `run_pipelines` does, so a client can connect into the
        // accept backlog at once with no bind-race retry.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (readiness, rx) = Readiness::channel();
        let handle = tokio::spawn(serve_on(listener, rx));
        Server { addr, readiness, handle }
    }

    #[tokio::test]
    async fn readyz_is_503_before_ready_and_200_after() {
        let server = spawn_server().await;
        server.readiness.begin(&["in".to_string()]);

        let (code, _head, body) = request_raw(&server.addr, "GET", "/readyz").await;
        assert_eq!(code, 503);
        assert_eq!(body, "starting");

        server.readiness.set_node("in", NodeState::Running);
        server.readiness.ready();
        let (code, _head, body) = request_raw(&server.addr, "GET", "/readyz").await;
        assert_eq!(code, 200);
        assert_eq!(body, "ok");

        server.readiness.draining();
        let (code, _head, body) = request_raw(&server.addr, "GET", "/readyz").await;
        assert_eq!(code, 503);
        assert_eq!(body, "draining");

        server.handle.abort();
    }

    #[tokio::test]
    async fn healthz_is_always_200_regardless_of_readiness() {
        let server = spawn_server().await;
        server.readiness.begin(&[]);
        // Still `Starting`.
        let (code, _head, body) = request_raw(&server.addr, "GET", "/healthz").await;
        assert_eq!(code, 200);
        assert_eq!(body, "ok");
        server.handle.abort();
    }

    #[tokio::test]
    async fn readyz_format_json_reports_the_documented_shape() {
        let server = spawn_server().await;
        server.readiness.begin(&["in".to_string()]);
        server.readiness.set_node("in", NodeState::Running);
        server.readiness.ready();

        let (code, head, body) = request_raw(&server.addr, "GET", "/readyz?format=json").await;
        assert_eq!(code, 200);
        assert!(head.to_lowercase().contains("content-type: application/json"));
        let value: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(value["status"], "ok");
        assert_eq!(value["components"]["in"], "running");
        assert!(value["since"].is_string());

        server.handle.abort();
    }

    #[tokio::test]
    async fn an_unknown_path_is_404() {
        let server = spawn_server().await;
        let (code, _head, _body) = request_raw(&server.addr, "GET", "/nope").await;
        assert_eq!(code, 404);
        server.handle.abort();
    }

    /// RFC 9110 §9.3.2: `HEAD` gets `GET`'s headers, `content-length` included, and no body.
    #[tokio::test]
    async fn head_mirrors_gets_headers_and_sends_no_body() {
        let server = spawn_server().await;
        server.readiness.begin(&[]);
        server.readiness.ready();

        let (get_code, get_head, get_body) = request_raw(&server.addr, "GET", "/readyz").await;
        let (head_code, head_head, head_body) = request_raw(&server.addr, "HEAD", "/readyz").await;

        assert_eq!(get_body, "ok");
        assert_eq!(head_code, get_code);
        assert_eq!(head_body, "", "HEAD must return no body");

        fn content_length(head: &str) -> Option<String> {
            head.lines()
                .find(|line| line.to_ascii_lowercase().starts_with("content-length:"))
                .and_then(|line| line.split_once(':'))
                .map(|(_, value)| value.trim().to_string())
        }
        assert_eq!(content_length(&head_head), content_length(&get_head));
        assert_eq!(content_length(&head_head).as_deref(), Some("2"), "the length `ok` would be");

        server.handle.abort();
    }

    /// A 17th concurrent connection waits for a permit rather than being refused.
    #[tokio::test]
    async fn a_17th_concurrent_connection_waits_for_a_free_permit() {
        let server = spawn_server().await;
        server.readiness.begin(&[]);
        server.readiness.ready();

        let mut held: Vec<TcpStream> = Vec::new();
        for _ in 0..MAX_CONCURRENT_CONNECTIONS {
            held.push(
                TcpStream::connect(&server.addr).await.expect("the listener is already bound"),
            );
        }

        let mut seventeenth =
            TcpStream::connect(&server.addr).await.expect("the listener is already bound");
        seventeenth
            .write_all(b"GET /healthz HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();

        let mut probe = [0u8; 1];
        let no_response_yet =
            tokio::time::timeout(Duration::from_millis(150), seventeenth.read(&mut probe)).await;
        assert!(
            no_response_yet.is_err(),
            "the 17th connection should still be waiting for a free permit"
        );

        held.pop(); // frees one permit

        let mut buf = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), seventeenth.read_to_end(&mut buf))
            .await
            .expect("the 17th connection should now be served")
            .unwrap();
        assert!(String::from_utf8_lossy(&buf).starts_with("HTTP/1.1 200"));

        server.handle.abort();
    }
}
