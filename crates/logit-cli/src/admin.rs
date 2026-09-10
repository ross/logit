//! The readiness/liveness HTTP endpoint (`docs/plans/operator-surface.md`, workstream C; ADR
//! `admin-readiness-endpoint.md`). Serves exactly two routes -- `GET /readyz`, `GET /healthz` --
//! nothing else: no `/metrics` (rejected per ADR `internal-telemetry-as-pipeline-events`), no
//! config dump. HTTP/1.1 only, no TLS: this is a loopback/pod-local endpoint, not one meant to
//! cross a network boundary (`docs/deploying.md`).
//!
//! The accept loop below mirrors `logit_inputs::otlp::OtlpInput::run`'s shape (permit-gated
//! concurrency, one spawned task per connection) at a much smaller scale -- an admin probe
//! answers one cheap GET at a time, nowhere near `otlp_in`'s connection budget.

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

/// An admin probe answers one cheap GET at a time -- nowhere near `otlp_in`'s
/// `MAX_CONCURRENT_CONNECTIONS` (1024). Bounds this server's worst case to a handful of stuck
/// connections, not an unbounded accept loop.
const MAX_CONCURRENT_CONNECTIONS: usize = 16;

/// How long one connection (from accept to the response finishing) may take before this server
/// gives up on it and closes it -- a slow or hung client (or a port-scanner) must not pin one of
/// the 16 connection slots forever. Generous for a same-host probe.
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the accept loop pauses after an `accept()` failure that is not one client's own
/// accident -- fd exhaustion (`EMFILE`/`ENFILE`) is the realistic case, and it neither clears
/// instantly nor persists forever. Long enough that a sustained one cannot spin a core; short
/// enough that a probe arriving just after it clears is not noticeably delayed.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Serves `/readyz`/`/healthz` off `readiness` on an already-bound `listener`, for as long as the
/// process lives. Takes a bound listener, not a `bind` address, so the caller
/// (`logit-cli::pipeline::run_pipelines`) can bind *synchronously* and map a failure there to
/// `RunError::Startup` before spawning anything else -- mirroring `Input::bind`'s own pre-pass,
/// rather than duplicating a second bind-then-serve wrapper nothing else calls. Spawned alongside
/// the existing kill-switch task, and aborted the same way once the pipeline itself returns --
/// deliberately the *only* teardown. A shutdown signal must **not** close this port: the drain it
/// starts is precisely the window `/readyz` exists to answer `503 draining` in
/// (`docs/plans/operator-surface.md`), and a closed port during that window is
/// indistinguishable, to any orchestrator, from a process that crashed.
///
/// Returns nothing, rather than `anyhow::Result<()>`: no failure here has anywhere to go. A
/// failed `accept()` is logged and retried (below), and the caller only ever `abort()`s this
/// task -- never joins it -- so an `Err` return would vanish unread instead of being reported.
pub async fn serve_on(listener: TcpListener, readiness: watch::Receiver<PipelineState>) {
    let connection_limit = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_CONNECTIONS));
    loop {
        let stream = match listener.accept().await {
            Ok((stream, _peer)) => stream,
            Err(err) => {
                // One failed `accept()` must never end this loop. This task's `JoinHandle` is
                // only ever aborted, never awaited, so returning here would take the admin
                // endpoint down permanently and *silently* -- turning a healthy process into an
                // endless restart loop under any orchestrator polling `/readyz`, with no
                // diagnostic trail at all. `ConnectionAborted`/`ConnectionReset`/`Interrupted`
                // are one client's own accident (it hung up between SYN and accept; a signal
                // interrupted the syscall) and cost nothing to retry immediately. Anything else
                // -- `EMFILE`/`ENFILE` under fd pressure being the realistic case -- is a
                // process-wide condition that clears on its own timescale, so it gets a short
                // pause first; without one, a sustained fd exhaustion spins this loop hot
                // against a listener that stays readable and keeps failing.
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

        // Acquired *after* accept, same reasoning as `otlp_in`'s own accept loop: the kernel's
        // own backlog absorbs a burst while every permit is held, rather than refusing the
        // connection outright.
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
    // `HEAD` routes exactly like `GET`, and the full body is built either way: hyper's own HTTP/1
    // server suppresses a HEAD response's body bytes on the wire while still deriving
    // `content-length` from the body it was handed -- which is precisely RFC 9110 §9.3.2's "the
    // same header fields that would have been sent to a GET". Handing it an empty body instead
    // suppresses the header entirely, so `HEAD /readyz` and `GET /readyz` disagree about a length
    // the client is entitled to trust.
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

/// `Phase` -> the wire word `/readyz` actually returns -- deliberately not the same spelling as
/// `Phase::as_str()` (`ready` -> `ok`, `failed` -- degraded`): this mapping is an HTTP-response
/// concern, `Phase`'s own vocabulary is the runtime's.
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

/// Always `200 ok`: this only proves the admin task itself can answer (the tokio runtime is
/// alive), deliberately not the pipeline's own state -- that's `/readyz`'s job.
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

    // `serve_on`/`handle`'s routing and permit logic, driven end to end over real TCP sockets --
    // `handle` itself takes `hyper::body::Incoming`, which (unlike every other body type this
    // crate deals with) has no public constructor outside an actual accepted connection, so
    // there is no cheaper way to exercise the dispatch than a real request.

    /// Sends a raw HTTP/1.1 request and returns (status code, headers block, body) -- the same
    /// raw-socket idiom `crates/logit-inputs/src/otlp.rs`'s own tests (`post_raw`) use, since
    /// this server's whole point is not depending on `logit-cli` having its own HTTP client.
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
        // Bound here, synchronously, before the task is even spawned -- exactly
        // `run_pipelines`'s own "bind first, fail startup on error" shape, and what makes
        // `request_raw` above able to connect immediately with no bind-race retry loop needed:
        // a client can connect into the kernel's accept backlog before `serve_on`'s task ever
        // runs its first `.accept()`.
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
        // Not `ready()`d -- still `Starting`, and `/healthz` must not care.
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

    /// RFC 9110 §9.3.2: a `HEAD` response carries the header fields a `GET` would have sent --
    /// `content-length` included, naming the length of the body the `GET` *would* have returned
    /// -- while sending no body bytes at all. hyper's own HTTP/1 encoder does the suppression
    /// (`can_have_body(HEAD, ..)` forces a zero-length encoder) off the *real* body it is handed,
    /// which is why `handle` builds the full body for `HEAD` and lets hyper drop it.
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

    /// A 17th concurrent connection waits on the permit rather than being refused or erroring --
    /// proven by holding all 16 slots open with idle (request-less) connections, confirming a
    /// 17th gets no response yet, then freeing one slot and confirming it's served immediately
    /// after.
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
