//! A stand-in for STS, a container credential endpoint, or IMDS, for tests of the chain and of
//! the sinks that use it. Behind the `test-util` feature outside this crate's own tests.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// One request an [`HttpStub`] received.
#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    /// The path and query.
    pub path: String,
    /// Header names in lowercase.
    pub headers: HashMap<String, String>,
    pub body: String,
}

/// What an [`HttpStub`] answers a request, given it and how many came before it: a status and
/// a body.
pub type HttpAnswer = Box<dyn Fn(&HttpRequest, usize) -> (u16, String) + Send>;

/// The shared state of an [`HttpStub`].
pub struct HttpStubState {
    pub requests: Vec<HttpRequest>,
    pub answer: HttpAnswer,
}

/// An HTTP/1.1 server on `127.0.0.1` that records every request and answers it with
/// [`HttpStubState::answer`]: a stand-in for STS, a container credential endpoint, or IMDS. It
/// answers `404` until a test sets an answer.
pub struct HttpStub {
    /// `http://127.0.0.1:<port>`, no trailing `/`.
    pub url: String,
    state: Arc<Mutex<HttpStubState>>,
    task: JoinHandle<()>,
}

impl Drop for HttpStub {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl HttpStub {
    pub async fn start() -> Self {
        use http_body_util::{BodyExt, Full};
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(HttpStubState {
            requests: Vec::new(),
            answer: Box::new(|_, _| (404, String::new())),
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let state = state.clone();
                        async move {
                            let method = req.method().to_string();
                            let path = req
                                .uri()
                                .path_and_query()
                                .map(|p| p.as_str().to_string())
                                .unwrap_or_default();
                            let headers = req
                                .headers()
                                .iter()
                                .map(|(name, value)| {
                                    (
                                        name.as_str().to_string(),
                                        value.to_str().unwrap_or_default().to_string(),
                                    )
                                })
                                .collect();
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let request = HttpRequest {
                                method,
                                path,
                                headers,
                                body: String::from_utf8_lossy(&body).into_owned(),
                            };
                            let (status, text) = {
                                let mut state = state.lock().unwrap();
                                let n = state.requests.len();
                                let answer = (state.answer)(&request, n);
                                state.requests.push(request);
                                answer
                            };
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .status(status)
                                    .body(Full::new(Bytes::from(text)))
                                    .unwrap(),
                            )
                        }
                    });
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(stream), service)
                        .await;
                });
            }
        });
        Self { url, state, task }
    }

    pub fn state(&self) -> MutexGuard<'_, HttpStubState> {
        self.state.lock().unwrap()
    }

    /// Sets what every request is answered with.
    pub fn answer(&self, answer: impl Fn(&HttpRequest, usize) -> (u16, String) + Send + 'static) {
        self.state().answer = Box::new(answer);
    }

    /// Every request received so far.
    pub fn requests(&self) -> Vec<HttpRequest> {
        self.state().requests.clone()
    }
}
