//! An OAuth 2.0 token endpoint for the `OAUTHBEARER` client-credentials tests.

use std::sync::{Arc, Mutex, MutexGuard};

use bytes::Bytes;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// One request a [`TokenEndpoint`] received.
#[derive(Debug, Clone)]
pub struct TokenRequest {
    pub authorization: String,
    pub content_type: String,
    pub body: String,
}

/// What a [`TokenEndpoint`] answers its `n`th request (from 0): a status and a body.
pub type TokenAnswer = Box<dyn Fn(usize) -> (u16, String) + Send>;

/// The shared state of a [`TokenEndpoint`].
pub struct TokenState {
    pub requests: Vec<TokenRequest>,
    pub answer: TokenAnswer,
}

/// An OAuth 2.0 token endpoint on `127.0.0.1` over HTTP/1.1, answering every `POST` with
/// [`TokenState::answer`]. By default it issues `token-<n>` with an hour's `expires_in`.
pub struct TokenEndpoint {
    pub url: String,
    state: Arc<Mutex<TokenState>>,
    task: JoinHandle<()>,
}

impl Drop for TokenEndpoint {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TokenEndpoint {
    pub async fn start() -> Self {
        use http_body_util::{BodyExt, Full};
        use hyper::service::service_fn;
        use hyper_util::rt::TokioIo;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/oauth2/token", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(TokenState {
            requests: Vec::new(),
            answer: Box::new(|n| {
                (
                    200,
                    format!(
                        r#"{{"access_token":"token-{n}","token_type":"Bearer","expires_in":3600}}"#
                    ),
                )
            }),
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let state = shared.clone();
                tokio::spawn(async move {
                    let service = service_fn(move |req: http::Request<hyper::body::Incoming>| {
                        let state = state.clone();
                        async move {
                            let header = |name: http::header::HeaderName| {
                                req.headers()
                                    .get(name)
                                    .and_then(|v| v.to_str().ok())
                                    .unwrap_or_default()
                                    .to_string()
                            };
                            let request = TokenRequest {
                                authorization: header(http::header::AUTHORIZATION),
                                content_type: header(http::header::CONTENT_TYPE),
                                body: String::new(),
                            };
                            let body = req.into_body().collect().await.unwrap().to_bytes();
                            let (status, text) = {
                                let mut state = state.lock().unwrap();
                                let n = state.requests.len();
                                state.requests.push(TokenRequest {
                                    body: String::from_utf8_lossy(&body).into_owned(),
                                    ..request
                                });
                                (state.answer)(n)
                            };
                            Ok::<_, std::convert::Infallible>(
                                http::Response::builder()
                                    .status(status)
                                    .header(http::header::CONTENT_TYPE, "application/json")
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

    pub fn state(&self) -> MutexGuard<'_, TokenState> {
        self.state.lock().unwrap()
    }

    /// How many token requests have arrived.
    pub fn fetches(&self) -> usize {
        self.state().requests.len()
    }
}
