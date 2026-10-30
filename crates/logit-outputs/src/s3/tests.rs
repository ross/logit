//! `s3_out` against an in-process S3 stand-in: one test per row of the module doc's response
//! table, the key and the head's pin across retries, the multipart path's parts, poisoning, and
//! aborts, and objects read back as `replay_in` reads them.

use super::*;
use logit_core::{AttrMap, BodyFormat, Event, LogRecord, Resource, Value};
use logit_pipeline::test_util::{wait_until, TelemetryProbe};
use logit_pipeline::{classify, SinkQueue, SinkQueueConfig, SinkStore};
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::io::Read;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::net::TcpListener;
use tokio::sync::Notify;

/// 2026-10-30T14:25:01.123Z, in nanoseconds.
const T0: i64 = 1_793_370_301_123_000_000;

thread_local! {
    /// The test clock: each test runs on its own current-thread runtime.
    static NOW: Cell<i64> = const { Cell::new(T0) };
}

fn test_clock() -> i64 {
    NOW.with(Cell::get)
}

// ---------------------------------------------------------------------------------------------
// The stand-in
// ---------------------------------------------------------------------------------------------

/// One request the stand-in received.
#[derive(Debug, Clone)]
struct Seen {
    method: String,
    path: String,
    /// Names lowercased.
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// How the stand-in answers one request.
enum Answer {
    Reply {
        status: u16,
        headers: Vec<(&'static str, String)>,
        body: Vec<u8>,
    },
    /// Never answers.
    Hang,
    /// Answers once the test notifies.
    After(Arc<Notify>, Box<Answer>),
    /// Answers `status`, sends `body`, and drops the connection before the rest of the length it
    /// announced.
    Cut {
        status: u16,
        body: Vec<u8>,
    },
}

/// A response body that sends its bytes, then fails, so the server drops the connection short of
/// the length it announced. It yields once between the two, which lets the server flush the head
/// and the bytes before the failure.
struct CutBody {
    bytes: Option<bytes::Bytes>,
    yielded: bool,
    announced: u64,
}

impl hyper::body::Body for CutBody {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<hyper::body::Frame<bytes::Bytes>, std::io::Error>>> {
        if let Some(bytes) = self.bytes.take() {
            return std::task::Poll::Ready(Some(Ok(hyper::body::Frame::data(bytes))));
        }
        if !self.yielded {
            self.yielded = true;
            cx.waker().wake_by_ref();
            return std::task::Poll::Pending;
        }
        std::task::Poll::Ready(Some(Err(std::io::Error::other("the stand-in cut the body"))))
    }

    fn size_hint(&self) -> hyper::body::SizeHint {
        hyper::body::SizeHint::with_exact(self.announced)
    }
}

type StandinBody = http_body_util::combinators::BoxBody<bytes::Bytes, std::io::Error>;

/// A case's name and the answer it gets.
type Named = (&'static str, fn() -> Answer);

fn reply(status: u16, body: impl Into<Vec<u8>>) -> Answer {
    Answer::Reply { status, headers: Vec::new(), body: body.into() }
}

fn ok() -> Answer {
    reply(200, Vec::new())
}

/// An S3 error body naming `code`.
fn s3_error(code: &str) -> Vec<u8> {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<Error><Code>{code}</Code>\
         <Message>the store says {code}</Message><RequestId>R</RequestId></Error>"
    )
    .into_bytes()
}

type SeenLog = Arc<Mutex<Vec<Seen>>>;

/// An HTTP/1.1 stand-in on `127.0.0.1` that records every request, then answers the `n`th (from
/// 0) with `respond(n, request)`.
async fn standin(
    respond: impl Fn(usize, &Seen) -> Answer + Send + Sync + 'static,
) -> (SocketAddr, SeenLog) {
    use http_body_util::BodyExt as _;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let log: SeenLog = Arc::default();
    let respond = Arc::new(respond);
    let task_log = Arc::clone(&log);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let (log, respond) = (Arc::clone(&task_log), Arc::clone(&respond));
            tokio::spawn(async move {
                let svc = hyper::service::service_fn(move |req: http::Request<_>| {
                    let (log, respond) = (Arc::clone(&log), Arc::clone(&respond));
                    async move {
                        let method = req.method().to_string();
                        let path = req.uri().to_string();
                        let headers = req
                            .headers()
                            .iter()
                            .map(|(k, v)| (k.as_str().to_string(), v.to_str().unwrap().to_string()))
                            .collect();
                        let incoming: hyper::body::Incoming = req.into_body();
                        let body = incoming.collect().await.unwrap().to_bytes().to_vec();
                        let seen = Seen { method, path, headers, body };
                        let n = {
                            let mut log = log.lock().unwrap();
                            log.push(seen.clone());
                            log.len() - 1
                        };
                        let mut answer = respond(n, &seen);
                        let (status, headers, body): (_, _, StandinBody) = loop {
                            match answer {
                                Answer::Reply { status, headers, body } => {
                                    let body = http_body_util::Full::new(bytes::Bytes::from(body))
                                        .map_err(|never| match never {})
                                        .boxed();
                                    break (status, headers, body);
                                }
                                Answer::Cut { status, body } => {
                                    let announced = 2 * body.len() as u64 + 1;
                                    let body = CutBody {
                                        bytes: Some(body.into()),
                                        yielded: false,
                                        announced,
                                    };
                                    let body = body.boxed();
                                    break (status, Vec::new(), body);
                                }
                                Answer::Hang => std::future::pending().await,
                                Answer::After(notify, then) => {
                                    notify.notified().await;
                                    answer = *then;
                                }
                            }
                        };
                        let mut response = http::Response::builder().status(status);
                        for (name, value) in headers {
                            response = response.header(name, value);
                        }
                        Ok::<_, std::convert::Infallible>(response.body(body).unwrap())
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(hyper_util::rt::TokioIo::new(stream), svc)
                    .await;
            });
        }
    });
    (addr, log)
}

fn seen(log: &SeenLog) -> Vec<Seen> {
    log.lock().unwrap().clone()
}

// ---------------------------------------------------------------------------------------------
// The sink and its batches
// ---------------------------------------------------------------------------------------------

const ACCESS_KEY_ID: &str = "AKIDEXAMPLE";
const SECRET: &str = "secret-access-key-value";

fn settings(addr: SocketAddr) -> S3Settings {
    let mut settings = S3Settings::new("bucket", "us-east-1");
    settings.endpoint = Some(format!("http://{addr}"));
    settings.prefix = "logs/".to_string();
    settings.credentials = Some(StaticCredentials {
        access_key_id: ACCESS_KEY_ID.to_string(),
        secret_access_key: SECRET.to_string(),
        session_token: None,
    });
    settings
}

fn sink_with(settings: S3Settings, probe: &TelemetryProbe) -> S3Output {
    S3Output::new(settings)
        .unwrap()
        .with_clock(test_clock)
        .with_telemetry(probe.telemetry("out", "s3_out", "sink"))
}

fn sink(addr: SocketAddr) -> S3Output {
    sink_with(settings(addr), &TelemetryProbe::new())
}

/// A batch of one log event whose message is `message`.
fn batch(message: &str) -> EventBatch {
    let record = LogRecord {
        message: Value::str(message),
        severity: None,
        body_format: BodyFormat::Raw,
        trace: None,
        event_name: None,
        observed_timestamp: 0,
        dropped_attributes_count: 0,
    };
    EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![Event::log(1_000, AttrMap::new(), record)],
    }
}

const STORE: [u8; 16] = [0xab; 16];

fn seq(n: u64) -> SeqId {
    SeqId { id: STORE, seq: n }
}

/// Stages `batches` as batches `first..` of the store and completes the object, as the write
/// loop does for one window.
async fn put(out: &mut S3Output, first: u64, batches: &[EventBatch]) -> anyhow::Result<()> {
    for (n, batch) in (first..).zip(batches) {
        out.submit(batch, BatchContext::default(), seq(n)).await?;
    }
    out.await_ack().await
}

/// The fault of one `PutObject` the stand-in answers with `answer`, and the error's text.
async fn fault_of(answer: impl Fn() -> Answer + Send + Sync + 'static) -> (Fault, String) {
    let (addr, _log) = standin(move |_, _| answer()).await;
    let err = put(&mut sink(addr), 1, &[batch("a")]).await.unwrap_err();
    (classify(&err), format!("{err:#}"))
}

/// Asserts each of `codes` answered with `status` reads as `want`.
async fn assert_codes(status: u16, codes: &[&'static str], want: Fault) {
    for &code in codes {
        let (fault, message) = fault_of(move || reply(status, s3_error(code))).await;
        assert_eq!(fault, want, "{status} {code}: {message}");
        assert!(message.contains(code), "{message}");
    }
}

fn gunzip(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::MultiGzDecoder::new(body).read_to_end(&mut out).unwrap();
    out
}

fn unzstd(body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut rest = body;
    while !rest.is_empty() {
        let mut decoder = ruzstd::decoding::StreamingDecoder::new(&mut rest).unwrap();
        decoder.read_to_end(&mut out).unwrap();
    }
    out
}

/// The messages of an object's events, read as `replay_in` reads a `format: json` file.
fn json_messages(lines: &[u8]) -> Vec<String> {
    let batches = logit_proto::event_json::EventJsonDecoder::new().decode_batches(lines).unwrap();
    messages(batches)
}

/// The messages of an object's events, read as `replay_in` reads a `format: native` file:
/// concatenated bare frames.
fn native_messages(frames: &[u8]) -> Vec<String> {
    let mut rest = bytes::Bytes::copy_from_slice(frames);
    let mut batches = Vec::new();
    while !rest.is_empty() {
        let (codec, mut payload) = logit_proto::frame::read_frame(&mut rest).unwrap();
        assert_eq!(codec, logit_proto::native::CODEC_BATCH);
        let budget = logit_proto::native::DecodeBudget::default();
        batches.push(logit_proto::native::decode_batch(&mut payload, &budget).unwrap());
    }
    messages(batches)
}

fn messages(batches: Vec<EventBatch>) -> Vec<String> {
    batches
        .into_iter()
        .flat_map(|b| b.events)
        .map(|e| e.log.unwrap().message.as_str().unwrap().to_string())
        .collect()
}

/// How many events the gzipped JSON objects the stand-in received from request `from` on hold.
fn stored_messages(log: &SeenLog, from: usize) -> usize {
    seen(log).iter().skip(from).map(|r| json_messages(&gunzip(&r.body)).len()).sum()
}

fn names(n: usize) -> Vec<String> {
    (1..=n).map(|i| format!("m{i}")).collect()
}

/// The `format: json` bytes of `batch(message)`.
fn line_len(message: &str) -> u64 {
    let mut out = String::new();
    crate::ndjson::render_ndjson(&mut out, &batch(message));
    out.len() as u64
}

/// The multipart stand-in's settings: no compression, so part boundaries are byte counts, and
/// parts of 32 bytes, so every batch cuts at least one.
fn multipart(addr: SocketAddr) -> S3Settings {
    let mut settings = settings(addr);
    settings.compression = ObjectCompression::None;
    settings.part_size = PART;
    settings
}

const PART: u64 = 32;

/// Which S3 operation `seen` is.
fn op_of(seen: &Seen) -> &'static str {
    match (seen.method.as_str(), param(seen, "uploads").is_some(), param(seen, "partNumber")) {
        ("DELETE", ..) => "abort",
        ("POST", true, _) => "create",
        ("POST", false, _) => "complete",
        ("PUT", _, Some(_)) => "part",
        _ => "put",
    }
}

/// The value of `name` in `seen`'s query, `Some("")` when it has none.
fn param(seen: &Seen, name: &str) -> Option<String> {
    let (_, query) = seen.path.split_once('?')?;
    query.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        (key == name).then(|| value.to_string())
    })
}

/// `seen`'s path without its query: the bucket and the key.
fn key_path(seen: &Seen) -> String {
    seen.path.split_once('?').map_or(seen.path.as_str(), |(path, _)| path).to_string()
}

/// A working S3 store: it answers every request as S3 does and keeps what it stores, by path.
#[derive(Default)]
struct Model {
    uploads: AtomicUsize,
    /// Each upload's parts by number.
    parts: Mutex<HashMap<String, BTreeMap<u32, Vec<u8>>>>,
    objects: Mutex<HashMap<String, Vec<u8>>>,
}

impl Model {
    fn answer(&self, seen: &Seen) -> Answer {
        let upload = param(seen, "uploadId").unwrap_or_default();
        match op_of(seen) {
            "create" => {
                let id = format!("upload-{}", self.uploads.fetch_add(1, Ordering::SeqCst));
                self.parts.lock().unwrap().insert(id.clone(), BTreeMap::new());
                reply(
                    200,
                    format!(
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<InitiateMultipartUploadResult>\
                         <Bucket>bucket</Bucket><Key>k</Key><UploadId>{id}</UploadId>\
                         </InitiateMultipartUploadResult>"
                    ),
                )
            }
            "part" => {
                let number: u32 = param(seen, "partNumber").unwrap().parse().unwrap();
                let mut parts = self.parts.lock().unwrap();
                let Some(upload_parts) = parts.get_mut(&upload) else {
                    return reply(404, s3_error("NoSuchUpload"));
                };
                upload_parts.insert(number, seen.body.clone());
                Answer::Reply {
                    status: 200,
                    headers: vec![("etag", format!("\"{upload}-{number}\""))],
                    body: Vec::new(),
                }
            }
            "complete" => match self.assembled(&upload) {
                Some(body) => {
                    self.objects.lock().unwrap().insert(key_path(seen), body);
                    self.parts.lock().unwrap().remove(&upload);
                    reply(
                        200,
                        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
                         <CompleteMultipartUploadResult><Key>k</Key><ETag>\"x-2\"</ETag>\
                         </CompleteMultipartUploadResult>",
                    )
                }
                None => reply(404, s3_error("NoSuchUpload")),
            },
            "abort" => match self.parts.lock().unwrap().remove(&upload) {
                Some(_) => reply(204, Vec::new()),
                None => reply(404, s3_error("NoSuchUpload")),
            },
            _ => {
                self.objects.lock().unwrap().insert(key_path(seen), seen.body.clone());
                ok()
            }
        }
    }

    /// `upload`'s parts in number order, or `None` for an upload it doesn't hold.
    fn assembled(&self, upload: &str) -> Option<Vec<u8>> {
        let parts = self.parts.lock().unwrap();
        Some(parts.get(upload)?.values().flatten().copied().collect())
    }

    /// The messages of every stored object, by key, read as uncompressed JSON lines.
    fn stored(&self) -> BTreeMap<String, Vec<String>> {
        let objects = self.objects.lock().unwrap();
        objects.iter().map(|(key, body)| (key.clone(), json_messages(body))).collect()
    }
}

/// A stand-in that answers as `model` does.
async fn model_standin(model: &Arc<Model>) -> (SocketAddr, SeenLog) {
    let model = Arc::clone(model);
    standin(move |_, seen| model.answer(seen)).await
}

/// The operations of every request `log` holds, in order.
fn ops(log: &SeenLog) -> Vec<&'static str> {
    seen(log).iter().map(op_of).collect()
}

// ---------------------------------------------------------------------------------------------
// Response classes, one test per row (the module doc's table)
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_2xx_delivers_the_object() {
    let (addr, log) = standin(|_, _| ok()).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink_with(settings(addr), &probe);
    put(&mut out, 7, &[batch("m1"), batch("m2")]).await.unwrap();
    assert_eq!(out.window(), 1, "one answer owed behind the head");
    out.await_ack().await.unwrap();
    assert_eq!(out.window(), usize::MAX);

    let requests = seen(&log);
    assert_eq!(requests.len(), 1);
    let request = &requests[0];
    assert_eq!(request.method, "PUT");
    let key = "logs/dt=2026-10-30/hour=14/20261030T142501.123Z-abababababababababababababababab-\
               00000000000000000007.ndjson.gz";
    assert_eq!(request.path, format!("/bucket/{}", key.replace('=', "%3D")));
    let hash = object::hex(ring::digest::digest(&ring::digest::SHA256, &request.body).as_ref());
    assert_eq!(request.headers["x-amz-content-sha256"], hash);
    assert_eq!(request.headers["content-length"], request.body.len().to_string());
    assert_eq!(request.headers["content-type"], "application/gzip");
    assert_eq!(request.headers["x-amz-date"], "20261030T142501Z");
    assert!(!request.headers.contains_key("content-encoding"));
    assert!(!request.headers.contains_key("x-amz-security-token"));
    assert!(!request.headers.contains_key("transfer-encoding"), "a fixed-length body");
    let authorization = &request.headers["authorization"];
    let prefix = "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20261030/us-east-1/s3/aws4_request,\
                  SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date,Signature=";
    assert!(authorization.starts_with(prefix), "{authorization}");
    assert!(!authorization.contains(SECRET));
    // The signature over what arrived, with the decoded path, is the one sent.
    let signed: Vec<(String, String)> =
        ["content-type", "host", "x-amz-content-sha256", "x-amz-date"]
            .iter()
            .map(|name| (name.to_string(), request.headers[*name].clone()))
            .collect();
    let credentials = out.credentials().await.unwrap();
    let date = AmzDate::from_unix_nanos(T0);
    let expected = sign::authorization(
        &credentials,
        "us-east-1",
        &date,
        "PUT",
        &format!("/bucket/{key}"),
        &[],
        &signed,
        &hash,
    );
    assert_eq!(*authorization, expected);
    assert_eq!(json_messages(&gunzip(&request.body)), names(2));

    let totals = probe.poll();
    assert_eq!(totals.sum("logit.output.requests", &[("op", "put_object"), ("class", "2xx")]), 1.0);
    assert_eq!(totals.sum("logit.output.objects", &[]), 1.0);
    assert_eq!(totals.sum("logit.output.bytes", &[]), request.body.len() as f64);
}

#[tokio::test]
async fn a_3xx_is_refused_and_names_the_buckets_region() {
    for status in [301, 307, 302] {
        let (fault, message) = fault_of(move || Answer::Reply {
            status,
            headers: vec![("x-amz-bucket-region", "eu-west-1".to_string())],
            body: s3_error("PermanentRedirect"),
        })
        .await;
        assert_eq!(fault, Fault::Refused, "{status}: {message}");
        assert!(message.contains(r#"region "eu-west-1""#), "{message}");
    }
}

#[tokio::test]
async fn a_body_that_didnt_arrive_whole_is_clean() {
    assert_codes(
        400,
        &["RequestTimeout", "IncompleteBody", "BadDigest", "XAmzContentSHA256Mismatch"],
        Fault::Clean,
    )
    .await;
}

/// `ExpiredToken` drops the chain's cached credentials, so the retry signs with fresh ones.
#[tokio::test]
async fn an_expired_token_is_clean_and_forgets_the_chains_credentials() {
    let creds = logit_aws::test_stub::HttpStub::start().await;
    creds.answer(|_, n| {
        (
            200,
            format!(
                r#"{{"AccessKeyId":"AKID{n}","SecretAccessKey":"secret-{n}",
                    "Token":"session-{n}","Expiration":"2099-01-01T00:00:00Z"}}"#
            ),
        )
    });
    let full = format!("{}/creds", creds.url);
    let env: logit_aws::creds::Env = Arc::new(move |name: &str| {
        (name == "AWS_CONTAINER_CREDENTIALS_FULL_URI").then(|| full.clone())
    });
    let closed = "http://127.0.0.1:1".to_string();
    let endpoints =
        logit_aws::creds::Endpoints { imds: closed.clone(), container: closed, sts: None };
    let (addr, log) = standin(|n, _| {
        if n == 0 {
            reply(400, s3_error("ExpiredToken"))
        } else if n == 1 {
            reply(400, s3_error("TokenRefreshRequired"))
        } else {
            ok()
        }
    })
    .await;
    let mut settings = settings(addr);
    settings.credentials = None;
    let mut out = S3Output::new(settings)
        .unwrap()
        .with_clock(test_clock)
        .with_credential_chain(Chain::with(env, endpoints, Duration::from_secs(5)));
    for code in ["ExpiredToken", "TokenRefreshRequired"] {
        let err = put(&mut out, 1, &[batch("a")]).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean, "{err:#}");
        assert!(format!("{err:#}").contains(code));
    }
    put(&mut out, 1, &[batch("a")]).await.unwrap();
    assert_eq!(creds.requests().len(), 3, "each expired token fetched the credentials again");
    let tokens: Vec<String> =
        seen(&log).iter().map(|r| r.headers["x-amz-security-token"].clone()).collect();
    assert_eq!(tokens, ["session-0", "session-1", "session-2"]);
    for request in seen(&log) {
        assert!(request.headers["authorization"].contains(
            "SignedHeaders=content-type;host;\
             x-amz-content-sha256;x-amz-date;x-amz-security-token,"
        ));
    }
}

#[tokio::test]
async fn a_body_too_large_is_refused() {
    assert_codes(400, &["EntityTooLarge"], Fault::Refused).await;
    let (fault, message) = fault_of(|| reply(413, "Request Entity Too Large")).await;
    assert_eq!(fault, Fault::Refused, "{message}");
}

#[tokio::test]
async fn a_configuration_code_is_refused() {
    assert_codes(
        400,
        &[
            "InvalidArgument",
            "InvalidToken",
            "AuthorizationHeaderMalformed",
            "InvalidBucketName",
            "InvalidEncryptionAlgorithmError",
            "KMS.NotFoundException",
            "KMS.DisabledException",
            "InvalidStorageClass",
            "KeyTooLongError",
            "InvalidRequest",
        ],
        Fault::Refused,
    )
    .await;
    // A KMS code reads as configuration under any `4xx`: MinIO answers one with `409`.
    assert_codes(409, &["KMS.DefaultKeyAlreadyConfiguredException"], Fault::Refused).await;
}

#[tokio::test]
async fn a_401_405_or_407_is_refused() {
    for status in [401, 405, 407] {
        let (fault, message) = fault_of(move || reply(status, "")).await;
        assert_eq!(fault, Fault::Refused, "{status}: {message}");
    }
    assert_codes(405, &["MethodNotAllowed"], Fault::Refused).await;
}

#[tokio::test]
async fn a_403_is_refused() {
    assert_codes(
        403,
        &["AccessDenied", "SignatureDoesNotMatch", "InvalidAccessKeyId"],
        Fault::Refused,
    )
    .await;
    let (fault, message) = fault_of(|| reply(403, "Forbidden")).await;
    assert_eq!(fault, Fault::Refused, "{message}");
}

#[tokio::test]
async fn a_404_is_refused() {
    assert_codes(404, &["NoSuchBucket", "NoSuchKey"], Fault::Refused).await;
    let (fault, message) = fault_of(|| reply(404, "Not Found")).await;
    assert_eq!(fault, Fault::Refused, "{message}");
}

#[tokio::test]
async fn a_408_is_ambiguous() {
    let (fault, message) = fault_of(|| reply(408, "")).await;
    assert_eq!(fault, Fault::Ambiguous, "{message}");
}

#[tokio::test]
async fn a_409_operation_aborted_is_clean() {
    assert_codes(409, &["OperationAborted"], Fault::Clean).await;
}

#[tokio::test]
async fn a_411_is_refused() {
    assert_codes(411, &["MissingContentLength"], Fault::Refused).await;
}

#[tokio::test]
async fn a_429_or_a_503_with_a_code_is_clean() {
    assert_codes(429, &["SlowDown"], Fault::Clean).await;
    let (fault, message) = fault_of(|| reply(429, "Too Many Requests")).await;
    assert_eq!(fault, Fault::Clean, "{message}");
    assert_codes(
        503,
        &["SlowDown", "ServiceUnavailable", "SlowDownWrite", "SlowDownRead"],
        Fault::Clean,
    )
    .await;
}

#[tokio::test]
async fn a_503_with_no_code_is_ambiguous() {
    let (fault, message) =
        fault_of(|| reply(503, "<html><body>503 Service Unavailable</body></html>")).await;
    assert_eq!(fault, Fault::Ambiguous, "{message}");
}

#[tokio::test]
async fn a_5xx_is_ambiguous() {
    assert_codes(500, &["InternalError"], Fault::Ambiguous).await;
    for status in [500, 502, 504, 599] {
        let (fault, message) = fault_of(move || reply(status, "")).await;
        assert_eq!(fault, Fault::Ambiguous, "{status}: {message}");
    }
}

#[tokio::test]
async fn a_501_is_refused() {
    assert_codes(501, &["NotImplemented"], Fault::Refused).await;
}

#[tokio::test]
async fn any_other_4xx_is_rejected() {
    assert_codes(400, &["MalformedXML", "SomethingNew"], Fault::Rejected).await;
    assert_codes(409, &["BucketAlreadyExists"], Fault::Rejected).await;
    // The sink sends no ACL, so this can't arrive; it has no row of its own.
    assert_codes(400, &["AccessControlListNotSupported"], Fault::Rejected).await;
    for status in [400, 418, 422] {
        let (fault, message) = fault_of(move || reply(status, "")).await;
        assert_eq!(fault, Fault::Rejected, "{status}: {message}");
    }
}

#[tokio::test]
async fn a_refused_or_stalled_dial_is_clean() {
    let addr = crate::test_support::refused_addr().await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink_with(settings(addr), &probe);
    let err = put(&mut out, 1, &[batch("a")]).await.unwrap_err();
    assert_eq!(classify(&err), Fault::Clean, "{err:#}");
    assert_eq!(out.reached_destination(1), 1, "the request was sent toward the store");

    let (addr, _listener, _held) = crate::test_support::full_backlog_listener().await;
    let mut settings = settings(addr);
    settings.connect_timeout = Duration::from_millis(200);
    let mut out = sink_with(settings, &probe);
    let err = put(&mut out, 1, &[batch("a")]).await.unwrap_err();
    assert_eq!(classify(&err), Fault::Clean, "{err:#}");
    assert_eq!(
        probe
            .poll()
            .sum("logit.output.requests", &[("op", "put_object"), ("class", "connect_error")]),
        2.0
    );
    assert_eq!(probe.poll().sum("logit.output.bytes", &[]), 0.0, "a failed dial sent nothing");
}

#[tokio::test]
async fn a_request_timeout_is_ambiguous() {
    let (addr, _log) = standin(|_, _| Answer::Hang).await;
    let mut probe = TelemetryProbe::new();
    let mut settings = settings(addr);
    settings.timeout = Duration::from_millis(200);
    let mut out = sink_with(settings, &probe);
    let err = put(&mut out, 1, &[batch("a")]).await.unwrap_err();
    assert_eq!(classify(&err), Fault::Ambiguous, "{err:#}");
    assert_eq!(
        probe
            .poll()
            .sum("logit.output.requests", &[("op", "put_object"), ("class", "network_error")]),
        1.0
    );
}

// ---------------------------------------------------------------------------------------------
// The window, the key, and the pin
// ---------------------------------------------------------------------------------------------

/// The object closes once its encoded bytes reach `max_bytes`; a compressed body past
/// `part_size` cuts parts and leaves it open.
#[tokio::test]
async fn an_object_is_full_at_max_bytes_not_at_part_size() {
    let model = Arc::new(Model::default());
    let (addr, _log) = model_standin(&model).await;
    let one = line_len("m1");
    let mut settings = settings(addr);
    settings.max_bytes = 2 * one;
    let mut out = sink_with(settings, &TelemetryProbe::new());
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    assert!(out.collecting().is_some());
    out.submit(&batch("m2"), BatchContext::default(), seq(2)).await.unwrap();
    assert_eq!(out.collecting(), None, "max_bytes reached");

    let mut settings = multipart(addr);
    settings.part_size = one;
    let mut out = sink_with(settings, &TelemetryProbe::new());
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    out.submit(&batch("m2"), BatchContext::default(), seq(2)).await.unwrap();
    assert!(out.collecting().is_some(), "two parts cut, and the object still collects");
}

/// A retry of the head writes the same key, keeps its `object_time` though the clock moved, and
/// closes at the pinned batch count though more batches wait in the store; the next object's key
/// takes the clock's new time.
#[tokio::test]
async fn a_retry_writes_the_same_key_and_closes_at_the_pinned_count() {
    let release = Arc::new(Notify::new());
    let held = Arc::clone(&release);
    let (addr, log) = standin(move |n, _| match n {
        0 => Answer::After(Arc::clone(&held), Box::new(reply(503, s3_error("SlowDown")))),
        _ => ok(),
    })
    .await;
    let mut settings = settings(addr);
    settings.max_age = Duration::from_millis(300);
    let mut probe = TelemetryProbe::new();
    let telemetry = probe.telemetry("out", "s3_out", "sink");
    let mut out = sink_with(settings, &probe);
    let store =
        Arc::new(SinkStore::Memory(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone())));
    let push = |message: &str| {
        let store = Arc::clone(&store);
        let batch = Arc::new(batch(message));
        async move { store.push((batch, BatchContext::default())).await }
    };
    for message in ["m1", "m2", "m3"] {
        push(message).await;
    }
    let producer = async {
        wait_until("the first upload", || seen(&log).len() == 1).await;
        NOW.with(|now| now.set(T0 + 5_000_000_000));
        push("m4").await;
        push("m5").await;
        release.notify_one();
        wait_until("the retry", || seen(&log).len() == 2).await;
        store.close();
    };
    let config = crate::test_support::fast_retry();
    logit_pipeline::test_util::drive_write_loop_over(
        &mut out,
        Arc::clone(&store),
        config,
        telemetry,
        producer,
    )
    .await;

    let requests = seen(&log);
    assert_eq!(requests.len(), 3, "the failed object, its retry, and the next object");
    assert_eq!(requests[0].path, requests[1].path, "the retry writes the same key");
    assert!(requests[1].path.contains("20261030T142501.123Z"), "{}", requests[1].path);
    assert_eq!(json_messages(&gunzip(&requests[0].body)), names(3));
    assert_eq!(json_messages(&gunzip(&requests[1].body)), names(3), "closed at the pin");
    assert!(requests[2].path.contains("20261030T142506.123Z-"), "{}", requests[2].path);
    assert!(requests[2].path.ends_with("-00000000000000000004.ndjson.gz"), "{}", requests[2].path);
    assert_eq!(json_messages(&gunzip(&requests[2].body)), ["m4", "m5"]);
    let totals = probe.poll();
    assert_eq!(totals.sum("logit.component.window.closed", &[("cause", "full")]), 1.0);
    assert_eq!(totals.sum("logit.output.objects", &[]), 2.0);
    assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 5.0);
}

/// A retry that closes at fewer batches than the pin, as store pressure or the shutdown signal
/// would close it, lowers the pin, and the next retry closes there.
#[tokio::test]
async fn a_retry_that_closes_earlier_lowers_the_pin() {
    let (addr, log) = standin(|n, _| if n < 2 { reply(500, "") } else { ok() }).await;
    let mut out = sink(addr);
    let batches: Vec<EventBatch> = names(3).iter().map(|m| batch(m)).collect();
    put(&mut out, 1, &batches).await.unwrap_err();
    put(&mut out, 1, &batches[..2]).await.unwrap_err();
    out.submit(&batches[0], BatchContext::default(), seq(1)).await.unwrap();
    assert!(out.collecting().is_some());
    out.submit(&batches[1], BatchContext::default(), seq(2)).await.unwrap();
    assert_eq!(out.collecting(), None, "the pin fell to 2");
    out.await_ack().await.unwrap();
    let requests = seen(&log);
    assert!(requests.iter().all(|r| r.path == requests[0].path), "one key throughout");
    assert_eq!(json_messages(&gunzip(&requests[2].body)), names(2));

    // A new head gets a new pin.
    out.await_ack().await.unwrap();
    put(&mut out, 3, &batches[2..]).await.unwrap();
    assert_ne!(seen(&log)[3].path, requests[0].path);
}

/// An encode that fails past the head leaves the object as it was; one at the head is the
/// batch's own `Rejected` fault.
#[tokio::test]
async fn an_encode_failure_leaves_the_object_staged() {
    let (addr, log) = standin(|_, _| ok()).await;
    let mut settings = settings(addr);
    settings.format = ObjectFormat::Native;
    let mut out = sink_with(settings, &TelemetryProbe::new());
    let mut huge = batch("x");
    huge.events[0].log.as_mut().unwrap().message =
        Value::str("x".repeat(logit_proto::frame::MAX_SANE_UNCOMPRESSED_LEN as usize + 1));
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    out.submit(&huge, BatchContext::default(), seq(2)).await.unwrap_err();
    out.await_ack().await.unwrap();
    assert_eq!(native_messages(&gunzip(&seen(&log)[0].body)), ["m1"]);

    let err = out.submit(&huge, BatchContext::default(), seq(2)).await.unwrap_err();
    assert_eq!(classify(&err), Fault::Rejected, "{err:#}");
}

/// The grace counts none of an object as having reached the store until its `PutObject` is
/// sent.
#[tokio::test]
async fn nothing_reaches_the_destination_before_the_put_is_sent() {
    let (addr, log) = standin(|_, _| Answer::Hang).await;
    let mut out = sink(addr);
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    assert_eq!(out.reached_destination(1), 0);
    let upload = tokio::time::timeout(Duration::from_millis(200), out.await_ack()).await;
    assert!(upload.is_err(), "the stand-in never answers");
    assert_eq!(out.reached_destination(1), 1);
    // A submit after the cancelled close opens a new object under the same key.
    NOW.with(|now| now.set(T0 + 5_000_000_000));
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    assert_eq!(out.reached_destination(1), 0);
    let upload = tokio::time::timeout(Duration::from_millis(200), out.await_ack()).await;
    assert!(upload.is_err(), "the stand-in never answers");
    let requests = seen(&log);
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[1].path, requests[0].path, "the same key, its object_time pinned");
}

/// A head that fills the object alone opens no collecting window, and the fill stops at it: three
/// queued batches under `max_bytes: 1` are three objects of one batch each.
#[tokio::test]
async fn a_head_that_fills_the_object_alone_is_its_object() {
    let (addr, log) = standin(|_, _| ok()).await;
    let mut settings = settings(addr);
    settings.max_bytes = 1;
    let probe = TelemetryProbe::new();
    let telemetry = probe.telemetry("out", "s3_out", "sink");
    let mut out = sink_with(settings, &probe);
    let store =
        Arc::new(SinkStore::Memory(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone())));
    for message in ["m1", "m2", "m3"] {
        store.push((Arc::new(batch(message)), BatchContext::default())).await;
    }
    let producer = async {
        wait_until("every batch uploaded", || stored_messages(&log, 0) == 3).await;
        store.close();
    };
    let config = crate::test_support::fast_retry();
    logit_pipeline::test_util::drive_write_loop_over(
        &mut out,
        Arc::clone(&store),
        config,
        telemetry,
        producer,
    )
    .await;
    let bodies: Vec<Vec<String>> =
        seen(&log).iter().map(|r| json_messages(&gunzip(&r.body))).collect();
    assert_eq!(bodies, [["m1"], ["m2"], ["m3"]]);
}

/// A one-batch object the deadline closed is retried holding its head alone, though more batches
/// arrived during the backoff: its pin is 1, and the earlier attempt may still land.
#[tokio::test]
async fn a_retry_of_a_one_batch_object_holds_the_head_alone() {
    let release = Arc::new(Notify::new());
    let held = Arc::clone(&release);
    let (addr, log) = standin(move |n, _| match n {
        0 => Answer::After(Arc::clone(&held), Box::new(reply(500, s3_error("InternalError")))),
        _ => ok(),
    })
    .await;
    let mut settings = settings(addr);
    settings.max_age = Duration::from_millis(100);
    let probe = TelemetryProbe::new();
    let telemetry = probe.telemetry("out", "s3_out", "sink");
    let mut out = sink_with(settings, &probe);
    let store =
        Arc::new(SinkStore::Memory(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone())));
    store.push((Arc::new(batch("m1")), BatchContext::default())).await;
    let producer = async {
        wait_until("the first upload", || seen(&log).len() == 1).await;
        for message in ["m2", "m3"] {
            store.push((Arc::new(batch(message)), BatchContext::default())).await;
        }
        release.notify_one();
        wait_until("every batch uploaded after the failure", || stored_messages(&log, 1) == 3)
            .await;
        store.close();
    };
    let config = crate::test_support::fast_retry();
    logit_pipeline::test_util::drive_write_loop_over(
        &mut out,
        Arc::clone(&store),
        config,
        telemetry,
        producer,
    )
    .await;
    let requests = seen(&log);
    assert_eq!(requests[1].path, requests[0].path, "the retry writes the same key");
    assert_eq!(json_messages(&gunzip(&requests[0].body)), ["m1"]);
    assert_eq!(json_messages(&gunzip(&requests[1].body)), ["m1"], "the retry holds the head alone");
    assert_eq!(requests.len(), 3, "the failed object, its retry, and the next object");
    assert_ne!(requests[2].path, requests[0].path);
    assert_eq!(json_messages(&gunzip(&requests[2].body)), ["m2", "m3"]);
}

// ---------------------------------------------------------------------------------------------
// Formats, compression, and headers
// ---------------------------------------------------------------------------------------------

/// Every format and compression reads back as `replay_in` decodes it. `replay_in` doesn't
/// decompress, so the test decompresses with `flate2` and `ruzstd` first.
#[tokio::test]
async fn every_object_round_trips_through_replay_ins_decoders() {
    use ObjectCompression as C;
    use ObjectFormat as F;
    let cases = [
        (F::Json, C::Gzip, "ndjson.gz", "application/gzip"),
        (F::Json, C::Zstd, "ndjson.zst", "application/zstd"),
        (F::Json, C::None, "ndjson", "application/x-ndjson"),
        (F::Native, C::Gzip, "logit.gz", "application/gzip"),
        (F::Native, C::Zstd, "logit.zst", "application/zstd"),
        (F::Native, C::None, "logit", "application/octet-stream"),
    ];
    for (format, compression, ext, content_type) in cases {
        let (addr, log) = standin(|_, _| ok()).await;
        let mut settings = settings(addr);
        settings.format = format;
        settings.compression = compression;
        let mut out = sink_with(settings, &TelemetryProbe::new());
        let batches: Vec<EventBatch> = names(3).iter().map(|m| batch(m)).collect();
        put(&mut out, 1, &batches).await.unwrap();
        let request = &seen(&log)[0];
        assert!(request.path.ends_with(&format!(".{ext}")), "{}", request.path);
        assert_eq!(request.headers["content-type"], content_type);
        let raw = match compression {
            C::Gzip => gunzip(&request.body),
            C::Zstd => unzstd(&request.body),
            C::None => request.body.clone(),
        };
        let got = match format {
            F::Json => json_messages(&raw),
            F::Native => native_messages(&raw),
        };
        assert_eq!(got, names(3), "{format:?} {compression:?}");
    }
}

#[tokio::test]
async fn storage_class_sse_and_a_session_token_are_sent_and_signed() {
    let (addr, log) = standin(|_, _| ok()).await;
    let mut settings = settings(addr);
    settings.storage_class = Some("STANDARD_IA".to_string());
    settings.sse =
        Some(Sse::AwsKms { kms_key_id: Some("key-1".to_string()), bucket_key: Some(true) });
    settings.credentials.as_mut().unwrap().session_token = Some("session".to_string());
    put(&mut sink_with(settings, &TelemetryProbe::new()), 1, &[batch("m1")]).await.unwrap();
    let request = &seen(&log)[0];
    let headers = &request.headers;
    assert_eq!(headers["x-amz-storage-class"], "STANDARD_IA");
    assert_eq!(headers["x-amz-server-side-encryption"], "aws:kms");
    assert_eq!(headers["x-amz-server-side-encryption-aws-kms-key-id"], "key-1");
    assert_eq!(headers["x-amz-server-side-encryption-bucket-key-enabled"], "true");
    assert_eq!(headers["x-amz-security-token"], "session");
    assert!(headers["authorization"].contains(
        "SignedHeaders=content-type;host;x-amz-content-sha256;x-amz-date;x-amz-security-token;\
         x-amz-server-side-encryption;x-amz-server-side-encryption-aws-kms-key-id;\
         x-amz-server-side-encryption-bucket-key-enabled;x-amz-storage-class,"
    ));

    let (addr, log) = standin(|_, _| ok()).await;
    let mut settings = self::settings(addr);
    settings.sse = Some(Sse::Aes256);
    put(&mut sink_with(settings, &TelemetryProbe::new()), 1, &[batch("m1")]).await.unwrap();
    let headers = &seen(&log)[0].headers;
    assert_eq!(headers["x-amz-server-side-encryption"], "AES256");
    assert!(!headers.contains_key("x-amz-server-side-encryption-aws-kms-key-id"));
}

// ---------------------------------------------------------------------------------------------
// Multipart
// ---------------------------------------------------------------------------------------------

/// An object past `part_size` goes up as a multipart upload: `CreateMultipartUpload` once the
/// first part fills, an `UploadPart` per full part inside `submit`, the last part and
/// `CompleteMultipartUpload` from the head's `await_ack`. Every part but the last is `part_size`
/// bytes, and each is signed with its own body's hash.
#[tokio::test]
async fn parts_are_part_size_and_each_is_signed_by_its_own_hash() {
    let model = Arc::new(Model::default());
    let (addr, log) = model_standin(&model).await;
    let mut probe = TelemetryProbe::new();
    let mut settings = multipart(addr);
    settings.storage_class = Some("STANDARD_IA".to_string());
    let mut out = sink_with(settings, &probe);
    let batches: Vec<EventBatch> = names(5).iter().map(|m| batch(m)).collect();
    out.submit(&batches[0], BatchContext::default(), seq(1)).await.unwrap();
    assert_eq!(ops(&log)[..2], ["create", "part"], "the head's submit cut a part");
    for (n, batch) in (2..).zip(&batches[1..]) {
        out.submit(batch, BatchContext::default(), seq(n)).await.unwrap();
    }
    let before_close = seen(&log).len();
    assert_eq!(out.reached_destination(5), 0, "no part makes the object visible");
    out.await_ack().await.unwrap();

    let requests = seen(&log);
    let parts: Vec<&Seen> = requests.iter().filter(|r| op_of(r) == "part").collect();
    let total: u64 = (1..=5).map(|n| line_len(&format!("m{n}"))).sum();
    assert_eq!(parts.len() as u64, total.div_ceil(PART));
    for (n, part) in parts.iter().enumerate() {
        let size = part.body.len() as u64;
        if n + 1 < parts.len() {
            assert_eq!(size, PART, "part {} is part_size", n + 1);
        } else {
            assert!(0 < size && size <= PART, "the last part is {size} bytes");
        }
        assert_eq!(param(part, "partNumber").unwrap(), (n + 1).to_string());
        assert_eq!(param(part, "uploadId").unwrap(), "upload-0");
        let hash = object::hex(ring::digest::digest(&ring::digest::SHA256, &part.body).as_ref());
        assert_eq!(part.headers["x-amz-content-sha256"], hash);
        assert_eq!(part.headers["content-length"], part.body.len().to_string());
        assert!(!part.headers.contains_key("content-type"));
        assert!(!part.headers.contains_key("x-amz-storage-class"));
    }
    assert!(requests[..before_close].iter().filter(|r| op_of(r) == "part").count() >= 4);
    assert_eq!(ops(&log).iter().filter(|op| **op == "create").count(), 1);
    assert_eq!(*ops(&log).last().unwrap(), "complete");
    assert_eq!(out.reached_destination(5), 5);

    // The object's own headers go on `CreateMultipartUpload`, signed with its query.
    let create = &requests[0];
    assert_eq!(create.method, "POST");
    assert!(create.path.ends_with(".ndjson?uploads"), "{}", create.path);
    assert_eq!(create.headers["content-type"], "application/x-ndjson");
    assert_eq!(create.headers["x-amz-storage-class"], "STANDARD_IA");
    let signed: Vec<(String, String)> =
        ["content-type", "host", "x-amz-content-sha256", "x-amz-date", "x-amz-storage-class"]
            .iter()
            .map(|name| (name.to_string(), create.headers[*name].clone()))
            .collect();
    let path = key_path(create).replace("%3D", "=");
    let expected = sign::authorization(
        &out.credentials().await.unwrap(),
        "us-east-1",
        &AmzDate::from_unix_nanos(T0),
        "POST",
        &path,
        &[("uploads", "")],
        &signed,
        EMPTY_SHA256,
    );
    assert_eq!(create.headers["authorization"], expected);

    // `CompleteMultipartUpload` names each part's ETag in order.
    let complete = requests.last().unwrap();
    let body = String::from_utf8(complete.body.clone()).unwrap();
    for n in 1..=parts.len() {
        assert!(
            body.contains(&format!("<ETag>\"upload-0-{n}\"</ETag><PartNumber>{n}</PartNumber>"))
        );
    }
    let hash = object::hex(ring::digest::digest(&ring::digest::SHA256, &complete.body).as_ref());
    assert_eq!(complete.headers["x-amz-content-sha256"], hash);

    let stored = model.stored();
    assert_eq!(stored.into_values().collect::<Vec<_>>(), [names(5)]);
    let totals = probe.poll();
    assert_eq!(totals.sum("logit.output.parts", &[]), parts.len() as f64);
    for (op, count) in [("create_upload", 1.0), ("upload_part", parts.len() as f64)] {
        assert_eq!(
            totals.sum("logit.output.requests", &[("op", op), ("class", "2xx")]),
            count,
            "{op}"
        );
    }
    assert_eq!(
        totals.sum("logit.output.requests", &[("op", "complete_upload"), ("class", "2xx")]),
        1.0
    );
    assert_eq!(totals.sum("logit.output.objects", &[]), 1.0);
    assert_eq!(totals.sum("logit.output.bytes", &[]), total as f64);
}

/// A gzip object cut into parts reads back whole once its parts are joined, and a zstd one too.
#[tokio::test]
async fn a_compressed_object_cut_into_parts_reads_back_whole() {
    for compression in [ObjectCompression::Gzip, ObjectCompression::Zstd] {
        let model = Arc::new(Model::default());
        let (addr, log) = model_standin(&model).await;
        let mut settings = multipart(addr);
        settings.compression = compression;
        let mut out = sink_with(settings, &TelemetryProbe::new());
        let messages: Vec<String> = (0..200).map(|n| format!("message {n} {}", n * 7919)).collect();
        let batches: Vec<EventBatch> = messages.iter().map(|m| batch(m)).collect();
        put(&mut out, 1, &batches).await.unwrap();
        assert!(ops(&log).iter().filter(|op| **op == "part").count() > 2, "{compression:?}");
        let objects = model.objects.lock().unwrap();
        let body = objects.values().next().unwrap();
        let raw = match compression {
            ObjectCompression::Gzip => gunzip(body),
            _ => unzstd(body),
        };
        assert_eq!(json_messages(&raw), messages, "{compression:?}");
    }
}

/// What the close leaves decides the request: one part is one `PutObject`, and a close that cut
/// a second part, though staging cut none, is a multipart upload, so no request body is over
/// `part_size`. Under `zstd`, two small batches stay in the uncompressed chunk until the close.
#[tokio::test]
async fn no_request_body_is_over_part_size() {
    for (part_size, want) in [(64, "create"), (4096, "put")] {
        let model = Arc::new(Model::default());
        let (addr, log) = model_standin(&model).await;
        let mut settings = multipart(addr);
        settings.compression = ObjectCompression::Zstd;
        settings.part_size = part_size;
        let mut out = sink_with(settings, &TelemetryProbe::new());
        out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
        out.submit(&batch("m2"), BatchContext::default(), seq(2)).await.unwrap();
        assert!(seen(&log).is_empty(), "staging cut no part");
        out.await_ack().await.unwrap();
        assert_eq!(ops(&log)[0], want, "{part_size}: {:?}", ops(&log));
        for request in seen(&log).iter().filter(|r| matches!(op_of(r), "put" | "part")) {
            assert!(request.body.len() as u64 <= part_size, "{part_size}: {}", request.path);
        }
        let objects = model.objects.lock().unwrap();
        assert_eq!(json_messages(&unzstd(objects.values().next().unwrap())), names(2));
    }
}

/// A `200` `CompleteMultipartUpload` whose body is an `<Error>` isn't delivered: it takes its
/// code's class, the upload is aborted, and no object is counted.
#[tokio::test]
async fn a_200_complete_with_an_error_body_is_not_delivered() {
    for (code, want) in [
        ("InternalError", Fault::Ambiguous),
        ("SlowDown", Fault::Clean),
        ("NoSuchUpload", Fault::Clean),
        ("AccessDenied", Fault::Refused),
        ("SomethingNew", Fault::Ambiguous),
    ] {
        let model = Arc::new(Model::default());
        let answers = Arc::clone(&model);
        let (addr, log) = standin(move |_, seen| match op_of(seen) {
            "complete" => reply(200, s3_error(code)),
            _ => answers.answer(seen),
        })
        .await;
        let mut probe = TelemetryProbe::new();
        let mut out = sink_with(multipart(addr), &probe);
        let err = put(&mut out, 1, &[batch("m1"), batch("m2")]).await.unwrap_err();
        assert_eq!(classify(&err), want, "{code}: {err:#}");
        assert!(!logit_pipeline::is_head_only(&err));
        assert!(
            format!("{err:#}").contains(&format!("(200 OK) {code}: the store says")),
            "{err:#}"
        );
        assert_eq!(*ops(&log).last().unwrap(), "abort", "{code}");
        assert!(model.stored().is_empty());
        let totals = probe.poll();
        assert_eq!(totals.sum("logit.output.objects", &[]), 0.0, "{code}");
        assert_eq!(
            totals.sum("logit.output.uploads.abandoned", &[("aborted", "true")]),
            1.0,
            "{code}"
        );
    }
}

/// `NoSuchUpload` starts the object again in a new upload, and the old upload is aborted.
#[tokio::test]
async fn no_such_upload_starts_the_object_again_and_aborts_the_old_upload() {
    let model = Arc::new(Model::default());
    let answers = Arc::clone(&model);
    let failed = Arc::new(AtomicUsize::new(0));
    let fail = Arc::clone(&failed);
    let (addr, log) = standin(move |_, seen| {
        // The second part of the first upload finds the upload gone.
        if op_of(seen) == "part"
            && param(seen, "partNumber").as_deref() == Some("2")
            && fail.fetch_add(1, Ordering::SeqCst) == 0
        {
            return reply(404, s3_error("NoSuchUpload"));
        }
        answers.answer(seen)
    })
    .await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink_with(multipart(addr), &probe);
    let err = put(&mut out, 1, &[batch("m1")]).await.unwrap_err();
    assert_eq!(classify(&err), Fault::Clean, "{err:#}");
    assert!(format!("{err:#}").contains("NoSuchUpload"), "{err:#}");
    let aborts: Vec<Seen> = seen(&log).into_iter().filter(|r| op_of(r) == "abort").collect();
    assert_eq!(aborts.len(), 1);
    assert_eq!(param(&aborts[0], "uploadId").as_deref(), Some("upload-0"));

    put(&mut out, 1, &[batch("m1")]).await.unwrap();
    let completes: Vec<Seen> = seen(&log).into_iter().filter(|r| op_of(r) == "complete").collect();
    assert_eq!(completes.len(), 1);
    assert_eq!(param(&completes[0], "uploadId").as_deref(), Some("upload-1"), "a new upload");
    assert_eq!(model.stored().into_values().collect::<Vec<_>>(), [names(1)]);
    let totals = probe.poll();
    assert_eq!(
        totals.sum("logit.output.requests", &[("op", "upload_part"), ("class", "4xx")]),
        1.0
    );
    assert_eq!(
        totals.sum("logit.output.requests", &[("op", "abort_upload"), ("class", "2xx")]),
        1.0
    );
}

/// `InvalidPart`, `InvalidPartOrder`, and `EntityTooSmall` mean the sink built a bad upload:
/// `Clean`, the upload aborted, and the object starts again.
#[tokio::test]
async fn a_bad_upload_is_clean_and_starts_the_object_again() {
    for code in ["InvalidPart", "InvalidPartOrder", "EntityTooSmall"] {
        let model = Arc::new(Model::default());
        let answers = Arc::clone(&model);
        let (addr, log) = standin(move |_, seen| {
            if op_of(seen) == "complete" && param(seen, "uploadId").as_deref() == Some("upload-0") {
                return reply(400, s3_error(code));
            }
            answers.answer(seen)
        })
        .await;
        let mut out = sink_with(multipart(addr), &TelemetryProbe::new());
        let err = put(&mut out, 1, &[batch("m1")]).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean, "{code}: {err:#}");
        assert_eq!(*ops(&log).last().unwrap(), "abort", "{code}");
        put(&mut out, 1, &[batch("m1")]).await.unwrap();
        assert_eq!(model.stored().into_values().collect::<Vec<_>>(), [names(1)], "{code}");
    }
}

/// An answer the table reads `Ambiguous` to `CreateMultipartUpload` or `UploadPart`, a timeout
/// included, is `Clean`: nothing is visible before completion. `CompleteMultipartUpload`'s stays
/// `Ambiguous`, and any other class passes through.
#[tokio::test]
async fn an_ambiguous_part_is_clean_and_an_ambiguous_complete_stays_ambiguous() {
    type Case = (&'static str, fn() -> Answer, Fault);
    let cases: [Case; 9] = [
        ("create", || reply(500, s3_error("InternalError")), Fault::Clean),
        ("create", || reply(503, "<html>busy</html>"), Fault::Clean),
        ("part", || reply(500, s3_error("InternalError")), Fault::Clean),
        ("part", || reply(408, ""), Fault::Clean),
        ("part", || Answer::Hang, Fault::Clean),
        ("part", || reply(403, s3_error("AccessDenied")), Fault::Refused),
        ("complete", || reply(500, s3_error("InternalError")), Fault::Ambiguous),
        ("complete", || reply(502, ""), Fault::Ambiguous),
        ("complete", || Answer::Hang, Fault::Ambiguous),
    ];
    for (op, answer, want) in cases {
        let model = Arc::new(Model::default());
        let answers = Arc::clone(&model);
        let (addr, _log) =
            standin(move |_, seen| if op_of(seen) == op { answer() } else { answers.answer(seen) })
                .await;
        let mut settings = multipart(addr);
        settings.timeout = Duration::from_millis(200);
        let mut out = sink_with(settings, &TelemetryProbe::new());
        let err = put(&mut out, 1, &[batch("m1")]).await.unwrap_err();
        assert_eq!(classify(&err), want, "{op}: {err:#}");
        assert_eq!(
            out.reached_destination(1),
            usize::from(op == "complete"),
            "{op}: only a Complete makes the object visible"
        );
    }
}

/// A part that fails inside `submit` past the head poisons the object: the `submit` fails, the
/// upload is aborted once and never completed, and the head's `await_ack` fails with the part's
/// class and without `HeadOnly`, so the loop starts the object again from the head.
#[tokio::test]
async fn a_part_failure_past_the_head_fails_the_heads_answer() {
    let model = Arc::new(Model::default());
    let answers = Arc::clone(&model);
    // The head's own parts go up; the next batch's first part is refused.
    let head_parts = (line_len("m1") / PART) as usize;
    let refused = (head_parts + 1).to_string();
    let (addr, log) = standin(move |_, seen| {
        if op_of(seen) == "part" && param(seen, "partNumber") == Some(refused.clone()) {
            return reply(403, s3_error("AccessDenied"));
        }
        answers.answer(seen)
    })
    .await;
    let mut want = vec!["create"];
    want.extend(std::iter::repeat_n("part", head_parts + 1));
    want.push("abort");
    let mut probe = TelemetryProbe::new();
    let mut out = sink_with(multipart(addr), &probe);
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    out.submit(&batch("m2"), BatchContext::default(), seq(2)).await.unwrap_err();
    assert_eq!(ops(&log), want);
    assert_eq!(out.window(), usize::MAX);
    assert_eq!(out.collecting(), None);
    assert_eq!(out.reached_destination(1), 0);
    let err = out.await_ack().await.unwrap_err();
    assert_eq!(classify(&err), Fault::Refused, "{err:#}");
    assert!(!logit_pipeline::is_head_only(&err));
    assert!(format!("{err:#}").contains("AccessDenied"), "{err:#}");
    assert_eq!(ops(&log), want, "never completed");
    let totals = probe.poll();
    assert_eq!(totals.sum("logit.output.uploads.abandoned", &[("aborted", "true")]), 1.0);
}

/// A part that fails inside the head's own `submit` is the head's fault, which the loop
/// classifies: the upload is aborted, and the head's next `submit` starts a new one.
#[tokio::test]
async fn a_part_failure_at_the_head_is_the_heads_own() {
    let model = Arc::new(Model::default());
    let answers = Arc::clone(&model);
    let failed = Arc::new(AtomicUsize::new(0));
    let fail = Arc::clone(&failed);
    let (addr, log) = standin(move |_, seen| {
        if op_of(seen) == "part" && fail.fetch_add(1, Ordering::SeqCst) == 0 {
            return reply(503, s3_error("SlowDown"));
        }
        answers.answer(seen)
    })
    .await;
    let mut out = sink_with(multipart(addr), &TelemetryProbe::new());
    let err = out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap_err();
    assert_eq!(classify(&err), Fault::Clean, "{err:#}");
    assert_eq!(ops(&log), ["create", "part", "abort"]);
    put(&mut out, 1, &[batch("m1")]).await.unwrap();
    assert_eq!(model.stored().into_values().collect::<Vec<_>>(), [names(1)]);
    let completes: Vec<Seen> = seen(&log).into_iter().filter(|r| op_of(r) == "complete").collect();
    assert_eq!(param(&completes[0], "uploadId").as_deref(), Some("upload-1"));

    // A `Rejected` part at the head drops the head, and the next batch opens its own object.
    let model = Arc::new(Model::default());
    let answers = Arc::clone(&model);
    let (addr, _log) = standin(move |_, seen| {
        if op_of(seen) == "part" && param(seen, "uploadId").as_deref() == Some("upload-0") {
            return reply(400, s3_error("SomethingNew"));
        }
        answers.answer(seen)
    })
    .await;
    let mut out = sink_with(multipart(addr), &TelemetryProbe::new());
    let err = out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap_err();
    assert_eq!(classify(&err), Fault::Rejected, "{err:#}");
    put(&mut out, 2, &[batch("m2")]).await.unwrap();
    assert_eq!(model.stored().into_values().collect::<Vec<_>>(), [["m2"]]);
}

/// Through the write loop: a part that fails past the head is never completed, its upload is
/// aborted once, and every batch is stored, once, by the object the loop starts again from the
/// head.
#[tokio::test]
async fn a_poisoned_object_starts_again_from_the_head_in_a_new_upload() {
    let model = Arc::new(Model::default());
    let answers = Arc::clone(&model);
    let parts = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&parts);
    // The first part request after the head's own parts is the second batch's: past the head.
    let head_parts = (line_len("m1") / PART) as usize;
    let (addr, log) = standin(move |_, seen| {
        if op_of(seen) == "part" && counted.fetch_add(1, Ordering::SeqCst) == head_parts {
            return reply(500, s3_error("InternalError"));
        }
        answers.answer(seen)
    })
    .await;
    let mut settings = multipart(addr);
    settings.max_age = Duration::from_millis(100);
    let mut probe = TelemetryProbe::new();
    let telemetry = probe.telemetry("out", "s3_out", "sink");
    let mut out = sink_with(settings, &probe);
    let store =
        Arc::new(SinkStore::Memory(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone())));
    for message in names(4) {
        store.push((Arc::new(batch(&message)), BatchContext::default())).await;
    }
    let producer = async {
        wait_until("the object stored", || !model.stored().is_empty()).await;
        store.close();
    };
    let config = crate::test_support::fast_retry();
    logit_pipeline::test_util::drive_write_loop_over(
        &mut out,
        Arc::clone(&store),
        config,
        telemetry,
        producer,
    )
    .await;
    let requests = seen(&log);
    let completes: Vec<&Seen> = requests.iter().filter(|r| op_of(r) == "complete").collect();
    assert_eq!(completes.len(), 1);
    assert_eq!(param(completes[0], "uploadId").as_deref(), Some("upload-1"));
    let aborts: Vec<&Seen> = requests.iter().filter(|r| op_of(r) == "abort").collect();
    assert_eq!(aborts.len(), 1, "aborted once");
    assert_eq!(param(aborts[0], "uploadId").as_deref(), Some("upload-0"));
    assert_eq!(model.stored().into_values().collect::<Vec<_>>(), [names(4)]);
    let totals = probe.poll();
    assert_eq!(totals.sum("logit.component.batches.delivered", &[]), 4.0);
    assert_eq!(totals.sum("logit.component.batches.dropped", &[]), 0.0);
    assert_eq!(totals.sum("logit.output.uploads.abandoned", &[("aborted", "true")]), 1.0);
}

/// An abort that fails is counted `aborted="false"` and not retried; one answered `NoSuchUpload`
/// left no parts, and counts `aborted="true"`.
#[tokio::test]
async fn a_failed_abort_is_counted_and_not_retried() {
    let model = Arc::new(Model::default());
    let answers = Arc::clone(&model);
    let (addr, log) = standin(move |_, seen| match op_of(seen) {
        "complete" => reply(400, s3_error("InvalidPart")),
        "abort" => reply(500, s3_error("InternalError")),
        _ => answers.answer(seen),
    })
    .await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink_with(multipart(addr), &probe);
    put(&mut out, 1, &[batch("m1")]).await.unwrap_err();
    assert_eq!(ops(&log).iter().filter(|op| **op == "abort").count(), 1);
    let totals = probe.poll();
    assert_eq!(totals.sum("logit.output.uploads.abandoned", &[("aborted", "false")]), 1.0);
    assert_eq!(
        totals.sum("logit.output.requests", &[("op", "abort_upload"), ("class", "5xx")]),
        1.0
    );
}

/// A `200` that lacks what the sink needs from it stored nothing visible: a
/// `CreateMultipartUpload` with no `<UploadId>`, and an `UploadPart` with no `ETag`.
#[tokio::test]
async fn a_200_create_with_no_upload_id_or_part_with_no_etag_is_clean() {
    let cases: [Named; 2] = [
        ("create", || reply(200, Vec::new())),
        ("part", || Answer::Reply { status: 200, headers: Vec::new(), body: Vec::new() }),
    ];
    for (op, answer) in cases {
        let model = Arc::new(Model::default());
        let answers = Arc::clone(&model);
        let (addr, log) =
            standin(move |_, seen| if op_of(seen) == op { answer() } else { answers.answer(seen) })
                .await;
        let mut out = sink_with(multipart(addr), &TelemetryProbe::new());
        let err = put(&mut out, 1, &[batch("m1")]).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Clean, "{op}: {err:#}");
        assert!(!ops(&log).contains(&"complete"), "{op}");
    }
}

/// A `CompleteMultipartUpload` `200` whose answer can't be read whole may have assembled the
/// object: one over 64 KiB, and one whose connection drops mid-body, are `Ambiguous`.
#[tokio::test]
async fn a_complete_answer_over_64_kib_or_cut_short_is_ambiguous() {
    let cases: [Named; 2] = [
        ("over 64 KiB", || reply(200, vec![b' '; 64 * 1024 + 1])),
        ("cut short", || Answer::Cut {
            status: 200,
            body: b"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n<CompleteMultipart".to_vec(),
        }),
    ];
    for (case, answer) in cases {
        let model = Arc::new(Model::default());
        let answers = Arc::clone(&model);
        let (addr, _log) =
            standin(
                move |_, seen| {
                    if op_of(seen) == "complete" {
                        answer()
                    } else {
                        answers.answer(seen)
                    }
                },
            )
            .await;
        let mut out = sink_with(multipart(addr), &TelemetryProbe::new());
        let err = put(&mut out, 1, &[batch("m1")]).await.unwrap_err();
        assert_eq!(classify(&err), Fault::Ambiguous, "{case}: {err:#}");
        if case == "cut short" {
            // The head arrived and the body didn't: the read, not the send, failed.
            assert!(format!("{err:#}").contains("error decoding response body"), "{err:#}");
        }
    }
}

/// An abort answered `NoSuchUpload` left no parts behind, so it counts as aborted.
#[tokio::test]
async fn an_abort_answered_no_such_upload_counts_as_aborted() {
    let model = Arc::new(Model::default());
    let answers = Arc::clone(&model);
    let (addr, log) = standin(move |_, seen| match op_of(seen) {
        "complete" => reply(400, s3_error("InvalidPart")),
        "abort" => reply(404, s3_error("NoSuchUpload")),
        _ => answers.answer(seen),
    })
    .await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink_with(multipart(addr), &probe);
    put(&mut out, 1, &[batch("m1")]).await.unwrap_err();
    assert_eq!(ops(&log).iter().filter(|op| **op == "abort").count(), 1);
    let totals = probe.poll();
    assert_eq!(totals.sum("logit.output.uploads.abandoned", &[("aborted", "true")]), 1.0);
    assert_eq!(totals.sum("logit.output.uploads.abandoned", &[("aborted", "false")]), 0.0);
}

/// Under temporary credentials every multipart request carries and signs the session token, not
/// only the one that creates the object.
#[tokio::test]
async fn every_multipart_request_carries_the_session_token() {
    let model = Arc::new(Model::default());
    let answers = Arc::clone(&model);
    let (addr, log) = standin(move |_, seen| match op_of(seen) {
        "complete" => reply(400, s3_error("InvalidPart")),
        _ => answers.answer(seen),
    })
    .await;
    let mut settings = multipart(addr);
    settings.credentials.as_mut().unwrap().session_token = Some("session".to_string());
    put(&mut sink_with(settings, &TelemetryProbe::new()), 1, &[batch("m1")]).await.unwrap_err();
    let requests = seen(&log);
    let mut sent: Vec<_> = requests.iter().map(op_of).collect();
    sent.dedup();
    assert_eq!(sent, ["create", "part", "complete", "abort"]);
    for request in &requests {
        let op = op_of(request);
        assert_eq!(
            request.headers.get("x-amz-security-token").map(String::as_str),
            Some("session"),
            "{op}"
        );
        assert!(request.headers["authorization"].contains(";x-amz-security-token"), "{op}");
    }
}

/// A completed upload is done with: the next object starts its own upload, and nothing aborts
/// the one that completed.
#[tokio::test]
async fn a_completed_upload_is_never_aborted() {
    let model = Arc::new(Model::default());
    let (addr, log) = model_standin(&model).await;
    let mut probe = TelemetryProbe::new();
    let mut out = sink_with(multipart(addr), &probe);
    put(&mut out, 1, &[batch("m1")]).await.unwrap();
    put(&mut out, 2, &[batch("m2")]).await.unwrap();
    let sent = ops(&log);
    assert_eq!(sent.iter().filter(|op| **op == "create").count(), 2, "{sent:?}");
    assert!(!sent.contains(&"abort"), "{sent:?}");
    let stored: Vec<_> = model.stored().into_values().collect();
    assert_eq!(stored, [vec!["m1".to_string()], vec!["m2".to_string()]]);
    let totals = probe.poll();
    assert_eq!(totals.sum("logit.output.uploads.abandoned", &[("aborted", "true")]), 0.0);
    assert_eq!(totals.sum("logit.output.uploads.abandoned", &[("aborted", "false")]), 0.0);
}

/// The late-Complete case of decision 3: an earlier attempt's `CompleteMultipartUpload`, answered
/// `Ambiguous`, lands after its retry committed. The retry closed at the head's pinned count
/// though more batches had arrived, so the late attempt holds every batch the retry committed,
/// and the batches after them are in the next object.
#[tokio::test]
async fn a_late_complete_leaves_every_committed_batch_at_the_key() {
    let model = Arc::new(Model::default());
    let answers = Arc::clone(&model);
    let release = Arc::new(Notify::new());
    let landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (held, landing) = (Arc::clone(&release), Arc::clone(&landed));
    let (addr, log) = standin(move |_, seen| {
        if op_of(seen) == "complete" && param(seen, "uploadId").as_deref() == Some("upload-0") {
            // The store applies this Complete only once the test releases it, after the retry.
            let body = answers.assembled("upload-0").unwrap();
            let (model, held, key) = (Arc::clone(&answers), Arc::clone(&held), key_path(seen));
            let landing = Arc::clone(&landing);
            tokio::spawn(async move {
                held.notified().await;
                model.objects.lock().unwrap().insert(key, body);
                landing.store(true, Ordering::SeqCst);
            });
            return reply(500, s3_error("InternalError"));
        }
        answers.answer(seen)
    })
    .await;
    let mut settings = multipart(addr);
    settings.max_age = Duration::from_millis(100);
    let probe = TelemetryProbe::new();
    let telemetry = probe.telemetry("out", "s3_out", "sink");
    let mut out = sink_with(settings, &probe);
    let store =
        Arc::new(SinkStore::Memory(SinkQueue::new(SinkQueueConfig::default(), telemetry.clone())));
    let push = |message: String| {
        let store = Arc::clone(&store);
        async move { store.push((Arc::new(batch(&message)), BatchContext::default())).await }
    };
    for message in names(3) {
        push(message).await;
    }
    let producer = async {
        wait_until("the first Complete", || ops(&log).contains(&"complete")).await;
        push("m4".to_string()).await;
        push("m5".to_string()).await;
        wait_until("every batch committed", || store.queued() == 0).await;
        release.notify_one();
        wait_until("the late Complete landed", || landed.load(Ordering::SeqCst)).await;
        store.close();
    };
    let config = crate::test_support::fast_retry();
    logit_pipeline::test_util::drive_write_loop_over(
        &mut out,
        Arc::clone(&store),
        config,
        telemetry,
        producer,
    )
    .await;
    let stored: Vec<String> = model.stored().into_values().flatten().collect();
    let mut sorted = stored.clone();
    sorted.sort();
    assert_eq!(sorted, names(5), "every committed batch is stored once: {stored:?}");
}
