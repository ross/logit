//! `s3_out` against an in-process S3 stand-in: one test per row of the module doc's response
//! table that a `PutObject` can receive, the key and the head's pin across retries, and objects
//! read back as `replay_in` reads them.

use super::*;
use logit_core::{AttrMap, BodyFormat, Event, LogRecord, Resource, Value};
use logit_pipeline::test_util::{wait_until, TelemetryProbe};
use logit_pipeline::{classify, SinkQueue, SinkQueueConfig, SinkStore};
use std::cell::Cell;
use std::collections::HashMap;
use std::io::Read;
use std::net::SocketAddr;
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
}

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
                        let (status, headers, body) = loop {
                            match answer {
                                Answer::Reply { status, headers, body } => {
                                    break (status, headers, body)
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
                        Ok::<_, std::convert::Infallible>(
                            response
                                .body(http_body_util::Full::new(bytes::Bytes::from(body)))
                                .unwrap(),
                        )
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

fn names(n: usize) -> Vec<String> {
    (1..=n).map(|i| format!("m{i}")).collect()
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
            "AccessControlListNotSupported",
            "KeyTooLongError",
            "InvalidRequest",
        ],
        Fault::Refused,
    )
    .await;
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
    for status in [400, 405, 422] {
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

/// The rows only the multipart path can receive, through the classifier their tests in W3b reach.
#[test]
fn the_multipart_rows_classify_as_the_table_says() {
    for code in ["InvalidPart", "InvalidPartOrder", "EntityTooSmall"] {
        assert_eq!(classify_answer(400, code), Fault::Clean, "{code}");
    }
    assert_eq!(classify_answer(404, "NoSuchUpload"), Fault::Clean);
}

fn classify_answer(status: u16, code: &str) -> Fault {
    super::classify(status, Some(code))
}

// ---------------------------------------------------------------------------------------------
// The window, the key, and the pin
// ---------------------------------------------------------------------------------------------

/// The object closes once its encoded bytes reach `max_bytes`, and, W3a's limit, once its
/// compressed body reaches `part_size`.
#[tokio::test]
async fn an_object_is_full_at_max_bytes_or_at_part_size() {
    let (addr, _log) = standin(|_, _| ok()).await;
    let mut settings = settings(addr);
    let one = {
        let mut out = String::new();
        crate::ndjson::render_ndjson(&mut out, &batch("m1"));
        out.len() as u64
    };
    settings.max_bytes = 2 * one;
    let mut out = sink_with(settings, &TelemetryProbe::new());
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    assert!(out.collecting().is_some());
    out.submit(&batch("m2"), BatchContext::default(), seq(2)).await.unwrap();
    assert_eq!(out.collecting(), None, "max_bytes reached");

    let mut settings = self::settings(addr);
    settings.compression = ObjectCompression::None;
    settings.part_size = 2 * one;
    let mut out = sink_with(settings, &TelemetryProbe::new());
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    assert!(out.collecting().is_some());
    out.submit(&batch("m2"), BatchContext::default(), seq(2)).await.unwrap();
    assert_eq!(out.collecting(), None, "part_size reached");
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
    let (addr, _log) = standin(|_, _| Answer::Hang).await;
    let mut out = sink(addr);
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    assert_eq!(out.reached_destination(1), 0);
    let upload = tokio::time::timeout(Duration::from_millis(200), out.await_ack()).await;
    assert!(upload.is_err(), "the stand-in never answers");
    assert_eq!(out.reached_destination(1), 1);
    // A submit after the cancelled close opens a new object under the same key.
    out.submit(&batch("m1"), BatchContext::default(), seq(1)).await.unwrap();
    assert_eq!(out.reached_destination(1), 0);
}

// ---------------------------------------------------------------------------------------------
// Formats, compression, and headers
// ---------------------------------------------------------------------------------------------

/// Every format and compression reads back as `replay_in` decodes it. `replay_in` doesn't yet
/// decompress (W4 of the plan), so the test decompresses with `flate2` and `ruzstd` first.
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
