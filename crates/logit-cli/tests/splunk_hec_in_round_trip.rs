//! `splunk_hec_in` over a real socket: every data route, posted the way a HEC client posts it
//! (identity and gzip, `Authorization: Splunk <token>`), delivers the batches its body encodes and
//! nothing else.
//!
//! Each `/event` body is built by `logit_proto::splunk::SplunkEncoder` from batches that are
//! themselves one decode of a hand-written HEC body, so they are on the codec's fixed point
//! (`crates/logit-proto/tests/splunk_fixed_point.rs`): decoding the encoder's output gives the
//! encoder's input back, whole-`EventBatch` equal. What this file adds is the HTTP hop: routing,
//! authentication, `Content-Encoding`, the channel and `ackId`, and delivery to the `Fanout`. It
//! also covers the busy contract (`crates/logit-inputs/src/splunk.rs`'s "Backpressure" section).

use logit_core::EventBatch;
use logit_inputs::splunk::SplunkHecInput;
use logit_pipeline::{Fanout, Input};
use logit_proto::splunk::{Envelope, SplunkDecoder, SplunkEncoder};
use logit_proto::Encoder;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Every hand-written object carries a `time`, so nothing decodes to this.
const RECEIVED_AT: i64 = 1_699_000_000_000_000_000;

/// The busy bound the busy test builds its listener with, via
/// [`SplunkHecInput::with_busy_after`]'s test/tuning hook, so it doesn't wait out the real 5s
/// default.
const TEST_BUSY_AFTER: Duration = Duration::from_millis(200);

const TOKEN: &str = "11111111-2222-3333-4444-555555555555";

/// A body in the OpenTelemetry Collector `splunk_hec` exporter's shape: logs under one envelope,
/// metrics under another, and a span whose `fields` are its resource, so it decodes to three
/// batches.
const OTEL_EXPORTER_BODY: &str = concat!(
    r#"{"time":1700000000.123,"host":"web-1","source":"app","sourcetype":"otel","index":"main","event":"user logged in","fields":{"service.name":"auth","otel.log.severity.text":"INFO","otel.log.severity.number":9,"otel.log.name":"login","trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","user.id":42}}"#,
    r#"{"time":1700000000.2,"host":"web-1","source":"app","sourcetype":"otel","index":"main","event":{"msg":"structured","attempt":2},"fields":{"service.name":"auth"}}"#,
    r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Gauge","metric_name:process.memory.usage":123456789}}"#,
    r#"{"time":1700000001,"host":"web-1","event":"metric","fields":{"service.name":"auth","metric_type":"Sum","metric_name:http.server.requests":1500}}"#,
    r#"{"time":1700000002.5,"host":"web-1","source":"app","sourcetype":"otel","index":"traces","event":{"trace_id":"0af7651916cd43dd8448eb211c80319c","span_id":"b7ad6b7169203331","parent_span_id":"00f067aa0ba902b7","name":"POST /login","attributes":{"http.method":"POST"},"end_time":1700000002750000000,"kind":"SPAN_KIND_SERVER","status":{"message":"","code":"STATUS_CODE_UNSET"},"start_time":1700000002500000000},"fields":{"service.name":"auth"}}"#,
);

// -------------------------------------------------------------------------------------------------
// Listener and client
// -------------------------------------------------------------------------------------------------

async fn start(capacity: usize) -> (SocketAddr, mpsc::Receiver<logit_pipeline::Delivered>) {
    start_with_busy_after(capacity, Duration::from_secs(5)).await
}

async fn start_with_busy_after(
    capacity: usize,
    busy_after: Duration,
) -> (SocketAddr, mpsc::Receiver<logit_pipeline::Delivered>) {
    let mut input = SplunkHecInput::new("127.0.0.1:0")
        .with_tokens(vec![TOKEN.to_string()])
        .with_busy_after(busy_after);
    input.bind().await.expect("binding splunk_hec_in");
    let addr = input.local_addr().expect("bind() leaves an address");
    let (tx, rx) = mpsc::channel(capacity);
    let sink = Fanout::new(vec![tx]);
    tokio::spawn(async move {
        let _ = input.run(sink).await;
    });
    (addr, rx)
}

fn client() -> reqwest::Client {
    reqwest::Client::builder().no_gzip().build().expect("a default client always builds")
}

fn gzip(body: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    e.write_all(body).unwrap();
    e.finish().unwrap()
}

/// Posts `body` to `path` with the token, gzipped when `gzipped`, naming `channel` when given.
/// Returns the status and the response body.
async fn post(
    addr: SocketAddr,
    path: &str,
    body: &[u8],
    gzipped: bool,
    channel: Option<&str>,
) -> (reqwest::StatusCode, String) {
    let mut request = client()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", format!("Splunk {TOKEN}"));
    request = if gzipped {
        request.header("Content-Encoding", "gzip").body(gzip(body))
    } else {
        request.body(body.to_vec())
    };
    if let Some(channel) = channel {
        request = request.header("X-Splunk-Request-Channel", channel);
    }
    let response = request.send().await.expect("the request reaches splunk_hec_in");
    let status = response.status();
    (status, response.text().await.unwrap_or_default())
}

async fn recv(rx: &mut mpsc::Receiver<logit_pipeline::Delivered>) -> EventBatch {
    let delivered = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("splunk_hec_in delivers a batch within 5s")
        .expect("the Fanout channel is open");
    logit_pipeline::unwrap_batch(delivered)
}

/// One decode of `body`, and the encoder's `/event` body for it: the batches the listener must
/// deliver, and what to post.
fn fixed_point_case(body: &[u8]) -> (Vec<EventBatch>, Vec<u8>) {
    let expected = SplunkDecoder::new().decode_events(body, RECEIVED_AT).expect("the seed decodes");
    let mut encoder = SplunkEncoder::new();
    let mut encoded = Vec::new();
    for batch in &expected {
        encoded.extend_from_slice(&encoder.encode(batch).expect("encode never fails"));
    }
    (expected, encoded)
}

// -------------------------------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------------------------------

/// Every `/event` alias, identity and gzip, delivers the batches the encoder was given, equal,
/// one per resource, in order.
#[tokio::test]
async fn every_event_alias_delivers_the_encoded_batches_identity_and_gzip() {
    let (addr, mut rx) = start(16).await;
    let (expected, body) = fixed_point_case(OTEL_EXPORTER_BODY.as_bytes());
    assert_eq!(expected.len(), 3, "logs, metrics, and the span's resource");
    for path in
        ["/services/collector", "/services/collector/event", "/services/collector/event/1.0"]
    {
        for gzipped in [false, true] {
            let (status, text) = post(addr, path, &body, gzipped, None).await;
            assert_eq!(status, reqwest::StatusCode::OK, "{path} gzip={gzipped}: {text}");
            assert_eq!(text, r#"{"text":"Success","code":0}"#);
            for batch in &expected {
                assert_eq!(&recv(&mut rx).await, batch, "{path} gzip={gzipped}");
            }
        }
    }
    assert!(rx.try_recv().is_err(), "one batch per resource, nothing more");
}

/// `/raw` delivers one log per line under the query string's envelope, what
/// `SplunkDecoder::decode_raw` gives for the same body and envelope, modulo receipt time.
#[tokio::test]
async fn raw_delivers_one_log_per_line_under_the_query_envelope() {
    let (addr, mut rx) = start(16).await;
    let body = b"Sep 25 12:00:00 web-1 app: started\r\nSep 25 12:00:01 web-1 app: ready\n";
    let envelope = Envelope {
        host: Some("web-1".into()),
        source: Some("udp:514".into()),
        sourcetype: Some("syslog".into()),
        index: Some("main".into()),
    };
    let mut expected = SplunkDecoder::new().decode_raw(body, &envelope, RECEIVED_AT);
    let path = "/services/collector/raw?host=web-1&source=udp%3A514&sourcetype=syslog&index=main";
    let (status, text) = post(addr, path, body, true, None).await;
    assert_eq!(status, reqwest::StatusCode::OK, "{text}");
    let mut delivered = recv(&mut rx).await;
    assert_eq!(delivered.events.len(), 2);
    for event in delivered.events.iter_mut().chain(expected.events.iter_mut()) {
        event.timestamp = 0; // receipt time on both sides
    }
    assert_eq!(delivered, expected);
}

/// A client with a channel gets an `ackId` per request, and `/ack` reports each one delivered.
#[tokio::test]
async fn a_channel_draws_ack_ids_that_ack_reports_true() {
    let (addr, mut rx) = start(16).await;
    let (_, body) = fixed_point_case(br#"{"time":1,"event":"x"}"#);
    let channel = Some("0f3c2a1e-7d4b-4c55-9a1d-3b0e8d6f2c10");
    let (_, first) = post(addr, "/services/collector/event", &body, false, channel).await;
    let (_, second) = post(addr, "/services/collector/event", &body, true, channel).await;
    assert_eq!(first, r#"{"text":"Success","code":0,"ackId":1}"#);
    assert_eq!(second, r#"{"text":"Success","code":0,"ackId":2}"#);
    recv(&mut rx).await;
    recv(&mut rx).await;
    let (status, acks) =
        post(addr, "/services/collector/ack", br#"{"acks":[1,2]}"#, false, channel).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    assert_eq!(acks, r#"{"acks":{"1":true,"2":true}}"#);
}

/// The OpenTelemetry exporter's startup probe: `GET /services/collector/health`, no token.
#[tokio::test]
async fn health_answers_without_a_token() {
    let (addr, _rx) = start(16).await;
    let response = client()
        .get(format!("http://{addr}/services/collector/health"))
        .send()
        .await
        .expect("the request reaches splunk_hec_in");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    assert_eq!(response.text().await.unwrap(), r#"{"text":"HEC is healthy","code":17}"#);
}

/// The busy contract: a one-slot channel with a parked consumer takes the first request's batch;
/// the second request's send waits out `TEST_BUSY_AFTER` and is answered `503` code 9 with
/// `Retry-After`, delivering nothing. Once the consumer drains, the client's retry succeeds.
#[tokio::test]
async fn a_stalled_downstream_is_answered_503_code_9_and_the_retry_succeeds() {
    let (addr, mut rx) = start_with_busy_after(1, TEST_BUSY_AFTER).await;
    let (expected, body) = fixed_point_case(br#"{"time":1,"host":"h","event":"x"}"#);
    let path = "/services/collector/event";

    let (status, _) = post(addr, path, &body, false, None).await;
    assert_eq!(status, reqwest::StatusCode::OK, "the first batch fills the one slot");

    let started = Instant::now();
    let response = client()
        .post(format!("http://{addr}{path}"))
        .header("Authorization", format!("Splunk {TOKEN}"))
        .body(body.clone())
        .send()
        .await
        .expect("the request reaches splunk_hec_in");
    let elapsed = started.elapsed();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(response.headers().get("retry-after").and_then(|v| v.to_str().ok()), Some("1"));
    assert!(elapsed >= TEST_BUSY_AFTER, "answered after the bound, not before: {elapsed:?}");
    assert!(elapsed < Duration::from_secs(2), "answered promptly: {elapsed:?}");
    assert_eq!(response.text().await.unwrap(), r#"{"text":"Server is busy","code":9}"#);

    assert_eq!(recv(&mut rx).await, expected[0]);
    assert!(
        tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err(),
        "a 503'd request delivers nothing"
    );

    let (status, _) = post(addr, path, &body, true, None).await;
    assert_eq!(status, reqwest::StatusCode::OK, "the client's retry succeeds");
    assert_eq!(recv(&mut rx).await, expected[0]);
}

/// A body with a syntax error delivers the objects before the bad one, as Splunk indexes them,
/// and answers `400` code 6 naming it: after object 0, the first object; in object 0, nothing;
/// after the last object's closing brace, all three, naming 3.
#[tokio::test]
async fn a_code_6_delivers_the_objects_before_the_one_it_names() {
    let (addr, mut rx) = start(16).await;
    let (expected, _) = fixed_point_case(
        br#"{"time":1,"host":"h","event":"a"}{"time":2,"host":"h","event":"b"}{"time":3,"host":"h","event":"c"}"#,
    );
    let objects: Vec<Vec<u8>> = expected[0]
        .events
        .iter()
        .map(|event| {
            let batch = EventBatch { events: vec![event.clone()], ..expected[0].clone() };
            SplunkEncoder::new().encode(&batch).expect("encode never fails").to_vec()
        })
        .collect();
    let bad: &[u8] = br#"{"time":9,"host":"h","event":"#;
    let [a, b, c] = [&objects[0][..], &objects[1][..], &objects[2][..]];
    let path = "/services/collector/event";

    for (parts, index, delivered) in
        [(vec![a, bad, c], 1, 1), (vec![bad, b, c], 0, 0), (vec![a, b, c, b"}"], 3, 3)]
    {
        let body = parts.concat();
        for gzipped in [false, true] {
            let (status, text) = post(addr, path, &body, gzipped, None).await;
            assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "{text}");
            assert_eq!(
                text,
                format!(
                    r#"{{"text":"Invalid data format","code":6,"invalid-event-number":{index}}}"#
                )
            );
            if delivered > 0 {
                let batch = recv(&mut rx).await;
                assert_eq!(batch.events, expected[0].events[..delivered], "index {index}");
            }
            assert!(
                tokio::time::timeout(Duration::from_millis(100), rx.recv()).await.is_err(),
                "nothing from object {index} on"
            );
        }
    }
}

/// A wrong token is refused with Splunk's `403` code 4 and delivers nothing.
#[tokio::test]
async fn a_wrong_token_is_403_code_4() {
    let (addr, mut rx) = start(16).await;
    let response = client()
        .post(format!("http://{addr}/services/collector/event"))
        .header("Authorization", "Splunk not-the-token")
        .body(r#"{"time":1,"event":"x"}"#)
        .send()
        .await
        .expect("the request reaches splunk_hec_in");
    assert_eq!(response.status(), reqwest::StatusCode::FORBIDDEN);
    assert_eq!(response.text().await.unwrap(), r#"{"text":"Invalid token","code":4}"#);
    assert!(tokio::time::timeout(Duration::from_millis(300), rx.recv()).await.is_err());
}

// -------------------------------------------------------------------------------------------------
// Recorded traffic (testdata/interop/splunk/, `script/record-fixtures splunk`)
// -------------------------------------------------------------------------------------------------

/// Every request four real HEC clients sent (the OpenTelemetry Collector contrib 0.161.0
/// `splunk_hec` exporter, Docker 29.8.1's `splunk` log driver, Splunk Connect for Syslog 3.40.0,
/// and splunk-library-javalogging 1.11.11's Logback appenders), replayed with its recorded
/// method, path (query included), headers, and body (still gzip where sent). No `tokens` are
/// configured, so every recorded `Authorization` value passes. Every request, `OPTIONS` and `GET`
/// included, is answered `2xx`; the channel is generously sized and drained as it fills, so no
/// request sees backpressure.
#[tokio::test]
async fn every_recorded_request_is_answered_2xx() {
    let dir =
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/splunk");
    let mut input = SplunkHecInput::new("127.0.0.1:0");
    input.bind().await.expect("binding splunk_hec_in");
    let addr = input.local_addr().expect("bind() leaves an address");
    let (tx, mut rx) = mpsc::channel(16);
    tokio::spawn(async move {
        let _ = input.run(Fanout::new(vec![tx])).await;
    });
    // Drains continuously, concurrently with the send loop below, so a bounded channel never
    // fills and no request waits out `BUSY_AFTER`.
    let delivered = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = delivered.clone();
    tokio::spawn(async move {
        while rx.recv().await.is_some() {
            counted.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    });

    let mut stems: Vec<String> = std::fs::read_dir(&dir)
        .expect("the recorded corpus")
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".headers"))
        .map(|n| n.trim_end_matches(".headers").to_string())
        .collect();
    stems.sort();
    assert_eq!(stems.len(), 32, "the whole recorded corpus");
    let mut posts = 0;
    for stem in &stems {
        let sidecar = std::fs::read_to_string(dir.join(format!("{stem}.headers"))).unwrap();
        let body = std::fs::read(dir.join(format!("{stem}.bin"))).unwrap();
        let mut fields = sidecar.lines().filter_map(|l| l.split_once(": "));
        let (_, method) = fields.next().expect("method first");
        let (_, path) = fields.next().expect("path second");
        let method = reqwest::Method::from_bytes(method.as_bytes()).unwrap();
        if method == reqwest::Method::POST {
            posts += 1;
        }
        let mut request = client().request(method, format!("http://{addr}{path}"));
        for (name, value) in fields {
            // reqwest writes its own framing and host; `connection` is a hop-by-hop header the
            // recorded client sent its own proxy, not this listener.
            if !matches!(name, "host" | "content-length" | "connection") {
                request = request.header(name, value);
            }
        }
        let response = request.body(body).send().await.expect("the request reaches splunk_hec_in");
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        assert!(status.is_success(), "{stem} ({path}): {status} {text}");
    }

    // Every `POST` is an `/event` or `/raw` capture that carries at least one event, so each
    // delivers at least one batch; the `OPTIONS` and `GET` captures deliver nothing. The drain
    // task trails the last answer, so wait for it rather than read once.
    assert_eq!(posts, 28, "the corpus's /event and /raw captures");
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let mut seen = delivered.load(std::sync::atomic::Ordering::Relaxed);
    while seen < posts && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
        seen = delivered.load(std::sync::atomic::Ordering::Relaxed);
    }
    assert!(seen >= posts, "at least one batch per POST capture: {seen} batches for {posts}");
}
