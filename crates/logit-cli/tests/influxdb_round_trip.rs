//! `influxdb_in -> influxdb_out` round trip over real sockets. A client posts each corpus body to
//! a bound, live `influxdb_in`; the batch it delivers goes through `influxdb_out` to a capturing
//! fake InfluxDB, and the captured body must equal the case's `.expected` bytes. The components
//! run in-process, not through `logit run`. This lives in `logit-cli` because it already depends
//! on both `logit-inputs` and `logit-outputs`.
//!
//! Each byte-for-byte case asserts two things: the bytes `influxdb_out` sends equal `.expected`,
//! and a decode of those bytes equals the batch `influxdb_in` delivered, whole-event equal. That
//! is the fixed point ADR `lossless-transit` requires, modulo the numbered list in
//! `logit_proto::influxdb`'s module doc (`crates/logit-proto/src/influxdb/mod.rs`, "Permitted
//! normalizations"), which is the canonical copy. The table below cites its numbers.
//!
//! ## Fixture corpus (`tests/fixtures/influxdb/`)
//!
//! One file pair per case: `<name>.in`, a write body as a client sends it, and `<name>.expected`,
//! the body `influxdb_out` must send for it, or the marker [`SAME_AS_INPUT`]. Every `.in` is
//! hand-written, and every `.expected` was written from the normalization list, not copied from
//! the encoder. A `.in` with no `.expected` is a body `influxdb_in` must refuse. No `.in` leaves a
//! timestamp off, since the output would then depend on the receipt time;
//! [`an_absent_timestamp_leaves_as_the_receipt_time_truncated_to_precision`] covers that.
//!
//! `influxdb_out` writes tags in `logit_core::AttrMap`'s order, which is the order the process
//! first interned each key in, not string order. Normalization 3 says tags leave in interning
//! order, and influxd sorts the series key itself, but the bytes a `.expected` pins depend on it, and a
//! `cargo test` run shares one interner across every test in this file. So every [`Harness`]
//! first interns the corpus's tag keys in string order ([`CORPUS_TAG_KEYS`]), which makes the
//! `.expected` tag order string order under either runner.
//!
//! | Fixture | Normalizations | What it pins |
//! |---|---|---|
//! | `canonical` | none ([`SAME_AS_INPUT`]) | the baseline: one typed point with a string field |
//! | `telegraf-system` | **2**, **4** | Telegraf's `cpu`, `mem`, and `system` samples; `system`'s three same-instant points merge into one |
//! | `escaping` | **3**, **6** | probe rows 1 and 2: escapes dropped before `,`, space, and `=`; `\\` and `\` before a letter kept; a measurement's `\=` leaves as a bare `=` |
//! | `trailing-backslash` | none, refused | probe row 3: a tag value ending in `\` is a `400` naming the line, and the valid line before it isn't delivered |
//! | `duplicates` | **2**, **4**, **13** | tags in another order still merge, a later field overwrites across types or appends, a different timestamp is another point, and a repeated field key keeps its last value |
//! | `numbers` | **5** | `5.`, `.5`, `1.5e+3`, `007i`, `t`, and `FALSE` leave in their shortest spelling; `-0`, `u64::MAX`, and `i64::MIN` survive |
//! | `field-order` | **4** | string fields leave after the numeric ones |
//! | `comments` | **8** | comments, blank lines, and runs of spaces drop |
//! | `precision-s` | **7** | a body posted at `precision=s` leaves in nanoseconds, a negative timestamp included |
//! | `reserved-prefix` | **12** | a tag under `_logit.` or `influxdb.field.` drops, counted; any other `influxdb.` tag passes |

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use logit_core::{interner, EventBatch};
use logit_inputs::influxdb::InfluxDbInput;
use logit_outputs::influxdb::InfluxDbOutput;
use logit_pipeline::test_util::{assert_no_batch, recv_batch, TelemetryProbe};
use logit_pipeline::{Delivered, Fanout, Input, Output};
use logit_proto::influxdb::{InfluxDecoder, Precision};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::mpsc;

/// The `.expected` marker meaning "byte-identical to the `.in` file", shared with the other
/// round-trip corpora.
const SAME_AS_INPUT: &[u8] = b"== SAME AS INPUT ==";

const TOKEN: &str = "logit-round-trip-token";

/// Every tag key the corpus names, in string order. See the module doc for why [`Harness::new`]
/// interns them before anything else.
const CORPUS_TAG_KEYS: [&str; 8] =
    ["b", "city", "country", "cpu", "host", "influxdb.foo", "t,k", "w"];

/// `influxdb_in` delivers a request's batch, or refuses it, before it answers, so nothing a
/// request caused is in flight once its response is read; the window is a margin against a stray
/// later batch, not a delivery interval.
const NOTHING_MORE: Duration = Duration::from_millis(300);

/// The wall clock in Unix nanoseconds, as `influxdb_in` reads it.
fn now_nanos() -> i64 {
    let since_epoch = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is after 1970");
    i64::try_from(since_epoch.as_nanos()).expect("before 2262")
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/influxdb")
}

fn read(path: &Path) -> Vec<u8> {
    std::fs::read(path).unwrap_or_else(|e| panic!("reading fixture {}: {e}", path.display()))
}

/// One write the capturing fake received: its query string and body.
#[derive(Debug, Clone)]
struct Captured {
    query: String,
    authorization: String,
    body: Vec<u8>,
}

/// A fake InfluxDB that answers every request `204` and records it.
async fn spawn_capture() -> (SocketAddr, Arc<Mutex<Vec<Captured>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let captured: Arc<Mutex<Vec<Captured>>> = Arc::default();
    let record = Arc::clone(&captured);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else { return };
            let record = Arc::clone(&record);
            tokio::spawn(async move {
                let service = service_fn(move |req: Request<Incoming>| {
                    let record = Arc::clone(&record);
                    async move {
                        let query = req.uri().query().unwrap_or_default().to_string();
                        let authorization = req
                            .headers()
                            .get("authorization")
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        let body = req.into_body().collect().await.unwrap().to_bytes().to_vec();
                        record.lock().unwrap().push(Captured { query, authorization, body });
                        Ok::<_, Infallible>(
                            Response::builder().status(204).body(Full::new(Bytes::new())).unwrap(),
                        )
                    }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    (addr, captured)
}

/// A live `influxdb_in` and a capturing fake InfluxDB behind one `influxdb_out`.
struct Harness {
    input_addr: SocketAddr,
    rx: mpsc::Receiver<Delivered>,
    captured: Arc<Mutex<Vec<Captured>>>,
    out: InfluxDbOutput,
    probe: TelemetryProbe,
    client: reqwest::Client,
}

impl Harness {
    async fn new() -> Self {
        for key in CORPUS_TAG_KEYS {
            interner::intern(key);
        }
        let probe = TelemetryProbe::new();
        let mut input = InfluxDbInput::new("127.0.0.1:0")
            .with_tokens(vec![TOKEN.to_string()])
            .with_telemetry(probe.telemetry("in", "influxdb_in", "listener"));
        input.bind().await.expect("binding influxdb_in");
        let input_addr = input.local_addr().expect("bind() leaves an address");
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            let _ = input.run(Fanout::new(vec![tx])).await;
        });
        let (capture_addr, captured) = spawn_capture().await;
        let out = InfluxDbOutput::new(
            format!("http://{capture_addr}"),
            "central".into(),
            "relayed".into(),
            "sink-token".into(),
        )
        .with_telemetry(probe.telemetry("out", "influxdb_out", "sink"));
        Self { input_addr, rx, captured, out, probe, client: reqwest::Client::new() }
    }

    /// Posts `body` as a client would, at `precision`, and returns the status and response body.
    async fn post(&self, body: Vec<u8>, precision: &str) -> (u16, String) {
        let resp = self
            .client
            .post(format!("http://{}/api/v2/write", self.input_addr))
            .query(&[("org", "edge"), ("bucket", "telegraf"), ("precision", precision)])
            .header("Authorization", format!("Token {TOKEN}"))
            .body(body)
            .send()
            .await
            .expect("influxdb_in answers");
        let status = resp.status().as_u16();
        (status, resp.text().await.unwrap())
    }

    /// Posts `body`, expects `204`, and returns the batch `influxdb_in` delivered.
    async fn deliver(&mut self, body: Vec<u8>, precision: &str) -> EventBatch {
        let (status, text) = self.post(body, precision).await;
        assert_eq!(status, 204, "influxdb_in accepts the body: {text}");
        recv_batch(&mut self.rx).await
    }

    /// Sends `batch` through `influxdb_out` and returns the one write the fake captured.
    async fn relay(&mut self, batch: &EventBatch) -> Captured {
        let before = self.captured.lock().unwrap().len();
        self.out.send(batch).await.expect("the fake answers 204");
        let captured = self.captured.lock().unwrap();
        assert_eq!(captured.len(), before + 1, "one write per batch");
        captured[before].clone()
    }
}

/// The two assertions of the module doc for one corpus case.
async fn assert_case(name: &str) -> Harness {
    let input = read(&fixtures_dir().join(format!("{name}.in")));
    let expected = read(&fixtures_dir().join(format!("{name}.expected")));
    let expected = if expected == SAME_AS_INPUT { input.clone() } else { expected };
    let precision = if name.starts_with("precision-s") { "s" } else { "ns" };

    let mut h = Harness::new().await;
    let delivered = h.deliver(input, precision).await;
    let captured = h.relay(&delivered).await;
    assert_eq!(
        String::from_utf8_lossy(&captured.body),
        String::from_utf8_lossy(&expected),
        "{name}: influxdb_out's body"
    );
    assert_eq!(captured.query, "org=central&bucket=relayed&precision=ns", "{name}");
    assert_eq!(captured.authorization, "Token sink-token", "{name}");

    let redecoded = InfluxDecoder::new()
        .decode_body(Bytes::from(captured.body), Precision::Ns, 0, delivered.resource.clone())
        .unwrap_or_else(|err| panic!("{name}: the relayed body decodes: {err}"));
    assert_eq!(redecoded, delivered, "{name}: decode(relayed) equals what influxdb_in delivered");
    assert_no_batch(&mut h.rx, NOTHING_MORE, "one batch per write").await;
    h
}

#[tokio::test]
async fn canonical() {
    assert_case("canonical").await;
}

#[tokio::test]
async fn telegraf_system() {
    assert_case("telegraf-system").await;
}

#[tokio::test]
async fn escaping() {
    assert_case("escaping").await;
}

#[tokio::test]
async fn duplicates() {
    assert_case("duplicates").await;
}

#[tokio::test]
async fn numbers() {
    assert_case("numbers").await;
}

#[tokio::test]
async fn field_order() {
    assert_case("field-order").await;
}

#[tokio::test]
async fn comments() {
    assert_case("comments").await;
}

#[tokio::test]
async fn precision_s() {
    assert_case("precision-s").await;
}

#[tokio::test]
async fn reserved_prefix() {
    let mut h = assert_case("reserved-prefix").await;
    let totals = h.probe.poll();
    assert_eq!(totals.sum("logit.input.tags.omitted", &[("reason", "reserved_prefix")]), 2.0);
    assert!(!totals.has("logit.input.tags.dropped", &[]), "the point is delivered");
}

/// Every `.in` file is covered by a test above or by `trailing_backslash`, and every `.expected`
/// has its `.in`.
#[test]
fn every_corpus_file_has_a_test() {
    let tested = [
        "canonical",
        "telegraf-system",
        "escaping",
        "trailing-backslash",
        "duplicates",
        "numbers",
        "field-order",
        "comments",
        "precision-s",
        "reserved-prefix",
    ];
    assert!(CORPUS_TAG_KEYS.windows(2).all(|w| w[0] < w[1]), "CORPUS_TAG_KEYS is in string order");
    for entry in std::fs::read_dir(fixtures_dir()).unwrap() {
        let path = entry.unwrap().path();
        let stem = path.file_stem().unwrap().to_str().unwrap().to_string();
        assert!(tested.contains(&stem.as_str()), "{} has no test", path.display());
        assert!(path.with_extension("in").exists(), "{} has no .in", path.display());
    }
}

/// Probe row 3: a tag value ending in a backslash has no spelling, so the line fails; influxd's
/// `400` names the line, and the valid line before it isn't delivered either (one request, one
/// batch, all or nothing).
#[tokio::test]
async fn trailing_backslash() {
    let mut h = Harness::new().await;
    let (status, text) = h.post(read(&fixtures_dir().join("trailing-backslash.in")), "ns").await;
    assert_eq!(status, 400);
    let body: serde_json::Value = serde_json::from_str(&text).expect("influxd's error JSON");
    assert_eq!(body["code"], "invalid");
    let message = body["message"].as_str().unwrap();
    assert!(
        message.starts_with(r"unable to parse 'cpu,host=web-1\ f=2i 1700000000000000000': "),
        "{message}"
    );
    assert_no_batch(&mut h.rx, NOTHING_MORE, "a refused body delivers nothing").await;
    let totals = h.probe.poll();
    assert_eq!(totals.sum("logit.input.requests.rejected", &[("reason", "malformed")]), 1.0);
    assert!(h.captured.lock().unwrap().is_empty());
}

/// Normalization 2: six lines arrive as four points, `system`-style merges included, and the body
/// `influxdb_out` sends writes each merged point once with its last value per field.
#[tokio::test]
async fn merged_duplicates_leave_as_one_line_per_point() {
    let mut h = Harness::new().await;
    let input = read(&fixtures_dir().join("duplicates.in"));
    let lines = input.split(|&b| b == b'\n').filter(|l| !l.is_empty()).count();
    let delivered = h.deliver(input, "ns").await;
    assert_eq!((lines, delivered.events.len()), (6, 4), "two merges in six lines");

    let paris = &delivered.events[0];
    let names: Vec<&str> = paris.metrics.iter().map(|m| interner::resolve(m.name)).collect();
    assert_eq!(names, ["weather.temp", "weather.wind"], "hum left the metrics for a string");
    assert_eq!(paris.attributes.get("influxdb.field.hum").and_then(|v| v.as_str()), Some("high"));

    let body = String::from_utf8(h.relay(&delivered).await.body).unwrap();
    assert_eq!(
        body.lines().next(),
        Some(r#"weather,city=paris,country=fr temp=21,wind=3.5,hum="high" 1700000000000000000"#)
    );
    assert_eq!(body.lines().count(), 4);
}

/// Normalizations 7 and 14: points with no timestamp leave with the request's receipt time
/// truncated to `precision`, one instant for every such point in the request.
#[tokio::test]
async fn an_absent_timestamp_leaves_as_the_receipt_time_truncated_to_precision() {
    let mut h = Harness::new().await;
    for (precision, unit) in [("s", 1_000_000_000), ("ms", 1_000_000), ("us", 1_000), ("ns", 1)] {
        let before = now_nanos();
        let delivered =
            h.deliver(b"clock,host=a f=1i\nclock,host=b f=2i\n".to_vec(), precision).await;
        let after = now_nanos();
        let stamps: Vec<i64> = delivered.events.iter().map(|e| e.timestamp).collect();
        assert_eq!(stamps.len(), 2, "{precision}");
        assert_eq!(stamps[0], stamps[1], "{precision}: one instant per request");
        let ts = stamps[0];
        assert_eq!(ts % unit, 0, "{precision}: truncated");
        assert!(
            (before - before.rem_euclid(unit)..=after).contains(&ts),
            "{precision}: {ts} within the request's window {before}..={after}"
        );

        let body = String::from_utf8(h.relay(&delivered).await.body).unwrap();
        assert_eq!(
            body,
            format!("clock,host=a f=1i {ts}\nclock,host=b f=2i {ts}\n"),
            "{precision}"
        );
    }
}
