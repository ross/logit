//! Test support for [`super::Slots`]: [`Held`], an HTTP receiver that answers each request as a
//! script says, now or when the test releases it, and the scenarios each sink on the slot helper
//! runs through the real write loop. A sink that sends a batch as several requests runs them on
//! batches that each send one.
//!
//! Each request gets its own connection (`Connection: close`) and its own task, so a held request
//! never delays another, and a request whose client goes away while it's held is marked closed.

use logit_core::{
    AttrMap, BodyFormat, Event, EventBatch, LogRecord, SpanKind, SpanRecord, SpanStatus, Telemetry,
    Value,
};
use logit_pipeline::test_util::{drive_write_loop_until, wait_until, TelemetryProbe, Totals};
use logit_pipeline::{DeliveryPosture, Output, RetryConfig, WriteLoopConfig};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub(crate) const RESP_204: &str = "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
pub(crate) const RESP_400: &str =
    "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub(crate) const RESP_401: &str =
    "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub(crate) const RESP_405: &str =
    "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
pub(crate) const RESP_500: &str =
    "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";

/// What [`Held`] does with a request, given its batch and how many earlier requests carried
/// that batch.
pub(crate) enum Answer {
    Now(&'static str),
    Hold,
}

struct Arrival {
    batch: u64,
    /// The request target's path.
    path: String,
    body: Vec<u8>,
    reply: Option<tokio::sync::oneshot::Sender<&'static str>>,
    /// The client closed the connection before an answer was written.
    closed: bool,
}

type Script = dyn Fn(u64, &str, usize) -> Answer + Send + Sync;

pub(crate) struct Held {
    url: String,
    arrivals: Arc<Mutex<Vec<Arrival>>>,
}

impl Held {
    /// Starts the receiver. `id_of` reads which batch a request body carries.
    pub(crate) async fn start(
        id_of: fn(&[u8]) -> u64,
        script: impl Fn(u64, usize) -> Answer + Send + Sync + 'static,
    ) -> Self {
        Self::start_by_path(id_of, move |batch, _, earlier| script(batch, earlier)).await
    }

    /// [`Held::start`], whose script also reads the request's path.
    pub(crate) async fn start_by_path(
        id_of: fn(&[u8]) -> u64,
        script: impl Fn(u64, &str, usize) -> Answer + Send + Sync + 'static,
    ) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let arrivals: Arc<Mutex<Vec<Arrival>>> = Arc::default();
        let script: Arc<Script> = Arc::new(script);
        let shared = Arc::clone(&arrivals);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else { return };
                let (arrivals, script) = (Arc::clone(&shared), Arc::clone(&script));
                tokio::spawn(serve(stream, arrivals, script, id_of));
            }
        });
        Self { url, arrivals }
    }

    pub(crate) fn url(&self) -> &str {
        &self.url
    }

    /// Every request's batch, in arrival order.
    pub(crate) fn batches(&self) -> Vec<u64> {
        self.arrivals.lock().unwrap().iter().map(|a| a.batch).collect()
    }

    /// The bodies of `batch`'s requests, in arrival order.
    pub(crate) fn bodies(&self, batch: u64) -> Vec<Vec<u8>> {
        let arrivals = self.arrivals.lock().unwrap();
        arrivals.iter().filter(|a| a.batch == batch).map(|a| a.body.clone()).collect()
    }

    /// The paths of `batch`'s requests, in arrival order.
    pub(crate) fn paths(&self, batch: u64) -> Vec<String> {
        let arrivals = self.arrivals.lock().unwrap();
        arrivals.iter().filter(|a| a.batch == batch).map(|a| a.path.clone()).collect()
    }

    /// Requests waiting for [`Held::release`].
    pub(crate) fn held(&self) -> usize {
        self.arrivals.lock().unwrap().iter().filter(|a| a.reply.is_some()).count()
    }

    /// Answers the held request carrying `batch` with `response`.
    pub(crate) fn release(&self, batch: u64, response: &'static str) {
        let mut arrivals = self.arrivals.lock().unwrap();
        let arrival = arrivals
            .iter_mut()
            .find(|a| a.batch == batch && a.reply.is_some())
            .unwrap_or_else(|| panic!("no held request carries batch {batch}"));
        let _ = arrival.reply.take().unwrap().send(response);
    }

    /// Whether the client closed every held request's connection before it was answered.
    pub(crate) fn all_closed(&self) -> bool {
        self.arrivals.lock().unwrap().iter().all(|a| a.closed)
    }
}

async fn serve(
    mut stream: tokio::net::TcpStream,
    arrivals: Arc<Mutex<Vec<Arrival>>>,
    script: Arc<Script>,
    id_of: fn(&[u8]) -> u64,
) {
    let Some((path, body)) = read_request(&mut stream).await else { return };
    let batch = id_of(&body);
    let (tx, rx) = tokio::sync::oneshot::channel();
    let (index, answer) = {
        let mut arrivals = arrivals.lock().unwrap();
        let earlier = arrivals.iter().filter(|a| a.batch == batch).count();
        let answer = script(batch, &path, earlier);
        let reply = matches!(answer, Answer::Hold).then_some(tx);
        arrivals.push(Arrival { batch, path, body, reply, closed: false });
        (arrivals.len() - 1, answer)
    };
    let response = match answer {
        Answer::Now(response) => response,
        Answer::Hold => {
            let mut buf = [0u8; 64];
            tokio::select! {
                response = rx => match response {
                    Ok(response) => response,
                    Err(_) => return,
                },
                _ = stream.read(&mut buf) => {
                    arrivals.lock().unwrap()[index].closed = true;
                    return;
                }
            }
        }
    };
    let _ = stream.write_all(response.as_bytes()).await;
    let _ = stream.shutdown().await;
}

/// One request's path and body, the body read by its `Content-Length`.
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<(String, Vec<u8>)> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    let head_end = loop {
        if let Some(at) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
    let path = head.split(' ').nth(1).unwrap_or_default().to_string();
    let length: usize = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(0);
    while buf.len() < head_end + length {
        let n = stream.read(&mut chunk).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&chunk[..n]);
    }
    Some((path, buf[head_end..head_end + length].to_vec()))
}

/// The batch number a body carries as [`marker`]`(n)`, as each multi-request sink's test batches
/// write it into a log message, a span name, or a metric name.
pub(crate) fn id_after_marker(body: &[u8]) -> u64 {
    const MARKER: &[u8] = b"batch-";
    let at = body
        .windows(MARKER.len())
        .position(|w| w == MARKER)
        .unwrap_or_else(|| panic!("no batch marker in {:?}", String::from_utf8_lossy(body)));
    let digits: String = body[at + MARKER.len()..]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .map(|&b| char::from(b))
        .collect();
    digits.parse().expect("a batch number after the marker")
}

/// `batch-<n>.`, the marker [`id_after_marker`] reads. The `.` ends the number, since a
/// protobuf body's next byte can be an ASCII digit.
pub(crate) fn marker(n: u64) -> String {
    format!("batch-{n}.")
}

/// A log whose message is [`marker`]`(n)`.
pub(crate) fn marker_log(timestamp: i64, n: u64) -> Event {
    Event::log(
        timestamp,
        AttrMap::new(),
        LogRecord {
            message: Value::str(marker(n)),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// A span named [`marker`]`(n)`, carrying `attributes`.
pub(crate) fn marker_span(timestamp: i64, n: u64, attributes: AttrMap) -> Event {
    Event::span(
        timestamp,
        attributes,
        SpanRecord {
            trace_id: [1; 16],
            span_id: [n as u8; 8],
            parent_span_id: None,
            name: Value::str(marker(n)),
            kind: SpanKind::Internal,
            status: SpanStatus::Unset,
            events: Vec::new(),
            links: Vec::new(),
            end_timestamp: timestamp + 1_000,
            flags: 0,
            ext: None,
        },
    )
}

/// A sink on the slot helper, as each scenario builds and feeds it.
pub(crate) struct Subject<O> {
    pub kind: &'static str,
    /// The sink at `url` with `concurrency`, a request timeout far past the test's, and
    /// `telemetry` as its handle.
    pub make: fn(url: &str, concurrency: u32, telemetry: Telemetry) -> O,
    /// Batch `n`, which `id_of` reads back from its request body.
    pub batch: fn(u64) -> EventBatch,
    pub id_of: fn(&[u8]) -> u64,
    /// An encode-side counter each batch counts once.
    pub encoded: (&'static str, &'static [(&'static str, &'static str)]),
    /// A response the sink reads as `Refused` ([`RESP_401`] on most).
    pub refuse: &'static str,
}

fn config(posture: DeliveryPosture) -> WriteLoopConfig {
    WriteLoopConfig {
        retry: RetryConfig {
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(10),
        },
        delivery_override: Some(posture),
        shutdown_grace: Duration::from_millis(50),
        ..WriteLoopConfig::default()
    }
}

/// Runs `batches` through `write_loop` beside `control`, signalling shutdown once `stop`
/// resolves. Returns the sink's totals and the grace cut's drops, with the sink still alive.
pub(crate) async fn run<O: Output + Send>(
    output: &mut O,
    probe: &mut TelemetryProbe,
    telemetry: Telemetry,
    batches: Vec<EventBatch>,
    posture: DeliveryPosture,
    stop: impl std::future::Future<Output = ()>,
    control: impl std::future::Future<Output = ()>,
) -> (Totals, u64) {
    let (dropped, ()) = tokio::join!(
        drive_write_loop_until(output, batches, config(posture), telemetry, stop),
        control
    );
    (probe.poll().clone(), dropped)
}

pub(crate) fn sink<O>(
    subject: &Subject<O>,
    held: &Held,
    concurrency: u32,
) -> (O, TelemetryProbe, Telemetry) {
    let probe = TelemetryProbe::new();
    let telemetry = probe.telemetry("out", subject.kind, "sink");
    ((subject.make)(held.url(), concurrency, telemetry.clone()), probe, telemetry)
}

pub(crate) fn delivered(totals: &Totals) -> f64 {
    totals.sum("logit.component.batches.delivered", &[])
}

/// Holds every first request until all of batches `1..=n` are in flight, then answers each by
/// `answer`, in reverse batch order; every later request is answered 204 at once.
async fn hold_all(held: &Held, n: u64, answer: impl Fn(u64) -> &'static str) {
    wait_until("every batch in flight at once", || held.held() == n as usize).await;
    let mut seen = held.batches();
    seen.sort_unstable();
    assert_eq!(seen, (1..=n).collect::<Vec<_>>(), "one request per batch");
    for batch in (1..=n).rev() {
        held.release(batch, answer(batch));
    }
}

fn first_held(_: u64, earlier: usize) -> Answer {
    if earlier == 0 {
        Answer::Hold
    } else {
        Answer::Now(RESP_204)
    }
}

/// Four batches in flight at once, answered out of order, each delivered once.
pub(crate) async fn n_requests_in_flight_answered_out_of_order<O: Output + Send>(
    subject: Subject<O>,
) {
    let held = Held::start(subject.id_of, first_held).await;
    let (mut output, mut probe, telemetry) = sink(&subject, &held, 4);
    let probe_view = TelemetryProbe::with_registry(Arc::clone(probe.registry()));
    let control = async {
        let mut probe = probe_view;
        wait_until("four requests held", || held.held() == 4).await;
        assert_eq!(probe.gauge("logit.output.in_flight", &[]), Some(4.0));
        hold_all(&held, 4, |_| RESP_204).await;
    };
    let pending = std::future::pending();
    let (totals, _) = run(
        &mut output,
        &mut probe,
        telemetry,
        (1..=4).map(subject.batch).collect(),
        DeliveryPosture::AtLeastOnce,
        pending,
        control,
    )
    .await;
    assert_eq!(delivered(&totals), 4.0);
    assert_eq!(held.batches().len(), 4, "no batch was sent twice: {:?}", held.batches());
    assert_eq!(totals.gauge("logit.output.in_flight", &[]), Some(0.0));
}

/// A `500` on one of four batches in flight resends that batch alone; the others deliver once,
/// and each batch's encode-side counter counts once.
pub(crate) async fn a_500_on_one_slot_is_resent_alone<O: Output + Send>(subject: Subject<O>) {
    let held = Held::start(subject.id_of, first_held).await;
    let (mut output, mut probe, telemetry) = sink(&subject, &held, 4);
    let control = hold_all(&held, 4, |b| if b == 2 { RESP_500 } else { RESP_204 });
    let (totals, _) = run(
        &mut output,
        &mut probe,
        telemetry,
        (1..=4).map(subject.batch).collect(),
        DeliveryPosture::AtLeastOnce,
        std::future::pending(),
        control,
    )
    .await;
    let mut batches = held.batches();
    batches.sort_unstable();
    assert_eq!(batches, [1, 2, 2, 3, 4], "batch 2 alone was resent");
    assert_eq!(delivered(&totals), 4.0);
    assert_eq!(totals.sum("logit.output.requests", &[("class", "5xx")]), 1.0);
    assert_eq!(totals.sum("logit.output.requests", &[("class", "2xx")]), 4.0);
    assert_eq!(totals.sum("logit.component.retries", &[]), 1.0);
    let (name, tags) = subject.encoded;
    assert_eq!(totals.sum(name, tags), 4.0, "{name} once per batch");
}

/// A `400` on one of four batches in flight drops that batch alone.
pub(crate) async fn a_400_on_one_slot_is_dropped_alone<O: Output + Send>(subject: Subject<O>) {
    let held = Held::start(subject.id_of, first_held).await;
    let (mut output, mut probe, telemetry) = sink(&subject, &held, 4);
    let control = hold_all(&held, 4, |b| if b == 2 { RESP_400 } else { RESP_204 });
    let (totals, _) = run(
        &mut output,
        &mut probe,
        telemetry,
        (1..=4).map(subject.batch).collect(),
        DeliveryPosture::AtLeastOnce,
        std::future::pending(),
        control,
    )
    .await;
    assert_eq!(held.batches().len(), 4, "nothing was resent: {:?}", held.batches());
    assert_eq!(totals.sum("logit.component.batches.dropped", &[("reason", "rejected")]), 1.0);
    assert_eq!(delivered(&totals), 3.0);
}

/// A head the receiver refuses (`401`, or the subject's `refuse`) three times holds the fill at
/// one window: only the two batches the window holds reach the receiver until the head is
/// answered `204`.
pub(crate) async fn a_401_head_holds_the_fill_at_one_window<O: Output + Send>(subject: Subject<O>) {
    let refuse = subject.refuse;
    let held = Held::start(subject.id_of, move |batch, earlier| match (batch, earlier) {
        (1, 0..=2) => Answer::Now(refuse),
        _ => Answer::Now(RESP_204),
    })
    .await;
    let (mut output, mut probe, telemetry) = sink(&subject, &held, 2);
    let (totals, _) = run(
        &mut output,
        &mut probe,
        telemetry,
        (1..=5).map(subject.batch).collect(),
        DeliveryPosture::AtLeastOnce,
        std::future::pending(),
        async {},
    )
    .await;
    let batches = held.batches();
    let accepted = batches
        .iter()
        .enumerate()
        .filter(|(_, b)| **b == 1)
        .nth(3)
        .map(|(i, _)| i)
        .expect("batch 1's fourth request was accepted");
    assert!(
        batches[..accepted].iter().all(|b| *b <= 2),
        "only the window's two batches reach a refusing receiver: {batches:?}"
    );
    assert_eq!(delivered(&totals), 5.0);
    assert_eq!(totals.sum("logit.output.requests", &[("class", "4xx")]), 3.0);
}

/// Under at-most-once, a `500` on one of four batches in flight drops that batch alone, and
/// nothing is resent.
pub(crate) async fn at_most_once_drops_one_ambiguous_slot_alone<O: Output + Send>(
    subject: Subject<O>,
) {
    let held = Held::start(subject.id_of, first_held).await;
    let (mut output, mut probe, telemetry) = sink(&subject, &held, 4);
    let control = hold_all(&held, 4, |b| if b == 2 { RESP_500 } else { RESP_204 });
    let (totals, _) = run(
        &mut output,
        &mut probe,
        telemetry,
        (1..=4).map(subject.batch).collect(),
        DeliveryPosture::AtMostOnce,
        std::future::pending(),
        control,
    )
    .await;
    assert_eq!(held.batches().len(), 4, "nothing was resent: {:?}", held.batches());
    let dropped =
        totals.sum("logit.component.batches.dropped", &[("reason", "ambiguous_at_most_once")]);
    assert_eq!(dropped, 1.0, "only the ambiguous batch is dropped");
    assert_eq!(delivered(&totals), 3.0);
}

/// The shutdown grace cuts four requests in flight, and each is aborted: the receiver sees every
/// held connection closed, long before any request's own timeout.
pub(crate) async fn a_grace_cut_aborts_the_requests_in_flight<O: Output + Send>(
    subject: Subject<O>,
) {
    let held = Held::start(subject.id_of, |_, _| Answer::Hold).await;
    let (mut output, mut probe, telemetry) = sink(&subject, &held, 4);
    let stop = async {
        wait_until("four requests held", || held.held() == 4).await;
    };
    let (totals, dropped) = run(
        &mut output,
        &mut probe,
        telemetry,
        (1..=4).map(subject.batch).collect(),
        DeliveryPosture::AtLeastOnce,
        stop,
        async {},
    )
    .await;
    assert_eq!(delivered(&totals), 0.0);
    assert_eq!(dropped, 0, "at-least-once leaves the cut batches queued");
    wait_until("every request in flight aborted", || held.all_closed()).await;
    drop(output);
}

/// A batch answered at once behind a head held for `HEAD_WAIT` and then retried records its own
/// request's duration in `logit.component.send.duration`, not the head's wait. Of the three
/// samples (the head's held attempt, its resend, and the second batch), only the first reaches
/// `HEAD_WAIT`, so the median is under it.
pub(crate) async fn a_batch_behind_a_retried_head_times_its_own_request<O: Output + Send>(
    subject: Subject<O>,
) {
    const HEAD_WAIT: Duration = Duration::from_millis(300);
    let held = Held::start(subject.id_of, |batch, earlier| match (batch, earlier) {
        (1, 0) => Answer::Hold,
        _ => Answer::Now(RESP_204),
    })
    .await;
    let (mut output, mut probe, telemetry) = sink(&subject, &held, 2);
    let control = async {
        wait_until("both batches sent", || held.batches().len() == 2).await;
        // Sizes the head's wait, which the second batch's sample must not include.
        tokio::time::sleep(HEAD_WAIT).await;
        held.release(1, RESP_500);
    };
    let (totals, _) = run(
        &mut output,
        &mut probe,
        telemetry,
        (1..=2).map(subject.batch).collect(),
        DeliveryPosture::AtLeastOnce,
        std::future::pending(),
        control,
    )
    .await;
    assert_eq!(delivered(&totals), 2.0);
    let mut durations = logit_core::DdSketch::new();
    for metric in totals.events.iter().flat_map(|event| event.metrics.iter()) {
        if logit_core::interner::resolve(metric.name) == "logit.component.send.duration" {
            match &metric.kind {
                logit_core::MetricKind::Distribution(sketch) => durations.merge(sketch),
                other => panic!("send.duration is a distribution, got {other:?}"),
            }
        }
    }
    assert_eq!(durations.count(), 3, "one sample per answered request");
    let median = durations.quantile(0.5).expect("three samples");
    assert!(median < HEAD_WAIT.as_secs_f64() / 2.0, "median {median}s includes the head's wait");
}
