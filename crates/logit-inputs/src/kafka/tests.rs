//! `kafka_in` against the fake broker, through the real `Input`: each format under each
//! `record:` mode read back from `kafka_out`'s encoder, the carriers, and one test per row of the
//! outcome table in the parent module's doc.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use kafka_protocol::messages::ApiKey;
use kafka_protocol::records::Compression;
use logit_core::interner::resolve;
use logit_core::{AttrMap, BodyFormat, Diagnostics, Event, EventBatch, LogRecord, Resource, Value};
use logit_kafka::consumer::{ConsumerSettings, StartPosition};
use logit_kafka::error::code;
use logit_kafka::fake_broker::{Action, FakeCluster, Step};
use logit_outputs::kafka::{KafkaEncoder, RecordFormat, RecordKey, RecordMeta, RecordMode};
use logit_pipeline::test_util::{
    fanout_channel, recv_batch, recv_events, spawn_input, wait_until, TelemetryProbe, RECV_TIMEOUT,
};
use logit_pipeline::{Edge, Fanout};
use logit_proto::kafka::batch::ProducerRecord;
use logit_proto::{FramedEncoder, MessageBuf};
use tokio::time::Instant;

use super::{KafkaInFormat, KafkaInSettings, KafkaInput};

const GROUP: &str = "g";

/// A cluster of one node with topic `t` of one partition.
async fn cluster() -> FakeCluster {
    let cluster = FakeCluster::start(1).await;
    cluster.state().add_topic("t", &[0]);
    cluster
}

/// Settings scaled for a test: short waits and heartbeats, from the log start, and no periodic
/// commit unless a test sets one.
fn settings(cluster: &FakeCluster, format: KafkaInFormat) -> KafkaInSettings {
    let mut c = ConsumerSettings::new(vec![cluster.addr(0)], GROUP, vec!["t".to_string()]);
    c.start = StartPosition::Earliest;
    c.session_timeout = Duration::from_secs(6);
    c.heartbeat_interval = Duration::from_millis(50);
    c.rebalance_timeout = Duration::from_secs(3);
    c.commit_interval = Duration::from_secs(60);
    c.timeout = Duration::from_secs(2);
    c.fetch.max_wait = Duration::from_millis(50);
    KafkaInSettings::new(c, format)
}

fn input(probe: &TelemetryProbe, settings: KafkaInSettings) -> KafkaInput {
    let telemetry = probe.telemetry("in", "kafka_in", "listener");
    let diag = Diagnostics::new("in").with_telemetry(telemetry.clone());
    KafkaInput::new(settings).with_telemetry(telemetry).with_diagnostics(diag)
}

fn committed(cluster: &FakeCluster) -> Option<i64> {
    cluster.state().groups.get(GROUP)?.committed.get(&("t".to_string(), 0)).copied()
}

fn members(cluster: &FakeCluster) -> usize {
    cluster.state().groups.get(GROUP).map_or(0, |g| g.members.len())
}

fn record(value: Option<&[u8]>) -> ProducerRecord {
    ProducerRecord {
        key: None,
        value: value.map(Bytes::copy_from_slice),
        headers: Default::default(),
        timestamp: 1_000,
    }
}

/// Appends one batch of `values` to `t`/0.
fn append(cluster: &FakeCluster, values: &[Option<&[u8]>]) {
    let records: Vec<_> = values.iter().map(|v| record(*v)).collect();
    cluster.state().append("t", 0, &records, Compression::None);
}

fn log(message: &str, attrs: &[(&str, &str)]) -> Event {
    let mut attributes = AttrMap::new();
    for &(k, v) in attrs {
        attributes.insert(k, Value::str(v));
    }
    Event::log(
        1_700_000_000_123_000_000,
        attributes,
        LogRecord {
            message: Value::str(message),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    )
}

/// `event` with its `_logit.kafka.*` carriers removed.
fn without_carriers(mut event: Event) -> Event {
    let keys: Vec<&str> = event
        .attributes
        .iter()
        .map(|(k, _)| resolve(k))
        .filter(|k| k.starts_with("_logit.kafka."))
        .collect();
    for k in keys {
        event.attributes.remove(k);
    }
    event
}

fn message(event: &Event) -> String {
    match &event.log.as_ref().expect("a log").message {
        Value::Str(b) | Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        other => panic!("a message of {other:?}"),
    }
}

/// The records `kafka_out` writes for `batch` under `format` and `mode`.
fn encoded(batch: &EventBatch, format: RecordFormat, mode: RecordMode) -> Vec<ProducerRecord> {
    let mut encoder = KafkaEncoder::new(&RecordKey::None, format, mode, 1 << 20);
    let mut out: MessageBuf<RecordMeta> = MessageBuf::default();
    encoder.encode_into(batch, &mut out);
    out.iter_with()
        .map(|(value, meta)| ProducerRecord {
            key: meta.key.clone(),
            value: Some(Bytes::copy_from_slice(value)),
            headers: Default::default(),
            timestamp: meta.timestamp,
        })
        .collect()
}

/// One batch of `events` as `kafka_out` writes it under `format`, one value.
fn one_value(events: Vec<Event>, format: RecordFormat) -> Bytes {
    let batch = EventBatch { resource: Arc::new(Resource::default()), scope: None, events };
    let mut records = encoded(&batch, format, RecordMode::Batch);
    assert_eq!(records.len(), 1);
    records.pop().unwrap().value.unwrap()
}

/// What `kafka_out` writes for `batch` under `format` and `mode`, appended to `t`/0 as one record
/// batch.
fn produce(cluster: &FakeCluster, batch: &EventBatch, format: RecordFormat, mode: RecordMode) {
    let records = encoded(batch, format, mode);
    cluster.state().append("t", 0, &records, Compression::None);
}

#[tokio::test]
async fn every_format_under_each_record_mode_reads_back_what_kafka_out_wrote() {
    let resource = Arc::new(Resource::default());
    let events = vec![
        log("first line\nsecond \\ line", &[("a", "1")]),
        log("plain", &[("b", "2"), ("c", "3")]),
        log("", &[]),
    ];
    let batch = EventBatch { resource, scope: None, events: events.clone() };
    let cases = [
        (RecordFormat::Json, RecordMode::Event, KafkaInFormat::Json),
        (RecordFormat::Json, RecordMode::Batch, KafkaInFormat::Json),
        (RecordFormat::Native, RecordMode::Event, KafkaInFormat::Native),
        (RecordFormat::Native, RecordMode::Batch, KafkaInFormat::Native),
        (RecordFormat::Text, RecordMode::Event, KafkaInFormat::Text { batch: false }),
        (RecordFormat::Text, RecordMode::Batch, KafkaInFormat::Text { batch: true }),
    ];
    for (out_format, mode, in_format) in cases {
        let name = format!("{out_format:?} under {mode:?}");
        let cluster = cluster().await;
        produce(&cluster, &batch, out_format, mode);
        let probe = TelemetryProbe::new();
        let (sink, mut rx) = fanout_channel(16);
        let running = spawn_input(input(&probe, settings(&cluster, in_format)), sink).await;
        let got: Vec<Event> =
            recv_events(&mut rx, events.len()).await.into_iter().map(without_carriers).collect();
        running.stop().await;
        assert_eq!(got.len(), events.len(), "{name}");
        if matches!(in_format, KafkaInFormat::Text { .. }) {
            let want: Vec<_> = events.iter().map(message).collect();
            assert_eq!(got.iter().map(message).collect::<Vec<_>>(), want, "{name}");
            // A text log carries the record's timestamp, which `kafka_out` writes in milliseconds.
            for e in &got {
                assert_eq!(e.timestamp, 1_700_000_000_123_000_000, "{name}");
            }
        } else {
            assert_eq!(got, events, "{name}");
        }
    }
}

// ---- carriers ---------------------------------------------------------------------------

fn varint(out: &mut Vec<u8>, v: i64) {
    let mut z = ((v << 1) ^ (v >> 63)) as u64;
    while z >= 0x80 {
        out.push((z as u8 & 0x7f) | 0x80);
        z >>= 7;
    }
    out.push(z as u8);
}

fn field(out: &mut Vec<u8>, f: Option<&[u8]>) {
    match f {
        None => varint(out, -1),
        Some(b) => {
            varint(out, b.len() as i64);
            out.extend_from_slice(b);
        }
    }
}

type Hdr<'a> = (&'a [u8], Option<&'a [u8]>);

/// One record's bytes: `batch::encode` keys headers by name, so it can't repeat one.
fn raw_record(offset: i64, key: Option<&[u8]>, value: &[u8], headers: &[Hdr]) -> Vec<u8> {
    let mut body = vec![0u8];
    varint(&mut body, 0);
    varint(&mut body, offset);
    field(&mut body, key);
    field(&mut body, Some(value));
    varint(&mut body, headers.len() as i64);
    for (n, v) in headers {
        field(&mut body, Some(n));
        field(&mut body, *v);
    }
    let mut out = Vec::new();
    varint(&mut out, body.len() as i64);
    out.extend(body);
    out
}

/// A magic-2, uncompressed, `CreateTime` batch of `count` records at `base_timestamp`.
fn raw_batch(base_timestamp: i64, count: i32, section: &[u8]) -> Bytes {
    let mut b = Vec::new();
    b.extend_from_slice(&0i64.to_be_bytes());
    b.extend_from_slice(&((section.len() + 49) as i32).to_be_bytes());
    b.extend_from_slice(&0i32.to_be_bytes());
    b.push(2);
    b.extend_from_slice(&0u32.to_be_bytes());
    b.extend_from_slice(&0i16.to_be_bytes());
    b.extend_from_slice(&(count - 1).to_be_bytes());
    b.extend_from_slice(&base_timestamp.to_be_bytes());
    b.extend_from_slice(&base_timestamp.to_be_bytes());
    b.extend_from_slice(&(-1i64).to_be_bytes());
    b.extend_from_slice(&(-1i16).to_be_bytes());
    b.extend_from_slice(&(-1i32).to_be_bytes());
    b.extend_from_slice(&count.to_be_bytes());
    b.extend_from_slice(section);
    let crc = crc32c::crc32c(&b[21..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(b)
}

fn attr<'a>(event: &'a Event, key: &str) -> Option<&'a Value> {
    event.attributes.get(key)
}

#[tokio::test]
async fn every_event_carries_its_records_metadata() {
    let cluster = cluster().await;
    let headers: [Hdr; 4] =
        [(b"h", Some(b"a")), (b"x", None), (b"h", Some(b"\xff")), (b"only", Some(b"v"))];
    let section =
        [raw_record(0, Some(b"k"), b"one\ntwo", &headers), raw_record(1, Some(b""), b"three", &[])]
            .concat();
    cluster.state().append_raw("t", 0, raw_batch(1_000, 2, &section), 2);
    cluster.state().append_raw("t", 0, raw_batch(-1, 1, &raw_record(0, None, b"four", &[])), 1);
    let probe = TelemetryProbe::new();
    let (sink, mut rx) = fanout_channel(16);
    let running =
        spawn_input(input(&probe, settings(&cluster, KafkaInFormat::Text { batch: true })), sink)
            .await;
    let got = recv_events(&mut rx, 4).await;
    running.stop().await;

    // Both logs of the first record carry its carriers.
    for e in &got[..2] {
        assert_eq!(attr(e, "_logit.kafka.topic"), Some(&Value::str("t")));
        assert_eq!(attr(e, "_logit.kafka.partition"), Some(&Value::I64(0)));
        assert_eq!(attr(e, "_logit.kafka.offset"), Some(&Value::I64(0)));
        assert_eq!(attr(e, "_logit.kafka.key"), Some(&Value::str("k")));
        assert_eq!(attr(e, "_logit.kafka.timestamp"), Some(&Value::Timestamp(1_000_000_000)));
        assert_eq!(attr(e, "_logit.kafka.timestamp_type"), Some(&Value::str("create_time")));
        let Some(Value::Map(h)) = attr(e, "_logit.kafka.headers") else { panic!("headers") };
        let names: Vec<_> = h.iter().map(|(k, _)| resolve(k)).collect();
        assert_eq!(names, ["h", "x", "only"], "first-appearance order");
        assert_eq!(
            h.get("h"),
            Some(&Value::Array(vec![Value::str("a"), Value::Bytes(Bytes::from_static(b"\xff"))]))
        );
        assert_eq!(h.get("x"), Some(&Value::Null));
        assert_eq!(h.get("only"), Some(&Value::str("v")));
    }
    assert_eq!(got.iter().map(message).collect::<Vec<_>>(), ["one", "two", "three", "four"]);
    // An empty key is an empty string; a record with no headers has no header map.
    assert_eq!(attr(&got[2], "_logit.kafka.key"), Some(&Value::str("")));
    assert_eq!(attr(&got[2], "_logit.kafka.offset"), Some(&Value::I64(1)));
    assert_eq!(attr(&got[2], "_logit.kafka.headers"), None);
    // A null key and a `-1` timestamp are absent.
    assert_eq!(attr(&got[3], "_logit.kafka.key"), None);
    assert_eq!(attr(&got[3], "_logit.kafka.timestamp"), None);
    assert_eq!(attr(&got[3], "_logit.kafka.offset"), Some(&Value::I64(2)));
}

#[tokio::test]
async fn a_header_name_that_is_not_utf8_is_omitted_and_counted() {
    let cluster = cluster().await;
    let headers: [Hdr; 2] = [(b"\xff\xfe", Some(b"v")), (b"ok", Some(b"w"))];
    cluster.state().append_raw(
        "t",
        0,
        raw_batch(1_000, 1, &raw_record(0, None, b"m", &headers)),
        1,
    );
    let mut probe = TelemetryProbe::new();
    let (sink, mut rx) = fanout_channel(16);
    let running =
        spawn_input(input(&probe, settings(&cluster, KafkaInFormat::Text { batch: false })), sink)
            .await;
    let got = recv_events(&mut rx, 1).await;
    running.stop().await;
    let Some(Value::Map(h)) = attr(&got[0], "_logit.kafka.headers") else { panic!("headers") };
    assert_eq!(h.len(), 1);
    assert_eq!(h.get("ok"), Some(&Value::str("w")));
    assert_eq!(probe.sum("logit.input.headers.omitted", &[("reason", "invalid_name")]), 1.0);
    assert_eq!(committed(&cluster), Some(1));
}

// ---- outcomes ---------------------------------------------------------------------------

#[tokio::test]
async fn a_tombstone_an_empty_value_and_an_undecodable_one_are_counted_and_committed_past() {
    let cluster = cluster().await;
    let ok = one_value(vec![log("ok", &[])], RecordFormat::Json);
    append(&cluster, &[None, Some(b""), Some(b"{not json"), Some(&ok)]);
    let mut probe = TelemetryProbe::new();
    let (sink, mut rx) = fanout_channel(16);
    let running = spawn_input(input(&probe, settings(&cluster, KafkaInFormat::Json)), sink).await;
    let got = recv_events(&mut rx, 1).await;
    assert_eq!(message(&got[0]), "ok");
    assert_eq!(attr(&got[0], "_logit.kafka.offset"), Some(&Value::I64(3)));
    running.stop().await;
    assert_eq!(probe.sum("logit.input.records.skipped", &[("reason", "tombstone")]), 1.0);
    assert_eq!(probe.sum("logit.input.records.skipped", &[("reason", "empty")]), 1.0);
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "undecodable")]), 1.0);
    assert!(probe.sum("logit.component.diagnostics", &[("key", "records_dropped")]) >= 1.0);
    assert_eq!(committed(&cluster), Some(4));
}

#[tokio::test]
async fn a_native_value_that_is_not_one_batch_frame_is_undecodable() {
    let cluster = cluster().await;
    let frame = one_value(vec![log("m", &[])], RecordFormat::Native);
    let mut trailing = frame.to_vec();
    trailing.push(0);
    append(&cluster, &[Some(&trailing), Some(b"not a frame"), Some(&frame)]);
    let mut probe = TelemetryProbe::new();
    let (sink, mut rx) = fanout_channel(16);
    let running = spawn_input(input(&probe, settings(&cluster, KafkaInFormat::Native)), sink).await;
    let got = recv_events(&mut rx, 1).await;
    assert_eq!(attr(&got[0], "_logit.kafka.offset"), Some(&Value::I64(2)));
    running.stop().await;
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "undecodable")]), 2.0);
    assert_eq!(committed(&cluster), Some(3));
}

#[tokio::test]
async fn a_batch_over_max_batch_bytes_is_counted_and_committed_past() {
    let cluster = cluster().await;
    let big = vec![b'x'; 4096];
    let records = [record(Some(&big)), record(Some(&big))];
    cluster.state().append("t", 0, &records, Compression::Gzip);
    append(&cluster, &[Some(b"small")]);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, KafkaInFormat::Text { batch: false });
    s.consumer.max_batch_bytes = 1024;
    let (sink, mut rx) = fanout_channel(16);
    let running = spawn_input(input(&probe, s), sink).await;
    let got = recv_events(&mut rx, 1).await;
    assert_eq!(message(&got[0]), "small");
    running.stop().await;
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "oversize")]), 2.0);
    assert_eq!(committed(&cluster), Some(3));
}

#[tokio::test]
async fn batches_are_cut_at_batch_max_events_and_bytes_without_splitting_a_record() {
    let cluster = cluster().await;
    append(&cluster, &[Some(b"a\nb\nc"), Some(b"d"), Some(b"e"), Some(b"0123456789")]);
    let probe = TelemetryProbe::new();
    let mut s = settings(&cluster, KafkaInFormat::Text { batch: true });
    s.batch_max_events = 2;
    s.batch_max_bytes = 8;
    let (sink, mut rx) = fanout_channel(16);
    let running = spawn_input(input(&probe, s), sink).await;
    let mut sizes = Vec::new();
    while sizes.iter().sum::<usize>() < 6 {
        sizes.push(recv_batch(&mut rx).await.events.len());
    }
    running.stop().await;
    // A record of three lines over `batch_max_events` goes alone; the oversize value too.
    assert_eq!(sizes, [3, 2, 1]);
}

#[tokio::test]
async fn a_revoke_whose_drain_deadline_passes_defers_and_commits_nothing_past_it() {
    let cluster = cluster().await;
    append(&cluster, &[Some(b"a"), Some(b"b")]);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, KafkaInFormat::Text { batch: false });
    s.batch_max_events = 1;
    s.drain_timeout = Duration::from_millis(200);
    // A channel of one: `a` fills it, and `b` waits for room the test doesn't give.
    let (sink, mut rx) = fanout_channel(1);
    let running = spawn_input(input(&probe, s), sink).await;
    wait_until("a delivered", || rx.len() == 1).await;
    cluster.script(Step::api(ApiKey::Heartbeat, Action::Error(code::REBALANCE_IN_PROGRESS)));
    probe.wait_for("b deferred", |t| t.sum("logit.input.batches.deferred", &[]) >= 1.0).await;
    wait_until("the revoke's commit", || committed(&cluster) == Some(1)).await;
    assert_eq!(probe.sum("logit.input.batches.deferred", &[]), 1.0);
    // After the rejoin, `b` is read again from the committed offset and delivered once there's
    // room.
    let got = recv_events(&mut rx, 2).await;
    assert_eq!(got.iter().map(message).collect::<Vec<_>>(), ["a", "b"]);
    running.stop().await;
    assert_eq!(committed(&cluster), Some(2));
}

#[tokio::test]
async fn a_closed_consumer_stops_commits_the_batch_before_and_leaves() {
    let cluster = cluster().await;
    append(&cluster, &[Some(b"a"), Some(b"b"), Some(b"c")]);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, KafkaInFormat::Text { batch: false });
    s.batch_max_events = 1;
    let (sink, rx) = fanout_channel(1);
    let running = spawn_input(input(&probe, s), sink).await;
    wait_until("a delivered", || rx.len() == 1).await;
    wait_until("a member", || members(&cluster) == 1).await;
    drop(rx);
    tokio::time::timeout(RECV_TIMEOUT, running.handle)
        .await
        .expect("the input ends on its own")
        .expect("no panic")
        .expect("no error");
    assert_eq!(probe.sum("logit.input.batches.dropped", &[("reason", "closed_consumer")]), 1.0);
    assert_eq!(probe.sum("logit.input.batches.deferred", &[]), 1.0);
    assert!(probe.sum("logit.component.diagnostics", &[("key", "closed_consumer")]) >= 1.0);
    assert_eq!(committed(&cluster), Some(1));
    assert_eq!(members(&cluster), 0, "the input left the group");
}

#[tokio::test]
async fn shutdown_commits_and_leaves_within_shutdown_grace() {
    let cluster = cluster().await;
    append(&cluster, &[Some(b"a"), Some(b"b")]);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, KafkaInFormat::Text { batch: false });
    s.batch_max_events = 1;
    s.shutdown_grace = Duration::from_secs(2);
    let (sink, rx) = fanout_channel(1);
    let running = spawn_input(input(&probe, s), sink).await;
    wait_until("a delivered", || rx.len() == 1).await;
    wait_until("a member", || members(&cluster) == 1).await;
    let began = Instant::now();
    running.stop().await;
    // `b` waits half the 2 s grace, 1 s, then the commit and LeaveGroup on loopback take
    // milliseconds: the 3 s bound leaves 2 s of slack for a loaded runner, and still fails a
    // drain that took `drain_timeout`'s 5 s.
    assert!(began.elapsed() < Duration::from_secs(3), "{:?}", began.elapsed());
    // `b` waited half the grace, then was deferred; `a` is committed and the group left.
    assert_eq!(probe.sum("logit.input.batches.deferred", &[]), 1.0);
    assert_eq!(committed(&cluster), Some(1));
    assert_eq!(members(&cluster), 0);
    drop(rx);
}

#[tokio::test]
async fn the_periodic_commit_lands_within_commit_interval() {
    let cluster = cluster().await;
    append(&cluster, &[Some(b"a")]);
    let probe = TelemetryProbe::new();
    let mut s = settings(&cluster, KafkaInFormat::Text { batch: false });
    s.consumer.commit_interval = Duration::from_millis(300);
    let (sink, mut rx) = fanout_channel(16);
    let running = spawn_input(input(&probe, s), sink).await;
    recv_batch(&mut rx).await;
    let delivered = Instant::now();
    wait_until("the periodic commit", || committed(&cluster) == Some(1)).await;
    // The commit is due one 300 ms interval after the last, so at most that after delivery,
    // plus a fetch's 50 ms `max_wait` before `next` runs again: 350 ms. The 2 s bound leaves over
    // 1.6 s for scheduling on a loaded runner and still fails a commit left to shutdown, which
    // `running.stop()` below would make only after the wait gave up at 5 s.
    assert!(delivered.elapsed() < Duration::from_secs(2), "{:?}", delivered.elapsed());
    running.stop().await;
}

#[tokio::test]
async fn shutdown_during_a_revoke_drain_caps_it_at_half_the_grace() {
    let cluster = cluster().await;
    append(&cluster, &[Some(b"a"), Some(b"b")]);
    let mut probe = TelemetryProbe::new();
    let mut s = settings(&cluster, KafkaInFormat::Text { batch: false });
    s.batch_max_events = 1;
    s.drain_timeout = Duration::from_secs(4);
    s.shutdown_grace = Duration::from_secs(1);
    // A channel of one: `a` fills it, and `b` waits for room the test doesn't give.
    let (sink, rx) = fanout_channel(1);
    let running = spawn_input(input(&probe, s), sink).await;
    wait_until("a delivered", || rx.len() == 1).await;
    wait_until("a member", || members(&cluster) == 1).await;
    cluster.script(Step::api(ApiKey::Heartbeat, Action::Error(code::REBALANCE_IN_PROGRESS)));
    probe
        .wait_for("the revoke raised", |t| {
            t.sum(
                "logit.input.broker.errors",
                &[("api", "heartbeat"), ("error", "rebalance_in_progress")],
            ) >= 1.0
        })
        .await;
    // Three of the input's 250 ms send slices, so `b`'s next attempt sees the revoke and starts
    // its 4 s drain before the shutdown below. Too short a wait only lets shutdown be seen
    // first, which this test then passes without covering the revoke-first order.
    tokio::time::sleep(Duration::from_millis(750)).await;
    let began = Instant::now();
    running.stop().await;
    // Shutdown is seen within one 250 ms slice and caps the drain at half the 1 s grace, then
    // the commit and LeaveGroup on loopback take milliseconds: 750 ms of timers. The 2 s bound
    // leaves 1.25 s of slack and fails the revoke's uncapped 4 s drain.
    assert!(began.elapsed() < Duration::from_secs(2), "{:?}", began.elapsed());
    assert_eq!(probe.sum("logit.input.batches.deferred", &[]), 1.0);
    assert_eq!(committed(&cluster), Some(1));
    assert_eq!(members(&cluster), 0, "the input left the group");
    drop(rx);
}

#[tokio::test]
async fn a_blocked_batch_keeps_its_place_ahead_of_a_later_sender() {
    let cluster = cluster().await;
    append(&cluster, &[Some(b"a")]);
    let mut probe = TelemetryProbe::new();
    // One inbox of one slot, fed by `kafka_in` and by a second producer. Each producer's edge
    // records `inbox.full` under its own handle, so the test can tell whose wait began.
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let edge =
        |id: &str| Edge::new(tx.clone()).with_telemetry(probe.telemetry(id, "stdio_out", "sink"));
    let kafka_edge = Fanout::from_edges(vec![edge("out_kafka")]);
    let other = Fanout::from_edges(vec![edge("out_other")]);
    let full = |t: &logit_pipeline::test_util::Totals, id: &str| {
        t.sum("logit.component.inbox.full", &[("component", id)])
    };
    let filler = || EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: vec![log("other", &[])],
    };
    assert!(other.send(filler()).await, "the filler takes the one slot");
    let running = spawn_input(
        input(&probe, settings(&cluster, KafkaInFormat::Text { batch: false })),
        kafka_edge,
    )
    .await;
    probe.wait_for("kafka_in parked", |t| full(t, "out_kafka") >= 1.0).await;
    let later = tokio::spawn(async move { other.send(filler()).await });
    probe.wait_for("the later sender parked", |t| full(t, "out_other") >= 1.0).await;
    // A negative window: three of `kafka_in`'s 250 ms send slices, in which it must not leave
    // the line and queue again (which would count another `inbox.full`).
    tokio::time::sleep(Duration::from_millis(750)).await;
    assert_eq!(probe.sum("logit.component.inbox.full", &[("component", "out_kafka")]), 1.0);
    // Freeing the slot hands it to the oldest waiter, `kafka_in`.
    assert_eq!(message(&recv_batch(&mut rx).await.events[0]), "other");
    assert_eq!(message(&recv_batch(&mut rx).await.events[0]), "a");
    assert_eq!(message(&recv_batch(&mut rx).await.events[0]), "other");
    assert!(later.await.unwrap());
    running.stop().await;
}
