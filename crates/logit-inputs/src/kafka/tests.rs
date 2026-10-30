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
use logit_proto::event_json::EventJsonDecoder;
use logit_proto::kafka::batch::ProducerRecord;
use logit_proto::{FramedEncoder, MessageBuf, Signal};
use tokio::time::Instant;

use super::decode::{Decoded, RecordDecoder};
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
        (RecordFormat::Msgpack, RecordMode::Event, KafkaInFormat::Msgpack),
        (RecordFormat::Msgpack, RecordMode::Batch, KafkaInFormat::Msgpack),
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
        let running = spawn_input(input(&probe, settings(&cluster, in_format.clone())), sink).await;
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
    assert!(began.elapsed() < Duration::from_secs(2), "{:?}", began.elapsed());
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
    // One interval, plus a fetch's `max_wait` before `next` runs again, plus scheduling slack.
    assert!(delivered.elapsed() < Duration::from_secs(1), "{:?}", delivered.elapsed());
    running.stop().await;
}

// ---- msgpack -----------------------------------------------------------------------------

/// `batches` as the lossless `format: json` lines, one per event, resource and scope included.
/// `kafka_out` leaves the `_logit.kafka.*` carriers out.
fn render(batches: &[EventBatch]) -> Vec<String> {
    let mut encoder =
        KafkaEncoder::new(&RecordKey::None, RecordFormat::Json, RecordMode::Event, usize::MAX);
    let mut out: MessageBuf<RecordMeta> = MessageBuf::default();
    let mut lines = Vec::new();
    for batch in batches {
        encoder.encode_into(batch, &mut out);
        lines.extend(out.iter_with().map(|(v, _)| String::from_utf8(v.to_vec()).expect("utf-8")));
    }
    lines
}

/// `value` as a record at `offset`.
fn kafka_record(offset: i64, value: Bytes) -> logit_proto::kafka::Record {
    logit_proto::kafka::Record {
        offset,
        timestamp: 1_000,
        timestamp_type: logit_proto::kafka::TimestampType::CreateTime,
        key: None,
        value: Some(value),
        headers: Vec::new(),
    }
}

/// Every record's batches through a `RecordDecoder` of `format`, each record's on `topic_of` its
/// meta, in record order.
fn read_back(
    format: KafkaInFormat,
    records: &[(Bytes, RecordMeta)],
    topic_of: impl Fn(&RecordMeta) -> &'static str,
) -> Vec<EventBatch> {
    let mut decoder = RecordDecoder::new(format);
    let mut batches = Vec::new();
    for (i, (value, meta)) in records.iter().enumerate() {
        let record = kafka_record(i as i64, value.clone());
        match decoder.decode(topic_of(meta), 0, &record, 0).0 {
            Decoded::Batches(b) => batches.extend(b),
            other => panic!("record {i}: {other:?}"),
        }
    }
    batches
}

fn encode_records(
    batch: &EventBatch,
    key: &RecordKey,
    format: RecordFormat,
    mode: RecordMode,
    budget: usize,
) -> Vec<(Bytes, RecordMeta)> {
    let mut encoder = KafkaEncoder::new(key, format, mode, budget);
    let mut out: MessageBuf<RecordMeta> = MessageBuf::default();
    encoder.encode_into(batch, &mut out);
    out.iter_with().map(|(v, meta)| (Bytes::copy_from_slice(v), meta.clone())).collect()
}

/// Every transit case's events, one entry per case file, as `format: json` reads them.
fn transit_cases(dirs: &[&str]) -> Vec<(String, Vec<EventBatch>)> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../spec/transit/cases");
    let mut cases = Vec::new();
    for dir in std::fs::read_dir(&root).expect("the transit cases") {
        let dir = dir.unwrap().path();
        let name = dir.file_name().unwrap().to_string_lossy().into_owned();
        if !dirs.is_empty() && !dirs.contains(&name.as_str()) {
            continue;
        }
        let mut files: Vec<_> =
            std::fs::read_dir(&dir).unwrap().map(|f| f.unwrap().path()).collect();
        files.sort();
        for file in files.into_iter().filter(|f| f.extension().is_some_and(|e| e == "json")) {
            let bytes = std::fs::read(&file).unwrap();
            let batches = EventJsonDecoder::new()
                .decode_batches(&bytes)
                .unwrap_or_else(|e| panic!("{}: {e}", file.display()));
            cases.push((file.display().to_string(), batches));
        }
    }
    cases
}

/// A batch the transit cases leave out: every `Value` variant nested, non-finite floats in a
/// value and a metric, a `Bytes` message, and a span with its extension, events, and links.
fn shapes_batch() -> EventBatch {
    let mut attrs = AttrMap::new();
    attrs.insert("v.bytes", Value::Bytes(Bytes::from_static(b"\xff\\\x00")));
    attrs.insert("v.nan", Value::F64(f64::NAN));
    attrs.insert("v.inf", Value::F64(f64::NEG_INFINITY));
    attrs.insert("v.u64", Value::U64(u64::MAX));
    attrs.insert("v.u64small", Value::U64(7));
    attrs.insert("v.ts", Value::Timestamp(-1));
    let mut inner = AttrMap::new();
    inner
        .insert("deep", Value::Array(vec![Value::Bytes(Bytes::from_static(b"\x01")), Value::Null]));
    attrs.insert("v.nested", Value::Map(Box::new(inner)));
    let mut log = log("x", &[]);
    log.attributes = attrs;
    log.log.as_mut().unwrap().message = Value::Bytes(Bytes::from_static(b"\xfe body"));
    let mut metric = Event::empty(5, AttrMap::new());
    metric.metrics.push(logit_core::MetricRecord::new(
        logit_core::interner::intern("m.inf"),
        logit_core::MetricKind::gauge(f64::INFINITY),
    ));
    let span = logit_core::SpanRecord {
        trace_id: [0xab; 16],
        span_id: [0xcd; 8],
        parent_span_id: Some([0xef; 8]),
        name: Value::Bytes(Bytes::from_static(b"\xff")),
        kind: logit_core::SpanKind::Server,
        status: logit_core::SpanStatus::Error,
        events: vec![logit_core::SpanEvent {
            timestamp: 1_200,
            name: Value::str("retry"),
            attributes: AttrMap::new(),
            dropped_attributes_count: 4,
        }],
        links: vec![logit_core::SpanLink {
            trace_id: [1; 16],
            span_id: [2; 8],
            attributes: AttrMap::new(),
            flags: 1,
            trace_state: Some(Bytes::from_static(b"a=b")),
            dropped_attributes_count: 5,
        }],
        end_timestamp: 500,
        flags: 0x301,
        ext: Some(Box::new(logit_core::SpanExt {
            status_message: Some(Bytes::from_static(b"boom\xfe")),
            trace_state: None,
            dropped_attributes_count: 1,
            dropped_events_count: 0,
            dropped_links_count: 0,
        })),
    };
    let span = Event::span(1_000, AttrMap::new(), span);
    let scope = logit_core::Scope {
        name: Bytes::from_static(b"io.logit\xff"),
        version: Bytes::from_static(b"1"),
        attributes: AttrMap::new(),
        dropped_attributes_count: 2,
        schema_url: None,
    };
    EventBatch {
        resource: Arc::new(Resource::default()),
        scope: Some(Arc::new(scope)),
        events: vec![log, metric, span],
    }
}

/// Every transit case, and the shapes they leave out, through `kafka_out`'s `msgpack` under
/// both `record:` modes and back through the reader, equal as their lossless JSON lines.
#[test]
fn msgpack_reads_back_every_transit_case_under_both_record_modes() {
    let mut cases = transit_cases(&[]);
    assert!(cases.len() > 50, "found {} case files", cases.len());
    cases.push(("shapes".to_string(), vec![shapes_batch()]));
    for (name, batches) in cases {
        let want = render(&batches);
        for mode in [RecordMode::Event, RecordMode::Batch] {
            let mut got = Vec::new();
            for batch in &batches {
                let records =
                    encode_records(batch, &RecordKey::None, RecordFormat::Msgpack, mode, 64 << 20);
                got.extend(read_back(KafkaInFormat::Msgpack, &records, |_| "t"));
            }
            assert_eq!(render(&got), want, "{name} under {mode:?}");
        }
    }
}

#[tokio::test]
async fn a_msgpack_value_that_does_not_decode_is_dropped_and_committed_past() {
    let cluster = cluster().await;
    let ok = one_value(vec![log("ok", &[])], RecordFormat::Msgpack);
    let truncated = ok.slice(..ok.len() - 1);
    append(&cluster, &[Some(&truncated), Some(&ok)]);
    let mut probe = TelemetryProbe::new();
    let (sink, mut rx) = fanout_channel(16);
    let running =
        spawn_input(input(&probe, settings(&cluster, KafkaInFormat::Msgpack)), sink).await;
    let got = recv_events(&mut rx, 1).await;
    assert_eq!(message(&got[0]), "ok");
    assert_eq!(attr(&got[0], "_logit.kafka.offset"), Some(&Value::I64(1)));
    running.stop().await;
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "undecodable")]), 1.0);
    assert_eq!(committed(&cluster), Some(2));
}

// ---- OTLP --------------------------------------------------------------------------------

const OTLP_TOPICS: [(&str, Signal); 3] =
    [("logs", Signal::Logs), ("metrics", Signal::Metrics), ("spans", Signal::Traces)];

fn otlp_format(json: bool) -> KafkaInFormat {
    let topics = OTLP_TOPICS.iter().map(|&(t, s)| (t.to_string(), s)).collect();
    KafkaInFormat::Otlp { json, topics }
}

fn topic_of(meta: &RecordMeta) -> &'static str {
    let signal = meta.signal.expect("an OTLP record names its signal");
    OTLP_TOPICS.iter().find(|(_, s)| *s == signal).unwrap().0
}

/// The OTLP transit cases through `kafka_out`'s `otlp_proto` and `otlp_json`, whole and split by
/// `key: trace_id` and by a small `max_request_bytes`, and back through the reader: every split
/// reads back the events the whole request does, each signal from its own topic.
#[test]
fn otlp_reads_back_kafka_outs_requests_whole_and_split() {
    let mut split = [[false; 2]; 2];
    for (name, mut batches) in transit_cases(&["otlp", "otlp-logs", "otlp-traces"]) {
        // The OTLP encoder stamps an unset observed time with the clock, so each encode would
        // differ.
        for event in batches.iter_mut().flat_map(|b| &mut b.events) {
            if let Some(log) = &mut event.log {
                log.observed_timestamp = log.observed_timestamp.max(1);
            }
        }
        for (j, (out_format, json)) in
            [(RecordFormat::OtlpProto, false), (RecordFormat::OtlpJson, true)]
                .into_iter()
                .enumerate()
        {
            let read = |key: RecordKey, budget: usize| {
                let mut lines = Vec::new();
                let mut records = 0;
                for batch in &batches {
                    let r = encode_records(batch, &key, out_format, RecordMode::Request, budget);
                    records += r.len();
                    lines.extend(render(&read_back(otlp_format(json), &r, topic_of)));
                }
                lines.sort();
                (lines, records)
            };
            let (want, whole) = read(RecordKey::None, usize::MAX);
            assert!(!want.is_empty(), "{name}");
            // The largest request of one event, plus a trace id key: every event fits alone,
            // and a request of several splits.
            let one = batches
                .iter()
                .flat_map(|b| {
                    b.events.iter().map(|e| EventBatch {
                        resource: b.resource.clone(),
                        scope: b.scope.clone(),
                        events: vec![e.clone()],
                    })
                })
                .flat_map(|b| {
                    let r = encode_records(
                        &b,
                        &RecordKey::None,
                        out_format,
                        RecordMode::Request,
                        usize::MAX,
                    );
                    r.into_iter().map(|(v, _)| v.len())
                })
                .max()
                .unwrap();
            let splits = [(RecordKey::TraceId, usize::MAX), (RecordKey::None, one + 32)];
            for (k, (key, budget)) in splits.into_iter().enumerate() {
                let (got, records) = read(key.clone(), budget);
                split[k][j] |= records > whole;
                assert_eq!(got, want, "{name} as {out_format:?} under {key:?} and {budget}");
            }
        }
    }
    assert_eq!(split, [[true; 2]; 2], "some request split by trace id, and some by size");
}

/// A cluster with the three OTLP topics, one partition each.
async fn otlp_cluster() -> FakeCluster {
    let cluster = FakeCluster::start(1).await;
    for (topic, _) in OTLP_TOPICS {
        cluster.state().add_topic(topic, &[0]);
    }
    cluster
}

fn otlp_settings(cluster: &FakeCluster, json: bool) -> KafkaInSettings {
    let mut s = settings(cluster, otlp_format(json));
    s.consumer.topics = OTLP_TOPICS.iter().map(|(t, _)| t.to_string()).collect();
    s
}

fn committed_on(cluster: &FakeCluster, topic: &str) -> Option<i64> {
    cluster.state().groups.get(GROUP)?.committed.get(&(topic.to_string(), 0)).copied()
}

/// A log-only batch whose resource names `host`.
fn host_batch(host: &str) -> EventBatch {
    let mut resource = Resource::default();
    resource.attributes.insert("host.name", Value::str(host));
    EventBatch { resource: Arc::new(resource), scope: None, events: vec![log(host, &[])] }
}

/// One `otlp_proto` logs request with a resource per host: protobuf concatenates repeated
/// fields, so two requests back to back are one request holding both.
fn multi_resource_request(hosts: &[&str]) -> Bytes {
    let mut value = Vec::new();
    for host in hosts {
        let records = encode_records(
            &host_batch(host),
            &RecordKey::None,
            RecordFormat::OtlpProto,
            RecordMode::Request,
            usize::MAX,
        );
        value.extend_from_slice(&records[0].0);
    }
    Bytes::from(value)
}

fn host(batch: &EventBatch) -> Value {
    batch.resource.attributes.get("host.name").cloned().expect("a host")
}

#[tokio::test]
async fn each_signal_reads_from_its_own_topic_through_the_input() {
    for json in [false, true] {
        let cluster = otlp_cluster().await;
        let batch = &transit_cases(&["otlp-traces"])
            .into_iter()
            .find(|(n, _)| n.ends_with("beside.json"))
            .expect("the beside case")
            .1[0];
        let format = if json { RecordFormat::OtlpJson } else { RecordFormat::OtlpProto };
        let records = encode_records(batch, &RecordKey::None, format, RecordMode::Request, 1 << 20);
        let want = render(&read_back(otlp_format(json), &records, topic_of));
        for (value, meta) in &records {
            let record = ProducerRecord {
                key: None,
                value: Some(value.clone()),
                headers: Default::default(),
                timestamp: 1_000,
            };
            cluster.state().append(topic_of(meta), 0, &[record], Compression::None);
        }
        let probe = TelemetryProbe::new();
        let (sink, mut rx) = fanout_channel(16);
        let running = spawn_input(input(&probe, otlp_settings(&cluster, json)), sink).await;
        let mut got = Vec::new();
        while got.len() < want.len() {
            got.extend(render(&[recv_batch(&mut rx).await]));
        }
        running.stop().await;
        for topic in ["logs", "metrics", "spans"] {
            assert_eq!(committed_on(&cluster, topic), Some(1), "{topic}");
        }
        got.sort();
        let mut want = want;
        want.sort();
        assert_eq!(got, want, "json: {json}");
    }
}

/// A request of two resources is two batches, and its record is marked only after the second
/// is accepted: a revoke that defers the second reads the whole record again, the first batch
/// included.
#[tokio::test]
async fn a_multi_resource_request_is_marked_only_after_its_last_batch() {
    let cluster = otlp_cluster().await;
    let value = multi_resource_request(&["a", "b"]);
    let record = ProducerRecord {
        key: None,
        value: Some(value),
        headers: Default::default(),
        timestamp: 1_000,
    };
    cluster.state().append("logs", 0, &[record], Compression::None);
    let mut probe = TelemetryProbe::new();
    let mut s = otlp_settings(&cluster, false);
    s.drain_timeout = Duration::from_millis(200);
    // A channel of one: `a` fills it, and `b` waits for room the test doesn't give.
    let (sink, mut rx) = fanout_channel(1);
    let running = spawn_input(input(&probe, s), sink).await;
    wait_until("a delivered", || rx.len() == 1).await;
    cluster.script(Step::api(ApiKey::Heartbeat, Action::Error(code::REBALANCE_IN_PROGRESS)));
    probe.wait_for("b deferred", |t| t.sum("logit.input.batches.deferred", &[]) >= 1.0).await;
    let hosts: Vec<Value> =
        [recv_batch(&mut rx).await, recv_batch(&mut rx).await, recv_batch(&mut rx).await]
            .iter()
            .map(host)
            .collect();
    assert_eq!(hosts, [Value::str("a"), Value::str("a"), Value::str("b")]);
    running.stop().await;
    assert_eq!(committed_on(&cluster, "logs"), Some(1));
}

#[tokio::test]
async fn an_otlp_request_on_another_signals_topic_is_undecodable() {
    let cluster = otlp_cluster().await;
    let logs = multi_resource_request(&["a"]);
    let on = |topic: &str, value: &Bytes| {
        let record = ProducerRecord {
            key: None,
            value: Some(value.clone()),
            headers: Default::default(),
            timestamp: 1_000,
        };
        cluster.state().append(topic, 0, &[record], Compression::None);
    };
    on("metrics", &logs);
    on("logs", &logs);
    let mut probe = TelemetryProbe::new();
    let (sink, mut rx) = fanout_channel(16);
    let running = spawn_input(input(&probe, otlp_settings(&cluster, false)), sink).await;
    let got = recv_batch(&mut rx).await;
    assert_eq!(host(&got), Value::str("a"));
    probe
        .wait_for("the mismatch dropped", |t| {
            t.sum("logit.input.records.dropped", &[("reason", "undecodable")]) >= 1.0
        })
        .await;
    running.stop().await;
    assert_eq!(probe.sum("logit.input.records.dropped", &[("reason", "undecodable")]), 1.0);
    assert_eq!(committed_on(&cluster, "metrics"), Some(1));
    assert_eq!(committed_on(&cluster, "logs"), Some(1));
}

#[tokio::test]
async fn an_otlp_request_with_no_records_is_skipped_empty() {
    let cluster = otlp_cluster().await;
    // An empty `LogsData` is zero bytes in protobuf and `{}` in OTLP/JSON.
    let record = |value: &'static [u8]| ProducerRecord {
        key: None,
        value: Some(Bytes::from_static(value)),
        headers: Default::default(),
        timestamp: 1_000,
    };
    for json in [false, true] {
        let value: &'static [u8] = if json { b"{}" } else { b"" };
        cluster.state().append("logs", 0, &[record(value)], Compression::None);
        let mut probe = TelemetryProbe::new();
        let (sink, _rx) = fanout_channel(16);
        let running = spawn_input(input(&probe, otlp_settings(&cluster, json)), sink).await;
        probe
            .wait_for("skipped", |t| {
                t.sum("logit.input.records.skipped", &[("reason", "empty")]) >= 1.0
            })
            .await;
        running.stop().await;
        let offset = if json { 2 } else { 1 };
        assert_eq!(committed_on(&cluster, "logs"), Some(offset), "json: {json}");
    }
}
