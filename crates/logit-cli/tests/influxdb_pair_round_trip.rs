//! `influxdb_out -> influxdb_in` over a real socket: the sink's `url` points at a listener on
//! `127.0.0.1:0`, and each batch arrives as sent.
//!
//! Each seed is one decode of a hand-written line-protocol body under the resource the listener
//! builds from the sink's `org` and `bucket`, so it sits on the codec's fixed point
//! (`crates/logit-proto/tests/influxdb_fixed_point.rs`), and `influxdb_out` re-encoding it gives
//! the same batch back through `influxdb_in`. Whole-`EventBatch` equality is the right check, not a
//! looser one: normalization 3 (tag order) and 4 (string fields after numeric ones) are wire-only,
//! invisible in the model, whose attributes are a sorted map and whose string fields are
//! attributes. The numbered list is `logit_proto::influxdb`'s module doc, "Permitted
//! normalizations". What this file adds over the codec tests is the HTTP hop: the write route, the
//! query parameters, the token, and the response classes the sink reads.

use bytes::Bytes;
use logit_core::{Event, EventBatch, MetricKind, MetricRecord, MetricValue, Resource, Value};
use logit_inputs::influxdb::InfluxDbInput;
use logit_outputs::influxdb::{Destination, InfluxDbOutput, Organization};
use logit_pipeline::test_util::{assert_no_batch, recv_batch, TelemetryProbe};
use logit_pipeline::{classify, Delivered, Fanout, Fault, Input, Output};
use logit_proto::influxdb::{
    InfluxDecoder, Precision, ATTR_MEASUREMENT, RESOURCE_ATTR_BUCKET, RESOURCE_ATTR_ORG,
};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

const TOKEN: &str = "logit-pair-token";
const ORG: &str = "logit";
const BUCKET: &str = "metrics";

/// Every seed line carries a timestamp, so nothing decodes to this.
const RECEIVED_AT: i64 = 1_699_000_000_000_000_000;

/// `influxdb_in` delivers a request's batch before it answers and the sink's `send` returns only
/// after the answer, so nothing a send caused is still in flight; the window is a margin against a
/// stray later batch, not a delivery interval.
const NOTHING_MORE: Duration = Duration::from_millis(300);

async fn listener(probe: &TelemetryProbe) -> (SocketAddr, mpsc::Receiver<Delivered>) {
    let mut input = InfluxDbInput::new("127.0.0.1:0")
        .with_tokens(vec![TOKEN.to_string()])
        .with_telemetry(probe.telemetry("in", "influxdb_in", "listener"));
    input.bind().await.expect("binding influxdb_in");
    let addr = input.local_addr().expect("bind() leaves an address");
    let (tx, rx) = mpsc::channel(64);
    tokio::spawn(async move {
        let _ = input.run(Fanout::new(vec![tx])).await;
    });
    (addr, rx)
}

/// An `influxdb_out` pointed at `addr`, writing to `org` and `bucket` with `token`.
fn sink(
    addr: SocketAddr,
    probe: &TelemetryProbe,
    org: &str,
    bucket: &str,
    token: &str,
) -> InfluxDbOutput {
    InfluxDbOutput::new(
        format!("http://{addr}"),
        Organization::Fixed(org.into()),
        Destination::Fixed(bucket.into()),
        token.into(),
    )
    .with_telemetry(probe.telemetry("out", "influxdb_out", "sink"))
}

/// The resource `influxdb_in` builds for a write naming [`ORG`] and [`BUCKET`].
fn resource() -> Arc<Resource> {
    let mut resource = Resource::default();
    resource.attributes.insert(RESOURCE_ATTR_ORG, Value::from(ORG));
    resource.attributes.insert(RESOURCE_ATTR_BUCKET, Value::from(BUCKET));
    Arc::new(resource)
}

/// The one batch `body` decodes to under [`resource`].
fn seed(body: &str) -> EventBatch {
    InfluxDecoder::new()
        .decode_body(
            Bytes::copy_from_slice(body.as_bytes()),
            Precision::Ns,
            RECEIVED_AT,
            resource(),
        )
        .unwrap_or_else(|err| panic!("the seed decodes: {err}"))
}

/// Every `MetricValue` variant on one point, `-0` and `u64::MAX` among them, beside a string
/// field, tags written out of order, and a second series.
fn typed() -> EventBatch {
    seed(concat!(
        "cpu,host=web-1,cpu=cpu0 usage_user=1.5,neg=-0,count=-3i,big=18446744073709551615u,",
        r#"up=true,down=F,note="a \"quoted\", spaced = string \\ done" 1700000000000000000"#,
        "\n",
        "mem,host=web-1 used_percent=42.25,available=1024i 1700000000000000000\n",
    ))
}

/// Every escapable character in a measurement, tag key, tag value, and field key, and a literal
/// backslash before a letter, which no escape touches (probe row 1).
fn escaped() -> EventBatch {
    seed(concat!(
        r"disk\ io\,x=y,tag\ key\,a\=b=v\ 1\,2\=3,path=C:\temp,z=first\x ",
        r#"field\ key\,c\=d=7i,s="one\ttwo" 1700000000000000001"#,
        "\n",
    ))
}

/// The value of `name` on the batch's one event that carries it.
fn gauge(batch: &EventBatch, name: &str) -> MetricValue {
    let name = logit_core::interner::intern(name);
    batch
        .events
        .iter()
        .flat_map(|e| e.metrics.iter())
        .find(|m| m.name == name)
        .map(|m| match m.kind {
            MetricKind::Gauge(v) => v,
            ref other => panic!("not a gauge: {other:?}"),
        })
        .unwrap_or_else(|| panic!("no metric {name:?}"))
}

/// The seeds carry what they claim, so the relay test covers each.
#[test]
fn the_seeds_cover_every_metric_value_a_string_field_and_escaping() {
    let typed = typed();
    assert_eq!(typed.events.len(), 2);
    assert_eq!(gauge(&typed, "cpu.usage_user"), MetricValue::F64(1.5));
    assert!(
        matches!(gauge(&typed, "cpu.neg"), MetricValue::F64(v) if v.to_bits() == (-0.0f64).to_bits())
    );
    assert_eq!(gauge(&typed, "cpu.count"), MetricValue::I64(-3));
    assert_eq!(gauge(&typed, "cpu.big"), MetricValue::U64(u64::MAX));
    assert_eq!(gauge(&typed, "cpu.up"), MetricValue::Bool(true));
    assert_eq!(gauge(&typed, "cpu.down"), MetricValue::Bool(false));
    let cpu = &typed.events[0].attributes;
    assert_eq!(
        cpu.get("influxdb.field.note").and_then(Value::as_str),
        Some(r#"a "quoted", spaced = string \ done"#)
    );

    let escaped = escaped();
    let attrs = &escaped.events[0].attributes;
    assert_eq!(attrs.get(ATTR_MEASUREMENT).and_then(Value::as_str), Some("disk io,x=y"));
    assert_eq!(attrs.get("tag key,a=b").and_then(Value::as_str), Some("v 1,2=3"));
    assert_eq!(attrs.get("path").and_then(Value::as_str), Some(r"C:\temp"));
    assert_eq!(attrs.get("z").and_then(Value::as_str), Some(r"first\x"));
    assert_eq!(gauge(&escaped, "disk io,x=y.field key,c=d"), MetricValue::I64(7));
    assert_eq!(attrs.get("influxdb.field.s").and_then(Value::as_str), Some(r"one\ttwo"));
}

/// Every seed relays unchanged, whole-`EventBatch` equal, and each write is counted `ok` on both
/// ends.
#[tokio::test]
async fn every_seed_relays_its_batch_unchanged() {
    let mut probe = TelemetryProbe::new();
    let (addr, mut rx) = listener(&probe).await;
    let mut out = sink(addr, &probe, ORG, BUCKET, TOKEN);
    let seeds = [("typed", typed()), ("escaped", escaped())];
    for (name, batch) in &seeds {
        out.send(batch).await.unwrap_or_else(|err| panic!("{name}: {err:#}"));
        let delivered = recv_batch(&mut rx).await;
        assert_eq!(&delivered, batch, "{name}");
        // `PartialEq` on `f64` can't tell `-0.0` from `0.0`.
        if *name == "typed" {
            assert!(
                matches!(gauge(&delivered, "cpu.neg"), MetricValue::F64(v) if v.is_sign_negative())
            );
        }
    }
    assert_no_batch(&mut rx, NOTHING_MORE, "nothing more reaches influxdb_in").await;

    let sent = seeds.len() as f64;
    let totals = probe.poll();
    assert_eq!(totals.sum("logit.input.requests", &[("route", "write"), ("class", "ok")]), sent);
    assert_eq!(totals.sum("logit.input.requests.rejected", &[]), 0.0);
    assert_eq!(totals.sum("logit.output.requests", &[("class", "2xx")]), sent);
    // `influxdb_out`'s unit tests show each of these counted nonzero, so a zero here isn't a
    // name nothing emits.
    assert_eq!(totals.sum("logit.output.tags.omitted", &[]), 0.0);
    assert_eq!(totals.sum("logit.output.metrics.dropped", &[]), 0.0);
    assert_eq!(totals.sum("logit.output.metrics.skipped", &[]), 0.0);
}

/// An event in another protocol's shape, a `Sum` with no measurement carrier, takes the
/// per-record path and arrives as the shape a decode makes: measurement `requests`, field `value`,
/// and a `Gauge` (normalization 10 drops the temporality).
#[tokio::test]
async fn a_per_record_event_arrives_in_the_decoded_shape() {
    let probe = TelemetryProbe::new();
    let (addr, mut rx) = listener(&probe).await;
    let mut out = sink(addr, &probe, ORG, BUCKET, TOKEN);

    let mut attributes = logit_core::AttrMap::new();
    attributes.insert("host", Value::from("web-1"));
    let mut event = Event::empty(1_700_000_000_000_000_000, attributes);
    event.metrics.push(MetricRecord::new(
        logit_core::interner::intern("requests"),
        MetricKind::Sum(logit_core::Sum {
            value: MetricValue::I64(12),
            temporality: logit_core::Temporality::Delta,
            monotonic: true,
        }),
    ));
    let sent =
        EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![event] };
    out.send(&sent).await.expect("written");

    assert_eq!(
        recv_batch(&mut rx).await,
        seed("requests,host=web-1 value=12i 1700000000000000000")
    );
}

/// The listener's answers the sink must class right: a wrong token and an empty bucket hold the
/// queue (`Refused`), an empty org drops the batch (`Rejected`), and nothing is delivered for any.
#[tokio::test]
async fn refusals_reach_the_sink_as_their_fault_class() {
    let mut probe = TelemetryProbe::new();
    let (addr, mut rx) = listener(&probe).await;
    let cases = [
        ("wrong token", ORG, BUCKET, "wrong", Fault::Refused, "auth"),
        ("empty bucket", ORG, "", TOKEN, Fault::Refused, "no_bucket"),
        ("empty org", "", BUCKET, TOKEN, Fault::Rejected, "no_org"),
    ];
    for (name, org, bucket, token, fault, reason) in cases {
        let err = sink(addr, &probe, org, bucket, token).send(&typed()).await.unwrap_err();
        assert_eq!(classify(&err), fault, "{name}: {err:#}");
        assert_eq!(
            probe.sum("logit.input.requests.rejected", &[("reason", reason)]),
            1.0,
            "{name}"
        );
    }
    assert_no_batch(&mut rx, NOTHING_MORE, "nothing reaches influxdb_in").await;
    let totals = probe.poll();
    assert_eq!(totals.sum("logit.input.requests", &[("class", "rejected")]), 3.0);
    assert_eq!(totals.sum("logit.input.requests", &[("class", "ok")]), 0.0);
    assert_eq!(totals.sum("logit.output.requests", &[("class", "4xx")]), 3.0);
}

/// `influxdb_in -> influxdb_out -> influxdb_in` with no `bucket:` on the sink: writes to two
/// buckets through one listener each arrive at the second listener under the bucket and org their
/// client named (`logit_outputs::influxdb`'s module doc, "Destination resolution").
#[tokio::test]
async fn a_carried_bucket_and_org_relay_through_one_sink() {
    let probe = TelemetryProbe::new();
    let (front, mut front_rx) = listener(&probe).await;
    let (back, mut back_rx) = listener(&probe).await;
    let mut out = InfluxDbOutput::new(
        format!("http://{back}"),
        Organization::Carried { default_org: None },
        Destination::Carried { default_bucket: None },
        TOKEN.into(),
    )
    .with_telemetry(probe.telemetry("out", "influxdb_out", "sink"));
    let client = reqwest::Client::new();
    for bucket in ["alpha", "beta"] {
        let status = client
            .post(format!("http://{front}/api/v2/write"))
            .query(&[("org", ORG), ("bucket", bucket), ("precision", "ns")])
            .header("Authorization", format!("Token {TOKEN}"))
            .body("cpu,host=web-1 usage=1.5 1700000000000000000\n")
            .send()
            .await
            .expect("posting to influxdb_in")
            .status();
        assert_eq!(status.as_u16(), 204, "{bucket}");
        let received = recv_batch(&mut front_rx).await;
        out.send(&received).await.unwrap_or_else(|err| panic!("{bucket}: {err:#}"));
        let relayed = recv_batch(&mut back_rx).await;
        let carried = |key: &str| relayed.resource.attributes.get(key).and_then(Value::as_str);
        assert_eq!(carried(RESOURCE_ATTR_BUCKET), Some(bucket));
        assert_eq!(carried(RESOURCE_ATTR_ORG), Some(ORG), "{bucket}");
        assert_eq!(relayed.events, received.events, "{bucket}");
    }
    assert_no_batch(&mut back_rx, NOTHING_MORE, "nothing more reaches the second listener").await;
}
