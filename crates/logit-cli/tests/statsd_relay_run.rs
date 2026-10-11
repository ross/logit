//! A `statsd_in -> aggregate -> statsd_out` relay through a real `logit run`, with no
//! `multi_value` key in the config, so it pins the config default: an aggregated timer and set
//! leave as the dotted counter and gauge lines `statsd_in` decodes. `statsd_round_trip.rs` covers
//! the encoder in-process; this covers the config-to-encoder wiring the binary does.

mod support;

use std::collections::BTreeMap;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use logit_core::{MetricKind, Resource, Temporality};
use logit_proto::statsd::StatsdDecoder;
use logit_proto::Decoder;
use support::{ephemeral_addr, wait_until_ready, KillOnDrop, TempConfig, PROCESS_DEADLINE};
use tokio::net::UdpSocket;

#[tokio::test(flavor = "multi_thread")]
async fn a_default_statsd_relay_expands_an_aggregated_timer_and_set() {
    let capture = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let capture_addr = capture.local_addr().unwrap();
    let statsd_addr = {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        socket.local_addr().unwrap().to_string()
    };
    let admin_addr = ephemeral_addr().await;
    let config = TempConfig::write(
        "statsd-relay-default-multi-value",
        format!(
            "admin:\n  bind: \"{admin_addr}\"\ncomponents:\n  in:\n    type: statsd_in\n    \
             bind: \"{statsd_addr}\"\n  agg:\n    type: aggregate\n    sources: [in]\n    \
             interval: 200ms\n  out:\n    type: statsd_out\n    sources: [agg]\n    endpoint: \
             \"{capture_addr}\"\n"
        ),
    );

    let child = Command::new(env!("CARGO_BIN_EXE_logit"))
        .arg("run")
        .arg(&config.0)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawning logit run");
    let _child = KillOnDrop(child);
    // Ready means every listener is bound, so the datagram below can't reach an unbound port.
    wait_until_ready(&admin_addr).await;

    // One datagram, so every line lands in one decode and one aggregation window.
    let sender = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    sender
        .send_to(
            b"req.latency:10|ms\nreq.latency:20|ms\nuniq.users:alice|s\nuniq.users:bob|s",
            &statsd_addr,
        )
        .await
        .unwrap();

    let mut decoder = StatsdDecoder::new(Arc::new(Resource::default()));
    let mut decoded: BTreeMap<String, MetricKind> = BTreeMap::new();
    let deadline = Instant::now() + PROCESS_DEADLINE;
    let mut buf = vec![0u8; 65_536];
    while !(decoded.contains_key("req.latency.count") && decoded.contains_key("uniq.users.count")) {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let (n, _) = tokio::time::timeout(remaining, capture.recv_from(&mut buf))
            .await
            .unwrap_or_else(|_| {
                panic!("no expanded lines within {PROCESS_DEADLINE:?}: {decoded:?}")
            })
            .unwrap();
        let batch = decoder.decode(Bytes::copy_from_slice(&buf[..n])).expect("statsd lines");
        for event in batch.events {
            for metric in event.metrics {
                decoded.insert(logit_core::interner::resolve(metric.name).to_string(), metric.kind);
            }
        }
    }

    let counter = |name: &str| match &decoded[name] {
        MetricKind::Sum(s) if s.temporality == Temporality::Delta && s.monotonic => s.value,
        other => panic!("{name} isn't a counter: {other:?}"),
    };
    assert_eq!(counter("req.latency.count"), 2.0);
    assert_eq!(counter("req.latency.sum"), 30.0);
    assert!(matches!(decoded["req.latency.min"], MetricKind::Gauge(v) if v == 10.0), "{decoded:?}");
    assert!(matches!(decoded["req.latency.max"], MetricKind::Gauge(v) if v == 20.0), "{decoded:?}");
    assert!(matches!(decoded["uniq.users.count"], MetricKind::Gauge(v) if v == 2.0), "{decoded:?}");
    for q in ["q0_5", "q0_75", "q0_9", "q0_95", "q0_99"] {
        let name = format!("req.latency.{q}");
        assert!(matches!(decoded.get(&name), Some(MetricKind::Gauge(_))), "{name}: {decoded:?}");
    }
    assert!(!decoded.contains_key("req.latency"), "no raw timer line: {decoded:?}");
}
