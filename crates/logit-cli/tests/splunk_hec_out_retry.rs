//! `splunk_hec_out` under the runtime's retry loop: a busy Splunk (`503` code 9) that hasn't
//! taken any body of a batch is retried under the sink's default at-most-once posture, and the
//! batch is delivered once. The graph and its `NodeSpec`s are built through `logit_pipeline`'s
//! public API so the listener can be a test double that sends one batch and returns.

use logit_config::{Component, Config};
use logit_core::{AttrMap, BodyFormat, Event, EventBatch, LogRecord, Resource, Value};
use logit_outputs::splunk::{SplunkCompression, SplunkHecOutput};
use logit_pipeline::{
    graph, Fanout, Input, InputRuntimeConfig, NodeSpec, SinkQueueConfig, SinkStoreConfig,
    WriteLoopConfig,
};
use logit_proto::splunk::SplunkDecoder;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "11111111-2222-3333-4444-555555555555";
const BUSY: &str = r#"{"text":"Server is busy","code":9}"#;
const SUCCESS: &str = r#"{"text":"Success","code":0}"#;

/// Sends one batch, then returns, which closes the sink's inbox once the batch is queued.
struct OneBatch(Option<EventBatch>);

#[async_trait::async_trait]
impl Input for OneBatch {
    async fn run(&mut self, sink: Fanout) -> anyhow::Result<()> {
        if let Some(batch) = self.0.take() {
            sink.send(batch).await;
        }
        Ok(())
    }
}

fn batch() -> EventBatch {
    let mut resource = Resource::default();
    resource.attributes.insert("host.name", Value::str("web-1"));
    let log = LogRecord {
        message: Value::str("hello"),
        severity: None,
        body_format: BodyFormat::Raw,
        trace: None,
        event_name: None,
        observed_timestamp: 0,
        dropped_attributes_count: 0,
    };
    EventBatch {
        resource: Arc::new(resource),
        scope: None,
        events: vec![Event::log(1_700_000_000_000_000_000, AttrMap::new(), log)],
    }
}

/// A HEC stand-in that answers `503` code 9 to the first `busy` requests and `200` after,
/// recording each request body. One request per connection, so no keep-alive state matters.
async fn busy_then_ok(busy: usize) -> (SocketAddr, Arc<Mutex<Vec<Vec<u8>>>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let bodies: Arc<Mutex<Vec<Vec<u8>>>> = Arc::default();
    let seen = bodies.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else { return };
            let mut buf = Vec::new();
            let mut chunk = [0u8; 4096];
            let body_start = loop {
                let n = stream.read(&mut chunk).await.unwrap();
                if n == 0 {
                    break None;
                }
                buf.extend_from_slice(&chunk[..n]);
                if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                    break Some(i + 4);
                }
            };
            let Some(body_start) = body_start else { continue };
            let head = String::from_utf8_lossy(&buf[..body_start]).to_ascii_lowercase();
            let length: usize = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .map(|v| v.trim().parse().unwrap())
                .unwrap_or(0);
            while buf.len() < body_start + length {
                let n = stream.read(&mut chunk).await.unwrap();
                buf.extend_from_slice(&chunk[..n]);
            }
            let answered = {
                let mut bodies = seen.lock().unwrap();
                bodies.push(buf[body_start..].to_vec());
                bodies.len()
            };
            let (status, body) = if answered <= busy {
                ("503 Service Unavailable", BUSY)
            } else {
                ("200 OK", SUCCESS)
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\
                 Connection: close\r\n\r\n{body}",
                body.len()
            );
            stream.write_all(response.as_bytes()).await.unwrap();
            let _ = stream.shutdown().await;
        }
    });
    (addr, bodies)
}

fn component(json: &str) -> Component {
    serde_json::from_str(json).expect("a valid component")
}

#[tokio::test]
async fn a_busy_splunk_is_retried_and_the_batch_delivered_once() {
    let (addr, bodies) = busy_then_ok(2).await;
    let endpoint = format!("http://{addr}/services/collector");

    let components = HashMap::from([
        ("in".to_string(), component(r#"{"type": "splunk_hec_in", "bind": "127.0.0.1:0"}"#)),
        (
            "out".to_string(),
            component(&format!(
                r#"{{"type": "splunk_hec_out", "sources": ["in"], "endpoint": "{endpoint}",
                    "token": "{TOKEN}"}}"#
            )),
        ),
    ]);
    let graph = graph::resolve(Config { components, ..Default::default() }).expect("resolves");

    let output =
        SplunkHecOutput::new(endpoint, TOKEN).unwrap().with_compression(SplunkCompression::None);
    assert!(!logit_pipeline::Output::duplicate_safe(&output), "the default posture: at-most-once");
    let specs: HashMap<String, NodeSpec> = HashMap::from([
        (
            "in".to_string(),
            NodeSpec::Input(Box::new(OneBatch(Some(batch()))), InputRuntimeConfig::default()),
        ),
        (
            "out".to_string(),
            NodeSpec::Output(
                Box::new(output),
                SinkStoreConfig::Memory(SinkQueueConfig::default()),
                WriteLoopConfig::default(),
            ),
        ),
    ]);

    tokio::time::timeout(
        Duration::from_secs(20),
        logit_pipeline::run_with_shutdown(graph, specs, std::future::pending()),
    )
    .await
    .expect("the pipeline finishes once its one batch is delivered")
    .expect("the pipeline runs cleanly");

    let bodies = bodies.lock().unwrap().clone();
    assert_eq!(bodies.len(), 3, "two busy answers, then the one accepted");
    for body in &bodies {
        let decoded = SplunkDecoder::new().decode_events(body, 0).expect("a HEC body");
        let messages: Vec<&Value> = decoded
            .iter()
            .flat_map(|b| &b.events)
            .map(|e| &e.log.as_ref().expect("a log").message)
            .collect();
        assert_eq!(messages, [&Value::str("hello")], "every attempt carries the whole batch");
    }
}
