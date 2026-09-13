//! End-to-end proof of `docs/adr/target-components.md`'s headline topology: a real `logit_in`
//! feeding a real `route` router, splitting onto two `target`s, with the router's own outbound
//! edge catching whatever nothing claims. Modelled on `logit_round_trip.rs`'s in-process pattern
//! (`ephemeral_addr`, `round_trip_with_provenance`'s `observe_batch`-then-`send` idiom) for the
//! wire side, and `durable_buffer_restart.rs`'s `Config` -> `graph::resolve` -> `NodeSpec`s ->
//! `logit_pipeline::run` pattern for the central side -- a real graph, not a hand-rolled `Fanout`
//! chain, so this exercises the actual `Router`/`Target` runtime wiring
//! (`crates/logit-pipeline/src/runtime.rs`'s pre-spawn target-`Fanout` pass and `run_router`), not
//! a simplified stand-in for it.
//!
//! Topology:
//!
//! ```text
//! central_in (logit_in) -> split (route, by: {provenance: origin})
//!                                   |-> host_stream (target) -> host_sink (recording)
//!                                   |-> app_stream  (target) -> app_sink  (recording)
//!                                   \-> (unrouted, split's own consumer) -> forward_sink (recording)
//! ```
//!
//! `docs/plans/target-components.md` workstream W4.

use logit_config::{
    BufferConfig, Component, ComponentKind, Config, ProvenanceField, ReceiveConfig, RouteBy,
};
use logit_core::interner::intern;
use logit_core::{AttrMap, BodyFormat, Event, EventBatch, LogRecord, Provenance, Resource, Value};
use logit_inputs::logit::LogitInput;
use logit_outputs::logit::LogitOutput;
use logit_pipeline::{
    graph, run, BatchContext, InputRuntimeConfig, NodeSpec, Output, SinkQueueConfig,
    SinkStoreConfig, TraceContext, WriteLoopConfig,
};
use logit_transforms::Route;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// Same bind-drop-rebind idiom `logit_round_trip.rs` uses to learn a free port before
/// constructing the component that will actually bind it.
async fn ephemeral_addr() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().to_string()
}

/// One log event naming which leg of the split it should end up on, so a received batch's own
/// content confirms placement independent of which channel it arrived on.
fn tagged_batch(tag: &str) -> EventBatch {
    let mut attrs = AttrMap::new();
    attrs.insert("tag", Value::str(tag));
    let event = Event::log(
        0,
        attrs,
        LogRecord {
            message: Value::str(tag),
            severity: None,
            body_format: BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );
    EventBatch { resource: Arc::new(Resource::default()), scope: None, events: vec![event] }
}

/// Records every batch this sink is asked to deliver, paired with the [`Provenance`]
/// `Output::observe_batch` handed it immediately beforehand -- `write_loop` calls that hook once
/// per delivery attempt, always before `send`, so the two always describe the same batch
/// (`crate::runtime`'s own doc comment on `Output::observe_batch`).
struct RecordingOutput {
    tx: mpsc::UnboundedSender<(EventBatch, Provenance)>,
    last_provenance: Provenance,
}

#[async_trait::async_trait]
impl Output for RecordingOutput {
    fn observe_batch(&mut self, ctx: BatchContext) {
        self.last_provenance = ctx.provenance;
    }

    async fn send(&mut self, batch: &EventBatch) -> anyhow::Result<()> {
        let _ = self.tx.send((batch.clone(), self.last_provenance));
        Ok(())
    }
}

fn recording_sink() -> (mpsc::UnboundedReceiver<(EventBatch, Provenance)>, RecordingOutput) {
    let (tx, rx) = mpsc::unbounded_channel();
    (rx, RecordingOutput { tx, last_provenance: Provenance::default() })
}

fn output_spec(output: RecordingOutput) -> NodeSpec {
    NodeSpec::Output(
        Box::new(output),
        SinkStoreConfig::Memory(SinkQueueConfig::default()),
        WriteLoopConfig::default(),
    )
}

/// A no-field, no-source `Component` -- shared shape for every kind built below that doesn't need
/// `buffer:`/`receive:`.
fn component(sources: Vec<&str>, kind: ComponentKind) -> Component {
    Component {
        buffer: BufferConfig::default(),
        receive: ReceiveConfig::default(),
        sources: sources.into_iter().map(String::from).collect(),
        targets: Vec::new(),
        kind,
    }
}

/// A real, resolved graph for the topology in this module's doc comment, plus the ephemeral
/// address `central_in` is bound to. Sink kinds are `null_out` -- only their *shape* (a sink, one
/// source) matters to `graph::resolve`; their actual runtime behaviour comes from the
/// [`RecordingOutput`] `NodeSpec`s built separately, exactly as `durable_buffer_restart.rs`'s
/// `graph_and_topology` pairs an `influxdb_out`-shaped `Config` with a hand-rolled `NodeSpec`
/// (spec kind and config kind are independent at the runtime layer,
/// `crates/logit-pipeline/src/runtime.rs`'s own tests make the same trade for `Router`/`Target`).
async fn central_graph() -> (graph::Graph, String) {
    let addr = ephemeral_addr().await;
    let mut routes = BTreeMap::new();
    routes.insert("edge_host".to_string(), "host_stream".to_string());
    routes.insert("edge_app".to_string(), "app_stream".to_string());

    let mut components = HashMap::new();
    components.insert(
        "central_in".to_string(),
        component(
            vec![],
            ComponentKind::LogitIn { bind: addr.clone(), tls: None, max_frame_bytes: None },
        ),
    );
    components.insert(
        "split".to_string(),
        component(
            vec!["central_in"],
            ComponentKind::Route { by: RouteBy::Provenance(ProvenanceField::Origin), routes },
        ),
    );
    components.insert("host_stream".to_string(), component(vec![], ComponentKind::Target {}));
    components.insert("app_stream".to_string(), component(vec![], ComponentKind::Target {}));
    components
        .insert("host_sink".to_string(), component(vec!["host_stream"], ComponentKind::NullOut {}));
    components
        .insert("app_sink".to_string(), component(vec!["app_stream"], ComponentKind::NullOut {}));
    components
        .insert("forward_sink".to_string(), component(vec!["split"], ComponentKind::NullOut {}));

    let graph = graph::resolve(Config { components, ..Default::default() })
        .expect("topology should resolve");
    (graph, addr)
}

/// Drives the whole graph: three batches over one `LogitOutput` connection to `central_in`, each
/// primed with a different provenance beforehand (`Output::observe_batch`'s own client-side
/// mirror, `LogitOutput::observe_batch`), matching `logit_round_trip.rs`'s
/// `round_trip_with_provenance` idiom. Returns whichever of the three recording sinks actually
/// received something for each send, paired with a timeout so a misrouted batch fails the test
/// instead of hanging it.
#[tokio::test]
async fn a_route_component_splits_a_real_logit_in_stream_onto_its_targets() {
    let (graph, addr) = central_graph().await;

    // `component.targets` is the graph's own resolved slot order for `split`
    // (`graph::targets_of`'s output, `docs/adr/target-components.md`) -- read back rather than
    // hardcoded, so this test can't silently pass by guessing the same order `Route::new` needs.
    let split_targets = graph.components["split"].targets.clone();
    assert_eq!(
        split_targets.len(),
        2,
        "split should direct at exactly the two targets its routes: map names"
    );

    let mut routes = BTreeMap::new();
    routes.insert("edge_host".to_string(), "host_stream".to_string());
    routes.insert("edge_app".to_string(), "app_stream".to_string());
    let route = Route::new(RouteBy::Provenance(ProvenanceField::Origin), &routes, &split_targets);

    let (mut host_rx, host_output) = recording_sink();
    let (mut app_rx, app_output) = recording_sink();
    let (mut forward_rx, forward_output) = recording_sink();

    let mut specs: HashMap<String, NodeSpec> = HashMap::new();
    specs.insert(
        "central_in".to_string(),
        NodeSpec::Input(Box::new(LogitInput::new(addr.clone())), InputRuntimeConfig::default()),
    );
    specs.insert("split".to_string(), NodeSpec::Router(Box::new(route)));
    specs.insert("host_stream".to_string(), NodeSpec::Target);
    specs.insert("app_stream".to_string(), NodeSpec::Target);
    specs.insert("host_sink".to_string(), output_spec(host_output));
    specs.insert("app_sink".to_string(), output_spec(app_output));
    specs.insert("forward_sink".to_string(), output_spec(forward_output));

    tokio::spawn(run(graph, specs));
    // Gives `central_in`'s pre-spawn `Input::bind` pass time to actually open the socket before
    // the client below tries to connect -- same idiom, same duration, as `logit_round_trip.rs`'s
    // `round_trip`/`round_trip_with_provenance`.
    tokio::time::sleep(Duration::from_millis(50)).await;

    let mut output = LogitOutput::new(addr);

    // -- Batch 1: origin edge_host -> host_stream ------------------------------------------
    output.observe_batch(BatchContext {
        trace: TraceContext::new_root(),
        provenance: Provenance {
            origin: Some(intern("edge_host")),
            previous: Some(intern("tag_host")),
        },
    });
    output
        .send(&tagged_batch("host"))
        .await
        .expect("send should succeed against a live central_in");

    let (host_batch, host_provenance) =
        tokio::time::timeout(Duration::from_secs(5), host_rx.recv())
            .await
            .expect("host_sink should receive within the timeout")
            .expect("channel should not have closed");
    assert_eq!(host_batch.events.len(), 1);
    assert_eq!(host_batch.events[0].attributes.get("tag"), Some(&Value::str("host")));
    assert_eq!(
        host_provenance.origin_str(),
        Some("edge_host"),
        "origin must ride through the target untouched"
    );
    assert_eq!(
        host_provenance.previous_str(),
        Some("host_stream"),
        "previous downstream of a target is the target's own id, never the router's \
         (docs/adr/target-components.md)"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), app_rx.recv()).await.is_err(),
        "app_sink must not receive the host-tagged batch"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), forward_rx.recv()).await.is_err(),
        "forward_sink must not receive a batch a route claimed"
    );

    // -- Batch 2: origin edge_app -> app_stream --------------------------------------------
    output.observe_batch(BatchContext {
        trace: TraceContext::new_root(),
        provenance: Provenance {
            origin: Some(intern("edge_app")),
            previous: Some(intern("tag_app")),
        },
    });
    output.send(&tagged_batch("app")).await.expect("send should succeed on the reused connection");

    let (app_batch, app_provenance) = tokio::time::timeout(Duration::from_secs(5), app_rx.recv())
        .await
        .expect("app_sink should receive within the timeout")
        .expect("channel should not have closed");
    assert_eq!(app_batch.events[0].attributes.get("tag"), Some(&Value::str("app")));
    assert_eq!(app_provenance.origin_str(), Some("edge_app"));
    assert_eq!(app_provenance.previous_str(), Some("app_stream"));
    assert!(
        tokio::time::timeout(Duration::from_millis(200), host_rx.recv()).await.is_err(),
        "host_sink must not receive the app-tagged batch"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), forward_rx.recv()).await.is_err(),
        "forward_sink must not receive a batch a route claimed"
    );

    // -- Batch 3: an origin no route names -> split's own consumer (forward_sink) ----------
    output.observe_batch(BatchContext {
        trace: TraceContext::new_root(),
        provenance: Provenance {
            origin: Some(intern("edge_unknown")),
            previous: Some(intern("tag_unknown")),
        },
    });
    output
        .send(&tagged_batch("unknown"))
        .await
        .expect("send should succeed on the reused connection");

    let (forward_batch, forward_provenance) =
        tokio::time::timeout(Duration::from_secs(5), forward_rx.recv())
            .await
            .expect("forward_sink should receive the unrouted batch within the timeout")
            .expect("channel should not have closed");
    assert_eq!(forward_batch.events[0].attributes.get("tag"), Some(&Value::str("unknown")));
    assert_eq!(
        forward_provenance.origin_str(),
        Some("edge_unknown"),
        "an unrouted batch's provenance is untouched by the router beyond its own previous stamp"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), host_rx.recv()).await.is_err(),
        "host_sink must not receive the unrouted batch"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(200), app_rx.recv()).await.is_err(),
        "app_sink must not receive the unrouted batch"
    );
}
