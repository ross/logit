//! Throughput benches over the reference nginx pipeline. Run with `script/bench`.
//!
//! Deliberately **not** part of `script/cibuild`: wall-clock benchmarking on shared CI runners
//! measures the runner. The allocation numbers that *do* need to hold every build are assertions
//! in `tests/allocations.rs` instead.
//!
//! Almost every bench here calls decoders, transforms, and encoders **directly**, sidestepping the
//! tokio runtime and the channels between nodes entirely. That's a constraint, not a
//! simplification: `divan::AllocProfiler` only counts allocations on threads Divan controls. The
//! actual boundary is **no cross-thread hop** (a `tokio::spawn`, a multi-thread runtime, a real OS
//! thread) -- not "no channel" -- which is what lets `mod runtime` below drive a real
//! `tokio::sync::mpsc` channel and still trust its allocation column: a `current_thread` runtime's
//! `block_on` keeps everything on the one thread Divan is already watching. See that module's own
//! doc comment, and `docs/design/memory.md` §7, for the full account. What a full multi-node graph
//! costs in wall-clock terms, spread across the real worker/OS threads `run_with_shutdown` actually
//! spawns, is still a separate question needing a load generator, not a microbenchmark.

use divan::{AllocProfiler, Bencher};
use logit_bench::fixtures;
use logit_outputs::influxdb::InfluxLineEncoder;
use logit_outputs::stdio::{EventDump, Format};
use logit_outputs::syslog::{Format as SyslogFormat, MessageBuf, SyslogEncoder};
use logit_pipeline::Transform;
use logit_proto::{Decoder, Encoder};
use logit_script::ScriptWorker;

/// Divan's own allocator, so every bench reports allocation count and bytes alongside its timing.
/// Note that counting allocations is itself work that happens inside the timed region, so these
/// timings are slightly pessimistic in absolute terms -- they're for comparing shapes against each
/// other, not for quoting as throughput ceilings.
#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main();
}

#[divan::bench(args = [1, 10, 100])]
fn syslog_decode(bencher: Bencher, lines: usize) {
    let datagram = fixtures::nginx_syslog_datagram(lines);
    let mut decoder = fixtures::syslog_decoder();
    bencher.bench_local(|| decoder.decode(divan::black_box(datagram.clone())));
}

#[divan::bench(args = [1, 10, 100])]
fn statsd_decode(bencher: Bencher, lines: usize) {
    let datagram = fixtures::statsd_datagram(lines);
    let mut decoder = fixtures::statsd_decoder();
    bencher.bench_local(|| decoder.decode(divan::black_box(datagram.clone())));
}

#[divan::bench]
fn json_parse(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut json = fixtures::json_parser();
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::nginx_syslog_datagram(1);
    bencher
        .with_inputs(|| {
            decoder.decode(datagram.clone()).expect("should decode").events.pop().expect("an event")
        })
        .bench_local_values(|event| json.process(&resource, event));
}

#[divan::bench]
fn kv_metrics(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut kv = fixtures::kv_metrics();
    let mut json = fixtures::json_parser();
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::nginx_syslog_datagram(1);
    bencher
        .with_inputs(|| {
            let event = decoder
                .decode(datagram.clone())
                .expect("should decode")
                .events
                .pop()
                .expect("an event");
            json.process(&resource, event).expect("json forwards")
        })
        .bench_local_values(|event| kv.process(&resource, event));
}

#[divan::bench]
fn keep(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut keep = fixtures::keep();
    bencher
        .with_inputs(fixtures::nginx_event)
        .bench_local_values(|event| keep.process(&resource, event));
}

#[divan::bench]
fn aggregate_absorb(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut keep = fixtures::keep();
    let mut agg = fixtures::aggregator();
    bencher
        .with_inputs(|| keep.process(&resource, fixtures::nginx_event()).expect("keep forwards"))
        .bench_local_values(|event| agg.process(&resource, event));
}

/// What each extra fan-out consumer costs per event (`logit_pipeline::Fanout::send` deep-clones
/// the batch for every consumer but the last). The `Arc<EventBatch>` copy-on-write change
/// described in `docs/design/memory.md` is aimed squarely at this number.
mod clone {
    use super::*;

    #[divan::bench]
    fn nginx_shape(bencher: Bencher) {
        let event = fixtures::nginx_event();
        bencher.bench_local(|| divan::black_box(&event).clone());
    }

    #[divan::bench]
    fn statsd_shape(bencher: Bencher) {
        let event = fixtures::statsd_event();
        bencher.bench_local(|| divan::black_box(&event).clone());
    }

    #[divan::bench]
    fn distribution_shape(bencher: Bencher) {
        let event = fixtures::distribution_event();
        bencher.bench_local(|| divan::black_box(&event).clone());
    }
}

mod encode {
    use super::*;

    #[divan::bench(args = [1, 100])]
    fn influx(bencher: Bencher, events: usize) {
        let batch = fixtures::nginx_batch(events);
        let mut encoder = InfluxLineEncoder::default();
        bencher.bench_local(|| encoder.encode(divan::black_box(&batch)));
    }

    #[divan::bench(args = [1, 100])]
    fn stdio(bencher: Bencher, events: usize) {
        let batch = fixtures::nginx_batch(events);
        let dump = EventDump::new(Format::Human);
        bencher.bench_local(|| dump.encode(divan::black_box(&batch)));
    }

    #[divan::bench(args = [1, 100])]
    fn syslog(bencher: Bencher, events: usize) {
        let batch = fixtures::nginx_batch(events);
        let mut encoder = SyslogEncoder::new(SyslogFormat::Rfc5424, 16);
        let mut out = MessageBuf::default();
        bencher.bench_local(|| encoder.encode_into(divan::black_box(&batch), &mut out));
    }
}

/// The comparison `docs/known-gaps.md` has been carrying as an open item: `docs/design/lua-api.md`
/// commits to the userdata proxy over full table conversion on reasoning alone. `proxy` is what a
/// script pays reading two attributes through `EventProxy`; `to_table` is what the rejected
/// design would have cost on every event whether the script touched an attribute or not.
mod lua {
    use super::*;

    #[divan::bench]
    fn proxy(bencher: Bencher) {
        let worker = ScriptWorker::new(fixtures::LUA_ENRICH_SCRIPT).expect("script should load");
        bencher
            .with_inputs(fixtures::nginx_event)
            .bench_local_values(|event| worker.process(event));
    }

    #[divan::bench]
    fn to_table(bencher: Bencher) {
        const SCRIPT: &str = r#"
            function process(event)
              local t = event:to_table()
              if t.attributes.host ~= nil then
                event.attributes.env = "prod"
              end
              return event
            end
        "#;
        let worker = ScriptWorker::new(SCRIPT).expect("script should load");
        bencher
            .with_inputs(fixtures::nginx_event)
            .bench_local_values(|event| worker.process(event));
    }
}

/// Direct-call benches for `logit_proto::buffer::InMemoryBuffer` -- the sync buffer
/// `logit_pipeline::SinkQueue` wraps (`docs/adr/0021-buffered-sink-delivery.md`). Called directly,
/// never through `SinkQueue`/tokio, for the same reason every other bench in this file calls its
/// subject directly: `divan::AllocProfiler` only counts allocations on threads Divan controls, and
/// a bench that hops through a channel or a tokio task would misreport.
mod sink_queue {
    use super::*;
    use logit_core::EventBatch;
    use logit_proto::buffer::{Buffer, InMemoryBuffer, OverflowPolicy};
    use std::sync::Arc;

    fn item() -> Arc<EventBatch> {
        Arc::new(fixtures::nginx_batch(1))
    }

    /// The common case: a queue nowhere near its bound, so every push is a plain
    /// `VecDeque::push_back` and every commit a plain `pop_front`, never the eviction path.
    #[divan::bench]
    fn push_commit_steady_state(bencher: Bencher) {
        let mut buf: InMemoryBuffer<Arc<EventBatch>> =
            InMemoryBuffer::new(1024, u64::MAX, OverflowPolicy::DropOldest);
        let batch = item();
        bencher.bench_local(|| {
            let weight = batch.estimated_heap_bytes();
            drop(buf.push(Arc::clone(&batch), weight));
            drop(buf.commit());
        });
    }

    #[divan::bench]
    fn peek(bencher: Bencher) {
        let mut buf: InMemoryBuffer<Arc<EventBatch>> =
            InMemoryBuffer::new(1024, u64::MAX, OverflowPolicy::DropOldest);
        let batch = item();
        drop(buf.push(Arc::clone(&batch), batch.estimated_heap_bytes()));
        bencher.bench_local(|| divan::black_box(buf.peek().is_some()));
    }

    /// The worst case for `DropOldest`: the buffer is held exactly at its bound (one slot, never
    /// committed), so every push evicts the current head -- isolates what the eviction path itself
    /// costs, on top of steady-state push/commit above.
    #[divan::bench]
    fn push_drop_oldest_always_evicting(bencher: Bencher) {
        let mut buf: InMemoryBuffer<Arc<EventBatch>> =
            InMemoryBuffer::new(1, u64::MAX, OverflowPolicy::DropOldest);
        let batch = item();
        drop(buf.push(Arc::clone(&batch), batch.estimated_heap_bytes()));
        bencher.bench_local(|| {
            let weight = batch.estimated_heap_bytes();
            drop(buf.push(Arc::clone(&batch), weight));
        });
    }
}

/// Decode through aggregation for one access-log line -- the number that bounds ingest throughput
/// for the reference config.
#[divan::bench]
fn full_chain(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut decoder = fixtures::syslog_decoder();
    let mut json = fixtures::json_parser();
    let mut kv = fixtures::kv_metrics();
    let mut keep = fixtures::keep();
    let mut agg = fixtures::aggregator();
    let datagram = fixtures::nginx_syslog_datagram(1);

    bencher.bench_local(|| {
        let batch = decoder.decode(divan::black_box(datagram.clone())).expect("should decode");
        for event in batch.events {
            let event = json.process(&resource, event).expect("json forwards");
            let event = kv.process(&resource, event).expect("kv forwards");
            let event = keep.process(&resource, event).expect("keep forwards");
            drop(agg.process(&resource, event));
        }
    });
}

/// The node-runtime paths `tests/allocations.rs`'s "Runtime" section pins by exact allocation
/// count -- this module is their throughput/wall-clock view.
///
/// `fanout_send_one_consumer`/`fanout_send_two_consumers`/`send_batch_through_a_noop_output` are
/// the deliberate exceptions to this file's module doc above: they *do* cross a
/// `tokio::sync::mpsc` channel (the fanout pair) or call through `#[async_trait]` (`send_batch`).
/// Both are still safe to read the allocation column on, for the reason the module doc above gives
/// in full: neither ever calls `tokio::spawn`, so nothing here leaves the one thread Divan is
/// watching. `tests/allocations.rs`'s own `fanout_send_one_consumer_costs_nothing`,
/// `fanout_send_two_consumers_costs_one_clone_plus_one_arc`, and
/// `send_batch_through_a_noop_output_disabled_telemetry` use the identical construction and are
/// the numbers to cross-check this module's allocation column against.
mod runtime {
    use super::*;
    use logit_core::{EventBatch, Telemetry};
    use logit_pipeline::{
        process_batch, send_batch, unwrap_batch, Delivered, Fanout, TraceContext,
    };

    /// `run_transform`'s per-batch body (`logit_pipeline::process_batch`), with no channel or
    /// runtime involved at all -- a plain synchronous call, so both of this bench's columns are
    /// trustworthy the same way every other bench above it is.
    #[divan::bench]
    fn process_batch_through_keep(bencher: Bencher) {
        let mut keep = fixtures::keep();
        let telemetry = Telemetry::default();
        bencher
            .with_inputs(|| fixtures::nginx_batch(1))
            .bench_local_values(|batch| process_batch(&mut keep, batch, &telemetry));
    }

    /// One consumer -- the common case (every listener's first hop, every interior edge of a
    /// linear chain) -- costing nothing, per `tests/allocations.rs`'s
    /// `fanout_send_one_consumer_costs_nothing`.
    #[divan::bench]
    fn fanout_send_one_consumer(bencher: Bencher) {
        let rt =
            tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let fanout = Fanout::new(vec![tx]);
        bencher.with_inputs(|| fixtures::nginx_batch(1)).bench_local_values(|batch| {
            rt.block_on(async {
                fanout.send(batch).await;
                unwrap_batch(rx.recv().await.expect("should receive"))
            })
        });
    }

    /// A real fan-out: one branch clones (`Arc::try_unwrap` fails, `unwrap_batch` falls back), the
    /// other doesn't -- see `tests/allocations.rs`'s
    /// `fanout_send_two_consumers_costs_one_clone_plus_one_arc` for the exact accounting this
    /// bench's allocation column should match.
    #[divan::bench]
    fn fanout_send_two_consumers(bencher: Bencher) {
        let rt =
            tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
        let (tx_a, mut rx_a) = tokio::sync::mpsc::channel(1);
        let (tx_b, mut rx_b) = tokio::sync::mpsc::channel(1);
        let fanout = Fanout::new(vec![tx_a, tx_b]);
        bencher.with_inputs(|| fixtures::nginx_batch(1)).bench_local_values(|batch: EventBatch| {
            rt.block_on(async {
                fanout.send(batch).await;
                let a: Delivered = rx_a.recv().await.expect("a should receive");
                let b: Delivered = rx_b.recv().await.expect("b should receive");
                (unwrap_batch(a), unwrap_batch(b))
            })
        });
    }

    /// A no-op `Output`, matching `tests/allocations.rs`'s own -- isolates `send_batch`'s own
    /// accounting from any real sink's encode/write cost.
    struct NoopOutput;

    #[async_trait::async_trait]
    impl logit_pipeline::Output for NoopOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// `run_output`'s per-batch body (`logit_pipeline::send_batch`) -- added in review alongside
    /// `tests/allocations.rs`'s own `send_batch` coverage, closing the gap where this module had a
    /// throughput bench for `run_transform`'s body but none for `run_output`'s.
    #[divan::bench]
    fn send_batch_through_a_noop_output(bencher: Bencher) {
        let rt =
            tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
        let mut output = NoopOutput;
        let telemetry = Telemetry::default();
        bencher
            .with_inputs(|| Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default()))
            .bench_local_values(|delivered| {
                rt.block_on(async {
                    send_batch("out", &mut output, &delivered, &telemetry)
                        .await
                        .expect("noop output never errors")
                })
            });
    }

    /// Always fails, matching `tests/allocations.rs`'s own -- the throughput counterpart to
    /// `send_batch_through_a_failing_output_disabled_telemetry`, added alongside it in the second
    /// round of review (every `send_batch` bench before this one only ever succeeded).
    struct FailingOutput;

    #[async_trait::async_trait]
    impl logit_pipeline::Output for FailingOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            Err(anyhow::anyhow!("simulated output failure"))
        }
    }

    #[divan::bench]
    fn send_batch_through_a_failing_output(bencher: Bencher) {
        let rt =
            tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
        let mut output = FailingOutput;
        let telemetry = Telemetry::default();
        bencher
            .with_inputs(|| Delivered::Owned(fixtures::nginx_batch(1), TraceContext::default()))
            .bench_local_values(|delivered| {
                rt.block_on(async {
                    drop(send_batch("out", &mut output, &delivered, &telemetry).await)
                })
            });
    }
}
