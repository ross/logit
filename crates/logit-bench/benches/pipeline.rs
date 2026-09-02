//! Throughput benches over the reference nginx pipeline. Run with `script/bench`.
//!
//! Deliberately **not** part of `script/cibuild`: wall-clock benchmarking on shared CI runners
//! measures the runner. The allocation numbers that *do* need to hold every build are assertions
//! in `tests/allocations.rs` instead.
//!
//! Every bench calls decoders, transforms, and encoders **directly** rather than driving the tokio
//! runtime and the channels between nodes. That's a constraint, not a simplification:
//! `divan::AllocProfiler` only counts allocations on threads Divan controls, so anything measured
//! across a channel hop would report allocation numbers that are quietly wrong. What a full
//! multi-node graph costs in wall-clock terms is a separate question, and needs a load generator
//! rather than a microbenchmark -- see `docs/design/memory.md`.

use divan::{AllocProfiler, Bencher};
use logit_bench::fixtures;
use logit_outputs::influxdb::InfluxLineEncoder;
use logit_outputs::stdio::{EventDump, Format};
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
