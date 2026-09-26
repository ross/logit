//! Throughput benches over the reference nginx pipeline. Run with `script/bench`.
//!
//! **Not** part of `script/cibuild`: wall-clock benchmarking on shared CI runners measures the
//! runner. The allocation numbers that must hold every build are assertions in
//! `tests/allocations.rs` instead.
//!
//! Almost every bench here calls decoders, transforms, and encoders **directly**, bypassing the
//! tokio runtime and the channels between nodes. That's a constraint, not a simplification:
//! `divan::AllocProfiler` only counts allocations on threads divan controls. The boundary is **no
//! cross-thread hop** (a `tokio::spawn`, a multi-thread runtime, a real OS thread), not "no
//! channel", which is what lets `mod runtime` below drive a real `tokio::sync::mpsc` channel and
//! still trust its allocation column: a `current_thread` runtime's `block_on` keeps everything on
//! the one thread divan is watching. See that module's doc and `docs/design/memory.md` §7. What a
//! full multi-node graph costs across the worker and OS threads `run_with_shutdown` spawns is
//! `logit-perf`'s job (`docs/adr/load-test-harness.md`), not a microbenchmark's.

use divan::{AllocProfiler, Bencher};
use logit_bench::fixtures;
use logit_outputs::influxdb::InfluxLineEncoder;
use logit_outputs::statsd::{Format as StatsdFormat, StatsdEncoder};
use logit_outputs::stdio::{EventDump, Format};
use logit_outputs::syslog::{Format as SyslogFormat, SyslogEncoder};
use logit_pipeline::Transform;
use logit_proto::{Decoder, Encoder, FramedEncoder, MessageBuf};
use logit_script::ScriptWorker;

/// divan's counting allocator, so every bench reports allocation count and bytes beside its
/// timing. The counting runs inside the timed region, so the timings are slightly pessimistic:
/// compare shapes with each other, don't quote them as throughput ceilings.
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

/// `generate_in`'s two render paths, called straight through `build_batch` with no runtime or
/// channel in between, so the allocation column is trustworthy. `tests/allocations.rs`'s
/// `generate_render_literal_100_events`/`generate_render_templated_100_events` pin the counts;
/// these two are their wall-clock view, and the gap between them is what two placeholders cost per
/// event.
#[divan::bench]
fn generate_render_literal(bencher: Bencher) {
    let mut input = fixtures::generate_literal();
    // The first call settles the render path and renders the prototype; keeping it out of the
    // timed region matches every other bench's warm-up here.
    drop(input.build_batch(0, 0, 100, 1));
    let mut batch_index = 0u64;
    bencher.bench_local(move || {
        batch_index += 1;
        input.build_batch(batch_index, batch_index * 100, 100, 2)
    });
}

#[divan::bench]
fn generate_render_templated(bencher: Bencher) {
    let mut input = fixtures::generate_templated();
    drop(input.build_batch(0, 0, 100, 1));
    let mut batch_index = 0u64;
    bencher.bench_local(move || {
        batch_index += 1;
        input.build_batch(batch_index, batch_index * 100, 100, 2)
    });
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
        .bench_local_refs(|event| json.process(&resource, event));
}

/// [`json_parse`] on the 28-key pino-shaped line (`fixtures::WIDE_JSON_SYSLOG_LINE`): the per-key
/// cost the parser's key cache targets, at a width where it dominates.
#[divan::bench]
fn json_parse_wide(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut json = fixtures::json_parser();
    let mut decoder = fixtures::syslog_decoder();
    let datagram = fixtures::wide_json_syslog_datagram(1);
    bencher
        .with_inputs(|| {
            decoder.decode(datagram.clone()).expect("should decode").events.pop().expect("an event")
        })
        .bench_local_refs(|event| json.process(&resource, event));
}

/// `logfmt` on `fixtures::LOGFMT_LINE` (nine fields, all zero-copy): the transform-level
/// counterpart of the `logfmt-parse` load-test scenario.
#[divan::bench]
fn logfmt_parse(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut logfmt = fixtures::logfmt_parser();
    bencher
        .with_inputs(fixtures::logfmt_event)
        .bench_local_refs(|event| logfmt.process(&resource, event));
}

/// `kv` on `fixtures::KV_LINE` (three `a=1&b=2` pairs).
#[divan::bench]
fn kv_parse(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut kv = fixtures::kv_parser();
    bencher.with_inputs(fixtures::kv_event).bench_local_refs(|event| kv.process(&resource, event));
}

/// `graphite_in`'s plaintext decode of one line carrying two carbon tags
/// (`fixtures::graphite_tagged_datagram`): the path interned per line, the two tag keys through
/// the decoder's key cache.
#[divan::bench]
fn graphite_decode_tagged(bencher: Bencher) {
    let datagram = fixtures::graphite_tagged_datagram();
    let mut decoder = fixtures::graphite_decoder();
    bencher.bench_local(|| decoder.decode(divan::black_box(datagram.clone())));
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
            let mut event = decoder
                .decode(datagram.clone())
                .expect("should decode")
                .events
                .pop()
                .expect("an event");
            assert!(json.process(&resource, &mut event), "json forwards");
            event
        })
        .bench_local_refs(|event| kv.process(&resource, event));
}

#[divan::bench]
fn keep(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut keep = fixtures::keep();
    bencher
        .with_inputs(fixtures::nginx_event)
        .bench_local_refs(|event| keep.process(&resource, event));
}

/// `shape` over the nginx shape (10 attributes, 4 metrics, a log body): the whole per-event path,
/// covering the recursive attribute walk, the `resolve` per key for its length, the key-set hash,
/// and the in-place rewrite into a measurement event (`docs/adr/shape-observer-component.md`).
#[divan::bench]
fn shape(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut shape = fixtures::shape();
    bencher
        .with_inputs(fixtures::nginx_event)
        .bench_local_refs(|event| shape.process(&resource, event));
}

#[divan::bench]
fn aggregate_absorb(bencher: Bencher) {
    let resource = fixtures::resource();
    let mut keep = fixtures::keep();
    let mut agg = fixtures::aggregator();
    bencher
        .with_inputs(|| {
            let mut event = fixtures::nginx_event();
            assert!(keep.process(&resource, &mut event), "keep forwards");
            event
        })
        .bench_local_refs(|event| agg.process(&resource, event));
}

/// One gauge absorbed with `groups` resource groups already open, each event under the next
/// resource in rotation: what `group_for`'s linear scan over groups costs as the count grows
/// (`docs/adr/aggregation-window-semantics.md`'s "The groups bound" section). Every group is
/// opened before timing starts, so the list has a fixed length.
///
/// The cost of one event depends on its resource's position in that list, so `sample_size` is a
/// multiple of every argument: each sample covers whole rotations and averages over every
/// position. With divan's adaptive sample size, a sample could cover only the cheap front of the
/// list.
#[divan::bench(args = [1, 100, 1000], sample_count = 100, sample_size = 1000)]
fn aggregate_absorb_with_groups(bencher: Bencher, groups: usize) {
    let resources = fixtures::resources_for_groups(groups);
    let prototype = fixtures::gauge_event_after_keep();
    let mut agg = fixtures::aggregator();
    for resource in &resources {
        let mut event = prototype.clone();
        agg.process(resource, &mut event);
        assert!(event.metrics.is_empty(), "the gauge is absorbed");
    }
    let mut next = 0;
    bencher.with_inputs(|| prototype.clone()).bench_local_refs(|event| {
        let resource = &resources[next];
        next = (next + 1) % resources.len();
        agg.process(resource, event)
    });
}

/// The interner's probes in isolation, on the six nginx keys cycled in order: what one key of one
/// event costs a parser that goes to the process-wide table (`intern_hit`, `lookup_hit`), an
/// encoder that goes back (`resolve`), and a parser that fronts the table with a
/// `logit_core::interner::KeyCache` (`key_cache_hit` for the in-order steady state,
/// `key_cache_resync` for a producer that reverses its key order every line, so every key is a
/// wrapping scan rather than a cursor hit). Single-threaded, so the shard lock is uncontended: the
/// pipeline pays more whenever two nodes probe at once.
mod interner {
    use super::*;
    use logit_core::interner::{self, intern, lookup, KeyCache, Symbol};

    const KEYS: [&str; 6] = [
        "host",
        "request_method",
        "status",
        "body_bytes_sent",
        "request_time",
        "upstream_response_time",
    ];

    #[divan::bench]
    fn intern_hit(bencher: Bencher) {
        for key in KEYS {
            intern(key);
        }
        let mut i = 0;
        bencher.bench_local(move || {
            i = (i + 1) % KEYS.len();
            intern(divan::black_box(KEYS[i]))
        });
    }

    #[divan::bench]
    fn lookup_hit(bencher: Bencher) {
        for key in KEYS {
            intern(key);
        }
        let mut i = 0;
        bencher.bench_local(move || {
            i = (i + 1) % KEYS.len();
            lookup(divan::black_box(KEYS[i]))
        });
    }

    #[divan::bench]
    fn resolve(bencher: Bencher) {
        let symbols: Vec<Symbol> = KEYS.iter().map(|key| intern(key)).collect();
        let mut i = 0;
        bencher.bench_local(move || {
            i = (i + 1) % symbols.len();
            interner::resolve(divan::black_box(symbols[i]))
        });
    }

    #[divan::bench]
    fn key_cache_hit(bencher: Bencher) {
        let mut cache = KeyCache::new();
        for key in KEYS {
            cache.get_or_intern(key);
        }
        let mut i = 0;
        bencher.bench_local(move || {
            i = (i + 1) % KEYS.len();
            cache.get_or_intern(divan::black_box(KEYS[i]))
        });
    }

    #[divan::bench]
    fn key_cache_resync(bencher: Bencher) {
        let mut cache = KeyCache::new();
        for key in KEYS {
            cache.get_or_intern(key);
        }
        let mut i = KEYS.len();
        bencher.bench_local(move || {
            i = (i + KEYS.len() - 1) % KEYS.len();
            cache.get_or_intern(divan::black_box(KEYS[i]))
        });
    }
}

/// `Event::clone` per fixture shape: what a mutating fan-out consumer pays per event when
/// `unwrap_batch`'s copy-on-write finds the batch still shared (`docs/design/memory.md` §3,
/// "The `Arc<EventBatch>` copy-on-write change").
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
        let mut dump = EventDump::new(Format::Human);
        bencher.bench_local(|| dump.encode(divan::black_box(&batch)));
    }

    #[divan::bench(args = [1, 100])]
    fn syslog(bencher: Bencher, events: usize) {
        let batch = fixtures::nginx_batch(events);
        let mut encoder = SyslogEncoder::new(SyslogFormat::Rfc5424, 16);
        let mut out = MessageBuf::default();
        bencher.bench_local(|| encoder.encode_into(divan::black_box(&batch), &mut out));
    }

    #[divan::bench(args = [1, 100])]
    fn statsd(bencher: Bencher, events: usize) {
        let batch = fixtures::statsd_batch(events);
        let mut encoder = StatsdEncoder::new(StatsdFormat::DogStatsd);
        let mut out = MessageBuf::default();
        bencher.bench_local(|| encoder.encode_into(divan::black_box(&batch), &mut out));
    }
}

/// `docs/design/lua-api.md`'s userdata proxy against the full table conversion it rejected.
/// `proxy` is what a script pays reading two attributes through `EventProxy`; `to_table` is what
/// the rejected design would cost on every event, whether or not the script touches an
/// attribute.
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

/// Direct-call benches for `logit_proto::buffer::InMemoryBuffer`, the sync buffer
/// `logit_pipeline::SinkQueue` wraps (`docs/adr/buffered-sink-delivery.md`). Called directly, never
/// through `SinkQueue` or tokio, for the module doc's no-cross-thread-hop reason.
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

    /// The worst case for `DropOldest`: the buffer sits at its bound (one slot, never committed),
    /// so every push evicts the current head. Isolates the eviction path's cost on top of the
    /// steady-state push/commit above.
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

/// The six survey-derived shapes (`docs/design/memory.md` §7, "The six survey-derived shapes"),
/// across the operations a sizing decision turns on: build, lookup (hit and miss), mutate (one
/// insert, one remove), **clone**, a 1000-event batch scan, and a native encode/decode.
/// `tests/allocations.rs`'s "Survey-derived shapes" section pins the allocation counts for the
/// same fixtures; this module is their wall-clock view.
///
/// **Run pinned** (`taskset -c 2`) on the perf VM. Unpinned runs on heterogeneous cores (Zen 5 and
/// 5c) are bimodal by about 2x (`docs/design/performance.md` §0), and recorded figures come from
/// the VM only.
///
/// Parameterized by shape, not width, because the shapes differ in more than width:
/// `pino_http_log` is narrower than `flat_json_log` and far more expensive to clone (four boxed
/// `Value::Map`s), and `collectd_3_record` is narrow enough to stay inline while its *metric* list
/// spills. A benchmark indexed by attribute count alone would miss both.
mod survey_shapes {
    use super::*;
    use logit_core::{Event, EventBatch, Value};
    use logit_proto::native::{NativeDecoder, NativeEncoder};

    /// Every shape. `hit_key` names an attribute each one carries, so `lookup_hit` measures a
    /// successful binary search rather than a miss in disguise.
    const SHAPES: [&str; 6] = [
        "flat_json_log",
        "pino_http_log",
        "access_log",
        "server_span",
        "collectd_3_record",
        "otlp_log_record",
    ];

    /// A key no shape carries, for `lookup_miss`. `AttrMap::get` doesn't intern
    /// (`docs/design/memory.md` §4), so a miss costs a failed interner *lookup* and never grows the
    /// table; a key never interned returns before the binary search.
    const MISSING_KEY: &str = "no.such.attribute.anywhere";

    fn hit_key(shape: &str) -> &'static str {
        match shape {
            "flat_json_log" => "status",
            "pino_http_log" => "reqId",
            "access_log" => "backend_type",
            "server_span" => "user_agent.original",
            "collectd_3_record" => "collectd.type",
            "otlp_log_record" => "thread.name",
            other => unreachable!("unknown shape {other}"),
        }
    }

    /// The fixture for `shape`, already through whatever parser produces it, so every bench below
    /// starts from the same event `tests/allocations.rs` measures.
    fn event(shape: &str) -> Event {
        let resource = fixtures::resource();
        let mut json = fixtures::json_parser();
        let mut parse = |mut event: Event| {
            assert!(json.process(&resource, &mut event), "json always forwards");
            event
        };
        match shape {
            "flat_json_log" => parse(fixtures::flat_json_log_event()),
            "pino_http_log" => parse(fixtures::pino_http_log_event()),
            "access_log" => parse(fixtures::access_log_event()),
            "server_span" => fixtures::wide_server_span_event(),
            "collectd_3_record" => fixtures::collectd_three_record_event(),
            "otlp_log_record" => {
                fixtures::enriched_resource_batch().events.into_iter().next().expect("5 events")
            }
            other => unreachable!("unknown shape {other}"),
        }
    }

    fn one_event_batch(shape: &str) -> EventBatch {
        EventBatch { resource: fixtures::resource(), scope: None, events: vec![event(shape)] }
    }

    /// Building the shape from scratch. For the three JSON-bodied shapes this is the real `json`
    /// transform over a `tail_in`-shaped event (the leg `docs/design/data-shapes.md` §5.3
    /// measured); for the other three it is the fixture's own `insert` loop, `AttrMap`'s
    /// O(k²)-bytes sorted insert with no parser in front of it.
    #[divan::bench(args = SHAPES)]
    fn build(bencher: Bencher, shape: &str) {
        let resource = fixtures::resource();
        let mut json = fixtures::json_parser();
        drop(event(shape)); // warm the key cache and the interner
        match shape {
            "flat_json_log" => bencher
                .with_inputs(fixtures::flat_json_log_event)
                .bench_local_refs(|event| json.process(&resource, event)),
            "pino_http_log" => bencher
                .with_inputs(fixtures::pino_http_log_event)
                .bench_local_refs(|event| json.process(&resource, event)),
            "access_log" => bencher
                .with_inputs(fixtures::access_log_event)
                .bench_local_refs(|event| json.process(&resource, event)),
            "server_span" => bencher.bench_local(fixtures::wide_server_span_event),
            "collectd_3_record" => bencher.bench_local(fixtures::collectd_three_record_event),
            "otlp_log_record" => bencher.bench_local(fixtures::enriched_resource_batch),
            other => unreachable!("unknown shape {other}"),
        }
    }

    #[divan::bench(args = SHAPES)]
    fn lookup_hit(bencher: Bencher, shape: &str) {
        let event = event(shape);
        let key = hit_key(shape);
        assert!(event.attributes.get(key).is_some(), "{shape}: {key} should be present");
        bencher.bench_local(|| divan::black_box(&event).attributes.get(divan::black_box(key)));
    }

    #[divan::bench(args = SHAPES)]
    fn lookup_miss(bencher: Bencher, shape: &str) {
        let event = event(shape);
        bencher
            .bench_local(|| divan::black_box(&event).attributes.get(divan::black_box(MISSING_KEY)));
    }

    /// One `insert` into an already-built map: a `set`/`trace_context`/`regex`-shaped mutation.
    /// The map is rebuilt per iteration (outside the timed region), so the insert is always the
    /// (k+1)th, never an overwrite of the previous iteration's.
    #[divan::bench(args = SHAPES)]
    fn insert_one(bencher: Bencher, shape: &str) {
        bencher.with_inputs(|| event(shape)).bench_local_refs(|event| {
            event.attributes.insert("bench.inserted", Value::I64(1));
        });
    }

    /// One `remove`: `keep`/`remove`'s per-attribute cost, an O(k) `Vec::remove` after the same
    /// binary search `lookup_hit` measures.
    #[divan::bench(args = SHAPES)]
    fn remove_one(bencher: Bencher, shape: &str) {
        let key = hit_key(shape);
        bencher.with_inputs(|| event(shape)).bench_local_refs(|event| {
            event.attributes.remove(divan::black_box(key));
        });
    }

    /// What one extra fan-out branch costs. `docs/design/data-shapes.md` §6: VRL's own
    /// flat-versus-tree crossover moved from about 128 fields to about 16 once the benchmark
    /// cloned, so this is the bench the sizing arms are judged on, not `build`.
    #[divan::bench(args = SHAPES)]
    fn clone(bencher: Bencher, shape: &str) {
        let event = event(shape);
        bencher.bench_local(|| divan::black_box(&event).clone());
    }

    /// A 1000-event batch (`receive.batch_max_events`' default) scanned end to end, reading one
    /// attribute per event: the cache-density cost `size_of::<Event>()` moves and `AttrMap::get`
    /// alone doesn't. At 864 bytes an `Event`, this walks 864 KB per iteration.
    #[divan::bench(args = SHAPES)]
    fn scan_1000(bencher: Bencher, shape: &str) {
        let key = hit_key(shape);
        let one = event(shape);
        let events: Vec<Event> = (0..1000).map(|_| one.clone()).collect();
        bencher.bench_local(|| {
            let mut found = 0usize;
            for event in divan::black_box(&events) {
                if event.attributes.get(key).is_some() {
                    found += 1;
                }
            }
            found
        });
    }

    #[divan::bench(args = SHAPES)]
    fn native_encode(bencher: Bencher, shape: &str) {
        let batch = one_event_batch(shape);
        let mut encoder = NativeEncoder::default();
        drop(encoder.encode(&batch));
        bencher.bench_local(|| encoder.encode(divan::black_box(&batch)));
    }

    /// The decode side. `read_attr_map_at` (`crates/logit-proto/src/native/value.rs`) reads the
    /// exact attribute count off the wire but doesn't reserve with it, then rebuilds the map by k
    /// sorted `insert_sym`s in the *writer's* symbol order, which dictionary remapping can leave
    /// unsorted for the reader: the O(k²)-bytes build with no parser in front of it.
    #[divan::bench(args = SHAPES)]
    fn native_decode(bencher: Bencher, shape: &str) {
        let batch = one_event_batch(shape);
        let mut encoder = NativeEncoder::default();
        let framed = encoder.encode(&batch).expect("should encode");
        let mut decoder = NativeDecoder;
        let mut warm = Vec::new();
        drop(decoder.decode_into(framed.clone(), 0, &mut warm));
        bencher.bench_local(|| {
            let mut events = Vec::new();
            drop(decoder.decode_into(divan::black_box(framed.clone()), 0, &mut events));
            events
        });
    }
}

/// The batch-level half of the survey shapes: five events sharing a 17-attribute `Resource`
/// (`docs/design/data-shapes.md` §3's median collector batch carrying §4's median resource), and
/// the runtime paths a batch of any shape goes through.
///
/// What is reachable here and what is not:
///
/// - **`retain_mut`**: reachable, as `logit_pipeline::process_batch`, which *is* the
///   `events.retain_mut(...)` path (ADR `in-place-transform-process`). Benched below.
/// - **`route_batch`**: reachable, a plain synchronous call. Benched below.
/// - **`drain_inbox`**: reachable but not usefully benchable per iteration. It is a loop that
///   returns only when its inbox closes (`crates/logit-pipeline/src/runtime.rs`), so every
///   iteration would build a fresh `mpsc` channel and `SinkStore`, drop the sender, and measure
///   mostly that setup. `tests/allocations.rs`'s
///   `drain_inbox_single_consumer_owned_batch_costs_exactly_the_arc` counts its allocations
///   instead, where the one-shot shape doesn't distort the measurement.
/// - **`Fanout::send`'s copy-on-write clone**: benched in `mod runtime` below
///   (`fanout_send_two_consumers`), on the nginx shape.
mod survey_batch {
    use super::*;
    use logit_core::{EventBatch, Telemetry};
    use logit_pipeline::{process_batch, unwrap_batch, BatchContext, Delivered, RouterScratch};
    use std::sync::Arc;

    /// `EventBatch::clone` over the measured collector batch: five 9-attribute events (one slot
    /// past inline, so every one of them spills) and a 17-attribute `Resource` that is
    /// `Arc`-shared and therefore *not* copied. `tests/allocations.rs`'s
    /// `clone_enriched_resource_batch` pins that asymmetry.
    #[divan::bench]
    fn clone_enriched_batch(bencher: Bencher) {
        let batch = fixtures::enriched_resource_batch();
        bencher.bench_local(|| divan::black_box(&batch).clone());
    }

    /// The same clone reached the way the runtime reaches it: `unwrap_batch` on a contended
    /// `Delivered::Shared`, which is what a two-mutating-consumer fan-out always pays
    /// (`docs/design/memory.md` §3).
    #[divan::bench]
    fn unwrap_contended_enriched_batch(bencher: Bencher) {
        let shared = Arc::new(fixtures::enriched_resource_batch());
        bencher.bench_local(|| {
            let _keep_alive = Arc::clone(&shared);
            unwrap_batch(Delivered::Shared(Arc::clone(&shared), BatchContext::default()))
        });
    }

    fn wide_batch(count: usize) -> EventBatch {
        let resource = fixtures::resource();
        let mut json = fixtures::json_parser();
        let mut event = fixtures::access_log_event();
        assert!(json.process(&resource, &mut event), "json always forwards");
        EventBatch { resource, scope: None, events: (0..count).map(|_| event.clone()).collect() }
    }

    /// `process_batch`, the `Vec::retain_mut` path every transform node runs, over 100 of the
    /// widest survey shape, through `keep`. `keep` rebuilds the map, so this shows a 30-entry
    /// `AttrMap`'s per-event cost at batch scale.
    #[divan::bench]
    fn process_batch_100_access_logs(bencher: Bencher) {
        let mut keep = fixtures::keep();
        let telemetry = Telemetry::default();
        bencher
            .with_inputs(|| wide_batch(100))
            .bench_local_values(|batch| process_batch(&mut keep, batch, &telemetry));
    }

    /// `route_batch`'s partition-and-move pass over the same 100 wide events: one
    /// `size_of::<Event>()` move per event (`docs/design/memory.md` §3's routing section), the
    /// batch-level cost that scales with `Event`'s size rather than its allocation count.
    #[divan::bench]
    fn route_batch_100_access_logs(bencher: Bencher) {
        let mut router = logit_transforms::Route::new(
            logit_config::RouteBy::Attribute("backend_type".to_string()),
            &[("client backend".to_string(), "backends".to_string())].into_iter().collect(),
            &["backends".to_string()],
        );
        let mut scratch = RouterScratch::new(1);
        let telemetry = Telemetry::default();
        bencher.with_inputs(|| wide_batch(100)).bench_local_values(|batch| {
            logit_pipeline::runtime::route_batch(&mut router, &mut scratch, batch, &telemetry)
        });
    }
}

/// Decode through aggregation for one access-log line: the number that bounds ingest throughput
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
        for mut event in batch.events {
            assert!(json.process(&resource, &mut event), "json forwards");
            assert!(kv.process(&resource, &mut event), "kv forwards");
            assert!(keep.process(&resource, &mut event), "keep forwards");
            agg.process(&resource, &mut event);
        }
    });
}

/// The node-runtime paths `tests/allocations.rs`'s "Runtime" section pins by exact allocation
/// count; this module is their wall-clock view.
///
/// `fanout_send_one_consumer`/`fanout_send_two_consumers`/`send_batch_through_a_noop_output` are
/// the exceptions to the module doc's direct-call rule: they cross a `tokio::sync::mpsc` channel
/// (the fanout pair) or call through `#[async_trait]` (`send_batch`). Their allocation column is
/// still trustworthy, for the reason the module doc gives: nothing calls `tokio::spawn`, so
/// nothing leaves the one thread divan is watching. Cross-check this module's allocation column
/// against `tests/allocations.rs`'s `fanout_send_one_consumer_costs_nothing`,
/// `fanout_send_two_consumers_costs_one_clone_plus_one_arc`, and
/// `send_batch_through_a_noop_output_disabled_telemetry`, which use the identical construction.
mod runtime {
    use super::*;
    use logit_core::{EventBatch, Telemetry};
    use logit_pipeline::{
        process_batch, send_batch, unwrap_batch, BatchContext, Delivered, Fanout,
    };

    /// `run_transform`'s per-batch body (`logit_pipeline::process_batch`), a plain synchronous call
    /// with no channel or runtime.
    #[divan::bench]
    fn process_batch_through_keep(bencher: Bencher) {
        let mut keep = fixtures::keep();
        let telemetry = Telemetry::default();
        bencher
            .with_inputs(|| fixtures::nginx_batch(1))
            .bench_local_values(|batch| process_batch(&mut keep, batch, &telemetry));
    }

    /// One consumer, the common case (every listener's first hop, every interior edge of a linear
    /// chain), which allocates nothing per `tests/allocations.rs`'s
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
    /// other doesn't. `tests/allocations.rs`'s
    /// `fanout_send_two_consumers_costs_one_clone_plus_one_arc` has the accounting this bench's
    /// allocation column should match.
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

    /// A no-op `Output`, matching `tests/allocations.rs`'s, so `send_batch`'s own accounting is
    /// isolated from any real sink's encode/write cost.
    struct NoopOutput;

    #[async_trait::async_trait]
    impl logit_pipeline::Output for NoopOutput {
        async fn send(&mut self, _batch: &EventBatch) -> anyhow::Result<()> {
            Ok(())
        }
    }

    /// `run_output`'s per-batch body (`logit_pipeline::send_batch`), as
    /// `process_batch_through_keep` is `run_transform`'s.
    #[divan::bench]
    fn send_batch_through_a_noop_output(bencher: Bencher) {
        let rt =
            tokio::runtime::Builder::new_current_thread().build().expect("runtime should build");
        let mut output = NoopOutput;
        let telemetry = Telemetry::default();
        bencher
            .with_inputs(|| Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default()))
            .bench_local_values(|delivered| {
                rt.block_on(async {
                    send_batch("out", &mut output, &delivered, &telemetry)
                        .await
                        .expect("noop output never errors")
                })
            });
    }

    /// Always fails, matching `tests/allocations.rs`'s: `send_batch`'s error path, the wall-clock
    /// counterpart to `send_batch_through_a_failing_output_disabled_telemetry`.
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
            .with_inputs(|| Delivered::Owned(fixtures::nginx_batch(1), BatchContext::default()))
            .bench_local_values(|delivered| {
                rt.block_on(async {
                    drop(send_batch("out", &mut output, &delivered, &telemetry).await)
                })
            });
    }
}
