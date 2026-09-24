//! Throughput and encoded-size benches for the native-wire-format bake-off
//! (`docs/design/wire-protocol.md`'s "Encoding: decided — hand-rolled",
//! `docs/adr/native-wire-format-encoding.md`). Run with `script/bench wire_format`.
//!
//! Every arm is held to the fidelity gate in `crates/logit-bench/tests/wire_format_bakeoff.rs`
//! first (the OTLP control modulo its named degradations); a fast, lossy codec isn't a candidate.
//! Two shapes: [`Shape::NginxMixed`], the reference
//! mixed workload, and [`Shape::DistributionHeavy`], the one that most directly exercises the
//! sketch-carrying claim this format exists to make. Each runs at three batch sizes, because a
//! dictionary amortizes across a batch (`docs/design/wire-protocol.md`): a 1-event batch is every
//! dictionary-based arm's worst case and a 1000-event batch its best. The fidelity gate's
//! `representative_batches()` also covers logs-only, wide-JSON, and span shapes; they are left out
//! here to keep a `script/bench` run short and its output readable.
//!
//! **Encoded size** is reported beside wall-clock: `encoded_bytes` prints sizes through
//! `println!`, because divan has no "report a number, not a duration" mode. Read its stdout rows,
//! not its time column.

use divan::{AllocProfiler, Bencher};
use logit_bench::{bakeoff, fixtures};
use logit_core::{Event, EventBatch, Resource};
use std::sync::Arc;

#[global_allocator]
static ALLOC: AllocProfiler = AllocProfiler::system();

fn main() {
    divan::main();
}

#[derive(Debug, Clone, Copy)]
enum Shape {
    NginxMixed,
    DistributionHeavy,
}

impl Shape {
    fn batch(self, count: usize) -> EventBatch {
        match self {
            Shape::NginxMixed => fixtures::nginx_batch(count),
            Shape::DistributionHeavy => repeat(fixtures::distribution_heavy_event, count),
        }
    }
}

fn repeat(build: impl Fn() -> Event, count: usize) -> EventBatch {
    EventBatch {
        resource: Arc::new(Resource::default()),
        scope: None,
        events: (0..count).map(|_| build()).collect(),
    }
}

const SHAPES: [Shape; 2] = [Shape::NginxMixed, Shape::DistributionHeavy];
const SIZES: [usize; 3] = [1, 100, 1000];

// -- native ---------------------------------------------------------------------------------------

#[divan::bench(args = SHAPES)]
fn native_encode_1(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(1);
    bencher.bench_local(|| bakeoff::native_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn native_encode_100(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(100);
    bencher.bench_local(|| bakeoff::native_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn native_encode_1000(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(1000);
    bencher.bench_local(|| bakeoff::native_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn native_decode_100(bencher: Bencher, shape: Shape) {
    let framed = bakeoff::native_encode(&shape.batch(100));
    bencher.bench_local(|| bakeoff::native_decode(divan::black_box(framed.clone())));
}

// -- otlp (control arm) ----------------------------------------------------------------------------

#[divan::bench(args = SHAPES)]
fn otlp_encode_1(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(1);
    bencher.bench_local(|| bakeoff::otlp_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn otlp_encode_100(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(100);
    bencher.bench_local(|| bakeoff::otlp_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn otlp_encode_1000(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(1000);
    bencher.bench_local(|| bakeoff::otlp_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn otlp_decode_100(bencher: Bencher, shape: Shape) {
    let payloads = bakeoff::otlp_encode(&shape.batch(100));
    bencher.bench_local(|| {
        let mut decoder = logit_proto::otlp::OtlpDecoder::new();
        for (signal, bytes) in divan::black_box(payloads.clone()) {
            let _ = logit_proto::SignalDecoder::decode_signal(&mut decoder, signal, bytes);
        }
    });
}

// -- rkyv -------------------------------------------------------------------------------------------

#[divan::bench(args = SHAPES)]
fn rkyv_encode_1(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(1);
    bencher.bench_local(|| bakeoff::rkyv_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn rkyv_encode_100(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(100);
    bencher.bench_local(|| bakeoff::rkyv_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn rkyv_encode_1000(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(1000);
    bencher.bench_local(|| bakeoff::rkyv_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn rkyv_decode_100(bencher: Bencher, shape: Shape) {
    let bytes = bakeoff::rkyv_encode(&shape.batch(100));
    bencher.bench_local(|| bakeoff::rkyv_decode(divan::black_box(&bytes)));
}

// -- postcard ---------------------------------------------------------------------------------------

#[divan::bench(args = SHAPES)]
fn postcard_encode_1(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(1);
    bencher.bench_local(|| bakeoff::postcard_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn postcard_encode_100(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(100);
    bencher.bench_local(|| bakeoff::postcard_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn postcard_encode_1000(bencher: Bencher, shape: Shape) {
    let batch = shape.batch(1000);
    bencher.bench_local(|| bakeoff::postcard_encode(divan::black_box(&batch)));
}

#[divan::bench(args = SHAPES)]
fn postcard_decode_100(bencher: Bencher, shape: Shape) {
    let bytes = bakeoff::postcard_encode(&shape.batch(100));
    bencher.bench_local(|| bakeoff::postcard_decode(divan::black_box(&bytes)));
}

// -- encoded size, every arm, every shape and size ---------------------------------------------------
//
// Not a timing bench: each call prints one size-table row per batch size to stdout, the
// print-a-number pattern this crate's allocation tests use under `--no-capture`. divan still calls
// it once per timed iteration, so the (deterministic) rows repeat. Its time column means nothing.

#[divan::bench(args = SHAPES)]
fn encoded_bytes(shape: Shape) {
    for &size in &SIZES {
        let batch = shape.batch(size);
        let native = bakeoff::native_encode(&batch).len();
        let native_lz4 = {
            let mut encoder =
                logit_proto::native::NativeEncoder::new(logit_proto::frame::Compression::Lz4);
            logit_proto::Encoder::encode(&mut encoder, &batch).expect("native lz4 encode").len()
        };
        let otlp: usize = bakeoff::otlp_encode(&batch).iter().map(|(_, b)| b.len()).sum();
        let rkyv = bakeoff::rkyv_encode(&batch).len();
        let postcard = bakeoff::postcard_encode(&batch).len();
        println!(
            "encoded_bytes shape={shape:?} size={size:>4} native={native:>8} native_lz4={native_lz4:>8} otlp={otlp:>8} rkyv={rkyv:>8} postcard={postcard:>8}"
        );
    }
}
