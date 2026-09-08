//! Throughput and encoded-size benches for the native-wire-format bake-off
//! (`docs/design/wire-protocol.md`'s "Encoding: decide with a benchmark, not up front",
//! `docs/adr/native-wire-format-encoding.md`). Run with `script/bench wire_format`.
//!
//! Every arm here already passed the fidelity gate in
//! `crates/logit-bench/tests/wire_format_bakeoff.rs` -- a fast, lossy codec isn't measured here at
//! all. Two representative shapes ([`Shape::NginxMixed`], the reference mixed workload, and
//! [`Shape::DistributionHeavy`], the shape that most directly exercises the sketch-carrying claim
//! this format exists to make) at three batch sizes each, per
//! `docs/design/memory.md` §0's "don't generalize a measurement from one event shape" and
//! `docs/design/wire-protocol.md`'s own note that a dictionary amortizes across a batch, so a
//! 1-event batch is every dictionary-based arm's worst case and a 1000-event batch its best. The
//! fidelity gate's `representative_batches()` covers the other three shapes (logs-only, wide-JSON,
//! span) for correctness; narrowed here to keep this bench's run time and output reasonable for a
//! `script/bench` invocation someone actually reads.
//!
//! **Encoded size**, not just wall-clock, is reported: `*_encoded_bytes` benches do no timing at
//! all (`Bencher::bench_local` isn't used) -- they run once and report through `println!`, since
//! divan has no built-in "report a number, not a duration" mode and this crate's own conventions
//! (`docs/design/memory.md`) favor exact, reproducible measurements over inventing a fake timing
//! column for a non-time quantity.

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
// Not a timing bench: runs once (`#[divan::bench]` with no `Bencher` parameter runs the function
// body directly, once per registration) and prints a size table row. `script/bench wire_format`'s
// own stdout is the report; there's no divan column for "bytes produced", so this is the
// established `--no-capture` pattern this crate's allocation tests already use for a
// non-timing number.

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
