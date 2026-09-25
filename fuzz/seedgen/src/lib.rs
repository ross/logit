//! Builds the committed starting seeds under `fuzz/seeds/<target>/` from `testdata/interop/` and
//! encoder round trips, so every fuzz target starts from inputs its decoder accepts.
//!
//! A target that takes a selector byte gets it prepended, matching the target's own doc: the
//! OTLP targets' signal (`0` logs, `1` metrics, `2` traces), `prom_decompress`'s encoding, and
//! `prom_remote_write`'s version. [`generate`] is deterministic, so a rerun rewrites the same
//! bytes and leaves `git status` clean.

use bytes::Bytes;
use logit_core::interner::intern;
use logit_core::{DdSketch, EventBatch, HyperLogLog, Mapping, Provenance};
use logit_proto::frame::{write_frame, write_frame_with_flags, Compression, FLAG_CONTROL};
use logit_proto::native::control::{Ack, ControlMessage, Hello, HelloAck, Reject};
use logit_proto::native::{encode_batch, encode_batch_v2, CODEC_NATIVE_V1, CODEC_NATIVE_V2};
use logit_proto::otlp::{OtlpDecoder, OtlpEncoder};
use logit_proto::prometheus::compression::{decompress_bounded, Encoding};
use logit_proto::prometheus::remote_write::Version;
use logit_proto::{Signal, SignalEncoder};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;

/// A seed larger than this is left out: libFuzzer's `-max_len` would truncate it anyway, and the
/// seeds are committed.
pub const MAX_SEED_BYTES: usize = 256 * 1024;

/// Seeds by target, then by file name.
pub type Seeds = BTreeMap<&'static str, BTreeMap<String, Vec<u8>>>;

const SIGNALS: [(Signal, &str, u8); 3] =
    [(Signal::Logs, "logs", 0), (Signal::Metrics, "metrics", 1), (Signal::Traces, "traces", 2)];

/// Every seed, keyed by target. `testdata` is the repository's `testdata/` directory. Seeds over
/// [`MAX_SEED_BYTES`] are dropped and named in the second return value.
pub fn generate(testdata: &Path) -> std::io::Result<(Seeds, Vec<String>)> {
    let mut seeds = Seeds::new();
    let mut add = |target: &'static str, name: String, bytes: Vec<u8>| {
        seeds.entry(target).or_default().insert(name, bytes);
    };

    let mut batches: Vec<(String, EventBatch)> = Vec::new();
    for (signal, label, selector) in SIGNALS {
        // Newline-delimited: one export request per line (testdata/interop/otlp/README.md).
        let file = std::fs::read(testdata.join(format!("interop/otlp/{label}.json")))?;
        let lines = file.split(|&b| b == b'\n').filter(|line| !line.is_empty());
        for (line_no, json) in lines.enumerate() {
            let name = format!("{label}-{line_no}");
            add("otlp_json", name.clone(), prefixed(selector, json));
            let decoded = OtlpDecoder::new()
                .decode_signal_json(signal, Bytes::copy_from_slice(json))
                .map_err(|e| std::io::Error::other(format!("interop/otlp/{name}: {e}")))?;
            for (i, mut batch) in decoded.into_iter().enumerate() {
                // The OTLP encoder stamps a zero `observed_timestamp` with the wall clock; the
                // event's own timestamp keeps the seed stable.
                for event in &mut batch.events {
                    if let Some(log) = event.log.as_mut().filter(|l| l.observed_timestamp == 0) {
                        log.observed_timestamp = event.timestamp;
                    }
                }
                batches.push((format!("{name}-{i}"), batch));
            }
        }
    }

    let mut stream = Vec::new();
    for (name, batch) in &batches {
        let payloads = OtlpEncoder::new()
            .encode_signals(batch)
            .map_err(|e| std::io::Error::other(format!("encoding {name}: {e}")))?;
        for (signal, body) in payloads {
            let selector = SIGNALS.iter().find(|(s, _, _)| *s == signal).map(|s| s.2).unwrap();
            add("otlp_proto", name.clone(), prefixed(selector, &body));
            add("otlp_grpc", format!("{name}-plain"), prefixed(selector, &grpc_frame(0, &body)));
            add(
                "otlp_grpc",
                format!("{name}-gzip"),
                prefixed(selector, &grpc_frame(1, &gzip(&body))),
            );
        }

        let v1 = encode_batch(batch);
        let provenance =
            Provenance { origin: Some(intern("seed_in")), previous: Some(intern("seed_enrich")) };
        let v2 = encode_batch_v2(batch, provenance);
        for (codec, payload, version) in
            [(CODEC_NATIVE_V1, &v1, "v1"), (CODEC_NATIVE_V2, &v2, "v2")]
        {
            for (compression, label) in [(Compression::None, "none"), (Compression::Lz4, "lz4")] {
                let frame = write_frame(codec, compression, payload).expect("None and Lz4 encode");
                stream.extend_from_slice(&frame);
                add("native_frame", format!("{name}-{version}-{label}"), frame.to_vec());
            }
        }
        add("native_batch_v1", name.clone(), v1.to_vec());
        add("native_batch_v2", name.clone(), v2.to_vec());
    }

    let controls = [
        (
            "hello",
            ControlMessage::Hello(Hello {
                version: 1,
                codecs: vec![CODEC_NATIVE_V2, CODEC_NATIVE_V1],
                compressions: vec![Compression::Lz4 as u8, Compression::None as u8],
                max_frame_bytes: 16 << 20,
                window: 1,
            }),
        ),
        (
            "hello-ack",
            ControlMessage::HelloAck(HelloAck {
                version: 1,
                codec: CODEC_NATIVE_V2,
                compression: Compression::Lz4 as u8,
                max_frame_bytes: 16 << 20,
                window: 1,
            }),
        ),
        ("ack", ControlMessage::Ack(Ack { seq: 42 })),
        ("reject", ControlMessage::Reject(Reject { code: 1, message: "no common codec".into() })),
    ];
    for (name, message) in controls {
        let body = message.encode();
        let frame = write_frame_with_flags(0, Compression::None, FLAG_CONTROL, &body)
            .expect("None encodes");
        stream.extend_from_slice(&frame);
        add("native_control", name.to_string(), body.to_vec());
    }
    add("native_frame", "stream".to_string(), stream);

    let mut sketches = BTreeMap::new();
    for (mapping, sketch_label) in [(None, "default"), (Some(Mapping::agent()), "agent")] {
        for count in [0usize, 1, 1000] {
            let mut sketch = match &mapping {
                Some(m) => DdSketch::with_mapping(m.clone()),
                None => DdSketch::new(),
            };
            for i in 0..count {
                // Negative, zero, and positive values, so both stores and the zero count fill.
                sketch.add(i as f64 * 1.5 - 100.0);
            }
            let bytes = sketch.to_bytes();
            add("sketch_bytes", format!("{sketch_label}-{count}"), bytes.clone());
            sketches.insert(format!("{sketch_label}-{count}"), bytes);
        }
    }
    for (a, b) in
        [("default-1000", "agent-1000"), ("agent-1", "agent-1000"), ("default-0", "default-1")]
    {
        let (a_bytes, b_bytes) = (&sketches[a], &sketches[b]);
        let mut seed = (a_bytes.len() as u16).to_be_bytes().to_vec();
        seed.extend_from_slice(a_bytes);
        seed.extend_from_slice(b_bytes);
        add("sketch_merge", format!("{a}+{b}"), seed);
    }

    for count in [0usize, 1, 50, 100_000] {
        let mut hll = HyperLogLog::new();
        for i in 0..count {
            hll.insert(format!("member-{i}").as_bytes());
        }
        add("hll_bytes", format!("members-{count}"), hll.to_bytes());
    }

    let mut captures: Vec<_> = std::fs::read_dir(testdata.join("interop/prometheus"))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "bin"))
        .collect();
    captures.sort();
    for path in captures {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        let body = std::fs::read(&path)?;
        let headers = std::fs::read_to_string(path.with_extension("headers"))?;
        let header = |name: &str| {
            headers.lines().find_map(|line| {
                let (key, value) = line.split_once(':')?;
                key.trim().eq_ignore_ascii_case(name).then(|| value.trim().to_string())
            })
        };
        let encoding = header("content-encoding")
            .and_then(|v| Encoding::from_header(&v))
            .ok_or_else(|| std::io::Error::other(format!("{stem}: no remote-write encoding")))?;
        let version = header("content-type")
            .and_then(|v| Version::from_content_type(&v))
            .ok_or_else(|| std::io::Error::other(format!("{stem}: no remote-write version")))?;
        let encoding_selector = match encoding {
            Encoding::Snappy => 0,
            Encoding::Zstd => 1,
        };
        add("prom_decompress", stem.clone(), prefixed(encoding_selector, &body));
        let decompressed = decompress_bounded(encoding, &body, MAX_SEED_BYTES)
            .map_err(|e| std::io::Error::other(format!("{stem}: {e}")))?;
        let version_selector = match version {
            Version::V1 => 0,
            Version::V2 => 1,
        };
        add("prom_remote_write", stem, prefixed(version_selector, &decompressed));
    }

    let mut skipped = Vec::new();
    for (target, files) in seeds.iter_mut() {
        files.retain(|name, bytes| {
            let keep = bytes.len() <= MAX_SEED_BYTES;
            if !keep {
                skipped.push(format!("{target}/{name} ({} bytes)", bytes.len()));
            }
            keep
        });
    }
    Ok((seeds, skipped))
}

fn prefixed(selector: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + body.len());
    out.push(selector);
    out.extend_from_slice(body);
    out
}

/// One gRPC length-prefixed message (`logit_proto::otlp::grpc`'s module doc).
fn grpc_frame(flag: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![flag];
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// flate2's gzip header carries an mtime of 0 unless one is set, so the output is stable.
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(bytes).expect("writing to a Vec never fails");
    encoder.finish().expect("writing to a Vec never fails")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn testdata() -> std::path::PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testdata")
    }

    #[test]
    fn two_runs_produce_identical_bytes() {
        let (first, _) = generate(&testdata()).unwrap();
        let (second, _) = generate(&testdata()).unwrap();
        let names = |seeds: &Seeds| -> Vec<String> {
            seeds
                .iter()
                .flat_map(|(t, files)| files.keys().map(move |n| format!("{t}/{n}")))
                .collect()
        };
        assert_eq!(names(&first), names(&second));
        let differing: Vec<_> = first
            .iter()
            .flat_map(|(t, files)| files.iter().map(move |(n, b)| (t, n, b)))
            .filter(|(t, n, b)| second[*t][*n] != **b)
            .map(|(t, n, _)| format!("{t}/{n}"))
            .collect();
        assert!(differing.is_empty(), "seeds that differ between runs: {differing:?}");
    }

    #[test]
    fn every_target_gets_at_least_one_seed() {
        let (seeds, skipped) = generate(&testdata()).unwrap();
        assert!(skipped.is_empty(), "skipped: {skipped:?}");
        let targets: Vec<_> = seeds.keys().copied().collect();
        assert_eq!(
            targets,
            [
                "hll_bytes",
                "native_batch_v1",
                "native_batch_v2",
                "native_control",
                "native_frame",
                "otlp_grpc",
                "otlp_json",
                "otlp_proto",
                "prom_decompress",
                "prom_remote_write",
                "sketch_bytes",
                "sketch_merge",
            ]
        );
    }
}
