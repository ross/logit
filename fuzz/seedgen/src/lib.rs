//! Builds the committed starting seeds under `fuzz/seeds/<target>/` from `testdata/interop/` and
//! encoder round trips, so every fuzz target starts from inputs its decoder accepts.
//!
//! A target that takes a selector byte gets it prepended, matching the target's own doc: the
//! OTLP targets' signal (`0` logs, `1` metrics, `2` traces), `prom_decompress`'s encoding,
//! `prom_remote_write`'s version, `stream_framing`'s four bytes of mode, bound, and chunking,
//! and `syslog`'s line splitting (`1` on, `0` off). [`generate`] is deterministic, so a rerun
//! rewrites the same bytes and leaves `git status` clean.

use bytes::{Bytes, BytesMut};
use logit_core::interner::intern;
use logit_core::{DdSketch, EventBatch, HyperLogLog, Mapping, Provenance};
use logit_proto::frame::{write_frame, write_frame_with_flags, Compression, FLAG_CONTROL};
use logit_proto::native::control::{
    Ack, AckStatus, ControlMessage, Hello, HelloAck, Reject, ACK_REJECTED_DECODE_BUDGET,
};
use logit_proto::native::varint::write_uvarint;
use logit_proto::native::{encode_batch, encode_hop_batch, SeqId, CODEC_BATCH, CODEC_HOP_BATCH};
use logit_proto::otlp::{OtlpDecoder, OtlpEncoder};
use logit_proto::prometheus::compression::{decompress_bounded, Encoding};
use logit_proto::prometheus::remote_write::Version;
use logit_proto::proxy::V2_SIGNATURE;
use logit_proto::{Signal, SignalEncoder, SignalPayload};
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
    for (i, (name, batch)) in batches.iter().enumerate() {
        let payloads = OtlpEncoder::new()
            .encode_signals(batch)
            .map_err(|e| std::io::Error::other(format!("encoding {name}: {e}")))?;
        for SignalPayload { signal, bytes: body, .. } in payloads {
            let selector = SIGNALS.iter().find(|(s, _, _)| *s == signal).map(|s| s.2).unwrap();
            add("otlp_proto", name.clone(), prefixed(selector, &body));
            add("otlp_grpc", format!("{name}-plain"), prefixed(selector, &grpc_frame(0, &body)));
            add(
                "otlp_grpc",
                format!("{name}-gzip"),
                prefixed(selector, &grpc_frame(1, &gzip(&body))),
            );
        }

        let bare = encode_batch(batch);
        let provenance =
            Provenance { origin: Some(intern("seed_in")), previous: Some(intern("seed_enrich")) };
        let seq = SeqId { id: *b"seed-sender-id16", seq: 1 + i as u64 };
        let hop = encode_hop_batch(batch, provenance, seq);
        for (codec, payload, shape) in
            [(CODEC_BATCH, &bare, "batch"), (CODEC_HOP_BATCH, &hop, "hop")]
        {
            for (compression, label) in [(Compression::None, "none"), (Compression::Lz4, "lz4")] {
                let frame = write_frame(codec, compression, payload).expect("None and Lz4 encode");
                stream.extend_from_slice(&frame);
                add("native_frame", format!("{name}-{shape}-{label}"), frame.to_vec());
            }
        }
        add("native_batch", name.clone(), bare.to_vec());
        add("native_hop_batch", name.clone(), hop.to_vec());
    }

    // Sender pairs that `read_hop_prefix` rejects (ADR `native-hop-no-compatibility`, decision
    // 2): a sequence of 0 ahead of a valid batch, and payloads that end inside the pair.
    if let Some((name, batch)) = batches.first() {
        let bare = encode_batch(batch);
        let id: &[u8] = b"seed-sender-id16";
        add(
            "native_hop_batch",
            format!("{name}-bad-prefix-seq0"),
            with_prefix(&[id, &[0]].concat(), &bare),
        );
        add("native_hop_batch", format!("{name}-bad-prefix-id15"), id[..15].to_vec());
        add("native_hop_batch", format!("{name}-bad-prefix-seq-truncated"), [id, &[0x81]].concat());
    }

    let controls = [
        (
            "hello",
            ControlMessage::Hello(Hello {
                version: 1,
                codecs: vec![CODEC_HOP_BATCH],
                compressions: vec![Compression::Lz4 as u8, Compression::None as u8],
                max_frame_bytes: 16 << 20,
                window: 1,
                senders: vec![*b"logit-fuzz-seed!", *b"logit-fuzz-seed2"],
            }),
        ),
        (
            "hello-ack",
            ControlMessage::HelloAck(HelloAck {
                version: 1,
                codec: CODEC_HOP_BATCH,
                compression: Compression::Lz4 as u8,
                max_frame_bytes: 16 << 20,
                window: 1,
                marks: vec![(*b"logit-fuzz-seed!", 7), (*b"logit-fuzz-seed2", 0)],
            }),
        ),
        ("ack", ControlMessage::Ack(Ack::accepted(*b"logit-fuzz-seed!", 1))),
        (
            "ack-rejected",
            ControlMessage::Ack(Ack {
                id: *b"logit-fuzz-seed!",
                seq: 2,
                status: AckStatus::rejected(ACK_REJECTED_DECODE_BUDGET, "over the decode budget"),
            }),
        ),
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

    for (name, bytes) in proxy_headers() {
        add("proxy_header", name.to_string(), bytes);
    }

    for (name, bytes) in forwarding_headers() {
        add("forwarded", name.to_string(), bytes);
    }

    for (name, bytes) in stream_framing_seeds(testdata)? {
        add("stream_framing", name, bytes);
    }

    for (name, bytes) in statsd_seeds(testdata)? {
        add("statsd", name, bytes);
    }

    for (name, bytes) in syslog_seeds(testdata)? {
        add("syslog", name, bytes);
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

/// `prefix` written as given, a bare batch payload, and an empty hop trailer.
fn with_prefix(prefix: &[u8], bare: &[u8]) -> Vec<u8> {
    let mut out = BytesMut::from(prefix);
    out.extend_from_slice(bare);
    write_uvarint(&mut out, 0);
    out.to_vec()
}

/// PROXY protocol headers from HAProxy's `proxy-protocol.txt`, each followed by a payload line as
/// a listener receives it: v1 for every family at its longest, and v2 for every command, family,
/// and transport, one with TLVs after its address block.
fn proxy_headers() -> Vec<(&'static str, Vec<u8>)> {
    let payload = b"<13>hello\n";
    let full = "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff";
    let v1 = |line: String| [line.as_bytes(), payload].concat();
    let v2 = |ver_cmd: u8, fam: u8, body: &[u8]| {
        let mut out = V2_SIGNATURE.to_vec();
        out.extend_from_slice(&[ver_cmd, fam]);
        out.extend_from_slice(&(body.len() as u16).to_be_bytes());
        out.extend_from_slice(body);
        out.extend_from_slice(payload);
        out
    };
    let mut inet = vec![192, 168, 0, 1, 192, 168, 0, 11];
    inet.extend_from_slice(&56324u16.to_be_bytes());
    inet.extend_from_slice(&443u16.to_be_bytes());
    let mut inet6 = [0x20, 0x01, 0x0d, 0xb8].repeat(8);
    inet6.extend_from_slice(&[0xc0, 0x04, 0x01, 0xbb]);
    let mut unix = vec![0u8; 216];
    unix[..13].copy_from_slice(b"/run/app.sock");
    let mut with_tlvs = inet.clone();
    with_tlvs.extend_from_slice(&[0x02, 0x00, 0x0B]);
    with_tlvs.extend_from_slice(b"example.com");
    with_tlvs.extend_from_slice(&[0x05, 0x00, 0x04, 1, 2, 3, 4]);
    vec![
        ("v1-tcp4", v1("PROXY TCP4 192.168.0.1 192.168.0.11 56324 443\r\n".into())),
        (
            "v1-tcp4-longest",
            v1("PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535\r\n".into()),
        ),
        ("v1-tcp6", v1(format!("PROXY TCP6 {full} {full} 65535 65535\r\n"))),
        ("v1-unknown", v1("PROXY UNKNOWN\r\n".into())),
        ("v1-unknown-longest", v1(format!("PROXY UNKNOWN {full} {full} 65535 65535\r\n"))),
        ("v2-local", v2(0x20, 0x00, &[])),
        ("v2-unspec", v2(0x21, 0x00, &[])),
        ("v2-tcp4", v2(0x21, 0x11, &inet)),
        ("v2-udp4", v2(0x21, 0x12, &inet)),
        ("v2-tcp6", v2(0x21, 0x21, &inet6)),
        ("v2-unix-stream", v2(0x21, 0x31, &unix)),
        ("v2-tcp4-tlvs", v2(0x21, 0x11, &with_tlvs)),
    ]
}

/// Forwarding header values from `logit_proto::forwarded`'s unit vectors, RFC 7239's examples
/// among them, each behind the selector byte the `forwarded` target reads: `0` `X-Forwarded-For`,
/// `1` `Forwarded`, `2` `X-Real-IP`.
fn forwarding_headers() -> Vec<(&'static str, Vec<u8>)> {
    let xff = |value: &str| prefixed(0, value.as_bytes());
    let fwd = |value: &str| prefixed(1, value.as_bytes());
    let real = |value: &str| prefixed(2, value.as_bytes());
    vec![
        ("xff-chain", xff("  203.0.113.7 , 10.0.0.9, 10.0.0.1")),
        ("xff-v4-port", xff("203.0.113.7:5678")),
        ("xff-v6-port", xff("[2001:db8::1]:443")),
        ("xff-v6-bare", xff("2001:db8::5:1")),
        ("xff-v4-mapped", xff("::ffff:192.0.2.1")),
        ("xff-unknown", xff("unknown")),
        ("fwd-obfuscated", fwd(r#"for="_gazonk""#)),
        ("fwd-v6-port", fwd(r#"For="[2001:db8:cafe::17]:4711""#)),
        ("fwd-pairs", fwd("for=192.0.2.60;proto=http;by=203.0.113.43")),
        ("fwd-elements", fwd("for=192.0.2.43, for=198.51.100.17")),
        ("fwd-v6-bracketed", fwd(r#"for="[2001:db8::cafe]""#)),
        ("fwd-escaped", fwd(r#"host="a,b;c";for="192.0.2.\1:80""#)),
        ("fwd-unquoted-v6", fwd("for=[2001:db8::1]:443")),
        ("real-ip", real(" 198.51.100.2 ")),
        ("real-ip-v6", real("2001:db8::1")),
    ]
}

/// `stream_framing`'s selector bytes (`fuzz/fuzz_targets/stream_framing.rs`).
const FRAMING_AUTO: u8 = 0;
const FRAMING_LINES_DRAIN: u8 = 1;
const FRAMING_LINES_FATAL: u8 = 2;
const FRAMING_PREFIX_BE: u8 = 3;
const FRAMING_PREFIX_LE: u8 = 4;
/// The bound selector that means `MAX_FRAME_BYTES`; any other value `v` means `1 + v % 4096`.
const BOUND_MAX: u16 = u16::MAX;

/// A `stream_framing` input: mode, bound, chunking seed, then the stream.
fn framing_seed(mode: u8, bound: u16, chunking: u8, stream: &[u8]) -> Vec<u8> {
    let [hi, lo] = bound.to_be_bytes();
    let mut out = vec![mode, hi, lo, chunking];
    out.extend_from_slice(stream);
    out
}

/// Recorded streams under the framing their listener uses, the UDP syslog captures wrapped in
/// RFC 6587 octet counts, and the framing edge cases: keepalive newlines, a zero and a padded
/// count, ten count digits, and a `u32::MAX` length prefix.
fn stream_framing_seeds(testdata: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let read = |path: &str| std::fs::read(testdata.join("interop").join(path));
    let mut out = Vec::new();

    let rsyslog = read("syslog/rsyslog-tcp-000.raw")?;
    for chunking in [0, 7] {
        let seed = framing_seed(FRAMING_AUTO, BOUND_MAX, chunking, &rsyslog);
        out.push((format!("auto-rsyslog-tcp-{chunking}"), seed));
    }

    let mut datagrams: Vec<_> = std::fs::read_dir(testdata.join("interop/syslog"))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "raw"))
        .filter(|path| !path.file_name().is_some_and(|name| name == "rsyslog-tcp-000.raw"))
        .collect();
    datagrams.sort();
    let mut all_counted = Vec::new();
    for path in &datagrams {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        let msg = std::fs::read(path)?;
        let mut counted = format!("{} ", msg.len()).into_bytes();
        counted.extend_from_slice(&msg);
        all_counted.extend_from_slice(&counted);
        out.push((
            format!("auto-octet-{stem}"),
            framing_seed(FRAMING_AUTO, BOUND_MAX, 3, &counted),
        ));
    }
    out.push((
        "auto-octet-all".to_string(),
        framing_seed(FRAMING_AUTO, BOUND_MAX, 11, &all_counted),
    ));

    let mut line_streams = vec![("graphite".to_string(), read("graphite/write-graphite-000.raw")?)];
    let mut statsd: Vec<_> = std::fs::read_dir(testdata.join("interop/statsd"))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("statsd-plain-"))
        })
        .collect();
    statsd.sort();
    for path in statsd {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        line_streams.push((stem, std::fs::read(&path)?));
    }
    // A bound of 64 is shorter than some recorded lines, so both oversize paths are reachable.
    for (stem, stream) in &line_streams {
        for (mode, label) in [(FRAMING_LINES_DRAIN, "drain"), (FRAMING_LINES_FATAL, "fatal")] {
            out.push((format!("lines-{label}-{stem}"), framing_seed(mode, 63, 5, stream)));
        }
    }

    for capture in ["graphite-pickle-p2-000", "graphite-pickle-p5-000"] {
        let stream = read(&format!("graphite/{capture}.raw"))?;
        out.push((format!("be-{capture}"), framing_seed(FRAMING_PREFIX_BE, BOUND_MAX, 1, &stream)));
    }
    for capture in ["dogstatsd-unix-stream-000", "dogstatsd-unix-stream-001"] {
        let stream = read(&format!("datadog/{capture}.raw"))?;
        out.push((format!("le-{capture}"), framing_seed(FRAMING_PREFIX_LE, BOUND_MAX, 1, &stream)));
    }

    let newlines = vec![b'\n'; 4096];
    for (mode, label) in
        [(FRAMING_AUTO, "auto"), (FRAMING_LINES_DRAIN, "drain"), (FRAMING_LINES_FATAL, "fatal")]
    {
        out.push((format!("{label}-all-newlines"), framing_seed(mode, BOUND_MAX, 2, &newlines)));
    }
    for (name, stream) in [
        ("auto-zero-count", &b"0 x"[..]),
        ("auto-padded-count", b"012 x"),
        ("auto-ten-digit-count", b"1234567890 x"),
    ] {
        out.push((name.to_string(), framing_seed(FRAMING_AUTO, BOUND_MAX, 0, stream)));
    }
    let huge = [0xFF, 0xFF, 0xFF, 0xFF, b'x'];
    out.push((
        "be-u32-max-prefix".to_string(),
        framing_seed(FRAMING_PREFIX_BE, BOUND_MAX, 0, &huge),
    ));
    out.push((
        "le-u32-max-prefix".to_string(),
        framing_seed(FRAMING_PREFIX_LE, BOUND_MAX, 0, &huge),
    ));
    Ok(out)
}

/// Every recorded statsd datagram, the `datadog` client's Unix datagrams and its buffered stream
/// connection's packets with their length prefixes stripped, and constructed lines for each shape
/// the grammar names. The unbuffered stream connection is left out: its nine packets are the same
/// nine calls as the Unix datagrams (`testdata/interop/datadog/README.md`).
fn statsd_seeds(testdata: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    let raw_files = |dir: &str, prefix: &str| -> std::io::Result<Vec<std::path::PathBuf>> {
        let mut paths: Vec<_> = std::fs::read_dir(testdata.join("interop").join(dir))?
            .filter_map(|entry| entry.ok().map(|e| e.path()))
            .filter(|path| path.extension().is_some_and(|ext| ext == "raw"))
            .filter(|path| {
                path.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(prefix))
            })
            .collect();
        paths.sort();
        Ok(paths)
    };
    for path in
        raw_files("statsd", "statsd-")?.into_iter().chain(raw_files("datadog", "dogstatsd-unix-0")?)
    {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        out.push((stem, std::fs::read(&path)?));
    }

    let stream = std::fs::read(testdata.join("interop/datadog/dogstatsd-unix-stream-001.raw"))?;
    let mut rest = &stream[..];
    let mut packet = 0;
    while let Some((prefix, body)) = rest.split_first_chunk::<4>() {
        let len = u32::from_le_bytes(*prefix) as usize;
        let (frame, tail) = body.split_at(len.min(body.len()));
        out.push((format!("dogstatsd-unix-stream-001-{packet}"), frame.to_vec()));
        rest = tail;
        packet += 1;
    }

    for (name, datagram) in [
        ("event-escapes", &b"_e{5,14}:title|one\\ntwo\\n\\n|t:warning|p:low|#env:a\n"[..]),
        ("event-every-field", b"_e{2,4}:hi|body|d:1700000000|h:web|p:normal|t:error|k:agg|s:src|#a,b:1|c:cid|e:ext|card:low"),
        ("event-trailing-space", b"_e{1,3}:t|a  "),
        ("service-check", b"_sc|db.ok|2|d:1700000000|h:db|#env:a|m:slow upstream|c:cid|card:high"),
        ("multi-value-timer", b"latency:1:2:3|ms|@0.5|#route:/a"),
        ("multi-value-counter", b"hits:1:2:-3|c|@0.25"),
        ("gauge-deltas", b"load:+1|g\nload:-2|g\nload:3|g"),
        ("set", b"users:alice:bob:alice|s|#team:a,team:b,team:a"),
        ("timestamp", b"hits:1|c|T1700000000\nhits:1|c|T9223372036"),
        ("origin-fields", b"hits:1|c|c:ci-0123|e:it-false,cn-app|card:orchestrator"),
        ("tags", b"hits:1|c|#urgent,urgent:1,env:prod|#env:dev"),
        ("sample-rate", b"latency:12|h|@0.001\nsize:4|d|@1"),
        ("float-extremes", b"big:1.7e308|c|@0.99\nmax:-1.7976931348623157e308|g\nlat:1e-300:4.9e-324|ms"),
        ("mixed-lines", b"a:1|c\r\n  b:2|g  \n\nbad line\n_total.count:1|c\n"),
    ] {
        out.push((format!("constructed-{name}"), datagram.to_vec()));
    }
    Ok(out)
}

/// Every recorded syslog capture under line splitting and as one frame without it, and
/// constructed lines for the shapes `crates/logit-proto/src/syslog/mod.rs` names: STRUCTURED-DATA
/// with repeated PARAM-NAMEs, each escape, and SD-NAMEs at and past 32 bytes; nil and empty
/// fields; a BOM; RFC 3164 with and without a tag and PID; PRI at and past its bounds; digit-led
/// MSGs against the dialect sniff; and the leniencies.
fn syslog_seeds(testdata: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    const SPLIT: u8 = 1;
    const FRAMED: u8 = 0;
    let mut out = Vec::new();
    let mut paths: Vec<_> = std::fs::read_dir(testdata.join("interop/syslog"))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "raw"))
        .collect();
    paths.sort();
    for path in paths {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        let capture = std::fs::read(&path)?;
        out.push((format!("{stem}-split"), prefixed(SPLIT, &capture)));
        out.push((format!("{stem}-framed"), prefixed(FRAMED, &capture)));
    }

    let id_32 = "i".repeat(32);
    let id_33 = "i".repeat(33);
    let constructed: Vec<(&str, u8, Vec<u8>)> = vec![
        ("sd-repeated-params", SPLIT, br#"<134>1 2003-10-11T22:14:15.003Z host app 42 ID1 [rep@1 k="a" k="b" j="c" k="d"][two@1 k="e"] msg"#.to_vec()),
        ("sd-escapes", SPLIT, br#"<134>1 - - - - - [esc@1 q="a\"b" b="x\]y" s="c\\d" o="e\nf"] msg"#.to_vec()),
        ("sd-unescaped-bracket", SPLIT, br#"<134>1 - - - - - [a@1 k="a]b"] msg"#.to_vec()),
        ("sd-name-32", SPLIT, format!(r#"<134>1 - - - - - [{id_32} {id_32}="v"] msg"#).into_bytes()),
        ("sd-name-33", SPLIT, format!(r#"<134>1 - - - - - [{id_33} k="v"] msg"#).into_bytes()),
        ("nil-fields", SPLIT, b"<134>1 - - - - - - nil fields".to_vec()),
        ("empty-fields", SPLIT, br#"<134>1  host  - - [a@1 k="v"]msg"#.to_vec()),
        ("nil-sd-no-space", SPLIT, b"<134>1 - - - - - -msg".to_vec()),
        ("bom", SPLIT, b"<165>1 2003-10-11T22:14:15.003Z host app - - - \xEF\xBB\xBFbom message".to_vec()),
        ("non-utf8-msg", SPLIT, b"<134>1 - host app 7 - - \xff\xfe binary".to_vec()),
        ("rfc3164-tag-pid", SPLIT, b"<13>Oct 11 22:14:15 host app[123]: with a pid".to_vec()),
        ("rfc3164-tag", SPLIT, b"<13>Oct  1 22:14:15 host app: with a tag".to_vec()),
        ("rfc3164-no-tag", SPLIT, b"<13>Oct 11 22:14:15 a message with no tag".to_vec()),
        ("rfc3164-str-pid", SPLIT, b"<13>app[worker-1]: str pid\n<13>app[007]: padded pid".to_vec()),
        ("pri-bounds", SPLIT, b"<0>pri zero\n<191>pri max\n<192>past the max\n<00>leading zero".to_vec()),
        ("digit-led", SPLIT, b"<13>4 requests failed\n<14>1 worker died\n<13>10 workers started".to_vec()),
        ("crlf-lines", SPLIT, b"<13>a\r\n\r\n<13>b\r\r\n<13>c".to_vec()),
        ("framed-multiline", FRAMED, b"<13>line one\nline two\r\n".to_vec()),
    ];
    for (name, selector, datagram) in constructed {
        out.push((format!("constructed-{name}"), prefixed(selector, &datagram)));
    }
    Ok(out)
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
                "forwarded",
                "hll_bytes",
                "native_batch",
                "native_control",
                "native_frame",
                "native_hop_batch",
                "otlp_grpc",
                "otlp_json",
                "otlp_proto",
                "prom_decompress",
                "prom_remote_write",
                "proxy_header",
                "sketch_bytes",
                "sketch_merge",
                "statsd",
                "stream_framing",
                "syslog",
            ]
        );
    }
}
