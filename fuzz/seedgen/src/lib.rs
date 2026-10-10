//! Builds the committed starting seeds under `fuzz/seeds/<target>/` from `testdata/interop/`,
//! `testdata/differential/`, and encoder round trips, so every fuzz target starts from inputs its
//! decoder accepts.
//!
//! A target that takes a selector byte gets it prepended, matching the target's own doc: the
//! OTLP targets' signal (`0` logs, `1` metrics, `2` traces), `prom_decompress`'s encoding,
//! `prom_remote_write`'s version and mode, `prom_text`'s dialect, `stream_framing`'s four bytes of mode, bound, and chunking,
//! `syslog`'s line splitting (`1` on, `0` off), `graphite_pickle`'s mode (`0` a payload, `1` a
//! build spec), `collectd`'s two-byte cut position, and the four `message_*` targets' entry point,
//! delimiter, `bare_keys`, or separator pair ([`message_seeds`]). [`generate`] is deterministic, so a rerun
//! rewrites the same bytes and leaves `git status` clean.

use bytes::{Bytes, BytesMut};
use logit_core::interner::intern;
use logit_core::{
    AttrMap, DdSketch, Event, EventBatch, HyperLogLog, LogRecord, Mapping, MetricKind,
    MetricRecord, Provenance, Resource, Severity, Sum, Temporality, Value,
};
use logit_proto::collectd::{
    CollectdEncoder, ATTR_HOST, ATTR_INTERVAL, ATTR_PLUGIN, ATTR_PLUGIN_INSTANCE, ATTR_SEVERITY,
    ATTR_TYPE, ATTR_TYPE_INSTANCE, DEFAULT_MAX_PACKET_BYTES, MAX_VALUES_PER_LIST,
};
use logit_proto::frame::{write_frame, write_frame_with_flags, Compression, FLAG_CONTROL};
use logit_proto::graphite::pickle::write_datapoints;
use logit_proto::native::control::{
    Ack, AckStatus, ControlMessage, Hello, HelloAck, Reject, ACK_REJECTED_DECODE_BUDGET,
};
use logit_proto::native::varint::write_uvarint;
use logit_proto::native::{encode_batch, encode_hop_batch, SeqId, CODEC_BATCH, CODEC_HOP_BATCH};
use logit_proto::otlp::{OtlpDecoder, OtlpEncoder};
use logit_proto::prometheus::compression::{decompress_bounded, Encoding};
use logit_proto::prometheus::remote_write::{self, Version};
use logit_proto::prometheus::text::{self, Dialect};
use logit_proto::prometheus::PrometheusDecoder;
use logit_proto::proxy::V2_SIGNATURE;
use logit_proto::{FramedEncoder, MessageBuf, Signal, SignalEncoder, SignalPayload};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

/// A seed larger than this is left out: libFuzzer's `-max_len` would truncate it anyway, and the
/// seeds are committed.
pub const MAX_SEED_BYTES: usize = 256 * 1024;

/// Seeds by target, then by file name.
pub type Seeds = BTreeMap<&'static str, BTreeMap<String, Vec<u8>>>;

const SIGNALS: [(Signal, &str, u8); 3] =
    [(Signal::Logs, "logs", 0), (Signal::Metrics, "metrics", 1), (Signal::Traces, "traces", 2)];

/// `prom_text`'s dialect selectors.
const DIALECTS: [(Dialect, &str, u8); 2] =
    [(Dialect::Text0_0_4, "text", 0), (Dialect::OpenMetrics1_0, "om", 1)];

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
        add("prom_remote_write", stem.clone(), prefixed(version_selector, &decompressed));
        let decoded = remote_write::decode(&decompressed, version, &mut PrometheusDecoder::new())
            .map_err(|e| std::io::Error::other(format!("{stem}: {e}")))?;
        for (i, families) in decoded.groups.iter().enumerate().take(3) {
            for (dialect, label, selector) in DIALECTS {
                let mut body = Vec::new();
                text::write(families, dialect, &mut body);
                add("prom_text", format!("{stem}-{i}-{label}"), prefixed(selector, &body));
            }
        }
    }

    for (name, bytes) in prom_text_seeds(testdata)? {
        add("prom_text", name, bytes);
    }

    for (name, bytes) in prom_remote_write_built_seeds() {
        add("prom_remote_write", name, bytes);
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

    for (name, bytes) in graphite_plaintext_seeds(testdata)? {
        add("graphite_plaintext", name, bytes);
    }

    for (name, bytes) in graphite_pickle_seeds(testdata)? {
        add("graphite_pickle", name, bytes);
    }

    for (name, bytes) in collectd_seeds(testdata)? {
        add("collectd", name, bytes);
    }

    for (target, seeds) in message_seeds() {
        for (name, bytes) in seeds {
            add(target, name.to_string(), bytes);
        }
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

/// collectd's `write_graphite` capture as one datagram and as its first read cycle's 25 lines,
/// one per seed, and constructed lines for the shapes `crates/logit-proto/src/graphite/mod.rs`'s
/// decode table names: tags, a repeated tag key, malformed tags, the `-1` sentinel, fractional and
/// boundary timestamps, non-finite values, a non-UTF-8 line, and Unicode whitespace.
fn graphite_plaintext_seeds(testdata: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let capture = std::fs::read(testdata.join("interop/graphite/write-graphite-000.raw"))?;
    let mut out = vec![("write-graphite-000".to_string(), capture.clone())];
    for (i, line) in capture.split_inclusive(|&b| b == b'\n').take(25).enumerate() {
        out.push((format!("write-graphite-000-line-{i:02}"), line.to_vec()));
    }
    for (name, datagram) in [
        ("tags", &b"sys.cpu;env=prod;host=web-1 0.5 1700000000\nsys.mem;k=a=b 2 1700000001"[..]),
        ("repeated-tag", b"a.b;team=a;team=b 1 1700000000"),
        ("bad-tags", b"a.b;novalue 1 1\na.b;=v 1 1\na.b;n= 1 1\n;k=v 1 1\na.b; 1 1"),
        ("sentinel", b"a.b 1 -1\na.b 1 -1.0\na.b 1 -1e0\na.b 1 -1.0000000000000002\na.b 1 -0.0"),
        ("fractional", b"a.b 1 1700000000.25\na.b 1 0.9999999999\na.b 1 0.5011891235"),
        ("boundaries", b"a.b 1 2147483647\na.b 1 2147483648.5\na.b 1 9223372036.854775807\na.b 1 1e300\na.b 1 4.9e-324"),
        ("values", b"a.b NaN 1\na.b inf 1\na.b -inf 1\na.b 1e308 1\na.b -0 1\na.b 5e-324 1\na.b +1.5 1"),
        ("fields", b"a.b 1\na.b 1 1 1\n\t a.b\t1  1 \r\n   \n"),
        ("non-utf8", b"good 1 1\nbad.\xff 1 1\ngood.two 1 1"),
        ("unicode-whitespace", "a.b\u{a0}1\u{2003}1700000000\na\u{85}b 1 1".as_bytes()),
        ("sanitized", b"a/b\\c 1 1\na.b;x!=1;x^=2;t=~v 1 1\na.\x01b 1 1"),
    ] {
        out.push((format!("constructed-{name}"), datagram.to_vec()));
    }
    Ok(out)
}

/// `p = 'robustness.host.cpu;env=prod'; pickle.dumps([(p, (1700000000, 0.5)),
/// ('robustness.host.mem', (1700000001, 2**31 + 5)), (p, (1700000002, -1.25))], protocol=2)`,
/// CPython 3.14's bytes, copied from `crates/logit-proto/tests/robustness.rs`'s
/// `GRAPHITE_PICKLE`: a memoized repeat through `BINGET` and a `LONG1` value.
const CPYTHON_MEMOIZED: &[u8] = &[
    0x80, 0x02, 0x5d, 0x71, 0x00, 0x28, 0x58, 0x1c, 0x00, 0x00, 0x00, 0x72, 0x6f, 0x62, 0x75, 0x73,
    0x74, 0x6e, 0x65, 0x73, 0x73, 0x2e, 0x68, 0x6f, 0x73, 0x74, 0x2e, 0x63, 0x70, 0x75, 0x3b, 0x65,
    0x6e, 0x76, 0x3d, 0x70, 0x72, 0x6f, 0x64, 0x71, 0x01, 0x4a, 0x00, 0xf1, 0x53, 0x65, 0x47, 0x3f,
    0xe0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x71, 0x02, 0x86, 0x71, 0x03, 0x58, 0x13, 0x00,
    0x00, 0x00, 0x72, 0x6f, 0x62, 0x75, 0x73, 0x74, 0x6e, 0x65, 0x73, 0x73, 0x2e, 0x68, 0x6f, 0x73,
    0x74, 0x2e, 0x6d, 0x65, 0x6d, 0x71, 0x04, 0x4a, 0x01, 0xf1, 0x53, 0x65, 0x8a, 0x05, 0x05, 0x00,
    0x00, 0x80, 0x00, 0x86, 0x71, 0x05, 0x86, 0x71, 0x06, 0x68, 0x01, 0x4a, 0x02, 0xf1, 0x53, 0x65,
    0x47, 0xbf, 0xf4, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x86, 0x71, 0x07, 0x86, 0x71, 0x08, 0x65,
    0x2e,
];

/// Under mode `0`: the five recorded pickle captures with their length prefix stripped (protocols
/// 0, 2, and 5 from CPython 3, Python 2's `cPickle` protocol 0, and Dropwizard's
/// `PickledGraphite`), every payload in `testdata/differential/graphite-pickle/` as
/// `diff-<case>`, a CPython dump with memoized repeats and a `LONG1`, `write_datapoints`
/// output, and hand-assembled payloads in the shapes other producers write (og-rek's
/// `MARK … LIST`, one `APPEND` per item, protocol 1's `MARK … TUPLE`, a stray `None`, a
/// numeric-string value, the memo key past 255 a batch of more than 256 datapoints reaches, a
/// list-shaped datapoint, and protocol-0 strings with every escape the reader decodes). Under
/// mode `1`: build specs covering every item kind under each of the three outer-list shapes, in
/// both spellings.
fn graphite_pickle_seeds(testdata: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    const PAYLOAD: u8 = 0;
    const BUILD: u8 = 1;
    let mut out = Vec::new();
    for stem in [
        "graphite-dropwizard-000",
        "graphite-pickle-p0-000",
        "graphite-pickle-p2-000",
        "graphite-pickle-p5-000",
        "graphite-pickle-py2-000",
    ] {
        let capture = std::fs::read(testdata.join(format!("interop/graphite/{stem}.raw")))?;
        let mut rest = &capture[..];
        let mut frame = 0;
        while let Some((prefix, body)) = rest.split_first_chunk::<4>() {
            let len = u32::from_be_bytes(*prefix) as usize;
            let (payload, tail) = body.split_at(len.min(body.len()));
            out.push((format!("{stem}-{frame}"), prefixed(PAYLOAD, payload)));
            rest = tail;
            frame += 1;
        }
    }
    out.push(("cpython-memoized".to_string(), prefixed(PAYLOAD, CPYTHON_MEMOIZED)));

    // Every case of the carbon pickle differential corpus, accepted and rejected alike.
    let mut cases: Vec<_> = std::fs::read_dir(testdata.join("differential/graphite-pickle"))?
        .map(|entry| entry.map(|e| e.path()))
        .collect::<Result<_, _>>()?;
    cases.retain(|path| path.extension().is_some_and(|ext| ext == "pkl"));
    cases.sort();
    for path in cases {
        let stem = path.file_stem().and_then(|s| s.to_str()).expect("a UTF-8 case name");
        out.push((format!("diff-{stem}"), prefixed(PAYLOAD, &std::fs::read(&path)?)));
    }

    let written: [(&str, Vec<(&str, i64, f64)>); 4] = [
        ("one", vec![("sys.cpu;host=web-1", 1_700_000_000, 0.5)]),
        ("sentinel-and-long1", vec![("a.b", -1, 1.0), ("far.future", (1 << 31) + 5, -2.25)]),
        (
            "non-finite",
            vec![("a.b", 1_700_000_000, f64::NAN), ("c.d", 1_700_000_000, f64::INFINITY)],
        ),
        ("bad-timestamps", vec![("a.b", 0, 1.0), ("a.b", -2, 1.0), ("a.b", i64::MAX, 1.0)]),
    ];
    for (name, datapoints) in written {
        let mut payload = Vec::new();
        write_datapoints(&mut payload, datapoints);
        out.push((format!("written-{name}"), prefixed(PAYLOAD, &payload)));
    }
    let many: Vec<(String, i64, f64)> =
        (0..300).map(|i| (format!("m.{i}"), 1_700_000_000 + i, i as f64)).collect();
    let mut payload = Vec::new();
    write_datapoints(&mut payload, many.iter().map(|(p, t, v)| (p.as_str(), *t, *v)));
    out.push(("written-300".to_string(), prefixed(PAYLOAD, &payload)));

    // `X` is `BINUNICODE`, `U` `SHORT_BINSTRING`, `J` `BININT`, `K` `BININT1`, `G` `BINFLOAT`,
    // `(` `MARK`, `t` `TUPLE`, `\x86` `TUPLE2`, `l` `LIST`, `a` `APPEND`, `]` `EMPTY_LIST`.
    let float_one = [0x47, 0x3f, 0xf0, 0, 0, 0, 0, 0, 0];
    let mut og_rek = vec![0x80, 2, 0x28, 0x55, 3];
    og_rek.extend_from_slice(b"a.b");
    og_rek.extend_from_slice(&[0x4a, 0x00, 0xf1, 0x53, 0x65]);
    og_rek.extend_from_slice(&float_one);
    og_rek.extend_from_slice(&[0x86, 0x86, 0x6c, 0x2e]);
    out.push(("og-rek-mark-list".to_string(), prefixed(PAYLOAD, &og_rek)));
    let mut append_each = vec![0x80, 2, 0x5d];
    for path in [&b"a.b"[..], b"c.d"] {
        append_each.extend_from_slice(&[0x28, 0x55, 3]);
        append_each.extend_from_slice(path);
        append_each.extend_from_slice(&[0x28, 0x4b, 7]);
        append_each.extend_from_slice(&float_one);
        append_each.extend_from_slice(&[0x74, 0x74, 0x61]);
    }
    append_each.push(0x2e);
    out.push(("append-each-protocol-1-tuples".to_string(), prefixed(PAYLOAD, &append_each)));
    let mut mixed = vec![0x80, 2, 0x5d, 0x28, 0x4e, 0x58, 3, 0, 0, 0];
    mixed.extend_from_slice(b"a.b");
    mixed.extend_from_slice(&[0x58, 3, 0, 0, 0]);
    mixed.extend_from_slice(b"1.5");
    mixed.extend_from_slice(&[0x58, 3, 0, 0, 0]);
    mixed.extend_from_slice(b"2.5");
    mixed.extend_from_slice(&[0x86, 0x86, 0x65, 0x2e]);
    out.push(("none-and-numeric-strings".to_string(), prefixed(PAYLOAD, &mixed)));
    let mut list_shaped = vec![0x80, 2, 0x5d, 0x28, 0x5d, 0x28, 0x58, 3, 0, 0, 0];
    list_shaped.extend_from_slice(b"a.b");
    list_shaped.extend_from_slice(&[0x5d, 0x28, 0x4b, 1]);
    list_shaped.extend_from_slice(&float_one);
    list_shaped.extend_from_slice(&[0x65, 0x65, 0x65, 0x2e]);
    out.push(("list-shaped-datapoint".to_string(), prefixed(PAYLOAD, &list_shaped)));

    // Protocol 0: every `STRING` escape, `UNICODE`'s `\u`/`\U`/Latin-1, a `LONG` without its `L`,
    // `I01`, `F nan`, and a `GET` of a path memoized at cPickle's key 1.
    let escapes: &[u8] =
        b"(lp1\n(S'a\\\\b\\'c\\\"d\\a\\b\\f\\n\\r\\t\\v\\x41\\101\\q;k=\\x76'\np2\n\
        (I1700000000\nF0.5\ntp3\ntp4\na(V\\u0071\\U0001f600\xe9\\x\np5\n(L1700000001\nFnan\nttp6\na\
        (g2\n(I01\nS'2.5'\nttp7\na.";
    out.push(("protocol-0-escapes".to_string(), prefixed(PAYLOAD, escapes)));

    // A build spec's first byte picks the outer list (`0` `APPENDS`, `1` `LIST`, `2` one `APPEND`
    // per item); each byte after it is one item, `byte % 7` its kind and `byte / 7` its number.
    // Bit 7 of the first byte spells the payload in protocol 0's text opcodes, and bit 6 numbers
    // the memo from 1, as Python 2's `cPickle` does; `n` up to 3 covers each escape spelling.
    let every_kind: Vec<u8> = (0..28).collect();
    for (outer, name) in [
        (0u8, "appends"),
        (1, "list"),
        (2, "append-each"),
        (0x80, "text-appends"),
        (0x81, "text-list"),
        (0xc2, "text-append-each-cpickle-memo"),
    ] {
        let spec = [&[outer][..], &every_kind].concat();
        out.push((format!("build-{name}-every-kind"), prefixed(BUILD, &spec)));
        // Alternating a new tuple (kind 0) and a memo repeat of an earlier one (kind 2).
        let tuples: Vec<u8> =
            [outer].into_iter().chain((0..40).map(|i| 7 * (i / 2) + 2 * (i % 2))).collect();
        out.push((format!("build-{name}-tuples-and-repeats"), prefixed(BUILD, &tuples)));
    }
    Ok(out)
}

/// The four recorded collectd datagrams, and `CollectdEncoder` output for constructed events:
/// value lists with elided identity, every data-source kind, a NaN GAUGE, a notification, and a
/// list of `MAX_VALUES_PER_LIST` values; a list of one more, whose length matches its count,
/// behind those lists; and a datagram with Signature and Encryption parts. Each seed's
/// two-byte cut falls mid-datagram but the last's.
fn collectd_seeds(testdata: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let cut_at_half = |datagram: &[u8]| -> Vec<u8> {
        let cut = (datagram.len() / 2) as u16;
        [&cut.to_be_bytes()[..], datagram].concat()
    };
    let mut out = Vec::new();
    let mut paths: Vec<_> = std::fs::read_dir(testdata.join("interop/collectd"))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "raw"))
        .collect();
    paths.sort();
    for path in paths {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        out.push((stem, cut_at_half(&std::fs::read(&path)?)));
    }

    let identity =
        |plugin: &str, instance: Option<&str>, type_: &str, type_instance: Option<&str>| {
            let mut attrs = AttrMap::new();
            attrs.insert(ATTR_HOST, "seed-host");
            attrs.insert(ATTR_PLUGIN, plugin);
            if let Some(instance) = instance {
                attrs.insert(ATTR_PLUGIN_INSTANCE, instance);
            }
            attrs.insert(ATTR_TYPE, type_);
            if let Some(type_instance) = type_instance {
                attrs.insert(ATTR_TYPE_INSTANCE, type_instance);
            }
            attrs.insert(ATTR_INTERVAL, Value::F64(10.0));
            attrs
        };
    let list = |attrs: AttrMap, kinds: Vec<MetricKind>| {
        let mut event = Event::empty(1_700_000_000_123_456_789, attrs);
        for (i, kind) in kinds.into_iter().enumerate() {
            event.metrics.push(MetricRecord::new(intern(&format!("seed.{i}")), kind));
        }
        event
    };
    let sum = |value: f64, temporality: Temporality, monotonic: bool| {
        MetricKind::Sum(Sum { value, temporality, monotonic })
    };
    let mut nan = MetricRecord::new(intern("seed.nan"), MetricKind::Gauge(0.0));
    nan.flags = MetricRecord::FLAG_NO_RECORDED_VALUE;
    let mut nan_list = list(identity("memory", None, "memory", Some("free")), vec![]);
    nan_list.metrics.push(nan);

    let mut notification_attrs = identity("load", None, "load", None);
    notification_attrs.remove(ATTR_INTERVAL);
    notification_attrs.insert(ATTR_SEVERITY, Value::U64(2));
    let notification = Event::log(
        1_700_000_001_000_000_000,
        notification_attrs,
        LogRecord {
            message: Value::str("load is above its warning threshold"),
            severity: Some(Severity::Warn),
            body_format: logit_core::BodyFormat::Raw,
            trace: None,
            event_name: None,
            observed_timestamp: 0,
            dropped_attributes_count: 0,
        },
    );

    let batches: Vec<(&str, Vec<Event>)> = vec![
        (
            "lists",
            vec![
                list(identity("load", None, "load", None), vec![MetricKind::Gauge(0.5); 3]),
                list(
                    identity("interface", Some("eth0"), "if_octets", None),
                    vec![
                        sum(1.0, Temporality::Cumulative, false),
                        sum(2.0, Temporality::Cumulative, false),
                    ],
                ),
                list(
                    identity("interface", Some("eth0"), "if_packets", None),
                    vec![sum(3.0, Temporality::Cumulative, false); 2],
                ),
                list(
                    identity("cpu", Some("0"), "cpu", Some("user")),
                    vec![sum(7.0, Temporality::Cumulative, true)],
                ),
                list(
                    identity("cpu", Some("0"), "cpu", Some("idle")),
                    vec![sum(9.0, Temporality::Delta, true)],
                ),
                nan_list,
            ],
        ),
        ("notification", vec![notification]),
        (
            "max-values",
            vec![list(
                identity("wide", None, "wide", None),
                vec![MetricKind::Gauge(1.0); MAX_VALUES_PER_LIST],
            )],
        ),
    ];
    for (name, events) in batches {
        let batch = EventBatch { resource: Arc::new(Resource::default()), scope: None, events };
        let mut packets: MessageBuf<usize> = MessageBuf::default();
        CollectdEncoder::new()
            .with_max_packet_bytes(DEFAULT_MAX_PACKET_BYTES)
            .encode_into(&batch, &mut packets);
        for (i, packet) in packets.iter().enumerate() {
            out.push((format!("encoded-{name}-{i}"), cut_at_half(packet)));
        }
    }

    // A Signature part (skipped by length) ahead of a list, then an Encryption part, which stops
    // the walk, with bytes after it that would otherwise be a malformed part.
    let mut signed = Vec::new();
    signed.extend_from_slice(&[0x02, 0x00, 0x00, 0x28]);
    signed.extend_from_slice(&[0xab; 36]);
    let first = out.iter().find(|(name, _)| name == "encoded-lists-0").unwrap().1[2..].to_vec();
    signed.extend_from_slice(&first);
    signed.extend_from_slice(&[
        0x02, 0x10, 0x00, 0x08, 0xde, 0xad, 0xbe, 0xef, 0x00, 0x06, 0x00, 0x01,
    ]);
    out.push(("signed-then-encrypted".to_string(), [&[0xff, 0xff][..], &signed].concat()));

    // The encoded lists, then a Values part of `MAX_VALUES_PER_LIST + 1` GAUGEs whose length is
    // the one that count implies: past the cap, so a `bad_part` that keeps the lists before it.
    let mut past_cap = first;
    let count = MAX_VALUES_PER_LIST + 1;
    past_cap.extend_from_slice(&0x0006u16.to_be_bytes());
    past_cap.extend_from_slice(&((6 + 9 * count) as u16).to_be_bytes());
    past_cap.extend_from_slice(&(count as u16).to_be_bytes());
    past_cap.extend(std::iter::repeat_n(1u8, count));
    for _ in 0..count {
        past_cap.extend_from_slice(&1.5f64.to_le_bytes());
    }
    out.push(("values-count-past-the-cap".to_string(), cut_at_half(&past_cap)));
    Ok(out)
}

/// `logit-cli`'s exposition fixtures, by their dialect suffix, and the scrape target the recorded
/// metadata capture points at, then hand-written bodies for what neither has: OpenMetrics
/// `_created`, `# EOF` misplaced and missing, a histogram missing `+Inf`, fractional and
/// non-finite counts, exemplars, `info` and `stateset`, quantiles outside `[0, 1]`, every escape
/// at the end of a label value, and a text 0.0.4 `# EOF`, which is a comment.
fn prom_text_seeds(testdata: &Path) -> std::io::Result<Vec<(String, Vec<u8>)>> {
    let mut out = Vec::new();
    let fixtures = testdata.join("../crates/logit-cli/tests/fixtures/prometheus");
    let mut paths: Vec<_> = std::fs::read_dir(&fixtures)?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| path.extension().is_some_and(|ext| ext == "in"))
        .collect();
    paths.sort();
    for path in paths {
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        let selector = if stem.ends_with(".om") { 1 } else { 0 };
        out.push((format!("cli-{stem}"), prefixed(selector, &std::fs::read(&path)?)));
    }
    let target = testdata.join("../tools/record-fixtures/prometheus-metadata-target.prom");
    out.push(("metadata-target".to_string(), prefixed(0, &std::fs::read(target)?)));

    let om: [(&str, &str); 9] = [
        (
            "created",
            "# TYPE req counter\nreq_total{a=\"1\"} 3 1605281325.5\nreq_created{a=\"1\"} 1605281325.123\n\
             # TYPE lat summary\nlat_count 2\nlat_created 1.605281325e9\n# EOF\n",
        ),
        ("eof-missing", "# TYPE g gauge\ng 1\n"),
        ("eof-then-content", "g 1\n# EOF\ng 2\n"),
        ("eof-then-blank", "g 1\n# EOF\n\n \t\r\n"),
        (
            "histogram-no-inf",
            "# TYPE h histogram\nh_bucket{le=\"1\"} 2\nh_bucket{le=\"5\"} 4\nh_count 6\nh_sum 9\n# EOF\n",
        ),
        (
            "fractional-counts",
            "# TYPE h gaugehistogram\nh_bucket{le=\"1\"} 0.5\nh_bucket{le=\"+Inf\"} 2.5\nh_gcount 2.49\n\
             h_gsum 1\n# TYPE c histogram\nc_bucket{le=\"1\"} NaN\nc_bucket{le=\"2\"} -1\n\
             c_bucket{le=\"+Inf\"} 1e19\nc_count +Inf\n# EOF\n",
        ),
        (
            "exemplars",
            "# TYPE r counter\nr_total 3 # {trace_id=\"0123456789abcdef0123456789abcdef\",\
             span_id=\"fedcba9876543210\"} 0.5 1605281325.5\n# TYPE h histogram\n\
             h_bucket{le=\"0.1\"} 1 # {slow=\"no\"} 0.05\nh_bucket{le=\"+Inf\"} 2 # {} 7\n# EOF\n",
        ),
        (
            "info-and-stateset",
            "# TYPE build info\nbuild_info{version=\"1.2.3\"} 1\n# TYPE s stateset\n\
             s{s=\"a\"} 0\ns{s=\"b\"} 1\n# TYPE u unknown\n# UNIT u_seconds seconds\nu 1\n# EOF\n",
        ),
        (
            "quantiles",
            "# TYPE q summary\nq{quantile=\"0.5\"} 1\nq{quantile=\"5\"} 2\nq{quantile=\"-Inf\"} 3\n\
             q{quantile=\"NaN\"} 4\nq_sum 1\nq_count 3\n# EOF\n",
        ),
    ];
    for (name, body) in om {
        out.push((format!("om-{name}"), prefixed(1, body.as_bytes())));
    }
    let text: [(&str, &str); 2] = [
        (
            "escapes",
            "e{a=\"x\\\\\",b=\"y\\\"\",c=\"z\\n\",d=\"\\t\"} 1\nf{a=\"open\\\"} 1\nf{a=\"cut\\\n",
        ),
        ("eof-is-a-comment", "g 1\n# EOF\ng{a=\"b\"} 2\n# HELP g A gauge.\n"),
    ];
    for (name, body) in text {
        out.push((format!("text-{name}"), prefixed(0, body.as_bytes())));
    }
    Ok(out)
}

/// `prom_remote_write`'s build-mode inputs (selector bit 1), in the byte order the target's
/// `Build` reads them. Each 2.0 request has the four symbols `"", "__name__", "foo", "a"` (k = 4)
/// and one series named `foo`, with a reference at k - 1, at k, or at `u32::MAX`, an odd
/// `labels_refs`, and a non-empty `symbols[0]`; the 1.0 ones carry a valid and an invalid series,
/// and metadata.
fn prom_remote_write_built_seeds() -> Vec<(String, Vec<u8>)> {
    // Indices into the target's `STRINGS`.
    const EMPTY: u8 = 0;
    const NAME: u8 = 1;
    const FOO: u8 = 2;
    const A: u8 = 12;
    const HELP: u8 = 13;
    const GAUGE: u8 = 2;
    // k, the four symbols, then a byte that empties `symbols[0]` unless it's a multiple of 8.
    let symbols = |first: u8, keep_first: u8| vec![4, first, NAME, FOO, A, keep_first];
    // One series: its references, one sample of 1.0 at 0 ms, no native histogram, no exemplar,
    // then the metadata selector, type, help reference, and unit reference.
    let series = |refs: &[u8], help: u8| {
        let mut out = vec![1, refs.len() as u8];
        out.extend_from_slice(refs);
        out.extend_from_slice(&[1, 0, 0, 0, 1, 0, 1, GAUGE, help, 0]);
        out
    };
    let v2 = |first: u8, keep_first: u8, refs: &[u8], help: u8| {
        let mut out = vec![0b11];
        out.extend(symbols(first, keep_first));
        out.extend(series(refs, help));
        out
    };
    let v1 = {
        let mut out = vec![0b10, 2];
        // `__name__="foo",a="a"`, a sample of 2.5 at 0 ms, an exemplar `{a="a"} 1.0` at 0 ms, and
        // no native histogram.
        out.extend_from_slice(&[2, NAME, FOO, A, A, 1, 3, 0, 0, 1, 1, A, A, 0, 0, 1]);
        // An empty `__name__`, no samples or exemplars, and a native histogram, which an invalid
        // series doesn't count.
        out.extend_from_slice(&[1, NAME, EMPTY, 0, 0, 0]);
        // One metadata entry: `foo` is a gauge, with help.
        out.extend_from_slice(&[1, GAUGE, FOO, HELP, EMPTY]);
        out
    };
    vec![
        ("built-v2-ref-k-minus-1".to_string(), v2(EMPTY, 1, &[1, 2], 3)),
        ("built-v2-ref-k".to_string(), v2(EMPTY, 1, &[1, 2], 4)),
        ("built-v2-ref-u32-max".to_string(), v2(EMPTY, 1, &[1, 255], 0)),
        ("built-v2-odd-refs".to_string(), v2(EMPTY, 1, &[1, 2, 3], 0)),
        ("built-v2-symbol0-not-empty".to_string(), v2(A, 8, &[1, 2], 0)),
        ("built-v1".to_string(), v1),
    ]
}

/// An nginx `access_json_full` line (`fixtures/nginx/nginx.conf`), as `escape=json` writes one.
const NGINX_ACCESS_JSON: &str = concat!(
    r#"{"time":"2026-09-07T06:52:01+00:00","remote_addr":"203.0.113.7","host":"shop.example.com","#,
    r#""request_method":"GET","request_uri":"/api/v1/orders?id=42&sort=desc","status":200,"#,
    r#""body_bytes_sent":612,"request_time":0.012,"upstream_response_time":"0.010","#,
    r#""http_referer":"","http_user_agent":"Mozilla/5.0 (X11; Linux x86_64) Firefox/131.0","#,
    r#""http_x_forwarded_for":""}"#
);

/// The same format with what `escape=json` escapes: a quote and a backslash in the request URI,
/// and an ESC byte as `\u001B`.
const NGINX_ACCESS_JSON_ESCAPED: &str = concat!(
    r#"{"time":"2026-09-07T06:52:02+00:00","remote_addr":"198.51.100.23","host":"shop.example.com","#,
    r#""request_method":"GET","request_uri":"/search?q=\"a\\b\"","status":499,"#,
    r#""body_bytes_sent":0,"request_time":0.000,"upstream_response_time":"","#,
    r#""http_referer":"https://shop.example.com/","http_user_agent":"curl/8.5.0 \u001B[31m","#,
    r#""http_x_forwarded_for":"203.0.113.7, 10.0.0.1"}"#
);

/// nginx's `access_semconv` format (`fixtures/nginx/nginx.conf`), the `http_access` input.
const NGINX_ACCESS_SEMCONV: &str = concat!(
    r#"{"http.request.method":"POST","url.original":"/api/v1/orders","url.scheme":"https","#,
    r#""network.protocol.version":"HTTP/2.0","http.response.status_code":"201","#,
    r#""http.response.body.size":57,"http.response.size":312,"http.request.size":1024,"#,
    r#""http.request.duration_s":0.018,"server.address":"shop.example.com","#,
    r#""client.address":"203.0.113.7","user_agent.original":"Mozilla/5.0","#,
    r#""http.request.header.referer":"","user.name":"","upstream.address":"10.0.0.17:8080","#,
    r#""upstream.status":"201","upstream.duration_s":"0.017"}"#
);

/// pino-http's completion record, two levels of nested objects
/// (`crates/logit-bench/src/fixtures.rs`'s `PINO_HTTP_LOG_BODY`).
const PINO_HTTP_JSON: &str = concat!(
    r#"{"level":30,"time":1725091200123,"pid":4821,"hostname":"api-7c9f8d6b5-abcde","#,
    r#""reqId":"req-8461","req":{"method":"POST","url":"/api/v1/orders","#,
    r#""headers":{"host":"shop.example.com","content-type":"application/json"}},"#,
    r#""res":{"statusCode":201,"headers":{"content-length":"57","vary":"Accept-Encoding"}},"#,
    r#""responseTime":18,"msg":"request completed","tags":["a",["b",{}],[]]}"#
);

/// The delimiter selector `message_csv` reads for `delim`: its index among the delimiters graph
/// rule 32 admits, in byte order.
fn csv_selector(delim: u8) -> u8 {
    (0u8..delim).filter(|b| !matches!(b, b'"' | b'\n' | b'\r')).count() as u8
}

/// `message_kv`'s separator table, in the target's order (`fuzz/fuzz_targets/message_kv.rs`).
const KV_SEPARATORS: [(&str, &str); 9] = [
    ("&", "="),
    (" ", "="),
    (",", "="),
    (", ", ": "),
    (";", "="),
    ("\t", ":"),
    (",", " "),
    (" :: ", " -> "),
    ("¦", "→"),
];

/// Constructed seeds for the four log-message targets: no recorded corpus holds these messages.
/// `message_json`'s selector is `0` `parse_object` and `1` `parse_object_prefix`,
/// `message_logfmt`'s is `bare_keys`, `message_csv`'s is [`csv_selector`], and `message_kv`'s is
/// an index into [`KV_SEPARATORS`] shifted left one, `bare_keys` in the low bit.
fn message_seeds() -> Vec<(&'static str, Vec<(&'static str, Vec<u8>)>)> {
    let json = |prefix: bool, body: &str| prefixed(prefix as u8, body.as_bytes());
    let json_seeds = vec![
        ("nginx-access", json(false, NGINX_ACCESS_JSON)),
        ("nginx-access-escaped", json(false, NGINX_ACCESS_JSON_ESCAPED)),
        ("nginx-semconv", json(false, NGINX_ACCESS_SEMCONV)),
        // The bare `000` `$status` writes for a request nginx never answered isn't valid JSON.
        (
            "nginx-status-000",
            json(false, &NGINX_ACCESS_JSON.replace(r#""status":200"#, r#""status":000"#)),
        ),
        ("pino-nested", json(false, PINO_HTTP_JSON)),
        (
            "numbers",
            json(
                false,
                r#"{"u":18446744073709551615,"u_over":18446744073709551616,"i":-9223372036854775808,"i_over":-9223372036854775809,"zero":0,"neg_zero":-0,"f":1.5e308,"small":4.9e-324,"e":1E-7,"frac":0.1}"#,
            ),
        ),
        (
            "escapes",
            json(
                false,
                r#"{"s":"\"\\\/\b\f\n\r\t\u00e9\ud83d\ude00\u0000","esc\u0061ped key":"x","":""}"#,
            ),
        ),
        (
            "duplicate-keys",
            json(false, r#"{"a":1,"a":"two","n":{"b":1,"b":{"c":null}},"a":[true]}"#),
        ),
        ("whitespace", json(false, " \t\r\n{ \"a\" : [ 1 , 2 ] , \"b\" : false }\n")),
        ("empty-object", json(false, "{}")),
        ("prefix-trailing-text", json(true, r#"{"level":"info","msg":"ok"} took=3ms"#)),
        ("prefix-second-object", json(true, r#"{"a":1}{"b":2}"#)),
        ("prefix-nginx", json(true, NGINX_ACCESS_JSON)),
        ("top-level-array", json(false, "[1,2]")),
        ("trailing-content", json(false, r#"{"a":1} x"#)),
        ("unterminated", json(false, r#"{"a":"b"#)),
    ];

    let csv = |delim: u8, row: &str| prefixed(csv_selector(delim), row.as_bytes());
    let csv_seeds = vec![
        ("access", csv(b',', "10.0.0.1,2026-09-07T06:52:01Z,GET,\"/a,b\",200,612,0.012")),
        (
            "doubled-quotes",
            csv(
                b',',
                r#"203.0.113.7,"GET /search?q=""a,b"" HTTP/1.1",200,"Mozilla/5.0 (""compatible"")""#,
            ),
        ),
        ("header", csv(b',', "client,time,method,path,status,bytes,duration")),
        ("empty-fields", csv(b',', ",,\"\",,")),
        ("quote-in-unquoted", csv(b',', r#"he said "hi",b"#)),
        ("tab", csv(b'\t', "203.0.113.7\t-\t\"GET / HTTP/1.1\"\t200")),
        ("semicolon", csv(b';', "a;\"b;c\";\"\"\"\"")),
        ("pipe", csv(b'|', "frontend|backend/srv1|0/0/1/2/3|200")),
        ("unterminated", csv(b',', "a,\"b")),
        ("trailing-after-quote", csv(b',', "\"a\"b,c")),
        ("empty", csv(b',', "")),
    ];

    let logfmt = |bare: bool, line: &str| prefixed(bare as u8, line.as_bytes());
    let logfmt_seeds = vec![
        (
            "go-kit",
            logfmt(
                false,
                "level=info ts=2026-09-07T06:52:01Z caller=metrics.go:159 component=frontend \
                 org_id=fake latency=fast duration=12.3ms status=200 msg=\"query stats\"",
            ),
        ),
        ("escaped", logfmt(false, "level=info query=\"{job=\\\"nginx\\\"}\" status=200")),
        (
            "lua-example",
            logfmt(
                false,
                "level=info msg=request method=GET path=/api/orders/42 status=200 dur=12ms \
                 user=alice",
            ),
        ),
        ("go-log-prefix", logfmt(false, "2026/09/07 12:00:00 level=info msg=started")),
        ("barewords", logfmt(true, "debug cached level=info ready")),
        ("no-space-after-quote", logfmt(false, "a=\"x\"b=1 c=\"y\"z")),
        ("keyless", logfmt(false, "=1 a=2 == b=c=d")),
        ("escapes", logfmt(false, "a=\"\\\\ \\\" \\n \\r \\t \\u00e9 \\x41\" b=\"\"")),
        ("crlf-tabs", logfmt(false, "a=1\tb=2\r\nc= d")),
        ("unterminated", logfmt(false, "a=1 b=\"open")),
        ("trailing-backslash", logfmt(false, "a=\"x\\")),
    ];

    let kv = |index: usize, bare: bool, line: &str| {
        debug_assert!(index < KV_SEPARATORS.len());
        prefixed((index as u8) << 1 | bare as u8, line.as_bytes())
    };
    let kv_seeds = vec![
        ("query", kv(0, false, "a=1&b=2&c=hello")),
        ("query-empty-segments", kv(0, true, "a=1&&b=2&flag&=3&")),
        ("space", kv(1, false, "a=1 b=2 c=hello")),
        ("comma", kv(2, false, "a=1, b=2,c = hello")),
        ("colon", kv(3, false, "level: info, msg: hello world, dur: 3ms")),
        ("cookie", kv(4, true, "session=abc123; theme=dark; HttpOnly; lang=en")),
        (
            "ltsv",
            kv(5, false, "host:127.0.0.1\tident:-\ttime:[07/Sep/2026:06:52:01 +0000]\tstatus:200"),
        ),
        ("value-after-space", kv(6, false, "a 1,b 2,c  three four")),
        ("arrows", kv(7, false, "a -> 1 :: b -> 2 -> 3 :: c")),
        ("non-ascii", kv(8, true, "a→1¦b→2¦c")),
        ("no-pairs", kv(0, false, "=1&=2")),
    ];

    vec![
        ("message_json", json_seeds),
        ("message_csv", csv_seeds),
        ("message_logfmt", logfmt_seeds),
        ("message_kv", kv_seeds),
    ]
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
                "collectd",
                "forwarded",
                "graphite_pickle",
                "graphite_plaintext",
                "hll_bytes",
                "message_csv",
                "message_json",
                "message_kv",
                "message_logfmt",
                "native_batch",
                "native_control",
                "native_frame",
                "native_hop_batch",
                "otlp_grpc",
                "otlp_json",
                "otlp_proto",
                "prom_decompress",
                "prom_remote_write",
                "prom_text",
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
