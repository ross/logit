//! A decompressed remote-write body through `remote_write::decode`. Byte 0's bit 0 picks the
//! message (`0` 1.0 `WriteRequest`, `1` 2.0 `Request`), and bit 1 the mode: `0` takes the rest
//! as the body, `1` builds a request from it with `prost` ([`Build`]), so the symbol-table and
//! label rules are reachable without the fuzzer first finding protobuf's framing.
//!
//! Oracles:
//! - built requests: the verdict `remote_write.rs`'s "Malformed input: what is a `400`, and what
//!   is a counted skip" section gives. A 2.0 request is `Malformed` if and only if `symbols[0]`
//!   isn't empty, a `labels_refs` list has an odd length, or a reference is out of range: in any
//!   series' labels, or in the metadata or an exemplar of a series whose labels are valid, the only
//!   ones whose metadata and exemplars are read. A 1.0 request is never `Malformed`. Otherwise
//!   `invalid_labels` counts the series with no `__name__`, an empty label name or value, or a
//!   repeated name (`series_labels`), and `native_histogram` the native histograms of every other
//!   series. `prometheus_remote_write_fixed_point.rs`'s
//!   `version_2_symbol_table_errors_fail_the_whole_request` pins one case of each rule;
//! - every input that decodes, in both modes and versions: `B1 = encode(decode(x))` may differ
//!   from `x` by the module doc's "Permitted normalizations", so the property starts at `B1`:
//!   it decodes with nothing skipped or degraded, and encoding that decode gives `B1`'s bytes.
#![no_main]

#[path = "shared/prometheus.rs"]
mod prometheus;

use libfuzzer_sys::fuzz_target;
use logit_proto::prometheus::generated::io::prometheus::write::v2 as pb2;
use logit_proto::prometheus::generated::prometheus as pb1;
use logit_proto::prometheus::remote_write::{decode, encode, Decoded, Version};
use logit_proto::prometheus::{PrometheusEncoder, STALE_NAN_BITS};
use logit_proto::CodecError;
use prometheus::{counted_decoder, reasons};
use prost::Message;
use std::collections::BTreeMap;

/// The strings a built request draws from: the names the decoder routes on, label names and
/// values it validates, and an exemplar's trace reference.
const STRINGS: [&str; 20] = [
    "",
    "__name__",
    "foo",
    "foo_total",
    "foo_bucket",
    "foo_count",
    "foo_sum",
    "le",
    "quantile",
    "0.5",
    "+Inf",
    "job",
    "a",
    "A help text.",
    "seconds",
    "trace_id",
    "0123456789abcdef0123456789abcdef",
    "span_id",
    "fedcba9876543210",
    "foo_created",
];

/// Sample and exemplar values: ordinary readings, the edges, `NaN`, and the stale marker.
fn value(byte: u8) -> f64 {
    match byte % 8 {
        0 => 1.0,
        1 => 0.0,
        2 => -1.0,
        3 => 2.5,
        4 => f64::NAN,
        5 => f64::from_bits(STALE_NAN_BITS),
        6 => f64::INFINITY,
        _ => 7.0,
    }
}

/// Reads the input a byte at a time, `0` once it runs out.
struct Build<'a>(&'a [u8]);

impl Build<'_> {
    fn byte(&mut self) -> u8 {
        let Some((&first, rest)) = self.0.split_first() else { return 0 };
        self.0 = rest;
        first
    }

    fn string(&mut self) -> String {
        STRINGS[usize::from(self.byte()) % STRINGS.len()].to_string()
    }

    /// A symbol reference into a table of `k`: in range, past it by up to 3, or `u32::MAX`.
    fn reference(&mut self, k: usize) -> u32 {
        match self.byte() {
            255 => u32::MAX,
            byte => u32::from(byte) % (k as u32 + 4),
        }
    }

    /// Up to 8 references, so an odd count comes up.
    fn references(&mut self, k: usize) -> Vec<u32> {
        (0..self.byte() % 9).map(|_| self.reference(k)).collect()
    }

    fn samples(&mut self) -> Vec<(f64, i64, i64)> {
        (0..self.byte() % 4)
            .map(|_| {
                let value = value(self.byte());
                let timestamp = i64::from(self.byte() % 3) * 1000;
                let start = i64::from(self.byte() % 2);
                (value, timestamp, start)
            })
            .collect()
    }

    fn v1(&mut self) -> pb1::WriteRequest {
        let timeseries = (0..self.byte() % 5)
            .map(|_| pb1::TimeSeries {
                labels: (0..self.byte() % 5)
                    .map(|_| pb1::Label { name: self.string(), value: self.string() })
                    .collect(),
                samples: self
                    .samples()
                    .into_iter()
                    .map(|(value, timestamp, _)| pb1::Sample { value, timestamp })
                    .collect(),
                exemplars: (0..self.byte() % 3)
                    .map(|_| pb1::Exemplar {
                        labels: (0..self.byte() % 3)
                            .map(|_| pb1::Label { name: self.string(), value: self.string() })
                            .collect(),
                        value: value(self.byte()),
                        timestamp: i64::from(self.byte() % 3) * 1000,
                    })
                    .collect(),
                histograms: (0..u8::from(self.byte() % 8 == 0))
                    .map(|_| Default::default())
                    .collect(),
            })
            .collect();
        let metadata = (0..self.byte() % 4)
            .map(|_| pb1::MetricMetadata {
                r#type: i32::from(self.byte() % 9),
                metric_family_name: self.string(),
                help: self.string(),
                unit: self.string(),
            })
            .collect();
        pb1::WriteRequest { timeseries, metadata }
    }

    fn v2(&mut self) -> pb2::Request {
        let k = usize::from(self.byte() % 17);
        let mut symbols: Vec<String> = (0..k).map(|_| self.string()).collect();
        if let Some(first) = symbols.first_mut() {
            if self.byte() % 8 != 0 {
                first.clear();
            }
        }
        let timeseries = (0..self.byte() % 5)
            .map(|_| pb2::TimeSeries {
                labels_refs: self.references(k),
                samples: self
                    .samples()
                    .into_iter()
                    .map(|(value, timestamp, start_timestamp)| pb2::Sample {
                        value,
                        timestamp,
                        start_timestamp,
                    })
                    .collect(),
                histograms: (0..u8::from(self.byte() % 8 == 0))
                    .map(|_| Default::default())
                    .collect(),
                exemplars: (0..self.byte() % 3)
                    .map(|_| pb2::Exemplar {
                        labels_refs: self.references(k),
                        value: value(self.byte()),
                        timestamp: i64::from(self.byte() % 3) * 1000,
                    })
                    .collect(),
                metadata: (self.byte() % 3 != 0).then(|| pb2::Metadata {
                    r#type: i32::from(self.byte() % 9),
                    help_ref: self.reference(k),
                    unit_ref: self.reference(k),
                }),
            })
            .collect();
        pb2::Request { symbols, timeseries }
    }
}

/// Whether `series_labels` accepts a label set: a non-empty `__name__`, no empty name or value,
/// and no name twice.
fn labels_valid(pairs: &[(&str, &str)]) -> bool {
    let mut names: Vec<&str> = pairs.iter().map(|(name, _)| *name).collect();
    names.sort_unstable();
    pairs.iter().all(|(name, value)| !name.is_empty() && !value.is_empty())
        && names.contains(&"__name__")
        && names.windows(2).all(|w| w[0] != w[1])
}

/// What the doc's table expects of a request: `Err(())` for `Malformed`, else the
/// `invalid_labels` and `native_histogram` counts.
type Verdict = Result<(u64, u64), ()>;

fn v1_verdict(request: &pb1::WriteRequest) -> Verdict {
    let (mut invalid, mut histograms) = (0, 0);
    for series in &request.timeseries {
        let pairs: Vec<(&str, &str)> =
            series.labels.iter().map(|l| (l.name.as_str(), l.value.as_str())).collect();
        if labels_valid(&pairs) {
            histograms += series.histograms.len() as u64;
        } else {
            invalid += 1;
        }
    }
    Ok((invalid, histograms))
}

fn v2_verdict(request: &pb2::Request) -> Verdict {
    let symbols = &request.symbols;
    if symbols.first().is_some_and(|first| !first.is_empty()) {
        return Err(());
    }
    let in_range = |reference: u32| (reference as usize) < symbols.len();
    let resolve = |refs: &[u32]| -> Option<Vec<(&str, &str)>> {
        if refs.len() % 2 != 0 || !refs.iter().all(|r| in_range(*r)) {
            return None;
        }
        Some(
            refs.chunks(2)
                .map(|p| (symbols[p[0] as usize].as_str(), symbols[p[1] as usize].as_str()))
                .collect(),
        )
    };
    let mut resolved = Vec::new();
    for series in &request.timeseries {
        resolved.push(resolve(&series.labels_refs).ok_or(())?);
    }
    let (mut invalid, mut histograms) = (0, 0);
    for (series, pairs) in request.timeseries.iter().zip(&resolved) {
        if !labels_valid(pairs) {
            invalid += 1;
            continue;
        }
        histograms += series.histograms.len() as u64;
        if let Some(metadata) = &series.metadata {
            // `0` is the empty symbol and means absent, even when the table is empty.
            if [metadata.help_ref, metadata.unit_ref].iter().any(|r| *r != 0 && !in_range(*r)) {
                return Err(());
            }
        }
        if series.exemplars.iter().any(|e| resolve(&e.labels_refs).is_none()) {
            return Err(());
        }
    }
    Ok((invalid, histograms))
}

struct Counted {
    result: Result<Decoded, CodecError>,
    skipped: BTreeMap<String, u64>,
    degraded: BTreeMap<String, u64>,
}

fn decode_counted(body: &[u8], version: Version) -> Counted {
    let (registry, mut decoder) = counted_decoder();
    let result = decode(body, version, &mut decoder);
    Counted {
        result,
        skipped: reasons(&registry, "skipped"),
        degraded: reasons(&registry, "degraded"),
    }
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, rest)) = data.split_first() else { return };
    let version = if selector & 1 == 0 { Version::V1 } else { Version::V2 };
    let (body, verdict) = if selector & 2 == 0 {
        (rest.to_vec(), None)
    } else {
        let mut build = Build(rest);
        match version {
            Version::V1 => {
                let request = build.v1();
                (request.encode_to_vec(), Some(v1_verdict(&request)))
            }
            Version::V2 => {
                let request = build.v2();
                (request.encode_to_vec(), Some(v2_verdict(&request)))
            }
        }
    };

    let first = decode_counted(&body, version);
    if let Some(verdict) = verdict {
        match (&first.result, verdict) {
            (Err(CodecError::Malformed(_)), Err(())) => {}
            (Ok(decoded), Ok((invalid, histograms))) => {
                let count = |reason: &str| first.skipped.get(reason).copied().unwrap_or(0);
                assert_eq!(count("invalid_labels"), invalid, "built: invalid_labels");
                assert_eq!(count("native_histogram"), histograms, "built: native_histogram");
                assert_eq!(decoded.histograms_skipped, histograms, "built: histograms_skipped");
            }
            (result, verdict) => {
                panic!(
                    "built: decoded {:?} where the doc's table says {verdict:?}",
                    result.as_ref().err()
                )
            }
        }
    }
    let Ok(decoded) = first.result else { return };

    let b1 = encode(&decoded.groups, version, &mut PrometheusEncoder::new());
    let second = decode_counted(&b1, version);
    let redecoded = second.result.expect("fixed point: B1 fails to decode");
    assert!(second.skipped.is_empty(), "fixed point: B1 skips {:?}", second.skipped);
    assert!(second.degraded.is_empty(), "fixed point: B1 degrades {:?}", second.degraded);
    let b2 = encode(&redecoded.groups, version, &mut PrometheusEncoder::new());
    assert!(
        b1 == b2,
        "fixed point: encode(decode(B1)) != B1\nfirst: {:?}\nsecond: {:?}",
        decoded.groups,
        redecoded.groups
    );
});
