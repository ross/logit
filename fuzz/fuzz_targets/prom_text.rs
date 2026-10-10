//! A scrape body through `prometheus::text::parse_with` and its assembler, as `prometheus_in`
//! hands one over. Byte 0's low bit picks the dialect (`0` text 0.0.4, `1` OpenMetrics 1.0); the
//! rest is the body.
//!
//! Oracles, each over every input:
//! - skip or reject: a body fails as a whole only for the two reasons `text.rs`'s "Malformed
//!   input" section names, an OpenMetrics body with no `# EOF` line and content after one, and
//!   only for those; every other fault is a counted skip inside an `Ok`;
//! - canonical text: `T1 = write(parse(x))` may differ from `x` by the "Permitted
//!   normalizations" in `crates/logit-proto/src/prometheus/mod.rs`, so the property starts at
//!   `T1`: it parses with nothing skipped or degraded, and writing that parse gives `T1`'s bytes.
//!   Comparing text sidesteps `NaN` under `PartialEq`;
//! - histograms (`mod.rs`'s `Point::Histogram`, `text.rs`'s "Leniencies"): bounds strictly
//!   ascending and never `NaN`, the last one `+Inf`, its count the total; the model mapping skips a
//!   series if and only if its cumulative counts decrease, and otherwise keeps every count.
//!   Summary quantiles are strictly ascending numbers. Their range isn't checked, by the parser or
//!   here: Prometheus's own parsers keep a quantile outside `[0, 1]` too;
//! - counts: a `_count`, `_gcount`, or `_bucket` line whose value is `NaN`, infinite, or negative
//!   never becomes a `u64` count (`assemble.rs`'s `count_value`), so writing `+Inf` or `-Inf` in
//!   place of every such value leaves each histogram and summary as it was.
#![no_main]

#[path = "shared/prometheus.rs"]
mod prometheus;

use libfuzzer_sys::fuzz_target;
use logit_core::MetricKind;
use logit_proto::prometheus::text::{parse_with, write, Dialect};
use logit_proto::prometheus::{families_to_events_with, MetricFamily, Point};
use logit_proto::CodecError;
use prometheus::{counted_decoder, reasons};

const RECEIVED_AT: i64 = 1_700_000_000_123_456_789;

struct Parsed {
    result: Result<Vec<MetricFamily>, CodecError>,
    skipped: std::collections::BTreeMap<String, u64>,
    degraded: std::collections::BTreeMap<String, u64>,
}

fn parse(body: &[u8], dialect: Dialect) -> Parsed {
    let (registry, mut decoder) = counted_decoder();
    let result = parse_with(body, dialect, &mut decoder);
    Parsed {
        result,
        skipped: reasons(&registry, "skipped"),
        degraded: reasons(&registry, "degraded"),
    }
}

fn written(families: &[MetricFamily], dialect: Dialect) -> Vec<u8> {
    let mut out = Vec::new();
    write(families, dialect, &mut out);
    out
}

/// A line as the parser sees it: `[ \t]` off the front, `[ \t\r]` off the back.
fn trimmed(mut line: &[u8]) -> &[u8] {
    while let [b' ' | b'\t', rest @ ..] = line {
        line = rest;
    }
    while let [rest @ .., b' ' | b'\t' | b'\r'] = line {
        line = rest;
    }
    line
}

/// The whole-body verdict `text.rs`'s "Malformed input" section gives: the reason, or `None`.
fn expected_rejection(body: &[u8], dialect: Dialect) -> Option<&'static str> {
    if dialect != Dialect::OpenMetrics1_0 {
        return None;
    }
    let mut saw_eof = false;
    for line in body.split(|&b| b == b'\n').map(trimmed).filter(|l| !l.is_empty()) {
        if saw_eof {
            return Some("content after `# EOF`");
        }
        let text = std::str::from_utf8(line).ok();
        if text.and_then(|t| t.strip_prefix('#')).is_some_and(|rest| rest.trim_start() == "EOF") {
            saw_eof = true;
        }
    }
    (!saw_eof).then_some("openmetrics exposition is missing its trailing `# EOF`")
}

/// A sample line's metric name and value token, read without the parser: a name, an optional
/// quoted label set, whitespace, a token. `None` for anything else, a comment included.
fn name_and_value(line: &[u8]) -> Option<(&[u8], &[u8])> {
    let name_end = line
        .iter()
        .position(|b| !(b.is_ascii_alphanumeric() || *b == b'_' || *b == b':'))
        .unwrap_or(line.len());
    let (name, mut rest) = line.split_at(name_end);
    if name.is_empty() {
        return None;
    }
    if rest.first() == Some(&b'{') {
        let (mut quoted, mut escaped) = (false, false);
        let close = rest.iter().position(|&b| {
            if escaped {
                escaped = false;
            } else if quoted && b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                quoted = !quoted;
            } else if !quoted && b == b'}' {
                return true;
            }
            false
        })?;
        rest = &rest[close + 1..];
    }
    let value_start = rest.iter().position(|b| *b != b' ' && *b != b'\t')?;
    if value_start == 0 {
        return None;
    }
    let value = &rest[value_start..];
    let end = value.iter().position(|b| *b == b' ' || *b == b'\t').unwrap_or(value.len());
    Some((name, &value[..end]))
}

/// The byte range of a line's value token when the line is one the count oracle rewrites: a
/// count-shaped name whose value can't be a count. A line with a `quantile` label is left alone,
/// since a summary named `x_count` carries its quantile values, which may be anything, on lines
/// called `x_count`.
fn bad_count_value(line: &[u8]) -> Option<std::ops::Range<usize>> {
    if line.windows(8).any(|w| w == b"quantile") {
        return None;
    }
    let (name, value) = name_and_value(trimmed(line))?;
    let counted = [&b"_count"[..], b"_gcount", b"_bucket"].iter().any(|s| name.ends_with(s));
    let parsed = std::str::from_utf8(value).ok().and_then(|v| v.parse::<f64>().ok());
    if !counted || !parsed.is_some_and(|v| !v.is_finite() || v < 0.0) {
        return None;
    }
    let start = value.as_ptr() as usize - line.as_ptr() as usize;
    Some(start..start + value.len())
}

/// `body` with every [`bad_count_value`] replaced by `replacement`.
fn with_bad_counts(body: &[u8], replacement: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(body.len());
    for (i, line) in body.split(|&b| b == b'\n').enumerate() {
        if i > 0 {
            out.push(b'\n');
        }
        match bad_count_value(line) {
            Some(range) => {
                out.extend_from_slice(&line[..range.start]);
                out.extend_from_slice(replacement);
                out.extend_from_slice(&line[range.end..]);
            }
            None => out.extend_from_slice(line),
        }
    }
    out
}

/// Every histogram's and summary's point, by family and label set.
fn counted_points(families: &[MetricFamily]) -> Vec<String> {
    let mut out = Vec::new();
    for family in families {
        for series in &family.series {
            if matches!(series.point, Point::Histogram { .. } | Point::Summary { .. }) {
                out.push(format!("{} {:?} {:?}", family.name, series.labels, series.point));
            }
        }
    }
    out
}

fn check_points(families: &[MetricFamily]) {
    for family in families {
        for series in &family.series {
            match &series.point {
                Point::Stale => panic!("model: text never decodes a stale marker"),
                Point::Histogram { buckets, count, .. } => {
                    assert!(!buckets.is_empty(), "histograms: no buckets");
                    assert!(
                        buckets.iter().all(|(le, _)| !le.is_nan()),
                        "histograms: a NaN bound in {buckets:?}"
                    );
                    assert!(
                        buckets.windows(2).all(|w| w[0].0 < w[1].0),
                        "histograms: bounds not strictly ascending: {buckets:?}"
                    );
                    let (last, total) = buckets[buckets.len() - 1];
                    assert_eq!(last, f64::INFINITY, "histograms: no +Inf bucket: {buckets:?}");
                    assert_eq!(*count, total, "histograms: the count isn't the +Inf bucket");
                }
                Point::Summary { quantiles, .. } => {
                    assert!(
                        quantiles.iter().all(|(q, _)| !q.is_nan())
                            && quantiles.windows(2).all(|w| w[0].0 < w[1].0),
                        "summaries: quantiles not strictly ascending numbers: {quantiles:?}"
                    );
                }
                _ => {}
            }
        }
    }

    let mut skipped = Vec::new();
    let mut decoder = logit_proto::prometheus::PrometheusDecoder::new();
    let events = families_to_events_with(families, RECEIVED_AT, &mut decoder, &mut |f, s| {
        skipped.push((f.name.clone(), s.labels.clone()));
    });
    let mut kept = events.iter();
    for family in families {
        for series in &family.series {
            let decreasing = match &series.point {
                Point::Histogram { buckets, .. } => buckets.windows(2).any(|w| w[1].1 < w[0].1),
                _ => false,
            };
            let was_skipped = skipped.contains(&(family.name.clone(), series.labels.clone()));
            assert_eq!(
                was_skipped, decreasing,
                "histograms: the model skips a series if and only if its counts decrease"
            );
            if was_skipped {
                continue;
            }
            let event = kept.next().expect("model: one event per kept series");
            if let (Point::Histogram { count, .. }, MetricKind::Histogram(h)) =
                (&series.point, &event.metrics[0].kind)
            {
                let sum: u64 = h.buckets.iter().map(|(_, c)| *c).sum();
                assert_eq!(sum, *count, "histograms: per-bucket counts don't add up to the total");
            }
        }
    }
    assert!(kept.next().is_none(), "model: more events than kept series");
}

fuzz_target!(|data: &[u8]| {
    let Some((&selector, body)) = data.split_first() else { return };
    let dialect = if selector & 1 == 0 { Dialect::Text0_0_4 } else { Dialect::OpenMetrics1_0 };
    let parsed = parse(body, dialect);

    let expected = expected_rejection(body, dialect);
    let families = match (parsed.result, expected) {
        (Ok(families), None) => families,
        (Err(CodecError::Malformed(reason)), Some(expected)) => {
            assert_eq!(reason, expected, "skip or reject: the wrong rejection");
            return;
        }
        (result, expected) => {
            panic!("skip or reject: {result:?} where the doc's table says {expected:?}")
        }
    };

    check_points(&families);

    let t1 = written(&families, dialect);
    let again = parse(&t1, dialect);
    let reparsed = again.result.expect("canonical text: T1 fails to parse");
    assert!(again.skipped.is_empty(), "canonical text: T1 skips {:?}", again.skipped);
    assert!(again.degraded.is_empty(), "canonical text: T1 degrades {:?}", again.degraded);
    let t2 = written(&reparsed, dialect);
    assert!(
        t1 == t2,
        "canonical text: write(parse(T1)) != T1\nT1:\n{}\nT2:\n{}",
        String::from_utf8_lossy(&t1),
        String::from_utf8_lossy(&t2)
    );

    // An unguarded cast would read `+Inf` as `u64::MAX` and `-Inf` as `0`; a guarded one skips
    // both, so the line still routes where it did and nothing else changes.
    if body.split(|&b| b == b'\n').any(|line| bad_count_value(line).is_some()) {
        for replacement in [&b"+Inf"[..], b"-Inf"] {
            let swapped = parse(&with_bad_counts(body, replacement), dialect);
            let swapped = swapped.result.expect("counts: swapping a value changed the verdict");
            assert_eq!(
                counted_points(&families),
                counted_points(&swapped),
                "counts: a non-finite or negative count line changed a histogram or summary"
            );
        }
    }
});
