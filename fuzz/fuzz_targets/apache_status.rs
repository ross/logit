//! One scraped body through `apache_status::parse` and `ApacheStatus::to_events`, as
//! `apache_status_in` decodes it. Oracles, over every input:
//! - the parser accepts the body if and only if one of its `\n`-terminated lines starts with a
//!   listed key and `": "`;
//! - each field equals what an independent reading of that key's last such line gives: digits
//!   that fit an `i64` for a count (times 1024 for `Total kBytes`), a finite float for a float,
//!   and a byte tally for `Scoreboard`; and each listed key whose last value didn't read is named
//!   in `malformed`;
//! - `to_events` gives one event per field present, plus twelve for a scoreboard and one more when
//!   it holds an unknown byte, each a single point whose value is its field's.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_core::{MetricKind, MetricValue, Sum};
use logit_proto::apache_status::{parse, SCOREBOARD_STATES};

const COUNTS: [&str; 9] = [
    "ServerUptimeSeconds",
    "Total Accesses",
    "Total kBytes",
    "Total Duration",
    "BusyWorkers",
    "IdleWorkers",
    "ConnsTotal",
    "ConnsAsyncWriting",
    "ConnsAsyncKeepAlive",
];
const MORE_COUNTS: [&str; 1] = ["ConnsAsyncClosing"];
const FLOATS: [&str; 8] = [
    "Load1",
    "Load5",
    "Load15",
    "CPUUser",
    "CPUSystem",
    "CPUChildrenUser",
    "CPUChildrenSystem",
    "CPULoad",
];

/// The value on the last `\n`-terminated line whose key is `key`.
fn last<'a>(data: &'a [u8], key: &str) -> Option<&'a [u8]> {
    let end = data.iter().rposition(|&b| b == b'\n')?;
    let mut prefix = key.as_bytes().to_vec();
    prefix.extend_from_slice(b": ");
    data[..end]
        .split(|&b| b == b'\n')
        .filter_map(|line| line.strip_suffix(b"\r").unwrap_or(line).strip_prefix(&prefix[..]))
        .last()
}

fn count(v: &[u8]) -> Option<i64> {
    if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(v).ok()?.parse().ok()
}

fn float(v: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(v).ok()?;
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_digit() || b".eE+-".contains(&b)) {
        return None;
    }
    s.parse::<f64>().ok().filter(|f| f.is_finite())
}

fn bits(v: Option<f64>) -> Option<u64> {
    v.map(f64::to_bits)
}

fuzz_target!(|data: &[u8]| {
    let all: Vec<&str> =
        COUNTS.iter().chain(&MORE_COUNTS).chain(&FLOATS).copied().chain(["Scoreboard"]).collect();
    let present = all.iter().any(|key| last(data, key).is_some());
    let status = match parse(data) {
        Ok(status) => status,
        Err(_) => {
            assert!(!present);
            return;
        }
    };
    assert!(present);
    assert_eq!(status.truncated, data.last().is_some_and(|&b| b != b'\n'));

    let c = |key| last(data, key).and_then(count);
    assert_eq!(status.uptime, c("ServerUptimeSeconds"));
    assert_eq!(status.accesses, c("Total Accesses"));
    assert_eq!(status.traffic_bytes, c("Total kBytes").and_then(|k| k.checked_mul(1024)));
    assert_eq!(status.duration_ms, c("Total Duration"));
    assert_eq!(status.busy_workers, c("BusyWorkers"));
    assert_eq!(status.idle_workers, c("IdleWorkers"));
    assert_eq!(status.conns_total, c("ConnsTotal"));
    assert_eq!(
        status.conns_async,
        [c("ConnsAsyncWriting"), c("ConnsAsyncKeepAlive"), c("ConnsAsyncClosing")]
    );
    let f = |key| bits(last(data, key).and_then(float));
    assert_eq!(status.load.map(bits), [f("Load1"), f("Load5"), f("Load15")]);
    assert_eq!(
        status.cpu.map(bits),
        [f("CPUUser"), f("CPUSystem"), f("CPUChildrenUser"), f("CPUChildrenSystem")]
    );
    assert_eq!(bits(status.cpu_load), f("CPULoad"));

    let mut malformed: Vec<&str> = status.malformed.iter().map(|(k, _)| *k).collect();
    malformed.sort();
    let mut expected: Vec<&str> = all
        .iter()
        .copied()
        .filter(|&key| key != "Scoreboard")
        .filter(|&key| last(data, key).is_some())
        .filter(|&key| {
            let v = last(data, key).unwrap();
            if FLOATS.contains(&key) {
                float(v).is_none()
            } else if key == "Total kBytes" {
                count(v).and_then(|k| k.checked_mul(1024)).is_none()
            } else {
                count(v).is_none()
            }
        })
        .collect();
    expected.sort();
    assert_eq!(malformed, expected);

    let board = last(data, "Scoreboard");
    match (&status.scoreboard, board) {
        (None, None) => {}
        (Some(parsed), Some(raw)) => {
            assert_eq!(parsed.slots, raw.len() as i64);
            for (i, (byte, _)) in SCOREBOARD_STATES.iter().enumerate() {
                assert_eq!(parsed.states[i], raw.iter().filter(|b| *b == byte).count() as i64);
            }
            assert_eq!(parsed.states.iter().sum::<i64>() + parsed.unknown, parsed.slots);
        }
        other => panic!("scoreboard mismatch: {other:?}"),
    }

    let mut values: Vec<f64> = Vec::new();
    values.extend(status.uptime.map(|v| v as f64));
    values.extend(status.load.iter().flatten());
    for v in [status.accesses, status.traffic_bytes, status.duration_ms].into_iter().flatten() {
        values.push(v as f64);
    }
    values.extend(status.cpu.iter().flatten());
    values.extend(status.cpu_load);
    for v in [status.busy_workers, status.idle_workers, status.conns_total].into_iter().flatten() {
        values.push(v as f64);
    }
    values.extend(status.conns_async.iter().flatten().map(|&v| v as f64));
    if let Some(board) = &status.scoreboard {
        values.extend(board.states.iter().map(|&v| v as f64));
        if board.unknown > 0 {
            values.push(board.unknown as f64);
        }
        values.push(board.slots as f64);
    }
    let events = status.to_events(0);
    let got: Vec<f64> = events
        .iter()
        .map(|e| {
            assert_eq!(e.metrics.len(), 1);
            match e.metrics[0].kind {
                MetricKind::Sum(Sum { value: MetricValue::I64(v), .. })
                | MetricKind::Gauge(MetricValue::I64(v)) => v as f64,
                MetricKind::Gauge(MetricValue::F64(v)) => v,
                ref other => panic!("unexpected kind {other:?}"),
            }
        })
        .collect();
    assert_eq!(
        got.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
        values.iter().map(|v| v.to_bits()).collect::<Vec<_>>()
    );
});
