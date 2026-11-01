//! Recorded interop fixtures: `mod_status` pages real httpd 2.4.69 servers served under the
//! `event`, `worker`, and `prefork` MPMs, `ExtendedStatus Off`, and `mod_ssl` with an `shmcb`
//! session cache, replayed through `logit_proto::apache_status`. See
//! `testdata/interop/apache-status/README.md` for the provenance table; regenerate with
//! `script/record-fixtures apache-status`.
//!
//! Assertions are on the mapping (the module doc's table, one row per event), on which keys each
//! variant omits, and on what the recording set up, never on a re-record's exact counts: each
//! value must equal what the body's own line for its key reads.

use logit_core::interner::resolve;
use logit_core::{Event, MetricKind, MetricValue, Sum, Temporality};
use logit_proto::apache_status::{parse, ParseError};
use std::path::PathBuf;

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/apache-status")
}

/// The value on `body`'s line for `key`, read independently of the decoder.
fn value<'a>(body: &'a str, key: &str) -> Option<&'a str> {
    body.lines().find_map(|line| line.strip_prefix(key)?.strip_prefix(": "))
}

fn point(e: &Event) -> (&'static str, &'static str, f64, Vec<(String, String)>) {
    assert_eq!(e.metrics.len(), 1);
    let m = &e.metrics[0];
    let v = match m.kind {
        MetricKind::Sum(Sum {
            value: MetricValue::I64(v),
            temporality: Temporality::Cumulative,
            monotonic: true,
        }) => v as f64,
        MetricKind::Gauge(MetricValue::I64(v)) => v as f64,
        MetricKind::Gauge(MetricValue::F64(v)) => v,
        ref other => panic!("unexpected kind {other:?}"),
    };
    let attrs = e
        .attributes
        .iter()
        .map(|(k, v)| (resolve(k).to_string(), v.as_str().unwrap().to_string()))
        .collect();
    (resolve(m.name), resolve(m.unit.unwrap()), v, attrs)
}

#[test]
fn every_recorded_page_maps_onto_the_table_and_omits_what_its_variant_omits() {
    let mut bodies: Vec<_> = std::fs::read_dir(corpus())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "body"))
        .collect();
    bodies.sort();
    assert_eq!(bodies.len(), 11, "two pages from each of five variants and the HTML page");

    for path in bodies {
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let bytes = std::fs::read(&path).unwrap();
        let headers = std::fs::read_to_string(path.with_extension("headers")).unwrap();
        if name == "html" {
            assert!(headers.contains("Content-Type: text/html"), "{name}: {headers}");
            assert_eq!(parse(&bytes), Err(ParseError::NoStatusKeys));
            continue;
        }
        assert!(
            headers.lines().any(|l| l == "Content-Type: text/plain; charset=ISO-8859-1"),
            "{name}: {headers}"
        );
        let body = std::str::from_utf8(&bytes).unwrap();
        let variant = name.rsplit_once('-').unwrap().0;
        let async_mpm = matches!(variant, "event" | "ext-off" | "ssl");
        let extended = variant != "ext-off";
        assert_eq!(value(body, "ServerMPM"), Some(if async_mpm { "event" } else { variant }));

        let status = parse(&bytes).unwrap_or_else(|err| panic!("{name}: {err}"));
        assert!(status.malformed.is_empty(), "{name}: {:?}", status.malformed);
        assert_eq!((status.duplicates, status.truncated), (0, false), "{name}");

        let mut want: Vec<(&str, &str, f64, Vec<(String, String)>)> = Vec::new();
        let num = |key: &str| -> f64 {
            value(body, key).unwrap_or_else(|| panic!("{name}: no {key}")).parse().unwrap()
        };
        let attrs = |pairs: &[(&str, &str)]| {
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect::<Vec<_>>()
        };
        want.push(("apache.uptime", "s", num("ServerUptimeSeconds"), vec![]));
        for (key, metric) in
            [("Load1", "apache.load.1"), ("Load5", "apache.load.5"), ("Load15", "apache.load.15")]
        {
            want.push((metric, "1", num(key), vec![]));
        }
        if extended {
            want.push(("apache.request.count", "{request}", num("Total Accesses"), vec![]));
            want.push(("apache.traffic", "By", num("Total kBytes") * 1024.0, vec![]));
            want.push(("apache.request.time", "ms", num("Total Duration"), vec![]));
            for (key, level, mode) in [
                ("CPUUser", "self", "user"),
                ("CPUSystem", "self", "system"),
                ("CPUChildrenUser", "children", "user"),
                ("CPUChildrenSystem", "children", "system"),
            ] {
                let a = attrs(&[("apache.process.level", level), ("cpu.mode", mode)]);
                want.push(("apache.cpu.time", "s", num(key), a));
            }
            // `mod_status` omits `CPULoad` while the server has spent no CPU time.
            if value(body, "CPULoad").is_some() {
                want.push(("apache.cpu.load", "%", num("CPULoad"), vec![]));
            }
        } else {
            for key in ["Total Accesses", "Total kBytes", "CPUUser", "CPULoad"] {
                assert_eq!(value(body, key), None, "{name}: {key}");
            }
        }
        want.push(("apache.worker.active", "{worker}", num("BusyWorkers"), vec![]));
        want.push(("apache.worker.idle", "{worker}", num("IdleWorkers"), vec![]));
        if async_mpm {
            want.push(("apache.connection.active", "{connection}", num("ConnsTotal"), vec![]));
            for (key, state) in [
                ("ConnsAsyncWriting", "writing"),
                ("ConnsAsyncKeepAlive", "keepalive"),
                ("ConnsAsyncClosing", "closing"),
            ] {
                let a = attrs(&[("apache.connection.state", state)]);
                want.push(("apache.connection.status", "{connection}", num(key), a));
            }
        } else {
            assert_eq!(value(body, "ConnsTotal"), None, "{name}");
        }
        let board = value(body, "Scoreboard").unwrap();
        for (c, state) in "_SRWKDCLGI.".chars().zip([
            "waiting",
            "starting",
            "reading",
            "sending",
            "keepalive",
            "dnslookup",
            "closing",
            "logging",
            "finishing",
            "idle_cleanup",
            "open",
        ]) {
            let n = board.chars().filter(|&b| b == c).count() as f64;
            let a = attrs(&[("apache.worker.state", state)]);
            want.push(("apache.worker.status", "{worker}", n, a));
        }
        assert!(board.chars().all(|c| "_SRWKDCLGI.".contains(c)), "{name}: {board}");
        want.push(("apache.worker.limit", "{worker}", board.len() as f64, vec![]));

        let got: Vec<_> = status.to_events(1).iter().map(point).collect();
        assert_eq!(got, want, "{name}");

        // The scrape counts itself on the scoreboard.
        assert!(board.contains('W'), "{name}");
        if name.ends_with("-1") {
            assert!(board.contains('R'), "{name}: the half-sent request");
            if async_mpm {
                assert!(num("ConnsAsyncKeepAlive") > 0.0, "{name}: the second keepalive wave");
            } else {
                assert!(board.matches('K').count() >= 3, "{name}: the keepalive connections");
            }
        }
        if variant == "ssl" {
            assert!(value(body, "ServerVersion").unwrap().contains("OpenSSL"), "{name}");
            assert_eq!(value(body, "CacheType"), Some("SHMCB"), "{name}");
        }
    }
}
