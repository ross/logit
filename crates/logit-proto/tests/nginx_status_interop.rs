//! Recorded interop fixtures: `stub_status` pages real nginx stable and mainline, Angie, and
//! freenginx servers served, replayed through `logit_proto::nginx_status`. See
//! `testdata/interop/nginx-status/README.md` for the provenance table; regenerate with
//! `script/record-fixtures nginx-status`.
//!
//! Assertions are on the mapping (ADR `server-status-scrape-inputs` section 9's table, one row per
//! event) and on what the recording set up, never on a re-record's exact counts: each value must
//! equal the digits the body holds at its field's place, every page's own scrape counts toward
//! `requests` and `Writing`, and each `-1` page was served with three idle connections and a
//! half-sent request held open.

use logit_core::interner::resolve;
use logit_core::{MetricKind, MetricValue, Sum, Temporality, Value};
use logit_proto::nginx_status::parse;
use std::path::PathBuf;

fn corpus() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../testdata/interop/nginx-status")
}

/// Every maximal run of ASCII digits in `body`, in order, as an `i64`.
fn digit_runs(body: &[u8]) -> Vec<i64> {
    body.split(|b| !b.is_ascii_digit())
        .filter(|run| !run.is_empty())
        .map(|run| std::str::from_utf8(run).unwrap().parse().unwrap())
        .collect()
}

#[test]
fn every_recorded_page_maps_onto_the_adrs_table() {
    let mut bodies: Vec<_> = std::fs::read_dir(corpus())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "body"))
        .collect();
    bodies.sort();
    assert_eq!(bodies.len(), 8, "two pages from each of four servers: {bodies:?}");

    for path in bodies {
        let name = path.file_stem().unwrap().to_string_lossy().into_owned();
        let body = std::fs::read(&path).unwrap();
        let headers = std::fs::read_to_string(path.with_extension("headers")).unwrap();
        assert!(
            headers.lines().any(|l| l.eq_ignore_ascii_case("content-type: text/plain")),
            "{name}: {headers}"
        );

        let status = parse(&body).unwrap_or_else(|err| panic!("{name}: {err}"));
        assert!(!status.tengine, "{name}");
        assert_eq!(status.out_of_range(), 0, "{name}");

        // Page order: active, accepts, handled, requests, reading, writing, waiting.
        let [active, accepts, handled, requests, reading, writing, waiting] = digit_runs(&body)[..]
        else {
            panic!("{name}: expected seven numbers");
        };

        let events = status.to_events(1);
        let rows: Vec<_> = events
            .iter()
            .map(|e| {
                assert_eq!(e.metrics.len(), 1, "{name}");
                let m = &e.metrics[0];
                (
                    resolve(m.name),
                    m.unit.map(resolve),
                    m.kind.clone(),
                    e.attributes.get("state").cloned(),
                )
            })
            .collect();
        let sum = |v| {
            MetricKind::Sum(Sum {
                value: MetricValue::I64(v),
                temporality: Temporality::Cumulative,
                monotonic: true,
            })
        };
        let gauge = |v| MetricKind::Gauge(MetricValue::I64(v));
        let state = |s: &str| Some(Value::str(s));
        let conns = Some("{connections}");
        assert_eq!(
            rows,
            [
                ("nginx.connections_accepted", conns, sum(accepts), None),
                ("nginx.connections_handled", conns, sum(handled), None),
                ("nginx.requests", Some("{requests}"), sum(requests), None),
                ("nginx.connections_current", conns, gauge(active), state("active")),
                ("nginx.connections_current", conns, gauge(reading), state("reading")),
                ("nginx.connections_current", conns, gauge(writing), state("writing")),
                ("nginx.connections_current", conns, gauge(waiting), state("waiting")),
            ],
            "{name}"
        );

        assert!(requests >= 1 && writing >= 1 && active >= 1, "{name}: the scrape counts itself");
        assert!(accepts >= handled, "{name}");
        if name.ends_with("-1") {
            assert!(waiting >= 3 && reading >= 1, "{name}: the held connections");
            assert!(active >= reading + writing + waiting, "{name}");
        }
    }
}
