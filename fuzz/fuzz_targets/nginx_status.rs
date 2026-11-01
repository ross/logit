//! One scraped body through `nginx_status::parse` and `StubStatus::to_events`, as
//! `nginx_stub_status_in` decodes it. Oracles, over every input the parser accepts:
//! - the body's maximal runs of ASCII digits are the page's numbers in page order, seven, or
//!   eight under Tengine's header, and each mapped field is `Some` if and only if its run fits an
//!   `i64`, with that value;
//! - `to_events` gives one event per field that fits, `7 - out_of_range()` in all, each a single
//!   `I64` point whose value is its field's;
//! - the page re-rendered in nginx's own format parses back to the same reading.
#![no_main]

use libfuzzer_sys::fuzz_target;
use logit_core::{MetricKind, MetricValue, Sum};
use logit_proto::nginx_status::{parse, StubStatus};

fn digit_runs(body: &[u8]) -> Vec<Option<i64>> {
    body.split(|b| !b.is_ascii_digit())
        .filter(|run| !run.is_empty())
        .map(|run| std::str::from_utf8(run).unwrap().parse().ok())
        .collect()
}

fn render(s: &StubStatus) -> Option<Vec<u8>> {
    let [a, ac, h, r, rd, wr, wa] =
        [s.active, s.accepts, s.handled, s.requests, s.reading, s.writing, s.waiting];
    let (a, ac, h, r, rd, wr, wa) = (a?, ac?, h?, r?, rd?, wr?, wa?);
    let (header, extra) = if s.tengine { (" request_time", " 0") } else { ("", "") };
    Some(
        format!(
            "Active connections: {a} \nserver accepts handled requests{header}\n \
             {ac} {h} {r}{extra} \nReading: {rd} Writing: {wr} Waiting: {wa} \n"
        )
        .into_bytes(),
    )
}

fuzz_target!(|data: &[u8]| {
    let Ok(status) = parse(data) else { return };

    let runs = digit_runs(data);
    let mapped = if status.tengine {
        assert_eq!(runs.len(), 8);
        [runs[0], runs[1], runs[2], runs[3], runs[5], runs[6], runs[7]]
    } else {
        assert_eq!(runs.len(), 7);
        [runs[0], runs[1], runs[2], runs[3], runs[4], runs[5], runs[6]]
    };
    let fields = [
        status.active,
        status.accepts,
        status.handled,
        status.requests,
        status.reading,
        status.writing,
        status.waiting,
    ];
    assert_eq!(fields, mapped);

    let events = status.to_events(0);
    assert_eq!(events.len(), 7 - status.out_of_range());
    // `to_events` writes the three counters, then the four gauges.
    let in_event_order = [
        status.accepts,
        status.handled,
        status.requests,
        status.active,
        status.reading,
        status.writing,
        status.waiting,
    ];
    let expected: Vec<i64> = in_event_order.into_iter().flatten().collect();
    let values: Vec<i64> = events
        .iter()
        .map(|e| {
            assert_eq!(e.metrics.len(), 1);
            match e.metrics[0].kind {
                MetricKind::Sum(Sum { value: MetricValue::I64(v), .. })
                | MetricKind::Gauge(MetricValue::I64(v)) => v,
                ref other => panic!("unexpected kind {other:?}"),
            }
        })
        .collect();
    assert_eq!(values, expected);

    if let Some(page) = render(&status) {
        assert_eq!(parse(&page).as_ref(), Ok(&status));
    }
});
