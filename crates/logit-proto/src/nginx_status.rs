//! nginx's `stub_status` page (`ngx_http_stub_status_module`), parsed into the OpenTelemetry
//! Collector's `nginxreceiver` metrics. `nginx_stub_status_in` reads every scraped body through
//! [`parse`] and [`StubStatus::to_events`]. The decisions behind the grammar and the mapping are
//! [ADR `server-status-scrape-inputs`](../../../docs/adr/server-status-scrape-inputs.md)'s
//! sections 7 to 9.
//!
//! # Grammar
//!
//! Four lines in this order, each ending in `\n`. nginx, freenginx, and Angie print the same
//! format string:
//!
//! ```text
//! Active connections: <n>
//! server accepts handled requests
//!  <accepts> <handled> <requests>
//! Reading: <n> Writing: <n> Waiting: <n>
//! ```
//!
//! - The grammar is over bytes. Words are separated by single spaces, the number line starts with
//!   one, and `<n>` is one or more ASCII digits.
//! - Trailing spaces, and a `\r` after them, are ignored before each `\n`. nginx ends lines 1, 3,
//!   and 4 with a space.
//! - The final `\n` is required: the handler ends every line it writes with one, so a body without
//!   it was cut short.
//! - The header line may read `server accepts handled requests request_time`, Tengine's form, and
//!   then the number line holds four numbers. The fourth is checked as a number and not emitted:
//!   the Collector has no name for it.
//! - Empty lines after the fourth are ignored.
//!
//! Any other body is a [`ParseError`], and no partial reading is emitted. The page is
//! fixed-format, so a mismatch means the target isn't a `stub_status` page or the body was cut.
//!
//! A field whose digits don't fit an `i64` parses as `None`, and [`StubStatus::to_events`] leaves
//! its point out; the caller counts each one (`logit.input.metrics.dropped{reason="out_of_range"}`)
//! from [`StubStatus::out_of_range`]. nginx prints the counters as an unsigned machine word
//! (`%uA`), so a connection gauge that is transiently below zero prints as a number near 2^64.
//!
//! # Mapping
//!
//! One event per row, in this order, with no attributes beyond `state` and no start time:
//!
//! | Page field | Metric | Kind | Value | Unit | `state` |
//! |---|---|---|---|---|---|
//! | `accepts` | `nginx.connections_accepted` | cumulative monotonic `Sum` | `I64` | `{connections}` | — |
//! | `handled` | `nginx.connections_handled` | cumulative monotonic `Sum` | `I64` | `{connections}` | — |
//! | `requests` | `nginx.requests` | cumulative monotonic `Sum` | `I64` | `{requests}` | — |
//! | `Active connections` | `nginx.connections_current` | `Gauge` | `I64` | `{connections}` | `active` |
//! | `Reading` | `nginx.connections_current` | `Gauge` | `I64` | `{connections}` | `reading` |
//! | `Writing` | `nginx.connections_current` | `Gauge` | `I64` | `{connections}` | `writing` |
//! | `Waiting` | `nginx.connections_current` | `Gauge` | `I64` | `{connections}` | `waiting` |
//! | Tengine `request_time` | not mapped | | | | |
//!
//! The Collector models `nginx.connections_current` as a non-monotonic cumulative `Sum`; here it
//! is a `Gauge`, which every sink reads back as a gauge with no degradation. `active` counts every
//! client connection, so it overlaps the other three states: summing across `state`
//! double-counts.

use std::fmt;

use logit_core::interner::intern;
use logit_core::{AttrMap, Event, MetricKind, MetricRecord, MetricValue, Sum, Temporality, Value};

/// `nginx.connections_accepted`.
pub const CONNECTIONS_ACCEPTED: &str = "nginx.connections_accepted";
/// `nginx.connections_handled`.
pub const CONNECTIONS_HANDLED: &str = "nginx.connections_handled";
/// `nginx.requests`.
pub const REQUESTS: &str = "nginx.requests";
/// `nginx.connections_current`, one point per [`STATE`].
pub const CONNECTIONS_CURRENT: &str = "nginx.connections_current";
/// The attribute that tells `nginx.connections_current`'s four points apart.
pub const STATE: &str = "state";

const UNIT_CONNECTIONS: &str = "{connections}";
const UNIT_REQUESTS: &str = "{requests}";

const ACTIVE_PREFIX: &[u8] = b"Active connections: ";
const HEADER: &[u8] = b"server accepts handled requests";
const HEADER_TENGINE: &[u8] = b"server accepts handled requests request_time";

/// One parsed page. Each field is `None` when its digits don't fit an `i64`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StubStatus {
    pub active: Option<i64>,
    pub accepts: Option<i64>,
    pub handled: Option<i64>,
    pub requests: Option<i64>,
    pub reading: Option<i64>,
    pub writing: Option<i64>,
    pub waiting: Option<i64>,
    /// Whether the page had Tengine's fourth `request_time` column.
    pub tengine: bool,
}

/// Why a body isn't a `stub_status` page. Its `Display` names the line and what it should hold,
/// never the body's bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// The body is empty.
    Empty,
    /// The body doesn't end in `\n`.
    Unterminated,
    /// The body ends before line `line`.
    MissingLine { line: usize },
    /// Line `line` doesn't hold what its position requires.
    BadLine { line: usize, expected: &'static str },
    /// The number line holds `found` numbers where the header names `expected`.
    ColumnCount { expected: usize, found: usize },
    /// A non-empty line follows the fourth, at `line`.
    ExtraLine { line: usize },
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("not an nginx stub_status page: ")?;
        match self {
            ParseError::Empty => f.write_str("the body is empty"),
            ParseError::Unterminated => {
                f.write_str("the body doesn't end in a newline, so it was cut short")
            }
            ParseError::MissingLine { line } => write!(f, "the body ends before line {line}"),
            ParseError::BadLine { line, expected } => write!(f, "line {line} isn't {expected}"),
            ParseError::ColumnCount { expected, found } => {
                write!(f, "line 3 holds {found} numbers where the header names {expected}")
            }
            ParseError::ExtraLine { line } => {
                write!(f, "line {line} follows the page's four lines and isn't empty")
            }
        }
    }
}

impl std::error::Error for ParseError {}

/// `line` without its trailing `\r` and then its trailing spaces.
fn trim_end(line: &[u8]) -> &[u8] {
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    let end = line.iter().rposition(|&b| b != b' ').map_or(0, |i| i + 1);
    &line[..end]
}

/// One or more ASCII digits, as `Some` when they fit an `i64`. `Err` when `digits` is empty or
/// holds anything else.
fn number(digits: &[u8]) -> Result<Option<i64>, ()> {
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(());
    }
    let mut value: i64 = 0;
    for &d in digits {
        match value.checked_mul(10).and_then(|v| v.checked_add(i64::from(d - b'0'))) {
            Some(v) => value = v,
            None => return Ok(None),
        }
    }
    Ok(Some(value))
}

/// `label` followed by a number, then `rest`: `Some((number, rest))`.
fn labelled<'a>(line: &'a [u8], label: &[u8]) -> Option<(Option<i64>, &'a [u8])> {
    let rest = line.strip_prefix(label)?;
    let end = rest.iter().position(|&b| b == b' ').unwrap_or(rest.len());
    let value = number(&rest[..end]).ok()?;
    Some((value, &rest[end..]))
}

const LINE_1: &str = "'Active connections: <n>'";
const LINE_2: &str = "'server accepts handled requests', with or without ' request_time'";
const LINE_3: &str = "a space and then space-separated numbers";
const LINE_4: &str = "'Reading: <n> Writing: <n> Waiting: <n>'";

/// Parses one `stub_status` body (the module doc's grammar).
pub fn parse(body: &[u8]) -> Result<StubStatus, ParseError> {
    if body.is_empty() {
        return Err(ParseError::Empty);
    }
    let Some(body) = body.strip_suffix(b"\n") else {
        return Err(ParseError::Unterminated);
    };
    let mut lines = body.split(|&b| b == b'\n').map(trim_end);
    let mut next = |line: usize| lines.next().ok_or(ParseError::MissingLine { line });

    let mut status = StubStatus::default();

    let line = next(1)?;
    status.active = match labelled(line, ACTIVE_PREFIX) {
        Some((value, b"")) => value,
        _ => return Err(ParseError::BadLine { line: 1, expected: LINE_1 }),
    };

    let columns = match next(2)? {
        HEADER => 3,
        HEADER_TENGINE => 4,
        _ => return Err(ParseError::BadLine { line: 2, expected: LINE_2 }),
    };
    status.tengine = columns == 4;

    let line = next(3)?;
    let Some(numbers) = line.strip_prefix(b" ") else {
        return Err(ParseError::BadLine { line: 3, expected: LINE_3 });
    };
    let mut values = Vec::with_capacity(4);
    for digits in numbers.split(|&b| b == b' ') {
        values
            .push(number(digits).map_err(|()| ParseError::BadLine { line: 3, expected: LINE_3 })?);
    }
    if values.len() != columns {
        return Err(ParseError::ColumnCount { expected: columns, found: values.len() });
    }
    status.accepts = values[0];
    status.handled = values[1];
    status.requests = values[2];

    let line = next(4)?;
    let bad = || ParseError::BadLine { line: 4, expected: LINE_4 };
    let (reading, rest) = labelled(line, b"Reading: ").ok_or_else(bad)?;
    let (writing, rest) = labelled(rest, b" Writing: ").ok_or_else(bad)?;
    let (waiting, rest) = labelled(rest, b" Waiting: ").ok_or_else(bad)?;
    if !rest.is_empty() {
        return Err(bad());
    }
    status.reading = reading;
    status.writing = writing;
    status.waiting = waiting;

    for (offset, line) in lines.enumerate() {
        if !line.is_empty() {
            return Err(ParseError::ExtraLine { line: 5 + offset });
        }
    }
    Ok(status)
}

fn record(name: &str, unit: &str, kind: MetricKind) -> MetricRecord {
    let mut record = MetricRecord::new(intern(name), kind);
    record.unit = Some(intern(unit));
    record
}

fn counter(timestamp: i64, name: &str, unit: &str, value: i64) -> Event {
    let kind = MetricKind::Sum(Sum {
        value: MetricValue::I64(value),
        temporality: Temporality::Cumulative,
        monotonic: true,
    });
    Event::metric(timestamp, AttrMap::new(), record(name, unit, kind))
}

fn connections(timestamp: i64, state: &str, value: i64) -> Event {
    let mut attributes = AttrMap::new();
    attributes.insert(STATE, Value::str(state));
    let kind = MetricKind::Gauge(MetricValue::I64(value));
    Event::metric(timestamp, attributes, record(CONNECTIONS_CURRENT, UNIT_CONNECTIONS, kind))
}

impl StubStatus {
    /// The page's points as one event each, in the module doc's table order, each stamped
    /// `timestamp`. A field that didn't fit an `i64` has no event.
    pub fn to_events(&self, timestamp: i64) -> Vec<Event> {
        let counters = [
            (CONNECTIONS_ACCEPTED, UNIT_CONNECTIONS, self.accepts),
            (CONNECTIONS_HANDLED, UNIT_CONNECTIONS, self.handled),
            (REQUESTS, UNIT_REQUESTS, self.requests),
        ];
        let gauges = [
            ("active", self.active),
            ("reading", self.reading),
            ("writing", self.writing),
            ("waiting", self.waiting),
        ];
        let mut events = Vec::with_capacity(7);
        for (name, unit, value) in counters {
            if let Some(value) = value {
                events.push(counter(timestamp, name, unit, value));
            }
        }
        for (state, value) in gauges {
            if let Some(value) = value {
                events.push(connections(timestamp, state, value));
            }
        }
        events
    }

    /// How many of the seven mapped fields didn't fit an `i64`, and so have no event.
    pub fn out_of_range(&self) -> usize {
        [
            self.active,
            self.accepts,
            self.handled,
            self.requests,
            self.reading,
            self.writing,
            self.waiting,
        ]
        .iter()
        .filter(|v| v.is_none())
        .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::resolve;

    /// nginx 1.31.8's page, verbatim: lines 1, 3, and 4 end in a space.
    const PAGE: &[u8] = b"Active connections: 5 \nserver accepts handled requests\n 6 6 3 \n\
Reading: 1 Writing: 1 Waiting: 3 \n";

    /// Tengine's page, built from its handler's format strings (`alibaba/tengine`,
    /// `src/http/modules/ngx_http_stub_status_module.c` at commit `e8a99391`, the 3.1.0 release
    /// line): `"server accepts handled requests request_time\n"` and `" %uA %uA %uA %uA\n"`, the
    /// number line with no trailing space.
    const TENGINE_PAGE: &[u8] = b"Active connections: 2 \n\
server accepts handled requests request_time\n 10 10 12 345\nReading: 0 Writing: 1 Waiting: 1 \n";

    fn expected(
        active: i64,
        accepts: i64,
        handled: i64,
        requests: i64,
        rww: [i64; 3],
    ) -> StubStatus {
        StubStatus {
            active: Some(active),
            accepts: Some(accepts),
            handled: Some(handled),
            requests: Some(requests),
            reading: Some(rww[0]),
            writing: Some(rww[1]),
            waiting: Some(rww[2]),
            tengine: false,
        }
    }

    #[test]
    fn the_four_line_page_parses_with_and_without_trailing_spaces_and_with_crlf() {
        let want = expected(5, 6, 6, 3, [1, 1, 3]);
        assert_eq!(parse(PAGE), Ok(want.clone()));
        let bare = b"Active connections: 5\nserver accepts handled requests\n 6 6 3\n\
Reading: 1 Writing: 1 Waiting: 3\n";
        assert_eq!(parse(bare), Ok(want.clone()));
        let crlf = b"Active connections: 5 \r\nserver accepts handled requests\r\n 6 6 3 \r\n\
Reading: 1 Writing: 1 Waiting: 3 \r\n";
        assert_eq!(parse(crlf), Ok(want.clone()));
        let trailing_blank = [PAGE, b"\n\r\n  \n"].concat();
        assert_eq!(parse(&trailing_blank), Ok(want));
    }

    #[test]
    fn tengines_four_column_form_reads_its_fourth_number_and_emits_seven_points() {
        let status = parse(TENGINE_PAGE).unwrap();
        assert_eq!(status, StubStatus { tengine: true, ..expected(2, 10, 10, 12, [0, 1, 1]) });
        let events = status.to_events(0);
        assert_eq!(events.len(), 7);
        assert!(events.iter().all(|e| e.metrics.iter().all(|m| match m.kind {
            MetricKind::Sum(Sum { value, .. }) | MetricKind::Gauge(value) =>
                value != MetricValue::I64(345),
            _ => false,
        })));

        let not_a_number = b"Active connections: 2 \n\
server accepts handled requests request_time\n 10 10 12 x\nReading: 0 Writing: 1 Waiting: 1 \n";
        assert_eq!(parse(not_a_number), Err(ParseError::BadLine { line: 3, expected: LINE_3 }));
    }

    #[test]
    fn every_parse_error_shape() {
        let cases: &[(&[u8], ParseError)] = &[
            (b"", ParseError::Empty),
            (&PAGE[..PAGE.len() - 1], ParseError::Unterminated),
            (
                b"Active connections: 5 \nserver accepts handled requests\n 6 6 3 \n",
                ParseError::MissingLine { line: 4 },
            ),
            (b"Active connections: 5 \n", ParseError::MissingLine { line: 2 }),
            (
                b"Active connections: 5 \nserver accepts handled requests\n 6 6 3 \n\
Reading: 1 Writing: 1 Waiting: 3 \nextra\n",
                ParseError::ExtraLine { line: 5 },
            ),
            (
                b"server accepts handled requests\nActive connections: 5 \n 6 6 3 \n\
Reading: 1 Writing: 1 Waiting: 3 \n",
                ParseError::BadLine { line: 1, expected: LINE_1 },
            ),
            (
                b"Active connections: 5 \nserver accepts handled requests\n\
Reading: 1 Writing: 1 Waiting: 3 \n 6 6 3 \n",
                ParseError::BadLine { line: 3, expected: LINE_3 },
            ),
            (
                b"Active connections: 5 \nserver accepts handled requests\n 6 6 3 9 \n\
Reading: 1 Writing: 1 Waiting: 3 \n",
                ParseError::ColumnCount { expected: 3, found: 4 },
            ),
            (
                b"Active connections: 5 \nserver accepts handled requests request_time\n 6 6 3 \n\
Reading: 1 Writing: 1 Waiting: 3 \n",
                ParseError::ColumnCount { expected: 4, found: 3 },
            ),
            (
                b"Active connections: 5x \nserver accepts handled requests\n 6 6 3 \n\
Reading: 1 Writing: 1 Waiting: 3 \n",
                ParseError::BadLine { line: 1, expected: LINE_1 },
            ),
            (
                b"Active connections: 5 \nserver accepts handled requests\n 6 -6 3 \n\
Reading: 1 Writing: 1 Waiting: 3 \n",
                ParseError::BadLine { line: 3, expected: LINE_3 },
            ),
            (
                b"Active connections: 5 \nserver accepts handled requests\n 6  6 3 \n\
Reading: 1 Writing: 1 Waiting: 3 \n",
                ParseError::BadLine { line: 3, expected: LINE_3 },
            ),
            (
                b"Active connections: 5 \nserver accepts handled requests\n 6 6 3 \n\
Reading: 1 Writing: 1 Waiting: 3x \n",
                ParseError::BadLine { line: 4, expected: LINE_4 },
            ),
            (
                b"Active connections: 5 \nserver accepts handled requests\n 6 6 3 \n\
Writing: 1 Reading: 1 Waiting: 3 \n",
                ParseError::BadLine { line: 4, expected: LINE_4 },
            ),
            (b"<html><body>404</body></html>\n", ParseError::BadLine { line: 1, expected: LINE_1 }),
        ];
        for (body, want) in cases {
            assert_eq!(parse(body).as_ref(), Err(want), "{:?}", String::from_utf8_lossy(body));
        }
    }

    #[test]
    fn a_field_past_i64_max_is_none_and_the_other_six_points_are_emitted() {
        let body = b"Active connections: 18446744073709551615 \nserver accepts handled requests\n \
6 6 3 \nReading: 1 Writing: 1 Waiting: 3 \n";
        let status = parse(body).unwrap();
        assert_eq!(status.active, None);
        assert_eq!(status.out_of_range(), 1);
        let events = status.to_events(7);
        assert_eq!(events.len(), 6);
        assert!(events.iter().all(|e| e.attributes.get(STATE) != Some(&Value::str("active"))));

        let at_max =
            b"Active connections: 9223372036854775807 \nserver accepts handled requests\n \
6 6 3 \nReading: 1 Writing: 1 Waiting: 3 \n";
        assert_eq!(parse(at_max).unwrap().active, Some(i64::MAX));
    }

    #[test]
    fn to_events_follows_the_mapping_table() {
        let events = parse(PAGE).unwrap().to_events(42);
        let rows: Vec<_> = events
            .iter()
            .map(|e| {
                assert_eq!(e.timestamp, 42);
                let m = &e.metrics[0];
                assert_eq!(m.start_timestamp, 0);
                assert_eq!(m.description, None);
                let state = e.attributes.get(STATE).cloned();
                (
                    resolve(m.name).to_string(),
                    m.unit.map(|u| resolve(u).to_string()),
                    m.kind.clone(),
                    state,
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
        let s = |v: &str| Some(v.to_string());
        let state = |v: &str| Some(Value::str(v));
        assert_eq!(
            rows,
            vec![
                (CONNECTIONS_ACCEPTED.to_string(), s("{connections}"), sum(6), None),
                (CONNECTIONS_HANDLED.to_string(), s("{connections}"), sum(6), None),
                (REQUESTS.to_string(), s("{requests}"), sum(3), None),
                (CONNECTIONS_CURRENT.to_string(), s("{connections}"), gauge(5), state("active")),
                (CONNECTIONS_CURRENT.to_string(), s("{connections}"), gauge(1), state("reading")),
                (CONNECTIONS_CURRENT.to_string(), s("{connections}"), gauge(1), state("writing")),
                (CONNECTIONS_CURRENT.to_string(), s("{connections}"), gauge(3), state("waiting")),
            ]
        );
    }
}
