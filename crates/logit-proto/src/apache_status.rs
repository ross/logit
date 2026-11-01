//! Apache httpd's `mod_status` page in its `?auto` form, parsed into the OpenTelemetry
//! Collector's `apachereceiver` metrics under their new-format names. `apache_status_in` reads
//! every scraped body through [`parse`] and [`ApacheStatus::to_events`]. The decisions behind the
//! grammar and the mapping are
//! [ADR `server-status-scrape-inputs`](../../../docs/adr/server-status-scrape-inputs.md)'s
//! sections 7, 8, and 10.
//!
//! # Grammar
//!
//! - The grammar is over bytes: the page is ISO-8859-1, and only its keys and numbers, which are
//!   ASCII, are read. A non-ASCII byte in an ignored value, such as `ServerVersion`, changes
//!   nothing.
//! - The body is lines ending in `\n`, and a `\r` before the `\n` is ignored. Each line splits at
//!   its first `": "` into a key and a value.
//! - A line with no `": "` (the server name 2.4 prints first and 2.2 doesn't, or
//!   `TLSSessionCacheStatus`) and a key the table below doesn't list are ignored, uncounted: the
//!   key set varies with the version, the MPM, `ExtendedStatus`, and the loaded modules.
//! - A listed key that repeats keeps its last value, as the Collector's `parseStats` map does;
//!   [`ApacheStatus::duplicates`] counts each repeat.
//! - A listed key's value parses as its type. A count is ASCII digits that fit an `i64`. A float
//!   is a finite `%g` form such as `.266667`, `0`, or `1e+06`. A value that doesn't parse leaves
//!   its point out and is named in [`ApacheStatus::malformed`]; the rest of the page is still read.
//! - A last line with no `\n` is discarded and [`ApacheStatus::truncated`] set, and the lines
//!   before it are still read. Every line `mod_status` and the status hooks write ends in `\n`, so
//!   an unterminated one was cut short.
//! - A body with no listed key, such as the HTML page a URL without `?auto` gets, is a
//!   [`ParseError`].
//!
//! A listed key that's absent emits nothing: `ExtendedStatus Off` drops every `Total*` and `CPU*`
//! key, the `prefork` and `worker` MPMs drop the `Conns*` keys, and `mod_status` omits `CPULoad`
//! when CPU time is zero.
//!
//! # Mapping
//!
//! One event per point, in this order, with no attributes beyond those listed and no start time:
//!
//! | `?auto` key | Metric | Kind | Value | Unit | Attributes |
//! |---|---|---|---|---|---|
//! | `ServerUptimeSeconds` | `apache.uptime` | cumulative monotonic `Sum` | `I64` | `s` | — |
//! | `Load1`, `Load5`, `Load15` | `apache.load.1`, `apache.load.5`, `apache.load.15` | `Gauge` | `F64` | `1` | — |
//! | `Total Accesses` | `apache.request.count` | cumulative monotonic `Sum` | `I64` | `{request}` | — |
//! | `Total kBytes` | `apache.traffic` | cumulative monotonic `Sum` | `I64`, the value × 1024 | `By` | — |
//! | `Total Duration` | `apache.request.time` | cumulative monotonic `Sum` | `I64` | `ms` | — |
//! | `CPUUser` | `apache.cpu.time` | `Gauge` | `F64` | `s` | `apache.process.level` = `self`, `cpu.mode` = `user` |
//! | `CPUSystem` | `apache.cpu.time` | `Gauge` | `F64` | `s` | `apache.process.level` = `self`, `cpu.mode` = `system` |
//! | `CPUChildrenUser` | `apache.cpu.time` | `Gauge` | `F64` | `s` | `apache.process.level` = `children`, `cpu.mode` = `user` |
//! | `CPUChildrenSystem` | `apache.cpu.time` | `Gauge` | `F64` | `s` | `apache.process.level` = `children`, `cpu.mode` = `system` |
//! | `CPULoad` | `apache.cpu.load` | `Gauge` | `F64` | `%` | — |
//! | `BusyWorkers` | `apache.worker.active` | `Gauge` | `I64` | `{worker}` | — |
//! | `IdleWorkers` | `apache.worker.idle` | `Gauge` | `I64` | `{worker}` | — |
//! | `ConnsTotal` | `apache.connection.active` | `Gauge` | `I64` | `{connection}` | — |
//! | `ConnsAsyncWriting` | `apache.connection.status` | `Gauge` | `I64` | `{connection}` | `apache.connection.state` = `writing` |
//! | `ConnsAsyncKeepAlive` | `apache.connection.status` | `Gauge` | `I64` | `{connection}` | `apache.connection.state` = `keepalive` |
//! | `ConnsAsyncClosing` | `apache.connection.status` | `Gauge` | `I64` | `{connection}` | `apache.connection.state` = `closing` |
//! | `Scoreboard` | `apache.worker.status` | `Gauge` | `I64` | `{worker}` | `apache.worker.state` (below) |
//! | `Scoreboard` | `apache.worker.limit` | `Gauge` | `I64`, the scoreboard's length | `{worker}` | — |
//!
//! Every other key is unmapped, the server name, the time strings, the lifetime averages
//! (`ReqPerSec` and the rest), `GracefulWorkers`, `Processes`, `Stopping`, `ConnsAsyncWaitIO`, and
//! the `mod_ssl` session-cache block among them; the ADR's table gives each one's reason.
//!
//! `Scoreboard` counts each byte as an `apache.worker.state`, as the Collector's `parseScoreboard`
//! does: `_` `waiting`, `S` `starting`, `R` `reading`, `W` `sending`, `K` `keepalive`, `D`
//! `dnslookup`, `C` `closing`, `L` `logging`, `G` `finishing`, `I` `idle_cleanup`, `.` `open`, and
//! any other byte `unknown`. Those eleven states are emitted on every page, `0` included, in that
//! order; `unknown` follows them only when it's nonzero.
//!
//! Where the Collector differs: every value that can decrease is a `Gauge` where it has a
//! non-monotonic `Sum`, and `apache.cpu.time` is a `Gauge` where it has a monotonic one, since a
//! child process's exit lowers the total; `apache.cpu.time` is in `s`, not `{jiff}`; `apache.load.*`
//! is `1`, not `%`; and `apache.connection.status` and `apache.worker.status` are `{connection}`
//! and `{worker}`, not `1`.

use std::fmt;

use logit_core::interner::intern;
use logit_core::{AttrMap, Event, MetricKind, MetricRecord, MetricValue, Sum, Temporality, Value};

/// `apache.uptime`.
pub const UPTIME: &str = "apache.uptime";
/// `apache.load.1`, `apache.load.5`, and `apache.load.15`, in that order.
pub const LOAD: [&str; 3] = ["apache.load.1", "apache.load.5", "apache.load.15"];
/// `apache.request.count`.
pub const REQUEST_COUNT: &str = "apache.request.count";
/// `apache.traffic`.
pub const TRAFFIC: &str = "apache.traffic";
/// `apache.request.time`.
pub const REQUEST_TIME: &str = "apache.request.time";
/// `apache.cpu.time`, one point per [`PROCESS_LEVEL`] and [`CPU_MODE`].
pub const CPU_TIME: &str = "apache.cpu.time";
/// `apache.cpu.load`.
pub const CPU_LOAD: &str = "apache.cpu.load";
/// `apache.worker.active`.
pub const WORKER_ACTIVE: &str = "apache.worker.active";
/// `apache.worker.idle`.
pub const WORKER_IDLE: &str = "apache.worker.idle";
/// `apache.connection.active`.
pub const CONNECTION_ACTIVE: &str = "apache.connection.active";
/// `apache.connection.status`, one point per [`CONNECTION_STATE`].
pub const CONNECTION_STATUS: &str = "apache.connection.status";
/// `apache.worker.status`, one point per [`WORKER_STATE`].
pub const WORKER_STATUS: &str = "apache.worker.status";
/// `apache.worker.limit`.
pub const WORKER_LIMIT: &str = "apache.worker.limit";

/// `apache.cpu.time`'s `self` or `children` attribute.
pub const PROCESS_LEVEL: &str = "apache.process.level";
/// `apache.cpu.time`'s `user` or `system` attribute.
pub const CPU_MODE: &str = "cpu.mode";
/// `apache.connection.status`'s attribute.
pub const CONNECTION_STATE: &str = "apache.connection.state";
/// `apache.worker.status`'s attribute.
pub const WORKER_STATE: &str = "apache.worker.state";

/// The scoreboard states in emission order, each with the byte `mod_status` writes for it.
pub const SCOREBOARD_STATES: [(u8, &str); 11] = [
    (b'_', "waiting"),
    (b'S', "starting"),
    (b'R', "reading"),
    (b'W', "sending"),
    (b'K', "keepalive"),
    (b'D', "dnslookup"),
    (b'C', "closing"),
    (b'L', "logging"),
    (b'G', "finishing"),
    (b'I', "idle_cleanup"),
    (b'.', "open"),
];

/// The state a scoreboard byte outside [`SCOREBOARD_STATES`] counts as.
pub const UNKNOWN_STATE: &str = "unknown";

/// `apache.cpu.time`'s four points in emission order: key, `apache.process.level`, `cpu.mode`.
const CPU_KEYS: [(&str, &str, &str); 4] = [
    ("CPUUser", "self", "user"),
    ("CPUSystem", "self", "system"),
    ("CPUChildrenUser", "children", "user"),
    ("CPUChildrenSystem", "children", "system"),
];

/// `apache.connection.status`'s three points in emission order: key, `apache.connection.state`.
const ASYNC_KEYS: [(&str, &str); 3] = [
    ("ConnsAsyncWriting", "writing"),
    ("ConnsAsyncKeepAlive", "keepalive"),
    ("ConnsAsyncClosing", "closing"),
];

/// How a listed key's value parses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Type {
    /// ASCII digits that fit an `i64`.
    Count,
    /// A count of KiB, multiplied by 1024 into bytes.
    Kibibytes,
    /// A finite `%g` form.
    Float,
    /// Any bytes, one per worker slot.
    Scoreboard,
}

impl Type {
    /// What a value of this type must be, for the `malformed_value` diagnostic.
    fn expected(self) -> &'static str {
        match self {
            Type::Count => "a whole number that fits a 64-bit integer",
            Type::Kibibytes => "a whole number whose value in bytes fits a 64-bit integer",
            Type::Float => "a finite number",
            Type::Scoreboard => "a scoreboard",
        }
    }
}

/// Every listed key, in emission order. [`ApacheStatus`]'s fields are indexed into this table.
const KEYS: [(&str, Type); 19] = [
    ("ServerUptimeSeconds", Type::Count),
    ("Load1", Type::Float),
    ("Load5", Type::Float),
    ("Load15", Type::Float),
    ("Total Accesses", Type::Count),
    ("Total kBytes", Type::Kibibytes),
    ("Total Duration", Type::Count),
    (CPU_KEYS[0].0, Type::Float),
    (CPU_KEYS[1].0, Type::Float),
    (CPU_KEYS[2].0, Type::Float),
    (CPU_KEYS[3].0, Type::Float),
    ("CPULoad", Type::Float),
    ("BusyWorkers", Type::Count),
    ("IdleWorkers", Type::Count),
    ("ConnsTotal", Type::Count),
    (ASYNC_KEYS[0].0, Type::Count),
    (ASYNC_KEYS[1].0, Type::Count),
    (ASYNC_KEYS[2].0, Type::Count),
    ("Scoreboard", Type::Scoreboard),
];

/// A parsed `Scoreboard` value.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Scoreboard {
    /// The count of each of [`SCOREBOARD_STATES`], in its order.
    pub states: [i64; 11],
    /// Bytes outside [`SCOREBOARD_STATES`].
    pub unknown: i64,
    /// The scoreboard's length: one byte per worker slot `mod_status` printed.
    pub slots: i64,
}

/// One parsed page. Each value is `None` when its key was absent or its value didn't parse.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ApacheStatus {
    pub uptime: Option<i64>,
    /// `Load1`, `Load5`, `Load15`.
    pub load: [Option<f64>; 3],
    pub accesses: Option<i64>,
    /// `Total kBytes` × 1024.
    pub traffic_bytes: Option<i64>,
    pub duration_ms: Option<i64>,
    /// `CPUUser`, `CPUSystem`, `CPUChildrenUser`, `CPUChildrenSystem`.
    pub cpu: [Option<f64>; 4],
    pub cpu_load: Option<f64>,
    pub busy_workers: Option<i64>,
    pub idle_workers: Option<i64>,
    pub conns_total: Option<i64>,
    /// `ConnsAsyncWriting`, `ConnsAsyncKeepAlive`, `ConnsAsyncClosing`.
    pub conns_async: [Option<i64>; 3],
    pub scoreboard: Option<Scoreboard>,
    /// Each listed key whose value didn't parse, with what it should have been, in table order.
    pub malformed: Vec<(&'static str, &'static str)>,
    /// How many listed-key lines repeated an earlier one.
    pub duplicates: usize,
    /// Whether the body's last line had no `\n` and was discarded.
    pub truncated: bool,
}

/// Why a body isn't a `mod_status` `?auto` page. Its `Display` never carries the body's bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ParseError {
    /// No line holds a listed key: an empty body, the HTML page, or another page entirely.
    NoStatusKeys,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::NoStatusKeys => f.write_str(
                "not an Apache mod_status ?auto page: the body holds none of its keys; a \
                 /server-status URL needs ?auto in its query string, as in \
                 http://HOST/server-status?auto, or Apache serves its HTML page",
            ),
        }
    }
}

impl std::error::Error for ParseError {}

/// ASCII digits that fit an `i64`.
fn count(value: &[u8]) -> Option<i64> {
    if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
        return None;
    }
    value.iter().try_fold(0i64, |acc, &d| acc.checked_mul(10)?.checked_add(i64::from(d - b'0')))
}

/// A finite number in a `%g` form: digits, `.`, an exponent, and signs only, so Rust's own
/// `inf` and `nan` spellings don't parse.
fn float(value: &[u8]) -> Option<f64> {
    if value.is_empty() || !value.iter().all(|&b| b.is_ascii_digit() || b".eE+-".contains(&b)) {
        return None;
    }
    let parsed: f64 = std::str::from_utf8(value).ok()?.parse().ok()?;
    parsed.is_finite().then_some(parsed)
}

fn scoreboard(value: &[u8]) -> Scoreboard {
    let mut board = Scoreboard { slots: value.len() as i64, ..Scoreboard::default() };
    for &b in value {
        match SCOREBOARD_STATES.iter().position(|&(c, _)| c == b) {
            Some(i) => board.states[i] += 1,
            None => board.unknown += 1,
        }
    }
    board
}

/// Parses one `?auto` body (the module doc's grammar).
pub fn parse(body: &[u8]) -> Result<ApacheStatus, ParseError> {
    let mut raw: [Option<&[u8]>; KEYS.len()] = [None; KEYS.len()];
    let mut status = ApacheStatus::default();

    let (lines, rest) = match body.iter().rposition(|&b| b == b'\n') {
        Some(end) => (&body[..end], &body[end + 1..]),
        None => (&body[..0], body),
    };
    status.truncated = !rest.is_empty();
    let mut found = false;
    // With no `\n` at all, `lines` is empty and splits into one empty line, which holds no key.
    for line in lines.split(|&b| b == b'\n') {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let Some(split) = line.windows(2).position(|w| w == b": ") else { continue };
        let (key, value) = (&line[..split], &line[split + 2..]);
        let Some(index) = KEYS.iter().position(|(k, _)| k.as_bytes() == key) else {
            continue;
        };
        found = true;
        if raw[index].replace(value).is_some() {
            status.duplicates += 1;
        }
    }
    if !found {
        return Err(ParseError::NoStatusKeys);
    }

    let mut counts = [None; KEYS.len()];
    let mut floats = [None; KEYS.len()];
    for (index, value) in raw.iter().enumerate() {
        let Some(value) = value else { continue };
        let (key, ty) = KEYS[index];
        let ok = match ty {
            Type::Count => {
                counts[index] = count(value);
                counts[index].is_some()
            }
            Type::Kibibytes => {
                counts[index] = count(value).and_then(|kib| kib.checked_mul(1024));
                counts[index].is_some()
            }
            Type::Float => {
                floats[index] = float(value);
                floats[index].is_some()
            }
            Type::Scoreboard => {
                status.scoreboard = Some(scoreboard(value));
                true
            }
        };
        if !ok {
            status.malformed.push((key, ty.expected()));
        }
    }

    status.uptime = counts[0];
    status.load = [floats[1], floats[2], floats[3]];
    status.accesses = counts[4];
    status.traffic_bytes = counts[5];
    status.duration_ms = counts[6];
    status.cpu = [floats[7], floats[8], floats[9], floats[10]];
    status.cpu_load = floats[11];
    status.busy_workers = counts[12];
    status.idle_workers = counts[13];
    status.conns_total = counts[14];
    status.conns_async = [counts[15], counts[16], counts[17]];
    Ok(status)
}

fn event(
    timestamp: i64,
    name: &str,
    unit: &str,
    kind: MetricKind,
    attrs: &[(&str, &str)],
) -> Event {
    let mut record = MetricRecord::new(intern(name), kind);
    record.unit = Some(intern(unit));
    let mut attributes = AttrMap::new();
    for &(key, value) in attrs {
        attributes.insert(key, Value::str(value));
    }
    Event::metric(timestamp, attributes, record)
}

fn counter(value: i64) -> MetricKind {
    MetricKind::Sum(Sum {
        value: MetricValue::I64(value),
        temporality: Temporality::Cumulative,
        monotonic: true,
    })
}

fn gauge_i64(value: i64) -> MetricKind {
    MetricKind::Gauge(MetricValue::I64(value))
}

fn gauge_f64(value: f64) -> MetricKind {
    MetricKind::Gauge(MetricValue::F64(value))
}

impl ApacheStatus {
    /// The page's points as one event each, in the module doc's table order, each stamped
    /// `timestamp`. An absent or malformed key has no event.
    pub fn to_events(&self, timestamp: i64) -> Vec<Event> {
        let mut events = Vec::with_capacity(40);
        let mut push =
            |name: &str, unit: &str, kind: Option<MetricKind>, attrs: &[(&str, &str)]| {
                if let Some(kind) = kind {
                    events.push(event(timestamp, name, unit, kind, attrs));
                }
            };
        push(UPTIME, "s", self.uptime.map(counter), &[]);
        for (name, value) in LOAD.iter().zip(self.load) {
            push(name, "1", value.map(gauge_f64), &[]);
        }
        push(REQUEST_COUNT, "{request}", self.accesses.map(counter), &[]);
        push(TRAFFIC, "By", self.traffic_bytes.map(counter), &[]);
        push(REQUEST_TIME, "ms", self.duration_ms.map(counter), &[]);
        for ((_, level, mode), value) in CPU_KEYS.iter().zip(self.cpu) {
            push(CPU_TIME, "s", value.map(gauge_f64), &[(PROCESS_LEVEL, level), (CPU_MODE, mode)]);
        }
        push(CPU_LOAD, "%", self.cpu_load.map(gauge_f64), &[]);
        push(WORKER_ACTIVE, "{worker}", self.busy_workers.map(gauge_i64), &[]);
        push(WORKER_IDLE, "{worker}", self.idle_workers.map(gauge_i64), &[]);
        push(CONNECTION_ACTIVE, "{connection}", self.conns_total.map(gauge_i64), &[]);
        for ((_, state), value) in ASYNC_KEYS.iter().zip(self.conns_async) {
            let attrs = [(CONNECTION_STATE, *state)];
            push(CONNECTION_STATUS, "{connection}", value.map(gauge_i64), &attrs);
        }
        if let Some(board) = &self.scoreboard {
            for ((_, state), n) in SCOREBOARD_STATES.iter().zip(board.states) {
                push(WORKER_STATUS, "{worker}", Some(gauge_i64(n)), &[(WORKER_STATE, state)]);
            }
            if board.unknown > 0 {
                let attrs = [(WORKER_STATE, UNKNOWN_STATE)];
                push(WORKER_STATUS, "{worker}", Some(gauge_i64(board.unknown)), &attrs);
            }
            push(WORKER_LIMIT, "{worker}", Some(gauge_i64(board.slots)), &[]);
        }
        events
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use logit_core::interner::resolve;

    /// httpd 2.4.69's page under `event`, from `testdata/interop/apache-status/event-1.body`,
    /// with its scoreboard cut to ten slots.
    const PAGE: &[u8] = b"status-server\n\
ServerVersion: Apache/2.4.69 (Unix)\n\
ServerMPM: event\n\
Server Built: Oct 28 2026 01:15:47\n\
CurrentTime: Sunday, 01-Nov-2026 15:25:45 UTC\n\
RestartTime: Sunday, 01-Nov-2026 15:25:43 UTC\n\
ParentServerConfigGeneration: 1\n\
ParentServerMPMGeneration: 0\n\
ServerUptimeSeconds: 2\n\
ServerUptime: 2 seconds\n\
Load1: 7.24\n\
Load5: 4.63\n\
Load15: 2.69\n\
Total Accesses: 5\n\
Total kBytes: 8\n\
Total Duration: 1\n\
CPUUser: .01\n\
CPUSystem: .02\n\
CPUChildrenUser: 0\n\
CPUChildrenSystem: 1e-05\n\
CPULoad: 1.5\n\
Uptime: 2\n\
ReqPerSec: 2.5\n\
BytesPerSec: 4096\n\
BytesPerReq: 1638.4\n\
DurationPerReq: .2\n\
BusyWorkers: 2\n\
GracefulWorkers: 0\n\
IdleWorkers: 98\n\
Processes: 4\n\
Stopping: 0\n\
ConnsTotal: 25\n\
ConnsAsyncWaitIO: 0\n\
ConnsAsyncWriting: 1\n\
ConnsAsyncKeepAlive: 12\n\
ConnsAsyncClosing: 12\n\
Scoreboard: R___W____.\n";

    type Row = (String, String, MetricKind, Vec<(String, String)>);

    fn rows(events: &[Event]) -> Vec<Row> {
        events
            .iter()
            .map(|e| {
                assert_eq!(e.timestamp, 9);
                assert_eq!(e.metrics.len(), 1);
                let m = &e.metrics[0];
                assert_eq!(m.start_timestamp, 0);
                assert_eq!(m.description, None);
                let mut attrs: Vec<_> = e
                    .attributes
                    .iter()
                    .map(|(k, v)| (resolve(k).to_string(), v.as_str().unwrap().to_string()))
                    .collect();
                attrs.sort();
                (
                    resolve(m.name).to_string(),
                    resolve(m.unit.unwrap()).to_string(),
                    m.kind.clone(),
                    attrs,
                )
            })
            .collect()
    }

    fn row(name: &str, unit: &str, kind: MetricKind, attrs: &[(&str, &str)]) -> Row {
        let mut attrs: Vec<_> = attrs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        attrs.sort();
        (name.to_string(), unit.to_string(), kind, attrs)
    }

    #[test]
    fn every_listed_key_follows_the_mapping_table_and_every_other_key_is_ignored() {
        let status = parse(PAGE).unwrap();
        assert!(status.malformed.is_empty());
        assert_eq!(status.duplicates, 0);
        assert!(!status.truncated);

        let ws = |state| [(WORKER_STATE, state)];
        let mut want = vec![
            row(UPTIME, "s", counter(2), &[]),
            row("apache.load.1", "1", gauge_f64(7.24), &[]),
            row("apache.load.5", "1", gauge_f64(4.63), &[]),
            row("apache.load.15", "1", gauge_f64(2.69), &[]),
            row(REQUEST_COUNT, "{request}", counter(5), &[]),
            row(TRAFFIC, "By", counter(8 * 1024), &[]),
            row(REQUEST_TIME, "ms", counter(1), &[]),
            row(CPU_TIME, "s", gauge_f64(0.01), &[(PROCESS_LEVEL, "self"), (CPU_MODE, "user")]),
            row(CPU_TIME, "s", gauge_f64(0.02), &[(PROCESS_LEVEL, "self"), (CPU_MODE, "system")]),
            row(CPU_TIME, "s", gauge_f64(0.0), &[(PROCESS_LEVEL, "children"), (CPU_MODE, "user")]),
            row(
                CPU_TIME,
                "s",
                gauge_f64(1e-5),
                &[(PROCESS_LEVEL, "children"), (CPU_MODE, "system")],
            ),
            row(CPU_LOAD, "%", gauge_f64(1.5), &[]),
            row(WORKER_ACTIVE, "{worker}", gauge_i64(2), &[]),
            row(WORKER_IDLE, "{worker}", gauge_i64(98), &[]),
            row(CONNECTION_ACTIVE, "{connection}", gauge_i64(25), &[]),
            row(CONNECTION_STATUS, "{connection}", gauge_i64(1), &[(CONNECTION_STATE, "writing")]),
            row(
                CONNECTION_STATUS,
                "{connection}",
                gauge_i64(12),
                &[(CONNECTION_STATE, "keepalive")],
            ),
            row(CONNECTION_STATUS, "{connection}", gauge_i64(12), &[(CONNECTION_STATE, "closing")]),
        ];
        for (state, n) in [
            ("waiting", 7),
            ("starting", 0),
            ("reading", 1),
            ("sending", 1),
            ("keepalive", 0),
            ("dnslookup", 0),
            ("closing", 0),
            ("logging", 0),
            ("finishing", 0),
            ("idle_cleanup", 0),
            ("open", 1),
        ] {
            want.push(row(WORKER_STATUS, "{worker}", gauge_i64(n), &ws(state)));
        }
        want.push(row(WORKER_LIMIT, "{worker}", gauge_i64(10), &[]));
        assert_eq!(rows(&status.to_events(9)), want);
    }

    #[test]
    fn float_values_take_every_g_form_and_reject_the_rest() {
        for (text, want) in [
            (&b".0689655"[..], Some(0.0689655)),
            (b"0", Some(0.0)),
            (b"12", Some(12.0)),
            (b"1e+06", Some(1e6)),
            (b"2.5e-05", Some(2.5e-5)),
            (b"-1.00", Some(-1.0)),
            (b"", None),
            (b"inf", None),
            (b"nan", None),
            (b"1e999", None),
            (b"1.2.3", None),
            (b"1,5", None),
            (b" 1", None),
        ] {
            assert_eq!(float(text), want, "{:?}", String::from_utf8_lossy(text));
        }
        for (text, want) in [
            (&b"0"[..], Some(0)),
            (b"9223372036854775807", Some(i64::MAX)),
            (b"9223372036854775808", None),
            (b"-1", None),
            (b"+1", None),
            (b"1.0", None),
            (b"", None),
        ] {
            assert_eq!(count(text), want, "{:?}", String::from_utf8_lossy(text));
        }
    }

    #[test]
    fn a_malformed_value_is_skipped_and_named_and_the_rest_of_the_page_is_read() {
        let status = parse(
            b"Total Accesses: lots\nTotal kBytes: 9007199254740993\nLoad1: x\n\
BusyWorkers: 3\n",
        )
        .unwrap();
        assert_eq!(status.accesses, None);
        assert_eq!(status.traffic_bytes, None);
        assert_eq!(status.load[0], None);
        assert_eq!(status.busy_workers, Some(3));
        assert_eq!(
            status.malformed,
            [
                ("Load1", Type::Float.expected()),
                ("Total Accesses", Type::Count.expected()),
                ("Total kBytes", Type::Kibibytes.expected()),
            ]
        );
        assert_eq!(status.to_events(9).len(), 1);
    }

    #[test]
    fn a_repeated_key_keeps_its_last_value_and_counts_each_repeat() {
        let status =
            parse(b"BusyWorkers: 1\nBusyWorkers: x\nBusyWorkers: 3\nIdleWorkers: 4\n").unwrap();
        assert_eq!(status.busy_workers, Some(3));
        assert_eq!(status.duplicates, 2);
        assert!(status.malformed.is_empty());

        let last_malformed = parse(b"BusyWorkers: 1\nBusyWorkers: x\n").unwrap();
        assert_eq!(last_malformed.busy_workers, None);
        assert_eq!(last_malformed.malformed, [("BusyWorkers", Type::Count.expected())]);
    }

    #[test]
    fn an_unterminated_last_line_is_discarded_and_the_lines_before_it_read() {
        let status = parse(b"BusyWorkers: 2\nIdleWorkers: 98\nScoreboard: __W").unwrap();
        assert!(status.truncated);
        assert_eq!(status.busy_workers, Some(2));
        assert_eq!(status.idle_workers, Some(98));
        assert_eq!(status.scoreboard, None);

        let terminated = parse(b"BusyWorkers: 2\nScoreboard: __W\n").unwrap();
        assert!(!terminated.truncated);
        assert_eq!(terminated.scoreboard.unwrap().slots, 3);

        // The only listed key is on the cut line, so nothing is left to read.
        assert_eq!(parse(b"status-server\nBusyWorkers: 2"), Err(ParseError::NoStatusKeys));
    }

    #[test]
    fn crlf_line_ends_and_a_page_with_no_server_name_line_parse() {
        let crlf = parse(b"localhost\r\nBusyWorkers: 2\r\nScoreboard: _W\r\n").unwrap();
        assert_eq!(crlf.busy_workers, Some(2));
        assert_eq!(crlf.scoreboard.unwrap().slots, 2);

        // httpd 2.2's page starts at its first key.
        let v22 =
            parse(b"Total Accesses: 3\nBusyWorkers: 1\nIdleWorkers: 7\nScoreboard: _W\n").unwrap();
        assert_eq!(v22.accesses, Some(3));
        assert_eq!(v22.to_events(9).len(), 3 + 11 + 1);
    }

    #[test]
    fn unknown_keys_lines_without_a_separator_and_non_ascii_bytes_are_ignored() {
        let body = b"status-server\nServerVersion: Apache/2.4.69 (Unix) caf\xe9\n\
Unknown Key: 7\nBusyWorkers 9\nBusyWorkers:9\nTLSSessionCacheStatus\nCacheUsage: 0%\n\
GracefulWorkers: 1\nIdleWorkers: 4\n\xff\xfe: 1\n";
        let status = parse(body).unwrap();
        assert_eq!(status, ApacheStatus { idle_workers: Some(4), ..ApacheStatus::default() });
    }

    #[test]
    fn the_html_page_and_an_empty_body_are_parse_errors_naming_auto() {
        for body in [
            &b""[..],
            b"\n",
            b"\n\n",
            b"status-server\n",
            b"<html><head><title>Apache Status</title></head><body>\n\
<dt>Server Version: Apache/2.4.69 (Unix)</dt>\n<pre>__W__</pre>\n",
        ] {
            assert_eq!(parse(body), Err(ParseError::NoStatusKeys));
        }
        assert!(ParseError::NoStatusKeys.to_string().contains("?auto"));
    }

    #[test]
    fn every_scoreboard_byte_counts_as_its_state_and_unknown_only_when_nonzero() {
        let status = parse(b"Scoreboard: _SRWKDCLGI.\n").unwrap();
        let board = status.scoreboard.clone().unwrap();
        assert_eq!(board, Scoreboard { states: [1; 11], unknown: 0, slots: 11 });
        let events = status.to_events(9);
        assert_eq!(events.len(), 12);
        assert!(events
            .iter()
            .all(|e| e.attributes.get(WORKER_STATE) != Some(&Value::str(UNKNOWN_STATE))));

        let status = parse(b"Scoreboard: __?\xe9\n").unwrap();
        let board = status.scoreboard.clone().unwrap();
        assert_eq!(board.states[0], 2);
        assert_eq!(board.states[1..], [0; 10]);
        assert_eq!((board.unknown, board.slots), (2, 4));
        let rows = rows(&status.to_events(9));
        assert_eq!(rows.len(), 13);
        assert_eq!(
            rows[11],
            row(WORKER_STATUS, "{worker}", gauge_i64(2), &[(WORKER_STATE, "unknown")])
        );
        assert_eq!(rows[12], row(WORKER_LIMIT, "{worker}", gauge_i64(4), &[]));

        let empty = parse(b"Scoreboard: \n").unwrap();
        assert_eq!(empty.scoreboard, Some(Scoreboard::default()));
        assert_eq!(empty.to_events(9).len(), 12);
    }
}
