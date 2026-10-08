"""Reads what a `logit` container wrote: `internal` telemetry as NDJSON on stdout, and the
`--log-format json` self-log on stderr.

stdout follows the grammar in `crates/logit-outputs/src/ndjson.rs`'s module doc, which lists this
file among its readers: one event per line, `timestamp` RFC 3339 UTC with nine fractional digits,
`metrics` an array of `{name, kind, value, ...}`, and the component's identity in `attributes`
(`component`, `kind`, `role`). A `sum` from `internal` is a delta per drain.

A container stopped and started keeps one log, so its stdout spans several process lives.
`lives()` splits them where `logit.process.uptime` (seconds since `internal` started) decreases,
and each point belongs to the newest life that started at or before its timestamp. Counters are
summed per life; gauges are series.

stderr is mostly JSON objects with `timestamp`, `level`, `target`, `component`, `key`, and
`message` at the top level (`docs/deploying.md`, "Self-logging"). A Rust panic message is raw
text (`thread '<name>' panicked at <file>:<line>:<col>:`), so stderr is classified line by line.
"""

import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone

UPTIME = "logit.process.uptime"
PANIC_RE = re.compile(r"thread '[^']*' panicked at ")
_RFC3339 = re.compile(
    r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$"
)


def parse_rfc3339(text):
    """RFC 3339 -> epoch seconds as a float. The fraction is added separately because
    `datetime` holds microseconds and the grammar writes nine digits."""
    match = _RFC3339.match(text or "")
    if not match:
        raise ValueError(f"not an RFC 3339 timestamp: {text!r}")
    base, frac, zone = match.groups()
    zone = "+00:00" if zone == "Z" else zone
    seconds = datetime.fromisoformat(base + zone).astimezone(timezone.utc).timestamp()
    if frac:
        seconds += int(frac) / 10 ** len(frac)
    return seconds


@dataclass
class Point:
    ts: float
    name: str
    kind: str
    value: object
    attributes: dict
    life: int = 0


@dataclass
class Telemetry:
    points: list = field(default_factory=list)
    bad_lines: int = 0
    # Epoch seconds each process life started (its first uptime point's timestamp minus uptime).
    life_starts: list = field(default_factory=list)
    # Distinct drain timestamps, from the uptime points: one per `internal` tick.
    drains: list = field(default_factory=list)

    def matching(self, name, **attrs):
        return [p for p in self.points if p.name == name
                and all(p.attributes.get(k) == v for k, v in attrs.items())]

    def counter_by_life(self, name, **attrs):
        """{life: summed delta} for a `sum` metric."""
        sums = {}
        for point in self.matching(name, **attrs):
            if isinstance(point.value, (int, float)):
                sums[point.life] = sums.get(point.life, 0) + point.value
        return sums

    def counter_in(self, name, start, end, **attrs):
        """A `sum` metric's points summed over drains in [start, end)."""
        return sum(p.value for p in self.matching(name, **attrs)
                   if start <= p.ts < end and isinstance(p.value, (int, float)))

    def gauge_series(self, name, **attrs):
        """[(ts, value, life)] for a gauge, in timestamp order."""
        series = [(p.ts, p.value, p.life) for p in self.matching(name, **attrs)
                  if isinstance(p.value, (int, float))]
        series.sort(key=lambda item: item[0])
        return series


def parse_ndjson(lines):
    """`Telemetry` from NDJSON lines (any iterable of str)."""
    telemetry = Telemetry()
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
            ts = parse_rfc3339(event["timestamp"])
        except (ValueError, KeyError, TypeError):
            telemetry.bad_lines += 1
            continue
        attributes = event.get("attributes") or {}
        for metric in event.get("metrics") or []:
            telemetry.points.append(Point(
                ts=ts,
                name=metric.get("name", ""),
                kind=metric.get("kind", ""),
                value=metric.get("value"),
                attributes=attributes,
            ))
    assign_lives(telemetry)
    return telemetry


def read_ndjson(path):
    try:
        with open(path) as handle:
            return parse_ndjson(handle)
    except OSError:
        return Telemetry()


def assign_lives(telemetry):
    uptimes = sorted(
        ((p.ts, p.value) for p in telemetry.points
         if p.name == UPTIME and isinstance(p.value, (int, float))),
        key=lambda item: item[0],
    )
    starts = []
    previous = None
    for ts, uptime in uptimes:
        if previous is None or uptime < previous:
            starts.append(ts - uptime)
        previous = uptime
    telemetry.life_starts = starts
    telemetry.drains = sorted({ts for ts, _ in uptimes})
    for point in telemetry.points:
        life = 0
        for index, start in enumerate(starts):
            # A drain's timestamp can precede `ts - uptime` by the uptime's rounding.
            if point.ts >= start - 1.0:
                life = index
        point.life = life


@dataclass
class LogLine:
    kind: str           # "json", "panic", or "text"
    raw: str
    ts: object = None   # epoch seconds, when the line carried a timestamp
    level: str = ""
    key: str = ""
    message: str = ""
    component: str = ""
    fields: dict = field(default_factory=dict)


def classify(line):
    """One stderr line as a `LogLine`. A JSON object is `json`; the raw panic text is `panic`
    (JSON or not, since a panic message can be nested in a JSON field too); anything else is
    `text`."""
    raw = line.rstrip("\n")
    stripped = raw.strip()
    if stripped.startswith("{"):
        try:
            obj = json.loads(stripped)
        except ValueError:
            obj = None
        if isinstance(obj, dict):
            fields = dict(obj.get("fields") or {})
            fields.update({k: v for k, v in obj.items() if k != "fields"})
            try:
                ts = parse_rfc3339(fields.get("timestamp", ""))
            except ValueError:
                ts = None
            return LogLine(
                kind="panic" if PANIC_RE.search(str(fields.get("message", ""))) else "json",
                raw=raw,
                ts=ts,
                level=str(fields.get("level", "")).upper(),
                key=str(fields.get("key", "")),
                message=str(fields.get("message", "")),
                component=str(fields.get("component", "")),
                fields=fields,
            )
    if PANIC_RE.search(raw):
        return LogLine(kind="panic", raw=raw)
    return LogLine(kind="text", raw=raw)


def read_stderr(path):
    try:
        with open(path, errors="replace") as handle:
            return [classify(line) for line in handle if line.strip()]
    except OSError:
        return []
