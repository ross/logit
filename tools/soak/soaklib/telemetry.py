"""Reads what a `logit` container wrote: `internal` telemetry as NDJSON on stdout, and the
`--log-format json` self-log on stderr.

stdout follows the grammar in `crates/logit-outputs/src/ndjson.rs`'s module doc, which lists this
file among its readers: one event per line, `timestamp` RFC 3339 UTC with nine fractional digits,
`metrics` an array of `{name, kind, value, ...}`, and the component's identity in `attributes`
(`component`, `kind`, `role`). A `sum` from `internal` is a delta per drain.

A container stopped and started keeps one log, so its stdout spans several process lives.
`assign_lives()` splits them where `logit.process.uptime` (seconds since `internal` started)
decreases, and each point belongs to the newest life that started at or before its timestamp.
Counters are summed per life; gauges are series.

An hours-long run holds hundreds of thousands of points, and the checks query them thousands of
times, so `Telemetry` indexes points by name and caches each (name, attributes) selection in
timestamp order; `counter_in` bisects it. Points sharing a line share one attributes dict, and
identical attribute sets are interned to one, which keeps an 8-hour run's points in a few
hundred MB.

stderr is mostly JSON objects with `timestamp`, `level`, `target`, `component`, `key`, and
`message` at the top level (`docs/deploying.md`, "Self-logging"). A Rust panic message is raw
text (`thread '<name>' panicked at <file>:<line>:<col>:`), so stderr is classified line by line.
"""

import bisect
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


@dataclass(slots=True)
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
    # Caches built on first use: points by name, each selection in timestamp order with its
    # timestamps, and each life's drains. `assign_lives()` clears them.
    _by_name: dict = field(default_factory=dict, repr=False)
    _selections: dict = field(default_factory=dict, repr=False)
    _life_drains: dict = field(default_factory=dict, repr=False)

    def _clear(self):
        self._by_name.clear()
        self._selections.clear()
        self._life_drains.clear()

    def _selection(self, name, attrs):
        """`(points, timestamps)` of `name` whose attributes match `attrs`, in timestamp order
        (file order among equal timestamps)."""
        if self._by_name.get(None) != len(self.points):
            # Points were added since the index was built.
            self._clear()
        key = (name, tuple(sorted(attrs.items())))
        cached = self._selections.get(key)
        if cached is None:
            if not self._by_name:
                self._by_name[None] = len(self.points)
                for point in self.points:
                    self._by_name.setdefault(point.name, []).append(point)
                for key_name, points in self._by_name.items():
                    if key_name is not None:
                        points.sort(key=lambda p: p.ts)
            chosen = [p for p in self._by_name.get(name, ())
                      if all(p.attributes.get(k) == v for k, v in attrs.items())]
            cached = (chosen, [p.ts for p in chosen])
            self._selections[key] = cached
        return cached

    def matching(self, name, **attrs):
        return list(self._selection(name, attrs)[0])

    def counter_by_life(self, name, **attrs):
        """{life: summed delta} for a `sum` metric."""
        sums = {}
        for point in self.matching(name, **attrs):
            if isinstance(point.value, (int, float)):
                sums[point.life] = sums.get(point.life, 0) + point.value
        return sums

    def counter_in(self, name, start, end, **attrs):
        """A `sum` metric's points summed over drains in [start, end)."""
        points, stamps = self._selection(name, attrs)
        low = bisect.bisect_left(stamps, start)
        high = bisect.bisect_left(stamps, end)
        return sum(p.value for p in points[low:high] if isinstance(p.value, (int, float)))

    def counter_points(self, name, **attrs):
        """[(ts, summed delta, life)] for a `sum` metric, one entry per drain, in timestamp
        order. Points sharing a drain (one per attribute set, such as per `reason`) are summed."""
        by_ts = {}
        for point in self.matching(name, **attrs):
            if isinstance(point.value, (int, float)):
                value, _ = by_ts.get(point.ts, (0, point.life))
                by_ts[point.ts] = (value + point.value, point.life)
        return [(ts, value, life) for ts, (value, life) in sorted(by_ts.items())]

    def life_drains(self, life):
        """The drain timestamps of one process life, in order."""
        if life not in self._life_drains:
            self._life_drains[life] = sorted({p.ts for p in self._selection(UPTIME, {})[0]
                                              if p.life == life})
        return list(self._life_drains[life])

    def gauge_series(self, name, **attrs):
        """[(ts, value, life)] for a gauge, in timestamp order."""
        series = [(p.ts, p.value, p.life) for p in self.matching(name, **attrs)
                  if isinstance(p.value, (int, float))]
        series.sort(key=lambda item: item[0])
        return series


def parse_ndjson(lines):
    """`Telemetry` from NDJSON lines (any iterable of str)."""
    telemetry = Telemetry()
    # Every line of one drain carries the same timestamp, and a run repeats a few dozen
    # attribute sets, so both are parsed or kept once.
    stamps = {}
    interned = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
            text = event["timestamp"]
            ts = stamps.get(text) if isinstance(text, str) else None
            if ts is None:
                ts = parse_rfc3339(text)
                stamps[text] = ts
        except (ValueError, KeyError, TypeError):
            telemetry.bad_lines += 1
            continue
        attributes = event.get("attributes") or {}
        try:
            attributes = interned.setdefault(tuple(sorted(attributes.items())), attributes)
        except TypeError:
            pass  # an unhashable value: keep this line's own dict
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
    # A drain's timestamp can precede `ts - uptime` by the uptime's rounding, so a point
    # belongs to the newest life that started at most 1 s after it.
    shifted = [start - 1.0 for start in starts]
    ordered = all(a <= b for a, b in zip(shifted, shifted[1:]))
    for point in telemetry.points:
        if ordered:
            point.life = max(0, bisect.bisect_right(shifted, point.ts) - 1)
        else:
            point.life = max((i for i, s in enumerate(shifted) if point.ts >= s), default=0)
    telemetry._clear()


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
