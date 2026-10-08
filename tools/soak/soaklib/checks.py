"""Scores a run directory: one function per check, each returning a `Result` with a status
(`PASS`, `WARN`, `FAIL`, or `SKIP`), a one-line detail, and the lines behind it.

docs/plans/soak-harness.md's "The checks" is the spec. Every check reads the run directory
only, so `soak.py check <run-dir>` re-scores a run offline.

A "fault window" is a fault's span, from its apply starting to its revert finishing, plus the
scenario's `recovery_bound` after it. Checks that judge steady state skip fault windows, the
warmup where the check says so, and everything from the end sequence on.

The ledger, `identity.sink`, and `recovery` checks are W1b's; here they report `SKIP`.
"""

import json
import statistics
from dataclasses import dataclass, field
from pathlib import Path

from . import collect, telemetry
from .scenario import LOGIT_SERVICES, SERVICES

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"
_ORDER = {PASS: 0, SKIP: 0, WARN: 1, FAIL: 2}

# The self-log lines a sink writes while it can't deliver, expected inside a fault window:
# `retrying` (ERROR, keyed), `send_failed` (WARN, keyed), and `degraded` (WARN, the message, no
# key). docs/deploying.md's "Self-logging" table has their levels.


def is_sink_fault_line(entry):
    return entry.key in ("retrying", "send_failed") or (
        entry.message == "degraded" and not entry.key)
READY_WITHIN_S = 30
FRESHNESS_MAX_AGE_S = 30
# How many `internal` intervals stdout may go quiet while the container runs unpaused.
DRAIN_GAP_INTERVALS = 3
SECONDS_PER_HOUR = 3600
MIB = 1024 * 1024
# A life's RSS slope is judged only over at least this many samples spanning this long.
SLOPE_MIN_SAMPLES = 6
SLOPE_MIN_SPAN_S = 60


@dataclass
class Result:
    id: str
    status: str
    detail: str
    lines: list = field(default_factory=list)

    def to_json(self):
        return {"id": self.id, "status": self.status, "detail": self.detail,
                "lines": self.lines}


def worst(statuses):
    return max(statuses, key=lambda s: _ORDER[s], default=PASS)


def slope(points):
    """Least-squares slope of [(x, y)], or None for fewer than two distinct x."""
    if len(points) < 2:
        return None
    mean_x = sum(x for x, _ in points) / len(points)
    mean_y = sum(y for _, y in points) / len(points)
    var = sum((x - mean_x) ** 2 for x, _ in points)
    if var == 0:
        return None
    return sum((x - mean_x) * (y - mean_y) for x, y in points) / var


def parse_docker_time(text):
    """Docker's `StartedAt`/`FinishedAt` (RFC 3339, nanoseconds) -> epoch seconds, or None for
    Docker's zero time."""
    if not text or text.startswith("0001-"):
        return None
    try:
        return telemetry.parse_rfc3339(text)
    except ValueError:
        return None


def parse_mem(text):
    """`docker stats`' `MemUsage` left side (`45.2MiB`) -> bytes, or None."""
    units = {"B": 1, "KiB": 1024, "MiB": MIB, "GiB": 1024 * MIB, "kB": 1000, "MB": 1000 ** 2,
             "GB": 1000 ** 3}
    value = (text or "").split("/")[0].strip()
    for unit in sorted(units, key=len, reverse=True):
        if value.endswith(unit):
            try:
                return float(value[: -len(unit)]) * units[unit]
            except ValueError:
                return None
    return None


@dataclass
class Fault:
    step: str
    action: str
    on: str
    start: float
    end: float
    affects_udp_ingress: bool
    affects_egress: bool


class RunData:
    """Everything a check reads, loaded once from a run directory."""

    def __init__(self, run_dir):
        self.run_dir = Path(run_dir)
        try:
            self.resolved = json.loads((self.run_dir / "scenario.resolved.json").read_text())
        except (OSError, ValueError):
            self.resolved = {}
        self.timeline = collect.read_jsonl(self.run_dir / "timeline.jsonl")
        self.watchdog = collect.read_jsonl(self.run_dir / "watchdog.jsonl")
        self.stats = collect.read_jsonl(self.run_dir / "stats.ndjson")
        self.freshness = collect.read_jsonl(self.run_dir / "vm-freshness.jsonl")
        self.recovery_bound = float(self.resolved.get("recovery_bound", 0))
        self.warmup = float(self.resolved.get("warmup", 0))
        self.thresholds = self.resolved.get("thresholds", {})
        self.ledger = self.resolved.get("ledger", {})
        self.duration = float(self.resolved.get("duration", 0))

        phases = {r["phase"]: r for r in self.timeline if r.get("event") == "phase"}
        self.phases = phases
        self.t0 = phases.get("start", {}).get("t0")
        last = max((r.get("t") or r.get("finished_at") or 0 for r in self.timeline), default=0)
        self.end = phases.get("end_begin", phases.get("fail_fast", {})).get("t") or last

        self.faults = []
        applies = {}
        for record in self.timeline:
            if record.get("event") == "apply":
                applies[record["step"]] = record
            elif record.get("event") == "revert" and record["step"] in applies:
                apply = applies.pop(record["step"])
                self.faults.append(self._fault(apply, record["finished_at"]))
        for apply in applies.values():
            self.faults.append(self._fault(apply, self.end))

        self.telemetry = {}
        self.stderr = {}
        self.inspect = {}
        for service in SERVICES:
            stdout = self.run_dir / "logs" / f"{service}.stdout"
            if service in LOGIT_SERVICES and stdout.exists():
                self.telemetry[service] = telemetry.read_ndjson(stdout)
            stderr = self.run_dir / "logs" / f"{service}.stderr"
            if service in LOGIT_SERVICES and stderr.exists():
                self.stderr[service] = telemetry.read_stderr(stderr)
            try:
                self.inspect[service] = json.loads(
                    (self.run_dir / "inspect" / f"{service}.json").read_text())
            except (OSError, ValueError):
                pass

    @staticmethod
    def _fault(apply, end):
        return Fault(
            step=apply["step"], action=apply["action"], on=apply["on"],
            start=apply["started_at"], end=end,
            affects_udp_ingress=apply.get("affects_udp_ingress", False),
            affects_egress=apply.get("affects_egress", False),
        )

    def offset(self, t):
        return "?" if t is None or self.t0 is None else f"{t - self.t0:+.0f}s"

    def windows(self, on=None, actions=None, extend=True):
        """[(start, end)] of fault windows, optionally only `on` one service or for some
        actions, extended by `recovery_bound` unless `extend` is False."""
        extra = self.recovery_bound if extend else 0.0
        return [(f.start, f.end + extra) for f in self.faults
                if (on is None or f.on == on) and (actions is None or f.action in actions)]

    def in_window(self, t, windows):
        return any(start <= t <= end for start, end in windows)

    def steady(self, t, after_warmup=False):
        """Whether `t` is inside the judged steady state: after the timeline's zero (and the
        warmup when asked), before the end sequence, and outside every fault window."""
        if self.t0 is None or t is None:
            return False
        if t < self.t0 + (self.warmup if after_warmup else 0) or t >= self.end:
            return False
        return not self.in_window(t, self.windows())

    def quiet_intervals(self, start):
        """[(start, end)] spans from `start` to the end sequence outside every fault window."""
        spans = [(start, self.end)]
        for w_start, w_end in sorted(self.windows()):
            cut = []
            for s, e in spans:
                if w_end <= s or w_start >= e:
                    cut.append((s, e))
                    continue
                if s < w_start:
                    cut.append((s, w_start))
                if w_end < e:
                    cut.append((w_end, e))
            spans = cut
        return spans


# ---- the watchdog --------------------------------------------------------------------------------


def check_exit(data):
    if not data.inspect:
        return Result("exit", SKIP, "no inspect/ files")
    lines = []
    status = PASS
    stop_windows = {}
    for record in data.timeline:
        if record.get("event") in ("apply", "revert") and record.get("action") in ("stop",
                                                                                     "restart"):
            if record["event"] == "apply":
                stop_windows.setdefault(record["on"], []).append(
                    (record["started_at"] - 1, record["finished_at"] + 1))
    end_begin = data.phases.get("end_begin", {}).get("t")
    exits = 0
    for service in LOGIT_SERVICES:
        seen = {}
        for record in data.watchdog:
            if record.get("svc") == service and record.get("status") == "exited":
                seen[record.get("finished_at")] = record.get("exit_code")
        final = (data.inspect.get(service) or {}).get("State") or {}
        if final.get("Status") == "exited":
            seen[final.get("FinishedAt")] = final.get("ExitCode")
        for finished, code in sorted(seen.items(), key=lambda item: item[0] or ""):
            exits += 1
            t = parse_docker_time(finished)
            scheduled = t is not None and (
                data.in_window(t, stop_windows.get(service, []))
                or (end_begin is not None and t >= end_begin)
            )
            if not scheduled:
                status = FAIL
                lines.append(f"{service} exited at {data.offset(t)} with code {code}, outside "
                             "every scheduled stop")
            elif code != 0:
                status = FAIL
                lines.append(f"{service} exited at {data.offset(t)} with code {code}")
        if final.get("Status") != "exited":
            status = FAIL
            lines.append(f"{service} final state is {final.get('Status')}, not exited")
    for service in SERVICES:
        state = (data.inspect.get(service) or {}).get("State") or {}
        if state.get("OOMKilled"):
            status = FAIL
            lines.append(f"{service} was OOM-killed")
    if status == PASS:
        detail = f"{exits} exit(s) of the logit services, each a scheduled stop with code 0"
    else:
        detail = lines[0]
    return Result("exit", status, detail, lines)


def check_restarts(data):
    if not data.watchdog:
        return Result("restarts", SKIP, "no watchdog.jsonl")
    starts = {}
    for record in data.timeline:
        event, action = record.get("event"), record.get("action")
        if (event == "revert" and action == "stop") or (event == "apply" and action == "restart"):
            starts.setdefault(record["on"], []).append(
                (record["started_at"] - 1, record["finished_at"] + 2))
    lines = []
    status = PASS
    changes = 0
    for service in SERVICES:
        seq = []
        for record in data.watchdog:
            if record.get("svc") == service and record.get("started_at"):
                if not seq or seq[-1] != record["started_at"]:
                    seq.append(record["started_at"])
                if record.get("restart_count"):
                    status = FAIL
                    lines.append(f"{service} RestartCount is {record['restart_count']}")
        final = ((data.inspect.get(service) or {}).get("State") or {}).get("StartedAt")
        if final and (not seq or seq[-1] != final):
            seq.append(final)
        for started in seq[1:]:
            changes += 1
            t = parse_docker_time(started)
            if t is None or not data.in_window(t, starts.get(service, [])):
                status = FAIL
                lines.append(f"{service} started at {data.offset(t)} with no scheduled start")
    lines = sorted(set(lines), key=lines.index)
    detail = (f"{changes} start(s), each a scheduled start or restart" if status == PASS
              else lines[0])
    return Result("restarts", status, detail, lines)


def check_self_log(data):
    if not data.stderr:
        return Result("self_log", SKIP, "no logs/*.stderr")
    windows = data.windows()
    status = PASS
    lines = []
    counts = {"error": 0, "fault_key_inside": 0, "fault_key_outside": 0, "text": 0}
    for service, entries in data.stderr.items():
        for entry in entries:
            if entry.kind == "panic":
                status = FAIL
                lines.append(f"{service}: panic: {entry.raw[:200]}")
                continue
            if entry.kind == "text":
                counts["text"] += 1
                status = worst([status, WARN])
                lines.append(f"{service}: non-JSON stderr: {entry.raw[:200]}")
                continue
            if entry.message == "exiting":
                code = entry.fields.get("code")
                if code not in (0, "0", None):
                    status = FAIL
                    lines.append(f"{service}: exiting with code {code} at {data.offset(entry.ts)}"
                                 f": {entry.fields.get('reason', '')}")
                continue
            if entry.key == "thread_panicked":
                status = FAIL
                lines.append(f"{service}: thread_panicked: {entry.raw[:200]}")
                continue
            if is_sink_fault_line(entry):
                if entry.ts is not None and data.in_window(entry.ts, windows):
                    counts["fault_key_inside"] += 1
                else:
                    counts["fault_key_outside"] += 1
                    status = worst([status, WARN])
                    lines.append(f"{service}: {entry.level} {entry.key or entry.message} at "
                                 f"{data.offset(entry.ts)}, outside every fault window: "
                                 f"{entry.message[:160]}")
                continue
            if entry.level == "ERROR":
                counts["error"] += 1
                status = FAIL
                lines.append(f"{service}: ERROR at {data.offset(entry.ts)}: "
                             f"key={entry.key or '-'} {entry.message[:160]}")
    summary = (f"{counts['fault_key_inside']} sink fault line(s) inside fault windows, "
               f"{counts['fault_key_outside']} outside, {counts['error']} other ERROR, "
               f"{counts['text']} non-JSON")
    return Result("self_log", status, summary if status == PASS else f"{lines[0]} ({summary})",
                  lines)


def check_ready(data):
    if not data.watchdog:
        return Result("ready", SKIP, "no watchdog.jsonl")
    status = PASS
    lines = []
    judged = 0
    for record in data.watchdog:
        if record.get("svc") not in LOGIT_SERVICES or record.get("status") != "running":
            continue
        if not data.steady(record.get("t")):
            continue
        judged += 1
        if record.get("health") != "healthy":
            status = FAIL
            lines.append(f"{record['svc']} health {record.get('health')} at "
                         f"{data.offset(record['t'])}, outside every fault window")
    readies = [r for r in data.timeline if r.get("event") == "ready"]
    for record in readies:
        if not record.get("ok") or record.get("ready_s", 0) > READY_WITHIN_S:
            status = FAIL
            lines.append(f"{record['on']} after {record.get('why')} ({record.get('step')}): "
                         f"{'ready' if record.get('ok') else 'not ready'} after "
                         f"{record.get('ready_s')}s")
    slowest = max((r.get("ready_s", 0) for r in readies), default=None)
    detail = (f"{judged} health sample(s) healthy outside fault windows; {len(readies)} "
              "start(s) or unpause(s) ready in time"
              + (f", slowest ready {slowest:.1f}s" if slowest is not None else ""))
    return Result("ready", status, detail if status == PASS else lines[0], lines)


def check_progress(data):
    sut = data.telemetry.get("logit")
    if sut is None or data.t0 is None:
        return Result("progress", SKIP, "no SUT telemetry or no timeline start")
    sink = data.ledger.get("sut_sink")
    window = float(data.thresholds.get("progress_window", 30))
    status = PASS
    lines = []
    judged = 0
    for start, end in data.quiet_intervals(data.t0 + window):
        t = start
        while t + window <= end:
            judged += 1
            delivered = sut.counter_in("logit.component.batches.delivered", t, t + window,
                                       component=sink)
            if delivered <= 0:
                verdict, why = classify_stall(sut, sink, t, t + window)
                status = worst([status, verdict])
                lines.append(f"no {sink} deliveries in [{data.offset(t)}, "
                             f"{data.offset(t + window)}): {why}")
            t += window

    for service, tel in data.telemetry.items():
        drains = [d for d in tel.drains if data.t0 <= d < data.end]
        if len(drains) < 3:
            continue
        interval = statistics.median(b - a for a, b in zip(drains, drains[1:]))
        held = data.windows(on=service, actions=("stop", "pause", "restart"), extend=False)
        for a, b in zip(drains, drains[1:]):
            if b - a > DRAIN_GAP_INTERVALS * interval and not any(
                s <= b and e >= a for s, e in held
            ):
                status = FAIL
                lines.append(f"{service} stdout went quiet for {b - a:.0f}s from "
                             f"{data.offset(a)} (internal interval {interval:.0f}s)")

    fresh_judged = 0
    for record in data.freshness:
        t = record.get("t")
        if record.get("skipped") or t is None or t < data.t0 + window or not data.steady(t):
            continue
        fresh_judged += 1
        age = record.get("age_s")
        if record.get("error") or age is None or age >= FRESHNESS_MAX_AGE_S:
            status = FAIL
            lines.append(f"VictoriaMetrics freshness at {data.offset(t)}: "
                         f"{record.get('error') or f'newest sample {age}s old'}")
    detail = (f"{judged} {window:.0f}s window(s) with deliveries outside fault windows; "
              f"{fresh_judged} freshness sample(s) under {FRESHNESS_MAX_AGE_S}s")
    return Result("progress", status, detail if status == PASS else lines[0], lines)


def classify_stall(tel, sink, start, end):
    """A window with no deliveries: a slow drain (WARN) if the sink is retrying, a hang (FAIL)
    if work is queued with nothing moving, else FAIL with nothing arriving."""
    retries = tel.counter_in("logit.component.retries", start, end, component=sink)
    errors = tel.counter_in("logit.component.errors", start, end, component=sink)
    inbox_full = tel.counter_in("logit.component.inbox.full", start, end, component=sink)
    queued = [v for ts, v, _ in tel.gauge_series("logit.component.buffer.batches",
                                                 component=sink) if start <= ts < end]
    if retries > 0 or errors > 0:
        return WARN, f"slow drain past recovery_bound ({retries:g} retries, {errors:g} errors)"
    if (queued and max(queued) > 0) or inbox_full > 0:
        return FAIL, (f"hang: {max(queued, default=0):g} batch(es) queued, {inbox_full:g} "
                      "inbox.full, retries and errors flat")
    return FAIL, "nothing delivered and nothing queued"


def _rss_points(data, service):
    tel = data.telemetry.get(service)
    by_life = {}
    if tel is not None:
        for ts, value, life in tel.gauge_series("logit.process.memory.resident.bytes"):
            if data.steady(ts, after_warmup=True):
                by_life.setdefault(life, []).append((ts, value))
    if by_life:
        return by_life, "telemetry"
    for record in data.stats:
        if record.get("svc") != service:
            continue
        value = parse_mem(record.get("MemUsage"))
        if value is not None and data.steady(record.get("t"), after_warmup=True):
            by_life.setdefault(0, []).append((record["t"], value))
    return by_life, "docker stats"


def check_rss_slope(data):
    limit = float(data.thresholds.get("rss_growth_mib_per_hour", 64))
    over = WARN if data.duration < SECONDS_PER_HOUR else FAIL
    status = PASS
    lines = []
    judged = []
    for service in LOGIT_SERVICES:
        by_life, source = _rss_points(data, service)
        for life, points in sorted(by_life.items()):
            span = points[-1][0] - points[0][0] if points else 0
            if len(points) < SLOPE_MIN_SAMPLES or span < SLOPE_MIN_SPAN_S:
                lines.append(f"{service} life {life}: {len(points)} sample(s) over {span:.0f}s, "
                             "too few to judge")
                continue
            per_hour = slope(points) * SECONDS_PER_HOUR / MIB
            judged.append(f"{service}/{life} {per_hour:+.1f}")
            line = (f"{service} life {life}: {per_hour:+.1f} MiB/h over {len(points)} samples, "
                    f"{span:.0f}s ({source})")
            lines.append(line)
            if per_hour > limit:
                status = worst([status, over])
    if not judged:
        return Result("rss_slope", SKIP, "no life with enough steady-state samples", lines)
    detail = f"MiB/h per service/life: {', '.join(judged)} (limit {limit:g})"
    return Result("rss_slope", status, detail, lines)


def check_fd_slope(data):
    growth = float(data.thresholds.get("fd_growth", 8))
    status = PASS
    lines = []
    summaries = []
    if data.t0 is None:
        return Result("fd_slope", SKIP, "no timeline start")
    for service in LOGIT_SERVICES:
        tel = data.telemetry.get(service)
        if tel is None:
            continue
        series = [(ts, v) for ts, v, _ in tel.gauge_series("logit.process.fds")
                  if ts < data.end]
        warm = [v for ts, v in series if data.t0 <= ts < data.t0 + data.warmup]
        if not warm or not series:
            lines.append(f"{service}: no warmup samples")
            continue
        baseline = statistics.median(warm)
        end_value = series[-1][1]
        summaries.append(f"{service} {baseline:g}->{end_value:g}")
        if end_value - baseline > growth:
            status = FAIL
            lines.append(f"{service}: {end_value:g} fds at the end, {baseline:g} in the warmup")
        for fault in data.faults:
            after = fault.end + data.recovery_bound
            sample = next((v for ts, v in series if ts >= after), None)
            if sample is not None and sample - baseline > growth:
                status = FAIL
                lines.append(f"{service}: {sample:g} fds {data.recovery_bound:.0f}s after "
                             f"{fault.step} ({fault.action} on {fault.on}), baseline "
                             f"{baseline:g}")
    if not summaries:
        return Result("fd_slope", SKIP, "no fd samples", lines)
    detail = f"warmup median -> end: {', '.join(summaries)} (limit +{growth:g})"
    return Result("fd_slope", status, detail if status == PASS else lines[0], lines)


# ---- W1b ----------------------------------------------------------------------------------------


def _w1b(check_id):
    def stub(data):
        return Result(check_id, SKIP, "W1b: not built yet")
    stub.__name__ = f"check_{check_id.replace('.', '_')}"
    return stub


check_ledger_wire = _w1b("ledger.wire")
check_ledger_intake = _w1b("ledger.intake")
check_ledger_edge = _w1b("ledger.edge")
check_ledger_aggregate = _w1b("ledger.aggregate")
check_ledger_egress = _w1b("ledger.egress")
check_identity_sink = _w1b("identity.sink")
check_recovery = _w1b("recovery")

CHECKS = (
    check_exit, check_restarts, check_self_log, check_ready, check_progress, check_rss_slope,
    check_fd_slope, check_ledger_wire, check_ledger_intake, check_ledger_edge,
    check_ledger_aggregate, check_ledger_egress, check_identity_sink, check_recovery,
)


def run_all(run_dir):
    data = RunData(run_dir)
    results = []
    for check in CHECKS:
        try:
            results.append(check(data))
        except Exception as err:  # a checker bug must not hide the other rows
            results.append(Result(check.__name__.removeprefix("check_"), FAIL,
                                  f"checker error: {type(err).__name__}: {err}"))
    return results
