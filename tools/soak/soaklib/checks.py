"""Scores a run directory: one function per check, each returning a `Result` with a status
(`PASS`, `WARN`, `FAIL`, or `SKIP`), a one-line detail, and the lines behind it.

docs/plans/soak-harness.md's "The checks" is the spec. Every check reads the run directory
only, so `soak.py check <run-dir>` re-scores a run offline.

A "fault window" is a fault's span, from its apply starting to its revert finishing, plus the
scenario's `recovery_bound` after it. Checks that judge steady state skip fault windows, the
warmup where the check says so, and everything from the end sequence on.

`SKIP` means a check had nothing to read, such as a run with no telemetry.
"""

import json
import re
import statistics
from dataclasses import dataclass, field
from pathlib import Path

from . import collect, telemetry, vm
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
# `progress` and `rss_slope` WARN when what they judged spans less than this share of the run
# after warmup, so a PASS resting on a few minutes of a fault-dense, hours-long run says so.
COVERAGE_MIN_SHARE = 0.05
# Phases that mean the driver didn't run the schedule to its end sequence.
RUN_FAIL_PHASES = ("aborted", "error", "interrupted")


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
        try:
            self.vm_series = vm.parse_export((self.run_dir / "vm-export.jsonl").read_text())
        except OSError:
            self.vm_series = []
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

        # A fault whose apply returned nonzero gets no window, so nothing after it is excused;
        # `timeline` FAILs the action itself.
        self.faults = []
        applies = {}
        for record in self.timeline:
            if record.get("event") == "apply":
                applies[record["step"]] = record
            elif record.get("event") == "revert" and record["step"] in applies:
                apply = applies.pop(record["step"])
                if not apply.get("rc"):
                    self.faults.append(self._fault(apply, record["finished_at"]))
        for apply in applies.values():
            if not apply.get("rc"):
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

    def post_warmup_s(self):
        """Seconds from the end of warmup to the end sequence, or 0 with no timeline start."""
        if self.t0 is None:
            return 0.0
        return max(0.0, self.end - (self.t0 + self.warmup))

    def coverage(self, judged_s):
        """(share, text) for `judged_s` seconds of the run after warmup; share is None when the
        run has no time after warmup."""
        post = self.post_warmup_s()
        if post <= 0:
            return None, f"{judged_s:.0f}s judged, no time after warmup"
        share = judged_s / post
        return share, (f"{judged_s:.0f}s of {post:.0f}s after warmup judged ({share:.0%}; WARN "
                       f"under {COVERAGE_MIN_SHARE:.0%})")

    def steady_span(self, points):
        """Seconds between consecutive [(ts, value)] points with no fault window between them:
        a life's samples can sit on both sides of a fault that took most of the run."""
        windows = self.windows()
        return sum(b - a for (a, _), (b, _) in zip(points, points[1:])
                   if not any(start < b and end > a for start, end in windows))

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


def check_run(data):
    """FAILs a run the driver didn't take from `start` through `end_end`: aborted, errored,
    interrupted, or missing either phase. The driver still exits 130 when interrupted."""
    lines = []
    for name in RUN_FAIL_PHASES:
        record = data.phases.get(name)
        if record is None:
            continue
        reason = record.get("reason") or ""
        if name == "error":
            reason = (record.get("traceback") or "").strip().splitlines()[-1:] or [""]
            reason = reason[0]
        lines.append(f"{name} at {data.offset(record.get('t'))}" + (f": {reason[:300]}"
                                                                     if reason else ""))
    lines += [f"no {name} phase" for name in ("start", "end_end") if name not in data.phases]
    if lines:
        return Result("run", FAIL, "; ".join(lines), lines)
    return Result("run", PASS, "ran from start through end_end")


def check_timeline(data):
    """FAILs when the schedule didn't run as written: an apply or revert that returned nonzero,
    a fail-fast stop, or a start or unpause of a `logit` service with no `ready` record. An
    early revert in the end sequence isn't probed, so it needs none."""
    if data.t0 is None:
        return Result("timeline", SKIP, "no timeline start")
    lines = []
    actions = [r for r in data.timeline if r.get("event") in ("apply", "revert")]
    for record in actions:
        if record.get("rc"):
            lines.append(f"{record['event']} {record.get('step')} ({record.get('action')} on "
                         f"{record.get('on')}) at {data.offset(record.get('started_at'))} "
                         f"returned rc {record['rc']}: {(record.get('stderr') or '')[:160]}")
    fail_fast = data.phases.get("fail_fast")
    if fail_fast is not None:
        lines.append(f"fail fast at {data.offset(fail_fast.get('t'))}: "
                     f"{fail_fast.get('reason')}")
    probed = {(r.get("on"), r.get("step")) for r in data.timeline if r.get("event") == "ready"}
    starts = 0
    for record in actions:
        event, action = record["event"], record.get("action")
        if not ((event == "revert" and action in ("stop", "pause"))
                or (event == "apply" and action == "restart")):
            continue
        if record.get("on") not in LOGIT_SERVICES or record.get("early"):
            continue
        starts += 1
        if (record["on"], record.get("step")) not in probed:
            lines.append(f"{record['on']} {'unpaused' if action == 'pause' else 'started'} at "
                         f"{data.offset(record.get('finished_at'))} ({record.get('step')}) "
                         "with no readiness probe recorded")
    if lines:
        return Result("timeline", FAIL, lines[0], lines)
    return Result("timeline", PASS, f"{len(actions)} apply/revert action(s) returned 0; "
                  f"{starts} start(s) or unpause(s) each probed; no fail fast")


def check_exit(data):
    if not data.inspect:
        return Result("exit", SKIP, "no inspect/ files")
    lines = []
    status = PASS
    stop_windows = {}
    for record in data.timeline:
        if record.get("event") in ("apply", "revert") and record.get("action") in ("stop",
                                                                                     "restart"):
            if record["event"] == "apply" and not record.get("rc"):
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
    judged_after_warmup_s = 0.0
    for start, end in data.quiet_intervals(data.t0 + window):
        t = start
        while t + window <= end:
            judged += 1
            if t >= data.t0 + data.warmup:
                judged_after_warmup_s += window
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
            why = (record.get("error") or ("no sample matches vm_selector" if age is None
                                           else f"newest sample {age}s old"))
            lines.append(f"VictoriaMetrics freshness at {data.offset(t)}: {why}")
    share, coverage = data.coverage(judged_after_warmup_s)
    if share is not None and share < COVERAGE_MIN_SHARE:
        status = worst([status, WARN])
        lines.append(f"delivery windows cover little of the run: {coverage}")
    detail = (f"{judged} {window:.0f}s window(s) with deliveries outside fault windows; "
              f"{fresh_judged} freshness sample(s) under {FRESHNESS_MAX_AGE_S}s; {coverage}")
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
    coverage = []
    for service in LOGIT_SERVICES:
        by_life, source = _rss_points(data, service)
        judged_s = 0.0
        for life, points in sorted(by_life.items()):
            span = points[-1][0] - points[0][0] if points else 0
            if len(points) < SLOPE_MIN_SAMPLES or span < SLOPE_MIN_SPAN_S:
                lines.append(f"{service} life {life}: {len(points)} sample(s) over {span:.0f}s, "
                             "too few to judge")
                continue
            judged_s += data.steady_span(points)
            per_hour = slope(points) * SECONDS_PER_HOUR / MIB
            judged.append(f"{service}/{life} {per_hour:+.1f}")
            line = (f"{service} life {life}: {per_hour:+.1f} MiB/h over {len(points)} samples, "
                    f"{span:.0f}s ({source})")
            lines.append(line)
            if per_hour > limit:
                status = worst([status, over])
        if by_life:
            share, text = data.coverage(judged_s)
            coverage.append(f"{service} {text}")
            if share is not None and share < COVERAGE_MIN_SHARE:
                status = worst([status, WARN])
                lines.append(f"{service}: slopes cover little of the run: {text}")
    if not judged:
        return Result("rss_slope", SKIP, "no life with enough steady-state samples", lines)
    detail = (f"MiB/h per service/life: {', '.join(judged)} (limit {limit:g}); "
              f"{'; '.join(coverage)}")
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
            sample = next((v for ts, v in series if ts >= after and data.steady(ts)), None)
            if sample is not None and sample - baseline > growth:
                status = FAIL
                lines.append(f"{service}: {sample:g} fds {data.recovery_bound:.0f}s after "
                             f"{fault.step} ({fault.action} on {fault.on}), baseline "
                             f"{baseline:g}")
    if not summaries:
        return Result("fd_slope", SKIP, "no fd samples", lines)
    detail = f"warmup median -> end: {', '.join(summaries)} (limit +{growth:g})"
    return Result("fd_slope", status, detail if status == PASS else lines[0], lines)


# ---- the ledger, the sink identity, and recovery --------------------------------------------------
#
# One unit runs through the ledger: a datagram is one line is one event is one increment. The
# plan's "The ledger and identities" has the symbol table; `Ledger` computes each symbol per SUT
# process life.

# The receive queue's and a batch's defaults (`receive.max_datagrams`, `receive.batch_max_events`),
# used when the SUT config doesn't set them.
DEFAULT_MAX_DATAGRAMS = 10000
DEFAULT_BATCH_MAX_EVENTS = 1000
# Batches of at most `batch_max_events` that can be in flight past the receive queue at a
# shutdown signal: the accumulator, the batch the listener holds while its send waits on a full
# inbox, `aggregate`'s 64-slot inbox, and the batch `aggregate` has taken but not yet absorbed.
RESIDUAL_BATCHES = 67
# The UDP listener's `warn` lines for datagrams it counted as `datagrams.dropped{reason=
# "shutdown"}` after `internal`'s final drain (`crates/logit-inputs/src/udp.rs`, `ReadHalf` and
# `ResidualOnDrop`).
SHUTDOWN_DROP_RE = re.compile(
    r"^(\d+) datagram\(s\) (?:still in the receive queue|read off the socket but never queued)")
# `recovery`'s limits for the gauges and the ingest rate, against the warmup baseline.
RECOVERED_UTILIZATION = 0.05
RECOVERED_RATE_SHARE = 0.95


def _int(value):
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _n(value):
    """A count for a detail string: thousands separated, integral when it is one."""
    if isinstance(value, float) and not value.is_integer():
        return f"{value:,.1f}"
    return f"{int(value):,}"


def _yaml_int(text, key):
    """The first `key: <int>` in a YAML text, block or flow style, or None. The SUT config has
    one UDP listener, so the first match is its `receive:` field."""
    match = re.search(rf"\b{key}\s*:\s*[\"']?(\d+)", text or "")
    return int(match.group(1)) if match else None


class Ledger:
    """The ledger's symbols per SUT process life, read once from a run's telemetry, stderr, and
    VictoriaMetrics export. Lives are the SUT's; the generator's G is one number for the run."""

    def __init__(self, data):
        cfg = data.ledger
        self.data = data
        self.listener = cfg.get("sut_listener")
        self.aggregate = cfg.get("sut_aggregate")
        self.sink = cfg.get("sut_sink")
        self.gen_input = cfg.get("generator_input")
        self.gen_sink = cfg.get("generator_sink")
        self.sut = data.telemetry.get("logit")
        self.gen = data.telemetry.get("generator")
        self.problems = []
        if self.sut is None:
            self.problems.append("no SUT telemetry")
            self.lives = []
            return
        self.life_starts = self.sut.life_starts
        self.lives = list(range(len(self.life_starts))) or [0]
        self.final = self.lives[-1]

        sut, listener = self.sut, self.listener
        self.W = sut.counter_by_life("logit.input.datagrams", component=listener)
        self.K = sut.counter_by_life("logit.input.kernel.drops", component=listener)
        self.D_tel = sut.counter_by_life("logit.component.datagrams.dropped", component=listener)
        self.B = sut.counter_by_life("logit.component.diagnostics", component=listener,
                                     key="bad_line")
        self.E = sut.counter_by_life("logit.component.events.sent", component=listener)
        self.A = sut.counter_by_life("logit.component.events.received",
                                     component=self.aggregate)
        self.Ab = sut.counter_by_life("logit.transform.metrics.absorbed",
                                      component=self.aggregate)

        # Shutdown drops and `drain complete` land after the final drain, so they come from
        # stderr, each credited to the life its timestamp falls in.
        self.D_log = {}
        self.batches_dropped = {}
        self.drain_completes = {}
        for entry in data.stderr.get("logit", []):
            if entry.kind != "json" or entry.ts is None:
                continue
            life = vm.life_at(self.life_starts, entry.ts)
            match = SHUTDOWN_DROP_RE.match(entry.message)
            if match and entry.component == listener:
                self.D_log[life] = self.D_log.get(life, 0) + int(match.group(1))
            elif entry.message == "drain complete":
                self.drain_completes[life] = self.drain_completes.get(life, 0) + 1
                self.batches_dropped[life] = (self.batches_dropped.get(life, 0)
                                              + _int(entry.fields.get("batches_dropped")))

        self.series = data.vm_series
        self.V, self.V_total, self.resets, self.mismatched = vm.totals_by_life(
            self.series, self.life_starts)

        if self.gen is not None:
            self.G = sum(self.gen.counter_by_life("logit.output.messages",
                                                  component=self.gen_sink).values())
            self.G_datagrams = sum(self.gen.counter_by_life("logit.output.datagrams",
                                                            component=self.gen_sink).values())
            self.gen_dropped_events = sum(self.gen.counter_by_life(
                "logit.component.events.dropped", component=self.gen_sink).values())
            self.gen_dropped_batches = sum(self.gen.counter_by_life(
                "logit.component.batches.dropped", component=self.gen_sink).values())
            sent = sum(self.gen.counter_by_life("logit.component.events.sent",
                                                component=self.gen_input).values())
            batches = sum(self.gen.counter_by_life("logit.component.batches.sent",
                                                   component=self.gen_input).values())
            self.gen_batch = sent / batches if batches else 0.0
        else:
            self.problems.append("no generator telemetry")

        text = self._sut_config_text()
        max_datagrams = _yaml_int(text, "max_datagrams") or DEFAULT_MAX_DATAGRAMS
        batch_max = _yaml_int(text, "batch_max_events") or DEFAULT_BATCH_MAX_EVENTS
        self.R = max_datagrams + RESIDUAL_BATCHES * batch_max
        self.R_text = f"R {_n(self.R)} = {_n(max_datagrams)} + {RESIDUAL_BATCHES} x {_n(batch_max)}"

    def _sut_config_text(self):
        """The SUT config: the run directory's copy, else the path `compose.env` names."""
        name = (self.data.resolved.get("configs") or {}).get("sut")
        candidates = [self.data.run_dir / "configs" / name] if name else []
        try:
            for line in (self.data.run_dir / "compose.env").read_text().splitlines():
                if line.startswith("SOAK_SUT_CONFIG="):
                    candidates.append(Path(line.split("=", 1)[1]))
        except OSError:
            pass
        for path in candidates:
            try:
                return path.read_text()
            except OSError:
                continue
        return ""

    def get(self, symbol, life):
        return getattr(self, symbol).get(life, 0)

    def D(self, life):
        return self.get("D_tel", life) + self.get("D_log", life)

    def symbols(self, life):
        return (f"W {_n(self.get('W', life))}, K {_n(self.get('K', life))}, D {_n(self.D(life))} "
                f"({_n(self.get('D_tel', life))} telemetry + {_n(self.get('D_log', life))} "
                f"stderr), B {_n(self.get('B', life))}, E {_n(self.get('E', life))}, "
                f"A {_n(self.get('A', life))}, Ab {_n(self.get('Ab', life))}, "
                f"V {_n(self.get('V', life))}")

    def residual(self, life):
        """W − D − B − Ab: what a life read but didn't absorb by its last drain."""
        return self.get("W", life) - self.D(life) - self.get("B", life) - self.get("Ab", life)

    def egress(self, life):
        """`(status, verdict, gap)` for one life's Ab − V: `ok`, `counted`, or `uncounted`."""
        gap = self.get("Ab", life) - self.get("V", life)
        dropped = self.batches_dropped.get(life, 0)
        if gap == 0:
            return PASS, "ok", gap
        if life != self.final and -self.R <= gap < 0:
            return PASS, "ok", gap
        if gap > 0 and dropped > 0:
            return PASS, "counted", gap
        return FAIL, "uncounted", gap


def _ledger(data):
    if not hasattr(data, "_ledger"):
        data._ledger = Ledger(data)
    return data._ledger


def _skip_without_sut(check_id, led):
    if not led.lives or led.sut is None:
        return Result(check_id, SKIP, "; ".join(led.problems) or "no SUT telemetry")
    return None


def _wire_buckets(data, led):
    """G − (W + K) per SUT drain interval, classified `fault`, `steady`, or `end`.

    G is cumulative at each generator drain, anchored at 0 at the generator's start and held
    flat across a gap between its lives, and is read at each SUT drain timestamp by linear
    interpolation, because the two processes' drains are out of phase. Within a run of steady
    intervals the interpolation telescopes; each edge of a run errs by at most one generator
    batch, which is the judgment's tolerance. Returns `[(start, end, kind, label, g, wk)]`."""
    gen, sut = led.gen, led.sut
    anchors = []
    cum = 0.0
    points = gen.counter_points("logit.output.messages", component=led.gen_sink)
    starts = list(gen.life_starts)
    for life, start in enumerate(starts):
        anchors.append((start, cum))
        for ts, value, point_life in points:
            if point_life == life:
                cum += value
                anchors.append((ts, cum))
    anchors.sort(key=lambda item: item[0])

    def g_at(t):
        if not anchors or t <= anchors[0][0]:
            return 0.0
        for (t1, c1), (t2, c2) in zip(anchors, anchors[1:]):
            if t1 <= t <= t2:
                return c1 if t2 == t1 else c1 + (c2 - c1) * (t - t1) / (t2 - t1)
        return anchors[-1][1]

    counts = {}
    for name in ("logit.input.datagrams", "logit.input.kernel.drops"):
        for ts, value, _ in sut.counter_points(name, component=led.listener):
            counts[ts] = counts.get(ts, 0) + value
    drains = sorted(set(sut.drains) | set(counts))
    windows = [(f.start, f.end + data.recovery_bound, f"{f.step} {f.action} on {f.on}")
               for f in data.faults if f.affects_udp_ingress]
    buckets = []
    previous = led.life_starts[0] if led.life_starts else (drains[0] if drains else 0)
    for drain in drains:
        if drain <= previous:
            continue
        hits = [label for start, end, label in windows if start < drain and end > previous]
        if hits:
            kind, label = "fault", ", ".join(hits)
        elif drain > data.end:
            kind, label = "end", "end sequence"
        else:
            kind, label = "steady", "no UDP-affecting fault"
        buckets.append((previous, drain, kind, label, g_at(drain) - g_at(previous),
                        counts.get(drain, 0)))
        previous = drain
    return buckets


def check_ledger_wire(data):
    """G − (W + K), loss by design: reported per run of drain intervals, and judged only
    outside UDP-affecting fault windows and at the end. See `_wire_buckets` for the tolerance."""
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.wire", led)
    if skipped or led.gen is None:
        return skipped or Result("ledger.wire", SKIP, "no generator telemetry")
    status = PASS
    lines = []
    W = sum(led.W.values())
    K = sum(led.K.values())
    gap = led.G - W - K
    if led.G != led.G_datagrams:
        status = FAIL
        lines.append(f"precondition: generator output.messages {_n(led.G)} != output.datagrams "
                     f"{_n(led.G_datagrams)}, so a datagram isn't one line")

    buckets = _wire_buckets(data, led)
    runs = []
    for start, end, kind, label, g, wk in buckets:
        if runs and runs[-1][2] == kind and runs[-1][3] == label:
            runs[-1][1] = end
            runs[-1][4] += g
            runs[-1][5] += wk
        else:
            runs.append([start, end, kind, label, g, wk])
    sums = {"fault": 0.0, "steady": 0.0, "end": 0.0}
    g_steady = 0.0
    edges = 0
    for index, (start, end, kind, label, g, wk) in enumerate(runs):
        sums[kind] += g - wk
        lines.append(f"[{data.offset(start)}, {data.offset(end)}] {kind} ({label}): "
                     f"G {_n(round(g))}, W+K {_n(wk)}, gap {_n(round(g - wk))}")
        if kind == "steady":
            g_steady += g
            edges += sum(1 for other in (index - 1, index + 1)
                         if 0 <= other < len(runs) and runs[other][2] != "steady")

    limit = float(data.ledger.get("wire_loss_outside_faults", 0.0))
    tolerance = led.gen_batch * edges
    judged = sums["steady"] + max(0.0, sums["end"])
    allowed = limit * g_steady + tolerance
    if judged > allowed:
        status = FAIL
        lines.insert(0, f"{_n(round(judged))} lost outside UDP-affecting windows, over "
                        f"{limit:g} x {_n(round(g_steady))} + {edges} edge(s) x one "
                        f"{_n(led.gen_batch)}-line batch")
    last_queued = [v for _, v, _ in led.gen.gauge_series("logit.component.buffer.batches",
                                                         component=led.gen_sink)]
    end_allow = ((last_queued[-1] if last_queued else 0) + 1) * led.gen_batch
    if sums["end"] < -end_allow:
        status = FAIL
        lines.insert(0, f"G falls {_n(round(-sums['end']))} short of W + K at the end, over "
                        f"the end allowance "
                        f"{_n(end_allow)} (the generator sink's last buffer.batches + 1 batch)")
    detail = (f"G {_n(led.G)} = W {_n(W)} + K {_n(K)} + wire {_n(gap)} (by design: "
              f"{_n(round(sums['fault']))} in UDP-affecting windows, "
              f"{_n(round(sums['steady']))} steady, {_n(round(sums['end']))} at the end; "
              f"steady limit {_n(round(allowed))}); the generator's counted drop_newest loss, "
              f"not in G: {_n(led.gen_dropped_events)} event(s) in "
              f"{_n(led.gen_dropped_batches)} batch(es)")
    if status != PASS:
        detail = f"{lines[0]}; {detail}"
    return Result("ledger.wire", status, detail, lines)


def _per_life_lines(led):
    return [f"life {life}{' (final)' if life == led.final else ''}: {led.symbols(life)}"
            for life in led.lives]


def check_ledger_intake(data):
    """The final life's W − D == E + B; each earlier life's W − D − B − Ab in [0, R]."""
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.intake", led)
    if skipped:
        return skipped
    status = PASS
    lines = _per_life_lines(led)
    life = led.final
    lhs = led.get("W", life) - led.D(life)
    rhs = led.get("E", life) + led.get("B", life)
    parts = [f"final life {life}: W − D {_n(lhs)} vs E + B {_n(rhs)}"]
    if lhs != rhs:
        status = FAIL
        parts[0] += f", off by {_n(lhs - rhs)}"
    for earlier in led.lives[:-1]:
        residual = led.residual(earlier)
        ok = 0 <= residual <= led.R
        parts.append(f"life {earlier}: W − D − B − Ab {_n(residual)} "
                     f"{'within' if ok else 'outside'} [0, R]")
        if not ok:
            status = FAIL
    detail = "; ".join(parts) + f" ({led.R_text})"
    return Result("ledger.intake", status, detail, lines)


def _final_equality(data, check_id, left, right, what):
    led = _ledger(data)
    skipped = _skip_without_sut(check_id, led)
    if skipped:
        return skipped
    lines = []
    for life in led.lives:
        a, b = led.get(left, life), led.get(right, life)
        lines.append(f"life {life}{' (final)' if life == led.final else ''}: {left} {_n(a)}, "
                     f"{right} {_n(b)}, {left} − {right} {_n(a - b)}"
                     + ("" if life == led.final else " (reported, not judged)"))
    a, b = led.get(left, led.final), led.get(right, led.final)
    detail = f"final life {led.final}: {left} {_n(a)} == {right} {_n(b)} ({what})"
    if a != b:
        return Result(check_id, FAIL, f"final life {led.final}: {left} {_n(a)} != {right} "
                      f"{_n(b)}, off by {_n(a - b)} ({what})", lines)
    return Result(check_id, PASS, detail, lines)


def check_ledger_edge(data):
    """The final life's E == A, a single-consumer edge."""
    return _final_equality(data, "ledger.edge", "E", "A", "listener to aggregate")


def check_ledger_aggregate(data):
    """The final life's Ab == A."""
    return _final_equality(data, "ledger.aggregate", "Ab", "A", "every event absorbed")


def check_ledger_egress(data):
    """Ab − V per life: 0, or within [−R, 0] for an earlier life, passes; a positive gap with
    that life's `drain complete` `batches_dropped` above 0 is counted, never reconciled; any
    other gap is uncounted loss. An export with no series can't pass."""
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.egress", led)
    if skipped:
        return skipped
    status = PASS
    lines = []
    parts = []
    for life in led.lives:
        verdict_status, verdict, gap = led.egress(life)
        status = worst([status, verdict_status])
        dropped = led.batches_dropped.get(life, 0)
        part = (f"life {life}{' (final)' if life == led.final else ''}: Ab {_n(led.get('Ab', life))}"
                f" − V {_n(led.get('V', life))} = {_n(gap)}, {verdict}")
        if gap > 0:
            part += f" (drain complete batches_dropped {_n(dropped)})"
        parts.append(part)
    if not led.series:
        status = FAIL
        parts.insert(0, "no series in vm-export.jsonl match vm_selector "
                        f"{data.ledger.get('vm_selector')!r}")
    lives = len(led.lives)
    wrong = {name: n for name, n in led.resets.items() if n != lives - 1}
    lines.append(f"{len(led.series)} series, reset-aware V {_n(led.V_total)}; resets per series "
                 f"should be {lives - 1} (SUT lives − 1)")
    if wrong:
        status = worst([status, WARN])
        sample = ", ".join(f"{name} {n}" for name, n in list(wrong.items())[:5])
        lines.insert(0, f"{len(wrong)} series with resets other than {lives - 1}: {sample}; "
                        "their segments are credited by timestamp")
    for life in led.lives:
        if life != led.final and led.drain_completes.get(life, 0) == 0:
            lines.append(f"life {life} has no drain complete line")
    detail = "; ".join(parts) + f" ({led.R_text})"
    if wrong:
        detail += f"; {len(wrong)} series' resets aren't SUT lives − 1"
    return Result("ledger.egress", status, detail, lines)


def check_ledger_summary(data):
    """The final life's uncounted = (W − D − E − B) + (E − A) + (A − Ab) + (Ab − V), which must
    be 0, with wire loss beside it by design. A counted egress term is shown and not judged."""
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.summary", led)
    if skipped:
        return skipped
    life = led.final
    g = led.get
    terms = [
        ("W − D − E − B", g("W", life) - led.D(life) - g("E", life) - g("B", life)),
        ("E − A", g("E", life) - g("A", life)),
        ("A − Ab", g("A", life) - g("Ab", life)),
    ]
    _, verdict, egress_gap = led.egress(life)
    counted = verdict == "counted"
    judged = sum(value for _, value in terms) + (0 if counted else egress_gap)
    shown = [f"{name} {_n(value)}" for name, value in terms]
    shown.append(f"Ab − V {_n(egress_gap)}" + (" counted" if counted else ""))
    wire = ""
    if led.gen is not None:
        wire = (f"; wire G − (W + K) {_n(led.G - sum(led.W.values()) - sum(led.K.values()))} "
                "over the run, by design")
    status = PASS if judged == 0 else FAIL
    detail = f"final life {life}: uncounted {_n(judged)} = " + " + ".join(shown) + wire
    return Result("ledger.summary", status, detail, _per_life_lines(led))


def check_identity_sink(data):
    """At the final life's last drain before its shutdown drain: batches.received ==
    delivered + dropped (every reason) + buffer.batches, one batch in flight allowed."""
    led = _ledger(data)
    skipped = _skip_without_sut("identity.sink", led)
    if skipped:
        return skipped
    sut, sink, life = led.sut, led.sink, led.final
    drains = [d for d in sut.life_drains(life)]
    if len(drains) < 2:
        return Result("identity.sink", SKIP, f"final life {life} has under two drains")
    at = drains[-2]

    def upto(name):
        return sum(v for ts, v, point_life in sut.counter_points(name, component=sink)
                   if point_life == life and ts <= at)

    received = upto("logit.component.batches.received")
    delivered = upto("logit.component.batches.delivered")
    dropped = upto("logit.component.batches.dropped")
    queued = next((v for ts, v, point_life in reversed(sut.gauge_series(
        "logit.component.buffer.batches", component=sink)) if point_life == life and ts <= at), 0)
    gap = received - delivered - dropped - queued
    detail = (f"final life {life} at {data.offset(at)}: received {_n(received)} vs delivered "
              f"{_n(delivered)} + dropped {_n(dropped)} + buffer.batches {_n(queued)}, "
              f"gap {_n(gap)} (one batch in flight allowed)")
    return Result("identity.sink", PASS if 0 <= gap <= 1 else FAIL, detail)


def _step_value(series, t, default=0.0):
    """A gauge's last value at or before `t`, or `default`."""
    value = default
    for ts, v, _ in series:
        if ts > t:
            break
        value = v
    return value


def check_recovery(data):
    """Each fault has recovered when, at some SUT drain within `recovery_bound` of its end, the
    sink's `retrying` is 0, its `buffer.utilization` and the listener's `receive.utilization`
    are under 0.05, and the ingest rate over the drain interval ending there is at least 95% of
    the generator's warmup rate. A drain interval another fault overlaps, or that spans two SUT
    lives, isn't eligible; a fault with no eligible interval is skipped and listed. A generator
    `rate_behind` diagnostic turns a throughput shortfall into a WARN."""
    led = _ledger(data)
    skipped = _skip_without_sut("recovery", led)
    if skipped:
        return skipped
    if not data.faults:
        return Result("recovery", PASS, "no faults to recover from")
    if led.gen is None or data.t0 is None:
        return Result("recovery", SKIP, "no generator telemetry or no timeline start")
    sut, gen = led.sut, led.gen
    warm = [(ts, v) for ts, v, _ in gen.counter_points("logit.component.events.sent",
                                                       component=led.gen_input)
            if data.t0 <= ts <= data.t0 + data.warmup]
    if len(warm) < 2 or warm[-1][0] <= warm[0][0]:
        return Result("recovery", SKIP, "too few generator drains in warmup for a baseline")
    baseline = sum(v for _, v in warm[1:]) / (warm[-1][0] - warm[0][0])
    behind = sum(v for _, v, _ in gen.counter_points("logit.component.diagnostics",
                                                     component=led.gen_input, key="rate_behind"))
    retrying = sut.gauge_series("logit.component.retrying", component=led.sink)
    buffer_util = sut.gauge_series("logit.component.buffer.utilization", component=led.sink)
    receive_util = sut.gauge_series("logit.component.receive.utilization",
                                    component=led.listener)
    ingest = {ts: v for ts, v, _ in sut.counter_points("logit.input.datagrams",
                                                       component=led.listener)}
    life_of = {p.ts: p.life for p in sut.points if p.name == telemetry.UPTIME}
    drains = sorted(life_of)

    def assess(start, end):
        """(problems, rate problem or None, rate, share) for the drain interval (start, end]."""
        problems = []
        if _step_value(retrying, end) != 0:
            problems.append(f"retrying {_step_value(retrying, end):g}")
        for label, series in (("buffer.utilization", buffer_util),
                              ("receive.utilization", receive_util)):
            value = _step_value(series, end)
            if value >= RECOVERED_UTILIZATION:
                problems.append(f"{label} {value:.3f}")
        rate = ingest.get(end, 0) / (end - start)
        share = rate / baseline if baseline else 0.0
        slow = None
        if share < RECOVERED_RATE_SHARE:
            slow = f"ingest {rate:,.0f}/s is {share:.0%} of the warmup rate"
        return problems, slow, rate, share

    status = PASS
    lines = []
    judged = 0
    for fault in data.faults:
        bound = fault.end + data.recovery_bound
        name = f"{fault.step} ({fault.action} on {fault.on})"
        eligible = [(a, b) for a, b in zip(drains, drains[1:])
                    if a >= fault.end and b <= bound and b < data.end
                    and life_of[a] == life_of[b]
                    and not any(f is not fault and f.start < b and f.end > a
                                for f in data.faults)]
        if not eligible:
            lines.append(f"{name}: skipped, no drain interval within {data.recovery_bound:g}s "
                         "of its end clear of other faults")
            continue
        judged += 1
        last = None
        for a, b in eligible:
            problems, slow, rate, share = assess(a, b)
            if not problems and slow is None:
                lines.append(f"{name}: recovered at {data.offset(b)}, {b - fault.end:.0f}s "
                             f"after its end, ingest {rate:,.0f}/s ({share:.0%})")
                break
            last = (b, problems, slow)
        else:
            b, problems, slow = last
            verdict = FAIL if problems else (WARN if behind > 0 else FAIL)
            if slow is not None:
                problems.append(slow + (f", but the generator logged rate_behind {_n(behind)} "
                                        "time(s)" if behind > 0 else ""))
            status = worst([status, verdict])
            lines.append(f"{name}: not recovered by {data.offset(b)}: {'; '.join(problems)}")
    failing = [line for line in lines if "not recovered" in line]
    detail = (f"{judged} of {len(data.faults)} fault(s) judged, each recovered within "
              f"recovery_bound {data.recovery_bound:g}s of its end; warmup rate "
              f"{baseline:,.0f}/s")
    if failing:
        detail = f"{failing[0]}; {judged} of {len(data.faults)} fault(s) judged"
    return Result("recovery", status, detail, lines)


CHECKS = (
    check_run, check_timeline, check_exit, check_restarts, check_self_log, check_ready,
    check_progress, check_rss_slope, check_fd_slope, check_ledger_wire, check_ledger_intake,
    check_ledger_edge, check_ledger_aggregate, check_ledger_egress, check_ledger_summary,
    check_identity_sink, check_recovery,
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
