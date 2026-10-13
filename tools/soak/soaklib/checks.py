"""Scores a run directory: one function per check, each returning a `Result` with a status
(`PASS`, `WARN`, `FAIL`, or `SKIP`), a one-line detail, and the lines behind it.

docs/plans/soak-harness.md's "The checks" is the spec. Every check reads the run directory
only, so `soak.py check <run-dir>` re-scores a run offline.

A "fault window" is a fault's span, from its apply starting to its revert finishing, plus the
scenario's `recovery_bound` after it. Checks that judge steady state skip fault windows, the
warmup where the check says so, and everything from the end sequence on.

`SKIP` means a check had nothing it could judge, such as a run with no telemetry, or SUT
telemetry whose process lives disagree with the timeline.

A run against an external target (`[target] kind = "external"`) has no backend query, so every
row that reads V or the freshness samples SKIPs or leaves them out, and `ledger.sent` ends the
ledger at the sinks' own telemetry instead. A local run's `ledger.sent` SKIPs.

A run against a Kafka target (`[target] kind = "kafka"`) has no VictoriaMetrics: the freshness
samples and `ledger.windows` SKIP, `ledger.sent` judges the sink as for an external target, and
V comes from the topic's distinct records, read back by the driver and deduplicated on their
raw bytes (topic.py), so `ledger.egress` and `ledger.summary` judge Ab − V against it, and a
surplus FAILs as on any target. A Kafka target has one sink, whose V the topic is.
`ledger.topic` judges the topic's distinct record count against the records the sink saw
accepted, and its duplicate copies per life against `EPOCH_BUMP_REASONS`; it SKIPs for every
other target.

The ledger follows each `[[ledger.sink]]`. A row about a sink (`progress`, `identity.sink`,
`recovery`, `ledger.egress`, `ledger.windows`, `ledger.replay`, `ledger.sent`) keeps its id,
reports one line per sink, and takes the worst status. Under `split = "copy"` each sink holds a
full copy of `aggregate`'s output, so each sink's V is judged alone; under `split = "route"` the
sinks' V add up to it, so the egress gap is judged on their sum.

The ledger also follows each `[[ledger.source]]`, a generator and the SUT listener it reaches.
`ledger.wire` reports one line per source, each judged against the fault windows of its own
generator and of every service that isn't a generator (`affects_source`), and `ledger.intake`
judges each listener's final life alone. From the edge into `aggregate` on, every row sums the
listeners' symbols, since their events merge there; a source's `sut_transforms` are judged hop
by hop in `ledger.edge`. `recovery` judges the listeners' summed ingest against the generators'
summed warmup rate.

`rss_slope` and `fd_slope` judge each `logit` service on its own line, and a scenario's
`[expected]` table can set the verdict a service should get on either (`judge_services`).
"""

import bisect
import json
import re
import statistics
from dataclasses import dataclass, field
from pathlib import Path

from . import collect, faults, telemetry, topic, vm
from .scenario import (EXPECT_REDUCERS, EXTERNAL_TARGET, GENERATOR_ROLE, KAFKA_TARGET,
                       LOCAL_TARGET, LOGIT_ROLES, RUN_WINDOW, parse_duration)

PASS, WARN, FAIL, SKIP = "PASS", "WARN", "FAIL", "SKIP"
_ORDER = {PASS: 0, SKIP: 0, WARN: 1, FAIL: 2}


def is_start(record):
    """Whether a timeline record started a container again: a revert of a `stop` or `kill`, a
    revert of an `exit` that found the container exited, or a `restart`'s apply. Each start of
    a `logit` service begins a new process life."""
    event, action = record.get("event"), record.get("action")
    if event == "revert" and action == "exit":
        return bool(record.get("started"))
    return ((event == "revert" and action in faults.STARTS_ON_REVERT)
            or (event == "apply" and action in faults.STARTS_ON_APPLY))

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
    # A `mark`'s `ready` word, and an `exit`'s `code` and `key`; None elsewhere.
    ready: object = None
    code: object = None
    key: object = None


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
        self.sinks = [sink["id"] for sink in self.ledger.get("sink") or []]
        self.split = self.ledger.get("split", "copy")
        # `{service: role}` as the driver resolved it from compose.
        self.services = self.resolved.get("services") or {}
        self.logit_services = sorted(name for name, role in self.services.items()
                                     if role in LOGIT_ROLES)
        # `{sink: series}`, each from `vm-export/<sink>.jsonl`.
        self.vm_series = {}
        for sink in self.sinks:
            try:
                self.vm_series[sink] = vm.parse_export(
                    (self.run_dir / "vm-export" / f"{sink}.jsonl").read_text())
            except OSError:
                self.vm_series[sink] = []
        self.duration = float(self.resolved.get("duration", 0))
        # A run directory with no target is a local one.
        self.target = self.resolved.get("target") or {"kind": LOCAL_TARGET,
                                                      "name": LOCAL_TARGET}
        self.external = self.target.get("kind") == EXTERNAL_TARGET
        self.kafka = self.target.get("kind") == KAFKA_TARGET
        self.has_vm = self.target.get("kind") == LOCAL_TARGET
        # A Kafka target's topic, read back after the SUT stopped; None when the file is absent.
        try:
            self.topic_text = (self.run_dir / topic.TOPIC_FILE).read_text()
        except OSError:
            self.topic_text = None

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
        for service in sorted(self.services):
            stdout = self.run_dir / "logs" / f"{service}.stdout"
            if service in self.logit_services and stdout.exists():
                self.telemetry[service] = telemetry.read_ndjson(stdout)
            stderr = self.run_dir / "logs" / f"{service}.stderr"
            if service in self.logit_services and stderr.exists():
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
            ready=apply.get("ready"), code=apply.get("code"), key=apply.get("key"),
        )

    def offset(self, t):
        return "?" if t is None or self.t0 is None else f"{t - self.t0:+.0f}s"

    def exit_windows(self, service="logit"):
        """[(start, end, fault)] of each `exit` step on `service`, its span widened by 1 s on
        each side, as a `stop`'s is for `exit`."""
        return [(f.start - 1, f.end + 1, f) for f in self.faults
                if f.action == "exit" and f.on == service]

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
        if not (is_start(record) or (event == "revert"
                                     and action in faults.RESUMES_ON_REVERT)):
            continue
        if record.get("on") not in data.logit_services or record.get("early"):
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
    """Every exit of a `logit` service falls inside a scheduled stop, restart, or kill, with the
    code that action leaves (0, or 137 for a kill), inside an `exit` step with the code the step
    names, or in the end sequence with 0; every `exit` step saw its exit; the final state is
    exited; and nothing was OOM-killed."""
    if not data.inspect:
        return Result("exit", SKIP, "no inspect/ files")
    lines = []
    status = PASS
    # {service: [(start, end, action, expected exit code, step)]}
    stop_windows = {}
    for record in data.timeline:
        action = record.get("action")
        if (record.get("event") == "apply" and action in faults.EXIT_CODES
                and not record.get("rc")):
            stop_windows.setdefault(record["on"], []).append(
                (record["started_at"] - 1, record["finished_at"] + 1, action,
                 faults.EXIT_CODES[action], record.get("step")))
    for service in data.logit_services:
        for start, end, fault in data.exit_windows(service):
            stop_windows.setdefault(service, []).append(
                (start, end, "exit", fault.code, fault.step))
    end_begin = data.phases.get("end_begin", {}).get("t")
    kills = 0
    exits = 0
    expected = 0
    seen_steps = set()
    for service in data.logit_services:
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
            window = None if t is None else next(
                (w for w in stop_windows.get(service, []) if w[0] <= t <= w[1]), None)
            if window is not None:
                _, _, action, want, step = window
            elif t is not None and end_begin is not None and t >= end_begin:
                action, want, step = "end sequence", 0, None
            else:
                status = FAIL
                lines.append(f"{service} exited at {data.offset(t)} with code {code}, outside "
                             "every scheduled stop")
                continue
            if code != want:
                status = FAIL
                lines.append(f"{service} exited at {data.offset(t)} with code {code}, inside a "
                             f"scheduled {action}, which leaves {want}")
                continue
            if action == "kill":
                kills += 1
            elif action == "exit":
                expected += 1
                seen_steps.add(step)
                lines.append(f"{service} exited at {data.offset(t)} with code {code}, inside "
                             f"expected exit {step}")
        if final.get("Status") != "exited":
            status = FAIL
            lines.append(f"{service} final state is {final.get('Status')}, not exited")
    for service in data.logit_services:
        for start, end, fault in data.exit_windows(service):
            if fault.step not in seen_steps:
                status = FAIL
                lines.append(f"expected exit did not happen: {service} has no exit with code "
                             f"{fault.code} in {fault.step}'s window [{data.offset(start)}, "
                             f"{data.offset(end)}]")
    for service in sorted(data.services):
        state = (data.inspect.get(service) or {}).get("State") or {}
        if state.get("OOMKilled"):
            status = FAIL
            lines.append(f"{service} was OOM-killed")
    if status == PASS:
        detail = f"{exits} exit(s) of the logit services, each a scheduled stop with code 0"
        if kills:
            detail += f" or a scheduled kill with code {faults.KILL_EXIT_CODE} ({kills} kill(s))"
        if expected:
            detail += (f" or an expected exit with its step's code ({expected}: "
                       + ", ".join(line for line in lines if "expected exit" in line) + ")")
    else:
        detail = next(line for line in lines if "inside expected exit" not in line)
    return Result("exit", status, detail, lines)


def check_restarts(data):
    if not data.watchdog:
        return Result("restarts", SKIP, "no watchdog.jsonl")
    starts = {}
    for record in data.timeline:
        if is_start(record):
            starts.setdefault(record["on"], []).append(
                (record["started_at"] - 1, record["finished_at"] + 2))
    lines = []
    status = PASS
    changes = 0
    for service in sorted(data.services):
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
    """FAILs a panic, a nonzero `exiting` line, or an ERROR line, except the sink fault lines
    inside a fault window, and, inside an `exit` step's window, the `exiting` line with the
    step's code and an ERROR line keyed with its `key`. Each `exit` step must show both, or the
    expected exit didn't happen as named. The `Error: ` text line `main` prints as it returns
    the failure, right after that `exiting` line, has no timestamp and is excused by its place.
    """
    if not data.stderr:
        return Result("self_log", SKIP, "no logs/*.stderr")
    windows = data.windows()
    status = PASS
    lines = []
    counts = {"error": 0, "fault_key_inside": 0, "fault_key_outside": 0, "text": 0,
              "expected": 0}
    # {step: {"exiting", "key"}} for each `exit` step: which of its two lines appeared.
    found = {}
    exit_windows = {service: data.exit_windows(service) for service in data.stderr}

    def expected_exit(service, entry, matches):
        """The `exit` step whose window holds `entry` and whose fields `matches` accepts."""
        if entry.ts is None:
            return None
        return next((fault for start, end, fault in exit_windows.get(service, [])
                     if start <= entry.ts <= end and matches(fault)), None)

    for service, entries in data.stderr.items():
        after_expected_exit = False
        for entry in entries:
            follows, after_expected_exit = after_expected_exit, False
            if follows and entry.kind == "text" and entry.raw.startswith("Error: "):
                counts["expected"] += 1
                continue
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
                if code in (0, "0", None):
                    continue
                fault = expected_exit(service, entry,
                                      lambda f, code=code: str(f.code) == str(code))
                if fault is not None:
                    counts["expected"] += 1
                    found.setdefault(fault.step, set()).add("exiting")
                    after_expected_exit = True
                    continue
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
                fault = expected_exit(service, entry, lambda f: entry.key and f.key == entry.key)
                if fault is not None:
                    counts["expected"] += 1
                    found.setdefault(fault.step, set()).add("key")
                    continue
                counts["error"] += 1
                status = FAIL
                lines.append(f"{service}: ERROR at {data.offset(entry.ts)}: "
                             f"key={entry.key or '-'} {entry.message[:160]}")
    for service, windows_of in exit_windows.items():
        for _, _, fault in windows_of:
            have = found.get(fault.step, set())
            missing = [label for what, label in (
                ("exiting", f"exiting line with code {fault.code}"),
                ("key", f"ERROR line keyed {fault.key}")) if what not in have]
            if missing:
                status = FAIL
                lines.insert(0, f"expected exit did not happen: {service} logged no "
                                f"{' and no '.join(missing)} in {fault.step}'s window")
    summary = (f"{counts['fault_key_inside']} sink fault line(s) inside fault windows, "
               f"{counts['fault_key_outside']} outside, {counts['expected']} expected-exit "
               f"line(s), {counts['error']} other ERROR, {counts['text']} non-JSON")
    return Result("self_log", status, summary if status == PASS else f"{lines[0]} ({summary})",
                  lines)


def readiness_word(output):
    """The status word in a `logit ready` output: `ok` on success, else the word before ` (` in
    `stalled (503 Service Unavailable)`; the first word of any other output, such as a failed
    connection's, or None for none."""
    words = (output or "").split()
    return words[0] if words else None


def health_samples(data):
    """[(t, service, word)] from each `logit` service's `health_log`: the image HEALTHCHECK's
    `logit ready`, timed at its `Start`, one per `Start`, since the watchdog records the newest
    entry every 5 s and the check runs every 10 s."""
    samples = {}
    for record in data.watchdog:
        entry = record.get("health_log")
        service = record.get("svc")
        if service not in data.logit_services or not isinstance(entry, dict):
            continue
        t = parse_docker_time(entry.get("Start"))
        if t is not None:
            samples[(service, entry.get("Start"))] = (t, service,
                                                      readiness_word(entry.get("Output")))
    return sorted(samples.values(), key=lambda sample: sample[0])


def check_ready(data):
    """Readiness, from the status word `logit ready` reports in each `health_log` sample: `ok` in
    every sample outside fault windows; at least one sample reading a `mark`'s `ready` word
    inside that mark; and `degraded`, which means a node has exited, only inside an `exit` step.
    A run whose watchdog recorded no `health_log` is judged on `Health.Status` instead. Every
    start or unpause of a `logit` service is ready within `READY_WITHIN_S`."""
    if not data.watchdog:
        return Result("ready", SKIP, "no watchdog.jsonl")
    status = PASS
    lines = []
    judged = 0
    samples = health_samples(data)
    marks = [f for f in data.faults if f.action == "mark" and f.ready]
    # One line per mark whose word was read, for the detail.
    seen = []
    if samples:
        for t, service, word in samples:
            if word == "degraded" and not data.in_window(
                    t, [(start, end) for start, end, _ in data.exit_windows(service)]):
                status = FAIL
                lines.append(f"{service} read degraded at {data.offset(t)}, outside every "
                             "exit step: a node exited that no step expected")
            elif data.steady(t):
                judged += 1
                if word != "ok":
                    status = FAIL
                    lines.append(f"{service} read {word} at {data.offset(t)}, outside every "
                                 "fault window")
        for mark in marks:
            hits = [t for t, service, word in samples
                    if service == mark.on and word == mark.ready and mark.start <= t <= mark.end]
            if hits:
                seen.append(f"{mark.step}: {mark.on} read {mark.ready} {len(hits)} time(s), "
                            f"first at {data.offset(hits[0])}")
            else:
                status = FAIL
                lines.insert(0, f"{mark.step}: {mark.on} never read {mark.ready} between "
                                f"{data.offset(mark.start)} and {data.offset(mark.end)}")
        source = f"{judged} readiness sample(s) read ok outside fault windows"
    else:
        for record in data.watchdog:
            if record.get("svc") not in data.logit_services or record.get("status") != "running":
                continue
            if not data.steady(record.get("t")):
                continue
            judged += 1
            if record.get("health") != "healthy":
                status = FAIL
                lines.append(f"{record['svc']} health {record.get('health')} at "
                             f"{data.offset(record['t'])}, outside every fault window")
        for mark in marks:
            status = FAIL
            lines.insert(0, f"{mark.step}: no health_log samples, so {mark.ready} can't be "
                            "judged")
        source = f"{judged} health sample(s) healthy outside fault windows (no health_log)"
    readies = [r for r in data.timeline if r.get("event") == "ready"]
    for record in readies:
        if not record.get("ok") or record.get("ready_s", 0) > READY_WITHIN_S:
            status = FAIL
            lines.append(f"{record['on']} after {record.get('why')} ({record.get('step')}): "
                         f"{'ready' if record.get('ok') else 'not ready'} after "
                         f"{record.get('ready_s')}s")
    slowest = max((r.get("ready_s", 0) for r in readies), default=None)
    detail = (f"{source}; {len(readies)} start(s) or unpause(s) ready in time"
              + (f", slowest ready {slowest:.1f}s" if slowest is not None else ""))
    if seen:
        detail += "; " + "; ".join(seen)
    return Result("ready", status, detail if status == PASS else lines[0], lines + seen)


def check_progress(data):
    """Every progress window outside fault windows has a delivery from every sink, each
    logit's stdout keeps its drain cadence, and each sink's newest stored sample stays fresh."""
    sut = data.telemetry.get("logit")
    if sut is None or data.t0 is None:
        return Result("progress", SKIP, "no SUT telemetry or no timeline start")
    window = float(data.thresholds.get("progress_window", 30))
    status = PASS
    lines = []
    judged = 0
    judged_after_warmup_s = 0.0
    stalled = {sink: 0 for sink in data.sinks}
    for start, end in data.quiet_intervals(data.t0 + window):
        t = start
        while t + window <= end:
            judged += 1
            if t >= data.t0 + data.warmup:
                judged_after_warmup_s += window
            for sink in data.sinks:
                delivered = sut.counter_in("logit.component.batches.delivered", t, t + window,
                                           component=sink)
                if delivered <= 0:
                    stalled[sink] += 1
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
        held = data.windows(on=service, actions=("stop", "kill", "pause", "restart", "exit"),
                            extend=False)
        for a, b in zip(drains, drains[1:]):
            if b - a > DRAIN_GAP_INTERVALS * interval and not any(
                s <= b and e >= a for s, e in held
            ):
                status = FAIL
                lines.append(f"{service} stdout went quiet for {b - a:.0f}s from "
                             f"{data.offset(a)} (internal interval {interval:.0f}s)")

    fresh_judged = {sink: 0 for sink in data.sinks}
    for record in data.freshness:
        t = record.get("t")
        sink = record.get("sink")
        if record.get("skipped") or t is None or t < data.t0 + window or not data.steady(t):
            continue
        fresh_judged[sink] = fresh_judged.get(sink, 0) + 1
        age = record.get("age_s")
        if record.get("error") or age is None or age >= FRESHNESS_MAX_AGE_S:
            status = FAIL
            why = (record.get("error") or ("no sample matches its vm_selector" if age is None
                                           else f"newest sample {age}s old"))
            lines.append(f"{sink} freshness on {record.get('backend')} at {data.offset(t)}: "
                         f"{why}")
    share, coverage = data.coverage(judged_after_warmup_s)
    if share is not None and share < COVERAGE_MIN_SHARE:
        status = worst([status, WARN])
        lines.append(f"delivery windows cover little of the run: {coverage}")
    per_sink = []
    for sink in data.sinks:
        fresh = ("no freshness samples" if not data.has_vm else
                 f"{fresh_judged.get(sink, 0)} freshness sample(s)")
        per_sink.append(f"{sink}: deliveries in {judged - stalled[sink]} of {judged}, {fresh}")
    lines += per_sink
    backend = (f" ({no_vm_reason(data)})" if not data.has_vm else
               f", each freshness sample under {FRESHNESS_MAX_AGE_S}s")
    detail = (f"{judged} {window:.0f}s window(s) outside fault windows, judged per sink"
              f"{backend}: {'; '.join(per_sink)}; {coverage}")
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


def judge_services(data, check_id, judged):
    """`(status, lines)` for a row that judges each `logit` service apart, from `judged`,
    `{service: (status, text)}`, and the scenario's `[expected]` verdicts for `check_id`. A
    service with an expectation scores PASS when its own status equals it, saying so, and FAIL
    when it doesn't; a service with none keeps its own status. A service the row couldn't judge
    has status SKIP, which matches no expectation; a row with every service SKIP, or none, SKIPs."""
    expected = (data.resolved.get("expected") or {}).get(check_id) or {}
    statuses, lines = [], []
    for service, (status, text) in sorted(judged.items()):
        want = expected.get(service)
        if want is None:
            statuses.append(status)
            lines.append(f"{service} {status}: {text}")
        elif want == status:
            statuses.append(PASS)
            lines.append(f"{service} {status}, as expected: {text}")
        else:
            statuses.append(FAIL)
            lines.append(f"{service}: expected {want}, got {status}: {text}")
    for service in sorted(set(expected) - set(judged)):
        statuses.append(FAIL)
        lines.append(f"{service}: expected {expected[service]}, got no telemetry")
    if all(status == SKIP for status in statuses):
        return SKIP, lines
    return worst([status for status in statuses if status != SKIP]), lines


def check_rss_slope(data):
    """Per `logit` service: the least-squares RSS slope of each life over steady samples after
    warmup, against `rss_growth_mib_per_hour`, a growth WARNing under an hour's run and FAILing
    from one; WARN when the judged slopes cover under `COVERAGE_MIN_SHARE` of the run. Each
    service is judged apart and `[expected]` can set its verdict (`judge_services`)."""
    limit = float(data.thresholds.get("rss_growth_mib_per_hour", 64))
    over = WARN if data.duration < SECONDS_PER_HOUR else FAIL
    judged = {}
    extra = []
    fitted = False
    for service in data.logit_services:
        by_life, source = _rss_points(data, service)
        if not by_life:
            judged[service] = (SKIP, "no steady-state RSS samples")
            continue
        status = PASS
        slopes = []
        judged_s = 0.0
        for life, points in sorted(by_life.items()):
            span = points[-1][0] - points[0][0] if points else 0
            if len(points) < SLOPE_MIN_SAMPLES or span < SLOPE_MIN_SPAN_S:
                extra.append(f"{service} life {life}: {len(points)} sample(s) over {span:.0f}s, "
                             "too few to judge")
                continue
            judged_s += data.steady_span(points)
            per_hour = slope(points) * SECONDS_PER_HOUR / MIB
            slopes.append(f"life {life} {per_hour:+.1f}")
            extra.append(f"{service} life {life}: {per_hour:+.1f} MiB/h over {len(points)} "
                         f"samples, {span:.0f}s ({source})")
            if per_hour > limit:
                status = worst([status, over])
        fitted = fitted or bool(slopes)
        # A service with samples but no life long enough to fit judges 0 seconds, which the
        # coverage rule WARNs, so it can't read as a pass beside another service's good slope.
        share, text = data.coverage(judged_s)
        if share is not None and share < COVERAGE_MIN_SHARE:
            status = worst([status, WARN])
            extra.append(f"{service}: slopes cover little of the run: {text}")
        fits = f"MiB/h {', '.join(slopes)}" if slopes else "no life with enough samples"
        judged[service] = (status, f"{fits} (limit {limit:g}); {text}")
    expected = (data.resolved.get("expected") or {}).get("rss_slope")
    status, lines = judge_services(data, "rss_slope", judged)
    if status == SKIP or not (fitted or expected):
        return Result("rss_slope", SKIP, "no life with enough steady-state samples",
                      lines + extra)
    return Result("rss_slope", status, " | ".join(lines), lines + extra)


def check_fd_slope(data):
    """Per `logit` service: open fds at the end, and at the first steady sample after each
    fault's recovery, at most `fd_growth` over the warmup median. Each service is judged apart
    and `[expected]` can set its verdict (`judge_services`)."""
    growth = float(data.thresholds.get("fd_growth", 8))
    judged = {}
    extra = []
    for service in data.logit_services:
        tel = data.telemetry.get(service)
        if data.t0 is None:
            judged[service] = (SKIP, "no timeline start")
            continue
        if tel is None:
            judged[service] = (SKIP, "no fd samples")
            continue
        series = [(ts, v) for ts, v, _ in tel.gauge_series("logit.process.fds")
                  if ts < data.end]
        warm = [v for ts, v in series if data.t0 <= ts < data.t0 + data.warmup]
        if not warm or not series:
            judged[service] = (SKIP, "no warmup samples")
            continue
        baseline = statistics.median(warm)
        end_value = series[-1][1]
        status = PASS
        problems = []
        if end_value - baseline > growth:
            status = FAIL
            problems.append(f"{end_value:g} fds at the end, {baseline:g} in the warmup")
        for fault in data.faults:
            after = fault.end + data.recovery_bound
            sample = next((v for ts, v in series if ts >= after and data.steady(ts)), None)
            if sample is not None and sample - baseline > growth:
                status = FAIL
                problems.append(f"{sample:g} fds {data.recovery_bound:.0f}s after {fault.step} "
                                f"({fault.action} on {fault.on}), baseline {baseline:g}")
        text = f"warmup median {baseline:g} -> end {end_value:g} (limit +{growth:g})"
        judged[service] = (status, "; ".join([text] + problems))
    status, lines = judge_services(data, "fd_slope", judged)
    if status == SKIP:
        return Result("fd_slope", SKIP, "no fd samples" if data.t0 is not None
                      else "no timeline start", lines + extra)
    return Result("fd_slope", status, " | ".join(lines), lines + extra)


# ---- the ledger, the sink identity, and recovery --------------------------------------------------
#
# One unit runs through the ledger: a datagram is one line is one event is one increment. The
# plan's "The ledger and identities" has the symbol table; `Ledger` computes each symbol per SUT
# process life. Each `[[ledger.source]]` is a generator and the SUT listener its datagrams reach:
# the wire is judged per source, the intake per listener, and from the edge into `aggregate` on
# the listeners' symbols are summed.

# The receive queue's and a batch's defaults (`receive.max_datagrams`, `receive.batch_max_events`),
# used when the SUT config doesn't set them.
DEFAULT_MAX_DATAGRAMS = 10000
DEFAULT_BATCH_MAX_EVENTS = 1000
# Batches of at most `batch_max_events` that can be in flight past one listener's receive queue
# at a shutdown signal: its accumulator and the batch it holds while its send waits on a full
# inbox.
LISTENER_RESIDUAL_BATCHES = 2
# What `aggregate` adds, once whatever the listeners: its 64-slot inbox and the batch it has taken
# but not yet absorbed. Each node between a listener and `aggregate`, a `lua` script or a
# source's `sut_transforms`, adds the same: its own inbox and the batch it has taken off it.
NODE_RESIDUAL_BATCHES = 65
# The UDP listener's `warn` lines for datagrams it counted as `datagrams.dropped{reason=
# "shutdown"}` (`crates/logit-inputs/src/udp.rs`: `ReadHalf`, `ResidualOnDrop`, and `Undecoded`).
# Telemetry carries the same count, so these only cross-check it.
SHUTDOWN_DROP_RE = re.compile(
    r"^(\d+) datagram\(s\) (?:still in the receive queue|read off the socket but never queued"
    r"|taken off the receive queue but not decoded)")
# `recovery`'s limits for the gauges and the ingest rate, against the warmup baseline.
RECOVERED_UTILIZATION = 0.05
RECOVERED_RATE_SHARE = 0.95
# Why a row that reads V SKIPs on an external target.
NO_BACKEND = "external target: no backend query"
NO_VM_KAFKA = "kafka target: no VictoriaMetrics"


def no_vm_reason(data):
    return NO_VM_KAFKA if data.kafka else NO_BACKEND


# `ledger.sent`'s reading of an external sink's telemetry (`datadog_out`'s module doc, "Faults,
# retries, and duplicate safety" and "Telemetry"): the `records.dropped` reasons and the batch
# drop reason that mean the destination refused the data or it was too large to send, the
# diagnostics quoting its answer to a dropped request (or the pre-send `oversize` drop), and the
# diagnostics for a refusal the sink holds its queue on. Every other counted drop WARNs.
# `kafka_out` adds the records a broker named (`invalid_timestamp`, `invalid_record`) and keys its
# diagnostics by the drop reason (its sink module doc, "Telemetry").
REJECTED_RECORD_REASONS = ("rejected", "oversize", "invalid_timestamp", "invalid_record")
REJECTED_BATCH_REASONS = ("rejected",)
REJECTION_KEYS = ("request_rejected", "series_rejected", "oversize", "rejected",
                  "invalid_timestamp", "invalid_record")
REFUSAL_KEYS = ("api_key_rejected", "request_refused")
# `kafka_out`'s `records.dropped` reasons for a partition batch settled without being appended,
# each of which bumps the producer epoch (`crates/logit-outputs/src/kafka/producer.rs`'s module
# doc). The broker's duplicate detection doesn't span an epoch bump, so a life that counted one
# may store a record twice (docs/known-gaps/kafka.md). `oversize` drops a record before it gets a
# sequence, so it bumps nothing. The `MESSAGE_TOO_LARGE` split and `OUT_OF_ORDER_SEQUENCE_NUMBER`
# bump the epoch with no drop counted, so a duplicate they cause FAILs `ledger.topic` and needs
# reading by hand.
EPOCH_BUMP_REASONS = ("rejected", "invalid_timestamp", "invalid_record", "ambiguous_at_most_once")
# The `batches.dropped` reasons that still count an egress gap but FAIL `ledger.egress` and
# `ledger.summary` wherever they appear, each with what it says went wrong: against a healthy
# backend a sink has nothing rejected and a spool reads back nothing corrupt.
FAULT_DROP_REASONS = {"rejected": "rejected by the destination",
                      "disk_corrupt": "dropped from a corrupt disk spool"}
_ANSWERED_RE = re.compile(r"answered (\d{3})")


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
    """The first `key: <int>` in a YAML text, block or flow style, or None. `component_int`
    passes it one component's block."""
    match = re.search(rf"\b{key}\s*:\s*[\"']?(\d+)", text or "")
    return int(match.group(1)) if match else None


def _component_blocks(text, component):
    """Each `<component>:` line's text in a YAML config, block or flow style: the rest of the
    line and every deeper-indented line after it."""
    lines = (text or "").splitlines()
    for index, line in enumerate(lines):
        match = re.match(rf"^(\s*){re.escape(component)}\s*:(.*)$", line)
        if not match:
            continue
        indent = len(match.group(1))
        block = [match.group(2)]
        for following in lines[index + 1:]:
            if following.strip() and len(following) - len(following.lstrip()) <= indent:
                break
            block.append(following)
        yield "\n".join(block)


def component_int(text, component, key):
    """An integer field of one component in a YAML text, such as a listener's
    `receive.max_datagrams`, or None."""
    for block in _component_blocks(text, component):
        value = _yaml_int(block, key)
        if value is not None:
            return value
    return None


def component_duration(text, component, key):
    """A duration field of one component in a YAML text, block or flow style, in seconds, or
    None."""
    for block in _component_blocks(text, component):
        found = re.search(rf"\b{key}\s*:\s*[\"']?([0-9][0-9a-z.]*)", block)
        if found:
            try:
                return parse_duration(found.group(1))
            except ValueError:
                return None
    return None


# The listener symbols the ledger keeps per listener, each also summed over the listeners.
LISTENER_SYMBOLS = ("W", "K", "D_tel", "D_shutdown", "B", "E", "X")


def _summed(by_listener):
    """`{life: value}` summed over `{listener: {life: value}}`."""
    total = {}
    for lives in by_listener.values():
        for life, value in lives.items():
            total[life] = total.get(life, 0) + value
    return total


class Ledger:
    """The ledger's symbols per SUT process life, read once from a run's telemetry, stderr, and
    VictoriaMetrics exports. Lives are the SUT's; each generator's G is one number for the run.
    The listener symbols are kept per listener (`per[listener][symbol]`) and summed under their
    own names, which every row from the edge into `aggregate` on reads. V is per sink:
    `V[sink]`, each from its own export."""

    def __init__(self, data):
        cfg = data.ledger
        self.data = data
        # Each `[[ledger.source]]`: a generator service and the SUT listener it reaches.
        self.sources = [dict(source) for source in cfg.get("source") or []]
        self.listeners = [source.get("sut_listener") for source in self.sources]
        self.aggregate = cfg.get("sut_aggregate")
        self.sinks = list(data.sinks)
        self.split = data.split
        # Under `route`, the `target` feeding each sink.
        self.targets = {sink["id"]: sink.get("target") for sink in cfg.get("sink") or []}
        # The `lua` component between the listener and `aggregate`, or None.
        self.script = cfg.get("sut_script")
        # How the egress rows write the increments `aggregate` absorbed from the listener's
        # events: with a script, its `flush()` events are absorbed too, and V excludes them.
        self.ab_text = "Ab − L_flush" if self.script else "Ab"
        self.residual_text = ("W − D − B − X − (L_drop + L_err) − (Ab − L_flush)" if self.script
                              else "W − D − B − X − Ab")
        self.sut = data.telemetry.get("logit")
        # `{service: telemetry}` for each source's generator that wrote any.
        self.gens = {source.get("service"): data.telemetry[source.get("service")]
                     for source in self.sources if source.get("service") in data.telemetry}
        # Reasons every row that reads the SUT's telemetry SKIPs.
        self.problems = []
        self.killed = []
        if self.sut is None:
            self.problems.append("no SUT telemetry")
            self.lives = []
            return
        if not self.sut.drains:
            self.problems.append("SUT telemetry has no logit.process.uptime points")
            self.lives = []
            return
        self.life_starts = self.sut.life_starts
        self.lives = list(range(len(self.life_starts))) or [0]
        self.final = self.lives[-1]
        # Each start of the SUT container begins a life: a stop's or a kill's revert, an
        # end-sequence early one included, and a restart's apply. An unpause doesn't. A life
        # ended by a kill writes no shutdown drain, so its last drain is an ordinary one, and the
        # next life's first uptime is lower. A life under `internal`'s
        # interval leaves at most its shutdown uptime point, so the next life's first uptime isn't
        # lower and the two merge; the ledger isn't judged over merged windows.
        expected = 1 + sum(
            1 for record in data.timeline
            if record.get("on") == "logit" and not record.get("rc") and is_start(record))
        if len(self.lives) != expected:
            self.problems.append(
                f"SUT telemetry shows {len(self.lives)} life/lives where logit.process.uptime "
                f"decreases, but the timeline starts the SUT into {expected}")

        sut = self.sut
        self.per = {}
        for listener in self.listeners:
            one = self.per[listener] = {}
            one["W"] = sut.counter_by_life("logit.input.datagrams", component=listener)
            one["K"] = sut.counter_by_life("logit.input.kernel.drops", component=listener)
            # D, every reason. Every SUT config gives `internal` a dedicated pipeline, so its
            # final drain runs after every data node has exited and carries the shutdown drops.
            one["D_tel"] = sut.counter_by_life("logit.component.datagrams.dropped",
                                               component=listener)
            one["D_shutdown"] = sut.counter_by_life("logit.component.datagrams.dropped",
                                                    component=listener, reason="shutdown")
            # B: a statsd line the decoder refused, and a line over `lines_in`'s
            # `max_line_bytes`, which its decoder drops and counts without a decoder error. Each
            # is 0 on a listener that has no such rule.
            one["B"] = _summed({
                "bad_line": sut.counter_by_life("logit.component.diagnostics",
                                                component=listener, key="bad_line"),
                "oversize": sut.counter_by_life("logit.input.frames.dropped",
                                                component=listener, reason="oversize")})
            one["E"] = sut.counter_by_life("logit.component.events.sent", component=listener)
            # Events the listener counted as sent and then dropped, every reason: a batch sent
            # into the inbox of a node that has failed is `closed_consumer`.
            one["X"] = sut.counter_by_life("logit.component.events.dropped", component=listener)
        for symbol in LISTENER_SYMBOLS:
            setattr(self, symbol, _summed({listener: one[symbol]
                                           for listener, one in self.per.items()}))
        # Batches any data node dropped for shutdown, the sink's and a Lua node's alike, which
        # `drain complete`'s `batches_dropped` cross-checks.
        self.shutdown_batches = sut.counter_by_life("logit.component.batches.dropped",
                                                    reason="shutdown")
        # `{life: {(component, reason): batches}}` under every reason, which `drop_credit` reads.
        # The reason set is open: a reason this file doesn't name still counts a gap.
        self.dropped_batches = {}
        for point in sut.matching("logit.component.batches.dropped"):
            if isinstance(point.value, (int, float)) and point.value:
                key = (point.attributes.get("component"), point.attributes.get("reason"))
                life = self.dropped_batches.setdefault(point.life, {})
                life[key] = life.get(key, 0) + point.value
        self.A = sut.counter_by_life("logit.component.events.received",
                                     component=self.aggregate)
        self.Ab = sut.counter_by_life("logit.transform.metrics.absorbed",
                                      component=self.aggregate)
        if self.script:
            script = self.script
            self.L_in = sut.counter_by_life("logit.component.events.received", component=script)
            # Counts events: an `emit_many` outcome adds one per event it returns.
            self.L_emit = sut.counter_by_life("logit.script.events.emitted", component=script)
            self.L_drop = sut.counter_by_life("logit.component.events.dropped", component=script,
                                              reason="script_drop")
            self.L_err = sut.counter_by_life("logit.component.errors", component=script,
                                             reason="process")
            self.L_flush = sut.counter_by_life("logit.component.flush.events", component=script)
            self.L_sent = sut.counter_by_life("logit.component.events.sent", component=script)

        # The lives a scheduled `kill` ended, each the life in force when the kill was applied,
        # with the time it was.
        self.killed_at = {
            vm.life_at(self.life_starts, record["started_at"]): record["started_at"]
            for record in data.timeline
            if record.get("event") == "apply" and record.get("action") == "kill"
            and record.get("on") == "logit" and not record.get("rc")}
        self.killed = sorted(self.killed_at)

        # The stderr copies of the shutdown counts, each credited to the life its timestamp falls
        # in, read only to cross-check telemetry (`datagram_mismatch`, `batch_mismatch`). `drain
        # complete` also counts the `internal` pipeline's own shutdown drops, which no drain can
        # export. A killed life writes neither.
        self.D_log = {}
        self.D_log_per = {listener: {} for listener in self.listeners}
        self.batches_dropped = {}
        self.drain_completes = {}
        for entry in data.stderr.get("logit", []):
            if entry.kind != "json" or entry.ts is None:
                continue
            life = vm.life_at(self.life_starts, entry.ts)
            match = SHUTDOWN_DROP_RE.match(entry.message)
            if match and life in self.killed:
                continue
            if match and entry.component in self.D_log_per:
                logged = self.D_log_per[entry.component]
                logged[life] = logged.get(life, 0) + int(match.group(1))
                self.D_log[life] = self.D_log.get(life, 0) + int(match.group(1))
            elif entry.message == "drain complete":
                self.drain_completes[life] = self.drain_completes.get(life, 0) + 1
                self.batches_dropped[life] = (self.batches_dropped.get(life, 0)
                                              + _int(entry.fields.get("batches_dropped")))

        # Per sink: its series, V per life, the reset-aware total, the resets per series, and
        # the series whose segments don't line up with the lives.
        self.series = data.vm_series
        self.V, self.V_total, self.resets, self.mismatched = {}, {}, {}, {}
        for sink in self.sinks:
            (self.V[sink], self.V_total[sink], self.resets[sink],
             self.mismatched[sink]) = vm.totals_by_life(self.series.get(sink, []),
                                                        self.life_starts)
        # A Kafka target's one sink has the topic for its V: the distinct records' increments,
        # credited to lives by record timestamp, with each extra copy credited the same way.
        self.records = []
        self.unparsed = 0
        self.duplicates = {}
        self.kafka_sink = self.sinks[0] if data.kafka and self.sinks else None
        if self.kafka_sink is not None:
            sink = self.kafka_sink
            self.records, self.unparsed, copies = topic.parse_records(data.topic_text or "")
            self.V[sink], self.V_total[sink] = topic.totals_by_life(self.records,
                                                                    self.life_starts)
            self.duplicates = topic.duplicates_by_life(copies, self.life_starts)
            self.resets[sink], self.mismatched[sink] = {}, []
        # Per sink, what `edge` judges: the events it received, and under `route` the events
        # its target sent; and the events `aggregate` sent, once per send whatever its fan-out.
        self.S_in = {sink: sut.counter_by_life("logit.component.events.received",
                                               component=sink) for sink in self.sinks}
        self.T_sent = {sink: sut.counter_by_life("logit.component.events.sent",
                                                 component=self.targets.get(sink))
                       for sink in self.sinks if self.targets.get(sink)}
        self.A_sent = sut.counter_by_life("logit.component.events.sent",
                                          component=self.aggregate)

        # Each source's `sut_transforms`, and every one's events received and sent, which
        # `ledger.edge` judges hop by hop.
        self.transforms = {source.get("sut_listener"): list(source.get("sut_transforms") or [])
                           for source in self.sources}
        self.T_in, self.T_out = {}, {}
        for chain in self.transforms.values():
            for component in chain:
                self.T_in[component] = sut.counter_by_life("logit.component.events.received",
                                                           component=component)
                self.T_out[component] = sut.counter_by_life("logit.component.events.sent",
                                                            component=component)

        # Per source, from its generator's telemetry: G and its datagram precondition, the
        # sink's counted `drop_newest` loss, and the `generate_in` batch size.
        self.gen_stats = {}
        for source in self.sources:
            service = source.get("service")
            gen = self.gens.get(service)
            if gen is None:
                continue

            def total(name, component, gen=gen):
                return sum(gen.counter_by_life(name, component=component).values())

            sink, load = source.get("generator_sink"), source.get("generator_input")
            sent = total("logit.component.events.sent", load)
            batches = total("logit.component.batches.sent", load)
            self.gen_stats[service] = {
                "G": total("logit.output.messages", sink),
                "G_datagrams": total("logit.output.datagrams", sink),
                "dropped_events": total("logit.component.events.dropped", sink),
                "dropped_batches": total("logit.component.batches.dropped", sink),
                "batch": sent / batches if batches else 0.0,
            }
        self.G = sum(stats["G"] for stats in self.gen_stats.values())

        # R: per listener its receive queue and `LISTENER_RESIDUAL_BATCHES`, plus
        # `NODE_RESIDUAL_BATCHES` for each node between it and `aggregate`; and `aggregate`'s own
        # `NODE_RESIDUAL_BATCHES`, plus a script's, in batches of the largest `batch_max_events`.
        text = self._sut_config_text()
        self.R, parts, largest = 0, [], 0
        for listener in self.listeners:
            max_datagrams = (component_int(text, listener, "max_datagrams")
                             or DEFAULT_MAX_DATAGRAMS)
            batch_max = (component_int(text, listener, "batch_max_events")
                         or DEFAULT_BATCH_MAX_EVENTS)
            largest = max(largest, batch_max)
            batches = (LISTENER_RESIDUAL_BATCHES
                       + NODE_RESIDUAL_BATCHES * len(self.transforms.get(listener, [])))
            self.R += max_datagrams + batches * batch_max
            parts.append(f"{_n(max_datagrams)} + {batches} x {_n(batch_max)}"
                         + (f" ({listener})" if len(self.listeners) > 1 else ""))
        shared = NODE_RESIDUAL_BATCHES * (2 if self.script else 1)
        self.R += shared * largest
        parts.append(f"{shared} x {_n(largest)}")
        self.R_text = f"R {_n(self.R)} = " + " + ".join(parts)
        # The aggregate's configured window, in seconds, or None when the config doesn't say.
        self.interval = component_duration(text, self.aggregate or "", "interval")

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

    def lget(self, listener, symbol, life):
        """A listener symbol for one listener in `life`."""
        return self.per[listener][symbol].get(life, 0)

    def into_aggregate(self, life):
        """`{listener: events}` each source sent toward `aggregate` in `life`: its last
        transform's events.sent, or with none the listener's E."""
        out = {}
        for listener in self.listeners:
            chain = self.transforms.get(listener)
            out[listener] = (self.T_out[chain[-1]].get(life, 0) if chain
                             else self.lget(listener, "E", life))
        return out

    def epoch_bumps(self, life):
        """`{reason: records}` for the Kafka sink's `EPOCH_BUMP_REASONS` drops in one life."""
        bumps = {}
        for reason in EPOCH_BUMP_REASONS:
            value = self.sut.counter_by_life("logit.output.records.dropped",
                                             component=self.kafka_sink,
                                             reason=reason).get(life, 0)
            if value:
                bumps[reason] = value
        return bumps

    def v(self, life, sinks):
        """V in `life` summed over `sinks`."""
        return sum(self.V.get(sink, {}).get(life, 0) for sink in sinks)

    def egress_groups(self):
        """`[(label, sinks)]`, each judged as one egress: under `copy` each sink alone, under
        `route` all of them, whose V add up to what `aggregate` absorbed."""
        if self.split == "route":
            return [(" + ".join(self.sinks), list(self.sinks))]
        return [(sink, [sink]) for sink in self.sinks]

    def v_text(self, life, sinks):
        if len(sinks) == 1:
            return f"V[{sinks[0]}] {_n(self.v(life, sinks))}"
        parts = " + ".join(f"V[{sink}] {_n(self.v(life, [sink]))}" for sink in sinks)
        return f"ΣV {_n(self.v(life, sinks))} ({parts})"

    def D(self, life):
        return self.get("D_tel", life)

    def datagram_mismatch(self, life):
        """A detail when stderr's shutdown `warn` lines count more datagrams than telemetry's
        `datagrams.dropped{reason="shutdown"}` for a life, else None. A killed life has neither.

        Only stderr above telemetry is a disagreement. Under `receive: { overflow: block }`, a
        `push_many` cancelled while blocked counts its datagrams as shutdown drops without
        logging a line, so telemetry can exceed stderr on a correct ledger."""
        found = []
        for listener in self.listeners:
            logged = self.D_log_per[listener].get(life, 0)
            counted = self.lget(listener, "D_shutdown", life)
            if logged > counted:
                found.append(f"life {life}: {listener}'s stderr shutdown lines count "
                             f"{_n(logged)} datagram(s), telemetry's datagrams.dropped"
                             f"{{reason=\"shutdown\"}} {_n(counted)}; D reads telemetry")
        return "; ".join(found) or None

    def batch_mismatch(self, life):
        """A detail when `drain complete`'s `batches_dropped` and telemetry's
        `batches.dropped{reason="shutdown"}` disagree for a life, else None. A killed life has
        neither."""
        if life in self.killed and life != self.final:
            return None
        logged, counted = self.batches_dropped.get(life, 0), self.get("shutdown_batches", life)
        if logged == counted:
            return None
        return (f"life {life}: drain complete's batches_dropped {_n(logged)}, telemetry's "
                f"batches.dropped{{reason=\"shutdown\"}} {_n(counted)}; a larger stderr count "
                "is the internal pipeline's own drops, or a config whose internal pipeline isn't "
                "dedicated")

    def symbols(self, life):
        script = ""
        if self.script:
            script = "".join(f"{symbol} {_n(self.get(symbol, life))}, " for symbol in (
                "L_in", "L_emit", "L_drop", "L_err", "L_flush", "L_sent"))
        text = (f"W {_n(self.get('W', life))}, K {_n(self.get('K', life))}, D {_n(self.D(life))} "
                f"({_n(self.get('D_shutdown', life))} at shutdown; stderr says "
                f"{_n(self.get('D_log', life))}), B {_n(self.get('B', life))}, "
                f"E {_n(self.get('E', life))}, X {_n(self.get('X', life))}, {script}"
                f"A {_n(self.get('A', life))}, Ab {_n(self.get('Ab', life))}, "
                + ", ".join(f"V[{sink}] {_n(self.v(life, [sink]))}" for sink in self.sinks))
        if len(self.listeners) > 1:
            text += "; " + "; ".join(
                f"{listener}: " + ", ".join(f"{name} {_n(self.lget(listener, symbol, life))}"
                                            for name, symbol in (("W", "W"), ("K", "K"),
                                                                 ("D", "D_tel"), ("B", "B"),
                                                                 ("E", "E"), ("X", "X")))
                for listener in self.listeners)
        return text

    def absorbed(self, life):
        """The increments `aggregate` absorbed from the listener's events: Ab, less a script's
        `flush()` events, each of which carries one metric whose series V excludes."""
        return self.get("Ab", life) - (self.get("L_flush", life) if self.script else 0)

    def script_dropped(self, life):
        """The events a script dropped (`script_drop`) or failed on (`errors{process}`), both
        counted, 0 with no script."""
        if not self.script:
            return 0
        return self.get("L_drop", life) + self.get("L_err", life)

    def residual(self, life):
        """What a life read but neither absorbed nor dropped, counted, by its last drain:
        W − D − B − X − Ab, and with a script W − D − B − X − (L_drop + L_err) − (Ab − L_flush)."""
        return (self.get("W", life) - self.D(life) - self.get("B", life) - self.get("X", life)
                - self.script_dropped(life) - self.absorbed(life))

    def kill_band(self, life):
        """`(low, high, text)`: the Ab − V a killed life may show with nothing lost, from its
        ingest rate over its last drain interval.

        Below: the residual at the last drain and the reads after it, up to one drain interval,
        are in no Ab, and an `aggregate` flush before the kill can carry them into V. Above: the
        kill discards `aggregate`'s unflushed window, whose events Ab counts and V never holds,
        up to one aggregate interval of ingest, plus one drain interval of queue growth after
        the last drain. The residual itself is reported beside the band and never judged with
        it: `ledger.intake` bounds it."""
        drains = self.sut.life_drains(life)
        gaps = [b - a for a, b in zip(drains, drains[1:])]
        interval = statistics.median(gaps) if gaps else 0.0
        last_w = sum(v for listener in self.listeners
                     for ts, v, point_life in self.sut.counter_points(
                         "logit.input.datagrams", component=listener)
                     if point_life == life and drains and ts == drains[-1])
        rate = last_w / gaps[-1] if gaps and gaps[-1] > 0 else 0.0
        window = self.interval
        source = "configured"
        if window is None:
            spaced = next((one for one in self.series.values() if one), [])
            window = vm.sample_spacing_s(spaced) or 0.0
            source = "the export's sample spacing, the config naming none"
        residual = max(0, self.residual(life))
        low = -(residual + round(rate * interval))
        high = round(rate * window) + round(rate * interval)
        text = (f"band [{_n(low)}, {_n(high)}]: ingest {rate:,.0f}/s over the last drain "
                f"interval; below, the residual {_n(residual)} plus one {interval:g}s drain "
                f"interval of ingest, absorbed after the last drain and flushed before the "
                f"kill; above, one {window:g}s aggregate window ({source}) unflushed at the "
                f"kill plus one drain interval of queue growth after the last drain")
        return low, high, text

    def egress(self, life, sinks):
        """`(status, verdict, gap)` for one life's egress to `sinks`, whose V is summed (one
        sink under `copy`, all under `route`): `ok`, `counted`, `within band`, `surplus`, or
        `uncounted`.

        A positive gap is counted when the life's telemetry has a batch dropped, for any reason,
        at a node that can explain it (`drop_credit`). Whatever the gap, a `FAULT_DROP_REASONS`
        drop in that credit FAILs the life (`fault_drops`), its verdict unchanged.

        The final life's gap is Ab − V, and with a script Ab − L_flush − V, here and below. A
        stopped earlier life's is W − D − B − X − V, less a script's counted drops, the residual
        plus Ab − V, because `aggregate`'s close-time flush delivers what that life read but
        hadn't absorbed by its last ordinary drain into V; the final drain runs after that
        flush, so it exports the shutdown drops and the script's counted drops and errors. A
        killed life has no close-time window and no shutdown drops: its gap is Ab − V alone,
        judged inside `kill_band`; below the band it is a surplus, above it uncounted loss. Its
        residual is what the kill lost from the receive queue and inbox, by design, and is
        reported, not judged here.

        On a Kafka target, V counts each distinct record once, so a negative gap in a life that
        wasn't killed is a `surplus`, never a duplicate: increments V holds and Ab doesn't."""
        status, verdict, gap = self._egress(life, sinks)
        if self.fault_drops(life, sinks):
            status = FAIL
        return status, verdict, gap

    def _egress(self, life, sinks):
        gap = self.absorbed(life) - self.v(life, sinks)
        if life in self.killed and life != self.final:
            low, high, _ = self.kill_band(life)
            if gap == 0:
                return PASS, "ok", gap
            if low <= gap <= high:
                return PASS, "within band", gap
            return FAIL, ("surplus" if gap < low else "uncounted"), gap
        if life != self.final:
            gap += self.residual(life)
        if gap == 0:
            return PASS, "ok", gap
        if gap > 0 and sum(self.drop_credit(life, sinks).values()) > 0:
            return PASS, "counted", gap
        if gap < 0 and self.data.kafka:
            return FAIL, "surplus", gap
        return FAIL, "uncounted", gap

    def _drops(self, life, keep):
        """`{reason: batches}` dropped in `life` at the components `keep` accepts."""
        drops = {}
        for (component, reason), batches in self.dropped_batches.get(life, {}).items():
            if keep(component):
                drops[reason] = drops.get(reason, 0) + batches
        return drops

    def other_sinks_drops(self, life, sinks):
        """`{reason: batches}` the sinks outside `sinks` dropped in `life`."""
        others = set(self.sinks) - set(sinks)
        return self._drops(life, lambda component: component in others)

    def drop_credit(self, life, sinks):
        """`{reason: batches}` dropped in `life` that can explain a gap at `sinks`: every node's,
        less those of the sinks outside the group. Under `copy` another sink's drop loses
        nothing this sink was sent, while a drop upstream of the fan-out (the script,
        `aggregate`) or at this sink can; under `route`, and with one sink, the group holds
        every sink and the count is the life's whole."""
        others = set(self.sinks) - set(sinks)
        return self._drops(life, lambda component: component not in others)

    def fault_drops(self, life, sinks):
        """The `FAULT_DROP_REASONS` drops in `life`'s `drop_credit`, as text, or ""."""
        credit = self.drop_credit(life, sinks)
        return ", ".join(f"{_n(credit[reason])} batch(es) {what}"
                         for reason, what in FAULT_DROP_REASONS.items() if credit.get(reason))


def _reasons(drops):
    """`{reason: batches}` as `overflow_oldest 12, shutdown 2`, or `0`."""
    return ", ".join(f"{reason} {_n(n)}" for reason, n in sorted(drops.items())) or "0"


def _ledger(data):
    if not hasattr(data, "_ledger"):
        data._ledger = Ledger(data)
    return data._ledger


def _skip_without_sut(check_id, led):
    if led.problems:
        return Result(check_id, SKIP, "; ".join(led.problems))
    return None


def affects_source(data, fault, service):
    """Whether `fault` can lose UDP on the wire of the source whose generator is `service`: a
    UDP-affecting fault on the SUT or anywhere off the generators affects every source, and one
    on a generator affects that generator's source alone."""
    if not fault.affects_udp_ingress:
        return False
    return data.services.get(fault.on) != GENERATOR_ROLE or fault.on == service


def _wire_buckets(data, led, source):
    """G − (W + K) for one source per SUT drain interval, classified `fault`, `steady`, or
    `end`: its generator's G against its listener's W + K, with the fault windows that can
    affect its wire (`affects_source`).

    G is cumulative at each generator drain, anchored at 0 at the generator's start and held
    flat across a gap between its lives, and is read at each SUT drain timestamp by linear
    interpolation, because the two processes' drains are out of phase. Within a run of steady
    intervals the interpolation telescopes; each edge of a run errs by at most one generator
    batch, which is the judgment's tolerance. Returns `[(start, end, kind, label, g, wk)]`."""
    service = source.get("service")
    gen, sut = led.gens[service], led.sut
    anchors = []
    cum = 0.0
    points = gen.counter_points("logit.output.messages", component=source.get("generator_sink"))
    starts = list(gen.life_starts)
    for life, start in enumerate(starts):
        anchors.append((start, cum))
        for ts, value, point_life in points:
            if point_life == life:
                cum += value
                anchors.append((ts, cum))
    anchors.sort(key=lambda item: item[0])
    anchor_ts = [t for t, _ in anchors]

    def g_at(t):
        if not anchors or t <= anchors[0][0]:
            return 0.0
        # The first anchor pair (t1, t2) with t1 <= t <= t2: t2 is the first anchor at or
        # after t, and t is past the first anchor, so index >= 1.
        index = bisect.bisect_left(anchor_ts, t)
        if index >= len(anchors):
            return anchors[-1][1]
        (t1, c1), (t2, c2) = anchors[index - 1], anchors[index]
        return c1 if t2 == t1 else c1 + (c2 - c1) * (t - t1) / (t2 - t1)

    counts = {}
    for name in ("logit.input.datagrams", "logit.input.kernel.drops"):
        for ts, value, _ in sut.counter_points(name, component=source.get("sut_listener")):
            counts[ts] = counts.get(ts, 0) + value
    drains = sorted(set(sut.drains) | set(counts))
    windows = [(f.start, f.end + data.recovery_bound, f"{f.step} {f.action} on {f.on}")
               for f in data.faults if affects_source(data, f, service)]
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
    """G − (W + K) per source, loss by design: reported per run of drain intervals, and judged
    only outside the fault windows that can affect that source's wire and at the end. See
    `_wire_buckets` for the tolerance. One detail per source; the worst status wins."""
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.wire", led)
    if skipped:
        return skipped
    if not led.gens:
        return Result("ledger.wire", SKIP, "no generator telemetry")
    statuses, details, lines = [], [], []
    for source in led.sources:
        service = source.get("service")
        if service not in led.gens:
            statuses.append(FAIL)
            details.append(f"{service}: no generator telemetry")
            continue
        status, detail, source_lines = _wire_one(data, led, source)
        statuses.append(status)
        label = f"{service} -> {source.get('sut_listener')}: " if len(led.sources) > 1 else ""
        details.append(label + detail)
        lines += [label + line for line in source_lines]
    return Result("ledger.wire", worst(statuses), " | ".join(details), lines)


def _wire_one(data, led, source):
    """`(status, detail, lines)` of `ledger.wire` for one source."""
    service, listener = source.get("service"), source.get("sut_listener")
    stats = led.gen_stats[service]
    status = PASS
    lines = []
    W = sum(led.per[listener]["W"].values())
    K = sum(led.per[listener]["K"].values())
    G = stats["G"]
    gap = G - W - K
    if G != stats["G_datagrams"]:
        status = FAIL
        lines.append(f"precondition: generator output.messages {_n(G)} != output.datagrams "
                     f"{_n(stats['G_datagrams'])}, so a datagram isn't one line")

    buckets = _wire_buckets(data, led, source)
    runs = []
    for start, end, kind, label, g, wk in buckets:
        if runs and runs[-1][2] == kind and runs[-1][3] == label:
            runs[-1][1] = end
            runs[-1][4] += g
            runs[-1][5] += wk
        else:
            runs.append([start, end, kind, label, g, wk])
    sums = {"fault": 0.0, "steady": 0.0, "end": 0.0}
    # Each steady run counts at 0 when W + K exceeds G in it, so a surplus in one run can't
    # net out loss in another.
    steady_lost = 0.0
    g_steady = 0.0
    edges = 0
    for index, (start, end, kind, label, g, wk) in enumerate(runs):
        sums[kind] += g - wk
        lines.append(f"[{data.offset(start)}, {data.offset(end)}] {kind} ({label}): "
                     f"G {_n(round(g))}, W+K {_n(wk)}, gap {_n(round(g - wk))}")
        if kind == "steady":
            steady_lost += max(0.0, g - wk)
            g_steady += g
            edges += sum(1 for other in (index - 1, index + 1)
                         if 0 <= other < len(runs) and runs[other][2] != "steady")

    batch = stats["batch"]
    limit = float(data.ledger.get("wire_loss_outside_faults", 0.0))
    tolerance = batch * edges
    judged = steady_lost + max(0.0, sums["end"])
    allowed = limit * g_steady + tolerance
    if judged > allowed:
        status = FAIL
        lines.insert(0, f"{_n(round(judged))} lost outside UDP-affecting windows, over "
                        f"{limit:g} x {_n(round(g_steady))} + {edges} edge(s) x one "
                        f"{_n(batch)}-line batch")
    last_queued = [v for _, v, _ in led.gens[service].gauge_series(
        "logit.component.buffer.batches", component=source.get("generator_sink"))]
    end_allow = ((last_queued[-1] if last_queued else 0) + 1) * batch
    if sums["end"] < -end_allow:
        status = FAIL
        lines.insert(0, f"G falls {_n(round(-sums['end']))} short of W + K at the end, over "
                        f"the end allowance "
                        f"{_n(end_allow)} (the generator sink's last buffer.batches + 1 batch)")
    detail = (f"G {_n(G)} = W {_n(W)} + K {_n(K)} + wire {_n(gap)} (by design: "
              f"{_n(round(sums['fault']))} in UDP-affecting windows, "
              f"{_n(round(sums['steady']))} steady, {_n(round(sums['end']))} at the end; "
              f"steady loss judged {_n(round(steady_lost))}, each run at 0 or more, against "
              f"limit {_n(round(allowed))}); the generator's counted drop_newest loss, "
              f"not in G: {_n(stats['dropped_events'])} event(s) in "
              f"{_n(stats['dropped_batches'])} batch(es)")
    if status != PASS:
        detail = f"{lines[0]}; {detail}"
    return status, detail, lines


def _per_life_lines(led):
    return [f"life {life}{' (final)' if life == led.final else ''}"
            f"{' (killed)' if life in led.killed else ''}: {led.symbols(life)}"
            for life in led.lives]


def check_ledger_intake(data):
    """The final life's W − D == E + B per listener; each earlier life's residual
    (`Ledger.residual`), summed over the listeners, in [0, R], a sanity bound. `ledger.egress`
    ties that residual to V. A life whose stderr shutdown lines disagree with telemetry's
    shutdown drops WARNs."""
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.intake", led)
    if skipped:
        return skipped
    status = PASS
    lines = _per_life_lines(led)
    life = led.final
    parts = []
    for listener in led.listeners:
        lhs = led.lget(listener, "W", life) - led.lget(listener, "D_tel", life)
        rhs = led.lget(listener, "E", life) + led.lget(listener, "B", life)
        name = f" {listener}" if len(led.listeners) > 1 else ""
        part = f"final life {life}{name}: W − D {_n(lhs)} vs E + B {_n(rhs)}"
        if lhs != rhs:
            status = FAIL
            # A listener off its balance leads the detail.
            parts.insert(0, part + f", off by {_n(lhs - rhs)}")
        else:
            parts.append(part)
    for earlier in led.lives[:-1]:
        residual = led.residual(earlier)
        ok = 0 <= residual <= led.R
        parts.append(f"life {earlier}: {led.residual_text} {_n(residual)} "
                     f"{'within' if ok else 'outside'} [0, R]")
        if not ok:
            status = FAIL
    mismatches = [m for m in map(led.datagram_mismatch, led.lives) if m]
    if mismatches:
        status = worst([status, WARN])
        parts += mismatches
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
    """The final life's edges, each `sent == received`: into `aggregate`, ΣE == A over the
    listeners, or with a script L_sent == A, the edge from the script, since `ledger.script`
    judges the listeners'; with a source's `sut_transforms`, each hop from its listener through
    them, every transform forwarding every event it received (received == sent), and the last
    transform's sent in place of that listener's E in the sum; and out to each sink, under `copy` `aggregate`'s events.sent == the sink's events.received
    (a fan-out counts one send per batch, however many consumers it reaches), and under `route`
    the sink's target's events.sent == the sink's events.received. Earlier lives are
    reported."""
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.edge", led)
    if skipped:
        return skipped
    if led.script:
        edges = [("L_sent", "A", led.L_sent, led.A, "script to aggregate")]
    else:
        edges = []
        for listener in led.listeners:
            chain = led.transforms.get(listener) or []
            left, lhs = f"E[{listener}]", led.per[listener]["E"]
            for component in chain:
                edges.append((left, f"T_in[{component}]", lhs, led.T_in[component],
                              f"into {component}"))
                edges.append((f"T_in[{component}]", f"T_out[{component}]",
                              led.T_in[component], led.T_out[component],
                              f"{component} forwards every event"))
                left, lhs = f"T_out[{component}]", led.T_out[component]
        into = {life: sum(led.into_aggregate(life).values()) for life in led.lives}
        if len(led.listeners) == 1 and not any(led.transforms.values()):
            edges.append(("E", "A", led.E, led.A, "listener to aggregate"))
        else:
            edges.append(("ΣE" if not any(led.transforms.values()) else "Σ(sent into aggregate)",
                          "A", into, led.A, "listeners to aggregate"))
    for sink in led.sinks:
        received = led.S_in[sink]
        if led.split == "route":
            target = led.targets.get(sink)
            edges.append((f"T_sent[{target}]", f"S_in[{sink}]", led.T_sent.get(sink, {}),
                          received, f"target {target} to {sink}"))
        else:
            edges.append(("A_sent", f"S_in[{sink}]", led.A_sent, received,
                          f"aggregate to {sink}"))
    lines = []
    parts = []
    off = []
    final = led.final
    for left, right, lhs, rhs, what in edges:
        for life in led.lives:
            a, b = lhs.get(life, 0), rhs.get(life, 0)
            lines.append(f"{what}, life {life}{' (final)' if life == final else ''}: {left} "
                         f"{_n(a)}, {right} {_n(b)}, {left} − {right} {_n(a - b)}"
                         + ("" if life == final else " (reported, not judged)"))
        a, b = lhs.get(final, 0), rhs.get(final, 0)
        if a != b:
            off.append(f"{left} {_n(a)} != {right} {_n(b)}, off by {_n(a - b)} ({what})")
        else:
            parts.append(f"{left} {_n(a)} == {right} {_n(b)} ({what})")
    if off:
        return Result("ledger.edge", FAIL, f"final life {final}: " + "; ".join(off + parts),
                      lines)
    return Result("ledger.edge", PASS, f"final life {final}: " + "; ".join(parts), lines)


def check_ledger_script(data):
    """For a `[ledger] sut_script`, else SKIP, the final life's script hop: E == L_in (the
    listener's edge), L_in == L_emit + L_drop + L_err (every event emitted, dropped, or failed
    on), and L_sent == L_emit + L_flush (what it sent is what `process()` emitted plus what
    `flush()` returned). Earlier lives are reported."""
    if not data.ledger.get("sut_script"):
        return Result("ledger.script", SKIP, "no [ledger] sut_script")
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.script", led)
    if skipped:
        return skipped
    g = led.get

    def identities(life):
        return [
            ("E == L_in", g("E", life), g("L_in", life)),
            ("L_in == L_emit + L_drop + L_err", g("L_in", life),
             g("L_emit", life) + g("L_drop", life) + g("L_err", life)),
            ("L_sent == L_emit + L_flush", g("L_sent", life),
             g("L_emit", life) + g("L_flush", life)),
        ]

    lines = []
    for life in led.lives:
        shown = ", ".join(f"{name.split(' == ')[0]} {_n(a)} vs {_n(b)}"
                          for name, a, b in identities(life))
        lines.append(f"life {life}{' (final)' if life == led.final else ' (reported, not judged)'}"
                     f": {shown}; {led.symbols(life)}")
    life = led.final
    off = [f"{name} off by {_n(a - b)} ({_n(a)} vs {_n(b)})"
           for name, a, b in identities(life) if a != b]
    summary = (f"final life {life}: E {_n(g('E', life))} == L_in {_n(g('L_in', life))} == L_emit "
               f"{_n(g('L_emit', life))} + L_drop {_n(g('L_drop', life))} + L_err "
               f"{_n(g('L_err', life))}; L_sent {_n(g('L_sent', life))} == L_emit + L_flush "
               f"{_n(g('L_flush', life))}")
    if off:
        return Result("ledger.script", FAIL, f"final life {life}: {'; '.join(off)}", lines)
    return Result("ledger.script", PASS, summary, lines)


def check_ledger_aggregate(data):
    """The final life's Ab == A."""
    return _final_equality(data, "ledger.aggregate", "Ab", "A", "every event absorbed")


def check_ledger_egress(data):
    """The egress gap per life and per egress group (`Ledger.egress_groups`: each sink under
    `copy`, the sinks' summed V under `route`), Ab − V for the final life and a killed one, and
    W − D − B − X − V for a stopped earlier one: 0 passes; a positive gap with a batch that
    life's telemetry counts in `batches.dropped`, under any reason, at a node that can explain
    it (`Ledger.drop_credit`) is counted, never reconciled; a killed life's gap passes inside
    `Ledger.kill_band`; any other gap is uncounted loss, or a surplus below a killed life's
    band or below 0 on a Kafka target. A `FAULT_DROP_REASONS` drop in a life's credit FAILs it
    whatever its gap. A sink whose export has no series, or a Kafka topic with no records,
    can't pass."""
    if data.external:
        return Result("ledger.egress", SKIP, f"{NO_BACKEND}; ledger.sent judges the sinks")
    if data.kafka and data.phases.get("kafka_read") is None:
        failed = data.phases.get("kafka_read_failed") or {}
        return Result("ledger.egress", FAIL, "the topic wasn't read back, so V is unknown: "
                      + (json.dumps({k: v for k, v in failed.items()
                                     if k not in ("event", "phase", "t", "offset")})[:500]
                         if failed else "no kafka_read phase in the timeline"))
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.egress", led)
    if skipped:
        return skipped
    if not led.sinks:
        return Result("ledger.egress", FAIL, "no [[ledger.sink]] to judge")
    status = PASS
    lines = []
    groups = []
    for label, sinks in led.egress_groups():
        parts = []
        for life in led.lives:
            verdict_status, verdict, gap = led.egress(life, sinks)
            status = worst([status, verdict_status])
            ab_v = led.absorbed(life) - led.v(life, sinks)
            flush = f" (L_flush {_n(led.get('L_flush', life))})" if led.script else ""
            absorbed = (f"{led.ab_text} {_n(led.absorbed(life))}{flush} − "
                        f"{led.v_text(life, sinks)}")
            if life == led.final:
                part = f"life {life} (final): {absorbed} = {_n(gap)}, {verdict}"
            elif life in led.killed:
                _, _, band = led.kill_band(life)
                part = (f"life {life} (killed): {absorbed} = {_n(gap)}, {verdict} ({band}); "
                        f"residual {led.residual_text} {_n(led.residual(life))} at its last "
                        "drain, what the kill lost from the receive queue and inbox, by design, "
                        "not judged here; D has no shutdown part: a SIGKILL counts no "
                        "shutdown drops")
            else:
                whole = ("W − D − B − X − (L_drop + L_err) − V" if led.script
                         else "W − D − B − X − V")
                part = (f"life {life}: {whole} {_n(gap)} = residual "
                        f"{_n(led.residual(life))} + {led.ab_text} − V {_n(ab_v)}, {verdict}")
            credit = led.drop_credit(life, sinks)
            if gap > 0 and not (life in led.killed and life != led.final):
                others = ""
                elsewhere = led.other_sinks_drops(life, sinks)
                if elsewhere:
                    others = f", not counting {_reasons(elsewhere)} at the other sinks"
                part += f" (batches dropped: {_reasons(credit)}{others})"
            elif credit and life in led.killed and life != led.final:
                # A SIGKILL counts no shutdown drops, but the life's drains can carry others,
                # such as a queue's overflow during a sink outage before the kill.
                part += (f"; batches dropped: {_reasons(credit)}, reported, not judged: the "
                         "band judges a killed life's gap")
            fault = led.fault_drops(life, sinks)
            if fault:
                part += f"; FAIL: {fault}"
            parts.append(part)
        empty = [sink for sink in sinks if not led.series.get(sink) and sink != led.kafka_sink]
        if led.kafka_sink in sinks:
            if not led.records:
                status = FAIL
                parts.insert(0, f"no records in {topic.TOPIC_FILE}")
            if led.unparsed:
                status = FAIL
                parts.insert(0, f"{_n(led.unparsed)} topic record(s) aren't JSON objects")
        for sink in empty:
            status = FAIL
            selector = next((one.get("vm_selector") for one in data.ledger.get("sink") or []
                             if one.get("id") == sink), None)
            parts.insert(0, f"no series in vm-export/{sink}.jsonl match vm_selector "
                            f"{selector!r}")
        groups.append(f"{label}: " + "; ".join(parts))
    lines += groups
    mismatches = list(filter(None, map(led.batch_mismatch, led.lives)))
    if mismatches:
        status = worst([status, WARN])
    lives = len(led.lives)
    wrong_total = 0
    for sink in led.sinks:
        if sink == led.kafka_sink:
            copies = sum(led.duplicates.values())
            lines.append(f"{sink}: {_n(len(led.records) + copies)} topic record(s), "
                         f"{_n(len(led.records))} distinct, {_n(copies)} duplicate copies; V "
                         f"{_n(led.V_total[sink])} (the sum of the distinct records' delta sums)")
            continue
        wrong = {name: n for name, n in led.resets[sink].items() if n != lives - 1}
        lines.append(f"{sink}: {len(led.series.get(sink, []))} series, reset-aware V "
                     f"{_n(led.V_total[sink])}; resets per series should be {lives - 1} (SUT "
                     "lives − 1)")
        if wrong:
            wrong_total += len(wrong)
            status = worst([status, WARN])
            sample = ", ".join(f"{name} {n}" for name, n in list(wrong.items())[:5])
            lines.insert(0, f"{sink}: {len(wrong)} series with resets other than {lives - 1}: "
                            f"{sample}; their segments are credited by timestamp")
    for life in led.lives:
        if life in led.killed and life != led.final:
            lines.append(f"life {life} was killed: no final drain and no drain complete line")
        elif life != led.final and led.drain_completes.get(life, 0) == 0:
            lines.append(f"life {life} has no drain complete line")
    split = f"split {led.split}"
    detail = " | ".join(groups + mismatches) + f" ({split}; {led.R_text})"
    if wrong_total:
        detail += f"; {wrong_total} series' resets aren't SUT lives − 1"
    return Result("ledger.egress", status, detail, lines)


def check_ledger_summary(data):
    """The final life's uncounted = (W − D − E − B) + (E − A) + (A − Ab) + (Ab − V), which must
    be 0, with wire loss beside it by design. There is one Ab − V term per egress group, and
    each must be 0 or counted on its own, so a surplus at one sink can't net out loss at
    another. With a script, E − A becomes its four script terms (`check_ledger_script`'s three
    identities and L_sent − A), the egress term is Ab − L_flush − V, and the script's drops and
    errors are shown beside it, counted. A counted egress term is shown and not judged, but a
    `FAULT_DROP_REASONS` drop in its group's credit FAILs the row as it FAILs `ledger.egress`;
    on an external target the Ab − V term SKIPs, and the row judges the others. On a Kafka target V counts the topic's distinct records, so a
    duplicate never enters the sum."""
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.summary", led)
    if skipped:
        return skipped
    life = led.final
    g = led.get
    terms = [("W − D − E − B", g("W", life) - led.D(life) - g("E", life) - g("B", life))]
    if led.script:
        terms += [
            ("E − L_in", g("E", life) - g("L_in", life)),
            ("L_in − L_emit − L_drop − L_err",
             g("L_in", life) - g("L_emit", life) - g("L_drop", life) - g("L_err", life)),
            ("L_emit + L_flush − L_sent", g("L_emit", life) + g("L_flush", life)
             - g("L_sent", life)),
            ("L_sent − A", g("L_sent", life) - g("A", life)),
        ]
    else:
        terms.append(("E − A", g("E", life) - g("A", life)))
    terms.append(("A − Ab", g("A", life) - g("Ab", life)))
    shown = [f"{name} {_n(value)}" for name, value in terms]
    uncounted_egress = False
    faults = []
    if data.external:
        judged = sum(value for _, value in terms)
        shown.append(f"{led.ab_text} − V SKIP ({NO_BACKEND})")
    else:
        judged = sum(value for _, value in terms)
        for _, sinks in led.egress_groups():
            _, verdict, egress_gap = led.egress(life, sinks)
            counted = verdict == "counted"
            if not counted:
                judged += egress_gap
                uncounted_egress = uncounted_egress or egress_gap != 0
            v = "ΣV" if len(sinks) > 1 else f"V[{sinks[0]}]"
            fault = led.fault_drops(life, sinks)
            if fault:
                faults.append(fault)
            shown.append(f"{led.ab_text} − {v} {_n(egress_gap)}"
                         + (f" {verdict}" if counted else "")
                         + (f" (FAIL: {fault})" if fault else ""))
    counted_script = ""
    if led.script:
        counted_script = (f"; the script's L_drop {_n(g('L_drop', life))} and L_err "
                          f"{_n(g('L_err', life))}, counted")
    wire = ""
    if led.gens:
        wire = (f"; wire G − (W + K) {_n(led.G - sum(led.W.values()) - sum(led.K.values()))} "
                "over the run, by design")
    status = PASS if judged == 0 and not uncounted_egress and not faults else FAIL
    detail = (f"final life {life}: uncounted {_n(judged)} = " + " + ".join(shown)
              + counted_script + wire)
    return Result("ledger.summary", status, detail, _per_life_lines(led))


def check_identity_sink(data):
    """For each sink, at the final life's last drain before its shutdown drain:
    batches.received + buffer.disk.replayed == delivered + dropped (every reason) +
    buffer.batches, one batch in flight allowed."""
    led = _ledger(data)
    skipped = _skip_without_sut("identity.sink", led)
    if skipped:
        return skipped
    sut, life = led.sut, led.final
    drains = [d for d in sut.life_drains(life)]
    if len(drains) < 2:
        return Result("identity.sink", SKIP, f"final life {life} has under two drains")
    at = drains[-2]
    status = PASS
    lines = []
    for sink in led.sinks:
        def upto(name, sink=sink):
            return sum(v for ts, v, point_life in sut.counter_points(name, component=sink)
                       if point_life == life and ts <= at)

        received = upto("logit.component.batches.received")
        # A disk spool opens holding what an earlier process queued: those batches are
        # delivered in this life but were received in another, and `buffer.disk.replayed`
        # counts them.
        replayed = upto("logit.component.buffer.disk.replayed")
        delivered = upto("logit.component.batches.delivered")
        dropped = upto("logit.component.batches.dropped")
        queued = next((v for ts, v, point_life in reversed(sut.gauge_series(
            "logit.component.buffer.batches", component=sink))
            if point_life == life and ts <= at), 0)
        gap = received + replayed - delivered - dropped - queued
        replay = f" + replayed {_n(replayed)}" if replayed else ""
        if not 0 <= gap <= 1:
            status = FAIL
        lines.append(f"{sink}: received {_n(received)}{replay} vs delivered {_n(delivered)} + "
                     f"dropped {_n(dropped)} + buffer.batches {_n(queued)}, gap {_n(gap)}")
    detail = (f"final life {life} at {data.offset(at)}: " + "; ".join(lines)
              + " (one batch in flight allowed)")
    return Result("identity.sink", status, detail, lines)


def check_ledger_windows(data):
    """Opt-in, under `[ledger] vm_every_window`: every series has a sample for every
    `aggregate` window the SUT emitted, in every sink's export. Per life and series, consecutive samples are at most
    1.5 x the configured interval apart, unless the SUT's `aggregate` sent no batch in the span
    the missing windows would have been flushed in; a killed life's last sample is at most
    1.1 x the interval before the kill, because the kill discards only the window in progress,
    and 10% covers a flush's own lateness; and a later life's first sample is at most 2 x the
    interval after the life starts, the first window plus startup. Opt-in because `aggregate`
    emits a series only in a window that updated it, so an idle series has gaps by design. Under
    `temporality: cumulative` V reads only each life's last value, so a lost middle window
    changes no total and only this row sees it."""
    if not data.has_vm:
        return Result("ledger.windows", SKIP, no_vm_reason(data))
    if data.ledger.get("vm_every_window") is not True:
        return Result("ledger.windows", SKIP, "not enabled ([ledger] vm_every_window)")
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.windows", led)
    if skipped:
        return skipped
    interval = led.interval
    if not interval:
        return Result("ledger.windows", FAIL, f"the SUT config names no interval for aggregate "
                      f"{led.aggregate!r}, so no window can be judged")
    empty = [sink for sink in led.sinks if not led.series.get(sink)]
    if empty:
        return Result("ledger.windows", FAIL, "no series match vm_selector in "
                      + ", ".join(f"vm-export/{sink}.jsonl" for sink in empty))
    max_gap, tail_bound, head_bound = 1.5 * interval, 1.1 * interval, 2 * interval
    # A gap (a, b] is idle when `aggregate` sent no batch in the span its missing windows would
    # have been flushed in: an idle flush sends nothing, so no window existed to store. A flush
    # at f is reported by the drain at f + d, d from about 0 to one drain period p, since the
    # two timers share a phase and either may fire first at a shared tick. The windows missing
    # from (a, b] flush at a + k x interval, 0 < k < (b - a) / interval, so their drains fall
    # in [a + interval, b - interval + p]; a's own drain falls in [a, a + p] and b's in
    # [b, b + p]. Counting over [a + (interval + p) / 2, b - (interval - p) / 2) splits both
    # separations in half, a margin of (interval - p) / 2 on each side for timer jitter, and
    # needs p < interval. A paused SUT writes no drain and flushes nothing, so a gap across a
    # pause counts 0 over no drains and is idle: no window closed, so none was lost.
    gaps = [b - a for a, b in zip(led.sut.drains, led.sut.drains[1:]) if b > a]
    period = round(statistics.median(gaps), 1) if gaps else None
    sent = [(ts, value) for ts, value, _ in led.sut.counter_points(
        "logit.component.batches.sent", component=led.aggregate) if value]
    drains = led.sut.drains
    idle_rule = period is not None and period < interval

    def flushes(a, b):
        """(batches sent, drains, span start, span end) over the span between a and b."""
        lo, hi = a + (interval + period) / 2, b - (interval - period) / 2
        n_sent = sum(value for ts, value in sent if lo <= ts < hi)
        n_drains = bisect.bisect_left(drains, hi) - bisect.bisect_left(drains, lo)
        return n_sent, n_drains, lo, hi

    idle = {}
    problems = []
    widest = 0.0
    every = [(sink, one) for sink in led.sinks for one in led.series[sink]]
    for sink, one in every:
        name = f"{sink} {vm.series_name(one)}"
        by_life = {}
        for stamp in one.get("timestamps") or []:
            t = stamp / 1000
            by_life.setdefault(vm.life_at(led.life_starts, t), []).append(t)
        for life in led.lives:
            stamps = sorted(by_life.get(life, []))
            if not stamps:
                problems.append(f"{name}: no sample in life {life}")
                continue
            for a, b in zip(stamps, stamps[1:]):
                if b - a <= max_gap:
                    widest = max(widest, b - a)
                    continue
                gap = f"life {life} gap {b - a:g}s from {data.offset(a)} to {data.offset(b)}"
                if not idle_rule:
                    widest = max(widest, b - a)
                    problems.append(f"{name}: {gap}")
                    continue
                n_sent, n_drains, lo, hi = flushes(a, b)
                span = (f"{led.aggregate} sent {n_sent} batch(es) over {n_drains} drain(s) "
                        f"from {data.offset(lo)} to {data.offset(hi)}")
                if n_sent == 0:
                    idle.setdefault((life, a, b), [0, span])[0] += 1
                    continue
                widest = max(widest, b - a)
                problems.append(f"{name}: {gap}, windows emitted but not stored: {span}")
            if life in led.killed_at and life != led.final:
                kill = led.killed_at[life]
                if stamps[-1] < kill - tail_bound:
                    problems.append(f"{name}: killed life {life}'s last sample at "
                                    f"{data.offset(stamps[-1])}, {kill - stamps[-1]:g}s before "
                                    f"the kill at {data.offset(kill)}")
            if life > 0 and stamps[0] > led.life_starts[life] + head_bound:
                problems.append(f"{name}: life {life}'s first sample at "
                                f"{data.offset(stamps[0])}, "
                                f"{stamps[0] - led.life_starts[life]:g}s after the life started "
                                f"at {data.offset(led.life_starts[life])}")
    if idle_rule:
        rules = (f"gaps <= {max_gap:g}s (1.5 x the {interval:g}s aggregate interval) or idle, "
                 f"with no {led.aggregate} batches.sent from a + {(interval + period) / 2:g}s to "
                 f"b - {(interval - period) / 2:g}s ({period:g}s drains); a killed life's last "
                 f"sample within {tail_bound:g}s (1.1 x) of the kill, a later life's first "
                 f"within {head_bound:g}s (2 x) of its start")
    else:
        rules = (f"gaps <= {max_gap:g}s (1.5 x the {interval:g}s aggregate interval), no idle "
                 f"excusal because the drain period {period}s isn't under the interval; a killed "
                 f"life's last sample within {tail_bound:g}s (1.1 x) of the kill, a later life's "
                 f"first within {head_bound:g}s (2 x) of its start")
    # One line per distinct idle gap: every series flushes at the same instants, so a gap
    # usually spans them all.
    excused = [f"idle gap excused: life {life} {data.offset(a)} to {data.offset(b)} ({b - a:g}s, "
               f"{count} series), {span}" for (life, a, b), (count, span) in sorted(idle.items())]
    if excused:
        shown = "; ".join(line.removeprefix("idle gap excused: ") for line in excused[:3])
        more = f"; {len(excused) - 3} more in the lines" if len(excused) > 3 else ""
        rules += f"; {len(excused)} idle gap(s) excused: {shown}{more}"
    if problems:
        return Result("ledger.windows", FAIL,
                      f"missing windows, {len(problems)} finding(s), first: {problems[0]} "
                      f"({rules})", problems + excused)
    return Result("ledger.windows", PASS,
                  f"{len(every)} series over {len(led.sinks)} sink(s) x {len(led.lives)} "
                  "life/lives, widest gap "
                  f"{widest:g}s ({rules})", excused)


def check_ledger_replay(data):
    """For each killed life followed by another, and each sink: the next life's first-drain
    `buffer.disk.replayed` equals the killed life's `buffer.batches` at its last drain, one
    batch in flight allowed. What a kill leaves queued comes back only through a disk spool's
    replay at open, so a short replay is lost windows `identity.sink` balances away."""
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.replay", led)
    if skipped:
        return skipped
    pairs = [life for life in led.killed if life + 1 in led.lives]
    if not pairs:
        return Result("ledger.replay", SKIP, "no killed life followed by another")
    sut = led.sut
    status = PASS
    parts = []
    for sink, life in [(sink, life) for sink in led.sinks for life in pairs]:
        drains = sut.life_drains(life)
        nxt = sut.life_drains(life + 1)
        queued = next((v for ts, v, point_life in reversed(sut.gauge_series(
            "logit.component.buffer.batches", component=sink))
            if point_life == life and drains and ts <= drains[-1]), 0)
        replayed = sum(v for ts, v, point_life in sut.counter_points(
            "logit.component.buffer.disk.replayed", component=sink)
            if point_life == life + 1 and nxt and ts == nxt[0])
        ok = abs(replayed - queued) <= 1
        status = worst([status, PASS if ok else FAIL])
        parts.append(f"{sink}: life {life + 1}'s first drain replayed {_n(replayed)} vs killed life "
                     f"{life}'s last-drain buffer.batches {_n(queued)}"
                     + ("" if ok else ", off by " + _n(replayed - queued)))
    return Result("ledger.replay", status,
                  "; ".join(parts) + " (one batch in flight allowed)", parts)


def _by_attr(tel, name, attr, **attrs):
    """`{value of attr: summed delta}` for a `sum` metric over the whole run."""
    sums = {}
    for point in tel.matching(name, **attrs):
        if isinstance(point.value, (int, float)):
            key = point.attributes.get(attr, "?")
            sums[key] = sums.get(key, 0) + point.value
    return {key: value for key, value in sums.items() if value}


def check_ledger_sent(data):
    """For an external target, where the ledger ends at the sinks' telemetry: `SENT` (a PASS
    whose detail starts with the word) when every batch each sink received ended in an accepted
    response, judged per sink and SUT life by the sink's batch identity and over the run by its
    drops, refusals, and request classes. A Kafka target's sink is judged the same way, and its
    ledger goes on to the topic. A local target SKIPs. `docs/plans/soak-harness.md`,
    "8. External targets (W5)", lists each FAIL and WARN."""
    if data.has_vm:
        return Result("ledger.sent", SKIP, "local target: ledger.egress judges delivery against "
                      "the backends")
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.sent", led)
    if skipped:
        return skipped
    status = PASS
    problems = []
    lines = []
    summaries = []

    def flag(verdict, text):
        nonlocal status
        status = worst([status, verdict])
        problems.append((verdict, text))

    for mismatch in filter(None, map(led.batch_mismatch, led.lives)):
        flag(WARN, mismatch)
    drained = data.phases.get("sink_drained")
    if drained and not drained.get("held"):
        left = {sink: (one or {}).get("buffer_batches")
                for sink, one in (drained.get("sinks") or {}).items()}
        shown = ", ".join(f"{sink} {'unknown' if n is None else _n(n)}"
                          for sink, n in sorted(left.items())) or "unknown"
        flag(FAIL, f"a sink's queue never emptied after the generator stopped: sink_drained "
                   f"held=false after {drained.get('bound_s')}s with buffer.batches {shown} "
                   "left unsent")
    for sink in led.sinks:
        summaries.append(_sent_one(data, led, sink, flag, lines))
    evidence = ("ledger.egress and ledger.topic read the topic back" if data.kafka else
                f"{NO_BACKEND}, so a 2xx is the end of the evidence")
    summary = "; ".join(summaries) + f" (one batch in flight allowed); {evidence}"
    if status == PASS:
        # `SENT` marks a ledger that ends at the sink; a Kafka target's goes on to the topic.
        lead = "" if data.kafka else "SENT "
        return Result("ledger.sent", PASS, f"{lead}to {data.target.get('name')}: every batch "
                      f"received ended in an accepted response; {summary}", lines)
    worst_first = sorted(problems, key=lambda item: -_ORDER[item[0]])
    return Result("ledger.sent", status, f"{worst_first[0][1]}; {summary}",
                  [text for _, text in worst_first] + lines)


def _sent_one(data, led, sink, flag, lines):
    """`ledger.sent`'s reading of one sink: flags each problem through `flag`, appends its
    lines, and returns its summary."""
    sut = led.sut
    # The request class an accepted answer carries: an HTTP sink's status class, or the class
    # `kafka_out` counts a successful produce attempt under.
    accepted_class = "ok" if data.kafka else "2xx"
    identities = []
    # Flagged after the drops below, so a rejection, which explains a gap, leads the detail.
    off_identity = []
    for life in led.lives:
        drains = sut.life_drains(life)
        if not drains:
            flag(FAIL, f"{sink}: life {life} has no drain")
            continue
        at = drains[-2] if life == led.final and len(drains) >= 2 else drains[-1]

        def upto(name, life=life, at=at):
            return sum(v for ts, v, point_life in sut.counter_points(name, component=sink)
                       if point_life == life and ts <= at)

        received = upto("logit.component.batches.received")
        replayed = upto("logit.component.buffer.disk.replayed")
        delivered = upto("logit.component.batches.delivered")
        dropped = upto("logit.component.batches.dropped")
        queued = next((v for ts, v, point_life in reversed(sut.gauge_series(
            "logit.component.buffer.batches", component=sink))
            if point_life == life and ts <= at), 0)
        gap = received + replayed - delivered - dropped - queued
        replay = f" + replayed {_n(replayed)}" if replayed else ""
        text = (f"{sink} life {life} at {data.offset(at)}: received {_n(received)}{replay} vs "
                f"delivered {_n(delivered)} + dropped {_n(dropped)} + buffer.batches "
                f"{_n(queued)}, gap {_n(gap)}")
        identities.append(text)
        lines.append(text)
        if not 0 <= gap <= 1:
            off_identity.append(f"{text}, outside [0, 1]")
        if life == led.final and queued > 0:
            off_identity.append(f"{sink} life {life}'s buffer.batches at its quiet drain at "
                                f"{data.offset(at)} is {_n(queued)}: {_n(queued)} batch(es) "
                                "left unsent")

    diagnostics = [entry for entry in data.stderr.get("logit", [])
                   if entry.kind == "json" and entry.key in REJECTION_KEYS + REFUSAL_KEYS
                   and entry.component == sink]

    def quoted(*keys):
        """The first stderr line carrying one of `keys`, with the status it names."""
        first = next((entry for entry in diagnostics if entry.key in keys), None)
        if first is None:
            return f"no {' or '.join(keys)} line on stderr"
        answered = _ANSWERED_RE.search(first.message)
        if answered:
            status_text = f"status {answered.group(1)}"
        elif first.key == "oversize":
            status_text = "dropped before sending"
        elif data.kafka:
            status_text = "the broker's error"
        else:
            status_text = "a 2xx whose body named dropped series"
        return (f"{status_text}: {first.key} at {data.offset(first.ts)}: {first.message[:300]}")

    for key in REFUSAL_KEYS:
        count = sum(1 for entry in diagnostics if entry.key == key)
        if count:
            flag(FAIL, f"{sink}: the destination refused a request, so the sink held its "
                       f"queue: {count} {key} line(s) ({quoted(key)})")

    batch_drops = _by_attr(sut, "logit.component.batches.dropped", "reason", component=sink)
    for reason, value in sorted(batch_drops.items()):
        if reason in REJECTED_BATCH_REASONS:
            flag(FAIL, f"{sink}: {_n(value)} batch(es) dropped {reason}, the destination "
                       f"refused them ({quoted('request_rejected')})")
        else:
            flag(WARN, f"{sink}: {_n(value)} batch(es) dropped {reason}, counted")
    record_drops = _by_attr(sut, "logit.output.records.dropped", "reason", component=sink)
    for reason, value in sorted(record_drops.items()):
        if reason in REJECTED_RECORD_REASONS:
            flag(FAIL, f"{sink}: {_n(value)} record(s) dropped {reason}, refused by the "
                       "destination or too large to send "
                       f"({quoted('request_rejected', 'oversize')})")
        else:
            flag(WARN, f"{sink}: {_n(value)} record(s) dropped {reason} before sending, counted")
    named = sum(sut.counter_by_life("logit.output.records.rejected", component=sink).values())
    if named:
        flag(FAIL, f"{sink}: {_n(named)} record(s) named dropped in an accepted response's body "
                   f"({quoted('series_rejected')})")
    skipped_metrics = _by_attr(sut, "logit.output.metrics.skipped", "reason", component=sink)
    skipped_metrics.update({f"metric_kind={k}": v for k, v in _by_attr(
        sut, "logit.output.metrics.skipped", "metric_kind", component=sink).items()
        if k != "?"})
    skipped_metrics.pop("?", None)
    total_skipped = sum(sut.counter_by_life("logit.output.metrics.skipped",
                                            component=sink).values())
    if total_skipped:
        detail = ", ".join(f"{k} {_n(v)}" for k, v in sorted(skipped_metrics.items()))
        flag(WARN, f"{sink}: {_n(total_skipped)} metric(s) skipped by the encoder, never sent"
                   + (f" ({detail})" if detail else ""))

    for text in off_identity:
        flag(FAIL, text)

    requests = sut.matching("logit.output.requests", component=sink)
    classes = {}
    outside = {}
    windows = data.windows()
    drains = sut.drains
    for point in requests:
        if not isinstance(point.value, (int, float)) or not point.value:
            continue
        cls = point.attributes.get("class", "?")
        classes[cls] = classes.get(cls, 0) + point.value
        if cls == accepted_class:
            continue
        # A drain's deltas cover the interval since the drain before it.
        index = bisect.bisect_left(drains, point.ts)
        previous = drains[index - 1] if index > 0 else point.ts
        if not any(start < point.ts and end > previous for start, end in windows):
            outside[cls] = outside.get(cls, 0) + point.value
            lines.append(f"{sink}: {_n(point.value)} request(s) answered {cls} in the drain at "
                         f"{data.offset(point.ts)}, outside every fault window")
    shown = ", ".join(f"{cls} {_n(value)}" for cls, value in sorted(classes.items()))
    if not classes:
        flag(FAIL, f"no logit.output.requests from {sink}: nothing shows a request was sent")
    elif not classes.get(accepted_class):
        flag(FAIL, f"{sink}: no request to {data.target.get('name')} answered {accepted_class} "
                   f"({shown})")
    if outside:
        flag(WARN, f"{sink}: {_n(sum(outside.values()))} request(s) answered other than "
                   f"{accepted_class} outside every fault window ("
                   + ", ".join(f"{cls} {_n(v)}" for cls, v in sorted(outside.items())) + ")")
    accepted = sum(sut.counter_by_life("logit.output.records", component=sink).values())
    lines.insert(0, f"{sink}: requests by class: {shown or 'none'}; records accepted "
                    f"{_n(accepted)}")
    lines += [f"{sink}: {entry.key} at {data.offset(entry.ts)}: {entry.message[:300]}"
              for entry in diagnostics]
    return (f"{sink}: requests {shown or 'none'}; records accepted {_n(accepted)}; "
            + "; ".join(identities))


def check_ledger_topic(data):
    """For a Kafka target: the topic's distinct record count T against the records `kafka_out`
    saw accepted, A (`logit.output.records`, summed over every SUT life), and the topic's
    duplicate copies per life. T below A FAILs, records the broker acknowledged and no longer
    holds, and T above A FAILs, records the sink never saw accepted, unless the sink dropped an
    attempt it never saw answered (an `ambiguous_at_most_once` record drop or any batch drop),
    which may have landed: a WARN. When a scheduled kill ended a SUT life, a T − A difference
    in either direction WARNs and names the killed lives: the kill loses that life's last
    telemetry interval of A, and a spooled batch counted before the kill and replayed after it
    is in A twice but stored once. A duplicate copy FAILs when the life
    it's credited to has no epoch-bumping event: it isn't the final life, so no later producer id
    resent it, and the sink counted no `EPOCH_BUMP_REASONS` drop in it. Within one epoch the
    broker discards a resend. Duplicates after an epoch bump WARN, which docs/known-gaps/kafka.md
    permits. A topic not read back FAILs. Every other target SKIPs."""
    if not data.kafka:
        return Result("ledger.topic", SKIP, f"{data.target.get('kind')} target: no topic")
    read = data.phases.get("kafka_read")
    if read is None:
        failed = data.phases.get("kafka_read_failed") or {}
        shown = {k: v for k, v in failed.items() if k not in ("event", "phase", "t", "offset")}
        return Result("ledger.topic", FAIL, "the topic wasn't read back: "
                      + (json.dumps(shown)[:500] if shown else "no kafka_read phase"))
    led = _ledger(data)
    skipped = _skip_without_sut("ledger.topic", led)
    if skipped:
        return skipped
    t_count = len(led.records)
    copies = sum(led.duplicates.values())
    by_life = led.sut.counter_by_life("logit.output.records", component=led.kafka_sink)
    accepted = sum(by_life.values())
    lines = [f"topic {data.target.get('topic')!r}: end offsets {read.get('offsets')}, "
             f"{_n(t_count + copies)} record(s) read back, {_n(t_count)} distinct",
             "records accepted by life: " + ", ".join(
                 f"life {life} {_n(value)}" for life, value in sorted(by_life.items()))]
    status = PASS
    problems = []
    for life, count in sorted(led.duplicates.items()):
        bumps = led.epoch_bumps(life)
        why = []
        if life != led.final:
            why.append("a later life resent under a new producer id")
        why += [f"{_n(value)} record(s) dropped {reason}" for reason, value in bumps.items()]
        if why:
            status = worst([status, WARN])
            problems.append(f"life {life}: {_n(count)} duplicate copies after an epoch bump "
                            f"({'; '.join(why)}), which at_least_once permits")
        else:
            status = FAIL
            problems.append(f"life {life}: {_n(count)} duplicate copies with no epoch bump: "
                            "the broker stored a resend it should have discarded")
    gap = t_count - accepted
    detail = (f"T {_n(t_count)} distinct records in the topic ({_n(t_count + copies)} read, "
              f"{_n(copies)} duplicate copies) vs A {_n(accepted)} records the sink saw "
              f"accepted, T − A {_n(gap)}")
    killed = ", ".join(str(life) for life in led.killed)
    if gap and led.killed:
        status = worst([status, WARN])
        problems.insert(0, f"T − A differs across killed SUT life {killed}: the kill loses that "
                           "life's last telemetry interval of A, and a spooled batch replayed "
                           "after it is counted twice in A and stored once")
    elif gap < 0:
        status = FAIL
        problems.insert(0, "the broker acknowledged records it no longer holds")
    elif gap > 0:
        unsettled = sum(led.sut.counter_by_life("logit.output.records.dropped",
                                                component=led.kafka_sink,
                                                reason="ambiguous_at_most_once").values())
        batches = sum(led.sut.counter_by_life("logit.component.batches.dropped",
                                              component=led.kafka_sink).values())
        if unsettled or batches:
            status = worst([status, WARN])
            problems.insert(0, f"records the sink never saw accepted, which an attempt it "
                               f"dropped unanswered may have landed ({_n(unsettled)} record(s) "
                               f"dropped ambiguous_at_most_once, {_n(batches)} batch(es) "
                               "dropped)")
        else:
            status = FAIL
            problems.insert(0, "records the sink never saw accepted")
    if problems:
        detail += ": " + "; ".join(problems)
    return Result("ledger.topic", status, detail, lines)


def _step_value(series, t, default=0.0):
    """A gauge's last value at or before `t`, or `default`."""
    value = default
    for ts, v, _ in series:
        if ts > t:
            break
        value = v
    return value


def check_recovery(data):
    """Each fault has recovered at each sink when, at some SUT drain within `recovery_bound` of
    its end, that sink's `retrying` is 0, its `buffer.utilization` is under 0.05 or its
    `buffer.batches` at most 1, every listener's `receive.utilization` is under 0.05, and the
    ingest rate summed over the listeners over the drain interval ending there is at least 95%
    of the generators' summed warmup rate. A drain interval another fault overlaps, or that
    spans two SUT lives, isn't eligible; a fault with no eligible interval is skipped and
    listed. A `rate_behind` diagnostic from any generator turns a throughput shortfall into a
    WARN."""
    led = _ledger(data)
    skipped = _skip_without_sut("recovery", led)
    if skipped:
        return skipped
    if not data.faults:
        return Result("recovery", PASS, "no faults to recover from")
    if data.t0 is None or any(source.get("service") not in led.gens for source in led.sources):
        return Result("recovery", SKIP, "no telemetry from a generator or no timeline start")
    sut = led.sut
    # The baseline sums each generator's own warmup rate, each over its own drains.
    baseline = 0.0
    behind = 0
    for source in led.sources:
        gen, load = led.gens[source.get("service")], source.get("generator_input")
        warm = [(ts, v) for ts, v, _ in gen.counter_points("logit.component.events.sent",
                                                           component=load)
                if data.t0 <= ts <= data.t0 + data.warmup]
        if len(warm) < 2 or warm[-1][0] <= warm[0][0]:
            return Result("recovery", SKIP, f"too few {source.get('service')} drains in warmup "
                          "for a baseline")
        baseline += sum(v for _, v in warm[1:]) / (warm[-1][0] - warm[0][0])
        behind += sum(v for _, v, _ in gen.counter_points("logit.component.diagnostics",
                                                          component=load, key="rate_behind"))
    gauges = {sink: {name: sut.gauge_series(f"logit.component.{name}", component=sink)
                     for name in ("retrying", "buffer.utilization", "buffer.batches")}
              for sink in led.sinks}
    receive_util = {listener: sut.gauge_series("logit.component.receive.utilization",
                                               component=listener)
                    for listener in led.listeners}
    ingest = {}
    for listener in led.listeners:
        for ts, v, _ in sut.counter_points("logit.input.datagrams", component=listener):
            ingest[ts] = ingest.get(ts, 0) + v
    life_of = {p.ts: p.life for p in sut.points if p.name == telemetry.UPTIME}
    drains = sorted(life_of)

    def assess(sink, start, end):
        """(problems, rate problem or None, rate, share) for one sink over the drain interval
        (start, end]."""
        problems = []
        retrying = gauges[sink]["retrying"]
        buffer_batches = gauges[sink]["buffer.batches"]
        if _step_value(retrying, end) != 0:
            problems.append(f"retrying {_step_value(retrying, end):g}")
        receive = [(f"{listener} receive.utilization" if len(receive_util) > 1
                    else "receive.utilization", series)
                   for listener, series in receive_util.items()]
        for label, series in [("buffer.utilization", gauges[sink]["buffer.utilization"])] + receive:
            value = _step_value(series, end)
            # One batch in a small queue is a large share of it: a batch the sink holds
            # while it sends is recovered at any `max_batches`, unless that batch fills it.
            if (label == "buffer.utilization" and value < 1.0
                    and _step_value(buffer_batches, end) <= 1):
                continue
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
        for sink in led.sinks:
            last = None
            for a, b in eligible:
                problems, slow, rate, share = assess(sink, a, b)
                if not problems and slow is None:
                    lines.append(f"{name}: {sink} recovered at {data.offset(b)}, "
                                 f"{b - fault.end:.0f}s after its end, ingest {rate:,.0f}/s "
                                 f"({share:.0%})")
                    break
                last = (b, problems, slow)
            else:
                b, problems, slow = last
                verdict = FAIL if problems else (WARN if behind > 0 else FAIL)
                if slow is not None:
                    problems.append(slow + (f", but a generator logged rate_behind "
                                            f"{_n(behind)} time(s)" if behind > 0 else ""))
                status = worst([status, verdict])
                lines.append(f"{name}: {sink} not recovered by {data.offset(b)}: "
                             f"{'; '.join(problems)}")
    if judged == 0:
        return Result("recovery", WARN, f"none of {len(data.faults)} fault(s) had an eligible "
                      f"drain interval within recovery_bound {data.recovery_bound:g}s of its "
                      "end", lines)
    failing = [line for line in lines if "not recovered" in line]
    detail = (f"{judged} of {len(data.faults)} fault(s) judged at {len(led.sinks)} sink(s), "
              f"each recovered within recovery_bound {data.recovery_bound:g}s of its end; "
              f"warmup rate {baseline:,.0f}/s")
    if failing:
        detail = f"{failing[0]}; {judged} of {len(data.faults)} fault(s) judged"
    return Result("recovery", status, detail, lines)


# ---- per-scenario expectations --------------------------------------------------------------------
#
# A scenario's `[[expect]]` tables, copied into `scenario.resolved.json`'s `expect`. The plan's
# "The scenario schema" has the fields; `scenario._expect_problems()` validates them.


def expect_window(data, fault, window):
    """`(start, end]` of an expectation's window around one fault occurrence: `during` is the
    fault's span, apply start to revert finish; `after` is the `recovery_bound` after it; and
    `through` is both, for a bound that must hold until the effect ends, since a blocked chain
    stays blocked until the sink's next retry after the revert. The window is open at its start
    because a drain's deltas cover the interval before it. `run` takes no fault: it is warmup's
    end through the start of the end sequence."""
    if window == RUN_WINDOW:
        return data.t0 + data.warmup, data.end
    if window == "during":
        return fault.start, fault.end
    if window == "through":
        return fault.start, fault.end + data.recovery_bound
    return fault.end, fault.end + data.recovery_bound


def reduce_counter(tel, metric, attrs, start, end, reduce):
    """`(value, note)` for a `sum` metric over the drains timestamped in (start, end]: `delta`
    sums them, and `min_delta` is the smallest single drain's delta, a drain with no point
    counting 0, since a loss counter isn't emitted when it's zero. `value` is None when no
    drain falls in the window."""
    per_drain = {ts: value for ts, value, _ in tel.counter_points(metric, **attrs)}
    drains = [d for d in sorted(set(tel.drains) | set(per_drain)) if start < d <= end]
    if not drains:
        return None, "no drain in the window"
    values = [per_drain.get(d, 0) for d in drains]
    if reduce == "delta":
        return sum(values), f"over {len(drains)} drain(s)"
    return min(values), f"smallest of {len(drains)} drain(s)"


def _life_at(tel, t):
    """The process life whose drains cover `t`, or None when no running life does: before the
    first drain, or after a life's last drain and before the next life starts."""
    for life in range(len(tel.life_starts) - 1, -1, -1):
        drains = tel.life_drains(life)
        if drains and drains[0] <= t:
            return life if t <= drains[-1] else None
    return None


def reduce_gauge(tel, metric, attrs, start, end, reduce):
    """`(value, note)` for a gauge over (start, end]: the value in force at `start` and every
    sample in the window. The value in force comes from the life whose drains cover `start`
    only (a gauge is exported only in a drain after it was set), so a gauge a previous process
    set reads as unset in the next. `last` is the value in force at `end`. `value` is None when
    the gauge was never set by `end`.

    `slope` is the least-squares slope of the window's own samples, in the metric's unit per
    hour, fitted per process life, since a restart resets a process gauge: `value` is
    `{life: per_hour}` over the lives with at least `SLOPE_MIN_SAMPLES` samples spanning
    `SLOPE_MIN_SPAN_S`, and None when no life has them."""
    series = tel.gauge_series(metric, **attrs)
    if reduce == "slope":
        by_life = {}
        for ts, v, life in series:
            if start < ts <= end:
                by_life.setdefault(life, []).append((ts, v))
        fitted, notes = {}, []
        for life, points in sorted(by_life.items()):
            span = points[-1][0] - points[0][0]
            fit = slope(points)
            if len(points) < SLOPE_MIN_SAMPLES or span < SLOPE_MIN_SPAN_S or fit is None:
                notes.append(f"life {life} {len(points)} sample(s) over {span:.0f}s, too few")
                continue
            fitted[life] = fit * SECONDS_PER_HOUR
            notes.append(f"life {life} {len(points)} samples over {span:.0f}s")
        if not fitted:
            return None, ("too few samples to fit a slope (at least "
                          f"{SLOPE_MIN_SAMPLES} over {SLOPE_MIN_SPAN_S}s in one life)"
                          + (f": {', '.join(notes)}" if notes else ""))
        return fitted, ", ".join(notes)
    life = _life_at(tel, start)
    before = [v for ts, v, point_life in series if ts <= start and point_life == life]
    inside = [v for ts, v, _ in series if start < ts <= end]
    values = before[-1:] + inside
    if not values:
        return None, "no sample by the window's end"
    note = f"{len(inside)} sample(s)" + (" and the value in force at its start" if before else "")
    if reduce == "last":
        return values[-1], note
    return (max(values) if reduce == "max" else min(values)), note


def _expect_occurrences(data, exp):
    """`(scheduled ids, faults)`: the step ids the resolved schedule has for this expectation,
    and the faults among them whose apply returned 0. A `run` window has one occurrence, the
    run itself, with no fault."""
    if exp.get("window") == RUN_WINDOW:
        return [RUN_WINDOW], [None]
    step = exp.get("step")
    scheduled = [s["id"] for s in data.resolved.get("steps", [])
                 if s.get("id") == step or (isinstance(step, int) and not isinstance(step, bool)
                                            and s.get("spec_index") == step - 1)]
    faults = [f for f in data.faults if f.step in scheduled]
    return scheduled, faults


def _expect_one(data, exp):
    name = exp.get("name") or "?"
    check_id = f"expect.{name}"
    service, metric, reduce = exp.get("service"), exp.get("metric"), exp.get("reduce")
    attrs = dict(exp.get("attrs") or {})
    lo, hi = exp.get("min"), exp.get("max")
    want = " and ".join(part for part in (
        None if lo is None else f">= {lo:g}", None if hi is None else f"<= {hi:g}") if part)
    where = (RUN_WINDOW if exp.get("window") == RUN_WINDOW
             else f"{exp.get('window')} {exp.get('step')}")
    label = (f"{reduce} of {metric}"
             + (f"{{{', '.join(f'{k}={v}' for k, v in sorted(attrs.items()))}}}" if attrs else "")
             + f" on {service}, {where}")
    scheduled, faults = _expect_occurrences(data, exp)
    if not scheduled:
        return Result(check_id, SKIP, f"{label}: the run's schedule has no such step")
    tel = data.telemetry.get(service)
    if tel is None:
        return Result(check_id, FAIL, f"{label}: no {service} telemetry")
    if exp.get("window") == RUN_WINDOW and data.t0 is None:
        return Result(check_id, FAIL, f"{label}: no timeline start, so no run window")
    kind = EXPECT_REDUCERS.get(reduce, "gauge")
    seen = {p.kind for p in tel.matching(metric)}
    if seen and kind not in seen:
        return Result(check_id, FAIL, f"{label}: reduce {reduce} reads a {kind}, and the run's "
                      f"{metric} points are {', '.join(sorted(seen))}")
    status = PASS
    lines = []
    applied = {RUN_WINDOW} if faults == [None] else {f.step for f in faults}
    for step_id in scheduled:
        if step_id not in applied:
            status = FAIL
            lines.append(f"{step_id}: never applied cleanly, so it has no window")
    def within(value):
        return (lo is None or value >= lo) and (hi is None or value <= hi)

    for fault in faults:
        start, end = expect_window(data, fault, exp.get("window"))
        reducer = reduce_counter if kind == "sum" else reduce_gauge
        value, note = reducer(tel, metric, attrs, start, end, reduce)
        name = RUN_WINDOW if fault is None else fault.step
        span = f"({data.offset(start)}, {data.offset(end)}]"
        if value is None:
            status = FAIL
            lines.append(f"{name} {span}: {note}")
            continue
        if isinstance(value, dict):
            # A slope per life, each judged against the bounds.
            parts = []
            for life, per_hour in sorted(value.items()):
                ok = within(per_hour)
                if not ok:
                    status = FAIL
                parts.append(f"life {life} {_n(per_hour)}/h" + ("" if ok else " FAIL"))
            lines.append(f"{name} {span}: {', '.join(parts)} ({note}), want {want}")
            continue
        ok = within(value)
        if not ok:
            status = FAIL
        shown = _n(value) if float(value).is_integer() else f"{value:.3f}"
        lines.append(f"{name} {span}: {shown} ({note}), want {want}"
                     + ("" if ok else ", FAIL"))
    detail = f"{label}: " + "; ".join(lines)
    return Result(check_id, status, detail, lines)


def check_expect(data):
    """One row per `[[expect]]`, `expect.<name>`: PASS when the reduced value lies within
    `min` and `max` in every occurrence of its step, FAIL otherwise. SKIPs as one `expect` row
    when the scenario has none, and per row when the run's schedule doesn't have the step."""
    expectations = data.resolved.get("expect") or []
    if not expectations:
        return [Result("expect", SKIP, "the scenario has no [[expect]] tables")]
    return [_expect_one(data, exp) for exp in expectations]


CHECKS = (
    check_run, check_timeline, check_exit, check_restarts, check_self_log, check_ready,
    check_progress, check_rss_slope, check_fd_slope, check_ledger_wire, check_ledger_intake,
    check_ledger_edge, check_ledger_script, check_ledger_aggregate, check_ledger_egress,
    check_ledger_windows, check_ledger_replay, check_ledger_summary, check_ledger_sent,
    check_ledger_topic, check_identity_sink, check_recovery, check_expect,
)


def run_all(run_dir):
    data = RunData(run_dir)
    results = []
    for check in CHECKS:
        try:
            result = check(data)
            results.extend(result if isinstance(result, list) else [result])
        except Exception as err:  # a checker bug must not hide the other rows
            results.append(Result(check.__name__.removeprefix("check_"), FAIL,
                                  f"checker error: {type(err).__name__}: {err}"))
    return results
