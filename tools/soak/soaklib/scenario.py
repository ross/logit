"""Loads, validates, and expands a scenario's `scenario.toml`.

The schema and the validation rules are docs/plans/soak-harness.md's "The scenario schema";
`validate()` is their canonical copy in code, and the self-test exercises every rule.

Times in a scenario are durations (`"90s"`, `"6m30s"`, `"1h"`). Step offsets (`at`) are relative
to the start of each cycle, and cycles start after `warmup`. `expand(duration)` repeats the cycle
while it fits before `cooldown`; a last cycle that doesn't fit whole keeps only the steps whose
fault ends before `cooldown` starts, so a short `--duration` still runs the schedule's head.
"""

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

# The services compose.yaml defines, and the two that run `logit`.
SERVICES = ("logit", "generator", "victoria-metrics")
LOGIT_SERVICES = ("logit", "generator")

# Every action the scenario schema accepts. faults.py implements them; the names must match its
# ACTIONS.
ACTION_NAMES = ("netem", "pause", "stop", "kill", "restart", "partition")
# Actions that only make sense against a `logit` process: a `kill` exists to end one with no
# shutdown, and the watchdog judges its exit code only for `logit` services.
LOGIT_ONLY_ACTIONS = ("kill",)

# Faults during which a container's network namespace is gone or unusable for a one-shot
# `docker run --network container:<id>`.
NAMESPACE_FAULTS = ("stop", "kill", "pause", "partition", "restart")

TOP_LEVEL_KEYS = {
    "name", "description", "duration", "warmup", "cooldown", "recovery_bound", "cycle",
    "configs", "ledger", "thresholds", "step", "expect",
}
STEP_KEYS = {"at", "action", "on", "args", "for"}
CONFIG_KEYS = {"sut", "generator"}
LEDGER_KEYS = {
    "vm_selector", "generator_input", "generator_sink", "sut_listener", "sut_aggregate",
    "sut_sink", "wire_loss_outside_faults",
}
THRESHOLD_KEYS = {"progress_window", "rss_growth_mib_per_hour", "fd_growth"}
EXPECT_KEYS = {"name", "service", "metric", "component", "attrs", "step", "window", "reduce",
               "min", "max"}
EXPECT_WINDOWS = ("during", "after", "through")
# Each reducer reads one metric kind: a counter (`sum`, a delta per drain) or a gauge.
EXPECT_REDUCERS = {"delta": "sum", "min_delta": "sum", "max": "gauge", "min": "gauge",
                   "last": "gauge"}
# Kinds of the metrics the shipped expectations name, so `validate()` refuses a reducer of the
# wrong kind before a run. docs/design/internal-telemetry.md is the canonical list; a metric
# missing here is checked against the kind its points carry when the run is scored.
METRIC_KINDS = {
    "logit.input.datagrams": "sum",
    "logit.input.kernel.drops": "sum",
    "logit.component.datagrams.dropped": "sum",
    "logit.component.batches.dropped": "sum",
    "logit.component.batches.delivered": "sum",
    "logit.component.inbox.full": "sum",
    "logit.component.retries": "sum",
    "logit.component.buffer.batches": "gauge",
    "logit.component.buffer.utilization": "gauge",
    "logit.component.receive.datagrams": "gauge",
    "logit.component.receive.utilization": "gauge",
    "logit.component.retrying": "gauge",
    "logit.component.buffer.disk.replayed": "sum",
    "logit.component.buffer.disk.segments": "gauge",
}
_EXPECT_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_STEP_ID = re.compile(r"^c(\d+)s(\d+)$")

_DURATION_PART = re.compile(r"(\d+(?:\.\d+)?)(ms|h|m|s)")
_UNIT_SECONDS = {"h": 3600.0, "m": 60.0, "s": 1.0, "ms": 0.001}


class ScenarioError(Exception):
    """A scenario that can't run. `problems` lists every rule it broke."""

    def __init__(self, problems):
        self.problems = list(problems)
        super().__init__("; ".join(self.problems))


def parse_duration(text):
    """`"6m30s"` -> 390.0 seconds. Units `h`, `m`, `s`, `ms`, largest first, each once; a bare
    number is refused, so a missing unit can't read as seconds by accident."""
    if not isinstance(text, str) or not text:
        raise ValueError(f"not a duration: {text!r}")
    pos = 0
    total = 0.0
    seen = []
    order = ["h", "m", "s", "ms"]
    for match in _DURATION_PART.finditer(text):
        if match.start() != pos:
            break
        unit = match.group(2)
        if seen and order.index(unit) <= order.index(seen[-1]):
            raise ValueError(f"not a duration: {text!r} (units must be distinct, largest first)")
        seen.append(unit)
        total += float(match.group(1)) * _UNIT_SECONDS[unit]
        pos = match.end()
    if pos != len(text) or not seen:
        raise ValueError(f"not a duration: {text!r}")
    return total


def format_duration(seconds):
    """390.0 -> `"6m30s"`; for messages and the resolved schedule."""
    seconds = round(seconds, 3)
    whole = int(seconds)
    frac = seconds - whole
    hours, rest = divmod(whole, 3600)
    minutes, secs = divmod(rest, 60)
    out = ""
    if hours:
        out += f"{hours}h"
    if minutes:
        out += f"{minutes}m"
    if secs or frac or not out:
        out += f"{secs + frac:g}s"
    return out


@dataclass
class StepSpec:
    """One `[[step]]` as written: offsets within a cycle, in seconds."""

    index: int
    at: float
    action: str
    on: str
    args: str
    for_: float


@dataclass
class Step:
    """One fault occurrence in an expanded schedule. `start` and `end` are seconds after the
    stack came up (the timeline's zero): `warmup + cycle * n + at`, and `start + for`."""

    id: str
    spec_index: int
    cycle: int
    action: str
    on: str
    args: str
    start: float
    end: float

    def to_json(self):
        return {
            "id": self.id, "spec_index": self.spec_index, "cycle": self.cycle,
            "action": self.action, "on": self.on, "args": self.args,
            "start": self.start, "end": self.end,
        }


@dataclass
class Expectation:
    """One `[[expect]]`: a bound on one metric, reduced over a window around a step.

    `step` is an expanded step id (`"c0s1"`) or a 1-based `[[step]]` index (an int), which
    names that step's occurrence in every cycle. `attrs` holds every attribute filter,
    `component` included."""

    index: int
    name: str
    service: str
    metric: str
    attrs: dict
    step: object
    window: str
    reduce: str
    min: object = None
    max: object = None

    def to_json(self):
        return {"name": self.name, "service": self.service, "metric": self.metric,
                "attrs": dict(self.attrs), "step": self.step, "window": self.window,
                "reduce": self.reduce, "min": self.min, "max": self.max}


@dataclass
class Scenario:
    path: Path
    name: str
    description: str
    duration: float
    warmup: float
    cooldown: float
    recovery_bound: float
    cycle: float
    configs: dict
    ledger: dict
    thresholds: dict
    steps: list = field(default_factory=list)
    expectations: list = field(default_factory=list)

    @property
    def directory(self):
        return self.path.parent

    def config_path(self, role):
        return (self.directory / self.configs[role]).resolve()

    def to_json(self, duration=None):
        duration = self.duration if duration is None else duration
        return {
            "name": self.name,
            "description": self.description,
            "path": str(self.path),
            "duration": duration,
            "warmup": self.warmup,
            "cooldown": self.cooldown,
            "recovery_bound": self.recovery_bound,
            "cycle": self.cycle,
            "configs": dict(self.configs),
            "ledger": dict(self.ledger),
            "thresholds": {
                "progress_window": self.thresholds["progress_window"],
                "rss_growth_mib_per_hour": self.thresholds["rss_growth_mib_per_hour"],
                "fd_growth": self.thresholds["fd_growth"],
            },
            "steps": [step.to_json() for step in expand(self, duration)],
            "expect": [e.to_json() for e in self.expectations],
        }


def load(path):
    """Parses `path` and returns a `Scenario`, raising `ScenarioError` for anything `validate()`
    would refuse, so a loaded scenario is always a runnable one."""
    path = Path(path)
    try:
        raw = tomllib.loads(path.read_text())
    except (OSError, tomllib.TOMLDecodeError) as err:
        raise ScenarioError([f"{path}: {err}"]) from None
    return from_dict(raw, path)


def from_dict(raw, path):
    problems = []

    def need(table, key, where):
        if key not in table:
            problems.append(f"{where}: missing `{key}`")
            return None
        return table[key]

    def dur(table, key, where):
        value = need(table, key, where)
        if value is None:
            return 0.0
        try:
            return parse_duration(value)
        except ValueError as err:
            problems.append(f"{where}.{key}: {err}")
            return 0.0

    for key in sorted(set(raw) - TOP_LEVEL_KEYS):
        problems.append(f"unknown top-level key `{key}`")

    name = need(raw, "name", "scenario") or ""
    description = raw.get("description", "")
    duration = dur(raw, "duration", "scenario")
    warmup = dur(raw, "warmup", "scenario")
    cooldown = dur(raw, "cooldown", "scenario")
    recovery_bound = dur(raw, "recovery_bound", "scenario")
    cycle = dur(raw, "cycle", "scenario")

    configs = raw.get("configs", {})
    ledger = raw.get("ledger", {})
    thresholds = dict(raw.get("thresholds", {}))
    for table, keys, where in (
        (configs, CONFIG_KEYS, "configs"),
        (ledger, LEDGER_KEYS, "ledger"),
        (thresholds, THRESHOLD_KEYS, "thresholds"),
    ):
        if not isinstance(table, dict):
            problems.append(f"`{where}` must be a table")
            continue
        for key in sorted(keys - set(table)):
            problems.append(f"{where}: missing `{key}`")
        for key in sorted(set(table) - keys):
            problems.append(f"{where}: unknown key `{key}`")
    if "progress_window" in thresholds:
        thresholds["progress_window"] = dur(thresholds, "progress_window", "thresholds")
    for role in CONFIG_KEYS & set(configs):
        if not (path.parent / configs[role]).is_file():
            problems.append(f"configs.{role}: {configs[role]} not found beside {path.name}")

    steps = []
    for index, raw_step in enumerate(raw.get("step", [])):
        where = f"step {index + 1}"
        for key in sorted(set(raw_step) - STEP_KEYS):
            problems.append(f"{where}: unknown key `{key}`")
        steps.append(StepSpec(
            index=index,
            at=dur(raw_step, "at", where),
            action=raw_step.get("action", ""),
            on=raw_step.get("on", ""),
            args=raw_step.get("args", ""),
            for_=dur(raw_step, "for", where) if "for" in raw_step else 0.0,
        ))
        if "for" not in raw_step:
            problems.append(f"{where}: every fault needs `for`, so its revert is scheduled with it")
        if "action" not in raw_step:
            problems.append(f"{where}: missing `action`")
        if "on" not in raw_step:
            problems.append(f"{where}: missing `on`")

    expectations = []
    raw_expect = raw.get("expect", [])
    if not isinstance(raw_expect, list):
        problems.append("`expect` must be an array of tables, written [[expect]]")
        raw_expect = []
    for index, raw_one in enumerate(raw_expect):
        where = f"expect {index + 1}"
        if not isinstance(raw_one, dict):
            problems.append(f"{where}: must be a table")
            continue
        for key in sorted(set(raw_one) - EXPECT_KEYS):
            problems.append(f"{where}: unknown key `{key}`")
        for key in ("name", "service", "metric", "step", "window", "reduce"):
            if key not in raw_one:
                problems.append(f"{where}: missing `{key}`")
        attrs = raw_one.get("attrs", {})
        if not isinstance(attrs, dict) or not all(isinstance(v, str) for v in attrs.values()):
            problems.append(f"{where}: `attrs` must be a table of strings")
            attrs = {}
        attrs = dict(attrs)
        if "component" in raw_one:
            if "component" in attrs:
                problems.append(f"{where}: `component` is set both alone and in `attrs`")
            elif not isinstance(raw_one["component"], str):
                problems.append(f"{where}: `component` must be a string")
            else:
                attrs["component"] = raw_one["component"]
        expectations.append(Expectation(
            index=index, name=raw_one.get("name", ""), service=raw_one.get("service", ""),
            metric=raw_one.get("metric", ""), attrs=attrs, step=raw_one.get("step", ""),
            window=raw_one.get("window", ""), reduce=raw_one.get("reduce", ""),
            min=raw_one.get("min"), max=raw_one.get("max"),
        ))

    scenario = Scenario(
        path=path, name=name, description=description, duration=duration, warmup=warmup,
        cooldown=cooldown, recovery_bound=recovery_bound, cycle=cycle, configs=configs,
        ledger=ledger, thresholds=thresholds, steps=steps, expectations=expectations,
    )
    if problems:
        raise ScenarioError(problems)
    validate(scenario)
    return scenario


def validate(scenario, duration=None, seed=None):
    """Raises `ScenarioError` listing every rule `scenario` breaks for a run of `duration`
    seconds (the scenario's own when None)."""
    problems = []
    if seed is not None:
        problems.append(
            "--seed is refused until W4 adds a [random] table "
            "(docs/plans/soak-harness.md, \"Workstreams\")"
        )
    if scenario.cooldown < scenario.recovery_bound:
        problems.append(
            f"cooldown {format_duration(scenario.cooldown)} is shorter than recovery_bound "
            f"{format_duration(scenario.recovery_bound)}"
        )
    if scenario.cycle <= 0:
        problems.append("cycle must be longer than 0s")
    if scenario.thresholds.get("progress_window", 0) <= 0:
        problems.append("thresholds.progress_window must be longer than 0s")

    for spec in scenario.steps:
        where = f"step {spec.index + 1} ({spec.action or '?'} on {spec.on or '?'})"
        if spec.action not in ACTION_NAMES:
            hint = ""
            if spec.action in ("clear", "unpause", "start", "connect"):
                hint = "; reverts are scheduled from a fault's `for`, never written as steps"
            problems.append(f"{where}: unknown action `{spec.action}`{hint}")
        if spec.on not in SERVICES:
            problems.append(f"{where}: unknown service `{spec.on}`")
        elif spec.action in LOGIT_ONLY_ACTIONS and spec.on not in LOGIT_SERVICES:
            problems.append(f"{where}: {spec.action} is for {' or '.join(LOGIT_SERVICES)} only")
        if spec.for_ <= 0:
            problems.append(f"{where}: `for` must be longer than 0s")
        if spec.action == "netem":
            words = spec.args.split()
            if not words:
                problems.append(f"{where}: netem needs `args`")
            elif words[0] in ("clear", "del", "delete"):
                problems.append(f"{where}: netem clear is a fault's revert, never a step")
        elif spec.args:
            problems.append(f"{where}: `args` is for netem only")
        if spec.at + spec.for_ > scenario.cycle:
            problems.append(
                f"{where}: ends at {format_duration(spec.at + spec.for_)}, after the "
                f"{format_duration(scenario.cycle)} cycle"
            )

    # Two faults on one container may not overlap. A netem overlapping a stop, kill, pause,
    # restart, or partition of its container gets its own message: the qdisc is lost with the
    # namespace, and a one-shot netem container needs a running target.
    by_service = {}
    for spec in scenario.steps:
        by_service.setdefault(spec.on, []).append(spec)
    for service, specs in by_service.items():
        specs = sorted(specs, key=lambda s: s.at)
        for i, first in enumerate(specs):
            for second in specs[i + 1:]:
                if second.at < first.at + first.for_:
                    pair = {first.action, second.action}
                    if "netem" in pair and pair & set(NAMESPACE_FAULTS):
                        problems.append(
                            f"steps {first.index + 1} and {second.index + 1}: netem on "
                            f"{service} during its {(pair - {'netem'}).pop()}"
                        )
                    else:
                        problems.append(
                            f"steps {first.index + 1} and {second.index + 1}: two faults "
                            f"overlap on {service}"
                        )

    problems += _expect_problems(scenario, duration)

    run_duration = scenario.duration if duration is None else duration
    quiet_end = run_duration - scenario.cooldown
    if quiet_end < scenario.warmup:
        problems.append(
            f"duration {format_duration(run_duration)} leaves no time between warmup "
            f"{format_duration(scenario.warmup)} and cooldown {format_duration(scenario.cooldown)}"
        )
    if duration is None and scenario.steps and scenario.warmup + scenario.cycle > quiet_end:
        problems.append(
            f"the scenario's own duration {format_duration(run_duration)} doesn't fit one whole "
            f"cycle: warmup + cycle ends at {format_duration(scenario.warmup + scenario.cycle)}, "
            f"after cooldown starts at {format_duration(quiet_end)}"
        )
    if problems:
        raise ScenarioError(problems)


def _expect_problems(scenario, duration):
    """Every rule the `[[expect]]` tables break. A step id is checked against the scenario's
    own schedule; a shorter `--duration` that drops the step leaves the expectation to SKIP."""
    problems = []
    names = set()
    scheduled = {step.id for step in expand(scenario)} if scenario.cycle > 0 else set()
    for exp in scenario.expectations:
        where = f"expect {exp.index + 1} ({exp.name or '?'})"
        if not isinstance(exp.name, str) or not _EXPECT_NAME.match(exp.name):
            problems.append(f"{where}: `name` must be lowercase letters, digits, `_`, or `-`")
        elif exp.name in names:
            problems.append(f"{where}: `name` repeats another expectation's")
        else:
            names.add(exp.name)
        if exp.service not in LOGIT_SERVICES:
            problems.append(f"{where}: unknown service `{exp.service}`; telemetry comes from "
                            f"{' or '.join(LOGIT_SERVICES)}")
        if not isinstance(exp.metric, str) or not exp.metric:
            problems.append(f"{where}: `metric` must name a metric")
        if exp.window not in EXPECT_WINDOWS:
            problems.append(f"{where}: unknown window `{exp.window}`; one of "
                            f"{', '.join(EXPECT_WINDOWS)}")
        kind = EXPECT_REDUCERS.get(exp.reduce) if isinstance(exp.reduce, str) else None
        if kind is None:
            problems.append(f"{where}: unknown reduce `{exp.reduce}`; one of "
                            f"{', '.join(EXPECT_REDUCERS)}")
        elif isinstance(exp.metric, str) and METRIC_KINDS.get(exp.metric, kind) != kind:
            problems.append(f"{where}: reduce `{exp.reduce}` reads a {kind}, and {exp.metric} "
                            f"is a {METRIC_KINDS[exp.metric]}")
        bounds = [b for b in (exp.min, exp.max) if b is not None]
        if not bounds:
            problems.append(f"{where}: needs `min`, `max`, or both")
        elif not all(isinstance(b, (int, float)) and not isinstance(b, bool) for b in bounds):
            problems.append(f"{where}: `min` and `max` must be numbers")
        elif len(bounds) == 2 and exp.min > exp.max:
            problems.append(f"{where}: `min` {exp.min:g} is above `max` {exp.max:g}")
        match = _STEP_ID.match(exp.step) if isinstance(exp.step, str) else None
        if isinstance(exp.step, int) and not isinstance(exp.step, bool):
            if not 1 <= exp.step <= len(scenario.steps):
                problems.append(f"{where}: step index {exp.step} is outside the scenario's "
                                f"{len(scenario.steps)} [[step]] table(s)")
        elif match:
            if not 1 <= int(match.group(2)) <= len(scenario.steps):
                problems.append(f"{where}: step `{exp.step}` names no [[step]] table")
            elif duration is None and exp.step not in scheduled:
                problems.append(f"{where}: step `{exp.step}` isn't in the scenario's own "
                                "schedule")
        else:
            problems.append(f"{where}: `step` must be a step id such as \"c0s1\" or a "
                            "1-based [[step]] index")
    return problems


def expand(scenario, duration=None):
    """The fault occurrences a run of `duration` seconds performs, in start order. No fault
    ends after `duration - cooldown`."""
    duration = scenario.duration if duration is None else duration
    quiet_end = duration - scenario.cooldown
    steps = []
    cycle = 0
    while scenario.cycle > 0:
        base = scenario.warmup + cycle * scenario.cycle
        if base >= quiet_end:
            break
        for spec in scenario.steps:
            start = base + spec.at
            end = start + spec.for_
            if end <= quiet_end:
                steps.append(Step(
                    id=f"c{cycle}s{spec.index + 1}", spec_index=spec.index, cycle=cycle,
                    action=spec.action, on=spec.on, args=spec.args, start=start, end=end,
                ))
        cycle += 1
    steps.sort(key=lambda s: (s.start, s.spec_index))
    return steps


def shipped(root):
    """Every scenario directory under tools/soak/scenarios/, sorted, as `scenario.toml` paths."""
    base = Path(root) / "scenarios"
    return sorted(p / "scenario.toml" for p in base.iterdir() if (p / "scenario.toml").is_file())
