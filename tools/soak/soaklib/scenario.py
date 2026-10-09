"""Loads, validates, and expands a scenario's `scenario.toml`.

The schema and the validation rules are docs/plans/soak-harness.md's "The scenario schema";
`validate()` is their canonical copy in code, and the self-test exercises every rule.

Times in a scenario are durations (`"90s"`, `"6m30s"`, `"1h"`). A scenario's schedule is either
fixed or random:

- Fixed: `cycle` and `[[step]]` tables. Step offsets (`at`) are relative to the start of each
  cycle, and cycles start after `warmup`. `expand(duration)` repeats the cycle while it fits
  before `cooldown`; a last cycle that doesn't fit whole keeps only the steps whose fault ends
  before `cooldown` starts, so a short `--duration` still runs the schedule's head.
- Random: a `[random]` table. `expand(duration, seed)` draws one fault at a time from
  `random.Random(seed)`: a gap, a fault template by weight, its `for`, and for netem one of its
  `args`. Faults never overlap, and each is preceded by a gap of at least `gap.min`, which
  `validate()` holds at `recovery_bound + 2 x progress_window` or more, so every fault's recovery
  is followed by judged steady state. The first fault whose end would pass the start of
  `cooldown` ends the schedule, so a shorter run's schedule is a prefix of a longer one's for the
  same seed. Draws use `Random.random()` only, whose sequence for an integer seed is stable
  across Python versions; `randrange()` and `choices()` are not promised to be.
"""

import random
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
# Actions allowed on the SUT only. A `kill` exists to end a `logit` process with no shutdown;
# on the generator, the lines it sent after its last telemetry drain reach the SUT with no G
# behind them, and `ledger.wire` would net them against real loss.
SUT_ONLY_ACTIONS = ("kill",)

# Faults during which a container's network namespace is gone or unusable for a one-shot
# `docker run --network container:<id>`.
NAMESPACE_FAULTS = ("stop", "kill", "pause", "partition", "restart")

TOP_LEVEL_KEYS = {
    "name", "description", "duration", "warmup", "cooldown", "recovery_bound", "cycle",
    "configs", "ledger", "thresholds", "step", "expect", "random",
}
STEP_KEYS = {"at", "action", "on", "args", "for"}
RANDOM_KEYS = {"seed", "gap", "fault"}
RANDOM_FAULT_KEYS = {"weight", "action", "on", "args", "for"}
RANGE_KEYS = {"min", "max"}
CONFIG_KEYS = {"sut", "generator"}
LEDGER_KEYS = {
    "vm_selector", "generator_input", "generator_sink", "sut_listener", "sut_aggregate",
    "sut_sink", "wire_loss_outside_faults",
}
OPTIONAL_LEDGER_KEYS = {"vm_every_window"}
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
    "logit.component.buffer.disk.truncated": "sum",
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
class FaultTemplate:
    """One `[[random.fault]]`: an allowed fault, its weight, the range its `for` is drawn from,
    and, for netem, the `args` it picks from. Times in seconds."""

    index: int
    weight: float
    action: str
    on: str
    args: list
    for_min: float
    for_max: float

    def to_json(self):
        return {"weight": self.weight, "action": self.action, "on": self.on,
                "args": list(self.args), "for": {"min": self.for_min, "max": self.for_max}}


@dataclass
class RandomSpec:
    """The `[random]` table: the default seed, the gap range in seconds, and the templates."""

    seed: int
    gap_min: float
    gap_max: float
    faults: list

    def to_json(self):
        return {"seed": self.seed, "gap": {"min": self.gap_min, "max": self.gap_max},
                "fault": [template.to_json() for template in self.faults]}


@dataclass
class Step:
    """One fault occurrence in an expanded schedule. `start` and `end` are seconds after the
    stack came up (the timeline's zero): `warmup + cycle * n + at`, and `start + for`. A random
    schedule's step has id `r<n>`, `cycle` None, and `spec_index` its template's index."""

    id: str
    spec_index: int
    cycle: object
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
    random: object = None

    @property
    def directory(self):
        return self.path.parent

    def config_path(self, role):
        return (self.directory / self.configs[role]).resolve()

    def seed_for(self, seed=None):
        """The seed a run uses: `seed` (from `--seed`) when given, else the `[random]` table's;
        None for a fixed schedule."""
        if self.random is None:
            return None
        return self.random.seed if seed is None else seed

    def to_json(self, duration=None, seed=None):
        """The resolved scenario a run records: its schedule expanded for `duration` and, for a
        random schedule, the seed it was drawn with, so `check` re-scores the run offline and
        `--seed` reproduces it."""
        duration = self.duration if duration is None else duration
        seed = self.seed_for(seed)
        return {
            "name": self.name,
            "description": self.description,
            "path": str(self.path),
            "duration": duration,
            "warmup": self.warmup,
            "cooldown": self.cooldown,
            "recovery_bound": self.recovery_bound,
            "cycle": None if self.random is not None else self.cycle,
            "seed": seed,
            "random": None if self.random is None else self.random.to_json(),
            "configs": dict(self.configs),
            "ledger": dict(self.ledger),
            "thresholds": {
                "progress_window": self.thresholds["progress_window"],
                "rss_growth_mib_per_hour": self.thresholds["rss_growth_mib_per_hour"],
                "fd_growth": self.thresholds["fd_growth"],
            },
            "steps": [step.to_json() for step in expand(self, duration, seed)],
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
    # A schedule is fixed (`cycle` and `[[step]]`) or random (`[random]`), never both.
    random_spec = None
    cycle = 0.0
    if "random" in raw:
        for key in ("cycle", "step"):
            if key in raw:
                problems.append(f"`{key}` is for a fixed schedule; a scenario has either "
                                "[random] or `cycle` with [[step]] tables, not both")
        if "expect" in raw:
            problems.append("[[expect]] names a step, and a [random] schedule's steps change "
                            "with the seed; the watchdog, ledger, and recovery rows judge a "
                            "random schedule")
        random_spec = _parse_random(raw["random"], problems, dur)
    else:
        cycle = dur(raw, "cycle", "scenario")

    configs = raw.get("configs", {})
    ledger = raw.get("ledger", {})
    thresholds = dict(raw.get("thresholds", {}))
    for table, keys, optional, where in (
        (configs, CONFIG_KEYS, set(), "configs"),
        (ledger, LEDGER_KEYS, OPTIONAL_LEDGER_KEYS, "ledger"),
        (thresholds, THRESHOLD_KEYS, set(), "thresholds"),
    ):
        if not isinstance(table, dict):
            problems.append(f"`{where}` must be a table")
            continue
        for key in sorted(keys - set(table)):
            problems.append(f"{where}: missing `{key}`")
        for key in sorted(set(table) - keys - optional):
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
        random=random_spec,
    )
    if problems:
        raise ScenarioError(problems)
    validate(scenario)
    return scenario


def _parse_random(table, problems, dur):
    """The `[random]` table as a `RandomSpec`, appending to `problems` what can't be parsed.
    Ranges and weights are checked against the schedule's rules in `validate()`."""
    if not isinstance(table, dict):
        problems.append("`random` must be a table, written [random]")
        return None
    for key in sorted(set(table) - RANDOM_KEYS):
        problems.append(f"random: unknown key `{key}`")
    seed = table.get("seed")
    if seed is None:
        problems.append("random: missing `seed`")
        seed = 0
    elif not isinstance(seed, int) or isinstance(seed, bool) or seed < 0:
        problems.append(f"random.seed must be an integer 0 or more, got {seed!r}")
        seed = 0

    def span(raw, where):
        if not isinstance(raw, dict):
            problems.append(f"{where} must be a table such as {{ min = \"30s\", max = \"2m\" }}")
            return 0.0, 0.0
        for key in sorted(set(raw) - RANGE_KEYS):
            problems.append(f"{where}: unknown key `{key}`")
        return dur(raw, "min", where), dur(raw, "max", where)

    if "gap" not in table:
        problems.append("random: missing `gap`")
        gap_min = gap_max = 0.0
    else:
        gap_min, gap_max = span(table["gap"], "random.gap")
    raw_faults = table.get("fault")
    if not isinstance(raw_faults, list) or not raw_faults:
        problems.append("random: needs at least one [[random.fault]] table")
        raw_faults = []
    templates = []
    for index, raw in enumerate(raw_faults):
        where = f"random.fault {index + 1}"
        if not isinstance(raw, dict):
            problems.append(f"{where}: must be a table")
            continue
        for key in sorted(set(raw) - RANDOM_FAULT_KEYS):
            problems.append(f"{where}: unknown key `{key}`")
        for key in ("weight", "action", "on", "for"):
            if key not in raw:
                problems.append(f"{where}: missing `{key}`")
        weight = raw.get("weight", 0)
        if not isinstance(weight, (int, float)) or isinstance(weight, bool):
            problems.append(f"{where}: `weight` must be a number")
            weight = 0
        args = raw.get("args", [])
        if not isinstance(args, list) or not all(isinstance(a, str) for a in args):
            problems.append(f"{where}: `args` must be an array of netem specs, one picked per "
                            "occurrence")
            args = []
        for_min, for_max = span(raw["for"], f"{where}.for") if "for" in raw else (0.0, 0.0)
        templates.append(FaultTemplate(
            index=index, weight=float(weight), action=raw.get("action", ""),
            on=raw.get("on", ""), args=list(args), for_min=for_min, for_max=for_max,
        ))
    return RandomSpec(seed=seed, gap_min=gap_min, gap_max=gap_max, faults=templates)


def validate(scenario, duration=None, seed=None):
    """Raises `ScenarioError` listing every rule `scenario` breaks for a run of `duration`
    seconds (the scenario's own when None), drawn with `seed` (from `--seed`) for a random
    schedule."""
    problems = []
    if seed is not None and scenario.random is None:
        problems.append(
            f"--seed applies to a [random] schedule only; {scenario.name or 'this scenario'} "
            "runs fixed [[step]] tables, which a seed can't change"
        )
    elif seed is not None and (not isinstance(seed, int) or seed < 0):
        problems.append(f"--seed must be an integer 0 or more, got {seed!r}")
    if scenario.cooldown < scenario.recovery_bound:
        problems.append(
            f"cooldown {format_duration(scenario.cooldown)} is shorter than recovery_bound "
            f"{format_duration(scenario.recovery_bound)}"
        )
    if scenario.random is None and scenario.cycle <= 0:
        problems.append("cycle must be longer than 0s")
    if scenario.thresholds.get("progress_window", 0) <= 0:
        problems.append("thresholds.progress_window must be longer than 0s")
    if not isinstance(scenario.ledger.get("vm_every_window", False), bool):
        problems.append("ledger.vm_every_window must be true or false")

    for spec in scenario.steps:
        where = f"step {spec.index + 1} ({spec.action or '?'} on {spec.on or '?'})"
        if spec.action not in ACTION_NAMES:
            hint = ""
            if spec.action in ("clear", "unpause", "start", "connect"):
                hint = "; reverts are scheduled from a fault's `for`, never written as steps"
            problems.append(f"{where}: unknown action `{spec.action}`{hint}")
        if spec.on not in SERVICES:
            problems.append(f"{where}: unknown service `{spec.on}`")
        elif spec.action in SUT_ONLY_ACTIONS and spec.on != "logit":
            problems.append(
                f"{where}: {spec.action} is for logit only: on the generator, the lines it sent "
                "after its last telemetry drain reach the SUT with no G behind them, so "
                "ledger.wire would net them against real loss")
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
    if scenario.random is not None:
        template_problems = _random_problems(scenario, duration)
        problems += template_problems
        if not template_problems and not problems:
            # The generated schedules, at the scenario's own duration and the run's, meet every
            # rule above by construction; checking them keeps that a tested fact.
            for length in sorted({scenario.duration, run_duration}):
                steps = expand(scenario, length, scenario.seed_for(seed))
                problems += _schedule_problems(scenario, steps, length)
    if problems:
        raise ScenarioError(problems)


def _random_problems(scenario, duration):
    """Every rule a `[random]` table breaks. `gap.min` must cover a fault's recovery and two
    progress windows, so each fault is followed by steady state the checks judge, and `cooldown`
    must cover it too, for the last fault."""
    spec = scenario.random
    problems = []
    window = scenario.thresholds.get("progress_window", 0) or 0
    floor = scenario.recovery_bound + 2 * window
    if spec.gap_min > spec.gap_max:
        problems.append(f"random.gap.min {format_duration(spec.gap_min)} is above max "
                        f"{format_duration(spec.gap_max)}")
    if spec.gap_min < floor:
        problems.append(
            f"random.gap.min {format_duration(spec.gap_min)} is under recovery_bound "
            f"{format_duration(scenario.recovery_bound)} + 2 x progress_window "
            f"{format_duration(window)} = {format_duration(floor)}, so a fault's recovery could "
            "run into the next fault with no steady state judged between them")
    if scenario.cooldown < spec.gap_min:
        problems.append(
            f"cooldown {format_duration(scenario.cooldown)} is shorter than random.gap.min "
            f"{format_duration(spec.gap_min)}, so the last fault could leave no steady state "
            "judged after its recovery")
    for template in spec.faults:
        where = f"random.fault {template.index + 1} ({template.action or '?'} on " \
                f"{template.on or '?'})"
        if template.weight <= 0:
            problems.append(f"{where}: `weight` must be above 0")
        if template.action not in ACTION_NAMES:
            problems.append(f"{where}: unknown action `{template.action}`")
        if template.on not in SERVICES:
            problems.append(f"{where}: unknown service `{template.on}`")
        elif template.action in SUT_ONLY_ACTIONS and template.on != "logit":
            problems.append(f"{where}: {template.action} is for logit only")
        if template.for_min <= 0:
            problems.append(f"{where}: `for.min` must be longer than 0s")
        if template.for_min > template.for_max:
            problems.append(f"{where}: `for.min` {format_duration(template.for_min)} is above "
                            f"`for.max` {format_duration(template.for_max)}")
        if template.action == "netem":
            if not template.args:
                problems.append(f"{where}: netem needs `args`, the specs it picks from")
            for args in template.args:
                words = args.split()
                if not words:
                    problems.append(f"{where}: an empty netem spec")
                elif words[0] in ("clear", "del", "delete"):
                    problems.append(f"{where}: netem clear is a fault's revert, never a fault")
        elif template.args:
            problems.append(f"{where}: `args` is for netem only")
    if duration is None and spec.faults:
        longest = max(template.for_max for template in spec.faults)
        quiet_end = scenario.duration - scenario.cooldown
        if scenario.warmup + spec.gap_max + longest > quiet_end:
            problems.append(
                f"the scenario's own duration {format_duration(scenario.duration)} doesn't "
                f"fit every draw of one fault: warmup + gap.max + the longest for.max ends at "
                f"{format_duration(scenario.warmup + spec.gap_max + longest)}, after cooldown "
                f"starts at {format_duration(quiet_end)}")
    return problems


def _schedule_problems(scenario, steps, duration):
    """Every rule an expanded schedule breaks: known actions and services, a kill on the SUT
    only, each fault inside warmup's end and cooldown's start, no two faults overlapping on one
    container, no netem during a namespace fault, and, for a random schedule, no two faults
    overlapping anywhere, each after a gap of at least `gap.min`."""
    problems = []
    quiet_end = duration - scenario.cooldown
    for step in steps:
        where = f"step {step.id} ({step.action} on {step.on})"
        if step.action not in ACTION_NAMES:
            problems.append(f"{where}: unknown action")
        if step.on not in SERVICES:
            problems.append(f"{where}: unknown service")
        elif step.action in SUT_ONLY_ACTIONS and step.on != "logit":
            problems.append(f"{where}: {step.action} is for logit only")
        if step.end <= step.start:
            problems.append(f"{where}: ends at or before its start")
        if step.start < scenario.warmup:
            problems.append(f"{where}: starts inside warmup")
        if step.end > quiet_end:
            problems.append(f"{where}: ends inside cooldown")
    ordered = sorted(steps, key=lambda s: s.start)
    for i, first in enumerate(ordered):
        for second in ordered[i + 1:]:
            if second.start >= first.end:
                break
            if first.on != second.on:
                continue
            pair = {first.action, second.action}
            if "netem" in pair and pair & set(NAMESPACE_FAULTS):
                problems.append(f"steps {first.id} and {second.id}: netem on {first.on} during "
                                f"its {(pair - {'netem'}).pop()}")
            else:
                problems.append(f"steps {first.id} and {second.id}: two faults overlap on "
                                f"{first.on}")
    if scenario.random is not None:
        previous_end = scenario.warmup
        for step in ordered:
            gap = step.start - previous_end
            if gap < 0:
                problems.append(f"step {step.id}: starts before the previous fault ends; a "
                                "random schedule runs one fault at a time")
            elif gap < scenario.random.gap_min:
                problems.append(f"step {step.id}: starts {format_duration(gap)} after the "
                                "previous fault's end or warmup, under random.gap.min "
                                f"{format_duration(scenario.random.gap_min)}")
            previous_end = max(previous_end, step.end)
    return problems


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


def expand(scenario, duration=None, seed=None):
    """The fault occurrences a run of `duration` seconds performs, in start order. No fault
    ends after `duration - cooldown`. A random schedule is drawn with `seed`, the `[random]`
    table's when None; a fixed one ignores it."""
    duration = scenario.duration if duration is None else duration
    quiet_end = duration - scenario.cooldown
    if scenario.random is not None:
        return _expand_random(scenario, quiet_end, scenario.seed_for(seed))
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


def _draw(rng, low, high):
    """A whole number of seconds in [low, high], from one `rng.random()`; the driver ticks once
    a second, so a fraction would only move the step to the next tick."""
    return min(high, max(low, float(round(low + (high - low) * rng.random()))))


def _expand_random(scenario, quiet_end, seed):
    """One fault at a time from the end of warmup: a gap, a template by weight, its `for`, and
    for netem one of its `args`, in that order per fault, until a fault would end after
    `quiet_end`."""
    spec = scenario.random
    templates = [t for t in spec.faults if t.weight > 0]
    total = sum(t.weight for t in templates)
    if not templates or total <= 0:
        return []
    rng = random.Random(seed)
    steps = []
    t = scenario.warmup
    while True:
        gap = _draw(rng, spec.gap_min, spec.gap_max)
        pick = rng.random() * total
        template = templates[-1]
        for candidate in templates:
            pick -= candidate.weight
            if pick < 0:
                template = candidate
                break
        length = _draw(rng, template.for_min, template.for_max)
        args = ""
        if template.args:
            args = template.args[min(len(template.args) - 1,
                                     int(rng.random() * len(template.args)))]
        start = t + gap
        end = start + length
        if end > quiet_end or length <= 0:
            return steps
        steps.append(Step(
            id=f"r{len(steps) + 1}", spec_index=template.index, cycle=None,
            action=template.action, on=template.on, args=args, start=start, end=end,
        ))
        t = end


def shipped(root):
    """Every scenario directory under tools/soak/scenarios/, sorted, as `scenario.toml` paths."""
    base = Path(root) / "scenarios"
    return sorted(p / "scenario.toml" for p in base.iterdir() if (p / "scenario.toml").is_file())
