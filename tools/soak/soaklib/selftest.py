"""`soak.py self-test`: the driver's pure parts against embedded fixtures, plus `validate()` over
every shipped scenario. `run` calls it first, so a broken reducer or rule can't score a run.

It covers the duration parser, every `validate()` rejection rule, `expand()`'s cycle
repetition, the NDJSON reducers (counters per life split where uptime decreases, gauge series),
the stderr classifier (a raw `thread '...' panicked at` line included), the slope function, and
the checks against small synthetic run directories: a failed fault action, an aborted run and its
exit code, an fd sample inside a later fault, and a steady state too thin to judge. For the
ledger, it covers the reset-aware VictoriaMetrics total and its split per life, at a decrease
and at a life start with none, and a two-life run whose final life balances and whose earlier
life's residual reaches V, each rule broken in turn: shutdown drops on stderr, a counted and an
uncounted egress gap, an earlier life's residual that never reaches V, an empty export, wire loss
in and out of a fault window and at the end, the sink identity, and recovery, a two-batch sink
queue holding one batch and a one-batch queue it fills included. Empty SUT telemetry, and lives the timeline disagrees with,
SKIP every ledger row. For `[[expect]]`, it covers every validation rule and each reducer
against a synthetic run: the window's open start, `through` reaching past the revert, a drain
with no point, a gauge's value in force at the window's start and never across a SUT life,
attribute filters, a step index across cycles, a step the schedule lacks, and a step whose apply
failed. For a `kill`, it covers its validation rules (the SUT only), the watchdog around it
(exit code 137 only inside a scheduled kill, its start, its readiness probe, and the stdout gap
it leaves), and a three-life run whose middle life ends by a kill: its egress band at both
edges, with and without a residual, a surplus below it and a lost last window above it, a D
with no stderr part, the final life's sink identity counting a spool's replayed batches,
`ledger.windows` against lost middle, tail, and head windows, and `ledger.replay` against a
short replay. `ledger.wire` counts a steady run's surplus as 0, and `ledger.windows` excuses a
gap the aggregate sent no batch in, with the flushes reported on time or one drain late,
across a pause with no drains, and past 11 s after a partition, but FAILs one it sent a batch
in, the one window lost right before or right after a fault included.

For a `[random]` schedule, it covers every validation rule, a pinned head of the shipped
scenario's schedule (so a seed keeps reproducing a recorded run), determinism, and, for 12 seeds
at five durations up to 24 hours, that every drawn schedule passes `validate()`, honors its gap
and `for` ranges, leaves two progress windows after each recovery, and is a prefix of a longer
run's; the schedule rules against hand-built schedules; and where the seed is recorded. It also
covers the chunked log capture against a fake `docker logs` with out-of-order and late lines
and a failed chunk whose record keeps the CLI's error, the polls' absolute deadlines, a hung
daemon ending an inspect round, and a SIGTERM or SIGHUP inherited as ignored staying ignored.
"""

import copy
import json
import signal
import subprocess
import sys
import tempfile
import tomllib
from datetime import datetime, timezone
from pathlib import Path

from . import checks, collect, docker, driver, faults, report, scenario, telemetry, vm

_FAILURES = []
_PASSED = [0]


def expect(condition, what):
    if condition:
        _PASSED[0] += 1
    else:
        _FAILURES.append(what)


def _durations():
    cases = {"15m": 900, "6m30s": 390, "0s": 0, "1h": 3600, "250ms": 0.25, "1.5s": 1.5,
             "1h2m3s": 3723}
    for text, seconds in cases.items():
        expect(scenario.parse_duration(text) == seconds, f"parse_duration({text!r})")
    for bad in ("", "15", "5s3m", "3mm", "m", "1h1h", "10 s", "-5s"):
        try:
            scenario.parse_duration(bad)
            expect(False, f"parse_duration({bad!r}) accepted")
        except ValueError:
            expect(True, "")
    expect(scenario.format_duration(390) == "6m30s", "format_duration(390)")


def _base(root):
    path = root / "scenarios" / "statsd-vm" / "scenario.toml"
    return path, tomllib.loads(path.read_text())


def _refused(path, raw, needle, what, seed=None, duration=None):
    try:
        loaded = scenario.from_dict(raw, path)
        scenario.validate(loaded, duration=duration, seed=seed)
    except scenario.ScenarioError as err:
        expect(any(needle in problem for problem in err.problems),
               f"{what}: expected a problem containing {needle!r}, got {err.problems}")
        return
    expect(False, f"{what}: accepted")


def _rules(root):
    path, base = _base(root)

    def mutate(edit):
        raw = copy.deepcopy(base)
        edit(raw)
        return raw

    def drop_for(raw):
        del raw["step"][0]["for"]

    def overlap(raw):
        raw["step"].append({"at": "30s", "action": "pause", "on": "generator", "for": "10s"})
        raw["step"].append({"at": "35s", "action": "stop", "on": "generator", "for": "10s"})

    def netem_in_stop(raw):
        raw["step"].append({"at": "6m40s", "action": "netem", "on": "victoria-metrics",
                            "args": "delay 10ms", "for": "10s"})

    def netem_in_kill(raw):
        raw["step"].append({"at": "11m10s", "action": "kill", "on": "logit", "for": "20s"})
        raw["step"].append({"at": "11m20s", "action": "netem", "on": "logit",
                            "args": "delay 10ms", "for": "5s"})

    def kill_vm(raw):
        raw["step"].append({"at": "11m10s", "action": "kill", "on": "victoria-metrics",
                            "for": "20s"})

    def kill_generator(raw):
        raw["step"].append({"at": "11m10s", "action": "kill", "on": "generator", "for": "20s"})

    def kill_sut(raw):
        raw["step"].append({"at": "11m10s", "action": "kill", "on": "logit", "for": "20s"})

    _refused(path, mutate(netem_in_kill), "netem on logit during its kill",
             "netem during a kill")
    _refused(path, mutate(kill_vm), "kill is for logit only", "a kill on victoria-metrics")
    _refused(path, mutate(kill_generator), "with no G behind them", "a kill on the generator")
    try:
        loaded = scenario.from_dict(mutate(kill_sut), path)
        expect(loaded.steps[-1].action == "kill", "a kill on the SUT loads")
    except scenario.ScenarioError as err:
        expect(False, f"a kill on the SUT is refused: {err.problems}")
    _refused(path, mutate(lambda r: r["ledger"].update(vm_every_window="yes")),
             "ledger.vm_every_window must be true or false", "a non-boolean vm_every_window")
    try:
        loaded = scenario.from_dict(mutate(lambda r: r["ledger"].update(vm_every_window=True)),
                                    path)
        expect(loaded.ledger["vm_every_window"] is True, "vm_every_window = true loads")
    except scenario.ScenarioError as err:
        expect(False, f"vm_every_window = true is refused: {err.problems}")
    expect(set(faults.ACTIONS) == set(scenario.ACTION_NAMES),
           f"faults.ACTIONS and scenario.ACTION_NAMES name the same actions, got "
           f"{sorted(faults.ACTIONS)} vs {sorted(scenario.ACTION_NAMES)}")
    _refused(path, mutate(drop_for), "every fault needs `for`", "a step without for")
    _refused(path, mutate(overlap), "two faults overlap on generator", "overlapping faults")
    _refused(path, mutate(netem_in_stop), "netem on victoria-metrics during its stop",
             "netem during a stop")
    _refused(path, mutate(lambda r: r.update(cooldown="30s")), "shorter than recovery_bound",
             "cooldown under recovery_bound")
    _refused(path, mutate(lambda r: r["step"][0].update(at="12m")), "after the 13m cycle",
             "a step past its cycle")
    _refused(path, mutate(lambda r: r["step"][0].update(action="explode")), "unknown action",
             "an unknown action")
    _refused(path, mutate(lambda r: r["step"][0].update(action="clear")),
             "reverts are scheduled", "a revert written as a step")
    _refused(path, mutate(lambda r: r["step"][0].update(args="clear")),
             "netem clear is a fault's revert", "netem clear as a step")
    _refused(path, mutate(lambda r: r["step"][0].update(on="influxdb")), "unknown service",
             "an unknown service")
    _refused(path, mutate(lambda r: r["step"][3].update(args="loss 1%")), "for netem only",
             "args on a lifecycle fault")
    _refused(path, mutate(lambda r: r.update(duration="10m")),
             "doesn't fit one whole cycle", "a duration shorter than one cycle")
    _refused(path, mutate(lambda r: r.update(durration="10m")), "unknown top-level key",
             "a misspelled key")
    _refused(path, mutate(lambda r: r["ledger"].pop("vm_selector")),
             "ledger: missing `vm_selector`", "a missing ledger key")
    _refused(path, copy.deepcopy(base), "--seed applies to a [random] schedule only",
             "--seed on a fixed schedule", seed=7)
    _refused(path, copy.deepcopy(base), "leaves no time", "a duration inside warmup+cooldown",
             duration=150)


VALID_EXPECT = {"name": "buffer-climbs", "service": "logit",
                "metric": "logit.component.buffer.batches", "component": "victoria_metrics",
                "step": "c0s4", "window": "during", "reduce": "max", "min": 2}


def _expect_rules(root):
    path, base = _base(root)

    def with_expect(**changes):
        raw = copy.deepcopy(base)
        one = {**VALID_EXPECT, **changes}
        raw["expect"] = [{k: v for k, v in one.items() if v is not None}]
        return raw

    loaded = scenario.from_dict(with_expect(attrs={"reason": "overflow_oldest"}), path)
    exp = loaded.expectations[0]
    expect(exp.attrs == {"component": "victoria_metrics", "reason": "overflow_oldest"},
           f"`component` joins `attrs`, got {exp.attrs}")
    resolved = loaded.to_json()
    expect(resolved["expect"][0]["name"] == "buffer-climbs",
           "the resolved scenario carries its expectations")
    scenario.from_dict(with_expect(step=4, min=None, max=3), path)
    expect(True, "a step index and a lone max are accepted")
    scenario.from_dict(with_expect(window="through"), path)
    expect(True, "the through window is accepted")

    cases = [
        (dict(windw="during"), "unknown key `windw`", "an unknown expect key"),
        (dict(metric=None), "missing `metric`", "an expectation without a metric"),
        (dict(service="victoria-metrics"), "unknown service `victoria-metrics`",
         "an expectation on a service with no telemetry"),
        (dict(window="before"), "unknown window `before`", "an unknown window"),
        (dict(reduce="mean"), "unknown reduce `mean`", "an unknown reducer"),
        (dict(reduce="delta"), "reads a sum, and logit.component.buffer.batches is a gauge",
         "a counter reducer on a gauge"),
        (dict(metric="logit.input.kernel.drops", reduce="max"),
         "reads a gauge, and logit.input.kernel.drops is a sum", "a gauge reducer on a counter"),
        (dict(min=None), "needs `min`, `max`, or both", "an expectation without bounds"),
        (dict(max=1), "`min` 2 is above `max` 1", "min above max"),
        (dict(min="2"), "must be numbers", "a bound that isn't a number"),
        (dict(min=True), "must be numbers", "a boolean bound"),
        (dict(step=8), "step index 8 is outside", "a step index past the [[step]] tables"),
        (dict(step=0), "step index 0 is outside", "a step index of 0"),
        (dict(step="c0s9"), "names no [[step]] table", "a step id naming no [[step]]"),
        (dict(step="c3s1"), "isn't in the scenario's own schedule",
         "a step id the schedule never reaches"),
        (dict(step="first"), "must be a step id", "a malformed step"),
        (dict(name="Buffer climbs"), "`name` must be lowercase", "a name that isn't a slug"),
        (dict(attrs={"component": "x"}), "set both alone and in `attrs`",
         "component set twice"),
        (dict(attrs={"count": 2}), "`attrs` must be a table of strings", "a non-string attr"),
        (dict(component=5), "`component` must be a string", "a non-string component"),
        (dict(name=["a"]), "`name` must be lowercase", "a list-valued name"),
        (dict(reduce=["max"]), "unknown reduce", "a list-valued reduce"),
        (dict(metric=["x"]), "`metric` must name a metric", "a list-valued metric"),
    ]
    for changes, needle, what in cases:
        _refused(path, with_expect(**changes), needle, what)
    twice = copy.deepcopy(base)
    twice["expect"] = [dict(VALID_EXPECT), dict(VALID_EXPECT)]
    _refused(path, twice, "repeats another expectation's", "two expectations with one name")
    # A shorter --duration that drops the step is accepted; the row SKIPs.
    loaded = scenario.from_dict(with_expect(), path)
    scenario.validate(loaded, duration=300)
    expect(True, "a short --duration may drop an expectation's step")


def _expand(root):
    path, base = _base(root)
    loaded = scenario.from_dict(copy.deepcopy(base), path)
    full = scenario.expand(loaded)
    expect(len(full) == 7, f"expand(16m) has 7 steps, got {len(full)}")
    expect(full[0].start == 60 and full[0].end == 180, "the first step starts after warmup")
    expect(all(s.end <= 960 - 120 for s in full), "no fault ends inside cooldown")
    short = scenario.expand(loaded, 300)
    expect([s.id for s in short] == ["c0s1"], f"expand(5m) keeps only c0s1, got {short}")
    long = scenario.expand(loaded, 2000)
    expect(len(long) == 15, f"expand(2000s) repeats the cycle, got {len(long)}")
    expect(len({s.id for s in long}) == len(long), "expanded step ids are unique")


def _line(ts, name, kind, value, component="self", attrs=None, **fields):
    metric = {"name": name, "kind": kind}
    if kind == "sum":
        metric.update(value=value, temporality="delta", monotonic=True)
    else:
        metric["value"] = value
    metric.update(fields)
    attributes = {"component": component, "kind": "x", "role": "sink", **(attrs or {})}
    return json.dumps({"timestamp": ts, "metrics": [metric], "attributes": attributes},
                      separators=(",", ":"))


def _random_base(root):
    path = root / "scenarios" / "random-faults" / "scenario.toml"
    return path, tomllib.loads(path.read_text())


# The first three faults the shipped random-faults scenario draws with its own seed. A change
# here means a seed no longer reproduces the schedule an earlier run recorded.
RANDOM_HEAD = [("r1", "kill", "logit", "", 260.0, 314.0),
               ("r2", "netem", "logit", "delay 1s", 451.0, 591.0),
               ("r3", "kill", "logit", "", 810.0, 839.0)]


def _random(root):
    path, base = _random_base(root)
    loaded = scenario.load(path)
    spec = loaded.random
    templates = {t.index: t for t in spec.faults}
    head = [(s.id, s.action, s.on, s.args, s.start, s.end) for s in scenario.expand(loaded)[:3]]
    expect(head == RANDOM_HEAD, f"the scenario's own seed draws the recorded head, got {head}")
    first = [s.to_json() for s in scenario.expand(loaded, 8 * 3600, 5)]
    again = [s.to_json() for s in scenario.expand(loaded, 8 * 3600, 5)]
    expect(first == again and first, "the same seed draws the same schedule")
    drawn = {json.dumps([s.to_json() for s in scenario.expand(loaded, 3600, seed)])
             for seed in range(12)}
    expect(len(drawn) == 12, f"12 seeds draw 12 different schedules, got {len(drawn)}")
    window = loaded.thresholds["progress_window"]
    for seed in range(12):
        long = scenario.expand(loaded, 24 * 3600, seed)
        for duration in (600, 1200, 3600, 8 * 3600, 24 * 3600):
            where = f"seed {seed} at {scenario.format_duration(duration)}"
            try:
                scenario.validate(loaded, duration=duration, seed=seed)
            except scenario.ScenarioError as err:
                expect(False, f"{where}: validate() refused a generated schedule: {err}")
            steps = scenario.expand(loaded, duration, seed)
            problems = scenario._schedule_problems(loaded, steps, duration)
            expect(not problems, f"{where}: the schedule breaks {problems}")
            expect([s.to_json() for s in steps] == [s.to_json() for s in long[:len(steps)]],
                   f"{where}: a shorter run's schedule is a prefix of a longer one's")
            expect([s.id for s in steps] == [f"r{i + 1}" for i in range(len(steps))],
                   f"{where}: steps are r1, r2, ... in order")
            previous = loaded.warmup
            busy = 0.0
            # One expectation per schedule and rule, each naming the first step that breaks it.
            broken = {}
            for step in steps:
                template = templates[step.spec_index]
                gap = step.start - previous
                length = step.end - step.start
                for rule, holds in (
                    ("its gap is outside random.gap", spec.gap_min <= gap <= spec.gap_max),
                    ("it follows a recovery with under two progress windows of steady state",
                     gap - loaded.recovery_bound >= 2 * window),
                    ("it isn't a draw of its template",
                     template.for_min <= length <= template.for_max
                     and (step.action, step.on) == (template.action, template.on)
                     and (step.args in template.args if template.args else step.args == "")),
                    ("it ends inside cooldown", step.end <= duration - loaded.cooldown),
                ):
                    if not holds:
                        broken.setdefault(rule, step)
                busy += length + loaded.recovery_bound
                previous = step.end
            expect(not broken, f"{where}: " + "; ".join(
                f"{step.id} ({step.action} on {step.on}, {step.start:g}s to {step.end:g}s): "
                f"{rule}" for rule, step in broken.items()))
            if duration >= 8 * 3600:
                share = 1 - busy / (duration - loaded.warmup)
                expect(share >= 0.25, f"{where}: steady state is {share:.0%} of the run after "
                                      "warmup, under the 25% gap.min guarantees")

    resolved = loaded.to_json(3600, 42)
    expect(resolved["seed"] == 42 and resolved["cycle"] is None
           and resolved["random"]["seed"] == spec.seed
           and resolved["steps"] == [s.to_json() for s in scenario.expand(loaded, 3600, 42)],
           "scenario.resolved.json records the seed used and the schedule it drew")
    expect(loaded.to_json(3600)["seed"] == spec.seed,
           "with no --seed, the resolved seed is the scenario's own")
    statsd = scenario.load(root / "scenarios" / "statsd-vm" / "scenario.toml")
    expect(statsd.to_json()["seed"] is None and statsd.to_json()["random"] is None,
           "a fixed schedule records no seed")
    run = driver.Run(root, loaded, 3600, None, False, "/nonexistent", [], "logit:soak")
    expect(run.seed == spec.seed and run.seed_source == "scenario.toml"
           and run.steps == scenario.expand(loaded, 3600),
           f"the driver draws with the scenario's seed by default, got {run.seed}")
    run = driver.Run(root, loaded, 3600, 7, False, "/nonexistent", [], "logit:soak")
    expect(run.seed == 7 and run.seed_source == "--seed"
           and run.steps == scenario.expand(loaded, 3600, 7),
           f"--seed overrides the scenario's seed, got {run.seed}")
    try:
        scenario.validate(loaded, seed=7)
        expect(True, "")
    except scenario.ScenarioError as err:
        expect(False, f"--seed on a [random] schedule is refused: {err}")

    def mutate(edit):
        raw = copy.deepcopy(base)
        edit(raw)
        return raw

    def fault(raw, **changes):
        raw["random"]["fault"][0].update(changes)

    def lifecycle(raw, **changes):
        # The first template that isn't netem.
        next(f for f in raw["random"]["fault"] if f["action"] != "netem").update(changes)

    _refused(path, mutate(lambda r: r.update(cycle="13m")), "not both", "[random] with a cycle")
    _refused(path, mutate(lambda r: r.update(step=[{"at": "0s", "action": "pause",
                                                    "on": "logit", "for": "10s"}])),
             "not both", "[random] with [[step]] tables")
    _refused(path, mutate(lambda r: r.update(expect=[dict(VALID_EXPECT)])),
             "[[expect]] names a step", "[[expect]] beside [random]")
    _refused(path, mutate(lambda r: r["random"]["gap"].update(min="104s")),
             "random.gap.min 1m44s is under recovery_bound", "a gap under recovery + 2 windows")
    _refused(path, mutate(lambda r: r.update(cooldown="110s")),
             "shorter than random.gap.min", "a cooldown under gap.min")
    _refused(path, mutate(lambda r: r["random"]["gap"].update(max="110s")),
             "random.gap.min 2m is above max", "gap.min above gap.max")
    _refused(path, mutate(lambda r: fault(r, **{"for": {"min": "3m", "max": "1m"}})),
             "`for.min` 3m is above `for.max` 1m", "for.min above for.max")
    _refused(path, mutate(lambda r: fault(r, **{"for": {"min": "0s", "max": "1m"}})),
             "`for.min` must be longer than 0s", "a zero for.min")
    _refused(path, mutate(lambda r: fault(r, weight=0)), "`weight` must be above 0",
             "a zero weight")
    _refused(path, mutate(lambda r: fault(r, weight="3")), "`weight` must be a number",
             "a string weight")
    _refused(path, mutate(lambda r: fault(r, action="explode")), "unknown action",
             "an unknown random action")
    _refused(path, mutate(lambda r: fault(r, on="influxdb")),
             "random.fault 1 (netem on influxdb): unknown service",
             "an unknown random service")
    _refused(path, mutate(lambda r: lifecycle(r, action="kill", on="generator")),
             "random.fault 3 (kill on generator): kill is for logit only",
             "a random kill on the generator")
    _refused(path, mutate(lambda r: fault(r, args=[])), "netem needs `args`",
             "netem with no args")
    _refused(path, mutate(lambda r: fault(r, args=["clear"])), "netem clear is a fault's revert",
             "netem clear as a random spec")
    _refused(path, mutate(lambda r: fault(r, args="loss 1%")), "`args` must be an array",
             "netem args as a string")
    _refused(path, mutate(lambda r: lifecycle(r, args=["loss 1%"])), "`args` is for netem only",
             "args on a random lifecycle fault")
    _refused(path, mutate(lambda r: r["random"].update(sede=1)), "random: unknown key `sede`",
             "a misspelled [random] key")
    _refused(path, mutate(lambda r: fault(r, fro="1m")), "unknown key `fro`",
             "a misspelled [[random.fault]] key")
    _refused(path, mutate(lambda r: r["random"].pop("seed")), "random: missing `seed`",
             "a [random] table with no seed")
    _refused(path, mutate(lambda r: r["random"].update(seed=-1)), "integer 0 or more",
             "a negative seed")
    _refused(path, mutate(lambda r: r["random"].update(seed="7")), "integer 0 or more",
             "a string seed")
    _refused(path, mutate(lambda r: r["random"].update(fault=[])), "at least one",
             "a [random] table with no fault")
    _refused(path, mutate(lambda r: r["random"].pop("gap")), "random: missing `gap`",
             "a [random] table with no gap")
    _refused(path, mutate(lambda r: r.update(duration="8m")), "doesn't fit every draw",
             "an own duration one draw can overrun")
    _refused(path, copy.deepcopy(base), "--seed must be an integer 0 or more",
             "a negative --seed", seed=-3)

    # The schedule rules, against hand-built schedules no draw produces.
    def step(n, action, on, start, end, args=""):
        return scenario.Step(id=f"r{n}", spec_index=0, cycle=None, action=action, on=on,
                             args=args, start=start, end=end)

    for steps, needle, what in (
        ([step(1, "stop", "logit", 300, 400), step(2, "pause", "generator", 350, 380)],
         "starts before the previous fault ends",
         "faults overlapping across containers"),
        ([step(1, "stop", "logit", 300, 400), step(2, "netem", "logit", 350, 380, "loss 1%")],
         "netem on logit during its stop", "netem inside a stop"),
        ([step(1, "stop", "logit", 300, 400), step(2, "pause", "logit", 450, 480)],
         "under random.gap.min", "a gap under gap.min"),
        ([step(1, "stop", "logit", 100, 200)], "under random.gap.min",
         "a first fault too soon after warmup"),
        ([step(1, "pause", "logit", 30, 200)], "starts inside warmup", "a fault in warmup"),
        ([step(1, "pause", "logit", 3000, 3550)], "ends inside cooldown", "a fault in cooldown"),
        ([step(1, "kill", "generator", 300, 330)], "kill is for logit only",
         "a kill on the generator"),
    ):
        problems = scenario._schedule_problems(loaded, steps, 3600)
        expect(any(needle in problem for problem in problems),
               f"{what}: expected a problem containing {needle!r}, got {problems}")


class _FakeLogs:
    """A container log for `LogCapture`, read as moby's json-file reader does: `--since` skips
    lines before it only until the first line at or after it, and `--until` stops at the first
    line after it. `fail_next` makes the next call write its first line and `FAIL_TEXT` to the
    stderr file, as the CLI does, and return 1 with an empty `Result.stderr`."""

    FAIL_TEXT = "Error response from daemon: No such container: id1"

    def __init__(self):
        self.lines = []
        self.calls = []
        self.fail_next = False

    def logs_to(self, container, stdout_path, stderr_path, since=None, until=None,
                timeout=0):
        self.calls.append((since, until))
        since_ns = None if since is None else _ns(since)
        until_ns = None if until is None else _ns(until)
        chosen = []
        for ts, stream, text in self.lines:
            if since_ns is not None:
                if ts < since_ns:
                    continue
                since_ns = None
            if until_ns is not None and ts > until_ns:
                break
            chosen.append((stream, text))
        failing, self.fail_next = self.fail_next, False
        if failing:
            chosen = chosen[:1]
        with open(stdout_path, "w") as out, open(stderr_path, "w") as err:
            for stream, text in chosen:
                (out if stream == "stdout" else err).write(text + "\n")
            if failing:
                err.write(self.FAIL_TEXT + "\n")
        return docker.Result(1 if failing else 0, "", "")


def _ns(text):
    """An RFC 3339 timestamp with nine fractional digits -> epoch nanoseconds."""
    base, frac = text[:-1].split(".")
    return int(telemetry.parse_rfc3339(base + "Z")) * 1_000_000_000 + int(frac)


def _log_capture():
    ns = 1_800_000_000 * 1_000_000_000
    expect(collect.rfc3339_ns(ns + 123_456_789) == "2027-01-15T08:00:00.123456789Z"
           and _ns(collect.rfc3339_ns(ns + 7)) == ns + 7,
           f"rfc3339_ns writes nine digits, got {collect.rfc3339_ns(ns + 123_456_789)}")
    second = 1_000_000_000
    with tempfile.TemporaryDirectory() as tmp:
        fake = _FakeLogs()
        capture = collect.LogCapture(Path(tmp), fake)
        written = []

        def add(offset_s, stream, text):
            fake.lines.append((ns + int(offset_s * second), stream, text))
            written.append((stream, text))

        add(1, "stdout", "a")
        add(2, "stderr", "b")
        add(9, "stdout", "c")
        # stderr's copier timestamped this before "c" and wrote it after: out of order.
        add(8.5, "stderr", "d")
        add(14, "stdout", "e")
        expect(capture.chunk({"logit": "id1"}, now_ns=ns + 15 * second),
               "the first chunk succeeds")
        expect(fake.calls[-1] == (None, collect.rfc3339_ns(ns + 10 * second)),
               f"the first chunk reads from the start until 5 s ago, got {fake.calls[-1]}")
        # Written after the first chunk with a timestamp inside it, behind a line past it: the
        # next chunk starts at that later line, by position, so it isn't skipped.
        add(9.5, "stdout", "f")
        add(16, "stderr", "g")
        expect(capture.chunk({"logit": "id1"}, now_ns=ns + 21 * second),
               "the second chunk succeeds")
        expect(fake.calls[-1][0] == collect.rfc3339_ns(ns + 10 * second + 1),
               f"the next chunk starts 1 ns after the last one's until, got {fake.calls[-1]}")
        add(30, "stdout", "h")
        fake.fail_next = True
        expect(not capture.chunk({"logit": "id1", "generator": "id2"},
                                 now_ns=ns + 40 * second),
               "a failed chunk fails the round")
        expect(len(fake.calls) == 3, f"a failed chunk stops the round, got {fake.calls}")
        failed_since = fake.calls[-1][0]
        add(41, "stderr", "i")
        expect(capture.chunk({"logit": "id1"}, final=True), "the final chunk succeeds")
        expect(fake.calls[-1] == (failed_since, None),
               f"a failed chunk leaves the cursor, and the final chunk reads to the end, got "
               f"{fake.calls[-1]} after {failed_since}")
        capture.close()
        logs = Path(tmp) / "logs"
        got_out = (logs / "logit.stdout").read_text().split()
        got_err = (logs / "logit.stderr").read_text().split()
        want_out = [t for stream, t in written if stream == "stdout"]
        want_err = [t for stream, t in written if stream == "stderr"]
        expect(got_out == want_out and got_err == want_err,
               f"chunks partition the log with no line lost or repeated, got {got_out} "
               f"{got_err}, want {want_out} {want_err}")
        records = collect.read_jsonl(Path(tmp) / "log-chunks.jsonl")
        expect(len(records) == len(fake.calls) and [r["rc"] for r in records] == [0, 0, 1, 0],
               f"every chunk is recorded in log-chunks.jsonl, got {records}")
        expect(records[2].get("stderr", "").endswith(_FakeLogs.FAIL_TEXT),
               f"a failed chunk's record keeps the CLI's error from its stderr file, got "
               f"{records[2]}")
        expect(not list(logs.glob(".*.chunk")), "no temporary chunk file is left behind")


class _HungDocker:
    """`inspect` that takes `POLL_TIMEOUT_S` on a fake clock and fails, as a hung daemon's
    does."""

    def __init__(self, clock):
        self.clock = clock
        self.calls = 0

    def inspect(self, container, timeout=0):
        self.calls += 1
        self.clock[0] += timeout
        return None


class _Records(list):
    def write(self, record):
        self.append(record)


def _hung_inspect(root):
    loaded = scenario.load(root / "scenarios" / "statsd-vm" / "scenario.toml")
    run = driver.Run(root, loaded, 600, None, False, "/nonexistent", [], "logit:soak")
    clock = [1000.0]
    real_now = driver._now
    driver._now = lambda: clock[0]
    try:
        run.ids = {"logit": "a", "generator": "b", "victoria-metrics": "c"}
        run.docker = _HungDocker(clock)
        run.watchdog = _Records()
        run.t0 = 0.0
        run.inspect_all()
    finally:
        driver._now = real_now
    expect(run.docker.calls == 1 and len(run.watchdog) == 1,
           f"a timed-out inspect ends the round, got {run.docker.calls} call(s)")


def _signals(root):
    """`interrupt_on_signals` in a child process, so this one's handlers stay as they are."""
    probe = ("import signal, sys\n"
             "sys.path.insert(0, sys.argv[1])\n"
             "from soaklib import driver\n"
             "# The parent may run under nohup, which hands down an ignored SIGHUP, so each case\n"
             "# sets both dispositions itself.\n"
             "signal.signal(signal.SIGTERM, signal.SIG_DFL)\n"
             "signal.signal(signal.SIGHUP, signal.SIG_IGN if sys.argv[2] == 'ignored'\n"
             "              else signal.SIG_DFL)\n"
             "driver.interrupt_on_signals()\n"
             "names = {signal.SIG_IGN: 'ignored', signal.default_int_handler: 'interrupt'}\n"
             "print(names.get(signal.getsignal(signal.SIGTERM), 'other'),\n"
             "      names.get(signal.getsignal(signal.SIGHUP), 'other'))\n")
    for inherited, want in (("ignored", "interrupt ignored"), ("default", "interrupt interrupt")):
        out = subprocess.run([sys.executable, "-c", probe, str(root), inherited],
                             capture_output=True, text=True, timeout=60)
        expect(out.stdout.strip() == want,
               f"with SIGHUP inherited {inherited}, SIGTERM and SIGHUP end up {want!r}, got "
               f"{out.stdout.strip()!r} {out.stderr.strip()[-300:]!r}")


def _deadlines():
    expect(driver._next_multiple(4.99, 5) == 5 and driver._next_multiple(5.0, 5) == 10
           and driver._next_multiple(17.3, 5) == 20 and driver._next_multiple(899.9, 300) == 900,
           "a poll's next deadline is the next multiple of its period")


def _ndjson():
    stamp = "2026-10-08T12:00:%02d.123456789Z"
    lines = [
        _line(stamp % 5, "logit.process.uptime", "gauge", 5.0),
        _line(stamp % 5, "logit.component.batches.delivered", "sum", 2, component="sink"),
        _line(stamp % 10, "logit.process.uptime", "gauge", 10.0),
        _line(stamp % 10, "logit.component.batches.delivered", "sum", 3, component="sink"),
        _line(stamp % 10, "logit.process.fds", "gauge", 12),
        "not json",
        # The second life: uptime drops.
        _line(stamp % 30, "logit.process.uptime", "gauge", 4.0),
        _line(stamp % 30, "logit.component.batches.delivered", "sum", 7, component="sink"),
        _line(stamp % 35, "logit.process.uptime", "gauge", 9.0),
        _line(stamp % 35, "logit.process.fds", "gauge", 14),
    ]
    tel = telemetry.parse_ndjson(lines)
    expect(tel.bad_lines == 1, "the non-JSON line is counted")
    expect(len(tel.life_starts) == 2, f"two lives, got {tel.life_starts}")
    by_life = tel.counter_by_life("logit.component.batches.delivered", component="sink")
    expect(by_life == {0: 5, 1: 7}, f"counter sums per life, got {by_life}")
    fds = tel.gauge_series("logit.process.fds")
    expect([(v, life) for _, v, life in fds] == [(12, 0), (14, 1)], f"gauge series, got {fds}")
    base = telemetry.parse_rfc3339("2026-10-08T12:00:00Z")
    expect(abs(fds[0][0] - (base + 10.123456789)) < 1e-6, "nine fractional digits kept")
    expect(len(tel.drains) == 4, "one drain per uptime point")
    window = tel.counter_in("logit.component.batches.delivered", base, base + 20,
                            component="sink")
    expect(window == 5, f"counter over a window, got {window}")


def _stderr():
    cases = [
        ('{"timestamp":"2026-10-08T12:00:00.000000Z","level":"ERROR","fields":{"message":'
         '"sink retrying","key":"retrying"},"target":"logit_pipeline","component":"vm"}',
         "json", "ERROR", "retrying"),
        ('{"timestamp":"2026-10-08T12:00:01.000000Z","level":"ERROR","message":"exiting",'
         '"code":2,"reason":"node failed","target":"logit"}', "json", "ERROR", ""),
        ("thread 'tokio-runtime-worker' panicked at crates/x/src/lib.rs:10:5:", "panic", "", ""),
        ('{"timestamp":"2026-10-08T12:00:02.000000Z","level":"ERROR","message":"panicked",'
         '"key":"thread_panicked"}', "json", "ERROR", "thread_panicked"),
        ("note: run with `RUST_BACKTRACE=1`", "text", "", ""),
    ]
    for raw, kind, level, key in cases:
        line = telemetry.classify(raw)
        expect((line.kind, line.level, line.key) == (kind, level, key),
               f"classify({raw[:40]!r}) -> {(line.kind, line.level, line.key)}")
    exiting = telemetry.classify(cases[1][0])
    expect(exiting.message == "exiting" and exiting.fields.get("code") == 2,
           "exiting carries its code")
    expect(telemetry.classify(cases[0][0]).ts is not None, "a JSON line's timestamp parses")
    sink_lines = {
        '{"level":"ERROR","message":"sink retrying","key":"retrying"}': True,
        '{"level":"WARN","message":"batch dropped","key":"send_failed"}': True,
        '{"level":"WARN","message":"degraded","component":"vm"}': True,
        '{"level":"WARN","message":"degraded","key":"other"}': False,
        '{"level":"ERROR","message":"bind failed","key":"bind"}': False,
    }
    for raw, expected in sink_lines.items():
        expect(checks.is_sink_fault_line(telemetry.classify(raw)) == expected,
               f"is_sink_fault_line({raw})")


def _slope():
    expect(checks.slope([(0, 0), (1, 2), (2, 4)]) == 2, "slope of a line")
    expect(checks.slope([(5, 1)]) is None, "slope of one point")
    expect(checks.slope([(1, 1), (1, 2)]) is None, "slope with no x spread")
    expect(checks.parse_mem("45.5MiB / 31GiB") == 45.5 * 1024 * 1024, "docker stats MemUsage")


T0 = telemetry.parse_rfc3339("2026-10-08T12:00:00Z")
RESOLVED = {"name": "fixture", "duration": 600.0, "warmup": 60.0, "cooldown": 120.0,
            "recovery_bound": 45.0, "ledger": {"sut_sink": "sink"},
            "thresholds": {"progress_window": 30.0, "fd_growth": 8,
                           "rss_growth_mib_per_hour": 64}}


def _stamp(t):
    return datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _phase(name, offset, **extra):
    return {"event": "phase", "phase": name, "t": T0 + offset, **extra}


def _action(event, step, action, on, start, end=None, **extra):
    return {"event": event, "step": step, "action": action, "on": on, "rc": 0,
            "started_at": T0 + start, "finished_at": T0 + (start + 1 if end is None else end),
            **extra}


def _ready(step, on, offset):
    return {"event": "ready", "on": on, "step": step, "why": "start", "ok": True,
            "ready_s": 0.1, "t": T0 + offset}


def _run_dir(tmp, name, timeline, stdout=(), **resolved):
    run_dir = Path(tmp) / name
    (run_dir / "logs").mkdir(parents=True)
    (run_dir / "timeline.jsonl").write_text("".join(json.dumps(r) + "\n" for r in timeline))
    (run_dir / "scenario.resolved.json").write_text(json.dumps({**RESOLVED, **resolved}))
    if stdout:
        (run_dir / "logs" / "logit.stdout").write_text("\n".join(stdout) + "\n")
    return run_dir


def _status(run_dir, check):
    return check(checks.RunData(run_dir))


def _sut_stdout(end_s, fds=lambda offset: 10):
    """A SUT's NDJSON every 5 s: uptime, one delivered batch, flat RSS, and `fds(offset)`."""
    lines = []
    for offset in range(0, int(end_s), 5):
        stamp = _stamp(T0 + offset)
        lines.append(_line(stamp, "logit.process.uptime", "gauge", offset + 1.0))
        lines.append(_line(stamp, "logit.component.batches.delivered", "sum", 1,
                           component="sink"))
        lines.append(_line(stamp, "logit.process.memory.resident.bytes", "gauge", 50 * 2 ** 20))
        lines.append(_line(stamp, "logit.process.fds", "gauge", fds(offset)))
    return lines


def _checks():
    with tempfile.TemporaryDirectory() as tmp:
        start, end = _phase("start", 0, t0=T0), [_phase("end_begin", 400), _phase("end_end", 450)]

        clean = _run_dir(tmp, "clean", [start, _action("apply", "c0s1", "netem", "logit", 60),
                                        _action("revert", "c0s1", "netem", "logit", 180)] + end)
        expect(_status(clean, checks.check_timeline).status == checks.PASS,
               "timeline PASSes actions that returned 0")
        expect(_status(clean, checks.check_run).status == checks.PASS,
               "run PASSes a timeline from start through end_end")
        expect(len(checks.RunData(clean).faults) == 1, "a clean apply opens a fault window")

        failed = _run_dir(tmp, "failed", [
            start, _action("apply", "c0s1", "netem", "logit", 60, rc=2,
                           stderr="Error: Specified qdisc kind is unknown."),
            _action("revert", "c0s1", "netem", "logit", 180)] + end)
        result = _status(failed, checks.check_timeline)
        expect(result.status == checks.FAIL and "rc 2" in result.detail,
               f"timeline FAILs an apply with rc 2, got {result}")
        expect(checks.RunData(failed).faults == [], "a failed apply opens no fault window")

        fail_fast = _run_dir(tmp, "fail-fast", [start, _phase("fail_fast", 72, reason="x")] + end)
        expect(_status(fail_fast, checks.check_timeline).status == checks.FAIL,
               "timeline FAILs a fail-fast phase")

        stop = [_action("apply", "c0s1", "stop", "logit", 60),
                _action("revert", "c0s1", "stop", "logit", 90)]
        unprobed = _run_dir(tmp, "unprobed", [start] + stop + end)
        result = _status(unprobed, checks.check_timeline)
        expect(result.status == checks.FAIL and "no readiness probe" in result.detail,
               f"timeline FAILs a start with no ready record, got {result}")
        probed = _run_dir(tmp, "probed", [start] + stop + [_ready("c0s1", "logit", 92)] + end)
        expect(_status(probed, checks.check_timeline).status == checks.PASS,
               "timeline PASSes a start with a ready record")
        early = _run_dir(tmp, "early", [start, stop[0], _phase("end_begin", 80),
                                        {**stop[1], "early": True}, _phase("end_end", 450)])
        expect(_status(early, checks.check_timeline).status == checks.PASS,
               "timeline needs no ready record after an early revert")

        aborted = _run_dir(tmp, "aborted", [
            {"event": "phase", "phase": "up_begin", "t": T0 - 10},
            {"event": "phase", "phase": "up_end", "t": T0 - 5, "rc": 1},
            {"event": "phase", "phase": "aborted", "t": T0 - 5,
             "reason": "compose up failed: pull access denied"},
            {"event": "phase", "phase": "collected", "t": T0},
            {"event": "phase", "phase": "down", "t": T0 + 1, "rc": 0}])
        results = checks.run_all(aborted)
        run_row = next(r for r in results if r.id == "run")
        expect(run_row.status == checks.FAIL and "aborted" in run_row.detail
               and "no start" in run_row.detail, f"run FAILs an aborted run, got {run_row}")
        expect(driver.exit_code(results) == 1, "check exits 1 on an aborted run dir")
        passing = [checks.Result("x", checks.PASS, "")]
        expect(driver.exit_code(passing, "aborted") == 1, "run exits 1 when aborted")
        expect(driver.exit_code(passing, "error") == 1, "run exits 1 on a driver error")
        expect(driver.exit_code(passing, "interrupted") == 130, "run exits 130 when interrupted")
        expect(driver.exit_code(passing) == 0, "run exits 0 with no FAIL")

        # c0s1's recovery ends at +145, inside c0s2's window, where the fds rise; the
        # after-recovery sample must come from steady state, at +250.
        fd_timeline = [start, _action("apply", "c0s1", "pause", "generator", 60, 61),
                       _action("revert", "c0s1", "pause", "generator", 100, 100),
                       _action("apply", "c0s2", "stop", "victoria-metrics", 140, 141),
                       _action("revert", "c0s2", "stop", "victoria-metrics", 200, 200)] + end
        fd_run = _run_dir(tmp, "fd", fd_timeline,
                          _sut_stdout(400, lambda o: 30 if 140 <= o <= 245 else 10))
        result = _status(fd_run, checks.check_fd_slope)
        expect(result.status == checks.PASS,
               f"fd_slope skips an after-recovery sample inside a later fault, got {result}")

        # A fault over almost the whole hour leaves about 150 s of steady state after warmup,
        # 4% of the run.
        long_end = [_phase("end_begin", 3600), _phase("end_end", 3650)]
        thin = _run_dir(tmp, "thin", [start, _action("apply", "c0s1", "netem", "logit", 100),
                                      _action("revert", "c0s1", "netem", "logit", 3450)]
                        + long_end, _sut_stdout(3600), duration=3600.0)
        for check in (checks.check_progress, checks.check_rss_slope):
            result = _status(thin, check)
            expect(result.status == checks.WARN and "cover little" in " ".join(result.lines),
                   f"{check.__name__} WARNs on thin coverage, got {result}")
        wide = _run_dir(tmp, "wide", [start] + long_end, _sut_stdout(3600), duration=3600.0)
        for check in (checks.check_progress, checks.check_rss_slope):
            result = _status(wide, check)
            expect(result.status == checks.PASS,
                   f"{check.__name__} PASSes a fault-free hour, got {result}")


def _vm_resets():
    def series(name, values, seconds):
        return {"metric": {"__name__": name}, "values": values,
                "timestamps": [int((T0 + s) * 1000) for s in seconds]}

    one = series("a_total", [5, 9, 9, 3, 7, 2], [10, 20, 30, 40, 50, 60])
    total, segments = vm.reset_aware(one)
    expect(total == 9 + 7 + 2, f"reset-aware total adds the value after each decrease, got {total}")
    expect([amount for _, amount in segments] == [9, 7, 2], f"segments per reset, got {segments}")
    expect(vm.reset_aware({"values": [], "timestamps": []})[0] == 0, "an empty series is 0")

    starts = [T0, T0 + 35]
    two = [series("a_total", [5, 9, 3, 7], [10, 20, 40, 50]),
           series("b_total", [1, 4, 2], [10, 20, 40])]
    by_life, total, resets, mismatched = vm.totals_by_life(two, starts)
    expect(by_life == {0: 13, 1: 9} and total == 22, f"V split per life, got {by_life} {total}")
    expect(resets == {"a_total": 1, "b_total": 1} and not mismatched, f"resets, got {resets}")
    # A series that only lived in the second life has no reset: credited by timestamp, listed.
    late = [series("c_total", [4, 6], [40, 50])]
    by_life, _, resets, mismatched = vm.totals_by_life(late, starts)
    expect(by_life == {1: 6} and mismatched == ["c_total"],
           f"a series with too few resets is credited by timestamp, got {by_life} {mismatched}")
    # A restart whose first value isn't below the last life's final value shows no decrease.
    rising = [{"metric": {"__name__": "d_total"}, "values": [100, 300, 350, 550, 750],
               "timestamps": [s * 1000 for s in (2900, 2910, 3010, 3020, 3030)]}]
    by_life, total, resets, mismatched = vm.totals_by_life(rising, [1000, 3000])
    expect(by_life == {0: 300, 1: 750} and total == 1050 and resets == {"d_total": 1}
           and not mismatched,
           f"V splits at a life boundary with no decrease, got {by_life} {total} {resets}")
    # A decrease inside one life plus an absence from another keeps the segment count at the
    # number of lives; crediting by position would hide the mismatch and misplace the amounts.
    folded = [{"metric": {"__name__": "e_total"}, "values": [100, 200, 50, 80, 10, 20],
               "timestamps": [s * 1000 for s in (1100, 1200, 1300, 1400, 2100, 2200)]}]
    by_life, total, resets, mismatched = vm.totals_by_life(folded, [1000, 2000, 3000])
    expect(by_life == {0: 280, 1: 20} and total == 300 and mismatched == ["e_total"],
           f"V credits each segment to its own life, got {by_life} {total} {mismatched}")


LEDGER = {"vm_selector": "{__name__=~\"x_[0-9]+_total\"}", "generator_input": "load",
          "generator_sink": "out", "sut_listener": "in", "sut_aggregate": "window",
          "sut_sink": "sink", "wire_loss_outside_faults": 0.0}
# The fixture's SUT stop: applied at +101, reverted by +131, so life 1 starts at +131.
STOP = (101, 131)
PER_DRAIN = 1000


def _ledger_run(tmp, name, life0_extra=50, shutdown_drops=20, v0_extra=30, v1_short=0,
                life0_dropped=0, life1_dropped=0, steady_loss=0, fault_loss=0, end_extra=0,
                e1_short=0, a1_short=0, ab1_short=0, late_series=False, receive_util=0.0, slow_after_stop=False, rate_behind=0,
                undelivered=0, empty_export=False, ab0_short=0, merged_lives=False,
                empty_sut=False, buffer_batches=0, buffer_util=0.0, steady_surplus=0,
                **resolved):
    """A two-life run directory for the ledger checks. The generator sends `PER_DRAIN` lines
    per 5 s drain from +0 to +300; the SUT is stopped over `STOP`; each keyword breaks one
    rule. By default life 0 ends with `life0_extra` read but not absorbed, `shutdown_drops` of
    them dropped on stderr over two of the listener's shutdown lines, and `v0_extra` reaching
    VictoriaMetrics after its last drain, and life 1 balances."""
    gen = []
    for offset in range(5, 301, 5):
        stamp = _stamp(T0 + offset)
        gen.append(_line(stamp, "logit.process.uptime", "gauge", float(offset)))
        gen.append(_line(stamp, "logit.output.messages", "sum", PER_DRAIN, component="out"))
        gen.append(_line(stamp, "logit.output.datagrams", "sum", PER_DRAIN, component="out"))
        gen.append(_line(stamp, "logit.component.events.sent", "sum", PER_DRAIN,
                         component="load"))
        gen.append(_line(stamp, "logit.component.batches.sent", "sum", PER_DRAIN // 100,
                         component="load"))
        gen.append(_line(stamp, "logit.component.buffer.batches", "gauge", 0, component="out"))
    if rate_behind:
        gen.append(_line(_stamp(T0 + 150), "logit.component.diagnostics", "sum", rate_behind,
                         component="load", attrs={"key": "rate_behind"}))

    sut = []
    totals = {0: {"W": 0, "E": 0}, 1: {"W": 0, "E": 0}}
    drains = [(o, 0) for o in range(5, STOP[0], 5)] + [(o, 1) for o in range(135, 321, 5)]
    last0 = max(o for o, life in drains if life == 0)
    for offset, life in drains:
        stamp = _stamp(T0 + offset)
        start = -1 if life == 0 or merged_lives else STOP[1]
        sut.append(_line(stamp, "logit.process.uptime", "gauge", float(offset - start)))
        sending = offset <= 300
        w = PER_DRAIN if sending else 0
        if life == 1 and offset == 135:
            w = PER_DRAIN  # the generator's lines from the stop never arrive: fault-window loss
        if slow_after_stop and life == 1 and offset <= 180:
            w //= 2
        if offset == 60:
            w -= steady_loss
        if offset == 250:
            w += steady_surplus
        if offset == 135:
            w -= fault_loss
        if offset == 320:
            w += end_extra
        e = w
        if offset == last0:
            w += life0_extra
        if life == 1 and offset == 200:
            e -= e1_short
        a = e - (a1_short if life == 1 and offset == 210 else 0)
        ab = a - (ab1_short if life == 1 and offset == 220 else 0)
        if life == 0:
            ab -= ab0_short
        totals[life]["W"] += w
        totals[life]["E"] += ab
        for metric, value, comp in (("logit.input.datagrams", w, "in"),
                                    ("logit.component.events.sent", e, "in"),
                                    ("logit.component.events.received", a, "window"),
                                    ("logit.transform.metrics.absorbed", ab, "window"),
                                    ("logit.component.batches.received", 1, "sink"),
                                    ("logit.component.batches.delivered",
                                     0 if (life == 1 and offset in (200, 205) and undelivered)
                                     else 1, "sink")):
            sut.append(_line(stamp, metric, "sum", value, component=comp))
        sut.append(_line(stamp, "logit.component.buffer.batches", "gauge",
                         buffer_batches if life == 1 else 0, component="sink"))
        sut.append(_line(stamp, "logit.component.buffer.utilization", "gauge",
                         buffer_util if life == 1 else 0.0, component="sink"))
        sut.append(_line(stamp, "logit.component.receive.utilization", "gauge",
                         receive_util if life == 1 else 0.0, component="in"))
        sut.append(_line(stamp, "logit.component.retrying", "gauge", 0, component="sink"))

    timeline = [_phase("start", 0, t0=T0),
                _action("apply", "c0s1", "stop", "logit", STOP[0], affects_udp_ingress=True),
                _action("revert", "c0s1", "stop", "logit", STOP[1] - 1, STOP[1],
                        affects_udp_ingress=True),
                _ready("c0s1", "logit", STOP[1] + 1),
                _phase("end_begin", 300), _phase("end_end", 330)]
    run_dir = _run_dir(tmp, name, timeline, sut, ledger=LEDGER, **resolved)
    if empty_sut:
        (run_dir / "logs" / "logit.stdout").write_text("")
    (run_dir / "logs" / "generator.stdout").write_text("\n".join(gen) + "\n")

    def log(offset, level, message, **fields):
        return json.dumps({"timestamp": _stamp(T0 + offset), "level": level,
                           "message": message, "target": "logit", **fields})

    stderr = [log(0, "INFO", "ready")]
    if shutdown_drops:
        undecoded = shutdown_drops // 4
        stderr.append(log(STOP[0] + 0.5, "WARN", f"{shutdown_drops - undecoded} datagram(s) "
                          "still in the receive queue when this listener was stopped at its "
                          "shutdown grace, undecoded", component="in"))
        stderr.append(log(STOP[0] + 0.5, "WARN", f"{undecoded} datagram(s) taken off the "
                          "receive queue but not decoded when this listener was stopped at its "
                          "shutdown grace", component="in"))
    for offset, dropped in ((STOP[0] + 0.6, life0_dropped), (321, life1_dropped)):
        if dropped:
            stderr.append(log(offset, "WARN", "drain complete", duration="1s",
                              batches_dropped=dropped))
        else:
            stderr.append(log(offset, "INFO", "drain complete", duration="1ms"))
    (run_dir / "logs" / "logit.stderr").write_text("\n".join(stderr) + "\n")

    ab0 = totals[0]["E"]
    ab1 = totals[1]["E"]
    v0 = ab0 + v0_extra
    v1 = ab1 - v1_short
    export = []
    if late_series:
        # x_1 first appears in life 1, so it has no reset.
        export = [{"metric": {"__name__": "x_0_total"}, "values": [v0, v1 // 2],
                   "timestamps": [int((T0 + s) * 1000) for s in (100, 200)]},
                  {"metric": {"__name__": "x_1_total"}, "values": [v1 - v1 // 2],
                   "timestamps": [int((T0 + 300) * 1000)]}]
    elif not empty_export:
        for index, share in enumerate((v0 // 2, v0 - v0 // 2)):
            other = (v1 // 2, v1 - v1 // 2)[index]
            export.append({"metric": {"__name__": f"x_{index}_total"},
                           "values": [share // 2, share, other // 2, other],
                           "timestamps": [int((T0 + s) * 1000) for s in (50, 100, 200, 300)]})
    (run_dir / "vm-export.jsonl").write_text("".join(json.dumps(s) + "\n" for s in export))
    return run_dir


def _ledger_checks():
    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            run_dir = _ledger_run(tmp, name, **kwargs)
            return {r.id: r for r in checks.run_all(run_dir)}

        clean = results("ledger-clean")
        for check_id in ("ledger.wire", "ledger.intake", "ledger.edge", "ledger.aggregate",
                         "ledger.egress", "ledger.summary", "identity.sink", "recovery"):
            expect(clean[check_id].status == checks.PASS,
                   f"{check_id} PASSes the balanced two-life fixture, got {clean[check_id]}")
        led = checks.Ledger(checks.RunData(Path(tmp) / "ledger-clean"))
        expect(led.D_log == {0: 20}, f"the listener's shutdown warn lines go into D, got "
                                     f"{led.D_log}")
        expect(led.residual(0) == 30, f"life 0's W − D − B − Ab, got {led.residual(0)}")
        expect(led.R == 77000, f"R from the defaults, got {led.R}")
        expect("within [0, R]" in clean["ledger.intake"].detail,
               f"intake reports the earlier life's residual, got {clean['ledger.intake'].detail}")
        expect("fault (c0s1 stop on logit)" in " ".join(clean["ledger.wire"].lines),
               "wire buckets the stop as a UDP-affecting window")

        over_r = results("ledger-over-r", life0_extra=80000)
        expect(over_r["ledger.intake"].status == checks.FAIL,
               f"intake FAILs an earlier life's residual over R, got {over_r['ledger.intake']}")
        negative = results("ledger-negative-residual", life0_extra=0, v0_extra=0)
        expect(negative["ledger.intake"].status == checks.FAIL,
               "intake FAILs an earlier life's residual under 0, got "
               f"{negative['ledger.intake']}")

        short = results("ledger-intake", e1_short=5)
        expect(short["ledger.intake"].status == checks.FAIL,
               f"intake FAILs W − D != E + B in the final life, got {short['ledger.intake']}")
        expect(short["ledger.summary"].status == checks.FAIL,
               f"the summary FAILs a nonzero W − D − E − B, got {short['ledger.summary']}")

        edge = results("ledger-edge", a1_short=7)
        expect(edge["ledger.edge"].status == checks.FAIL,
               f"edge FAILs E != A in the final life, got {edge['ledger.edge']}")
        expect(edge["ledger.summary"].status == checks.FAIL,
               f"the summary FAILs a nonzero E − A, got {edge['ledger.summary']}")
        absorbed = results("ledger-aggregate", ab1_short=9)
        expect(absorbed["ledger.aggregate"].status == checks.FAIL,
               f"aggregate FAILs Ab != A in the final life, got {absorbed['ledger.aggregate']}")
        expect(absorbed["ledger.summary"].status == checks.FAIL,
               f"the summary FAILs a nonzero A − Ab, got {absorbed['ledger.summary']}")
        late = results("ledger-late-series", late_series=True)
        expect(late["ledger.egress"].status == checks.WARN
               and "resets aren't SUT lives − 1" in late["ledger.egress"].detail,
               f"egress WARNs a series whose resets aren't lives − 1, got {late['ledger.egress']}")

        unreached = results("ledger-residual-unreached", life0_extra=77020, v0_extra=0)
        expect(unreached["ledger.intake"].status == checks.PASS
               and unreached["ledger.egress"].status == checks.FAIL
               and "residual 77,000 + Ab − V 0, uncounted" in unreached["ledger.egress"].detail,
               f"egress FAILs an earlier life's residual that never reached V, got "
               f"{unreached['ledger.egress']}")
        unabsorbed = results("ledger-unabsorbed", ab0_short=750, v0_extra=0)
        expect(unabsorbed["ledger.egress"].status == checks.FAIL
               and "Ab − V 0, uncounted" in unabsorbed["ledger.egress"].detail,
               f"egress FAILs an earlier life's lines read but neither absorbed nor delivered, "
               f"got {unabsorbed['ledger.egress']}")

        uncounted = results("ledger-uncounted", v0_extra=-500)
        expect(uncounted["ledger.egress"].status == checks.FAIL
               and "uncounted" in uncounted["ledger.egress"].detail,
               f"egress FAILs an earlier life's positive gap with no batches_dropped, got "
               f"{uncounted['ledger.egress']}")
        counted = results("ledger-counted", v0_extra=-500, life0_dropped=3)
        expect(counted["ledger.egress"].status == checks.PASS
               and "counted (drain complete batches_dropped 3)" in counted["ledger.egress"].detail,
               f"egress marks a positive gap with batches_dropped as counted, got "
               f"{counted['ledger.egress']}")
        expect(counted["ledger.summary"].status == checks.PASS,
               f"a counted earlier life leaves the summary PASS, got {counted['ledger.summary']}")

        final_lost = results("ledger-final-uncounted", v1_short=40)
        expect(final_lost["ledger.egress"].status == checks.FAIL,
               f"egress FAILs a final-life gap, got {final_lost['ledger.egress']}")
        expect(final_lost["ledger.summary"].status == checks.FAIL,
               f"the summary FAILs an uncounted Ab − V, got {final_lost['ledger.summary']}")
        final_counted = results("ledger-final-counted", v1_short=40, life1_dropped=1)
        expect(final_counted["ledger.summary"].status == checks.PASS
               and "Ab − V 40 counted" in final_counted["ledger.summary"].detail,
               f"the summary shows a counted egress term and judges the rest, got "
               f"{final_counted['ledger.summary']}")
        final_dup = results("ledger-final-negative", v1_short=-40)
        expect(final_dup["ledger.egress"].status == checks.FAIL,
               f"egress FAILs a negative final-life gap, got {final_dup['ledger.egress']}")

        empty = results("ledger-empty-export", empty_export=True)
        expect(empty["ledger.egress"].status == checks.FAIL
               and "no series" in empty["ledger.egress"].detail,
               f"egress FAILs an export with no series, got {empty['ledger.egress']}")

        steady = results("wire-steady", steady_loss=400)
        expect(steady["ledger.wire"].status == checks.FAIL,
               f"wire FAILs loss outside UDP-affecting windows past the tolerance, got "
               f"{steady['ledger.wire']}")
        masked = results("wire-surplus-masks", steady_loss=400, steady_surplus=400)
        expect(masked["ledger.wire"].status == checks.FAIL
               and "steady loss judged 350" in masked["ledger.wire"].detail,
               f"wire FAILs steady loss that a surplus in another steady run would net out, "
               f"got {masked['ledger.wire']}")
        tolerated = results("wire-tolerated", steady_loss=100)
        expect(tolerated["ledger.wire"].status == checks.PASS,
               f"wire tolerates one batch per steady-run edge, got {tolerated['ledger.wire']}")
        in_fault = results("wire-fault", fault_loss=900)
        expect(in_fault["ledger.wire"].status == checks.PASS,
               f"wire doesn't judge loss inside a UDP-affecting window, got "
               f"{in_fault['ledger.wire']}")
        over_end = results("wire-end", end_extra=300)
        expect(over_end["ledger.wire"].status == checks.FAIL
               and "end allowance" in over_end["ledger.wire"].detail,
               f"wire FAILs W + K past G by more than the end allowance, got "
               f"{over_end['ledger.wire']}")
        within_end = results("wire-end-ok", end_extra=60)
        expect(within_end["ledger.wire"].status == checks.PASS,
               f"wire allows W + K past G by up to one batch at the end, got "
               f"{within_end['ledger.wire']}")

        unbalanced = results("identity", undelivered=1)
        expect(unbalanced["identity.sink"].status == checks.FAIL,
               f"identity.sink FAILs two batches neither delivered, dropped, nor queued, got "
               f"{unbalanced['identity.sink']}")

        rows = ("ledger.wire", "ledger.intake", "ledger.edge", "ledger.aggregate",
                "ledger.egress", "ledger.summary", "identity.sink", "recovery")
        no_sut = results("ledger-empty-sut", empty_sut=True)
        expect(all(no_sut[r].status == checks.SKIP and "no logit.process.uptime points"
                   in no_sut[r].detail for r in rows),
               f"every ledger row SKIPs empty SUT telemetry, got "
               f"{[no_sut[r] for r in rows]}")
        merged = results("ledger-merged-lives", merged_lives=True)
        expect(all(merged[r].status == checks.SKIP and "timeline starts the SUT into 2"
                   in merged[r].detail for r in rows),
               f"every ledger row SKIPs lives the timeline disagrees with, got "
               f"{[merged[r] for r in rows]}")
        unjudged = results("recovery-unjudged", recovery_bound=1.0)
        expect(unjudged["recovery"].status == checks.WARN,
               f"recovery WARNs when no fault has an eligible interval, got "
               f"{unjudged['recovery']}")

        # A 2-batch sink queue holding the batch it sends reads 0.5 full.
        small = results("recovery-small-queue", buffer_batches=1, buffer_util=0.5)
        expect(small["recovery"].status == checks.PASS,
               f"recovery PASSes a small queue holding one batch, got {small['recovery']}")
        full = results("recovery-full-queue", buffer_batches=1, buffer_util=1.0)
        expect(full["recovery"].status == checks.FAIL,
               f"recovery FAILs a one-batch queue that one batch fills, got {full['recovery']}")
        backed_up = results("recovery-backed-up", buffer_batches=2, buffer_util=0.5)
        expect(backed_up["recovery"].status == checks.FAIL,
               f"recovery FAILs a queue holding two batches over 5% full, got "
               f"{backed_up['recovery']}")
        busy = results("recovery-busy", receive_util=0.5)
        expect(busy["recovery"].status == checks.FAIL,
               f"recovery FAILs a receive queue still full, got {busy['recovery']}")
        slow = results("recovery-slow", slow_after_stop=True)
        expect(slow["recovery"].status == checks.FAIL,
               f"recovery FAILs ingest under 95% of the warmup rate, got {slow['recovery']}")
        behind = results("recovery-behind", slow_after_stop=True, rate_behind=2)
        expect(behind["recovery"].status == checks.WARN,
               f"recovery WARNs a shortfall when the generator fell behind, got "
               f"{behind['recovery']}")


def _expect_run(tmp, name, expectations, apply_rc=0, second_cycle=False):
    """A run directory for `check_expect`: c0s1 held over [+100, +190] (and c1s1 over
    [+300, +390] with `second_cycle`), and SUT drains every 5 s from +5 to +450.

    - `logit.input.datagrams{component=in}`: 1,000 per drain, none at +150 and +155.
    - `logit.component.datagrams.dropped{component=in}`: 10 per drain from +120 to +180 under
      `reason=overflow_oldest`, and 7 at +140 under `reason=shutdown`.
    - `logit.component.buffer.batches{component=sink}`: set to 5 at +50, 2 at +120, 0 at +200,
      and 3 at +330. Nothing else sets it, as a gauge only drains after it's set.
    - `logit.input.kernel.drops{component=in}`: 5 at +200, after the revert.
    - `logit.component.inbox.full{component=window}` exported as a gauge, a kind mismatch."""
    stdout = []
    for offset in range(5, 451, 5):
        stamp = _stamp(T0 + offset)
        stdout.append(_line(stamp, "logit.process.uptime", "gauge", float(offset)))
        if offset not in (150, 155):
            stdout.append(_line(stamp, "logit.input.datagrams", "sum", 1000, component="in"))
        if 120 <= offset <= 180:
            stdout.append(_line(stamp, "logit.component.datagrams.dropped", "sum", 10,
                                component="in", attrs={"reason": "overflow_oldest"}))
        if offset == 140:
            stdout.append(_line(stamp, "logit.component.datagrams.dropped", "sum", 7,
                                component="in", attrs={"reason": "shutdown"}))
        if offset in (50, 120, 200, 330):
            value = {50: 5, 120: 2, 200: 0, 330: 3}[offset]
            stdout.append(_line(stamp, "logit.component.buffer.batches", "gauge", value,
                                component="sink"))
        if offset == 200:
            stdout.append(_line(stamp, "logit.input.kernel.drops", "sum", 5, component="in"))
        stdout.append(_line(stamp, "logit.component.inbox.full", "gauge", 1, component="window"))
    timeline = [_phase("start", 0, t0=T0),
                _action("apply", "c0s1", "stop", "victoria-metrics", 100, rc=apply_rc),
                _action("revert", "c0s1", "stop", "victoria-metrics", 189, 190)]
    steps = [{"id": "c0s1", "spec_index": 0, "cycle": 0, "action": "stop",
              "on": "victoria-metrics", "args": "", "start": 100, "end": 190}]
    if second_cycle:
        timeline += [_action("apply", "c1s1", "stop", "victoria-metrics", 300),
                     _action("revert", "c1s1", "stop", "victoria-metrics", 389, 390)]
        steps.append({**steps[0], "id": "c1s1", "cycle": 1, "start": 300, "end": 390})
    timeline += [_phase("end_begin", 450), _phase("end_end", 480)]
    return _run_dir(tmp, name, timeline, stdout, expect=expectations, steps=steps)


def _expect_checks():
    def exp(**fields):
        base = {"name": "x", "service": "logit", "metric": "logit.input.datagrams",
                "attrs": {"component": "in"}, "step": "c0s1", "window": "during",
                "reduce": "delta", "min": None, "max": None}
        return {**base, **fields}

    with tempfile.TemporaryDirectory() as tmp:
        counter = 0

        def rows(expectations, **kwargs):
            nonlocal counter
            counter += 1
            run_dir = _expect_run(tmp, f"expect-{counter}", expectations, **kwargs)
            return [r for r in checks.run_all(run_dir) if r.id.startswith("expect")]

        def one(what, want, **fields):
            result = rows([exp(**fields)])[0]
            expect(result.status == want, f"{what}: want {want}, got {result}")
            return result

        none = rows([])
        expect(len(none) == 1 and none[0].id == "expect" and none[0].status == checks.SKIP,
               f"check_expect SKIPs a scenario with no expectations, got {none}")

        # c0s1's during window (+100, +190] holds drains +105..+190: 18 drains, two missing.
        result = one("delta sums the window's drains", checks.PASS, min=16000, max=16000)
        expect(result.id == "expect.x", f"the row id is expect.<name>, got {result.id}")
        one("delta under its min FAILs", checks.FAIL, min=16001)
        one("min_delta counts a drain with no point as 0", checks.FAIL, reduce="min_delta",
            min=1)
        one("min_delta passes a counter that rose in every drain", checks.PASS,
            reduce="min_delta", min=1000, window="after")
        one("after reads the recovery_bound after the revert", checks.PASS, window="after",
            min=9000, max=9000)
        one("attrs filter on reason", checks.PASS, metric="logit.component.datagrams.dropped",
            attrs={"component": "in", "reason": "overflow_oldest"}, min=130, max=130)
        one("a loss counter never emitted in the window reads 0", checks.PASS,
            metric="logit.input.kernel.drops", max=0)
        # through is (+100, +235]: 27 drains, two missing.
        one("through spans the fault and its recovery_bound", checks.PASS, window="through",
            min=25000, max=25000)
        one("through sees a counter that moves after the revert", checks.FAIL,
            window="through", metric="logit.input.kernel.drops", max=0)
        one("a counter that should stay 0 FAILs when it moved", checks.FAIL,
            metric="logit.component.datagrams.dropped", max=0)

        gauge = dict(metric="logit.component.buffer.batches", attrs={"component": "sink"})
        one("max sees the value in force at the window's start", checks.PASS, reduce="max",
            min=5, **gauge)
        one("min of a gauge over the window", checks.PASS, reduce="min", min=2, max=2, **gauge)
        one("min over its max FAILs", checks.FAIL, reduce="min", max=1, **gauge)
        one("last is the value in force at the window's end", checks.PASS, reduce="last",
            window="after", max=0, **gauge)
        one("last above its max FAILs", checks.FAIL, reduce="last", max=1, **gauge)
        one("a gauge never set FAILs", checks.FAIL, reduce="max", max=0,
            metric="logit.component.retrying", attrs={"component": "sink"})
        one("a reducer of the wrong kind for the run's points FAILs", checks.FAIL,
            metric="logit.component.inbox.full", attrs={"component": "window"}, min=1)

        one("a step the run's schedule doesn't have SKIPs", checks.SKIP, step="c1s1", min=0)
        result = rows([exp(min=1)], apply_rc=1)[0]
        expect(result.status == checks.FAIL and "never applied" in result.detail,
               f"a scheduled step whose apply failed FAILs, got {result}")
        # c0s1's max is 5 and c1s1's is 3.
        result = rows([exp(step=1, reduce="max", min=4, **gauge)], second_cycle=True)[0]
        expect(result.status == checks.FAIL and "c0s1" in result.detail
               and "c1s1" in result.detail,
               f"a step index judges every cycle's occurrence, got {result}")
        result = rows([exp(name="a", min=1), exp(name="b", max=0)])
        expect([(r.id, r.status) for r in result]
               == [("expect.a", checks.PASS), ("expect.b", checks.FAIL)],
               f"one row per expectation, got {result}")


def _gauge_lives():
    """A gauge a previous process set isn't in force in the next one. Life 0 drains every 5 s
    from +5 to +100 and sets `retrying` to 1 at +100; life 1 starts at +130, drains from +135,
    and sets it to 0 at +150."""
    lines = []
    for offset in list(range(5, 101, 5)) + list(range(135, 201, 5)):
        stamp = _stamp(T0 + offset)
        uptime = offset if offset <= 100 else offset - 130
        lines.append(_line(stamp, "logit.process.uptime", "gauge", float(uptime)))
        if offset in (100, 150):
            lines.append(_line(stamp, "logit.component.retrying", "gauge",
                               1 if offset == 100 else 0, component="sink"))
    tel = telemetry.parse_ndjson(lines)
    attrs = {"component": "sink"}
    value, _ = checks.reduce_gauge(tel, "logit.component.retrying", attrs, T0 + 130, T0 + 175,
                                   "max")
    expect(value == 0, f"a window opening between lives ignores the earlier life's value, "
                       f"got {value}")
    value, _ = checks.reduce_gauge(tel, "logit.component.retrying", attrs, T0 + 137, T0 + 145,
                                   "last")
    expect(value is None, f"a gauge the covering life hasn't set yet is unset, got {value}")
    value, _ = checks.reduce_gauge(tel, "logit.component.retrying", attrs, T0 + 100, T0 + 120,
                                   "last")
    expect(value == 1, f"the value in force inside one life still counts, got {value}")


def _kill_watchdog():
    """The watchdog around a `kill` of the SUT applied at +60 and reverted (started) by +90: its
    exit code, its start, its readiness probe, and the stdout gap it leaves."""
    kill = [_action("apply", "c0s1", "kill", "logit", 60, 60.1),
            _action("revert", "c0s1", "kill", "logit", 89, 90)]
    start, end = _phase("start", 0, t0=T0), [_phase("end_begin", 400), _phase("end_end", 450)]

    def run(tmp, name, steps=kill, code=137, ready=True, stdout=()):
        timeline = [start] + steps + ([_ready("c0s1", "logit", 91)] if ready else []) + end
        run_dir = _run_dir(tmp, name, timeline, stdout)
        first, second = _stamp(T0 - 5), _stamp(T0 + 89.5)
        watchdog = []
        for offset in range(0, 400, 5):
            for svc in ("logit", "generator"):
                exited = svc == "logit" and 60 < offset < 90
                watchdog.append({
                    "t": T0 + offset, "svc": svc, "status": "exited" if exited else "running",
                    "exit_code": code if exited else 0, "health": "healthy",
                    "started_at": second if svc == "logit" and offset >= 90 else first,
                    "finished_at": _stamp(T0 + 60.05) if svc == "logit" and offset > 60 else "",
                    "restart_count": 0})
        (run_dir / "watchdog.jsonl").write_text("".join(json.dumps(r) + "\n" for r in watchdog))
        (run_dir / "inspect").mkdir()
        for svc, started, finished in (("logit", second, T0 + 460), ("generator", first,
                                                                      T0 + 401)):
            (run_dir / "inspect" / f"{svc}.json").write_text(json.dumps({"State": {
                "Status": "exited", "ExitCode": 0, "OOMKilled": False, "StartedAt": started,
                "FinishedAt": _stamp(finished)}}))
        return run_dir

    with tempfile.TemporaryDirectory() as tmp:
        clean = run(tmp, "kill-clean")
        for check in (checks.check_exit, checks.check_restarts, checks.check_timeline):
            result = _status(clean, check)
            expect(result.status == checks.PASS,
                   f"{check.__name__} PASSes a scheduled kill and its start, got {result}")
        result = _status(clean, checks.check_exit)
        expect("1 kill(s)" in result.detail, f"exit names the kill, got {result}")
        wrong_code = run(tmp, "kill-code-1", code=1)
        result = _status(wrong_code, checks.check_exit)
        expect(result.status == checks.FAIL and "which leaves 137" in result.detail,
               f"exit FAILs an exit inside a kill with a code other than 137, got {result}")
        stop = [dict(record, action="stop") for record in kill]
        stopped = run(tmp, "stop-137", steps=stop)
        result = _status(stopped, checks.check_exit)
        expect(result.status == checks.FAIL and "which leaves 0" in result.detail,
               f"exit FAILs code 137 inside a scheduled stop, got {result}")
        unprobed = run(tmp, "kill-unprobed", ready=False)
        result = _status(unprobed, checks.check_timeline)
        expect(result.status == checks.FAIL and "no readiness probe" in result.detail,
               f"timeline FAILs a kill's start with no ready record, got {result}")
        quiet = [line for line in _sut_stdout(400)
                 if not 60 < (telemetry.parse_rfc3339(json.loads(line)["timestamp"]) - T0) < 90]
        gap = run(tmp, "kill-gap", stdout=quiet)
        result = _status(gap, checks.check_progress)
        expect(not any("went quiet" in line for line in result.lines),
               f"progress doesn't flag the stdout gap a kill leaves, got {result}")


# The kill fixture's SUT config: a small R (100 + 67 x 10 = 770), and the 10 s aggregate
# interval the band and `ledger.windows` read.
KILL_SUT_CONFIG = ("components:\n"
                   "  in: { type: statsd_in, receive: { max_datagrams: 100, "
                   "batch_max_events: 10 } }\n"
                   "  window:\n"
                   "    type: aggregate\n"
                   "    interval: 10s   # every window\n")
# Life 0 ends at a stop over +101..+131; life 1 at a kill over +203..+213; life 2 is final.
KILL_STOP = (101, 131)
KILL_KILL = (203, 213)


def _kill_run(tmp, name, kill_gap=1500, stray_drop=False, replayed=3, queued=None, drop=(),
              every_window=True, kill_residual=0, silence=None, idle=(), extra_sent=(),
              late_report=False, no_drains=None):
    """A three-life run for the ledger: life 1 ends by a kill. The generator and the SUT move
    `PER_DRAIN` lines per 5 s drain (200/s), and the export samples every 10 s. Lives 0 and 2
    balance. Life 1's Ab − V is `kill_gap`, the window the kill discarded; its band is
    [−1,000, 3,000]: the residual 0 plus 200/s x the 5 s drain interval below, 200/s x the 10 s
    window plus 200/s x the drain interval above. `stray_drop` puts a listener shutdown-drop
    line inside life 1, which a killed life can't write. Life 1's last drain shows `queued`
    batches (default `replayed`), and life 2's sink opens a spool holding `replayed` of them and
    delivers them in its first drain. `drop` deletes the export's samples at those offsets from
    every series, a window the aggregate flushed but the store lost; `idle` deletes those
    samples and the aggregate's flush of them, a window it never emitted. The aggregate reports
    each flush's batch in the drain at the flush's offset, or the next drain under
    `late_report`, and one more batch in each drain at the offsets in `extra_sent`. `no_drains`
    is a `(start, end)` span with no SUT drain, as a pause leaves. `every_window` sets `[ledger]
    vm_every_window`. `kill_residual` lines are read in the drain before life 1's last and never
    absorbed. `silence` is `(service, action)` of a fault over +240..+270 in life 2, or
    `(service, action, start, end)` of one over those offsets."""
    queued = replayed if queued is None else queued
    gen = []
    for offset in range(5, 301, 5):
        stamp = _stamp(T0 + offset)
        gen.append(_line(stamp, "logit.process.uptime", "gauge", float(offset)))
        for metric in ("logit.output.messages", "logit.output.datagrams"):
            gen.append(_line(stamp, metric, "sum", PER_DRAIN, component="out"))
        gen.append(_line(stamp, "logit.component.events.sent", "sum", PER_DRAIN,
                         component="load"))
        gen.append(_line(stamp, "logit.component.batches.sent", "sum", PER_DRAIN // 100,
                         component="load"))
        gen.append(_line(stamp, "logit.component.buffer.batches", "gauge", 0, component="out"))

    lives = [(0, -1, range(5, KILL_STOP[0], 5)),
             (1, KILL_STOP[1], range(135, KILL_KILL[0], 5)),
             (2, KILL_KILL[1], range(215, 321, 5))]
    sut = []
    absorbed = {}
    flushed = [s for s in (*range(10, 101, 10), *range(140, 201, 10), *range(220, 321, 10))
               if s not in idle]
    reported = [s + 5 if late_report else s for s in flushed] + list(extra_sent)
    for life, start, offsets in lives:
        for offset in offsets:
            if no_drains is not None and no_drains[0] < offset < no_drains[1]:
                continue
            stamp = _stamp(T0 + offset)
            if offset in reported:
                sut.append(_line(stamp, "logit.component.batches.sent", "sum",
                                 reported.count(offset), component="window"))
            sut.append(_line(stamp, "logit.process.uptime", "gauge", float(offset - start)))
            w = PER_DRAIN if offset <= 300 else 0
            absorbed[life] = absorbed.get(life, 0) + w
            unabsorbed = kill_residual if life == 1 and offset == offsets[-2] else 0
            for metric, value, comp in (("logit.input.datagrams", w + unabsorbed, "in"),
                                        ("logit.component.events.sent", w, "in"),
                                        ("logit.component.events.received", w, "window"),
                                        ("logit.transform.metrics.absorbed", w, "window"),
                                        ("logit.component.batches.received", 1, "sink"),
                                        ("logit.component.batches.delivered",
                                         1 + (replayed if offset == 215 else 0), "sink")):
                sut.append(_line(stamp, metric, "sum", value, component=comp))
            if offset == 215 and replayed:
                sut.append(_line(stamp, "logit.component.buffer.disk.replayed", "sum", replayed,
                                 component="sink"))
            sut.append(_line(stamp, "logit.component.buffer.batches", "gauge",
                             queued if offset == offsets[-1] and life == 1 else 0,
                             component="sink"))
            for metric, comp in (("logit.component.buffer.utilization", "sink"),
                                 ("logit.component.receive.utilization", "in"),
                                 ("logit.component.retrying", "sink")):
                sut.append(_line(stamp, metric, "gauge", 0, component=comp))

    timeline = [_phase("start", 0, t0=T0),
                _action("apply", "c0s1", "stop", "logit", KILL_STOP[0], affects_udp_ingress=True),
                _action("revert", "c0s1", "stop", "logit", KILL_STOP[1] - 1, KILL_STOP[1],
                        affects_udp_ingress=True),
                _ready("c0s1", "logit", KILL_STOP[1] + 1),
                _action("apply", "c0s2", "kill", "logit", KILL_KILL[0], KILL_KILL[0] + 0.1,
                        affects_udp_ingress=True),
                _action("revert", "c0s2", "kill", "logit", KILL_KILL[1] - 1, KILL_KILL[1],
                        affects_udp_ingress=True),
                _ready("c0s2", "logit", KILL_KILL[1] + 1)]
    if silence is not None:
        on, action, s_start, s_end = (*silence, 240, 270) if len(silence) == 2 else silence
        timeline += [_action("apply", "c0s3", action, on, s_start, s_start + 0.2),
                     _action("revert", "c0s3", action, on, s_end - 0.2, s_end)]
    timeline += [_phase("end_begin", 300), _phase("end_end", 330)]
    ledger = {**LEDGER, "vm_every_window": True} if every_window else LEDGER
    run_dir = _run_dir(tmp, name, timeline, sut, ledger=ledger,
                       configs={"sut": "logit-sut.yaml"})
    (run_dir / "configs").mkdir()
    (run_dir / "configs" / "logit-sut.yaml").write_text(KILL_SUT_CONFIG)
    (run_dir / "logs" / "generator.stdout").write_text("\n".join(gen) + "\n")

    def log(offset, level, message, **fields):
        return json.dumps({"timestamp": _stamp(T0 + offset), "level": level,
                           "message": message, "target": "logit", **fields})

    stderr = [log(0, "INFO", "ready"), log(KILL_STOP[0] + 0.6, "INFO", "drain complete"),
              log(321, "INFO", "drain complete")]
    if stray_drop:
        stderr.append(log(150, "WARN", "20 datagram(s) still in the receive queue when this "
                          "listener was stopped at its shutdown grace, undecoded",
                          component="in"))
    (run_dir / "logs" / "logit.stderr").write_text("\n".join(stderr) + "\n")

    # Life 1's samples carry its own timestamps: a spool replayed by life 2 delivers them late,
    # and the export still places them by time.
    totals = {0: absorbed[0], 1: absorbed[1] - kill_gap, 2: absorbed[2]}
    stamps = {0: range(10, 101, 10), 1: range(140, 201, 10), 2: range(220, 321, 10)}
    export = []
    for index in range(2):
        values, timestamps = [], []
        for life in (0, 1, 2):
            share = totals[life] // 2 if index == 0 else totals[life] - totals[life] // 2
            n = len(stamps[life])
            for i, s in enumerate(stamps[life]):
                if s not in drop and s not in idle:
                    values.append(share * (i + 1) // n)
                    timestamps.append(int((T0 + s) * 1000))
        export.append({"metric": {"__name__": f"x_{index}_total"}, "values": values,
                       "timestamps": timestamps})
    (run_dir / "vm-export.jsonl").write_text("".join(json.dumps(s) + "\n" for s in export))
    return run_dir


def _kill_ledger():
    rows = ("ledger.wire", "ledger.intake", "ledger.edge", "ledger.aggregate", "ledger.egress",
            "ledger.windows", "ledger.replay", "ledger.summary", "identity.sink", "recovery")
    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            return {r.id: r for r in checks.run_all(_kill_run(tmp, name, **kwargs))}

        clean = results("kill-clean")
        for row in rows:
            expect(clean[row].status == checks.PASS,
                   f"{row} PASSes the three-life fixture with a killed life, got {clean[row]}")
        led = checks.Ledger(checks.RunData(Path(tmp) / "kill-clean"))
        expect(led.killed == [1] and led.R == 770, f"life 1 is the killed one and R is 770, got "
                                                    f"{led.killed} {led.R}")
        egress = clean["ledger.egress"].detail
        expect("life 1 (killed): Ab 14,000 − V 12,500 = 1,500, within band" in egress
               and "band [-1,000, 3,000]: ingest 200/s" in egress
               and "residual W − D − B − Ab 0 at its last drain" in egress
               and "D has no stderr part" in egress,
               f"egress reports the killed life's gap inside its band, got {egress}")
        expect("life 2 (final): Ab 18,000 − V 18,000 = 0, ok" in egress,
               f"the final life stays exact, got {egress}")

        edge_high = results("kill-high-edge", kill_gap=3000)
        expect(edge_high["ledger.egress"].status == checks.PASS,
               f"a killed life's gap of one window plus one drain interval of ingest passes, got "
               f"{edge_high['ledger.egress']}")
        over = results("kill-over", kill_gap=3001)
        expect(over["ledger.egress"].status == checks.FAIL
               and "life 1 (killed): Ab 14,000 − V 10,999 = 3,001, uncounted"
               in over["ledger.egress"].detail,
               f"egress FAILs a killed life's gap above its band as uncounted, got "
               f"{over['ledger.egress']}")
        low = results("kill-low-edge", kill_gap=-1000)
        expect(low["ledger.egress"].status == checks.PASS,
               f"a killed life's V may exceed Ab by one drain interval of ingest, got "
               f"{low['ledger.egress']}")
        under = results("kill-under", kill_gap=-1001)
        expect(under["ledger.egress"].status == checks.FAIL
               and "= -1,001, surplus" in under["ledger.egress"].detail,
               f"egress FAILs a killed life's gap below its band as a surplus, got "
               f"{under['ledger.egress']}")
        residual = results("kill-residual", kill_residual=500, kill_gap=2600)
        detail = residual["ledger.egress"].detail
        expect(residual["ledger.egress"].status == checks.PASS
               and "Ab 14,000 − V 11,400 = 2,600, within band (band [-1,500, 3,000]" in detail
               and "residual W − D − B − Ab 500 at its last drain" in detail,
               f"a killed life's residual is reported beside Ab − V, never added to it, got "
               f"{residual['ledger.egress']}")
        residual_low = results("kill-residual-low", kill_residual=500, kill_gap=-1500)
        expect(residual_low["ledger.egress"].status == checks.PASS,
               f"a killed life's V may exceed Ab by its residual plus one drain interval of "
               f"ingest, got {residual_low['ledger.egress']}")
        residual_under = results("kill-residual-under", kill_residual=500, kill_gap=-1501)
        expect(residual_under["ledger.egress"].status == checks.FAIL,
               f"egress FAILs a surplus past the residual plus one drain interval, got "
               f"{residual_under['ledger.egress']}")
        last = results("kill-last-window", drop=(200,))
        expect(last["ledger.egress"].status == checks.FAIL
               and "uncounted" in last["ledger.egress"].detail
               and last["ledger.windows"].status == checks.FAIL,
               f"losing the killed life's last spooled window FAILs egress and windows, got "
               f"{last['ledger.egress']} {last['ledger.windows']}")

        expect(clean["ledger.windows"].status == checks.PASS
               and "2 series x 3 life/lives, widest gap 10s" in clean["ledger.windows"].detail,
               f"windows PASSes a sample in every window, got {clean['ledger.windows']}")
        middle = results("kill-middle-windows", drop=(150, 160, 170, 180, 190))
        expect(middle["ledger.windows"].status == checks.FAIL
               and "x_0_total: life 1 gap 60s from +140s to +200s"
               in middle["ledger.windows"].detail
               and middle["ledger.egress"].status == checks.PASS,
               f"windows FAILs five lost middle windows, which egress can't see, got "
               f"{middle['ledger.windows']} {middle['ledger.egress']}")
        tail = results("kill-tail-windows", drop=(180, 190, 200))
        expect(tail["ledger.windows"].status == checks.FAIL
               and "killed life 1's last sample at +170s, 33s before the kill at +203s"
               in tail["ledger.windows"].detail,
               f"windows FAILs a killed life's lost tail, got {tail['ledger.windows']}")
        head = results("kill-head-windows", drop=(220, 230))
        expect(head["ledger.windows"].status == checks.FAIL
               and "life 2's first sample at +240s, 27s after the life started"
               in head["ledger.windows"].detail,
               f"windows FAILs a later life's late first sample, got {head['ledger.windows']}")
        # The aggregate flushes every 10 s; its batch is reported in the drain at the flush.
        # Between samples at +240 and +270 it counts drains from +247.5 to +267.5.
        idle = results("windows-idle", idle=(250, 260))
        expect(idle["ledger.windows"].status == checks.PASS
               and "1 idle gap(s) excused: life 2 +240s to +270s (30s, 2 series), window sent 0 "
               "batch(es) over 4 drain(s) from +248s to +268s" in idle["ledger.windows"].detail,
               f"windows excuses a gap the aggregate flushed nothing in, got "
               f"{idle['ledger.windows']}")
        emitted = results("windows-idle-sent", idle=(250, 260), extra_sent=(255,))
        expect(emitted["ledger.windows"].status == checks.FAIL
               and "x_0_total: life 2 gap 30s from +240s to +270s, windows emitted but not "
               "stored: window sent 1 batch(es)" in emitted["ledger.windows"].detail,
               f"windows FAILs a gap the aggregate sent a batch in, got "
               f"{emitted['ledger.windows']}")
        # A flush reported one drain late puts +240's batch at +245 and +270's at +275, both
        # outside the span.
        idle_late = results("windows-idle-late", idle=(250, 260), late_report=True)
        lost_late = results("windows-lost-late", drop=(250, 260), late_report=True)
        expect(idle_late["ledger.windows"].status == checks.PASS
               and lost_late["ledger.windows"].status == checks.FAIL,
               f"windows counts neither sample's own flush when the drain reports it one drain "
               f"late, got {idle_late['ledger.windows']} {lost_late['ledger.windows']}")
        # The r43 shape: a partition of the SUT over +247..+258.92, the first sample after it
        # 11.08 s after the revert, and no flush between.
        r43 = results("windows-r43", idle=(250, 260),
                      silence=("logit", "partition", 247, 258.92))
        expect(r43["ledger.windows"].status == checks.PASS
               and "1 idle gap(s) excused" in r43["ledger.windows"].detail,
               f"windows excuses an idle gap ending 11.08 s after a partition, got "
               f"{r43['ledger.windows']}")
        pause = ("logit", "pause", 245, 265)
        before = results("windows-lost-before", drop=(240,), idle=(250, 260), silence=pause)
        expect(before["ledger.windows"].status == checks.FAIL
               and "life 2 gap 40s from +230s to +270s" in before["ledger.windows"].detail,
               f"windows FAILs a window the aggregate flushed right before a pause, got "
               f"{before['ledger.windows']}")
        after = results("windows-lost-after", drop=(270,), idle=(250, 260), silence=pause)
        expect(after["ledger.windows"].status == checks.FAIL
               and "life 2 gap 40s from +240s to +280s" in after["ledger.windows"].detail,
               f"windows FAILs a window the aggregate flushed right after a pause, got "
               f"{after['ledger.windows']}")
        # A paused SUT writes no drain and flushes nothing.
        paused = results("windows-pause-no-drains", idle=(250, 260, 270), no_drains=(242, 277),
                         silence=("logit", "pause", 242, 277))
        expect(paused["ledger.windows"].status == checks.PASS
               and "window sent 0 batch(es) over 0 drain(s)" in paused["ledger.windows"].detail,
               f"windows excuses a gap across a pause with no drains, got "
               f"{paused['ledger.windows']}")
        stopped = results("windows-generator-stop", idle=(250, 260, 270, 280, 290),
                          silence=("generator", "stop", 245, 285))
        expect(stopped["ledger.windows"].status == checks.PASS
               and "life 2 +240s to +300s (60s, 2 series)" in stopped["ledger.windows"].detail,
               f"windows excuses an idle gap of any length, got {stopped['ledger.windows']}")
        netem = results("windows-netem", drop=(250, 260, 270), silence=("logit", "netem"))
        expect(netem["ledger.windows"].status == checks.FAIL,
               f"windows FAILs windows the aggregate flushed whatever fault is running, got "
               f"{netem['ledger.windows']}")
        unset = results("kill-windows-unset", every_window=False)
        expect(unset["ledger.windows"].status == checks.SKIP,
               f"windows SKIPs without vm_every_window, got {unset['ledger.windows']}")
        expect(checks._component_duration(KILL_SUT_CONFIG, "window", "interval") == 10.0
               and checks._component_duration("components:\n  window: { type: aggregate, "
                                              "interval: 2m }\n", "window", "interval") == 120.0
               and checks._component_duration(KILL_SUT_CONFIG, "in", "interval") is None,
               "the aggregate interval reads from a block or flow component, and only its own")

        expect(clean["ledger.replay"].status == checks.PASS
               and "life 2's first drain replayed 3 vs killed life 1's last-drain "
               "buffer.batches 3" in clean["ledger.replay"].detail,
               f"replay ties the killed life's queue to the next life's replay, got "
               f"{clean['ledger.replay']}")
        short = results("kill-short-replay", replayed=1, queued=6)
        expect(short["ledger.replay"].status == checks.FAIL
               and "off by -5" in short["ledger.replay"].detail
               and short["identity.sink"].status == checks.PASS,
               f"replay FAILs 1 of 6 queued batches replayed, which identity.sink balances, got "
               f"{short['ledger.replay']} {short['identity.sink']}")
        in_flight = results("kill-replay-in-flight", replayed=3, queued=4)
        expect(in_flight["ledger.replay"].status == checks.PASS,
               f"replay allows one batch in flight, got {in_flight['ledger.replay']}")

        expect("received 21 + replayed 3 vs delivered 24" in clean["identity.sink"].detail,
               f"identity.sink counts a spool's replayed batches as received, got "
               f"{clean['identity.sink']}")
        stray = results("kill-stray-drop", stray_drop=True)
        stray_led = checks.Ledger(checks.RunData(Path(tmp) / "kill-stray-drop"))
        expect(1 not in stray_led.D_log and stray["ledger.intake"].status == checks.PASS,
               f"a killed life's D takes nothing from stderr, got {stray_led.D_log} "
               f"{stray['ledger.intake']}")


def _report():
    with tempfile.TemporaryDirectory() as tmp:
        run_dir = Path(tmp)
        rows = [checks.Result("run", checks.PASS, "ok")]
        (run_dir / "scenario.resolved.json").write_text(json.dumps(
            {"name": "random-faults", "duration": 3600.0, "seed": 42}))
        report.write(run_dir, rows)
        text = (run_dir / "results.md").read_text()
        expect(", 3600s scheduled, random schedule seed 42." in text
               and json.loads((run_dir / "results.json").read_text())["seed"] == 42,
               f"results.md and results.json carry the seed, got {text[:200]}")
        (run_dir / "scenario.resolved.json").write_text(json.dumps(
            {"name": "statsd-vm", "duration": 960.0, "seed": None}))
        report.write(run_dir, rows)
        expect("seed" not in (run_dir / "results.md").read_text().splitlines()[2],
               "a fixed schedule's header names no seed")


def _shipped(root):
    paths = scenario.shipped(root)
    expect(bool(paths), "at least one shipped scenario")
    for path in paths:
        try:
            loaded = scenario.load(path)
            expect(loaded.name == path.parent.name,
                   f"{path}: name {loaded.name!r} matches its directory")
        except scenario.ScenarioError as err:
            expect(False, f"{path}: {err}")


def run(root, quiet=False):
    """Returns 0 when every expectation holds, else prints the failures and returns 1."""
    root = Path(root)
    _FAILURES.clear()
    _PASSED[0] = 0
    for part in (_durations, _rules, _expect_rules, _expand, _random, _log_capture, _signals,
                 _deadlines,
                 _hung_inspect, _ndjson, _stderr, _slope, _checks, _vm_resets, _ledger_checks, _kill_watchdog,
                 _kill_ledger, _expect_checks, _gauge_lives, _report, _shipped):
        try:
            takes_root = (_rules, _expect_rules, _expand, _random, _signals, _hung_inspect,
                          _shipped)
            part(root) if part in takes_root else part()
        except Exception as err:  # report the part that broke, then keep going
            _FAILURES.append(f"{part.__name__}: {type(err).__name__}: {err}")
    if _FAILURES:
        for failure in _FAILURES:
            print(f"self-test FAIL: {failure}")
        return 1
    if not quiet:
        print(f"self-test: {_PASSED[0]} expectations passed")
    return 0
