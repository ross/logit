"""`soak.py self-test`: the driver's pure parts against embedded fixtures, plus `validate()` over
every shipped scenario. `run` calls it first, so a broken reducer or rule can't score a run.

It covers the duration parser, every `validate()` rejection rule, `expand()`'s cycle
repetition, the NDJSON reducers (counters per life split where uptime decreases, gauge series),
the stderr classifier (a raw `thread '...' panicked at` line included), the slope function, and
the checks against small synthetic run directories: a failed fault action, an aborted run and its
exit code, an fd sample inside a later fault, and a steady state too thin to judge. For the
ledger, it covers the reset-aware VictoriaMetrics total and its split per life, at a decrease
and at a life start with none, and a two-life run whose final life balances and whose earlier
life's residual reaches V, each rule broken in turn: a shutdown drop telemetry and stderr both
carry counted once, and either copy alone WARNed, a counted and an
uncounted egress gap, an earlier life's residual that never reaches V, an empty export, wire loss
in and out of a fault window and at the end, the sink identity, `drops` (a WARN for each
`*.dropped` no other row judges, a FAIL for a rejected one, and a PASS for the terms the ledger
balances), and recovery, a two-batch sink
queue holding one batch and a one-batch queue it fills included. Empty SUT telemetry, and lives the timeline disagrees with,
SKIP every ledger row. For `[[expect]]`, it covers every validation rule and each reducer
against a synthetic run: the window's open start, `through` reaching past the revert, a drain
with no point, a gauge's value in force at the window's start and never across a SUT life,
attribute filters, a step index across cycles, a step the schedule lacks, and a step whose apply
failed. For a `kill`, it covers its validation rules (the SUT only), the watchdog around it
(exit code 137 only inside a scheduled kill, its start, its readiness probe, and the stdout gap
it leaves), and a three-life run whose middle life ends by a kill: its egress band at both
edges, with and without a residual, a surplus below it and a lost last window above it, a D
with no shutdown part, the final life's sink identity counting a spool's replayed batches,
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

For an external target, it covers every `[target]` validation rule and what such a target
refuses (a VictoriaMetrics ledger key, a fault on `victoria-metrics`), that compose.yaml passes
each variable a target may list and keeps `victoria-metrics` in the `local` profile, the env
file check and its `export` and quote grammar, the private plain copy compose reads (its
directory, mode, content, and removal, an early return from `execute()` included), the compose arguments and profile each kind of run gets, the end sequence's wait for
the sink's queue to empty, and the checks over an external two-life run: `ledger.sent` reading
SENT, a rejected batch and rejected records each FAILing with the status and the sink's text, a
non-2xx answer inside and outside a fault window, counted drops and encoder skips WARNing (and
left out of `drops`), a
batch neither delivered nor dropped, and no request telemetry; the rows that read V SKIP; and a
local run's `ledger.sent` SKIPs.

For `mark` and `exit` steps, it covers every validation rule (`ready` on a `mark` only and a
known word, `code` and `key` required on an `exit` and refused elsewhere, an `exit` on the SUT
only, no netem inside one, neither in a `[random]` schedule) and `[ledger] sut_script`; the
driver around an `exit` (its revert starts only an exited container, and fail-fast accepts
`exited` only inside its window); `ready` from `health_log`'s status word (`ok` in steady
state, a mark's word inside it, `degraded` only inside an exit step, a sample timed at its
`Start` and counted once, and the `Health.Status` fallback); the watchdog around an expected
exit (its code, its start, the two self-log lines it needs and the `Error: ` line after them,
and an exit that never came); and the ledger with a script between the listener and
`aggregate`: `ledger.script`'s identities, the egress gap without the flush events, a stopped
life's gap judged with no allowance for the script, the wider residual band, and an earlier life
ended by an expected exit.

For service roles and several sinks, it covers compose.yaml's `soak.role` labels against
`BASE_ROLES`, reading a role map from `docker compose config`'s JSON, every role-map rule, the
rules that need the service list (a step or a sink on a service the stack lacks, a `kill` on a
second backend, an `[[expect]]` on a backend), the fault predicates by role, and every
`[[ledger.sink]]`, `split`, and `compose` rule; `prepare()` with an override and a given role
map, with no role map and no daemon, and with a role map the scenario's services miss; a
backend's port re-read alone after its revert, freshness per sink, and the end sequence's wait
for every sink's total. Over two exports, under `copy` and `route`, it covers each per-sink row:
a sink short or empty while the other balances, a surplus at one sink that can't net out loss
at the other, the summed V under `route`, a reset on one backend only, a sink edge short of
what was sent to it, and `identity.sink`, `recovery`, and `progress` naming the sink that broke.

For several generators, it covers every `[[ledger.source]]` and per-generator `[configs]` rule
(the old scalar `[ledger]` keys refused, a missing or empty list, a missing or unknown key, two
sources on one generator or one listener, a source on a service that isn't a generator, a
generator with no source, and `sut_transforms`), each generator's compose variable, the fault
predicates per source, `compose.env` and the end sequence with two generators, and a two-source
run: wire loss inside its own generator's fault window passing and inside the other's FAILing,
each source's datagram precondition, an imbalance on one listener FAILing named, the listeners'
summed E against A, a source's transforms judged hop by hop, `lines_in`'s oversize drops in B,
R per listener from its own `receive:`, and `recovery` against the summed rate.

For a source's `wire`, it covers the key and its load-time check against the SUT config's
listener `type:` and `transport:` (a mismatch either way, a listener the config lacks, a
transport with no wire, `lines_in`'s TCP default, and a comment ignored), the fault predicates
per wire, each TCP window judged or reported, a partition recording its addresses through a
fake Docker and the checks reading them, the driver recording every wire's flag, and a TCP
run: W from `input.frames`, the reversed datagram precondition, R with no receive queue, a
framer's counted drop as wire loss kept out of `drops`, the same lines uncounted FAILing, a
generator netem judged on TCP but not on UDP, a SUT netem judged, a judged window's surplus
against its ambiguous drops, a partition judged or reported by its address, `recovery` and a
killed life's band reading the frames, and a source naming `wire = "udp"` scoring as one
naming none.

For two SUT services, it covers a further SUT declared by its own `[configs]` key and the
`[[ledger.hop]]` joining it to `logit` (the chain they make, their config variable, and each
rule the keys, the hop, and the role map are held to), the fault predicates, under which only
`logit` can lose UDP, `compose.env`, the aggregate window read from the last SUT's config, the
end sequence stopping each SUT in chain order with a phase each and waiting before `logit`
stops until its hop sink is drained with no frame in flight, and a two-SUT run: a kill on either
side making two lives there alone, a kill the telemetry doesn't show named for its service, the
listener rows on `logit`'s lives and the rows from `aggregate` on on `collector`'s, fed by
`logit_in`'s events sent, V split on `collector`'s lives, `ledger.hop` exact with no kill or
pause and within its allowance for a killed side's tail and a send window of duplicates after a
kill or a pause of `collector`, a counted hop drop, a rejected hop batch, `ledger.summary`'s
end-to-end bound, the hop sink's replay band and identity, `recovery` on each side's own drains,
`drops` keyed on service, and an end-sequence phase read for its own service.

For a stand-in target, it covers the Snappy decoder against blocks `snap::raw` wrote
(`standin/snap-vectors.json`) and the malformed blocks it refuses, the `WriteRequest` walker,
V rebuilt from a record (a later write replacing an earlier one, a same-value resend counted, a
changed value and an undecodable body named), the stand-in's own handlers on loopback ports (its
cue rules, each answer, a reset, a wrong path, and the record line each prints), `post_cue`,
every `respond`, `class`, and `[target] kind = "standin"` rule, the stand-in's compose service,
the driver re-reading its control port and sending a step's cue and an empty one, and the checks
over a stand-in two-life run: V judged as an export, the black-hole control FAILing as uncounted
loss, each `ledger.standin` rule broken in turn, each class's signature with and without its
effect, under both postures, and a `rejected` drop excused inside a cued step and FAILing
outside it. For the `datadog` shape, it covers a series body `datadog_out` sent
(`standin/datadog-vectors.json`) through gzip and the `MetricPayload` walker, the other
encodings and the bodies refused, V as the distinct points' sum (a duplicate named, a changed
value summed), the `series_errors` answer and cue rules on loopback ports, the `[target] shape`,
`series_errors`, and class rules, and the checks over a datadog stand-in run: duplicates
reported under `at_least_once` and FAILing under `at_most_once` or after a 2xx, the
`series_errors` and rejected-step reconciliation against `records.dropped`, and `ledger.sent`
and `drops` with cued and uncued refusals and drops.

No part can reach a daemon. `run` sets `$DOCKER` to a path that doesn't exist for the whole
suite and restores the caller's value after it, so a `Docker()` built without `prefix=`, through
a slip in a part's own stand-ins included, gets rc 127 from `Docker.run`'s `OSError` instead of
running a command. A part that passes `prefix=` names its own fake and is unaffected.
"""

import base64
import copy
import gzip
import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import tomllib
import zlib
from datetime import datetime, timezone
from pathlib import Path

from . import (checks, collect, docker, driver, faults, report, scenario, standin, telemetry,
               topic, vm)

_FAILURES = []
_PASSED = [0]
_NO_DAEMON = "/nonexistent/soak-self-test-docker"


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


def _refused(path, raw, needle, what, seed=None, duration=None, roles=None):
    """Expects `raw` refused with a problem containing `needle`, by `load()`'s rules or by
    `service_problems()` against `roles`, the base role map by default."""
    try:
        loaded = scenario.from_dict(raw, path)
        scenario.validate(loaded, duration=duration, seed=seed)
        scenario.check_services(loaded, scenario.base_roles(loaded) if roles is None
                                else roles)
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
    _refused(path, mutate(lambda r: r["ledger"].pop("sut_aggregate")),
             "ledger: missing `sut_aggregate`", "a missing ledger key")
    _refused(path, copy.deepcopy(base), "--seed applies to a [random] schedule only",
             "--seed on a fixed schedule", seed=7)
    _refused(path, copy.deepcopy(base), "leaves no time", "a duration inside warmup+cooldown",
             duration=150)


MARK = {"at": "11m10s", "action": "mark", "on": "logit", "for": "60s", "ready": "stalled"}
EXIT = {"at": "11m10s", "action": "exit", "on": "logit", "for": "60s", "code": 2,
        "key": "memory_limit_exceeded"}


def _expected_event_rules(root):
    """`mark` and `exit` steps, their fields, and `[ledger] sut_script`."""
    path, base = _base(root)

    def with_step(**fields):
        raw = copy.deepcopy(base)
        raw["step"].append({k: v for k, v in fields.items() if v is not None})
        return raw

    for fields, what in ((MARK, "a mark with a ready word"),
                         ({**MARK, "ready": None}, "a mark with no ready word"),
                         ({**MARK, "on": "victoria-metrics", "ready": None},
                          "a mark on another service"),
                         (EXIT, "an exit with a code and a key")):
        try:
            loaded = scenario.from_dict(with_step(**fields), path)
            step = next(s for s in scenario.expand(loaded) if s.action == fields["action"])
            expect(step.to_json().get("ready") == fields.get("ready")
                   and step.to_json().get("code") == fields.get("code"),
                   f"{what}: the expanded step carries its fields, got {step.to_json()}")
        except scenario.ScenarioError as err:
            expect(False, f"{what} is refused: {err.problems}")
    expect(faults.ACTIONS["mark"].affects_udp_ingress(scenario.Step(
        "c0s8", 7, 0, "mark", "logit", "", 0, 1), scenario.BASE_ROLES),
        "a mark on the SUT is UDP-affecting")
    expect(not faults.ACTIONS["mark"].affects_udp_ingress(scenario.Step(
        "c0s8", 7, 0, "mark", "victoria-metrics", "", 0, 1), scenario.BASE_ROLES),
        "a mark on VictoriaMetrics isn't UDP-affecting")
    expect("exit" in faults.STARTS_ON_REVERT and "exit" not in faults.EXIT_CODES,
           "an exit's revert is a start, and its code comes from the step")

    cases = [
        ({**MARK, "ready": "ok"}, "`ready` must be one of", "a mark expecting ok"),
        ({**MARK, "ready": "wedged"}, "`ready` must be one of", "an unknown ready word"),
        ({**MARK, "action": "pause", "on": "generator"}, "`ready` is for mark only",
         "ready on a pause"),
        ({**MARK, "code": 2}, "`code` is for exit only", "code on a mark"),
        ({**MARK, "key": "x"}, "`key` is for exit only", "key on a mark"),
        ({**EXIT, "code": None}, "exit needs `code`", "an exit without code"),
        ({**EXIT, "key": None}, "exit needs `key`", "an exit without key"),
        ({**EXIT, "code": 256}, "`code` must be an integer from 0 to 255", "a code over 255"),
        ({**EXIT, "code": "2"}, "`code` must be an integer", "a string code"),
        ({**EXIT, "code": True}, "`code` must be an integer", "a boolean code"),
        ({**EXIT, "key": ""}, "`key` must be a self-log key", "an empty key"),
        ({**EXIT, "on": "generator"}, "exit is for logit only", "an exit on the generator"),
        ({**EXIT, "ready": "degraded"}, "`ready` is for mark only", "ready on an exit"),
    ]
    for fields, needle, what in cases:
        _refused(path, with_step(**fields), needle, what)
    netem_in_exit = with_step(**EXIT)
    netem_in_exit["step"].append({"at": "11m20s", "action": "netem", "on": "logit",
                                  "args": "delay 10ms", "for": "5s"})
    _refused(path, netem_in_exit, "netem on logit during its exit", "netem during an exit")
    _refused(path, with_step(**{**MARK, "at": "10m50s"}), "two faults overlap on logit",
             "a mark overlapping a fault on its container")
    script = copy.deepcopy(base)
    script["ledger"]["sut_script"] = ""
    _refused(path, script, "ledger.sut_script must name", "an empty sut_script")
    script["ledger"]["sut_script"] = "enrich"
    try:
        expect(scenario.from_dict(script, path).ledger["sut_script"] == "enrich",
               "a sut_script loads")
    except scenario.ScenarioError as err:
        expect(False, f"a sut_script is refused: {err.problems}")

    raw = tomllib.loads((root / "scenarios" / "random-faults" / "scenario.toml").read_text())
    for action in ("mark", "exit"):
        changed = copy.deepcopy(raw)
        changed["random"]["fault"][0].update(action=action, on="logit", args=[])
        _refused(root / "scenarios" / "random-faults" / "scenario.toml", changed,
                 "write it as a [[step]]", f"`{action}` in a [random] schedule")


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
        (dict(metric="logit.input.kernel.dropped", reduce="max"),
         "reads a gauge, and logit.input.kernel.dropped is a sum", "a gauge reducer on a counter"),
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



class _EnvDocker:
    def inspect(self, container, timeout=None):
        return {"Id": container, "State": {"Status": "exited"},
                "Config": {"Env": ["DD_API_KEY=secret", "PATH=/usr/bin"]}}


def _inspect_redacted():
    with tempfile.TemporaryDirectory() as tmp:
        collect.service_inspect(tmp, _EnvDocker(), {"logit": "a"})
        text = (Path(tmp) / "inspect" / "logit.json").read_text()
        written = json.loads(text)
        expect("secret" not in text
               and written["Config"]["Env"] == ["DD_API_KEY=<redacted>", "PATH=<redacted>"]
               and written["State"] == {"Status": "exited"},
               f"inspect/<svc>.json keeps no environment value, got {written['Config']}")

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
            "recovery_bound": 45.0, "ledger": {"sink": [{"id": "sink"}], "split": "copy"},
            "services": dict(scenario.BASE_ROLES),
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


LEDGER = {"source": [{"service": "generator", "generator_input": "load",
                      "generator_sink": "out", "sut_listener": "in"}],
          "sut_aggregate": "window", "wire_loss_outside_faults": 0.0, "split": "copy",
          "sink": [{"id": "sink", "backend": "victoria-metrics",
                    "vm_selector": "{__name__=~\"x_[0-9]+_total\"}"}]}
# The fixture's SUT stop: applied at +101, reverted by +131, so life 1 starts at +131.
STOP = (101, 131)
PER_DRAIN = 1000


def _ledger_run(tmp, name, life0_extra=50, shutdown_drops=20, v0_extra=30, v1_short=0,
                life0_dropped=0, life1_dropped=0, steady_loss=0, fault_loss=0, end_extra=0,
                e1_short=0, a1_short=0, ab1_short=0, late_series=False, receive_util=0.0, slow_after_stop=False, rate_behind=0,
                undelivered=0, empty_export=False, ab0_short=0, merged_lives=False,
                empty_sut=False, buffer_batches=0, buffer_util=0.0, steady_surplus=0,
                external=False, sut_extra=(), stderr_extra=(), timeline_extra=(),
                script=None, l_in_short=0, l_err_unreported=0, l_sent_short=0,
                sut_end="stop", shutdown_in=("telemetry", "stderr"), dropped_at="sink",
                dropped_reason="shutdown", kafka=False, topic_extra=(), topic_read=True, topic_records=None,
                standin_edit=None, standin_shape="remote_write", **resolved):
    """A two-life run directory for the ledger checks. The generator sends `PER_DRAIN` lines
    per 5 s drain from +0 to +300; the SUT is stopped over `STOP`; each keyword breaks one
    rule. By default life 0 ends with `life0_extra` read but not absorbed, `shutdown_drops` of
    them dropped at shutdown, and `v0_extra` reaching VictoriaMetrics after its last drain, and
    life 1 balances. `shutdown_in` names where the shutdown drops, of datagrams and of
    `life0_dropped`/`life1_dropped` batches, are written: telemetry, as a dedicated `internal`
    pipeline exports them, and stderr, as the listener's `warn` lines and `drain complete`.
    `dropped_at` names the component whose telemetry carries those batches, and
    `dropped_reason` their reason; a reason other than `shutdown` leaves them out of stderr,
    whose `drain complete` counts shutdown drops alone.

    `script` puts a `lua` component, `lua`, between the listener and `window`: a dict of the
    events it drops (`drop`), fails on (`err`), and returns from `flush()` (`flush`) per drain,
    each flush event one metric `window` absorbs and V excludes. `l_in_short`,
    `l_err_unreported`, and `l_sent_short` break its hop in life 1's drain at +200. `sut_end`
    `"exit"` ends life 0 with an `exit` step (code 2) in place of the stop, its revert starting
    the SUT.

    With `kafka`, the target is a Kafka topic: the same V lands as four delta records in
    `kafka-topic.ndjson`, two per life, plus `topic_extra` lines, and the timeline records the
    read unless `topic_read` is false. `topic_records(v0, v1)`, when given, returns the
    `(offset, amount)` records to write in place of the four.

    With `standin_edit`, the target is a stand-in: the same export becomes its record, one
    stored `200` request per sample timestamp a second before it (`_standin_record`), the SUT's
    `logit.output.requests` counts each in the drain after it, and `standin_edit(requests)`
    returns the request lines to write, edited. Under `standin_shape` `"datadog"` the record is
    `_datadog_record`'s, the export's samples as delta points."""
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
        flush = 0
        if script is not None:
            l_in = e - (l_in_short if life == 1 and offset == 200 else 0)
            drop, err, flush = script["drop"], script["err"], script["flush"]
            emit = l_in - drop - err
            l_sent = emit + flush - (l_sent_short if life == 1 and offset == 200 else 0)
            for metric, value, attrs in (
                    ("logit.component.events.received", l_in, None),
                    ("logit.script.events.emitted", emit, {"outcome": "emit"}),
                    ("logit.component.events.discarded", drop, None),
                    ("logit.component.events.dropped", err
                     - (l_err_unreported if life == 1 and offset == 200 else 0),
                     {"reason": "script_error"}),
                    ("logit.component.errors", err, {"reason": "process"}),
                    ("logit.component.flush.events", flush, None),
                    ("logit.component.events.sent", l_sent, None)):
                sut.append(_line(stamp, metric, "sum", value, component="lua", attrs=attrs))
            e_into_window = l_sent
        else:
            e_into_window = e
        a = e_into_window - (a1_short if life == 1 and offset == 210 else 0)
        ab = a - (ab1_short if life == 1 and offset == 220 else 0)
        if life == 0:
            ab -= ab0_short
        totals[life]["W"] += w
        # What V holds: every increment `window` absorbed but the script's flush events.
        totals[life]["E"] += ab - flush
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
        # The final drain of each life carries its shutdown drops.
        if "telemetry" in shutdown_in and offset == (last0 if life == 0 else drains[-1][0]):
            if life == 0 and shutdown_drops:
                sut.append(_line(stamp, "logit.component.datagrams.dropped", "sum",
                                 shutdown_drops, component="in", attrs={"reason": "shutdown"}))
            batches = life0_dropped if life == 0 else life1_dropped
            if batches:
                sut.append(_line(stamp, "logit.component.batches.dropped", "sum", batches,
                                 component=dropped_at, attrs={"reason": dropped_reason}))

    end_fields = {"code": 2, "key": "memory_limit_exceeded"} if sut_end == "exit" else {}
    timeline = [_phase("start", 0, t0=T0),
                _action("apply", "c0s1", sut_end, "logit", STOP[0], affects_udp_ingress=True,
                        **end_fields),
                _action("revert", "c0s1", sut_end, "logit", STOP[1] - 1, STOP[1],
                        affects_udp_ingress=True, **end_fields,
                        **({"started": True} if sut_end == "exit" else {})),
                _ready("c0s1", "logit", STOP[1] + 1),
                _phase("end_begin", 300), _phase("end_end", 330)] + list(timeline_extra)
    if external:
        resolved.setdefault("target", {"kind": "external", "name": "datadog",
                                       "env": ["DD_API_KEY"]})
        ledger = {**LEDGER, "sink": [{"id": "sink"}]}
    elif standin_edit is not None:
        resolved.setdefault("target", {"kind": "standin", "name": "standin", "env": [],
                                       "shape": standin_shape})
        resolved.setdefault("services", {"logit": "sut", "generator": "generator",
                                         "standin": "standin"})
        ledger = resolved.pop("ledger", {**LEDGER, "sink": [{"id": "sink"}]})
    elif kafka:
        resolved.setdefault("target", {"kind": "kafka", "name": "kafka", "env": [],
                                       "topic": "soak"})
        ledger = {**LEDGER, "sink": [{"id": "sink"}]}
        if topic_read:
            timeline.append(_phase("kafka_read", 325, topic="soak", offsets={"0": 4},
                                   end_offsets=4, records=4, processed=4))
        else:
            timeline.append(_phase("kafka_read_failed", 325, step="offsets", rc=1,
                                   stderr="broker down"))
    else:
        ledger = dict(LEDGER)
    if script is not None:
        ledger["sut_script"] = "lua"
    run_dir = _run_dir(tmp, name, timeline, list(sut) + list(sut_extra), ledger=ledger,
                       **resolved)
    if empty_sut:
        (run_dir / "logs" / "logit.stdout").write_text("")
    (run_dir / "logs" / "generator.stdout").write_text("\n".join(gen) + "\n")

    def log(offset, level, message, **fields):
        return json.dumps({"timestamp": _stamp(T0 + offset), "level": level,
                           "message": message, "target": "logit", **fields})

    stderr = [log(0, "INFO", "ready")]
    if shutdown_drops and "stderr" in shutdown_in:
        undecoded = shutdown_drops // 4
        stderr.append(log(STOP[0] + 0.5, "WARN", f"{shutdown_drops - undecoded} datagram(s) "
                          "still in the receive queue when this listener was stopped at its "
                          "shutdown grace, undecoded", component="in"))
        stderr.append(log(STOP[0] + 0.5, "WARN", f"{undecoded} datagram(s) taken off the "
                          "receive queue but not decoded when this listener was stopped at its "
                          "shutdown grace", component="in"))
    for offset, dropped in ((STOP[0] + 0.6, life0_dropped), (321, life1_dropped)):
        if dropped and "stderr" in shutdown_in and dropped_reason == "shutdown":
            stderr.append(log(offset, "WARN", "drain complete", duration="1s",
                              batches_dropped=dropped))
        else:
            stderr.append(log(offset, "INFO", "drain complete", duration="1ms"))
    stderr += [log(offset, level, message, **fields)
               for offset, level, message, fields in stderr_extra]
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
    if standin_edit is not None:
        record = (_datadog_record(export) if standin_shape == standin.DATADOG
                  else _standin_record(export))
        requests = standin_edit(record)
        _write_standin(run_dir, requests, shape=standin_shape)
        with open(run_dir / "logs" / "logit.stdout", "a") as out:
            out.write("\n".join(_standin_telemetry(requests)) + "\n")
    if external or kafka or standin_edit is not None:
        # None of these targets has VictoriaMetrics, so the driver writes no export.
        export = []
    _write_export(run_dir, "sink", export)
    if kafka:
        shares = ((50, v0 // 2), (100, v0 - v0 // 2), (200, v1 // 2), (300, v1 - v1 // 2))
        if topic_records is not None:
            shares = topic_records(v0, v1)
        records = [_topic_record(T0 + offset, amount) for offset, amount in shares]
        (run_dir / topic.TOPIC_FILE).write_text(
            "\n".join(records + list(topic_extra)) + "\n")
    return run_dir


def _standin_request(seq, offset, series, status=200, stored=True, cue=None, answer="status",
                     encoding="snappy", path="/api/v1/write"):
    """One stand-in record line at `T0 + offset` whose body holds `series`, `[(labels,
    [(timestamp_ms, value)])]`, compressed as literals."""
    body = standin.snappy_compress_literal(standin.encode_write_request(series))
    return {"event": "request", "seq": seq, "t": T0 + offset, "path": path,
            "content_type": "application/x-protobuf", "content_encoding": encoding,
            "bytes": len(body), "cue": cue, "answer": answer,
            "status": None if answer == "reset" else status, "stored": stored,
            "body_b64": base64.b64encode(body).decode()}


def _standin_record(export):
    """The requests a stand-in would record for `export`: one stored `200` per sample
    timestamp, a second before it, holding every series' sample at that timestamp."""
    by_stamp = {}
    for one in export:
        labels = dict(one["metric"])
        for stamp, value in zip(one["timestamps"], one["values"]):
            by_stamp.setdefault(stamp, []).append((labels, [(stamp, float(value))]))
    return [_standin_request(seq, stamp / 1000 - T0 - 1, series)
            for seq, (stamp, series) in enumerate(sorted(by_stamp.items()), start=1)]


def _datadog_request(seq, offset, series, status=202, stored=True, cue=None, answer="status",
                     encoding="gzip", path="/api/v2/series", series_errors=None):
    """One `datadog` stand-in record line at `T0 + offset` whose body is a `MetricPayload` of
    `series`, `[(metric, [(timestamp_s, value)])]`, each a `count` on host `logit-soak`, under
    `encoding`."""
    payload = standin.encode_metric_payload([
        {"metric": metric, "resources": [("host", "logit-soak")], "tags": [],
         "points": points, "type": "count", "interval": 10} for metric, points in series])
    if encoding == "gzip":
        body = gzip.compress(payload, mtime=0)
    elif encoding == "deflate":
        body = zlib.compress(payload)
    else:
        body = payload
    line = {"event": "request", "seq": seq, "t": T0 + offset, "path": path,
            "content_type": "application/x-protobuf", "content_encoding": encoding,
            "bytes": len(body), "cue": cue, "answer": answer,
            "status": None if answer == "reset" else status, "stored": stored,
            "body_b64": base64.b64encode(body).decode()}
    if series_errors is not None:
        line["series_errors"] = series_errors
    return line


def _datadog_record(export):
    """The requests a `datadog` stand-in would record for `export`: one stored `202` per sample
    timestamp, a second before it, holding every series' increment since its previous sample in
    the same SUT life (the first sample of a life is its own increment), named without the
    `_total` suffix."""
    by_stamp = {}
    for one in export:
        metric = one["metric"]["__name__"].removesuffix("_total")
        previous = {}
        for stamp, value in zip(one["timestamps"], one["values"]):
            life = 0 if stamp / 1000 < T0 + STOP[1] else 1
            delta = value - previous.get(life, 0)
            previous[life] = value
            by_stamp.setdefault(stamp, []).append((metric, [(stamp // 1000, float(delta))]))
    return [_datadog_request(seq, stamp / 1000 - T0 - 1, series)
            for seq, (stamp, series) in enumerate(sorted(by_stamp.items()), start=1)]


def _write_standin(run_dir, requests, extra=(), shape="remote_write"):
    """The stand-in's stdout: a `ready` line, `requests`, and `extra` raw lines."""
    lines = [json.dumps({"event": "ready", "t": T0 - 5, "shape": shape})]
    lines += [json.dumps(r) for r in requests] + list(extra)
    (run_dir / "logs" / "standin.stdout").write_text("\n".join(lines) + "\n")


def _standin_telemetry(requests, sink="sink"):
    """The SUT's `logit.output.requests` for `requests`, each in the first 5 s drain at or
    after it, by `standin.status_class`."""
    lines = []
    for request in requests:
        drain = -(-(request["t"] - T0) // 5) * 5
        lines.append(_line(_stamp(T0 + drain), "logit.output.requests", "sum", 1,
                           component=sink, attrs={"class": standin.status_class(request)}))
    return lines


def _topic_record(t, amount, name="x_0"):
    """One `kafka_out` `format: json` record value: an `aggregate` delta window's sum."""
    return json.dumps({"timestamp": _stamp(t), "metrics": [
        {"name": name, "kind": "sum", "value": amount, "temporality": "delta",
         "monotonic": True}], "attributes": {}}, separators=(",", ":"))


def _write_export(run_dir, sink, series):
    """Writes one sink's export where the driver does: `vm-export/<sink>.jsonl`."""
    (run_dir / "vm-export").mkdir(exist_ok=True)
    (run_dir / "vm-export" / f"{sink}.jsonl").write_text(
        "".join(json.dumps(one) + "\n" for one in series))


# Two sinks, each to its own backend; under `route` each fed by its own target.
TWO_SINKS = (("vm_a", "victoria-metrics", "tier_a"), ("vm_b", "victoria-metrics-b", "tier_b"))
# What the multi-sink fixture's `window` sends per drain: 10 events, under `route` 5 to each target.
AGGREGATE_SENT = 10


def _sinks_run(tmp, name, split="copy", edit=None, export_edit=None, **kwargs):
    """`_ledger_run`'s two-life run directory, rewritten for two sinks (`TWO_SINKS`): each line
    the fixture's one sink wrote is written once per sink, `window` sends `AGGREGATE_SENT` events
    per drain that each sink receives under `copy` and half of which each target sends and its
    sink receives under `route`, and the export goes to each sink whole under `copy` and split
    by series under `route` (`x_0_total` to `vm_a`, `x_1_total` to `vm_b`).

    `edit(sink, record)` may change or drop (return None) each per-sink telemetry record, and
    `export_edit(sink, series)` each sink's export."""
    run_dir = _ledger_run(tmp, name, **kwargs)
    sinks = [one for one, _, _ in TWO_SINKS]
    out = []
    for line in (run_dir / "logs" / "logit.stdout").read_text().splitlines():
        record = json.loads(line)
        attributes = record["attributes"]
        if attributes["component"] != "sink":
            out.append(line)
            if record["metrics"][0]["name"] == "logit.process.uptime":
                stamp = record["timestamp"]
                out.append(_line(stamp, "logit.component.events.sent", "sum", AGGREGATE_SENT,
                                 component="window"))
                for sink, _, target in TWO_SINKS:
                    share = AGGREGATE_SENT if split == "copy" else AGGREGATE_SENT // 2
                    received = json.loads(_line(stamp, "logit.component.events.received", "sum",
                                                share, component=sink))
                    received = edit(sink, received) if edit else received
                    if received is not None:
                        out.append(json.dumps(received))
                    if split == "route":
                        out.append(_line(stamp, "logit.component.events.sent", "sum", share,
                                         component=target))
            continue
        for sink in sinks:
            copied = json.loads(line)
            copied["attributes"]["component"] = sink
            copied = edit(sink, copied) if edit else copied
            if copied is not None:
                out.append(json.dumps(copied))
    (run_dir / "logs" / "logit.stdout").write_text("\n".join(out) + "\n")
    resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
    resolved["services"] = {**scenario.BASE_ROLES, "victoria-metrics-b": "backend"}
    selector = resolved["ledger"]["sink"][0].get("vm_selector")
    resolved["ledger"]["split"] = split
    resolved["ledger"]["sink"] = [
        {"id": sink, "backend": backend, "vm_selector": selector,
         **({"target": target} if split == "route" else {})}
        for sink, backend, target in TWO_SINKS]
    (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
    series = vm.parse_export((run_dir / "vm-export" / "sink.jsonl").read_text())
    (run_dir / "vm-export" / "sink.jsonl").unlink()
    for index, sink in enumerate(sinks):
        mine = (copy.deepcopy(series) if split == "copy" else
                [copy.deepcopy(one) for one in series
                 if one["metric"]["__name__"] == f"x_{index}_total"])
        _write_export(run_dir, sink, export_edit(sink, mine) if export_edit else mine)
    return run_dir


def _sink_ledger():
    """The ledger with two sinks: under `copy` each sink's V judged alone against Ab, under
    `route` their sum, and every per-sink row (`progress`, `identity.sink`, `recovery`,
    `ledger.edge`, `ledger.egress`, `ledger.windows`) reporting one line per sink with the worst
    status winning."""
    rows = ("ledger.edge", "ledger.egress", "ledger.summary", "identity.sink", "recovery",
            "progress")

    def short_final(target, by):
        """An `export_edit` taking `by` off `target`'s last value of each series."""
        def edit(sink, series):
            if sink == target:
                for one in series:
                    one["values"][-1] -= by
            return series
        return edit

    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            return {r.id: r for r in checks.run_all(_sinks_run(tmp, name, **kwargs))}

        for split in ("copy", "route"):
            clean = results(f"sinks-{split}", split=split)
            for row in rows:
                expect(clean[row].status == checks.PASS,
                       f"{row} PASSes the balanced two-sink {split} fixture, got {clean[row]}")
            for sink, _, _ in TWO_SINKS:
                for row in ("identity.sink", "recovery", "progress", "ledger.edge"):
                    expect(sink in clean[row].detail or sink in " ".join(clean[row].lines),
                           f"{row} reports {sink} under {split}, got {clean[row]}")
        copied = results("sinks-copy-detail")
        egress = copied["ledger.egress"].detail
        expect("vm_a: life 0" in egress and "vm_b: life 0" in egress
               and "V[vm_a]" in egress and "V[vm_b]" in egress and "split copy" in egress,
               f"egress under copy judges each sink alone, got {egress}")
        routed = results("sinks-route-detail", split="route")
        egress = routed["ledger.egress"].detail
        expect("vm_a + vm_b: life 0" in egress and "ΣV" in egress and "split route" in egress,
               f"egress under route judges the summed V, got {egress}")
        expect("T_sent[tier_a]" in routed["ledger.edge"].detail
               and "S_in[vm_b]" in routed["ledger.edge"].detail,
               f"edge under route judges each target against its sink, got "
               f"{routed['ledger.edge']}")

        # One sink short in the final life: under copy only that sink's line FAILs.
        lost = results("sinks-copy-lost", export_edit=short_final("vm_b", 20))
        egress = lost["ledger.egress"].detail
        expect(lost["ledger.egress"].status == checks.FAIL
               and "vm_b: " in egress and "V[vm_b] " in egress
               and egress.split(" | ")[0].endswith("ok")
               and "uncounted" in egress.split(" | ")[1],
               f"copy FAILs the short sink alone, got {egress}")
        expect(lost["ledger.summary"].status == checks.FAIL
               and "Ab − V[vm_b] 40" in lost["ledger.summary"].detail,
               f"the summary FAILs the short sink's term, got {lost['ledger.summary']}")
        # A surplus at one sink and a matching loss at the other can't net out in the summary.
        def opposite(sink, series):
            for one in series:
                one["values"][-1] += 20 if sink == "vm_a" else -20
            return series
        netted = results("sinks-copy-netted", export_edit=opposite)
        expect(netted["ledger.summary"].status == checks.FAIL,
               f"the summary FAILs opposite gaps at two sinks, got {netted['ledger.summary']}")
        # 100 increments moved from each of vm_b's series to each of vm_a's: under route,
        # where each sink holds one series, the sum still balances; under copy, where each
        # sink holds both, each sink is 200 off.
        def moved(sink, series):
            for one in series:
                one["values"][-1] += 100 if sink == "vm_a" else -100
            return series
        routed_moved = results("sinks-route-moved", split="route", export_edit=moved)
        expect(routed_moved["ledger.egress"].status == checks.PASS,
               f"route's summed V balances a total moved between sinks, got "
               f"{routed_moved['ledger.egress']}")
        copied_moved = results("sinks-copy-moved", export_edit=moved)
        expect(copied_moved["ledger.egress"].status == checks.FAIL
               and "V[vm_a] 34,200 = -200, uncounted" in copied_moved["ledger.egress"].detail
               and "V[vm_b] 33,800 = 200, uncounted" in copied_moved["ledger.egress"].detail,
               f"copy FAILs the same move at each sink, got {copied_moved['ledger.egress']}")

        # A shutdown drop counts a gap only at a node that can explain it: under copy, every
        # node's but the other sinks'; under route, every node's.
        elsewhere = results("sinks-copy-dropped-elsewhere", life1_dropped=1, dropped_at="vm_a",
                            export_edit=short_final("vm_b", 20))
        egress = elsewhere["ledger.egress"].detail
        expect(elsewhere["ledger.egress"].status == checks.FAIL
               and "V[vm_b] 33,960 = 40, uncounted (batches dropped: 0, not "
               "counting shutdown 1 at the other sinks)" in egress
               and elsewhere["ledger.summary"].status == checks.FAIL,
               f"under copy another sink's shutdown drop doesn't count vm_b's gap, got "
               f"{elsewhere['ledger.egress']} / {elsewhere['ledger.summary']}")
        attributed = results("sinks-copy-attributed", life1_dropped=1, dropped_at="vm_b",
                             export_edit=short_final("vm_b", 20))
        expect(attributed["ledger.egress"].status == checks.PASS
               and "V[vm_b] 33,960 = 40, counted (batches dropped: shutdown 1)"
               in attributed["ledger.egress"].detail
               and attributed["ledger.summary"].status == checks.PASS,
               f"under copy a sink's own shutdown drop counts its gap, got "
               f"{attributed['ledger.egress']} / {attributed['ledger.summary']}")
        def short_both(sink, series):
            for one in series:
                one["values"][-1] -= 20
            return series
        upstream = results("sinks-copy-upstream", life1_dropped=1, dropped_at="window",
                           export_edit=short_both)
        egress = upstream["ledger.egress"].detail
        expect(upstream["ledger.egress"].status == checks.PASS
               and "V[vm_a] 33,960 = 40, counted (batches dropped: shutdown 1)" in egress
               and "V[vm_b] 33,960 = 40, counted (batches dropped: shutdown 1)" in egress
               and upstream["ledger.summary"].status == checks.PASS,
               f"under copy a shutdown drop upstream of the fan-out counts every sink's gap, got "
               f"{upstream['ledger.egress']} / {upstream['ledger.summary']}")
        for dropper, short in (("vm_a", "vm_b"), ("vm_b", "vm_a")):
            routed_drop = results(f"sinks-route-counted-{dropper}", split="route",
                                  life1_dropped=1, dropped_at=dropper,
                                  export_edit=short_final(short, 20))
            expect(routed_drop["ledger.egress"].status == checks.PASS
                   and "= 20, counted (batches dropped: shutdown 1)"
                   in routed_drop["ledger.egress"].detail
                   and routed_drop["ledger.summary"].status == checks.PASS,
                   f"under route {dropper}'s shutdown drop counts the summed gap, got "
                   f"{routed_drop['ledger.egress']} / {routed_drop['ledger.summary']}")
        # Every reason counts, under the same group rule as a shutdown drop.
        overflow_elsewhere = results("sinks-copy-overflow-elsewhere", life1_dropped=1,
                                     dropped_at="vm_a", dropped_reason="overflow_oldest",
                                     export_edit=short_final("vm_b", 20))
        expect(overflow_elsewhere["ledger.egress"].status == checks.FAIL
               and "V[vm_b] 33,960 = 40, uncounted (batches dropped: 0, not counting "
               "overflow_oldest 1 at the other sinks)" in overflow_elsewhere["ledger.egress"].detail
               and overflow_elsewhere["ledger.summary"].status == checks.FAIL,
               f"under copy another sink's overflow drop doesn't count vm_b's gap, got "
               f"{overflow_elsewhere['ledger.egress']} / {overflow_elsewhere['ledger.summary']}")
        overflow_own = results("sinks-copy-overflow-own", life1_dropped=1, dropped_at="vm_b",
                               dropped_reason="overflow_oldest",
                               export_edit=short_final("vm_b", 20))
        expect(overflow_own["ledger.egress"].status == checks.PASS
               and "V[vm_b] 33,960 = 40, counted (batches dropped: overflow_oldest 1)"
               in overflow_own["ledger.egress"].detail
               and overflow_own["ledger.summary"].status == checks.PASS,
               f"under copy a sink's own overflow drop counts its gap, got "
               f"{overflow_own['ledger.egress']} / {overflow_own['ledger.summary']}")
        for dropper, short in (("vm_a", "vm_b"), ("vm_b", "vm_a")):
            routed_overflow = results(f"sinks-route-overflow-{dropper}", split="route",
                                      life1_dropped=1, dropped_at=dropper,
                                      dropped_reason="overflow_oldest",
                                      export_edit=short_final(short, 20))
            expect(routed_overflow["ledger.egress"].status == checks.PASS
                   and "= 20, counted (batches dropped: overflow_oldest 1)"
                   in routed_overflow["ledger.egress"].detail
                   and routed_overflow["ledger.summary"].status == checks.PASS,
                   f"under route {dropper}'s overflow drop counts the summed gap, got "
                   f"{routed_overflow['ledger.egress']} / {routed_overflow['ledger.summary']}")
        half = results("sinks-copy-half", split="copy",
                       export_edit=lambda sink, series: [one for one in series
                                                        if sink == "vm_a"
                                                        or one["metric"]["__name__"] == "x_0_total"])
        expect(half["ledger.egress"].status == checks.FAIL
               and half["ledger.egress"].detail.split(" | ")[0].endswith("ok"),
               f"copy FAILs a sink holding half the series, got {half['ledger.egress']}")
        route_lost = results("sinks-route-lost", split="route",
                             export_edit=short_final("vm_a", 20))
        expect(route_lost["ledger.egress"].status == checks.FAIL
               and "vm_a + vm_b: " in route_lost["ledger.egress"].detail
               and "uncounted" in route_lost["ledger.egress"].detail,
               f"route FAILs a summed V short at one sink, got {route_lost['ledger.egress']}")
        empty = results("sinks-copy-empty",
                        export_edit=lambda sink, series: [] if sink == "vm_b" else series)
        expect(empty["ledger.egress"].status == checks.FAIL
               and "no series in vm-export/vm_b.jsonl" in empty["ledger.egress"].detail
               and "vm-export/vm_a.jsonl" not in empty["ledger.egress"].detail,
               f"egress FAILs the sink whose export is empty, by name, got {empty['ledger.egress']}")

        # A reset on one backend only: vm_b's series fall back inside the final life, so its
        # resets aren't lives − 1, and egress WARNs naming vm_b and not vm_a.
        def reset_b(sink, series):
            if sink == "vm_b":
                for one in series:
                    one["values"] = one["values"][:3] + [1, one["values"][3]]
                    one["timestamps"] = (one["timestamps"][:3] + [one["timestamps"][3] - 1000]
                                         + one["timestamps"][3:])
            return series
        reset = results("sinks-copy-reset-b", export_edit=reset_b)
        lines = " ".join(reset["ledger.egress"].lines)
        expect(reset["ledger.egress"].status in (checks.WARN, checks.FAIL)
               and "vm_b: 2 series with resets other than 1" in lines
               and "vm_a: 2 series with resets" not in lines,
               f"egress names the one backend whose series reset, got {reset['ledger.egress']}")

        # Per-sink rows: the worst sink's status wins, and the line names it.
        def undelivered_b(sink, record):
            metric = record["metrics"][0]
            if (sink == "vm_b" and metric["name"] == "logit.component.batches.delivered"
                    and record["timestamp"] in (_stamp(T0 + 200), _stamp(T0 + 205))):
                metric["value"] = 0
            return record
        stuck = results("sinks-identity-b", edit=undelivered_b)
        expect(stuck["identity.sink"].status == checks.FAIL
               and "vm_b: received" in stuck["identity.sink"].detail
               and "gap 2" in stuck["identity.sink"].detail
               and "gap 0" in stuck["identity.sink"].detail,
               f"identity.sink FAILs the sink that lost batches, by name, got "
               f"{stuck['identity.sink']}")

        def busy_b(sink, record):
            metric = record["metrics"][0]
            if sink == "vm_b" and metric["name"] == "logit.component.buffer.utilization":
                metric["value"] = 0.5
            if sink == "vm_b" and metric["name"] == "logit.component.buffer.batches":
                metric["value"] = 2
            return record
        busy = results("sinks-recovery-b", edit=busy_b)
        lines = busy["recovery"].lines
        expect(busy["recovery"].status == checks.FAIL
               and any("vm_b not recovered" in line for line in lines)
               and any("vm_a recovered" in line for line in lines),
               f"recovery judges each sink, got {busy['recovery']}")

        def silent_b(sink, record):
            metric = record["metrics"][0]
            if (sink == "vm_b" and metric["name"] == "logit.component.batches.delivered"
                    and record["timestamp"] >= _stamp(T0 + 240)):
                return None
            return record
        silent = results("sinks-progress-b", edit=silent_b)
        expect(silent["progress"].status == checks.FAIL
               and "no vm_b deliveries" in silent["progress"].detail,
               f"progress FAILs the sink with no deliveries, by name, got {silent['progress']}")

        def short_receipt(sink, record):
            metric = record["metrics"][0]
            if (sink == "vm_b" and metric["name"] == "logit.component.events.received"
                    and record["timestamp"] == _stamp(T0 + 250)):
                metric["value"] -= 3
            return record
        for split, needle in (("copy", "A_sent 380 != S_in[vm_b] 377"),
                              ("route", "T_sent[tier_b] 190 != S_in[vm_b] 187")):
            edge = results(f"sinks-edge-{split}", split=split, edit=short_receipt)["ledger.edge"]
            expect(edge.status == checks.FAIL and needle in edge.detail,
                   f"edge FAILs a sink receiving less than was sent to it under {split}, got "
                   f"{edge}")


def _declared_roles(text):
    """`{service: role}` from a compose file's text: each service under `services:` at two
    spaces, and the `soak.role` label in its block. For the shipped files only, which label each
    service inline; the driver reads `docker compose config` instead."""
    roles = {}
    current = None
    inside = False
    for line in text.splitlines():
        if line.startswith("services:"):
            inside = True
            continue
        if not inside:
            continue
        if line and not line.startswith(" "):
            break
        header = re.match(r"^  ([A-Za-z0-9_.-]+):\s*$", line)
        if header:
            current = header.group(1)
            roles[current] = None
            continue
        label = re.search(r"soak\.role:\s*([a-z]+)", line)
        if label and current is not None:
            roles[current] = label.group(1)
    return roles


def _override_roles(loaded):
    """The role map the driver would resolve for a shipped scenario: `base_roles()` with each
    service its override labels."""
    roles = scenario.base_roles(loaded)
    if loaded.compose is not None:
        # An override that only adds to a base service, as `depends_on` on `logit`, labels it
        # through the base file; compose merges the two.
        roles.update({name: role for name, role in
                      _declared_roles(loaded.compose_path().read_text()).items()
                      if role is not None or name not in roles})
    return roles


def _roles(root):
    """Service roles: compose.yaml's labels are `BASE_ROLES`; the role map's own rules; reading
    `docker compose config`'s JSON; the rules that need the service list, against the base map
    and an override's; and the fault predicates, which judge a service by its role."""
    base = (root / "compose.yaml").read_text()
    declared = {**scenario.BASE_ROLES, **scenario.KAFKA_ROLES, **scenario.STANDIN_ROLES}
    expect(_declared_roles(base) == declared,
           f"compose.yaml labels each service as BASE_ROLES, KAFKA_ROLES, and STANDIN_ROLES "
           f"say, got "
           f"{_declared_roles(base)}")
    config = json.dumps({"name": "p", "services": {
        "logit": {"labels": {"soak.role": "sut"}, "image": "x"},
        "generator": {"labels": {"soak.role": "generator"}},
        "victoria-metrics": {"labels": {"soak.role": "backend"}},
        "victoria-metrics-b": {"labels": {"soak.role": "backend", "other": "1"}},
        "nginx": {"image": "nginx"}}})
    roles = scenario.roles_from_config(config)
    expect(roles == {"logit": "sut", "generator": "generator", "victoria-metrics": "backend",
                     "victoria-metrics-b": "backend", "nginx": None},
           f"roles_from_config reads each service's soak.role label, got {roles}")
    for junk in ("", "{}", "not json"):
        try:
            scenario.roles_from_config(junk)
            expect(False, f"roles_from_config accepted {junk!r}")
        except ValueError:
            expect(True, "")
    for bad, needle, what in (
            ({**scenario.BASE_ROLES, "nginx": None}, "compose service nginx: label soak.role",
             "an unlabeled service"),
            ({**scenario.BASE_ROLES, "x": "sink"}, "must be one of sut, generator, backend",
             "an unknown role"),
            ({**scenario.BASE_ROLES, "logit-2": "sut"}, "one sut service, logit, and has logit, "
             "logit-2", "two SUTs"),
            ({"sut": "sut", "generator": "generator"}, "one sut service, logit, and has sut",
             "a SUT under another name"),
            ({"logit": "sut"}, "at least one generator service, and has none",
             "no generator")):
        problems = scenario.role_problems(bad)
        expect(any(needle in problem for problem in problems),
               f"{what}: expected {needle!r}, got {problems}")
    expect(scenario.role_problems(scenario.BASE_ROLES) == [], "BASE_ROLES breaks no role rule")

    path, raw = _base(root)
    copy_path = root / "scenarios" / "fanout-copy" / "scenario.toml"
    two = scenario.load(copy_path)
    with_b = _override_roles(two)
    expect(with_b == {**scenario.BASE_ROLES, "victoria-metrics-b": "backend"},
           f"fanout-copy's override adds a second backend, got {with_b}")
    expect(scenario.service_problems(two, with_b) == [],
           f"fanout-copy's steps and sinks hold with its override's roles, got "
           f"{scenario.service_problems(two, with_b)}")
    problems = scenario.service_problems(two, scenario.BASE_ROLES)
    expect(any("unknown service `victoria-metrics-b`" in p for p in problems)
           and any("backend `victoria-metrics-b` isn't a backend service" in p for p in problems),
           f"without the override, a step and a sink on victoria-metrics-b are refused, got "
           f"{problems}")

    def mutate(edit):
        changed = copy.deepcopy(raw)
        edit(changed)
        return changed

    _refused(path, mutate(lambda r: r["ledger"]["sink"][0].update(backend="logit")),
             "backend `logit` isn't a backend service", "a sink whose backend is the SUT")
    _refused(path, mutate(lambda r: r["ledger"]["sink"][0].update(backend="vm")),
             "backend `vm` isn't a backend service; the stack's are victoria-metrics",
             "a sink whose backend isn't in the stack")
    _refused(path, mutate(lambda r: r.update(expect=[{
        "name": "x", "service": "victoria-metrics", "metric": "m", "step": 1,
        "window": "during", "reduce": "delta", "min": 1}])),
             "unknown service `victoria-metrics`; telemetry comes from generator or logit",
             "an expectation on a backend")
    _refused(path, mutate(lambda r: r["step"].append(
        {"at": "11m10s", "action": "kill", "on": "victoria-metrics-b", "for": "20s"})),
             "kill is for logit only", "a kill on a second backend",
             roles={**scenario.BASE_ROLES, "victoria-metrics-b": "backend"})
    _refused(path, raw, "one generator service", "a scenario run on a stack with no generator",
             roles={"logit": "sut", "victoria-metrics": "backend"})

    step = scenario.Step("c0s1", 0, 0, "stop", "victoria-metrics-b", "", 0, 1)
    roles_b = {**scenario.BASE_ROLES, "victoria-metrics-b": "backend"}
    for action in ("netem", "pause", "stop", "partition"):
        step.action = action
        expect(faults.ACTIONS[action].affects_egress(step, roles_b)
               and not faults.ACTIONS[action].affects_udp_ingress(step, roles_b),
               f"a {action} on a second backend affects egress only")
    step.on, step.action = "generator", "netem"
    expect(faults.ACTIONS["netem"].affects_udp_ingress(step, roles_b)
           and not faults.ACTIONS["netem"].affects_egress(step, roles_b),
           "a netem on the generator affects UDP ingress only")
    step.on, step.action = "logit", "stop"
    expect(faults.ACTIONS["stop"].affects_udp_ingress(step, roles_b)
           and faults.ACTIONS["stop"].affects_egress(step, roles_b),
           "a stop on the SUT affects both")


def _sink_rules(root):
    """`[[ledger.sink]]`, `[ledger] split`, and `compose`: every rule `load()` holds them to."""
    path, raw = _base(root)

    def mutate(edit):
        changed = copy.deepcopy(raw)
        edit(changed)
        return changed

    def sink(**changes):
        return lambda r: r["ledger"]["sink"][0].update(changes)

    def route(*sinks):
        def edit(r):
            r["ledger"]["split"] = "route"
            r["ledger"]["sink"] = [dict(r["ledger"]["sink"][0], **one) for one in sinks]
        return edit

    loaded = scenario.load(path)
    expect(loaded.sinks == [{"id": "victoria_metrics", "backend": "victoria-metrics",
                             "vm_selector": '{__name__=~"soak_requests_[0-9]+_total"}'}]
           and loaded.split == "copy" and loaded.to_json()["ledger"]["split"] == "copy",
           f"statsd-vm has one sink and the default split, got {loaded.sinks} {loaded.split}")
    cases = [
        (lambda r: r["ledger"].pop("sink"), "ledger: missing `sink`", "no sink"),
        (lambda r: r["ledger"].update(sink=[]), "ledger.sink must be one or more tables",
         "an empty sink array"),
        (lambda r: r["ledger"].update(sink="vm"), "ledger.sink must be one or more tables",
         "a sink that isn't a table"),
        (sink(sinks="x"), "ledger.sink 1: unknown key `sinks`", "an unknown sink key"),
        (lambda r: r["ledger"]["sink"][0].pop("id"), "ledger.sink 1: `id` must name",
         "a sink with no id"),
        (sink(id="vm a"), "ledger.sink 1: `id` must name", "a sink id with a space"),
        (lambda r: r["ledger"]["sink"].append(dict(r["ledger"]["sink"][0])),
         "ledger.sink 2: `id` 'victoria_metrics' repeats", "a repeated sink id"),
        (lambda r: r["ledger"]["sink"][0].pop("backend"), "ledger.sink 1: missing `backend`",
         "a local sink with no backend"),
        (lambda r: r["ledger"]["sink"][0].pop("vm_selector"),
         "ledger.sink 1: missing `vm_selector`", "a local sink with no selector"),
        (lambda r: r["ledger"].update(split="mirror"), "ledger.split must be one of copy, route",
         "an unknown split"),
        (sink(target="t"), "ledger.sink 1: `target` is for split = \"route\"",
         "a target under copy"),
        (route({"target": "t"}), "needs two or more [[ledger.sink]] tables",
         "a route with one sink"),
        (route({"target": "t"}, {"id": "b"}), "ledger.sink 2: under split = \"route\" a sink "
         "needs `target`", "a routed sink with no target"),
        (route({"target": "t"}, {"id": "b", "target": "t"}), "ledger.sink 2: `target` 't' "
         "repeats", "two sinks fed by one target"),
        (lambda r: r.update(compose=3), "`compose` must name an override file",
         "a compose that isn't a string"),
        (lambda r: r.update(compose="../compose.yaml"), "`compose` must name an override file",
         "a compose outside the scenario's directory"),
        (lambda r: r.update(compose="missing.yaml"), "compose: missing.yaml not found",
         "a compose file that doesn't exist"),
    ]
    for edit, needle, what in cases:
        _refused(path, mutate(edit), needle, what)
    try:
        routed = scenario.from_dict(mutate(route({"target": "t"}, {"id": "b", "target": "u"})),
                                    path)
        expect(routed.split == "route" and [s["target"] for s in routed.sinks] == ["t", "u"],
               f"two routed sinks with distinct targets load, got {routed.sinks}")
    except scenario.ScenarioError as err:
        expect(False, f"two routed sinks are refused: {err.problems}")


class _ConfigDocker:
    """A `Docker` whose only call is `config_json`, answering `result`; any other call raises,
    so a test using it can't start anything."""

    def __init__(self, result):
        self.result = result
        self.calls = 0

    def config_json(self):
        self.calls += 1
        return self.result

    def __getattr__(self, name):
        raise AssertionError(f"_ConfigDocker.{name} called")


def _driver_roles(root):
    """`prepare()` with a compose override and a role map: the override is passed after the
    base file and copied into the run, and the resolved scenario records it and the roles; a
    run built without `roles=` asks compose, here a daemon that isn't there, and stops before
    writing the resolved scenario; a role map missing the override's service fails the
    scenario's checks there too. The driver's per-backend ports and the freshness poll per
    sink follow."""
    two = scenario.load(root / "scenarios" / "fanout-copy" / "scenario.toml")
    roles = _override_roles(two)
    with tempfile.TemporaryDirectory() as tmp:
        argv = docker.Docker("p", ["c.yaml", "o.yaml"], ["a.env"], ["local"], prefix=["docker"])
        expect(argv.compose_args() == ["compose", "--progress", "quiet", "-p", "p", "-f",
                                       "c.yaml", "-f", "o.yaml", "--env-file", "a.env",
                                       "--profile", "local"],
               f"compose gets the base file, then the override, got {argv.compose_args()}")
        run = driver.Run(root, two, two.duration, None, False, Path(tmp) / "given", [],
                         "logit:soak", roles=roles)
        run.prepare()
        resolved = json.loads((run.run_dir / "scenario.resolved.json").read_text())
        expect(run.docker.compose_files == [str(root / "compose.yaml"), str(two.compose_path())]
               and (run.run_dir / "configs" / "compose.override.yaml").is_file()
               and resolved["compose"] == "compose.override.yaml"
               and resolved["services"] == roles
               and [s["backend"] for s in resolved["ledger"]["sink"]]
               == ["victoria-metrics", "victoria-metrics-b"],
               f"prepare passes and copies the override and records it and the roles, got "
               f"{run.docker.compose_files} {resolved.get('compose')} {resolved.get('services')}")
        for jsonl in (run.timeline, run.watchdog, run.stats, run.freshness, run.capture):
            jsonl.close()

        asked = driver.Run(root, two, two.duration, None, False, Path(tmp) / "asked", [],
                           "logit:soak")
        try:
            asked.prepare()
            expect(False, "prepare() with no daemon resolved a role map")
        except scenario.ScenarioError as err:
            expect("docker compose config failed (rc 127)" in str(err)
                   and not (asked.run_dir / "scenario.resolved.json").exists(),
                   f"prepare() asks compose for the roles and stops when it can't, got {err}")
        narrow = driver.Run(root, two, two.duration, None, False, Path(tmp) / "narrow", [],
                            "logit:soak", roles=scenario.BASE_ROLES)
        try:
            narrow.prepare()
            expect(False, "prepare() ran fanout-copy on a stack without victoria-metrics-b")
        except scenario.ScenarioError as err:
            expect("unknown service `victoria-metrics-b`" in str(err)
                   and not (narrow.run_dir / "scenario.resolved.json").exists(),
                   f"prepare() checks the scenario's services before anything starts, got {err}")

        resolver = driver.Run(root, two, two.duration, None, False, "/nonexistent", [],
                              "logit:soak")
        resolver.docker = _ConfigDocker(docker.Result(0, config_text(roles), ""))
        expect(resolver.resolve_roles() == roles and resolver.docker.calls == 1,
               "resolve_roles reads the role map from compose config's JSON")
        resolver.docker = _ConfigDocker(docker.Result(0, "Warning: x\n", ""))
        try:
            resolver.resolve_roles()
            expect(False, "resolve_roles accepted output that isn't JSON")
        except scenario.ScenarioError as err:
            expect("docker compose config" in str(err), f"got {err}")

    # Ports per backend: a revert on victoria-metrics-b re-reads its port only.
    class Ports:
        def __init__(self):
            self.asked = []

        def port(self, service, private):
            self.asked.append((service, private))
            return f"127.0.0.1:{9000 + len(self.asked)}"

        def inspect(self, container, timeout=None):
            return {"State": {"StartedAt": "x"}}

    run = driver.Run(root, two, two.duration, None, False, "/nonexistent", [], "logit:soak",
                     roles=roles)
    run.docker = Ports()
    run.ids = {name: name for name in roles}
    step = scenario.Step("c0s1", 0, 0, "stop", "victoria-metrics-b", "", 0, 1)
    run.after("apply", step, {"rc": 0})
    run.after("revert", step, {"rc": 0})
    expect(run.docker.asked == [("victoria-metrics-b", 8428)]
           and run.vm_base == {"victoria-metrics-b": "http://127.0.0.1:9001"},
           f"a revert on a backend re-reads that backend's port only, got {run.docker.asked}")

    # Freshness: one record per sink, a held backend's skipped.
    run.down = {"victoria-metrics-b"}
    run.vm_base = {"victoria-metrics": "http://a", "victoria-metrics-b": "http://b"}
    run.freshness = _Records()
    run.t0 = 0.0
    real = vm.freshness
    vm.freshness = lambda base, selector, now, lookback: {"series": 1, "newest_ms": 1,
                                                         "age_s": 1.0, "base": base}
    try:
        run.sample_freshness()
    finally:
        vm.freshness = real
    by_sink = {record["sink"]: record for record in run.freshness}
    expect(by_sink["vm_a"].get("base") == "http://a"
           and by_sink["vm_b"].get("skipped") == "victoria-metrics-b held by a step",
           f"freshness is sampled per sink, skipping a held backend, got {by_sink}")

    # The end sequence's wait holds only once every sink's total holds.
    totals = {"http://a": [10, 20, 20, 20, 20, 20, 20, 20],
              "http://b": [5, 5, 5, 9, 9, 9, 9, 9]}
    seen = {"http://a": 0, "http://b": 0}

    def export(base, selector, start, end=None):
        index = min(seen[base], len(totals[base]) - 1)
        seen[base] += 1
        return [{"metric": {"__name__": "x"}, "values": [totals[base][index]],
                 "timestamps": [0]},
                {"metric": {"__name__": "x"}, "values": [1, 2], "timestamps": [0, 2000]}]

    clock = [1000.0]
    reals = (vm.export, vm.force_flush, driver._now, driver.time.sleep)
    vm.export, vm.force_flush = export, lambda base: None
    driver._now = lambda: clock[0]
    driver.time.sleep = lambda s: clock.__setitem__(0, clock[0] + s)
    run.docker = Ports()
    run.vm_base = {}
    run.timeline = _Records()
    real_refresh = run.refresh_vm_base
    run.refresh_vm_base = lambda backend: run.vm_base.__setitem__(
        backend, "http://a" if backend == "victoria-metrics" else "http://b")
    try:
        run.wait_quiet()
    finally:
        vm.export, vm.force_flush, driver._now, driver.time.sleep = reals
        run.refresh_vm_base = real_refresh
    quiet = run.timeline[-1]
    expect(quiet["held"] and quiet["totals"] == {"vm_a": 22, "vm_b": 11}
           and seen["http://b"] >= 5,
           f"wait_quiet holds once both sinks' totals hold, got {quiet}")


def config_text(roles):
    """`docker compose config --format json`-shaped output for `roles`."""
    return json.dumps({"services": {name: {"labels": {"soak.role": role}}
                                    for name, role in roles.items()}})


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
        expect(clean["drops"].status == checks.PASS,
               f"drops PASSes when the ledger balances every *.dropped term (a listener's "
               f"shutdown datagrams), got {clean['drops']}")
        stamp = _stamp(T0 + 200)
        unjudged = results("ledger-drops-unjudged", sut_extra=(
            _line(stamp, "logit.component.events.dropped", "sum", 3, component="window",
                  attrs={"reason": "closed_consumer"}),
            _line(stamp, "logit.output.metrics.dropped", "sum", 4, component="sink",
                  attrs={"reason": "unresolved_gauge_delta"})))
        expect(unjudged["drops"].status == checks.WARN
               and "window: logit.component.events.dropped{reason=\"closed_consumer\"} 3"
               in unjudged["drops"].detail
               and "sink: logit.output.metrics.dropped{reason=\"unresolved_gauge_delta\"} 4"
               in unjudged["drops"].detail,
               f"drops WARNs every *.dropped no other row judges, by component and reason, "
               f"got {unjudged['drops']}")
        refused = results("ledger-drops-rejected", sut_extra=(
            _line(stamp, "logit.output.records.dropped", "sum", 2, component="sink",
                  attrs={"reason": "rejected"}),))
        expect(refused["drops"].status == checks.FAIL,
               f"drops FAILs a rejected drop, got {refused['drops']}")
        balanced = results("ledger-drops-balanced", life1_dropped=2, sut_extra=(
            _line(stamp, "logit.input.kernel.dropped", "sum", 5, component="in",
                  attrs={"reason": "socket"}),
            _line(stamp, "logit.input.messages.dropped", "sum", 0, component="in",
                  attrs={"reason": "malformed"}),
            _line(_stamp(T0 + 320), "logit.component.events.dropped", "sum", 200,
                  component="sink", attrs={"reason": "shutdown"})))
        expect(balanced["drops"].status == checks.PASS,
               f"drops leaves out the terms the ledger and identity.sink balance (K, a sink's "
               f"batch drop and its events), got {balanced['drops']}")
        led = checks.Ledger(checks.RunData(Path(tmp) / "ledger-clean"))
        expect(led.D_log == {0: 20} and led.D_shutdown == {0: 20} and led.D(0) == 20
               and "D 20 (20 at shutdown; stderr says 20)" in " ".join(clean["ledger.intake"].lines),
               f"a shutdown drop telemetry and stderr both carry counts once in D, got "
               f"D {led.D(0)}, D_log {led.D_log}, D_shutdown {led.D_shutdown}")
        stderr_only = results("ledger-shutdown-stderr-only", shutdown_in=("stderr",))
        expect(stderr_only["ledger.intake"].status == checks.WARN
               and "stderr shutdown lines count 20 datagram(s), telemetry's "
               "datagrams.dropped{reason=\"shutdown\"} 0" in stderr_only["ledger.intake"].detail,
               f"intake WARNs shutdown drops on stderr that telemetry lacks, got "
               f"{stderr_only['ledger.intake']}")
        tel_only = results("ledger-shutdown-telemetry-only", shutdown_in=("telemetry",))
        expect(tel_only["ledger.intake"].status == checks.PASS
               and tel_only["ledger.egress"].status == checks.PASS,
               f"intake doesn't WARN on shutdown drops in telemetry that stderr lacks (a "
               f"cancelled blocked push counts without logging), and D still reads telemetry, "
               f"got {tel_only['ledger.intake']} {tel_only['ledger.egress']}")
        expect(led.residual(0) == 30, f"life 0's W − D − B − X − Ab, got {led.residual(0)}")
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
               and "counted (batches dropped: shutdown 3)" in counted["ledger.egress"].detail,
               f"egress marks a positive gap with a shutdown batch drop as counted, got "
               f"{counted['ledger.egress']}")
        for reason in ("overflow_oldest", "disk_full"):
            other = results(f"ledger-counted-{reason}", v0_extra=-500, life0_dropped=3,
                            dropped_reason=reason)
            expect(other["ledger.egress"].status == checks.PASS
                   and f"counted (batches dropped: {reason} 3)" in other["ledger.egress"].detail
                   and other["ledger.summary"].status == checks.PASS,
                   f"egress counts a gap by a {reason} batch drop, got {other['ledger.egress']} "
                   f"/ {other['ledger.summary']}")
        # Shutdown and overflow drops in one life, stderr's `drain complete` counting the
        # shutdown ones: the cross-check compares shutdown drops alone and doesn't WARN.
        overflow = [_line(_stamp(T0 + 50), "logit.component.batches.dropped", "sum", 12,
                          component="sink", attrs={"reason": "overflow_oldest"})]
        mixed = results("ledger-counted-mixed", v0_extra=-500, life0_dropped=2,
                        sut_extra=overflow)
        expect(mixed["ledger.egress"].status == checks.PASS
               and "counted (batches dropped: overflow_oldest 12, shutdown 2)"
               in mixed["ledger.egress"].detail
               and "drain complete's batches_dropped" not in mixed["ledger.egress"].detail,
               f"egress names every crediting reason, and its stderr cross-check reads shutdown "
               f"drops alone, got {mixed['ledger.egress']}")
        overflow_only = results("ledger-overflow-stderr-zero", v0_extra=-500, sut_extra=overflow)
        expect(overflow_only["ledger.egress"].status == checks.PASS
               and "drain complete's batches_dropped" not in overflow_only["ledger.egress"].detail,
               f"an overflow drop with stderr's drain complete at 0 doesn't WARN, got "
               f"{overflow_only['ledger.egress']}")
        # A rejected batch counts the gap and still FAILs both rows, with or without a gap.
        for name, v1_short in (("ledger-rejected", 40), ("ledger-rejected-no-gap", 0)):
            rejected = results(name, v1_short=v1_short, life1_dropped=1,
                               dropped_reason="rejected")
            for row in ("ledger.egress", "ledger.summary"):
                expect(rejected[row].status == checks.FAIL
                       and "FAIL: 1 batch(es) rejected by the destination" in rejected[row].detail,
                       f"{row} FAILs a rejected batch drop and names it ({name}), got "
                       f"{rejected[row]}")
            expect(v1_short == 0 or "40, counted (batches dropped: rejected 1)"
                   in rejected["ledger.egress"].detail,
                   f"a rejected batch still counts the gap, got {rejected['ledger.egress']}")
        corrupt = results("ledger-disk-corrupt", v1_short=40, life1_dropped=1,
                          dropped_reason="disk_corrupt")
        expect(corrupt["ledger.egress"].status == checks.FAIL
               and "1 batch(es) dropped from a corrupt disk spool" in corrupt["ledger.egress"].detail
               and corrupt["ledger.summary"].status == checks.FAIL,
               f"a corrupt-spool drop FAILs egress and the summary, got "
               f"{corrupt['ledger.egress']} / {corrupt['ledger.summary']}")
        log_counted = results("ledger-counted-stderr-only", v0_extra=-500, life0_dropped=3,
                              shutdown_in=("stderr",))
        expect(log_counted["ledger.egress"].status == checks.FAIL
               and "drain complete's batches_dropped 3, telemetry's batches.dropped"
               in log_counted["ledger.egress"].detail,
               f"egress counts a gap from telemetry only, and reports a drain complete count "
               f"telemetry lacks, got {log_counted['ledger.egress']}")
        expect(counted["ledger.summary"].status == checks.PASS,
               f"a counted earlier life leaves the summary PASS, got {counted['ledger.summary']}")

        final_lost = results("ledger-final-uncounted", v1_short=40)
        expect(final_lost["ledger.egress"].status == checks.FAIL,
               f"egress FAILs a final-life gap, got {final_lost['ledger.egress']}")
        expect(final_lost["ledger.summary"].status == checks.FAIL,
               f"the summary FAILs an uncounted Ab − V, got {final_lost['ledger.summary']}")
        final_counted = results("ledger-final-counted", v1_short=40, life1_dropped=1)
        expect(final_counted["ledger.summary"].status == checks.PASS
               and "Ab − V[sink] 40 counted" in final_counted["ledger.summary"].detail,
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


def _script_ledger():
    """The ledger with a `lua` component between the listener and `aggregate`: `ledger.script`'s
    three identities, `ledger.edge` moving to the script's edge, the egress gap leaving out the
    flush events V excludes, a stopped life's gap judged with no allowance for the script, the
    residual band one inbox and one batch wider, and an earlier life ended by an expected exit."""
    per_drain = {"drop": 20, "err": 10, "flush": 1}
    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            run_dir = _ledger_run(tmp, name, script=dict(per_drain), **kwargs)
            return {r.id: r for r in checks.run_all(run_dir)}

        clean = results("script-clean")
        for check_id in ("ledger.intake", "ledger.edge", "ledger.script", "ledger.aggregate",
                         "ledger.egress", "ledger.summary"):
            expect(clean[check_id].status == checks.PASS,
                   f"{check_id} PASSes the balanced script fixture, got {clean[check_id]}")
        expect(clean["drops"].status == checks.PASS,
               f"drops leaves the script's events.dropped{{reason=\"script_error\"}} to "
               f"ledger.script, got {clean['drops']}")
        expect("script to aggregate" in clean["ledger.edge"].detail,
               f"edge judges the script's edge, got {clean['ledger.edge']}")
        expect("Ab − L_flush" in clean["ledger.egress"].detail
               and "Ab − L_flush" in clean["ledger.summary"].detail
               and "counted" in clean["ledger.summary"].detail,
               f"egress and the summary name the flush term and the counted script drops, got "
               f"{clean['ledger.egress']} / {clean['ledger.summary']}")
        led = checks.Ledger(checks.RunData(Path(tmp) / "script-clean"))
        expect(led.residual(0) == 30, f"life 0's residual less the script's drops and the "
                                      f"flush events, got {led.residual(0)}")
        expect(led.R == 142000, f"R grows by one inbox and one batch with a script, got {led.R}")

        lost_edge = results("script-l-in", l_in_short=4)
        expect(lost_edge["ledger.script"].status == checks.FAIL
               and "E == L_in off by 4" in lost_edge["ledger.script"].detail,
               f"ledger.script FAILs events lost between the listener and the script, got "
               f"{lost_edge['ledger.script']}")
        expect(lost_edge["ledger.summary"].status == checks.FAIL,
               f"the summary FAILs a nonzero E − L_in, got {lost_edge['ledger.summary']}")
        unaccounted = results("script-l-err", l_err_unreported=3)
        expect(unaccounted["ledger.script"].status == checks.FAIL
               and "L_in == L_emit + L_drop + L_err off by 3" in
               unaccounted["ledger.script"].detail,
               f"ledger.script FAILs events neither emitted, dropped, nor failed on, got "
               f"{unaccounted['ledger.script']}")
        unsent = results("script-l-sent", l_sent_short=6)
        expect(unsent["ledger.script"].status == checks.FAIL
               and "L_sent == L_emit + L_flush off by -6" in unsent["ledger.script"].detail,
               f"ledger.script FAILs a send short of what it emitted, got "
               f"{unsent['ledger.script']}")
        expect(unsent["ledger.edge"].status == checks.PASS,
               f"edge judges L_sent == A, which a short send keeps, got {unsent['ledger.edge']}")
        edge = results("script-edge", a1_short=7)
        expect(edge["ledger.edge"].status == checks.FAIL
               and "L_sent" in edge["ledger.edge"].detail,
               f"edge FAILs L_sent != A, got {edge['ledger.edge']}")

        lost = results("script-final-uncounted", v1_short=40)
        expect(lost["ledger.egress"].status == checks.FAIL
               and lost["ledger.summary"].status == checks.FAIL,
               f"egress and the summary FAIL a final-life gap with a script, got "
               f"{lost['ledger.egress']} / {lost['ledger.summary']}")

        # Life 0's script drops and fails on events, and the final drain exports both counts, so
        # a stopped life's gap of 1 with no shutdown drop is loss like any other.
        gap = results("script-stopped-gap", v0_extra=29)
        expect(gap["ledger.egress"].status == checks.FAIL
               and "life 0: W − D − B − X − (L_drop + L_err) − V 1" in gap["ledger.egress"].detail
               and "uncounted" in gap["ledger.egress"].detail,
               f"egress FAILs a stopped scripted life's 1-increment gap with no shutdown drop, "
               f"got {gap['ledger.egress']}")
        # A residual between the plain R (77,000) and the script's (142,000).
        wide = results("script-wide-residual", life0_extra=100030, v0_extra=100010)
        expect(wide["ledger.intake"].status == checks.PASS
               and wide["ledger.egress"].status == checks.PASS,
               f"an earlier life's residual within the script's R passes, got "
               f"{wide['ledger.intake']} / {wide['ledger.egress']}")

        # Life 0 ends with an expected exit: two lives, its gap counted by the script node's own
        # shutdown batch drops.
        exited = results("script-exit", sut_end="exit", v0_extra=-500, life0_dropped=3,
                         dropped_at="lua")
        expect(not exited["ledger.egress"].detail.startswith("SUT telemetry shows")
               and exited["ledger.egress"].status == checks.PASS
               and "counted (batches dropped: shutdown 3)" in exited["ledger.egress"].detail,
               f"an exit-ended life is a stopped earlier life with a counted gap, got "
               f"{exited['ledger.egress']}")
        expect(exited["ledger.script"].status == checks.PASS
               and exited["ledger.summary"].status == checks.PASS,
               f"the final life after an exit balances, got {exited['ledger.script']} / "
               f"{exited['ledger.summary']}")
        # The script node failed, and the listener's next batch went to its closed inbox,
        # counted as `closed_consumer`: that life's gap is 0, not 100 uncounted.
        closed = [_line(_stamp(T0 + 100), "logit.component.events.dropped", "sum", 100,
                        component="in", attrs={"reason": "closed_consumer"})]
        closed_run = results("script-exit-closed-consumer", sut_end="exit", sut_extra=closed,
                             v0_extra=-70)
        expect(closed_run["ledger.egress"].status == checks.PASS
               and "W − D − B − X − (L_drop + L_err) − V 0" in closed_run["ledger.egress"].detail,
               f"egress counts the listener's dropped events, got {closed_run['ledger.egress']}")
        unexited = _ledger_run(tmp, "script-exit-unstarted", script=dict(per_drain),
                               sut_end="exit")
        timeline = collect.read_jsonl(unexited / "timeline.jsonl")
        for record in timeline:
            if record.get("event") == "revert":
                record["started"] = False
        (unexited / "timeline.jsonl").write_text("".join(json.dumps(r) + "\n" for r in timeline))
        result = checks.check_ledger_egress(checks.RunData(unexited))
        expect(result.status == checks.SKIP and "timeline starts the SUT into 1" in result.detail,
               f"an exit revert that started nothing starts no life, got {result}")

        none = _ledger_run(tmp, "script-none")
        result = checks.check_ledger_script(checks.RunData(none))
        expect(result.status == checks.SKIP, f"ledger.script SKIPs with no sut_script, got "
                                             f"{result}")


def _expect_run(tmp, name, expectations, apply_rc=0, second_cycle=False):
    """A run directory for `check_expect`: c0s1 held over [+100, +190] (and c1s1 over
    [+300, +390] with `second_cycle`), and SUT drains every 5 s from +5 to +450.

    - `logit.input.datagrams{component=in}`: 1,000 per drain, none at +150 and +155.
    - `logit.component.datagrams.dropped{component=in}`: 10 per drain from +120 to +180 under
      `reason=overflow_oldest`, and 7 at +140 under `reason=shutdown`.
    - `logit.component.buffer.batches{component=sink}`: set to 5 at +50, 2 at +120, 0 at +200,
      and 3 at +330. Nothing else sets it, as a gauge only drains after it's set.
    - `logit.input.kernel.dropped{component=in}`: 5 at +200, after the revert.
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
            stdout.append(_line(stamp, "logit.input.kernel.dropped", "sum", 5, component="in"))
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
            metric="logit.input.kernel.dropped", max=0)
        # through is (+100, +235]: 27 drains, two missing.
        one("through spans the fault and its recovery_bound", checks.PASS, window="through",
            min=25000, max=25000)
        one("through sees a counter that moves after the revert", checks.FAIL,
            window="through", metric="logit.input.kernel.dropped", max=0)
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


def _service_stdout(end_s, rss, fds, strings, restart_at=None):
    """A `logit` service's NDJSON every 5 s from +0 to `end_s`: uptime, and `rss(offset)`,
    `fds(offset)`, and `strings(offset)` as its resident bytes, fds, and interner strings. A
    `restart_at` offset starts a second life there, its uptime from 1 again."""
    lines = []
    for offset in range(0, int(end_s) + 1, 5):
        stamp = _stamp(T0 + offset)
        uptime = offset + 1.0 if restart_at is None or offset < restart_at else (
            offset - restart_at + 1.0)
        lines.append(_line(stamp, "logit.process.uptime", "gauge", uptime))
        lines.append(_line(stamp, "logit.component.batches.delivered", "sum", 1,
                           component="sink"))
        lines.append(_line(stamp, "logit.process.memory.resident.bytes", "gauge", rss(offset)))
        lines.append(_line(stamp, "logit.process.fds", "gauge", fds(offset)))
        lines.append(_line(stamp, "logit.process.interner.strings", "gauge", strings(offset)))
    return lines


def _slope_run(tmp, name, sut=None, gen=None, end_s=3600, **resolved):
    """A run directory of `end_s` seconds after a 60 s warmup with no faults, `logit` and
    `generator` NDJSON from `_service_stdout(**sut)` and `(**gen)`, flat by default."""
    flat = {"rss": lambda o: 50 * 2 ** 20, "fds": lambda o: 10, "strings": lambda o: 100}
    timeline = [_phase("start", 0, t0=T0), _phase("end_begin", end_s),
                _phase("end_end", end_s + 50)]
    run_dir = _run_dir(tmp, name, timeline,
                       _service_stdout(end_s + 50, **{**flat, **(sut or {})}),
                       duration=float(end_s), **resolved)
    (run_dir / "logs" / "generator.stdout").write_text(
        "\n".join(_service_stdout(end_s + 50, **{**flat, **(gen or {})})) + "\n")
    return run_dir


# 128 MiB an hour, in bytes a second: twice the fixtures' 64 MiB/h limit.
_FAST_RSS = 128 * 2 ** 20 / 3600


def _per_service():
    """`rss_slope` and `fd_slope` judge each service apart, one detail line each, and an
    `[expected]` verdict scores a service PASS when it comes out as expected, FAIL when not."""
    growing = {"rss": lambda o: 50 * 2 ** 20 + _FAST_RSS * o}
    with tempfile.TemporaryDirectory() as tmp:
        def row(name, check, **kwargs):
            return _status(_slope_run(tmp, name, **kwargs), check)

        result = row("rss-plain", checks.check_rss_slope, sut=growing)
        expect(result.status == checks.FAIL and len(result.detail.split(" | ")) == 2
               and "generator PASS: MiB/h life 0 +0.0" in result.detail
               and "logit FAIL: MiB/h life 0 +128.0" in result.detail,
               f"rss_slope judges each service on its own line, got {result}")
        result = row("rss-gen-only", checks.check_rss_slope, gen=growing)
        expect(result.status == checks.FAIL and "generator FAIL" in result.detail
               and "logit PASS" in result.detail,
               f"rss_slope names the generator as the service that grew, got {result}")
        result = row("rss-expected", checks.check_rss_slope, sut=growing,
                     expected={"rss_slope": {"logit": "FAIL"}})
        expect(result.status == checks.PASS
               and "logit FAIL, as expected: MiB/h life 0 +128.0" in result.detail
               and "generator PASS" in result.detail,
               f"an expected FAIL scores PASS, got {result}")
        result = row("rss-expected-missed", checks.check_rss_slope,
                     expected={"rss_slope": {"logit": "FAIL"}})
        expect(result.status == checks.FAIL
               and "logit: expected FAIL, got PASS" in result.detail,
               f"an expected FAIL that came out PASS FAILs, got {result}")
        result = row("rss-expected-other", checks.check_rss_slope, sut=growing,
                     expected={"rss_slope": {"generator": "PASS"}})
        expect(result.status == checks.FAIL and "logit FAIL" in result.detail
               and "generator PASS, as expected" in result.detail,
               f"a service with no expectation is judged normally, got {result}")
        result = row("rss-expected-warn", checks.check_rss_slope, sut=growing, end_s=1800,
                     expected={"rss_slope": {"logit": "FAIL"}})
        expect(result.status == checks.FAIL
               and "logit: expected FAIL, got WARN" in result.detail,
               f"growth in a run under an hour WARNs, which an expected FAIL refuses, got "
               f"{result}")
        result = row("rss-expected-fd", checks.check_rss_slope, sut=growing,
                     expected={"fd_slope": {"logit": "FAIL"}})
        expect(result.status == checks.FAIL,
               f"an expectation for another row doesn't touch rss_slope, got {result}")

        leaking = {"fds": lambda o: 10 if o < 1800 else 30}
        result = row("fd-plain", checks.check_fd_slope, gen=leaking)
        expect(result.status == checks.FAIL and len(result.detail.split(" | ")) == 2
               and "generator FAIL: warmup median 10 -> end 30" in result.detail
               and "logit PASS: warmup median 10 -> end 10" in result.detail,
               f"fd_slope judges each service on its own line, got {result}")
        result = row("fd-expected", checks.check_fd_slope, gen=leaking,
                     expected={"fd_slope": {"generator": "FAIL"}})
        expect(result.status == checks.PASS and "generator FAIL, as expected" in result.detail,
               f"fd_slope takes an expected FAIL, got {result}")
        result = row("fd-expected-missed", checks.check_fd_slope,
                     expected={"fd_slope": {"generator": "FAIL"}})
        expect(result.status == checks.FAIL
               and "generator: expected FAIL, got PASS" in result.detail,
               f"fd_slope FAILs an expected FAIL that came out PASS, got {result}")


def _slope_edges():
    """The slope rows SKIP with no service to judge, WARN on a service with samples but no
    fittable life, and let an `[expected]` verdict FAIL where they would otherwise SKIP."""
    with tempfile.TemporaryDirectory() as tmp:
        start, end = _phase("start", 0, t0=T0), [_phase("end_begin", 3600),
                                                   _phase("end_end", 3650)]
        # A run recorded before the role map has no `services`, so no service to judge.
        none = _run_dir(tmp, "no-services", [start] + end, _sut_stdout(3600), services={},
                        duration=3600.0)
        for check in (checks.check_rss_slope, checks.check_fd_slope):
            result = _status(none, check)
            expect(result.status == checks.SKIP,
                   f"{check.__name__} SKIPs with no service to judge, got {result}")

        # The generator's telemetry stops at +80: five samples after warmup, too few to fit.
        short = _slope_run(tmp, "unfittable")
        (short / "logs" / "generator.stdout").write_text("\n".join(
            _service_stdout(80, lambda o: 50 * 2 ** 20, lambda o: 10, lambda o: 100)) + "\n")
        result = _status(short, checks.check_rss_slope)
        expect(result.status == checks.WARN
               and "generator WARN: no life with enough samples" in result.detail
               and "logit PASS" in result.detail,
               f"a service with samples but no fittable life WARNs beside a good one, got "
               f"{result}")

        # Every service sampled, none fittable, and no expectation: nothing to judge.
        unfit = _slope_run(tmp, "all-unfittable", end_s=80)
        result = _status(unfit, checks.check_rss_slope)
        expect(result.status == checks.SKIP,
               f"rss_slope SKIPs when no service has a fittable life, got {result}")

        expected = {"fd_slope": {"logit": "FAIL"}}
        untimed = _run_dir(tmp, "fd-no-start", end, _sut_stdout(3600), duration=3600.0,
                           expected=expected)
        result = _status(untimed, checks.check_fd_slope)
        expect(result.status == checks.FAIL and "expected FAIL, got SKIP" in result.detail,
               f"fd_slope FAILs an expectation it can't judge for want of a start, got {result}")
        silent = _run_dir(tmp, "fd-no-samples", [start] + end, duration=3600.0,
                          expected=expected)
        result = _status(silent, checks.check_fd_slope)
        expect(result.status == checks.FAIL and "logit" in result.detail,
               f"fd_slope FAILs an expectation on a service with no fd samples, got {result}")


def _expected_rules(root):
    path, base = _base(root)

    def with_expected(table):
        raw = copy.deepcopy(base)
        raw["expected"] = table
        return raw

    loaded = scenario.from_dict(with_expected({"rss_slope": {"logit": "FAIL"},
                                               "fd_slope": {"generator": "PASS"}}), path)
    scenario.check_services(loaded, scenario.base_roles(loaded))
    expect(loaded.to_json()["expected"] == {"rss_slope": {"logit": "FAIL"},
                                            "fd_slope": {"generator": "PASS"}},
           "the resolved scenario carries [expected]")
    cases = [
        ({"ledger.egress": {"logit": "FAIL"}}, "only rss_slope, fd_slope take an expected",
         "an expectation on a row that doesn't judge per service"),
        ({"rss_slope": {"logit": "fail"}}, "verdict must be one of PASS, WARN, FAIL",
         "a lowercase verdict"),
        ({"rss_slope": {"logit": "SKIP"}}, "verdict must be one of", "a SKIP verdict"),
        ({"rss_slope": {"victoria-metrics": "FAIL"}}, "unknown service `victoria-metrics`",
         "an expectation on a service that doesn't run logit"),
        ({"rss_slope": {}}, "must be a table of services", "an empty row table"),
        ({"rss_slope": "FAIL"}, "must be a table of services", "a bare verdict"),
        ("FAIL", "`expected` must be a table", "a non-table [expected]"),
    ]
    for table, needle, what in cases:
        _refused(path, with_expected(table), needle, what)


def _slope_reducer():
    """The `slope` reducer: the window's samples fitted per life, per hour; the `run` window
    from warmup's end to the end sequence."""
    def exp(**fields):
        return {"name": "x", "service": "logit", "metric": "logit.process.interner.strings",
                "attrs": {}, "step": None, "window": "run", "reduce": "slope", "min": None,
                "max": None, **fields}

    # 100 strings a second after warmup, 360,000 an hour. Warmup holds 0 and the end sequence
    # drops to 0, so a window reaching into either fits another slope.
    after_warmup = {"strings": lambda o: 0 if o <= 60 or o > 3600 else 100 * (o - 60)}
    with tempfile.TemporaryDirectory() as tmp:
        counter = 0

        def rows(expectations, **kwargs):
            nonlocal counter
            counter += 1
            run_dir = _slope_run(tmp, f"slope-{counter}", expect=expectations, **kwargs)
            return [r for r in checks.run_all(run_dir) if r.id.startswith("expect")]

        result = rows([exp(min=359999, max=360001)], sut=after_warmup)[0]
        expect(result.status == checks.PASS and "life 0 360,000/h" in result.detail
               and "run (+60s, +3600s]" in result.detail,
               f"slope over the run window is 100/s as 360,000/h, got {result}")
        result = rows([exp(min=400000)], sut=after_warmup)[0]
        expect(result.status == checks.FAIL and "FAIL" in result.detail,
               f"a slope under its min FAILs, got {result}")
        result = rows([exp(max=-1)], sut=after_warmup)[0]
        expect(result.status == checks.FAIL, f"a slope over its max FAILs, got {result}")

        result = rows([exp(min=0)], end_s=85)[0]
        expect(result.status == checks.FAIL and "too few samples" in result.detail,
               f"a run window under {checks.SLOPE_MIN_SPAN_S}s FAILs, got {result}")

        # Life 0 grows 100/s to +1800; life 1 starts at 0 there and grows 50/s.
        two = {"strings": lambda o: 100 * o if o < 1800 else 50 * (o - 1800),
               "restart_at": 1800}
        result = rows([exp(min=350000)], sut=two)[0]
        expect(result.status == checks.FAIL and "life 0 360,000/h" in result.detail
               and "life 1 180,000/h FAIL" in result.detail,
               f"slope fits each life and judges each, got {result}")
        result = rows([exp(min=170000)], sut=two)[0]
        expect(result.status == checks.PASS,
               f"a restart's drop isn't fitted as a slope, got {result}")
        result = rows([exp(metric="logit.process.memory.resident.bytes", min=0, max=1)])[0]
        expect(result.status == checks.PASS and "life 0 0/h" in result.detail,
               f"a flat gauge's slope is 0, got {result}")

    path, base = _base(Path(__file__).resolve().parent.parent)

    def with_expect(**changes):
        raw = copy.deepcopy(base)
        one = {**VALID_EXPECT, "reduce": "slope", "min": 0, **changes}
        raw["expect"] = [{k: v for k, v in one.items() if v is not None}]
        return raw

    loaded = scenario.from_dict(with_expect(window="run", step=None), path)
    expect(loaded.expectations[0].step is None and loaded.to_json()["expect"][0]["step"] is None,
           "a run window loads with no step")
    _refused(path, with_expect(window="run"), "window `run` spans the whole run and takes no",
             "a run window with a step")
    _refused(path, with_expect(step=None), "missing `step`", "a step window with no step")
    _refused(path, with_expect(metric="logit.input.kernel.dropped"),
             "reads a gauge, and logit.input.kernel.dropped is a sum", "a slope of a counter")
    _refused(path, with_expect(metric="logit.process.interner.strings", reduce="delta"),
             "logit.process.interner.strings is a gauge", "a counter reducer on interner.strings")
    _refused(path, with_expect(metric="logit.process.memory.resident.bytes", reduce="delta"),
             "logit.process.memory.resident.bytes is a gauge", "a counter reducer on RSS")


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


class _ExitDocker:
    """A stand-in for an `exit` step's revert and the driver's start bookkeeping: `inspect`
    answers `status` and `started`, and `run` records each command."""

    def __init__(self, status):
        self.status = status
        self.calls = []

    def inspect(self, container, timeout=None):
        return {"State": {"Status": self.status, "StartedAt": "second"}}

    def run(self, args, timeout=None):
        self.calls.append(list(args))
        return docker.Result(0, "", "")


def _exit_driver():
    """The driver around an `exit` step: its revert starts the container only when it has
    exited, and the fail-fast check accepts `exited` only inside its window."""
    step = scenario.Step("c0s1", 0, 0, "exit", "logit", "", 10.0, 20.0, code=2,
                         key="memory_limit_exceeded")
    for status, starts in (("exited", True), ("running", False)):
        fake = _ExitDocker(status)
        ctx = faults.Context(docker=fake, ids={"logit": "sut"}, netem_image="netem")
        result = faults.ACTIONS["exit"].revert(ctx, step)
        expect(result["rc"] == 0 and result["started"] is starts
               and (fake.calls == [["start", "sut"]]) is starts,
               f"an exit's revert on a {status} container starts it: {starts}, got {result} "
               f"{fake.calls}")
        expect(faults.ACTIONS["exit"].apply(ctx, step) == {"rc": 0, "stderr": ""}
               and len(fake.calls) == int(starts), "an exit's apply makes no Docker call")

    def fresh():
        run = driver.Run.__new__(driver.Run)
        run.down, run.paused, run.exiting = set(), set(), set()
        run.pending_ready, run.fail_fast = {}, None
        run.started_at = {"logit": "first"}
        run.ids = {"logit": "sut"}
        run.roles = dict(scenario.BASE_ROLES)
        run.docker = _ExitDocker("running")
        return run

    exited = {"status": "exited", "exit_code": 2, "started_at": "first"}
    run = fresh()
    run.check_expected("logit", exited)
    expect(run.fail_fast is not None, "an exit with no step behind it fails fast")
    run = fresh()
    run.after("apply", step, {"rc": 0})
    run.check_expected("logit", exited)
    run.check_expected("logit", {"status": "running", "exit_code": 0, "started_at": "first"})
    expect(run.fail_fast is None, f"an exit step accepts exited and running, got "
                                  f"{run.fail_fast}")
    run.after("revert", step, {"rc": 0, "started": True})
    expect(run.started_at["logit"] == "second" and "logit" in run.pending_ready
           and not run.exiting, "a revert that started the SUT probes its readiness")
    run.check_expected("logit", {**exited, "started_at": "second"})
    expect(run.fail_fast is not None, "after the window, exited fails fast again")
    run = fresh()
    run.after("apply", step, {"rc": 0})
    run.after("revert", step, {"rc": 0, "started": False})
    expect(not run.pending_ready and run.started_at["logit"] == "first",
           "a revert that started nothing probes nothing")


def _health(offset, word, start=None):
    """A `health_log` entry for a HEALTHCHECK that started at `start` (default `offset`)."""
    output = "ok\n" if word == "ok" else f"{word} (503 Service Unavailable)\n"
    started = _stamp(T0 + (offset if start is None else start))
    return {"Start": started, "End": started, "ExitCode": 0 if word == "ok" else 1,
            "Output": output}


def _ready_words():
    """`ready` from the status word in `health_log`: `ok` in steady time, a mark's word inside
    it, `degraded` only inside an exit step, each sample timed at its `Start` and counted once,
    and `Health.Status` when no `health_log` was recorded."""
    mark = [_action("apply", "c0s1", "mark", "logit", 100, 100, ready="stalled"),
            _action("revert", "c0s1", "mark", "logit", 160, 160, ready="stalled")]
    exit_step = [_action("apply", "c0s1", "exit", "logit", 100, 100, code=2,
                         key="memory_limit_exceeded"),
                 _action("revert", "c0s1", "exit", "logit", 160, 161, code=2,
                         key="memory_limit_exceeded", started=True),
                 _ready("c0s1", "logit", 162)]
    start, end = _phase("start", 0, t0=T0), [_phase("end_begin", 400), _phase("end_end", 450)]

    def run(tmp, name, steps, words, health_log=True):
        """`words(offset)` -> (word, start offset or None) for the SUT's sample at `offset`."""
        run_dir = _run_dir(tmp, name, [start] + steps + end)
        records = []
        for offset in range(0, 400, 5):
            word, started = words(offset)
            for svc in ("logit", "generator"):
                record = {"t": T0 + offset, "svc": svc, "status": "running",
                          "health": "healthy" if svc != "logit" or word == "ok" else "unhealthy"}
                if health_log:
                    record["health_log"] = (_health(offset, word, started) if svc == "logit"
                                            else _health(offset, "ok"))
                records.append(record)
        (run_dir / "watchdog.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
        return run_dir

    def ok(offset):
        return "ok", None

    def stalled_in(low, high):
        return lambda offset: ("stalled", None) if low <= offset <= high else ("ok", None)

    with tempfile.TemporaryDirectory() as tmp:
        result = _status(run(tmp, "ready-ok", [], ok), checks.check_ready)
        expect(result.status == checks.PASS and "read ok" in result.detail,
               f"ready PASSes ok samples, got {result}")
        samples = checks.health_samples(checks.RunData(Path(tmp) / "ready-ok"))
        expect(len(samples) == 160 and samples[0][2] == "ok",
               f"one sample per service and Start, got {len(samples)}")

        result = _status(run(tmp, "ready-mark", mark, stalled_in(120, 140)), checks.check_ready)
        expect(result.status == checks.PASS and "c0s1: logit read stalled 5 time(s)"
               in result.detail, f"ready PASSes a stall inside a mark that expects it, got "
                                 f"{result}")
        result = _status(run(tmp, "ready-mark-missing", mark, ok), checks.check_ready)
        expect(result.status == checks.FAIL and "never read stalled" in result.detail,
               f"ready FAILs a mark whose word never appears, got {result}")
        result = _status(run(tmp, "ready-stall-steady", [], stalled_in(250, 260)),
                         checks.check_ready)
        expect(result.status == checks.FAIL and "read stalled at +250s" in result.detail,
               f"ready FAILs a stall outside every fault window, got {result}")
        # Recorded at +200 to +215, after the mark's window and its recovery, but started at
        # +150, inside the mark: the HEALTHCHECK's own time decides.
        late = run(tmp, "ready-late-record", mark,
                   lambda o: ("stalled", 150) if 200 <= o <= 215 else ("ok", None))
        result = _status(late, checks.check_ready)
        expect(result.status == checks.PASS and "read stalled 1 time(s)" in result.detail,
               f"ready times a sample at its Start and counts it once, got {result}")

        degraded = lambda o: ("degraded", None) if 120 <= o <= 130 else ("ok", None)
        result = _status(run(tmp, "ready-degraded-exit", exit_step, degraded),
                         checks.check_ready)
        expect(result.status == checks.PASS, f"ready accepts degraded inside an exit step, got "
                                             f"{result}")
        result = _status(run(tmp, "ready-degraded-mark", mark, lambda o: (
            ("degraded", None) if 120 <= o <= 130 else ("stalled", None) if o == 140
            else ("ok", None))), checks.check_ready)
        expect(result.status == checks.FAIL and "degraded" in result.detail
               and "outside every exit step" in result.detail,
               f"ready FAILs degraded outside every exit step, even inside a mark, got {result}")

        result = _status(run(tmp, "ready-fallback", [], stalled_in(250, 260), health_log=False),
                         checks.check_ready)
        expect(result.status == checks.FAIL and "health unhealthy" in result.detail,
               f"ready falls back to Health.Status with no health_log, got {result}")
        result = _status(run(tmp, "ready-fallback-mark", mark, ok, health_log=False),
                         checks.check_ready)
        expect(result.status == checks.FAIL and "can't be judged" in result.detail,
               f"a mark's word can't pass with no health_log, got {result}")
    expect(checks.readiness_word("stalled (503 Service Unavailable)") == "stalled"
           and checks.readiness_word("ok\n") == "ok" and checks.readiness_word("") is None,
           "readiness_word takes the status word")


def _exit_watchdog():
    """The watchdog around an `exit` step applied at +60 and reverted at +120: the SUT exits
    by itself at +75 with code 2 and an ERROR line keyed `memory_limit_exceeded`, and the
    revert starts it again."""
    keyed = {"code": 2, "key": "memory_limit_exceeded"}
    steps = [_action("apply", "c0s1", "exit", "logit", 60, 60, **keyed),
             _action("revert", "c0s1", "exit", "logit", 119, 120, started=True, **keyed)]
    start, end = _phase("start", 0, t0=T0), [_phase("end_begin", 400), _phase("end_end", 450)]

    def log(offset, level, message, **fields):
        return json.dumps({"timestamp": _stamp(T0 + offset), "level": level,
                           "message": message, "target": "logit", **fields})

    trip = [log(74.5, "ERROR", "Lua VM holds 70000000 bytes after 8 full collections",
                key="memory_limit_exceeded", component="enrich"),
            log(75, "ERROR", "exiting", code=2, reason="a node failed"),
            "Error: component 'enrich': Lua VM holds 70000000 bytes after 8 full collections"]

    def run(tmp, name, exits=True, code=2, at=75, stderr=trip, steps=steps, ready=True):
        timeline = [start] + steps + ([_ready("c0s1", "logit", 122)] if ready else []) + end
        run_dir = _run_dir(tmp, name, timeline)
        first, second = _stamp(T0 - 5), _stamp(T0 + 119.5)
        watchdog = []
        for offset in range(0, 400, 5):
            for svc in ("logit", "generator"):
                down = svc == "logit" and exits and at < offset < 120
                restarted = svc == "logit" and exits and offset >= 120
                watchdog.append({
                    "t": T0 + offset, "svc": svc, "status": "exited" if down else "running",
                    "exit_code": code if down else 0, "health": "healthy",
                    "started_at": second if restarted else first,
                    "finished_at": _stamp(T0 + at) if svc == "logit" and exits and offset > at
                    else "", "restart_count": 0})
        (run_dir / "watchdog.jsonl").write_text("".join(json.dumps(r) + "\n" for r in watchdog))
        (run_dir / "inspect").mkdir()
        for svc, started in (("logit", second if exits else first), ("generator", first)):
            (run_dir / "inspect" / f"{svc}.json").write_text(json.dumps({"State": {
                "Status": "exited", "ExitCode": 0, "OOMKilled": False, "StartedAt": started,
                "FinishedAt": _stamp(T0 + 450)}}))
        (run_dir / "logs" / "logit.stderr").write_text("\n".join(
            [log(0, "INFO", "ready")] + list(stderr)) + "\n")
        return run_dir

    with tempfile.TemporaryDirectory() as tmp:
        clean = run(tmp, "exit-clean")
        for check in (checks.check_exit, checks.check_restarts, checks.check_timeline,
                      checks.check_self_log):
            result = _status(clean, check)
            expect(result.status == checks.PASS,
                   f"{check.__name__} PASSes an expected exit and its start, got {result}")
        result = _status(clean, checks.check_exit)
        expect("inside expected exit c0s1" in result.detail, f"exit names the step, got {result}")
        result = _status(clean, checks.check_self_log)
        expect("3 expected-exit line(s)" in result.detail,
               f"self_log counts the ERROR, exiting, and Error: lines, got {result}")
        stray = run(tmp, "exit-stray-error-text", stderr=trip + [trip[2]])
        result = _status(stray, checks.check_self_log)
        expect(result.status == checks.WARN and "non-JSON" in result.detail,
               f"self_log excuses only the Error: line right after an expected exiting line, "
               f"got {result}")

        unstarted = [steps[0], {**steps[1], "started": False}]
        missing = run(tmp, "exit-missing", exits=False, stderr=(), steps=unstarted, ready=False)
        for check in (checks.check_exit, checks.check_self_log):
            result = _status(missing, check)
            expect(result.status == checks.FAIL
                   and "expected exit did not happen" in result.detail,
                   f"{check.__name__} FAILs an exit step whose exit never came, got {result}")
        result = _status(missing, checks.check_restarts)
        expect(result.status == checks.PASS, f"restarts needs no start when the revert started "
                                             f"nothing, got {result}")

        wrong = run(tmp, "exit-code-1", code=1)
        result = _status(wrong, checks.check_exit)
        expect(result.status == checks.FAIL and "which leaves 2" in result.detail,
               f"exit FAILs an exit with a code other than its step's, got {result}")
        unkeyed = run(tmp, "exit-no-key", stderr=trip[1:])
        result = _status(unkeyed, checks.check_self_log)
        expect(result.status == checks.FAIL
               and "no ERROR line keyed memory_limit_exceeded" in result.detail,
               f"self_log FAILs an expected exit with no ERROR line keyed its key, got {result}")
        other_code = run(tmp, "exit-line-code-3", stderr=[trip[0], log(75, "ERROR", "exiting",
                                                                         code=3)])
        result = _status(other_code, checks.check_self_log)
        expect(result.status == checks.FAIL
               and "exiting with code 3" in " ".join(result.lines),
               f"self_log FAILs an exiting line with a code the step doesn't name, got {result}")
        outside = run(tmp, "exit-line-outside", stderr=trip + [
            log(300, "ERROR", "Lua VM holds", key="memory_limit_exceeded")])
        result = _status(outside, checks.check_self_log)
        expect(result.status == checks.FAIL and "+300s" in result.detail,
               f"self_log FAILs the step's key outside its window, got {result}")
        quiet = [line for line in _sut_stdout(400)
                 if not 75 < (telemetry.parse_rfc3339(json.loads(line)["timestamp"]) - T0) < 120]
        gap = run(tmp, "exit-gap")
        (gap / "logs" / "logit.stdout").write_text("\n".join(quiet) + "\n")
        result = _status(gap, checks.check_progress)
        expect(not any("went quiet" in line for line in result.lines),
               f"progress doesn't flag the stdout gap an expected exit leaves, got {result}")
        early = run(tmp, "exit-before-window", at=50,
                    stderr=[log(49.5, "ERROR", "x", key="memory_limit_exceeded"),
                            log(50, "ERROR", "exiting", code=2)])
        result = _status(early, checks.check_exit)
        expect(result.status == checks.FAIL and "outside every scheduled stop" in result.detail,
               f"exit FAILs an exit before its step's window, got {result}")


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
              late_report=False, no_drains=None, sut_extra=()):
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
    `(service, action, start, end)` of one over those offsets. `sut_extra` adds raw SUT
    telemetry lines."""
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
    run_dir = _run_dir(tmp, name, timeline, sut + list(sut_extra), ledger=ledger,
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
    _write_export(run_dir, "sink", export)
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
        expect("life 1 (killed): Ab 14,000 − V[sink] 12,500 = 1,500, within band" in egress
               and "band [-1,000, 3,000]: ingest 200/s" in egress
               and "residual W − D − B − X − Ab 0 at its last drain" in egress
               and "D has no shutdown part" in egress,
               f"egress reports the killed life's gap inside its band, got {egress}")
        expect("life 2 (final): Ab 18,000 − V[sink] 18,000 = 0, ok" in egress,
               f"the final life stays exact, got {egress}")
        overflow = [_line(_stamp(T0 + 180), "logit.component.batches.dropped", "sum", 4,
                          component="sink", attrs={"reason": "overflow_oldest"})]
        reported = results("kill-overflow", sut_extra=overflow)
        expect(reported["ledger.egress"].status == checks.PASS
               and "within band" in reported["ledger.egress"].detail
               and "batches dropped: overflow_oldest 4, reported, not judged"
               in reported["ledger.egress"].detail,
               f"a killed life's overflow drops are reported beside its band, got "
               f"{reported['ledger.egress']}")

        edge_high = results("kill-high-edge", kill_gap=3000)
        expect(edge_high["ledger.egress"].status == checks.PASS,
               f"a killed life's gap of one window plus one drain interval of ingest passes, got "
               f"{edge_high['ledger.egress']}")
        over = results("kill-over", kill_gap=3001)
        expect(over["ledger.egress"].status == checks.FAIL
               and "life 1 (killed): Ab 14,000 − V[sink] 10,999 = 3,001, uncounted"
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
               and "Ab 14,000 − V[sink] 11,400 = 2,600, within band (band [-1,500, 3,000]" in detail
               and "residual W − D − B − X − Ab 500 at its last drain" in detail,
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
               and "2 series over 1 sink(s) x 3 life/lives, widest gap 10s" in clean["ledger.windows"].detail,
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
        expect(checks.component_duration(KILL_SUT_CONFIG, "window", "interval") == 10.0
               and checks.component_duration("components:\n  window: { type: aggregate, "
                                              "interval: 2m }\n", "window", "interval") == 120.0
               and checks.component_duration(KILL_SUT_CONFIG, "in", "interval") is None,
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
        # Life 2's spool opens on a torn tail: the batch life 1 lost with it is counted dropped
        # in life 2, which never received it.
        torn = results("kill-torn-tail", sut_extra=(
            _line(_stamp(T0 + 215), "logit.component.batches.dropped", "sum", 1,
                  component="sink", attrs={"reason": "disk_torn"}),))
        expect(torn["identity.sink"].status == checks.PASS
               and "received 21 + replayed 3 + torn 1 vs delivered 24 + dropped 1"
               in torn["identity.sink"].detail,
               f"identity.sink settles a torn tail's drop against the batch an earlier life "
               f"received, got {torn['identity.sink']}")
        # The same drop can't explain a gap in life 2: it settles a batch life 1 received.
        torn_gap = results("kill-torn-tail-gap", sut_extra=(
            _line(_stamp(T0 + 215), "logit.component.batches.dropped", "sum", 1,
                  component="sink", attrs={"reason": "disk_torn"}),
            _line(_stamp(T0 + 220), "logit.transform.metrics.absorbed", "sum", 500,
                  component="window")))
        expect(torn_gap["ledger.egress"].status == checks.FAIL
               and "uncounted" in torn_gap["ledger.egress"].detail,
               f"egress doesn't credit a torn tail's drop against the life that counts it, got "
               f"{torn_gap['ledger.egress']}")
        stray = results("kill-stray-drop", stray_drop=True)
        stray_led = checks.Ledger(checks.RunData(Path(tmp) / "kill-stray-drop"))
        expect(1 not in stray_led.D_log and stray["ledger.intake"].status == checks.PASS,
               f"a killed life's D takes nothing from stderr, got {stray_led.D_log} "
               f"{stray['ledger.intake']}")


def _external_base(root):
    path = root / "scenarios" / "statsd-datadog" / "scenario.toml"
    return path, tomllib.loads(path.read_text())


def _target_rules(root):
    """The `[target]` table's validation rules, and what an external target refuses."""
    path, base = _external_base(root)
    vm_path, vm_base = _base(root)

    def mutate(raw, edit):
        raw = copy.deepcopy(raw)
        edit(raw)
        return raw

    loaded = scenario.from_dict(copy.deepcopy(base), path)
    expect(loaded.external and loaded.target == {"kind": "external", "name": "datadog",
                                                 "env": ["DD_API_KEY"]},
           f"statsd-datadog loads as an external target, got {loaded.target}")
    resolved = loaded.to_json()
    expect(resolved["target"]["kind"] == "external"
           and resolved["ledger"]["sink"] == [{"id": "datadog"}],
           f"the resolved scenario records the target, got {resolved['target']}")
    local = scenario.from_dict(copy.deepcopy(vm_base), vm_path)
    expect(not local.external and local.to_json()["target"]["kind"] == "victoria-metrics",
           "a scenario with no [target] is the local target")

    def table(**changes):
        return lambda raw: raw["target"].update(changes)

    cases = [
        (base, table(kinds="external"), "target: unknown key `kinds`", "an unknown target key"),
        (base, table(kind="splunk"), "target.kind must be", "an unknown target kind"),
        (base, table(name="Data Dog"), "target.name must be lowercase", "a target name"),
        (base, lambda raw: raw["target"].pop("name"), "target.name must be lowercase",
         "an external target with no name"),
        (base, table(env="DD_API_KEY"), "target.env must be an array", "a string env"),
        (base, table(env=["dd-api-key"]), "isn't an environment variable name",
         "a malformed variable name"),
        (base, table(env=["DD_SITE"]), "compose.yaml doesn't pass DD_SITE",
         "a variable compose.yaml doesn't pass"),
        (base, table(env=["DD_API_KEY", "DD_API_KEY"]), "target.env repeats a variable",
         "a repeated variable"),
        (base, lambda raw: raw["ledger"]["sink"][0].update(vm_selector="{x}"),
         "`vm_selector` names a backend query", "vm_selector on an external target"),
        (base, lambda raw: raw["ledger"]["sink"][0].update(backend="victoria-metrics"),
         "`backend` names a backend query", "a backend on an external target"),
        (base, lambda raw: raw["ledger"].update(vm_every_window=True),
         "ledger.vm_every_window names a VictoriaMetrics query",
         "vm_every_window on an external target"),
        (base, lambda raw: raw["ledger"].pop("sink"), "ledger: missing `sink`",
         "an external target still needs the sink id"),
        (base, lambda raw: raw["step"].append({"at": "11m", "action": "stop",
                                               "on": "victoria-metrics", "for": "20s"}),
         "unknown service `victoria-metrics`; an external target's stack runs no backend",
         "a fault on victoria-metrics with an external target"),
        (vm_base, lambda raw: raw.update(target={"kind": "victoria-metrics", "name": "vm"}),
         "target.name is for an external or kafka target", "a name on the local target"),
        (vm_base, lambda raw: raw.update(target="external"), "`target` must be a table",
         "a target that isn't a table"),
    ]
    for raw, edit, needle, what in cases:
        _refused(path if raw is base else vm_path, mutate(raw, edit), needle, what)

    random_path, random_base = _random_base(root)

    def external_random(raw):
        raw["target"] = {"kind": "external", "name": "datadog", "env": ["DD_API_KEY"]}
        raw["ledger"]["sink"] = [{"id": "datadog"}]
        raw["ledger"].pop("vm_every_window", None)
        raw["random"]["fault"].append({"weight": 1, "action": "pause",
                                       "on": "victoria-metrics",
                                       "for": {"min": "30s", "max": "60s"}})

    _refused(random_path, mutate(random_base, external_random),
             "random.fault", "a random fault on victoria-metrics with an external target")

    compose = (root / "compose.yaml").read_text()
    for variable in scenario.EXTERNAL_ENV_PASSED:
        expect(f"{variable}: ${{{variable}:-}}" in compose,
               f"compose.yaml passes {variable} to the SUT, empty when unset")
    expect("profiles: [local]" in compose.split("  logit:")[0],
           "compose.yaml puts victoria-metrics in the local profile")
    expect("required: false" in compose.split("  logit:")[1].split("  generator:")[0],
           "the SUT's depends_on doesn't require victoria-metrics")

    with tempfile.TemporaryDirectory() as tmp:
        env = Path(tmp) / "external.env"
        expect(scenario.missing_env(loaded, env) == ["DD_API_KEY"],
               "a missing env file sets nothing")
        env.write_text("# a comment\nDD_API_KEY=\nOTHER=1\n")
        expect(scenario.missing_env(loaded, env) == ["DD_API_KEY"], "an empty value is missing")
        env.write_text("DD_API_KEY=\nDD_API_KEY=abc\n")
        expect(scenario.missing_env(loaded, env) == [], "the last line for a key wins")
        expect(scenario.missing_env(local, Path(tmp) / "nothing") == [],
               "a local target needs no env file")
        env.write_text("# shell form\n\nexport DD_API_KEY='abc def'\nexport  OTHER=\"x\"\n"
                       "PLAIN= y \nHASH=a#b\nODD='z\"\nTWICE=''x''\n")
        values = scenario.read_env_file(env)
        expect(values == {"DD_API_KEY": "abc def", "OTHER": "x", "PLAIN": "y", "HASH": "a#b",
                          "ODD": "'z\"", "TWICE": "'x'"},
               f"read_env_file strips export and one pair of matching quotes, got {values}")
        copy_path = scenario.private_env_copy(env, ["DD_API_KEY", "MISSING"])
        try:
            copy_dir = copy_path.parent
            expect(copy_dir.name.startswith(scenario.PRIVATE_ENV_PREFIX)
                   and not copy_dir.is_relative_to(tmp)
                   and not copy_dir.is_relative_to(root.parent.parent),
                   f"the private copy is in its own temporary directory, got {copy_path}")
            expect((copy_path.stat().st_mode & 0o777) == 0o600
                   and (copy_dir.stat().st_mode & 0o777) == 0o700,
                   f"the private copy is mode 0600 in a 0700 directory, got "
                   f"{copy_path.stat().st_mode:o} {copy_dir.stat().st_mode:o}")
            expect(copy_path.read_text() == "DD_API_KEY=abc def\n",
                   "the private copy holds the listed variables it sets, plain and unquoted")
        finally:
            scenario.remove_private_env(copy_path)
        expect(not copy_dir.exists(), "remove_private_env removes the copy's directory")
        bystander = Path(tmp) / "keep" / "x.env"
        bystander.parent.mkdir()
        bystander.write_text("")
        scenario.remove_private_env(bystander)
        expect(bystander.exists(), "remove_private_env leaves a directory it didn't make")
        env.write_text("export DD_API_KEY=abc\n")

        argv = docker.Docker("p", ["c.yaml"], ["a.env", "b.env"], ["local"], prefix=["docker"])
        expect(argv.compose_args() == ["compose", "--progress", "quiet", "-p", "p", "-f",
                                       "c.yaml", "--env-file", "a.env", "--env-file", "b.env",
                                       "--profile", "local"],
               f"compose gets every env file in order and each profile, got "
               f"{argv.compose_args()}")

        run = driver.Run(root, loaded, loaded.duration, None, False, tmp, [], "logit:soak",
                         env, roles=scenario.base_roles(loaded))
        run.prepare()
        private = run.private_env
        expect(run.docker.profiles == [] and private is not None
               and run.docker.env_files[-1] == str(private)
               and private.read_text() == "DD_API_KEY=abc\n"
               and not private.is_relative_to(run.run_dir),
               f"an external run has no local profile and passes a plain private copy of its "
               f"env file last, got {run.docker.profiles} {run.docker.env_files}")
        expect("DD_API_KEY" not in (run.run_dir / "compose.env").read_text()
               and json.loads((run.run_dir / "scenario.resolved.json").read_text())
               ["target"]["name"] == "datadog",
               "compose.env holds no credential, and the resolved scenario names the target")
        for jsonl in (run.timeline, run.watchdog, run.stats, run.freshness, run.capture):
            jsonl.close()
        run.docker = _BusyDocker()
        real_print = driver._print
        driver._print = lambda text, stream=None: None
        try:
            code = run.execute()
        finally:
            driver._print = real_print
        expect(code == 1 and not private.exists() and not private.parent.exists(),
               "execute() removes the private env copy, an early return included")
        vm_run = driver.Run(root, local, local.duration, None, False, Path(tmp) / "vm", [],
                            "logit:soak", roles=scenario.BASE_ROLES)
        vm_run.prepare()
        expect(vm_run.docker.profiles == ["local"] and len(vm_run.docker.env_files) == 1,
               f"a local run enables the local profile, got {vm_run.docker.profiles}")
        for jsonl in (vm_run.timeline, vm_run.watchdog, vm_run.stats, vm_run.freshness,
                      vm_run.capture):
            jsonl.close()
        _empty_env(root, env, path, tmp)
        _unsafe_env(root, loaded, env, path, tmp)
        _prepare_raises(root, path, env, tmp)


class _Refused(Exception):
    pass


def _no_docker(*args, **kwargs):
    """Stands in for `driver.Docker` around a `driver.run` that must stop before
    `prepare()`."""
    raise _Refused()


def _empty_env(root, env, path, tmp):
    """`driver.run` refuses an env file that sets a listed variable empty, by name, before it
    builds a `Docker`."""
    env.write_text("DD_API_KEY=\n")
    real_docker, real_signals = driver.Docker, driver.interrupt_on_signals
    driver.Docker, driver.interrupt_on_signals = _no_docker, lambda: None
    try:
        driver.run(root, path, None, None, False, Path(tmp) / "refused", [], "logit:soak", env)
        expect(False, "a run with DD_API_KEY empty started")
    except _Refused:
        expect(False, "a run with DD_API_KEY empty reached prepare()")
    except scenario.ScenarioError as err:
        expect("doesn't set DD_API_KEY" in str(err) and "abc" not in str(err),
               f"a run names the missing variable, got {err}")
    finally:
        driver.Docker, driver.interrupt_on_signals = real_docker, real_signals


def _isolated():
    """`run` points `$DOCKER` at a path that doesn't exist for the whole suite."""
    expect(os.environ.get("DOCKER") == _NO_DAEMON and docker.docker_argv() == [_NO_DAEMON],
           f"the self-test runs with DOCKER={_NO_DAEMON}, got {os.environ.get('DOCKER')!r}")


def _unsafe_env(root, loaded, env, path, tmp):
    """A listed value holding a character compose's env-file parser alters or rejects is
    refused by name, by `unsafe_env`, `soak.py target --env-file`, and `driver.run`."""
    env.write_text("DD_API_KEY=FAKEkey0123\n")
    expect(scenario.unsafe_env(loaded, env) == [], "a plain value passes the compose check")
    cases = [("$", "a `$`"), ("'", "a single quote"), ('"', "a double quote"),
             ("`", "a backtick"), ("\\", "a backslash"), ("#", "a `#`"), (" ", "whitespace"),
             ("\t", "whitespace")]
    for char, label in cases:
        env.write_text(f"DD_API_KEY=FAKE{char}key\n")
        got = scenario.unsafe_env(loaded, env)
        expect(got == [f"DD_API_KEY contains {label}"],
               f"a value with {label} is refused by name, got {got}")
    env.write_text("export DD_API_KEY=\"FAKEkey0123\n")
    out = subprocess.run([sys.executable, str(root / "soak.py"), "target", "statsd-datadog",
                          "--env-file", str(env), "--private-copy"],
                         capture_output=True, text=True, timeout=60)
    if out.stdout.strip():
        scenario.remove_private_env(Path(out.stdout.strip()))
    expect(out.returncode == 1 and out.stdout == ""
           and "DD_API_KEY contains a double quote" in out.stderr
           and "FAKEkey" not in out.stderr,
           f"soak.py target refuses an unmatched quote by name and writes no copy, got "
           f"{out.returncode} {out.stdout!r} {out.stderr.strip()[-300:]!r}")

    # With the check removed, the run must stop here rather than reach the daemon.
    real_docker, real_signals = driver.Docker, driver.interrupt_on_signals
    driver.Docker, driver.interrupt_on_signals = _no_docker, lambda: None
    try:
        driver.run(root, path, None, None, False, Path(tmp) / "unsafe", [], "logit:soak", env)
        expect(False, "a run with a quote in DD_API_KEY started")
    except _Refused:
        expect(False, "a run with a quote in DD_API_KEY reached prepare()")
    except scenario.ScenarioError as err:
        expect("DD_API_KEY contains a double quote" in str(err) and "FAKEkey" not in str(err)
               and not (Path(tmp) / "unsafe").exists(),
               f"a run refuses an unsafe value by name before it writes anything, got {err}")
    finally:
        driver.Docker, driver.interrupt_on_signals = real_docker, real_signals


def _prepare_raises(root, path, env, tmp):
    """`driver.run` installs its signal handlers before `prepare()`, and removes the private
    env copy when `prepare()` raises after writing it."""
    env.write_text("DD_API_KEY=FAKEkey0123\n")
    order = []

    class Raising:
        def __init__(self, project, compose_files, env_files, profiles=(), prefix=None):
            order.append(("docker", Path(env_files[-1])))
            raise RuntimeError("no daemon")

    real_docker, real_signals = driver.Docker, driver.interrupt_on_signals
    driver.Docker = Raising
    driver.interrupt_on_signals = lambda: order.append(("signals", None))
    try:
        driver.run(root, path, None, None, False, Path(tmp) / "raises", [], "logit:soak", env)
        expect(False, "a run whose Docker constructor raised returned")
    except RuntimeError:
        pass
    finally:
        driver.Docker, driver.interrupt_on_signals = real_docker, real_signals
    kinds = [kind for kind, _ in order]
    expect(kinds == ["signals", "docker"],
           f"run() installs its signal handlers before prepare(), got {kinds}")
    copies = [copy_path for kind, copy_path in order if kind == "docker"]
    expect(len(copies) == 1 and copies[0].parent.name.startswith(scenario.PRIVATE_ENV_PREFIX)
           and not copies[0].parent.exists(),
           f"run() removes the private env copy when prepare() raises after writing it, got "
           f"{copies}")


class _BusyDocker:
    """A daemon where the run's compose project already has containers, so `execute()` returns
    before it starts anything."""

    prefix = ["docker"]
    compose_files = ["compose.yaml"]

    def project_containers(self):
        return ["leftover"]


class _TailDocker:
    """`logs_tail` returning the SUT stdout `lines(now)` builds, on a fake clock."""

    def __init__(self, clock, lines):
        self.clock = clock
        self.lines = lines
        self.calls = 0

    def logs_tail(self, container, lines, timeout=0):
        self.calls += 1
        return docker.Result(0, "\n".join(self.lines(self.clock[0])) + "\n", "")


def _sink_drain(root):
    """The external end sequence's wait: two aggregate windows after the generator stops, then
    the sink's `buffer.batches` 0 and `retrying` 0 or never set, at the newest drain."""
    loaded = scenario.load(root / "scenarios" / "statsd-datadog" / "scenario.toml")
    expect(driver.Run(root, loaded, 600, None, False, "/nonexistent", [],
                      "logit:soak").aggregate_interval() == 10.0,
           "the wait reads the SUT config's aggregate interval")

    def drains(queued, retrying=None):
        def lines(now):
            out = []
            for t in range(int(now) - 30, int(now) + 1, 5):
                stamp = _stamp(t)
                out.append(_line(stamp, "logit.process.uptime", "gauge", float(t - 900)))
                out.append(_line(stamp, "logit.component.buffer.batches", "gauge", queued(t),
                                 component="datadog"))
                if retrying is not None:
                    out.append(_line(stamp, "logit.component.retrying", "gauge", retrying(t),
                                     component="datadog"))
            return out
        return lines

    def wait(lines):
        run = driver.Run(root, loaded, 600, None, False, "/nonexistent", [], "logit:soak")
        clock = [1000.0]
        real_now, real_sleep, real_print = driver._now, driver.time.sleep, driver._print
        driver._now = lambda: clock[0]
        driver._print = lambda text, stream=None: None
        driver.time.sleep = lambda s: clock.__setitem__(0, clock[0] + s)
        try:
            run.ids = {"logit": "a"}
            run.docker = _TailDocker(clock, lines)
            run.timeline = _Records()
            run.t0 = 900.0
            run.wait_sink_drained()
        finally:
            driver._now, driver.time.sleep, driver._print = real_now, real_sleep, real_print
        return run.timeline[-1]

    record = wait(drains(lambda t: 0))
    expect(record["phase"] == "sink_drained" and record["held"] and record["waited_s"] >= 20,
           f"an empty queue holds once two windows pass, got {record}")
    record = wait(drains(lambda t: 1 if t < 1040 else 0, lambda t: 1 if t < 1030 else 0))
    expect(record["held"] and record["waited_s"] >= 40,
           f"the wait outlasts a queue still sending, got {record}")
    record = wait(drains(lambda t: 1))
    expect(not record["held"] and record["sinks"]["datadog"]["buffer_batches"] == 1,
           f"a queue that never empties ends the wait at its bound, got {record}")
    record = wait(drains(lambda t: 0, lambda t: 1))
    expect(not record["held"] and record["sinks"]["datadog"]["retrying"] == 1,
           f"a sink still retrying never counts as drained, got {record}")


def _datadog_lines(rejected_at=None, retry_at=(), stale=0, skipped=0, records=True,
                   batch=True, cls="2xx", reason="rejected", named=0, queued_at=None,
                   unsent=0):
    """`datadog_out` telemetry for the ledger fixture's SUT drains: one request answered `cls`
    and 100 records accepted per drain, plus what each keyword adds. A rejection at
    `rejected_at` drops its records (`records`, under `reason`) and its batch (`batch`): a
    request rejected while another of the batch's was accepted drops records only. `named`
    records come out of an accepted body at +250; `queued_at` adds three batches received
    there and never sent, for a `buffer_batches=3` fixture; `unsent` metrics are dropped by the
    encoder at +250."""
    drains = [o for o in range(5, STOP[0], 5)] + [o for o in range(135, 321, 5)]
    lines = []
    for offset in drains:
        stamp = _stamp(T0 + offset)
        lines.append(_line(stamp, "logit.output.requests", "sum", 1, component="sink",
                           attrs={"route": "series", "class": cls}))
        lines.append(_line(stamp, "logit.output.records", "sum", 100, component="sink",
                           attrs={"route": "series"}))
    for offset in retry_at:
        lines.append(_line(_stamp(T0 + offset), "logit.output.requests", "sum", 2,
                           component="sink", attrs={"route": "series", "class": "5xx"}))
    if rejected_at is not None:
        stamp = _stamp(T0 + rejected_at)
        lines.append(_line(stamp, "logit.output.requests", "sum", 1, component="sink",
                           attrs={"route": "series", "class": "4xx"}))
        if records:
            lines.append(_line(stamp, "logit.output.records.dropped", "sum", 100,
                               component="sink", attrs={"route": "series", "reason": reason}))
        if batch:
            lines.append(_line(stamp, "logit.component.batches.dropped", "sum", 1,
                               component="sink", attrs={"reason": "rejected"}))
            lines.append(_line(stamp, "logit.component.batches.received", "sum", 1,
                               component="sink"))
    if stale:
        lines.append(_line(_stamp(T0 + 250), "logit.output.records.dropped", "sum", stale,
                           component="sink", attrs={"route": "series", "reason": "stale"}))
    if skipped:
        lines.append(_line(_stamp(T0 + 250), "logit.output.metrics.skipped", "sum", skipped,
                           component="sink", attrs={"metric_kind": "cumulative_sum"}))
    if unsent:
        lines.append(_line(_stamp(T0 + 250), "logit.output.metrics.dropped", "sum", unsent,
                           component="sink", attrs={"reason": "non_finite_value"}))
    if named:
        lines.append(_line(_stamp(T0 + 250), "logit.output.records.dropped", "sum", named,
                           component="sink", attrs={"route": "series", "reason": "rejected"}))
    if queued_at is not None:
        lines.append(_line(_stamp(T0 + queued_at), "logit.component.batches.received", "sum",
                           3, component="sink"))
    return lines


def _external_checks():
    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            run_dir = _ledger_run(tmp, name, external=True, **kwargs)
            return {r.id: r for r in checks.run_all(run_dir)}

        clean = results("external-clean", sut_extra=_datadog_lines())
        sent = clean["ledger.sent"]
        expect(sent.status == checks.PASS and sent.detail.startswith("SENT to datadog")
               and "a 2xx is the end of the evidence" in sent.detail,
               f"ledger.sent reads SENT for a clean external run, got {sent}")
        for check_id in ("ledger.egress", "ledger.windows"):
            expect(clean[check_id].status == checks.SKIP
                   and checks.NO_BACKEND in clean[check_id].detail,
                   f"{check_id} SKIPs on an external target, got {clean[check_id]}")
        summary = clean["ledger.summary"]
        expect(summary.status == checks.PASS and f"Ab − V SKIP ({checks.NO_BACKEND})"
               in summary.detail, f"ledger.summary SKIPs its V term, got {summary}")
        expect("no freshness samples" in clean["progress"].detail,
               f"progress names no freshness samples, got {clean['progress'].detail}")
        for check_id in ("ledger.intake", "ledger.edge", "ledger.aggregate", "identity.sink"):
            expect(clean[check_id].status == checks.PASS,
                   f"{check_id} judges an external run as a local one, got {clean[check_id]}")

        # The final life's first drain after the stop (+135) is inside its fault window.
        inside = results("external-retry-inside", sut_extra=_datadog_lines(retry_at=(135,)))
        expect(inside["ledger.sent"].status == checks.PASS,
               f"a 5xx inside a fault window is the fault's retry, got {inside['ledger.sent']}")
        outside = results("external-retry-outside",
                          sut_extra=_datadog_lines(retry_at=(250, 260)))
        expect(outside["ledger.sent"].status == checks.WARN
               and "4 request(s) answered other than 2xx outside every fault window (5xx 4)"
               in outside["ledger.sent"].detail,
               f"5xx outside every fault window WARNs with the count, got "
               f"{outside['ledger.sent']}")

        rejection = (250, "WARN", "https://api.datadoghq.com/api/v2/series answered 400 Bad "
                     "Request, 100 record(s) dropped: {\"errors\":[\"Invalid\"]} (x1, further "
                     "occurrences suppressed)", {"component": "sink", "key": "request_rejected"})
        rejected = results("external-rejected", sut_extra=_datadog_lines(rejected_at=250),
                           stderr_extra=[rejection])
        row = rejected["ledger.sent"]
        expect(row.status == checks.FAIL and "status 400" in row.detail
               and "dropped rejected" in row.detail and "answered 400 Bad Request" in row.detail,
               f"a rejected batch FAILs with the status and the sink's text, got {row}")
        expect(rejected["drops"].status == checks.PASS,
               f"drops leaves a sink's rejected batch to ledger.sent's FAIL, got "
               f"{rejected['drops']}")
        corrupt_at = _stamp(T0 + 250)
        corrupt = results("external-disk-corrupt", sut_extra=_datadog_lines() + [
            _line(corrupt_at, "logit.component.batches.dropped", "sum", 1, component="sink",
                  attrs={"reason": "disk_corrupt"}),
            _line(corrupt_at, "logit.component.events.dropped", "sum", 40, component="sink",
                  attrs={"reason": "disk_corrupt"})])
        expect(corrupt["ledger.sent"].status == checks.FAIL
               and "1 batch(es) dropped from a corrupt disk spool" in corrupt["ledger.sent"].detail
               and corrupt["drops"].status == checks.PASS,
               f"on an external target ledger.sent FAILs a sink's disk_corrupt batch, and drops "
               f"leaves it and its events there, got {corrupt['ledger.sent']} {corrupt['drops']}")
        upstream = results("external-rejected-upstream", sut_extra=_datadog_lines() + [
            _line(corrupt_at, "logit.component.batches.dropped", "sum", 1, component="window",
                  attrs={"reason": "rejected"})])
        expect(upstream["drops"].status == checks.FAIL
               and upstream["drops"].lines == [
                   "window: logit.component.batches.dropped{reason=\"rejected\"} 1"],
               f"on an external target, where no ledger row FAILs it, drops FAILs a fault "
               f"drop at a node other than a sink, got {upstream['drops']}")
        for name, kwargs, needle in (
                ("external-rejected-records", dict(batch=False), "100 record(s) dropped rejected"),
                ("external-rejected-batch", dict(records=False), "1 batch(es) dropped rejected")):
            row = results(name, sut_extra=_datadog_lines(rejected_at=250, **kwargs),
                          stderr_extra=[rejection])["ledger.sent"]
            expect(row.status == checks.FAIL and needle in row.detail
                   and "status 400" in row.detail,
                   f"{needle} alone FAILs with the status, got {row}")
        unquoted = results("external-rejected-no-line",
                           sut_extra=_datadog_lines(rejected_at=250))
        expect(unquoted["ledger.sent"].status == checks.FAIL
               and "no request_rejected line on stderr" in
               unquoted["ledger.sent"].detail,
               f"a rejection with no stderr line still FAILs, got {unquoted['ledger.sent']}")
        stale = results("external-stale", sut_extra=_datadog_lines(stale=7, skipped=3,
                                                                    unsent=2))
        expect(stale["ledger.sent"].status == checks.WARN
               and "7 record(s) dropped stale" in " ".join(stale["ledger.sent"].lines)
               and "3 metric(s) skipped" in " ".join(stale["ledger.sent"].lines)
               and "2 metric(s) dropped by the encoder, never sent (non_finite_value 2)"
               in " ".join(stale["ledger.sent"].lines),
               f"a counted pre-send drop and an encoder skip WARN, got {stale['ledger.sent']}")
        expect(stale["drops"].status == checks.PASS,
               f"drops leaves an external sink's records.dropped and metrics.dropped to "
               f"ledger.sent, got {stale['drops']}")
        short = results("external-undelivered", sut_extra=_datadog_lines(), undelivered=1)
        expect(short["ledger.sent"].status == checks.FAIL
               and "outside [0, 1]" in short["ledger.sent"].detail,
               f"a batch neither delivered nor dropped FAILs, got {short['ledger.sent']}")
        refused = results("external-refused", sut_extra=_datadog_lines(), stderr_extra=[
            (250, "WARN", "Datadog refused the API key: https://api.datadoghq.com/api/v2/series "
             "answered 403 -- check 'api_key', and that 'site' is the one the key belongs to",
             {"component": "sink", "key": "api_key_rejected"})])["ledger.sent"]
        expect(refused.status == checks.FAIL and "1 api_key_rejected line(s)" in refused.detail
               and "status 403: api_key_rejected at" in refused.detail,
               f"a refused key FAILs quoting the line, got {refused}")
        refusal = results("external-request-refused", sut_extra=_datadog_lines(), stderr_extra=[
            (250, "WARN", "https://api.datadoghq.com/api/v2/series answered 401: unauthorized",
             {"component": "sink", "key": "request_refused"})])["ledger.sent"]
        expect(refusal.status == checks.FAIL and "1 request_refused line(s)" in refusal.detail
               and "status 401: request_refused at" in refusal.detail,
               f"a request_refused line FAILs quoting it, got {refusal}")
        queued = results("external-queued", sut_extra=_datadog_lines(queued_at=250),
                         buffer_batches=3)["ledger.sent"]
        expect(queued.status == checks.FAIL and "3 batch(es) left unsent" in queued.detail
               and "gap 0" in queued.detail,
               f"batches still queued at the final life's quiet drain FAIL, got {queued}")
        undrained = results("external-undrained", sut_extra=_datadog_lines(), timeline_extra=[
            _phase("sink_drained", 320, held=False, hold_s=20, polls=60, bound_s=120,
                   sinks={"sink": {"buffer_batches": 2, "retrying": 1}})])["ledger.sent"]
        expect(undrained.status == checks.FAIL and "held=false after 120s" in undrained.detail
               and "buffer.batches sink 2 left unsent" in undrained.detail,
               f"a sink_drained that never held FAILs with the count, got {undrained}")
        drained = results("external-drained", sut_extra=_datadog_lines(), timeline_extra=[
            _phase("sink_drained", 320, held=True, hold_s=20, polls=3, waited_s=24,
                   newest_drain=T0 + 320,
                   sinks={"sink": {"buffer_batches": 0, "retrying": 0}})])["ledger.sent"]
        expect(drained.status == checks.PASS and drained.detail.startswith("SENT"),
               f"a sink_drained that held reads SENT, got {drained}")
        no_2xx = results("external-no-2xx", sut_extra=_datadog_lines(cls="4xx"))["ledger.sent"]
        expect(no_2xx.status == checks.FAIL and "answered 2xx (4xx" in no_2xx.detail,
               f"no request answered 2xx FAILs, got {no_2xx}")
        oversize = (250, "WARN", "https://api.datadoghq.com/api/v2/series answered 413, request "
                    "too large, 100 record(s) dropped: too big",
                    {"component": "sink", "key": "request_rejected"})
        too_big = results("external-oversize", sut_extra=_datadog_lines(
            rejected_at=250, batch=False, reason="oversize"), stderr_extra=[oversize])
        expect(too_big["ledger.sent"].status == checks.FAIL
               and "100 record(s) dropped oversize" in too_big["ledger.sent"].detail
               and "status 413" in too_big["ledger.sent"].detail,
               f"records dropped oversize FAIL with the status, got {too_big['ledger.sent']}")
        series_202 = (240, "WARN", "https://api.datadoghq.com/api/v2/series accepted the request "
                      "but dropped 2 of 100 series; the first: too many tags",
                      {"component": "sink", "key": "series_rejected"})
        named = results("external-named", sut_extra=_datadog_lines(named=2),
                        stderr_extra=[series_202])["ledger.sent"]
        expect(named.status == checks.FAIL and "2 record(s) dropped rejected" in named.detail
               and "a 2xx whose body named dropped series: series_rejected" in named.detail
               and "no request_rejected" not in named.detail,
               f"records named in a 202 body FAIL quoting series_rejected, got {named}")
        both = results("external-202-then-400", sut_extra=_datadog_lines(rejected_at=250),
                       stderr_extra=[series_202, rejection])["ledger.sent"]
        expect(both.status == checks.FAIL and "status 400: request_rejected" in both.detail
               and "a 2xx whose body named dropped series: series_rejected"
               in " ".join(both.lines),
               f"a 400 and a 202 that both drop records are both quoted, got {both}")
        early_400 = (240,) + rejection[1:]
        late_202 = (250,) + series_202[1:]
        named_late = results("external-400-then-202", sut_extra=_datadog_lines(named=2),
                             stderr_extra=[early_400, late_202])["ledger.sent"]
        expect(named_late.status == checks.FAIL
               and "a 2xx whose body named dropped series: series_rejected" in named_late.detail
               and "status 400: request_rejected" in named_late.detail,
               f"named records quote series_rejected beside an earlier 400, got {named_late}")
        pre_send = (250, "WARN", "dropped one event on the series route: it encodes to 9000000 "
                    "bytes (800000 compressed), over the route's per-request limit",
                    {"component": "sink", "key": "oversize"})
        too_large = results("external-too-large", sut_extra=_datadog_lines(
            rejected_at=250, batch=False, reason="oversize"), stderr_extra=[pre_send])
        expect(too_large["ledger.sent"].status == checks.FAIL
               and "dropped before sending: oversize at" in too_large["ledger.sent"].detail
               and "it encodes to 9000000 bytes" in too_large["ledger.sent"].detail,
               f"a pre-send oversize drop quotes its line, got {too_large['ledger.sent']}")
        silent = results("external-no-requests")
        expect(silent["ledger.sent"].status == checks.FAIL
               and "no logit.output.requests" in silent["ledger.sent"].detail,
               f"no request telemetry FAILs, got {silent['ledger.sent']}")

        local = {r.id: r for r in checks.run_all(_ledger_run(tmp, "local-sent"))}
        expect(local["ledger.sent"].status == checks.SKIP
               and local["ledger.egress"].status == checks.PASS,
               f"a local run SKIPs ledger.sent and judges egress, got {local['ledger.sent']}")


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


def _kafka_base(root):
    path = root / "scenarios" / "kafka-broker-restart" / "scenario.toml"
    return path, tomllib.loads(path.read_text())


def _kafka_rules(root):
    """The `[target] kind = "kafka"` rules: `topic` required and legal, no `env`, no
    VictoriaMetrics ledger key, one sink with no backend query, a fault on `kafka` for a Kafka
    target only and none on `victoria-metrics` for one; the roles, profile, and image each kind
    of target gets; and compose.yaml's `kafka` service, the broker, in its own profile."""
    path, base = _kafka_base(root)
    vm_path, vm_base = _base(root)
    ext_path, ext_base = _external_base(root)

    def mutate(raw, edit):
        raw = copy.deepcopy(raw)
        edit(raw)
        return raw

    loaded = scenario.from_dict(copy.deepcopy(base), path)
    expect(loaded.kafka and not loaded.external and not loaded.has_vm
           and loaded.target == {"kind": "kafka", "name": "kafka", "env": [], "topic": "soak"},
           f"kafka-broker-restart loads as a Kafka target, got {loaded.target}")
    roles = scenario.base_roles(loaded)
    expect(roles == {"logit": "sut", "generator": "generator", "kafka": "broker"},
           f"a Kafka target's stack runs the broker kafka and no backend, got {roles}")
    expect(loaded.sinks == [{"id": "kafka"}],
           f"kafka-broker-restart's one sink names no backend, got {loaded.sinks}")
    expect(loaded.to_json()["target"]["topic"] == "soak",
           "the resolved scenario records the topic")
    local = scenario.from_dict(copy.deepcopy(vm_base), vm_path)
    external = scenario.from_dict(copy.deepcopy(ext_base), ext_path)
    expect(scenario.base_roles(local) == scenario.BASE_ROLES
           and scenario.base_roles(external) == {"logit": "sut", "generator": "generator"},
           f"the local stack runs victoria-metrics only, an external one neither, got "
           f"{scenario.base_roles(local)} {scenario.base_roles(external)}")
    problems = scenario.service_problems(loaded, scenario.base_roles(external))
    expect(any("reads its topic from the broker service `kafka`" in p for p in problems),
           f"a Kafka target on a stack with no broker is refused, got {problems}")
    expect("topic" not in local.to_json()["target"] and "topic" not in external.to_json()["target"],
           "only a Kafka target's resolved scenario has a topic")
    expect(driver.profiles_for(loaded) == ["kafka"] and driver.profiles_for(local) == ["local"]
           and driver.profiles_for(external) == [],
           "each target gets its own backend's compose profile")

    def table(**changes):
        return lambda raw: raw["target"].update(changes)

    cases = [
        (base, lambda raw: raw["target"].pop("topic"), "a kafka target needs `topic`",
         "a Kafka target with no topic"),
        (base, table(topic="has space"), "target.topic must be a legal Kafka topic name",
         "an illegal topic name"),
        (base, table(topic=".."), "target.topic must be a legal Kafka topic name",
         "the topic `..`"),
        (base, table(topic="x" * 250), "target.topic must be a legal Kafka topic name",
         "a 250-character topic"),
        (base, table(env=["DD_API_KEY"]), "target.env is for an external target",
         "env on a Kafka target"),
        (base, table(name="Kafka Two"), "target.name must be lowercase",
         "a Kafka target name"),
        (base, lambda raw: raw["ledger"].update(vm_every_window=True),
         "ledger.vm_every_window names a VictoriaMetrics query, and the kafka target",
         "vm_every_window on a Kafka target"),
        (base, lambda raw: raw["ledger"]["sink"][0].update(vm_selector="{x}"),
         "ledger.sink 1: `vm_selector` names a backend query, and the kafka target",
         "vm_selector on a Kafka target's sink"),
        (base, lambda raw: raw["ledger"]["sink"][0].update(backend="victoria-metrics"),
         "ledger.sink 1: `backend` names a backend query, and the kafka target",
         "a backend on a Kafka target's sink"),
        (base, lambda raw: raw["ledger"]["sink"].append({"id": "other"}),
         "a kafka target has one [[ledger.sink]]", "two sinks on a Kafka target"),
        (base, lambda raw: raw["step"].append({"at": "6m", "action": "stop",
                                               "on": "victoria-metrics", "for": "10s"}),
         "unknown service `victoria-metrics`; a kafka target's stack runs the broker",
         "a fault on victoria-metrics with a Kafka target"),
        (base, lambda raw: raw["step"][0].update(action="kill"), "kill is for logit only",
         "a kill on the broker"),
        (vm_base, lambda raw: raw["step"].append({"at": "12m", "action": "stop", "on": "kafka",
                                                  "for": "10s"}),
         "unknown service `kafka`", "a fault on kafka with the local target"),
        (ext_base, lambda raw: raw["step"].append({"at": "11m30s", "action": "stop",
                                                   "on": "kafka", "for": "10s"}),
         "unknown service `kafka`; an external target's stack runs no backend",
         "a fault on kafka with an external target"),
        (ext_base, table(topic="soak"), "target.topic is for a kafka target",
         "a topic on an external target"),
        (vm_base, lambda raw: raw.update(target={"kind": "victoria-metrics", "topic": "x"}),
         "target.topic is for an external or kafka target", "a topic on the local target"),
    ]
    paths = {id(base): path, id(vm_base): vm_path, id(ext_base): ext_path}
    for raw, edit, needle, what in cases:
        _refused(paths[id(raw)], mutate(raw, edit), needle, what)

    compose = (root / "compose.yaml").read_text()
    kafka_service = compose.split("\n  kafka:")[1].split("\n  logit:")[0]
    expect("profiles: [kafka]" in kafka_service and driver.KAFKA_IMAGE in kafka_service
           and 'restart: "no"' in kafka_service and "soak.role: broker" in kafka_service,
           "compose.yaml runs the pinned broker in the kafka profile, labeled broker, never "
           "restarted by docker")
    sut = compose.split("\n  logit:")[1].split("\n  generator:")[0]
    expect("kafka: { condition: service_healthy, required: false }" in sut,
           "the SUT waits for a healthy broker when its profile runs, and only then")
    for action in faults.ACTIONS.values():
        expect(action.affects_egress(scenario.Step(id="x", spec_index=0, cycle=0, action="stop",
                                                   on="kafka", args="", start=0, end=1), roles),
               "every fault on the broker affects the SUT's egress")


def _kafka_topic():
    """The topic read's parsers: end offsets, the consumer's count, record values to V per
    life, and what isn't a JSON object."""
    offsets = topic.end_offsets("soak:0:120\nsoak:1:7\nnoise\n")
    expect(offsets == {0: 120, 1: 7}, f"kafka-get-offsets lines parse, got {offsets}")
    expect(topic.end_offsets("Error: no such topic") is None,
           "an answer with no offset line is None")
    expect(topic.processed_count("[2026] WARN x\nProcessed a total of 42 messages\n") == 42
           and topic.processed_count("") is None, "the consumer's closing count parses")
    expect(topic.consumer_argv("soak", 9)[-2:] == ["--max-messages", "9"]
           and "--from-beginning" in topic.consumer_argv("soak"),
           "the consumer reads from the beginning, as many records as the offsets promise")
    stamp = topic.parse_time("2026-10-11T12:00:00.250000000Z")
    expect(stamp is not None and abs(stamp - 1791720000.25) < 1e-6,
           f"a nine-digit RFC 3339 time parses, got {stamp}")
    lines = [_topic_record(1000, 30), _topic_record(1000, 5, name="other"),
             _topic_record(2500, 70), "not json", "[1, 2]",
             json.dumps({"timestamp": _stamp(2600), "metrics": [
                 {"name": "x_0", "kind": "gauge", "value": 9}]})]
    records, unparsed, copies = topic.parse_records("\n".join(lines) + "\n\n")
    expect(len(records) == 4 and unparsed == 2 and copies == [],
           f"two of six lines aren't JSON objects, got {records} {unparsed} {copies}")
    prefixed, _, _ = topic.parse_records("\n".join(lines), prefix="x_")
    expect([amount for _, amount in prefixed] == [30, 0, 70, 0],
           f"only sums under the prefix count, got {prefixed}")
    by_life, total = topic.totals_by_life(records, [900, 2000])
    expect(by_life == {0: 35, 1: 70} and total == 105,
           f"each record goes to the life its timestamp falls in; a gauge adds 0, got "
           f"{by_life} {total}")
    resent = lines[:3] + [lines[2], lines[0], lines[2]]
    records, _, copies = topic.parse_records("\n".join(resent))
    by_life, total = topic.totals_by_life(records, [900, 2000])
    expect(len(records) == 3 and total == 105
           and topic.duplicates_by_life(copies, [900, 2000]) == {0: 1, 1: 2},
           f"a record's copies count once in V and per life as duplicates, got {records} "
           f"{copies}")


def _kafka_lines(accepted=4, ok=True):
    """`kafka_out` telemetry for the ledger fixture's SUT: one produce attempt per drain,
    answered `ok` (else `clean`), and `accepted` records over the run at +300."""
    drains = [o for o in range(5, STOP[0], 5)] + [o for o in range(135, 321, 5)]
    lines = [_line(_stamp(T0 + o), "logit.output.requests", "sum", 1, component="sink",
                   attrs={"class": "ok" if ok else "clean"}) for o in drains]
    lines.append(_line(_stamp(T0 + 300), "logit.output.records", "sum", accepted,
                       component="sink"))
    return lines


def _kafka_checks():
    """The checks over a Kafka two-life run: V from the topic's distinct records,
    `ledger.egress` and `ledger.summary` judging it, a surplus and a loss a duplicate offsets
    FAILing, a lost record FAILing, a topic not read back, `ledger.topic`'s verdicts on T − A and
    on duplicates with and without an epoch bump, and `ledger.sent` on `kafka_out`'s classes."""
    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            kwargs.setdefault("sut_extra", _kafka_lines())
            run_dir = _ledger_run(tmp, name, kafka=True, **kwargs)
            return {r.id: r for r in checks.run_all(run_dir)}

        clean = results("kafka-clean")
        for check_id in ("ledger.intake", "ledger.edge", "ledger.aggregate", "ledger.egress",
                         "ledger.summary", "ledger.topic", "identity.sink"):
            expect(clean[check_id].status == checks.PASS,
                   f"{check_id} PASSes a balanced Kafka run, got {clean[check_id]}")
        led = checks.Ledger(checks.RunData(Path(tmp) / "kafka-clean"))
        expect(led.V == {"sink": {0: led.get("Ab", 0) + 30, 1: led.get("Ab", 1)}}
               and led.kafka_sink == "sink" and not led.unparsed,
               f"V is the topic's increments per life, got {led.V}")
        sent = clean["ledger.sent"]
        expect(sent.status == checks.PASS and not sent.detail.startswith("SENT")
               and "read the topic back" in sent.detail and "ok 58" in sent.detail,
               f"ledger.sent judges kafka_out's ok class and doesn't end the evidence, got "
               f"{sent}")
        expect(clean["drops"].status == checks.PASS,
               f"drops PASSes a balanced Kafka run, got {clean['drops']}")
        faulted = results("kafka-fault-drops", life1_dropped=1, dropped_at="window",
                          dropped_reason="disk_corrupt")
        expect(faulted["ledger.egress"].status == checks.FAIL
               and faulted["drops"].status == checks.PASS,
               f"on a Kafka target ledger.egress FAILs a disk_corrupt batch drop and drops "
               f"leaves it there, got {faulted['ledger.egress']} {faulted['drops']}")
        expect(clean["ledger.windows"].status == checks.SKIP
               and checks.NO_VM_KAFKA in clean["ledger.windows"].detail,
               f"ledger.windows SKIPs on a Kafka target, got {clean['ledger.windows']}")
        expect(checks.NO_VM_KAFKA in clean["progress"].detail,
               f"progress names no freshness samples, got {clean['progress'].detail}")

        surplus = results("kafka-surplus", topic_extra=[_topic_record(T0 + 300, 25)])
        expect(surplus["ledger.egress"].status == checks.FAIL
               and "= -25, surplus" in surplus["ledger.egress"].detail,
               f"a distinct record's increments above Ab FAIL as a surplus, got "
               f"{surplus['ledger.egress']}")
        expect(surplus["ledger.summary"].status == checks.FAIL,
               f"ledger.summary judges a surplus, got {surplus['ledger.summary']}")
        expect(surplus["ledger.topic"].status == checks.FAIL
               and "T 5 distinct records" in surplus["ledger.topic"].detail
               and "never saw accepted" in surplus["ledger.topic"].detail,
               f"a distinct record the sink never saw accepted FAILs, got "
               f"{surplus['ledger.topic']}")

        # Life 1 loses 7 and stores a 7 record twice: the raw sum equals Ab, the distinct V
        # doesn't.
        offset = results("kafka-offset", v1_short=7, sut_extra=_kafka_lines(accepted=5),
                         topic_records=lambda v0, v1: (
                             (50, v0 // 2), (100, v0 - v0 // 2), (200, v1 // 2), (250, 7),
                             (250, 7), (300, v1 - v1 // 2 - 7)))
        expect(offset["ledger.egress"].status == checks.FAIL
               and "= 7, uncounted" in offset["ledger.egress"].detail
               and "6 topic record(s), 5 distinct, 1 duplicate copies" in "\n".join(
                   offset["ledger.egress"].lines),
               f"a loss a duplicate of the same size offsets FAILs, got "
               f"{offset['ledger.egress']}")
        expect(offset["ledger.summary"].status == checks.FAIL,
               f"ledger.summary FAILs the offset loss, got {offset['ledger.summary']}")

        def resent(copy_offset):
            def shares(v0, v1):
                base = [(50, v0 // 2), (100, v0 - v0 // 2), (200, v1 // 2), (300, v1 - v1 // 2)]
                return base + [next(r for r in base if r[0] == copy_offset)]
            return shares

        clean_dup = results("kafka-dup-clean", topic_records=resent(300))
        expect(clean_dup["ledger.egress"].status == checks.PASS,
               f"V counts a duplicated record once, got {clean_dup['ledger.egress']}")
        expect(clean_dup["ledger.topic"].status == checks.FAIL
               and "T 4 distinct records in the topic (5 read, 1 duplicate copies) vs A 4"
               in clean_dup["ledger.topic"].detail
               and "life 1: 1 duplicate copies with no epoch bump"
               in clean_dup["ledger.topic"].detail,
               f"a duplicate in a life with no epoch bump FAILs, got {clean_dup['ledger.topic']}")

        bumped = results("kafka-dup-bumped", topic_records=resent(300),
                         sut_extra=_kafka_lines() + [_line(
                             _stamp(T0 + 250), "logit.output.records.dropped", "sum", 3,
                             component="sink", attrs={"reason": "rejected"})])
        expect(bumped["ledger.topic"].status == checks.WARN
               and "3 record(s) dropped rejected" in bumped["ledger.topic"].detail,
               f"a duplicate after a rejection's epoch bump WARNs, got {bumped['ledger.topic']}")

        restarted = results("kafka-dup-restart", topic_records=resent(100))
        expect(restarted["ledger.topic"].status == checks.WARN
               and "a later life resent under a new producer id"
               in restarted["ledger.topic"].detail,
               f"a duplicate in a life a restart ended WARNs, got {restarted['ledger.topic']}")

        lost = results("kafka-lost", v1_short=7, sut_extra=_kafka_lines(accepted=5))
        expect(lost["ledger.egress"].status == checks.FAIL
               and "= 7, uncounted" in lost["ledger.egress"].detail,
               f"increments missing from the topic FAIL, got {lost['ledger.egress']}")
        expect(lost["ledger.summary"].status == checks.FAIL,
               f"ledger.summary FAILs a missing increment, got {lost['ledger.summary']}")
        expect(lost["ledger.topic"].status == checks.FAIL
               and "acknowledged records it no longer holds" in lost["ledger.topic"].detail,
               f"a topic below the accepted count FAILs, got {lost['ledger.topic']}")

        kill = [_action("apply", "k0", "kill", "logit", 100, 100.1)]
        killed_over = results("kafka-killed-over", topic_extra=[_topic_record(T0 + 300, 25)],
                              timeline_extra=kill)["ledger.topic"]
        expect(killed_over.status == checks.WARN and "killed SUT life 0" in killed_over.detail,
               f"T above A across a killed life WARNs naming it, got {killed_over}")
        killed_under = results("kafka-killed-under", v1_short=7, sut_extra=_kafka_lines(accepted=5),
                               timeline_extra=kill)["ledger.topic"]
        expect(killed_under.status == checks.WARN and "killed SUT life 0" in killed_under.detail,
               f"T below A across a killed life WARNs naming it, got {killed_under}")

        garbled = results("kafka-garbled", topic_extra=["not json"])
        expect(garbled["ledger.egress"].status == checks.FAIL
               and "1 topic record(s) aren't JSON objects" in garbled["ledger.egress"].detail,
               f"a record that isn't a JSON object FAILs, got {garbled['ledger.egress']}")

        unread = results("kafka-unread", topic_read=False)
        for check_id in ("ledger.egress", "ledger.topic"):
            expect(unread[check_id].status == checks.FAIL
                   and "broker down" in unread[check_id].detail,
                   f"{check_id} FAILs a topic never read back, quoting why, got "
                   f"{unread[check_id]}")

        no_ok = results("kafka-no-ok", sut_extra=_kafka_lines(ok=False))
        expect(no_ok["ledger.sent"].status == checks.FAIL
               and "answered ok (clean 58)" in no_ok["ledger.sent"].detail,
               f"no attempt answered ok FAILs, got {no_ok['ledger.sent']}")

        local = {r.id: r for r in checks.run_all(_ledger_run(tmp, "kafka-local"))}
        expect(local["ledger.topic"].status == checks.SKIP,
               f"ledger.topic SKIPs on the local target, got {local['ledger.topic']}")


# The second source the fan-in fixtures add: a generator service and the SUT listener it reaches.
LINES_SOURCE = {"service": "generator-lines", "generator_input": "load",
                "generator_sink": "out", "sut_listener": "lines"}


def _sources_run(tmp, name, sut_edit=None, gen_edit=None, transforms=(), sut_config=None,
                 timeline_extra=(), **kwargs):
    """`_ledger_run`'s two-life run directory with a second source (`LINES_SOURCE`): the
    generator `generator-lines` repeats `generator`'s telemetry, the SUT listener `lines`
    repeats `in`'s lines (its stderr shutdown lines included), and `window`'s received and
    absorbed counts and the export's values double, so every identity holds over the two.

    `sut_edit(record)` may change or drop (return None) each of `lines`' telemetry records, and
    `gen_edit(record)` each of `generator-lines`'. `transforms` names nodes between `lines` and
    `window`, each forwarding what `lines` sent, written as that source's `sut_transforms`;
    `sut_config` is the SUT config text the run's `configs/` holds."""
    run_dir = _ledger_run(tmp, name, timeline_extra=timeline_extra, **kwargs)
    out = []
    for line in (run_dir / "logs" / "logit.stdout").read_text().splitlines():
        record = json.loads(line)
        component = record["attributes"]["component"]
        metric = record["metrics"][0]
        if component == "window" and metric["name"] in ("logit.component.events.received",
                                                        "logit.transform.metrics.absorbed"):
            metric["value"] *= 2
            out.append(json.dumps(record))
            continue
        out.append(line)
        if component != "in":
            continue
        copied = copy.deepcopy(record)
        copied["attributes"]["component"] = "lines"
        if metric["name"] == "logit.component.events.sent":
            for node in transforms:
                for name_ in ("logit.component.events.received", "logit.component.events.sent"):
                    hop = copy.deepcopy(copied)
                    hop["attributes"]["component"] = node
                    hop["metrics"][0]["name"] = name_
                    hop = sut_edit(hop) if sut_edit else hop
                    if hop is not None:
                        out.append(json.dumps(hop))
        copied = sut_edit(copied) if sut_edit else copied
        if copied is not None:
            out.append(json.dumps(copied))
    (run_dir / "logs" / "logit.stdout").write_text("\n".join(out) + "\n")
    stderr = []
    for line in (run_dir / "logs" / "logit.stderr").read_text().splitlines():
        stderr.append(line)
        entry = json.loads(line)
        if entry.get("component") == "in":
            stderr.append(json.dumps({**entry, "component": "lines"}))
    (run_dir / "logs" / "logit.stderr").write_text("\n".join(stderr) + "\n")
    gen = []
    for line in (run_dir / "logs" / "generator.stdout").read_text().splitlines():
        record = json.loads(line)
        record = gen_edit(record) if gen_edit else record
        if record is not None:
            gen.append(json.dumps(record))
    (run_dir / "logs" / "generator-lines.stdout").write_text("\n".join(gen) + "\n")
    series = vm.parse_export((run_dir / "vm-export" / "sink.jsonl").read_text())
    for one in series:
        one["values"] = [value * 2 for value in one["values"]]
    _write_export(run_dir, "sink", series)
    resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
    resolved["services"] = {**resolved["services"], "generator-lines": "generator"}
    second = dict(LINES_SOURCE, **({"sut_transforms": list(transforms)} if transforms else {}))
    resolved["ledger"]["source"] = list(resolved["ledger"]["source"]) + [second]
    if sut_config is not None:
        (run_dir / "configs").mkdir(exist_ok=True)
        (run_dir / "configs" / "sut.yaml").write_text(sut_config)
        resolved["configs"] = {"sut": "sut.yaml"}
    (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
    return run_dir


def _at(offset, change):
    """A `sut_edit`/`gen_edit` applying `change(record)` to the records stamped at `offset`
    seconds and keeping the rest."""
    def edit(record):
        if record["timestamp"] == _stamp(T0 + offset):
            change(record)
        return record
    return edit


def _sources_ledger():
    """The ledger with two sources: `ledger.wire` judged per source against the fault windows
    of its own generator and of everything off the generators, `ledger.intake` per listener,
    `ledger.edge` over the listeners' summed E and through a source's `sut_transforms`, R
    summed per listener from each listener's own `receive:`, and `recovery` against the summed
    rate."""

    def less(metric, by, component="lines"):
        def change(record):
            if (record["metrics"][0]["name"] == metric
                    and record["attributes"]["component"] == component):
                record["metrics"][0]["value"] -= by
        return change

    def netem(on):
        return (_action("apply", "x1", "netem", on, 52, affects_udp_ingress=True),
                _action("revert", "x1", "netem", on, 62, affects_udp_ingress=True))

    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            return {r.id: r for r in checks.run_all(_sources_run(tmp, name, **kwargs))}

        clean = results("sources-clean")
        for row in ("ledger.wire", "ledger.intake", "ledger.edge", "ledger.aggregate",
                    "ledger.egress", "ledger.summary", "recovery", "identity.sink"):
            expect(clean[row].status == checks.PASS,
                   f"{row} PASSes the balanced two-source fixture, got {clean[row]}")
        wire = clean["ledger.wire"].detail
        expect("generator -> in: G 60,000" in wire and "generator-lines -> lines: G 60,000" in wire,
               f"ledger.wire reports one detail per source, got {wire}")
        intake = clean["ledger.intake"].detail
        expect("final life 1 in: W − D" in intake and "final life 1 lines: W − D" in intake,
               f"ledger.intake judges each listener, got {intake}")
        expect("ΣE 68,000 == A 68,000" in clean["ledger.edge"].detail,
               f"ledger.edge sums E over the two listeners, got {clean['ledger.edge']}")
        expect("R 89,000 = 10,000 + 2 x 1,000 (in) + 10,000 + 2 x 1,000 (lines) + 65 x 1,000"
               in clean["ledger.intake"].detail,
               f"R holds each listener's queue and batches and aggregate's once, got "
               f"{clean['ledger.intake'].detail}")

        # Lines lost on the second source's wire at +60: inside a netem on its own generator,
        # by design; inside a netem on the other generator, steady loss for this source.
        lost = _at(60, less("logit.input.datagrams", 400))
        own = results("sources-wire-own", sut_edit=lost, timeline_extra=netem("generator-lines"))
        expect(own["ledger.wire"].status == checks.PASS,
               f"wire loss inside the source's own generator's fault window passes, got "
               f"{own['ledger.wire']}")
        other = results("sources-wire-other", sut_edit=lost, timeline_extra=netem("generator"))
        detail = other["ledger.wire"].detail
        expect(other["ledger.wire"].status == checks.FAIL
               and re.search(r"generator-lines -> lines: \d+ lost outside UDP-affecting windows",
                             detail)
               and "generator -> in: G 60,000" in detail,
               f"the same loss inside the other generator's fault window FAILs this source "
               f"alone, got {detail}")
        sut_fault = results("sources-wire-sut", sut_edit=lost, timeline_extra=netem("logit"))
        expect(sut_fault["ledger.wire"].status == checks.PASS,
               f"a UDP-affecting fault on the SUT excuses every source's wire, got "
               f"{sut_fault['ledger.wire']}")
        datagrams = results("sources-wire-datagrams", gen_edit=_at(100, less(
            "logit.output.datagrams", 1, component="out")))
        expect(datagrams["ledger.wire"].status == checks.FAIL
               and "generator-lines -> lines: precondition" in datagrams["ledger.wire"].detail
               and "generator -> in: precondition" not in datagrams["ledger.wire"].detail,
               f"each source's messages == datagrams precondition is its own, got "
               f"{datagrams['ledger.wire']}")

        # The second listener sends 5 events fewer than it read in the final life.
        short = results("sources-intake", sut_edit=_at(200, less(
            "logit.component.events.sent", 5)))
        detail = short["ledger.intake"].detail
        expect(short["ledger.intake"].status == checks.FAIL
               and detail.startswith("final life 1 lines: W − D 34,000 vs E + B 33,995, off "
                                     "by 5")
               and "final life 1 in: W − D 34,000 vs E + B 34,000" in detail,
               f"an imbalance on one listener FAILs naming it, got {detail}")
        expect(short["ledger.edge"].status == checks.FAIL
               and "ΣE 67,995 != A 68,000" in short["ledger.edge"].detail,
               f"the listeners' summed E short of A FAILs the edge, got {short['ledger.edge']}")
        oversize = results("sources-intake-oversize", sut_edit=_at(200, less(
            "logit.component.events.sent", 5)), sut_extra=[_line(
                _stamp(T0 + 200), "logit.input.frames.dropped", "sum", 5, component="lines",
                attrs={"reason": "oversize"})])
        expect(oversize["ledger.intake"].status == checks.PASS,
               f"lines_in's oversize drops are B, got {oversize['ledger.intake']}")

        # Transforms between `lines` and `window`: each forwards every event, else the edge
        # FAILs at the hop.
        chained = results("sources-transforms", transforms=("parse", "count"))
        expect(chained["ledger.edge"].status == checks.PASS
               and "T_in[count] 34,000 == T_out[count] 34,000 (count forwards every event)"
               in chained["ledger.edge"].detail,
               f"ledger.edge walks a source's transforms, got {chained['ledger.edge']}")
        expect("10,000 + 132 x 1,000 (lines)" in chained["ledger.intake"].detail,
               f"each transform adds an inbox and a batch to R, got "
               f"{chained['ledger.intake'].detail}")
        dropped = results("sources-transforms-drop", transforms=("parse", "count"),
                          sut_edit=lambda record: (_at(200, less(
                              "logit.component.events.sent", 3, component="count"))(record)))
        detail = dropped["ledger.edge"].detail
        expect(dropped["ledger.edge"].status == checks.FAIL
               and "T_in[count] 34,000 != T_out[count] 33,997" in detail,
               f"a transform that drops events FAILs the edge at its hop, got {detail}")

        # Each listener's own `receive:` sizes its share of R.
        sized = results("sources-r", sut_config=(
            "components:\n  in: { type: statsd_in, bind: 0.0.0.0:8125 }\n"
            "  lines:\n    type: lines_in\n    receive: { max_datagrams: 500, "
            "batch_max_events: 100 }\n"))
        expect("R 77,700 = 10,000 + 2 x 1,000 (in) + 500 + 2 x 100 (lines) + 65 x 1,000"
               in sized["ledger.intake"].detail,
               f"R reads each listener's receive block, got {sized['ledger.intake'].detail}")

        # Recovery sums the listeners' ingest against the generators' summed rate: one source
        # at half its rate after the stop is 75% of the whole.
        def half(record):
            stamp = record["timestamp"]
            if (record["metrics"][0]["name"] == "logit.input.datagrams"
                    and _stamp(T0 + 135) <= stamp <= _stamp(T0 + 180)):
                record["metrics"][0]["value"] //= 2
            return record
        slow = results("sources-recovery", sut_edit=half)
        expect(slow["recovery"].status == checks.FAIL
               and "is 75% of the warmup rate" in slow["recovery"].detail,
               f"recovery judges the summed ingest against the summed rate, got "
               f"{slow['recovery']}")


def _source_rules(root):
    """`[[ledger.source]]` and `[configs]` per generator: every rule `load()` holds them to, the
    rules that need the role map, and each generator's compose variable."""
    path, raw = _base(root)

    def mutate(edit):
        changed = copy.deepcopy(raw)
        edit(changed)
        return changed

    lines = dict(LINES_SOURCE)

    def second(r, **changes):
        r["ledger"]["source"].append({**lines, **changes})
        r["configs"]["generator-lines"] = "logit-generator.yaml"

    _refused(path, mutate(lambda r: r["ledger"].pop("source")), "ledger: missing `source`",
             "no [[ledger.source]]")
    _refused(path, mutate(lambda r: r["ledger"].update(source=[])),
             "ledger.source must be one or more tables", "an empty [[ledger.source]] list")
    for key in ("generator_input", "generator_sink", "sut_listener"):
        _refused(path, mutate(lambda r, key=key: r["ledger"].update({key: "statsd"})),
                 f"ledger.{key} is a [[ledger.source]] key", f"the old scalar `{key}`")
        _refused(path, mutate(lambda r, key=key: r["ledger"]["source"][0].pop(key)),
                 f"ledger.source 1: missing `{key}`", f"a source without `{key}`")
    _refused(path, mutate(lambda r: r["ledger"]["source"][0].pop("service")),
             "ledger.source 1: missing `service`", "a source without `service`")
    _refused(path, mutate(lambda r: r["ledger"]["source"][0].update(rate=1)),
             "ledger.source 1: unknown key `rate`", "an unknown source key")
    _refused(path, mutate(lambda r: second(r, service="generator")),
             "ledger.source 2: `service` 'generator' repeats another source's",
             "two sources on one generator")
    _refused(path, mutate(lambda r: second(r, sut_listener="statsd")),
             "ledger.source 2: `sut_listener` 'statsd' repeats another source's",
             "two sources into one listener")
    _refused(path, mutate(lambda r: second(r, service="Lines!")),
             "`service` must name a generator service", "a service that isn't a slug")
    _refused(path, mutate(lambda r: r["ledger"]["source"].append(dict(lines))),
             "configs: missing `generator-lines`", "a source whose generator has no config")
    _refused(path, mutate(lambda r: r["configs"].update(
        {"generator-lines": "logit-generator.yaml"})),
             "configs: SUT services logit and generator-lines need a [[ledger.hop]]",
             "a config for no source, read as a further SUT")
    _refused(path, mutate(lambda r: second(r, sut_transforms=["parse", "lines"])),
             "`sut_transforms` repeats a component or names a listener",
             "a transform that is a listener")
    _refused(path, mutate(lambda r: (r["ledger"]["source"][0].update(sut_transforms=["parse"]),
                                     second(r, sut_transforms=["count", "parse"]))),
             "ledger.source 2: `sut_transforms` names parse, which another source's chain "
             "lists", "one transform on two sources' chains")
    _refused(path, mutate(lambda r: (second(r, service="generator_lines"),
                                     r["ledger"]["source"].append(dict(lines)),
                                     r["configs"].update(
                                         {"generator_lines": "logit-generator.yaml"}))),
             "ledger.source 3: `service` 'generator-lines' would mount its config through "
             "SOAK_GENERATOR_LINES_CONFIG", "two services sharing one compose variable")
    _refused(path, mutate(lambda r: (second(r, service="sut"),
                                     r["configs"].pop("generator-lines"))),
             "`service` 'sut' would mount its config through SOAK_SUT_CONFIG",
             "a generator whose variable is the SUT's")
    _refused(path, mutate(lambda r: second(r, sut_transforms="parse")),
             "`sut_transforms` must list the component ids", "a transform list that isn't one")
    _refused(path, mutate(lambda r: (second(r, sut_transforms=["parse"]),
                                     r["ledger"].update(sut_script="lua"))),
             "`sut_transforms` and [ledger] sut_script can't be combined",
             "transforms beside a script")
    def moved(r, service):
        r["ledger"]["source"][0]["service"] = service
        r["configs"][service] = r["configs"].pop("generator")

    _refused(path, mutate(lambda r: moved(r, "logit")),
             "ledger.source 1: service `logit` isn't a generator service", "a source on the SUT")
    _refused(path, mutate(lambda r: moved(r, "victoria-metrics")),
             "service `victoria-metrics` isn't a generator service", "a source on a backend")
    _refused(path, raw, "generator service generator-lines has no [[ledger.source]]",
             "a generator with no source",
             roles={**scenario.BASE_ROLES, "generator-lines": "generator"})

    for service, name in (("generator", "SOAK_GENERATOR_CONFIG"),
                          ("generator-lines", "SOAK_GENERATOR_LINES_CONFIG")):
        expect(scenario.config_env(service) == name,
               f"{service}'s config is in {name}, got {scenario.config_env(service)}")

    fanin = scenario.load(root / "scenarios" / "fanin" / "scenario.toml")
    roles = _override_roles(fanin)
    expect(roles == {**scenario.BASE_ROLES, "generator-lines": "generator"}
           and scenario.service_problems(fanin, roles) == [],
           f"fanin's override adds a second generator its sources name, got {roles}")
    expect(fanin.generators == ["generator", "generator-lines"]
           and fanin.config_path("generator-lines").name == "logit-generator-lines.yaml",
           f"fanin names each generator's config, got {fanin.generators}")
    problems = scenario.service_problems(fanin, scenario.BASE_ROLES)
    expect(any("service `generator-lines` isn't a generator service" in p for p in problems),
           f"without the override, fanin's second source is refused, got {problems}")
    base_text = (root / "compose.yaml").read_text()
    override = fanin.compose_path().read_text()
    expect("${SOAK_GENERATOR_LINES_CONFIG:?" in override
           and "${SOAK_GENERATOR_CONFIG:?" in base_text,
           "each generator's service mounts the config its compose variable names")

    # Fault predicates and per-source windows: a fault on one generator affects that source's
    # wire alone; one on the SUT affects both.
    data = type("Data", (), {"services": roles})()
    for on, generator, lines_ in (("generator", True, False), ("generator-lines", False, True),
                                  ("logit", True, True)):
        fault = checks.Fault("x", "netem" if on != "logit" else "stop", on, 0, 1,
                             affects_udp_ingress=True, affects_egress=False)
        got = (checks.affects_source(data, fault, "generator"),
               checks.affects_source(data, fault, "generator-lines"))
        expect(got == (generator, lines_),
               f"a fault on {on} affects (generator, generator-lines) wires {got}")
    step = scenario.Step("c0s1", 0, 0, "netem", "generator-lines", "loss 1%", 0, 1)
    expect(faults.ACTIONS["netem"].affects_udp_ingress(step, roles),
           "a netem on a second generator affects UDP ingress")
    _refused(path, mutate(lambda r: r["step"].append(
        {"at": "11m10s", "action": "kill", "on": "generator-lines", "for": "20s"})),
             "kill is for logit only", "a kill on a second generator",
             roles={**scenario.BASE_ROLES, "generator-lines": "generator"})


def _fanin_driver(root):
    """The driver with two generators: `compose.env` sets each generator's config variable,
    and the end sequence stops every generator before it waits on the sinks."""
    fanin = scenario.load(root / "scenarios" / "fanin" / "scenario.toml")
    roles = _override_roles(fanin)
    with tempfile.TemporaryDirectory() as tmp:
        run = driver.Run(root, fanin, fanin.duration, None, False, Path(tmp) / "fanin", [],
                         "logit:soak", roles=roles)
        run.prepare()
        env = (run.run_dir / "compose.env").read_text()
        expect(f"SOAK_GENERATOR_CONFIG={fanin.config_path('generator')}\n" in env
               and f"SOAK_GENERATOR_LINES_CONFIG={fanin.config_path('generator-lines')}\n"
               in env and (run.run_dir / "configs" / "logit-generator-lines.yaml").is_file(),
               f"compose.env sets each generator's config and the run copies both, got {env}")
        for jsonl in (run.timeline, run.watchdog, run.stats, run.freshness, run.capture):
            jsonl.close()

    class Stops:
        def __init__(self):
            self.calls = []

        def run(self, args, timeout=None):
            self.calls.append(args)
            return docker.Result(0, "", "")

        def inspect(self, container, timeout=None):
            return {"State": {"ExitCode": 0}}

    run = driver.Run(root, fanin, fanin.duration, None, False, "/nonexistent", [], "logit:soak",
                     roles=roles)
    run.docker = Stops()
    run.ids = {name: f"id-{name}" for name in roles}
    run.timeline = _Records()
    run.t0 = 0.0
    run.has_vm = False
    run.inspect_all = lambda: None
    run.wait_sink_drained = lambda *args, **kwargs: None
    real_print = driver._print
    driver._print = lambda text, stream=None: None
    try:
        run.end_sequence()
    finally:
        driver._print = real_print
    stopped = [call[-1] for call in run.docker.calls if call[0] == "stop"]
    expect(stopped == ["id-generator", "id-generator-lines", "id-logit"],
           f"the end sequence stops each generator, then the SUT, got {stopped}")


# A second SUT service, `collector`, after `logit` in the chain: the role map an override adding
# it would give.
TWO_SUT_ROLES = {**scenario.BASE_ROLES, "collector": "sut"}


def _tcp_run(tmp, name, sut_edit=None, gen_edit=None, datagrams=False, wire="tcp",
             timeline_extra=(), **kwargs):
    """`_ledger_run`'s two-life run directory with its one source on a TCP wire: the listener
    counts `logit.input.frames` in place of datagrams and no datagram drop, so life 0's
    residual is the 30 that reach V after its last drain, the generator counts
    no `output.datagrams` unless `datagrams`, and the stop over `STOP` is TCP-affecting, as the
    driver records it. `sut_edit(record)` and `gen_edit(record)` may change or drop (return
    None) each SUT and generator telemetry record. `wire` is the source's `wire`, or None for a
    source that names none."""
    kwargs.setdefault("life0_extra", 30)
    run_dir = _ledger_run(tmp, name, shutdown_drops=0, timeline_extra=timeline_extra, **kwargs)
    out = []
    for line in (run_dir / "logs" / "logit.stdout").read_text().splitlines():
        record = json.loads(line)
        metric = record["metrics"][0]
        if metric["name"] == "logit.input.datagrams":
            metric["name"] = "logit.input.frames"
        record = sut_edit(record) if sut_edit else record
        out += [json.dumps(one) for one in (record if isinstance(record, list) else [record])
                if one is not None]
    (run_dir / "logs" / "logit.stdout").write_text("\n".join(out) + "\n")
    gen = []
    for line in (run_dir / "logs" / "generator.stdout").read_text().splitlines():
        record = json.loads(line)
        if record["metrics"][0]["name"] == "logit.output.datagrams" and not datagrams:
            continue
        record = gen_edit(record) if gen_edit else record
        gen += [json.dumps(one) for one in (record if isinstance(record, list) else [record])
                if one is not None]
    (run_dir / "logs" / "generator.stdout").write_text("\n".join(gen) + "\n")
    timeline = []
    for line in (run_dir / "timeline.jsonl").read_text().splitlines():
        record = json.loads(line)
        if record.get("step") == "c0s1":
            record["affects_tcp_ingress"] = True
        timeline.append(json.dumps(record))
    (run_dir / "timeline.jsonl").write_text("\n".join(timeline) + "\n")
    resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
    source = dict(resolved["ledger"]["source"][0])
    if wire is None:
        source.pop("wire", None)
    else:
        source["wire"] = wire
    resolved["ledger"]["source"] = [source]
    (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
    return run_dir


def _fault_pair(step, action, on, start, end, udp=False, tcp=False, **extra):
    """An apply and a revert of `action` on `on`, recorded with its per-wire ingress flags;
    `extra` goes on the apply, and a partition's `ips_after` on the revert."""
    after = {"ips_after": extra.pop("ips_after")} if "ips_after" in extra else {}
    flags = {"affects_udp_ingress": udp, "affects_tcp_ingress": tcp,
             "affects_file_ingress": False}
    return (_action("apply", step, action, on, start, **flags, **extra),
            _action("revert", step, action, on, end - 1, end, **flags, **after))


def _tcp_ledger():
    """The ledger on a TCP source: W from `input.frames`, F from the framer's counted drops,
    the `output.datagrams` precondition reversed, R with no receive queue, the wire judged
    through netem on the generator and reported through a stop, a judged window's surplus
    against its ambiguous drops, a partition judged or reported by its address, and `recovery`
    reading the frames; and a source naming `wire = "udp"` scored as one naming none."""

    def ambiguous(offset, batches):
        stamp = _stamp(T0 + offset)

        def edit(record):
            if (record["metrics"][0]["name"] == "logit.component.events.sent"
                    and record["timestamp"] == stamp):
                return [record, json.loads(_line(
                    stamp, "logit.component.batches.dropped", "sum", batches, component="out",
                    attrs={"reason": "ambiguous_at_most_once"}))]
            return record
        return edit

    def flat(edit):
        """A `gen_edit` that may return a list of records, written one per line."""
        def wrapped(record):
            return edit(record)
        return wrapped

    with tempfile.TemporaryDirectory() as tmp:
        def results(name, **kwargs):
            return {r.id: r for r in checks.run_all(_tcp_run(tmp, name, **kwargs))}

        clean = results("tcp-clean")
        for row in ("ledger.wire", "ledger.intake", "ledger.edge", "ledger.egress",
                    "ledger.summary", "recovery", "drops"):
            expect(clean[row].status == checks.PASS,
                   f"{row} PASSes the balanced TCP fixture, got {clean[row]}")
        wire = clean["ledger.wire"].detail
        expect(wire.startswith("G 60,000 = W 54,030 + K 0 + counted 0 + gap 5,970 (6,000 in "
                               "reported windows, 0 in judged TCP-affecting windows, -30 "
                               "steady"),
               f"a TCP source's W is its listener's input.frames, and a stop's loss is "
               f"reported, got {wire}")
        expect(any("fault (c0s1 stop on logit)" in line for line in clean["ledger.wire"].lines)
               and any("steady (no TCP-affecting fault)" in line
                       for line in clean["ledger.wire"].lines),
               f"a TCP source's windows are TCP-affecting, got {clean['ledger.wire'].lines}")
        expect("R 67,000 = 0 + 2 x 1,000 + 65 x 1,000" in clean["ledger.intake"].detail,
               f"a TCP listener has no receive queue in R, got {clean['ledger.intake'].detail}")
        expect("F 0" in clean["ledger.summary"].detail
               and "wire G − (W + K + F) 5,970 over the run, F 0 counted"
               in clean["ledger.summary"].detail,
               f"ledger.summary shows F on a TCP source, got {clean['ledger.summary'].detail}")

        # The generator's sink counted datagrams: it isn't sending over TCP.
        udp_sink = results("tcp-datagrams", datagrams=True)
        expect(udp_sink["ledger.wire"].status == checks.FAIL
               and "precondition: generator output.datagrams 60,000 isn't 0"
               in udp_sink["ledger.wire"].detail,
               f"a TCP source FAILs a generator counting datagrams, got {udp_sink['ledger.wire']}")

        # 400 more lines sent at +60 that the framer dropped are counted wire loss, F, not
        # steady loss; the same lines with no count are lost.
        def sent_more(record):
            if (record["metrics"][0]["name"] == "logit.output.messages"
                    and record["timestamp"] == _stamp(T0 + 60)):
                record["metrics"][0]["value"] += 400
            return record

        def truncated(record):
            if (record["metrics"][0]["name"] == "logit.input.frames"
                    and record["timestamp"] == _stamp(T0 + 60)):
                return [record, json.loads(_line(
                    record["timestamp"], "logit.input.frames.dropped", "sum", 400,
                    component="in", attrs={"reason": "truncated"}))]
            return record

        framed = results("tcp-truncated", sut_edit=truncated, gen_edit=sent_more)
        expect(framed["ledger.wire"].status == checks.PASS
               and "counted 400 (truncated 400)" in framed["ledger.wire"].detail,
               f"a framer's truncated drop is counted wire loss, got {framed['ledger.wire']}")
        expect(framed["drops"].status == checks.PASS
               and framed["ledger.intake"].status == checks.PASS,
               f"F stays out of drops and of the intake balance, got {framed['drops']}, "
               f"{framed['ledger.intake']}")
        lost = results("tcp-lost", gen_edit=sent_more)
        expect(lost["ledger.wire"].status == checks.FAIL
               and lost["ledger.wire"].detail.startswith(
                   "370 lost outside reported TCP windows"),
               f"lines lost uncounted on a TCP wire FAIL, got {lost['ledger.wire']}")

        # Netem on the generator: UDP loss by design, but TCP retransmits, so it's judged.
        gen_netem = _fault_pair("x1", "netem", "generator", 52, 62, udp=True)
        on_udp = {r.id: r for r in checks.run_all(_ledger_run(
            tmp, "udp-gen-netem", steady_loss=400, timeline_extra=gen_netem))}
        expect(on_udp["ledger.wire"].status == checks.PASS,
               f"UDP loss inside a generator netem passes, got {on_udp['ledger.wire']}")
        on_tcp = results("tcp-gen-netem", steady_loss=400, timeline_extra=gen_netem)
        expect(on_tcp["ledger.wire"].status == checks.FAIL
               and on_tcp["ledger.wire"].detail.startswith("370 lost outside reported TCP"),
               f"TCP loss inside a generator netem FAILs, got {on_tcp['ledger.wire']}")

        # Netem on the SUT carries the ACKs: TCP-affecting and judged, never UDP-affecting.
        sut_netem = _fault_pair("x1", "netem", "logit", 52, 62, tcp=True)
        judged = results("tcp-sut-netem", steady_loss=1000, timeline_extra=sut_netem)
        lines = judged["ledger.wire"].lines
        expect(judged["ledger.wire"].status == checks.FAIL
               and any("judged (x1 netem on logit)" in line for line in lines),
               f"TCP loss inside a SUT netem is judged, got {judged['ledger.wire']}")

        # A judged window's surplus: a write that failed partway left whole lines the generator
        # never counted, one batch per ambiguous drop.
        late = _fault_pair("x1", "netem", "logit", 200, 210, tcp=True)
        over = results("tcp-surplus", steady_surplus=350, timeline_extra=late)
        expect(over["ledger.wire"].status == checks.FAIL
               and "W + counted exceeds G by 350, over the 200 its 0 ambiguous drop(s) and 2 "
               "edge(s) allow" in over["ledger.wire"].detail,
               f"a surplus past a judged window's edges FAILs, got {over['ledger.wire']}")
        allowed = results("tcp-surplus-ambiguous", steady_surplus=350, timeline_extra=late,
                          gen_edit=ambiguous(230, 2))
        expect(allowed["ledger.wire"].status == checks.PASS
               and "surplus allowed 400 (2 ambiguous drop(s) + 2 edge(s), x one 100-line "
               "batch)" in "\n".join(allowed["ledger.wire"].lines)
               and "ambiguous_at_most_once 2" in allowed["ledger.wire"].detail,
               f"two ambiguous drops allow two batches of surplus, got {allowed['ledger.wire']}")

        # A partition: judged when the SUT kept its address, reported when the reconnect moved
        # it, and reported when no address was recorded after it.
        kept = _fault_pair("x1", "partition", "logit", 52, 62, udp=True, tcp=True,
                           ips={"soak": "172.18.0.3"}, ips_after={"soak": "172.18.0.3"})
        same = results("tcp-partition-kept", steady_loss=1000, timeline_extra=kept)
        expect(same["ledger.wire"].status == checks.FAIL
               and any("judged (x1 partition on logit (IP 172.18.0.3 kept))" in line
                       for line in same["ledger.wire"].lines),
               f"a partition that kept the address is judged, got {same['ledger.wire']}")
        moved = _fault_pair("x1", "partition", "logit", 52, 62, udp=True, tcp=True,
                            ips={"soak": "172.18.0.3"}, ips_after={"soak": "172.18.0.5"})
        away = results("tcp-partition-moved", steady_loss=1000, timeline_extra=moved)
        expect(away["ledger.wire"].status == checks.PASS
               and any("fault (x1 partition on logit (IP 172.18.0.3 -> 172.18.0.5))" in line
                       for line in away["ledger.wire"].lines),
               f"a partition that moved the address is reported, got {away['ledger.wire']}")
        unknown = _fault_pair("x1", "partition", "logit", 52, 62, udp=True, tcp=True,
                              ips={"soak": "172.18.0.3"})
        blind = results("tcp-partition-unknown", steady_loss=1000, timeline_extra=unknown)
        expect(blind["ledger.wire"].status == checks.PASS
               and "IP 172.18.0.3 -> unknown" in "\n".join(blind["ledger.wire"].lines),
               f"a partition with no address after it is reported, got {blind['ledger.wire']}")

        # Recovery reads the frames: half the frames after the stop is half the rate.
        def half(record):
            stamp = record["timestamp"]
            if (record["metrics"][0]["name"] == "logit.input.frames"
                    and _stamp(T0 + 135) <= stamp <= _stamp(T0 + 180)):
                record["metrics"][0]["value"] //= 2
            return record
        slow = results("tcp-recovery", sut_edit=half)
        expect(slow["recovery"].status == checks.FAIL
               and "is 50% of the warmup rate" in slow["recovery"].detail,
               f"recovery judges a TCP listener's frames, got {slow['recovery']}")

        # A killed life's band reads its ingest rate from the frames.
        kill_dir = _kill_run(tmp, "tcp-kill")
        stdout = kill_dir / "logs" / "logit.stdout"
        stdout.write_text(stdout.read_text().replace('"logit.input.datagrams"',
                                                     '"logit.input.frames"'))
        resolved_path = kill_dir / "scenario.resolved.json"
        resolved = json.loads(resolved_path.read_text())
        resolved["ledger"]["source"] = [{**resolved["ledger"]["source"][0], "wire": "tcp"}]
        resolved_path.write_text(json.dumps(resolved))
        killed = {r.id: r for r in checks.run_all(kill_dir)}
        expect(killed["ledger.egress"].status == checks.PASS
               and "band [-1,000, 3,000]: ingest 200/s" in killed["ledger.egress"].detail,
               f"a killed TCP life's band reads the frames' rate, got {killed['ledger.egress']}")

        # A source naming `wire = "udp"` scores as one naming none.
        named = {r.id: (r.status, r.detail) for r in checks.run_all(
            _ledger_run(tmp, "udp-named", steady_loss=400, timeline_extra=gen_netem))}
        resolved_path = Path(tmp) / "udp-named" / "scenario.resolved.json"
        resolved = json.loads(resolved_path.read_text())
        resolved["ledger"]["source"][0]["wire"] = "udp"
        resolved_path.write_text(json.dumps(resolved))
        renamed = {r.id: (r.status, r.detail)
                   for r in checks.run_all(Path(tmp) / "udp-named")}
        expect(named == renamed, "a source naming wire udp scores as one naming none")


def _tcp_rules(root):
    """The `wire` key and its load-time cross-check against the SUT config, the fault
    predicates per wire, a partition recording its addresses, the driver recording every
    wire's flag, and the shipped tcp-ingress scenario."""
    path, raw = _base(root)

    def mutate(edit):
        changed = copy.deepcopy(raw)
        edit(changed)
        return changed

    def with_config(text, wire, listener="statsd"):
        """`load()`'s problems for statsd-vm with its SUT config replaced by `text` and its
        source's `wire` set, None for no `wire` key."""
        with tempfile.TemporaryDirectory() as tmp:
            scenario_path = Path(tmp) / "scenario.toml"
            changed = copy.deepcopy(raw)
            for key, name in changed["configs"].items():
                (Path(tmp) / name).write_text(
                    text if key == "sut" else (path.parent / name).read_text())
            changed["ledger"]["source"][0]["sut_listener"] = listener
            if wire is not None:
                changed["ledger"]["source"][0]["wire"] = wire
            try:
                scenario.from_dict(changed, scenario_path)
                return []
            except scenario.ScenarioError as err:
                return err.problems

    _refused(path, mutate(lambda r: r["ledger"]["source"][0].update(wire="sctp")),
             "ledger.source 1: `wire` must be one of udp, tcp, file", "an unknown wire")
    _refused(path, mutate(lambda r: r["ledger"]["source"][0].update(wire="tcp")),
             "ledger.source 1: `wire` is tcp, but `sut_listener` 'statsd' is a statsd_in with "
             "no transport:, a udp listener", "a TCP source on a UDP listener")
    tcp_config = "components:\n  statsd:\n    type: statsd_in\n    transport: tcp  # lines\n"
    problems = with_config(tcp_config, None)
    expect(any("`wire` is udp, but `sut_listener` 'statsd' is a statsd_in over tcp, a tcp "
               "listener" in p for p in problems),
           f"a source naming no wire on a TCP listener is refused, got {problems}")
    expect(with_config(tcp_config, "tcp") == [],
           f"a TCP source on a TCP listener loads, got {with_config(tcp_config, 'tcp')}")
    flow = "components:\n  statsd: { type: statsd_in, transport: udp, bind: 0.0.0.0:8125 }\n"
    expect(with_config(flow, "udp") == [] and with_config(flow, None) == [],
           "a flow-style UDP listener loads with wire udp or none")
    lines_tcp = "components:\n  lines: { type: lines_in, bind: 0.0.0.0:5514 }  # transport: udp\n"
    problems = with_config(lines_tcp, None, listener="lines")
    expect(any("is a lines_in with no transport:, a tcp listener" in p for p in problems),
           f"lines_in with no transport is TCP, and a comment isn't read, got {problems}")
    problems = with_config(tcp_config, "tcp", listener="missing")
    expect(any("`sut_listener` 'missing' isn't a component of the SUT config" in p
               for p in problems),
           f"a listener the SUT config lacks is refused, got {problems}")
    unix = "components:\n  statsd: { type: statsd_in, transport: unix_stream, bind: /s }\n"
    problems = with_config(unix, "tcp")
    expect(any("is a statsd_in over unix_stream, which the ledger has no wire for" in p
               for p in problems),
           f"a transport with no wire is refused, got {problems}")
    tailed = "components:\n  statsd: { type: tail_in, paths: [/shared/app.log] }\n"
    problems = with_config(tailed, "file")
    expect(problems == ["ledger.source 1: a file wire's `generator_sink` must be a file_out, "
                        "and 'statsd' is a statsd_out"],
           f"a tail_in source takes wire file, and its writer must be a file_out, got "
           f"{problems}")

    # The fault predicates per wire.
    roles = scenario.BASE_ROLES
    for action, on, udp, tcp, file in (
            ("netem", "generator", True, False, False),
            ("netem", "logit", False, True, False),
            ("pause", "logit", True, True, True),
            ("pause", "generator", False, True, False),
            ("stop", "generator", False, True, False),
            ("partition", "generator", True, True, False),
            ("partition", "logit", True, True, False),
            ("kill", "logit", True, True, True),
            ("netem", "victoria-metrics", False, False, False),
            ("stop", "victoria-metrics", False, False, False)):
        step = scenario.Step("c0s1", 0, 0, action, on, "", 0, 1)
        got = tuple(faults.ACTIONS[action].affects(wire, step, roles)
                    for wire in ("udp", "tcp", "file"))
        expect(got == (udp, tcp, file),
               f"{action} on {on} affects (udp, tcp, file) {got}, expected {(udp, tcp, file)}")
    for action, on, how in (("stop", "logit", "fault"), ("kill", "logit", "fault"),
                            ("pause", "logit", "judged"), ("netem", "logit", "judged"),
                            ("pause", "generator", "judged")):
        fault = checks.Fault("x", action, on, 0, 1, affects_udp_ingress=True,
                             affects_egress=False, affects_tcp_ingress=True)
        expect(checks.wire_judgment(fault, "tcp") == how
               and checks.wire_judgment(fault, "udp") == "fault",
               f"a {action} on {on} is {checks.wire_judgment(fault, 'tcp')} on TCP")

    # A partition records the addresses before its disconnect and after its reconnect.
    class Networks:
        def __init__(self):
            self.address = "172.18.0.3"
            self.calls = []

        def inspect(self, container, timeout=None):
            return {"NetworkSettings": {"Networks": {"soak": {
                "Aliases": ["logit"], "IPAddress": self.address}}}}

        def run(self, argv, timeout=None):
            self.calls.append(argv)
            if argv[:2] == ["network", "connect"]:
                self.address = "172.18.0.5"
            return docker.Result(0, "", "")

    fake = Networks()
    ctx = faults.Context(docker=fake, ids={"logit": "abc"}, netem_image="netem")
    step = scenario.Step("c0s7", 6, 0, "partition", "logit", "", 0, 45)
    applied = faults.partition_apply(ctx, step)
    reverted = faults.partition_revert(ctx, step)
    expect(applied["ips"] == {"soak": "172.18.0.3"}
           and reverted["ips_after"] == {"soak": "172.18.0.5"}
           and ["network", "connect", "--alias", "logit", "soak", "abc"] in fake.calls,
           f"a partition records its addresses either side, got {applied}, {reverted}")
    with tempfile.TemporaryDirectory() as tmp:
        timeline = [_phase("start", 0, t0=T0),
                    {**_action("apply", "c0s7", "partition", "logit", 10), **applied},
                    {**_action("revert", "c0s7", "partition", "logit", 55), **reverted}]
        data = checks.RunData(_run_dir(tmp, "partition-ips", timeline))
        fault = data.faults[0]
        expect(fault.ip_text() == "IP 172.18.0.3 -> 172.18.0.5",
               f"the checks read a partition's addresses, got {fault.ip_text()!r}")

    # The driver records each wire's flag on every action.
    loaded = scenario.load(root / "scenarios" / "statsd-vm" / "scenario.toml")
    run = driver.Run(root, loaded, 600, None, False, "/nonexistent", [], "logit:soak",
                     roles=dict(roles))
    run.timeline = _Records()
    run.ctx = faults.Context(docker=None, ids={}, netem_image="netem")
    run.t0 = 0.0
    run.say = lambda text: None
    run.after = lambda kind, step, result: None
    run.act("apply", scenario.Step("c0s1", 0, 0, "mark", "logit", "", 0, 1))
    record = run.timeline[-1]
    expect(record.get("affects_udp_ingress") is True and record.get("affects_tcp_ingress") is True
           and record.get("affects_file_ingress") is False,
           f"the driver records every wire's flag, got {record}")

    tcp = scenario.load(root / "scenarios" / "tcp-ingress" / "scenario.toml")
    expect(tcp.sources[0].get("wire") == "tcp"
           and [step.action for step in tcp.steps] == [
               "netem", "netem", "netem", "pause", "stop", "kill", "partition", "pause"],
           f"tcp-ingress names a TCP source and its eight faults, got {tcp.sources}")


def _as_file_wire(run_dir, w_minus_g=0, counted=True, line_bytes=95, files_open=None):
    """Turns a ledger fixture into a file-wire run in place: the listener's datagrams become
    `logit.input.lines`, the generator's `output.messages` its `file_out`'s
    `logit.component.events.delivered`, set so the run's W − G is `w_minus_g`, and the source
    names `wire = "file"`. `counted` records the driver's `shared_counted` phase with G's lines
    at `line_bytes` each (a dict replaces it). `files_open` is `{offset: n}` of
    `logit.input.files.open` gauges written for the listener `in`."""
    stdout = run_dir / "logs" / "logit.stdout"
    out, w = [], 0
    for line in stdout.read_text().splitlines():
        record = json.loads(line)
        if record["metrics"][0]["name"] == "logit.input.datagrams":
            record["metrics"][0]["name"] = "logit.input.lines"
            w += record["metrics"][0]["value"]
        out.append(json.dumps(record))
    for offset, n in (files_open or {}).items():
        out.append(_line(_stamp(T0 + offset), "logit.input.files.open", "gauge", n,
                         component="in"))
    stdout.write_text("\n".join(out) + "\n")
    g = w - w_minus_g
    gen_path = run_dir / "logs" / "generator.stdout"
    gen, first = [], True
    for line in gen_path.read_text().splitlines():
        record = json.loads(line)
        name = record["metrics"][0]["name"]
        if name == "logit.output.datagrams":
            continue
        if name == "logit.output.messages":
            record["metrics"][0]["name"] = "logit.component.events.delivered"
            record["metrics"][0]["value"] = 0
            if first:
                record["metrics"][0]["value"] = g
                first = False
        gen.append(json.dumps(record))
    gen_path.write_text("\n".join(gen) + "\n")
    if counted:
        phase = (counted if isinstance(counted, dict)
                 else {"files": 3, "lines": g, "bytes": g * line_bytes})
        with open(run_dir / "timeline.jsonl", "a") as timeline:
            timeline.write(json.dumps(_phase("shared_counted", 301, **phase)) + "\n")
            timeline.write(json.dumps(_phase("tail_caught_up", 302, reached=True, w=w, g=g,
                                             reads=2)) + "\n")
    resolved_path = run_dir / "scenario.resolved.json"
    resolved = json.loads(resolved_path.read_text())
    resolved["ledger"]["source"] = [{**resolved["ledger"]["source"][0], "wire": "file"}]
    resolved_path.write_text(json.dumps(resolved))
    return run_dir


def _file_ledger():
    """The ledger on a file source: W from `input.lines` and G from the writer's
    `events.delivered`, held to the shared directory's count; the run's W − G judged against
    the replay band, a stopped life's split lines one per open file, and a killed life's
    replay above and lost counts below; and `ledger.summary`'s end-to-end bound widened by the
    band's upper edge."""
    with tempfile.TemporaryDirectory() as tmp:
        def stopped(name, **kwargs):
            run_dir = _ledger_run(tmp, name, shutdown_drops=0, life0_extra=30)
            return {r.id: r for r in checks.run_all(_as_file_wire(run_dir, **kwargs))}

        clean = stopped("file-clean")
        for row in ("ledger.wire", "ledger.intake", "ledger.edge", "ledger.egress",
                    "ledger.summary", "drops"):
            expect(clean[row].status == checks.PASS,
                   f"{row} PASSes the balanced file-wire fixture, got {clean[row]}")
        wire = clean["ledger.wire"].detail
        expect(wire.startswith("G 54,030 (shared directory: 54,030 lines in 3 files) vs W "
                               "54,030 + counted 0: W + F − G 0 against the replay band [0, 1]")
               and "the tailers reached G" in wire,
               f"a file source reads input.lines and events.delivered, got {wire}")
        expect(any("life 0 (stopped): [0, +1]" in line for line in clean["ledger.wire"].lines),
               f"a stopped life allows one split line per file, got {clean['ledger.wire'].lines}")
        split = stopped("file-split", w_minus_g=1)
        expect(split["ledger.wire"].status == checks.PASS,
               f"one split line at a stop is inside the band, got {split['ledger.wire']}")
        over = stopped("file-surplus", w_minus_g=2)
        expect(over["ledger.wire"].status == checks.FAIL
               and over["ledger.wire"].detail.startswith(
                   "W + F − G 2 is surplus, outside the replay band [0, 1]"),
               f"a surplus past the band FAILs, got {over['ledger.wire']}")
        wider = stopped("file-files-open", w_minus_g=3, files_open={100: 3})
        expect(wider["ledger.wire"].status == checks.PASS
               and "[0, 3]" in wider["ledger.wire"].detail,
               f"a stopped life with three files open allows three splits, got "
               f"{wider['ledger.wire']}")
        lost = stopped("file-lost", w_minus_g=-1)
        expect(lost["ledger.wire"].status == checks.FAIL
               and lost["ledger.wire"].detail.startswith(
                   "W + F − G -1 is uncounted loss, outside the replay band [0, 1]"),
               f"a line never read FAILs, got {lost['ledger.wire']}")
        missing = stopped("file-uncounted", counted=False)
        expect(missing["ledger.wire"].status == checks.FAIL
               and "precondition: no shared_counted phase" in missing["ledger.wire"].detail,
               f"a run with no shared count FAILs, got {missing['ledger.wire']}")
        mismatch = stopped("file-mismatch", counted={"files": 3, "lines": 54_000,
                                                     "bytes": 5_130_000})
        expect(mismatch["ledger.wire"].status == checks.FAIL
               and "the shared directory holds 54,000 line(s) in 3 file(s), but the writer's "
               "events.delivered is 54,030" in mismatch["ledger.wire"].detail,
               f"G short of the shared directory's count FAILs, got {mismatch['ledger.wire']}")

        # A killed life at 200 lines/s, 5 s drains, and a 5 s checkpoint_interval: below, 1,000
        # lost counts; above, 1,000 lines plus two files x 690 lines a 64 KiB read holds at 95
        # bytes a line; the stopped life adds one split.
        def killed(name, w_minus_g):
            run_dir = _kill_run(tmp, name, every_window=False)
            return {r.id: r for r in checks.run_all(_as_file_wire(run_dir,
                                                                  w_minus_g=w_minus_g))}

        edge = killed("file-kill-high", 2_381)
        lines = "\n".join(edge["ledger.wire"].lines)
        expect(edge["ledger.wire"].status == checks.PASS
               and "against the replay band [-1,000, 2,381]" in edge["ledger.wire"].detail
               and "life 1 (killed): [-1,000, +2,380]: 200 lines/s x a 5s drain interval below"
               in lines and "2 file(s) x one 64 KiB read of 690 lines above" in lines,
               f"a killed life's replay band, got {edge['ledger.wire']}")
        past = killed("file-kill-over", 2_382)
        expect(past["ledger.wire"].status == checks.FAIL,
               f"a replay past the band FAILs, got {past['ledger.wire']}")
        short = killed("file-kill-low", -1_000)
        expect(short["ledger.wire"].status == checks.PASS,
               f"a killed life's lost counts inside the band pass, got {short['ledger.wire']}")
        shorter = killed("file-kill-lost", -1_001)
        expect(shorter["ledger.wire"].status == checks.FAIL,
               f"loss past a killed life's band FAILs, got {shorter['ledger.wire']}")
        # V holds the replayed lines too, so it exceeds G less counted loss by up to the band.
        expect(edge["ledger.summary"].status == checks.PASS
               and "plus the replay band's 2,381" in edge["ledger.summary"].detail,
               f"the end-to-end bound adds the band's upper edge, got "
               f"{edge['ledger.summary'].detail}")

    # The driver's line count: newlines only, every file, a partial write left out.
    with tempfile.TemporaryDirectory() as tmp:
        (Path(tmp) / "app.log").write_text("a\nb\nc")
        (Path(tmp) / "app.log.1").write_text("d\ne\n")
        counted = collect.count_lines(tmp)
        expect(counted == {"files": 2, "lines": 4, "bytes": 9},
               f"count_lines counts newlines across every file, got {counted}")


def _file_rules(root):
    """A file wire's writer must be a `file_out`, and only a file wire's may be; the driver's
    shared directory and its compose variable; and its wait for the tailers, which ends when W
    reaches the shared count or stops growing."""
    tail = scenario.load(root / "scenarios" / "tail-rotate" / "scenario.toml")
    expect([source["wire"] for source in tail.file_sources] == ["file"],
           f"tail-rotate's one source is a file wire, got {tail.file_sources}")
    with tempfile.TemporaryDirectory() as tmp:
        tail_dir = Path(tmp) / "tail-rotate"
        shutil.copytree(root / "scenarios" / "tail-rotate", tail_dir)
        config = tail_dir / "logit-generator.yaml"
        config.write_text(config.read_text().replace("type: file_out", "type: stdio_out"))
        try:
            scenario.load(tail_dir / "scenario.toml")
            expect(False, "a file wire whose writer isn't a file_out loads")
        except scenario.ScenarioError as err:
            expect(any("a file wire's `generator_sink` must be a file_out, and 'file' is a "
                       "stdio_out" in p for p in err.problems),
                   f"a file wire's writer must be a file_out, got {err.problems}")
        config.write_text(config.read_text().replace("type: stdio_out", "type: file_out"))
        toml = tail_dir / "scenario.toml"
        toml.write_text(toml.read_text().replace('wire = "file"', ""))
        try:
            scenario.load(toml)
            expect(False, "a file_out writer on a source with no wire loads")
        except scenario.ScenarioError as err:
            expect(any("`generator_sink` 'file' is a file_out, which only a file wire reads"
                       in p for p in err.problems),
                   f"a file_out writer needs a file wire, got {err.problems}")

        run = driver.Run(root, tail, tail.duration, None, False, tmp, [], "logit:soak",
                         roles=_override_roles(tail))
        run.prepare()
        shared = run.run_dir / "shared"
        env = (run.run_dir / "compose.env").read_text()
        expect(shared.is_dir() and (shared.stat().st_mode & 0o777) == 0o777
               and f"SOAK_SHARED_DIR={shared.resolve()}\n" in env,
               f"prepare() makes a 0777 shared directory and names it, got {env}")
        override = tail.compose_path().read_text()
        expect(override.count("${SOAK_SHARED_DIR:?") == 2,
               "tail-rotate's override mounts the shared directory into both services")

        # The wait: W read from captured chunks reaches the shared count, or stops growing.
        (shared / "app.log").write_text("x\n" * 10)
        stdout = run.run_dir / "logs" / "logit.stdout"

        class Capture:
            def __init__(self, deltas):
                self.deltas = list(deltas)
                self.offset = 0

            def chunk(self, ids):
                delta = self.deltas.pop(0) if self.deltas else 0
                self.offset += 5
                with open(stdout, "a") as out:
                    out.write(_line(_stamp(T0 + self.offset), "logit.process.uptime", "gauge",
                                    float(self.offset)) + "\n")
                    out.write(_line(_stamp(T0 + self.offset), "logit.input.lines", "sum",
                                    delta, component="tail") + "\n")

        clock = [1000.0]
        reals = (driver._now, driver.time.sleep)
        driver._now = lambda: clock[0]
        driver.time.sleep = lambda s: clock.__setitem__(0, clock[0] + s)
        try:
            run.ids = {"logit": "a"}
            run.t0 = clock[0]
            run.timeline = _Records()
            run.capture = Capture([4, 4, 2])
            run.wait_tailed()
            reached = run.timeline[-1]
            stdout.write_text("")
            run.timeline = _Records()
            run.capture = Capture([4, 3])
            run.wait_tailed()
            held = run.timeline[-1]
        finally:
            driver._now, driver.time.sleep = reals
        expect(run.timeline[0]["phase"] == "shared_counted"
               and run.timeline[0]["lines"] == 10 and run.timeline[0]["files"] == 1,
               f"the wait counts the shared lines first, got {run.timeline[0]}")
        expect(reached["phase"] == "tail_caught_up" and reached["reached"] is True
               and reached["w"] == 10 and reached["reads"] == 3,
               f"the wait ends once W reaches the shared count, got {reached}")
        expect(held["reached"] is False and held.get("held") is True and held["w"] == 7,
               f"the wait ends once W holds short of it, got {held}")


def _two_sut_dir(root, tmp, interval="7s"):
    """A copy of statsd-vm in `tmp` whose `[configs]` adds `collector`, a further SUT service,
    with its own config: statsd-vm's SUT config with `aggregate`'s window set to `interval`."""
    base = root / "scenarios" / "statsd-vm"
    directory = Path(tmp) / "two-suts"
    directory.mkdir()
    text = (base / "scenario.toml").read_text()
    marker = 'generator = "logit-generator.yaml"\n'
    (directory / "scenario.toml").write_text(
        text.replace(marker, marker + 'collector = "logit-collector.yaml"\n', 1)
        + '\n[[ledger.hop]]\nfrom = "logit"\nsink = "hop"\nto = "collector"\n'
          'listener = "hop"\n')
    sut = (base / "logit-sut.yaml").read_text()
    for name in ("logit-sut.yaml", "logit-generator.yaml"):
        (directory / name).write_text((base / name).read_text())
    (directory / "logit-collector.yaml").write_text(
        sut.replace("interval: 10s", f"interval: {interval}", 1))
    return directory / "scenario.toml"


def _two_sut_rules(root):
    """A further SUT service, declared by its own `[configs]` key and run by a service with the
    role `sut`, and the `[[ledger.hop]]` joining it to `logit`: the chain it makes, every rule
    `load()` and the role map hold them to, and the fault predicates, under which only `logit`,
    which holds every source's listener, can lose UDP."""
    path, raw = _base(root)

    def mutate(edit):
        changed = copy.deepcopy(raw)
        edit(changed)
        return changed

    def with_hop(r, **hop):
        """`r` with `collector` declared and a hop to it, `hop` overriding its keys."""
        r["configs"]["collector"] = "logit-sut.yaml"
        r["ledger"]["hop"] = [{**TWO_SUT_HOP, "listener": "hop", **hop}]

    with tempfile.TemporaryDirectory() as tmp:
        two = scenario.load(_two_sut_dir(root, tmp))
        expect(two.suts == ["logit", "collector"] and two.sink_sut == "collector"
               and two.generators == ["generator"]
               and two.sut_config_path("collector").name == "logit-collector.yaml"
               and two.sut_config_path("logit").name == "logit-sut.yaml",
               f"a further [configs] key is a SUT service after logit, got {two.suts}")
        expect(two.to_json()["suts"] == ["logit", "collector"]
               and two.to_json()["ledger"]["hop"] == [{"from": "logit", "sink": "hop",
                                                       "to": "collector", "listener": "hop"}],
               "the resolved scenario records the SUT chain and the hop")
        expect(scenario.service_problems(two, TWO_SUT_ROLES) == [],
               f"a stack running collector as a sut holds, got "
               f"{scenario.service_problems(two, TWO_SUT_ROLES)}")
        problems = scenario.service_problems(two, scenario.BASE_ROLES)
        expect(any("the stack needs the sut services logit and the [configs] keys collector, "
                   "and has logit" in p for p in problems),
               f"a declared SUT the stack doesn't run is refused, got {problems}")
        problems = scenario.service_problems(two, {**TWO_SUT_ROLES, "collector": "generator"})
        expect(any("the [configs] keys collector, and has logit" in p for p in problems),
               f"a declared SUT running as a generator is refused, got {problems}")
        killed = mutate(with_hop)
        killed["step"].append({"at": "11m10s", "action": "kill", "on": "collector",
                               "for": "20s"})
        loaded = scenario.from_dict(killed, path)
        expect(scenario.service_problems(loaded, TWO_SUT_ROLES) == [],
               "a kill on a declared second SUT is allowed")
    _refused(path, raw, "one sut service, logit, and has collector, logit; a further sut "
             "service needs its own [configs] key", "a second sut the scenario doesn't declare",
             roles=TWO_SUT_ROLES)
    _refused(path, mutate(lambda r: r["configs"].update(logit="logit-sut.yaml")),
             "configs.logit: the SUT logit's config is `sut`", "a [configs] key for logit")
    _refused(path, mutate(lambda r: r["configs"].update({"Col!": "logit-sut.yaml"})),
             "configs.Col!: a key other than `sut`", "a SUT service that isn't a slug")
    _refused(path, mutate(lambda r: r["configs"].update(
        {"col-a": "logit-sut.yaml", "col_a": "logit-sut.yaml"})),
             "configs.col_a: SUT service 'col_a' would mount its config through "
             "SOAK_COL_A_CONFIG", "two SUT services sharing one compose variable")
    _refused(path, mutate(lambda r: (r["ledger"]["source"].append(dict(LINES_SOURCE)),
                                     r["configs"].update({"generator-lines": "logit-sut.yaml",
                                                          "generator_lines": "logit-sut.yaml"}))),
             "configs.generator_lines: SUT service 'generator_lines' would mount its config "
             "through SOAK_GENERATOR_LINES_CONFIG", "a SUT service sharing a source's variable")
    _refused(path, mutate(lambda r: r["configs"].update(collector="missing.yaml")),
             "configs.collector: missing.yaml not found", "a SUT config that isn't there")
    _refused(path, mutate(lambda r: (with_hop(r),
                                     r["step"].append({"at": "11m10s", "action": "kill",
                                                       "on": "generator", "for": "20s"}))),
             "kill is for logit or collector only", "a kill on the generator beside two SUTs",
             roles=TWO_SUT_ROLES)

    _refused(path, mutate(lambda r: r["configs"].update(collector="logit-sut.yaml")),
             "configs: SUT services logit and collector need a [[ledger.hop]]",
             "two SUTs with no hop")
    _refused(path, mutate(lambda r: (with_hop(r), r["ledger"]["hop"].append(dict(TWO_SUT_HOP)))),
             "ledger.hop must be one table", "two hops")
    _refused(path, mutate(lambda r: (with_hop(r), r["ledger"]["hop"][0].pop("listener"))),
             "ledger.hop: missing `listener`", "a hop without its logit_in")
    _refused(path, mutate(lambda r: with_hop(r, window=8)), "ledger.hop: unknown key `window`",
             "an unknown hop key")
    _refused(path, mutate(lambda r: with_hop(r, sink="a b")),
             "ledger.hop: `sink` must name a component id", "a hop sink that isn't an id")
    _refused(path, mutate(lambda r: with_hop(r, **{"from": "collector"})),
             "ledger.hop: `from` must be logit", "a hop from the far SUT")
    _refused(path, mutate(lambda r: with_hop(r, to="generator")),
             "ledger.hop: `to` must be the one further SUT service [configs] names, collector",
             "a hop to a service [configs] doesn't declare a SUT")
    _refused(path, mutate(lambda r: r["ledger"].update(hop=[dict(TWO_SUT_HOP)])),
             "`to` must be the one further SUT service [configs] names; it names none",
             "a hop with one SUT")
    _refused(path, mutate(lambda r: (with_hop(r), r["ledger"].update(sut_script="lua"))),
             "ledger.hop and ledger.sut_script can't be combined", "a hop beside a script")
    _refused(path, mutate(lambda r: (with_hop(r),
                                     r["ledger"]["source"][0].update(sut_transforms=["x"]))),
             "ledger.hop and a source's `sut_transforms` can't be combined",
             "a hop beside a source's transforms")

    for action in ("pause", "stop", "kill", "restart", "partition", "mark", "exit"):
        for on, udp in (("logit", True), ("collector", False)):
            step = scenario.Step("c0s1", 0, 0, action, on, "", 0, 1)
            expect(faults.ACTIONS[action].affects_udp_ingress(step, TWO_SUT_ROLES) == udp
                   and faults.ACTIONS[action].affects_egress(step, TWO_SUT_ROLES),
                   f"a {action} on {on} affects UDP ingress: {udp}, and egress")


class _ChainDocker:
    """Records the end sequence's stops and the container each tail reads, on a fake clock."""

    def __init__(self, clock, events, lines):
        self.clock = clock
        self.events = events
        self.lines = lines

    def run(self, args, timeout=None):
        self.events.append(("stop", args[-1]) if args[0] == "stop" else tuple(args))
        return docker.Result(0, "", "")

    def inspect(self, container, timeout=None):
        return {"State": {"ExitCode": 0}}

    def logs_tail(self, container, lines, timeout=0):
        self.events.append(("tail", container))
        return docker.Result(0, "\n".join(self.lines(self.clock[0])) + "\n", "")


def _two_sut_driver(root):
    """The driver with two SUT services: `compose.env` sets the further SUT's config variable,
    `aggregate_interval` reads the config of the SUT holding `aggregate`, and the end sequence
    stops the generators, then each SUT in chain order, waiting before `logit` stops until every
    sink it reports is drained with nothing in flight, and before `collector` stops on its
    sinks' backends."""
    with tempfile.TemporaryDirectory() as tmp:
        two = scenario.load(_two_sut_dir(root, tmp))
        run = driver.Run(root, two, two.duration, None, False, Path(tmp) / "out", [],
                         "logit:soak", roles=TWO_SUT_ROLES)
        run.prepare()
        env = (run.run_dir / "compose.env").read_text()
        resolved = json.loads((run.run_dir / "scenario.resolved.json").read_text())
        expect(f"SOAK_COLLECTOR_CONFIG={two.config_path('collector')}\n" in env
               and f"SOAK_SUT_CONFIG={two.config_path('sut')}\n" in env
               and (run.run_dir / "configs" / "logit-collector.yaml").is_file()
               and resolved["suts"] == ["logit", "collector"],
               f"compose.env sets each SUT's config and the run copies both, got {env}")
        for jsonl in (run.timeline, run.watchdog, run.stats, run.freshness, run.capture):
            jsonl.close()
        expect(run.aggregate_interval() == 7.0,
               f"the wait reads collector's aggregate window, got {run.aggregate_interval()}")

        def hop_lines(in_flight, sinks=("hop",)):
            def lines(now):
                out = []
                for t in range(int(now) - 30, int(now) + 1, 5):
                    stamp = _stamp(t)
                    out.append(_line(stamp, "logit.process.uptime", "gauge", float(t - 900)))
                    for sink in sinks:
                        out.append(_line(stamp, "logit.component.buffer.batches", "gauge", 0,
                                         component=sink))
                    out.append(_line(stamp, "logit.output.in_flight", "gauge", in_flight(t),
                                     component="hop"))
                return out
            return lines

        def end(lines, has_vm=True):
            clock = [1000.0]
            events = []
            seq = driver.Run(root, two, two.duration, None, False, tempfile.mkdtemp(dir=tmp),
                             [], "logit:soak", roles=TWO_SUT_ROLES)
            seq.run_dir.mkdir(parents=True)
            seq.refresh_vm_base = lambda backend: None
            seq.docker = _ChainDocker(clock, events, lines)
            seq.ids = {name: f"id-{name}" for name in TWO_SUT_ROLES}
            seq.timeline = _Records()
            seq.t0 = 900.0
            seq.has_vm = has_vm
            seq.inspect_all = lambda: None
            seq.wait_quiet = lambda: events.append(("quiet",))
            reals = (driver._now, driver.time.sleep, driver._print, vm.force_flush,
                     vm.export_raw)
            driver._now = lambda: clock[0]
            driver.time.sleep = lambda s: clock.__setitem__(0, clock[0] + s)
            driver._print = lambda text, stream=None: None
            vm.force_flush = lambda base: None
            vm.export_raw = lambda base, selector, start, timeout=None: ""
            try:
                seq.end_sequence()
            finally:
                (driver._now, driver.time.sleep, driver._print, vm.force_flush,
                 vm.export_raw) = reals
            return events, seq.timeline

        events, timeline = end(hop_lines(lambda t: 3 if t < 1040 else 0))
        order = [e for e in events if e[0] in ("stop", "quiet")]
        expect(order == [("stop", "id-generator"), ("stop", "id-logit"), ("quiet",),
                         ("stop", "id-collector")],
               f"generators, then logit, then collector once its backends are quiet, got "
               f"{order}")
        tails = {e[1] for e in events if e[0] == "tail"}
        drained = [r for r in timeline if r.get("phase") == "sink_drained"]
        stopped = [r.get("service") for r in timeline if r.get("phase") == "sut_stopped"]
        expect(tails == {"id-logit"} and len(drained) == 1 and drained[0]["service"] == "logit"
               and drained[0]["held"] and drained[0]["waited_s"] >= 40
               and drained[0]["sinks"] == {"hop": {"buffer_batches": 0, "retrying": None,
                                                   "in_flight": 0}},
               f"logit stops once every sink it reports is empty with nothing in flight, got "
               f"{tails} {drained}")
        expect(stopped == ["logit", "collector"],
               f"one sut_stopped phase per SUT service, in chain order, got {stopped}")
        _, timeline = end(hop_lines(lambda t: 1))
        drained = [r for r in timeline if r.get("phase") == "sink_drained"]
        expect(not drained[0]["held"] and drained[0]["sinks"]["hop"]["in_flight"] == 1,
               f"frames in flight never count as drained, got {drained}")
        events, timeline = end(hop_lines(lambda t: 0, ("hop", "victoria_metrics")),
                               has_vm=False)
        order = [e for i, e in enumerate(events) if e[0] == "stop"
                 or (e[0] == "tail" and events[i - 1] != e)]
        drained = [(r["service"], sorted(r["sinks"])) for r in timeline
                   if r.get("phase") == "sink_drained"]
        expect(order == [("stop", "id-generator"), ("tail", "id-logit"), ("stop", "id-logit"),
                         ("tail", "id-collector"), ("stop", "id-collector")]
               and drained == [("logit", ["hop"]), ("collector", ["victoria_metrics"])],
               f"with no backend, collector stops once its ledger sinks drain, read from its "
               f"own stdout, got {order} {drained}")


# Two SUT services: `logit` holds the listener `in` and a `hop` sink, `collector` holds
# `window` and `sink`. A kill on collector over +203..+213 ends its life 0.
TWO_SUT_KILL = (203, 213)
TWO_SUT_HOP = {"from": "logit", "sink": "hop", "to": "collector", "listener": "hop_in"}
TWO_SUT_HOP_CONFIG = "  hop: { type: logit_out, endpoint: collector:7400, window: 8 }\n"
TWO_SUT_COLLECTOR_CONFIG = ("components:\n"
                            "  window:\n"
                            "    type: aggregate\n"
                            "    interval: 10s\n")


def _two_sut_run(tmp, name, kill_on="collector", collector_lives=2, extra=0, short=0,
                 hop_dropped=0, drops=(), pause=False, logit_lives=None, replayed=3,
                 stalled=False, partial=False, collector_restart=False):
    """A two-SUT run across a native hop: the generator and `logit` move `PER_DRAIN` lines per
    5 s drain into `logit`'s one life, whose hop sink `hop` takes each drain's lines in 10
    frames; `collector`'s `logit_in` `hop_in` forwards them into `window`, which delivers one
    batch per drain to `sink`. The timeline holds a kill on `kill_on` (None for none) over
    `TWO_SUT_KILL`, and `collector`'s telemetry shows `collector_lives` lives, 1 or 2. Its
    killed life misses the two drains the kill spans and leaves 2 batches queued; its next life
    forwards the 2,000 lines it missed in its first drain and replays the 2 batches. Its killed
    life's V is one 1,000-line window short, inside its band. `extra` lines are forwarded,
    absorbed, and stored once more in that first drain, a surplus across the hop, and `short`
    lines fewer, with `hop_dropped` of them counted dropped at `logit`'s hop sink. `pause` adds
    a pause on `collector` over +240..+250. A kill on `logit` gives it two lives, unless
    `logit_lives` is 1, and nothing reaches it or crosses the hop while it is down; its killed
    life leaves 3 hop batches queued, and its next life's first drain replays `replayed`; under
    `stalled` its hop sink delivers nothing in its last two drains and leaves 23 queued; under
    `partial` it delivers 2 of its last drain's 10 and leaves 11 queued. `collector_restart`
    stops `collector` over the same span as that kill, a graceful restart.
    `collector` counts one
    `events.dropped{reason="stale"}` at `window`. `drops` adds a count of 1 for each `(service,
    metric, component, reason)`, `service` being "logit" or "collector"."""
    if logit_lives is None:
        logit_lives = 2 if kill_on == "logit" else 1
    down = range(TWO_SUT_KILL[0], TWO_SUT_KILL[1]) if kill_on == "logit" else range(0)
    gen, ingress, egress = [], [], []
    for offset in range(5, 301, 5):
        stamp = _stamp(T0 + offset)
        gen.append(_line(stamp, "logit.process.uptime", "gauge", float(offset)))
        for metric in ("logit.output.messages", "logit.output.datagrams"):
            gen.append(_line(stamp, metric, "sum", PER_DRAIN, component="out"))
        gen.append(_line(stamp, "logit.component.events.sent", "sum", PER_DRAIN,
                         component="load"))
        gen.append(_line(stamp, "logit.component.batches.sent", "sum", PER_DRAIN // 100,
                         component="load"))
        if offset in down:
            continue
        # `logit` drains 2 s before the generator and `collector`, as an unrelated process does.
        early = _stamp(T0 + offset - 2)
        restarted = logit_lives == 2 and offset > TWO_SUT_KILL[1]
        ingress.append(_line(early, "logit.process.uptime", "gauge",
                             float(offset - 2 - (TWO_SUT_KILL[1] if restarted else 0))))
        for metric, value, comp in (("logit.input.datagrams", PER_DRAIN, "in"),
                                    ("logit.component.events.sent", PER_DRAIN, "in"),
                                    ("logit.component.events.received", PER_DRAIN, "hop"),
                                    ("logit.component.batches.received", 10, "hop"),
                                    ("logit.component.batches.delivered",
                                     0 if stalled and offset in (195, 200) else
                                     2 if partial and offset == 200 else
                                     10 + (replayed if restarted and offset == 215 else 0),
                                     "hop")):
            ingress.append(_line(early, metric, "sum", value, component=comp))
        ingress.append(_line(early, "logit.component.receive.utilization", "gauge", 0,
                             component="in"))
        last = logit_lives == 2 and offset == max(o for o in range(5, TWO_SUT_KILL[0], 5))
        queued = (23 if stalled else 11 if partial else 3) if last else 0
        ingress.append(_line(early, "logit.component.buffer.batches", "gauge", queued,
                             component="hop"))
        if logit_lives == 2 and offset == TWO_SUT_KILL[1] + 2:
            ingress.append(_line(early, "logit.component.buffer.disk.replayed", "sum",
                                 replayed, component="hop"))
    if hop_dropped:
        ingress.append(_line(_stamp(T0 + 98), "logit.component.events.dropped", "sum",
                             hop_dropped, component="hop", attrs={"reason": "overflow_oldest"}))
    if collector_lives == 2:
        lives = [(0, 0, range(5, TWO_SUT_KILL[0], 5)),
                 (1, TWO_SUT_KILL[1], range(215, 321, 5))]
    else:
        lives = [(0, 0, range(5, 321, 5))]
    for life, start, offsets in lives:
        for offset in offsets:
            stamp = _stamp(T0 + offset)
            w = PER_DRAIN if offset <= 300 and offset not in down else 0
            if life == 1 and offset == 215 and kill_on != "logit":
                w += 2 * PER_DRAIN
            if offset == (215 if collector_lives == 2 else 250):
                w += extra
            if offset == 100:
                w -= short
            egress.append(_line(stamp, "logit.process.uptime", "gauge", float(offset - start)))
            for metric, value, comp in (("logit.component.events.sent", w, "hop_in"),
                                        ("logit.component.events.received", w, "window"),
                                        ("logit.transform.metrics.absorbed", w, "window"),
                                        ("logit.component.events.sent", 1, "window"),
                                        ("logit.component.events.received", 1, "sink"),
                                        ("logit.component.batches.received", 1, "sink"),
                                        ("logit.component.batches.delivered",
                                         1 + (2 if life == 1 and offset == 215 else 0),
                                         "sink")):
                egress.append(_line(stamp, metric, "sum", value, component=comp))
            if life == 1 and offset == 215:
                egress.append(_line(stamp, "logit.component.buffer.disk.replayed", "sum", 2,
                                    component="sink"))
                egress.append(_line(stamp, "logit.input.batches.resends", "sum", 3,
                                    component="hop_in"))
            queued = 2 if collector_lives == 2 and life == 0 and offset == offsets[-1] else 0
            egress.append(_line(stamp, "logit.component.buffer.batches", "gauge", queued,
                                component="sink"))
            for metric in ("logit.component.buffer.utilization", "logit.component.retrying"):
                egress.append(_line(stamp, metric, "gauge", 0, component="sink"))
    egress.append(_line(_stamp(T0 + 100), "logit.component.events.dropped", "sum", 1,
                        component="window", attrs={"reason": "stale"}))
    for service, metric, component, reason in drops:
        line = _line(_stamp(T0 + 150), metric, "sum", 1, component=component,
                     attrs={"reason": reason})
        (ingress if service == "logit" else egress).append(line)
    timeline = [_phase("start", 0, t0=T0)]
    if kill_on is not None:
        timeline += [
            _action("apply", "c0s1", "kill", kill_on, TWO_SUT_KILL[0], TWO_SUT_KILL[0] + 0.1,
                    affects_udp_ingress=kill_on == "logit", affects_egress=True),
            _action("revert", "c0s1", "kill", kill_on, TWO_SUT_KILL[1] - 1, TWO_SUT_KILL[1],
                    affects_udp_ingress=kill_on == "logit", affects_egress=True),
            _ready("c0s1", kill_on, TWO_SUT_KILL[1] + 1)]
    if collector_restart:
        timeline += [
            _action("apply", "c0s3", "stop", "collector", TWO_SUT_KILL[0], TWO_SUT_KILL[0] + 0.1,
                    affects_egress=True),
            _action("revert", "c0s3", "stop", "collector", TWO_SUT_KILL[1] - 1, TWO_SUT_KILL[1],
                    affects_egress=True),
            _ready("c0s3", "collector", TWO_SUT_KILL[1] + 1)]
    if pause:
        timeline += [_action("apply", "c0s2", "pause", "collector", 240, 240.1,
                             affects_egress=True),
                     _action("revert", "c0s2", "pause", "collector", 249, 250,
                             affects_egress=True)]
    timeline += [_phase("end_begin", 300), _phase("sink_drained", 305, service="logit", held=True),
                 _phase("sut_stopped", 310, service="logit"), _phase("end_end", 330)]
    ledger = {**LEDGER, "sink": [{"id": "sink", "backend": "victoria-metrics",
                                  "vm_selector": "{__name__=~\"x_[0-9]+_total\"}"}],
              "hop": [TWO_SUT_HOP]}
    run_dir = _run_dir(tmp, name, timeline, ingress, ledger=ledger,
                       services=TWO_SUT_ROLES, suts=["logit", "collector"],
                       configs={"sut": "logit-sut.yaml", "collector": "logit-collector.yaml"})
    (run_dir / "configs").mkdir()
    (run_dir / "configs" / "logit-sut.yaml").write_text(
        KILL_SUT_CONFIG.split("  window:")[0] + TWO_SUT_HOP_CONFIG)
    (run_dir / "configs" / "logit-collector.yaml").write_text(TWO_SUT_COLLECTOR_CONFIG)
    (run_dir / "logs" / "collector.stdout").write_text("\n".join(egress) + "\n")
    (run_dir / "logs" / "generator.stdout").write_text("\n".join(gen) + "\n")
    # V: one series whose total restarts at collector's life 1: its killed life 0 one window
    # short of the 40,000 it absorbed, inside its band, and life 1 all 20,000 it absorbed.
    values, timestamps = [], []
    segments = [(39000 - short, range(10, 201, 10)), (20000 + extra, range(220, 321, 10))]
    if collector_lives == 1:
        segments = [(60000 - (2 * PER_DRAIN if down else 0) + extra - short,
                     range(10, 321, 10))]
    for total, stamps in segments:
        for i, s in enumerate(stamps):
            values.append(total * (i + 1) // len(stamps))
            timestamps.append(int((T0 + s) * 1000))
    _write_export(run_dir, "sink", [{"metric": {"__name__": "x_0_total"}, "values": values,
                                     "timestamps": timestamps}])
    return run_dir


def _tcp_hop_run(tmp, name, extra, ambiguous_at=None):
    """`_two_sut_run`'s unkilled one-life hop run with its one source on a TCP wire: `logit`
    counts `input.frames` in place of datagrams, the generator counts no `output.datagrams`,
    `extra` lines reach V as surplus, and one `ambiguous_at_most_once` batch drop is counted on
    the generator's sink at that offset when `ambiguous_at` is set."""
    run_dir = _two_sut_run(tmp, name, kill_on=None, collector_lives=1, extra=extra)

    def rewrite(path, edit):
        out = []
        for line in path.read_text().splitlines():
            record = json.loads(line)
            for one in edit(record):
                out.append(json.dumps(one))
        path.write_text("\n".join(out) + "\n")

    def sut(record):
        if record["metrics"][0]["name"] == "logit.input.datagrams":
            record["metrics"][0]["name"] = "logit.input.frames"
        return [record]

    def gen(record):
        name = record["metrics"][0]["name"]
        if name == "logit.output.datagrams":
            return []
        if (name == "logit.component.events.sent" and ambiguous_at is not None
                and record["timestamp"] == _stamp(T0 + ambiguous_at)):
            return [record, json.loads(_line(
                record["timestamp"], "logit.component.batches.dropped", "sum", 1,
                component="out", attrs={"reason": "ambiguous_at_most_once"}))]
        return [record]

    rewrite(run_dir / "logs" / "logit.stdout", sut)
    rewrite(run_dir / "logs" / "generator.stdout", gen)
    resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
    resolved["ledger"]["source"] = [{**resolved["ledger"]["source"][0], "wire": "tcp"}]
    (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
    return run_dir


def _two_sut_checks():
    """The checks across a native hop: each service's lives come from its own telemetry and the
    starts the timeline gives it, so a kill on `collector` makes two lives there and leaves
    `logit` one; the listener rows judge `logit`'s final life and the rows from `aggregate` on
    `collector`'s, fed by `logit_in`'s E_B, with V split on `collector`'s lives; `ledger.hop`
    judges the hop on run totals, exact with no kill or pause, with a killed far side's tail and
    one send window of surplus allowed, and a counted hop drop leaving the gap; `ledger.summary`
    FAILs a surplus end to end; `progress`, `ledger.replay`, `identity.sink`, and `recovery`
    read each sink in its own SUT; `drops` keys each term on its service; and the end
    sequence's phases are read per service."""
    with tempfile.TemporaryDirectory() as tmp:
        run_dir = _two_sut_run(tmp, "two-kill")
        data = checks.RunData(run_dir)
        led = checks.Ledger(data)
        expect(led.problems == [] and led.across and led.inn.service == "logit"
               and led.service == "collector",
               f"two SUTs each read their own telemetry with no problem, got {led.problems}")
        expect(led.inn.lives == [0] and led.inn.killed == []
               and led.lives == [0, 1] and led.killed == [0]
               and led.killed_at == {0: T0 + TWO_SUT_KILL[0]},
               f"a kill on collector makes two collector lives and leaves logit one, got "
               f"{led.inn.lives} {led.inn.killed} {led.lives} {led.killed_at}")
        expect(led.V.get("sink") == {0: 39000, 1: 20000},
               f"V splits on collector's lives, got {led.V.get('sink')}")
        expect(led.R == 770 and led.interval == 10.0 and led.window == 8
               and led.per_frame == 100 and led.R_hop == 6500,
               f"R and the window read logit's config, the aggregate window collector's, and a "
               f"frame carries 100 events, got {led.R} {led.interval} {led.window} "
               f"{led.per_frame} {led.R_hop}")
        rows = {r.id: r for r in checks.run_all(run_dir)}
        for row in ("ledger.intake", "ledger.aggregate", "ledger.edge", "ledger.replay",
                    "progress", "ledger.wire", "recovery", "ledger.hop", "ledger.egress",
                    "ledger.summary", "identity.sink"):
            expect(rows[row].status == checks.PASS, f"{row} PASSes two SUTs, got {rows[row]}")
        detail = rows["ledger.intake"].detail
        expect("final life 0: W − D 60,000 vs E + B 60,000" in detail
               and "collector life 0: E_B − X_B − Ab 0 within [0, R_hop]" in detail,
               f"ledger.intake judges logit's final life and collector's earlier one, got "
               f"{detail}")
        expect(rows["ledger.aggregate"].detail.startswith("final life 1: Ab 20,000 == A 20,000"),
               f"ledger.aggregate judges collector's final life, got "
               f"{rows['ledger.aggregate'].detail}")
        detail = rows["ledger.edge"].detail
        expect("logit final life 0: E 60,000 == S_hop[hop] 60,000" in detail
               and "collector final life 1: E_B[hop_in] 20,000 == A 20,000" in detail
               and "A_sent 22 == S_in[sink] 22 (aggregate to sink)" in detail,
               f"ledger.edge judges each side in its own final life, got {detail}")
        detail = rows["ledger.hop"].detail
        expect("ΣS_hop 60,000 − hop dropped 0 − ΣE_B 60,000 = 0, ok" in detail
               and "resends 3 at collector" in detail
               and "collector life 0 killed: −800 (its marks lost), +1,000" in detail,
               f"ledger.hop balances the hop on run totals, got {detail}")
        detail = rows["ledger.egress"].detail
        expect("life 0 (killed): Ab 40,000 − V[sink] 39,000 = 1,000, within band" in detail
               and "life 1 (final): Ab 20,000 − V[sink] 20,000 = 0, ok" in detail,
               f"ledger.egress judges collector's lives fed by the hop, got {detail}")
        expect("end to end over the run, G 60,000 less counted loss 0: sink: V 59,000 <= G − "
               "counted 60,000" in rows["ledger.summary"].detail,
               f"ledger.summary bounds V end to end, got {rows['ledger.summary'].detail}")
        expect("logit hop: final life 0" in rows["identity.sink"].detail
               and "gap 0" in rows["identity.sink"].detail,
               f"identity.sink judges the hop sink in logit, got {rows['identity.sink']}")
        lines = "\n".join(rows["recovery"].lines)
        expect("c0s1 (kill on collector): collector sink recovered at" in lines
               and "c0s1 (kill on collector): logit hop recovered at" in lines
               and "ingest 200/s (100%)" in lines,
               f"recovery judges each SUT's sinks on its own drains, got {lines}")
        expect("sink: life 1's first drain replayed 2 vs killed life 0's last-drain "
               "buffer.batches 2" in rows["ledger.replay"].detail,
               f"ledger.replay judges collector's killed life, got {rows['ledger.replay']}")
        expect(rows["drops"].status == checks.WARN
               and rows["drops"].lines == [
                   "collector window: logit.component.events.dropped{reason=\"stale\"} 1"],
               f"drops reads each SUT's counters and names the service, got {rows['drops']}")
        expect(data.sut_phase("sink_drained", "collector") is None
               and data.sut_phase("sink_drained", "logit")["held"],
               "an end-sequence phase is read for its own SUT service")

        def hop_row(name, **kwargs):
            return {r.id: r for r in checks.run_all(_two_sut_run(tmp, name, **kwargs))}

        within = hop_row("two-within", extra=500)
        expect(within["ledger.hop"].status == checks.PASS
               and "= -500, within allowance (allowance [-800, 1,000]"
               in within["ledger.hop"].detail,
               f"a surplus inside one send window after collector's kill passes, got "
               f"{within['ledger.hop']}")
        beyond = hop_row("two-beyond", extra=900)
        expect(beyond["ledger.hop"].status == checks.FAIL
               and "= -900, surplus" in beyond["ledger.hop"].detail,
               f"a surplus past one send window FAILs, got {beyond['ledger.hop']}")
        exact = hop_row("two-exact", kill_on=None, collector_lives=1, extra=1)
        expect(exact["ledger.hop"].status == checks.FAIL
               and "= -1, surplus (no kill or pause of either side, so 0)"
               in exact["ledger.hop"].detail,
               f"with no kill or pause the hop must balance, got {exact['ledger.hop']}")
        expect(exact["ledger.summary"].status == checks.FAIL
               and exact["ledger.summary"].detail.startswith(
                   "end to end over the run, G 60,000 less counted loss 0: sink: V 60,001 > G "
                   "− counted 60,000, a surplus of 1"),
               f"ledger.summary FAILs a surplus end to end that every per-life term balances, "
               f"got {exact['ledger.summary']}")
        # A write that failed partway leaves whole lines in V that G doesn't count: one
        # generator batch per ambiguous drop on a TCP wire, none on UDP.
        def tcp_hop(name, extra, ambiguous_at=None):
            return {r.id: r for r in checks.run_all(
                _tcp_hop_run(tmp, name, extra, ambiguous_at))}

        partial = tcp_hop("two-tcp-partial", 100, ambiguous_at=150)
        expect(partial["ledger.summary"].status == checks.PASS
               and "end to end over the run, G 60,000 less counted loss 0 and a partial-frame "
               "allowance 100: sink: V 60,100 <= G − counted 60,100"
               in partial["ledger.summary"].detail,
               f"one ambiguous drop on a TCP source allows one batch of surplus end to end, "
               f"got {partial['ledger.summary']}")
        beyond_partial = tcp_hop("two-tcp-partial-beyond", 101, ambiguous_at=150)
        expect(beyond_partial["ledger.summary"].status == checks.FAIL
               and "sink: V 60,101 > G − counted 60,100, a surplus of 1"
               in beyond_partial["ledger.summary"].detail,
               f"a surplus past the ambiguous allowance FAILs, got "
               f"{beyond_partial['ledger.summary']}")

        short = hop_row("two-short", kill_on=None, collector_lives=1, short=10)
        expect(short["ledger.hop"].status == checks.FAIL
               and "= 10, uncounted" in short["ledger.hop"].detail,
               f"lines the hop lost uncounted FAIL, got {short['ledger.hop']}")
        counted = hop_row("two-counted", kill_on=None, collector_lives=1, short=10,
                          hop_dropped=10)
        expect(counted["ledger.hop"].status == checks.PASS
               and "hop dropped 10" in counted["ledger.hop"].detail,
               f"a counted hop drop leaves the gap, got {counted['ledger.hop']}")
        expect(counted["drops"].lines == [
                   "collector window: logit.component.events.dropped{reason=\"stale\"} 1"],
               f"drops leaves the hop sink's counted drops to ledger.hop, got "
               f"{counted['drops'].lines}")
        tail = hop_row("two-tail", short=500)
        expect(tail["ledger.hop"].status == checks.PASS
               and "= 500, within allowance" in tail["ledger.hop"].detail,
               f"a killed collector life's uncounted tail passes, got {tail['ledger.hop']}")
        paused = hop_row("two-paused", kill_on=None, collector_lives=1, extra=300,
                         pause=True)
        expect(paused["ledger.hop"].status == checks.PASS
               and "c0s2 pause on collector: −800" in paused["ledger.hop"].detail,
               f"a pause of collector allows one send window of surplus, got "
               f"{paused['ledger.hop']}")
        extra_drops = hop_row("two-drops", kill_on=None, collector_lives=1, drops=(
            ("logit", "logit.component.batches.dropped", "hop", "rejected"),
            ("logit", "logit.component.batches.dropped", "in", "closed_consumer"),
            ("logit", "logit.component.datagrams.dropped", "in", "queue_full"),
            ("collector", "logit.component.batches.dropped", "sink", "shutdown"),
            ("collector", "logit.component.events.dropped", "hop_in", "closed_consumer"),
            ("collector", "logit.component.events.dropped", "in", "closed_consumer")))
        expect(extra_drops["ledger.hop"].status == checks.FAIL
               and "FAIL: 1 batch(es) rejected by the destination"
               in extra_drops["ledger.hop"].detail,
               f"a rejected hop batch FAILs ledger.hop, got {extra_drops['ledger.hop']}")
        expect(extra_drops["drops"].lines == [
                   "logit in: logit.component.batches.dropped{reason=\"closed_consumer\"} 1",
                   "collector in: logit.component.events.dropped{reason=\"closed_consumer\"} 1",
                   "collector window: logit.component.events.dropped{reason=\"stale\"} 1"],
               f"drops leaves out what ledger.hop, the ledger's listener terms, X_B, and "
               f"ledger.egress judge, and keeps logit's own batch drop off the hop and a "
               f"collector node that shares a listener's id, got {extra_drops['drops'].lines}")

        killed = hop_row("two-logit-killed", kill_on="logit", collector_lives=1)
        for row in ("ledger.hop", "ledger.replay", "ledger.intake", "ledger.edge",
                    "identity.sink", "ledger.egress", "ledger.summary"):
            expect(killed[row].status == checks.PASS,
                   f"{row} PASSes a kill on logit, got {killed[row]}")
        expect("logit life 0 killed: −1,000 (200/s over its last drain interval), +100 (the "
               "batch in hand))" in killed["ledger.hop"].detail,
               f"ledger.hop allows a killed logit life's tail below and one frame above, and "
               f"no replay with collector up throughout, got {killed['ledger.hop']}")
        lost = hop_row("two-logit-lost", kill_on="logit", collector_lives=1, short=500)
        expect(lost["ledger.hop"].status == checks.FAIL
               and "= 500, uncounted" in lost["ledger.hop"].detail,
               f"more than one frame missing after a logit kill FAILs, got {lost['ledger.hop']}")
        twice = hop_row("two-logit-twice", kill_on="logit", collector_lives=1, extra=1250)
        expect(twice["ledger.hop"].status == checks.FAIL
               and "= -1,250, surplus" in twice["ledger.hop"].detail,
               f"a killed logit's replay forwarded again by a collector that kept its marks "
               f"FAILs, got {twice['ledger.hop']}")
        again = hop_row("two-logit-again", kill_on="logit", collector_restart=True, extra=1250)
        expect(again["ledger.hop"].status == checks.PASS
               and "= -1,250, within allowance (allowance [-1,300, 100]" in
               again["ledger.hop"].detail
               and "−300 (its replay of frames committed after its last cursor persist, which "
               "collector, restarted at +213s, forwards again)" in again["ledger.hop"].detail,
               f"a killed logit's replay forwarded again by a collector that restarted in "
               f"between passes, got {again['ledger.hop']}")
        lost = hop_row("two-logit-tail", kill_on="logit", collector_lives=1, extra=500)
        expect(lost["ledger.hop"].status == checks.PASS
               and "= -500, within allowance" in lost["ledger.hop"].detail,
               f"lines a killed logit life took after its last drain pass, got "
               f"{lost['ledger.hop']}")
        expect("logit hop: life 1's first drain replayed 3 vs killed life 0's last-drain "
               "buffer.batches 3, band [-12, 14]: 2.0 batches/s received and 2.0 delivered"
               in killed["ledger.replay"].detail,
               f"ledger.replay judges the hop sink's replay within its band, got "
               f"{killed['ledger.replay']}")
        late = hop_row("two-logit-late", kill_on="logit", collector_lives=1, replayed=10)
        expect(late["ledger.replay"].status == checks.PASS,
               f"frames delivered since the last checkpoint replay again, inside the band, got "
               f"{late['ledger.replay']}")
        stalled = hop_row("two-logit-stalled", kill_on="logit", collector_lives=1,
                          replayed=36, stalled=True)
        expect(stalled["ledger.replay"].status == checks.PASS
               and "band [-1, 14]" in stalled["ledger.replay"].detail
               and "its last delivering 2.0/s" in stalled["ledger.replay"].detail,
               f"commits before a stalled hop's last checkpoint replay again, from its last "
               f"delivering drain's rate, got {stalled['ledger.replay']}")
        partial = hop_row("two-logit-partial", kill_on="logit", collector_lives=1,
                          replayed=24, partial=True)
        expect(partial["ledger.replay"].status == checks.PASS
               and "band [-4, 14]" in partial["ledger.replay"].detail,
               f"a drain interval whose delivery stopped partway doesn't understate the commits "
               f"a replay repeats, got {partial['ledger.replay']}")
        over = hop_row("two-logit-over", kill_on="logit", collector_lives=1, replayed=20)
        expect(over["ledger.replay"].status == checks.FAIL
               and "off by 17" in over["ledger.replay"].detail,
               f"a hop replay past the band FAILs, got {over['ledger.replay']}")
        on_logit = checks.Ledger(checks.RunData(
            _two_sut_run(tmp, "two-kill-logit", kill_on="logit", collector_lives=1,
                         logit_lives=1)))
        expect(any("SUT logit telemetry shows 1 life/lives where logit.process.uptime "
                   "decreases, but the timeline starts the service into 2" in p
                   for p in on_logit.problems)
               and not any("collector" in p for p in on_logit.problems),
               f"a kill on logit counts logit's lives alone, got {on_logit.problems}")
        short = checks.Ledger(checks.RunData(
            _two_sut_run(tmp, "two-kill-short", collector_lives=1)))
        expect(any("SUT collector telemetry shows 1 life/lives" in p for p in short.problems),
               f"a kill on collector its telemetry doesn't show is named, got {short.problems}")


def _standin_base(root):
    path = root / "scenarios" / "standin-remote-write" / "scenario.toml"
    return path, tomllib.loads(path.read_text())


def _standin_codec(root):
    """The stand-in record's decoders: Snappy blocks `snap::raw` wrote, a `WriteRequest` from
    one, the literal-only encoder the fixtures use, and the malformed blocks it refuses."""
    vectors = json.loads((root / "standin" / "snap-vectors.json").read_text())
    text = b"abcabcabcabc" * 30 + b"0123456789" * 3 + bytes(range(256)) * 2
    block = bytes.fromhex(vectors["text"])
    expect(standin.snappy_decompress(block) == text,
           "a snap::raw block with copies of both widths decodes to its input")
    expect(len(block) < len(text) // 2, "the vector holds copies, not only literals")
    series = [({"__name__": f"soak_requests_{i}_total"},
               [(1791720000000 + 10000 * k, float(100 * k + i)) for k in range(3)])
              for i in range(40)]
    decoded = standin.write_request(standin.snappy_decompress(
        bytes.fromhex(vectors["write_request"])))
    expect(decoded == series, f"a snap::raw WriteRequest decodes to its 40 series, got "
           f"{decoded[:2]}")
    real = standin.write_request(standin.snappy_decompress(
        bytes.fromhex(vectors["prometheus_out_body"])))
    want = vectors["prometheus_out_body_expect"]
    expect(len(real) == want["series"]
           and all(len(samples) == 1 and samples[0][0] == want["timestamp_ms"]
                   for _, samples in real)
           and [real[0][0]["__name__"], real[0][1][0][1]] == want["first"]
           and sum(samples[0][1] for _, samples in real) == want["value_sum"],
           f"a body prometheus_out sent decodes to its series, got {real[:1]}")
    raw = standin.encode_write_request(series)
    expect(standin.snappy_decompress(standin.snappy_compress_literal(raw)) == raw
           and standin.snappy_decompress(standin.snappy_compress_literal(b"x" * 70000))
           == b"x" * 70000 and standin.snappy_decompress(b"\x00") == b"",
           "the literal-only encoder round-trips short, long, and empty input")
    negative = [({"__name__": "x"}, [(-5, -1.5)])]
    expect(standin.write_request(standin.encode_write_request(negative)) == negative,
           "a negative timestamp decodes as an int64")
    for bad, what in ((block[:-3], "a truncated block"),
                      (bytes([5, 0x01 | (1 << 2), 0x01]), "a copy before any output"),
                      (bytes([9, 0x08, ord("a"), ord("b"), ord("c")]), "a length mismatch"),
                      (b"", "an empty buffer")):
        try:
            standin.snappy_decompress(bad)
            expect(False, f"{what} decoded")
        except standin.DecodeError:
            expect(True, "")
    try:
        standin.decode_body({"body_b64": base64.b64encode(block).decode(),
                             "content_encoding": "zstd"})
        expect(False, "a zstd body decoded")
    except standin.DecodeError as err:
        expect("not snappy" in str(err), f"a body not under snappy is named, got {err}")


def _standin_record_parts():
    """`stored_series` over a record: later writes replace earlier ones, a resend of the same
    value counts, a changed value is a conflict, an unstored or undecodable request is left out
    of V, and the request classes."""
    a, b = {"__name__": "a_total"}, {"__name__": "b_total"}
    requests = [
        _standin_request(1, 10, [(a, [(1000, 1.0)]), (b, [(1000, 5.0)])]),
        _standin_request(2, 11, [(a, [(1000, 1.0)])]),
        _standin_request(3, 12, [(a, [(2000, 3.0)])], status=400, stored=False),
        _standin_request(4, 13, [(b, [(1000, 6.0)])], status=503, stored=True, cue="c0s1"),
        {**_standin_request(5, 14, [(a, [(3000, 9.0)])]), "body_b64": "!!"},
        _standin_request(6, 15, [(a, [(4000, 4.0)])], answer="reset", stored=False),
    ]
    series, rewrites, conflicts, undecodable = standin.stored_series(requests)
    expect(series == [{"metric": a, "timestamps": [1000], "values": [1.0]},
                      {"metric": b, "timestamps": [1000], "values": [6.0]}],
           f"V keeps each stored sample, the last write's value, got {series}")
    expect(rewrites == 1 and conflicts == [(tuple(b.items()), 1000, 5.0, 6.0)]
           and [seq for seq, _ in undecodable] == [5],
           f"one same-value resend, one changed value, one bad body, got {rewrites} "
           f"{conflicts} {undecodable}")
    expect([standin.status_class(r) for r in requests]
           == ["2xx", "2xx", "4xx", "5xx", "2xx", "network_error"],
           "a request's class is its status bucket, a reset network_error")
    text = "\n".join([json.dumps(requests[0]), json.dumps({"event": "cue", "cue": {}}),
                      "Traceback (most recent call last):", "[1]", ""])
    got, cues, unparsed = standin.read_record(text)
    expect(len(got) == 1 and len(cues) == 1 and unparsed == 2,
           f"the record splits requests, cues, and lines that aren't objects, got "
           f"{len(got)} {len(cues)} {unparsed}")


def _load_standin_server(root):
    """tools/soak/standin/standin.py as a module, for its handlers."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("soak_standin_server",
                                                  root / "standin" / "standin.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _standin_server(root):
    """The stand-in's own handlers on loopback ports: its cue rules, the default answer, a
    cued status with its body, `apply`, a reset, a write to the wrong path, and the record line
    each prints; and `faults.post_cue` against its control port."""
    import http.client
    import http.server
    import io
    import threading
    server = _load_standin_server(root)
    for cue, needle in (({"step": "x"}, "one of `status` and `answer`"),
                        ({"step": "x", "status": 200, "answer": "reset"}, "one of"),
                        ({"step": "x", "status": 99}, "100 to 599"),
                        ({"step": "x", "status": True}, "100 to 599"),
                        ({"step": "x", "answer": "close"}, "`answer` must be"),
                        ({"step": "x", "answer": "reset", "body": "x"}, "`body`"),
                        ({"step": "x", "status": 200, "apply": 1}, "`apply`"),
                        ({"status": 200}, "`step`"), ({"step": "x", "status": 200, "y": 1},
                                                      "unknown keys"), ([], "JSON object")):
        problems = server.cue_problems(cue)
        expect(any(needle in p for p in problems), f"cue {cue} refused with {needle!r}, got "
               f"{problems}")
    expect(server.cue_problems({}) == [] and server.cue_problems(
        {"step": "c0s1", "status": 400, "body": "x", "apply": True}) == [],
           "an empty cue and a full one are accepted")

    state = server.State("remote_write", "/api/v1/write")
    data = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.data_handler(state))
    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.control_handler(state))
    threads = [threading.Thread(target=one.serve_forever, daemon=True) for one in (data, control)]
    for thread in threads:
        thread.start()
    printed = io.StringIO()
    real_stdout = server.sys.stdout
    server.sys.stdout = printed
    control_base = f"http://127.0.0.1:{control.server_address[1]}"

    def post(path, body=b"payload"):
        conn = http.client.HTTPConnection("127.0.0.1", data.server_address[1], timeout=5)
        try:
            conn.request("POST", path, body=body, headers={"Content-Encoding": "snappy"})
            response = conn.getresponse()
            return response.status, response.read().decode()
        except (ConnectionError, http.client.HTTPException) as err:
            return type(err).__name__, ""
        finally:
            conn.close()

    try:
        expect(vm.healthy(control_base), "the control port answers /health OK")
        first = post("/api/v1/write")
        rc, _ = faults.post_cue(control_base, {"step": "c0s4", "status": 400,
                                               "body": "out of order sample"})
        second = post("/api/v1/write")
        rc_bad, err = faults.post_cue(control_base, {"step": "c0s5", "status": 99})
        faults.post_cue(control_base, {"step": "c0s6", "status": 503, "apply": True})
        third = post("/api/v1/write")
        faults.post_cue(control_base, {"step": "c0s7", "answer": "reset"})
        fourth = post("/api/v1/write")
        faults.post_cue(control_base, {})
        fifth = post("/api/v2/write")
        sixth = post("/api/v1/write")
        # A request's line is printed after its answer is written, so the last one can trail
        # the client's read.
        deadline = time.monotonic() + 5
        while (printed.getvalue().count('"event": "request"') < 6
               and time.monotonic() < deadline):
            time.sleep(0.01)
    finally:
        server.sys.stdout = real_stdout
        for one in (data, control):
            one.shutdown()
            one.server_close()
    expect(first == (200, "") and second == (400, "out of order sample") and third[0] == 503
           and fourth[0] in ("RemoteDisconnected", "ConnectionResetError")
           and fifth[0] == 404 and sixth == (200, ""),
           f"the stand-in answers its default, then each cue, got "
           f"{[first, second, third, fourth, fifth, sixth]}")
    expect(rc == 0 and rc_bad == 1 and "100 to 599" in err,
           f"post_cue returns 0 on a 200 and 1 with the stand-in's reason, got {rc} {rc_bad} "
           f"{err}")
    expect(faults.post_cue(None, {})[0] == 1, "a stand-in with no port fails the cue")
    lines = [json.loads(line) for line in printed.getvalue().splitlines()]
    requests = [line for line in lines if line["event"] == "request"]
    got = [(r["status"], r["answer"], r["stored"], r["cue"]) for r in requests]
    expect(got == [(200, "status", True, None), (400, "status", False, "c0s4"),
                   (503, "status", True, "c0s6"), (None, "reset", False, "c0s7"),
                   (404, "status", False, None), (200, "status", True, None)],
           f"each request's record line says how it was answered and whether it was stored, "
           f"got {got}")
    expect(base64.b64decode(requests[0]["body_b64"]) == b"payload"
           and requests[0]["content_encoding"] == "snappy",
           "a record line carries the raw body and its encoding")
    expect(sum(1 for line in lines if line["event"] == "cue") == 4,
           "every accepted cue is in the record, a refused one isn't")


def _respond_rules(root):
    """The `respond` action's and `class` key's rules, the `[target] kind = "standin"` rules,
    the stand-in's roles, profile, image, and compose service, and the step's cue."""
    path, base = _standin_base(root)
    vm_path, vm_base = _base(root)

    def mutate(raw, edit):
        raw = copy.deepcopy(raw)
        edit(raw)
        return raw

    def step(index, **changes):
        def edit(raw):
            for key in [k for k, v in changes.items() if v is None]:
                raw["step"][index].pop(key, None)
            raw["step"][index].update({k: v for k, v in changes.items() if v is not None})
        return edit

    windows = copy.deepcopy(base)
    windows["ledger"]["vm_every_window"] = True
    expect(scenario.from_dict(windows, path).ledger["vm_every_window"] is True,
           "a stand-in target takes vm_every_window: its V has an export's shape")
    loaded = scenario.from_dict(copy.deepcopy(base), path)
    expect(loaded.standin and not loaded.kafka and not loaded.has_vm
           and loaded.target == {"kind": "standin", "name": "standin", "env": [],
                                 "shape": "remote_write"},
           f"standin-remote-write loads as a stand-in target, got {loaded.target}")
    roles = scenario.base_roles(loaded)
    expect(roles == {"logit": "sut", "generator": "generator", "standin": "standin"},
           f"a stand-in target's stack runs the stand-in and no backend, got {roles}")
    expect(driver.profiles_for(loaded) == ["standin"],
           "a stand-in target runs the standin compose profile")
    first = scenario.expand(loaded)[0]
    expect(first.cue() == {"step": "c0s1", "status": 503}
           and first.expected() == {"status": 503, "class": "ambiguous"},
           f"a respond step's cue and timeline fields, got {first.cue()} {first.expected()}")
    applied = next(s for s in scenario.expand(loaded) if s.apply is not None)
    expect(applied.cue() == {"step": applied.id, "status": 503, "apply": True},
           f"`apply` travels in the cue, got {applied.cue()}")
    for name, action in faults.ACTIONS.items():
        one = scenario.Step(id="x", spec_index=0, cycle=0, action=name, on="standin", args="",
                            start=0, end=1)
        expect(action.affects_egress(one, roles),
               f"a {name} on the stand-in affects the SUT's egress")
    respond = scenario.Step(id="x", spec_index=0, cycle=0, action="respond", on="standin",
                            args="", start=0, end=1)
    expect(not faults.ACTIONS["respond"].affects_udp_ingress(respond, roles),
           "a respond never affects UDP ingress")

    cases = [
        (base, step(0, status=None), "respond needs one of `status` and `answer`",
         "a respond with neither"),
        (base, step(0, answer="reset"), "respond needs one of `status` and `answer`",
         "a respond with both"),
        (base, step(0, status=600), "`status` must be an integer from 100 to 599",
         "a status past 599"),
        (base, step(0, status=True), "`status` must be an integer", "a boolean status"),
        (base, step(0, status="503"), "`status` must be an integer", "a string status"),
        (base, step(0, status=None, answer="close"), "`answer` must be one of reset",
         "an unknown answer"),
        (base, step(17, body="x"), "`body` is a string, and goes with `status` only",
         "a body on a reset"),
        (base, step(0, body=5), "`body` is a string", "a body that isn't a string"),
        (base, step(0, apply="yes"), "`apply` must be true or false", "a string apply"),
        (base, step(18, status=503), "`status` is for respond only", "a status on a stop"),
        (base, step(18, answer="reset"), "`answer` is for respond only", "an answer on a stop"),
        (base, step(0, **{"class": "lost"}), "`class` must be one of clean, ambiguous, "
         "rejected, refused", "an unknown class"),
        (base, lambda raw: raw["step"].append({"at": "29m", "action": "netem", "on": "logit",
                                               "args": "loss 1%", "for": "10s",
                                               "class": "clean"}),
         "`class` is for respond and stop only", "a class on a netem"),
        (base, step(0, on="logit"), "respond is for a standin service", "a respond on the SUT"),
        (base, lambda raw: raw["step"].append({"at": "29m", "action": "stop", "on": "generator",
                                               "for": "10s", "class": "clean"}),
         "`class` on a stop is for a standin service", "a class on a generator's stop"),
        (base, lambda raw: raw.update(target={"kind": "standin", "env": ["DD_API_KEY"]}),
         "target.env is for an external target", "env on a stand-in target"),
        (base, lambda raw: raw.update(target={"kind": "standin", "topic": "x"}),
         "target.topic is for a kafka target", "a topic on a stand-in target"),
        (base, lambda raw: raw.update(target={"kind": "standin", "name": "Stand In"}),
         "target.name must be lowercase", "a stand-in target name"),
        (base, lambda raw: raw["ledger"]["sink"].append({"id": "other"}),
         "a standin target has one [[ledger.sink]]", "two sinks on a stand-in target"),
        (base, lambda raw: raw["ledger"]["sink"][0].update(backend="victoria-metrics"),
         "`backend` names a backend query, and the standin target", "a backend on its sink"),
        (vm_base, lambda raw: raw["step"].append({"at": "12m", "action": "respond",
                                                  "on": "standin", "status": 503,
                                                  "for": "10s"}),
         "unknown service `standin`", "a respond with the local target"),
    ]
    paths = {id(base): path, id(vm_base): vm_path}
    for raw, edit, needle, what in cases:
        _refused(paths[id(raw)], mutate(raw, edit), needle, what)
    _refused(path, base, "reads its V from the standin service `standin`",
             "a stand-in target on a stack with no stand-in",
             roles={"logit": "sut", "generator": "generator"})
    random_raw = copy.deepcopy(base)
    for key in ("cycle", "step", "expect"):
        random_raw.pop(key, None)
    random_raw["random"] = {"seed": 1, "gap": {"min": "2m", "max": "3m"}, "fault": [
        {"weight": 1, "action": "respond", "on": "standin", "for": {"min": "10s",
                                                                   "max": "20s"}}]}
    _refused(path, random_raw, "respond takes a status or an answer, which a [random] fault "
             "can't set", "a respond in a [random] schedule")

    compose = (root / "compose.yaml").read_text()
    service = compose.split("\n  standin:")[1].split("\n  logit:")[0]
    expect(f"image: {driver.STANDIN_IMAGE}" in service and "profiles: [standin]" in service
           and "soak.role: standin" in service and 'restart: "no"' in service
           and '"127.0.0.1::8081"' in service and "./standin:/standin:ro" in service,
           "compose.yaml runs the pinned stand-in in its profile, labeled standin, its "
           "control port published on loopback")
    sut = compose.split("\n  logit:")[1].split("\n  generator:")[0]
    expect("standin: { condition: service_healthy, required: false }" in sut,
           "the SUT waits for a healthy stand-in when its profile runs")


def _standin_driver(root):
    """The driver around a stand-in: its control port re-read after a revert of a fault on it
    and after a start, and a respond's apply and revert reaching `post_cue` with the step's cue
    and an empty one."""
    loaded = scenario.load(root / "scenarios" / "standin-remote-write" / "scenario.toml")
    roles = scenario.base_roles(loaded)
    run = driver.Run(root, loaded, loaded.duration, None, False, "/nonexistent", [],
                     "logit:soak", roles=roles)

    class Ports:
        def __init__(self):
            self.port_calls = []

        def port(self, service, port):
            self.port_calls.append((service, port))
            return f"127.0.0.1:{40000 + len(self.port_calls)}"

        def inspect(self, container, timeout=None):
            return {"State": {"StartedAt": "x"}}

    run.docker = Ports()
    run.ids = {name: f"id-{name}" for name in roles}
    stop = next(s for s in run.steps if s.action == "stop")
    respond = run.steps[0]
    run.after("apply", stop, {"rc": 0})
    expect(run.docker.port_calls == [], "a stand-in's port isn't read while it is stopped")
    run.after("revert", stop, {"rc": 0})
    run.after("revert", respond, {"rc": 0})
    expect(run.docker.port_calls == [("standin", 8081), ("standin", 8081)]
           and run.standin_base["standin"] == "http://127.0.0.1:40002",
           f"the control port is re-read after each revert on the stand-in, got "
           f"{run.docker.port_calls} {run.standin_base}")
    sent = []
    real = faults.post_cue
    faults.post_cue = lambda base, cue, timeout=None: (sent.append((base, cue)), (0, ""))[1]
    try:
        ctx = faults.Context(docker=None, ids={}, netem_image="x",
                             standin_base={"standin": "http://h"})
        applied = faults.ACTIONS["respond"].apply(ctx, respond)
        reverted = faults.ACTIONS["respond"].revert(ctx, respond)
    finally:
        faults.post_cue = real
    expect(sent == [("http://h", {"step": "c0s1", "status": 503}), ("http://h", {})]
           and applied["rc"] == 0 and reverted["rc"] == 0,
           f"a respond POSTs its cue, then an empty one, got {sent}")


def _respond_fault(step, start, end, status=None, cls=None, answer=None, action="respond",
                   on="standin", series_errors=None):
    """A clean apply and revert of one step on the stand-in, offsets from `T0`."""
    extra = {k: v for k, v in (("status", status), ("class", cls), ("answer", answer),
                               ("series_errors", series_errors)) if v is not None}
    return [_action("apply", step, action, on, start, affects_egress=True, **extra),
            _action("revert", step, action, on, end - 1, end, affects_egress=True, **extra)]


def _standin_checks():
    """The checks over a stand-in two-life run: V from the record, `ledger.egress` judging it,
    the black-hole control (a 2xx the stand-in didn't store) FAILing as uncounted loss,
    `ledger.standin`'s record, request-class, cue, and class-signature rules, a `rejected` drop
    excused inside a cued step and FAILing outside one, and `ledger.sent` SKIPping under the
    `remote_write` shape."""
    with tempfile.TemporaryDirectory() as tmp:
        def results(name, edit=lambda requests: requests, **kwargs):
            run_dir = _ledger_run(tmp, name, standin_edit=edit, **kwargs)
            return {r.id: r for r in checks.run_all(run_dir)}, run_dir

        rows, _ = results("clean")
        for row in ("ledger.egress", "ledger.summary", "ledger.standin", "drops"):
            expect(rows[row].status == checks.PASS,
                   f"a stand-in run that balances PASSes {row}, got {rows[row]}")
        expect(rows["ledger.sent"].status == checks.SKIP
               and rows["ledger.topic"].status == checks.SKIP,
               f"ledger.sent and ledger.topic SKIP on a stand-in target, got "
               f"{rows['ledger.sent']} {rows['ledger.topic']}")

        rows, _ = results("windows", ledger={**LEDGER, "sink": [{"id": "sink"}],
                                             "vm_every_window": True})
        expect(rows["ledger.windows"].status != checks.SKIP,
               f"ledger.windows judges a stand-in's V under vm_every_window, got "
               f"{rows['ledger.windows']}")

        def unstore(requests):
            requests[-1]["stored"] = False
            return requests

        rows, _ = results("black-hole", unstore)
        expect(rows["ledger.egress"].status == checks.FAIL
               and "uncounted" in rows["ledger.egress"].detail,
               f"a 2xx the stand-in didn't store FAILs as uncounted loss, got "
               f"{rows['ledger.egress']}")

        # A cued rejection gives the final life a drop credit, so ledger.egress counts any gap
        # there; a black hole on the last write, answered 200 under its own cue, is then
        # ledger.standin's alone to catch.
        rejected = [_line(_stamp(T0 + 250), "logit.component.batches.dropped", "sum", 2,
                          component="sink", attrs={"reason": "rejected"})]

        def cued_hole(requests):
            requests[-1].update(stored=False, cue="c0s3")
            return requests

        timeline = (_respond_fault("c0s2", 240, 260, status=400, cls="rejected")
                    + _respond_fault("c0s3", 290, 305, status=200))
        rows, _ = results("credited-hole", cued_hole, sut_extra=rejected,
                          timeline_extra=timeline)
        expect(rows["ledger.egress"].status == checks.PASS
               and "counted" in rows["ledger.egress"].detail,
               f"the rejection's credit lets ledger.egress count the hole, got "
               f"{rows['ledger.egress']}")
        expect(rows["ledger.standin"].status == checks.FAIL
               and "answered 2xx were never stored" in rows["ledger.standin"].detail,
               f"ledger.standin FAILs a 2xx the stand-in didn't store beside a cued rejection, "
               f"got {rows['ledger.standin']}")

        rows, run_dir = results("no-record")
        (run_dir / "logs" / "standin.stdout").unlink()
        rows = {r.id: r for r in checks.run_all(run_dir)}
        expect(rows["ledger.standin"].status == checks.FAIL
               and "no stand-in record" in rows["ledger.standin"].detail
               and rows["ledger.egress"].status == checks.FAIL,
               f"a missing record FAILs ledger.standin and leaves V empty, got "
               f"{rows['ledger.standin']} {rows['ledger.egress']}")

        def changed(requests):
            body = standin.write_request(standin.snappy_decompress(
                base64.b64decode(requests[0]["body_b64"])))
            body[0][1][0] = (body[0][1][0][0], body[0][1][0][1] + 1)
            return requests + [_standin_request(99, 60, body)]

        rows, _ = results("changed", changed)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "a resend changed a sample" in rows["ledger.standin"].detail,
               f"a resend storing a different value FAILs, got {rows['ledger.standin']}")

        def garbled(requests):
            requests[0]["body_b64"] = base64.b64encode(b"\x05nope").decode()
            return requests

        rows, _ = results("garbled", garbled)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "don't decode" in rows["ledger.standin"].detail,
               f"a body that doesn't decode FAILs, got {rows['ledger.standin']}")

        # A request the stand-in recorded that the sink never counted: the telemetry is
        # written from the edited list, so drop its line by recording one more afterwards.
        run_dir = _ledger_run(tmp, "uncounted-request", standin_edit=lambda r: r)
        requests = standin.read_record((run_dir / "logs" / "standin.stdout").read_text())[0]
        _write_standin(run_dir, requests + [_standin_request(50, 250, [], status=503,
                                                             stored=False, cue="c9s9")])
        rows = {r.id: r for r in checks.run_all(run_dir)}
        detail = rows["ledger.standin"].detail + " ".join(rows["ledger.standin"].lines)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "answered 1 request(s) 5xx, sink counted 0" in detail
               and "which no respond step applied cleanly" in detail,
               f"a request the sink didn't count, under an unknown cue, FAILs, got "
               f"{rows['ledger.standin']}")

        def reset_uncounted(requests):
            return requests + [_standin_request(51, 250, [], answer="reset", stored=False,
                                                cue="c0s2")]

        run_dir = _ledger_run(tmp, "reset", standin_edit=lambda r: r,
                              timeline_extra=_respond_fault("c0s2", 240, 260, answer="reset"))
        requests = standin.read_record((run_dir / "logs" / "standin.stdout").read_text())[0]
        _write_standin(run_dir, reset_uncounted(requests))
        rows = {r.id: r for r in checks.run_all(run_dir)}
        expect(rows["ledger.standin"].status == checks.FAIL
               and "reset 1 request(s), sink counted 0 network_error"
               in rows["ledger.standin"].detail,
               f"a reset the sink didn't count as network_error FAILs, got "
               f"{rows['ledger.standin']}")
        with open(run_dir / "logs" / "logit.stdout", "a") as out:
            out.write("\n".join(_line(_stamp(T0 + o), "logit.output.requests", "sum", 1,
                                      component="sink", attrs={"class": "network_error"})
                                for o in (250, 255)) + "\n")
        rows = {r.id: r for r in checks.run_all(run_dir)}
        expect(rows["ledger.standin"].status == checks.PASS,
               f"network_error at or above the resets PASSes (a connect failure counts there "
               f"too), got {rows['ledger.standin']}")

        def outside(requests):
            requests[-1]["cue"] = "c0s2"
            requests[-1]["status"] = 503
            requests[-1]["stored"] = True
            return requests

        rows, _ = results("outside-span", outside,
                          timeline_extra=_respond_fault("c0s2", 240, 260, status=503))
        detail = " ".join(rows["ledger.standin"].lines)
        expect(rows["ledger.standin"].status == checks.FAIL and "outside its span" in detail,
               f"an answer under a cue outside its step's span FAILs, got "
               f"{rows['ledger.standin']}")

        def uncued(requests):
            requests[-1]["status"] = 503
            return requests

        rows, _ = results("uncued", uncued)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "with no cue" in " ".join(rows["ledger.standin"].lines),
               f"a non-default answer with no cue FAILs, got {rows['ledger.standin']}")


def _class_signatures():
    """`ledger.standin`'s check of each classed step, and the cued `rejected` excusal in
    `ledger.egress`, `ledger.summary`, and `drops`, each against a run with the step and one
    without it."""
    with tempfile.TemporaryDirectory() as tmp:
        def log(offset, message, key="retrying", component="sink", level="ERROR"):
            return (offset, level, message, {"key": key, "component": component})

        def run(name, cls, stderr=(), drops=(), status=503, answer=None, sut_config=None,
                action="respond"):
            extra = [_line(_stamp(T0 + offset), "logit.component.batches.dropped", "sum", n,
                           component="sink", attrs={"reason": reason})
                     for offset, reason, n in drops]
            timeline = _respond_fault("c0s2", 240, 260, status=None if answer else status,
                                      cls=cls, answer=answer, action=action)
            run_dir = _ledger_run(tmp, name, standin_edit=lambda r: r, sut_extra=extra,
                                  stderr_extra=stderr, timeline_extra=timeline)
            if sut_config is not None:
                (run_dir / "configs").mkdir(exist_ok=True)
                (run_dir / "configs" / "sut.yaml").write_text(sut_config)
                resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
                resolved["configs"] = {"sut": "sut.yaml"}
                (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
            return {r.id: r for r in checks.run_all(run_dir)}

        held = [log(241, "send failed (ambiguous): 503: busy; retrying until it succeeds")]
        rows = run("ambiguous", "ambiguous", held)
        expect(rows["ledger.standin"].status == checks.PASS,
               f"an ambiguous step with a retrying (ambiguous) line PASSes, got "
               f"{rows['ledger.standin']}")
        rows = run("ambiguous-as-refused", "ambiguous",
                   [log(241, "send failed (refused): 503: busy; retrying until it succeeds")])
        expect(rows["ledger.standin"].status == checks.FAIL
               and "expected retrying (ambiguous) and no drop" in rows["ledger.standin"].detail,
               f"an ambiguous step the sink read as refused FAILs, got {rows['ledger.standin']}")
        rows = run("refused-silent", "refused", status=401)
        expect(rows["ledger.standin"].status == checks.FAIL,
               f"a refused step with no retrying line FAILs, got {rows['ledger.standin']}")
        rows = run("refused-dropped", "refused",
                   [log(241, "send failed (refused): 401: no; retrying until it succeeds")],
                   drops=[(250, "rejected", 1)], status=401)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "dropped rejected 1" in rows["ledger.standin"].detail,
               f"a refused step that dropped a batch FAILs, got {rows['ledger.standin']}")
        rows = run("clean", "clean",
                   [log(241, "send failed (clean): connect error; retrying until it succeeds")],
                   action="stop")
        expect(rows["ledger.standin"].status == checks.PASS,
               f"a stop of the stand-in read clean PASSes, got {rows['ledger.standin']}")

        rejected = [(250, "rejected", 2)]
        rows = run("rejected", "rejected", drops=rejected, status=400)
        expect(rows["ledger.standin"].status == checks.PASS,
               f"a rejected step that dropped a batch PASSes, got {rows['ledger.standin']}")
        for row in ("ledger.egress", "ledger.summary", "drops"):
            expect(rows[row].status == checks.PASS,
                   f"a rejected drop inside a cued rejected step doesn't FAIL {row}, got "
                   f"{rows[row]}")
        expect("rejected 2 cued by c0s2, counted" in rows["ledger.egress"].detail,
               f"ledger.egress names the cued drops, got {rows['ledger.egress'].detail}")
        rows = run("rejected-held", "rejected", held, status=400)
        expect(rows["ledger.standin"].status == checks.FAIL,
               f"a rejected step the sink retried FAILs, got {rows['ledger.standin']}")
        late = [(260 + 10 + 45 + 5, "rejected", 1)]
        rows = run("rejected-late", "rejected", drops=rejected + late, status=400)
        for row in ("ledger.egress", "ledger.summary"):
            expect(rows[row].status == checks.FAIL and "1 batch(es) rejected" in rows[row].detail,
                   f"a rejected drop past the step's window FAILs {row}, got {rows[row]}")
        rows = run("rejected-uncued", "refused", drops=rejected, status=401,
                   stderr=[log(241, "send failed (refused): 401: no; retrying")])
        expect(rows["ledger.egress"].status == checks.FAIL,
               f"a rejected drop inside a step that expects another class FAILs ledger.egress, "
               f"got {rows['ledger.egress']}")

        amo = ("components:\n  window: { type: aggregate, interval: 10s }\n"
               "  sink:\n    type: prometheus_out\n    buffer: { delivery: at_most_once }\n")
        rows = run("amo", "ambiguous", drops=[(250, "ambiguous_at_most_once", 1)],
                   sut_config=amo)
        expect(rows["ledger.standin"].status == checks.PASS
               and "at_most_once" in rows["ledger.standin"].detail,
               f"under at_most_once an ambiguous step drops, got {rows['ledger.standin']}")
        rows = run("amo-held", "ambiguous", held, sut_config=amo)
        expect(rows["ledger.standin"].status == checks.FAIL,
               f"under at_most_once an ambiguous step that retried FAILs, got "
               f"{rows['ledger.standin']}")


def _datadog_codec(root):
    """The `datadog` shape's decoders: a series body `datadog_out` sent, from
    `standin/datadog-vectors.json`, through gzip and the `MetricPayload` walker; deflate and no
    compression; the fixture encoder's round trip; and the bodies and encodings refused."""
    vectors = json.loads((root / "standin" / "datadog-vectors.json").read_text())
    raw = bytes.fromhex(vectors["datadog_out_series_body"])
    want = vectors["datadog_out_series_body_expect"]["series"]
    decoded = standin.metric_payload(standin.decompress(raw, vectors["content_encoding"]))
    got = [{"metric": one["metric"], "resources": [list(r) for r in one["resources"]],
            "tags": one["tags"], "points": [list(p) for p in one["points"]],
            "type": one["type"], "interval": one["interval"]} for one in decoded]
    expect(vectors["content_encoding"] == "gzip" and got == want and len(got) == 5
           and all(one["type"] == "count" and one["interval"] == 2
                   and one["resources"] == [["host", "logit-soak"]] for one in got),
           f"a gzip series body datadog_out sent decodes to its 5 count series, got {got[:1]}")
    request = {"body_b64": base64.b64encode(raw).decode(), "content_encoding": "gzip"}
    body = standin.decode_body(request, standin.DATADOG)
    expect(len(body) == 5 and body[0][0] == {"__name__": want[0]["metric"], "type": "count",
                                             "tags": "", "resources": "host:logit-soak"}
           and body[0][1] == [(want[0]["points"][0][0] * 1000, want[0]["points"][0][1])],
           f"decode_body names a datadog series by its name, type, tags, and resources, "
           f"timestamps in ms, got {body[:1]}")
    series = [{"metric": "m.b", "resources": [("host", "h"), ("device", "sda")],
               "tags": ["env:x", "a"], "points": [(1791720000, 1.5), (-3, -2.0)],
               "type": "gauge", "interval": 0},
              {"metric": "m.a", "resources": [], "tags": [], "points": [], "type": "rate",
               "interval": 20}]
    payload = standin.encode_metric_payload(series)
    expect(standin.metric_payload(payload) == series,
           "the fixture encoder round-trips every field, a negative timestamp as an int64")
    for encoding, data in (("gzip", gzip.compress(payload)), ("deflate", zlib.compress(payload)),
                           (None, payload)):
        expect(standin.decompress(data, encoding) == payload,
               f"a {encoding or 'plain'} body decompresses")
    labels = standin.datadog_labels(series[0])
    expect(labels["tags"] == "a,env:x" and labels["resources"] == "device:sda,host:h",
           f"a series' labels sort its tags and resources, got {labels}")
    for data, encoding, needle in ((payload, "gzip", "gzip"), (b"\x78\x9c", "deflate",
                                                               "deflate"),
                                   (payload, "zstd", "not gzip, deflate, or none"),
                                   (b"\x0a\x05\x12", None, "truncated")):
        try:
            standin.decode_body({"body_b64": base64.b64encode(data).decode(),
                                 "content_encoding": encoding}, standin.DATADOG)
            expect(False, f"a {encoding} body {data[:4]!r} decoded")
        except standin.DecodeError as err:
            expect(needle in str(err), f"a bad {encoding} body names {needle!r}, got {err}")


def _datadog_counts():
    """`stored_counts` over a `datadog` record: V sums distinct copies with no last write
    winning, a same-value copy is a duplicate naming both requests, a changed value is a
    conflict and still summed, a `series_errors` answer's first N series are left out and
    named, an unstored or undecodable request is left out of V; and `acknowledged_unstored`
    leaves the named series out."""
    requests = [
        _datadog_request(1, 10, [("a", [(1000, 1.0)]), ("b", [(1000, 5.0)])]),
        _datadog_request(2, 11, [("a", [(1000, 1.0)])], status=503, cue="c0s1"),
        _datadog_request(3, 12, [("b", [(1000, 6.0)])]),
        _datadog_request(4, 13, [("a", [(2000, 3.0)])], status=400, stored=False),
        _datadog_request(5, 14, [("c", [(3000, 7.0)]), ("a", [(3000, 2.0)])],
                         series_errors=1, cue="c0s2"),
        {**_datadog_request(6, 15, [("a", [(4000, 4.0)])]), "body_b64": "!!"},
        _datadog_request(7, 16, [("a", [(5000, 9.0)])], encoding="deflate"),
    ]
    counts = standin.stored_counts(requests)
    a = standin.series_key(standin.datadog_labels({"metric": "a", "type": "count", "tags": [],
                                                   "resources": [("host", "logit-soak")]}))
    b = standin.series_key(dict(a) | {"__name__": "b"})
    expect(sorted(counts.points) == [(1000, 1.0), (1000, 5.0), (1000, 6.0), (3000, 2.0),
                                     (5000, 9.0)],
           f"V's terms are every distinct stored copy, a changed value summed beside the "
           f"first, got {counts.points}")
    expect(counts.duplicates == [(a, 1000000, 1.0, 0, 1)]
           and counts.conflicts == [(b, 1000000, 5.0, 6.0)],
           f"a same-value copy is a duplicate naming both requests and a changed one a "
           f"conflict, got {counts.duplicates} {counts.conflicts}")
    expect(counts.named == [(5, 1, T0 + 14)] and [s for s, _ in counts.undecodable] == [6],
           f"a series_errors answer names its first series and a bad body is listed, got "
           f"{counts.named} {counts.undecodable}")
    b_series = next(one for one in counts.series if one["metric"]["__name__"] == "b")
    expect(b_series["values"] == [11.0] and not any(
        one["metric"]["__name__"] == "c" for one in counts.series),
           f"the export sums a point's copies and holds no named series, got {counts.series}")
    lost = standin.acknowledged_unstored(requests, counts.series, standin.DATADOG)
    expect(lost == [], f"every 2xx point is stored but the named one, got {lost}")
    counts.series = [one for one in counts.series if one["metric"]["__name__"] != "a"]
    lost = standin.acknowledged_unstored(requests, counts.series, standin.DATADOG)
    expect(sorted(stamp for _, stamp in lost) == [1000000, 3000000, 5000000],
           f"a 2xx point missing from V is named, got {lost}")
    expect(standin.points_in(requests[4], standin.DATADOG) == 2
           and standin.points_in(requests[5], standin.DATADOG) == 0,
           "points_in counts a body's points, and 0 for one that doesn't decode")


def _datadog_server(root):
    """The stand-in's `datadog` shape on loopback ports: the `series_errors` cue rules, the
    default `202` on `/api/v2/series`, and a `series_errors` answer whose body `datadog_out`'s
    `series_errors` reads as N entries, recorded with the count."""
    import http.client
    import http.server
    import io
    import threading
    server = _load_standin_server(root)
    for cue, shape, needle in (
            ({"step": "x", "status": 202, "series_errors": 2}, "remote_write",
             "for the datadog shape"),
            ({"step": "x", "status": 202, "series_errors": 0}, "datadog", "positive integer"),
            ({"step": "x", "status": 202, "series_errors": True}, "datadog", "positive integer"),
            ({"step": "x", "status": 202, "series_errors": 1, "body": "x"}, "datadog",
             "takes no `body`"),
            ({"step": "x", "status": 400, "series_errors": 1}, "datadog", "2xx `status`"),
            ({"step": "x", "answer": "reset", "series_errors": 1}, "datadog", "2xx `status`")):
        problems = server.cue_problems(cue, shape)
        expect(any(needle in p for p in problems), f"cue {cue} under {shape} refused with "
               f"{needle!r}, got {problems}")
    expect(server.cue_problems({"step": "x", "status": 202, "series_errors": 3}, "datadog") == [],
           "a 202 with series_errors under the datadog shape is accepted")

    def errors(body):
        """`datadog_out`'s `series_errors`: field 1 entries, stopping at anything else."""
        found, pos = [], 0
        while pos < len(body):
            key, pos = standin._varint(body, pos)
            if key != (1 << 3 | 2):
                break
            size, pos = standin._varint(body, pos)
            found.append(body[pos:pos + size].decode())
            pos += size
        return found

    state = server.State("datadog", "/api/v2/series")
    data = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.data_handler(state))
    control = http.server.ThreadingHTTPServer(("127.0.0.1", 0), server.control_handler(state))
    for one in (data, control):
        threading.Thread(target=one.serve_forever, daemon=True).start()
    printed = io.StringIO()
    real_stdout = server.sys.stdout
    server.sys.stdout = printed
    control_base = f"http://127.0.0.1:{control.server_address[1]}"

    def post(path):
        conn = http.client.HTTPConnection("127.0.0.1", data.server_address[1], timeout=5)
        try:
            conn.request("POST", path, body=b"payload", headers={"Content-Encoding": "gzip"})
            response = conn.getresponse()
            return (response.status, response.getheader("Content-Type"), response.read())
        finally:
            conn.close()

    try:
        first = post("/api/v2/series")
        rc, _ = faults.post_cue(control_base, {"step": "c0s1", "status": 202,
                                               "series_errors": 2})
        second = post("/api/v2/series")
        rc_bad, err = faults.post_cue(control_base, {"step": "c0s2", "status": 400,
                                                     "series_errors": 1})
        faults.post_cue(control_base, {})
        third = post("/api/v1/write")
        deadline = time.monotonic() + 5
        while (printed.getvalue().count('"event": "request"') < 3
               and time.monotonic() < deadline):
            time.sleep(0.01)
    finally:
        server.sys.stdout = real_stdout
        for one in (data, control):
            one.shutdown()
            one.server_close()
    expect(first[0] == 202 and first[2] == b"" and second[0] == 202
           and second[1] == "application/x-protobuf" and len(errors(second[2])) == 2
           and third[0] == 404,
           f"the datadog shape answers 202, then a body naming 2 series, and 404 off its "
           f"path, got {[first, second, third]}")
    expect(rc == 0 and rc_bad == 1 and "2xx `status`" in err,
           f"the control port refuses series_errors on a 400, got {rc} {rc_bad} {err}")
    requests = [json.loads(line) for line in printed.getvalue().splitlines()
                if '"event": "request"' in line]
    got = [(r["status"], r["stored"], r.get("series_errors")) for r in requests]
    expect(got == [(202, True, None), (202, True, 2), (404, False, None)],
           f"the record carries series_errors on the answer that named series, got {got}")


def _datadog_rules(root):
    """`[target] shape`, the `series_errors` key, and the `datadog` shape's `class` rules;
    `standin-datadog` loading with its shape, cue, and compose variable."""
    path = root / "scenarios" / "standin-datadog" / "scenario.toml"
    base = tomllib.loads(path.read_text())
    rw_path, rw_base = _standin_base(root)
    vm_path, vm_base = _base(root)
    loaded = scenario.load(path)
    expect(loaded.standin_shape == "datadog"
           and loaded.target == {"kind": "standin", "name": "standin", "env": [],
                                 "shape": "datadog"}
           and scenario.target_json(loaded.target)["shape"] == "datadog",
           f"standin-datadog loads as a datadog stand-in, got {loaded.target}")
    first = scenario.expand(loaded)[0]
    expect(first.cue() == {"step": "c0s1", "status": 202, "series_errors": 2}
           and first.expected() == {"status": 202, "series_errors": 2},
           f"series_errors travels in the cue and the timeline, got {first.cue()} "
           f"{first.expected()}")
    expect(scenario.load(rw_path).standin_shape == "remote_write"
           and scenario.load(vm_path).standin_shape is None,
           "a stand-in target's shape defaults to remote_write, and other targets have none")
    compose = (root / "compose.yaml").read_text()
    expect(f'"${{{scenario.STANDIN_SHAPE_ENV}:-remote_write}}"' in compose,
           "compose.yaml passes the shape's compose variable to standin.py")

    def mutate(raw, edit):
        raw = copy.deepcopy(raw)
        edit(raw)
        return raw

    def step(index, **changes):
        def edit(raw):
            for key in [k for k, v in changes.items() if v is None]:
                raw["step"][index].pop(key, None)
            raw["step"][index].update({k: v for k, v in changes.items() if v is not None})
        return edit

    cases = [
        (base, lambda raw: raw["target"].update(shape="splunk"),
         "target.shape must be one of remote_write, datadog", "an unknown shape"),
        (vm_base, lambda raw: raw.update(target={"kind": "victoria-metrics",
                                                 "shape": "datadog"}),
         "target.shape is for a standin target", "a shape on the local target"),
        (rw_base, step(0, status=202, series_errors=1, **{"class": None}),
         "`series_errors` is for a standin target with shape 'datadog'",
         "series_errors under the remote_write shape"),
        (base, step(0, series_errors=0), "`series_errors` must be a positive integer",
         "a zero series_errors"),
        (base, step(0, series_errors="2"), "`series_errors` must be a positive integer",
         "a string series_errors"),
        (base, step(0, body="x"), "`series_errors` writes the body", "a body beside it"),
        (base, step(1, series_errors=1), "`series_errors` goes with a 2xx `status`",
         "series_errors on a 400"),
        (base, step(15, series_errors=1), "`series_errors` is for respond only",
         "series_errors on a stop"),
        (base, step(1, **{"class": None}), "a respond other than a 2xx needs `class`",
         "a 400 with no class"),
        (base, step(14, **{"class": None}), "a respond other than a 2xx needs `class`",
         "a reset with no class"),
        (base, step(0, **{"class": "ambiguous"}), "a 2xx isn't a fault, so it takes no `class`",
         "a class on a 202"),
    ]
    paths = {id(base): path, id(rw_base): rw_path, id(vm_base): vm_path}
    for raw, edit, needle, what in cases:
        _refused(paths[id(raw)], mutate(raw, edit), needle, what)
    remote = copy.deepcopy(rw_base)
    remote["step"][11]["status"] = 501
    remote["step"][11].pop("class", None)
    expect(scenario.from_dict(remote, rw_path).steps[11].fault_class is None,
           "the remote_write shape doesn't require a class on a 501")
    # The scenario's class is the expectation the checks judge against the sink, so the
    # loader takes any class word on a fault.
    stated = mutate(base, step(1, **{"class": "refused"}))
    expect(scenario.from_dict(stated, path).steps[1].fault_class == "refused",
           "the datadog shape loads a 400 classed refused, for the checks to judge")


def _datadog_checks():
    """The checks over a `datadog` stand-in two-life run: V as the sum of distinct points,
    `ledger.sent` judging the sink with each cued refusal, rejected batch, and rejected or
    oversize record excused inside a matching step and FAILing outside one, `drops` leaving the
    sink's records to it, duplicates reported under `at_least_once` and FAILing under
    `at_most_once` or after a 2xx, a changed resend summed into a surplus, the `series_errors`
    and rejected-status reconciliation against `records.dropped`, and each request's class
    against `datadog_out`'s table."""
    with tempfile.TemporaryDirectory() as tmp:
        def log(offset, message, key, component="sink", level="WARN"):
            return (offset, level, message, {"key": key, "component": component})

        def drop(offset, metric, reason, n, received=False):
            out = [_line(_stamp(T0 + offset), metric, "sum", n, component="sink",
                         attrs={"reason": reason, "route": "series"})]
            if received:
                out.append(_line(_stamp(T0 + offset), "logit.component.batches.received",
                                 "sum", n, component="sink"))
            return out

        def results(name, edit=lambda requests: requests, sut_config=None, **kwargs):
            run_dir = _ledger_run(tmp, name, standin_edit=edit, standin_shape="datadog",
                                  **kwargs)
            if sut_config is not None:
                (run_dir / "configs").mkdir(exist_ok=True)
                (run_dir / "configs" / "sut.yaml").write_text(sut_config)
                resolved = json.loads((run_dir / "scenario.resolved.json").read_text())
                resolved["configs"] = {"sut": "sut.yaml"}
                (run_dir / "scenario.resolved.json").write_text(json.dumps(resolved))
            return {r.id: r for r in checks.run_all(run_dir)}

        rows = results("dd-clean")
        for row in ("ledger.egress", "ledger.summary", "ledger.standin", "ledger.sent",
                    "drops"):
            expect(rows[row].status == checks.PASS,
                   f"a datadog stand-in run that balances PASSes {row}, got {rows[row]}")
        expect(not rows["ledger.sent"].detail.startswith("SENT")
               and "ledger.egress and ledger.standin read the stand-in's record"
               in rows["ledger.sent"].detail,
               f"ledger.sent on a stand-in names the record as the rest of the evidence, got "
               f"{rows['ledger.sent'].detail}")
        expect("0 duplicate copies" in rows["ledger.standin"].detail
               and "the sum of the distinct points' values" in " ".join(
                   rows["ledger.egress"].lines),
               f"V is the distinct points' sum, with no duplicate, got "
               f"{rows['ledger.standin'].detail}")

        # A 503 after storing (`apply`), resent: the copy is a duplicate.
        def resent(requests):
            again = copy.deepcopy(requests[-1])
            requests[-1].update(status=503, cue="c0s2")
            again.update(seq=99, t=again["t"] + 0.5)
            return requests + [again]

        applied = _respond_fault("c0s2", 290, 305, status=503)
        rows = results("dd-duplicate", resent, timeline_extra=applied)
        expect(rows["ledger.standin"].status == checks.PASS
               and "2 duplicate copies of stored points (life 1 2)"
               in rows["ledger.standin"].detail
               and "which at_least_once permits" in " ".join(rows["ledger.standin"].lines),
               f"a resend after an applied 503 is a duplicate at_least_once permits, reported, "
               f"got {rows['ledger.standin']}")
        expect(rows["ledger.egress"].status == checks.PASS,
               f"a duplicate copy isn't summed into V, got {rows['ledger.egress']}")
        amo = ("components:\n  window: { type: aggregate, interval: 10s }\n"
               "  sink:\n    type: datadog_out\n    buffer: { delivery: at_most_once }\n")
        rows = results("dd-duplicate-amo", resent, timeline_extra=applied, sut_config=amo)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "a resend under at_most_once" in rows["ledger.standin"].detail,
               f"a duplicate under at_most_once FAILs, got {rows['ledger.standin']}")

        def resent_accepted(requests):
            again = copy.deepcopy(requests[-1])
            again.update(seq=99, t=again["t"] + 0.5)
            return requests + [again]

        rows = results("dd-duplicate-2xx", resent_accepted)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "the sink resent a point it saw accepted" in rows["ledger.standin"].detail,
               f"a resend of a point the stand-in answered 2xx FAILs, got "
               f"{rows['ledger.standin']}")

        # A 2xx in life 0 sent again in life 1: a new SUT life resends what its spool held.
        def resent_next_life(requests):
            again = copy.deepcopy(requests[0])
            again.update(seq=99, t=T0 + 250)
            return requests + [again]

        rows = results("dd-duplicate-next-life", resent_next_life)
        expect(rows["ledger.standin"].status == checks.PASS
               and "2 duplicate copies of stored points (life 0 2)"
               in rows["ledger.standin"].detail,
               f"a 2xx point resent in a later SUT life is a reported duplicate, got "
               f"{rows['ledger.standin']}")

        # The stand-in restarts after the applied 503, and its request numbers start again at
        # 1, so a later 2xx carries the 503's number. The resend must still read as one.
        def restarted(requests):
            requests = resent(requests)
            applied_seq = requests[-2]["seq"]
            requests[-1]["seq"] = 1
            requests += [_datadog_request(seq, 301 + seq, []) for seq in
                         range(2, applied_seq + 1)]
            return requests

        rows = results("dd-duplicate-restarted", restarted, timeline_extra=applied)
        expect(rows["ledger.standin"].status == checks.PASS
               and "2 duplicate copies of stored points (life 1 2)"
               in rows["ledger.standin"].detail,
               f"a resend after a stand-in restart reuses request numbers and is still a "
               f"permitted duplicate, got {rows['ledger.standin']}")

        def changed(requests):
            again = copy.deepcopy(requests[-1])
            body = standin.metric_payload(gzip.decompress(base64.b64decode(again["body_b64"])))
            body[0]["points"] = [(stamp, value + 7) for stamp, value in body[0]["points"]]
            payload = gzip.compress(standin.encode_metric_payload(body), mtime=0)
            again.update(seq=99, t=again["t"] + 0.5,
                         body_b64=base64.b64encode(payload).decode())
            return requests + [again]

        rows = results("dd-changed", changed)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "a resend changed a sample" in rows["ledger.standin"].detail
               and rows["ledger.egress"].status == checks.FAIL
               and "surplus" in rows["ledger.egress"].detail,
               f"a changed resend is summed, no last write winning: a surplus, got "
               f"{rows['ledger.standin']} {rows['ledger.egress']}")

        # A 202 naming the request's first series, at +299 under its cue.
        def named(requests):
            requests[-1].update(series_errors=1, cue="c0s2")
            return requests

        cued_named = _respond_fault("c0s2", 290, 305, status=202, series_errors=1)
        rows = results("dd-named", named, timeline_extra=cued_named,
                       sut_extra=drop(300, "logit.output.records.dropped", "rejected", 1))
        for row in ("ledger.standin", "ledger.egress", "ledger.summary", "ledger.sent",
                    "drops"):
            expect(rows[row].status == checks.PASS,
                   f"a cued series_errors drop counted by the sink PASSes {row}, got {rows[row]}")
        expect("records dropped: rejected 1" in rows["ledger.egress"].detail
               and "record(s) dropped rejected 1 by c0s2" in rows["ledger.sent"].detail
               and "1 series named by series_errors" in rows["ledger.standin"].detail,
               f"the named series are counted, cued, and reported, got "
               f"{rows['ledger.egress'].detail} | {rows['ledger.sent'].detail}")
        rows = results("dd-named-uncounted", named, timeline_extra=cued_named)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "counted 0 record(s) dropped rejected, the record holds 1"
               in rows["ledger.standin"].detail,
               f"a named series the sink didn't count FAILs, got {rows['ledger.standin']}")
        rows = results("dd-named-uncued", named,
                       sut_extra=drop(300, "logit.output.records.dropped", "rejected", 1))
        expect(rows["ledger.sent"].status == checks.FAIL
               and "1 record(s) dropped rejected, refused" in rows["ledger.sent"].detail
               and rows["drops"].status == checks.PASS,
               f"an uncued record drop FAILs ledger.sent and stays out of drops, got "
               f"{rows['ledger.sent']} {rows['drops']}")

        # A 400 rejecting the last request, inside a cued rejected step and outside one.
        def rejected(requests, status=400, cue="c0s2"):
            requests[-1].update(status=status, stored=False, cue=cue)
            return requests

        def rejected_extra(reason="rejected"):
            return (drop(300, "logit.output.records.dropped", reason, 2)
                    + drop(300, "logit.component.batches.dropped", "rejected", 1, received=True))

        held = log(299, "send failed (rejected): 400", "retrying", level="ERROR")
        rejection = log(299.5, "https://standin/api/v2/series answered 400, 2 record(s) "
                        "dropped: bad", "request_rejected")
        cued_400 = _respond_fault("c0s2", 290, 305, status=400, cls="rejected")
        rows = results("dd-rejected", rejected, timeline_extra=cued_400,
                       sut_extra=rejected_extra(), stderr_extra=[rejection])
        for row in ("ledger.standin", "ledger.sent", "ledger.egress", "drops"):
            expect(rows[row].status == checks.PASS,
                   f"a cued 400 PASSes {row}, got {rows[row]}")
        expect("batch(es) dropped rejected 1 by c0s2" in rows["ledger.sent"].detail
               and "record(s) dropped rejected 2 by c0s2" in rows["ledger.sent"].detail,
               f"ledger.sent names the cued drops, got {rows['ledger.sent'].detail}")
        rows = results("dd-rejected-uncued", rejected,
                       sut_extra=rejected_extra(), stderr_extra=[rejection],
                       timeline_extra=_respond_fault("c0s2", 290, 305, status=400))
        expect(rows["ledger.sent"].status == checks.FAIL
               and "1 batch(es) dropped rejected, the destination refused them"
               in rows["ledger.sent"].detail,
               f"a rejected batch inside a step with no class FAILs ledger.sent, got "
               f"{rows['ledger.sent']}")
        rows = results("dd-rejected-held", rejected, timeline_extra=cued_400,
                       sut_extra=rejected_extra(), stderr_extra=[rejection, held])
        expect(rows["ledger.standin"].status == checks.FAIL,
               f"a 400 the sink retried FAILs its signature, got {rows['ledger.standin']}")
        rows = results("dd-rejected-uncounted", rejected, timeline_extra=cued_400,
                       sut_extra=drop(300, "logit.component.batches.dropped", "rejected", 1,
                                      received=True))
        expect(rows["ledger.standin"].status == checks.FAIL
               and "counted 0 record(s) dropped rejected, the record holds 2"
               in rows["ledger.standin"].detail,
               f"a 400's points the sink didn't count dropped FAIL, got "
               f"{rows['ledger.standin']}")

        cued_413 = _respond_fault("c0s2", 290, 305, status=413, cls="rejected")
        rows = results("dd-oversize", lambda r: rejected(r, 413), timeline_extra=cued_413,
                       sut_extra=rejected_extra("oversize"))
        expect(rows["ledger.sent"].status == checks.PASS
               and rows["ledger.standin"].status == checks.PASS
               and "record(s) dropped oversize 2 by c0s2" in rows["ledger.sent"].detail,
               f"a cued 413's oversize records are excused, got {rows['ledger.sent']} "
               f"{rows['ledger.standin']}")
        rows = results("dd-oversize-in-400", rejected, timeline_extra=cued_400,
                       sut_extra=rejected_extra("oversize"))
        expect(rows["ledger.sent"].status == checks.FAIL
               and "2 record(s) dropped oversize" in rows["ledger.sent"].detail
               and rows["ledger.standin"].status == checks.FAIL,
               f"oversize records inside a 400's step aren't its doing, got "
               f"{rows['ledger.sent']} {rows['ledger.standin']}")

        # A refusal: the sink holds, logs its class and the refusal line, and resends.
        def refused(requests, status=403):
            first = copy.deepcopy(requests[-1])
            first.update(status=status, stored=False, cue="c0s2", seq=98)
            requests[-1]["t"] += 0.5
            return requests[:-1] + [first, requests[-1]]

        key_line = log(299.2, "Datadog refused the API key: https://standin/api/v2/series "
                       "answered 403 -- check 'api_key'", "api_key_rejected")
        retrying = log(299.1, "send failed (refused): 403; retrying until it succeeds",
                       "retrying", level="ERROR")
        cued_403 = _respond_fault("c0s2", 290, 305, status=403, cls="refused")
        rows = results("dd-refused", refused, timeline_extra=cued_403,
                       stderr_extra=[retrying, key_line])
        expect(rows["ledger.sent"].status == checks.PASS
               and "api_key_rejected line(s) 1 by c0s2" in rows["ledger.sent"].detail
               and rows["ledger.standin"].status == checks.PASS,
               f"a cued 403's refusal line is excused, got {rows['ledger.sent']} "
               f"{rows['ledger.standin']}")
        late = log(250, "https://standin/api/v2/series answered 401: no", "request_refused")
        rows = results("dd-refused-uncued", refused, timeline_extra=cued_403,
                       stderr_extra=[retrying, key_line, late])
        expect(rows["ledger.sent"].status == checks.FAIL
               and "1 request_refused line(s) (status 401" in rows["ledger.sent"].detail,
               f"a refusal line outside every cued refused step FAILs, got "
               f"{rows['ledger.sent']}")
        rows = results("dd-refused-as-ambiguous", refused, stderr_extra=[retrying, key_line],
                       timeline_extra=_respond_fault("c0s2", 290, 305, status=403,
                                                     cls="ambiguous"))
        detail = " ".join(rows["ledger.standin"].lines)
        expect(rows["ledger.standin"].status == checks.FAIL
               and "c0s2 (respond on standin, 403): ambiguous: retrying (refused), dropped 0; "
               "expected retrying (ambiguous) and no drop" in detail
               and rows["ledger.sent"].status == checks.FAIL,
               f"a 403 the sink logged refused under a step classed ambiguous FAILs its "
               f"signature and isn't excused, got "
               f"{rows['ledger.standin']} {rows['ledger.sent']}")
        rows = results("dd-501", lambda r: refused(r, 501), timeline_extra=_respond_fault(
            "c0s2", 290, 305, status=501, cls="ambiguous"),
            stderr_extra=[log(299.1, "send failed (ambiguous): 501; retrying", "retrying",
                              level="ERROR")])
        expect(rows["ledger.standin"].status == checks.PASS,
               f"a 501 the sink read as ambiguous PASSes, got {rows['ledger.standin']}")
        rows = results("dd-501-refused", lambda r: refused(r, 501), timeline_extra=_respond_fault(
            "c0s2", 290, 305, status=501, cls="ambiguous"),
            stderr_extra=[log(299.1, "send failed (refused): 501; retrying", "retrying",
                              level="ERROR")])
        expect(rows["ledger.standin"].status == checks.FAIL
               and "expected retrying (ambiguous)" in rows["ledger.standin"].detail,
               f"a 501 the sink read as refused FAILs its signature, got "
               f"{rows['ledger.standin']}")


def _shipped(root):
    paths = scenario.shipped(root)
    expect(bool(paths), "at least one shipped scenario")
    for path in paths:
        try:
            loaded = scenario.load(path)
            expect(loaded.name == path.parent.name,
                   f"{path}: name {loaded.name!r} matches its directory")
            # The services an override adds are read from its labels, as compose would merge
            # them; `run` checks against compose's own answer before the stack starts.
            scenario.check_services(loaded, _override_roles(loaded))
        except scenario.ScenarioError as err:
            expect(False, f"{path}: {err}")


def run(root, quiet=False):
    """Returns 0 when every expectation holds, else prints the failures and returns 1."""
    root = Path(root)
    _FAILURES.clear()
    _PASSED[0] = 0
    # `soak.py run` goes on to the real run after this, so the caller's `$DOCKER` comes back.
    previous = os.environ.get("DOCKER")
    os.environ["DOCKER"] = _NO_DAEMON
    try:
        for part in (_isolated, _durations, _rules, _expect_rules, _expand, _random,
                     _log_capture, _signals, _deadlines, _hung_inspect, _inspect_redacted,
                     _ndjson, _stderr, _slope, _checks, _vm_resets, _ledger_checks,
                     _kill_watchdog, _kill_ledger, _expect_checks, _gauge_lives, _target_rules,
                     _sink_drain, _external_checks, _kafka_rules, _kafka_topic, _kafka_checks,
                     _report, _expected_event_rules, _exit_driver, _ready_words, _exit_watchdog,
                     _script_ledger, _sink_ledger, _roles, _sink_rules, _driver_roles,
                     _sources_ledger, _source_rules, _fanin_driver, _tcp_ledger, _tcp_rules,
                     _file_ledger, _file_rules,
                     _two_sut_rules,
                     _two_sut_driver, _two_sut_checks, _per_service, _slope_edges,
                     _expected_rules, _slope_reducer, _standin_codec, _standin_record_parts,
                     _standin_server, _respond_rules, _standin_driver, _standin_checks,
                     _class_signatures, _datadog_codec, _datadog_counts, _datadog_server,
                     _datadog_rules, _datadog_checks, _shipped):
            try:
                takes_root = (_rules, _expect_rules, _expand, _random, _signals, _hung_inspect,
                              _target_rules, _sink_drain, _kafka_rules, _expected_event_rules,
                              _shipped, _roles, _sink_rules, _driver_roles, _source_rules,
                              _fanin_driver, _tcp_rules, _file_rules, _two_sut_rules,
                              _two_sut_driver,
                              _expected_rules, _standin_codec, _standin_server,
                              _respond_rules, _standin_driver, _datadog_codec, _datadog_server,
                              _datadog_rules)
                part(root) if part in takes_root else part()
            except Exception as err:  # report the part that broke, then keep going
                _FAILURES.append(f"{part.__name__}: {type(err).__name__}: {err}")
    finally:
        if previous is None:
            os.environ.pop("DOCKER", None)
        else:
            os.environ["DOCKER"] = previous
    if _FAILURES:
        for failure in _FAILURES:
            print(f"self-test FAIL: {failure}")
        return 1
    if not quiet:
        print(f"self-test: {_PASSED[0]} expectations passed")
    return 0
